"""Sogou/WeChat HTTP, proxy, and article parsing backend."""

from __future__ import annotations

import argparse
import base64
import concurrent.futures
import hashlib
import hmac
import json
import os
import random
import re
import shlex
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import quote, urljoin, urlparse, urlsplit

import requests
from bs4 import BeautifulSoup


SOGOU_HOME = "https://weixin.sogou.com/"
SOGOU_SEARCH = "https://weixin.sogou.com/weixin"
KDL_ACCOUNT_ORDERS_URL = "https://dev.kdlapi.com/api/getaccountorders"
KDL_ORDER_SECRET_URL = "https://dev.kdlapi.com/api/getordersecret"
KDL_PROXY_AUTH_URL = "https://dev.kdlapi.com/api/getproxyauthorization"
KDL_TPS_URL = "https://tps.kdlapi.com/api/gettps"
KDL_API_MIN_INTERVAL = 1.05

HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.7",
    "Cache-Control": "no-cache",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0.0.0 Safari/537.36"
    ),
}

BLOCK_MARKERS = (
    "请输入验证码",
    "访问过于频繁",
    "您的访问出错了",
    "antispider",
)

WECHAT_UNAVAILABLE_MARKERS = {
    "publisher_deleted": "该内容已被发布者删除",
    "violations_removed": "此内容因违规无法查看",
    "account_blocked": "此账号已被屏蔽, 内容无法查看",
    "account_banned": "该公众号已被封禁",
}

WECHAT_BLOCK_MARKERS = {
    "environment_error": "环境异常",
    "too_frequent": "访问过于频繁",
    "slider": "请按住滑块",
}

MAX_WECHAT_DECODED_BYTES = 4_000_000
STREAM_CHUNK_BYTES = 32 * 1024
STRUCTURED_TAIL_BYTES = 32 * 1024


class WeChatArticleError(ValueError):
    """Base class for classified, non-successful WeChat responses."""


class WeChatUnavailableError(WeChatArticleError):
    """The publisher or platform made the article unavailable."""


class WeChatCaptchaError(WeChatArticleError):
    """WeChat returned an interactive verification page."""


class WeChatBlockedError(WeChatArticleError):
    """WeChat returned a risk-control page."""


class WeChatContentError(WeChatArticleError):
    """The response did not contain a usable complete article body."""


class KuaidailiError(RuntimeError):
    """Kuaidaili credentials, order, or TPS resolution failed."""

    def __init__(self, stage: str, message: str, code: int | None = None) -> None:
        super().__init__(message)
        self.stage = stage
        self.message = message
        self.code = code


@dataclass
class Article:
    title: str | None
    summary: str | None
    account: str | None
    published_at_unix: int | None
    sogou_link: str | None
    image: str | None


@dataclass
class Attempt:
    proxy: str
    ok: bool
    elapsed_seconds: float
    status: int | None = None
    reason: str | None = None


@dataclass
class FetchedArticle:
    title: str | None
    account: str | None
    published_at: str | None
    resolved_url: str
    source_sogou_link: str | None
    cover_image: str | None
    body_text: str
    body_html: str
    content_quality: str
    image_count: int
    link_count: int
    embedded_media_count: int
    transport: str


def normalize_proxy(value: str) -> str | None:
    """Normalize common exported proxy formats without printing credentials."""
    value = value.strip()
    if not value or value.startswith("#"):
        return None
    if "://" in value:
        return value

    parts = value.split(":")
    if len(parts) == 2 and parts[1].isdigit():
        return f"http://{value}"
    if len(parts) == 4 and parts[1].isdigit():
        host, port, username, password = parts
        return f"http://{quote(username)}:{quote(password)}@{host}:{port}"
    raise ValueError(f"Unsupported proxy format: {redact_proxy(value)}")


def redact_proxy(proxy: str) -> str:
    """Return a log-safe proxy label."""
    proxy = proxy.removeprefix("http://").removeprefix("https://")
    if "@" in proxy:
        return proxy.rsplit("@", 1)[-1]
    parts = proxy.split(":")
    if len(parts) == 4 and parts[1].isdigit():
        return f"{parts[0]}:{parts[1]}"
    return proxy


def redact_error(exc: Exception, proxy: str) -> str:
    message = str(exc).replace(proxy, redact_proxy(proxy))
    message = re.sub(r"(https?://)[^/@\s]+@", r"\1***@", message)
    message = re.sub(
        r"([?&](?:token|signature)=)[^&\s]+", r"\1***", message, flags=re.I
    )
    return f"{type(exc).__name__}: {message}"


def _parse_env_value(value: str) -> str:
    try:
        parsed = shlex.split(value, posix=True)
    except ValueError:
        parsed = []
    return parsed[0] if parsed else value.strip().strip("\"'")


def kuaidaili_credentials(
    environ: dict[str, str] | None = None,
    env_path: Path | None = None,
) -> tuple[str, str]:
    """Load Kuaidaili account credentials without exposing them in argv."""
    environ = os.environ if environ is None else environ
    secret_id = environ.get("KDL_SECRET_ID", "")
    secret_key = environ.get("KDL_SECRET_KEY", "")
    path = env_path or Path.home() / ".op" / "myenv.env"
    if (not secret_id or not secret_key) and path.is_file():
        values: dict[str, str] = {}
        for raw in path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip().removeprefix("export ").strip()
            if key in {"KDL_SECRET_ID", "KDL_SECRET_KEY"}:
                values[key] = _parse_env_value(value)
        secret_id = secret_id or values.get("KDL_SECRET_ID", "")
        secret_key = secret_key or values.get("KDL_SECRET_KEY", "")
    if not secret_id or not secret_key:
        raise KuaidailiError(
            "credentials",
            "Configure KDL_SECRET_ID and KDL_SECRET_KEY for proxy fallback.",
        )
    return secret_id, secret_key


def _kdl_signature(
    secret_key: str, method: str, endpoint: str, params: dict[str, Any]
) -> str:
    path = urlsplit(endpoint).path
    query = "&".join(f"{key}={params[key]}" for key in sorted(params))
    raw = f"{method.upper()}{path}?{query}".encode("utf-8")
    digest = hmac.new(secret_key.encode("utf-8"), raw, hashlib.sha1).digest()
    return base64.b64encode(digest).decode("ascii")


def _kdl_params(
    secret_id: str,
    secret_key: str,
    endpoint: str,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {
        "secret_id": secret_id,
        "sign_type": "hmacsha1",
        "timestamp": int(time.time()),
        "nonce": random.randint(1, 100_000_000),
    }
    params.update(extra or {})
    params["signature"] = _kdl_signature(secret_key, "GET", endpoint, params)
    return params


def _kdl_json(
    session: requests.Session,
    stage: str,
    endpoint: str,
    secret_id: str,
    secret_key: str,
    extra: dict[str, Any] | None = None,
    timeout: float = 15,
) -> Any:
    try:
        response = session.get(
            endpoint,
            params=_kdl_params(secret_id, secret_key, endpoint, extra),
            timeout=timeout,
        )
        result = response.json()
    except Exception as exc:
        raise KuaidailiError(stage, type(exc).__name__) from exc
    if response.status_code != 200:
        raise KuaidailiError(stage, f"HTTP {response.status_code}")
    if result.get("code") != 0:
        raise KuaidailiError(
            stage,
            result.get("msg") or "Kuaidaili API request failed.",
            result.get("code"),
        )
    return result.get("data")


def fetch_kuaidaili_tps(source_timeout: float = 15) -> list[str]:
    """Resolve the active Kuaidaili TPS order to one authenticated endpoint."""
    account_id, account_key = kuaidaili_credentials()
    with direct_session() as session:
        orders = _kdl_json(
            session,
            "orders",
            KDL_ACCOUNT_ORDERS_URL,
            account_id,
            account_key,
            {"product": "TPS"},
            source_timeout,
        )
        active = [
            order
            for order in (orders or [])
            if order.get("product") == "TPS" and order.get("status") == "VALID"
        ]
        if not active:
            raise KuaidailiError("orders", "No active TPS order is available.")

        time.sleep(KDL_API_MIN_INTERVAL)
        order_secret = _kdl_json(
            session,
            "order_secret",
            KDL_ORDER_SECRET_URL,
            account_id,
            account_key,
            {"orderid": active[0]["orderid"]},
            source_timeout,
        )
        order_id = (order_secret or {}).get("secret_id", "")
        order_key = (order_secret or {}).get("secret_key", "")
        if not order_id or not order_key:
            raise KuaidailiError("order_secret", "Order credentials are missing.")

        time.sleep(KDL_API_MIN_INTERVAL)
        proxy_data = _kdl_json(
            session,
            "tps",
            KDL_TPS_URL,
            order_id,
            order_key,
            {"num": 1, "format": "json"},
            source_timeout,
        )
        proxies = (proxy_data or {}).get("proxy_list") or []
        if not proxies:
            raise KuaidailiError("tps", "The TPS order returned no endpoint.")

        time.sleep(KDL_API_MIN_INTERVAL)
        auth = _kdl_json(
            session,
            "proxy_auth",
            KDL_PROXY_AUTH_URL,
            order_id,
            order_key,
            {"plaintext": 1},
            source_timeout,
        )
        username = (auth or {}).get("username", "")
        password = (auth or {}).get("password", "")
        if not username or not password:
            raise KuaidailiError("proxy_auth", "Proxy credentials are missing.")

    try:
        host, port = proxies[0].rsplit(":", 1)
        int(port)
    except (ValueError, AttributeError) as exc:
        raise KuaidailiError("tps", "Invalid proxy endpoint.") from exc
    return [
        f"http://{quote(username, safe='')}:{quote(password, safe='')}@{host}:{port}"
    ]


def load_proxy_file(path: Path) -> list[str]:
    proxies = [normalize_proxy(line) for line in path.read_text().splitlines()]
    return [proxy for proxy in proxies if proxy]


def parse_articles(html: str) -> list[Article]:
    soup = BeautifulSoup(html, "lxml")
    articles: list[Article] = []
    for item in soup.select("ul.news-list > li"):
        anchor = item.select_one("h3 a")
        script = item.select_one("span.s2 script")
        timestamp_match = re.search(
            r"timeConvert\('([0-9]+)'\)", script.get_text() if script else ""
        )
        image = item.select_one("img")
        href = anchor.get("href") if anchor else None
        articles.append(
            Article(
                title=anchor.get_text(" ", strip=True) if anchor else None,
                summary=(
                    item.select_one("p.txt-info").get_text(" ", strip=True)
                    if item.select_one("p.txt-info")
                    else None
                ),
                account=(
                    item.select_one("span.all-time-y2").get_text(" ", strip=True)
                    if item.select_one("span.all-time-y2")
                    else None
                ),
                published_at_unix=(
                    int(timestamp_match.group(1)) if timestamp_match else None
                ),
                sogou_link=urljoin(SOGOU_HOME, href) if href else None,
                image=urljoin(SOGOU_HOME, image.get("src")) if image else None,
            )
        )
    return articles


def extract_wechat_target(html: str) -> str | None:
    """Extract the mp.weixin.qq.com target assembled by Sogou's link script."""
    pieces = re.findall(r"url\s*\+=\s*'([^']*)';", html)
    target = "".join(pieces).replace("@", "")
    if not target:
        direct = re.search(
            r"(?:location\.replace|location\.href)\(['\"]([^'\"]+)", html
        )
        target = direct.group(1) if direct else ""
    parsed = urlparse(target)
    if parsed.scheme == "https" and parsed.hostname == "mp.weixin.qq.com":
        return target
    return None


def _meta_content(soup: BeautifulSoup, key: str) -> str | None:
    node = soup.select_one(f'meta[property="{key}"]') or soup.select_one(
        f'meta[name="{key}"]'
    )
    value = node.get("content") if node else None
    return value.strip() if isinstance(value, str) else None


def _extract_js_quoted_value(source: str, key: str) -> str | None:
    """Extract a single- or double-quoted JS object value without evaluating it."""
    match = re.search(rf"\b{re.escape(key)}\s*:\s*(['\"])", source)
    if match is None:
        return None
    quote_char = match.group(1)
    cursor = match.end()
    start = cursor
    while cursor < len(source):
        char = source[cursor]
        if char == "\\":
            cursor += 2
            continue
        if char == quote_char:
            return source[start:cursor]
        cursor += 1
    return None


def _decode_js_string(value: str) -> str:
    """Decode the limited JS escapes used by WeChat's content_noencode field."""
    output: list[str] = []
    cursor = 0
    simple_escapes = {
        "n": "\n",
        "r": "\r",
        "t": "\t",
        "b": "\b",
        "f": "\f",
        "v": "\v",
    }
    while cursor < len(value):
        char = value[cursor]
        if char != "\\":
            output.append(char)
            cursor += 1
            continue
        cursor += 1
        if cursor >= len(value):
            break
        escaped = value[cursor]
        if escaped == "x" and cursor + 2 < len(value):
            try:
                output.append(chr(int(value[cursor + 1 : cursor + 3], 16)))
                cursor += 3
                continue
            except ValueError:
                pass
        if escaped == "u" and cursor + 4 < len(value):
            try:
                codepoint = int(value[cursor + 1 : cursor + 5], 16)
                if 0xD800 <= codepoint <= 0xDBFF:
                    pair = value[cursor + 5 : cursor + 11]
                    if pair.startswith("\\u") and len(pair) == 6:
                        low = int(pair[2:], 16)
                        if 0xDC00 <= low <= 0xDFFF:
                            codepoint = (
                                0x10000
                                + ((codepoint - 0xD800) << 10)
                                + (low - 0xDC00)
                            )
                            cursor += 6
                output.append(chr(codepoint))
                cursor += 5
                continue
            except (ValueError, UnicodeEncodeError):
                pass
        output.append(simple_escapes.get(escaped, escaped))
        cursor += 1
    return "".join(output)


def _complete_js_value_end(payload: bytes, key: bytes) -> int | None:
    marker_at = payload.find(key)
    if marker_at < 0:
        return None
    colon_at = payload.find(b":", marker_at + len(key))
    if colon_at < 0:
        return None
    quote_at = -1
    quote_byte = 0
    for cursor in range(colon_at + 1, min(colon_at + 32, len(payload))):
        if payload[cursor] in (34, 39):
            quote_at = cursor
            quote_byte = payload[cursor]
            break
    if quote_at < 0:
        return None
    cursor = quote_at + 1
    while cursor < len(payload):
        byte = payload[cursor]
        if byte == 92:
            cursor += 2
            continue
        if byte == quote_byte:
            return cursor + 1
        cursor += 1
    return None


def read_wechat_response(
    session: requests.Session,
    url: str,
    timeout: float,
    headers: dict[str, str] | None = None,
) -> tuple[str, str, int]:
    """Stream enough of a WeChat page to obtain a complete article body."""
    with session.get(
        url,
        headers=headers,
        timeout=max(timeout, 12),
        allow_redirects=True,
        stream=True,
    ) as response:
        response.raise_for_status()
        payload = bytearray()
        for chunk in response.iter_content(chunk_size=STREAM_CHUNK_BYTES):
            if not chunk:
                continue
            payload.extend(chunk)
            content_end = _complete_js_value_end(
                payload, b"content_noencode"
            )
            has_structured_tail = (
                content_end is not None
                and (
                    len(payload) - content_end >= STRUCTURED_TAIL_BYTES
                    or (
                        b"create_time:" in payload[content_end:]
                        and b"nickname:" in payload[content_end:]
                    )
                )
            )
            terminal_page = any(
                marker.encode("utf-8") in payload
                for marker in (
                    *WECHAT_UNAVAILABLE_MARKERS.values(),
                    *WECHAT_BLOCK_MARKERS.values(),
                )
            )
            if (
                has_structured_tail
                or b'id="js_pc_qr_code"' in payload
                or terminal_page
                or len(payload) >= MAX_WECHAT_DECODED_BYTES
            ):
                break
        encoding = response.encoding or "utf-8"
        return (
            bytes(payload).decode(encoding, errors="replace"),
            response.url,
            response.status_code,
        )


def direct_session() -> requests.Session:
    session = requests.Session()
    session.trust_env = False
    session.headers.update(HEADERS)
    return session


def parse_wechat_article(
    html: str,
    resolved_url: str,
    source_sogou_link: str | None,
    transport: str = "proxy",
    account_hint: str | None = None,
) -> FetchedArticle:
    soup = BeautifulSoup(html, "lxml")
    parsed_url = urlparse(resolved_url)
    if "appmsgcaptcha" in parsed_url.path:
        raise WeChatCaptchaError("WeChat returned an article verification page")
    for reason, marker in WECHAT_UNAVAILABLE_MARKERS.items():
        if marker in html:
            raise WeChatUnavailableError(f"WeChat article unavailable: {reason}")
    for reason, marker in WECHAT_BLOCK_MARKERS.items():
        if marker in html:
            raise WeChatBlockedError(f"WeChat risk control: {reason}")

    encoded_content = _extract_js_quoted_value(html, "content_noencode")
    if encoded_content is not None:
        decoded_content = _decode_js_string(encoded_content)
        content_soup = BeautifulSoup(decoded_content, "lxml")
        content = content_soup.body or content_soup
    else:
        content = soup.select_one("#js_content")
    if content is None:
        raise WeChatContentError(
            "WeChat response did not contain a complete article body"
        )

    for unwanted in content.select("script, style, noscript"):
        unwanted.decompose()
    for image in content.select("img[data-src]"):
        if not image.get("src"):
            image["src"] = image.get("data-src")

    title_node = soup.select_one("#activity-name")
    account_node = soup.select_one("#js_name")
    published_node = soup.select_one("#publish_time")
    title = (
        title_node.get_text(" ", strip=True)
        if title_node
        else _meta_content(soup, "og:title")
    )
    meta_author = _meta_content(soup, "og:article:author")
    if meta_author in {"", "请关注", "微信公众平台"}:
        meta_author = None
    account = (
        account_node.get_text(" ", strip=True)
        if account_node
        else (
            account_hint
            or meta_author
            or _extract_js_quoted_value(html, "nickname")
            or _extract_js_quoted_value(html, "alias")
        )
    )
    published_at = (
        published_node.get_text(" ", strip=True)
        if published_node
        else _extract_js_quoted_value(html, "create_time")
    )
    lines = [line.strip() for line in content.get_text("\n").splitlines()]
    body_text = "\n".join(line for line in lines if line)
    image_count = len(content.select("img"))
    link_count = len(content.select("a"))
    embedded_media_count = len(
        content.select("video, iframe, mp-common-videosnap, mpvoice")
    )
    normalized_chars = len(re.sub(r"\s+", "", body_text).replace("\u200b", ""))
    if normalized_chars >= 50:
        content_quality = "substantive_text"
    elif normalized_chars > 0 and (image_count or embedded_media_count):
        content_quality = "short_mixed"
    elif image_count or embedded_media_count:
        content_quality = "media_only"
    elif normalized_chars > 0:
        content_quality = "minimal_text"
    else:
        raise WeChatContentError("WeChat article body was empty")
    return FetchedArticle(
        title=title,
        account=account,
        published_at=published_at,
        resolved_url=resolved_url,
        source_sogou_link=source_sogou_link,
        cover_image=_meta_content(soup, "og:image"),
        body_text=body_text,
        body_html=content.decode_contents().strip(),
        content_quality=content_quality,
        image_count=image_count,
        link_count=link_count,
        embedded_media_count=embedded_media_count,
        transport=transport,
    )


def is_blocked(response: requests.Response) -> bool:
    sample = f"{response.url}\n{response.text[:12000]}".lower()
    return any(marker.lower() in sample for marker in BLOCK_MARKERS)


def proxy_session(proxy: str) -> requests.Session:
    session = requests.Session()
    session.trust_env = False
    session.headers.update(HEADERS)
    if proxy != "direct":
        session.proxies.update({"http": proxy, "https": proxy})
    return session


def search_once(
    proxy: str,
    keyword: str,
    page: int,
    timeout: float,
) -> tuple[list[Article], Attempt]:
    started = time.monotonic()
    label = redact_proxy(proxy)
    try:
        with proxy_session(proxy) as session:
            response = session.get(
                SOGOU_SEARCH,
                params={
                    "type": "2",
                    "query": keyword,
                    "page": str(page),
                    "ie": "utf8",
                    "s_from": "input",
                    "_sug_": "n",
                },
                headers={"Referer": SOGOU_HOME},
                timeout=timeout,
            )
            response.raise_for_status()
            if is_blocked(response):
                raise RuntimeError("Sogou returned an anti-bot/captcha page")
            articles = parse_articles(response.text)
            return articles, Attempt(
                proxy=label,
                ok=True,
                status=response.status_code,
                elapsed_seconds=round(time.monotonic() - started, 3),
            )
    except Exception as exc:
        status = getattr(getattr(exc, "response", None), "status_code", None)
        return [], Attempt(
            proxy=label,
            ok=False,
            status=status,
            elapsed_seconds=round(time.monotonic() - started, 3),
            reason=redact_error(exc, proxy),
        )


def iter_candidates(proxies: Iterable[str], attempts: int) -> Iterable[str]:
    candidates = list(dict.fromkeys(proxies))
    random.SystemRandom().shuffle(candidates)
    yield from candidates[:attempts]


def search_parallel(
    proxies: Iterable[str],
    keyword: str,
    page: int,
    timeout: float,
    concurrency: int,
) -> tuple[list[Article], Attempt | None, list[Attempt]]:
    """Try proxies in bounded parallel batches and keep the fastest success."""
    candidates = list(proxies)
    attempts: list[Attempt] = []
    for start in range(0, len(candidates), concurrency):
        batch = candidates[start : start + concurrency]
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=len(batch), thread_name_prefix="sogou-proxy"
        ) as executor:
            futures = [
                executor.submit(search_once, proxy, keyword, page, timeout)
                for proxy in batch
            ]
            successes: list[tuple[list[Article], Attempt]] = []
            for future in concurrent.futures.as_completed(futures):
                articles, attempt = future.result()
                attempts.append(attempt)
                if attempt.ok:
                    successes.append((articles, attempt))
        if successes:
            return min(successes, key=lambda item: item[1].elapsed_seconds) + (
                attempts,
            )
    return [], None, attempts


def _select_reference_article(
    articles: list[Article], reference: dict[str, Any]
) -> Article:
    title = reference.get("title")
    account = reference.get("account")
    for article in articles:
        if article.title == title and (not account or article.account == account):
            return article
    raise LookupError("The referenced search result is no longer on this result page")


def fetch_direct_once(
    url: str,
    timeout: float,
    source_sogou_link: str | None = None,
    referer: str | None = None,
    account_hint: str | None = None,
) -> tuple[FetchedArticle | None, Attempt]:
    started = time.monotonic()
    try:
        with direct_session() as session:
            html, resolved_url, status = read_wechat_response(
                session,
                url,
                timeout,
                headers={"Referer": referer} if referer else None,
            )
        article = parse_wechat_article(
            html,
            resolved_url,
            source_sogou_link,
            transport="direct",
            account_hint=account_hint,
        )
        return article, Attempt(
            proxy="direct",
            ok=True,
            status=status,
            elapsed_seconds=round(time.monotonic() - started, 3),
        )
    except Exception as exc:
        status = getattr(getattr(exc, "response", None), "status_code", None)
        return None, Attempt(
            proxy="direct",
            ok=False,
            status=status,
            elapsed_seconds=round(time.monotonic() - started, 3),
            reason=f"{type(exc).__name__}: {exc}",
        )


def fetch_once(
    proxy: str,
    source: dict[str, Any],
    timeout: float,
    article_transport: str = "auto",
) -> tuple[FetchedArticle | None, Attempt]:
    started = time.monotonic()
    label = redact_proxy(proxy)
    try:
        with proxy_session(proxy) as session:
            source_sogou_link: str | None = None
            account_hint: str | None = None
            if source["kind"] == "search":
                params = {
                    "type": "2",
                    "query": source["query"],
                    "page": str(source["page"]),
                    "ie": "utf8",
                    "s_from": "input",
                    "_sug_": "n",
                }
                search_response = session.get(
                    SOGOU_SEARCH,
                    params=params,
                    headers={"Referer": SOGOU_HOME},
                    timeout=timeout,
                )
                search_response.raise_for_status()
                if is_blocked(search_response):
                    raise RuntimeError("Sogou returned an anti-bot/captcha page")
                selected = _select_reference_article(
                    parse_articles(search_response.text), source
                )
                if not selected.sogou_link:
                    raise ValueError("Search result did not contain a Sogou link")
                account_hint = selected.account
                source_sogou_link = selected.sogou_link
                referer = search_response.url
            else:
                source_url = source["url"]
                parsed_source = urlparse(source_url)
                if parsed_source.hostname == "mp.weixin.qq.com":
                    if article_transport in ("auto", "direct"):
                        article, direct_attempt = fetch_direct_once(
                            source_url, timeout
                        )
                        if article is not None:
                            return article, direct_attempt
                        if article_transport == "direct":
                            raise RuntimeError(direct_attempt.reason)
                    article_html, article_url, article_status = read_wechat_response(
                        session, source_url, timeout
                    )
                    article = parse_wechat_article(
                        article_html,
                        article_url,
                        None,
                        transport="proxy",
                    )
                    return article, Attempt(
                        proxy=label,
                        ok=True,
                        status=article_status,
                        elapsed_seconds=round(time.monotonic() - started, 3),
                    )
                source_sogou_link = source_url
                referer = SOGOU_HOME

            link_response = session.get(
                source_sogou_link,
                headers={"Referer": referer},
                timeout=timeout,
                allow_redirects=True,
            )
            link_response.raise_for_status()
            parsed_link = urlparse(link_response.url)
            if parsed_link.hostname == "mp.weixin.qq.com":
                target = link_response.url
            else:
                target = extract_wechat_target(link_response.text)
                if not target:
                    raise ValueError(
                        "Sogou link response did not expose a WeChat target"
                    )
            if article_transport in ("auto", "direct"):
                article, direct_attempt = fetch_direct_once(
                    target,
                    timeout,
                    source_sogou_link=source_sogou_link,
                    referer=link_response.url,
                    account_hint=account_hint,
                )
                if article is not None:
                    direct_attempt.proxy = label
                    direct_attempt.elapsed_seconds = round(
                        time.monotonic() - started, 3
                    )
                    return article, direct_attempt
                if article_transport == "direct":
                    raise RuntimeError(direct_attempt.reason)

            article_html, article_url, article_status = read_wechat_response(
                session,
                target,
                timeout,
                headers={"Referer": link_response.url},
            )
            article = parse_wechat_article(
                article_html,
                article_url,
                source_sogou_link,
                transport="proxy",
                account_hint=account_hint,
            )
            return article, Attempt(
                proxy=label,
                ok=True,
                status=article_status,
                elapsed_seconds=round(time.monotonic() - started, 3),
            )
    except Exception as exc:
        status = getattr(getattr(exc, "response", None), "status_code", None)
        return None, Attempt(
            proxy=label,
            ok=False,
            status=status,
            elapsed_seconds=round(time.monotonic() - started, 3),
            reason=redact_error(exc, proxy),
        )


def fetch_parallel(
    proxies: Iterable[str],
    source: dict[str, Any],
    timeout: float,
    concurrency: int,
    article_transport: str = "auto",
) -> tuple[FetchedArticle | None, Attempt | None, list[Attempt]]:
    candidates = list(proxies)
    attempts: list[Attempt] = []
    for start in range(0, len(candidates), concurrency):
        batch = candidates[start : start + concurrency]
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=len(batch), thread_name_prefix="sogou-fetch"
        ) as executor:
            futures = [
                executor.submit(
                    fetch_once, proxy, source, timeout, article_transport
                )
                for proxy in batch
            ]
            successes: list[tuple[FetchedArticle, Attempt]] = []
            for future in concurrent.futures.as_completed(futures):
                article, attempt = future.result()
                attempts.append(attempt)
                if article is not None:
                    successes.append((article, attempt))
        if successes:
            article, winner = min(successes, key=lambda item: item[1].elapsed_seconds)
            return article, winner, attempts
    return None, None, attempts


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Search Sogou Weixin directly with Kuaidaili TPS fallback."
    )
    parser.add_argument("keyword")
    parser.add_argument("--page", type=int, default=1)
    parser.add_argument("--attempts", type=int, default=8)
    parser.add_argument(
        "--concurrency",
        type=int,
        default=4,
        help="Number of proxies tested at once (default: 4, maximum: 16).",
    )
    parser.add_argument("--timeout", type=float, default=8.0)
    parser.add_argument("--pool-size", type=int, default=100)
    parser.add_argument(
        "--proxy-file",
        type=Path,
        help="Explicit private proxy override instead of Kuaidaili TPS.",
    )
    parser.add_argument(
        "--show-attempts",
        action="store_true",
        help="Include redacted per-proxy diagnostics in the JSON output.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.page < 1 or args.attempts < 1 or args.pool_size < 1:
        raise SystemExit("page, attempts, and pool-size must be positive")
    if not 1 <= args.concurrency <= 16:
        raise SystemExit("concurrency must be between 1 and 16")

    articles, direct_attempt = search_once(
        "direct", args.keyword, args.page, args.timeout
    )
    attempts = [direct_attempt]
    winner = direct_attempt if direct_attempt.ok else None
    if winner is None:
        proxies = (
            load_proxy_file(args.proxy_file)
            if args.proxy_file
            else fetch_kuaidaili_tps(source_timeout=20)
        )
        if not proxies:
            raise SystemExit("The fallback returned no proxy endpoint")
        articles, winner, fallback_attempts = search_parallel(
            proxies=iter_candidates(proxies, args.attempts),
            keyword=args.keyword,
            page=args.page,
            timeout=args.timeout,
            concurrency=min(args.concurrency, args.attempts),
        )
        attempts.extend(fallback_attempts)
    if winner:
        output: dict[str, object] = {
            "keyword": args.keyword,
            "page": args.page,
            "proxy": winner.proxy,
            "count": len(articles),
            "articles": [asdict(article) for article in articles],
        }
        if args.show_attempts:
            output["attempts"] = [asdict(item) for item in attempts]
        print(json.dumps(output, ensure_ascii=False, indent=2))
        return 0

    output = {
        "keyword": args.keyword,
        "page": args.page,
        "error": "Direct search and Kuaidaili fallback both failed",
        "attempts": [asdict(item) for item in attempts],
    }
    print(json.dumps(output, ensure_ascii=False, indent=2))
    return 2


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except requests.RequestException as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        raise SystemExit(3) from exc
