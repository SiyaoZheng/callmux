"""Search, fetch, batching, and proxy orchestration for Sogou Weixin."""

from __future__ import annotations

import argparse
import asyncio
import base64
import concurrent.futures
import hashlib
import html
import importlib.metadata
import json
import os
import platform
import re
import sqlite3
import sys
import threading
import time
import zlib
from dataclasses import asdict
from pathlib import Path
from typing import Any, Sequence
from urllib.parse import parse_qsl, urljoin, urlparse

import requests

from sogou_weixin_transport import (
    SOGOU_HOME,
    Attempt,
    Article,
    FetchedArticle,
    KuaidailiError,
    fetch_kuaidaili_tps,
    fetch_direct_once,
    fetch_parallel,
    fetch_once,
    iter_candidates,
    kuaidaili_credentials,
    load_proxy_file,
    proxy_session,
    redact_error,
    redact_proxy,
    search_parallel,
    search_once,
)


VERSION = "0.6.1"
EXIT_INVALID = 2
EXIT_NETWORK = 3
EXIT_UNAVAILABLE = 4
EXIT_NO_PROXY = 5
CACHE_SEARCH_TTL_SECONDS = 6 * 60 * 60
CACHE_ARTICLE_TTL_SECONDS = 30 * 24 * 60 * 60
CACHE_MAX_BYTES_DEFAULT = 48 * 1024 * 1024
CACHE_INDEX_BUDGET_BYTES = 32 * 1024 * 1024
CACHE_TOTAL_LIMIT_BYTES = 96 * 1024 * 1024
CACHE_PATH_DEFAULT = Path(
    os.environ.get(
        "SOGOU_WEIXIN_CACHE",
        str(Path.home() / ".cache" / "sogou-weixin" / "cache.sqlite3"),
    )
)


class CliError(Exception):
    def __init__(
        self,
        code: str,
        message: str,
        exit_code: int,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.exit_code = exit_code
        self.details = details or {}


class SharedProxyProvider:
    """Load one proxy pool lazily and share it across batch worker threads."""

    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.lock = threading.Lock()
        self.loaded = False
        self.value: tuple[list[str], str] | None = None
        self.error: CliError | None = None

    def get(self) -> tuple[list[str], str]:
        with self.lock:
            if not self.loaded:
                try:
                    self.value = get_proxies(self.args)
                except CliError as exc:
                    self.error = exc
                self.loaded = True
            if self.error is not None:
                raise self.error
            assert self.value is not None
            return self.value


class AsyncStartRateLimiter:
    """Space batch item starts at one global rate."""

    def __init__(self, requests_per_second: float) -> None:
        self.interval = 1.0 / requests_per_second
        self.lock = asyncio.Lock()
        self.next_start = 0.0

    async def wait(self) -> None:
        loop = asyncio.get_running_loop()
        async with self.lock:
            now = loop.time()
            scheduled = max(now, self.next_start)
            self.next_start = scheduled + self.interval
        delay = scheduled - loop.time()
        if delay > 0:
            await asyncio.sleep(delay)


class CompressedCache:
    """Small zlib-compressed SQLite cache with indexed LRU eviction."""

    def __init__(
        self,
        path: Path,
        max_bytes: int,
        index_budget_bytes: int = CACHE_INDEX_BUDGET_BYTES,
        total_limit_bytes: int = CACHE_TOTAL_LIMIT_BYTES,
    ) -> None:
        self.path = path
        self.max_bytes = max_bytes
        self.index_budget_bytes = index_budget_bytes
        self.total_limit_bytes = total_limit_bytes
        self.lock = threading.Lock()

    def _connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path, timeout=10)
        connection.execute("PRAGMA auto_vacuum=FULL")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA journal_size_limit=1048576")
        connection.execute("PRAGMA wal_autocheckpoint=100")
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS entries (
                cache_key TEXT PRIMARY KEY,
                namespace TEXT NOT NULL,
                created_at REAL NOT NULL,
                accessed_at REAL NOT NULL,
                compressed_bytes INTEGER NOT NULL,
                payload BLOB NOT NULL
            )
            """
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS entries_lru ON entries(accessed_at)"
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS article_index (
                article_id TEXT PRIMARY KEY,
                title TEXT,
                account TEXT,
                published_at TEXT,
                fetch_ref TEXT,
                image TEXT,
                url TEXT,
                body_type TEXT,
                text_chars INTEGER,
                html_chars INTEGER,
                body_sha256 TEXT,
                first_seen REAL NOT NULL,
                last_seen REAL NOT NULL,
                fetched_at REAL
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS query_index (
                query TEXT NOT NULL,
                page INTEGER NOT NULL,
                rank INTEGER NOT NULL,
                article_id TEXT NOT NULL,
                last_seen REAL NOT NULL,
                PRIMARY KEY(query, page, article_id)
            )
            """
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS article_title_account "
            "ON article_index(title, account)"
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS query_lookup "
            "ON query_index(query, page, rank)"
        )
        if int(connection.execute("PRAGMA user_version").fetchone()[0]) < 2:
            connection.execute(
                "UPDATE article_index SET fetch_ref=NULL, image=NULL, url=NULL"
            )
            connection.execute("PRAGMA user_version=2")
        return connection

    @staticmethod
    def _database_bytes(connection: sqlite3.Connection) -> int:
        page_count = int(connection.execute("PRAGMA page_count").fetchone()[0])
        page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
        return page_count * page_size

    def _estimated_index_bytes(self, connection: sqlite3.Connection) -> int:
        compressed = int(
            connection.execute(
                "SELECT COALESCE(SUM(compressed_bytes), 0) FROM entries"
            ).fetchone()[0]
        )
        return max(0, self._database_bytes(connection) - compressed)

    def _index_can_grow(self, connection: sqlite3.Connection) -> bool:
        return self._estimated_index_bytes(connection) < self.index_budget_bytes

    def _content_limit(self, connection: sqlite3.Connection) -> int:
        remaining_total = max(
            0, self.total_limit_bytes - self._estimated_index_bytes(connection)
        )
        return min(self.max_bytes, remaining_total)

    def _prune_content(self, connection: sqlite3.Connection) -> None:
        content_limit = self._content_limit(connection)
        total = int(
            connection.execute(
                "SELECT COALESCE(SUM(compressed_bytes), 0) FROM entries"
            ).fetchone()[0]
        )
        if total <= content_limit:
            return
        for old_key, old_bytes in connection.execute(
            "SELECT cache_key, compressed_bytes FROM entries "
            "ORDER BY accessed_at ASC"
        ):
            connection.execute(
                "DELETE FROM entries WHERE cache_key = ?", (old_key,)
            )
            total -= int(old_bytes)
            if total <= content_limit:
                break

    @staticmethod
    def key(namespace: str, value: Any) -> str:
        canonical = json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        return hashlib.sha256(f"{namespace}\0{canonical}".encode()).hexdigest()

    def get(
        self, namespace: str, value: Any, ttl_seconds: int | None
    ) -> Any | None:
        cache_key = self.key(namespace, value)
        now = time.time()
        with self.lock, self._connect() as connection:
            row = connection.execute(
                "SELECT created_at, payload FROM entries WHERE cache_key = ?",
                (cache_key,),
            ).fetchone()
            if row is None:
                return None
            if ttl_seconds is not None and now - float(row[0]) > ttl_seconds:
                return None
            connection.execute(
                "UPDATE entries SET accessed_at = ? WHERE cache_key = ?",
                (now, cache_key),
            )
            try:
                return json.loads(zlib.decompress(row[1]).decode("utf-8"))
            except (ValueError, zlib.error, UnicodeDecodeError):
                connection.execute(
                    "DELETE FROM entries WHERE cache_key = ?", (cache_key,)
                )
                return None

    def put(self, namespace: str, value: Any, payload: Any) -> None:
        cache_key = self.key(namespace, value)
        encoded = json.dumps(
            payload, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
        compressed = zlib.compress(encoded, level=9)
        if len(compressed) > self.max_bytes:
            return
        now = time.time()
        with self.lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO entries(
                    cache_key, namespace, created_at, accessed_at,
                    compressed_bytes, payload
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(cache_key) DO UPDATE SET
                    namespace=excluded.namespace,
                    created_at=excluded.created_at,
                    accessed_at=excluded.accessed_at,
                    compressed_bytes=excluded.compressed_bytes,
                    payload=excluded.payload
                """,
                (cache_key, namespace, now, now, len(compressed), compressed),
            )
            self._prune_content(connection)

    @staticmethod
    def article_id(article: dict[str, Any]) -> str:
        identity = {
            "title": article.get("title"),
            "account": article.get("account"),
            "published_at": article.get("published_at")
            or article.get("published_at_unix"),
            "url": article.get("url"),
        }
        return CompressedCache.key("article-index", identity)

    def index_search(
        self, query: str, page: int, articles: list[dict[str, Any]]
    ) -> None:
        now = time.time()
        with self.lock, self._connect() as connection:
            for rank, article in enumerate(articles, 1):
                article_id = self.article_id(article)
                article_exists = connection.execute(
                    "SELECT 1 FROM article_index WHERE article_id = ?",
                    (article_id,),
                ).fetchone() is not None
                if not article_exists and not self._index_can_grow(connection):
                    continue
                connection.execute(
                    """
                    INSERT INTO article_index(
                        article_id, title, account, published_at,
                        first_seen, last_seen
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(article_id) DO UPDATE SET
                        title=excluded.title,
                        account=excluded.account,
                        published_at=excluded.published_at,
                        last_seen=excluded.last_seen
                    """,
                    (
                        article_id,
                        article.get("title"),
                        article.get("account"),
                        str(article.get("published_at_unix") or "") or None,
                        now,
                        now,
                    ),
                )
                mapping_exists = connection.execute(
                    """
                    SELECT 1 FROM query_index
                    WHERE query = ? AND page = ? AND article_id = ?
                    """,
                    (query, page, article_id),
                ).fetchone() is not None
                if mapping_exists or not article_exists or self._index_can_grow(
                    connection
                ):
                    connection.execute(
                        """
                        INSERT INTO query_index(
                            query, page, rank, article_id, last_seen
                        ) VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(query, page, article_id) DO UPDATE SET
                            rank=excluded.rank,
                            last_seen=excluded.last_seen
                        """,
                        (query, page, rank, article_id, now),
                    )
            self._prune_content(connection)

    def index_fetch(
        self, source: dict[str, Any], article: dict[str, Any]
    ) -> None:
        now = time.time()
        body = article.get("body", {})
        text = body.get("text") or ""
        html_value = body.get("html") or ""
        body_hash = hashlib.sha256(
            (text + "\0" + html_value).encode("utf-8")
        ).hexdigest()
        with self.lock, self._connect() as connection:
            article_id = None
            if source.get("kind") == "search":
                row = connection.execute(
                    """
                    SELECT ai.article_id
                    FROM article_index ai
                    JOIN query_index qi ON qi.article_id = ai.article_id
                    WHERE qi.query = ? AND qi.page = ? AND ai.title = ?
                      AND (? IS NULL OR ai.account = ?)
                    ORDER BY qi.last_seen DESC LIMIT 1
                    """,
                    (
                        source.get("query"),
                        source.get("page"),
                        source.get("title"),
                        source.get("account"),
                        source.get("account"),
                    ),
                ).fetchone()
                article_id = row[0] if row else None
            if article_id is None:
                identity_article = dict(article)
                if source.get("kind") == "search":
                    identity_article["url"] = None
                elif source.get("kind") == "url":
                    identity_article["url"] = source.get("url")
                article_id = self.article_id(identity_article)
            article_exists = connection.execute(
                "SELECT 1 FROM article_index WHERE article_id = ?",
                (article_id,),
            ).fetchone() is not None
            if not article_exists and not self._index_can_grow(connection):
                return
            source_url = source.get("url")
            durable_url = (
                source_url
                if source.get("kind") == "url"
                and isinstance(source_url, str)
                and urlparse(source_url).hostname == "mp.weixin.qq.com"
                else None
            )
            connection.execute(
                """
                INSERT INTO article_index(
                    article_id, title, account, published_at, url, body_type,
                    text_chars, html_chars, body_sha256,
                    first_seen, last_seen, fetched_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(article_id) DO UPDATE SET
                    title=excluded.title,
                    account=excluded.account,
                    published_at=excluded.published_at,
                    url=COALESCE(excluded.url, article_index.url),
                    body_type=excluded.body_type,
                    text_chars=excluded.text_chars,
                    html_chars=excluded.html_chars,
                    body_sha256=excluded.body_sha256,
                    last_seen=excluded.last_seen,
                    fetched_at=excluded.fetched_at
                """,
                (
                    article_id,
                    article.get("title"),
                    article.get("account"),
                    article.get("published_at"),
                    durable_url,
                    body.get("type"),
                    body.get("text_chars"),
                    body.get("html_chars"),
                    body_hash,
                    now,
                    now,
                    now,
                ),
            )
            self._prune_content(connection)

    def stats(self) -> dict[str, Any]:
        if not self.path.exists():
            return {
                "entries": 0,
                "indexed_articles": 0,
                "compressed_bytes": 0,
                "file_bytes": 0,
                "disk_bytes": 0,
                "estimated_index_bytes": 0,
                "content_limit_bytes": self.max_bytes,
                "index_budget_bytes": self.index_budget_bytes,
                "total_limit_bytes": self.total_limit_bytes,
                "index_writable": True,
            }
        with self.lock, self._connect() as connection:
            entries, compressed = connection.execute(
                "SELECT COUNT(*), COALESCE(SUM(compressed_bytes), 0) FROM entries"
            ).fetchone()
            indexed = connection.execute(
                "SELECT COUNT(*) FROM article_index"
            ).fetchone()[0]
            estimated_index = self._estimated_index_bytes(connection)
            content_limit = self._content_limit(connection)
            index_writable = self._index_can_grow(connection)
        disk_bytes = sum(
            candidate.stat().st_blocks * 512
            for candidate in (
                self.path,
                Path(f"{self.path}-wal"),
                Path(f"{self.path}-shm"),
            )
            if candidate.exists()
        )
        return {
            "entries": int(entries),
            "indexed_articles": int(indexed),
            "compressed_bytes": int(compressed),
            "file_bytes": self.path.stat().st_size,
            "disk_bytes": disk_bytes,
            "estimated_index_bytes": estimated_index,
            "content_limit_bytes": content_limit,
            "index_budget_bytes": self.index_budget_bytes,
            "total_limit_bytes": self.total_limit_bytes,
            "index_writable": index_writable,
        }


def envelope(command: str, data: Any) -> dict[str, Any]:
    return {
        "ok": True,
        "data": data,
        "meta": {"command": command, "version": VERSION},
    }


def error_envelope(error: CliError) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "ok": False,
        "error": {"code": error.code, "message": error.message},
        "meta": {"version": VERSION},
    }
    if error.details:
        payload["error"]["details"] = error.details
    return payload


def emit_json(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def add_proxy_options(
    parser: argparse.ArgumentParser, *, include_attempts: bool
) -> None:
    default_file = os.environ.get("SOGOU_WEIXIN_PROXY_FILE")
    parser.add_argument(
        "--proxy-file",
        type=Path,
        default=Path(default_file) if default_file else None,
        help=(
            "Explicit private proxy list override; otherwise fallback uses "
            "the configured Kuaidaili TPS order."
        ),
    )
    parser.add_argument("--pool-size", type=int, default=100)
    parser.add_argument("--timeout", type=float, default=8.0)
    if include_attempts:
        parser.add_argument("--attempts", type=int, default=20)
        parser.add_argument("--concurrency", type=int, default=4)
        parser.add_argument(
            "--show-attempts",
            action="store_true",
            help="Include redacted per-proxy diagnostics.",
        )


def add_batch_options(
    parser: argparse.ArgumentParser,
    *,
    default_concurrency: int,
    default_rps: float,
    default_max_items: int = 500,
) -> None:
    parser.add_argument(
        "input",
        nargs="?",
        default="-",
        help="Input file or '-' for stdin; accepts lines, JSONL, or a JSON array.",
    )
    parser.add_argument(
        "--batch-concurrency",
        type=int,
        default=default_concurrency,
        help=(
            "Number of independent items processed concurrently (1-16); "
            f"defaults to {default_concurrency}."
        ),
    )
    parser.add_argument(
        "--rps",
        type=float,
        default=default_rps,
        help=f"Global batch item start rate; defaults to {default_rps:g}/second.",
    )
    parser.add_argument(
        "--max-items",
        type=int,
        default=default_max_items,
        help=(
            "Maximum input items accepted in this run (1-2000); "
            f"defaults to {default_max_items}."
        ),
    )


def add_cache_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Bypass content cache reads and writes; the durable index is unchanged.",
    )
    parser.add_argument(
        "--cache-max-mb",
        type=float,
        default=CACHE_MAX_BYTES_DEFAULT / (1024 * 1024),
        help=(
            "Compressed content-cache cap in MiB (1-64); "
            f"defaults to {CACHE_MAX_BYTES_DEFAULT // (1024 * 1024)}."
        ),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="sogou-weixin",
        description="Search and fetch public WeChat articles with direct-first transport.",
    )
    parser.add_argument("--json", action="store_true", help="Emit stable JSON only.")
    parser.add_argument("--version", action="version", version=f"%(prog)s {VERSION}")
    commands = parser.add_subparsers(dest="command", required=True)

    search = commands.add_parser("search", help="Search public WeChat articles.")
    search.add_argument("query")
    search.add_argument("--page", type=int, default=1)
    search.add_argument("--limit", type=int, default=10)
    add_proxy_options(search, include_attempts=True)
    add_cache_options(search)

    batch_search = commands.add_parser(
        "batch-search", help="Search multiple queries with bounded async concurrency."
    )
    add_batch_options(batch_search, default_concurrency=4, default_rps=2.0)
    batch_search.add_argument("--page", type=int, default=1)
    batch_search.add_argument("--limit", type=int, default=10)
    add_proxy_options(batch_search, include_attempts=True)
    add_cache_options(batch_search)

    fetch = commands.add_parser("fetch", help="Fetch one article returned by search.")
    fetch.add_argument(
        "result",
        help="A search fetch_ref, result JSON object, Sogou link, WeChat URL, or '-' for stdin.",
    )
    fetch.add_argument("--format", choices=("text", "html", "both"), default="text")
    fetch.add_argument("--max-chars", type=int, default=200_000)
    fetch.add_argument(
        "--article-transport",
        choices=("auto", "direct", "proxy"),
        default="auto",
        help=(
            "How to fetch mp.weixin.qq.com after Sogou resolution. "
            "auto tries direct first and then Kuaidaili TPS."
        ),
    )
    add_proxy_options(fetch, include_attempts=True)
    add_cache_options(fetch)

    batch_fetch = commands.add_parser(
        "batch-fetch", help="Fetch multiple articles with bounded async concurrency."
    )
    add_batch_options(batch_fetch, default_concurrency=16, default_rps=8.0)
    batch_fetch.add_argument("--format", choices=("text", "html", "both"), default="text")
    batch_fetch.add_argument("--max-chars", type=int, default=200_000)
    batch_fetch.add_argument(
        "--article-transport",
        choices=("auto", "direct", "proxy"),
        default="auto",
        help=(
            "How to fetch mp.weixin.qq.com after Sogou resolution. "
            "auto tries direct first and then the shared Kuaidaili TPS fallback."
        ),
    )
    add_proxy_options(batch_fetch, include_attempts=True)
    add_cache_options(batch_fetch)

    doctor = commands.add_parser(
        "doctor", help="Check runtime, proxy source, and setup."
    )
    doctor.add_argument(
        "--offline",
        action="store_true",
        help="Check Kuaidaili credentials without resolving the live TPS endpoint.",
    )
    add_proxy_options(doctor, include_attempts=False)
    add_cache_options(doctor)

    cache_command = commands.add_parser(
        "cache", help="Inspect the compressed cache and durable article index."
    )
    cache_commands = cache_command.add_subparsers(
        dest="cache_command", required=True
    )
    cache_info = cache_commands.add_parser("info", help="Show cache/index size.")
    add_cache_options(cache_info)

    proxies = commands.add_parser("proxies", help="Discover proxy candidates.")
    proxy_commands = proxies.add_subparsers(dest="proxy_command", required=True)
    proxy_list = proxy_commands.add_parser(
        "list", help="List redacted proxy candidates."
    )
    proxy_list.add_argument("--limit", type=int, default=20)
    add_proxy_options(proxy_list, include_attempts=False)

    request = commands.add_parser(
        "request", help="Read-only raw Sogou request escape hatch."
    )
    request_commands = request.add_subparsers(dest="request_command", required=True)
    request_get = request_commands.add_parser(
        "get", help="GET a weixin.sogou.com path."
    )
    request_get.add_argument("path")
    request_get.add_argument(
        "--param",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Repeatable query parameter.",
    )
    request_get.add_argument("--max-bytes", type=int, default=20_000)
    add_proxy_options(request_get, include_attempts=True)
    return parser


def validate_positive(name: str, value: int | float) -> None:
    if value <= 0:
        raise CliError("invalid_input", f"{name} must be positive", EXIT_INVALID)


def validate_proxy_args(args: argparse.Namespace, *, include_attempts: bool) -> None:
    validate_positive("pool-size", args.pool_size)
    validate_positive("timeout", args.timeout)
    if include_attempts:
        validate_positive("attempts", args.attempts)
        if not 1 <= args.concurrency <= 16:
            raise CliError(
                "invalid_input", "concurrency must be between 1 and 16", EXIT_INVALID
            )


def validate_batch_args(args: argparse.Namespace) -> None:
    if not 1 <= args.batch_concurrency <= 16:
        raise CliError(
            "invalid_input",
            "batch-concurrency must be between 1 and 16",
            EXIT_INVALID,
        )
    validate_positive("rps", args.rps)
    if not 1 <= args.max_items <= 2_000:
        raise CliError(
            "invalid_input", "max-items must be between 1 and 2000", EXIT_INVALID
        )


def read_batch_values(input_value: str, max_items: int) -> list[str]:
    try:
        raw = sys.stdin.read() if input_value == "-" else Path(input_value).read_text(
            encoding="utf-8"
        )
    except OSError as exc:
        raise CliError(
            "invalid_input",
            f"Could not read batch input: {type(exc).__name__}",
            EXIT_INVALID,
        ) from exc
    raw = raw.strip()
    if not raw:
        raise CliError("invalid_input", "Batch input is empty", EXIT_INVALID)

    values: list[Any]
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        lines = [
            line.strip()
            for line in raw.splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        ]
        values = []
        for line in lines:
            try:
                values.append(json.loads(line))
            except json.JSONDecodeError:
                values.append(line)
    else:
        if isinstance(parsed, list):
            values = parsed
        elif isinstance(parsed, dict):
            data = parsed.get("data", {})
            items = data.get("items") if isinstance(data, dict) else None
            articles = data.get("articles") if isinstance(data, dict) else None
            legacy_results = data.get("results") if isinstance(data, dict) else None
            if isinstance(items, list):
                values = []
                for item in items:
                    nested = (
                        item.get("articles")
                        if isinstance(item, dict) and item.get("ok")
                        else None
                    )
                    if isinstance(nested, list):
                        values.extend(nested)
            elif isinstance(articles, list):
                values = articles
            elif isinstance(legacy_results, list) and any(
                isinstance(item, dict) and "ok" in item and "data" in item
                for item in legacy_results
            ):
                values = []
                for item in legacy_results:
                    nested = item.get("data", {}).get("results") if item.get("ok") else None
                    if isinstance(nested, list):
                        values.extend(nested)
            else:
                values = legacy_results if isinstance(legacy_results, list) else [parsed]
        else:
            values = [parsed]

    serialized = [
        value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
        for value in values
    ]
    serialized = [value.strip() for value in serialized if value.strip()]
    if not serialized:
        raise CliError("invalid_input", "Batch input has no items", EXIT_INVALID)
    if len(serialized) > max_items:
        raise CliError(
            "batch_too_large",
            f"Batch has {len(serialized)} items; max-items is {max_items}",
            EXIT_INVALID,
        )
    return serialized


def rotate_candidates(proxies: list[str], offset: int) -> list[str]:
    if not proxies:
        return proxies
    start = offset % len(proxies)
    return proxies[start:] + proxies[:start]


def fallback_proxy_candidates(
    proxies: list[str], source: str, attempts: int
) -> list[str]:
    if source == "kuaidaili-tps" and proxies:
        return [proxies[0]] * min(attempts, 3)
    return list(iter_candidates(proxies, attempts))


def normalize_title(value: str | None) -> str | None:
    if value is None:
        return None
    decoded = html.unescape(value).replace("\u200b", "")
    return "".join(character.casefold() for character in decoded if character.isalnum())


def cache_for_args(args: argparse.Namespace) -> CompressedCache:
    max_mb = float(
        getattr(
            args,
            "cache_max_mb",
            CACHE_MAX_BYTES_DEFAULT / (1024 * 1024),
        )
    )
    if not 1 <= max_mb <= 64:
        raise CliError(
            "invalid_input", "cache-max-mb must be between 1 and 64", EXIT_INVALID
        )
    return CompressedCache(CACHE_PATH_DEFAULT, int(max_mb * 1024 * 1024))


def search_cache_key(query: str, page: int) -> dict[str, Any]:
    return {"query": query, "page": page}


def fetch_cache_key(source: dict[str, Any]) -> dict[str, Any]:
    if source.get("kind") == "search":
        return {
            "kind": "search",
            "query": source.get("query"),
            "page": source.get("page"),
            "title": source.get("title"),
            "account": source.get("account"),
        }
    return {"kind": "url", "url": source.get("url")}


def get_proxies(args: argparse.Namespace) -> tuple[list[str], str]:
    try:
        if args.proxy_file:
            proxies = load_proxy_file(args.proxy_file)
            source = "file"
        else:
            proxies = fetch_kuaidaili_tps(source_timeout=min(args.timeout, 20))
            source = "kuaidaili-tps"
    except (OSError, ValueError, requests.RequestException, KuaidailiError) as exc:
        raise CliError(
            "kuaidaili_unavailable",
            "Could not resolve the Kuaidaili TPS fallback",
            EXIT_NETWORK,
            (
                {
                    "stage": getattr(exc, "stage", None),
                    "code": getattr(exc, "code", None),
                }
                if isinstance(exc, KuaidailiError)
                else None
            ),
        ) from exc
    if not proxies:
        raise CliError(
            "no_proxies", "The proxy source returned no candidates", EXIT_NO_PROXY
        )
    return proxies, source


def command_doctor(args: argparse.Namespace) -> dict[str, Any]:
    dependency_versions: dict[str, str] = {}
    for package in ("requests", "beautifulsoup4", "lxml"):
        try:
            dependency_versions[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            dependency_versions[package] = "missing"

    checks: list[dict[str, Any]] = [
        {
            "name": "python",
            "ok": sys.version_info >= (3, 11),
            "detail": platform.python_version(),
        },
        {
            "name": "dependencies",
            "ok": all(value != "missing" for value in dependency_versions.values()),
            "detail": dependency_versions,
        },
    ]
    proxy_source = "file" if args.proxy_file else "kuaidaili-tps"
    if args.proxy_file:
        checks.append(
            {
                "name": "proxy_file",
                "ok": args.proxy_file.is_file(),
                "detail": str(args.proxy_file),
            }
        )
    elif args.offline:
        try:
            kuaidaili_credentials()
            credential_state = "credentials configured; live resolution skipped"
            credential_ok = True
        except KuaidailiError:
            credential_state = "credentials missing; live resolution skipped"
            credential_ok = False
        checks.append(
            {
                "name": "kuaidaili_tps",
                "ok": credential_ok,
                "detail": credential_state,
            }
        )
    else:
        try:
            candidates = fetch_kuaidaili_tps(source_timeout=min(args.timeout, 15))
            checks.append(
                {
                    "name": "kuaidaili_tps",
                    "ok": bool(candidates),
                    "detail": "active TPS fallback resolved",
                }
            )
        except (requests.RequestException, KuaidailiError) as exc:
            checks.append(
                {
                    "name": "kuaidaili_tps",
                    "ok": False,
                    "detail": getattr(exc, "stage", type(exc).__name__),
                }
            )

    cache = cache_for_args(args)
    cache_stats = cache.stats()
    checks.append(
        {
            "name": "cache",
            "ok": True,
            "detail": {
                "path": str(cache.path),
                "max_bytes": cache.max_bytes,
                **cache_stats,
            },
        }
    )

    ready = all(check["ok"] is not False for check in checks)
    return {
        "ready": ready,
        "version": VERSION,
        "auth": {
            "direct_required": False,
            "fallback": "KDL_SECRET_ID/KDL_SECRET_KEY",
        },
        "transport": "direct-first",
        "fallback_source": proxy_source,
        "checks": checks,
    }


def command_proxies_list(args: argparse.Namespace) -> dict[str, Any]:
    validate_proxy_args(args, include_attempts=False)
    if not 1 <= args.limit <= 2_000:
        raise CliError(
            "invalid_input", "limit must be between 1 and 2000", EXIT_INVALID
        )
    proxies, source = get_proxies(args)
    selected = proxies[: args.limit]
    return {
        "source": source,
        "count": len(selected),
        "available": len(proxies),
        "proxies": [redact_proxy(proxy) for proxy in selected],
    }


def command_cache_info(args: argparse.Namespace) -> dict[str, Any]:
    cache = cache_for_args(args)
    return {
        "path": str(cache.path),
        "index_retention": "permanent",
        "index_when_full": "keep_existing_stop_new",
        "content_retention": "compressed_lru_no_ttl",
        **cache.stats(),
    }


def make_fetch_ref(
    query: str, page: int, title: str | None, account: str | None
) -> str:
    payload = json.dumps(
        {
            "v": 1,
            "query": query,
            "page": page,
            "title": title,
            "account": account,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    token = base64.urlsafe_b64encode(payload).decode().rstrip("=")
    return f"swx1_{token}"


def decode_fetch_ref(value: str) -> dict[str, Any]:
    if not value.startswith("swx1_"):
        raise CliError("invalid_fetch_ref", "Unsupported fetch reference", EXIT_INVALID)
    token = value.removeprefix("swx1_")
    try:
        padded = token + "=" * (-len(token) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded).decode())
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CliError(
            "invalid_fetch_ref", "Malformed fetch reference", EXIT_INVALID
        ) from exc
    required = ("query", "page", "title")
    if payload.get("v") != 1 or any(key not in payload for key in required):
        raise CliError("invalid_fetch_ref", "Incomplete fetch reference", EXIT_INVALID)
    return {"kind": "search", **payload}


def parse_fetch_input(value: str) -> dict[str, Any]:
    raw = sys.stdin.read() if value == "-" else value
    raw = raw.strip()
    if raw.startswith("{"):
        try:
            item = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise CliError(
                "invalid_input", "Fetch input is not valid JSON", EXIT_INVALID
            ) from exc
        if isinstance(item, dict) and isinstance(item.get("fetch_ref"), str):
            return decode_fetch_ref(item["fetch_ref"])
        data = item.get("data", {}) if isinstance(item, dict) else {}
        results = data.get("articles")
        if not isinstance(results, list):
            results = data.get("results")
        if (
            isinstance(results, list)
            and len(results) == 1
            and isinstance(results[0], dict)
        ):
            fetch_ref = results[0].get("fetch_ref")
            if isinstance(fetch_ref, str):
                return decode_fetch_ref(fetch_ref)
        raise CliError(
            "invalid_input",
            "JSON input must be one search result or an envelope containing exactly one article",
            EXIT_INVALID,
        )
    if raw.startswith("swx1_"):
        return decode_fetch_ref(raw)
    parsed = urlparse(raw)
    if parsed.scheme == "https" and (
        parsed.hostname == "mp.weixin.qq.com"
        or (parsed.hostname == "weixin.sogou.com" and parsed.path == "/link")
    ):
        return {"kind": "url", "url": raw}
    raise CliError(
        "invalid_input",
        "fetch expects a fetch_ref, result JSON, Sogou /link URL, or mp.weixin.qq.com URL",
        EXIT_INVALID,
    )


def command_search(
    args: argparse.Namespace,
    proxy_provider: SharedProxyProvider | None = None,
    proxy_offset: int = 0,
    cache: CompressedCache | None = None,
) -> dict[str, Any]:
    validate_proxy_args(args, include_attempts=True)
    validate_positive("page", args.page)
    if not 1 <= args.limit <= 10:
        raise CliError("invalid_input", "limit must be between 1 and 10", EXIT_INVALID)
    if cache is None and not args.no_cache:
        cache = cache_for_args(args)
    cached = None
    if cache is not None:
        cached = cache.get(
            "search",
            search_cache_key(args.query, args.page),
            CACHE_SEARCH_TTL_SECONDS,
        )
    attempts: list[Attempt] = []
    transport = "cache"
    winner: Attempt | None = None
    if isinstance(cached, list):
        articles = [Article(**item) for item in cached]
        winner = Attempt(proxy="cache", ok=True, elapsed_seconds=0.0)
    else:
        articles, direct_attempt = search_once(
            "direct", args.query, args.page, args.timeout
        )
        attempts.append(direct_attempt)
        winner = direct_attempt if direct_attempt.ok else None
        transport = "direct"
        if winner is None:
            proxies, transport = (
                proxy_provider.get()
                if proxy_provider is not None
                else get_proxies(args)
            )
            proxies = rotate_candidates(proxies, proxy_offset)
            articles, winner, proxy_attempts = search_parallel(
                proxies=fallback_proxy_candidates(
                    proxies, transport, args.attempts
                ),
                keyword=args.query,
                page=args.page,
                timeout=args.timeout,
                concurrency=min(args.concurrency, args.attempts),
            )
            attempts.extend(proxy_attempts)
    if not winner:
        raise CliError(
            "search_failed",
            "Direct search and Kuaidaili fallback both failed",
            EXIT_NO_PROXY,
            (
                {"attempts": [asdict(attempt) for attempt in attempts]}
                if args.show_attempts
                else None
            ),
        )
    if transport != "cache" and cache is not None:
        cache.put(
            "search",
            search_cache_key(args.query, args.page),
            [asdict(article) for article in articles],
        )
    indexed_results: list[dict[str, Any]] = []
    for article in articles:
        indexed_results.append(
            {
                "title": article.title,
                "summary": article.summary,
                "account": article.account,
                "published_at_unix": article.published_at_unix,
                "fetch_ref": make_fetch_ref(
                    args.query, args.page, article.title, article.account
                ),
                "image": article.image,
            }
        )
    results = indexed_results[: args.limit]
    data: dict[str, Any] = {
        "articles": results,
        "query": args.query,
        "page": args.page,
        "count": len(results),
        "next_page": args.page + 1 if len(articles) == 10 else None,
        "retrieval": {"transport": transport},
    }
    if cache is not None:
        cache.index_search(args.query, args.page, indexed_results)
    if args.show_attempts:
        data["diagnostics"] = {
            "proxy": winner.proxy,
            "attempts": [asdict(attempt) for attempt in attempts],
        }
    return data


def command_fetch(
    args: argparse.Namespace,
    proxy_provider: SharedProxyProvider | None = None,
    proxy_offset: int = 0,
    cache: CompressedCache | None = None,
) -> dict[str, Any]:
    validate_proxy_args(args, include_attempts=True)
    if not 1 <= args.max_chars <= 2_000_000:
        raise CliError(
            "invalid_input", "max-chars must be between 1 and 2000000", EXIT_INVALID
        )
    source = parse_fetch_input(args.result)
    if cache is None and not args.no_cache:
        cache = cache_for_args(args)
    article = None
    winner = None
    attempts: list[Attempt] = []
    retrieval_transport = "cache"

    cached = None
    if cache is not None:
        cached = cache.get("article", fetch_cache_key(source), None)
    if isinstance(cached, dict):
        article = FetchedArticle(**cached)
        winner = Attempt(proxy="cache", ok=True, elapsed_seconds=0.0)

    source_url = source.get("url") if source.get("kind") == "url" else None
    is_direct_wechat_url = (
        isinstance(source_url, str)
        and urlparse(source_url).hostname == "mp.weixin.qq.com"
    )
    if article is None and args.article_transport in ("auto", "direct"):
        retrieval_transport = "direct"
        if is_direct_wechat_url:
            article, winner = fetch_direct_once(source_url, args.timeout)
        else:
            article, winner = fetch_once(
                "direct", source, args.timeout, article_transport="direct"
            )
        attempts.append(winner)
        terminal_direct_errors = {
            "WeChatUnavailableError": "article_unavailable",
            "WeChatContentError": "article_content_missing",
            "WeChatCaptchaError": "article_captcha",
        }
        terminal_error = next(
            (
                (marker, code)
                for marker, code in terminal_direct_errors.items()
                if article is None
                and winner.reason
                and marker in winner.reason
            ),
            None,
        )
        if terminal_error is not None:
            _, code = terminal_error
            raise CliError(
                code,
                winner.reason.rsplit(":", 1)[-1].strip(),
                EXIT_UNAVAILABLE,
                (
                    {"attempts": [asdict(attempt) for attempt in attempts]}
                    if args.show_attempts
                    else None
                ),
            )
        if article is None and args.article_transport == "direct":
            raise CliError(
                "fetch_failed",
                "Direct article fetch failed",
                EXIT_NETWORK,
                (
                    {"attempts": [asdict(attempt) for attempt in attempts]}
                    if args.show_attempts
                    else None
                ),
            )

    if article is None:
        proxies, retrieval_transport = (
            proxy_provider.get() if proxy_provider is not None else get_proxies(args)
        )
        proxies = rotate_candidates(proxies, proxy_offset)
        fallback_transport = (
            "proxy" if is_direct_wechat_url else args.article_transport
        )
        article, winner, proxy_attempts = fetch_parallel(
            proxies=fallback_proxy_candidates(
                proxies, retrieval_transport, args.attempts
            ),
            source=source,
            timeout=args.timeout,
            concurrency=min(args.concurrency, args.attempts),
            article_transport=fallback_transport,
        )
        attempts.extend(proxy_attempts)
    if article is None or winner is None:
        raise CliError(
            "fetch_failed",
            "No proxy completed the article fetch",
            EXIT_NO_PROXY,
            (
                {"attempts": [asdict(attempt) for attempt in attempts]}
                if args.show_attempts
                else None
            ),
        )

    if retrieval_transport != "cache" and cache is not None:
        cache.put("article", fetch_cache_key(source), asdict(article))

    text_original_chars = len(article.body_text)
    html_original_chars = len(article.body_html)
    body: dict[str, Any] = {}
    truncated = False
    if args.format in ("text", "both"):
        body["text"] = article.body_text[: args.max_chars]
        truncated |= text_original_chars > args.max_chars
    if args.format in ("html", "both"):
        body["html"] = article.body_html[: args.max_chars]
        truncated |= html_original_chars > args.max_chars

    body.update(
        {
            "type": article.content_quality,
            "returned_format": args.format,
            "truncated": truncated,
            "text_chars": text_original_chars,
            "html_chars": html_original_chars,
            "images": article.image_count,
            "links": article.link_count,
            "embedded_media": article.embedded_media_count,
        }
    )

    expected_title = source.get("title")
    title_match = (
        normalize_title(article.title) == normalize_title(expected_title)
        if expected_title not in (None, "") and article.title is not None
        else None
    )

    data: dict[str, Any] = {
        "article": {
            "title": article.title,
            "body": body,
            "title_consistency": {
                "expected": expected_title,
                "matches": title_match,
            },
            "account": article.account,
            "published_at": article.published_at,
            "url": article.resolved_url,
            "cover_image": article.cover_image,
        },
        "retrieval": {
            "transport": retrieval_transport,
            "article_transport": article.transport,
            "via_sogou": source.get("kind") == "search",
        },
    }
    if cache is not None:
        cache.index_fetch(
            source,
            {
                "title": article.title,
                "account": article.account,
                "published_at": article.published_at,
                "url": article.resolved_url,
                "body": {
                    "text": article.body_text,
                    "html": article.body_html,
                    "type": article.content_quality,
                    "text_chars": text_original_chars,
                    "html_chars": html_original_chars,
                },
            },
        )
    if args.show_attempts:
        data["retrieval"]["diagnostics"] = {
            "proxy": winner.proxy,
            "attempts": [asdict(attempt) for attempt in attempts],
        }
    return data


def parse_batch_search_item(
    raw: str, defaults: argparse.Namespace
) -> argparse.Namespace:
    query: Any = raw
    page: Any = defaults.page
    limit: Any = defaults.limit
    if raw.startswith("{"):
        try:
            item = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise CliError(
                "invalid_input", "Batch search item is not valid JSON", EXIT_INVALID
            ) from exc
        if not isinstance(item, dict):
            raise CliError(
                "invalid_input", "Batch search JSON item must be an object", EXIT_INVALID
            )
        query = item.get("query")
        page = item.get("page", page)
        limit = item.get("limit", limit)
    if not isinstance(query, str) or not query.strip():
        raise CliError(
            "invalid_input", "Batch search query must be a non-empty string", EXIT_INVALID
        )
    if not isinstance(page, int) or not isinstance(limit, int):
        raise CliError(
            "invalid_input", "Batch search page and limit must be integers", EXIT_INVALID
        )
    item_args = argparse.Namespace(**vars(defaults))
    item_args.query = query.strip()
    item_args.page = page
    item_args.limit = limit
    return item_args


def batch_input_label(raw: str) -> str:
    compact = re.sub(r"\s+", " ", raw).strip()
    return compact if len(compact) <= 500 else compact[:497] + "..."


async def run_batch_items(
    values: list[str],
    args: argparse.Namespace,
    handler: Any,
) -> dict[str, Any]:
    semaphore = asyncio.Semaphore(args.batch_concurrency)
    limiter = AsyncStartRateLimiter(args.rps)
    batch_started = time.monotonic()

    async def run_one(index: int, raw: str) -> dict[str, Any]:
        async with semaphore:
            await limiter.wait()
            started = time.monotonic()
            try:
                data = await asyncio.to_thread(handler, index, raw)
                item = {
                    "index": index,
                    "ok": True,
                    **data,
                }
                if args.show_attempts:
                    item["elapsed_seconds"] = round(time.monotonic() - started, 3)
                return item
            except CliError as exc:
                error: dict[str, Any] = {
                    "code": exc.code,
                    "message": exc.message,
                }
                if exc.details:
                    error["details"] = exc.details
                item = {
                    "index": index,
                    "input": batch_input_label(raw),
                    "ok": False,
                    "error": error,
                }
                if args.show_attempts:
                    item["elapsed_seconds"] = round(time.monotonic() - started, 3)
                return item
            except Exception as exc:
                item = {
                    "index": index,
                    "input": batch_input_label(raw),
                    "ok": False,
                    "error": {
                        "code": "internal_error",
                        "message": f"Unexpected batch item failure: {type(exc).__name__}",
                    },
                }
                if args.show_attempts:
                    item["elapsed_seconds"] = round(time.monotonic() - started, 3)
                return item

    results = await asyncio.gather(
        *(run_one(index, raw) for index, raw in enumerate(values))
    )
    succeeded = sum(item["ok"] is True for item in results)
    summary: dict[str, Any] = {
        "requested": len(results),
        "succeeded": succeeded,
        "failed": len(results) - succeeded,
    }
    if args.show_attempts:
        summary["diagnostics"] = {
            "batch_concurrency": args.batch_concurrency,
            "rps": args.rps,
            "elapsed_seconds": round(time.monotonic() - batch_started, 3),
        }
    return {
        "items": results,
        "summary": summary,
    }


def command_batch_search(args: argparse.Namespace) -> dict[str, Any]:
    validate_proxy_args(args, include_attempts=True)
    validate_batch_args(args)
    validate_positive("page", args.page)
    if not 1 <= args.limit <= 10:
        raise CliError("invalid_input", "limit must be between 1 and 10", EXIT_INVALID)
    values = read_batch_values(args.input, args.max_items)
    provider = SharedProxyProvider(args)
    cache = cache_for_args(args)

    def handle(index: int, raw: str) -> dict[str, Any]:
        item_args = parse_batch_search_item(raw, args)
        return command_search(
            item_args, provider, proxy_offset=index, cache=cache
        )

    return asyncio.run(run_batch_items(values, args, handle))


def command_batch_fetch(args: argparse.Namespace) -> dict[str, Any]:
    validate_proxy_args(args, include_attempts=True)
    validate_batch_args(args)
    if not 1 <= args.max_chars <= 2_000_000:
        raise CliError(
            "invalid_input", "max-chars must be between 1 and 2000000", EXIT_INVALID
        )
    values = read_batch_values(args.input, args.max_items)
    provider = SharedProxyProvider(args)
    cache = cache_for_args(args)

    def handle(index: int, raw: str) -> dict[str, Any]:
        item_args = argparse.Namespace(**vars(args))
        item_args.result = raw
        return command_fetch(
            item_args, provider, proxy_offset=index, cache=cache
        )

    return asyncio.run(run_batch_items(values, args, handle))


def parse_params(values: Sequence[str]) -> dict[str, str]:
    params: dict[str, str] = {}
    for value in values:
        pairs = parse_qsl(value, keep_blank_values=True)
        if len(pairs) != 1 or "=" not in value:
            raise CliError(
                "invalid_input", f"Invalid --param value: {value!r}", EXIT_INVALID
            )
        key, item = pairs[0]
        params[key] = item
    return params


def raw_get_once(
    proxy: str,
    url: str,
    params: dict[str, str],
    timeout: float,
    max_bytes: int,
) -> tuple[dict[str, Any] | None, Attempt]:
    started = time.monotonic()
    label = redact_proxy(proxy)
    try:
        with proxy_session(proxy) as session:
            response = session.get(
                url,
                params=params,
                headers={"Referer": SOGOU_HOME},
                timeout=timeout,
            )
            response.raise_for_status()
            encoded = response.content[:max_bytes]
            data = {
                "status": response.status_code,
                "url": response.url,
                "content_type": response.headers.get("content-type"),
                "truncated": len(response.content) > max_bytes,
                "body": encoded.decode(response.encoding or "utf-8", errors="replace"),
            }
            return data, Attempt(
                proxy=label,
                ok=True,
                status=response.status_code,
                elapsed_seconds=round(time.monotonic() - started, 3),
            )
    except Exception as exc:
        return None, Attempt(
            proxy=label,
            ok=False,
            elapsed_seconds=round(time.monotonic() - started, 3),
            reason=redact_error(exc, proxy),
        )


def command_request_get(args: argparse.Namespace) -> dict[str, Any]:
    validate_proxy_args(args, include_attempts=True)
    validate_positive("max-bytes", args.max_bytes)
    url = urljoin(SOGOU_HOME, args.path)
    parsed = urlparse(url)
    if parsed.scheme != "https" or parsed.hostname != "weixin.sogou.com":
        raise CliError(
            "invalid_target",
            "Raw GET is restricted to https://weixin.sogou.com",
            EXIT_INVALID,
        )
    params = parse_params(args.param)
    proxies, source = get_proxies(args)
    candidates = list(iter_candidates(proxies, args.attempts))
    diagnostics: list[Attempt] = []
    for start in range(0, len(candidates), args.concurrency):
        batch = candidates[start : start + args.concurrency]
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(batch)) as executor:
            futures = [
                executor.submit(
                    raw_get_once, proxy, url, params, args.timeout, args.max_bytes
                )
                for proxy in batch
            ]
            successes: list[tuple[dict[str, Any], Attempt]] = []
            for future in concurrent.futures.as_completed(futures):
                result, attempt = future.result()
                diagnostics.append(attempt)
                if result is not None:
                    successes.append((result, attempt))
        if successes:
            result, winner = min(successes, key=lambda item: item[1].elapsed_seconds)
            result.update({"proxy_source": source, "proxy": winner.proxy})
            if args.show_attempts:
                result["attempts"] = [asdict(item) for item in diagnostics]
            return result
    raise CliError(
        "no_working_proxy",
        "No proxy completed the raw GET",
        EXIT_NO_PROXY,
        {"attempts": [asdict(item) for item in diagnostics]},
    )


def render_human(command: str, data: dict[str, Any]) -> None:
    if command == "search":
        print(f"{data['count']} articles for {data['query']!r} (page {data['page']})")
        for index, result in enumerate(data["articles"], 1):
            print(f"{index:2}. [{result['account'] or '-'}] {result['title'] or '-'}")
            print(f"    fetch: sogou-weixin fetch {result['fetch_ref']}")
    elif command == "fetch":
        article = data["article"]
        print(article["title"] or "Untitled")
        print(f"Account: {article['account'] or '-'}")
        print(f"URL: {article['url']}")
        body = article["body"]
        if body["returned_format"] == "html":
            print(body["html"])
        elif body["returned_format"] == "both":
            print(body["text"])
            print("\n--- HTML ---\n")
            print(body["html"])
        else:
            print(body["text"])
    elif command in ("batch-search", "batch-fetch"):
        summary = data["summary"]
        print(
            f"{command}: {summary['succeeded']}/{summary['requested']} succeeded"
        )
        for item in data["items"]:
            if item["ok"]:
                if command == "batch-search":
                    detail = f"{item['count']} result(s) for {item['query']!r}"
                else:
                    detail = item["article"]["title"] or "Untitled"
                print(f"  OK   [{item['index']}] {detail}")
            else:
                print(
                    f"  FAIL [{item['index']}] {item['error']['code']}: "
                    f"{item['error']['message']}"
                )
    elif command == "doctor":
        print(
            f"sogou-weixin {data['version']}: {'ready' if data['ready'] else 'not ready'}"
        )
        for check in data["checks"]:
            marker = (
                "OK" if check["ok"] else ("SKIP" if check["ok"] is None else "FAIL")
            )
            print(f"  {marker:4} {check['name']}: {check['detail']}")
    elif command == "cache info":
        print(f"Index: {data['indexed_articles']} article(s), permanent")
        print(
            f"Content cache: {data['entries']} item(s), "
            f"{data['compressed_bytes']} compressed bytes"
        )
        print(f"File: {data['path']} ({data['file_bytes']} bytes)")
    elif command == "proxies list":
        print(f"{data['count']} proxy candidates from {data['source']}")
        for proxy in data["proxies"]:
            print(proxy)
    elif command == "request get":
        print(data["body"])


def dispatch(args: argparse.Namespace) -> tuple[str, dict[str, Any]]:
    if args.command == "search":
        return "search", command_search(args)
    if args.command == "batch-search":
        return "batch-search", command_batch_search(args)
    if args.command == "fetch":
        return "fetch", command_fetch(args)
    if args.command == "batch-fetch":
        return "batch-fetch", command_batch_fetch(args)
    if args.command == "doctor":
        return "doctor", command_doctor(args)
    if args.command == "cache" and args.cache_command == "info":
        return "cache info", command_cache_info(args)
    if args.command == "proxies" and args.proxy_command == "list":
        return "proxies list", command_proxies_list(args)
    if args.command == "request" and args.request_command == "get":
        return "request get", command_request_get(args)
    raise CliError("invalid_command", "Unsupported command", EXIT_INVALID)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        command, data = dispatch(args)
        if args.json:
            emit_json(envelope(command, data))
        else:
            render_human(command, data)
        if command == "doctor" and not data["ready"]:
            return EXIT_NETWORK
        return 0
    except CliError as exc:
        if args.json:
            emit_json(error_envelope(exc))
        else:
            print(f"error [{exc.code}]: {exc.message}", file=sys.stderr)
        return exc.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
