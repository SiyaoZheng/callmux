#!/usr/bin/env python3

"""Full-stack FastMCP server for public Sogou Weixin research."""

from __future__ import annotations

import asyncio
import json
import re
from argparse import Namespace
from datetime import datetime
from typing import Annotated, Any, Literal
from zoneinfo import ZoneInfo

from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from fastmcp.tools import ToolResult
from mcp.types import ToolAnnotations
from pydantic import Field

import sogou_weixin_backend as backend

SHANGHAI = ZoneInfo("Asia/Shanghai")

mcp = FastMCP(
    "sogou-weixin",
    version="1.0.0",
    instructions=(
        "Search and fetch public WeChat articles in-process. Results expose "
        "normalized fields for agents and raw_backend for exact-result caching."
    ),
)
READ_ONLY = ToolAnnotations(
    readOnlyHint=True,
    destructiveHint=False,
    idempotentHint=True,
    openWorldHint=True,
)


def _parse_args(command: str, *arguments: str) -> Namespace:
    try:
        return backend.build_parser().parse_args([command, *arguments])
    except SystemExit as exc:
        raise ToolError(f"Invalid {command} arguments") from exc


def _invoke(command: str, function: Any, args: Namespace) -> dict[str, Any]:
    try:
        return backend.envelope(command, function(args))
    except backend.CliError as exc:
        raise ToolError(f"{exc.code}: {exc.message}") from exc


def _search_backend(query: str, page: int, limit: int) -> dict[str, Any]:
    args = _parse_args(
        "search",
        query,
        "--page",
        str(page),
        "--limit",
        str(limit),
        "--no-cache",
    )
    return _invoke("search", backend.command_search, args)


def _fetch_backend(result: str, format: str, max_chars: int) -> dict[str, Any]:
    args = _parse_args(
        "fetch",
        result,
        "--format",
        format,
        "--max-chars",
        str(max_chars),
        "--no-cache",
    )
    return _invoke("fetch", backend.command_fetch, args)


def _batch_search_backend(
    queries: list[str], page: int, limit: int
) -> dict[str, Any]:
    args = _parse_args(
        "batch-search",
        "-",
        "--page",
        str(page),
        "--limit",
        str(limit),
        "--max-items",
        str(len(queries)),
        "--no-cache",
    )
    provider = backend.SharedProxyProvider(args)
    values = [
        json.dumps({"query": query, "page": page, "limit": limit}, ensure_ascii=False)
        for query in queries
    ]

    def handle(index: int, raw: str) -> dict[str, Any]:
        item_args = backend.parse_batch_search_item(raw, args)
        return backend.command_search(
            item_args,
            provider,
            proxy_offset=index,
        )

    return backend.envelope(
        "batch-search",
        asyncio.run(backend.run_batch_items(values, args, handle)),
    )


def _batch_fetch_backend(
    candidates: list[dict[str, Any]], max_chars: int
) -> dict[str, Any]:
    args = _parse_args(
        "batch-fetch",
        "-",
        "--format",
        "text",
        "--max-chars",
        str(max_chars),
        "--max-items",
        str(len(candidates)),
        "--no-cache",
    )
    provider = backend.SharedProxyProvider(args)
    values = [
        json.dumps(candidate["search"], ensure_ascii=False) for candidate in candidates
    ]

    def handle(index: int, raw: str) -> dict[str, Any]:
        item_args = Namespace(**vars(args))
        item_args.result = raw
        return backend.command_fetch(
            item_args,
            provider,
            proxy_offset=index,
        )

    return backend.envelope(
        "batch-fetch",
        asyncio.run(backend.run_batch_items(values, args, handle)),
    )


def _data(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data")
    if not isinstance(data, dict):
        raise ToolError("sogou-weixin returned no data object")
    return data


def _compact(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    return re.sub(r"\s+", " ", value).strip() or None


def _preview(value: Any, chars: int) -> str | None:
    text = _compact(value)
    if text is None or len(text) <= chars:
        return text
    return text[:chars].rstrip() + "…"


def _published(value: Any) -> str | None:
    if not isinstance(value, (int, float)):
        return _compact(value)
    timestamp = float(value) / (1000 if value > 10_000_000_000 else 1)
    try:
        return datetime.fromtimestamp(timestamp, SHANGHAI).isoformat()
    except (OverflowError, OSError, ValueError):
        return None


def _search_articles(data: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "rank": rank,
            "title": _compact(article.get("title")),
            "account": _compact(article.get("account")),
            "published_at": _published(article.get("published_at_unix")),
            "published_at_unix": article.get("published_at_unix"),
            "summary": _compact(article.get("summary")),
            "fetch_ref": article.get("fetch_ref"),
            "image": article.get("image"),
        }
        for rank, article in enumerate(data.get("articles", []), 1)
        if isinstance(article, dict)
    ]


def _fetched_article(data: dict[str, Any], preview_chars: int) -> dict[str, Any]:
    article = data.get("article")
    if not isinstance(article, dict):
        raise ToolError("sogou-weixin fetch returned no article")
    body = article.get("body") if isinstance(article.get("body"), dict) else {}
    consistency = (
        article.get("title_consistency")
        if isinstance(article.get("title_consistency"), dict)
        else {}
    )
    return {
        "title": _compact(article.get("title")),
        "account": _compact(article.get("account") or article.get("account_name")),
        "published_at": _published(article.get("published_at")),
        "url": article.get("url"),
        "cover_image": article.get("cover_image"),
        "text_preview": _preview(body.get("text"), preview_chars),
        "html_preview": _preview(body.get("html"), preview_chars),
        "body": {
            key: body.get(key)
            for key in (
                "type",
                "returned_format",
                "truncated",
                "text_chars",
                "html_chars",
                "images",
                "links",
                "embedded_media",
            )
        },
        "title_consistency": {
            "expected": consistency.get("expected"),
            "matches": consistency.get("matches"),
        },
    }


@mcp.tool(annotations=READ_ONLY)
def web_search_sogou(
    query: Annotated[str, Field(min_length=1, description="Search query")],
    page: Annotated[int, Field(ge=1, description="Results page")] = 1,
    limit: Annotated[int, Field(ge=1, le=10, description="Maximum results")] = 10,
) -> ToolResult:
    """Search WeChat articles and return normalized results with fetch_ref values."""

    raw = _search_backend(query, page, limit)
    data = _data(raw)
    result = {
        "ok": True,
        "query": data.get("query", query),
        "page": data.get("page", page),
        "count": data.get("count", 0),
        "next_page": data.get("next_page"),
        "retrieval": data.get("retrieval", {}),
        "articles": _search_articles(data),
        "raw_backend": raw,
    }
    text = "\n".join(
        [
            f"{article['rank']}. [{article['account'] or '-'}] "
            f"{article['title'] or '无标题'}\n"
            f"   时间: {article['published_at'] or '-'}\n"
            f"   摘要: {article['summary'] or '-'}\n"
            f"   fetch_ref: {article['fetch_ref'] or '-'}"
            for article in result["articles"]
        ]
    )
    return ToolResult(content=text or "没有结果", structured_content=result)


@mcp.tool(annotations=READ_ONLY)
def web_fetch_sogou(
    result: Annotated[
        str,
        Field(
            min_length=1,
            description="Search fetch_ref, result JSON, Sogou link, or WeChat URL",
        ),
    ],
    format: Literal["text", "html", "both"] = "text",
    max_chars: Annotated[int, Field(ge=1, le=2_000_000)] = 200_000,
    preview_chars: Annotated[int, Field(ge=1, le=20_000)] = 4_000,
) -> ToolResult:
    """Fetch one article with flat metadata, a compact preview, and exact raw JSON."""

    raw = _fetch_backend(result, format, max_chars)
    data = _data(raw)
    article = _fetched_article(data, preview_chars)
    structured = {
        "ok": True,
        "article": article,
        "retrieval": data.get("retrieval", {}),
        "raw_backend": raw,
    }
    text = "\n".join(
        [
            f"# {article['title'] or '无标题'}",
            f"公众号: {article['account'] or '-'}",
            f"时间: {article['published_at'] or '-'}",
            f"URL: {article['url'] or '-'}",
            "",
            article["text_preview"] or article["html_preview"] or "无正文",
        ]
    )
    return ToolResult(content=text, structured_content=structured)


@mcp.tool(annotations=READ_ONLY)
def batch_research_sogou(
    queries: Annotated[
        list[str],
        Field(
            min_length=1,
            max_length=20,
            description="Queries to search and then fetch",
        ),
    ],
    page: Annotated[int, Field(ge=1)] = 1,
    limit: Annotated[int, Field(ge=1, le=10)] = 5,
    preview_chars: Annotated[int, Field(ge=1, le=20_000)] = 1_200,
    max_chars: Annotated[int, Field(ge=1, le=2_000_000)] = 200_000,
) -> ToolResult:
    """Batch-search and fetch articles with bounded in-process concurrency."""

    queries = [query.strip() for query in queries]
    if any(not query for query in queries):
        raise ToolError("queries must contain only non-empty strings")

    search_raw = _batch_search_backend(queries, page, limit)
    search_data = _data(search_raw)
    candidates: list[dict[str, Any]] = []
    search_errors: list[dict[str, Any]] = []
    for item in search_data.get("items", []):
        if not isinstance(item, dict):
            continue
        if item.get("ok") is not True:
            search_errors.append(
                {"query": item.get("input"), "error": item.get("error")}
            )
            continue
        for article in item.get("articles", []):
            if isinstance(article, dict):
                candidates.append(
                    {
                        "query": item.get("query"),
                        "fetch_ref": article.get("fetch_ref"),
                        "search": article,
                    }
                )

    if not candidates:
        structured = {
            "ok": True,
            "summary": {
                "queries_requested": len(queries),
                "queries_succeeded": len(queries) - len(search_errors),
                "queries_failed": len(search_errors),
                "articles_requested": 0,
                "articles_succeeded": 0,
                "articles_failed": 0,
            },
            "items": [],
            "search_errors": search_errors,
            "raw_backend": {"batch_search": search_raw, "batch_fetch": None},
        }
        return ToolResult(
            content="批量搜索没有返回可抓取文章。",
            structured_content=structured,
        )

    fetch_raw = _batch_fetch_backend(candidates, max_chars)
    fetch_items = _data(fetch_raw).get("items", [])
    items: list[dict[str, Any]] = []
    for index, candidate in enumerate(candidates):
        fetched = fetch_items[index] if index < len(fetch_items) else {}
        if not isinstance(fetched, dict) or fetched.get("ok") is not True:
            search = candidate["search"]
            items.append(
                {
                    "index": index,
                    "query": candidate["query"],
                    "ok": False,
                    "title": _compact(search.get("title")),
                    "account": _compact(search.get("account")),
                    "fetch_ref": candidate["fetch_ref"],
                    "error": (
                        fetched.get("error")
                        if isinstance(fetched, dict)
                        else {"code": "missing_result"}
                    ),
                }
            )
            continue

        article = _fetched_article(fetched, preview_chars)
        search = candidate["search"]
        items.append(
            {
                "index": index,
                "query": candidate["query"],
                "ok": True,
                "title": article["title"] or _compact(search.get("title")),
                "account": article["account"] or _compact(search.get("account")),
                "published_at": article["published_at"]
                or _published(search.get("published_at_unix")),
                "fetch_ref": candidate["fetch_ref"],
                "url": article["url"],
                "text_preview": article["text_preview"],
                "body": article["body"],
                "error": None,
            }
        )

    succeeded = sum(item["ok"] is True for item in items)
    structured = {
        "ok": True,
        "summary": {
            "queries_requested": len(queries),
            "queries_succeeded": len(queries) - len(search_errors),
            "queries_failed": len(search_errors),
            "articles_requested": len(items),
            "articles_succeeded": succeeded,
            "articles_failed": len(items) - succeeded,
        },
        "items": items,
        "search_errors": search_errors,
        "raw_backend": {"batch_search": search_raw, "batch_fetch": fetch_raw},
    }
    text = "\n\n".join(
        [
            f"{item['index'] + 1}. [{item.get('account') or '-'}] "
            f"{item.get('title') or '无标题'}\n"
            f"查询: {item.get('query') or '-'}\n"
            f"时间: {item.get('published_at') or '-'}\n"
            f"URL: {item.get('url') or '-'}\n"
            + (
                f"正文预览: {item.get('text_preview') or '无正文'}"
                if item["ok"]
                else f"错误: {item.get('error')}"
            )
            for item in items
        ]
    )
    return ToolResult(content=text, structured_content=structured)


if __name__ == "__main__":
    mcp.run()
