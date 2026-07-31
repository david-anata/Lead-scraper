"""Joined production URL inventory for Website Ops."""

from __future__ import annotations

import hashlib
import re
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import urlparse, urlunparse


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _production_url(value: Any) -> str:
    raw = _clean(value)
    parsed = urlparse(raw)
    host = (parsed.hostname or "").lower().removeprefix("www.")
    if parsed.scheme not in {"http", "https"} or host != "anatainc.com":
        return ""
    path = re.sub(r"/+", "/", parsed.path or "/")
    if path != "/":
        path = path.rstrip("/")
    return urlunparse(("https", "anatainc.com", path, "", parsed.query, ""))


def _records(value: Any) -> list[dict[str, Any]]:
    return [dict(item) for item in (value or []) if isinstance(item, Mapping)]


def _record_url(record: Mapping[str, Any]) -> str:
    for key in ("url", "page_url", "address"):
        url = _production_url(record.get(key))
        if url:
            return url
    return ""


def _warning_names(record: Mapping[str, Any]) -> list[str]:
    return sorted(
        {
            _clean(item.get("report")).lower()
            for item in _records(record.get("warnings"))
            if _clean(item.get("report"))
        }
    )


def _content_signature(record: Mapping[str, Any]) -> str:
    parts = [
        _clean(record.get("title")).casefold(),
        _clean(record.get("meta_description")).casefold(),
        _clean(record.get("h1")).casefold(),
    ]
    if not any(parts):
        return ""
    return hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()[:20]


def _evidence_coverage(report_names: set[str]) -> dict[str, str]:
    groups = {
        "crawlability_indexability": ("internal_all", "directives_all", "nonindexable", "blocked"),
        "status_codes": ("response_codes_all", "client_error", "server_error", "no_response"),
        "redirects": ("redirect_chain", "redirect_loop", "redirects_to_error"),
        "canonicals": ("canonicals_all", "canonical"),
        "sitemaps": ("sitemaps_all", "sitemap"),
        "titles_descriptions": ("page_titles_all", "meta_description_all"),
        "heading_structure": ("h1_all", "h2_all"),
        "content_duplicates": ("content_all", "duplicate", "content_low"),
        "internal_links_orphans_depth": ("links_all", "inlinks", "orphan", "pages_without_internal"),
        "images": ("images_all", "missing_alt", "missing_size"),
        "pagination": ("pagination_all",),
        "javascript_rendering": ("javascript_issues",),
        "structured_data": ("structured_data_all", "jsonld", "schema"),
        "core_web_vitals": ("pagespeed_all", "layout_shift", "lcp_"),
        "search_console": ("search_console_all",),
    }
    return {
        key: (
            "available"
            if any(any(token in name for token in tokens) for name in report_names)
            else "missing_export"
        )
        for key, tokens in groups.items()
    }


def build_production_inventory(
    *,
    sitemap_urls: Iterable[str],
    crawl_inventory: Mapping[str, Any],
    indexing_inventory: Mapping[str, Any],
    intent_coverage: Mapping[str, Any],
) -> dict[str, Any]:
    """Join production evidence without treating crawler warnings as facts."""

    crawl_by_url = {
        url: item
        for item in _records(crawl_inventory.get("records"))
        if (url := _record_url(item))
    }
    indexing_by_url = {
        url: item
        for item in _records(indexing_inventory.get("records"))
        if (url := _record_url(item))
    }
    intent_by_url = {
        url: item
        for item in _records(intent_coverage.get("records"))
        if (url := _record_url(item))
    }
    sitemap = {url for value in sitemap_urls if (url := _production_url(value))}
    urls = sorted(sitemap | set(crawl_by_url) | set(indexing_by_url) | set(intent_by_url))
    report_names = {
        _clean(name).lower()
        for item in crawl_by_url.values()
        for name in item.get("source_reports", []) or []
        if _clean(name)
    }
    records: list[dict[str, Any]] = []
    for url in urls:
        crawl = crawl_by_url.get(url, {})
        indexing = indexing_by_url.get(url, {})
        intent = intent_by_url.get(url, {})
        warnings = _warning_names(crawl)
        status_code = int(crawl.get("status_code", 0) or 0)
        inlinks = crawl.get("inlinks")
        depth = crawl.get("crawl_depth")
        record = {
            "url": url,
            "in_sitemap": url in sitemap,
            "in_crawl": url in crawl_by_url,
            "in_indexing_export": url in indexing_by_url,
            "in_intent_map": url in intent_by_url,
            "primary_intent": _clean(intent.get("primary_intent")),
            "intent_type": _clean(intent.get("intent_type")),
            "intent_coverage_status": _clean(intent.get("coverage_status")) or "unknown",
            "status_code": status_code or None,
            "indexability": _clean(crawl.get("indexability")),
            "indexing_reason": _clean(indexing.get("reason") or indexing.get("indexing_reason")),
            "canonical": _clean(crawl.get("canonical")),
            "crawl_depth": depth,
            "inlinks": inlinks,
            "word_count": crawl.get("word_count"),
            "title": _clean(crawl.get("title")),
            "meta_description": _clean(crawl.get("meta_description")),
            "h1": _clean(crawl.get("h1")),
            "h2": _clean(crawl.get("h2")),
            "warning_reports": warnings,
            "warning_status": "unverified" if warnings else "none",
            "broken": status_code >= 400,
            "redirect": 300 <= status_code < 400,
            "orphan_candidate": bool(
                (inlinks == 0 and url != "https://anatainc.com/")
                or any("orphan" in name for name in warnings)
            ),
            "deep_candidate": isinstance(depth, int) and depth > 3,
            "content_signature": _content_signature(crawl),
        }
        records.append(record)

    signatures: dict[str, list[str]] = {}
    for item in records:
        signature = _clean(item.get("content_signature"))
        if signature:
            signatures.setdefault(signature, []).append(item["url"])
    duplicate_groups = [
        {"signature": signature, "urls": grouped_urls}
        for signature, grouped_urls in sorted(signatures.items())
        if len(grouped_urls) > 1
    ]
    return {
        "policy": "Crawler warnings remain unverified until rendered-page and repository evidence agree.",
        "records": records,
        "duplicate_groups": duplicate_groups,
        "evidence_coverage": _evidence_coverage(report_names),
        "summary": {
            "known_production_urls": len(records),
            "sitemap_urls": sum(1 for item in records if item["in_sitemap"]),
            "crawl_urls": sum(1 for item in records if item["in_crawl"]),
            "indexing_urls": sum(1 for item in records if item["in_indexing_export"]),
            "intent_mapped_urls": sum(1 for item in records if item["in_intent_map"]),
            "urls_missing_intent_owner": sum(1 for item in records if not item["in_intent_map"]),
            "intent_routes_missing_from_sitemap": sum(
                1 for item in records if item["in_intent_map"] and not item["in_sitemap"]
            ),
            "broken_candidates": sum(1 for item in records if item["broken"]),
            "redirect_candidates": sum(1 for item in records if item["redirect"]),
            "orphan_candidates": sum(1 for item in records if item["orphan_candidate"]),
            "deep_candidates": sum(1 for item in records if item["deep_candidate"]),
            "exact_duplicate_groups": len(duplicate_groups),
            "missing_export_categories": sum(
                1 for status in _evidence_coverage(report_names).values() if status == "missing_export"
            ),
        },
    }
