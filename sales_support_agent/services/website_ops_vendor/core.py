#!/usr/bin/env python3
"""Read-only website ops collectors, reporting, and feedback storage."""

from __future__ import annotations

import html as html_module
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_WEBSITE_OPS_ROOT = ROOT_DIR / "website-ops"
DEFAULT_CONFIG_PATH = ROOT_DIR / "config" / "website_ops.json"
DEFAULT_USER_AGENT = "anata-website-ops/1.0"
DEFAULT_TIMEOUT_SECONDS = 20
DEFAULT_REPORT_TITLE = "Website Ops Daily Report"

GENERIC_PRIMARY_HEADINGS = {
    "contact us",
    "free analysis",
    "book a call",
    "book a free analysis",
    "schedule a call",
    "free consultation",
    "let's talk",
    "let s talk",
    "talk to us",
}

CTA_PATH_HINTS = (
    "contact",
    "analysis",
    "consult",
    "book",
    "call",
    "quote",
    "talk",
)

PRIORITY_ORDER = {"P0": 0, "P1": 1, "P2": 2, "P3": 3}


@dataclass(frozen=True)
class WebsiteOpsConfig:
    website_ops_root: Path = DEFAULT_WEBSITE_OPS_ROOT
    daily_reports_dir: Path = DEFAULT_WEBSITE_OPS_ROOT / "reports" / "daily"
    feedback_dir: Path = DEFAULT_WEBSITE_OPS_ROOT / "feedback"
    user_agent: str = DEFAULT_USER_AGENT
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS
    report_title: str = DEFAULT_REPORT_TITLE


class _ObservationHTMLParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._in_title = False
        self._title_buffer: List[str] = []
        self._heading_level: Optional[int] = None
        self._heading_buffer: List[str] = []
        self._ignore_depth = 0
        self._text_buffer: List[str] = []
        self.title = ""
        self.meta: Dict[str, str] = {}
        self.h1: List[str] = []
        self.h2: List[str] = []
        self.h3: List[str] = []
        self.canonical_url = ""

    def handle_starttag(self, tag: str, attrs: List[tuple[str, Optional[str]]]) -> None:
        attrs_map = {key.lower(): (value or "") for key, value in attrs}
        tag = tag.lower()
        if tag == "title":
            self._in_title = True
            self._title_buffer = []
            return
        if tag in {"h1", "h2", "h3"}:
            self._heading_level = int(tag[1])
            self._heading_buffer = []
            return
        if tag in {"script", "style", "noscript"}:
            self._ignore_depth += 1
            return
        if tag == "meta":
            key = attrs_map.get("name") or attrs_map.get("property")
            content = normalize_text(attrs_map.get("content", ""))
            if key and content:
                self.meta[key.strip().lower()] = content
            return
        if tag == "link":
            rel = attrs_map.get("rel", "").lower().split()
            if "canonical" in rel and attrs_map.get("href"):
                self.canonical_url = attrs_map["href"].strip()

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag == "title":
            self._in_title = False
            if self._title_buffer:
                self.title = normalize_text("".join(self._title_buffer))
            self._title_buffer = []
            return
        if tag in {"h1", "h2", "h3"}:
            if self._heading_level == int(tag[1]) and self._heading_buffer:
                heading = normalize_text("".join(self._heading_buffer))
                if heading:
                    getattr(self, tag).append(heading)
            self._heading_level = None
            self._heading_buffer = []
            return
        if tag in {"script", "style", "noscript"} and self._ignore_depth > 0:
            self._ignore_depth -= 1

    def handle_data(self, data: str) -> None:
        if self._ignore_depth > 0 or not data:
            return
        self._text_buffer.append(data)
        if self._in_title:
            self._title_buffer.append(data)
        if self._heading_level:
            self._heading_buffer.append(data)

    def finalize(self) -> None:
        if self._in_title and self._title_buffer and not self.title:
            self.title = normalize_text("".join(self._title_buffer))
        if self._heading_level and self._heading_buffer:
            heading = normalize_text("".join(self._heading_buffer))
            if heading:
                getattr(self, f"h{self._heading_level}").append(heading)

    @property
    def text_content(self) -> str:
        return normalize_text(" ".join(self._text_buffer))


def normalize_text(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def normalize_key(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", " ", normalize_text(value).lower()).strip()


def normalize_url(value: Any) -> str:
    return str(value or "").strip().rstrip("/")


def slugify(value: Any) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", normalize_key(value)).strip("-")
    return slug or "report"


def read_json_file(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError:
        return {}


def _path_from_value(value: Any) -> Optional[Path]:
    if value in (None, ""):
        return None
    return Path(str(value)).expanduser()


def load_config(
    config_path: Optional[Path | str] = None,
    *,
    overrides: Optional[Mapping[str, Any]] = None,
) -> WebsiteOpsConfig:
    path = _path_from_value(config_path) or DEFAULT_CONFIG_PATH
    config_data = read_json_file(path) if path.exists() else {}
    merged: Dict[str, Any] = {
        "website_ops_root": DEFAULT_WEBSITE_OPS_ROOT,
        "daily_reports_dir": DEFAULT_WEBSITE_OPS_ROOT / "reports" / "daily",
        "feedback_dir": DEFAULT_WEBSITE_OPS_ROOT / "feedback",
        "user_agent": DEFAULT_USER_AGENT,
        "timeout_seconds": DEFAULT_TIMEOUT_SECONDS,
        "report_title": DEFAULT_REPORT_TITLE,
    }
    merged.update(config_data)
    env_overrides = {
        "website_ops_root": os.getenv("WEBSITE_OPS_ROOT", "").strip(),
        "daily_reports_dir": os.getenv("WEBSITE_OPS_DAILY_REPORTS_DIR", "").strip(),
        "feedback_dir": os.getenv("WEBSITE_OPS_FEEDBACK_DIR", "").strip(),
        "user_agent": os.getenv("WEBSITE_OPS_USER_AGENT", "").strip(),
        "timeout_seconds": os.getenv("WEBSITE_OPS_TIMEOUT_SECONDS", "").strip(),
        "report_title": os.getenv("WEBSITE_OPS_REPORT_TITLE", "").strip(),
    }
    for key, raw_value in env_overrides.items():
        if raw_value:
            merged[key] = raw_value
    if overrides:
        merged.update(overrides)

    root = _path_from_value(merged.get("website_ops_root")) or DEFAULT_WEBSITE_OPS_ROOT
    daily_reports_dir = _path_from_value(merged.get("daily_reports_dir")) or (root / "reports" / "daily")
    feedback_dir = _path_from_value(merged.get("feedback_dir")) or (root / "feedback")
    user_agent = normalize_text(merged.get("user_agent") or DEFAULT_USER_AGENT) or DEFAULT_USER_AGENT
    report_title = normalize_text(merged.get("report_title") or DEFAULT_REPORT_TITLE) or DEFAULT_REPORT_TITLE
    try:
        timeout_seconds = int(merged.get("timeout_seconds", DEFAULT_TIMEOUT_SECONDS) or DEFAULT_TIMEOUT_SECONDS)
    except (TypeError, ValueError):
        timeout_seconds = DEFAULT_TIMEOUT_SECONDS
    return WebsiteOpsConfig(
        website_ops_root=root,
        daily_reports_dir=daily_reports_dir,
        feedback_dir=feedback_dir,
        user_agent=user_agent,
        timeout_seconds=timeout_seconds,
        report_title=report_title,
    )


def parse_html_document(html_text: str, *, headers: Optional[Mapping[str, str]] = None) -> Dict[str, Any]:
    parser = _ObservationHTMLParser()
    parser.feed(html_text or "")
    parser.close()
    parser.finalize()

    header_map = {str(key).lower(): normalize_text(value) for key, value in (headers or {}).items()}
    robots_meta = normalize_text(parser.meta.get("robots", ""))
    header_robots = header_map.get("x-robots-tag", "")
    noindex = "noindex" in robots_meta.lower() or "noindex" in header_robots.lower()

    return {
        "title": parser.title,
        "meta_description": normalize_text(parser.meta.get("description", "")),
        "canonical_url": normalize_text(parser.canonical_url),
        "robots": robots_meta or header_robots,
        "noindex": noindex,
        "h1": list(parser.h1),
        "h2": list(parser.h2),
        "h3": list(parser.h3),
        "heading_counts": {"h1": len(parser.h1), "h2": len(parser.h2), "h3": len(parser.h3)},
        "text_length": len(parser.text_content),
        "text_excerpt": parser.text_content[:240],
    }


def inspect_html_document(
    url: str,
    html_text: str,
    *,
    status_code: Optional[int] = 200,
    final_url: Optional[str] = None,
    headers: Optional[Mapping[str, str]] = None,
    fetched_at: Optional[datetime] = None,
    response_error: str = "",
) -> Dict[str, Any]:
    extracted = parse_html_document(html_text, headers=headers)
    observation: Dict[str, Any] = {
        "url": url,
        "final_url": final_url or url,
        "status_code": status_code,
        "fetched_at": (fetched_at or datetime.now(timezone.utc)).isoformat(),
        "response_error": normalize_text(response_error),
        **extracted,
    }
    observation["issues"] = detect_page_issues(observation)
    observation["status"] = page_status_from_issues(observation["issues"], status_code=status_code)
    return observation


def fetch_url(url: str, *, config: Optional[WebsiteOpsConfig] = None) -> Dict[str, Any]:
    config = config or load_config()
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": config.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
        method="GET",
    )
    fetched_at = datetime.now(timezone.utc)
    try:
        with urllib.request.urlopen(request, timeout=config.timeout_seconds) as response:
            body = response.read().decode("utf-8", errors="replace")
            return {
                "url": url,
                "final_url": response.geturl(),
                "status_code": getattr(response, "status", response.getcode()),
                "headers": dict(response.headers.items()),
                "body": body,
                "fetched_at": fetched_at,
                "response_error": "",
            }
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return {
            "url": url,
            "final_url": exc.geturl() if hasattr(exc, "geturl") else url,
            "status_code": exc.code,
            "headers": dict(getattr(exc, "headers", {}) or {}),
            "body": body,
            "fetched_at": fetched_at,
            "response_error": f"HTTPError {exc.code}: {exc.reason}",
        }
    except urllib.error.URLError as exc:
        reason = getattr(exc, "reason", exc)
        return {
            "url": url,
            "final_url": url,
            "status_code": None,
            "headers": {},
            "body": "",
            "fetched_at": fetched_at,
            "response_error": f"URLError: {reason}",
        }


def collect_page_observation(url: str, *, config: Optional[WebsiteOpsConfig] = None) -> Dict[str, Any]:
    response = fetch_url(url, config=config)
    return inspect_html_document(
        response["url"],
        response.get("body", ""),
        status_code=response.get("status_code"),
        final_url=response.get("final_url"),
        headers=response.get("headers", {}),
        fetched_at=response.get("fetched_at"),
        response_error=response.get("response_error", ""),
    )


def collect_page_observations(
    urls: Sequence[str],
    *,
    config: Optional[WebsiteOpsConfig] = None,
) -> List[Dict[str, Any]]:
    return [collect_page_observation(url, config=config) for url in urls]


def page_path(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    return parsed.path or "/"


def is_cta_or_contact_page(url: str) -> bool:
    path = page_path(url).lower()
    return any(hint in path for hint in CTA_PATH_HINTS)


def make_issue(
    *,
    code: str,
    priority: str,
    page_url: str,
    summary: str,
    recommendation: str,
    details: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    issue = {
        "code": code,
        "priority": priority,
        "status": "open",
        "page_url": page_url,
        "summary": summary,
        "recommendation": recommendation,
    }
    if details:
        issue["details"] = dict(details)
    return issue


def detect_page_issues(observation: Mapping[str, Any]) -> List[Dict[str, Any]]:
    url = str(observation.get("url") or "")
    final_url = normalize_text(observation.get("final_url", ""))
    issues: List[Dict[str, Any]] = []
    status_code = observation.get("status_code")
    response_error = normalize_text(observation.get("response_error", ""))
    title = normalize_text(observation.get("title", ""))
    h1 = list(observation.get("h1") or [])
    canonical_url = normalize_text(observation.get("canonical_url", ""))
    noindex = bool(observation.get("noindex"))

    if status_code is None:
        issues.append(
            make_issue(
                code="UNREACHABLE",
                priority="P0",
                page_url=url,
                summary="The page could not be fetched.",
                recommendation="Verify the URL, DNS, and origin server response before making content changes.",
                details={"response_error": response_error, "status_code": status_code},
            )
        )
        return issues

    if int(status_code) >= 400:
        issues.append(
            make_issue(
                code="HTTP_ERROR",
                priority="P0",
                page_url=url,
                summary=f"The page returned HTTP {status_code}.",
                recommendation="Restore a 2xx response or redirect the URL to the current canonical page.",
                details={"status_code": status_code, "response_error": response_error},
            )
        )

    if final_url and normalize_url(final_url) and normalize_url(final_url) != normalize_url(url):
        issues.append(
            make_issue(
                code="REDIRECTED_URL",
                priority="P2",
                page_url=url,
                summary="The monitored URL redirects to a different live URL.",
                recommendation="Monitor and internally link to the final preferred URL, or standardize the redirect target as the canonical route.",
                details={"final_url": final_url},
            )
        )

    if not title:
        issues.append(
            make_issue(
                code="MISSING_TITLE",
                priority="P1",
                page_url=url,
                summary="The page is missing a <title> tag.",
                recommendation="Add a unique, descriptive page title for crawlers and users.",
            )
        )

    if not h1:
        issues.append(
            make_issue(
                code="MISSING_H1",
                priority="P1",
                page_url=url,
                summary="The page does not expose a primary H1 heading.",
                recommendation="Add one clear H1 that matches the page topic.",
            )
        )
    elif len(h1) > 1:
        issues.append(
            make_issue(
                code="MULTIPLE_H1",
                priority="P1",
                page_url=url,
                summary="The page exposes more than one H1.",
                recommendation="Keep one primary H1 and move the rest of the page headings to H2 or H3.",
                details={"h1_count": len(h1)},
            )
        )

    first_h1 = normalize_key(h1[0] if h1 else "")
    if first_h1 in GENERIC_PRIMARY_HEADINGS and not is_cta_or_contact_page(url):
        issues.append(
            make_issue(
                code="GENERIC_PRIMARY_HEADING",
                priority="P1",
                page_url=url,
                summary="The primary H1 reads like a generic CTA instead of a page-specific topic.",
                recommendation="Replace the H1 with a topic-specific heading and move CTA copy into a supporting block.",
                details={"primary_h1": h1[0]},
            )
        )

    if not canonical_url:
        issues.append(
            make_issue(
                code="MISSING_CANONICAL",
                priority="P2",
                page_url=url,
                summary="The page does not declare a canonical URL.",
                recommendation="Add a canonical link tag so search engines can identify the preferred version.",
            )
        )
    elif normalize_url(canonical_url) != normalize_url(final_url or url):
        issues.append(
            make_issue(
                code="CANONICAL_MISMATCH",
                priority="P2",
                page_url=url,
                summary="The canonical URL points to a different route than the live page.",
                recommendation="Align the canonical tag, monitored URL, and redirect target to one preferred route.",
                details={"canonical_url": canonical_url, "final_url": final_url or url},
            )
        )

    if noindex:
        issues.append(
            make_issue(
                code="NOINDEX",
                priority="P1",
                page_url=url,
                summary="The page is marked noindex.",
                recommendation="Remove noindex from public production pages unless this is intentional.",
            )
        )

    return issues


def page_status_from_issues(issues: Sequence[Mapping[str, Any]], *, status_code: Optional[int] = None) -> str:
    if status_code is None:
        return "unreachable"
    if status_code >= 400:
        return "needs-attention"
    if not issues:
        return "healthy"
    if any(str(issue.get("priority")) in {"P0", "P1"} for issue in issues):
        return "needs-attention"
    return "review"


def highest_priority(issues: Sequence[Mapping[str, Any]]) -> str:
    if not issues:
        return "P3"
    ranked = sorted((PRIORITY_ORDER.get(str(issue.get("priority")), 99), str(issue.get("priority", "P3"))) for issue in issues)
    return ranked[0][1]


def build_daily_report(
    observations: Sequence[Mapping[str, Any]],
    *,
    report_date: Optional[date | str] = None,
    title: str = DEFAULT_REPORT_TITLE,
    notes: Optional[Sequence[str]] = None,
    feedback_entries: Optional[Sequence[Mapping[str, Any]]] = None,
    report_type: str = "website_ops_daily",
    scope: str = "read-only",
    executed_actions: Optional[Sequence[Mapping[str, Any]]] = None,
) -> Dict[str, Any]:
    if isinstance(report_date, str):
        parsed_date = datetime.fromisoformat(report_date).date()
    else:
        parsed_date = report_date or datetime.now(timezone.utc).date()

    pages: List[Dict[str, Any]] = []
    issues: List[Dict[str, Any]] = []
    for observation in observations:
        obs_issues = list(observation.get("issues") or detect_page_issues(observation))
        page = dict(observation)
        page["issues"] = obs_issues
        page["issue_count"] = len(obs_issues)
        page["priority"] = highest_priority(obs_issues)
        page["status"] = page_status_from_issues(obs_issues, status_code=observation.get("status_code"))
        pages.append(page)
        issues.extend(obs_issues)

    issue_counts_by_priority = Counter(str(issue.get("priority", "P3")) for issue in issues)
    issue_counts_by_code = Counter(str(issue.get("code", "UNKNOWN")) for issue in issues)
    pages_with_issues = sum(1 for page in pages if page["issues"])
    recommendations = unique_preserving(
        issue["recommendation"] for issue in sorted_issues(issues)
    )
    feedback_records = [dict(item) for item in (feedback_entries or [])]
    feedback_records.sort(key=lambda item: (str(item.get("submitted_at") or item.get("recorded_at") or ""), str(item.get("feedback_id") or item.get("_path") or "")), reverse=True)
    open_feedback = [
        item for item in feedback_records
        if str(item.get("status", "")).strip().lower() not in {"closed", "resolved", "done"}
    ]
    recent_feedback = [
        {
            "feedback_id": item.get("feedback_id") or Path(str(item.get("_path", ""))).stem,
            "submitted_at": item.get("submitted_at") or item.get("recorded_at", ""),
            "category": item.get("category") or item.get("type", ""),
            "priority": item.get("priority", ""),
            "summary": item.get("summary") or item.get("title", ""),
            "page_url": item.get("page_url", ""),
            "status": item.get("status", ""),
        }
        for item in feedback_records[:5]
    ]
    executed_records = [dict(item) for item in (executed_actions or [])]

    return {
        "report_type": report_type,
        "title": title,
        "slug": slugify(title),
        "date": parsed_date.isoformat(),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scope": scope,
        "status": "needs-attention" if issues else "healthy",
        "pages_reviewed": len(pages),
        "pages_healthy": len(pages) - pages_with_issues,
        "pages_with_issues": pages_with_issues,
        "issues_found": len(issues),
        "issue_counts_by_priority": {key: issue_counts_by_priority.get(key, 0) for key in ["P0", "P1", "P2", "P3"]},
        "issue_counts_by_code": dict(sorted(issue_counts_by_code.items(), key=lambda item: (-item[1], item[0]))),
        "pages": pages,
        "issues": sorted_issues(issues),
        "recommendations": recommendations,
        "notes": list(notes or []),
        "feedback_received": len(feedback_records),
        "feedback_open": len(open_feedback),
        "recent_feedback": recent_feedback,
        "changes_applied": len(executed_records),
        "executed_actions": executed_records,
    }


def sorted_issues(issues: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(
        (dict(issue) for issue in issues),
        key=lambda issue: (
            PRIORITY_ORDER.get(str(issue.get("priority")), 99),
            str(issue.get("page_url", "")),
            str(issue.get("code", "")),
        ),
    )


def unique_preserving(values: Iterable[str]) -> List[str]:
    seen = set()
    ordered: List[str] = []
    for value in values:
        normalized = normalize_text(value)
        if normalized and normalized not in seen:
            seen.add(normalized)
            ordered.append(normalized)
    return ordered


def render_daily_report_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        f"# {report['title']}",
        "",
        f"Date: {report['date']}",
        f"Generated: {report['generated_at']}",
        f"Scope: {report.get('scope', 'read-only')}",
        "",
        "## Executive Summary",
        "",
        f"- Pages reviewed: `{report['pages_reviewed']}`",
        f"- Healthy pages: `{report['pages_healthy']}`",
        f"- Pages with issues: `{report['pages_with_issues']}`",
        f"- Issues found: `{report['issues_found']}`",
        f"- Status: `{report['status']}`",
        "",
        "## Issue Mix",
    ]
    for priority in ["P0", "P1", "P2", "P3"]:
        lines.append(f"- {priority}: `{report['issue_counts_by_priority'].get(priority, 0)}`")

    lines.extend(
        [
            "",
            "## Goal",
            "",
            f"- Primary goal: {report.get('goal', {}).get('primary', 'Not defined.')}",
        ]
    )
    for metric in report.get("goal", {}).get("success_metrics", []):
        lines.append(f"- Success metric: {metric}")

    analytics_status = report.get("analytics_status", {})
    lines.extend(
        [
            "",
            "## Analytics Status",
            "",
            f"- Search Console connected: `{analytics_status.get('search_console', False)}`",
            f"- GA4 connected: `{analytics_status.get('ga4', False)}`",
        ]
    )
    for note in analytics_status.get("notes", []):
        lines.append(f"- Note: {note}")

    lines.extend(
        [
            "",
            "## Feedback Loop",
            "",
            f"- Feedback received: `{report.get('feedback_received', 0)}`",
            f"- Open feedback items: `{report.get('feedback_open', 0)}`",
            f"- Changes applied: `{report.get('changes_applied', 0)}`",
        ]
    )
    if report.get("recent_feedback"):
        lines.append("- Recent intake:")
        for entry in report["recent_feedback"]:
            lines.append(
                "  - "
                + f"`{entry.get('priority', '')}` {entry.get('summary', 'Feedback item')} "
                + (f"on `{entry.get('page_url')}`" if entry.get("page_url") else "")
            )

    lines.extend(["", "## Pages Reviewed"])
    for page in report.get("pages", []):
        if page.get("title"):
            heading = page["title"]
        elif page.get("h1"):
            heading = page["h1"][0]
        else:
            heading = ""
        lines.extend(
            [
                f"- `{page['url']}`",
                f"  - status: `{page['status']}`",
                f"  - title: `{heading}`",
                f"  - h1 count: `{len(page.get('h1', []))}`",
                f"  - issues: `{page['issue_count']}`",
            ]
        )

    lines.extend(["", "## Issues Found"])
    if report.get("issues"):
        for issue in report["issues"]:
            lines.extend(
                [
                    f"- `{issue['priority']}` `{issue['code']}` on `{issue['page_url']}`",
                    f"  - {issue['summary']}",
                    f"  - recommendation: {issue['recommendation']}",
                ]
            )
    else:
        lines.append("- None.")

    lines.extend(["", "## Recommended Fixes"])
    if report.get("recommendations"):
        lines.extend(f"- {item}" for item in report["recommendations"])
    else:
        lines.append("- None.")

    lines.extend(["", "## Action Queue"])
    if report.get("action_queue"):
        for action in report["action_queue"]:
            lines.extend(
                [
                    f"- `{action.get('action_type', 'action')}` on `{action.get('page_url', '')}`",
                    f"  - section: {action.get('section_name', '')}",
                    f"  - before: {action.get('before_state', '')}",
                    f"  - after: {action.get('after_state', '')}",
                    f"  - why: {action.get('reason', '')}",
                    f"  - source: {action.get('insight_source', '')}",
                    f"  - expected impact: {action.get('expected_impact', '')}",
                    f"  - confidence: `{action.get('confidence', '')}`",
                ]
            )
    else:
        lines.append("- None.")

    lines.extend(["", "## Team Support Needed"])
    if report.get("support_requests"):
        lines.extend(f"- {item}" for item in report["support_requests"])
    else:
        lines.append("- None.")

    lines.extend(["", "## System Guidance"])
    for label, key in [("Start doing", "start_doing"), ("Stop doing", "stop_doing"), ("Do more of", "do_more_of")]:
        lines.append(f"- {label}:")
        values = report.get(key) or []
        if values:
            lines.extend(f"  - {item}" for item in values)
        else:
            lines.append("  - None.")

    lines.extend(["", "## Changes Applied"])
    if report.get("executed_actions"):
        for action in report["executed_actions"]:
            lines.append(
                f"- `{action.get('action_type', 'action')}` on `{action.get('page_url', '')}`"
            )
            summary = action.get("summary") or {}
            if summary:
                lines.append(
                    f"  - `{summary.get('before_text', '')}` -> `{summary.get('after_text', '')}`"
                )
            lines.append(f"  - verification: `{action.get('verification_status', '')}`")
    else:
        lines.append("- None.")

    if report.get("notes"):
        lines.extend(["", "## Notes"])
        lines.extend(f"- {note}" for note in report["notes"])

    return "\n".join(lines).strip() + "\n"


def render_daily_report_html(report: Mapping[str, Any]) -> str:
    def esc(value: Any) -> str:
        return html_module.escape(str(value))

    issue_rows = []
    for issue in report.get("issues", []):
        issue_rows.append(
            "<li>"
            f"<strong>{esc(issue['priority'])} {esc(issue['code'])}</strong> "
            f"on <code>{esc(issue['page_url'])}</code><br>"
            f"{esc(issue['summary'])}<br>"
            f"<span class=\"muted\">Recommendation:</span> {esc(issue['recommendation'])}"
            "</li>"
        )

    page_rows = []
    for page in report.get("pages", []):
        title = page.get("title") or (page.get("h1") or [""])[0]
        page_rows.append(
            "<li>"
            f"<strong><code>{esc(page['url'])}</code></strong>"
            f"<div>Status: {esc(page['status'])}</div>"
            f"<div>Title: {esc(title)}</div>"
            f"<div>H1 count: {esc(len(page.get('h1', [])))}</div>"
            f"<div>Issues: {esc(page.get('issue_count', 0))}</div>"
            "</li>"
        )

    notes = "".join(f"<li>{esc(note)}</li>" for note in report.get("notes", []))
    recommendations = "".join(f"<li>{esc(item)}</li>" for item in report.get("recommendations", []))
    issue_counts = "".join(
        f"<li><strong>{esc(priority)}</strong>: {esc(report['issue_counts_by_priority'].get(priority, 0))}</li>"
        for priority in ["P0", "P1", "P2", "P3"]
    )
    feedback_rows = "".join(
        "<li>"
        f"<strong>{esc(item.get('priority', ''))}</strong> "
        f"{esc(item.get('summary', 'Feedback item'))}"
        + (f"<br><code>{esc(item.get('page_url', ''))}</code>" if item.get("page_url") else "")
        + "</li>"
        for item in report.get("recent_feedback", [])
    )
    action_rows = "".join(
        "<li>"
        f"<strong>{esc(item.get('action_type', 'action'))}</strong> on <code>{esc(item.get('page_url', ''))}</code>"
        + (
            f"<br><span class=\"muted\">{esc((item.get('summary') or {}).get('before_text', ''))} -> {esc((item.get('summary') or {}).get('after_text', ''))}</span>"
            if item.get("summary")
            else ""
        )
        + f"<br>Verification: {esc(item.get('verification_status', ''))}"
        + "</li>"
        for item in report.get("executed_actions", [])
    )
    action_queue_rows = "".join(
        "<li>"
        f"<strong>{esc(item.get('action_type', 'action'))}</strong> on <code>{esc(item.get('page_url', ''))}</code>"
        f"<br><span class=\"muted\">Section:</span> {esc(item.get('section_name', ''))}"
        f"<br><span class=\"muted\">Before:</span> {esc(item.get('before_state', ''))}"
        f"<br><span class=\"muted\">After:</span> {esc(item.get('after_state', ''))}"
        f"<br><span class=\"muted\">Why:</span> {esc(item.get('reason', ''))}"
        f"<br><span class=\"muted\">Insight source:</span> {esc(item.get('insight_source', ''))}"
        f"<br><span class=\"muted\">Expected impact:</span> {esc(item.get('expected_impact', ''))}"
        f"<br><span class=\"muted\">Confidence:</span> {esc(item.get('confidence', ''))}"
        + "</li>"
        for item in report.get("action_queue", [])
    )
    support_rows = "".join(f"<li>{esc(item)}</li>" for item in report.get("support_requests", []))
    guidance_blocks = "".join(
        f"<div><strong>{esc(label)}</strong><ul>{''.join(f'<li>{esc(item)}</li>' for item in report.get(key, [])) or '<li>None.</li>'}</ul></div>"
        for label, key in [("Start doing", "start_doing"), ("Stop doing", "stop_doing"), ("Do more of", "do_more_of")]
    )
    analytics_notes = "".join(f"<li>{esc(item)}</li>" for item in report.get("analytics_status", {}).get("notes", []))
    page_insight_rows = "".join(
        "<li>"
        f"<strong><code>{esc(item.get('page_url', ''))}</code></strong>"
        f"<br>Bucket: {esc(item.get('bucket', ''))} · Score: {esc(item.get('score', ''))}"
        f"<br>GSC: {esc(int((item.get('search_console') or {}).get('impressions', 0)))} impressions, {esc(round(float((item.get('search_console') or {}).get('ctr', 0) or 0) * 100, 2))}% CTR"
        f"<br>GA4: {esc(int((item.get('ga4') or {}).get('sessions', 0)))} sessions, {esc(int((item.get('ga4') or {}).get('conversions', 0)))} conversions"
        + (
            "<br><span class=\"muted\">Insights:</span> "
            + esc(" | ".join(item.get("insights", [])))
            if item.get("insights")
            else ""
        )
        + "</li>"
        for item in report.get("page_insights", [])[:10]
    )

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{esc(report['title'])}</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f7f4ef;
      --panel: #ffffff;
      --text: #17212b;
      --muted: #5c6773;
      --line: #d6d0c7;
      --accent: #0f6d66;
    }}
    body {{
      margin: 0;
      font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: linear-gradient(180deg, #f6f2eb 0%, #f1efe8 100%);
      color: var(--text);
    }}
    main {{
      max-width: 980px;
      margin: 0 auto;
      padding: 32px 20px 48px;
    }}
    .panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 20px 22px;
      box-shadow: 0 12px 36px rgba(15, 21, 31, 0.06);
      margin-bottom: 18px;
    }}
    h1, h2 {{ margin: 0 0 12px; line-height: 1.1; }}
    h1 {{ font-size: clamp(2rem, 3.4vw, 3.2rem); }}
    h2 {{ font-size: 1.1rem; text-transform: uppercase; letter-spacing: 0.08em; color: var(--accent); }}
    p, li {{ line-height: 1.55; }}
    ul {{ margin: 0; padding-left: 1.2rem; }}
    .meta {{ color: var(--muted); display: grid; gap: 4px; }}
    .grid {{ display: grid; gap: 16px; grid-template-columns: repeat(auto-fit, minmax(230px, 1fr)); }}
    .muted {{ color: var(--muted); }}
    code {{ background: #f0ece4; padding: 0.1rem 0.3rem; border-radius: 6px; }}
  </style>
</head>
<body>
  <main>
    <div class="panel">
      <h1>{esc(report['title'])}</h1>
      <div class="meta">
        <div>Date: {esc(report['date'])}</div>
        <div>Generated: {esc(report['generated_at'])}</div>
        <div>Scope: {esc(report.get('scope', 'read-only'))}</div>
        <div>Status: {esc(report['status'])}</div>
      </div>
    </div>
    <div class="panel">
      <h2>Executive Summary</h2>
      <div class="grid">
        <div><strong>Pages reviewed</strong><br>{esc(report['pages_reviewed'])}</div>
        <div><strong>Healthy pages</strong><br>{esc(report['pages_healthy'])}</div>
        <div><strong>Pages with issues</strong><br>{esc(report['pages_with_issues'])}</div>
        <div><strong>Issues found</strong><br>{esc(report['issues_found'])}</div>
      </div>
    </div>
    <div class="panel">
      <h2>Issue Mix</h2>
      <ul>{issue_counts}</ul>
    </div>
    <div class="panel">
      <h2>Goal</h2>
      <p>{esc(report.get('goal', {}).get('primary', 'Not defined.'))}</p>
      <ul>{''.join(f'<li>{esc(item)}</li>' for item in report.get('goal', {}).get('success_metrics', [])) or '<li>No success metrics defined.</li>'}</ul>
    </div>
    <div class="panel">
      <h2>Analytics Status</h2>
      <div class="grid">
        <div><strong>Search Console</strong><br>{esc(report.get('analytics_status', {}).get('search_console', False))}</div>
        <div><strong>GA4</strong><br>{esc(report.get('analytics_status', {}).get('ga4', False))}</div>
      </div>
      <ul>{analytics_notes if analytics_notes else '<li>No analytics warnings.</li>'}</ul>
    </div>
    <div class="panel">
      <h2>Feedback Loop</h2>
      <div class="grid">
        <div><strong>Feedback received</strong><br>{esc(report.get('feedback_received', 0))}</div>
        <div><strong>Open items</strong><br>{esc(report.get('feedback_open', 0))}</div>
        <div><strong>Changes applied</strong><br>{esc(report.get('changes_applied', 0))}</div>
      </div>
      <ul>{feedback_rows if feedback_rows else '<li>No feedback records yet.</li>'}</ul>
    </div>
    <div class="panel">
      <h2>Priority Action Queue</h2>
      <ul>{action_queue_rows if action_queue_rows else '<li>No priority actions generated in this run.</li>'}</ul>
    </div>
    <div class="panel">
      <h2>Page Insights</h2>
      <ul>{page_insight_rows if page_insight_rows else '<li>No page insights generated.</li>'}</ul>
    </div>
    <div class="panel">
      <h2>Team Support Needed</h2>
      <ul>{support_rows if support_rows else '<li>No support prompts generated.</li>'}</ul>
    </div>
    <div class="panel">
      <h2>System Guidance</h2>
      <div class="grid">{guidance_blocks}</div>
    </div>
    <div class="panel">
      <h2>Changes Applied</h2>
      <ul>{action_rows if action_rows else '<li>No approved actions executed in this run.</li>'}</ul>
    </div>
    <div class="panel">
      <h2>Pages Reviewed</h2>
      <ul>{''.join(page_rows)}</ul>
    </div>
    <div class="panel">
      <h2>Issues Found</h2>
      <ul>{''.join(issue_rows) if issue_rows else '<li>None.</li>'}</ul>
    </div>
    <div class="panel">
      <h2>Recommended Fixes</h2>
      <ul>{recommendations if recommendations else '<li>None.</li>'}</ul>
    </div>
    {"<div class='panel'><h2>Notes</h2><ul>" + notes + "</ul></div>" if notes else ""}
  </main>
</body>
</html>"""


def write_daily_report_artifacts(
    report: Mapping[str, Any],
    *,
    output_dir: Optional[Path | str] = None,
    config: Optional[WebsiteOpsConfig] = None,
) -> Dict[str, Path]:
    config = config or load_config()
    directory = _path_from_value(output_dir) or config.daily_reports_dir
    directory.mkdir(parents=True, exist_ok=True)
    base = f"{report['date']}-{slugify(report['title'])}"
    json_path = directory / f"{base}.json"
    markdown_path = directory / f"{base}.md"
    html_path = directory / f"{base}.html"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True))
    markdown_path.write_text(render_daily_report_markdown(report))
    html_path.write_text(render_daily_report_html(report))
    return {"json": json_path, "markdown": markdown_path, "html": html_path}


def run_daily_report_pipeline(
    urls: Sequence[str],
    *,
    config: Optional[WebsiteOpsConfig] = None,
    report_date: Optional[date | str] = None,
    output_dir: Optional[Path | str] = None,
    persist: bool = True,
    feedback_entries: Optional[Sequence[Mapping[str, Any]]] = None,
    title: Optional[str] = None,
    report_type: str = "website_ops_daily",
    scope: str = "read-only",
    notes: Optional[Sequence[str]] = None,
    executed_actions: Optional[Sequence[Mapping[str, Any]]] = None,
) -> Dict[str, Any]:
    config = config or load_config()
    observations = collect_page_observations(urls, config=config)
    report = build_daily_report(
        observations,
        report_date=report_date,
        title=title or config.report_title,
        notes=notes,
        feedback_entries=feedback_entries if feedback_entries is not None else load_feedback_entries(config=config),
        report_type=report_type,
        scope=scope,
        executed_actions=executed_actions,
    )
    artifacts = write_daily_report_artifacts(report, output_dir=output_dir, config=config) if persist else {}
    return {
        "config": config,
        "observations": observations,
        "report": report,
        "artifacts": artifacts,
    }


def save_feedback_entry(
    entry: Mapping[str, Any],
    *,
    config: Optional[WebsiteOpsConfig] = None,
    feedback_dir: Optional[Path | str] = None,
    timestamp: Optional[datetime] = None,
) -> Path:
    config = config or load_config()
    directory = _path_from_value(feedback_dir) or config.feedback_dir
    timestamp = timestamp or datetime.now(timezone.utc)
    date_dir = directory / timestamp.date().isoformat()
    date_dir.mkdir(parents=True, exist_ok=True)
    slug_source = entry.get("summary") or entry.get("title") or entry.get("page_url") or "feedback"
    path = date_dir / f"{timestamp.strftime('%H%M%SZ')}-{slugify(slug_source)}.json"
    payload = dict(entry)
    payload["recorded_at"] = timestamp.isoformat()
    payload["recorded_date"] = timestamp.date().isoformat()
    path.write_text(json.dumps(payload, indent=2, sort_keys=True))
    return path


def write_feedback_entry(entry: Mapping[str, Any], *, path: Path) -> Path:
    payload = {key: value for key, value in dict(entry).items() if not str(key).startswith("_")}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True))
    return path


def update_feedback_entry(
    entry: Mapping[str, Any],
    updates: Mapping[str, Any],
) -> Dict[str, Any]:
    payload = dict(entry)
    payload.update(dict(updates))
    path_value = payload.get("_path")
    if path_value:
        write_feedback_entry(payload, path=Path(str(path_value)))
        payload["_path"] = str(path_value)
    return payload


def load_feedback_entries(
    *,
    config: Optional[WebsiteOpsConfig] = None,
    feedback_dir: Optional[Path | str] = None,
) -> List[Dict[str, Any]]:
    config = config or load_config()
    directory = _path_from_value(feedback_dir) or config.feedback_dir
    if not directory.exists():
        return []
    entries: List[Dict[str, Any]] = []
    for path in sorted(directory.rglob("*.json")):
        try:
            payload = json.loads(path.read_text())
        except json.JSONDecodeError:
            continue
        payload["_path"] = str(path)
        entries.append(payload)
    entries.sort(key=lambda item: (item.get("recorded_at", ""), item.get("_path", "")))
    return entries
