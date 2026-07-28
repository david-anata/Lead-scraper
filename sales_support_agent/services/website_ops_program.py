"""Deterministic program planning for the autonomous Website Ops system."""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping
from urllib.parse import urlparse


INDEXING_REASON_POLICIES: dict[str, tuple[str, str, str]] = {
    "submitted and indexed": (
        "indexed",
        "low",
        "Retain the canonical, sitemap membership, and crawlable internal links.",
    ),
    "indexed, not submitted in sitemap": (
        "indexed",
        "medium",
        "Confirm whether the canonical URL belongs in the production sitemap.",
    ),
    "blocked due to access forbidden (403)": (
        "investigate",
        "high",
        "Confirm whether the URL is an intentionally protected system path or an unexpectedly blocked marketing page.",
    ),
    "crawled - currently not indexed": (
        "investigate",
        "high",
        "Test uniqueness, intent ownership, canonical consistency, internal links, rendering, and useful content depth.",
    ),
    "discovered - currently not indexed": (
        "investigate",
        "high",
        "Test crawlable internal links, crawl depth, sitemap state, server availability, and URL duplication.",
    ),
    "not found (404)": (
        "investigate",
        "medium",
        "Check internal links, backlinks, sitemap membership, historical value, and whether a genuinely equivalent replacement exists.",
    ),
    "page with redirect": (
        "redirect",
        "medium",
        "Remove redirecting URLs from sitemaps and update internal links to the final destination.",
    ),
    "duplicate without user-selected canonical": (
        "canonical alternate",
        "high",
        "Confirm intent and content duplication before selecting, consolidating, or redirecting to a preferred URL.",
    ),
    "alternate page with proper canonical tag": (
        "canonical alternate",
        "low",
        "Verify the canonical target remains indexable, internally linked, and present in the sitemap.",
    ),
}

INTENTIONAL_BLOCK_PATTERNS = ("/wp-*.php",)


def _normalized_reason(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _normalized_url(value: Any) -> str:
    url = str(value or "").strip()
    if not url:
        return ""
    parsed = urlparse(url)
    if parsed.scheme and parsed.netloc:
        path = parsed.path or "/"
        return f"{parsed.scheme.lower()}://{parsed.netloc.lower()}{path}"
    return url


def _intentional_block(url: str, reason: str) -> bool:
    if reason != "blocked due to access forbidden (403)":
        return False
    lowered = url.lower()
    return any(pattern in lowered for pattern in INTENTIONAL_BLOCK_PATTERNS)


def classify_indexing_record(record: Mapping[str, Any]) -> dict[str, Any]:
    """Return a deterministic desired state and next operation for one GSC row."""

    url = _normalized_url(record.get("url"))
    reason = _normalized_reason(record.get("reason"))
    desired_state, priority, operation = INDEXING_REASON_POLICIES.get(
        reason,
        (
            "investigate",
            "medium",
            "Reconcile the URL against the sitemap, rendered crawl, canonical, robots directives, internal links, and intent map.",
        ),
    )
    intentional = _intentional_block(url, reason)
    if intentional:
        desired_state = "blocked intentionally"
        priority = "low"
        operation = (
            "Retain the protected WordPress system-path rule and monitor for any real marketing URL entering this reason group."
        )
    return {
        "url": url,
        "reason": str(record.get("reason", "")).strip(),
        "last_crawled": str(record.get("last_crawled", "")).strip(),
        "desired_state": desired_state,
        "priority": priority,
        "intentional": intentional,
        "next_operation": operation,
        "source": str(record.get("source", "Google Search Console")).strip(),
        "observed_at": str(record.get("observed_at", "")).strip(),
        "verdict": str(record.get("verdict", "")).strip(),
        "robots_txt_state": str(record.get("robots_txt_state", "")).strip(),
        "indexing_state": str(record.get("indexing_state", "")).strip(),
        "page_fetch_state": str(record.get("page_fetch_state", "")).strip(),
        "google_canonical": str(record.get("google_canonical", "")).strip(),
        "user_canonical": str(record.get("user_canonical", "")).strip(),
        "crawled_as": str(record.get("crawled_as", "")).strip(),
    }


def build_indexing_inventory(records: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """Classify and summarize imported Search Console indexing evidence."""

    classified = [classify_indexing_record(record) for record in records]
    classified = [item for item in classified if item["url"]]
    classified.sort(
        key=lambda item: (
            item["intentional"],
            {"high": 0, "medium": 1, "low": 2}.get(item["priority"], 3),
            item["reason"],
            item["url"],
        )
    )
    reason_counts: dict[str, int] = {}
    state_counts: dict[str, int] = {}
    for item in classified:
        reason = item["reason"] or "Unspecified"
        reason_counts[reason] = reason_counts.get(reason, 0) + 1
        state = item["desired_state"]
        state_counts[state] = state_counts.get(state, 0) + 1
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "records": classified,
        "summary": {
            "known_urls": len(classified),
            "needs_action": sum(
                1
                for item in classified
                if item["desired_state"] not in {"indexed", "blocked intentionally"}
            ),
            "indexed": sum(
                1 for item in classified if item["desired_state"] == "indexed"
            ),
            "intentional_exclusions": sum(1 for item in classified if item["intentional"]),
            "reason_counts": dict(sorted(reason_counts.items())),
            "desired_state_counts": dict(sorted(state_counts.items())),
        },
    }


def reconcile_indexing_inventory(
    inventory: Mapping[str, Any],
    observations: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    """Join Google's historical state to the newest rendered production response."""

    current_status: dict[str, int] = {}
    for observation in observations:
        url = _normalized_url(observation.get("url"))
        if not url:
            continue
        status = int(
            observation.get("status_code")
            or observation.get("response_status")
            or 0
        )
        if status:
            current_status[url] = status

    records = [
        dict(item)
        for item in list(inventory.get("records") or [])
        if isinstance(item, Mapping)
    ]
    for record in records:
        status = current_status.get(_normalized_url(record.get("url")), 0)
        reason = _normalized_reason(record.get("reason"))
        record["production_status"] = status or None
        if status == 200 and reason in {
            "not found (404)",
            "blocked due to access forbidden (403)",
        }:
            record.update(
                {
                    "desired_state": "recrawl pending",
                    "priority": "medium",
                    "next_operation": (
                        "Production returns HTTP 200. Retain the canonical sitemap URL "
                        "and crawlable internal links, then verify after Google's next crawl."
                    ),
                    "reconciliation": (
                        f"Google's last observed state was {record.get('reason')}; "
                        "fresh rendered production returns HTTP 200."
                    ),
                }
            )
        elif status == 200 and reason == "url is unknown to google":
            record.update(
                {
                    "desired_state": "discovery pending",
                    "priority": "medium",
                    "next_operation": (
                        "Retain the canonical URL in the submitted sitemap and add or "
                        "verify crawlable contextual internal links from established pages."
                    ),
                    "reconciliation": (
                        "Fresh rendered production returns HTTP 200, but Google has not "
                        "discovered or recorded the URL yet."
                    ),
                }
            )

    summary = dict(inventory.get("summary") or {})
    state_counts: dict[str, int] = {}
    for record in records:
        state = str(record.get("desired_state", "investigate"))
        state_counts[state] = state_counts.get(state, 0) + 1
    summary.update(
        {
            "known_urls": len(records),
            "needs_action": sum(
                1
                for record in records
                if record.get("desired_state")
                not in {"indexed", "blocked intentionally"}
            ),
            "indexed": sum(
                1 for record in records if record.get("desired_state") == "indexed"
            ),
            "desired_state_counts": dict(sorted(state_counts.items())),
        }
    )
    return {
        **dict(inventory),
        "records": records,
        "summary": summary,
    }


def _csv_rows(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        rows: list[dict[str, Any]] = []
        for row in reader:
            normalized = {str(key or "").strip().lower().replace(" ", "_"): value for key, value in row.items()}
            url = normalized.get("url") or normalized.get("page") or normalized.get("address")
            reason = normalized.get("reason") or normalized.get("coverage") or normalized.get("status")
            if not url:
                continue
            rows.append(
                {
                    "url": url,
                    "reason": reason or path.stem,
                    "last_crawled": normalized.get("last_crawled") or normalized.get("last_crawl") or "",
                    "source": "Google Search Console export",
                }
            )
        return rows


def load_indexing_inventory(root: Path) -> dict[str, Any]:
    """Load durable inventory or classify recognized CSV/JSON imports."""

    directory = root / "indexing"
    inventory_path = directory / "inventory.json"
    if inventory_path.exists():
        try:
            payload = json.loads(inventory_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                return payload
        except (OSError, json.JSONDecodeError):
            pass

    records: list[Mapping[str, Any]] = []
    if directory.exists():
        for path in sorted(directory.glob("*.csv")):
            try:
                records.extend(_csv_rows(path))
            except OSError:
                continue
        for path in sorted(directory.glob("*.json")):
            if path.name in {
                inventory_path.name,
                "crawl_inventory.json",
                "crawl_verification.json",
            }:
                continue
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if isinstance(payload, list):
                records.extend(item for item in payload if isinstance(item, Mapping))
            elif isinstance(payload, Mapping):
                values = payload.get("records") or payload.get("rows") or []
                records.extend(item for item in values if isinstance(item, Mapping))
    return build_indexing_inventory(records)


@dataclass(frozen=True)
class ProgramWorkItem:
    title: str
    state: str
    work_type: str
    target: str
    evidence: str
    business_impact: str
    confidence: str
    risk: str
    next_operation: str
    start_condition: str
    validation: str
    owner: str = "Website Ops"
    needs_david: bool = False


def build_program_plan(
    *,
    analytics_status: Mapping[str, Any],
    action_queue: list[Mapping[str, Any]],
    support_requests: list[str],
    indexing_inventory: Mapping[str, Any],
    crawl_verification: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the visible current/next-work plan from verified system state."""

    items: list[ProgramWorkItem] = []
    decision_ready = bool(analytics_status.get("search_console") and analytics_status.get("ga4"))
    if not decision_ready:
        notes = "; ".join(str(item) for item in analytics_status.get("notes", []) if str(item).strip())
        items.append(
            ProgramWorkItem(
                title="Repair ranking data connections",
                state="Blocked",
                work_type="Source readiness",
                target="Search Console and GA4",
                evidence=notes or "One or more required decision-data sources are unavailable.",
                business_impact="Ranking-led production work cannot be selected safely without trustworthy source data.",
                confidence="High",
                risk="Low",
                next_operation="Repair the named provider connection and rerun the daily sweep.",
                start_condition="Credentials and configured properties validate successfully.",
                validation="Both providers return a successful observed or zero-row response.",
                needs_david=True,
            )
        )

    for action in action_queue[:3]:
        items.append(
            ProgramWorkItem(
                title=str(action.get("section_name") or action.get("action_type") or "Qualified website improvement"),
                state="Ready" if str(action.get("execution_eligibility", "")) == "auto_execute" else "Needs review",
                work_type=str(action.get("action_type", "Website improvement")).replace("_", " ").title(),
                target=str(action.get("page_url", "")),
                evidence="; ".join(str(value) for value in list(action.get("evidence") or [])[:2])
                or str(action.get("reason", "")),
                business_impact=str(action.get("expected_impact", "Improves the owning page against the current ranking goal.")),
                confidence=str(action.get("confidence", "medium")).title(),
                risk="Low" if str(action.get("execution_eligibility", "")) == "auto_execute" else "Medium",
                next_operation=str(action.get("after_state", "") or action.get("reason", "")),
                start_condition=(
                    "The next execution worker run."
                    if str(action.get("execution_eligibility", "")) == "auto_execute"
                    else "The recommendation passes its remaining approval or claim gate."
                ),
                validation="Build, rendered preview, deployment identity, production recrawl, and rollback checks pass.",
            )
        )

    indexing_summary = dict(indexing_inventory.get("summary") or {})
    crawl_summary = dict((crawl_verification or {}).get("summary") or {})
    confirmed_crawl_urls = int(crawl_summary.get("confirmed_urls", 0) or 0)
    pending_crawl_urls = int(crawl_summary.get("pending_urls", 0) or 0)
    if confirmed_crawl_urls:
        items.append(
            ProgramWorkItem(
                title=f"Resolve {confirmed_crawl_urls} rendered-confirmed crawl defects",
                state="Ready",
                work_type="Technical SEO remediation",
                target="anatainc.com production crawl",
                evidence=f"{confirmed_crawl_urls} production URLs still reproduce one or more crawler warnings in fresh rendered evidence.",
                business_impact="Confirmed crawl and page-structure defects can prevent discovery, indexing, relevance, and confident AI citation.",
                confidence="High",
                risk="Medium",
                next_operation="Apply the smallest marketing-site correction for each confirmed warning, then deploy and recrawl.",
                start_condition="The next eligible Website Ops implementation run.",
                validation="The production URL passes a fresh rendered observation and the next crawl no longer reports the defect.",
            )
        )
    if pending_crawl_urls:
        items.append(
            ProgramWorkItem(
                title=f"Verify {pending_crawl_urls} crawl-warning URLs",
                state="Measuring",
                work_type="Crawler evidence verification",
                target="anatainc.com production crawl",
                evidence=f"{pending_crawl_urls} URLs have warnings that are not yet proved by rendered-page, resource-header, or link-graph evidence.",
                business_impact="Verification prevents stale crawler output and false positives from causing unnecessary production changes.",
                confidence="Medium",
                risk="Low",
                next_operation="Run the required resource, header, semantic, or internal-link check for each pending warning class.",
                start_condition="The next daily evidence sweep.",
                validation="Every warning becomes confirmed or disproved with timestamped production evidence.",
            )
        )
    if not indexing_summary.get("known_urls"):
        items.append(
            ProgramWorkItem(
                title="Import and classify Search Console indexing exclusions",
                state="Next",
                work_type="Indexing inventory",
                target="anatainc.com known URLs",
                evidence="Search Console indexing reason URLs have not yet been imported into the durable Website Ops inventory.",
                business_impact="The system cannot repair valuable unindexed pages or suppress intentional exclusions until each affected URL has a desired state.",
                confidence="High",
                risk="Low",
                next_operation="Import Search Console Page Indexing exports and join them to sitemap, crawl, canonical, and intent evidence.",
                start_condition="A recognized Search Console CSV or JSON export is present in Website Ops indexing storage.",
                validation="Every imported URL has a reason, desired state, priority, and exact next operation.",
            )
        )
    elif int(indexing_summary.get("needs_action", 0) or 0):
        count = int(indexing_summary.get("needs_action", 0) or 0)
        items.append(
            ProgramWorkItem(
                title=f"Resolve {count} indexing URLs that need action",
                state="Ready",
                work_type="Indexing remediation",
                target="anatainc.com indexing inventory",
                evidence=f"{count} imported URLs are not intentional exclusions.",
                business_impact="Valuable marketing pages cannot earn rankings or AI citations until they are indexable and considered useful.",
                confidence="High",
                risk="Medium",
                next_operation="Reconcile each URL against the sitemap, rendered crawl, canonical, links, content fingerprint, and intent owner.",
                start_condition="The next daily or weekly indexing worker run.",
                validation="Each URL reaches its recorded desired search state or remains blocked with a specific reason.",
            )
        )

    ga4_trust = str(analytics_status.get("ga4_trust_status", "missing")).lower()
    if ga4_trust != "trusted":
        items.append(
            ProgramWorkItem(
                title="Validate qualified-lead attribution",
                state="Needs David",
                work_type="Measurement",
                target=str(analytics_status.get("primary_lead_event", "generate_lead")),
                evidence=f"GA4 lead-event trust is {ga4_trust or 'missing'}.",
                business_impact="Agent cannot prioritize safely against qualified business outcomes until a real submit is reconciled.",
                confidence="High",
                risk="Low",
                next_operation="Submit one real service-page lead and reconcile the event with the CRM record.",
                start_condition="David completes one real service-page submission.",
                validation="One successful submit produces one GA4 event with correct landing-page attribution and one matching CRM record.",
                needs_david=True,
            )
        )

    if not items:
        items.append(
            ProgramWorkItem(
                title="Measure the latest verified production changes",
                state="Measuring",
                work_type="Outcome learning",
                target="Current intent owners",
                evidence="No new action currently passes the evidence and risk gates.",
                business_impact="Comparable outcome evidence prevents unnecessary publishing and teaches the next decision cycle.",
                confidence="Medium",
                risk="Low",
                next_operation="Wait for the next comparable Search Console and qualified-conversion observation window.",
                start_condition="The next scheduled observation window closes.",
                validation="Like-for-like observations are recorded without claiming causation.",
            )
        )

    david_requests = [str(item).strip() for item in support_requests if str(item).strip()]
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "current": asdict(items[0]),
        "next": [asdict(item) for item in items[1:5]],
        "needs_david_count": sum(1 for item in items if item.needs_david),
        "support_requests": david_requests,
    }
