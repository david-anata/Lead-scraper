"""Website ops dashboard and execution helpers for the agent admin app."""

from __future__ import annotations

import html
import hashlib
import json
import os
import re
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from sales_support_agent.config import Settings
from sales_support_agent.integrations.resend import ResendClient
from sales_support_agent.services.admin_nav import render_agent_favicon_links, render_agent_nav, render_agent_nav_styles
from sales_support_agent.services.website_ops_autonomy import build_autonomy_overlay
from sales_support_agent.services.website_ops_candidates import (
    build_candidates,
    candidate_summary,
    lane_registry,
    load_candidate_ledger,
    persist_candidate_ledger,
    select_bounded_actions,
)
from sales_support_agent.services.website_ops_control_panels import (
    render_daily_portfolio_panel,
    render_production_inventory_panel,
)
from sales_support_agent.services.website_ops_content_strategy import (
    load_content_strategy,
)
from sales_support_agent.services.website_ops_query_intelligence import (
    load_query_intelligence,
)
from sales_support_agent.services.website_ops_inventory import build_production_inventory
from sales_support_agent.services.website_ops_program import (
    build_program_plan,
    load_indexing_inventory,
)
from sales_support_agent.services.website_ops_portfolio import build_daily_action_portfolio
from sales_support_agent.services.website_ops_screaming_frog import (
    build_crawl_verification,
    collect_crawl_resource_observations,
    load_crawl_inventory,
    load_crawl_verification,
    save_crawl_verification,
)
from sales_support_agent.services.website_ops_github import (
    CONTENT_ACTION_TYPES,
    METADATA_ACTION_TYPES,
    execute_github_article_action,
    execute_github_metadata_action,
    github_metadata_is_configured,
)
from sales_support_agent.services.website_ops_outcomes import FAILED_OUTCOME, classify_run_outcome
from sales_support_agent.services import website_ops_vendor as website_ops


@dataclass(frozen=True)
class WebsiteOpsActionResult:
    ok: bool
    message: str
    report: dict[str, Any] | None = None
    record: dict[str, Any] | None = None


RUN_MODES = ("daily", "weekly", "monthly")
RUN_STATUSES = {"idle", "queued", "running", "succeeded", "failed", "failed_outcome"}
MVP_MODE_ACTIVE = True
MVP_ALLOWED_ACTION_TYPES = {
    "inject_faq_block",
    "expand_service_page_section",
    "meta_update",
    "meta_title_update",
    "meta_description_update",
    "canonical_update",
    "publish_blog_article",
}
WORKFLOW_OWNED_FEEDBACK_FIELDS = {
    "status",
    "reviewer_name",
    "review_notes",
    "action_type",
    "action_value",
    "target_post_id",
    "reviewed_at",
    "last_execution_at",
    "execution_result",
    "execution_error",
}


def _mvp_action_allowed(action_type: str) -> bool:
    return str(action_type or "").strip() in MVP_ALLOWED_ACTION_TYPES


def _mvp_filter_action_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [item for item in items if _mvp_action_allowed(str(item.get("action_type", "")).strip())]


def _mvp_filter_feedback_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not MVP_MODE_ACTIVE:
        return records
    filtered: list[dict[str, Any]] = []
    for record in records:
        if not bool(record.get("auto_generated")):
            filtered.append(record)
            continue
        action_type = str(record.get("suggested_action_type", "") or record.get("action_type", "")).strip()
        if _mvp_action_allowed(action_type):
            filtered.append(record)
    return filtered


def _mvp_filter_report_payload(payload: dict[str, Any]) -> dict[str, Any]:
    filtered = dict(payload)
    filtered["action_queue"] = _mvp_filter_action_items(list(payload.get("action_queue") or []))
    filtered["content_tasks"] = _mvp_filter_action_items(list(payload.get("content_tasks") or []))
    filtered["mvp_mode_active"] = MVP_MODE_ACTIVE
    filtered["mvp_allowed_action_types"] = sorted(MVP_ALLOWED_ACTION_TYPES)
    analytics_status = dict(payload.get("analytics_status") or {})
    analytics_status["mvp_mode_active"] = MVP_MODE_ACTIVE
    analytics_status["mvp_allowed_action_types"] = sorted(MVP_ALLOWED_ACTION_TYPES)
    filtered["analytics_status"] = analytics_status
    return filtered
SYSTEM_OWNED_FEEDBACK_FIELDS = {
    "category",
    "priority",
    "page_url",
    "page_title",
    "summary",
    "details",
    "desired_outcome",
    "recommended_fix",
    "automation_key",
    "auto_generated",
    "source_report_slug",
    "source_report_date",
    "source_insight",
    "section_name",
    "before_state",
    "after_state",
    "expected_impact",
    "confidence",
    "requires_approval",
    "suggested_action_type",
    "suggested_action_value",
    "evidence",
    "confidence_basis",
    "execution_eligibility",
    "target_region",
    "verification_requirements",
    "ga4_trust_status",
    "primary_lead_event",
    "conversion_weight_enabled",
    "execution_reason",
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _state_dir(settings: Settings) -> Path:
    return settings.website_ops_root / "state"


def _run_state_path(settings: Settings) -> Path:
    return _state_dir(settings) / "website_ops_run_state.json"


def _default_mode_run_state(mode: str) -> dict[str, str]:
    normalized_mode = mode if mode in RUN_MODES else "daily"
    return {
        "mode": normalized_mode,
        "status": "idle",
        "run_date": "",
        "trigger": "",
        "last_started_at": "",
        "last_completed_at": "",
        "last_successful_date": "",
        "last_error": "",
        "attempt_count": "0",
        "recovery_status": "",
        "outcome_status": "",
        "outcome_message": "",
        "expected_output": "",
        "actual_output": "",
        "production_delta_count": "0",
        "last_stage": "",
        "next_operation": "",
        "failure_stage": "",
    }


def load_website_ops_run_state(settings: Settings) -> dict[str, Any]:
    _ensure_storage(settings)
    path = _run_state_path(settings)
    payload: dict[str, Any] = {}
    if path.exists():
        try:
            payload = json.loads(path.read_text())
        except json.JSONDecodeError:
            payload = {}
    runs_payload = payload.get("runs") if isinstance(payload.get("runs"), dict) else {}
    runs: dict[str, dict[str, str]] = {}
    for mode in RUN_MODES:
        raw = runs_payload.get(mode) if isinstance(runs_payload, dict) else {}
        merged = _default_mode_run_state(mode)
        if isinstance(raw, dict):
            for key in merged:
                value = str(raw.get(key, "") or "").strip()
                if key == "status" and value not in RUN_STATUSES:
                    continue
                merged[key] = value
        runs[mode] = merged
    return {
        "runs": runs,
        "updated_at": str(payload.get("updated_at", "") or "").strip(),
    }


def get_website_ops_run_state(settings: Settings, mode: str = "daily") -> dict[str, str]:
    state = load_website_ops_run_state(settings)
    return dict(state["runs"].get(mode, _default_mode_run_state(mode)))


def write_website_ops_run_state(settings: Settings, mode: str, updates: Mapping[str, Any]) -> dict[str, str]:
    normalized_mode = mode if mode in RUN_MODES else "daily"
    state = load_website_ops_run_state(settings)
    current = dict(state["runs"].get(normalized_mode, _default_mode_run_state(normalized_mode)))
    for key, value in updates.items():
        if key not in current:
            continue
        cleaned = str(value or "").strip()
        if key == "status" and cleaned not in RUN_STATUSES:
            continue
        current[key] = cleaned
    state["runs"][normalized_mode] = current
    state["updated_at"] = _utc_now().isoformat()
    path = _run_state_path(settings)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, sort_keys=True))
    return current


def website_ops_run_is_due(settings: Settings, mode: str = "daily", *, today: date | None = None) -> bool:
    normalized_mode = mode if mode in RUN_MODES else "daily"
    current_date = today or date.today()
    current_day = current_date.isoformat()
    state = get_website_ops_run_state(settings, normalized_mode)
    if state.get("status") in {"queued", "running"} and state.get("run_date") == current_day:
        return False
    try:
        last_successful = date.fromisoformat(str(state.get("last_successful_date", "")))
    except ValueError:
        return True
    if normalized_mode == "daily":
        return last_successful != current_date
    if normalized_mode == "weekly":
        return last_successful.isocalendar()[:2] != current_date.isocalendar()[:2]
    return (last_successful.year, last_successful.month) != (
        current_date.year,
        current_date.month,
    )


def _config(settings: Settings) -> website_ops.WebsiteOpsConfig:
    root = settings.website_ops_root
    return website_ops.load_config(
        overrides={
            "website_ops_root": root,
            "daily_reports_dir": root / "reports" / "daily",
            "feedback_dir": root / "feedback",
            "report_title": "Anata Website Ops Daily Report",
        }
    )


def _ensure_storage(settings: Settings) -> None:
    root = settings.website_ops_root
    (root / "reports" / "daily").mkdir(parents=True, exist_ok=True)
    (root / "reports" / "weekly").mkdir(parents=True, exist_ok=True)
    (root / "reports" / "monthly").mkdir(parents=True, exist_ok=True)
    (root / "feedback").mkdir(parents=True, exist_ok=True)
    (root / "backups").mkdir(parents=True, exist_ok=True)
    (root / "state").mkdir(parents=True, exist_ok=True)
    (root / "emails").mkdir(parents=True, exist_ok=True)


def discover_website_ops_urls(settings: Settings) -> tuple[str, ...]:
    """Discover the public marketing-site scope from its sitemap.

    The explicit URL setting remains a fail-safe only. App, preview, booking,
    report, and tokenized paths never enter the marketing-site scope.
    """

    configured_urls = tuple(getattr(settings, "website_ops_site_urls", ()) or ())
    configured_host = (
        (urlparse(str(configured_urls[0])).hostname or "").lower()
        if configured_urls
        else ""
    )
    sitemap_url = str(
        getattr(settings, "website_ops_sitemap_url", "")
        or os.getenv("WEBSITE_OPS_SITEMAP_URL", "")
        or (f"https://{configured_host}/sitemap.xml" if configured_host else "")
        or "https://anatainc.com/sitemap.xml"
    ).strip()
    allowed_host = str(
        getattr(settings, "website_ops_allowed_host", "")
        or os.getenv("WEBSITE_OPS_ALLOWED_HOST", "")
        or configured_host
        or "anatainc.com"
    ).strip().lower()
    excluded_prefixes = (
        "/book",
        "/brand",
        "/preview",
        "/x/",
        "/tools/advertising-audit/results/",
        "/tools/fulfillment-rate-sheet/results",
    )

    def _allowed(value: str) -> bool:
        parsed = urlparse(value)
        path = parsed.path or "/"
        return (
            parsed.scheme in {"http", "https"}
            and (parsed.hostname or "").lower() == allowed_host
            and not any(path == prefix or path.startswith(prefix) for prefix in excluded_prefixes)
        )

    discovered: list[str] = []
    try:
        request = urllib.request.Request(
            sitemap_url,
            headers={"User-Agent": "anata-website-ops/2.0"},
        )
        with urllib.request.urlopen(request, timeout=20) as response:
            root = ET.fromstring(response.read())
        for node in root.iter():
            if node.tag.rsplit("}", 1)[-1] != "loc" or not node.text:
                continue
            value = node.text.strip()
            if _allowed(value):
                discovered.append(value)
    except (OSError, ValueError, ET.ParseError):
        discovered = []

    if not discovered:
        discovered = [
            str(value).strip()
            for value in configured_urls
            if _allowed(str(value).strip())
        ]

    unique = sorted(set(discovered), key=lambda value: (urlparse(value).path != "/", urlparse(value).path))
    return tuple(unique)


def _report_change_fingerprint(report: Mapping[str, Any]) -> str:
    def _stable_records(values: Any, fields: tuple[str, ...]) -> list[dict[str, Any]]:
        records = []
        for value in values or []:
            if not isinstance(value, Mapping):
                continue
            records.append(
                {field: value.get(field) for field in fields if value.get(field) not in (None, "")}
            )
        return sorted(records, key=lambda item: json.dumps(item, sort_keys=True, default=str))

    stable_payload = {
        "status": report.get("status"),
        "issues": _stable_records(
            report.get("issues"),
            ("page_url", "url", "issue_type", "category", "priority", "status", "evidence"),
        ),
        "action_queue": _stable_records(
            report.get("action_queue"),
            (
                "automation_key",
                "page_url",
                "action_type",
                "before_state",
                "after_state",
                "status",
                "execution_eligibility",
            ),
        ),
        "executed_actions": _stable_records(
            report.get("executed_actions"),
            ("feedback_id", "page_url", "action_type", "verification_status"),
        ),
        "analytics_status": {
            key: value
            for key, value in dict(report.get("analytics_status") or {}).items()
            if key in {"search_console", "ga4", "ga4_trust_status", "primary_lead_event"}
        },
        "support_requests": sorted(
            str(value).strip()
            for value in report.get("support_requests", []) or []
            if str(value).strip()
        ),
        "program_plan": {
            "current": {
                key: value
                for key, value in dict((report.get("program_plan") or {}).get("current") or {}).items()
                if key in {"title", "state", "work_type", "target", "next_operation", "needs_david"}
            },
            "next": _stable_records(
                (report.get("program_plan") or {}).get("next"),
                ("title", "state", "work_type", "target", "next_operation", "needs_david"),
            ),
        },
        "indexing_summary": dict((report.get("indexing_inventory") or {}).get("summary") or {}),
        "operations_summary": dict(report.get("operations_summary") or {}),
    }
    return hashlib.sha256(
        json.dumps(stable_payload, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()


def _build_operations_summary(report: Mapping[str, Any]) -> dict[str, Any]:
    """Explain the complete candidate funnel, including work that did not execute."""

    has_durable_ledger = bool((report.get("candidate_ledger") or {}).get("candidates"))
    ledger = dict(report.get("candidate_ledger") or {})
    if not ledger.get("candidates"):
        legacy_candidates = build_candidates(report)
        ledger = {
            "summary": candidate_summary(legacy_candidates),
            "lanes": lane_registry(legacy_candidates),
            "candidates": legacy_candidates,
        }
    ledger_summary = dict(ledger.get("summary") or {})
    states = dict(ledger_summary.get("by_state") or {})
    crawl = dict((report.get("crawl_verification") or {}).get("summary") or {})
    query_intelligence = dict(report.get("query_intelligence") or {})
    query = dict(query_intelligence.get("summary") or {})
    article = dict(query_intelligence.get("article_pipeline") or {})
    content_strategy = dict(report.get("content_strategy") or {})
    content_summary = dict(content_strategy.get("summary") or {})
    content_next = dict(content_strategy.get("next_operation") or {})
    queue = [dict(item) for item in report.get("action_queue", []) or []]
    content = [dict(item) for item in report.get("content_tasks", []) or []]
    executed = [dict(item) for item in report.get("executed_actions", []) or []]
    auto_ready = sum(
        1 for item in queue if str(item.get("execution_eligibility", "")) == "auto_execute"
    )
    review_required = len(queue) - auto_ready
    suggestion_only = sum(
        1
        for item in content
        if str(item.get("action_type", "")) in {"inject_faq_block", "expand_service_page_section"}
    )
    deferred_reasons = [
        {
            "reason": "Crawler evidence still needs rendered-page or repository proof.",
            "count": int(crawl.get("pending_warnings", 0) or 0),
        },
        {
            "reason": "Crawler warnings were disproved by current production evidence.",
            "count": int(crawl.get("disproved_warnings", 0) or 0),
        },
        {
            "reason": "Crawler warnings were classified as non-remediation noise.",
            "count": int(crawl.get("noise_warnings", 0) or 0),
        },
        {
            "reason": "Query clusters remain hypotheses and cannot authorize a change.",
            "count": int(query.get("hypothesis_clusters", 0) or 0),
        },
        {
            "reason": "Intent ownership conflicts block publishing.",
            "count": int(query.get("ownership_conflicts", 0) or 0),
        },
        {
            "reason": "FAQ and page-expansion ideas are suggestion-only until deterministic executors ship.",
            "count": suggestion_only,
        },
    ]
    deferred_reasons = [item for item in deferred_reasons if item["count"]]
    return {
        "observed_candidates": (
            int(ledger_summary.get("total_candidates", 0) or 0)
            if has_durable_ledger
            else (
                int(crawl.get("confirmed_warnings", 0) or 0)
                + int(crawl.get("pending_warnings", 0) or 0)
                + int(query.get("total_clusters", 0) or 0)
            )
        ),
        "validated_candidates": (
            int(states.get("validated", 0) or 0)
            if has_durable_ledger
            else (
                int(crawl.get("confirmed_warnings", 0) or 0)
                + int(query.get("validated_clusters", 0) or 0)
            )
        ),
        "count_basis": "durable_candidates" if has_durable_ledger else "legacy_evidence_observations",
        "queued_actions": int(states.get("queued", len(queue)) or 0),
        "auto_ready_actions": int(ledger_summary.get("ready_candidates", auto_ready) or 0),
        "review_required_actions": review_required,
        "executed_actions": int(states.get("completed", len(executed)) or 0),
        "content_tasks": len(content),
        "article_pipeline_status": str(article.get("status", "unavailable") or "unavailable"),
        "article_pipeline_message": str(article.get("message", "") or ""),
        "content_strategy": {
            "total_briefs": int(content_summary.get("total_briefs", 0) or 0),
            "ready_to_publish": int(content_summary.get("ready_to_publish", 0) or 0),
            "researching_sources": int(content_summary.get("researching_sources", 0) or 0),
            "scheduled_for_validation": int(content_summary.get("scheduled_for_validation", 0) or 0),
            "improve_existing": int(content_summary.get("improve_existing", 0) or 0),
            "daily_article_minimum": int(content_strategy.get("daily_article_minimum", 8) or 8),
            "daily_article_target": int(content_strategy.get("daily_article_target", 8) or 8),
            "weekly_article_budget": int(content_strategy.get("weekly_article_budget", 56) or 56),
            "next_topic": str(content_next.get("topic", "") or ""),
            "next_operation": str(content_next.get("next_operation", "") or ""),
            "earliest_publish_date": str(content_next.get("earliest_publish_date", "") or ""),
            "drilldown_url": "/admin/website-ops/strategy",
        },
        "crawl": crawl,
        "query": query,
        "candidate_states": states,
        "candidate_drilldown_url": "/admin/website-ops/candidates",
        "daily_action_portfolio": dict(report.get("daily_action_portfolio") or {}),
        "deferred_reasons": deferred_reasons,
        "execution_coverage": [
            {
                "lane": str(item.get("label", "")),
                "lane_id": str(item.get("lane_id", "")),
                "status": str(item.get("executor_status", "")),
                "candidate_count": int(item.get("candidate_count", 0) or 0),
                "run_budget": int(item.get("run_budget", 0) or 0),
                "concurrency": int(item.get("concurrency", 0) or 0),
                "drilldown_url": (
                    "/admin/website-ops/candidates?lane="
                    + str(item.get("lane_id", ""))
                ),
            }
            for item in ledger.get("lanes", []) or []
        ],
    }


def _notification_state_path(settings: Settings) -> Path:
    return _state_dir(settings) / "website_ops_notification_state.json"


def _load_notification_state(settings: Settings) -> dict[str, Any]:
    path = _notification_state_path(settings)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_email_delivery(settings: Settings, payload: Mapping[str, Any]) -> None:
    _ensure_storage(settings)
    path = settings.website_ops_root / "emails" / "deliveries.jsonl"
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(dict(payload), sort_keys=True, default=str) + "\n")


def send_website_ops_report_email(
    settings: Settings,
    *,
    mode: str,
    report: Mapping[str, Any],
    force: bool = False,
) -> dict[str, Any]:
    """Send a daily operations brief plus weekly and monthly summaries."""

    recipients = tuple(getattr(settings, "website_ops_report_email_to", ()) or ())
    if not recipients:
        recipients = tuple(
            value.strip()
            for value in os.getenv("WEBSITE_OPS_REPORT_EMAIL_TO", "david@anatainc.com").split(",")
            if value.strip()
        )
    fingerprint = _report_change_fingerprint(report)
    state = _load_notification_state(settings)
    previous = str(state.get(f"{mode}_fingerprint", "") or "")
    changed = fingerprint != previous
    should_send = bool(force or changed)
    result = {
        "attempted": False,
        "sent": False,
        "changed": changed,
        "reason": "",
        "provider_message_id": "",
    }
    resend = ResendClient(settings)
    if not recipients or not resend.is_configured(
        from_address=str(getattr(settings, "website_ops_email_from", "") or "")
    ):
        result["reason"] = "email_not_configured"
        return result
    if not should_send:
        result["reason"] = "unchanged"
        return result

    issues = list(report.get("issues") or [])
    executed = list(report.get("executed_actions") or [])
    support_requests = list(
        dict.fromkeys(
            str(value).strip()
            for value in report.get("support_requests", []) or []
            if str(value).strip()
        )
    )
    priority_counts = dict(report.get("issue_counts_by_priority") or {})
    change_lines = [
        "- "
        + " | ".join(
            value
            for value in (
                str(item.get("action_type", "")).replace("_", " ").title(),
                str(item.get("page_url", "")).strip(),
                str(item.get("verification_status", "")).replace("_", " ").title(),
            )
            if value
        )
        for item in executed
    ]
    if not change_lines:
        change_lines = ["- No production SEO changes were applied in this cycle."]
    todo_lines = [f"- {item}" for item in support_requests]
    if not todo_lines:
        todo_lines = ["- Nothing requires your attention today."]
    program_plan = dict(report.get("program_plan") or {})
    current_work = dict(program_plan.get("current") or {})
    next_work = [dict(item) for item in list(program_plan.get("next") or []) if isinstance(item, Mapping)]
    work_lines = []
    if current_work:
        work_lines.append(
            "- NOW | "
            + " | ".join(
                value
                for value in (
                    str(current_work.get("title", "")).strip(),
                    str(current_work.get("state", "")).strip(),
                    str(current_work.get("next_operation", "")).strip(),
                )
                if value
            )
        )
    work_lines.extend(
        "- NEXT | "
        + " | ".join(
            value
            for value in (
                str(item.get("title", "")).strip(),
                str(item.get("state", "")).strip(),
            )
            if value
        )
        for item in next_work[:4]
    )
    if not work_lines:
        work_lines = ["- Run the daily sweep to generate the next source-backed work plan."]
    operations = dict(report.get("operations_summary") or {})
    content_strategy = dict(operations.get("content_strategy") or {})
    if content_strategy.get("next_topic"):
        work_lines.append(
            "- CONTENT | "
            + " | ".join(
                value
                for value in (
                    str(content_strategy.get("next_topic", "")).strip(),
                    str(content_strategy.get("next_operation", "")).strip(),
                    (
                        "Earliest publish "
                        + str(content_strategy.get("earliest_publish_date", "")).strip()
                        if content_strategy.get("earliest_publish_date")
                        else ""
                    ),
                )
                if value
            )
        )
    deferred_lines = [
        f"- {int(item.get('count', 0) or 0)} | {str(item.get('reason', '')).strip()}"
        for item in operations.get("deferred_reasons", []) or []
        if int(item.get("count", 0) or 0)
    ]
    if not deferred_lines:
        deferred_lines = ["- No candidates were deferred in this cycle."]
    subject = (
        f"Website Ops {mode}: {len(executed)} changed, "
        f"{int(operations.get('auto_ready_actions', 0) or 0)} ready, "
        f"{len(support_requests)} for you"
    )
    text = "\n".join(
        [
            f"Anata Website Ops {mode.title()} Report",
            "",
            f"Status: {report.get('status', 'unknown')}",
            f"Pages reviewed: {report.get('pages_reviewed', 0)}",
            f"Open findings: {len(issues)}",
            f"Automated corrections: {len(executed)}",
            f"Candidates observed: {int(operations.get('observed_candidates', 0) or 0)}",
            f"Candidates validated: {int(operations.get('validated_candidates', 0) or 0)}",
            f"Actions ready to run: {int(operations.get('auto_ready_actions', 0) or 0)}",
            f"Actions requiring review: {int(operations.get('review_required_actions', 0) or 0)}",
            f"Content briefs: {int(content_strategy.get('total_briefs', 0) or 0)}",
            f"Articles ready: {int(content_strategy.get('ready_to_publish', 0) or 0)}",
            f"Article daily minimum: {int(content_strategy.get('daily_article_minimum', 8) or 8)}",
            f"Article daily target: {int(content_strategy.get('daily_article_target', 8) or 8)}",
            (
                "Priority: "
                + ", ".join(
                    f"{key} {value}" for key, value in sorted(priority_counts.items())
                )
            ),
            "",
            "Changes completed:",
            *change_lines,
            "",
            "Why other work did not run:",
            *deferred_lines,
            "",
            "Your to-do list:",
            *todo_lines,
            "",
            "What Agent is working on next:",
            *work_lines,
            "",
            "Review the evidence and full report:",
            "https://agent.anatainc.com/admin/website-ops/reports/latest",
            "Content strategy:",
            "https://agent.anatainc.com/admin/website-ops/strategy",
        ]
    )
    result["attempted"] = True
    try:
        message_id = resend.send_message(
            to=recipients,
            subject=subject,
            text=text,
            idempotency_key=(
                f"website-ops-{mode}-{_utc_now().date().isoformat()}-{fingerprint[:16]}"
            ),
            from_address=str(getattr(settings, "website_ops_email_from", "") or ""),
        )
        result.update({"sent": True, "provider_message_id": message_id})
        state[f"{mode}_fingerprint"] = fingerprint
        state[f"{mode}_sent_at"] = _utc_now().isoformat()
        path = _notification_state_path(settings)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(state, indent=2, sort_keys=True))
    except Exception as exc:  # noqa: BLE001 - delivery failure belongs in the run record
        result["reason"] = str(exc)
    _write_email_delivery(
        settings,
        {
            **result,
            "mode": mode,
            "fingerprint": fingerprint,
            "created_at": _utc_now().isoformat(),
            "recipients": list(recipients),
        },
    )
    return result


def send_website_ops_failure_email(settings: Settings, *, mode: str, error: str) -> dict[str, Any]:
    recipients = tuple(getattr(settings, "website_ops_report_email_to", ()) or ())
    if not recipients:
        recipients = tuple(
            value.strip()
            for value in os.getenv("WEBSITE_OPS_REPORT_EMAIL_TO", "david@anatainc.com").split(",")
            if value.strip()
        )
    resend = ResendClient(settings)
    result = {"attempted": False, "sent": False, "provider_message_id": "", "reason": ""}
    if not recipients or not resend.is_configured(
        from_address=str(getattr(settings, "website_ops_email_from", "") or "")
    ):
        result["reason"] = "email_not_configured"
        return result
    result["attempted"] = True
    try:
        message_id = resend.send_message(
            to=recipients,
            subject=f"Website Ops {mode} failed",
            text=(
                f"The {mode} Website Ops run failed.\n\n"
                f"Error: {error}\n\n"
                "Review the run at https://agent.anatainc.com/admin/website-ops"
            ),
            idempotency_key=(
                f"website-ops-failure-{mode}-"
                f"{hashlib.sha256(error.encode('utf-8')).hexdigest()[:24]}"
            ),
            from_address=str(getattr(settings, "website_ops_email_from", "") or ""),
        )
        result.update({"sent": True, "provider_message_id": message_id})
    except Exception as exc:  # noqa: BLE001
        result["reason"] = str(exc)
    _write_email_delivery(
        settings,
        {
            **result,
            "mode": mode,
            "failure": True,
            "created_at": _utc_now().isoformat(),
            "recipients": list(recipients),
        },
    )
    return result


def _feedback_status(value: str) -> str:
    normalized = re.sub(r"[^a-z]+", "-", str(value or "").strip().lower()).strip("-")
    return normalized or "new"


def _feedback_status_label(value: str) -> str:
    labels = {
        "new": "Needs review",
        "approved": "Approved to run",
        "in-progress": "Running",
        "done": "Completed",
        "rejected": "Rejected",
        "error": "Failed",
    }
    return labels.get(_feedback_status(value), _feedback_status(value).replace("-", " ").title())


def _humanize_label(value: str) -> str:
    cleaned = re.sub(r"[_\-]+", " ", str(value or "").strip())
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned.title() if cleaned else ""


def _extract_report_metadata(text: str, path: Path) -> dict[str, str]:
    title_match = re.search(r"^#\s+(.+)$", text, re.MULTILINE)
    date_match = re.search(r"^Date:\s*(.+)$", text, re.MULTILINE)
    scope_match = re.search(r"^Scope:\s*(.+)$", text, re.MULTILINE)
    title = title_match.group(1).strip() if title_match else path.stem.replace("-", " ").title()
    excerpt = ""
    for chunk in text.split("\n\n"):
        stripped = " ".join(line.strip() for line in chunk.splitlines() if line.strip())
        if stripped and not stripped.startswith("#") and not re.match(r"^(Date|Generated|Scope):", stripped):
            excerpt = stripped[:220]
            break
    return {
        "title": title,
        "date": date_match.group(1).strip() if date_match else "",
        "scope": scope_match.group(1).strip() if scope_match else "",
        "excerpt": excerpt,
    }


def _report_entries(settings: Settings, *, mode: str | None = None) -> list[dict[str, Any]]:
    _ensure_storage(settings)
    root = settings.website_ops_root / "reports"
    candidates = root.rglob("*.md") if mode is None else (root / mode).glob("*.md")
    entries: list[dict[str, Any]] = []
    for path in candidates:
        try:
            text = path.read_text()
        except OSError:
            continue
        metadata = _extract_report_metadata(text, path)
        json_path = path.with_suffix(".json")
        artifact_mtime = max(
            path.stat().st_mtime,
            json_path.stat().st_mtime if json_path.exists() else 0,
        )
        generated_epoch = 0.0
        if json_path.exists():
            try:
                generated_at = str(
                    json.loads(json_path.read_text()).get("generated_at", "") or ""
                )
                generated = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
                if generated.tzinfo is None:
                    generated = generated.replace(tzinfo=timezone.utc)
                generated_epoch = generated.timestamp()
            except (OSError, ValueError, json.JSONDecodeError):
                generated_epoch = 0.0
        entries.append(
            {
                "path": path,
                "mode": path.parent.name,
                "slug": path.stem,
                "title": metadata["title"],
                "date": metadata["date"],
                "scope": metadata["scope"],
                "excerpt": metadata["excerpt"],
                "html_path": path.with_suffix(".html"),
                "modified": datetime.fromtimestamp(artifact_mtime).astimezone().strftime("%Y-%m-%d %H:%M %Z"),
                "_artifact_mtime": artifact_mtime,
                "_generated_epoch": generated_epoch,
            }
        )
    return sorted(
        entries,
        key=lambda item: (
            item["_generated_epoch"],
            item["_artifact_mtime"],
        ),
        reverse=True,
    )


def _report_payload(entry: dict[str, Any]) -> dict[str, Any]:
    json_path = Path(entry["path"]).with_suffix(".json")
    if not json_path.exists():
        return {}
    try:
        return json.loads(json_path.read_text())
    except json.JSONDecodeError:
        return {}


def latest_report_entry(settings: Settings) -> dict[str, Any] | None:
    entries = _report_entries(settings)
    return entries[0] if entries else None


def latest_report_payload(settings: Settings) -> dict[str, Any]:
    entry = latest_report_entry(settings)
    return _report_payload(entry) if entry else {}


def website_ops_operating_state(
    settings: Settings,
    *,
    now: datetime | None = None,
    max_age_hours: int = 36,
) -> dict[str, Any]:
    """Return the canonical evidence-backed operating state used by every surface."""

    now = now or _utc_now()
    report = latest_report_payload(settings)
    analytics = dict(report.get("analytics_status") or {})
    generated_at = str(report.get("generated_at", "") or "").strip()
    age_hours: float | None = None
    if generated_at:
        try:
            generated = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
            if generated.tzinfo is None:
                generated = generated.replace(tzinfo=timezone.utc)
            age_hours = max(0.0, (now - generated.astimezone(timezone.utc)).total_seconds() / 3600)
        except ValueError:
            age_hours = None
    search_console_ready = analytics.get("search_console") is True
    ga4_ready = analytics.get("ga4") is True
    evidence_fresh = age_hours is not None and age_hours <= max_age_hours
    blockers = [
        str(value).strip()
        for value in analytics.get("notes", []) or []
        if str(value).strip()
    ]
    if not report:
        blockers.append("Website Ops has not completed an evidence-backed report yet.")
    elif not evidence_fresh:
        blockers.append("The latest decision-data evidence is stale.")
    if not search_console_ready and not any("Search Console" in item for item in blockers):
        blockers.append("Search Console did not pass the latest live collection.")
    if not ga4_ready and not any("GA4" in item for item in blockers):
        blockers.append("GA4 did not pass the latest live collection.")
    ready = bool(report and evidence_fresh and search_console_ready and ga4_ready)
    return {
        "status": "ready" if ready else "blocked",
        "decision_data": "ready" if ready else "blocked",
        "search_console": "ready" if search_console_ready and evidence_fresh else "blocked",
        "ga4": "ready" if ga4_ready and evidence_fresh else "blocked",
        "evidence_generated_at": generated_at,
        "evidence_age_hours": round(age_hours, 2) if age_hours is not None else None,
        "blockers": list(dict.fromkeys(blockers)),
        "support_requests": list(
            dict.fromkeys(
                str(value).strip()
                for value in report.get("support_requests", []) or []
                if str(value).strip()
            )
        ),
    }


def get_report_entry(settings: Settings, mode: str, slug: str) -> dict[str, Any] | None:
    for entry in _report_entries(settings, mode=mode):
        if entry["slug"] == slug:
            return entry
    return None


def load_feedback_records(settings: Settings) -> list[dict[str, Any]]:
    _ensure_storage(settings)
    config = _config(settings)
    records = website_ops.load_feedback_entries(config=config)
    normalized: list[dict[str, Any]] = []
    for record in records:
        item = dict(record)
        item["feedback_id"] = item.get("feedback_id") or Path(str(item.get("_path", ""))).stem
        item["status"] = _feedback_status(str(item.get("status", "")))
        normalized.append(item)
    normalized.sort(
        key=lambda item: (
            str(item.get("submitted_at") or item.get("recorded_at") or ""),
            str(item.get("feedback_id") or ""),
        ),
        reverse=True,
    )
    return normalized


def get_feedback_record(settings: Settings, feedback_id: str) -> dict[str, Any] | None:
    for record in load_feedback_records(settings):
        if str(record.get("feedback_id")) == str(feedback_id):
            return record
    return None


def _automation_key(item: Mapping[str, Any]) -> str:
    raw = "||".join(
        [
            str(item.get("page_url", "")).strip(),
            str(item.get("action_type", "")).strip(),
            str(item.get("section_name", "")).strip(),
            str(item.get("after_state", "")).strip(),
        ]
    )
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]
    return f"auto-{digest}"


def _action_item_category(item: Mapping[str, Any]) -> str:
    source = str(item.get("insight_source", "")).lower()
    if "analytics" in source or "ga4" in source:
        return "Conversion"
    return "SEO"


def _action_item_priority(item: Mapping[str, Any]) -> str:
    confidence = str(item.get("confidence", "medium")).lower()
    if confidence == "high":
        return "High"
    return "Medium"


def _action_item_summary(item: Mapping[str, Any]) -> str:
    page_label = str(item.get("page_title") or _short_page_label(str(item.get("page_url", ""))))
    section = str(item.get("section_name", "Page update")).strip() or "Page update"
    return f"Review: {page_label} / {section}"


def _sync_action_queue_feedback(
    settings: Settings,
    action_queue: list[dict[str, Any]],
    existing_records: list[dict[str, Any]],
    *,
    report_slug: str = "",
) -> list[dict[str, Any]]:
    existing_by_key: dict[str, dict[str, Any]] = {}
    for record in existing_records:
        key = str(record.get("automation_key", "")).strip()
        if key and key not in existing_by_key:
            existing_by_key[key] = record
    synced_items: list[dict[str, Any]] = []
    report_date = _utc_now().date().isoformat()
    for item in action_queue:
        synced = dict(item)
        automation_key = _automation_key(item)
        base_payload = {
            "category": _action_item_category(item),
            "priority": _action_item_priority(item),
            "page_url": str(item.get("page_url", "")).strip(),
            "page_title": str(item.get("page_title", "")).strip(),
            "summary": _action_item_summary(item),
            "details": str(item.get("reason", "")).strip(),
            "desired_outcome": str(item.get("after_state", "")).strip(),
            "recommended_fix": str(item.get("expected_impact", "")).strip(),
            "status": "new",
            "action_type": "",
            "action_value": "",
            "target_post_id": "",
            "automation_key": automation_key,
            "auto_generated": True,
            "source_report_slug": report_slug,
            "source_report_date": report_date,
            "source_insight": str(item.get("insight_source", "")).strip(),
            "section_name": str(item.get("section_name", "")).strip(),
            "before_state": str(item.get("before_state", "")).strip(),
            "after_state": str(item.get("after_state", "")).strip(),
            "expected_impact": str(item.get("expected_impact", "")).strip(),
            "confidence": str(item.get("confidence", "")).strip(),
            "requires_approval": bool(item.get("requires_approval")),
            "suggested_action_type": str(item.get("action_type", "")).strip(),
            "suggested_action_value": str(item.get("action_value", "")).strip(),
            "evidence": list(item.get("evidence") or []),
            "confidence_basis": list(item.get("confidence_basis") or []),
            "execution_eligibility": str(item.get("execution_eligibility", "")).strip(),
            "target_region": str(item.get("target_region", "")).strip(),
            "verification_requirements": list(item.get("verification_requirements") or []),
            "ga4_trust_status": str(item.get("ga4_trust_status", "")).strip(),
            "primary_lead_event": str(item.get("primary_lead_event", "")).strip(),
            "conversion_weight_enabled": bool(item.get("conversion_weight_enabled")),
            "execution_reason": str(item.get("execution_reason", "")).strip(),
        }
        existing = existing_by_key.get(automation_key)
        if existing and existing.get("status") == "rejected":
            # A rejection is a durable editorial decision for this exact automation key.
            # A materially different recommendation will receive a different key.
            continue
        if existing and existing.get("status") == "done" and str(existing.get("source_report_date", "")).strip() not in {"", report_date}:
            reopened_payload = dict(base_payload)
            reopened_payload["reopened_from_feedback_id"] = existing.get("feedback_id") or Path(str(existing.get("_path", ""))).stem
            reopened_payload["reopened_reason"] = "recommendation_reappeared"
            record = save_feedback_record(settings, reopened_payload)
            existing_by_key[automation_key] = record
        elif existing:
            preserved = {key: existing.get(key) for key in WORKFLOW_OWNED_FEEDBACK_FIELDS if key in existing}
            updates = dict(base_payload)
            updates.update(preserved)
            record = website_ops.update_feedback_entry(existing, updates)
            record["feedback_id"] = existing.get("feedback_id") or Path(str(existing.get("_path", ""))).stem
        else:
            record = save_feedback_record(settings, base_payload)
            existing_by_key[automation_key] = record
        synced["feedback_id"] = str(record.get("feedback_id", "")).strip()
        synced["feedback_status"] = _feedback_status(str(record.get("status", "") or "new"))
        synced["feedback_status_label"] = _feedback_status_label(str(record.get("status", "") or "new"))
        synced["queue_url"] = f"/admin/website-ops/feedback/{html.escape(synced['feedback_id'], quote=True)}" if synced["feedback_id"] else ""
        synced_items.append(synced)
    return synced_items


def save_feedback_record(settings: Settings, payload: dict[str, Any]) -> dict[str, Any]:
    config = _config(settings)
    entry = {
        "feedback_id": payload.get("feedback_id") or "",
        "category": str(payload.get("category", "")).strip() or "General",
        "priority": str(payload.get("priority", "")).strip() or "Medium",
        "page_url": str(payload.get("page_url", "")).strip(),
        "page_title": str(payload.get("page_title", "")).strip(),
        "summary": str(payload.get("summary", "")).strip() or "Feedback item",
        "details": str(payload.get("details", "")).strip(),
        "desired_outcome": str(payload.get("desired_outcome", "")).strip(),
        "recommended_fix": str(payload.get("recommended_fix", "")).strip(),
        "reporter_name": str(payload.get("reporter_name", "")).strip(),
        "reporter_email": str(payload.get("reporter_email", "")).strip(),
        "status": _feedback_status(str(payload.get("status", "") or "new")),
        "action_type": str(payload.get("action_type", "")).strip(),
        "action_value": str(payload.get("action_value", "")).strip(),
        "target_post_id": str(payload.get("target_post_id", "")).strip(),
        "submitted_at": datetime.now(timezone.utc).isoformat(),
    }
    for key in ("automation_key", "auto_generated", "source_report_slug", "source_insight"):
        if key in payload:
            entry[key] = payload[key]
    for key in (
        "source_report_date",
        "section_name",
        "before_state",
        "after_state",
        "expected_impact",
        "confidence",
        "requires_approval",
        "suggested_action_type",
        "suggested_action_value",
        "evidence",
        "confidence_basis",
        "execution_eligibility",
        "target_region",
        "verification_requirements",
        "ga4_trust_status",
        "primary_lead_event",
        "conversion_weight_enabled",
        "execution_reason",
        "reopened_from_feedback_id",
        "reopened_reason",
    ):
        if key in payload:
            entry[key] = payload[key]
    for key in WORKFLOW_OWNED_FEEDBACK_FIELDS:
        if key in payload:
            entry[key] = payload[key]
    path = website_ops.save_feedback_entry(entry, config=config)
    record = json.loads(path.read_text())
    record["_path"] = str(path)
    record["feedback_id"] = Path(path).stem
    record["status"] = _feedback_status(str(record.get("status", "")))
    return record


def _is_auto_executable_action(action_type: str, execution_eligibility: str = "") -> bool:
    supported = {
        "meta_update",
        "meta_title_update",
        "meta_description_update",
        "canonical_update",
        "publish_blog_article",
    }
    normalized_action = action_type.strip()
    normalized_eligibility = execution_eligibility.strip()
    if normalized_action not in supported:
        return False
    return normalized_eligibility == "auto_execute"


def _record_is_auto_executable(record: Mapping[str, Any]) -> bool:
    return _is_auto_executable_action(
        str(record.get("suggested_action_type", "") or record.get("action_type", "")),
        str(record.get("execution_eligibility", "")),
    )


def _execute_feedback_action(
    settings: Settings,
    record: Mapping[str, Any],
    *,
    config: website_ops.WebsiteOpsConfig,
) -> dict[str, Any]:
    action_type = str(record.get("action_type", "")).strip()
    if action_type in CONTENT_ACTION_TYPES and github_metadata_is_configured():
        return execute_github_article_action(record, config=config)
    if action_type in METADATA_ACTION_TYPES and github_metadata_is_configured():
        return execute_github_metadata_action(record, config=config)
    return website_ops.execute_feedback_action(record, config=config)


def _autofill_review_updates(existing: Mapping[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    updates = dict(payload)
    status = _feedback_status(str(updates.get("status", "")))
    if status != "approved" or str(updates.get("action_type", "")).strip():
        return updates
    suggested_action_type = str(existing.get("suggested_action_type", "")).strip()
    if not _is_auto_executable_action(suggested_action_type, str(existing.get("execution_eligibility", ""))):
        return updates
    updates["action_type"] = suggested_action_type
    suggested_action_value = str(existing.get("suggested_action_value", "")).strip()
    if suggested_action_value and not str(updates.get("action_value", "")).strip():
        updates["action_value"] = suggested_action_value
    if not str(updates.get("target_post_id", "")).strip() and str(existing.get("target_post_id", "")).strip():
        updates["target_post_id"] = str(existing.get("target_post_id", "")).strip()
    return updates


def review_feedback_record(
    settings: Settings,
    feedback_id: str,
    payload: dict[str, Any],
    *,
    reviewer: Mapping[str, Any] | None = None,
) -> WebsiteOpsActionResult:
    existing = get_feedback_record(settings, feedback_id)
    if not existing:
        return WebsiteOpsActionResult(ok=False, message="Feedback record not found.")
    payload = _autofill_review_updates(existing, payload)
    reviewer_label = str(payload.get("reviewer_name", "")).strip()
    if not reviewer_label and reviewer:
        reviewer_label = str(reviewer.get("email") or reviewer.get("name") or "").strip()
    updates = {
        "status": _feedback_status(str(payload.get("status", ""))),
        "reviewer_name": reviewer_label,
        "review_notes": str(payload.get("review_notes", "")).strip(),
        "action_type": str(payload.get("action_type", "")).strip(),
        "action_value": str(payload.get("action_value", "")).strip(),
        "target_post_id": str(payload.get("target_post_id", "")).strip(),
        "reviewed_at": datetime.now(timezone.utc).isoformat(),
    }
    record = website_ops.update_feedback_entry(existing, updates)
    if settings.website_ops_execute_approved and record.get("status") == "approved" and record.get("action_type") and _record_is_auto_executable(record):
        try:
            result = _execute_feedback_action(settings, record, config=_config(settings))
        except website_ops.ExecutionError as exc:
            record = website_ops.update_feedback_entry(
                record,
                {
                    "status": "error",
                    "execution_error": str(exc),
                    "last_execution_at": datetime.now(timezone.utc).isoformat(),
                },
            )
            return WebsiteOpsActionResult(ok=False, message=f"Approved action failed: {exc}", record=record)
        record = website_ops.update_feedback_entry(
            record,
            {
                "status": "done",
                "last_execution_at": result["executed_at"],
                "execution_result": result,
            },
        )
        return WebsiteOpsActionResult(ok=True, message="Approved action executed and verified.", record=record)
    return WebsiteOpsActionResult(ok=True, message="Review saved.", record=record)


def _execute_record(
    settings: Settings,
    config: website_ops.WebsiteOpsConfig,
    record: Mapping[str, Any],
    *,
    require_auto_executable: bool = True,
) -> dict[str, Any] | None:
    if record.get("status") != "approved" or not record.get("action_type"):
        return None
    if not _mvp_action_allowed(str(record.get("action_type", "")).strip()):
        return None
    if require_auto_executable and not _record_is_auto_executable(record):
        return None
    try:
        result = _execute_feedback_action(settings, record, config=config)
    except website_ops.ExecutionError as exc:
        website_ops.update_feedback_entry(
            record,
            {
                "status": "error",
                "execution_error": str(exc),
                "last_execution_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        return None
    website_ops.update_feedback_entry(
        record,
        {
            "status": "done",
            "last_execution_at": result["executed_at"],
            "execution_result": result,
        },
    )
    return result


def execute_approved_website_ops_actions(settings: Settings) -> WebsiteOpsActionResult:
    if not settings.website_ops_execute_approved:
        return WebsiteOpsActionResult(
            ok=False,
            message="Website Ops execution is disabled for this environment.",
            report={"attempted": 0, "executed": 0, "skipped": 0},
        )
    config = _config(settings)
    feedback_entries = _mvp_filter_feedback_records(load_feedback_records(settings))
    candidates = [
        record
        for record in feedback_entries
        if record.get("status") == "approved"
        and str(record.get("action_type", "")).strip()
        and _mvp_action_allowed(str(record.get("action_type", "")).strip())
    ]
    executed_actions: list[dict[str, Any]] = []
    for record in candidates:
        result = _execute_record(settings, config, record, require_auto_executable=False)
        if result:
            executed_actions.append(result)
    attempted = len(candidates)
    executed = len(executed_actions)
    skipped = attempted - executed
    if not attempted:
        message = "No approved Website Ops actions are ready to execute."
    elif skipped:
        message = f"Executed {executed} approved action(s); {skipped} failed or were skipped."
    else:
        message = f"Executed {executed} approved Website Ops action(s)."
    return WebsiteOpsActionResult(
        ok=skipped == 0,
        message=message,
        report={"attempted": attempted, "executed": executed, "skipped": skipped, "executed_actions": executed_actions},
    )


def run_website_ops(settings: Settings, *, mode: str = "daily") -> WebsiteOpsActionResult:
    config = _config(settings)
    has_baseline = bool(_report_entries(settings))
    feedback_entries = load_feedback_records(settings)
    visible_feedback_entries = _mvp_filter_feedback_records(feedback_entries)
    executed_actions: list[dict[str, Any]] = []
    if settings.website_ops_execute_approved and has_baseline:
        approved_candidates = [
            {
                **dict(record),
                "action_type": str(
                    record.get("action_type", "")
                    or record.get("suggested_action_type", "")
                ),
            }
            for record in visible_feedback_entries
            if str(record.get("status", "")).strip().lower() == "approved"
        ]
        bounded_approved, _ = select_bounded_actions(approved_candidates)
        for record in bounded_approved:
            result = _execute_record(settings, config, record)
            if result:
                executed_actions.append(result)
        feedback_entries = load_feedback_records(settings)
        visible_feedback_entries = _mvp_filter_feedback_records(feedback_entries)

    report_title = {
        "daily": "Anata Website Ops Daily Report",
        "weekly": "Anata Website Ops Weekly Report",
        "monthly": "Anata Website Ops Monthly Report",
    }[mode]
    output_dir = settings.website_ops_root / "reports" / mode
    monitored_urls = discover_website_ops_urls(settings)
    pipeline = website_ops.run_daily_report_pipeline(
        list(monitored_urls),
        config=config,
        output_dir=output_dir,
        feedback_entries=visible_feedback_entries,
        title=report_title,
        report_type=f"website_ops_{mode}",
        scope=f"agent-admin {mode} sweep",
        notes=[
            f"Run mode: {mode}.",
            f"Monitored URLs: {len(monitored_urls)}.",
            "Scope source: production sitemap with marketing-site host restrictions.",
            f"Feedback loaded: {len(feedback_entries)}.",
            f"Changes applied: {len(executed_actions)}.",
        ],
        report_date=datetime.now(timezone.utc).date().isoformat(),
        executed_actions=executed_actions,
    )
    enriched_report = dict(pipeline["report"])
    # Persist a truthful sitemap-backed inventory before slower analytics,
    # article generation, or external publishing can fail. Later enrichment
    # replaces this checkpoint with the fully joined intent projection.
    checkpoint_indexing_inventory = load_indexing_inventory(settings.website_ops_root)
    checkpoint_crawl_inventory = load_crawl_inventory(settings.website_ops_root)
    checkpoint_query_intelligence = dict(
        enriched_report.get("query_intelligence") or {}
    )
    enriched_report["production_inventory"] = build_production_inventory(
        sitemap_urls=monitored_urls,
        crawl_inventory=checkpoint_crawl_inventory,
        indexing_inventory=checkpoint_indexing_inventory,
        intent_coverage=dict(
            checkpoint_query_intelligence.get("intent_coverage") or {}
        ),
    )
    website_ops.write_daily_report_artifacts(
        enriched_report,
        output_dir=output_dir,
        config=config,
    )
    enriched_report.update(
        build_autonomy_overlay(
            settings=settings,
            report=enriched_report,
            observations=list(pipeline.get("observations") or []),
            feedback_entries=visible_feedback_entries,
            run_mode=mode,
        )
    )
    enriched_report = _mvp_filter_report_payload(enriched_report)
    enriched_report["automation_mode"] = (
        "validated_autopush" if has_baseline else "baseline_report_only"
    )
    enriched_report["action_queue"] = _sync_action_queue_feedback(
        settings,
        list(enriched_report.get("action_queue") or []),
        visible_feedback_entries,
        report_slug=_slugify_text(report_title),
    )
    bounded_actions, deferred_actions = select_bounded_actions(
        list(enriched_report.get("action_queue") or [])
    )
    enriched_report["action_queue"] = bounded_actions + deferred_actions
    bounded_feedback_ids = {
        str(item.get("feedback_id", "")).strip()
        for item in bounded_actions
        if str(item.get("feedback_id", "")).strip()
    }
    if settings.website_ops_execute_approved and has_baseline:
        current_records = {str(item.get("feedback_id", "")): item for item in _mvp_filter_feedback_records(load_feedback_records(settings))}
        for item in enriched_report["action_queue"]:
            feedback_id = str(item.get("feedback_id", "")).strip()
            record = current_records.get(feedback_id)
            if not record:
                continue
            if feedback_id not in bounded_feedback_ids:
                continue
            if record.get("status") == "new" and _record_is_auto_executable(record):
                record = website_ops.update_feedback_entry(
                    record,
                    {
                        "status": "approved",
                        "action_type": str(record.get("suggested_action_type", "")).strip(),
                        "action_value": str(record.get("suggested_action_value", "")).strip(),
                        "reviewed_at": datetime.now(timezone.utc).isoformat(),
                        "review_notes": "Auto-approved by Website Ops: high-confidence deterministic action.",
                    },
                )
                result = _execute_record(settings, config, record)
                if result:
                    executed_actions.append(result)
        if executed_actions:
            feedback_entries = load_feedback_records(settings)
            visible_feedback_entries = _mvp_filter_feedback_records(feedback_entries)
            enriched_report["executed_actions"] = list(enriched_report.get("executed_actions") or []) + executed_actions
            enriched_report["changes_applied"] = int(enriched_report.get("changes_applied", 0) or 0) + len(executed_actions)
            enriched_report["auto_executed_today"] = len(executed_actions)
            enriched_report["action_queue"] = _sync_action_queue_feedback(
                settings,
                list(enriched_report.get("action_queue") or []),
                visible_feedback_entries,
                report_slug=_slugify_text(report_title),
            )
    indexing_inventory = load_indexing_inventory(settings.website_ops_root)
    crawl_inventory = load_crawl_inventory(settings.website_ops_root)
    crawl_observations = collect_crawl_resource_observations(
        crawl_inventory,
        list(pipeline.get("observations") or []),
    )
    crawl_verification = build_crawl_verification(
        crawl_inventory,
        crawl_observations,
    )
    save_crawl_verification(settings.website_ops_root, crawl_verification)
    enriched_report["indexing_inventory"] = indexing_inventory
    enriched_report["crawl_inventory"] = {
        "generated_at": crawl_inventory.get("generated_at", ""),
        "imports": list(crawl_inventory.get("imports") or []),
        "summary": dict(crawl_inventory.get("summary") or {}),
    }
    enriched_report["crawl_verification"] = crawl_verification
    query_intelligence = dict(enriched_report.get("query_intelligence") or {})
    enriched_report["production_inventory"] = build_production_inventory(
        sitemap_urls=monitored_urls,
        crawl_inventory=crawl_inventory,
        indexing_inventory=indexing_inventory,
        intent_coverage=dict(query_intelligence.get("intent_coverage") or {}),
    )
    run_id = hashlib.sha256(
        (
            f"{mode}:{enriched_report.get('generated_at', '')}:"
            f"{len(monitored_urls)}"
        ).encode("utf-8")
    ).hexdigest()[:24]
    enriched_report["candidate_ledger"] = persist_candidate_ledger(
        settings.website_ops_root,
        candidates=build_candidates(enriched_report),
        run_id=run_id,
    )
    enriched_report["program_plan"] = build_program_plan(
        analytics_status=dict(enriched_report.get("analytics_status") or {}),
        action_queue=list(enriched_report.get("action_queue") or []),
        support_requests=list(enriched_report.get("support_requests") or []),
        indexing_inventory=indexing_inventory,
        crawl_verification=crawl_verification,
    )
    enriched_report["daily_action_portfolio"] = build_daily_action_portfolio(
        action_queue=list(enriched_report.get("action_queue") or []),
        candidate_ledger=dict(enriched_report.get("candidate_ledger") or {}),
    )
    enriched_report["operations_summary"] = _build_operations_summary(enriched_report)
    enriched_report["run_outcome"] = classify_run_outcome(enriched_report)
    artifacts = website_ops.write_daily_report_artifacts(enriched_report, output_dir=output_dir, config=config)
    enriched_report["email_delivery"] = send_website_ops_report_email(
        settings,
        mode=mode,
        report=enriched_report,
    )
    return WebsiteOpsActionResult(
        ok=enriched_report["run_outcome"]["status"] != FAILED_OUTCOME,
        message=str(enriched_report["run_outcome"]["summary"]),
        report=enriched_report,
    )


def _status_chip(value: str) -> str:
    return f'<span class="status-chip status-{html.escape(_feedback_status(value), quote=True)}">{html.escape(_feedback_status_label(value))}</span>'


def _slugify_text(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().lower()).strip("-")
    return slug or "report"


def _summary_chip(label: str, value: Any, *, tone: str = "neutral") -> str:
    return (
        f'<div class="summary-chip summary-{html.escape(tone, quote=True)}">'
        f'<span>{html.escape(label)}</span>'
        f"<strong>{html.escape(str(value))}</strong>"
        "</div>"
    )


def _mini_chip(label: str, value: Any) -> str:
    return (
        '<div class="mini-chip">'
        f'<span>{html.escape(label)}</span>'
        f"<strong>{html.escape(str(value))}</strong>"
        "</div>"
    )


def _short_page_label(value: str) -> str:
    cleaned = re.sub(r"^https?://", "", str(value or "")).strip()
    return cleaned or "Unspecified page"


def _action_source_chip(source: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "-", str(source or "").strip().lower()).strip("-") or "system"
    return f'<span class="source-chip source-{html.escape(normalized, quote=True)}">{html.escape(source or "System")}</span>'


def _analytics_connection_cards(analytics_status: dict[str, Any], *, include_identity: bool = False) -> str:
    notes = [str(item).strip() for item in analytics_status.get("notes", []) if str(item).strip()]
    project_id = str(analytics_status.get("project_id", "") or "").strip()
    client_email = str(analytics_status.get("client_email", "") or "").strip()
    search_console_property = str(analytics_status.get("search_console_property", "") or "").strip()
    ga4_property_id = str(analytics_status.get("ga4_property_id", "") or "").strip()
    ga4_trust_status = str(analytics_status.get("ga4_trust_status", "") or "").strip()
    primary_lead_event = str(analytics_status.get("primary_lead_event", "") or "").strip()
    auto_executable_today = int(analytics_status.get("auto_executable_today", 0) or 0)
    approval_required_today = int(analytics_status.get("approval_required_today", 0) or 0)
    action_type_coverage = list(analytics_status.get("action_type_coverage") or [])
    identity_block = ""
    if include_identity and (project_id or client_email):
        identity_lines = []
        if project_id:
            identity_lines.append(
                f"<div class='meta-pair'><span>Project</span><code>{html.escape(project_id)}</code></div>"
            )
        if client_email:
            identity_lines.append(
                f"<div class='meta-pair'><span>Service account</span><code>{html.escape(client_email)}</code></div>"
            )
        identity_block = f"<div class='identity-grid'>{''.join(identity_lines)}</div>"
    cards = [
        f"""
        <article class="setup-card {'is-connected' if analytics_status.get('search_console') else 'is-blocked'}">
          <div class="row-actions">
            <h3>Search Console</h3>
            <span class="status-pill {'status-ok' if analytics_status.get('search_console') else 'status-warn'}">{'Connected' if analytics_status.get('search_console') else 'Needs setup'}</span>
          </div>
          <p class="lead-sm">{html.escape(next((note for note in notes if 'Search Console' in note), 'Live search query data is available for Website Ops decisions.'))}</p>
          {f"<div class='meta-pair'><span>Property</span><code>{html.escape(search_console_property)}</code></div>" if search_console_property else ""}
          {f"<div class='meta-pair'><span>Freshness</span><strong>{html.escape(str(analytics_status.get('search_console_freshness', 'connected')).replace('-', ' ').title())}</strong></div>" if analytics_status.get('search_console_freshness') else ""}
          {identity_block}
        </article>
        """,
        f"""
        <article class="setup-card {'is-connected' if analytics_status.get('ga4') else 'is-blocked'}">
          <div class="row-actions">
            <h3>GA4</h3>
            <span class="status-pill {'status-ok' if analytics_status.get('ga4') else 'status-warn'}">{'Connected' if analytics_status.get('ga4') else 'Needs setup'}</span>
          </div>
          <p class="lead-sm">{html.escape(next((note for note in notes if 'GA4' in note), 'Landing-page and conversion data is available for Website Ops decisions.'))}</p>
          {f"<div class='meta-pair'><span>Property ID</span><code>{html.escape(ga4_property_id)}</code></div>" if ga4_property_id else ""}
          {f"<div class='meta-pair'><span>Lead Event</span><code>{html.escape(primary_lead_event)}</code></div>" if primary_lead_event else ""}
          {f"<div class='meta-pair'><span>Trust</span><strong>{html.escape(ga4_trust_status.title())}</strong></div>" if ga4_trust_status else ""}
          {identity_block}
        </article>
        """,
    ]
    if include_identity and (action_type_coverage or auto_executable_today or approval_required_today):
        cards.append(
            f"""
            <article class="setup-card is-connected">
              <div class="row-actions">
                <h3>Execution coverage</h3>
                <span class="status-pill status-neutral">Live</span>
              </div>
              <div class="mini-grid">
                {_mini_chip("Auto Execute", auto_executable_today)}
                {_mini_chip("Approval First", approval_required_today)}
                {_mini_chip("Action Types", len(action_type_coverage))}
              </div>
              <p class="lead-sm">{html.escape(', '.join(_humanize_label(item) for item in action_type_coverage) or 'No action types surfaced yet.')}</p>
            </article>
            """
        )
    return "".join(cards)


def _connection_summary_chips(analytics_status: dict[str, Any]) -> str:
    return "".join(
        [
            _summary_chip("Search Console", "Connected" if analytics_status.get("search_console") else "Needs Setup", tone="good" if analytics_status.get("search_console") else "warn"),
            _summary_chip("GA4", "Connected" if analytics_status.get("ga4") else "Needs Setup", tone="good" if analytics_status.get("ga4") else "warn"),
        ]
    )


def _team_help_cards(support_requests: list[str], analytics_status: dict[str, Any]) -> str:
    analytics_notes = {str(item).strip() for item in analytics_status.get("notes", []) if str(item).strip()}
    team_items = [str(item).strip() for item in support_requests if str(item).strip() and str(item).strip() not in analytics_notes]
    if not team_items:
        return """
        <article class="task-card">
          <div class="row-actions">
            <h3>No manual blockers</h3>
            <span class="status-pill status-ok">Clear</span>
          </div>
            <p class="muted">Website Ops does not need anything from you based on the latest completed run.</p>
        </article>
        """
    return "".join(
        f"""
        <article class="task-card">
          <div class="row-actions">
            <h3>Action for you</h3>
            <span class="status-pill status-warn">Needed</span>
          </div>
          <p>{html.escape(item)}</p>
        </article>
        """
        for item in team_items[:4]
    )


def _latest_report_panel(entry: dict[str, Any] | None, payload: dict[str, Any]) -> str:
    if not entry:
        return """
        <div class="card stack">
          <h2>Latest report</h2>
          <p class="lead">No report has been generated yet.</p>
        </div>
        """
    status = str(payload.get("status") or entry.get("mode") or "unknown")
    analytics_status = payload.get("analytics_status") if isinstance(payload.get("analytics_status"), dict) else {}
    operational_status = str(analytics_status.get("operational_status", "unknown") or "unknown")
    stats = [
        ("Pages reviewed", payload.get("pages_reviewed", "0"), "neutral"),
        ("Healthy", payload.get("pages_healthy", "0"), "good"),
        ("Needs work", payload.get("pages_with_issues", "0"), "warn" if int(payload.get("pages_with_issues", 0) or 0) else "neutral"),
        ("Issues found", payload.get("issues_found", "0"), "warn" if int(payload.get("issues_found", 0) or 0) else "neutral"),
        ("Technical crawl", status.replace("-", " "), "bad" if status == "needs-attention" else "good"),
        (
            "Ranking operations",
            operational_status.replace("-", " "),
            "good" if operational_status == "operational" else "bad",
        ),
    ]
    return f"""
    <div class="card stack">
      <h2>Latest report</h2>
      {_mvp_mode_banner()}
      <div class="summary-grid">
        {''.join(_summary_chip(label, value, tone=tone) for label, value, tone in stats)}
      </div>
      <div class="button-row">
        <a href="/admin/website-ops/reports/{html.escape(entry['mode'], quote=True)}/{html.escape(entry['slug'], quote=True)}" class="text-link">Open {html.escape(entry['title'])}</a>
      </div>
    </div>
    """


def _mvp_mode_banner() -> str:
    allowed = ", ".join(sorted(MVP_ALLOWED_ACTION_TYPES))
    return (
        "<div class='flash'>"
        "<strong>Autonomous publishing guardrails active.</strong> "
        f"Production-approved action types: {html.escape(allowed)}. "
        "Every change requires evidence, verification, and rollback support."
        "</div>"
    )


def _dashboard_stat_card(title: str, value: int, note: str, href: str) -> str:
    return (
        '<div class="card stat">'
        f'<p class="eyebrow">{html.escape(title)}</p>'
        f"<strong>{html.escape(str(value))}</strong>"
        f"<p class='muted'>{html.escape(note)}</p>"
        f"<a class='stat-link' href='{html.escape(href, quote=True)}'>View</a>"
        "</div>"
    )


def _issue_help_block() -> str:
    return """
    <details class="help-details">
      <summary aria-label="How to use this form">?</summary>
      <div class="help-copy">
        <p>Use this form when you see a page issue, UX problem, conversion gap, or SEO opportunity that is not already in the queue.</p>
        <p><strong>Examples:</strong> “Shipping page headline is vague.” “Contact page form has no proof.” “AI page needs clearer offer framing.”</p>
      </div>
    </details>
    """


def _system_details_panel(settings: Settings, analytics_status: dict[str, Any]) -> str:
    project_id = str(analytics_status.get("project_id", "") or "").strip()
    client_email = str(analytics_status.get("client_email", "") or "").strip()
    search_console_property = str(analytics_status.get("search_console_property", "") or "").strip()
    ga4_property_id = str(analytics_status.get("ga4_property_id", "") or "").strip()
    return f"""
    <section class="card stack card-muted">
      <p class="eyebrow">System details</p>
      <div class="mini-grid">
        {_mini_chip("Sitemap Seeds", len(settings.website_ops_site_urls))}
        {_mini_chip("Workspace", _humanize_label(settings.website_ops_root.name))}
        {_mini_chip("Search Console Property", search_console_property or "Not set")}
        {_mini_chip("GA4 Property", ga4_property_id or "Not set")}
      </div>
      <details class="system-details">
        <summary>Developer details</summary>
        <div class="identity-grid">
          {f"<div class='meta-pair'><span>Google Project</span><code>{html.escape(project_id)}</code></div>" if project_id else ""}
          {f"<div class='meta-pair'><span>Service Account</span><code>{html.escape(client_email)}</code></div>" if client_email else ""}
        </div>
      </details>
    </section>
    """


def _run_state_notice(state: Mapping[str, Any]) -> tuple[str, str]:
    status = _feedback_status(str(state.get("status", "") or "idle"))
    run_date = str(state.get("run_date", "") or "").strip()
    last_successful_date = str(state.get("last_successful_date", "") or "").strip()
    last_error = str(state.get("last_error", "") or "").strip()
    today = date.today().isoformat()
    if status in {"queued", "running"} and run_date == today:
        return ("neutral", "Daily sweep running")
    if status in {"failed", "failed-outcome"} and run_date == today:
        return ("warn", f"Last daily sweep failed{': ' + last_error if last_error else ''}")
    if last_successful_date == today:
        outcome = str(state.get("outcome_message", "") or "").strip()
        return ("good", outcome or "Daily sweep completed today")
    return ("neutral", "Daily sweep will start automatically when needed")


def _run_state_summary(state: Mapping[str, Any]) -> str:
    tone, text = _run_state_notice(state)
    return _summary_chip("Daily Sweep", text, tone=tone)


def _dashboard_auto_run_script(run_state: Mapping[str, Any]) -> str:
    status = _feedback_status(str(run_state.get("status", "") or "idle"))
    if status not in {"queued", "running"}:
        return ""
    return f"""
    <script>
      (function () {{
        let attempts = 0;
        function poll() {{
          attempts += 1;
          fetch("/admin/api/website-ops/status?mode=daily", {{
            method: "GET",
            headers: {{"Accept": "application/json"}},
            credentials: "same-origin"
          }}).then(function (response) {{
            if (!response.ok) {{
              return null;
            }}
            return response.json();
          }}).then(function (payload) {{
            if (!payload || !payload.details) {{
              return;
            }}
            const details = payload.details;
            if (details.status === "queued" || details.status === "running") {{
              if (attempts < 45) {{
                window.setTimeout(poll, 2000);
              }}
              return;
            }}
            window.location.reload();
          }}).catch(function () {{
            if (attempts < 45) {{
              window.setTimeout(poll, 4000);
            }}
          }});
        }}
        poll();
      }})();
    </script>
    """


def _action_queue_workflow_chip(status: str) -> str:
    normalized = _feedback_status(status)
    tone_map = {
        "new": "warn",
        "approved": "ok",
        "in-progress": "neutral",
        "done": "ok",
        "error": "bad",
        "rejected": "neutral",
    }
    label_map = {
        "new": "Needs review",
        "approved": "Approved to run",
        "in-progress": "Running",
        "done": "Completed",
        "error": "Failed",
        "rejected": "Rejected",
    }
    return f'<span class="status-pill status-{html.escape(tone_map.get(normalized, "neutral"), quote=True)}">{html.escape(label_map.get(normalized, _feedback_status_label(normalized)))}</span>'


def _action_queue_link_label(status: str) -> str:
    normalized = _feedback_status(status)
    if normalized == "approved":
        return "View approved item"
    if normalized == "done":
        return "View completed item"
    if normalized == "error":
        return "View failed item"
    if normalized == "in-progress":
        return "View item"
    return "Open review item"


def _action_queue_cards(action_queue: list[dict[str, Any]]) -> str:
    if not action_queue:
        return "<div class='list-card'><p class='muted'>No action queue generated yet.</p></div>"
    cards = []
    for item in action_queue:
        confidence = str(item.get("confidence", "medium")).strip().lower() or "medium"
        requires_approval = bool(item.get("requires_approval"))
        feedback_status = str(item.get("feedback_status", "new") or "new")
        link_label = _action_queue_link_label(feedback_status)
        evidence = list(item.get("evidence") or [])
        target_region = str(item.get("target_region", "")).strip()
        execution_eligibility = str(item.get("execution_eligibility", "")).strip() or ("approval_required" if requires_approval else "auto_execute")
        ga4_trust_status = str(item.get("ga4_trust_status", "")).strip()
        verification_requirements = list(item.get("verification_requirements") or [])
        cards.append(
            f"""
            <article class="action-card">
              <div class="row-actions">
                {_action_source_chip(str(item.get("insight_source", "System")))}
                <div class="chip-row">
                  {_action_queue_workflow_chip(feedback_status)}
                  <span class="status-pill {'status-warn' if requires_approval else 'status-ok'}">{'Approval required' if requires_approval else 'Auto execute'}</span>
                  <span class="status-pill status-neutral">{html.escape(confidence.title())} confidence</span>
                </div>
              </div>
              <h3>{html.escape(str(item.get("page_title") or _short_page_label(str(item.get("page_url", "")))))}</h3>
              <p class="muted">{html.escape(_short_page_label(str(item.get("page_url", ""))))}</p>
              <div class="mini-grid">
                {_mini_chip("Section", str(item.get("section_name", "Unspecified section")))}
                {_mini_chip("Impact", str(item.get("expected_impact", "Improves performance against the current goal.")))}
                {_mini_chip("Target", target_region or "Page region")}
                {_mini_chip("Execution", _humanize_label(execution_eligibility) or execution_eligibility)}
                {_mini_chip("GA4 Trust", ga4_trust_status.title() if ga4_trust_status else "n/a")}
              </div>
              <div class="diff-grid">
                <div class="diff-block">
                  <p class="eyebrow">Before</p>
                  <p>{html.escape(str(item.get("before_state", "Not captured")))}</p>
                </div>
                <div class="diff-block">
                  <p class="eyebrow">After</p>
                  <p>{html.escape(str(item.get("after_state", "No proposed state")))}</p>
                </div>
              </div>
              <p><strong>Why this matters:</strong> {html.escape(str(item.get("reason", "No rationale supplied.")))}</p>
              {f"<ul class='compact-list'>{''.join(f'<li>{html.escape(str(line))}</li>' for line in evidence[:3])}</ul>" if evidence else ""}
              {f"<p class='muted'><strong>Verification:</strong> {html.escape('; '.join(str(line) for line in verification_requirements))}</p>" if verification_requirements else ""}
              {f"<div class='button-row'><a class='text-link' href='/admin/website-ops/feedback/{html.escape(str(item.get('feedback_id', '')), quote=True)}'>{html.escape(link_label)}</a></div>" if item.get('feedback_id') else ""}
            </article>
            """
        )
    return "".join(cards)


def _insight_snapshot_cards(page_insights: list[dict[str, Any]]) -> str:
    if not page_insights:
        return "<div class='list-card'><p class='muted'>No analytics insights generated yet.</p></div>"
    cards = []
    for item in page_insights:
        top_query = ""
        queries = list(item.get("top_queries") or [])
        if queries:
            top_query = f"<p class='muted'><strong>Top query:</strong> {html.escape(str(queries[0].get('query', '')))}</p>"
        insights = ""
        if item.get("insights"):
            insights = (
                "<ul class='compact-list'>"
                + "".join(f"<li>{html.escape(str(note))}</li>" for note in item.get("insights", [])[:2])
                + "</ul>"
            )
        debug_chips = "".join(
            [
                _mini_chip("Questions", int(item.get("customer_question_count", 0) or 0)),
                _mini_chip("Blueprint", "Yes" if item.get("blueprint_found") else "No"),
                _mini_chip("FAQ Demand", "Yes" if item.get("faq_demand_detected") else "No"),
                _mini_chip("Thin Enough", "Yes" if item.get("page_thin_enough") else "No"),
            ]
        )
        query_seed = ""
        if item.get("query_seed"):
            query_seed = f"<p class='muted'><strong>Query seed:</strong> {html.escape(str(item.get('query_seed', '')))}</p>"
        block_reason = ""
        if item.get("task_block_reason"):
            block_reason = f"<p class='muted'><strong>Task block reason:</strong> {html.escape(str(item.get('task_block_reason', '')))}</p>"
        aeo = item.get("aeo") if isinstance(item.get("aeo"), dict) else {}
        aeo_eligibility = str(aeo.get("technical_eligibility", "unknown"))
        aeo_readiness = str(aeo.get("answer_readiness", "unknown"))
        aeo_reasons = list(aeo.get("technical_blockers") or []) + list(aeo.get("answer_readiness_issues") or [])
        simulated_prompts = list(aeo.get("simulated_coverage_prompts") or [])
        aeo_details = ""
        if aeo:
            reason_items = "".join(
                f"<li>{html.escape(str(reason))}</li>" for reason in aeo_reasons[:5]
            )
            prompt_items = "".join(
                f"<li><strong>{html.escape(str(prompt.get('facet', '')).title())}:</strong> "
                f"{html.escape(str(prompt.get('prompt', '')))}</li>"
                for prompt in simulated_prompts[:6]
            )
            aeo_details = f"""
              <div class="mini-grid">
                {_mini_chip("Answer-engine access", aeo_eligibility.title())}
                {_mini_chip("Structural readiness", aeo_readiness.replace("-", " ").title())}
                {_mini_chip("Observed queries", len(list(aeo.get("observed_queries") or [])))}
                {_mini_chip("Simulated prompts", len(simulated_prompts))}
              </div>
              <details>
                <summary class="text-link">AEO evidence and coverage</summary>
                {f"<ul class='compact-list'>{reason_items}</ul>" if reason_items else "<p class='muted'>No current eligibility or answer-readiness issues.</p>"}
                <p class="eyebrow">Simulated coverage prompts</p>
                <ul class="compact-list">{prompt_items}</ul>
                <p class="muted">{html.escape(str(aeo.get("method_note", "")))}</p>
              </details>
            """
        cards.append(
            f"""
            <article class="insight-card">
              <div class="row-actions">
                <h3>{html.escape(str(item.get("page_title") or _short_page_label(str(item.get("page_url", "")))))}</h3>
                <span class="status-pill status-neutral">Score {html.escape(str(item.get("score"))) if item.get("score") is not None else "Unavailable"}</span>
              </div>
              <p class="muted">{html.escape(_short_page_label(str(item.get("page_url", ""))))}</p>
              <div class="mini-grid">
                {_mini_chip("Bucket", str(item.get("bucket", "hold")).title())}
                {_mini_chip("Impressions", int((item.get("search_console") or {}).get("impressions", 0)) if (item.get("metric_availability") or {}).get("search_console") == "observed" else "Unavailable")}
                {_mini_chip("CTR", f"{round(float((item.get('search_console') or {}).get('ctr', 0) or 0) * 100, 2)}%" if (item.get("metric_availability") or {}).get("search_console") == "observed" else "Unavailable")}
                {_mini_chip("Sessions", int((item.get("ga4") or {}).get("sessions", 0)) if (item.get("metric_availability") or {}).get("ga4") == "observed" else "Unavailable")}
                {_mini_chip("Lead Events", int((item.get("ga4") or {}).get("lead_conversions", 0)) if (item.get("metric_availability") or {}).get("ga4") == "observed" else "Unavailable")}
                {_mini_chip("Trust", str(item.get("ga4_trust_status", "missing")).title())}
              </div>
              {top_query}
              <div class="mini-grid">{debug_chips}</div>
              {query_seed}
              {block_reason}
              {aeo_details}
              {insights}
            </article>
            """
        )
    return "".join(cards)


def _customer_question_cards(questions: list[dict[str, Any]]) -> str:
    if not questions:
        return "<div class='list-card'><p class='muted'>No customer questions extracted yet.</p></div>"
    cards = []
    for item in questions[:8]:
        cards.append(
            f"""
            <article class="list-card">
              <div class="row-actions">
                <span class="status-pill status-neutral">{html.escape(str(item.get("intent", "informational")).title())}</span>
                <span class="muted">{html.escape(str(item.get("frequency", 0)))} mentions</span>
              </div>
              <h3>{html.escape(str(item.get("question", "")))}</h3>
              <p class="muted">{html.escape(_humanize_label(str(item.get("related_service", ""))) or "General")} · {html.escape(str(item.get("source", "gmail")).title())}</p>
            </article>
            """
        )
    return "".join(cards)


def _serp_blueprint_cards(blueprints: list[dict[str, Any]]) -> str:
    if not blueprints:
        return "<div class='list-card'><p class='muted'>No SERP blueprints generated yet.</p></div>"
    cards = []
    for item in blueprints[:8]:
        faq_html = ""
        faq_patterns = list(item.get("faq_patterns") or [])
        if faq_patterns:
            faq_lines = "".join(
                f"<li>{html.escape(str(pattern.get('question', pattern)))}</li>"
                for pattern in faq_patterns[:3]
            )
            faq_html = f"<ul class='compact-list'>{faq_lines}</ul>"
        cards.append(
            f"""
            <article class="list-card">
              <div class="row-actions">
                <h3>{html.escape(str(item.get("query", "")))}</h3>
                <span class="status-pill status-neutral">{html.escape(str(len(list(item.get("source_urls") or []))))} sources</span>
              </div>
              <p class="muted">{html.escape(', '.join(str(entry.get("heading", "")) for entry in list(item.get("heading_structure") or [])[:3]) or 'No repeated headings yet.')}</p>
              {faq_html}
            </article>
            """
        )
    return "".join(cards)


def _content_task_cards(tasks: list[dict[str, Any]]) -> str:
    if not tasks:
        return "<div class='list-card'><p class='muted'>No content tasks generated yet.</p></div>"
    cards = []
    for item in tasks[:8]:
        cards.append(
            f"""
            <article class="task-card">
              <div class="row-actions">
                <h3>{html.escape(str(item.get("page_title") or _short_page_label(str(item.get("page_url", "")))))}</h3>
                <span class="status-pill {'status-ok' if str(item.get('execution_eligibility', '')) == 'auto_execute' else 'status-warn'}">{html.escape(_humanize_label(str(item.get("action_type", ""))) or "Task")}</span>
              </div>
              <p class="muted">{html.escape(str(item.get("section_name", "Content task")))}</p>
              <p>{html.escape(str(item.get("reason", "No rationale supplied.")))}</p>
            </article>
            """
        )
    return "".join(cards)


def _page_shell(title: str, body: str) -> str:
    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{html.escape(title)}</title>
    {render_agent_favicon_links()}
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Montserrat:wght@700;800&display=swap" rel="stylesheet">
    <link rel="stylesheet" href="/static/admin.css?v=2">
    <style>
      :root {{
        --anata-ink: #2b3644;
        --anata-ink-soft: #4b5668;
        --anata-sky: #85bbda;
        --anata-sky-deep: #4f84c4;
        --anata-sand: #bfa889;
        --anata-sand-soft: #f9f7f3;
        --anata-paper: #ffffff;
        --anata-line: rgba(43, 54, 68, 0.10);
        --anata-shadow: rgba(43, 54, 68, 0.10);
        --anata-muted: #6b7688;
        --panel: var(--anata-paper);
        --ink: var(--anata-ink);
        --muted: var(--anata-muted);
        --line: var(--anata-line);
        --accent: var(--anata-sky);
        --accent-2: var(--anata-sand);
        --good: #0f766e;
        --warn: #a16207;
        --bad: #b91c1c;
      }}
      * {{ box-sizing: border-box; }}
      body {{ margin: 0; background: var(--anata-sand-soft); color: var(--ink); font-family: "Inter", "Segoe UI", sans-serif; }}
      a {{ color: var(--anata-ink); }}
      {render_agent_nav_styles()}
      .shell {{ max-width: 1320px; margin: 0 auto; padding: 28px 24px 64px; display: grid; gap: 20px; }}
      .hero {{ display: grid; gap: 20px; grid-template-columns: minmax(0,1.2fr) minmax(300px,.8fr); align-items: start; }}
      .card {{ background: var(--panel); border: 1px solid var(--line); border-radius: 26px; padding: 24px; box-shadow: 0 18px 40px var(--anata-shadow); }}
      .eyebrow {{ margin: 0; text-transform: uppercase; letter-spacing: .18em; font-size: 12px; font-weight: 800; color: var(--accent); font-family: "Montserrat", sans-serif; }}
      h1,h2,h3,p {{ margin: 0; }}
      h1, h2, h3 {{ font-family: "Montserrat", sans-serif; color: var(--anata-ink); }}
      h1 {{ font-size: clamp(2.2rem, 4vw, 3.8rem); line-height: .98; letter-spacing: -0.03em; }}
      h2 {{ font-size: 30px; line-height: 1.05; letter-spacing: -0.02em; }}
      h3 {{ font-size: 18px; line-height: 1.25; }}
      .lead {{ color: var(--anata-ink-soft); line-height: 1.55; font-size: 18px; }}
      .lead-sm {{ color: var(--anata-ink-soft); line-height: 1.45; font-size: 14px; }}
      .stats {{ display: grid; grid-template-columns: repeat(4, minmax(0,1fr)); gap: 14px; }}
      .stat strong {{ display: block; font-size: 28px; line-height: 1.05; margin-top: 8px; }}
      .stat-link {{ margin-top: 10px; font-size: 13px; font-weight: 700; text-decoration: underline; text-underline-offset: 3px; }}
      .grid-2 {{ display: grid; grid-template-columns: repeat(2, minmax(0,1fr)); gap: 20px; }}
      .stack {{ display: grid; gap: 12px; }}
      .list-card {{ display: grid; gap: 10px; padding: 16px; border: 1px solid var(--line); border-radius: 22px; background: #fff; }}
      .card-muted {{ opacity: 0.96; }}
      .muted {{ color: var(--muted); }}
      .status-chip {{ display: inline-flex; align-items: center; padding: 6px 10px; border-radius: 999px; font-size: 12px; font-weight: 700; background: #f3f4f6; }}
      .status-approved, .status-done {{ background: rgba(15,118,110,.1); color: var(--good); }}
      .status-new, .status-in-progress {{ background: rgba(161,98,7,.12); color: var(--warn); }}
      .status-rejected, .status-error {{ background: rgba(185,28,28,.1); color: var(--bad); }}
      .feedback-actions, .row-actions, .button-row {{ display: flex; flex-wrap: wrap; gap: 8px; }}
      form.inline {{ margin: 0; }}
      input, textarea, select, button {{ font: inherit; }}
      input[type="text"], textarea, select {{ width: 100%; padding: 12px 14px; border-radius: 14px; border: 1px solid var(--line); background: #fff; color: var(--ink); }}
      textarea {{ min-height: 120px; resize: vertical; }}
      button {{ appearance: none; border: 0; border-radius: 999px; padding: 11px 16px; background: var(--ink); color: #fff; font-weight: 800; cursor: pointer; }}
      button:disabled {{ cursor: not-allowed; opacity: .48; }}
      button.ghost {{ background: #fff; color: var(--ink); border: 1px solid var(--line); }}
      button.tiny {{ padding: 8px 12px; font-size: 12px; }}
      button.active {{ background: var(--accent); color: var(--ink); }}
      .form-grid {{ display: grid; grid-template-columns: repeat(2, minmax(0,1fr)); gap: 14px; }}
      .span-2 {{ grid-column: 1 / -1; }}
      .detail-layout {{ display: grid; grid-template-columns: minmax(260px,.75fr) minmax(0,1.25fr); gap: 18px; align-items: start; }}
      .summary-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(140px,1fr)); gap: 10px; }}
      .summary-chip {{ border: 1px solid var(--line); border-radius: 18px; padding: 14px; background: #fcfbf8; display: grid; gap: 6px; }}
      .summary-chip span {{ font-size: 12px; letter-spacing: .04em; text-transform: uppercase; color: var(--muted); font-family: "Montserrat", sans-serif; font-weight: 700; }}
      .summary-chip strong {{ font-size: 22px; line-height: 1.05; }}
      .summary-good strong {{ color: var(--good); }}
      .summary-warn strong, .summary-bad strong {{ color: var(--warn); }}
      .summary-bad strong {{ color: var(--bad); }}
      .mini-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(140px,1fr)); gap: 10px; }}
      .mini-chip {{ display: grid; gap: 4px; padding: 12px 14px; border-radius: 16px; background: rgba(247,243,236,.8); border: 1px solid rgba(29,45,68,0.08); }}
      .mini-chip span {{ font-size: 11px; font-weight: 700; letter-spacing: .08em; text-transform: uppercase; color: var(--muted); font-family: "Montserrat", sans-serif; }}
      .mini-chip strong {{ font-size: 14px; line-height: 1.4; }}
      .setup-grid {{ display: grid; grid-template-columns: repeat(2, minmax(0,1fr)); gap: 14px; }}
      .setup-card, .task-card, .action-card {{ display: grid; gap: 10px; padding: 18px; border: 1px solid var(--line); border-radius: 22px; background: #fff; }}
      .identity-grid {{ display: grid; gap: 10px; grid-template-columns: 1fr; padding-top: 4px; }}
      .identity-grid code {{ word-break: break-word; }}
      .meta-pair {{ display: grid; gap: 4px; }}
      .meta-pair span {{ font-size: 11px; font-weight: 700; letter-spacing: .08em; text-transform: uppercase; color: var(--muted); font-family: "Montserrat", sans-serif; }}
      .meta-pair code {{ width: fit-content; max-width: 100%; }}
      .setup-card.is-blocked {{ border-color: rgba(161,98,7,.28); background: #fffaf0; }}
      .setup-card.is-connected {{ border-color: rgba(15,118,110,.18); background: linear-gradient(180deg, #fbfffd 0%, #f4fbf8 100%); }}
      .status-pill {{ display: inline-flex; align-items: center; gap: 6px; padding: 6px 10px; border-radius: 999px; font-size: 12px; font-weight: 700; border: 1px solid transparent; }}
      .status-ok {{ background: rgba(15,118,110,.1); color: var(--good); }}
      .status-warn {{ background: rgba(161,98,7,.12); color: var(--warn); }}
      .status-bad {{ background: rgba(185,28,28,.1); color: var(--bad); }}
      .status-neutral {{ background: rgba(133, 187, 218, 0.14); color: var(--ink); border-color: rgba(79,132,196,0.12); }}
      .chip-row {{ display: flex; flex-wrap: wrap; gap: 8px; }}
      .text-link {{ font-weight: 700; text-decoration: underline; text-underline-offset: 3px; }}
      .action-card {{ background: linear-gradient(180deg, #fff 0%, #fdfbf7 100%); }}
      .insight-card {{ display: grid; gap: 10px; padding: 18px; border: 1px solid var(--line); border-radius: 22px; background: linear-gradient(180deg, #fff 0%, #fbfcfe 100%); align-content: start; }}
      .widget-scroll {{ display: grid; gap: 12px; max-height: 560px; overflow: auto; padding-right: 4px; }}
      .compact-scroll {{ max-height: 420px; }}
      .help-details {{ position: relative; }}
      .help-details summary {{ list-style: none; width: 28px; height: 28px; border-radius: 999px; display: inline-flex; align-items: center; justify-content: center; background: rgba(133, 187, 218, 0.16); border: 1px solid rgba(29,45,68,0.08); cursor: pointer; font-weight: 800; }}
      .help-details summary::-webkit-details-marker {{ display: none; }}
      .help-copy {{ position: absolute; top: calc(100% + 8px); right: 0; z-index: 15; width: min(320px, 75vw); padding: 12px 14px; border-radius: 16px; background: #fff; border: 1px solid var(--line); box-shadow: 0 18px 32px rgba(29,45,68,0.12); display: grid; gap: 8px; }}
      .help-copy p {{ font-size: 14px; line-height: 1.45; }}
      .system-details summary {{ cursor: pointer; font-weight: 700; color: var(--anata-ink-soft); }}
      .source-chip {{ display: inline-flex; align-items: center; padding: 6px 10px; border-radius: 999px; font-size: 12px; font-weight: 700; background: #edf5ff; color: #25577a; }}
      .source-google-search-console, .source-google-search-console-source, .source-google-search-console-audit {{ background: #edf7ff; color: #275e83; }}
      .source-google-analytics-4 {{ background: #fff6ea; color: #8f5d0f; }}
      .source-structural-audit {{ background: #f2f7f4; color: #1e6259; }}
      .diff-grid {{ display: grid; grid-template-columns: repeat(2, minmax(0,1fr)); gap: 12px; align-items: start; }}
      .diff-block {{ padding: 14px; border-radius: 18px; background: var(--anata-sand-soft); border: 1px solid rgba(29,45,68,0.08); min-height: 100%; }}
      .report-frame {{ border: 1px solid var(--line); border-radius: 18px; overflow: hidden; min-height: 640px; background: #fff; }}
      .report-frame iframe {{ width: 100%; min-height: 640px; border: 0; }}
      .flash {{ padding: 14px 16px; border-radius: 16px; background: rgba(133,187,218,.18); border: 1px solid rgba(133,187,218,.35); }}
      code {{ background: #f3efe6; padding: 2px 6px; border-radius: 6px; }}
      .compact-list {{ margin: 0; padding-left: 18px; color: var(--muted); display: grid; gap: 4px; }}
      .ops-state {{ display: grid; grid-template-columns: minmax(0,1fr) auto; gap: 24px; align-items: center; padding: 24px; border: 1px solid var(--line); border-radius: 24px; }}
      .ops-state--blocked {{ background: #fffaf0; border-color: rgba(161,98,7,.28); }}
      .ops-state--ready {{ background: #f4fbf8; border-color: rgba(15,118,110,.20); }}
      .ops-state__action {{ justify-items: end; }}
      .section-heading {{ display:flex; justify-content:space-between; align-items:flex-start; gap:16px; }}
      .loop-grid {{ display:grid; grid-template-columns:repeat(5,minmax(0,1fr)); gap:10px; }}
      .loop-step {{ padding:16px; border:1px solid var(--line); border-radius:18px; background:#fcfbf8; }}
      .loop-step > span {{ display:inline-grid; place-items:center; width:28px; height:28px; border-radius:999px; background:rgba(133,187,218,.18); font-weight:800; }}
      .loop-step h3 {{ margin-top:12px; }}
      .loop-step p {{ margin-top:6px; color:var(--muted); font-size:13px; line-height:1.45; }}
      .data-workspace {{ width:100%; overflow:auto; border:1px solid var(--line); border-radius:20px; background:#fff; }}
      .data-table {{ width:100%; min-width:980px; border-collapse:collapse; }}
      .data-table th, .data-table td {{ padding:13px 14px; border-bottom:1px solid var(--line); text-align:left; vertical-align:top; }}
      .data-table th {{ position:sticky; top:0; z-index:2; background:#f8f6f1; color:var(--muted); font-family:"Montserrat",sans-serif; font-size:11px; letter-spacing:.06em; text-transform:uppercase; }}
      .data-table tr:last-child td {{ border-bottom:0; }}
      .data-table td {{ font-size:13px; line-height:1.45; }}
      .query-label {{ display:grid; gap:5px; min-width:220px; }}
      .query-label strong {{ font-family:"Montserrat",sans-serif; font-size:14px; }}
      .query-owner {{ max-width:240px; overflow-wrap:anywhere; }}
      @media (max-width: 900px) {{
        .hero, .grid-2, .detail-layout, .stats, .form-grid, .setup-grid, .diff-grid, .mini-grid, .ops-state, .loop-grid {{ grid-template-columns: 1fr; }}
        .ops-state__action {{ justify-items:start; }}
        .shell {{ width: auto; min-width: 0; display: block; padding: 24px 12px 48px; }}
        .shell > * {{ min-width: 0; }}
        .shell > * + * {{ margin-top: 20px; }}
        .card.stack {{ display: block; }}
        .card.stack > * + * {{ margin-top: 12px; }}
        .card form.stack {{ display: block; }}
        .card form.stack > * + * {{ margin-top: 12px; }}
        .row-actions {{ display: grid; grid-template-columns: minmax(0, 1fr); }}
        .row-actions > * {{ min-width: 0; }}
        input[type="file"] {{ max-width: 100%; }}
        .help-copy {{ right: auto; left: 0; width: min(300px, 70vw); }}
      }}
    </style>
  </head>
  <body class="app app--operator">
    {body}
</body>
</html>"""


def _nav(active: str = "website_ops", *, website_ops_section: str = "", user: dict | None = None) -> str:
    return render_agent_nav(active, website_ops_section=website_ops_section, user=user)


def _inject_admin_nav_into_report_html(report_html: str, *, active: str = "reports", user: dict | None = None) -> str:
    nav_styles = render_agent_nav_styles()
    font_links = """
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Montserrat:wght@700;800&display=swap" rel="stylesheet">
    <link rel="stylesheet" href="/static/admin.css?v=2">
    """
    nav_style_block = f"<style>{nav_styles}</style>"
    shell_styles = """
    <style>
      body {
        background: #f9f7f3;
        color: #2b3644;
        font-family: "Inter", "Segoe UI", sans-serif;
      }
      h1, h2, h3, h4, h5, h6 {
        font-family: "Montserrat", sans-serif;
        color: #2b3644;
      }
      .admin-report-shell {
        max-width: 1320px;
        margin: 0 auto;
        padding: 28px 24px 64px;
      }
      @media (max-width: 900px) {
        .admin-report-shell {
          width: auto;
          padding: 24px 12px 48px;
        }
      }
    </style>
    """
    injected = report_html
    if "</head>" in injected:
        injected = injected.replace("</head>", f"{font_links}{nav_style_block}{shell_styles}</head>", 1)
    if "<body" in injected:
        injected = re.sub(
            r"(<body[^>]*>)",
            r"\1" + render_agent_nav(active, user=user) + '<main id="agent-main-content" class="admin-report-shell app-container app-page">',
            injected,
            count=1,
            flags=re.IGNORECASE,
        )
        if "</body>" in injected:
            injected = injected.replace("</body>", "</main></body>", 1)
    return injected


_MD_STRIP = re.compile(r"[`*_~]|^\s*-\s*", re.MULTILINE)


def _strip_md(text: str) -> str:
    return _MD_STRIP.sub("", str(text)).strip()


def _report_cards(entries: list[dict[str, Any]]) -> str:
    if not entries:
        return "<div class='list-card'><p class='muted'>No reports yet.</p></div>"
    cards = []
    for entry in entries:
        cards.append(
            f"""
            <article class="list-card">
              <p class="eyebrow">{html.escape(entry.get('mode', '').title())} · {html.escape(entry.get('date', '') or entry.get('modified', ''))}</p>
              <h3><a href="/admin/website-ops/reports/{html.escape(entry['mode'], quote=True)}/{html.escape(entry['slug'], quote=True)}">{html.escape(entry['title'])}</a></h3>
              <p class="muted">{html.escape(_strip_md(entry.get('excerpt', '') or 'No summary available.'))}</p>
            </article>
            """
        )
    return "".join(cards)


def _decision_data_ready(analytics_status: Mapping[str, Any]) -> bool:
    return bool(analytics_status.get("search_console") and analytics_status.get("ga4"))


def _operator_blocker_panel(analytics_status: Mapping[str, Any]) -> str:
    if _decision_data_ready(analytics_status):
        return """
        <section class="ops-state ops-state--ready" aria-labelledby="ops-state-title">
          <div>
            <p class="eyebrow">Operating state</p>
            <h2 id="ops-state-title">Continuous optimization is ready.</h2>
            <p>Agent can measure demand and conversions, validate eligible updates, publish them, verify production, and retain the audit trail.</p>
          </div>
          <span class="status-pill status-ok">Ready</span>
        </section>
        """
    notes = [
        str(item).strip()
        for item in analytics_status.get("notes", [])
        if str(item).strip()
    ]
    reason = notes[0] if notes else "Search Console or GA4 decision data is unavailable."
    return f"""
    <section class="ops-state ops-state--blocked" aria-labelledby="ops-state-title">
      <div class="stack">
        <p class="eyebrow">Operating state</p>
        <h2 id="ops-state-title">Ranking optimization is paused safely.</h2>
        <p>{html.escape(reason)}</p>
        <p class="muted">Technical monitoring continues. Agent will resume ranking-led recommendations automatically after both Google connections pass validation.</p>
      </div>
      <div class="stack ops-state__action">
        <span class="status-pill status-bad">Blocked</span>
        <a class="btn" href="/admin/website-ops#data-sources">Repair Google connections</a>
      </div>
    </section>
    """


def _continuous_loop_panel() -> str:
    steps = (
        ("1", "Observe", "Crawl production and collect search and conversion evidence."),
        ("2", "Decide", "Prioritize one-page-one-intent opportunities with explicit evidence."),
        ("3", "Improve", "Apply only eligible marketing-site corrections."),
        ("4", "Verify", "Check deployment, rendered production, indexability, and rollback state."),
        ("5", "Learn", "Track outcomes and improve the next decision cycle."),
    )
    return (
        '<section class="card stack"><div class="section-heading">'
        '<div><p class="eyebrow">Self-sustaining system</p><h2>Continuous optimization loop</h2></div>'
        '<span class="status-pill status-neutral">Runs automatically</span></div>'
        '<div class="loop-grid">'
        + "".join(
            f"<article class='loop-step'><span>{number}</span><h3>{title}</h3><p>{copy}</p></article>"
            for number, title, copy in steps
        )
        + "</div></section>"
    )


def _feedback_empty_state(status_filter: str = "", *, decision_data_ready: bool = True) -> str:
    if status_filter == "approved":
        title = "No approved actions ready."
        copy = "Approve an exact action from Needs review first, then execute the approved batch from this queue."
    elif status_filter == "done":
        title = "No completed actions yet."
        copy = "Completed Website Ops changes will appear here after an approved action runs and verifies."
    elif status_filter == "error":
        title = "No failed actions."
        copy = "Execution failures will appear here with the error that needs intervention."
    elif status_filter == "rejected":
        title = "No rejected actions."
        copy = "Rejected recommendations will appear here for audit history."
    else:
        title = "No Website Ops records need review."
        copy = (
            "Ranking-led page recommendations are paused until Search Console and GA4 "
            "are connected. Source-backed editorial production continues from the "
            "approved service-aligned backlog."
            if not decision_data_ready
            else "No new evidence-backed actions were generated in the latest completed run."
        )
    run_control = """
        <form class="inline" action="/admin/api/website-ops/run" method="post">
          <input type="hidden" name="mode" value="daily">
          <button type="submit">Run Daily Sweep</button>
        </form>
        """
    return f"""
    <div class="list-card empty-state">
      <h3>{html.escape(title)}</h3>
      <p class="muted">{html.escape(copy)}</p>
      <div class="button-row">
        {run_control}
        {('<a class="text-link" href="/admin/website-ops#data-sources">Repair Google connections</a>' if not decision_data_ready else '')}
        <a class="text-link" href="/admin/website-ops#submit-issue">Submit issue</a>
        <a class="text-link" href="/admin/website-ops/reports/latest">Open latest report</a>
      </div>
    </div>
    """


def _feedback_cards(
    entries: list[dict[str, Any]],
    *,
    with_actions: bool = False,
    empty_context: str = "",
    decision_data_ready: bool = True,
) -> str:
    if not entries:
        return _feedback_empty_state(
            empty_context,
            decision_data_ready=decision_data_ready,
        )
    cards = []
    for entry in entries:
        actions = ""
        if with_actions:
            actions = f"""
            <div class="feedback-actions">
              {''.join(
                f'''
                <form class="inline" action="/admin/api/website-ops/feedback/{html.escape(str(entry.get("feedback_id", "")), quote=True)}/review" method="post">
                  <input type="hidden" name="status" value="{status}">
                  <button class="ghost tiny" type="submit">{label}</button>
                </form>
                '''
                for status, label in [("approved", "Approve to run"), ("in-progress", "Mark running"), ("rejected", "Reject"), ("done", "Mark completed")]
              )}
            </div>
            """
        cards.append(
            f"""
            <article class="list-card">
              <div class="row-actions">
                {_status_chip(str(entry.get('status', 'new')))}
                <span class="muted">{html.escape(str(entry.get('priority', 'Medium')))}</span>
              </div>
              <h3><a href="/admin/website-ops/feedback/{html.escape(str(entry.get('feedback_id', '')), quote=True)}">{html.escape(str(entry.get('summary', 'Feedback item')))}</a></h3>
              <p class="muted">{html.escape(str(entry.get('page_url', '') or entry.get('page_title', '') or 'No page specified'))}</p>
              {actions}
            </article>
            """
        )
    return "".join(cards)


def _program_work_card(item: Mapping[str, Any], *, rank: int, current: bool = False) -> str:
    state = str(item.get("state", "Next")).strip() or "Next"
    state_tone = "status-ok" if state in {"Ready", "Measuring"} else "status-warn"
    if state == "Blocked":
        state_tone = "status-bad"
    return f"""
      <article class="action-card">
        <div class="row-actions">
          <span class="status-pill status-neutral">{'Now' if current else f'Next {rank}'}</span>
          <span class="status-pill {state_tone}">{html.escape(state)}</span>
          <span class="source-chip">{html.escape(str(item.get('work_type', 'Website improvement')))}</span>
        </div>
        <h3>{html.escape(str(item.get('title', 'Website Ops work item')))}</h3>
        <p class="muted">{html.escape(str(item.get('target', 'anatainc.com')))}</p>
        <p><strong>Next operation:</strong> {html.escape(str(item.get('next_operation', 'No operation recorded.')))}</p>
        <p class="muted"><strong>Why now:</strong> {html.escape(str(item.get('evidence', 'No evidence recorded.')))}</p>
        <div class="mini-grid">
          {_mini_chip("Impact", str(item.get("business_impact", "Not recorded")))}
          {_mini_chip("Confidence", str(item.get("confidence", "Unknown")))}
          {_mini_chip("Risk", str(item.get("risk", "Unknown")))}
          {_mini_chip("Owner", str(item.get("owner", "Website Ops")))}
        </div>
        <details>
          <summary class="text-link">Start and validation conditions</summary>
          <p class="muted"><strong>Starts when:</strong> {html.escape(str(item.get('start_condition', 'Not recorded.')))}</p>
          <p class="muted"><strong>Validated by:</strong> {html.escape(str(item.get('validation', 'Not recorded.')))}</p>
        </details>
      </article>
    """


def _program_plan_panel(plan: Mapping[str, Any]) -> str:
    current = dict(plan.get("current") or {})
    next_items = [dict(item) for item in list(plan.get("next") or []) if isinstance(item, Mapping)]
    if not current:
        return """
          <section class="card stack">
            <p class="eyebrow">Current and next work</p>
            <h2>No work plan has been generated yet.</h2>
            <p class="lead">Run the daily sweep to build a source-backed operating plan.</p>
          </section>
        """
    cards = [_program_work_card(current, rank=0, current=True)]
    cards.extend(
        _program_work_card(item, rank=index, current=False)
        for index, item in enumerate(next_items, start=1)
    )
    generated_at = str(plan.get("generated_at", "")).strip()
    return f"""
      <section class="card stack">
        <div class="row-actions">
          <div class="stack">
            <p class="eyebrow">Current and next work</p>
            <h2>What Agent is working on next</h2>
          </div>
          {f"<span class='muted'>Plan generated {html.escape(generated_at)}</span>" if generated_at else ""}
        </div>
        <p class="lead">This plan comes from the latest source health, qualified actions, indexing inventory, and measurement trust. It distinguishes active work from work that needs you.</p>
        <div class="widget-scroll">{''.join(cards)}</div>
      </section>
    """


def render_dashboard_page(settings: Settings, *, flash_message: str = "", user: dict | None = None) -> str:
    reports = _report_entries(settings)
    latest = reports[0] if reports else None
    latest_payload = _mvp_filter_report_payload(_report_payload(latest) if latest else {})
    feedback = _mvp_filter_feedback_records(load_feedback_records(settings))
    active_feedback = [item for item in feedback if item.get("status") not in {"done", "rejected"}]
    run_state = get_website_ops_run_state(settings, "daily")
    status_counts: dict[str, int] = {}
    for item in feedback:
        status_counts[item["status"]] = status_counts.get(item["status"], 0) + 1
    error_count = status_counts.get("error", 0)
    action_queue = list(latest_payload.get("action_queue") or [])[:6]
    support_requests = list(latest_payload.get("support_requests") or [])[:5]
    analytics_status = dict(latest_payload.get("analytics_status") or {})
    decision_data_ready = bool(
        analytics_status.get("search_console") and analytics_status.get("ga4")
    )
    analytics_status.setdefault(
        "operational_status",
        "operational" if decision_data_ready else "blocked",
    )
    analytics_status.setdefault("customer_language_status", "quarantined")
    latest_payload["analytics_status"] = analytics_status
    indexing_inventory = dict(
        latest_payload.get("indexing_inventory")
        or load_indexing_inventory(settings.website_ops_root)
    )
    crawl_verification = dict(
        latest_payload.get("crawl_verification")
        or load_crawl_verification(settings.website_ops_root)
    )
    program_plan = dict(
        latest_payload.get("program_plan")
        or build_program_plan(
            analytics_status=analytics_status,
            action_queue=action_queue,
            support_requests=support_requests,
            indexing_inventory=indexing_inventory,
            crawl_verification=crawl_verification,
        )
    )
    decision_ready = _decision_data_ready(analytics_status)
    production_inventory = dict(latest_payload.get("production_inventory") or {})
    daily_action_portfolio = dict(latest_payload.get("daily_action_portfolio") or {})
    page_insights = [
        dict(item)
        for item in list(latest_payload.get("page_insights") or [])[:5]
    ]
    if not decision_data_ready:
        for item in page_insights:
            item["score"] = None
            item["bucket"] = "data unavailable"
            item["metric_availability"] = {
                "search_console": "unavailable",
                "ga4": "unavailable",
            }
            item["task_block_reason"] = (
                "Decision data is unavailable. Ranking-led recommendations are suspended."
            )
    customer_questions = (
        list(latest_payload.get("customer_questions") or [])[:6]
        if analytics_status.get("customer_language_status") == "enabled"
        else []
    )
    serp_blueprints = list(latest_payload.get("serp_blueprints") or [])[:6]
    content_tasks = list(latest_payload.get("content_tasks") or [])[:6]
    query_intelligence = dict(
        latest_payload.get("query_intelligence") or load_query_intelligence(settings)
    )
    query_summary = dict(query_intelligence.get("summary") or {})
    article_pipeline = dict(query_intelligence.get("article_pipeline") or {})
    monitored_count = int(latest_payload.get("pages_reviewed", 0) or len(settings.website_ops_site_urls))
    schedule_note = (
        "<p class='muted'>Scheduled hourly pulses from 8:00 AM through 3:00 PM America/Denver. "
        "Email is sent when completed work or the action state changes.</p>"
    )
    body = f"""
      {_nav("website_ops", website_ops_section="seo_dashboard", user=user)}
      <main id="agent-main-content" class="shell app-container app-page">
        {f"<div class='flash'>{html.escape(flash_message)}</div>" if flash_message else ""}
        <section class="card stack">
            <p class="eyebrow">Website Ops</p>
            <h1>Continuous website <span style="color:var(--accent)">optimization</span>.</h1>
            <p class="lead">Agent continuously observes, improves, verifies, and learns across the anatainc.com marketing site.</p>
            <div class="button-row">
              <form action="/admin/api/website-ops/run" method="post"><input type="hidden" name="mode" value="daily"><button type="submit">Run Daily Sweep</button></form>
              {('<a class="btn btn--ghost" href="#data-sources">Repair Google connections</a>' if not decision_ready else '')}
              {('<button class="ghost" type="button" disabled aria-disabled="true">Weekly sweep unavailable</button>' if not decision_ready else '<form action="/admin/api/website-ops/run" method="post"><input type="hidden" name="mode" value="weekly"><button class="ghost" type="submit">Run Weekly Sweep</button></form>')}
              <a class="btn btn--ghost" href="/admin/website-ops/reports/latest">Open Latest Report</a>
            </div>
            {schedule_note}
        </section>
        {_operator_blocker_panel(analytics_status)}
        {_program_plan_panel(program_plan)}
        {render_daily_portfolio_panel(daily_action_portfolio)}
        {render_production_inventory_panel(production_inventory)}
        <section class="hero">
          <div id="submit-issue" class="card stack">
            <p class="eyebrow">Current scope</p>
            <div class="summary-grid">
              {_summary_chip("Marketing pages", monitored_count, tone="neutral")}
              {_summary_chip("Approved action runner", "Enabled" if settings.website_ops_execute_approved else "Disabled", tone="good" if settings.website_ops_execute_approved else "warn")}
              {_summary_chip("Metadata autopush", "Configured" if github_metadata_is_configured() else "Needs GitHub token", tone="good" if github_metadata_is_configured() else "warn")}
              {_run_state_summary(run_state)}
              {_connection_summary_chips(analytics_status)}
            </div>
            <p class="muted">Scope is restricted to anatainc.com and discovered from the production sitemap. High-confidence metadata changes require a documented reason and evidence, then commit to the marketing repository and verify on production.</p>
          </div>
          <div class="card stack">
            <p class="eyebrow">Automation policy</p>
            <h2>Validated autopush</h2>
            <p class="lead-sm">Eligible marketing updates publish without routine approval only after evidence, scope, intent, build, deployment, production, and rollback checks pass.</p>
            <div class="summary-grid">
              {_summary_chip("Writable host", "anatainc.com", tone="good")}
              {_summary_chip("Daily email", "Changes and your to-do list", tone="neutral")}
              {_summary_chip("Schedule", "Hourly · 8 AM–3 PM", tone="neutral")}
              {_summary_chip("Query validation", f"{query_summary.get('validated_clusters', 0)} validated", tone="good" if query_summary.get('validated_clusters') else "neutral")}
            </div>
            <a class="text-link" href="/admin/website-ops/queries">Inspect query ownership and citations</a>
          </div>
        </section>
        {_continuous_loop_panel()}
        <section class="card stack">
          <div class="section-heading">
            <div class="stack">
              <p class="eyebrow">Next autonomous work</p>
              <h2>Content program: brief, source, publish, measure.</h2>
              <p class="lead-sm">{html.escape(str(article_pipeline.get("message", "The next sweep will calculate article eligibility.")))}</p>
            </div>
            <span class="status-pill {'status-ok' if article_pipeline.get('status') == 'eligible' else 'status-warn'}">{html.escape(str(article_pipeline.get("status", "not calculated")).replace("_", " ").title())}</span>
          </div>
          <div class="summary-grid">
            {_summary_chip("Daily article minimum", "8", tone="good")}
            {_summary_chip("Daily article target", "8", tone="neutral")}
            {_summary_chip("Validated content gaps", article_pipeline.get("validated_informational_gaps", 0), tone="neutral")}
            {_summary_chip("Source-qualified candidates", article_pipeline.get("source_qualified_candidates", 0), tone="good" if article_pipeline.get("source_qualified_candidates") else "neutral")}
            {_summary_chip("Publishing policy", "Validated autopush", tone="good")}
          </div>
          <p class="muted">Each scheduled pulse audits the site, researches promising questions, advances briefs, completes eligible fixes, and verifies production. Agent targets eight qualified daily SEO actions, including two source-qualified educational articles for each service pillar when eight publishable intents pass every quality gate.</p>
          <div class="button-row">
            <a class="text-link" href="/admin/website-ops/strategy">Open content strategy and briefs</a>
            <a class="text-link" href="/admin/website-ops/queries">Inspect query evidence</a>
          </div>
        </section>
        <section class="grid-2">
          <div class="card stack">
            <p class="eyebrow">Authority growth</p>
            <h2>Earn citations. Never manufacture links.</h2>
            <p class="lead-sm">Agent monitors answer-engine citations and promotes only original, source-backed assets. Automated link creation, paid ranking links, excessive exchanges, and scaled guest-post outreach remain prohibited.</p>
            <div class="summary-grid">
              {_summary_chip("Cited clusters", query_summary.get("cited_clusters", 0), tone="good" if query_summary.get("cited_clusters") else "neutral")}
              {_summary_chip("Citation gains", query_summary.get("citation_gains", 0), tone="good" if query_summary.get("citation_gains") else "neutral")}
              {_summary_chip("Citation losses", query_summary.get("citation_losses", 0), tone="warn" if query_summary.get("citation_losses") else "neutral")}
              {_summary_chip("Outreach policy", "Relevant and evidence-led", tone="good")}
            </div>
          </div>
          <div class="card stack">
            <p class="eyebrow">Outcome learning</p>
            <h2>Measure movement without claiming causation.</h2>
            <p class="lead-sm">Agent records comparable Search Console and GA4 observations after changes. Movement is labeled as an association until the evidence supports a stronger conclusion.</p>
            <div class="summary-grid">
              {_summary_chip("Observed pages", query_summary.get("observed_outcome_pages", 0), tone="neutral")}
              {_summary_chip("Associated lead growth", query_summary.get("pages_with_associated_lead_growth", 0), tone="good" if query_summary.get("pages_with_associated_lead_growth") else "neutral")}
              {_summary_chip("Lead-event trust", str(analytics_status.get("ga4_trust_status", "unavailable")).title(), tone="good" if analytics_status.get("ga4_trust_status") == "trusted" else "warn")}
            </div>
          </div>
        </section>
        <section class="stats">
          {_dashboard_stat_card("Reports", len(reports), "Daily, weekly, monthly", "/admin/website-ops/reports")}
          {_dashboard_stat_card("Validated Queries", query_summary.get('validated_clusters', 0), "One page, one intent", "/admin/website-ops/queries?status=validated")}
          {_dashboard_stat_card("Needs Review", status_counts.get('new', 0), "Needs a decision", "/admin/website-ops/queue?status=new")}
          {_dashboard_stat_card("Approved to Run", status_counts.get('approved', 0) + status_counts.get('in-progress', 0), "Approved or running", "/admin/website-ops/queue?status=approved")}
          {_dashboard_stat_card("Completed", status_counts.get('done', 0), "Completed safely", "/admin/website-ops/queue?status=done")}
          {_dashboard_stat_card("Failed", error_count, "Needs intervention", "/admin/website-ops/queue?status=error") if error_count else ""}
        </section>
        <section class="grid-2">
          <div class="card stack">
            <p class="eyebrow">Primary goal</p>
            <h2>{html.escape(str((latest_payload.get('goal') or {}).get('primary', 'Increase qualified organic leads with less manual website work.')))}</h2>
            <p class="lead">This is the system objective the dashboard should optimize against, not just a list of page checks.</p>
          </div>
          <div class="card stack">
            <p class="eyebrow">Your to-do list</p>
            <p class="lead">Only work the system cannot complete safely appears here.</p>
            {_team_help_cards(support_requests, analytics_status)}
          </div>
        </section>
        <section class="grid-2">
          {_latest_report_panel(latest, latest_payload)}
          <div class="card stack">
            <div class="row-actions"><h2>Submit a new issue</h2>{_issue_help_block()}</div>
            <form action="/admin/api/website-ops/feedback" method="post" class="form-grid">
              <div><label>Category</label><select name="category"><option>SEO</option><option>Content</option><option>UX</option><option>Conversion</option><option>Technical</option><option>Strategy</option></select></div>
              <div><label>Priority</label><select name="priority"><option>Low</option><option selected>Medium</option><option>High</option><option>Urgent</option></select></div>
              <div class="span-2"><label>Page URL</label><input type="text" name="page_url" placeholder="https://anatainc.com/services/..."></div>
              <div class="span-2"><label>Summary</label><input type="text" name="summary" placeholder="Short description of the issue"></div>
              <div class="span-2"><label>Details</label><textarea name="details" placeholder="What is wrong, why it matters, and what outcome is needed."></textarea></div>
              <div class="span-2"><button type="submit">Save Feedback</button></div>
            </form>
          </div>
        </section>
        <section class="grid-2">
          <div class="card stack">
            <h2>Priority action queue</h2>
            <p class="lead">Agent records why each change qualified, how it was validated, and what happened in production.</p>
            <div class="button-row">
              <a href="/admin/website-ops/queue" class="text-link">Inspect action ledger</a>
              <span class="muted">Blocked, validating, published, failed, and rolled-back work remains auditable.</span>
            </div>
            <div class="widget-scroll">{_action_queue_cards(action_queue)}</div>
          </div>
          <div class="card stack">
            <h2>Insight snapshots</h2>
            <p class="lead">Compact page snapshots for quick triage across search demand, traffic, and conversion performance.</p>
            <div class="widget-scroll">{_insight_snapshot_cards(page_insights)}</div>
          </div>
        </section>
        <section class="grid-2">
          <div class="card stack">
            <h2>Customer-language evidence</h2>
            <p class="lead">{'Gmail-derived questions are quarantined until relevance and privacy validation is complete.' if analytics_status.get('customer_language_status') == 'quarantined' else 'Sanitized, relevant customer questions available for content decisions.'}</p>
            <div class="widget-scroll compact-scroll">{_customer_question_cards(customer_questions)}</div>
          </div>
          <div class="card stack">
            <h2>Search patterns from ranking pages</h2>
            <p class="lead">Repeated heading and FAQ patterns from ranking pages for the highest-signal service queries.</p>
            <div class="widget-scroll compact-scroll">{_serp_blueprint_cards(serp_blueprints)}</div>
          </div>
        </section>
        <section class="grid-2">
          <div class="card stack">
            <h2>Recommended content updates</h2>
            <p class="lead">Structured content updates generated from search demand and buyer language.</p>
            <div class="widget-scroll compact-scroll">{_content_task_cards(content_tasks)}</div>
          </div>
          <div class="card stack"><h2>Open issues</h2><div class="widget-scroll compact-scroll">{_feedback_cards(active_feedback[:8], with_actions=True, decision_data_ready=decision_ready)}</div></div>
        </section>
        <section class="grid-2">
          <div class="card stack"><h2>Recent reports</h2><div class="widget-scroll compact-scroll">{_report_cards(reports[:8])}</div></div>
          <div id="data-sources" class="card stack"><h2>Data sources</h2><p class="lead">Both sources must pass before ranking-led recommendations resume.</p><div class="setup-grid">{_analytics_connection_cards(analytics_status)}</div></div>
        </section>
        <section class="grid-2">
          {_system_details_panel(settings, analytics_status)}
        </section>
      </main>
      {_dashboard_auto_run_script(run_state)}
    """
    return _page_shell("agent | Website Ops", body)


def render_indexing_page(
    settings: Settings,
    *,
    user: dict | None = None,
    import_message: str = "",
    import_error: str = "",
) -> str:
    inventory = load_indexing_inventory(settings.website_ops_root)
    crawl_inventory = load_crawl_inventory(settings.website_ops_root)
    crawl_verification = load_crawl_verification(settings.website_ops_root)
    summary = dict(inventory.get("summary") or {})
    inspection = dict(inventory.get("inspection") or {})
    sitemap_submission = dict(inventory.get("sitemap_submission") or {})
    crawl_summary = dict(crawl_inventory.get("summary") or {})
    verification_summary = dict(crawl_verification.get("summary") or {})
    imports = [
        dict(item)
        for item in list(crawl_inventory.get("imports") or [])
        if isinstance(item, Mapping)
    ]
    records = [dict(item) for item in list(inventory.get("records") or []) if isinstance(item, Mapping)]
    verification_records = [
        dict(item)
        for item in list(crawl_verification.get("records") or [])
        if isinstance(item, Mapping)
    ]
    reason_counts = dict(summary.get("reason_counts") or {})
    reason_cards = "".join(
        _summary_chip(reason, count, tone="neutral")
        for reason, count in sorted(reason_counts.items(), key=lambda item: (-int(item[1]), item[0]))
    )
    rows = "".join(
        f"""
          <tr>
            <td><a class="text-link" href="{html.escape(str(item.get('url', '')), quote=True)}">{html.escape(_short_page_label(str(item.get('url', ''))))}</a></td>
            <td>{html.escape(str(item.get('reason', 'Unspecified')))}</td>
            <td><span class="status-pill {'status-ok' if item.get('intentional') or item.get('desired_state') == 'indexed' else 'status-warn'}">{html.escape(str(item.get('desired_state', 'investigate')).replace('_', ' ').title())}</span></td>
            <td>{html.escape(str(item.get('priority', 'medium')).title())}</td>
            <td>{html.escape(str(item.get('page_fetch_state', '') or item.get('verdict', '') or 'Unavailable').replace('_', ' ').title())}{f'<div class="muted">Production HTTP {html.escape(str(item.get("production_status")))}</div>' if item.get('production_status') else ''}</td>
            <td>{html.escape(str(item.get('next_operation', '')))}</td>
            <td>{html.escape(str(item.get('last_crawled', '') or 'Unavailable'))}</td>
          </tr>
        """
        for item in records
    )
    verification_rows = "".join(
        f"""
          <tr>
            <td><a class="text-link" href="{html.escape(str(item.get('url', '')), quote=True)}">{html.escape(_short_page_label(str(item.get('url', ''))))}</a></td>
            <td><span class="status-pill {'status-bad' if item.get('state') == 'confirmed' else 'status-warn' if item.get('state') == 'pending' else 'status-neutral' if item.get('state') == 'noise' else 'status-ok'}">{html.escape(str(item.get('state', 'pending')).title())}</span></td>
            <td>{html.escape('; '.join(str(warning.get('report', '')) for warning in list(item.get('warning_results') or [])[:3]))}</td>
            <td>{html.escape('; '.join(str(warning.get('reason', '')) for warning in list(item.get('warning_results') or [])[:2]))}</td>
            <td>{html.escape(str(item.get('rendered_at', '') or 'Not observed'))}</td>
          </tr>
        """
        for item in verification_records[:100]
    )
    empty = """
      <div class="list-card empty-state">
        <h3>No Search Console indexing URLs observed yet.</h3>
        <p class="muted">The next weekly sweep will inspect every canonical marketing URL through Search Console. CSV or JSON exports remain supported as a fallback.</p>
        <p class="muted">Agent records coverage, fetch, robots, canonical, and last-crawl evidence without treating missing data as zero.</p>
      </div>
    """
    body = f"""
      {_nav("website_ops", website_ops_section="indexing", user=user)}
      <main id="agent-main-content" class="shell app-container app-page">
        {f'<div class="alert alert--success">{html.escape(import_message)}</div>' if import_message else ''}
        {f'<div class="alert alert--error">{html.escape(import_error)}</div>' if import_error else ''}
        <section class="card stack">
          <p class="eyebrow">Website Ops indexing</p>
          <h1>Every known URL gets a desired search state.</h1>
          <p class="lead">Search Console exclusions are reconciled against production evidence before Agent improves, consolidates, redirects, or intentionally excludes a URL.</p>
          <div class="button-row">
            <form action="/admin/api/website-ops/run" method="post">
              <input type="hidden" name="mode" value="weekly">
              <button type="submit">Run Weekly Inspection</button>
            </form>
            <a class="btn btn--ghost" href="/admin/website-ops">Return to Overview</a>
          </div>
        </section>
        <section class="card stack">
          <div class="row-actions">
            <div class="stack">
              <h2>Import crawl evidence</h2>
              <p class="lead-sm">Upload each Screaming Frog ZIP. Agent merges recognized reports, separates production from Vercel, and keeps every crawler warning unverified until rendered-page and repository checks agree.</p>
            </div>
            <span class="status-pill status-neutral">{len(imports)} imports</span>
          </div>
          <form action="/admin/website-ops/indexing" method="post" enctype="multipart/form-data" class="stack">
            <label for="crawl-report">Screaming Frog ZIP export</label>
            <input id="crawl-report" name="report" type="file" accept=".zip,application/zip" required>
            <div class="button-row">
              <button type="submit">Import crawl evidence</button>
              <span class="muted">Maximum 10 MB per archive.</span>
            </div>
          </form>
        </section>
        <section class="stats">
          {_dashboard_stat_card("Known URLs", summary.get("known_urls", 0), "Imported indexing evidence", "/admin/website-ops/indexing")}
          {_dashboard_stat_card("Indexed", summary.get("indexed", 0), "Confirmed by Google", "/admin/website-ops/indexing")}
          {_dashboard_stat_card("Needs Action", summary.get("needs_action", 0), "Improve, consolidate, or investigate", "/admin/website-ops/indexing")}
          {_dashboard_stat_card("Intentional", summary.get("intentional_exclusions", 0), "Protected or deliberately excluded", "/admin/website-ops/indexing")}
          {_dashboard_stat_card("Inspected", inspection.get("succeeded", 0), f"{inspection.get('failed', 0)} API failures", "/admin/website-ops/indexing")}
          {_dashboard_stat_card("Sitemap", str(sitemap_submission.get("status", "not submitted")).replace("_", " ").title(), "Search Console discovery feed", "/admin/website-ops/indexing")}
        </section>
        {f'''
        <section class="card stack">
          <div class="section-heading">
            <div class="stack">
              <p class="eyebrow">URL Inspection API</p>
              <h2>The latest inspection attempt needs attention.</h2>
              <p class="lead-sm">{html.escape(str(inspection.get("failed", 0)))} of {html.escape(str(inspection.get("attempted", 0)))} canonical URL inspections failed. The last good inventory remains intact and the scheduler will retry.</p>
            </div>
            <span class="status-pill status-bad">Failed</span>
          </div>
          <ul class="evidence-list">{"".join(f"<li>{html.escape(str(item))}</li>" for item in list(inspection.get("failure_samples") or []))}</ul>
        </section>
        ''' if int(inspection.get("failed", 0) or 0) else ""}
        <section class="stats">
          {_dashboard_stat_card("Production Crawl", crawl_summary.get("production_urls", 0), "anatainc.com URLs only", "/admin/website-ops/indexing")}
          {_dashboard_stat_card("Vercel Sandbox", crawl_summary.get("sandbox_urls", 0), "Kept separate from production", "/admin/website-ops/indexing")}
          {_dashboard_stat_card("Crawler Warnings", crawl_summary.get("urls_with_warnings", 0), "Evidence awaiting verification", "/admin/website-ops/indexing")}
        </section>
        <section class="stats">
          {_dashboard_stat_card("Confirmed", verification_summary.get("confirmed_urls", 0), "Eligible for remediation planning", "/admin/website-ops/indexing")}
          {_dashboard_stat_card("Pending Proof", verification_summary.get("pending_urls", 0), "Requires stronger evidence", "/admin/website-ops/indexing")}
          {_dashboard_stat_card("Noise", verification_summary.get("noise_urls", 0), "Real signal outside ranking remediation", "/admin/website-ops/indexing")}
          {_dashboard_stat_card("Stale", verification_summary.get("disproved_urls", 0), "Disproved by fresh production evidence", "/admin/website-ops/indexing")}
        </section>
        <section class="card stack">
          <div class="row-actions">
            <div class="stack">
              <h2>Crawl verification ledger</h2>
              <p class="lead-sm">Only production-confirmed ranking defects may become remediation work. Pending warnings need stronger evidence, noise stays outside SEO, and stale findings remain visible so the system does not rediscover old work.</p>
            </div>
            <span class="status-pill status-neutral">{len(verification_records)} warning URLs</span>
          </div>
          {f'''
          <div class="data-workspace">
            <table class="data-table">
              <thead><tr><th>URL</th><th>Verdict</th><th>Crawler reports</th><th>Fresh evidence</th><th>Observed</th></tr></thead>
              <tbody>{verification_rows}</tbody>
            </table>
          </div>
          ''' if verification_records else '<div class="list-card empty-state"><h3>No crawl verification run yet.</h3><p class="muted">Run the daily sweep to compare imported crawler warnings with fresh production evidence.</p></div>'}
        </section>
        <section class="card stack">
          <h2>Reason groups</h2>
          <div class="summary-grid">{reason_cards or _summary_chip("No imported reasons", "Waiting", tone="neutral")}</div>
        </section>
        <section class="card stack">
          <div class="row-actions">
            <div class="stack">
              <h2>URL classification ledger</h2>
              <p class="lead-sm">A crawler or Search Console warning is evidence to investigate, not automatic permission to change production.</p>
            </div>
            <span class="status-pill status-neutral">{len(records)} records</span>
          </div>
          {f'''
          <div class="data-workspace">
            <table class="data-table">
              <thead><tr><th>URL</th><th>Search Console reason</th><th>Desired state</th><th>Priority</th><th>Fetch evidence</th><th>Next operation</th><th>Last crawled</th></tr></thead>
              <tbody>{rows}</tbody>
            </table>
          </div>
          ''' if records else empty}
        </section>
      </main>
    """
    return _page_shell("agent | Website Ops — Indexing", body)


def _query_filter_matches(cluster: Mapping[str, Any], status_filter: str) -> bool:
    if not status_filter:
        return True
    if status_filter == "validated":
        return cluster.get("validation_status") == "validated"
    if status_filter == "hypothesis":
        return cluster.get("validation_status") == "hypothesis"
    if status_filter == "quarantined":
        return cluster.get("quality_status") == "quarantined"
    if status_filter == "conflict":
        return cluster.get("ownership_status") == "conflict"
    if status_filter == "cited":
        return dict(cluster.get("citation") or {}).get("status") == "cited"
    if status_filter == "citation-lost":
        return dict(cluster.get("citation") or {}).get("change") == "lost"
    return True


def render_query_map_page(
    settings: Settings,
    *,
    status_filter: str = "",
    user: dict | None = None,
) -> str:
    intelligence = load_query_intelligence(settings)
    summary = dict(intelligence.get("summary") or {})
    article_pipeline = dict(intelligence.get("article_pipeline") or {})
    intent_coverage = dict(intelligence.get("intent_coverage") or {})
    intent_records = [
        dict(item)
        for item in list(intent_coverage.get("records") or [])
        if isinstance(item, Mapping)
    ]
    clusters = [
        dict(item)
        for item in intelligence.get("clusters", []) or []
        if isinstance(item, Mapping) and _query_filter_matches(item, status_filter)
    ]
    recommendations = [
        dict(item)
        for item in intelligence.get("recommendations", []) or []
        if isinstance(item, Mapping)
    ]
    rows: list[str] = []
    for cluster in clusters:
        evidence = ", ".join(
            str(item).replace("_", " ").title()
            for item in cluster.get("evidence_classes", []) or []
        ) or "No evidence"
        quality_status = str(cluster.get("quality_status", "eligible") or "eligible")
        quality_reasons = " ".join(
            str(item) for item in cluster.get("quality_reasons", []) or []
        )
        if quality_status == "quarantined":
            evidence = f"{evidence} · Excluded from validation: {quality_reasons}"
        citation = dict(cluster.get("citation") or {})
        citation_status = str(citation.get("status", "Not tested") or "Not tested")
        ownership_status = str(cluster.get("ownership_status", "assigned") or "assigned")
        conflict_count = len(list(cluster.get("conflict_urls") or []))
        rows.append(
            f"""
            <tr>
              <td><div class="query-label"><strong>{html.escape(str(cluster.get("label", "Query cluster")))}</strong><span class="muted">{html.escape(str(cluster.get("intent", "unknown")).title())} · {html.escape(str(cluster.get("funnel_stage", "unknown")).title())}</span></div></td>
              <td><span class="status-pill {'status-bad' if quality_status == 'quarantined' else 'status-ok' if cluster.get('validation_status') == 'validated' else 'status-neutral'}">{html.escape('Quarantined' if quality_status == 'quarantined' else str(cluster.get("validation_status", "hypothesis")).title())}</span></td>
              <td>{html.escape(evidence)}</td>
              <td class="query-owner"><a href="{html.escape(str(cluster.get("owner_url", "#")), quote=True)}">{html.escape(str(cluster.get("owner_url", "Unassigned")))}</a></td>
              <td><span class="status-pill {'status-bad' if ownership_status == 'conflict' else 'status-ok'}">{html.escape(ownership_status.replace('_', ' ').title())}</span>{f'<div class="muted">{conflict_count} competing page(s)</div>' if conflict_count else ''}{f'<div class="muted">{len(list(cluster.get("supporting_urls") or []))} supporting brand page(s)</div>' if ownership_status == 'brand_coverage' else ''}</td>
              <td>{html.escape(citation_status.replace("-", " ").title())}</td>
              <td>{html.escape(str(cluster.get("observed_impressions", "Unavailable"))) if "observed_search" in cluster.get("evidence_classes", []) else "Unavailable"}</td>
            </tr>
            """
        )
    intent_rows = "".join(
        f"""
          <tr>
            <td><a class="text-link" href="{html.escape(str(item.get('url', '')), quote=True)}">{html.escape(str(item.get('path', '')))}</a></td>
            <td><div class="query-label"><strong>{html.escape(str(item.get('primary_intent', '')))}</strong><span class="muted">{html.escape(str(item.get('intent_type', '')).title())}</span></div></td>
            <td><span class="status-pill {'status-ok' if item.get('coverage_status') == 'observed' else 'status-neutral'}">{html.escape(str(item.get('coverage_status', 'unobserved')).title())}</span></td>
            <td>{html.escape(str(item.get('cluster_count', 0)))}</td>
            <td><span class="status-pill {'status-bad' if item.get('ownership_conflicts') else 'status-ok'}">{html.escape('Conflict' if item.get('ownership_conflicts') else 'Unique')}</span></td>
          </tr>
        """
        for item in intent_records
    )
    empty = """
      <div class="ops-state ops-state--blocked">
        <div class="stack">
          <p class="eyebrow">Query map state</p>
          <h2>No matching query clusters.</h2>
          <p class="lead-sm">Run a Website Ops sweep after the Google connections are healthy, or select another evidence filter.</p>
        </div>
        <a class="btn btn--ghost" href="/admin/website-ops">Open Website Ops</a>
      </div>
    """
    recommendation_cards = "".join(
        f"""
        <article class="action-card">
          <div class="section-heading">
            <h3>{html.escape(str(item.get("query_cluster", "Validated cluster")))}</h3>
            <span class="status-pill {'status-ok' if item.get('execution_status') == 'eligible' else 'status-warn'}">{html.escape(str(item.get("execution_status", "shadow")).title())}</span>
          </div>
          <p class="lead-sm">{html.escape(str(item.get("reason", "")))}</p>
          <div class="mini-grid">
            {_mini_chip("Page", str(item.get("page_url", "")))}
            {_mini_chip("Target", str(item.get("target", "")))}
            {_mini_chip("Action", str(item.get("action_type", "")).replace("_", " ").title())}
          </div>
          {f'<p class="muted"><strong>Blocked:</strong> {html.escape(" ".join(str(reason) for reason in item.get("block_reasons", [])))}</p>' if item.get("block_reasons") else ''}
        </article>
        """
        for item in recommendations[:12]
    )
    body = f"""
      {_nav("queries", website_ops_section="queries", user=user)}
      <main id="agent-main-content" class="shell app-container app-page">
        <section class="card stack">
          <p class="eyebrow">Website Ops query intelligence</p>
          <h1>One query cluster. <span style="color:var(--accent)">One owning page.</span></h1>
          <p class="lead">Separate observed demand from hypotheses, catch cannibalization, and track whether answer engines cite Anata.</p>
          <div class="summary-grid">
            {_summary_chip("Canonical routes", summary.get("canonical_routes", 0), tone="neutral")}
            {_summary_chip("Unique intents", summary.get("unique_primary_intents", 0), tone="good" if summary.get("unique_primary_intents") else "neutral")}
            {_summary_chip("Duplicate intents", summary.get("duplicate_primary_intents", 0), tone="bad" if summary.get("duplicate_primary_intents") else "good")}
            {_summary_chip("Observed owners", summary.get("routes_with_observed_demand", 0), tone="neutral")}
            {_summary_chip("Validated", summary.get("validated_clusters", 0), tone="good")}
            {_summary_chip("Hypotheses", summary.get("hypothesis_clusters", 0), tone="neutral")}
            {_summary_chip("Quarantined", summary.get("quarantined_clusters", 0), tone="warn" if summary.get("quarantined_clusters") else "neutral")}
            {_summary_chip("Ownership conflicts", summary.get("ownership_conflicts", 0), tone="bad" if summary.get("ownership_conflicts") else "neutral")}
            {_summary_chip("Cited clusters", summary.get("cited_clusters", 0), tone="good" if summary.get("cited_clusters") else "neutral")}
            {_summary_chip("Shadow cycles", summary.get("weekly_validation_cycles", 0), tone="warn" if int(summary.get("weekly_validation_cycles", 0) or 0) < 2 else "good")}
          </div>
          <div class="button-row">
            <a class="{'btn' if not status_filter else 'btn btn--ghost'}" href="/admin/website-ops/queries">All</a>
            <a class="{'btn' if status_filter == 'validated' else 'btn btn--ghost'}" href="/admin/website-ops/queries?status=validated">Validated</a>
            <a class="{'btn' if status_filter == 'hypothesis' else 'btn btn--ghost'}" href="/admin/website-ops/queries?status=hypothesis">Hypotheses</a>
            <a class="{'btn' if status_filter == 'quarantined' else 'btn btn--ghost'}" href="/admin/website-ops/queries?status=quarantined">Quarantined</a>
            <a class="{'btn' if status_filter == 'conflict' else 'btn btn--ghost'}" href="/admin/website-ops/queries?status=conflict">Conflicts</a>
            <a class="{'btn' if status_filter == 'cited' else 'btn btn--ghost'}" href="/admin/website-ops/queries?status=cited">Cited</a>
          </div>
        </section>
        <section class="card stack">
          <div class="section-heading">
            <div class="stack">
              <p class="eyebrow">Continuous content gate</p>
              <h2>{html.escape(str(article_pipeline.get("status", "not calculated")).replace("_", " ").title())}</h2>
              <p class="lead-sm">{html.escape(str(article_pipeline.get("message", "Run a sweep to calculate article eligibility.")))}</p>
            </div>
            <span class="status-pill {'status-ok' if article_pipeline.get('status') == 'eligible' else 'status-warn'}">{html.escape(f"{article_pipeline.get('cycles_completed', 0)} / {article_pipeline.get('cycles_required', 2)} cycles")}</span>
          </div>
          <div class="summary-grid">
            {_summary_chip("Validated content gaps", article_pipeline.get("validated_informational_gaps", 0), tone="neutral")}
            {_summary_chip("Source-qualified", article_pipeline.get("source_qualified_candidates", 0), tone="good" if article_pipeline.get("source_qualified_candidates") else "neutral")}
            {_summary_chip("Execution", "Autopush after validation", tone="good")}
          </div>
          <p class="muted">Cycles are counted by distinct ISO week, not by number of button presses. This prevents duplicate runs from manufacturing confidence.</p>
        </section>
        <section class="grid-2">
          <div class="card stack">
            <p class="eyebrow">Authority signals</p>
            <h2>Earned citation monitoring</h2>
            <div class="summary-grid">
              {_summary_chip("Cited clusters", summary.get("cited_clusters", 0), tone="good" if summary.get("cited_clusters") else "neutral")}
              {_summary_chip("Citation gains", summary.get("citation_gains", 0), tone="good" if summary.get("citation_gains") else "neutral")}
              {_summary_chip("Citation losses", summary.get("citation_losses", 0), tone="warn" if summary.get("citation_losses") else "neutral")}
            </div>
            <p class="muted">Outreach can promote a verified original asset to a relevant editorial audience. It cannot buy, exchange, or automatically create ranking links.</p>
          </div>
          <div class="card stack">
            <p class="eyebrow">Learning loop</p>
            <h2>Observed business outcomes</h2>
            <div class="summary-grid">
              {_summary_chip("Observed pages", summary.get("observed_outcome_pages", 0), tone="neutral")}
              {_summary_chip("Associated lead growth", summary.get("pages_with_associated_lead_growth", 0), tone="good" if summary.get("pages_with_associated_lead_growth") else "neutral")}
            </div>
            <p class="muted">Before-and-after movement is association-only. Agent does not attribute a ranking or lead change to one publication without stronger evidence.</p>
          </div>
        </section>
        <section class="card stack">
          <div class="section-heading"><div class="stack"><h2>Canonical intent owners</h2><p class="lead-sm">Every public canonical route declares one primary intent. “Unobserved” means Search Console has not supplied demand evidence yet, not that the page should be deleted or rewritten.</p></div><span class="status-pill status-neutral">{html.escape(str(intent_coverage.get("status", "unavailable")).title())}</span></div>
          {f'<div class="data-workspace"><table class="data-table"><thead><tr><th>Route</th><th>Primary intent</th><th>Demand evidence</th><th>Clusters</th><th>Ownership</th></tr></thead><tbody>{intent_rows}</tbody></table></div>' if intent_rows else '<div class="list-card empty-state"><h3>Intent manifest unavailable.</h3><p class="muted">Agent will retain the last valid manifest and retry the public contract on the next sweep.</p></div>'}
        </section>
        <section class="card stack">
          <div class="section-heading"><div class="stack"><h2>Query map</h2><p class="lead-sm">{len(clusters)} matching cluster(s). Impressions appear only for observed Search Console evidence.</p></div></div>
          {f'<div class="data-workspace"><table class="data-table"><thead><tr><th>Query cluster</th><th>Validation</th><th>Evidence</th><th>Owning page</th><th>Ownership</th><th>Citation</th><th>Impressions</th></tr></thead><tbody>{"".join(rows)}</tbody></table></div>' if rows else empty}
        </section>
        <section class="card stack">
          <div class="section-heading"><div class="stack"><h2>Validated recommendations</h2><p class="lead-sm">Recommendations remain in shadow mode until two comparable weekly cycles and all production gates pass.</p></div></div>
          {recommendation_cards or '<p class="muted">No validated query recommendation is ready yet.</p>'}
        </section>
      </main>
    """
    return _page_shell("agent | Website Ops — Query Map", body)


def render_candidates_page(
    settings: Settings,
    *,
    state_filter: str = "",
    lane_filter: str = "",
    page: int = 1,
    user: dict | None = None,
) -> str:
    ledger = load_candidate_ledger(settings.website_ops_root)
    summary = dict(ledger.get("summary") or {})
    states = dict(summary.get("by_state") or {})
    lanes = [dict(item) for item in ledger.get("lanes", []) or []]
    filtered_candidates = [
        dict(item)
        for item in ledger.get("candidates", []) or []
        if (not state_filter or str(item.get("state", "")) == state_filter)
        and (not lane_filter or str(item.get("lane_id", "")) == lane_filter)
    ]
    page_size = 100
    total_filtered = len(filtered_candidates)
    total_pages = max(1, (total_filtered + page_size - 1) // page_size)
    current_page = min(max(1, page), total_pages)
    start_index = (current_page - 1) * page_size
    candidates = filtered_candidates[start_index : start_index + page_size]

    def candidate_target(value: Any) -> str:
        target = str(value or "").strip()
        parsed = urlparse(target)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            return '<span class="muted">Invalid target</span>'
        escaped = html.escape(target, quote=True)
        return f'<a class="text-link" href="{escaped}">{html.escape(target)}</a>'

    state_links = "".join(
        (
            f'<div class="summary-chip summary-{tone}">'
            f"<span>{html.escape(state.replace('_', ' ').title())}</span>"
            f'<strong><a class="text-link" href="/admin/website-ops/candidates?state={html.escape(state, quote=True)}">{int(count or 0)}</a></strong>'
            "</div>"
        )
        for state, count in states.items()
        if int(count or 0)
        for tone in [
            (
                "good"
                if state in {"completed", "learned"}
                else "bad"
                if state in {"failed", "rolling_back"}
                else "warn"
                if state in {"validated", "queued", "deferred", "verifying"}
                else "neutral"
            )
        ]
    )
    lane_rows = "".join(
        f"""
        <tr>
          <td><a class="text-link" href="/admin/website-ops/candidates?lane={html.escape(str(item.get('lane_id', '')), quote=True)}">{html.escape(str(item.get('label', '')))}</a></td>
          <td><span class="status-pill {'status-ok' if item.get('executor_status') == 'autonomous' else 'status-warn' if item.get('executor_status') == 'suggestion_only' else 'status-neutral'}">{html.escape(str(item.get('executor_status', '')).replace('_', ' ').title())}</span></td>
          <td>{int(item.get('candidate_count', 0) or 0)}</td>
          <td>{int(item.get('run_budget', 0) or 0)}</td>
          <td>{int(item.get('concurrency', 0) or 0)}</td>
          <td>{html.escape(str(item.get('validation', '')))}</td>
        </tr>
        """
        for item in lanes
    )
    candidate_rows = "".join(
        f"""
        <tr>
          <td><code>{html.escape(str(item.get('candidate_id', '')))}</code></td>
          <td><span class="status-pill {'status-ok' if item.get('state') in {'completed', 'learned'} else 'status-bad' if item.get('state') in {'failed', 'rolling_back'} else 'status-warn' if item.get('state') in {'validated', 'queued', 'deferred', 'verifying'} else 'status-neutral'}">{html.escape(str(item.get('state', '')).replace('_', ' ').title())}</span></td>
          <td>{html.escape(str(item.get('lane_label', '')))}</td>
          <td>{candidate_target(item.get('target_url')) if item.get('target_url') else '<span class="muted">Intent-level candidate</span>'}</td>
          <td>{html.escape(str(item.get('state_reason', '')))}</td>
          <td>{html.escape(str(item.get('required_gate', '')) or 'No remaining gate recorded.')}</td>
          <td>{html.escape(str(item.get('earliest_eligible_at', '')) or 'Next eligible run')}</td>
        </tr>
        """
        for item in candidates
    )
    filters = " · ".join(
        value
        for value in (
            f"State: {state_filter.replace('_', ' ').title()}" if state_filter else "",
            f"Lane: {lane_filter.replace('_', ' ').title()}" if lane_filter else "",
        )
        if value
    )
    filter_query = "&".join(
        value
        for value in (
            f"state={html.escape(state_filter, quote=True)}" if state_filter else "",
            f"lane={html.escape(lane_filter, quote=True)}" if lane_filter else "",
        )
        if value
    )

    def page_href(target_page: int) -> str:
        suffix = f"&{filter_query}" if filter_query else ""
        return f"/admin/website-ops/candidates?page={target_page}{suffix}"

    pagination = ""
    if total_pages > 1:
        previous = (
            f'<a class="secondary" href="{page_href(current_page - 1)}">Previous</a>'
            if current_page > 1
            else '<span class="status-pill status-neutral">Previous</span>'
        )
        following = (
            f'<a class="secondary" href="{page_href(current_page + 1)}">Next</a>'
            if current_page < total_pages
            else '<span class="status-pill status-neutral">Next</span>'
        )
        pagination = (
            f'<div class="row-actions"><div class="page-actions">{previous}{following}</div>'
            f'<span class="muted">Page {current_page} of {total_pages}</span></div>'
        )
    body = f"""
      {_nav('website_ops', website_ops_section='candidates', user=user)}
      <main id="agent-main-content" class="shell app-container app-page">
        <section class="page-header">
          <div class="stack">
            <p class="eyebrow">Website Ops</p>
            <h1>Candidate ledger</h1>
            <p class="lead">Every observed opportunity has one current state, one action lane, and one reason. Nothing disappears merely because it cannot run yet.</p>
          </div>
          <div class="page-actions"><a class="secondary" href="/admin/website-ops/candidates">Clear filters</a></div>
        </section>
        <section class="card stack">
          <div class="row-actions">
            <div class="stack"><h2>Improvement funnel</h2><p class="lead-sm">Counts are non-overlapping candidate states, not overlapping crawler observations.</p></div>
            <span class="status-pill status-neutral">{int(summary.get('total_candidates', 0) or 0)} candidates</span>
          </div>
          <div class="summary-grid">{state_links or _summary_chip("No candidates", "Run the daily sweep", tone="neutral")}</div>
        </section>
        <section class="card stack">
          <div class="row-actions"><div class="stack"><h2>Action-lane coverage</h2><p class="lead-sm">Capacity is a ceiling. Unsupported lanes remain visible and cannot execute.</p></div></div>
          <div class="data-workspace">
            <table class="data-table">
              <thead><tr><th>Lane</th><th>Executor</th><th>Candidates</th><th>Budget</th><th>Concurrency</th><th>Required proof</th></tr></thead>
              <tbody>{lane_rows}</tbody>
            </table>
          </div>
        </section>
        <section class="card stack">
          <div class="row-actions">
            <div class="stack"><h2>Candidate detail</h2><p class="lead-sm">{html.escape(filters or 'All current candidates')}</p></div>
            <span class="status-pill status-neutral">{len(candidates)} of {total_filtered} shown</span>
          </div>
          {pagination}
          {f'''
          <div class="data-workspace">
            <table class="data-table">
              <thead><tr><th>ID</th><th>State</th><th>Lane</th><th>Target</th><th>Reason</th><th>Remaining gate</th><th>Eligible</th></tr></thead>
              <tbody>{candidate_rows}</tbody>
            </table>
          </div>
          {pagination}
          ''' if candidates else '<div class="list-card empty-state"><h3>No candidates match this filter.</h3><p class="muted">Clear the filters or run a fresh daily sweep.</p></div>'}
        </section>
      </main>
    """
    return _page_shell("agent | Website Ops — Candidates", body)


def render_content_strategy_page(
    settings: Settings,
    *,
    stage_filter: str = "",
    user: dict | None = None,
) -> str:
    strategy = load_content_strategy(settings.website_ops_root)
    summary = dict(strategy.get("summary") or {})
    production_quota = dict(strategy.get("production_quota") or {})
    current_day = datetime.now(ZoneInfo("America/Denver")).date().isoformat()
    published_today = sum(
        1
        for item in load_feedback_records(settings)
        if str(
            item.get("action_type", "")
            or item.get("suggested_action_type", "")
        ).strip()
        == "publish_blog_article"
        and str(item.get("status", "")).strip().lower() == "done"
        and str(item.get("last_execution_at", "")).startswith(current_day)
    )
    daily_minimum = int(strategy.get("daily_article_minimum", 8) or 8)
    quota_met = published_today >= daily_minimum
    next_operation = dict(strategy.get("next_operation") or {})
    briefs = [
        dict(item)
        for item in strategy.get("briefs", []) or []
        if not stage_filter or str(item.get("stage", "")) == stage_filter
    ]
    stage_links = "".join(
        f'<a class="summary-chip summary-neutral" href="/admin/website-ops/strategy?stage={html.escape(stage, quote=True)}">'
        f"<span>{html.escape(label)}</span><strong>{int(summary.get(count_key, 0) or 0)}</strong></a>"
        for stage, label, count_key in (
            ("ready", "Ready to publish", "ready_to_publish"),
            ("researching", "Researching sources", "researching_sources"),
            ("scheduled", "Scheduled validation", "scheduled_for_validation"),
            ("improve_existing", "Improve existing", "improve_existing"),
            ("validating", "Validating", "validating"),
            ("blocked", "Blocked", "blocked"),
        )
    )
    rules = "".join(
        f"<li>{html.escape(str(item))}</li>"
        for item in strategy.get("operating_rules", []) or []
    )
    displayed_briefs = briefs[:100]
    rows = "".join(
        f"""
        <tr>
          <td><strong>{html.escape(str(item.get('topic', '')))}</strong><br><span class="muted">{html.escape(str(item.get('pillar', '')))}</span></td>
          <td>{html.escape(str(item.get('content_type', '')))}</td>
          <td><span class="status-pill {'status-ok' if item.get('stage') == 'ready' else 'status-warn' if item.get('stage') in {'researching', 'scheduled', 'validating'} else 'status-neutral'}">{html.escape(str(item.get('stage', '')).replace('_', ' ').title())}</span></td>
          <td>{f'<a class="text-link" href="{html.escape(str(item.get("owner_url", "")), quote=True)}">{html.escape(str(item.get("owner_url", "")))}</a>' if item.get('owner_url') else '<span class="muted">Owner required</span>'}</td>
          <td>{int(item.get('source_count', 0) or 0)}</td>
          <td>{html.escape(str(item.get('earliest_publish_date', '')) or 'Evidence driven')}</td>
          <td>{html.escape(str(item.get('next_operation', '')))}</td>
          <td>{html.escape(str(item.get('internal_link_plan', '')))}</td>
        </tr>
        """
        for item in displayed_briefs
    )
    body = f"""
      {_nav("website_ops", website_ops_section="strategy", user=user)}
      <main id="agent-main-content" class="shell app-container app-page">
        <section class="page-header">
          <div class="stack">
            <p class="eyebrow">Website Ops</p>
            <h1>Content strategy and publishing program</h1>
            <p class="lead">{html.escape(str(strategy.get('objective', 'Build qualified organic discovery with a durable content system.')))}</p>
          </div>
          <div class="page-actions"><a class="secondary" href="/admin/website-ops/strategy">Clear filters</a></div>
        </section>
        <section class="card stack">
          <div class="row-actions">
            <div class="stack"><h2>Today’s operating plan</h2><p class="lead-sm">Agent runs hourly from 8 AM through 3 PM Mountain. Each pulse audits, advances briefs, completes eligible fixes, and verifies production.</p></div>
            <span class="status-pill status-neutral">{int(strategy.get('daily_article_minimum', 8) or 8)} articles / day minimum · target {int(strategy.get('daily_article_target', 8) or 8)}</span>
          </div>
          <div class="summary-grid">
            {_summary_chip("Published today", published_today, tone="good" if quota_met else "warn")}
            {_summary_chip("Daily minimum", daily_minimum, tone="neutral")}
            {_summary_chip("Daily target", int(strategy.get('daily_article_target', 8) or 8), tone="neutral")}
            {_summary_chip("Generated today", int(production_quota.get('generated_today', summary.get('generated_today', 0)) or 0), tone="neutral")}
          </div>
          <div class="alert {'alert--success' if quota_met else 'alert--error'}">
            {'Daily publishing minimum met.' if quota_met else f'Daily publishing minimum is short by {daily_minimum - published_today}. The next pulse must backfill qualified inventory.'}
          </div>
          <div class="summary-grid">{stage_links}</div>
          {f'''
          <div class="list-card">
            <span class="status-pill status-warn">Next operation</span>
            <h3>{html.escape(str(next_operation.get('topic', 'No eligible topic yet.')))}</h3>
            <p>{html.escape(str(next_operation.get('next_operation', 'The next sweep will rebuild the strategy from current evidence.')))}</p>
            <p class="muted">Earliest publish: {html.escape(str(next_operation.get('earliest_publish_date', 'Evidence driven')) or 'Evidence driven')} · Sources: {int(next_operation.get('source_count', 0) or 0)} · Owner: {html.escape(str(next_operation.get('owner_url', 'Unassigned')) or 'Unassigned')}</p>
          </div>
          ''' if next_operation else ''}
        </section>
        <section class="grid-2">
          <div class="card stack"><h2>Publishing rules</h2><ul>{rules}</ul></div>
          <div class="card stack">
            <h2>What the agent does automatically</h2>
            <ol>
              <li>Collect query, page, citation, and conversion evidence.</li>
              <li>Assign one canonical owner and decide improve-versus-create.</li>
              <li>Maintain a source and internal-link brief.</li>
              <li>Generate and publish eight eligible source-backed articles per day, balanced as two articles across each of the four service pillars.</li>
              <li>Publish through GitHub, verify production, roll back failures, and measure outcomes.</li>
            </ol>
          </div>
        </section>
        <section class="card stack">
          <div class="row-actions">
            <div class="stack"><h2>Editorial work queue</h2><p class="lead-sm">{html.escape(('Stage: ' + stage_filter.replace('_', ' ').title()) if stage_filter else 'All current briefs')}</p></div>
            <span class="status-pill status-neutral">Showing {len(displayed_briefs)} of {len(briefs)} briefs</span>
          </div>
          {f'''
          <div class="data-workspace">
            <table class="data-table">
              <thead><tr><th>Topic and pillar</th><th>Work type</th><th>Stage</th><th>Current owner</th><th>Sources</th><th>Earliest publish</th><th>Next operation</th><th>Internal-link plan</th></tr></thead>
              <tbody>{rows}</tbody>
            </table>
          </div>
          ''' if briefs else '<div class="list-card empty-state"><h3>No briefs match this stage.</h3><p class="muted">Clear the filter or run a daily sweep.</p></div>'}
        </section>
      </main>
    """
    return _page_shell("agent | Website Ops — Content Strategy", body)


def render_queue_page(settings: Settings, *, flash_message: str = "", status_filter: str = "", user: dict | None = None) -> str:
    reports = _report_entries(settings)
    latest_payload = _report_payload(reports[0]) if reports else {}
    analytics_status = dict(latest_payload.get("analytics_status") or {})
    decision_ready = _decision_data_ready(analytics_status)
    normalized_filter = _feedback_status(status_filter) if status_filter else ""
    entries = _mvp_filter_feedback_records(load_feedback_records(settings))
    if normalized_filter:
        if normalized_filter == "approved":
            entries = [item for item in entries if item.get("status") in {"approved", "in-progress"}]
        else:
            entries = [item for item in entries if item.get("status") == normalized_filter]
    else:
        entries = [item for item in entries if item.get("status") not in {"done", "rejected"}]
    approved_count = sum(
        1 for item in load_feedback_records(settings)
        if str(item.get("status", "")).strip().lower() in {"approved", "in-progress"}
    )
    body = f"""
      {_nav("queue", website_ops_section="queue", user=user)}
      <main id="agent-main-content" class="shell app-container app-page">
        {f"<div class='flash'>{html.escape(flash_message)}</div>" if flash_message else ""}
        {("" if decision_ready else _operator_blocker_panel(analytics_status))}
        <section class="card stack">
          <p class="eyebrow">Website Ops queue</p>
          <h1>Website action <span style="color:var(--accent)">ledger</span>.</h1>
          <p class="lead">Inspect what Agent blocked, validated, published, verified, failed, or rolled back.</p>
          <div class="button-row" style="margin-top:8px">
            <a href="/admin/website-ops/queue" class="{'btn' if not normalized_filter else 'btn btn--ghost'}" style="font-size:13px">Needs review</a>
            <a href="/admin/website-ops/queue?status=approved" class="{'btn' if normalized_filter == 'approved' else 'btn btn--ghost'}" style="font-size:13px">Approved to run</a>
            <a href="/admin/website-ops/queue?status=done" class="{'btn' if normalized_filter == 'done' else 'btn btn--ghost'}" style="font-size:13px">Completed</a>
            <a href="/admin/website-ops/queue?status=error" class="{'btn' if normalized_filter == 'error' else 'btn btn--ghost'}" style="font-size:13px">Failed</a>
            <a href="/admin/website-ops/queue?status=rejected" class="{'btn' if normalized_filter == 'rejected' else 'btn btn--ghost'}" style="font-size:13px">Rejected</a>
            <form class="inline" action="/admin/api/website-ops/actions/execute-approved" method="post">
              <button type="submit" {('' if decision_ready and approved_count else 'disabled aria-disabled="true"')}>Execute approved now</button>
            </form>
          </div>
        </section>
        <section class="card stack">
          {_feedback_cards(entries, with_actions=True, empty_context=normalized_filter, decision_data_ready=decision_ready)}
        </section>
      </main>
    """
    return _page_shell("agent | Website Ops — Queue", body)


def render_feedback_detail_page(settings: Settings, feedback_id: str, *, flash_message: str = "", user: dict | None = None) -> str:
    record = get_feedback_record(settings, feedback_id)
    if not record:
        return _page_shell("Not Found", f"{_nav('queue', website_ops_section='queue', user=user)}<main id='agent-main-content' class='shell app-container app-page'><section class='card'><h1>Not found</h1><p class='lead'>The feedback record could not be located.</p></section></main>")
    is_auto_generated = bool(record.get("auto_generated"))
    confidence = str(record.get("confidence", "")).strip()
    suggested_action_type = str(record.get("suggested_action_type", "")).strip()
    is_auto_executable = _record_is_auto_executable(record)
    recommendation_cta = "Approve safe action" if is_auto_executable else "Approve recommendation"
    recommendation_note = (
        "This recommendation maps to a supported safe action. Approving it will execute immediately when auto-execution is enabled."
        if is_auto_executable
        else "This recommendation will move into the approved queue. Use the form below only if you want to override or add execution details."
    )
    workflow_notice = ""
    if record.get("status") == "approved":
        workflow_notice = "<div class='flash'>Approved for implementation. This item should remain out of awaiting review until it is completed or reopened.</div>"
    elif record.get("status") == "done":
        executed_at = str(record.get("last_execution_at", "") or "").strip()
        execution_result = record.get("execution_result") if isinstance(record.get("execution_result"), dict) else {}
        execution_type = str((execution_result or {}).get("action_type", "") or "").strip()
        detail_bits = []
        if executed_at:
            detail_bits.append(f"Executed at {html.escape(executed_at)}.")
        if execution_type:
            detail_bits.append(f"Action: {html.escape(_humanize_label(execution_type) or execution_type)}.")
        workflow_notice = f"<div class='flash'>Completed successfully. {' '.join(detail_bits)}</div>"
    elif record.get("status") == "error":
        workflow_notice = f"<div class='flash'>{html.escape(str(record.get('execution_error', '') or 'The last execution failed.'))}</div>"
    body = f"""
      {_nav("queue", website_ops_section="queue", user=user)}
      <main id="agent-main-content" class="shell app-container app-page">
        {f"<div class='flash'>{html.escape(flash_message)}</div>" if flash_message else ""}
        {workflow_notice}
        <section class="detail-layout">
          <aside class="card stack">
            <p class="eyebrow">Feedback record</p>
            <h2>{html.escape(str(record.get('summary', 'Feedback item')))}</h2>
            {_status_chip(str(record.get('status', 'new')))}
            <p class="lead">{html.escape(str(record.get('page_url', '') or 'No page specified'))}</p>
            <p class="muted">Priority: {html.escape(str(record.get('priority', 'Medium')))}</p>
            <p class="muted">Category: {html.escape(str(record.get('category', 'General')))}</p>
            {f"<p class='muted'>Source: {html.escape(str(record.get('source_insight', '') or 'Website Ops'))}</p>" if record.get('source_insight') else ""}
          </aside>
          <section class="card stack">
            <p class="lead"><strong>Details:</strong> {html.escape(str(record.get('details', '') or 'No details provided.'))}</p>
            <p class="lead"><strong>Desired outcome:</strong> {html.escape(str(record.get('desired_outcome', '') or 'Not specified.'))}</p>
            <p class="lead"><strong>Recommended fix:</strong> {html.escape(str(record.get('recommended_fix', '') or 'Not specified.'))}</p>
            {f"<div class='diff-grid'><div class='diff-block'><p class='eyebrow'>Current state</p><p>{html.escape(str(record.get('before_state', '') or 'Not captured.'))}</p></div><div class='diff-block'><p class='eyebrow'>Proposed update</p><p>{html.escape(str(record.get('after_state', '') or record.get('desired_outcome', '') or 'Not specified.'))}</p></div></div>" if record.get('before_state') or record.get('after_state') else ""}
            {f"<div class='summary-grid'>{_summary_chip('Section', record.get('section_name', 'General'), tone='neutral')}{_summary_chip('Confidence', confidence.title() if confidence else 'Medium', tone='neutral')}{_summary_chip('Suggested action', _humanize_label(suggested_action_type) or 'Manual review', tone='neutral')}{_summary_chip('Execution', 'Auto-executable' if is_auto_executable else 'Approval only', tone='neutral')}{_summary_chip('Target region', record.get('target_region', 'Page region'), tone='neutral')}{_summary_chip('GA4 trust', str(record.get('ga4_trust_status', 'missing')).title(), tone='neutral')}</div>" if is_auto_generated else ""}
            {f"<ul class='compact-list'>{''.join(f'<li>{html.escape(str(line))}</li>' for line in (record.get('evidence') or [])[:4])}</ul>" if is_auto_generated and record.get('evidence') else ""}
            {f"<p class='muted'><strong>Verification:</strong> {html.escape('; '.join(str(line) for line in (record.get('verification_requirements') or [])))}</p>" if is_auto_generated and record.get('verification_requirements') else ""}
            {f"<div class='button-row'><form class='inline' action='/admin/api/website-ops/feedback/{html.escape(str(record.get('feedback_id', '')), quote=True)}/review' method='post'><input type='hidden' name='status' value='approved'><button type='submit'>{recommendation_cta}</button></form><form class='inline' action='/admin/api/website-ops/feedback/{html.escape(str(record.get('feedback_id', '')), quote=True)}/review' method='post'><input type='hidden' name='status' value='rejected'><button class='ghost' type='submit'>Reject Recommendation</button></form><span class='muted'>{html.escape(recommendation_note)}</span></div>" if is_auto_generated else ""}
            <form action="/admin/api/website-ops/feedback/{html.escape(str(record.get('feedback_id', '')), quote=True)}/review" method="post" class="form-grid">
              <div><label>Status</label><select name="status">
                <option value="new" {'selected' if record.get('status') == 'new' else ''}>Needs review</option>
                <option value="approved" {'selected' if record.get('status') == 'approved' else ''}>Approved to run</option>
                <option value="in-progress" {'selected' if record.get('status') == 'in-progress' else ''}>Running</option>
                <option value="done" {'selected' if record.get('status') == 'done' else ''}>Completed</option>
                <option value="rejected" {'selected' if record.get('status') == 'rejected' else ''}>Rejected</option>
                <option value="error" {'selected' if record.get('status') == 'error' else ''}>Failed</option>
              </select></div>
              <div><label>Reviewer</label><input type="text" name="reviewer_name" value="{html.escape(str(record.get('reviewer_name', '')), quote=True)}"></div>
              <div><label>Safe action</label><select name="action_type"><option value="">Manual review only</option><option value="inject_faq_block" {'selected' if record.get('action_type') == 'inject_faq_block' else ''}>Add FAQ block</option><option value="expand_service_page_section" {'selected' if record.get('action_type') == 'expand_service_page_section' else ''}>Expand service page section</option></select></div>
              <div><label>WordPress page ID</label><input type="text" name="target_post_id" value="{html.escape(str(record.get('target_post_id', '')), quote=True)}" placeholder="Optional WordPress page ID"></div>
              <div class="span-2"><label>Exact content or change</label><textarea name="action_value" placeholder="Paste the exact content, JSON payload, or implementation note.">{html.escape(str(record.get('action_value', '')))}</textarea></div>
              <div class="span-2"><label>Review notes</label><textarea name="review_notes">{html.escape(str(record.get('review_notes', '')))}</textarea></div>
              <div class="span-2"><button type="submit">Save decision</button></div>
            </form>
          </section>
        </section>
      </main>
    """
    return _page_shell("agent | Website Ops — Feedback", body)


def render_reports_page(settings: Settings, *, user: dict | None = None) -> str:
    reports = _report_entries(settings)
    body = f"""
      {_nav("reports", website_ops_section="reports", user=user)}
      <main id="agent-main-content" class="shell app-container app-page">
        <section class="card stack">
          <p class="eyebrow">Website Ops reports</p>
          <h1>Daily, weekly, and monthly <span style="color:var(--accent)">history</span>.</h1>
          <p class="lead">Every report is generated into the agent’s runtime workspace and remains available for review.</p>
        </section>
        <section class="card stack">
          {_report_cards(reports)}
        </section>
      </main>
    """
    return _page_shell("agent | Website Ops — Reports", body)


def _blocked_report_page(
    entry: Mapping[str, Any],
    payload: Mapping[str, Any],
    *,
    user: dict | None = None,
) -> str:
    analytics_status = dict(payload.get("analytics_status") or {})
    page_insights = [dict(item) for item in list(payload.get("page_insights") or [])[:10]]
    for item in page_insights:
        item["score"] = None
        item["bucket"] = "data unavailable"
        item["metric_availability"] = {
            "search_console": "unavailable",
            "ga4": "unavailable",
        }
        item["task_block_reason"] = (
            "Decision data was unavailable for this run. No ranking-led conclusion is valid."
        )
    technical_status = str(payload.get("status", "unknown") or "unknown").replace("-", " ")
    body = f"""
      {_nav("reports", website_ops_section="reports", user=user)}
      <main id="agent-main-content" class="shell app-container app-page">
        <p class="breadcrumb"><a href="/admin/website-ops/reports">← All reports</a></p>
        <section class="card stack">
          <p class="eyebrow">{html.escape(str(entry.get('mode', '')).title())} report · archived evidence</p>
          <h1>{html.escape(str(entry.get('title', 'Website Ops report')))}</h1>
          <p class="lead">This report predates corrected missing-data semantics. Its raw performance scores are suppressed because Search Console and GA4 were unavailable.</p>
        </section>
        {_operator_blocker_panel(analytics_status)}
        <section class="card stack">
          <div class="section-heading">
            <div><p class="eyebrow">Verified scope</p><h2>Technical crawl only</h2></div>
            <span class="status-pill status-warn">Archived · decision data unavailable</span>
          </div>
          <div class="summary-grid">
            {_summary_chip("Pages reviewed", payload.get("pages_reviewed", 0), tone="neutral")}
            {_summary_chip("Technical crawl", technical_status, tone="good" if technical_status == "healthy" else "warn")}
            {_summary_chip("Ranking operations", "Blocked", tone="bad")}
            {_summary_chip("Changes applied", payload.get("changes_applied", 0), tone="neutral")}
          </div>
        </section>
        <section class="card stack">
          <h2>Decision-data sources</h2>
          <div class="setup-grid">{_analytics_connection_cards(analytics_status)}</div>
        </section>
        <section class="card stack">
          <h2>Page evidence</h2>
          <p class="lead-sm">Technical observations remain available. Search and conversion values are withheld rather than represented as zero.</p>
          <div class="widget-scroll">{_insight_snapshot_cards(page_insights)}</div>
        </section>
      </main>
    """
    return _page_shell(str(entry.get("title", "Website Ops report")), body)


def render_report_page(settings: Settings, mode: str, slug: str, *, user: dict | None = None) -> str:
    entry = get_report_entry(settings, mode, slug)
    if not entry:
        return _page_shell("Not Found", f"{_nav('reports', website_ops_section='reports', user=user)}<main id='agent-main-content' class='shell app-container app-page'><section class='card'><h1>Not found</h1><p class='lead'>The requested report was not found.</p></section></main>")
    payload = _mvp_filter_report_payload(_report_payload(entry))
    analytics_status = dict(payload.get("analytics_status") or {})
    if analytics_status and not _decision_data_ready(analytics_status):
        return _blocked_report_page(entry, payload, user=user)
    debug_insights = list(payload.get("page_insights") or [])[:6]
    debug_panel = ""
    if MVP_MODE_ACTIVE and debug_insights:
        debug_panel = (
            "<section style=\"max-width:1320px;margin:24px auto 0;padding:0 24px;\">"
            "<div style=\"background:#fff;border:1px solid rgba(25,55,109,0.12);border-radius:24px;padding:24px;box-shadow:0 16px 40px rgba(24,39,75,0.08);\">"
            "<p style=\"margin:0 0 8px;font-size:12px;letter-spacing:0.16em;text-transform:uppercase;color:#5d6b82;\">Generation trace</p>"
            "<h2 style=\"margin:0 0 12px;font-size:28px;line-height:1.1;color:#16233b;\">Per-page generation trace</h2>"
            f"{_insight_snapshot_cards(debug_insights)}"
            "</div></section>"
        )
    html_path = entry["html_path"]
    if html_path.exists():
        rendered = _inject_admin_nav_into_report_html(html_path.read_text(), active="reports", user=user)
        banner = _mvp_mode_banner() if MVP_MODE_ACTIVE else ""
        breadcrumb = '<p class="breadcrumb" style="font-size:14px;margin-bottom:18px;color:rgba(43,54,68,0.60)"><a href="/admin/website-ops/reports" style="color:rgba(43,54,68,0.60);text-decoration:none">← All reports</a></p>'
        return rendered.replace('<div class="admin-report-shell">', f'<div class="admin-report-shell">{breadcrumb}{banner}{debug_panel}', 1)
    markdown_path = entry["path"]
    return _page_shell(
        entry["title"],
        f"{_nav('reports', website_ops_section='reports', user=user)}<main id='agent-main-content' class='shell app-container app-page'><section class='card stack'>{_mvp_mode_banner() if MVP_MODE_ACTIVE else ''}<p class='eyebrow'>{html.escape(mode.title())}</p><h1>{html.escape(entry['title'])}</h1><pre>{html.escape(markdown_path.read_text())}</pre></section>{debug_panel}</main>",
    )
