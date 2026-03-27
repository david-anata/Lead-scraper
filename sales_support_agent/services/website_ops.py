"""Website ops dashboard and execution helpers for the agent admin app."""

from __future__ import annotations

import html
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from sales_support_agent.config import Settings
from sales_support_agent.services.admin_nav import render_agent_nav, render_agent_nav_styles
from sales_support_agent.services.website_ops_autonomy import build_autonomy_overlay
from sales_support_agent.services import website_ops_vendor as website_ops


@dataclass(frozen=True)
class WebsiteOpsActionResult:
    ok: bool
    message: str
    report: dict[str, Any] | None = None
    record: dict[str, Any] | None = None


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


def _feedback_status(value: str) -> str:
    normalized = re.sub(r"[^a-z]+", "-", str(value or "").strip().lower()).strip("-")
    return normalized or "new"


def _feedback_status_label(value: str) -> str:
    labels = {
        "new": "New",
        "approved": "Approved",
        "in-progress": "In Progress",
        "done": "Done",
        "rejected": "Rejected",
        "error": "Error",
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
    for path in sorted(candidates, key=lambda item: item.stat().st_mtime, reverse=True):
        try:
            text = path.read_text()
        except OSError:
            continue
        metadata = _extract_report_metadata(text, path)
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
                "modified": datetime.fromtimestamp(path.stat().st_mtime).astimezone().strftime("%Y-%m-%d %H:%M %Z"),
            }
        )
    return entries


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
    existing_by_key = {
        str(record.get("automation_key", "")).strip(): record
        for record in existing_records
        if str(record.get("automation_key", "")).strip()
    }
    synced_items: list[dict[str, Any]] = []
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
            "source_insight": str(item.get("insight_source", "")).strip(),
            "section_name": str(item.get("section_name", "")).strip(),
            "before_state": str(item.get("before_state", "")).strip(),
            "after_state": str(item.get("after_state", "")).strip(),
            "expected_impact": str(item.get("expected_impact", "")).strip(),
            "confidence": str(item.get("confidence", "")).strip(),
            "requires_approval": bool(item.get("requires_approval")),
            "suggested_action_type": str(item.get("action_type", "")).strip(),
            "suggested_action_value": str(item.get("action_value", "")).strip(),
        }
        existing = existing_by_key.get(automation_key)
        if existing:
            record = website_ops.update_feedback_entry(existing, base_payload)
            record["feedback_id"] = existing.get("feedback_id") or Path(str(existing.get("_path", ""))).stem
        else:
            record = save_feedback_record(settings, base_payload)
            existing_by_key[automation_key] = record
        synced["feedback_id"] = str(record.get("feedback_id", "")).strip()
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
    for key in ("section_name", "before_state", "after_state", "expected_impact", "confidence", "requires_approval", "suggested_action_type", "suggested_action_value"):
        if key in payload:
            entry[key] = payload[key]
    path = website_ops.save_feedback_entry(entry, config=config)
    record = json.loads(path.read_text())
    record["_path"] = str(path)
    record["feedback_id"] = Path(path).stem
    record["status"] = _feedback_status(str(record.get("status", "")))
    return record


def _is_auto_executable_action(action_type: str) -> bool:
    return action_type.strip() == "replace_primary_heading"


def _autofill_review_updates(existing: Mapping[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    updates = dict(payload)
    status = _feedback_status(str(updates.get("status", "")))
    if status != "approved" or str(updates.get("action_type", "")).strip():
        return updates
    suggested_action_type = str(existing.get("suggested_action_type", "")).strip()
    if not _is_auto_executable_action(suggested_action_type):
        return updates
    updates["action_type"] = suggested_action_type
    suggested_action_value = str(existing.get("suggested_action_value", "")).strip()
    if suggested_action_value and not str(updates.get("action_value", "")).strip():
        updates["action_value"] = suggested_action_value
    if not str(updates.get("target_post_id", "")).strip() and str(existing.get("target_post_id", "")).strip():
        updates["target_post_id"] = str(existing.get("target_post_id", "")).strip()
    return updates


def review_feedback_record(settings: Settings, feedback_id: str, payload: dict[str, Any]) -> WebsiteOpsActionResult:
    existing = get_feedback_record(settings, feedback_id)
    if not existing:
        return WebsiteOpsActionResult(ok=False, message="Feedback record not found.")
    payload = _autofill_review_updates(existing, payload)
    updates = {
        "status": _feedback_status(str(payload.get("status", ""))),
        "reviewer_name": str(payload.get("reviewer_name", "")).strip(),
        "review_notes": str(payload.get("review_notes", "")).strip(),
        "action_type": str(payload.get("action_type", "")).strip(),
        "action_value": str(payload.get("action_value", "")).strip(),
        "target_post_id": str(payload.get("target_post_id", "")).strip(),
        "reviewed_at": datetime.now(timezone.utc).isoformat(),
    }
    record = website_ops.update_feedback_entry(existing, updates)
    if settings.website_ops_execute_approved and record.get("status") == "approved" and record.get("action_type"):
        try:
            result = website_ops.execute_feedback_action(record, config=_config(settings))
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


def run_website_ops(settings: Settings, *, mode: str = "daily") -> WebsiteOpsActionResult:
    config = _config(settings)
    feedback_entries = load_feedback_records(settings)
    executed_actions: list[dict[str, Any]] = []
    if settings.website_ops_execute_approved:
        for record in feedback_entries:
            if record.get("status") != "approved" or not record.get("action_type"):
                continue
            try:
                result = website_ops.execute_feedback_action(record, config=config)
            except website_ops.ExecutionError as exc:
                website_ops.update_feedback_entry(
                    record,
                    {
                        "status": "error",
                        "execution_error": str(exc),
                        "last_execution_at": datetime.now(timezone.utc).isoformat(),
                    },
                )
            else:
                executed_actions.append(result)
                website_ops.update_feedback_entry(
                    record,
                    {
                        "status": "done",
                        "last_execution_at": result["executed_at"],
                        "execution_result": result,
                    },
                )
        feedback_entries = load_feedback_records(settings)

    report_title = {
        "daily": "Anata Website Ops Daily Report",
        "weekly": "Anata Website Ops Weekly Report",
        "monthly": "Anata Website Ops Monthly Report",
    }[mode]
    output_dir = settings.website_ops_root / "reports" / mode
    pipeline = website_ops.run_daily_report_pipeline(
        list(settings.website_ops_site_urls),
        config=config,
        output_dir=output_dir,
        feedback_entries=feedback_entries,
        title=report_title,
        report_type=f"website_ops_{mode}",
        scope=f"agent-admin {mode} sweep",
        notes=[
            f"Run mode: {mode}.",
            f"Monitored URLs: {len(settings.website_ops_site_urls)}.",
            f"Feedback loaded: {len(feedback_entries)}.",
            f"Changes applied: {len(executed_actions)}.",
        ],
        report_date=datetime.now(timezone.utc).date().isoformat(),
        executed_actions=executed_actions,
    )
    enriched_report = dict(pipeline["report"])
    enriched_report.update(
        build_autonomy_overlay(
            settings=settings,
            report=enriched_report,
            observations=list(pipeline.get("observations") or []),
            feedback_entries=feedback_entries,
        )
    )
    enriched_report["action_queue"] = _sync_action_queue_feedback(
        settings,
        list(enriched_report.get("action_queue") or []),
        feedback_entries,
        report_slug=_slugify_text(report_title),
    )
    artifacts = website_ops.write_daily_report_artifacts(enriched_report, output_dir=output_dir, config=config)
    return WebsiteOpsActionResult(
        ok=True,
        message=f"{mode.title()} website ops run completed.",
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
          {identity_block}
        </article>
        """,
    ]
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
          <p class="muted">Website Ops does not need a team intervention from the latest run beyond normal approval review.</p>
        </article>
        """
    return "".join(
        f"""
        <article class="task-card">
          <div class="row-actions">
            <h3>Team action</h3>
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
    stats = [
        ("Pages reviewed", payload.get("pages_reviewed", "0"), "neutral"),
        ("Healthy", payload.get("pages_healthy", "0"), "good"),
        ("Needs work", payload.get("pages_with_issues", "0"), "warn" if int(payload.get("pages_with_issues", 0) or 0) else "neutral"),
        ("Issues found", payload.get("issues_found", "0"), "warn" if int(payload.get("issues_found", 0) or 0) else "neutral"),
        ("Status", status.replace("-", " "), "bad" if status == "needs-attention" else "good"),
    ]
    return f"""
    <div class="card stack">
      <h2>Latest report</h2>
      <div class="summary-grid">
        {''.join(_summary_chip(label, value, tone=tone) for label, value, tone in stats)}
      </div>
      <div class="button-row">
        <a href="/admin/website-ops/reports/{html.escape(entry['mode'], quote=True)}/{html.escape(entry['slug'], quote=True)}" class="text-link">Open {html.escape(entry['title'])}</a>
      </div>
    </div>
    """


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
        {_mini_chip("Monitored Pages", len(settings.website_ops_site_urls))}
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


def _dashboard_auto_run_script(latest: dict[str, Any] | None) -> str:
    latest_date = str((latest or {}).get("date", "") or "")
    today = date.today().isoformat()
    if latest_date == today:
        return ""
    return f"""
    <script>
      (function () {{
        const todayKey = "website-ops-auto-run-{today}";
        if (window.localStorage && window.localStorage.getItem(todayKey) === "done") {{
          return;
        }}
        const body = new URLSearchParams({{mode: "daily"}});
        fetch("/admin/api/website-ops/run", {{
          method: "POST",
          headers: {{"Content-Type": "application/x-www-form-urlencoded"}},
          body: body.toString(),
          credentials: "same-origin"
        }}).then(function () {{
          if (window.localStorage) {{
            window.localStorage.setItem(todayKey, "done");
          }}
          window.location.reload();
        }}).catch(function () {{
          if (window.localStorage) {{
            window.localStorage.setItem(todayKey, "failed");
          }}
        }});
      }})();
    </script>
    """


def _action_queue_cards(action_queue: list[dict[str, Any]]) -> str:
    if not action_queue:
        return "<div class='list-card'><p class='muted'>No action queue generated yet.</p></div>"
    cards = []
    for item in action_queue:
        confidence = str(item.get("confidence", "medium")).strip().lower() or "medium"
        requires_approval = bool(item.get("requires_approval"))
        cards.append(
            f"""
            <article class="action-card">
              <div class="row-actions">
                {_action_source_chip(str(item.get("insight_source", "System")))}
                <div class="chip-row">
                  <span class="status-pill {'status-warn' if requires_approval else 'status-ok'}">{'Approval required' if requires_approval else 'Safe to apply'}</span>
                  <span class="status-pill status-neutral">{html.escape(confidence.title())} confidence</span>
                </div>
              </div>
              <h3>{html.escape(str(item.get("page_title") or _short_page_label(str(item.get("page_url", "")))))}</h3>
              <p class="muted">{html.escape(_short_page_label(str(item.get("page_url", ""))))}</p>
              <div class="mini-grid">
                {_mini_chip("Section", str(item.get("section_name", "Unspecified section")))}
                {_mini_chip("Impact", str(item.get("expected_impact", "Improves performance against the current goal.")))}
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
              {f"<div class='button-row'><a class='text-link' href='/admin/website-ops/feedback/{html.escape(str(item.get('feedback_id', '')), quote=True)}'>Open review item</a></div>" if item.get('feedback_id') else ""}
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
        cards.append(
            f"""
            <article class="insight-card">
              <div class="row-actions">
                <h3>{html.escape(str(item.get("page_title") or _short_page_label(str(item.get("page_url", "")))))}</h3>
                <span class="status-pill status-neutral">Score {html.escape(str(item.get("score", "")))}</span>
              </div>
              <p class="muted">{html.escape(_short_page_label(str(item.get("page_url", ""))))}</p>
              <div class="mini-grid">
                {_mini_chip("Bucket", str(item.get("bucket", "hold")).title())}
                {_mini_chip("Impressions", int((item.get("search_console") or {}).get("impressions", 0)))}
                {_mini_chip("CTR", f"{round(float((item.get('search_console') or {}).get('ctr', 0) or 0) * 100, 2)}%")}
                {_mini_chip("Sessions", int((item.get("ga4") or {}).get("sessions", 0)))}
                {_mini_chip("Conversions", int((item.get("ga4") or {}).get("conversions", 0)))}
              </div>
              {top_query}
              {insights}
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
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Montserrat:wght@700;800&display=swap" rel="stylesheet">
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
      .shell {{ max-width: 1180px; margin: 0 auto; padding: 28px 18px 64px; display: grid; gap: 20px; }}
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
      @media (max-width: 900px) {{
        .hero, .grid-2, .detail-layout, .stats, .form-grid, .setup-grid, .diff-grid, .mini-grid {{ grid-template-columns: 1fr; }}
        .shell {{ width: auto; padding: 24px 12px 48px; }}
        .help-copy {{ right: auto; left: 0; width: min(300px, 70vw); }}
      }}
    </style>
  </head>
  <body>
    {body}
</body>
</html>"""


def _nav(active: str = "website_ops", *, website_ops_section: str = "") -> str:
    return render_agent_nav(active, website_ops_section=website_ops_section)


def _inject_admin_nav_into_report_html(report_html: str, *, active: str = "reports") -> str:
    nav_styles = render_agent_nav_styles()
    font_links = """
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Montserrat:wght@700;800&display=swap" rel="stylesheet">
    """
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
        max-width: 1180px;
        margin: 0 auto;
        padding: 28px 18px 64px;
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
        injected = injected.replace("</head>", f"{font_links}{nav_styles}{shell_styles}</head>", 1)
    if "<body" in injected:
        injected = re.sub(
            r"(<body[^>]*>)",
            r"\1" + render_agent_nav(active) + '<div class="admin-report-shell">',
            injected,
            count=1,
            flags=re.IGNORECASE,
        )
        if "</body>" in injected:
            injected = injected.replace("</body>", "</div></body>", 1)
    return injected


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
              <p class="muted">{html.escape(entry.get('excerpt', '') or 'No summary available.')}</p>
            </article>
            """
        )
    return "".join(cards)


def _feedback_cards(entries: list[dict[str, Any]], *, with_actions: bool = False) -> str:
    if not entries:
        return "<div class='list-card'><p class='muted'>No feedback records yet.</p></div>"
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
                for status, label in [("approved", "Approve"), ("in-progress", "In Progress"), ("rejected", "Reject"), ("done", "Done")]
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


def render_dashboard_page(settings: Settings, *, flash_message: str = "") -> str:
    reports = _report_entries(settings)
    latest = reports[0] if reports else None
    latest_payload = _report_payload(latest) if latest else {}
    feedback = load_feedback_records(settings)
    status_counts: dict[str, int] = {}
    for item in feedback:
        status_counts[item["status"]] = status_counts.get(item["status"], 0) + 1
    action_queue = list(latest_payload.get("action_queue") or [])[:6]
    support_requests = list(latest_payload.get("support_requests") or [])[:5]
    page_insights = list(latest_payload.get("page_insights") or [])[:5]
    analytics_status = latest_payload.get("analytics_status") or {}
    today = date.today().isoformat()
    latest_date = str(latest.get("date", "") if latest else "")
    auto_refresh_note = (
        "<p class='muted'>Refreshing today’s Website Ops signals automatically on load.</p>"
        if latest_date != today
        else ""
    )
    body = f"""
      {_nav("website_ops", website_ops_section="seo_dashboard")}
      <main class="shell">
        {f"<div class='flash'>{html.escape(flash_message)}</div>" if flash_message else ""}
        <section class="hero">
          <div class="card stack">
            <p class="eyebrow">Website Ops</p>
            <h1>SEO <span style="color:var(--accent)">control tower</span>.</h1>
            <p class="lead">Review daily website reports, approve changes, and route safe live actions through the same internal agent dashboard your team already uses.</p>
            <div class="button-row">
              <form action="/admin/api/website-ops/run" method="post"><input type="hidden" name="mode" value="daily"><button type="submit">Run Daily Sweep</button></form>
              <form action="/admin/api/website-ops/run" method="post"><input type="hidden" name="mode" value="weekly"><button class="ghost" type="submit">Run Weekly Sweep</button></form>
              <a href="/admin/website-ops/reports/latest" style="text-decoration:none;"><button class="ghost" type="button">Open Latest Report</button></a>
            </div>
            {auto_refresh_note}
          </div>
          <div class="card stack">
            <p class="eyebrow">Current scope</p>
            <div class="summary-grid">
              {_summary_chip("Monitored Pages", len(settings.website_ops_site_urls), tone="neutral")}
              {_summary_chip("Auto execution", "Enabled" if settings.website_ops_execute_approved else "Disabled", tone="good" if settings.website_ops_execute_approved else "warn")}
              {_connection_summary_chips(analytics_status)}
            </div>
            <p class="muted">Core system status only. Full connection and developer details are lower on the page.</p>
          </div>
        </section>
        <section class="stats">
          {_dashboard_stat_card("Reports", len(reports), "Daily, weekly, monthly", "/admin/website-ops/reports")}
          {_dashboard_stat_card("Awaiting Review", status_counts.get('new', 0), "Needs a decision", "/admin/website-ops/queue?status=new")}
          {_dashboard_stat_card("Approved", status_counts.get('approved', 0), "Ready for action", "/admin/website-ops/queue?status=approved")}
          {_dashboard_stat_card("Done", status_counts.get('done', 0), "Completed safely", "/admin/website-ops/queue?status=done")}
        </section>
        <section class="grid-2">
          <div class="card stack">
            <p class="eyebrow">Primary goal</p>
            <h2>{html.escape(str((latest_payload.get('goal') or {}).get('primary', 'Increase qualified organic leads with less manual website work.')))}</h2>
            <p class="lead">This is the system objective the dashboard should optimize against, not just a list of page checks.</p>
          </div>
          <div class="card stack">
            <p class="eyebrow">How the team helps</p>
            <p class="lead">These are the manual decisions or assets Website Ops still needs from the team.</p>
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
            <p class="lead">Each card shows the page, exact section, current state, proposed state, and why the change supports the goal.</p>
            <div class="button-row">
              <a href="/admin/website-ops/queue" class="text-link">Open approval queue</a>
              <span class="muted">Approve tasks there, then the next run executes the approved safe actions.</span>
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
          <div class="card stack"><h2>Open queue</h2><div class="widget-scroll compact-scroll">{_feedback_cards(feedback[:8], with_actions=True)}</div></div>
          <div class="card stack"><h2>Recent reports</h2><div class="widget-scroll compact-scroll">{_report_cards(reports[:8])}</div></div>
        </section>
        <section class="grid-2">
          <div class="card stack"><h2>Data connection notes</h2><p class="lead">Website Ops uses these signals to decide what to change next.</p><div class="setup-grid">{_analytics_connection_cards(analytics_status)}</div></div>
          {_system_details_panel(settings, analytics_status)}
        </section>
      </main>
      {_dashboard_auto_run_script(latest)}
    """
    return _page_shell("Agent Website Ops", body)


def render_queue_page(settings: Settings, *, flash_message: str = "", status_filter: str = "") -> str:
    normalized_filter = _feedback_status(status_filter) if status_filter else ""
    entries = load_feedback_records(settings)
    if normalized_filter:
        entries = [item for item in entries if item.get("status") == normalized_filter]
    else:
        entries = [item for item in entries if item.get("status") not in {"done", "rejected"}]
    queue_title = _humanize_label(normalized_filter) if normalized_filter else "Active"
    body = f"""
      {_nav("queue", website_ops_section="queue")}
      <main class="shell">
        {f"<div class='flash'>{html.escape(flash_message)}</div>" if flash_message else ""}
        <section class="card stack">
          <p class="eyebrow">Website Ops queue</p>
          <h1>Review <span style="color:var(--accent)">and approve</span>.</h1>
          <p class="lead">Showing: {html.escape(queue_title)} items. Approve a deterministic action when the requested change is exact. Leave it as manual review if the request is still ambiguous.</p>
        </section>
        <section class="card stack">
          {_feedback_cards(entries, with_actions=True)}
        </section>
      </main>
    """
    return _page_shell("Agent Website Ops Queue", body)


def render_feedback_detail_page(settings: Settings, feedback_id: str, *, flash_message: str = "") -> str:
    record = get_feedback_record(settings, feedback_id)
    if not record:
        return _page_shell("Not Found", f"{_nav('queue', website_ops_section='queue')}<main class='shell'><section class='card'><h1>Not found</h1><p class='lead'>The feedback record could not be located.</p></section></main>")
    is_auto_generated = bool(record.get("auto_generated"))
    confidence = str(record.get("confidence", "")).strip()
    suggested_action_type = str(record.get("suggested_action_type", "")).strip()
    is_auto_executable = _is_auto_executable_action(suggested_action_type)
    recommendation_cta = "Approve and Execute" if is_auto_executable else "Approve Recommendation"
    recommendation_note = (
        "This recommendation maps to a supported safe action. Approving it will execute immediately when auto-execution is enabled."
        if is_auto_executable
        else "This recommendation will move into the approved queue. Use the form below only if you want to override or add execution details."
    )
    body = f"""
      {_nav("queue", website_ops_section="queue")}
      <main class="shell">
        {f"<div class='flash'>{html.escape(flash_message)}</div>" if flash_message else ""}
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
            {f"<div class='summary-grid'>{_summary_chip('Section', record.get('section_name', 'General'), tone='neutral')}{_summary_chip('Confidence', confidence.title() if confidence else 'Medium', tone='neutral')}{_summary_chip('Suggested action', _humanize_label(suggested_action_type) or 'Manual review', tone='neutral')}{_summary_chip('Execution', 'Auto-executable' if is_auto_executable else 'Approval only', tone='neutral')}</div>" if is_auto_generated else ""}
            {f"<div class='button-row'><form class='inline' action='/admin/api/website-ops/feedback/{html.escape(str(record.get('feedback_id', '')), quote=True)}/review' method='post'><input type='hidden' name='status' value='approved'><button type='submit'>{recommendation_cta}</button></form><form class='inline' action='/admin/api/website-ops/feedback/{html.escape(str(record.get('feedback_id', '')), quote=True)}/review' method='post'><input type='hidden' name='status' value='rejected'><button class='ghost' type='submit'>Reject Recommendation</button></form><span class='muted'>{html.escape(recommendation_note)}</span></div>" if is_auto_generated else ""}
            <form action="/admin/api/website-ops/feedback/{html.escape(str(record.get('feedback_id', '')), quote=True)}/review" method="post" class="form-grid">
              <div><label>Status</label><select name="status">
                <option value="new" {'selected' if record.get('status') == 'new' else ''}>New</option>
                <option value="approved" {'selected' if record.get('status') == 'approved' else ''}>Approved</option>
                <option value="in-progress" {'selected' if record.get('status') == 'in-progress' else ''}>In Progress</option>
                <option value="done" {'selected' if record.get('status') == 'done' else ''}>Done</option>
                <option value="rejected" {'selected' if record.get('status') == 'rejected' else ''}>Rejected</option>
                <option value="error" {'selected' if record.get('status') == 'error' else ''}>Error</option>
              </select></div>
              <div><label>Reviewer</label><input type="text" name="reviewer_name" value="{html.escape(str(record.get('reviewer_name', '')), quote=True)}"></div>
              <div><label>Action type</label><select name="action_type"><option value="">Manual only</option><option value="replace_primary_heading" {'selected' if record.get('action_type') == 'replace_primary_heading' else ''}>Replace Primary Heading</option></select></div>
              <div><label>Target post ID</label><input type="text" name="target_post_id" value="{html.escape(str(record.get('target_post_id', '')), quote=True)}" placeholder="Optional WordPress page ID"></div>
              <div class="span-2"><label>Action value</label><input type="text" name="action_value" value="{html.escape(str(record.get('action_value', '')), quote=True)}" placeholder="Exact new H1 text"></div>
              <div class="span-2"><label>Review notes</label><textarea name="review_notes">{html.escape(str(record.get('review_notes', '')))}</textarea></div>
              <div class="span-2"><button type="submit">Submit Review</button></div>
            </form>
          </section>
        </section>
      </main>
    """
    return _page_shell("Agent Website Ops Feedback", body)


def render_reports_page(settings: Settings) -> str:
    reports = _report_entries(settings)
    body = f"""
      {_nav("reports", website_ops_section="reports")}
      <main class="shell">
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
    return _page_shell("Agent Website Ops Reports", body)


def render_report_page(settings: Settings, mode: str, slug: str) -> str:
    entry = get_report_entry(settings, mode, slug)
    if not entry:
        return _page_shell("Not Found", f"{_nav('reports', website_ops_section='reports')}<main class='shell'><section class='card'><h1>Not found</h1><p class='lead'>The requested report was not found.</p></section></main>")
    html_path = entry["html_path"]
    if html_path.exists():
        return _inject_admin_nav_into_report_html(html_path.read_text(), active="reports")
    markdown_path = entry["path"]
    return _page_shell(
        entry["title"],
        f"{_nav('reports', website_ops_section='reports')}<main class='shell'><section class='card stack'><p class='eyebrow'>{html.escape(mode.title())}</p><h1>{html.escape(entry['title'])}</h1><pre>{html.escape(markdown_path.read_text())}</pre></section></main>",
    )
