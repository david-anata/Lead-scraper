"""Read-only fulfillment CS dashboard pages for the admin app."""

from __future__ import annotations

import html
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sales_support_agent.services.admin_nav import render_agent_nav, render_agent_nav_styles


DEFAULT_TITLE = "Fulfillment CS Review"
ACTION_STATE_ORDER = ("clarifying", "investigating", "ready_to_answer", "escalated", "resolved")
LIFECYCLE_STATE_ORDER = ("new", "investigating", "responded", "escalated", "waiting_human", "resolved")


@dataclass(frozen=True)
class FulfillmentReportEntry:
    slug: str
    title: str
    generated_at: str
    status: str
    candidate_count: int
    action_counts: dict[str, int]
    lifecycle_counts: dict[str, int]
    artifact_formats: tuple[str, ...]
    excerpt: str
    path: Path


def _load_report(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _summary(payload: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    summary = payload.get("summary", {})
    if isinstance(summary, dict):
        return summary
    return {}


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _normalized_counts(raw: Any, order: tuple[str, ...]) -> dict[str, int]:
    if not isinstance(raw, dict):
        raw = {}
    normalized = {str(key): _int(value, 0) for key, value in raw.items() if str(key).strip()}
    for key in order:
        normalized.setdefault(key, 0)
    return normalized


def _artifact_formats(reports_dir: Path, slug: str) -> tuple[str, ...]:
    formats = []
    for ext in ("json", "html", "md"):
        if (reports_dir / f"{slug}.{ext}").exists():
            formats.append(ext)
    return tuple(formats)


def _report_excerpt(candidate_count: int, action_counts: dict[str, int], lifecycle_counts: dict[str, int]) -> str:
    action_parts = [f"{key}: {action_counts[key]}" for key in ACTION_STATE_ORDER if action_counts.get(key)]
    lifecycle_parts = [f"{key}: {lifecycle_counts[key]}" for key in LIFECYCLE_STATE_ORDER if lifecycle_counts.get(key)]
    summary_parts = []
    if action_parts:
        summary_parts.append("Actions: " + ", ".join(action_parts))
    if lifecycle_parts:
        summary_parts.append("States: " + ", ".join(lifecycle_parts))
    if not summary_parts:
        summary_parts.append("No counts recorded.")
    return f"{candidate_count} candidate thread(s). {' '.join(summary_parts)}"


def _entry_from_payload(path: Path, payload: dict[str, Any]) -> FulfillmentReportEntry:
    summary = _summary(payload)
    slug = str(payload.get("report_slug", "")).strip() or path.stem
    action_counts = _normalized_counts(summary.get("action_counts", payload.get("action_counts", {})), ACTION_STATE_ORDER)
    lifecycle_counts = _normalized_counts(summary.get("lifecycle_counts", payload.get("lifecycle_counts", {})), LIFECYCLE_STATE_ORDER)
    candidate_count = _int(summary.get("candidate_count", payload.get("candidate_count", 0)), 0)
    return FulfillmentReportEntry(
        slug=slug,
        title=str(payload.get("title", DEFAULT_TITLE)).strip() or DEFAULT_TITLE,
        generated_at=str(payload.get("generated_at", "")).strip(),
        status=str(payload.get("status", "unknown")).strip() or "unknown",
        candidate_count=candidate_count,
        action_counts=action_counts,
        lifecycle_counts=lifecycle_counts,
        artifact_formats=_artifact_formats(path.parent, slug),
        excerpt=_report_excerpt(candidate_count, action_counts, lifecycle_counts),
        path=path,
    )


def _entries_from_index(reports_dir: Path) -> list[FulfillmentReportEntry]:
    index_payload = _load_report(reports_dir / "index.json")
    if not index_payload:
        return []
    reports = index_payload.get("reports", [])
    if not isinstance(reports, list):
        return []
    entries: list[FulfillmentReportEntry] = []
    for item in reports:
        if not isinstance(item, dict):
            continue
        slug = str(item.get("report_slug", "")).strip() or str(item.get("report_id", "")).strip()
        if slug and not slug.startswith("support-review-"):
            slug = f"support-review-{slug}"
        if not slug:
            continue
        payload = _load_report(reports_dir / f"{slug}.json")
        if payload:
            entries.append(_entry_from_payload(reports_dir / f"{slug}.json", payload))
            continue
        action_counts = _normalized_counts(item.get("action_counts", {}), ACTION_STATE_ORDER)
        lifecycle_counts = _normalized_counts(item.get("lifecycle_counts", {}), LIFECYCLE_STATE_ORDER)
        candidate_count = _int(item.get("candidate_count", 0), 0)
        entries.append(
            FulfillmentReportEntry(
                slug=slug,
                title=str(item.get("title", DEFAULT_TITLE)).strip() or DEFAULT_TITLE,
                generated_at=str(item.get("generated_at", "")).strip(),
                status="unknown",
                candidate_count=candidate_count,
                action_counts=action_counts,
                lifecycle_counts=lifecycle_counts,
                artifact_formats=tuple(str(value) for value in item.get("artifact_formats", ()) if str(value).strip()),
                excerpt=_report_excerpt(candidate_count, action_counts, lifecycle_counts),
                path=reports_dir / f"{slug}.json",
            )
        )
    return entries


def fulfillment_report_entries(reports_dir: Path) -> list[FulfillmentReportEntry]:
    if not reports_dir.exists():
        return []
    indexed_entries = _entries_from_index(reports_dir)
    if indexed_entries:
        return indexed_entries
    entries: list[FulfillmentReportEntry] = []
    for path in sorted(reports_dir.glob("support-review-*.json"), key=lambda item: item.stat().st_mtime, reverse=True):
        payload = _load_report(path)
        if not payload:
            continue
        entries.append(_entry_from_payload(path, payload))
    return entries


def load_fulfillment_report_by_slug(reports_dir: Path, slug: str) -> dict[str, Any] | None:
    slug = str(slug).strip().split("/", 1)[0]
    if not slug:
        return None
    candidate = reports_dir / f"{slug}.json"
    return _load_report(candidate) if candidate.exists() and candidate.is_file() else None


def load_fulfillment_report_artifact(reports_dir: Path, slug: str, extension: str) -> tuple[str, str] | None:
    slug = str(slug).strip().split("/", 1)[0]
    extension = str(extension).strip().lstrip(".").lower()
    if not slug or extension not in {"json", "html", "md"}:
        return None
    candidate = reports_dir / f"{slug}.{extension}"
    if not candidate.exists() or not candidate.is_file():
        return None
    content_type = {
        "json": "application/json",
        "html": "text/html; charset=utf-8",
        "md": "text/markdown; charset=utf-8",
    }[extension]
    try:
        return candidate.read_text(), content_type
    except OSError:
        return None


def load_latest_fulfillment_report(reports_dir: Path) -> dict[str, Any] | None:
    latest_path = reports_dir / "latest.json"
    if latest_path.exists() and latest_path.is_file():
        return _load_report(latest_path)
    entries = fulfillment_report_entries(reports_dir)
    return _load_report(entries[0].path) if entries else None


def latest_fulfillment_report_entry(reports_dir: Path) -> FulfillmentReportEntry | None:
    entries = fulfillment_report_entries(reports_dir)
    return entries[0] if entries else None


def _metric(title: str, value: str, note: str) -> str:
    return (
        '<section class="metric">'
        f"<span>{html.escape(title)}</span>"
        f"<strong>{html.escape(value)}</strong>"
        f"<small>{html.escape(note)}</small>"
        "</section>"
    )


def _summary_rows(items: list[dict[str, Any]], *, label_key: str, empty_text: str) -> str:
    if not items:
        return f'<p class="empty">{html.escape(empty_text)}</p>'
    rendered = []
    for item in items:
        label = str(item.get(label_key, "")).strip() or "Unknown"
        count = _int(item.get("count", 0), 0)
        extra = ""
        if label_key == "account_name":
            account_id = str(item.get("account_id", "")).strip()
            if account_id:
                extra = f" <small>({html.escape(account_id)})</small>"
        rendered.append(f'<div class="summary-row"><span>{html.escape(label)}{extra}</span><strong>{count}</strong></div>')
    return "".join(rendered)


def _count_rows(counts: dict[str, int], order: tuple[str, ...], *, empty_text: str) -> str:
    rows = []
    for key in order:
        value = _int(counts.get(key, 0), 0)
        if value:
            rows.append(f'<div class="summary-row"><span>{html.escape(key.replace("_", " ").title())}</span><strong>{value}</strong></div>')
    if not rows:
        return f'<p class="empty">{html.escape(empty_text)}</p>'
    return "".join(rows)


def _warning_block(warnings: list[str]) -> str:
    if not warnings:
        return ""
    items = "".join(f"<li>{html.escape(item)}</li>" for item in warnings)
    return f'<section class="warning-panel"><strong>Attention needed</strong><ul>{items}</ul></section>'


def _candidate_cards(candidates: list[dict[str, Any]]) -> str:
    if not candidates:
        return '<p class="empty">No candidate support threads are available yet.</p>'
    cards: list[str] = []
    for candidate in candidates:
        draft_reply = str(candidate.get("draft_reply", "")).strip()
        evidence_summary = str(candidate.get("evidence_summary", "")).strip()
        escalation_reason = str(candidate.get("escalation_reason", "") or "").strip()
        cards.append(
            f"""
            <article class="candidate-card">
              <div class="candidate-top">
                <span class="candidate-brand">{html.escape(str(candidate.get('brand', candidate.get('brand_name', candidate.get('channel_name', 'Unknown')))))}</span>
                <span class="candidate-action">{html.escape(str(candidate.get('ui_recommendation', 'investigating')).replace('_', ' '))}</span>
              </div>
              <p class="candidate-channel">{html.escape(str(candidate.get('channel_name', candidate.get('channel', ''))))}</p>
              <h3>{html.escape(str(candidate.get('question_summary', 'No summary available.')))}</h3>
              <div class="meta-pills">
                <span class="pill">Case: {html.escape(str(candidate.get('case_id', 'n/a')))}</span>
                <span class="pill">Lifecycle: {html.escape(str(candidate.get('lifecycle_state', 'new')).replace('_', ' '))}</span>
              </div>
              <p><strong>Draft reply:</strong> {html.escape(draft_reply or 'No draft reply recorded.')}</p>
              <p class="candidate-meta">{html.escape(evidence_summary or 'No evidence summary recorded.')}</p>
              {f'<p class="candidate-meta"><strong>Escalation reason:</strong> {html.escape(escalation_reason)}</p>' if escalation_reason else ''}
              <p><a href="{html.escape(str(candidate.get('customer_thread_link', candidate.get('permalink', '#'))), quote=True)}" target="_blank" rel="noreferrer">Open Slack thread</a></p>
            </article>
            """
        )
    return "".join(cards)


def _page_shell(*, title: str, eyebrow: str, heading: str, intro: str, body: str, active_subnav: str) -> str:
    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{html.escape(title)}</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@700;800&family=Inter:wght@400;500;600&display=swap" rel="stylesheet">
    <style>
      :root {{
        --dark-blue: #2B3644;
        --light-blue: #85BBDA;
        --light-brown: #F9F7F3;
        --white: #FFFFFF;
        --shadow: rgba(43, 54, 68, 0.10);
        --warning-bg: #fff4d9;
        --warning-border: #d2a94b;
      }}
      * {{ box-sizing: border-box; }}
      body {{
        margin: 0;
        background: var(--light-brown);
        color: var(--dark-blue);
        font-family: "Inter", "Segoe UI", sans-serif;
      }}
      a {{ color: var(--dark-blue); }}
      {render_agent_nav_styles()}
      .shell {{
        max-width: 1180px;
        margin: 0 auto;
        padding: 28px 18px 64px;
      }}
      .workspace {{
        background: var(--white);
        border: 1px solid rgba(43, 54, 68, 0.10);
        border-radius: 26px;
        box-shadow: 0 18px 40px var(--shadow);
        padding: 24px;
      }}
      .page-header {{
        display: grid;
        grid-template-columns: minmax(0, 1.15fr) minmax(300px, 0.85fr);
        gap: 22px;
        align-items: end;
        padding-bottom: 20px;
        border-bottom: 1px solid rgba(43, 54, 68, 0.10);
        margin-bottom: 22px;
      }}
      .eyebrow {{
        display: inline-block;
        padding: 11px 16px;
        border-radius: 6px;
        background: var(--dark-blue);
        color: var(--white);
        font-family: "Montserrat", sans-serif;
        font-weight: 700;
        font-size: 15px;
        line-height: 1;
        letter-spacing: 0.04em;
        text-transform: uppercase;
        margin-bottom: 16px;
      }}
      .page-title {{
        margin: 0;
        font-family: "Montserrat", sans-serif;
        font-weight: 800;
        font-size: 52px;
        line-height: 0.96;
        letter-spacing: -0.035em;
        color: var(--dark-blue);
      }}
      .highlight {{
        color: var(--light-blue);
      }}
      .page-copy {{
        font-size: 17px;
        line-height: 1.5;
      }}
      .warning-panel {{
        margin-bottom: 18px;
        border: 1px solid var(--warning-border);
        background: var(--warning-bg);
        border-radius: 16px;
        padding: 16px 18px;
      }}
      .warning-panel strong {{
        display: block;
        font-family: "Montserrat", sans-serif;
        margin-bottom: 8px;
      }}
      .warning-panel ul {{
        margin: 0;
        padding-left: 20px;
      }}
      .metrics {{
        display: grid;
        grid-template-columns: repeat(5, minmax(0, 1fr));
        gap: 12px;
        margin-bottom: 22px;
      }}
      .metric {{
        background: var(--white);
        border: 2px solid rgba(43, 54, 68, 0.10);
        border-radius: 18px;
        padding: 18px;
        display: grid;
        gap: 8px;
      }}
      .metric span {{
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        color: rgba(43, 54, 68, 0.65);
        font-family: "Montserrat", sans-serif;
        font-weight: 700;
      }}
      .metric strong {{
        font-family: "Montserrat", sans-serif;
        font-size: 30px;
      }}
      .metric small {{
        font-size: 13px;
        line-height: 1.5;
        color: rgba(43, 54, 68, 0.72);
      }}
      .layout-two {{
        display: grid;
        grid-template-columns: minmax(0, 1.2fr) minmax(300px, 0.8fr);
        gap: 18px;
        margin-bottom: 22px;
      }}
      .panel {{
        background: var(--white);
        border: 2px solid rgba(43, 54, 68, 0.10);
        border-radius: 18px;
        padding: 20px 22px;
      }}
      .panel h2, .panel h3 {{
        margin: 0 0 12px;
        font-family: "Montserrat", sans-serif;
      }}
      .panel p {{
        margin: 0 0 14px;
        line-height: 1.5;
      }}
      .report-list, .candidate-list {{
        display: grid;
        gap: 14px;
      }}
      .report-card, .candidate-card {{
        border: 2px solid rgba(43, 54, 68, 0.10);
        border-radius: 16px;
        padding: 18px;
        background: rgba(249, 247, 243, 0.55);
      }}
      .report-card h3, .candidate-card h3 {{
        margin: 0 0 8px;
        font-family: "Montserrat", sans-serif;
      }}
      .report-meta, .candidate-meta, .candidate-channel {{
        color: rgba(43, 54, 68, 0.72);
        font-size: 14px;
      }}
      .candidate-top {{
        display: flex;
        justify-content: space-between;
        gap: 12px;
        align-items: center;
        margin-bottom: 10px;
      }}
      .candidate-brand, .candidate-action {{
        font-family: "Montserrat", sans-serif;
        font-weight: 700;
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.05em;
      }}
      .candidate-action {{
        color: var(--light-blue);
      }}
      .meta-pills {{
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
        margin: 10px 0 14px;
      }}
      .pill {{
        display: inline-flex;
        align-items: center;
        padding: 6px 10px;
        border-radius: 999px;
        background: rgba(43, 54, 68, 0.07);
        font-size: 12px;
        font-weight: 600;
      }}
      .summary-list {{
        display: grid;
        gap: 10px;
      }}
      .summary-row {{
        display: flex;
        justify-content: space-between;
        gap: 12px;
        padding: 10px 0;
        border-bottom: 1px solid rgba(43, 54, 68, 0.08);
      }}
      .summary-row:last-child {{
        border-bottom: none;
      }}
      .summary-row strong {{
        font-family: "Montserrat", sans-serif;
      }}
      .report-format-list {{
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
      }}
      .format-badge {{
        display: inline-flex;
        align-items: center;
        padding: 6px 10px;
        border-radius: 999px;
        background: rgba(43, 54, 68, 0.07);
        font-size: 12px;
        font-weight: 700;
        text-transform: uppercase;
      }}
      .empty {{
        color: rgba(43, 54, 68, 0.72);
      }}
      @media (max-width: 980px) {{
        .page-header, .layout-two, .metrics {{
          grid-template-columns: 1fr;
        }}
      }}
    </style>
  </head>
  <body>
    {render_agent_nav("fulfillment", website_ops_section=active_subnav)}
    <div class="shell">
      <div class="workspace">
        <section class="page-header">
          <div>
            <div class="eyebrow">{html.escape(eyebrow)}</div>
            <h1 class="page-title">{heading}</h1>
          </div>
          <div class="page-copy">{html.escape(intro)}</div>
        </section>
        {body}
      </div>
    </div>
  </body>
</html>"""


def render_fulfillment_dashboard_page(report: dict[str, Any] | None, entries: list[FulfillmentReportEntry]) -> str:
    summary = _summary(report)
    action_counts = _normalized_counts(summary.get("action_counts", {}), ACTION_STATE_ORDER)
    lifecycle_counts = _normalized_counts(summary.get("lifecycle_counts", {}), LIFECYCLE_STATE_ORDER)
    warnings = report.get("warnings", []) if isinstance(report, dict) and isinstance(report.get("warnings", []), list) else []
    recent_candidates = report.get("recent_candidates", []) if isinstance(report, dict) and isinstance(report.get("recent_candidates", []), list) else []
    brand_counts = summary.get("brand_counts", []) if isinstance(summary.get("brand_counts", []), list) else []
    account_counts = summary.get("account_counts", []) if isinstance(summary.get("account_counts", []), list) else []
    candidate_count = _int(summary.get("candidate_count", report.get("candidate_count", 0) if isinstance(report, dict) else 0), 0)
    escalation_count = _int(summary.get("escalation_count", 0), 0)
    unresolved_count = _int(summary.get("unresolved_count", 0), 0)
    status = str(report.get("status", "not-ready") if isinstance(report, dict) else "not-ready")
    body = (
        _warning_block(warnings)
        + '<section class="metrics">'
        + _metric("Candidate threads", str(candidate_count), "Open candidate threads in the latest review snapshot.")
        + _metric("Unresolved", str(unresolved_count), "Lifecycle state is not resolved.")
        + _metric("Escalated", str(escalation_count), "Cases requiring explicit human follow-up.")
        + _metric("Ready to answer", str(action_counts.get("ready_to_answer", 0)), "Cases with enough evidence to answer directly.")
        + _metric("Investigating", str(action_counts.get("investigating", 0)), "Cases still gathering evidence or waiting on confirmation.")
        + "</section>"
        + '<section class="layout-two">'
        + '<section class="panel"><h2>Latest review</h2>'
        + (
            f"<p>{html.escape(str(report.get('title', DEFAULT_TITLE)))}</p>"
            f"<p class=\"report-meta\">Generated: {html.escape(str(report.get('generated_at', '')))}</p>"
            f"<p class=\"report-meta\">Status: {html.escape(status)}</p>"
            f"<p><a href=\"/admin/fulfillment-cs/reports/latest\">Open latest report</a></p>"
            if isinstance(report, dict)
            else '<p>No fulfillment review report has been generated yet.</p>'
        )
        + "</section>"
        + '<aside class="panel"><h3>Report library</h3>'
        + (f"<p>{len(entries)} timestamped report(s) available.</p>" if entries else "<p>No report files found yet.</p>")
        + '<p><a href="/admin/fulfillment-cs/reports/">Browse all reports</a></p>'
        + "</aside></section>"
        + '<section class="layout-two">'
        + f'<section class="panel"><h2>Action recommendations</h2><div class="summary-list">{_count_rows(action_counts, ACTION_STATE_ORDER, empty_text="No action recommendations recorded yet.")}</div></section>'
        + f'<section class="panel"><h2>Lifecycle states</h2><div class="summary-list">{_count_rows(lifecycle_counts, LIFECYCLE_STATE_ORDER, empty_text="No lifecycle states recorded yet.")}</div></section>'
        + "</section>"
        + '<section class="layout-two">'
        + f'<section class="panel"><h2>Brands</h2><div class="summary-list">{_summary_rows(brand_counts[:6], label_key="brand", empty_text="No brand counts recorded yet.")}</div></section>'
        + f'<section class="panel"><h2>Accounts</h2><div class="summary-list">{_summary_rows(account_counts[:6], label_key="account_name", empty_text="No account counts recorded yet.")}</div></section>'
        + "</section>"
        + '<section class="panel"><h2>Candidate preview</h2><div class="candidate-list">'
        + _candidate_cards(recent_candidates[:6] if isinstance(recent_candidates, list) else [])
        + "</div></section>"
    )
    return _page_shell(
        title="agent | Fulfillment CS",
        eyebrow="Fulfillment CS",
        heading='Fulfillment <span class="highlight">CS</span>.',
        intro="Artifact-driven visibility into fulfillment support candidates, state, and escalation needs.",
        body=body,
        active_subnav="fulfillment_dashboard",
    )


def render_fulfillment_reports_page(entries: list[FulfillmentReportEntry]) -> str:
    cards = "".join(
        f"""
        <article class="report-card">
          <p class="report-meta">{html.escape(entry.generated_at)}</p>
          <h3><a href="/admin/fulfillment-cs/reports/{html.escape(entry.slug)}">{html.escape(entry.title)}</a></h3>
          <p>{html.escape(entry.excerpt)}</p>
          <div class="report-format-list">{''.join(f'<span class="format-badge">{html.escape(fmt)}</span>' for fmt in entry.artifact_formats)}</div>
        </article>
        """
        for entry in entries
    ) or '<p class="empty">No support-review reports found yet.</p>'
    body = f'<section class="panel"><h2>Report library</h2><div class="report-list">{cards}</div></section>'
    return _page_shell(
        title="agent | Fulfillment CS Reports",
        eyebrow="Fulfillment CS",
        heading='Support <span class="highlight">Reports</span>.',
        intro="Timestamped, read-only fulfillment support artifacts rendered from prepared report files.",
        body=body,
        active_subnav="fulfillment_reports",
    )


def render_fulfillment_report_detail_page(report: dict[str, Any]) -> str:
    summary = _summary(report)
    action_counts = _normalized_counts(summary.get("action_counts", report.get("action_counts", {})), ACTION_STATE_ORDER)
    lifecycle_counts = _normalized_counts(summary.get("lifecycle_counts", report.get("lifecycle_counts", {})), LIFECYCLE_STATE_ORDER)
    warnings = report.get("warnings", []) if isinstance(report.get("warnings", []), list) else []
    escalations = report.get("escalations", []) if isinstance(report.get("escalations", []), list) else []
    body = (
        _warning_block(warnings)
        + '<section class="metrics">'
        + _metric("Candidate threads", str(_int(summary.get("candidate_count", report.get("candidate_count", 0)), 0)), "Threads included in this report.")
        + _metric("Unresolved", str(_int(summary.get("unresolved_count", 0), 0)), "Cases still open after this review pass.")
        + _metric("Escalated", str(_int(summary.get("escalation_count", 0), 0)), "Cases needing human escalation or review.")
        + _metric("Clarifying", str(action_counts.get("clarifying", 0)), "Cases missing the identifiers needed to answer safely.")
        + _metric("Ready to answer", str(action_counts.get("ready_to_answer", 0)), "Cases with enough verified evidence to respond.")
        + "</section>"
        + '<section class="layout-two">'
        + f'<section class="panel"><h2>Action recommendations</h2><div class="summary-list">{_count_rows(action_counts, ACTION_STATE_ORDER, empty_text="No action recommendation counts recorded.")}</div></section>'
        + f'<section class="panel"><h2>Lifecycle states</h2><div class="summary-list">{_count_rows(lifecycle_counts, LIFECYCLE_STATE_ORDER, empty_text="No lifecycle counts recorded.")}</div></section>'
        + "</section>"
        + '<section class="layout-two">'
        + f'<section class="panel"><h2>Brands</h2><div class="summary-list">{_summary_rows(summary.get("brand_counts", []) if isinstance(summary.get("brand_counts", []), list) else [], label_key="brand", empty_text="No brand counts recorded.")}</div></section>'
        + f'<section class="panel"><h2>Accounts</h2><div class="summary-list">{_summary_rows(summary.get("account_counts", []) if isinstance(summary.get("account_counts", []), list) else [], label_key="account_name", empty_text="No account counts recorded.")}</div></section>'
        + "</section>"
        + '<section class="panel"><h2>Escalations</h2><div class="summary-list">'
        + (
            "".join(
                f'<div class="summary-row"><span>{html.escape(str(item.get("case_id", "")))}</span><strong>{html.escape(str(item.get("reason", "unknown")))}</strong></div>'
                for item in escalations
            )
            if escalations
            else '<p class="empty">No escalations recorded in this report.</p>'
        )
        + "</div></section>"
        + '<section class="panel"><h2>Candidate threads</h2><div class="candidate-list">'
        + _candidate_cards(report.get("candidates", []) if isinstance(report.get("candidates", []), list) else [])
        + "</div></section>"
    )
    return _page_shell(
        title=f"agent | {str(report.get('title', DEFAULT_TITLE))}",
        eyebrow="Fulfillment CS",
        heading='Report <span class="highlight">Detail</span>.',
        intro=f"Generated at {str(report.get('generated_at', 'unknown'))}.",
        body=body,
        active_subnav="fulfillment_reports",
    )


def render_fulfillment_not_found_page(message: str) -> str:
    body = f'<section class="panel"><h2>Not found</h2><p>{html.escape(message)}</p><p><a href="/admin/fulfillment-cs/reports/">Browse reports</a></p></section>'
    return _page_shell(
        title="agent | Fulfillment CS",
        eyebrow="Fulfillment CS",
        heading='Fulfillment <span class="highlight">CS</span>.',
        intro="The requested fulfillment support view could not be found.",
        body=body,
        active_subnav="fulfillment_dashboard",
    )
