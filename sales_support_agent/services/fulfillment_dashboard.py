"""Read-only fulfillment CS dashboard pages for the admin app."""

from __future__ import annotations

import html
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sales_support_agent.services.admin_nav import render_agent_nav, render_agent_nav_styles


@dataclass(frozen=True)
class FulfillmentReportEntry:
    slug: str
    title: str
    generated_at: str
    status: str
    candidate_count: int
    action_counts: dict[str, int]
    excerpt: str
    path: Path


def _load_report(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _format_action_counts(action_counts: dict[str, Any]) -> str:
    if not action_counts:
        return "No action counts recorded."
    return ", ".join(f"{key}: {action_counts[key]}" for key in sorted(action_counts))


def fulfillment_report_entries(reports_dir: Path) -> list[FulfillmentReportEntry]:
    if not reports_dir.exists():
        return []
    entries: list[FulfillmentReportEntry] = []
    for path in sorted(reports_dir.glob("support-review-*.json"), key=lambda item: item.stat().st_mtime, reverse=True):
        payload = _load_report(path)
        if not payload:
            continue
        raw_counts = payload.get("action_counts", {})
        action_counts = {
            str(key): int(value or 0)
            for key, value in raw_counts.items()
            if str(key).strip()
        } if isinstance(raw_counts, dict) else {}
        candidate_count = int(payload.get("candidate_count", 0) or 0)
        entries.append(
            FulfillmentReportEntry(
                slug=path.stem,
                title=str(payload.get("title", "Fulfillment Support Review")).strip() or "Fulfillment Support Review",
                generated_at=str(payload.get("generated_at", "")).strip(),
                status=str(payload.get("status", "unknown")).strip() or "unknown",
                candidate_count=candidate_count,
                action_counts=action_counts,
                excerpt=f"{candidate_count} candidate thread(s). {_format_action_counts(action_counts)}",
                path=path,
            )
        )
    return entries


def load_fulfillment_report_by_slug(reports_dir: Path, slug: str) -> dict[str, Any] | None:
    slug = str(slug).strip().split("/", 1)[0]
    if not slug:
        return None
    candidate = reports_dir / f"{slug}.json"
    return _load_report(candidate) if candidate.exists() and candidate.is_file() else None


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


def _candidate_cards(candidates: list[dict[str, Any]]) -> str:
    if not candidates:
        return '<p class="empty">No candidate support threads are available yet.</p>'
    cards: list[str] = []
    for candidate in candidates:
        action = candidate.get("recommended_action", {}) if isinstance(candidate.get("recommended_action", {}), dict) else {}
        identifiers = candidate.get("identifiers", {}) if isinstance(candidate.get("identifiers", {}), dict) else {}
        evidence = candidate.get("evidence", {}) if isinstance(candidate.get("evidence", {}), dict) else {}
        id_bits: list[str] = []
        for key in ("order_numbers", "tracking_numbers", "po_numbers"):
            values = identifiers.get(key, [])
            if isinstance(values, list) and values:
                id_bits.append(f"{key.replace('_', ' ').title()}: {', '.join(str(value) for value in values)}")
        evidence_bits: list[str] = []
        for key in ("labelogics", "shopify"):
            source = evidence.get(key, {})
            if isinstance(source, dict) and str(source.get("status", "")).strip():
                evidence_bits.append(f"{key.title()}: {source['status']}")
        cards.append(
            f"""
            <article class="candidate-card">
              <div class="candidate-top">
                <span class="candidate-brand">{html.escape(str(candidate.get('brand_name', candidate.get('channel', 'Unknown'))))}</span>
                <span class="candidate-action">{html.escape(str(action.get('reply_type', 'unknown')))}</span>
              </div>
              <p class="candidate-channel">{html.escape(str(candidate.get('channel', '')))}</p>
              <h3>{html.escape(str(candidate.get('question_summary', 'No summary available.')))}</h3>
              <p><strong>Draft reply:</strong> {html.escape(str(action.get('customer_reply', '')))}</p>
              <p class="candidate-meta">{html.escape(' | '.join(id_bits) if id_bits else 'No extracted identifiers yet.')}</p>
              <p class="candidate-meta">{html.escape(' | '.join(evidence_bits) if evidence_bits else 'No system evidence attached yet.')}</p>
              <p><a href="{html.escape(str(candidate.get('permalink', '#')), quote=True)}" target="_blank" rel="noreferrer">Open Slack thread</a></p>
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
      .metrics {{
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
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
    candidates = report.get("candidates", []) if isinstance(report, dict) else []
    action_counts = report.get("action_counts", {}) if isinstance(report, dict) and isinstance(report.get("action_counts", {}), dict) else {}
    body = (
        '<section class="metrics">'
        + _metric("Candidate threads", str(report.get("candidate_count", 0) if isinstance(report, dict) else 0), "Open customer-service candidates in the latest review.")
        + _metric("Status", str(report.get("status", "not-ready") if isinstance(report, dict) else "not-ready"), "Current review-pipeline status.")
        + _metric("Clarifying", str(int(action_counts.get("clarifying", 0) or 0)), "Cases waiting on missing identifiers or context.")
        + _metric("Investigating", str(int(action_counts.get("investigating", 0) or 0)), "Cases that still require system or ops follow-up.")
        + "</section>"
        + '<section class="layout-two">'
        + '<section class="panel"><h2>Latest review</h2>'
        + (
            f"<p>{html.escape(str(report.get('title', 'Fulfillment Support Review')))}</p>"
            f"<p class=\"report-meta\">Generated: {html.escape(str(report.get('generated_at', '')))}</p>"
            f"<p><a href=\"/admin/fulfillment-cs/reports/latest\">Open latest report</a></p>"
            if isinstance(report, dict)
            else '<p>No fulfillment review report has been generated yet.</p>'
        )
        + "</section>"
        + '<aside class="panel"><h3>Report library</h3>'
        + (f"<p>{len(entries)} timestamped report(s) available.</p>" if entries else "<p>No report files found yet.</p>")
        + '<p><a href="/admin/fulfillment-cs/reports/">Browse all reports</a></p>'
        + "</aside></section>"
        + '<section class="panel"><h2>Candidate preview</h2><div class="candidate-list">'
        + _candidate_cards(candidates[:6] if isinstance(candidates, list) else [])
        + "</div></section>"
    )
    return _page_shell(
        title="agent | Fulfillment CS",
        eyebrow="Fulfillment CS",
        heading='Fulfillment <span class="highlight">CS</span>.',
        intro="Slack-first support review for fulfillment questions, draft replies, and escalation candidates.",
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
        </article>
        """
        for entry in entries
    ) or '<p class="empty">No support-review reports found yet.</p>'
    body = f'<section class="panel"><h2>Report library</h2><div class="report-list">{cards}</div></section>'
    return _page_shell(
        title="agent | Fulfillment CS Reports",
        eyebrow="Fulfillment CS",
        heading='Support <span class="highlight">Reports</span>.',
        intro="Timestamped review artifacts generated from the fulfillment support pipeline.",
        body=body,
        active_subnav="fulfillment_reports",
    )


def render_fulfillment_report_detail_page(report: dict[str, Any]) -> str:
    action_counts = report.get("action_counts", {}) if isinstance(report.get("action_counts", {}), dict) else {}
    body = (
        '<section class="metrics">'
        + _metric("Candidate threads", str(int(report.get("candidate_count", 0) or 0)), "Threads included in this report.")
        + _metric("Status", str(report.get("status", "unknown")), "Report readiness for this snapshot.")
        + _metric("Clarifying", str(int(action_counts.get("clarifying", 0) or 0)), "Missing-info cases in this snapshot.")
        + _metric("Investigating", str(int(action_counts.get("investigating", 0) or 0)), "Escalation or follow-up cases in this snapshot.")
        + "</section>"
        + '<section class="panel"><h2>Candidate threads</h2><div class="candidate-list">'
        + _candidate_cards(report.get("candidates", []) if isinstance(report.get("candidates", []), list) else [])
        + "</div></section>"
    )
    return _page_shell(
        title=f"agent | {str(report.get('title', 'Fulfillment Support Review'))}",
        eyebrow="Fulfillment CS",
        heading='Report <span class="highlight">Detail</span>.',
        intro=f"Generated at {str(report.get('generated_at', 'unknown'))}.",
        body=body,
        active_subnav="fulfillment_reports",
    )


def render_fulfillment_not_found_page(message: str) -> str:
    body = f'<section class="panel"><h2>Not found</h2><p>{html.escape(message)}</p></section>'
    return _page_shell(
        title="agent | Fulfillment CS",
        eyebrow="Fulfillment CS",
        heading='Fulfillment <span class="highlight">CS</span>.',
        intro="The requested fulfillment support view could not be found.",
        body=body,
        active_subnav="fulfillment_dashboard",
    )
