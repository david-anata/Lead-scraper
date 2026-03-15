"""Admin dashboard data and HTML rendering."""

from __future__ import annotations

import html
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

from sqlalchemy import Select, func, select
from sqlalchemy.orm import Session

from sales_support_agent.config import Settings
from sales_support_agent.models.entities import AutomationRun, LeadMirror, MailboxSignal
from sales_support_agent.services.notification_policy import STALE_URGENCY_LABELS, STALE_URGENCY_ORDER
from sales_support_agent.services.reminders import ReminderService
from sales_support_agent.services.reply_templates import format_date_label


@dataclass(frozen=True)
class DashboardActionItem:
    owner_name: str
    urgency: str
    title: str
    subtitle: str
    action_summary: str
    suggested_reply: str
    source: str
    link_url: str
    date_label: str
    sort_timestamp: float


@dataclass(frozen=True)
class DashboardOwnerQueue:
    owner_name: str
    total_items: int
    overdue_count: int
    immediate_count: int
    follow_up_count: int
    items: list[DashboardActionItem]


@dataclass(frozen=True)
class DashboardData:
    as_of_date: date
    total_active_leads: int
    stale_counts: dict[str, int]
    mailbox_findings: int
    owner_queues: list[DashboardOwnerQueue]
    latest_sync_at: datetime | None
    latest_run_summary: dict
    lead_builder_ready: bool
    lead_builder_missing: list[str]


def dashboard_data_to_dict(data: DashboardData) -> dict[str, object]:
    return {
        "as_of_date": data.as_of_date.isoformat(),
        "total_active_leads": data.total_active_leads,
        "stale_counts": data.stale_counts,
        "mailbox_findings": data.mailbox_findings,
        "owner_queues": [
            {
                "owner_name": queue.owner_name,
                "total_items": queue.total_items,
                "overdue_count": queue.overdue_count,
                "immediate_count": queue.immediate_count,
                "follow_up_count": queue.follow_up_count,
                "items": [
                    {
                        "owner_name": item.owner_name,
                        "urgency": item.urgency,
                        "title": item.title,
                        "subtitle": item.subtitle,
                        "action_summary": item.action_summary,
                        "suggested_reply": item.suggested_reply,
                        "source": item.source,
                        "link_url": item.link_url,
                        "date_label": item.date_label,
                    }
                    for item in queue.items
                ],
            }
            for queue in data.owner_queues
        ],
        "latest_sync_at": data.latest_sync_at.isoformat() if data.latest_sync_at else "",
        "latest_run_summary": data.latest_run_summary,
        "lead_builder_ready": data.lead_builder_ready,
        "lead_builder_missing": data.lead_builder_missing,
    }


def dashboard_data_from_dict(payload: dict[str, object]) -> DashboardData:
    owner_queues = []
    for queue_payload in payload.get("owner_queues", []):
        queue_dict = dict(queue_payload)
        items = [
            DashboardActionItem(
                owner_name=str(item.get("owner_name", "")),
                urgency=str(item.get("urgency", "follow_up_due")),
                title=str(item.get("title", "")),
                subtitle=str(item.get("subtitle", "")),
                action_summary=str(item.get("action_summary", "")),
                suggested_reply=str(item.get("suggested_reply", "")),
                source=str(item.get("source", "")),
                link_url=str(item.get("link_url", "")),
                date_label=str(item.get("date_label", "")),
                sort_timestamp=0.0,
            )
            for item in queue_dict.get("items", [])
        ]
        owner_queues.append(
            DashboardOwnerQueue(
                owner_name=str(queue_dict.get("owner_name", "")),
                total_items=int(queue_dict.get("total_items", len(items)) or len(items)),
                overdue_count=int(queue_dict.get("overdue_count", 0) or 0),
                immediate_count=int(queue_dict.get("immediate_count", 0) or 0),
                follow_up_count=int(queue_dict.get("follow_up_count", 0) or 0),
                items=items,
            )
        )

    latest_sync_raw = str(payload.get("latest_sync_at", "") or "")
    latest_sync_at = datetime.fromisoformat(latest_sync_raw) if latest_sync_raw else None
    return DashboardData(
        as_of_date=date.fromisoformat(str(payload.get("as_of_date"))),
        total_active_leads=int(payload.get("total_active_leads", 0) or 0),
        stale_counts=dict(payload.get("stale_counts", {})),
        mailbox_findings=int(payload.get("mailbox_findings", 0) or 0),
        owner_queues=owner_queues,
        latest_sync_at=latest_sync_at,
        latest_run_summary=dict(payload.get("latest_run_summary", {})),
        lead_builder_ready=bool(payload.get("lead_builder_ready")),
        lead_builder_missing=[str(item) for item in payload.get("lead_builder_missing", [])],
    )


def build_dashboard_data(
    *,
    settings: Settings,
    session: Session,
    lead_builder_status: dict[str, object],
    as_of_date: date | None = None,
    max_items_per_owner: int = 8,
) -> DashboardData:
    effective_date = as_of_date or date.today()
    reminder_service = ReminderService(settings, session)

    leads_query: Select[tuple[LeadMirror]] = (
        select(LeadMirror)
        .where(LeadMirror.list_id == settings.clickup_list_id)
        .order_by(LeadMirror.updated_at.desc(), LeadMirror.last_sync_at.desc())
    )
    leads = list(session.execute(leads_query).scalars())
    latest_sync_at = max((lead.last_sync_at for lead in leads if lead.last_sync_at), default=None)

    stale_counts = {urgency: 0 for urgency in STALE_URGENCY_ORDER}
    owner_items: dict[str, list[DashboardActionItem]] = defaultdict(list)
    active_lead_count = 0

    for lead in leads:
        if not (lead.status or "").strip():
            continue
        evaluation = reminder_service.evaluate_lead(lead, as_of_date=effective_date, comments=[])
        if evaluation is None:
            continue
        active_lead_count += 1
        digest_item = reminder_service.build_digest_item(evaluation)
        stale_counts[digest_item.urgency] += 1
        owner_name = digest_item.owner_label or "Assigned AE"
        owner_items[owner_name].append(
            DashboardActionItem(
                owner_name=owner_name,
                urgency=digest_item.urgency,
                title=evaluation.lead.task_name,
                subtitle=evaluation.lead.status,
                action_summary=digest_item.action_summary,
                suggested_reply=digest_item.suggested_reply_draft,
                source="stale lead",
                link_url=evaluation.lead.task_url,
                date_label=format_date_label(evaluation.assessment.anchor_date),
                sort_timestamp=float(datetime.combine(evaluation.assessment.anchor_date, datetime.min.time()).timestamp()),
            )
        )

    mailbox_start = datetime.combine(effective_date - timedelta(days=7), datetime.min.time(), tzinfo=timezone.utc)
    mailbox_query = (
        select(MailboxSignal)
        .where(MailboxSignal.received_at >= mailbox_start)
        .order_by(MailboxSignal.received_at.desc())
        .limit(100)
    )
    mailbox_signals = list(session.execute(mailbox_query).scalars())
    for signal in mailbox_signals:
        owner_name = signal.owner_name or "Triage"
        owner_items[owner_name].append(
            DashboardActionItem(
                owner_name=owner_name,
                urgency=signal.urgency or "follow_up_due",
                title=signal.subject or signal.task_name or signal.sender_email or "Mailbox signal",
                subtitle=signal.task_name or signal.sender_email or signal.sender_domain or "Unmatched mailbox item",
                action_summary=signal.action_summary or "Review and decide the next action.",
                suggested_reply=signal.suggested_reply_draft or "Review the message and reply with the next step.",
                source="mailbox",
                link_url=signal.task_url,
                date_label=format_date_label(signal.received_at),
                sort_timestamp=signal.received_at.timestamp() if signal.received_at else 0.0,
            )
        )

    owner_queues: list[DashboardOwnerQueue] = []
    for owner_name, items in owner_items.items():
        ordered_items = sorted(
            items,
            key=lambda item: (
                STALE_URGENCY_ORDER.index(item.urgency) if item.urgency in STALE_URGENCY_ORDER else len(STALE_URGENCY_ORDER),
                -item.sort_timestamp,
                item.title.lower(),
            ),
        )
        owner_queues.append(
            DashboardOwnerQueue(
                owner_name=owner_name,
                total_items=len(ordered_items),
                overdue_count=sum(1 for item in ordered_items if item.urgency == "overdue"),
                immediate_count=sum(1 for item in ordered_items if item.urgency == "needs_immediate_review"),
                follow_up_count=sum(1 for item in ordered_items if item.urgency == "follow_up_due"),
                items=ordered_items[:max_items_per_owner],
            )
        )

    owner_queues.sort(
        key=lambda queue: (
            -queue.overdue_count,
            -queue.immediate_count,
            -queue.follow_up_count,
            queue.owner_name.lower(),
        )
    )

    latest_run = session.execute(
        select(AutomationRun)
        .where(AutomationRun.run_type == "stale_lead_scan")
        .order_by(AutomationRun.started_at.desc())
        .limit(1)
    ).scalar_one_or_none()

    latest_run_summary = latest_run.summary_json if latest_run else {}

    return DashboardData(
        as_of_date=effective_date,
        total_active_leads=active_lead_count,
        stale_counts=stale_counts,
        mailbox_findings=len(mailbox_signals),
        owner_queues=owner_queues,
        latest_sync_at=latest_sync_at,
        latest_run_summary=latest_run_summary,
        lead_builder_ready=bool(lead_builder_status.get("ready")),
        lead_builder_missing=[str(item) for item in lead_builder_status.get("missing", [])],
    )


def render_login_page(*, error_message: str = "") -> str:
    error_html = (
        f'<div class="notice error">{html.escape(error_message)}</div>'
        if error_message
        else ""
    )
    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Sales Admin Login</title>
    <style>
      :root {{
        --bg: #f4efe4;
        --panel: #fffaf0;
        --text: #1a1a1a;
        --muted: #6a6458;
        --border: #d7cfbf;
        --accent: #294d3c;
        --accent-2: #d68c45;
        --danger: #8a3f2f;
      }}
      * {{ box-sizing: border-box; }}
      body {{
        margin: 0;
        min-height: 100vh;
        display: grid;
        place-items: center;
        padding: 24px;
        background:
          radial-gradient(circle at top left, rgba(214,140,69,.22), transparent 30%),
          radial-gradient(circle at bottom right, rgba(41,77,60,.18), transparent 36%),
          var(--bg);
        color: var(--text);
        font-family: Georgia, "Times New Roman", serif;
      }}
      .card {{
        width: min(440px, 100%);
        background: var(--panel);
        border: 1px solid var(--border);
        border-radius: 20px;
        padding: 32px;
        box-shadow: 0 28px 80px rgba(26,26,26,.10);
      }}
      h1 {{
        margin: 0 0 10px;
        font-size: 40px;
        line-height: 1;
      }}
      p {{
        margin: 0 0 22px;
        color: var(--muted);
        line-height: 1.5;
      }}
      label {{
        display: block;
        font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
        font-size: 12px;
        letter-spacing: .08em;
        text-transform: uppercase;
        margin-bottom: 8px;
      }}
      input {{
        width: 100%;
        padding: 14px 16px;
        border-radius: 12px;
        border: 1px solid var(--border);
        background: white;
        font-size: 16px;
        margin-bottom: 16px;
      }}
      button {{
        width: 100%;
        border: 0;
        border-radius: 999px;
        padding: 14px 18px;
        background: linear-gradient(135deg, var(--accent), #1f362b);
        color: white;
        font-size: 16px;
        font-weight: 600;
        cursor: pointer;
      }}
      .notice {{
        border-radius: 12px;
        padding: 12px 14px;
        margin-bottom: 16px;
        font-size: 14px;
      }}
      .error {{
        background: rgba(138,63,47,.08);
        color: var(--danger);
        border: 1px solid rgba(138,63,47,.18);
      }}
    </style>
  </head>
  <body>
    <main class="card">
      <h1>Agent Admin</h1>
      <p>Password-protected access for the sales review dashboard and lead build actions.</p>
      {error_html}
      <form method="post" action="/admin/login">
        <label for="password">Password</label>
        <input id="password" name="password" type="password" autocomplete="current-password" required />
        <button type="submit">Enter dashboard</button>
      </form>
    </main>
  </body>
</html>"""


def render_dashboard_page(data: DashboardData) -> str:
    def _card(title: str, value: str, note: str) -> str:
        return (
            '<section class="metric">'
            f"<span>{html.escape(title)}</span>"
            f"<strong>{html.escape(value)}</strong>"
            f"<small>{html.escape(note)}</small>"
            "</section>"
        )

    metric_cards = "".join(
        [
            _card("Active tracked leads", str(data.total_active_leads), "Current ClickUp leads in active statuses"),
            _card("Overdue", str(data.stale_counts.get("overdue", 0)), "Highest priority follow-up risk"),
            _card(
                "Needs immediate review",
                str(data.stale_counts.get("needs_immediate_review", 0)),
                "Untouched or missing-next-step leads",
            ),
            _card("Follow-up due", str(data.stale_counts.get("follow_up_due", 0)), "Routine queue ready for review"),
            _card("Mailbox findings", str(data.mailbox_findings), "Signals captured in the last 7 days"),
        ]
    )

    owner_sections = []
    for queue in data.owner_queues:
        item_cards = []
        for item in queue.items:
            urgency_label = STALE_URGENCY_LABELS.get(item.urgency, item.urgency.replace("_", " ").title())
            link_html = (
                f'<a href="{html.escape(item.link_url)}" target="_blank" rel="noreferrer">Open task</a>'
                if item.link_url
                else ""
            )
            item_cards.append(
                f"""
                <article class="action-item urgency-{html.escape(item.urgency)}">
                  <div class="action-top">
                    <span class="badge">{html.escape(urgency_label)}</span>
                    <span class="source">{html.escape(item.source)}</span>
                    <span class="date">{html.escape(item.date_label)}</span>
                  </div>
                  <h4>{html.escape(item.title)}</h4>
                  <p class="subtitle">{html.escape(item.subtitle)}</p>
                  <p><strong>Action:</strong> {html.escape(item.action_summary)}</p>
                  <p><strong>Draft:</strong> {html.escape(item.suggested_reply)}</p>
                  {link_html}
                </article>
                """
            )
        owner_sections.append(
            f"""
            <section class="owner-card">
              <header>
                <div>
                  <h3>{html.escape(queue.owner_name)}</h3>
                  <p>{queue.total_items} items queued</p>
                </div>
                <div class="owner-stats">
                  <span>Overdue {queue.overdue_count}</span>
                  <span>Review {queue.immediate_count}</span>
                  <span>Due {queue.follow_up_count}</span>
                </div>
              </header>
              <div class="owner-items">
                {''.join(item_cards) or '<p class="empty">No action items yet.</p>'}
              </div>
            </section>
            """
        )

    latest_sync = format_date_label(data.latest_sync_at) if data.latest_sync_at else "not synced yet"
    latest_run_json = html.escape(json.dumps(data.latest_run_summary, indent=2, default=str)) if data.latest_run_summary else "No stale scan run recorded yet."
    lead_builder_notice = (
        '<div class="notice warning">Lead builder is missing env vars: '
        + html.escape(", ".join(data.lead_builder_missing))
        + "</div>"
        if not data.lead_builder_ready
        else '<div class="notice success">Lead builder is ready. Running it here will still add leads to Instantly and return the CSV immediately.</div>'
    )
    today_value = data.as_of_date.isoformat()

    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Agent Admin Dashboard</title>
    <style>
      :root {{
        --bg: #f3efe5;
        --panel: #fffaf1;
        --panel-strong: #fff;
        --text: #171717;
        --muted: #69614f;
        --border: #d7cfbf;
        --accent: #204b3a;
        --accent-soft: #dcb16c;
        --danger: #a64d31;
        --warn: #a57722;
        --shadow: rgba(16, 16, 16, 0.08);
      }}
      * {{ box-sizing: border-box; }}
      body {{
        margin: 0;
        color: var(--text);
        background:
          linear-gradient(180deg, rgba(255,255,255,.5), rgba(243,239,229,.9)),
          var(--bg);
        font-family: Georgia, "Times New Roman", serif;
      }}
      a {{ color: var(--accent); }}
      .shell {{ max-width: 1240px; margin: 0 auto; padding: 28px 20px 60px; }}
      .hero {{
        display: grid;
        gap: 20px;
        grid-template-columns: 1.6fr 1fr;
        margin-bottom: 26px;
      }}
      .panel {{
        background: var(--panel);
        border: 1px solid var(--border);
        border-radius: 24px;
        box-shadow: 0 18px 60px var(--shadow);
        padding: 24px;
      }}
      .hero h1 {{
        margin: 0 0 10px;
        font-size: 54px;
        line-height: .95;
      }}
      .hero p {{
        margin: 0;
        color: var(--muted);
        line-height: 1.55;
        max-width: 62ch;
      }}
      .topline {{
        font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
        text-transform: uppercase;
        font-size: 12px;
        letter-spacing: .08em;
        color: var(--muted);
        margin-bottom: 16px;
      }}
      .logout {{
        float: right;
        font-size: 14px;
      }}
      .metrics {{
        display: grid;
        grid-template-columns: repeat(5, minmax(0, 1fr));
        gap: 14px;
        margin-bottom: 26px;
      }}
      .metric {{
        background: var(--panel-strong);
        border: 1px solid var(--border);
        border-radius: 18px;
        padding: 18px;
      }}
      .metric span {{
        display: block;
        color: var(--muted);
        font-size: 13px;
        margin-bottom: 12px;
      }}
      .metric strong {{
        display: block;
        font-size: 34px;
        margin-bottom: 8px;
      }}
      .metric small {{
        color: var(--muted);
        display: block;
        line-height: 1.4;
      }}
      .layout {{
        display: grid;
        gap: 22px;
        grid-template-columns: minmax(0, 2fr) minmax(320px, 1fr);
      }}
      .owner-card {{
        background: var(--panel);
        border: 1px solid var(--border);
        border-radius: 22px;
        padding: 22px;
        margin-bottom: 18px;
      }}
      .owner-card header {{
        display: flex;
        justify-content: space-between;
        gap: 18px;
        align-items: flex-start;
        margin-bottom: 16px;
      }}
      .owner-card h3 {{
        margin: 0 0 6px;
        font-size: 28px;
      }}
      .owner-card p {{
        margin: 0;
        color: var(--muted);
      }}
      .owner-stats {{
        display: flex;
        gap: 10px;
        flex-wrap: wrap;
        justify-content: flex-end;
      }}
      .owner-stats span,
      .badge,
      .source {{
        display: inline-flex;
        align-items: center;
        border-radius: 999px;
        padding: 6px 10px;
        font-size: 12px;
        font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
        background: #f2ebdd;
        color: #4f4637;
      }}
      .owner-items {{
        display: grid;
        gap: 12px;
      }}
      .action-item {{
        background: var(--panel-strong);
        border: 1px solid var(--border);
        border-left: 5px solid var(--accent-soft);
        border-radius: 18px;
        padding: 16px;
      }}
      .urgency-overdue {{ border-left-color: var(--danger); }}
      .urgency-needs_immediate_review {{ border-left-color: var(--warn); }}
      .urgency-follow_up_due {{ border-left-color: var(--accent); }}
      .action-top {{
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
        align-items: center;
        margin-bottom: 10px;
      }}
      .date {{
        color: var(--muted);
        font-size: 12px;
      }}
      .action-item h4 {{
        margin: 0 0 6px;
        font-size: 22px;
      }}
      .action-item p {{
        margin: 0 0 8px;
        line-height: 1.45;
      }}
      .subtitle {{
        color: var(--muted);
      }}
      .notice {{
        border-radius: 16px;
        padding: 14px 16px;
        margin-bottom: 14px;
        line-height: 1.5;
      }}
      .success {{
        background: rgba(32,75,58,.08);
        border: 1px solid rgba(32,75,58,.18);
      }}
      .warning {{
        background: rgba(166,77,49,.08);
        border: 1px solid rgba(166,77,49,.18);
      }}
      .tool-card h2,
      .meta-card h2 {{
        margin: 0 0 14px;
        font-size: 28px;
      }}
      .tool-card p,
      .meta-card p {{
        color: var(--muted);
        line-height: 1.55;
      }}
      .tool-card form {{
        display: grid;
        gap: 12px;
        margin-top: 14px;
      }}
      .tool-card label {{
        display: grid;
        gap: 6px;
        font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
        font-size: 12px;
        letter-spacing: .08em;
        text-transform: uppercase;
      }}
      .tool-card input {{
        width: 100%;
        padding: 12px 14px;
        border-radius: 12px;
        border: 1px solid var(--border);
        font-size: 16px;
        background: white;
      }}
      .tool-card button {{
        border: 0;
        border-radius: 999px;
        padding: 14px 18px;
        background: linear-gradient(135deg, var(--accent), #142d24);
        color: white;
        font-size: 16px;
        font-weight: 600;
        cursor: pointer;
      }}
      #run-status {{
        margin-top: 14px;
        font-size: 14px;
        color: var(--muted);
      }}
      pre {{
        white-space: pre-wrap;
        word-break: break-word;
        font-size: 12px;
        line-height: 1.45;
        background: #fbf6eb;
        border: 1px solid var(--border);
        border-radius: 14px;
        padding: 12px;
      }}
      @media (max-width: 980px) {{
        .hero,
        .layout,
        .metrics {{
          grid-template-columns: 1fr;
        }}
        .owner-card header {{
          display: block;
        }}
        .owner-stats {{
          justify-content: flex-start;
          margin-top: 10px;
        }}
      }}
    </style>
  </head>
  <body>
    <div class="shell">
      <section class="hero">
        <div class="panel">
          <div class="topline">Agent Admin / {html.escape(data.as_of_date.isoformat())}<a class="logout" href="/admin/logout">Log out</a></div>
          <h1>Sales review and lead engine control.</h1>
          <p>This dashboard ranks action items for each owner from highest urgency to lowest, surfaces mirrored mailbox signals, and gives you one protected place to run the lead scraper and download the Instantly-ready CSV immediately.</p>
        </div>
        <div class="panel meta-card">
          <h2>Ops snapshot</h2>
          <p><strong>Latest ClickUp mirror sync:</strong> {html.escape(latest_sync)}</p>
          <p><strong>Latest stale scan summary:</strong></p>
          <pre>{latest_run_json}</pre>
        </div>
      </section>

      <section class="metrics">{metric_cards}</section>

      <section class="layout">
        <div>
          {''.join(owner_sections) or '<section class="owner-card"><p class="empty">No owner queues yet. Run a sync or stale scan to populate the dashboard.</p></section>'}
        </div>
        <aside class="panel tool-card">
          <h2>Dashboard sync</h2>
          <p>Refresh the mirrored ClickUp data and recompute stale priorities before reviewing the owner queues. Gmail sync stays off here until OAuth is fixed.</p>
          <button id="sync-dashboard-button" type="button">Sync dashboard data</button>
          <div id="sync-status">Ready.</div>

          <h2>Lead scraper</h2>
          <p>Run the existing lead build pipeline from this admin panel. The run still pushes leads into Instantly first, then returns the CSV download directly here.</p>
          {lead_builder_notice}
          <form id="lead-build-form">
            <label>
              Run date
              <input type="date" name="date" value="{html.escape(today_value)}" required />
            </label>
            <label>
              Max domains
              <input type="number" name="max_domains" min="1" max="1000" step="1" value="150" required />
            </label>
            <button type="submit">Run scraper and download CSV</button>
          </form>
          <div id="run-status">Ready.</div>
        </aside>
      </section>
    </div>
    <script>
      const syncButton = document.getElementById("sync-dashboard-button");
      const syncStatus = document.getElementById("sync-status");
      const form = document.getElementById("lead-build-form");
      const status = document.getElementById("run-status");
      syncButton?.addEventListener("click", async () => {{
        syncStatus.textContent = "Refreshing ClickUp mirror and stale queue...";
        try {{
          const response = await fetch("/admin/api/sync-dashboard", {{
            method: "POST",
          }});
          const payload = await response.json().catch(() => ({{ detail: "Dashboard sync failed." }}));
          if (!response.ok) {{
            syncStatus.textContent = payload.detail || "Dashboard sync failed.";
            return;
          }}
          syncStatus.textContent = "Dashboard sync completed. Reloading...";
          window.setTimeout(() => window.location.reload(), 900);
        }} catch (error) {{
          syncStatus.textContent = "Dashboard sync failed before a response came back.";
        }}
      }});
      form?.addEventListener("submit", async (event) => {{
        event.preventDefault();
        status.textContent = "Running lead build. This can take a minute...";
        const formData = new FormData(form);
        const payload = {{
          date: formData.get("date"),
          max_domains: Number(formData.get("max_domains") || 150),
        }};
        try {{
          const response = await fetch("/admin/api/run-lead-build", {{
            method: "POST",
            headers: {{ "Content-Type": "application/json" }},
            body: JSON.stringify(payload),
          }});
          const contentType = response.headers.get("content-type") || "";
          if (response.ok && contentType.includes("text/csv")) {{
            const blob = await response.blob();
            const disposition = response.headers.get("content-disposition") || "";
            const match = disposition.match(/filename="([^"]+)"/);
            const filename = match ? match[1] : "instantly_upload.csv";
            const url = URL.createObjectURL(blob);
            const anchor = document.createElement("a");
            anchor.href = url;
            anchor.download = filename;
            document.body.appendChild(anchor);
            anchor.click();
            anchor.remove();
            URL.revokeObjectURL(url);
            status.textContent = "Lead build finished. CSV download started.";
            return;
          }}

          const payloadJson = await response.json().catch(() => ({{ detail: "Lead build failed." }}));
          status.textContent = payloadJson.message || payloadJson.detail || payloadJson.error_type || "Lead build did not return a CSV.";
        }} catch (error) {{
          status.textContent = "Lead build failed before a response came back.";
        }}
      }});
    </script>
  </body>
</html>"""
