"""Admin dashboard data and HTML rendering."""

from __future__ import annotations

import html
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

from sqlalchemy import Select, func, select
from sqlalchemy.orm import Session

from sales_support_agent.config import Settings
from sales_support_agent.models.entities import AutomationRun, LeadMirror, MailboxSignal
from sales_support_agent.services.notification_policy import STALE_URGENCY_LABELS, STALE_URGENCY_ORDER
from sales_support_agent.services.reminders import ReminderService
from sales_support_agent.services.reply_templates import format_date_label, trim_for_slack


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
    <title>anata | Agent Admin</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@700;800&family=Roboto:wght@300;400&display=swap" rel="stylesheet">
    <style>
      :root {{
        --dark-blue: #2B3644;
        --alt-dark-blue: #33445C;
        --light-blue: #85BBDA;
        --brown: #BFA889;
        --light-brown: #F9F7F3;
        --white: #FFFFFF;
        --text: #2B3644;
        --shadow: rgba(43, 54, 68, 0.10);
        --danger: #8b4c42;
      }}
      * {{ box-sizing: border-box; }}
      body {{
        margin: 0;
        min-height: 100vh;
        background: var(--light-brown);
        color: var(--text);
        font-family: "Roboto", sans-serif;
      }}
      .topbar {{
        background: var(--alt-dark-blue);
        color: var(--white);
        padding: 18px 32px;
      }}
      .topbar-inner {{
        max-width: 1160px;
        margin: 0 auto;
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 24px;
      }}
      .brand {{
        font-family: "Montserrat", sans-serif;
        font-weight: 800;
        font-size: 42px;
        line-height: 1;
        letter-spacing: -0.06em;
      }}
      .brand .dot {{
        color: var(--light-blue);
      }}
      .cta {{
        display: inline-flex;
        align-items: center;
        justify-content: center;
        min-width: 180px;
        padding: 14px 24px;
        border-radius: 999px;
        background: var(--light-blue);
        color: var(--white);
        font-family: "Montserrat", sans-serif;
        font-weight: 700;
        font-size: 18px;
        text-decoration: none;
      }}
      .shell {{
        max-width: 1160px;
        margin: 0 auto;
        padding: 44px 24px 72px;
      }}
      .workspace {{
        background: var(--white);
        border: 1px solid rgba(43, 54, 68, 0.10);
        border-radius: 28px;
        box-shadow: 0 18px 40px var(--shadow);
        padding: 34px;
      }}
      .split {{
        display: grid;
        grid-template-columns: 1.05fr .95fr;
        gap: 40px;
        align-items: start;
      }}
      .eyebrow {{
        display: inline-block;
        padding: 14px 22px;
        border-radius: 6px;
        background: var(--dark-blue);
        color: var(--white);
        font-family: "Montserrat", sans-serif;
        font-weight: 700;
        font-size: 16px;
        line-height: 1;
        letter-spacing: 0.04em;
        text-transform: uppercase;
        margin-bottom: 24px;
      }}
      h1 {{
        margin: 0;
        font-family: "Montserrat", sans-serif;
        font-weight: 800;
        font-size: 58px;
        line-height: 0.96;
        letter-spacing: -0.05em;
        color: var(--dark-blue);
      }}
      .highlight {{
        color: var(--light-blue);
      }}
      .copy {{
        font-family: "Roboto", sans-serif;
        font-weight: 300;
        font-size: 18px;
        line-height: 1.5;
        color: var(--dark-blue);
      }}
      .copy p {{
        margin: 0 0 22px;
      }}
      .login-card {{
        margin-top: 16px;
        padding-top: 18px;
        border-top: 2px solid rgba(43, 54, 68, 0.12);
      }}
      .login-card h2 {{
        margin: 0 0 14px;
        font-family: "Montserrat", sans-serif;
        font-weight: 700;
        font-size: 36px;
        line-height: 1;
        color: var(--dark-blue);
      }}
      .login-card p {{
        margin: 0 0 26px;
        font-family: "Roboto", sans-serif;
        font-weight: 300;
        font-size: 18px;
        line-height: 1.5;
        color: var(--dark-blue);
      }}
      label {{
        display: block;
        font-family: "Montserrat", sans-serif;
        font-weight: 700;
        font-size: 16px;
        letter-spacing: 0.04em;
        text-transform: uppercase;
        margin-bottom: 12px;
      }}
      input {{
        width: 100%;
        padding: 18px 20px;
        border-radius: 10px;
        border: 2px solid rgba(43, 54, 68, 0.16);
        background: var(--white);
        font-family: "Roboto", sans-serif;
        font-weight: 300;
        font-size: 18px;
        margin-bottom: 22px;
        color: var(--dark-blue);
      }}
      button {{
        width: auto;
        border: 0;
        border-radius: 999px;
        padding: 16px 28px;
        background: var(--light-blue);
        color: var(--white);
        font-family: "Montserrat", sans-serif;
        font-weight: 700;
        font-size: 18px;
        cursor: pointer;
        box-shadow: 0 18px 34px var(--shadow);
      }}
      .notice {{
        border-radius: 10px;
        padding: 16px 18px;
        margin-bottom: 20px;
        font-family: "Roboto", sans-serif;
        font-weight: 300;
        font-size: 16px;
      }}
      .error {{
        background: rgba(138,63,47,.08);
        color: var(--danger);
        border: 1px solid rgba(138,63,47,.18);
      }}
      .footer-bar {{
        height: 18px;
        background: var(--alt-dark-blue);
        margin-top: 64px;
      }}
      @media (max-width: 1200px) {{
        .topbar {{
          padding: 18px 24px;
        }}
        .shell {{
          padding: 32px 20px 60px;
        }}
        .split {{
          grid-template-columns: 1fr;
          gap: 28px;
        }}
        .brand {{
          font-size: 38px;
        }}
        h1 {{
          font-size: clamp(40px, 10vw, 58px);
        }}
      }}
      @media (max-width: 920px) {{
        .topbar-inner {{
          flex-wrap: wrap;
        }}
        .cta {{
          min-width: 160px;
        }}
        .copy,
        .login-card p,
        input,
        label,
        button {{
          font-size: 16px;
        }}
      }}
    </style>
  </head>
  <body>
    <header class="topbar">
      <div class="topbar-inner">
        <div class="brand">agent<span class="dot">.</span></div>
        <div class="cta">AGENT LOGIN</div>
      </div>
    </header>
    <main class="shell">
      <div class="workspace">
      <div class="split">
        <section>
          <div class="eyebrow">Agent admin</div>
          <h1>Access your <span class="highlight">sales</span> workspace.</h1>
        </section>
        <section class="copy">
          <p>This dashboard keeps lead pulls, owner priorities, and sync controls in one place so the team can move quickly without digging through multiple tools.</p>
          <div class="login-card">
            <h2>Enter the dashboard.</h2>
            <p>Use the admin password to review priorities, sync fresh data, and run a lead pull.</p>
            {error_html}
            <form method="post" action="/admin/login">
              <label for="password">Password</label>
              <input id="password" name="password" type="password" autocomplete="current-password" required />
              <button type="submit">GET STARTED</button>
            </form>
          </div>
        </section>
      </div>
      </div>
    </main>
    <div class="footer-bar" aria-hidden="true"></div>
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

    def _format_summary_value(value: object) -> str:
        if isinstance(value, bool):
            return "Yes" if value else "No"
        return str(value)

    def _summary_row(label: str, value: object) -> str:
        return (
            '<div class="snapshot-row">'
            f"<span>{html.escape(label)}</span>"
            f"<strong>{html.escape(_format_summary_value(value))}</strong>"
            "</div>"
        )

    metric_cards = "".join(
        [
            _card("Leads", str(data.total_active_leads), "Current ClickUp leads in active statuses"),
            _card("Overdue", str(data.stale_counts.get("overdue", 0)), "Highest priority follow-up risk"),
            _card(
                "Review",
                str(data.stale_counts.get("needs_immediate_review", 0)),
                "Untouched or missing-next-step leads",
            ),
            _card("Follow-up", str(data.stale_counts.get("follow_up_due", 0)), "Routine queue ready for review"),
            _card("Mailbox", str(data.mailbox_findings), "Signals captured in the last 7 days"),
        ]
    )

    owner_options = "".join(
        f'<option value="{html.escape(queue.owner_name)}">{html.escape(queue.owner_name)}</option>'
        for queue in data.owner_queues
    )

    owner_sections = []
    for queue in data.owner_queues:
        item_cards = []
        for item in queue.items:
            urgency_label = STALE_URGENCY_LABELS.get(item.urgency, item.urgency.replace("_", " ").title())
            draft_preview = trim_for_slack(item.suggested_reply, limit=120)
            link_html = (
                f'<a href="{html.escape(item.link_url)}" target="_blank" rel="noreferrer">Open task</a>'
                if item.link_url
                else ""
            )
            item_cards.append(
                f"""
                <article class="action-item urgency-{html.escape(item.urgency)}" data-owner="{html.escape(queue.owner_name)}" data-urgency="{html.escape(item.urgency)}">
                  <div class="action-top">
                    <span class="badge">{html.escape(urgency_label)}</span>
                    <span class="source">{html.escape(item.source)}</span>
                    <span class="date">{html.escape(item.date_label)}</span>
                  </div>
                  <h4>{html.escape(item.title)}</h4>
                  <p class="subtitle">{html.escape(item.subtitle)}</p>
                  <p><strong>Action:</strong> {html.escape(item.action_summary)}</p>
                  <p><strong>Draft:</strong> {html.escape(draft_preview)}</p>
                  {link_html}
                </article>
                """
            )
        owner_sections.append(
            f"""
            <section class="owner-card" data-owner="{html.escape(queue.owner_name)}">
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
    lead_builder_notice = (
        '<div class="notice warning">Lead builder is missing env vars: '
        + html.escape(", ".join(data.lead_builder_missing))
        + "</div>"
        if not data.lead_builder_ready
        else '<div class="notice success">Lead builder is ready. Running it here will still add leads to Instantly and return the CSV immediately.</div>'
    )
    today_value = data.as_of_date.isoformat()
    latest_run_summary = data.latest_run_summary or {}
    snapshot_rows = [
        _summary_row("Latest ClickUp sync", latest_sync),
        _summary_row("Stale scan status", latest_run_summary.get("status", "No stale scan recorded")),
        _summary_row("Inspected leads", latest_run_summary.get("inspected", 0)),
        _summary_row("Alerts prepared", latest_run_summary.get("alerted", 0)),
        _summary_row("Comments posted", latest_run_summary.get("commented", 0)),
        _summary_row("Tasks synced", latest_run_summary.get("synced_tasks", 0)),
        _summary_row("Failed items", latest_run_summary.get("failed", 0)),
    ]
    if "digest_posted" in latest_run_summary:
        snapshot_rows.append(_summary_row("Digest posted", latest_run_summary.get("digest_posted")))
    if "immediate_alerted" in latest_run_summary:
        snapshot_rows.append(_summary_row("Immediate alerts", latest_run_summary.get("immediate_alerted")))

    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>anata | Agent Admin Dashboard</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@700;800&family=Roboto:wght@300;400&display=swap" rel="stylesheet">
    <style>
      :root {{
        --dark-blue: #2B3644;
        --alt-dark-blue: #33445C;
        --light-blue: #85BBDA;
        --brown: #BFA889;
        --light-brown: #F9F7F3;
        --white: #FFFFFF;
        --text: #2B3644;
        --danger: #9A5A4E;
        --warn: #BFA889;
        --shadow: rgba(43, 54, 68, 0.10);
      }}
      * {{ box-sizing: border-box; }}
      body {{
        margin: 0;
        background: var(--light-brown);
        color: var(--text);
        font-family: "Roboto", sans-serif;
      }}
      a {{ color: var(--dark-blue); }}
      .topbar {{
        background: var(--alt-dark-blue);
        color: var(--white);
        padding: 18px 32px;
      }}
      .topbar-inner {{
        max-width: 1240px;
        margin: 0 auto;
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 24px;
      }}
      .brandmark {{
        display: inline-flex;
        align-items: center;
        gap: 0;
        font-family: "Montserrat", sans-serif;
        font-weight: 800;
        font-size: 42px;
        line-height: 1;
        letter-spacing: -0.06em;
        color: var(--white);
      }}
      .brandmark .dot {{
        color: var(--light-blue);
      }}
      .topcta {{
        display: inline-flex;
        align-items: center;
        justify-content: center;
        min-width: 150px;
        padding: 14px 22px;
        border-radius: 999px;
        background: var(--light-blue);
        color: var(--white);
        font-family: "Montserrat", sans-serif;
        font-weight: 700;
        font-size: 18px;
        text-decoration: none;
      }}
      .shell {{
        max-width: 1240px;
        margin: 0 auto;
        padding: 32px 20px 72px;
      }}
      .workspace {{
        background: var(--white);
        border: 1px solid rgba(43, 54, 68, 0.10);
        border-radius: 28px;
        box-shadow: 0 18px 40px var(--shadow);
        padding: 28px;
      }}
      .page-header {{
        display: grid;
        grid-template-columns: minmax(0, 1.25fr) minmax(280px, 0.75fr);
        gap: 24px;
        align-items: end;
        padding-bottom: 22px;
        border-bottom: 1px solid rgba(43, 54, 68, 0.10);
        margin-bottom: 24px;
      }}
      .eyebrow {{
        display: inline-block;
        padding: 12px 18px;
        border-radius: 6px;
        background: var(--dark-blue);
        color: var(--white);
        font-family: "Montserrat", sans-serif;
        font-weight: 700;
        font-size: 16px;
        line-height: 1;
        letter-spacing: 0.04em;
        text-transform: uppercase;
        margin-bottom: 18px;
      }}
      .page-title {{
        margin: 0;
        font-family: "Montserrat", sans-serif;
        font-weight: 800;
        font-size: 60px;
        line-height: 0.94;
        letter-spacing: -0.05em;
        color: var(--dark-blue);
      }}
      .highlight {{
        color: var(--light-blue);
      }}
      .page-copy {{
        font-weight: 300;
        font-size: 18px;
        line-height: 1.5;
        color: var(--dark-blue);
      }}
      .controls-grid {{
        display: grid;
        grid-template-columns: minmax(0, 0.9fr) minmax(0, 1.2fr) minmax(280px, 0.9fr);
        gap: 18px;
        margin-bottom: 24px;
      }}
      .panel-card {{
        background: var(--white);
        border: 2px solid rgba(43, 54, 68, 0.10);
        border-radius: 18px;
        padding: 20px 22px;
      }}
      .panel-card h3 {{
        margin: 0 0 8px;
        font-family: "Montserrat", sans-serif;
        font-weight: 700;
        font-size: 30px;
        color: var(--dark-blue);
      }}
      .panel-card p {{
        margin: 0 0 18px;
        font-weight: 300;
        font-size: 18px;
        line-height: 1.45;
      }}
      .panel-card button,
      .lead-form button {{
        width: auto;
        border: 0;
        border-radius: 999px;
        padding: 14px 24px;
        background: var(--light-blue);
        color: var(--white);
        font-family: "Montserrat", sans-serif;
        font-weight: 700;
        font-size: 18px;
        cursor: pointer;
      }}
      .metrics {{
        display: grid;
        grid-template-columns: repeat(5, minmax(0, 1fr));
        gap: 14px;
        margin-bottom: 24px;
      }}
      .metric {{
        background: var(--white);
        border: 2px solid rgba(43, 54, 68, 0.10);
        border-radius: 18px;
        padding: 18px;
        min-height: 156px;
      }}
      .metric span {{
        display: block;
        font-family: "Montserrat", sans-serif;
        font-weight: 700;
        font-size: 16px;
        line-height: 1;
        letter-spacing: 0.04em;
        text-transform: uppercase;
        color: var(--alt-dark-blue);
        margin-bottom: 14px;
      }}
      .metric strong {{
        display: block;
        font-family: "Montserrat", sans-serif;
        font-weight: 800;
        font-size: 40px;
        line-height: 1;
        color: var(--dark-blue);
        margin-bottom: 10px;
      }}
      .metric small {{
        color: var(--dark-blue);
        display: block;
        font-weight: 300;
        font-size: 15px;
        line-height: 1.45;
      }}
      .layout {{
        display: grid;
        gap: 24px;
        grid-template-columns: minmax(0, 1.55fr) minmax(320px, .85fr);
      }}
      .section-bar {{
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 16px;
        margin-bottom: 18px;
      }}
      .section-title {{
        margin: 0;
        font-family: "Montserrat", sans-serif;
        font-weight: 700;
        font-size: 34px;
        line-height: 1;
        color: var(--dark-blue);
      }}
      .filters {{
        display: flex;
        flex-wrap: wrap;
        gap: 12px;
        align-items: center;
      }}
      .filters select {{
        min-width: 190px;
        border: 2px solid rgba(43, 54, 68, 0.14);
        border-radius: 999px;
        padding: 12px 16px;
        font-family: "Roboto", sans-serif;
        font-size: 16px;
        background: var(--white);
        color: var(--dark-blue);
      }}
      .filter-buttons {{
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
      }}
      .filter-button {{
        border: 1px solid rgba(43, 54, 68, 0.14);
        border-radius: 999px;
        padding: 10px 14px;
        background: var(--white);
        color: var(--dark-blue);
        font-family: "Montserrat", sans-serif;
        font-size: 14px;
        font-weight: 700;
        cursor: pointer;
      }}
      .filter-button.is-active {{
        background: var(--dark-blue);
        color: var(--white);
        border-color: var(--dark-blue);
      }}
      .owner-card {{
        background: var(--white);
        border: 2px solid rgba(43, 54, 68, 0.10);
        border-radius: 22px;
        padding: 20px;
        margin-bottom: 16px;
      }}
      .owner-card header {{
        display: flex;
        justify-content: space-between;
        gap: 18px;
        align-items: flex-start;
        margin-bottom: 16px;
      }}
      .owner-card h3 {{
        margin: 0 0 8px;
        font-family: "Montserrat", sans-serif;
        font-weight: 700;
        font-size: 28px;
        line-height: 1;
        color: var(--dark-blue);
      }}
      .owner-card p {{
        margin: 0;
        color: var(--dark-blue);
        font-weight: 300;
        font-size: 16px;
      }}
      .owner-stats {{
        display: flex;
        gap: 10px;
        flex-wrap: wrap;
        justify-content: flex-end;
      }}
      .owner-stats span {{
        display: inline-flex;
        align-items: center;
        border-radius: 999px;
        padding: 10px 14px;
        font-family: "Montserrat", sans-serif;
        font-weight: 700;
        font-size: 14px;
        background: rgba(133, 187, 218, 0.20);
        color: var(--dark-blue);
      }}
      .badge,
      .source {{
        display: inline-flex;
        align-items: center;
        border-radius: 999px;
        padding: 7px 12px;
        font-family: "Montserrat", sans-serif;
        font-weight: 700;
        font-size: 12px;
        background: rgba(191, 168, 137, 0.22);
        color: var(--dark-blue);
      }}
      .owner-items {{
        display: grid;
        gap: 16px;
      }}
      .action-item {{
        background: var(--light-brown);
        border: 2px solid rgba(43, 54, 68, 0.08);
        border-left: 8px solid var(--light-blue);
        border-radius: 18px;
        padding: 18px;
      }}
      .urgency-overdue {{ border-left-color: var(--dark-blue); background: rgba(51, 68, 92, 0.06); }}
      .urgency-needs_immediate_review {{ border-left-color: var(--brown); }}
      .urgency-follow_up_due {{ border-left-color: var(--light-blue); background: rgba(133, 187, 218, 0.10); }}
      .action-top {{
        display: flex;
        flex-wrap: wrap;
        gap: 10px;
        align-items: center;
        margin-bottom: 14px;
      }}
      .date {{
        color: var(--dark-blue);
        font-family: "Roboto", sans-serif;
        font-weight: 300;
        font-size: 14px;
      }}
      .action-item h4 {{
        margin: 0 0 10px;
        font-family: "Montserrat", sans-serif;
        font-weight: 700;
        font-size: 24px;
        line-height: 1.02;
        color: var(--dark-blue);
      }}
      .action-item p {{
        margin: 0 0 10px;
        font-weight: 300;
        font-size: 16px;
        line-height: 1.45;
      }}
      .subtitle {{
        color: var(--alt-dark-blue);
      }}
      .notice {{
        border-radius: 12px;
        padding: 14px 16px;
        margin-bottom: 14px;
        line-height: 1.35;
        font-weight: 300;
        font-size: 15px;
      }}
      .success {{
        background: rgba(133, 187, 218, 0.14);
        border: 1px solid rgba(133, 187, 218, 0.30);
      }}
      .warning {{
        background: rgba(191, 168, 137, 0.18);
        border: 1px solid rgba(191, 168, 137, 0.30);
      }}
      .status-line {{
        margin-top: 14px;
        font-weight: 300;
        font-size: 14px;
        color: var(--dark-blue);
      }}
      .meta-card {{
        background: var(--white);
        border: 2px solid rgba(43, 54, 68, 0.10);
        border-radius: 22px;
        padding: 20px;
      }}
      .meta-card h2 {{
        margin: 0 0 14px;
        font-family: "Montserrat", sans-serif;
        font-weight: 700;
        font-size: 28px;
        color: var(--dark-blue);
      }}
      .meta-card p {{
        margin: 0 0 18px;
        font-weight: 300;
        font-size: 16px;
        line-height: 1.45;
      }}
      .snapshot-rows {{
        display: grid;
        gap: 10px;
      }}
      .snapshot-row {{
        display: flex;
        justify-content: space-between;
        gap: 12px;
        align-items: center;
        padding: 10px 0;
        border-bottom: 1px solid rgba(43, 54, 68, 0.08);
      }}
      .snapshot-row:last-child {{
        border-bottom: 0;
        padding-bottom: 0;
      }}
      .snapshot-row span {{
        font-size: 14px;
        color: var(--alt-dark-blue);
        text-transform: uppercase;
        letter-spacing: 0.04em;
        font-family: "Montserrat", sans-serif;
        font-weight: 700;
      }}
      .snapshot-row strong {{
        font-size: 16px;
        color: var(--dark-blue);
        font-family: "Roboto", sans-serif;
        font-weight: 400;
      }}
      .tools-column {{
        display: grid;
        gap: 16px;
        align-content: start;
      }}
      .lead-form {{
        display: grid;
        gap: 14px;
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }}
      .lead-form label {{
        display: grid;
        gap: 8px;
        font-family: "Montserrat", sans-serif;
        font-weight: 700;
        font-size: 14px;
        text-transform: uppercase;
        letter-spacing: 0.04em;
      }}
      .lead-form input {{
        width: 100%;
        padding: 18px 20px;
        border-radius: 10px;
        border: 2px solid rgba(43, 54, 68, 0.16);
        background: var(--white);
        font-family: "Roboto", sans-serif;
        font-weight: 300;
        font-size: 16px;
        color: var(--dark-blue);
      }}
      .lead-form .lead-submit {{
        grid-column: 1 / -1;
        display: flex;
        align-items: end;
      }}
      .footer-bar {{
        height: 18px;
        background: var(--alt-dark-blue);
        margin-top: 72px;
      }}
      .empty {{
        font-size: 16px;
      }}
      @media (max-width: 1280px) {{
        .topbar {{
          padding: 18px 24px;
        }}
        .shell {{
          padding: 24px 16px 56px;
        }}
        .page-header,
        .controls-grid,
        .layout,
        .metrics,
        .lead-form {{
          grid-template-columns: 1fr;
        }}
        .page-title {{
          font-size: clamp(38px, 9vw, 60px);
        }}
      }}
      @media (max-width: 960px) {{
        .topbar-inner {{
          flex-wrap: wrap;
        }}
        .topcta {{
          min-width: 130px;
        }}
        .brandmark {{
          font-size: 36px;
        }}
        .eyebrow,
        .metric span,
        .lead-form label {{
          font-size: 13px;
        }}
        .page-copy,
        .action-item p,
        .owner-card p,
        .meta-card p,
        .panel-card p,
        .lead-form input {{
          font-size: 15px;
        }}
        .section-title,
        .metric strong {{
          font-size: 28px;
        }}
        .owner-card h3,
        .action-item h4,
        .meta-card h2,
        .panel-card h3 {{
          font-size: 24px;
        }}
      }}
    </style>
  </head>
  <body>
    <header class="topbar">
      <div class="topbar-inner">
        <div class="brandmark">agent<span class="dot">.</span></div>
        <a class="topcta" href="/admin/logout">LOG OUT</a>
      </div>
    </header>
    <div class="shell">
      <div class="workspace">
      <section class="page-header">
        <div>
          <div class="eyebrow">Agent dashboard</div>
          <h1 class="page-title">Sales <span class="highlight">Priorities</span>.</h1>
        </div>
        <div class="page-copy">
          This dashboard keeps owner action items, sync controls, and lead pulls in one place so the team can move quickly and review the queue without extra noise.
        </div>
      </section>

      <section class="controls-grid">
        <div class="panel-card">
          <h3>Sync data</h3>
          <p>Refresh the ClickUp mirror and recompute stale priorities before reviewing the board.</p>
          <button id="sync-dashboard-button" type="button">SYNC DATA</button>
          <div class="status-line" id="sync-status">Ready.</div>
        </div>
        <div class="panel-card" id="lead-pull-panel">
          <h3>Run lead pull</h3>
          <p>Run the existing lead build pipeline here. Leads still go to Instantly first, then the CSV downloads immediately.</p>
          {lead_builder_notice}
          <form class="lead-form" id="lead-build-form">
            <label>
              Run date
              <input type="date" name="date" value="{html.escape(today_value)}" required />
            </label>
            <label>
              Max domains
              <input type="number" name="max_domains" min="1" max="1000" step="1" value="150" required />
            </label>
            <div class="lead-submit">
              <button type="submit">PULL LEADS</button>
            </div>
          </form>
          <div class="status-line" id="run-status">Scrape Status: Ready.</div>
        </div>
        <section class="meta-card">
          <h2>Ops snapshot</h2>
          <div class="snapshot-rows">
            {''.join(snapshot_rows)}
          </div>
        </section>
      </section>

      <section class="metrics">{metric_cards}</section>

      <section class="layout">
        <div>
          <div class="section-bar">
            <h2 class="section-title">Owner priorities.</h2>
            <div class="filters">
              <select id="owner-filter" aria-label="Filter by owner">
                <option value="all">All owners</option>
                {owner_options}
              </select>
              <div class="filter-buttons" id="urgency-filter">
                <button class="filter-button is-active" type="button" data-urgency="all">All</button>
                <button class="filter-button" type="button" data-urgency="overdue">Overdue</button>
                <button class="filter-button" type="button" data-urgency="needs_immediate_review">Review</button>
                <button class="filter-button" type="button" data-urgency="follow_up_due">Due</button>
              </div>
            </div>
          </div>
          {''.join(owner_sections) or '<section class="owner-card"><p class="empty">No owner queues yet. Run a sync or stale scan to populate the dashboard.</p></section>'}
        </div>
        <div class="tools-column">
          <section class="meta-card">
            <h2>Queue guide</h2>
            <p><strong>Overdue</strong> means the follow-up window has passed and should be handled first.</p>
            <p><strong>Review</strong> flags items that need a decision, response, or status cleanup.</p>
            <p><strong>Due</strong> covers routine follow-ups that are ready for the next touch.</p>
          </section>
        </div>
      </section>
      </div>
    </div>
    <div class="footer-bar" aria-hidden="true"></div>
    <script>
      const syncButton = document.getElementById("sync-dashboard-button");
      const syncStatus = document.getElementById("sync-status");
      const form = document.getElementById("lead-build-form");
      const status = document.getElementById("run-status");
      const ownerFilter = document.getElementById("owner-filter");
      const urgencyButtons = document.querySelectorAll("#urgency-filter .filter-button");
      let activeUrgency = "all";

      function applyQueueFilters() {{
        const selectedOwner = ownerFilter?.value || "all";
        const ownerCards = document.querySelectorAll(".owner-card[data-owner]");

        ownerCards.forEach((card) => {{
          const ownerName = card.dataset.owner || "";
          const itemNodes = card.querySelectorAll(".action-item");
          let visibleItems = 0;

          itemNodes.forEach((item) => {{
            const matchesOwner = selectedOwner === "all" || ownerName === selectedOwner;
            const matchesUrgency = activeUrgency === "all" || item.dataset.urgency === activeUrgency;
            const shouldShow = matchesOwner && matchesUrgency;
            item.style.display = shouldShow ? "" : "none";
            if (shouldShow) {{
              visibleItems += 1;
            }}
          }});

          card.style.display = visibleItems > 0 ? "" : "none";
        }});
      }}

      ownerFilter?.addEventListener("change", applyQueueFilters);
      urgencyButtons.forEach((button) => {{
        button.addEventListener("click", () => {{
          activeUrgency = button.dataset.urgency || "all";
          urgencyButtons.forEach((node) => node.classList.toggle("is-active", node === button));
          applyQueueFilters();
        }});
      }});

      syncButton?.addEventListener("click", async () => {{
        syncStatus.textContent = "Refreshing sync...";
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
      applyQueueFilters();
    </script>
  </body>
</html>"""
