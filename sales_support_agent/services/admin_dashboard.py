"""Admin dashboard data and HTML rendering."""

from __future__ import annotations

import html
import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

from sqlalchemy import Select, func, select
from sqlalchemy.orm import Session

from sales_support_agent.config import Settings
from sales_support_agent.models.entities import AutomationRun, CanvaConnection, CommunicationEvent, LeadMirror, MailboxSignal
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
    deck_generator_ready: bool
    deck_generator_missing: list[str]
    deck_canva_connected: bool
    deck_canva_display_name: str
    deck_canva_capabilities: dict[str, bool]
    deck_google_source: str
    deck_template_id: str
    recent_deck_runs: list[dict[str, object]]


@dataclass(frozen=True)
class ExecutiveOwnerScorecard:
    owner_name: str
    active_leads: int
    overdue_count: int
    review_count: int
    due_count: int
    avg_days_since_touch: float | None
    late_stage_leads: int
    late_stage_stale_leads: int
    mailbox_signals_pending: int
    value_total: float | None


@dataclass(frozen=True)
class ExecutiveRiskLead:
    owner_name: str
    task_name: str
    status: str
    source: str
    urgency: str
    value_label: str
    value_numeric: float | None
    days_since_touch: int | None
    next_step: str
    link_url: str


@dataclass(frozen=True)
class ExecutiveLeadRecord:
    owner_name: str
    task_name: str
    status: str
    source: str
    urgency: str
    value_label: str
    value_numeric: float | None
    days_since_touch: int | None
    late_stage: bool
    late_stage_stale: bool
    missing_next_action: bool
    missing_meeting_outcome: bool
    untouched_new_or_contacted: bool
    next_step: str
    link_url: str


@dataclass(frozen=True)
class ExecutiveDistributionItem:
    label: str
    count: int


@dataclass(frozen=True)
class ExecutiveOwnerMetric:
    owner_name: str
    count: int


@dataclass(frozen=True)
class ExecutiveData:
    as_of_date: date
    latest_sync_at: datetime | None
    latest_run_summary: dict
    summary_text: str
    kpis: dict[str, int]
    owner_scorecards: list[ExecutiveOwnerScorecard]
    status_distribution: list[ExecutiveDistributionItem]
    source_distribution: list[ExecutiveDistributionItem]
    aging_buckets: list[ExecutiveDistributionItem]
    late_stage_distribution: list[ExecutiveDistributionItem]
    risk_leads: list[ExecutiveRiskLead]
    inbound_replies_by_owner: list[ExecutiveOwnerMetric]
    mailbox_signals_by_owner: list[ExecutiveOwnerMetric]
    hygiene_counts: dict[str, int]
    filters: dict[str, list[str]]
    lead_records: list[ExecutiveLeadRecord]


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
        "deck_generator_ready": data.deck_generator_ready,
        "deck_generator_missing": data.deck_generator_missing,
        "deck_canva_connected": data.deck_canva_connected,
        "deck_canva_display_name": data.deck_canva_display_name,
        "deck_canva_capabilities": data.deck_canva_capabilities,
        "deck_google_source": data.deck_google_source,
        "deck_template_id": data.deck_template_id,
        "recent_deck_runs": data.recent_deck_runs,
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
        deck_generator_ready=bool(payload.get("deck_generator_ready")),
        deck_generator_missing=[str(item) for item in payload.get("deck_generator_missing", [])],
        deck_canva_connected=bool(payload.get("deck_canva_connected")),
        deck_canva_display_name=str(payload.get("deck_canva_display_name", "")),
        deck_canva_capabilities={str(key): bool(value) for key, value in dict(payload.get("deck_canva_capabilities", {})).items()},
        deck_google_source=str(payload.get("deck_google_source", "")),
        deck_template_id=str(payload.get("deck_template_id", "")),
        recent_deck_runs=[dict(item) for item in payload.get("recent_deck_runs", [])],
    )


def executive_data_to_dict(data: ExecutiveData) -> dict[str, object]:
    return {
        "as_of_date": data.as_of_date.isoformat(),
        "latest_sync_at": data.latest_sync_at.isoformat() if data.latest_sync_at else "",
        "latest_run_summary": data.latest_run_summary,
        "summary_text": data.summary_text,
        "kpis": data.kpis,
        "owner_scorecards": [
            {
                "owner_name": item.owner_name,
                "active_leads": item.active_leads,
                "overdue_count": item.overdue_count,
                "review_count": item.review_count,
                "due_count": item.due_count,
                "avg_days_since_touch": item.avg_days_since_touch,
                "late_stage_leads": item.late_stage_leads,
                "late_stage_stale_leads": item.late_stage_stale_leads,
                "mailbox_signals_pending": item.mailbox_signals_pending,
                "value_total": item.value_total,
            }
            for item in data.owner_scorecards
        ],
        "status_distribution": [{"label": item.label, "count": item.count} for item in data.status_distribution],
        "source_distribution": [{"label": item.label, "count": item.count} for item in data.source_distribution],
        "aging_buckets": [{"label": item.label, "count": item.count} for item in data.aging_buckets],
        "late_stage_distribution": [{"label": item.label, "count": item.count} for item in data.late_stage_distribution],
        "risk_leads": [
            {
                "owner_name": item.owner_name,
                "task_name": item.task_name,
                "status": item.status,
                "source": item.source,
                "urgency": item.urgency,
                "value_label": item.value_label,
                "value_numeric": item.value_numeric,
                "days_since_touch": item.days_since_touch,
                "next_step": item.next_step,
                "link_url": item.link_url,
            }
            for item in data.risk_leads
        ],
        "inbound_replies_by_owner": [{"owner_name": item.owner_name, "count": item.count} for item in data.inbound_replies_by_owner],
        "mailbox_signals_by_owner": [{"owner_name": item.owner_name, "count": item.count} for item in data.mailbox_signals_by_owner],
        "hygiene_counts": data.hygiene_counts,
        "filters": data.filters,
        "lead_records": [
            {
                "owner_name": item.owner_name,
                "task_name": item.task_name,
                "status": item.status,
                "source": item.source,
                "urgency": item.urgency,
                "value_label": item.value_label,
                "value_numeric": item.value_numeric,
                "days_since_touch": item.days_since_touch,
                "late_stage": item.late_stage,
                "late_stage_stale": item.late_stage_stale,
                "missing_next_action": item.missing_next_action,
                "missing_meeting_outcome": item.missing_meeting_outcome,
                "untouched_new_or_contacted": item.untouched_new_or_contacted,
                "next_step": item.next_step,
                "link_url": item.link_url,
            }
            for item in data.lead_records
        ],
    }


def executive_data_from_dict(payload: dict[str, object]) -> ExecutiveData:
    def _distribution(key: str) -> list[ExecutiveDistributionItem]:
        return [
            ExecutiveDistributionItem(label=str(item.get("label", "")), count=int(item.get("count", 0) or 0))
            for item in payload.get(key, [])
        ]

    return ExecutiveData(
        as_of_date=date.fromisoformat(str(payload.get("as_of_date"))),
        latest_sync_at=(datetime.fromisoformat(str(payload.get("latest_sync_at"))) if str(payload.get("latest_sync_at") or "") else None),
        latest_run_summary=dict(payload.get("latest_run_summary", {})),
        summary_text=str(payload.get("summary_text", "")),
        kpis={str(key): int(value or 0) for key, value in dict(payload.get("kpis", {})).items()},
        owner_scorecards=[
            ExecutiveOwnerScorecard(
                owner_name=str(item.get("owner_name", "")),
                active_leads=int(item.get("active_leads", 0) or 0),
                overdue_count=int(item.get("overdue_count", 0) or 0),
                review_count=int(item.get("review_count", 0) or 0),
                due_count=int(item.get("due_count", 0) or 0),
                avg_days_since_touch=float(item["avg_days_since_touch"]) if item.get("avg_days_since_touch") is not None else None,
                late_stage_leads=int(item.get("late_stage_leads", 0) or 0),
                late_stage_stale_leads=int(item.get("late_stage_stale_leads", 0) or 0),
                mailbox_signals_pending=int(item.get("mailbox_signals_pending", 0) or 0),
                value_total=float(item["value_total"]) if item.get("value_total") is not None else None,
            )
            for item in payload.get("owner_scorecards", [])
        ],
        status_distribution=_distribution("status_distribution"),
        source_distribution=_distribution("source_distribution"),
        aging_buckets=_distribution("aging_buckets"),
        late_stage_distribution=_distribution("late_stage_distribution"),
        risk_leads=[
            ExecutiveRiskLead(
                owner_name=str(item.get("owner_name", "")),
                task_name=str(item.get("task_name", "")),
                status=str(item.get("status", "")),
                source=str(item.get("source", "")),
                urgency=str(item.get("urgency", "")),
                value_label=str(item.get("value_label", "")),
                value_numeric=float(item["value_numeric"]) if item.get("value_numeric") is not None else None,
                days_since_touch=int(item["days_since_touch"]) if item.get("days_since_touch") is not None else None,
                next_step=str(item.get("next_step", "")),
                link_url=str(item.get("link_url", "")),
            )
            for item in payload.get("risk_leads", [])
        ],
        inbound_replies_by_owner=[
            ExecutiveOwnerMetric(owner_name=str(item.get("owner_name", "")), count=int(item.get("count", 0) or 0))
            for item in payload.get("inbound_replies_by_owner", [])
        ],
        mailbox_signals_by_owner=[
            ExecutiveOwnerMetric(owner_name=str(item.get("owner_name", "")), count=int(item.get("count", 0) or 0))
            for item in payload.get("mailbox_signals_by_owner", [])
        ],
        hygiene_counts={str(key): int(value or 0) for key, value in dict(payload.get("hygiene_counts", {})).items()},
        filters={str(key): [str(item) for item in value] for key, value in dict(payload.get("filters", {})).items()},
        lead_records=[
            ExecutiveLeadRecord(
                owner_name=str(item.get("owner_name", "")),
                task_name=str(item.get("task_name", "")),
                status=str(item.get("status", "")),
                source=str(item.get("source", "")),
                urgency=str(item.get("urgency", "")),
                value_label=str(item.get("value_label", "")),
                value_numeric=float(item["value_numeric"]) if item.get("value_numeric") is not None else None,
                days_since_touch=int(item["days_since_touch"]) if item.get("days_since_touch") is not None else None,
                late_stage=bool(item.get("late_stage")),
                late_stage_stale=bool(item.get("late_stage_stale")),
                missing_next_action=bool(item.get("missing_next_action")),
                missing_meeting_outcome=bool(item.get("missing_meeting_outcome")),
                untouched_new_or_contacted=bool(item.get("untouched_new_or_contacted")),
                next_step=str(item.get("next_step", "")),
                link_url=str(item.get("link_url", "")),
            )
            for item in payload.get("lead_records", [])
        ],
    )


def _safe_numeric_value(raw_value: str) -> float | None:
    value = (raw_value or "").strip().lower()
    if not value:
        return None
    cleaned = value.replace("$", "").replace(",", "").replace("usd", "").strip()
    multiplier = 1.0
    if cleaned.endswith("k"):
        multiplier = 1000.0
        cleaned = cleaned[:-1]
    elif cleaned.endswith("m"):
        multiplier = 1_000_000.0
        cleaned = cleaned[:-1]
    cleaned = re.sub(r"[^0-9.\-]", "", cleaned)
    if not cleaned or cleaned in {"-", ".", "-."}:
        return None
    try:
        return float(cleaned) * multiplier
    except ValueError:
        return None


def _display_source_name(raw_value: str) -> str:
    value = (raw_value or "").strip()
    if not value:
        return "Unknown"

    normalized = re.sub(r"[_\-]+", " ", value)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    if not normalized:
        return "Unknown"

    known_labels = {
        "apollo": "Apollo",
        "clickup": "ClickUp",
        "gmail": "Gmail",
        "instantly": "Instantly",
        "linkedin": "LinkedIn",
        "storeleads": "StoreLeads",
        "manual": "Manual",
        "referral": "Referral",
        "unknown": "Unknown",
    }
    lower = normalized.lower()
    if lower in known_labels:
        return known_labels[lower]

    return " ".join(part.upper() if len(part) <= 3 else part.capitalize() for part in normalized.split())


def _latest_touch_for_lead(lead: LeadMirror) -> datetime | None:
    return max(
        (dt for dt in [lead.last_meaningful_touch_at, lead.last_outbound_at, lead.last_inbound_at] if dt is not None),
        default=None,
    )


def _days_since_touch(lead: LeadMirror, effective_date: date) -> int | None:
    last_touch = _latest_touch_for_lead(lead)
    if last_touch is not None:
        return max((effective_date - last_touch.date()).days, 0)
    if lead.created_at is not None:
        return max((effective_date - lead.created_at.date()).days, 0)
    return None


def _distribution_from_counter(counter: Counter[str], *, preferred_order: list[str] | None = None) -> list[ExecutiveDistributionItem]:
    items = list(counter.items())
    if preferred_order:
        order_map = {label: index for index, label in enumerate(preferred_order)}
        items.sort(key=lambda item: (order_map.get(item[0], len(order_map)), -item[1], item[0].lower()))
    else:
        items.sort(key=lambda item: (-item[1], item[0].lower()))
    return [ExecutiveDistributionItem(label=label, count=count) for label, count in items]


def _build_executive_summary_text(
    *,
    total_active_leads: int,
    overdue_count: int,
    review_count: int,
    untouched_7_plus_count: int,
    late_stage_stale_count: int,
    top_risk_owner: str,
    top_risk_owner_count: int,
    top_source: str,
    pipeline_value: float,
    pipeline_target: float,
) -> str:
    if total_active_leads == 0:
        return "No active leads are currently mirrored into the executive summary."

    summary = (
        f"{total_active_leads} active leads are currently tracked. "
        f"{overdue_count} are overdue and {review_count} need review. "
    )
    if pipeline_value > 0:
        progress_percent = int(round((pipeline_value / pipeline_target) * 100)) if pipeline_target > 0 else 0
        summary += f"Parseable pipeline value is ${pipeline_value:,.0f}, or {progress_percent}% of the $100,000 target. "
    if top_risk_owner_count > 0:
        summary += f"{top_risk_owner} carries the largest risk queue with {top_risk_owner_count} overdue leads. "
    if late_stage_stale_count > 0:
        summary += f"{late_stage_stale_count} late-stage leads are currently stale. "
    if untouched_7_plus_count > 0:
        summary += f"{untouched_7_plus_count} leads have gone untouched for 7+ days. "
    if top_source:
        summary += f"The largest current source bucket is {top_source}."
    return summary.strip()


def build_dashboard_data(
    *,
    settings: Settings,
    session: Session,
    lead_builder_status: dict[str, object],
    clickup_client: object | None = None,
    as_of_date: date | None = None,
    max_items_per_owner: int = 8,
) -> DashboardData:
    effective_date = as_of_date or date.today()
    reminder_service = ReminderService(settings, session)
    comment_cache: dict[str, list[dict[str, object]]] = {}

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
        comments = _get_task_comments(clickup_client, lead.clickup_task_id, comment_cache)
        evaluation = reminder_service.evaluate_lead(lead, as_of_date=effective_date, comments=comments)
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

    deck_generator_missing = [
        env_name
        for env_name, attr_name in (
            ("GOOGLE_SHEETS_SPREADSHEET_ID", "google_sheets_spreadsheet_id"),
            ("GOOGLE_SHEETS_SALES_RANGE", "google_sheets_sales_range"),
            ("GOOGLE_SERVICE_ACCOUNT_JSON", "google_service_account_json"),
            ("CANVA_CLIENT_ID", "canva_client_id"),
            ("CANVA_CLIENT_SECRET", "canva_client_secret"),
            ("CANVA_REDIRECT_URI", "canva_redirect_uri"),
            ("CANVA_BRAND_TEMPLATE_ID", "canva_brand_template_id"),
            ("CANVA_TOKEN_SECRET", "canva_token_secret"),
        )
        if not getattr(settings, attr_name, "")
    ]
    deck_connection = session.execute(
        select(CanvaConnection)
        .order_by(CanvaConnection.updated_at.desc(), CanvaConnection.id.desc())
        .limit(1)
    ).scalar_one_or_none()
    deck_runs = list(
        session.execute(
            select(AutomationRun)
            .where(AutomationRun.run_type == "deck_generation")
            .order_by(AutomationRun.started_at.desc())
            .limit(5)
        ).scalars()
    )
    recent_deck_runs = [
        {
            "id": run.id,
            "status": dict(run.summary_json or {}).get("status") or run.status,
            "message": dict(run.summary_json or {}).get("message", ""),
            "design_id": dict(run.summary_json or {}).get("design_id", ""),
            "design_title": dict(run.summary_json or {}).get("design_title", ""),
            "edit_url": dict(run.summary_json or {}).get("edit_url", ""),
            "view_url": dict(run.summary_json or {}).get("view_url", ""),
            "warnings": list(dict(run.summary_json or {}).get("warnings", []) or []),
            "started_at": run.started_at.isoformat() if run.started_at else "",
            "completed_at": run.completed_at.isoformat() if run.completed_at else "",
        }
        for run in deck_runs
    ]
    deck_capabilities = dict(deck_connection.capabilities_json) if deck_connection else {}

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
        deck_generator_ready=not deck_generator_missing,
        deck_generator_missing=deck_generator_missing,
        deck_canva_connected=deck_connection is not None,
        deck_canva_display_name=(deck_connection.display_name if deck_connection else ""),
        deck_canva_capabilities={
            "autofill": bool(deck_capabilities.get("autofill")),
            "brand_template": bool(deck_capabilities.get("brand_template")),
        },
        deck_google_source=str(getattr(settings, "google_sheets_sales_range", "")),
        deck_template_id=str(getattr(settings, "canva_brand_template_id", "")),
        recent_deck_runs=recent_deck_runs,
    )


def build_executive_data(
    *,
    settings: Settings,
    session: Session,
    clickup_client: object | None = None,
    as_of_date: date | None = None,
    risk_limit: int = 15,
) -> ExecutiveData:
    effective_date = as_of_date or date.today()
    reminder_service = ReminderService(settings, session)
    active_statuses = set(settings.active_statuses)
    comment_cache: dict[str, list[dict[str, object]]] = {}

    late_stage_status_keys = {
        "working qualified",
        "working needs offer",
        "working offered",
        "working negotiating",
    }
    late_stage_stale_status_keys = {
        "working needs offer",
        "working offered",
        "working negotiating",
    }
    new_or_contacted_status_keys = {
        "new lead",
        "contacted cold",
        "contacted warm",
    }

    leads_query: Select[tuple[LeadMirror]] = (
        select(LeadMirror)
        .where(LeadMirror.list_id == settings.clickup_list_id)
        .order_by(LeadMirror.updated_at.desc(), LeadMirror.last_sync_at.desc())
    )
    leads = list(session.execute(leads_query).scalars())
    latest_sync_at = max((lead.last_sync_at for lead in leads if lead.last_sync_at), default=None)

    latest_run = session.execute(
        select(AutomationRun)
        .where(AutomationRun.run_type == "stale_lead_scan")
        .order_by(AutomationRun.started_at.desc())
        .limit(1)
    ).scalar_one_or_none()
    latest_run_summary = latest_run.summary_json if latest_run else {}

    mailbox_start = datetime.combine(effective_date - timedelta(days=7), datetime.min.time(), tzinfo=timezone.utc)
    mailbox_signals = list(
        session.execute(
            select(MailboxSignal)
            .where(MailboxSignal.received_at >= mailbox_start)
            .order_by(MailboxSignal.received_at.desc())
        ).scalars()
    )
    inbox_reply_events = list(
        session.execute(
            select(CommunicationEvent)
            .where(
                CommunicationEvent.event_type == "inbound_reply_received",
                CommunicationEvent.occurred_at >= mailbox_start,
            )
        ).scalars()
    )

    lead_records: list[ExecutiveLeadRecord] = []
    status_counter: Counter[str] = Counter()
    source_counter: Counter[str] = Counter()
    age_counter: Counter[str] = Counter({"0-2 days": 0, "3-7 days": 0, "8-14 days": 0, "15+ days": 0, "No touch": 0})
    late_stage_counter: Counter[str] = Counter()
    owner_mailbox_counter: Counter[str] = Counter()
    owner_reply_counter: Counter[str] = Counter()
    owner_stats: dict[str, dict[str, object]] = defaultdict(
        lambda: {
            "active": 0,
            "overdue": 0,
            "review": 0,
            "due": 0,
            "touch_ages": [],
            "late_stage": 0,
            "late_stage_stale": 0,
            "mailbox": 0,
            "value_total": 0.0,
            "has_value": False,
        }
    )
    lead_owner_map: dict[str, str] = {}
    overdue_by_owner: Counter[str] = Counter()

    for signal in mailbox_signals:
        owner = signal.owner_name or "Assigned AE"
        owner_mailbox_counter[owner] += 1

    for lead in leads:
        status = (lead.status or "").strip()
        status_key = " ".join(status.lower().split())
        if not status or status_key not in active_statuses:
            continue

        comments = _get_task_comments(clickup_client, lead.clickup_task_id, comment_cache)
        evaluation = reminder_service.evaluate_lead(lead, as_of_date=effective_date, comments=comments)
        if evaluation is None:
            continue

        digest_item = reminder_service.build_digest_item(evaluation)
        owner_name = lead.assignee_name or "Assigned AE"
        lead_owner_map[lead.clickup_task_id] = owner_name
        source_name = _display_source_name(lead.source)
        value_numeric = _safe_numeric_value(lead.value)
        days_since_touch = _days_since_touch(lead, effective_date)
        late_stage = status_key in late_stage_status_keys
        late_stage_stale = status_key in late_stage_stale_status_keys and digest_item.urgency in {"overdue", "needs_immediate_review"}
        missing_next_action = not (lead.recommended_next_action or "").strip()
        missing_meeting_outcome = late_stage and not (lead.last_meeting_outcome or "").strip()
        untouched_new_or_contacted = status_key in new_or_contacted_status_keys and evaluation.last_meaningful_touch_at is None

        lead_records.append(
            ExecutiveLeadRecord(
                owner_name=owner_name,
                task_name=lead.task_name,
                status=status,
                source=source_name,
                urgency=digest_item.urgency,
                value_label=lead.value or "",
                value_numeric=value_numeric,
                days_since_touch=days_since_touch,
                late_stage=late_stage,
                late_stage_stale=late_stage_stale,
                missing_next_action=missing_next_action,
                missing_meeting_outcome=missing_meeting_outcome,
                untouched_new_or_contacted=untouched_new_or_contacted,
                next_step=lead.recommended_next_action or evaluation.assessment.recommended_next_action,
                link_url=lead.task_url,
            )
        )

        status_counter[status] += 1
        source_counter[source_name] += 1
        if status_key in late_stage_status_keys:
            late_stage_counter[status] += 1
        if days_since_touch is None:
            age_counter["No touch"] += 1
        elif days_since_touch <= 2:
            age_counter["0-2 days"] += 1
        elif days_since_touch <= 7:
            age_counter["3-7 days"] += 1
        elif days_since_touch <= 14:
            age_counter["8-14 days"] += 1
        else:
            age_counter["15+ days"] += 1

        stats = owner_stats[owner_name]
        stats["active"] = int(stats["active"]) + 1
        if digest_item.urgency == "overdue":
            stats["overdue"] = int(stats["overdue"]) + 1
            overdue_by_owner[owner_name] += 1
        elif digest_item.urgency == "needs_immediate_review":
            stats["review"] = int(stats["review"]) + 1
        elif digest_item.urgency == "follow_up_due":
            stats["due"] = int(stats["due"]) + 1
        if days_since_touch is not None:
            touch_ages = list(stats["touch_ages"])
            touch_ages.append(days_since_touch)
            stats["touch_ages"] = touch_ages
        if late_stage:
            stats["late_stage"] = int(stats["late_stage"]) + 1
        if late_stage_stale:
            stats["late_stage_stale"] = int(stats["late_stage_stale"]) + 1
        if value_numeric is not None:
            stats["value_total"] = float(stats["value_total"]) + value_numeric
            stats["has_value"] = True

    for owner_name, count in owner_mailbox_counter.items():
        owner_stats[owner_name]["mailbox"] = count

    for event in inbox_reply_events:
        owner_name = lead_owner_map.get(event.clickup_task_id, "Assigned AE")
        owner_reply_counter[owner_name] += 1

    owner_scorecards = [
        ExecutiveOwnerScorecard(
            owner_name=owner_name,
            active_leads=int(stats["active"]),
            overdue_count=int(stats["overdue"]),
            review_count=int(stats["review"]),
            due_count=int(stats["due"]),
            avg_days_since_touch=(
                round(sum(stats["touch_ages"]) / len(stats["touch_ages"]), 1)
                if stats["touch_ages"]
                else None
            ),
            late_stage_leads=int(stats["late_stage"]),
            late_stage_stale_leads=int(stats["late_stage_stale"]),
            mailbox_signals_pending=int(stats["mailbox"]),
            value_total=(round(float(stats["value_total"]), 2) if stats["has_value"] else None),
        )
        for owner_name, stats in owner_stats.items()
    ]
    owner_scorecards.sort(
        key=lambda item: (
            -item.overdue_count,
            -item.review_count,
            -item.late_stage_stale_leads,
            -item.active_leads,
            item.owner_name.lower(),
        )
    )

    risk_leads = sorted(
        [
            ExecutiveRiskLead(
                owner_name=item.owner_name,
                task_name=item.task_name,
                status=item.status,
                source=item.source,
                urgency=item.urgency,
                value_label=item.value_label,
                value_numeric=item.value_numeric,
                days_since_touch=item.days_since_touch,
                next_step=item.next_step,
                link_url=item.link_url,
            )
            for item in lead_records
        ],
        key=lambda item: (
            STALE_URGENCY_ORDER.index(item.urgency) if item.urgency in STALE_URGENCY_ORDER else len(STALE_URGENCY_ORDER),
            0 if "working" in item.status.lower() else 1,
            -(item.days_since_touch or 0),
            -(item.value_numeric or 0.0),
            item.task_name.lower(),
        ),
    )[:risk_limit]

    status_distribution = _distribution_from_counter(status_counter)
    source_distribution = _distribution_from_counter(source_counter)
    aging_buckets = _distribution_from_counter(
        age_counter,
        preferred_order=["0-2 days", "3-7 days", "8-14 days", "15+ days", "No touch"],
    )
    late_stage_distribution = _distribution_from_counter(
        late_stage_counter,
        preferred_order=[
            "WORKING QUALIFIED",
            "WORKING NEEDS OFFER",
            "WORKING OFFERED",
            "WORKING NEGOTIATING",
            "working qualified",
            "working needs offer",
            "working offered",
            "working negotiating",
        ],
    )

    total_active_leads = len(lead_records)
    overdue_count = sum(1 for item in lead_records if item.urgency == "overdue")
    review_count = sum(1 for item in lead_records if item.urgency == "needs_immediate_review")
    due_count = sum(1 for item in lead_records if item.urgency == "follow_up_due")
    untouched_7_plus_count = sum(
        1 for item in lead_records if item.days_since_touch is not None and item.days_since_touch >= 7
    )
    late_stage_stale_count = sum(1 for item in lead_records if item.late_stage_stale)
    pipeline_value = round(sum(item.value_numeric or 0.0 for item in lead_records), 2)
    pipeline_target = 100000.0
    top_source = source_distribution[0].label if source_distribution else ""
    top_risk_owner, top_risk_owner_count = overdue_by_owner.most_common(1)[0] if overdue_by_owner else ("", 0)

    hygiene_counts = {
        "missing_next_action": sum(1 for item in lead_records if item.missing_next_action),
        "missing_meeting_outcome": sum(1 for item in lead_records if item.missing_meeting_outcome),
        "untouched_new_or_contacted": sum(1 for item in lead_records if item.untouched_new_or_contacted),
        "inbound_replies_last_7_days": sum(owner_reply_counter.values()),
        "mailbox_signals_last_7_days": sum(owner_mailbox_counter.values()),
    }

    summary_text = _build_executive_summary_text(
        total_active_leads=total_active_leads,
        overdue_count=overdue_count,
        review_count=review_count,
        untouched_7_plus_count=untouched_7_plus_count,
        late_stage_stale_count=late_stage_stale_count,
        top_risk_owner=top_risk_owner,
        top_risk_owner_count=top_risk_owner_count,
        top_source=top_source,
        pipeline_value=pipeline_value,
        pipeline_target=pipeline_target,
    )

    return ExecutiveData(
        as_of_date=effective_date,
        latest_sync_at=latest_sync_at,
        latest_run_summary=latest_run_summary,
        summary_text=summary_text,
        kpis={
            "active_leads": total_active_leads,
            "overdue": overdue_count,
            "review": review_count,
            "due": due_count,
            "untouched_7_plus": untouched_7_plus_count,
            "late_stage_stale": late_stage_stale_count,
            "pipeline_value": int(round(pipeline_value)),
            "pipeline_target": int(round(pipeline_target)),
            "pipeline_gap": int(round(max(pipeline_target - pipeline_value, 0))),
        },
        owner_scorecards=owner_scorecards,
        status_distribution=status_distribution,
        source_distribution=source_distribution,
        aging_buckets=aging_buckets,
        late_stage_distribution=late_stage_distribution,
        risk_leads=risk_leads,
        inbound_replies_by_owner=[
            ExecutiveOwnerMetric(owner_name=owner_name, count=count)
            for owner_name, count in sorted(owner_reply_counter.items(), key=lambda item: (-item[1], item[0].lower()))
        ],
        mailbox_signals_by_owner=[
            ExecutiveOwnerMetric(owner_name=owner_name, count=count)
            for owner_name, count in sorted(owner_mailbox_counter.items(), key=lambda item: (-item[1], item[0].lower()))
        ],
        hygiene_counts=hygiene_counts,
        filters={
            "owners": sorted({item.owner_name for item in lead_records}),
            "statuses": sorted({item.status for item in lead_records}),
            "sources": sorted({item.source for item in lead_records}),
            "urgencies": ["overdue", "needs_immediate_review", "follow_up_due"],
        },
        lead_records=lead_records,
    )


def _get_task_comments(
    clickup_client: object | None,
    task_id: str | None,
    cache: dict[str, list[dict[str, object]]],
) -> list[dict[str, object]]:
    if clickup_client is None or not task_id:
        return []
    if task_id in cache:
        return cache[task_id]
    try:
        comments = list(clickup_client.get_task_comments(task_id))
    except Exception:
        comments = []
    cache[task_id] = comments
    return comments


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

    def info_hint(text: str) -> str:
        escaped = html.escape(text, quote=True)
        return (
            '<span class="info-hint" tabindex="0">'
            '<span class="info-dot" aria-hidden="true">?</span>'
            f'<span class="tooltip-bubble" role="tooltip">{escaped}</span>'
            "</span>"
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

    deck_ready_notice = (
        '<div class="notice warning">Deck generator is missing env vars: '
        + html.escape(", ".join(data.deck_generator_missing))
        + "</div>"
        if not data.deck_generator_ready
        else '<div class="notice success">Deck generator is configured. Connect Canva once, then upload a competitor CSV to generate a fresh deck copy.</div>'
    )
    canva_connection_label = data.deck_canva_display_name or "No Canva account connected yet"
    deck_capability_bits = "".join(
        f'<span>{html.escape(label)}</span>'
        for label in (
            f"Autofill {'ready' if data.deck_canva_capabilities.get('autofill') else 'missing'}",
            f"Brand template {'ready' if data.deck_canva_capabilities.get('brand_template') else 'missing'}",
        )
    )
    recent_deck_runs_html = "".join(
        f"""
        <article class="deck-run-item">
          <div>
            <strong>{html.escape(str(run.get("design_title") or run.get("design_id") or f"Run {run.get('id', '')}"))}</strong>
            <p>{html.escape(str(run.get("message") or run.get("status") or ""))}</p>
          </div>
          <div class="deck-run-links">
            {f'<a href="{html.escape(str(run.get("edit_url") or ""))}" target="_blank" rel="noreferrer">Edit</a>' if run.get("edit_url") else ""}
            {f'<a href="{html.escape(str(run.get("view_url") or ""))}" target="_blank" rel="noreferrer">View</a>' if run.get("view_url") else ""}
          </div>
        </article>
        """
        for run in data.recent_deck_runs
    )

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
      .top-actions {{
        display: flex;
        align-items: center;
        gap: 10px;
        flex-wrap: wrap;
      }}
      .toplink {{
        display: inline-flex;
        align-items: center;
        justify-content: center;
        min-width: 170px;
        padding: 14px 22px;
        border-radius: 999px;
        background: rgba(255, 255, 255, 0.12);
        color: var(--white);
        font-family: "Montserrat", sans-serif;
        font-weight: 700;
        font-size: 16px;
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
        grid-template-columns: repeat(3, minmax(0, 1fr));
        gap: 18px;
        margin-bottom: 24px;
      }}
      .secondary-tools {{
        display: grid;
        grid-template-columns: minmax(0, 1fr);
        gap: 18px;
        margin-bottom: 24px;
      }}
      .panel-card {{
        background: var(--white);
        border: 2px solid rgba(43, 54, 68, 0.10);
        border-radius: 18px;
        padding: 20px 22px;
      }}
      .card-title-line {{
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 12px;
        margin-bottom: 8px;
      }}
      .card-title-line h2,
      .card-title-line h3 {{
        margin-bottom: 0;
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
      .draft-form {{
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
      .draft-form label {{
        display: grid;
        gap: 8px;
        font-family: "Montserrat", sans-serif;
        font-weight: 700;
        font-size: 14px;
        text-transform: uppercase;
        letter-spacing: 0.04em;
      }}
      .lead-form input,
      .lead-form textarea,
      .draft-form input,
      .draft-form textarea {{
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
      .lead-form textarea,
      .draft-form textarea {{
        min-height: 180px;
        resize: vertical;
      }}
      .lead-form input[type="file"],
      .draft-form input[type="file"] {{
        padding: 14px 16px;
      }}
      .draft-panel {{
        margin-bottom: 24px;
      }}
      .draft-form .draft-body-field,
      .draft-form .draft-submit,
      .draft-form .draft-help {{
        grid-column: 1 / -1;
      }}
      .checkbox-label {{
        display: inline-flex;
        align-items: center;
        gap: 10px;
        font-family: "Roboto", sans-serif;
        font-weight: 300;
        font-size: 15px;
        text-transform: none;
        letter-spacing: 0;
      }}
      .checkbox-label input {{
        width: auto;
        margin: 0;
        padding: 0;
      }}
      .draft-help {{
        color: var(--alt-dark-blue);
        font-family: "Roboto", sans-serif;
        font-weight: 300;
        font-size: 15px;
        line-height: 1.45;
      }}
      .draft-results {{
        margin-top: 14px;
        display: grid;
        gap: 12px;
      }}
      .draft-results .result-block {{
        background: rgba(249, 247, 243, 0.9);
        border: 1px solid rgba(43, 54, 68, 0.10);
        border-radius: 14px;
        padding: 14px 16px;
      }}
      .draft-results .result-block strong {{
        display: block;
        margin-bottom: 8px;
        font-family: "Montserrat", sans-serif;
        font-size: 15px;
        color: var(--dark-blue);
      }}
      .draft-results ul {{
        margin: 0;
        padding-left: 20px;
      }}
      .draft-results li {{
        margin-bottom: 6px;
        font-size: 14px;
        line-height: 1.45;
      }}
      .deck-capabilities {{
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
        margin-bottom: 14px;
      }}
      .deck-capabilities span,
      .deck-run-links a {{
        display: inline-flex;
        align-items: center;
        border-radius: 999px;
        padding: 8px 12px;
        background: rgba(43, 54, 68, 0.08);
        color: var(--dark-blue);
        font-family: "Montserrat", sans-serif;
        font-weight: 700;
        font-size: 12px;
        text-decoration: none;
      }}
      .panel-stack {{
        display: grid;
        gap: 14px;
      }}
      .deck-run-list {{
        display: grid;
        gap: 10px;
        margin-top: 14px;
      }}
      .deck-run-item {{
        display: flex;
        justify-content: space-between;
        gap: 14px;
        align-items: flex-start;
        padding: 12px 0;
        border-top: 1px solid rgba(43, 54, 68, 0.08);
      }}
      .deck-run-item:first-child {{
        border-top: 0;
        padding-top: 0;
      }}
      .deck-run-item strong {{
        display: block;
        margin-bottom: 6px;
        font-family: "Montserrat", sans-serif;
        font-size: 16px;
        color: var(--dark-blue);
      }}
      .deck-run-item p {{
        margin: 0;
        font-size: 14px;
      }}
      .deck-run-links {{
        display: flex;
        gap: 8px;
        flex-wrap: wrap;
        justify-content: flex-end;
      }}
      .button-link {{
        display: inline-flex;
        align-items: center;
        justify-content: center;
        width: auto;
        border: 0;
        border-radius: 999px;
        padding: 14px 24px;
        background: var(--dark-blue);
        color: var(--white);
        font-family: "Montserrat", sans-serif;
        font-weight: 700;
        font-size: 16px;
        text-decoration: none;
        cursor: pointer;
      }}
      .lead-form .lead-submit {{
        grid-column: 1 / -1;
        display: flex;
        align-items: end;
        gap: 12px;
        flex-wrap: wrap;
      }}
      .info-hint {{
        position: relative;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        flex-shrink: 0;
        outline: none;
      }}
      .info-dot {{
        width: 24px;
        height: 24px;
        border-radius: 999px;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        background: rgba(133, 187, 218, 0.18);
        border: 1px solid rgba(43, 54, 68, 0.12);
        color: var(--dark-blue);
        font-family: "Montserrat", sans-serif;
        font-size: 13px;
        font-weight: 800;
        line-height: 1;
        cursor: help;
      }}
      .tooltip-bubble {{
        position: absolute;
        top: calc(100% + 12px);
        right: 0;
        z-index: 12;
        width: min(320px, 80vw);
        padding: 12px 14px;
        border-radius: 14px;
        background: var(--dark-blue);
        color: var(--white);
        font-size: 14px;
        line-height: 1.45;
        box-shadow: 0 16px 30px rgba(43, 54, 68, 0.18);
        opacity: 0;
        pointer-events: none;
        transform: translateY(-6px);
        transition: opacity 0.16s ease, transform 0.16s ease;
      }}
      .tooltip-bubble::after {{
        content: "";
        position: absolute;
        top: -7px;
        right: 12px;
        width: 14px;
        height: 14px;
        background: var(--dark-blue);
        transform: rotate(45deg);
      }}
      .info-hint:hover .tooltip-bubble,
      .info-hint:focus .tooltip-bubble,
      .info-hint:focus-within .tooltip-bubble {{
        opacity: 1;
        pointer-events: auto;
        transform: translateY(0);
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
        .secondary-tools,
        .layout,
        .metrics,
        .lead-form,
        .draft-form {{
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
        .lead-form label,
        .draft-form label {{
          font-size: 13px;
        }}
        .page-copy,
        .action-item p,
        .owner-card p,
        .meta-card p,
        .panel-card p,
        .lead-form input,
        .lead-form textarea,
        .draft-form input,
        .draft-form textarea,
        .deck-run-item p {{
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
        <div class="top-actions">
          <a class="toplink" href="/admin/executive">EXECUTIVE SUMMARY</a>
          <a class="topcta" href="/admin/logout">LOG OUT</a>
        </div>
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
          <div class="card-title-line">
            <h3>Sync data</h3>
            {info_hint("Refreshes the ClickUp mirror and recalculates the stale-priority queue. Run this when you want the board to reflect the latest task state before reviewing owner work.")}
          </div>
          <p>Refresh the ClickUp mirror and recompute stale priorities before reviewing the board.</p>
          <button id="sync-dashboard-button" type="button">SYNC DATA</button>
          <div class="status-line" id="sync-status">Ready.</div>
        </div>
        <div class="panel-card" id="lead-pull-panel">
          <div class="card-title-line">
            <h3>Run lead pull</h3>
            {info_hint("Runs the outbound lead pipeline from this dashboard. It sources fresh companies, finds matched contacts, adds accepted leads into Instantly, and then returns the CSV download for review.")}
          </div>
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
          <div class="card-title-line">
            <h2>Ops snapshot</h2>
            {info_hint("Quick readout of the latest sync and stale-scan activity so you can see whether the board is fresh and whether recent automation runs completed cleanly.")}
          </div>
          <div class="snapshot-rows">
            {''.join(snapshot_rows)}
          </div>
        </section>
      </section>

      <section class="secondary-tools">
        <div class="panel-card" id="deck-generator-panel">
          <div class="card-title-line">
            <h3>Generate sales deck</h3>
            {info_hint("Pulls sales metrics from Google Sheets, combines them with your uploaded competitor CSV, and generates a fresh Canva deck from the configured brand template.")}
          </div>
          <p>Pull sales metrics from Google Sheets, upload the competitor CSV, and generate a fresh Canva deck copy from the configured brand template.</p>
          {deck_ready_notice}
          <div class="panel-stack">
            <div class="snapshot-rows">
              {_summary_row("Google sheet range", data.deck_google_source or "Not configured")}
              {_summary_row("Brand template", data.deck_template_id or "Not configured")}
              {_summary_row("Canva connection", canva_connection_label)}
            </div>
            <div class="deck-capabilities">
              {deck_capability_bits}
            </div>
          </div>
          <form class="lead-form" id="deck-generator-form">
            <label>
              Competitor CSV
              <input type="file" name="competitor_csv" accept=".csv,text/csv" required />
            </label>
            <label>
              Report date
              <input type="date" name="report_date" value="{html.escape(today_value)}" />
            </label>
            <label>
              Reporting period
              <input type="text" name="reporting_period" placeholder="Q1 2026" />
            </label>
            <label>
              Deck title
              <input type="text" name="run_label" placeholder="Sales Deck | Q1 2026" />
            </label>
            <div class="lead-submit">
              <button type="submit">GENERATE DECK</button>
              <a class="button-link" id="connect-canva-button" href="/admin/api/canva/connect">CONNECT CANVA</a>
            </div>
          </form>
          <div class="status-line" id="deck-status">Deck status: Ready.</div>
          <div class="deck-run-list" id="deck-run-list">
            {recent_deck_runs_html or '<p class="empty">No deck generation runs yet.</p>'}
          </div>
        </div>
      </section>

      <section class="panel-card draft-panel" id="gmail-drafts-panel">
        <div class="card-title-line">
          <h3>Create Gmail drafts</h3>
          {info_hint("Uploads a CSV and creates Gmail drafts in bulk without sending anything. Preview mode lets you inspect the merged rows first so you can catch template or data issues before draft creation.")}
        </div>
        <p>Upload a simple CSV and create Gmail drafts in bulk without sending anything. Required column: <strong>email</strong>. Optional columns: <strong>first_name</strong>, <strong>last_name</strong>, <strong>company</strong>, <strong>subject</strong>, <strong>body</strong>, plus any custom fields you want to reference.</p>
        <form class="draft-form" id="gmail-drafts-form" enctype="multipart/form-data">
          <label>
            Contacts CSV
            <input type="file" name="contacts_csv" accept=".csv,text/csv" required />
          </label>
          <label>
            Sales objective
            <input type="text" name="sales_objective" placeholder="book intro calls with Amazon operators" />
          </label>
          <label>
            Subject template
            <input type="text" name="subject_template" placeholder="Idea for {{company}}" />
          </label>
          <label>
            Preview mode
            <span class="checkbox-label"><input type="checkbox" name="dry_run" value="true" checked /> Preview only before creating drafts</span>
          </label>
          <label class="draft-body-field">
            Body template
            <textarea name="body_template" placeholder="Hi {{first_name}},&#10;&#10;Reaching out because {{objective}}.&#10;&#10;Would you be open to a quick conversation next week?&#10;&#10;Best,&#10;David"></textarea>
          </label>
          <div class="draft-help">
            Use placeholders like <strong>{'{{first_name}}'}</strong>, <strong>{'{{company}}'}</strong>, <strong>{'{{objective}}'}</strong>, or any normalized CSV header. If your CSV already includes <strong>subject</strong> or <strong>body</strong> columns, you can leave the template fields blank.
          </div>
          <div class="lead-submit draft-submit">
            <button type="submit">PREVIEW / CREATE DRAFTS</button>
            <a class="button-link" href="https://mail.google.com/mail/u/0/#drafts" target="_blank" rel="noreferrer">OPEN GMAIL DRAFTS</a>
          </div>
        </form>
        <div class="status-line" id="drafts-status">Drafts: Ready.</div>
        <div class="draft-results" id="drafts-results"></div>
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
      const deckForm = document.getElementById("deck-generator-form");
      const deckStatus = document.getElementById("deck-status");
      const draftsForm = document.getElementById("gmail-drafts-form");
      const draftsStatus = document.getElementById("drafts-status");
      const draftsResults = document.getElementById("drafts-results");
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
      deckForm?.addEventListener("submit", async (event) => {{
        event.preventDefault();
        deckStatus.innerHTML = "Generating Canva deck. This can take a minute...";
        const formData = new FormData(deckForm);
        try {{
          const response = await fetch("/admin/api/generate-deck", {{
            method: "POST",
            body: formData,
          }});
          const payload = await response.json().catch(() => ({{ detail: "Deck generation failed." }}));
          if (!response.ok) {{
            deckStatus.textContent = payload.detail || payload.message || "Deck generation failed.";
            return;
          }}
          const details = payload.details || {{}};
          const links = [];
          if (details.edit_url) {{
            links.push(`<a href="${{details.edit_url}}" target="_blank" rel="noreferrer">Open edit link</a>`);
          }}
          if (details.view_url) {{
            links.push(`<a href="${{details.view_url}}" target="_blank" rel="noreferrer">Open view link</a>`);
          }}
          deckStatus.innerHTML = `Deck generated. ${{links.join(" | ")}}`;
          window.setTimeout(() => window.location.reload(), 1200);
        }} catch (error) {{
          deckStatus.textContent = "Deck generation failed before a response came back.";
        }}
      }});
      draftsForm?.addEventListener("submit", async (event) => {{
        event.preventDefault();
        draftsStatus.textContent = "Preparing Gmail drafts...";
        draftsResults.innerHTML = "";
        const formData = new FormData(draftsForm);
        try {{
          const response = await fetch("/admin/api/create-gmail-drafts", {{
            method: "POST",
            body: formData,
          }});
          const payload = await response.json().catch(() => ({{ detail: "Draft creation failed." }}));
          if (!response.ok) {{
            draftsStatus.textContent = payload.detail || payload.message || "Draft creation failed.";
            return;
          }}
          const details = payload.details || {{}};
          const summary = `${{payload.message || "Draft workflow completed."}} Prepared ${{details.prepared || 0}}, created ${{details.created || 0}}, failed ${{details.failed || 0}}.`;
          draftsStatus.innerHTML = details.drafts_url
            ? `${{summary}} <a href="${{details.drafts_url}}" target="_blank" rel="noreferrer">Open Gmail drafts</a>`
            : summary;

          const blocks = [];
          if (Array.isArray(details.available_placeholders) && details.available_placeholders.length) {{
            blocks.push(`
              <div class="result-block">
                <strong>Available placeholders</strong>
                <div>${{details.available_placeholders.map((item) => `<span class="source">${{item}}</span>`).join(" ")}}</div>
              </div>
            `);
          }}
          if (Array.isArray(details.previews) && details.previews.length) {{
            blocks.push(`
              <div class="result-block">
                <strong>${{details.dry_run ? "Preview rows" : "Created drafts"}}</strong>
                <ul>
                  ${{details.previews.map((item) => `<li><strong>${{item.email}}</strong> - ${{item.subject}}</li>`).join("")}}
                </ul>
              </div>
            `);
          }}
          if (Array.isArray(details.failed_rows) && details.failed_rows.length) {{
            const failureItems = details.failed_rows.map((item) => {{
              const emailPart = item.email ? ` (${{item.email}})` : "";
              return `<li>Row ${{item.row_number}}${{emailPart}}: ${{item.error}}</li>`;
            }}).join("");
            blocks.push(`
              <div class="result-block">
                <strong>Rows that need fixes</strong>
                <ul>${{failureItems}}</ul>
              </div>
            `);
          }}
          draftsResults.innerHTML = blocks.join("");
        }} catch (error) {{
          draftsStatus.textContent = "Draft creation failed before a response came back.";
        }}
      }});
      applyQueueFilters();
    </script>
  </body>
</html>"""


def render_executive_page(data: ExecutiveData) -> str:
    payload_json = json.dumps(executive_data_to_dict(data)).replace("</", "<\\/")
    latest_sync = format_date_label(data.latest_sync_at) if data.latest_sync_at else "not synced yet"
    def info_hint(text: str) -> str:
        escaped = html.escape(text, quote=True)
        return (
            '<span class="info-hint" tabindex="0">'
            '<span class="info-dot" aria-hidden="true">i</span>'
            f'<span class="tooltip-bubble" role="tooltip">{escaped}</span>'
            "</span>"
        )
    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>agent | Executive Summary</title>
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
        --border: rgba(43, 54, 68, 0.10);
        --shadow: rgba(43, 54, 68, 0.10);
      }}
      * {{ box-sizing: border-box; }}
      body {{
        margin: 0;
        background: var(--light-brown);
        color: var(--text);
        font-family: "Roboto", sans-serif;
      }}
      a {{ color: var(--dark-blue); text-decoration: none; }}
      .topbar {{
        background: var(--alt-dark-blue);
        color: var(--white);
        padding: 18px 32px;
      }}
      .topbar-inner {{
        max-width: 1280px;
        margin: 0 auto;
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 18px;
      }}
      .brandmark {{
        font-family: "Montserrat", sans-serif;
        font-weight: 800;
        font-size: 42px;
        line-height: 1;
        letter-spacing: -0.04em;
      }}
      .brandmark .dot {{
        color: var(--light-blue);
      }}
      .top-actions {{
        display: flex;
        gap: 10px;
        flex-wrap: wrap;
      }}
      .top-link {{
        display: inline-flex;
        align-items: center;
        justify-content: center;
        min-width: 150px;
        padding: 14px 22px;
        border-radius: 999px;
        background: rgba(255,255,255,0.12);
        color: var(--white);
        font-family: "Montserrat", sans-serif;
        font-weight: 700;
        font-size: 16px;
      }}
      .top-link.primary {{
        background: var(--light-blue);
      }}
      .shell {{
        max-width: 1280px;
        margin: 0 auto;
        padding: 32px 20px 72px;
      }}
      .workspace {{
        background: var(--white);
        border: 1px solid var(--border);
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
        border-bottom: 1px solid var(--border);
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
        letter-spacing: 0.06em;
        text-transform: uppercase;
        margin-bottom: 18px;
      }}
      .page-title {{
        margin: 0;
        font-family: "Montserrat", sans-serif;
        font-weight: 800;
        font-size: 60px;
        line-height: 0.94;
        letter-spacing: -0.02em;
        color: var(--dark-blue);
      }}
      .highlight {{
        color: var(--light-blue);
      }}
      .page-copy {{
        font-size: 18px;
        line-height: 1.5;
        color: var(--dark-blue);
      }}
      .summary-card {{
        background: rgba(133, 187, 218, 0.10);
        border: 1px solid rgba(133, 187, 218, 0.25);
        border-radius: 18px;
        padding: 18px 20px;
        margin: 24px 0;
      }}
      .summary-card h2 {{
        margin: 0 0 8px;
        font-family: "Montserrat", sans-serif;
        font-size: 24px;
        letter-spacing: 0.01em;
      }}
      .heading-line {{
        display: inline-flex;
        align-items: center;
        gap: 8px;
      }}
      .info-hint {{
        position: relative;
        display: inline-flex;
        align-items: center;
      }}
      .info-dot {{
        display: inline-flex;
        align-items: center;
        justify-content: center;
        width: 18px;
        height: 18px;
        border-radius: 999px;
        background: rgba(133, 187, 218, 0.18);
        color: var(--alt-dark-blue);
        font-family: "Montserrat", sans-serif;
        font-size: 11px;
        font-weight: 700;
        line-height: 1;
        cursor: help;
      }}
      .tooltip-bubble {{
        position: absolute;
        left: 50%;
        bottom: calc(100% + 10px);
        transform: translateX(-50%) translateY(4px);
        width: min(260px, 60vw);
        padding: 10px 12px;
        border-radius: 12px;
        background: var(--alt-dark-blue);
        color: var(--white);
        font-family: "Roboto", sans-serif;
        font-size: 13px;
        line-height: 1.45;
        box-shadow: 0 10px 24px rgba(43, 54, 68, 0.18);
        opacity: 0;
        visibility: hidden;
        pointer-events: none;
        transition: opacity 120ms ease, transform 120ms ease, visibility 120ms ease;
        z-index: 20;
      }}
      .tooltip-bubble::after {{
        content: "";
        position: absolute;
        left: 50%;
        top: 100%;
        width: 10px;
        height: 10px;
        background: var(--alt-dark-blue);
        transform: translateX(-50%) rotate(45deg);
      }}
      .info-hint:hover .tooltip-bubble,
      .info-hint:focus .tooltip-bubble,
      .info-hint:focus-within .tooltip-bubble {{
        opacity: 1;
        visibility: visible;
        transform: translateX(-50%) translateY(0);
      }}
      .summary-card p {{
        margin: 0;
        font-size: 16px;
        line-height: 1.5;
      }}
      .filters {{
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: 12px;
        margin-bottom: 20px;
      }}
      .filter label {{
        display: grid;
        gap: 6px;
        font-family: "Montserrat", sans-serif;
        font-size: 13px;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.04em;
      }}
      .filter select {{
        width: 100%;
        padding: 12px 14px;
        border-radius: 12px;
        border: 2px solid rgba(43, 54, 68, 0.14);
        font-family: "Roboto", sans-serif;
        font-size: 15px;
        color: var(--dark-blue);
        background: var(--white);
      }}
      .kpis {{
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: 14px;
        margin-bottom: 24px;
      }}
      .kpi {{
        background: var(--white);
        border: 2px solid var(--border);
        border-radius: 18px;
        padding: 18px;
      }}
      .kpi span {{
        display: block;
        font-family: "Montserrat", sans-serif;
        font-size: 14px;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        margin-bottom: 12px;
      }}
      .kpi strong {{
        display: block;
        font-family: "Montserrat", sans-serif;
        font-size: 34px;
        line-height: 1;
        margin-bottom: 8px;
      }}
      .kpi small {{
        display: block;
        font-size: 14px;
        line-height: 1.4;
      }}
      .layout {{
        display: grid;
        grid-template-columns: minmax(0, 1.45fr) minmax(320px, 0.85fr);
        gap: 24px;
      }}
      .section {{
        background: var(--white);
        border: 2px solid var(--border);
        border-radius: 20px;
        padding: 20px;
        margin-bottom: 18px;
      }}
      .section h2 {{
        margin: 0 0 16px;
        font-family: "Montserrat", sans-serif;
        font-size: 28px;
        letter-spacing: 0.01em;
      }}
      table {{
        width: 100%;
        border-collapse: collapse;
      }}
      th, td {{
        text-align: left;
        padding: 10px 8px;
        border-bottom: 1px solid rgba(43, 54, 68, 0.08);
        font-size: 14px;
        vertical-align: top;
      }}
      th {{
        font-family: "Montserrat", sans-serif;
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.04em;
      }}
      .dist-list {{
        display: grid;
        gap: 10px;
      }}
      .dist-row {{
        display: grid;
        grid-template-columns: 160px 1fr auto;
        gap: 12px;
        align-items: center;
      }}
      .dist-row span {{
        font-size: 14px;
      }}
      .dist-bar {{
        height: 10px;
        border-radius: 999px;
        background: rgba(43, 54, 68, 0.08);
        overflow: hidden;
      }}
      .dist-bar-fill {{
        height: 100%;
        background: var(--light-blue);
      }}
      .pill {{
        display: inline-flex;
        align-items: center;
        border-radius: 999px;
        padding: 6px 10px;
        font-family: "Montserrat", sans-serif;
        font-size: 11px;
        font-weight: 700;
        background: rgba(133, 187, 218, 0.18);
      }}
      .risk-list {{
        display: grid;
        gap: 12px;
      }}
      .risk-item {{
        background: var(--light-brown);
        border: 1px solid rgba(43,54,68,0.08);
        border-left: 6px solid var(--light-blue);
        border-radius: 16px;
        padding: 14px 16px;
      }}
      .risk-item.overdue {{
        border-left-color: var(--dark-blue);
        background: rgba(51, 68, 92, 0.06);
      }}
      .risk-item.review {{
        border-left-color: var(--brown);
      }}
      .risk-meta {{
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
        margin-bottom: 8px;
      }}
      .risk-item h3 {{
        margin: 0 0 8px;
        font-family: "Montserrat", sans-serif;
        font-size: 20px;
      }}
      .risk-item p {{
        margin: 0 0 6px;
        font-size: 14px;
        line-height: 1.45;
      }}
      .snapshot {{
        display: grid;
        gap: 10px;
      }}
      .snapshot-row {{
        display: flex;
        justify-content: space-between;
        gap: 12px;
        padding: 8px 0;
        border-bottom: 1px solid rgba(43, 54, 68, 0.08);
      }}
      .snapshot-row:last-child {{
        border-bottom: 0;
      }}
      .snapshot-row span {{
        font-family: "Montserrat", sans-serif;
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.04em;
      }}
      .snapshot-row strong {{
        font-size: 14px;
      }}
      .empty {{
        font-size: 15px;
        line-height: 1.5;
      }}
      .footer-bar {{
        height: 18px;
        background: var(--alt-dark-blue);
        margin-top: 72px;
      }}
      @media (max-width: 1180px) {{
        .page-header, .layout, .filters, .kpis {{
          grid-template-columns: 1fr;
        }}
      }}
      @media (max-width: 860px) {{
        .topbar {{
          padding: 18px 20px;
        }}
        .shell {{
          padding: 24px 14px 56px;
        }}
        .workspace {{
          padding: 18px;
        }}
        .page-title {{
          font-size: 42px;
        }}
        .brandmark {{
          font-size: 36px;
        }}
      }}
    </style>
  </head>
  <body>
    <header class="topbar">
      <div class="topbar-inner">
        <div class="brandmark">agent<span class="dot">.</span></div>
        <div class="top-actions">
          <a class="top-link" href="/admin">FOLLOW-UP BOARD</a>
          <a class="top-link primary" href="/admin/logout">LOG OUT</a>
        </div>
      </div>
    </header>
    <div class="shell">
      <div class="workspace">
        <section class="page-header">
          <div>
            <div class="eyebrow">Executive summary</div>
            <h1 class="page-title">Pipeline <span class="highlight">Health</span>.</h1>
          </div>
          <div class="page-copy">
            Leadership view for AE performance, pipeline risk, and follow-up execution. Latest ClickUp sync: {html.escape(latest_sync)}.
          </div>
        </section>

        <section class="summary-card">
          <h2 class="heading-line">Executive summary {info_hint("Leadership readout generated from the current active ClickUp mirror, response signals, and stale-priority logic.")}</h2>
          <p id="summary-text">{html.escape(data.summary_text)}</p>
        </section>

        <section class="filters">
          <div class="filter">
            <label for="owner-filter">Owner</label>
            <select id="owner-filter"></select>
          </div>
          <div class="filter">
            <label for="status-filter">Status</label>
            <select id="status-filter"></select>
          </div>
          <div class="filter">
            <label for="source-filter">Source</label>
            <select id="source-filter"></select>
          </div>
          <div class="filter">
            <label for="urgency-filter">Urgency</label>
            <select id="urgency-filter"></select>
          </div>
        </section>

        <section class="kpis" id="kpi-grid"></section>

        <section class="layout">
          <div>
            <section class="section">
              <h2 class="heading-line">AE scorecard {info_hint("Owner-level view of active pipeline, follow-up risk, late-stage exposure, and parseable value totals.")}</h2>
              <div id="scorecard-table"></div>
            </section>
            <section class="section">
              <h2 class="heading-line">At-risk leads {info_hint("Prioritized by urgency first, then late stage, then time since last touch, then parseable value.")}</h2>
              <div class="risk-list" id="risk-list"></div>
            </section>
          </div>
          <div>
            <section class="section">
              <h2 class="heading-line">Leads by status {info_hint("Current pipeline mix across the active ClickUp sales statuses.")}</h2>
              <div class="dist-list" id="status-distribution"></div>
            </section>
            <section class="section">
              <h2 class="heading-line">Leads by source {info_hint("Distribution of the current active pipeline by lead source.")}</h2>
              <div class="dist-list" id="source-distribution"></div>
            </section>
            <section class="section">
              <h2 class="heading-line">Last-touch aging {info_hint("How long it has been since the most recent meaningful touch on each active lead.")}</h2>
              <div class="dist-list" id="aging-distribution"></div>
            </section>
            <section class="section">
              <h2 class="heading-line">Late-stage mix {info_hint("Pipeline concentration in qualified, needs offer, offered, and negotiating stages.")}</h2>
              <div class="dist-list" id="late-stage-distribution"></div>
            </section>
            <section class="section">
              <h2 class="heading-line">Response and hygiene {info_hint("Inbound reply activity, mailbox signals, and missing follow-up hygiene items that can slow deal movement.")}</h2>
              <div class="snapshot" id="hygiene-snapshot"></div>
              <div id="owner-response"></div>
            </section>
          </div>
        </section>
      </div>
    </div>
    <div class="footer-bar" aria-hidden="true"></div>
    <script>
      const executiveData = {payload_json};
      const ownerFilter = document.getElementById("owner-filter");
      const statusFilter = document.getElementById("status-filter");
      const sourceFilter = document.getElementById("source-filter");
      const urgencyFilter = document.getElementById("urgency-filter");

      function formatNumber(value) {{
        return new Intl.NumberFormat("en-US").format(Number(value || 0));
      }}

      function formatCurrency(value) {{
        if (value === null || value === undefined || Number.isNaN(Number(value))) return "n/a";
        return new Intl.NumberFormat("en-US", {{ style: "currency", currency: "USD", maximumFractionDigits: 0 }}).format(Number(value));
      }}

      function formatPercent(value) {{
        return `${{Math.round(Number(value || 0))}}%`;
      }}

      function formatSourceLabel(value) {{
        return value && String(value).trim() ? value : "Unknown";
      }}

      function renderInfoHint(text) {{
        return `<span class="info-hint" tabindex="0"><span class="info-dot" aria-hidden="true">i</span><span class="tooltip-bubble" role="tooltip">${{text}}</span></span>`;
      }}

      function initFilters() {{
        const filterDefs = [
          [ownerFilter, ["all", ...(executiveData.filters.owners || [])]],
          [statusFilter, ["all", ...(executiveData.filters.statuses || [])]],
          [sourceFilter, ["all", ...(executiveData.filters.sources || [])]],
          [urgencyFilter, ["all", ...(executiveData.filters.urgencies || [])]],
        ];
        filterDefs.forEach(([element, values]) => {{
          element.innerHTML = values
            .map((value) => `<option value="${{value}}">${{value === "all" ? "All" : (element === sourceFilter ? formatSourceLabel(value) : value)}}</option>`)
            .join("");
          element.addEventListener("change", renderExecutiveView);
        }});
      }}

      function getFilteredLeads() {{
        const selectedOwner = ownerFilter.value;
        const selectedStatus = statusFilter.value;
        const selectedSource = sourceFilter.value;
        const selectedUrgency = urgencyFilter.value;

        return (executiveData.lead_records || []).filter((lead) => {{
          return (
            (selectedOwner === "all" || lead.owner_name === selectedOwner) &&
            (selectedStatus === "all" || lead.status === selectedStatus) &&
            (selectedSource === "all" || lead.source === selectedSource) &&
            (selectedUrgency === "all" || lead.urgency === selectedUrgency)
          );
        }});
      }}

      function distributionRows(items) {{
        const maxCount = Math.max(1, ...items.map((item) => item.count || 0));
        return items.map((item) => `
          <div class="dist-row">
            <span>${{formatSourceLabel(item.label)}}</span>
            <div class="dist-bar"><div class="dist-bar-fill" style="width:${{Math.max(6, (item.count / maxCount) * 100)}}%"></div></div>
            <strong>${{formatNumber(item.count)}}</strong>
          </div>
        `).join("");
      }}

      function aggregateOwnerScorecards(leads) {{
        const ownerMap = new Map();
        const mailboxMap = new Map((executiveData.mailbox_signals_by_owner || []).map((item) => [item.owner_name, item.count]));
        leads.forEach((lead) => {{
          const key = lead.owner_name || "Assigned AE";
          if (!ownerMap.has(key)) {{
            ownerMap.set(key, {{
              owner_name: key,
              active_leads: 0,
              overdue_count: 0,
              review_count: 0,
              due_count: 0,
              touchAges: [],
              late_stage_leads: 0,
              late_stage_stale_leads: 0,
              mailbox_signals_pending: mailboxMap.get(key) || 0,
              value_total: 0,
              has_value: false,
            }});
          }}
          const row = ownerMap.get(key);
          row.active_leads += 1;
          if (lead.urgency === "overdue") row.overdue_count += 1;
          if (lead.urgency === "needs_immediate_review") row.review_count += 1;
          if (lead.urgency === "follow_up_due") row.due_count += 1;
          if (lead.days_since_touch !== null && lead.days_since_touch !== undefined) row.touchAges.push(Number(lead.days_since_touch));
          if (lead.late_stage) row.late_stage_leads += 1;
          if (lead.late_stage_stale) row.late_stage_stale_leads += 1;
          if (lead.value_numeric !== null && lead.value_numeric !== undefined) {{
            row.value_total += Number(lead.value_numeric);
            row.has_value = true;
          }}
        }});
        return Array.from(ownerMap.values())
          .map((row) => ({{
            ...row,
            avg_days_since_touch: row.touchAges.length ? (row.touchAges.reduce((sum, value) => sum + value, 0) / row.touchAges.length).toFixed(1) : "n/a",
          }}))
          .sort((a, b) => (
            b.overdue_count - a.overdue_count ||
            b.review_count - a.review_count ||
            b.late_stage_stale_leads - a.late_stage_stale_leads ||
            b.active_leads - a.active_leads ||
            a.owner_name.localeCompare(b.owner_name)
          ));
      }}

      function renderKpis(leads) {{
        const pipelineValue = leads.reduce((sum, lead) => sum + Number(lead.value_numeric || 0), 0);
        const pipelineTarget = Number(executiveData.kpis.pipeline_target || 100000);
        const pipelineProgress = pipelineTarget > 0 ? Math.min((pipelineValue / pipelineTarget) * 100, 999) : 0;
        const kpis = [
          ["Active leads", leads.length, "Current active pipeline", "number", "Current active pipeline in the filtered view."],
          ["Pipeline value", pipelineValue, "Parseable close value across filtered leads", "currency", "Only leads with parseable value are included in this rollup."],
          ["Goal progress", pipelineProgress, `Toward the $${{formatNumber(pipelineTarget)}} pipeline target`, "percent", "Share of the $100k target covered by parseable pipeline value."],
          ["Overdue", leads.filter((lead) => lead.urgency === "overdue").length, "Highest urgency follow-up risk", "number", "Leads whose follow-up window has already passed."],
          ["Review", leads.filter((lead) => lead.urgency === "needs_immediate_review").length, "Needs a decision or clean-up", "number", "Leads needing immediate review because they are untouched or missing the next step."],
          ["Due", leads.filter((lead) => lead.urgency === "follow_up_due").length, "Routine next touches", "number", "Leads due for a routine follow-up today."],
          ["Untouched 7+ days", leads.filter((lead) => lead.days_since_touch !== null && lead.days_since_touch >= 7).length, "Aging engagement risk", "number", "Leads with seven or more days since their last meaningful touch."],
          ["Late-stage stale", leads.filter((lead) => lead.late_stage_stale).length, "Needs offer / offered / negotiating at risk", "number", "Late-stage leads that are overdue or require immediate review."],
        ];
        document.getElementById("kpi-grid").innerHTML = kpis.map(([label, value, note, type, tooltip]) => `
          <section class="kpi">
            <span class="heading-line">${{label}} ${{renderInfoHint(tooltip)}}</span>
            <strong>${{type === "currency" ? formatCurrency(value) : type === "percent" ? formatPercent(value) : formatNumber(value)}}</strong>
            <small>${{note}}</small>
          </section>
        `).join("");
      }}

      function renderScorecards(leads) {{
        const rows = aggregateOwnerScorecards(leads);
        const container = document.getElementById("scorecard-table");
        if (!rows.length) {{
          container.innerHTML = '<p class="empty">No active leads match the current filters.</p>';
          return;
        }}
        container.innerHTML = `
          <table>
            <thead>
              <tr>
                <th>Owner</th>
                <th>Active</th>
                <th>Overdue</th>
                <th>Review</th>
                <th>Due</th>
                <th>Avg touch age</th>
                <th>Late stage</th>
                <th>Late-stage stale</th>
                <th>Mailbox</th>
                <th>Value</th>
              </tr>
            </thead>
            <tbody>
              ${{rows.map((row) => `
                <tr>
                  <td>${{row.owner_name}}</td>
                  <td>${{formatNumber(row.active_leads)}}</td>
                  <td>${{formatNumber(row.overdue_count)}}</td>
                  <td>${{formatNumber(row.review_count)}}</td>
                  <td>${{formatNumber(row.due_count)}}</td>
                  <td>${{row.avg_days_since_touch}}</td>
                  <td>${{formatNumber(row.late_stage_leads)}}</td>
                  <td>${{formatNumber(row.late_stage_stale_leads)}}</td>
                  <td>${{formatNumber(row.mailbox_signals_pending)}}</td>
                  <td>${{row.has_value ? formatCurrency(row.value_total) : "n/a"}}</td>
                </tr>
              `).join("")}}
            </tbody>
          </table>
        `;
      }}

      function renderDistributions(leads) {{
        const statusCounter = new Map();
        const sourceCounter = new Map();
        const agingCounter = new Map([["0-2 days", 0], ["3-7 days", 0], ["8-14 days", 0], ["15+ days", 0], ["No touch", 0]]);
        const lateStageCounter = new Map([["working qualified", 0], ["working needs offer", 0], ["working offered", 0], ["working negotiating", 0]]);

        leads.forEach((lead) => {{
          statusCounter.set(lead.status, (statusCounter.get(lead.status) || 0) + 1);
          sourceCounter.set(lead.source, (sourceCounter.get(lead.source) || 0) + 1);
          if (lead.days_since_touch === null || lead.days_since_touch === undefined) agingCounter.set("No touch", (agingCounter.get("No touch") || 0) + 1);
          else if (lead.days_since_touch <= 2) agingCounter.set("0-2 days", (agingCounter.get("0-2 days") || 0) + 1);
          else if (lead.days_since_touch <= 7) agingCounter.set("3-7 days", (agingCounter.get("3-7 days") || 0) + 1);
          else if (lead.days_since_touch <= 14) agingCounter.set("8-14 days", (agingCounter.get("8-14 days") || 0) + 1);
          else agingCounter.set("15+ days", (agingCounter.get("15+ days") || 0) + 1);

          const statusKey = (lead.status || "").toLowerCase();
          if (lateStageCounter.has(statusKey)) {{
            lateStageCounter.set(statusKey, (lateStageCounter.get(statusKey) || 0) + 1);
          }}
        }});

        const statusRows = Array.from(statusCounter.entries()).sort((a, b) => b[1] - a[1]).map(([label, count]) => ({{ label, count }}));
        const sourceRows = Array.from(sourceCounter.entries()).sort((a, b) => b[1] - a[1]).map(([label, count]) => ({{ label: formatSourceLabel(label), count }}));
        const agingRows = Array.from(agingCounter.entries()).map(([label, count]) => ({{ label, count }}));
        const lateStageRows = Array.from(lateStageCounter.entries()).map(([label, count]) => ({{ label, count }})).filter((item) => item.count > 0);

        document.getElementById("status-distribution").innerHTML = statusRows.length ? distributionRows(statusRows) : '<p class="empty">No status data for this filter set.</p>';
        document.getElementById("source-distribution").innerHTML = sourceRows.length ? distributionRows(sourceRows) : '<p class="empty">No source data for this filter set.</p>';
        document.getElementById("aging-distribution").innerHTML = distributionRows(agingRows);
        document.getElementById("late-stage-distribution").innerHTML = lateStageRows.length ? distributionRows(lateStageRows) : '<p class="empty">No late-stage leads in this filter set.</p>';
      }}

      function renderRiskList(leads) {{
        const urgencyRank = {{ overdue: 0, needs_immediate_review: 1, follow_up_due: 2 }};
        const risks = [...leads].sort((a, b) => (
          (urgencyRank[a.urgency] ?? 99) - (urgencyRank[b.urgency] ?? 99) ||
          (b.late_stage ? 1 : 0) - (a.late_stage ? 1 : 0) ||
          (b.days_since_touch || 0) - (a.days_since_touch || 0) ||
          (b.value_numeric || 0) - (a.value_numeric || 0) ||
          a.task_name.localeCompare(b.task_name)
        )).slice(0, 15);

        const list = document.getElementById("risk-list");
        if (!risks.length) {{
          list.innerHTML = '<p class="empty">No active risk items match the current filters.</p>';
          return;
        }}
        list.innerHTML = risks.map((item) => `
          <article class="risk-item ${{item.urgency === "overdue" ? "overdue" : item.urgency === "needs_immediate_review" ? "review" : ""}}">
            <div class="risk-meta">
              <span class="pill">${{item.owner_name}}</span>
              <span class="pill">${{item.status}}</span>
              <span class="pill">${{item.source}}</span>
              <span class="pill">${{item.urgency.replaceAll("_", " ")}}</span>
            </div>
            <h3>${{item.task_name}}</h3>
            <p><strong>Days since last touch:</strong> ${{item.days_since_touch ?? "n/a"}} | <strong>Value:</strong> ${{item.value_numeric !== null && item.value_numeric !== undefined ? formatCurrency(item.value_numeric) : (item.value_label || "n/a")}}</p>
            <p><strong>Next step:</strong> ${{item.next_step || "Review and define the next action."}}</p>
            ${{item.link_url ? `<p><a href="${{item.link_url}}" target="_blank" rel="noreferrer">Open ClickUp task</a></p>` : ""}}
          </article>
        `).join("");
      }}

      function renderResponseSection(leads) {{
        const hygiene = [
          ["Missing next action", leads.filter((lead) => lead.missing_next_action).length],
          ["Missing meeting outcome", leads.filter((lead) => lead.missing_meeting_outcome).length],
          ["Untouched new / contacted", leads.filter((lead) => lead.untouched_new_or_contacted).length],
          ["Inbound replies (7d)", executiveData.hygiene_counts.inbound_replies_last_7_days || 0],
          ["Mailbox signals (7d)", executiveData.hygiene_counts.mailbox_signals_last_7_days || 0],
        ];
        document.getElementById("hygiene-snapshot").innerHTML = hygiene.map(([label, value]) => `
          <div class="snapshot-row"><span>${{label}}</span><strong>${{formatNumber(value)}}</strong></div>
        `).join("");

        const selectedOwner = ownerFilter.value;
        const ownerReplies = (executiveData.inbound_replies_by_owner || []).filter((item) => selectedOwner === "all" || item.owner_name === selectedOwner);
        const ownerMailbox = (executiveData.mailbox_signals_by_owner || []).filter((item) => selectedOwner === "all" || item.owner_name === selectedOwner);
        const rows = [...new Set([...ownerReplies.map((item) => item.owner_name), ...ownerMailbox.map((item) => item.owner_name)])].map((owner) => ({{
          owner,
          replies: ownerReplies.find((item) => item.owner_name === owner)?.count || 0,
          mailbox: ownerMailbox.find((item) => item.owner_name === owner)?.count || 0,
        }})).sort((a, b) => (b.replies + b.mailbox) - (a.replies + a.mailbox) || a.owner.localeCompare(b.owner));

        const container = document.getElementById("owner-response");
        if (!rows.length) {{
          container.innerHTML = '<p class="empty" style="margin-top:16px;">No owner response signals available.</p>';
          return;
        }}
        container.innerHTML = `
          <table style="margin-top:16px;">
            <thead>
              <tr>
                <th>Owner</th>
                <th>Inbound replies</th>
                <th>Mailbox signals</th>
              </tr>
            </thead>
            <tbody>
              ${{rows.map((row) => `
                <tr>
                  <td>${{row.owner}}</td>
                  <td>${{formatNumber(row.replies)}}</td>
                  <td>${{formatNumber(row.mailbox)}}</td>
                </tr>
              `).join("")}}
            </tbody>
          </table>
        `;
      }}

      function renderExecutiveView() {{
        const leads = getFilteredLeads();
        renderKpis(leads);
        renderScorecards(leads);
        renderDistributions(leads);
        renderRiskList(leads);
        renderResponseSection(leads);
      }}

      initFilters();
      renderExecutiveView();
    </script>
  </body>
</html>"""
