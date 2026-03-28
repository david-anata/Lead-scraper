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

from sales_support_agent.config import Settings, is_active_pipeline_status, normalize_status_key
from sales_support_agent.models.entities import AutomationRun, CanvaConnection, CommunicationEvent, LeadMirror, MailboxSignal
from sales_support_agent.services.admin_nav import render_agent_nav, render_agent_nav_styles
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
    sync_auto_enabled: bool
    sync_stale_after_minutes: int
    lead_builder_ready: bool
    lead_builder_missing: list[str]
    deck_generator_ready: bool
    deck_generator_missing: list[str]
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
    last_touch_source: str
    context_summary: str
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
    last_touch_source: str
    context_summary: str
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


def _format_dashboard_date(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        if "T" in raw:
            return datetime.fromisoformat(raw.replace("Z", "+00:00")).strftime("%m/%d/%Y")
        return date.fromisoformat(raw).strftime("%m/%d/%Y")
    except ValueError:
        return raw


def _format_deck_channel_label(value: str) -> str:
    mapping = {
        "amazon": "Amazon",
        "shopify": "Shopify",
        "tiktok_shop": "TikTok Shop",
        "3pl": "3PL",
        "shipping_os": "Shipping OS",
    }
    key = str(value or "").strip().lower()
    return mapping.get(key, str(value or "").replace("_", " ").title())


def _build_deck_view_analytics(summary: dict[str, object]) -> dict[str, object]:
    analytics = dict(summary.get("view_analytics", {}) or {})
    if analytics:
        return analytics
    return {
        "internal": {"unique_visitors": 0, "total_visits": 0, "first_viewed_at": "", "last_viewed_at": "", "daily_counts": {"7": {}, "30": {}, "90": {}, "all": {}}},
        "external": {
            "unique_visitors": int(summary.get("view_count", 0) or 0),
            "total_visits": int(summary.get("view_count", 0) or 0),
            "first_viewed_at": str(summary.get("first_viewed_at", "") or ""),
            "last_viewed_at": str(summary.get("last_viewed_at", "") or ""),
            "daily_counts": {"7": {}, "30": {}, "90": {}, "all": {}},
        },
    }


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
        "sync_auto_enabled": data.sync_auto_enabled,
        "sync_stale_after_minutes": data.sync_stale_after_minutes,
        "lead_builder_ready": data.lead_builder_ready,
        "lead_builder_missing": data.lead_builder_missing,
        "deck_generator_ready": data.deck_generator_ready,
        "deck_generator_missing": data.deck_generator_missing,
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
        sync_auto_enabled=bool(payload.get("sync_auto_enabled", True)),
        sync_stale_after_minutes=int(payload.get("sync_stale_after_minutes", 30) or 30),
        lead_builder_ready=bool(payload.get("lead_builder_ready")),
        lead_builder_missing=[str(item) for item in payload.get("lead_builder_missing", [])],
        deck_generator_ready=bool(payload.get("deck_generator_ready")),
        deck_generator_missing=[str(item) for item in payload.get("deck_generator_missing", [])],
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
                "last_touch_source": item.last_touch_source,
                "context_summary": item.context_summary,
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
                "last_touch_source": item.last_touch_source,
                "context_summary": item.context_summary,
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
                last_touch_source=str(item.get("last_touch_source", "")),
                context_summary=str(item.get("context_summary", "")),
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
                last_touch_source=str(item.get("last_touch_source", "")),
                context_summary=str(item.get("context_summary", "")),
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


def _days_since_datetime(
    last_touch_at: datetime | None,
    *,
    effective_date: date,
    fallback_created_at: datetime | None = None,
) -> int | None:
    if last_touch_at is not None:
        return max((effective_date - last_touch_at.date()).days, 0)
    if fallback_created_at is not None:
        return max((effective_date - fallback_created_at.date()).days, 0)
    return None


def _clean_context_text(raw_text: str, *, limit: int = 220) -> str:
    text = re.sub(r"\s+", " ", (raw_text or "").strip())
    return trim_for_slack(text, limit=limit) if text else ""


def _latest_human_comment(comments: list[dict[str, object]]) -> tuple[datetime | None, str]:
    latest_at: datetime | None = None
    latest_text = ""
    for comment in comments:
        raw_text = str(comment.get("comment_text") or comment.get("comment") or "")
        if not raw_text or raw_text.startswith("[Sales Support Agent]"):
            continue
        raw_date = comment.get("date") or comment.get("date_created")
        parsed_at: datetime | None = None
        if isinstance(raw_date, datetime):
            parsed_at = raw_date
        else:
            raw_value = str(raw_date or "").strip()
            if raw_value.isdigit():
                parsed_at = datetime.fromtimestamp(int(raw_value) / 1000, tz=timezone.utc)
            elif raw_value:
                try:
                    parsed_at = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
                except ValueError:
                    parsed_at = None
        if parsed_at and (latest_at is None or parsed_at > latest_at):
            latest_at = parsed_at
            latest_text = _clean_context_text(raw_text)
    return latest_at, latest_text


def _latest_communication_summary(event: CommunicationEvent | None) -> tuple[datetime | None, str]:
    if event is None:
        return None, ""
    text = _clean_context_text(
        " | ".join(
            part
            for part in [
                event.summary,
                event.outcome,
                event.recommended_next_action,
            ]
            if (part or "").strip()
        )
    )
    return event.occurred_at, text


def _latest_mailbox_context(signal: MailboxSignal | None) -> tuple[datetime | None, str]:
    if signal is None:
        return None, ""
    parts = [signal.action_summary, signal.snippet, signal.subject]
    return signal.received_at, _clean_context_text(next((part for part in parts if (part or "").strip()), ""))


def _build_executive_context(
    *,
    lead: LeadMirror,
    comments: list[dict[str, object]],
    latest_event: CommunicationEvent | None,
    latest_mailbox_signal: MailboxSignal | None,
) -> tuple[str, str]:
    comment_at, comment_text = _latest_human_comment(comments)
    event_at, event_text = _latest_communication_summary(latest_event)
    mailbox_at, mailbox_text = _latest_mailbox_context(latest_mailbox_signal)

    candidates: list[tuple[datetime, str, str]] = []
    if comment_at and comment_text:
        candidates.append((comment_at, "comment", comment_text))
    if event_at and event_text:
        candidates.append((event_at, "email", event_text))
    if mailbox_at and mailbox_text:
        candidates.append((mailbox_at, "mailbox", mailbox_text))
    if candidates:
        _, source, text = max(candidates, key=lambda item: item[0])
        source_labels = {
            "comment": "Comment",
            "email": "Email event",
            "mailbox": "Mailbox signal",
        }
        return source_labels.get(source, source.title()), text

    fallback = _clean_context_text(lead.communication_summary or lead.last_meeting_outcome or lead.recommended_next_action)
    if fallback:
        return "ClickUp summary", fallback
    return "", ""


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
        status = (lead.status or "").strip()
        status_key = normalize_status_key(status)
        if not status:
            continue
        if not is_active_pipeline_status(
            status,
            active_statuses=settings.active_statuses,
            inactive_statuses=settings.inactive_statuses,
        ):
            continue
        active_lead_count += 1
        evaluation = reminder_service.evaluate_lead(lead, as_of_date=effective_date, comments=[])
        if evaluation is None:
            continue
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

    deck_generator_missing: list[str] = []
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
            "output_type": dict(run.summary_json or {}).get("output_type", ""),
            "deck_slug": dict(run.summary_json or {}).get("deck_slug", ""),
            "channels": list(dict(run.summary_json or {}).get("channels", []) or []),
            "view_count": int(dict(run.summary_json or {}).get("view_count", 0) or 0),
            "first_viewed_at": dict(run.summary_json or {}).get("first_viewed_at", ""),
            "last_viewed_at": dict(run.summary_json or {}).get("last_viewed_at", ""),
            "view_analytics": _build_deck_view_analytics(dict(run.summary_json or {})),
            "started_at": run.started_at.isoformat() if run.started_at else "",
            "completed_at": run.completed_at.isoformat() if run.completed_at else "",
        }
        for run in deck_runs
    ]

    return DashboardData(
        as_of_date=effective_date,
        total_active_leads=active_lead_count,
        stale_counts=stale_counts,
        mailbox_findings=len(mailbox_signals),
        owner_queues=owner_queues,
        latest_sync_at=latest_sync_at,
        latest_run_summary=latest_run_summary,
        sync_auto_enabled=settings.dashboard_auto_sync_enabled,
        sync_stale_after_minutes=max(1, settings.dashboard_auto_sync_max_age_minutes),
        lead_builder_ready=bool(lead_builder_status.get("ready")),
        lead_builder_missing=[str(item) for item in lead_builder_status.get("missing", [])],
        deck_generator_ready=True,
        deck_generator_missing=deck_generator_missing,
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
            .where(CommunicationEvent.occurred_at >= mailbox_start)
            .order_by(CommunicationEvent.occurred_at.desc())
        ).scalars()
    )
    active_leads = [
        lead
        for lead in leads
        if is_active_pipeline_status(
            (lead.status or "").strip(),
            active_statuses=settings.active_statuses,
            inactive_statuses=settings.inactive_statuses,
        )
    ]
    active_task_ids = [lead.clickup_task_id for lead in active_leads if lead.clickup_task_id]
    latest_meaningful_event_by_task: dict[str, CommunicationEvent] = {}
    if active_task_ids:
        meaningful_events = list(
            session.execute(
                select(CommunicationEvent)
                .where(
                    CommunicationEvent.clickup_task_id.in_(active_task_ids),
                    CommunicationEvent.event_type.in_(
                        [
                            "outbound_email_sent",
                            "inbound_reply_received",
                            "call_completed",
                            "meeting_completed",
                            "offer_sent",
                            "note_logged",
                        ]
                    ),
                )
                .order_by(CommunicationEvent.occurred_at.desc())
            ).scalars()
        )
        for event in meaningful_events:
            if event.clickup_task_id and event.clickup_task_id not in latest_meaningful_event_by_task:
                latest_meaningful_event_by_task[event.clickup_task_id] = event

    latest_event_by_task: dict[str, CommunicationEvent] = {}
    for event in inbox_reply_events:
        if event.clickup_task_id and event.clickup_task_id not in latest_event_by_task:
            latest_event_by_task[event.clickup_task_id] = event
    latest_mailbox_signal_by_task: dict[str, MailboxSignal] = {}
    for signal in mailbox_signals:
        if signal.matched_task_id and signal.matched_task_id not in latest_mailbox_signal_by_task:
            latest_mailbox_signal_by_task[signal.matched_task_id] = signal

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

    for lead in active_leads:
        status = (lead.status or "").strip()
        status_key = " ".join(status.lower().split())
        latest_event = latest_meaningful_event_by_task.get(lead.clickup_task_id)
        evaluation = reminder_service.evaluate_lead(
            lead,
            as_of_date=effective_date,
            comments=[],
            latest_event_at=latest_event.occurred_at if latest_event else None,
            comment_touch_at=None,
        )
        if evaluation is None:
            continue

        digest_item = reminder_service.build_digest_item(evaluation)
        owner_name = lead.assignee_name or "Assigned AE"
        lead_owner_map[lead.clickup_task_id] = owner_name
        source_name = _display_source_name(lead.source)
        value_numeric = _safe_numeric_value(lead.value)
        days_since_touch = _days_since_datetime(
            evaluation.last_meaningful_touch_at,
            effective_date=effective_date,
            fallback_created_at=lead.created_at,
        )
        last_touch_source, context_summary = _build_executive_context(
            lead=lead,
            comments=[],
            latest_event=latest_event,
            latest_mailbox_signal=latest_mailbox_signal_by_task.get(lead.clickup_task_id),
        )
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
                last_touch_source=last_touch_source,
                context_summary=context_summary,
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
        if event.event_type != "inbound_reply_received":
            continue
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
                last_touch_source=item.last_touch_source,
                context_summary=item.context_summary,
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
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Montserrat:wght@700;800&display=swap" rel="stylesheet">
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
        font-family: "Inter", "Segoe UI", sans-serif;
        display: flex;
        flex-direction: column;
      }}
      .shell {{
        max-width: 1160px;
        margin: 0 auto;
        padding: 32px 24px 48px;
        width: 100%;
        flex: 1 0 auto;
        display: grid;
        align-items: center;
      }}
      .workspace {{
        background: var(--white);
        border: 1px solid rgba(43, 54, 68, 0.10);
        border-radius: 28px;
        box-shadow: 0 18px 40px var(--shadow);
        padding: 34px;
        min-height: calc(100vh - 98px);
        display: grid;
        align-content: center;
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
        font-family: "Inter", "Segoe UI", sans-serif;
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
        font-family: "Inter", "Segoe UI", sans-serif;
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
        font-family: "Inter", "Segoe UI", sans-serif;
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
        font-family: "Inter", "Segoe UI", sans-serif;
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
        margin-top: 0;
      }}
      @media (max-width: 1200px) {{
        .shell {{
          padding: 24px 20px 36px;
        }}
        .split {{
          grid-template-columns: 1fr;
          gap: 28px;
        }}
        h1 {{
          font-size: clamp(40px, 10vw, 58px);
        }}
      }}
      @media (max-width: 920px) {{
        .workspace {{
          min-height: auto;
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

    item_display_limit = 4
    total_queue_items = 0
    owner_sections = []
    for queue in data.owner_queues:
        total_queue_items += queue.total_items
        item_cards = []
        for index, item in enumerate(queue.items):
            urgency_label = STALE_URGENCY_LABELS.get(item.urgency, item.urgency.replace("_", " ").title())
            draft_preview = trim_for_slack(item.suggested_reply, limit=140)
            link_html = (
                f'<a href="{html.escape(item.link_url)}" target="_blank" rel="noreferrer">Open task</a>'
                if item.link_url
                else ""
            )
            search_blob = " ".join(
                part.lower()
                for part in [queue.owner_name, item.title, item.subtitle, item.action_summary, item.source, item.date_label]
                if part
            )
            collapsed_class = " is-collapsed-by-limit" if index >= item_display_limit else ""
            item_cards.append(
                f"""
                <article class="action-item urgency-{html.escape(item.urgency)}{collapsed_class}" data-owner="{html.escape(queue.owner_name)}" data-urgency="{html.escape(item.urgency)}" data-search="{html.escape(search_blob, quote=True)}">
                  <div class="action-top">
                    <span class="badge">{html.escape(urgency_label)}</span>
                    <span class="source">{html.escape(item.source)}</span>
                    <span class="date">{html.escape(item.date_label)}</span>
                  </div>
                  <h4>{html.escape(item.title)}</h4>
                  <p class="subtitle">{html.escape(item.subtitle)}</p>
                  <p><strong>Action:</strong> {html.escape(item.action_summary)}</p>
                  <details class="draft-preview">
                    <summary>Suggested draft</summary>
                    <p>{html.escape(draft_preview or "No draft suggested yet.")}</p>
                  </details>
                  {link_html}
                </article>
                """
            )

        show_more_button = (
            f'<button class="show-more-button" type="button" data-expanded="false">Show {queue.total_items - item_display_limit} more</button>'
            if queue.total_items > item_display_limit
            else ""
        )
        owner_sections.append(
            f"""
            <section class="owner-card" data-owner="{html.escape(queue.owner_name)}" data-display-limit="{item_display_limit}">
              <header>
                <div>
                  <h3>{html.escape(queue.owner_name)}</h3>
                  <p><span class="owner-visible-count">{min(queue.total_items, item_display_limit)}</span> of {queue.total_items} items shown</p>
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
              {show_more_button}
            </section>
            """
        )

    latest_sync = format_date_label(data.latest_sync_at) if data.latest_sync_at else "not synced yet"
    latest_sync_iso = data.latest_sync_at.isoformat() if data.latest_sync_at else ""
    sync_status_initial = (
        f"Using cached board from {html.escape(latest_sync)}. Auto-refresh runs when the mirror is stale."
        if data.sync_auto_enabled
        else "Ready."
    )
    lead_builder_notice = (
        '<div class="notice warning">Lead builder is missing env vars: '
        + html.escape(", ".join(data.lead_builder_missing))
        + "</div>"
        if not data.lead_builder_ready
        else '<div class="notice success">Lead builder is ready. Running it here will still add leads to Instantly and return the CSV immediately.</div>'
    )
    today_value = data.as_of_date.isoformat()
    latest_run_summary = data.latest_run_summary or {}
    dashboard_error = str(latest_run_summary.get("dashboard_error", "") or "").strip()
    dashboard_error_notice = (
        '<div class="notice warning">Board data is temporarily unavailable. '
        + html.escape(dashboard_error)
        + "</div>"
        if dashboard_error
        else ""
    )
    empty_queue_message = (
        "No owner queues available because the board feed could not be loaded yet."
        if dashboard_error
        else (
            f"{data.total_active_leads} active leads are synced, but none currently require follow-up."
            if data.total_active_leads
            else "No owner queues yet. Run a sync or stale scan to populate the dashboard."
        )
    )
    snapshot_rows = [
        _summary_row("Latest ClickUp sync", latest_sync),
        _summary_row("Stale scan status", latest_run_summary.get("status", "No stale scan recorded")),
        _summary_row("Inspected leads", latest_run_summary.get("inspected", 0)),
        _summary_row("Alerts prepared", latest_run_summary.get("alerted", 0)),
        _summary_row("Comments posted", latest_run_summary.get("commented", 0)),
        _summary_row("Comments suppressed", latest_run_summary.get("comment_skipped_duplicate", 0)),
        _summary_row("Tasks synced", latest_run_summary.get("synced_tasks", 0)),
        _summary_row("Failed items", latest_run_summary.get("failed", 0)),
    ]
    if "digest_posted" in latest_run_summary:
        snapshot_rows.append(_summary_row("Digest posted", latest_run_summary.get("digest_posted")))
    if "immediate_alerted" in latest_run_summary:
        snapshot_rows.append(_summary_row("Immediate alerts", latest_run_summary.get("immediate_alerted")))
    headline_snapshot_rows = [
        _summary_row("Board updated", latest_sync),
        _summary_row("Stale scan", latest_run_summary.get("status", "No stale scan recorded")),
        _summary_row("Failed items", latest_run_summary.get("failed", 0)),
    ]
    extended_snapshot_rows = snapshot_rows[3:]

    deck_ready_notice = (
        '<div class="notice warning">Deck generator is missing env vars: '
        + html.escape(", ".join(data.deck_generator_missing))
        + ".</div>"
        if data.deck_generator_missing
        else '<div class="notice success">Deck generator is configured for the Amazon-first HTML workflow.</div>'
    )
    recent_deck_runs_html = "".join(
        f"""
        <article class="deck-run-item">
          <div>
            <strong>{html.escape(str(run.get("design_title") or run.get("design_id") or f"Run {run.get('id', '')}"))}</strong>
            <p class="muted">Created {html.escape(_format_dashboard_date(str(run.get("started_at") or "")) or "Today")}</p>
            <ul class="deck-run-bullets">
              {''.join(f"<li>{html.escape(_format_deck_channel_label(channel))}</li>" for channel in (run.get("channels") or []))}
            </ul>
          </div>
          <div class="deck-run-links">
            {f'<a href="{html.escape(str(run.get("view_url") or ""))}?viewer=internal" target="_blank" rel="noreferrer">Open deck</a>' if run.get("view_url") else ""}
            <button type="button" class="analytics-button" data-analytics='{html.escape(json.dumps(run.get("view_analytics") or {}))}'>View analytics</button>
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
    <title>agent | Admin Dashboard</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Montserrat:wght@700;800&display=swap" rel="stylesheet">
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
      }}
      * {{ box-sizing: border-box; }}
      body {{
        margin: 0;
        background: var(--light-brown);
        color: var(--text);
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
      .header-meta {{
        display: grid;
        gap: 12px;
      }}
      .page-copy {{
        font-weight: 300;
        font-size: 17px;
        line-height: 1.5;
        color: var(--dark-blue);
      }}
      .freshness-strip {{
        display: flex;
        flex-wrap: wrap;
        gap: 10px;
      }}
      .freshness-pill {{
        display: inline-flex;
        align-items: center;
        gap: 8px;
        padding: 10px 14px;
        border-radius: 999px;
        background: rgba(133, 187, 218, 0.16);
        color: var(--dark-blue);
        font-family: "Montserrat", sans-serif;
        font-size: 12px;
        font-weight: 700;
        letter-spacing: 0.03em;
        text-transform: uppercase;
      }}
      .freshness-pill strong {{
        font-size: 12px;
      }}
      .controls-grid {{
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 16px;
        margin-bottom: 22px;
      }}
      .panel-card,
      .meta-card {{
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
        margin: 0;
      }}
      .panel-card h3,
      .meta-card h2 {{
        font-family: "Montserrat", sans-serif;
        font-weight: 700;
        font-size: 27px;
        color: var(--dark-blue);
      }}
      .panel-card p,
      .meta-card p {{
        margin: 0 0 16px;
        font-weight: 300;
        font-size: 16px;
        line-height: 1.45;
      }}
      .panel-card button,
      .lead-form button {{
        width: auto;
        border: 0;
        border-radius: 999px;
        padding: 13px 22px;
        background: var(--light-blue);
        color: var(--white);
        font-family: "Montserrat", sans-serif;
        font-weight: 700;
        font-size: 16px;
        cursor: pointer;
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
        min-height: 142px;
      }}
      .metric span {{
        display: block;
        font-family: "Montserrat", sans-serif;
        font-weight: 700;
        font-size: 14px;
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
        font-size: 34px;
        line-height: 1;
        color: var(--dark-blue);
        margin-bottom: 10px;
      }}
      .metric small {{
        color: var(--dark-blue);
        display: block;
        font-weight: 300;
        font-size: 14px;
        line-height: 1.45;
      }}
      .snapshot-rows {{
        display: grid;
        gap: 8px;
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
        font-size: 13px;
        color: var(--alt-dark-blue);
        text-transform: uppercase;
        letter-spacing: 0.04em;
        font-family: "Montserrat", sans-serif;
        font-weight: 700;
      }}
      .snapshot-row strong {{
        font-size: 15px;
        color: var(--dark-blue);
        font-family: "Inter", "Segoe UI", sans-serif;
        font-weight: 400;
      }}
      .notice {{
        border-radius: 12px;
        padding: 14px 16px;
        margin-bottom: 14px;
        line-height: 1.35;
        font-weight: 300;
        font-size: 14px;
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
      .lead-form,
      .draft-form {{
        display: grid;
        gap: 14px;
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }}
      .lead-form label,
      .draft-form label {{
        display: grid;
        gap: 8px;
        font-family: "Montserrat", sans-serif;
        font-weight: 700;
        font-size: 13px;
        text-transform: uppercase;
        letter-spacing: 0.04em;
      }}
      .lead-form input,
      .lead-form textarea,
      .draft-form input,
      .draft-form textarea {{
        width: 100%;
        padding: 16px 18px;
        border-radius: 10px;
        border: 2px solid rgba(43, 54, 68, 0.16);
        background: var(--white);
        font-family: "Inter", "Segoe UI", sans-serif;
        font-weight: 300;
        font-size: 15px;
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
      .lead-form .lead-submit,
      .draft-form .draft-submit {{
        grid-column: 1 / -1;
        display: flex;
        align-items: end;
        gap: 12px;
        flex-wrap: wrap;
      }}
      .draft-form .draft-body-field,
      .draft-form .draft-help {{
        grid-column: 1 / -1;
      }}
      .lead-form button[disabled] {{
        opacity: 0.68;
        background: var(--brown);
        cursor: wait;
      }}
      .lead-form .full-width {{
        grid-column: 1 / -1;
      }}
      .offer-toggle-group {{
        grid-column: 1 / -1;
        display: grid;
        gap: 12px;
        padding: 16px 18px;
        border-radius: 12px;
        border: 2px solid rgba(43, 54, 68, 0.12);
        background: rgba(191, 168, 137, 0.08);
      }}
      .offer-builder {{
        display: grid;
        gap: 14px;
      }}
      .offer-builder-head {{
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 12px;
        flex-wrap: wrap;
      }}
      .offer-builder-head p {{
        margin: 0;
        color: var(--alt-dark-blue);
      }}
      .offer-builder-actions {{
        display: flex;
        gap: 10px;
        flex-wrap: wrap;
      }}
      .offer-builder-actions button {{
        border: 0;
        border-radius: 999px;
        padding: 10px 14px;
        background: rgba(43, 54, 68, 0.10);
        color: var(--dark-blue);
        font-family: "Montserrat", sans-serif;
        font-weight: 700;
        cursor: pointer;
      }}
      .offer-editor-list {{
        display: grid;
        gap: 14px;
      }}
      .offer-editor {{
        display: grid;
        gap: 14px;
        padding: 16px;
        border-radius: 12px;
        border: 1px solid rgba(43, 54, 68, 0.12);
        background: rgba(255, 255, 255, 0.8);
      }}
      .offer-editor-top {{
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 12px;
        flex-wrap: wrap;
      }}
      .offer-editor-toggle {{
        border: 0;
        background: transparent;
        padding: 0;
        color: var(--dark-blue);
        font-family: "Montserrat", sans-serif;
        font-size: 15px;
        font-weight: 700;
        cursor: pointer;
        display: inline-flex;
        align-items: center;
        gap: 8px;
      }}
      .offer-editor-toggle::after {{
        content: "▾";
        font-size: 12px;
      }}
      .offer-editor-toggle[aria-expanded="true"]::after {{
        content: "▴";
      }}
      .offer-editor-body[hidden] {{
        display: none;
      }}
      .offer-editor-grid {{
        display: grid;
        grid-template-columns: repeat(3, minmax(0, 1fr));
        gap: 12px;
      }}
      .offer-editor-grid .full-width {{
        grid-column: 1 / -1;
      }}
      .checkbox-label {{
        display: inline-flex;
        align-items: center;
        gap: 10px;
        font-family: "Inter", "Segoe UI", sans-serif;
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
      .toggle-switch {{
        position: relative;
        width: 46px;
        height: 26px;
        display: inline-flex;
        align-items: center;
      }}
      .toggle-switch input {{
        position: absolute;
        inset: 0;
        opacity: 0;
      }}
      .toggle-switch span {{
        width: 46px;
        height: 26px;
        border-radius: 999px;
        background: rgba(43, 54, 68, 0.16);
        position: relative;
        transition: background 140ms ease;
      }}
      .toggle-switch span::after {{
        content: "";
        position: absolute;
        top: 3px;
        left: 3px;
        width: 20px;
        height: 20px;
        border-radius: 50%;
        background: #fff;
        box-shadow: 0 2px 6px rgba(0,0,0,0.16);
        transition: transform 140ms ease;
      }}
      .toggle-switch input:checked + span {{
        background: var(--light-blue);
      }}
      .toggle-switch input:checked + span::after {{
        transform: translateX(20px);
      }}
      .draft-help {{
        color: var(--alt-dark-blue);
        font-family: "Inter", "Segoe UI", sans-serif;
        font-weight: 300;
        font-size: 14px;
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
        font-size: 14px;
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
      .button-link {{
        display: inline-flex;
        align-items: center;
        justify-content: center;
        width: auto;
        border: 0;
        border-radius: 999px;
        padding: 13px 22px;
        background: var(--dark-blue);
        color: var(--white);
        font-family: "Montserrat", sans-serif;
        font-weight: 700;
        font-size: 15px;
        text-decoration: none;
        cursor: pointer;
      }}
      .utility-hub {{
        margin-bottom: 22px;
      }}
      .utility-drawers {{
        display: grid;
        gap: 12px;
      }}
      .utility-drawer {{
        border: 1px solid rgba(43, 54, 68, 0.10);
        border-radius: 16px;
        background: rgba(249, 247, 243, 0.70);
        overflow: hidden;
      }}
      .utility-drawer[open] {{
        background: var(--white);
      }}
      .utility-drawer summary {{
        list-style: none;
        cursor: pointer;
        padding: 16px 18px;
        font-family: "Montserrat", sans-serif;
        font-size: 18px;
        font-weight: 700;
        color: var(--dark-blue);
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 12px;
      }}
      .utility-drawer summary::-webkit-details-marker {{
        display: none;
      }}
      .utility-drawer summary::after {{
        content: "+";
        font-size: 20px;
      }}
      .utility-drawer[open] summary::after {{
        content: "-";
      }}
      .utility-body {{
        padding: 0 18px 18px;
      }}
      .panel-stack {{
        display: grid;
        gap: 14px;
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
        font-size: 15px;
        color: var(--dark-blue);
      }}
      .deck-run-item p {{
        margin: 0;
        font-size: 14px;
      }}
      .deck-run-bullets {{
        margin: 8px 0 0;
        padding-left: 18px;
        display: grid;
        gap: 4px;
        color: var(--alt-dark-blue);
      }}
      .deck-run-links {{
        display: flex;
        gap: 8px;
        flex-wrap: wrap;
        justify-content: flex-end;
      }}
      .deck-run-links button,
      .analytics-button {{
        display: inline-flex;
        align-items: center;
        border: 0;
        border-radius: 999px;
        padding: 8px 12px;
        background: rgba(43, 54, 68, 0.08);
        color: var(--dark-blue);
        font-family: "Montserrat", sans-serif;
        font-weight: 700;
        font-size: 12px;
        cursor: pointer;
      }}
      .analytics-modal {{
        position: fixed;
        inset: 0;
        background: rgba(43, 54, 68, 0.55);
        display: none;
        align-items: center;
        justify-content: center;
        padding: 24px;
        z-index: 30;
      }}
      .analytics-modal.is-visible {{
        display: flex;
      }}
      .analytics-dialog {{
        width: min(920px, 100%);
        max-height: 88vh;
        overflow: auto;
        background: white;
        border-radius: 18px;
        padding: 22px;
        box-shadow: 0 28px 60px rgba(43, 54, 68, 0.26);
      }}
      .analytics-head {{
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 12px;
        margin-bottom: 16px;
      }}
      .analytics-head button {{
        border: 0;
        background: transparent;
        font-size: 22px;
        cursor: pointer;
      }}
      .analytics-grid {{
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 14px;
        margin-bottom: 18px;
      }}
      .analytics-card {{
        border: 1px solid rgba(43, 54, 68, 0.10);
        border-radius: 14px;
        padding: 14px 16px;
        background: rgba(249, 247, 243, 0.84);
      }}
      .analytics-card ul {{
        margin: 10px 0 0;
        padding-left: 18px;
        display: grid;
        gap: 6px;
      }}
      .analytics-tabs {{
        display: flex;
        gap: 8px;
        flex-wrap: wrap;
        margin-bottom: 10px;
      }}
      .analytics-tabs button {{
        border: 0;
        border-radius: 999px;
        padding: 8px 12px;
        cursor: pointer;
        background: rgba(43, 54, 68, 0.08);
        font-family: "Montserrat", sans-serif;
        font-weight: 700;
        font-size: 12px;
      }}
      .analytics-tabs button.is-active {{
        background: var(--light-blue);
        color: var(--dark-blue);
      }}
      .section-bar {{
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 16px;
        margin-bottom: 16px;
        flex-wrap: wrap;
      }}
      .section-title {{
        margin: 0;
        font-family: "Montserrat", sans-serif;
        font-weight: 700;
        font-size: 32px;
        line-height: 1;
        color: var(--dark-blue);
      }}
      .filters {{
        display: flex;
        flex-wrap: wrap;
        gap: 10px;
        align-items: center;
      }}
      .filters select,
      .filters input {{
        min-width: 190px;
        border: 2px solid rgba(43, 54, 68, 0.14);
        border-radius: 999px;
        padding: 12px 16px;
        font-family: "Inter", "Segoe UI", sans-serif;
        font-size: 15px;
        background: var(--white);
        color: var(--dark-blue);
      }}
      .filters input {{
        min-width: 240px;
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
        font-size: 13px;
        font-weight: 700;
        cursor: pointer;
      }}
      .filter-button.is-active {{
        background: var(--dark-blue);
        color: var(--white);
        border-color: var(--dark-blue);
      }}
      .queue-toolbar {{
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 16px;
        flex-wrap: wrap;
        margin-bottom: 18px;
      }}
      .urgency-legend {{
        display: flex;
        gap: 10px;
        flex-wrap: wrap;
      }}
      .legend-chip {{
        display: inline-flex;
        align-items: center;
        gap: 8px;
        padding: 9px 12px;
        border-radius: 999px;
        color: var(--dark-blue);
        font-family: "Montserrat", sans-serif;
        font-size: 12px;
        font-weight: 700;
        letter-spacing: 0.03em;
        text-transform: uppercase;
      }}
      .legend-chip.overdue {{
        background: rgba(51, 68, 92, 0.10);
      }}
      .legend-chip.review {{
        background: rgba(191, 168, 137, 0.20);
      }}
      .legend-chip.due {{
        background: rgba(133, 187, 218, 0.16);
      }}
      .filter-results {{
        font-size: 14px;
        color: var(--alt-dark-blue);
      }}
      .filtered-empty {{
        display: none;
        margin-bottom: 18px;
        padding: 16px 18px;
        border: 1px dashed rgba(43, 54, 68, 0.18);
        border-radius: 16px;
        background: rgba(249, 247, 243, 0.75);
        color: var(--dark-blue);
      }}
      .filtered-empty.is-visible {{
        display: block;
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
        font-size: 26px;
        line-height: 1;
        color: var(--dark-blue);
      }}
      .owner-card p {{
        margin: 0;
        color: var(--dark-blue);
        font-weight: 300;
        font-size: 15px;
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
        padding: 9px 12px;
        font-family: "Montserrat", sans-serif;
        font-weight: 700;
        font-size: 13px;
        background: rgba(133, 187, 218, 0.20);
        color: var(--dark-blue);
      }}
      .owner-items {{
        display: grid;
        gap: 14px;
      }}
      .badge,
      .source {{
        display: inline-flex;
        align-items: center;
        border-radius: 999px;
        padding: 7px 11px;
        font-family: "Montserrat", sans-serif;
        font-weight: 700;
        font-size: 11px;
        background: rgba(191, 168, 137, 0.22);
        color: var(--dark-blue);
      }}
      .action-item {{
        background: var(--light-brown);
        border: 2px solid rgba(43, 54, 68, 0.08);
        border-left: 8px solid var(--light-blue);
        border-radius: 18px;
        padding: 16px;
      }}
      .action-item.is-collapsed-by-limit {{
        display: none;
      }}
      .urgency-overdue {{
        border-left-color: var(--dark-blue);
        background: rgba(51, 68, 92, 0.06);
      }}
      .urgency-needs_immediate_review {{
        border-left-color: var(--brown);
      }}
      .urgency-follow_up_due {{
        border-left-color: var(--light-blue);
        background: rgba(133, 187, 218, 0.10);
      }}
      .action-top {{
        display: flex;
        flex-wrap: wrap;
        gap: 10px;
        align-items: center;
        margin-bottom: 12px;
      }}
      .date {{
        color: var(--dark-blue);
        font-family: "Inter", "Segoe UI", sans-serif;
        font-weight: 300;
        font-size: 13px;
      }}
      .action-item h4 {{
        margin: 0 0 10px;
        font-family: "Montserrat", sans-serif;
        font-weight: 700;
        font-size: 22px;
        line-height: 1.05;
        color: var(--dark-blue);
      }}
      .action-item p {{
        margin: 0 0 10px;
        font-weight: 300;
        font-size: 15px;
        line-height: 1.45;
      }}
      .subtitle {{
        color: var(--alt-dark-blue);
      }}
      .draft-preview {{
        margin: 10px 0 12px;
        border-top: 1px solid rgba(43, 54, 68, 0.08);
        padding-top: 10px;
      }}
      .draft-preview summary {{
        cursor: pointer;
        list-style: none;
        font-family: "Montserrat", sans-serif;
        font-weight: 700;
        font-size: 12px;
        letter-spacing: 0.04em;
        text-transform: uppercase;
        color: var(--alt-dark-blue);
      }}
      .draft-preview summary::-webkit-details-marker {{
        display: none;
      }}
      .show-more-button {{
        margin-top: 14px;
        border: 1px solid rgba(43, 54, 68, 0.12);
        border-radius: 999px;
        padding: 10px 14px;
        background: rgba(133, 187, 218, 0.10);
        color: var(--dark-blue);
        font-family: "Montserrat", sans-serif;
        font-size: 13px;
        font-weight: 700;
        cursor: pointer;
      }}
      .empty {{
        font-size: 16px;
      }}
      .draft-mode-note {{
        margin: -6px 0 14px;
        font-size: 14px;
        line-height: 1.45;
        color: var(--alt-dark-blue);
      }}
      .draft-mode-note strong {{
        font-family: "Montserrat", sans-serif;
        font-size: 13px;
        letter-spacing: 0.02em;
        text-transform: uppercase;
      }}
      .draft-summary-grid {{
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: 10px;
        margin-bottom: 14px;
      }}
      .draft-summary-card {{
        background: rgba(133, 187, 218, 0.08);
        border: 1px solid rgba(43, 54, 68, 0.10);
        border-radius: 14px;
        padding: 12px 14px;
      }}
      .draft-summary-card span {{
        display: block;
        font-family: "Montserrat", sans-serif;
        font-size: 12px;
        font-weight: 700;
        letter-spacing: 0.04em;
        text-transform: uppercase;
        color: var(--alt-dark-blue);
        margin-bottom: 8px;
      }}
      .draft-summary-card strong {{
        display: block;
        font-family: "Montserrat", sans-serif;
        font-size: 24px;
        font-weight: 800;
        color: var(--dark-blue);
      }}
      .draft-summary-card small {{
        display: block;
        margin-top: 6px;
        font-size: 13px;
        line-height: 1.4;
        color: var(--dark-blue);
      }}
      .result-meta {{
        margin: 4px 0 12px;
        font-size: 14px;
        color: var(--alt-dark-blue);
      }}
      .preview-card-list,
      .created-card-list {{
        display: grid;
        gap: 12px;
      }}
      .preview-card,
      .created-card {{
        background: rgba(249, 247, 243, 0.8);
        border: 1px solid rgba(43, 54, 68, 0.10);
        border-radius: 16px;
        padding: 14px 16px;
      }}
      .preview-card-head,
      .created-card-head {{
        display: flex;
        justify-content: space-between;
        gap: 12px;
        align-items: flex-start;
        margin-bottom: 10px;
      }}
      .preview-card h4,
      .created-card h4 {{
        margin: 0 0 4px;
        font-family: "Montserrat", sans-serif;
        font-size: 18px;
        font-weight: 700;
        color: var(--dark-blue);
      }}
      .preview-card p,
      .created-card p {{
        margin: 0;
        font-size: 14px;
        line-height: 1.45;
      }}
      .preview-subject {{
        margin: 10px 0 8px;
        font-size: 14px;
        color: var(--dark-blue);
      }}
      .preview-body {{
        margin: 0;
        padding: 12px 14px;
        border-radius: 14px;
        background: var(--white);
        border: 1px solid rgba(43, 54, 68, 0.10);
        white-space: pre-wrap;
        word-break: break-word;
        font-size: 14px;
        line-height: 1.5;
        color: var(--dark-blue);
      }}
      .preview-card-tags,
      .created-card-tags {{
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
      }}
      .draft-chip {{
        display: inline-flex;
        align-items: center;
        justify-content: center;
        padding: 7px 10px;
        border-radius: 999px;
        background: rgba(133, 187, 218, 0.16);
        border: 1px solid rgba(43, 54, 68, 0.08);
        font-family: "Montserrat", sans-serif;
        font-size: 12px;
        font-weight: 700;
        color: var(--dark-blue);
      }}
      .draft-chip.success {{
        background: rgba(133, 187, 218, 0.22);
      }}
      .draft-chip.warn {{
        background: rgba(191, 168, 137, 0.22);
      }}
      .result-block ul {{
        margin: 10px 0 0;
        padding-left: 18px;
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
      @media (max-width: 1180px) {{
        .page-header,
        .controls-grid,
        .metrics,
        .draft-summary-grid,
        .lead-form,
        .draft-form {{
          grid-template-columns: 1fr;
        }}
        .page-title {{
          font-size: clamp(38px, 8vw, 52px);
        }}
      }}
      @media (max-width: 960px) {{
        .topbar-inner {{
          flex-wrap: wrap;
        }}
        .brandmark {{
          font-size: 34px;
        }}
        .offer-editor-grid,
        .analytics-grid {{
          grid-template-columns: 1fr;
        }}
        .metric strong,
        .section-title,
        .panel-card h3,
        .meta-card h2 {{
          font-size: 24px;
        }}
      }}
    </style>
  </head>
  <body>
    {render_agent_nav("sales")}
    <div class="shell">
      <div class="workspace">
        <section class="page-header">
          <div>
            <div class="eyebrow">Agent dashboard</div>
            <h1 class="page-title">Sales <span class="highlight">Priorities</span>.</h1>
          </div>
          <div class="header-meta">
            <div class="page-copy">
              Keep the board centered on owner actions first, lead pulls second, and hide secondary tools until they are actually needed.
            </div>
            <div class="freshness-strip">
              <div class="freshness-pill">Queue <strong>{total_queue_items}</strong></div>
              <div class="freshness-pill">Updated <strong>{html.escape(latest_sync)}</strong></div>
              <div class="freshness-pill">Mailbox <strong>{data.mailbox_findings}</strong></div>
            </div>
          </div>
        </section>

        <section class="controls-grid">
          <div class="panel-card">
            <div class="card-title-line">
              <h3>Sync data</h3>
              {info_hint("Refreshes the ClickUp mirror and recalculates the stale-priority queue. Run this when you want the board to reflect the latest task state before reviewing owner work.")}
            </div>
            <p>The board loads from the last cached ClickUp sync and refreshes itself when that cache gets stale.</p>
            <button id="sync-dashboard-button" type="button">REFRESH NOW</button>
            <div class="status-line" id="sync-status">{sync_status_initial}</div>
          </div>

          <div class="panel-card" id="lead-pull-panel">
            <div class="card-title-line">
              <h3>Run lead pull</h3>
              {info_hint("Runs the outbound lead pipeline from this dashboard. It sources fresh companies, finds matched contacts, adds accepted leads into Instantly, and then returns the CSV download for review.")}
            </div>
            <p>Run the active lead pipeline here. Leads still go to Instantly first, then the CSV downloads immediately.</p>
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
              <h2>Board health</h2>
              {info_hint("Quick readout of the latest sync and stale-scan activity so you can see whether the board is fresh and whether recent automation runs completed cleanly.")}
            </div>
            <div class="snapshot-rows">
              {''.join(headline_snapshot_rows)}
            </div>
            <details class="draft-preview">
              <summary>Show scan details</summary>
              <div class="snapshot-rows">
                {''.join(extended_snapshot_rows) or _summary_row("Details", "No extra run details yet")}
              </div>
            </details>
          </section>
        </section>

        {dashboard_error_notice}

        <section class="meta-card utility-hub">
          <div class="card-title-line">
            <h2>Optional tools</h2>
            {info_hint("These workflows are helpful, but they are not required for reviewing priorities. They stay collapsed by default so the queue remains the center of the page.")}
          </div>
          <div class="utility-drawers">
            <details class="utility-drawer" id="gmail-drafts-panel">
              <summary>Bulk Gmail drafts</summary>
              <div class="utility-body">
                <p>Upload a CSV and create Gmail drafts in bulk without sending anything. Required column: <strong>email</strong>. Optional columns: <strong>first_name</strong>, <strong>last_name</strong>, <strong>company</strong>, <strong>subject</strong>, <strong>body</strong>, plus any custom fields you want to reference.</p>
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
                  <div class="draft-mode-note" id="draft-mode-note"><strong>Preview mode is on.</strong> We will validate rows and show rendered email previews, but nothing will be created in Gmail until you turn preview mode off.</div>
                  <label class="draft-body-field">
                    Body template
                    <textarea name="body_template" placeholder="Hi {{first_name}},&#10;&#10;Reaching out because {{objective}}.&#10;&#10;Would you be open to a quick conversation next week?&#10;&#10;Best,&#10;David"></textarea>
                  </label>
                  <div class="draft-help">
                    Use placeholders like <strong>{'{{first_name}}'}</strong>, <strong>{'{{company}}'}</strong>, <strong>{'{{objective}}'}</strong>, or any normalized CSV header. If your CSV already includes <strong>subject</strong> or <strong>body</strong> columns, you can leave the template fields blank.
                  </div>
                  <div class="draft-submit">
                    <button type="submit" id="drafts-submit-button">PREVIEW DRAFTS</button>
                    <a class="button-link" href="https://mail.google.com/mail/u/0/#drafts" target="_blank" rel="noreferrer">OPEN GMAIL DRAFTS</a>
                  </div>
                </form>
                <div class="status-line" id="drafts-status">Drafts: Ready.</div>
                <div class="draft-results" id="drafts-results"></div>
              </div>
            </details>

            <details class="utility-drawer" id="deck-generator-panel">
              <summary>Generate sales deck</summary>
              <div class="utility-body">
                <p>Upload one or more competitor and keyword CSVs for the niche, provide the prospect product URL or ASIN, and configure the recommended engagement. Case studies and the full service-offering section are embedded automatically.</p>
                {deck_ready_notice}
                <form class="lead-form" id="deck-generator-form">
                  <label>
                    Target product URL or ASIN
                    <input type="text" name="target_product_input" placeholder="Prospect product URL or B0ABC12345" />
                  </label>
                  <label>
                    Competitor CSVs
                    <input type="file" name="competitor_xray_csv" accept=".csv,text/csv" multiple />
                  </label>
                  <label>
                    Keyword CSVs
                    <input type="file" name="keyword_xray_csv" accept=".csv,text/csv" multiple />
                  </label>
                  <label>
                    Cerebro CSV
                    <input type="file" name="cerebro_csv" accept=".csv,text/csv" />
                  </label>
                  <label>
                    Word frequency CSV
                    <input type="file" name="word_frequency_csv" accept=".csv,text/csv" />
                  </label>
                  <label>
                    Creative mockup URL
                    <input type="url" name="creative_mockup_url" placeholder="https://www.canva.com/design/..." />
                  </label>
                  <div class="draft-help full-width">Case studies are embedded automatically from the shared public deck link. Xray and keyword uploads accept multiple files and merge them before deck generation. Cerebro and word frequency uploads are optional and feed the search-behavior story.</div>
                  <fieldset class="offer-toggle-group">
                    <legend>Recommended plan options</legend>
                    <label class="checkbox-label">
                      <span>Include recommended plan slide</span>
                      <span class="toggle-switch"><input type="checkbox" id="deck-include-plan" name="include_recommended_plan" value="true" checked /><span aria-hidden="true"></span></span>
                    </label>
                    <div class="offer-builder">
                      <div class="offer-builder-head">
                        <p>Edit the offer cards directly. These values feed the deck as written here.</p>
                        <div class="offer-builder-actions">
                          <button type="button" id="deck-add-offer">ADD OFFER</button>
                        </div>
                      </div>
                      <input type="hidden" name="offer_payload_json" id="deck-offer-payload-json" value="" />
                      <div class="offer-editor-list" id="deck-offer-list">
                        <div class="offer-editor" data-offer-index="0">
                          <div class="offer-editor-top">
                            <button type="button" class="offer-editor-toggle" aria-expanded="false">Channel management</button>
                            <label class="checkbox-label"><span>Include</span><span class="toggle-switch"><input type="checkbox" class="offer-enabled" checked /><span aria-hidden="true"></span></span></label>
                          </div>
                          <div class="offer-editor-body" hidden>
                          <div class="offer-editor-grid">
                            <label class="full-width">
                              Offer title
                              <input type="text" class="offer-title" value="Channel management" />
                            </label>
                            <label class="full-width">
                              Description
                              <textarea class="offer-description">Full-service Amazon marketing and operations support, including graphic designers, advertising management, and more.</textarea>
                            </label>
                            <label>
                              Price
                              <input type="text" class="offer-price" value="$3,000" />
                            </label>
                            <label>
                              Price label
                              <input type="text" class="offer-price-label" value="Monthly retainer fee" />
                            </label>
                            <label>
                              Commission
                              <input type="text" class="offer-commission" value="5%" />
                            </label>
                            <label>
                              Commission label
                              <input type="text" class="offer-commission-label" value="Commission on growth" />
                            </label>
                            <label>
                              Baseline
                              <input type="text" class="offer-baseline" value="$10,000" />
                            </label>
                            <label>
                              Baseline label
                              <input type="text" class="offer-baseline-label" value="Commission baseline" />
                            </label>
                            <label class="full-width">
                              Bonus / note
                              <input type="text" class="offer-bonus" value="+TikTok Shop Support" />
                            </label>
                          </div>
                          </div>
                        </div>
                        <div class="offer-editor" data-offer-index="1">
                          <div class="offer-editor-top">
                            <button type="button" class="offer-editor-toggle" aria-expanded="false">Commission Model + Shipping OS</button>
                            <label class="checkbox-label"><span>Include</span><span class="toggle-switch"><input type="checkbox" class="offer-enabled" checked /><span aria-hidden="true"></span></span></label>
                          </div>
                          <div class="offer-editor-body" hidden>
                          <div class="offer-editor-grid">
                            <label class="full-width">
                              Offer title
                              <input type="text" class="offer-title" value="Commission Model + Shipping OS" />
                            </label>
                            <label class="full-width">
                              Description
                              <textarea class="offer-description">A performance-based growth model that aligns marketing, inventory, and fulfillment under one operating system - ensuring every dollar of demand can be fulfilled profitably.</textarea>
                            </label>
                            <label>
                              Price
                              <input type="text" class="offer-price" value="$0" />
                            </label>
                            <label>
                              Price label
                              <input type="text" class="offer-price-label" value="Monthly retainer fee" />
                            </label>
                            <label>
                              Commission
                              <input type="text" class="offer-commission" value="10%" />
                            </label>
                            <label>
                              Commission label
                              <input type="text" class="offer-commission-label" value="Commission over baseline" />
                            </label>
                            <label>
                              Baseline
                              <input type="text" class="offer-baseline" value="$TBD" />
                            </label>
                            <label>
                              Baseline label
                              <input type="text" class="offer-baseline-label" value="Commission baseline" />
                            </label>
                            <label class="full-width">
                              Bonus / note
                              <input type="text" class="offer-bonus" value="Shipping OS | Required (* Order Min.)" />
                            </label>
                          </div>
                          </div>
                        </div>
                      </div>
                    </div>
                  </fieldset>
                  <div class="lead-submit">
                    <button type="submit" id="deck-submit-button">GENERATE DECK</button>
                  </div>
                </form>
                <div class="draft-help">
                  This workflow creates a first-party HTML deck with Anata branding, a persistent URL, embedded case studies, and a fixed service-offering section. The keyword CSV is optional but recommended for SEO slides.
                </div>
                <div class="status-line" id="deck-status">Deck status: Ready.</div>
                <div class="deck-run-list" id="deck-run-list">
                  {recent_deck_runs_html or '<p class="empty">No deck generation runs yet.</p>'}
                </div>
                <div class="analytics-modal" id="deck-analytics-modal" aria-hidden="true">
                  <div class="analytics-dialog">
                    <div class="analytics-head">
                      <h3>Deck analytics</h3>
                      <button type="button" id="deck-analytics-close" aria-label="Close analytics">×</button>
                    </div>
                    <div class="analytics-grid" id="deck-analytics-summary"></div>
                    <div class="analytics-tabs" id="deck-analytics-tabs">
                      <button type="button" class="is-active" data-window="7">7 days</button>
                      <button type="button" data-window="30">30 days</button>
                      <button type="button" data-window="90">90 days</button>
                      <button type="button" data-window="all">All time</button>
                    </div>
                    <div class="analytics-card">
                      <h4>Visits by day</h4>
                      <div id="deck-analytics-daily"></div>
                    </div>
                    <p class="draft-help">Visit counts, first and last visits, and daily trend windows are available here. Visit-length and per-section time tracking are not enabled yet.</p>
                  </div>
                </div>
              </div>
            </details>
          </div>
        </section>

        <section class="metrics">{metric_cards}</section>

        <section class="section-bar">
          <h2 class="section-title">Owner priorities.</h2>
          <div class="filters">
            <select id="owner-filter" aria-label="Filter by owner">
              <option value="all">All owners</option>
              {owner_options}
            </select>
            <input id="queue-search" type="search" placeholder="Search lead, source, or action" aria-label="Search queue" />
            <div class="filter-buttons" id="urgency-filter">
              <button class="filter-button is-active" type="button" data-urgency="all">All</button>
              <button class="filter-button" type="button" data-urgency="overdue">Overdue {data.stale_counts.get("overdue", 0)}</button>
              <button class="filter-button" type="button" data-urgency="needs_immediate_review">Review {data.stale_counts.get("needs_immediate_review", 0)}</button>
              <button class="filter-button" type="button" data-urgency="follow_up_due">Due {data.stale_counts.get("follow_up_due", 0)}</button>
            </div>
          </div>
        </section>

        <section class="queue-toolbar">
          <div class="urgency-legend">
            <span class="legend-chip overdue">Overdue = first touch</span>
            <span class="legend-chip review">Review = decision needed</span>
            <span class="legend-chip due">Due = routine follow-up</span>
          </div>
          <div class="filter-results" id="filter-results">Showing {total_queue_items} queue items across {len(data.owner_queues)} owners.</div>
        </section>

        <div class="filtered-empty" id="queue-empty-state">No queue items match the current filters. Try a different owner, urgency, or search term.</div>

        {''.join(owner_sections) or f'<section class="owner-card"><p class="empty">{html.escape(empty_queue_message)}</p></section>'}
      </div>
    </div>
    <div class="footer-bar" aria-hidden="true"></div>
    <script>
      const syncButton = document.getElementById("sync-dashboard-button");
      const syncStatus = document.getElementById("sync-status");
      const latestSyncIso = {json.dumps(latest_sync_iso)};
      const dashboardAutoSyncEnabled = {json.dumps(data.sync_auto_enabled)};
      const dashboardSyncMaxAgeMinutes = {int(data.sync_stale_after_minutes)};
      const form = document.getElementById("lead-build-form");
      const status = document.getElementById("run-status");
      const deckForm = document.getElementById("deck-generator-form");
      const deckStatus = document.getElementById("deck-status");
      const deckSubmitButton = document.getElementById("deck-submit-button");
      const deckIncludePlanCheckbox = document.getElementById("deck-include-plan");
      const deckOfferList = document.getElementById("deck-offer-list");
      const deckOfferPayloadInput = document.getElementById("deck-offer-payload-json");
      const deckAddOfferButton = document.getElementById("deck-add-offer");
      const deckRunList = document.getElementById("deck-run-list");
      const deckAnalyticsModal = document.getElementById("deck-analytics-modal");
      const deckAnalyticsClose = document.getElementById("deck-analytics-close");
      const deckAnalyticsSummary = document.getElementById("deck-analytics-summary");
      const deckAnalyticsDaily = document.getElementById("deck-analytics-daily");
      const deckAnalyticsTabs = document.getElementById("deck-analytics-tabs");
      const draftsForm = document.getElementById("gmail-drafts-form");
      const draftsStatus = document.getElementById("drafts-status");
      const draftsResults = document.getElementById("drafts-results");
      const draftsSubmitButton = document.getElementById("drafts-submit-button");
      const draftModeNote = document.getElementById("draft-mode-note");
      const draftsDryRunCheckbox = draftsForm?.querySelector('input[name="dry_run"]');
      const ownerFilter = document.getElementById("owner-filter");
      const searchInput = document.getElementById("queue-search");
      const filterResults = document.getElementById("filter-results");
      const queueEmptyState = document.getElementById("queue-empty-state");
      const urgencyButtons = document.querySelectorAll("#urgency-filter .filter-button");
      let activeUrgency = "all";
      let syncStatusPollHandle = null;
      let syncReloadPending = false;
      let activeDeckAnalytics = null;

      function latestSyncLooksStale() {{
        if (!latestSyncIso) {{
          return true;
        }}
        const parsed = new Date(latestSyncIso);
        if (Number.isNaN(parsed.getTime())) {{
          return true;
        }}
        return (Date.now() - parsed.getTime()) > dashboardSyncMaxAgeMinutes * 60 * 1000;
      }}

      async function fetchSyncStatus() {{
        const response = await fetch("/admin/api/sync-dashboard/status", {{ method: "GET" }});
        const payload = await response.json().catch(() => ({{ detail: "Unable to load sync status." }}));
        if (!response.ok) {{
          throw new Error(payload.detail || "Unable to load sync status.");
        }}
        return payload.details || {{}};
      }}

      function stopSyncPolling() {{
        if (syncStatusPollHandle) {{
          window.clearInterval(syncStatusPollHandle);
          syncStatusPollHandle = null;
        }}
      }}

      function startSyncPolling() {{
        if (syncStatusPollHandle) {{
          return;
        }}
        syncStatusPollHandle = window.setInterval(async () => {{
          try {{
            const details = await fetchSyncStatus();
            if (details.running) {{
              syncStatus.textContent = details.message || "Syncing cached board in the background...";
              return;
            }}
            stopSyncPolling();
            syncStatus.textContent = details.message || "Board sync completed. Reloading...";
            if (!syncReloadPending) {{
              syncReloadPending = true;
              window.setTimeout(() => window.location.reload(), 900);
            }}
          }} catch (error) {{
            stopSyncPolling();
            syncStatus.textContent = error instanceof Error ? error.message : "Unable to track board sync status.";
          }}
        }}, 2500);
      }}

      async function requestDashboardSync(options = {{}}) {{
        const background = options.background !== false;
        const onlyIfStale = options.onlyIfStale === true;
        const response = await fetch(`/admin/api/sync-dashboard?background=${{background ? "true" : "false"}}&only_if_stale=${{onlyIfStale ? "true" : "false"}}`, {{
          method: "POST",
        }});
        const payload = await response.json().catch(() => ({{ detail: "Dashboard sync failed." }}));
        if (!response.ok) {{
          throw new Error(payload.detail || payload.message || "Dashboard sync failed.");
        }}
        const details = payload.details || {{}};
        syncStatus.textContent = details.message || payload.message || "Dashboard sync requested.";
        if (details.running) {{
          startSyncPolling();
        }} else if (details.status === "skipped" && !syncReloadPending) {{
          syncStatus.textContent = details.message || "Board cache is still fresh.";
        }} else if (!syncReloadPending) {{
          syncReloadPending = true;
          window.setTimeout(() => window.location.reload(), 900);
        }}
        return details;
      }}

      function escapeHtml(value) {{
        return String(value ?? "")
          .replaceAll("&", "&amp;")
          .replaceAll("<", "&lt;")
          .replaceAll(">", "&gt;")
          .replaceAll('"', "&quot;")
          .replaceAll("'", "&#39;");
      }}

      function collectOfferPayload() {{
        return Array.from(deckOfferList?.querySelectorAll(".offer-editor") || []).map((editor) => ({{
          enabled: Boolean(editor.querySelector(".offer-enabled")?.checked),
          title: editor.querySelector(".offer-title")?.value || "",
          description: editor.querySelector(".offer-description")?.value || "",
          price: editor.querySelector(".offer-price")?.value || "",
          price_label: editor.querySelector(".offer-price-label")?.value || "",
          commission: editor.querySelector(".offer-commission")?.value || "",
          commission_label: editor.querySelector(".offer-commission-label")?.value || "",
          baseline: editor.querySelector(".offer-baseline")?.value || "",
          baseline_label: editor.querySelector(".offer-baseline-label")?.value || "",
          bonus: editor.querySelector(".offer-bonus")?.value || "",
        }}));
      }}

      function buildOfferEditor(index) {{
        const wrapper = document.createElement("div");
        wrapper.className = "offer-editor";
        wrapper.dataset.offerIndex = String(index);
        wrapper.innerHTML = `
          <div class="offer-editor-top">
            <button type="button" class="offer-editor-toggle" aria-expanded="false">Custom offer ${{index + 1}}</button>
            <label class="checkbox-label"><span>Include</span><span class="toggle-switch"><input type="checkbox" class="offer-enabled" checked /><span aria-hidden="true"></span></span></label>
          </div>
          <div class="offer-editor-body" hidden>
          <div class="offer-editor-grid">
            <label class="full-width">
              Offer title
              <input type="text" class="offer-title" value="Custom offer ${{index + 1}}" />
            </label>
            <label class="full-width">
              Description
              <textarea class="offer-description">Describe the scope, operating model, and why this offer fits the prospect.</textarea>
            </label>
            <label>
              Price
              <input type="text" class="offer-price" value="$TBD" />
            </label>
            <label>
              Price label
              <input type="text" class="offer-price-label" value="Monthly retainer fee" />
            </label>
            <label>
              Commission
              <input type="text" class="offer-commission" value="TBD" />
            </label>
            <label>
              Commission label
              <input type="text" class="offer-commission-label" value="Commission" />
            </label>
            <label>
              Baseline
              <input type="text" class="offer-baseline" value="TBD" />
            </label>
            <label>
              Baseline label
              <input type="text" class="offer-baseline-label" value="Baseline" />
            </label>
            <label class="full-width">
              Bonus / note
              <input type="text" class="offer-bonus" value="" />
            </label>
          </div>
          </div>`;
        return wrapper;
      }}

      function syncOfferEditorTitles() {{
        Array.from(deckOfferList?.querySelectorAll(".offer-editor") || []).forEach((editor, index) => {{
          const titleInput = editor.querySelector(".offer-title");
          const toggle = editor.querySelector(".offer-editor-toggle");
          if (titleInput && toggle) {{
            toggle.textContent = titleInput.value.trim() || `Custom offer ${{index + 1}}`;
          }}
        }});
      }}

      function toggleOfferEditor(editor, forceOpen = null) {{
        const body = editor?.querySelector(".offer-editor-body");
        const toggle = editor?.querySelector(".offer-editor-toggle");
        if (!body || !toggle) {{
          return;
        }}
        const nextOpen = forceOpen == null ? Boolean(body.hidden) : Boolean(forceOpen);
        body.hidden = !nextOpen;
        toggle.setAttribute("aria-expanded", nextOpen ? "true" : "false");
      }}

      function formatDeckChannelLabel(value) {{
        const labels = {{
          amazon: "Amazon",
          shopify: "Shopify",
          tiktok_shop: "TikTok Shop",
          "3pl": "3PL",
          shipping_os: "Shipping OS",
        }};
        return labels[String(value || "").toLowerCase()] || String(value || "").replaceAll("_", " ");
      }}

      function formatDeckDate(value) {{
        if (!value) return "Not available";
        const parsed = new Date(value);
        if (Number.isNaN(parsed.getTime())) return String(value);
        const month = String(parsed.getUTCMonth() + 1).padStart(2, "0");
        const day = String(parsed.getUTCDate()).padStart(2, "0");
        const year = parsed.getUTCFullYear();
        return `${{month}}/${{day}}/${{year}}`;
      }}

      function buildDeckRunHtml(run) {{
        const channels = Array.isArray(run.channels) && run.channels.length ? run.channels : ["amazon", "tiktok_shop", "shopify", "3pl", "shipping_os"];
        const viewUrl = run.view_url || "";
        const safeTitle = escapeHtml(run.design_title || `Run ${{run.id || ""}}`);
        const bulletHtml = channels.map((channel) => `<li>${{escapeHtml(formatDeckChannelLabel(channel))}}</li>`).join("");
        const analyticsPayload = escapeHtml(JSON.stringify(run.view_analytics || {{}}));
        return `
          <article class="deck-run-item">
            <div>
              <strong>${{safeTitle}}</strong>
              <p class="muted">Created ${{escapeHtml(formatDeckDate(run.started_at || ""))}}</p>
              <ul class="deck-run-bullets">${{bulletHtml}}</ul>
            </div>
            <div class="deck-run-links">
              ${{viewUrl ? `<a href="${{escapeHtml(viewUrl)}}?viewer=internal" target="_blank" rel="noreferrer">Open deck</a>` : ""}}
              <button type="button" class="analytics-button" data-analytics='${{analyticsPayload}}'>View analytics</button>
            </div>
          </article>`;
      }}

      function renderDeckAnalyticsDaily(windowKey) {{
        if (!deckAnalyticsDaily || !activeDeckAnalytics) return;
        const internalDaily = activeDeckAnalytics.internal?.daily_counts?.[windowKey] || {{}};
        const externalDaily = activeDeckAnalytics.external?.daily_counts?.[windowKey] || {{}};
        const allDays = Array.from(new Set([...Object.keys(internalDaily), ...Object.keys(externalDaily)])).sort().reverse();
        if (!allDays.length) {{
          deckAnalyticsDaily.innerHTML = "<p class='muted'>No visits recorded for this window yet.</p>";
          return;
        }}
        deckAnalyticsDaily.innerHTML = `<table><thead><tr><th>Date</th><th>Internal</th><th>External</th></tr></thead><tbody>${{allDays.map((day) => `<tr><td>${{escapeHtml(formatDeckDate(day))}}</td><td>${{escapeHtml(String(internalDaily[day] || 0))}}</td><td>${{escapeHtml(String(externalDaily[day] || 0))}}</td></tr>`).join("")}}</tbody></table>`;
      }}

      function openDeckAnalytics(payload) {{
        activeDeckAnalytics = payload || {{}};
        if (deckAnalyticsSummary) {{
          const internal = activeDeckAnalytics.internal || {{}};
          const external = activeDeckAnalytics.external || {{}};
          deckAnalyticsSummary.innerHTML = `
            <article class="analytics-card">
              <h4>Internal views</h4>
              <ul>
                <li>Unique visitors: ${{escapeHtml(String(internal.unique_visitors || 0))}}</li>
                <li>Total visits: ${{escapeHtml(String(internal.total_visits || 0))}}</li>
                <li>First visited: ${{escapeHtml(formatDeckDate(internal.first_viewed_at || ""))}}</li>
                <li>Last visited: ${{escapeHtml(formatDeckDate(internal.last_viewed_at || ""))}}</li>
                <li>Visit length: Not tracked yet</li>
              </ul>
            </article>
            <article class="analytics-card">
              <h4>External views</h4>
              <ul>
                <li>Unique visitors: ${{escapeHtml(String(external.unique_visitors || 0))}}</li>
                <li>Total visits: ${{escapeHtml(String(external.total_visits || 0))}}</li>
                <li>First visited: ${{escapeHtml(formatDeckDate(external.first_viewed_at || ""))}}</li>
                <li>Last visited: ${{escapeHtml(formatDeckDate(external.last_viewed_at || ""))}}</li>
                <li>Visit length: Not tracked yet</li>
              </ul>
            </article>`;
        }}
        deckAnalyticsTabs?.querySelectorAll("button").forEach((button) => button.classList.toggle("is-active", button.dataset.window === "7"));
        renderDeckAnalyticsDaily("7");
        deckAnalyticsModal?.classList.add("is-visible");
        deckAnalyticsModal?.setAttribute("aria-hidden", "false");
      }}

      function updateDraftModeUi() {{
        const previewOnly = Boolean(draftsDryRunCheckbox?.checked);
        if (draftsSubmitButton) {{
          draftsSubmitButton.textContent = previewOnly ? "PREVIEW DRAFTS" : "CREATE DRAFTS";
        }}
        if (draftModeNote) {{
          draftModeNote.innerHTML = previewOnly
            ? "<strong>Preview mode is on.</strong> We will validate rows and show rendered email previews, but nothing will be created in Gmail until you turn preview mode off."
            : "<strong>Create mode is on.</strong> Clicking the button will create Gmail drafts for all valid rows in this upload."
        }}
      }}

      function applyQueueFilters() {{
        const selectedOwner = ownerFilter?.value || "all";
        const searchTerm = (searchInput?.value || "").trim().toLowerCase();
        const ownerCards = document.querySelectorAll(".owner-card[data-owner]");
        let visibleOwners = 0;
        let visibleItemsTotal = 0;

        ownerCards.forEach((card) => {{
          const ownerName = card.dataset.owner || "";
          const displayLimit = Number(card.dataset.displayLimit || 4);
          const itemNodes = Array.from(card.querySelectorAll(".action-item"));
          const showMoreButton = card.querySelector(".show-more-button");
          const isExpanded = showMoreButton?.dataset.expanded === "true";
          const matchedItems = [];

          itemNodes.forEach((item) => {{
            const matchesOwner = selectedOwner === "all" || ownerName === selectedOwner;
            const matchesUrgency = activeUrgency === "all" || item.dataset.urgency === activeUrgency;
            const matchesSearch = !searchTerm || (item.dataset.search || "").includes(searchTerm);
            const shouldMatch = matchesOwner && matchesUrgency && matchesSearch;
            if (shouldMatch) {{
              matchedItems.push(item);
            }}
            item.style.display = "none";
            item.classList.remove("is-collapsed-by-limit");
          }});

          matchedItems.forEach((item, index) => {{
            const hiddenByLimit = !isExpanded && index >= displayLimit;
            item.classList.toggle("is-collapsed-by-limit", hiddenByLimit);
            item.style.display = hiddenByLimit ? "none" : "";
          }});

          const visibleItems = isExpanded ? matchedItems.length : Math.min(displayLimit, matchedItems.length);
          const hiddenItems = Math.max(0, matchedItems.length - displayLimit);

          if (showMoreButton) {{
            if (matchedItems.length > displayLimit) {{
              showMoreButton.hidden = false;
              showMoreButton.textContent = isExpanded ? "Show fewer" : `Show ${{hiddenItems}} more`;
            }} else {{
              showMoreButton.hidden = true;
              showMoreButton.dataset.expanded = "false";
            }}
          }}

          const visibleCountNode = card.querySelector(".owner-visible-count");
          if (visibleCountNode) {{
            visibleCountNode.textContent = String(visibleItems);
          }}

          card.style.display = visibleItems > 0 ? "" : "none";
          if (visibleItems > 0) {{
            visibleOwners += 1;
            visibleItemsTotal += visibleItems;
          }}
        }});

        if (filterResults) {{
          filterResults.textContent = visibleItemsTotal
            ? `Showing ${{visibleItemsTotal}} queue items across ${{visibleOwners}} owners.`
            : "No queue items match the current filters.";
        }}
        if (queueEmptyState) {{
          queueEmptyState.classList.toggle("is-visible", visibleItemsTotal === 0);
        }}
      }}

      ownerFilter?.addEventListener("change", applyQueueFilters);
      searchInput?.addEventListener("input", applyQueueFilters);
      urgencyButtons.forEach((button) => {{
        button.addEventListener("click", () => {{
          activeUrgency = button.dataset.urgency || "all";
          urgencyButtons.forEach((node) => node.classList.toggle("is-active", node === button));
          applyQueueFilters();
        }});
      }});
      document.querySelectorAll(".show-more-button").forEach((button) => {{
        button.addEventListener("click", () => {{
          button.dataset.expanded = button.dataset.expanded === "true" ? "false" : "true";
          applyQueueFilters();
        }});
      }});

      syncButton?.addEventListener("click", async () => {{
        syncStatus.textContent = "Refreshing board cache...";
        try {{
          await requestDashboardSync({{ background: true, onlyIfStale: false }});
        }} catch (error) {{
          syncStatus.textContent = error instanceof Error ? error.message : "Dashboard sync failed before a response came back.";
        }}
      }});

      window.setTimeout(async () => {{
        if (!dashboardAutoSyncEnabled) {{
          return;
        }}
        try {{
          const details = await fetchSyncStatus();
          if (details.running) {{
            syncStatus.textContent = details.message || "Refreshing stale board in the background...";
            startSyncPolling();
            return;
          }}
          if (details.stale || latestSyncLooksStale()) {{
            await requestDashboardSync({{ background: true, onlyIfStale: true }});
          }}
        }} catch (error) {{
          syncStatus.textContent = error instanceof Error ? error.message : "Unable to auto-refresh the board.";
        }}
      }}, 150);

      form?.addEventListener("submit", async (event) => {{
        event.preventDefault();
        status.textContent = "Queueing lead build...";
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
          const payloadJson = await response.json().catch(() => ({{ detail: "Lead build failed." }}));
          if (!response.ok) {{
            status.textContent = payloadJson.message || payloadJson.detail || payloadJson.error_type || "Lead build failed.";
            return;
          }}

          const runId = payloadJson.details?.run_id;
          const pollUrl = payloadJson.details?.poll_url;
          const downloadUrl = payloadJson.details?.download_url;
          if (!runId || !pollUrl) {{
            status.textContent = "Lead build queued, but the run ID was missing.";
            return;
          }}

          status.textContent = `Lead build queued. Run ID: ${{runId}}. Waiting for completion...`;

          const pollRun = async () => {{
            const statusResponse = await fetch(pollUrl, {{ method: "GET" }});
            const statusPayload = await statusResponse.json().catch(() => ({{ detail: "Lead run polling failed." }}));
            if (!statusResponse.ok) {{
              status.textContent = statusPayload.detail || "Lead run polling failed.";
              return true;
            }}

            const details = statusPayload.details || {{}};
            const runStatus = details.status || "unknown";
            const currentStage = details.current_stage || "queued";
            const summary = details.summary || {{}};

            if (runStatus === "completed") {{
              const contactsFound = summary.personal_contacts_found || 0;
              if (details.has_csv && downloadUrl) {{
                const csvResponse = await fetch(downloadUrl, {{ method: "GET" }});
                if (csvResponse.ok) {{
                  const blob = await csvResponse.blob();
                  const disposition = csvResponse.headers.get("content-disposition") || "";
                  const match = disposition.match(/filename=\"([^\"]+)\"/);
                  const filename = match ? match[1] : "instantly_upload.csv";
                  const url = URL.createObjectURL(blob);
                  const anchor = document.createElement("a");
                  anchor.href = url;
                  anchor.download = filename;
                  document.body.appendChild(anchor);
                  anchor.click();
                  anchor.remove();
                  URL.revokeObjectURL(url);
                  status.textContent = `Lead build finished. CSV download started. Contacts selected: ${{contactsFound}}.`;
                  return true;
                }}
              }}

              status.textContent = summary.message || `Lead build finished. No CSV produced. Contacts selected: ${{contactsFound}}.`;
              return true;
            }}

            if (runStatus === "failed") {{
              status.textContent = details.error_message || "Lead build failed.";
              return true;
            }}

            status.textContent = `Lead build running. Stage: ${{currentStage}}...`;
            return false;
          }};

          for (let attempt = 0; attempt < 180; attempt += 1) {{
            const done = await pollRun();
            if (done) {{
              return;
            }}
            await new Promise((resolve) => window.setTimeout(resolve, 2000));
          }}

          status.textContent = "Lead build is still running. Refresh later to check the run status.";
        }} catch (error) {{
          status.textContent = "Lead build failed before a response came back.";
        }}
      }});

      deckForm?.addEventListener("submit", async (event) => {{
        event.preventDefault();
        if (deckSubmitButton) {{
          deckSubmitButton.disabled = true;
          deckSubmitButton.textContent = "GENERATING...";
        }}
        deckStatus.innerHTML = "Generating deck. This can take a minute...";
        if (deckOfferPayloadInput) {{
          deckOfferPayloadInput.value = JSON.stringify(collectOfferPayload());
        }}
        const formData = new FormData(deckForm);
        formData.delete("include_recommended_plan");
        formData.append("include_recommended_plan", deckIncludePlanCheckbox?.checked ? "true" : "false");
        ["amazon", "tiktok_shop", "shopify", "3pl", "shipping_os"].forEach((channel) => formData.append("channels", channel));
        try {{
          const response = await fetch("/admin/api/generate-deck", {{
            method: "POST",
            body: formData,
          }});
          const payload = await response.json().catch(() => ({{ detail: "Deck generation failed." }}));
          if (!response.ok) {{
            deckStatus.textContent = payload.detail || payload.message || "Deck generation failed.";
            if (deckSubmitButton) {{
              deckSubmitButton.disabled = false;
              deckSubmitButton.textContent = "GENERATE DECK";
            }}
            return;
          }}
          const details = payload.details || {{}};
          const openUrl = details.view_url ? `${{details.view_url}}?viewer=internal` : "";
          const createdRun = {{
            id: details.run_id,
            design_title: details.design_title,
            view_url: details.view_url,
            channels: ["amazon", "tiktok_shop", "shopify", "3pl", "shipping_os"],
            started_at: new Date().toISOString(),
            view_analytics: {{
              internal: {{ unique_visitors: 0, total_visits: 0, first_viewed_at: "", last_viewed_at: "", daily_counts: {{ "7": {{}}, "30": {{}}, "90": {{}}, "all": {{}} }} }},
              external: {{ unique_visitors: 0, total_visits: 0, first_viewed_at: "", last_viewed_at: "", daily_counts: {{ "7": {{}}, "30": {{}}, "90": {{}}, "all": {{}} }} }},
            }},
          }};
          if (deckRunList) {{
            const empty = deckRunList.querySelector(".empty");
            if (empty) empty.remove();
            deckRunList.insertAdjacentHTML("afterbegin", buildDeckRunHtml(createdRun));
          }}
          if (openUrl) {{
            window.open(openUrl, "_blank", "noopener,noreferrer");
          }}
          deckStatus.innerHTML = `Deck generated. ${{openUrl ? `<a href="${{openUrl}}" target="_blank" rel="noreferrer">Open deck</a>` : ""}}`;
          if (deckSubmitButton) {{
            deckSubmitButton.disabled = false;
            deckSubmitButton.textContent = "GENERATE DECK";
          }}
        }} catch (error) {{
          deckStatus.textContent = "Deck generation failed before a response came back.";
          if (deckSubmitButton) {{
            deckSubmitButton.disabled = false;
            deckSubmitButton.textContent = "GENERATE DECK";
          }}
        }}
      }});

      deckAddOfferButton?.addEventListener("click", () => {{
        if (!deckOfferList) {{
          return;
        }}
        const nextIndex = deckOfferList.querySelectorAll(".offer-editor").length;
        deckOfferList.appendChild(buildOfferEditor(nextIndex));
        syncOfferEditorTitles();
      }});

      deckOfferList?.addEventListener("click", (event) => {{
        const target = event.target;
        if (!(target instanceof HTMLElement)) return;
        const toggle = target.closest(".offer-editor-toggle");
        if (!toggle) return;
        const editor = toggle.closest(".offer-editor");
        toggleOfferEditor(editor);
      }});

      deckOfferList?.addEventListener("input", (event) => {{
        const target = event.target;
        if (!(target instanceof HTMLElement)) return;
        if (target.classList.contains("offer-title")) {{
          syncOfferEditorTitles();
        }}
      }});

      document.addEventListener("click", (event) => {{
        const target = event.target;
        if (!(target instanceof HTMLElement)) return;
        const analyticsButton = target.closest(".analytics-button");
        if (analyticsButton) {{
          try {{
            openDeckAnalytics(JSON.parse(analyticsButton.getAttribute("data-analytics") || "{{}}"));
          }} catch (_error) {{
            openDeckAnalytics({{}});
          }}
        }}
      }});

      deckAnalyticsClose?.addEventListener("click", () => {{
        deckAnalyticsModal?.classList.remove("is-visible");
        deckAnalyticsModal?.setAttribute("aria-hidden", "true");
      }});

      deckAnalyticsModal?.addEventListener("click", (event) => {{
        if (event.target === deckAnalyticsModal) {{
          deckAnalyticsModal.classList.remove("is-visible");
          deckAnalyticsModal.setAttribute("aria-hidden", "true");
        }}
      }});

      deckAnalyticsTabs?.querySelectorAll("button").forEach((button) => {{
        button.addEventListener("click", () => {{
          deckAnalyticsTabs.querySelectorAll("button").forEach((node) => node.classList.toggle("is-active", node === button));
          renderDeckAnalyticsDaily(button.dataset.window || "7");
        }});
      }});

      syncOfferEditorTitles();

      draftsForm?.addEventListener("submit", async (event) => {{
        event.preventDefault();
        const previewOnly = Boolean(draftsDryRunCheckbox?.checked);
        draftsStatus.textContent = previewOnly ? "Preparing Gmail preview..." : "Creating Gmail drafts...";
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
          blocks.push(`
            <div class="draft-summary-grid">
              <div class="draft-summary-card">
                <span>Total rows</span>
                <strong>${{details.rows_total || 0}}</strong>
                <small>Rows detected in the uploaded CSV.</small>
              </div>
              <div class="draft-summary-card">
                <span>Prepared</span>
                <strong>${{details.prepared || 0}}</strong>
                <small>Rows that were valid enough to turn into drafts.</small>
              </div>
              <div class="draft-summary-card">
                <span>Created</span>
                <strong>${{details.created || 0}}</strong>
                <small>${{details.dry_run ? "Preview mode does not create drafts." : "Drafts successfully created in Gmail."}}</small>
              </div>
              <div class="draft-summary-card">
                <span>Failed</span>
                <strong>${{details.failed || 0}}</strong>
                <small>Rows that need fixes before they can be drafted.</small>
              </div>
            </div>
          `);
          if (Array.isArray(details.available_placeholders) && details.available_placeholders.length) {{
            blocks.push(`
              <div class="result-block">
                <strong>Available placeholders (${{details.available_placeholders.length}})</strong>
                <div>${{details.available_placeholders.map((item) => `<span class="source">${{escapeHtml(item)}}</span>`).join(" ")}}</div>
              </div>
            `);
          }}
          if (Array.isArray(details.previews) && details.previews.length) {{
            const previewHeading = details.dry_run ? "Preview drafts" : "Created draft content";
            const previewMeta = details.previewed < details.prepared
              ? `Showing the first ${{details.previewed}} rendered emails out of ${{details.prepared}} prepared rows.`
              : `Showing all ${{details.previewed}} rendered emails.`;
            blocks.push(`
              <div class="result-block">
                <strong>${{previewHeading}}</strong>
                <div class="result-meta">${{previewMeta}}</div>
                <div class="preview-card-list">
                  ${{details.previews.map((item) => `
                    <article class="preview-card">
                      <div class="preview-card-head">
                        <div>
                          <h4>${{escapeHtml(item.email)}}</h4>
                          <p>Row ${{item.row_number}}${{item.first_name || item.last_name ? ` · ${{escapeHtml([item.first_name, item.last_name].filter(Boolean).join(" "))}}` : ""}}${{item.company ? ` · ${{escapeHtml(item.company)}}` : ""}}</p>
                        </div>
                        <div class="preview-card-tags">
                          <span class="draft-chip">${{item.body_length || 0}} chars</span>
                          <span class="draft-chip">${{details.dry_run ? "Preview" : "Drafted"}}</span>
                        </div>
                      </div>
                      <div class="preview-subject"><strong>Subject:</strong> ${{escapeHtml(item.subject)}}</div>
                      <pre class="preview-body">${{escapeHtml(item.body || "")}}</pre>
                    </article>
                  `).join("")}}
                </div>
              </div>
            `);
          }}
          if (Array.isArray(details.created_rows) && details.created_rows.length) {{
            blocks.push(`
              <div class="result-block">
                <strong>Created draft records</strong>
                <div class="created-card-list">
                  ${{details.created_rows.map((item) => `
                    <article class="created-card">
                      <div class="created-card-head">
                        <div>
                          <h4>${{escapeHtml(item.email)}}</h4>
                          <p>Row ${{item.row_number}}</p>
                        </div>
                        <div class="created-card-tags">
                          <span class="draft-chip success">Draft created</span>
                        </div>
                      </div>
                      <p><strong>Subject:</strong> ${{escapeHtml(item.subject)}}</p>
                      <p><strong>Draft ID:</strong> ${{escapeHtml(item.draft_id || "n/a")}}${{item.message_id ? ` · <strong>Message ID:</strong> ${{escapeHtml(item.message_id)}}` : ""}}</p>
                    </article>
                  `).join("")}}
                </div>
              </div>
            `);
          }}
          if (Array.isArray(details.failed_rows) && details.failed_rows.length) {{
            const failureItems = details.failed_rows.map((item) => {{
              const emailPart = item.email ? ` (${{escapeHtml(item.email)}})` : "";
              return `<li>Row ${{item.row_number}}${{emailPart}}: ${{escapeHtml(item.error)}}</li>`;
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
      draftsDryRunCheckbox?.addEventListener("change", updateDraftModeUi);
      updateDraftModeUi();
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
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Montserrat:wght@700;800&display=swap" rel="stylesheet">
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
        font-family: "Inter", "Segoe UI", sans-serif;
      }}
      a {{ color: var(--dark-blue); text-decoration: none; }}
      {render_agent_nav_styles()}
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
        font-family: "Inter", "Segoe UI", sans-serif;
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
        font-family: "Inter", "Segoe UI", sans-serif;
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
        .shell {{
          padding: 24px 14px 56px;
        }}
        .workspace {{
          padding: 18px;
        }}
        .page-title {{
          font-size: 42px;
        }}
      }}
    </style>
  </head>
  <body>
    {render_agent_nav("executive")}
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
            <p><strong>Days since last touch:</strong> ${{item.days_since_touch ?? "n/a"}} | <strong>Last-touch source:</strong> ${{item.last_touch_source || "n/a"}} | <strong>Value:</strong> ${{item.value_numeric !== null && item.value_numeric !== undefined ? formatCurrency(item.value_numeric) : (item.value_label || "n/a")}}</p>
            <p><strong>Latest context:</strong> ${{item.context_summary || "No recent comment or inbox context captured."}}</p>
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
      window.setTimeout(() => window.location.reload(), 60 * 60 * 1000);
    </script>
  </body>
</html>"""
