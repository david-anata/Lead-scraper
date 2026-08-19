"""Deterministic Building lead alerts and staff follow-up plans."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from sales_support_agent.integrations.slack import SlackClient


def _detail(details: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = str(details.get(key) or "").strip()
        if value:
            return value
    return ""


def event_candidate_dates(
    preferred_date: Any,
    details: dict[str, Any],
) -> list[str]:
    """Return the primary and optional backup dates without duplicates."""

    preferred = (
        preferred_date.isoformat()
        if hasattr(preferred_date, "isoformat")
        else str(preferred_date or "").strip()
    )
    values = [
        preferred,
        _detail(details, "alternateDate", "alternate_date"),
        _detail(details, "backupDate2", "backup_date_2"),
    ]
    return list(dict.fromkeys(value for value in values if value))[:3]


def prefill_event_interview(
    *,
    preferred_date: Any,
    details: dict[str, Any],
) -> tuple[dict[str, str], list[str]]:
    """Map existing prospect answers into the staff interview without completing it."""

    dates = event_candidate_dates(preferred_date, details)
    guest_start = _detail(details, "guestStartTime", "guest_start_time")
    guest_end = _detail(details, "guestEndTime", "guest_end_time")
    access_start = _detail(details, "accessStartTime", "access_start_time")
    access_end = _detail(details, "accessEndTime", "access_end_time")
    guest_schedule = ""
    if guest_start or guest_end:
        guest_schedule = f"{guest_start or 'Start not provided'}–{guest_end or 'End not provided'}"
    access_schedule = ""
    if access_start or access_end:
        access_schedule = f"{access_start or 'Start not provided'}–{access_end or 'End not provided'}"
    elif _detail(details, "setupRequired", "setup_required").lower() in {
        "1",
        "true",
        "yes",
        "on",
    }:
        access_schedule = "Setup or vendor access requested before guests arrive."
    values = {
        "event_format": _detail(details, "eventType", "event_type"),
        "candidate_dates": "; ".join(dates),
        "guest_schedule": guest_schedule,
        "access_schedule": access_schedule,
        "attendance": _detail(details, "groupSize", "attendance"),
        "catering": _detail(details, "catering"),
        "alcohol": _detail(details, "alcohol"),
        "av_and_sound": _detail(details, "avNeeds", "av_needs"),
        "accessibility": _detail(
            details, "accessibilityNeeds", "accessibility_needs"
        ),
        "vendors_and_load_in": _detail(details, "vendorPlan", "vendor_plan"),
        "special_requests": _detail(details, "notes"),
    }
    answers = {key: value for key, value in values.items() if value}
    return answers, sorted(answers)


#: Only what a contract actually needs to exist. Event purpose, format and the
#: agreed next step are good sales hygiene and are still prompted for, but the
#: package builder never reads them, so gating a contract on them stopped work
#: for no gain.
EVENT_QUALIFICATION_REQUIREMENTS = (
    ("candidate_dates", "candidate dates"),
    ("guest_schedule", "guest schedule"),
    ("attendance", "attendance"),
)


def event_qualification_missing(
    interview: dict[str, Any], details: dict[str, Any] | None = None
) -> list[str]:
    """Return the smallest evidence set required before event qualification."""

    missing = [
        label
        for key, label in EVENT_QUALIFICATION_REQUIREMENTS
        if not str(interview.get(key) or "").strip()
    ]
    raw_details = dict(details or {})
    setup_requested = _detail(
        raw_details, "setupRequired", "setup_required"
    ).lower() in {"1", "true", "yes", "on"}
    if setup_requested and not str(interview.get("access_schedule") or "").strip():
        missing.append("setup and teardown access")
    if (
        _detail(raw_details, "alcohol").casefold() == "yes"
        and not str(interview.get("alcohol") or "").strip()
    ):
        missing.append("alcohol plan")
    return missing


def build_follow_up_sequence(received_at: datetime, response_sla_hours: int) -> list[dict[str, Any]]:
    """Return an internal sequence; these steps never send customer messages."""

    return [
        {
            "key": "review",
            "label": "Review the request and assign an owner",
            "due_at": received_at.isoformat(),
            "status": "due",
        },
        {
            "key": "first_response",
            "label": "Make the first personal response",
            "due_at": (received_at + timedelta(hours=response_sla_hours)).isoformat(),
            "status": "queued",
        },
        {
            "key": "interview",
            "label": "Complete the event interview and date review",
            "due_at": (received_at + timedelta(days=1)).isoformat(),
            "status": "queued",
        },
        {
            "key": "follow_up",
            "label": "Follow up if the prospect has not decided",
            "due_at": (received_at + timedelta(days=3)).isoformat(),
            "status": "queued",
        },
        {
            "key": "close_loop",
            "label": "Close the loop or record the next agreed date",
            "due_at": (received_at + timedelta(days=7)).isoformat(),
            "status": "queued",
        },
    ]


def advance_follow_up_sequence(
    sequence: list[dict[str, Any]],
    *,
    lifecycle_stage: str,
    changed_at: datetime,
    interview_complete: bool = False,
) -> list[dict[str, Any]]:
    """Advance only steps supported by recorded staff evidence."""

    completed = {"review"}
    if lifecycle_stage in {"responded", "qualified", "closed_won", "closed_lost"}:
        completed.add("first_response")
    if interview_complete:
        completed.add("interview")
    terminal = lifecycle_stage in {"closed_won", "closed_lost"}
    result: list[dict[str, Any]] = []
    for raw in sequence:
        step = dict(raw)
        key = str(step.get("key") or "")
        if key in completed:
            step["status"] = "completed"
            step.setdefault("completed_at", changed_at.isoformat())
        elif terminal:
            step["status"] = "closed"
            step.setdefault("completed_at", changed_at.isoformat())
        result.append(step)
    return result


def notify_new_building_lead(settings: Any, inquiry: Any) -> dict[str, Any]:
    """Notify staff in Slack without changing whether the inquiry is accepted."""

    client = SlackClient(settings)
    if not client.is_configured():
        return {"status": "not_configured", "provider": "slack"}
    preferred = inquiry.preferred_date.isoformat() if inquiry.preferred_date else "No date supplied"
    details = dict(inquiry.payload_json or {})
    candidate_dates = event_candidate_dates(inquiry.preferred_date, details)
    event_type = _detail(details, "eventType", "event_type") or "Not supplied"
    attendance = _detail(details, "groupSize", "attendance") or "Not supplied"
    fallback = (
        f"New Building {inquiry.kind} lead: {inquiry.name} · {preferred} · "
        f"owner {inquiry.assigned_owner}"
    )
    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "New Anata Building lead"},
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Prospect*\n{inquiry.name}"},
                {"type": "mrkdwn", "text": f"*Journey*\n{inquiry.kind.title()}"},
                {"type": "mrkdwn", "text": f"*Preferred date*\n{preferred}"},
                {"type": "mrkdwn", "text": f"*Owner*\n{inquiry.assigned_owner}"},
                {
                    "type": "mrkdwn",
                    "text": f"*Candidate dates*\n{', '.join(candidate_dates) or 'Not supplied'}",
                },
                {"type": "mrkdwn", "text": f"*Event type*\n{event_type}"},
                {"type": "mrkdwn", "text": f"*Attendance*\n{attendance}"},
            ],
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"Open <https://agent.anatainc.com/admin/building/inquiries/{inquiry.id}|"
                    "this lead in Agent> to review the original submission, "
                    "respond, and complete the event interview."
                ),
            },
        },
    ]
    result = client.post_message(text=fallback, blocks=blocks)
    return {
        "status": "delivered" if result.get("ok") else "failed",
        "provider": "slack",
        "provider_reference": str(result.get("ts") or ""),
        "reason": str(result.get("reason") or ""),
    }
