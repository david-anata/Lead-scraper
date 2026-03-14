"""Pure helpers for working with raw Instantly webhook payloads."""

from __future__ import annotations

import hashlib
from datetime import date, datetime
from typing import Any


SUPPORTED_EVENT_MAP = {
    "email_sent": "outbound_email_sent",
    "reply_received": "inbound_reply_received",
    "lead_meeting_completed": "meeting_completed",
    "lead_meeting_booked": "note_logged",
    "lead_interested": "note_logged",
    "lead_not_interested": "note_logged",
    "lead_neutral": "note_logged",
}


def extract_email(payload: dict[str, Any]) -> str:
    return str(
        payload.get("lead_email")
        or payload.get("email")
        or payload.get("lead", {}).get("email", "")
    ).strip()


def build_external_event_key(payload: dict[str, Any]) -> str:
    pieces = [
        str(payload.get("event_type") or ""),
        str(payload.get("timestamp") or ""),
        str(payload.get("lead_email") or payload.get("email") or ""),
        str(payload.get("email_id") or ""),
        str(payload.get("campaign_id") or ""),
    ]
    raw_key = "|".join(pieces)
    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()


def parse_occurred_at(payload: dict[str, Any]) -> datetime | None:
    raw = str(payload.get("timestamp") or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def _truncate(text: str, limit: int = 400) -> str:
    normalized = " ".join((text or "").split())
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 3].rstrip() + "..."


def _step_description(payload: dict[str, Any]) -> str:
    step = payload.get("step")
    variant = payload.get("variant")
    parts = []
    if step is not None:
        parts.append(f"step {step}")
    if variant is not None:
        parts.append(f"variant {variant}")
    return ", ".join(parts)


def build_summary(payload: dict[str, Any]) -> str:
    event_type = str(payload.get("event_type") or "")
    step_info = _step_description(payload)
    if event_type == "email_sent":
        subject = str(payload.get("email_subject") or "").strip()
        base = "Instantly sent an outbound email"
        if subject:
            base += f' with subject "{subject}"'
        if step_info:
            base += f" ({step_info})"
        return base + "."
    if event_type == "reply_received":
        reply = str(payload.get("reply_text_snippet") or payload.get("reply_text") or "").strip()
        if reply:
            return f"Instantly recorded a reply: {_truncate(reply)}"
        return "Instantly recorded an inbound reply."
    if event_type == "lead_meeting_booked":
        return "Instantly marked the lead as meeting booked."
    if event_type == "lead_meeting_completed":
        return "Instantly marked the lead as meeting completed."
    if event_type == "lead_interested":
        return "Instantly marked the lead as interested."
    if event_type == "lead_not_interested":
        return "Instantly marked the lead as not interested."
    if event_type == "lead_neutral":
        return "Instantly marked the lead as neutral."
    return f"Instantly event received: {event_type or 'unknown'}."


def build_outcome(payload: dict[str, Any]) -> str:
    event_type = str(payload.get("event_type") or "")
    if event_type == "lead_meeting_completed":
        return _truncate(str(payload.get("reply_text_snippet") or payload.get("reply_text") or "").strip())
    return ""


def build_recommended_next_action(payload: dict[str, Any]) -> str:
    event_type = str(payload.get("event_type") or "")
    if event_type == "email_sent":
        return "Monitor for a reply and follow up based on the current ClickUp status timing."
    if event_type == "reply_received":
        return "Review the reply, respond promptly, and update the next step in ClickUp."
    if event_type == "lead_meeting_booked":
        return "Prepare for the meeting and log the outcome after it happens."
    if event_type == "lead_meeting_completed":
        return "Log meeting notes and confirm the next follow-up step in ClickUp."
    if event_type == "lead_interested":
        return "Review the conversation and move the lead forward in ClickUp if appropriate."
    if event_type == "lead_not_interested":
        return "Review the conversation and decide whether the lead should move to a closed status."
    if event_type == "lead_neutral":
        return "Review the conversation and decide the next follow-up step."
    return "Review the Instantly activity and update ClickUp if needed."


def build_suggested_reply_draft(payload: dict[str, Any]) -> str:
    event_type = str(payload.get("event_type") or "")
    if event_type == "reply_received":
        return (
            "Thanks for the reply. I appreciate the update and would love to keep this moving. "
            "Let me know the best next step and I’ll take care of it."
        )
    if event_type == "lead_interested":
        return (
            "Thanks for the interest. I’d be happy to walk you through the next step and share the right details. "
            "What timing works best on your side?"
        )
    if event_type == "lead_meeting_booked":
        return (
            "Thanks for booking time. I’m looking forward to the conversation and will come prepared with the right context."
        )
    if event_type == "lead_meeting_completed":
        return (
            "Thanks again for the conversation. I’m summarizing the next steps and will follow up with exactly what we discussed."
        )
    return ""


def build_suggested_status(payload: dict[str, Any]) -> str:
    event_type = str(payload.get("event_type") or "")
    if event_type == "lead_interested":
        return "CONTACTED WARM"
    if event_type == "lead_not_interested":
        return "LOST"
    return ""


def build_next_follow_up_date(payload: dict[str, Any]) -> date | None:
    raw = str(payload.get("meeting_start") or payload.get("meeting_date") or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).date()
    except ValueError:
        return None
