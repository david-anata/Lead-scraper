"""Reusable action-summary and reply-draft helpers."""

from __future__ import annotations

from datetime import date, datetime


def format_date_label(value: date | datetime | None) -> str:
    if value is None:
        return "unknown"
    if isinstance(value, datetime):
        return value.date().isoformat()
    return value.isoformat()


def trim_for_slack(text: str, *, limit: int = 180) -> str:
    normalized = " ".join((text or "").split())
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 3].rstrip() + "..."


def display_first_name(task_name: str = "", email: str = "") -> str:
    raw = (task_name or "").strip()
    if raw:
        return raw.split()[0]
    local = (email or "").split("@", 1)[0].replace(".", " ").replace("_", " ").strip()
    if local:
        return local.split()[0].title()
    return "there"


def build_stale_action_summary(*, task_name: str, status: str, last_touch_label: str, next_step: str, urgency_label: str) -> str:
    return (
        f"{urgency_label} for {task_name} in {status}. "
        f"Last meaningful touch was {last_touch_label}. "
        f"Action: {next_step}"
    )


def build_stale_reply_draft(*, task_name: str, status: str, next_step: str, as_of_date: date) -> str:
    first_name = display_first_name(task_name)
    return (
        f"Hi {first_name}, following up on {task_name} as of {as_of_date.isoformat()}. "
        f"I'd love to keep things moving. {next_step}"
    )


def build_event_action_summary(
    *,
    classification: str,
    occurred_at: date | datetime | None,
    recommended_next_action: str,
) -> str:
    occurred = format_date_label(occurred_at)
    label_map = {
        "reply_received": "Lead replied",
        "pricing_or_offer_request": "Lead asked for pricing or an offer",
        "meeting_action_needed": "Lead email needs meeting follow-up",
        "lead_source_email": "Lead-source email arrived",
        "triage_unmatched": "Unmatched lead-related email needs review",
    }
    prefix = label_map.get(classification, "Lead activity needs review")
    return f"{prefix} on {occurred}. Action: {recommended_next_action}"


def build_event_reply_draft(
    *,
    classification: str,
    task_name: str,
    sender_name: str = "",
    sender_email: str = "",
    occurred_at: date | datetime | None = None,
) -> str:
    first_name = display_first_name(sender_name or task_name, sender_email)
    occurred = format_date_label(occurred_at)
    if classification == "pricing_or_offer_request":
        return (
            f"Hi {first_name}, thanks for the note on {occurred}. "
            f"Happy to send pricing and the right next-step options for {task_name}. "
            f"What timeline are you working against?"
        )
    if classification == "meeting_action_needed":
        return (
            f"Hi {first_name}, thanks for the update on {occurred}. "
            f"I'm happy to coordinate the next meeting step for {task_name}. "
            f"What time works best for you?"
        )
    if classification == "lead_source_email":
        return (
            f"Hi {first_name}, thanks for sending this over. "
            f"I’m reviewing the details now and will route the right next step shortly."
        )
    if classification == "triage_unmatched":
        return (
            f"Hi {first_name}, thanks for the note. "
            f"We’re reviewing ownership and the right next step now, and we’ll follow up shortly."
        )
    return (
        f"Hi {first_name}, thanks for the reply on {occurred}. "
        f"I appreciate the quick response on {task_name}. "
        f"I’ll keep this moving and follow up with the most helpful next step."
    )
