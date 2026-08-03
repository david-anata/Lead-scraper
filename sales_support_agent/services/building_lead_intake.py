"""Deterministic Building lead alerts and staff follow-up plans."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from sales_support_agent.integrations.slack import SlackClient


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
            ],
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "Open *Agent → Building → Sales* to review, respond, and complete the event interview.",
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
