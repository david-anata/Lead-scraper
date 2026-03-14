"""Pure helpers for notification urgency and formatting."""

from __future__ import annotations


STALE_URGENCY_OVERDUE = "overdue"
STALE_URGENCY_IMMEDIATE_REVIEW = "needs_immediate_review"
STALE_URGENCY_FOLLOW_UP_DUE = "follow_up_due"

STALE_URGENCY_ORDER = (
    STALE_URGENCY_OVERDUE,
    STALE_URGENCY_IMMEDIATE_REVIEW,
    STALE_URGENCY_FOLLOW_UP_DUE,
)

STALE_URGENCY_LABELS = {
    STALE_URGENCY_OVERDUE: "Overdue",
    STALE_URGENCY_IMMEDIATE_REVIEW: "Needs immediate review",
    STALE_URGENCY_FOLLOW_UP_DUE: "Follow-up due",
}


def classify_stale_assessment_state(state: str) -> str:
    normalized = (state or "").strip().lower()
    if normalized == "overdue":
        return STALE_URGENCY_OVERDUE
    if normalized in {"new_and_untouched", "missing_next_step"}:
        return STALE_URGENCY_IMMEDIATE_REVIEW
    return STALE_URGENCY_FOLLOW_UP_DUE


def determine_stale_notification_mode(urgency: str, immediate_alert_urgencies: tuple[str, ...]) -> str:
    normalized = (urgency or "").strip().lower()
    allowed = {(item or "").strip().lower() for item in immediate_alert_urgencies}
    if normalized in allowed:
        return "immediate_alert"
    return "digest_only"


def build_clickup_owner_reference(assignee_name: str) -> str:
    name = (assignee_name or "").strip()
    if not name:
        return "Assigned AE"
    return f"@{name}"
