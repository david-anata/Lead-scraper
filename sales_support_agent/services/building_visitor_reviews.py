"""Front-desk star taps and private notes for The Anata Building.

Every star tap is stored. Scores of 4 and 5 are the people sent to Google.
Scores of 1 to 3 can carry a private note. Alerts follow new Building
inquiries: Slack on the agent channel, and email to the Building inbox when
Resend is configured.
"""

from __future__ import annotations

import logging
import re
import secrets
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from sales_support_agent.integrations.resend import ResendClient
from sales_support_agent.integrations.slack import SlackClient
from sales_support_agent.models.entities import BuildingAuditEvent, BuildingVisitorReview
from sales_support_agent.services.building_sender import building_from_address

logger = logging.getLogger(__name__)

GOOGLE_REVIEW_URL = "https://g.page/r/CUcMOut7Sd-pEBM/review"
GOOGLE_REDIRECT_SECONDS = 2
# The QR sits on one shared front-desk network, so the cap has to leave room
# for a busy lobby while still stopping a script.
STAR_RATE_LIMIT = 120
FEEDBACK_RATE_LIMIT = 60
RATE_WINDOW_SECONDS = 600
MAX_COMMENT_LENGTH = 2000
MAX_NAME_LENGTH = 120
MAX_CONTACT_LENGTH = 160
ADMIN_ROW_LIMIT = 40
BUILDING_ADDRESS = "1657 N State Street, Lehi, UT 84043"
ADMIN_REVIEWS_URL = (
    "https://agent.anatainc.com/admin/building/operations#front-desk-reviews"
)

VISIT_REASONS: dict[str, str] = {
    "event": "Event",
    "office_tenant": "Office or tenant",
    "coworking": "Coworking",
    "warehouse": "Warehouse",
    "other": "Other",
}

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_PHONE_RE = re.compile(r"^[0-9+\-().\s]+$")
_ID_RE = re.compile(r"^[A-Za-z0-9_-]{8,64}$")
_MOUNTAIN = ZoneInfo("America/Denver")
_ALERT_SENT = {"pending", "delivered", "partial"}


def new_review_id() -> str:
    return secrets.token_urlsafe(18)


def parse_stars(value: Any) -> int | None:
    text = str(value or "").strip()
    if text not in {"1", "2", "3", "4", "5"}:
        return None
    return int(text)


def is_public_google_score(stars: int) -> bool:
    return stars >= 4


def clean_text(value: Any, *, limit: int) -> tuple[str, str]:
    """Return stripped text and an error when it is longer than the limit."""

    text = str(value or "").replace("\x00", "").strip()
    if len(text) > limit:
        return "", f"Please keep this under {limit} characters."
    return text, ""


def clean_contact(value: Any) -> tuple[str, str]:
    text, error = clean_text(value, limit=MAX_CONTACT_LENGTH)
    if error or not text:
        return text, error
    if "@" in text:
        if not _EMAIL_RE.fullmatch(text) or len(text) > 254:
            return "", "Use an email address or a phone number, or leave this blank."
        return text, ""
    digits = re.sub(r"\D", "", text)
    if not _PHONE_RE.fullmatch(text) or len(digits) < 7 or len(digits) > 15:
        return "", "Use an email address or a phone number, or leave this blank."
    return text, ""


def clean_reasons(values: Any) -> list[str]:
    raw = values if isinstance(values, (list, tuple)) else [values]
    chosen: list[str] = []
    for item in raw:
        key = str(item or "").strip()
        if key in VISIT_REASONS and key not in chosen:
            chosen.append(key)
    return chosen


def reason_labels(keys: list[str]) -> str:
    return ", ".join(VISIT_REASONS[key] for key in keys if key in VISIT_REASONS)


def record_star(session: Session, stars: int) -> BuildingVisitorReview:
    now = datetime.now(timezone.utc)
    row = BuildingVisitorReview(
        id=new_review_id(),
        stars=stars,
        comment="",
        visitor_name="",
        contact="",
        visit_reasons_json=[],
        feedback_submitted=False,
        notification_status="",
        notification_json={},
        created_at=now,
        updated_at=now,
    )
    session.add(row)
    session.add(
        BuildingAuditEvent(
            entity_type="visitor_review",
            entity_id=row.id,
            action="star_recorded",
            actor="front-desk",
            after_json={"stars": stars},
        )
    )
    return row


def load_review(session: Session, review_id: str) -> BuildingVisitorReview | None:
    if not _ID_RE.fullmatch(review_id):
        return None
    return session.get(BuildingVisitorReview, review_id)


def save_private_feedback(
    session: Session,
    row: BuildingVisitorReview,
    *,
    stars: int,
    comment: str,
    visitor_name: str,
    contact: str,
    reasons: list[str],
) -> bool:
    """Store the note. Return whether the team still needs an alert."""

    now = datetime.now(timezone.utc)
    should_alert = (
        not is_public_google_score(stars)
        and row.notification_status not in _ALERT_SENT
    )
    row.stars = stars
    row.comment = comment
    row.visitor_name = visitor_name
    row.contact = contact
    row.visit_reasons_json = reasons
    row.updated_at = now
    if is_public_google_score(stars):
        action = "stars_updated"
    else:
        row.feedback_submitted = True
        action = "feedback_recorded"
        if should_alert:
            row.notification_status = "pending"
    session.add(row)
    session.add(
        BuildingAuditEvent(
            entity_type="visitor_review",
            entity_id=row.id,
            action=action,
            actor="front-desk",
            after_json={
                "stars": stars,
                "feedback_submitted": bool(row.feedback_submitted),
            },
        )
    )
    return should_alert


def _slack_text(value: str) -> str:
    return (
        str(value or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def notify_visitor_feedback(settings: Any, row: BuildingVisitorReview) -> dict[str, str]:
    """Alert the way new Building inquiries do, without failing the visitor."""

    reasons = reason_labels(list(row.visit_reasons_json or [])) or "Not given"
    name = row.visitor_name or "Not given"
    contact = row.contact or "Not given"
    comment = (row.comment or "").strip()
    result = {
        "slack": "not_configured",
        "email": "not_configured",
        "slack_reference": "",
        "email_reference": "",
    }
    fallback = (
        f"Anata Building front desk note: {row.stars} stars from {name}. {comment}"
    )
    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "Anata Building front desk note"},
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Stars*\n{row.stars} of 5"},
                {"type": "mrkdwn", "text": f"*Name*\n{_slack_text(name)}"},
                {"type": "mrkdwn", "text": f"*Contact*\n{_slack_text(contact)}"},
                {"type": "mrkdwn", "text": f"*Here for*\n{_slack_text(reasons)}"},
            ],
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*What we could do better*\n{_slack_text(comment[:1500])}",
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"Open <{ADMIN_REVIEWS_URL}|front desk reviews in Agent>.",
            },
        },
    ]
    client = SlackClient(settings)
    try:
        posted = client.post_message(text=fallback[:3000], blocks=blocks)
        if posted.get("ok"):
            result["slack"] = "delivered"
            result["slack_reference"] = str(posted.get("ts") or "")
        elif posted.get("skipped"):
            result["slack"] = "not_configured"
        else:
            result["slack"] = "failed"
    except Exception as exc:  # The note is already stored.
        logger.exception("Front desk Slack alert failed for %s", row.id)
        result["slack"] = "failed"
        result["slack_reference"] = str(exc)[:180]

    resend = ResendClient(settings)
    inbox = building_from_address()
    if resend.is_configured() and inbox:
        reply_to = contact if "@" in contact and contact != "Not given" else ""
        body = (
            "A visitor left a private note from the front desk page.\n\n"
            f"Stars: {row.stars} of 5\n"
            f"Name: {name}\n"
            f"Contact: {contact}\n"
            f"Here for: {reasons}\n\n"
            f"{comment}\n\n"
            f"Open {ADMIN_REVIEWS_URL}\n"
        )
        try:
            message_id = resend.send_message(
                to=inbox,
                subject=f"Front desk note · {row.stars} stars · The Anata Building",
                text=body,
                reply_to=reply_to,
                idempotency_key=f"building-review-{row.id}",
            )
            result["email"] = "delivered"
            result["email_reference"] = message_id
        except Exception as exc:
            logger.exception("Front desk email alert failed for %s", row.id)
            result["email"] = "failed"
            result["email_reference"] = str(exc)[:180]
    return result


def notification_outcome(result: dict[str, str]) -> str:
    slack = result.get("slack") or "not_configured"
    email = result.get("email") or "not_configured"
    delivered = {slack, email} & {"delivered"}
    failed = {slack, email} & {"failed"}
    if delivered and failed:
        return "partial"
    if delivered:
        return "delivered"
    if failed:
        return "failed"
    return "not_configured"


def record_notification(
    session: Session,
    review_id: str,
    result: dict[str, str],
) -> None:
    row = session.get(BuildingVisitorReview, review_id)
    if row is None:
        return
    row.notification_status = notification_outcome(result)
    row.notification_json = dict(result)
    row.updated_at = datetime.now(timezone.utc)
    session.add(row)


def _mountain_label(value: datetime | None) -> str:
    if value is None:
        return ""
    aware = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    return aware.astimezone(_MOUNTAIN).strftime("%b %d, %Y · %I:%M %p MT")


def admin_review_snapshot(session: Session) -> dict[str, Any]:
    """Recent taps plus the all-time rating mix for Building Control."""

    counts = {
        int(stars): int(count)
        for stars, count in session.execute(
            select(BuildingVisitorReview.stars, func.count())
            .group_by(BuildingVisitorReview.stars)
        ).all()
        if stars is not None
    }
    by_stars = {star: counts.get(star, 0) for star in range(1, 6)}
    total = sum(by_stars.values())
    private_notes = int(
        session.execute(
            select(func.count())
            .select_from(BuildingVisitorReview)
            .where(BuildingVisitorReview.feedback_submitted.is_(True))
        ).scalar_one()
    )
    average = ""
    if total:
        weighted = sum(star * count for star, count in by_stars.items())
        average = f"{weighted / total:.1f}"
    rows = session.execute(
        select(BuildingVisitorReview)
        .order_by(BuildingVisitorReview.created_at.desc())
        .limit(ADMIN_ROW_LIMIT)
    ).scalars().all()
    return {
        "total": total,
        "private_notes": private_notes,
        "average": average,
        "by_stars": by_stars,
        "rows": [
            {
                "id": row.id,
                "stars": row.stars,
                "when": _mountain_label(row.created_at),
                "comment": row.comment or "",
                "name": row.visitor_name or "",
                "contact": row.contact or "",
                "visit_reasons": reason_labels(list(row.visit_reasons_json or [])),
                "kind": "Private note" if row.feedback_submitted else "Star only",
            }
            for row in rows
        ],
    }
