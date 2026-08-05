"""Versioned, evidence-gated transactional messages for event bookings."""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import NAMESPACE_URL, uuid5
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.orm import Session

from sales_support_agent.integrations.resend import ResendClient
from sales_support_agent.models.entities import (
    BuildingAgreement,
    BuildingAuditEvent,
    BuildingCommunicationPreference,
    BuildingContact,
    BuildingInvoice,
    BuildingProposal,
    BuildingReservation,
    BuildingTransactionalMessage,
)
from sales_support_agent.services.building_inquiry_receipt import receipt_delivery_ready
from sales_support_agent.services.building_sender import building_from_address


TEMPLATES: dict[str, dict[str, str | int]] = {
    "quote_sent": {
        "version": 1,
        "subject": "Your Anata Building event quote",
        "lead": "Your event quote is ready for review. Your date is not booked yet.",
    },
    "agreement_signed": {
        "version": 1,
        "subject": "Your Anata Building agreement is complete",
        "lead": "We recorded the completed agreement from our signature provider. Required payment and final confirmation may still remain.",
    },
    "invoice_ready": {
        "version": 1,
        "subject": "Your Anata Building invoice is available",
        "lead": "Your QuickBooks invoice is available for review and payment. The booking is confirmed only after all required evidence is complete.",
    },
    "payment_received": {
        "version": 1,
        "subject": "We received your Anata Building payment",
        "lead": "QuickBooks shows the required payment as cleared. We will confirm the booking after the remaining booking checks pass.",
    },
    "booking_confirmed": {
        "version": 1,
        "subject": "Your Anata Building event is confirmed",
        "lead": "Your event booking is confirmed in Anata Agent. Please review the event and access windows below.",
    },
    "booking_changed": {
        "version": 1,
        "subject": "Your Anata Building event details changed",
        "lead": "Your event details were updated after staff review. Please review the current dates and status below.",
    },
    "booking_cancelled": {
        "version": 1,
        "subject": "Your Anata Building event request was cancelled",
        "lead": "Your event request is now closed. Reply to this email if you need help understanding the recorded status.",
    },
    "event_reminder": {
        "version": 1,
        "subject": "Reminder for your Anata Building event",
        "lead": "Your event is coming up. Please review the current event and access windows below.",
    },
    "post_event": {
        "version": 1,
        "subject": "Thank you for hosting at Anata Building",
        "lead": "Thank you for hosting your event with us. Reply to this email if you have a post-event question.",
    },
}
MOUNTAIN = ZoneInfo("America/Denver")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _latest(session: Session, model: Any, reservation_id: str) -> Any:
    return session.execute(
        select(model)
        .where(model.reservation_id == reservation_id)
        .order_by(model.created_at.desc())
    ).scalars().first()


def _evidence_ready(session: Session, reservation: BuildingReservation, milestone: str) -> bool:
    if milestone == "quote_sent":
        quote = session.execute(
            select(BuildingProposal)
            .where(BuildingProposal.reservation_id == reservation.id)
            .order_by(BuildingProposal.version.desc())
        ).scalars().first()
        return bool(quote and quote.status in {"sent", "accepted"})
    if milestone == "agreement_signed":
        agreement = _latest(session, BuildingAgreement, reservation.id)
        return bool(agreement and agreement.status == "signed" and agreement.provider_reference)
    if milestone == "invoice_ready":
        invoice = _latest(session, BuildingInvoice, reservation.id)
        return bool(invoice and invoice.provider == "quickbooks" and invoice.status in {"open", "paid"})
    if milestone == "payment_received":
        return reservation.deposit_status == "paid"
    if milestone in {"booking_confirmed", "event_reminder"}:
        return reservation.status in {"confirmed", "pre_event"}
    if milestone == "booking_cancelled":
        return reservation.status == "cancelled"
    if milestone == "post_event":
        return reservation.status == "completed"
    if milestone == "booking_changed":
        change = session.execute(
            select(BuildingAuditEvent).where(
                BuildingAuditEvent.entity_type == "reservation",
                BuildingAuditEvent.entity_id == reservation.id,
                BuildingAuditEvent.action == "event_booking_changed",
            )
        ).scalars().first()
        return bool(change and reservation.status not in {"cancelled", "expired"})
    return False


def _status_url(request: Any, reservation: BuildingReservation, contact: BuildingContact) -> str:
    from sales_support_agent.api.building_booking_router import _encode_customer_status_token

    expires_at = (_now() + timedelta(days=30)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    token = _encode_customer_status_token(
        request,
        reservation_id=reservation.id,
        contact_id=contact.id,
        expires_at=expires_at,
    )
    base = str(request.app.state.settings.building_public_base_url).rstrip("/")
    return f"{base}/event-status?token={token}"


def attempt_booking_message(
    session: Session,
    *,
    request: Any,
    reservation: BuildingReservation,
    milestone: str,
    actor: str,
) -> dict[str, Any]:
    """Send one approved milestone message; failures never change booking state."""

    template = TEMPLATES.get(milestone)
    if template is None:
        raise ValueError("Unsupported booking message milestone.")
    contact = session.get(BuildingContact, reservation.contact_id) if reservation.contact_id else None
    if contact is None or contact.status != "active":
        return {"status": "blocked", "reason": "Active customer contact is missing."}
    if not _evidence_ready(session, reservation, milestone):
        return {"status": "blocked", "reason": "Authoritative milestone evidence is missing."}
    preference = session.get(BuildingCommunicationPreference, contact.id)
    if preference is not None and not preference.transactional_allowed:
        return {"status": "suppressed", "reason": "Transactional contact is disabled."}

    version = int(template["version"])
    reference = f"building-transactional:{milestone}:v{version}"
    message_id = str(uuid5(NAMESPACE_URL, f"{reservation.id}:{milestone}:v{version}"))
    row = session.get(BuildingTransactionalMessage, message_id)
    if row is not None and row.status in {"sent", "delivered", "delivery_delayed"}:
        return {"status": row.status, "provider_reference": row.provider_message_id}
    status_access_error = ""
    if row is None:
        try:
            status_url = _status_url(request, reservation, contact)
        except Exception:
            status_url = ""
            status_access_error = "Signed customer status access is not configured."
        starts = (reservation.guest_starts_at or reservation.starts_at).astimezone(MOUNTAIN)
        ends = (reservation.guest_ends_at or reservation.ends_at).astimezone(MOUNTAIN)
        body = (
            f"Hi {contact.full_name.split(' ', 1)[0] or 'there'},\n\n"
            f"{template['lead']}\n\n"
            f"Event: {starts.strftime('%b %d, %Y at %I:%M %p')} to "
            f"{ends.strftime('%b %d, %Y at %I:%M %p')} MT\n"
            + (f"Current status and documents: {status_url}\n\n" if status_url else "")
            + "The Anata Team"
        )
        checksum = hashlib.sha256(f"{template['subject']}\n{body}".encode()).hexdigest()
        row = BuildingTransactionalMessage(
            id=message_id,
            reservation_id=reservation.id,
            contact_id=contact.id,
            milestone=milestone,
            template_version=version,
            template_reference=reference,
            to_email=contact.email,
            subject=str(template["subject"]),
            body_text=body,
            content_checksum=checksum,
        )
        session.add(row)
        session.flush()
    else:
        checksum = row.content_checksum
        if not row.body_text.strip() or not checksum:
            return {"status": "blocked", "reason": "Frozen message evidence is incomplete."}
    row.attempted_at = _now()
    ready, reason = receipt_delivery_ready(request.app.state.settings)
    if status_access_error:
        ready, reason = False, status_access_error
    if not ready:
        row.status = "not_configured"
        row.last_error = reason
    else:
        try:
            row.provider_message_id = ResendClient(request.app.state.settings).send_message(
                to=(contact.email,),
                subject=row.subject,
                text=row.body_text,
                reply_to=building_from_address(),
                from_address=building_from_address(),
                cc=(),
                idempotency_key=f"building-booking:{reservation.id}:{milestone}:v{version}",
            )
            row.status = "sent"
            row.sent_at = _now()
            row.last_error = ""
        except Exception as exc:
            row.status = "failed"
            row.last_error = str(exc)[:1000]
    row.updated_at = _now()
    session.add(row)
    session.add(BuildingAuditEvent(
        entity_type="transactional_message",
        entity_id=row.id,
        action=f"booking_message_{row.status}",
        actor=actor,
        after_json={
            "reservation_id": reservation.id,
            "milestone": milestone,
            "template_reference": reference,
            "content_checksum": checksum,
            "provider_reference": row.provider_message_id,
            "claims_evidence_checked": True,
        },
    ))
    return {
        "status": row.status,
        "provider_reference": row.provider_message_id,
        "reason": row.last_error,
    }
