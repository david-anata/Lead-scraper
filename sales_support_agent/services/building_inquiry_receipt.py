"""Idempotent, non-blocking receipt for a newly accepted Building inquiry."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.orm import Session

from sales_support_agent.integrations.resend import ResendClient
from sales_support_agent.models.entities import (
    BuildingAuditEvent,
    BuildingInquiry,
    BuildingInquiryReceipt,
)
from sales_support_agent.services.building_sender import (
    building_cc,
    building_from_address,
)


RECEIPT_SUBJECT = "We received your event inquiry"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def receipt_body(name: str) -> str:
    """Return the owner-approved plain receipt without a booking promise."""

    first_name = str(name or "").strip().split(" ", 1)[0] or "there"
    return (
        f"Hi {first_name},\n\n"
        "Thank you for your interest in hosting your event at The Arena. "
        "We received your inquiry and will review the details. "
        "We’ll be in contact soon.\n\n"
        "The Anata Team"
    )


def receipt_delivery_ready(settings: Any) -> tuple[bool, str]:
    """Require credentials and verified delivery feedback before automation."""

    client = ResendClient(settings)
    if not client.is_configured(from_address=building_from_address()):
        return False, "Customer email credentials or sender verification are missing."
    if not str(getattr(settings, "resend_webhook_secret", "") or "").strip():
        return False, "Customer email delivery feedback is not configured."
    return True, ""


def _public_payload(row: BuildingInquiryReceipt) -> dict[str, Any]:
    return {
        "status": row.status,
        "provider": row.provider,
        "provider_reference": row.provider_message_id,
        "reason": row.last_error,
        "attempted_at": row.attempted_at.isoformat() if row.attempted_at else "",
        "sent_at": row.sent_at.isoformat() if row.sent_at else "",
        "delivered_at": row.delivered_at.isoformat() if row.delivered_at else "",
    }


def attempt_inquiry_receipt(
    session: Session,
    *,
    settings: Any,
    inquiry: BuildingInquiry,
    actor: str,
) -> dict[str, Any]:
    """Send once; failures never roll back or reject the accepted inquiry."""

    row = session.get(BuildingInquiryReceipt, inquiry.id)
    if row is not None and row.status in {"sent", "delivered", "delivery_delayed"}:
        return _public_payload(row)

    body = receipt_body(inquiry.name)
    checksum = hashlib.sha256(
        f"{RECEIPT_SUBJECT}\n{body}".encode("utf-8")
    ).hexdigest()
    if row is None:
        row = BuildingInquiryReceipt(
            inquiry_id=inquiry.id,
            status="queued",
            to_email=inquiry.email,
            subject=RECEIPT_SUBJECT,
            content_checksum=checksum,
        )
    row.attempted_at = _now()
    row.updated_at = row.attempted_at
    ready, reason = receipt_delivery_ready(settings)
    if not ready:
        row.status = "not_configured"
        row.last_error = reason
    else:
        try:
            message_id = ResendClient(settings).send_message(
                to=(inquiry.email,),
                subject=RECEIPT_SUBJECT,
                text=body,
                reply_to=building_from_address(),
                from_address=building_from_address(),
                cc=building_cc(exclude=(inquiry.email,)),
                idempotency_key=f"building-inquiry-receipt:{inquiry.id}:v1",
            )
            row.status = "sent"
            row.provider_message_id = message_id
            row.last_error = ""
            row.sent_at = _now()
            row.updated_at = row.sent_at
        except Exception as exc:  # The lead must survive a provider outage.
            row.status = "failed"
            row.last_error = str(exc)[:1000]
            row.updated_at = _now()
    session.add(row)
    payload = _public_payload(row)
    inquiry_payload = dict(inquiry.payload_json or {})
    inquiry_payload["_customer_receipt"] = payload
    inquiry.payload_json = inquiry_payload
    inquiry.updated_at = _now()
    session.add(inquiry)
    session.add(BuildingAuditEvent(
        entity_type="inquiry",
        entity_id=inquiry.id,
        action=f"customer_receipt_{row.status}",
        actor=actor,
        after_json={
            "status": row.status,
            "provider": row.provider,
            "provider_reference": row.provider_message_id,
            "content_checksum": row.content_checksum,
            "availability_claimed": False,
            "booking_claimed": False,
        },
    ))
    return payload
