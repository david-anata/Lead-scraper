"""Authenticated Resend delivery feedback for Building campaigns."""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import time
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from sqlalchemy import select

from sales_support_agent.models.database import session_scope
from sales_support_agent.models.entities import (
    BuildingAuditEvent,
    BuildingCampaignRecipient,
    BuildingCommunicationPreference,
    BuildingContact,
    BuildingEmailEvent,
    BuildingSuppression,
)


router = APIRouter(prefix="/api/integrations/resend", tags=["resend-webhook"])
SUPPORTED_EVENTS = {
    "email.delivered",
    "email.bounced",
    "email.complained",
    "email.delivery_delayed",
    "email.failed",
}
SUPPRESSION_EVENTS = {
    "email.bounced": "bounce",
    "email.complained": "complaint",
}
RECIPIENT_STATUSES = {
    "email.delivered": "delivered",
    "email.bounced": "bounced",
    "email.complained": "complained",
    "email.delivery_delayed": "delivery_delayed",
    "email.failed": "failed",
}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _webhook_secret(request: Request) -> str:
    secret = str(
        getattr(request.app.state.settings, "resend_webhook_secret", "") or ""
    ).strip()
    if not secret:
        raise HTTPException(
            status_code=503,
            detail="Resend webhook verification is not configured.",
        )
    return secret


def _secret_bytes(secret: str) -> bytes:
    encoded = secret.removeprefix("whsec_")
    try:
        return base64.b64decode(encoded, validate=True)
    except (ValueError, binascii.Error):
        return secret.encode()


def verify_resend_webhook(
    *,
    raw_body: bytes,
    event_id: str,
    timestamp: str,
    signature_header: str,
    secret: str,
    now_seconds: int | None = None,
) -> None:
    """Verify the exact Svix-signed body and reject stale replay attempts."""

    if not event_id or not timestamp or not signature_header:
        raise HTTPException(status_code=401, detail="Missing Resend webhook signature.")
    try:
        timestamp_seconds = int(timestamp)
    except ValueError as exc:
        raise HTTPException(status_code=401, detail="Invalid Resend webhook timestamp.") from exc
    current = int(time.time() if now_seconds is None else now_seconds)
    if abs(current - timestamp_seconds) > 300:
        raise HTTPException(status_code=401, detail="Resend webhook is outside the five-minute window.")
    signed = b".".join(
        (event_id.encode(), timestamp.encode(), raw_body)
    )
    expected = base64.b64encode(
        hmac.new(_secret_bytes(secret), signed, hashlib.sha256).digest()
    ).decode()
    signatures = {
        value.split(",", 1)[1]
        for value in signature_header.split()
        if value.startswith("v1,") and "," in value
    }
    if not any(hmac.compare_digest(expected, candidate) for candidate in signatures):
        raise HTTPException(status_code=401, detail="Invalid Resend webhook signature.")


def _email_from_payload(data: dict[str, Any]) -> str:
    recipients = data.get("to")
    if isinstance(recipients, list) and recipients:
        return str(recipients[0] or "").strip().lower()[:255]
    return str(recipients or data.get("email") or "").strip().lower()[:255]


@router.post("/webhook")
async def ingest_resend_webhook(request: Request) -> dict[str, Any]:
    raw_body = await request.body()
    event_id = str(request.headers.get("svix-id") or "").strip()
    timestamp = str(request.headers.get("svix-timestamp") or "").strip()
    signature = str(request.headers.get("svix-signature") or "").strip()
    verify_resend_webhook(
        raw_body=raw_body,
        event_id=event_id,
        timestamp=timestamp,
        signature_header=signature,
        secret=_webhook_secret(request),
    )
    if len(event_id) > 255:
        raise HTTPException(status_code=400, detail="Resend event ID is too long.")
    try:
        payload = json.loads(raw_body)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="Invalid Resend webhook payload.") from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Invalid Resend webhook payload.")
    event_type = str(payload.get("type") or "").strip().lower()[:64]
    data = payload.get("data")
    if not isinstance(data, dict):
        data = {}
    provider_message_id = str(
        data.get("email_id") or data.get("id") or ""
    ).strip()[:255]
    email = _email_from_payload(data)

    with session_scope(request.app.state.session_factory) as session:
        existing = session.execute(
            select(BuildingEmailEvent).where(
                BuildingEmailEvent.provider_event_id == event_id
            )
        ).scalar_one_or_none()
        if existing is not None:
            return {
                "ok": True,
                "duplicate": True,
                "event_type": existing.event_type,
                "status": existing.status,
            }

        recipient = None
        if provider_message_id:
            recipient = session.execute(
                select(BuildingCampaignRecipient).where(
                    BuildingCampaignRecipient.provider_message_id
                    == provider_message_id
                )
            ).scalar_one_or_none()
        if recipient is None and email:
            recipient = session.execute(
                select(BuildingCampaignRecipient)
                .where(BuildingCampaignRecipient.email == email)
                .order_by(BuildingCampaignRecipient.created_at.desc())
                .limit(1)
            ).scalar_one_or_none()

        supported = event_type in SUPPORTED_EVENTS
        status = "processed" if supported else "ignored"
        event_row = BuildingEmailEvent(
            provider="resend",
            provider_event_id=event_id,
            provider_message_id=provider_message_id,
            event_type=event_type or "unknown",
            email=email,
            campaign_recipient_id=recipient.id if recipient else None,
            status=status,
            payload_json=payload,
            processed_at=_now(),
        )
        session.add(event_row)
        session.flush()

        suppression_reason = SUPPRESSION_EVENTS.get(event_type)
        if recipient is not None and supported:
            recipient.status = RECIPIENT_STATUSES[event_type]
            if suppression_reason:
                recipient.exclusion_reason = (
                    f"Provider reported {suppression_reason}; future marketing is suppressed."
                )
        if suppression_reason and email:
            suppression = session.get(BuildingSuppression, email)
            if suppression is None:
                suppression = BuildingSuppression(
                    email=email,
                    scope="marketing",
                    reason=suppression_reason,
                    source="resend_webhook",
                )
            else:
                suppression.scope = "marketing"
                suppression.reason = suppression_reason
                suppression.source = "resend_webhook"
            session.add(suppression)
            contact = session.execute(
                select(BuildingContact).where(BuildingContact.email == email)
            ).scalar_one_or_none()
            if contact is not None and event_type == "email.complained":
                preference = session.get(
                    BuildingCommunicationPreference, contact.id
                )
                if preference is None:
                    preference = BuildingCommunicationPreference(contact_id=contact.id)
                preference.marketing_status = "unsubscribed"
                preference.marketing_source = "resend_complaint"
                preference.marketing_changed_at = _now()
                preference.updated_by = "resend-webhook"
                preference.updated_at = _now()
                session.add(preference)
        session.add(BuildingAuditEvent(
            entity_type="email_event",
            entity_id=str(event_row.id),
            action="processed" if supported else "ignored",
            actor="resend-webhook",
            after_json={
                "provider_event_id": event_id,
                "provider_message_id": provider_message_id,
                "event_type": event_type,
                "email": email,
                "campaign_recipient_id": recipient.id if recipient else None,
                "suppression_reason": suppression_reason or "",
            },
        ))
        return {
            "ok": True,
            "duplicate": False,
            "event_type": event_type,
            "status": status,
            "recipient_matched": recipient is not None,
            "suppressed": bool(suppression_reason and email),
        }
