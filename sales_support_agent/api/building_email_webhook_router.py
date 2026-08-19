"""Authenticated Resend delivery feedback for Building campaigns."""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import logging
import re
import time
from datetime import datetime, timezone
from typing import Any, Sequence

from fastapi import APIRouter, HTTPException, Request
from sqlalchemy import select

from sales_support_agent.models.database import session_scope
from sales_support_agent.models.entities import (
    BuildingAuditEvent,
    BuildingCampaignRecipient,
    BuildingCommunicationPreference,
    BuildingContact,
    BuildingEmailEvent,
    BuildingInquiry,
    BuildingInquiryReceipt,
    BuildingTransactionalMessage,
    BuildingSuppression,
)


logger = logging.getLogger(__name__)
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


def _webhook_secrets(request: Request) -> tuple[str, ...]:
    """Every signing secret this endpoint will accept.

    Resend signs with a secret that belongs to the endpoint, not to the
    account, so one URL reachable from two configured endpoints needs both
    secrets. That is the ordinary state of affairs twice over: while a signing
    secret is being rotated, and after a move to new hosting when the old
    endpoint has not been deleted yet. Accepting a list means neither of those
    silently throws delivery feedback away.

    RESEND_WEBHOOK_SECRET may therefore hold several secrets separated by
    commas or whitespace.
    """

    configured = str(
        getattr(request.app.state.settings, "resend_webhook_secret", "") or ""
    )
    secrets = tuple(part for part in re.split(r"[,\s]+", configured) if part)
    if not secrets:
        raise HTTPException(
            status_code=503,
            detail="Resend webhook verification is not configured.",
        )
    return secrets


def _secret_bytes(secret: str) -> bytes:
    encoded = secret.removeprefix("whsec_")
    try:
        return base64.b64decode(encoded, validate=True)
    except (ValueError, binascii.Error):
        return secret.encode()


def _reject(reason: str, *, event_id: str, detail: str) -> HTTPException:
    """Refuse the delivery and say why in the log.

    Every rejection here used to be an indistinguishable 401. A wrong signing
    secret, a replayed delivery and a genuinely forged one produced the same
    silent line, so a misconfigured endpoint could throw away delivery feedback
    for weeks and look exactly like background noise. The event ID is Resend's
    own identifier for the delivery; the signature and the secret are never
    written down.
    """

    logger.warning(
        "resend_webhook_rejected reason=%s event_id=%s", reason, event_id or "missing"
    )
    return HTTPException(status_code=401, detail=detail)


def verify_resend_webhook(
    *,
    raw_body: bytes,
    event_id: str,
    timestamp: str,
    signature_header: str,
    secret: str | Sequence[str],
    now_seconds: int | None = None,
) -> None:
    """Verify the exact Svix-signed body and reject stale replay attempts.

    ``secret`` accepts several signing secrets, because one URL can be served
    by more than one configured Resend endpoint and each signs with its own.
    """

    secrets = (secret,) if isinstance(secret, str) else tuple(secret)
    secrets = tuple(candidate for candidate in secrets if candidate)
    if not secrets:
        raise HTTPException(
            status_code=503,
            detail="Resend webhook verification is not configured.",
        )
    if not event_id or not timestamp or not signature_header:
        raise _reject(
            "missing_signature_headers",
            event_id=event_id,
            detail="Missing Resend webhook signature.",
        )
    try:
        timestamp_seconds = int(timestamp)
    except ValueError as exc:
        raise _reject(
            "unreadable_timestamp",
            event_id=event_id,
            detail="Invalid Resend webhook timestamp.",
        ) from exc
    current = int(time.time() if now_seconds is None else now_seconds)
    if abs(current - timestamp_seconds) > 300:
        # Usually a retry of a delivery that was already refused for another
        # reason, so the log says how stale it is: a steady stream of these
        # means something rejected the first attempt, not that Resend is late.
        raise _reject(
            f"outside_five_minute_window skew_seconds={current - timestamp_seconds}",
            event_id=event_id,
            detail="Resend webhook is outside the five-minute window.",
        )
    signed = b".".join(
        (event_id.encode(), timestamp.encode(), raw_body)
    )
    signatures = {
        value.split(",", 1)[1]
        for value in signature_header.split()
        if value.startswith("v1,") and "," in value
    }
    for candidate in secrets:
        expected = base64.b64encode(
            hmac.new(_secret_bytes(candidate), signed, hashlib.sha256).digest()
        ).decode()
        if any(hmac.compare_digest(expected, offered) for offered in signatures):
            return
    raise _reject(
        f"no_configured_secret_matched secrets_tried={len(secrets)}",
        event_id=event_id,
        detail="Invalid Resend webhook signature.",
    )


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
        secret=_webhook_secrets(request),
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
        receipt = None
        transactional = None
        if provider_message_id:
            recipient = session.execute(
                select(BuildingCampaignRecipient).where(
                    BuildingCampaignRecipient.provider_message_id
                    == provider_message_id
                )
            ).scalar_one_or_none()
            receipt = session.execute(
                select(BuildingInquiryReceipt).where(
                    BuildingInquiryReceipt.provider_message_id == provider_message_id
                )
            ).scalar_one_or_none()
            transactional = session.execute(
                select(BuildingTransactionalMessage).where(
                    BuildingTransactionalMessage.provider_message_id
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
        if receipt is not None and supported:
            receipt.status = RECIPIENT_STATUSES[event_type]
            receipt.updated_at = _now()
            if event_type == "email.delivered":
                receipt.delivered_at = _now()
                receipt.last_error = ""
            elif event_type in {"email.bounced", "email.complained", "email.failed"}:
                receipt.last_error = f"Provider reported {event_type.removeprefix('email.').replace('_', ' ')}."
            inquiry = session.get(BuildingInquiry, receipt.inquiry_id)
            if inquiry is not None:
                inquiry_payload = dict(inquiry.payload_json or {})
                inquiry_payload["_customer_receipt"] = {
                    **dict(inquiry_payload.get("_customer_receipt") or {}),
                    "status": receipt.status,
                    "delivered_at": receipt.delivered_at.isoformat() if receipt.delivered_at else "",
                    "reason": receipt.last_error,
                }
                inquiry.payload_json = inquiry_payload
                inquiry.updated_at = _now()
        if transactional is not None and supported:
            transactional.status = RECIPIENT_STATUSES[event_type]
            transactional.updated_at = _now()
            if event_type == "email.delivered":
                transactional.delivered_at = _now()
                transactional.last_error = ""
            elif event_type in {"email.bounced", "email.complained", "email.failed"}:
                transactional.last_error = (
                    f"Provider reported {event_type.removeprefix('email.').replace('_', ' ')}."
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
                "inquiry_id": receipt.inquiry_id if receipt else None,
                "transactional_message_id": transactional.id if transactional else None,
                "suppression_reason": suppression_reason or "",
            },
        ))
        return {
            "ok": True,
            "duplicate": False,
            "event_type": event_type,
            "status": status,
            "recipient_matched": (
                recipient is not None or receipt is not None or transactional is not None
            ),
            "suppressed": bool(suppression_reason and email),
        }
