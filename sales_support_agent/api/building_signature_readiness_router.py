"""Governed provider-neutral signature-request readiness.

This module freezes who would sign which approved agreement. It never calls an
e-sign provider, sends email, creates a provider request, or records a
signature. The current Google Docs eSignature step remains staff-controlled,
so delivery is a deliberate operator handoff and completion is recorded only
from QuickBooks evidence.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Literal
from urllib.parse import urlencode
from uuid import uuid4

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import RedirectResponse
from sqlalchemy import select

from sales_support_agent.models.database import session_scope
from sales_support_agent.models.entities import (
    BuildingAgreement,
    BuildingAuditEvent,
    BuildingContact,
    BuildingReservation,
    BuildingSignatureRequestReadiness,
)
from sales_support_agent.services.auth_deps import require_tool
from sales_support_agent.services.building_security import (
    require_building_form_security,
)


router = APIRouter(
    prefix="/admin/building/contracts",
    tags=["building-signature-readiness"],
)
FORM_DEPS = [Depends(require_building_form_security)]
TRANSITIONS = {
    "prepared": {"in_review"},
    "in_review": {"approved"},
    "approved": set(),
    "expired": set(),
    "cancelled": set(),
}
QUICKBOOKS_CONTRACT_PROVIDER = "quickbooks_contract_builder"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _aware(value: datetime | None) -> datetime | None:
    if value is None or value.tzinfo is not None:
        return value
    return value.replace(tzinfo=timezone.utc)


def _checksum(snapshot: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(snapshot, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _actor(user: dict[str, Any]) -> str:
    return str(user.get("email") or "building-operator")


def _redirect(
    agreement_id: str, *, notice: str = "", error: str = ""
) -> RedirectResponse:
    query = urlencode({"notice": notice} if notice else {"error": error})
    return RedirectResponse(
        f"/admin/building/contracts/{agreement_id}?{query}",
        status_code=303,
    )


def _active_hold(reservation: BuildingReservation, now: datetime) -> bool:
    expiry = _aware(reservation.hold_expires_at)
    return bool(
        reservation.status == "soft_hold"
        and expiry is not None
        and expiry > now
    )


@router.post(
    "/{agreement_id}/signature-readiness",
    dependencies=FORM_DEPS,
)
def prepare_signature_readiness(
    agreement_id: str,
    request: Request,
    user: dict = Depends(require_tool("building.agreements.prepare")),
) -> RedirectResponse:
    """Freeze a signer handoff for an approved agreement; send nothing."""

    now = _now()
    actor = _actor(user)
    with session_scope(request.app.state.session_factory) as session:
        agreement = session.get(BuildingAgreement, agreement_id)
        if agreement is None:
            return _redirect(agreement_id, error="Contract not found.")
        if (
            agreement.preparation_status != "approved"
            or not agreement.package_checksum
        ):
            return _redirect(
                agreement_id,
                error="Approve the frozen agreement package first.",
            )
        reservation = session.get(
            BuildingReservation, agreement.reservation_id
        )
        if reservation is None or not _active_hold(reservation, now):
            return _redirect(
                agreement_id,
                error="An active temporary hold is required.",
            )
        contact = (
            session.get(BuildingContact, reservation.contact_id)
            if reservation.contact_id
            else None
        )
        if (
            contact is None
            or contact.status != "active"
            or not contact.full_name.strip()
            or not contact.email.strip()
        ):
            return _redirect(
                agreement_id,
                error="An active customer with a name and email is required.",
            )
        snapshot = {
            "agreement_id": agreement.id,
            "agreement_version": agreement.version,
            "agreement_checksum": agreement.package_checksum,
            "reservation_id": reservation.id,
            "signer": {
                "name": contact.full_name,
                "email": contact.email,
                "role": "customer",
            },
            "provider": QUICKBOOKS_CONTRACT_PROVIDER,
            "delivery": "not_sent",
        }
        checksum = _checksum(snapshot)
        existing = session.execute(
            select(BuildingSignatureRequestReadiness).where(
                BuildingSignatureRequestReadiness.agreement_id == agreement.id,
                BuildingSignatureRequestReadiness.version == 1,
            )
        ).scalar_one_or_none()
        if existing is not None:
            if (
                existing.checksum != checksum
                or existing.agreement_checksum != agreement.package_checksum
            ):
                return _redirect(
                    agreement_id,
                    error=(
                        "A different signature snapshot already exists. "
                        "Prepare a new agreement version instead."
                    ),
                )
            return _redirect(
                agreement_id,
                notice=(
                    "Signature readiness already exists; nothing was sent."
                ),
            )
        row = BuildingSignatureRequestReadiness(
            id=f"signature-{uuid4().hex[:32]}",
            reservation_id=reservation.id,
            agreement_id=agreement.id,
            version=1,
            status="prepared",
            signer_name=contact.full_name,
            signer_email=contact.email,
            agreement_checksum=agreement.package_checksum,
            snapshot_json=snapshot,
            checksum=checksum,
            provider=QUICKBOOKS_CONTRACT_PROVIDER,
            delivery_status="not_sent",
            created_by=actor,
            created_at=now,
            updated_at=now,
        )
        session.add(row)
        session.add(BuildingAuditEvent(
            entity_type="signature_request_readiness",
            entity_id=row.id,
            action="signature_request_readiness_prepared",
            actor=actor,
            after_json={
                "agreement_id": agreement.id,
                "agreement_checksum": agreement.package_checksum,
                "signer_email": contact.email,
                "readiness_checksum": checksum,
                "status": "prepared",
                "delivery_status": "not_sent",
                "provider": QUICKBOOKS_CONTRACT_PROVIDER,
                "provider_write": False,
                "message_sent": False,
            },
        ))
    return _redirect(
        agreement_id,
        notice=(
            "QuickBooks contract handoff prepared for review; nothing was sent."
        ),
    )


@router.post(
    "/{agreement_id}/signature-readiness/transition",
    dependencies=FORM_DEPS,
)
def transition_signature_readiness(
    agreement_id: str,
    request: Request,
    target_status: Literal["in_review", "approved"] = Form(...),
    confirmation: str = Form(...),
    user: dict = Depends(require_tool("building.agreements.approve")),
) -> RedirectResponse:
    """Review or approve the frozen handoff; still make no provider call."""

    now = _now()
    actor = _actor(user)
    with session_scope(request.app.state.session_factory) as session:
        agreement = session.get(BuildingAgreement, agreement_id)
        row = session.execute(
            select(BuildingSignatureRequestReadiness).where(
                BuildingSignatureRequestReadiness.agreement_id == agreement_id
            )
        ).scalar_one_or_none()
        if agreement is None or row is None:
            return _redirect(
                agreement_id,
                error="Prepare signature readiness first.",
            )
        reservation = session.get(
            BuildingReservation, agreement.reservation_id
        )
        if reservation is None or not _active_hold(reservation, now):
            before = row.status
            if row.status not in {"expired", "cancelled"}:
                row.status = "expired"
                row.updated_at = now
                session.add(row)
                session.add(BuildingAuditEvent(
                    entity_type="signature_request_readiness",
                    entity_id=row.id,
                    action="signature_request_readiness_expired",
                    actor=actor,
                    before_json={"status": before},
                    after_json={
                        "status": "expired",
                        "delivery_status": row.delivery_status,
                        "provider_write": False,
                        "message_sent": False,
                    },
                ))
            return _redirect(
                agreement_id,
                error="The temporary hold expired; signature readiness is blocked.",
            )
        if agreement.preparation_status != "approved":
            return _redirect(
                agreement_id,
                error="The agreement package is no longer approved.",
            )
        if (
            row.agreement_checksum != agreement.package_checksum
            or row.checksum != _checksum(dict(row.snapshot_json or {}))
        ):
            return _redirect(
                agreement_id,
                error="Signature readiness checksum verification failed.",
            )
        expected = (
            f"{'REVIEW' if target_status == 'in_review' else 'APPROVE'} "
            f"SIGNATURE {row.id}"
        )
        if confirmation.strip() != expected:
            return _redirect(
                agreement_id,
                error=f"Type {expected} to continue.",
            )
        if target_status not in TRANSITIONS.get(row.status, set()):
            return _redirect(
                agreement_id,
                error=f"Cannot move signature readiness from {row.status}.",
            )
        before = row.status
        row.status = target_status
        row.updated_at = now
        if target_status == "in_review":
            row.reviewed_by = actor
            row.reviewed_at = now
        else:
            row.approved_by = actor
            row.approved_at = now
        session.add(row)
        session.add(BuildingAuditEvent(
            entity_type="signature_request_readiness",
            entity_id=row.id,
            action=f"signature_request_readiness_{target_status}",
            actor=actor,
            before_json={"status": before},
            after_json={
                "status": target_status,
                "delivery_status": row.delivery_status,
                "provider": row.provider or "unselected",
                "provider_write": False,
                "message_sent": False,
            },
        ))
    return _redirect(
        agreement_id,
        notice=(
            f"Signature readiness moved to {target_status.replace('_', ' ')}; "
            "nothing was sent."
        ),
    )


@router.post(
    "/{agreement_id}/signature-readiness/recovery",
    dependencies=FORM_DEPS,
)
def record_signature_handoff_recovery(
    agreement_id: str,
    request: Request,
    target_status: Literal["failed", "not_sent"] = Form(...),
    failure_reason: str = Form(""),
    user: dict = Depends(require_tool("building.agreements.prepare")),
) -> RedirectResponse:
    """Record a failed manual handoff or make it retryable; send nothing."""

    actor = _actor(user)
    now = _now()
    with session_scope(request.app.state.session_factory) as session:
        agreement = session.get(BuildingAgreement, agreement_id)
        row = session.execute(
            select(BuildingSignatureRequestReadiness).where(
                BuildingSignatureRequestReadiness.agreement_id == agreement_id
            )
        ).scalar_one_or_none()
        if agreement is None or row is None or row.status != "approved":
            return _redirect(
                agreement_id,
                error="Approve the frozen QuickBooks handoff first.",
            )
        if (
            row.agreement_checksum != agreement.package_checksum
            or row.checksum != _checksum(dict(row.snapshot_json or {}))
        ):
            return _redirect(
                agreement_id,
                error="Signature readiness checksum verification failed.",
            )
        reason = failure_reason.strip()
        if target_status == "failed" and not reason:
            return _redirect(
                agreement_id,
                error="Describe the failed QuickBooks handoff before recording it.",
            )
        if row.delivery_status in {"sent", "completed"}:
            return _redirect(
                agreement_id,
                error="Delivered QuickBooks evidence cannot be reset from Agent.",
            )
        before = row.delivery_status
        row.delivery_status = target_status
        row.updated_at = now
        session.add(row)
        session.add(BuildingAuditEvent(
            entity_type="signature_request_readiness",
            entity_id=row.id,
            action=(
                "signature_handoff_failed"
                if target_status == "failed"
                else "signature_handoff_retry_ready"
            ),
            actor=actor,
            before_json={"delivery_status": before},
            after_json={
                "delivery_status": target_status,
                "failure_reason": reason,
                "provider": QUICKBOOKS_CONTRACT_PROVIDER,
                "provider_write": False,
                "message_sent": False,
            },
        ))
    return _redirect(
        agreement_id,
        notice=(
            "QuickBooks handoff failure recorded; nothing was sent."
            if target_status == "failed"
            else "QuickBooks handoff is ready to retry; nothing was sent."
        ),
    )
