"""Provider-neutral agreement package and payment-request readiness workflows."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Literal, Optional
from urllib.parse import urlencode
from uuid import uuid4

from fastapi import APIRouter, Depends, Form, Header, HTTPException, Request
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field, ValidationError, field_validator
from sqlalchemy import select

from sales_support_agent.models.database import session_scope
from sales_support_agent.models.entities import (
    BuildingAgreement,
    BuildingAgreementTemplate,
    BuildingAuditEvent,
    BuildingContact,
    BuildingEventLifecycleCommand,
    BuildingPaymentRequestReadiness,
    BuildingProposal,
    BuildingReservation,
    BuildingSpace,
)
from sales_support_agent.services.auth_deps import require_tool
from sales_support_agent.services.building_security import (
    require_building_form_security,
)


internal_router = APIRouter(
    prefix="/api/internal/building/agreement-readiness",
    tags=["building-agreement-readiness"],
)
admin_router = APIRouter(
    prefix="/admin/building/agreement-readiness",
    tags=["building-agreement-readiness-admin"],
)
FORM_DEPS = [Depends(require_building_form_security)]
#: The readiness admin surface now lives in the Building contract workspace.
CONTRACTS_URL = "/admin/building/contracts"

PREPARATION_TRANSITIONS = {
    "prepared": {"in_review"},
    "in_review": {"approved"},
    "approved": set(),
    "expired": set(),
    "cancelled": set(),
}
TEMPLATE_TRANSITIONS = {
    "draft": {"in_review"},
    "in_review": {"approved"},
    "approved": {"retired"},
    "retired": set(),
}
ALLOWED_MERGE_FIELDS = {
    "customer_name",
    "customer_email",
    "event_space",
    "setup_starts_at",
    "guest_starts_at",
    "guest_ends_at",
    "teardown_ends_at",
    "attendance",
    "quote_total",
    "currency",
    "deposit_amount",
    "deposit_type",
    "cancellation_policy",
    "tax_terms",
    "included",
    "addons",
}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _aware(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _require_internal_key(request: Request, provided: Optional[str]) -> None:
    configured = str(
        getattr(request.app.state.settings, "internal_api_key", "") or ""
    ).strip()
    if not configured:
        raise HTTPException(status_code=503, detail="Internal API is not configured.")
    if str(provided or "").strip() != configured:
        raise HTTPException(status_code=401, detail="Invalid internal API key.")


def _checksum(value: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


class AgreementTemplateInput(BaseModel):
    id: str = Field(min_length=1, max_length=64)
    template_key: str = Field(min_length=1, max_length=64)
    version: int = Field(ge=1)
    name: str = Field(min_length=1, max_length=255)
    template_reference: str = Field(min_length=1, max_length=1024)
    merge_fields: list[str] = Field(min_length=1, max_length=30)
    actor: str = Field(min_length=1, max_length=255)

    @field_validator("merge_fields")
    @classmethod
    def valid_merge_fields(cls, value: list[str]) -> list[str]:
        normalized = [item.strip() for item in value]
        unknown = sorted(set(normalized) - ALLOWED_MERGE_FIELDS)
        if unknown:
            raise ValueError(f"Unsupported merge fields: {', '.join(unknown)}")
        if len(normalized) != len(set(normalized)):
            raise ValueError("Merge fields must be unique.")
        return normalized


class ReviewActionInput(BaseModel):
    target_status: Literal["in_review", "approved", "retired"]
    confirmation: str = Field(min_length=1, max_length=255)
    evidence: str = Field(default="", max_length=2000)
    actor: str = Field(min_length=1, max_length=255)


class AgreementPackageInput(BaseModel):
    reservation_id: str = Field(min_length=1, max_length=64)
    quote_id: str = Field(min_length=1, max_length=64)
    template_id: str = Field(min_length=1, max_length=64)
    agreement_version: int = Field(default=1, ge=1)
    payment_version: int = Field(default=1, ge=1)
    actor: str = Field(min_length=1, max_length=255)


class ReadinessTransitionInput(BaseModel):
    target_status: Literal["in_review", "approved"]
    confirmation: str = Field(min_length=1, max_length=255)
    actor: str = Field(min_length=1, max_length=255)


def _template_payload(row: BuildingAgreementTemplate) -> dict[str, Any]:
    return {
        "id": row.id,
        "template_key": row.template_key,
        "version": row.version,
        "name": row.name,
        "status": row.status,
        "template_reference": row.template_reference,
        "merge_fields": list(row.merge_fields_json or []),
        "approval_evidence": row.approval_evidence,
        "approved_by": row.approved_by,
        "approved_at": row.approved_at.isoformat() if row.approved_at else None,
    }


def _readiness_payload(
    agreement: BuildingAgreement,
    payment: BuildingPaymentRequestReadiness,
) -> dict[str, Any]:
    return {
        "agreement": {
            "id": agreement.id,
            "version": agreement.version,
            "status": agreement.status,
            "preparation_status": agreement.preparation_status,
            "template_id": agreement.template_id,
            "checksum": agreement.package_checksum,
            "snapshot": dict(agreement.package_snapshot_json or {}),
            "provider": agreement.provider or None,
            "provider_reference": agreement.provider_reference or None,
        },
        "payment_request": {
            "id": payment.id,
            "version": payment.version,
            "status": payment.status,
            "request_type": payment.request_type,
            "amount_cents": payment.amount_cents,
            "currency": payment.currency,
            "checksum": payment.checksum,
            "metadata": dict(payment.metadata_json or {}),
            "provider_object_created": False,
            "payment_success": False,
        },
        "gates": {
            "agreement_package_approved": agreement.preparation_status == "approved",
            "contract_sent": False,
            "signature_verified": False,
            "payment_request_approved": payment.status == "approved",
            "payment_verified": False,
            "booking_confirmed": False,
        },
    }


@internal_router.put("/templates/{template_id}")
def upsert_agreement_template(
    template_id: str,
    payload: AgreementTemplateInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    if payload.id != template_id:
        raise HTTPException(status_code=422, detail="Template ID does not match route.")
    with session_scope(request.app.state.session_factory) as session:
        row = session.get(BuildingAgreementTemplate, template_id)
        if row is not None and row.status in {"approved", "retired"}:
            raise HTTPException(
                status_code=409,
                detail="Approved templates are immutable; create a new version.",
            )
        duplicate = session.execute(
            select(BuildingAgreementTemplate).where(
                BuildingAgreementTemplate.template_key == payload.template_key,
                BuildingAgreementTemplate.version == payload.version,
                BuildingAgreementTemplate.id != template_id,
            )
        ).scalar_one_or_none()
        if duplicate:
            raise HTTPException(
                status_code=409,
                detail="That agreement template version already exists.",
            )
        before = _template_payload(row) if row else {}
        row = row or BuildingAgreementTemplate(
            id=template_id,
            template_key=payload.template_key,
            version=payload.version,
            name=payload.name,
            created_by=payload.actor,
        )
        row.template_key = payload.template_key
        row.version = payload.version
        row.name = payload.name
        row.template_reference = payload.template_reference.strip()
        row.merge_fields_json = payload.merge_fields
        row.updated_at = _now()
        session.add(row)
        session.add(BuildingAuditEvent(
            entity_type="agreement_template",
            entity_id=row.id,
            action="agreement_template_draft_saved",
            actor=payload.actor,
            before_json=before,
            after_json=_template_payload(row),
        ))
        return {"ok": True, "template": _template_payload(row)}


@internal_router.post("/templates/{template_id}/transition")
def transition_agreement_template(
    template_id: str,
    payload: ReviewActionInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        row = session.get(BuildingAgreementTemplate, template_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Agreement template not found.")
        expected = f"{payload.target_status.upper()} TEMPLATE {row.id}"
        if payload.confirmation.strip() != expected:
            raise HTTPException(status_code=422, detail=f"Type exactly: {expected}")
        if row.status == payload.target_status:
            return {
                "ok": True,
                "replayed": True,
                "template": _template_payload(row),
            }
        if payload.target_status not in TEMPLATE_TRANSITIONS.get(row.status, set()):
            raise HTTPException(
                status_code=409,
                detail=f"Cannot move template from {row.status} to {payload.target_status}.",
            )
        if payload.target_status == "approved" and not payload.evidence.strip():
            raise HTTPException(
                status_code=422, detail="Template approval evidence is required."
            )
        before = row.status
        row.status = payload.target_status
        row.updated_at = _now()
        if row.status == "approved":
            row.approval_evidence = payload.evidence.strip()
            row.approved_by = payload.actor
            row.approved_at = _now()
        session.add(BuildingAuditEvent(
            entity_type="agreement_template",
            entity_id=row.id,
            action=f"agreement_template_{row.status}",
            actor=payload.actor,
            before_json={"status": before},
            after_json={
                "status": row.status,
                "version": row.version,
                "approval_evidence": row.approval_evidence,
            },
        ))
        return {"ok": True, "template": _template_payload(row)}


@internal_router.post("/packages", status_code=201)
def prepare_agreement_package(
    payload: AgreementPackageInput,
    request: Request,
    idempotency_key: str = Header(
        alias="Idempotency-Key", min_length=8, max_length=128
    ),
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    request_hash = _checksum(payload.model_dump(mode="json"))
    with session_scope(request.app.state.session_factory) as session:
        command = session.execute(
            select(BuildingEventLifecycleCommand).where(
                BuildingEventLifecycleCommand.idempotency_key == idempotency_key
            )
        ).scalar_one_or_none()
        if command:
            if command.request_hash != request_hash:
                raise HTTPException(
                    status_code=409,
                    detail="This idempotency key was used for a different request.",
                )
            agreement = session.get(
                BuildingAgreement,
                str(command.response_json.get("agreement_id") or ""),
            )
            payment = session.get(
                BuildingPaymentRequestReadiness,
                str(command.response_json.get("payment_readiness_id") or ""),
            )
            if agreement is None or payment is None:
                raise HTTPException(
                    status_code=409,
                    detail="Original preparation evidence is incomplete; operator review is required.",
                )
            return {**_readiness_payload(agreement, payment), "replayed": True}

        reservation = session.get(BuildingReservation, payload.reservation_id)
        if reservation is None or reservation.kind != "event":
            raise HTTPException(status_code=404, detail="Event hold not found.")
        if (
            reservation.status != "soft_hold"
            or reservation.hold_expires_at is None
            or _aware(reservation.hold_expires_at) <= _now()
        ):
            raise HTTPException(
                status_code=409,
                detail="An active, unexpired Agent temporary hold is required.",
            )
        quote = session.get(BuildingProposal, payload.quote_id)
        if (
            quote is None
            or quote.reservation_id != reservation.id
            or quote.proposal_type != "quote"
            or quote.status != "draft"
            or not quote.rate_plan_snapshot_json
        ):
            raise HTTPException(
                status_code=409,
                detail="A frozen internal quote draft is required.",
            )
        template = session.get(BuildingAgreementTemplate, payload.template_id)
        if template is None or template.status != "approved":
            raise HTTPException(
                status_code=409,
                detail="An approved versioned agreement template is required.",
            )
        contact = (
            session.get(BuildingContact, reservation.contact_id)
            if reservation.contact_id
            else None
        )
        space = session.get(BuildingSpace, reservation.space_id)
        if contact is None or contact.status != "active":
            raise HTTPException(
                status_code=409,
                detail="An active responsible contact is required.",
            )
        if space is None:
            raise HTTPException(status_code=409, detail="Reviewed event space is missing.")
        if session.execute(
            select(BuildingAgreement).where(
                BuildingAgreement.reservation_id == reservation.id,
                BuildingAgreement.version == payload.agreement_version,
            )
        ).scalar_one_or_none():
            raise HTTPException(
                status_code=409,
                detail="Agreement version already exists; retry with the original key or choose a new version.",
            )

        rate = dict(quote.rate_plan_snapshot_json or {})
        deposit_type = str(rate.get("deposit_type") or "none")
        deposit_cents = {
            "fixed": min(int(rate.get("deposit_amount_cents") or 0), quote.amount_cents),
            "percent": min(
                (
                    quote.amount_cents * int(rate.get("deposit_percent_bps") or 0)
                    + 5000
                )
                // 10000,
                quote.amount_cents,
            ),
            "none": quote.amount_cents,
        }.get(deposit_type)
        if deposit_cents is None or deposit_cents <= 0:
            raise HTTPException(
                status_code=409,
                detail="Frozen terms do not produce a valid payment request amount.",
            )
        request_type = "full_amount" if deposit_type == "none" else "deposit"
        merge_values = {
            "customer_name": contact.full_name,
            "customer_email": contact.email,
            "event_space": space.name,
            "setup_starts_at": reservation.starts_at.isoformat(),
            "guest_starts_at": (
                reservation.guest_starts_at.isoformat()
                if reservation.guest_starts_at
                else None
            ),
            "guest_ends_at": (
                reservation.guest_ends_at.isoformat()
                if reservation.guest_ends_at
                else None
            ),
            "teardown_ends_at": reservation.ends_at.isoformat(),
            "attendance": reservation.attendance,
            "quote_total": quote.amount_cents,
            "currency": quote.currency,
            "deposit_amount": deposit_cents,
            "deposit_type": deposit_type,
            "cancellation_policy": str(rate.get("cancellation_policy") or ""),
            "tax_terms": {
                "status": str(rate.get("tax_status") or "review_required"),
                "rate_bps": int(rate.get("tax_rate_bps") or 0),
                "note": str(rate.get("tax_note") or ""),
            },
            "included": list(rate.get("included") or []),
            "addons": list(rate.get("addons") or []),
        }
        selected_merge_values = {
            field: merge_values[field]
            for field in list(template.merge_fields_json or [])
        }
        package_snapshot = {
            "schema_version": 1,
            "reservation_id": reservation.id,
            "agreement_version": payload.agreement_version,
            "template": {
                "id": template.id,
                "template_key": template.template_key,
                "version": template.version,
                "name": template.name,
                "reference": template.template_reference,
                "approved_by": template.approved_by,
                "approved_at": (
                    template.approved_at.isoformat() if template.approved_at else None
                ),
                "approval_evidence": template.approval_evidence,
                "merge_fields": list(template.merge_fields_json or []),
            },
            "event_window": {
                "setup_starts_at": reservation.starts_at.isoformat(),
                "guest_starts_at": (
                    reservation.guest_starts_at.isoformat()
                    if reservation.guest_starts_at
                    else None
                ),
                "guest_ends_at": (
                    reservation.guest_ends_at.isoformat()
                    if reservation.guest_ends_at
                    else None
                ),
                "teardown_ends_at": reservation.ends_at.isoformat(),
            },
            "quote": {
                "id": quote.id,
                "version": quote.version,
                "currency": quote.currency,
                "amount_cents": quote.amount_cents,
                "line_items": list(quote.line_items_json or []),
                "rate_plan_id": quote.rate_plan_id,
                "rate_plan_snapshot": rate,
                "terms_summary": quote.terms_summary,
            },
            "merge_values": selected_merge_values,
            "prepared_at": _now().isoformat(),
        }
        agreement = BuildingAgreement(
            id=str(uuid4()),
            reservation_id=reservation.id,
            version=payload.agreement_version,
            status="draft",
            template_name=template.name,
            template_id=template.id,
            preparation_status="prepared",
            package_snapshot_json=package_snapshot,
            package_checksum=_checksum(package_snapshot),
            evidence_json={
                "provider_neutral": True,
                "document_generated": False,
                "sent": False,
                "signed": False,
            },
            created_by=payload.actor,
            updated_at=_now(),
        )
        session.add(agreement)
        session.flush()
        payment_metadata = {
            "schema_version": 1,
            "reservation_id": reservation.id,
            "agreement_id": agreement.id,
            "agreement_checksum": agreement.package_checksum,
            "quote_id": quote.id,
            "quote_version": quote.version,
            "rate_plan_id": quote.rate_plan_id,
            "deposit_type": deposit_type,
            "tax_status": str(rate.get("tax_status") or "review_required"),
            "provider": None,
            "provider_object_created": False,
            "invoice_created": False,
            "payment_received": False,
        }
        payment_snapshot = {
            "request_type": request_type,
            "amount_cents": deposit_cents,
            "currency": quote.currency,
            "metadata": payment_metadata,
        }
        payment = BuildingPaymentRequestReadiness(
            id=str(uuid4()),
            reservation_id=reservation.id,
            agreement_id=agreement.id,
            version=payload.payment_version,
            status="prepared",
            request_type=request_type,
            amount_cents=deposit_cents,
            currency=quote.currency,
            metadata_json=payment_metadata,
            checksum=_checksum(payment_snapshot),
            created_by=payload.actor,
            updated_at=_now(),
        )
        session.add(payment)
        session.add(BuildingAuditEvent(
            entity_type="agreement",
            entity_id=agreement.id,
            action="agreement_package_prepared",
            actor=payload.actor,
            after_json={
                "reservation_id": reservation.id,
                "version": agreement.version,
                "template_id": template.id,
                "checksum": agreement.package_checksum,
                "provider_write": False,
            },
        ))
        session.add(BuildingAuditEvent(
            entity_type="payment_request_readiness",
            entity_id=payment.id,
            action="payment_request_prepared",
            actor=payload.actor,
            after_json={
                "reservation_id": reservation.id,
                "agreement_id": agreement.id,
                "amount_cents": payment.amount_cents,
                "currency": payment.currency,
                "checksum": payment.checksum,
                "provider_write": False,
            },
        ))
        session.add(BuildingEventLifecycleCommand(
            id=str(uuid4()),
            idempotency_key=idempotency_key,
            command_type="prepare_agreement_and_payment_readiness",
            request_hash=request_hash,
            inquiry_id=reservation.inquiry_id or "",
            reservation_id=reservation.id,
            response_json={
                "agreement_id": agreement.id,
                "payment_readiness_id": payment.id,
            },
            actor=payload.actor,
        ))
        reservation.agreement_status = "draft"
        reservation.updated_at = _now()
        session.flush()
        return {**_readiness_payload(agreement, payment), "replayed": False}


def _transition_readiness(
    session,
    *,
    row: BuildingAgreement | BuildingPaymentRequestReadiness,
    entity_type: str,
    payload: ReadinessTransitionInput,
) -> None:
    current = (
        row.preparation_status
        if isinstance(row, BuildingAgreement)
        else row.status
    )
    noun = "AGREEMENT" if isinstance(row, BuildingAgreement) else "PAYMENT"
    verb = "REVIEW" if payload.target_status == "in_review" else "APPROVE"
    expected = f"{verb} {noun} {row.id}"
    if payload.confirmation.strip() != expected:
        raise HTTPException(status_code=422, detail=f"Type exactly: {expected}")
    if current == payload.target_status:
        return
    if payload.target_status not in PREPARATION_TRANSITIONS.get(current, set()):
        raise HTTPException(
            status_code=409,
            detail=f"Cannot move {entity_type} from {current} to {payload.target_status}.",
        )
    if isinstance(row, BuildingAgreement):
        row.preparation_status = payload.target_status
    else:
        row.status = payload.target_status
    if payload.target_status == "in_review":
        row.reviewed_by = payload.actor
        row.reviewed_at = _now()
    else:
        row.approved_by = payload.actor
        row.approved_at = _now()
    row.updated_at = _now()
    session.add(BuildingAuditEvent(
        entity_type=entity_type,
        entity_id=row.id,
        action=f"{entity_type}_{payload.target_status}",
        actor=payload.actor,
        before_json={"status": current},
        after_json={
            "status": payload.target_status,
            "checksum": (
                row.package_checksum
                if isinstance(row, BuildingAgreement)
                else row.checksum
            ),
            "provider_write": False,
        },
    ))


@internal_router.post("/packages/{agreement_id}/transition")
def transition_agreement_package(
    agreement_id: str,
    payload: ReadinessTransitionInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        agreement = session.get(BuildingAgreement, agreement_id)
        if agreement is None or not agreement.package_checksum:
            raise HTTPException(status_code=404, detail="Agreement package not found.")
        payment = session.execute(
            select(BuildingPaymentRequestReadiness).where(
                BuildingPaymentRequestReadiness.agreement_id == agreement.id
            )
        ).scalar_one()
        reservation = session.get(BuildingReservation, agreement.reservation_id)
        if (
            reservation is None
            or reservation.status != "soft_hold"
            or reservation.hold_expires_at is None
            or _aware(reservation.hold_expires_at) <= _now()
        ):
            raise HTTPException(status_code=409, detail="The Agent hold is no longer active.")
        if _checksum(dict(agreement.package_snapshot_json or {})) != agreement.package_checksum:
            raise HTTPException(
                status_code=409,
                detail="Agreement package checksum mismatch; prepare a new version.",
            )
        _transition_readiness(
            session,
            row=agreement,
            entity_type="agreement_package",
            payload=payload,
        )
        return _readiness_payload(agreement, payment)


@internal_router.post("/payments/{payment_id}/transition")
def transition_payment_readiness(
    payment_id: str,
    payload: ReadinessTransitionInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        payment = session.get(BuildingPaymentRequestReadiness, payment_id)
        if payment is None:
            raise HTTPException(status_code=404, detail="Payment readiness not found.")
        agreement = session.get(BuildingAgreement, payment.agreement_id)
        reservation = session.get(BuildingReservation, payment.reservation_id)
        if agreement is None or reservation is None:
            raise HTTPException(status_code=409, detail="Readiness evidence is incomplete.")
        if (
            reservation.status != "soft_hold"
            or reservation.hold_expires_at is None
            or _aware(reservation.hold_expires_at) <= _now()
        ):
            raise HTTPException(status_code=409, detail="The Agent hold is no longer active.")
        payment_snapshot = {
            "request_type": payment.request_type,
            "amount_cents": payment.amount_cents,
            "currency": payment.currency,
            "metadata": dict(payment.metadata_json or {}),
        }
        if _checksum(payment_snapshot) != payment.checksum:
            raise HTTPException(
                status_code=409,
                detail="Payment readiness checksum mismatch; prepare a new version.",
            )
        if payload.target_status == "approved":
            if agreement.preparation_status != "approved":
                raise HTTPException(
                    status_code=409,
                    detail="Approve the agreement package before payment readiness.",
                )
            if str((payment.metadata_json or {}).get("tax_status")) == "review_required":
                raise HTTPException(
                    status_code=409,
                    detail="Tax treatment must be resolved before payment-request approval.",
                )
        _transition_readiness(
            session,
            row=payment,
            entity_type="payment_request",
            payload=payload,
        )
        return _readiness_payload(agreement, payment)


@internal_router.get("/reservations/{reservation_id}")
def get_reservation_readiness(
    reservation_id: str,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        agreement = session.execute(
            select(BuildingAgreement)
            .where(
                BuildingAgreement.reservation_id == reservation_id,
                BuildingAgreement.package_checksum != "",
            )
            .order_by(BuildingAgreement.version.desc())
        ).scalars().first()
        if agreement is None:
            raise HTTPException(status_code=404, detail="Agreement readiness not found.")
        payment = session.execute(
            select(BuildingPaymentRequestReadiness).where(
                BuildingPaymentRequestReadiness.agreement_id == agreement.id
            )
        ).scalar_one()
        return _readiness_payload(agreement, payment)


def _actor(user: dict[str, Any]) -> str:
    return str(user.get("email") or "building-operator")


def _internal_key(request: Request) -> str:
    key = str(
        getattr(request.app.state.settings, "internal_api_key", "") or ""
    ).strip()
    if not key:
        raise HTTPException(status_code=503, detail="Internal API is not configured.")
    return key


def _redirect(*, notice: str = "", error: str = "") -> RedirectResponse:
    query = urlencode({"notice": notice} if notice else {"error": error})
    return RedirectResponse(
        f"{CONTRACTS_URL}?{query}", status_code=303
    )


@admin_router.get("")
def agreement_readiness_page(request: Request) -> RedirectResponse:
    """Permanent redirect to the Building contract workspace.

    The bare template registry and identifier form this page used to render was
    replaced by /admin/building/contracts, which shows the same records with the
    customer, space, dates, value, and audit history attached.
    """

    return RedirectResponse(CONTRACTS_URL, status_code=308)


@admin_router.post("/packages", dependencies=FORM_DEPS)
def prepare_package_from_admin(
    request: Request,
    reservation_id: str = Form(...),
    quote_id: str = Form(...),
    template_id: str = Form(...),
    idempotency_key: str = Form(...),
    agreement_version: int = Form(1),
    payment_version: int = Form(1),
    user: dict = Depends(require_tool("building.agreements.prepare")),
) -> RedirectResponse:
    try:
        prepare_agreement_package(
            AgreementPackageInput(
                reservation_id=reservation_id.strip(),
                quote_id=quote_id.strip(),
                template_id=template_id.strip(),
                agreement_version=agreement_version,
                payment_version=payment_version,
                actor=_actor(user),
            ),
            request,
            idempotency_key.strip(),
            _internal_key(request),
        )
    except HTTPException as exc:
        return _redirect(error=str(exc.detail))
    return _redirect(
        notice="Agreement package and payment request prepared for review; nothing was sent."
    )


@admin_router.post("/templates", dependencies=FORM_DEPS)
def save_template_from_admin(
    request: Request,
    template_id: str = Form(...),
    template_key: str = Form(...),
    version: int = Form(...),
    name: str = Form(...),
    template_reference: str = Form(...),
    merge_fields: str = Form(...),
    user: dict = Depends(require_tool("building.agreements.prepare")),
) -> RedirectResponse:
    try:
        upsert_agreement_template(
            template_id.strip(),
            AgreementTemplateInput(
                id=template_id.strip(),
                template_key=template_key.strip(),
                version=version,
                name=name.strip(),
                template_reference=template_reference.strip(),
                merge_fields=[
                    item.strip()
                    for item in merge_fields.split(",")
                    if item.strip()
                ],
                actor=_actor(user),
            ),
            request,
            _internal_key(request),
        )
    except (HTTPException, ValidationError, ValueError) as exc:
        return _redirect(error=str(getattr(exc, "detail", exc)))
    return _redirect(notice="Agreement template draft saved; it is not approved.")


@admin_router.post("/templates/transition", dependencies=FORM_DEPS)
def transition_template_from_admin(
    request: Request,
    template_id: str = Form(...),
    target_status: Literal["in_review", "approved", "retired"] = Form(...),
    confirmation: str = Form(...),
    evidence: str = Form(""),
    user: dict = Depends(require_tool("building.agreements.approve")),
) -> RedirectResponse:
    try:
        transition_agreement_template(
            template_id.strip(),
            ReviewActionInput(
                target_status=target_status,
                confirmation=confirmation.strip(),
                evidence=evidence.strip(),
                actor=_actor(user),
            ),
            request,
            _internal_key(request),
        )
    except HTTPException as exc:
        return _redirect(error=str(exc.detail))
    return _redirect(notice=f"Template moved to {target_status.replace('_', ' ')}.")


@admin_router.post("/packages/transition", dependencies=FORM_DEPS)
def transition_package_from_admin(
    request: Request,
    agreement_id: str = Form(...),
    target_status: Literal["in_review", "approved"] = Form(...),
    confirmation: str = Form(...),
    user: dict = Depends(require_tool("building.agreements.approve")),
) -> RedirectResponse:
    try:
        transition_agreement_package(
            agreement_id.strip(),
            ReadinessTransitionInput(
                target_status=target_status,
                confirmation=confirmation.strip(),
                actor=_actor(user),
            ),
            request,
            _internal_key(request),
        )
    except HTTPException as exc:
        return _redirect(error=str(exc.detail))
    return _redirect(
        notice=f"Agreement package moved to {target_status.replace('_', ' ')}; nothing was sent."
    )


@admin_router.post("/payments/transition", dependencies=FORM_DEPS)
def transition_payment_from_admin(
    request: Request,
    payment_id: str = Form(...),
    target_status: Literal["in_review", "approved"] = Form(...),
    confirmation: str = Form(...),
    user: dict = Depends(require_tool("building.payments.prepare")),
) -> RedirectResponse:
    try:
        transition_payment_readiness(
            payment_id.strip(),
            ReadinessTransitionInput(
                target_status=target_status,
                confirmation=confirmation.strip(),
                actor=_actor(user),
            ),
            request,
            _internal_key(request),
        )
    except HTTPException as exc:
        return _redirect(error=str(exc.detail))
    return _redirect(
        notice=f"Payment request moved to {target_status.replace('_', ' ')}; no provider object exists."
    )
