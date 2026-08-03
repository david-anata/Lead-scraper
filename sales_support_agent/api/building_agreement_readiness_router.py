"""Provider-neutral agreement package and payment-request readiness workflows."""

from __future__ import annotations

import hashlib
import html
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, Optional
from urllib.parse import urlencode
from uuid import uuid4

from fastapi import APIRouter, Depends, Form, Header, HTTPException, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse
from pydantic import (
    BaseModel,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)
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
from sales_support_agent.services.admin_nav import (
    render_agent_favicon_links,
    render_agent_nav,
    render_agent_nav_styles,
)
from sales_support_agent.services.auth_deps import require_tool
from sales_support_agent.services.building_contract_templates import (
    EVENT_MERGE_FIELDS,
    document_checksum,
    merge_fields_for,
    normalize_clauses,
    render_document_text,
    unresolved_fields,
    validate_template_content,
)
from sales_support_agent.services.building_contracts import (
    compute_event_merge_values,
)
from sales_support_agent.services.building_launch_readiness import (
    sync_arena_agreement_template_decision,
)
from sales_support_agent.services.building_security import (
    csrf_token,
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
    "in_review": {"draft", "approved"},
    "approved": {"retired"},
    "retired": set(),
}
#: Event merge fields, kept as one source of truth with the template editor.
ALLOWED_MERGE_FIELDS = set(EVENT_MERGE_FIELDS)
ARENA_REVIEW_TEMPLATE_ID = "arena-event-agreement-business-terms-v2"
ARENA_REVIEW_TEMPLATE_KEY = "arena-event-agreement"
ARENA_REVIEW_TEMPLATE_VERSION = 2
ARENA_REVIEW_TEMPLATE_NAME = "Arena event agreement business terms"
ARENA_REVIEW_DOCUMENT = (
    Path(__file__).resolve().parents[2]
    / "docs"
    / "building"
    / "agreements"
    / "arena-event-agreement-business-terms-v2.md"
)
ARENA_REVIEW_MERGE_FIELDS = [
    "customer_name",
    "customer_email",
    "event_space",
    "setup_starts_at",
    "guest_starts_at",
    "guest_ends_at",
    "teardown_ends_at",
    "attendance",
    "subtotal_before_discount",
    "discount_amount",
    "discount_reason",
    "quote_total",
    "currency",
    "deposit_amount",
    "deposit_type",
    "cancellation_policy",
    "tax_terms",
    "included",
    "addons",
]


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


def _arena_review_document() -> tuple[str, str, str]:
    """Return the governed Arena review artifact, checksum, and durable reference."""

    try:
        content = ARENA_REVIEW_DOCUMENT.read_text(encoding="utf-8")
    except OSError as exc:
        raise HTTPException(
            status_code=503,
            detail="The Arena agreement-review document is unavailable.",
        ) from exc
    checksum = hashlib.sha256(content.encode("utf-8")).hexdigest()
    reference = (
        "repository:docs/building/agreements/"
        f"{ARENA_REVIEW_DOCUMENT.name}#sha256={checksum}"
    )
    return content, checksum, reference


class AgreementTemplateInput(BaseModel):
    """A template version.

    Authoring is either external (a durable ``template_reference`` plus an
    explicit merge-field list) or in-Agent (``body_markdown`` and/or
    ``clauses``, whose merge fields are derived from the tokens actually used).
    """

    id: str = Field(min_length=1, max_length=64)
    template_key: str = Field(min_length=1, max_length=64)
    version: int = Field(ge=1)
    name: str = Field(min_length=1, max_length=255)
    contract_type: str = Field(default="event", max_length=32)
    template_reference: str = Field(default="", max_length=1024)
    body_markdown: str = Field(default="", max_length=60000)
    clauses: list[dict[str, str]] = Field(default_factory=list, max_length=60)
    merge_fields: list[str] = Field(default_factory=list, max_length=30)
    actor: str = Field(min_length=1, max_length=255)

    @field_validator("merge_fields")
    @classmethod
    def unique_merge_fields(cls, value: list[str]) -> list[str]:
        normalized = [item.strip() for item in value]
        if len(normalized) != len(set(normalized)):
            raise ValueError("Merge fields must be unique.")
        return normalized

    @model_validator(mode="after")
    def valid_authoring(self) -> "AgreementTemplateInput":
        authored = bool(self.body_markdown.strip() or self.clauses)
        if authored:
            self.clauses = normalize_clauses(self.clauses)
            # Derived fields are authoritative for authored templates: the body
            # is the contract, so its tokens define what must be merged.
            self.merge_fields = validate_template_content(
                contract_type=self.contract_type,
                body_markdown=self.body_markdown,
                clauses=self.clauses,
            )
            return self
        if not self.template_reference.strip():
            raise ValueError(
                "Provide contract body text, clauses, or a durable approved "
                "repository reference."
            )
        if not self.merge_fields:
            raise ValueError(
                "An externally authored template must list its merge fields."
            )
        allowed = set(merge_fields_for(self.contract_type))
        unknown = sorted(set(self.merge_fields) - allowed)
        if unknown:
            raise ValueError(f"Unsupported merge fields: {', '.join(unknown)}")
        return self


class ReviewActionInput(BaseModel):
    target_status: Literal["draft", "in_review", "approved", "retired"]
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
        "contract_type": getattr(row, "contract_type", "") or "event",
        "template_reference": row.template_reference,
        "body_checksum": (
            document_checksum(row.body_markdown) if row.body_markdown else ""
        ),
        "clause_count": len(row.clauses_json or []),
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
        row.contract_type = payload.contract_type
        row.template_reference = payload.template_reference.strip()
        row.body_markdown = payload.body_markdown.strip()
        row.clauses_json = payload.clauses
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
        if (
            payload.target_status in {"draft", "approved"}
            and not payload.evidence.strip()
        ):
            raise HTTPException(
                status_code=422,
                detail=(
                    "A review change note is required."
                    if payload.target_status == "draft"
                    else "Template approval evidence is required."
                ),
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
                "transition_evidence": payload.evidence.strip(),
            },
        ))
        sync_arena_agreement_template_decision(
            session,
            template=row,
            actor=payload.actor,
        )
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
        merge_values, deposit_cents, request_type = compute_event_merge_values(
            reservation=reservation, contact=contact, space=space, quote=quote
        )
        if deposit_cents <= 0:
            raise HTTPException(
                status_code=409,
                detail="Frozen terms do not produce a valid payment request amount.",
            )
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
        # An authored template freezes its rendered text with the package, so the
        # approved contract has verifiable content rather than a bare reference.
        if (getattr(template, "body_markdown", "") or "") or (
            getattr(template, "clauses_json", None) or []
        ):
            document_text = render_document_text(
                name=template.name,
                body_markdown=template.body_markdown or "",
                clauses=template.clauses_json or [],
                merge_values=selected_merge_values,
            )
            if unresolved_fields(document_text):
                raise HTTPException(
                    status_code=409,
                    detail=(
                        "The template uses merge fields this booking cannot "
                        "supply. Resolve the missing values or approve a "
                        "template version that does not require them."
                    ),
                )
            package_snapshot["document"] = {
                "format": "markdown",
                "text": document_text,
                "checksum": document_checksum(document_text),
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
        f"/admin/building/agreement-readiness?{query}", status_code=303
    )


@admin_router.get(
    "/arena-review-package/download",
    response_class=PlainTextResponse,
)
def download_arena_review_package(
    user: dict = Depends(require_tool("building.agreements.prepare")),
) -> PlainTextResponse:
    """Download the internal business-terms schedule without approving or sending it."""

    content, checksum, _ = _arena_review_document()
    return PlainTextResponse(
        content,
        media_type="text/markdown; charset=utf-8",
        headers={
            "Content-Disposition": (
                'attachment; filename="anata-arena-agreement-business-terms-v2.md"'
            ),
            "X-Content-SHA256": checksum,
            "Cache-Control": "private, no-store",
        },
    )


@admin_router.post(
    "/arena-review-package/prepare",
    dependencies=FORM_DEPS,
)
def prepare_arena_review_package(
    request: Request,
    confirmation: str = Form(...),
    user: dict = Depends(require_tool("building.agreements.prepare")),
) -> RedirectResponse:
    """Register the repository artifact for review without claiming legal approval."""

    expected_confirmation = "PREPARE ARENA AGREEMENT REVIEW"
    if confirmation.strip() != expected_confirmation:
        return _redirect(error=f"Type exactly: {expected_confirmation}")
    _, checksum, reference = _arena_review_document()
    actor = _actor(user)
    with session_scope(request.app.state.session_factory) as session:
        row = session.get(BuildingAgreementTemplate, ARENA_REVIEW_TEMPLATE_ID)
        expected_values = {
            "template_key": ARENA_REVIEW_TEMPLATE_KEY,
            "version": ARENA_REVIEW_TEMPLATE_VERSION,
            "name": ARENA_REVIEW_TEMPLATE_NAME,
            "template_reference": reference,
            "merge_fields": ARENA_REVIEW_MERGE_FIELDS,
        }
        if row is not None:
            actual_values = {
                "template_key": row.template_key,
                "version": row.version,
                "name": row.name,
                "template_reference": row.template_reference,
                "merge_fields": list(row.merge_fields_json or []),
            }
            if actual_values != expected_values:
                return _redirect(
                    error=(
                        "The existing Arena review template differs from the "
                        "repository artifact. Create a new version after review."
                    )
                )
            if row.status == "retired":
                return _redirect(
                    error="The Arena review template is retired; prepare a new version."
                )
            if row.status == "approved":
                return _redirect(
                    notice=(
                        "The matching Arena agreement template is already approved. "
                        "No record was changed and nothing was sent."
                    )
                )
            if row.status == "in_review":
                return _redirect(
                    notice=(
                        "The matching Arena business-terms package is already in "
                        "legal review. Nothing was sent."
                    )
                )
        else:
            row = BuildingAgreementTemplate(
                id=ARENA_REVIEW_TEMPLATE_ID,
                template_key=ARENA_REVIEW_TEMPLATE_KEY,
                version=ARENA_REVIEW_TEMPLATE_VERSION,
                name=ARENA_REVIEW_TEMPLATE_NAME,
                status="draft",
                template_reference=reference,
                merge_fields_json=ARENA_REVIEW_MERGE_FIELDS,
                created_by=actor,
                updated_at=_now(),
            )
            session.add(row)
            session.flush()
            session.add(BuildingAuditEvent(
                entity_type="agreement_template",
                entity_id=row.id,
                action="agreement_template_draft_saved",
                actor=actor,
                after_json={
                    **_template_payload(row),
                    "artifact_checksum": checksum,
                    "legal_approval": False,
                    "provider_write": False,
                    "customer_delivery": False,
                },
            ))
        before = row.status
        row.status = "in_review"
        row.updated_at = _now()
        session.add(BuildingAuditEvent(
            entity_type="agreement_template",
            entity_id=row.id,
            action="agreement_template_in_review",
            actor=actor,
            before_json={"status": before},
            after_json={
                "status": "in_review",
                "artifact_checksum": checksum,
                "legal_approval": False,
                "provider_write": False,
                "customer_delivery": False,
            },
        ))
    return _redirect(
        notice=(
            "Arena agreement business terms prepared for legal review. "
            "They are not approved, signed, or sent."
        )
    )


@admin_router.get("", response_class=HTMLResponse)
def agreement_readiness_page(
    request: Request,
    user: dict = Depends(require_tool("building.agreements.prepare")),
) -> RedirectResponse:
    """Redirect the retired identifier-driven page to the customer-first workspace."""

    return RedirectResponse(CONTRACTS_URL, status_code=308)

    # Retained temporarily below as rollback/reference code while the contract
    # workspace replaces the identifier-driven UI. This branch is unreachable.
    with session_scope(request.app.state.session_factory) as session:
        templates = session.execute(
            select(BuildingAgreementTemplate).order_by(
                BuildingAgreementTemplate.template_key,
                BuildingAgreementTemplate.version.desc(),
            )
        ).scalars().all()
        agreements = session.execute(
            select(BuildingAgreement)
            .where(BuildingAgreement.package_checksum != "")
            .order_by(BuildingAgreement.created_at.desc())
        ).scalars().all()
        payments = {
            row.agreement_id: row
            for row in session.execute(
                select(BuildingPaymentRequestReadiness)
            ).scalars().all()
        }
    arena_review_template = next(
        (item for item in templates if item.id == ARENA_REVIEW_TEMPLATE_ID),
        None,
    )
    _, arena_review_checksum, _ = _arena_review_document()
    arena_review_status = (
        str(arena_review_template.status).replace("_", " ")
        if arena_review_template is not None
        else "not prepared"
    )
    esc = lambda value: html.escape(str(value or ""))
    template_rows = "".join(
        f"<tr><td>{esc(item.name)} v{item.version}</td><td>{esc(item.status)}</td>"
        f"<td>{esc(', '.join(item.merge_fields_json or []))}</td><td>{esc(item.template_reference)}</td></tr>"
        for item in templates
    ) or "<tr><td colspan='4'>No agreement templates yet.</td></tr>"
    readiness_rows = "".join(
        f"<tr><td>{esc(item.reservation_id)}</td><td>{esc(item.preparation_status)}</td>"
        f"<td><code>{esc(item.package_checksum[:12])}</code></td>"
        f"<td>{esc(payments[item.id].status if item.id in payments else 'missing')}</td>"
        f"<td>{esc(payments[item.id].currency if item.id in payments else '')} "
        f"{(payments[item.id].amount_cents / 100):,.2f}</td></tr>"
        for item in agreements
    ) or "<tr><td colspan='5'>No prepared packages yet.</td></tr>"
    nav = render_agent_nav("building", user=user)
    body = f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <title>Agreement readiness · Anata Agent</title>{render_agent_favicon_links()}
    <style>{render_agent_nav_styles()}
    body{{margin:0;background:#f5f7f8;color:#2b3644;font-family:Inter,Segoe UI,sans-serif}}
    main{{max-width:1320px;margin:auto;padding:28px 24px 60px}}h1,h2{{font-family:Montserrat,Inter,sans-serif}}
    .notice{{padding:12px 14px;background:#e9f7f5;border:1px solid #8ac9c1;margin:14px 0}}
    .error{{padding:12px 14px;background:#fff1ef;border:1px solid #d98b82;margin:14px 0}}
    section{{background:white;border:1px solid #d9e0e4;border-radius:12px;margin:18px 0;padding:20px}}
    .review-card{{border-color:#9fc7d8;background:linear-gradient(135deg,#fff 0,#f1f8fb 100%)}}
    .review-head{{display:flex;align-items:start;justify-content:space-between;gap:18px}}
    .status{{display:inline-flex;padding:5px 9px;border-radius:99px;background:#fff0d2;color:#845407;font-size:12px;font-weight:800;text-transform:capitalize}}
    .actions{{display:flex;align-items:end;flex-wrap:wrap;gap:10px;margin-top:16px}}
    .download{{display:inline-flex;align-items:center;min-height:42px;padding:0 16px;border:1px solid #aab5bd;border-radius:7px;background:#fff;color:#243746;font-weight:700;text-decoration:none}}
    table{{width:100%;border-collapse:collapse}}th,td{{text-align:left;padding:10px;border-bottom:1px solid #e4e8eb;vertical-align:top}}
    .grid{{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}}
    label{{display:grid;gap:5px;font-weight:650}}input,textarea,select{{min-height:42px;padding:8px;border:1px solid #aab5bd;border-radius:7px}}
    button{{min-height:42px;background:#243746;color:white;border:0;border-radius:7px;padding:0 16px;font-weight:700}}
    .wide{{grid-column:1/-1}}.muted{{color:#5f6d77;font-size:13px}}code{{font-size:12px}}
    @media(max-width:700px){{.grid{{grid-template-columns:1fr}}.wide{{grid-column:auto}}main{{padding-inline:16px}}.review-head{{display:block}}.status{{margin-top:10px}}}}
    </style></head><body>{nav}<main><a href="/admin/building">← Building Control</a>
    <h1>Agreement and payment readiness</h1>
    <p>Prepare frozen evidence for later provider handoff. Nothing here sends a contract, creates an invoice, charges a card, or confirms a booking.</p>
    {f'<div class="notice">{esc(request.query_params.get("notice"))}</div>' if request.query_params.get("notice") else ''}
    {f'<div class="error">{esc(request.query_params.get("error"))}</div>' if request.query_params.get("error") else ''}
    <section class="review-card">
      <div class="review-head">
        <div><h2>Arena agreement review package</h2>
        <p>Your approved business rules are consolidated into one reusable, versioned schedule for legal review. It replaces the Vivint-specific document as the starting point without pretending legal approval already exists.</p></div>
        <span class="status">{esc(arena_review_status)}</span>
      </div>
      <p class="muted">Artifact checksum: <code>{esc(arena_review_checksum)}</code>. Preparing it records an audited template in <strong>in review</strong> state. It does not approve, generate, send, sign, invoice, charge, hold a date, or confirm a booking.</p>
      <div class="actions">
        <a class="download" href="/admin/building/agreement-readiness/arena-review-package/download">Download business terms</a>
        <form method="post" action="/admin/building/agreement-readiness/arena-review-package/prepare">
          <input type="hidden" name="_csrf_token" value="{esc(csrf_token(user))}">
          <label>Typed confirmation
            <input name="confirmation" required placeholder="PREPARE ARENA AGREEMENT REVIEW">
          </label>
          <button type="submit">Prepare for legal review</button>
        </form>
      </div>
    </section>
    <section><h2>Template registry</h2><form class="grid" method="post" action="/admin/building/agreement-readiness/templates">
    <input type="hidden" name="_csrf_token" value="{esc(csrf_token(user))}">
    <label>Template ID<input name="template_id" required></label><label>Template key<input name="template_key" required></label>
    <label>Version<input name="version" type="number" min="1" value="1" required></label><label>Name<input name="name" required></label>
    <label class="wide">Repository or document reference<input name="template_reference" required placeholder="approved-repository:event-agreement-v1"></label>
    <label class="wide">Merge fields, comma separated<textarea name="merge_fields" required placeholder="customer_name, customer_email, event_space, setup_starts_at"></textarea></label>
    <div class="wide"><button type="submit">Save template draft</button></div></form>
    <form class="grid" method="post" action="/admin/building/agreement-readiness/templates/transition">
    <input type="hidden" name="_csrf_token" value="{esc(csrf_token(user))}">
    <label>Template ID<input name="template_id" required></label><label>Next state<select name="target_status"><option value="in_review">In review</option><option value="approved">Approved</option><option value="retired">Retired</option></select></label>
    <label class="wide">Typed confirmation<input name="confirmation" required placeholder="APPROVED TEMPLATE template-id"></label>
    <label class="wide">Approval evidence<textarea name="evidence" placeholder="Required for approval"></textarea></label>
    <div class="wide"><button type="submit">Change template state</button></div></form>
    <table><thead><tr><th>Template</th><th>Status</th><th>Allowed merge fields</th><th>Reference</th></tr></thead><tbody>{template_rows}</tbody></table></section>
    <section><h2>Prepare package</h2><form class="grid" method="post" action="/admin/building/agreement-readiness/packages">
    <input type="hidden" name="_csrf_token" value="{esc(csrf_token(user))}">
    <label>Reservation ID<input name="reservation_id" required></label>
    <label>Frozen quote ID<input name="quote_id" required></label>
    <label>Approved template ID<input name="template_id" required></label>
    <label>Idempotency key<input name="idempotency_key" minlength="8" maxlength="128" required></label>
    <label>Agreement version<input name="agreement_version" type="number" min="1" value="1"></label>
    <label>Payment readiness version<input name="payment_version" type="number" min="1" value="1"></label>
    <div class="wide"><button type="submit">Prepare immutable package</button> <span class="muted">Preparation creates no provider objects.</span></div>
    </form></section>
    <section><h2>Review and approve prepared evidence</h2><div class="grid">
    <form method="post" action="/admin/building/agreement-readiness/packages/transition">
    <input type="hidden" name="_csrf_token" value="{esc(csrf_token(user))}">
    <label>Agreement package ID<input name="agreement_id" required></label>
    <label>Next state<select name="target_status"><option value="in_review">In review</option><option value="approved">Approved</option></select></label>
    <label>Typed confirmation<input name="confirmation" required placeholder="REVIEW AGREEMENT package-id"></label>
    <button type="submit">Change agreement readiness</button></form>
    <form method="post" action="/admin/building/agreement-readiness/payments/transition">
    <input type="hidden" name="_csrf_token" value="{esc(csrf_token(user))}">
    <label>Payment readiness ID<input name="payment_id" required></label>
    <label>Next state<select name="target_status"><option value="in_review">In review</option><option value="approved">Approved</option></select></label>
    <label>Typed confirmation<input name="confirmation" required placeholder="APPROVE PAYMENT readiness-id"></label>
    <button type="submit">Change payment readiness</button></form></div>
    <p class="muted">Approval authorizes only a future provider handoff. It does not create a document, invoice, payment link, or charge.</p></section>
    <section><h2>Prepared records</h2><table><thead><tr><th>Reservation</th><th>Agreement</th><th>Checksum</th><th>Payment request</th><th>Required amount</th></tr></thead><tbody>{readiness_rows}</tbody></table>
    <p class="muted">Review and approval use the internal API typed confirmations shown in the documentation. This page intentionally offers no send, sign, invoice, or charge action.</p></section>
    </main></body></html>"""
    return HTMLResponse(body)


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
