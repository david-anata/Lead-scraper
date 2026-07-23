"""Audited financial exception workflows for building invoices."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal, Optional
from uuid import uuid4

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel, Field, model_validator
from sqlalchemy import func, select

from sales_support_agent.models.database import session_scope
from sales_support_agent.models.entities import (
    BuildingAuditEvent,
    BuildingBillingAdjustment,
    BuildingInvoice,
)


router = APIRouter(
    prefix="/api/internal/building/billing/adjustments",
    tags=["building-billing-adjustments"],
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _require_internal_key(request: Request, provided: Optional[str]) -> None:
    configured = str(
        getattr(request.app.state.settings, "internal_api_key", "") or ""
    ).strip()
    if not configured:
        raise HTTPException(status_code=503, detail="Internal API is not configured.")
    if str(provided or "").strip() != configured:
        raise HTTPException(status_code=401, detail="Invalid internal API key.")


class AdjustmentRequestInput(BaseModel):
    invoice_id: str = Field(min_length=1, max_length=64)
    adjustment_type: Literal["refund", "credit", "write_off"]
    amount_cents: int = Field(gt=0)
    reason: str = Field(min_length=10, max_length=2000)
    actor: str = Field(min_length=1, max_length=255)


class AdjustmentApprovalInput(BaseModel):
    actor: str = Field(min_length=1, max_length=255)


class AdjustmentEvidenceInput(BaseModel):
    status: Literal["provider_confirmed", "accounting_confirmed", "voided"]
    provider_reference: str = Field(default="", max_length=255)
    qbo_reference: str = Field(default="", max_length=255)
    note: str = Field(min_length=3, max_length=2000)
    actor: str = Field(min_length=1, max_length=255)

    @model_validator(mode="after")
    def evidence_matches_status(self) -> "AdjustmentEvidenceInput":
        if self.status == "provider_confirmed" and not self.provider_reference.strip():
            raise ValueError("Provider-confirmed adjustments require a provider reference.")
        if self.status == "accounting_confirmed" and not self.qbo_reference.strip():
            raise ValueError("Accounting-confirmed adjustments require a QBO reference.")
        return self


def _payload(row: BuildingBillingAdjustment) -> dict[str, Any]:
    return {
        "id": row.id,
        "invoice_id": row.invoice_id,
        "adjustment_type": row.adjustment_type,
        "amount_cents": row.amount_cents,
        "currency": row.currency,
        "status": row.status,
        "reason": row.reason,
        "provider_reference": row.provider_reference,
        "qbo_reference": row.qbo_reference,
        "requested_by": row.requested_by,
        "approved_by": row.approved_by,
        "confirmed_by": row.confirmed_by,
        "created_at": row.created_at.isoformat(),
        "updated_at": row.updated_at.isoformat(),
    }


@router.get("")
def list_adjustments(
    request: Request,
    invoice_id: str = "",
    status: str = "",
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        query = select(BuildingBillingAdjustment).order_by(
            BuildingBillingAdjustment.created_at.desc()
        )
        if invoice_id:
            query = query.where(BuildingBillingAdjustment.invoice_id == invoice_id)
        if status:
            query = query.where(BuildingBillingAdjustment.status == status)
        rows = session.execute(query).scalars().all()
        return {"adjustments": [_payload(row) for row in rows]}


def _committed_adjustment_total(session, invoice_id: str, adjustment_type: str) -> int:
    return int(
        session.execute(
            select(func.coalesce(func.sum(BuildingBillingAdjustment.amount_cents), 0))
            .where(
                BuildingBillingAdjustment.invoice_id == invoice_id,
                BuildingBillingAdjustment.adjustment_type == adjustment_type,
                BuildingBillingAdjustment.status.in_(
                    ("approved", "provider_confirmed", "accounting_confirmed")
                ),
            )
        ).scalar_one()
        or 0
    )


@router.post("", status_code=201)
def request_adjustment(
    payload: AdjustmentRequestInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        invoice = session.get(BuildingInvoice, payload.invoice_id)
        if invoice is None:
            raise HTTPException(status_code=404, detail="Invoice not found.")
        already_committed = _committed_adjustment_total(
            session, invoice.id, payload.adjustment_type
        )
        if payload.adjustment_type == "refund":
            available = max(0, invoice.amount_paid_cents - already_committed)
            evidence_label = "provider-confirmed paid amount"
        else:
            outstanding = max(
                0, invoice.amount_due_cents - invoice.amount_paid_cents
            )
            available = max(0, outstanding - already_committed)
            evidence_label = "remaining invoice balance"
        if payload.amount_cents > available:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Adjustment exceeds the {evidence_label}. "
                    f"Available amount is {available} cents."
                ),
            )
        now = _now()
        row = BuildingBillingAdjustment(
            id=str(uuid4()),
            invoice_id=invoice.id,
            adjustment_type=payload.adjustment_type,
            amount_cents=payload.amount_cents,
            currency=invoice.currency,
            reason=payload.reason.strip(),
            requested_by=payload.actor,
            created_at=now,
            updated_at=now,
        )
        session.add(row)
        session.add(
            BuildingAuditEvent(
                entity_type="billing_adjustment",
                entity_id=row.id,
                action="adjustment_requested",
                actor=payload.actor,
                after_json={
                    "invoice_id": invoice.id,
                    "adjustment_type": row.adjustment_type,
                    "amount_cents": row.amount_cents,
                    "reason": row.reason,
                },
            )
        )
        return {"ok": True, "adjustment": _payload(row)}


@router.post("/{adjustment_id}/approve")
def approve_adjustment(
    adjustment_id: str,
    payload: AdjustmentApprovalInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        row = session.get(BuildingBillingAdjustment, adjustment_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Adjustment not found.")
        if row.status != "requested":
            raise HTTPException(status_code=409, detail="Adjustment is not awaiting approval.")
        if row.requested_by.strip().lower() == payload.actor.strip().lower():
            raise HTTPException(
                status_code=409,
                detail="A different finance operator must approve this adjustment.",
            )
        row.status = "approved"
        row.approved_by = payload.actor
        row.approved_at = _now()
        row.updated_at = _now()
        session.add(
            BuildingAuditEvent(
                entity_type="billing_adjustment",
                entity_id=row.id,
                action="adjustment_approved",
                actor=payload.actor,
                before_json={"status": "requested"},
                after_json={"status": "approved"},
            )
        )
        return {"ok": True, "adjustment": _payload(row)}


@router.post("/{adjustment_id}/evidence")
def record_adjustment_evidence(
    adjustment_id: str,
    payload: AdjustmentEvidenceInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        row = session.get(BuildingBillingAdjustment, adjustment_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Adjustment not found.")
        if row.status != "approved":
            raise HTTPException(
                status_code=409,
                detail="Only an approved adjustment can receive final evidence.",
            )
        if (
            row.adjustment_type == "refund"
            and payload.status == "accounting_confirmed"
        ):
            raise HTTPException(
                status_code=409,
                detail="A refund requires payment-provider evidence before accounting confirmation.",
            )
        if (
            row.adjustment_type in {"credit", "write_off"}
            and payload.status == "provider_confirmed"
        ):
            raise HTTPException(
                status_code=409,
                detail="Credits and write-offs require accounting evidence, not payment-provider evidence.",
            )
        before = {"status": row.status}
        row.status = payload.status
        row.provider_reference = payload.provider_reference.strip()
        row.qbo_reference = payload.qbo_reference.strip()
        row.evidence_json = {"note": payload.note.strip()}
        row.confirmed_by = payload.actor
        row.confirmed_at = _now()
        row.updated_at = _now()
        session.add(
            BuildingAuditEvent(
                entity_type="billing_adjustment",
                entity_id=row.id,
                action=f"adjustment_{payload.status}",
                actor=payload.actor,
                before_json=before,
                after_json={
                    "status": row.status,
                    "provider_reference": row.provider_reference,
                    "qbo_reference": row.qbo_reference,
                    "note": payload.note.strip(),
                },
            )
        )
        return {"ok": True, "adjustment": _payload(row)}
