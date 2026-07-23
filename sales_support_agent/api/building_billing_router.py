"""Native building billing schedules with Stripe collection evidence."""

from __future__ import annotations

import calendar
from datetime import date, datetime, timedelta, timezone
from typing import Any, Literal, Optional
from uuid import NAMESPACE_URL, uuid4, uuid5

from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import select

from sales_support_agent.integrations.stripe_billing import (
    StripeBillingClient,
    StripeBillingError,
)
from sales_support_agent.models.database import session_scope
from sales_support_agent.models.entities import (
    BuildingAuditEvent,
    BuildingBillingAccount,
    BuildingBillingSchedule,
    BuildingContact,
    BuildingDepositEvidence,
    BuildingInvoice,
    BuildingPayment,
    BuildingReservation,
    BuildingStripeEvent,
)


internal_router = APIRouter(prefix="/api/internal/building/billing", tags=["building-billing"])
webhook_router = APIRouter(prefix="/api/integrations/stripe", tags=["stripe-webhook"])

SCHEDULE_TYPES = {"one_time", "monthly", "deposit", "final_balance"}
COLLECTION_METHODS = {"send_invoice", "charge_automatically"}
SCHEDULE_STATUSES = {"draft", "approved", "paused", "completed", "cancelled"}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _require_internal_key(request: Request, provided: Optional[str]) -> None:
    configured = str(getattr(request.app.state.settings, "internal_api_key", "") or "").strip()
    if not configured:
        raise HTTPException(status_code=503, detail="Internal API is not configured.")
    if str(provided or "").strip() != configured:
        raise HTTPException(status_code=401, detail="Invalid internal API key.")


def _add_month(value: date) -> date:
    year = value.year + (1 if value.month == 12 else 0)
    month = 1 if value.month == 12 else value.month + 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


class BillingAccountInput(BaseModel):
    id: str = Field(min_length=1, max_length=64)
    contact_id: str | None = Field(default=None, max_length=64)
    account_name: str = Field(min_length=1, max_length=255)
    billing_email: str = Field(min_length=3, max_length=255)
    qbo_customer_id: str = Field(default="", max_length=64)
    metadata: dict[str, Any] = Field(default_factory=dict)
    actor: str = Field(min_length=1, max_length=255)

    @field_validator("billing_email")
    @classmethod
    def valid_email(cls, value: str) -> str:
        email = value.strip().lower()
        if "@" not in email or "." not in email.rsplit("@", 1)[-1]:
            raise ValueError("Enter a valid billing email.")
        return email


class BillingScheduleInput(BaseModel):
    id: str = Field(min_length=1, max_length=64)
    billing_account_id: str = Field(min_length=1, max_length=64)
    reservation_id: str | None = Field(default=None, max_length=64)
    schedule_type: str
    description: str = Field(min_length=1, max_length=512)
    amount_cents: int = Field(gt=0)
    currency: str = Field(default="usd", pattern=r"^[a-z]{3}$")
    collection_method: str = "send_invoice"
    days_until_due: int = Field(default=7, ge=1, le=90)
    starts_on: date
    ends_on: date | None = None
    actor: str = Field(min_length=1, max_length=255)

    @field_validator("schedule_type")
    @classmethod
    def valid_schedule_type(cls, value: str) -> str:
        if value not in SCHEDULE_TYPES:
            raise ValueError("Unsupported schedule type.")
        return value

    @field_validator("collection_method")
    @classmethod
    def valid_collection_method(cls, value: str) -> str:
        if value not in COLLECTION_METHODS:
            raise ValueError("Unsupported collection method.")
        return value


class ScheduleApprovalInput(BaseModel):
    actor: str = Field(min_length=1, max_length=255)


class InvoiceRunInput(BaseModel):
    schedule_id: str = Field(min_length=1, max_length=64)
    idempotency_key: str = Field(min_length=8, max_length=128)
    execute: bool = False
    actor: str = Field(min_length=1, max_length=255)


class AccountingLinkInput(BaseModel):
    qbo_invoice_id: str = Field(default="", max_length=64)
    accounting_status: Literal["pending_qbo", "synced_qbo", "reconciled", "failed"]
    note: str = Field(default="", max_length=1000)
    actor: str = Field(min_length=1, max_length=255)


def _invoice_payload(row: BuildingInvoice) -> dict[str, Any]:
    return {
        "id": row.id,
        "billing_account_id": row.billing_account_id,
        "billing_schedule_id": row.billing_schedule_id,
        "reservation_id": row.reservation_id,
        "provider": row.provider,
        "provider_invoice_id": row.provider_invoice_id,
        "status": row.status,
        "accounting_status": row.accounting_status,
        "amount_due_cents": row.amount_due_cents,
        "amount_paid_cents": row.amount_paid_cents,
        "currency": row.currency,
        "due_at": row.due_at.isoformat() if row.due_at else None,
        "hosted_invoice_url": row.hosted_invoice_url,
        "qbo_invoice_id": row.qbo_invoice_id,
    }


@internal_router.put("/accounts/{account_id}")
def upsert_billing_account(
    account_id: str,
    payload: BillingAccountInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    if payload.id != account_id:
        raise HTTPException(status_code=422, detail="Billing account ID does not match route.")
    with session_scope(request.app.state.session_factory) as session:
        if payload.contact_id and session.get(BuildingContact, payload.contact_id) is None:
            raise HTTPException(status_code=422, detail="Unknown contact.")
        row = session.get(BuildingBillingAccount, account_id)
        before = {"billing_email": row.billing_email} if row else {}
        if row is None:
            row = BuildingBillingAccount(
                id=account_id,
                account_name=payload.account_name,
                billing_email=payload.billing_email,
            )
        row.contact_id = payload.contact_id
        row.account_name = payload.account_name
        row.billing_email = payload.billing_email
        row.qbo_customer_id = payload.qbo_customer_id
        row.metadata_json = payload.metadata
        row.updated_at = _now()
        session.add(row)
        session.add(BuildingAuditEvent(
            entity_type="billing_account",
            entity_id=row.id,
            action="upserted",
            actor=payload.actor,
            before_json=before,
            after_json={
                "billing_email": row.billing_email,
                "contact_id": row.contact_id,
                "qbo_customer_id": row.qbo_customer_id,
            },
        ))
        return {
            "ok": True,
            "account": {
                "id": row.id,
                "account_name": row.account_name,
                "billing_email": row.billing_email,
                "stripe_customer_id": row.stripe_customer_id,
                "qbo_customer_id": row.qbo_customer_id,
            },
        }


@internal_router.put("/schedules/{schedule_id}")
def upsert_billing_schedule(
    schedule_id: str,
    payload: BillingScheduleInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    if payload.id != schedule_id:
        raise HTTPException(status_code=422, detail="Schedule ID does not match route.")
    if payload.ends_on and payload.ends_on < payload.starts_on:
        raise HTTPException(status_code=422, detail="Schedule end precedes start.")
    with session_scope(request.app.state.session_factory) as session:
        if session.get(BuildingBillingAccount, payload.billing_account_id) is None:
            raise HTTPException(status_code=422, detail="Unknown billing account.")
        if payload.reservation_id and session.get(BuildingReservation, payload.reservation_id) is None:
            raise HTTPException(status_code=422, detail="Unknown reservation.")
        row = session.get(BuildingBillingSchedule, schedule_id)
        if row and row.status not in {"draft", "paused"}:
            raise HTTPException(status_code=409, detail="Approved billing schedules are immutable.")
        if row is None:
            row = BuildingBillingSchedule(
                id=schedule_id,
                billing_account_id=payload.billing_account_id,
                schedule_type=payload.schedule_type,
                description=payload.description,
                amount_cents=payload.amount_cents,
                starts_on=payload.starts_on,
                created_by=payload.actor,
            )
        row.billing_account_id = payload.billing_account_id
        row.reservation_id = payload.reservation_id
        row.schedule_type = payload.schedule_type
        row.description = payload.description
        row.amount_cents = payload.amount_cents
        row.currency = payload.currency
        row.collection_method = payload.collection_method
        row.days_until_due = payload.days_until_due
        row.starts_on = payload.starts_on
        row.ends_on = payload.ends_on
        row.next_invoice_on = payload.starts_on
        row.status = "draft"
        row.updated_at = _now()
        session.add(row)
        session.add(BuildingAuditEvent(
            entity_type="billing_schedule",
            entity_id=row.id,
            action="draft_saved",
            actor=payload.actor,
            after_json={
                "schedule_type": row.schedule_type,
                "amount_cents": row.amount_cents,
                "starts_on": row.starts_on.isoformat(),
                "collection_method": row.collection_method,
            },
        ))
        return {"ok": True, "schedule_id": row.id, "status": row.status}


@internal_router.post("/schedules/{schedule_id}/approve")
def approve_billing_schedule(
    schedule_id: str,
    payload: ScheduleApprovalInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        row = session.get(BuildingBillingSchedule, schedule_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Billing schedule not found.")
        if row.status != "draft":
            raise HTTPException(status_code=409, detail="Only draft schedules can be approved.")
        row.status = "approved"
        row.approved_by = payload.actor
        row.approved_at = _now()
        row.updated_at = _now()
        session.add(BuildingAuditEvent(
            entity_type="billing_schedule",
            entity_id=row.id,
            action="approved",
            actor=payload.actor,
            after_json={"amount_cents": row.amount_cents, "next_invoice_on": row.next_invoice_on.isoformat()},
        ))
        return {"ok": True, "schedule_id": row.id, "status": row.status}


@internal_router.post("/invoices")
def create_invoice_from_schedule(
    payload: InvoiceRunInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        existing = session.execute(
            select(BuildingInvoice).where(
                BuildingInvoice.idempotency_key == payload.idempotency_key
            )
        ).scalar_one_or_none()
        if existing:
            return {"ok": True, "duplicate": True, "invoice": _invoice_payload(existing)}
        schedule = session.get(BuildingBillingSchedule, payload.schedule_id)
        if schedule is None:
            raise HTTPException(status_code=404, detail="Billing schedule not found.")
        if schedule.status != "approved":
            raise HTTPException(status_code=409, detail="Billing schedule must be approved.")
        account = session.get(BuildingBillingAccount, schedule.billing_account_id)
        if account is None or account.status != "active":
            raise HTTPException(status_code=409, detail="Billing account is unavailable.")
        proposal = {
            "schedule_id": schedule.id,
            "account_id": account.id,
            "billing_email": account.billing_email,
            "description": schedule.description,
            "amount_cents": schedule.amount_cents,
            "currency": schedule.currency,
            "collection_method": schedule.collection_method,
            "days_until_due": schedule.days_until_due,
            "next_invoice_on": schedule.next_invoice_on.isoformat() if schedule.next_invoice_on else None,
            "accounting_destination": "quickbooks",
        }
        if not payload.execute:
            return {"ok": True, "execute": False, "proposal": proposal}

        client = StripeBillingClient(request.app.state.settings)
        if not client.is_configured:
            raise HTTPException(status_code=503, detail="Stripe billing is not configured.")
        invoice_id = str(uuid5(NAMESPACE_URL, f"building-invoice:{payload.idempotency_key}"))
        if not account.stripe_customer_id:
            try:
                customer = client.create_customer(
                    email=account.billing_email,
                    name=account.account_name,
                    internal_account_id=account.id,
                    idempotency_key=f"{payload.idempotency_key}:customer",
                )
            except StripeBillingError as exc:
                raise HTTPException(status_code=502, detail=str(exc)) from exc
            account.stripe_customer_id = str(customer.get("id") or "")
            if not account.stripe_customer_id:
                raise HTTPException(status_code=502, detail="Stripe customer creation returned no ID.")
        try:
            provider_invoice = client.create_invoice(
                customer_id=account.stripe_customer_id,
                amount_cents=schedule.amount_cents,
                currency=schedule.currency,
                description=schedule.description,
                collection_method=schedule.collection_method,
                days_until_due=schedule.days_until_due,
                internal_invoice_id=invoice_id,
                idempotency_key=payload.idempotency_key,
            )
        except StripeBillingError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        provider_id = str(provider_invoice.get("id") or "")
        if not provider_id:
            raise HTTPException(status_code=502, detail="Stripe invoice creation returned no ID.")
        due_timestamp = provider_invoice.get("due_date")
        due_at = (
            datetime.fromtimestamp(int(due_timestamp), tz=timezone.utc)
            if due_timestamp
            else _now() + timedelta(days=schedule.days_until_due)
        )
        row = BuildingInvoice(
            id=invoice_id,
            billing_account_id=account.id,
            billing_schedule_id=schedule.id,
            reservation_id=schedule.reservation_id,
            idempotency_key=payload.idempotency_key,
            provider="stripe",
            provider_invoice_id=provider_id,
            description=schedule.description,
            status=str(provider_invoice.get("status") or "draft"),
            accounting_status="pending_qbo",
            amount_due_cents=int(provider_invoice.get("amount_due") or schedule.amount_cents),
            amount_paid_cents=int(provider_invoice.get("amount_paid") or 0),
            currency=str(provider_invoice.get("currency") or schedule.currency),
            due_at=due_at,
            hosted_invoice_url=str(provider_invoice.get("hosted_invoice_url") or ""),
            provider_payload_json=provider_invoice,
            created_by=payload.actor,
        )
        session.add(row)
        schedule.last_invoice_on = date.today()
        if schedule.schedule_type == "monthly":
            schedule.next_invoice_on = _add_month(schedule.next_invoice_on or date.today())
            if schedule.ends_on and schedule.next_invoice_on > schedule.ends_on:
                schedule.status = "completed"
                schedule.next_invoice_on = None
        else:
            schedule.status = "completed"
            schedule.next_invoice_on = None
        schedule.updated_at = _now()
        session.add(BuildingAuditEvent(
            entity_type="invoice",
            entity_id=row.id,
            action="created_in_stripe",
            actor=payload.actor,
            after_json={
                "provider_invoice_id": row.provider_invoice_id,
                "amount_due_cents": row.amount_due_cents,
                "accounting_status": row.accounting_status,
            },
        ))
        return {"ok": True, "duplicate": False, "invoice": _invoice_payload(row)}


@internal_router.get("/invoices")
def list_invoices(
    request: Request,
    status: str = "",
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        query = select(BuildingInvoice).order_by(BuildingInvoice.created_at.desc())
        if status:
            query = query.where(BuildingInvoice.status == status)
        rows = session.execute(query).scalars().all()
        return {"invoices": [_invoice_payload(row) for row in rows]}


@internal_router.get("/qbo-export")
def qbo_export_queue(
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Return controlled invoice facts for the existing QBO accounting process."""

    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        rows = session.execute(
            select(BuildingInvoice)
            .where(BuildingInvoice.accounting_status.in_(["pending_qbo", "failed"]))
            .order_by(BuildingInvoice.created_at)
        ).scalars().all()
        account_ids = {row.billing_account_id for row in rows}
        accounts = (
            {
                row.id: row
                for row in session.execute(
                    select(BuildingBillingAccount).where(
                        BuildingBillingAccount.id.in_(account_ids)
                    )
                ).scalars().all()
            }
            if account_ids
            else {}
        )
        return {
            "source": "agent_building",
            "destination": "quickbooks",
            "invoices": [
                {
                    **_invoice_payload(row),
                    "account_name": accounts[row.billing_account_id].account_name,
                    "billing_email": accounts[row.billing_account_id].billing_email,
                    "qbo_customer_id": accounts[row.billing_account_id].qbo_customer_id,
                    "description": row.description,
                    "evidence_note": (
                        "Provider invoice state; accounting posting must be confirmed in QBO."
                    ),
                }
                for row in rows
            ],
        }


@internal_router.put("/invoices/{invoice_id}/accounting-link")
def record_accounting_link(
    invoice_id: str,
    payload: AccountingLinkInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Record the reviewed QBO result without pretending Agent is the ledger."""

    _require_internal_key(request, x_internal_api_key)
    if payload.accounting_status in {"synced_qbo", "reconciled"} and not payload.qbo_invoice_id:
        raise HTTPException(
            status_code=422,
            detail="A QBO invoice ID is required for synced or reconciled status.",
        )
    with session_scope(request.app.state.session_factory) as session:
        row = session.get(BuildingInvoice, invoice_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Invoice not found.")
        before = {
            "accounting_status": row.accounting_status,
            "qbo_invoice_id": row.qbo_invoice_id,
        }
        row.accounting_status = payload.accounting_status
        row.qbo_invoice_id = payload.qbo_invoice_id
        row.updated_at = _now()
        session.add(BuildingAuditEvent(
            entity_type="invoice",
            entity_id=row.id,
            action="accounting_link_updated",
            actor=payload.actor,
            before_json=before,
            after_json={
                "accounting_status": row.accounting_status,
                "qbo_invoice_id": row.qbo_invoice_id,
                "note": payload.note,
            },
        ))
        return {"ok": True, "invoice": _invoice_payload(row)}


@webhook_router.post("/webhook")
async def stripe_webhook(
    request: Request,
    stripe_signature: Optional[str] = Header(default=None, alias="Stripe-Signature"),
) -> JSONResponse:
    payload = await request.body()
    client = StripeBillingClient(request.app.state.settings)
    try:
        event = client.verify_webhook(
            payload=payload,
            signature_header=stripe_signature or "",
        )
    except StripeBillingError as exc:
        return JSONResponse(status_code=400, content={"ok": False, "error": str(exc)})
    event_id = str(event["id"])
    event_type = str(event["type"])
    with session_scope(request.app.state.session_factory) as session:
        existing_event = session.get(BuildingStripeEvent, event_id)
        if existing_event is not None:
            return JSONResponse(content={"ok": True, "duplicate": True})
        event_row = BuildingStripeEvent(
            event_id=event_id,
            event_type=event_type,
            payload_json=event,
        )
        session.add(event_row)
        data_object = ((event.get("data") or {}).get("object") or {})
        if not isinstance(data_object, dict):
            data_object = {}
        internal_invoice_id = str(
            ((data_object.get("metadata") or {}).get("building_invoice_id") or "")
        )
        provider_invoice_id = str(data_object.get("id") or "")
        invoice = None
        if internal_invoice_id:
            invoice = session.get(BuildingInvoice, internal_invoice_id)
        if invoice is None and provider_invoice_id:
            invoice = session.execute(
                select(BuildingInvoice).where(
                    BuildingInvoice.provider_invoice_id == provider_invoice_id
                )
            ).scalar_one_or_none()
        try:
            if event_type.startswith("invoice.") and invoice is not None:
                status_map = {
                    "invoice.finalized": "open",
                    "invoice.paid": "paid",
                    "invoice.payment_succeeded": "paid",
                    "invoice.payment_failed": "open",
                    "invoice.voided": "void",
                    "invoice.marked_uncollectible": "uncollectible",
                }
                invoice.status = status_map.get(
                    event_type, str(data_object.get("status") or invoice.status)
                )
                invoice.amount_due_cents = int(
                    data_object.get("amount_due") or invoice.amount_due_cents
                )
                invoice.amount_paid_cents = int(
                    data_object.get("amount_paid") or invoice.amount_paid_cents
                )
                invoice.hosted_invoice_url = str(
                    data_object.get("hosted_invoice_url") or invoice.hosted_invoice_url
                )
                invoice.provider_payload_json = data_object
                invoice.updated_at = _now()
                if invoice.status == "paid":
                    payment_reference = str(
                        data_object.get("payment_intent")
                        or data_object.get("charge")
                        or event_id
                    )
                    payment = session.execute(
                        select(BuildingPayment).where(
                            BuildingPayment.provider_payment_id == payment_reference
                        )
                    ).scalar_one_or_none()
                    if payment is None:
                        payment = BuildingPayment(
                            id=str(uuid4()),
                            invoice_id=invoice.id,
                            provider="stripe",
                            provider_payment_id=payment_reference,
                            status="paid",
                            amount_cents=invoice.amount_paid_cents,
                            currency=invoice.currency,
                            evidence_class="provider_confirmed",
                            provider_payload_json=data_object,
                        )
                    payment.posted_at = _now()
                    payment.updated_at = _now()
                    session.add(payment)
                    schedule = (
                        session.get(BuildingBillingSchedule, invoice.billing_schedule_id)
                        if invoice.billing_schedule_id
                        else None
                    )
                    if (
                        schedule
                        and schedule.schedule_type == "deposit"
                        and invoice.reservation_id
                    ):
                        reservation = session.get(
                            BuildingReservation, invoice.reservation_id
                        )
                        if reservation:
                            reservation.deposit_status = "paid"
                            reservation.updated_at = _now()
                            existing_deposit = session.execute(
                                select(BuildingDepositEvidence).where(
                                    BuildingDepositEvidence.provider_reference
                                    == payment_reference
                                )
                            ).scalar_one_or_none()
                            if existing_deposit is None:
                                session.add(BuildingDepositEvidence(
                                    id=str(uuid4()),
                                    reservation_id=reservation.id,
                                    status="paid",
                                    amount_cents=invoice.amount_paid_cents,
                                    provider="stripe",
                                    provider_reference=payment_reference,
                                    evidence_json={"invoice_id": invoice.id, "event_id": event_id},
                                    recorded_by="stripe-webhook",
                                ))
                session.add(BuildingAuditEvent(
                    entity_type="invoice",
                    entity_id=invoice.id,
                    action=event_type,
                    actor="stripe-webhook",
                    after_json={
                        "status": invoice.status,
                        "amount_paid_cents": invoice.amount_paid_cents,
                        "provider_event_id": event_id,
                    },
                ))
            event_row.status = "processed"
            event_row.processed_at = _now()
        except Exception as exc:  # noqa: BLE001 - persist provider event for retry
            event_row.status = "failed"
            event_row.error_message = str(exc)[:1000]
            return JSONResponse(status_code=500, content={"ok": False, "error": "Webhook processing failed."})
        return JSONResponse(content={"ok": True, "duplicate": False})
