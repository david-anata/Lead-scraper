"""Native building billing schedules with Stripe collection evidence."""

from __future__ import annotations

import calendar
import hashlib
import json
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
from sales_support_agent.integrations.building_quickbooks import (
    BuildingQuickBooksClient,
    BuildingQuickBooksError,
)
from sales_support_agent.integrations.resend import ResendClient
from sales_support_agent.models.database import session_scope
from sales_support_agent.models.entities import (
    BuildingAuditEvent,
    BuildingAgreement,
    BuildingBillingAccount,
    BuildingBillingSchedule,
    BuildingCollectionCase,
    BuildingContact,
    BuildingDepositEvidence,
    BuildingInvoice,
    BuildingPayment,
    BuildingPaymentRequestReadiness,
    BuildingProposal,
    BuildingReservation,
    BuildingStripeEvent,
    BuildingSuppression,
)
from sales_support_agent.services.building_transactional_messages import (
    attempt_booking_message,
)


internal_router = APIRouter(prefix="/api/internal/building/billing", tags=["building-billing"])
webhook_router = APIRouter(prefix="/api/integrations/stripe", tags=["stripe-webhook"])

SCHEDULE_TYPES = {
    "one_time",
    "monthly",
    "deposit",
    "final_balance",
    "security_deposit",
}
COLLECTION_METHODS = {"send_invoice", "charge_automatically"}
SCHEDULE_STATUSES = {"draft", "approved", "paused", "completed", "cancelled"}
COLLECTION_STATUSES = {
    "open",
    "contacted",
    "promised",
    "disputed",
    "resolved",
    "waived",
}
COLLECTION_TERMINAL_STATUSES = {"resolved", "waived"}


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


class ScheduleFromProposalInput(BaseModel):
    """Everything except the money, which only the accepted quote may set."""

    id: str = Field(min_length=1, max_length=64)
    proposal_id: str = Field(min_length=1, max_length=64)
    billing_account_id: str = Field(min_length=1, max_length=64)
    schedule_type: str = "one_time"
    description: str = Field(default="", max_length=512)
    collection_method: str = "send_invoice"
    days_until_due: int = Field(default=7, ge=1, le=90)
    starts_on: date
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


class EventBillingPreparationInput(BaseModel):
    actor: str = Field(min_length=1, max_length=255)


def _quote_checksum(proposal: BuildingProposal) -> str:
    evidence = {
        "id": proposal.id,
        "version": proposal.version,
        "status": proposal.status,
        "amount_cents": proposal.amount_cents,
        "currency": proposal.currency,
        "line_items": list(proposal.line_items_json or []),
        "rate_plan_id": proposal.rate_plan_id,
        "rate_plan_snapshot": dict(proposal.rate_plan_snapshot_json or {}),
    }
    return hashlib.sha256(
        json.dumps(evidence, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


class AccountingLinkInput(BaseModel):
    qbo_invoice_id: str = Field(default="", max_length=64)
    accounting_status: Literal["pending_qbo", "synced_qbo", "reconciled", "failed"]
    note: str = Field(default="", max_length=1000)
    actor: str = Field(min_length=1, max_length=255)


class CollectionRefreshInput(BaseModel):
    execute: bool = False
    default_owner: str = Field(default="", max_length=255)
    actor: str = Field(min_length=1, max_length=255)


class CollectionTransitionInput(BaseModel):
    status: Literal["open", "contacted", "promised", "disputed", "resolved", "waived"]
    assigned_owner: str = Field(default="", max_length=255)
    next_action_at: datetime | None = None
    notes: str = Field(default="", max_length=4000)
    resolution: str = Field(default="", max_length=4000)
    actor: str = Field(min_length=1, max_length=255)


class CollectionReminderInput(BaseModel):
    confirmation: str = Field(min_length=1, max_length=255)
    next_action_at: datetime | None = None
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


def _collection_payload(
    case: BuildingCollectionCase,
    invoice: BuildingInvoice,
    account: BuildingBillingAccount,
) -> dict[str, Any]:
    now = _now()
    due_at = invoice.due_at
    comparable_due = (
        due_at.replace(tzinfo=timezone.utc)
        if due_at is not None and due_at.tzinfo is None
        else due_at
    )
    days_overdue = (
        max(0, (now.date() - comparable_due.date()).days)
        if comparable_due
        else 0
    )
    return {
        "id": case.id,
        "invoice_id": invoice.id,
        "billing_account_id": account.id,
        "account_name": account.account_name,
        "billing_email": account.billing_email,
        "status": case.status,
        "assigned_owner": case.assigned_owner,
        "next_action_at": (
            case.next_action_at.isoformat() if case.next_action_at else None
        ),
        "notes": case.notes,
        "reminder_count": case.reminder_count,
        "last_reminder_at": (
            case.last_reminder_at.isoformat() if case.last_reminder_at else None
        ),
        "resolution": case.resolution,
        "invoice_status": invoice.status,
        "amount_due_cents": invoice.amount_due_cents,
        "amount_paid_cents": invoice.amount_paid_cents,
        "outstanding_cents": max(
            0, invoice.amount_due_cents - invoice.amount_paid_cents
        ),
        "currency": invoice.currency,
        "due_at": invoice.due_at.isoformat() if invoice.due_at else None,
        "days_overdue": days_overdue,
        "hosted_invoice_url": invoice.hosted_invoice_url,
    }


def _outstanding_overdue_invoices(session) -> list[BuildingInvoice]:
    now = _now()
    rows = session.execute(
        select(BuildingInvoice).where(
            BuildingInvoice.due_at.is_not(None),
            BuildingInvoice.due_at < now,
            BuildingInvoice.amount_paid_cents < BuildingInvoice.amount_due_cents,
            BuildingInvoice.status.not_in(
                {"paid", "void", "voided", "uncollectible"}
            ),
        )
    ).scalars().all()
    return rows


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


@internal_router.post("/reservations/{reservation_id}/prepare", status_code=201)
def prepare_event_billing(
    reservation_id: str,
    payload: EventBillingPreparationInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Prepare an approved booking package's exact billing components.

    This creates only Agent billing drafts. It neither creates a QuickBooks
    customer nor writes or sends an invoice.
    """

    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        reservation = session.get(BuildingReservation, reservation_id)
        if reservation is None or reservation.kind != "event":
            raise HTTPException(status_code=404, detail="Event booking not found.")
        contact = (
            session.get(BuildingContact, reservation.contact_id)
            if reservation.contact_id
            else None
        )
        if contact is None or contact.status != "active":
            raise HTTPException(status_code=409, detail="An active billing contact is required.")
        agreement = session.execute(
            select(BuildingAgreement)
            .where(BuildingAgreement.reservation_id == reservation_id)
            .order_by(BuildingAgreement.version.desc())
        ).scalars().first()
        if agreement is None or agreement.preparation_status != "approved":
            raise HTTPException(
                status_code=409,
                detail="An approved current agreement package is required before billing.",
            )
        payment_readiness = session.execute(
            select(BuildingPaymentRequestReadiness)
            .where(BuildingPaymentRequestReadiness.reservation_id == reservation_id)
            .order_by(BuildingPaymentRequestReadiness.version.desc())
        ).scalars().first()
        if payment_readiness is None or payment_readiness.status != "approved":
            raise HTTPException(status_code=409, detail="Approve payment readiness first.")
        quote_id = str(
            (agreement.package_snapshot_json or {}).get("quote", {}).get("id") or ""
        )
        proposal = session.get(BuildingProposal, quote_id) if quote_id else None
        if proposal is None:
            raise HTTPException(status_code=409, detail="The frozen quote is unavailable.")
        frozen_quote = dict((agreement.package_snapshot_json or {}).get("quote") or {})
        if (
            frozen_quote.get("version") != proposal.version
            or int(frozen_quote.get("amount_cents") or 0) != proposal.amount_cents
        ):
            raise HTTPException(
                status_code=409,
                detail="The current quote differs from the approved agreement package.",
            )

        account_id = str(uuid5(NAMESPACE_URL, f"building-billing-contact:{contact.id}"))
        account = session.get(BuildingBillingAccount, account_id)
        if account is None:
            account = BuildingBillingAccount(
                id=account_id,
                contact_id=contact.id,
                account_name=contact.full_name,
                billing_email=contact.email.strip().lower(),
                metadata_json={"source": "approved_event_agreement"},
            )
            session.add(account)
        elif account.contact_id != contact.id:
            raise HTTPException(status_code=409, detail="Billing account identity conflict.")

        rate = dict(proposal.rate_plan_snapshot_json or {})
        commercial = dict(rate.get("commercial_terms") or {})
        security = dict(commercial.get("security_deposit") or {})
        # Lead-priced quotes freeze the refundable security deposit directly on
        # the rate snapshot. Older canonical plans used the nested commercial
        # structure. Support both so the separately disclosed deposit cannot
        # disappear between the customer workspace and QuickBooks.
        if not security and int(rate.get("security_deposit_cents") or 0) > 0:
            security = {
                "amount_cents": int(rate["security_deposit_cents"]),
                "refundable": True,
            }
        balance_days = int(commercial.get("balance_due_days_before_event") or 7)
        quote_checksum = _quote_checksum(proposal)
        components: list[tuple[str, int, date, str]] = []
        required_cents = int(payment_readiness.amount_cents or 0)
        if required_cents <= 0 or required_cents > proposal.amount_cents:
            raise HTTPException(status_code=409, detail="Approved payment amount is invalid.")
        if payment_readiness.request_type == "deposit":
            components.append(("deposit", required_cents, _now().date(), "Booking deposit"))
            balance_cents = proposal.amount_cents - required_cents
            if balance_cents:
                components.append((
                    "final_balance",
                    balance_cents,
                    reservation.starts_at.date() - timedelta(days=balance_days),
                    "Remaining event balance",
                ))
        else:
            components.append(("one_time", required_cents, _now().date(), "Event balance"))
        security_cents = int(security.get("amount_cents") or 0)
        if security_cents:
            if not security.get("refundable"):
                raise HTTPException(
                    status_code=409,
                    detail="Security-deposit tax treatment is not verified as refundable.",
                )
            components.append((
                "security_deposit",
                security_cents,
                reservation.starts_at.date() - timedelta(days=balance_days),
                "Refundable security deposit — non-taxable unless retained or applied",
            ))

        schedule_ids: list[str] = []
        duplicates = 0
        for component, amount_cents, starts_on, description in components:
            schedule_id = str(uuid5(
                NAMESPACE_URL,
                f"building-event-billing:{reservation.id}:{proposal.id}:{proposal.version}:{component}",
            ))
            schedule_ids.append(schedule_id)
            existing = session.get(BuildingBillingSchedule, schedule_id)
            if existing is not None:
                if (
                    existing.source_quote_checksum != quote_checksum
                    or existing.amount_cents != amount_cents
                ):
                    raise HTTPException(
                        status_code=409,
                        detail=f"Existing {component.replace('_', ' ')} draft differs from the signed quote.",
                    )
                duplicates += 1
                continue
            session.add(BuildingBillingSchedule(
                id=schedule_id,
                billing_account_id=account.id,
                reservation_id=reservation.id,
                schedule_type=component,
                billing_component=component,
                description=description,
                amount_cents=amount_cents,
                currency=(proposal.currency or "USD").lower(),
                collection_method="send_invoice",
                days_until_due=balance_days if component != "deposit" else 2,
                starts_on=starts_on,
                next_invoice_on=starts_on,
                status="draft",
                created_by=payload.actor,
                source_proposal_id=proposal.id,
                source_proposal_version=proposal.version,
                source_amount_cents=amount_cents,
                source_quote_total_cents=proposal.amount_cents,
                source_quote_checksum=quote_checksum,
            ))
        session.add(BuildingAuditEvent(
            entity_type="reservation",
            entity_id=reservation.id,
            action="quickbooks_billing_drafts_prepared",
            actor=payload.actor,
            after_json={
                "account_id": account.id,
                "schedule_ids": schedule_ids,
                "quote_id": proposal.id,
                "quote_version": proposal.version,
                "quote_checksum": quote_checksum,
                "provider_write": False,
                "invoice_sent": False,
            },
        ))
        return {
            "ok": True,
            "duplicate": duplicates == len(components),
            "billing_account_id": account.id,
            "schedule_ids": schedule_ids,
            "component_count": len(components),
            "provider_write": False,
        }


@internal_router.post("/schedules/from-proposal", status_code=201)
def create_schedule_from_proposal(
    payload: ScheduleFromProposalInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Bill exactly what the customer accepted.

    The amount is read from the accepted quote and never from the caller, so a
    discount recorded on the quote reaches the invoice without anyone retyping
    it. The quote's identity and total are stored alongside the schedule; the
    invoice run re-checks them, so a later revision cannot be billed at the old
    number by accident.
    """

    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        proposal = session.get(BuildingProposal, payload.proposal_id)
        if proposal is None:
            raise HTTPException(status_code=404, detail="Proposal not found.")
        if proposal.status != "accepted":
            raise HTTPException(
                status_code=409,
                detail="Only an accepted quote can be billed. Send it and record acceptance first.",
            )
        if proposal.amount_cents <= 0:
            raise HTTPException(
                status_code=422,
                detail="The accepted quote has no billable total.",
            )

        account = session.get(BuildingBillingAccount, payload.billing_account_id)
        if account is None or account.status != "active":
            raise HTTPException(status_code=422, detail="Billing account is unavailable.")

        existing = session.execute(
            select(BuildingBillingSchedule).where(
                BuildingBillingSchedule.source_proposal_id == proposal.id
            )
        ).scalars().all()
        for row in existing:
            if row.source_proposal_version == proposal.version:
                # Idempotent: the same accepted quote yields the same schedule.
                return {
                    "ok": True,
                    "duplicate": True,
                    "schedule_id": row.id,
                    "status": row.status,
                    "amount_cents": row.amount_cents,
                }
            if row.status not in {"cancelled", "paused"}:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"Schedule {row.id} still bills version {row.source_proposal_version} "
                        "of this quote. Cancel or pause it before billing a new version."
                    ),
                )

        if session.get(BuildingBillingSchedule, payload.id) is not None:
            raise HTTPException(status_code=409, detail="Schedule ID already exists.")

        description = payload.description.strip() or (
            proposal.terms_summary.strip()[:512]
            or f"Event quote {proposal.id} v{proposal.version}"
        )
        row = BuildingBillingSchedule(
            id=payload.id,
            billing_account_id=account.id,
            reservation_id=proposal.reservation_id,
            schedule_type=payload.schedule_type,
            description=description,
            amount_cents=proposal.amount_cents,
            currency=(proposal.currency or "usd").lower(),
            collection_method=payload.collection_method,
            days_until_due=payload.days_until_due,
            starts_on=payload.starts_on,
            next_invoice_on=payload.starts_on,
            status="draft",
            created_by=payload.actor,
            source_proposal_id=proposal.id,
            source_proposal_version=proposal.version,
            source_amount_cents=proposal.amount_cents,
        )
        session.add(row)
        session.add(BuildingAuditEvent(
            entity_type="billing_schedule",
            entity_id=row.id,
            action="drafted_from_proposal",
            actor=payload.actor,
            after_json={
                "proposal_id": proposal.id,
                "proposal_version": proposal.version,
                "amount_cents": row.amount_cents,
                "line_items": proposal.line_items_json or [],
            },
        ))
        return {
            "ok": True,
            "schedule_id": row.id,
            "status": row.status,
            "amount_cents": row.amount_cents,
            "proposal_id": proposal.id,
            "proposal_version": proposal.version,
        }


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
        if schedule.next_invoice_on and schedule.next_invoice_on > _now().date():
            raise HTTPException(
                status_code=409,
                detail=(
                    "The next scheduled invoice date has not arrived. "
                    "Change the reviewed schedule instead of billing early."
                ),
            )
        if schedule.source_proposal_id:
            # The quote is the authority. If it moved after this schedule was
            # drafted, stop rather than bill a number nobody agreed to.
            proposal = session.get(BuildingProposal, schedule.source_proposal_id)
            if proposal is None:
                raise HTTPException(
                    status_code=409,
                    detail="The quote behind this schedule no longer exists.",
                )
            quote_total = int(schedule.source_quote_total_cents or schedule.source_amount_cents)
            checksum_mismatch = bool(
                schedule.source_quote_checksum
                and _quote_checksum(proposal) != schedule.source_quote_checksum
            )
            if (
                proposal.version != schedule.source_proposal_version
                or proposal.amount_cents != quote_total
                or schedule.amount_cents != schedule.source_amount_cents
                or checksum_mismatch
            ):
                raise HTTPException(
                    status_code=409,
                    detail=(
                        "This schedule no longer matches its accepted quote. "
                        "Re-draft the schedule from the current quote before invoicing."
                    ),
                )
            if schedule.reservation_id and schedule.billing_component != "full_amount":
                agreement = session.execute(
                    select(BuildingAgreement)
                    .where(
                        BuildingAgreement.reservation_id == schedule.reservation_id,
                        BuildingAgreement.preparation_status == "approved",
                    )
                    .order_by(BuildingAgreement.version.desc())
                ).scalars().first()
                frozen_quote = dict(
                    (agreement.package_snapshot_json or {}).get("quote") or {}
                ) if agreement is not None else {}
                if (
                    agreement is None
                    or str(frozen_quote.get("id") or "") != proposal.id
                    or int(frozen_quote.get("version") or 0) != proposal.version
                    or int(frozen_quote.get("amount_cents") or 0)
                    != proposal.amount_cents
                ):
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            "The event invoice no longer matches its approved "
                            "agreement package. Prepare billing again."
                        ),
                    )
            elif proposal.status != "accepted":
                raise HTTPException(
                    status_code=409,
                    detail="The quote behind this schedule is no longer accepted.",
                )

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
        if schedule.schedule_type == "monthly":
            raise HTTPException(
                status_code=409,
                detail="Monthly Building billing has no verified QuickBooks event item.",
            )

        client = BuildingQuickBooksClient()
        if not client.is_configured:
            raise HTTPException(status_code=503, detail="QuickBooks billing is not configured.")
        invoice_id = str(uuid5(NAMESPACE_URL, f"building-invoice:{payload.idempotency_key}"))
        if not account.qbo_customer_id:
            try:
                customer = client.ensure_customer(
                    email=account.billing_email,
                    name=account.account_name,
                )
            except BuildingQuickBooksError as exc:
                raise HTTPException(status_code=502, detail=str(exc)) from exc
            account.qbo_customer_id = str(customer.get("Id") or "")
            if not account.qbo_customer_id:
                raise HTTPException(status_code=502, detail="QuickBooks customer creation returned no ID.")
        try:
            provider_invoice = client.create_draft_invoice(
                customer_id=account.qbo_customer_id,
                amount_cents=schedule.amount_cents,
                description=schedule.description,
                schedule_type=schedule.schedule_type,
                due_date=_now().date() + timedelta(days=schedule.days_until_due),
                idempotency_key=payload.idempotency_key,
            )
        except BuildingQuickBooksError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        provider_id = str(provider_invoice.get("Id") or "")
        if not provider_id:
            raise HTTPException(status_code=502, detail="QuickBooks invoice creation returned no ID.")
        provider_total_cents = round(
            float(provider_invoice.get("TotalAmt") or schedule.amount_cents / 100) * 100
        )
        total_matches = provider_total_cents == schedule.amount_cents
        due_date = date.fromisoformat(
            str(provider_invoice.get("DueDate") or (_now().date() + timedelta(days=schedule.days_until_due)).isoformat())
        )
        due_at = datetime.combine(due_date, datetime.min.time(), tzinfo=timezone.utc)
        row = BuildingInvoice(
            id=invoice_id,
            billing_account_id=account.id,
            billing_schedule_id=schedule.id,
            reservation_id=schedule.reservation_id,
            idempotency_key=payload.idempotency_key,
            provider="quickbooks",
            provider_invoice_id=provider_id,
            qbo_invoice_id=provider_id,
            description=schedule.description,
            status="draft",
            accounting_status="synced_qbo" if total_matches else "failed",
            amount_due_cents=schedule.amount_cents,
            amount_paid_cents=0,
            currency=schedule.currency,
            due_at=due_at,
            hosted_invoice_url=f"https://qbo.intuit.com/app/invoice?txnId={provider_id}",
            provider_payload_json={
                **provider_invoice,
                "agent_expected_total_cents": schedule.amount_cents,
                "agent_provider_total_cents": provider_total_cents,
                "agent_total_matches": total_matches,
            },
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
            action=(
                "draft_created_in_quickbooks"
                if total_matches
                else "qbo_draft_created_with_total_mismatch"
            ),
            actor=payload.actor,
            after_json={
                "provider_invoice_id": row.provider_invoice_id,
                "amount_due_cents": row.amount_due_cents,
                "accounting_status": row.accounting_status,
                "sent": False,
                "provider_total_cents": provider_total_cents,
                "total_matches": total_matches,
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


@internal_router.post("/invoices/{invoice_id}/sync-qbo")
def sync_quickbooks_invoice(
    invoice_id: str,
    payload: EventBillingPreparationInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Refresh invoice and cleared-balance evidence from QuickBooks."""

    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        row = session.get(BuildingInvoice, invoice_id)
        if row is None or row.provider != "quickbooks" or not row.qbo_invoice_id:
            raise HTTPException(status_code=404, detail="QuickBooks invoice not found.")
        client = BuildingQuickBooksClient()
        if not client.is_configured:
            raise HTTPException(status_code=503, detail="QuickBooks billing is not configured.")
        try:
            provider = client.get_invoice(row.qbo_invoice_id)
        except BuildingQuickBooksError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        if str(provider.get("Id") or "") != row.qbo_invoice_id:
            raise HTTPException(status_code=409, detail="QuickBooks returned the wrong invoice.")
        provider_total = round(float(provider.get("TotalAmt") or 0) * 100)
        if provider_total != row.amount_due_cents:
            row.accounting_status = "failed"
            row.provider_payload_json = {
                **provider,
                "agent_sync_error": "total_mismatch",
                "agent_expected_total_cents": row.amount_due_cents,
                "agent_synced_at": _now().isoformat(),
            }
            row.updated_at = _now()
            session.add(BuildingAuditEvent(
                entity_type="invoice",
                entity_id=row.id,
                action="qbo_invoice_total_mismatch",
                actor=payload.actor,
                after_json={
                    "expected_cents": row.amount_due_cents,
                    "provider_cents": provider_total,
                    "qbo_invoice_id": row.qbo_invoice_id,
                },
            ))
            return {"ok": False, "invoice": _invoice_payload(row), "recovery": "review_in_qbo"}
        balance_cents = round(float(provider.get("Balance") or 0) * 100)
        amount_paid_cents = max(0, provider_total - balance_cents)
        before = {"status": row.status, "amount_paid_cents": row.amount_paid_cents}
        row.amount_paid_cents = amount_paid_cents
        row.status = "paid" if provider_total > 0 and balance_cents == 0 else (
            "open" if str(provider.get("EmailStatus") or "").lower() == "emailsent" else "draft"
        )
        row.accounting_status = "reconciled"
        row.provider_payload_json = {
            **provider,
            "agent_synced_at": _now().isoformat(),
        }
        row.updated_at = _now()
        if row.status == "paid" and row.reservation_id and row.billing_schedule_id:
            schedule = session.get(BuildingBillingSchedule, row.billing_schedule_id)
            reservation = session.get(BuildingReservation, row.reservation_id)
            if (
                schedule is not None
                and reservation is not None
                and schedule.billing_component in {"deposit", "full_amount", "one_time"}
            ):
                evidence_id = str(uuid5(NAMESPACE_URL, f"qbo-deposit:{row.qbo_invoice_id}"))
                evidence = session.get(BuildingDepositEvidence, evidence_id)
                if evidence is None:
                    evidence = BuildingDepositEvidence(
                        id=evidence_id,
                        reservation_id=reservation.id,
                    )
                evidence.status = "paid"
                evidence.amount_cents = amount_paid_cents
                evidence.provider = "quickbooks"
                evidence.provider_reference = row.qbo_invoice_id
                evidence.evidence_json = {
                    "invoice_id": row.id,
                    "qbo_invoice_id": row.qbo_invoice_id,
                    "balance_cents": balance_cents,
                    "total_cents": provider_total,
                    "sync_token": str(provider.get("SyncToken") or ""),
                    "provider_observed_at": _now().isoformat(),
                }
                evidence.recorded_by = payload.actor
                evidence.recorded_at = _now()
                session.add(evidence)
                reservation.deposit_status = "paid"
                reservation.updated_at = _now()
        session.add(BuildingAuditEvent(
            entity_type="invoice",
            entity_id=row.id,
            action="qbo_invoice_synced",
            actor=payload.actor,
            before_json=before,
            after_json={
                "status": row.status,
                "amount_paid_cents": row.amount_paid_cents,
                "balance_cents": balance_cents,
                "qbo_invoice_id": row.qbo_invoice_id,
                "provider_observed": True,
            },
        ))
        reservation = (
            session.get(BuildingReservation, row.reservation_id)
            if row.reservation_id
            else None
        )
        if reservation is not None and reservation.kind == "event":
            if row.status in {"open", "paid"}:
                attempt_booking_message(
                    session,
                    request=request,
                    reservation=reservation,
                    milestone="invoice_ready",
                    actor=payload.actor,
                )
            if row.status == "paid" and reservation.deposit_status == "paid":
                attempt_booking_message(
                    session,
                    request=request,
                    reservation=reservation,
                    milestone="payment_received",
                    actor=payload.actor,
                )
        return {"ok": True, "invoice": _invoice_payload(row)}


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


@internal_router.post("/collections/refresh")
def refresh_collection_cases(
    payload: CollectionRefreshInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        invoices = _outstanding_overdue_invoices(session)
        existing_by_invoice = {
            item.invoice_id: item
            for item in session.execute(
                select(BuildingCollectionCase).where(
                    BuildingCollectionCase.invoice_id.in_(
                        [invoice.id for invoice in invoices]
                    )
                )
            ).scalars().all()
        } if invoices else {}
        would_create = [
            invoice for invoice in invoices if invoice.id not in existing_by_invoice
        ]
        created = 0
        if payload.execute:
            for invoice in would_create:
                case = BuildingCollectionCase(
                    id=str(uuid5(NAMESPACE_URL, f"anata-building-collection:{invoice.id}")),
                    invoice_id=invoice.id,
                    status="open",
                    assigned_owner=payload.default_owner.strip(),
                    next_action_at=_now(),
                    created_by=payload.actor,
                )
                session.add(case)
                session.add(BuildingAuditEvent(
                    entity_type="collection_case",
                    entity_id=case.id,
                    action="opened_from_aging",
                    actor=payload.actor,
                    after_json={
                        "invoice_id": invoice.id,
                        "assigned_owner": case.assigned_owner,
                        "amount_outstanding_cents": (
                            invoice.amount_due_cents - invoice.amount_paid_cents
                        ),
                        "due_at": invoice.due_at.isoformat() if invoice.due_at else None,
                    },
                ))
                created += 1
        return {
            "ok": True,
            "execute": payload.execute,
            "overdue_invoice_count": len(invoices),
            "existing_case_count": len(existing_by_invoice),
            "would_create_count": len(would_create),
            "created_count": created,
            "invoice_ids": [invoice.id for invoice in would_create],
        }


@internal_router.get("/collections")
def list_collection_cases(
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        cases = session.execute(
            select(BuildingCollectionCase).order_by(
                BuildingCollectionCase.status,
                BuildingCollectionCase.next_action_at,
                BuildingCollectionCase.created_at,
            )
        ).scalars().all()
        invoice_ids = [case.invoice_id for case in cases]
        invoices = {
            item.id: item
            for item in session.execute(
                select(BuildingInvoice).where(BuildingInvoice.id.in_(invoice_ids))
            ).scalars().all()
        } if invoice_ids else {}
        account_ids = {invoice.billing_account_id for invoice in invoices.values()}
        accounts = {
            item.id: item
            for item in session.execute(
                select(BuildingBillingAccount).where(
                    BuildingBillingAccount.id.in_(account_ids)
                )
            ).scalars().all()
        } if account_ids else {}
        return {
            "collections": [
                _collection_payload(
                    case,
                    invoices[case.invoice_id],
                    accounts[invoices[case.invoice_id].billing_account_id],
                )
                for case in cases
                if case.invoice_id in invoices
                and invoices[case.invoice_id].billing_account_id in accounts
            ]
        }


@internal_router.put("/collections/{case_id}")
def transition_collection_case(
    case_id: str,
    payload: CollectionTransitionInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        case = session.get(BuildingCollectionCase, case_id)
        if case is None:
            raise HTTPException(status_code=404, detail="Collection case not found.")
        if case.status in COLLECTION_TERMINAL_STATUSES:
            raise HTTPException(status_code=409, detail="Closed collection cases are immutable.")
        if payload.status in {"contacted", "promised", "disputed"}:
            if not payload.assigned_owner.strip():
                raise HTTPException(
                    status_code=422, detail="Active collection work requires an owner."
                )
            if payload.next_action_at is None:
                raise HTTPException(
                    status_code=422,
                    detail="Active collection work requires a next action time.",
                )
        if payload.status in COLLECTION_TERMINAL_STATUSES and not payload.resolution.strip():
            raise HTTPException(
                status_code=422,
                detail="Closing a collection case requires a resolution.",
            )
        before = {
            "status": case.status,
            "assigned_owner": case.assigned_owner,
            "next_action_at": (
                case.next_action_at.isoformat() if case.next_action_at else None
            ),
        }
        case.status = payload.status
        case.assigned_owner = payload.assigned_owner.strip()
        case.next_action_at = payload.next_action_at
        case.notes = payload.notes.strip()
        case.resolution = payload.resolution.strip()
        case.updated_at = _now()
        session.add(BuildingAuditEvent(
            entity_type="collection_case",
            entity_id=case.id,
            action=f"collection_{payload.status}",
            actor=payload.actor,
            before_json=before,
            after_json={
                "status": case.status,
                "assigned_owner": case.assigned_owner,
                "next_action_at": (
                    case.next_action_at.isoformat() if case.next_action_at else None
                ),
                "notes": case.notes,
                "resolution": case.resolution,
            },
        ))
        invoice = session.get(BuildingInvoice, case.invoice_id)
        account = (
            session.get(BuildingBillingAccount, invoice.billing_account_id)
            if invoice
            else None
        )
        if invoice is None or account is None:
            raise HTTPException(status_code=409, detail="Collection billing evidence is incomplete.")
        return {"ok": True, "collection": _collection_payload(case, invoice, account)}


@internal_router.post("/collections/{case_id}/remind")
def send_collection_reminder(
    case_id: str,
    payload: CollectionReminderInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    expected = f"REMIND {case_id}"
    if payload.confirmation.strip() != expected:
        raise HTTPException(status_code=409, detail=f"Type {expected} to confirm.")
    if payload.next_action_at is None:
        raise HTTPException(
            status_code=422,
            detail="Schedule the next collection follow-up before sending a reminder.",
        )
    with session_scope(request.app.state.session_factory) as session:
        case = session.get(BuildingCollectionCase, case_id)
        if case is None:
            raise HTTPException(status_code=404, detail="Collection case not found.")
        if case.status in COLLECTION_TERMINAL_STATUSES:
            raise HTTPException(status_code=409, detail="Collection case is already closed.")
        invoice = session.get(BuildingInvoice, case.invoice_id)
        if invoice is None:
            raise HTTPException(status_code=409, detail="Invoice evidence is missing.")
        if invoice.amount_paid_cents >= invoice.amount_due_cents:
            raise HTTPException(status_code=409, detail="Invoice no longer has an outstanding balance.")
        if not invoice.hosted_invoice_url:
            raise HTTPException(status_code=409, detail="Invoice has no secure payment link.")
        account = session.get(BuildingBillingAccount, invoice.billing_account_id)
        if account is None:
            raise HTTPException(status_code=409, detail="Billing account is missing.")
        suppression = session.get(BuildingSuppression, account.billing_email)
        if suppression is not None and suppression.scope == "all":
            raise HTTPException(status_code=409, detail="All email is suppressed for this address.")
        client = ResendClient(request.app.state.settings)
        if not client.is_configured():
            raise HTTPException(status_code=503, detail="Email delivery is not configured.")
        outstanding = max(0, invoice.amount_due_cents - invoice.amount_paid_cents)
        amount = f"{invoice.currency.upper()} {outstanding / 100:,.2f}"
        provider_id = client.send_message(
            to=account.billing_email,
            subject=f"Anata Building invoice reminder — {amount} outstanding",
            text=(
                f"Hello,\n\nThis is a reminder that {amount} remains outstanding "
                f"for {invoice.description}. You can review and pay the invoice "
                f"securely here:\n{invoice.hosted_invoice_url}\n\n"
                "If you have already arranged payment or need help, reply to this email."
            ),
        )
        case.status = "contacted"
        case.reminder_count += 1
        case.last_reminder_at = _now()
        case.last_reminder_provider_id = (
            provider_id if isinstance(provider_id, str) else "resend"
        )
        case.next_action_at = payload.next_action_at
        case.updated_at = _now()
        session.add(BuildingAuditEvent(
            entity_type="collection_case",
            entity_id=case.id,
            action="reminder_sent",
            actor=payload.actor,
            after_json={
                "invoice_id": invoice.id,
                "recipient": account.billing_email,
                "outstanding_cents": outstanding,
                "provider_message_id": case.last_reminder_provider_id,
                "reminder_count": case.reminder_count,
                "next_action_at": (
                    case.next_action_at.isoformat() if case.next_action_at else None
                ),
            },
        ))
        return {"ok": True, "collection": _collection_payload(case, invoice, account)}


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
            if (
                event_type.startswith("invoice.")
                and invoice is not None
                and invoice.provider != "stripe"
            ):
                event_row.status = "ignored"
                event_row.error_message = (
                    f"Provider mismatch: Stripe event cannot update {invoice.provider} invoice."
                )
                event_row.processed_at = _now()
                session.add(BuildingAuditEvent(
                    entity_type="invoice",
                    entity_id=invoice.id,
                    action="stripe_event_ignored",
                    actor="stripe-webhook",
                    after_json={
                        "invoice_provider": invoice.provider,
                        "provider_event_id": event_id,
                        "reason": "provider_mismatch",
                    },
                ))
            elif event_type.startswith("invoice.") and invoice is not None:
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
                if invoice.status in {"paid", "void", "uncollectible"}:
                    collection_case = session.execute(
                        select(BuildingCollectionCase).where(
                            BuildingCollectionCase.invoice_id == invoice.id
                        )
                    ).scalar_one_or_none()
                    if (
                        collection_case is not None
                        and collection_case.status not in COLLECTION_TERMINAL_STATUSES
                    ):
                        collection_case.status = (
                            "resolved" if invoice.status == "paid" else "waived"
                        )
                        collection_case.resolution = (
                            "Stripe confirmed the invoice was paid."
                            if invoice.status == "paid"
                            else f"Stripe marked the invoice {invoice.status}."
                        )
                        collection_case.next_action_at = None
                        collection_case.updated_at = _now()
                        session.add(BuildingAuditEvent(
                            entity_type="collection_case",
                            entity_id=collection_case.id,
                            action="closed_from_stripe",
                            actor="stripe-webhook",
                            after_json={
                                "invoice_id": invoice.id,
                                "invoice_status": invoice.status,
                                "status": collection_case.status,
                                "resolution": collection_case.resolution,
                                "provider_event_id": event_id,
                            },
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
            if event_row.status != "ignored":
                event_row.status = "processed"
                event_row.processed_at = _now()
        except Exception as exc:  # noqa: BLE001 - persist provider event for retry
            event_row.status = "failed"
            event_row.error_message = str(exc)[:1000]
            return JSONResponse(status_code=500, content={"ok": False, "error": "Webhook processing failed."})
        return JSONResponse(content={"ok": True, "duplicate": False})
