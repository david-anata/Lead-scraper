"""Authenticated Building Control forms for bookings and billing operations."""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Callable
from urllib.parse import urlencode
from uuid import uuid4
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import RedirectResponse
from pydantic import ValidationError

from sales_support_agent.api.building_billing_router import (
    BillingAccountInput,
    BillingScheduleInput,
    InvoiceRunInput,
    ScheduleApprovalInput,
    approve_billing_schedule,
    create_invoice_from_schedule,
    upsert_billing_account,
    upsert_billing_schedule,
)
from sales_support_agent.api.building_booking_router import (
    AgreementInput,
    DepositInput,
    ReservationInput,
    TransitionInput,
    create_reservation,
    record_agreement,
    record_deposit,
    transition_reservation,
)
from sales_support_agent.api.building_calendar_router import (
    CalendarSyncInput,
    sync_calendar_projections,
)
from sales_support_agent.models.database import session_scope
from sales_support_agent.models.entities import BuildingBillingSchedule
from sales_support_agent.services.auth_deps import require_tool
from sales_support_agent.services.building_security import (
    require_building_form_security,
)


router = APIRouter(prefix="/admin/building", tags=["building-admin-operations"])
FORM_DEPS = [Depends(require_building_form_security)]
MOUNTAIN = ZoneInfo("America/Denver")


def _redirect(*, notice: str = "", error: str = "") -> RedirectResponse:
    query = urlencode({"notice": notice} if notice else {"error": error})
    return RedirectResponse(f"/admin/building?{query}", status_code=303)


def _internal_key(request: Request) -> str:
    key = str(getattr(request.app.state.settings, "internal_api_key", "") or "").strip()
    if not key:
        raise HTTPException(status_code=503, detail="Internal API is not configured.")
    return key


def _actor(user: dict) -> str:
    return str(user.get("email") or "building-operator")


def _local_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(str(value or "").strip())
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=MOUNTAIN)
    return parsed.astimezone(timezone.utc)


def _dollars_to_cents(value: str) -> int:
    try:
        amount = Decimal(str(value or "").replace(",", "").replace("$", "").strip())
    except InvalidOperation as exc:
        raise ValueError("Enter a valid dollar amount.") from exc
    if amount < 0:
        raise ValueError("Amount cannot be negative.")
    return int((amount * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _run_form_action(action: Callable[[], object], success: str) -> RedirectResponse:
    try:
        action()
    except (ValidationError, ValueError) as exc:
        if isinstance(exc, ValidationError):
            message = exc.errors()[0].get("msg", "Review the form values.")
        else:
            message = str(exc)
        return _redirect(error=message)
    except HTTPException as exc:
        return _redirect(error=str(exc.detail))
    return _redirect(notice=success)


@router.post("/reservations", dependencies=FORM_DEPS)
def create_reservation_from_control_room(
    request: Request,
    kind: str = Form(...),
    space_id: str = Form(...),
    offering_id: str = Form(""),
    inquiry_id: str = Form(""),
    contact_id: str = Form(""),
    starts_at: str = Form(...),
    ends_at: str = Form(...),
    attendance: int = Form(0),
    deposit_required: bool = Form(False),
    assigned_owner: str = Form(""),
    requirements: str = Form(""),
    source: str = Form("control_room"),
    source_reference: str = Form(""),
    user: dict = Depends(require_tool("building.manage")),
) -> RedirectResponse:
    def action() -> None:
        payload = ReservationInput(
            id=str(uuid4()),
            kind=kind,
            space_id=space_id.strip(),
            offering_id=offering_id.strip() or None,
            inquiry_id=inquiry_id.strip() or None,
            contact_id=contact_id.strip() or None,
            starts_at=_local_datetime(starts_at),
            ends_at=_local_datetime(ends_at),
            attendance=attendance,
            deposit_required=deposit_required,
            assigned_owner=assigned_owner.strip(),
            requirements={"operator_notes": requirements.strip()} if requirements.strip() else {},
            source=source.strip() or "control_room",
            source_reference=source_reference.strip(),
            actor=_actor(user),
        )
        create_reservation(payload, request, _internal_key(request))

    return _run_form_action(action, "Booking workflow created as an inquiry.")


@router.post("/reservations/{reservation_id}/transition", dependencies=FORM_DEPS)
def transition_reservation_from_control_room(
    reservation_id: str,
    request: Request,
    target_status: str = Form(...),
    hold_expires_at: str = Form(""),
    reason: str = Form(""),
    user: dict = Depends(require_tool("building.manage")),
) -> RedirectResponse:
    def action() -> None:
        payload = TransitionInput(
            target_status=target_status,
            hold_expires_at=(
                _local_datetime(hold_expires_at) if hold_expires_at.strip() else None
            ),
            actor=_actor(user),
            reason=reason.strip(),
        )
        transition_reservation(
            reservation_id, payload, request, _internal_key(request)
        )

    return _run_form_action(action, f"Booking moved to {target_status.replace('_', ' ')}.")


@router.post("/reservations/{reservation_id}/agreements", dependencies=FORM_DEPS)
def record_agreement_from_control_room(
    reservation_id: str,
    request: Request,
    version: int = Form(1),
    status: str = Form(...),
    provider: str = Form(""),
    provider_reference: str = Form(""),
    template_name: str = Form(""),
    document_url: str = Form(""),
    user: dict = Depends(require_tool("building.manage")),
) -> RedirectResponse:
    def action() -> None:
        payload = AgreementInput(
            version=version,
            status=status,
            provider=provider.strip(),
            provider_reference=provider_reference.strip(),
            template_name=template_name.strip(),
            document_url=document_url.strip(),
            evidence={"recorded_in": "building_control"},
            actor=_actor(user),
        )
        record_agreement(reservation_id, payload, request, _internal_key(request))

    return _run_form_action(action, f"Agreement evidence recorded as {status}.")


@router.post("/reservations/{reservation_id}/deposits", dependencies=FORM_DEPS)
def record_deposit_from_control_room(
    reservation_id: str,
    request: Request,
    status: str = Form(...),
    amount: str = Form("0"),
    provider: str = Form(""),
    provider_reference: str = Form(""),
    user: dict = Depends(require_tool("building.manage")),
) -> RedirectResponse:
    def action() -> None:
        payload = DepositInput(
            status=status,
            amount_cents=_dollars_to_cents(amount),
            provider=provider.strip(),
            provider_reference=provider_reference.strip(),
            evidence={"recorded_in": "building_control"},
            actor=_actor(user),
        )
        record_deposit(reservation_id, payload, request, _internal_key(request))

    return _run_form_action(action, f"Deposit evidence recorded as {status}.")


@router.post("/billing/accounts", dependencies=FORM_DEPS)
def save_billing_account_from_control_room(
    request: Request,
    account_id: str = Form(...),
    contact_id: str = Form(""),
    account_name: str = Form(...),
    billing_email: str = Form(...),
    qbo_customer_id: str = Form(""),
    user: dict = Depends(require_tool("building.manage")),
) -> RedirectResponse:
    def action() -> None:
        payload = BillingAccountInput(
            id=account_id.strip(),
            contact_id=contact_id.strip() or None,
            account_name=account_name.strip(),
            billing_email=billing_email,
            qbo_customer_id=qbo_customer_id.strip(),
            metadata={"created_in": "building_control"},
            actor=_actor(user),
        )
        upsert_billing_account(
            payload.id, payload, request, _internal_key(request)
        )

    return _run_form_action(action, "Billing account saved.")


@router.post("/billing/schedules", dependencies=FORM_DEPS)
def save_billing_schedule_from_control_room(
    request: Request,
    schedule_id: str = Form(...),
    billing_account_id: str = Form(...),
    reservation_id: str = Form(""),
    schedule_type: str = Form(...),
    description: str = Form(...),
    amount: str = Form(...),
    collection_method: str = Form("send_invoice"),
    days_until_due: int = Form(7),
    starts_on: str = Form(...),
    ends_on: str = Form(""),
    user: dict = Depends(require_tool("building.manage")),
) -> RedirectResponse:
    def action() -> None:
        payload = BillingScheduleInput(
            id=schedule_id.strip(),
            billing_account_id=billing_account_id.strip(),
            reservation_id=reservation_id.strip() or None,
            schedule_type=schedule_type,
            description=description.strip(),
            amount_cents=_dollars_to_cents(amount),
            collection_method=collection_method,
            days_until_due=days_until_due,
            starts_on=date.fromisoformat(starts_on),
            ends_on=date.fromisoformat(ends_on) if ends_on.strip() else None,
            actor=_actor(user),
        )
        upsert_billing_schedule(
            payload.id, payload, request, _internal_key(request)
        )

    return _run_form_action(action, "Billing schedule saved as a draft.")


@router.post("/billing/schedules/{schedule_id}/approve", dependencies=FORM_DEPS)
def approve_billing_schedule_from_control_room(
    schedule_id: str,
    request: Request,
    user: dict = Depends(require_tool("building.manage")),
) -> RedirectResponse:
    def action() -> None:
        approve_billing_schedule(
            schedule_id,
            ScheduleApprovalInput(actor=_actor(user)),
            request,
            _internal_key(request),
        )

    return _run_form_action(action, "Billing schedule approved and locked.")


@router.post("/billing/schedules/{schedule_id}/invoice", dependencies=FORM_DEPS)
def create_invoice_from_control_room(
    schedule_id: str,
    request: Request,
    confirmation: str = Form(...),
    user: dict = Depends(require_tool("building.manage")),
) -> RedirectResponse:
    expected = f"INVOICE {schedule_id}"
    if confirmation.strip() != expected:
        return _redirect(error=f"Type {expected} to create the provider invoice.")
    with session_scope(request.app.state.session_factory) as session:
        schedule = session.get(BuildingBillingSchedule, schedule_id)
        if schedule is None:
            return _redirect(error="Billing schedule not found.")
        invoice_for = schedule.next_invoice_on or date.today()
    idempotency_key = f"building:{schedule_id}:{invoice_for.isoformat()}"

    try:
        result = create_invoice_from_schedule(
            InvoiceRunInput(
                schedule_id=schedule_id,
                idempotency_key=idempotency_key,
                execute=True,
                actor=_actor(user),
            ),
            request,
            _internal_key(request),
        )
    except (ValidationError, ValueError) as exc:
        if isinstance(exc, ValidationError):
            message = exc.errors()[0].get("msg", "Review the form values.")
        else:
            message = str(exc)
        return _redirect(error=message)
    except HTTPException as exc:
        return _redirect(error=str(exc.detail))
    if result.get("duplicate"):
        return _redirect(notice="That scheduled invoice already exists; no duplicate was created.")
    return _redirect(notice="Stripe invoice created; QBO handoff is pending.")


@router.post("/calendar/sync", dependencies=FORM_DEPS)
def sync_calendar_from_control_room(
    request: Request,
    confirmation: str = Form(...),
    user: dict = Depends(require_tool("building.manage")),
) -> RedirectResponse:
    if confirmation.strip() != "SYNC CALENDAR":
        return _redirect(error="Type SYNC CALENDAR to update Google Calendar.")
    try:
        result = sync_calendar_projections(
            CalendarSyncInput(
                execute=True,
                max_items=25,
                actor=_actor(user),
            ),
            request,
            _internal_key(request),
        )
    except HTTPException as exc:
        return _redirect(error=str(exc.detail))
    if result.get("failed_count"):
        return _redirect(
            error=(
                f"{result.get('failed_count')} calendar item(s) need retry; "
                "review the calendar projection queue."
            )
        )
    return _redirect(
        notice=f"{result.get('synced_count', 0)} calendar item(s) synchronized."
    )
