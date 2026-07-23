"""Authenticated Building Control forms for bookings and billing operations."""

from __future__ import annotations

import hashlib
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
from sales_support_agent.api.building_router import (
    InquiryInput,
    InquiryLifecycleInput,
    InquiryRetryInput,
    create_inquiry,
    retry_inquiry_hubspot,
    update_inquiry_lifecycle,
)
from sales_support_agent.api.building_calendar_router import (
    CalendarSyncInput,
    sync_calendar_projections,
)
from sales_support_agent.api.building_checklist_router import (
    ChecklistItemInput,
    ChecklistItemStatusInput,
    add_checklist_item,
    update_checklist_item_status,
)
from sales_support_agent.api.building_adjustment_router import (
    AdjustmentApprovalInput,
    AdjustmentEvidenceInput,
    AdjustmentRequestInput,
    approve_adjustment,
    record_adjustment_evidence,
    request_adjustment,
)
from sales_support_agent.api.building_service_request_router import (
    ServiceRequestInput,
    ServiceRequestTransitionInput,
    create_service_request,
    transition_service_request,
)
from sales_support_agent.models.database import session_scope
from sales_support_agent.models.entities import BuildingBillingSchedule
from sales_support_agent.services.auth_deps import (
    require_all_tools,
    require_recent_tool,
    require_tool,
)
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


def _building_site_key(request: Request) -> str:
    key = str(
        getattr(request.app.state.settings, "building_site_intake_key", "") or ""
    ).strip()
    if not key:
        raise HTTPException(
            status_code=503,
            detail="Building inquiry intake is not configured.",
        )
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


@router.post("/inquiries", dependencies=FORM_DEPS)
def create_assisted_inquiry_from_control_room(
    request: Request,
    kind: str = Form(...),
    name: str = Form(...),
    email: str = Form(...),
    phone: str = Form(""),
    preferred_date: str = Form(""),
    offering_id: str = Form(""),
    source: str = Form(...),
    source_reference: str = Form(""),
    details: str = Form(""),
    consent_to_contact: bool = Form(False),
    consent_to_marketing: bool = Form(False),
    user: dict = Depends(require_tool("building.manage")),
) -> RedirectResponse:
    external_sources = {"facebook_marketplace", "eventective"}
    normalized_source = source.strip()
    normalized_reference = source_reference.strip()
    if normalized_source in external_sources and not normalized_reference:
        return _redirect(
            error="Marketplace and Eventective leads require the original message or listing reference."
        )
    identity = (
        f"{normalized_source}:{normalized_reference or uuid4()}:{email.strip().lower()}"
    )
    idempotency_key = "assisted:" + hashlib.sha256(identity.encode()).hexdigest()

    def action() -> None:
        request.state.building_inquiry_actor = _actor(user)
        create_inquiry(
            InquiryInput(
                kind=kind,
                name=name.strip(),
                email=email,
                phone=phone.strip(),
                preferred_date=(
                    date.fromisoformat(preferred_date)
                    if preferred_date.strip()
                    else None
                ),
                offering_id=offering_id.strip() or None,
                source=normalized_source,
                source_reference=normalized_reference,
                consent_to_contact=consent_to_contact,
                consent_to_marketing=consent_to_marketing,
                details={
                    "assisted_intake": True,
                    "operator_notes": details.strip(),
                },
            ),
            request,
            _building_site_key(request),
            idempotency_key,
        )

    return _run_form_action(
        action,
        "Lead added to the inquiry and CRM response queue.",
    )


@router.post("/inquiries/{inquiry_id}/lifecycle", dependencies=FORM_DEPS)
def update_inquiry_lifecycle_from_control_room(
    inquiry_id: str,
    request: Request,
    target_stage: str = Form(...),
    assigned_owner: str = Form(""),
    channel: str = Form("email"),
    notes: str = Form(""),
    user: dict = Depends(require_tool("building.manage")),
) -> RedirectResponse:
    def action() -> None:
        update_inquiry_lifecycle(
            inquiry_id,
            InquiryLifecycleInput(
                target_stage=target_stage,
                assigned_owner=assigned_owner.strip(),
                channel=channel,
                notes=notes.strip(),
                actor=_actor(user),
            ),
            request,
            _internal_key(request),
        )

    return _run_form_action(
        action,
        f"Inquiry moved to {target_stage.replace('_', ' ')}.",
    )


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


@router.post("/checklists/{checklist_id}/items", dependencies=FORM_DEPS)
def add_checklist_item_from_control_room(
    checklist_id: str,
    request: Request,
    label: str = Form(...),
    is_required: bool = Form(False),
    user: dict = Depends(require_tool("building.manage")),
) -> RedirectResponse:
    def action() -> None:
        add_checklist_item(
            checklist_id,
            ChecklistItemInput(
                label=label.strip(),
                is_required=is_required,
                actor=_actor(user),
            ),
            request,
            _internal_key(request),
        )

    return _run_form_action(action, "Operational checklist item added.")


@router.post("/checklists/items/{item_id}/status", dependencies=FORM_DEPS)
def update_checklist_item_from_control_room(
    item_id: str,
    request: Request,
    status: str = Form(...),
    reason: str = Form(""),
    user: dict = Depends(require_tool("building.manage")),
) -> RedirectResponse:
    def action() -> None:
        update_checklist_item_status(
            item_id,
            ChecklistItemStatusInput(
                status=status,
                reason=reason.strip(),
                actor=_actor(user),
            ),
            request,
            _internal_key(request),
        )

    return _run_form_action(
        action,
        f"Operational item marked {status.replace('_', ' ')}.",
    )


@router.post("/billing/adjustments", dependencies=FORM_DEPS)
def request_adjustment_from_control_room(
    request: Request,
    invoice_id: str = Form(...),
    adjustment_type: str = Form(...),
    amount: str = Form(...),
    reason: str = Form(...),
    user: dict = Depends(require_all_tools("building.manage", "finance")),
) -> RedirectResponse:
    def action() -> None:
        request_adjustment(
            AdjustmentRequestInput(
                invoice_id=invoice_id.strip(),
                adjustment_type=adjustment_type,
                amount_cents=_dollars_to_cents(amount),
                reason=reason.strip(),
                actor=_actor(user),
            ),
            request,
            _internal_key(request),
        )

    return _run_form_action(
        action,
        "Financial adjustment requested; a different finance operator must approve it.",
    )


@router.post(
    "/billing/adjustments/{adjustment_id}/approve",
    dependencies=FORM_DEPS,
)
def approve_adjustment_from_control_room(
    adjustment_id: str,
    request: Request,
    confirmation: str = Form(...),
    user: dict = Depends(
        require_recent_tool("building.manage", "finance", max_age_minutes=30)
    ),
) -> RedirectResponse:
    expected = f"APPROVE {adjustment_id}"
    if confirmation.strip() != expected:
        return _redirect(error=f"Type {expected} to approve this adjustment.")

    def action() -> None:
        approve_adjustment(
            adjustment_id,
            AdjustmentApprovalInput(actor=_actor(user)),
            request,
            _internal_key(request),
        )

    return _run_form_action(
        action,
        "Financial adjustment approved; no provider or accounting action was implied.",
    )


@router.post(
    "/billing/adjustments/{adjustment_id}/evidence",
    dependencies=FORM_DEPS,
)
def record_adjustment_evidence_from_control_room(
    adjustment_id: str,
    request: Request,
    status: str = Form(...),
    provider_reference: str = Form(""),
    qbo_reference: str = Form(""),
    note: str = Form(...),
    confirmation: str = Form(...),
    user: dict = Depends(
        require_recent_tool("building.manage", "finance", max_age_minutes=30)
    ),
) -> RedirectResponse:
    expected = f"CONFIRM {adjustment_id}"
    if confirmation.strip() != expected:
        return _redirect(error=f"Type {expected} to record final evidence.")

    def action() -> None:
        record_adjustment_evidence(
            adjustment_id,
            AdjustmentEvidenceInput(
                status=status,
                provider_reference=provider_reference.strip(),
                qbo_reference=qbo_reference.strip(),
                note=note.strip(),
                actor=_actor(user),
            ),
            request,
            _internal_key(request),
        )

    return _run_form_action(
        action,
        "Financial adjustment evidence recorded.",
    )


@router.post("/inquiries/{inquiry_id}/retry-hubspot", dependencies=FORM_DEPS)
def retry_inquiry_hubspot_from_control_room(
    inquiry_id: str,
    request: Request,
    user: dict = Depends(require_tool("building.manage")),
) -> RedirectResponse:
    try:
        result = retry_inquiry_hubspot(
            inquiry_id,
            InquiryRetryInput(actor=_actor(user)),
            request,
            _internal_key(request),
        )
    except HTTPException as exc:
        return _redirect(error=str(exc.detail))
    if not result.get("ok"):
        return _redirect(
            error=(
                str(result.get("error") or "HubSpot retry failed.")
                + " The inquiry remains safely queued."
            )
        )
    return _redirect(notice="Inquiry synchronized to HubSpot.")


@router.post("/service-requests", dependencies=FORM_DEPS)
def create_service_request_from_control_room(
    request: Request,
    category: str = Form(...),
    priority: str = Form("normal"),
    title: str = Form(...),
    description: str = Form(""),
    space_id: str = Form(""),
    contact_id: str = Form(""),
    reservation_id: str = Form(""),
    source: str = Form("operator"),
    source_reference: str = Form(""),
    assigned_owner: str = Form(""),
    due_at: str = Form(""),
    user: dict = Depends(require_tool("building.manage")),
) -> RedirectResponse:
    def action() -> None:
        create_service_request(
            ServiceRequestInput(
                id=str(uuid4()),
                category=category,
                priority=priority,
                title=title.strip(),
                description=description.strip(),
                space_id=space_id.strip() or None,
                contact_id=contact_id.strip() or None,
                reservation_id=reservation_id.strip() or None,
                source=source.strip() or "operator",
                source_reference=source_reference.strip(),
                assigned_owner=assigned_owner.strip(),
                due_at=_local_datetime(due_at) if due_at.strip() else None,
                reported_by=_actor(user),
            ),
            request,
            _internal_key(request),
        )

    return _run_form_action(action, "Service request added to the operator queue.")


@router.post(
    "/service-requests/{service_request_id}/transition",
    dependencies=FORM_DEPS,
)
def transition_service_request_from_control_room(
    service_request_id: str,
    request: Request,
    target_status: str = Form(...),
    assigned_owner: str = Form(""),
    due_at: str = Form(""),
    resolution: str = Form(""),
    reason: str = Form(...),
    user: dict = Depends(require_tool("building.manage")),
) -> RedirectResponse:
    def action() -> None:
        transition_service_request(
            service_request_id,
            ServiceRequestTransitionInput(
                target_status=target_status,
                assigned_owner=assigned_owner.strip(),
                due_at=_local_datetime(due_at) if due_at.strip() else None,
                resolution=resolution.strip(),
                reason=reason.strip(),
                actor=_actor(user),
            ),
            request,
            _internal_key(request),
        )

    return _run_form_action(
        action,
        f"Service request moved to {target_status.replace('_', ' ')}.",
    )
