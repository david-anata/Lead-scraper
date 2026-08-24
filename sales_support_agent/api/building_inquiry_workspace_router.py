"""Authenticated staff workspace for one Building inquiry."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
import re
from typing import Any, Optional
from zoneinfo import ZoneInfo
from urllib.parse import quote_plus
from uuid import uuid4

from fastapi import APIRouter, Depends, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import delete, select

from sales_support_agent.models.database import session_scope
from sales_support_agent.models.entities import (
    BuildingAgreement,
    BuildingAgreementTemplate,
    BuildingAuditEvent,
    BuildingAvailabilityBlock,
    BuildingCalendarProjection,
    BuildingContact,
    BuildingInquiry,
    BuildingOffering,
    BuildingRatePlan,
    BuildingRelationship,
    BuildingReservation,
)
from sales_support_agent.services.admin_nav import render_agent_nav
from sales_support_agent.services.auth_deps import require_tool
from sales_support_agent.services.building_event_calendar import (
    access_window,
    clock_label,
    guest_hour_options,
    month_availability,
)
from sales_support_agent.services.building_inquiry_workspace import (
    attendance_guess,
    is_test_inquiry,
    render_inquiry_workspace,
)
from sales_support_agent.services.building_lead_removal import (
    is_archived,
    lead_attachments,
    remove_lead,
    remove_test_leads,
    restore_lead,
)
from sales_support_agent.services.building_calendar import (
    queue_calendar_projection,
)
from sales_support_agent.services.building_agreement_readiness import (
    propagate_event_readiness_terminal_state,
)
from sales_support_agent.services.building_contract_readiness import (
    active_reservation_for,
    contract_readiness,
    undo_refusal,
)
from sales_support_agent.services.building_event_journey import load_event_journey
from sales_support_agent.services.building_lead_intake import prefill_event_interview
from pydantic import ValidationError

from sales_support_agent.api.building_booking_router import (
    EventReviewInput,
    create_event_review,
)
from sales_support_agent.api.building_calendar_router import (
    CalendarSyncInput,
    sync_calendar_projections,
)
from sales_support_agent.api.building_agreement_readiness_router import (
    AgreementPackageInput,
    prepare_agreement_package,
)
from sales_support_agent.services.building_lead_quote_sync import (
    sync_quote_from_lead_pricing,
)
from sales_support_agent.services.building_lead_pricing import (
    LeadPricingError,
    compute_totals,
    default_pricing,
    parse_pricing_form,
)
from sales_support_agent.services.building_public_availability import candidate_date_availability
from sales_support_agent.integrations.building_google_calendar import BuildingGoogleCalendarClient
from sales_support_agent.services.building_security import (
    csrf_token,
    require_building_form_security,
)


MOUNTAIN = ZoneInfo("America/Denver")
CANONICAL_ARENA_OFFERING_ID = "arena-events"


def _event_offering(session: Any, requested_id: str = "") -> BuildingOffering | None:
    """Resolve event work to the governed Arena offering whenever it exists."""

    requested = session.get(BuildingOffering, requested_id) if requested_id else None
    canonical = session.get(BuildingOffering, CANONICAL_ARENA_OFFERING_ID)
    for offering in (canonical, requested):
        if offering is not None and offering.offering_type == "event":
            return offering
    return session.execute(
        select(BuildingOffering)
        .where(BuildingOffering.offering_type == "event")
        .order_by(BuildingOffering.name, BuildingOffering.id)
    ).scalars().first()
FORM_DEPS = [Depends(require_building_form_security)]
router = APIRouter(
    prefix="/admin/building/inquiries",
    tags=["building-inquiry-workspace"],
)


def _clock_value(raw: Any) -> str:
    """Read a stored time as a whole-hour option value, or return empty."""

    text = str(raw or "").strip().upper().replace(".", "")
    text = re.sub(r"(?<=\d)(AM|PM)$", r" \1", text)
    if not text:
        return ""
    for pattern in ("%I:%M %p", "%I %p", "%H:%M", "%H"):
        try:
            return f"{datetime.strptime(text, pattern).hour:02d}:00"
        except ValueError:
            continue
    return ""


def _guest_schedule_window(raw: Any) -> tuple[str, str]:
    """Return an unambiguous whole-hour range from the saved interview.

    Staff may write ``Guests 09:00–15:00`` or ``5pm to 10pm`` in the event
    interview. Exactly two recognizable times are required; prose containing
    fewer or additional times is left for the operator instead of guessed.
    """

    matches = re.findall(
        r"(?<!\d)(?:[01]?\d|2[0-3])(?::[0-5]\d)?\s*(?:a\.?m\.?|p\.?m\.?)?(?!\d)",
        str(raw or ""),
        flags=re.IGNORECASE,
    )
    if len(matches) != 2:
        return "", ""
    start, end = (_clock_value(value) for value in matches)
    if not start or not end or start == end:
        return "", ""
    return start, end


def _details_with_interview_hours(
    details: dict[str, Any], interview: dict[str, Any]
) -> dict[str, Any]:
    """Fill missing structured hours from the operator-validated interview."""

    effective = dict(details)
    start, end = _guest_schedule_window(interview.get("guest_schedule"))
    if start and not _clock_value(effective.get("guestStartTime")):
        effective["guestStartTime"] = start
    if end and not _clock_value(effective.get("guestEndTime")):
        effective["guestEndTime"] = end
    return effective


def _calendar_view(
    session: Any,
    *,
    inquiry: BuildingInquiry,
    details: dict[str, Any],
    month: str,
    date_choice: str,
) -> dict[str, Any]:
    """Build the month grid and, when a day is chosen, that day's hours.

    The prospect's own submission decides what is preselected, so an operator
    confirms a date rather than retyping one that is already on the page.
    """

    offering = _event_offering(session, str(inquiry.offering_id or ""))
    if offering is None:
        return {}

    preferred = inquiry.preferred_date
    selected: Optional[date] = None
    for candidate in (date_choice, preferred.isoformat() if preferred else ""):
        try:
            selected = date.fromisoformat(str(candidate))
            break
        except ValueError:
            continue
    try:
        shown = date.fromisoformat(f"{month}-01") if month else None
    except ValueError:
        shown = None
    if shown is None:
        shown = (selected or datetime.now(MOUNTAIN).date()).replace(day=1)

    requested: list[date] = []
    for raw in (
        preferred.isoformat() if preferred else "",
        details.get("alternateDate") or details.get("alternate_date") or "",
        details.get("backupDate2") or details.get("backup_date_2") or "",
    ):
        try:
            requested.append(date.fromisoformat(str(raw)))
        except ValueError:
            continue

    calendar_client = BuildingGoogleCalendarClient()
    view = month_availability(
        session,
        calendar=calendar_client,
        month=shown,
        space_id=offering.space_id,
        exclude_inquiry_id=inquiry.id,
        requested=requested,
    )
    result: dict[str, Any] = {"calendar": view}
    if selected is None or not any(
        cell["iso"] == selected.isoformat() and cell["selectable"]
        for cell in view["cells"]
    ):
        return result

    busy = view["busy"].get(selected.isoformat(), [])
    options = guest_hour_options(busy, selected)
    open_values = [item["value"] for item in options if not item["taken"]]
    guest_start = _clock_value(details.get("guestStartTime")) or (
        open_values[0] if open_values else options[0]["value"]
    )
    guest_end = _clock_value(details.get("guestEndTime")) or (
        open_values[-1] if open_values else options[-1]["value"]
    )
    setup, guests_in, guests_out, teardown = access_window(
        selected,
        time(int(guest_start[:2])),
        time(int(guest_end[:2])),
    )

    def stamp(value: datetime) -> str:
        return value.strftime("%a %b %d, ") + clock_label(value.hour)

    chosen = next(
        (item for item in view["cells"] if item["iso"] == selected.isoformat()), {}
    )
    result.update({
        "selected_date": selected.isoformat(),
        "selected_label": selected.strftime("%A, %B %d, %Y"),
        "selected_occupied": bool(chosen.get("occupied")),
        "selected_note": str(chosen.get("note") or ""),
        "hour_options": options,
        "guest_start": guest_start,
        "guest_end": guest_end,
        "preview_window": {
            "setup": stamp(setup),
            "teardown": stamp(teardown),
            "guests": f"{clock_label(guests_in.hour)} to {clock_label(guests_out.hour)}",
        },
    })
    return result


def _contract_target_label(view: dict[str, Any]) -> str:
    """Name the date and hours the press is about to take, before it is taken.

    Saying it on the button is what makes one press safe: the operator reads
    the decision rather than discovering it afterwards.
    """

    label = str(view.get("selected_label") or "")
    if not label:
        return ""
    window = dict(view.get("preview_window") or {})
    guests = str(window.get("guests") or "")
    return f"{label} (guests {guests})" if guests else label


def _contract_confirmation(view: dict[str, Any]) -> dict[str, Any]:
    """Describe the date a contract is about to take, and what it would sit on.

    Named plainly rather than hidden behind a refusal: taking an occupied date
    is allowed, but only as a decision someone made looking at the clash.
    """

    selected = str(view.get("selected_date") or "")
    if not selected:
        return {
            "ready": False,
            "message": (
                "Choose a day on the calendar above, then create the contract. "
                "It attaches to that date."
            ),
        }
    cell = next(
        (item for item in view.get("calendar", {}).get("cells", [])
         if item["iso"] == selected),
        {},
    )
    window = dict(view.get("preview_window") or {})
    clash = str(cell.get("note") or "") if cell.get("state") in {
        "pending", "booked", "external",
    } else ""
    return {
        "ready": True,
        "date": selected,
        "label": str(view.get("selected_label") or selected),
        "setup": window.get("setup", ""),
        "teardown": window.get("teardown", ""),
        "guests": window.get("guests", ""),
        "guest_start": str(view.get("guest_start") or ""),
        "guest_end": str(view.get("guest_end") or ""),
        "clash": clash,
    }


def _seeded_pricing(
    session: Any,
    payload: dict[str, Any],
    *,
    transaction_date: date | None = None,
) -> dict[str, Any]:
    """Return this lead's pricing, seeded from the approved plan when unset.

    The page and the save path must seed identically. When they did not, the
    first save replaced the plan the operator was looking at with an unbound
    baseline, and the contract lost its cancellation policy with it.
    """

    stored = dict(payload.get("_pricing") or {})
    approved = session.execute(
        select(BuildingRatePlan)
        .where(BuildingRatePlan.status == "approved")
        .order_by(BuildingRatePlan.name, BuildingRatePlan.version.desc())
    ).scalars().all()
    effective_on = transaction_date or date.today()
    plan = next(
        (
            row for row in approved
            if row.effective_from <= effective_on
            and (row.effective_until is None or row.effective_until >= effective_on)
        ),
        None,
    )
    if stored:
        # Pricing saved before tax approval keeps the customer's negotiated
        # money, but inherits the current approved tax determination. Tax is a
        # jurisdictional rule, not an operator discount field.
        if plan is not None:
            stored["tax_status"] = plan.tax_status
            stored["tax_rate_bps"] = plan.tax_rate_bps
        return stored
    return default_pricing({
        "id": plan.id,
        "name": plan.name,
        "currency": plan.currency,
        "unit_amount_cents": plan.unit_amount_cents,
        "minimum_units": plan.minimum_units,
        "deposit_percent_bps": plan.deposit_percent_bps,
        "tax_status": plan.tax_status,
        "tax_rate_bps": plan.tax_rate_bps,
    } if plan is not None else None)


@router.get("/{inquiry_id}/availability")
def inquiry_date_availability(
    inquiry_id: str,
    request: Request,
    dates: str = Query(min_length=10, max_length=32),
    setup_start_time: str = Query(default="", max_length=8),
    guest_start_time: str = Query(default="", max_length=8),
    guest_end_time: str = Query(default="", max_length=8),
    teardown_end_time: str = Query(default="", max_length=8),
    user: dict = Depends(require_tool("building.events.manage")),
) -> dict:
    """Compare candidate dates without exposing calendar event details."""

    del user
    raw_dates = list(dict.fromkeys(item.strip() for item in dates.split(",") if item.strip()))
    if not raw_dates or len(raw_dates) > 3:
        raise HTTPException(status_code=422, detail="Provide one to three candidate dates.")
    try:
        candidates = [date.fromisoformat(item) for item in raw_dates]
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="Candidate dates must use YYYY-MM-DD.") from exc
    today = date.today()
    if any(item < today or item > today + timedelta(days=730) for item in candidates):
        raise HTTPException(status_code=422, detail="Candidate dates must be within the next two years.")
    with session_scope(request.app.state.session_factory) as session:
        inquiry = session.get(BuildingInquiry, inquiry_id)
        if inquiry is None or inquiry.kind != "event":
            raise HTTPException(status_code=404, detail="Event inquiry not found.")
        try:
            return candidate_date_availability(
                session,
                calendar=BuildingGoogleCalendarClient(),
                candidates=candidates,
                setup_start_time=setup_start_time,
                guest_start_time=guest_start_time,
                guest_end_time=guest_end_time,
                teardown_end_time=teardown_end_time,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.get("/{inquiry_id}", response_class=HTMLResponse)
def inquiry_workspace(
    inquiry_id: str,
    request: Request,
    notice: str = "",
    error: str = "",
    month: str = "",
    confirm: str = "",
    date_choice: str = Query(default="", alias="date"),
    user: dict = Depends(require_tool("building.manage")),
) -> HTMLResponse:
    """Show one inquiry without exposing unrelated customer records."""

    with session_scope(request.app.state.session_factory) as session:
        inquiry = session.get(BuildingInquiry, inquiry_id)
        if inquiry is None:
            raise HTTPException(status_code=404, detail="Inquiry not found.")
        relationship = session.execute(
            select(BuildingRelationship).where(
                BuildingRelationship.source_reference == f"inquiry:{inquiry.id}"
            )
        ).scalars().first()
        contact = (
            session.get(BuildingContact, relationship.contact_id)
            if relationship is not None
            else None
        )
        reservation = active_reservation_for(session, inquiry.id)
        activity = session.execute(
            select(BuildingAuditEvent)
            .where(
                BuildingAuditEvent.entity_type == "inquiry",
                BuildingAuditEvent.entity_id == inquiry.id,
                # Autosaves stay in the record but not on the page: they are a
                # keystroke mechanic, not something an operator did.
                BuildingAuditEvent.action != "event_interview_autosaved",
            )
            .order_by(BuildingAuditEvent.created_at.desc())
            .limit(100)
        ).scalars().all()
        contact_options = [] if contact is not None else [
            {"id": row.id, "label": f"{row.full_name or row.email} · {row.email}"}
            for row in session.execute(
                select(BuildingContact)
                .where(BuildingContact.status == "active")
                .order_by(BuildingContact.full_name, BuildingContact.email)
                .limit(500)
            ).scalars().all()
        ]
        rate_plan_rows = session.execute(
            select(BuildingRatePlan)
            .where(BuildingRatePlan.status == "approved")
            .order_by(BuildingRatePlan.name, BuildingRatePlan.version.desc())
        ).scalars().all()
        payload = dict(inquiry.payload_json or {})
        stored_pricing = _seeded_pricing(
            session,
            payload,
            transaction_date=(reservation.starts_at.date() if reservation else date.today()),
        )
        public_details = {
            key: value for key, value in payload.items() if not str(key).startswith("_")
        }
        corrections = dict(payload.get("_lead_corrections") or {})
        effective_details = {
            **public_details,
            **{
                key: value
                for key, value in {
                    "eventType": corrections.get("event_type"),
                    "guestStartTime": corrections.get("guest_start_time"),
                    "guestEndTime": corrections.get("guest_end_time"),
                }.items()
                if str(value or "").strip()
            },
        }
        prefilled_interview, _ = prefill_event_interview(
            preferred_date=inquiry.preferred_date,
            details=effective_details,
        )
        event_interview = {
            **prefilled_interview,
            **dict(payload.get("_event_interview") or {}),
        }
        effective_details = _details_with_interview_hours(
            effective_details, event_interview
        )
        data = {
            "id": inquiry.id,
            "name": inquiry.name,
            "email": inquiry.email,
            "phone": inquiry.phone or (contact.phone if contact else ""),
            "kind": inquiry.kind,
            "preferred_date": (
                inquiry.preferred_date.isoformat() if inquiry.preferred_date else ""
            ),
            "source": inquiry.source,
            "source_reference": inquiry.source_reference,
            "assigned_owner": inquiry.assigned_owner,
            "response_due_at": inquiry.response_due_at,
            "created_at": inquiry.created_at,
            "details": public_details,
            "lead_corrections": corrections,
            "lifecycle": dict(payload.get("_lifecycle") or {}),
            "attribution": dict(payload.get("_attribution") or {}),
            "event_interview": event_interview,
            "lead_notification": dict(payload.get("_lead_notification") or {}),
            "lead_escalation": dict(payload.get("_lead_escalation") or {}),
            "customer_receipt": dict(payload.get("_customer_receipt") or {}),
            "follow_up_sequence": list(payload.get("_follow_up_sequence") or []),
            "reservation_id": reservation.id if reservation else "",
            "contact": {
                "id": contact.id,
                "full_name": contact.full_name,
                "email": contact.email,
                "phone": contact.phone or "",
            } if contact is not None else {},
            "contact_options": contact_options,
            "pricing": stored_pricing,
            "pricing_totals": compute_totals(stored_pricing),
            "rate_plans": [
                {
                    "name": row.name,
                    "version": row.version,
                    "currency": row.currency,
                    "unit_amount_cents": row.unit_amount_cents,
                    "public_price_display": row.public_price_display,
                    "booking_unit": row.booking_unit,
                    "deposit_type": row.deposit_type,
                    "deposit_amount_cents": row.deposit_amount_cents,
                    "deposit_percent_bps": row.deposit_percent_bps,
                    "tax_status": row.tax_status,
                    "tax_rate_bps": row.tax_rate_bps,
                }
                for row in rate_plan_rows
            ],
            "offering_options": [
                {"id": row.id, "name": row.name}
                for row in session.execute(
                    select(BuildingOffering)
                    .where(BuildingOffering.offering_type == "event")
                    .order_by(BuildingOffering.name)
                ).scalars().all()
            ],
            "offering_id": inquiry.offering_id or "",
            "is_archived": is_archived(inquiry),
            "attachments": lead_attachments(session, inquiry.id),
            "is_test": is_test_inquiry(
                name=inquiry.name,
                email=inquiry.email,
                source=inquiry.source,
            ),
            "activity": [
                {
                    "action": row.action,
                    "actor": row.actor,
                    "created_at": row.created_at,
                }
                for row in activity
            ],
        }
        if reservation is not None:
            data["journey"] = load_event_journey(session, reservation)
            # Compatibility for the existing pricing/contract control while
            # the richer joined journey powers every later section.
            contract = dict(data["journey"].get("contract") or {})
            if contract:
                current_total = compute_totals(stored_pricing)["total_cents"]
                data["agreement"] = {
                    "id": contract.get("id"),
                    "version": contract.get("version"),
                    "status": contract.get("preparation_status"),
                    "document_url": contract.get("document_url"),
                    "amount_cents": contract.get("amount_cents"),
                    "current_total_cents": current_total,
                    "is_stale": int(contract.get("amount_cents") or 0) != current_total,
                }
        if inquiry.kind == "event" and reservation is None:
            data.update(
                _calendar_view(
                    session,
                    inquiry=inquiry,
                    details=effective_details,
                    month=month,
                    date_choice=date_choice,
                )
            )
            if confirm == "contract":
                data["confirm_contract"] = _contract_confirmation(data)
        # The page and the route ask the same question, so the page can never
        # offer a press the route would refuse.
        verdict = contract_readiness(
            session,
            inquiry,
            current_total_cents=compute_totals(stored_pricing)["total_cents"],
        )
        if verdict.blocked and verdict.reason != "already_has_contract":
            data["contract_blocked"] = {
                "reason": verdict.reason,
                "message": verdict.message,
                "fix_url": verdict.fix_url,
                "fix_label": verdict.fix_label,
            }
        elif verdict.ready and reservation is None:
            label = _contract_target_label(data)
            if not label and inquiry.preferred_date:
                # A lead intake filed as a workspace request never builds the
                # calendar view, but it still has the date the customer asked
                # for, and that is the date the press will take.
                label = inquiry.preferred_date.strftime("%A, %B %d, %Y")
            data["contract_target"] = label
        if reservation is not None:
            agreement_row = session.execute(
                select(BuildingAgreement)
                .where(BuildingAgreement.reservation_id == reservation.id)
                .order_by(BuildingAgreement.version.desc())
            ).scalars().first()
            data["contract_undoable"] = not undo_refusal(
                session, agreement_row, reservation
            )
    return HTMLResponse(
        render_inquiry_workspace(
            navigation=render_agent_nav("building", user=user),
            data=data,
            csrf_token=csrf_token(user),
            notice=notice,
            error=error,
        ),
        headers={"Cache-Control": "private, no-store"},
    )


@router.post("/{inquiry_id}/details", dependencies=FORM_DEPS)
async def update_lead_details(
    inquiry_id: str,
    request: Request,
    user: dict = Depends(require_tool("building.manage")),
) -> RedirectResponse:
    """Correct staff-operational lead fields without rewriting the submission.

    Marketplace and staff-created leads are sometimes filed under the wrong
    journey.  The correction is explicit and audited; the prospect's original
    payload stays intact for evidence and the corrected values drive the event
    calendar, interview prefill, and contract workflow.
    """

    target = f"/admin/building/inquiries/{inquiry_id}"
    form = await request.form()
    kind = str(form.get("kind") or "").strip().lower()
    if kind not in {"event", "tour", "workspace"}:
        return RedirectResponse(
            f"{target}?error={quote_plus('Choose Event, Tour, or Workspace.')}",
            status_code=303,
        )
    name = str(form.get("name") or "").strip()
    email = str(form.get("email") or "").strip().lower()
    if not name:
        return RedirectResponse(
            f"{target}?error={quote_plus('Customer name is required.')}", status_code=303
        )
    if "@" not in email or email.startswith("@") or email.endswith("@"):
        return RedirectResponse(
            f"{target}?error={quote_plus('Enter a valid customer email.')}", status_code=303
        )
    raw_date = str(form.get("preferred_date") or "").strip()
    try:
        preferred_date = date.fromisoformat(raw_date) if raw_date else None
    except ValueError:
        return RedirectResponse(
            f"{target}?error={quote_plus('Requested date must be a valid date.')}",
            status_code=303,
        )
    offering_id = str(form.get("offering_id") or "").strip()
    actor = str(user.get("email") or "building-operator")
    with session_scope(request.app.state.session_factory) as session:
        inquiry = session.get(BuildingInquiry, inquiry_id)
        if inquiry is None:
            raise HTTPException(status_code=404, detail="Inquiry not found.")
        offering = session.get(BuildingOffering, offering_id) if offering_id else None
        if offering is not None and offering.offering_type != "event":
            return RedirectResponse(
                f"{target}?error={quote_plus('Choose an event offering for an event lead.')}",
                status_code=303,
            )
        if kind == "event":
            offering = _event_offering(session, offering_id)
        before = {
            "kind": inquiry.kind,
            "name": inquiry.name,
            "email": inquiry.email,
            "phone": inquiry.phone,
            "preferred_date": inquiry.preferred_date.isoformat()
            if inquiry.preferred_date else "",
            "offering_id": inquiry.offering_id or "",
            "corrections": dict((inquiry.payload_json or {}).get("_lead_corrections") or {}),
        }
        payload = dict(inquiry.payload_json or {})
        corrections = {
            "event_type": str(form.get("event_type") or "").strip(),
            "guest_start_time": str(form.get("guest_start_time") or "").strip(),
            "guest_end_time": str(form.get("guest_end_time") or "").strip(),
            "updated_by": actor,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        inquiry.kind = kind
        inquiry.name = name
        inquiry.email = email
        inquiry.phone = str(form.get("phone") or "").strip()
        inquiry.preferred_date = preferred_date
        inquiry.offering_id = offering.id if offering is not None else None
        payload["_lead_corrections"] = corrections
        inquiry.payload_json = payload
        inquiry.updated_at = datetime.now(timezone.utc)
        session.add(BuildingAuditEvent(
            entity_type="inquiry",
            entity_id=inquiry.id,
            action="lead_details_corrected",
            actor=actor,
            before_json=before,
            after_json={
                "kind": inquiry.kind,
                "name": inquiry.name,
                "email": inquiry.email,
                "phone": inquiry.phone,
                "preferred_date": inquiry.preferred_date.isoformat()
                if inquiry.preferred_date else "",
                "offering_id": inquiry.offering_id or "",
                "corrections": corrections,
                "original_submission_changed": False,
                "customer_contacted": False,
            },
        ))
    return RedirectResponse(
        f"{target}?notice={quote_plus('Lead details updated. Nothing was sent.')}",
        status_code=303,
    )


@router.post("/{inquiry_id}/link-contact", dependencies=FORM_DEPS)
def link_existing_contact(
    inquiry_id: str,
    request: Request,
    contact_id: str = Form(...),
    user: dict = Depends(require_tool("building.crm.manage")),
) -> RedirectResponse:
    """Attach an existing customer to this lead.

    Only the relationship is written. The saved contact's name, email, phone,
    and company are left exactly as they are — the control-room contact form
    overwrites those fields, so linking deliberately does not go through it.
    """

    target = f"/admin/building/inquiries/{inquiry_id}"
    with session_scope(request.app.state.session_factory) as session:
        inquiry = session.get(BuildingInquiry, inquiry_id)
        if inquiry is None:
            raise HTTPException(status_code=404, detail="Inquiry not found.")
        contact = session.get(BuildingContact, contact_id.strip())
        if contact is None:
            return RedirectResponse(
                f"{target}?error=That+customer+no+longer+exists.", status_code=303
            )
        reference = f"inquiry:{inquiry.id}"
        existing = session.execute(
            select(BuildingRelationship).where(
                BuildingRelationship.source_reference == reference,
                BuildingRelationship.status == "active",
            )
        ).scalars().first()
        if existing is not None:
            if existing.contact_id == contact.id:
                return RedirectResponse(
                    f"{target}?notice=That+customer+is+already+linked.", status_code=303
                )
            return RedirectResponse(
                f"{target}?error=This+lead+is+already+linked+to+another+customer.",
                status_code=303,
            )
        session.add(BuildingRelationship(
            id=str(uuid4()),
            contact_id=contact.id,
            relationship_type="prospect",
            status="active",
            source_reference=reference,
        ))
        session.add(BuildingAuditEvent(
            entity_type="inquiry",
            entity_id=inquiry.id,
            action="inquiry_contact_linked",
            actor=str(user.get("email") or "building-operator"),
            after_json={
                "contact_id": contact.id,
                "created_contact": False,
                "contact_details_modified": False,
            },
        ))
    return RedirectResponse(
        f"{target}?notice=Customer+linked+to+this+lead.", status_code=303
    )


@router.post("/{inquiry_id}/pricing", dependencies=FORM_DEPS)
async def save_lead_pricing(
    inquiry_id: str,
    request: Request,
    user: dict = Depends(require_tool("building.manage")),
) -> RedirectResponse:
    """Save pricing for this lead only.

    Never writes to the rate plan, so the standard rate and every other lead are
    untouched. Deliberately editable at any stage; each generated contract keeps
    its own copy of the numbers it was built from.
    """

    target = f"/admin/building/inquiries/{inquiry_id}"
    form = await request.form()
    with session_scope(request.app.state.session_factory) as session:
        inquiry = session.get(BuildingInquiry, inquiry_id)
        if inquiry is None:
            raise HTTPException(status_code=404, detail="Inquiry not found.")
        payload = dict(inquiry.payload_json or {})
        try:
            pricing = parse_pricing_form(
                form,
                existing=_seeded_pricing(session, payload),
                actor=str(user.get("email") or "building-operator"),
            )
        except LeadPricingError as exc:
            return RedirectResponse(
                f"{target}?error={quote_plus(str(exc))}", status_code=303
            )
        before = dict(payload.get("_pricing") or {})
        payload["_pricing"] = pricing
        inquiry.payload_json = payload
        inquiry.updated_at = datetime.now(timezone.utc)
        session.add(inquiry)
        totals = compute_totals(pricing)
        session.add(BuildingAuditEvent(
            entity_type="inquiry",
            entity_id=inquiry.id,
            action="lead_pricing_updated",
            actor=str(user.get("email") or "building-operator"),
            before_json={"total_cents": compute_totals(before)["total_cents"]} if before else {},
            after_json={
                "total_cents": totals["total_cents"],
                "deposit_cents": totals["deposit_cents"],
                "rate_plan_changed": False,
                "customer_contacted": False,
            },
        ))
    return RedirectResponse(
        f"{target}?notice=Pricing+saved+for+this+lead+only.", status_code=303
    )


@router.post("/{inquiry_id}/contract", dependencies=FORM_DEPS)
async def create_contract_from_lead(
    inquiry_id: str,
    request: Request,
    user: dict = Depends(require_tool("building.agreements.prepare")),
) -> RedirectResponse:
    """Turn this lead into a contract without leaving it.

    A contract needs a date to attach to, so one is taken here rather than sent
    back for. The operator is shown the exact window first, and any clash it
    would double-book, because that decision is theirs to make knowingly.

    Writes the lead's pricing into the booking's quote, then prepares the
    agreement package from it. The quote keeps its existing shape, so billing,
    invoicing, and the QuickBooks handoff are untouched.
    """

    target = f"/admin/building/inquiries/{inquiry_id}"
    actor = str(user.get("email") or "building-operator")
    form = await request.form()
    override = str(form.get("override_conflicts") or "").strip().lower() in {
        "1", "true", "yes", "on",
    }
    with session_scope(request.app.state.session_factory) as session:
        inquiry = session.get(BuildingInquiry, inquiry_id)
        if inquiry is None:
            raise HTTPException(status_code=404, detail="Inquiry not found.")
        # The same verdict the page used to decide whether to draw a button. A
        # press that arrives anyway (a stale tab, a resubmit) gets the reason in
        # writing rather than a reload that says nothing.
        verdict = contract_readiness(
            session,
            inquiry,
            current_total_cents=compute_totals(
                _seeded_pricing(session, dict(inquiry.payload_json or {}))
            )["total_cents"],
        )
        reservation = active_reservation_for(session, inquiry.id)
        has_reservation = reservation is not None
        reservation_id = reservation.id if reservation else ""
    if verdict.blocked:
        return RedirectResponse(
            f"{target}?error={quote_plus(verdict.message)}", status_code=303
        )
    if not has_reservation:
        plan, plan_error = _auto_hold_plan(request, inquiry_id=inquiry_id, form=form)
        if plan_error:
            return RedirectResponse(
                f"{target}?error={quote_plus(plan_error)}", status_code=303
            )
        # One press does the job. The single exception is a date already taken:
        # there is one Arena, so double-booking it is a decision somebody makes
        # looking at what they would be booking over.
        if (plan.get("clash") or plan.get("needs_attendance")) and not override:
            return RedirectResponse(
                f"{target}?confirm=contract#date-review", status_code=303
            )
        reservation_id, error = _take_the_date(
            request,
            inquiry_id=inquiry_id,
            form=plan["form"],
            actor=actor,
            override=override,
        )
        if error:
            return RedirectResponse(
                f"{target}?error={quote_plus(error)}", status_code=303
            )
    with session_scope(request.app.state.session_factory) as session:
        inquiry = session.get(BuildingInquiry, inquiry_id)
        reservation = session.get(BuildingReservation, reservation_id)
        if inquiry is None or reservation is None:
            raise HTTPException(status_code=404, detail="Inquiry not found.")
        payload = dict(inquiry.payload_json or {})
        # The pricing shown on the page is the pricing, whether or not anyone
        # pressed Save on figures they never changed. Seeding here uses the same
        # approved plan the panel displays, so the contract cannot disagree with
        # what the operator was looking at.
        pricing = _seeded_pricing(session, payload)
        if not payload.get("_pricing"):
            payload["_pricing"] = pricing
            inquiry.payload_json = payload
            session.add(inquiry)
        quote = sync_quote_from_lead_pricing(
            session, reservation=reservation, pricing=pricing, actor=actor
        )
        session.flush()
        quote_id = quote.id
        session.add(BuildingAuditEvent(
            entity_type="inquiry",
            entity_id=inquiry.id,
            action="lead_quote_synced",
            actor=actor,
            after_json={
                "quote_id": quote_id,
                "quote_version": quote.version,
                "amount_cents": quote.amount_cents,
                "customer_contacted": False,
            },
        ))

    template = _approved_template_id(request)
    if not template:
        return RedirectResponse(
            f"{target}?error={quote_plus('No approved contract template exists yet.')}",
            status_code=303,
        )
    try:
        result = prepare_agreement_package(
            AgreementPackageInput(
                reservation_id=reservation_id,
                quote_id=quote_id,
                template_id=template,
                agreement_version=_next_agreement_version(request, reservation_id),
                payment_version=_next_agreement_version(request, reservation_id),
                actor=actor,
            ),
            request,
            f"lead-contract-{uuid4().hex[:16]}",
            _internal_api_key(request),
        )
    except (HTTPException, ValidationError, ValueError) as exc:
        return RedirectResponse(
            f"{target}?error={quote_plus(str(getattr(exc, 'detail', exc)))}",
            status_code=303,
        )
    return RedirectResponse(
        f"{target}?notice=Contract+prepared+from+this+lead."
        "+Nothing+was+sent.#agreement",
        status_code=303,
    )


@router.post("/{inquiry_id}/contract/undo", dependencies=FORM_DEPS)
async def undo_contract_from_lead(
    inquiry_id: str,
    request: Request,
    user: dict = Depends(require_tool("building.agreements.prepare")),
) -> RedirectResponse:
    """Put the lead back the way it was before the contract was created.

    One press created the hold, the quote and the package, so one press takes
    all three back. Nothing is deleted: the agreement and quote versions stay in
    the history as cancelled, because billing and invoicing read those records
    and a deleted quote would strand an invoice.
    """

    target = f"/admin/building/inquiries/{inquiry_id}"
    actor = str(user.get("email") or "building-operator")
    now = datetime.now(timezone.utc)
    with session_scope(request.app.state.session_factory) as session:
        inquiry = session.get(BuildingInquiry, inquiry_id)
        if inquiry is None:
            raise HTTPException(status_code=404, detail="Inquiry not found.")
        reservation = active_reservation_for(session, inquiry.id)
        agreement = (
            session.execute(
                select(BuildingAgreement)
                .where(BuildingAgreement.reservation_id == reservation.id)
                .order_by(BuildingAgreement.version.desc())
            ).scalars().first()
            if reservation is not None
            else None
        )
        refusal = undo_refusal(session, agreement, reservation)
        if refusal:
            return RedirectResponse(
                f"{target}?error={quote_plus(refusal)}", status_code=303
            )

        before = {
            "reservation_status": reservation.status,
            "agreement_id": agreement.id,
            "agreement_version": agreement.version,
            "agreement_status": agreement.preparation_status,
        }
        session.execute(
            delete(BuildingAvailabilityBlock).where(
                BuildingAvailabilityBlock.source_reference
                == f"reservation:{reservation.id}"
            )
        )
        reservation.status = "cancelled"
        reservation.hold_expires_at = None
        reservation.updated_at = now
        # Cancels the prepared package and its payment readiness in one place,
        # the same way an expiring hold does, so an undone contract and an
        # expired one leave the records in the same shape.
        propagate_event_readiness_terminal_state(
            session, reservation, terminal_status="cancelled", actor=actor
        )
        queue_calendar_projection(session, reservation)
        session.add(BuildingAuditEvent(
            entity_type="inquiry",
            entity_id=inquiry.id,
            action="lead_contract_undone",
            actor=actor,
            before_json=before,
            after_json={
                "reservation_status": "cancelled",
                "availability_released": True,
                "undone_at": now.isoformat(),
                "customer_contacted": False,
            },
        ))
    return RedirectResponse(
        f"{target}?notice=Undone.+The+date+is+free+again+and+the+contract+is"
        "+cancelled.",
        status_code=303,
    )


@router.post("/{inquiry_id}/remove", dependencies=FORM_DEPS)
def remove_lead_from_board(
    inquiry_id: str,
    request: Request,
    user: dict = Depends(require_tool("building.manage")),
) -> RedirectResponse:
    """Take a lead off the list. Deletes it outright when nothing hangs off it."""

    actor = str(user.get("email") or "building-operator")
    with session_scope(request.app.state.session_factory) as session:
        inquiry = session.get(BuildingInquiry, inquiry_id)
        if inquiry is None:
            raise HTTPException(status_code=404, detail="Inquiry not found.")
        outcome = remove_lead(session, inquiry, actor=actor)
    if outcome.action == "refused":
        return RedirectResponse(
            f"/admin/building/inquiries/{inquiry_id}"
            f"?error={quote_plus(outcome.message)}",
            status_code=303,
        )
    # A deleted lead has no page left to return to.
    target = (
        "/admin/building/sales"
        if outcome.action == "deleted"
        else f"/admin/building/inquiries/{inquiry_id}"
    )
    return RedirectResponse(
        f"{target}?notice={quote_plus(outcome.message)}", status_code=303
    )


@router.post("/{inquiry_id}/restore", dependencies=FORM_DEPS)
def restore_lead_to_board(
    inquiry_id: str,
    request: Request,
    user: dict = Depends(require_tool("building.manage")),
) -> RedirectResponse:
    actor = str(user.get("email") or "building-operator")
    with session_scope(request.app.state.session_factory) as session:
        inquiry = session.get(BuildingInquiry, inquiry_id)
        if inquiry is None:
            raise HTTPException(status_code=404, detail="Inquiry not found.")
        outcome = restore_lead(session, inquiry, actor=actor)
    key = "error" if outcome.action == "refused" else "notice"
    return RedirectResponse(
        f"/admin/building/inquiries/{inquiry_id}"
        f"?{key}={quote_plus(outcome.message)}",
        status_code=303,
    )


def _approved_template_id(request: Request) -> str:
    with session_scope(request.app.state.session_factory) as session:
        row = session.execute(
            select(BuildingAgreementTemplate.id)
            .where(BuildingAgreementTemplate.status == "approved")
            .order_by(BuildingAgreementTemplate.version.desc())
        ).scalars().first()
    return str(row or "")


def _next_agreement_version(request: Request, reservation_id: str) -> int:
    with session_scope(request.app.state.session_factory) as session:
        used = session.execute(
            select(BuildingAgreement.version).where(
                BuildingAgreement.reservation_id == reservation_id
            )
        ).scalars().all()
    return (max(used) + 1) if used else 1


def _internal_api_key(request: Request) -> str:
    key = str(getattr(request.app.state.settings, "internal_api_key", "") or "").strip()
    if not key:
        raise HTTPException(status_code=503, detail="Internal API is not configured.")
    return key


def _auto_hold_plan(
    request: Request, *, inquiry_id: str, form: Any
) -> tuple[dict[str, Any], str]:
    """Work out the date and hours this press should take, without asking.

    The page already derives all of it from the customer's own request: the day
    they asked for, the hours they gave, and the owner's setup and teardown
    buffers. Making the operator re-pick it on a second screen only asked them
    to retype what the lead already said. A date the form carries wins, because
    that is a deliberate choice made on the clash screen.
    """

    carried = {
        key: str(form.get(key) or "").strip()
        for key in ("event_date", "guest_start_time", "guest_end_time", "attendance")
    }
    with session_scope(request.app.state.session_factory) as session:
        inquiry = session.get(BuildingInquiry, inquiry_id)
        if inquiry is None:
            raise HTTPException(status_code=404, detail="Inquiry not found.")
        payload = dict(inquiry.payload_json or {})
        details = {
            key: value for key, value in payload.items() if not str(key).startswith("_")
        }
        corrections = dict(payload.get("_lead_corrections") or {})
        details.update({
            key: value
            for key, value in {
                "guestStartTime": corrections.get("guest_start_time"),
                "guestEndTime": corrections.get("guest_end_time"),
            }.items()
            if str(value or "").strip()
        })
        interview = dict(payload.get("_event_interview") or {})
        details = _details_with_interview_hours(details, interview)
        view = _calendar_view(
            session,
            inquiry=inquiry,
            details=details,
            month="",
            date_choice=carried["event_date"],
        )
    if not view:
        return {}, "No event offering is set up to book against."

    event_date = carried["event_date"] or str(view.get("selected_date") or "")
    if not event_date:
        return {}, (
            "This lead has no requested date, so there is nothing to hold. "
            "Add a date to the lead, then create the contract."
        )
    guest_start = carried["guest_start_time"] or str(view.get("guest_start") or "")
    guest_end = carried["guest_end_time"] or str(view.get("guest_end") or "")
    if not guest_start or not guest_end:
        return {}, (
            "This lead has no event hours, so there is nothing to hold. Add "
            "the hours to the lead, then create the contract."
        )

    raw_attendance = carried["attendance"] or interview.get("attendance") or ""
    attendance = attendance_guess({"attendance": raw_attendance})
    needs_attendance = bool(str(raw_attendance).strip()) and not bool(attendance)
    if not str(raw_attendance).strip():
        attendance = "1"

    cell = next(
        (
            item
            for item in view.get("calendar", {}).get("cells", [])
            if item.get("iso") == event_date
        ),
        {},
    )
    clash = (
        str(cell.get("note") or "the calendar")
        if cell.get("state") in {"pending", "booked", "external"}
        else ""
    )
    return {
        "form": {
            "event_date": event_date,
            "guest_start_time": guest_start,
            "guest_end_time": guest_end,
            "attendance": attendance,
        },
        "clash": clash,
        "needs_attendance": needs_attendance,
        "label": str(view.get("selected_label") or event_date),
    }, ""


def _take_the_date(
    request: Request,
    *,
    inquiry_id: str,
    form: Any,
    actor: str,
    override: bool,
) -> tuple[str, str]:
    """Hold a date for this lead. Returns (reservation_id, error message).

    Shared by the date panel and by creating a contract, so a contract that
    holds its own date goes through exactly the same conflict checks, quote
    freeze, and audit trail as one held by hand.
    """

    with session_scope(request.app.state.session_factory) as session:
        inquiry = session.get(BuildingInquiry, inquiry_id)
        if inquiry is None:
            raise HTTPException(status_code=404, detail="Inquiry not found.")
        # A released hold must not block the next press, or undo would be a
        # one-way door.
        if active_reservation_for(session, inquiry.id) is not None:
            return "", "This lead already has a booking."
        relationship = session.execute(
            select(BuildingRelationship).where(
                BuildingRelationship.source_reference == f"inquiry:{inquiry.id}"
            )
        ).scalars().first()
        if relationship is None:
            return "", "Link or create the customer on this lead first."
        contact_id = relationship.contact_id
        offering = _event_offering(session, str(inquiry.offering_id or ""))
        if offering is None:
            return "", "No event offering exists to book against."
        offering_id, space_id = offering.id, offering.space_id
        pricing = dict((inquiry.payload_json or {}).get("_pricing") or {})
        interview = dict((inquiry.payload_json or {}).get("_event_interview") or {})
        assigned_owner = str(inquiry.assigned_owner or "").strip()

    # The operator picks a day and the guest hours; setup and teardown are the
    # owner-set three-hour buffers, so there is nothing to retype and nothing to
    # put out of order.
    try:
        event_day = date.fromisoformat(str(form.get("event_date") or "").strip())
    except ValueError:
        return "", "Choose a date on the calendar first."
    clock: list[time] = []
    for field in ("guest_start_time", "guest_end_time"):
        raw = str(form.get(field) or "").strip()
        try:
            clock.append(time.fromisoformat(raw))
        except ValueError:
            return "", "Choose when guests arrive and when they leave."
    if clock[0] == clock[1]:
        return "", "Guests cannot arrive and leave at the same time."
    setup, guest_start, guest_end, teardown = access_window(
        event_day, clock[0], clock[1]
    )

    raw_attendance = form.get("attendance") or interview.get("attendance") or ""
    attendance_text = attendance_guess({"attendance": raw_attendance})
    if str(raw_attendance).strip() and not attendance_text:
        return "", (
            "Expected attendance needs a number. Correct Attendance in the "
            "event interview, then try again."
        )
    attendance = int(attendance_text or "1")

    reservation_id = f"event-{uuid4().hex[:12]}"
    hours = max(1, int(pricing.get("hours") or 1))
    try:
        create_event_review(
            EventReviewInput(
                inquiry_id=inquiry_id,
                reservation_id=reservation_id,
                space_id=space_id,
                offering_id=offering_id,
                contact_id=contact_id,
                setup_starts_at=setup.astimezone(timezone.utc),
                guest_starts_at=guest_start.astimezone(timezone.utc),
                guest_ends_at=guest_end.astimezone(timezone.utc),
                teardown_ends_at=teardown.astimezone(timezone.utc),
                hold_expires_at=datetime.now(timezone.utc) + timedelta(days=7),
                attendance=attendance,
                units=hours,
                addons=[],
                terms_summary=(
                    f"{hours} hour booking held from this lead by {actor}."
                ),
                assigned_owner=assigned_owner or actor,
                actor=actor,
                override_conflicts=override,
            ),
            request,
            f"lead-hold-{uuid4().hex[:16]}",
            _internal_api_key(request),
        )
    except (HTTPException, ValidationError, ValueError) as exc:
        return "", str(getattr(exc, "detail", exc))
    return reservation_id, ""


@router.post("/{inquiry_id}/hold-date", dependencies=FORM_DEPS)
async def hold_date_from_lead(
    inquiry_id: str,
    request: Request,
    user: dict = Depends(require_tool("building.events.manage")),
) -> RedirectResponse:
    """Take the date for this lead without leaving it.

    Creates the authoritative access window and its frozen quote through the
    same conflict-checked path the booking workspace uses, so nothing about how
    a hold is made changes — only where it can be started.
    """

    target = f"/admin/building/inquiries/{inquiry_id}"
    form = await request.form()
    override = str(form.get("override_conflicts") or "").strip().lower() in {
        "1", "true", "yes", "on",
    }
    _reservation, error = _take_the_date(
        request,
        inquiry_id=inquiry_id,
        form=form,
        actor=str(user.get("email") or "building-operator"),
        override=override,
    )
    if error:
        return RedirectResponse(
            f"{target}?error={quote_plus(error)}#date-review", status_code=303
        )
    return RedirectResponse(
        f"{target}?notice={quote_plus(('Date double-booked on your authority; held seven days and a quote frozen. Nothing was sent.' if override else 'Date held for seven days and a quote frozen. Nothing was sent.'))}",
        status_code=303,
    )


@router.post("/{inquiry_id}/calendar-sync", dependencies=FORM_DEPS)
def sync_inquiry_calendar(
    inquiry_id: str,
    request: Request,
    confirmation: str = Form(...),
    user: dict = Depends(require_tool("building.manage")),
) -> RedirectResponse:
    """Retry only this event's dedicated-calendar upsert or deletion."""

    target = f"/admin/building/inquiries/{inquiry_id}"
    actor = str(user.get("email") or "building-operator")
    with session_scope(request.app.state.session_factory) as session:
        reservation = session.execute(
            select(BuildingReservation)
            .where(BuildingReservation.inquiry_id == inquiry_id)
            .order_by(BuildingReservation.created_at.desc())
        ).scalars().first()
        if reservation is None:
            raise HTTPException(status_code=404, detail="Event booking not found.")
        expected = f"SYNC {reservation.id}"
        if confirmation.strip() != expected:
            return RedirectResponse(
                f"{target}?error={quote_plus(f'Type {expected} to retry this calendar update.')}#confirmation",
                status_code=303,
            )
        if reservation.status not in {"confirmed", "pre_event", "cancelled", "expired"}:
            return RedirectResponse(
                f"{target}?error=Only+a+confirmed+event+or+cancelled+event+cleanup+can+be+synced.#confirmation",
                status_code=303,
            )
        projection = session.execute(
            select(BuildingCalendarProjection).where(
                BuildingCalendarProjection.reservation_id == reservation.id
            )
        ).scalar_one_or_none()
        if projection is None:
            return RedirectResponse(
                f"{target}?error=No+calendar+projection+exists+for+this+event.#confirmation",
                status_code=303,
            )
        expected_action = (
            "delete" if reservation.status in {"cancelled", "expired"} else "upsert"
        )
        if projection.desired_action != expected_action:
            return RedirectResponse(
                f"{target}?error={quote_plus('The calendar projection does not match the booking state; no provider write was attempted.')}#confirmation",
                status_code=303,
            )
        if projection.status in {"error", "claimed"}:
            before = {"status": projection.status, "last_error": projection.last_error}
            projection.status = "pending"
            projection.claim_token = ""
            projection.claimed_at = None
            projection.next_attempt_at = None
            projection.updated_at = datetime.now(timezone.utc)
            session.add(
                BuildingAuditEvent(
                    entity_type="calendar_projection",
                    entity_id=projection.id,
                    action="calendar_projection_retry_requested",
                    actor=actor,
                    before_json=before,
                    after_json={"status": "pending", "reservation_id": reservation.id},
                )
            )
        reservation_id = reservation.id
    try:
        result = sync_calendar_projections(
            CalendarSyncInput(
                execute=True,
                dry_run=False,
                max_items=1,
                reservation_id=reservation_id,
                actor=actor,
            ),
            request,
            _internal_api_key(request),
        )
    except HTTPException as exc:
        return RedirectResponse(
            f"{target}?error={quote_plus(str(exc.detail))}#confirmation",
            status_code=303,
        )
    delivered = int(result.get("synced_count") or 0)
    return RedirectResponse(
        f"{target}?notice={quote_plus(f'Calendar update completed for this event ({delivered} delivered).')}#confirmation",
        status_code=303,
    )
