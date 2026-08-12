"""Read-only composition for one inquiry-originated Building event journey.

This module creates no new source of truth. It joins the existing governed
records so the inquiry workspace can show one customer from intake through
closeout without copying provider or financial state into the inquiry payload.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select

from sales_support_agent.models.entities import (
    BuildingAgreement,
    BuildingBillingAccount,
    BuildingBillingSchedule,
    BuildingCalendarProjection,
    BuildingDepositEvidence,
    BuildingInvoice,
    BuildingOperationalChecklist,
    BuildingOperationalChecklistItem,
    BuildingProposal,
    BuildingReservation,
    BuildingServiceRequest,
    BuildingSpace,
    BuildingTransactionalMessage,
)
from sales_support_agent.services.building_contracts import load_contract_detail


def _aware(value: datetime | None) -> datetime | None:
    if value is None or value.tzinfo is not None:
        return value
    return value.replace(tzinfo=timezone.utc)


def _item(row: BuildingOperationalChecklistItem) -> dict[str, Any]:
    return {
        "id": row.id,
        "label": row.label,
        "status": row.status,
        "is_required": row.is_required,
        "assigned_owner": row.assigned_owner,
        "due_at": _aware(row.due_at),
        "completion_reason": row.completion_reason,
        "evidence_reference": row.evidence_reference,
    }


def load_event_journey(session: Any, reservation: BuildingReservation) -> dict[str, Any]:
    """Join existing event records for presentation on the inquiry workspace."""

    proposals = session.execute(
        select(BuildingProposal)
        .where(BuildingProposal.reservation_id == reservation.id)
        .order_by(BuildingProposal.version.desc())
    ).scalars().all()
    agreement = session.execute(
        select(BuildingAgreement)
        .where(BuildingAgreement.reservation_id == reservation.id)
        .order_by(BuildingAgreement.version.desc())
    ).scalars().first()
    contract = load_contract_detail(session, agreement.id) if agreement else None
    schedules = session.execute(
        select(BuildingBillingSchedule)
        .where(BuildingBillingSchedule.reservation_id == reservation.id)
        .order_by(BuildingBillingSchedule.starts_on, BuildingBillingSchedule.id)
    ).scalars().all()
    invoices = session.execute(
        select(BuildingInvoice)
        .where(BuildingInvoice.reservation_id == reservation.id)
        .order_by(BuildingInvoice.created_at.desc())
    ).scalars().all()
    account = (
        session.execute(
            select(BuildingBillingAccount)
            .where(BuildingBillingAccount.contact_id == reservation.contact_id)
            .order_by(BuildingBillingAccount.updated_at.desc())
        ).scalars().first()
        if reservation.contact_id
        else None
    )
    calendar = session.execute(
        select(BuildingCalendarProjection).where(
            BuildingCalendarProjection.reservation_id == reservation.id
        )
    ).scalar_one_or_none()
    messages = session.execute(
        select(BuildingTransactionalMessage)
        .where(BuildingTransactionalMessage.reservation_id == reservation.id)
        .order_by(BuildingTransactionalMessage.created_at.desc())
    ).scalars().all()
    checklists = session.execute(
        select(BuildingOperationalChecklist)
        .where(BuildingOperationalChecklist.reservation_id == reservation.id)
        .order_by(BuildingOperationalChecklist.created_at)
    ).scalars().all()
    checklist_ids = [row.id for row in checklists]
    checklist_items = (
        session.execute(
            select(BuildingOperationalChecklistItem)
            .where(BuildingOperationalChecklistItem.checklist_id.in_(checklist_ids))
            .order_by(
                BuildingOperationalChecklistItem.checklist_id,
                BuildingOperationalChecklistItem.sort_order,
            )
        ).scalars().all()
        if checklist_ids
        else []
    )
    items_by_checklist: dict[str, list[dict[str, Any]]] = {}
    for row in checklist_items:
        items_by_checklist.setdefault(row.checklist_id, []).append(_item(row))
    service_requests = session.execute(
        select(BuildingServiceRequest)
        .where(BuildingServiceRequest.reservation_id == reservation.id)
        .order_by(BuildingServiceRequest.created_at.desc())
    ).scalars().all()
    deposit = session.execute(
        select(BuildingDepositEvidence)
        .where(BuildingDepositEvidence.reservation_id == reservation.id)
        .order_by(BuildingDepositEvidence.recorded_at.desc())
    ).scalars().first()
    space = session.get(BuildingSpace, reservation.space_id)

    return {
        "reservation": {
            "id": reservation.id,
            "status": reservation.status,
            "space_id": reservation.space_id,
            "space_name": space.name if space else "Unknown space",
            "starts_at": _aware(reservation.starts_at),
            "ends_at": _aware(reservation.ends_at),
            "guest_starts_at": _aware(reservation.guest_starts_at),
            "guest_ends_at": _aware(reservation.guest_ends_at),
            "hold_expires_at": _aware(reservation.hold_expires_at),
            "attendance": reservation.attendance,
            "agreement_status": reservation.agreement_status,
            "deposit_status": reservation.deposit_status,
            "deposit_required": reservation.deposit_required,
            "assigned_owner": reservation.assigned_owner,
        },
        "quotes": [
            {
                "id": row.id,
                "version": row.version,
                "status": row.status,
                "amount_cents": row.amount_cents,
                "currency": row.currency,
                "checksum": str(
                    (row.rate_plan_snapshot_json or {}).get("quote_checksum") or ""
                ),
            }
            for row in proposals
        ],
        "contract": contract or {},
        "billing": {
            "account": (
                {
                    "id": account.id,
                    "account_name": account.account_name,
                    "billing_email": account.billing_email,
                    "qbo_customer_id": account.qbo_customer_id,
                    "status": account.status,
                }
                if account
                else {}
            ),
            "schedules": [
                {
                    "id": row.id,
                    "component": row.billing_component or row.schedule_type,
                    "description": row.description,
                    "amount_cents": row.amount_cents,
                    "currency": row.currency,
                    "starts_on": row.starts_on,
                    "status": row.status,
                    "quote_checksum": row.source_quote_checksum,
                }
                for row in schedules
            ],
            "invoices": [
                {
                    "id": row.id,
                    "billing_schedule_id": row.billing_schedule_id,
                    "status": row.status,
                    "accounting_status": row.accounting_status,
                    "amount_due_cents": row.amount_due_cents,
                    "amount_paid_cents": row.amount_paid_cents,
                    "currency": row.currency,
                    "qbo_invoice_id": row.qbo_invoice_id,
                    "url": row.hosted_invoice_url,
                    "updated_at": _aware(row.updated_at),
                }
                for row in invoices
            ],
        },
        "deposit_evidence": (
            {
                "status": deposit.status,
                "amount_cents": deposit.amount_cents,
                "provider": deposit.provider,
                "provider_reference": deposit.provider_reference,
                "recorded_at": _aware(deposit.recorded_at),
            }
            if deposit
            else {}
        ),
        "calendar": (
            {
                "id": calendar.id,
                "status": calendar.status,
                "desired_action": calendar.desired_action,
                "provider_event_id": calendar.provider_event_id,
                "target_calendar_id": calendar.target_calendar_id,
                "last_error": calendar.last_error,
                "updated_at": _aware(calendar.updated_at),
            }
            if calendar
            else {}
        ),
        "communications": [
            {
                "id": row.id,
                "milestone": row.milestone,
                "template_version": row.template_version,
                "status": row.status,
                "provider_reference": row.provider_message_id,
                "last_error": row.last_error,
                "sent_at": _aware(row.sent_at),
                "delivered_at": _aware(row.delivered_at),
            }
            for row in messages
        ],
        "checklists": [
            {
                "id": row.id,
                "title": row.title,
                "status": row.status,
                "assigned_owner": row.assigned_owner,
                "due_at": _aware(row.due_at),
                "items": items_by_checklist.get(row.id, []),
            }
            for row in checklists
        ],
        "service_requests": [
            {
                "id": row.id,
                "title": row.title,
                "priority": row.priority,
                "status": row.status,
                "assigned_owner": row.assigned_owner,
                "due_at": _aware(row.due_at),
                "resolution": row.resolution,
            }
            for row in service_requests
        ],
    }


def resolve_event_next_action(data: dict[str, Any]) -> dict[str, Any]:
    """Return the single next action from authoritative joined evidence."""

    journey = dict(data.get("journey") or {})
    reservation = dict(journey.get("reservation") or {})
    if not reservation:
        return {}
    status = str(reservation.get("status") or "")
    if status in {"cancelled", "expired"}:
        return {
            "stage": status,
            "title": f"This event is {status}.",
            "body": "Inventory is not held. Review the activity before reopening or starting a new request.",
            "href": "#activity",
            "label": "Review activity",
            "evidence_state": "blocked",
        }
    if status == "completed":
        return {
            "stage": "completed",
            "title": "This event is closed out.",
            "body": "The customer, financial, calendar, communication, and operational evidence remains available below.",
            "href": "#activity",
            "label": "Review record",
            "evidence_state": "confirmed",
        }
    contract = dict(journey.get("contract") or {})
    if not contract:
        return {
            "stage": "agreement",
            "title": "Create the customer agreement.",
            "body": "The date and event pricing are saved. Freeze them into the approved agreement template; nothing is sent.",
            "href": "#lead-pricing",
            "label": "Review pricing",
            "evidence_state": "ready",
        }
    if contract.get("preparation_status") != "approved" or not contract.get("document_url"):
        return {
            "stage": "agreement",
            "title": "Approve and create the signing copy.",
            "body": "Review the frozen terms, then create the Google Doc. This does not email the customer or request a signature.",
            "href": "#agreement",
            "label": "Review agreement",
            "evidence_state": "needs_review",
        }
    if reservation.get("agreement_status") != "signed":
        return {
            "stage": "signature",
            "title": "Request and verify the customer signature.",
            "body": "Send from Google Docs, then record the signed document and provider evidence here.",
            "href": "#agreement",
            "label": "Finish signature",
            "evidence_state": "needs_review",
        }
    billing = dict(journey.get("billing") or {})
    schedules = list(billing.get("schedules") or [])
    invoices = list(billing.get("invoices") or [])
    if not schedules:
        return {
            "stage": "billing",
            "title": "Prepare the exact billing drafts.",
            "body": "Use the signed agreement and accepted quote. This creates no QuickBooks object and sends nothing.",
            "href": "#billing",
            "label": "Prepare billing",
            "evidence_state": "ready",
        }
    if not invoices:
        return {
            "stage": "invoice",
            "title": "Approve and create the QuickBooks invoice.",
            "body": "Review each frozen charge before creating the provider draft. Nothing is sent automatically.",
            "href": "#billing",
            "label": "Review invoice",
            "evidence_state": "needs_review",
        }
    if reservation.get("deposit_required") and reservation.get("deposit_status") != "paid":
        return {
            "stage": "payment",
            "title": "Verify the required payment.",
            "body": "Refresh QuickBooks evidence. Staff intent is not payment evidence.",
            "href": "#billing",
            "label": "Review payment",
            "evidence_state": "needs_review",
        }
    if status not in {"confirmed", "pre_event"}:
        return {
            "stage": "confirmation",
            "title": "Run the final confirmation gate.",
            "body": "Agent will recheck the signed agreement, cleared required payment, inventory, and Anata Events calendar.",
            "href": "#confirmation",
            "label": "Review confirmation",
            "evidence_state": "ready",
        }
    calendar = dict(journey.get("calendar") or {})
    if calendar.get("status") != "synced":
        return {
            "stage": "calendar",
            "title": "Verify the Anata Events calendar.",
            "body": "The booking is confirmed, but the dedicated-calendar projection is not yet verified as synced.",
            "href": "#confirmation",
            "label": "Review calendar",
            "evidence_state": "blocked" if calendar.get("status") == "error" else "needs_review",
        }
    checklists = list(journey.get("checklists") or [])
    if not checklists or any(row.get("status") != "completed" for row in checklists):
        return {
            "stage": "operations",
            "title": "Complete event operations.",
            "body": "Finish or explicitly waive every required event-day and closeout item with evidence.",
            "href": "#operations",
            "label": "Review operations",
            "evidence_state": "needs_review",
        }
    return {
        "stage": "closeout",
        "title": "Close out the event.",
        "body": "Required operations are complete. Record the final event outcome and preserve the deposit disposition evidence.",
        "href": "#operations",
        "label": "Review closeout",
        "evidence_state": "ready",
    }
