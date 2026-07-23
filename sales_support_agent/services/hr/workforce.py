"""Contractor and offboarding workflows that stay separate from W-2 payroll."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timezone

from sqlalchemy.orm import Session

from sales_support_agent.models.database import get_engine
from sales_support_agent.models.hr import (
    HRAuditEvent,
    HRContractorPayment,
    HREmployee,
    HREmploymentProfile,
    HROffboardingChecklist,
)
from sales_support_agent.models.entities import AppUser
from sales_support_agent.services.hr.store import cents_to_dollars, dollars_to_cents


@contextmanager
def _session():
    session = Session(get_engine(), expire_on_commit=False)
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def _audit(session: Session, actor: str, action: str, entity_type: str,
           entity_id: object, details: dict | None = None) -> None:
    session.add(HRAuditEvent(
        actor_email=(actor or "system").strip().lower(), action=action,
        entity_type=entity_type, entity_id=str(entity_id), details=details or {},
    ))


def create_contractor_payment(
    *, contractor_email: str, service_start: date, service_end: date,
    due_date: date, amount: str, currency: str, description: str,
    invoice_reference: str, actor: str,
) -> tuple[bool, str]:
    email = (contractor_email or "").strip().lower()
    currency = (currency or "USD").strip().upper()
    if service_end < service_start or due_date < service_end:
        return False, "contractor_dates_invalid"
    if len(currency) != 3 or dollars_to_cents(amount) <= 0:
        return False, "contractor_amount_invalid"
    with _session() as session:
        contractor = session.query(HREmployee).filter_by(
            email=email, employee_type="contractor", status="active"
        ).first()
        if not contractor:
            return False, "contractor_not_found"
        row = HRContractorPayment(
            contractor_email=email, service_start=service_start, service_end=service_end,
            due_date=due_date, amount_minor=dollars_to_cents(amount), currency=currency,
            description=(description or "").strip(),
            invoice_reference=(invoice_reference or "").strip(), prepared_by=actor,
        )
        session.add(row)
        session.flush()
        _audit(session, actor, "contractor.payment_prepared", "contractor_payment", row.id)
        return True, "contractor_payment_prepared"


def contractor_payment_action(payment_id: int, *, action: str,
                              wise_reference: str, evidence_note: str,
                              actor: str) -> tuple[bool, str]:
    with _session() as session:
        row = session.get(HRContractorPayment, payment_id)
        if not row:
            return False, "contractor_payment_not_found"
        now = datetime.now(timezone.utc)
        if action == "approve":
            if row.status != "draft":
                return False, "contractor_payment_not_draft"
            if row.prepared_by.strip().lower() == actor.strip().lower():
                return False, "self_approval_blocked"
            row.status, row.approved_by, row.approved_at = "approved", actor, now
        elif action == "record_paid":
            if row.status != "approved" or not wise_reference.strip() or not evidence_note.strip():
                return False, "wise_evidence_required"
            row.status, row.paid_at = "paid", now
            row.wise_transfer_reference = wise_reference.strip()
            row.evidence_note = evidence_note.strip()
        elif action == "reconcile":
            if row.status != "paid" or not evidence_note.strip():
                return False, "wise_evidence_required"
            row.status, row.reconciled_by = "reconciled", actor
            row.evidence_note = evidence_note.strip()
        else:
            return False, "contractor_action_invalid"
        _audit(session, actor, f"contractor.payment_{action}",
               "contractor_payment", row.id, {"wise_reference": row.wise_transfer_reference})
        return True, f"contractor_payment_{row.status}"


def list_contractor_payments() -> list[dict]:
    with _session() as session:
        rows = session.query(HRContractorPayment).order_by(
            HRContractorPayment.due_date.desc(), HRContractorPayment.id.desc()
        ).limit(200).all()
        return [{
            "id": row.id, "contractor_email": row.contractor_email,
            "service_start": row.service_start, "service_end": row.service_end,
            "due_date": row.due_date, "amount": cents_to_dollars(row.amount_minor),
            "currency": row.currency, "description": row.description,
            "invoice_reference": row.invoice_reference, "status": row.status,
            "prepared_by": row.prepared_by, "approved_by": row.approved_by,
            "wise_transfer_reference": row.wise_transfer_reference,
            "evidence_note": row.evidence_note,
        } for row in rows]


def create_offboarding(
    *, employee_email: str, separation_type: str, last_working_day: date,
    final_pay_date: date, reason: str, actor: str,
) -> tuple[bool, str]:
    email = (employee_email or "").strip().lower()
    if separation_type not in {"resignation", "termination", "contract_end"}:
        return False, "offboarding_invalid"
    if final_pay_date < last_working_day:
        return False, "final_pay_date_invalid"
    with _session() as session:
        employee = session.query(HREmployee).filter_by(email=email).first()
        if not employee or employee.status != "active":
            return False, "employee_not_found"
        if session.query(HROffboardingChecklist).filter_by(
            employee_email=email, status="open"
        ).first():
            return False, "offboarding_already_open"
        checklist = {
            "time_reviewed": False, "final_pay_confirmed": False,
            "pto_reviewed": False, "company_property_returned": False,
            "app_access_removed": False, "records_retained": False,
        }
        row = HROffboardingChecklist(
            employee_email=email, separation_type=separation_type,
            last_working_day=last_working_day, final_pay_date=final_pay_date,
            reason=(reason or "").strip(), checklist_json=checklist, created_by=actor,
        )
        session.add(row)
        session.flush()
        _audit(session, actor, "offboarding.started", "offboarding", row.id)
        return True, "offboarding_started"


def update_offboarding(checklist_id: int, *, completed_steps: list[str],
                       actor: str) -> tuple[bool, str]:
    with _session() as session:
        row = session.get(HROffboardingChecklist, checklist_id)
        if not row or row.status != "open":
            return False, "offboarding_not_found"
        checklist = dict(row.checklist_json or {})
        for key in checklist:
            checklist[key] = key in set(completed_steps or [])
        row.checklist_json = checklist
        if all(checklist.values()):
            employee = session.query(HREmployee).filter_by(email=row.employee_email).first()
            employment = session.query(HREmploymentProfile).filter_by(
                employee_email=row.employee_email
            ).first()
            if employee:
                employee.status = "inactive"
            if employment:
                employment.termination_date = row.last_working_day
            app_user = session.query(AppUser).filter_by(email=row.employee_email).first()
            if app_user:
                app_user.status = "suspended"
            row.status, row.completed_by = "complete", actor
            row.completed_at = datetime.now(timezone.utc)
        _audit(session, actor, "offboarding.updated", "offboarding", row.id, checklist)
        return True, "offboarding_complete" if row.status == "complete" else "offboarding_saved"


def list_offboarding() -> list[dict]:
    with _session() as session:
        rows = session.query(HROffboardingChecklist).order_by(
            HROffboardingChecklist.created_at.desc()
        ).limit(100).all()
        return [{
            "id": row.id, "employee_email": row.employee_email,
            "separation_type": row.separation_type,
            "last_working_day": row.last_working_day,
            "final_pay_date": row.final_pay_date, "reason": row.reason,
            "checklist": row.checklist_json or {}, "status": row.status,
        } for row in rows]
