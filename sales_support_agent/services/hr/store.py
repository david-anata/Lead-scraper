"""HR data access — employees, teams, and dashboard counts.

Short-lived ORM Sessions on the shared engine (mirrors access/store.py). Money
is held in integer cents in the DB; helpers convert to/from dollar strings at
the form boundary so the rest of the app deals in dollars.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
import hashlib
import json
import os
import secrets
from typing import Optional
from zoneinfo import ZoneInfo

from sqlalchemy import func
from sqlalchemy.orm import Session

from sales_support_agent.models.database import get_engine
from sales_support_agent.models.hr import (
    HRAuditEvent,
    HRComplianceTask,
    HRCompensationChange,
    HREmployee,
    HREmployeeHandbook,
    HRHandbookAcknowledgement,
    HREmployeeOnboarding,
    HREmploymentProfile,
    HRPTORequest,
    HRPTOLedger,
    HRPayrollRun,
    HRTaxElection,
    HRTeam,
    HRTimeCorrection,
    HRTimeEntry,
    HRTimesheetApproval,
    HRTaxLiability,
    HRContractorPayment,
)
from sales_support_agent.services.access import store as access_store
from sales_support_agent.services.token_seal import seal_token


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


# --- money helpers (dollars <-> cents) -------------------------------------

def dollars_to_cents(value) -> int:
    try:
        return int(round(float(str(value).replace("$", "").replace(",", "").strip() or 0) * 100))
    except (TypeError, ValueError):
        return 0


def cents_to_dollars(cents: Optional[int]) -> str:
    return f"{(cents or 0) / 100:,.2f}"


HR_ROLES = ("employee", "manager", "owner", "admin")
EMPLOYEE_TYPES = ("hourly", "salaried", "contractor")
ANATA_TIMEZONE = ZoneInfo("America/Denver")
PTO_ANNUAL_HOURS = 40.0
PTO_ACCRUAL_DIVISOR = 52.0
HR_EMPLOYEE_ROLE_NAME = "HR Employee"
CURRENT_POLICY_VERSION = "2026.1"
ANATA_PAID_HOLIDAYS = (
    "New Year's Day",
    "Memorial Day",
    "Independence Day",
    "Labor Day",
    "Thanksgiving",
    "Christmas Day",
)


def _audit(session: Session, actor: str, action: str, entity_type: str,
           entity_id: object = "", details: Optional[dict] = None) -> None:
    session.add(HRAuditEvent(actor_email=(actor or "system").strip().lower(), action=action,
                             entity_type=entity_type, entity_id=str(entity_id or ""),
                             details=details or {}))


def _supersede_open_payrolls(session: Session, *, actor: str,
                             effective_start: date,
                             effective_end: Optional[date] = None,
                             reason: str) -> None:
    effective_end = effective_end or effective_start
    runs = session.query(HRPayrollRun).filter(
        HRPayrollRun.status.in_(("prepared", "approved")),
        HRPayrollRun.pay_period_start <= effective_end,
        HRPayrollRun.pay_period_end >= effective_start,
    ).all()
    for run in runs:
        run.status = "superseded"
        _audit(session, actor, "payroll.superseded", "payroll_run", run.base44_id, {
            "reason": reason,
        })


def current_policy(employee_email: str) -> dict:
    email = (employee_email or "").strip().lower()
    with _session() as session:
        row = session.query(HREmployeeHandbook).filter_by(
            version=CURRENT_POLICY_VERSION, is_active=True
        ).first()
        if not row:
            row = HREmployeeHandbook(
                base44_id=f"policy-{CURRENT_POLICY_VERSION}",
                title="Anata Employee Operating Policies",
                file_url="/admin/hr/policies", version=CURRENT_POLICY_VERSION,
                uploaded_by="system", is_active=True,
            )
            session.add(row)
            session.flush()
        acknowledged = session.query(HRHandbookAcknowledgement).filter_by(
            handbook_id=row.base44_id, employee_email=email
        ).first()
        return {
            "id": row.base44_id, "title": row.title, "version": row.version,
            "acknowledged": bool(acknowledged),
            "acknowledged_at": acknowledged.acknowledged_at if acknowledged else None,
        }


def acknowledge_current_policy(employee_email: str, *, actor: str,
                               attested: bool) -> tuple[bool, str]:
    email = (employee_email or "").strip().lower()
    if not attested or actor.strip().lower() != email:
        return False, "attestation_required"
    policy = current_policy(email)
    with _session() as session:
        if session.query(HRHandbookAcknowledgement).filter_by(
            handbook_id=policy["id"], employee_email=email
        ).first():
            return True, "policy_already_acknowledged"
        employee = session.query(HREmployee).filter_by(email=email).first()
        if not employee:
            return False, "employee_not_found"
        row = HRHandbookAcknowledgement(
            base44_id=f"ack_{secrets.token_hex(12)}", handbook_id=policy["id"],
            employee_email=email, employee_name=employee.full_name,
            acknowledged_at=datetime.now(timezone.utc),
        )
        session.add(row)
        session.flush()
        _audit(session, actor, "policy.acknowledged", "handbook", policy["id"], {
            "version": policy["version"],
        })
        return True, "policy_acknowledged"


# --- employees -------------------------------------------------------------

def _emp_dict(e: HREmployee) -> dict:
    return {
        "id": e.id,
        "email": e.email,
        "full_name": e.full_name or e.email,
        "hr_role": e.hr_role,
        "team_id": e.team_id,
        "status": e.status,
        "employee_type": e.employee_type,
        "hourly_rate": cents_to_dollars(e.hourly_rate_cents),
        "hourly_rate_cents": e.hourly_rate_cents,
        "annual_salary": cents_to_dollars(e.annual_salary_cents),
        "annual_salary_cents": e.annual_salary_cents,
        "phone": e.phone,
        "address_line1": e.address_line1,
        "address_line2": e.address_line2,
        "city": e.city,
        "state": e.state,
        "zip_code": e.zip,
        "onboarding_complete": bool(e.onboarding_complete),
    }


def _employment_dict(row: Optional[HREmploymentProfile]) -> dict:
    if not row:
        return {}
    return {
        "id": row.id, "employee_email": row.employee_email, "hire_date": row.hire_date,
        "termination_date": row.termination_date, "title": row.title,
        "manager_email": row.manager_email, "work_state": row.work_state,
        "classification": row.classification, "pay_basis": row.pay_basis,
        "fixed_pay_per_period": cents_to_dollars(row.fixed_pay_per_period_cents),
        "fixed_pay_per_period_cents": row.fixed_pay_per_period_cents,
        "standard_weekly_hours": float(row.standard_weekly_hours or 0),
        "standard_period_hours": float(row.standard_period_hours or 0),
        "pto_eligible_date": row.pto_eligible_date,
        "holiday_eligible_date": row.holiday_eligible_date,
    }


def list_employees(*, include_inactive: bool = True) -> list:
    with _session() as s:
        q = s.query(HREmployee)
        if not include_inactive:
            q = q.filter(HREmployee.status == "active")
        rows = q.order_by(HREmployee.full_name.asc(), HREmployee.email.asc()).all()
        employment_by_email = {
            row.employee_email: row for row in s.query(HREmploymentProfile).all()
        }
        results = []
        for employee in rows:
            item = _emp_dict(employee)
            item["employment"] = _employment_dict(employment_by_email.get(employee.email))
            results.append(item)
        return results


def get_employee(emp_id: int) -> Optional[dict]:
    with _session() as s:
        e = s.get(HREmployee, emp_id)
        if not e:
            return None
        result = _emp_dict(e)
        result["employment"] = _employment_dict(
            s.query(HREmploymentProfile).filter_by(employee_email=e.email).first()
        )
        return result


def get_employee_by_email(email: str) -> Optional[dict]:
    email = (email or "").strip().lower()
    with _session() as s:
        e = s.query(HREmployee).filter(HREmployee.email == email).first()
        return _emp_dict(e) if e else None


def create_employee(*, email: str, full_name: str = "", hr_role: str = "employee",
                    employee_type: str = "hourly", team_id: Optional[str] = None,
                    hourly_rate: str = "0", annual_salary: str = "0",
                    phone: str = "", status: str = "active", actor: str = "system") -> Optional[int]:
    email = (email or "").strip().lower()
    if not email:
        return None
    with _session() as s:
        if s.query(HREmployee).filter(HREmployee.email == email).first():
            return None  # already exists
        e = HREmployee(
            email=email,
            full_name=full_name.strip(),
            hr_role=hr_role if hr_role in HR_ROLES else "employee",
            employee_type=employee_type if employee_type in EMPLOYEE_TYPES else "hourly",
            team_id=team_id or None,
            hourly_rate_cents=dollars_to_cents(hourly_rate),
            annual_salary_cents=dollars_to_cents(annual_salary),
            phone=phone.strip(),
            status="active" if status != "inactive" else "inactive",
        )
        s.add(e)
        s.flush()
        _audit(s, actor, "employee.created", "employee", e.id, {"email": email})
        return e.id


def update_employee(emp_id: int, *, actor: str = "system", **fields) -> bool:
    with _session() as s:
        e = s.get(HREmployee, emp_id)
        if not e:
            return False
        if "full_name" in fields:
            e.full_name = (fields["full_name"] or "").strip()
        if "hr_role" in fields and fields["hr_role"] in HR_ROLES:
            e.hr_role = fields["hr_role"]
        if "employee_type" in fields and fields["employee_type"] in EMPLOYEE_TYPES:
            e.employee_type = fields["employee_type"]
        if "team_id" in fields:
            e.team_id = fields["team_id"] or None
        if "hourly_rate" in fields:
            e.hourly_rate_cents = dollars_to_cents(fields["hourly_rate"])
        if "annual_salary" in fields:
            e.annual_salary_cents = dollars_to_cents(fields["annual_salary"])
        if "phone" in fields:
            e.phone = (fields["phone"] or "").strip()
        if "status" in fields:
            e.status = "inactive" if fields["status"] == "inactive" else "active"
        e.updated_at = datetime.utcnow()
        _audit(s, actor, "employee.updated", "employee", emp_id,
               {"fields": sorted(k for k in fields if k not in {"ssn", "bank_account"})})
        return True


def record_compensation_change(
    employee_email: str, *, effective_date: date, prior: dict, new: dict,
    reason: str, actor: str,
) -> tuple[bool, str]:
    """Append a deliberate pay change after the employee update succeeds."""
    if not effective_date or not reason.strip() or prior == new:
        return False, "compensation_change_invalid"
    with _session() as session:
        row = HRCompensationChange(
            employee_email=employee_email.strip().lower(),
            effective_date=effective_date,
            prior_json=prior,
            new_json=new,
            reason=reason.strip(),
            changed_by=actor.strip().lower(),
        )
        session.add(row)
        session.flush()
        _audit(session, actor, "compensation.changed", "compensation_change", row.id, {
            "employee_email": row.employee_email,
            "effective_date": effective_date.isoformat(),
            "changed_fields": sorted(
                key for key in set(prior) | set(new)
                if prior.get(key) != new.get(key)
            ),
        })
    return True, "compensation_change_recorded"


def list_compensation_changes(employee_email: str) -> list[dict]:
    with _session() as session:
        rows = session.query(HRCompensationChange).filter_by(
            employee_email=employee_email.strip().lower()
        ).order_by(
            HRCompensationChange.effective_date.desc(),
            HRCompensationChange.id.desc(),
        ).all()
        return [{
            "id": row.id, "effective_date": row.effective_date,
            "prior": dict(row.prior_json or {}), "new": dict(row.new_json or {}),
            "reason": row.reason, "changed_by": row.changed_by,
            "created_at": row.created_at,
        } for row in rows]


def upsert_employment_profile(employee_email: str, *, hire_date: Optional[date],
                              title: str = "", manager_email: str = "",
                              classification: str = "nonexempt",
                              pay_basis: str = "hourly",
                              fixed_pay_per_period: str = "0",
                              standard_weekly_hours: float = 40,
                              standard_period_hours: float = 86.67,
                              actor: str = "system") -> bool:
    """Create/update employer-owned employment facts and eligibility dates."""
    email = (employee_email or "").strip().lower()
    if classification not in {"exempt", "nonexempt"}:
        return False
    if pay_basis not in {"hourly", "fixed_semimonthly"}:
        return False
    with _session() as s:
        employee = s.query(HREmployee).filter_by(email=email).first()
        if not employee:
            return False
        row = s.query(HREmploymentProfile).filter_by(employee_email=email).first()
        if not row:
            row = HREmploymentProfile(employee_email=email)
            s.add(row)
        row.hire_date = hire_date
        row.title = (title or "").strip()
        row.manager_email = (manager_email or "").strip().lower()
        row.work_state = "UT"
        row.classification = classification
        row.pay_basis = pay_basis
        row.fixed_pay_per_period_cents = dollars_to_cents(fixed_pay_per_period)
        row.standard_weekly_hours = Decimal(str(max(0, standard_weekly_hours)))
        row.standard_period_hours = Decimal(str(max(0, standard_period_hours)))
        row.pto_eligible_date = hire_date + timedelta(days=90) if hire_date else None
        row.holiday_eligible_date = row.pto_eligible_date
        row.updated_by = actor
        row.updated_at = datetime.now(timezone.utc)
        if hire_date and employee.employee_type != "contractor":
            compliance = s.query(HRComplianceTask).filter_by(
                employee_email=email, task_type="utah_new_hire_report"
            ).first()
            if not compliance:
                compliance = HRComplianceTask(
                    employee_email=email, task_type="utah_new_hire_report",
                    due_date=hire_date + timedelta(days=20),
                )
                s.add(compliance)
        _supersede_open_payrolls(
            s, actor=actor, effective_start=date.today(),
            reason="employment profile changed",
        )
        employee.employee_type = "salaried" if pay_basis == "fixed_semimonthly" else "hourly"
        _audit(s, actor, "employment.updated", "employee", employee.id, {
            "hire_date": hire_date.isoformat() if hire_date else None,
            "classification": classification, "pay_basis": pay_basis,
        })
        return True


def list_compliance_tasks() -> list[dict]:
    """List filing tasks and safely backfill Utah new-hire tasks."""
    with _session() as session:
        existing = {
            row[0] for row in session.query(HRComplianceTask.employee_email).filter_by(
                task_type="utah_new_hire_report"
            ).all()
        }
        profiles = session.query(HREmploymentProfile).all()
        for profile in profiles:
            employee = session.query(HREmployee).filter_by(
                email=profile.employee_email
            ).first()
            if (
                profile.hire_date and employee and employee.employee_type != "contractor"
                and profile.employee_email not in existing
            ):
                session.add(HRComplianceTask(
                    employee_email=profile.employee_email,
                    task_type="utah_new_hire_report",
                    due_date=profile.hire_date + timedelta(days=20),
                ))
        session.flush()
        rows = session.query(HRComplianceTask).order_by(
            HRComplianceTask.status, HRComplianceTask.due_date,
            HRComplianceTask.employee_email,
        ).all()
        return [{
            "id": row.id, "employee_email": row.employee_email,
            "task_type": row.task_type, "due_date": row.due_date,
            "status": row.status,
            "confirmation_reference": row.confirmation_reference,
            "evidence_note": row.evidence_note,
            "completed_by": row.completed_by,
            "completed_at": row.completed_at,
            "overdue": row.status != "confirmed" and row.due_date < date.today(),
        } for row in rows]


def ensure_annual_compliance_tasks(year: int) -> None:
    """Create the known filing checklist; a reviewer still confirms actual deadlines."""
    def next_weekday(value: date) -> date:
        while value.weekday() >= 5:
            value += timedelta(days=1)
        return value

    quarterly_due = (
        next_weekday(date(year, 4, 30)),
        next_weekday(date(year, 7, 31)),
        next_weekday(date(year, 10, 31)),
        next_weekday(date(year + 1, 1, 31)),
    )
    tasks: list[tuple[str, date]] = []
    for quarter, due_date in enumerate(quarterly_due, start=1):
        tasks.extend([
            (f"federal_941_{year}_q{quarter}", due_date),
            (f"utah_tc941e_{year}_q{quarter}", due_date),
            (f"utah_ui_wage_report_{year}_q{quarter}", due_date),
        ])
    annual_due = next_weekday(date(year + 1, 1, 31))
    tasks.extend([
        (f"federal_940_{year}", annual_due),
        (f"federal_w2_w3_{year}", annual_due),
        (f"utah_annual_reconciliation_{year}", annual_due),
    ])
    with _session() as session:
        existing = {
            row[0] for row in session.query(HRComplianceTask.task_type).filter(
                HRComplianceTask.task_type.in_([task[0] for task in tasks])
            ).all()
        }
        for task_type, due_date in tasks:
            if task_type not in existing:
                session.add(HRComplianceTask(
                    employee_email="", task_type=task_type, due_date=due_date
                ))


def record_compliance_task(
    task_id: int, *, action: str, confirmation_reference: str,
    evidence_note: str, actor: str
) -> tuple[bool, str]:
    """Confirm or reopen a compliance task without deleting its history."""
    if action not in {"confirmed", "reopened"} or not evidence_note.strip():
        return False, "compliance_evidence_required"
    if action == "confirmed" and not confirmation_reference.strip():
        return False, "compliance_confirmation_required"
    with _session() as session:
        row = session.get(HRComplianceTask, task_id)
        if not row:
            return False, "compliance_task_not_found"
        row.status = "confirmed" if action == "confirmed" else "open"
        row.confirmation_reference = (
            confirmation_reference.strip() if action == "confirmed" else ""
        )
        row.evidence_note = evidence_note.strip()
        row.completed_by = actor.strip().lower() if action == "confirmed" else ""
        row.completed_at = (
            datetime.now(timezone.utc) if action == "confirmed" else None
        )
        _audit(session, actor, f"compliance.{action}", "compliance_task", row.id, {
            "task_type": row.task_type, "employee_email": row.employee_email,
            "due_date": str(row.due_date),
        })
        return True, f"compliance_{action}"


def get_employment_profile(employee_email: str) -> dict:
    email = (employee_email or "").strip().lower()
    with _session() as s:
        return _employment_dict(
            s.query(HREmploymentProfile).filter_by(employee_email=email).first()
        )


def _ensure_employee_access_role() -> str:
    for role in access_store.list_roles():
        if role.get("name") == HR_EMPLOYEE_ROLE_NAME:
            return role["id"]
    return access_store.create_role(
        HR_EMPLOYEE_ROLE_NAME, ["hr.access"],
        description="Employee self-service access to the employee's own HR record.",
    )


def create_employee_invitation(employee_email: str, *, actor: str,
                               expires_days: int = 7) -> dict:
    """Create the existing secure app invite plus HR onboarding progress."""
    email = (employee_email or "").strip().lower()
    with _session() as s:
        employee = s.query(HREmployee).filter_by(email=email).first()
        if not employee:
            return {"ok": False, "error": "employee_not_found"}
    token = secrets.token_urlsafe(32)
    role_id = _ensure_employee_access_role()
    expires_at = datetime.now(timezone.utc) + timedelta(days=max(1, min(expires_days, 30)))
    invite_id = access_store.create_invite(
        email, role_id, token=token, invited_by=actor, expires_at=expires_at
    )
    with _session() as s:
        row = s.query(HREmployeeOnboarding).filter_by(employee_email=email).first()
        if not row:
            row = HREmployeeOnboarding(employee_email=email)
            s.add(row)
        row.app_invite_id = invite_id
        row.status = "sent"
        row.updated_at = datetime.now(timezone.utc)
        _audit(s, actor, "onboarding.invited", "employee", email, {"invite_id": invite_id})
    return {"ok": True, "token": token, "invite_id": invite_id, "expires_at": expires_at}


def get_onboarding(employee_email: str) -> dict:
    email = (employee_email or "").strip().lower()
    with _session() as s:
        row = s.query(HREmployeeOnboarding).filter_by(employee_email=email).first()
        if not row:
            return {"employee_email": email, "status": "not_started"}
        return {
            "id": row.id, "employee_email": row.employee_email, "status": row.status,
            "profile_complete": row.profile_complete, "w4_complete": row.w4_complete,
            "i9_employee_complete": row.i9_employee_complete,
            "i9_employer_complete": row.i9_employer_complete,
            "policies_complete": row.policies_complete,
            "emergency_contact_name": row.emergency_contact_name,
            "emergency_contact_relationship": row.emergency_contact_relationship,
            "emergency_contact_phone": row.emergency_contact_phone,
            "emergency_contact_email": row.emergency_contact_email,
            "correction_reason": row.correction_reason,
        }


def get_current_tax_election(employee_email: str) -> Optional[dict]:
    """Return only the safe-to-display fields from the employee's current W-4."""
    email = (employee_email or "").strip().lower()
    with _session() as s:
        row = (
            s.query(HRTaxElection)
            .filter_by(employee_email=email, superseded_at=None)
            .order_by(HRTaxElection.effective_date.desc(), HRTaxElection.id.desc())
            .first()
        )
        if not row:
            return None
        return {
            "effective_date": row.effective_date.isoformat(),
            "filing_status": row.filing_status,
            "two_jobs": bool(row.two_jobs),
            "exempt_from_federal_withholding": bool(
                row.exempt_from_federal_withholding
            ),
            "dependents_credit": cents_to_dollars(row.dependents_credit_cents),
            "other_income": cents_to_dollars(row.other_income_cents),
            "deductions": cents_to_dollars(row.deductions_cents),
            "extra_withholding": cents_to_dollars(row.extra_withholding_cents),
            "ssn_last4": row.ssn_last4,
        }


def request_onboarding_correction(employee_email: str, *, reason: str,
                                  actor: str) -> tuple[bool, str]:
    """Return onboarding to the employee without erasing their signed history."""
    email = (employee_email or "").strip().lower()
    reason = (reason or "").strip()
    if not reason:
        return False, "correction_reason_required"
    with _session() as s:
        employee = s.query(HREmployee).filter_by(email=email).first()
        row = s.query(HREmployeeOnboarding).filter_by(employee_email=email).first()
        if not employee or not row:
            return False, "onboarding_incomplete"
        row.status = "correction_requested"
        row.correction_reason = reason
        row.updated_at = datetime.now(timezone.utc)
        _audit(s, actor, "onboarding.correction_requested", "employee", employee.id, {
            "reason": reason,
        })
        return True, "onboarding_correction_requested"


def save_employee_profile(employee_email: str, *, phone: str, address_line1: str,
                          address_line2: str, city: str, state: str, zip_code: str,
                          emergency_name: str, emergency_relationship: str,
                          emergency_phone: str, emergency_email: str,
                          actor: str) -> tuple[bool, str]:
    email = (employee_email or "").strip().lower()
    with _session() as s:
        employee = s.query(HREmployee).filter_by(email=email).first()
        if not employee:
            return False, "employee_not_found"
        employee.phone = (phone or "").strip()
        employee.address_line1 = (address_line1 or "").strip()
        employee.address_line2 = (address_line2 or "").strip()
        employee.city = (city or "").strip()
        employee.state = (state or "").strip().upper()
        employee.zip = (zip_code or "").strip()
        row = s.query(HREmployeeOnboarding).filter_by(employee_email=email).first()
        if not row:
            row = HREmployeeOnboarding(employee_email=email, status="employee_in_progress")
            s.add(row)
        row.emergency_contact_name = (emergency_name or "").strip()
        row.emergency_contact_relationship = (emergency_relationship or "").strip()
        row.emergency_contact_phone = (emergency_phone or "").strip()
        row.emergency_contact_email = (emergency_email or "").strip().lower()
        row.profile_complete = all([
            employee.address_line1, employee.city, employee.state, employee.zip,
            row.emergency_contact_name, row.emergency_contact_phone,
        ])
        row.status = "employee_in_progress"
        row.correction_reason = ""
        row.updated_at = datetime.now(timezone.utc)
        _audit(s, actor, "onboarding.profile_saved", "employee", employee.id,
               {"complete": row.profile_complete})
        return True, "profile_saved"


def save_w4(employee_email: str, *, ssn: str, filing_status: str, two_jobs: bool,
            dependents_credit: str, other_income: str, deductions: str,
            extra_withholding: str, exempt: bool, attested: bool,
            actor: str) -> tuple[bool, str]:
    """Seal the SSN and save an immutable employee-attested W-4 snapshot."""
    email = (employee_email or "").strip().lower()
    digits = "".join(ch for ch in (ssn or "") if ch.isdigit())
    if len(digits) != 9 or filing_status not in {"single", "married_joint", "head_household"}:
        return False, "invalid_w4"
    if not attested or actor.strip().lower() != email:
        return False, "attestation_required"
    secret = (os.getenv("HR_PII_SECRET") or "").strip()
    if not secret:
        return False, "pii_secret_missing"
    payload = {
        "employee_email": email, "effective_date": date.today().isoformat(),
        "filing_status": filing_status, "two_jobs": bool(two_jobs),
        "exempt_from_federal_withholding": bool(exempt),
        "dependents_credit_cents": dollars_to_cents(dependents_credit),
        "other_income_cents": dollars_to_cents(other_income),
        "deductions_cents": dollars_to_cents(deductions),
        "extra_withholding_cents": dollars_to_cents(extra_withholding),
        "ssn_last4": digits[-4:],
    }
    snapshot_hash = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    with _session() as s:
        if not s.query(HREmployee).filter_by(email=email).first():
            return False, "employee_not_found"
        existing = s.query(HRTaxElection).filter_by(snapshot_hash=snapshot_hash).first()
        if existing and existing.employee_email == email:
            onboarding = s.query(HREmployeeOnboarding).filter_by(
                employee_email=email
            ).first()
            if onboarding:
                onboarding.w4_complete = True
                onboarding.updated_at = datetime.now(timezone.utc)
            _audit(s, actor, "onboarding.w4_reconfirmed", "employee", email, {
                "effective_date": payload["effective_date"], "ssn_last4": digits[-4:],
            })
            return True, "w4_saved"
        previous = s.query(HRTaxElection).filter_by(
            employee_email=email, superseded_at=None
        ).all()
        now = datetime.now(timezone.utc)
        for row in previous:
            row.superseded_at = now
        election = HRTaxElection(
            employee_email=email, effective_date=date.today(),
            filing_status=filing_status, two_jobs=bool(two_jobs),
            exempt_from_federal_withholding=bool(exempt),
            dependents_credit_cents=payload["dependents_credit_cents"],
            other_income_cents=payload["other_income_cents"],
            deductions_cents=payload["deductions_cents"],
            extra_withholding_cents=payload["extra_withholding_cents"],
            sealed_ssn=seal_token(secret, digits), ssn_last4=digits[-4:],
            attested_by=email, attested_at=now, snapshot_hash=snapshot_hash,
        )
        s.add(election)
        _supersede_open_payrolls(
            s, actor=actor, effective_start=election.effective_date,
            reason="W-4 election changed",
        )
        onboarding = s.query(HREmployeeOnboarding).filter_by(employee_email=email).first()
        if not onboarding:
            onboarding = HREmployeeOnboarding(employee_email=email)
            s.add(onboarding)
        onboarding.w4_complete = True
        onboarding.status = "employee_in_progress"
        onboarding.updated_at = now
        _audit(s, actor, "onboarding.w4_attested", "employee", email,
               {"effective_date": payload["effective_date"], "ssn_last4": digits[-4:]})
        return True, "w4_saved"


def save_employee_attestations(employee_email: str, *, i9_attested: bool,
                               policies_attested: bool, actor: str) -> tuple[bool, str]:
    email = (employee_email or "").strip().lower()
    if actor.strip().lower() != email:
        return False, "attestation_required"
    with _session() as s:
        if not s.query(HREmployee).filter_by(email=email).first():
            return False, "employee_not_found"
        row = s.query(HREmployeeOnboarding).filter_by(employee_email=email).first()
        if not row:
            row = HREmployeeOnboarding(employee_email=email)
            s.add(row)
        row.i9_employee_complete = bool(i9_attested)
        row.policies_complete = bool(policies_attested)
        row.status = "employer_review" if (
            row.profile_complete and row.w4_complete
            and row.i9_employee_complete and row.policies_complete
        ) else "employee_in_progress"
        row.updated_at = datetime.now(timezone.utc)
        _audit(s, actor, "onboarding.employee_attested", "employee", email, {
            "i9_employee_complete": row.i9_employee_complete,
            "policies_complete": row.policies_complete,
        })
        return True, "attestations_saved"


def complete_employer_onboarding(employee_email: str, *, i9_document_type: str,
                                 i9_verified_date: date, i9_expiration_date: Optional[date],
                                 actor: str) -> tuple[bool, str]:
    email = (employee_email or "").strip().lower()
    if not i9_document_type.strip():
        return False, "i9_incomplete"
    with _session() as s:
        employee = s.query(HREmployee).filter_by(email=email).first()
        onboarding = s.query(HREmployeeOnboarding).filter_by(employee_email=email).first()
        employment = s.query(HREmploymentProfile).filter_by(employee_email=email).first()
        if not employee or not onboarding or not employment:
            return False, "onboarding_incomplete"
        employee.i9_document_type = i9_document_type.strip()
        employee.i9_verified_date = i9_verified_date
        employee.i9_expiration_date = i9_expiration_date
        employee.i9_status = "verified"
        onboarding.i9_employer_complete = True
        required = (
            onboarding.profile_complete and onboarding.w4_complete
            and onboarding.i9_employee_complete and onboarding.policies_complete
            and employment.hire_date is not None
        )
        onboarding.status = "complete" if required else "employer_review"
        onboarding.completed_at = datetime.now(timezone.utc) if required else None
        onboarding.updated_at = datetime.now(timezone.utc)
        employee.onboarding_complete = required
        _audit(s, actor, "onboarding.employer_reviewed", "employee", employee.id, {
            "complete": required, "i9_document_type": i9_document_type.strip(),
        })
        return True, "onboarding_complete" if required else "onboarding_incomplete"


# --- teams -----------------------------------------------------------------

def _team_dict(t: HRTeam) -> dict:
    return {"id": t.id, "name": t.name, "manager_email": t.manager_email,
            "description": t.description}


def list_teams() -> list:
    with _session() as s:
        rows = s.query(HRTeam).order_by(HRTeam.name.asc()).all()
        return [_team_dict(t) for t in rows]


def create_team(*, name: str, manager_email: str = "", description: str = "") -> Optional[int]:
    name = (name or "").strip()
    if not name:
        return None
    with _session() as s:
        t = HRTeam(name=name, manager_email=(manager_email or "").strip().lower(),
                   description=(description or "").strip())
        s.add(t)
        s.flush()
        return t.id


# --- dashboard -------------------------------------------------------------

def dashboard_stats() -> dict:
    with _session() as s:
        total = s.query(func.count(HREmployee.id)).scalar() or 0
        active = s.query(func.count(HREmployee.id)).filter(HREmployee.status == "active").scalar() or 0
        teams = s.query(func.count(HRTeam.id)).scalar() or 0
        onboarding = (s.query(func.count(HREmployee.id))
                      .filter(HREmployee.status == "active", HREmployee.onboarding_complete.is_(False))
                      .scalar() or 0)
        attention = []
        correction_count = s.query(func.count(HRTimeCorrection.id)).filter_by(
            status="requested"
        ).scalar() or 0
        pto_count = s.query(func.count(HRPTORequest.id)).filter_by(
            status="pending"
        ).scalar() or 0
        overdue_tax = s.query(func.count(HRTaxLiability.id)).filter(
            HRTaxLiability.status != "reconciled",
            HRTaxLiability.due_date <= datetime.now(ANATA_TIMEZONE).date(),
        ).scalar() or 0
        contractor_due = s.query(func.count(HRContractorPayment.id)).filter(
            HRContractorPayment.status.in_(("draft", "approved")),
            HRContractorPayment.due_date <= datetime.now(ANATA_TIMEZONE).date(),
        ).scalar() or 0
        for count, label, url in (
            (onboarding, "employee onboarding record(s) incomplete", "/admin/hr/employees"),
            (correction_count, "time correction(s) need review", "/admin/hr/time"),
            (pto_count, "PTO request(s) need review", "/admin/hr/time"),
            (overdue_tax, "tax liability item(s) due or overdue", "/admin/hr/payroll"),
            (contractor_due, "contractor payment(s) due", "/admin/hr/contractors"),
        ):
            if count:
                attention.append({"count": count, "label": label, "url": url})
        return {"total_employees": total, "active_employees": active,
                "teams": teams, "onboarding_incomplete": onboarding,
                "attention": attention}


def employee_dashboard_stats(employee_email: str) -> dict:
    """Return only the signed-in employee's own HR status."""
    email = (employee_email or "").strip().lower()
    onboarding = get_onboarding(email)
    pto = pto_summary(email)
    with _session() as s:
        pending_corrections = s.query(func.count(HRTimeCorrection.id)).filter_by(
            employee_email=email, status="requested"
        ).scalar() or 0
        pending_pto = s.query(func.count(HRPTORequest.id)).filter_by(
            employee_email=email, status="pending"
        ).scalar() or 0
    return {
        "onboarding_complete": onboarding.get("status") == "complete",
        "onboarding_steps_complete": sum(bool(onboarding.get(key)) for key in (
            "profile_complete", "w4_complete", "i9_employee_complete",
            "i9_employer_complete", "policies_complete",
        )),
        "pto_available": pto.get("available", 0),
        "pending_pto": pending_pto,
        "pending_corrections": pending_corrections,
    }


# --- paid holiday calendar -------------------------------------------------

def _nth_weekday(year: int, month: int, weekday: int, occurrence: int) -> date:
    first = date(year, month, 1)
    offset = (weekday - first.weekday()) % 7
    return first + timedelta(days=offset + (occurrence - 1) * 7)


def _last_weekday(year: int, month: int, weekday: int) -> date:
    if month == 12:
        cursor = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        cursor = date(year, month + 1, 1) - timedelta(days=1)
    return cursor - timedelta(days=(cursor.weekday() - weekday) % 7)


def observed_pay_date(actual_date: date) -> date:
    """Apply Anata's weekend rule: Saturday Friday, Sunday Monday."""
    if actual_date.weekday() == 5:
        return actual_date - timedelta(days=1)
    if actual_date.weekday() == 6:
        return actual_date + timedelta(days=1)
    return actual_date


def paid_holidays(year: int) -> list[dict]:
    """Generate the approved six-day holiday calendar for a year."""
    dates = (
        ("New Year's Day", date(year, 1, 1)),
        ("Memorial Day", _last_weekday(year, 5, 0)),
        ("Independence Day", date(year, 7, 4)),
        ("Labor Day", _nth_weekday(year, 9, 0, 1)),
        ("Thanksgiving", _nth_weekday(year, 11, 3, 4)),
        ("Christmas Day", date(year, 12, 25)),
    )
    return [
        {"name": name, "actual_date": actual, "observed_date": observed_pay_date(actual)}
        for name, actual in dates
    ]


def holiday_pay_proposals(employee_email: str, start_date: date, end_date: date) -> list[dict]:
    """Propose eligible holiday hours; payroll remains responsible for approval."""
    email = (employee_email or "").strip().lower()
    with _session() as s:
        employee = s.query(HREmployee).filter_by(email=email).first()
        employment = s.query(HREmploymentProfile).filter_by(employee_email=email).first()
        if not employee or not employment or employee.employee_type == "contractor":
            return []
        eligible_date = employment.holiday_eligible_date
        if not eligible_date:
            return []
        years = range(start_date.year, end_date.year + 1)
        proposals = []
        for year in years:
            for holiday in paid_holidays(year):
                observed = holiday["observed_date"]
                if not (start_date <= observed <= end_date) or observed < eligible_date:
                    continue
                hours = 0.0
                if employment.pay_basis == "hourly":
                    hours = min(8.0, float(employment.standard_weekly_hours or 0) / 5)
                proposals.append({
                    **holiday,
                    "employee_email": email,
                    "hours": round(hours, 2),
                    "pay_treatment": (
                        "separate_hourly_line" if employment.pay_basis == "hourly"
                        else "included_in_fixed_salary"
                    ),
                    "counts_toward_overtime": False,
                })
        return proposals


# --- daily time clock ------------------------------------------------------

def list_time_entries(employee_email: Optional[str] = None, *, limit: int = 60) -> list:
    with _session() as s:
        q = s.query(HRTimeEntry)
        if employee_email:
            q = q.filter(HRTimeEntry.employee_email == employee_email.strip().lower())
        rows = q.order_by(HRTimeEntry.date.desc(), HRTimeEntry.id.desc()).limit(limit).all()
        return [{"id": r.id, "employee_email": r.employee_email, "date": r.date,
                 "start_time": r.start_time, "stop_time": r.stop_time,
                 "hours": (
                     float(r.elapsed_seconds) / 3600
                     if r.elapsed_seconds else float(r.hours or 0)
                 ), "elapsed_seconds": r.elapsed_seconds or 0, "notes": r.notes,
                 "is_open": bool(r.clocked_in_at and not r.stop_time)} for r in rows]


def current_clock(employee_email: str) -> Optional[dict]:
    email = (employee_email or "").strip().lower()
    with _session() as s:
        row = (s.query(HRTimeEntry).filter(HRTimeEntry.employee_email == email,
                                           HRTimeEntry.clocked_in_at.is_not(None),
                                           HRTimeEntry.stop_time == "")
               .order_by(HRTimeEntry.id.desc()).first())
        return ({"id": row.id, "clocked_in_at": row.clocked_in_at} if row else None)


def time_review_flags(employee_email: Optional[str] = None) -> list[dict]:
    """Flag long shifts and 36/40-hour Sunday-Saturday thresholds."""
    entries = list_time_entries(employee_email, limit=500)
    flags: list[dict] = []
    week_totals: dict[tuple[str, date], float] = {}
    for entry in entries:
        hours = float(entry["hours"])
        if hours > 16:
            flags.append({
                "severity": "blocker", "employee_email": entry["employee_email"],
                "message": f"{entry['date']} is {hours:.2f} hours; review the punch.",
            })
        worked_date = entry["date"]
        sunday = worked_date - timedelta(days=(worked_date.weekday() + 1) % 7)
        key = (entry["employee_email"], sunday)
        week_totals[key] = week_totals.get(key, 0) + hours
    for (email, sunday), hours in sorted(week_totals.items()):
        if hours >= 40:
            flags.append({
                "severity": "overtime", "employee_email": email,
                "message": f"Week of {sunday}: {hours:.2f} worked hours; overtime is payable.",
            })
        elif hours >= 36:
            flags.append({
                "severity": "warning", "employee_email": email,
                "message": f"Week of {sunday}: {hours:.2f} hours; approaching overtime.",
            })
    return flags


def _timesheet_source_hash(
    session: Session, employee_email: str, period_start: date, period_end: date
) -> str:
    rows = session.query(HRTimeEntry).filter(
        HRTimeEntry.employee_email == employee_email,
        HRTimeEntry.date >= period_start,
        HRTimeEntry.date <= period_end,
    ).order_by(HRTimeEntry.id).all()
    payload = [[
        row.id, str(row.date), row.start_time, row.stop_time,
        int(row.elapsed_seconds or 0), str(row.hours or 0),
    ] for row in rows]
    return hashlib.sha256(
        json.dumps(payload, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def submit_timesheet(
    employee_email: str, *, period_start: date, period_end: date, actor: str
) -> tuple[bool, str]:
    """Attest that the employee's closed time is complete for a pay period."""
    email = (employee_email or "").strip().lower()
    if actor.strip().lower() != email or period_end < period_start:
        return False, "timesheet_attestation_required"
    with _session() as session:
        employee = session.query(HREmployee).filter_by(email=email).first()
        if not employee:
            return False, "employee_not_found"
        open_entry = session.query(HRTimeEntry).filter(
            HRTimeEntry.employee_email == email,
            HRTimeEntry.date >= period_start, HRTimeEntry.date <= period_end,
            HRTimeEntry.stop_time == "",
        ).first()
        pending = session.query(HRTimeCorrection).filter(
            HRTimeCorrection.employee_email == email,
            HRTimeCorrection.status == "requested",
        ).first()
        closed_count = session.query(func.count(HRTimeEntry.id)).filter(
            HRTimeEntry.employee_email == email,
            HRTimeEntry.date >= period_start, HRTimeEntry.date <= period_end,
            HRTimeEntry.stop_time != "",
        ).scalar() or 0
        if open_entry:
            return False, "timesheet_open_punch"
        if pending:
            return False, "timesheet_correction_pending"
        if not closed_count:
            return False, "timesheet_empty"
        source_hash = _timesheet_source_hash(session, email, period_start, period_end)
        row = session.query(HRTimesheetApproval).filter_by(
            employee_email=email, period_start=period_start, period_end=period_end
        ).first()
        if not row:
            row = HRTimesheetApproval(
                employee_email=email, period_start=period_start, period_end=period_end
            )
            session.add(row)
        elif row.status == "approved" and row.source_hash == source_hash:
            return True, "timesheet_already_approved"
        row.source_hash = source_hash
        row.status = "submitted"
        row.submitted_by = email
        row.submitted_at = datetime.now(timezone.utc)
        row.reviewed_by = ""
        row.review_note = ""
        row.reviewed_at = None
        session.flush()
        _audit(session, actor, "timesheet.submitted", "timesheet_approval", row.id, {
            "period_start": str(period_start), "period_end": str(period_end),
        })
        return True, "timesheet_submitted"


def decide_timesheet(
    approval_id: int, *, decision: str, review_note: str, actor: str
) -> tuple[bool, str]:
    if decision not in {"approved", "rejected"} or not review_note.strip():
        return False, "timesheet_review_invalid"
    actor_email = (actor or "").strip().lower()
    with _session() as session:
        row = session.get(HRTimesheetApproval, approval_id)
        if not row or row.status != "submitted":
            return False, "timesheet_review_not_found"
        if row.submitted_by.strip().lower() == actor_email:
            return False, "self_approval_blocked"
        current_hash = _timesheet_source_hash(
            session, row.employee_email, row.period_start, row.period_end
        )
        if current_hash != row.source_hash:
            row.status = "stale"
            return False, "timesheet_changed"
        row.status = decision
        row.reviewed_by = actor_email
        row.review_note = review_note.strip()
        row.reviewed_at = datetime.now(timezone.utc)
        _audit(session, actor, f"timesheet.{decision}", "timesheet_approval", row.id, {
            "period_start": str(row.period_start), "period_end": str(row.period_end),
        })
        return True, f"timesheet_{decision}"


def list_timesheet_approvals(
    period_start: date, period_end: date, employee_email: Optional[str] = None
) -> list[dict]:
    with _session() as session:
        query = session.query(HRTimesheetApproval).filter_by(
            period_start=period_start, period_end=period_end
        )
        if employee_email:
            query = query.filter_by(employee_email=employee_email.strip().lower())
        rows = query.order_by(HRTimesheetApproval.employee_email).all()
        result = []
        for row in rows:
            current_hash = _timesheet_source_hash(
                session, row.employee_email, row.period_start, row.period_end
            )
            status = row.status
            if status in {"submitted", "approved"} and current_hash != row.source_hash:
                status = "stale"
            result.append({
                "id": row.id, "employee_email": row.employee_email,
                "period_start": row.period_start, "period_end": row.period_end,
                "status": status, "submitted_by": row.submitted_by,
                "reviewed_by": row.reviewed_by, "review_note": row.review_note,
            })
        return result


def clock_in(employee_email: str, *, actor: str) -> tuple[bool, str]:
    email = (employee_email or "").strip().lower()
    now = datetime.now(timezone.utc)
    local_now = now.astimezone(ANATA_TIMEZONE)
    with _session() as s:
        open_row = s.query(HRTimeEntry).filter(HRTimeEntry.employee_email == email,
                                               HRTimeEntry.clocked_in_at.is_not(None),
                                               HRTimeEntry.stop_time == "").first()
        if open_row:
            return False, "already_clocked_in"
        row = HRTimeEntry(employee_email=email, date=local_now.date(),
                          start_time=local_now.strftime("%H:%M"), clocked_in_at=now, hours=0)
        s.add(row)
        s.flush()
        _audit(s, actor, "time.clock_in", "time_entry", row.id)
        return True, "clocked_in"


def clock_out(employee_email: str, *, actor: str) -> tuple[bool, str]:
    email = (employee_email or "").strip().lower()
    now = datetime.now(timezone.utc)
    with _session() as s:
        row = (s.query(HRTimeEntry).filter(HRTimeEntry.employee_email == email,
                                           HRTimeEntry.clocked_in_at.is_not(None),
                                           HRTimeEntry.stop_time == "")
               .order_by(HRTimeEntry.id.desc()).first())
        if not row:
            return False, "not_clocked_in"
        started = row.clocked_in_at
        if started.tzinfo is None:
            started = started.replace(tzinfo=timezone.utc)
        seconds = max(0, (now - started).total_seconds())
        row.stop_time = now.astimezone(ANATA_TIMEZONE).strftime("%H:%M")
        row.elapsed_seconds = int(round(seconds))
        row.hours = Decimal(str(round(seconds / 3600, 4)))
        _audit(s, actor, "time.clock_out", "time_entry", row.id,
               {"hours": float(row.hours)})
        return True, "clocked_out"


# --- time corrections and PTO policy --------------------------------------


def request_time_correction(time_entry_id: int, *, employee_email: str,
                            proposed_start: str, proposed_stop: str,
                            reason: str, actor: str) -> tuple[bool, str]:
    if not reason.strip() or not proposed_start or not proposed_stop:
        return False, "correction_invalid"
    email = (employee_email or "").strip().lower()
    try:
        start_value = time.fromisoformat(proposed_start)
        stop_value = time.fromisoformat(proposed_stop)
    except ValueError:
        return False, "correction_invalid"
    if datetime.combine(date.today(), stop_value) <= datetime.combine(date.today(), start_value):
        return False, "correction_invalid"
    with _session() as s:
        entry = s.get(HRTimeEntry, time_entry_id)
        if not entry or entry.employee_email != email:
            return False, "correction_not_found"
        existing = s.query(HRTimeCorrection).filter_by(
            time_entry_id=entry.id, status="requested"
        ).first()
        if existing:
            return False, "correction_pending"
        original = {
            "start_time": entry.start_time, "stop_time": entry.stop_time,
            "hours": float(entry.hours or 0), "date": entry.date.isoformat() if entry.date else None,
        }
        proposed_hours = (
            datetime.combine(entry.date or date.today(), stop_value)
            - datetime.combine(entry.date or date.today(), start_value)
        ).total_seconds() / 3600
        proposed = {
            "start_time": proposed_start, "stop_time": proposed_stop,
            "hours": round(proposed_hours, 4), "date": original["date"],
        }
        row = HRTimeCorrection(
            time_entry_id=entry.id, employee_email=email,
            original_json=original, proposed_json=proposed,
            reason=reason.strip(), requested_by=actor,
        )
        s.add(row)
        s.flush()
        _audit(s, actor, "time.correction_requested", "time_correction", row.id)
        return True, "correction_requested"


def list_time_corrections(employee_email: Optional[str] = None) -> list:
    with _session() as s:
        query = s.query(HRTimeCorrection)
        if employee_email:
            query = query.filter_by(employee_email=employee_email.strip().lower())
        rows = query.order_by(HRTimeCorrection.created_at.desc()).limit(100).all()
        return [{
            "id": row.id, "time_entry_id": row.time_entry_id,
            "employee_email": row.employee_email, "original": row.original_json or {},
            "proposed": row.proposed_json or {}, "final": row.final_json or {},
            "reason": row.reason, "status": row.status,
            "requested_by": row.requested_by, "reviewed_by": row.reviewed_by,
        } for row in rows]


def decide_time_correction(correction_id: int, *, decision: str,
                           reviewer_reason: str, actor: str) -> tuple[bool, str]:
    if decision not in {"approved", "denied"}:
        return False, "correction_invalid"
    with _session() as s:
        row = s.get(HRTimeCorrection, correction_id)
        if not row or row.status != "requested":
            return False, "correction_not_found"
        if row.requested_by.strip().lower() == actor.strip().lower():
            return False, "self_approval_blocked"
        entry = s.get(HRTimeEntry, row.time_entry_id)
        if not entry:
            return False, "correction_not_found"
        if decision == "approved":
            proposed = row.proposed_json or {}
            entry.start_time = proposed.get("start_time", entry.start_time)
            entry.stop_time = proposed.get("stop_time", entry.stop_time)
            entry.hours = Decimal(str(proposed.get("hours", float(entry.hours or 0))))
            entry.elapsed_seconds = int(round(float(proposed.get("hours", 0)) * 3600))
            entry.notes = (
                f"{entry.notes}\nCorrection approved by {actor}: {reviewer_reason}".strip()
            )
            row.final_json = dict(proposed)
            _supersede_open_payrolls(
                s, actor=actor, effective_start=entry.date or date.today(),
                reason="approved time correction",
            )
        row.status = decision
        row.reviewed_by = actor
        row.reviewed_at = datetime.now(timezone.utc)
        _audit(s, actor, f"time.correction_{decision}", "time_correction", row.id, {
            "reviewer_reason": (reviewer_reason or "").strip(),
        })
        return True, f"correction_{decision}"


def _preview_accrued_hours(session: Session, email: str, employment: HREmploymentProfile) -> float:
    """Preview accrual until payroll posting creates authoritative ledger entries."""
    if not employment.hire_date:
        return 0.0
    today = datetime.now(ANATA_TIMEZONE).date()
    if today < employment.hire_date:
        return 0.0
    if employment.pay_basis == "fixed_semimonthly":
        months = (today.year - employment.hire_date.year) * 12 + today.month - employment.hire_date.month
        completed_periods = max(0, months * 2)
        if today.day >= 16:
            completed_periods += 1
        eligible_hours = completed_periods * float(employment.standard_period_hours or 0)
    else:
        eligible_hours = float(
            session.query(func.coalesce(func.sum(HRTimeEntry.hours), 0)).filter(
                HRTimeEntry.employee_email == email,
                HRTimeEntry.date >= employment.hire_date,
            ).scalar() or 0
        )
    return min(PTO_ANNUAL_HOURS, eligible_hours / PTO_ACCRUAL_DIVISOR)


def pto_summary(employee_email: str) -> dict:
    email = (employee_email or "").strip().lower()
    with _session() as s:
        employment = s.query(HREmploymentProfile).filter_by(employee_email=email).first()
        ledger_total = float(
            s.query(func.coalesce(func.sum(HRPTOLedger.hours), 0)).filter_by(
                employee_email=email
            ).scalar() or 0
        )
        preview = _preview_accrued_hours(s, email, employment) if employment else 0.0
        ledger_earned = float(
            s.query(func.coalesce(func.sum(HRPTOLedger.hours), 0)).filter(
                HRPTOLedger.employee_email == email,
                HRPTOLedger.entry_type.in_(("earned", "adjusted")),
            ).scalar() or 0
        )
        used = -float(
            s.query(func.coalesce(func.sum(HRPTOLedger.hours), 0)).filter_by(
                employee_email=email, entry_type="used"
            ).scalar() or 0
        )
        accrued = min(PTO_ANNUAL_HOURS, max(ledger_earned, preview))
        available = max(0.0, accrued + ledger_total - ledger_earned)
        eligible = bool(
            employment and employment.pto_eligible_date
            and datetime.now(ANATA_TIMEZONE).date() >= employment.pto_eligible_date
        )
        return {
            "accrued": round(accrued, 2), "used": round(used, 2),
            "available": round(available, 2), "eligible": eligible,
            "eligible_date": employment.pto_eligible_date if employment else None,
            "source": "ledger" if ledger_earned else "preview",
        }


def _pto_summary_in_session(session: Session, email: str,
                            employment: Optional[HREmploymentProfile]) -> dict:
    ledger_total = float(
        session.query(func.coalesce(func.sum(HRPTOLedger.hours), 0)).filter_by(
            employee_email=email
        ).scalar() or 0
    )
    preview = _preview_accrued_hours(session, email, employment) if employment else 0.0
    ledger_earned = float(
        session.query(func.coalesce(func.sum(HRPTOLedger.hours), 0)).filter(
            HRPTOLedger.employee_email == email,
            HRPTOLedger.entry_type.in_(("earned", "adjusted")),
        ).scalar() or 0
    )
    used = -float(
        session.query(func.coalesce(func.sum(HRPTOLedger.hours), 0)).filter_by(
            employee_email=email, entry_type="used"
        ).scalar() or 0
    )
    accrued = min(PTO_ANNUAL_HOURS, max(ledger_earned, preview))
    return {
        "accrued": round(accrued, 2), "used": round(used, 2),
        "available": round(max(0.0, accrued + ledger_total - ledger_earned), 2),
    }


def create_pto_request(employee_email: str, *, start_date: date, end_date: date,
                       hours: float, reason: str, actor: str) -> tuple[bool, str]:
    if end_date < start_date or hours <= 0:
        return False, "invalid_request"
    from sales_support_agent.services.hr.payroll import semimonthly_period
    if (
        semimonthly_period(start_date).start_date
        != semimonthly_period(end_date).start_date
    ):
        return False, "pto_split_period_required"
    email = (employee_email or "").strip().lower()
    with _session() as s:
        employment = s.query(HREmploymentProfile).filter_by(employee_email=email).first()
        if not employment or not employment.pto_eligible_date:
            return False, "pto_setup_required"
        if start_date < employment.pto_eligible_date:
            return False, "pto_not_eligible"
        summary = _pto_summary_in_session(s, email, employment)
        pending = float(
            s.query(func.coalesce(func.sum(HRPTORequest.hours), 0)).filter(
                HRPTORequest.employee_email == email,
                HRPTORequest.status == "pending",
            ).scalar() or 0
        )
        if hours > max(0, summary["available"] - pending):
            return False, "pto_insufficient"
        row = HRPTORequest(employee_email=email, start_date=start_date, end_date=end_date,
                           hours=Decimal(str(round(hours, 2))), reason=(reason or "").strip())
        s.add(row)
        s.flush()
        _audit(s, actor, "pto.requested", "pto_request", row.id, {"hours": hours})
        return True, "pto_requested"


def list_pto_requests(employee_email: Optional[str] = None) -> list:
    with _session() as s:
        q = s.query(HRPTORequest)
        if employee_email:
            q = q.filter(HRPTORequest.employee_email == employee_email.strip().lower())
        rows = q.order_by(HRPTORequest.created_at.desc()).limit(100).all()
        return [{"id": r.id, "employee_email": r.employee_email,
                 "start_date": r.start_date, "end_date": r.end_date,
                 "hours": float(r.hours), "reason": r.reason, "status": r.status}
                for r in rows]


def decide_pto(request_id: int, *, decision: str, actor: str) -> bool:
    if decision not in {"approved", "denied"}:
        return False
    with _session() as s:
        row = s.get(HRPTORequest, request_id)
        if not row or row.status != "pending":
            return False
        if row.employee_email == actor.strip().lower():
            return False
        row.status, row.decided_by, row.decided_at = decision, actor, datetime.now(timezone.utc)
        if decision == "approved":
            s.add(HRPTOLedger(
                employee_email=row.employee_email, entry_type="reserved",
                hours=-row.hours, effective_date=row.start_date,
                source_type="pto_request", source_id=str(row.id),
                note="Reserved when PTO request was approved.", created_by=actor,
            ))
        _supersede_open_payrolls(
            s, actor=actor, effective_start=row.start_date, effective_end=row.end_date,
            reason=f"PTO request {decision}",
        )
        _audit(s, actor, f"pto.{decision}", "pto_request", row.id)
        return True
