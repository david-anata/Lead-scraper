"""HR data access — employees, teams, and dashboard counts.

Short-lived ORM Sessions on the shared engine (mirrors access/store.py). Money
is held in integer cents in the DB; helpers convert to/from dollar strings at
the form boundary so the rest of the app deals in dollars.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Optional
from zoneinfo import ZoneInfo

from sqlalchemy import func
from sqlalchemy.orm import Session

from sales_support_agent.models.database import get_engine
from sales_support_agent.models.hr import HRAuditEvent, HREmployee, HRPTORequest, HRTeam, HRTimeEntry


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


def _audit(session: Session, actor: str, action: str, entity_type: str,
           entity_id: object = "", details: Optional[dict] = None) -> None:
    session.add(HRAuditEvent(actor_email=(actor or "system").strip().lower(), action=action,
                             entity_type=entity_type, entity_id=str(entity_id or ""),
                             details=details or {}))


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
        "onboarding_complete": bool(e.onboarding_complete),
    }


def list_employees(*, include_inactive: bool = True) -> list:
    with _session() as s:
        q = s.query(HREmployee)
        if not include_inactive:
            q = q.filter(HREmployee.status == "active")
        rows = q.order_by(HREmployee.full_name.asc(), HREmployee.email.asc()).all()
        return [_emp_dict(e) for e in rows]


def get_employee(emp_id: int) -> Optional[dict]:
    with _session() as s:
        e = s.get(HREmployee, emp_id)
        return _emp_dict(e) if e else None


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
        return {"total_employees": total, "active_employees": active,
                "teams": teams, "onboarding_incomplete": onboarding}


# --- daily time clock ------------------------------------------------------

def list_time_entries(employee_email: Optional[str] = None, *, limit: int = 60) -> list:
    with _session() as s:
        q = s.query(HRTimeEntry)
        if employee_email:
            q = q.filter(HRTimeEntry.employee_email == employee_email.strip().lower())
        rows = q.order_by(HRTimeEntry.date.desc(), HRTimeEntry.id.desc()).limit(limit).all()
        return [{"id": r.id, "employee_email": r.employee_email, "date": r.date,
                 "start_time": r.start_time, "stop_time": r.stop_time,
                 "hours": float(r.hours or 0), "notes": r.notes,
                 "is_open": bool(r.clocked_in_at and not r.stop_time)} for r in rows]


def current_clock(employee_email: str) -> Optional[dict]:
    email = (employee_email or "").strip().lower()
    with _session() as s:
        row = (s.query(HRTimeEntry).filter(HRTimeEntry.employee_email == email,
                                           HRTimeEntry.clocked_in_at.is_not(None),
                                           HRTimeEntry.stop_time == "")
               .order_by(HRTimeEntry.id.desc()).first())
        return ({"id": row.id, "clocked_in_at": row.clocked_in_at} if row else None)


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
        row.hours = Decimal(str(round(seconds / 3600, 4)))
        _audit(s, actor, "time.clock_out", "time_entry", row.id,
               {"hours": float(row.hours)})
        return True, "clocked_out"


# --- simple PTO policy -----------------------------------------------------

PTO_ANNUAL_HOURS = 40.0
PTO_ACCRUAL_DIVISOR = 52.0
ANATA_TIMEZONE = ZoneInfo("America/Denver")


def pto_summary(employee_email: str) -> dict:
    email = (employee_email or "").strip().lower()
    with _session() as s:
        worked = s.query(func.coalesce(func.sum(HRTimeEntry.hours), 0)).filter(
            HRTimeEntry.employee_email == email).scalar() or 0
        used = s.query(func.coalesce(func.sum(HRPTORequest.hours), 0)).filter(
            HRPTORequest.employee_email == email, HRPTORequest.status == "approved").scalar() or 0
        accrued = min(PTO_ANNUAL_HOURS, float(worked) / PTO_ACCRUAL_DIVISOR)
        return {"accrued": round(accrued, 2), "used": round(float(used), 2),
                "available": round(max(0.0, accrued - float(used)), 2)}


def create_pto_request(employee_email: str, *, start_date: date, end_date: date,
                       hours: float, reason: str, actor: str) -> tuple[bool, str]:
    if end_date < start_date or hours <= 0:
        return False, "invalid_request"
    email = (employee_email or "").strip().lower()
    with _session() as s:
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
        row.status, row.decided_by, row.decided_at = decision, actor, datetime.now(timezone.utc)
        _audit(s, actor, f"pto.{decision}", "pto_request", row.id)
        return True
