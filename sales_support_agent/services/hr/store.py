"""HR data access — employees, teams, and dashboard counts.

Short-lived ORM Sessions on the shared engine (mirrors access/store.py). Money
is held in integer cents in the DB; helpers convert to/from dollar strings at
the form boundary so the rest of the app deals in dollars.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from typing import Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from sales_support_agent.models.database import get_engine
from sales_support_agent.models.hr import HREmployee, HRTeam


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
                    phone: str = "", status: str = "active") -> Optional[int]:
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
        return e.id


def update_employee(emp_id: int, **fields) -> bool:
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
