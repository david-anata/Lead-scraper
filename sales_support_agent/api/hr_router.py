"""HR section routes — /admin/hr/*.

Most pages are gated by `hr.access`; the money/config pages (payroll, settings)
by `hr.payroll`. Server-rendered HTML (no JSON API). POSTs redirect (303).
"""

from __future__ import annotations

from datetime import date
from collections import defaultdict, deque
from threading import Lock
from time import monotonic
from urllib.parse import urlparse

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import (
    HTMLResponse,
    JSONResponse,
    PlainTextResponse,
    RedirectResponse,
    Response,
)

from sales_support_agent.services.auth_deps import (
    require_all_tools,
    require_any_tool,
    require_recent_tool,
    require_tool,
)
from sales_support_agent.services.access.notify import send_invite_email
from sales_support_agent.services.access import store as access_store
from sales_support_agent.services.hr import store
from sales_support_agent.services.hr import payroll_store
from sales_support_agent.services.hr import legacy_import
from sales_support_agent.services.hr import workforce
from sales_support_agent.services.hr import pto_workflow


def _correction_duration(payload: dict) -> float:
    """Derive duration from visible clock values; legacy stored hours may be corrupt."""
    try:
        start_hour, start_minute = map(int, str(payload.get("start_time")).split(":")[:2])
        stop_hour, stop_minute = map(int, str(payload.get("stop_time")).split(":")[:2])
        minutes = (stop_hour * 60 + stop_minute) - (start_hour * 60 + start_minute)
        if minutes < 0:
            minutes += 24 * 60
        return round(minutes / 60, 4)
    except (TypeError, ValueError):
        return float(payload.get("hours") or 0)
from sales_support_agent.services.hr import reporting
from sales_support_agent.services.hr.provider_contract import contract_descriptor
from sales_support_agent.services.hr.pages import (
    render_hr_coming_soon,
    render_hr_dashboard,
    render_hr_employee_form,
    render_hr_employees,
    render_hr_invitation,
    render_hr_onboarding,
    render_hr_employee_record_missing,
    render_hr_payroll_control,
    render_hr_payroll_approval,
    render_hr_payroll_run,
    render_hr_pay_statements,
    render_hr_settings,
    render_hr_legacy_import,
    render_hr_contractors,
    render_hr_compliance,
    render_hr_offboarding,
    render_hr_reports,
    render_hr_setup,
    render_hr_policies,
    render_hr_teams,
    render_hr_team_detail,
    render_hr_time,
    render_hr_access_training,
)

async def _same_origin_write(request: Request) -> None:
    """Reject cross-site browser writes while preserving server/test clients."""
    if request.method in {"GET", "HEAD", "OPTIONS"}:
        return
    if (request.headers.get("sec-fetch-site") or "").lower() == "cross-site":
        raise HTTPException(status_code=403, detail="Cross-site HR write rejected.")
    origin = request.headers.get("origin")
    if origin and urlparse(origin).netloc.lower() != request.url.netloc.lower():
        raise HTTPException(status_code=403, detail="HR form origin does not match.")
    # Browsers include Origin/Sec-Fetch headers. Non-browser API/test clients
    # remain compatible, while real browser writes require a session-bound token.
    if origin or request.headers.get("sec-fetch-mode"):
        from sales_support_agent.services.auth_deps import get_current_user
        from sales_support_agent.services.hr.security import valid_csrf_token
        form = await request.form()
        if not valid_csrf_token(
            get_current_user(request), str(form.get("_csrf_token") or "")
        ):
            raise HTTPException(status_code=403, detail="HR form security token is invalid.")


router = APIRouter(prefix="/admin/hr", dependencies=[Depends(_same_origin_write)])

_guard = require_tool("hr.access")
_pay_guard = require_tool("hr.payroll")
_people_guard = require_any_tool("hr.people.manage", "hr.payroll")
_people_comp_guard = require_all_tools(
    "hr.people.manage", "hr.compensation.manage", legacy_keys=("hr.payroll",)
)
_time_review_guard = require_any_tool("hr.time.approve_team", "hr.payroll")
_pay_view_guard = require_any_tool(
    "hr.payroll.view", "hr.payroll.prepare", "hr.payroll.approve",
    "hr.payroll.submit", "hr.payroll",
)
_pay_prepare_guard = require_any_tool("hr.payroll.prepare", "hr.payroll")
_pay_approve_guard = require_any_tool("hr.payroll.approve", "hr.payroll")
_recent_pay_approve_guard = require_recent_tool(
    "hr.payroll.approve", legacy_keys=("hr.payroll",), max_age_minutes=30
)
_pay_submit_guard = require_any_tool("hr.payroll.submit", "hr.payroll")
_settings_guard = require_any_tool("hr.settings.manage", "hr.payroll")
_recent_settings_guard = require_recent_tool(
    "hr.settings.manage", legacy_keys=("hr.payroll",), max_age_minutes=30
)
_reports_guard = require_any_tool("hr.audit.view", "hr.payroll.view", "hr.payroll")

_sensitive_attempts: dict[str, deque] = defaultdict(deque)
_sensitive_attempts_lock = Lock()


async def _sensitive_rate_limit(request: Request) -> None:
    """Limit rapid repeat writes on payroll approval/submission endpoints."""
    if request.method in {"GET", "HEAD", "OPTIONS"}:
        return
    identity = request.cookies.get(next(iter(request.cookies), ""), "")
    key = f"{request.client.host if request.client else 'unknown'}:{hash(identity)}"
    now = monotonic()
    with _sensitive_attempts_lock:
        attempts = _sensitive_attempts[key]
        while attempts and attempts[0] < now - 60:
            attempts.popleft()
        if len(attempts) >= 20:
            raise HTTPException(
                status_code=429,
                detail="Too many sensitive HR actions. Wait one minute and retry.",
            )
        attempts.append(now)


def _flash(request: Request):
    return request.query_params.get("ok") or request.query_params.get("err")


def _can_manage(user: dict) -> bool:
    permissions = user.get("permissions") or set()
    return bool(user.get("is_superadmin") or {
        "hr.people.view", "hr.people.manage", "hr.payroll",
    }.intersection(permissions))


def _can_view_compensation(user: dict) -> bool:
    permissions = user.get("permissions") or set()
    return bool(user.get("is_superadmin") or {
        "hr.payroll", "hr.compensation.view", "hr.compensation.manage",
    }.intersection(permissions))


def _can_review_time(user: dict) -> bool:
    permissions = user.get("permissions") or set()
    return bool(
        user.get("is_superadmin")
        or {"hr.payroll", "hr.time.approve_team"}.intersection(permissions)
    )


def _hide_compensation(employee: dict) -> dict:
    safe = dict(employee)
    safe.pop("hourly_rate", None)
    safe.pop("hourly_rate_cents", None)
    safe.pop("annual_salary", None)
    safe.pop("annual_salary_cents", None)
    if safe.get("employment"):
        safe["employment"] = dict(safe["employment"])
        safe["employment"].pop("fixed_pay_per_period", None)
        safe["employment"].pop("fixed_pay_per_period_cents", None)
    return safe


def _has_full_hr_admin(user: dict) -> bool:
    return bool(
        user.get("is_superadmin")
        or "hr.payroll" in (user.get("permissions") or set())
    )


def _employee_reference_error(
    *, employee_email: str, manager_email: str, team_id: str
) -> str:
    """Validate reporting and team references before changing an employee."""
    manager = (manager_email or "").strip().lower()
    email = (employee_email or "").strip().lower()
    if manager:
        manager_row = store.get_employee_by_email(manager)
        if not manager_row or manager_row.get("status") != "active":
            return "Manager must be an active employee record."
        if manager == email:
            return "An employee cannot be their own manager."
    if team_id:
        try:
            valid_team = store.get_team(int(team_id))
        except (TypeError, ValueError):
            valid_team = None
        if not valid_team:
            return "Choose an existing team."
    return ""


def _employee_classification_error(
    *, employee_type: str, pay_basis: str, classification: str,
    standard_weekly_hours: float,
) -> str:
    if employee_type not in {"hourly", "salaried", "contractor"}:
        return "Choose a valid worker record type."
    if pay_basis not in {"hourly", "fixed_semimonthly"}:
        return "Choose hourly or fixed semimonthly pay."
    if classification not in {"exempt", "nonexempt"}:
        return "Choose exempt or nonexempt overtime classification."
    if not 0 <= standard_weekly_hours <= 168:
        return "Standard weekly hours must be between 0 and 168."
    return ""


def _hr_login_email_error(value: str) -> str:
    normalized = (value or "").strip().lower()
    if not access_store.valid_email(normalized):
        return "Use a valid email address."
    return ""


def _managed_employee_emails(user: dict) -> set[str]:
    if _has_full_hr_admin(user):
        return {item["email"] for item in store.list_employees()}
    manager_login = (user.get("email") or "").strip().lower()
    manager_record = store.get_employee_by_email(manager_login)
    manager = ((manager_record or {}).get("email") or manager_login).strip().lower()
    return {
        item["email"] for item in store.list_employees()
        if ((item.get("employment") or {}).get("manager_email") or "")
        .strip().lower() == manager
    }


def _employee_record_email(user: dict) -> str:
    """Map a personal HR login back to the immutable employee record key."""
    login_email = (user.get("email") or "").strip().lower()
    employee = store.get_employee_by_email(login_email)
    return (employee or {}).get("email") or login_email


def _require_team_record(user: dict, records: list[dict], record_id: int) -> None:
    if _has_full_hr_admin(user):
        return
    selected = next(
        (item for item in records if int(item.get("id") or 0) == record_id), None
    )
    if (
        not selected
        or selected.get("employee_email") not in _managed_employee_emails(user)
    ):
        raise HTTPException(
            status_code=403, detail="That employee is not assigned to you."
        )


# --- dashboard -------------------------------------------------------------

FIRST_LIVE_PAYROLL_DATE = date(2026, 8, 1)


def _default_payroll_date(today: date | None = None) -> date:
    """Keep pre-launch views on the approved first live payroll period."""
    current = today or date.today()
    return max(current, FIRST_LIVE_PAYROLL_DATE)


def _ooo_calendar_readiness() -> dict:
    current = pto_workflow.calendar_readiness()
    tested = payroll_store.latest_ooo_calendar_test()
    same_configuration = bool(
        tested
        and tested.get("calendar_id") == current.get("calendar_id")
        and tested.get("service_account_email") == current.get("service_account_email")
    )
    current["verified"] = bool(same_configuration and tested.get("ready"))
    current["tested_state"] = tested.get("state") if same_configuration else ""
    current["tested_at"] = tested.get("tested_at") if same_configuration else None
    if current["verified"]:
        current["status"] = "Ready"
    return current

@router.get("", response_class=HTMLResponse)
async def hr_dashboard(request: Request, user: dict = Depends(_guard)):
    stats = (
        store.dashboard_stats() if _can_manage(user)
        else store.employee_dashboard_stats(_employee_record_email(user))
    )
    return HTMLResponse(render_hr_dashboard(
        stats, user=user, flash=_flash(request), manager_view=_can_manage(user)
    ))


@router.get("/setup", response_class=HTMLResponse)
async def hr_setup(request: Request, user: dict = Depends(_pay_view_guard)):
    """Show one evidence-backed checklist for reaching payroll readiness."""
    return HTMLResponse(render_hr_setup(
        payroll_store.control_room(_default_payroll_date()),
        payroll_store.get_company_profile(),
        _ooo_calendar_readiness(),
        user=user,
        flash=_flash(request),
    ))


# --- employees -------------------------------------------------------------

@router.get("/employees", response_class=HTMLResponse)
async def employees_list(request: Request, user: dict = Depends(_guard)):
    if not _can_manage(user):
        return RedirectResponse("/admin/hr/onboarding", status_code=303)
    employees = store.list_employees() if _can_manage(user) else [
        item for item in store.list_employees()
        if item["email"] == (user.get("email") or "").strip().lower()
    ]
    if not _can_view_compensation(user):
        employees = [_hide_compensation(item) for item in employees]
    else:
        store.audit_sensitive_read(
            user.get("email", ""), scope="compensation_directory",
            purpose="employee list",
        )
    return HTMLResponse(render_hr_employees(employees, user=user, flash=_flash(request)))


@router.get("/employees/new", response_class=HTMLResponse)
async def employee_new(request: Request, user: dict = Depends(_people_comp_guard)):
    return HTMLResponse(render_hr_employee_form(None, store.list_teams(), user=user))


@router.post("/employees/new", response_class=HTMLResponse)
async def employee_create(
    request: Request,
    email: str = Form(""),
    hr_login_email: str = Form(""),
    full_name: str = Form(""),
    hr_role: str = Form("employee"),
    employee_type: str = Form("hourly"),
    payroll_relationship: str = Form("w2"),
    team_id: str = Form(""),
    hourly_rate: str = Form("0"),
    fixed_pay_per_period: str = Form("0"),
    hire_date: date | None = Form(None),
    title: str = Form(""),
    manager_email: str = Form(""),
    classification: str = Form("nonexempt"),
    pay_basis: str = Form("hourly"),
    standard_weekly_hours: float = Form(40),
    phone: str = Form(""),
    status: str = Form("active"),
    user: dict = Depends(_people_comp_guard),
):
    entered = {
        "_is_new": True,
        "email": email.strip().lower(), "hr_login_email": hr_login_email.strip().lower(),
        "full_name": full_name, "hr_role": hr_role,
        "employee_type": (
            "contractor" if employee_type == "contractor" else
            "salaried" if pay_basis == "fixed_semimonthly" else "hourly"
        ),
        "team_id": team_id, "hourly_rate": hourly_rate,
        "phone": phone, "status": status, "id": "",
        "employment": {
            "fixed_pay_per_period": fixed_pay_per_period, "hire_date": hire_date or "",
            "title": title, "manager_email": manager_email,
            "classification": classification, "pay_basis": pay_basis,
            "payroll_eligible": payroll_relationship == "w2",
            "standard_weekly_hours": standard_weekly_hours,
        },
    }
    if payroll_relationship not in {"w2", "not_on_payroll"}:
        return HTMLResponse(render_hr_employee_form(
            entered, store.list_teams(), user=user,
            error="Choose whether this person is included in Anata W-2 payroll.",
        ), status_code=422)
    if not email.strip():
        return HTMLResponse(render_hr_employee_form(entered, store.list_teams(), user=user,
                                                    error="Email is required."), status_code=422)
    if employee_type != "contractor" and not hr_login_email.strip():
        return HTMLResponse(render_hr_employee_form(
            entered, store.list_teams(), user=user,
            error="Add the employee’s personal HR sign-in email.",
        ), status_code=422)
    if hr_login_email.strip() and (login_error := _hr_login_email_error(hr_login_email)):
        return HTMLResponse(render_hr_employee_form(
            entered, store.list_teams(), user=user, error=login_error,
        ), status_code=422)
    classification_error = _employee_classification_error(
        employee_type=employee_type, pay_basis=pay_basis,
        classification=classification,
        standard_weekly_hours=standard_weekly_hours,
    )
    if classification_error:
        return HTMLResponse(
            render_hr_employee_form(
                entered, store.list_teams(), user=user,
                error=classification_error,
            ),
            status_code=422,
        )
    reference_error = _employee_reference_error(
        employee_email=email, manager_email=manager_email, team_id=team_id
    )
    if reference_error:
        return HTMLResponse(
            render_hr_employee_form(
                entered, store.list_teams(), user=user, error=reference_error
            ),
            status_code=422,
        )
    try:
        hourly_rate_cents = store.strict_dollars_to_cents(hourly_rate)
        fixed_pay_cents = store.strict_dollars_to_cents(fixed_pay_per_period)
    except ValueError as exc:
        return HTMLResponse(
            render_hr_employee_form(entered, store.list_teams(), user=user, error=str(exc)),
            status_code=422,
        )
    if employee_type == "contractor" and (hourly_rate_cents or fixed_pay_cents):
        return HTMLResponse(render_hr_employee_form(
            entered, store.list_teams(), user=user,
            error="Contractor fees belong in the separate Contractor workflow.",
        ), status_code=422)
    if employee_type != "contractor" and pay_basis == "hourly" and fixed_pay_cents:
        return HTMLResponse(render_hr_employee_form(
            entered, store.list_teams(), user=user,
            error="Hourly employees need one hourly rate and no fixed check amount.",
        ), status_code=422)
    if (
        employee_type != "contractor"
        and pay_basis == "fixed_semimonthly"
        and hourly_rate_cents
    ):
        return HTMLResponse(render_hr_employee_form(
            entered, store.list_teams(), user=user,
            error="Salaried employees need one fixed semimonthly amount and no hourly rate.",
        ), status_code=422)
    new_id = store.create_employee(
        email=email, hr_login_email=hr_login_email,
        full_name=full_name, hr_role=hr_role,
        employee_type=(
            "contractor" if employee_type == "contractor" else
            "salaried" if pay_basis == "fixed_semimonthly" else "hourly"
        ),
        team_id=team_id or None, hourly_rate=hourly_rate, annual_salary="0",
        phone=phone, status=status, actor=user.get("email", "system"))
    if new_id is None:
        return HTMLResponse(render_hr_employee_form(
            entered,
            store.list_teams(), user=user,
            error="An employee with that email already exists."), status_code=422)
    if employee_type != "contractor":
        store.upsert_employment_profile(
            email, hire_date=hire_date, title=title, manager_email=manager_email,
            classification=classification, pay_basis=pay_basis,
            payroll_eligible=payroll_relationship == "w2",
            fixed_pay_per_period=fixed_pay_per_period,
            standard_weekly_hours=standard_weekly_hours, standard_period_hours=86.67,
            actor=user.get("email", "system"),
        )
    return RedirectResponse("/admin/hr/employees?ok=created", status_code=303)


@router.get("/employees/{emp_id}", response_class=HTMLResponse)
async def employee_edit(emp_id: int, request: Request, user: dict = Depends(_people_comp_guard)):
    emp = store.get_employee(emp_id)
    if not emp:
        return RedirectResponse("/admin/hr/employees", status_code=303)
    emp["compensation_history"] = store.list_compensation_changes(emp["email"])
    return HTMLResponse(render_hr_employee_form(emp, store.list_teams(), user=user))


@router.post("/employees/{emp_id}", response_class=HTMLResponse)
async def employee_update(
    emp_id: int,
    request: Request,
    hr_login_email: str = Form(""),
    full_name: str = Form(""),
    hr_role: str = Form("employee"),
    employee_type: str = Form("hourly"),
    payroll_relationship: str = Form("w2"),
    team_id: str = Form(""),
    hourly_rate: str = Form("0"),
    annual_salary: str = Form("0"),
    hire_date: date | None = Form(None),
    title: str = Form(""),
    manager_email: str = Form(""),
    classification: str = Form("nonexempt"),
    pay_basis: str = Form("hourly"),
    fixed_pay_per_period: str = Form("0"),
    compensation_effective_date: date | None = Form(None),
    compensation_reason: str = Form(""),
    standard_weekly_hours: float = Form(40),
    phone: str = Form(""),
    status: str = Form("active"),
    user: dict = Depends(_people_comp_guard),
):
    employee = store.get_employee(emp_id)
    if not employee:
        return RedirectResponse("/admin/hr/employees?err=not_found", status_code=303)
    if payroll_relationship not in {"w2", "not_on_payroll"}:
        employee["compensation_history"] = store.list_compensation_changes(
            employee["email"]
        )
        return HTMLResponse(render_hr_employee_form(
            employee, store.list_teams(), user=user,
            error="Choose whether this person is included in Anata W-2 payroll.",
        ), status_code=422)
    classification_error = _employee_classification_error(
        employee_type=employee_type, pay_basis=pay_basis,
        classification=classification,
        standard_weekly_hours=standard_weekly_hours,
    )
    if classification_error:
        employee["compensation_history"] = store.list_compensation_changes(
            employee["email"]
        )
        return HTMLResponse(render_hr_employee_form(
            employee, store.list_teams(), user=user, error=classification_error,
        ), status_code=422)
    reference_error = _employee_reference_error(
        employee_email=employee["email"], manager_email=manager_email,
        team_id=team_id,
    )
    if reference_error:
        employee["compensation_history"] = store.list_compensation_changes(
            employee["email"]
        )
        return HTMLResponse(render_hr_employee_form(
            employee, store.list_teams(), user=user, error=reference_error,
        ), status_code=422)
    if employee.get("employee_type") == "contractor":
        employee_type = "contractor"
    else:
        employee_type = "salaried" if pay_basis == "fixed_semimonthly" else "hourly"
    try:
        hourly_rate_cents = store.strict_dollars_to_cents(hourly_rate)
        fixed_pay_cents = store.strict_dollars_to_cents(fixed_pay_per_period)
    except ValueError as exc:
        employee["compensation_history"] = store.list_compensation_changes(
            employee["email"]
        )
        return HTMLResponse(render_hr_employee_form(
            employee, store.list_teams(), user=user, error=str(exc),
        ), status_code=422)
    if employee_type == "contractor" and (hourly_rate_cents or fixed_pay_cents):
        employee["compensation_history"] = store.list_compensation_changes(
            employee["email"]
        )
        return HTMLResponse(render_hr_employee_form(
            employee, store.list_teams(), user=user,
            error="Contractor fees belong in the separate Contractor workflow.",
        ), status_code=422)
    if employee_type != "contractor" and pay_basis == "hourly" and fixed_pay_cents:
        employee["compensation_history"] = store.list_compensation_changes(
            employee["email"]
        )
        return HTMLResponse(render_hr_employee_form(
            employee, store.list_teams(), user=user,
            error="Hourly employees need one hourly rate and no fixed check amount.",
        ), status_code=422)
    if (
        employee_type != "contractor"
        and pay_basis == "fixed_semimonthly"
        and hourly_rate_cents
    ):
        employee["compensation_history"] = store.list_compensation_changes(
            employee["email"]
        )
        return HTMLResponse(render_hr_employee_form(
            employee, store.list_teams(), user=user,
            error="Salaried employees need one fixed semimonthly amount and no hourly rate.",
        ), status_code=422)
    employment = employee.get("employment") or {}
    stored_pay_basis = employment.get("pay_basis") or (
        "fixed_semimonthly" if employee.get("employee_type") == "salaried" else "hourly"
    )
    if employee_type == "contractor":
        stored_pay_basis = "contractor"
    prior_compensation = {
        "employee_type": (
            "contractor" if stored_pay_basis == "contractor" else
            "salaried" if stored_pay_basis == "fixed_semimonthly" else "hourly"
        ),
        "hourly_rate_cents": int(employee.get("hourly_rate_cents") or 0),
        "pay_basis": stored_pay_basis,
        "fixed_pay_per_period_cents": int(
            employment.get("fixed_pay_per_period_cents") or 0
        ),
        "payroll_eligible": employment.get("payroll_eligible", True),
    }
    new_compensation = {
        "employee_type": employee_type,
        "hourly_rate_cents": hourly_rate_cents,
        "pay_basis": "contractor" if employee_type == "contractor" else pay_basis,
        "fixed_pay_per_period_cents": fixed_pay_cents,
        "payroll_eligible": payroll_relationship == "w2",
    }
    compensation_changed = prior_compensation != new_compensation
    employment_changed = employee_type != "contractor" and {
        "hire_date": employment.get("hire_date"),
        "title": (employment.get("title") or "").strip(),
        "manager_email": (employment.get("manager_email") or "").strip().lower(),
        "classification": employment.get("classification") or "nonexempt",
        "pay_basis": employment.get("pay_basis") or "hourly",
        "payroll_eligible": employment.get("payroll_eligible", True),
        "fixed_pay_per_period_cents": int(
            employment.get("fixed_pay_per_period_cents") or 0
        ),
        "standard_weekly_hours": float(
            employment.get("standard_weekly_hours") or 40
        ),
    } != {
        "hire_date": hire_date,
        "title": title.strip(),
        "manager_email": manager_email.strip().lower(),
        "classification": classification,
        "pay_basis": pay_basis,
        "payroll_eligible": payroll_relationship == "w2",
        "fixed_pay_per_period_cents": fixed_pay_cents,
        "standard_weekly_hours": float(standard_weekly_hours),
    }
    if compensation_changed and (
        not compensation_effective_date or not compensation_reason.strip()
    ):
        employee["compensation_history"] = store.list_compensation_changes(
            employee["email"]
        )
        return HTMLResponse(render_hr_employee_form(
            employee, store.list_teams(), user=user,
            error="Pay changes require an effective date and business reason.",
        ), status_code=422)
    prior_login_email = employee.get("hr_login_email") or ""
    login_ok, login_message = store.set_employee_hr_login_email(
        emp_id, hr_login_email, actor=user.get("email", "system")
    )
    if not login_ok:
        employee["compensation_history"] = store.list_compensation_changes(
            employee["email"]
        )
        return HTMLResponse(render_hr_employee_form(
            employee, store.list_teams(), user=user,
            error={
                "hr_login_email_invalid": "Use a valid email address.",
                "hr_login_email_in_use": "That personal HR sign-in is already assigned to another employee.",
            }.get(login_message, "The personal HR sign-in email could not be saved."),
        ), status_code=422)
    if prior_login_email and prior_login_email != hr_login_email.strip().lower():
        prior_user = access_store.get_user_by_email(prior_login_email)
        if prior_user:
            access_store.set_user_status(prior_user["id"], "suspended")
    store.update_employee(emp_id, full_name=full_name, hr_role=hr_role,
                          employee_type=employee_type, team_id=team_id or None,
                          hourly_rate=hourly_rate,
                          phone=phone, status=status, actor=user.get("email", "system"))
    if status == "inactive":
        for identity in {
            employee["email"], employee.get("hr_login_email") or "",
            hr_login_email.strip().lower(),
        }:
            app_user = access_store.get_user_by_email(identity) if identity else None
            if app_user:
                access_store.set_user_status(app_user["id"], "suspended")
    if employment_changed:
        store.upsert_employment_profile(
            employee["email"], hire_date=hire_date, title=title,
            manager_email=manager_email, classification=classification,
            pay_basis=pay_basis, payroll_eligible=payroll_relationship == "w2",
            fixed_pay_per_period=fixed_pay_per_period,
            standard_weekly_hours=standard_weekly_hours,
            standard_period_hours=86.67, actor=user.get("email", "system"),
        )
    if compensation_changed:
        store.record_compensation_change(
            employee["email"], effective_date=compensation_effective_date,
            prior=prior_compensation, new=new_compensation,
            reason=compensation_reason, actor=user.get("email", "system"),
        )
    return RedirectResponse(f"/admin/hr/employees/{emp_id}?ok=employment_saved", status_code=303)


@router.post("/employees/{emp_id}/status", response_class=HTMLResponse)
async def employee_status_update(
    emp_id: int,
    status: str = Form(...),
    user: dict = Depends(_people_comp_guard),
):
    if status not in {"active", "inactive"}:
        raise HTTPException(status_code=422, detail="Invalid employee status.")
    employee = store.get_employee(emp_id)
    if not employee:
        return RedirectResponse("/admin/hr/employees?err=not_found", status_code=303)
    store.update_employee(
        emp_id,
        status=status,
        actor=user.get("email", "system"),
    )
    if status == "inactive":
        for identity in {employee["email"], employee.get("hr_login_email") or ""}:
            app_user = access_store.get_user_by_email(identity) if identity else None
            if app_user:
                access_store.set_user_status(app_user["id"], "suspended")
    return RedirectResponse(
        f"/admin/hr/employees/{emp_id}?ok=status_saved",
        status_code=303,
    )


@router.post("/employees/{emp_id}/invite", response_class=HTMLResponse)
async def employee_invite(emp_id: int, request: Request, user: dict = Depends(_people_guard)):
    employee = store.get_employee(emp_id)
    if not employee:
        return RedirectResponse("/admin/hr/employees?err=not_found", status_code=303)
    result = store.create_employee_invitation(
        employee["email"], actor=user.get("email", "system")
    )
    if not result.get("ok"):
        return RedirectResponse(f"/admin/hr/employees/{emp_id}?err={result.get('error')}",
                                status_code=303)
    base = str(request.base_url).rstrip("/")
    if "localhost" not in base and "127.0.0.1" not in base:
        base = base.replace("http://", "https://")
    invite_link = f"{base}/admin/access/invite/{result['token']}"
    email_sent = send_invite_email(
        getattr(request.app.state, "agent_settings", None),
        to_email=result["login_email"], invite_link=invite_link,
        invited_by=user.get("email", ""), role_name="HR Employee",
        experience_name="Anata employee app",
    )
    return HTMLResponse(render_hr_invitation(
        invite_link, employee, user=user, email_sent=email_sent
    ))


# --- employee onboarding ---------------------------------------------------

@router.get("/onboarding", response_class=HTMLResponse)
async def employee_onboarding(request: Request, user: dict = Depends(_guard)):
    login_email = (user.get("email") or "").strip().lower()
    employee = store.get_employee_by_email(login_email)
    if not employee:
        return HTMLResponse(
            render_hr_employee_record_missing(user=user),
            status_code=404,
        )
    return HTMLResponse(render_hr_onboarding(
        employee, store.get_onboarding(employee["email"]),
        tax_election=store.get_current_tax_election(employee["email"]),
        user=user, flash=_flash(request)
    ))


@router.post("/onboarding/profile")
async def onboarding_profile(
    personal_email: str = Form(""), phone: str = Form(""), address_line1: str = Form(""),
    address_line2: str = Form(""), city: str = Form(""),
    state: str = Form("UT"), zip_code: str = Form(""),
    emergency_name: str = Form(""), emergency_relationship: str = Form(""),
    emergency_phone: str = Form(""), emergency_email: str = Form(""),
    user: dict = Depends(_guard),
):
    email = _employee_record_email(user)
    ok, message = store.save_employee_profile(
        email, personal_email=personal_email, phone=phone,
        address_line1=address_line1, address_line2=address_line2,
        city=city, state=state, zip_code=zip_code, emergency_name=emergency_name,
        emergency_relationship=emergency_relationship, emergency_phone=emergency_phone,
        emergency_email=emergency_email, actor=email,
    )
    return RedirectResponse(f"/admin/hr/onboarding?{'ok' if ok else 'err'}={message}", status_code=303)


@router.get("/access-training", response_class=HTMLResponse)
async def access_training(
    request: Request, user: dict = Depends(_people_guard)
):
    """Explain the employee access lifecycle without exposing invitation tokens."""
    return HTMLResponse(render_hr_access_training(user=user, flash=_flash(request)))


@router.post("/onboarding/w4")
async def onboarding_w4(
    ssn: str = Form(""), filing_status: str = Form(""),
    two_jobs: bool = Form(False), dependents_credit: str = Form("0"),
    other_income: str = Form("0"), deductions: str = Form("0"),
    extra_withholding: str = Form("0"), exempt: bool = Form(False),
    attested: bool = Form(False),
    user: dict = Depends(_guard),
):
    email = _employee_record_email(user)
    ok, message = store.save_w4(
        email, ssn=ssn, filing_status=filing_status, two_jobs=two_jobs,
        dependents_credit=dependents_credit, other_income=other_income,
        deductions=deductions, extra_withholding=extra_withholding,
        exempt=exempt, attested=attested, actor=email,
    )
    return RedirectResponse(f"/admin/hr/onboarding?{'ok' if ok else 'err'}={message}", status_code=303)


@router.post("/onboarding/attestations")
async def onboarding_attestations(
    i9_attested: bool = Form(False), policies_attested: bool = Form(False),
    user: dict = Depends(_guard),
):
    email = _employee_record_email(user)
    ok, message = store.save_employee_attestations(
        email, i9_attested=i9_attested, policies_attested=policies_attested,
        actor=email,
    )
    if ok and policies_attested:
        store.acknowledge_current_policy(email, actor=email, attested=True)
    return RedirectResponse(f"/admin/hr/onboarding?{'ok' if ok else 'err'}={message}", status_code=303)


@router.post("/employees/{emp_id}/onboarding-review")
async def onboarding_employer_review(
    emp_id: int, i9_document_type: str = Form(""),
    i9_verified_date: date = Form(...), i9_expiration_date: date | None = Form(None),
    user: dict = Depends(_people_guard),
):
    employee = store.get_employee(emp_id)
    if not employee:
        return RedirectResponse("/admin/hr/employees?err=not_found", status_code=303)
    ok, message = store.complete_employer_onboarding(
        employee["email"], i9_document_type=i9_document_type,
        i9_verified_date=i9_verified_date, i9_expiration_date=i9_expiration_date,
        actor=user.get("email", "system"),
    )
    return RedirectResponse(f"/admin/hr/employees/{emp_id}?{'ok' if ok else 'err'}={message}",
                            status_code=303)


@router.post("/employees/{emp_id}/onboarding-correction")
async def onboarding_correction_request(
    emp_id: int, reason: str = Form(""), user: dict = Depends(_people_guard),
):
    employee = store.get_employee(emp_id)
    if not employee:
        return RedirectResponse("/admin/hr/employees?err=not_found", status_code=303)
    ok, message = store.request_onboarding_correction(
        employee["email"], reason=reason, actor=user.get("email", "system"),
    )
    return RedirectResponse(
        f"/admin/hr/employees/{emp_id}?{'ok' if ok else 'err'}={message}",
        status_code=303,
    )


# --- teams -----------------------------------------------------------------

@router.get("/teams", response_class=HTMLResponse)
async def teams_list(request: Request, user: dict = Depends(_people_guard)):
    return HTMLResponse(render_hr_teams(store.list_teams(), user=user, flash=_flash(request)))


@router.post("/teams", response_class=HTMLResponse)
async def team_create(
    request: Request,
    name: str = Form(""),
    manager_email: str = Form(""),
    description: str = Form(""),
    user: dict = Depends(_people_guard),
):
    team_id = store.create_team(
        name=name, manager_email=manager_email, description=description,
        actor=user.get("email", "system"),
    )
    return RedirectResponse(
        "/admin/hr/teams?ok=team_created" if team_id else
        "/admin/hr/teams?err=team_name_or_manager_invalid",
        status_code=303,
    )


@router.get("/teams/{team_id}", response_class=HTMLResponse)
async def team_detail(
    team_id: int, request: Request, user: dict = Depends(_people_guard)
):
    team = store.get_team(team_id)
    if not team:
        return RedirectResponse("/admin/hr/teams?err=team_not_found", status_code=303)
    return HTMLResponse(render_hr_team_detail(
        team, store.list_employees(), user=user, flash=_flash(request)
    ))


@router.post("/teams/{team_id}")
async def team_update(
    team_id: int, name: str = Form(""), manager_email: str = Form(""),
    description: str = Form(""), user: dict = Depends(_people_guard),
):
    ok, message = store.update_team(
        team_id, name=name, manager_email=manager_email,
        description=description, actor=user.get("email", "system"),
    )
    return RedirectResponse(
        f"/admin/hr/teams/{team_id}?{'ok' if ok else 'err'}={message}",
        status_code=303,
    )


@router.post("/teams/{team_id}/members")
async def team_member_update(
    team_id: int, employee_id: int = Form(...), action: str = Form("assign"),
    user: dict = Depends(_people_guard),
):
    target_team = None if action == "remove" else team_id
    ok, message = store.set_employee_team(
        employee_id, team_id=target_team, actor=user.get("email", "system"),
        expected_current_team_id=team_id if action == "remove" else None,
    )
    return RedirectResponse(
        f"/admin/hr/teams/{team_id}?{'ok' if ok else 'err'}={message}",
        status_code=303,
    )


# --- time and PTO ----------------------------------------------------------

@router.get("/time", response_class=HTMLResponse)
async def hr_time(
    request: Request, period_date: date | None = None, page: int = 1,
    page_size: int = 10, user: dict = Depends(_guard)
):
    email = _employee_record_email(user)
    can_review = bool(
        user.get("is_superadmin")
        or {"hr.payroll", "hr.time.approve_team"}.intersection(
            user.get("permissions") or set()
        )
    )
    period = payroll_store.semimonthly_period(period_date or date.today())
    managed = _managed_employee_emails(user) if can_review else {email}

    def scoped(items: list[dict]) -> list[dict]:
        return [
            item for item in items
            if item.get("employee_email") in managed
        ]

    scoped_entries = [
        item for item in scoped(store.list_time_entries(None, limit=500))
        if period.start_date <= item.get("date") <= period.end_date
    ]
    page_size = page_size if page_size in {10, 25} else 10
    page_count = max(1, (len(scoped_entries) + page_size - 1) // page_size)
    page = max(1, min(page, page_count))
    entry_page = scoped_entries[(page - 1) * page_size:page * page_size]
    corrections = scoped(store.list_time_corrections(None))
    for correction in corrections:
        employee = store.get_employee_by_email(correction.get("employee_email") or "")
        original_hours = _correction_duration(correction.get("original") or {})
        proposed_hours = _correction_duration(correction.get("proposed") or {})
        correction["hours_delta"] = round(proposed_hours - original_hours, 4)
        correction["work_date"] = (
            (correction.get("proposed") or {}).get("date")
            or (correction.get("original") or {}).get("date")
        )
        correction["estimated_gross_impact"] = (
            round(correction["hours_delta"] * float(employee.get("hourly_rate") or 0), 2)
            if employee and int(employee.get("hourly_rate_cents") or 0) > 0
            else None
        )
    return HTMLResponse(render_hr_time(
        entry_page, store.pto_summary(email),
        scoped(store.list_pto_requests(None)), store.current_clock(email),
        corrections,
        scoped(store.time_review_flags(
            None, start_date=period.start_date, end_date=period.end_date
        )),
        scoped(store.list_timesheet_approvals(
            period.start_date, period.end_date, None
        )),
        period, clock_summary=store.time_clock_summary(email),
        entry_total=len(scoped_entries), entry_page=page,
        entry_page_size=page_size, entry_page_count=page_count,
        user=user, flash=_flash(request)))


@router.post("/time/clock")
async def hr_time_clock(action: str = Form(""), user: dict = Depends(_guard)):
    email = _employee_record_email(user)
    ok, message = (store.clock_out(email, actor=email) if action == "out"
                   else store.clock_in(email, actor=email))
    key = "ok" if ok else "err"
    return RedirectResponse(f"/admin/hr/time?{key}={message}", status_code=303)


@router.post("/time/{time_entry_id}/correction")
async def hr_time_correction(
    time_entry_id: int, proposed_start: str = Form(""),
    proposed_stop: str = Form(""), reason: str = Form(""),
    user: dict = Depends(_guard),
):
    actor = _employee_record_email(user)
    entries = store.list_time_entries(None, limit=500)
    entry = next(
        (item for item in entries if int(item.get("id") or 0) == time_entry_id),
        None,
    )
    if not entry:
        return RedirectResponse(
            "/admin/hr/time?err=correction_not_found", status_code=303
        )
    target_email = (entry.get("employee_email") or "").strip().lower()
    if target_email != actor:
        if not _can_review_time(user):
            raise HTTPException(
                status_code=403, detail="You cannot correct another employee's time."
            )
        _require_team_record(user, entries, time_entry_id)
    ok, message = store.request_time_correction(
        time_entry_id, employee_email=target_email, proposed_start=proposed_start,
        proposed_stop=proposed_stop, reason=reason, actor=actor,
    )
    return RedirectResponse(f"/admin/hr/time?{'ok' if ok else 'err'}={message}", status_code=303)


@router.post("/time/missed-punch")
async def hr_time_missed_punch(
    work_date: date = Form(...), proposed_start: str = Form(""),
    proposed_stop: str = Form(""), reason: str = Form(""),
    user: dict = Depends(_guard),
):
    email = _employee_record_email(user)
    ok, message = store.request_missed_punch(
        email, work_date=work_date, proposed_start=proposed_start,
        proposed_stop=proposed_stop, reason=reason, actor=email,
    )
    return RedirectResponse(
        f"/admin/hr/time?{'ok' if ok else 'err'}={message}", status_code=303
    )


@router.post("/time/corrections/{correction_id}/decision")
async def hr_time_correction_decision(
    correction_id: int, decision: str = Form(""),
    reviewer_reason: str = Form(""), user: dict = Depends(_time_review_guard),
):
    actor = (user.get("email") or "").strip().lower()
    _require_team_record(
        user, store.list_time_corrections(None), correction_id
    )
    ok, message = store.decide_time_correction(
        correction_id, decision=decision, reviewer_reason=reviewer_reason, actor=actor,
    )
    return RedirectResponse(f"/admin/hr/time?{'ok' if ok else 'err'}={message}", status_code=303)


@router.post("/time/timesheets/submit")
async def hr_timesheet_submit(
    period_start: date = Form(...), period_end: date = Form(...),
    attested: bool = Form(False), user: dict = Depends(_guard),
):
    email = _employee_record_email(user)
    if not attested:
        ok, message = False, "timesheet_attestation_required"
    else:
        ok, message = store.submit_timesheet(
            email, period_start=period_start, period_end=period_end, actor=email
        )
    return RedirectResponse(
        f"/admin/hr/time?period_date={period_start}&{'ok' if ok else 'err'}={message}",
        status_code=303,
    )


@router.post("/time/timesheets/{approval_id}/decision")
async def hr_timesheet_decision(
    approval_id: int, period_start: date = Form(...),
    decision: str = Form(""), review_note: str = Form(""),
    user: dict = Depends(_time_review_guard),
):
    period = payroll_store.semimonthly_period(period_start)
    _require_team_record(
        user,
        store.list_timesheet_approvals(period.start_date, period.end_date, None),
        approval_id,
    )
    ok, message = store.decide_timesheet(
        approval_id, decision=decision, review_note=review_note,
        actor=user.get("email", ""),
    )
    return RedirectResponse(
        f"/admin/hr/time?period_date={period_start}&{'ok' if ok else 'err'}={message}",
        status_code=303,
    )


@router.post("/time/pto/{request_id}/decision")
async def hr_pto_decision(request: Request, request_id: int, decision: str = Form(""),
                          user: dict = Depends(_time_review_guard)):
    actor = (user.get("email") or "").strip().lower()
    _require_team_record(user, store.list_pto_requests(None), request_id)
    ok = store.decide_pto(request_id, decision=decision, actor=actor)
    message = "updated"
    if ok:
        settings = getattr(request.app.state, "agent_settings", None)
        pto_workflow.notify_employee(
            settings, request_id=request_id, base_url=str(request.base_url)
        )
        if decision == "approved":
            synced, sync_message = pto_workflow.sync_approved_request(request_id)
            message = "pto_approved_calendar_synced" if synced else f"pto_approved_{sync_message}"
    return RedirectResponse(f"/admin/hr/time?{'ok=' + message if ok else 'err=invalid_request'}",
                            status_code=303)


@router.post("/time/pto/{request_id}/withdraw")
async def hr_pto_withdraw(request_id: int, user: dict = Depends(_guard)):
    email = _employee_record_email(user)
    ok, message = store.withdraw_pto(
        request_id, employee_email=email, actor=email
    )
    return RedirectResponse(
        f"/admin/hr/time?{'ok' if ok else 'err'}={message}", status_code=303
    )


@router.post("/time/pto/{request_id}/calendar-sync")
async def hr_pto_calendar_sync(request_id: int, user: dict = Depends(_time_review_guard)):
    _require_team_record(user, store.list_pto_requests(None), request_id)
    item = store.get_pto_request(request_id)
    ok, message = (
        pto_workflow.sync_revoked_request(request_id)
        if item and item.get("status") == "revoked"
        else pto_workflow.sync_approved_request(request_id)
    )
    return RedirectResponse(
        f"/admin/hr/time?{'ok' if ok else 'err'}=pto_calendar_{message}",
        status_code=303,
    )


@router.post("/time/pto/{request_id}/notify-reviewer")
async def hr_pto_notify_reviewer(
    request: Request, request_id: int, user: dict = Depends(_time_review_guard)
):
    _require_team_record(user, store.list_pto_requests(None), request_id)
    item = store.get_pto_request(request_id)
    if not item or item.get("status") != "pending":
        return RedirectResponse(
            "/admin/hr/time?err=pto_notification_not_allowed", status_code=303
        )
    sent = pto_workflow.notify_reviewer(
        getattr(request.app.state, "agent_settings", None),
        request_id=request_id, base_url=str(request.base_url),
    )
    return RedirectResponse(
        "/admin/hr/time?" + (
            "ok=pto_manager_notified" if sent else "err=pto_notification_failed"
        ), status_code=303,
    )


@router.post("/time/pto/{request_id}/revoke")
async def hr_pto_revoke(
    request: Request, request_id: int, reason: str = Form(""),
    user: dict = Depends(_time_review_guard),
):
    _require_team_record(user, store.list_pto_requests(None), request_id)
    actor = (user.get("email") or "").strip().lower()
    ok, message = store.revoke_pto(
        request_id, reason=reason, actor=actor
    )
    if ok:
        pto_workflow.sync_revoked_request(request_id)
        pto_workflow.notify_employee(
            getattr(request.app.state, "agent_settings", None),
            request_id=request_id, base_url=str(request.base_url),
        )
    return RedirectResponse(
        f"/admin/hr/time?{'ok' if ok else 'err'}={message}", status_code=303
    )


@router.post("/time/pto")
async def hr_pto_request(request: Request, start_date: date = Form(...), end_date: date = Form(...),
                         hours: float = Form(...), reason: str = Form(""),
                         user: dict = Depends(_guard)):
    email = _employee_record_email(user)
    ok, message = store.create_pto_request(email, start_date=start_date, end_date=end_date,
                                           hours=hours, reason=reason, actor=email)
    if ok:
        request_id = store.latest_pto_request_id(email)
        if request_id:
            sent = pto_workflow.notify_reviewer(
                getattr(request.app.state, "agent_settings", None),
                request_id=request_id, base_url=str(request.base_url),
            )
            message = "pto_requested_manager_notified" if sent else "pto_requested_notification_pending"
    key = "ok" if ok else "err"
    return RedirectResponse(f"/admin/hr/time?{key}={message}", status_code=303)


@router.get("/reports", response_class=HTMLResponse)
async def hr_reports(request: Request, user: dict = Depends(_reports_guard)):
    store.audit_sensitive_read(
        user.get("email", ""), scope="hr_reports", purpose="reports page"
    )
    return HTMLResponse(render_hr_reports(user=user))


@router.get("/reports/{kind}.csv")
async def hr_report_csv(
    kind: str,
    year: int | None = None,
    quarter: int | None = None,
    user: dict = Depends(_reports_guard),
):
    content = reporting.export_csv(kind, year=year, quarter=quarter)
    if content is None:
        return PlainTextResponse("Unknown HR export.", status_code=404)
    store.audit_sensitive_read(
        user.get("email", ""), scope="hr_export", entity_id=kind,
        purpose="CSV download",
    )
    suffix = ""
    if year:
        suffix += f"-{year}"
    if quarter:
        suffix += f"-q{quarter}"
    return PlainTextResponse(
        content, media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="anata-hr-{kind}{suffix}.csv"',
                 "Cache-Control": "no-store"},
    )


@router.get("/reports/backup.zip")
async def hr_backup_zip(
    year: int | None = None, user: dict = Depends(_reports_guard)
):
    report_year = year or date.today().year
    store.audit_sensitive_read(
        user.get("email", ""), scope="hr_backup", entity_id=report_year,
        purpose="verified backup download",
    )
    content = reporting.export_backup_zip(year=report_year)
    return Response(
        content,
        media_type="application/zip",
        headers={
            "Content-Disposition":
                f'attachment; filename="anata-hr-backup-{report_year}.zip"',
            "Cache-Control": "no-store",
            "X-Content-Type-Options": "nosniff",
        },
    )


@router.get("/compliance", response_class=HTMLResponse)
async def hr_compliance(
    request: Request, year: int = date.today().year,
    user: dict = Depends(_pay_view_guard),
):
    safe_year = min(max(year, 2026), date.today().year + 2)
    store.ensure_annual_compliance_tasks(safe_year)
    return HTMLResponse(render_hr_compliance(
        store.list_compliance_tasks(),
        payroll_store.annual_payroll_calendar(safe_year),
        year=safe_year, user=user, flash=_flash(request),
    ))


@router.post("/compliance/{task_id}")
async def hr_compliance_update(
    task_id: int, action: str = Form(""),
    confirmation_reference: str = Form(""), evidence_note: str = Form(""),
    user: dict = Depends(_pay_submit_guard),
    _rate_limit: None = Depends(_sensitive_rate_limit),
):
    ok, message = store.record_compliance_task(
        task_id, action=action, confirmation_reference=confirmation_reference,
        evidence_note=evidence_note, actor=user.get("email", ""),
    )
    return RedirectResponse(
        f"/admin/hr/compliance?{'ok' if ok else 'err'}={message}",
        status_code=303,
    )


@router.get("/policies", response_class=HTMLResponse)
async def hr_policies(request: Request, user: dict = Depends(_guard)):
    email = _employee_record_email(user)
    return HTMLResponse(render_hr_policies(
        store.current_policy(email), user=user, flash=_flash(request)
    ))


@router.post("/policies/acknowledge")
async def hr_policy_acknowledge(
    attested: bool = Form(False), user: dict = Depends(_guard),
):
    email = _employee_record_email(user)
    ok, message = store.acknowledge_current_policy(
        email, actor=email, attested=attested
    )
    return RedirectResponse(
        f"/admin/hr/policies?{'ok' if ok else 'err'}={message}", status_code=303
    )


@router.get("/payroll", response_class=HTMLResponse)
async def hr_payroll(request: Request, period_date: date | None = None,
                     user: dict = Depends(_pay_view_guard)):
    return HTMLResponse(render_hr_payroll_control(
        payroll_store.control_room(period_date or _default_payroll_date()),
        user=user, flash=_flash(request),
    ))


@router.get("/contractors", response_class=HTMLResponse)
async def hr_contractors(request: Request, user: dict = Depends(_pay_prepare_guard)):
    contractors = [
        row for row in store.list_employees()
        if row.get("employee_type") == "contractor"
    ]
    return HTMLResponse(render_hr_contractors(
        contractors, workforce.list_contractor_profiles(),
        workforce.list_contractor_payments(),
        user=user, flash=_flash(request),
    ))


@router.post("/contractors/profile")
async def hr_contractor_profile_save(
    contractor_email: str = Form(""), country_code: str = Form(""),
    engagement_start: date = Form(...), engagement_end: date | None = Form(None),
    flat_fee: str = Form(""), currency: str = Form("USD"),
    fee_terms: str = Form(""), contract_reference: str = Form(""),
    engagement_status: str = Form("active"),
    tax_form_type: str = Form("undetermined"),
    tax_form_status: str = Form("missing"), received_date: date | None = Form(None),
    expiration_date: date | None = Form(None),
    wise_recipient_reference: str = Form(""), review_note: str = Form(""),
    user: dict = Depends(_pay_prepare_guard),
):
    ok, message = workforce.save_contractor_profile(
        contractor_email=contractor_email, country_code=country_code,
        engagement_start=engagement_start, engagement_end=engagement_end,
        flat_fee=flat_fee, currency=currency, fee_terms=fee_terms,
        contract_reference=contract_reference, engagement_status=engagement_status,
        tax_form_type=tax_form_type,
        tax_form_status=tax_form_status, received_date=received_date,
        expiration_date=expiration_date,
        wise_recipient_reference=wise_recipient_reference,
        review_note=review_note, actor=user.get("email", ""),
    )
    return RedirectResponse(
        f"/admin/hr/contractors?{'ok' if ok else 'err'}={message}", status_code=303
    )


@router.post("/contractors/payments")
async def hr_contractor_payment_create(
    contractor_email: str = Form(""), service_start: date = Form(...),
    service_end: date = Form(...), due_date: date = Form(...),
    amount: str = Form(""), currency: str = Form("USD"),
    description: str = Form(""), invoice_reference: str = Form(""),
    user: dict = Depends(_pay_prepare_guard),
):
    ok, message = workforce.create_contractor_payment(
        contractor_email=contractor_email, service_start=service_start,
        service_end=service_end, due_date=due_date, amount=amount,
        currency=currency, description=description,
        invoice_reference=invoice_reference, actor=user.get("email", ""),
    )
    return RedirectResponse(
        f"/admin/hr/contractors?{'ok' if ok else 'err'}={message}", status_code=303
    )


@router.post("/contractors/payments/{payment_id}")
async def hr_contractor_payment_action(
    payment_id: int, action: str = Form(""), wise_reference: str = Form(""),
    evidence_note: str = Form(""), user: dict = Depends(_pay_submit_guard),
):
    ok, message = workforce.contractor_payment_action(
        payment_id, action=action, wise_reference=wise_reference,
        evidence_note=evidence_note, actor=user.get("email", ""),
    )
    return RedirectResponse(
        f"/admin/hr/contractors?{'ok' if ok else 'err'}={message}", status_code=303
    )


@router.get("/offboarding", response_class=HTMLResponse)
async def hr_offboarding(request: Request, user: dict = Depends(_people_guard)):
    return HTMLResponse(render_hr_offboarding(
        store.list_employees(), workforce.list_offboarding(),
        user=user, flash=_flash(request),
    ))


@router.post("/offboarding")
async def hr_offboarding_create(
    employee_email: str = Form(""), separation_type: str = Form(""),
    last_working_day: date = Form(...), final_pay_date: date = Form(...),
    reason: str = Form(""), user: dict = Depends(_people_guard),
):
    ok, message = workforce.create_offboarding(
        employee_email=employee_email, separation_type=separation_type,
        last_working_day=last_working_day, final_pay_date=final_pay_date,
        reason=reason, actor=user.get("email", ""),
    )
    return RedirectResponse(
        f"/admin/hr/offboarding?{'ok' if ok else 'err'}={message}", status_code=303
    )


@router.post("/offboarding/{checklist_id}")
async def hr_offboarding_update(
    checklist_id: int, completed_steps: list[str] = Form(default=[]),
    final_pay_reference: str = Form(""),
    final_pay_evidence_note: str = Form(""),
    user: dict = Depends(_people_guard),
):
    ok, message = workforce.update_offboarding(
        checklist_id, completed_steps=completed_steps,
        final_pay_reference=final_pay_reference,
        final_pay_evidence_note=final_pay_evidence_note,
        actor=user.get("email", ""),
    )
    return RedirectResponse(
        f"/admin/hr/offboarding?{'ok' if ok else 'err'}={message}", status_code=303
    )


@router.post("/payroll/inputs")
async def hr_payroll_input(
    period_date: date = Form(...), employee_email: str = Form(""),
    input_type: str = Form(""), amount: str = Form(""),
    taxable: bool = Form(False), description: str = Form(""),
    source_reference: str = Form(""), recurring: bool = Form(False),
    user: dict = Depends(_pay_prepare_guard),
):
    period = payroll_store.semimonthly_period(period_date)
    ok, message = payroll_store.add_payroll_input(
        employee_email=employee_email, period_start=period.start_date,
        period_end=period.end_date, input_type=input_type, amount=amount,
        taxable=taxable, description=description, source_reference=source_reference,
        recurring=recurring, actor=user.get("email", ""),
    )
    return RedirectResponse(
        f"/admin/hr/payroll?period_date={period_date}&{'ok' if ok else 'err'}={message}",
        status_code=303,
    )


@router.post("/payroll/inputs/{input_id}/decision")
async def hr_payroll_input_decision(
    input_id: int, period_date: date = Form(...), decision: str = Form(""),
    user: dict = Depends(_pay_prepare_guard),
):
    ok, message = payroll_store.decide_payroll_input(
        input_id, decision=decision, actor=user.get("email", "")
    )
    return RedirectResponse(
        f"/admin/hr/payroll?period_date={period_date}&{'ok' if ok else 'err'}={message}",
        status_code=303,
    )


@router.post("/payroll/prepare")
async def hr_payroll_prepare(period_date: date = Form(...),
                             user: dict = Depends(_pay_prepare_guard)):
    ok, message = payroll_store.prepare_payroll(
        period_date, actor=user.get("email", "")
    )
    return RedirectResponse(
        f"/admin/hr/payroll?period_date={period_date}&{'ok' if ok else 'err'}={message}",
        status_code=303,
    )


@router.post("/payroll/{run_id}/approve")
async def hr_payroll_approve(
    run_id: str, period_date: date = Form(...), approval_text: str = Form(""),
    user: dict = Depends(_recent_pay_approve_guard),
    _rate_limit: None = Depends(_sensitive_rate_limit),
):
    ok, message = payroll_store.approve_payroll(
        run_id, actor=user.get("email", ""), approval_text=approval_text
    )
    return RedirectResponse(
        f"/admin/hr/payroll?period_date={period_date}&{'ok' if ok else 'err'}={message}",
        status_code=303,
    )


@router.post("/payroll/{run_id}/reject")
async def hr_payroll_reject(
    run_id: str, period_date: date = Form(...), reason: str = Form(""),
    user: dict = Depends(_recent_pay_approve_guard),
    _rate_limit: None = Depends(_sensitive_rate_limit),
):
    ok, message = payroll_store.reject_payroll(
        run_id, actor=user.get("email", ""), reason=reason
    )
    return RedirectResponse(
        f"/admin/hr/payroll?period_date={period_date}&"
        f"{'ok' if ok else 'err'}={message}",
        status_code=303,
    )


@router.get("/payroll/runs/{run_id}/approve", response_class=HTMLResponse)
async def hr_payroll_approval_review(
    run_id: str, request: Request, user: dict = Depends(_pay_approve_guard),
):
    run = payroll_store.payroll_run_detail(
        run_id, actor=user.get("email", "")
    )
    if not run:
        return RedirectResponse("/admin/hr/payroll?err=run_not_found", status_code=303)
    if run["status"] != "prepared":
        return RedirectResponse(
            f"/admin/hr/payroll/runs/{run_id}?err=payroll_not_prepared",
            status_code=303,
        )
    return HTMLResponse(
        render_hr_payroll_approval(run, user=user, flash=_flash(request))
    )


@router.post("/payroll/liabilities/{liability_id}")
async def hr_payroll_liability_action(
    liability_id: int, period_date: date = Form(...), action: str = Form(""),
    confirmation_number: str = Form(""), confirmed_amount: str = Form(""),
    filing_confirmation_number: str = Form(""),
    evidence_note: str = Form(""),
    user: dict = Depends(_pay_submit_guard),
    _rate_limit: None = Depends(_sensitive_rate_limit),
):
    ok, message = payroll_store.record_liability_action(
        liability_id, action=action, confirmation_number=confirmation_number,
        filing_confirmation_number=filing_confirmation_number,
        confirmed_amount=confirmed_amount, evidence_note=evidence_note,
        actor=user.get("email", ""),
    )
    return RedirectResponse(
        f"/admin/hr/payroll?period_date={period_date}&{'ok' if ok else 'err'}={message}",
        status_code=303,
    )


@router.get("/payroll/runs/{run_id}", response_class=HTMLResponse)
async def hr_payroll_run_review(
    run_id: str, request: Request, user: dict = Depends(_pay_view_guard),
):
    run = payroll_store.payroll_run_detail(
        run_id, actor=user.get("email", "")
    )
    if not run:
        return RedirectResponse("/admin/hr/payroll?err=run_not_found", status_code=303)
    return HTMLResponse(render_hr_payroll_run(run, user=user, flash=_flash(request)))


@router.get("/payroll/runs/{run_id}/provider.csv")
async def hr_payroll_provider_export(
    run_id: str, user: dict = Depends(_pay_view_guard)
):
    content = reporting.payroll_provider_csv(run_id)
    if content is None:
        return PlainTextResponse("Approved payroll not found.", status_code=404)
    store.audit_sensitive_read(
        user.get("email", ""), scope="provider_handoff", entity_id=run_id,
        purpose="provider handoff download",
    )
    return PlainTextResponse(
        content,
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition":
                f'attachment; filename="anata-payroll-provider-{run_id}.csv"',
            "Cache-Control": "no-store",
        },
    )


@router.post("/payroll/runs/{run_id}/provider")
async def hr_payroll_provider_action(
    run_id: str, action: str = Form(""), provider_name: str = Form(""),
    provider_reference: str = Form(""), gross: str = Form(""),
    net: str = Form(""), taxes: str = Form(""),
    employer_cost: str = Form(""), evidence_note: str = Form(""),
    user: dict = Depends(_pay_submit_guard),
    _rate_limit: None = Depends(_sensitive_rate_limit),
):
    ok, message = payroll_store.record_provider_handoff(
        run_id, action=action, provider_name=provider_name,
        provider_reference=provider_reference, gross=gross, net=net,
        taxes=taxes, employer_cost=employer_cost,
        evidence_note=evidence_note, actor=user.get("email", ""),
    )
    return RedirectResponse(
        f"/admin/hr/payroll/runs/{run_id}?{'ok' if ok else 'err'}={message}",
        status_code=303,
    )


@router.post("/payroll/runs/{run_id}/checks")
async def hr_payroll_issue_check(
    run_id: str, employee_email: str = Form(""), check_number: str = Form(""),
    user: dict = Depends(_pay_submit_guard),
    _rate_limit: None = Depends(_sensitive_rate_limit),
):
    ok, message = payroll_store.issue_printed_check(
        run_id, employee_email=employee_email, check_number=check_number,
        actor=user.get("email", ""),
    )
    return RedirectResponse(
        f"/admin/hr/payroll/runs/{run_id}?{'ok' if ok else 'err'}={message}",
        status_code=303,
    )


@router.post("/payroll/runs/{run_id}/checks/confirm")
async def hr_payroll_confirm_check(
    run_id: str, employee_email: str = Form(""),
    confirmation_reference: str = Form(""),
    evidence_note: str = Form(""),
    user: dict = Depends(_pay_submit_guard),
    _rate_limit: None = Depends(_sensitive_rate_limit),
):
    ok, message = payroll_store.confirm_printed_check(
        run_id, employee_email=employee_email,
        confirmation_reference=confirmation_reference,
        evidence_note=evidence_note, actor=user.get("email", ""),
    )
    return RedirectResponse(
        f"/admin/hr/payroll/runs/{run_id}?{'ok' if ok else 'err'}={message}",
        status_code=303,
    )


@router.post("/payroll/runs/{run_id}/checks/reissue")
async def hr_payroll_reissue_check(
    run_id: str, employee_email: str = Form(""), reason: str = Form(""),
    new_check_number: str = Form(""), user: dict = Depends(_pay_approve_guard),
    _rate_limit: None = Depends(_sensitive_rate_limit),
):
    ok, message = payroll_store.void_and_reissue_check(
        run_id, employee_email=employee_email, reason=reason,
        new_check_number=new_check_number, actor=user.get("email", ""),
    )
    return RedirectResponse(
        f"/admin/hr/payroll/runs/{run_id}?{'ok' if ok else 'err'}={message}",
        status_code=303,
    )


@router.post("/payroll/runs/{run_id}/close")
async def hr_payroll_close_run(
    run_id: str, user: dict = Depends(_pay_submit_guard),
    _rate_limit: None = Depends(_sensitive_rate_limit),
):
    ok, message = payroll_store.close_payroll_run(
        run_id, actor=user.get("email", "")
    )
    return RedirectResponse(
        f"/admin/hr/payroll/runs/{run_id}?{'ok' if ok else 'err'}={message}",
        status_code=303,
    )


@router.get("/pay-statements", response_class=HTMLResponse)
async def hr_pay_statements(user: dict = Depends(_guard)):
    email = _employee_record_email(user)
    runs = payroll_store.employee_pay_statements(email)
    return HTMLResponse(render_hr_pay_statements(runs, user=user))


@router.get("/pay-statements/{run_id}", response_class=HTMLResponse)
async def hr_pay_statement_detail(run_id: str, request: Request,
                                  user: dict = Depends(_guard)):
    email = _employee_record_email(user)
    run = payroll_store.payroll_run_detail(run_id, employee_email=email)
    if not run or not run["calculations"] or run["status"] not in {"checks_issued", "closed"}:
        return RedirectResponse("/admin/hr/pay-statements", status_code=303)
    return HTMLResponse(render_hr_payroll_run(
        run, user=user, employee_view=True, flash=_flash(request)
    ))


@router.get("/settings", response_class=HTMLResponse)
async def hr_settings(request: Request, user: dict = Depends(_settings_guard)):
    store.audit_sensitive_read(
        user.get("email", ""), scope="payroll_settings",
        purpose="tax and opening balance review",
    )
    return HTMLResponse(render_hr_settings(
        payroll_store.get_payroll_settings(), payroll_store.get_company_profile(),
        _ooo_calendar_readiness(),
        store.list_employees(),
        [
            account for account in access_store.list_users()
            if account.get("status") == "active"
            and "hr.payroll.approve" in account.get("permissions", set())
        ],
        payroll_store.list_opening_balances(2026),
        store.list_handbooks(),
        user=user, flash=_flash(request)
    ))


@router.get("/settings/provider-contract.json")
async def hr_provider_contract(user: dict = Depends(_settings_guard)):
    """Authenticated contract handoff for the internal payroll team."""

    store.audit_sensitive_read(
        user.get("email", ""),
        scope="payroll_provider_contract",
        purpose="internal payroll integration contract download",
    )
    return JSONResponse(
        contract_descriptor(),
        headers={
            "Cache-Control": "no-store",
            "X-Content-Type-Options": "nosniff",
            "Content-Disposition": (
                'attachment; filename="anata-internal-payroll-contract-2026-07-23.json"'
            ),
        },
    )


@router.get("/settings/legacy-import", response_class=HTMLResponse)
async def hr_legacy_import_page(user: dict = Depends(_settings_guard)):
    return HTMLResponse(render_hr_legacy_import(user=user))


async def _read_legacy_archive(archive: UploadFile) -> bytes:
    if not archive.filename or not archive.filename.lower().endswith(".zip"):
        raise legacy_import.LegacyImportError(
            "Choose the Base44 HR data-table ZIP."
        )
    payload = await archive.read(legacy_import.MAX_ARCHIVE_BYTES + 1)
    if len(payload) > legacy_import.MAX_ARCHIVE_BYTES:
        raise legacy_import.LegacyImportError(
            "The selected ZIP is larger than the safe import limit."
        )
    return payload


@router.post("/settings/legacy-import/preview", response_class=HTMLResponse)
async def hr_legacy_import_preview(
    archive: UploadFile = File(...),
    user: dict = Depends(_settings_guard),
    _rate_limit: None = Depends(_sensitive_rate_limit),
):
    try:
        preview = legacy_import.preview_legacy_export(
            await _read_legacy_archive(archive)
        )
    except legacy_import.LegacyImportError as exc:
        return HTMLResponse(
            render_hr_legacy_import(user=user, error=str(exc)), status_code=422
        )
    store.audit_sensitive_read(
        user.get("email", ""), scope="legacy_hr_export",
        entity_id=preview["digest"][:16], purpose="Base44 recovery preview",
    )
    return HTMLResponse(render_hr_legacy_import(user=user, preview=preview))


@router.post("/settings/legacy-import/commit", response_class=HTMLResponse)
async def hr_legacy_import_commit(
    archive: UploadFile = File(...),
    expected_digest: str = Form(""),
    attested: bool = Form(False),
    user: dict = Depends(_recent_settings_guard),
    _rate_limit: None = Depends(_sensitive_rate_limit),
):
    try:
        result = legacy_import.import_legacy_export(
            await _read_legacy_archive(archive),
            actor=user.get("email", ""),
            expected_digest=expected_digest,
            attested=attested,
        )
    except legacy_import.LegacyImportError as exc:
        return HTMLResponse(
            render_hr_legacy_import(user=user, error=str(exc)), status_code=422
        )
    return HTMLResponse(render_hr_legacy_import(user=user, result=result))


@router.post("/settings")
async def hr_settings_save(
    utah_ui_rate: str = Form(""), qualified_tax_review: bool = Form(False),
    eftps_ready: bool = Form(False), utah_tap_ready: bool = Form(False),
    utah_ui_ready: bool = Form(False),
    opening_balances_confirmed: bool = Form(False),
    opening_balance_note: str = Form(""), user: dict = Depends(_settings_guard),
):
    try:
        payroll_store.save_payroll_settings(
            utah_ui_rate=utah_ui_rate, qualified_tax_review=qualified_tax_review,
            eftps_ready=eftps_ready, utah_tap_ready=utah_tap_ready,
            utah_ui_ready=utah_ui_ready,
            opening_balances_confirmed=opening_balances_confirmed,
            opening_balance_note=opening_balance_note,
            actor=user.get("email", ""),
        )
    except ValueError:
        return RedirectResponse("/admin/hr/settings?err=invalid_input", status_code=303)
    return RedirectResponse("/admin/hr/settings?ok=settings_saved", status_code=303)


@router.post("/settings/ooo-calendar/test")
async def hr_ooo_calendar_test(
    user: dict = Depends(_settings_guard),
    _rate_limit: None = Depends(_sensitive_rate_limit),
):
    ok, state, _message = pto_workflow.test_calendar_connection()
    calendar = pto_workflow.calendar_readiness()
    payroll_store.audit_hr_event(
        actor=user.get("email", ""), action="ooo_calendar.connection_tested",
        entity_type="calendar", entity_id="anata_ooo",
        details={
            "state": state, "ready": ok,
            "calendar_id": calendar.get("calendar_id") or "",
            "service_account_email": calendar.get("service_account_email") or "",
        },
    )
    return RedirectResponse(
        f"/admin/hr/settings?{'ok' if ok else 'err'}=calendar_{state}#ooo-calendar",
        status_code=303,
    )


@router.post("/settings/handbook")
async def hr_handbook_publish(
    title: str = Form(""),
    version: str = Form(""),
    file_url: str = Form(""),
    attested: bool = Form(False),
    user: dict = Depends(_settings_guard),
    _rate_limit: None = Depends(_sensitive_rate_limit),
):
    ok, message = store.publish_handbook(
        title=title,
        version=version,
        file_url=file_url,
        actor=user.get("email", ""),
        attested=attested,
    )
    return RedirectResponse(
        f"/admin/hr/settings?{'ok' if ok else 'err'}={message}",
        status_code=303,
    )


@router.post("/settings/company")
async def hr_company_profile_save(
    legal_name: str = Form(""), trade_name: str = Form(""),
    ein_last4: str = Form(""), address_line1: str = Form(""),
    address_line2: str = Form(""), city: str = Form(""),
    state: str = Form("UT"), zip_code: str = Form(""),
    payroll_contact_email: str = Form(""),
    final_approver_email: str = Form(""),
    utah_withholding_account_last4: str = Form(""),
    utah_ui_account_last4: str = Form(""),
    federal_deposit_schedule: str = Form(""),
    utah_withholding_payment_frequency: str = Form(""),
    source_note: str = Form(""),
    user: dict = Depends(_settings_guard),
):
    ok, message = payroll_store.save_company_profile(
        legal_name=legal_name, trade_name=trade_name, ein_last4=ein_last4,
        address_line1=address_line1, address_line2=address_line2,
        city=city, state=state, zip_code=zip_code,
        payroll_contact_email=payroll_contact_email,
        final_approver_email=final_approver_email,
        utah_withholding_account_last4=utah_withholding_account_last4,
        utah_ui_account_last4=utah_ui_account_last4,
        federal_deposit_schedule=federal_deposit_schedule,
        utah_withholding_payment_frequency=utah_withholding_payment_frequency,
        source_note=source_note, actor=user.get("email", ""),
    )
    return RedirectResponse(
        f"/admin/hr/settings?{'ok' if ok else 'err'}={message}", status_code=303
    )


@router.post("/settings/opening-balance")
async def hr_opening_balance_save(
    employee_email: str = Form(""), tax_year: int = Form(2026),
    gross_wages: str = Form("0"), social_security_wages: str = Form("0"),
    medicare_wages: str = Form("0"), futa_wages: str = Form("0"),
    utah_ui_wages: str = Form("0"), federal_withheld: str = Form("0"),
    utah_withheld: str = Form("0"), employee_ss_withheld: str = Form("0"),
    employee_medicare_withheld: str = Form("0"), source_note: str = Form(""),
    user: dict = Depends(_settings_guard),
):
    ok, message = payroll_store.save_opening_balance(
        employee_email=employee_email, tax_year=tax_year, gross_wages=gross_wages,
        social_security_wages=social_security_wages, medicare_wages=medicare_wages,
        futa_wages=futa_wages, utah_ui_wages=utah_ui_wages,
        federal_withheld=federal_withheld, utah_withheld=utah_withheld,
        employee_ss_withheld=employee_ss_withheld,
        employee_medicare_withheld=employee_medicare_withheld,
        source_note=source_note, actor=user.get("email", ""),
    )
    return RedirectResponse(
        f"/admin/hr/settings?{'ok' if ok else 'err'}={message}", status_code=303
    )


@router.post("/settings/qualified-review")
async def hr_qualified_review_save(
    tax_year: int = Form(2026), reviewer_name: str = Form(""),
    reviewer_email: str = Form(""), reviewed_on: date = Form(...),
    evidence_reference: str = Form(""), review_note: str = Form(""),
    attested: bool = Form(False), user: dict = Depends(_settings_guard),
):
    ok, message = payroll_store.save_payroll_review(
        tax_year=tax_year, reviewer_name=reviewer_name,
        reviewer_email=reviewer_email, reviewed_on=reviewed_on,
        evidence_reference=evidence_reference, review_note=review_note,
        attested=attested, actor=user.get("email", ""),
    )
    return RedirectResponse(
        f"/admin/hr/settings?{'ok' if ok else 'err'}={message}",
        status_code=303,
    )


@router.post("/settings/opening-balance/{balance_id}/decision")
async def hr_opening_balance_decision(
    balance_id: int, decision: str = Form(""), review_note: str = Form(""),
    user: dict = Depends(_settings_guard),
):
    ok, message = payroll_store.decide_opening_balance(
        balance_id, decision=decision, review_note=review_note,
        actor=user.get("email", ""),
    )
    return RedirectResponse(
        f"/admin/hr/settings?{'ok' if ok else 'err'}={message}",
        status_code=303,
    )
