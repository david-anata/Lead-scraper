"""HR section routes — /admin/hr/*.

Most pages are gated by `hr.access`; the money/config pages (payroll, settings)
by `hr.payroll`. Server-rendered HTML (no JSON API). POSTs redirect (303).
"""

from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from sales_support_agent.services.auth_deps import require_tool
from sales_support_agent.services.hr import store
from sales_support_agent.services.hr.pages import (
    render_hr_coming_soon,
    render_hr_dashboard,
    render_hr_employee_form,
    render_hr_employees,
    render_hr_payroll_control,
    render_hr_settings,
    render_hr_teams,
    render_hr_time,
)

router = APIRouter(prefix="/admin/hr")

_guard = require_tool("hr.access")
_pay_guard = require_tool("hr.payroll")


def _flash(request: Request):
    return request.query_params.get("ok") or request.query_params.get("err")


# --- dashboard -------------------------------------------------------------

@router.get("", response_class=HTMLResponse)
async def hr_dashboard(request: Request, user: dict = Depends(_guard)):
    return HTMLResponse(render_hr_dashboard(store.dashboard_stats(), user=user, flash=_flash(request)))


# --- employees -------------------------------------------------------------

@router.get("/employees", response_class=HTMLResponse)
async def employees_list(request: Request, user: dict = Depends(_guard)):
    return HTMLResponse(render_hr_employees(store.list_employees(), user=user, flash=_flash(request)))


@router.get("/employees/new", response_class=HTMLResponse)
async def employee_new(request: Request, user: dict = Depends(_guard)):
    return HTMLResponse(render_hr_employee_form(None, store.list_teams(), user=user))


@router.post("/employees/new", response_class=HTMLResponse)
async def employee_create(
    request: Request,
    email: str = Form(""),
    full_name: str = Form(""),
    hr_role: str = Form("employee"),
    employee_type: str = Form("hourly"),
    team_id: str = Form(""),
    hourly_rate: str = Form("0"),
    annual_salary: str = Form("0"),
    phone: str = Form(""),
    status: str = Form("active"),
    user: dict = Depends(_guard),
):
    if not email.strip():
        return HTMLResponse(render_hr_employee_form(None, store.list_teams(), user=user,
                                                    error="Email is required."), status_code=422)
    new_id = store.create_employee(
        email=email, full_name=full_name, hr_role=hr_role, employee_type=employee_type,
        team_id=team_id or None, hourly_rate=hourly_rate, annual_salary=annual_salary,
        phone=phone, status=status, actor=user.get("email", "system"))
    if new_id is None:
        return HTMLResponse(render_hr_employee_form(
            {"email": email.strip().lower(), "full_name": full_name, "hr_role": hr_role,
             "employee_type": employee_type, "team_id": team_id, "hourly_rate": hourly_rate,
             "annual_salary": annual_salary, "phone": phone, "status": status, "id": ""},
            store.list_teams(), user=user,
            error="An employee with that email already exists."), status_code=422)
    return RedirectResponse("/admin/hr/employees?ok=created", status_code=303)


@router.get("/employees/{emp_id}", response_class=HTMLResponse)
async def employee_edit(emp_id: int, request: Request, user: dict = Depends(_guard)):
    emp = store.get_employee(emp_id)
    if not emp:
        return RedirectResponse("/admin/hr/employees", status_code=303)
    return HTMLResponse(render_hr_employee_form(emp, store.list_teams(), user=user))


@router.post("/employees/{emp_id}", response_class=HTMLResponse)
async def employee_update(
    emp_id: int,
    request: Request,
    full_name: str = Form(""),
    hr_role: str = Form("employee"),
    employee_type: str = Form("hourly"),
    team_id: str = Form(""),
    hourly_rate: str = Form("0"),
    annual_salary: str = Form("0"),
    phone: str = Form(""),
    status: str = Form("active"),
    user: dict = Depends(_guard),
):
    store.update_employee(emp_id, full_name=full_name, hr_role=hr_role,
                          employee_type=employee_type, team_id=team_id or None,
                          hourly_rate=hourly_rate, annual_salary=annual_salary,
                          phone=phone, status=status, actor=user.get("email", "system"))
    return RedirectResponse("/admin/hr/employees?ok=updated", status_code=303)


# --- teams -----------------------------------------------------------------

@router.get("/teams", response_class=HTMLResponse)
async def teams_list(request: Request, user: dict = Depends(_guard)):
    return HTMLResponse(render_hr_teams(store.list_teams(), user=user, flash=_flash(request)))


@router.post("/teams", response_class=HTMLResponse)
async def team_create(
    request: Request,
    name: str = Form(""),
    manager_email: str = Form(""),
    description: str = Form(""),
    user: dict = Depends(_guard),
):
    store.create_team(name=name, manager_email=manager_email, description=description)
    return RedirectResponse("/admin/hr/teams?ok=team_created", status_code=303)


# --- time and PTO ----------------------------------------------------------

@router.get("/time", response_class=HTMLResponse)
async def hr_time(request: Request, user: dict = Depends(_guard)):
    email = (user.get("email") or "").strip().lower()
    can_review = bool(user.get("is_superadmin") or "hr.payroll" in (user.get("permissions") or set()))
    return HTMLResponse(render_hr_time(
        store.list_time_entries(email), store.pto_summary(email),
        store.list_pto_requests(None if can_review else email), store.current_clock(email),
        user=user, flash=_flash(request)))


@router.post("/time/clock")
async def hr_time_clock(action: str = Form(""), user: dict = Depends(_guard)):
    email = (user.get("email") or "").strip().lower()
    ok, message = (store.clock_out(email, actor=email) if action == "out"
                   else store.clock_in(email, actor=email))
    key = "ok" if ok else "err"
    return RedirectResponse(f"/admin/hr/time?{key}={message}", status_code=303)


@router.post("/time/pto/{request_id}/decision")
async def hr_pto_decision(request_id: int, decision: str = Form(""),
                          user: dict = Depends(_pay_guard)):
    actor = (user.get("email") or "").strip().lower()
    ok = store.decide_pto(request_id, decision=decision, actor=actor)
    return RedirectResponse(f"/admin/hr/time?{'ok=updated' if ok else 'err=invalid_request'}",
                            status_code=303)


@router.post("/time/pto")
async def hr_pto_request(start_date: date = Form(...), end_date: date = Form(...),
                         hours: float = Form(...), reason: str = Form(""),
                         user: dict = Depends(_guard)):
    email = (user.get("email") or "").strip().lower()
    ok, message = store.create_pto_request(email, start_date=start_date, end_date=end_date,
                                           hours=hours, reason=reason, actor=email)
    key = "ok" if ok else "err"
    return RedirectResponse(f"/admin/hr/time?{key}={message}", status_code=303)


@router.get("/reports", response_class=HTMLResponse)
async def hr_reports(request: Request, user: dict = Depends(_guard)):
    return HTMLResponse(render_hr_coming_soon(
        "reports", "HR Reports", "Hours and tax-withholding reports across employees.", user=user))


@router.get("/payroll", response_class=HTMLResponse)
async def hr_payroll(request: Request, user: dict = Depends(_pay_guard)):
    return HTMLResponse(render_hr_payroll_control(user=user))


@router.get("/settings", response_class=HTMLResponse)
async def hr_settings(request: Request, user: dict = Depends(_pay_guard)):
    return HTMLResponse(render_hr_settings(user=user))
