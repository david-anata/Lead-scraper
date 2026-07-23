"""HR section routes — /admin/hr/*.

Most pages are gated by `hr.access`; the money/config pages (payroll, settings)
by `hr.payroll`. Server-rendered HTML (no JSON API). POSTs redirect (303).
"""

from __future__ import annotations

from datetime import date
from urllib.parse import urlparse

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse

from sales_support_agent.services.auth_deps import require_tool
from sales_support_agent.services.access.notify import send_invite_email
from sales_support_agent.services.hr import store
from sales_support_agent.services.hr import payroll_store
from sales_support_agent.services.hr import workforce
from sales_support_agent.services.hr import reporting
from sales_support_agent.services.hr.pages import (
    render_hr_coming_soon,
    render_hr_dashboard,
    render_hr_employee_form,
    render_hr_employees,
    render_hr_invitation,
    render_hr_onboarding,
    render_hr_payroll_control,
    render_hr_payroll_run,
    render_hr_pay_statements,
    render_hr_settings,
    render_hr_contractors,
    render_hr_offboarding,
    render_hr_reports,
    render_hr_policies,
    render_hr_teams,
    render_hr_time,
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


router = APIRouter(prefix="/admin/hr", dependencies=[Depends(_same_origin_write)])

_guard = require_tool("hr.access")
_pay_guard = require_tool("hr.payroll")


def _flash(request: Request):
    return request.query_params.get("ok") or request.query_params.get("err")


def _can_manage(user: dict) -> bool:
    permissions = user.get("permissions") or set()
    return bool(user.get("is_superadmin") or "hr.payroll" in permissions)


# --- dashboard -------------------------------------------------------------

@router.get("", response_class=HTMLResponse)
async def hr_dashboard(request: Request, user: dict = Depends(_guard)):
    stats = (
        store.dashboard_stats() if _can_manage(user)
        else store.employee_dashboard_stats(user.get("email", ""))
    )
    return HTMLResponse(render_hr_dashboard(
        stats, user=user, flash=_flash(request), manager_view=_can_manage(user)
    ))


# --- employees -------------------------------------------------------------

@router.get("/employees", response_class=HTMLResponse)
async def employees_list(request: Request, user: dict = Depends(_guard)):
    employees = store.list_employees() if _can_manage(user) else [
        item for item in store.list_employees()
        if item["email"] == (user.get("email") or "").strip().lower()
    ]
    return HTMLResponse(render_hr_employees(employees, user=user, flash=_flash(request)))


@router.get("/employees/new", response_class=HTMLResponse)
async def employee_new(request: Request, user: dict = Depends(_pay_guard)):
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
    user: dict = Depends(_pay_guard),
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
async def employee_edit(emp_id: int, request: Request, user: dict = Depends(_pay_guard)):
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
    hire_date: date | None = Form(None),
    title: str = Form(""),
    manager_email: str = Form(""),
    classification: str = Form("nonexempt"),
    pay_basis: str = Form("hourly"),
    fixed_pay_per_period: str = Form("0"),
    standard_weekly_hours: float = Form(40),
    phone: str = Form(""),
    status: str = Form("active"),
    user: dict = Depends(_pay_guard),
):
    employee = store.get_employee(emp_id)
    if not employee:
        return RedirectResponse("/admin/hr/employees?err=not_found", status_code=303)
    store.update_employee(emp_id, full_name=full_name, hr_role=hr_role,
                          employee_type=employee_type, team_id=team_id or None,
                          hourly_rate=hourly_rate, annual_salary=annual_salary,
                          phone=phone, status=status, actor=user.get("email", "system"))
    store.upsert_employment_profile(
        employee["email"], hire_date=hire_date, title=title, manager_email=manager_email,
        classification=classification, pay_basis=pay_basis,
        fixed_pay_per_period=fixed_pay_per_period,
        standard_weekly_hours=standard_weekly_hours,
        standard_period_hours=86.67, actor=user.get("email", "system"),
    )
    return RedirectResponse(f"/admin/hr/employees/{emp_id}?ok=employment_saved", status_code=303)


@router.post("/employees/{emp_id}/invite", response_class=HTMLResponse)
async def employee_invite(emp_id: int, request: Request, user: dict = Depends(_pay_guard)):
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
        to_email=employee["email"], invite_link=invite_link,
        invited_by=user.get("email", ""), role_name="HR Employee",
    )
    return HTMLResponse(render_hr_invitation(
        invite_link, employee, user=user, email_sent=email_sent
    ))


# --- employee onboarding ---------------------------------------------------

@router.get("/onboarding", response_class=HTMLResponse)
async def employee_onboarding(request: Request, user: dict = Depends(_guard)):
    email = (user.get("email") or "").strip().lower()
    employee = store.get_employee_by_email(email)
    if not employee:
        return HTMLResponse("Employee record not found.", status_code=404)
    return HTMLResponse(render_hr_onboarding(
        employee, store.get_onboarding(email),
        tax_election=store.get_current_tax_election(email),
        user=user, flash=_flash(request)
    ))


@router.post("/onboarding/profile")
async def onboarding_profile(
    phone: str = Form(""), address_line1: str = Form(""),
    address_line2: str = Form(""), city: str = Form(""),
    state: str = Form("UT"), zip_code: str = Form(""),
    emergency_name: str = Form(""), emergency_relationship: str = Form(""),
    emergency_phone: str = Form(""), emergency_email: str = Form(""),
    user: dict = Depends(_guard),
):
    email = (user.get("email") or "").strip().lower()
    ok, message = store.save_employee_profile(
        email, phone=phone, address_line1=address_line1, address_line2=address_line2,
        city=city, state=state, zip_code=zip_code, emergency_name=emergency_name,
        emergency_relationship=emergency_relationship, emergency_phone=emergency_phone,
        emergency_email=emergency_email, actor=email,
    )
    return RedirectResponse(f"/admin/hr/onboarding?{'ok' if ok else 'err'}={message}", status_code=303)


@router.post("/onboarding/w4")
async def onboarding_w4(
    ssn: str = Form(""), filing_status: str = Form(""),
    two_jobs: bool = Form(False), dependents_credit: str = Form("0"),
    other_income: str = Form("0"), deductions: str = Form("0"),
    extra_withholding: str = Form("0"), exempt: bool = Form(False),
    attested: bool = Form(False),
    user: dict = Depends(_guard),
):
    email = (user.get("email") or "").strip().lower()
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
    email = (user.get("email") or "").strip().lower()
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
    user: dict = Depends(_pay_guard),
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
    emp_id: int, reason: str = Form(""), user: dict = Depends(_pay_guard),
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
async def teams_list(request: Request, user: dict = Depends(_pay_guard)):
    return HTMLResponse(render_hr_teams(store.list_teams(), user=user, flash=_flash(request)))


@router.post("/teams", response_class=HTMLResponse)
async def team_create(
    request: Request,
    name: str = Form(""),
    manager_email: str = Form(""),
    description: str = Form(""),
    user: dict = Depends(_pay_guard),
):
    store.create_team(name=name, manager_email=manager_email, description=description)
    return RedirectResponse("/admin/hr/teams?ok=team_created", status_code=303)


# --- time and PTO ----------------------------------------------------------

@router.get("/time", response_class=HTMLResponse)
async def hr_time(request: Request, user: dict = Depends(_guard)):
    email = (user.get("email") or "").strip().lower()
    can_review = bool(user.get("is_superadmin") or "hr.payroll" in (user.get("permissions") or set()))
    return HTMLResponse(render_hr_time(
        store.list_time_entries(None if can_review else email), store.pto_summary(email),
        store.list_pto_requests(None if can_review else email), store.current_clock(email),
        store.list_time_corrections(None if can_review else email),
        store.time_review_flags(None if can_review else email),
        user=user, flash=_flash(request)))


@router.post("/time/clock")
async def hr_time_clock(action: str = Form(""), user: dict = Depends(_guard)):
    email = (user.get("email") or "").strip().lower()
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
    email = (user.get("email") or "").strip().lower()
    ok, message = store.request_time_correction(
        time_entry_id, employee_email=email, proposed_start=proposed_start,
        proposed_stop=proposed_stop, reason=reason, actor=email,
    )
    return RedirectResponse(f"/admin/hr/time?{'ok' if ok else 'err'}={message}", status_code=303)


@router.post("/time/corrections/{correction_id}/decision")
async def hr_time_correction_decision(
    correction_id: int, decision: str = Form(""),
    reviewer_reason: str = Form(""), user: dict = Depends(_pay_guard),
):
    actor = (user.get("email") or "").strip().lower()
    ok, message = store.decide_time_correction(
        correction_id, decision=decision, reviewer_reason=reviewer_reason, actor=actor,
    )
    return RedirectResponse(f"/admin/hr/time?{'ok' if ok else 'err'}={message}", status_code=303)


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
async def hr_reports(request: Request, user: dict = Depends(_pay_guard)):
    return HTMLResponse(render_hr_reports(user=user))


@router.get("/reports/{kind}.csv")
async def hr_report_csv(kind: str, user: dict = Depends(_pay_guard)):
    content = reporting.export_csv(kind)
    if content is None:
        return PlainTextResponse("Unknown HR export.", status_code=404)
    return PlainTextResponse(
        content, media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="anata-hr-{kind}.csv"',
                 "Cache-Control": "no-store"},
    )


@router.get("/policies", response_class=HTMLResponse)
async def hr_policies(request: Request, user: dict = Depends(_guard)):
    email = (user.get("email") or "").strip().lower()
    return HTMLResponse(render_hr_policies(
        store.current_policy(email), user=user, flash=_flash(request)
    ))


@router.post("/policies/acknowledge")
async def hr_policy_acknowledge(
    attested: bool = Form(False), user: dict = Depends(_guard),
):
    email = (user.get("email") or "").strip().lower()
    ok, message = store.acknowledge_current_policy(
        email, actor=email, attested=attested
    )
    return RedirectResponse(
        f"/admin/hr/policies?{'ok' if ok else 'err'}={message}", status_code=303
    )


@router.get("/payroll", response_class=HTMLResponse)
async def hr_payroll(request: Request, period_date: date | None = None,
                     user: dict = Depends(_pay_guard)):
    return HTMLResponse(render_hr_payroll_control(
        payroll_store.control_room(period_date or date.today()),
        user=user, flash=_flash(request),
    ))


@router.get("/contractors", response_class=HTMLResponse)
async def hr_contractors(request: Request, user: dict = Depends(_pay_guard)):
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
    contractor_email: str = Form(""), tax_form_type: str = Form("undetermined"),
    tax_form_status: str = Form("missing"), received_date: date | None = Form(None),
    expiration_date: date | None = Form(None),
    wise_recipient_reference: str = Form(""), review_note: str = Form(""),
    user: dict = Depends(_pay_guard),
):
    ok, message = workforce.save_contractor_profile(
        contractor_email=contractor_email, tax_form_type=tax_form_type,
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
    user: dict = Depends(_pay_guard),
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
    evidence_note: str = Form(""), user: dict = Depends(_pay_guard),
):
    ok, message = workforce.contractor_payment_action(
        payment_id, action=action, wise_reference=wise_reference,
        evidence_note=evidence_note, actor=user.get("email", ""),
    )
    return RedirectResponse(
        f"/admin/hr/contractors?{'ok' if ok else 'err'}={message}", status_code=303
    )


@router.get("/offboarding", response_class=HTMLResponse)
async def hr_offboarding(request: Request, user: dict = Depends(_pay_guard)):
    return HTMLResponse(render_hr_offboarding(
        store.list_employees(), workforce.list_offboarding(),
        user=user, flash=_flash(request),
    ))


@router.post("/offboarding")
async def hr_offboarding_create(
    employee_email: str = Form(""), separation_type: str = Form(""),
    last_working_day: date = Form(...), final_pay_date: date = Form(...),
    reason: str = Form(""), user: dict = Depends(_pay_guard),
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
    user: dict = Depends(_pay_guard),
):
    ok, message = workforce.update_offboarding(
        checklist_id, completed_steps=completed_steps, actor=user.get("email", "")
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
    user: dict = Depends(_pay_guard),
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
    user: dict = Depends(_pay_guard),
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
                             user: dict = Depends(_pay_guard)):
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
    user: dict = Depends(_pay_guard),
):
    ok, message = payroll_store.approve_payroll(
        run_id, actor=user.get("email", ""), approval_text=approval_text
    )
    return RedirectResponse(
        f"/admin/hr/payroll?period_date={period_date}&{'ok' if ok else 'err'}={message}",
        status_code=303,
    )


@router.post("/payroll/liabilities/{liability_id}")
async def hr_payroll_liability_action(
    liability_id: int, period_date: date = Form(...), action: str = Form(""),
    confirmation_number: str = Form(""), confirmed_amount: str = Form(""),
    filing_confirmation_number: str = Form(""),
    evidence_note: str = Form(""),
    user: dict = Depends(_pay_guard),
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
    run_id: str, request: Request, user: dict = Depends(_pay_guard),
):
    run = payroll_store.payroll_run_detail(run_id)
    if not run:
        return RedirectResponse("/admin/hr/payroll?err=run_not_found", status_code=303)
    return HTMLResponse(render_hr_payroll_run(run, user=user, flash=_flash(request)))


@router.post("/payroll/runs/{run_id}/checks")
async def hr_payroll_issue_check(
    run_id: str, employee_email: str = Form(""), check_number: str = Form(""),
    user: dict = Depends(_pay_guard),
):
    ok, message = payroll_store.issue_printed_check(
        run_id, employee_email=employee_email, check_number=check_number,
        actor=user.get("email", ""),
    )
    return RedirectResponse(
        f"/admin/hr/payroll/runs/{run_id}?{'ok' if ok else 'err'}={message}",
        status_code=303,
    )


@router.post("/payroll/runs/{run_id}/checks/reissue")
async def hr_payroll_reissue_check(
    run_id: str, employee_email: str = Form(""), reason: str = Form(""),
    new_check_number: str = Form(""), user: dict = Depends(_pay_guard),
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
async def hr_payroll_close_run(run_id: str, user: dict = Depends(_pay_guard)):
    ok, message = payroll_store.close_payroll_run(
        run_id, actor=user.get("email", "")
    )
    return RedirectResponse(
        f"/admin/hr/payroll/runs/{run_id}?{'ok' if ok else 'err'}={message}",
        status_code=303,
    )


@router.get("/pay-statements", response_class=HTMLResponse)
async def hr_pay_statements(user: dict = Depends(_guard)):
    email = (user.get("email") or "").strip().lower()
    runs = payroll_store.employee_pay_statements(email)
    return HTMLResponse(render_hr_pay_statements(runs, user=user))


@router.get("/pay-statements/{run_id}", response_class=HTMLResponse)
async def hr_pay_statement_detail(run_id: str, request: Request,
                                  user: dict = Depends(_guard)):
    email = (user.get("email") or "").strip().lower()
    run = payroll_store.payroll_run_detail(run_id, employee_email=email)
    if not run or not run["calculations"] or run["status"] not in {"checks_issued", "closed"}:
        return RedirectResponse("/admin/hr/pay-statements", status_code=303)
    return HTMLResponse(render_hr_payroll_run(
        run, user=user, employee_view=True, flash=_flash(request)
    ))


@router.get("/settings", response_class=HTMLResponse)
async def hr_settings(request: Request, user: dict = Depends(_pay_guard)):
    return HTMLResponse(render_hr_settings(
        payroll_store.get_payroll_settings(), payroll_store.get_company_profile(),
        store.list_employees(),
        payroll_store.list_opening_balances(2026),
        user=user, flash=_flash(request)
    ))


@router.post("/settings")
async def hr_settings_save(
    utah_ui_rate: str = Form(""), qualified_tax_review: bool = Form(False),
    eftps_ready: bool = Form(False), utah_tap_ready: bool = Form(False),
    utah_ui_ready: bool = Form(False),
    opening_balances_confirmed: bool = Form(False),
    opening_balance_note: str = Form(""), user: dict = Depends(_pay_guard),
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


@router.post("/settings/company")
async def hr_company_profile_save(
    legal_name: str = Form(""), trade_name: str = Form(""),
    ein_last4: str = Form(""), address_line1: str = Form(""),
    address_line2: str = Form(""), city: str = Form(""),
    state: str = Form("UT"), zip_code: str = Form(""),
    payroll_contact_email: str = Form(""),
    utah_withholding_account_last4: str = Form(""),
    utah_ui_account_last4: str = Form(""),
    federal_deposit_schedule: str = Form(""),
    utah_withholding_payment_frequency: str = Form(""),
    source_note: str = Form(""),
    user: dict = Depends(_pay_guard),
):
    ok, message = payroll_store.save_company_profile(
        legal_name=legal_name, trade_name=trade_name, ein_last4=ein_last4,
        address_line1=address_line1, address_line2=address_line2,
        city=city, state=state, zip_code=zip_code,
        payroll_contact_email=payroll_contact_email,
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
    user: dict = Depends(_pay_guard),
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
