from sales_support_agent.services.hr.pages import (
    render_hr_payroll_approval,
    render_hr_payroll_control,
    render_hr_payroll_run,
    render_hr_settings,
)
from sales_support_agent.services.hr.payroll import SemimonthlyPeriod
from datetime import date


def _run(*, prepared_by: str = "val@anatainc.com", status: str = "prepared") -> dict:
    return {
        "id": "payroll-version-123",
        "status": status,
        "period_start": "2026-08-01",
        "period_end": "2026-08-15",
        "pay_date": "2026-08-20",
        "prepared_by": prepared_by,
        "required_approver_email": "david@anatainc.com",
        "gross": "3,000.00",
        "net": "2,400.00",
        "taxes": "600.00",
        "deductions": "50.00",
        "cash_impact": "3,250.00",
        "provider_handoff": {"status": "not_submitted", "provider_name": ""},
        "calculations": [{}, {}, {}],
    }


def test_confirmation_identifies_version_totals_provider_and_approver():
    html = render_hr_payroll_approval(
        _run(),
        user={"name": "David Narayan", "email": "david@anatainc.com"},
    )

    assert "payroll-version-123" in html
    assert "2026-08-20" in html
    assert "3,250.00" in html
    assert "Employees included</strong><p>3" in html
    assert "David Narayan" in html
    assert "Manual controlled payroll — no provider connected" in html
    assert "does <strong>not</strong> transfer wages" in html
    assert "Approve this exact payroll version" in html
    assert "Reject this version" in html


def test_preparer_cannot_approve_own_version():
    html = render_hr_payroll_approval(
        _run(prepared_by="david@anatainc.com"),
        user={"name": "David Narayan", "email": "david@anatainc.com"},
    )

    assert "prepared this payroll cannot approve" in html
    assert "Approve this exact payroll version" not in html


def test_only_configured_final_approver_sees_approval_form():
    html = render_hr_payroll_approval(
        _run(),
        user={"name": "Val", "email": "val@anatainc.com"},
    )

    assert "Only the configured final approver" in html
    assert "david@anatainc.com" in html
    assert "Approve this exact payroll version" not in html


def test_control_room_links_to_confirmation_instead_of_inline_approval():
    run = _run()
    control = {
        "period": SemimonthlyPeriod(
            start_date=date(2026, 8, 1),
            end_date=date(2026, 8, 15),
            pay_date=date(2026, 8, 20),
        ),
        "readiness": {"ready": True, "blockers": []},
        "employees": [],
        "inputs": [],
        "runs": [{
            **run,
            "employee_count": 3,
            "initiated_by": run["prepared_by"],
            "gross_change_percent": None,
        }],
        "liabilities": [],
        "final_approver_email": "david@anatainc.com",
    }

    html = render_hr_payroll_control(
        control, user={"email": "david@anatainc.com"}
    )

    assert (
        'href="/admin/hr/payroll/runs/payroll-version-123/approve"'
        in html
    )
    assert 'action="/admin/hr/payroll/payroll-version-123/approve"' not in html
    assert "Review and approve payroll" in html


def test_final_approver_who_prepared_version_gets_exact_val_handoff():
    run = _run(prepared_by="david@anatainc.com")
    control = {
        "period": SemimonthlyPeriod(
            start_date=date(2026, 8, 1), end_date=date(2026, 8, 15),
            pay_date=date(2026, 8, 20),
        ),
        "readiness": {"ready": True, "blockers": []},
        "employees": [], "inputs": [], "liabilities": [],
        "opening_balances": [], "timesheets": [], "settings": {},
        "final_approver_email": "david@anatainc.com",
        "runs": [{
            **run, "employee_count": 3,
            "initiated_by": "david@anatainc.com",
            "gross_change_percent": None,
        }],
    }

    html = render_hr_payroll_control(
        control, user={"email": "david@anatainc.com"}
    )

    assert "You prepared this version, so you cannot approve it" in html
    assert "Val must sign in" in html
    assert "Prepare immutable payroll version" not in html
    assert (
        'href="/admin/hr/payroll/runs/payroll-version-123/approve"'
        not in html
    )
    assert "first live run remains blocked" not in html


def test_payroll_run_formats_exact_hours_for_people():
    run = _run(status="prepared")
    run["calculations"] = [{
        "employee_email": "hourly@anatainc.com",
        "inputs": {
            "regular_hours": "32.03055555555555555555555555",
            "overtime_hours": "0E-27",
            "holiday_hours": "0",
            "pto_hours": "4",
        },
        "results": {},
        "check_number": "",
    }]

    html = render_hr_payroll_run(
        run, user={"email": "david@anatainc.com"}
    )

    assert "32.03" in html
    assert "0.00" in html
    assert "4.00" in html
    assert "32.03055555555555555555555555" not in html
    assert "0E-27" not in html


def test_settings_lists_authorized_owner_without_employee_record():
    html = render_hr_settings(
        {},
        {"final_approver_email": ""},
        {"configured": False, "status": "Setup needed", "reason": "Missing."},
        [],
        [{
            "email": "david@anatainc.com",
            "name": "David Narayan",
            "status": "active",
            "permissions": {"hr.payroll.approve"},
        }],
        [],
        [],
        user={"email": "david@anatainc.com"},
    )

    assert "They do not need to be a W-2 employee" in html
    assert "David Narayan — david@anatainc.com" in html
    assert "Choose an authorized payroll approver" in html
    assert 'id="qualified-review" class="hr-callout blocked"' in html
    assert "This is the only item still blocking payroll" in html
    assert "Do not change the employer profile" in html


def _blocked_control(*, confirmed_by: str = "david@anatainc.com") -> dict:
    control = {
        "period": SemimonthlyPeriod(
            start_date=date(2026, 8, 1), end_date=date(2026, 8, 15),
            pay_date=date(2026, 8, 20),
        ),
        "readiness": {"ready": False, "blockers": [{
            "kind": "opening_balance", "employee_email": "val@anatainc.com",
            "message": "Opening balance still needs independent approval",
            "href": "/admin/hr/payroll?period_date=2026-08-01#opening-balances",
            "action": "Review opening balance",
        }]},
        "employees": [{
            "id": 1, "email": "val@anatainc.com", "full_name": "Val",
            "hourly_rate": "20.00", "employment": {"pay_basis": "hourly"},
        }],
        "opening_balances": [{
            "id": 7, "employee_email": "val@anatainc.com",
            "gross_wages": "100.00", "federal_withheld": "10.00",
            "utah_withheld": "5.00", "source_note": "Prior system",
            "approval_status": "unreviewed", "confirmed_by": confirmed_by,
        }],
        "inputs": [], "runs": [], "liabilities": [], "timesheets": [],
        "settings": {}, "tax_elections": {},
    }
    return control


def test_authorized_opening_balance_enterer_can_approve():
    html = render_hr_payroll_control(
        _blocked_control(),
        user={"email": "david@anatainc.com"},
    )

    assert "Payroll cannot be prepared — 1 task remaining" in html
    assert html.index("Payroll cannot be prepared") < html.index("Today's process")
    assert "Approve balance" in html


def test_opening_balance_form_does_not_send_operator_to_broken_reauth_loop():
    html = render_hr_payroll_control(
        _blocked_control(confirmed_by="val@anatainc.com"),
        user={"email": "david@anatainc.com", "session_issued_at": "0"},
    )

    assert "Sign in again to approve" not in html
    assert "Approve balance" in html


def test_company_setup_blocker_links_to_calculation_review_form():
    control = _blocked_control()
    control["readiness"]["blockers"] = [{
        "kind": "tax_setup", "message": "Qualified payroll tax calculation review not ready",
        "href": "/admin/hr/settings", "action": "Review payroll setup",
    }]
    html = render_hr_payroll_control(control, user={"email": "david@anatainc.com"})

    assert 'href="#calculation-review">Complete calculation review</a>' in html
    assert 'action="/admin/hr/payroll/qualified-review"' in html
