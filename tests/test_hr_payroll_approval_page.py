from sales_support_agent.services.hr.pages import (
    render_hr_payroll_approval,
    render_hr_payroll_control,
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
    }

    html = render_hr_payroll_control(
        control, user={"email": "david@anatainc.com"}
    )

    assert (
        'href="/admin/hr/payroll/runs/payroll-version-123/approve"'
        in html
    )
    assert 'action="/admin/hr/payroll/payroll-version-123/approve"' not in html


def test_settings_lists_authorized_owner_without_employee_record():
    html = render_hr_settings(
        {},
        {"final_approver_email": ""},
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
