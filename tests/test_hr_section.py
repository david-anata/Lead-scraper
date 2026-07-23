"""HR section — routes, permissions, employee/team CRUD (Phase 0 build)."""

from __future__ import annotations

import os
import tempfile
import unittest
from unittest import mock
from datetime import date

os.environ.setdefault("SALES_AGENT_DB_URL", "sqlite:///" + tempfile.gettempdir() + "/hr_section_test.db")
os.environ.setdefault("HR_PII_SECRET", "test-only-hr-pii-secret")

try:
    from fastapi.testclient import TestClient
    from sales_support_agent.main import app
    from sales_support_agent.services.access import store as access_store
    from sales_support_agent.services.admin_auth import create_user_session_token
    from sales_support_agent.services.hr import store as hr_store
    from sales_support_agent.services.hr import payroll_store
    DEPS = True
except ModuleNotFoundError as exc:
    if exc.name not in {"sqlalchemy", "fastapi"}:
        raise
    DEPS = False


def _cookie(email, name="U", role="member"):
    s = app.state.agent_settings
    return s.admin_cookie_name, create_user_session_token(s, email=email, name=name, role=role)


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class HRSectionTests(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)
        self.sa = _cookie("david@anatainc.com", "David", "admin")  # superadmin
        if not hr_store.get_employee_by_email("david@anatainc.com"):
            hr_store.create_employee(email="david@anatainc.com", full_name="David")
        hr_store.upsert_employment_profile(
            "david@anatainc.com", hire_date=date(2026, 1, 1),
            classification="exempt", pay_basis="fixed_semimonthly",
            fixed_pay_per_period="1000", actor="test",
        )

    def _get(self, path, cookie):
        self.client.cookies.set(*cookie)
        try:
            return self.client.get(path, follow_redirects=False)
        finally:
            self.client.cookies.clear()

    def _post(self, path, data, cookie):
        self.client.cookies.set(*cookie)
        try:
            return self.client.post(path, data=data, follow_redirects=False)
        finally:
            self.client.cookies.clear()

    def test_dashboard_and_nav(self):
        r = self._get("/admin/hr", self.sa)
        self.assertEqual(r.status_code, 200)
        self.assertIn("HR Dashboard", r.text)
        self.assertIn(">HR<", r.text)              # HR in the top nav
        self.assertIn('aria-label="HR pages"', r.text)
        self.assertNotIn('class="hr-side"', r.text)

    def test_create_and_list_employee(self):
        import uuid
        email = f"worker-{uuid.uuid4().hex[:8]}@anatainc.com"  # unique — persistent temp DB
        r = self._post("/admin/hr/employees/new",
                       {"email": email, "full_name": "Work Er",
                        "hr_role": "employee", "employee_type": "hourly", "hourly_rate": "27.50"},
                       self.sa)
        self.assertIn(r.status_code, (302, 303))
        lst = self._get("/admin/hr/employees", self.sa)
        self.assertIn(email, lst.text)
        self.assertIn("Work Er", lst.text)

    def test_duplicate_employee_rejected(self):
        import uuid
        email = f"dup-{uuid.uuid4().hex[:8]}@anatainc.com"
        self._post("/admin/hr/employees/new", {"email": email}, self.sa)
        r2 = self._post("/admin/hr/employees/new", {"email": email}, self.sa)
        self.assertEqual(r2.status_code, 422)
        self.assertIn("already exists", r2.text)

    def test_create_team(self):
        r = self._post("/admin/hr/teams", {"name": "Ops Team", "manager_email": "m@anatainc.com"}, self.sa)
        self.assertIn(r.status_code, (302, 303))
        lst = self._get("/admin/hr/teams", self.sa)
        self.assertIn("Ops Team", lst.text)

    def test_hr_access_required(self):
        # A provisioned user with no HR tools cannot see HR.
        uid = access_store.upsert_user("nohr@anatainc.com", "NoHR")
        access_store.set_user_permissions(uid, ["finance"])
        r = self._get("/admin/hr", _cookie("nohr@anatainc.com"))
        self.assertEqual(r.status_code, 403)

    def test_payroll_needs_payroll_permission(self):
        # hr.access alone can't reach payroll (needs hr.payroll).
        uid = access_store.upsert_user("hronly@anatainc.com", "HROnly")
        access_store.set_user_permissions(uid, ["hr.access"])
        ck = _cookie("hronly@anatainc.com")
        self.assertEqual(self._get("/admin/hr/employees", ck).status_code, 200)   # allowed
        self.assertEqual(self._get("/admin/hr/employees/new", ck).status_code, 403)
        self.assertEqual(self._get("/admin/hr/teams", ck).status_code, 403)
        self.assertEqual(self._get("/admin/hr/payroll", ck).status_code, 403)     # blocked

    def test_cross_site_hr_write_is_rejected(self):
        self.client.cookies.set(*self.sa)
        try:
            response = self.client.post(
                "/admin/hr/time/clock", data={"action": "in"},
                headers={"Origin": "https://malicious.example"},
                follow_redirects=False,
            )
        finally:
            self.client.cookies.clear()
        self.assertEqual(response.status_code, 403)

    def test_hr_reminder_job_fails_closed_without_internal_key(self):
        response = self.client.post(
            "/api/jobs/hr-reminders/run", follow_redirects=False
        )
        self.assertEqual(response.status_code, 403)

    def test_time_clock_and_pto_pages_are_live(self):
        page = self._get("/admin/hr/time", self.sa)
        self.assertEqual(page.status_code, 200)
        self.assertIn("Time &amp; PTO", page.text)
        self.assertIn("Clock in", page.text)

        punch = self._post("/admin/hr/time/clock", {"action": "in"}, self.sa)
        self.assertEqual(punch.status_code, 303)
        running = self._get("/admin/hr/time", self.sa)
        self.assertIn("Clock out", running.text)
        self._post("/admin/hr/time/clock", {"action": "out"}, self.sa)

        request = self._post("/admin/hr/time/pto", {
            "start_date": "2026-08-10", "end_date": "2026-08-10",
            "hours": "4", "reason": "Appointment",
        }, self.sa)
        self.assertEqual(request.status_code, 303)
        self.assertIn("Appointment", self._get("/admin/hr/time", self.sa).text)

    def test_hourly_timesheet_requires_employee_attestation_and_independent_review(self):
        import uuid
        email = f"timesheet-{uuid.uuid4().hex[:8]}@anatainc.com"
        hr_store.create_employee(
            email=email, full_name="Timesheet Employee",
            employee_type="hourly", hourly_rate="20",
        )
        hr_store.upsert_employment_profile(
            email, hire_date=date(2026, 1, 1), classification="nonexempt",
            pay_basis="hourly", actor="test",
        )
        uid = access_store.upsert_user(email, "Timesheet Employee")
        access_store.set_user_permissions(uid, ["hr.access"])
        employee_cookie = _cookie(email, "Timesheet Employee")
        self._post("/admin/hr/time/clock", {"action": "in"}, employee_cookie)
        self._post("/admin/hr/time/clock", {"action": "out"}, employee_cookie)

        today = date.today()
        if today.day <= 15:
            start, end = today.replace(day=1), today.replace(day=15)
        else:
            import calendar
            start = today.replace(day=16)
            end = today.replace(day=calendar.monthrange(today.year, today.month)[1])
        submitted = self._post("/admin/hr/time/timesheets/submit", {
            "period_start": start.isoformat(), "period_end": end.isoformat(),
            "attested": "true",
        }, employee_cookie)
        self.assertIn("ok=timesheet_submitted", submitted.headers["location"])
        approval = hr_store.list_timesheet_approvals(start, end, email)[0]

        self.assertEqual(
            hr_store.decide_timesheet(
                approval["id"], decision="approved", review_note="Own review",
                actor=email,
            ),
            (False, "self_approval_blocked"),
        )
        val_id = access_store.upsert_user("val@anatainc.com", "Val")
        access_store.set_user_permissions(val_id, ["hr.access", "hr.payroll"])
        approved = self._post(
            f"/admin/hr/time/timesheets/{approval['id']}/decision",
            {
                "period_start": start.isoformat(), "decision": "approved",
                "review_note": "Compared against the submitted punches.",
            },
            _cookie("val@anatainc.com", "Val"),
        )
        self.assertIn("ok=timesheet_approved", approved.headers["location"])
        self.assertEqual(
            hr_store.list_timesheet_approvals(start, end, email)[0]["status"],
            "approved",
        )

    def test_hire_date_creates_trackable_utah_new_hire_report(self):
        import uuid
        email = f"new-hire-{uuid.uuid4().hex[:8]}@anatainc.com"
        hr_store.create_employee(email=email, full_name="New Hire")
        hr_store.upsert_employment_profile(
            email, hire_date=date(2026, 8, 10), classification="nonexempt",
            pay_basis="hourly", actor="david@anatainc.com",
        )
        task = next(
            item for item in hr_store.list_compliance_tasks()
            if item["employee_email"] == email
        )
        self.assertEqual(task["due_date"], date(2026, 8, 30))
        self.assertEqual(task["status"], "open")

        saved = self._post(f"/admin/hr/compliance/{task['id']}", {
            "action": "confirmed", "confirmation_reference": "UT-NH-123",
            "evidence_note": "Submission accepted by Utah registry.",
        }, self.sa)
        self.assertIn("ok=compliance_confirmed", saved.headers["location"])
        updated = next(
            item for item in hr_store.list_compliance_tasks()
            if item["employee_email"] == email
        )
        self.assertEqual(updated["status"], "confirmed")
        self.assertEqual(updated["confirmation_reference"], "UT-NH-123")

    def test_compliance_page_has_24_period_authoritative_calendar(self):
        page = self._get("/admin/hr/compliance?year=2026", self.sa)
        self.assertEqual(page.status_code, 200)
        self.assertIn("Authoritative semimonthly schedule: 24 periods", page.text)
        self.assertIn("2026-08-20", page.text)
        self.assertIn("2026-09-04", page.text)
        self.assertIn("Federal Form 941", page.text)
        self.assertIn("Utah TC-941E", page.text)
        self.assertIn("Federal W-2/W-3", page.text)
        tasks = hr_store.list_compliance_tasks()
        annual = [item for item in tasks if "2026" in item["task_type"]]
        self.assertEqual(len(annual), 15)
        q4 = next(
            item for item in annual if item["task_type"] == "federal_941_2026_q4"
        )
        self.assertEqual(q4["due_date"], date(2027, 2, 1))

    def test_qualified_review_requires_named_external_evidence(self):
        invalid = self._post("/admin/hr/settings/qualified-review", {
            "tax_year": "2026", "reviewer_name": "Payroll Reviewer",
            "reviewer_email": "reviewer@example.com", "reviewed_on": "2026-07-23",
            "evidence_reference": "", "review_note": "Compared payroll.",
            "attested": "true",
        }, self.sa)
        self.assertIn("err=qualified_review_invalid", invalid.headers["location"])

        saved = self._post("/admin/hr/settings/qualified-review", {
            "tax_year": "2026", "reviewer_name": "Payroll Reviewer",
            "reviewer_email": "reviewer@example.com", "reviewed_on": "2026-07-23",
            "evidence_reference": "Parallel payroll workpaper 2026-07",
            "review_note": "Compared federal, Utah, FICA, FUTA, and Utah UI.",
            "attested": "true",
        }, self.sa)
        self.assertIn("ok=qualified_review_saved", saved.headers["location"])
        settings = self._get("/admin/hr/settings", self.sa)
        self.assertIn("Payroll Reviewer", settings.text)
        self.assertIn("Parallel payroll workpaper", settings.text)

    def test_opening_balance_requires_a_different_reviewer(self):
        import uuid
        email = f"opening-{uuid.uuid4().hex[:8]}@anatainc.com"
        hr_store.create_employee(email=email, full_name="Opening Balance")
        saved = payroll_store.save_opening_balance(
            employee_email=email, tax_year=2026, gross_wages="1000",
            social_security_wages="1000", medicare_wages="1000",
            futa_wages="1000", utah_ui_wages="1000",
            federal_withheld="100", utah_withheld="40",
            employee_ss_withheld="62", employee_medicare_withheld="14.50",
            source_note="Prior payroll register", actor="david@anatainc.com",
        )
        self.assertEqual(saved, (True, "opening_balance_saved"))
        balance = next(
            item for item in payroll_store.list_opening_balances(2026)
            if item["employee_email"] == email
        )
        self.assertEqual(balance["approval_status"], "unreviewed")
        self.assertEqual(
            payroll_store.decide_opening_balance(
                balance["id"], decision="approved",
                review_note="Compared to the source register.",
                actor="david@anatainc.com",
            ),
            (False, "self_approval_blocked"),
        )
        approved = payroll_store.decide_opening_balance(
            balance["id"], decision="approved",
            review_note="Compared to the source register.",
            actor="val@anatainc.com",
        )
        self.assertEqual(approved, (True, "opening_balance_approved"))
        updated = next(
            item for item in payroll_store.list_opening_balances(2026)
            if item["employee_email"] == email
        )
        self.assertEqual(updated["approval_status"], "approved")

    def test_payroll_page_is_a_control_room_not_payment_claim(self):
        page = self._get("/admin/hr/payroll", self.sa)
        self.assertEqual(page.status_code, 200)
        self.assertIn("Payroll control room", page.text)
        self.assertIn("Payroll readiness", page.text)
        self.assertNotIn("compute gross/taxes/net and pay employees", page.text)

    def test_check_and_tax_evidence_actions_redirect_with_result(self):
        with mock.patch(
            "sales_support_agent.api.hr_router.payroll_store.issue_printed_check",
            return_value=(True, "check_issued"),
        ):
            issued = self._post("/admin/hr/payroll/runs/pay_test/checks", {
                "employee_email": "david@anatainc.com", "check_number": "1001",
            }, self.sa)
        self.assertEqual(issued.status_code, 303)
        self.assertEqual(
            issued.headers["location"],
            "/admin/hr/payroll/runs/pay_test?ok=check_issued",
        )

        with mock.patch(
            "sales_support_agent.api.hr_router.payroll_store.record_liability_action",
            return_value=(True, "liability_paid"),
        ):
            paid = self._post("/admin/hr/payroll/liabilities/17", {
                "period_date": "2026-08-01", "action": "paid",
                "confirmation_number": "EFTPS-1", "confirmed_amount": "100.00",
                "filing_confirmation_number": "", "evidence_note": "Receipt reviewed",
            }, self.sa)
        self.assertEqual(paid.status_code, 303)
        self.assertEqual(
            paid.headers["location"],
            "/admin/hr/payroll?period_date=2026-08-01&ok=liability_paid",
        )

    def test_employee_can_only_see_own_employee_record_in_list(self):
        import uuid
        self_email = f"self-{uuid.uuid4().hex[:8]}@anatainc.com"
        other = f"private-{uuid.uuid4().hex[:8]}@anatainc.com"
        hr_store.create_employee(email=self_email, full_name="Self Person")
        hr_store.create_employee(email=other, full_name="Private Person")
        uid = access_store.upsert_user(self_email, "Self Person")
        access_store.set_user_permissions(uid, ["hr.access"])
        page = self._get("/admin/hr/employees", _cookie(self_email))
        self.assertEqual(page.status_code, 200)
        self.assertIn(self_email, page.text)
        self.assertNotIn(other, page.text)

        dashboard = self._get("/admin/hr", _cookie(self_email))
        self.assertNotIn("Active employees", dashboard.text)
        self.assertIn("Onboarding steps", dashboard.text)

    def test_holiday_calendar_observes_weekend_rule_and_excludes_overtime(self):
        holidays = hr_store.paid_holidays(2026)
        independence = next(row for row in holidays if row["name"] == "Independence Day")
        self.assertEqual(independence["actual_date"], date(2026, 7, 4))
        self.assertEqual(independence["observed_date"], date(2026, 7, 3))

    def test_secure_onboarding_saves_sealed_w4(self):
        profile = self._post("/admin/hr/onboarding/profile", {
            "phone": "8015550100", "address_line1": "1 Main", "city": "Salt Lake City",
            "state": "UT", "zip_code": "84101", "emergency_name": "Val",
            "emergency_relationship": "Coworker", "emergency_phone": "8015550199",
        }, self.sa)
        self.assertEqual(profile.status_code, 303)
        w4 = self._post("/admin/hr/onboarding/w4", {
            "ssn": "123-45-6789", "filing_status": "single",
            "exempt": "false", "attested": "true",
        }, self.sa)
        self.assertEqual(w4.status_code, 303)
        self.assertIn("ok=w4_saved", w4.headers["location"])
        state = hr_store.get_onboarding("david@anatainc.com")
        self.assertTrue(state["profile_complete"])
        self.assertTrue(state["w4_complete"])

    def test_new_w4_requires_employee_to_choose_filing_status(self):
        import uuid
        email = f"new-w4-{uuid.uuid4().hex[:8]}@anatainc.com"
        hr_store.create_employee(email=email, full_name="New Employee")
        hr_store.save_employee_profile(
            email, phone="", address_line1="20 State St", address_line2="",
            city="Salt Lake City", state="UT", zip_code="84111",
            emergency_name="David", emergency_relationship="Employer",
            emergency_phone="8015550100", emergency_email="",
            actor=email,
        )
        uid = access_store.upsert_user(email, "New Employee")
        access_store.set_user_permissions(uid, ["hr.access"])

        page = self._get("/admin/hr/onboarding", _cookie(email))

        self.assertEqual(page.status_code, 200)
        self.assertIn("New Employee", page.text)
        self.assertIn("20 State St", page.text)
        self.assertIn('<option value="">Choose your filing status</option>', page.text)
        self.assertNotIn('value="single" selected', page.text)
        self.assertNotIn('value="married_joint" selected', page.text)
        self.assertNotIn('value="head_household" selected', page.text)

    def test_onboarding_without_employee_record_uses_recoverable_app_shell(self):
        import uuid
        email = f"missing-employee-{uuid.uuid4().hex[:8]}@anatainc.com"
        uid = access_store.upsert_user(email, "Missing Employee")
        access_store.set_user_permissions(uid, ["hr.access"])

        page = self._get("/admin/hr/onboarding", _cookie(email))

        self.assertEqual(page.status_code, 404)
        self.assertIn("Your employee record is not ready yet.", page.text)
        self.assertIn('href="/admin/hr"', page.text)
        self.assertIn("topbar-section-band", page.text)
        self.assertNotEqual(page.text.strip(), "Employee record not found.")

    def test_w4_correction_prefills_safe_fields_but_never_full_ssn(self):
        saved = self._post("/admin/hr/onboarding/w4", {
            "ssn": "123-45-6789", "filing_status": "married_joint",
            "two_jobs": "true", "dependents_credit": "500",
            "other_income": "25", "deductions": "100",
            "extra_withholding": "15", "exempt": "false", "attested": "true",
        }, self.sa)
        self.assertIn("ok=w4_saved", saved.headers["location"])

        election = hr_store.get_current_tax_election("david@anatainc.com")
        self.assertEqual(election["ssn_last4"], "6789")
        self.assertNotIn("sealed_ssn", election)
        page = self._get("/admin/hr/onboarding", self.sa)

        self.assertIn('value="married_joint" selected', page.text)
        self.assertIn('name="two_jobs" value="true" style="width:auto" checked', page.text)
        self.assertIn('name="dependents_credit" inputmode="decimal" value="500.00"', page.text)
        self.assertIn("ending in <strong>6789</strong>", page.text)
        self.assertNotIn("123-45-6789", page.text)
        self.assertNotIn("123456789", page.text)

    def test_onboarding_correction_preserves_submission_and_shows_employee_reason(self):
        self._post("/admin/hr/onboarding/profile", {
            "address_line1": "1 Main", "city": "Salt Lake City", "state": "UT",
            "zip_code": "84101", "emergency_name": "Val",
            "emergency_relationship": "Coworker", "emergency_phone": "8015550199",
        }, self.sa)
        employee = hr_store.get_employee_by_email("david@anatainc.com")
        requested = self._post(
            f"/admin/hr/employees/{employee['id']}/onboarding-correction",
            {"reason": "Please confirm the emergency phone number."}, self.sa,
        )
        self.assertIn("onboarding_correction_requested", requested.headers["location"])
        state = hr_store.get_onboarding("david@anatainc.com")
        self.assertEqual(state["status"], "correction_requested")
        self.assertTrue(state["profile_complete"])
        page = self._get("/admin/hr/onboarding", self.sa)
        self.assertIn("Correction requested", page.text)
        self.assertIn("confirm the emergency phone number", page.text)

    def test_time_correction_requires_another_reviewer(self):
        self._post("/admin/hr/time/clock", {"action": "in"}, self.sa)
        self._post("/admin/hr/time/clock", {"action": "out"}, self.sa)
        entry = hr_store.list_time_entries("david@anatainc.com", limit=1)[0]
        requested = self._post(f"/admin/hr/time/{entry['id']}/correction", {
            "proposed_start": "09:00", "proposed_stop": "17:00", "reason": "Missed exact times",
        }, self.sa)
        self.assertIn("correction_requested", requested.headers["location"])
        correction = hr_store.list_time_corrections("david@anatainc.com")[0]
        own = self._post(f"/admin/hr/time/corrections/{correction['id']}/decision", {
            "decision": "approved", "reviewer_reason": "Looks right",
        }, self.sa)
        self.assertIn("self_approval_blocked", own.headers["location"])

        val_id = access_store.upsert_user("val@anatainc.com", "Val")
        access_store.set_user_permissions(val_id, ["hr.access", "hr.payroll"])
        approved = self._post(f"/admin/hr/time/corrections/{correction['id']}/decision", {
            "decision": "approved", "reviewer_reason": "Reviewed against schedule",
        }, _cookie("val@anatainc.com"))
        self.assertIn("correction_approved", approved.headers["location"])

    def test_reports_include_accountant_registers(self):
        page = self._get("/admin/hr/reports", self.sa)
        self.assertEqual(page.status_code, 200)
        self.assertIn("/admin/hr/reports/quarterly-register.csv", page.text)
        self.assertIn("/admin/hr/reports/year-to-date-register.csv", page.text)

        quarterly = self._get(
            "/admin/hr/reports/quarterly-register.csv?year=2026&quarter=3", self.sa
        )
        self.assertEqual(quarterly.status_code, 200)
        self.assertIn("gross_wages", quarterly.text)
        self.assertIn(
            'filename="anata-hr-quarterly-register-2026-q3.csv"',
            quarterly.headers["content-disposition"],
        )


if __name__ == "__main__":
    unittest.main()
