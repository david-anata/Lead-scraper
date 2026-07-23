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
        self.assertIn("Anata planning estimates", page.text)
        self.assertIn("authoritative payroll service's result", page.text)
        self.assertIn("Estimated tax liability", page.text)
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
        self.assertIn("preserve ordinary payroll and employment history for seven years", page.text)
        self.assertIn("No record is automatically deleted", page.text)

        quarterly = self._get(
            "/admin/hr/reports/quarterly-register.csv?year=2026&quarter=3", self.sa
        )
        self.assertEqual(quarterly.status_code, 200)
        self.assertIn("gross_wages", quarterly.text)
        self.assertIn(
            'filename="anata-hr-quarterly-register-2026-q3.csv"',
            quarterly.headers["content-disposition"],
        )

    def test_internal_payroll_contract_download_is_private_and_versioned(self):
        response = self._get(
            "/admin/hr/settings/provider-contract.json", self.sa
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["cache-control"], "no-store")
        self.assertEqual(
            response.json()["authority_decision"],
            "required_before_production",
        )
        self.assertIn("ssn", response.json()["run_request"]["forbidden_fields"])
        self.assertIn(
            "anata-internal-payroll-contract-2026-07-23.json",
            response.headers["content-disposition"],
        )
        import uuid
        employee_email = f"contract-denied-{uuid.uuid4().hex[:8]}@anatainc.com"
        user_id = access_store.upsert_user(employee_email, "Contract Denied")
        access_store.set_user_permissions(user_id, ["hr.access"])
        denied = self._get(
            "/admin/hr/settings/provider-contract.json",
            _cookie(employee_email),
        )
        self.assertEqual(denied.status_code, 403)

    def test_hr_backup_is_private_and_checksum_verifiable(self):
        import hashlib
        import io
        import json
        import zipfile

        response = self._get("/admin/hr/reports/backup.zip?year=2026", self.sa)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["cache-control"], "no-store")
        self.assertEqual(response.headers["x-content-type-options"], "nosniff")
        with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
            names = set(archive.namelist())
            self.assertIn("manifest.json", names)
            self.assertIn("year-to-date-register.csv", names)
            manifest = json.loads(archive.read("manifest.json"))
            self.assertFalse(manifest["contains_full_ssns"])
            self.assertFalse(manifest["contains_sealed_tax_forms"])
            for name, details in manifest["files"].items():
                self.assertEqual(
                    hashlib.sha256(archive.read(name)).hexdigest(),
                    details["sha256"],
                )

    def test_employee_mobile_shortcuts_are_present_and_mark_current_page(self):
        page = self._get("/admin/hr/time", self.sa)
        self.assertEqual(page.status_code, 200)
        self.assertIn('class="hr-mobile-nav"', page.text)
        self.assertIn(
            '<a href="/admin/hr/time" aria-current="page">Time</a>', page.text
        )
        self.assertIn('href="/admin/hr/pay-statements"', page.text)
        self.assertIn("cell.setAttribute('data-label'", page.text)
        self.assertIn(".hr-js .hr-tbl td::before", page.text)
        self.assertIn('name="hr-csrf-token"', page.text)
        self.assertIn("csrfInput.name = '_csrf_token'", page.text)

    def test_browser_hr_writes_require_session_bound_csrf_token(self):
        import re

        self.client.cookies.set(*self.sa)
        try:
            page = self.client.get("/admin/hr/policies")
            token = re.search(
                r'name="hr-csrf-token" content="([a-f0-9]+)"', page.text
            ).group(1)
            missing = self.client.post(
                "/admin/hr/policies/acknowledge",
                data={"attested": "true"},
                headers={"Origin": "http://testserver"},
                follow_redirects=False,
            )
            self.assertEqual(missing.status_code, 403)
            accepted = self.client.post(
                "/admin/hr/policies/acknowledge",
                data={"attested": "true", "_csrf_token": token},
                headers={"Origin": "http://testserver"},
                follow_redirects=False,
            )
            self.assertEqual(accepted.status_code, 303)
        finally:
            self.client.cookies.clear()

    def test_granular_people_permission_does_not_imply_compensation_access(self):
        import uuid
        email = f"people-view-{uuid.uuid4().hex[:8]}@anatainc.com"
        hr_store.create_employee(
            email=email, full_name="People Viewer", hourly_rate="42.50"
        )
        uid = access_store.upsert_user(email, "People Viewer")
        access_store.set_user_permissions(uid, ["hr.access", "hr.people.view"])

        listing = self._get("/admin/hr/employees", _cookie(email))
        self.assertEqual(listing.status_code, 200)
        self.assertIn("Restricted", listing.text)
        self.assertNotIn("$42.50/hr", listing.text)

    def test_granular_payroll_viewer_cannot_prepare_payroll(self):
        import uuid
        email = f"pay-view-{uuid.uuid4().hex[:8]}@anatainc.com"
        uid = access_store.upsert_user(email, "Payroll Viewer")
        access_store.set_user_permissions(
            uid, ["hr.access", "hr.payroll.view"]
        )
        page = self._get("/admin/hr/payroll", _cookie(email))
        self.assertEqual(page.status_code, 200)
        blocked = self._post(
            "/admin/hr/payroll/prepare",
            {"period_date": "2026-08-01"},
            _cookie(email),
        )
        self.assertEqual(blocked.status_code, 403)

    def test_people_and_compensation_manager_can_open_employee_setup(self):
        import uuid
        email = f"people-manage-{uuid.uuid4().hex[:8]}@anatainc.com"
        uid = access_store.upsert_user(email, "People Manager")
        access_store.set_user_permissions(
            uid,
            ["hr.access", "hr.people.manage", "hr.compensation.manage"],
        )
        page = self._get("/admin/hr/employees/new", _cookie(email))
        self.assertEqual(page.status_code, 200)
        self.assertIn("Hourly rate", page.text)

    def test_compensation_change_requires_effective_date_and_keeps_history(self):
        import uuid
        employee_email = f"pay-change-{uuid.uuid4().hex[:8]}@anatainc.com"
        employee_id = hr_store.create_employee(
            email=employee_email, full_name="Pay Change", hourly_rate="20"
        )
        missing = self._post(
            f"/admin/hr/employees/{employee_id}",
            {
                "full_name": "Pay Change", "hr_role": "employee",
                "employee_type": "hourly", "hourly_rate": "22",
                "annual_salary": "0", "pay_basis": "hourly",
                "fixed_pay_per_period": "0", "standard_weekly_hours": "40",
                "status": "active",
            },
            self.sa,
        )
        self.assertEqual(missing.status_code, 422)
        self.assertIn("effective date and business reason", missing.text)

        saved = self._post(
            f"/admin/hr/employees/{employee_id}",
            {
                "full_name": "Pay Change", "hr_role": "employee",
                "employee_type": "hourly", "hourly_rate": "22",
                "annual_salary": "0", "pay_basis": "hourly",
                "fixed_pay_per_period": "0", "standard_weekly_hours": "40",
                "status": "active",
                "compensation_effective_date": "2026-08-01",
                "compensation_reason": "Approved merit increase",
            },
            self.sa,
        )
        self.assertEqual(saved.status_code, 303)
        history = hr_store.list_compensation_changes(employee_email)
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["prior"]["hourly_rate_cents"], 2000)
        self.assertEqual(history[0]["new"]["hourly_rate_cents"], 2200)

    def test_time_approver_is_limited_to_assigned_employees(self):
        import uuid
        from sqlalchemy.orm import Session
        from sales_support_agent.models.database import get_engine
        from sales_support_agent.models.hr import HRTimesheetApproval

        suffix = uuid.uuid4().hex[:8]
        manager = f"manager-{suffix}@anatainc.com"
        assigned = f"assigned-{suffix}@anatainc.com"
        outside = f"outside-{suffix}@anatainc.com"
        for email in (assigned, outside):
            hr_store.create_employee(email=email, full_name=email.split("@")[0])
        hr_store.upsert_employment_profile(
            assigned, hire_date=date(2026, 1, 1), manager_email=manager,
            actor="test",
        )
        hr_store.upsert_employment_profile(
            outside, hire_date=date(2026, 1, 1),
            manager_email="someone-else@anatainc.com", actor="test",
        )
        uid = access_store.upsert_user(manager, "Manager")
        access_store.set_user_permissions(
            uid, ["hr.access", "hr.time.approve_team"]
        )
        with Session(get_engine()) as session:
            assigned_row = HRTimesheetApproval(
                employee_email=assigned, period_start=date(2026, 8, 1),
                period_end=date(2026, 8, 15), status="submitted",
                source_hash="assigned", submitted_by=assigned,
            )
            outside_row = HRTimesheetApproval(
                employee_email=outside, period_start=date(2026, 8, 1),
                period_end=date(2026, 8, 15), status="submitted",
                source_hash="outside", submitted_by=outside,
            )
            session.add_all([assigned_row, outside_row])
            session.commit()
            outside_id = outside_row.id

        page = self._get(
            "/admin/hr/time?period_date=2026-08-01", _cookie(manager)
        )
        self.assertEqual(page.status_code, 200)
        self.assertIn(assigned, page.text)
        self.assertNotIn(outside, page.text)
        blocked = self._post(
            f"/admin/hr/time/timesheets/{outside_id}/decision",
            {
                "period_start": "2026-08-01", "decision": "approved",
                "review_note": "Should not be allowed",
            },
            _cookie(manager),
        )
        self.assertEqual(blocked.status_code, 403)


if __name__ == "__main__":
    unittest.main()
