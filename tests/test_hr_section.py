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

    def _grant_test_pto(self, employee_email: str, hours: float = 40) -> None:
        """Seed an explicit balance only for tests that exercise PTO workflow."""
        from sqlalchemy.orm import Session
        from sales_support_agent.models.database import get_engine
        from sales_support_agent.models.hr import HRPTOLedger

        with Session(get_engine()) as session:
            session.add(HRPTOLedger(
                employee_email=employee_email,
                entry_type="adjusted",
                hours=hours,
                effective_date=date(2026, 8, 1),
                source_type="test_setup",
                source_id=employee_email,
                note="Explicit test-only opening PTO balance.",
                created_by="test",
            ))
            session.commit()

    def test_dashboard_and_nav(self):
        r = self._get("/admin/hr", self.sa)
        self.assertEqual(r.status_code, 200)
        self.assertIn("HR Dashboard", r.text)
        self.assertIn(">HR<", r.text)              # HR in the top nav
        self.assertIn('aria-label="HR pages"', r.text)
        self.assertNotIn('class="hr-side"', r.text)
        self.assertEqual(r.text.count('id="agent-main-content"'), 1)
        self.assertIn("field.labels && field.labels.length", r.text)
        self.assertIn("candidate.htmlFor = field.id", r.text)
        self.assertIn(".top-actions .top-link", r.text)

    def test_setup_checklist_uses_live_readiness_and_is_payroll_private(self):
        page = self._get("/admin/hr/setup", self.sa)
        self.assertEqual(page.status_code, 200)
        self.assertIn("Finish setup without re-entering known information", page.text)
        self.assertIn("Reconcile 2026 opening balances", page.text)
        self.assertIn("Review the recovered Base44 drafts; do not", page.text)
        self.assertIn('href="/admin/hr/setup"', page.text)

        import uuid
        email = f"setup-viewer-{uuid.uuid4().hex[:8]}@anatainc.com"
        uid = access_store.upsert_user(email, "Setup Viewer")
        access_store.set_user_permissions(uid, ["hr.access"])
        denied = self._get("/admin/hr/setup", _cookie(email))
        self.assertEqual(denied.status_code, 403)

    def test_calendar_setup_shows_safe_service_account_identity(self):
        import os

        old_calendar = os.environ.get("HR_OOO_GOOGLE_CALENDAR_ID")
        old_credential = os.environ.get("HR_OOO_GOOGLE_CALENDAR_SERVICE_ACCOUNT_JSON")
        os.environ.pop("HR_OOO_GOOGLE_CALENDAR_ID", None)
        os.environ["HR_OOO_GOOGLE_CALENDAR_SERVICE_ACCOUNT_JSON"] = (
            '{"client_email":"ooo-calendar-agent@example.com"}'
        )
        try:
            page = self._get("/admin/hr/settings", self.sa)
        finally:
            if old_calendar is None:
                os.environ.pop("HR_OOO_GOOGLE_CALENDAR_ID", None)
            else:
                os.environ["HR_OOO_GOOGLE_CALENDAR_ID"] = old_calendar
            if old_credential is None:
                os.environ.pop("HR_OOO_GOOGLE_CALENDAR_SERVICE_ACCOUNT_JSON", None)
            else:
                os.environ["HR_OOO_GOOGLE_CALENDAR_SERVICE_ACCOUNT_JSON"] = old_credential
        self.assertEqual(page.status_code, 200)
        self.assertIn("Service account to share the calendar with", page.text)
        self.assertIn("ooo-calendar-agent@example.com", page.text)
        self.assertNotIn("client_email", page.text)

    def test_signed_in_superadmin_is_available_as_final_payroll_approver(self):
        page = self._get("/admin/hr/settings", self.sa)
        self.assertEqual(page.status_code, 200)
        self.assertIn(
            '<option value="david@anatainc.com"', page.text
        )

    def test_prelaunch_payroll_defaults_to_approved_first_live_period(self):
        from sales_support_agent.api.hr_router import _default_payroll_date

        self.assertEqual(
            _default_payroll_date(date(2026, 7, 28)),
            date(2026, 8, 1),
        )
        self.assertEqual(
            _default_payroll_date(date(2026, 8, 20)),
            date(2026, 8, 1),
        )
        # The default period follows the clock once the first live date passes,
        # so derive the expected period instead of pinning one that expires.
        from sales_support_agent.services.hr.payroll import semimonthly_period

        expected = semimonthly_period(_default_payroll_date())
        page = self._get("/admin/hr/payroll", self.sa)
        self.assertEqual(page.status_code, 200)
        self.assertIn(
            f"Review and freeze {expected.start_date}–{expected.end_date}", page.text,
        )

    def test_create_and_list_employee(self):
        import uuid
        email = f"worker-{uuid.uuid4().hex[:8]}@anatainc.com"  # unique — persistent temp DB
        login_email = f"worker-{uuid.uuid4().hex[:8]}@example.com"
        r = self._post("/admin/hr/employees/new",
                       {"email": email, "hr_login_email": login_email, "full_name": "Work Er",
                        "hr_role": "employee", "employee_type": "hourly", "hourly_rate": "27.50"},
                       self.sa)
        self.assertIn(r.status_code, (302, 303))
        lst = self._get("/admin/hr/employees", self.sa)
        self.assertIn(email, lst.text)
        self.assertIn("Work Er", lst.text)

    def test_not_on_payroll_requires_effective_reason_and_removes_w2_blockers(self):
        import uuid
        suffix = uuid.uuid4().hex[:8]
        email = f"admin-only-{suffix}@anatainc.com"
        employee_id = hr_store.create_employee(
            email=email, full_name="Admin Only", employee_type="hourly",
            hourly_rate="25", actor="test",
        )
        hr_store.upsert_employment_profile(
            email, hire_date=date(2026, 1, 1), pay_basis="hourly",
            payroll_eligible=True, actor="test",
        )
        payload = {
            "full_name": "Admin Only", "hr_role": "admin",
            "employee_type": "hourly", "payroll_relationship": "not_on_payroll",
            "team_id": "", "hourly_rate": "25", "fixed_pay_per_period": "0",
            "hire_date": "2026-01-01", "title": "Owner", "manager_email": "",
            "classification": "nonexempt", "pay_basis": "hourly",
            "standard_weekly_hours": "40", "phone": "", "status": "active",
        }
        blocked = self._post(f"/admin/hr/employees/{employee_id}", payload, self.sa)
        self.assertEqual(blocked.status_code, 422)
        self.assertIn("Pay changes require an effective date", blocked.text)

        saved = self._post(f"/admin/hr/employees/{employee_id}", {
            **payload,
            "compensation_effective_date": "2026-08-01",
            "compensation_reason": "Owner is not paid through Anata payroll.",
        }, self.sa)
        self.assertEqual(saved.status_code, 303)
        employee = hr_store.get_employee(employee_id)
        self.assertFalse(employee["employment"]["payroll_eligible"])
        control = payroll_store.control_room(date(2026, 8, 1))
        self.assertNotIn(email, {item["email"] for item in control["employees"]})
        self.assertFalse(any(
            item.get("employee_email") == email
            for item in control["readiness"]["blockers"]
        ))

    def test_duplicate_employee_rejected(self):
        import uuid
        email = f"dup-{uuid.uuid4().hex[:8]}@anatainc.com"
        login_email = f"dup-{uuid.uuid4().hex[:8]}@example.com"
        self._post("/admin/hr/employees/new", {
            "email": email, "hr_login_email": login_email,
        }, self.sa)
        r2 = self._post("/admin/hr/employees/new", {
            "email": email, "hr_login_email": login_email,
        }, self.sa)
        self.assertEqual(r2.status_code, 422)
        self.assertIn("already exists", r2.text)

    def test_create_team(self):
        import uuid
        team_name = f"Ops Team {uuid.uuid4().hex[:8]}"
        r = self._post("/admin/hr/teams", {"name": team_name, "manager_email": ""}, self.sa)
        self.assertIn(r.status_code, (302, 303))
        lst = self._get("/admin/hr/teams", self.sa)
        self.assertIn(team_name, lst.text)

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
        self.assertEqual(self._get("/admin/hr/employees", ck).status_code, 303)  # sent to own profile
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
        import uuid
        employee_email = f"time-pto-{uuid.uuid4().hex[:8]}@anatainc.com"
        hr_store.create_employee(
            email=employee_email, full_name="Time PTO Employee",
            employee_type="salaried", annual_salary="24000",
        )
        hr_store.upsert_employment_profile(
            employee_email, hire_date=date(2026, 1, 1),
            classification="exempt", pay_basis="fixed_semimonthly",
            fixed_pay_per_period="1000", standard_weekly_hours=40,
            actor="test",
        )
        self._grant_test_pto(employee_email)
        uid = access_store.upsert_user(employee_email, "Time PTO Employee")
        access_store.set_user_permissions(uid, ["hr.access"])
        employee_cookie = _cookie(employee_email, "Time PTO Employee")

        page = self._get("/admin/hr/time", employee_cookie)
        self.assertEqual(page.status_code, 200)
        self.assertIn("Time &amp; PTO", page.text)
        self.assertIn("Clock in", page.text)

        punch = self._post("/admin/hr/time/clock", {"action": "in"}, employee_cookie)
        self.assertEqual(punch.status_code, 303)
        running = self._get("/admin/hr/time", employee_cookie)
        self.assertIn("Clock out", running.text)
        self._post("/admin/hr/time/clock", {"action": "out"}, employee_cookie)

        request = self._post("/admin/hr/time/pto", {
            "start_date": "2026-08-10", "end_date": "2026-08-10",
            "hours": "4", "reason": "Appointment",
        }, employee_cookie)
        self.assertEqual(request.status_code, 303)
        self.assertIn("Appointment", self._get("/admin/hr/time", employee_cookie).text)

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

        placeholder = self._post("/admin/hr/settings/qualified-review", {
            "tax_year": "2026", "reviewer_name": "Payroll Reviewer",
            "reviewer_email": "reviewer@example.com", "reviewed_on": "2026-07-23",
            "evidence_reference": "n/a", "review_note": "N/A",
            "attested": "true",
        }, self.sa)
        self.assertIn("err=qualified_review_invalid", placeholder.headers["location"])

        saved = self._post("/admin/hr/settings/qualified-review", {
            "tax_year": "2026", "reviewer_name": "Payroll Reviewer",
            "reviewer_email": "reviewer@example.com", "reviewed_on": "2026-07-23",
            "evidence_reference": "Parallel payroll workpaper 2026-07",
            "review_note": "Compared federal, Utah, FICA, FUTA, and Utah UI.",
            "attested": "true",
        }, self.sa)
        self.assertIn("ok=qualified_review_saved", saved.headers["location"])
        self.assertTrue(saved.headers["location"].endswith("#qualified-review"))
        settings = self._get("/admin/hr/settings", self.sa)
        self.assertIn("Payroll Reviewer", settings.text)
        self.assertIn("Parallel payroll workpaper", settings.text)

    def test_settings_tax_operations_copy_reflects_saved_readiness(self):
        payroll_store.save_payroll_settings(
            utah_ui_rate="0.001",
            qualified_tax_review=False,
            utah_tap_ready=False,
            eftps_ready=False,
            utah_ui_ready=False,
            opening_balances_confirmed=False,
            opening_balance_note="Test readiness state.",
            actor="test",
        )
        settings = self._get("/admin/hr/settings", self.sa)
        self.assertEqual(settings.status_code, 200)
        self.assertIn("Utah TAP access: Not yet confirmed", settings.text)
        self.assertIn("Federal deposit schedule:", settings.text)
        self.assertIn("EFTPS access: Not yet confirmed", settings.text)
        self.assertIn("Utah unemployment portal access: Not yet confirmed", settings.text)
        self.assertNotIn("Utah TAP access confirmed", settings.text)

    def test_authorized_operator_can_approve_an_opening_balance_they_entered(self):
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
        approved = payroll_store.decide_opening_balance(
            balance["id"], decision="approved",
            review_note="Compared to the source register.",
            actor="david@anatainc.com",
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
        self.assertIn("No payroll provider is connected", page.text)
        self.assertIn("independently compare the exact run", page.text)
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
        self.assertEqual(page.status_code, 303)
        self.assertEqual(page.headers["location"], "/admin/hr/onboarding")

        dashboard = self._get("/admin/hr", _cookie(self_email))
        self.assertNotIn("Active employees", dashboard.text)
        self.assertIn("Onboarding steps", dashboard.text)
        self.assertIn("Anata employee app", dashboard.text)
        self.assertNotIn('href="/admin/sales"', dashboard.text)
        self.assertNotIn('href="/admin/hr/employees"', dashboard.text)
        self.assertIn('rel="manifest" href="/app.webmanifest"', dashboard.text)

    def test_holiday_calendar_observes_weekend_rule_and_excludes_overtime(self):
        holidays = hr_store.paid_holidays(2026)
        independence = next(row for row in holidays if row["name"] == "Independence Day")
        self.assertEqual(independence["actual_date"], date(2026, 7, 4))
        self.assertEqual(independence["observed_date"], date(2026, 7, 3))

    def test_secure_onboarding_saves_sealed_w4(self):
        profile = self._post("/admin/hr/onboarding/profile", {
            "personal_email": "david.personal@example.com",
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
            email, personal_email=f"personal-{uuid.uuid4().hex[:8]}@example.com",
            phone="", address_line1="20 State St", address_line2="",
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
        self.assertIn("employee-app-header", page.text)
        self.assertNotIn('href="/admin/finances"', page.text)
        self.assertNotEqual(page.text.strip(), "Employee record not found.")

    def test_personal_email_is_required_outside_anata_and_drives_action_list(self):
        import uuid
        email = f"personal-contact-{uuid.uuid4().hex[:8]}@anatainc.com"
        hr_store.create_employee(email=email, full_name="Personal Contact")
        uid = access_store.upsert_user(email, "Personal Contact")
        access_store.set_user_permissions(uid, ["hr.access"])

        dashboard = self._get("/admin/hr", _cookie(email))
        self.assertIn("Add your personal contact email", dashboard.text)

        invalid = self._post(
            "/admin/hr/onboarding/profile",
            {
                "personal_email": "person@anatainc.com",
                "address_line1": "1 Main", "city": "Salt Lake City",
                "state": "UT", "zip_code": "84101",
                "emergency_name": "Contact", "emergency_phone": "8015550100",
            },
            _cookie(email),
        )
        self.assertIn("err=personal_email_invalid", invalid.headers["location"])

        valid = self._post(
            "/admin/hr/onboarding/profile",
            {
                "personal_email": "person@example.com",
                "address_line1": "1 Main", "city": "Salt Lake City",
                "state": "UT", "zip_code": "84101",
                "emergency_name": "Contact", "emergency_phone": "8015550100",
            },
            _cookie(email),
        )
        self.assertIn("ok=profile_saved", valid.headers["location"])
        self.assertEqual(
            hr_store.get_employee_by_email(email)["personal_email"],
            "person@example.com",
        )

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
        self.assertIn("✓ W-4 saved", page.text)
        self.assertIn("Review or replace my W-4", page.text)
        self.assertIn("Other income ($)", page.text)
        self.assertIn("Exempt is uncommon", page.text)

    def test_team_only_edit_does_not_require_pay_change_reason_and_roster_updates(self):
        import uuid
        suffix = uuid.uuid4().hex[:8]
        team_id = hr_store.create_team(name=f"Building {suffix}", manager_email="val@anatainc.com")
        employee_id = hr_store.create_employee(
            email=f"val-{suffix}@anatainc.com", full_name="Val Test",
            hourly_rate="20", employee_type="hourly",
        )
        saved = self._post(
            f"/admin/hr/employees/{employee_id}",
            {
                "full_name": "Val Test", "hr_role": "employee",
                "employee_type": "hourly", "team_id": str(team_id),
                "hourly_rate": "20", "annual_salary": "0",
                "pay_basis": "hourly", "fixed_pay_per_period": "0",
                "standard_weekly_hours": "40", "status": "active",
            },
            self.sa,
        )
        self.assertEqual(saved.status_code, 303)
        team_page = self._get("/admin/hr/teams", self.sa)
        self.assertIn("Val Test", team_page.text)
        self.assertIn(f"/admin/hr/employees/{employee_id}", team_page.text)
        detail = self._get(f"/admin/hr/teams/{team_id}", self.sa)
        self.assertEqual(detail.status_code, 200)
        self.assertIn("Team leadership and direct reporting are separate", detail.text)
        self.assertIn("Val Test", detail.text)

    def test_status_only_edit_ignores_hidden_legacy_annual_salary(self):
        import uuid
        suffix = uuid.uuid4().hex[:8]
        email = f"legacy-owner-{suffix}@anatainc.com"
        employee_id = hr_store.create_employee(
            email=email, full_name="Legacy Owner", employee_type="hourly",
        )
        hr_store.upsert_employment_profile(
            email, hire_date=None, pay_basis="hourly",
            standard_weekly_hours=40, actor="test",
        )
        hr_store.update_employee(
            employee_id, employee_type="salaried",
            annual_salary="90000", actor="legacy-import",
        )

        saved = self._post(
            f"/admin/hr/employees/{employee_id}",
            {
                "full_name": "Legacy Owner", "hr_role": "employee",
                "employee_type": "salaried", "team_id": "",
                "hourly_rate": "0", "pay_basis": "hourly",
                "fixed_pay_per_period": "0",
                "standard_weekly_hours": "40", "status": "inactive",
            },
            self.sa,
        )

        self.assertEqual(saved.status_code, 303)
        self.assertEqual(hr_store.get_employee(employee_id)["status"], "inactive")
        self.assertEqual(hr_store.list_compensation_changes(email), [])

    def test_dedicated_status_update_does_not_require_pay_change_metadata(self):
        import uuid
        suffix = uuid.uuid4().hex[:8]
        employee_email = f"status-only-{suffix}@anatainc.com"
        employee_id = hr_store.create_employee(
            email=employee_email,
            full_name="Status Only",
            employee_type="salaried",
            annual_salary="90000",
        )
        app_user_id = access_store.upsert_user(employee_email, "Status Only")
        access_store.set_user_permissions(app_user_id, ["hr.access"])

        page = self._get(f"/admin/hr/employees/{employee_id}", self.sa)
        self.assertIn(
            f'formaction="/admin/hr/employees/{employee_id}/status"',
            page.text,
        )
        saved = self._post(
            f"/admin/hr/employees/{employee_id}/status",
            {"status": "inactive"},
            self.sa,
        )

        self.assertEqual(saved.status_code, 303)
        self.assertIn("ok=status_saved", saved.headers["location"])
        self.assertEqual(hr_store.get_employee(employee_id)["status"], "inactive")
        self.assertEqual(
            access_store.get_user_by_email(employee_email)["status"], "suspended"
        )
        self.assertEqual(
            hr_store.list_compensation_changes(
                f"status-only-{suffix}@anatainc.com"
            ),
            [],
        )

    def test_employee_app_manifest_worker_and_access_training(self):
        manifest = self.client.get("/app.webmanifest")
        self.assertEqual(manifest.status_code, 200)
        self.assertEqual(manifest.json()["start_url"], "/app")
        self.assertEqual(manifest.json()["display"], "standalone")
        self.assertEqual(
            manifest.json()["icons"][0]["src"],
            "/brand-static/agent-favicon.png",
        )
        worker = self.client.get("/service-worker.js")
        self.assertEqual(worker.status_code, 200)
        self.assertIn("does not", self.client.get("/app/offline").text.lower())
        self.assertNotIn("admin/hr", worker.text)

        app_entry = self._get("/app", self.sa)
        self.assertEqual(app_entry.status_code, 303)
        self.assertEqual(app_entry.headers["location"], "/admin/hr")

        training = self._get("/admin/hr/access-training", self.sa)
        self.assertEqual(training.status_code, 200)
        self.assertIn("Give an employee secure app access", training.text)
        self.assertIn("mark the employee inactive", training.text)

        import uuid
        employee_email = f"training-denied-{uuid.uuid4().hex[:8]}@anatainc.com"
        uid = access_store.upsert_user(employee_email, "Employee")
        access_store.set_user_permissions(uid, ["hr.access"])
        denied = self._get("/admin/hr/access-training", _cookie(employee_email))
        self.assertEqual(denied.status_code, 403)

    def test_employee_invitation_preprovisions_only_hr_and_waits_for_acceptance(self):
        import uuid
        email = f"invite-employee-{uuid.uuid4().hex[:8]}@anatainc.com"
        login_email = f"invite-employee-{uuid.uuid4().hex[:8]}@example.com"
        employee_id = hr_store.create_employee(
            email=email, hr_login_email=login_email,
            full_name="Invited Employee", hr_role="employee"
        )
        work_user_id = access_store.upsert_user(email, "Work Account")
        access_store.set_user_permissions(work_user_id, ["sales.deals"])
        result = self._post(
            f"/admin/hr/employees/{employee_id}/invite", {}, self.sa
        )
        self.assertEqual(result.status_code, 200)
        invited = access_store.get_user_by_email(login_email)
        self.assertEqual(invited["permissions"], {"hr.access"})
        self.assertEqual(invited["status"], "suspended")
        self.assertEqual(
            access_store.get_user_by_email(email)["permissions"], {"sales.deals"}
        )
        self.assertIn(login_email, result.text)

    def test_yahoo_employee_invitation_requires_explicit_single_use_confirmation(self):
        import uuid

        record_email = f"yahoo-worker-{uuid.uuid4().hex[:8]}@anatainc.com"
        login_email = f"yahoo-worker-{uuid.uuid4().hex[:8]}@yahoo.com"
        employee_id = hr_store.create_employee(
            email=record_email,
            hr_login_email=login_email,
            full_name="Yahoo Worker",
            hr_role="employee",
        )
        invite = hr_store.create_employee_invitation(
            record_email, actor="david@anatainc.com"
        )
        self.assertTrue(invite["ok"])

        previewed = self.client.get(
            f"/admin/access/invite/{invite['token']}",
            follow_redirects=False,
        )

        self.assertEqual(previewed.status_code, 200)
        self.assertIn("Your invitation is ready", previewed.text)
        self.assertIn("Continue to Anata", previewed.text)
        self.assertNotIn(invite["token"], previewed.text)
        self.assertEqual(
            access_store.get_user_by_email(login_email)["status"], "suspended"
        )
        self.assertIsNotNone(
            access_store.get_pending_invite_by_token(invite["token"])
        )

        # A link scanner may preview the same URL repeatedly; GET remains safe.
        second_preview = self.client.get(
            f"/admin/access/invite/{invite['token']}", follow_redirects=False
        )
        self.assertEqual(second_preview.status_code, 200)

        accepted = self.client.post(
            "https://testserver/admin/access/invite/accept", follow_redirects=False
        )
        self.assertEqual(accepted.status_code, 302)
        self.assertEqual(accepted.headers["location"], "/app")
        self.assertIn(app.state.agent_settings.admin_cookie_name, accepted.cookies)
        user = access_store.get_user_by_email(login_email)
        self.assertEqual(user["status"], "active")
        self.assertEqual(user["permissions"], {"hr.access"})
        reused = self.client.get(
            f"/admin/access/invite/{invite['token']}",
            follow_redirects=False,
        )
        self.assertEqual(reused.status_code, 410)

    def test_existing_business_login_keeps_approved_tools_when_hr_is_linked(self):
        import uuid

        business_email = f"existing-business-{uuid.uuid4().hex[:8]}@anatainc.com"
        employee_id = hr_store.create_employee(
            email=business_email,
            hr_login_email=business_email,
            full_name="Existing Business User",
        )
        user_id = access_store.upsert_user(business_email, "Existing Business User")
        access_store.set_user_permissions(user_id, ["sales.deals"])

        invite = hr_store.create_employee_invitation(
            business_email, actor="david@anatainc.com"
        )

        self.assertTrue(invite["ok"])
        self.assertEqual(
            access_store.get_user_by_email(business_email)["permissions"],
            {"sales.deals", "hr.access"},
        )

    def test_employee_invitation_requires_personal_login_and_never_uses_work_email(self):
        import uuid
        email = f"no-personal-login-{uuid.uuid4().hex[:8]}@anatainc.com"
        employee_id = hr_store.create_employee(
            email=email, full_name="Not Ready"
        )

        result = self._post(
            f"/admin/hr/employees/{employee_id}/invite", {}, self.sa
        )

        self.assertEqual(result.status_code, 303)
        self.assertIn("err=hr_login_email_required", result.headers["location"])
        self.assertIsNone(access_store.get_user_by_email(email))

    def test_hr_login_must_be_personal_and_unique(self):
        import uuid
        first = f"login-first-{uuid.uuid4().hex[:8]}@anatainc.com"
        second = f"login-second-{uuid.uuid4().hex[:8]}@anatainc.com"
        shared_login = f"shared-{uuid.uuid4().hex[:8]}@example.com"
        first_id = hr_store.create_employee(email=first, full_name="First")
        second_id = hr_store.create_employee(email=second, full_name="Second")
        self.assertEqual(
            hr_store.set_employee_hr_login_email(
                first_id, "work@anatainc.com", actor="test"
            ),
            (True, "hr_login_saved"),
        )
        self.assertEqual(
            hr_store.set_employee_hr_login_email(
                first_id, shared_login, actor="test"
            ),
            (True, "hr_login_saved"),
        )
        self.assertEqual(
            hr_store.set_employee_hr_login_email(
                second_id, shared_login, actor="test"
            ),
            (False, "hr_login_email_in_use"),
        )

    def test_personal_hr_login_maps_to_existing_employee_history(self):
        import uuid
        email = f"history-{uuid.uuid4().hex[:8]}@anatainc.com"
        login_email = f"history-{uuid.uuid4().hex[:8]}@example.com"
        hr_store.create_employee(
            email=email, hr_login_email=login_email, full_name="History Person"
        )
        uid = access_store.upsert_user(login_email, "History Person")
        access_store.set_user_permissions(uid, ["hr.access"])

        page = self._get("/admin/hr/onboarding", _cookie(login_email))
        self.assertEqual(page.status_code, 200)
        self.assertIn("History Person", page.text)
        dashboard = self._get("/admin/hr", _cookie(login_email))
        self.assertEqual(dashboard.status_code, 200)
        self.assertIn("Anata employee app", dashboard.text)
        clocked_in = self._post(
            "/admin/hr/time/clock", {"action": "in"}, _cookie(login_email)
        )
        self.assertIn("ok=clocked_in", clocked_in.headers["location"])
        self.assertIsNotNone(hr_store.current_clock(email))
        self.assertEqual(hr_store.list_time_entries(email, limit=1)[0]["employee_email"], email)
        self._post("/admin/hr/time/clock", {"action": "out"}, _cookie(login_email))

    def test_team_detail_can_update_leader_and_manage_membership(self):
        import uuid
        suffix = uuid.uuid4().hex[:8]
        team_id = hr_store.create_team(name=f"Ops {suffix}", actor="test")
        hr_store.create_employee(
            email="val@anatainc.com", full_name="Val", status="active"
        )
        employee_id = hr_store.create_employee(
            email=f"team-member-{suffix}@anatainc.com", full_name="Team Member"
        )
        assigned = self._post(
            f"/admin/hr/teams/{team_id}/members",
            {"employee_id": str(employee_id), "action": "assign"}, self.sa,
        )
        self.assertIn("ok=team_membership_saved", assigned.headers["location"])
        updated = self._post(
            f"/admin/hr/teams/{team_id}",
            {
                "name": f"Operations {suffix}",
                "manager_email": "val@anatainc.com",
                "description": "Runs day-to-day operations",
            },
            self.sa,
        )
        self.assertIn("ok=team_updated", updated.headers["location"])
        team = hr_store.get_team(team_id)
        self.assertEqual(team["manager_email"], "val@anatainc.com")
        self.assertEqual(team["members"][0]["id"], employee_id)

        removed = self._post(
            f"/admin/hr/teams/{team_id}/members",
            {"employee_id": str(employee_id), "action": "remove"}, self.sa,
        )
        self.assertIn("ok=team_membership_saved", removed.headers["location"])
        self.assertEqual(hr_store.get_team(team_id)["members"], [])

    def test_time_page_explains_period_and_paginates_visible_punches(self):
        page = self._get("/admin/hr/time?page_size=10", self.sa)
        self.assertEqual(page.status_code, 200)
        self.assertIn("View pay period", page.text)
        self.assertIn("does not open or alter payroll", page.text)
        self.assertIn("Page 1 of", page.text)
        self.assertIn("Employees see only their own punches", page.text)
        self.assertIn("Hours this Sunday–Saturday workweek", page.text)
        self.assertIn("Request missed-day review", page.text)

    def test_missed_day_creates_time_only_after_independent_approval(self):
        import uuid
        suffix = uuid.uuid4().hex[:8]
        employee_email = f"missed-{suffix}@anatainc.com"
        reviewer_email = f"reviewer-{suffix}@anatainc.com"
        hr_store.create_employee(email=employee_email, full_name="Missed Punch")
        employee_user = access_store.upsert_user(employee_email, "Missed Punch")
        access_store.set_user_permissions(employee_user, ["hr.access"])
        reviewer_user = access_store.upsert_user(reviewer_email, "Reviewer")
        access_store.set_user_permissions(
            reviewer_user, ["hr.access", "hr.payroll"]
        )
        work_date = date(2026, 7, 24)
        requested = self._post(
            "/admin/hr/time/missed-punch",
            {
                "work_date": str(work_date), "proposed_start": "08:00",
                "proposed_stop": "16:30", "reason": "Forgot both punches",
            },
            _cookie(employee_email),
        )
        self.assertIn("ok=correction_requested", requested.headers["location"])
        self.assertEqual(hr_store.list_time_entries(employee_email), [])
        correction = hr_store.list_time_corrections(employee_email)[0]
        self.assertEqual(correction["original"]["missing"], True)

        own = hr_store.decide_time_correction(
            correction["id"], decision="approved",
            reviewer_reason="Reviewed schedule", actor=employee_email,
        )
        self.assertEqual(own, (False, "self_approval_blocked"))
        approved = self._post(
            f"/admin/hr/time/corrections/{correction['id']}/decision",
            {"decision": "approved", "reviewer_reason": "Confirmed with employee"},
            _cookie(reviewer_email),
        )
        self.assertIn("correction_approved", approved.headers["location"])
        entries = hr_store.list_time_entries(employee_email)
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["date"], work_date)
        self.assertEqual(entries[0]["hours"], 8.5)

    def test_pto_rejects_weekends_holidays_and_overlaps(self):
        import uuid
        email = f"pto-guard-{uuid.uuid4().hex[:8]}@anatainc.com"
        hr_store.create_employee(email=email, full_name="PTO Guard")
        hr_store.upsert_employment_profile(
            email, hire_date=date(2025, 1, 1), pay_basis="fixed_semimonthly",
            fixed_pay_per_period="1000", actor="test"
        )
        self._grant_test_pto(email)
        weekend = hr_store.create_pto_request(
            email, start_date=date(2026, 8, 8), end_date=date(2026, 8, 8),
            hours=8, reason="", actor=email,
        )
        holiday = hr_store.create_pto_request(
            email, start_date=date(2026, 9, 7), end_date=date(2026, 9, 7),
            hours=8, reason="", actor=email,
        )
        self.assertEqual(weekend, (False, "pto_non_workday"))
        self.assertEqual(holiday, (False, "pto_paid_holiday"))

        spanning_weekend = hr_store.create_pto_request(
            email, start_date=date(2026, 8, 7), end_date=date(2026, 8, 10),
            hours=16, reason="Long weekend", actor=email,
        )
        self.assertEqual(spanning_weekend, (True, "pto_requested"))
        request = hr_store.list_pto_requests(email)[0]
        self.assertEqual(request["working_day_count"], 2)
        self.assertEqual(request["excluded_day_count"], 2)

        too_many_hours = hr_store.create_pto_request(
            email, start_date=date(2026, 8, 11), end_date=date(2026, 8, 11),
            hours=12, reason="", actor=email,
        )
        self.assertEqual(too_many_hours, (False, "pto_hours_exceed_workdays"))

    def test_employee_can_withdraw_only_their_own_pending_pto(self):
        import uuid
        suffix = uuid.uuid4().hex[:8]
        employee_email = f"pto-owner-{suffix}@anatainc.com"
        outsider_email = f"pto-outsider-{suffix}@anatainc.com"
        for email in (employee_email, outsider_email):
            hr_store.create_employee(email=email, full_name=email.split("@")[0])
            hr_store.upsert_employment_profile(
                email, hire_date=date(2025, 1, 1),
                pay_basis="fixed_semimonthly", fixed_pay_per_period="1000",
                actor="test",
            )
            user_id = access_store.upsert_user(email, email.split("@")[0])
            access_store.set_user_permissions(user_id, ["hr.access"])
        self._grant_test_pto(employee_email)
        created = hr_store.create_pto_request(
            employee_email, start_date=date(2026, 8, 3),
            end_date=date(2026, 8, 3), hours=8, reason="Personal",
            actor=employee_email,
        )
        self.assertEqual(created, (True, "pto_requested"))
        request_id = hr_store.list_pto_requests(employee_email)[0]["id"]

        outsider = self._post(
            f"/admin/hr/time/pto/{request_id}/withdraw", {},
            _cookie(outsider_email),
        )
        self.assertIn("err=pto_withdraw_not_allowed", outsider.headers["location"])
        owner = self._post(
            f"/admin/hr/time/pto/{request_id}/withdraw", {},
            _cookie(employee_email),
        )
        self.assertIn("ok=pto_withdrawn", owner.headers["location"])
        self.assertEqual(
            hr_store.list_pto_requests(employee_email)[0]["status"], "withdrawn"
        )

    def test_authorized_revoke_releases_approved_pto_without_erasing_history(self):
        import uuid
        email = f"pto-revoke-{uuid.uuid4().hex[:8]}@anatainc.com"
        personal_login = f"pto-revoke-{uuid.uuid4().hex[:8]}@example.com"
        hr_store.create_employee(
            email=email, hr_login_email=personal_login, full_name="PTO Revoke"
        )
        hr_store.upsert_employment_profile(
            email, hire_date=date(2025, 1, 1), pay_basis="fixed_semimonthly",
            fixed_pay_per_period="1000", standard_weekly_hours=40, actor="test",
        )
        self._grant_test_pto(email)
        self.assertEqual(
            hr_store.create_pto_request(
                email, start_date=date(2026, 8, 3), end_date=date(2026, 8, 3),
                hours=8, reason="Personal", actor=email,
            ),
            (True, "pto_requested"),
        )
        request_id = hr_store.list_pto_requests(email)[0]["id"]
        self.assertFalse(hr_store.decide_pto(
            request_id, decision="approved", actor=personal_login
        ))
        self.assertTrue(hr_store.decide_pto(
            request_id, decision="approved", actor="david@anatainc.com"
        ))
        after_approval = hr_store.pto_summary(email)["available"]
        self.assertEqual(
            hr_store.revoke_pto(
                request_id, reason="Self revoke", actor=personal_login
            ),
            (False, "pto_revocation_not_allowed"),
        )
        self.assertEqual(
            hr_store.revoke_pto(
                request_id, reason="Employee no longer needs the day",
                actor="valeria@anatainc.com",
            ),
            (True, "pto_revoked"),
        )
        self.assertEqual(
            hr_store.list_pto_requests(email)[0]["status"], "revoked"
        )
        self.assertEqual(hr_store.pto_summary(email)["available"], after_approval + 8)
        self.assertEqual(
            hr_store.revoke_pto(
                request_id, reason="Duplicate", actor="david@anatainc.com"
            ),
            (False, "pto_revocation_not_allowed"),
        )

    def test_onboarding_correction_preserves_submission_and_shows_employee_reason(self):
        self._post("/admin/hr/onboarding/profile", {
            "personal_email": "david.personal@example.com",
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
        review_page = self._get("/admin/hr/time", self.sa)
        self.assertIn(
            f'aria-label="Required review note for correction #{correction["id"]}"',
            review_page.text,
        )
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

    def test_authorized_manager_can_propose_only_assigned_time_correction(self):
        import uuid
        from datetime import datetime, timezone
        from sqlalchemy.orm import Session
        from sales_support_agent.models.database import get_engine
        from sales_support_agent.models.hr import HRTimeEntry

        suffix = uuid.uuid4().hex[:8]
        manager = f"manager-correction-{suffix}@anatainc.com"
        employee = f"assigned-correction-{suffix}@anatainc.com"
        outsider = f"outside-correction-{suffix}@anatainc.com"
        for email in (employee, outsider):
            hr_store.create_employee(email=email, full_name=email.split("@")[0])
        hr_store.upsert_employment_profile(
            employee, hire_date=date(2026, 1, 1), manager_email=manager,
            actor="test",
        )
        manager_id = access_store.upsert_user(manager, "Time Manager")
        access_store.set_user_permissions(
            manager_id, ["hr.access", "hr.time.approve_team"]
        )
        with Session(get_engine()) as session:
            assigned_entry = HRTimeEntry(
                employee_email=employee, date=date(2026, 7, 23),
                start_time="08:00", stop_time="16:00", hours=8,
                elapsed_seconds=28800, clocked_in_at=datetime.now(timezone.utc),
            )
            outside_entry = HRTimeEntry(
                employee_email=outsider, date=date(2026, 7, 23),
                start_time="08:00", stop_time="16:00", hours=8,
                elapsed_seconds=28800, clocked_in_at=datetime.now(timezone.utc),
            )
            session.add_all([assigned_entry, outside_entry])
            session.commit()
            assigned_id, outside_id = assigned_entry.id, outside_entry.id

        proposed = self._post(
            f"/admin/hr/time/{assigned_id}/correction",
            {
                "proposed_start": "08:15", "proposed_stop": "16:00",
                "reason": "Manager reviewed the employee note",
            },
            _cookie(manager),
        )
        self.assertIn("correction_requested", proposed.headers["location"])
        correction = hr_store.list_time_corrections(employee)[0]
        self.assertEqual(correction["requested_by"], manager)
        self.assertEqual(correction["employee_email"], employee)

        blocked = self._post(
            f"/admin/hr/time/{outside_id}/correction",
            {
                "proposed_start": "08:15", "proposed_stop": "16:00",
                "reason": "Not assigned",
            },
            _cookie(manager),
        )
        self.assertEqual(blocked.status_code, 403)

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

    def test_handbook_publication_is_versioned_and_employee_acknowledges_current(self):
        import uuid

        built_in = self._get("/admin/hr/policies", self.sa)
        versions = {item["version"] for item in hr_store.list_handbooks()}
        self.assertIn("2026.2", versions)
        self.assertIn("effective August 1, 2026", built_in.text)
        self.assertIn("Equal opportunity and respectful workplace", built_in.text)
        self.assertIn("Regular full-time employees", built_in.text)

        version = f"test-{uuid.uuid4().hex[:8]}"
        published = self._post(
            "/admin/hr/settings/handbook",
            {
                "title": "Anata Employee Handbook",
                "version": version,
                "file_url": "https://docs.google.com/document/d/example",
                "attested": "true",
            },
            self.sa,
        )
        self.assertIn("ok=handbook_published", published.headers["location"])

        page = self._get("/admin/hr/policies", self.sa)
        self.assertIn(version, page.text)
        self.assertIn("Open handbook securely", page.text)
        self.assertIn("Acknowledgement required", page.text)

        acknowledged = self._post(
            "/admin/hr/policies/acknowledge",
            {"attested": "true"},
            self.sa,
        )
        self.assertIn("ok=policy_acknowledged", acknowledged.headers["location"])
        history = hr_store.list_handbooks()
        current = next(item for item in history if item["version"] == version)
        self.assertTrue(current["is_active"])
        self.assertGreaterEqual(current["acknowledgement_count"], 1)

        duplicate = self._post(
            "/admin/hr/settings/handbook",
            {
                "title": "Duplicate",
                "version": version,
                "file_url": "https://docs.google.com/document/d/duplicate",
                "attested": "true",
            },
            self.sa,
        )
        self.assertIn("err=handbook_version_exists", duplicate.headers["location"])

        unsafe = self._post(
            "/admin/hr/settings/handbook",
            {
                "title": "Unsafe",
                "version": f"{version}-unsafe",
                "file_url": "javascript:alert(1)",
                "attested": "true",
            },
            self.sa,
        )
        self.assertIn("err=handbook_invalid", unsafe.headers["location"])

    def test_pto_launch_ignores_pre_august_hours_and_prorates_part_time(self):
        import uuid
        from datetime import datetime, timezone
        from sqlalchemy.orm import Session
        from sales_support_agent.models.database import get_engine
        from sales_support_agent.models.hr import HRTimeEntry

        email = f"pto-launch-{uuid.uuid4().hex[:8]}@anatainc.com"
        hr_store.create_employee(email=email, full_name="Part Time Launch")
        hr_store.upsert_employment_profile(
            email, hire_date=date(2026, 1, 1), standard_weekly_hours=20,
            actor="test",
        )
        with Session(get_engine()) as session:
            session.add_all([
                HRTimeEntry(
                    employee_email=email, date=date(2026, 7, 31), hours=520,
                    start_time="08:00", stop_time="16:00",
                    clocked_in_at=datetime.now(timezone.utc),
                ),
                HRTimeEntry(
                    employee_email=email, date=date(2026, 8, 1), hours=52,
                    start_time="08:00", stop_time="16:00",
                    clocked_in_at=datetime.now(timezone.utc),
                ),
            ])
            session.commit()

        employment = hr_store.get_employment_profile(email)
        summary = hr_store.pto_summary(email)
        self.assertEqual(employment["employment_category"], "part_time")
        self.assertEqual(employment["pto_eligible_date"], date(2026, 8, 1))
        self.assertEqual(summary["accrued"], 1.0)
        self.assertEqual(summary["available"], 1.0)

    def test_holiday_policy_starts_august_first_and_prorates_part_time(self):
        import uuid

        email = f"holiday-launch-{uuid.uuid4().hex[:8]}@anatainc.com"
        hr_store.create_employee(email=email, full_name="Part Time Holiday")
        hr_store.upsert_employment_profile(
            email, hire_date=date(2026, 1, 1), standard_weekly_hours=20,
            actor="test",
        )
        july = hr_store.holiday_pay_proposals(
            email, date(2026, 7, 1), date(2026, 7, 31)
        )
        september = hr_store.holiday_pay_proposals(
            email, date(2026, 9, 1), date(2026, 9, 15)
        )
        self.assertEqual(july, [])
        self.assertEqual(len(september), 1)
        self.assertEqual(september[0]["name"], "Labor Day")
        self.assertEqual(september[0]["hours"], 4.0)

        hr_store.upsert_employment_profile(
            email, hire_date=date(2026, 1, 1), standard_weekly_hours=18,
            standard_workdays=[1, 2, 3], actor="test",
        )
        employment = hr_store.get_employment_profile(email)
        self.assertEqual(employment["standard_workdays"], [1, 2, 3])
        self.assertEqual(
            hr_store.holiday_pay_proposals(
                email, date(2026, 9, 1), date(2026, 9, 15)
            ),
            [],
        )
        self.assertEqual(
            hr_store.create_pto_request(
                email, start_date=date(2026, 8, 3), end_date=date(2026, 8, 3),
                hours=4, reason="Not a scheduled Monday", actor=email,
            ),
            (False, "pto_non_workday"),
        )

    def test_employee_workday_schedule_is_visible_and_requires_one_day(self):
        import uuid

        email = f"workdays-{uuid.uuid4().hex[:8]}@anatainc.com"
        login = f"workdays-{uuid.uuid4().hex[:8]}@example.com"
        response = self._post(
            "/admin/hr/employees/new",
            {
                "email": email, "hr_login_email": login,
                "full_name": "Scheduled Employee", "employee_type": "hourly",
                "pay_basis": "hourly", "hourly_rate": "20",
                "standard_weekly_hours": "24",
                "workday_schedule_present": "true",
            },
            self.sa,
        )
        self.assertEqual(response.status_code, 422)
        self.assertIn("Choose at least one normally scheduled workday", response.text)

        created = self._post(
            "/admin/hr/employees/new",
            {
                "email": email, "hr_login_email": login,
                "full_name": "Scheduled Employee", "employee_type": "hourly",
                "pay_basis": "hourly", "hourly_rate": "20",
                "standard_weekly_hours": "24",
                "standard_workdays": [0, 2, 4],
                "workday_schedule_present": "true",
            },
            self.sa,
        )
        self.assertEqual(created.status_code, 303)
        employment = hr_store.get_employment_profile(email)
        self.assertEqual(employment["standard_workdays"], [0, 2, 4])
        page = self._get(
            f"/admin/hr/employees/{hr_store.get_employee_by_email(email)['id']}",
            self.sa,
        )
        self.assertIn("Normally scheduled workdays", page.text)
        self.assertIn("Part-time", page.text)

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
            "padding:22px 16px calc(104px + env(safe-area-inset-bottom))",
            page.text,
        )
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

    def test_payroll_freeze_requires_deliberate_confirmation(self):
        blocked = self._post(
            "/admin/hr/payroll/prepare",
            {"period_date": "2026-08-01"},
            self.sa,
        )

        self.assertEqual(blocked.status_code, 303)
        self.assertIn("/admin/hr/payroll/freeze", blocked.headers["location"])
        self.assertIn("freeze_confirmation_required", blocked.headers["location"])

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

    def test_new_employee_setup_saves_employment_and_one_pay_basis(self):
        import uuid
        email = f"complete-{uuid.uuid4().hex[:8]}@anatainc.com"
        login_email = f"complete-{uuid.uuid4().hex[:8]}@example.com"
        response = self._post(
            "/admin/hr/employees/new",
            {
                "email": email, "hr_login_email": login_email,
                "full_name": "Complete Hire",
                "pay_basis": "fixed_semimonthly", "hourly_rate": "0",
                "fixed_pay_per_period": "1000.00", "hire_date": "2026-08-03",
                "title": "Coordinator", "classification": "exempt",
                "standard_weekly_hours": "40",
            },
            self.sa,
        )
        self.assertEqual(response.status_code, 303)
        employee = hr_store.get_employee_by_email(email)
        self.assertEqual(employee["employee_type"], "salaried")
        employment = hr_store.get_employment_profile(email)
        self.assertEqual(
            employment["fixed_pay_per_period_cents"], 100000
        )
        self.assertEqual(employment["hire_date"], date(2026, 8, 3))

    def test_employee_form_separates_worker_category_from_pay_basis(self):
        response = self._get("/admin/hr/employees/new", self.sa)
        self.assertEqual(response.status_code, 200)
        self.assertIn(">W-2 employee</option>", response.text)
        self.assertIn(">Contractor</option>", response.text)
        self.assertNotIn(">Salaried</option>", response.text)
        self.assertIn("choose hourly or fixed semimonthly pay below", response.text)

    def test_employee_money_is_rejected_instead_of_silently_becoming_zero(self):
        import uuid
        email = f"bad-money-{uuid.uuid4().hex[:8]}@anatainc.com"
        login_email = f"bad-money-{uuid.uuid4().hex[:8]}@example.com"
        invalid = self._post(
            "/admin/hr/employees/new",
            {
                "email": email, "hr_login_email": login_email, "pay_basis": "hourly",
                "hourly_rate": "twenty five",
            },
            self.sa,
        )
        self.assertEqual(invalid.status_code, 422)
        self.assertIn("valid dollar amount", invalid.text)
        self.assertIsNone(hr_store.get_employee_by_email(email))

        conflicting = self._post(
            "/admin/hr/employees/new",
            {
                "email": email, "hr_login_email": login_email, "pay_basis": "hourly",
                "hourly_rate": "25", "fixed_pay_per_period": "1000",
            },
            self.sa,
        )
        self.assertEqual(conflicting.status_code, 422)
        self.assertIn("no fixed check amount", conflicting.text)
        self.assertIsNone(hr_store.get_employee_by_email(email))

    def test_contractor_record_stays_outside_w2_pay_basis(self):
        import uuid
        email = f"contractor-{uuid.uuid4().hex[:8]}@example.com"
        response = self._post(
            "/admin/hr/employees/new",
            {
                "email": email, "full_name": "Overseas Contractor",
                "employee_type": "contractor", "hourly_rate": "0",
                "fixed_pay_per_period": "0",
            },
            self.sa,
        )
        self.assertEqual(response.status_code, 303)
        employee = hr_store.get_employee_by_email(email)
        self.assertEqual(employee["employee_type"], "contractor")
        self.assertEqual(hr_store.get_employment_profile(email), {})

    def test_team_names_are_unique_and_manager_must_be_active_employee(self):
        import uuid
        suffix = uuid.uuid4().hex[:8]
        team_name = f"Validated {suffix}"
        invalid = self._post(
            "/admin/hr/teams",
            {
                "name": team_name,
                "manager_email": f"missing-{suffix}@anatainc.com",
            },
            self.sa,
        )
        self.assertIn("err=team_name_or_manager_invalid", invalid.headers["location"])
        self.assertFalse(any(team["name"] == team_name for team in hr_store.list_teams()))

        manager = f"manager-{suffix}@anatainc.com"
        hr_store.create_employee(email=manager, full_name="Manager")
        created = self._post(
            "/admin/hr/teams",
            {"name": team_name, "manager_email": manager},
            self.sa,
        )
        self.assertIn("ok=team_created", created.headers["location"])
        duplicate = self._post(
            "/admin/hr/teams",
            {"name": team_name.lower(), "manager_email": manager},
            self.sa,
        )
        self.assertIn("err=team_name_or_manager_invalid", duplicate.headers["location"])

    def test_employee_reporting_manager_must_be_an_active_employee(self):
        import uuid
        suffix = uuid.uuid4().hex[:8]
        email = f"report-{suffix}@anatainc.com"
        login_email = f"report-{suffix}@example.com"
        invalid = self._post(
            "/admin/hr/employees/new",
            {
                "email": email, "hr_login_email": login_email,
                "manager_email": f"missing-{suffix}@anatainc.com",
                "pay_basis": "hourly", "hourly_rate": "20",
            },
            self.sa,
        )
        self.assertEqual(invalid.status_code, 422)
        self.assertIn("Manager must be an active employee record", invalid.text)
        self.assertIsNone(hr_store.get_employee_by_email(email))


if __name__ == "__main__":
    unittest.main()
