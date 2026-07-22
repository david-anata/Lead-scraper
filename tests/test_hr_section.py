"""HR section — routes, permissions, employee/team CRUD (Phase 0 build)."""

from __future__ import annotations

import os
import tempfile
import unittest

os.environ.setdefault("SALES_AGENT_DB_URL", "sqlite:///" + tempfile.gettempdir() + "/hr_section_test.db")

try:
    from fastapi.testclient import TestClient
    from sales_support_agent.main import app
    from sales_support_agent.services.access import store as access_store
    from sales_support_agent.services.admin_auth import create_user_session_token
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
        self.assertIn("Human Resources", r.text)   # left-side menu present

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
        self.assertEqual(self._get("/admin/hr/payroll", ck).status_code, 403)     # blocked

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

    def test_payroll_page_is_a_control_room_not_payment_claim(self):
        page = self._get("/admin/hr/payroll", self.sa)
        self.assertEqual(page.status_code, 200)
        self.assertIn("Payroll control room", page.text)
        self.assertIn("Not ready for final approval", page.text)
        self.assertNotIn("compute gross/taxes/net and pay employees", page.text)


if __name__ == "__main__":
    unittest.main()
