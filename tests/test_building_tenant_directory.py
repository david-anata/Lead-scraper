"""Tenant directory admin page, CSV import, and bot recipient read."""

from __future__ import annotations

import dataclasses
import os
import re
import tempfile
import unittest
from pathlib import Path

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/building_tenant_directory_boot.db",
)
os.environ.setdefault(
    "ADMIN_DASHBOARD_SESSION_SECRET",
    "building-tenant-directory-test-secret",
)

try:
    from fastapi.testclient import TestClient

    from sales_support_agent.main import app
    from sales_support_agent.models.database import create_session_factory, init_database
    from sales_support_agent.models.entities import (
        BuildingAuditEvent,
        BuildingTenantDirectoryContact,
    )
    from sales_support_agent.services.admin_auth import create_user_session_token

    DEPS = True
except ModuleNotFoundError as exc:
    if exc.name not in {"sqlalchemy", "fastapi"}:
        raise
    DEPS = False

MIGRATION = Path("supabase/migrations/20261110205000_building_tenant_directory.sql")
UNIFI_CSV = """First Name,Last Name,Email,Phone Number,User Group,User Status,Office
Ada,Lovelace,ada@example.com,801-555-0101,Analytical Engines,ACTIVE,Suite 12
Grace,Hopper,grace@example.com,801-555-0102,Navy,DEACTIVATED,Suite 4
Pat,Ready,pat@example.com,801-555-0103,Analytical Engines,ACTIVE,Suite 12
Pat,Again,pat@example.com,801-555-0103,Analytical Engines,ACTIVE,Suite 12
,Missing,not-an-email,555,Orphan,ACTIVE,Suite 1
"""


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class BuildingTenantDirectoryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        path = os.path.join(tempfile.gettempdir(), "building_tenant_directory_isolated.db")
        if os.path.exists(path):
            os.remove(path)
        factory = create_session_factory("sqlite:///" + path)
        init_database(factory)
        app.state.session_factory = factory
        app.state.settings = dataclasses.replace(
            app.state.settings,
            internal_api_key="tenant-directory-internal-key",
        )
        cls.factory = factory
        cls.client = TestClient(app)
        settings = app.state.agent_settings
        token = create_user_session_token(
            settings,
            email="david@anatainc.com",
            name="David",
            role="admin",
        )
        cls.client.cookies.set(settings.admin_cookie_name, token)
        cls.cookie_name = settings.admin_cookie_name
        page = cls.client.get("/admin/building/tenants")
        if page.status_code != 200:
            raise AssertionError(page.text)
        match = re.search(r'name="_csrf_token" value="([^"]+)"', page.text)
        if match is None:
            raise AssertionError("Tenant directory did not render a CSRF token.")
        cls.csrf = match.group(1)
        cls.browser_headers = {
            "Origin": "http://testserver",
            "Sec-Fetch-Mode": "navigate",
        }
        cls.internal_headers = {"X-Internal-Api-Key": "tenant-directory-internal-key"}

    def _post(self, path: str, data: dict, *, files=None):
        payload = {"_csrf_token": self.csrf, **data}
        return self.client.post(
            path,
            headers=self.browser_headers,
            follow_redirects=False,
            data=payload,
            files=files,
        )

    def test_migration_is_additive_and_locks_down_the_new_table(self) -> None:
        sql = MIGRATION.read_text()
        folded = sql.upper()
        self.assertIn("CREATE TABLE IF NOT EXISTS building_tenant_directory_contacts", sql)
        for column in (
            "full_name",
            "company_name",
            "email",
            "phone",
            "office_suite",
            "august_lock",
            "is_active",
            "receives_notices",
            "notes",
        ):
            self.assertIn(column, sql)
        self.assertIn("ENABLE ROW LEVEL SECURITY", sql)
        self.assertIn("REVOKE ALL ON TABLE building_tenant_directory_contacts FROM anon, authenticated", sql)
        self.assertIn("GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE building_tenant_directory_contacts TO agent_app", sql)
        self.assertNotIn("DROP TABLE", folded)
        self.assertNotIn("DELETE FROM", folded)
        self.assertNotIn("TRUNCATE", folded)
        self.assertNotIn("UPDATE ", folded)
        altered = [
            line for line in sql.splitlines()
            if line.upper().startswith("ALTER TABLE")
            and "building_tenant_directory_contacts" not in line
        ]
        self.assertEqual(altered, [])

    def test_page_requires_a_building_operator(self) -> None:
        anonymous = TestClient(app)
        response = anonymous.get("/admin/building/tenants", follow_redirects=False)
        self.assertIn(response.status_code, {302, 303})
        self.assertIn("/admin/login", response.headers["location"])
        outsider = TestClient(app)
        token = create_user_session_token(
            app.state.agent_settings,
            email="nobody@example.com",
            name="Nobody",
            role="user",
        )
        outsider.cookies.set(self.cookie_name, token)
        denied = outsider.get("/admin/building/tenants", follow_redirects=False)
        self.assertEqual(denied.status_code, 403)

    def test_directory_supports_several_contacts_and_excludes_owners(self) -> None:
        page = self.client.get("/admin/building/tenants")
        self.assertEqual(page.status_code, 200)
        self.assertIn("Tenant directory", page.text)
        self.assertIn("Fondell Industrial", page.text)
        self.assertIn("Copy notice recipients", page.text)

        created = self._post(
            "/admin/building/tenants",
            {
                "full_name": "Ada Lovelace",
                "company_name": "Analytical Engines",
                "email": "Ada@Example.com",
                "phone": "801-555-0101",
                "office_suite": "Suite 12",
                "has_august_lock": "true",
                "august_lock_name": "Front door",
                "receives_notices": "true",
                "is_active": "true",
                "notes": "Primary contact",
            },
        )
        self.assertEqual(created.status_code, 303, created.text)
        self.assertIn("notice=", created.headers["location"])
        teammate = self._post(
            "/admin/building/tenants",
            {
                "full_name": "Charles Babbage",
                "company_name": "Analytical Engines",
                "email": "charles@example.com",
                "office_suite": "Suite 14",
                "receives_notices": "true",
                "is_active": "true",
            },
        )
        self.assertEqual(teammate.status_code, 303, teammate.text)
        owner = self._post(
            "/admin/building/tenants",
            {
                "full_name": "Owner Desk",
                "company_name": "Fondell Industrial",
                "email": "owner@example.com",
                "office_suite": "Owner",
                "is_active": "true",
            },
        )
        self.assertEqual(owner.status_code, 303, owner.text)

        listing = self.client.get("/admin/building/tenants")
        self.assertIn("ada@example.com", listing.text)
        self.assertIn("charles@example.com", listing.text)
        self.assertIn("owner@example.com", listing.text)
        self.assertIn("Front door", listing.text)
        notices = self.client.get("/admin/building/tenants?status=notices")
        self.assertIn("ada@example.com", notices.text)
        self.assertNotIn("owner@example.com", notices.text)

        duplicate = self._post(
            "/admin/building/tenants",
            {
                "full_name": "Ada Again",
                "company_name": "Analytical Engines",
                "email": "ada@example.com",
                "receives_notices": "true",
                "is_active": "true",
            },
        )
        self.assertEqual(duplicate.status_code, 303)
        self.assertIn("error=", duplicate.headers["location"])

        found = self.client.get("/admin/building/tenants?q=suite%2014")
        self.assertIn("1 contact", found.text)
        self.assertIn("Charles Babbage", found.text)
        self.assertNotIn("Ada Lovelace", found.text)

        with self.factory() as session:
            ada = (
                session.query(BuildingTenantDirectoryContact)
                .filter(BuildingTenantDirectoryContact.email == "ada@example.com")
                .one()
            )
            ada_id = ada.id
            audits = (
                session.query(BuildingAuditEvent)
                .filter(BuildingAuditEvent.entity_id == ada_id)
                .all()
            )
            self.assertTrue(any(row.action == "tenant_directory_created" for row in audits))

        edited = self._post(
            "/admin/building/tenants",
            {
                "contact_id": ada_id,
                "full_name": "Ada Lovelace",
                "company_name": "Analytical Engines",
                "email": "ada@example.com",
                "phone": "801-555-0190",
                "office_suite": "Suite 12",
                "has_august_lock": "true",
                "august_lock_name": "Front door",
                "receives_notices": "true",
                "is_active": "true",
            },
        )
        self.assertEqual(edited.status_code, 303, edited.text)
        self.assertIn("801-555-0190", self.client.get("/admin/building/tenants").text)

        cleared_lock = self._post(
            "/admin/building/tenants",
            {
                "contact_id": ada_id,
                "full_name": "Ada Lovelace",
                "company_name": "Analytical Engines",
                "email": "ada@example.com",
                "phone": "801-555-0190",
                "office_suite": "Suite 12",
                "august_lock_name": "Front door",
                "receives_notices": "true",
                "is_active": "true",
            },
        )
        self.assertEqual(cleared_lock.status_code, 303, cleared_lock.text)
        with self.factory() as session:
            ada = (
                session.query(BuildingTenantDirectoryContact)
                .filter(BuildingTenantDirectoryContact.email == "ada@example.com")
                .one()
            )
            self.assertEqual(ada.august_lock, "")

        deactivated = self._post(f"/admin/building/tenants/{ada_id}/deactivate", {})
        self.assertEqual(deactivated.status_code, 303, deactivated.text)
        active_page = self.client.get("/admin/building/tenants")
        self.assertNotIn("ada@example.com", active_page.text)
        inactive_page = self.client.get("/admin/building/tenants?status=inactive")
        self.assertIn("ada@example.com", inactive_page.text)

        admin_copy = self.client.get("/admin/building/tenants/notice-recipients")
        self.assertEqual(admin_copy.status_code, 200)
        body = admin_copy.json()
        self.assertEqual(body["emails"], ["charles@example.com"])
        self.assertEqual(body["recipients"][0]["company_name"], "Analytical Engines")
        self.assertNotIn("notes", body["recipients"][0])

        missing_key = self.client.get("/api/internal/building/tenant-directory/notice-recipients")
        self.assertEqual(missing_key.status_code, 401)
        bot = self.client.get(
            "/api/internal/building/tenant-directory/notice-recipients",
            headers=self.internal_headers,
        )
        self.assertEqual(bot.status_code, 200)
        self.assertEqual(bot.json()["emails"], ["charles@example.com"])
        self.assertEqual(bot.headers["cache-control"], "no-store")

    def test_unifi_csv_preview_skips_duplicates_and_imports_the_rest(self) -> None:
        preview = self._post(
            "/admin/building/tenants/import/preview",
            {},
            files={"csv_file": ("unifi.csv", UNIFI_CSV, "text/csv")},
        )
        self.assertEqual(preview.status_code, 200, preview.text)
        self.assertIn("Already saved", preview.text)
        self.assertIn("Duplicate in file", preview.text)
        self.assertIn("Needs a fix", preview.text)
        self.assertIn("pat@example.com", preview.text)
        self.assertIn("grace@example.com", preview.text)
        token = re.search(r'name="preview_token" value="([^"]+)"', preview.text)
        csv_text = re.search(r'name="csv_text" hidden>(.*?)</textarea>', preview.text, re.S)
        self.assertIsNotNone(token)
        self.assertIsNotNone(csv_text)
        applied = self._post(
            "/admin/building/tenants/import/apply",
            {"preview_token": token.group(1), "csv_text": csv_text.group(1)},
        )
        self.assertEqual(applied.status_code, 303, applied.text)
        self.assertIn("notice=", applied.headers["location"])
        with self.factory() as session:
            emails = {
                row.email
                for row in session.query(BuildingTenantDirectoryContact).all()
            }
            grace = (
                session.query(BuildingTenantDirectoryContact)
                .filter(BuildingTenantDirectoryContact.email == "grace@example.com")
                .one()
            )
        self.assertIn("pat@example.com", emails)
        self.assertIn("grace@example.com", emails)
        self.assertEqual(sum(1 for email in emails if email == "pat@example.com"), 1)
        self.assertEqual(grace.company_name, "Navy")
        self.assertFalse(grace.is_active)
        bot = self.client.get(
            "/api/internal/building/tenant-directory/notice-recipients",
            headers=self.internal_headers,
        )
        bot_emails = bot.json()["emails"]
        self.assertIn("pat@example.com", bot_emails)
        self.assertNotIn("grace@example.com", bot_emails)
        self.assertNotIn("owner@example.com", bot_emails)
        self.assertEqual(bot_emails.count("pat@example.com"), 1)

        crlf_csv = (
            "name,email,company\r\n"
            "Line Ending,line-ending@example.com,Line Co\r\n"
        )
        crlf_preview = self._post(
            "/admin/building/tenants/import/preview",
            {},
            files={"csv_file": ("line-endings.csv", crlf_csv.replace("\r\n", "\n"), "text/csv")},
        )
        self.assertEqual(crlf_preview.status_code, 200, crlf_preview.text)
        crlf_token = re.search(r'name="preview_token" value="([^"]+)"', crlf_preview.text)
        self.assertIsNotNone(crlf_token)
        crlf_applied = self._post(
            "/admin/building/tenants/import/apply",
            {"preview_token": crlf_token.group(1), "csv_text": crlf_csv},
        )
        self.assertEqual(crlf_applied.status_code, 303, crlf_applied.text)
        self.assertIn("notice=", crlf_applied.headers["location"])
        with self.factory() as session:
            self.assertIsNotNone(
                session.query(BuildingTenantDirectoryContact)
                .filter(BuildingTenantDirectoryContact.email == "line-ending@example.com")
                .one_or_none()
            )

        tampered = self._post(
            "/admin/building/tenants/import/apply",
            {
                "preview_token": token.group(1),
                "csv_text": "name,email,company\nNew Person,new@example.com,New Co\n",
            },
        )
        self.assertIn("error=", tampered.headers["location"])
        with self.factory() as session:
            self.assertIsNone(
                session.query(BuildingTenantDirectoryContact)
                .filter(BuildingTenantDirectoryContact.email == "new@example.com")
                .one_or_none()
            )
