from __future__ import annotations

import ast
import dataclasses
import os
import re
import tempfile
import unittest
from pathlib import Path

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/building_roster_import_boot.db",
)
os.environ.setdefault(
    "ADMIN_DASHBOARD_SESSION_SECRET",
    "building-roster-import-test-secret",
)

try:
    from fastapi.testclient import TestClient
    from sqlalchemy import select

    from sales_support_agent.main import app
    from sales_support_agent.models.database import (
        create_session_factory,
        init_database,
    )
    from sales_support_agent.models.entities import (
        BuildingAuditEvent,
        BuildingCommunicationPreference,
        BuildingContact,
        BuildingRelationship,
        BuildingRosterImport,
    )
    from sales_support_agent.services.admin_auth import create_user_session_token

    DEPS = True
except ModuleNotFoundError as exc:
    if exc.name not in {"sqlalchemy", "fastapi"}:
        raise
    DEPS = False


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class BuildingRosterImportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        path = os.path.join(
            tempfile.gettempdir(),
            "building_roster_import_isolated.db",
        )
        if os.path.exists(path):
            os.remove(path)
        factory = create_session_factory("sqlite:///" + path)
        init_database(factory)
        app.state.session_factory = factory
        app.state.settings = dataclasses.replace(
            app.state.settings,
            internal_api_key="roster-test-key",
        )
        cls.factory = factory
        cls.client = TestClient(app)

    def setUp(self) -> None:
        with self.factory() as session:
            session.add(
                BuildingContact(
                    id="existing-opt-out",
                    email="existing@example.com",
                    full_name="Existing Person",
                    source="manual",
                )
            )
            session.add(
                BuildingCommunicationPreference(
                    contact_id="existing-opt-out",
                    marketing_status="unsubscribed",
                    marketing_source="unsubscribe",
                    updated_by="existing@example.com",
                )
            )
            session.commit()
        settings = app.state.agent_settings
        cookie = create_user_session_token(
            settings,
            email="david@anatainc.com",
            name="David",
            role="admin",
        )
        self.client.cookies.set(settings.admin_cookie_name, cookie)
        page = self.client.get("/admin/building")
        self.assertEqual(page.status_code, 200, page.text)
        token_match = re.search(r'name="_csrf_token" value="([^"]+)"', page.text)
        self.assertIsNotNone(token_match)
        self.csrf_token = token_match.group(1)
        self.browser_headers = {
            "Origin": "http://testserver",
            "Sec-Fetch-Mode": "navigate",
        }

    def tearDown(self) -> None:
        self.client.cookies.clear()
        with self.factory() as session:
            session.query(BuildingAuditEvent).delete()
            session.query(BuildingRelationship).delete()
            session.query(BuildingCommunicationPreference).delete()
            session.query(BuildingContact).delete()
            session.query(BuildingRosterImport).delete()
            session.commit()

    def test_preview_then_apply_preserves_existing_opt_out(self) -> None:
        csv_text = (
            "email,full_name,company_name,marketing_status,marketing_source,"
            "source_reference\n"
            "existing@example.com,Changed Name,Acme,subscribed,lease-consent,"
            "tenant-roster\n"
            "new@example.com,New Person,Acme,subscribed,signup-form,"
            "tenant-roster\n"
        )
        preview = self.client.post(
            "/admin/building/roster-imports/preview",
            headers=self.browser_headers,
            follow_redirects=False,
            data={
                "_csrf_token": self.csrf_token,
                "csv_text": csv_text,
                "filename": "acme-team.csv",
                "relationship_type": "tenant_employee",
                "organization": "Acme",
                "list_owner": "community@anatainc.com",
                "review_due_on": "2099-12-31",
            },
        )
        self.assertEqual(preview.status_code, 303, preview.text)
        self.assertIn("Roster+preview+ready", preview.headers["location"])
        with self.factory() as session:
            roster = session.query(BuildingRosterImport).one()
            self.assertEqual(roster.row_count, 2)
            self.assertEqual(roster.new_contact_count, 1)
            self.assertEqual(roster.existing_contact_count, 1)
            self.assertEqual(session.query(BuildingContact).count(), 1)
            roster_id = roster.id

        review_page = self.client.get("/admin/building")
        self.assertEqual(review_page.status_code, 200, review_page.text)
        self.assertIn("Review exact contacts", review_page.text)
        self.assertIn("existing@example.com", review_page.text)
        self.assertIn("new@example.com", review_page.text)
        self.assertIn(f"IMPORT {roster_id}", review_page.text)

        wrong = self.client.post(
            f"/admin/building/roster-imports/{roster_id}/apply",
            headers=self.browser_headers,
            follow_redirects=False,
            data={
                "_csrf_token": self.csrf_token,
                "confirmation": "IMPORT",
            },
        )
        self.assertIn("error=", wrong.headers["location"])
        with self.factory() as session:
            self.assertEqual(session.query(BuildingContact).count(), 1)

        applied = self.client.post(
            f"/admin/building/roster-imports/{roster_id}/apply",
            headers=self.browser_headers,
            follow_redirects=False,
            data={
                "_csrf_token": self.csrf_token,
                "confirmation": f"IMPORT {roster_id}",
            },
        )
        self.assertEqual(applied.status_code, 303, applied.text)
        self.assertIn("opt-outs+preserved", applied.headers["location"])
        with self.factory() as session:
            existing = session.get(BuildingContact, "existing-opt-out")
            existing_preference = session.get(
                BuildingCommunicationPreference,
                existing.id,
            )
            new_contact = session.execute(
                select(BuildingContact).where(
                    BuildingContact.email == "new@example.com"
                )
            ).scalar_one()
            new_preference = session.get(
                BuildingCommunicationPreference,
                new_contact.id,
            )
            self.assertEqual(existing.full_name, "Existing Person")
            self.assertEqual(existing_preference.marketing_status, "unsubscribed")
            self.assertEqual(new_preference.marketing_status, "subscribed")
            relationships = session.query(BuildingRelationship).all()
            self.assertEqual(len(relationships), 2)
            self.assertTrue(
                all(item.relationship_type == "tenant_employee" for item in relationships)
            )
            self.assertTrue(
                all(
                    item.metadata_json["list_owner"] == "community@anatainc.com"
                    for item in relationships
                )
            )
            roster = session.get(BuildingRosterImport, roster_id)
            self.assertEqual(roster.status, "applied")
            actions = {
                item.action
                for item in session.query(BuildingAuditEvent).all()
            }
            self.assertIn("previewed_from_control_room", actions)
            self.assertIn("applied_from_control_room", actions)

    def test_preview_rejects_subscribed_without_consent_source(self) -> None:
        response = self.client.post(
            "/admin/building/roster-imports/preview",
            headers=self.browser_headers,
            follow_redirects=False,
            data={
                "_csrf_token": self.csrf_token,
                "csv_text": (
                    "email,full_name,marketing_status\n"
                    "person@example.com,Person,subscribed\n"
                ),
                "filename": "unsafe.csv",
                "relationship_type": "tenant",
                "organization": "Acme",
                "list_owner": "",
                "review_due_on": "",
            },
        )
        self.assertEqual(response.status_code, 303, response.text)
        self.assertIn("error=", response.headers["location"])
        with self.factory() as session:
            self.assertEqual(session.query(BuildingRosterImport).count(), 0)

    def test_building_roster_code_uses_production_python_grammar(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        for relative_path in (
            "sales_support_agent/api/building_crm_router.py",
            "sales_support_agent/services/building_page.py",
        ):
            source = (repo_root / relative_path).read_text(encoding="utf-8")
            ast.parse(source, filename=relative_path, feature_version=(3, 11))
