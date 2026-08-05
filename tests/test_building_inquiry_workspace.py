from __future__ import annotations

import dataclasses
import os
import re
import tempfile
import unittest
import uuid
from unittest import mock
from datetime import date, datetime, timedelta, timezone

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/building_inquiry_workspace_boot.db",
)
os.environ.setdefault(
    "ADMIN_DASHBOARD_SESSION_SECRET",
    "building-inquiry-workspace-session",
)

from fastapi.testclient import TestClient

from sales_support_agent.main import app
from sales_support_agent.models.database import create_session_factory, init_database
from sales_support_agent.models.entities import (
    BuildingAuditEvent,
    BuildingContact,
    BuildingInquiry,
    BuildingRelationship,
)
from sales_support_agent.services.admin_auth import create_user_session_token


class BuildingInquiryWorkspaceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        path = os.path.join(
            tempfile.gettempdir(), f"building_inquiry_workspace_{uuid.uuid4().hex}.db"
        )
        cls.factory = create_session_factory("sqlite:///" + path)
        init_database(cls.factory)
        app.state.session_factory = cls.factory
        app.state.settings = dataclasses.replace(
            app.state.settings,
            internal_api_key="inquiry-workspace-internal",
            building_campaign_token_secret="inquiry-workspace-csrf",
        )
        cls.client = TestClient(app)
        token = create_user_session_token(
            app.state.agent_settings,
            email="david@anatainc.com",
            name="David",
            role="admin",
        )
        cls.client.cookies.set(app.state.agent_settings.admin_cookie_name, token)
        now = datetime.now(timezone.utc)
        with cls.factory() as session:
            session.add(BuildingContact(
                id="jordan-contact",
                email="jordan@example.com",
                phone="801-555-0142",
                full_name="Jordan Rivera",
            ))
            session.add(BuildingRelationship(
                id="jordan-inquiry-relationship",
                contact_id="jordan-contact",
                relationship_type="event_host",
                source_reference="inquiry:jordan-inquiry",
            ))
            session.add(BuildingInquiry(
                id="jordan-inquiry",
                idempotency_key="jordan-inquiry-key",
                kind="event",
                source="anata-building",
                source_reference="event-inquiry",
                name="Jordan Rivera",
                email="jordan@example.com",
                preferred_date=date.today() + timedelta(days=30),
                assigned_owner="building@anatainc.com",
                response_due_at=now - timedelta(hours=1),
                payload_json={
                    "eventType": "Company celebration",
                    "groupSize": "85",
                    "alternateDate": "2026-09-19",
                    "backupDate2": "2026-09-26",
                    "dateFlexibility": "Same month is workable",
                    "accessStartTime": "14:00",
                    "accessEndTime": "23:00",
                    "avNeeds": "Presentation display",
                    "accessibilityNeeds": "Step-free guest route",
                    "vendorPlan": "Caterer only",
                    "tourInterest": "Yes, please contact me",
                    "notes": "Need a stage and accessible seating.",
                    "_lifecycle": {"stage": "new"},
                    "_lead_notification": {"status": "delivered"},
                    "_customer_receipt": {"status": "sent"},
                },
            ))
            session.add(BuildingInquiry(
                id="internal-qa-inquiry",
                idempotency_key="internal-qa-inquiry-key",
                kind="event",
                source="production_qa",
                name="Production QA Test",
                email="building+qa@anatainc.com",
                payload_json={
                    "_lifecycle": {"stage": "new"},
                    "_lead_notification": {"status": "failed"},
                    "_customer_receipt": {"status": "not_configured"},
                },
            ))
            session.add(BuildingAuditEvent(
                entity_type="inquiry",
                entity_id="jordan-inquiry",
                action="inquiry_created",
                actor="public-site",
            ))
            session.commit()

    def test_workspace_preserves_submission_and_excludes_other_records(self) -> None:
        page = self.client.get("/admin/building/inquiries/jordan-inquiry")
        self.assertEqual(page.status_code, 200, page.text)
        self.assertEqual(page.headers["cache-control"], "private, no-store")
        self.assertIn("Jordan Rivera", page.text)
        self.assertIn("Company celebration", page.text)
        self.assertIn("Estimated attendance", page.text)
        self.assertIn("Third-choice date", page.text)
        self.assertIn("Date flexibility", page.text)
        self.assertIn("Setup or vendor access begins", page.text)
        self.assertIn("Accessibility needs", page.text)
        self.assertIn("Need a stage and accessible seating.", page.text)
        self.assertIn("Do this next", page.text)
        self.assertIn("Record response", page.text)
        self.assertIn('href="mailto:jordan@example.com"', page.text)
        self.assertIn("801-555-0142", page.text)
        self.assertIn("Staff Slack alert", page.text)
        self.assertNotIn("Production QA Test", page.text)

    def test_sales_defaults_to_prospects_and_supports_search_and_test_scope(self) -> None:
        default = self.client.get("/admin/building/sales")
        self.assertEqual(default.status_code, 200, default.text)
        self.assertIn("Jordan Rivera", default.text)
        self.assertNotIn("Production QA Test", default.text)
        self.assertIn('href="/admin/building/inquiries/jordan-inquiry"', default.text)
        self.assertIn("recognized test records are hidden", default.text)

        tests = self.client.get("/admin/building/sales?lead_scope=test&lead_status=all")
        self.assertIn("Production QA Test", tests.text)
        self.assertNotIn(
            'href="/admin/building/inquiries/jordan-inquiry"', tests.text
        )

        missing = self.client.get("/admin/building/sales?q=not-a-real-prospect")
        self.assertNotIn(
            'href="/admin/building/inquiries/jordan-inquiry"', missing.text
        )

    def test_today_links_directly_to_the_customer_workspace(self) -> None:
        page = self.client.get("/admin/building")
        self.assertIn('href="/admin/building/inquiries/jordan-inquiry"', page.text)

    def test_failed_delivery_evidence_has_permissioned_retry_actions(self) -> None:
        page = self.client.get("/admin/building/inquiries/internal-qa-inquiry")
        self.assertEqual(page.status_code, 200, page.text)
        self.assertIn("Retry staff alert", page.text)
        self.assertIn("Retry acknowledgement", page.text)
        self.assertIn('name="_csrf_token"', page.text)

    def test_delivered_staff_alert_retry_is_idempotent(self) -> None:
        page = self.client.get("/admin/building/inquiries/jordan-inquiry")
        token = re.search(r'name="_csrf_token" value="([^"]+)"', page.text)
        self.assertIsNotNone(token)
        with mock.patch(
            "sales_support_agent.api.building_admin_operations_router.notify_new_building_lead",
            side_effect=AssertionError("Delivered alerts must not be sent twice."),
        ):
            response = self.client.post(
                "/admin/building/inquiries/jordan-inquiry/notify",
                data={"_csrf_token": token.group(1)},
                headers={
                    "Origin": "http://testserver",
                    "Sec-Fetch-Mode": "navigate",
                },
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303, response.text)
        self.assertIn("already+delivered", response.headers["location"])

    def test_missing_and_unauthenticated_records_fail_closed(self) -> None:
        self.assertEqual(
            self.client.get("/admin/building/inquiries/not-real").status_code, 404
        )
        guest = TestClient(app)
        denied = guest.get(
            "/admin/building/inquiries/jordan-inquiry", follow_redirects=False
        )
        self.assertIn(denied.status_code, {302, 303})
        self.assertEqual(denied.headers["location"], "/admin/login")


if __name__ == "__main__":
    unittest.main()
