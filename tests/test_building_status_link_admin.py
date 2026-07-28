from __future__ import annotations

import dataclasses
import os
import re
import tempfile
import unittest
import uuid
from datetime import datetime, timedelta, timezone
from unittest import mock

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/building_status_link_admin_boot.db",
)
os.environ.setdefault(
    "ADMIN_DASHBOARD_SESSION_SECRET",
    "building-status-link-admin-session-secret",
)

try:
    from fastapi.testclient import TestClient

    from sales_support_agent.main import app
    from sales_support_agent.models.database import create_session_factory, init_database
    from sales_support_agent.models.entities import (
        BuildingAuditEvent,
        BuildingContact,
        BuildingReservation,
        BuildingSpace,
    )
    from sales_support_agent.services.admin_auth import create_user_session_token

    DEPS = True
except ModuleNotFoundError as exc:
    if exc.name not in {"sqlalchemy", "fastapi"}:
        raise
    DEPS = False


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class BuildingStatusLinkAdminTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        path = os.path.join(
            tempfile.gettempdir(), f"building_status_link_admin_{uuid.uuid4().hex}.db"
        )
        factory = create_session_factory("sqlite:///" + path)
        init_database(factory)
        app.state.session_factory = factory
        app.state.settings = dataclasses.replace(
            app.state.settings,
            internal_api_key="status-admin-internal-key",
            building_campaign_token_secret="status-admin-signing-secret",
            building_public_base_url="https://anatabuilding.com",
        )
        settings = app.state.agent_settings
        token = create_user_session_token(
            settings,
            email="david@anatainc.com",
            name="David",
            role="admin",
        )
        cls.factory = factory
        cls.client = TestClient(app)
        cls.client.cookies.set(settings.admin_cookie_name, token)
        cls.browser_headers = {
            "Origin": "http://testserver",
            "Sec-Fetch-Mode": "navigate",
        }
        now = datetime.now(timezone.utc) + timedelta(days=30)
        with factory() as session:
            session.add_all([
                BuildingContact(
                    id="status-admin-contact",
                    email="status-admin@example.com",
                    full_name="Status Admin Customer",
                    status="active",
                    source="test",
                ),
                BuildingSpace(
                    id="status-admin-arena",
                    slug="status-admin-arena",
                    name="Status Admin Arena",
                    space_type="event",
                    capacity=100,
                    status="available",
                    is_public=False,
                ),
                BuildingReservation(
                    id="status-admin-event",
                    kind="event",
                    space_id="status-admin-arena",
                    contact_id="status-admin-contact",
                    starts_at=now,
                    ends_at=now + timedelta(hours=4),
                    status="inquiry",
                    source="test",
                    created_by="test",
                ),
                BuildingReservation(
                    id="status-admin-workspace",
                    kind="workspace",
                    space_id="status-admin-arena",
                    contact_id="status-admin-contact",
                    starts_at=now,
                    ends_at=now + timedelta(days=30),
                    status="inquiry",
                    source="test",
                    created_by="test",
                ),
            ])
            session.commit()

    def setUp(self) -> None:
        app.state.settings = dataclasses.replace(
            app.state.settings,
            internal_api_key="status-admin-internal-key",
            building_campaign_token_secret="status-admin-signing-secret",
            building_public_base_url="https://anatabuilding.com",
        )
        with self.factory() as session:
            contact = session.get(BuildingContact, "status-admin-contact")
            contact.status = "active"
            event = session.get(BuildingReservation, "status-admin-event")
            event.contact_id = contact.id
            session.commit()
        page = self.client.get("/admin/building")
        match = re.search(r'name="_csrf_token" value="([^"]+)"', page.text)
        self.assertIsNotNone(match, page.text)
        self.csrf = match.group(1)

    def _post(self, reservation_id: str, **data):
        return self.client.post(
            f"/admin/building/reservations/{reservation_id}/customer-status-access",
            headers=self.browser_headers,
            follow_redirects=False,
            data={"_csrf_token": self.csrf, "expires_in_days": "30", **data},
        )

    def test_authorized_staff_prepares_copyable_unsent_link_with_audit(self) -> None:
        listing = self.client.get("/admin/building")
        self.assertIn("Prepare customer status link", listing.text)
        self.assertNotIn("/event-status?token=", listing.text)

        response = self._post("status-admin-event")
        self.assertEqual(response.status_code, 200, response.text)
        self.assertIn("Customer status link prepared", response.text)
        self.assertIn("https://anatabuilding.com/event-status?token=", response.text)
        self.assertIn("Not sent", response.text)
        self.assertIn("Agent made no email, SMS, or provider call", response.text)
        self.assertNotIn("/api/public/building/bookings/status?token=", response.text)
        self.assertEqual(response.headers["cache-control"], "no-store")
        self.assertEqual(response.headers["referrer-policy"], "no-referrer")

        with self.factory() as session:
            audit = (
                session.query(BuildingAuditEvent)
                .filter_by(
                    entity_type="reservation",
                    entity_id="status-admin-event",
                    action="customer_status_access_prepared",
                )
                .order_by(BuildingAuditEvent.created_at.desc())
                .first()
            )
            self.assertIsNotNone(audit)
            self.assertFalse(audit.after_json["sent"])
            self.assertNotIn("token", str(audit.after_json).lower())

    def test_permission_and_csrf_fail_closed(self) -> None:
        limited_user = {
            "email": "limited@example.com",
            "permissions": {"building.manage"},
            "is_superadmin": False,
            "session_issued_at": "",
        }
        with mock.patch(
            "sales_support_agent.services.auth_deps.get_current_user",
            return_value=limited_user,
        ):
            forbidden = self._post("status-admin-event")
        self.assertEqual(forbidden.status_code, 403, forbidden.text)

        invalid_csrf = self.client.post(
            "/admin/building/reservations/status-admin-event/customer-status-access",
            headers=self.browser_headers,
            follow_redirects=False,
            data={"_csrf_token": "invalid", "expires_in_days": "30"},
        )
        self.assertEqual(invalid_csrf.status_code, 403, invalid_csrf.text)

    def test_invalid_subject_expiry_and_configuration_fail_closed(self) -> None:
        for invalid_expiry in ("0", "91"):
            response = self._post(
                "status-admin-event", expires_in_days=invalid_expiry
            )
            self.assertEqual(response.status_code, 303, response.text)
            self.assertIn("error=", response.headers["location"])

        workspace = self._post("status-admin-workspace")
        self.assertEqual(workspace.status_code, 303, workspace.text)
        self.assertIn("error=", workspace.headers["location"])

        with self.factory() as session:
            contact = session.get(BuildingContact, "status-admin-contact")
            contact.status = "inactive"
            session.commit()
        inactive = self._post("status-admin-event")
        self.assertEqual(inactive.status_code, 303, inactive.text)
        self.assertIn("error=", inactive.headers["location"])

        app.state.settings = dataclasses.replace(
            app.state.settings, building_campaign_token_secret=""
        )
        missing_secret = self._post("status-admin-event")
        self.assertEqual(missing_secret.status_code, 303, missing_secret.text)
        self.assertIn("error=", missing_secret.headers["location"])


if __name__ == "__main__":
    unittest.main()
