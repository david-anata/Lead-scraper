from __future__ import annotations

import dataclasses
import os
import tempfile
import unittest
import uuid
from unittest import mock
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlparse

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/building_customer_status_boot.db",
)

try:
    from fastapi.testclient import TestClient

    from sales_support_agent.main import app
    from sales_support_agent.models.database import create_session_factory, init_database
    from sales_support_agent.models.entities import (
        BuildingAgreement,
        BuildingCalendarProjection,
        BuildingContact,
        BuildingPaymentRequestReadiness,
        BuildingProposal,
        BuildingReservation,
        BuildingSpace,
    )

    DEPS = True
except ModuleNotFoundError as exc:
    if exc.name not in {"sqlalchemy", "fastapi"}:
        raise
    DEPS = False


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class BuildingCustomerStatusTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        path = os.path.join(
            tempfile.gettempdir(), f"building_customer_status_{uuid.uuid4().hex}.db"
        )
        factory = create_session_factory("sqlite:///" + path)
        init_database(factory)
        app.state.session_factory = factory
        app.state.settings = dataclasses.replace(
            app.state.settings,
            internal_api_key="status-internal-key",
            building_campaign_token_secret="status-token-secret",
        )
        cls.factory = factory
        cls.client = TestClient(app)
        cls.headers = {"X-Internal-Api-Key": "status-internal-key"}
        start = datetime.now(timezone.utc) + timedelta(days=30)
        with factory() as session:
            session.add_all([
                BuildingContact(
                    id="status-contact",
                    email="customer@example.com",
                    full_name="Customer Name",
                    status="active",
                    source="test",
                ),
                BuildingSpace(
                    id="status-arena",
                    slug="status-arena",
                    name="The Arena",
                    space_type="event",
                    status="available",
                ),
                BuildingReservation(
                    id="status-event",
                    kind="event",
                    status="soft_hold",
                    space_id="status-arena",
                    contact_id="status-contact",
                    starts_at=start,
                    ends_at=start + timedelta(hours=8),
                    hold_expires_at=datetime.now(timezone.utc) + timedelta(days=2),
                    attendance=80,
                    agreement_status="draft",
                    deposit_status="not_started",
                    assigned_owner="private-operator@example.com",
                    requirements_json={"private_note": "never expose"},
                ),
                BuildingProposal(
                    id="status-quote",
                    reservation_id="status-event",
                    version=2,
                    status="sent",
                    amount_cents=999_999,
                    currency="USD",
                    created_by="operator",
                ),
                BuildingAgreement(
                    id="status-agreement",
                    reservation_id="status-event",
                    version=1,
                    status="draft",
                    preparation_status="reviewed",
                    provider_reference="private-provider-reference",
                ),
                BuildingPaymentRequestReadiness(
                    id="status-payment",
                    reservation_id="status-event",
                    agreement_id="status-agreement",
                    version=1,
                    status="prepared",
                    amount_cents=100_000,
                    currency="USD",
                    checksum="a" * 64,
                ),
                BuildingCalendarProjection(
                    id="status-calendar",
                    reservation_id="status-event",
                    status="pending",
                    desired_action="upsert",
                ),
            ])
            session.commit()

    def setUp(self) -> None:
        with self.factory() as session:
            contact = session.get(BuildingContact, "status-contact")
            contact.status = "active"
            reservation = session.get(BuildingReservation, "status-event")
            reservation.contact_id = "status-contact"
            session.commit()

    def test_prepares_unsent_link_and_returns_redacted_live_status(self) -> None:
        prepared = self.client.post(
            "/api/internal/building/bookings/status-event/customer-status-access",
            headers=self.headers,
            json={"expires_in_days": 14, "actor": "operator@example.com"},
        )
        self.assertEqual(prepared.status_code, 200, prepared.text)
        self.assertFalse(prepared.json()["sent"])
        token = parse_qs(urlparse(prepared.json()["status_url"]).query)["token"][0]
        status = self.client.get(
            "/api/public/building/bookings/status", params={"token": token}
        )
        self.assertEqual(status.status_code, 200, status.text)
        booking = status.json()["booking"]
        self.assertEqual(booking["status"]["label"], "Temporary hold")
        self.assertFalse(booking["status"]["is_booked"])
        self.assertEqual(booking["quote"], {"status": "sent", "version": 2})
        self.assertEqual(booking["agreement"]["preparation_status"], "reviewed")
        self.assertEqual(booking["payment"]["request_status"], "prepared")
        self.assertEqual(booking["operations"]["calendar_projection"], "pending")
        serialized = status.text
        self.assertNotIn("private-operator@example.com", serialized)
        self.assertNotIn("private_note", serialized)
        self.assertNotIn("private-provider-reference", serialized)
        self.assertNotIn("999999", serialized)

    def test_tampered_expired_and_unlinked_access_fail_closed(self) -> None:
        prepared = self.client.post(
            "/api/internal/building/bookings/status-event/customer-status-access",
            headers=self.headers,
            json={"expires_in_days": 1, "actor": "operator@example.com"},
        ).json()
        token = parse_qs(urlparse(prepared["status_url"]).query)["token"][0]
        tampered = self.client.get(
            "/api/public/building/bookings/status",
            params={"token": token[:-1] + ("0" if token[-1] != "0" else "1")},
        )
        self.assertEqual(tampered.status_code, 404, tampered.text)

        with mock.patch(
            "sales_support_agent.api.building_booking_router._now",
            return_value=datetime.now(timezone.utc) + timedelta(days=2),
        ):
            expired_response = self.client.get(
                "/api/public/building/bookings/status", params={"token": token}
            )
        self.assertEqual(expired_response.status_code, 410, expired_response.text)

        with self.factory() as session:
            contact = session.get(BuildingContact, "status-contact")
            contact.status = "inactive"
            session.commit()
        inactive = self.client.get(
            "/api/public/building/bookings/status", params={"token": token}
        )
        self.assertEqual(inactive.status_code, 404, inactive.text)

    def test_missing_secret_and_missing_contact_do_not_issue_links(self) -> None:
        original = app.state.settings
        app.state.settings = dataclasses.replace(
            original, building_campaign_token_secret=""
        )
        try:
            missing_secret = self.client.post(
                "/api/internal/building/bookings/status-event/customer-status-access",
                headers=self.headers,
                json={"expires_in_days": 30, "actor": "operator@example.com"},
            )
        finally:
            app.state.settings = original
        self.assertEqual(missing_secret.status_code, 503, missing_secret.text)

        with self.factory() as session:
            contact = session.get(BuildingContact, "status-contact")
            contact.status = "active"
            reservation = session.get(BuildingReservation, "status-event")
            reservation.contact_id = None
            session.commit()
        missing_contact = self.client.post(
            "/api/internal/building/bookings/status-event/customer-status-access",
            headers=self.headers,
            json={"expires_in_days": 30, "actor": "operator@example.com"},
        )
        self.assertEqual(missing_contact.status_code, 409, missing_contact.text)
