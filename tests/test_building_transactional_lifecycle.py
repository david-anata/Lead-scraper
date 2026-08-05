from __future__ import annotations

import base64
import dataclasses
import hashlib
import hmac
import json
import os
import tempfile
import time
import unittest
import uuid
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlparse
from unittest.mock import patch
from types import SimpleNamespace

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/building_transactional_boot.db",
)

from fastapi.testclient import TestClient

from sales_support_agent.main import app
from sales_support_agent.models.database import create_session_factory, init_database
from sales_support_agent.models.entities import (
    BuildingContact,
    BuildingReservation,
    BuildingServiceRequest,
    BuildingSpace,
    BuildingTransactionalMessage,
)
from sales_support_agent.services.building_transactional_messages import (
    attempt_booking_message,
)
from sales_support_agent.services.building_calendar import projection_payload


class BuildingTransactionalLifecycleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        path = os.path.join(tempfile.gettempdir(), f"building_transactional_{uuid.uuid4().hex}.db")
        cls.factory = create_session_factory("sqlite:///" + path)
        init_database(cls.factory)
        secret_bytes = b"building-transactional-webhook"
        cls.webhook_secret = "whsec_" + base64.b64encode(secret_bytes).decode()
        app.state.session_factory = cls.factory
        app.state.settings = dataclasses.replace(
            app.state.settings,
            internal_api_key="transactional-key",
            building_campaign_token_secret="customer-status-secret",
            building_public_base_url="https://anatabuilding.com",
            resend_webhook_secret=cls.webhook_secret,
        )
        cls.client = TestClient(app)
        now = datetime.now(timezone.utc)
        with cls.factory() as session:
            session.add(BuildingSpace(
                id="transactional-arena",
                slug="transactional-arena",
                name="The Arena",
                space_type="event",
            ))
            session.add(BuildingContact(
                id="transactional-contact",
                email="customer@example.com",
                full_name="Jordan Customer",
                status="active",
            ))
            session.add(BuildingReservation(
                id="transactional-reservation",
                kind="event",
                status="confirmed",
                contact_id="transactional-contact",
                space_id="transactional-arena",
                starts_at=now + timedelta(days=20),
                ends_at=now + timedelta(days=20, hours=8),
                assigned_owner="events@anatainc.com",
            ))
            session.commit()

    def _signed_headers(self, body: bytes, event_id: str) -> dict[str, str]:
        timestamp = str(int(time.time()))
        signed = b".".join((event_id.encode(), timestamp.encode(), body))
        key = base64.b64decode(self.webhook_secret.removeprefix("whsec_"))
        signature = base64.b64encode(hmac.new(key, signed, hashlib.sha256).digest()).decode()
        return {
            "svix-id": event_id,
            "svix-timestamp": timestamp,
            "svix-signature": f"v1,{signature}",
            "Content-Type": "application/json",
        }

    def test_01_message_is_versioned_idempotent_and_uses_team_signature(self) -> None:
        request = SimpleNamespace(app=self.client.app)
        with (
            patch(
                "sales_support_agent.services.building_transactional_messages.receipt_delivery_ready",
                return_value=(True, ""),
            ),
            patch(
                "sales_support_agent.services.building_transactional_messages.ResendClient.send_message",
                return_value="transactional-message-1",
            ) as send,
        ):
            with self.factory() as session:
                reservation = session.get(BuildingReservation, "transactional-reservation")
                first = attempt_booking_message(
                    session,
                    request=request,
                    reservation=reservation,
                    milestone="booking_confirmed",
                    actor="operator@example.com",
                )
                replay = attempt_booking_message(
                    session,
                    request=request,
                    reservation=reservation,
                    milestone="booking_confirmed",
                    actor="operator@example.com",
                )
                session.commit()
        self.assertEqual(first["status"], "sent")
        self.assertEqual(replay["status"], "sent")
        self.assertEqual(send.call_count, 1)
        with self.factory() as session:
            row = session.query(BuildingTransactionalMessage).one()
            self.assertEqual(row.template_version, 1)
            self.assertIn("The Anata Team", row.body_text)
            self.assertIn("anatabuilding.com/event-status?token=", row.body_text)

    def test_02_delivery_receipt_updates_the_exact_message(self) -> None:
        body = json.dumps({
            "type": "email.delivered",
            "data": {"email_id": "transactional-message-1", "to": ["customer@example.com"]},
        }, separators=(",", ":")).encode()
        response = self.client.post(
            "/api/integrations/resend/webhook",
            content=body,
            headers=self._signed_headers(body, "evt-transactional-delivered"),
        )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertTrue(response.json()["recipient_matched"])
        with self.factory() as session:
            row = session.query(BuildingTransactionalMessage).one()
            self.assertEqual(row.status, "delivered")
            self.assertIsNotNone(row.delivered_at)

    def test_03_customer_change_request_never_changes_booking(self) -> None:
        prepared = self.client.post(
            "/api/internal/building/bookings/transactional-reservation/customer-status-access",
            headers={"X-Internal-Api-Key": "transactional-key"},
            json={"expires_in_days": 30, "actor": "operator@example.com"},
        )
        token = parse_qs(urlparse(prepared.json()["status_url"]).query)["token"][0]
        endpoint = f"/api/public/building/bookings/status/requests?token={token}"
        payload = {
            "request_type": "reschedule",
            "details": "Could we move this event to the following Friday?",
            "requested_starts_at": (datetime.now(timezone.utc) + timedelta(days=27)).isoformat(),
            "requested_ends_at": (datetime.now(timezone.utc) + timedelta(days=27, hours=8)).isoformat(),
        }
        first = self.client.post(
            endpoint,
            headers={"Idempotency-Key": "customer-change-1"},
            json=payload,
        )
        replay = self.client.post(
            endpoint,
            headers={"Idempotency-Key": "customer-change-1"},
            json=payload,
        )
        self.assertEqual(first.status_code, 202, first.text)
        self.assertFalse(first.json()["booking_changed"])
        self.assertTrue(replay.json()["duplicate"])
        with self.factory() as session:
            reservation = session.get(BuildingReservation, "transactional-reservation")
            self.assertEqual(reservation.status, "confirmed")
            request_row = session.query(BuildingServiceRequest).one()
            self.assertEqual(request_row.source, "customer_status")
            self.assertEqual(request_row.status, "new")

    def test_04_customer_calendar_attendee_requires_confirmed_state(self) -> None:
        with self.factory() as session:
            reservation = session.get(BuildingReservation, "transactional-reservation")
            contact = session.get(BuildingContact, "transactional-contact")
            space = session.get(BuildingSpace, "transactional-arena")
            confirmed = projection_payload(reservation, space, contact)
            self.assertEqual(confirmed["attendees"][0]["email"], contact.email)
            reservation.status = "soft_hold"
            held = projection_payload(reservation, space, contact)
            self.assertNotIn("attendees", held)
            session.rollback()


if __name__ == "__main__":
    unittest.main()
