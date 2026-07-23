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
from types import SimpleNamespace
from unittest.mock import patch

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/building_email_webhook_boot.db",
)

try:
    from fastapi.testclient import TestClient

    from sales_support_agent.main import app
    from sales_support_agent.integrations.resend import ResendClient
    from sales_support_agent.models.database import create_session_factory, init_database
    from sales_support_agent.models.entities import (
        BuildingCampaign,
        BuildingCampaignRecipient,
        BuildingCommunicationPreference,
        BuildingContact,
        BuildingEmailEvent,
        BuildingSegment,
        BuildingSuppression,
    )

    DEPS = True
except ModuleNotFoundError as exc:
    if exc.name not in {"sqlalchemy", "fastapi"}:
        raise
    DEPS = False


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class BuildingEmailWebhookTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        path = os.path.join(tempfile.gettempdir(), "building_email_webhook_isolated.db")
        if os.path.exists(path):
            os.remove(path)
        factory = create_session_factory("sqlite:///" + path)
        init_database(factory)
        secret_bytes = b"resend-building-webhook-secret"
        cls.webhook_secret = "whsec_" + base64.b64encode(secret_bytes).decode()
        app.state.session_factory = factory
        app.state.settings = dataclasses.replace(
            app.state.settings,
            resend_webhook_secret=cls.webhook_secret,
        )
        cls.factory = factory
        cls.client = TestClient(app)
        with factory() as session:
            session.add(BuildingContact(
                id="webhook-contact",
                email="tenant@example.com",
                full_name="Tenant Example",
                source="test",
            ))
            session.add(BuildingCommunicationPreference(
                contact_id="webhook-contact",
                marketing_status="subscribed",
                transactional_allowed=True,
            ))
            session.add(BuildingSegment(
                id="webhook-segment",
                name="Webhook segment",
                rules_json={},
                created_by="test",
            ))
            session.add(BuildingCampaign(
                id="webhook-campaign",
                name="Webhook campaign",
                segment_id="webhook-segment",
                subject="Building news",
                body_text="News",
                status="sent",
                created_by="test",
            ))
            session.add(BuildingCampaignRecipient(
                campaign_id="webhook-campaign",
                contact_id="webhook-contact",
                email="tenant@example.com",
                full_name="Tenant Example",
                status="sent",
                provider_message_id="email-message-1",
            ))
            session.commit()

    def _signed_headers(self, body: bytes, event_id: str, *, timestamp: int | None = None) -> dict[str, str]:
        timestamp_value = str(timestamp or int(time.time()))
        signed = b".".join((event_id.encode(), timestamp_value.encode(), body))
        key = base64.b64decode(self.webhook_secret.removeprefix("whsec_"))
        signature = base64.b64encode(
            hmac.new(key, signed, hashlib.sha256).digest()
        ).decode()
        return {
            "svix-id": event_id,
            "svix-timestamp": timestamp_value,
            "svix-signature": f"v1,{signature}",
            "Content-Type": "application/json",
        }

    def _event(self, event_type: str, event_id: str) -> tuple[bytes, dict[str, str]]:
        body = json.dumps({
            "type": event_type,
            "data": {
                "email_id": "email-message-1",
                "to": ["tenant@example.com"],
                "subject": "Building news",
            },
        }, separators=(",", ":")).encode()
        return body, self._signed_headers(body, event_id)

    def test_00_signature_and_replay_window_fail_closed(self) -> None:
        body, headers = self._event("email.bounced", "evt-invalid")
        invalid = self.client.post(
            "/api/integrations/resend/webhook",
            content=body,
            headers={**headers, "svix-signature": "v1,invalid"},
        )
        self.assertEqual(invalid.status_code, 401)
        stale_headers = self._signed_headers(
            body,
            "evt-stale",
            timestamp=int(time.time()) - 301,
        )
        stale = self.client.post(
            "/api/integrations/resend/webhook",
            content=body,
            headers=stale_headers,
        )
        self.assertEqual(stale.status_code, 401)

    def test_00b_resend_client_retains_provider_message_id(self) -> None:
        response = SimpleNamespace(
            status_code=200,
            json=lambda: {"id": "email-provider-123"},
            text="",
        )
        client = ResendClient(SimpleNamespace(
            resend_api_key="test-key",
            resend_from="Anata Building <hello@example.com>",
        ))
        with patch(
            "sales_support_agent.integrations.resend.requests.post",
            return_value=response,
        ):
            provider_id = client.send_message(
                to="tenant@example.com",
                subject="Test",
                text="Message",
            )
        self.assertEqual(provider_id, "email-provider-123")

    def test_01_bounce_suppresses_marketing_and_is_idempotent(self) -> None:
        body, headers = self._event("email.bounced", "evt-bounce-1")
        first = self.client.post(
            "/api/integrations/resend/webhook",
            content=body,
            headers=headers,
        )
        second = self.client.post(
            "/api/integrations/resend/webhook",
            content=body,
            headers=headers,
        )
        self.assertEqual(first.status_code, 200, first.text)
        self.assertTrue(first.json()["suppressed"])
        self.assertTrue(second.json()["duplicate"])
        with self.factory() as session:
            self.assertEqual(session.query(BuildingEmailEvent).count(), 1)
            recipient = session.query(BuildingCampaignRecipient).one()
            suppression = session.get(BuildingSuppression, "tenant@example.com")
            preference = session.get(
                BuildingCommunicationPreference, "webhook-contact"
            )
            self.assertEqual(recipient.status, "bounced")
            self.assertEqual(suppression.reason, "bounce")
            self.assertEqual(suppression.scope, "marketing")
            self.assertEqual(preference.marketing_status, "subscribed")
            self.assertTrue(preference.transactional_allowed)

    def test_02_complaint_unsubscribes_marketing_but_keeps_transactional_permission(self) -> None:
        body, headers = self._event("email.complained", "evt-complaint-1")
        response = self.client.post(
            "/api/integrations/resend/webhook",
            content=body,
            headers=headers,
        )
        self.assertEqual(response.status_code, 200, response.text)
        with self.factory() as session:
            recipient = session.query(BuildingCampaignRecipient).one()
            suppression = session.get(BuildingSuppression, "tenant@example.com")
            preference = session.get(
                BuildingCommunicationPreference, "webhook-contact"
            )
            self.assertEqual(recipient.status, "complained")
            self.assertEqual(suppression.reason, "complaint")
            self.assertEqual(preference.marketing_status, "unsubscribed")
            self.assertTrue(preference.transactional_allowed)


if __name__ == "__main__":
    unittest.main()
