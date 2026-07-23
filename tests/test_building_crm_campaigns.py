from __future__ import annotations

import dataclasses
import os
import tempfile
import unittest
from unittest import mock

os.environ.setdefault("SALES_AGENT_DB_URL", "sqlite:///" + tempfile.gettempdir() + "/building_crm_boot.db")

try:
    from fastapi.testclient import TestClient
    from sales_support_agent.main import app
    from sales_support_agent.models.database import create_session_factory, init_database
    from sales_support_agent.models.entities import (
        BuildingCampaignRecipient,
        BuildingCommunicationPreference,
        BuildingSuppression,
    )
    DEPS = True
except ModuleNotFoundError as exc:
    if exc.name not in {"sqlalchemy", "fastapi"}:
        raise
    DEPS = False


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class BuildingCrmCampaignTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        path = os.path.join(tempfile.gettempdir(), "building_crm_isolated.db")
        if os.path.exists(path):
            os.remove(path)
        factory = create_session_factory("sqlite:///" + path)
        init_database(factory)
        app.state.session_factory = factory
        app.state.settings = dataclasses.replace(
            app.state.settings,
            internal_api_key="internal-test-key",
            building_campaign_token_secret="campaign-test-secret",
            resend_api_key="resend-test-key",
            resend_from="Anata Building <hello@example.com>",
        )
        cls.factory = factory
        cls.client = TestClient(app)
        cls.headers = {"X-Internal-Api-Key": "internal-test-key"}

    def _contact(self, contact_id: str, email: str, name: str) -> None:
        response = self.client.put(
            f"/api/internal/building/crm/contacts/{contact_id}",
            headers=self.headers,
            json={
                "email": email,
                "full_name": name,
                "source": "test",
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(response.status_code, 200, response.text)

    def _relationship(self, contact_id: str, relationship_type: str, reference: str) -> None:
        response = self.client.post(
            f"/api/internal/building/crm/contacts/{contact_id}/relationships",
            headers=self.headers,
            json={
                "relationship_type": relationship_type,
                "source_reference": reference,
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(response.status_code, 201, response.text)

    def _preference(self, contact_id: str, status: str) -> None:
        response = self.client.put(
            f"/api/internal/building/crm/contacts/{contact_id}/preference",
            headers=self.headers,
            json={
                "marketing_status": status,
                "source": "operator",
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(response.status_code, 200, response.text)

    def test_00_contact_can_hold_multiple_relationships(self) -> None:
        self._contact("contact-tenant", "tenant@example.com", "Taylor Tenant")
        self._relationship("contact-tenant", "tenant", "lease:1")
        self._relationship("contact-tenant", "event_host", "event:1")
        self._preference("contact-tenant", "subscribed")
        response = self.client.get(
            "/api/internal/building/crm/contacts/contact-tenant",
            headers=self.headers,
        )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(
            {item["type"] for item in response.json()["contact"]["relationships"]},
            {"tenant", "event_host"},
        )
        self.assertEqual(response.json()["contact"]["marketing_status"], "subscribed")

    def test_01_segment_preview_explains_inclusion_and_exclusion(self) -> None:
        self._contact("contact-prospect", "prospect@example.com", "Pat Prospect")
        self._relationship("contact-prospect", "prospect", "inquiry:1")
        segment = self.client.put(
            "/api/internal/building/crm/segments/current-tenants",
            headers=self.headers,
            json={
                "id": "current-tenants",
                "name": "Current tenants",
                "relationship_types": ["tenant"],
                "marketing_statuses": ["subscribed"],
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(segment.status_code, 200, segment.text)
        preview = self.client.get(
            "/api/internal/building/crm/segments/current-tenants/preview",
            headers=self.headers,
        )
        self.assertEqual(preview.status_code, 200, preview.text)
        rows = {item["contact_id"]: item for item in preview.json()["contacts"]}
        self.assertTrue(rows["contact-tenant"]["included"])
        self.assertIn("tenant", rows["contact-tenant"]["reason"])
        self.assertFalse(rows["contact-prospect"]["included"])
        self.assertIn("relationship does not match", rows["contact-prospect"]["reason"])

    def test_02_campaign_requires_matching_preview_before_approval(self) -> None:
        draft = self.client.put(
            "/api/internal/building/crm/campaigns/tenant-news-1",
            headers=self.headers,
            json={
                "id": "tenant-news-1",
                "name": "Tenant news",
                "segment_id": "current-tenants",
                "subject": "What is happening at Anata",
                "body_text": "A short building update.",
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(draft.status_code, 200, draft.text)
        bad = self.client.post(
            "/api/internal/building/crm/campaigns/tenant-news-1/approve",
            headers=self.headers,
            json={"preview_hash": "0" * 64, "actor": "approver@example.com"},
        )
        self.assertEqual(bad.status_code, 409)
        preview = self.client.post(
            "/api/internal/building/crm/campaigns/tenant-news-1/preview",
            headers=self.headers,
        )
        self.assertEqual(preview.status_code, 200, preview.text)
        self.assertEqual(preview.json()["included_count"], 1)
        with mock.patch(
            "sales_support_agent.api.building_crm_router.ResendClient"
        ) as client:
            client.return_value.is_configured.return_value = True
            test_send = self.client.post(
                "/api/internal/building/crm/campaigns/tenant-news-1/test-send",
                headers=self.headers,
                json={
                    "email": "operator@example.com",
                    "actor": "operator@example.com",
                },
            )
        self.assertEqual(test_send.status_code, 200, test_send.text)
        client.return_value.send_message.assert_called_once()
        approved = self.client.post(
            "/api/internal/building/crm/campaigns/tenant-news-1/approve",
            headers=self.headers,
            json={
                "preview_hash": preview.json()["preview_hash"],
                "actor": "approver@example.com",
            },
        )
        self.assertEqual(approved.status_code, 200, approved.text)
        self.assertEqual(approved.json()["recipient_count"], 1)

    def test_03_send_rechecks_suppression_after_approval(self) -> None:
        self._preference("contact-tenant", "unsubscribed")
        with mock.patch(
            "sales_support_agent.api.building_crm_router.ResendClient"
        ) as client:
            client.return_value.is_configured.return_value = True
            response = self.client.post(
                "/api/internal/building/crm/campaigns/tenant-news-1/send",
                headers=self.headers,
                json={"actor": "operator@example.com"},
            )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["sent"], 0)
        self.assertEqual(response.json()["suppressed"], 1)
        client.return_value.send_message.assert_not_called()
        with self.factory() as session:
            recipient = session.query(BuildingCampaignRecipient).one()
            self.assertEqual(recipient.status, "suppressed")
            self.assertIsNotNone(session.get(BuildingSuppression, "tenant@example.com"))

    def test_04_signed_unsubscribe_link_changes_marketing_only(self) -> None:
        from sales_support_agent.api.building_crm_router import _unsubscribe_token

        self._preference("contact-tenant", "subscribed")
        token = _unsubscribe_token(
            "campaign-test-secret", "contact-tenant", "tenant@example.com"
        )
        response = self.client.get(
            "/api/public/building/unsubscribe",
            params={"contact_id": "contact-tenant", "token": token},
        )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertIn("You’re unsubscribed", response.text)
        with self.factory() as session:
            preference = session.get(BuildingCommunicationPreference, "contact-tenant")
            self.assertEqual(preference.marketing_status, "unsubscribed")
            self.assertTrue(preference.transactional_allowed)

    def test_05_invalid_unsubscribe_token_fails_closed(self) -> None:
        response = self.client.get(
            "/api/public/building/unsubscribe",
            params={"contact_id": "contact-tenant", "token": "invalid"},
        )
        self.assertEqual(response.status_code, 401)

    def test_06_building_admin_requires_auth_and_is_in_tool_catalog(self) -> None:
        from sales_support_agent.services.access.catalog import ALL_TOOL_KEYS

        self.assertIn("building.manage", ALL_TOOL_KEYS)
        response = self.client.get("/admin/building", follow_redirects=False)
        self.assertIn(response.status_code, {302, 303})
        self.assertEqual(response.headers["location"], "/admin/login")

    def test_07_building_page_renders_empty_and_populated_states(self) -> None:
        from sales_support_agent.services.building_page import render_building_page

        body = render_building_page(
            user={"is_superadmin": True, "permissions": set(), "email": "admin@example.com"},
            spaces=[],
            offerings=[],
            contacts=[],
            segments=[],
            campaigns=[],
            inquiries=[],
            reservations=[],
        )
        self.assertIn("Building Control", body)
        self.assertIn("No spaces entered yet.", body)
        self.assertIn("No building contacts yet.", body)
        self.assertIn("No campaigns yet.", body)
