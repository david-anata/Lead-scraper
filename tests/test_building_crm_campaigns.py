from __future__ import annotations

import dataclasses
import os
import re
import tempfile
import unittest
from unittest import mock

os.environ.setdefault("SALES_AGENT_DB_URL", "sqlite:///" + tempfile.gettempdir() + "/building_crm_boot.db")
os.environ.setdefault("ADMIN_DASHBOARD_SESSION_SECRET", "building-browser-test-secret")

try:
    from fastapi.testclient import TestClient
    from sqlalchemy import select
    from sales_support_agent.main import app
    from sales_support_agent.models.database import create_session_factory, init_database
    from sales_support_agent.models.entities import (
        BuildingCampaignRecipient,
        BuildingCampaign,
        BuildingCommunicationPreference,
        BuildingContact,
        BuildingOffering,
        BuildingRatePlan,
        BuildingSegment,
        BuildingSpace,
        BuildingSuppression,
    )
    from sales_support_agent.services.admin_auth import create_user_session_token
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

    def test_01a_employee_lists_require_owner_and_current_review(self) -> None:
        self._contact("contact-employee", "employee@example.com", "Erin Employee")
        missing_governance = self.client.post(
            "/api/internal/building/crm/contacts/contact-employee/relationships",
            headers=self.headers,
            json={
                "id": "relationship-employee",
                "relationship_type": "tenant_employee",
                "source_reference": "tenant-roster:1",
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(missing_governance.status_code, 422, missing_governance.text)
        relationship = self.client.post(
            "/api/internal/building/crm/contacts/contact-employee/relationships",
            headers=self.headers,
            json={
                "id": "relationship-employee",
                "relationship_type": "tenant_employee",
                "source_reference": "tenant-roster:1",
                "list_owner": "community@example.com",
                "review_due_on": "2099-12-31",
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(relationship.status_code, 201, relationship.text)
        self._preference("contact-employee", "subscribed")
        segment = self.client.put(
            "/api/internal/building/crm/segments/current-employees",
            headers=self.headers,
            json={
                "id": "current-employees",
                "name": "Current tenant employees",
                "relationship_types": ["tenant_employee"],
                "marketing_statuses": ["subscribed"],
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(segment.status_code, 200, segment.text)
        preview = self.client.get(
            "/api/internal/building/crm/segments/current-employees/preview",
            headers=self.headers,
        )
        employee = next(
            row
            for row in preview.json()["contacts"]
            if row["contact_id"] == "contact-employee"
        )
        self.assertTrue(employee["included"])
        review = self.client.put(
            "/api/internal/building/crm/contacts/contact-employee/"
            "relationships/relationship-employee/review",
            headers=self.headers,
            json={
                "list_owner": "new-owner@example.com",
                "review_due_on": "2000-01-01",
                "status": "active",
                "actor": "reviewer@example.com",
            },
        )
        self.assertEqual(review.status_code, 200, review.text)
        expired_preview = self.client.get(
            "/api/internal/building/crm/segments/current-employees/preview",
            headers=self.headers,
        )
        employee = next(
            row
            for row in expired_preview.json()["contacts"]
            if row["contact_id"] == "contact-employee"
        )
        self.assertFalse(employee["included"])
        self.assertIn("review is overdue", employee["reason"])
        renewed = self.client.put(
            "/api/internal/building/crm/contacts/contact-employee/"
            "relationships/relationship-employee/review",
            headers=self.headers,
            json={
                "list_owner": "new-owner@example.com",
                "review_due_on": "2099-12-31",
                "status": "active",
                "actor": "reviewer@example.com",
            },
        )
        self.assertEqual(renewed.status_code, 200, renewed.text)

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
        self.assertEqual(
            preview.json()["sender_identity"],
            "Anata Building <hello@example.com>",
        )
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

    def test_05a_operational_notices_use_relationship_and_transactional_permission(self) -> None:
        self._contact("contact-tenant-two", "tenant-two@example.com", "Terry Tenant")
        self._relationship("contact-tenant-two", "tenant", "lease:2")
        self._preference("contact-tenant-two", "unsubscribed")
        draft = self.client.put(
            "/api/internal/building/crm/campaigns/tenant-operations-1",
            headers=self.headers,
            json={
                "id": "tenant-operations-1",
                "name": "Tenant operations",
                "segment_id": "current-tenants",
                "communication_class": "operational",
                "subject": "Required access update",
                "body_text": "The north entrance will close at 6 p.m.",
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(draft.status_code, 200, draft.text)
        preview = self.client.post(
            "/api/internal/building/crm/campaigns/tenant-operations-1/preview",
            headers=self.headers,
        )
        self.assertEqual(preview.status_code, 200, preview.text)
        self.assertEqual(preview.json()["communication_class"], "operational")
        self.assertEqual(preview.json()["included_count"], 2)
        self.assertIn("marketing opt-out does not apply", preview.json()["permission_rule"])
        with mock.patch(
            "sales_support_agent.api.building_crm_router.ResendClient"
        ) as client:
            client.return_value.is_configured.return_value = True
            self.client.post(
                "/api/internal/building/crm/campaigns/tenant-operations-1/test-send",
                headers=self.headers,
                json={"email": "operator@example.com", "actor": "operator@example.com"},
            )
            approved = self.client.post(
                "/api/internal/building/crm/campaigns/tenant-operations-1/approve",
                headers=self.headers,
                json={
                    "preview_hash": preview.json()["preview_hash"],
                    "actor": "approver@example.com",
                },
            )
            self.assertEqual(approved.status_code, 200, approved.text)
            with self.factory() as session:
                preference = session.get(
                    BuildingCommunicationPreference, "contact-tenant"
                )
                preference.transactional_allowed = False
                session.commit()
            response = self.client.post(
                "/api/internal/building/crm/campaigns/tenant-operations-1/send",
                headers=self.headers,
                json={"actor": "operator@example.com"},
            )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["sent"], 1)
        self.assertEqual(response.json()["suppressed"], 1)
        delivered_text = client.return_value.send_message.call_args.kwargs["text"]
        self.assertIn("required operational notice", delivered_text)
        self.assertNotIn("/unsubscribe", delivered_text)

    def test_05b_operational_campaign_rejects_prospect_audience(self) -> None:
        prospect_segment = self.client.put(
            "/api/internal/building/crm/segments/prospects-only",
            headers=self.headers,
            json={
                "id": "prospects-only",
                "name": "Prospects only",
                "relationship_types": ["prospect"],
                "marketing_statuses": ["subscribed"],
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(prospect_segment.status_code, 200, prospect_segment.text)
        response = self.client.put(
            "/api/internal/building/crm/campaigns/not-really-operational",
            headers=self.headers,
            json={
                "id": "not-really-operational",
                "name": "Invalid operational campaign",
                "segment_id": "prospects-only",
                "communication_class": "operational",
                "subject": "Promotion disguised as operations",
                "body_text": "This must fail closed.",
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(response.status_code, 422, response.text)
        self.assertIn("active audience", response.text)

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
            invoices=[],
            adjustments=[],
            billing_accounts=[],
            billing_schedules=[],
            calendar_projections=[],
            checklists=[],
            service_requests=[],
            can_finance=True,
        )
        self.assertIn("Building Control", body)
        self.assertIn("Operator queue", body)
        self.assertIn("No service requests.", body)
        self.assertIn("No spaces entered yet.", body)
        self.assertIn("No building contacts yet.", body)
        self.assertIn("No campaigns yet.", body)
        self.assertIn("No native invoices yet.", body)

    def test_08_operator_can_save_reviewed_space_and_offering_from_control_room(self) -> None:
        settings = app.state.agent_settings
        cookie = create_user_session_token(
            settings,
            email="david@anatainc.com",
            name="David",
            role="admin",
        )
        self.client.cookies.set(settings.admin_cookie_name, cookie)
        try:
            page = self.client.get("/admin/building")
            self.assertEqual(page.status_code, 200, page.text)
            match = re.search(r'name="_csrf_token" value="([^"]+)"', page.text)
            self.assertIsNotNone(match)
            token = match.group(1)
            browser_headers = {
                "Origin": "http://testserver",
                "Sec-Fetch-Mode": "navigate",
            }
            space = self.client.post(
                "/admin/building/spaces",
                headers=browser_headers,
                follow_redirects=False,
                data={
                    "_csrf_token": token,
                    "space_id": "office-pilot",
                    "slug": "office-pilot",
                    "name": "Pilot Office",
                    "space_type": "private_office",
                    "floor": "Second floor",
                    "capacity": "4",
                    "status": "available",
                    "features": "Natural light, Furnished",
                    "public_description": "A reviewed pilot office.",
                    "is_public": "true",
                },
            )
            self.assertEqual(space.status_code, 303, space.text)
            invalid_media = self.client.post(
                "/admin/building/spaces/office-pilot/media",
                headers=browser_headers,
                follow_redirects=False,
                data={
                    "_csrf_token": token,
                    "media_id": "office-pilot-card",
                    "src": "/media/office-pilot.webp",
                    "placement": "card",
                    "approved": "true",
                },
            )
            self.assertIn("error=", invalid_media.headers["location"])
            media = self.client.post(
                "/admin/building/spaces/office-pilot/media",
                headers=browser_headers,
                follow_redirects=False,
                data={
                    "_csrf_token": token,
                    "media_id": "office-pilot-card",
                    "src": "/media/office-pilot.webp",
                    "kind": "image",
                    "placement": "card",
                    "alt": "Natural-light private office at The Anata Building",
                    "sort_order": "0",
                    "approved": "true",
                },
            )
            self.assertEqual(media.status_code, 303, media.text)
            offering = self.client.post(
                "/admin/building/offerings",
                headers=browser_headers,
                follow_redirects=False,
                data={
                    "_csrf_token": token,
                    "offering_id": "office-pilot-membership",
                    "slug": "office-pilot-membership",
                    "name": "Pilot Office Membership",
                    "offering_type": "private_office",
                    "space_id": "office-pilot",
                    "price_display": "From $1,250/month",
                    "booking_unit": "month",
                    "call_to_action": "tour",
                    "features": "Conference access, Boom Standard",
                    "public_description": "A reviewed pilot offering.",
                    "is_published": "true",
                },
            )
            self.assertEqual(offering.status_code, 303, offering.text)
            rate_plan = self.client.post(
                "/admin/building/rate-plans",
                headers=browser_headers,
                follow_redirects=False,
                data={
                    "_csrf_token": token,
                    "offering_id": "office-pilot-membership",
                    "rate_plan_id": "office-pilot-v1",
                    "version": "1",
                    "name": "Pilot office monthly",
                    "status": "approved",
                    "currency": "USD",
                    "unit_amount_cents": "125000",
                    "public_price_display": "From $1,250/month",
                    "booking_unit": "month",
                    "minimum_units": "1",
                    "deposit_type": "fixed",
                    "deposit_amount_cents": "125000",
                    "deposit_percent": "0",
                    "cancellation_policy": "Thirty days written notice.",
                    "included": "Conference access, mail service",
                    "addons_json": "[]",
                    "effective_from": "2026-01-01",
                    "effective_until": "",
                },
            )
            self.assertEqual(rate_plan.status_code, 303, rate_plan.text)
            contact = self.client.post(
                "/admin/building/contacts",
                headers=browser_headers,
                follow_redirects=False,
                data={
                    "_csrf_token": token,
                    "email": "pilot-tenant@example.com",
                    "full_name": "Pilot Tenant",
                    "company_name": "Pilot Company",
                    "relationship_type": "tenant",
                    "organization": "Pilot Company",
                    "source_reference": "reviewed-pilot",
                    "marketing_status": "subscribed",
                    "consent_confirmed": "true",
                },
            )
            self.assertEqual(contact.status_code, 303, contact.text)
            segment = self.client.post(
                "/admin/building/segments",
                headers=browser_headers,
                follow_redirects=False,
                data={
                    "_csrf_token": token,
                    "segment_id": "pilot-tenants",
                    "name": "Pilot tenants",
                    "relationship_types": ["tenant"],
                    "marketing_statuses": ["subscribed"],
                    "relationship_status": "active",
                    "is_active": "true",
                },
            )
            self.assertEqual(segment.status_code, 303, segment.text)
            campaign = self.client.post(
                "/admin/building/campaigns",
                headers=browser_headers,
                follow_redirects=False,
                data={
                    "_csrf_token": token,
                    "campaign_id": "pilot-welcome",
                    "name": "Pilot welcome",
                    "segment_id": "pilot-tenants",
                    "subject": "Welcome to the building",
                    "body_text": "A good building full of people doing good work.",
                },
            )
            self.assertEqual(campaign.status_code, 303, campaign.text)
            preview = self.client.post(
                "/admin/building/campaigns/pilot-welcome/preview",
                headers=browser_headers,
                follow_redirects=False,
                data={"_csrf_token": token},
            )
            self.assertEqual(preview.status_code, 303, preview.text)
            with (
                mock.patch(
                    "sales_support_agent.api.building_crm_router.ResendClient.is_configured",
                    return_value=True,
                ),
                mock.patch(
                    "sales_support_agent.api.building_crm_router.ResendClient.send_message"
                ) as send_message,
            ):
                test_send = self.client.post(
                    "/admin/building/campaigns/pilot-welcome/test-send",
                    headers=browser_headers,
                    follow_redirects=False,
                    data={
                        "_csrf_token": token,
                        "test_email": "operator@example.com",
                    },
                )
                self.assertEqual(test_send.status_code, 303, test_send.text)
                approve = self.client.post(
                    "/admin/building/campaigns/pilot-welcome/approve",
                    headers=browser_headers,
                    follow_redirects=False,
                    data={"_csrf_token": token},
                )
                self.assertEqual(approve.status_code, 303, approve.text)
                wrong_confirmation = self.client.post(
                    "/admin/building/campaigns/pilot-welcome/send",
                    headers=browser_headers,
                    follow_redirects=False,
                    data={
                        "_csrf_token": token,
                        "confirmation": "send it",
                    },
                )
                self.assertIn("error=", wrong_confirmation.headers["location"])
                sent = self.client.post(
                    "/admin/building/campaigns/pilot-welcome/send",
                    headers=browser_headers,
                    follow_redirects=False,
                    data={
                        "_csrf_token": token,
                        "confirmation": "SEND pilot-welcome",
                    },
                )
                self.assertEqual(sent.status_code, 303, sent.text)
                self.assertEqual(send_message.call_count, 2)
            with self.factory() as session:
                saved_space = session.get(BuildingSpace, "office-pilot")
                saved_offering = session.get(
                    BuildingOffering, "office-pilot-membership"
                )
                saved_rate_plan = session.get(BuildingRatePlan, "office-pilot-v1")
                saved_contact = session.execute(
                    select(BuildingContact).where(
                        BuildingContact.email == "pilot-tenant@example.com"
                    )
                ).scalar_one()
                saved_segment = session.get(BuildingSegment, "pilot-tenants")
                saved_campaign = session.get(BuildingCampaign, "pilot-welcome")
                self.assertEqual(saved_space.capacity, 4)
                self.assertTrue(saved_space.is_public)
                self.assertEqual(saved_space.media_json[0]["id"], "office-pilot-card")
                self.assertTrue(saved_space.media_json[0]["approved"])
                self.assertEqual(saved_offering.space_id, saved_space.id)
                self.assertTrue(saved_offering.is_published)
                self.assertEqual(saved_rate_plan.status, "approved")
                self.assertEqual(saved_rate_plan.deposit_amount_cents, 125000)
                self.assertEqual(saved_contact.full_name, "Pilot Tenant")
                self.assertTrue(saved_segment.is_active)
                self.assertEqual(saved_campaign.status, "sent")
                self.assertTrue(saved_campaign.preview_hash)
        finally:
            self.client.cookies.clear()

    def test_09_browser_write_rejects_missing_csrf_token(self) -> None:
        settings = app.state.agent_settings
        cookie = create_user_session_token(
            settings,
            email="david@anatainc.com",
            name="David",
            role="admin",
        )
        self.client.cookies.set(settings.admin_cookie_name, cookie)
        try:
            response = self.client.post(
                "/admin/building/spaces",
                headers={
                    "Origin": "http://testserver",
                    "Sec-Fetch-Mode": "navigate",
                },
                data={
                    "space_id": "should-not-save",
                    "slug": "should-not-save",
                    "name": "Should Not Save",
                    "space_type": "private_office",
                },
            )
            self.assertEqual(response.status_code, 403)
        finally:
            self.client.cookies.clear()
