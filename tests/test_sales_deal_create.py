"""Route tests for validated HubSpot deal creation."""

from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/sales_deal_create_test.db",
)

from fastapi.testclient import TestClient  # noqa: E402

from sales_support_agent.integrations.hubspot import HubSpotClient  # noqa: E402
from sales_support_agent.main import app  # noqa: E402
from sales_support_agent.models.database import session_scope  # noqa: E402
from sales_support_agent.models.entities import HubSpotDeal, HubSpotDealContact, SalesDealAsset  # noqa: E402
from sales_support_agent.services.admin_auth import create_user_session_token  # noqa: E402
from sales_support_agent.services.fulfillment_deck import storage as fulfillment_storage  # noqa: E402
from sales_support_agent.services.sales.deal_create import (  # noqa: E402
    DealCreateOptions,
    PipelineOption,
    SelectOption,
)


def _cookie_for(email: str, name: str = "User", role: str = "member"):
    s = app.state.agent_settings
    return s.admin_cookie_name, create_user_session_token(s, email=email, name=name, role=role)


def _fake_options() -> DealCreateOptions:
    return DealCreateOptions(
        pipelines=(
            PipelineOption(
                value="default",
                label="Sales Pipeline",
                detail="default",
                stages=(
                    SelectOption("appointmentscheduled", "Appointment Scheduled", "appointmentscheduled"),
                    SelectOption("qualifiedtobuy", "Qualified To Buy", "qualifiedtobuy"),
                ),
            ),
        ),
        owners=(SelectOption("owner1", "David Narayan", "david@anatainc.com | owner1"),),
        companies=(
            SelectOption("company1", "Anata", "anatainc.com | company1"),
            SelectOption("company_toothy", "My Friend Toothy", "myfriendtoothy.com | company_toothy"),
        ),
        contacts=(
            SelectOption("contact1", "Maya Lee", "maya@anatainc.com | contact1"),
            SelectOption("contact_toothy", "Jamie Buyer", "jamie@myfriendtoothy.com | contact_toothy"),
        ),
        service_lines=(SelectOption("fulfillment", "Fulfillment"), SelectOption("marketing", "Marketing")),
        lead_sources=(SelectOption("agent", "Agent"), SelectOption("website", "Website")),
    )


class SalesDealCreateRouteTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.client = TestClient(app)
        cookie_name, token = _cookie_for("david@anatainc.com", "David")
        cls.client.cookies.set(cookie_name, token)

    def setUp(self) -> None:
        self.settings = app.state.agent_settings
        self._original_token = self.settings.hubspot_api_token
        self._original_portal = self.settings.hubspot_portal_id
        self._original_pipeline = self.settings.hubspot_sales_pipeline_id
        self.options_patcher = patch(
            "sales_support_agent.api.sales_router.load_deal_create_options",
            return_value=_fake_options(),
        )
        self.options_patcher.start()
        with session_scope(app.state.session_factory) as session:
            for row in session.query(HubSpotDealContact).all():
                session.delete(row)
            for row in session.query(HubSpotDeal).all():
                session.delete(row)

    def tearDown(self) -> None:
        object.__setattr__(self.settings, "hubspot_api_token", self._original_token)
        object.__setattr__(self.settings, "hubspot_portal_id", self._original_portal)
        object.__setattr__(self.settings, "hubspot_sales_pipeline_id", self._original_pipeline)
        self.options_patcher.stop()

    def test_create_form_renders_before_dynamic_deal_route(self) -> None:
        resp = self.client.get("/admin/sales/deals/create")
        self.assertEqual(resp.status_code, 200)
        self.assertIn('action="/admin/sales/deals/create"', resp.text)
        self.assertIn("Create HubSpot Deal", resp.text)
        self.assertNotIn("Deal not found", resp.text)

    def test_create_form_uses_readable_dropdowns_for_hubspot_ids(self) -> None:
        resp = self.client.get("/admin/sales/deals/create")
        self.assertEqual(resp.status_code, 200)
        body = resp.text
        self.assertIn('<select id="pipeline" name="pipeline"', body)
        self.assertIn("Sales Pipeline - default", body)
        self.assertIn("Appointment Scheduled - appointmentscheduled", body)
        self.assertIn('<select id="hubspot_owner_id" name="hubspot_owner_id"', body)
        self.assertIn("David Narayan - david@anatainc.com | owner1", body)
        self.assertIn('<select id="company_id" name="company_id"', body)
        self.assertIn("Anata - anatainc.com | company1", body)
        self.assertIn('<select id="contact_id" name="contact_id"', body)
        self.assertIn("Maya Lee - maya@anatainc.com | contact1", body)

    def test_create_form_prefills_from_rate_sheet_context(self) -> None:
        resp = self.client.get(
            "/admin/sales/deals/create"
            "?dealname=TabCo+Fulfillment"
            "&hubspot_company_id=company1"
            "&hubspot_contact_id=contact1"
            "&return_to=/admin/fulfillment/sales/runs/123/review"
            "&rate_sheet_run_id=123"
        )
        self.assertEqual(resp.status_code, 200)
        body = resp.text
        self.assertIn('value="TabCo Fulfillment"', body)
        self.assertIn('value="/admin/fulfillment/sales/runs/123/review"', body)
        self.assertIn('value="123"', body)
        self.assertIn('<option value="company1" selected>', body)
        self.assertIn('<option value="contact1" selected>', body)

    def test_create_form_audits_accessible_company_and_contact_matches(self) -> None:
        resp = self.client.get(
            "/admin/sales/deals/create"
            "?dealname=My+Friend+Toothy+Fulfillment"
            "&company_name=My+Friend+Toothy"
            "&company_domain=myfriendtoothy.com"
            "&contact_email=jamie@myfriendtoothy.com"
        )
        self.assertEqual(resp.status_code, 200)
        body = resp.text
        self.assertIn("Access audit", body)
        self.assertIn("Company auto-selected", body)
        self.assertIn("Contact auto-selected", body)
        self.assertIn('<option value="company_toothy" selected>', body)
        self.assertIn('<option value="contact_toothy" selected>', body)

    def test_create_validates_rules_before_hubspot_call(self) -> None:
        object.__setattr__(self.settings, "hubspot_api_token", "test-token")
        with patch.object(HubSpotClient, "create_deal") as create_deal:
            resp = self.client.post(
                "/admin/sales/deals/create",
                data={"dealname": "Acme"},
                follow_redirects=False,
            )
        self.assertEqual(resp.status_code, 400)
        self.assertIn("Missing required company association", resp.text)
        self.assertIn("Missing required contact association", resp.text)
        create_deal.assert_not_called()

    def test_create_requires_hubspot_token_after_validation(self) -> None:
        object.__setattr__(self.settings, "hubspot_api_token", "")
        resp = self.client.post(
            "/admin/sales/deals/create",
            data={
                "dealname": "Acme Fulfillment",
                "pipeline": "default",
                "dealstage": "appointmentscheduled",
                "anata_service_line": "fulfillment",
                "anata_lead_source_detail": "agent",
                "hubspot_owner_id": "owner1",
                "company_id": "company1",
                "contact_id": "contact1",
            },
            follow_redirects=False,
        )
        self.assertEqual(resp.status_code, 503)
        self.assertIn("HubSpot token is not configured", resp.text)

    def test_create_posts_to_hubspot_mirrors_and_redirects(self) -> None:
        object.__setattr__(self.settings, "hubspot_api_token", "test-token")
        object.__setattr__(self.settings, "hubspot_portal_id", "999")
        created = {
            "id": "deal_new",
            "properties": {
                "dealname": "Acme Fulfillment",
                "pipeline": "default",
                "dealstage": "appointmentscheduled",
                "amount": "12000",
                "hubspot_owner_id": "owner1",
            },
            "createdAt": "2026-06-29T12:00:00Z",
            "updatedAt": "2026-06-29T12:00:00Z",
        }
        with patch.object(HubSpotClient, "create_deal", return_value=created) as create_deal, patch(
            "sales_support_agent.api.sales_router.start_hubspot_sync"
        ) as sync:
            resp = self.client.post(
                "/admin/sales/deals/create",
                data={
                    "dealname": "Acme Fulfillment",
                    "pipeline": "default",
                    "dealstage": "appointmentscheduled",
                    "anata_service_line": "fulfillment",
                    "anata_lead_source_detail": "agent",
                    "hubspot_owner_id": "owner1",
                    "amount": "12000",
                    "company_id": "company1",
                    "contact_id": "contact1",
                },
                follow_redirects=False,
            )

        self.assertEqual(resp.status_code, 303)
        self.assertEqual(
            resp.headers["location"],
            "https://app.hubspot.com/contacts/999/record/0-3/deal_new",
        )
        props = create_deal.call_args.args[0]
        self.assertEqual(props["dealname"], "Acme Fulfillment")
        associations = create_deal.call_args.kwargs["associations"]
        self.assertEqual({a["to"]["id"] for a in associations}, {"company1", "contact1"})
        sync.assert_called_once()
        with session_scope(app.state.session_factory) as session:
            deal = session.get(HubSpotDeal, "deal_new")
            self.assertIsNotNone(deal)
            self.assertEqual(deal.deal_name, "Acme Fulfillment")
            self.assertEqual(deal.amount_cents, 1_200_000)
            self.assertEqual(deal.hubspot_company_id, "company1")
            link = session.query(HubSpotDealContact).filter_by(
                hubspot_deal_id="deal_new",
                hubspot_contact_id="contact1",
            ).one_or_none()
            self.assertIsNotNone(link)

    def test_create_from_rate_sheet_attaches_deal_and_returns_to_review(self) -> None:
        object.__setattr__(self.settings, "hubspot_api_token", "test-token")
        object.__setattr__(self.settings, "hubspot_portal_id", "999")
        run_id = fulfillment_storage.create_run(trigger="test")
        fulfillment_storage.save_draft(
            run_id,
            {
                "prospect": "TabCo",
                "view_path": f"/rate-sheets/tabco/{run_id}/token",
                "prospect_profile": {"company": "TabCo", "brand": "TabCo", "products": []},
            },
        )
        created = {
            "id": "deal_from_rate_sheet",
            "properties": {
                "dealname": "TabCo Fulfillment",
                "pipeline": "default",
                "dealstage": "appointmentscheduled",
                "amount": "12000",
                "hubspot_owner_id": "owner1",
            },
            "createdAt": "2026-06-29T12:00:00Z",
            "updatedAt": "2026-06-29T12:00:00Z",
        }
        with patch.object(HubSpotClient, "create_deal", return_value=created), patch(
            "sales_support_agent.api.sales_router.start_hubspot_sync"
        ), patch("sales_support_agent.services.fulfillment_deck.service.apply_profile_edits") as apply_edits:
            resp = self.client.post(
                "/admin/sales/deals/create",
                data={
                    "dealname": "TabCo Fulfillment",
                    "pipeline": "default",
                    "dealstage": "appointmentscheduled",
                    "anata_service_line": "fulfillment",
                    "anata_lead_source_detail": "agent",
                    "hubspot_owner_id": "owner1",
                    "amount": "12000",
                    "company_id": "company1",
                    "contact_id": "contact1",
                    "return_to": f"/admin/fulfillment/sales/runs/{run_id}/review",
                    "rate_sheet_run_id": str(run_id),
                },
                follow_redirects=False,
            )

        self.assertEqual(resp.status_code, 303)
        self.assertEqual(resp.headers["location"], f"/admin/fulfillment/sales/runs/{run_id}/review")
        apply_edits.assert_called_once()
        with session_scope(app.state.session_factory) as session:
            asset = (
                session.query(SalesDealAsset)
                .filter_by(
                    hubspot_deal_id="deal_from_rate_sheet",
                    asset_type="rate_sheet",
                    run_id=str(run_id),
                )
                .one_or_none()
            )
            self.assertIsNotNone(asset)
            self.assertEqual(asset.url, f"/rate-sheets/tabco/{run_id}/token")


if __name__ == "__main__":
    unittest.main()
