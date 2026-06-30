"""Route tests for Fulfillment > Sales Deck: tool gating, generate -> review
-> publish flow, draft gating of the public view, edit round-trip, heartbeat
persistence. Real backend app + temp SQLite (same harness as test_access_rbac)."""

from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault("SALES_AGENT_DB_URL", "sqlite:///" + tempfile.gettempdir() + "/fulfillment_deck_routes_test.db")

from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.orm import Session

from sales_support_agent.main import app
from sales_support_agent.models.database import create_session_factory, get_engine, init_database
from sales_support_agent.models.entities import (
    DeckVisitSession,
    HubSpotCompany,
    HubSpotContact,
    HubSpotDeal,
    HubSpotDealContact,
    SalesDealAsset,
)
from sales_support_agent.services.access import store
from sales_support_agent.services.admin_auth import create_user_session_token
from sales_support_agent.services.fulfillment_deck import storage

_NOTES = "Brand: TabCo\nWidget — 6 x 5 x 3 in, 1.5 lb, ~500 units/mo"
_BASE = "/admin/fulfillment/sales"


def _cookie_for(email: str, name: str = "User", role: str = "member"):
    s = app.state.agent_settings
    return s.admin_cookie_name, create_user_session_token(s, email=email, name=name, role=role)


class FulfillmentDeckRouteTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        # Other test files (e.g. test_clickup_sync's ":memory:" factories) reassign
        # the module-level database.engine global. Re-pin it to this suite's DB so
        # routes that write through get_engine() don't hit a stale/readonly engine
        # when we run inside the full suite rather than in isolation.
        factory = create_session_factory(os.environ["SALES_AGENT_DB_URL"])
        init_database(factory)
        cls.client = TestClient(app)
        cookie_name, token = _cookie_for("david@anatainc.com", "David")  # seeded superadmin
        cls.client.cookies.set(cookie_name, token)

    def setUp(self) -> None:
        patcher = mock.patch.dict(
            os.environ, {"ANTHROPIC_API_KEY": "", "ANATA_WMS_BASE_URL": ""}, clear=False
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def _generate(self) -> dict:
        """Generate a rate sheet; returns the (draft) history row."""
        response = self.client.post(
            f"{_BASE}/generate",
            data={"notes": _NOTES, "origin_zip": "84043"},
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        runs = storage.list_runs()
        self.assertTrue(runs)
        return runs[0]

    def _generate_published(self) -> dict:
        run = self._generate()
        response = self.client.post(
            f"{_BASE}/runs/{run['id']}/publish", follow_redirects=False
        )
        self.assertEqual(response.status_code, 303)
        return next(r for r in storage.list_runs() if r["id"] == run["id"])

    def test_landing_renders_for_superadmin(self) -> None:
        response = self.client.get(_BASE)
        self.assertEqual(response.status_code, 200)
        self.assertIn("Rate", response.text)
        self.assertIn("Generate rate sheet", response.text)
        self.assertIn("84043", response.text)

    def test_tool_gate_blocks_unauthorized_user(self) -> None:
        existing = store.get_role_by_name("FinanceOnlyRateSheet")
        rid = existing["id"] if existing else store.create_role("FinanceOnlyRateSheet", ["finance"], description="")
        store.upsert_user("fin_rs@anatainc.com", "Fin", role_id=rid)
        blocked = TestClient(app)
        cookie_name, token = _cookie_for("fin_rs@anatainc.com", "Fin")
        blocked.cookies.set(cookie_name, token)
        response = blocked.get(_BASE, follow_redirects=False)
        self.assertEqual(response.status_code, 403)

    def test_generate_requires_some_input(self) -> None:
        response = self.client.post(
            f"{_BASE}/generate", data={"notes": ""}, follow_redirects=False
        )
        self.assertEqual(response.status_code, 303)
        self.assertIn("kind=warn", response.headers["location"])

    # ------------------------------------------------------------------
    # Draft -> review -> publish flow
    # ------------------------------------------------------------------

    def test_generate_redirects_to_review_page(self) -> None:
        response = self.client.post(
            f"{_BASE}/generate",
            data={"notes": _NOTES, "origin_zip": "84043"},
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        run = storage.list_runs()[0]
        self.assertEqual(
            response.headers["location"], f"{_BASE}/runs/{run['id']}/review"
        )
        self.assertEqual(run["status"], "draft")

    def test_generate_from_hubspot_deal_links_rate_sheet_asset(self) -> None:
        with Session(get_engine()) as s:
            for model in (SalesDealAsset, HubSpotDealContact, HubSpotContact, HubSpotDeal, HubSpotCompany):
                for row in s.query(model).all():
                    if getattr(row, "hubspot_deal_id", "") in {"ctx_deal"} or getattr(row, "hubspot_company_id", "") in {"ctx_co"} or getattr(row, "hubspot_contact_id", "") in {"ctx_contact"}:
                        s.delete(row)
            s.flush()
            s.add(HubSpotCompany(hubspot_company_id="ctx_co", name="Context Co", domain=""))
            s.add(HubSpotDeal(
                hubspot_deal_id="ctx_deal",
                deal_name="Context Co - Fulfillment",
                hubspot_company_id="ctx_co",
                is_closed=False,
            ))
            s.add(HubSpotContact(hubspot_contact_id="ctx_contact", email="buyer@contextco.com"))
            s.add(HubSpotDealContact(hubspot_deal_id="ctx_deal", hubspot_contact_id="ctx_contact"))
            s.commit()

        response = self.client.post(
            f"{_BASE}/generate",
            data={
                "notes": "Needs 3PL pricing for 500 orders/mo.",
                "origin_zip": "84043",
                "hubspot_deal_id": "ctx_deal",
                "hubspot_company_id": "ctx_co",
                "hubspot_contact_ids": "ctx_contact",
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        run = storage.list_runs()[0]
        summary = dict(storage.get_run(run["id"]).summary_json)
        self.assertEqual(summary["hubspot_deal_id"], "ctx_deal")
        self.assertIn("sales_pricing", summary)
        with Session(get_engine()) as s:
            asset = (
                s.query(SalesDealAsset)
                .filter_by(hubspot_deal_id="ctx_deal", asset_type="rate_sheet", run_id=str(run["id"]))
                .first()
            )
            self.assertIsNotNone(asset)
            self.assertEqual(asset.url, summary["view_path"])

    def test_review_page_renders_for_draft(self) -> None:
        run = self._generate()
        response = self.client.get(f"{_BASE}/runs/{run['id']}/review")
        self.assertEqual(response.status_code, 200)
        self.assertIn("TabCo", response.text)
        self.assertIn(f"{_BASE}/runs/{run['id']}/preview", response.text)
        self.assertIn("Publish — get shareable link", response.text)
        self.assertIn("Save &amp; re-render", response.text)
        self.assertIn("Widget", response.text)

    def test_draft_public_view_is_404_but_admin_preview_works(self) -> None:
        run = self._generate()
        public = TestClient(app)
        self.assertEqual(public.get(run["view_path"]).status_code, 404)
        # Heartbeat is gated too.
        hb = public.post(
            run["view_path"] + "/heartbeat",
            json={"visitor_token": "draft-gate-token", "total_seconds": 1},
        )
        self.assertEqual(hb.status_code, 404)
        # Admin preview serves the draft HTML.
        preview = self.client.get(f"{_BASE}/runs/{run['id']}/preview")
        self.assertEqual(preview.status_code, 200)
        self.assertIn("TabCo", preview.text)
        self.assertIn("window.print()", preview.text)
        # Preview is admin-gated.
        self.assertNotEqual(
            TestClient(app).get(f"{_BASE}/runs/{run['id']}/preview", follow_redirects=False).status_code,
            200,
        )

    def test_publish_activates_public_view_and_token_gate(self) -> None:
        run = self._generate_published()
        self.assertEqual(run["status"], "completed")
        self.assertTrue(run["published"])
        view_path = run["view_path"]
        self.assertTrue(view_path.startswith("/rate-sheets/"))

        public = TestClient(app)  # logged-out client
        response = public.get(view_path)
        self.assertEqual(response.status_code, 200)
        self.assertIn("TabCo", response.text)
        self.assertIn("window.print()", response.text)

        bad = view_path.rsplit("/", 1)[0] + "/" + "0" * 32
        self.assertEqual(public.get(bad).status_code, 404)

    def test_publish_redirect_flash_contains_public_link(self) -> None:
        run = self._generate()
        response = self.client.post(
            f"{_BASE}/runs/{run['id']}/publish", follow_redirects=False
        )
        self.assertEqual(response.status_code, 303)
        self.assertIn("/review", response.headers["location"])

    def test_history_shows_draft_pill_then_open_after_publish(self) -> None:
        run = self._generate()
        page = self.client.get(_BASE).text
        self.assertIn("Draft", page)
        self.assertIn(f"{_BASE}/runs/{run['id']}/review", page)
        self.assertNotIn(f'href="{run["view_path"]}?viewer=internal"', page)

        self.client.post(f"{_BASE}/runs/{run['id']}/publish", follow_redirects=False)
        page = self.client.get(_BASE).text
        self.assertIn('class="action-menu"', page)
        self.assertIn('aria-label="Actions for TabCo"', page)
        self.assertIn(f'href="{run["view_path"]}?viewer=internal"', page)
        # Published rows expose the review link as an "Edit" action inside the row menu.
        self.assertIn(f'href="{_BASE}/runs/{run["id"]}/review"', page)
        self.assertIn(">Edit</a>", page)
        self.assertIn(">Share</button>", page)
        self.assertIn(">Create Quote</button>", page)

    def test_update_route_round_trip(self) -> None:
        run = self._generate()
        response = self.client.post(
            f"{_BASE}/runs/{run['id']}/update",
            data={
                "brand": "TabCo Prime",
                "origin_zip": "84043",
                "monthly_order_volume": "750",
                "current_cost_per_parcel_usd": "12.40",
                "destinations_note": "East Coast heavy",
                "current_costs_note": "About $12.40 with FedEx",
                "product_name": ["Widget XL"],
                "product_length": ["9"],
                "product_width": ["7"],
                "product_height": ["4"],
                "product_weight": ["2.25"],
                "product_units": ["750"],
                "product_estimated": ["0"],
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        self.assertIn(f"/runs/{run['id']}/review", response.headers["location"])

        review = self.client.get(f"{_BASE}/runs/{run['id']}/review")
        self.assertEqual(review.status_code, 200)
        self.assertIn("TabCo Prime", review.text)
        self.assertIn("Widget XL", review.text)
        self.assertIn('value="9"', review.text)
        self.assertIn("East Coast heavy", review.text)

        summary = dict(storage.get_run(run["id"]).summary_json)
        profile = summary["prospect_profile"]
        self.assertEqual(profile["brand"], "TabCo Prime")
        self.assertEqual(profile["monthly_order_volume"], 750)
        self.assertEqual(profile["current_cost_per_parcel_usd"], 12.40)
        self.assertEqual(len(profile["products"]), 1)
        self.assertEqual(profile["products"][0]["length_in"], 9.0)
        # Re-rendered HTML reflects the edit.
        self.assertIn("9 × 7 × 4 in", summary["deck_html"])

    def test_review_page_can_select_deal_and_mark_pricing_ready_for_quote(self) -> None:
        run = self._generate_published()
        with Session(get_engine()) as s:
            for model in (SalesDealAsset, HubSpotDeal):
                for row in s.query(model).all():
                    if getattr(row, "hubspot_deal_id", "") == "quote_ready_deal":
                        s.delete(row)
            s.add(HubSpotDeal(
                hubspot_deal_id="quote_ready_deal",
                deal_name="TabCo Fulfillment",
                is_closed=False,
            ))
            s.commit()

        review = self.client.get(f"{_BASE}/runs/{run['id']}/review")
        self.assertEqual(review.status_code, 200)
        self.assertIn("Deal &amp; Quote Readiness", review.text)
        self.assertIn("TabCo Fulfillment", review.text)
        self.assertIn('name="hubspot_deal_id"', review.text)

        response = self.client.post(
            f"{_BASE}/runs/{run['id']}/update",
            data={
                "brand": "TabCo",
                "origin_zip": "84043",
                "hubspot_deal_id": "quote_ready_deal",
                "sales_pricing_reviewed": "1",
                "product_name": ["Widget"],
                "product_length": ["6"],
                "product_width": ["5"],
                "product_height": ["3"],
                "product_weight": ["1.5"],
                "product_units": ["500"],
                "product_estimated": ["0"],
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        summary = dict(storage.get_run(run["id"]).summary_json)
        self.assertEqual(summary["hubspot_deal_id"], "quote_ready_deal")
        self.assertTrue(summary["sales_pricing"]["reviewed"])
        from sales_support_agent.services.fulfillment_deck.pricing_rules import validate_quote_readiness
        self.assertEqual(validate_quote_readiness(summary, published=True), [])
        with Session(get_engine()) as s:
            asset = (
                s.query(SalesDealAsset)
                .filter_by(hubspot_deal_id="quote_ready_deal", asset_type="rate_sheet", run_id=str(run["id"]))
                .first()
            )
            self.assertIsNotNone(asset)
            self.assertEqual(asset.url, summary["view_path"])

    def test_review_page_offers_create_deal_from_rate_sheet(self) -> None:
        run = self._generate_published()
        review = self.client.get(f"{_BASE}/runs/{run['id']}/review")
        self.assertEqual(review.status_code, 200)
        self.assertIn("Create new HubSpot deal", review.text)
        self.assertIn("/admin/sales/deals/create?", review.text)
        self.assertIn(f"rate_sheet_run_id={run['id']}", review.text)
        self.assertIn(f"return_to=%2Fadmin%2Ffulfillment%2Fsales%2Fruns%2F{run['id']}%2Freview", review.text)

    def test_update_route_quote_margin_override_round_trip(self) -> None:
        run = self._generate()
        # Review page exposes the override input (blank = automatic).
        review = self.client.get(f"{_BASE}/runs/{run['id']}/review")
        self.assertIn("Quote margin override %", review.text)
        self.assertIn('name="quote_margin_override"', review.text)

        response = self.client.post(
            f"{_BASE}/runs/{run['id']}/update",
            data={
                "brand": "TabCo",
                "origin_zip": "84043",
                "quote_margin_override": "12",
                "product_name": ["Widget"],
                "product_length": ["6"],
                "product_width": ["5"],
                "product_height": ["3"],
                "product_weight": ["1.5"],
                "product_units": ["500"],
                "product_estimated": ["0"],
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        summary = dict(storage.get_run(run["id"]).summary_json)
        self.assertEqual(summary["quote_margin_override"], 12.0)
        self.assertEqual(summary["fulfillment_quote"]["multiplier"], 1.12)
        self.assertEqual(summary["fulfillment_quote"]["margin_override_pct"], 12.0)
        # The re-rendered sheet carries the quote section.
        self.assertIn('data-key="quote"', summary["deck_html"])
        review = self.client.get(f"{_BASE}/runs/{run['id']}/review")
        self.assertIn('value="12"', review.text)

        # Blank clears the override -> back to automatic category margins.
        response = self.client.post(
            f"{_BASE}/runs/{run['id']}/update",
            data={
                "brand": "TabCo",
                "origin_zip": "84043",
                "quote_margin_override": "",
                "product_name": ["Widget"],
                "product_length": ["6"],
                "product_width": ["5"],
                "product_height": ["3"],
                "product_weight": ["1.5"],
                "product_units": ["500"],
                "product_estimated": ["0"],
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        summary = dict(storage.get_run(run["id"]).summary_json)
        self.assertIsNone(summary["quote_margin_override"])
        self.assertNotEqual(summary["fulfillment_quote"]["multiplier"], 1.12)

    def test_customer_fee_overrides_update_quote_and_margin_review(self) -> None:
        run = self._generate_published()
        response = self.client.post(
            f"{_BASE}/runs/{run['id']}/update",
            data={
                "brand": "TabCo",
                "origin_zip": "84043",
                "rate_pick_pack": "2",
                "rate_additional_item": "0.50",
                "rate_integration_setup_fee": "1500",
                "actual_costs_form": "1",
                "actual_pick_pack_per_order": "0.80",
                "actual_pick_pack_additional_item": "0.15",
                "actual_storage_per_pallet_mo": "30",
                "actual_monthly_tech_fee": "50",
                "actual_customer_service_monthly": "0",
                "sales_pricing_reviewed": "1",
                "product_name": ["Widget"],
                "product_length": ["6"],
                "product_width": ["5"],
                "product_height": ["3"],
                "product_weight": ["1.5"],
                "product_units": ["500"],
                "product_estimated": ["0"],
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        summary = dict(storage.get_run(run["id"]).summary_json)
        self.assertEqual(summary["rate_overrides"]["dtc_base_per_order"], 2.0)
        self.assertEqual(summary["rate_overrides"]["integration_setup_fee"], 1500.0)
        implementation_fee = next(
            fee for fee in summary["fulfillment_quote"]["one_time"]
            if fee.get("key") == "implementation"
        )
        self.assertEqual(implementation_fee["amount"], 1500.0)
        self.assertEqual(summary["fulfillment_actual_costs"]["pick_pack_per_order"], 0.80)
        self.assertEqual(summary["fulfillment_actual_costs"]["customer_service_monthly"], 0.0)
        self.assertIn("negotiation_history", summary)
        self.assertEqual(summary["negotiation_history"][-1]["event"], "Saved and re-rendered")
        pick_pack = next(line for line in summary["fulfillment_quote"]["lines"] if line.get("key") == "pick_pack")
        self.assertGreaterEqual(float(pick_pack["rate"]), 2.0)

        review = self.client.get(f"{_BASE}/runs/{run['id']}/review")
        self.assertEqual(review.status_code, 200)
        self.assertIn("Pricing definitions", review.text)
        self.assertIn("Customer-facing monthly estimate", review.text)
        self.assertIn("Estimated monthly net margin", review.text)
        self.assertIn("Internal Fulfillment Costs", review.text)
        self.assertIn('class="review-section"', review.text)
        self.assertIn("Save &amp; re-render agent preview", review.text)
        self.assertIn("Re-publish live sheet", review.text)
        self.assertIn("Negotiation history", review.text)
        self.assertIn('name="actual_pick_pack_per_order"', review.text)
        self.assertIn("Fulfillment pick &amp; pack cost", review.text)
        self.assertIn("Customer fee: DTC pick &amp; pack / order", review.text)
        self.assertIn("Customer fee: one-time implementation &amp; integration setup", review.text)

    def test_pipeline_cost_save_preserves_zero_values(self) -> None:
        run = self._generate_published()
        response = self.client.patch(
            f"{_BASE}/runs/{run['id']}/costs",
            json={
                "pick_pack_per_order": 0,
                "monthly_tech_fee": 0,
                "storage_per_pallet_mo": 30,
            },
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn("marginable_revenue", response.json()["margin"])
        summary = dict(storage.get_run(run["id"]).summary_json)
        self.assertEqual(summary["fulfillment_actual_costs"]["pick_pack_per_order"], 0.0)
        self.assertEqual(summary["fulfillment_actual_costs"]["monthly_tech_fee"], 0.0)
        self.assertEqual(summary["fulfillment_actual_costs"]["storage_per_pallet_mo"], 30.0)

    def test_shared_fulfillment_cost_form_saves_without_sales_pricing(self) -> None:
        run = self._generate()
        summary = dict(storage.get_run(run["id"]).summary_json)
        path = f"/fulfillment-costs/{run['id']}/{summary['export_token']}"
        public = TestClient(app)

        page = public.get(path)
        self.assertEqual(page.status_code, 200)
        self.assertIn("Anata fulfillment cost input", page.text)
        self.assertIn("Save fulfillment costs", page.text)
        self.assertIn("Suggested:", page.text)
        self.assertNotIn("Customer-facing monthly estimate", page.text)
        self.assertNotIn("Fee Card Adjustments", page.text)
        self.assertNotIn("Estimated monthly net margin", page.text)

        bad = public.get(f"/fulfillment-costs/{run['id']}/{'0' * 32}")
        self.assertEqual(bad.status_code, 404)

        posted = public.post(
            path,
            data={
                "actual_pick_pack_per_order": "0.91",
                "actual_pick_pack_additional_item": "0.16",
                "actual_storage_per_pallet_mo": "31",
                "actual_monthly_tech_fee": "0",
                "actual_customer_service_monthly": "200",
            },
            follow_redirects=False,
        )
        self.assertEqual(posted.status_code, 303)
        self.assertIn("saved=1", posted.headers["location"])
        updated = dict(storage.get_run(run["id"]).summary_json)
        self.assertEqual(updated["fulfillment_actual_costs"]["pick_pack_per_order"], 0.91)
        self.assertEqual(updated["fulfillment_actual_costs"]["monthly_tech_fee"], 0.0)
        self.assertEqual(updated["negotiation_history"][-1]["event"], "Fulfillment costs submitted")

    def test_review_page_exposes_fulfillment_cost_form_link(self) -> None:
        run = self._generate()
        response = self.client.get(f"{_BASE}/runs/{run['id']}/review")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Fulfillment cost form.", response.text)
        self.assertIn("Copy cost form link", response.text)
        self.assertIn("/fulfillment-costs/", response.text)

    def test_review_page_shows_volume_basis_hint(self) -> None:
        run = self._generate()
        summary = dict(storage.get_run(run["id"]).summary_json)
        profile = dict(summary["prospect_profile"])
        profile["volume_basis"] = "300 Shopify + 200 Amazon"
        profile["volume_provenance"] = "RFP deck p.2 orders table"
        storage.update_summary(run["id"], {"prospect_profile": profile})
        review = self.client.get(f"{_BASE}/runs/{run['id']}/review")
        # v5: BOTH the arithmetic and where it came from render as the
        # vetting hint; the public sheet only ever shows the basis.
        self.assertIn("Basis: 300 Shopify + 200 Amazon", review.text)
        self.assertIn("Source: RFP deck p.2 orders table", review.text)
        # Re-rendered public HTML never leaks the provenance.
        from sales_support_agent.config import load_settings
        from sales_support_agent.services.fulfillment_deck.service import (
            rerender_rate_sheet,
        )

        rerendered = rerender_rate_sheet(run["id"], settings=load_settings())
        self.assertNotIn("RFP deck p.2 orders table", rerendered["deck_html"])
        self.assertEqual(
            rerendered["prospect_profile"]["volume_provenance"],
            "RFP deck p.2 orders table",
        )

    def test_review_page_shows_assortment_vetting_hint(self) -> None:
        # v7: warehouse-approval hint — est. SKU count + deterministic size
        # variance — renders on the review page before publish.
        run = self._generate()
        summary = dict(storage.get_run(run["id"]).summary_json)
        profile = dict(summary["prospect_profile"])
        profile["estimated_sku_count"] = 80
        profile["sku_count_basis"] = "estimated from ~6 product lines"
        storage.update_summary(run["id"], {"prospect_profile": profile})
        review = self.client.get(f"{_BASE}/runs/{run['id']}/review")
        self.assertEqual(review.status_code, 200)
        self.assertIn("Warehouse approval — assortment", review.text)
        self.assertIn("Est. SKU count:", review.text)
        self.assertIn("80", review.text)
        self.assertIn("estimated from ~6 product lines", review.text)
        self.assertIn("Size range:", review.text)
        # Size variance needs >=2 fully-specced products to compare.
        self.client.post(
            f"{_BASE}/runs/{run['id']}/update",
            data={
                "brand": "TabCo", "origin_zip": "84043",
                "product_name": ["Widget", "Crate"],
                "product_length": ["6", "20"],
                "product_width": ["5", "16"],
                "product_height": ["3", "12"],
                "product_weight": ["1.5", "9"],
                "product_units": ["500", "100"],
                "product_estimated": ["0", "0"],
            },
            follow_redirects=False,
        )
        # SKU count survives the product edit (only products were edited).
        profile = dict(storage.get_run(run["id"]).summary_json)["prospect_profile"]
        profile["estimated_sku_count"] = 80
        profile["sku_count_basis"] = "estimated from ~6 product lines"
        storage.update_summary(run["id"], {"prospect_profile": profile})
        review = self.client.get(f"{_BASE}/runs/{run['id']}/review")
        self.assertIn("Size variance:", review.text)

    def test_update_remove_checkbox_drops_product(self) -> None:
        run = self._generate()
        response = self.client.post(
            f"{_BASE}/runs/{run['id']}/update",
            data={
                "brand": "TabCo",
                "origin_zip": "84043",
                "product_name": ["Widget", "Gadget"],
                "product_length": ["6", "8"],
                "product_width": ["5", "6"],
                "product_height": ["3", "4"],
                "product_weight": ["1.5", "2"],
                "product_units": ["500", "100"],
                "product_estimated": ["0", "0"],
                "product_remove": ["1"],
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        profile = dict(storage.get_run(run["id"]).summary_json)["prospect_profile"]
        self.assertEqual([p["name"] for p in profile["products"]], ["Widget"])

    # ------------------------------------------------------------------
    # Live requote (interactive map)
    # ------------------------------------------------------------------

    def test_requote_returns_zone_rates_and_clamps(self) -> None:
        run = self._generate_published()
        public = TestClient(app)
        response = public.post(
            run["view_path"] + "/requote",
            json={
                "origin_zip": "84043",
                "products": [
                    {"name": "Widget", "length_in": 10, "width_in": 8,
                     "height_in": 6, "weight_lb": 4.0},
                    {"name": "Bad dims", "length_in": 9999, "width_in": 8,
                     "height_in": 6, "weight_lb": 4.0},
                ],
            },
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        widget = next(p for p in data["products"] if p["name"] == "Widget")
        self.assertTrue(widget["zoneRates"])
        # v6 shape: each carrier maps to a rate-sorted LIST (its Pareto
        # frontier) so the viewer-side filter + optimizer can re-pick without a
        # round trip.
        first_zone = next(iter(widget["zoneRates"].values()))
        self.assertIn("USPS", first_zone)
        for carrier, frontier in first_zone.items():
            self.assertIsInstance(carrier, str)
            self.assertIsInstance(frontier, list)
            self.assertTrue(frontier)
            rates = [q["rate"] for q in frontier]
            self.assertEqual(rates, sorted(rates))  # rate-sorted
            for quote in frontier:
                self.assertGreater(quote["rate"], 0)
                self.assertIn("service", quote)
                self.assertIn("transit_days", quote)
        # 9999in length is clamped to None by the schema -> product excluded.
        self.assertFalse(any(p["name"] == "Bad dims" and p["zoneRates"] for p in data["products"]))

        # Empty body / missing products -> 400; bad token -> 404.
        self.assertEqual(public.post(run["view_path"] + "/requote", json={}).status_code, 400)
        bad = run["view_path"].rsplit("/", 1)[0] + "/" + "0" * 32 + "/requote"
        self.assertEqual(public.post(bad, json={"products": [{}]}).status_code, 404)

    def test_requote_persists_edits_and_returns_fragments(self) -> None:
        run = self._generate_published()
        before = dict(storage.get_run(run["id"]).summary_json)
        # Mark the stored product estimated so we can verify the requote
        # clears the flag (the viewer confirmed real numbers).
        profile = dict(before["prospect_profile"])
        products = [dict(p) for p in profile["products"]]
        products[0]["dims_estimated"] = True
        profile["products"] = products
        storage.update_summary(run["id"], {"prospect_profile": profile})

        public = TestClient(app)
        response = public.post(
            run["view_path"] + "/requote",
            json={
                "origin_zip": "84043",
                "products": [
                    {"name": "Widget", "length_in": 10, "width_in": 8,
                     "height_in": 6, "weight_lb": 4.0},
                ],
            },
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()

        # v6: the combined rates-explorer section is NEVER swapped (its
        # data-driven table updates client-side from the returned products),
        # so the requote re-ships ONLY the monthly-math + quote fragments.
        self.assertEqual(
            set(data["fragments"]), {"monthly-math", "quote"}
        )
        self.assertNotIn("carrier-rates", data["fragments"])
        self.assertNotIn("rates-explorer", data["fragments"])
        # The returned products carry the new list-shaped zoneRates (frontier).
        widget = next(p for p in data["products"] if p["name"] == "Widget")
        first_zone = next(iter(widget["zoneRates"].values()))
        self.assertIsInstance(next(iter(first_zone.values())), list)
        # The quote fragment re-ships the recomputed invoice.
        quote_frag = data["fragments"]["quote"]
        self.assertIn('data-key="quote"', quote_frag)
        self.assertTrue(quote_frag.startswith("<section"))
        self.assertTrue(quote_frag.endswith("</section>"))
        self.assertIn("Your estimated monthly invoice", quote_frag)
        # TabCo has units -> the monthly-math section re-ships.
        self.assertIn('data-key="monthly-math"', data["fragments"]["monthly-math"])
        self.assertIn("What this means monthly", data["fragments"]["monthly-math"])

        # Persistence: dims landed on the stored profile, estimated cleared,
        # deck re-rendered at the same link.
        stored = dict(storage.get_run(run["id"]).summary_json)
        widget = stored["prospect_profile"]["products"][0]
        self.assertEqual(widget["length_in"], 10.0)
        self.assertEqual(widget["weight_lb"], 4.0)
        self.assertFalse(widget["dims_estimated"])
        self.assertNotEqual(stored["deck_html"], before["deck_html"])
        self.assertIn("10 × 8 × 6 in", stored["deck_html"])
        self.assertEqual(stored["view_path"], before["view_path"])
        # The viewer can leave and come back: the public view serves the edit.
        self.assertIn("10 × 8 × 6 in", public.get(run["view_path"]).text)

    def test_requote_works_for_drafts(self) -> None:
        # The admin review preview embeds the same map, so drafts must be
        # requotable with the token even though the public view 404s.
        run = self._generate()
        public = TestClient(app)
        self.assertEqual(public.get(run["view_path"]).status_code, 404)
        response = public.post(
            run["view_path"] + "/requote",
            json={"products": [{"name": "Widget", "length_in": 6, "width_in": 5,
                                "height_in": 3, "weight_lb": 1.5}]},
        )
        self.assertEqual(response.status_code, 200)

    # ------------------------------------------------------------------
    # Engagement + delete (published sheets)
    # ------------------------------------------------------------------

    def test_heartbeat_creates_visit_session(self) -> None:
        run = self._generate_published()
        public = TestClient(app)
        response = public.post(
            run["view_path"] + "/heartbeat",
            json={
                "visitor_token": "11111111-2222-4333-8444-555555555555",
                "is_internal": False,
                "total_seconds": 42,
                "max_scroll_pct": 60,
                "sections": {"sec-01": 30, "sec-03": 12},
            },
        )
        self.assertEqual(response.status_code, 200)
        with Session(get_engine()) as s:
            row = s.execute(
                select(DeckVisitSession).where(
                    DeckVisitSession.run_id == run["id"],
                    DeckVisitSession.visitor_token == "11111111-2222-4333-8444-555555555555",
                )
            ).scalar_one_or_none()
        self.assertIsNotNone(row)
        self.assertEqual(row.total_seconds, 42)
        self.assertEqual(row.max_scroll_pct, 60)

        engagement = storage.engagement_for([run["id"]])
        self.assertEqual(engagement[run["id"]]["external_sessions"], 1)

        response = public.post(run["view_path"] + "/heartbeat", json={})
        self.assertEqual(response.status_code, 400)

    def test_delete_removes_run_and_engagement(self) -> None:
        run = self._generate_published()
        public = TestClient(app)
        public.post(
            run["view_path"] + "/heartbeat",
            json={"visitor_token": "delete-test-token", "total_seconds": 5},
        )
        response = self.client.post(
            f"{_BASE}/runs/{run['id']}/delete", follow_redirects=False
        )
        self.assertEqual(response.status_code, 303)
        self.assertIsNone(storage.get_run(run["id"]))
        self.assertEqual(TestClient(app).get(run["view_path"]).status_code, 404)
        with Session(get_engine()) as s:
            leftover = s.execute(
                select(DeckVisitSession).where(DeckVisitSession.run_id == run["id"])
            ).scalars().all()
        self.assertEqual(leftover, [])


if __name__ == "__main__":
    unittest.main()
