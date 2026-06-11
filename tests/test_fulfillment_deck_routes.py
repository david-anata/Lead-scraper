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
from sales_support_agent.models.database import get_engine
from sales_support_agent.models.entities import DeckVisitSession
from sales_support_agent.services.access import store
from sales_support_agent.services.admin_auth import create_user_session_token
from sales_support_agent.services.fulfillment_deck import storage
from sales_support_agent.services.fulfillment_deck.rates import build_rate_matrix
from sales_support_agent.services.fulfillment_deck.schema import ProductSpec
from sales_support_agent.services.fulfillment_deck.wms_client import MockWMSClient

_NOTES = "Brand: TabCo\nWidget — 6 x 5 x 3 in, 1.5 lb, ~500 units/mo"
_BASE = "/admin/fulfillment/sales"


def _cookie_for(email: str, name: str = "User", role: str = "member"):
    s = app.state.agent_settings
    return s.admin_cookie_name, create_user_session_token(s, email=email, name=name, role=role)


class FulfillmentDeckRouteTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
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
        self.assertIn("rate-sheets", response.headers["location"])

    def test_history_shows_draft_pill_then_open_after_publish(self) -> None:
        run = self._generate()
        page = self.client.get(_BASE).text
        self.assertIn("Draft", page)
        self.assertIn(f"{_BASE}/runs/{run['id']}/review", page)
        self.assertNotIn(f'href="{run["view_path"]}?viewer=internal"', page)

        self.client.post(f"{_BASE}/runs/{run['id']}/publish", follow_redirects=False)
        page = self.client.get(_BASE).text
        self.assertIn(f'href="{run["view_path"]}?viewer=internal"', page)
        self.assertIn("Review / edit", page)

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
        first_zone = next(iter(widget["zoneRates"].values()))
        self.assertGreater(first_zone["rate"], 0)
        self.assertIn("carrier", first_zone)
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

        # Fragments: re-rendered swappable sections keyed by data-key.
        self.assertEqual(
            set(data["fragments"]), {"carrier-rates", "volume-economics", "savings"}
        )
        frag = data["fragments"]["carrier-rates"]
        self.assertIn('data-key="carrier-rates"', frag)
        self.assertTrue(frag.startswith("<section"))
        self.assertTrue(frag.endswith("</section>"))
        self.assertIn("10 × 8 × 6 in", frag)
        # The new dims' rates show up in the fragment (deterministic mock math).
        expected, _ = build_rate_matrix(
            [ProductSpec(name="Widget", length_in=10.0, width_in=8.0,
                         height_in=6.0, weight_lb=4.0)],
            "84043",
            MockWMSClient(),
        )
        cheapest = expected.products[0].zones[0].quotes[0].rate_usd
        self.assertIn(f"${cheapest:,.2f}", frag)
        # TabCo has units -> volume section re-ships; no current cost -> no savings.
        self.assertIn('data-key="volume-economics"', data["fragments"]["volume-economics"])
        self.assertEqual(data["fragments"]["savings"], "")

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
