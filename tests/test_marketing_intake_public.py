"""Two-step marketing-site intake: create → needs → email unlock.

Proves: the shared-secret header gates every route, the identity lookup is
patched out (no network), needs are stored and filtered to known chips, and
the unlock enforces the shared one-per-email-per-UTC-day gate (second unlock
with the same email 429s), with the background delivery patched out.
"""

from __future__ import annotations

import os
import tempfile
import unittest
from unittest import mock

os.environ.setdefault("SALES_AGENT_DB_URL", "sqlite:///" + tempfile.gettempdir() + "/mkt_intake_test.db")
os.environ.setdefault("MARKETING_SITE_INTAKE_KEY", "test-intake-key")

try:
    from fastapi.testclient import TestClient
    from sales_support_agent.api import marketing_router as M
    from sales_support_agent.main import app
    DEPS = True
except ModuleNotFoundError as exc:
    if exc.name not in {"sqlalchemy", "fastapi"}:
        raise
    DEPS = False

HEADERS = {"X-Internal-Api-Key": "test-intake-key"}


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class MarketingShelfPayloadTests(unittest.TestCase):
    @staticmethod
    def _product(
        asin: str,
        *,
        revenue: float | None,
        units: float | None,
        floor: bool = False,
        price: float = 20.0,
    ):
        from sales_support_agent.services.helium10 import XrayProduct

        suffix = "+" if floor else ""
        return XrayProduct(
            display_order=1,
            title=f"Product {asin}",
            asin=asin,
            url=f"https://www.amazon.com/dp/{asin}",
            image_url=f"https://images.example/{asin}.jpg",
            brand="TestBrand",
            price=price,
            price_label=f"${price:.2f}",
            revenue=revenue,
            revenue_label=f"${revenue:,.0f}{suffix}" if revenue is not None else "N/A",
            units_sold=units,
            units_label=f"{int(units):,}{suffix}" if units is not None else "N/A",
            bsr=5000.0,
            bsr_label="5,000",
            rating=4.5,
            rating_label="4.5",
            review_count=120,
            category="Health",
            seller_country="",
            size_tier="",
            fulfillment="FBA",
            dimensions="10 x 6 x 3 in",
            weight="1.2 lb",
        )

    def test_product_payload_distinguishes_recent_sales_floor(self) -> None:
        payload = M._shelf_product_payload(
            self._product("B09ABCDEF1", revenue=10_000, units=500, floor=True)
        )
        self.assertEqual(payload["units_source"], "recent_sales")
        self.assertEqual(payload["revenue_source"], "recent_sales")
        self.assertEqual(payload["recent_sales"], 500)
        self.assertEqual(payload["estimated_revenue"], 10_000.0)

    def test_assembled_payload_uses_visible_five_product_set(self) -> None:
        products = [
            self._product(
                f"B09ABCDEF{i}",
                revenue=float(value),
                units=float(value / 20),
            )
            for i, value in enumerate((1000, 2000, 3000, 4000, 5000, 9000))
        ]
        payload = M._assemble_shelf_payload(
            self._product("B09TARGET01", revenue=2500, units=125, floor=True),
            products,
            ["Mixed evidence."],
        )
        self.assertEqual(payload["comparison_count"], 5)
        self.assertEqual(payload["revenue_product_count"], 5)
        self.assertEqual(payload["visible_revenue"], 15_000.0)
        self.assertEqual(payload["median_revenue"], 3_000.0)
        self.assertEqual(payload["target"]["units_source"], "recent_sales")
        self.assertEqual(payload["revenue_warning"], "Mixed evidence.")
        self.assertTrue(payload["captured_at"])


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class MarketingIntakeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from sales_support_agent.models.database import create_session_factory, init_database
        import dataclasses

        db = os.path.join(tempfile.gettempdir(), "mkt_intake_isolated.db")
        if os.path.exists(db):
            os.remove(db)
        factory = create_session_factory("sqlite:///" + db)
        init_database(factory)
        app.state.session_factory = factory
        app.state.settings = dataclasses.replace(
            app.state.settings, marketing_site_intake_key="test-intake-key"
        )
        cls.client = TestClient(app)

    def _create(self, kind: str = "asin", identifier: str = "B0TESTASIN1") -> dict:
        with mock.patch.object(
            M,
            "_asin_identity",
            return_value={
                "asin": "B0TESTASIN",
                "brand_name": "TestBrand",
                "product_title": "Test Product",
                "product_image": "https://img.example/x.jpg",
            },
        ), mock.patch.object(
            M,
            "_store_identity",
            return_value={
                "domain": "testbrand.com",
                "brand_name": "TestBrand",
                "product_title": "TestBrand Store",
                "product_image": "",
            },
        ):
            resp = self.client.post(
                "/api/public/marketing/intake",
                json={"identifier": identifier, "kind": kind, "source": "hero"},
                headers=HEADERS,
            )
        self.assertEqual(resp.status_code, 201, resp.text)
        return resp.json()

    def test_auth_required(self) -> None:
        resp = self.client.post(
            "/api/public/marketing/intake",
            json={"identifier": "B0TESTASIN1", "kind": "asin"},
        )
        self.assertEqual(resp.status_code, 401)

    def test_create_returns_identity_and_token(self) -> None:
        data = self._create()
        self.assertIn("intake_id", data)
        self.assertTrue(data["token"])
        self.assertEqual(data["brand_name"], "TestBrand")
        self.assertEqual(data["product_title"], "Test Product")
        self.assertNotIn("dtc_domain", data)

    def test_store_create_returns_dtc_domain(self) -> None:
        data = self._create(kind="store", identifier="testbrand.com")
        self.assertEqual(data["dtc_domain"], "testbrand.com")

    def test_needs_stored_and_filtered(self) -> None:
        data = self._create()
        with mock.patch.object(M, "_build_shelf"):
            resp = self.client.post(
            f"/api/public/marketing/intake/{data['intake_id']}/needs",
                json={"token": data["token"], "needs": ["analytics", "advertising", "bogus"]},
                headers=HEADERS,
            )
        self.assertEqual(resp.status_code, 200, resp.text)
        status = self.client.get(
            f"/api/public/marketing/intake/{data['intake_id']}",
            params={"token": data["token"]},
            headers=HEADERS,
        ).json()
        self.assertEqual(status["needs"], ["analytics", "advertising"])

    def test_needs_on_asin_intake_sets_shelf_pending(self) -> None:
        data = self._create()
        with mock.patch.object(M, "_build_shelf") as build:
            resp = self.client.post(
                f"/api/public/marketing/intake/{data['intake_id']}/needs",
                json={"token": data["token"], "needs": ["analytics"]},
                headers=HEADERS,
            )
        self.assertEqual(resp.status_code, 200, resp.text)
        build.assert_called_once()
        status = self.client.get(
            f"/api/public/marketing/intake/{data['intake_id']}",
            params={"token": data["token"]},
            headers=HEADERS,
        ).json()
        self.assertEqual(status["shelf"], {"status": "pending"})

    def test_needs_on_store_intake_has_no_shelf(self) -> None:
        data = self._create(kind="store", identifier="testbrand.com")
        with mock.patch.object(M, "_build_shelf") as build:
            resp = self.client.post(
                f"/api/public/marketing/intake/{data['intake_id']}/needs",
                json={"token": data["token"], "needs": ["analytics"]},
                headers=HEADERS,
            )
        self.assertEqual(resp.status_code, 200, resp.text)
        build.assert_not_called()
        status = self.client.get(
            f"/api/public/marketing/intake/{data['intake_id']}",
            params={"token": data["token"]},
            headers=HEADERS,
        ).json()
        self.assertIsNone(status["shelf"])

    def test_needs_wrong_token_403(self) -> None:
        data = self._create()
        resp = self.client.post(
            f"/api/public/marketing/intake/{data['intake_id']}/needs",
            json={"token": "wrong", "needs": ["analytics"]},
            headers=HEADERS,
        )
        self.assertEqual(resp.status_code, 403)

    def test_unlock_daily_gate_and_closers(self) -> None:
        import os as _os

        _os.environ["MARKETING_DAILY_GATE"] = "1"
        self.addCleanup(lambda: _os.environ.pop("MARKETING_DAILY_GATE", None))
        data = self._create()
        self.client.post(
            f"/api/public/marketing/intake/{data['intake_id']}/needs",
            json={"token": data["token"], "needs": ["analytics", "advertising"]},
            headers=HEADERS,
        )
        with mock.patch.object(M, "_run_analysis_and_deliver") as deliver:
            resp = self.client.post(
                f"/api/public/marketing/intake/{data['intake_id']}/unlock",
                json={"token": data["token"], "email": "gate@example.com"},
                headers=HEADERS,
            )
        self.assertEqual(resp.status_code, 202, resp.text)
        body = resp.json()
        self.assertEqual(body["status"], "building")
        self.assertTrue(body["closers"]["software"])
        self.assertTrue(body["closers"]["services"])
        deliver.assert_called_once()

        # Same email, same UTC day, a fresh intake → 429 daily_limit.
        second = self._create()
        with mock.patch.object(M, "_run_analysis_and_deliver"):
            resp2 = self.client.post(
                f"/api/public/marketing/intake/{second['intake_id']}/unlock",
                json={"token": second["token"], "email": "gate@example.com"},
                headers=HEADERS,
            )
        self.assertEqual(resp2.status_code, 429)
        self.assertEqual(resp2.json()["reason"], "daily_limit")

    def test_store_unlock_uses_store_delivery(self) -> None:
        data = self._create(kind="store", identifier="testbrand.com")
        with mock.patch.object(M, "_deliver_store_unlock") as deliver:
            resp = self.client.post(
                f"/api/public/marketing/intake/{data['intake_id']}/unlock",
                json={"token": data["token"], "email": "store@example.com"},
                headers=HEADERS,
            )
        self.assertEqual(resp.status_code, 202, resp.text)
        body = resp.json()
        self.assertFalse(body["closers"]["software"])
        self.assertFalse(body["closers"]["services"])
        deliver.assert_called_once()

    def test_unlock_sanitizes_and_forwards_qualification(self) -> None:
        data = self._create()
        self.client.post(
            f"/api/public/marketing/intake/{data['intake_id']}/needs",
            json={"token": data["token"], "needs": ["advertising"]},
            headers=HEADERS,
        )
        qualification = {
            "name": "  David Narayan  ",
            "company": " Anata ",
            "phone": " 385-204-4649 ",
            "storefront": " https://example.com ",
            "revenue_range": "$250K–$1M",
            "challenge": " Improve advertising efficiency ",
            "next_step": "Book my review",
            "ignored": "must not persist",
        }
        with mock.patch.object(M, "_run_analysis_and_deliver") as deliver:
            response = self.client.post(
                f"/api/public/marketing/intake/{data['intake_id']}/unlock",
                json={"token": data["token"], "email": "qualified@example.com", "qualification": qualification},
                headers=HEADERS,
            )
        self.assertEqual(response.status_code, 202, response.text)
        forwarded = deliver.call_args.kwargs["qualification"]
        self.assertEqual(forwarded["name"], "David Narayan")
        self.assertEqual(forwarded["phone"], "385-204-4649")
        self.assertNotIn("ignored", forwarded)
        with app.state.session_factory() as session:
            run = session.get(M.AutomationRun, int(data["intake_id"]))
            self.assertEqual(run.metadata_json["qualification"]["company"], "Anata")

    def test_qualified_contact_fields_sync_to_hubspot(self) -> None:
        client = mock.Mock()
        client.is_configured = True
        client.create_contact.return_value = {"id": "123"}
        with mock.patch.object(M, "HubSpotClient", return_value=client):
            M._record_hubspot_lead(
                app.state.settings,
                email="qualified@example.com",
                asin="B0TESTASIN",
                view_url="https://agent.example/deck",
                source="strategy-audit",
                needs=["advertising"],
                qualification={
                    "name": "David Narayan",
                    "company": "Anata",
                    "phone": "385-204-4649",
                    "storefront": "https://example.com",
                },
            )
        client.create_contact.assert_called_once_with({
            "email": "qualified@example.com",
            "firstname": "David Narayan",
            "company": "Anata",
            "phone": "385-204-4649",
            "website": "https://example.com",
        })
        client.update_contact.assert_called_once_with("123", {
            "firstname": "David Narayan",
            "company": "Anata",
            "phone": "385-204-4649",
            "website": "https://example.com",
        })

    def test_advertising_audit_rejects_store_url(self) -> None:
        response = self.client.post(
            "/api/public/marketing/advertising-audit",
            json={
                "product": "https://oceanrx.us",
                "email": "ads@example.com",
                "company": "Ocean Rx",
            },
            headers=HEADERS,
        )
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["reason"], "invalid_product")

    def test_advertising_audit_accepts_amazon_url_and_returns_status_handle(self) -> None:
        with mock.patch.object(M, "_run_analysis_and_deliver") as deliver:
            response = self.client.post(
                "/api/public/marketing/advertising-audit",
                json={
                    "product": "https://www.amazon.com/example/dp/B09239YTZQ",
                    "email": "ads-accepted@example.com",
                    "company": "Ocean Rx",
                    "source": "anatainc.com/tools/advertising-audit",
                },
                headers=HEADERS,
            )
        self.assertEqual(response.status_code, 202, response.text)
        body = response.json()
        self.assertEqual(body["asin"], "B09239YTZQ")
        self.assertEqual(body["status"], "accepted")
        self.assertTrue(body["token"])
        deliver.assert_called_once()
        forwarded = deliver.call_args.kwargs
        self.assertEqual(forwarded["needs"], ["advertising"])
        self.assertEqual(forwarded["qualification"]["company"], "Ocean Rx")

        status = self.client.get(
            f"/api/public/marketing/advertising-audit/{body['run_id']}",
            params={"token": body["token"]},
            headers=HEADERS,
        )
        self.assertEqual(status.status_code, 200, status.text)
        self.assertEqual(status.json(), {
            "status": "building",
            "strategy_audit": "building",
            "advertising_audit": "reports_required",
            "email_delivery": "pending",
        })

    def test_advertising_audit_status_requires_run_token(self) -> None:
        with mock.patch.object(M, "_run_analysis_and_deliver"):
            body = self.client.post(
                "/api/public/marketing/advertising-audit",
                json={
                    "product": "B09239YTZQ",
                    "email": "ads-token@example.com",
                    "company": "Ocean Rx",
                },
                headers=HEADERS,
            ).json()
        response = self.client.get(
            f"/api/public/marketing/advertising-audit/{body['run_id']}",
            params={"token": "wrong"},
            headers=HEADERS,
        )
        self.assertEqual(response.status_code, 403)

    def test_advertising_hubspot_note_names_tool_and_next_step(self) -> None:
        client = mock.Mock()
        client.is_configured = True
        client.create_contact.return_value = {"id": "321"}
        with mock.patch.object(M, "HubSpotClient", return_value=client):
            recorded = M._record_hubspot_lead(
                app.state.settings,
                email="ads-note@example.com",
                asin="B09239YTZQ",
                view_url="https://agent.example/deck",
                source="anatainc.com/tools/advertising-audit",
                needs=["advertising"],
                qualification={
                    "company": "Ocean Rx",
                    "next_step": "Call prospect and confirm the four-report handoff.",
                },
            )
        self.assertTrue(recorded)
        note = client.create_contact_note.call_args.kwargs["body"]
        self.assertIn("Advertising Audit requested", note)
        self.assertIn("Ocean Rx", note)
        self.assertIn("Call prospect and confirm the four-report handoff.", note)


if __name__ == "__main__":
    unittest.main()
