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

    def test_unlock_stores_sanitized_diagnostic_qualification(self) -> None:
        from sales_support_agent.models.entities import AutomationRun

        data = self._create()
        with mock.patch.object(M, "_run_analysis_and_deliver"):
            resp = self.client.post(
                f"/api/public/marketing/intake/{data['intake_id']}/unlock",
                json={
                    "token": data["token"],
                    "email": "qualified@example.com",
                    "qualification": {
                        "name": "  David  ",
                        "company": "Anata",
                        "storefront": "https://www.amazon.com/dp/B0TESTASIN",
                        "revenue_range": "$100k-$500k",
                        "challenge": "Unclear advertising efficiency",
                        "next_step": "Book an audit review",
                        "completed_engines": [
                            "advertising",
                            "market_shelf",
                            "content_alignment",
                            "profit",
                            "ignored",
                        ],
                        "untrusted": "must not persist",
                    },
                },
                headers=HEADERS,
            )
        self.assertEqual(resp.status_code, 202, resp.text)

        session = app.state.session_factory()
        try:
            run = session.get(AutomationRun, data["intake_id"])
            qualification = run.metadata_json["diagnostic_qualification"]
            self.assertEqual(qualification["name"], "David")
            self.assertEqual(qualification["company"], "Anata")
            self.assertEqual(
                qualification["completed_engines"],
                ["advertising", "market_shelf", "content_alignment", "profit"],
            )
            self.assertNotIn("untrusted", qualification)
        finally:
            session.close()


if __name__ == "__main__":
    unittest.main()
