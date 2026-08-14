from __future__ import annotations

import dataclasses
import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

os.environ.setdefault("SALES_AGENT_DB_URL", "sqlite:///" + tempfile.gettempdir() + "/shipping_label_address_test.db")
os.environ.setdefault("MARKETING_SITE_INTAKE_KEY", "test-intake-key")

from fastapi.testclient import TestClient

from sales_support_agent.api import shipping_label_router as router
from sales_support_agent.main import app


class ShippingLabelAddressTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        app.state.settings = dataclasses.replace(app.state.settings, marketing_site_intake_key="test-intake-key")
        cls.client = TestClient(app)

    def setUp(self) -> None:
        limiter = mock.patch.object(router, "durable_rate_limit_response", return_value=None)
        limiter.start()
        self.addCleanup(limiter.stop)

    def post(self, address: dict):
        return self.client.post(
            "/api/public/fulfillment/labels/address/verify",
            json={"address": address},
            headers={"X-Internal-Api-Key": "test-intake-key"},
        )

    def test_returns_sanitized_delivery_suggestions(self) -> None:
        wms = SimpleNamespace(verify_address=lambda _: {
            "verified": False,
            "suggestions": [{"street_1": "221 S 1050 W", "city": "Provo", "state": "utah", "postal": "84601", "postal_sub": "7080"}],
        })
        with mock.patch.object(router, "live_wms", return_value=wms):
            response = self.post({"name": "Test", "street_1": "221 S 1050 W", "city": "Provo", "state": "UT", "postal": "84602"})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["suggestions"][0], {
            "street_1": "221 S 1050 W", "street_2": "", "city": "Provo",
            "state": "UT", "postal": "84601", "postal_sub": "7080", "country": "US",
        })

    def test_verified_address_falls_back_to_submitted_value(self) -> None:
        with mock.patch.object(router, "live_wms", return_value=SimpleNamespace(verify_address=lambda _: {"verified": True})):
            response = self.post({"name": "Test", "street_1": "1 Main St", "city": "Denver", "state": "CO", "postal": "80202"})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertTrue(response.json()["verified"])
        self.assertEqual(response.json()["suggestions"][0]["postal"], "80202")

    def test_provider_failure_is_not_treated_as_verified(self) -> None:
        wms = SimpleNamespace(verify_address=mock.Mock(side_effect=RuntimeError("offline")))
        with mock.patch.object(router, "live_wms", return_value=wms):
            response = self.post({"name": "Test", "street_1": "1 Main St", "city": "Denver", "state": "CO", "postal": "80202"})
        self.assertEqual(response.status_code, 503)
        self.assertNotIn("verified", response.json())


if __name__ == "__main__":
    unittest.main()
