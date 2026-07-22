"""Customer-safe mapped Rate Sheet payload and public result routes."""

from __future__ import annotations

import dataclasses
import os
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault("SALES_AGENT_DB_URL", "sqlite:///" + tempfile.gettempdir() + "/fulfillment_public_payload_test.db")
os.environ.setdefault("MARKETING_SITE_INTAKE_KEY", "test-intake-key")

from fastapi.testclient import TestClient

from sales_support_agent.main import app
from sales_support_agent.models.database import create_session_factory, init_database
from sales_support_agent.services.fulfillment_deck import storage
from sales_support_agent.services.fulfillment_deck.public_payload import PUBLIC_MATRIX_KEYS, serialize_public_matrix

HEADERS = {"X-Internal-Api-Key": "test-intake-key"}


def live_summary(correlation: str = "public-correlation-abcdefghijkl") -> dict:
    return {
        "prospect": "TabCo",
        "rates_source": "wms",
        "origin_zip": "84043",
        "published_at": "2026-07-22T20:00:00+00:00",
        "public_correlation_id": correlation,
        "public_rate_sheet_status": "ready",
        "public_email_status": "sent",
        "public_sales_handoff_status": "complete",
        "public_shared_url": "https://agent.anatainc.com/x/sheet/token",
        "export_token": "export-token-abcdefghijklmnop",
        "internal_margin": 0.42,
        "fulfillment_quote": {"monthly_total": 9999},
        "prospect_profile": {
            "brand": "TabCo",
            "products": [{"name": "Widget", "length_in": 6, "width_in": 5, "height_in": 3, "weight_lb": 1.5, "dims_estimated": True}],
        },
        "rate_matrix": {
            "origin_zip": "84043",
            "products": [{
                "product": {"name": "Widget", "length_in": 6, "width_in": 5, "height_in": 3, "weight_lb": 1.5, "dims_estimated": True},
                "zones": [{
                    "zone": 5, "dest_zip": "75201", "dest_label": "Dallas, TX",
                    "quotes": [
                        {"carrier": "UPS", "service": "Ground", "rate_usd": 8.25, "transit_days": 3, "zone": 5, "source": "wms"},
                        {"carrier": "USPS", "service": "Priority Mail", "rate_usd": 9.10, "transit_days": 2, "zone": 5, "source": "wms"},
                    ],
                }],
            }],
        },
    }


class PublicPayloadTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        factory = create_session_factory(os.environ["SALES_AGENT_DB_URL"])
        init_database(factory)
        app.state.session_factory = factory
        app.state.settings = dataclasses.replace(
            app.state.settings,
            marketing_site_intake_key="test-intake-key",
            deck_public_base_url="https://agent.anatainc.com",
        )
        cls.client = TestClient(app)

    def test_serializer_is_allowlisted_and_marks_best_quotes(self) -> None:
        payload = serialize_public_matrix(live_summary(), preview=False)
        self.assertIsNotNone(payload)
        self.assertLessEqual(set(payload or {}), PUBLIC_MATRIX_KEYS)
        self.assertNotIn("internal_margin", payload or {})
        self.assertNotIn("fulfillment_quote", payload or {})
        quotes = (payload or {})["quotes"]
        self.assertTrue(quotes[0]["is_lowest_cost"])
        self.assertTrue(quotes[1]["is_fastest"])
        self.assertEqual((payload or {})["destinations"][0]["label"], "Dallas, TX")
        self.assertEqual((payload or {})["destinations"][0]["zip"], "75201")

    def test_serializer_rejects_mock_or_mixed_matrix(self) -> None:
        summary = live_summary()
        summary["rate_matrix"]["products"][0]["zones"][0]["quotes"][0]["source"] = "mock"
        self.assertIsNone(serialize_public_matrix(summary, preview=True))

    def test_status_and_result_routes_return_only_safe_contract(self) -> None:
        summary = live_summary()
        run_id = storage.create_run(trigger="public_funnel")
        storage.save_draft(run_id, summary)
        status = self.client.get(
            f"/api/public/fulfillment/rate-sheet/status/{summary['public_correlation_id']}", headers=HEADERS,
        )
        self.assertEqual(status.status_code, 200, status.text)
        self.assertEqual(status.json()["rate_sheet"]["status"], "ready")
        result = self.client.get(
            f"/api/public/fulfillment/rate-sheet/result/{summary['public_correlation_id']}", headers=HEADERS,
        )
        self.assertEqual(result.status_code, 200, result.text)
        self.assertNotIn("internal_margin", result.json())
        self.assertNotIn("fulfillment_quote", result.json())
        self.assertEqual(result.json()["quotes"][0]["rate_usd"], 8.25)

    def test_result_requires_key_and_ready_state(self) -> None:
        self.assertEqual(
            self.client.get("/api/public/fulfillment/rate-sheet/result/does-not-exist").status_code,
            401,
        )

    def test_repeated_unlock_reuses_ready_correlation(self) -> None:
        summary = live_summary("repeat-correlation-abcdefghijk")
        run_id = storage.create_run(trigger="public_funnel")
        storage.save_draft(run_id, summary)
        response = self.client.post(
            "/api/public/fulfillment/rate-sheet/unlock",
            headers=HEADERS,
            json={
                "run_id": run_id,
                "token": summary["export_token"],
                "email": "buyer@tabco.com",
            },
        )
        self.assertEqual(response.status_code, 202, response.text)
        self.assertEqual(response.json()["correlation_id"], summary["public_correlation_id"])
        self.assertEqual(response.json()["status"], "ready")


if __name__ == "__main__":
    unittest.main()
