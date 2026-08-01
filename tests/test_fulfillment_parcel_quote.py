"""Public parcel quote (anatainc.com shipping calculator) + prospect-site redirects.

Proves: the shared-secret header gates the route; bad dimensions, weights, and
ZIPs are refused rather than rated; the true zone is computed from the visitor's
own destination ZIP and rated against a metro in that same zone; mock rates are
NEVER returned to a visitor as real prices; and the prospect-site fetch follows
redirects (the reason every real store extracted zero products) without ever
following one to a private or metadata address.
"""

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

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/fulfillment_parcel_quote_test.db",
)
os.environ.setdefault("MARKETING_SITE_INTAKE_KEY", "test-intake-key")

from fastapi.testclient import TestClient

from sales_support_agent.api import fulfillment_public_router as P
from sales_support_agent.main import app
from sales_support_agent.models.database import create_session_factory, init_database
from sales_support_agent.services.fulfillment_deck.schema import RATE_SOURCE_WMS, RateQuote
from sales_support_agent.services.fulfillment_deck import intake

_HEADERS = {"X-Internal-Api-Key": "test-intake-key"}
_BOX = {"length_in": 10, "width_in": 8, "height_in": 6, "weight_lb": 3, "dest_zip": "10001"}


class ParcelQuoteTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        factory = create_session_factory(os.environ["SALES_AGENT_DB_URL"])
        init_database(factory)
        cls.client = TestClient(app)

    def _post(self, body, headers=_HEADERS):
        return self.client.post("/api/public/fulfillment/parcel-quote", json=body, headers=headers)

    def test_requires_the_shared_secret(self) -> None:
        self.assertEqual(self._post(_BOX, headers={}).status_code, 401)
        self.assertEqual(self._post(_BOX, headers={"X-Internal-Api-Key": "wrong"}).status_code, 401)

    def test_rejects_missing_or_impossible_parcels(self) -> None:
        for bad in (
            {**_BOX, "weight_lb": 0},
            {**_BOX, "weight_lb": 500},        # freight, not parcel
            {**_BOX, "length_in": 200},        # longer than any ground service takes
            {**_BOX, "height_in": "wide"},
            {k: v for k, v in _BOX.items() if k != "width_in"},
        ):
            self.assertEqual(self._post(bad).status_code, 400, bad)

    def test_rejects_an_unusable_destination_zip(self) -> None:
        self.assertEqual(self._post({**_BOX, "dest_zip": "abcde"}).status_code, 400)
        self.assertEqual(self._post({**_BOX, "dest_zip": "00000"}).status_code, 400)

    def test_mock_rates_are_never_shown_as_real_prices(self) -> None:
        """The default test env has no WMS, so the mock client answers. A visitor
        must see 'unavailable', never a fabricated price presented as a quote."""
        response = self._post(_BOX)
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["rates_source"], "unavailable")
        self.assertEqual(payload["rates"], [])

    def test_live_quotes_are_returned_cheapest_first_for_the_real_zone(self) -> None:
        captured = {}

        class FakeClient:
            def quote_rates(self, package, origin_zip, dest_zip):
                captured["origin"] = origin_zip
                captured["rating_zip"] = dest_zip
                captured["weight_lb"] = package.weight_lb
                return [
                    RateQuote(carrier="UPS", service="Ground", rate_usd=14.20,
                              transit_days=3, zone=5, source=RATE_SOURCE_WMS),
                    RateQuote(carrier="USPS", service="Ground Advantage", rate_usd=9.85,
                              transit_days=4, zone=5, source=RATE_SOURCE_WMS),
                ]

        with mock.patch.object(P, "get_wms_client", return_value=FakeClient()):
            payload = self._post(_BOX).json()

        self.assertEqual(payload["rates_source"], "live")
        self.assertEqual([r["rate_usd"] for r in payload["rates"]], [9.85, 14.20])
        self.assertEqual(payload["dest_zip"], "10001")
        self.assertEqual(captured["weight_lb"], 3)
        # Rated against a metro in the SAME zone as the visitor's real destination,
        # because carrier ground pricing is a function of zone.
        self.assertEqual(
            P.zone_for(captured["origin"], captured["rating_zip"]),
            P.zone_for(captured["origin"], "10001"),
        )

    def test_carrier_failure_is_reported_as_unavailable(self) -> None:
        class Boom:
            def quote_rates(self, *a, **k):
                raise RuntimeError("carrier down")

        with mock.patch.object(P, "get_wms_client", return_value=Boom()):
            self.assertEqual(self._post(_BOX).status_code, 503)


class ProspectSiteRedirectTests(unittest.TestCase):
    """A 301 carries an empty body, so refusing redirects meant every real store
    extracted nothing, rated nothing, and reported rates as unavailable."""

    def _fetch(self, responses):
        seq = iter(responses)

        class Resp:
            def __init__(self, code, loc=None, text=""):
                self.status_code = code
                self.headers = {"location": loc} if loc else {}
                self.text = text

        made = [Resp(*r) for r in responses]
        it = iter(made)
        warnings: list = []
        with mock.patch("requests.get", side_effect=lambda *a, **k: next(it)):
            return intake._fetch_website("https://example.com", warnings), warnings

    def test_follows_a_redirect_to_the_real_page(self) -> None:
        text, warnings = self._fetch([
            (301, "https://www.example.com/", ""),
            (200, None, "<html><body>Widget A 6x5x3 in 1.5 lb</body></html>"),
        ])
        self.assertIn("Widget A", text)
        self.assertEqual(warnings, [])

    def test_refuses_a_redirect_to_a_private_or_metadata_address(self) -> None:
        for target in ("http://127.0.0.1/admin", "http://169.254.169.254/latest/meta-data/", "http://10.0.0.5/"):
            text, warnings = self._fetch([(302, target, "")])
            self.assertEqual(text, "")
            self.assertTrue(warnings and "not a public address" in warnings[0])

    def test_stops_on_a_redirect_loop(self) -> None:
        text, warnings = self._fetch([(301, "https://example.com/next", "")] * 8)
        self.assertEqual(text, "")
        self.assertTrue(warnings and "too many redirects" in warnings[0])


if __name__ == "__main__":
    unittest.main()
