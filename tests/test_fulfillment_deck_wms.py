from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from sales_support_agent.services.fulfillment_deck.rates import build_rate_matrix
from sales_support_agent.services.fulfillment_deck.schema import (
    ANATA_HQ_ZIP,
    RATE_SOURCE_MOCK,
    ProductSpec,
    RateMatrix,
)
from sales_support_agent.services.fulfillment_deck.wms_client import (
    AnataWMSClient,
    MockWMSClient,
    get_wms_client,
)


def _small_product(name: str = "Widget") -> ProductSpec:
    return ProductSpec(name=name, length_in=8.0, width_in=6.0, height_in=4.0, weight_lb=1.5)


def _big_product(name: str = "Crate") -> ProductSpec:
    return ProductSpec(name=name, length_in=20.0, width_in=16.0, height_in=12.0, weight_lb=9.0)


class MockWMSClientTests(unittest.TestCase):
    def test_deterministic(self):
        client = MockWMSClient()
        first = client.quote_rates(_small_product(), "84043", "10001")
        second = client.quote_rates(_small_product(), "84043", "10001")
        self.assertEqual(first, second)

    def test_rates_plausible(self):
        client = MockWMSClient()
        for dest in ("84101", "80202", "60601", "10001"):
            for quote in client.quote_rates(_big_product(), "84043", dest):
                self.assertGreater(quote.rate_usd, 0)
                self.assertLess(quote.rate_usd, 200)
                self.assertEqual(quote.source, RATE_SOURCE_MOCK)
                self.assertIsNotNone(quote.transit_days)
                self.assertIsNotNone(quote.zone)

    def test_heavier_costs_more(self):
        client = MockWMSClient()
        small = client.quote_rates(_small_product(), "84043", "30303")
        big = client.quote_rates(_big_product(), "84043", "30303")
        self.assertGreater(
            sum(q.rate_usd for q in big),
            sum(q.rate_usd for q in small),
        )
        # And per matching carrier/service too.
        small_by_key = {(q.carrier, q.service): q.rate_usd for q in small}
        for quote in big:
            self.assertGreater(quote.rate_usd, small_by_key[(quote.carrier, quote.service)])

    def test_expected_carriers_and_services(self):
        quotes = MockWMSClient().quote_rates(_small_product(), "84043", "10001")
        self.assertEqual(
            {(q.carrier, q.service) for q in quotes},
            {
                ("USPS", "Ground Advantage"),
                ("UPS", "Ground"),
                ("FedEx", "Home Delivery"),
                ("USPS", "Priority Mail"),
            },
        )

    def test_farther_zone_costs_more(self):
        client = MockWMSClient()
        near = sum(q.rate_usd for q in client.quote_rates(_small_product(), "84043", "84101"))
        far = sum(q.rate_usd for q in client.quote_rates(_small_product(), "84043", "10001"))
        self.assertGreater(far, near)


class GetWMSClientTests(unittest.TestCase):
    def test_mock_without_env(self):
        env = {k: v for k, v in os.environ.items() if not k.startswith("ANATA_WMS_")}
        with mock.patch.dict(os.environ, env, clear=True):
            self.assertIsInstance(get_wms_client(), MockWMSClient)

    def test_real_client_with_env(self):
        with mock.patch.dict(
            os.environ,
            {
                "ANATA_WMS_BASE_URL": "https://wms.example.com",
                "ANATA_WMS_ACCOUNT_NUMBER": "ACCT-1",
                "ANATA_WMS_API_KEY": "key",
                "ANATA_WMS_API_PASSWORD": "secret",
            },
        ):
            client = get_wms_client()
        self.assertIsInstance(client, AnataWMSClient)
        self.assertEqual(client.base_url, "https://wms.example.com")
        self.assertEqual(client.account_number, "ACCT-1")

    def test_real_client_not_implemented(self):
        client = AnataWMSClient("https://wms.example.com", "ACCT-1", "key", "secret")
        with self.assertRaises(NotImplementedError):
            client.quote_rates(_small_product(), "84043", "10001")


class _AlwaysFailsClient:
    def quote_rates(self, package, origin_zip, dest_zip):
        raise RuntimeError("boom")


class BuildRateMatrixTests(unittest.TestCase):
    def test_skips_incomplete_products_with_warning(self):
        incomplete = ProductSpec(name="No Weight", length_in=8.0, width_in=6.0, height_in=4.0)
        matrix, warnings = build_rate_matrix(
            [_small_product("Widget"), incomplete], "84043", MockWMSClient()
        )
        self.assertEqual(len(matrix.products), 1)
        self.assertEqual(matrix.products[0].product.name, "Widget")
        self.assertTrue(
            any("No Weight" in w and "missing dims/weight" in w for w in warnings)
        )

    def test_dedupes_identical_specs_with_warning(self):
        matrix, warnings = build_rate_matrix(
            [_small_product("Widget"), _small_product("Widget Twin")],
            "84043",
            MockWMSClient(),
        )
        self.assertEqual(len(matrix.products), 1)
        self.assertTrue(
            any("Widget Twin" in w and "identical package spec" in w for w in warnings)
        )

    def test_matrix_source_is_mock(self):
        matrix, _ = build_rate_matrix([_small_product()], "84043", MockWMSClient())
        self.assertEqual(matrix.source, RATE_SOURCE_MOCK)

    def test_zones_sorted_ascending_with_sorted_quotes(self):
        matrix, _ = build_rate_matrix([_small_product()], "84043", MockWMSClient())
        zones = matrix.products[0].zones
        self.assertGreater(len(zones), 0)
        zone_numbers = [z.zone for z in zones]
        self.assertEqual(zone_numbers, sorted(zone_numbers))
        self.assertIn(8, zone_numbers)
        for zone in zones:
            rates = [q.rate_usd for q in zone.quotes]
            self.assertEqual(rates, sorted(rates))

    def test_invalid_origin_falls_back_to_hq(self):
        matrix, warnings = build_rate_matrix([_small_product()], "nope", MockWMSClient())
        self.assertEqual(matrix.origin_zip, ANATA_HQ_ZIP)
        self.assertTrue(any("invalid or unknown" in w for w in warnings))

    def test_not_implemented_falls_back_to_mock_rates(self):
        client = AnataWMSClient("https://wms.example.com", "ACCT-1", "key", "secret")
        matrix, warnings = build_rate_matrix([_small_product()], "84043", client)
        self.assertEqual(len(matrix.products), 1)
        self.assertGreater(len(matrix.products[0].zones), 0)
        self.assertEqual(matrix.source, RATE_SOURCE_MOCK)
        self.assertEqual(
            len([w for w in warnings if "WMS client unavailable" in w]), 1
        )

    def test_plain_exceptions_warn_and_skip_cells(self):
        matrix, warnings = build_rate_matrix(
            [_small_product()], "84043", _AlwaysFailsClient()
        )
        self.assertIsInstance(matrix, RateMatrix)
        self.assertEqual(len(matrix.products), 1)
        # Every cell failed: no zones, one warning per attempted zone.
        self.assertEqual(len(matrix.products[0].zones), 0)
        failure_notes = [w for w in warnings if "Rate quote failed" in w and "boom" in w]
        self.assertGreater(len(failure_notes), 0)


if __name__ == "__main__":
    unittest.main()
