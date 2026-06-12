from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from sales_support_agent.services.fulfillment_deck.rates import (
    build_rate_matrix,
    select_display_quotes,
)
from sales_support_agent.services.fulfillment_deck.schema import (
    ANATA_HQ_ZIP,
    RATE_SOURCE_MOCK,
    RATE_SOURCE_WMS,
    ProductSpec,
    RateMatrix,
    RateQuote,
)
from sales_support_agent.services.fulfillment_deck.wms_client import (
    AnataWMSClient,
    MockWMSClient,
    get_wms_client,
)
from sales_support_agent.services.fulfillment_deck.zones import zone_for


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

def _response(payload: dict) -> mock.Mock:
    response = mock.Mock()
    response.json.return_value = payload
    response.raise_for_status.return_value = None
    return response


def _token_response(token: str = "tok-1") -> mock.Mock:
    import time

    return _response({
        "result": "success",
        "data": {"tokens": {"access": {"token": token, "expires": time.time() + 3600}}},
    })


def _rates_response() -> mock.Mock:
    return _response({
        "result": "success",
        "data": {
            "rates": [
                {"carrier": "USPS", "service": "Ground Advantage", "rate": "7.42", "delivery_days": 3},
                {"carrier": "UPS", "service": "Ground", "rate": 9.10, "delivery_days": "4"},
                {"carrier": "Bad", "service": "Unparseable", "rate": "nope"},
                {"carrier": "Free", "service": "Zero", "rate": 0},
            ]
        },
    })


class AnataWMSClientUnitTests(unittest.TestCase):
    """EliteWorks client against canned requests.post responses — no network."""

    def setUp(self):
        AnataWMSClient._token_cache.clear()
        self.addCleanup(AnataWMSClient._token_cache.clear)
        self.client = AnataWMSClient("https://wms.example.com", "ACCT-1", "key", "secret")

    @mock.patch("requests.post")
    def test_quote_flow_auth_body_and_mapping(self, post):
        post.side_effect = [_token_response(), _rates_response()]
        quotes = self.client.quote_rates(_small_product(), "84043", "10001")

        # Call 1: token generation with Basic auth.
        token_call = post.call_args_list[0]
        self.assertEqual(token_call.args[0], "https://wms.example.com/api/auth/tokens/generate")
        import base64

        expected_basic = base64.b64encode(b"key:secret").decode()
        self.assertEqual(
            token_call.kwargs["headers"]["Authorization"], f"Basic {expected_basic}"
        )

        # Call 2: rating via /api/account/add with Bearer + AccountID.
        add_call = post.call_args_list[1]
        self.assertEqual(add_call.args[0], "https://wms.example.com/api/account/add")
        headers = add_call.kwargs["headers"]
        self.assertEqual(headers["Authorization"], "Bearer tok-1")
        self.assertEqual(headers["AccountID"], "ACCT-1")
        body = add_call.kwargs["json"]
        self.assertEqual(body["class_key"], "shipment")
        self.assertIs(body["model"]["rate"], True)
        self.assertIs(body["model"]["purchase"], False)
        package = body["model"]["shipment_packages"][0]
        self.assertTrue(package["shipment_items"])
        self.assertEqual(package["shipment_items"][0]["quantity"], 1)

        # Quote mapping: unparseable/zero rates dropped; the rest map cleanly.
        self.assertEqual(len(quotes), 2)
        usps = next(q for q in quotes if q.carrier == "USPS")
        self.assertEqual(usps.service, "Ground Advantage")
        self.assertEqual(usps.rate_usd, 7.42)
        self.assertEqual(usps.transit_days, 3)
        self.assertEqual(usps.source, RATE_SOURCE_WMS)
        self.assertEqual(usps.zone, zone_for("84043", "10001"))
        ups = next(q for q in quotes if q.carrier == "UPS")
        self.assertEqual(ups.transit_days, 4)

    @mock.patch("requests.post")
    def test_token_cached_across_quotes(self, post):
        post.side_effect = [_token_response(), _rates_response(), _rates_response()]
        self.client.quote_rates(_small_product(), "84043", "10001")
        self.client.quote_rates(_small_product(), "84043", "30303")
        # 1 token call + 2 rating calls — the cached token is reused.
        self.assertEqual(post.call_count, 3)
        self.assertEqual(
            post.call_args_list[2].kwargs["headers"]["Authorization"], "Bearer tok-1"
        )


class _AlwaysFailsClient:
    def quote_rates(self, package, origin_zip, dest_zip):
        raise RuntimeError("boom")


class _NotWiredClient:
    """Stands in for a real client that isn't implemented/configured yet."""

    def quote_rates(self, package, origin_zip, dest_zip):
        raise NotImplementedError("not wired up")


class _SixCarrierClient:
    """8 services across 6 carriers per cell — exercises display selection."""

    def quote_rates(self, package, origin_zip, dest_zip):
        zone = zone_for(origin_zip, dest_zip) or 5
        table = (
            ("CarrierA", "A-cheap", 5.0),
            ("CarrierA", "A-pricey", 6.5),
            ("CarrierB", "B-cheap", 6.0),
            ("CarrierB", "B-pricey", 9.0),
            ("CarrierC", "C-only", 7.0),
            ("CarrierD", "D-only", 8.0),
            ("CarrierE", "E-only", 9.5),
            ("CarrierF", "F-only", 11.0),
        )
        return [
            RateQuote(carrier=carrier, service=service, rate_usd=base + zone,
                      transit_days=3, zone=zone, source=RATE_SOURCE_WMS)
            for carrier, service, base in table
        ]


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
        matrix, warnings = build_rate_matrix([_small_product()], "84043", _NotWiredClient())
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


class SelectDisplayQuotesTests(unittest.TestCase):
    """Display selection: cheapest service per carrier, max-5 carriers."""

    def test_trims_to_cheapest_per_carrier_and_caps_carriers(self):
        matrix, warnings = build_rate_matrix(
            [_small_product()], "84043", _SixCarrierClient()
        )
        self.assertEqual(warnings, [])
        product = matrix.products[0]
        self.assertGreater(len(product.zones), 0)
        for zone in product.zones:
            carriers = [q.carrier for q in zone.quotes]
            # One quote per carrier (the cheapest service), 5 carriers max.
            self.assertEqual(len(carriers), len(set(carriers)))
            self.assertEqual(len(carriers), 5)
            # CarrierF has the worst average rate everywhere -> dropped.
            self.assertNotIn("CarrierF", carriers)
            # Cheapest-per-carrier survives: CarrierA keeps A-cheap, not A-pricey.
            carrier_a = next(q for q in zone.quotes if q.carrier == "CarrierA")
            self.assertEqual(carrier_a.service, "A-cheap")
            carrier_b = next(q for q in zone.quotes if q.carrier == "CarrierB")
            self.assertEqual(carrier_b.service, "B-cheap")
            # Quotes stay sorted by rate ascending.
            rates = [q.rate_usd for q in zone.quotes]
            self.assertEqual(rates, sorted(rates))

    def test_max_carriers_parameter(self):
        matrix, _ = build_rate_matrix([_small_product()], "84043", _SixCarrierClient())
        trimmed = select_display_quotes(matrix, max_carriers=2)
        for zone in trimmed.products[0].zones:
            self.assertEqual(
                sorted({q.carrier for q in zone.quotes}), ["CarrierA", "CarrierB"]
            )

    def test_excluded_carriers_dropped_and_fedex_surfaces(self):
        """YSP never displays; with YSP out of the way FEDEX ranks into the
        5-carrier cap (the v3 bug: YSP ate FedEx's slot)."""

        class _LiveLikeClient:
            """Mimics the live EliteWorks carrier mix: 6 carriers where YSP
            is cheap enough to claim a display slot ahead of FEDEX."""

            def quote_rates(self, package, origin_zip, dest_zip):
                zone = zone_for(origin_zip, dest_zip) or 5
                table = (
                    ("UNIUNI", "Standard", 4.0),
                    ("YSP", "Economy", 4.5),
                    ("USPS", "Ground Advantage", 5.0),
                    ("GLS", "Ground", 5.5),
                    ("UPS", "Ground", 6.0),
                    ("FEDEX", "Home Delivery", 6.5),
                )
                return [
                    RateQuote(carrier=carrier, service=service, rate_usd=base + zone,
                              transit_days=3, zone=zone, source=RATE_SOURCE_WMS)
                    for carrier, service, base in table
                ]

        matrix, _ = build_rate_matrix([_small_product()], "84043", _LiveLikeClient())
        for zone in matrix.products[0].zones:
            carriers = {q.carrier for q in zone.quotes}
            self.assertNotIn("YSP", carriers)
            self.assertIn("FEDEX", carriers)
            # Cap stays at 5 — and with YSP excluded all 5 remaining show.
            self.assertEqual(
                carriers, {"UNIUNI", "USPS", "GLS", "UPS", "FEDEX"}
            )

        # Env override replaces the default set, case-insensitively.
        with mock.patch.dict(os.environ, {"ANATA_RATE_EXCLUDED_CARRIERS": "ysp,gls"}):
            matrix, _ = build_rate_matrix([_small_product()], "84043", _LiveLikeClient())
            for zone in matrix.products[0].zones:
                carriers = {q.carrier for q in zone.quotes}
                self.assertEqual(carriers, {"UNIUNI", "USPS", "UPS", "FEDEX"})

        # Empty override falls back to the default exclusion set.
        with mock.patch.dict(os.environ, {"ANATA_RATE_EXCLUDED_CARRIERS": ""}):
            matrix, _ = build_rate_matrix([_small_product()], "84043", _LiveLikeClient())
            carriers = {q.carrier for q in matrix.products[0].zones[0].quotes}
            self.assertNotIn("YSP", carriers)

    def test_mock_data_passes_through_with_carrier_grouping(self):
        # Mock data has 3 carriers / 4 services: only USPS loses its pricier
        # Priority Mail service; everything else is untouched.
        matrix, _ = build_rate_matrix([_small_product()], "84043", MockWMSClient())
        for zone in matrix.products[0].zones:
            pairs = {(q.carrier, q.service) for q in zone.quotes}
            self.assertEqual(
                pairs,
                {
                    ("USPS", "Ground Advantage"),
                    ("UPS", "Ground"),
                    ("FedEx", "Home Delivery"),
                },
            )


if __name__ == "__main__":
    unittest.main()
