"""Tests for the Rainforest API client (Digital Shelf feature)."""

from __future__ import annotations

import dataclasses
import unittest
from unittest.mock import MagicMock, patch

from sales_support_agent.services.rainforest import (
    RainforestClient,
    _bsr_to_units,
    _normalize_asin,
)
from sales_support_agent.services.helium10 import Helium10XrayReport, XrayProduct


def _mock_product(
    asin: str,
    *,
    title: str = "Test Product",
    brand: str = "TestBrand",
    bsr: int = 5000,
    price: float = 29.99,
    rating: float = 4.3,
    ratings_total: int = 1200,
    category: str = "Health & Household",
    category_url: str = "https://www.amazon.com/Best-Sellers/zgbs/hpc/1/ref=zg_bs_nav_hpc_1",
) -> dict:
    return {
        "request_info": {"success": True},
        "product": {
            "asin": asin,
            "title": title,
            "brand": brand,
            "link": f"https://www.amazon.com/dp/{asin}",
            "main_image": {"link": f"https://images.amazon.com/{asin}.jpg"},
            "rating": rating,
            "ratings_total": ratings_total,
            "buybox_winner": {"price": {"symbol": "$", "value": price, "currency": "USD"}},
            "bestsellers_rank": [
                {"rank": bsr, "category": category, "category_url": category_url}
            ],
        },
    }


def _mock_bestsellers(asins: list[str]) -> dict:
    return {
        "request_info": {"success": True},
        "bestsellers": [
            {"asin": a, "rank": i + 1, "title": f"Product {a}"}
            for i, a in enumerate(asins)
        ],
    }


class TestBsrToUnits(unittest.TestCase):
    def test_standard_bsr(self):
        # BSR 1000 → ~75 units
        units = _bsr_to_units(1000)
        self.assertEqual(units, 75)

    def test_high_bsr_low_units(self):
        units = _bsr_to_units(750_000)
        self.assertGreaterEqual(units, 1)

    def test_very_low_bsr_capped(self):
        # BSR 1 would be 75,000 — capped at 50,000
        units = _bsr_to_units(1)
        self.assertEqual(units, 50_000)

    def test_none_bsr(self):
        self.assertEqual(_bsr_to_units(None), 0)

    def test_zero_bsr(self):
        self.assertEqual(_bsr_to_units(0), 0)


class TestNormalizeAsin(unittest.TestCase):
    def test_bare_asin(self):
        self.assertEqual(_normalize_asin("B09ABCDEF1"), "B09ABCDEF1")

    def test_amazon_url(self):
        url = "https://www.amazon.com/dp/B09ABCDEF1/ref=cm_sw"
        self.assertEqual(_normalize_asin(url), "B09ABCDEF1")

    def test_empty(self):
        self.assertEqual(_normalize_asin(""), "")

    def test_non_asin_non_url(self):
        self.assertEqual(_normalize_asin("not-an-asin"), "")


class TestRainforestClientProductToXray(unittest.TestCase):
    def setUp(self):
        self.client = RainforestClient(api_key="test_key")

    def test_converts_product_to_xray_product(self):
        raw = _mock_product("B09ABCDEF1", bsr=10_000, price=39.99)
        xp = self.client._product_to_xray(raw, display_order=1)
        self.assertIsInstance(xp, XrayProduct)
        self.assertEqual(xp.asin, "B09ABCDEF1")
        self.assertEqual(xp.brand, "TestBrand")
        self.assertAlmostEqual(xp.price, 39.99)
        self.assertEqual(xp.display_order, 1)

    def test_bsr_units_and_revenue_estimated(self):
        raw = _mock_product("B09ABCDEF1", bsr=5_000, price=20.00)
        xp = self.client._product_to_xray(raw, display_order=1)
        expected_units = _bsr_to_units(5_000)  # 15
        self.assertEqual(xp.units_sold, float(expected_units))
        self.assertAlmostEqual(xp.revenue, expected_units * 20.00)

    def test_missing_price_produces_none_revenue(self):
        raw = _mock_product("B09ABCDEF1", bsr=5_000, price=0.0)
        raw["product"]["buybox_winner"] = {}
        raw["product"].pop("price", None)
        xp = self.client._product_to_xray(raw, display_order=1)
        self.assertIsNone(xp.revenue)

    def test_empty_product_returns_none(self):
        xp = self.client._product_to_xray({}, display_order=1)
        self.assertIsNone(xp)


class TestRainforestBuildXrayReport(unittest.TestCase):
    def setUp(self):
        self.client = RainforestClient(api_key="test_key")

    @patch.object(RainforestClient, "get_product")
    @patch.object(RainforestClient, "get_bestsellers")
    def test_build_xray_report_returns_report_and_raw(
        self, mock_bestsellers, mock_get_product
    ):
        target_asin = "B09AAAAAAA"
        competitor_asins = [f"B09{str(i).zfill(7)}" for i in range(5)]
        category_url = "https://www.amazon.com/Best-Sellers/zgbs/hpc/1/"

        # get_product: first call = target, subsequent calls = competitors
        target_raw = _mock_product(target_asin, bsr=2000, price=49.99, category_url=category_url)
        comp_raws = {
            asin: _mock_product(asin, bsr=3000 + i * 500, price=39.99)
            for i, asin in enumerate(competitor_asins)
        }

        def mock_product_side_effect(asin):
            if asin == target_asin:
                return target_raw
            return comp_raws.get(asin, _mock_product(asin, bsr=9999))

        mock_get_product.side_effect = mock_product_side_effect
        mock_bestsellers.return_value = _mock_bestsellers(competitor_asins)

        report, raw = self.client.build_xray_report(target_asin, competitor_limit=5)

        self.assertIsInstance(report, Helium10XrayReport)
        self.assertIsInstance(raw, dict)
        self.assertGreater(len(report.products), 0)
        # All products should have BSR-estimated units
        for p in report.products:
            self.assertIsNotNone(p.units_sold)
        # Report totals should be non-zero
        self.assertGreater(report.total_revenue, 0)
        self.assertGreater(report.total_units_sold, 0)

    @patch.object(RainforestClient, "get_product")
    @patch.object(RainforestClient, "get_bestsellers")
    def test_products_sorted_by_bsr(self, mock_bestsellers, mock_get_product):
        target_asin = "B09AAAAAAA"
        competitor_asins = ["B09CC11111", "B09BB22222"]  # order in bestsellers
        category_url = "https://www.amazon.com/Best-Sellers/zgbs/hpc/1/"

        target_raw = _mock_product(target_asin, bsr=1000, category_url=category_url)
        # CC has higher BSR (worse rank), BB has lower BSR (better rank)
        comp_raws = {
            "B09CC11111": _mock_product("B09CC11111", bsr=8000),
            "B09BB22222": _mock_product("B09BB22222", bsr=3000),
        }

        mock_get_product.side_effect = lambda asin: (
            target_raw if asin == target_asin else comp_raws[asin]
        )
        mock_bestsellers.return_value = _mock_bestsellers(competitor_asins)

        report, _ = self.client.build_xray_report(target_asin, competitor_limit=10)

        bsrs = [p.bsr for p in report.products]
        self.assertEqual(bsrs, sorted(bsrs))

    def test_missing_api_key_raises(self):
        client = RainforestClient(api_key="")
        with self.assertRaises(RuntimeError, msg="RAINFOREST_API_KEY is not configured."):
            client.build_xray_report("B09ABCDEF1")

    def test_invalid_asin_raises(self):
        client = RainforestClient(api_key="test_key")
        with self.assertRaises(RuntimeError):
            client.build_xray_report("not-an-asin-or-url")


class TestRainforestWarnings(unittest.TestCase):
    @patch.object(RainforestClient, "get_product")
    @patch.object(RainforestClient, "get_bestsellers")
    def test_report_includes_bsr_estimate_warning(self, mock_bestsellers, mock_get_product):
        target_asin = "B09AAAAAAA"
        category_url = "https://www.amazon.com/Best-Sellers/zgbs/hpc/1/"
        mock_get_product.return_value = _mock_product(target_asin, bsr=5000, category_url=category_url)
        mock_bestsellers.return_value = _mock_bestsellers(["B09BB00001"])
        mock_get_product.side_effect = lambda a: (
            _mock_product(target_asin, bsr=5000, category_url=category_url)
            if a == target_asin
            else _mock_product(a, bsr=6000)
        )

        client = RainforestClient(api_key="key")
        report, _ = client.build_xray_report(target_asin)

        self.assertTrue(any("BSR" in w for w in report.warnings))


if __name__ == "__main__":
    unittest.main()
