from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from sales_support_agent.services.product_research import (
    ProductResearchService,
    _fetch_amazon_page_data,
    _merge_listing_copy_segments,
)


class ProductResearchTests(unittest.TestCase):
    def test_merge_listing_copy_segments_dedupes_and_preserves_order(self) -> None:
        merged = _merge_listing_copy_segments("First segment", "First segment", "Second segment")
        self.assertEqual(merged, "First segment\nSecond segment")

    def test_fetch_amazon_page_data_merges_feature_bullets_and_product_description(self) -> None:
        html = """
        <html>
          <head>
            <meta property="og:title" content="Golf Hydration Lemon Lime Electrolyte Powder" />
            <meta name="description" content="Meta description fallback." />
          </head>
          <body>
            <span id="productTitle">Golf Hydration Lemon Lime Electrolyte Powder</span>
            <div id="feature-bullets">
              <ul>
                <li><span>Hydration support for long rounds.</span></li>
                <li><span>Fast-mixing lemon lime formula.</span></li>
              </ul>
            </div>
            <div id="productDescription">
              <p>Built for golfers who need clean electrolyte support and steady energy.</p>
            </div>
            <a id="bylineInfo">Visit the Strokes Gained Store</a>
            <span class="a-offscreen">$34.99</span>
          </body>
        </html>
        """

        fake_response = SimpleNamespace(text=html, content=html.encode("utf-8"), raise_for_status=lambda: None)
        with mock.patch("requests.sessions.Session.get", return_value=fake_response):
            payload = _fetch_amazon_page_data("https://www.amazon.com/dp/B0TEST123")

        self.assertEqual(payload["title"], "Golf Hydration Lemon Lime Electrolyte Powder")
        self.assertEqual(payload["brand_name"], "Strokes Gained")
        self.assertIn("Hydration support for long rounds.", payload["description"])
        self.assertIn("Fast-mixing lemon lime formula.", payload["description"])
        self.assertIn("Built for golfers who need clean electrolyte support and steady energy.", payload["description"])
        self.assertIn("Meta description fallback.", payload["description"])

    def test_fetch_amazon_page_data_prefers_buy_box_total_over_unit_price(self) -> None:
        html = """
        <html><head><meta property="og:title" content="Three pack" /></head><body>
          <span id="productTitle">Three pack</span>
          <div id="corePriceDisplay_desktop_feature_div">
            <span class="a-price"><span class="a-offscreen">$28.50</span></span>
          </div></div>
          <span class="a-offscreen">$9.50</span><span>$9.50 per count</span>
        </body></html>
        """
        fake_response = SimpleNamespace(text=html, content=html.encode("utf-8"), raise_for_status=lambda: None)
        with mock.patch("requests.sessions.Session.get", return_value=fake_response):
            payload = _fetch_amazon_page_data("https://www.amazon.com/dp/B0TEST123")
        self.assertEqual(payload["price"], "$28.50")

    def test_fetch_amazon_page_data_ignores_per_count_price_ahead_of_checkout_total(self) -> None:
        # The buy-box widget can list the per-count amount first. The checkout
        # total still has to win, including when priceToPay JSON is nested.
        html = """
        <html><body>
          <div id="corePriceDisplay_desktop_feature_div">
            <span class="a-price a-text-price"><span class="a-offscreen">$9.50</span></span>
            <span>$9.50 per count</span>
          </div></div>
          <span class="a-price priceToPay"><span class="a-offscreen">$28.50</span></span>
          <script>{"unitPrice":{"priceAmount":"9.50"},"priceToPay":{"display":{"currency":"USD"},"priceAmount":28.50}}</script>
        </body></html>
        """
        fake_response = SimpleNamespace(text=html, content=html.encode("utf-8"), raise_for_status=lambda: None)
        with mock.patch("requests.sessions.Session.get", return_value=fake_response):
            payload = _fetch_amazon_page_data("https://www.amazon.com/dp/B0CZBQ7TCL")
        self.assertEqual(payload["price"], "$28.50")
        self.assertIn("9.50", payload["unit_prices"])

    def test_fetch_amazon_page_data_rejects_a_per_count_price_with_no_checkout_total(self) -> None:
        html = """
        <html><body>
          <span class="a-offscreen">$9.50</span><span>$9.50 per count</span>
          <script>"priceAmount":"9.50"</script>
        </body></html>
        """
        fake_response = SimpleNamespace(text=html, content=html.encode("utf-8"), raise_for_status=lambda: None)
        with mock.patch("requests.sessions.Session.get", return_value=fake_response):
            payload = _fetch_amazon_page_data("https://www.amazon.com/dp/B0CZBQ7TCL")
        self.assertEqual(payload["price"], "")

    def test_catalog_price_matching_a_per_count_amount_is_not_used(self) -> None:
        page = {
            "source_url": "https://www.amazon.com/dp/B0CZBQ7TCL",
            "title": "Dixie Belle Women's Underwear Nylon Brief 3-Pack",
            "image_url": "https://example.com/hero.jpg",
            "price": "",
            "unit_prices": ["9.50"],
            "description": "",
            "brand_name": "Dixie Belle",
            "category": "",
            "dimensions": "",
            "bsr": "",
            "rating": "4.4",
            "review_count": "788",
            "warnings": [],
        }
        client = SimpleNamespace(is_configured=lambda: False)
        service = ProductResearchService(amazon_client=client)  # type: ignore[arg-type]
        with (
            mock.patch("sales_support_agent.services.product_research._fetch_amazon_page_data", return_value=page),
            mock.patch(
                "sales_support_agent.services.product_research._fetch_remote_catalog_data",
                return_value={"price": "$9.50", "title": page["title"]},
            ),
            mock.patch("sales_support_agent.services.product_research._fetch_amazon_search_data", return_value={}),
            mock.patch("sales_support_agent.services.product_research._fetch_public_asin_fallback", return_value={}),
        ):
            product = service.enrich_target_product({"source_type": "amazon", "asin": "B0CZBQ7TCL", "source_url": page["source_url"]})
        self.assertEqual(product.price, "")


if __name__ == "__main__":
    unittest.main()
