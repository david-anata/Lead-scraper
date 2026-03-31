from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from sales_support_agent.services.product_research import _fetch_amazon_page_data, _merge_listing_copy_segments


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
        with mock.patch("sales_support_agent.services.product_research.requests.get", return_value=fake_response):
            payload = _fetch_amazon_page_data("https://www.amazon.com/dp/B0TEST123")

        self.assertEqual(payload["title"], "Golf Hydration Lemon Lime Electrolyte Powder")
        self.assertEqual(payload["brand_name"], "Strokes Gained")
        self.assertIn("Hydration support for long rounds.", payload["description"])
        self.assertIn("Fast-mixing lemon lime formula.", payload["description"])
        self.assertIn("Built for golfers who need clean electrolyte support and steady energy.", payload["description"])
        self.assertIn("Meta description fallback.", payload["description"])


if __name__ == "__main__":
    unittest.main()
