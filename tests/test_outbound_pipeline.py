"""Tests for the StoreLeads -> Clay outbound pipeline (outbound_pipeline.py).

These verify the ICP gate, the Clay payload mapping, dedup, caps, and dry-run,
all without any live key (network calls are injected).
"""

from __future__ import annotations

import unittest

import outbound_pipeline as op


def _store(**over):
    base = {
        "name": "goodbrand.com",
        "merchant_name": "Good Brand",
        "platform": "shopify",
        "country_code": "US",
        "estimated_sales_yearly": 5_000_000_00,  # $5M/yr, in band
        "categories": "Beauty & Skincare",
        "tags": "",
        "contact_info": [{"type": "email", "value": "hi@goodbrand.com"}],
        "apps": [],
    }
    base.update(over)
    return base


class TestIcpGate(unittest.TestCase):
    def test_good_store_matches(self):
        self.assertTrue(op.store_matches_icp(_store()))

    def test_niche_is_detected(self):
        self.assertEqual(op.store_niche(_store()), "beauty_wellness")

    def test_revenue_above_ceiling_rejected(self):
        self.assertFalse(op.store_matches_icp(_store(estimated_sales_yearly=20_000_000_00)))

    def test_revenue_below_floor_rejected(self):
        self.assertFalse(op.store_matches_icp(_store(estimated_sales_yearly=200_000_00)))

    def test_wrong_country_rejected(self):
        self.assertFalse(op.store_matches_icp(_store(country_code="DE")))

    def test_uk_is_normalized_and_allowed(self):
        self.assertTrue(op.store_matches_icp(_store(country_code="UK")))

    def test_excluded_keyword_rejected(self):
        self.assertFalse(op.store_matches_icp(_store(tags="dropshipping, print on demand")))

    def test_non_shopify_rejected(self):
        self.assertFalse(op.store_matches_icp(_store(platform="woocommerce")))

    def test_no_email_rejected(self):
        self.assertFalse(op.store_matches_icp(_store(contact_info=[])))

    def test_off_niche_rejected(self):
        self.assertFalse(op.store_matches_icp(_store(categories="Industrial Equipment", merchant_name="Widgets Co")))


class TestClayLead(unittest.TestCase):
    def test_payload_shape(self):
        lead = op.to_clay_lead(_store())
        self.assertEqual(lead["domain"], "goodbrand.com")
        self.assertEqual(lead["brand"], "Good Brand")
        self.assertEqual(lead["niche"], "beauty_wellness")
        self.assertEqual(lead["country"], "US")


class TestRunPipeline(unittest.TestCase):
    def _fetch_two_pages(self):
        pages = {
            0: [_store(name="a.com"), _store(name="b.com", categories="Pet Supplies")],
            1: [_store(name="c.com", categories="Coffee & Tea")],
        }

        def fetch(api_key, *, page=0, page_size=50):
            return pages.get(page, [])

        return fetch

    def test_pushes_fresh_leads(self):
        pushed = {}

        def push(url, leads):
            pushed["leads"] = leads
            return {"pushed": len(leads)}

        r = op.run_storeleads_to_clay(
            api_key="x", clay_webhook_url="https://clay/webhook",
            processed_domains=set(), max_new=100,
            fetch_page=self._fetch_two_pages(), push=push,
        )
        self.assertEqual(r.fresh, 3)
        self.assertEqual(r.pushed, 3)
        self.assertEqual(len(pushed["leads"]), 3)

    def test_skips_already_contacted(self):
        r = op.run_storeleads_to_clay(
            api_key="x", clay_webhook_url="https://clay/webhook",
            processed_domains={"a.com"}, max_new=100,
            fetch_page=self._fetch_two_pages(), push=lambda u, l: {"pushed": len(l)},
        )
        self.assertEqual(r.skipped_already_contacted, 1)
        self.assertEqual(r.fresh, 2)

    def test_respects_max_new(self):
        r = op.run_storeleads_to_clay(
            api_key="x", clay_webhook_url="https://clay/webhook",
            processed_domains=set(), max_new=2,
            fetch_page=self._fetch_two_pages(), push=lambda u, l: {"pushed": len(l)},
        )
        self.assertEqual(r.fresh, 2)

    def test_dry_run_pushes_nothing(self):
        called = {"n": 0}

        def push(url, leads):
            called["n"] += 1
            return {"pushed": len(leads)}

        r = op.run_storeleads_to_clay(
            api_key="x", clay_webhook_url="https://clay/webhook",
            processed_domains=set(), max_new=100, dry_run=True,
            fetch_page=self._fetch_two_pages(), push=push,
        )
        self.assertTrue(r.dry_run)
        self.assertEqual(r.pushed, 0)
        self.assertEqual(called["n"], 0)
        self.assertEqual(r.fresh, 3)  # still builds the preview list

    def test_no_webhook_is_dry_run(self):
        r = op.run_storeleads_to_clay(
            api_key="x", clay_webhook_url="",
            processed_domains=set(), max_new=100,
            fetch_page=self._fetch_two_pages(), push=lambda u, l: {"pushed": len(l)},
        )
        self.assertTrue(r.dry_run)
        self.assertEqual(r.pushed, 0)


if __name__ == "__main__":
    unittest.main()
