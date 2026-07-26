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
        self.assertFalse(op.store_matches_icp(_store(estimated_sales_yearly=25_000_000_00)))

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

        def fetch(api_key, *, page=0, page_size=50, **kw):
            return pages.get(page, [])

        return fetch

    def test_pushes_fresh_leads(self):
        pushed = {}

        def push(url, leads):
            pushed["leads"] = leads
            return {"pushed": len(leads)}

        r = op.run_storeleads_to_clay(
            api_key="x", clay_webhook_url="https://clay/webhook",
            processed_domains=set(), max_new=100, throttle_seconds=0,
            fetch_page=self._fetch_two_pages(), push=push,
        )
        self.assertEqual(r.fresh, 3)
        self.assertEqual(r.pushed, 3)
        self.assertEqual(len(pushed["leads"]), 3)

    def test_skips_already_contacted(self):
        r = op.run_storeleads_to_clay(
            api_key="x", clay_webhook_url="https://clay/webhook",
            processed_domains={"a.com"}, max_new=100, throttle_seconds=0,
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
            processed_domains=set(), max_new=100, dry_run=True, throttle_seconds=0,
            fetch_page=self._fetch_two_pages(), push=push,
        )
        self.assertTrue(r.dry_run)
        self.assertEqual(r.pushed, 0)
        self.assertEqual(called["n"], 0)
        self.assertEqual(r.fresh, 3)  # still builds the preview list

    def test_no_webhook_is_dry_run(self):
        r = op.run_storeleads_to_clay(
            api_key="x", clay_webhook_url="",
            processed_domains=set(), max_new=100, throttle_seconds=0,
            fetch_page=self._fetch_two_pages(), push=lambda u, l: {"pushed": len(l)},
        )
        self.assertTrue(r.dry_run)
        self.assertEqual(r.pushed, 0)


class TestAmazonReachesTheLead(unittest.TestCase):
    """The Amazon check has to actually run during a pull and actually win.

    Shipped once without these: brand_control existed, was fully unit tested, and
    was never called by run_storeleads_to_clay. Every lead kept the old
    plan-upgrade line and the Amazon work never reached a single email.
    """

    QUESTION = "There are a handful of other sellers on your listing. Are those all authorized?"

    def _fetch(self):
        def fetch(api_key, *, page=0, page_size=50, **kw):
            return [_store(name="a.com")] if page == 0 else []
        return fetch

    class _Recipe:
        key = "plan_upgrade"

        def params(self, now, settings):
            return {}

        def keeps(self, store, now, settings):
            return True

        def reason_for(self, settings):
            return "They upgraded their store plan recently"

    def _run(self, amazon_check, recipe=None):
        return op.run_storeleads_to_clay(
            api_key="x", clay_webhook_url="https://clay/webhook",
            processed_domains=set(), max_new=10, throttle_seconds=0, recipe=recipe,
            fetch_page=self._fetch(), push=lambda u, l: {"pushed": len(l), "leads": l},
            amazon_check=amazon_check,
        )

    def test_the_check_is_called_for_every_matched_brand(self):
        seen = []
        self._run(lambda lead: seen.append(lead["domain"]) or None)
        self.assertEqual(seen, ["a.com"], "the pull never called the Amazon check")

    def test_an_amazon_finding_beats_the_recipe_reason(self):
        """The recipe writes its reason AFTER the lead is built, so an Amazon
        result applied too early is silently overwritten and nobody notices."""
        captured = {}

        def push(url, leads):
            captured["leads"] = leads
            return {"pushed": len(leads)}

        op.run_storeleads_to_clay(
            api_key="x", clay_webhook_url="https://clay/webhook",
            processed_domains=set(), max_new=10, throttle_seconds=0,
            recipe=self._Recipe(),
            fetch_page=self._fetch(), push=push,
            amazon_check=lambda lead: {"reason": self.QUESTION, "confidence": "high",
                                       "marketplace": "amazon.com",
                                       "checked_at": "2026-07-26T12:00:00+00:00",
                                       "findings": {"listings": [{"sellers_unknown": 18}],
                                                    "absent": False}},
        )
        lead = captured["leads"][0]
        self.assertEqual(lead["reason"], self.QUESTION)
        self.assertNotIn("upgraded their store plan", lead["reason"])
        self.assertEqual(lead["signals"][0], self.QUESTION)
        self.assertEqual(lead["amazon_confidence"], "high")
        self.assertEqual(lead["amazon_sellers_unknown"], 18)

    def test_the_recipe_reason_stands_when_amazon_found_nothing(self):
        captured = {}

        def push(url, leads):
            captured["leads"] = leads
            return {"pushed": len(leads)}

        op.run_storeleads_to_clay(
            api_key="x", clay_webhook_url="https://clay/webhook",
            processed_domains=set(), max_new=10, throttle_seconds=0,
            recipe=self._Recipe(),
            fetch_page=self._fetch(), push=push,
            amazon_check=lambda lead: {"reason": "", "skipped_reason": "no confident match",
                                       "confidence": "low", "marketplace": "amazon.com",
                                       "checked_at": "2026-07-26T12:00:00+00:00",
                                       "findings": {"listings": [], "absent": False}},
        )
        lead = captured["leads"][0]
        self.assertIn("upgraded their store plan", lead["reason"])
        self.assertEqual(lead["amazon_skipped_reason"], "no confident match")

    def test_a_broken_amazon_check_does_not_kill_the_pull(self):
        """A data provider outage must cost us the finding, not the batch."""
        def boom(lead):
            raise RuntimeError("rainforest down")
        r = self._run(boom)
        self.assertEqual(r.fresh, 1)

    def test_no_check_supplied_behaves_exactly_as_before(self):
        r = self._run(None)
        self.assertEqual(r.fresh, 1)


class TestLeadsToCsv(unittest.TestCase):
    def test_headers_match_clay_columns_so_import_auto_maps(self):
        """Clay creates a NEW column when the header does not match an existing
        one, and case matters: 'domain' makes a second text column while 'Domain'
        matches the one the enrichment reads from."""
        header = op.leads_to_csv([]).strip()
        self.assertIn("Merchant Name", header)
        self.assertIn("Domain", header)
        self.assertIn("personalization", header)
        self.assertNotIn("estimated_sales_yearly_cents", header)
        # the raw lowercase field names must NOT be the header
        cols = header.split(",")
        self.assertNotIn("brand", cols)
        self.assertNotIn("domain", cols)
        self.assertNotIn("reason", cols)

    def test_revenue_is_exported_in_dollars_not_cents(self):
        """Cents in a human-read column makes an $8.4M brand look like $835M."""
        import csv as _csv, io as _io
        lead = op.to_clay_lead(_store(name="a.com", estimated_sales_yearly=835_227_012))
        rows = list(_csv.DictReader(_io.StringIO(op.leads_to_csv([lead]))))
        self.assertEqual(rows[0]["revenue_usd"], "8352270")   # $8.35M, readable

    def test_ceiling_is_twenty_million(self):
        self.assertTrue(op.store_matches_icp(_store(estimated_sales_yearly=19_000_000_00)))
        self.assertFalse(op.store_matches_icp(_store(estimated_sales_yearly=21_000_000_00)))

    def test_row_values_still_line_up_with_the_renamed_headers(self):
        import csv as _csv, io as _io
        lead = op.to_clay_lead(_store(name="a.com"))
        rows = list(_csv.DictReader(_io.StringIO(op.leads_to_csv([lead]))))
        self.assertEqual(rows[0]["Domain"], "a.com")
        self.assertEqual(rows[0]["Merchant Name"], "Good Brand")

    def test_csv_has_header_and_rows(self):
        leads = [op.to_clay_lead(_store(name="a.com")), op.to_clay_lead(_store(name="b.com", categories=["Pet", "Toys"]))]
        csv_text = op.leads_to_csv(leads)
        lines = csv_text.strip().splitlines()
        self.assertEqual(lines[0], ",".join(op.clay_header(c) for c in op.CLAY_CSV_COLUMNS))
        self.assertEqual(len(lines), 3)  # header + 2 rows
        self.assertIn("a.com", csv_text)
        self.assertIn("Pet, Toys", csv_text)  # list categories flattened

    def test_empty_leads_still_has_header(self):
        csv_text = op.leads_to_csv([])
        self.assertEqual(csv_text.strip(), ",".join(op.clay_header(c) for c in op.CLAY_CSV_COLUMNS))


if __name__ == "__main__":
    unittest.main()
