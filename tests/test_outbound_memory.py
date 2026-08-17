"""Tests for the never-email-twice outbound memory (SQLite-backed here)."""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from sqlalchemy import create_engine

from sales_support_agent.services import outbound_memory as m


class OutboundMemoryTests(unittest.TestCase):
    def _engine(self):
        return create_engine("sqlite://", future=True)  # in-memory, per-test

    def test_starts_empty(self):
        self.assertEqual(m.load_contacted(self._engine()), set())

    def test_record_normalizes_and_dedups(self):
        e = self._engine()
        n = m.record_contacted(e, ["A.com", "b.com ", " b.com", "", "x.com"])
        self.assertEqual(n, 3)
        self.assertEqual(m.load_contacted(e), {"a.com", "b.com", "x.com"})

    def test_record_is_idempotent(self):
        e = self._engine()
        m.record_contacted(e, ["a.com", "b.com"])
        m.record_contacted(e, ["a.com", "new.com"])  # overlap must not error/dupe
        self.assertEqual(m.load_contacted(e), {"a.com", "b.com", "new.com"})

    def test_exported_domains_excluded_on_next_pull(self):
        """The set load_contacted returns is exactly what the pipeline treats as
        already-contacted, so a re-export skips them."""
        e = self._engine()
        m.record_contacted(e, ["seen.com"])
        already = m.load_contacted(e)
        self.assertIn("seen.com", already)

    def test_load_fails_open_on_bad_engine(self):
        self.assertEqual(m.load_contacted(object()), set())  # no crash, empty set

    def test_record_fails_open_on_bad_engine(self):
        self.assertEqual(m.record_contacted(object(), ["a.com"]), 0)  # no crash

    def test_record_leads_persists_tier_and_signals(self):
        e = self._engine()
        n = m.record_leads(e, [
            {"domain": "A.com", "tier": "A", "signals": ["Runs Meta and Google ads"]},
            {"domain": "b.com", "tier": "C", "signals": []},
        ])
        self.assertEqual(n, 2)
        pushed = {p["domain"]: p for p in m.load_pushed(e)}
        self.assertEqual(pushed["a.com"]["tier"], "A")
        self.assertEqual(pushed["a.com"]["signals"], ["Runs Meta and Google ads"])
        self.assertEqual(pushed["b.com"]["signals"], [])

    def test_record_leads_also_dedups_future_pulls(self):
        e = self._engine()
        m.record_leads(e, [{"domain": "seen.com", "tier": "B", "signals": ["x"]}])
        self.assertIn("seen.com", m.load_contacted(e))

    def test_load_pushed_empty_on_bad_engine(self):
        self.assertEqual(m.load_pushed(object()), [])

    def test_record_leads_fails_open_on_bad_engine(self):
        self.assertEqual(m.record_leads(object(), [{"domain": "a.com"}]), 0)


class RunTrackingTests(unittest.TestCase):
    """Every pull is logged so we can see what we pulled, when, and from where."""

    def _engine(self):
        return create_engine("sqlite://", future=True)

    def test_starts_with_no_runs(self):
        self.assertEqual(m.load_runs(self._engine()), [])

    def test_records_and_reads_back_a_run(self):
        e = self._engine()
        ok = m.record_run(e, recipe="new_growth_app", scanned=200, matched=40,
                          fresh=25, skipped_seen=15)
        self.assertTrue(ok)
        runs = m.load_runs(e)
        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0]["recipe"], "new_growth_app")
        self.assertEqual(runs[0]["scanned"], 200)
        self.assertEqual(runs[0]["fresh"], 25)
        self.assertFalse(runs[0]["partial"])

    def test_partial_flag_survives(self):
        e = self._engine()
        m.record_run(e, recipe="x", scanned=1, matched=1, fresh=1,
                     skipped_seen=0, partial=True, note="rate limited")
        r = m.load_runs(e)[0]
        self.assertTrue(r["partial"])
        self.assertEqual(r["note"], "rate limited")

    def test_limit_is_respected(self):
        e = self._engine()
        for i in range(5):
            m.record_run(e, recipe=f"r{i}", scanned=1, matched=1, fresh=1, skipped_seen=0)
        self.assertEqual(len(m.load_runs(e, limit=3)), 3)

    def test_fails_open_on_bad_engine(self):
        self.assertFalse(m.record_run(object(), recipe="x", scanned=0, matched=0,
                                      fresh=0, skipped_seen=0))
        self.assertEqual(m.load_runs(object()), [])

    def test_exact_pull_membership_is_exportable_without_changing_contact_memory(self):
        e = self._engine()
        run_id = m.record_run(e, recipe="social_surge", scanned=20, matched=3,
                              fresh=2, skipped_seen=1)
        before = m.load_contacted(e)
        self.assertEqual(m.record_run_leads(e, run_id, [
            {"domain": "a.com", "brand": "A"}, {"domain": "b.com", "brand": "B"},
        ]), 2)
        leads = m.load_run_leads(e, [run_id])
        self.assertEqual({x["domain"] for x in leads}, {"a.com", "b.com"})
        self.assertEqual(leads[0]["pull_recipe"], "social_surge")
        self.assertEqual(m.load_contacted(e), before)

    def test_delivery_settings_round_trip(self):
        e = self._engine()
        self.assertTrue(m.save_delivery_settings(e, {
            "enabled": "1", "email_enabled": "1", "slack_enabled": "0",
            "frequency": "every_pull", "email_recipients": "david@anatainc.com",
            "content_mode": "link",
        }, actor="david@anatainc.com"))
        prefs = m.load_delivery_settings(e)
        self.assertTrue(prefs["enabled"])
        self.assertTrue(prefs["email_enabled"])
        self.assertFalse(prefs["slack_enabled"])
        self.assertEqual(prefs["email_recipients"], "david@anatainc.com")

    def test_export_history_records_only_metadata(self):
        e = self._engine()
        self.assertTrue(m.record_export(e, actor="david@anatainc.com", run_ids=[1, 2],
                                        source_rows=4, unique_companies=3,
                                        duplicates_removed=1, include_duplicates=False,
                                        filename="companies.csv"))
        item = m.load_exports(e)[0]
        self.assertEqual(item["run_ids"], "1,2")
        self.assertEqual(item["unique_companies"], 3)


class ReleaseTests(unittest.TestCase):
    """Brands pulled but never actually emailed must be recoverable, without
    ever making it easy to release a brand that really was contacted."""

    def _e(self):
        return create_engine("sqlite://", future=True)

    def test_release_named_domains_only(self):
        e = self._e()
        m.record_leads(e, [{"domain": "a.com"}, {"domain": "b.com"}])
        self.assertEqual(m.release_contacted(e, ["a.com"]), 1)
        self.assertEqual(m.load_contacted(e), {"b.com"})

    def test_release_all_returns_the_count(self):
        e = self._e()
        m.record_leads(e, [{"domain": "a.com"}, {"domain": "b.com"}, {"domain": "c.com"}])
        self.assertEqual(m.release_contacted(e), 3)
        self.assertEqual(m.load_contacted(e), set())

    def test_released_brand_can_be_sourced_again(self):
        e = self._e()
        m.record_leads(e, [{"domain": "a.com"}])
        m.release_contacted(e, ["a.com"])
        self.assertNotIn("a.com", m.load_contacted(e))

    def test_release_normalizes_input(self):
        e = self._e()
        m.record_leads(e, [{"domain": "a.com"}])
        self.assertEqual(m.release_contacted(e, ["  A.COM "]), 1)
        self.assertEqual(m.load_contacted(e), set())

    def test_release_fails_safe(self):
        self.assertEqual(m.release_contacted(None), 0)
        self.assertEqual(m.release_contacted(object(), ["a.com"]), 0)


class FullLeadRecordTests(unittest.TestCase):
    """Our database is the system of record for leads. Clay and Instantly are
    processors, so losing either must never lose the lead itself."""

    def _e(self):
        return create_engine("sqlite://", future=True)

    def _lead(self, domain="rho.com"):
        return {"domain": domain, "brand": "Rho Nutrition", "niche": "beauty_wellness",
                "country": "US", "tier": "A", "score": 16,
                "reason": "They upgraded their store plan", "recipe": "plan_upgrade",
                "estimated_sales_yearly_cents": 835227012,
                "categories": ["Health", "Vitamins"], "signals": ["upgrade"]}

    def test_the_whole_record_is_kept_not_just_the_domain(self):
        e = self._e()
        m.record_leads(e, [self._lead()], config_version=2)
        l = m.load_leads(e)[0]
        self.assertEqual(l["brand"], "Rho Nutrition")
        self.assertEqual(l["niche"], "beauty_wellness")
        self.assertEqual(l["country"], "US")
        self.assertEqual(l["tier"], "A")
        self.assertEqual(l["score"], 16)
        self.assertEqual(l["recipe"], "plan_upgrade")
        self.assertEqual(l["revenue_cents"], 835227012)
        self.assertEqual(l["config_version"], 2)
        self.assertIn("upgraded", l["reason"])

    def test_categories_are_flattened(self):
        e = self._e()
        m.record_leads(e, [self._lead()])
        self.assertEqual(m.load_leads(e)[0]["categories"], "Health, Vitamins")

    def test_signals_come_back_as_a_list(self):
        e = self._e()
        m.record_leads(e, [self._lead()])
        self.assertEqual(m.load_leads(e)[0]["signals"], ["upgrade"])

    def test_bad_numbers_do_not_break_the_record(self):
        e = self._e()
        bad = self._lead()
        bad["score"] = "not a number"
        bad["estimated_sales_yearly_cents"] = None
        m.record_leads(e, [bad])
        l = m.load_leads(e)[0]
        self.assertEqual(l["score"], 0)
        self.assertEqual(l["revenue_cents"], 0)

    def test_dedup_still_works_off_the_same_table(self):
        e = self._e()
        m.record_leads(e, [self._lead()])
        self.assertIn("rho.com", m.load_contacted(e))

    def test_load_leads_is_safe_without_a_database(self):
        self.assertEqual(m.load_leads(None), [])

    def test_complete_library_export_can_load_every_company(self):
        e = self._e()
        m.record_leads(e, [self._lead(f"brand-{i}.com") for i in range(4)])
        self.assertEqual(len(m.load_leads(e, limit=2)), 2)
        self.assertEqual(len(m.load_leads(e, limit=None)), 4)

    def test_company_library_csv_is_complete_and_read_only(self):
        from sales_support_agent.api.outbound_router import outbound_leads_csv

        e = self._e()
        m.record_leads(e, [self._lead("a.com"), self._lead("b.com")])
        before = m.load_contacted(e)
        with patch("sales_support_agent.models.database.get_engine", return_value=e):
            response = outbound_leads_csv(None)
        body = response.body.decode("utf-8")
        self.assertIn("a.com", body)
        self.assertIn("b.com", body)
        self.assertIn("anata_company_library_clay.csv", response.headers["content-disposition"])
        self.assertEqual(m.load_contacted(e), before)

    def test_company_library_groups_sourcing_filtering_and_export_actions(self):
        from sales_support_agent.api.outbound_router import outbound_leads

        e = self._e()
        m.record_leads(e, [self._lead("rho.com")])
        with (
            patch("sales_support_agent.models.database.get_engine", return_value=e),
            patch("sales_support_agent.api.outbound_router.get_current_user", return_value={}),
        ):
            response = outbound_leads(None)
        body = response.body.decode("utf-8")
        self.assertIn("Company Library", body)
        self.assertIn("Download all for Clay", body)
        self.assertIn("Find fresh companies", body)
        self.assertIn("Manage sourcing", body)
        self.assertIn("View prospecting performance", body)
        self.assertIn('id="ld-search"', body)
        self.assertIn("Companies held", body)
        self.assertIn("Average yearly sales", body)
        self.assertIn("Tier mix", body)
        self.assertIn("rho.com", body)

        empty = self._e()
        with (
            patch("sales_support_agent.models.database.get_engine", return_value=empty),
            patch("sales_support_agent.api.outbound_router.get_current_user", return_value={}),
        ):
            empty_body = outbound_leads(None).body.decode("utf-8")
        self.assertIn("Use Find fresh companies to create the first batch", empty_body)
        self.assertNotIn("No leads stored yet", empty_body)


if __name__ == "__main__":
    unittest.main()


class AmazonScanStorageTests(unittest.TestCase):
    """A scan revisits brands we already hold, which record_leads cannot do."""

    def _e(self):
        return create_engine("sqlite://", future=True)

    def _finding(self, **over):
        base = {"reason": "There are a handful of other sellers on your listing. All authorized?",
                "confidence": "high", "marketplace": "amazon.com",
                "checked_at": "2026-07-26T12:00:00+00:00", "skipped_reason": "",
                "findings": {"listings": [{"sellers_unknown": 18}], "absent": False}}
        base.update(over)
        return base

    def test_a_finding_lands_on_a_brand_we_already_hold(self):
        """record_leads leaves existing rows alone, so without this the scan
        would silently do nothing."""
        e = self._e()
        m.record_leads(e, [{"domain": "rho.com", "brand": "Rho", "tier": "A", "score": 9,
                            "reason": "They upgraded their store plan recently"}])
        self.assertTrue(m.update_amazon_finding(e, "rho.com", self._finding()))
        lead = m.load_leads(e)[0]
        self.assertIn("other sellers", lead["reason"])
        self.assertEqual(lead["amazon_confidence"], "high")
        self.assertEqual(lead["amazon_sellers_unknown"], 18)

    def test_a_skipped_brand_keeps_the_reason_it_had(self):
        """Overwriting with a blank would leave the lead with no opener at all."""
        e = self._e()
        m.record_leads(e, [{"domain": "rho.com", "brand": "Rho", "tier": "A",
                            "reason": "They upgraded their store plan recently"}])
        m.update_amazon_finding(e, "rho.com", self._finding(
            reason="", skipped_reason="no confident match", confidence="low"))
        lead = m.load_leads(e)[0]
        self.assertIn("upgraded their store plan", lead["reason"])
        self.assertEqual(lead["amazon_skipped_reason"], "no confident match")

    def test_an_unknown_brand_is_not_invented(self):
        self.assertFalse(m.update_amazon_finding(self._e(), "nope.com", self._finding()))

    def test_it_fails_safe_without_a_database(self):
        self.assertFalse(m.update_amazon_finding(None, "rho.com", self._finding()))

    def test_the_queue_puts_the_best_brands_first(self):
        """Each brand costs minutes and money, so we check the ones we would
        actually email, not whatever came back first."""
        e = self._e()
        m.record_leads(e, [
            {"domain": "c.com", "brand": "C", "tier": "C", "score": 2},
            {"domain": "a.com", "brand": "A", "tier": "A", "score": 14},
            {"domain": "b.com", "brand": "B", "tier": "B", "score": 6},
        ])
        order = [l["domain"] for l in m.leads_needing_amazon(e, limit=3)]
        self.assertEqual(order, ["a.com", "b.com", "c.com"])

    def test_the_queue_is_bounded(self):
        e = self._e()
        m.record_leads(e, [{"domain": f"{i}.com", "brand": str(i), "tier": "A"} for i in range(9)])
        self.assertEqual(len(m.leads_needing_amazon(e, limit=3)), 3)

    def test_a_brand_checked_recently_is_not_checked_again(self):
        e = self._e()
        m.record_leads(e, [{"domain": "rho.com", "brand": "Rho", "tier": "A"}])
        m.update_amazon_finding(e, "rho.com", self._finding(
            checked_at=datetime.now(timezone.utc).isoformat()))
        self.assertEqual(m.leads_needing_amazon(e, limit=3), [])

    def test_a_stale_finding_is_checked_again(self):
        """Seller counts move daily, so an old finding must not be sent as news."""
        e = self._e()
        m.record_leads(e, [{"domain": "rho.com", "brand": "Rho", "tier": "A"}])
        old = (datetime.now(timezone.utc) - timedelta(days=40)).isoformat()
        m.update_amazon_finding(e, "rho.com", self._finding(checked_at=old))
        self.assertEqual([l["domain"] for l in m.leads_needing_amazon(e, limit=3, max_age_days=7)],
                         ["rho.com"])


class FactsReachTheClayFileTests(unittest.TestCase):
    """The scan writes findings to our records; the file we hand Clay is built
    from those records. Twice now the two halves have been built and not joined,
    and both times everything looked fine because the columns existed and were
    simply empty.
    """

    def _e(self):
        return create_engine("sqlite://", future=True)

    def _amazon(self):
        return {"reason": "There are a handful of other sellers on your NAD+ listing. All authorized?",
                "confidence": "high", "marketplace": "amazon.com",
                "checked_at": "2026-07-27T15:54:00+00:00", "skipped_reason": "",
                "findings": {"absent": False, "sponsored_competitors": ["Cata-Kor", "Toniiq"],
                             "listings": [{"title": "Rho Nutrition Liposomal NAD+",
                                           "brand_price": 50.18, "cheapest": 48.0,
                                           "sellers_unknown": 18}]}}

    def _stored_lead(self):
        e = self._e()
        m.record_leads(e, [{"domain": "rho.com", "brand": "Rho", "tier": "A", "score": 16}])
        m.update_amazon_finding(e, "rho.com", self._amazon())
        return m.load_leads(e)[0]

    def test_a_scanned_finding_survives_being_stored_and_read_back(self):
        lead = self._stored_lead()
        self.assertEqual(lead.get("amz_situation"), "undercut")
        self.assertEqual(lead.get("amz_sellers_band"), "a lot of other sellers")

    def test_the_facts_actually_land_in_the_csv_clay_imports(self):
        """The end of the chain. Empty columns here mean Clay has nothing to
        write an opening line from and every lead gets the fallback."""
        import csv as _csv
        import io as _io

        import outbound_pipeline as op
        row = list(_csv.DictReader(_io.StringIO(op.leads_to_csv([self._stored_lead()]))))[0]
        self.assertEqual(row["amz_situation"], "undercut")
        self.assertEqual(row["amz_product"], "Rho Nutrition Liposomal NAD+")
        self.assertTrue(row["amz_undercut"])

    def test_no_figure_survives_the_round_trip_either(self):
        """Bucketing is worthless if a raw number sneaks back in on the way out."""
        import csv as _csv
        import io as _io

        import outbound_pipeline as op
        row = list(_csv.DictReader(_io.StringIO(op.leads_to_csv([self._stored_lead()]))))[0]
        for key, value in row.items():
            if key.startswith("amz_") and key != "amz_marketplace":
                self.assertFalse(any(ch.isdigit() for ch in str(value)),
                                 f"{key} leaked a figure into the Clay file: {value!r}")

    def test_a_lead_never_scanned_carries_no_facts(self):
        e = self._e()
        m.record_leads(e, [{"domain": "new.com", "brand": "New", "tier": "A"}])
        self.assertFalse(m.load_leads(e)[0].get("amz_situation"))


class ScannedExportOnlyShipsSendableBrandsTests(unittest.TestCase):
    """What goes to Clay must be sendable under THIS campaign."""

    def _e(self):
        return create_engine("sqlite://", future=True)

    def test_revenue_survives_the_round_trip(self):
        """Stored leads carry revenue_cents, freshly pulled ones carry the
        StoreLeads name. Reading only one exported every scanned brand at $0."""
        import csv as _csv
        import io as _io

        import outbound_pipeline as op
        e = self._e()
        store = {"name": "rho.com", "merchant_name": "Rho", "platform": "shopify",
                 "country_code": "US", "estimated_sales_yearly": 838176000,
                 "categories": "Health", "tags": "",
                 "contact_info": [{"type": "email", "value": "a@b.com"}], "apps": []}
        m.record_leads(e, [op.to_clay_lead(store)])
        row = list(_csv.DictReader(_io.StringIO(op.leads_to_csv([m.load_leads(e)[0]]))))[0]
        self.assertEqual(row["revenue_usd"], "8381760",
                         "a scanned brand exported at $0 makes every lead look tiny")
