"""Tests for the never-email-twice outbound memory (SQLite-backed here)."""

from __future__ import annotations

import unittest

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


if __name__ == "__main__":
    unittest.main()
