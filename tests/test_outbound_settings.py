"""Tests for tunable settings, the change log, and the config version.

The version is what makes results attributable: a date and a note alone cannot
tell you whether a week's results came from the old settings or the new ones.
"""

from __future__ import annotations

import unittest
from datetime import datetime, timezone

from sqlalchemy import create_engine

import outbound_recipes as rc
from sales_support_agent.services import outbound_settings as st

WED = datetime(2026, 7, 29, 12, 0, tzinfo=timezone.utc)


class SettingsStoreTests(unittest.TestCase):
    def _e(self):
        return create_engine("sqlite://", future=True)

    def test_starts_empty_at_version_zero(self):
        e = self._e()
        self.assertEqual(st.load_settings(e), {})
        self.assertEqual(st.config_version(e), 0)

    def test_change_saves_and_bumps_version(self):
        e = self._e()
        r = st.apply_changes(e, {"new_growth_app.window_days": 30}, note="widen it", changed_by="d@a.com")
        self.assertTrue(r["ok"])
        self.assertEqual(r["changed"], 1)
        self.assertEqual(r["version"], 1)
        self.assertEqual(st.load_settings(e)["new_growth_app.window_days"], "30")

    def test_no_op_change_does_not_bump_version(self):
        e = self._e()
        st.apply_changes(e, {"new_growth_app.window_days": 30}, note="first")
        r = st.apply_changes(e, {"new_growth_app.window_days": 30}, note="again")
        self.assertEqual(r["changed"], 0)
        self.assertEqual(st.config_version(e), 1)

    def test_change_log_records_old_new_note_and_who(self):
        e = self._e()
        st.apply_changes(e, {"new_growth_app.window_days": 30},
                         note="lift the yield", changed_by="david@anatainc.com")
        c = st.load_changes(e)[0]
        self.assertEqual(c["key"], "new_growth_app.window_days")
        self.assertEqual(c["new_value"], "30")
        self.assertEqual(c["note"], "lift the yield")
        self.assertEqual(c["changed_by"], "david@anatainc.com")
        self.assertEqual(c["version"], 1)

    def test_effective_merges_overrides_onto_defaults(self):
        e = self._e()
        st.apply_changes(e, {"new_growth_app.window_days": 30})
        eff = st.effective(e, rc.DEFAULT_SETTINGS)
        self.assertEqual(eff["new_growth_app.window_days"], 30)      # coerced to int
        self.assertEqual(eff["replatformed.window_days"],
                         rc.DEFAULT_SETTINGS["replatformed.window_days"])

    def test_fails_open_without_a_database(self):
        self.assertEqual(st.load_settings(None), {})
        self.assertEqual(st.config_version(None), 0)
        self.assertEqual(st.load_changes(None), [])
        self.assertFalse(st.apply_changes(None, {"a": 1})["ok"])


class TunablesChangeBehaviourTests(unittest.TestCase):
    """A setting is only real if it actually changes what we pull."""

    def test_window_change_moves_the_install_cutoff(self):
        r = rc.recipe("new_growth_app")
        store = {"apps": [{"platform": "shopify", "token": "triplewhale-1",
                           "installed_at": "2026-07-05T00:00:00Z"}]}   # 24 days before WED
        self.assertFalse(r.keeps(store, WED, {"new_growth_app.window_days": 14}))
        self.assertTrue(r.keeps(store, WED, {"new_growth_app.window_days": 30}))

    def test_churn_tools_per_day_widens_coverage(self):
        one = rc.churn_tokens_for(WED, {"churned_tool.tools_per_day": 1})
        two = rc.churn_tokens_for(WED, {"churned_tool.tools_per_day": 2})
        self.assertEqual(len(one), 1)
        self.assertEqual(len(two), 2)
        self.assertEqual(len(set(two)), 2)

    def test_window_settings_reach_the_query(self):
        p = rc.recipe("replatformed").params(WED, {"replatformed.window_days": 10})
        self.assertTrue(p["f:last_platform_change_at:min"].startswith("2026-07-19"))

    def test_growth_percent_setting_reaches_the_query(self):
        p = rc.recipe("social_surge").params(WED, {"social_surge.min_growth_pct": 60})
        self.assertEqual(p["f:tiktokfollowers30dpmin"], 60)

    def test_cap_override(self):
        self.assertEqual(rc.recipe("icp_baseline").cap({"icp_baseline.max_per_run": 99}), 99)

    def test_disabling_a_recipe_removes_it_from_the_day(self):
        keys = [r.key for r in rc.recipes_for_day(WED.weekday(),
                                                  {"new_growth_app.enabled": "false"})]
        self.assertNotIn("new_growth_app", keys)

    def test_reason_follows_the_tuned_window(self):
        """The reason reaches the prospect, so it must never claim a window we
        did not actually use."""
        r = rc.recipe("new_growth_app")
        self.assertIn("14 days", r.reason_for(None))
        self.assertIn("30 days", r.reason_for({"new_growth_app.window_days": 30}))
        self.assertNotIn("two weeks", r.reason_for({"new_growth_app.window_days": 30}))

    def test_reason_without_a_window_is_unchanged(self):
        self.assertEqual(rc.recipe("icp_baseline").reason_for({}),
                         rc.recipe("icp_baseline").reason)

    def test_tagged_lead_uses_the_tuned_reason(self):
        import outbound_pipeline as op
        store = {"name": "a.com", "merchant_name": "B", "platform": "shopify",
                 "country_code": "US", "estimated_sales_yearly": 5_000_000_00,
                 "categories": "Beauty & Skincare", "tags": "",
                 "contact_info": [{"type": "email", "value": "a@b.com"}],
                 "apps": [{"platform": "shopify", "token": "triplewhale-1",
                           "installed_at": "2026-07-05T00:00:00Z"}]}

        def fetch(api_key, *, page=0, page_size=50, extra_params=None, **kw):
            return [store] if page == 0 else []

        res = op.run_storeleads_to_clay(
            api_key="x", clay_webhook_url="", processed_domains=set(), max_new=5,
            throttle_seconds=0, recipe=rc.recipe("new_growth_app"), now=WED,
            settings={"new_growth_app.window_days": 30}, fetch_page=fetch,
        )
        self.assertEqual(len(res.leads), 1)
        self.assertIn("30 days", res.leads[0]["reason"])

    def test_defaults_still_apply_when_settings_absent(self):
        self.assertEqual(rc.recipe("icp_baseline").cap(None), 25)
        self.assertTrue(rc.recipe("icp_baseline").enabled(None))


class IcpBandTunableTests(unittest.TestCase):
    """The revenue band is the ICP lever David most needs to move, and a change
    must take effect on the very next pull, both server-side and in our own gate."""

    def test_band_reaches_the_storeleads_query_as_monthly_cents(self):
        p = rc.recipe("icp_baseline").params(
            WED, {"icp.revenue_min_usd": 2_000_000, "icp.revenue_max_usd": 10_000_000})
        self.assertEqual(p["f:ermin"], 2_000_000 * 100 // 12)
        self.assertEqual(p["f:ermax"], 10_000_000 * 100 // 12)

    def test_defaults_are_one_to_twenty_million(self):
        p = rc.recipe("icp_baseline").params(WED)
        self.assertEqual(p["f:ermin"], 1_000_000 * 100 // 12)
        self.assertEqual(p["f:ermax"], 20_000_000 * 100 // 12)

    def test_our_own_gate_honours_the_same_band(self):
        import outbound_pipeline as op

        def store(yearly_cents):
            return {"name": "a.com", "merchant_name": "B", "platform": "shopify",
                    "country_code": "US", "estimated_sales_yearly": yearly_cents,
                    "categories": "Beauty & Skincare", "tags": "",
                    "contact_info": [{"type": "email", "value": "a@b.com"}]}

        tight = {"icp.revenue_min_usd": 2_000_000, "icp.revenue_max_usd": 10_000_000}
        self.assertTrue(op.store_matches_icp(store(835_227_012), tight))    # $8.35M in
        self.assertFalse(op.store_matches_icp(store(1_337_650_920), tight))  # $13.4M out
        self.assertFalse(op.store_matches_icp(store(150_000_000), tight))    # $1.5M out

    def test_a_bad_setting_falls_back_instead_of_letting_everything_through(self):
        import outbound_pipeline as op
        store = {"name": "a.com", "merchant_name": "B", "platform": "shopify",
                 "country_code": "US", "estimated_sales_yearly": 50_000_000_00,
                 "categories": "Beauty & Skincare", "tags": "",
                 "contact_info": [{"type": "email", "value": "a@b.com"}]}
        # $50M with a nonsense setting must still be rejected by the default ceiling
        self.assertFalse(op.store_matches_icp(store, {"icp.revenue_max_usd": "abc"}))

    def test_band_is_listed_as_editable_in_the_app(self):
        self.assertIn("icp.revenue_min_usd", rc.TUNABLE_LABELS)
        self.assertIn("icp.revenue_max_usd", rc.TUNABLE_LABELS)


if __name__ == "__main__":
    unittest.main()
