"""Tests for the StoreLeads pull recipes (what we pull, and when)."""

from __future__ import annotations

import unittest
from datetime import datetime, timezone

import outbound_recipes as rc
import outbound_pipeline as op

# A Wednesday, so both trigger and baseline recipes are in play.
WED = datetime(2026, 7, 29, 12, 0, tzinfo=timezone.utc)
SAT = datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)
THU = datetime(2026, 7, 30, 12, 0, tzinfo=timezone.utc)


class BaseFilterTests(unittest.TestCase):
    def test_every_recipe_carries_the_icp_floor(self):
        for r in rc.RECIPES:
            p = r.params(WED)
            self.assertEqual(p["f:p"], "shopify", r.key)
            self.assertEqual(p["f:cc"], "US,GB,CA,AU", r.key)
            self.assertEqual(p["f:it"], "email", r.key)
            self.assertEqual(p["f:ds"], "Active", r.key)
            self.assertEqual(p["f:tags:op"], "not", r.key)

    def test_revenue_band_is_monthly_cents(self):
        p = rc.recipe("icp_baseline").params(WED)
        # $1M/yr and $15M/yr expressed as monthly cents
        self.assertEqual(p["f:ermin"], 1_000_000_00 // 12)
        self.assertEqual(p["f:ermax"], 15_000_000_00 // 12)

    def test_dropshippers_excluded(self):
        p = rc.recipe("icp_baseline").params(WED)
        self.assertIn("Dropshipper", p["f:tags"])
        self.assertIn("Print on Demand", p["f:tags"])


class TriggerWindowTests(unittest.TestCase):
    def test_new_growth_app_uses_14_day_window(self):
        p = rc.recipe("new_growth_app").params(WED)
        self.assertTrue(p["f:app_installed_at:min"].startswith("2026-07-15"))
        self.assertIn("shopify.triplewhale-1", p["f:an"])
        self.assertEqual(p["f:an:op"], "or")

    def test_churned_tool_uses_30_day_window(self):
        p = rc.recipe("churned_tool").params(WED)
        self.assertTrue(p["f:app_uninstalled_at:min"].startswith("2026-06-29"))

    def test_plan_upgrade_targets_plus_in_60_days(self):
        p = rc.recipe("plan_upgrade").params(WED)
        self.assertEqual(p["f:plan"], "Shopify Plus")
        self.assertTrue(p["f:last_plan_change_at:min"].startswith("2026-05-30"))

    def test_replatformed_uses_90_day_window(self):
        p = rc.recipe("replatformed").params(WED)
        self.assertTrue(p["f:last_platform_change_at:min"].startswith("2026-04-30"))

    def test_social_surge_uses_percentage_growth(self):
        p = rc.recipe("social_surge").params(WED)
        self.assertEqual(p["f:tiktokfollowers30dpmin"], 25)

    def test_baseline_has_no_time_window(self):
        p = rc.recipe("icp_baseline").params(WED)
        self.assertFalse([k for k in p if k.endswith(":min")])

    def test_timestamps_are_storeleads_json_format(self):
        p = rc.recipe("new_growth_app").params(WED)
        self.assertRegex(p["f:app_installed_at:min"], r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.000Z$")


class CadenceTests(unittest.TestCase):
    def test_weekends_pull_nothing(self):
        self.assertEqual(rc.recipes_for_day(SAT.weekday()), [])

    def test_tuesday_and_wednesday_run_triggers(self):
        keys = [r.key for r in rc.recipes_for_day(WED.weekday())]
        self.assertIn("new_growth_app", keys)
        self.assertIn("icp_baseline", keys)

    def test_other_weekdays_run_baseline_only(self):
        keys = [r.key for r in rc.recipes_for_day(THU.weekday())]
        self.assertEqual(keys, ["icp_baseline"])

    def test_daily_plan_totals_the_days_recipes(self):
        plan = rc.daily_plan(WED)
        self.assertEqual(plan["weekday"], "Wednesday")
        self.assertEqual(plan["planned_total"], sum(r["max_per_run"] for r in plan["recipes"]))
        self.assertGreater(plan["planned_total"], 0)

    def test_daily_plan_on_weekend_is_empty(self):
        self.assertEqual(rc.daily_plan(SAT)["planned_total"], 0)


class PipelineIntegrationTests(unittest.TestCase):
    """A recipe must reach StoreLeads as filters AND tag the leads it sources."""

    def _store(self, name):
        return {
            "name": name, "merchant_name": "Brand", "platform": "shopify",
            "country_code": "US", "estimated_sales_yearly": 5_000_000_00,
            "categories": "Beauty & Skincare", "tags": "",
            "contact_info": [{"type": "email", "value": "a@b.com"}], "apps": [],
        }

    def test_recipe_filters_reach_the_api_call(self):
        seen = {}

        def fetch(api_key, *, page=0, page_size=50, extra_params=None, **kw):
            seen["params"] = extra_params
            return [self._store("a.com")] if page == 0 else []

        op.run_storeleads_to_clay(
            api_key="x", clay_webhook_url="", processed_domains=set(),
            max_new=5, throttle_seconds=0, recipe=rc.recipe("new_growth_app"),
            now=WED, fetch_page=fetch,
        )
        self.assertIn("f:app_installed_at:min", seen["params"])
        self.assertEqual(seen["params"]["f:p"], "shopify")

    def test_leads_are_tagged_with_recipe_and_reason(self):
        def fetch(api_key, *, page=0, page_size=50, extra_params=None, **kw):
            return [self._store("a.com")] if page == 0 else []

        r = op.run_storeleads_to_clay(
            api_key="x", clay_webhook_url="", processed_domains=set(),
            max_new=5, throttle_seconds=0, recipe=rc.recipe("churned_tool"),
            now=WED, fetch_page=fetch,
        )
        self.assertEqual(r.recipe, "churned_tool")
        lead = r.leads[0]
        self.assertEqual(lead["recipe"], "churned_tool")
        self.assertEqual(lead["reason"], rc.recipe("churned_tool").reason)
        self.assertEqual(lead["signals"][0], lead["reason"])

    def test_recipe_column_is_in_the_csv(self):
        self.assertIn("recipe", op.CLAY_CSV_COLUMNS)

    def test_no_recipe_still_works(self):
        def fetch(api_key, *, page=0, page_size=50, extra_params=None, **kw):
            self.assertIsNone(extra_params)
            return [self._store("a.com")] if page == 0 else []

        r = op.run_storeleads_to_clay(
            api_key="x", clay_webhook_url="", processed_domains=set(),
            max_new=5, throttle_seconds=0, fetch_page=fetch,
        )
        self.assertEqual(r.recipe, "")


if __name__ == "__main__":
    unittest.main()
