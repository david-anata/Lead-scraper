"""Tests for the StoreLeads pull recipes (what we pull, and when)."""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

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
            self.assertEqual(p["f:ds"], "Active", r.key)
            self.assertEqual(p["f:tags:op"], "not", r.key)
            self.assertIn("f:ermin", p, r.key)
            self.assertIn("f:ermax", p, r.key)
            # Churn omits the contact and headcount filters on purpose: stacking
            # them on the single-app uninstall query returned an empty 200.
            # store_matches_icp still requires an email and the employee band.
            if r.key == "churned_tool":
                self.assertNotIn("f:it", p)
                self.assertNotIn("f:empcmin", p)
                self.assertNotIn("f:empcmax", p)
            else:
                self.assertEqual(p["f:it"], "email", r.key)
                self.assertIn("f:empcmin", p, r.key)
                self.assertIn("f:empcmax", p, r.key)

    def test_revenue_band_is_monthly_cents(self):
        """We think in dollars a year; StoreLeads filters on monthly cents."""
        p = rc.recipe("icp_baseline").params(WED)
        self.assertEqual(p["f:ermin"], 1_000_000 * 100 // 12)    # $1M/yr
        self.assertEqual(p["f:ermax"], 20_000_000 * 100 // 12)   # $20M/yr

    def test_there_is_only_one_definition_of_the_band(self):
        """A second copy of the band is how a retune silently fails to apply."""
        self.assertFalse(hasattr(rc, "BASE_FILTERS") and rc.BASE_FILTERS)
        self.assertFalse(hasattr(rc, "MONTHLY_MAX_CENTS"))

    def test_dropshippers_excluded(self):
        p = rc.recipe("icp_baseline").params(WED)
        self.assertIn("Dropshipper", p["f:tags"])
        self.assertIn("Print on Demand", p["f:tags"])


class TriggerWindowTests(unittest.TestCase):
    def test_new_growth_app_puts_the_install_window_on_one_app(self):
        # f:app_installed_at accepts one app id. A comma list returns zero rows,
        # so each app is its own call and the date is a server filter.
        p = rc.recipe("new_growth_app").params(WED)
        self.assertIn(p["f:app_installed_at"], rc.GROWTH_APP_TOKENS)
        self.assertNotIn(",", p["f:app_installed_at"])
        self.assertTrue(p["f:app_installed_at"].startswith("shopify."))
        self.assertTrue(p["f:app_installed_at:min"].startswith("2026-07-15"))
        self.assertNotIn("f:an", p)
        self.assertIsNotNone(rc.recipe("new_growth_app").client_filter)
        queried = [item["f:app_installed_at"] for item in rc.recipe("new_growth_app").param_sets(WED)]
        self.assertEqual(sorted(queried), sorted(rc.GROWTH_APP_TOKENS))
        self.assertEqual(len(queried), len(set(queried)))

    def test_churned_tool_uses_30_day_window(self):
        p = rc.recipe("churned_tool").params(WED)
        self.assertTrue(p["f:app_uninstalled_at:min"].startswith("2026-06-29"))
        self.assertTrue(p["f:app_uninstalled_at"].startswith("shopify."))
        sets = rc.recipe("churned_tool").param_sets(WED)
        self.assertEqual(len(sets), len(rc.GROWTH_APP_TOKENS))
        self.assertEqual(
            {item["f:app_uninstalled_at"] for item in sets},
            set(rc.GROWTH_APP_TOKENS),
        )

    def test_plan_upgrade_targets_plus_in_180_days(self):
        p = rc.recipe("plan_upgrade").params(WED)
        self.assertEqual(p["f:plan"], "Shopify Plus")
        self.assertTrue(p["f:last_plan_change_at:min"].startswith("2026-01-30"))
        self.assertEqual(p["sort"], rc.PLAN_UPGRADE_SORTS[WED.toordinal() % 3])
        self.assertEqual(len(rc.recipe("plan_upgrade").param_sets(WED)), 1)

    def test_plan_upgrade_sort_rotates_by_calendar_day(self):
        days = [datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(days=i) for i in range(3)]
        seen = [rc.recipe("plan_upgrade").params(day)["sort"] for day in days]
        self.assertEqual(seen, [rc.PLAN_UPGRADE_SORTS[day.toordinal() % 3] for day in days])
        self.assertEqual(set(seen), set(rc.PLAN_UPGRADE_SORTS))

    def test_replatformed_limits_the_previous_platform(self):
        p = rc.recipe("replatformed").params(WED)
        self.assertTrue(p["f:last_platform_change_at:min"].startswith("2026-01-30"))
        self.assertEqual(p["f:last_platform:op"], "or")
        for platform in ("woocommerce", "bigcommerce", "magento", "wix", "squarespace"):
            self.assertIn(platform, p["f:last_platform"])
        self.assertIn(p["sort"], rc.REPLATFORM_SORTS)

    def test_social_surge_does_not_send_the_undocumented_tiktok_filter(self):
        sets = rc.recipe("social_surge").param_sets(WED)
        filters = {key for item in sets for key in item if key.startswith("f:") and "follower" in key}
        self.assertNotIn("f:tiktokfollowers30dpmin", filters)
        self.assertIn("f:youtubefollowers30dpmin", filters)
        self.assertIn("f:pinterestfollowers30dpmin", filters)
        self.assertIn("f:twitterfollowers30dpmin", filters)
        self.assertEqual(sets[0]["sort"], "-fc30dp_10,rank")
        for item in sets:
            if "f:youtubefollowers30dpmin" in item:
                self.assertEqual(item["f:youtubefollowers30dpmin"], 25)
                self.assertEqual(item["sort"], "-fc30dp_8,rank")

    def test_baseline_has_no_time_window(self):
        p = rc.recipe("icp_baseline").params(WED)
        self.assertFalse([k for k in p if k.endswith(":min")])
        self.assertIn(p["sort"], rc.BASELINE_SORTS)

    def test_timestamps_are_storeleads_json_format(self):
        p = rc.recipe("churned_tool").params(WED)
        self.assertRegex(p["f:app_uninstalled_at:min"], r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.000Z$")
        growth = rc.recipe("new_growth_app").params(WED)
        self.assertRegex(growth["f:app_installed_at:min"], r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.000Z$")


class CadenceTests(unittest.TestCase):
    def test_weekends_pull_nothing(self):
        self.assertEqual(rc.recipes_for_day(SAT.weekday()), [])

    def test_triggers_are_spread_across_weekdays(self):
        # Monday is refresh day: baseline only. plan_upgrade runs once, so
        # Wednesday does not repeat Tuesday's identical pool.
        by_day = {day: {r.key for r in rc.recipes_for_day(day)} for day in range(5)}
        self.assertEqual(by_day[0], {"icp_baseline"})
        self.assertIn("new_growth_app", by_day[1])
        self.assertIn("new_growth_app", by_day[3])
        self.assertNotIn("plan_upgrade", by_day[1])
        self.assertIn("plan_upgrade", by_day[2])
        self.assertIn("icp_baseline", by_day[WED.weekday()])
        self.assertIn("social_surge", by_day[1])
        self.assertNotEqual(by_day[1], by_day[2])
        covered = set().union(*by_day.values())
        self.assertEqual(covered, {r.key for r in rc.RECIPES})

    def test_thursday_is_not_baseline_only(self):
        keys = {r.key for r in rc.recipes_for_day(THU.weekday())}
        self.assertIn("icp_baseline", keys)
        self.assertIn("new_growth_app", keys)

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
        self.assertIn("f:app_installed_at", seen["params"])
        self.assertNotIn(",", seen["params"]["f:app_installed_at"])
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
        self.assertEqual(lead["reason"], rc.recipe("churned_tool").reason_for(None))
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

    def test_zero_scan_logs_storeleads_status_and_error(self):
        def fetch_result(api_key, **_kwargs):
            return op.StoreLeadsPage([], None, status=422, error="invalid filter")

        with self.assertLogs("outbound_pipeline", level="WARNING") as logs:
            result = op.run_recipe_pull(
                api_key="x", clay_webhook_url="", processed_domains=set(),
                max_new=5, throttle_seconds=0, recipe=rc.recipe("plan_upgrade"),
                now=WED, fetch_result=fetch_result,
            )
        self.assertEqual(result.scanned, 0)
        self.assertTrue(any(
            "recipe=plan_upgrade scanned 0 status=422 error=invalid filter" in line
            for line in logs.output
        ))


class ClientFilterTests(unittest.TestCase):
    """App install/uninstall filters take ONE app id. A comma list returns zero
    rows, so each app is its own call and the row is re-checked here."""

    def _store_with_app(self, token, installed_at):
        return {
            "name": "a.com", "merchant_name": "B", "platform": "shopify",
            "country_code": "US", "estimated_sales_yearly": 5_000_000_00,
            "categories": "Beauty & Skincare", "tags": "",
            "contact_info": [{"type": "email", "value": "a@b.com"}],
            "apps": [{"platform": "shopify", "token": token, "installed_at": installed_at}],
        }

    def test_no_app_list_is_ever_sent_to_the_install_filter(self):
        for params in rc.recipe("new_growth_app").param_sets(WED):
            self.assertNotIn(",", params["f:app_installed_at"])
            self.assertTrue(params["f:app_installed_at"].startswith("shopify."))

    def test_recent_install_is_kept(self):
        r = rc.recipe("new_growth_app")
        s = self._store_with_app("triplewhale-1", "2026-07-20T00:00:00Z")  # 9 days before WED
        self.assertTrue(r.keeps(s, WED))

    def test_old_install_is_dropped(self):
        r = rc.recipe("new_growth_app")
        s = self._store_with_app("triplewhale-1", "2026-01-05T00:00:00Z")
        self.assertFalse(r.keeps(s, WED))

    def test_unrelated_app_is_dropped(self):
        r = rc.recipe("new_growth_app")
        s = self._store_with_app("some-random-app", "2026-07-20T00:00:00Z")
        self.assertFalse(r.keeps(s, WED))

    def test_bad_date_does_not_crash(self):
        r = rc.recipe("new_growth_app")
        s = self._store_with_app("triplewhale-1", "not-a-date")
        self.assertFalse(r.keeps(s, WED))

    def test_recipes_without_a_filter_keep_everything(self):
        self.assertTrue(rc.recipe("icp_baseline").keeps({}, WED))

    def test_churn_sends_a_single_app_id_and_rotates(self):
        p = rc.recipe("churned_tool").params(WED)
        self.assertNotIn(",", p["f:app_uninstalled_at"])
        other = rc.recipe("churned_tool").params(
            datetime(2026, 7, 30, tzinfo=timezone.utc))
        self.assertNotEqual(p["f:app_uninstalled_at"], other["f:app_uninstalled_at"])

    def test_recent_removal_is_kept_and_an_old_one_is_dropped(self):
        recipe = rc.recipe("churned_tool")
        token = recipe.params(WED)["f:app_uninstalled_at"].split(".", 1)[1]
        recent = self._store_with_app(token, "2026-01-01T00:00:00Z")
        recent["apps"][0]["removed_at"] = "2026-07-20T00:00:00.585839251Z"
        self.assertTrue(recipe.keeps(recent, WED))
        old = self._store_with_app(token, "2026-01-01T00:00:00Z")
        old["apps"][0]["removed_at"] = "2026-01-05T00:00:00Z"
        self.assertFalse(recipe.keeps(old, WED))

    def test_social_growth_below_the_threshold_is_dropped(self):
        recipe = rc.recipe("social_surge")
        flat = {"contact_info": [{"type": "tiktok", "followers": 1000, "followers_30d": 10}]}
        surging = {"contact_info": [{"type": "youtube", "followers": 1000, "followers_30d": 400}]}
        self.assertFalse(recipe.keeps(flat, WED))
        self.assertTrue(recipe.keeps(surging, WED))
        self.assertTrue(recipe.keeps({"contact_info": [{"type": "email", "value": "a@b.com"}]}, WED))

    def test_client_filter_runs_in_the_pipeline(self):
        old = self._store_with_app("triplewhale-1", "2026-01-05T00:00:00Z")
        new = self._store_with_app("triplewhale-1", "2026-07-20T00:00:00Z")
        new["name"] = "fresh.com"

        def fetch(api_key, *, page=0, page_size=50, extra_params=None, **kw):
            return [old, new] if page == 0 else []

        r = op.run_storeleads_to_clay(
            api_key="x", clay_webhook_url="", processed_domains=set(),
            max_new=10, throttle_seconds=0, recipe=rc.recipe("new_growth_app"),
            now=WED, fetch_page=fetch,
        )
        self.assertEqual(r.scanned, 2)
        self.assertEqual([l["domain"] for l in r.leads], ["fresh.com"])


if __name__ == "__main__":
    unittest.main()
