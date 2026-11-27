"""new_store_180d: filters, the rolling date, tag exclusion, and amz_off routing."""

from __future__ import annotations

import inspect
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from sqlalchemy import create_engine

import outbound_campaign_source as source
import outbound_campaigns as campaigns
import outbound_enrich as oe
import outbound_instantly_push as push
import outbound_pipeline as pipeline


NOW = datetime(2026, 11, 27, 20, 44, tzinfo=timezone.utc)


def _store(name: str, **over: object) -> dict:
    row = {
        "name": name,
        "merchant_name": str(name).split(".")[0].title(),
        "country_code": "US",
        "categories": "Beauty & Skincare",
        "estimated_sales_yearly": 5_000_000_00,
        "tags": [],
        "sales_channels": ["Etsy"],
    }
    row.update(over)
    return row


class NewStoreFilterTests(unittest.TestCase):
    def test_filters_are_the_approved_query_and_nothing_else(self):
        params = campaigns.new_store_180d_filters(NOW)
        self.assertEqual(params["f:p"], "shopify")
        self.assertEqual(params["f:cc"], "US")
        self.assertEqual(params["f:cc:op"], "or")
        self.assertEqual(params["f:ds"], "Active")
        self.assertEqual(params["f:it"], "email")
        self.assertEqual(params["f:ermin"], 1_000_000)
        self.assertEqual(params["f:ermax"], 166_666_666)
        self.assertEqual(params["f:ermin"], campaigns.MONTHLY_FLOOR_CENTS)
        self.assertEqual(params["f:ermax"], campaigns.MONTHLY_CEILING_CENTS)
        self.assertEqual(set(params), {
            "f:p", "f:cc", "f:cc:op", "f:ds", "f:it", "f:ermin", "f:ermax", "f:cratmin",
        })
        self.assertTrue(set(params) <= campaigns.CONFIRMED_FILTERS)
        self.assertNotIn("f:tags", params)
        self.assertNotIn("f:tags:op", params)
        self.assertNotIn("f:empcmin", params)
        self.assertNotIn("f:empcmax", params)
        self.assertNotIn("f:scs", params)

    def test_cratmin_is_midnight_utc_rolling_180_days(self):
        params = campaigns.new_store_180d_filters(NOW)
        expected = (
            datetime(2026, 11, 27, tzinfo=timezone.utc) - timedelta(days=180)
        ).strftime("%Y-%m-%dT00:00:00.000Z")
        self.assertEqual(expected, "2026-05-31T00:00:00.000Z")
        self.assertEqual(params["f:cratmin"], expected)
        later = campaigns.new_store_180d_filters(NOW + timedelta(days=3))
        self.assertNotEqual(later["f:cratmin"], params["f:cratmin"])
        self.assertTrue(later["f:cratmin"].endswith("T00:00:00.000Z"))
        moved = (
            datetime(2026, 11, 30, tzinfo=timezone.utc) - timedelta(days=campaigns.NEW_STORE_180D_DAYS)
        ).strftime("%Y-%m-%dT00:00:00.000Z")
        self.assertEqual(later["f:cratmin"], moved)
        body = inspect.getsource(campaigns.new_store_180d_filters)
        self.assertNotIn("2026-05-31", body)
        self.assertIn("timedelta", body)

    def test_cap_matches_the_seeded_amz_off_daily_cap(self):
        seeded = {
            row["recipe_key"]: row for row in campaigns.SEED_ROWS
        }
        self.assertEqual(campaigns.NEW_STORE_180D_MAX_PER_RUN, 40)
        self.assertEqual(
            campaigns.NEW_STORE_180D_MAX_PER_RUN,
            seeded["amz_off"]["daily_cap"],
        )
        self.assertNotIn("f:cratmin", seeded["amz_off"]["storeleads_filters"])
        for key in ("amz_on", "plan_upgrade", "replatform"):
            self.assertNotIn("f:cratmin", seeded[key]["storeleads_filters"])
        self.assertEqual(seeded["amz_on"]["daily_cap"], 40)
        self.assertEqual(seeded["plan_upgrade"]["daily_cap"], 30)
        self.assertEqual(seeded["replatform"]["daily_cap"], 25)
        self.assertNotIn(campaigns.NEW_STORE_180D_KEY, campaigns.STANDARD_REVENUE_RECIPES)
        self.assertNotIn(campaigns.NEW_STORE_180D_KEY, push.RECIPE_CAMPAIGN_MAP)
        self.assertEqual(campaigns.source_aliases("amz_off"), (campaigns.NEW_STORE_180D_KEY,))
        self.assertEqual(campaigns.source_aliases("amz_on"), ())
        self.assertEqual(campaigns.source_aliases("plan_upgrade"), ())
        self.assertEqual(campaigns.source_aliases("replatform"), ())


class NewStoreExclusionTests(unittest.TestCase):
    def test_dropshipper_and_print_on_demand_tags_are_excluded(self):
        self.assertEqual(
            campaigns.new_store_180d_skip_reason(
                {"tags": ["Dropshipper"], "sales_channels": ["Etsy"]},
            ),
            "tagged Dropshipper",
        )
        self.assertEqual(
            campaigns.new_store_180d_skip_reason(
                {"tags": "Print on Demand, Beauty", "sales_channels": ["Etsy"]},
            ),
            "tagged Print on Demand",
        )
        self.assertEqual(
            campaigns.new_store_180d_skip_reason(
                {"tags": [{"name": "Print on Demand"}], "sales_channels": ["Etsy"]},
            ),
            "tagged Print on Demand",
        )
        self.assertEqual(
            campaigns.new_store_180d_skip_reason(
                {"tags": ["Beauty"], "sales_channels": ["Etsy"]},
            ),
            "",
        )

    def test_amazon_stores_do_not_qualify_and_a_missing_channel_does_not_either(self):
        self.assertIn(
            "not an off-Amazon",
            campaigns.new_store_180d_skip_reason(
                {"tags": [], "sales_channels": ["Amazon", "Etsy"]},
            ),
        )
        self.assertIn(
            "not an off-Amazon",
            campaigns.new_store_180d_skip_reason({"tags": [], "sales_channels": []}),
        )
        self.assertIn(
            "not an off-Amazon",
            campaigns.new_store_180d_skip_reason({"tags": []}),
        )
        self.assertEqual(
            campaigns.new_store_180d_skip_reason(
                {"tags": [], "sales_channels": ["Etsy"]},
            ),
            "",
        )

    def test_digital_product_gate_is_a_hook_until_it_lands(self):
        self.assertFalse(hasattr(pipeline, "digital_product_reason"))
        self.assertEqual(campaigns.digital_product_block_reason({"name": "course.com"}), "")
        kept = {"name": "goods.com", "tags": [], "sales_channels": ["Etsy"]}
        self.assertEqual(campaigns.new_store_180d_skip_reason(kept), "")

        def checker(store: dict) -> str:
            if store.get("name") == "course.com":
                return "digital catalog"
            return ""

        pipeline.digital_product_reason = checker
        try:
            self.assertEqual(
                campaigns.digital_product_block_reason({"name": "course.com"}),
                "digital catalog",
            )
            self.assertEqual(
                campaigns.new_store_180d_skip_reason(
                    {"name": "course.com", "tags": [], "sales_channels": ["Etsy"]},
                ),
                "digital catalog",
            )
            self.assertEqual(campaigns.new_store_180d_skip_reason(kept), "")
        finally:
            del pipeline.digital_product_reason


class NewStoreRoutingTests(unittest.TestCase):
    def test_off_amazon_evidence_routes_to_the_existing_amz_off_campaign(self):
        target, status, route = push.resolve_recipe_campaign(
            "new_store_180d", [push.ROUTE_AMZ_OFF],
        )
        self.assertEqual(status, "routed")
        self.assertEqual(route, "amz_off")
        self.assertEqual(target, push.RECIPE_CAMPAIGN_MAP["amz_off"])
        mapped, mapped_status, mapped_route = push.resolve_recipe_campaign(
            "new_store_180d",
            [push.ROUTE_AMZ_OFF],
            campaign_map={"amz_off": push.RECIPE_CAMPAIGN_MAP["amz_off"]},
        )
        self.assertEqual(mapped_status, "routed")
        self.assertEqual(mapped_route, "amz_off")
        self.assertEqual(mapped, push.RECIPE_CAMPAIGN_MAP["amz_off"])

    def test_an_amazon_channel_is_not_routed_to_amz_off(self):
        target, status, route = push.resolve_recipe_campaign(
            "new_store_180d", [push.ROUTE_AMZ_ON],
        )
        self.assertEqual((target, status, route), ("", "queued", ""))
        bare, bare_status, bare_route = push.resolve_recipe_campaign("new_store_180d", [])
        self.assertEqual((bare, bare_status, bare_route), ("", "queued", ""))
        blocked, blocked_status, _route = push.resolve_recipe_campaign(
            "new_store_180d",
            [push.ROUTE_AMZ_OFF],
            campaign_map={"amz_off": push.RECIPE_CAMPAIGN_MAP["amz_off"]},
            blocked=["amz_off"],
        )
        self.assertEqual(blocked, "")
        self.assertEqual(blocked_status, "blocked")

    def test_enrichment_counts_the_recipe_against_the_amz_off_cap(self):
        off = oe._recipe_names({"recipe_key": "amz_off", "recipe_aliases": ""})
        self.assertIn("amz_off", off)
        self.assertIn("new_store_180d", off)
        for key, aliases in (
            ("amz_on", ""),
            ("plan_upgrade", ""),
            ("replatform", "replatformed"),
        ):
            names = oe._recipe_names({"recipe_key": key, "recipe_aliases": aliases})
            self.assertNotIn("new_store_180d", names)

    def test_pull_keeps_forty_off_amazon_stores_and_drops_the_rest(self):
        page = [
            _store("seen.com"),
            _store("drop.com", tags=["Dropshipper"]),
            _store("pod.com", tags="Print on Demand"),
            _store("amz.com", sales_channels=["Amazon"]),
            _store("unknown.com", sales_channels=[]),
            _store("silent.com", sales_channels=None),
        ]
        page.extend(_store(f"n{index}.com") for index in range(45))
        calls = []

        def fetch(api_key, **kwargs):
            calls.append(kwargs["extra_params"])
            return SimpleNamespace(domains=page, next_cursor="more", error="")

        seen = {"seen.com"}
        recorded = {}

        def record(leads, recipe_key):
            recorded["recipe"] = recipe_key
            recorded["domains"] = [lead["domain"] for lead in leads]
            return len(leads)

        out = source.pull_new_store_180d(
            api_key="storeleads-key",
            now=NOW,
            seen=seen,
            want=campaigns.NEW_STORE_180D_MAX_PER_RUN,
            fetch=fetch,
            record=record,
        )
        domains = {lead["domain"] for lead in out["leads"]}
        self.assertEqual(len(out["leads"]), 40)
        self.assertEqual(recorded["recipe"], "new_store_180d")
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["f:cratmin"], "2026-05-31T00:00:00.000Z")
        self.assertNotIn("f:tags", calls[0])
        for blocked in ("seen.com", "drop.com", "pod.com", "amz.com", "unknown.com", "silent.com"):
            self.assertNotIn(blocked, domains)
        self.assertGreaterEqual(out["skipped_seen"], 1)
        self.assertGreaterEqual(out["skipped_unfit"], 4)
        for lead in out["leads"]:
            self.assertEqual(lead["recipe"], "new_store_180d")
            self.assertIn(push.ROUTE_AMZ_OFF, lead["signals"])
            self.assertNotIn(push.ROUTE_AMZ_ON, lead["signals"])
            target, status, route = push.resolve_recipe_campaign(lead["recipe"], lead["signals"])
            self.assertEqual(status, "routed")
            self.assertEqual(route, "amz_off")
            self.assertEqual(target, push.RECIPE_CAMPAIGN_MAP["amz_off"])

    def test_morning_source_uses_amz_off_allowance_and_leaves_the_other_queries(self):
        page = [
            _store("seen.com"),
            _store("drop.com", tags=["Dropshipper"]),
            _store("pod.com", tags=["Print on Demand"]),
            _store("amz.com", sales_channels=["Amazon"]),
            _store("unknown.com", sales_channels=[]),
        ]
        page.extend(_store(f"fresh{index}.com") for index in range(3))
        calls = []

        def fetch(api_key, **kwargs):
            filters = dict(kwargs["extra_params"])
            calls.append(filters)
            if filters.get("f:cratmin"):
                return SimpleNamespace(domains=page, next_cursor=None, error="")
            return SimpleNamespace(domains=[], next_cursor=None, error="")

        engine = create_engine("sqlite://")
        out = source.source_active_campaigns(
            engine,
            api_key="storeleads-key",
            now=NOW,
            contacted={"seen.com"},
            fetch=fetch,
            company_room=300,
        )
        leads = {lead["domain"]: lead for lead in out["leads"]}
        self.assertEqual(set(leads), {"fresh0.com", "fresh1.com", "fresh2.com"})
        for lead in leads.values():
            self.assertEqual(lead["recipe"], "new_store_180d")
            target, status, route = push.resolve_recipe_campaign(lead["recipe"], lead["signals"])
            self.assertEqual((status, route, target), (
                "routed", "amz_off", push.RECIPE_CAMPAIGN_MAP["amz_off"],
            ))
        self.assertIn("new_store_180d", out["recipes"])
        rows = {row["recipe_key"]: row for row in campaigns.list_campaigns(engine)}
        self.assertNotIn("new_store_180d", rows)
        self.assertEqual(rows["amz_off"]["daily_cap"], 40)
        self.assertEqual(rows["amz_on"]["daily_cap"], 40)
        self.assertEqual(rows["plan_upgrade"]["daily_cap"], 30)
        self.assertEqual(rows["replatform"]["daily_cap"], 25)
        self.assertEqual(rows["amz_off"]["campaign_id"], push.RECIPE_CAMPAIGN_MAP["amz_off"])
        new_calls = [call for call in calls if call.get("f:cratmin")]
        self.assertEqual(len(new_calls), 1)
        others = [call for call in calls if "f:cratmin" not in call]
        self.assertTrue(any(call.get("f:scs:op") == "not" for call in others))
        self.assertTrue(any(
            call.get("f:scs") == "Amazon" and call.get("f:scs:op") != "not"
            and "f:plan" not in call and "f:last_platform" not in call
            for call in others
        ))
        self.assertTrue(any(call.get("f:plan") == "Shopify Plus" and "f:cratmin" not in call for call in others))
        self.assertTrue(any(call.get("f:last_platform") and "f:cratmin" not in call for call in others))
        summary = next(item for item in out["campaigns"] if item["recipe_key"] == "amz_off")
        self.assertEqual(summary["new_store_180d"], 3)
        self.assertEqual(summary["recorded"], 3)
        self.assertLessEqual(summary["new_store_180d"], campaigns.NEW_STORE_180D_MAX_PER_RUN)


if __name__ == "__main__":
    unittest.main()
