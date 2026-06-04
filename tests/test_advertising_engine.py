"""Pure-logic tests for the advertising audit engine (no DB)."""

from __future__ import annotations

import unittest

from sales_support_agent.services.advertising import engine as E
from sales_support_agent.services.advertising.schema import (
    AdRow,
    ExternalCostRow,
    Goals,
    SalesRow,
    acos_bps,
    parse_bps,
    parse_cents,
    parse_int,
)


def _kw(text, *, clicks, spend, sales, orders, bid=None, ad_type="SP"):
    return AdRow(ad_type=ad_type, entity_level="keyword", campaign_name="Camp", ad_group_name="AG",
                entity_text=text, match_type="exact", impressions=clicks * 25, clicks=clicks,
                spend_cents=spend, sales_cents=sales, orders=orders, units=orders, bid_cents=bid)


def _st(text, *, clicks, spend, sales, orders, ad_type="SP"):
    return AdRow(ad_type=ad_type, entity_level="search_term", campaign_name="Camp", ad_group_name="AG",
                entity_text=text, impressions=clicks * 20, clicks=clicks, spend_cents=spend,
                sales_cents=sales, orders=orders, units=orders)


class NumericHelpersTest(unittest.TestCase):
    def test_parse_cents(self):
        self.assertEqual(parse_cents("$1,234.56"), 123456)
        self.assertEqual(parse_cents("(12.00)"), -1200)
        self.assertEqual(parse_cents(""), 0)
        self.assertEqual(parse_cents("junk"), 0)
        self.assertEqual(parse_cents(5), 500)

    def test_parse_int(self):
        self.assertEqual(parse_int("1,000"), 1000)
        self.assertEqual(parse_int("3.0"), 3)
        self.assertEqual(parse_int(""), 0)

    def test_parse_bps(self):
        self.assertEqual(parse_bps("27.5%"), 2750)
        self.assertEqual(parse_bps("27.5"), 2750)
        self.assertEqual(parse_bps("0.275"), 2750)  # fractional form
        self.assertIsNone(parse_bps(""))

    def test_acos_bps(self):
        self.assertEqual(acos_bps(4000, 2000), 20000)  # 200%
        self.assertIsNone(acos_bps(4000, 0))           # undefined with no sales


class SummaryTest(unittest.TestCase):
    def test_per_ad_type_dominant_totals(self):
        # SP at keyword level + STV at campaign level + a redundant search_term.
        rows = [
            _kw("blue", clicks=40, spend=4000, sales=2000, orders=2, bid=120),
            _st("blue search", clicks=10, spend=9999, sales=0, orders=0),  # must NOT be summed
            AdRow(ad_type="STV", entity_level="campaign", campaign_name="TV",
                  impressions=9000, clicks=10, spend_cents=20000, sales_cents=5000, orders=1),
        ]
        sales = [SalesRow(asin="B1", sessions=900, units=22, ordered_product_sales_cents=32000)]
        s = E.compute_summary(rows, sales, [], Goals())
        self.assertEqual(s["ad_spend_cents"], 24000)   # 4000 SP keyword + 20000 STV
        self.assertEqual(s["ad_sales_cents"], 7000)
        self.assertEqual(s["total_sales_cents"], 32000)

    def test_partial_level_does_not_skew_totals(self):
        # Same SP spend in two breakdowns: a COMPLETE product_ad view ($300) and
        # a PARTIAL one-row ad_group view ($15). Totals must use the complete one,
        # not the coarser-but-partial ad_group, and must not sum both.
        rows = [
            AdRow(ad_type="SP", entity_level="product_ad", entity_text="SKU1",
                  impressions=1000, clicks=50, spend_cents=20000, sales_cents=60000, orders=10),
            AdRow(ad_type="SP", entity_level="product_ad", entity_text="SKU2",
                  impressions=800, clicks=40, spend_cents=10000, sales_cents=30000, orders=6),
            AdRow(ad_type="SP", entity_level="ad_group", entity_text="AG1",
                  impressions=160, clicks=18, spend_cents=1500, sales_cents=29000, orders=10),
        ]
        s = E.compute_summary(rows, [], [], Goals())
        self.assertEqual(s["ad_spend_cents"], 30000)  # product_ad total, not 31500 or 1500

    def test_search_term_excluded_from_totals(self):
        rows = [
            AdRow(ad_type="SP", entity_level="product_ad", entity_text="SKU1",
                  impressions=1000, clicks=50, spend_cents=20000, sales_cents=60000, orders=10),
            _st("some term", clicks=30, spend=20000, sales=60000, orders=10),  # same spend, diagnostic
        ]
        s = E.compute_summary(rows, [], [], Goals())
        self.assertEqual(s["ad_spend_cents"], 20000)  # search_term view not added

    def test_blended_tacos_includes_external(self):
        rows = [_kw("x", clicks=20, spend=1000, sales=10000, orders=5)]
        sales = [SalesRow(asin="B1", ordered_product_sales_cents=40000, units=10)]
        ext = [ExternalCostRow(channel="meta", amount_cents=3000),
               ExternalCostRow(channel="influencer", cost_type="commission", amount_cents=1000)]
        s = E.compute_summary(rows, sales, ext, Goals())
        self.assertEqual(s["external_spend_cents"], 4000)
        self.assertEqual(s["influencer_spend_cents"], 1000)
        # TACoS = ad spend / total sales = 1000/40000 = 250 bps
        self.assertEqual(s["tacos_bps"], 250)
        # Blended = (1000+4000)/40000 = 1250 bps
        self.assertEqual(s["blended_tacos_bps"], 1250)

    def test_revenue_gap(self):
        rows = [_kw("x", clicks=20, spend=1000, sales=10000, orders=5)]
        sales = [SalesRow(ordered_product_sales_cents=40000, units=10)]
        goals = Goals(revenue_target_cents=100000, units_target=50)
        s = E.compute_summary(rows, sales, [], goals)
        self.assertEqual(s["gap"]["revenue_gap_cents"], 60000)
        self.assertEqual(s["gap"]["units_delta"], -40)


class RulesTest(unittest.TestCase):
    def setUp(self):
        self.goals = Goals(acos_target_bps=3000)  # 30%

    def _cats(self, recs):
        return [r.category for r in recs]

    def test_wasted_spend_becomes_negative(self):
        rows = [_st("cheap junk", clicks=25, spend=5000, sales=0, orders=0)]
        recs = E.build_recommendations(rows, [], [], [], self.goals)
        neg = [r for r in recs if r.category == "negative_keyword"]
        self.assertEqual(len(neg), 1)
        self.assertTrue(neg[0].is_bulk_actionable)
        self.assertEqual(neg[0].bulk_row["action"], "create_negative")
        self.assertEqual(neg[0].projected_impact["spend_saved_cents"], 5000)

    def test_high_acos_bid_down(self):
        # ACoS 200% >> target 30% -> bid down below current bid
        rows = [_kw("blue", clicks=40, spend=4000, sales=2000, orders=2, bid=120)]
        recs = E.build_recommendations(rows, [], [], [], self.goals)
        bd = [r for r in recs if r.category == "bid_down"]
        self.assertEqual(len(bd), 1)
        self.assertTrue(bd[0].is_bulk_actionable)
        self.assertEqual(bd[0].bulk_row["action"], "set_bid")
        self.assertLess(bd[0].bulk_row["new_bid_cents"], 120)

    def test_low_acos_bid_up(self):
        # ACoS 10% well under 30% target, converting -> bid up
        rows = [_kw("red", clicks=60, spend=3000, sales=30000, orders=20, bid=50)]
        recs = E.build_recommendations(rows, [], [], [], self.goals)
        bu = [r for r in recs if r.category == "bid_up"]
        self.assertEqual(len(bu), 1)
        self.assertGreater(bu[0].bulk_row["new_bid_cents"], 50)

    def test_harvest_converting_search_term(self):
        rows = [_st("widget green", clicks=15, spend=1500, sales=6000, orders=4)]
        recs = E.build_recommendations(rows, [], [], [], self.goals)
        nk = [r for r in recs if r.category == "new_keyword"]
        self.assertEqual(len(nk), 1)
        self.assertEqual(nk[0].bulk_row["match_type"], "exact")

    def test_harvest_skips_existing_keyword(self):
        rows = [
            _st("widget green", clicks=15, spend=1500, sales=6000, orders=4),
            _kw("widget green", clicks=10, spend=500, sales=4000, orders=3, bid=60),
        ]
        recs = E.build_recommendations(rows, [], [], [], self.goals)
        self.assertEqual([r for r in recs if r.category == "new_keyword"], [])

    def test_external_spend_is_manual_context(self):
        ext = [ExternalCostRow(channel="meta", amount_cents=10000)]
        sales = [SalesRow(ordered_product_sales_cents=50000, units=10)]
        recs = E.build_recommendations([], sales, [], ext, self.goals)
        ex = [r for r in recs if r.category == "external"]
        self.assertEqual(len(ex), 1)
        self.assertFalse(ex[0].is_bulk_actionable)

    def test_unsupported_ad_type_flagged_manual(self):
        rows = [AdRow(ad_type="STV", entity_level="campaign", campaign_name="TV",
                      impressions=9000, clicks=10, spend_cents=20000, sales_cents=5000, orders=1)]
        recs = E.build_recommendations(rows, [], [], [], self.goals)
        manual = [r for r in recs if r.category == "manual" and r.ad_type == "STV"]
        self.assertEqual(len(manual), 1)
        self.assertFalse(manual[0].is_bulk_actionable)

    def test_ranking_orders_by_weighted_impact(self):
        rows = [
            _st("small waste", clicks=12, spend=1600, sales=0, orders=0),
            _st("big waste", clicks=60, spend=20000, sales=0, orders=0),
        ]
        recs = E.build_recommendations(rows, [], [], [], self.goals)
        negs = [r for r in recs if r.category == "negative_keyword"]
        # Bigger wasted spend ranks first.
        self.assertIn("big waste", negs[0].title)


if __name__ == "__main__":
    unittest.main()
