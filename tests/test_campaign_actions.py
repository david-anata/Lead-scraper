"""Campaign Actions tab — rolls the recommendations up per campaign so each
campaign shows its specific moves. Regression: it used to only sum
product_ad/campaign/ad_group rows, so accounts with keyword/target-only data got
an empty tab."""

from __future__ import annotations

import unittest
from types import SimpleNamespace as NS

try:
    from sales_support_agent.services.advertising.deliverable import _campaign_actions
    DEPS = True
except ModuleNotFoundError:
    DEPS = False


def _ad(campaign, spend, sales, level="keyword"):
    return NS(entity_level=level, campaign_name=campaign, spend_cents=spend, sales_cents=sales)


def _rec(category, campaign):
    return NS(category=category, entity_ref=f"{campaign} › ag › kw",
              bulk_row={"campaign_name": campaign})


@unittest.skipUnless(DEPS, "app required")
class CampaignActionsTests(unittest.TestCase):
    def test_keyword_level_account_is_not_empty(self):
        # All rows are keyword-level (the case that used to yield an empty tab).
        ad_rows = [_ad("Auto Camp", 60000, 7000), _ad("Manual Camp", 20000, 50000)]
        recs = [_rec("bid_down", "Auto Camp"), _rec("bid_down", "Auto Camp"),
                _rec("new_keyword", "Auto Camp"), _rec("bid_up", "Manual Camp"),
                _rec("negative_keyword", "Manual Camp")]
        out = _campaign_actions(ad_rows, recs, target_acos_bps=3000)
        self.assertEqual(len(out), 2)
        by = {r["campaign"]: r for r in out}
        # Auto Camp: 2 bid-downs + 1 new kw = 3 changes; spend dominates → first
        self.assertEqual(by["Auto Camp"]["changes"], 3)
        self.assertIn("↓2", by["Auto Camp"]["action"])
        self.assertIn("+1 kw", by["Auto Camp"]["action"])
        self.assertEqual(by["Manual Camp"]["changes"], 2)
        self.assertIn("↑1", by["Manual Camp"]["action"])
        self.assertIn("+1 neg", by["Manual Camp"]["action"])
        # Changed campaigns are present and spend is summed from keyword rows
        self.assertEqual(by["Auto Camp"]["spend"], 60000)

    def test_campaigns_with_changes_sort_first(self):
        ad_rows = [_ad("NoChange Big", 99999, 1000), _ad("Changed Small", 100, 200)]
        recs = [_rec("bid_down", "Changed Small")]
        out = _campaign_actions(ad_rows, recs, target_acos_bps=3000)
        self.assertEqual(out[0]["campaign"], "Changed Small")  # changed first despite tiny spend
        self.assertEqual(out[0]["action"], "↓1 bid")

    def test_campaign_name_falls_back_to_entity_ref(self):
        ad_rows = [_ad("Ref Camp", 1000, 2000)]
        rec = NS(category="bid_down", entity_ref="Ref Camp › ag › kw", bulk_row={})  # no campaign_name
        out = _campaign_actions(ad_rows, [rec], target_acos_bps=3000)
        self.assertEqual(out[0]["campaign"], "Ref Camp")
        self.assertEqual(out[0]["changes"], 1)


if __name__ == "__main__":
    unittest.main()
