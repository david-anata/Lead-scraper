"""The wasted-spend negative rule must (1) require a real no-convert signal
(clicks + spend, not a few dollars) and (2) never auto-negate a term in the
brand's own category — those go to review, not the apply file."""

from __future__ import annotations

import unittest

from sales_support_agent.services.advertising import engine
from sales_support_agent.services.advertising.schema import AdRow, SalesRow, Thresholds


def _st(term, clicks, spend_cents, orders=0):
    return AdRow(ad_type="SP", entity_level="search_term", campaign_name="C", ad_group_name="AG",
                campaign_id="c1", ad_group_id="a1", entity_text=term,
                clicks=clicks, spend_cents=spend_cents, orders=orders)


# Brand catalog: a weight-loss / appetite-suppressant supplement.
VOCAB = engine._brand_vocabulary([
    SalesRow(title="Zantrex SkinnyStix Energy Powder GLP-1 Appetite Suppressant Drink Mix"),
    SalesRow(title="Zantrex Black Thermogenic Energy Weight Management"),
])


class NegativeBarTest(unittest.TestCase):
    def _negs(self, rows):
        return engine._rule_wasted_spend_negatives(rows, Thresholds(), VOCAB)

    def test_off_target_term_over_bar_auto_negates(self):
        # "phone case" shares nothing with the catalog → off-target → apply-ready.
        recs = self._negs([_st("phone case", clicks=20, spend_cents=3000)])
        self.assertEqual(len(recs), 1)
        self.assertTrue(recs[0].is_bulk_actionable)
        self.assertEqual(recs[0].proposed_value, "negative exact")
        self.assertTrue(recs[0].title.startswith("Negate"))

    def test_core_category_term_is_review_only(self):
        # "appetite suppressant drink" is core Zantrex → review, NOT in the apply file.
        recs = self._negs([_st("appetite suppressant drink", clicks=20, spend_cents=3000)])
        self.assertEqual(len(recs), 1)
        self.assertFalse(recs[0].is_bulk_actionable)
        self.assertIn("review", recs[0].proposed_value)
        self.assertTrue(recs[0].title.startswith("Review"))

    def test_below_click_bar_no_negative(self):
        # Plenty of spend but only 8 clicks — not enough evidence to negate.
        self.assertEqual(self._negs([_st("phone case", clicks=8, spend_cents=5000)]), [])

    def test_below_spend_bar_no_negative(self):
        # 20 clicks but only $15 — under the $25 bar.
        self.assertEqual(self._negs([_st("phone case", clicks=20, spend_cents=1500)]), [])

    def test_converting_term_never_negated(self):
        self.assertEqual(self._negs([_st("phone case", clicks=30, spend_cents=5000, orders=2)]), [])


if __name__ == "__main__":
    unittest.main()
