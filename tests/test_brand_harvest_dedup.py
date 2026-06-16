"""Harvest de-dup against 'already exists' — the additions file kept getting
rejected because it re-harvested (a) brand variations (no 4 / n4 / no. 4) and
(b) search terms that already matched an EXACT keyword."""

from __future__ import annotations

import unittest
from types import SimpleNamespace as NS

try:
    from sales_support_agent.services.advertising import normalizers as N
    from sales_support_agent.services.advertising.engine import _rule_harvest_keywords, Thresholds
    from sales_support_agent.services.advertising.schema import AdRow
    DEPS = True
except ModuleNotFoundError:
    DEPS = False


def _kw_harvest(text):
    return NS(is_bulk_actionable=True,
              bulk_row={"action": "create_keyword", "keyword_text": text})


@unittest.skipUnless(DEPS, "app required")
class BrandVariantTests(unittest.TestCase):
    def test_variants_cover_numbered_brand_family(self):
        v = N._brand_variants("Number 4")
        for k in ("number 4", "number4", "no 4", "no4", "n4", "n 4"):
            self.assertIn(k, v)

    def test_drops_brand_variation_harvests_keeps_real(self):
        recs = [_kw_harvest("no 4 shampoo and conditioner"),  # brand variant -> drop
                _kw_harvest("no. 4 mini conditioner"),         # punctuation variant -> drop
                _kw_harvest("n4 hydrating shampoo"),           # abbrev -> drop
                _kw_harvest("vegamour hair serum")]            # competitor -> keep
        dropped = N.drop_brand_term_harvests(recs, "Number 4")
        self.assertEqual(dropped, 3)
        self.assertFalse(recs[0].is_bulk_actionable)
        self.assertFalse(recs[1].is_bulk_actionable)
        self.assertFalse(recs[2].is_bulk_actionable)
        self.assertTrue(recs[3].is_bulk_actionable)  # genuinely new


@unittest.skipUnless(DEPS, "app required")
class HarvestExactMatchTests(unittest.TestCase):
    def _row(self, level, text, *, match="", orders=0):
        return AdRow(ad_type="SP", entity_level=level, entity_text=text,
                     campaign_id="C1", ad_group_id="A1", campaign_name="Camp",
                     ad_group_name="AG", match_type=match, orders=orders,
                     sales_cents=5000, clicks=10, spend_cents=800)  # cpc=80

    def test_exact_matched_search_term_not_harvested(self):
        thr = Thresholds()
        rows = [
            # converting search term that ALREADY matched via EXACT -> skip harvest
            self._row("search_term", "leau de mare shampoo", match="exact",
                      orders=thr.promote_keyword_min_orders + 2),
            # converting search term from BROAD -> legit new harvest
            self._row("search_term", "brand new winning term", match="broad",
                      orders=thr.promote_keyword_min_orders + 2),
        ]
        recs = _rule_harvest_keywords(rows, thr)
        harvested = {r.bulk_row["keyword_text"] for r in recs}
        self.assertIn("brand new winning term", harvested)
        self.assertNotIn("leau de mare shampoo", harvested)  # already exact -> skipped


if __name__ == "__main__":
    unittest.main()
