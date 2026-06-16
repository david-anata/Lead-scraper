"""Auto-campaign bid expansion — an AUTOMATIC aggregate bid-down (no Target ID,
skipped from the apply sheet) is expanded into per-auto-target Update rows that
DO carry Target IDs + bids from the bulk file. MANUAL is left alone."""

from __future__ import annotations

import io
import unittest
from types import SimpleNamespace

try:
    import openpyxl
    from sales_support_agent.services.advertising import normalizers as N
    DEPS = True
except ModuleNotFoundError:
    DEPS = False


def _bulk_with_auto_targets() -> bytes:
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Sponsored Products Campaigns"
    ws.append(["Entity", "Campaign ID", "Ad Group ID", "Product Targeting ID",
               "Product Targeting Expression", "Bid"])
    for expr, tid, bid in [("close-match", "T1", 1.00), ("loose-match", "T2", 0.80),
                           ("substitutes", "T3", 0.60), ("complements", "T4", 0.50)]:
        ws.append(["Product Targeting", "C1", "AG1", tid, expr, bid])
    # A non-auto manual product target in the same workbook — must be ignored.
    ws.append(["Product Targeting", "C1", "AG1", "T9", 'asin="B000"', 2.00])
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def _auto_rec():
    return SimpleNamespace(
        current_value="$2.00", proposed_value="$0.50", is_bulk_actionable=True,
        bulk_row={"action": "set_bid", "ad_type": "SP", "campaign_id": "C1",
                  "keyword_id": "", "target_id": "", "keyword_text": "AUTOMATIC",
                  "new_bid_cents": 50})


@unittest.skipUnless(DEPS, "openpyxl + app required")
class AutoExpandTests(unittest.TestCase):
    def test_automatic_expands_to_auto_targets(self):
        out, n = N.expand_aggregate_bid_recs_for_apply([_auto_rec()], _bulk_with_auto_targets())
        self.assertEqual(n, 4)  # 4 auto-targets, not the manual one
        self.assertEqual(len(out), 4)
        # Every expanded row carries a Target ID + an actionable set_bid
        self.assertTrue(all(r.bulk_row["target_id"] for r in out))
        self.assertTrue(all(r.bulk_row["action"] == "set_bid" for r in out))
        self.assertTrue(all(r.is_bulk_actionable for r in out))
        # Ratio 0.25 (50/200): each target's bid scaled down from its own base
        exprs = {r.bulk_row["targeting_expression"] for r in out}
        self.assertEqual(exprs, {"close-match", "loose-match", "substitutes", "complements"})
        for r in out:
            self.assertGreaterEqual(r.bulk_row["new_bid_cents"], 2)   # >= Amazon min
            self.assertLessEqual(r.bulk_row["new_bid_cents"], 25)     # < base (cut)

    def test_manual_is_not_expanded(self):
        rec = _auto_rec()
        rec.bulk_row["keyword_text"] = "MANUAL"
        out, n = N.expand_aggregate_bid_recs_for_apply([rec], _bulk_with_auto_targets())
        self.assertEqual(n, 0)
        self.assertEqual(out, [rec])  # passed through untouched

    def test_keyword_with_id_passes_through(self):
        rec = _auto_rec()
        rec.bulk_row.update({"keyword_id": "K1", "keyword_text": "fluoro shampoo"})
        out, n = N.expand_aggregate_bid_recs_for_apply([rec], _bulk_with_auto_targets())
        self.assertEqual(n, 0)
        self.assertEqual(out, [rec])


if __name__ == "__main__":
    unittest.main()
