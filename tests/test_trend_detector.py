"""Unit tests for trend_detector.py — all pure-function, no DB required."""

from __future__ import annotations

import unittest
from datetime import date

from sales_support_agent.services.cashflow.trend_detector import (
    _calc_confidence,
    _infer_frequency,
    _is_already_tracked,
    _jaccard,
    _normalize_vendor,
    _to_date,
)


class TestNormalizeVendor(unittest.TestCase):
    """_normalize_vendor strips noise and returns a stable title-cased key."""

    def test_strips_ach_codes(self) -> None:
        raw = "ACH WITHDRAWAL Fora Financial WEB PPD"
        result = _normalize_vendor(raw)
        self.assertNotIn("ACH", result)
        self.assertNotIn("WEB", result)
        self.assertNotIn("PPD", result)
        self.assertIn("Fora", result)

    def test_strips_embedded_dates(self) -> None:
        raw = "PAYMENT 03/15/2024 Shopify Inc"
        result = _normalize_vendor(raw)
        self.assertNotIn("03", result)
        self.assertNotIn("2024", result)
        self.assertIn("Shopify", result)

    def test_strips_long_reference_numbers(self) -> None:
        raw = "Withdrawal 123456789 DAVID NARAYAN"
        result = _normalize_vendor(raw)
        self.assertNotIn("123456789", result)
        self.assertIn("David", result)

    def test_title_cases_result(self) -> None:
        raw = "stripe inc payments"
        result = _normalize_vendor(raw)
        self.assertTrue(result[0].isupper() or result == "", "Result should be title-cased")

    def test_empty_string_returns_empty(self) -> None:
        self.assertEqual(_normalize_vendor(""), "")

    def test_noise_only_returns_empty(self) -> None:
        # All tokens stripped → empty result
        result = _normalize_vendor("ACH WEB CCD PPD 999999 03/15")
        self.assertEqual(result, "")

    def test_caps_at_four_tokens(self) -> None:
        raw = "Alpha Beta Gamma Delta Epsilon Zeta"
        result = _normalize_vendor(raw)
        self.assertLessEqual(len(result.split()), 4)

    def test_real_bank_description(self) -> None:
        raw = "Withdrawal ACH CANYON VIEW MANAGEMENT Entry Class Code CCD"
        result = _normalize_vendor(raw)
        # Core vendor name should survive
        self.assertIn("Canyon", result)
        # ACH code and transaction boilerplate should be stripped
        self.assertNotIn("ACH", result)
        self.assertNotIn("CCD", result)
        self.assertNotIn("Withdrawal", result)


class TestInferFrequency(unittest.TestCase):
    """_infer_frequency maps median day-gaps to frequency labels."""

    def test_7_days_is_weekly(self) -> None:
        freq, tol = _infer_frequency(7.0)
        self.assertEqual(freq, "weekly")
        self.assertGreater(tol, 0)

    def test_14_days_is_biweekly(self) -> None:
        freq, _ = _infer_frequency(14.0)
        self.assertEqual(freq, "biweekly")

    def test_30_days_is_monthly(self) -> None:
        freq, _ = _infer_frequency(30.0)
        self.assertEqual(freq, "monthly")

    def test_31_days_is_monthly(self) -> None:
        # Month boundaries vary (28–31 days); 31 must still be monthly
        freq, _ = _infer_frequency(31.0)
        self.assertEqual(freq, "monthly")

    def test_90_days_is_quarterly(self) -> None:
        freq, _ = _infer_frequency(90.0)
        self.assertEqual(freq, "quarterly")

    def test_200_days_is_irregular(self) -> None:
        freq, _ = _infer_frequency(200.0)
        self.assertEqual(freq, "irregular")

    def test_tolerance_is_positive(self) -> None:
        for gap in (7.0, 14.0, 30.0, 90.0, 200.0):
            _, tol = _infer_frequency(gap)
            self.assertGreater(tol, 0, f"tolerance should be > 0 for gap={gap}")


class TestCalcConfidence(unittest.TestCase):
    """_calc_confidence returns a score in [0, 1]."""

    def test_high_confidence_scenario(self) -> None:
        score = _calc_confidence(
            occurrence_count=6,
            amount_cv=0.02,       # very stable amounts
            gap_consistency=1.0,  # perfect cadence
            frequency="monthly",
        )
        self.assertGreater(score, 0.75, "6 consistent monthly occurrences should score > 0.75")

    def test_low_confidence_scenario(self) -> None:
        score = _calc_confidence(
            occurrence_count=2,
            amount_cv=0.28,       # noisy amounts
            gap_consistency=0.5,  # inconsistent gaps
            frequency="irregular",
        )
        self.assertLess(score, 0.5, "2 irregular occurrences should score < 0.5")

    def test_score_bounded_0_to_1(self) -> None:
        for occ in (1, 3, 10, 50):
            for cv in (0.0, 0.15, 0.50, 1.0):
                score = _calc_confidence(
                    occurrence_count=occ, amount_cv=cv,
                    gap_consistency=1.0, frequency="monthly",
                )
                self.assertGreaterEqual(score, 0.0)
                self.assertLessEqual(score, 1.0)

    def test_more_occurrences_increases_score(self) -> None:
        base = dict(amount_cv=0.05, gap_consistency=0.9, frequency="monthly")
        low  = _calc_confidence(occurrence_count=2, **base)
        high = _calc_confidence(occurrence_count=6, **base)
        self.assertGreater(high, low)

    def test_known_frequency_bonus(self) -> None:
        base = dict(occurrence_count=4, amount_cv=0.05, gap_consistency=0.9)
        monthly  = _calc_confidence(**base, frequency="monthly")
        irregular = _calc_confidence(**base, frequency="irregular")
        self.assertGreater(monthly, irregular, "monthly cadence should score higher than irregular")


class TestIsAlreadyTracked(unittest.TestCase):
    """_is_already_tracked returns True when a template covers the pattern."""

    def _tmpl(self, vendor: str, event_type: str = "outflow", amount_cents: int = 100_00) -> dict:
        return {
            "vendor_or_customer": vendor,
            "name": vendor,
            "event_type": event_type,
            "amount_cents": amount_cents,
        }

    def test_exact_match_returns_true(self) -> None:
        tracked = _is_already_tracked("Stripe", "outflow", 100_00, [self._tmpl("Stripe")])
        self.assertTrue(tracked)

    def test_different_vendor_returns_false(self) -> None:
        tracked = _is_already_tracked("Shopify", "outflow", 100_00, [self._tmpl("Stripe")])
        self.assertFalse(tracked)

    def test_different_event_type_returns_false(self) -> None:
        tracked = _is_already_tracked("Stripe", "inflow", 100_00, [self._tmpl("Stripe", event_type="outflow")])
        self.assertFalse(tracked)

    def test_amount_far_off_returns_false(self) -> None:
        # Amount differs by 50% — beyond the 25% tolerance
        tracked = _is_already_tracked("Stripe", "outflow", 100_00, [self._tmpl("Stripe", amount_cents=200_00)])
        self.assertFalse(tracked)

    def test_empty_templates_returns_false(self) -> None:
        self.assertFalse(_is_already_tracked("Stripe", "outflow", 100_00, []))


class TestJaccard(unittest.TestCase):
    def test_identical_sets(self) -> None:
        self.assertEqual(_jaccard(["a", "b"], ["a", "b"]), 1.0)

    def test_disjoint_sets(self) -> None:
        self.assertEqual(_jaccard(["a", "b"], ["c", "d"]), 0.0)

    def test_partial_overlap(self) -> None:
        score = _jaccard(["a", "b"], ["a", "c"])
        self.assertAlmostEqual(score, 1 / 3)

    def test_empty_inputs(self) -> None:
        self.assertEqual(_jaccard([], ["a"]), 0.0)
        self.assertEqual(_jaccard(["a"], []), 0.0)


class TestToDate(unittest.TestCase):
    def test_date_passthrough(self) -> None:
        d = date(2026, 4, 7)
        self.assertEqual(_to_date(d), d)

    def test_iso_string(self) -> None:
        self.assertEqual(_to_date("2026-04-07"), date(2026, 4, 7))

    def test_iso_string_with_time(self) -> None:
        self.assertEqual(_to_date("2026-04-07T10:30:00"), date(2026, 4, 7))

    def test_none_returns_none(self) -> None:
        self.assertIsNone(_to_date(None))

    def test_invalid_string_returns_none(self) -> None:
        self.assertIsNone(_to_date("not-a-date"))


if __name__ == "__main__":
    unittest.main()
