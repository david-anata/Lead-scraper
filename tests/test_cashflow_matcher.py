"""Tests for auto_match_transactions and scoring helpers."""

from __future__ import annotations

import unittest
from datetime import date, datetime

from sales_support_agent.services.cashflow.matcher import (
    MatchResult,
    auto_match_transactions,
)


def _csv(event_id: str, vendor: str, amount: int, due: date, event_type: str = "outflow") -> dict:
    return {
        "id": event_id,
        "source": "csv",
        "event_type": event_type,
        "vendor_or_customer": vendor,
        "amount_cents": amount,
        "due_date": due,
        "status": "posted",
        "category": "other",
        "name": vendor,
    }


def _planned(event_id: str, vendor: str, amount: int, due: date, event_type: str = "outflow") -> dict:
    return {
        "id": event_id,
        "source": "manual",
        "event_type": event_type,
        "vendor_or_customer": vendor,
        "amount_cents": amount,
        "due_date": due,
        "status": "planned",
        "category": "other",
        "name": vendor,
    }


class TestAutoMatchTransactions(unittest.TestCase):
    def test_perfect_match(self) -> None:
        d = date(2026, 4, 7)
        csv_events = [_csv("c1", "ACME CORP", 100_00, d)]
        planned_events = [_planned("p1", "ACME CORP", 100_00, d)]
        results = auto_match_transactions(csv_events, planned_events)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].csv_event_id, "c1")
        self.assertEqual(results[0].planned_event_id, "p1")
        self.assertGreater(results[0].score, 0.5)

    def test_no_match_different_event_type(self) -> None:
        d = date(2026, 4, 7)
        csv_events = [_csv("c1", "ACME", 100_00, d, event_type="inflow")]
        planned_events = [_planned("p1", "ACME", 100_00, d, event_type="outflow")]
        results = auto_match_transactions(csv_events, planned_events)
        self.assertEqual(results[0].planned_event_id, None)

    def test_no_match_amount_too_far(self) -> None:
        d = date(2026, 4, 7)
        csv_events = [_csv("c1", "ACME", 100_00, d)]
        planned_events = [_planned("p1", "ACME", 500_00, d)]  # 400% difference
        results = auto_match_transactions(csv_events, planned_events)
        self.assertEqual(results[0].planned_event_id, None)

    def test_no_match_date_too_far(self) -> None:
        csv_events = [_csv("c1", "ACME", 100_00, date(2026, 4, 1))]
        planned_events = [_planned("p1", "ACME", 100_00, date(2026, 5, 1))]  # 30 days apart
        results = auto_match_transactions(csv_events, planned_events)
        self.assertEqual(results[0].planned_event_id, None)

    def test_amount_within_tolerance_matches(self) -> None:
        d = date(2026, 4, 7)
        # 5% off — within 10% tolerance
        csv_events = [_csv("c1", "FORA FINANCIAL", 95_00, d)]
        planned_events = [_planned("p1", "FORA FINANCIAL", 100_00, d)]
        results = auto_match_transactions(csv_events, planned_events)
        self.assertIsNotNone(results[0].planned_event_id)

    def test_chunkable_partial_payment_matches_only_with_strong_evidence(self) -> None:
        d = date(2026, 4, 7)
        transaction = _csv("c1", "ACME RENT", 40_00, d)
        obligation = _planned("p1", "ACME RENT", 100_00, d)
        obligation["flexibility"] = "chunkable"
        results = auto_match_transactions([transaction], [obligation])
        self.assertEqual(results[0].planned_event_id, "p1")
        self.assertIn("partial payment", results[0].reason)

    def test_chunkable_obligation_accepts_multiple_independently_evidenced_partials(self) -> None:
        d = date(2026, 4, 7)
        transactions = [
            _csv("c1", "ACME RENT", 40_00, d),
            _csv("c2", "ACME RENT", 40_00, d),
        ]
        obligation = _planned("p1", "ACME RENT", 100_00, d)
        obligation["flexibility"] = "chunk payable"

        results = auto_match_transactions(transactions, [obligation])

        self.assertEqual([result.planned_event_id for result in results], ["p1", "p1"])
        self.assertTrue(all("partial payment" in result.reason for result in results))

    def test_no_double_match(self) -> None:
        d = date(2026, 4, 7)
        csv_events = [
            _csv("c1", "ACME", 100_00, d),
            _csv("c2", "ACME", 100_00, d),
        ]
        planned_events = [_planned("p1", "ACME", 100_00, d)]
        results = auto_match_transactions(csv_events, planned_events)
        matched = [r for r in results if r.planned_event_id == "p1"]
        self.assertEqual(len(matched), 1, "Same planned event should not be matched twice")

    def test_empty_inputs_return_empty(self) -> None:
        self.assertEqual(auto_match_transactions([], []), [])
        # With CSV events but no planned events, returns one unmatched result per CSV event
        results = auto_match_transactions([_csv("c1", "X", 100_00, date(2026, 4, 7))], [])
        self.assertEqual(len(results), 1)
        self.assertIsNone(results[0].planned_event_id)

    def test_result_has_reason_string(self) -> None:
        d = date(2026, 4, 7)
        csv_events = [_csv("c1", "ACME", 100_00, d)]
        planned_events = [_planned("p1", "ACME", 100_00, d)]
        results = auto_match_transactions(csv_events, planned_events)
        self.assertIsInstance(results[0].reason, str)
        self.assertTrue(len(results[0].reason) > 0)

    def test_vendor_similarity_helps_match(self) -> None:
        d = date(2026, 4, 7)
        # Slightly different vendor strings (ACH boilerplate)
        csv_events = [_csv("c1", "WITHDRAWAL ACH FORAFINANCIAL WEB", 100_00, d)]
        planned_events = [_planned("p1", "FORAFINANCIAL", 100_00, d)]
        results = auto_match_transactions(csv_events, planned_events)
        self.assertIsNotNone(results[0].planned_event_id)

    def test_equal_candidates_fail_closed_as_ambiguous(self) -> None:
        d = date(2026, 4, 7)
        results = auto_match_transactions(
            [_csv("c1", "ACME", 100_00, d)],
            [_planned("p1", "ACME", 100_00, d), _planned("p2", "ACME", 100_00, d)],
        )
        self.assertIsNone(results[0].planned_event_id)
        self.assertEqual(results[0].match_status, "ambiguous")
        self.assertEqual(results[0].score_bps, 10_000)
        self.assertEqual(results[0].candidate_ids, ["p1", "p2"])


class CategoryEvidenceTests(unittest.TestCase):
    """Reading QuickBooks' own account must not cost us settlement matches.

    Before the account was read, almost every posted transaction and obligation
    fell back to "other", so the category bonus fired on two unknowns agreeing.
    The auto-match threshold was calibrated with that phantom bonus in it, so
    once real accounts arrived the same true payments scored 1,500 bps lower and
    stopped matching. This is a real bank descriptor shape: the obligation is
    "Boulder Ranch LLC" and the payment reads "Withdrawal ACH Boulder Ranch".
    """

    def _pair(self, obligation_category: str, actual_category: str):
        due = date(2026, 7, 10)
        actual = _csv("c1", "Withdrawal ACH Boulder Ranch", 505_00, date(2026, 7, 12))
        actual["category"] = actual_category
        planned = _planned("p1", "Boulder Ranch LLC", 500_00, due)
        planned["category"] = obligation_category
        return auto_match_transactions([actual], [planned])[0]

    def test_a_quickbooks_account_still_matches_an_uncategorised_obligation(self) -> None:
        booked = self._pair("other", "job materials")
        unknown = self._pair("other", "other")

        self.assertEqual(booked.score_bps, unknown.score_bps, "knowing more must not score less")
        self.assertEqual(booked.planned_event_id, "p1")

    def test_an_unknown_category_is_never_treated_as_a_disagreement(self) -> None:
        for obligation_category, actual_category in (
            ("other", "job materials"), ("job materials", "other"),
            ("", "job materials"), ("uncategorized", "job materials"),
        ):
            result = self._pair(obligation_category, actual_category)
            self.assertNotIn("conflict", result.reason, (obligation_category, actual_category))

    def test_two_known_categories_that_disagree_lose_the_points(self) -> None:
        """This never cost anything before, because nothing had a real category."""
        agree = self._pair("payroll", "payroll")
        conflict = self._pair("payroll", "software")

        self.assertEqual(agree.score_bps - conflict.score_bps, 1_500)
        self.assertEqual(agree.planned_event_id, "p1")
        self.assertIsNone(
            conflict.planned_event_id,
            "a bill booked as payroll settled by a software charge is not confident enough",
        )

    def test_a_disagreement_is_named_in_the_reason_when_it_still_matches(self) -> None:
        """Strong vendor, amount and date evidence still wins, but the operator
        should be able to see the books disagreed about what kind of spend it is."""
        due = date(2026, 7, 10)
        actual = _csv("c1", "Gusto Payroll", 500_00, due)
        actual["category"] = "software"
        planned = _planned("p1", "Gusto Payroll", 500_00, due)
        planned["category"] = "payroll"

        result = auto_match_transactions([actual], [planned])[0]

        self.assertEqual(result.planned_event_id, "p1")
        self.assertIn("category conflict", result.reason)


if __name__ == "__main__":
    unittest.main()
