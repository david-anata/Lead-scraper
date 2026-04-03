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
        self.assertEqual(auto_match_transactions([_csv("c1", "X", 100_00, date(2026, 4, 7))], []), [])

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


if __name__ == "__main__":
    unittest.main()
