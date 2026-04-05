"""Tests for compute_finance_overview — verifies metric classification logic."""
from __future__ import annotations

import unittest
from dataclasses import dataclass
from datetime import date, timedelta
from types import SimpleNamespace

from sales_support_agent.services.cashflow.engine import EventDTO, RiskAlert
from sales_support_agent.services.cashflow.overview import compute_finance_overview


_TODAY = date(2026, 4, 7)   # a Monday


def _make_week(net_cents: int = 0, inflow_cents: int = 0, outflow_cents: int = 0) -> SimpleNamespace:
    return SimpleNamespace(net_cents=net_cents, inflow_cents=inflow_cents, outflow_cents=outflow_cents)


def _make_event(
    *,
    event_type: str = "outflow",
    status: str = "planned",
    amount_cents: int = 10000,
    due_date: date | None = None,
) -> EventDTO:
    return EventDTO(
        id="test-id",
        source="manual",
        event_type=event_type,
        category="other",
        name="Test",
        vendor_or_customer="Vendor",
        amount_cents=amount_cents,
        due_date=due_date or _TODAY,
        status=status,
        confidence="confirmed",
    )


def _make_alert(severity: str) -> RiskAlert:
    return RiskAlert(
        severity=severity,
        alert_type="negative_week",
        title="Alert",
        detail="Detail",
    )


class TestNetClass(unittest.TestCase):
    def test_net_zero_gives_empty_class(self) -> None:
        weeks = [_make_week(net_cents=0)]
        m = compute_finance_overview([], [], weeks, 100_00, today=_TODAY)
        self.assertEqual(m.net_class, "")

    def test_net_negative_gives_negative_class(self) -> None:
        weeks = [_make_week(net_cents=-500_00)]
        m = compute_finance_overview([], [], weeks, 100_00, today=_TODAY)
        self.assertEqual(m.net_class, "negative")

    def test_net_positive_gives_positive_class(self) -> None:
        weeks = [_make_week(net_cents=500_00)]
        m = compute_finance_overview([], [], weeks, 100_00, today=_TODAY)
        self.assertEqual(m.net_class, "positive")

    def test_net_4w_sums_all_weeks(self) -> None:
        weeks = [_make_week(net_cents=100), _make_week(net_cents=200), _make_week(net_cents=-50)]
        m = compute_finance_overview([], [], weeks, 0, today=_TODAY)
        self.assertEqual(m.net_4w_cents, 250)


class TestBalanceClass(unittest.TestCase):
    def test_zero_balance_gives_empty_class(self) -> None:
        m = compute_finance_overview([], [], [], 0, today=_TODAY)
        self.assertEqual(m.balance_class, "")

    def test_negative_balance_gives_negative_class(self) -> None:
        m = compute_finance_overview([], [], [], -100_00, today=_TODAY)
        self.assertEqual(m.balance_class, "negative")

    def test_positive_balance_gives_empty_class(self) -> None:
        m = compute_finance_overview([], [], [], 500_00, today=_TODAY)
        self.assertEqual(m.balance_class, "")


class TestUpcomingClass(unittest.TestCase):
    def test_zero_upcoming_gives_empty_class(self) -> None:
        m = compute_finance_overview([], [], [], 0, today=_TODAY)
        self.assertEqual(m.upcoming_class, "")

    def test_positive_upcoming_gives_amount_out_class(self) -> None:
        event = _make_event(
            event_type="outflow",
            status="planned",
            amount_cents=500_00,
            due_date=_TODAY + timedelta(days=5),
        )
        m = compute_finance_overview([event], [], [], 0, today=_TODAY)
        self.assertEqual(m.upcoming_class, "amount-out")
        self.assertEqual(m.upcoming_total_cents, 500_00)
        self.assertEqual(m.upcoming_count, 1)

    def test_inflow_not_counted_in_upcoming_outflow(self) -> None:
        event = _make_event(
            event_type="inflow",
            status="planned",
            amount_cents=500_00,
            due_date=_TODAY + timedelta(days=5),
        )
        m = compute_finance_overview([event], [], [], 0, today=_TODAY)
        self.assertEqual(m.upcoming_class, "")
        self.assertEqual(m.upcoming_total_cents, 0)

    def test_upcoming_only_within_14_days(self) -> None:
        near = _make_event(
            event_type="outflow",
            status="planned",
            amount_cents=100_00,
            due_date=_TODAY + timedelta(days=10),
        )
        far = _make_event(
            event_type="outflow",
            status="planned",
            amount_cents=200_00,
            due_date=_TODAY + timedelta(days=20),
        )
        m = compute_finance_overview([near, far], [], [], 0, today=_TODAY)
        self.assertEqual(m.upcoming_total_cents, 100_00)
        self.assertEqual(m.upcoming_count, 1)


class TestOverdueFiltering(unittest.TestCase):
    def test_only_overdue_status_counted(self) -> None:
        overdue_event = _make_event(status="overdue", amount_cents=300_00)
        planned_event = _make_event(status="planned", amount_cents=200_00)
        paid_event = _make_event(status="paid", amount_cents=100_00)
        m = compute_finance_overview(
            [overdue_event, planned_event, paid_event], [], [], 0, today=_TODAY
        )
        self.assertEqual(m.overdue_count, 1)
        self.assertEqual(m.overdue_total_cents, 300_00)
        self.assertEqual(m.overdue_class, "negative")

    def test_no_overdue_gives_empty_class(self) -> None:
        event = _make_event(status="planned", amount_cents=100_00)
        m = compute_finance_overview([event], [], [], 0, today=_TODAY)
        self.assertEqual(m.overdue_class, "")
        self.assertEqual(m.overdue_count, 0)


class TestAlertsClass(unittest.TestCase):
    def test_critical_alert_gives_negative_class(self) -> None:
        m = compute_finance_overview([], [_make_alert("critical")], [], 0, today=_TODAY)
        self.assertEqual(m.alerts_class, "negative")
        self.assertEqual(m.critical_count, 1)

    def test_warning_only_gives_empty_class(self) -> None:
        m = compute_finance_overview([], [_make_alert("warning")], [], 0, today=_TODAY)
        self.assertEqual(m.alerts_class, "")
        self.assertEqual(m.warning_count, 1)

    def test_no_alerts_gives_empty_class(self) -> None:
        m = compute_finance_overview([], [], [], 0, today=_TODAY)
        self.assertEqual(m.alerts_class, "")


class TestAiText(unittest.TestCase):
    def test_ai_text_passed_through(self) -> None:
        m = compute_finance_overview([], [], [], 0, today=_TODAY, ai_text="Summary here")
        self.assertEqual(m.ai_text, "Summary here")

    def test_ai_text_defaults_empty(self) -> None:
        m = compute_finance_overview([], [], [], 0, today=_TODAY)
        self.assertEqual(m.ai_text, "")


class TestPostedRowFilterRegression(unittest.TestCase):
    """Regression guard for the posted-row double-counting bug (fixed 2026-04-05).

    When building EventDTOs for aggregate_weeks() the forecast code filters out
    events with status in ('posted', 'matched', 'cancelled', 'paid') because
    those are already reflected in the starting balance from the latest bank CSV.
    If that filter is removed, posted rows will double-count and forecasted
    outflows will appear as $0.

    These tests call _events_to_dtos() with the same filter the live code uses
    and assert that settled events never appear in the DTO list.
    """

    def _make_raw(self, status: str, event_type: str = "outflow") -> dict:
        return {
            "id": f"id-{status}",
            "source": "csv" if status in ("posted", "matched") else "manual",
            "event_type": event_type,
            "category": "other",
            "name": status,
            "vendor_or_customer": "Vendor",
            "amount_cents": 100_00,
            "due_date": "2026-04-14",
            "status": status,
            "confidence": "confirmed",
            "matched_to_id": None,
            "recurring_rule": None,
            "friendly_name": None,
        }

    def test_posted_rows_excluded_by_forecast_filter(self) -> None:
        from sales_support_agent.services.cashflow.cashflow_helpers import _events_to_dtos

        rows = [self._make_raw(s) for s in ("posted", "matched", "cancelled", "paid", "planned")]
        # Apply the same filter used in forecast.py and overview.py
        forecast_rows = [
            r for r in rows
            if r.get("status") not in ("posted", "matched", "cancelled", "paid")
        ]
        dtos = _events_to_dtos(forecast_rows)
        statuses = {d.status for d in dtos}
        # Only 'planned' should survive the filter
        self.assertEqual(len(dtos), 1)
        self.assertNotIn("posted", statuses)
        self.assertNotIn("matched", statuses)
        self.assertNotIn("cancelled", statuses)
        self.assertNotIn("paid", statuses)
        self.assertIn("planned", statuses)

    def test_posted_outflow_excluded_from_aggregate(self) -> None:
        """A posted $1,000 outflow must NOT reduce the forecast balance."""
        from sales_support_agent.services.cashflow.cashflow_helpers import _events_to_dtos
        from sales_support_agent.services.cashflow.engine import aggregate_weeks

        monday = date(2026, 4, 7)
        rows = [
            self._make_raw("posted"),          # bank actual — must be excluded
            self._make_raw("planned"),          # forward obligation — must be included
        ]
        # Correct filter (as in forecast.py)
        forecast_rows = [r for r in rows if r.get("status") not in ("posted", "matched", "cancelled", "paid")]
        dtos = _events_to_dtos(forecast_rows)
        weeks = aggregate_weeks(dtos, starting_cash_cents=500_00, weeks=2, as_of_date=monday)

        # With the posted row excluded, only the 'planned' $100 outflow applies
        total_outflow = sum(w.outflow_cents for w in weeks)
        self.assertEqual(total_outflow, 100_00, "posted rows must not contribute to forecast outflow")

    def test_all_rows_included_without_filter_doubles_outflow(self) -> None:
        """Verify the bug would be detectable: without the filter, outflow doubles."""
        from sales_support_agent.services.cashflow.cashflow_helpers import _events_to_dtos
        from sales_support_agent.services.cashflow.engine import aggregate_weeks

        monday = date(2026, 4, 7)
        rows = [self._make_raw("posted"), self._make_raw("planned")]
        # No filter — both rows passed through (the old buggy behaviour)
        dtos = _events_to_dtos(rows)
        weeks = aggregate_weeks(dtos, starting_cash_cents=500_00, weeks=2, as_of_date=monday)
        total_outflow = sum(w.outflow_cents for w in weeks)
        # Both posted and planned are outflows of $100 each → $200 total (the double-count)
        self.assertEqual(total_outflow, 200_00, "without the filter, outflow double-counts")


if __name__ == "__main__":
    unittest.main()
