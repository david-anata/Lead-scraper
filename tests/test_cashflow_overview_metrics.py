"""Tests for compute_finance_overview — verifies metric classification logic."""
from __future__ import annotations

import unittest
from dataclasses import dataclass
from datetime import date, timedelta
from types import SimpleNamespace

from sales_support_agent.services.cashflow.engine import EventDTO, RiskAlert
from sales_support_agent.services.cashflow.overview import compute_finance_overview


_TODAY = date(2026, 4, 3)


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


if __name__ == "__main__":
    unittest.main()
