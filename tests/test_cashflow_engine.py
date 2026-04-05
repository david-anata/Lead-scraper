"""Tests for cashflow engine: aggregate_weeks, flag_risks, apply_scenario."""

from __future__ import annotations

import unittest
from datetime import date, timedelta

from sales_support_agent.services.cashflow.engine import (
    EventDTO,
    ScenarioAdjustment,
    WeekBucket,
    aggregate_weeks,
    apply_scenario,
    flag_risks,
)


def _event(
    event_type: str = "outflow",
    amount_cents: int = 10000,
    due_date: date | None = None,
    status: str = "planned",
    event_id: str = "e1",
    source: str = "manual",
    category: str = "other",
) -> EventDTO:
    return EventDTO(
        id=event_id,
        source=source,
        event_type=event_type,
        category=category,
        name="Test Event",
        vendor_or_customer="Test Vendor",
        amount_cents=amount_cents,
        due_date=due_date or date(2026, 4, 7),  # Monday
        status=status,
        confidence="confirmed",
        matched_to_id=None,
        recurring_rule=None,
    )


class TestAggregateWeeks(unittest.TestCase):
    def test_empty_events_returns_weeks_with_starting_balance(self) -> None:
        weeks = aggregate_weeks([], starting_cash_cents=500_00, weeks=4)
        self.assertEqual(len(weeks), 4)
        for w in weeks:
            self.assertEqual(w.starting_cash_cents, 50000)
            self.assertEqual(w.inflow_cents, 0)
            self.assertEqual(w.outflow_cents, 0)
            self.assertEqual(w.net_cents, 0)
            self.assertEqual(w.ending_cash_cents, 50000)

    def test_outflow_reduces_cash(self) -> None:
        # Week starting 2026-04-06 (Monday)
        monday = date(2026, 4, 6)
        e = _event(event_type="outflow", amount_cents=100_00, due_date=monday)
        weeks = aggregate_weeks([e], starting_cash_cents=500_00, weeks=4, as_of_date=monday)
        self.assertEqual(weeks[0].outflow_cents, 100_00)
        self.assertEqual(weeks[0].net_cents, -100_00)
        self.assertEqual(weeks[0].ending_cash_cents, 400_00)

    def test_inflow_increases_cash(self) -> None:
        monday = date(2026, 4, 6)
        e = _event(event_type="inflow", amount_cents=200_00, due_date=monday)
        weeks = aggregate_weeks([e], starting_cash_cents=100_00, weeks=4, as_of_date=monday)
        self.assertEqual(weeks[0].inflow_cents, 200_00)
        self.assertEqual(weeks[0].ending_cash_cents, 300_00)

    def test_rolling_balance_carries_forward(self) -> None:
        monday = date(2026, 4, 6)
        next_monday = monday + timedelta(weeks=1)
        e1 = _event(event_type="outflow", amount_cents=50_00, due_date=monday, event_id="e1")
        e2 = _event(event_type="outflow", amount_cents=30_00, due_date=next_monday, event_id="e2")
        weeks = aggregate_weeks([e1, e2], starting_cash_cents=200_00, weeks=4, as_of_date=monday)
        self.assertEqual(weeks[0].ending_cash_cents, 150_00)
        self.assertEqual(weeks[1].starting_cash_cents, 150_00)
        self.assertEqual(weeks[1].ending_cash_cents, 120_00)

    def test_cancelled_events_excluded(self) -> None:
        monday = date(2026, 4, 6)
        e = _event(event_type="outflow", amount_cents=500_00, due_date=monday, status="cancelled")
        weeks = aggregate_weeks([e], starting_cash_cents=100_00, weeks=2, as_of_date=monday)
        self.assertEqual(weeks[0].outflow_cents, 0)

    def test_weeks_align_to_monday(self) -> None:
        monday = date(2026, 4, 6)
        weeks = aggregate_weeks([], starting_cash_cents=0, weeks=3, as_of_date=monday)
        for i, w in enumerate(weeks):
            self.assertEqual(w.week_start.weekday(), 0, f"Week {i} does not start on Monday")

    def test_is_negative_flag(self) -> None:
        monday = date(2026, 4, 6)
        e = _event(event_type="outflow", amount_cents=1000_00, due_date=monday)
        weeks = aggregate_weeks([e], starting_cash_cents=500_00, weeks=2, as_of_date=monday)
        # Week 0: starting=500, outflow=1000, ending=-500 → negative
        self.assertTrue(weeks[0].is_negative)
        # Week 1: starts at -500 (carries forward), no events → ending=-500, still negative
        self.assertTrue(weeks[1].is_negative)
        # With enough inflow to recover, week becomes positive
        e_in = _event(event_type="inflow", amount_cents=2000_00, due_date=monday + timedelta(weeks=1), event_id="e2")
        weeks2 = aggregate_weeks([e, e_in], starting_cash_cents=500_00, weeks=2, as_of_date=monday)
        self.assertTrue(weeks2[0].is_negative)
        self.assertFalse(weeks2[1].is_negative)

    def test_week_label_format(self) -> None:
        weeks = aggregate_weeks([], starting_cash_cents=0, weeks=1, as_of_date=date(2026, 4, 6))
        self.assertIn("Apr", weeks[0].label)


class TestFlagRisks(unittest.TestCase):
    def test_no_alerts_when_healthy(self) -> None:
        monday = date(2026, 4, 6)
        events = [_event(event_type="inflow", amount_cents=1000_00, due_date=monday)]
        weeks = aggregate_weeks(events, starting_cash_cents=5000_00, weeks=4, as_of_date=monday)
        alerts = flag_risks(weeks, events)
        critical = [a for a in alerts if a.severity == "critical"]
        self.assertEqual(critical, [])

    def test_negative_week_triggers_critical_alert(self) -> None:
        monday = date(2026, 4, 6)
        e = _event(event_type="outflow", amount_cents=1000_00, due_date=monday)
        weeks = aggregate_weeks([e], starting_cash_cents=500_00, weeks=4, as_of_date=monday)
        alerts = flag_risks(weeks, [e])
        alert_types = {a.alert_type for a in alerts}
        self.assertIn("negative_week", alert_types)
        critical = [a for a in alerts if a.severity == "critical"]
        self.assertTrue(len(critical) > 0)

    def test_overdue_outflow_triggers_alert(self) -> None:
        # past due date, still in planned status
        last_week = date(2026, 3, 30)  # a Monday in the past
        e = _event(
            event_type="outflow",
            amount_cents=500_00,
            due_date=last_week,
            status="planned",
            event_id="overdue1",
        )
        weeks = aggregate_weeks([e], starting_cash_cents=1000_00, weeks=4, as_of_date=date(2026, 4, 6))
        alerts = flag_risks(weeks, [e], as_of_date=date(2026, 4, 6))
        alert_types = {a.alert_type for a in alerts}
        self.assertIn("overdue", alert_types)

    def test_alerts_sorted_critical_first(self) -> None:
        monday = date(2026, 4, 6)
        big_out = _event(event_type="outflow", amount_cents=5000_00, due_date=monday, event_id="big")
        weeks = aggregate_weeks([big_out], starting_cash_cents=100_00, weeks=4, as_of_date=monday)
        alerts = flag_risks(weeks, [big_out])
        if len(alerts) > 1:
            severities = [a.severity for a in alerts]
            order = {"critical": 0, "warning": 1, "info": 2}
            for i in range(len(severities) - 1):
                self.assertLessEqual(
                    order[severities[i]], order[severities[i + 1]],
                    "Alerts not sorted critical → warning → info",
                )


class TestFlagRisksAdditional(unittest.TestCase):
    """Tests for the three flag_risks alert types not covered by TestFlagRisks:
    duplicate, outlier, and large_outflow."""

    def test_duplicate_outflows_trigger_warning(self) -> None:
        """Two outflows to the same vendor, same amount, within 7 days → duplicate warning."""
        monday = date(2026, 4, 6)
        e1 = _event(
            event_type="outflow", amount_cents=500_00, due_date=monday,
            event_id="dup1", source="manual", category="software",
        )
        e1.vendor_or_customer = "Acme SaaS"
        e2 = _event(
            event_type="outflow", amount_cents=500_00, due_date=monday + timedelta(days=3),
            event_id="dup2", source="manual", category="software",
        )
        e2.vendor_or_customer = "Acme SaaS"
        weeks = aggregate_weeks([e1, e2], starting_cash_cents=5000_00, weeks=4, as_of_date=monday)
        alerts = flag_risks(weeks, [e1, e2], as_of_date=monday)
        alert_types = {a.alert_type for a in alerts}
        self.assertIn("duplicate", alert_types, "Expected a duplicate alert for same vendor+amount within 7 days")
        dup_alerts = [a for a in alerts if a.alert_type == "duplicate"]
        self.assertEqual(dup_alerts[0].severity, "warning")

    def test_no_duplicate_when_amounts_differ_significantly(self) -> None:
        """Two outflows to the same vendor but very different amounts → no duplicate."""
        monday = date(2026, 4, 6)
        e1 = _event(event_type="outflow", amount_cents=100_00, due_date=monday, event_id="d1")
        e1.vendor_or_customer = "Widget Corp"
        e2 = _event(event_type="outflow", amount_cents=500_00, due_date=monday + timedelta(days=2), event_id="d2")
        e2.vendor_or_customer = "Widget Corp"
        weeks = aggregate_weeks([e1, e2], starting_cash_cents=5000_00, weeks=4, as_of_date=monday)
        alerts = flag_risks(weeks, [e1, e2], as_of_date=monday)
        alert_types = {a.alert_type for a in alerts}
        self.assertNotIn("duplicate", alert_types)

    def test_outlier_amount_triggers_warning(self) -> None:
        """An outflow 4× the category median triggers an outlier warning."""
        monday = date(2026, 4, 6)
        # Three 'normal' software expenses at $100 establish the median
        normals = [
            _event(event_type="outflow", amount_cents=100_00,
                   due_date=monday + timedelta(weeks=i), event_id=f"n{i}", category="software")
            for i in range(3)
        ]
        # One spike at $500 (5× the median)
        spike = _event(
            event_type="outflow", amount_cents=500_00,
            due_date=monday + timedelta(weeks=3), event_id="spike", category="software",
        )
        all_events = normals + [spike]
        weeks = aggregate_weeks(all_events, starting_cash_cents=10000_00, weeks=6, as_of_date=monday)
        alerts = flag_risks(weeks, all_events, as_of_date=monday, outlier_multiplier=3.0)
        alert_types = {a.alert_type for a in alerts}
        self.assertIn("outlier", alert_types, "Expected an outlier alert for 5× median outflow")
        outlier_alerts = [a for a in alerts if a.alert_type == "outlier"]
        self.assertEqual(outlier_alerts[0].severity, "warning")

    def test_no_outlier_with_fewer_than_three_samples(self) -> None:
        """Need at least 3 category samples for outlier detection — fewer samples → no alert."""
        monday = date(2026, 4, 6)
        e1 = _event(event_type="outflow", amount_cents=100_00, due_date=monday, event_id="e1", category="rent")
        e2 = _event(event_type="outflow", amount_cents=500_00, due_date=monday + timedelta(weeks=1), event_id="e2", category="rent")
        weeks = aggregate_weeks([e1, e2], starting_cash_cents=5000_00, weeks=4, as_of_date=monday)
        alerts = flag_risks(weeks, [e1, e2], as_of_date=monday)
        alert_types = {a.alert_type for a in alerts}
        self.assertNotIn("outlier", alert_types)

    def test_large_outflow_triggers_info_alert(self) -> None:
        """A single outflow above the $2,000 threshold triggers a large_outflow info alert."""
        monday = date(2026, 4, 6)
        big = _event(
            event_type="outflow", amount_cents=250_000,  # $2,500
            due_date=monday, event_id="big1",
        )
        weeks = aggregate_weeks([big], starting_cash_cents=100_000_00, weeks=4, as_of_date=monday)
        alerts = flag_risks(weeks, [big], as_of_date=monday, large_outflow_threshold_cents=200_000)
        alert_types = {a.alert_type for a in alerts}
        self.assertIn("large_outflow", alert_types)
        large_alerts = [a for a in alerts if a.alert_type == "large_outflow"]
        self.assertEqual(large_alerts[0].severity, "info")

    def test_small_outflow_does_not_trigger_large_outflow_alert(self) -> None:
        monday = date(2026, 4, 6)
        small = _event(event_type="outflow", amount_cents=50_00, due_date=monday, event_id="sm1")
        weeks = aggregate_weeks([small], starting_cash_cents=5000_00, weeks=4, as_of_date=monday)
        alerts = flag_risks(weeks, [small], as_of_date=monday, large_outflow_threshold_cents=200_000)
        alert_types = {a.alert_type for a in alerts}
        self.assertNotIn("large_outflow", alert_types)


class TestApplyScenario(unittest.TestCase):
    def test_remove_event(self) -> None:
        e = _event(event_id="e1")
        adj = ScenarioAdjustment(event_id="e1", new_amount_cents=None, new_due_date=None, remove=True)
        result = apply_scenario([e], [adj])
        self.assertEqual(len(result), 0)

    def test_change_amount(self) -> None:
        e = _event(event_id="e1", amount_cents=100_00)
        adj = ScenarioAdjustment(event_id="e1", new_amount_cents=50_00, new_due_date=None, remove=False)
        result = apply_scenario([e], [adj])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].amount_cents, 50_00)

    def test_change_due_date(self) -> None:
        e = _event(event_id="e1", due_date=date(2026, 4, 7))
        new_date = date(2026, 5, 1)
        adj = ScenarioAdjustment(event_id="e1", new_amount_cents=None, new_due_date=new_date, remove=False)
        result = apply_scenario([e], [adj])
        self.assertEqual(result[0].due_date, new_date)

    def test_no_adjustment_leaves_original(self) -> None:
        e = _event(event_id="e1", amount_cents=99_00)
        result = apply_scenario([e], [])
        self.assertEqual(result[0].amount_cents, 99_00)

    def test_does_not_mutate_originals(self) -> None:
        e = _event(event_id="e1", amount_cents=100_00)
        adj = ScenarioAdjustment(event_id="e1", new_amount_cents=1_00, new_due_date=None, remove=False)
        apply_scenario([e], [adj])
        self.assertEqual(e.amount_cents, 100_00)

    def test_unknown_event_id_ignored(self) -> None:
        e = _event(event_id="e1", amount_cents=100_00)
        adj = ScenarioAdjustment(event_id="nonexistent", new_amount_cents=1_00, new_due_date=None, remove=False)
        result = apply_scenario([e], [adj])
        self.assertEqual(result[0].amount_cents, 100_00)


if __name__ == "__main__":
    unittest.main()
