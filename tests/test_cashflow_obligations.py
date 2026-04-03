"""Tests for obligations CRUD and recurring template generation."""

from __future__ import annotations

import unittest
from datetime import date
from unittest.mock import patch

from sqlalchemy import create_engine, text


def _make_engine():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE cash_events (
                id TEXT PRIMARY KEY,
                source TEXT,
                source_id TEXT,
                event_type TEXT,
                category TEXT,
                name TEXT,
                vendor_or_customer TEXT,
                amount_cents INTEGER,
                due_date TEXT,
                status TEXT,
                confidence TEXT,
                notes TEXT,
                account_balance_cents INTEGER,
                matched_to_id TEXT,
                recurring_template_id TEXT,
                clickup_task_id TEXT,
                recurring_rule TEXT,
                created_at TEXT,
                updated_at TEXT
            )
        """))
        conn.execute(text("""
            CREATE TABLE recurring_templates (
                id TEXT PRIMARY KEY,
                name TEXT,
                vendor_or_customer TEXT,
                event_type TEXT,
                category TEXT,
                amount_cents INTEGER,
                frequency TEXT,
                next_due_date TEXT,
                day_of_month INTEGER,
                is_active INTEGER DEFAULT 1,
                created_at TEXT,
                updated_at TEXT
            )
        """))
    return engine


class TestObligationsCRUD(unittest.TestCase):
    def setUp(self) -> None:
        self._engine = _make_engine()
        self._patcher = patch(
            "sales_support_agent.services.cashflow.obligations.engine",
            self._engine,
        )
        self._patcher.start()

    def tearDown(self) -> None:
        self._patcher.stop()

    def test_create_and_get(self) -> None:
        from sales_support_agent.services.cashflow.obligations import (
            create_obligation,
            get_obligation,
        )
        event = create_obligation(
            db=None,  # type: ignore
            name="Test Payable",
            event_type="outflow",
            category="other",
            amount_cents=50000,
            due_date=date(2026, 5, 1),
        )
        self.assertIsNotNone(event)
        fetched = get_obligation(event["id"])
        self.assertEqual(fetched["name"], "Test Payable")
        self.assertEqual(fetched["amount_cents"], 50000)

    def test_create_sets_source_manual(self) -> None:
        from sales_support_agent.services.cashflow.obligations import create_obligation
        event = create_obligation(
            db=None,  # type: ignore
            name="Manual",
            event_type="outflow",
            category="other",
            amount_cents=1000,
            due_date=date(2026, 5, 1),
        )
        self.assertEqual(event["source"], "manual")

    def test_update_obligation(self) -> None:
        from sales_support_agent.services.cashflow.obligations import (
            create_obligation,
            update_obligation,
        )
        event = create_obligation(
            db=None,  # type: ignore
            name="Original",
            event_type="outflow",
            category="other",
            amount_cents=1000,
            due_date=date(2026, 5, 1),
        )
        updated = update_obligation(event["id"], name="Updated", amount_cents=2000)
        self.assertEqual(updated["name"], "Updated")
        self.assertEqual(updated["amount_cents"], 2000)

    def test_delete_obligation(self) -> None:
        from sales_support_agent.services.cashflow.obligations import (
            create_obligation,
            delete_obligation,
            get_obligation,
        )
        event = create_obligation(
            db=None,  # type: ignore
            name="To Delete",
            event_type="outflow",
            category="other",
            amount_cents=1000,
            due_date=date(2026, 5, 1),
        )
        deleted = delete_obligation(event["id"])
        self.assertTrue(deleted)
        self.assertIsNone(get_obligation(event["id"]))

    def test_delete_returns_false_for_nonexistent(self) -> None:
        from sales_support_agent.services.cashflow.obligations import delete_obligation
        self.assertFalse(delete_obligation("nonexistent-id"))

    def test_list_obligations_filter_by_type(self) -> None:
        from sales_support_agent.services.cashflow.obligations import (
            create_obligation,
            list_obligations,
        )
        create_obligation(db=None, name="Out", event_type="outflow", category="other", amount_cents=100, due_date=date(2026, 5, 1))  # type: ignore
        create_obligation(db=None, name="In", event_type="inflow", category="other", amount_cents=200, due_date=date(2026, 5, 2))  # type: ignore
        outflows = list_obligations(event_type="outflow")
        self.assertEqual(len(outflows), 1)
        self.assertEqual(outflows[0]["name"], "Out")

    def test_get_returns_none_for_missing(self) -> None:
        from sales_support_agent.services.cashflow.obligations import get_obligation
        self.assertIsNone(get_obligation("does-not-exist"))


class TestRecurringTemplates(unittest.TestCase):
    def setUp(self) -> None:
        self._engine = _make_engine()
        self._patcher = patch(
            "sales_support_agent.services.cashflow.obligations.engine",
            self._engine,
        )
        self._patcher.start()

    def tearDown(self) -> None:
        self._patcher.stop()

    def test_create_and_get_template(self) -> None:
        from sales_support_agent.services.cashflow.obligations import (
            create_recurring_template,
            get_recurring_template,
        )
        t = create_recurring_template(
            name="Office Rent",
            vendor_or_customer="Boulder Ranch",
            event_type="outflow",
            category="rent",
            amount_cents=250000,
            frequency="monthly",
            next_due_date=date(2026, 5, 1),
        )
        self.assertIsNotNone(t)
        fetched = get_recurring_template(t["id"])
        self.assertEqual(fetched["name"], "Office Rent")
        self.assertEqual(fetched["amount_cents"], 250000)

    def test_list_active_only(self) -> None:
        from sales_support_agent.services.cashflow.obligations import (
            create_recurring_template,
            list_recurring_templates,
            update_recurring_template,
        )
        t1 = create_recurring_template(
            name="Active", event_type="outflow", category="other",
            amount_cents=100, frequency="monthly", next_due_date=date(2026, 5, 1),
        )
        t2 = create_recurring_template(
            name="Inactive", event_type="outflow", category="other",
            amount_cents=200, frequency="monthly", next_due_date=date(2026, 5, 1),
        )
        update_recurring_template(t2["id"], is_active=0)
        active = list_recurring_templates(active_only=True)
        names = [t["name"] for t in active]
        self.assertIn("Active", names)
        self.assertNotIn("Inactive", names)

    def test_generate_creates_event(self) -> None:
        from sales_support_agent.services.cashflow.obligations import (
            create_recurring_template,
            generate_upcoming_from_templates,
            list_obligations,
        )
        create_recurring_template(
            name="Monthly SaaS",
            event_type="outflow",
            category="software",
            amount_cents=10000,
            frequency="monthly",
            next_due_date=date(2026, 4, 5),  # within 90 days
        )
        created = generate_upcoming_from_templates(horizon_days=90, advance_template=False)
        self.assertGreater(len(created), 0)
        obligations = list_obligations()
        names = [o["name"] for o in obligations]
        self.assertIn("Monthly SaaS", names)

    def test_generate_skips_beyond_horizon(self) -> None:
        from sales_support_agent.services.cashflow.obligations import (
            create_recurring_template,
            generate_upcoming_from_templates,
            list_obligations,
        )
        create_recurring_template(
            name="Far Future",
            event_type="outflow",
            category="other",
            amount_cents=500,
            frequency="monthly",
            next_due_date=date(2030, 1, 1),  # way beyond 90 days
        )
        created = generate_upcoming_from_templates(horizon_days=90, advance_template=False)
        names = [e["name"] for e in created]
        self.assertNotIn("Far Future", names)


if __name__ == "__main__":
    unittest.main()
