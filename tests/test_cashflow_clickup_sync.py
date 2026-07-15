from __future__ import annotations

import unittest
from datetime import date
from unittest.mock import MagicMock, patch

from sqlalchemy import create_engine, text

from sales_support_agent.models.database import create_session_factory, init_database, upsert_cash_event
from sales_support_agent.services.cashflow.clickup_sync import (
    _fetch_tasks,
    _match_existing_posted_transactions,
    _quarantine_legacy_clickup_template_expansions,
    _quarantine_probable_clickup_duplicates,
    _record_successful_list_snapshot,
    _task_to_event_dict,
    sync_clickup_finance,
)
from sales_support_agent.services.cashflow.control import build_forecast_paths, build_queue


class FetchTasksTests(unittest.TestCase):
    def test_fetch_tasks_uses_supported_clickup_query_params(self) -> None:
        first = MagicMock()
        first.json.return_value = {"tasks": [{"id": "task-1"}], "last_page": False}
        first.raise_for_status.return_value = None

        second = MagicMock()
        second.json.return_value = {"tasks": [{"id": "task-2"}], "last_page": True}
        second.raise_for_status.return_value = None

        with patch(
            "sales_support_agent.services.cashflow.clickup_sync.requests.get",
            side_effect=[first, second],
        ) as mock_get:
            tasks = _fetch_tasks("token-123", "901104880724")

        self.assertEqual(tasks, [{"id": "task-1"}, {"id": "task-2"}])
        self.assertEqual(mock_get.call_count, 2)

        first_call = mock_get.call_args_list[0]
        self.assertEqual(
            first_call.args[0],
            "https://api.clickup.com/api/v2/list/901104880724/task",
        )
        self.assertEqual(first_call.kwargs["headers"], {"Authorization": "token-123"})
        self.assertEqual(
            first_call.kwargs["params"],
            {
                "include_closed": "true",
                "subtasks": "false",
                "page": 0,
            },
        )
        self.assertNotIn("custom_fields", first_call.kwargs["params"])


class ClickUpFinanceSyncTests(unittest.TestCase):
    def _finance_task(self, **overrides) -> dict:
        task = {
            "id": "closed-task-1",
            "name": "Payroll Reserve 20th",
            "status": {"status": "Complete", "type": "closed"},
            "date_closed": "1782864000000",
            "priority": None,
            "custom_fields": [
                {"id": "6d61ee15-5e93-4b5f-8945-93a96659049e", "value": 5000},
            ],
            "due_date": "1772323200000",
        }
        task.update(overrides)
        return task

    def test_closed_task_uses_completed_lifecycle_not_paid(self) -> None:
        event = _task_to_event_dict(
            self._finance_task(), "outflow", date(2026, 7, 15)
        )

        self.assertEqual(event["status"], "completed")
        self.assertEqual(event["source_status"], "complete")
        self.assertIsNone(event["source_open_amount_cents"])
        self.assertTrue(event["apply_source_lifecycle"])
        self.assertNotEqual(event["status"], "paid")

    def test_open_task_still_uses_due_date_lifecycle(self) -> None:
        event = _task_to_event_dict(
            self._finance_task(status={"status": "Open", "type": "open"}),
            "outflow",
            date(2026, 7, 15),
        )

        self.assertEqual(event["status"], "overdue")
        self.assertEqual(event["source_open_amount_cents"], 500000)

    def test_clickup_completion_does_not_overwrite_bank_settlement(self) -> None:
        factory = create_session_factory("sqlite:///:memory:")
        init_database(factory)
        engine = factory.kw["bind"]
        with engine.begin() as conn:
            upsert_cash_event(conn, {
                "id": "clickup-settled", "source": "clickup", "source_id": "task-settled",
                "record_kind": "obligation", "event_type": "outflow", "amount_cents": 500000,
                "due_date": date(2026, 7, 1), "status": "paid", "source_status": "open",
            })
            upsert_cash_event(conn, {
                "id": "clickup-settled", "source": "clickup", "source_id": "task-settled",
                "record_kind": "obligation", "event_type": "outflow", "amount_cents": 500000,
                "due_date": date(2026, 7, 1), "status": "completed", "source_status": "complete",
                "source_open_amount_cents": 0, "preserve_settlement_truth": True,
                "apply_source_lifecycle": True,
            })
        with engine.connect() as conn:
            self.assertEqual(
                conn.execute(text("SELECT status FROM cash_events WHERE id='clickup-settled'")).scalar_one(),
                "paid",
            )

    def test_completed_task_is_audit_visible_but_not_forecasted(self) -> None:
        rows = [{
            "id": "completed-payroll", "source": "clickup", "record_kind": "obligation",
            "event_type": "outflow", "status": "completed", "amount_cents": 500000,
            "due_date": date(2026, 7, 1), "vendor_or_customer": "Payroll",
            "source_status": "complete",
        }]
        forecast = build_forecast_paths(
            rows, as_of=date(2026, 7, 15), starting_cash_cents=1_000_000,
        )
        queue = build_queue(rows, as_of=date(2026, 7, 15))

        self.assertEqual(forecast["minimum_committed_cash_cents"], 1_000_000)
        self.assertEqual(queue["count"], 1)
        self.assertEqual(queue["items"][0]["status"], "completed")
        self.assertEqual(queue["items"][0]["action_label"], "Completed in ClickUp")

    def test_missing_task_requires_two_successful_snapshots_before_flagging(self) -> None:
        factory = create_session_factory("sqlite:///:memory:")
        init_database(factory)
        engine = factory.kw["bind"]
        with engine.begin() as conn:
            upsert_cash_event(conn, {
                "id": "clickup-missing", "source": "clickup", "source_id": "missing-task",
                "record_kind": "obligation", "event_type": "outflow", "amount_cents": 500000,
                "due_date": date(2026, 7, 20), "status": "planned", "source_status": "open",
            })

        self.assertEqual(_record_successful_list_snapshot(engine, "outflow", {"current-task"}), 0)
        self.assertEqual(_record_successful_list_snapshot(engine, "outflow", {"current-task"}), 0)
        self.assertEqual(_record_successful_list_snapshot(engine, "outflow", {"current-task"}), 1)
        with engine.connect() as conn:
            self.assertEqual(
                conn.execute(text("SELECT source_status FROM cash_events WHERE id='clickup-missing'")).scalar_one(),
                "source_missing",
            )

    def test_quarantine_cancels_generated_rows_and_deactivates_templates(self) -> None:
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE cash_events (
                    id TEXT PRIMARY KEY,
                    source TEXT,
                    recurring_template_id TEXT,
                    status TEXT,
                    notes TEXT,
                    updated_at TEXT
                )
            """))
            conn.execute(text("""
                CREATE TABLE recurring_templates (
                    id TEXT PRIMARY KEY,
                    is_active BOOLEAN,
                    updated_at TEXT
                )
            """))
            conn.execute(text("""
                INSERT INTO cash_events
                    (id, source, recurring_template_id, status, notes)
                VALUES
                    ('generated', 'manual', 'clickup-tmpl-task-1', 'planned', ''),
                    ('manual', 'manual', NULL, 'planned', ''),
                    ('posted', 'csv', NULL, 'posted', '')
            """))
            conn.execute(text("""
                INSERT INTO recurring_templates (id, is_active)
                VALUES ('clickup-tmpl-task-1', TRUE), ('manual-template', TRUE)
            """))

        counts = _quarantine_legacy_clickup_template_expansions(engine)

        self.assertEqual(counts, (1, 1))
        with engine.connect() as conn:
            rows = {
                row.id: row
                for row in conn.execute(
                    text("SELECT id, status, notes FROM cash_events ORDER BY id")
                )
            }
            templates = {
                row.id: row.is_active
                for row in conn.execute(
                    text("SELECT id, is_active FROM recurring_templates ORDER BY id")
                )
            }
        self.assertEqual(rows["generated"].status, "cancelled")
        self.assertIn("quarantined:legacy-clickup-template-expansion", rows["generated"].notes)
        self.assertEqual(rows["manual"].status, "planned")
        self.assertEqual(rows["posted"].status, "posted")
        self.assertFalse(templates["clickup-tmpl-task-1"])
        self.assertTrue(templates["manual-template"])

    def test_existing_posted_bank_row_matches_new_clickup_obligation(self) -> None:
        factory = create_session_factory("sqlite:///:memory:")
        init_database(factory)
        engine = factory.kw["bind"]
        with engine.begin() as conn:
            upsert_cash_event(conn, {
                "id": "bank-row",
                "source": "csv",
                "source_id": "bank-row",
                "record_kind": "transaction",
                "event_type": "outflow",
                "vendor_or_customer": "VON",
                "amount_cents": 110000,
                "due_date": date(2026, 7, 6),
                "category": "fulfillment",
                "status": "posted",
            })
            upsert_cash_event(conn, {
                "id": "clickup-row",
                "source": "clickup",
                "source_id": "clickup-row",
                "record_kind": "obligation",
                "event_type": "outflow",
                "vendor_or_customer": "Fulfillment Pay - Von",
                "amount_cents": 110000,
                "due_date": date(2026, 7, 6),
                "category": "fulfillment",
                "status": "overdue",
            })

        rows = [
            {
                "id": "bank-row",
                "source": "csv",
                "status": "posted",
                "event_type": "outflow",
                "vendor_or_customer": "VON",
                "amount_cents": 110000,
                "due_date": date(2026, 7, 6),
                "category": "fulfillment",
            },
            {
                "id": "clickup-row",
                "source": "clickup",
                "status": "overdue",
                "event_type": "outflow",
                "vendor_or_customer": "Fulfillment Pay - Von",
                "amount_cents": 110000,
                "due_date": date(2026, 7, 6),
                "category": "fulfillment",
            },
        ]
        with patch(
            "sales_support_agent.services.cashflow.obligations.list_obligations",
            return_value=rows,
        ):
            matched = _match_existing_posted_transactions(engine)

        self.assertEqual(matched, 1)
        with engine.connect() as conn:
            bank = conn.execute(
                text("SELECT status, matched_to_id FROM cash_events WHERE id='bank-row'")
            ).one()
            planned = conn.execute(
                text("SELECT status FROM cash_events WHERE id='clickup-row'")
            ).one()
        self.assertEqual(bank.status, "matched")
        self.assertEqual(bank.matched_to_id, "clickup-row")
        self.assertEqual(planned.status, "paid")
        with engine.connect() as conn:
            allocation_count = conn.execute(
                text("SELECT COUNT(*) FROM settlement_allocations")
            ).scalar_one()
        self.assertEqual(allocation_count, 1)

    def test_exact_same_day_clickup_tasks_are_quarantined_as_probable_duplicates(self) -> None:
        factory = create_session_factory("sqlite:///:memory:")
        init_database(factory)
        engine = factory.kw["bind"]
        with engine.begin() as conn:
            for event_id, source_id, updated_at in (
                ("clickup-old", "task-old", "2026-07-15T08:00:00"),
                ("clickup-new", "task-new", "2026-07-15T09:00:00"),
            ):
                upsert_cash_event(conn, {
                    "id": event_id, "source": "clickup", "source_id": source_id,
                    "record_kind": "obligation", "event_type": "outflow",
                    "vendor_or_customer": "Fulfillment Pay - Von",
                    "name": "Fulfillment Pay - Von", "amount_cents": 110000,
                    "due_date": date(2026, 6, 22), "category": "fulfillment",
                    "status": "overdue", "source_status": "open",
                    "source_updated_at": updated_at, "recurring_rule": "weekly",
                })

        self.assertEqual(_quarantine_probable_clickup_duplicates(engine), 1)
        with engine.connect() as conn:
            statuses = {
                row.id: (row.source_status, row.match_status)
                for row in conn.execute(text("""
                    SELECT id, source_status, match_status
                    FROM cash_events ORDER BY id
                """))
            }
        self.assertEqual(statuses["clickup-old"], ("probable_duplicate", "duplicate"))
        self.assertEqual(statuses["clickup-new"], ("open", ""))

    def test_recurring_task_is_upserted_once_as_a_cash_event(self) -> None:
        settings = MagicMock(
            clickup_api_token="pk_test",
            clickup_ap_list_id="ap-list",
            clickup_ar_list_id="",
        )
        task = {
            "id": "task-1",
            "name": "Weekly Vendor",
            "status": {"status": "open"},
            "priority": None,
            "custom_fields": [
                {"id": "6d61ee15-5e93-4b5f-8945-93a96659049e", "value": 100},
                {"id": "6c6390ee-76ab-4071-8b0a-c60c883a1cc1", "value": 0},
            ],
            "due_date": "1782864000000",
        }
        engine = MagicMock()

        with (
            patch(
                "sales_support_agent.services.cashflow.clickup_sync._fetch_tasks",
                return_value=[task],
            ),
            patch(
                "sales_support_agent.services.cashflow.clickup_sync._quarantine_legacy_clickup_template_expansions",
                return_value=(0, 0),
            ),
            patch(
                "sales_support_agent.services.cashflow.clickup_sync._match_existing_posted_transactions",
                return_value=0,
            ),
            patch(
                "sales_support_agent.services.cashflow.clickup_sync._quarantine_probable_clickup_duplicates",
                return_value=0,
            ),
            patch(
                "sales_support_agent.services.cashflow.clickup_sync._record_successful_list_snapshot",
                return_value=0,
            ),
            patch(
                "sales_support_agent.services.cashflow.clickup_sync._upsert_event",
                return_value="created",
            ) as upsert_event,
            patch(
                "sales_support_agent.models.database.get_engine",
                return_value=engine,
            ),
        ):
            result = sync_clickup_finance(settings)

        self.assertEqual(result.rows_inserted, 1)
        upsert_event.assert_called_once()
        event = upsert_event.call_args.args[1]
        self.assertEqual(event["clickup_task_id"], "task-1")
        self.assertEqual(event["recurring_rule"], "weekly")


if __name__ == "__main__":
    unittest.main()
