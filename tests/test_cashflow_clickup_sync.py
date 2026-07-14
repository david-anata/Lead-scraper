from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from sqlalchemy import create_engine, text

from sales_support_agent.services.cashflow.clickup_sync import (
    _fetch_tasks,
    _quarantine_legacy_clickup_template_expansions,
    sync_clickup_finance,
)


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
