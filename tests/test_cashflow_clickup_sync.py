from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from sales_support_agent.services.cashflow.clickup_sync import _fetch_tasks


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


if __name__ == "__main__":
    unittest.main()
