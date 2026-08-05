from __future__ import annotations

import os
import tempfile

# Self-sufficient: these tests previously read SALES_AGENT_DB_URL straight from
# the environment, so they only passed when an earlier test file happened to
# set it. Alone they raised KeyError.
os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + os.path.join(tempfile.gettempdir(), "test_public_tool_alerts.db"),
)


import os
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from sales_support_agent.models.database import create_session_factory, init_database, session_scope
from sales_support_agent.models.entities import AutomationRun
from sales_support_agent.services.sales.public_tool_alerts import (
    ALERT_MARKER_KEY,
    send_public_tool_failure_alerts,
)


class TestPublicToolAlerts(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sf = create_session_factory(os.environ["SALES_AGENT_DB_URL"])
        init_database(cls.sf)

    def setUp(self):
        with session_scope(self.sf) as session:
            session.query(AutomationRun).delete()

    def _run(self, *, status="failed", summary=None, metadata=None, run_type="marketing_intake"):
        with session_scope(self.sf) as session:
            row = AutomationRun(
                run_type=run_type,
                status=status,
                metadata_json=metadata or {"email": "visitor@example.com", "token": "secret"},
                summary_json=summary or {"error": "visitor@example.com secret"},
            )
            session.add(row)
            session.flush()
            return row.id

    def _send(self, client):
        with patch(
            "sales_support_agent.services.sales.public_tool_alerts.SlackClient",
            return_value=client,
        ):
            with session_scope(self.sf) as session:
                return send_public_tool_failure_alerts(
                    session, SimpleNamespace(slack_bot_token="x", slack_channel_id="C1")
                )

    def test_posts_pii_free_digest_once_for_same_state(self):
        run_id = self._run()
        client = MagicMock()
        client.is_configured.return_value = True
        client.post_message.return_value = {"ok": True}

        first = self._send(client)
        second = self._send(client)

        self.assertTrue(first["sent"])
        self.assertTrue(second["skipped"])
        client.post_message.assert_called_once()
        payload = str(client.post_message.call_args.kwargs)
        self.assertIn(f"run `{run_id}`", payload)
        self.assertIn("report build failed", payload)
        self.assertNotIn("visitor@example.com", payload)
        self.assertNotIn("secret", payload)
        with session_scope(self.sf) as session:
            marker = session.get(AutomationRun, run_id).summary_json[ALERT_MARKER_KEY]
            self.assertTrue(marker["fingerprint"])

    def test_changed_failure_state_posts_again(self):
        run_id = self._run(summary={"email_delivery": "failed"}, status="success")
        client = MagicMock()
        client.is_configured.return_value = True
        client.post_message.return_value = {"ok": True}
        self._send(client)
        with session_scope(self.sf) as session:
            row = session.get(AutomationRun, run_id)
            row.summary_json = {**row.summary_json, "hubspot_handoff": "failed"}
        result = self._send(client)
        self.assertTrue(result["sent"])
        self.assertEqual(client.post_message.call_count, 2)

    def test_advertising_and_rate_sheet_are_classified(self):
        self._run(
            run_type="marketing_analysis_intake",
            metadata={"tool": "advertising_audit", "email": "a@example.com"},
        )
        self._run(
            run_type="fulfillment_rate_sheet",
            status="success",
            summary={
                "public_unlock_email": "rates@example.com",
                "public_rate_sheet_status": "ready",
                "public_email_status": "failed",
            },
        )
        client = MagicMock()
        client.is_configured.return_value = True
        client.post_message.return_value = {"ok": True}
        result = self._send(client)
        self.assertEqual(result["run_count"], 2)
        payload = str(client.post_message.call_args.kwargs)
        self.assertIn("Advertising Audit", payload)
        self.assertIn("Rate Sheet", payload)
        self.assertNotIn("a@example.com", payload)
        self.assertNotIn("rates@example.com", payload)

    def test_unconfigured_slack_does_not_mark_run(self):
        run_id = self._run()
        client = MagicMock()
        client.is_configured.return_value = False
        result = self._send(client)
        self.assertTrue(result["skipped"])
        with session_scope(self.sf) as session:
            self.assertNotIn(ALERT_MARKER_KEY, session.get(AutomationRun, run_id).summary_json)


if __name__ == "__main__":
    unittest.main()
