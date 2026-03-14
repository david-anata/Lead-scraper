from __future__ import annotations

import unittest
from datetime import date, datetime, timezone
from types import SimpleNamespace

try:
    from sales_support_agent.config import DEFAULT_STATUS_POLICIES, build_normalized_status_policies, normalize_status_key
    from sales_support_agent.models.database import create_session_factory, init_database, session_scope
    from sales_support_agent.models.entities import LeadMirror, MailboxSignal
    from sales_support_agent.services.admin_dashboard import build_dashboard_data

    SQLALCHEMY_AVAILABLE = True
except ModuleNotFoundError as exc:
    if exc.name != "sqlalchemy":
        raise
    SQLALCHEMY_AVAILABLE = False


@unittest.skipUnless(SQLALCHEMY_AVAILABLE, "sqlalchemy is required for dashboard tests")
class AdminDashboardTests(unittest.TestCase):
    def test_build_dashboard_data_groups_items_by_owner_and_priority(self) -> None:
        session_factory = create_session_factory("sqlite:///:memory:")
        init_database(session_factory)

        with session_scope(session_factory) as session:
            session.add(
                LeadMirror(
                    clickup_task_id="task-1",
                    list_id="list-123",
                    task_name="ACME Corp",
                    task_url="https://app.clickup.com/t/task-1",
                    status="new lead",
                    assignee_id="owner-1",
                    assignee_name="Valeria Morales",
                    created_at=datetime(2026, 3, 10, 12, 0, tzinfo=timezone.utc),
                    last_sync_at=datetime(2026, 3, 14, 11, 0, tzinfo=timezone.utc),
                )
            )
            session.add(
                MailboxSignal(
                    provider="gmail",
                    external_message_id="msg-1",
                    external_thread_id="thread-1",
                    dedupe_key="gmail_message:msg-1",
                    matched_task_id="task-1",
                    sender_email="buyer@example.com",
                    sender_domain="example.com",
                    subject="Need pricing",
                    classification="pricing_or_offer_request",
                    urgency="needs_immediate_review",
                    owner_name="David Narayan",
                    task_name="Globex",
                    task_url="https://app.clickup.com/t/task-2",
                    action_summary="Send pricing today.",
                    suggested_reply_draft="Thanks. I can send pricing today.",
                    received_at=datetime(2026, 3, 14, 13, 0, tzinfo=timezone.utc),
                    processed_at=datetime(2026, 3, 14, 13, 5, tzinfo=timezone.utc),
                )
            )

        settings = SimpleNamespace(
            clickup_list_id="list-123",
            active_statuses=(normalize_status_key("new lead"),),
            inactive_statuses=(),
            status_policies=build_normalized_status_policies(DEFAULT_STATUS_POLICIES),
            slack_assignee_map={},
            stale_lead_immediate_alert_urgencies=(),
            stale_lead_slack_digest_max_items=20,
            stale_lead_slack_digest_mention_channel=False,
        )

        with session_scope(session_factory) as session:
            dashboard = build_dashboard_data(
                settings=settings,
                session=session,
                lead_builder_status={"ready": True, "missing": []},
                as_of_date=date(2026, 3, 14),
            )

        self.assertEqual(dashboard.total_active_leads, 1)
        self.assertEqual(dashboard.mailbox_findings, 1)
        self.assertEqual(dashboard.stale_counts["overdue"], 1)
        self.assertEqual(dashboard.owner_queues[0].owner_name, "Valeria Morales")
        owner_names = [queue.owner_name for queue in dashboard.owner_queues]
        self.assertIn("David Narayan", owner_names)


if __name__ == "__main__":
    unittest.main()
