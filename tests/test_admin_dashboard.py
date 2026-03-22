from __future__ import annotations

import unittest
from datetime import date, datetime, timezone
from types import SimpleNamespace

try:
    from sales_support_agent.config import DEFAULT_STATUS_POLICIES, build_normalized_status_policies, normalize_status_key
    from sales_support_agent.models.database import create_session_factory, init_database, session_scope
    from sales_support_agent.models.entities import LeadMirror, MailboxSignal
    from sales_support_agent.services.admin_dashboard import (
        build_dashboard_data,
        dashboard_data_from_dict,
        dashboard_data_to_dict,
        render_dashboard_page,
    )

    SQLALCHEMY_AVAILABLE = True
except ModuleNotFoundError as exc:
    if exc.name != "sqlalchemy":
        raise
    SQLALCHEMY_AVAILABLE = False


@unittest.skipUnless(SQLALCHEMY_AVAILABLE, "sqlalchemy is required for dashboard tests")
class AdminDashboardTests(unittest.TestCase):
    class _FakeClickUpClient:
        def __init__(self, comments_by_task: dict[str, list[dict[str, object]]] | None = None) -> None:
            self.comments_by_task = comments_by_task or {}

        def get_task_comments(self, task_id: str) -> list[dict[str, object]]:
            return list(self.comments_by_task.get(task_id, []))

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
                clickup_client=self._FakeClickUpClient(),
                as_of_date=date(2026, 3, 14),
            )

        self.assertEqual(dashboard.total_active_leads, 1)
        self.assertEqual(dashboard.mailbox_findings, 1)
        self.assertEqual(dashboard.stale_counts["overdue"], 1)
        self.assertEqual(dashboard.owner_queues[0].owner_name, "Valeria Morales")
        owner_names = [queue.owner_name for queue in dashboard.owner_queues]
        self.assertIn("David Narayan", owner_names)

    def test_dashboard_payload_round_trip(self) -> None:
        session_factory = create_session_factory("sqlite:///:memory:")
        init_database(session_factory)
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
                lead_builder_status={"ready": False, "missing": ["STORELEADS_API_KEY"]},
                clickup_client=self._FakeClickUpClient(),
                as_of_date=date(2026, 3, 14),
            )

        rebuilt = dashboard_data_from_dict(dashboard_data_to_dict(dashboard))
        self.assertEqual(rebuilt.as_of_date.isoformat(), "2026-03-14")
        self.assertEqual(rebuilt.lead_builder_missing, ["STORELEADS_API_KEY"])
        self.assertFalse(rebuilt.deck_canva_connected)

    def test_dashboard_counts_active_leads_even_without_queue_items(self) -> None:
        session_factory = create_session_factory("sqlite:///:memory:")
        init_database(session_factory)

        with session_scope(session_factory) as session:
            session.add(
                LeadMirror(
                    clickup_task_id="task-3",
                    list_id="list-123",
                    task_name="Umbrella",
                    task_url="https://app.clickup.com/t/task-3",
                    status="working offered",
                    assignee_id="owner-3",
                    assignee_name="Taylor Kent",
                    created_at=datetime(2026, 3, 14, 12, 0, tzinfo=timezone.utc),
                    last_sync_at=datetime(2026, 3, 14, 12, 5, tzinfo=timezone.utc),
                    last_meaningful_touch_at=datetime(2026, 3, 14, 12, 30, tzinfo=timezone.utc),
                    next_follow_up_at=datetime(2026, 3, 20, 12, 0, tzinfo=timezone.utc),
                )
            )

        settings = SimpleNamespace(
            clickup_list_id="list-123",
            active_statuses=(normalize_status_key("working offered"),),
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
                clickup_client=self._FakeClickUpClient(),
                as_of_date=date(2026, 3, 14),
            )

        self.assertEqual(dashboard.total_active_leads, 1)
        self.assertEqual(len(dashboard.owner_queues), 0)

    def test_dashboard_render_includes_deck_generator_and_gmail_draft_controls(self) -> None:
        session_factory = create_session_factory("sqlite:///:memory:")
        init_database(session_factory)
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
                clickup_client=self._FakeClickUpClient(),
                as_of_date=date(2026, 3, 14),
            )

        html = render_dashboard_page(dashboard)
        self.assertIn("deck-generator-form", html)
        self.assertIn("CONNECT CANVA", html)
        self.assertIn("/admin/api/generate-deck", html)
        self.assertIn("Target product URL or ASIN", html)
        self.assertIn("Competitor Amazon links or ASINs", html)
        self.assertIn("top_products_by_bsr", html)
        self.assertIn("gmail-drafts-form", html)
        self.assertIn("/admin/api/create-gmail-drafts", html)
        self.assertIn("OPEN GMAIL DRAFTS", html)

    def test_dashboard_render_shows_feed_error_notice(self) -> None:
        session_factory = create_session_factory("sqlite:///:memory:")
        init_database(session_factory)
        dashboard = dashboard_data_from_dict(
            {
                "as_of_date": "2026-03-14",
                "total_active_leads": 0,
                "stale_counts": {"overdue": 0, "needs_immediate_review": 0, "follow_up_due": 0},
                "mailbox_findings": 0,
                "owner_queues": [],
                "latest_sync_at": None,
                "latest_run_summary": {"dashboard_error": "Sales support dashboard feed unavailable: timed out"},
                "sync_auto_enabled": False,
                "sync_stale_after_minutes": 0,
                "lead_builder_ready": True,
                "lead_builder_missing": [],
                "deck_generator_ready": False,
                "deck_generator_missing": [],
                "deck_canva_connected": False,
                "deck_canva_display_name": "",
                "deck_canva_capabilities": {"autofill": False, "brand_template": False},
                "deck_google_source": "",
                "deck_template_id": "",
                "recent_deck_runs": [],
            }
        )

        html = render_dashboard_page(dashboard)
        self.assertIn("Board data is temporarily unavailable.", html)
        self.assertIn("No owner queues available because the board feed could not be loaded yet.", html)


if __name__ == "__main__":
    unittest.main()
