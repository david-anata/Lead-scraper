from __future__ import annotations

import unittest
from datetime import date, datetime, timezone
from types import SimpleNamespace

try:
    from sales_support_agent.config import DEFAULT_STATUS_POLICIES, build_normalized_status_policies, normalize_status_key
    from sales_support_agent.models.database import create_session_factory, init_database, session_scope
    from sales_support_agent.models.entities import CommunicationEvent, LeadMirror, MailboxSignal
    from sales_support_agent.services.admin_dashboard import (
        build_executive_data,
        executive_data_from_dict,
        executive_data_to_dict,
        render_executive_page,
    )

    SQLALCHEMY_AVAILABLE = True
except ModuleNotFoundError as exc:
    if exc.name != "sqlalchemy":
        raise
    SQLALCHEMY_AVAILABLE = False


def _build_settings() -> SimpleNamespace:
    active_statuses = tuple(
        normalize_status_key(status)
        for status in (
            "new lead",
            "contacted cold",
            "contacted warm",
            "working qualified",
            "working needs offer",
            "working offered",
            "working negotiating",
        )
    )
    return SimpleNamespace(
        clickup_list_id="list-123",
        active_statuses=active_statuses,
        inactive_statuses=(normalize_status_key("FOLLOW UP"),),
        status_policies=build_normalized_status_policies(DEFAULT_STATUS_POLICIES),
        slack_assignee_map={},
        stale_lead_immediate_alert_urgencies=(),
        stale_lead_slack_digest_max_items=20,
        stale_lead_slack_digest_mention_channel=False,
    )


@unittest.skipUnless(SQLALCHEMY_AVAILABLE, "sqlalchemy is required for executive dashboard tests")
class AdminExecutiveTests(unittest.TestCase):
    def test_build_executive_data_aggregates_pipeline_health(self) -> None:
        session_factory = create_session_factory("sqlite:///:memory:")
        init_database(session_factory)

        with session_scope(session_factory) as session:
            session.add_all(
                [
                    LeadMirror(
                        clickup_task_id="task-1",
                        list_id="list-123",
                        task_name="Acme Wholesale",
                        task_url="https://app.clickup.com/t/task-1",
                        status="working offered",
                        assignee_id="owner-1",
                        assignee_name="Gabe Smedley",
                        source="Apollo",
                        value="$12,000",
                        created_at=datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc),
                        last_meaningful_touch_at=datetime(2026, 3, 9, 12, 0, tzinfo=timezone.utc),
                        last_sync_at=datetime(2026, 3, 18, 8, 0, tzinfo=timezone.utc),
                        recommended_next_action="Send the revised offer today.",
                    ),
                    LeadMirror(
                        clickup_task_id="task-2",
                        list_id="list-123",
                        task_name="Bluebird Commerce",
                        task_url="https://app.clickup.com/t/task-2",
                        status="new lead",
                        assignee_id="owner-2",
                        assignee_name="Valeria Morales",
                        source="Referral",
                        value="TBD",
                        created_at=datetime(2026, 3, 13, 9, 0, tzinfo=timezone.utc),
                        last_sync_at=datetime(2026, 3, 18, 8, 0, tzinfo=timezone.utc),
                        recommended_next_action="",
                    ),
                    LeadMirror(
                        clickup_task_id="task-3",
                        list_id="list-123",
                        task_name="Northstar Supply",
                        task_url="https://app.clickup.com/t/task-3",
                        status="working offered",
                        assignee_id="owner-1",
                        assignee_name="Gabe Smedley",
                        source="Apollo",
                        value="TBD",
                        created_at=datetime(2026, 3, 11, 9, 0, tzinfo=timezone.utc),
                        last_meaningful_touch_at=datetime(2026, 3, 12, 11, 0, tzinfo=timezone.utc),
                        last_sync_at=datetime(2026, 3, 18, 8, 0, tzinfo=timezone.utc),
                        recommended_next_action="Follow up on pricing.",
                    ),
                    LeadMirror(
                        clickup_task_id="task-4",
                        list_id="list-123",
                        task_name="Inactive Manual Follow Up",
                        task_url="https://app.clickup.com/t/task-4",
                        status="FOLLOW UP",
                        assignee_id="owner-3",
                        assignee_name="David Narayan",
                        created_at=datetime(2026, 3, 10, 9, 0, tzinfo=timezone.utc),
                        last_sync_at=datetime(2026, 3, 18, 8, 0, tzinfo=timezone.utc),
                    ),
                ]
            )
            session.add_all(
                [
                    MailboxSignal(
                        provider="gmail",
                        external_message_id="msg-1",
                        external_thread_id="thread-1",
                        dedupe_key="gmail_message:msg-1",
                        matched_task_id="task-1",
                        owner_name="Gabe Smedley",
                        sender_email="buyer@example.com",
                        sender_domain="example.com",
                        subject="Need updated offer",
                        classification="pricing_or_offer_request",
                        urgency="needs_immediate_review",
                        action_summary="Send the revised offer today.",
                        suggested_reply_draft="Happy to send the revised offer over today.",
                        received_at=datetime(2026, 3, 17, 14, 0, tzinfo=timezone.utc),
                        processed_at=datetime(2026, 3, 17, 14, 5, tzinfo=timezone.utc),
                    ),
                    MailboxSignal(
                        provider="gmail",
                        external_message_id="msg-2",
                        external_thread_id="thread-2",
                        dedupe_key="gmail_message:msg-2",
                        matched_task_id="task-2",
                        owner_name="Valeria Morales",
                        sender_email="lead@example.net",
                        sender_domain="example.net",
                        subject="Checking next steps",
                        classification="reply_received",
                        urgency="follow_up_due",
                        action_summary="Define the first outreach.",
                        suggested_reply_draft="Thanks for reaching out. I will send next steps today.",
                        received_at=datetime(2026, 3, 17, 16, 0, tzinfo=timezone.utc),
                        processed_at=datetime(2026, 3, 17, 16, 5, tzinfo=timezone.utc),
                    ),
                ]
            )
            session.add(
                CommunicationEvent(
                    clickup_task_id="task-1",
                    event_type="inbound_reply_received",
                    source="instantly",
                    summary="Lead asked for the updated offer.",
                    occurred_at=datetime(2026, 3, 17, 14, 0, tzinfo=timezone.utc),
                )
            )

        settings = _build_settings()
        with session_scope(session_factory) as session:
            executive = build_executive_data(
                settings=settings,
                session=session,
                as_of_date=date(2026, 3, 18),
            )

        self.assertEqual(executive.kpis["active_leads"], 3)
        self.assertEqual(executive.kpis["overdue"], 1)
        self.assertEqual(executive.kpis["review"], 1)
        self.assertEqual(executive.kpis["due"], 1)
        self.assertEqual(executive.kpis["late_stage_stale"], 1)
        self.assertEqual(executive.owner_scorecards[0].owner_name, "Gabe Smedley")
        self.assertEqual(executive.owner_scorecards[0].value_total, 12000.0)
        self.assertEqual(executive.risk_leads[0].task_name, "Acme Wholesale")
        self.assertEqual(executive.risk_leads[0].urgency, "overdue")
        self.assertEqual(executive.hygiene_counts["missing_next_action"], 1)
        self.assertEqual(executive.hygiene_counts["missing_meeting_outcome"], 2)
        self.assertEqual(executive.hygiene_counts["untouched_new_or_contacted"], 1)
        self.assertEqual(executive.inbound_replies_by_owner[0].owner_name, "Gabe Smedley")
        self.assertEqual(executive.inbound_replies_by_owner[0].count, 1)
        self.assertEqual(executive.mailbox_signals_by_owner[0].count, 1)
        self.assertIn("3 active leads are currently tracked.", executive.summary_text)
        self.assertNotIn("FOLLOW UP", {item.label for item in executive.status_distribution})

    def test_executive_payload_round_trip_and_render(self) -> None:
        session_factory = create_session_factory("sqlite:///:memory:")
        init_database(session_factory)
        settings = _build_settings()

        with session_scope(session_factory) as session:
            executive = build_executive_data(
                settings=settings,
                session=session,
                as_of_date=date(2026, 3, 18),
            )

        rebuilt = executive_data_from_dict(executive_data_to_dict(executive))
        self.assertEqual(rebuilt.as_of_date.isoformat(), "2026-03-18")
        self.assertEqual(rebuilt.kpis["active_leads"], 0)

        html = render_executive_page(rebuilt)
        self.assertIn("/admin", html)
        self.assertIn("Executive summary", html)
        self.assertIn("id=\"owner-filter\"", html)
        self.assertIn("id=\"scorecard-table\"", html)


if __name__ == "__main__":
    unittest.main()
