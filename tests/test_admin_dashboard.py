from __future__ import annotations

import unittest
from datetime import date, datetime, timezone
from pathlib import Path
import sys
from types import SimpleNamespace

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

try:
    from sales_support_agent.config import (
        DEFAULT_STATUS_POLICIES,
        build_normalized_status_policies,
        is_active_pipeline_status,
        normalize_status_key,
        status_policy_for,
    )
    from sales_support_agent.models.database import create_session_factory, init_database, session_scope
    from sales_support_agent.models.entities import AutomationRun, LeadMirror, MailboxSignal
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
        def get_task_comments(self, task_id: str) -> list[dict[str, object]]:
            return []

    def _settings(self) -> SimpleNamespace:
        return SimpleNamespace(
            clickup_list_id="list-123",
            active_statuses=(normalize_status_key("new lead"),),
            inactive_statuses=(),
            status_policies=build_normalized_status_policies(DEFAULT_STATUS_POLICIES),
            slack_assignee_map={},
            stale_lead_immediate_alert_urgencies=(),
            stale_lead_slack_digest_max_items=20,
            stale_lead_slack_digest_mention_channel=False,
            dashboard_auto_sync_enabled=False,
            dashboard_auto_sync_max_age_minutes=30,
        )

    def test_dashboard_payload_round_trip(self) -> None:
        session_factory = create_session_factory("sqlite:///:memory:")
        init_database(session_factory)
        with session_scope(session_factory) as session:
            dashboard = build_dashboard_data(
                settings=self._settings(),
                session=session,
                lead_builder_status={"ready": False, "missing": ["STORELEADS_API_KEY"]},
                clickup_client=self._FakeClickUpClient(),
                as_of_date=date(2026, 3, 14),
            )

        rebuilt = dashboard_data_from_dict(dashboard_data_to_dict(dashboard))
        self.assertEqual(rebuilt.as_of_date.isoformat(), "2026-03-14")
        self.assertEqual(rebuilt.lead_builder_missing, ["STORELEADS_API_KEY"])
        self.assertTrue(rebuilt.deck_generator_ready)

    def test_dashboard_render_includes_amazon_first_deck_controls(self) -> None:
        session_factory = create_session_factory("sqlite:///:memory:")
        init_database(session_factory)
        with session_scope(session_factory) as session:
            session.add(
                AutomationRun(
                    run_type="deck_generation",
                    status="success",
                    started_at=datetime(2026, 3, 14, 10, 0, tzinfo=timezone.utc),
                    summary_json={
                        "status": "success",
                        "message": "Deck generated successfully as an HTML report.",
                        "output_type": "html",
                        "design_title": "OceanRx x anata - Strategy Deck",
                        "view_url": "https://sales-support-agent.onrender.com/deck-exports/1/token",
                        "channels": ["amazon", "shopify"],
                        "view_count": 3,
                    },
                )
            )
            dashboard = build_dashboard_data(
                settings=self._settings(),
                session=session,
                lead_builder_status={"ready": True, "missing": []},
                clickup_client=self._FakeClickUpClient(),
                as_of_date=date(2026, 3, 14),
            )

        html = render_dashboard_page(dashboard)
        self.assertIn("deck-generator-form", html)
        self.assertIn("/admin/api/generate-deck", html)
        self.assertIn("Target Amazon ASIN or URL", html)
        self.assertIn("Competitor Xray CSV", html)
        self.assertIn("Keyword Xray CSV", html)
        self.assertIn("Include offering slides", html)
        self.assertIn("tiktok_shop", html)
        self.assertNotIn("CONNECT CANVA", html)
        self.assertNotIn("Google sheet range", html)
        self.assertNotIn("Competitor Amazon links or ASINs", html)
        self.assertIn("Failed sync state", html)
        self.assertIn("Sync now", html)
        self.assertIn("Needs review", html)
        self.assertIn("Due today", html)

    def test_unknown_non_terminal_statuses_are_treated_as_active(self) -> None:
        session_factory = create_session_factory("sqlite:///:memory:")
        init_database(session_factory)
        settings = self._settings()
        with session_scope(session_factory) as session:
            session.add(
                LeadMirror(
                    clickup_task_id="task-1",
                    list_id=settings.clickup_list_id,
                    task_name="Prospect One",
                    task_url="https://app.clickup.com/t/task-1",
                    status="FOLLOW UP",
                    created_at=datetime(2026, 3, 14, 9, 0, tzinfo=timezone.utc),
                    updated_at=datetime(2026, 3, 14, 9, 0, tzinfo=timezone.utc),
                    last_sync_at=datetime(2026, 3, 14, 9, 0, tzinfo=timezone.utc),
                    raw_task_payload={},
                )
            )
            dashboard = build_dashboard_data(
                settings=settings,
                session=session,
                lead_builder_status={"ready": True, "missing": []},
                clickup_client=self._FakeClickUpClient(),
                as_of_date=date(2026, 3, 14),
            )

        self.assertEqual(dashboard.total_active_leads, 1)


class AdminStatusPolicyTests(unittest.TestCase):
    def test_is_active_pipeline_status_excludes_closed_and_includes_unknown_open(self) -> None:
        active_statuses = (normalize_status_key("new lead"),)
        inactive_statuses = (normalize_status_key("lost"), normalize_status_key("won - active"))

        self.assertTrue(
            is_active_pipeline_status(
                "FOLLOW UP",
                active_statuses=active_statuses,
                inactive_statuses=inactive_statuses,
            )
        )
        self.assertFalse(
            is_active_pipeline_status(
                "WON - ACTIVE",
                active_statuses=active_statuses,
                inactive_statuses=inactive_statuses,
            )
        )

    def test_status_policy_for_falls_back_to_follow_up_policy(self) -> None:
        policies = build_normalized_status_policies(DEFAULT_STATUS_POLICIES)
        self.assertEqual(
            status_policy_for("FOLLOW UP", policies),
            policies[normalize_status_key("FOLLOW UP")],
        )


if __name__ == "__main__":
    unittest.main()
