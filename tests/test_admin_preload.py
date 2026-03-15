from __future__ import annotations

import unittest
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

try:
    from main import dashboard_needs_auto_sync
    from sales_support_agent.services.admin_dashboard import DashboardData

    FASTAPI_AVAILABLE = True
except ModuleNotFoundError as exc:
    if exc.name != "fastapi":
        raise
    FASTAPI_AVAILABLE = False


@unittest.skipUnless(FASTAPI_AVAILABLE, "fastapi is required for preload tests")
class AdminPreloadTests(unittest.TestCase):
    def test_dashboard_needs_auto_sync_when_sync_is_stale(self) -> None:
        dashboard = DashboardData(
            as_of_date=date(2026, 3, 14),
            total_active_leads=10,
            stale_counts={"overdue": 1, "needs_immediate_review": 2, "follow_up_due": 3},
            mailbox_findings=0,
            owner_queues=[],
            latest_sync_at=datetime(2026, 3, 14, 11, 0, tzinfo=timezone.utc),
            latest_run_summary={},
            lead_builder_ready=True,
            lead_builder_missing=[],
        )
        settings = SimpleNamespace(admin_auto_sync_max_age_minutes=30)

        self.assertTrue(
            dashboard_needs_auto_sync(
                dashboard,
                settings,
                now=datetime(2026, 3, 14, 11, 45, tzinfo=timezone.utc),
            )
        )
        self.assertFalse(
            dashboard_needs_auto_sync(
                dashboard,
                settings,
                now=datetime(2026, 3, 14, 11, 20, tzinfo=timezone.utc),
            )
        )

    def test_dashboard_needs_auto_sync_when_never_synced(self) -> None:
        dashboard = DashboardData(
            as_of_date=date(2026, 3, 14),
            total_active_leads=0,
            stale_counts={"overdue": 0, "needs_immediate_review": 0, "follow_up_due": 0},
            mailbox_findings=0,
            owner_queues=[],
            latest_sync_at=None,
            latest_run_summary={},
            lead_builder_ready=True,
            lead_builder_missing=[],
        )
        settings = SimpleNamespace(admin_auto_sync_max_age_minutes=30)
        self.assertTrue(dashboard_needs_auto_sync(dashboard, settings))


if __name__ == "__main__":
    unittest.main()
