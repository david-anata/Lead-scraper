from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

try:
    from fastapi.testclient import TestClient

    from sales_support_agent.main import create_app
    from sales_support_agent.services.admin_auth import create_admin_session_token
    from sales_support_agent.services.fulfillment_dashboard import (
        fulfillment_report_entries,
        render_fulfillment_dashboard_page,
    )

    FASTAPI_AVAILABLE = True
except ModuleNotFoundError as exc:
    if exc.name not in {"fastapi", "sqlalchemy"}:
        raise
    FASTAPI_AVAILABLE = False


@unittest.skipUnless(FASTAPI_AVAILABLE, "fastapi and sqlalchemy are required for fulfillment dashboard tests")
class FulfillmentDashboardTests(unittest.TestCase):
    def _sample_report(self) -> dict[str, object]:
        return {
            "title": "Fulfillment Support Review",
            "generated_at": "2026-03-27T21:50:11-06:00",
            "status": "ready",
            "candidate_count": 1,
            "action_counts": {"clarifying": 1},
            "candidates": [
                {
                    "brand_name": "Mule Deer Foundation",
                    "channel": "mule-deer-anatafulfillment",
                    "permalink": "https://anatainc.slack.com/archives/C099KMCAQ6A/p1774656761822649",
                    "question_summary": "Need PO verification for received boots.",
                    "identifiers": {"po_numbers": ["PO-108096"], "order_numbers": [], "tracking_numbers": []},
                    "evidence": {"labelogics": {"status": "pending_account_match"}},
                    "recommended_action": {
                        "reply_type": "clarifying",
                        "customer_reply": "Can you send the PO number or shipment reference so I can pull this up?",
                    },
                }
            ],
        }

    def test_render_dashboard_includes_submenu_and_candidate(self) -> None:
        html = render_fulfillment_dashboard_page(self._sample_report(), [])
        self.assertIn("/admin/fulfillment-cs/reports/", html)
        self.assertIn("Fulfillment CS", html)
        self.assertIn("Need PO verification for received boots.", html)

    def test_admin_routes_render_fulfillment_pages(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            reports_dir = Path(tmpdir) / "reports"
            reports_dir.mkdir(parents=True, exist_ok=True)
            report = self._sample_report()
            slug = "support-review-2026-03-27T21-50-11-06-00"
            (reports_dir / f"{slug}.json").write_text(json.dumps(report))
            (reports_dir / "latest.json").write_text(json.dumps(report))

            db_path = Path(tmpdir) / "agent.db"
            env = {
                "ADMIN_DASHBOARD_PASSWORD": "secret-pass",
                "ADMIN_DASHBOARD_SESSION_SECRET": "session-secret",
                "FULFILLMENT_CS_REPORTS_DIR": str(reports_dir),
                "SALES_AGENT_DB_URL": f"sqlite:///{db_path}",
            }
            with mock.patch.dict(os.environ, env, clear=False):
                app = create_app()
                client = TestClient(app)
                session_token = create_admin_session_token(app.state.settings)
                client.cookies.set(app.state.settings.admin_cookie_name, session_token)

                response = client.get("/admin/fulfillment-cs/")
                self.assertEqual(response.status_code, 200)
                self.assertIn("Fulfillment CS", response.text)
                self.assertIn("Candidate preview", response.text)

                response = client.get("/admin/fulfillment-cs/reports/")
                self.assertEqual(response.status_code, 200)
                self.assertIn("Fulfillment Support Review", response.text)

                response = client.get("/admin/fulfillment-cs/reports/latest", follow_redirects=False)
                self.assertEqual(response.status_code, 302)
                self.assertEqual(response.headers["location"], f"/admin/fulfillment-cs/reports/{slug}")

                response = client.get(f"/admin/fulfillment-cs/reports/{slug}")
                self.assertEqual(response.status_code, 200)
                self.assertIn("Mule Deer Foundation", response.text)

    def test_report_entries_read_timestamped_reports(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            reports_dir = Path(tmpdir)
            report = self._sample_report()
            (reports_dir / "support-review-2026-03-27T21-50-11-06-00.json").write_text(json.dumps(report))
            entries = fulfillment_report_entries(reports_dir)
            self.assertEqual(len(entries), 1)
            self.assertEqual(entries[0].candidate_count, 1)
            self.assertIn("clarifying", entries[0].excerpt)


if __name__ == "__main__":
    unittest.main()
