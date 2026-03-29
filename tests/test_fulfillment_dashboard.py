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
            "schema_version": "1.0",
            "report_id": "2026-03-27T21-50-11-06-00",
            "report_slug": "support-review-2026-03-27T21-50-11-06-00",
            "title": "Fulfillment CS Review",
            "generated_at": "2026-03-27T21:50:11-06:00",
            "status": "ready",
            "candidate_count": 1,
            "action_counts": {"clarifying": 1, "investigating": 0, "ready_to_answer": 0, "escalated": 0, "resolved": 0},
            "lifecycle_counts": {"new": 0, "investigating": 1, "responded": 0, "escalated": 0, "waiting_human": 0, "resolved": 0},
            "summary": {
                "candidate_count": 1,
                "action_counts": {"clarifying": 1, "investigating": 0, "ready_to_answer": 0, "escalated": 0, "resolved": 0},
                "lifecycle_counts": {"new": 0, "investigating": 1, "responded": 0, "escalated": 0, "waiting_human": 0, "resolved": 0},
                "brand_counts": [{"brand": "Mule Deer Foundation", "count": 1}],
                "account_counts": [{"account_name": "Mule Deer Foundation", "account_id": "pending:mule-deer-foundation", "count": 1}],
                "escalation_count": 0,
                "unresolved_count": 1,
            },
            "recent_candidates": [
                {
                    "case_id": "case-123",
                    "brand": "Mule Deer Foundation",
                    "channel_name": "mule-deer-anatafulfillment",
                    "customer_thread_link": "https://anatainc.slack.com/archives/C099KMCAQ6A/p1774656761822649",
                    "question_summary": "Need PO verification for received boots.",
                    "lifecycle_state": "investigating",
                    "ui_recommendation": "clarifying",
                    "draft_reply": "Can you send the PO number or shipment reference so I can pull this up?",
                    "evidence_summary": "Labelogics: pending account match",
                }
            ],
            "candidates": [
                {
                    "case_id": "case-123",
                    "brand": "Mule Deer Foundation",
                    "channel_name": "mule-deer-anatafulfillment",
                    "customer_thread_link": "https://anatainc.slack.com/archives/C099KMCAQ6A/p1774656761822649",
                    "question_summary": "Need PO verification for received boots.",
                    "lifecycle_state": "investigating",
                    "ui_recommendation": "clarifying",
                    "draft_reply": "Can you send the PO number or shipment reference so I can pull this up?",
                    "evidence_summary": "Labelogics: pending account match",
                }
            ],
            "escalations": [],
            "warnings": [],
        }

    def test_render_dashboard_includes_submenu_and_candidate(self) -> None:
        html = render_fulfillment_dashboard_page(self._sample_report(), [])
        self.assertIn("/admin/fulfillment-cs/reports/", html)
        self.assertIn("Fulfillment CS", html)
        self.assertIn("Need PO verification for received boots.", html)
        self.assertIn("Ready to answer", html)

    def test_admin_routes_render_fulfillment_pages(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            reports_dir = Path(tmpdir) / "reports"
            reports_dir.mkdir(parents=True, exist_ok=True)
            report = self._sample_report()
            slug = "support-review-2026-03-27T21-50-11-06-00"
            (reports_dir / f"{slug}.json").write_text(json.dumps(report))
            (reports_dir / f"{slug}.md").write_text("# Fulfillment CS Review\n")
            (reports_dir / f"{slug}.html").write_text("<html><body>artifact html</body></html>")
            (reports_dir / "latest.json").write_text(json.dumps(report))
            (reports_dir / "index.json").write_text(
                json.dumps(
                    {
                        "schema_version": "1.0",
                        "generated_at": report["generated_at"],
                        "latest_report_id": report["report_id"],
                        "reports": [
                            {
                                "report_id": report["report_id"],
                                "report_slug": slug,
                                "title": report["title"],
                                "generated_at": report["generated_at"],
                                "candidate_count": 1,
                                "action_counts": report["summary"]["action_counts"],
                                "lifecycle_counts": report["summary"]["lifecycle_counts"],
                                "artifact_formats": ["json"],
                                "links": {"detail": f"/admin/fulfillment-cs/reports/{slug}"},
                            }
                        ],
                    }
                )
            )

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
                self.assertIn("Unresolved", response.text)

                response = client.get("/admin/fulfillment-cs/reports/")
                self.assertEqual(response.status_code, 200)
                self.assertIn("Fulfillment CS Review", response.text)
                self.assertIn("JSON", response.text)

                response = client.get("/admin/fulfillment-cs/reports/latest", follow_redirects=False)
                self.assertEqual(response.status_code, 302)
                self.assertEqual(response.headers["location"], f"/admin/fulfillment-cs/reports/{slug}")

                response = client.get(f"/admin/fulfillment-cs/reports/{slug}")
                self.assertEqual(response.status_code, 200)
                self.assertIn("Mule Deer Foundation", response.text)
                self.assertIn("Escalations", response.text)

                response = client.get(f"/admin/fulfillment-cs/reports/{slug}.json")
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.headers["content-type"].split(";")[0], "application/json")

                response = client.get(f"/admin/fulfillment-cs/reports/{slug}.md")
                self.assertEqual(response.status_code, 200)
                self.assertIn("Fulfillment CS Review", response.text)

                response = client.get(f"/admin/fulfillment-cs/reports/{slug}.html")
                self.assertEqual(response.status_code, 200)
                self.assertIn("artifact html", response.text)

    def test_report_entries_read_timestamped_reports(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            reports_dir = Path(tmpdir)
            report = self._sample_report()
            (reports_dir / "support-review-2026-03-27T21-50-11-06-00.json").write_text(json.dumps(report))
            entries = fulfillment_report_entries(reports_dir)
            self.assertEqual(len(entries), 1)
            self.assertEqual(entries[0].candidate_count, 1)
            self.assertIn("clarifying", entries[0].excerpt)
            self.assertIn("json", entries[0].artifact_formats)


if __name__ == "__main__":
    unittest.main()
