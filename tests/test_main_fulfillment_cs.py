from __future__ import annotations

import importlib
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import ModuleType
from unittest import mock

from fastapi.testclient import TestClient

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from sales_support_agent.services.admin_auth import create_admin_session_token


class MainFulfillmentCSTests(unittest.TestCase):
    def _import_main_with_stubs(self):
        admin_dashboard = ModuleType("sales_support_agent.services.admin_dashboard")

        class DashboardData:  # pragma: no cover - test import stub
            pass

        class ExecutiveData:  # pragma: no cover - test import stub
            pass

        admin_dashboard.DashboardData = DashboardData
        admin_dashboard.ExecutiveData = ExecutiveData
        admin_dashboard.dashboard_data_from_dict = lambda payload: payload
        admin_dashboard.executive_data_from_dict = lambda payload: payload
        admin_dashboard.render_dashboard_page = lambda payload: "<html>dashboard</html>"
        admin_dashboard.render_executive_page = lambda payload: "<html>executive</html>"
        admin_dashboard.render_login_page = lambda error_message="": f"<html>login {error_message}</html>"

        website_ops = ModuleType("sales_support_agent.services.website_ops")
        website_ops.get_website_ops_run_state = lambda settings, mode="daily": {"status": "idle", "mode": mode}
        website_ops.latest_report_entry = lambda settings: None
        website_ops.render_dashboard_page = lambda settings: "<html>website ops dashboard</html>"
        website_ops.render_feedback_detail_page = lambda settings, feedback_id: "<html>feedback</html>"
        website_ops.render_queue_page = lambda settings, status_filter="": "<html>queue</html>"
        website_ops.render_report_page = lambda settings, mode, slug: "<html>report</html>"
        website_ops.render_reports_page = lambda settings: "<html>reports</html>"
        website_ops.review_feedback_record = lambda settings, feedback_id, payload: {"ok": True, "feedback_id": feedback_id}
        website_ops.run_website_ops = lambda settings, mode="daily": None
        website_ops.save_feedback_record = lambda settings, payload: payload
        website_ops.website_ops_run_is_due = lambda settings, mode="daily": False
        website_ops.write_website_ops_run_state = lambda settings, mode, payload: payload

        lead_build_revenue = ModuleType("sales_support_agent.services.lead_build_revenue")
        lead_build_revenue.build_revenue_fields = lambda *args, **kwargs: {}
        lead_build_revenue.format_money_compact = lambda value: str(value)
        lead_build_revenue.format_money_exact = lambda value: str(value)
        lead_build_revenue.parse_monthly_sales = lambda value: value

        revenue_ops = ModuleType("sales_support_agent.services.revenue_ops")
        for name in (
            "append_daily_import_count_db",
            "append_processed_domains_db",
            "append_processed_heyreach_leads_db",
            "complete_lead_run",
            "create_lead_run",
            "fail_lead_run",
            "get_lead_run",
            "get_lead_run_csv",
            "load_apollo_attempts_db",
            "load_daily_import_counts_db",
            "load_processed_domains_db",
            "load_processed_heyreach_leads_db",
            "load_source_cursor_db",
            "mark_lead_run_started",
            "record_lead_run_item",
            "save_source_cursor_db",
            "update_lead_run_stage",
            "upsert_apollo_attempts_db",
            "upsert_lead_rows",
        ):
            setattr(revenue_ops, name, lambda *args, **kwargs: None)

        stub_modules = {
            "sales_support_agent.services.admin_dashboard": admin_dashboard,
            "sales_support_agent.services.website_ops": website_ops,
            "sales_support_agent.services.lead_build_revenue": lead_build_revenue,
            "sales_support_agent.services.revenue_ops": revenue_ops,
        }
        with mock.patch.dict(sys.modules, stub_modules):
            return importlib.reload(importlib.import_module("main"))

    def _sample_report(self, slug: str) -> dict[str, object]:
        return {
            "schema_version": "1.0",
            "report_id": "2026-03-31T10-00-00-06-00",
            "report_slug": slug,
            "title": "Fulfillment CS Review",
            "generated_at": "2026-03-31T10:00:00-06:00",
            "status": "ok",
            "summary": {
                "candidate_count": 1,
                "action_counts": {
                    "clarifying": 1,
                    "investigating": 0,
                    "ready_to_answer": 0,
                    "escalated": 0,
                    "resolved": 0,
                },
                "lifecycle_counts": {
                    "new": 0,
                    "investigating": 1,
                    "responded": 0,
                    "escalated": 0,
                    "waiting_human": 0,
                    "resolved": 0,
                },
                "brand_counts": [{"brand": "Mule Deer Foundation", "count": 1}],
                "account_counts": [
                    {"account_name": "Mule Deer Foundation", "account_id": "pending:mule-deer-foundation", "count": 1}
                ],
                "escalation_count": 0,
                "unresolved_count": 1,
            },
            "recent_candidates": [
                {
                    "case_id": "case_123",
                    "channel_name": "mule-deer-anatafulfillment",
                    "thread_ts": "1774656761.822649",
                    "question_summary": "Need PO verification for received boots.",
                    "lifecycle_state": "investigating",
                    "ui_recommendation": "clarifying",
                    "brand": "Mule Deer Foundation",
                    "account_name": "Mule Deer Foundation",
                    "customer_thread_link": "https://anatainc.slack.com/archives/C099KMCAQ6A/p1774656761822649",
                    "draft_reply": "Can you send the PO number or shipment reference so I can pull this up?",
                    "evidence_summary": "Brand matched by channel; no shipment reference in thread.",
                }
            ],
            "candidates": [
                {
                    "case_id": "case_123",
                    "channel_name": "mule-deer-anatafulfillment",
                    "channel_id": "C099KMCAQ6A",
                    "thread_ts": "1774656761.822649",
                    "customer_thread_link": "https://anatainc.slack.com/archives/C099KMCAQ6A/p1774656761822649",
                    "question_summary": "Need PO verification for received boots.",
                    "lifecycle_state": "investigating",
                    "ui_recommendation": "clarifying",
                    "brand": "Mule Deer Foundation",
                    "account_id": "pending:mule-deer-foundation",
                    "account_name": "Mule Deer Foundation",
                    "draft_reply": "Can you send the PO number or shipment reference so I can pull this up?",
                    "evidence_summary": "Brand matched by channel; no shipment reference in thread.",
                    "updated_at": "2026-03-31T09:58:00-06:00",
                }
            ],
            "escalations": [],
            "links": {
                "self_json": f"/admin/fulfillment-cs/reports/{slug}.json",
                "self_html": f"/admin/fulfillment-cs/reports/{slug}.html",
                "reports_index": "/admin/fulfillment-cs/reports/",
                "latest": "/admin/fulfillment-cs/reports/latest",
            },
            "warnings": [],
        }

    def _write_report_fixture(self, reports_dir: Path, slug: str) -> None:
        report = self._sample_report(slug)
        reports_dir.mkdir(parents=True, exist_ok=True)
        (reports_dir / f"{slug}.json").write_text(json.dumps(report))
        (reports_dir / f"{slug}.html").write_text("<html><body>raw html report</body></html>")
        (reports_dir / f"{slug}.md").write_text("# raw markdown report")
        (reports_dir / "latest.json").write_text(json.dumps(report))
        (reports_dir / "index.json").write_text(
            json.dumps(
                {
                    "schema_version": "1.0",
                    "generated_at": "2026-03-31T10:00:00-06:00",
                    "latest_report_id": "2026-03-31T10-00-00-06-00",
                    "reports": [
                        {
                            "report_id": "2026-03-31T10-00-00-06-00",
                            "report_slug": slug,
                            "title": "Fulfillment CS Review",
                            "generated_at": "2026-03-31T10:00:00-06:00",
                            "candidate_count": 1,
                            "action_counts": {"clarifying": 1},
                            "lifecycle_counts": {"investigating": 1},
                            "artifact_formats": ["json", "html", "md"],
                            "links": {"detail": f"/admin/fulfillment-cs/reports/{slug}"},
                        }
                    ],
                }
            )
        )

    def test_top_level_admin_routes_render_fulfillment_pages_and_artifacts(self) -> None:
        slug = "support-review-2026-03-31T10-00-00-06-00"
        with tempfile.TemporaryDirectory() as tmpdir:
            reports_dir = Path(tmpdir) / "reports"
            self._write_report_fixture(reports_dir, slug)
            env = {
                "APOLLO_API_KEY": "apollo-test",
                "SLACK_BOT_TOKEN": "slack-test",
                "SLACK_CHANNEL_ID": "C123",
                "ADMIN_DASHBOARD_PASSWORD": "secret-pass",
                "ADMIN_DASHBOARD_SESSION_SECRET": "session-secret",
                "FULFILLMENT_CS_REPORTS_DIR": str(reports_dir),
            }
            with mock.patch.dict(os.environ, env, clear=False):
                lead_main = self._import_main_with_stubs()
                client = TestClient(lead_main.app)
                session_token = create_admin_session_token(lead_main.load_admin_dashboard_settings())
                client.cookies.set(lead_main.load_admin_dashboard_settings().admin_cookie_name, session_token)

                response = client.get("/admin/fulfillment-cs/")
                self.assertEqual(response.status_code, 200)
                self.assertIn("Fulfillment CS", response.text)
                self.assertIn("Candidate preview", response.text)

                response = client.get("/admin/fulfillment-cs/reports/")
                self.assertEqual(response.status_code, 200)
                self.assertIn("Fulfillment CS Review", response.text)

                response = client.get("/admin/fulfillment-cs/reports/latest", follow_redirects=False)
                self.assertEqual(response.status_code, 302)
                self.assertEqual(response.headers["location"], f"/admin/fulfillment-cs/reports/{slug}")

                response = client.get(f"/admin/fulfillment-cs/reports/{slug}")
                self.assertEqual(response.status_code, 200)
                self.assertIn("Mule Deer Foundation", response.text)

                response = client.get(f"/admin/fulfillment-cs/reports/{slug}.json")
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.headers["content-type"].split(";")[0], "application/json")

                response = client.get(f"/admin/fulfillment-cs/reports/{slug}.md")
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.headers["content-type"].split(";")[0], "text/markdown")

                response = client.get(f"/admin/fulfillment-cs/reports/{slug}.html")
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.headers["content-type"].split(";")[0], "text/html")

    def test_top_level_admin_routes_handle_empty_reports_gracefully(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            reports_dir = Path(tmpdir) / "reports"
            reports_dir.mkdir(parents=True, exist_ok=True)
            env = {
                "APOLLO_API_KEY": "apollo-test",
                "SLACK_BOT_TOKEN": "slack-test",
                "SLACK_CHANNEL_ID": "C123",
                "ADMIN_DASHBOARD_PASSWORD": "secret-pass",
                "ADMIN_DASHBOARD_SESSION_SECRET": "session-secret",
                "FULFILLMENT_CS_REPORTS_DIR": str(reports_dir),
            }
            with mock.patch.dict(os.environ, env, clear=False):
                lead_main = self._import_main_with_stubs()
                client = TestClient(lead_main.app)
                session_token = create_admin_session_token(lead_main.load_admin_dashboard_settings())
                client.cookies.set(lead_main.load_admin_dashboard_settings().admin_cookie_name, session_token)

                response = client.get("/admin/fulfillment-cs/")
                self.assertEqual(response.status_code, 200)
                self.assertIn("No fulfillment review report has been generated yet.", response.text)

                response = client.get("/admin/fulfillment-cs/reports/latest", follow_redirects=False)
                self.assertEqual(response.status_code, 302)
                self.assertEqual(response.headers["location"], "/admin/fulfillment-cs/reports/")


if __name__ == "__main__":
    unittest.main()
