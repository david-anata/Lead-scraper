from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from sales_support_agent.services import website_ops_vendor as website_ops
from sales_support_agent.services.website_ops import (
    latest_report_entry,
    load_feedback_records,
    render_dashboard_page,
    render_feedback_detail_page,
    review_feedback_record,
    run_website_ops,
    save_feedback_record,
)


class AdminWebsiteOpsTests(unittest.TestCase):
    def _fake_report(self) -> dict[str, object]:
        return {
            "date": "2026-03-26",
            "generated_at": "2026-03-26T00:00:00Z",
            "title": "Anata Website Ops Daily Report",
            "scope": "agent-admin daily sweep",
            "status": "healthy",
            "pages_reviewed": 0,
            "pages_healthy": 0,
            "pages_with_issues": 0,
            "issues_found": 0,
            "issue_counts_by_priority": {"P0": 0, "P1": 0, "P2": 0, "P3": 0},
            "pages": [],
            "issues": [],
            "recommendations": [],
            "notes": [],
            "feedback_received": 0,
            "feedback_open": 0,
            "recent_feedback": [],
            "changes_applied": 0,
            "executed_actions": [],
        }

    def _settings(self, root: Path, *, execute_approved: bool = False) -> SimpleNamespace:
        return SimpleNamespace(
            website_ops_root=root,
            website_ops_site_urls=(
                "https://example.com/",
                "https://example.com/services/",
            ),
            website_ops_execute_approved=execute_approved,
        )

    def test_dashboard_render_includes_controls(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = self._settings(Path(tmpdir))
            html = render_dashboard_page(settings)
            self.assertIn("control tower", html)
            self.assertIn("/admin/api/website-ops/run", html)
            self.assertIn("/admin/api/website-ops/feedback", html)

    def test_dashboard_render_uses_latest_report_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = self._settings(Path(tmpdir))
            reports_dir = settings.website_ops_root / "reports" / "daily"
            reports_dir.mkdir(parents=True, exist_ok=True)
            report_md = reports_dir / "2026-03-26-autonomy-report.md"
            report_json = reports_dir / "2026-03-26-autonomy-report.json"
            report_md.write_text("# Autonomy Report\n\nDate: 2026-03-26\nScope: agent-admin daily sweep\n\nSummary paragraph.\n")
            report_json.write_text(
                json.dumps(
                    {
                        "goal": {"primary": "Increase qualified leads."},
                        "pages_reviewed": 7,
                        "pages_healthy": 6,
                        "pages_with_issues": 1,
                        "issues_found": 1,
                        "status": "needs-attention",
                        "action_queue": [
                            {
                                "page_url": "https://anatainc.com/services/shipping/",
                                "page_title": "Shipping services",
                                "section_name": "Hero heading",
                                "before_state": "Old heading",
                                "after_state": "New heading",
                                "reason": "CTR is weak.",
                                "insight_source": "Google Search Console",
                            }
                        ],
                        "support_requests": ["Provide proof assets for shipping."],
                        "start_doing": ["Approve high-confidence structural fixes quickly."],
                        "stop_doing": ["Stop editing healthy pages without evidence."],
                        "do_more_of": ["Provide stronger proof assets."],
                        "page_insights": [
                            {
                                "page_url": "https://anatainc.com/services/shipping/",
                                "bucket": "repair",
                                "score": 61,
                                "search_console": {"impressions": 120, "ctr": 0.01},
                                "ga4": {"sessions": 22, "conversions": 0},
                            }
                        ],
                        "analytics_status": {
                            "search_console": True,
                            "ga4": False,
                            "notes": ["GA4 unavailable"],
                            "project_id": "sdr-support-agent",
                            "client_email": "codex-website-ops@sdr-support-agent.iam.gserviceaccount.com",
                            "search_console_property": "sc-domain:anatainc.com",
                            "ga4_property_id": "372887830",
                        },
                    }
                )
            )
            html = render_dashboard_page(settings)
            self.assertIn("Primary goal", html)
            self.assertIn("Increase qualified leads.", html)
            self.assertIn("Pages reviewed", html)
            self.assertIn("needs attention", html)
            self.assertIn("New heading", html)
            self.assertIn("Provide proof assets for shipping.", html)
            self.assertIn("GA4 unavailable", html)
            self.assertIn("Why this matters", html)
            self.assertIn("Needs setup", html)
            self.assertIn("sdr-support-agent", html)
            self.assertIn("codex-website-ops@sdr-support-agent.iam.gserviceaccount.com", html)

    def test_review_feedback_round_trip_saves_execution_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = self._settings(Path(tmpdir))
            record = save_feedback_record(
                settings,
                {
                    "category": "SEO",
                    "priority": "High",
                    "page_url": "https://anatainc.com/services/shipping/",
                    "summary": "Tighten shipping H1",
                    "details": "Current heading is too weak.",
                },
            )
            result = review_feedback_record(
                settings,
                record["feedback_id"],
                {
                    "status": "approved",
                    "reviewer_name": "SEO Lead",
                    "review_notes": "Use the revised commercial phrase.",
                    "action_type": "replace_primary_heading",
                    "action_value": "Amazon Shipping Operations for Faster Delivery",
                    "target_post_id": "5540",
                },
            )
            self.assertTrue(result.ok)
            updated = load_feedback_records(settings)[0]
            self.assertEqual(updated["status"], "approved")
            self.assertEqual(updated["reviewer_name"], "SEO Lead")
            self.assertEqual(updated["action_type"], "replace_primary_heading")
            self.assertEqual(updated["action_value"], "Amazon Shipping Operations for Faster Delivery")

    def test_render_feedback_detail_page_includes_review_controls(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = self._settings(Path(tmpdir))
            record = save_feedback_record(
                settings,
                {
                    "summary": "Improve AI page heading",
                    "details": "Current H1 is vague.",
                },
            )
            html = render_feedback_detail_page(settings, record["feedback_id"])
            self.assertIn("Submit Review", html)
            self.assertIn("replace_primary_heading", html)

    def test_run_website_ops_marks_error_when_execution_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = self._settings(Path(tmpdir), execute_approved=True)
            record = save_feedback_record(
                settings,
                {
                    "summary": "Apply heading change",
                    "status": "approved",
                    "action_type": "replace_primary_heading",
                    "action_value": "New Heading",
                    "page_url": "https://anatainc.com/services/ai/",
                },
            )
            with mock.patch.object(website_ops, "execute_feedback_action", side_effect=website_ops.ExecutionError("boom")):
                with mock.patch.object(
                    website_ops,
                    "run_daily_report_pipeline",
                    return_value={"report": self._fake_report(), "observations": [], "artifacts": {}},
                ):
                    result = run_website_ops(settings, mode="daily")
            self.assertTrue(result.ok)
            updated = next(item for item in load_feedback_records(settings) if item["feedback_id"] == record["feedback_id"])
            self.assertEqual(updated["status"], "error")
            self.assertIn("boom", updated["execution_error"])

    def test_run_website_ops_enriches_report_with_autonomy_overlay(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = self._settings(Path(tmpdir))
            fake_pipeline = {
                "report": self._fake_report(),
                "observations": [{"url": "https://anatainc.com/services/shipping/", "issues": []}],
                "artifacts": {},
            }
            fake_overlay = {
                "goal": {"primary": "Increase qualified leads."},
                "action_queue": [{"page_url": "https://anatainc.com/services/shipping/"}],
                "analytics_status": {"search_console": False, "ga4": False, "notes": []},
                "support_requests": [],
                "start_doing": [],
                "stop_doing": [],
                "do_more_of": [],
                "page_insights": [],
            }
            with mock.patch("sales_support_agent.services.website_ops.website_ops.run_daily_report_pipeline", return_value=fake_pipeline):
                with mock.patch("sales_support_agent.services.website_ops.build_autonomy_overlay", return_value=fake_overlay):
                    result = run_website_ops(settings, mode="daily")
            self.assertTrue(result.ok)
            assert result.report is not None
            self.assertEqual(result.report["goal"]["primary"], "Increase qualified leads.")
            self.assertEqual(result.report["action_queue"][0]["page_url"], "https://anatainc.com/services/shipping/")

    def test_latest_report_entry_reads_generated_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = self._settings(Path(tmpdir))
            (settings.website_ops_root / "reports" / "daily").mkdir(parents=True, exist_ok=True)
            report_path = settings.website_ops_root / "reports" / "daily" / "2026-03-26-demo-report.md"
            report_path.write_text("# Demo Report\n\nDate: 2026-03-26\nScope: agent-admin daily sweep\n\nSummary paragraph.\n")
            entry = latest_report_entry(settings)
            self.assertIsNotNone(entry)
            assert entry is not None
            self.assertEqual(entry["slug"], "2026-03-26-demo-report")
            self.assertEqual(entry["title"], "Demo Report")


if __name__ == "__main__":
    unittest.main()
