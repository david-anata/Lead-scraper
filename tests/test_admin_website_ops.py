from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from sales_support_agent.services import website_ops_vendor as website_ops
from sales_support_agent.services.website_ops_autonomy import build_autonomy_overlay
from sales_support_agent.services.website_ops_content import clean_generated_content
from sales_support_agent.services.website_ops import (
    get_website_ops_run_state,
    latest_report_entry,
    load_feedback_records,
    load_website_ops_run_state,
    render_dashboard_page,
    render_feedback_detail_page,
    review_feedback_record,
    run_website_ops,
    save_feedback_record,
    website_ops_run_is_due,
    write_website_ops_run_state,
)
from sales_support_agent.services.website_ops_vendor.executor import (
    faq_exists,
    inject_faq_block,
    resolve_insertion_point,
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
                                "action_type": "inject_faq_block",
                                "section_name": "Hero heading",
                                "before_state": "Old heading",
                                "after_state": "Insert structured shipping FAQ",
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
            self.assertIn("Insert structured shipping FAQ", html)
            self.assertIn("Provide proof assets for shipping.", html)
            self.assertIn("GA4 unavailable", html)
            self.assertIn("MVP mode active", html)
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
                    "auto_generated": True,
                    "section_name": "Hero CTA / proof block",
                    "before_state": "40 sessions and 0 conversions",
                    "after_state": "Clarify the offer and strengthen the primary CTA.",
                    "expected_impact": "Higher lead conversion rate from existing traffic.",
                    "confidence": "medium",
                    "suggested_action_type": "inject_faq_block",
                },
            )
            html = render_feedback_detail_page(settings, record["feedback_id"])
            self.assertIn("Submit Review", html)
            self.assertIn("inject_faq_block", html)
            self.assertIn("Approve Recommendation", html)
            self.assertIn("Current state", html)

    def test_review_feedback_approve_autofills_supported_action(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = self._settings(Path(tmpdir), execute_approved=True)
            record = save_feedback_record(
                settings,
                {
                    "summary": "Fix duplicate hero H1",
                    "page_url": "https://anatainc.com/services/ai/",
                    "auto_generated": True,
                    "suggested_action_type": "inject_faq_block",
                    "suggested_action_value": json.dumps({"heading": "AI FAQ", "questions": [{"question": "What does Anata automate?", "answer": "Anata answers directly: workflow automation is implemented safely."}]}),
                    "execution_eligibility": "auto_execute",
                    "before_state": "Contact Us | Faster, Smarter, Intelligent, Data.",
                    "after_state": "Keep one topic-specific H1 and demote the rest to H2.",
                },
            )
            with mock.patch.object(
                website_ops,
                "execute_feedback_action",
                return_value={"executed_at": "2026-03-27T00:00:00Z", "action_type": "inject_faq_block"},
            ):
                result = review_feedback_record(settings, record["feedback_id"], {"status": "approved"})
            self.assertTrue(result.ok)
            updated = load_feedback_records(settings)[0]
            self.assertEqual(updated["status"], "done")
            self.assertEqual(updated["action_type"], "inject_faq_block")

    def test_run_website_ops_marks_error_when_execution_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = self._settings(Path(tmpdir), execute_approved=True)
            record = save_feedback_record(
                settings,
                {
                    "summary": "Apply heading change",
                    "status": "approved",
                    "action_type": "inject_faq_block",
                    "action_value": json.dumps({"heading": "AI FAQ", "questions": [{"question": "What does Anata automate?", "answer": "Anata answers directly: automation is implemented safely."}]}),
                    "execution_eligibility": "auto_execute",
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
                "action_queue": [{"page_url": "https://anatainc.com/services/shipping/", "action_type": "inject_faq_block"}],
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
            self.assertTrue(result.report["action_queue"][0]["feedback_id"])
            records = load_feedback_records(settings)
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0]["automation_key"][:5], "auto-")

    def test_run_state_persists_daily_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = self._settings(Path(tmpdir))
            state = write_website_ops_run_state(
                settings,
                "daily",
                {
                    "status": "succeeded",
                    "run_date": "2026-03-27",
                    "last_started_at": "2026-03-27T00:00:00Z",
                    "last_completed_at": "2026-03-27T00:01:00Z",
                    "last_successful_date": "2026-03-27",
                    "trigger": "visit",
                },
            )
            self.assertEqual(state["status"], "succeeded")
            loaded = load_website_ops_run_state(settings)
            self.assertEqual(loaded["runs"]["daily"]["last_successful_date"], "2026-03-27")
            self.assertFalse(website_ops_run_is_due(settings, "daily", today=date(2026, 3, 27)))
            self.assertTrue(website_ops_run_is_due(settings, "daily", today=date(2026, 3, 28)))

    def test_run_website_ops_preserves_approved_auto_generated_item(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = self._settings(Path(tmpdir))
            existing = save_feedback_record(
                settings,
                {
                    "summary": "Review shipping title",
                    "status": "approved",
                    "automation_key": "auto-keep-approved",
                    "auto_generated": True,
                    "suggested_action_type": "inject_faq_block",
                    "source_report_date": "2026-03-27",
                    "reviewer_name": "SEO Lead",
                },
            )
            fake_pipeline = {"report": self._fake_report(), "observations": [], "artifacts": {}}
            fake_overlay = {
                "goal": {"primary": "Increase qualified leads."},
                "action_queue": [
                    {
                        "page_url": "https://anatainc.com/services/shipping/",
                        "page_title": "Shipping services",
                        "action_type": "inject_faq_block",
                        "section_name": "Title",
                        "after_state": "Tighten the commercial title.",
                        "reason": "CTR is weak.",
                        "insight_source": "Google Search Console",
                    }
                ],
                "analytics_status": {"search_console": True, "ga4": True, "notes": []},
                "support_requests": [],
                "page_insights": [],
            }
            with mock.patch("sales_support_agent.services.website_ops._automation_key", return_value="auto-keep-approved"):
                with mock.patch("sales_support_agent.services.website_ops.website_ops.run_daily_report_pipeline", return_value=fake_pipeline):
                    with mock.patch("sales_support_agent.services.website_ops.build_autonomy_overlay", return_value=fake_overlay):
                        result = run_website_ops(settings, mode="daily")
            self.assertTrue(result.ok)
            updated = next(item for item in load_feedback_records(settings) if item["feedback_id"] == existing["feedback_id"])
            self.assertEqual(updated["status"], "approved")
            self.assertEqual(updated["reviewer_name"], "SEO Lead")

    def test_run_website_ops_reopens_terminal_auto_generated_item_on_later_day(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = self._settings(Path(tmpdir))
            original = save_feedback_record(
                settings,
                {
                    "summary": "Review contact CTA",
                    "status": "done",
                    "automation_key": "auto-reopen",
                    "auto_generated": True,
                    "suggested_action_type": "inject_faq_block",
                    "source_report_date": "2026-03-26",
                },
            )
            fake_pipeline = {"report": self._fake_report(), "observations": [], "artifacts": {}}
            fake_overlay = {
                "goal": {"primary": "Increase qualified leads."},
                "action_queue": [
                    {
                        "page_url": "https://anatainc.com/contact/",
                        "page_title": "Contact",
                        "action_type": "inject_faq_block",
                        "section_name": "Hero CTA",
                        "after_state": "Strengthen contact proof block.",
                        "reason": "Traffic is not converting.",
                        "insight_source": "Google Analytics 4",
                    }
                ],
                "analytics_status": {"search_console": True, "ga4": True, "notes": []},
                "support_requests": [],
                "page_insights": [],
            }
            with mock.patch("sales_support_agent.services.website_ops._automation_key", return_value="auto-reopen"):
                with mock.patch("sales_support_agent.services.website_ops._utc_now", return_value=datetime(2026, 3, 27, 12, 0, tzinfo=timezone.utc)):
                    with mock.patch("sales_support_agent.services.website_ops.website_ops.run_daily_report_pipeline", return_value=fake_pipeline):
                        with mock.patch("sales_support_agent.services.website_ops.build_autonomy_overlay", return_value=fake_overlay):
                            result = run_website_ops(settings, mode="daily")
            self.assertTrue(result.ok)
            records = load_feedback_records(settings)
            self.assertEqual(len(records), 2)
            reopened = next(item for item in records if item["feedback_id"] != original["feedback_id"])
            self.assertEqual(reopened["status"], "new")
            self.assertEqual(reopened["reopened_from_feedback_id"], original["feedback_id"])
            self.assertEqual(reopened["reopened_reason"], "recommendation_reappeared")

    def test_build_autonomy_overlay_generates_mvp_only_actions(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = self._settings(Path(tmpdir))
            observations = [
                {
                    "url": "https://anatainc.com/services/fulfillment/",
                    "title": "Fulfillment",
                    "issues": [],
                }
            ]
            with mock.patch(
                "sales_support_agent.services.website_ops_autonomy.fetch_search_console_snapshot",
                return_value=(
                    {
                        "https://anatainc.com/services/fulfillment": {
                            "impressions": 140.0,
                            "clicks": 2.0,
                            "ctr": 0.014,
                            "position": 18.0,
                            "top_queries": [{"query": "amazon fulfillment services", "impressions": 80.0, "clicks": 2.0}],
                        }
                    },
                    [],
                ),
            ):
                with mock.patch(
                    "sales_support_agent.services.website_ops_autonomy.fetch_ga4_snapshot",
                    return_value=(
                        {
                            "https://anatainc.com/services/fulfillment": {
                                "sessions": 42.0,
                                "engaged_sessions": 30.0,
                                "lead_conversions": 0.0,
                                "lead_conversion_rate": 0.0,
                                "trust_status": "partial",
                            }
                        },
                        [],
                    ),
                ):
                    with mock.patch(
                        "sales_support_agent.services.website_ops_autonomy.collect_customer_questions",
                        return_value=[
                            {
                                "question_id": "cq_1",
                                "question": "How fast can onboarding happen?",
                                "intent": "transactional",
                                "frequency": 4,
                                "source": "gmail",
                                "related_service": "fulfillment",
                            }
                        ],
                    ):
                        with mock.patch(
                            "sales_support_agent.services.website_ops_autonomy.build_blueprint",
                            return_value={
                                "blueprint_id": "bp_1",
                                "query": "amazon fulfillment services",
                                "source_urls": ["https://example.com/a", "https://example.com/b"],
                                "heading_structure": [{"heading": "What is Amazon Fulfillment?", "level": "h2", "support_count": 3}],
                                "faq_patterns": [{"question": "How fast is onboarding?", "support_count": 2}],
                                "content_gaps": ["Missing onboarding timeline section"],
                            },
                        ):
                            overlay = build_autonomy_overlay(
                                settings=settings,
                                report=self._fake_report(),
                                observations=observations,
                                feedback_entries=[],
                            )
            self.assertEqual(overlay["analytics_status"]["ga4_trust_status"], "partial")
            self.assertEqual(overlay["analytics_status"]["primary_lead_event"], "generate_lead")
            action_types = {item["action_type"] for item in overlay["action_queue"]}
            self.assertEqual(action_types, {"inject_faq_block", "expand_service_page_section"})
            faq_action = next(item for item in overlay["action_queue"] if item["action_type"] == "inject_faq_block")
            section_action = next(item for item in overlay["action_queue"] if item["action_type"] == "expand_service_page_section")
            self.assertEqual(faq_action["execution_eligibility"], "auto_execute")
            self.assertEqual(section_action["execution_eligibility"], "approval_required")
            self.assertTrue(faq_action["evidence"])
            self.assertTrue(faq_action["verification_requirements"])

    def test_clean_generated_content_sanitizes_and_shortens(self) -> None:
        cleaned = clean_generated_content(
            "Search Atlas says this very long sentence should keep going well past any normal reader tolerance and keep naming competitor brands while avoiding a direct answer entirely."
        )
        self.assertNotIn("Search Atlas", cleaned)
        self.assertIn("competitor", cleaned.lower())
        self.assertLessEqual(len(cleaned.split()), 28)

    def test_faq_exists_detects_copy_and_schema(self) -> None:
        self.assertTrue(faq_exists('<section class="anata-faq"><h2>FAQ</h2></section>'))
        self.assertTrue(faq_exists("<h2>Frequently Asked Questions</h2>"))
        self.assertTrue(faq_exists('<script type="application/ld+json">{"@type":"FAQPage"}</script>'))
        self.assertFalse(faq_exists("<div><h2>Overview</h2></div>"))

    def test_resolve_insertion_point_prefers_major_section_then_cta_then_end(self) -> None:
        after_section = resolve_insertion_point("<h1>Title</h1><p>Intro copy.</p><div>More</div>")
        self.assertEqual(after_section["strategy"], "after_first_major_section")
        before_cta = resolve_insertion_point("<h1>Title</h1><div>Book a call</div>")
        self.assertEqual(before_cta["strategy"], "before_cta")
        fallback = resolve_insertion_point("Plain content with no markers")
        self.assertEqual(fallback["strategy"], "end_of_content")

    def test_inject_faq_block_creates_expected_html(self) -> None:
        elements = [
            {
                "id": "heading-1",
                "elType": "widget",
                "widgetType": "heading",
                "settings": {"title": "Fulfillment Services", "header_size": "h1"},
                "elements": [],
            },
            {
                "id": "text-1",
                "elType": "widget",
                "widgetType": "text-editor",
                "settings": {"editor": "<p>Intro copy.</p>"},
                "elements": [],
            },
        ]
        updated, summary = inject_faq_block(
            elements,
            {
                "heading": "Fulfillment FAQ",
                "questions": [
                    {
                        "question": "How fast can onboarding happen?",
                        "answer": "Anata answers directly: onboarding can start quickly after discovery and implementation planning.",
                    }
                ],
            },
        )
        html_output = json.dumps(updated)
        self.assertIn("anata-faq", html_output)
        self.assertIn("Fulfillment FAQ", html_output)
        self.assertEqual(summary["after_faq_count"], 1)

    def test_build_autonomy_overlay_generates_phase_one_faq_task(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = self._settings(Path(tmpdir))
            observations = [
                {
                    "url": "https://anatainc.com/services/fulfillment/",
                    "title": "Fulfillment",
                    "issues": [],
                    "text_length": 9042,
                    "h2": ["Fulfillment capabilities", "Onboarding and support"],
                    "h3": ["Inventory management", "Customer satisfaction"],
                }
            ]
            with mock.patch(
                "sales_support_agent.services.website_ops_autonomy.fetch_search_console_snapshot",
                return_value=(
                    {
                        "https://anatainc.com/services/fulfillment": {
                            "impressions": 124.0,
                            "clicks": 1.0,
                            "ctr": 0.0081,
                            "position": 18.0,
                            "top_queries": [],
                        }
                    },
                    [],
                ),
            ):
                with mock.patch(
                    "sales_support_agent.services.website_ops_autonomy.fetch_ga4_snapshot",
                    return_value=(
                        {
                            "https://anatainc.com/services/fulfillment": {
                                "sessions": 42.0,
                                "engaged_sessions": 30.0,
                                "lead_conversions": 0.0,
                                "lead_conversion_rate": 0.0,
                                "trust_status": "partial",
                            }
                        },
                        [],
                    ),
                ):
                    with mock.patch(
                        "sales_support_agent.services.website_ops_autonomy.collect_customer_questions",
                        return_value=[],
                    ):
                        with mock.patch(
                            "sales_support_agent.services.website_ops_autonomy.build_blueprint",
                            return_value={
                                "blueprint_id": "bp_1",
                                "query": "Fulfillment",
                                "source_urls": [],
                                "heading_structure": [],
                                "faq_patterns": [],
                                "content_gaps": ["SERP leaders frequently open with a direct definition block."],
                            },
                        ):
                            overlay = build_autonomy_overlay(
                                settings=settings,
                                report=self._fake_report(),
                                observations=observations,
                                feedback_entries=[],
                            )
            faq_action = next(item for item in overlay["action_queue"] if item["action_type"] == "inject_faq_block")
            self.assertEqual(faq_action["execution_eligibility"], "auto_execute")
            self.assertEqual({item["action_type"] for item in overlay["action_queue"]}, {"inject_faq_block"})
            self.assertFalse(overlay["customer_questions"])
            self.assertTrue(overlay["serp_blueprints"])
            self.assertTrue(overlay["content_tasks"])
            insight = overlay["page_insights"][0]
            self.assertEqual(insight["customer_question_count"], 0)
            self.assertTrue(insight["blueprint_found"])
            self.assertTrue(insight["faq_demand_detected"])
            self.assertFalse(insight["page_thin_enough"])
            self.assertEqual(insight["query_seed"], "Fulfillment")
            self.assertEqual(insight["task_block_reason"], "")

    def test_dashboard_render_shows_mvp_debug_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = self._settings(Path(tmpdir))
            reports_dir = settings.website_ops_root / "reports" / "daily"
            reports_dir.mkdir(parents=True, exist_ok=True)
            report_md = reports_dir / "2026-03-28-debug-report.md"
            report_json = reports_dir / "2026-03-28-debug-report.json"
            report_md.write_text("# Debug Report\n\nDate: 2026-03-28\nScope: agent-admin daily sweep\n\nSummary paragraph.\n")
            report_json.write_text(
                json.dumps(
                    {
                        "goal": {"primary": "Increase qualified leads."},
                        "action_queue": [],
                        "content_tasks": [],
                        "support_requests": [],
                        "analytics_status": {"search_console": True, "ga4": True, "notes": []},
                        "page_insights": [
                            {
                                "page_url": "https://anatainc.com/services/fulfillment/",
                                "page_title": "Fulfillment",
                                "bucket": "convert",
                                "score": 73,
                                "search_console": {"impressions": 124, "ctr": 0.0081},
                                "ga4": {"sessions": 43, "lead_conversions": 0},
                                "ga4_trust_status": "partial",
                                "customer_question_count": 0,
                                "blueprint_found": True,
                                "faq_demand_detected": True,
                                "page_thin_enough": False,
                                "task_block_reason": "The page is not thin enough for MVP section expansion.",
                                "query_seed": "Fulfillment",
                            }
                        ],
                    }
                )
            )
            html = render_dashboard_page(settings)
            self.assertIn("Questions", html)
            self.assertIn("Blueprint", html)
            self.assertIn("FAQ Demand", html)
            self.assertIn("Task block reason", html)
            self.assertIn("The page is not thin enough for MVP section expansion.", html)

    def test_run_website_ops_auto_executes_new_high_confidence_action(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = self._settings(Path(tmpdir), execute_approved=True)
            fake_pipeline = {
                "report": self._fake_report(),
                "observations": [{"url": "https://anatainc.com/services/shipping/", "title": "Shipping", "issues": []}],
                "artifacts": {},
            }
            fake_overlay = {
                "goal": {"primary": "Increase qualified leads."},
                "action_queue": [
                    {
                        "page_url": "https://anatainc.com/services/shipping/",
                        "page_title": "Shipping",
                        "action_type": "inject_faq_block",
                        "section_name": "FAQ block",
                        "before_state": "No structured FAQ block.",
                        "after_state": "Insert FAQ block from buyer questions.",
                        "reason": "CTR is weak against meaningful impressions and buyers keep asking the same questions.",
                        "insight_source": "SERP + Customer Language",
                        "expected_impact": "Broader query coverage and stronger direct-answer content.",
                        "confidence": "high",
                        "requires_approval": False,
                        "evidence": ["120 impressions", "4 repeated buyer questions"],
                        "execution_eligibility": "auto_execute",
                        "target_region": "FAQ insertion zone",
                        "verification_requirements": ["FAQ section exists after insert"],
                        "action_value": json.dumps({"heading": "Shipping FAQ", "questions": [{"question": "How fast is shipping setup?", "answer": "Anata answers directly: shipping setup starts with carrier and workflow planning."}]}),
                    }
                ],
                "analytics_status": {
                    "search_console": True,
                    "ga4": True,
                    "notes": [],
                    "ga4_trust_status": "trusted",
                    "primary_lead_event": "generate_lead",
                },
                "support_requests": [],
                "page_insights": [],
            }
            with mock.patch("sales_support_agent.services.website_ops.website_ops.run_daily_report_pipeline", return_value=fake_pipeline):
                with mock.patch("sales_support_agent.services.website_ops.build_autonomy_overlay", return_value=fake_overlay):
                    with mock.patch.object(
                        website_ops,
                        "execute_feedback_action",
                        return_value={"executed_at": "2026-03-27T00:00:00Z", "action_type": "inject_faq_block"},
                    ):
                        result = run_website_ops(settings, mode="daily")
            self.assertTrue(result.ok)
            records = load_feedback_records(settings)
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0]["status"], "done")
            self.assertEqual(records[0]["action_type"], "inject_faq_block")

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
