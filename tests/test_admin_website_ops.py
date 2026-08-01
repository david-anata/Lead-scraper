from __future__ import annotations

import json
import io
import os
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest import mock
from zoneinfo import ZoneInfo

from fastapi import FastAPI
from fastapi.testclient import TestClient

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from sales_support_agent.services import website_ops_vendor as website_ops
from sales_support_agent.services.website_ops_autonomy import (
    _load_service_account_info,
    _deterministic_metadata_actions,
    build_autonomy_overlay,
    inspect_search_console_indexing,
    refresh_retained_indexing_inventory,
    save_search_console_indexing_inventory,
    submit_search_console_sitemap,
)
from sales_support_agent.services.website_ops_content import clean_generated_content
from sales_support_agent.services.website_ops_article_engine import build_article_action
from sales_support_agent.services.website_ops import (
    _execute_record,
    discover_website_ops_urls,
    execute_approved_website_ops_actions,
    get_website_ops_run_state,
    latest_report_entry,
    load_feedback_records,
    load_website_ops_run_state,
    render_dashboard_page,
    render_feedback_detail_page,
    render_indexing_page,
    render_query_map_page,
    render_queue_page,
    render_report_page,
    reconcile_missing_generated_articles,
    review_feedback_record,
    run_website_ops,
    save_feedback_record,
    send_website_ops_report_email,
    website_ops_operating_state,
    website_ops_run_is_due,
    write_website_ops_run_state,
)
from sales_support_agent.services.website_ops_vendor.executor import (
    ExecutionError,
    execute_feedback_action,
    execution_target_details,
    faq_exists,
    inject_faq_block,
    resolve_insertion_point,
)
from sales_support_agent.api.website_ops_jobs_router import (
    _daily_run_has_verified_outcome,
    _daily_run_is_fresh,
    _latest_autonomous_execution_error,
    _run_due_modes,
    _run_embedded_pulse,
    _scheduled_modes,
    router as website_ops_jobs_router,
)
from sales_support_agent.services.website_ops_github import (
    execute_github_article_action,
    execute_github_metadata_action,
    generated_article_identities,
    route_source_path,
    update_generated_article_registry,
    update_static_metadata_source,
    validate_generated_article,
    validate_metadata_action,
)
from sales_support_agent.services.website_ops_program import (
    build_indexing_inventory,
    reconcile_indexing_inventory,
)


class AdminWebsiteOpsTests(unittest.TestCase):
    def test_missing_verified_article_is_requeued_from_validated_payload(self) -> None:
        article = json.loads(str(self._generated_article_record()["action_value"]))
        record = {
            "feedback_id": "published-article",
            "status": "done",
            "action_type": "publish_blog_article",
            "action_value": json.dumps(article),
            "execution_result": {"verification_status": "verified"},
        }
        updated = {**record, "status": "approved"}
        with mock.patch(
            "sales_support_agent.services.website_ops.github_metadata_is_configured",
            return_value=True,
        ), mock.patch(
            "sales_support_agent.services.website_ops.load_generated_article_identities",
            return_value={"slugs": set(), "evidence_ids": set(), "primary_intents": set()},
        ), mock.patch(
            "sales_support_agent.services.website_ops.load_feedback_records",
            return_value=[record],
        ), mock.patch.object(
            website_ops,
            "update_feedback_entry",
            return_value=updated,
        ) as update:
            reopened = reconcile_missing_generated_articles(SimpleNamespace())

        self.assertEqual(reopened, [updated])
        self.assertEqual(update.call_args.args[1]["status"], "approved")
        self.assertIn("missing from the durable production registry", update.call_args.args[1]["review_notes"])

    def test_durable_article_registry_entry_is_not_requeued(self) -> None:
        article = json.loads(str(self._generated_article_record()["action_value"]))
        record = {
            "feedback_id": "published-article",
            "status": "done",
            "action_type": "publish_blog_article",
            "action_value": json.dumps(article),
        }
        with mock.patch(
            "sales_support_agent.services.website_ops.github_metadata_is_configured",
            return_value=True,
        ), mock.patch(
            "sales_support_agent.services.website_ops.load_generated_article_identities",
            return_value={"slugs": {article["slug"]}},
        ), mock.patch(
            "sales_support_agent.services.website_ops.load_feedback_records",
            return_value=[record],
        ), mock.patch.object(website_ops, "update_feedback_entry") as update:
            reopened = reconcile_missing_generated_articles(SimpleNamespace())

        self.assertEqual(reopened, [])
        update.assert_not_called()

    def test_daily_pulse_retries_zero_delta_until_production_is_verified(self) -> None:
        settings = SimpleNamespace()
        active = SimpleNamespace(
            ok=True,
            message="work remains",
            report={
                "run_outcome": {
                    "status": "work_in_progress",
                    "production_delta_count": 0,
                }
            },
        )
        verified = SimpleNamespace(
            ok=True,
            message="published",
            report={
                "run_outcome": {
                    "status": "production_verified",
                    "production_delta_count": 1,
                }
            },
        )
        with (
            mock.patch(
                "sales_support_agent.api.website_ops_jobs_router.get_website_ops_run_state",
                return_value={},
            ),
            mock.patch(
                "sales_support_agent.api.website_ops_jobs_router.write_website_ops_run_state"
            ),
            mock.patch(
                "sales_support_agent.api.website_ops_jobs_router.run_website_ops",
                side_effect=[active, verified],
            ) as run,
        ):
            result = _run_due_modes(
                settings,
                ["daily"],
                trigger="test",
                force=True,
            )

        self.assertEqual(run.call_count, 2)
        self.assertEqual(result["daily"]["status"], "succeeded")
        self.assertEqual(result["daily"]["attempts"], 2)

    def test_daily_pulse_fails_truthfully_after_zero_delta_retries_exhaust(self) -> None:
        settings = SimpleNamespace()
        active = SimpleNamespace(
            ok=True,
            message="work remains",
            report={
                "run_outcome": {
                    "status": "work_in_progress",
                    "production_delta_count": 0,
                }
            },
        )
        with (
            mock.patch(
                "sales_support_agent.api.website_ops_jobs_router.get_website_ops_run_state",
                return_value={},
            ),
            mock.patch(
                "sales_support_agent.api.website_ops_jobs_router.write_website_ops_run_state"
            ),
            mock.patch(
                "sales_support_agent.api.website_ops_jobs_router.send_website_ops_failure_email"
            ) as failure_email,
            mock.patch(
                "sales_support_agent.api.website_ops_jobs_router.run_website_ops",
                return_value=active,
            ) as run,
        ):
            result = _run_due_modes(
                settings,
                ["daily"],
                trigger="test",
                force=True,
            )

        self.assertEqual(run.call_count, 3)
        self.assertEqual(result["daily"]["status"], "failed_outcome")
        self.assertEqual(result["daily"]["attempts"], 3)
        failure_email.assert_called_once()

    def test_generated_article_identity_parser_reads_registry_contract(self) -> None:
        source = '''
// WEBSITE_OPS_GENERATED_ARTICLES_START
export const GENERATED_ARTICLES = [
  {"slug":"existing","primaryIntent":"Existing Intent","evidenceId":"evidence-1"},
] as const;
// WEBSITE_OPS_GENERATED_ARTICLES_END
'''
        identities = generated_article_identities(source)
        self.assertEqual(identities["slugs"], {"existing"})
        self.assertEqual(identities["evidence_ids"], {"evidence-1"})
        self.assertEqual(identities["primary_intents"], {"existing intent"})

    def test_health_selects_latest_autonomous_execution_error(self) -> None:
        with mock.patch(
            "sales_support_agent.api.website_ops_jobs_router.load_feedback_records",
            return_value=[
                {
                    "auto_generated": True,
                    "status": "error",
                    "action_type": "publish_blog_article",
                    "page_url": "https://anatainc.com/blog/older",
                    "execution_error": "older failure",
                    "last_execution_at": "2026-08-01T17:00:00+00:00",
                },
                {
                    "auto_generated": True,
                    "status": "error",
                    "action_type": "publish_blog_article",
                    "page_url": "https://anatainc.com/blog/newer",
                    "execution_error": "production title did not match",
                    "last_execution_at": "2026-08-01T18:00:00+00:00",
                },
            ],
        ):
            latest = _latest_autonomous_execution_error(SimpleNamespace())

        self.assertEqual(latest["page_url"], "https://anatainc.com/blog/newer")
        self.assertEqual(latest["error"], "production title did not match")

    def test_autonomous_execution_error_is_recorded_and_rethrown_for_retry(self) -> None:
        settings = SimpleNamespace(website_ops_root=Path("runtime/test-website-ops"))
        record = {
            "feedback_id": "article-retry",
            "status": "approved",
            "action_type": "publish_blog_article",
            "suggested_action_type": "publish_blog_article",
            "execution_eligibility": "auto_execute",
            "action_value": json.dumps({"evidenceId": "retry-topic"}),
        }
        with (
            mock.patch(
                "sales_support_agent.services.website_ops._execute_feedback_action",
                side_effect=website_ops.ExecutionError("production verification failed"),
            ),
            mock.patch(
                "sales_support_agent.services.website_ops._release_failed_article_claim",
            ) as release,
            mock.patch.object(website_ops, "update_feedback_entry") as update,
            self.assertRaises(website_ops.ExecutionError),
        ):
            _execute_record(
                settings,
                SimpleNamespace(),
                record,
                raise_on_error=True,
            )

        release.assert_called_once_with(settings, record)
        self.assertEqual(update.call_args.args[1]["status"], "error")

    def test_google_credential_loader_accepts_render_multiline_object_format(self) -> None:
        payload = _load_service_account_info(
            """{
  type: "service_account",
  project_id: "anata-project",
  client_email: "website-ops@example.iam.gserviceaccount.com",
  private_key: "-----BEGIN PRIVATE KEY-----
example
-----END PRIVATE KEY-----
",
  token_uri: "https://oauth2.googleapis.com/token",
}"""
        )
        self.assertEqual(payload["project_id"], "anata-project")
        self.assertIn("BEGIN PRIVATE KEY", payload["private_key"])

    def test_url_inspection_builds_and_persists_indexing_inventory(self) -> None:
        class Response:
            ok = True
            status_code = 200

            def json(self) -> dict[str, object]:
                return {
                    "inspectionResult": {
                        "indexStatusResult": {
                            "verdict": "FAIL",
                            "coverageState": "Blocked due to access forbidden (403)",
                            "lastCrawlTime": "2026-07-27T12:00:00Z",
                            "robotsTxtState": "ALLOWED",
                            "indexingState": "INDEXING_ALLOWED",
                            "pageFetchState": "ACCESS_FORBIDDEN",
                            "googleCanonical": "https://anatainc.com/services/amazon-seo",
                            "userCanonical": "https://anatainc.com/services/amazon-seo",
                            "crawledAs": "DESKTOP",
                        }
                    }
                }

        with tempfile.TemporaryDirectory() as tmpdir:
            settings = SimpleNamespace(
                website_ops_root=Path(tmpdir),
                google_service_account_json='{"client_email":"a","private_key":"b","token_uri":"c"}',
                website_ops_gsc_property="sc-domain:anatainc.com",
                website_ops_ga4_property_id="123",
                website_ops_lookback_days=28,
                website_ops_ga4_primary_lead_event="generate_lead",
            )
            with mock.patch(
                "sales_support_agent.services.website_ops_autonomy._google_access_token",
                return_value="token",
            ):
                inventory, notes = inspect_search_console_indexing(
                    settings,
                    [
                        "https://anatainc.com/services/amazon-seo",
                        "https://agent.anatainc.com/admin/website-ops",
                    ],
                    requester=lambda *args, **kwargs: Response(),
                )
            self.assertEqual(notes, [])
            self.assertEqual(inventory["inspection"]["attempted"], 1)
            self.assertEqual(inventory["summary"]["needs_action"], 1)
            record = inventory["records"][0]
            self.assertEqual(record["page_fetch_state"], "ACCESS_FORBIDDEN")
            self.assertEqual(
                record["google_canonical"],
                "https://anatainc.com/services/amazon-seo",
            )
            save_search_console_indexing_inventory(settings, inventory)
            saved = json.loads(
                (Path(tmpdir) / "indexing" / "inventory.json").read_text()
            )
            self.assertEqual(saved["inspection"]["succeeded"], 1)

    def test_indexing_reconciliation_separates_current_200_from_historical_404(
        self,
    ) -> None:
        inventory = build_indexing_inventory(
            [
                {
                    "url": "https://anatainc.com/case-studies",
                    "reason": "Not found (404)",
                },
                {
                    "url": "https://anatainc.com/about",
                    "reason": "Submitted and indexed",
                },
            ]
        )

        reconciled = reconcile_indexing_inventory(
            inventory,
            [
                {
                    "url": "https://anatainc.com/case-studies",
                    "status_code": 200,
                },
                {"url": "https://anatainc.com/about", "status_code": 200},
            ],
        )

        by_url = {item["url"]: item for item in reconciled["records"]}
        self.assertEqual(
            by_url["https://anatainc.com/case-studies"]["desired_state"],
            "recrawl pending",
        )
        self.assertEqual(reconciled["summary"]["indexed"], 1)
        self.assertEqual(reconciled["summary"]["needs_action"], 1)

    def test_sitemap_submission_uses_canonical_production_feed(self) -> None:
        captured: dict[str, object] = {}

        class Response:
            ok = True
            status_code = 204

        def requester(url: str, **kwargs: object) -> Response:
            captured["url"] = url
            captured.update(kwargs)
            return Response()

        settings = SimpleNamespace(
            google_service_account_json='{"client_email":"a","private_key":"b","token_uri":"c"}',
            website_ops_gsc_property="sc-domain:anatainc.com",
            website_ops_ga4_property_id="123",
            website_ops_lookback_days=28,
            website_ops_ga4_primary_lead_event="generate_lead",
        )
        with mock.patch(
            "sales_support_agent.services.website_ops_autonomy._google_access_token",
            return_value="write-token",
        ):
            result = submit_search_console_sitemap(
                settings,
                requester=requester,
            )

        self.assertEqual(result["status"], "submitted")
        self.assertIn(
            "sc-domain%3Aanatainc.com/sitemaps/"
            "https%3A%2F%2Fanatainc.com%2Fsitemap.xml",
            str(captured["url"]),
        )
        self.assertEqual(
            captured["headers"],
            {"Authorization": "Bearer write-token"},
        )

    def test_daily_index_refresh_reconciles_and_submits_once(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = SimpleNamespace(website_ops_root=Path(tmpdir))
            save_search_console_indexing_inventory(
                settings,
                build_indexing_inventory(
                    [
                        {
                            "url": "https://anatainc.com/case-studies",
                            "reason": "Not found (404)",
                        }
                    ]
                ),
            )
            with mock.patch(
                "sales_support_agent.services.website_ops_autonomy.submit_search_console_sitemap",
                return_value={
                    "status": "submitted",
                    "sitemap_url": "https://anatainc.com/sitemap.xml",
                },
            ) as submit:
                refreshed, notes = refresh_retained_indexing_inventory(
                    settings,
                    [
                        {
                            "url": "https://anatainc.com/case-studies",
                            "status_code": 200,
                        }
                    ],
                )
                refreshed_again, second_notes = refresh_retained_indexing_inventory(
                    settings,
                    [
                        {
                            "url": "https://anatainc.com/case-studies",
                            "status_code": 200,
                        }
                    ],
                )

            self.assertEqual(notes, [])
            self.assertEqual(second_notes, [])
            self.assertEqual(
                refreshed["records"][0]["desired_state"],
                "recrawl pending",
            )
            self.assertEqual(
                refreshed_again["sitemap_submission"]["status"],
                "submitted",
            )
            submit.assert_called_once()

    def _generated_article_record(self) -> dict[str, object]:
        tldr = (
            "Structure Amazon PPC campaigns around one measurable purpose, clear ownership, "
            "and a defined decision rule. Separate discovery, branded defense, category growth, "
            "and product-level efficiency so budget changes have an interpretable result. Review "
            "search terms, placement performance, conversion rate, and contribution margin before "
            "changing bids or moving products. The right account structure makes waste visible, "
            "keeps experiments isolated, and gives operators a repeatable way to decide what to "
            "scale, repair, or stop."
        )
        section_names = (
            "Define campaign ownership",
            "Separate discovery from control",
            "Use evidence for budget decisions",
            "Review failure modes",
        )
        sections = [
            {
                "heading": heading,
                "paragraphs": [
                    (
                        f"{heading} principle {paragraph_index + 1}. "
                        + "Each campaign needs one operating purpose, an accountable owner, a "
                        "measurement window, and a decision threshold. Operators should document "
                        "the search terms included, the products promoted, the bidding constraint, "
                        "and the result that would justify more budget. This makes later changes "
                        "traceable and prevents unrelated goals from competing inside one campaign. "
                        "The review should connect advertising performance to conversion behavior "
                        "and contribution margin rather than treating clicks as the final outcome. "
                        "Teams should also record exclusions, placement assumptions, inventory limits, "
                        "and the exact date when the next evidence review will change the operating decision. "
                        "A useful review note should explain the decision in plain language, name the evidence that changed it, and preserve the previous setting so the team can compare outcomes without losing operational context. "
                    )
                    for paragraph_index in range(2)
                ],
            }
            for section_index, heading in enumerate(section_names)
        ]
        for section_index, section in enumerate(sections):
            source_title = "Amazon Ads campaign guidance" if section_index % 2 == 0 else "Google Search documentation"
            source_url = "https://advertising.amazon.com/library/guides" if section_index % 2 == 0 else "https://developers.google.com/search/docs"
            link_title = "Amazon advertising management" if section_index % 2 == 0 else "Amazon PPC management"
            link_href = "/services/amazon-advertising" if section_index % 2 == 0 else "/services/amazon-ppc-management"
            section["citations"] = [
                {"title": source_title, "href": source_url}
            ]
            section["internalLinks"] = [
                {"title": link_title, "href": link_href}
            ]
        article = {
            "slug": "amazon-ppc-account-structure",
            "primaryIntent": "how to structure an amazon ppc account",
            "evidenceId": "cluster-123",
            "generatedAt": "2026-07-27T14:00:00+00:00",
            "publishedAt": "2026-07-27T14:00:00+00:00",
            "modifiedAt": "2026-07-27T14:00:00+00:00",
            "author": {
                "type": "Organization",
                "name": "Anata Inc.",
                "url": "https://anatainc.com",
            },
            "title": "How to Structure an Amazon PPC Account",
            "description": (
                "A practical framework for organizing Amazon PPC campaigns around "
                "clear ownership, measurement, and ongoing optimization."
            ),
            "content": {
                "route": "/blog/amazon-ppc-account-structure",
                "eyebrow": "Amazon advertising",
                "h1": "How to Structure an Amazon PPC Account",
                "tldr": {"heading": "The short answer.", "answer": [tldr]},
                "sections": sections,
                "breadcrumbs": [
                    {"name": "Home", "href": "/"},
                    {"name": "Blog", "href": "/blog"},
                    {"name": "Amazon PPC account structure"},
                ],
                "schemaType": "article",
                "articleTitle": "How to Structure an Amazon PPC Account",
                "articleDescription": "A practical evidence-backed campaign structure.",
                "related": [
                    {
                        "title": "Amazon advertising management",
                        "href": "/services/amazon-advertising",
                    },
                    {
                        "title": "Amazon PPC management",
                        "href": "/services/amazon-ppc-management",
                    },
                ],
            },
            "sources": [
                {"title": "Amazon Ads campaign guidance", "url": "https://advertising.amazon.com/library/guides"},
                {"title": "Google Search documentation", "url": "https://developers.google.com/search/docs"},
            ],
        }
        return {
            "action_value": json.dumps(article),
            "confidence": "high",
            "reason": "Two independent observed sources show an unowned informational intent.",
            "evidence": ["Search Console cluster-123", "Observed buyer question cluster-123"],
        }

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
            self.assertIn("Continuous website", html)
            self.assertIn("Continuous optimization loop", html)
            self.assertIn("Repair Google connections", html)
            self.assertIn('action="/admin/api/website-ops/run"', html)
            self.assertIn("Run Daily Sweep", html)
            self.assertIn("Weekly sweep unavailable", html)
            self.assertIn("/admin/api/website-ops/feedback", html)
            self.assertIn("hourly pulses from 8:00 AM through 3:00 PM America/Denver", html)

    def test_query_map_renders_evidence_ownership_and_shadow_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            settings = self._settings(root)
            intelligence_root = root / "query_intelligence"
            intelligence_root.mkdir(parents=True)
            (intelligence_root / "snapshot.json").write_text(
                json.dumps(
                    {
                        "status": "ready",
                        "summary": {
                            "validated_clusters": 1,
                            "hypothesis_clusters": 2,
                            "ownership_conflicts": 0,
                            "cited_clusters": 1,
                            "weekly_validation_cycles": 1,
                        },
                        "clusters": [
                            {
                                "cluster_id": "cluster-1",
                                "label": "amazon ppc agency",
                                "intent": "commercial",
                                "funnel_stage": "consideration",
                                "validation_status": "validated",
                                "evidence_classes": [
                                    "observed_search",
                                    "simulated",
                                ],
                                "owner_url": "https://anatainc.com/services/amazon-ppc-management",
                                "ownership_status": "assigned",
                                "conflict_urls": [],
                                "citation": {"status": "cited"},
                                "observed_impressions": 42,
                            }
                        ],
                        "recommendations": [
                            {
                                "query_cluster": "amazon ppc agency",
                                "page_url": "https://anatainc.com/services/amazon-ppc-management",
                                "target": "title",
                                "action_type": "meta_title_update",
                                "reason": "The validated cluster is not aligned with the title.",
                                "execution_status": "shadow",
                                "block_reasons": [
                                    "Two comparable weekly shadow-mode cycles have not completed."
                                ],
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            page = render_query_map_page(settings)
            self.assertIn("One query cluster.", page)
            self.assertIn("Observed Search", page)
            self.assertIn("amazon ppc agency", page)
            self.assertIn("Cited", page)
            self.assertIn("Shadow", page)
            self.assertIn("Two comparable weekly", page)
            self.assertIn("Earned citation monitoring", page)
            self.assertIn("Observed business outcomes", page)
            self.assertIn("cannot buy, exchange, or automatically create", page)

    def test_sitemap_discovery_restricts_scope_and_private_routes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = SimpleNamespace(
                website_ops_root=Path(tmpdir),
                website_ops_site_urls=("https://anatainc.com/",),
                website_ops_sitemap_url="https://anatainc.com/sitemap.xml",
                website_ops_allowed_host="anatainc.com",
            )
            sitemap = b"""<?xml version="1.0"?>
            <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
              <url><loc>https://anatainc.com/</loc></url>
              <url><loc>https://anatainc.com/services</loc></url>
              <url><loc>https://anatainc.com/book</loc></url>
              <url><loc>https://app.anatainc.com/register</loc></url>
            </urlset>"""
            with mock.patch(
                "sales_support_agent.services.website_ops.urllib.request.urlopen",
                return_value=io.BytesIO(sitemap),
            ):
                urls = discover_website_ops_urls(settings)
            self.assertEqual(
                urls,
                ("https://anatainc.com/", "https://anatainc.com/services"),
            )

    def test_daily_email_sends_only_when_meaningful_state_changes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = SimpleNamespace(
                website_ops_root=Path(tmpdir),
                website_ops_report_email_to=("david@anatainc.com",),
                website_ops_email_from="Anata Agent <noreply@anatainc.com>",
                resend_api_key="test-key",
                resend_from="Anata Agent <noreply@anatainc.com>",
            )
            report = self._fake_report()
            with mock.patch(
                "sales_support_agent.services.website_ops.ResendClient.send_message",
                return_value="email-1",
            ) as send:
                first = send_website_ops_report_email(settings, mode="daily", report=report)
                second = send_website_ops_report_email(settings, mode="daily", report=report)
            self.assertTrue(first["sent"])
            self.assertFalse(second["sent"])
            self.assertFalse(second["changed"])
            self.assertEqual(second["reason"], "unchanged")
            self.assertEqual(send.call_count, 1)
            sent_text = send.call_args_list[0].kwargs["text"]
            self.assertIn("Changes completed:", sent_text)
            self.assertIn("Your to-do list:", sent_text)
            self.assertIn("What Agent is working on next:", sent_text)
            self.assertIn("Nothing requires your attention today.", sent_text)

    def test_daily_email_ignores_volatile_report_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = SimpleNamespace(
                website_ops_root=Path(tmpdir),
                website_ops_report_email_to=("david@anatainc.com",),
                website_ops_email_from="Anata Agent <noreply@anatainc.com>",
                resend_api_key="test-key",
                resend_from="Anata Agent <noreply@anatainc.com>",
            )
            first_report = self._fake_report()
            first_report["generated_at"] = "2026-07-26T14:00:00Z"
            second_report = dict(first_report)
            second_report["generated_at"] = "2026-07-27T14:00:00Z"
            with mock.patch(
                "sales_support_agent.services.website_ops.ResendClient.send_message",
                return_value="email-1",
            ) as send:
                send_website_ops_report_email(settings, mode="daily", report=first_report)
                second = send_website_ops_report_email(settings, mode="daily", report=second_report)
            self.assertFalse(second["sent"])
            self.assertFalse(second["changed"])
            self.assertEqual(second["reason"], "unchanged")
            self.assertEqual(send.call_count, 1)

    def test_run_due_respects_daily_weekly_and_monthly_periods(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = self._settings(Path(tmpdir))
            write_website_ops_run_state(
                settings,
                "daily",
                {"status": "succeeded", "last_successful_date": "2026-07-26"},
            )
            write_website_ops_run_state(
                settings,
                "weekly",
                {"status": "succeeded", "last_successful_date": "2026-07-20"},
            )
            write_website_ops_run_state(
                settings,
                "monthly",
                {"status": "succeeded", "last_successful_date": "2026-07-01"},
            )
            self.assertFalse(website_ops_run_is_due(settings, "daily", today=date(2026, 7, 26)))
            self.assertTrue(website_ops_run_is_due(settings, "daily", today=date(2026, 7, 27)))
            self.assertFalse(website_ops_run_is_due(settings, "weekly", today=date(2026, 7, 26)))
            self.assertTrue(website_ops_run_is_due(settings, "weekly", today=date(2026, 7, 27)))
            self.assertFalse(website_ops_run_is_due(settings, "monthly", today=date(2026, 7, 31)))
            self.assertTrue(website_ops_run_is_due(settings, "monthly", today=date(2026, 8, 1)))

    def test_scheduled_job_requires_internal_key_and_runs_requested_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            app = FastAPI()
            app.state.settings = SimpleNamespace(
                internal_api_key="test-internal-key",
                website_ops_root=Path(tmpdir),
            )
            app.include_router(website_ops_jobs_router)
            client = TestClient(app)
            unauthorized = client.post("/api/jobs/website-ops/run", json={"mode": "daily"})
            self.assertEqual(unauthorized.status_code, 401)
            with mock.patch(
                "sales_support_agent.api.website_ops_jobs_router.run_website_ops",
                return_value=SimpleNamespace(
                    ok=True,
                    message="Daily website ops run completed.",
                    report={
                        "run_outcome": {
                            "status": "production_verified",
                            "production_delta_count": 1,
                        }
                    },
                ),
            ) as run:
                response = client.post(
                    "/api/jobs/website-ops/run",
                    headers={"X-Internal-Api-Key": "test-internal-key"},
                    json={"mode": "daily", "force": True},
                )
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()["details"]["daily"]["status"], "succeeded")
            run.assert_called_once_with(app.state.settings, mode="daily")

    def test_forced_daily_run_recovers_a_stale_manual_lease(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = self._settings(Path(tmpdir))
            settings.internal_api_key = "test-internal-key"
            local_now = datetime.now(ZoneInfo("America/Denver"))
            write_website_ops_run_state(
                settings,
                "daily",
                {
                    "status": "running",
                    "run_date": local_now.date().isoformat(),
                    "last_started_at": (
                        datetime.now(timezone.utc) - timedelta(hours=2)
                    ).isoformat(),
                },
            )
            app = FastAPI()
            app.state.settings = settings
            app.include_router(website_ops_jobs_router)
            client = TestClient(app)
            lease = SimpleNamespace(
                job_key="website_ops",
                run_key="recovery",
                owner_token="owner",
            )
            with mock.patch(
                "sales_support_agent.models.database.get_engine",
                return_value=object(),
            ), mock.patch(
                "sales_support_agent.api.website_ops_jobs_router.claim_scheduled_job",
                side_effect=[None, lease],
            ) as claim, mock.patch(
                "sales_support_agent.api.website_ops_jobs_router.finish_scheduled_job",
            ), mock.patch(
                "sales_support_agent.api.website_ops_jobs_router.run_website_ops",
                return_value=SimpleNamespace(
                    ok=True,
                    message="Recovered.",
                    report={
                        "run_outcome": {
                            "status": "production_verified",
                            "production_delta_count": 1,
                        }
                    },
                ),
            ):
                response = client.post(
                    "/api/jobs/website-ops/run",
                    headers={"X-Internal-Api-Key": "test-internal-key"},
                    json={"mode": "daily", "force": True},
                )

            self.assertEqual(response.status_code, 200)
            self.assertEqual(claim.call_count, 2)
            self.assertIn(
                ":force-recovery:",
                claim.call_args_list[1].kwargs["run_key"],
            )

    def test_scheduled_modes_add_weekly_and_monthly_on_first_monday(self) -> None:
        first_monday = datetime(2026, 8, 3, 8, 0, tzinfo=timezone.utc)
        later_monday = datetime(2026, 8, 10, 8, 0, tzinfo=timezone.utc)
        tuesday = datetime(2026, 8, 4, 8, 0, tzinfo=timezone.utc)
        self.assertEqual(_scheduled_modes(first_monday), ["daily", "weekly", "monthly"])
        self.assertEqual(_scheduled_modes(later_monday), ["daily", "weekly"])
        self.assertEqual(_scheduled_modes(tuesday), ["daily"])

    def test_website_ops_runtime_health_reports_readiness_and_run_freshness(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = SimpleNamespace(
                internal_api_key="test-internal-key",
                resend_api_key="test-resend-key",
                openai_api_key="test-openai-key",
                website_ops_execute_approved=True,
                website_ops_root=Path(tmpdir),
            )
            write_website_ops_run_state(
                settings,
                "daily",
                {
                    "status": "succeeded",
                    "trigger": "render_cron",
                    "run_date": datetime.now(ZoneInfo("America/Denver")).date().isoformat(),
                    "last_successful_date": datetime.now(ZoneInfo("America/Denver")).date().isoformat(),
                    "outcome_status": "production_verified",
                    "production_delta_count": "1",
                },
            )
            report_dir = Path(tmpdir) / "reports" / "daily"
            report_dir.mkdir(parents=True, exist_ok=True)
            report_path = report_dir / "2026-07-27-anata-website-ops-daily-report"
            report_path.with_suffix(".md").write_text(
                "# Anata Website Ops Daily Report\n\nDate: 2026-07-27\n",
                encoding="utf-8",
            )
            report_path.with_suffix(".json").write_text(
                json.dumps(
                    {
                        "generated_at": datetime.now(timezone.utc).isoformat(),
                        "analytics_status": {
                            "search_console": True,
                            "ga4": True,
                            "notes": [],
                        },
                        "support_requests": [],
                    }
                ),
                encoding="utf-8",
            )
            app = FastAPI()
            app.state.settings = settings
            app.state.website_ops_scheduler_thread = SimpleNamespace(is_alive=lambda: True)
            app.include_router(website_ops_jobs_router)
            with mock.patch.dict(
                os.environ,
                {
                    "WEBSITE_OPS_REPORT_EMAIL_TO": "david@anatainc.com",
                    "WEBSITE_OPS_ALLOWED_HOST": "anatainc.com",
                    "WEBSITE_OPS_GITHUB_TOKEN": "test-github-key",
                    "WEBSITE_OPS_GITHUB_REPOSITORY": "david-anata/anata-website",
                    "GOOGLE_SERVICE_ACCOUNT_JSON": json.dumps(
                        {
                            "client_email": "website-ops@example.iam.gserviceaccount.com",
                            "private_key": "test-private-key",
                            "token_uri": "https://oauth2.googleapis.com/token",
                        }
                    ),
                    "WEBSITE_OPS_GSC_PROPERTY": "sc-domain:anatainc.com",
                    "WEBSITE_OPS_GA4_PROPERTY_ID": "372887830",
                },
                clear=False,
            ):
                response = TestClient(app).get("/api/jobs/website-ops/health")
            self.assertEqual(response.status_code, 200)
            payload = response.json()
            self.assertEqual(payload["status"], "ready")
            self.assertTrue(all(payload["checks"].values()))
            self.assertEqual(payload["schedule"]["hour"], 8)
            self.assertEqual(payload["schedule"]["source"], "embedded_scheduler")
            self.assertEqual(payload["runs"]["daily"]["status"], "succeeded")
            self.assertNotIn("last_error", payload["runs"]["daily"])
            self.assertEqual(payload["states"]["decision_data"], "ready")
            self.assertEqual(payload["user_todo"], [])

    def test_daily_run_freshness_blocks_stale_state_after_first_pulse(self) -> None:
        stale = {
            "runs": {
                "daily": {
                    "run_date": "2026-07-31",
                    "status": "succeeded",
                }
            }
        }
        self.assertFalse(
            _daily_run_is_fresh(
                stale,
                datetime(2026, 8, 1, 11, 0, tzinfo=timezone.utc),
            )
        )
        self.assertTrue(
            _daily_run_is_fresh(
                stale,
                datetime(2026, 8, 1, 7, 59, tzinfo=timezone.utc),
            )
        )

    def test_daily_run_freshness_blocks_stalled_running_state(self) -> None:
        running = {
            "runs": {
                "daily": {
                    "run_date": "2026-08-01",
                    "status": "running",
                    "last_started_at": "2026-08-01T16:00:00+00:00",
                }
            }
        }

        self.assertTrue(
            _daily_run_is_fresh(
                running,
                datetime(2026, 8, 1, 16, 44, tzinfo=timezone.utc),
            )
        )
        self.assertFalse(
            _daily_run_is_fresh(
                running,
                datetime(2026, 8, 1, 16, 46, tzinfo=timezone.utc),
            )
        )

    def test_daily_run_freshness_rejects_running_state_without_timestamp(self) -> None:
        running = {
            "runs": {
                "daily": {
                    "run_date": "2026-08-01",
                    "status": "running",
                }
            }
        }

        self.assertFalse(
            _daily_run_is_fresh(
                running,
                datetime(2026, 8, 1, 16, 1, tzinfo=timezone.utc),
            )
        )

    def test_daily_outcome_requires_a_verified_production_delta(self) -> None:
        local_now = datetime(2026, 8, 1, 18, 20, tzinfo=timezone.utc)
        no_delta = {
            "runs": {
                "daily": {
                    "run_date": "2026-08-01",
                    "status": "succeeded",
                    "outcome_status": "work_in_progress",
                    "production_delta_count": "0",
                }
            }
        }
        verified = {
            "runs": {
                "daily": {
                    "run_date": "2026-08-01",
                    "status": "succeeded",
                    "outcome_status": "production_verified",
                    "production_delta_count": "1",
                }
            }
        }

        self.assertFalse(_daily_run_has_verified_outcome(no_delta, local_now))
        self.assertTrue(_daily_run_has_verified_outcome(verified, local_now))

    def test_embedded_scheduler_catches_up_current_hour_once(self) -> None:
        settings = SimpleNamespace(website_ops_root=Path("runtime/test-website-ops"))
        local_now = datetime(2026, 8, 1, 11, 37, tzinfo=timezone.utc)
        with (
            mock.patch(
                "sales_support_agent.api.website_ops_jobs_router.get_website_ops_run_state",
                return_value={"last_pulse_slot": "2026-08-01:10"},
            ),
            mock.patch(
                "sales_support_agent.models.database.get_engine",
                side_effect=RuntimeError("isolated test"),
            ),
            mock.patch(
                "sales_support_agent.api.website_ops_jobs_router._run_due_modes",
                return_value={"daily": {"status": "succeeded"}},
            ) as run,
        ):
            result = _run_embedded_pulse(settings, local_now)
        self.assertEqual(result["status"], "succeeded")
        run.assert_called_once_with(
            settings,
            ["daily"],
            trigger="embedded_scheduler",
            pulse_slot="2026-08-01:11",
        )

    def test_embedded_scheduler_does_not_repeat_completed_slot(self) -> None:
        settings = SimpleNamespace(website_ops_root=Path("runtime/test-website-ops"))
        local_now = datetime(2026, 8, 1, 11, 52, tzinfo=timezone.utc)
        with (
            mock.patch(
                "sales_support_agent.api.website_ops_jobs_router.get_website_ops_run_state",
                return_value={"last_pulse_slot": "2026-08-01:11"},
            ),
            mock.patch(
                "sales_support_agent.api.website_ops_jobs_router._run_due_modes",
            ) as run,
        ):
            result = _run_embedded_pulse(settings, local_now)
        self.assertEqual(result["status"], "skipped")
        run.assert_not_called()

    def test_embedded_scheduler_retries_claimed_slot_when_durable_state_is_stale(self) -> None:
        settings = SimpleNamespace(website_ops_root=Path("runtime/test-website-ops"))
        local_now = datetime(2026, 8, 1, 12, 12, tzinfo=timezone.utc)
        lease = object()
        with (
            mock.patch(
                "sales_support_agent.models.database.get_engine",
                return_value=object(),
            ),
            mock.patch(
                "sales_support_agent.api.website_ops_jobs_router.database_mirror_enabled",
                return_value=False,
            ),
            mock.patch(
                "sales_support_agent.api.website_ops_jobs_router.get_website_ops_run_state",
                return_value={"run_date": "2026-07-31"},
            ),
            mock.patch(
                "sales_support_agent.api.website_ops_jobs_router.claim_scheduled_job",
                side_effect=[None, lease],
            ) as claim,
            mock.patch(
                "sales_support_agent.api.website_ops_jobs_router.finish_scheduled_job",
            ),
            mock.patch(
                "sales_support_agent.api.website_ops_jobs_router._run_due_modes",
                return_value={"daily": {"status": "succeeded"}},
            ) as run,
        ):
            result = _run_embedded_pulse(settings, local_now)

        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(claim.call_count, 2)
        self.assertIn(
            "stale-or-failed-recovery:",
            claim.call_args_list[1].kwargs["run_key"],
        )
        run.assert_called_once()

    def test_embedded_scheduler_recovers_stalled_same_slot_after_restart(self) -> None:
        settings = SimpleNamespace(website_ops_root=Path("runtime/test-website-ops"))
        local_now = datetime(2026, 8, 1, 11, 5, tzinfo=timezone.utc)
        lease = object()
        stalled = {
            "run_date": "2026-08-01",
            "status": "running",
            "last_pulse_slot": "2026-08-01:11",
            "last_started_at": "2026-08-01T10:00:00+00:00",
        }
        with (
            mock.patch(
                "sales_support_agent.models.database.get_engine",
                return_value=object(),
            ),
            mock.patch(
                "sales_support_agent.api.website_ops_jobs_router.database_mirror_enabled",
                return_value=False,
            ),
            mock.patch(
                "sales_support_agent.api.website_ops_jobs_router.get_website_ops_run_state",
                return_value=stalled,
            ),
            mock.patch(
                "sales_support_agent.api.website_ops_jobs_router.claim_scheduled_job",
                side_effect=[None, lease],
            ) as claim,
            mock.patch(
                "sales_support_agent.api.website_ops_jobs_router.finish_scheduled_job",
            ),
            mock.patch(
                "sales_support_agent.api.website_ops_jobs_router._run_due_modes",
                return_value={"daily": {"status": "succeeded"}},
            ) as run,
        ):
            result = _run_embedded_pulse(settings, local_now)

        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(claim.call_count, 2)
        self.assertIn(
            "stale-or-failed-recovery:",
            claim.call_args_list[1].kwargs["run_key"],
        )
        run.assert_called_once()

    def test_embedded_scheduler_restores_then_persists_database_mirror(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            settings = SimpleNamespace(website_ops_root=root)
            local_now = datetime(2026, 8, 1, 12, 7, tzinfo=timezone.utc)
            engine = object()
            transaction = mock.MagicMock()

            def run_due_modes(*args, **kwargs):
                (root / "state.json").write_text(
                    '{"last_pulse_slot":"2026-08-01:12"}',
                    encoding="utf-8",
                )
                return {"daily": {"status": "succeeded"}}

            with (
                mock.patch(
                    "sales_support_agent.models.database.get_engine",
                    return_value=engine,
                ),
                mock.patch(
                    "sales_support_agent.api.website_ops_jobs_router.database_mirror_enabled",
                    return_value=True,
                ),
                mock.patch(
                    "sales_support_agent.api.website_ops_jobs_router.website_ops_cache_transaction",
                    return_value=transaction,
                ) as cache_transaction,
                mock.patch(
                    "sales_support_agent.api.website_ops_jobs_router.get_website_ops_run_state",
                    return_value={"last_pulse_slot": "2026-08-01:11"},
                ),
                mock.patch(
                    "sales_support_agent.api.website_ops_jobs_router.claim_scheduled_job",
                    return_value=object(),
                ),
                mock.patch(
                    "sales_support_agent.api.website_ops_jobs_router.finish_scheduled_job",
                ),
                mock.patch(
                    "sales_support_agent.api.website_ops_jobs_router._run_due_modes",
                    side_effect=run_due_modes,
                ),
            ):
                result = _run_embedded_pulse(settings, local_now)

            self.assertEqual(result["status"], "succeeded")
            cache_transaction.assert_called_once_with(engine, root)
            transaction.__enter__.assert_called_once()
            transaction.__exit__.assert_called_once()
            self.assertTrue((root / "state.json").exists())

    def test_operating_state_uses_latest_live_evidence_not_secret_presence(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = self._settings(Path(tmpdir))
            report_dir = Path(tmpdir) / "reports" / "daily"
            report_dir.mkdir(parents=True, exist_ok=True)
            report_path = report_dir / "2026-07-27-anata-website-ops-daily-report"
            report_path.with_suffix(".md").write_text(
                "# Anata Website Ops Daily Report\n\nDate: 2026-07-27\n",
                encoding="utf-8",
            )
            report_path.with_suffix(".json").write_text(
                json.dumps(
                    {
                        "generated_at": datetime.now(timezone.utc).isoformat(),
                        "analytics_status": {
                            "search_console": False,
                            "ga4": False,
                            "notes": ["Search Console authentication failed."],
                        },
                        "support_requests": ["Repair the Google service account JSON."],
                    }
                ),
                encoding="utf-8",
            )

            state = website_ops_operating_state(settings)

            self.assertEqual(state["status"], "blocked")
            self.assertEqual(state["decision_data"], "blocked")
            self.assertIn("Repair the Google service account JSON.", state["support_requests"])

    def test_website_ops_runtime_health_blocks_invalid_decision_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = SimpleNamespace(
                internal_api_key="test-internal-key",
                resend_api_key="test-resend-key",
                openai_api_key="test-openai-key",
                website_ops_execute_approved=True,
                website_ops_root=Path(tmpdir),
                google_service_account_json="{not valid json",
                website_ops_gsc_property="sc-domain:anatainc.com",
                website_ops_ga4_property_id="372887830",
            )
            app = FastAPI()
            app.state.settings = settings
            app.include_router(website_ops_jobs_router)
            with mock.patch.dict(
                os.environ,
                {
                    "WEBSITE_OPS_REPORT_EMAIL_TO": "david@anatainc.com",
                    "WEBSITE_OPS_ALLOWED_HOST": "anatainc.com",
                    "WEBSITE_OPS_GITHUB_TOKEN": "test-github-key",
                    "WEBSITE_OPS_GITHUB_REPOSITORY": "david-anata/anata-website",
                },
                clear=False,
            ):
                payload = TestClient(app).get("/api/jobs/website-ops/health").json()
            self.assertEqual(payload["status"], "blocked")
            self.assertEqual(payload["states"]["decision_data"], "blocked")
            self.assertFalse(payload["checks"]["search_console_configuration"])
            self.assertTrue(payload["blockers"])

    def test_queue_empty_state_points_to_resolution_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = self._settings(Path(tmpdir))
            html = render_queue_page(settings)
            self.assertIn("No Website Ops records need review.", html)
            self.assertIn("Run Daily Sweep", html)
            self.assertIn("editorial production continues", html)
            self.assertIn("Repair Google connections", html)
            self.assertIn("/admin/website-ops#submit-issue", html)
            self.assertIn("/admin/api/website-ops/actions/execute-approved", html)
            self.assertIn('disabled aria-disabled="true"', html)

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
            self.assertIn("Autonomous publishing guardrails active", html)
            self.assertIn("Needs setup", html)
            self.assertIn("sdr-support-agent", html)
            self.assertIn("codex-website-ops@sdr-support-agent.iam.gserviceaccount.com", html)

    def test_dashboard_fails_closed_for_legacy_unavailable_analytics(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = self._settings(Path(tmpdir))
            reports_dir = settings.website_ops_root / "reports" / "daily"
            reports_dir.mkdir(parents=True, exist_ok=True)
            (reports_dir / "2026-07-27-report.md").write_text("# Report\n")
            (reports_dir / "2026-07-27-report.json").write_text(
                json.dumps(
                    {
                        "title": "Legacy report",
                        "status": "healthy",
                        "pages_reviewed": 1,
                        "page_insights": [
                            {
                                "page_url": "https://anatainc.com/",
                                "page_title": "Anata",
                                "score": 92,
                                "bucket": "build",
                                "search_console": {},
                                "ga4": {},
                                "task_block_reason": "Impressions below 25.",
                            }
                        ],
                        "customer_questions": [
                            {
                                "question": "private unrelated question?",
                                "source": "gmail",
                            }
                        ],
                        "analytics_status": {
                            "search_console": False,
                            "ga4": False,
                            "notes": ["Search Console unavailable", "GA4 unavailable"],
                        },
                    }
                )
            )
            html = render_dashboard_page(settings)
            self.assertIn("Score Unavailable", html)
            self.assertIn("Ranking operations", html)
            self.assertIn("blocked", html)
            self.assertIn("Gmail-derived questions are quarantined", html)
            self.assertNotIn("private unrelated question", html)
            self.assertNotIn("<a href=\"/admin/website-ops/reports/latest\"", html)

    def test_legacy_report_suppresses_false_performance_values(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = self._settings(Path(tmpdir))
            reports_dir = settings.website_ops_root / "reports" / "weekly"
            reports_dir.mkdir(parents=True, exist_ok=True)
            slug = "2026-07-27-legacy-report"
            (reports_dir / f"{slug}.md").write_text("# Legacy report\n")
            (reports_dir / f"{slug}.html").write_text(
                "<html><body><p>Score: 92 · GSC: 0 impressions</p></body></html>"
            )
            (reports_dir / f"{slug}.json").write_text(
                json.dumps(
                    {
                        "title": "Legacy report",
                        "status": "healthy",
                        "pages_reviewed": 1,
                        "page_insights": [
                            {
                                "page_url": "https://anatainc.com/",
                                "page_title": "Anata",
                                "score": 92,
                                "bucket": "build",
                                "search_console": {"impressions": 0},
                                "ga4": {"sessions": 0},
                            }
                        ],
                        "analytics_status": {
                            "search_console": False,
                            "ga4": False,
                            "notes": ["Search Console unavailable", "GA4 unavailable"],
                        },
                    }
                )
            )
            html = render_report_page(settings, "weekly", slug)
            self.assertIn("Archived · decision data unavailable", html)
            self.assertIn("Score Unavailable", html)
            self.assertIn("Ranking operations", html)
            self.assertNotIn("Score: 92", html)
            self.assertNotIn("GSC: 0 impressions", html)

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
            self.assertIn("Save decision", html)
            self.assertIn("inject_faq_block", html)
            self.assertIn("Approve recommendation", html)
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
                    "execution_eligibility": "suggestion_only",
                    "before_state": "Contact Us | Faster, Smarter, Intelligent, Data.",
                    "after_state": "Keep one topic-specific H1 and demote the rest to H2.",
                },
            )
            result = review_feedback_record(settings, record["feedback_id"], {"status": "approved"})
            self.assertTrue(result.ok)
            updated = load_feedback_records(settings)[0]
            self.assertEqual(updated["status"], "approved")
            self.assertEqual(updated["action_type"], "")

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
                    "execution_eligibility": "suggestion_only",
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
            self.assertEqual(updated["status"], "approved")
            self.assertEqual(updated.get("execution_error", ""), "")

    def test_execute_approved_website_ops_actions_runs_approved_records(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = self._settings(Path(tmpdir), execute_approved=True)
            record = save_feedback_record(
                settings,
                {
                    "summary": "Add fulfillment FAQ",
                    "status": "approved",
                    "action_type": "inject_faq_block",
                    "action_value": json.dumps({"heading": "Fulfillment FAQ", "questions": []}),
                    "page_url": "https://anatainc.com/services/fulfillment/",
                },
            )
            with mock.patch.object(
                website_ops,
                "execute_feedback_action",
                return_value={
                    "feedback_id": record["feedback_id"],
                    "action_type": "inject_faq_block",
                    "executed_at": "2026-03-27T00:00:00Z",
                    "verification_status": "verified",
                },
            ):
                result = execute_approved_website_ops_actions(settings)
            self.assertTrue(result.ok)
            self.assertEqual(result.report["executed"], 1)
            updated = next(item for item in load_feedback_records(settings) if item["feedback_id"] == record["feedback_id"])
            self.assertEqual(updated["status"], "done")
            self.assertEqual(updated["execution_result"]["verification_status"], "verified")

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

    def test_run_website_ops_keeps_rejected_auto_generated_item_suppressed(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = self._settings(Path(tmpdir))
            original = save_feedback_record(
                settings,
                {
                    "summary": "Reject stale FAQ",
                    "status": "rejected",
                    "automation_key": "auto-rejected",
                    "auto_generated": True,
                    "suggested_action_type": "inject_faq_block",
                    "source_report_date": "2026-03-26",
                },
            )
            fake_pipeline = {"report": self._fake_report(), "observations": [], "artifacts": {}}
            fake_overlay = {
                "goal": {"primary": "Increase qualified leads."},
                "action_queue": [{
                    "page_url": "https://anatainc.com/",
                    "page_title": "Anata Inc.",
                    "action_type": "inject_faq_block",
                    "section_name": "FAQ block",
                    "after_state": "Insert an FAQ block.",
                    "reason": "Old recommendation.",
                    "insight_source": "SERP + Customer Language",
                }],
                "analytics_status": {"search_console": True, "ga4": True, "notes": []},
                "support_requests": [],
                "page_insights": [],
            }
            with mock.patch("sales_support_agent.services.website_ops._automation_key", return_value="auto-rejected"):
                with mock.patch("sales_support_agent.services.website_ops._utc_now", return_value=datetime(2026, 3, 27, 12, 0, tzinfo=timezone.utc)):
                    with mock.patch("sales_support_agent.services.website_ops.website_ops.run_daily_report_pipeline", return_value=fake_pipeline):
                        with mock.patch("sales_support_agent.services.website_ops.build_autonomy_overlay", return_value=fake_overlay):
                            result = run_website_ops(settings, mode="daily")
            self.assertTrue(result.ok)
            records = load_feedback_records(settings)
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0]["feedback_id"], original["feedback_id"])
            self.assertEqual(records[0]["status"], "rejected")

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
            with mock.patch.dict(
                os.environ,
                {"WEBSITE_OPS_CUSTOMER_LANGUAGE_ENABLED": "true"},
                clear=False,
            ), mock.patch(
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
            self.assertEqual(faq_action["execution_eligibility"], "suggestion_only")
            self.assertEqual(section_action["execution_eligibility"], "suggestion_only")
            self.assertTrue(faq_action["evidence"])
            self.assertTrue(faq_action["verification_requirements"])

    def test_build_autonomy_overlay_does_not_score_unavailable_analytics(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = self._settings(Path(tmpdir))
            with mock.patch(
                "sales_support_agent.services.website_ops_autonomy.fetch_search_console_snapshot",
                return_value=({}, ["Search Console unavailable: invalid credentials."]),
            ), mock.patch(
                "sales_support_agent.services.website_ops_autonomy.fetch_ga4_snapshot",
                return_value=({}, ["GA4 unavailable: invalid credentials."]),
            ):
                overlay = build_autonomy_overlay(
                    settings=settings,
                    report=self._fake_report(),
                    observations=[
                        {
                            "url": "https://anatainc.com/services/fulfillment/",
                            "title": "Fulfillment",
                            "issues": [],
                        }
                    ],
                    feedback_entries=[],
                )
            insight = overlay["page_insights"][0]
            self.assertIsNone(insight["score"])
            self.assertEqual(insight["bucket"], "data unavailable")
            self.assertEqual(insight["metric_availability"]["search_console"], "unavailable")
            self.assertEqual(overlay["action_queue"], [])
            self.assertEqual(overlay["analytics_status"]["decision_data_status"], "blocked")
            self.assertEqual(overlay["analytics_status"]["customer_language_status"], "quarantined")

    def test_build_autonomy_overlay_degrades_invalid_google_credentials_to_setup_notes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = self._settings(Path(tmpdir))
            settings.google_service_account_json = "{not valid json"
            settings.website_ops_gsc_property = "sc-domain:anatainc.com"
            settings.website_ops_ga4_property_id = "372887830"
            settings.website_ops_lookback_days = 28
            settings.primary_lead_event = "generate_lead"
            settings.gmail_poll_max_messages = 0
            with mock.patch("sales_support_agent.services.website_ops_autonomy.collect_customer_questions", return_value=[]):
                overlay = build_autonomy_overlay(
                    settings=settings,
                    report=self._fake_report(),
                    observations=[{"url": "https://anatainc.com/services/fulfillment/", "title": "Fulfillment", "issues": []}],
                    feedback_entries=[],
                )
            self.assertFalse(overlay["analytics_status"]["search_console"])
            self.assertFalse(overlay["analytics_status"]["ga4"])
            self.assertTrue(any("Search Console unavailable" in note for note in overlay["analytics_status"]["notes"]))
            self.assertTrue(any("GA4 unavailable" in note for note in overlay["analytics_status"]["notes"]))

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

    def test_execution_target_details_uses_plugin_execution_path(self) -> None:
        feedback = {
            "action_type": "inject_faq_block",
            "page_url": "https://anatainc.com/services/fulfillment/",
        }
        with mock.patch.dict(os.environ, {"ANATA_OPS_SHARED_SECRET": "test-secret"}, clear=False):
            with mock.patch(
                "sales_support_agent.services.website_ops_vendor.executor._fetch_live_html",
                return_value="<h1>Fulfillment</h1><p>Intro copy.</p><div>Book a call</div>",
            ):
                details = execution_target_details(feedback)
        self.assertFalse(details["eligible"])
        self.assertEqual(details["execution_eligibility"], "suggestion_only")
        self.assertIn("suggestion only", details["reason"].lower())

    def test_execute_feedback_action_calls_plugin_endpoint(self) -> None:
        feedback = {
            "feedback_id": "fb_123",
            "action_type": "inject_faq_block",
            "page_url": "https://anatainc.com/services/fulfillment/",
            "action_value": json.dumps(
                {
                    "heading": "Fulfillment FAQ",
                    "questions": [
                        {
                            "question": "How fast can onboarding happen?",
                            "answer": "Anata answers directly: onboarding starts quickly after implementation planning.",
                        }
                    ],
                }
            ),
        }
        with self.assertRaises(ExecutionError):
            execute_feedback_action(feedback)

    def test_execution_target_details_meta_update_is_auto_executable(self) -> None:
        feedback = {
            "action_type": "meta_update",
            "page_url": "https://anatainc.com/services/fulfillment/",
            "action_value": json.dumps(
                {
                    "meta_title": "eCommerce Fulfillment Services | Anata",
                    "meta_description": "Direct-answer fulfillment services for growing brands.",
                }
            ),
        }
        with mock.patch.dict(
            os.environ,
            {
                "ANATA_OPS_SHARED_SECRET": "test-secret",
                "ANATA_OPS_BASE_URL": "https://anatainc.com",
            },
            clear=False,
        ):
            details = execution_target_details(feedback)
        self.assertTrue(details["eligible"])
        self.assertEqual(details["execution_eligibility"], "auto_execute")
        self.assertIn("seo metadata", details["reason"].lower())

    def test_github_metadata_route_mapping_is_marketing_only(self) -> None:
        self.assertEqual(
            route_source_path("https://anatainc.com/services/amazon-advertising/"),
            "src/app/services/amazon-advertising/page.tsx",
        )
        self.assertEqual(route_source_path("https://anatainc.com/"), "src/app/page.tsx")
        with self.assertRaises(ExecutionError):
            route_source_path("https://app.anatainc.com/services/amazon-advertising/")
        with self.assertRaises(ExecutionError):
            route_source_path("https://anatainc.com/book")

    def test_generated_article_requires_citations_and_independent_evidence(self) -> None:
        record = self._generated_article_record()
        article = validate_generated_article(record)
        self.assertEqual(article["slug"], "amazon-ppc-account-structure")
        with self.assertRaises(ExecutionError):
            validate_generated_article({**record, "evidence": ["one signal"]})
        invalid = json.loads(str(record["action_value"]))
        invalid["sources"] = invalid["sources"][:1]
        with self.assertRaises(ExecutionError):
            validate_generated_article({**record, "action_value": json.dumps(invalid)})

    def test_generated_article_rejects_slop_thin_copy_and_weak_links(self) -> None:
        record = self._generated_article_record()
        slop = json.loads(str(record["action_value"]))
        slop["content"]["sections"][0]["paragraphs"][0] += (
            " This is a game-changer for every brand."
        )
        with self.assertRaises(ExecutionError):
            validate_generated_article(
                {**record, "action_value": json.dumps(slop)}
            )

        thin = json.loads(str(record["action_value"]))
        thin["content"]["sections"] = thin["content"]["sections"][:2]
        with self.assertRaises(ExecutionError):
            validate_generated_article(
                {**record, "action_value": json.dumps(thin)}
            )

        weak_links = json.loads(str(record["action_value"]))
        weak_links["content"]["related"] = weak_links["content"]["related"][:1]
        with self.assertRaises(ExecutionError):
            validate_generated_article(
                {**record, "action_value": json.dumps(weak_links)}
            )

    def test_generated_article_requires_truthful_publication_identity(self) -> None:
        record = self._generated_article_record()
        invalid_author = json.loads(str(record["action_value"]))
        invalid_author["author"]["name"] = "SEO Bot"
        with self.assertRaises(ExecutionError):
            validate_generated_article(
                {**record, "action_value": json.dumps(invalid_author)}
            )

        invalid_dates = json.loads(str(record["action_value"]))
        invalid_dates["modifiedAt"] = "2026-07-26T14:00:00+00:00"
        with self.assertRaises(ExecutionError):
            validate_generated_article(
                {**record, "action_value": json.dumps(invalid_dates)}
            )

    def test_generated_article_rejects_competitor_and_low_authority_sources(self) -> None:
        record = self._generated_article_record()
        article = json.loads(str(record["action_value"]))
        article["sources"][1] = {
            "title": "Competing agency guide",
            "url": "https://competitor.example/blog/amazon-ppc",
        }
        with self.assertRaisesRegex(
            ExecutionError,
            "first-party platform, carrier, or government documentation",
        ):
            validate_generated_article(
                {**record, "action_value": json.dumps(article)}
            )

    def test_generated_article_requires_official_source_for_each_named_platform(self) -> None:
        record = self._generated_article_record()
        article = json.loads(str(record["action_value"]))
        article["content"]["sections"][0]["paragraphs"][0] += (
            " Walmart marketplace fees require separate planning."
        )
        with self.assertRaisesRegex(
            ExecutionError,
            "Walmart without an official walmart.com source",
        ):
            validate_generated_article(
                {**record, "action_value": json.dumps(article)}
            )

    def test_generated_article_rejects_malformed_semicolon_joins(self) -> None:
        record = self._generated_article_record()
        article = json.loads(str(record["action_value"]))
        article["content"]["sections"][0]["paragraphs"][0] += " costs;platform"
        with self.assertRaisesRegex(ExecutionError, "malformed punctuation joins"):
            validate_generated_article(
                {**record, "action_value": json.dumps(article)}
            )

    def test_generated_article_registry_enforces_one_page_one_intent(self) -> None:
        source = """import type { ArticlePageContent } from "@/components/pagekit/ArticlePage";
export type GeneratedArticle = { content: ArticlePageContent };
// WEBSITE_OPS_GENERATED_ARTICLES_START
export const GENERATED_ARTICLES: readonly GeneratedArticle[] = [];
// WEBSITE_OPS_GENERATED_ARTICLES_END
"""
        article = validate_generated_article(self._generated_article_record())
        updated = update_generated_article_registry(source, article)
        self.assertIn('"amazon-ppc-account-structure"', updated)
        with self.assertRaises(ExecutionError):
            update_generated_article_registry(updated, article)
        duplicate_intent = dict(article)
        duplicate_intent["slug"] = "another-ppc-structure-guide"
        with self.assertRaises(ExecutionError):
            update_generated_article_registry(updated, duplicate_intent)

    def test_article_engine_only_builds_from_repeated_validated_gap(self) -> None:
        settings = SimpleNamespace(openai_api_key="test-key")
        intelligence = {
            "summary": {"weekly_validation_cycles": 2},
            "clusters": [
                {
                    "cluster_id": "cluster-123",
                    "label": "how to structure an amazon ppc account",
                    "normalized_query": "how to structure an amazon ppc account",
                    "validation_status": "validated",
                    "quality_status": "eligible",
                    "ownership_status": "assigned",
                    "intent": "informational",
                    "owner_url": "https://anatainc.com/services/amazon-advertising",
                    "alignment": {"composite": 0.2},
                    "evidence_classes": ["observed_search", "observed_answer_engine"],
                    "citation": {
                        "cited_urls": [
                            {"title": "Amazon Ads", "url": "https://advertising.amazon.com/library/guides"},
                            {"title": "Google Search", "url": "https://developers.google.com/search/docs"},
                        ]
                    },
                }
            ],
        }
        generated = json.loads(str(self._generated_article_record()["action_value"]))

        def requester(**_: object) -> dict[str, object]:
            return generated

        action = build_article_action(
            settings=settings,
            query_intelligence=intelligence,
            requester=requester,
        )
        self.assertIsNotNone(action)
        assert action is not None
        self.assertEqual(action["action_type"], "publish_blog_article")
        self.assertEqual(action["execution_eligibility"], "auto_execute")
        validate_generated_article(action)
        intelligence["summary"] = {"weekly_validation_cycles": 1}
        self.assertIsNotNone(
            build_article_action(
                settings=settings,
                query_intelligence=intelligence,
                requester=requester,
            )
        )
        intelligence["clusters"][0]["label"] = (
            '"amazon ppc" -site:reddit.com -site:youtube.com'
        )
        fallback_action = build_article_action(
            settings=settings,
            query_intelligence=intelligence,
            requester=requester,
        )
        self.assertIsNotNone(fallback_action)
        assert fallback_action is not None
        self.assertEqual(
            json.loads(str(fallback_action["action_value"]))["primaryIntent"],
            "how to structure amazon ppc campaigns",
        )
        self.assertEqual(
            fallback_action["insight_source"],
            "Approved editorial backlog and one-page-one-intent map",
        )

    def test_article_engine_regenerates_contract_failure_before_queueing(self) -> None:
        valid = json.loads(str(self._generated_article_record()["action_value"]))
        invalid = json.loads(json.dumps(valid))
        invalid["content"]["sections"][0]["paragraphs"][0] += (
            " Walmart marketplace fees require separate planning."
        )
        with tempfile.TemporaryDirectory() as tmpdir, mock.patch(
            "sales_support_agent.services.website_ops_github.github_metadata_is_configured",
            return_value=False,
        ), mock.patch(
            "sales_support_agent.services.website_ops_article_engine._request_article",
            side_effect=[invalid, valid],
        ) as request:
            action = build_article_action(
                settings=SimpleNamespace(website_ops_root=Path(tmpdir)),
                query_intelligence={"clusters": []},
            )

        self.assertIsNotNone(action)
        self.assertEqual(request.call_count, 2)
        self.assertIn(
            "failed the publication contract",
            request.call_args_list[1].kwargs["prompt"],
        )
        self.assertIn(
            "Walmart without an official walmart.com source",
            request.call_args_list[1].kwargs["prompt"],
        )

    def test_github_metadata_validation_requires_reason_evidence_and_safe_lengths(self) -> None:
        record = {
            "action_type": "meta_update",
            "confidence": "high",
            "reason": "The production title is duplicated across two distinct intents.",
            "evidence": ["Rendered title matches another canonical page."],
            "page_url": "https://anatainc.com/services/amazon-advertising/",
            "action_value": json.dumps(
                {
                    "meta_title": "Amazon Advertising Agency | Anata",
                    "meta_description": "Connect sponsored ads and DSP to one accountable Amazon advertising plan with operator decisions made visible.",
                    "canonical_url": "https://anatainc.com/services/amazon-advertising/",
                }
            ),
        }
        validated = validate_metadata_action(record)
        self.assertEqual(validated["meta_title"], "Amazon Advertising Agency | Anata")
        with self.assertRaises(ExecutionError):
            validate_metadata_action({**record, "evidence": []})
        with self.assertRaises(ExecutionError):
            validate_metadata_action(
                {
                    **record,
                    "action_value": json.dumps(
                        {
                            "meta_title": "Too short",
                            "meta_description": validated["meta_description"],
                        }
                    ),
                }
            )

    def test_static_metadata_source_update_preserves_page_code(self) -> None:
        source = '''import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "Old Amazon Advertising Title | Anata",
  description:
    "This is the existing long description that should be safely replaced by the executor.",
};

export default function Page() {
  return <main>Keep this page body.</main>;
}
'''
        updated = update_static_metadata_source(
            source,
            {
                "meta_title": "Amazon Advertising Agency | Anata",
                "meta_description": "Connect sponsored ads and DSP to one accountable Amazon advertising plan with operator decisions made visible.",
                "canonical_url": "https://anatainc.com/services/amazon-advertising/",
            },
        )
        self.assertIn('title: "Amazon Advertising Agency | Anata"', updated)
        self.assertIn("operator decisions made visible", updated)
        self.assertIn(
            'alternates: { canonical: "https://anatainc.com/services/amazon-advertising/" }',
            updated,
        )
        self.assertIn("<main>Keep this page body.</main>", updated)

    def test_github_metadata_execution_commits_and_verifies(self) -> None:
        record = {
            "feedback_id": "fb_meta_github",
            "action_type": "meta_update",
            "confidence": "high",
            "reason": "The production metadata does not match the approved intent map.",
            "evidence": ["Rendered metadata differs from the approved title and description."],
            "page_url": "https://anatainc.com/services/amazon-advertising/",
            "action_value": json.dumps(
                {
                    "meta_title": "Amazon Advertising Agency | Anata",
                    "meta_description": "Connect sponsored ads and DSP to one accountable Amazon advertising plan with operator decisions made visible.",
                }
            ),
        }
        source = '''export const metadata = {
  title: "Old Amazon Advertising Title | Anata",
  description: "This is the existing long description that should be safely replaced by the executor.",
};
'''
        client = mock.Mock()
        client.repository = "david-anata/anata-website"
        client.branch = "main"
        client.get_file.return_value = (source, "source-sha")
        client.put_file.return_value = {"commit": {"sha": "commit-sha"}}
        with mock.patch(
            "sales_support_agent.services.website_ops_github.GitHubWebsiteClient",
            return_value=client,
        ):
            with mock.patch(
                "sales_support_agent.services.website_ops_github._live_metadata_matches",
                return_value=True,
            ):
                result = execute_github_metadata_action(
                    record,
                    config=SimpleNamespace(),
                    timestamp=datetime(2026, 7, 27, tzinfo=timezone.utc),
                )
        self.assertEqual(result["verification_status"], "verified")
        self.assertEqual(result["commit_sha"], "commit-sha")
        self.assertEqual(result["source_path"], "src/app/services/amazon-advertising/page.tsx")
        client.put_file.assert_called_once()

    def test_article_timeout_preserves_durable_commit_for_reconciliation(self) -> None:
        record = self._generated_article_record()
        source = '''import type { ArticlePageContent } from "@/components/pagekit/ArticlePage";
export type GeneratedArticle = { content: ArticlePageContent };
// WEBSITE_OPS_GENERATED_ARTICLES_START
export const GENERATED_ARTICLES: readonly GeneratedArticle[] = [];
// WEBSITE_OPS_GENERATED_ARTICLES_END
'''
        client = mock.Mock()
        client.repository = "david-anata/anata-website"
        client.branch = "main"
        client.get_file.return_value = (source, "source-sha")
        client.put_file.return_value = {"commit": {"sha": "commit-sha"}}
        with mock.patch(
            "sales_support_agent.services.website_ops_github.GitHubWebsiteClient",
            return_value=client,
        ), mock.patch(
            "sales_support_agent.services.website_ops_github.website_ops.collect_page_observation",
            return_value={"status_code": 404},
        ), mock.patch(
            "sales_support_agent.services.website_ops_github.time.monotonic",
            side_effect=[0, 0, 901],
        ), mock.patch(
            "sales_support_agent.services.website_ops_github.time.sleep"
        ), mock.patch.dict(
            os.environ,
            {"WEBSITE_OPS_DEPLOY_VERIFY_TIMEOUT_SECONDS": "1"},
            clear=False,
        ):
            with self.assertRaisesRegex(
                ExecutionError,
                "durable publication commit was preserved",
            ):
                execute_github_article_action(
                    record,
                    config=SimpleNamespace(),
                    timestamp=datetime(2026, 8, 1, tzinfo=timezone.utc),
                )

        client.put_file.assert_called_once()

    def test_article_verification_accepts_the_site_title_suffix(self) -> None:
        record = self._generated_article_record()
        article = json.loads(str(record["action_value"]))
        source = '''import type { ArticlePageContent } from "@/components/pagekit/ArticlePage";
export type GeneratedArticle = { content: ArticlePageContent };
// WEBSITE_OPS_GENERATED_ARTICLES_START
export const GENERATED_ARTICLES: readonly GeneratedArticle[] = [];
// WEBSITE_OPS_GENERATED_ARTICLES_END
'''
        client = mock.Mock()
        client.repository = "david-anata/anata-website"
        client.branch = "main"
        client.get_file.return_value = (source, "source-sha")
        client.put_file.return_value = {"commit": {"sha": "commit-sha"}}
        page_url = f"https://anatainc.com/blog/{article['slug']}"
        with mock.patch(
            "sales_support_agent.services.website_ops_github.GitHubWebsiteClient",
            return_value=client,
        ), mock.patch(
            "sales_support_agent.services.website_ops_github.website_ops.collect_page_observation",
            return_value={
                "status_code": 200,
                "title": f"{article['title']} | Anata",
                "canonical_url": page_url,
                "h1": [article["content"]["h1"]],
            },
        ):
            result = execute_github_article_action(
                record,
                config=SimpleNamespace(),
                timestamp=datetime(2026, 8, 1, tzinfo=timezone.utc),
            )

        self.assertEqual(result["verification_status"], "verified")
        self.assertEqual(result["production_url"], page_url)

    def test_article_execution_reconciles_an_exact_existing_registry_entry(self) -> None:
        record = self._generated_article_record()
        article = json.loads(str(record["action_value"]))
        source = '''import type { ArticlePageContent } from "@/components/pagekit/ArticlePage";
export type GeneratedArticle = { content: ArticlePageContent };
// WEBSITE_OPS_GENERATED_ARTICLES_START
export const GENERATED_ARTICLES: readonly GeneratedArticle[] = ''' + json.dumps([article]) + ''';
// WEBSITE_OPS_GENERATED_ARTICLES_END
'''
        client = mock.Mock()
        client.repository = "david-anata/anata-website"
        client.branch = "main"
        client.get_file.return_value = (source, "source-sha")
        page_url = f"https://anatainc.com/blog/{article['slug']}"
        with mock.patch(
            "sales_support_agent.services.website_ops_github.GitHubWebsiteClient",
            return_value=client,
        ), mock.patch(
            "sales_support_agent.services.website_ops_github.website_ops.collect_page_observation",
            return_value={
                "status_code": 200,
                "title": f"{article['title']} | Anata",
                "canonical_url": page_url,
                "h1": [article["content"]["h1"]],
            },
        ):
            result = execute_github_article_action(
                record,
                config=SimpleNamespace(),
                timestamp=datetime(2026, 8, 1, tzinfo=timezone.utc),
            )

        self.assertEqual(result["verification_status"], "verified")
        self.assertTrue(result["summary"]["reconciled_existing"])
        self.assertEqual(result["commit_sha"], "")
        client.put_file.assert_not_called()

    def test_article_execution_rejects_existing_slug_with_different_payload(self) -> None:
        record = self._generated_article_record()
        article = json.loads(str(record["action_value"]))
        conflicting = {**article, "title": "A Different Valid Article Title"}
        source = '''import type { ArticlePageContent } from "@/components/pagekit/ArticlePage";
export type GeneratedArticle = { content: ArticlePageContent };
// WEBSITE_OPS_GENERATED_ARTICLES_START
export const GENERATED_ARTICLES: readonly GeneratedArticle[] = ''' + json.dumps([conflicting]) + ''';
// WEBSITE_OPS_GENERATED_ARTICLES_END
'''
        client = mock.Mock()
        client.get_file.return_value = (source, "source-sha")
        with mock.patch(
            "sales_support_agent.services.website_ops_github.GitHubWebsiteClient",
            return_value=client,
        ):
            with self.assertRaisesRegex(
                ExecutionError,
                "different payload",
            ):
                execute_github_article_action(record, config=SimpleNamespace())

        client.put_file.assert_not_called()

    def test_missing_canonical_creates_high_confidence_github_action(self) -> None:
        page = {
            "url": "https://anatainc.com/services/amazon-advertising/",
            "final_url": "https://anatainc.com/services/amazon-advertising/",
            "title": "Amazon Advertising Agency | Anata",
            "canonical_url": "",
            "issues": [
                {
                    "code": "MISSING_CANONICAL",
                    "summary": "The page does not declare a canonical URL.",
                }
            ],
        }
        with mock.patch.dict(
            os.environ,
            {
                "WEBSITE_OPS_GITHUB_TOKEN": "test-token",
                "WEBSITE_OPS_GITHUB_REPOSITORY": "david-anata/anata-website",
            },
            clear=False,
        ):
            actions = _deterministic_metadata_actions(
                page,
                {},
                {},
                primary_lead_event="generate_lead",
            )
        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0]["action_type"], "canonical_update")
        self.assertEqual(actions[0]["confidence"], "high")
        self.assertEqual(actions[0]["execution_eligibility"], "auto_execute")
        self.assertIn("production sitemap", actions[0]["insight_source"])

    def test_execute_feedback_action_meta_update_calls_plugin_endpoint(self) -> None:
        feedback = {
            "feedback_id": "fb_meta_123",
            "action_type": "meta_update",
            "page_url": "https://anatainc.com/services/fulfillment/",
            "action_value": json.dumps(
                {
                    "meta_title": "eCommerce 3PL Warehousing and Fulfillment | Anata",
                    "meta_description": "Scale fulfillment with direct-answer onboarding, systems setup, and launch support.",
                    "canonical_url": "https://anatainc.com/services/fulfillment/",
                }
            ),
        }
        plugin_response = {
            "ok": True,
            "action_type": "meta_update",
            "target_post_id": 2640,
            "target_url": "https://anatainc.com/services/fulfillment/",
            "before_meta": {
                "meta_title": "",
                "meta_description": "",
                "canonical_url": "https://anatainc.com/services/fulfillment/",
            },
            "after_meta": {
                "meta_title": "eCommerce 3PL Warehousing and Fulfillment | Anata",
                "meta_description": "Scale fulfillment with direct-answer onboarding, systems setup, and launch support.",
                "canonical_url": "https://anatainc.com/services/fulfillment/",
            },
            "updated_fields": ["meta_title", "meta_description"],
            "backup_reference": "post-meta:_anata_ops_before_snapshot,_anata_ops_after_snapshot",
        }
        with mock.patch.dict(
            os.environ,
            {
                "ANATA_OPS_SHARED_SECRET": "test-secret",
                "ANATA_OPS_BASE_URL": "https://anatainc.com",
            },
            clear=False,
        ):
            with mock.patch(
                "sales_support_agent.services.website_ops_vendor.executor.plugin_request",
                return_value=plugin_response,
            ) as plugin_request_mock:
                with mock.patch(
                    "sales_support_agent.services.website_ops_vendor.executor.collect_page_observation",
                    return_value={
                        "title": "eCommerce 3PL Warehousing and Fulfillment | Anata",
                        "meta_description": "Scale fulfillment with direct-answer onboarding, systems setup, and launch support.",
                        "canonical_url": "https://anatainc.com/services/fulfillment/",
                    },
                ):
                    with mock.patch(
                        "sales_support_agent.services.website_ops_vendor.executor._fetch_live_html",
                        return_value="<html><head><title>eCommerce 3PL Warehousing and Fulfillment | Anata</title><meta name='description' content='Scale fulfillment with direct-answer onboarding, systems setup, and launch support.' /><link rel='canonical' href='https://anatainc.com/services/fulfillment/' /></head><body></body></html>",
                    ):
                        result = execute_feedback_action(feedback)
        self.assertEqual(result["verification_status"], "verified")
        self.assertEqual(result["target_post_id"], 2640)
        sent_payload = plugin_request_mock.call_args.args[0]
        self.assertEqual(sent_payload["action_type"], "meta_update")
        self.assertEqual(sent_payload["meta_title"], "eCommerce 3PL Warehousing and Fulfillment | Anata")
        self.assertEqual(sent_payload["meta_description"], "Scale fulfillment with direct-answer onboarding, systems setup, and launch support.")
        self.assertEqual(sent_payload["canonical_url"], "https://anatainc.com/services/fulfillment/")

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
                            "top_queries": [
                                {"query": "how does fulfillment work", "impressions": 70.0, "clicks": 1.0},
                                {"query": "what does fulfillment cost", "impressions": 54.0, "clicks": 0.0},
                            ],
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
                                "faq_patterns": ["How does fulfillment work?"],
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
            self.assertEqual(faq_action["execution_eligibility"], "suggestion_only")
            self.assertEqual({item["action_type"] for item in overlay["action_queue"]}, {"inject_faq_block"})
            self.assertFalse(overlay["customer_questions"])
            self.assertTrue(overlay["serp_blueprints"])
            self.assertTrue(overlay["content_tasks"])
            insight = overlay["page_insights"][0]
            self.assertEqual(insight["customer_question_count"], 0)
            self.assertTrue(insight["blueprint_found"])
            self.assertTrue(insight["faq_demand_detected"])
            self.assertFalse(insight["page_thin_enough"])
            self.assertEqual(insight["query_seed"], "how does fulfillment work")
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

    def test_dashboard_render_shows_current_and_next_work(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = self._settings(Path(tmpdir))
            html = render_dashboard_page(settings)
            self.assertIn("What Agent is working on next", html)
            self.assertIn("Import and classify Search Console indexing exclusions", html)
            self.assertIn("Validate qualified-lead attribution", html)
            self.assertIn("Earn citations. Never manufacture links.", html)
            self.assertIn("Measure movement without claiming causation.", html)

    def test_indexing_page_renders_classified_inventory(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = self._settings(Path(tmpdir))
            directory = settings.website_ops_root / "indexing"
            directory.mkdir(parents=True)
            (directory / "search-console.csv").write_text(
                "URL,Reason,Last crawled\n"
                "https://anatainc.com/services/fulfillment/,Crawled - currently not indexed,Jul 23 2026\n"
                "https://anatainc.com/wp-*.php,Blocked due to access forbidden (403),Jul 23 2026\n",
                encoding="utf-8",
            )
            html = render_indexing_page(settings)
            self.assertIn("Every known URL gets a desired search state", html)
            self.assertIn("services/fulfillment", html)
            self.assertIn("Blocked Intentionally", html)
            self.assertIn("2 records", html)
            self.assertIn("Import crawl evidence", html)
            self.assertIn('type="file"', html)
            self.assertIn("Production Crawl", html)

    def test_indexing_page_surfaces_failed_api_attempt(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = self._settings(Path(tmpdir))
            directory = settings.website_ops_root / "indexing"
            directory.mkdir(parents=True)
            (directory / "inventory.json").write_text(
                json.dumps(
                    {
                        "records": [],
                        "summary": {
                            "known_urls": 0,
                            "needs_action": 0,
                            "intentional_exclusions": 0,
                        },
                        "inspection": {
                            "attempted": 47,
                            "succeeded": 0,
                            "failed": 47,
                            "failure_samples": [
                                "https://anatainc.com/: ReadTimeout"
                            ],
                        },
                    }
                ),
                encoding="utf-8",
            )

            html = render_indexing_page(settings)

            self.assertIn("latest inspection attempt needs attention", html)
            self.assertIn("47 of 47 canonical URL inspections failed", html)
            self.assertIn("ReadTimeout", html)

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
                        "execution_eligibility": "suggestion_only",
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
                    result = run_website_ops(settings, mode="daily")
            self.assertTrue(result.ok)
            records = load_feedback_records(settings)
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0]["status"], "new")
            self.assertEqual(records[0]["action_type"], "")

    def test_run_website_ops_resumes_durable_new_action_before_fresh_analysis(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = self._settings(Path(tmpdir), execute_approved=True)
            report_dir = settings.website_ops_root / "reports" / "daily"
            report_dir.mkdir(parents=True)
            (report_dir / "prior-report.md").write_text(
                "# Prior report\n", encoding="utf-8"
            )
            record = save_feedback_record(
                settings,
                {
                    "feedback_id": "durable-new-action",
                    "status": "new",
                    "auto_generated": True,
                    "page_url": "https://anatainc.com/blog/durable-recovery-test",
                    "confidence": "high",
                    "suggested_action_type": "publish_blog_article",
                    "suggested_action_value": json.dumps({"slug": "durable-recovery-test"}),
                    "execution_eligibility": "auto_execute",
                    "evidence": ["Validated intent gap", "Official sources"],
                },
            )
            fake_pipeline = {
                "report": self._fake_report(),
                "observations": [],
                "artifacts": {},
            }
            fake_overlay = {
                "goal": {"primary": "Increase qualified leads."},
                "action_queue": [],
                "analytics_status": {},
                "support_requests": [],
                "page_insights": [],
            }
            execution_result = {
                "feedback_id": record["feedback_id"],
                "executed_at": "2026-08-01T00:00:00+00:00",
                "verification_status": "verified",
            }
            with mock.patch(
                "sales_support_agent.services.website_ops.website_ops.run_daily_report_pipeline",
                return_value=fake_pipeline,
            ), mock.patch(
                "sales_support_agent.services.website_ops.build_autonomy_overlay",
                return_value=fake_overlay,
            ), mock.patch(
                "sales_support_agent.services.website_ops._checkpoint_website_ops_cache",
                return_value=True,
            ) as checkpoint, mock.patch(
                "sales_support_agent.services.website_ops._execute_record",
                return_value=execution_result,
            ) as execute:
                result = run_website_ops(settings, mode="daily")

            self.assertIsNotNone(result.report)
            checkpoint.assert_called_once_with(settings)
            execute.assert_called_once()
            resumed = execute.call_args.args[2]
            self.assertEqual(resumed["status"], "approved")
            self.assertEqual(resumed["action_type"], "publish_blog_article")

    def test_run_website_ops_approves_entire_bounded_batch_before_execution(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = self._settings(Path(tmpdir), execute_approved=True)
            report_dir = settings.website_ops_root / "reports" / "daily"
            report_dir.mkdir(parents=True)
            (report_dir / "prior-report.md").write_text("# Prior report\n", encoding="utf-8")
            actions = [
                {
                    "page_url": f"https://anatainc.com/services/service-{index}/",
                    "page_title": f"Service {index}",
                    "action_type": "meta_title_update",
                    "section_name": "Title",
                    "before_state": f"Old title {index}",
                    "after_state": f"New title {index}",
                    "reason": "The canonical intent owner needs a specific title.",
                    "insight_source": "Validated intent map",
                    "expected_impact": "Clearer intent ownership.",
                    "confidence": "high",
                    "evidence": ["Rendered title", "Repository owner map"],
                    "execution_eligibility": "auto_execute",
                    "action_value": f"New title {index}",
                }
                for index in range(2)
            ]
            fake_pipeline = {
                "report": self._fake_report(),
                "observations": [],
                "artifacts": {},
            }
            fake_overlay = {
                "goal": {"primary": "Increase qualified leads."},
                "action_queue": actions,
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
            events: list[str] = []

            def interrupted_execution(*args: object, **kwargs: object) -> None:
                self.assertEqual(events, ["checkpoint"])
                raise website_ops.ExecutionError("deploy interrupted")

            with mock.patch(
                "sales_support_agent.services.website_ops.website_ops.run_daily_report_pipeline",
                return_value=fake_pipeline,
            ), mock.patch(
                "sales_support_agent.services.website_ops.build_autonomy_overlay",
                return_value=fake_overlay,
            ), mock.patch(
                "sales_support_agent.services.website_ops._checkpoint_website_ops_cache",
                side_effect=lambda _settings: events.append("checkpoint") or True,
            ), mock.patch(
                "sales_support_agent.services.website_ops._execute_record",
                side_effect=interrupted_execution,
            ):
                with self.assertRaises(website_ops.ExecutionError):
                    run_website_ops(settings, mode="daily")

            records = load_feedback_records(settings)
            self.assertEqual(len(records), 2)
            self.assertEqual({record["status"] for record in records}, {"approved"})
            self.assertEqual(
                {record["action_type"] for record in records},
                {"meta_title_update"},
            )

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

    def test_latest_report_entry_uses_enriched_json_artifact_time(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = self._settings(Path(tmpdir))
            daily = settings.website_ops_root / "reports" / "daily"
            weekly = settings.website_ops_root / "reports" / "weekly"
            daily.mkdir(parents=True, exist_ok=True)
            weekly.mkdir(parents=True, exist_ok=True)
            daily_md = daily / "2026-03-27-daily.md"
            weekly_md = weekly / "2026-03-26-weekly.md"
            daily_json = daily_md.with_suffix(".json")
            daily_md.write_text("# Daily\n\nDate: 2026-03-27\n")
            daily_json.write_text("{}")
            weekly_md.write_text("# Weekly\n\nDate: 2026-03-26\n")
            now = datetime.now(timezone.utc).timestamp()
            os.utime(daily_md, (now - 30, now - 30))
            os.utime(weekly_md, (now - 10, now - 10))
            os.utime(daily_json, (now, now))
            entry = latest_report_entry(settings)
            self.assertIsNotNone(entry)
            assert entry is not None
            self.assertEqual(entry["slug"], "2026-03-27-daily")


if __name__ == "__main__":
    unittest.main()
