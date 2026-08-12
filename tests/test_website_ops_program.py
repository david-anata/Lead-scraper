from __future__ import annotations

import json
import io
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest import mock

from sales_support_agent.services.website_ops_program import (
    build_indexing_inventory,
    build_program_plan,
    classify_indexing_record,
    load_indexing_inventory,
)
from sales_support_agent.services.website_ops_screaming_frog import (
    ScreamingFrogImportError,
    build_crawl_verification,
    collect_crawl_resource_observations,
    import_screaming_frog_zip,
    load_crawl_inventory,
    load_crawl_verification,
    save_crawl_verification,
)
from sales_support_agent.services.website_ops_vendor.core import inspect_html_document


class WebsiteOpsProgramTests(unittest.TestCase):
    @staticmethod
    def _crawl_zip(files: dict[str, str]) -> bytes:
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
            for name, content in files.items():
                archive.writestr(name, content)
        return buffer.getvalue()

    def test_production_router_registers_indexing_control_room(self) -> None:
        from sales_support_agent.api.router import router

        self.assertIn(
            "/admin/website-ops/indexing",
            {route.path for route in router.routes},
        )
        methods = {
            method
            for route in router.routes
            if route.path == "/admin/website-ops/indexing"
            for method in route.methods
        }
        self.assertIn("GET", methods)
        self.assertIn("POST", methods)

    def test_wordpress_system_pattern_is_an_intentional_403(self) -> None:
        record = classify_indexing_record(
            {
                "url": "https://anatainc.com/wp-*.php",
                "reason": "Blocked due to access forbidden (403)",
                "last_crawled": "2026-07-23",
            }
        )
        self.assertTrue(record["intentional"])
        self.assertEqual(record["desired_state"], "blocked intentionally")
        self.assertIn("Retain", record["next_operation"])

    def test_real_marketing_403_requires_high_priority_investigation(self) -> None:
        record = classify_indexing_record(
            {
                "url": "https://anatainc.com/services/fulfillment/",
                "reason": "Blocked due to access forbidden (403)",
            }
        )
        self.assertFalse(record["intentional"])
        self.assertEqual(record["desired_state"], "investigate")
        self.assertEqual(record["priority"], "high")

    def test_inventory_preserves_reason_and_desired_state_counts(self) -> None:
        inventory = build_indexing_inventory(
            [
                {
                    "url": "https://anatainc.com/a",
                    "reason": "Crawled - currently not indexed",
                },
                {
                    "url": "https://anatainc.com/b",
                    "reason": "Page with redirect",
                },
                {
                    "url": "https://anatainc.com/wp-*.php",
                    "reason": "Blocked due to access forbidden (403)",
                },
            ]
        )
        self.assertEqual(inventory["summary"]["known_urls"], 3)
        self.assertEqual(inventory["summary"]["needs_action"], 2)
        self.assertEqual(inventory["summary"]["intentional_exclusions"], 1)
        self.assertEqual(inventory["summary"]["desired_state_counts"]["investigate"], 1)

    def test_load_inventory_imports_search_console_csv(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            directory = root / "indexing"
            directory.mkdir()
            (directory / "crawled.csv").write_text(
                "URL,Last crawled,Reason\n"
                "https://anatainc.com/guide,Jul 23 2026,Crawled - currently not indexed\n",
                encoding="utf-8",
            )
            inventory = load_indexing_inventory(root)
        self.assertEqual(inventory["summary"]["known_urls"], 1)
        self.assertEqual(
            inventory["records"][0]["reason"],
            "Crawled - currently not indexed",
        )

    def test_durable_inventory_takes_precedence_over_imports(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            directory = root / "indexing"
            directory.mkdir()
            payload = build_indexing_inventory(
                [{"url": "https://anatainc.com/a", "reason": "Not found (404)"}]
            )
            (directory / "inventory.json").write_text(
                json.dumps(payload),
                encoding="utf-8",
            )
            (directory / "other.csv").write_text(
                "URL,Reason\nhttps://anatainc.com/b,Page with redirect\n",
                encoding="utf-8",
            )
            inventory = load_indexing_inventory(root)
        self.assertEqual(inventory["summary"]["known_urls"], 1)
        self.assertEqual(inventory["records"][0]["url"], "https://anatainc.com/a")

    def test_crawl_inventory_is_not_misreported_as_search_console_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            directory = root / "indexing"
            directory.mkdir()
            (directory / "crawl_inventory.json").write_text(
                json.dumps(
                    {
                        "records": [
                            {
                                "url": "https://anatainc.com/",
                                "environment": "production",
                                "warnings": [],
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            (directory / "crawl_verification.json").write_text(
                json.dumps({"records": [{"url": "https://anatainc.com/noise"}]}),
                encoding="utf-8",
            )
            inventory = load_indexing_inventory(root)
        self.assertEqual(inventory["summary"]["known_urls"], 0)

    def test_program_plan_exposes_indexing_and_measurement_next_work(self) -> None:
        plan = build_program_plan(
            analytics_status={
                "search_console": True,
                "ga4": True,
                "ga4_trust_status": "partial",
                "primary_lead_event": "generate_lead",
            },
            action_queue=[],
            support_requests=[],
            indexing_inventory=build_indexing_inventory([]),
        )
        self.assertEqual(
            plan["current"]["title"],
            "Import and classify Search Console indexing exclusions",
        )
        self.assertEqual(plan["next"][0]["title"], "Verify qualified-lead attribution")
        self.assertEqual(plan["next"][0]["state"], "Scheduled")
        self.assertFalse(plan["next"][0]["needs_david"])

    def test_screaming_frog_import_separates_hosts_and_preserves_warnings(self) -> None:
        payload = self._crawl_zip(
            {
                "crawl/internal_all.csv": (
                    "Address,Status Code,Indexability,Title 1,H1-1,Word Count,Crawl Depth\n"
                    "https://anatainc.com/services/amazon-advertising,200,Indexable,Amazon Ads,Amazon Ads,850,2\n"
                    "https://branch.vercel.app/services/amazon-advertising,200,Indexable,Preview,Preview,850,2\n"
                ),
                "crawl/issues_reports/h1_missing.csv": (
                    "Address,Issue\n"
                    "https://anatainc.com/services/amazon-advertising,H1 missing\n"
                ),
            }
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            inventory = import_screaming_frog_zip(
                filename="crawl.zip",
                payload=payload,
                root=root,
            )
            self.assertEqual(inventory["summary"]["production_urls"], 1)
            self.assertEqual(inventory["summary"]["sandbox_urls"], 1)
            self.assertEqual(inventory["summary"]["urls_with_warnings"], 1)
            production = next(
                row
                for row in inventory["records"]
                if row["environment"] == "production"
            )
            self.assertEqual(production["status_code"], 200)
            self.assertEqual(production["h1"], "Amazon Ads")
            self.assertEqual(
                production["warnings"][0]["report"],
                "h1_missing.csv",
            )
            self.assertIn("unverified evidence", inventory["policy"])
            self.assertEqual(load_crawl_inventory(root)["summary"], inventory["summary"])

    def test_crawl_verification_requires_fresh_rendered_evidence(self) -> None:
        inventory = {
            "records": [
                {
                    "url": "https://anatainc.com/services/amazon-advertising",
                    "environment": "production",
                    "warnings": [
                        {"report": "h1_missing.csv", "evidence": "H1 missing"},
                        {"report": "canonicals_missing.csv", "evidence": "Canonical missing"},
                    ],
                },
                {
                    "url": "https://anatainc.com/_next/static/app.js",
                    "environment": "production",
                    "warnings": [
                        {"report": "security_headers_missing.csv", "evidence": "Header missing"},
                    ],
                },
            ]
        }
        verification = build_crawl_verification(
            inventory,
            [
                {
                    "url": "https://anatainc.com/services/amazon-advertising/",
                    "status_code": 200,
                    "fetched_at": "2026-07-27T14:00:00+00:00",
                    "title": "Amazon Advertising",
                    "meta_description": "Accountable Amazon advertising operations for scaling brands.",
                    "canonical_url": "",
                    "h1": ["Amazon advertising, connected to profit."],
                    "h2": ["How we work"],
                }
            ],
        )
        page = verification["records"][0]
        self.assertEqual(page["state"], "confirmed")
        self.assertEqual(page["warning_results"][0]["verdict"], "disproved")
        self.assertEqual(page["warning_results"][1]["verdict"], "confirmed")
        resource = verification["records"][1]
        self.assertEqual(resource["state"], "pending")
        self.assertEqual(verification["summary"]["confirmed_urls"], 1)
        self.assertEqual(verification["summary"]["pending_urls"], 1)

    def test_crawl_verification_persists_without_becoming_gsc_inventory(self) -> None:
        payload = build_crawl_verification({"records": []}, [])
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            save_crawl_verification(root, payload)
            self.assertEqual(load_crawl_verification(root)["summary"], payload["summary"])
            self.assertEqual(load_indexing_inventory(root)["summary"]["known_urls"], 0)

    def test_rendered_evidence_captures_images_links_and_response_headers(self) -> None:
        observation = inspect_html_document(
            "https://anatainc.com/guide",
            (
                "<html><head><title>Guide</title></head><body>"
                '<img src="/informative.webp">'
                '<img src="/decorative.webp" alt="" width="20" height="20">'
                '<a href="/services">Services</a>'
                "</body></html>"
            ),
            headers={
                "X-Content-Type-Options": "nosniff",
                "Referrer-Policy": "strict-origin-when-cross-origin",
            },
        )
        self.assertFalse(observation["images"][0]["has_alt_attribute"])
        self.assertTrue(observation["images"][1]["has_alt_attribute"])
        self.assertEqual(observation["links"][0]["href"], "/services")
        self.assertEqual(
            observation["response_headers"]["x-content-type-options"],
            "nosniff",
        )

    def test_verification_separates_confirmed_defects_security_noise_and_stale_warnings(self) -> None:
        inventory = {
            "records": [
                {
                    "url": "https://anatainc.com/guide",
                    "environment": "production",
                    "warnings": [
                        {"report": "images_missing_alt_attribute_inlinks.csv"},
                        {"report": "security_missing_contentsecuritypolicy_header.csv"},
                        {"report": "security_missing_xcontenttypeoptions_header.csv"},
                        {"report": "blocked_by_robots_txt_inlinks.csv"},
                    ],
                }
            ]
        }
        observation = inspect_html_document(
            "https://anatainc.com/guide",
            '<html><body><img src="/guide.webp"><a href="/services">Services</a></body></html>',
            headers={"X-Content-Type-Options": "nosniff"},
        )
        observation["robots_allowed_googlebot"] = True
        verification = build_crawl_verification(inventory, [observation])
        verdicts = {
            item["report"]: item["verdict"]
            for item in verification["records"][0]["warning_results"]
        }
        self.assertEqual(verdicts["images_missing_alt_attribute_inlinks.csv"], "confirmed")
        self.assertEqual(
            verdicts["security_missing_contentsecuritypolicy_header.csv"],
            "noise",
        )
        self.assertEqual(
            verdicts["security_missing_xcontenttypeoptions_header.csv"],
            "disproved",
        )
        self.assertEqual(verdicts["blocked_by_robots_txt_inlinks.csv"], "disproved")
        self.assertEqual(verification["summary"]["confirmed_urls"], 1)
        self.assertEqual(verification["summary"]["noise_warnings"], 1)
        self.assertEqual(verification["summary"]["disproved_warnings"], 2)

    def test_resource_observation_is_bounded_to_unobserved_production_warning_urls(self) -> None:
        inventory = {
            "records": [
                {
                    "url": "https://anatainc.com/",
                    "environment": "production",
                    "warnings": [{"report": "security_missing_hsts_header.csv"}],
                },
                {
                    "url": "https://anatainc.com/image.webp",
                    "environment": "production",
                    "warnings": [{"report": "images_over_100_kb.csv"}],
                },
                {
                    "url": "https://example.com/external.webp",
                    "environment": "external",
                    "warnings": [{"report": "images_over_100_kb.csv"}],
                },
            ]
        }
        with (
            mock.patch(
                "sales_support_agent.services.website_ops_screaming_frog._head_observation",
                return_value={
                    "url": "https://anatainc.com/image.webp",
                    "status_code": 200,
                    "response_headers": {"content-length": "1200"},
                },
            ) as head,
            mock.patch(
                "sales_support_agent.services.website_ops_screaming_frog.urllib.robotparser.RobotFileParser.read",
                side_effect=OSError("offline"),
            ),
        ):
            observations = collect_crawl_resource_observations(
                inventory,
                [{"url": "https://anatainc.com/", "status_code": 200}],
            )
        head.assert_called_once()
        self.assertEqual(len(observations), 2)
        self.assertEqual(observations[1]["url"], "https://anatainc.com/image.webp")

    def test_screaming_frog_import_rejects_unsafe_archive_paths(self) -> None:
        payload = self._crawl_zip(
            {"../internal_all.csv": "Address\nhttps://anatainc.com/\n"}
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaises(ScreamingFrogImportError):
                import_screaming_frog_zip(
                    filename="crawl.zip",
                    payload=payload,
                    root=Path(temp_dir),
                )

    def test_qualified_action_precedes_indexing_backlog(self) -> None:
        plan = build_program_plan(
            analytics_status={
                "search_console": True,
                "ga4": True,
                "ga4_trust_status": "trusted",
            },
            action_queue=[
                {
                    "page_url": "https://anatainc.com/services/fulfillment/",
                    "action_type": "meta_update",
                    "section_name": "Search metadata",
                    "after_state": "Use intent-aligned metadata.",
                    "execution_eligibility": "auto_execute",
                    "confidence": "high",
                    "evidence": ["Observed non-branded query demand."],
                }
            ],
            support_requests=[],
            indexing_inventory=build_indexing_inventory([]),
        )
        self.assertEqual(plan["current"]["title"], "Search metadata")
        self.assertEqual(plan["current"]["state"], "Ready")
        self.assertEqual(
            plan["next"][0]["title"],
            "Import and classify Search Console indexing exclusions",
        )


if __name__ == "__main__":
    unittest.main()
