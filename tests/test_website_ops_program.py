from __future__ import annotations

import json
import io
import tempfile
import unittest
import zipfile
from pathlib import Path

from sales_support_agent.services.website_ops_program import (
    build_indexing_inventory,
    build_program_plan,
    classify_indexing_record,
    load_indexing_inventory,
)
from sales_support_agent.services.website_ops_screaming_frog import (
    ScreamingFrogImportError,
    import_screaming_frog_zip,
    load_crawl_inventory,
)


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
        self.assertEqual(plan["next"][0]["title"], "Validate qualified-lead attribution")
        self.assertTrue(plan["next"][0]["needs_david"])

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
