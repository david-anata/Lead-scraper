"""Tests for the Brand Analysis pipeline CRM: DB migrations, storage helpers,
PATCH/DELETE endpoints, and page rendering.

Uses an in-memory SQLite DB and the FastAPI test client — no LLM, no network.
"""

from __future__ import annotations

import os
import unittest

os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")
os.environ.setdefault("OPENAI_API_KEY", "test-key")
os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("GOOGLE_CLIENT_ID", "test-id")
os.environ.setdefault("GOOGLE_CLIENT_SECRET", "test-secret")

try:
    from fastapi.testclient import TestClient

    from sales_support_agent.main import app
    from sales_support_agent.models.database import get_engine
    from sales_support_agent.models.entities import BrandAnalysisReport as ReportRow
    from sales_support_agent.services.brand_analysis import storage
    from sales_support_agent.services.brand_analysis.report_page import (
        _STAGE_META,
        render_pipeline_page,
    )
    from sales_support_agent.services.brand_analysis.schema import CATEGORY_DTC
    from sqlalchemy.orm import Session

    DEPS_AVAILABLE = True
except (ModuleNotFoundError, ImportError) as exc:
    if getattr(exc, "name", None) not in {"sqlalchemy", "openpyxl", "fastapi"}:
        raise
    DEPS_AVAILABLE = False


def setUpModule() -> None:
    """Restore this module's DB engine before its tests run.

    The engine in ``models.database`` is a shared mutable global. Other test
    modules (e.g. test_admin_dashboard) call ``create_session_factory`` with a
    true ``:memory:`` URL and leave the global engine pointed at an ephemeral DB
    without ``brand_analysis_reports``. These tests read ``get_engine()``
    directly and hit the endpoint via a TestClient thread, so they need the same
    file-backed engine that ``main.create_app`` builds at import (a true
    ``:memory:`` engine would not persist across the TestClient thread). Re-run
    that exact init so the module is order-independent.
    """
    if not DEPS_AVAILABLE:
        return
    from sales_support_agent.config import load_settings
    from sales_support_agent.models.database import create_session_factory, init_database

    settings = load_settings()
    session_factory = create_session_factory(settings.sales_agent_db_url)
    init_database(session_factory)


def _make_row(brand: str = "TestBrand", stage: str = "new") -> str:
    """Insert a minimal BrandAnalysisReport row and return its id."""
    import uuid
    from datetime import datetime, timezone

    rid = str(uuid.uuid4())
    engine = get_engine()
    with Session(engine) as s:
        row = ReportRow(
            id=rid,
            brand=brand,
            category=CATEGORY_DTC,
            status="complete",
            grade="B",
            score_100=68,
            confidence="Medium",
            period_current="FY2025",
            period_prior="FY2024",
            stage=stage,
            slug=brand.lower(),
            share_token="tok123",
            report_json={
                "recommendation": "Conditional Buy",
                "current": {
                    "net_revenue_cents": 120_000_000,
                    "net_margin_bps": 1200,
                    "contribution_margin_bps": 4500,
                    "blended_mer": 3.1,
                },
                "yoy_revenue_growth_bps": 1800,
                "scorecard": {"dimensions": []},
                "investment_thesis": ["Strong DTC brand", "Growing repeat rate"],
                "key_risks": ["Single channel dependency"],
                "red_flags": [],
            },
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        s.add(row)
        s.commit()
    return rid


@unittest.skipUnless(DEPS_AVAILABLE, "pipeline deps required")
class StageMeta(unittest.TestCase):
    def test_all_stage_keys_have_label_and_color(self) -> None:
        for key, meta in _STAGE_META.items():
            self.assertIn("label", meta, f"stage {key} missing label")
            self.assertIn("color", meta, f"stage {key} missing color")

    def test_seven_stages(self) -> None:
        self.assertEqual(len(_STAGE_META), 7)


@unittest.skipUnless(DEPS_AVAILABLE, "pipeline deps required")
class ListPipelineReports(unittest.TestCase):
    def test_returns_stage_field(self) -> None:
        rid = _make_row("PipelineBrandA", stage="reviewing")
        rows = storage.list_pipeline_reports()
        match = next((r for r in rows if r["id"] == rid), None)
        self.assertIsNotNone(match, "inserted row not found in list_pipeline_reports")
        self.assertEqual(match["stage"], "reviewing")

    def test_returns_financial_fields(self) -> None:
        rid = _make_row("PipelineBrandB")
        rows = storage.list_pipeline_reports()
        match = next((r for r in rows if r["id"] == rid), None)
        self.assertIsNotNone(match)
        self.assertEqual(match["recommendation"], "Conditional Buy")
        self.assertEqual(match["net_revenue_cents"], 120_000_000)


@unittest.skipUnless(DEPS_AVAILABLE, "pipeline deps required")
class SetStage(unittest.TestCase):
    def test_valid_stage_persists(self) -> None:
        rid = _make_row("SetStageBrand")
        ok = storage.set_stage(rid, "loi")
        self.assertTrue(ok)
        rows = storage.list_pipeline_reports()
        match = next((r for r in rows if r["id"] == rid), None)
        self.assertIsNotNone(match)
        self.assertEqual(match["stage"], "loi")

    def test_nonexistent_id_returns_false(self) -> None:
        ok = storage.set_stage("00000000-0000-0000-0000-000000000000", "reviewing")
        self.assertFalse(ok)


def _make_test_client():
    """Mini FastAPI app with brand_analysis_router and auth dependency bypassed."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from sales_support_agent.api import brand_analysis_router as bar

    _app = FastAPI()
    _app.include_router(bar.router)
    _app.dependency_overrides[bar.router.dependencies[0].dependency] = lambda: {
        "email": "test@anatainc.com", "is_superadmin": True, "permissions": set(),
    }
    return TestClient(_app)


@unittest.skipUnless(DEPS_AVAILABLE, "pipeline deps required")
class PipelineEndpoints(unittest.TestCase):
    def setUp(self) -> None:
        self.client = _make_test_client()

    def test_stage_patch_valid(self) -> None:
        rid = _make_row("EndpointBrandA")
        resp = self.client.patch(
            f"/admin/executive/brand-analysis/{rid}/stage",
            json={"stage": "advancing"},
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json(), {"ok": True})

    def test_stage_patch_invalid_key(self) -> None:
        rid = _make_row("EndpointBrandB")
        resp = self.client.patch(
            f"/admin/executive/brand-analysis/{rid}/stage",
            json={"stage": "not_a_real_stage"},
        )
        self.assertEqual(resp.status_code, 422)

    def test_pipeline_page_renders(self) -> None:
        _make_row("PipelinePageBrand")
        resp = self.client.get("/admin/executive/brand-analysis/pipeline")
        self.assertEqual(resp.status_code, 200)
        self.assertIn("Pipeline", resp.text)
        self.assertIn("stage-select", resp.text)

    def test_note_patch_persists(self) -> None:
        rid = _make_row("NoteBrand")
        resp = self.client.patch(
            f"/admin/executive/brand-analysis/{rid}/note",
            json={"notes": "Great margins, check seller tenure"},
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json(), {"ok": True})
        rows = storage.list_pipeline_reports()
        match = next((r for r in rows if r["id"] == rid), None)
        self.assertIsNotNone(match)
        self.assertEqual(match["notes"], "Great margins, check seller tenure")

    def test_deal_patch_persists(self) -> None:
        rid = _make_row("DealBrand")
        resp = self.client.patch(
            f"/admin/executive/brand-analysis/{rid}/deal",
            json={"ask_price_cents": 250000_00},
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json(), {"ok": True})
        rows = storage.list_pipeline_reports()
        match = next((r for r in rows if r["id"] == rid), None)
        self.assertIsNotNone(match)
        self.assertEqual(match["ask_price_cents"], 250000_00)

    def test_contact_patch_persists(self) -> None:
        rid = _make_row("ContactBrand")
        resp = self.client.patch(
            f"/admin/executive/brand-analysis/{rid}/contact",
            json={"contact_name": "Jane Smith", "contact_email": "jane@brand.com"},
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json(), {"ok": True})
        rows = storage.list_pipeline_reports()
        match = next((r for r in rows if r["id"] == rid), None)
        self.assertIsNotNone(match)
        self.assertEqual(match["contact_name"], "Jane Smith")
        self.assertEqual(match["contact_email"], "jane@brand.com")

    def test_social_patch_recomputes_grade(self) -> None:
        rid = _make_row("SocialPatchBrand")
        resp = self.client.patch(
            f"/admin/executive/brand-analysis/{rid}/social",
            json={
                "email_list_size": 0,
                "social_handles": {},
                "social_signals": {"review_rating": 4.8, "review_count": 800},
            },
        )
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertTrue(data["ok"])
        self.assertIn(data["social_grade"], ("A", "B", "C", "D", "F"))
        self.assertGreater(data["social_score_100"], 0)
        # Persisted to report_json
        rows = storage.list_pipeline_reports()
        match = next((r for r in rows if r["id"] == rid), None)
        self.assertIsNotNone(match)
        self.assertEqual(match["social_grade"], data["social_grade"])

    def test_deal_patch_null_clears_price(self) -> None:
        rid = _make_row("DealBrandClear")
        self.client.patch(
            f"/admin/executive/brand-analysis/{rid}/deal",
            json={"ask_price_cents": 500000_00},
        )
        resp = self.client.patch(
            f"/admin/executive/brand-analysis/{rid}/deal",
            json={"ask_price_cents": None},
        )
        self.assertEqual(resp.status_code, 200)
        rows = storage.list_pipeline_reports()
        match = next((r for r in rows if r["id"] == rid), None)
        self.assertIsNone(match["ask_price_cents"])


@unittest.skipUnless(DEPS_AVAILABLE, "pipeline deps required")
class RenderPipelinePage(unittest.TestCase):
    def test_empty_state(self) -> None:
        html = render_pipeline_page([], user=None)
        self.assertIn("No analyses yet", html)

    def _make_pipeline_row(self, brand: str = "TestBrand") -> dict:
        return {
            "id": "abc123",
            "brand": brand,
            "label": "",
            "status": "complete",
            "stage": "reviewing",
            "grade": "B",
            "score_100": 70,
            "confidence": "High",
            "period_current": "FY2025",
            "period_prior": "FY2024",
            "share_token": "",
            "share_path": "",
            "slug": "",
            "updated_at": None,
            "created_at": None,
            "recommendation": "Conditional Buy",
            "net_revenue_cents": 5_000_000_00,
            "net_margin_bps": 1500,
            "contribution_margin_bps": 4000,
            "blended_mer": 3.2,
            "yoy_revenue_growth_bps": 2000,
            "social_grade": "A",
            "social_score_100": 82,
            "social_confidence": "High",
            "social_dimensions": [],
            "email_list_size": 0,
            "social_handles": {},
            "social_signals": {},
            "notes": "",
            "ask_price_cents": None,
            "scorecard_dimensions": [],
            "investment_thesis": ["Strong brand"],
            "key_risks": ["Concentration risk"],
            "red_flags": [],
        }

    def test_renders_brand_and_grade(self) -> None:
        row = self._make_pipeline_row()
        html = render_pipeline_page([row], user=None)
        self.assertIn("TestBrand", html)
        self.assertIn("Reviewing", html)
        self.assertIn("Conditional Buy", html)

    def test_renders_mer_and_cm_columns(self) -> None:
        row = self._make_pipeline_row()
        html = render_pipeline_page([row], user=None)
        self.assertIn("Blended MER", html)
        self.assertIn("Contribution margin %", html)

    def test_renders_social_score(self) -> None:
        row = self._make_pipeline_row()
        html = render_pipeline_page([row], user=None)
        self.assertIn("82/100", html)

    def test_renders_deal_info_zone(self) -> None:
        row = self._make_pipeline_row()
        html = render_pipeline_page([row], user=None)
        self.assertIn("Deal Info", html)
        self.assertIn("deal-note", html)
        self.assertIn("deal-price", html)


if __name__ == "__main__":
    unittest.main()
