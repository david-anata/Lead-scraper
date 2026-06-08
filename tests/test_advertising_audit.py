"""Integration tests for the advertising audit: storage, the run orchestrator,
and the HTTP routes (in-memory SQLite + FastAPI TestClient)."""

from __future__ import annotations

import io
import tempfile
import unittest

import openpyxl
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool


def _make_engine():
    return create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )


def _bootstrap_db(engine):
    from sqlalchemy import text
    from sales_support_agent.models.database import Base, _register_models
    _register_models()
    Base.metadata.create_all(bind=engine)
    # kv_store is created by raw-SQL migrations in prod (not an ORM model); the
    # advertising service persists generated workbooks there for durability.
    with engine.begin() as conn:
        conn.execute(text(
            "CREATE TABLE IF NOT EXISTS kv_store ("
            "key TEXT PRIMARY KEY, value TEXT NOT NULL DEFAULT '', updated_at TEXT NOT NULL DEFAULT '')"
        ))


def _patch_global_engine(engine):
    import sales_support_agent.models.database as db_module
    old = db_module.engine
    db_module.engine = engine
    return old


def _bulk_xlsx() -> bytes:
    header = ["Product", "Entity", "Operation", "Campaign ID", "Ad Group ID", "Keyword ID",
              "Campaign Name (Informational only)", "Ad Group Name (Informational only)",
              "Keyword Text", "Match Type", "Bid", "Impressions", "Clicks", "Spend", "Sales", "Orders", "Units"]
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Sponsored Products Campaigns"
    ws.append(header)

    def row(**k):
        ws.append([k.get(h, "") for h in header])

    row(Entity="Ad Group", **{"Campaign ID": "C1", "Ad Group ID": "A1",
                              "Campaign Name (Informational only)": "Brand",
                              "Ad Group Name (Informational only)": "AG"})
    row(Entity="Keyword", **{"Campaign ID": "C1", "Ad Group ID": "A1", "Keyword ID": "K1",
                             "Campaign Name (Informational only)": "Brand",
                             "Ad Group Name (Informational only)": "AG",
                             "Keyword Text": "widget blue", "Match Type": "exact", "Bid": 1.20,
                             "Impressions": 1000, "Clicks": 40, "Spend": 40, "Sales": 20, "Orders": 2, "Units": 2})
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


_SEARCH_TERM_CSV = (
    b"Campaign Name,Ad Group Name,Customer Search Term,Match Type,Impressions,Clicks,Spend,"
    b"7 Day Total Sales,7 Day Total Orders (#),7 Day Total Units (#)\n"
    b"Brand,AG,cheap junk,exact,500,25,50,0,0,0\n"
)
_BUSINESS_CSV = (
    b"(Child) ASIN,Title,Sessions - Total,Units Ordered,Ordered Product Sales\n"
    b"B001,Widget,900,22,$320.00\n"
)


class _Base(unittest.TestCase):
    def setUp(self):
        self.engine = _make_engine()
        _bootstrap_db(self.engine)
        self.old_engine = _patch_global_engine(self.engine)
        self.tmpdir = tempfile.mkdtemp()
        import sales_support_agent.services.advertising.storage as storage
        self.storage = storage
        self._old_bulk_dir = storage.BULK_RUNS_DIR
        storage.BULK_RUNS_DIR = self.tmpdir

    def tearDown(self):
        self.storage.BULK_RUNS_DIR = self._old_bulk_dir
        _patch_global_engine(self.old_engine)
        self.engine.dispose()


class StorageTest(_Base):
    def test_goals_roundtrip(self):
        from sales_support_agent.services.advertising.schema import Goals
        self.storage.save_goals(Goals(revenue_target_cents=100000, acos_target_bps=3000, period="weekly"))
        g = self.storage.get_active_goals()
        self.assertIsNotNone(g)
        self.assertEqual(g.revenue_target_cents, 100000)
        self.assertEqual(g.acos_target_bps, 3000)
        self.assertEqual(g.period, "weekly")

    def test_goals_upsert_deactivates_prior(self):
        from sales_support_agent.services.advertising.schema import Goals
        self.storage.save_goals(Goals(revenue_target_cents=1))
        self.storage.save_goals(Goals(revenue_target_cents=2))
        g = self.storage.get_active_goals()
        self.assertEqual(g.revenue_target_cents, 2)

    def test_external_costs_roundtrip(self):
        from sales_support_agent.services.advertising.schema import ExternalCostRow
        rid = self.storage.create_run(label="r")
        self.storage.save_external_costs(
            [ExternalCostRow(channel="meta", amount_cents=10000)], run_id=rid)
        rows = self.storage.get_external_costs(rid)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].amount_cents, 10000)

    def test_bulk_file_roundtrip(self):
        rid = self.storage.create_run(label="r")
        self.storage.save_bulk_file(rid, "combined", b"xlsxdata")
        self.assertEqual(self.storage.get_bulk_file(rid, "combined"), b"xlsxdata")
        self.assertIn("combined", self.storage.list_bulk_files(rid))
        self.assertIsNone(self.storage.get_bulk_file(rid, "missing"))

    def test_bulk_file_survives_disk_wipe(self):
        # Render wipes the filesystem on every deploy/restart; the durable DB copy
        # must keep History's downloads alive. Simulate the wipe.
        import os, shutil
        rid = self.storage.create_run(label="r")
        self.storage.save_bulk_file(rid, "combined", b"APPLY")
        self.storage.save_bulk_file(rid, "growth_plan", b"PLAN")
        shutil.rmtree(os.path.join(self.storage.BULK_RUNS_DIR, rid))  # disk gone
        self.assertEqual(self.storage.list_bulk_files(rid), ["combined", "growth_plan"])
        self.assertEqual(self.storage.get_bulk_file(rid, "combined"), b"APPLY")
        self.assertEqual(self.storage.get_bulk_file(rid, "growth_plan"), b"PLAN")


class RunAuditTest(_Base):
    def test_full_run_persists_everything(self):
        from sales_support_agent.services.advertising.audit import AuditInputs, run_audit
        from sales_support_agent.services.advertising.schema import ExternalCostRow, Goals
        self.storage.save_goals(Goals(revenue_target_cents=100000, acos_target_bps=3000))
        res = run_audit(
            AuditInputs(
                bulk_xlsx=_bulk_xlsx(),
                search_term_csv=_SEARCH_TERM_CSV,
                business_report_csv=_BUSINESS_CSV,
                external_costs_manual=[ExternalCostRow(channel="meta", amount_cents=10000)],
            ),
            label="Week 1",
        )
        self.assertEqual(res.status, "complete")
        self.assertGreaterEqual(res.counts["recommendations"], 2)
        self.assertEqual(res.counts["external"], 1)

        run = self.storage.get_run(res.run_id)
        self.assertEqual(run["status"], "complete")
        self.assertTrue(run["summary"])
        recs = self.storage.get_recommendations(res.run_id)
        self.assertTrue(recs)
        self.assertEqual(recs[0]["rank"], 1)
        # Apply sheets are split into a bids file and an additions file.
        files = self.storage.list_bulk_files(res.run_id)
        self.assertTrue("bids" in files or "additions" in files)
        self.assertGreaterEqual(res.bulk.applied, 1)

    def test_empty_inputs_still_creates_run(self):
        from sales_support_agent.services.advertising.audit import AuditInputs, run_audit
        res = run_audit(AuditInputs(business_report_csv=_BUSINESS_CSV))
        self.assertEqual(res.status, "complete")


class HttpTest(_Base):
    def _client(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from sales_support_agent.api import advertising_router as ar

        app = FastAPI()
        app.include_router(ar.router)
        app.dependency_overrides[ar._check_admin_access] = lambda: None
        return TestClient(app)

    def test_get_page_ok(self):
        client = self._client()
        resp = client.get("/admin/advertising/audit")
        self.assertEqual(resp.status_code, 200)
        self.assertIn("Burn", resp.text)
        self.assertIn("Run an audit", resp.text)
        self.assertIn("History", resp.text)
        # The burn-list table is NOT rendered inline anymore — only in the workbook.
        self.assertNotIn("prioritized optimizations", resp.text)
        # No confusing pre-upload 'Detected:' brand chips.
        self.assertNotIn("Detected:", resp.text)

    def test_save_goals_redirects(self):
        client = self._client()
        resp = client.post(
            "/admin/advertising/audit/goals",
            data={"revenue_target": "1000", "acos_target": "30", "period": "monthly"},
            follow_redirects=False,
        )
        self.assertEqual(resp.status_code, 303)
        g = self.storage.get_active_goals()
        self.assertEqual(g.revenue_target_cents, 100000)
        self.assertEqual(g.acos_target_bps, 3000)

    def test_run_via_http_and_download_bulk(self):
        client = self._client()
        resp = client.post(
            "/admin/advertising/audit/run",
            data={"label": "HTTP week", "ext_channel": "meta", "ext_amount": "100"},
            files={
                "bulk_xlsx": ("bulk.xlsx", _bulk_xlsx(),
                              "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                "search_term_csv": ("st.csv", _SEARCH_TERM_CSV, "text/csv"),
            },
            follow_redirects=False,
        )
        self.assertEqual(resp.status_code, 303)
        location = resp.headers["location"]
        self.assertIn("/admin/advertising/audit?run=", location)
        run_id = location.split("run=")[1].split("&")[0]

        # Page now shows the run + a bulk download link + the growth plan.
        page = client.get(f"/admin/advertising/audit?run={run_id}")
        self.assertEqual(page.status_code, 200)
        kind = "bids" if "bids" in self.storage.list_bulk_files(run_id) else "additions"
        self.assertIn(f"/admin/advertising/audit/{run_id}/bulk/{kind}.xlsx", page.text)
        self.assertIn(f"/admin/advertising/audit/{run_id}/plan.xlsx", page.text)

        dl = client.get(f"/admin/advertising/audit/{run_id}/bulk/{kind}.xlsx")
        self.assertEqual(dl.status_code, 200)
        self.assertEqual(
            dl.headers["content-type"],
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        self.assertTrue(dl.content)

    def test_run_form_saves_and_applies_goals(self):
        client = self._client()
        resp = client.post(
            "/admin/advertising/audit/run",
            data={"label": "Goals-in-run", "revenue_target": "450000", "acos_target": "30", "tacos_target": "18"},
            files={"business_report_csv": ("biz.csv", _BUSINESS_CSV, "text/csv")},
            follow_redirects=False,
        )
        self.assertEqual(resp.status_code, 303)
        # Goals submitted with the run are saved (no separate Save-goals step).
        g = self.storage.get_active_goals()
        self.assertEqual(g.revenue_target_cents, 45000000)
        self.assertEqual(g.acos_target_bps, 3000)
        # And applied to the run's goal snapshot.
        run_id = resp.headers["location"].split("run=")[1].split("&")[0]
        run = self.storage.get_run(run_id)
        self.assertEqual(run["goal_snapshot"].get("revenue_target_cents"), 45000000)

    def test_run_with_no_files_redirects_with_message(self):
        client = self._client()
        resp = client.post("/admin/advertising/audit/run", data={}, follow_redirects=False)
        self.assertEqual(resp.status_code, 303)
        self.assertIn("Upload+at+least+one", resp.headers["location"])

    def test_multiple_external_channels_recorded(self):
        client = self._client()
        resp = client.post(
            "/admin/advertising/audit/run",
            data={
                "label": "Ext multi",
                "ext_channel": ["meta", "tiktok", "influencer"],
                "ext_label": ["prospecting", "spark ads", "Jane Doe"],
                "ext_amount": ["100", "50", "25"],
            },
            files={"business_report_csv": ("biz.csv", _BUSINESS_CSV, "text/csv")},
            follow_redirects=False,
        )
        self.assertEqual(resp.status_code, 303)
        run_id = resp.headers["location"].split("run=")[1].split("&")[0]
        costs = self.storage.get_external_costs(run_id)
        self.assertEqual(len(costs), 3)
        by_channel = {c.channel: c for c in costs}
        self.assertEqual(by_channel["meta"].amount_cents, 10000)
        self.assertEqual(by_channel["influencer"].cost_type, "commission")
        self.assertEqual(by_channel["influencer"].label, "Jane Doe")

    def test_mass_upload_auto_detects_and_runs(self):
        client = self._client()
        # Two files under ONE `files` field — the tool must route each by content.
        resp = client.post(
            "/admin/advertising/audit/run",
            data={"label": "Mass"},
            files=[
                ("files", ("anything.xlsx", _bulk_xlsx(),
                           "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")),
                ("files", ("export.csv", _SEARCH_TERM_CSV, "text/csv")),
            ],
            follow_redirects=False,
        )
        self.assertEqual(resp.status_code, 303)
        loc = resp.headers["location"]
        self.assertIn("/admin/advertising/audit?run=", loc)
        self.assertIn("detail=", loc)  # detection report present
        run_id = loc.split("run=")[1].split("&")[0]
        # Bulk file was auto-detected, so a bulk sheet got generated.
        files = self.storage.list_bulk_files(run_id)
        self.assertTrue("bids" in files or "additions" in files)


if __name__ == "__main__":
    unittest.main()
