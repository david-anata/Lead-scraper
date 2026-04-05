"""
Self-testing infrastructure for the cashflow module.

Tests:
  A. Health endpoint — HTTP 200, valid JSON, no auth required
  B. Health endpoint with in-memory DB — status "ok", no missing columns
  C. Static INSERT SQL coverage — upload.py, clickup_sync.py, obligations.py
  D. Module import smoke tests — all cashflow modules import cleanly

Run with:
    python3 -m pytest tests/test_cashflow_health.py -v

No external DB, network, or env vars required.
"""

from __future__ import annotations

import importlib
import inspect
import unittest
from unittest.mock import patch

# ---------------------------------------------------------------------------
# Canonical NOT NULL column set for cash_events
# ---------------------------------------------------------------------------
REQUIRED_COLUMNS: frozenset = frozenset({
    "id", "source", "source_id", "event_type", "category",
    "subcategory", "description", "name", "vendor_or_customer",
    "amount_cents", "status", "confidence",
    "recurring_rule", "clickup_task_id",
    "bank_transaction_type", "bank_reference", "notes",
    "created_at", "updated_at",
})


def _src(module_path: str) -> str:
    return inspect.getsource(importlib.import_module(module_path))


# ===========================================================================
# A.  Health endpoint — no DB (graceful error path)
# ===========================================================================

class TestHealthEndpointNoDB(unittest.TestCase):
    """Health route must return 200 JSON even when no DB engine is wired up."""

    def setUp(self) -> None:
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from sales_support_agent.api.cashflow_router import router

        app = FastAPI()
        app.include_router(router)
        self.client = TestClient(app, raise_server_exceptions=False)

    def test_returns_200(self) -> None:
        resp = self.client.get("/admin/finances/health")
        self.assertEqual(resp.status_code, 200)

    def test_response_is_json(self) -> None:
        resp = self.client.get("/admin/finances/health")
        data = resp.json()
        self.assertIsInstance(data, dict)

    def test_has_status_key(self) -> None:
        resp = self.client.get("/admin/finances/health")
        self.assertIn("status", resp.json())

    def test_status_is_valid_value(self) -> None:
        resp = self.client.get("/admin/finances/health")
        self.assertIn(resp.json()["status"], {"ok", "degraded", "error"})

    def test_no_auth_cookie_required(self) -> None:
        """Must not redirect to login (303)."""
        resp = self.client.get("/admin/finances/health")
        self.assertNotEqual(resp.status_code, 303)


# ===========================================================================
# B.  Health endpoint — with full in-memory SQLite DB
# ===========================================================================

class TestHealthEndpointWithDB(unittest.TestCase):
    """
    Patch the DB engine with a fully-migrated SQLite in-memory instance.
    Health check should return status "ok" with no missing columns.
    """

    def setUp(self) -> None:
        from sqlalchemy import create_engine, text
        from sqlalchemy.pool import StaticPool
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from sales_support_agent.api.cashflow_router import router

        # StaticPool forces all connections to reuse the same in-memory DB
        engine = create_engine(
            "sqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE cash_events (
                    id                      TEXT    PRIMARY KEY,
                    source                  TEXT    NOT NULL DEFAULT 'manual',
                    source_id               TEXT    NOT NULL DEFAULT '',
                    event_type              TEXT    NOT NULL DEFAULT 'outflow',
                    category                TEXT    NOT NULL DEFAULT 'uncategorized',
                    subcategory             TEXT    NOT NULL DEFAULT '',
                    name                    TEXT    NOT NULL DEFAULT '',
                    description             TEXT    NOT NULL DEFAULT '',
                    vendor_or_customer      TEXT    NOT NULL DEFAULT '',
                    amount_cents            INTEGER NOT NULL DEFAULT 0,
                    due_date                TEXT,
                    effective_date          TEXT,
                    expected_date           TEXT,
                    status                  TEXT    NOT NULL DEFAULT 'planned',
                    confidence              TEXT    NOT NULL DEFAULT 'estimated',
                    recurring_template_id   TEXT,
                    recurring_rule          TEXT    NOT NULL DEFAULT '',
                    matched_to_id           TEXT,
                    clickup_task_id         TEXT    NOT NULL DEFAULT '',
                    account_balance_cents   INTEGER,
                    bank_transaction_type   TEXT    NOT NULL DEFAULT '',
                    bank_reference          TEXT    NOT NULL DEFAULT '',
                    notes                   TEXT    NOT NULL DEFAULT '',
                    created_at              TEXT    NOT NULL DEFAULT '',
                    updated_at              TEXT    NOT NULL DEFAULT ''
                )
            """))

        import sales_support_agent.models.database as _db_mod
        _db_mod.engine = engine
        self._db_mod = _db_mod
        self._original_engine = None  # already replaced above

        app = FastAPI()
        app.include_router(router)
        self.client = TestClient(app)

    def tearDown(self) -> None:
        self._db_mod.engine = None  # reset

    def test_returns_200(self) -> None:
        self.assertEqual(self.client.get("/admin/finances/health").status_code, 200)

    def test_status_is_ok(self) -> None:
        data = self.client.get("/admin/finances/health").json()
        self.assertEqual(data["status"], "ok", msg=str(data))

    def test_no_missing_columns(self) -> None:
        data = self.client.get("/admin/finances/health").json()
        self.assertEqual(data.get("missing_columns"), [])

    def test_table_exists_check_true(self) -> None:
        data = self.client.get("/admin/finances/health").json()
        self.assertTrue(data["checks"]["cash_events_table_exists"])

    def test_all_required_columns_present_true(self) -> None:
        data = self.client.get("/admin/finances/health").json()
        self.assertTrue(data["checks"]["all_required_columns_present"])

    def test_upload_insert_covered(self) -> None:
        data = self.client.get("/admin/finances/health").json()
        self.assertTrue(
            data["checks"]["upload_insert_coverage"]["covered"],
            msg=str(data["checks"]["upload_insert_coverage"]),
        )

    def test_clickup_sync_insert_covered(self) -> None:
        data = self.client.get("/admin/finances/health").json()
        self.assertTrue(
            data["checks"]["clickup_sync_insert_coverage"]["covered"],
            msg=str(data["checks"]["clickup_sync_insert_coverage"]),
        )

    def test_obligations_insert_covered(self) -> None:
        data = self.client.get("/admin/finances/health").json()
        self.assertTrue(
            data["checks"]["obligations_insert_coverage"]["covered"],
            msg=str(data["checks"]["obligations_insert_coverage"]),
        )


# ===========================================================================
# C.  Static INSERT SQL coverage (pure — no DB, no server)
# ===========================================================================

class TestUploadInsertCoverage(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.src = _src("sales_support_agent.services.cashflow.upload")

    def _col(self, col: str) -> None:
        self.assertIn(col, self.src, msg=f"upload.py INSERT missing: '{col}'")

    def test_subcategory(self):           self._col("subcategory")
    def test_description(self):           self._col("description")
    def test_bank_transaction_type(self): self._col("bank_transaction_type")
    def test_bank_reference(self):        self._col("bank_reference")
    def test_notes(self):                 self._col("notes")
    def test_recurring_rule(self):        self._col("recurring_rule")
    def test_clickup_task_id(self):       self._col("clickup_task_id")
    def test_vendor_or_customer(self):    self._col("vendor_or_customer")
    def test_amount_cents(self):          self._col("amount_cents")
    def test_created_at(self):            self._col("created_at")
    def test_updated_at(self):            self._col("updated_at")

    def test_all_required_columns(self) -> None:
        missing = sorted(c for c in REQUIRED_COLUMNS if c not in self.src)
        self.assertEqual(missing, [], msg=f"upload.py missing columns: {missing}")


class TestClickupSyncInsertCoverage(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.src = _src("sales_support_agent.services.cashflow.clickup_sync")

    def _col(self, col: str) -> None:
        self.assertIn(col, self.src, msg=f"clickup_sync.py INSERT missing: '{col}'")

    def test_subcategory(self):           self._col("subcategory")
    def test_description(self):           self._col("description")
    def test_bank_transaction_type(self): self._col("bank_transaction_type")
    def test_bank_reference(self):        self._col("bank_reference")
    def test_notes(self):                 self._col("notes")
    def test_recurring_rule(self):        self._col("recurring_rule")
    def test_clickup_task_id(self):       self._col("clickup_task_id")

    def test_all_required_columns(self) -> None:
        missing = sorted(c for c in REQUIRED_COLUMNS if c not in self.src)
        self.assertEqual(missing, [], msg=f"clickup_sync.py missing columns: {missing}")


class TestObligationsInsertCoverage(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.src = _src("sales_support_agent.services.cashflow.obligations")

    def _col(self, col: str) -> None:
        self.assertIn(col, self.src, msg=f"obligations.py INSERT missing: '{col}'")

    def test_subcategory(self):           self._col("subcategory")
    def test_description(self):           self._col("description")
    def test_bank_transaction_type(self): self._col("bank_transaction_type")
    def test_bank_reference(self):        self._col("bank_reference")
    def test_notes(self):                 self._col("notes")
    def test_recurring_rule(self):        self._col("recurring_rule")
    def test_clickup_task_id(self):       self._col("clickup_task_id")

    def test_all_required_columns(self) -> None:
        missing = sorted(c for c in REQUIRED_COLUMNS if c not in self.src)
        self.assertEqual(missing, [], msg=f"obligations.py missing columns: {missing}")


# ===========================================================================
# D.  Module import smoke tests
# ===========================================================================

class TestCashflowModuleImports(unittest.TestCase):
    """Every cashflow module must import without raising an exception."""

    MODULES = [
        "sales_support_agent.services.cashflow.upload",
        "sales_support_agent.services.cashflow.upload_page",
        "sales_support_agent.services.cashflow.clickup_sync",
        "sales_support_agent.services.cashflow.obligations",
        "sales_support_agent.services.cashflow.matcher",
        "sales_support_agent.services.cashflow.normalizers",
        "sales_support_agent.services.cashflow.categorizer",
        "sales_support_agent.services.cashflow.forecast",
        "sales_support_agent.services.cashflow.alerts",
        "sales_support_agent.services.cashflow.overview",
        "sales_support_agent.services.cashflow.scenario",
        "sales_support_agent.services.cashflow.engine",
        "sales_support_agent.api.cashflow_router",
        "sales_support_agent.services.auth_deps",
        "sales_support_agent.services.cashflow.qbo_sync",
    ]

    def _importable(self, path: str) -> None:
        try:
            importlib.import_module(path)
        except Exception as exc:
            self.fail(f"Import failed for {path}: {exc}")

    def test_upload(self):           self._importable("sales_support_agent.services.cashflow.upload")
    def test_upload_page(self):      self._importable("sales_support_agent.services.cashflow.upload_page")
    def test_clickup_sync(self):     self._importable("sales_support_agent.services.cashflow.clickup_sync")
    def test_obligations(self):      self._importable("sales_support_agent.services.cashflow.obligations")
    def test_matcher(self):          self._importable("sales_support_agent.services.cashflow.matcher")
    def test_normalizers(self):      self._importable("sales_support_agent.services.cashflow.normalizers")
    def test_categorizer(self):      self._importable("sales_support_agent.services.cashflow.categorizer")
    def test_forecast(self):         self._importable("sales_support_agent.services.cashflow.forecast")
    def test_alerts(self):           self._importable("sales_support_agent.services.cashflow.alerts")
    def test_overview(self):         self._importable("sales_support_agent.services.cashflow.overview")
    def test_scenario(self):         self._importable("sales_support_agent.services.cashflow.scenario")
    def test_engine(self):           self._importable("sales_support_agent.services.cashflow.engine")
    def test_cashflow_router(self):  self._importable("sales_support_agent.api.cashflow_router")
    def test_auth_deps(self):        self._importable("sales_support_agent.services.auth_deps")
    def test_qbo_sync(self):         self._importable("sales_support_agent.services.cashflow.qbo_sync")


if __name__ == "__main__":
    unittest.main()
