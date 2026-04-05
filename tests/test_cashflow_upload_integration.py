"""Integration tests for cashflow CSV upload using an in-memory SQLite DB.

Tests:
- run_csv_upload() with bank CSV bytes
- run_csv_upload() with QBO Open Invoices bytes
- Duplicate detection (same source_id not inserted twice)
- replace_range merge mode
- detect_csv_format routing
- insert_cash_event canonical helper
- get_events_for_range date-bounded query
- list_obligations filters
"""

from __future__ import annotations

import unittest
import uuid
from datetime import date, datetime

from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool
from sqlalchemy.orm import sessionmaker


# ---------------------------------------------------------------------------
# DB fixture helpers
# ---------------------------------------------------------------------------

def _make_engine():
    """Create a fresh in-memory SQLite engine with StaticPool so all
    connections share the same in-memory database."""
    return create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )


def _bootstrap_db(engine):
    """Bootstrap the cashflow schema on the given engine, bypassing
    the global module-level engine used by production code.

    We create the tables via Base.metadata and then run the SQLite
    compat migrations.
    """
    from sales_support_agent.models.database import (
        Base,
        _apply_sqlite_compat_migrations,
        _register_models,
    )
    _register_models()
    Base.metadata.create_all(bind=engine)
    _apply_sqlite_compat_migrations(engine)


def _patch_global_engine(engine):
    """Point the module-level engine reference at our test engine.

    Returns the old engine so the caller can restore it.
    """
    import sales_support_agent.models.database as db_module
    old = db_module.engine
    db_module.engine = engine
    return old


def _restore_global_engine(old_engine):
    import sales_support_agent.models.database as db_module
    db_module.engine = old_engine


# ---------------------------------------------------------------------------
# CSV fixtures
# ---------------------------------------------------------------------------

BANK_CSV_BYTES = b"""Transaction ID,Posting Date,Effective Date,Transaction Type,Amount,Balance,Description,Transaction Category,Type,Reference Number,Extended Description
TXN-001,03/15/2026,03/15/2026,Debit,-1500.00,23450.00,FORAFINANCIAL PAYMT,Loan Payments,ACH,REF001,
TXN-002,03/16/2026,03/16/2026,Credit,2500.00,25950.00,INTUIT DEPOSIT SQ,Income,ACH,REF002,
TXN-003,03/17/2026,03/17/2026,Debit,-200.00,25750.00,AMAZON.COM PURCHASE,Shopping,Card,REF003,
"""

BANK_CSV_DUPLICATE = b"""Transaction ID,Posting Date,Effective Date,Transaction Type,Amount,Balance,Description,Transaction Category,Type,Reference Number,Extended Description
TXN-001,03/15/2026,03/15/2026,Debit,-1500.00,23450.00,FORAFINANCIAL PAYMT,Loan Payments,ACH,REF001,
"""

QBO_CSV_BYTES = b"""Open Invoices Report,,,,,,
anata LLC,,,,,,
"As of Apr 4, 2026",,,,,,

,Date,Transaction type,Num,Term,Due date,Open balance
Acme Corp,,,,,,
,01/15/2026,Invoice,INV-001,Net 15,01/30/2026,"1,500.00"
Total for Acme Corp,,,,,,\"$1,500.00\"
Beta LLC,,,,,,
,02/01/2026,Invoice,INV-002,Net 30,03/03/2026,"750.00"
Total for Beta LLC,,,,,,\"$750.00\"
PaymentCo,,,,,,
,03/04/2026,Payment,,,03/04/2026,"-200.00"
Total for PaymentCo,,,,,,-$200.00
,TOTAL,,,,,"$2,050.00"
"""


# ---------------------------------------------------------------------------
# Base test class
# ---------------------------------------------------------------------------

class _CashflowIntegrationBase(unittest.TestCase):
    """Creates an isolated in-memory DB for each test, patches the global engine."""

    def setUp(self):
        self.engine = _make_engine()
        _bootstrap_db(self.engine)
        self.old_engine = _patch_global_engine(self.engine)

    def tearDown(self):
        _restore_global_engine(self.old_engine)
        self.engine.dispose()

    def _row_count(self, table="cash_events"):
        with self.engine.connect() as conn:
            row = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).fetchone()
        return row[0]

    def _fetch_all_events(self):
        with self.engine.connect() as conn:
            rows = conn.execute(text("SELECT * FROM cash_events ORDER BY due_date")).fetchall()
        return rows


# ---------------------------------------------------------------------------
# Tests: run_csv_upload — bank CSV
# ---------------------------------------------------------------------------

class TestBankCSVUpload(_CashflowIntegrationBase):

    def test_inserts_all_rows(self):
        from sales_support_agent.services.cashflow.upload import run_csv_upload
        result = run_csv_upload(BANK_CSV_BYTES)
        self.assertEqual(result.rows_inserted, 3)
        self.assertEqual(self._row_count(), 3)

    def test_rows_read_count(self):
        from sales_support_agent.services.cashflow.upload import run_csv_upload
        result = run_csv_upload(BANK_CSV_BYTES)
        self.assertEqual(result.rows_read, 3)

    def test_source_field_is_csv(self):
        from sales_support_agent.services.cashflow.upload import run_csv_upload
        run_csv_upload(BANK_CSV_BYTES)
        with self.engine.connect() as conn:
            sources = [r[0] for r in conn.execute(text("SELECT DISTINCT source FROM cash_events")).fetchall()]
        self.assertEqual(sources, ["csv"])

    def test_duplicate_skipped_on_second_upload(self):
        from sales_support_agent.services.cashflow.upload import run_csv_upload
        run_csv_upload(BANK_CSV_BYTES)
        result2 = run_csv_upload(BANK_CSV_DUPLICATE)
        self.assertEqual(result2.rows_inserted, 0)
        self.assertEqual(result2.rows_skipped_duplicate, 1)
        # Total count should still be 3
        self.assertEqual(self._row_count(), 3)

    def test_latest_balance_captured(self):
        from sales_support_agent.services.cashflow.upload import run_csv_upload
        result = run_csv_upload(BANK_CSV_BYTES)
        # Last row has balance 25750.00 → 2575000 cents
        self.assertEqual(result.latest_balance_cents, 2575000)

    def test_success_flag_true(self):
        from sales_support_agent.services.cashflow.upload import run_csv_upload
        result = run_csv_upload(BANK_CSV_BYTES)
        self.assertTrue(result.success)

    def test_replace_range_removes_existing(self):
        from sales_support_agent.services.cashflow.upload import run_csv_upload
        # First upload
        run_csv_upload(BANK_CSV_BYTES)
        self.assertEqual(self._row_count(), 3)
        # Second upload with replace_range should clear and re-insert
        result2 = run_csv_upload(BANK_CSV_BYTES, merge_mode="replace_range")
        self.assertEqual(self._row_count(), 3)
        self.assertEqual(result2.rows_inserted, 3)

    def test_upload_result_summary_is_string(self):
        from sales_support_agent.services.cashflow.upload import run_csv_upload
        result = run_csv_upload(BANK_CSV_BYTES)
        summary = result.summary()
        self.assertIsInstance(summary, str)
        self.assertIn("inserted", summary)

    def test_empty_csv_returns_zero_rows(self):
        from sales_support_agent.services.cashflow.upload import run_csv_upload
        header_only = b"Transaction ID,Posting Date,Amount,Balance,Description,Transaction Category,Type\n"
        result = run_csv_upload(header_only)
        self.assertEqual(result.rows_read, 0)
        self.assertEqual(result.rows_inserted, 0)


# ---------------------------------------------------------------------------
# Tests: run_csv_upload — QBO Open Invoices
# ---------------------------------------------------------------------------

class TestQBOUpload(_CashflowIntegrationBase):

    def test_qbo_detected_and_inserted(self):
        from sales_support_agent.services.cashflow.upload import run_csv_upload
        result = run_csv_upload(QBO_CSV_BYTES)
        # 2 invoices (Payment row is skipped, negative balance row is skipped)
        self.assertGreater(result.rows_inserted, 0)

    def test_qbo_rows_have_source_qbo_csv(self):
        from sales_support_agent.services.cashflow.upload import run_csv_upload
        run_csv_upload(QBO_CSV_BYTES)
        with self.engine.connect() as conn:
            sources = {r[0] for r in conn.execute(text("SELECT DISTINCT source FROM cash_events")).fetchall()}
        self.assertIn("qbo-csv", sources)

    def test_qbo_event_type_is_inflow(self):
        from sales_support_agent.services.cashflow.upload import run_csv_upload
        run_csv_upload(QBO_CSV_BYTES)
        with self.engine.connect() as conn:
            types = {r[0] for r in conn.execute(
                text("SELECT DISTINCT event_type FROM cash_events WHERE source='qbo-csv'")
            ).fetchall()}
        self.assertEqual(types, {"inflow"})

    def test_qbo_duplicate_not_inserted_twice(self):
        from sales_support_agent.services.cashflow.upload import run_csv_upload
        result1 = run_csv_upload(QBO_CSV_BYTES)
        inserted_first = result1.rows_inserted
        result2 = run_csv_upload(QBO_CSV_BYTES)
        self.assertEqual(result2.rows_inserted, 0)
        self.assertEqual(self._row_count(), inserted_first)


# ---------------------------------------------------------------------------
# Tests: insert_cash_event canonical helper
# ---------------------------------------------------------------------------

class TestInsertCashEvent(_CashflowIntegrationBase):

    def _insert_one(self, **overrides):
        from sales_support_agent.models.database import insert_cash_event
        now = datetime.utcnow().isoformat()
        kwargs = dict(
            id=str(uuid.uuid4()),
            source="manual",
            source_id="test-src-001",
            event_type="outflow",
            category="other",
            amount_cents=100_00,
            due_date=date(2026, 4, 7),
            status="planned",
            confidence="estimated",
            created_at=now,
            updated_at=now,
        )
        kwargs.update(overrides)
        with self.engine.begin() as conn:
            insert_cash_event(conn, **kwargs)

    def test_insert_creates_row(self):
        self._insert_one()
        self.assertEqual(self._row_count(), 1)

    def test_insert_values_persisted(self):
        event_id = str(uuid.uuid4())
        self._insert_one(id=event_id, amount_cents=55_00, event_type="inflow")
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT amount_cents, event_type FROM cash_events WHERE id=:id"),
                {"id": event_id}
            ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row[0], 5500)
        self.assertEqual(row[1], "inflow")

    def test_insert_with_friendly_name(self):
        event_id = str(uuid.uuid4())
        self._insert_one(id=event_id, friendly_name="My Label")
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT friendly_name FROM cash_events WHERE id=:id"),
                {"id": event_id}
            ).fetchone()
        self.assertEqual(row[0], "My Label")

    def test_insert_date_as_date_object(self):
        event_id = str(uuid.uuid4())
        self._insert_one(id=event_id, due_date=date(2026, 5, 15))
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT due_date FROM cash_events WHERE id=:id"),
                {"id": event_id}
            ).fetchone()
        self.assertIn("2026-05-15", str(row[0]))


# ---------------------------------------------------------------------------
# Tests: get_events_for_range
# ---------------------------------------------------------------------------

class TestGetEventsForRange(_CashflowIntegrationBase):

    def setUp(self):
        super().setUp()
        # Seed three events across different dates
        from sales_support_agent.models.database import insert_cash_event
        now = datetime.utcnow().isoformat()

        def _seed(event_id, due, amount, event_type="outflow", status="planned"):
            with self.engine.begin() as conn:
                insert_cash_event(
                    conn,
                    id=event_id, source="manual", source_id=event_id,
                    event_type=event_type, category="other",
                    amount_cents=amount, due_date=due, status=status,
                    confidence="estimated", created_at=now, updated_at=now,
                )

        _seed("e-jan", date(2026, 1, 10), 100_00)
        _seed("e-mar", date(2026, 3, 15), 200_00, event_type="inflow")
        _seed("e-may", date(2026, 5, 20), 300_00)
        _seed("e-cancelled", date(2026, 3, 10), 50_00, status="cancelled")

    def test_returns_only_events_in_range(self):
        from sales_support_agent.services.cashflow.obligations import get_events_for_range
        events = get_events_for_range(date(2026, 3, 1), date(2026, 3, 31))
        ids = [e["id"] for e in events]
        self.assertIn("e-mar", ids)
        self.assertNotIn("e-jan", ids)
        self.assertNotIn("e-may", ids)

    def test_excludes_cancelled_by_default(self):
        from sales_support_agent.services.cashflow.obligations import get_events_for_range
        events = get_events_for_range(date(2026, 3, 1), date(2026, 3, 31))
        ids = [e["id"] for e in events]
        self.assertNotIn("e-cancelled", ids)

    def test_filter_by_event_type(self):
        from sales_support_agent.services.cashflow.obligations import get_events_for_range
        events = get_events_for_range(
            date(2026, 1, 1), date(2026, 12, 31),
            event_type="inflow"
        )
        for e in events:
            self.assertEqual(e["event_type"], "inflow")

    def test_returns_dict_with_expected_keys(self):
        from sales_support_agent.services.cashflow.obligations import get_events_for_range
        events = get_events_for_range(date(2026, 1, 1), date(2026, 12, 31))
        self.assertGreater(len(events), 0)
        required_keys = {"id", "source", "event_type", "amount_cents", "due_date", "status"}
        for key in required_keys:
            self.assertIn(key, events[0])

    def test_wide_range_returns_all_non_cancelled(self):
        from sales_support_agent.services.cashflow.obligations import get_events_for_range
        events = get_events_for_range(date(2025, 1, 1), date(2027, 12, 31))
        ids = {e["id"] for e in events}
        self.assertIn("e-jan", ids)
        self.assertIn("e-mar", ids)
        self.assertIn("e-may", ids)
        self.assertNotIn("e-cancelled", ids)


# ---------------------------------------------------------------------------
# Tests: list_obligations
# ---------------------------------------------------------------------------

class TestListObligations(_CashflowIntegrationBase):

    def setUp(self):
        super().setUp()
        from sales_support_agent.models.database import insert_cash_event
        now = datetime.utcnow().isoformat()

        def _seed(event_id, due, amount, event_type="outflow", status="planned", source="manual"):
            with self.engine.begin() as conn:
                insert_cash_event(
                    conn,
                    id=event_id, source=source, source_id=event_id,
                    event_type=event_type, category="other",
                    amount_cents=amount, due_date=due, status=status,
                    confidence="estimated", created_at=now, updated_at=now,
                )

        _seed("o1", date(2026, 4, 1), 100_00, status="planned")
        _seed("o2", date(2026, 4, 5), 200_00, status="posted", source="csv")
        _seed("o3", date(2026, 4, 10), 300_00, status="cancelled")

    def test_returns_list_of_dicts(self):
        from sales_support_agent.services.cashflow.obligations import list_obligations
        rows = list_obligations(limit=100)
        self.assertIsInstance(rows, list)
        if rows:
            self.assertIsInstance(rows[0], dict)

    def test_all_statuses_by_default(self):
        from sales_support_agent.services.cashflow.obligations import list_obligations
        rows = list_obligations(limit=100)
        ids = {r["id"] for r in rows}
        # All 3 rows should be present (list_obligations returns all statuses)
        self.assertIn("o1", ids)
        self.assertIn("o2", ids)
        self.assertIn("o3", ids)

    def test_filter_by_status(self):
        from sales_support_agent.services.cashflow.obligations import list_obligations
        rows = list_obligations(limit=100, status="posted")
        ids = {r["id"] for r in rows}
        self.assertIn("o2", ids)
        self.assertNotIn("o1", ids)
        self.assertNotIn("o3", ids)

    def test_limit_respected(self):
        from sales_support_agent.services.cashflow.obligations import list_obligations
        rows = list_obligations(limit=1)
        self.assertLessEqual(len(rows), 1)


if __name__ == "__main__":
    unittest.main()
