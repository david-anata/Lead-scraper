"""Tests for run_csv_upload using in-memory SQLite."""

from __future__ import annotations

import io
import textwrap
import unittest


def _make_csv(rows: list[dict]) -> bytes:
    """Build a minimal CSV bytes object from a list of row dicts."""
    headers = [
        "Transaction ID", "Date", "Description", "Transaction Subtype",
        "Amount", "Debit", "Credit", "Account Balance", "Category",
        "Note", "Name", "Bank Reference", "Transaction Category",
    ]
    lines = [",".join(headers)]
    for r in rows:
        line = ",".join(str(r.get(h, "")) for h in headers)
        lines.append(line)
    return "\n".join(lines).encode()


def _sample_rows() -> list[dict]:
    return [
        {
            "Transaction ID": "TXN-001",
            "Date": "3/1/2026",
            "Description": "FORAFINANCIAL PAYMT",
            "Transaction Subtype": "ACH",
            "Amount": "-1500.00",
            "Debit": "1500.00",
            "Credit": "",
            "Account Balance": "23000.00",
            "Category": "Other",
            "Note": "", "Name": "", "Bank Reference": "REF1", "Transaction Category": "Loan Payments",
        },
        {
            "Transaction ID": "TXN-002",
            "Date": "3/5/2026",
            "Description": "INTUIT DEPOSIT SQ",
            "Transaction Subtype": "ACH",
            "Amount": "5000.00",
            "Debit": "",
            "Credit": "5000.00",
            "Account Balance": "28000.00",
            "Category": "Other",
            "Note": "", "Name": "", "Bank Reference": "REF2", "Transaction Category": "Income",
        },
    ]


class TestRunCsvUpload(unittest.TestCase):
    def setUp(self) -> None:
        """Patch the engine with an in-memory SQLite DB before each test."""
        from sqlalchemy import create_engine, text
        from unittest.mock import patch

        self._engine = create_engine("sqlite:///:memory:")
        with self._engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS cash_events (
                    id TEXT PRIMARY KEY,
                    source TEXT,
                    source_id TEXT,
                    event_type TEXT,
                    category TEXT,
                    name TEXT,
                    vendor_or_customer TEXT,
                    amount_cents INTEGER,
                    due_date TEXT,
                    status TEXT,
                    confidence TEXT,
                    notes TEXT,
                    account_balance_cents INTEGER,
                    matched_to_id TEXT,
                    recurring_template_id TEXT,
                    clickup_task_id TEXT,
                    recurring_rule TEXT,
                    created_at TEXT,
                    updated_at TEXT
                )
            """))

        # Patch the engine import inside the upload module
        self._patcher = patch(
            "sales_support_agent.services.cashflow.upload.engine",
            self._engine,
        )
        self._patcher.start()
        # Also patch obligations.engine so list_obligations works
        self._patcher2 = patch(
            "sales_support_agent.services.cashflow.obligations.engine",
            self._engine,
        )
        self._patcher2.start()

    def tearDown(self) -> None:
        self._patcher.stop()
        self._patcher2.stop()

    def test_basic_upload_inserts_rows(self) -> None:
        from sales_support_agent.services.cashflow.upload import run_csv_upload
        csv_bytes = _make_csv(_sample_rows())
        result = run_csv_upload(csv_bytes)
        self.assertEqual(result.rows_read, 2)
        self.assertEqual(result.rows_inserted, 2)
        self.assertEqual(result.rows_skipped_duplicate, 0)

    def test_duplicate_skipped_on_second_upload(self) -> None:
        from sales_support_agent.services.cashflow.upload import run_csv_upload
        csv_bytes = _make_csv(_sample_rows())
        run_csv_upload(csv_bytes)
        result2 = run_csv_upload(csv_bytes)
        self.assertEqual(result2.rows_inserted, 0)
        self.assertEqual(result2.rows_skipped_duplicate, 2)

    def test_latest_balance_captured(self) -> None:
        from sales_support_agent.services.cashflow.upload import run_csv_upload
        csv_bytes = _make_csv(_sample_rows())
        result = run_csv_upload(csv_bytes)
        # Last row has balance 28000.00 = 2800000 cents
        self.assertEqual(result.latest_balance_cents, 2800000)

    def test_success_flag(self) -> None:
        from sales_support_agent.services.cashflow.upload import run_csv_upload
        csv_bytes = _make_csv(_sample_rows())
        result = run_csv_upload(csv_bytes)
        self.assertTrue(result.success)

    def test_empty_csv_returns_zero_rows(self) -> None:
        from sales_support_agent.services.cashflow.upload import run_csv_upload
        csv_bytes = _make_csv([])
        result = run_csv_upload(csv_bytes)
        self.assertEqual(result.rows_read, 0)
        self.assertEqual(result.rows_inserted, 0)

    def test_summary_string_is_not_empty(self) -> None:
        from sales_support_agent.services.cashflow.upload import run_csv_upload
        csv_bytes = _make_csv(_sample_rows())
        result = run_csv_upload(csv_bytes)
        self.assertIn("rows read", result.summary())


if __name__ == "__main__":
    unittest.main()
