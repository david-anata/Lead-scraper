"""Tests for cashflow normalizers — bank CSV and ClickUp task normalization."""

from __future__ import annotations

import unittest
from datetime import date, datetime

from sales_support_agent.services.cashflow.normalizers import (
    normalize_bank_csv_row,
    normalize_clickup_task,
)


class TestNormalizeBankCsvRow(unittest.TestCase):
    """Tests using the actual 13-column CSV format."""

    def _make_row(self, **overrides) -> dict:
        row = {
            "Transaction ID": "TXN-001",
            "Date": "3/15/2026",
            "Description": "Withdrawal ACH FORAFINANCIAL PAYMT Entry Class Code: WEB",
            "Transaction Subtype": "ACH",
            "Amount": "-1500.00",
            "Debit": "1500.00",
            "Credit": "",
            "Account Balance": "23450.00",
            "Category": "Other",
            "Note": "",
            "Name": "",
            "Bank Reference": "REF001",
            "Transaction Category": "Loan Payments",
        }
        row.update(overrides)
        return row

    def test_source_and_status(self) -> None:
        result = normalize_bank_csv_row(self._make_row())
        self.assertEqual(result["source"], "csv")
        self.assertEqual(result["status"], "posted")
        self.assertEqual(result["confidence"], "confirmed")

    def test_source_id_from_transaction_id(self) -> None:
        result = normalize_bank_csv_row(self._make_row(**{"Transaction ID": "TXN-XYZ"}))
        self.assertEqual(result["source_id"], "TXN-XYZ")

    def test_amount_cents_is_int(self) -> None:
        result = normalize_bank_csv_row(self._make_row(**{"Amount": "-1500.00"}))
        self.assertIsInstance(result["amount_cents"], int)
        self.assertEqual(result["amount_cents"], 150000)

    def test_debit_is_outflow(self) -> None:
        result = normalize_bank_csv_row(self._make_row(**{"Debit": "500.00", "Credit": "", "Amount": "-500.00"}))
        self.assertEqual(result["event_type"], "outflow")

    def test_credit_is_inflow(self) -> None:
        result = normalize_bank_csv_row(self._make_row(**{
            "Debit": "", "Credit": "2500.00", "Amount": "2500.00",
            "Description": "INTUIT DEPOSIT SQ",
        }))
        self.assertEqual(result["event_type"], "inflow")

    def test_date_parsed_correctly(self) -> None:
        result = normalize_bank_csv_row(self._make_row(**{"Date": "3/15/2026"}))
        due = result["due_date"]
        self.assertEqual(due, date(2026, 3, 15))

    def test_balance_column_parsed(self) -> None:
        result = normalize_bank_csv_row(self._make_row(**{"Account Balance": "23450.00"}))
        self.assertEqual(result["account_balance_cents"], 2345000)

    def test_category_assigned(self) -> None:
        result = normalize_bank_csv_row(self._make_row(**{"Description": "FORAFINANCIAL PAYMT"}))
        self.assertEqual(result["category"], "debt")

    def test_ach_prefix_cleaned_from_name(self) -> None:
        result = normalize_bank_csv_row(self._make_row(**{
            "Description": "Withdrawal ACH FORAFINANCIAL PAYMT Entry Class Code: WEB"
        }))
        self.assertNotIn("Withdrawal ACH", result["name"])
        self.assertNotIn("Entry Class Code", result["name"])

    def test_dollar_amount_stripped(self) -> None:
        result = normalize_bank_csv_row(self._make_row(**{"Amount": "$1,234.56", "Debit": "1234.56", "Credit": ""}))
        self.assertEqual(result["amount_cents"], 123456)

    def test_missing_balance_is_none(self) -> None:
        result = normalize_bank_csv_row(self._make_row(**{"Account Balance": ""}))
        self.assertIsNone(result["account_balance_cents"])


class TestNormalizeClickupTask(unittest.TestCase):
    def _make_task(self, **overrides) -> dict:
        task = {
            "id": "CU-001",
            "name": "Pay Vendor Invoice",
            "status": {"status": "open"},
            "due_date": "1744070400000",  # Unix ms
            "custom_fields": [
                {"name": "Amount", "value": "1200"},
            ],
            "tags": [],
        }
        task.update(overrides)
        return task

    def test_returns_none_when_no_date_and_no_amount(self) -> None:
        task = self._make_task(due_date=None, custom_fields=[])
        result = normalize_clickup_task(task)
        self.assertIsNone(result)

    def test_source_is_clickup(self) -> None:
        result = normalize_clickup_task(self._make_task())
        self.assertIsNotNone(result)
        self.assertEqual(result["source"], "clickup")

    def test_clickup_task_id_preserved(self) -> None:
        result = normalize_clickup_task(self._make_task(**{"id": "CU-ABC"}))
        self.assertEqual(result["clickup_task_id"], "CU-ABC")

    def test_open_status_maps_to_planned(self) -> None:
        result = normalize_clickup_task(self._make_task(**{"status": {"status": "open"}}))
        self.assertEqual(result["status"], "planned")

    def test_done_status_maps_to_paid(self) -> None:
        result = normalize_clickup_task(self._make_task(**{"status": {"status": "done"}}))
        self.assertEqual(result["status"], "paid")

    def test_in_progress_maps_to_pending(self) -> None:
        result = normalize_clickup_task(self._make_task(**{"status": {"status": "in progress"}}))
        self.assertEqual(result["status"], "pending")

    def test_confidence_is_estimated(self) -> None:
        result = normalize_clickup_task(self._make_task())
        self.assertEqual(result["confidence"], "estimated")

    def test_task_name_preserved(self) -> None:
        result = normalize_clickup_task(self._make_task(**{"name": "Office Supplies"}))
        self.assertEqual(result["name"], "Office Supplies")


if __name__ == "__main__":
    unittest.main()
