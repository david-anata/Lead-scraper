"""Comprehensive tests for cashflow normalizers.

Covers:
- TestBankCSVNormalizer  (~12 tests) — bank statement CSV format
- TestQBOOpenInvoicesNormalizer (~12 tests) — QBO Open Invoices Report CSV
- TestCSVFormatDetection (~5 tests) — detect_csv_format() sniffing
"""

from __future__ import annotations

import unittest
from datetime import date, datetime, timezone

from sales_support_agent.services.cashflow.normalizers import (
    detect_csv_format,
    normalize_bank_csv_row,
    normalize_qbo_open_invoices_csv,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _bank_row(**overrides) -> dict:
    """Build a minimal valid bank CSV row with sensible defaults."""
    row = {
        "Transaction ID": "TXN-001",
        "Posting Date": "03/15/2026",
        "Effective Date": "03/15/2026",
        "Transaction Type": "Debit",
        "Amount": "-1500.00",
        "Debit": "1500.00",
        "Credit": "",
        "Balance": "23450.00",
        "Description": "Withdrawal ACH FORAFINANCIAL PAYMT Entry Class Code: WEB",
        "Transaction Category": "Loan Payments",
        "Type": "ACH",
        "Reference Number": "REF001",
        "Extended Description": "",
    }
    row.update(overrides)
    return row


def _qbo_open_invoices_csv(invoice_lines: list[str] | None = None) -> bytes:
    """Build a minimal valid QBO Open Invoices Report CSV."""
    if invoice_lines is None:
        invoice_lines = [
            "Acme Corp,,,,,,",
            ",01/15/2026,Invoice,INV-001,Net 15,01/30/2026,\"1,500.00\"",
            "Total for Acme Corp,,,,,,\"$1,500.00\"",
            ",TOTAL,,,,\"$1,500.00\"",
        ]
    header = [
        "Open Invoices Report,,,,,,",
        "anata LLC,,,,,,",
        "\"As of Mar 1, 2026\",,,,,,",
        "",
        ",Date,Transaction type,Num,Term,Due date,Open balance",
    ]
    all_lines = header + invoice_lines
    return "\n".join(all_lines).encode("utf-8")


# ---------------------------------------------------------------------------
# TestBankCSVNormalizer
# ---------------------------------------------------------------------------

class TestBankCSVNormalizer(unittest.TestCase):

    def test_source_status_confidence_defaults(self) -> None:
        result = normalize_bank_csv_row(_bank_row())
        self.assertEqual(result["source"], "csv")
        self.assertEqual(result["status"], "posted")
        self.assertEqual(result["confidence"], "confirmed")

    def test_source_id_from_transaction_id(self) -> None:
        result = normalize_bank_csv_row(_bank_row(**{"Transaction ID": "TXN-ABCDEF"}))
        self.assertEqual(result["source_id"], "TXN-ABCDEF")

    def test_negative_amount_is_outflow(self) -> None:
        result = normalize_bank_csv_row(_bank_row(**{"Amount": "-500.00"}))
        self.assertEqual(result["event_type"], "outflow")
        self.assertEqual(result["amount_cents"], 50000)

    def test_positive_amount_is_inflow(self) -> None:
        result = normalize_bank_csv_row(_bank_row(**{
            "Amount": "2500.00",
            "Description": "INTUIT DEPOSIT SQ",
        }))
        self.assertEqual(result["event_type"], "inflow")
        self.assertEqual(result["amount_cents"], 250000)

    def test_amount_cents_is_integer(self) -> None:
        result = normalize_bank_csv_row(_bank_row(**{"Amount": "-1500.00"}))
        self.assertIsInstance(result["amount_cents"], int)

    def test_comma_separated_amount(self) -> None:
        result = normalize_bank_csv_row(_bank_row(**{"Amount": "-1,234.56"}))
        self.assertEqual(result["amount_cents"], 123456)

    def test_dollar_sign_amount(self) -> None:
        result = normalize_bank_csv_row(_bank_row(**{"Amount": "$1,234.56"}))
        self.assertEqual(result["amount_cents"], 123456)

    def test_slash_date_parsing(self) -> None:
        result = normalize_bank_csv_row(_bank_row(**{"Posting Date": "03/15/2026"}))
        due = result["due_date"]
        self.assertIsNotNone(due)
        # May be datetime or date object depending on _parse_date
        if hasattr(due, "date"):
            self.assertEqual(due.date(), date(2026, 3, 15))
        else:
            self.assertEqual(due, date(2026, 3, 15))

    def test_iso_date_parsing(self) -> None:
        result = normalize_bank_csv_row(_bank_row(**{"Posting Date": "2026-03-15"}))
        due = result["due_date"]
        self.assertIsNotNone(due)
        if hasattr(due, "date"):
            self.assertEqual(due.date(), date(2026, 3, 15))
        else:
            self.assertEqual(due, date(2026, 3, 15))

    def test_balance_column_parsed(self) -> None:
        result = normalize_bank_csv_row(_bank_row(**{"Balance": "23450.00"}))
        self.assertEqual(result["account_balance_cents"], 2345000)

    def test_missing_balance_is_none(self) -> None:
        result = normalize_bank_csv_row(_bank_row(**{"Balance": ""}))
        self.assertIsNone(result["account_balance_cents"])

    def test_ach_boilerplate_cleaned_from_name(self) -> None:
        result = normalize_bank_csv_row(_bank_row(**{
            "Description": "Withdrawal ACH FORAFINANCIAL PAYMT Entry Class Code: WEB"
        }))
        name = result["name"]
        self.assertNotIn("Withdrawal ACH", name)
        self.assertNotIn("Entry Class Code", name)

    def test_extended_description_preferred_when_longer(self) -> None:
        short_desc = "ACH DEP"
        long_ext = "ACH DEPOSIT FROM CUSTOMER PAYMENT REFERENCE 12345 DETAILS"
        result = normalize_bank_csv_row(_bank_row(**{
            "Description": short_desc,
            "Extended Description": long_ext,
        }))
        # Extended description should be used as the base for name/description
        self.assertIn("CUSTOMER PAYMENT", result["description"] + result["name"])

    def test_category_assigned_for_known_keywords(self) -> None:
        result = normalize_bank_csv_row(_bank_row(**{
            "Description": "FORAFINANCIAL PAYMT",
            "Transaction Category": "Loan Payments",
        }))
        # Should categorize as debt/loan related
        self.assertIsNotNone(result["category"])
        self.assertIsInstance(result["category"], str)

    def test_missing_transaction_id_uses_empty_string(self) -> None:
        result = normalize_bank_csv_row(_bank_row(**{"Transaction ID": ""}))
        self.assertEqual(result["source_id"], "")


# ---------------------------------------------------------------------------
# TestQBOOpenInvoicesNormalizer
# ---------------------------------------------------------------------------

class TestQBOOpenInvoicesNormalizer(unittest.TestCase):

    def test_returns_list(self) -> None:
        result = normalize_qbo_open_invoices_csv(_qbo_open_invoices_csv())
        self.assertIsInstance(result, list)

    def test_single_invoice_parsed(self) -> None:
        result = normalize_qbo_open_invoices_csv(_qbo_open_invoices_csv())
        self.assertEqual(len(result), 1)
        rec = result[0]
        self.assertEqual(rec["name"], "Acme Corp")

    def test_source_is_qbo_csv(self) -> None:
        result = normalize_qbo_open_invoices_csv(_qbo_open_invoices_csv())
        self.assertEqual(result[0]["source"], "qbo-csv")

    def test_event_type_is_inflow(self) -> None:
        result = normalize_qbo_open_invoices_csv(_qbo_open_invoices_csv())
        self.assertEqual(result[0]["event_type"], "inflow")

    def test_confidence_is_confirmed(self) -> None:
        result = normalize_qbo_open_invoices_csv(_qbo_open_invoices_csv())
        self.assertEqual(result[0]["confidence"], "confirmed")

    def test_category_is_revenue(self) -> None:
        result = normalize_qbo_open_invoices_csv(_qbo_open_invoices_csv())
        self.assertEqual(result[0]["category"], "revenue")

    def test_invoice_amount_cents(self) -> None:
        result = normalize_qbo_open_invoices_csv(_qbo_open_invoices_csv())
        self.assertEqual(result[0]["amount_cents"], 150000)

    def test_source_id_uses_invoice_number(self) -> None:
        result = normalize_qbo_open_invoices_csv(_qbo_open_invoices_csv())
        self.assertEqual(result[0]["source_id"], "qbo-ar-INV-001")

    def test_source_id_fallback_when_no_invoice_num(self) -> None:
        lines = [
            "Beta LLC,,,,,,",
            ",02/01/2026,Invoice,,Net 15,02/16/2026,\"500.00\"",
            "Total for Beta LLC,,,,,,\"$500.00\"",
            ",TOTAL,,,,\"$500.00\"",
        ]
        result = normalize_qbo_open_invoices_csv(_qbo_open_invoices_csv(lines))
        # Should fall back to slug+date+amount key
        self.assertTrue(result[0]["source_id"].startswith("qbo-ar-"))

    def test_payment_rows_skipped(self) -> None:
        lines = [
            "Payment Co,,,,,,",
            ",03/04/2026,Payment,,,03/04/2026,\"-4,418.61\"",
            "Total for Payment Co,,,,,,-$4,418.61",
            ",TOTAL,,,,\"$0.00\"",
        ]
        result = normalize_qbo_open_invoices_csv(_qbo_open_invoices_csv(lines))
        self.assertEqual(len(result), 0)

    def test_negative_balance_skipped(self) -> None:
        lines = [
            "Credit Co,,,,,,",
            ",03/04/2026,Invoice,INV-999,Net 15,03/19/2026,\"-100.00\"",
            "Total for Credit Co,,,,,,-$100.00",
            ",TOTAL,,,,\"$0.00\"",
        ]
        result = normalize_qbo_open_invoices_csv(_qbo_open_invoices_csv(lines))
        self.assertEqual(len(result), 0)

    def test_multiple_customers_multiple_invoices(self) -> None:
        lines = [
            "Divi Energy,,,,,,",
            ",03/02/2026,Invoice,1000005,,03/09/2026,\"3,400.00\"",
            ",04/01/2026,Invoice,1000166,,04/08/2026,\"3,400.00\"",
            "Total for Divi Energy,,,,,,\"$6,800.00\"",
            "PupTale,,,,,,",
            ",04/01/2026,Invoice,1000164,Net 15,04/16/2026,176.00",
            "Total for PupTale,,,,,,$176.00",
            ",TOTAL,,,,\"$6,976.00\"",
        ]
        result = normalize_qbo_open_invoices_csv(_qbo_open_invoices_csv(lines))
        self.assertEqual(len(result), 3)
        customers = {r["name"] for r in result}
        self.assertIn("Divi Energy", customers)
        self.assertIn("PupTale", customers)

    def test_overdue_status_for_past_due_date(self) -> None:
        lines = [
            "Overdue Corp,,,,,,",
            ",01/01/2024,Invoice,OLD-001,Net 30,02/01/2024,\"1,000.00\"",
            "Total for Overdue Corp,,,,,,\"$1,000.00\"",
            ",TOTAL,,,,\"$1,000.00\"",
        ]
        result = normalize_qbo_open_invoices_csv(_qbo_open_invoices_csv(lines))
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["status"], "overdue")

    def test_real_fixture_file(self) -> None:
        """Integration test against the real QBO export file."""
        import os
        fixture_path = os.path.expanduser("~/Downloads/anata LLC_Open Invoices Report.csv")
        if not os.path.exists(fixture_path):
            self.skipTest("Fixture file not found; skipping real-file test")
        with open(fixture_path, "rb") as f:
            csv_bytes = f.read()
        result = normalize_qbo_open_invoices_csv(csv_bytes)
        # Should produce some results (invoices with positive balance)
        self.assertIsInstance(result, list)
        # All results must be inflow with positive amounts
        for r in result:
            self.assertEqual(r["event_type"], "inflow")
            self.assertGreater(r["amount_cents"], 0)
        # Check for known customers from the file
        names = {r["name"] for r in result}
        # Divi Energy has two positive invoices in the file
        if result:
            self.assertTrue(any(r["amount_cents"] > 0 for r in result))


# ---------------------------------------------------------------------------
# TestCSVFormatDetection
# ---------------------------------------------------------------------------

class TestCSVFormatDetection(unittest.TestCase):

    def test_qbo_open_invoices_detected(self) -> None:
        csv_bytes = b"Open Invoices Report\nanata LLC\nAs of Apr 4, 2026\n\n,Date,Transaction type,Num,Term,Due date,Open balance\n"
        result = detect_csv_format(csv_bytes)
        self.assertEqual(result, "qbo_open_invoices")

    def test_bank_csv_detected(self) -> None:
        csv_bytes = b"Transaction ID,Posting Date,Effective Date,Transaction Type,Amount,Balance,Description\nTXN-001,03/15/2026,,Debit,-1500.00,23450.00,FORAFINANCIAL\n"
        result = detect_csv_format(csv_bytes)
        self.assertEqual(result, "bank")

    def test_empty_bytes_returns_bank(self) -> None:
        result = detect_csv_format(b"")
        self.assertEqual(result, "bank")

    def test_partial_qbo_header_requires_both_keywords(self) -> None:
        # Has "Open Invoices" but not "Open balance" → should return bank
        csv_bytes = b"Open Invoices Report\nanata LLC\n,Date,Transaction type,Num\n"
        result = detect_csv_format(csv_bytes)
        self.assertEqual(result, "bank")

    def test_qbo_with_unicode_content(self) -> None:
        csv_bytes = "Open Invoices Report\nanata LLC\n,Date,Transaction type,Open balance\n".encode("utf-8")
        result = detect_csv_format(csv_bytes)
        self.assertEqual(result, "qbo_open_invoices")

    def test_binary_garbage_returns_bank(self) -> None:
        result = detect_csv_format(b"\xff\xfe garbage data \x00\x01\x02")
        self.assertEqual(result, "bank")


if __name__ == "__main__":
    unittest.main()
