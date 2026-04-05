"""Normalizers — convert raw source rows into CashEvent field dictionaries.

Each normalizer returns a dict of keyword arguments suitable for constructing
or updating a CashEvent ORM entity.  The callers (upload.py, obligations.py)
are responsible for DB writes.

Supported sources
-----------------
    normalize_bank_csv_row(row)          bank export CSV (13-column format)
    normalize_clickup_task(task)         ClickUp task dict from ClickUpClient
    normalize_qbo_open_invoices_csv(b)   QBO "Open Invoices Report" CSV export
    detect_csv_format(csv_bytes)         sniff bytes → 'bank' | 'qbo_open_invoices'
"""

from __future__ import annotations

import csv
import io
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from sales_support_agent.services.cashflow.categorizer import categorize


# ---------------------------------------------------------------------------
# Bank CSV normalizer
# ---------------------------------------------------------------------------

# Expected column names (case-insensitive match used below)
_CSV_COLUMNS = {
    "transaction_id": "Transaction ID",
    "posting_date": "Posting Date",
    "effective_date": "Effective Date",
    "transaction_type": "Transaction Type",
    "amount": "Amount",
    "check_number": "Check Number",
    "reference_number": "Reference Number",
    "description": "Description",
    "transaction_category": "Transaction Category",
    "type": "Type",
    "balance": "Balance",
    "memo": "Memo",
    "extended_description": "Extended Description",
}


def normalize_bank_csv_row(row: dict[str, str]) -> dict[str, Any]:
    """Convert one bank CSV row dict into CashEvent keyword arguments.

    The row dict is expected to come from csv.DictReader, so keys are the
    exact column headers from the file.  Missing or None values are handled
    gracefully — the normalizer never raises on bad data; it records what it
    can and leaves the rest at safe defaults.

    Args:
        row: A dict mapping CSV column header → raw string value.

    Returns:
        Dict of CashEvent field names → parsed values, ready to pass as
        **kwargs to the CashEvent constructor or to setattr for updates.
    """
    # --- Resolve column values tolerantly ---------------------------------
    def _get(*names: str) -> str:
        """Return the first non-empty value found for any of the given names."""
        for name in names:
            v = (row.get(name) or row.get(name.strip()) or "").strip()
            if v:
                return v
        return ""

    raw_id         = _get("Transaction ID")
    posting_date   = _get("Posting Date", "Date")
    effective_date = _get("Effective Date")
    txn_type       = _get("Transaction Type")   # Debit | Credit | Check
    raw_amount     = _get("Amount")
    description    = _get("Description")
    bank_category  = _get("Transaction Category")
    txn_subtype    = _get("Type")               # Card | Retail ACH | POS | Check
    raw_balance    = _get("Balance", "Account Balance")
    ext_desc       = _get("Extended Description")

    # Prefer Extended Description when richer than Description
    full_description = ext_desc if len(ext_desc) > len(description) else description

    # --- Amount → cents ---------------------------------------------------
    amount_cents = _parse_amount_cents(raw_amount)
    # CSV amounts are signed (negative = debit). We store absolute value.
    event_type = "inflow" if amount_cents >= 0 else "outflow"
    amount_cents_abs = abs(amount_cents)

    # --- Dates ------------------------------------------------------------
    due_date      = _parse_date(posting_date)
    effective_dt  = _parse_date(effective_date)

    # --- Balance → cents --------------------------------------------------
    balance_cents = _parse_amount_cents(raw_balance) if raw_balance else None

    # --- Category ---------------------------------------------------------
    category = categorize(full_description, bank_category)

    # --- Vendor extraction ------------------------------------------------
    vendor = _extract_vendor(full_description, txn_subtype)

    # --- Confidence -------------------------------------------------------
    # Bank CSV rows are confirmed facts (they already posted)
    confidence = "confirmed"

    # --- Status -----------------------------------------------------------
    # Posted bank transactions are 'posted' until matched to a planned event
    status = "posted"

    return {
        "source": "csv",
        "source_id": raw_id or "",
        "event_type": event_type,
        "category": category,
        "subcategory": txn_subtype,
        "name": _clean_name(full_description),
        "description": full_description,
        "vendor_or_customer": vendor,
        "amount_cents": amount_cents_abs,
        "due_date": due_date,
        "effective_date": effective_dt,
        "expected_date": None,
        "status": status,
        "confidence": confidence,
        "bank_transaction_type": txn_subtype,
        "bank_reference": _get("Reference Number"),
        "account_balance_cents": balance_cents,
        "notes": "",
    }


# ---------------------------------------------------------------------------
# ClickUp task normalizer
# ---------------------------------------------------------------------------

# Custom field names we look for on ClickUp tasks (case-insensitive)
_CU_AMOUNT_FIELDS = ("amount", "payment amount", "ap amount", "ar amount", "invoice amount")
_CU_VENDOR_FIELDS = ("vendor", "payee", "customer", "client", "counterparty")
_CU_CATEGORY_FIELDS = ("category", "expense category", "type")


def normalize_clickup_task(task: dict[str, Any]) -> dict[str, Any] | None:
    """Convert a ClickUp task dict into CashEvent keyword arguments.

    Returns None if the task lacks both a due date and an amount (i.e., it
    cannot be meaningfully placed on the forecast).

    Args:
        task: Raw task dict as returned by ClickUpClient.get_task() or from
              the tasks list in get_tasks().

    Returns:
        Dict of CashEvent field names → parsed values, or None if the task
        does not have enough financial data to be useful.
    """
    task_id   = (task.get("id") or "").strip()
    task_name = (task.get("name") or "").strip()
    task_url  = (task.get("url") or "").strip()

    # Due date from the task's native due_date field (Unix ms timestamp)
    raw_due = task.get("due_date")
    due_date = _parse_unix_ms(raw_due) if raw_due else None

    # Custom fields
    custom_fields: list[dict[str, Any]] = task.get("custom_fields") or []
    amount_cents  = _cu_find_amount_cents(custom_fields)
    vendor        = _cu_find_field(custom_fields, _CU_VENDOR_FIELDS) or ""
    cu_category   = _cu_find_field(custom_fields, _CU_CATEGORY_FIELDS) or ""

    # Require at least a due date OR a non-zero amount
    if due_date is None and amount_cents == 0:
        return None

    # Determine event type from status or name heuristics
    status_str = ((task.get("status") or {}).get("status") or "").lower()
    event_type = _infer_event_type(task_name, cu_category)

    # Map ClickUp status to our internal status
    internal_status = _map_clickup_status(status_str)

    # Category from description/name + cu_category hint
    category = categorize(task_name, cu_category)

    return {
        "source": "clickup",
        "source_id": task_id,
        "event_type": event_type,
        "category": category,
        "subcategory": "",
        "name": task_name,
        "description": f"Imported from ClickUp task {task_url or task_id}",
        "vendor_or_customer": vendor,
        "amount_cents": amount_cents,
        "due_date": due_date,
        "effective_date": None,
        "expected_date": None,
        "status": internal_status,
        "confidence": "estimated",   # ClickUp data is manually entered
        "clickup_task_id": task_id,
        "recurring_rule": _cu_detect_recurring(task),
        "bank_transaction_type": "",
        "bank_reference": "",
        "account_balance_cents": None,
        "notes": f"ClickUp: {task_url}" if task_url else "",
    }


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _parse_amount_cents(raw: str) -> int:
    """Parse a currency string into integer cents.

    Handles:
        "-2056.00000"  → -205600
        "1,335.50000"  → 133550
        "$1,234.56"    → 123456
        ""             → 0

    Uses Decimal arithmetic internally to avoid float rounding errors.
    """
    if not raw:
        return 0
    cleaned = re.sub(r"[,$\s]", "", raw.strip())
    try:
        value = Decimal(cleaned)
        return int(value * 100)
    except InvalidOperation:
        return 0


def _parse_date(raw: str) -> datetime | None:
    """Parse M/D/YYYY or YYYY-MM-DD date strings into a UTC datetime."""
    if not raw:
        return None
    raw = raw.strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y"):
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _parse_unix_ms(raw: Any) -> datetime | None:
    """Parse a Unix millisecond timestamp (int or string) into a UTC datetime."""
    try:
        ms = int(raw)
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    except (ValueError, TypeError, OSError):
        return None


def _clean_name(description: str) -> str:
    """Return a shorter display name from the full bank description.

    Strategy: take the first meaningful segment before common ACH boilerplate.
    """
    if not description:
        return ""
    # Strip leading "Withdrawal" / "Deposit" prefix
    cleaned = re.sub(r"^(Withdrawal|Deposit)\s+(Debit|ACH|Card|POS|Check|By Check)?\s*", "", description, flags=re.IGNORECASE).strip()
    # Truncate at ACH boilerplate keywords
    for stop in ("Entry Class Code", "ACH Trace Number", "Date \d\d", "Card \d{4}"):
        match = re.search(stop, cleaned, re.IGNORECASE)
        if match:
            cleaned = cleaned[: match.start()].strip()
    # Collapse multiple spaces
    cleaned = re.sub(r"\s{2,}", " ", cleaned).strip(" .,")
    return cleaned[:255] or description[:255]


def _extract_vendor(description: str, txn_subtype: str) -> str:
    """Attempt to extract a clean vendor name from the transaction description."""
    cleaned = _clean_name(description)
    # For card transactions the vendor name is usually the first word cluster
    if txn_subtype in ("Card", "POS") and cleaned:
        # Take text before the first location-like pattern (city/state)
        parts = re.split(r"\s{2,}|\d{3}-\d{3}-\d{4}", cleaned)
        return parts[0].strip()[:255]
    # For ACH, take the CO: field if present
    co_match = re.search(r"CO:\s*([^E]+?)(?:\s+Entry|\s*$)", description, re.IGNORECASE)
    if co_match:
        return co_match.group(1).strip()[:255]
    return cleaned[:255]


_INFLOW_KEYWORDS = re.compile(r"\b(ar|invoice|deposit|revenue|income|receivable|payment received)\b", re.IGNORECASE)
_OUTFLOW_KEYWORDS = re.compile(r"\b(ap|bill|expense|payable|rent|payroll|loan|subscription)\b", re.IGNORECASE)


def _infer_event_type(name: str, category: str) -> str:
    """Guess inflow vs outflow from task name and category hint."""
    text = f"{name} {category}".lower()
    if _INFLOW_KEYWORDS.search(text):
        return "inflow"
    return "outflow"  # default to outflow for AP tasks


def _map_clickup_status(status: str) -> str:
    """Map a ClickUp status string to our internal CashEvent status."""
    mapping = {
        "complete": "paid",
        "done": "paid",
        "closed": "paid",
        "paid": "paid",
        "cancelled": "cancelled",
        "canceled": "cancelled",
        "in progress": "pending",
        "in review": "pending",
        "open": "planned",
        "to do": "planned",
        "": "planned",
    }
    return mapping.get(status.lower().strip(), "planned")


def _cu_find_amount_cents(fields: list[dict[str, Any]]) -> int:
    """Extract the first numeric custom field matching known amount field names."""
    for f in fields:
        field_name = (f.get("name") or "").strip().lower()
        if field_name not in _CU_AMOUNT_FIELDS:
            continue
        raw = f.get("value")
        if raw is None:
            continue
        cents = _parse_amount_cents(str(raw))
        if cents != 0:
            return abs(cents)
    return 0


def _cu_find_field(fields: list[dict[str, Any]], names: tuple[str, ...]) -> str | None:
    """Extract the string value of the first matching custom field."""
    for f in fields:
        field_name = (f.get("name") or "").strip().lower()
        if field_name in names:
            val = f.get("value")
            if val and isinstance(val, str):
                return val.strip()
    return None


def _cu_detect_recurring(task: dict[str, Any]) -> str:
    """Detect a recurring pattern from ClickUp task tags or name."""
    tags = [t.get("name", "").lower() for t in (task.get("tags") or [])]
    name = (task.get("name") or "").lower()
    if any(t in tags for t in ("weekly", "every week")):
        return "weekly"
    if any(t in tags for t in ("biweekly", "every two weeks")):
        return "biweekly"
    if any(t in tags for t in ("monthly", "every month")):
        return "monthly"
    if "weekly" in name:
        return "weekly"
    if "monthly" in name:
        return "monthly"
    return ""


# ---------------------------------------------------------------------------
# QBO Open Invoices Report normalizer
# ---------------------------------------------------------------------------

def detect_csv_format(csv_bytes: bytes) -> str:
    """Sniff the first 2 KB to determine which CSV format was uploaded.

    Returns:
        'qbo_open_invoices'  — QBO "Open Invoices Report" export
        'bank'               — standard bank statement CSV (default)
    """
    try:
        head = csv_bytes[:2000].decode("utf-8", errors="replace")
    except Exception:
        return "bank"
    # QBO Open Invoices Report has these two strings near the top
    if "Open Invoices" in head and "Open balance" in head:
        return "qbo_open_invoices"
    return "bank"


def normalize_qbo_open_invoices_csv(csv_bytes: bytes) -> list[dict[str, Any]]:
    """Parse a QBO 'Open Invoices Report' CSV export into cash_event dicts.

    Format quirks handled:
    - 4-row preamble (title, company, date, blank) before the header row
    - Hierarchical: customer-name rows (col 0 non-empty) then transaction rows
    - "Total for …" subtotal rows and a final ",TOTAL,…" row are skipped
    - Only Transaction type == "Invoice" with positive Open balance imported
    - Payment and Journal Entry rows are skipped entirely
    - Invoice numbers used as dedup key; fallback to customer+date+amount

    Returns:
        List of dicts ready for INSERT into cash_events, one per open invoice.
        source='qbo-csv', event_type='inflow', status='planned'|'overdue',
        confidence='confirmed'.
    """
    today = datetime.now(timezone.utc).date()
    results: list[dict[str, Any]] = []

    lines = csv_bytes.decode("utf-8", errors="replace").splitlines(keepends=True)

    # Find the header row — contains both "Due date" and "Open balance"
    header_idx: int | None = None
    for i, line in enumerate(lines):
        if "Due date" in line and "Open balance" in line:
            header_idx = i
            break
    if header_idx is None:
        return results  # unrecognised format

    data_text = "".join(lines[header_idx:])
    reader = csv.DictReader(io.StringIO(data_text))

    current_customer = ""

    for row in reader:
        # Column 0 has no header → DictReader key is empty string
        col0 = (row.get("") or "").strip()
        date_raw = (row.get("Date") or "").strip()
        txn_type = (row.get("Transaction type") or "").strip()
        num = (row.get("Num") or "").strip()
        term = (row.get("Term") or "").strip()
        due_date_raw = (row.get("Due date") or "").strip()
        balance_raw = (row.get("Open balance") or "").strip()

        # Stop at the grand-total row
        if date_raw == "TOTAL":
            break

        # Customer name row (non-empty col0, not a subtotal)
        if col0 and not col0.startswith("Total"):
            current_customer = col0
            continue

        # Subtotal rows
        if col0.startswith("Total"):
            continue

        # Only process Invoice rows
        if txn_type != "Invoice":
            continue

        # Parse outstanding balance — skip zero or negative (credits)
        amount_cents = _parse_amount_cents(balance_raw)
        if amount_cents <= 0:
            continue

        # Parse dates
        due_dt = _parse_date(due_date_raw) or _parse_date(date_raw)
        inv_dt = _parse_date(date_raw)

        # Determine due date as a plain date for status check
        due_date_obj = None
        if due_dt is not None:
            due_date_obj = due_dt.date() if hasattr(due_dt, "date") else due_dt

        status = "overdue" if (due_date_obj and due_date_obj < today) else "planned"

        # Stable dedup key: invoice number preferred, then slug+date+cents
        customer_slug = re.sub(r"[^a-z0-9]+", "-", current_customer.lower()).strip("-")
        if num:
            source_id = f"qbo-ar-{num}"
        else:
            date_str = inv_dt.strftime("%Y%m%d") if inv_dt else "nodate"
            source_id = f"qbo-ar-{customer_slug}-{date_str}-{amount_cents}"

        description = f"Invoice #{num}" if num else f"Invoice — {current_customer}"
        if term:
            description += f" · {term}"

        results.append({
            "source": "qbo-csv",
            "source_id": source_id,
            "event_type": "inflow",
            "category": "revenue",
            "subcategory": "",
            "name": current_customer,
            "description": description,
            "vendor_or_customer": current_customer,
            "amount_cents": amount_cents,
            "due_date": due_dt,
            "status": status,
            "confidence": "confirmed",
            "bank_transaction_type": "",
            "bank_reference": "",
            "account_balance_cents": None,
            "notes": f"Terms: {term}" if term else "",
        })

    return results
