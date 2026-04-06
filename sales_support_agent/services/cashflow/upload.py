"""CSV bank statement upload — parse, categorise, dedup, auto-match."""

from __future__ import annotations

import csv
import io
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

from sales_support_agent.services.cashflow.matcher import auto_match_transactions
from sales_support_agent.services.cashflow.normalizers import (
    detect_csv_format,
    normalize_bank_csv_row,
    normalize_qbo_open_invoices_csv,
)
from sales_support_agent.services.cashflow.obligations import list_obligations


@dataclass
class UploadResult:
    rows_read: int = 0
    rows_inserted: int = 0
    rows_skipped_duplicate: int = 0
    rows_skipped_invalid: int = 0
    matches_made: int = 0
    errors: list[str] = field(default_factory=list)
    latest_balance_cents: Optional[int] = None

    @property
    def success(self) -> bool:
        return not self.errors

    def summary(self) -> str:
        parts = [
            f"{self.rows_read} rows read",
            f"{self.rows_inserted} inserted",
            f"{self.rows_skipped_duplicate} duplicate",
            f"{self.rows_skipped_invalid} invalid",
            f"{self.matches_made} auto-matched",
        ]
        if self.latest_balance_cents is not None:
            bal = self.latest_balance_cents / 100
            parts.append(f"balance ${bal:,.2f}")
        return " · ".join(parts)


def run_csv_upload(
    csv_bytes: bytes,
    *,
    merge_mode: str = "append",  # "append" | "replace_range"
) -> UploadResult:
    """
    Parse *csv_bytes*, insert new transactions into cash_events,
    then run auto-match against open planned obligations.

    merge_mode:
      - "append": skip rows whose source_id already exists
      - "replace_range": delete existing csv rows in the date range first
    """
    from sales_support_agent.models.database import get_engine
    from sqlalchemy import text

    # -- Auto-detect format and delegate ------------------------------------
    if detect_csv_format(csv_bytes) == "qbo_open_invoices":
        return _run_qbo_open_invoices_upload(csv_bytes, engine=get_engine())

    result = UploadResult()

    # -- Parse CSV ----------------------------------------------------------
    text_io = io.StringIO(csv_bytes.decode("utf-8", errors="replace"))
    reader = csv.DictReader(text_io)

    normalised_rows: list[dict[str, Any]] = []
    for raw_row in reader:
        result.rows_read += 1
        try:
            norm = normalize_bank_csv_row(raw_row)
            normalised_rows.append(norm)
        except Exception as exc:
            result.rows_skipped_invalid += 1
            result.errors.append(f"Row {result.rows_read}: {exc}")

    if not normalised_rows:
        return result

    # Track the latest balance (last row with a balance value)
    for row in reversed(normalised_rows):
        if row.get("account_balance_cents") is not None:
            result.latest_balance_cents = row["account_balance_cents"]
            break

    # -- Replace range if requested -----------------------------------------
    if merge_mode == "replace_range":
        dates = [r["due_date"] for r in normalised_rows if r.get("due_date")]
        if dates:
            min_date = min(dates)
            max_date = max(dates)
            with get_engine().begin() as conn:
                conn.execute(
                    text(
                        "DELETE FROM cash_events "
                        "WHERE source = 'csv' "
                        "AND due_date >= :min_d AND due_date <= :max_d"
                    ),
                    {
                        "min_d": min_date.isoformat() if hasattr(min_date, "isoformat") else str(min_date),
                        "max_d": max_date.isoformat() if hasattr(max_date, "isoformat") else str(max_date),
                    },
                )

    # -- Fetch existing source_ids to detect duplicates ---------------------
    with get_engine().connect() as conn:
        existing_ids: set[str] = {
            row[0]
            for row in conn.execute(
                text("SELECT source_id FROM cash_events WHERE source = 'csv'")
            ).fetchall()
        }

    # -- Insert new rows (single transaction for all inserts) ---------------
    now = datetime.utcnow().isoformat()
    new_events: list[dict[str, Any]] = []

    rows_to_insert = []
    for row in normalised_rows:
        source_id = row.get("source_id", "")
        if source_id in existing_ids:
            result.rows_skipped_duplicate += 1
            continue
        rows_to_insert.append(row)

    if rows_to_insert:
        with get_engine().begin() as conn:
            for row in rows_to_insert:
                source_id = row.get("source_id", "")
                event_id = str(uuid.uuid4())
                due_date = row.get("due_date")
                due_date_str = (
                    due_date.isoformat()
                    if hasattr(due_date, "isoformat")
                    else str(due_date)[:10] if due_date else None
                )
                # Auto-label: use vendor_or_customer as the initial friendly_name
                # so rows don't show "⚠ Unlabeled" immediately after upload.
                vendor = row.get("vendor_or_customer", "") or ""
                auto_friendly = vendor[:255] if vendor.strip() else None

                conn.execute(
                    text("""
                        INSERT INTO cash_events (
                            id, source, source_id, event_type, category,
                            subcategory, description, name, vendor_or_customer,
                            amount_cents, due_date, status, confidence,
                            account_balance_cents,
                            bank_transaction_type, bank_reference,
                            notes, recurring_rule, clickup_task_id,
                            friendly_name,
                            created_at, updated_at
                        ) VALUES (
                            :id, 'csv', :source_id, :event_type, :category,
                            :subcategory, :description, :name, :vendor_or_customer,
                            :amount_cents, :due_date, 'posted', 'confirmed',
                            :account_balance_cents,
                            :bank_transaction_type, :bank_reference,
                            '', '', '',
                            :friendly_name,
                            :now, :now
                        )
                    """),
                    {
                        "id": event_id,
                        "source_id": source_id,
                        "event_type": row.get("event_type", "outflow"),
                        "category": row.get("category", "other"),
                        "subcategory": row.get("subcategory", ""),
                        "description": row.get("description", "") or "",
                        "name": row.get("name", ""),
                        "vendor_or_customer": vendor,
                        "amount_cents": row.get("amount_cents", 0),
                        "due_date": due_date_str,
                        "account_balance_cents": row.get("account_balance_cents"),
                        "bank_transaction_type": row.get("bank_transaction_type", "") or "",
                        "bank_reference": row.get("bank_reference", "") or "",
                        "friendly_name": auto_friendly,
                        "now": now,
                    },
                )
                existing_ids.add(source_id)
                result.rows_inserted += 1
                new_events.append({"id": event_id, **row})

    # -- Auto-match against planned obligations -----------------------------
    if new_events:
        planned = list_obligations(
            status="planned",
            from_date=None,
            to_date=None,
        )
        match_results = auto_match_transactions(new_events, planned)

        for mr in match_results:
            if mr.planned_event_id is None:
                continue
            with get_engine().begin() as conn:
                conn.execute(
                    text("""
                        UPDATE cash_events
                        SET status = 'matched', matched_to_id = :planned_id, updated_at = :now
                        WHERE id = :csv_id
                    """),
                    {"planned_id": mr.planned_event_id, "csv_id": mr.csv_event_id, "now": now},
                )
                # Mark the planned obligation as paid/matched
                conn.execute(
                    text("""
                        UPDATE cash_events
                        SET status = 'matched', updated_at = :now
                        WHERE id = :planned_id AND status IN ('planned', 'pending')
                    """),
                    {"planned_id": mr.planned_event_id, "now": now},
                )
            result.matches_made += 1

    return result


# ---------------------------------------------------------------------------
# QBO Open Invoices upload path
# ---------------------------------------------------------------------------

def _run_qbo_open_invoices_upload(csv_bytes: bytes, *, engine) -> UploadResult:
    """Import a QBO Open Invoices Report CSV into cash_events.

    Behaviour:
    - source='qbo-csv', event_type='inflow', status='planned'|'overdue'
    - Dedup by source_id — re-uploading updates the outstanding balance and
      due date instead of inserting a duplicate.
    - Already-matched or paid invoices are left untouched on re-upload.
    - Bank CSV and QBO-CSV events never double-count: bank rows are
      source='csv'/status='posted' (actuals); QBO rows are source='qbo-csv'/
      status='planned' (expected). The auto-matcher links them when a bank
      CSV is uploaded later.
    """
    from sales_support_agent.models.database import upsert_cash_event
    from sqlalchemy import text

    result = UploadResult()
    now = datetime.utcnow().isoformat()

    invoices = normalize_qbo_open_invoices_csv(csv_bytes)
    result.rows_read = len(invoices)

    if not invoices:
        result.errors.append("No open invoices found — check the file is a QBO Open Invoices Report export.")
        return result

    # Fetch existing qbo-csv source_ids (for dedup / update logic)
    with engine.connect() as conn:
        existing: dict[str, str] = {
            row[0]: row[1]
            for row in conn.execute(
                text("SELECT source_id, status FROM cash_events WHERE source = 'qbo-csv'")
            ).fetchall()
        }

    new_events: list[dict[str, Any]] = []

    for row in invoices:
        source_id = row["source_id"]

        if source_id in existing:
            # Re-upload: update balance + status, but leave matched/paid alone
            existing_status = existing[source_id]
            if existing_status in ("matched", "paid", "cancelled"):
                result.rows_skipped_duplicate += 1
                continue
            # Use shared upsert; fetch the existing row id first
            with engine.connect() as conn:
                id_row = conn.execute(
                    text("SELECT id FROM cash_events WHERE source_id = :source_id AND source = 'qbo-csv'"),
                    {"source_id": source_id},
                ).fetchone()
            if id_row:
                event_dict = {"id": id_row[0], "source": "qbo-csv", **row}
                with engine.begin() as conn:
                    upsert_cash_event(conn, event_dict)
            result.rows_skipped_duplicate += 1  # counted as update, not new insert
            continue

        # New invoice — use shared upsert helper
        event_id = str(uuid.uuid4())
        event_dict = {"id": event_id, "source": "qbo-csv", **row}
        with engine.begin() as conn:
            upsert_cash_event(conn, event_dict)
        existing[source_id] = row["status"]
        result.rows_inserted += 1
        new_events.append({"id": event_id, **row})

    # Auto-match new QBO invoices against already-posted bank CSV inflows
    # (catches cases where bank CSV was uploaded before the QBO report)
    if new_events:
        posted_bank = list_obligations(status="posted", from_date=None, to_date=None)
        posted_inflows = [r for r in posted_bank if r.get("event_type") == "inflow"]
        if posted_inflows:
            match_results = auto_match_transactions(posted_inflows, new_events)
            for mr in match_results:
                if mr.planned_event_id is None:
                    continue
                with engine.begin() as conn:
                    conn.execute(
                        text("UPDATE cash_events SET status='matched', updated_at=:now WHERE id=:id"),
                        {"now": now, "id": mr.csv_event_id},
                    )
                    conn.execute(
                        text("UPDATE cash_events SET status='matched', updated_at=:now WHERE id=:id AND status IN ('planned','overdue')"),
                        {"now": now, "id": mr.planned_event_id},
                    )
                result.matches_made += 1

    return result
