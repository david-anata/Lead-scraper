"""CSV bank statement upload — parse, categorise, dedup, auto-match."""

from __future__ import annotations

import csv
import hashlib
import io
import json
from dataclasses import dataclass, field
from datetime import date, datetime
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
    rows_skipped_review: int = 0
    matches_made: int = 0
    source_exceptions: int = 0
    errors: list[str] = field(default_factory=list)
    latest_balance_cents: Optional[int] = None
    import_batch_id: Optional[str] = None

    @property
    def success(self) -> bool:
        return not self.errors

    def summary(self) -> str:
        parts = [
            f"{self.rows_read} rows read",
            f"{self.rows_inserted} inserted",
            f"{self.rows_skipped_duplicate} duplicate",
            f"{self.rows_skipped_invalid} invalid",
            f"{self.rows_skipped_review} review",
            f"{self.matches_made} auto-matched",
            f"{self.source_exceptions} source exceptions",
        ]
        if self.latest_balance_cents is not None:
            bal = self.latest_balance_cents / 100
            parts.append(f"balance ${bal:,.2f}")
        return " · ".join(parts)


def _latest_balance_row(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Return the balance-bearing row with the newest transaction date."""
    candidates: list[dict[str, Any]] = []
    for row_index, row in enumerate(rows):
        if row.get("account_balance_cents") is None:
            continue
        raw_date = row.get("due_date")
        if isinstance(raw_date, datetime):
            parsed_date = raw_date.date()
        elif isinstance(raw_date, date):
            parsed_date = raw_date
        elif isinstance(raw_date, str):
            try:
                parsed_date = date.fromisoformat(raw_date[:10])
            except ValueError:
                continue
        else:
            continue
        candidates.append(
            {
                **row,
                "_balance_date": parsed_date,
                "_source_row_index": row_index,
            }
        )

    if not candidates:
        return None
    return max(candidates, key=lambda row: row["_balance_date"])


def run_csv_upload(
    csv_bytes: bytes,
    *,
    merge_mode: str = "append",
) -> UploadResult:
    """
    Parse *csv_bytes*, insert new transactions into cash_events,
    then run auto-match against open planned obligations.

    Imports are append/merge only. Destructive range replacement is rejected.
    """
    from sales_support_agent.models.database import get_engine
    from sqlalchemy import text

    if merge_mode != "append":
        raise ValueError("Unsupported merge mode. Finance imports are append/merge only.")

    # -- Auto-detect format and delegate ------------------------------------
    if detect_csv_format(csv_bytes) == "qbo_open_invoices":
        return _run_qbo_open_invoices_upload(csv_bytes, engine=get_engine())

    result = UploadResult()

    # -- Parse CSV ----------------------------------------------------------
    text_io = io.StringIO(csv_bytes.decode("utf-8", errors="replace"))
    reader = csv.DictReader(text_io)

    normalised_rows: list[dict[str, Any]] = []
    staged_rows: list[dict[str, Any]] = []
    for raw_row in reader:
        result.rows_read += 1
        try:
            norm = normalize_bank_csv_row(raw_row)
            if norm.get("due_date") is None:
                raise ValueError("posting date is missing or invalid")
            if int(norm.get("amount_cents") or 0) <= 0:
                raise ValueError("transaction amount must be non-zero")
            normalised_rows.append(norm)
            staged_rows.append({"raw": raw_row, "normalized": norm})
        except Exception as exc:
            result.rows_skipped_invalid += 1
            result.errors.append(f"Row {result.rows_read}: {exc}")
            staged_rows.append({"raw": raw_row, "normalized": None, "error": str(exc)})

    if not staged_rows:
        return result

    latest_balance_row = _latest_balance_row(normalised_rows)
    if latest_balance_row is not None:
        result.latest_balance_cents = latest_balance_row["account_balance_cents"]

    # Stage, classify and post canonical rows/source identities atomically.
    from sales_support_agent.services.cashflow.imports import stage_and_post_bank_import

    posted = stage_and_post_bank_import(
        get_engine(),
        file_hash=hashlib.sha256(csv_bytes).hexdigest(),
        rows=staged_rows,
    )
    result.import_batch_id = posted.batch_id
    result.rows_inserted = posted.inserted
    result.rows_skipped_duplicate = posted.duplicates
    result.rows_skipped_review = posted.review
    if posted.review:
        result.errors.append(
            f"{posted.review} row(s) need review. No records were committed from this file."
        )
    # Invalid rows were already counted during normalization.
    new_events = posted.new_events

    # -- Persist balance snapshot so pages don't have to scan all CSV rows ---
    if (
        posted.status == "posted"
        and result.latest_balance_cents is not None
        and latest_balance_row is not None
    ):
        latest_date = latest_balance_row["_balance_date"].isoformat()
        try:
            from sales_support_agent.models.database import kv_set_json
            from sales_support_agent.services.cashflow.identity import assign_bank_identities
            identified_latest = assign_bank_identities(normalised_rows)
            latest_identity = next(
                (
                    row.get("source_id", "")
                    for row in identified_latest
                    if row.get("due_date") == latest_balance_row.get("due_date")
                    and row.get("account_balance_cents") == latest_balance_row.get("account_balance_cents")
                ),
                latest_balance_row.get("source_id", ""),
            )
            kv_set_json("balance_snapshot", {
                "balance_cents": result.latest_balance_cents,
                "as_of_date": latest_date,
                "source": "csv",
                "source_id": latest_identity,
                "bank_reference": latest_balance_row.get("bank_reference", ""),
                "source_row_index": latest_balance_row["_source_row_index"],
            })
        except Exception as _e:
            pass  # non-fatal; overview falls back to scanning CSV rows

    # -- Auto-match against planned obligations -----------------------------
    if new_events:
        planned = [
            row for row in list_obligations(limit=5000)
            if row.get("record_kind") != "transaction"
            and row.get("status") in ("planned", "pending", "overdue")
            and str(row.get("source_status") or "").lower() != "probable_duplicate"
            and str(row.get("match_status") or "").lower() != "duplicate"
        ]
        match_results = auto_match_transactions(new_events, planned)

        for mr in match_results:
            if mr.planned_event_id is None:
                if getattr(mr, "match_status", "") == "ambiguous":
                    with get_engine().begin() as conn:
                        conn.execute(text("""
                            UPDATE cash_events SET match_status='ambiguous',
                                match_candidates_json=:candidates, updated_at=:now
                            WHERE id=:id
                        """), {
                            "id": str(mr.csv_event_id),
                            "candidates": json.dumps(getattr(mr, "candidate_ids", []) or []),
                            "now": datetime.utcnow(),
                        })
                continue
            from sales_support_agent.services.cashflow.settlements import allocate_matched_transaction
            with get_engine().begin() as conn:
                allocate_matched_transaction(
                    conn,
                    obligation_event_id=str(mr.planned_event_id),
                    transaction_event_id=str(mr.csv_event_id),
                    idempotency_key=f"csv-auto-match:{mr.csv_event_id}:{mr.planned_event_id}",
                )
            result.matches_made += 1

    return result


# ---------------------------------------------------------------------------
# QBO Open Invoices upload path
# ---------------------------------------------------------------------------

def _run_qbo_open_invoices_upload(csv_bytes: bytes, *, engine) -> UploadResult:
    """Stage and atomically post a QBO Open Invoices Report CSV."""

    result = UploadResult()
    invoices = normalize_qbo_open_invoices_csv(csv_bytes)
    result.rows_read = len(invoices)

    if not invoices:
        result.errors.append("No open invoices found — check the file is a QBO Open Invoices Report export.")
        return result

    from sales_support_agent.services.cashflow.imports import stage_and_post_qbo_import

    posted = stage_and_post_qbo_import(
        engine,
        file_hash=hashlib.sha256(csv_bytes).hexdigest(),
        rows=[{"raw": row, "normalized": row} for row in invoices],
    )
    result.import_batch_id = posted.batch_id
    result.rows_inserted = posted.inserted
    result.rows_skipped_duplicate = posted.duplicates
    result.rows_skipped_invalid = posted.invalid
    result.rows_skipped_review = posted.review
    if posted.invalid:
        result.errors.append(
            f"{posted.invalid} invalid QBO invoice row(s). No records were committed from this file."
        )
    if posted.review:
        result.errors.append(
            f"{posted.review} changed QBO invoice row(s) need review. No records were committed from this file."
        )
    new_events = posted.new_events

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
                from sales_support_agent.services.cashflow.settlements import allocate_matched_transaction
                with engine.begin() as conn:
                    allocate_matched_transaction(
                        conn,
                        obligation_event_id=str(mr.planned_event_id),
                        transaction_event_id=str(mr.csv_event_id),
                        idempotency_key=f"qbo-csv-auto-match:{mr.csv_event_id}:{mr.planned_event_id}",
                    )
                result.matches_made += 1

    return result
