"""CSV bank statement upload — parse, categorise, dedup, auto-match."""

from __future__ import annotations

import csv
import io
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from sales_support_agent.services.cashflow.matcher import auto_match_transactions
from sales_support_agent.services.cashflow.normalizers import normalize_bank_csv_row
from sales_support_agent.services.cashflow.obligations import list_obligations


@dataclass
class UploadResult:
    rows_read: int = 0
    rows_inserted: int = 0
    rows_skipped_duplicate: int = 0
    rows_skipped_invalid: int = 0
    matches_made: int = 0
    errors: list[str] = field(default_factory=list)
    latest_balance_cents: int | None = None

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
    from sales_support_agent.models.database import engine
    from sqlalchemy import text

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
            with engine.begin() as conn:
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
    with engine.connect() as conn:
        existing_ids: set[str] = {
            row[0]
            for row in conn.execute(
                text("SELECT source_id FROM cash_events WHERE source = 'csv'")
            ).fetchall()
        }

    # -- Insert new rows ----------------------------------------------------
    now = datetime.utcnow().isoformat()
    new_events: list[dict[str, Any]] = []

    for row in normalised_rows:
        source_id = row.get("source_id", "")
        if source_id in existing_ids:
            result.rows_skipped_duplicate += 1
            continue

        event_id = str(uuid.uuid4())
        due_date = row.get("due_date")
        due_date_str = (
            due_date.isoformat()
            if hasattr(due_date, "isoformat")
            else str(due_date)[:10] if due_date else None
        )

        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO cash_events (
                        id, source, source_id, event_type, category,
                        name, vendor_or_customer, amount_cents,
                        due_date, status, confidence,
                        account_balance_cents,
                        created_at, updated_at
                    ) VALUES (
                        :id, 'csv', :source_id, :event_type, :category,
                        :name, :vendor_or_customer, :amount_cents,
                        :due_date, 'posted', 'confirmed',
                        :account_balance_cents,
                        :now, :now
                    )
                """),
                {
                    "id": event_id,
                    "source_id": source_id,
                    "event_type": row.get("event_type", "outflow"),
                    "category": row.get("category", "other"),
                    "name": row.get("name", ""),
                    "vendor_or_customer": row.get("vendor_or_customer", ""),
                    "amount_cents": row.get("amount_cents", 0),
                    "due_date": due_date_str,
                    "account_balance_cents": row.get("account_balance_cents"),
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
            with engine.begin() as conn:
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
