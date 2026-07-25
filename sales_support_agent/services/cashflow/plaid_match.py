"""Match real bank payments to the bills they settled.

The QuickBooks bank sync has always run auto-matching after import; the Plaid
sync did not, so real bank payments landed in the ledger without ever being
connected to the obligation they paid. That is the main reason obligations sit
in "no matching bank payment found yet".

This module proposes matches (no writes), confirms a chosen set as settlement
allocations grouped into an undoable run, and reverses a run. Protected
commitments (payroll, tax, debt) are never matched automatically on sync; they
require an explicit confirmation.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional
from uuid import uuid4

from sqlalchemy import text

from sales_support_agent.models.database import get_engine

logger = logging.getLogger(__name__)

HIGH_CONFIDENCE_BPS = 9_000
PROTECTED_TYPES = {"payroll", "tax", "debt"}
_OPEN_OBLIGATION_STATUSES = {"planned", "pending", "overdue", "completed"}


def _confidence_tier(score_bps: int) -> str:
    return "high" if score_bps >= HIGH_CONFIDENCE_BPS else "medium"


def _is_protected(row: dict[str, Any]) -> bool:
    return str(row.get("commitment_type") or "").lower() in PROTECTED_TYPES


def _open_obligations(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Obligations a posted bank payment may settle (mirrors the QBO sync)."""
    return [
        row for row in rows
        if row.get("record_kind") != "transaction"
        and str(row.get("status") or "").lower() in _OPEN_OBLIGATION_STATUSES
        and str(row.get("source_status") or "").lower() != "probable_duplicate"
        and str(row.get("match_status") or "").lower() != "duplicate"
        and int(row.get("amount_cents") or 0) > 0
        and not row.get("archived_at")
    ]


def _unsettled_plaid_transactions(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Posted Plaid transactions that are not already allocated to a bill."""
    candidates = [
        row for row in rows
        if str(row.get("source") or "").lower() == "plaid"
        and row.get("record_kind") == "transaction"
        and str(row.get("status") or "").lower() == "posted"
        and int(row.get("amount_cents") or 0) > 0
    ]
    if not candidates:
        return []
    with get_engine().connect() as connection:
        allocated = {
            str(row._mapping["transaction_event_id"])
            for row in connection.execute(text("""
                SELECT DISTINCT transaction_event_id
                FROM settlement_allocations
                WHERE transaction_event_id IS NOT NULL
            """)).fetchall()
        }
    return [row for row in candidates if str(row.get("id")) not in allocated]


def propose_matches(*, limit: int = 200) -> list[dict[str, Any]]:
    """Return confident, unwritten match proposals for operator review."""
    from sales_support_agent.services.cashflow.matcher import auto_match_transactions
    from sales_support_agent.services.cashflow.obligations import list_obligations

    rows = list_obligations(limit=5000)
    transactions = _unsettled_plaid_transactions(rows)
    obligations = _open_obligations(rows)
    if not transactions or not obligations:
        return []

    by_id = {str(row.get("id")): row for row in rows}
    proposals: list[dict[str, Any]] = []
    for result in auto_match_transactions(transactions, obligations):
        if result.planned_event_id is None:
            continue  # ambiguous/unmatched surface in the review list instead
        obligation = by_id.get(str(result.planned_event_id)) or {}
        transaction = by_id.get(str(result.csv_event_id)) or {}
        proposals.append({
            "transaction_id": str(result.csv_event_id),
            "obligation_id": str(result.planned_event_id),
            "score_bps": int(result.score_bps),
            "confidence": _confidence_tier(int(result.score_bps)),
            "reason": str(result.reason),
            "protected": _is_protected(obligation),
            "obligation_name": str(obligation.get("name") or obligation.get("vendor_or_customer") or "Bill"),
            "obligation_amount_cents": int(obligation.get("amount_cents") or 0),
            "obligation_due_date": str(obligation.get("due_date") or "")[:10],
            "transaction_name": str(transaction.get("name") or transaction.get("vendor_or_customer") or "Payment"),
            "transaction_amount_cents": int(transaction.get("amount_cents") or 0),
            "transaction_date": str(transaction.get("due_date") or transaction.get("effective_date") or "")[:10],
        })
    proposals.sort(key=lambda item: -item["score_bps"])
    return proposals[:limit]


def confirm_matches(
    pairs: list[tuple[str, str]], *, actor: str = "system", trigger: str = "manual",
) -> dict[str, Any]:
    """Allocate the given (transaction, obligation) pairs as one undoable run."""
    from sales_support_agent.services.cashflow.settlements import allocate_matched_transaction

    if not pairs:
        return {"run_id": "", "confirmed": 0, "failed": 0, "errors": []}

    run_id = str(uuid4())
    now = datetime.now(timezone.utc)
    confirmed = 0
    errors: list[str] = []
    engine = get_engine()
    with engine.begin() as connection:
        connection.execute(text("""
            INSERT INTO finance_match_runs (
                id, scope_key, trigger, actor, proposed_count, confirmed_count, created_at
            ) VALUES (:id, 'default', :trigger, :actor, :proposed, 0, :now)
        """), {"id": run_id, "trigger": trigger, "actor": actor or "system",
               "proposed": len(pairs), "now": now})

    for transaction_id, obligation_id in pairs:
        try:
            with engine.begin() as connection:
                allocation = allocate_matched_transaction(
                    connection,
                    obligation_event_id=obligation_id,
                    transaction_event_id=transaction_id,
                    idempotency_key=f"plaid-match:{transaction_id}:{obligation_id}",
                    notes="Matched Plaid bank payment to obligation",
                )
                connection.execute(text("""
                    INSERT INTO finance_match_run_items (
                        id, run_id, allocation_id, obligation_event_id,
                        transaction_event_id, score_bps, created_at
                    ) VALUES (:id, :run, :alloc, :oblig, :txn, 0, :now)
                """), {"id": str(uuid4()), "run": run_id,
                       "alloc": str(allocation.get("id") or ""),
                       "oblig": obligation_id, "txn": transaction_id, "now": now})
            confirmed += 1
        except Exception as exc:
            errors.append(f"{transaction_id}: {exc}")
            logger.warning("Plaid match failed txn=%s obligation=%s: %s", transaction_id, obligation_id, exc)

    with engine.begin() as connection:
        connection.execute(
            text("UPDATE finance_match_runs SET confirmed_count=:count WHERE id=:id"),
            {"count": confirmed, "id": run_id},
        )
    return {"run_id": run_id, "confirmed": confirmed, "failed": len(errors), "errors": errors}


def auto_match_on_sync(*, actor: str = "system") -> dict[str, Any]:
    """Confirm only high-confidence, non-protected matches after a bank sync."""
    proposals = [
        p for p in propose_matches()
        if p["confidence"] == "high" and not p["protected"]
    ]
    if not proposals:
        return {"run_id": "", "confirmed": 0, "failed": 0, "errors": []}
    pairs = [(p["transaction_id"], p["obligation_id"]) for p in proposals]
    return confirm_matches(pairs, actor=actor, trigger="sync")


def latest_run() -> Optional[dict[str, Any]]:
    """The most recent match run, for the undo affordance."""
    with get_engine().connect() as connection:
        row = connection.execute(text("""
            SELECT * FROM finance_match_runs
            WHERE undone_at IS NULL AND confirmed_count > 0
            ORDER BY created_at DESC LIMIT 1
        """)).fetchone()
    return dict(row._mapping) if row else None


def undo_run(run_id: str, *, actor: str = "system") -> dict[str, Any]:
    """Reverse every allocation made by a run, restoring the prior state."""
    from sales_support_agent.services.cashflow.settlements import reverse_settlement_allocation

    now = datetime.now(timezone.utc)
    with get_engine().connect() as connection:
        items = [
            dict(row._mapping) for row in connection.execute(text("""
                SELECT * FROM finance_match_run_items
                WHERE run_id=:run AND reversed_at IS NULL
            """), {"run": run_id}).fetchall()
        ]
    reversed_count = 0
    errors: list[str] = []
    for item in items:
        allocation_id = str(item.get("allocation_id") or "")
        if not allocation_id:
            continue
        try:
            reverse_settlement_allocation(
                allocation_id,
                idempotency_key=f"plaid-match-undo:{run_id}:{allocation_id}",
                source="plaid_match_undo",
                notes="Reversed an auto-matched bank payment",
            )
            with get_engine().begin() as connection:
                connection.execute(
                    text("UPDATE finance_match_run_items SET reversed_at=:now WHERE id=:id"),
                    {"now": now, "id": str(item["id"])},
                )
            reversed_count += 1
        except Exception as exc:
            errors.append(f"{allocation_id}: {exc}")
            logger.warning("Undo of match allocation %s failed: %s", allocation_id, exc)
    with get_engine().begin() as connection:
        connection.execute(
            text("UPDATE finance_match_runs SET undone_at=:now WHERE id=:id"),
            {"now": now, "id": run_id},
        )
    return {"reversed": reversed_count, "failed": len(errors), "errors": errors}
