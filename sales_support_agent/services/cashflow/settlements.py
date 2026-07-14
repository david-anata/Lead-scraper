"""Durable, idempotent settlement and installment operations.

Allocations are append-only. A reversal records correction evidence and makes
the referenced allocation inactive; financial history is never deleted.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Iterable, Mapping, Optional
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.engine import Connection

from sales_support_agent.models.database import get_engine


_ACTIVE_ALLOCATION_FILTER = """
    allocation.reversed_allocation_id IS NULL
    AND NOT EXISTS (
        SELECT 1
        FROM settlement_allocations AS reversal
        WHERE reversal.reversed_allocation_id = allocation.id
    )
"""


def _as_dict(row: Any) -> dict[str, Any]:
    return dict(row._mapping)


def _utc_datetime(value: date | datetime | None, *, default_now: bool = False) -> Optional[datetime]:
    if value is None:
        return datetime.now(timezone.utc) if default_now else None
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    return datetime.combine(value, time.min, tzinfo=timezone.utc)


def _same_day(left: Any, right: date | datetime) -> bool:
    left_day = left.date() if isinstance(left, datetime) else date.fromisoformat(str(left)[:10])
    right_day = right.date() if isinstance(right, datetime) else right
    return left_day == right_day


def _lock_suffix(connection: Connection) -> str:
    return " FOR UPDATE" if connection.dialect.name == "postgresql" else ""


def _get_cash_event(connection: Connection, event_id: str, *, lock: bool = False) -> dict[str, Any]:
    suffix = _lock_suffix(connection) if lock else ""
    row = connection.execute(
        text(f"SELECT * FROM cash_events WHERE id = :id{suffix}"),  # noqa: S608
        {"id": event_id},
    ).fetchone()
    if row is None:
        raise ValueError(f"Cash event not found: {event_id}")
    return _as_dict(row)


def _get_installment(connection: Connection, installment_id: str, *, lock: bool = False) -> dict[str, Any]:
    suffix = _lock_suffix(connection) if lock else ""
    row = connection.execute(
        text(f"SELECT * FROM payment_installments WHERE id = :id{suffix}"),  # noqa: S608
        {"id": installment_id},
    ).fetchone()
    if row is None:
        raise ValueError(f"Payment installment not found: {installment_id}")
    return _as_dict(row)


def _settled_amount(connection: Connection, obligation_event_id: str) -> int:
    return int(
        connection.execute(
            text(f"""
                SELECT COALESCE(SUM(allocation.amount_cents), 0)
                FROM settlement_allocations AS allocation
                WHERE allocation.obligation_event_id = :obligation_event_id
                  AND {_ACTIVE_ALLOCATION_FILTER}
            """),
            {"obligation_event_id": obligation_event_id},
        ).scalar_one()
        or 0
    )


def _open_balance(connection: Connection, obligation_event_id: str) -> int:
    event = _get_cash_event(connection, obligation_event_id)
    return max(int(event.get("amount_cents") or 0) - _settled_amount(connection, obligation_event_id), 0)


def get_settled_amount_cents(obligation_event_id: str) -> int:
    """Return active allocated cents for one obligation."""

    with get_engine().connect() as connection:
        _get_cash_event(connection, obligation_event_id)
        return _settled_amount(connection, obligation_event_id)


def get_open_balance_cents(obligation_event_id: str) -> int:
    """Return ``max(face amount - active allocations, 0)`` for an obligation."""

    with get_engine().connect() as connection:
        return _open_balance(connection, obligation_event_id)


def get_open_balances(obligation_event_ids: Iterable[str]) -> dict[str, int]:
    """Return open balances while preserving the requested event IDs."""

    ids = list(dict.fromkeys(obligation_event_ids))
    with get_engine().connect() as connection:
        return {event_id: _open_balance(connection, event_id) for event_id in ids}


def get_scheduled_amount_cents(
    obligation_event_id: str,
    *,
    from_date: date | datetime | None = None,
    to_date: date | datetime | None = None,
) -> int:
    """Return planned installment cents in the optional inclusive date window."""

    clauses = ["obligation_event_id = :obligation_event_id", "status = 'planned'"]
    params: dict[str, Any] = {"obligation_event_id": obligation_event_id}
    if from_date is not None:
        clauses.append("due_date >= :from_date")
        params["from_date"] = _utc_datetime(from_date)
    if to_date is not None:
        clauses.append("due_date <= :to_date")
        params["to_date"] = _utc_datetime(to_date)
    with get_engine().connect() as connection:
        _get_cash_event(connection, obligation_event_id)
        return int(
            connection.execute(
                text(f"SELECT COALESCE(SUM(amount_cents), 0) FROM payment_installments WHERE {' AND '.join(clauses)}"),  # noqa: S608
                params,
            ).scalar_one()
            or 0
        )


def create_payment_installment(
    *,
    obligation_event_id: str,
    amount_cents: int,
    due_date: date | datetime,
    idempotency_key: str,
) -> dict[str, Any]:
    """Create one planned installment, returning the prior row on a safe retry."""

    if amount_cents <= 0:
        raise ValueError("Installment amount_cents must be positive")
    if not idempotency_key.strip():
        raise ValueError("idempotency_key is required")
    due_at = _utc_datetime(due_date)
    now = datetime.now(timezone.utc)

    with get_engine().begin() as connection:
        existing = connection.execute(
            text("SELECT * FROM payment_installments WHERE idempotency_key = :key"),
            {"key": idempotency_key},
        ).fetchone()
        if existing is not None:
            result = _as_dict(existing)
            if (
                result["obligation_event_id"] != obligation_event_id
                or int(result["amount_cents"]) != amount_cents
                or not _same_day(result["due_date"], due_date)
            ):
                raise ValueError("idempotency_key is already used for a different installment")
            return result

        obligation = _get_cash_event(connection, obligation_event_id, lock=True)
        existing = connection.execute(
            text("SELECT * FROM payment_installments WHERE idempotency_key = :key"),
            {"key": idempotency_key},
        ).fetchone()
        if existing is not None:
            result = _as_dict(existing)
            if (
                result["obligation_event_id"] != obligation_event_id
                or int(result["amount_cents"]) != amount_cents
                or not _same_day(result["due_date"], due_date)
            ):
                raise ValueError("idempotency_key is already used for a different installment")
            return result
        if obligation.get("record_kind") == "transaction":
            raise ValueError("Installments can only be created for obligations")
        if obligation.get("status") == "cancelled":
            raise ValueError("Cannot schedule a cancelled obligation")

        scheduled = int(
            connection.execute(
                text("""
                    SELECT COALESCE(SUM(amount_cents), 0)
                    FROM payment_installments
                    WHERE obligation_event_id = :id AND status = 'planned'
                """),
                {"id": obligation_event_id},
            ).scalar_one()
            or 0
        )
        if scheduled + amount_cents > _open_balance(connection, obligation_event_id):
            raise ValueError("Planned installments cannot exceed the obligation open balance")

        installment_id = str(uuid4())
        connection.execute(
            text("""
                INSERT INTO payment_installments (
                    id, obligation_event_id, amount_cents, due_date, status,
                    idempotency_key, created_at, updated_at
                ) VALUES (
                    :id, :obligation_event_id, :amount_cents, :due_date, 'planned',
                    :idempotency_key, :created_at, :updated_at
                )
            """),
            {
                "id": installment_id,
                "obligation_event_id": obligation_event_id,
                "amount_cents": amount_cents,
                "due_date": due_at,
                "idempotency_key": idempotency_key,
                "created_at": now,
                "updated_at": now,
            },
        )
        return _get_installment(connection, installment_id)


def cancel_payment_installment(installment_id: str) -> dict[str, Any]:
    """Cancel an unpaid installment; repeated cancellation is a no-op."""

    with get_engine().begin() as connection:
        installment = _get_installment(connection, installment_id, lock=True)
        if installment["status"] == "paid":
            raise ValueError("A paid installment cannot be cancelled")
        if installment["status"] != "cancelled":
            connection.execute(
                text("UPDATE payment_installments SET status = 'cancelled', updated_at = :now WHERE id = :id"),
                {"id": installment_id, "now": datetime.now(timezone.utc)},
            )
        return _get_installment(connection, installment_id)


def _allocation_by_key(connection: Connection, idempotency_key: str) -> Optional[dict[str, Any]]:
    row = connection.execute(
        text("SELECT * FROM settlement_allocations WHERE idempotency_key = :key"),
        {"key": idempotency_key},
    ).fetchone()
    return _as_dict(row) if row is not None else None


def _refresh_installment_status(connection: Connection, installment_id: Optional[str]) -> None:
    if not installment_id:
        return
    installment = _get_installment(connection, installment_id)
    if installment["status"] == "cancelled":
        return
    allocated = int(
        connection.execute(
            text(f"""
                SELECT COALESCE(SUM(allocation.amount_cents), 0)
                FROM settlement_allocations AS allocation
                WHERE allocation.installment_id = :installment_id
                  AND {_ACTIVE_ALLOCATION_FILTER}
            """),
            {"installment_id": installment_id},
        ).scalar_one()
        or 0
    )
    status = "paid" if allocated >= int(installment["amount_cents"]) else "planned"
    connection.execute(
        text("UPDATE payment_installments SET status = :status, updated_at = :now WHERE id = :id"),
        {"id": installment_id, "status": status, "now": datetime.now(timezone.utc)},
    )


def _open_status(obligation: Mapping[str, Any]) -> str:
    due_value = obligation.get("due_date")
    if not due_value:
        return "planned"
    due_day = due_value.date() if isinstance(due_value, datetime) else date.fromisoformat(str(due_value)[:10])
    today = datetime.now(timezone.utc).date()
    if due_day < today:
        return "overdue"
    return "pending" if due_day <= today + timedelta(days=7) else "planned"


def _refresh_obligation_status(connection: Connection, obligation_event_id: str) -> None:
    obligation = _get_cash_event(connection, obligation_event_id)
    open_balance = _open_balance(connection, obligation_event_id)
    if open_balance == 0:
        status = "paid"
    elif obligation.get("status") in {"paid", "matched"}:
        status = _open_status(obligation)
    else:
        return
    connection.execute(
        text("UPDATE cash_events SET status = :status, updated_at = :now WHERE id = :id"),
        {"id": obligation_event_id, "status": status, "now": datetime.now(timezone.utc)},
    )


def create_settlement_allocation(
    *,
    obligation_event_id: str,
    amount_cents: int,
    idempotency_key: str,
    transaction_event_id: str | None = None,
    installment_id: str | None = None,
    allocation_date: date | datetime | None = None,
    source: str = "manual",
    confidence: str = "confirmed",
    notes: str = "",
) -> dict[str, Any]:
    """Append an allocation without allowing obligation or transaction over-allocation."""

    if amount_cents <= 0:
        raise ValueError("Allocation amount_cents must be positive")
    if not idempotency_key.strip():
        raise ValueError("idempotency_key is required")
    allocated_at = _utc_datetime(allocation_date, default_now=True)
    if allocated_at is not None and allocated_at.date() > datetime.now(timezone.utc).date():
        raise ValueError("A settlement allocation cannot be future-dated")

    with get_engine().begin() as connection:
        existing = _allocation_by_key(connection, idempotency_key)
        if existing is not None:
            expected = (obligation_event_id, transaction_event_id, installment_id, amount_cents)
            actual = (
                existing["obligation_event_id"],
                existing["transaction_event_id"],
                existing["installment_id"],
                int(existing["amount_cents"]),
            )
            if actual != expected or existing["reversed_allocation_id"] is not None:
                raise ValueError("idempotency_key is already used for a different allocation")
            return existing

        obligation = _get_cash_event(connection, obligation_event_id, lock=True)
        existing = _allocation_by_key(connection, idempotency_key)
        if existing is not None:
            expected = (obligation_event_id, transaction_event_id, installment_id, amount_cents)
            actual = (
                existing["obligation_event_id"],
                existing["transaction_event_id"],
                existing["installment_id"],
                int(existing["amount_cents"]),
            )
            if actual != expected or existing["reversed_allocation_id"] is not None:
                raise ValueError("idempotency_key is already used for a different allocation")
            return existing
        if obligation.get("record_kind") == "transaction":
            raise ValueError("Settlement target must be an obligation")
        if obligation.get("status") == "cancelled":
            raise ValueError("Cannot settle a cancelled obligation")
        if amount_cents > _open_balance(connection, obligation_event_id):
            raise ValueError("Allocation exceeds the obligation open balance")

        if transaction_event_id:
            transaction = _get_cash_event(connection, transaction_event_id, lock=True)
            if transaction.get("record_kind") != "transaction":
                raise ValueError("transaction_event_id must reference a transaction")
            if transaction.get("event_type") != obligation.get("event_type"):
                raise ValueError("Transaction and obligation cash directions must match")
            used = int(
                connection.execute(
                    text(f"""
                        SELECT COALESCE(SUM(allocation.amount_cents), 0)
                        FROM settlement_allocations AS allocation
                        WHERE allocation.transaction_event_id = :id
                          AND {_ACTIVE_ALLOCATION_FILTER}
                    """),
                    {"id": transaction_event_id},
                ).scalar_one()
                or 0
            )
            if used + amount_cents > int(transaction.get("amount_cents") or 0):
                raise ValueError("Allocation exceeds the transaction unallocated balance")

        if installment_id:
            installment = _get_installment(connection, installment_id, lock=True)
            if installment["obligation_event_id"] != obligation_event_id:
                raise ValueError("Installment belongs to a different obligation")
            if installment["status"] == "cancelled":
                raise ValueError("Cannot settle a cancelled installment")
            installment_allocated = int(
                connection.execute(
                    text(f"""
                        SELECT COALESCE(SUM(allocation.amount_cents), 0)
                        FROM settlement_allocations AS allocation
                        WHERE allocation.installment_id = :id
                          AND {_ACTIVE_ALLOCATION_FILTER}
                    """),
                    {"id": installment_id},
                ).scalar_one()
                or 0
            )
            if installment_allocated + amount_cents > int(installment["amount_cents"]):
                raise ValueError("Allocation exceeds the installment open amount")

        allocation_id = str(uuid4())
        connection.execute(
            text("""
                INSERT INTO settlement_allocations (
                    id, obligation_event_id, transaction_event_id, installment_id,
                    amount_cents, allocation_date, source, confidence,
                    idempotency_key, reversed_allocation_id, notes, created_at
                ) VALUES (
                    :id, :obligation_event_id, :transaction_event_id, :installment_id,
                    :amount_cents, :allocation_date, :source, :confidence,
                    :idempotency_key, NULL, :notes, :created_at
                )
            """),
            {
                "id": allocation_id,
                "obligation_event_id": obligation_event_id,
                "transaction_event_id": transaction_event_id,
                "installment_id": installment_id,
                "amount_cents": amount_cents,
                "allocation_date": allocated_at,
                "source": source,
                "confidence": confidence,
                "idempotency_key": idempotency_key,
                "notes": notes,
                "created_at": datetime.now(timezone.utc),
            },
        )
        _refresh_installment_status(connection, installment_id)
        _refresh_obligation_status(connection, obligation_event_id)
        return _allocation_by_key(connection, idempotency_key) or {}


def allocate_matched_transaction(
    connection: Connection,
    *,
    obligation_event_id: str,
    transaction_event_id: str,
    idempotency_key: str,
    notes: str = "Auto-matched posted transaction",
) -> dict[str, Any]:
    """Allocate one matched actual inside the caller's source-sync transaction."""
    if not idempotency_key.strip():
        raise ValueError("idempotency_key is required")
    existing = _allocation_by_key(connection, idempotency_key)
    if existing is not None:
        return existing

    obligation = _get_cash_event(connection, obligation_event_id, lock=True)
    transaction = _get_cash_event(connection, transaction_event_id, lock=True)
    if obligation.get("record_kind") == "transaction":
        raise ValueError("Settlement target must be an obligation")
    if transaction.get("record_kind") != "transaction":
        raise ValueError("Matched source must be a posted transaction")
    if transaction.get("event_type") != obligation.get("event_type"):
        raise ValueError("Transaction and obligation cash directions must match")

    transaction_used = int(connection.execute(text(f"""
        SELECT COALESCE(SUM(allocation.amount_cents), 0)
        FROM settlement_allocations AS allocation
        WHERE allocation.transaction_event_id = :id
          AND {_ACTIVE_ALLOCATION_FILTER}
    """), {"id": transaction_event_id}).scalar_one() or 0)
    transaction_open = max(int(transaction.get("amount_cents") or 0) - transaction_used, 0)
    amount_cents = min(transaction_open, _open_balance(connection, obligation_event_id))
    if amount_cents <= 0:
        raise ValueError("Matched transaction has no allocatable balance")

    allocation_id = str(uuid4())
    now = datetime.now(timezone.utc)
    connection.execute(text("""
        INSERT INTO settlement_allocations (
            id, obligation_event_id, transaction_event_id, installment_id,
            amount_cents, allocation_date, source, confidence,
            idempotency_key, reversed_allocation_id, notes, created_at
        ) VALUES (
            :id, :obligation_event_id, :transaction_event_id, NULL,
            :amount_cents, :allocation_date, 'auto_match', 'confirmed',
            :idempotency_key, NULL, :notes, :created_at
        )
    """), {
        "id": allocation_id,
        "obligation_event_id": obligation_event_id,
        "transaction_event_id": transaction_event_id,
        "amount_cents": amount_cents,
        "allocation_date": transaction.get("effective_date") or transaction.get("due_date") or now,
        "idempotency_key": idempotency_key,
        "notes": notes,
        "created_at": now,
    })
    connection.execute(text("""
        UPDATE cash_events
        SET status = 'matched', matched_to_id = :obligation_id, updated_at = :now
        WHERE id = :transaction_id
    """), {
        "obligation_id": obligation_event_id,
        "transaction_id": transaction_event_id,
        "now": now,
    })
    _refresh_obligation_status(connection, obligation_event_id)
    return _allocation_by_key(connection, idempotency_key) or {}


def reverse_settlement_allocation(
    allocation_id: str,
    *,
    idempotency_key: str,
    allocation_date: date | datetime | None = None,
    source: str = "manual",
    notes: str = "",
) -> dict[str, Any]:
    """Append one reversal record and reopen derived balances/statuses."""

    if not idempotency_key.strip():
        raise ValueError("idempotency_key is required")
    with get_engine().begin() as connection:
        existing = _allocation_by_key(connection, idempotency_key)
        if existing is not None:
            if existing["reversed_allocation_id"] != allocation_id:
                raise ValueError("idempotency_key is already used for a different reversal")
            return existing

        suffix = _lock_suffix(connection)
        row = connection.execute(
            text(f"SELECT * FROM settlement_allocations WHERE id = :id{suffix}"),  # noqa: S608
            {"id": allocation_id},
        ).fetchone()
        if row is None:
            raise ValueError(f"Settlement allocation not found: {allocation_id}")
        original = _as_dict(row)
        if original["reversed_allocation_id"] is not None:
            raise ValueError("A reversal allocation cannot itself be reversed")
        prior_reversal = connection.execute(
            text("SELECT * FROM settlement_allocations WHERE reversed_allocation_id = :id"),
            {"id": allocation_id},
        ).fetchone()
        if prior_reversal is not None:
            raise ValueError("Allocation has already been reversed")

        reversal_id = str(uuid4())
        connection.execute(
            text("""
                INSERT INTO settlement_allocations (
                    id, obligation_event_id, transaction_event_id, installment_id,
                    amount_cents, allocation_date, source, confidence,
                    idempotency_key, reversed_allocation_id, notes, created_at
                ) VALUES (
                    :id, :obligation_event_id, :transaction_event_id, :installment_id,
                    :amount_cents, :allocation_date, :source, :confidence,
                    :idempotency_key, :reversed_allocation_id, :notes, :created_at
                )
            """),
            {
                "id": reversal_id,
                "obligation_event_id": original["obligation_event_id"],
                "transaction_event_id": original["transaction_event_id"],
                "installment_id": original["installment_id"],
                "amount_cents": original["amount_cents"],
                "allocation_date": _utc_datetime(allocation_date, default_now=True),
                "source": source,
                "confidence": original["confidence"],
                "idempotency_key": idempotency_key,
                "reversed_allocation_id": allocation_id,
                "notes": notes,
                "created_at": datetime.now(timezone.utc),
            },
        )
        _refresh_installment_status(connection, original["installment_id"])
        _refresh_obligation_status(connection, original["obligation_event_id"])
        return _allocation_by_key(connection, idempotency_key) or {}


# Short aliases for call sites that do not need the storage terminology.
create_installment = create_payment_installment
cancel_installment = cancel_payment_installment
create_allocation = create_settlement_allocation
allocate_match = allocate_matched_transaction
reverse_allocation = reverse_settlement_allocation
