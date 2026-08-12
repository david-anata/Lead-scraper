"""Multi-bank account view: spendable cash and a per-account breakdown.

Spendable cash is deliberately narrow: only accounts the operator marks as
``spendable`` (checking by default) count toward the money available to pay
bills. Reserves remain owned cash. Liabilities are money owed and never inflate
cash. This module only reads balances; it never moves money.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from sqlalchemy import text

from sales_support_agent.models.database import get_engine

VALID_CASH_ROLES = ("spendable", "reserve", "liability", "excluded")


def _account_balance_cents(row: dict[str, Any]) -> int:
    """Available balance is preferred for cash decisions; current is fallback."""
    available = row.get("available_balance_cents")
    if available is not None:
        return int(available)
    current = row.get("current_balance_cents")
    return int(current) if current is not None else 0


def load_accounts_overview() -> dict[str, Any]:
    """Return spendable/reserve totals and accounts grouped by bank.

    Shape::

        {
          "spendable_cents": int,   # money available to pay bills today
          "reserve_cents": int,     # savings and other non-spendable balances
          "liability_cents": int,   # debt owed, never included in cash
          "as_of": str,             # ISO date of the freshest balance, or ""
          "account_count": int,
          "banks": [
            {"item_id", "display_name", "accounts": [ {account...} ]}
          ],
        }
    """
    with get_engine().connect() as connection:
        rows = connection.execute(text("""
            SELECT account.id, account.plaid_item_id, account.name, account.official_name,
                   account.mask, account.account_type, account.subtype, account.cash_role,
                   account.current_balance_cents, account.available_balance_cents,
                   account.balance_as_of,
                   item.display_name AS bank_name, item.created_at AS bank_created_at
            FROM plaid_accounts AS account
            JOIN plaid_items AS item ON item.id = account.plaid_item_id
            WHERE account.active = TRUE AND item.disconnected_at IS NULL
            ORDER BY item.created_at, account.name
        """)).fetchall()

    spendable_cents = 0
    reserve_cents = 0
    liability_cents = 0
    as_of_values: list[str] = []
    banks: dict[str, dict[str, Any]] = {}

    for raw in rows:
        row = dict(raw._mapping)
        balance = _account_balance_cents(row)
        role = str(row.get("cash_role") or "reserve")
        account_name = str(row.get("name") or row.get("official_name") or "Account")
        # Tax cash is a last-resort reserve, never ordinary bill-paying money.
        # Protect it even when an old/default Plaid classification says checking.
        tax_protected = "tax" in account_name.strip().lower()
        if tax_protected and role == "spendable":
            role = "reserve"
        if role == "spendable":
            spendable_cents += balance
        elif role == "reserve":
            reserve_cents += balance
        elif role == "liability":
            liability_cents += abs(balance)
        # 'liability' and 'excluded' contribute no cash.

        as_of = str(row.get("balance_as_of") or "")[:10]
        if as_of:
            as_of_values.append(as_of)

        item_id = str(row.get("plaid_item_id"))
        bank = banks.setdefault(item_id, {
            "item_id": item_id,
            "display_name": str(row.get("bank_name") or "Connected bank"),
            "accounts": [],
        })
        bank["accounts"].append({
            "id": str(row.get("id")),
            "name": account_name,
            "mask": str(row.get("mask") or ""),
            "subtype": str(row.get("subtype") or ""),
            "account_type": str(row.get("account_type") or ""),
            "cash_role": role,
            "tax_protected": tax_protected,
            "balance_cents": balance,
            "current_balance_cents": row.get("current_balance_cents"),
            "available_balance_cents": row.get("available_balance_cents"),
            "as_of": as_of,
        })

    return {
        "spendable_cents": spendable_cents,
        "reserve_cents": reserve_cents,
        "liability_cents": liability_cents,
        "as_of": max(as_of_values) if as_of_values else "",
        "account_count": len(rows),
        "banks": list(banks.values()),
    }


def spendable_cash_cents() -> int:
    """Convenience accessor: total spendable checking across all banks."""
    return int(load_accounts_overview()["spendable_cents"])


def set_cash_role(account_id: str, role: str, *, actor: str = "system") -> str:
    """Reclassify one account's cash role and append an audit record.

    Returns the applied role. Raises ValueError for an unknown role or account.
    """
    role = str(role or "").strip().lower()
    if role not in VALID_CASH_ROLES:
        raise ValueError(f"cash_role must be one of {VALID_CASH_ROLES}")
    now = datetime.now(timezone.utc)
    with get_engine().begin() as connection:
        existing = connection.execute(
            text("SELECT cash_role FROM plaid_accounts WHERE id=:id"),
            {"id": account_id},
        ).fetchone()
        if existing is None:
            raise ValueError("account not found")
        previous = str(existing._mapping.get("cash_role") or "reserve")
        connection.execute(
            text("UPDATE plaid_accounts SET cash_role=:role, updated_at=:now WHERE id=:id"),
            {"role": role, "now": now, "id": account_id},
        )
        connection.execute(text("""
            INSERT INTO finance_action_audit (
                id, scope_key, action_type, entity_type, entity_id, actor,
                evidence_json, created_at
            ) VALUES (
                :audit_id, 'default', 'plaid_account_reclassify', 'plaid_account',
                :account_id, :actor, :evidence, :now
            )
        """), {
            "audit_id": str(uuid4()), "account_id": account_id, "actor": actor or "system",
            "evidence": json.dumps({"from": previous, "to": role}), "now": now,
        })
    return role
