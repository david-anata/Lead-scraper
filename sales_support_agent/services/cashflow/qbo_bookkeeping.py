"""Guarded QuickBooks expense-category write-back.

Only an operator-confirmed, single-line Purchase that is still booked to an
uncategorized/suspense account is eligible. The existing Purchase is read back
from QuickBooks immediately before the update, so stale or already-corrected
records stop instead of being overwritten.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

import requests
from sqlalchemy import text

from sales_support_agent.models.database import get_engine
from sales_support_agent.services.cashflow.qbo_bank_sync import (
    QBO_PROD_BASE,
    QBO_SAND_BASE,
    _category_from_account,
    _qbo_query_all,
)


def _connection(settings: Any) -> tuple[str, str, str]:
    """Return base URL, realm and a fresh access token."""
    from sales_support_agent.api.qbo_auth_router import _load_tokens, get_valid_access_token

    token_row = _load_tokens()
    if not token_row or not token_row.get("realm_id"):
        raise ValueError("QuickBooks is not connected.")
    token = get_valid_access_token() or ""
    if not token:
        raise ValueError("QuickBooks needs to be reconnected.")
    base = QBO_SAND_BASE if bool(getattr(settings, "qbo_sandbox", False)) else QBO_PROD_BASE
    return base, str(token_row["realm_id"]), token


def list_expense_accounts(settings: Any) -> list[dict[str, str]]:
    """Expense/COGS accounts an operator may deliberately choose."""
    base, realm, token = _connection(settings)
    rows = _qbo_query_all(
        base, realm, token,
        "SELECT * FROM Account WHERE Active = true MAXRESULTS 1000",
    )
    allowed = {"Expense", "Cost of Goods Sold", "Other Expense"}
    result = [
        {"id": str(row.get("Id") or ""), "name": str(row.get("FullyQualifiedName") or row.get("Name") or "")}
        for row in rows if str(row.get("AccountType") or "") in allowed
    ]
    return sorted((row for row in result if row["id"] and row["name"]), key=lambda row: row["name"].lower())


def _local_event(event_id: str) -> dict[str, Any]:
    with get_engine().connect() as connection:
        row = connection.execute(text("""
            SELECT id, source, source_id, name, description, amount_cents, category,
                   subcategory, bank_reference, event_type
            FROM cash_events WHERE id=:id
        """), {"id": event_id}).fetchone()
    if row is None:
        raise ValueError("Transaction not found.")
    event = dict(row._mapping)
    if event.get("source") != "qbo_bank" or not str(event.get("source_id") or "").startswith("purchase-"):
        raise ValueError("Only QuickBooks purchases can be sent back.")
    if str(event.get("event_type") or "") != "outflow":
        raise ValueError("Only money going out belongs in expense bookkeeping.")
    return event


def preview_writeback(event_id: str, settings: Any) -> dict[str, Any]:
    """Read the latest QBO Purchase and prove it is safe to change."""
    event = _local_event(event_id)
    base, realm, token = _connection(settings)
    qbo_id = str(event["bank_reference"])
    response = requests.get(
        f"{base}/{realm}/purchase/{qbo_id}",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        params={"minorversion": "70"}, timeout=30,
    )
    response.raise_for_status()
    purchase = response.json().get("Purchase") or {}
    lines = purchase.get("Line") or []
    account_lines = [line for line in lines if line.get("AccountBasedExpenseLineDetail")]
    item_lines = [line for line in lines if line.get("ItemBasedExpenseLineDetail")]
    if len(account_lines) != 1 or item_lines:
        raise ValueError("This purchase has multiple or item-based lines. Review it directly in QuickBooks.")
    current_ref = account_lines[0]["AccountBasedExpenseLineDetail"].get("AccountRef") or {}
    current_name = str(current_ref.get("name") or "")
    if _category_from_account(current_name):
        with get_engine().begin() as connection:
            connection.execute(text("""
                UPDATE cash_events
                SET subcategory=:account, category=:category, updated_at=:now
                WHERE id=:id
            """), {
                "account": current_name[:64],
                "category": _category_from_account(current_name),
                "now": datetime.now(timezone.utc),
                "id": event_id,
            })
        raise ValueError(
            f"QuickBooks already files this under {current_name}. "
            "Anata refreshed its copy, so it will leave this list."
        )
    return {
        "event": event,
        "purchase": purchase,
        "current_account": current_name or "Uncategorized",
        "accounts": list_expense_accounts(settings),
    }


def confirm_writeback(
    event_id: str, account_id: str, *, settings: Any, actor: str,
) -> dict[str, Any]:
    """Update one reviewed QBO Purchase and append immutable before/after evidence."""
    preview = preview_writeback(event_id, settings)
    allowed = {row["id"]: row["name"] for row in preview["accounts"]}
    if account_id not in allowed:
        raise ValueError("Choose a current QuickBooks expense account.")
    account_name = allowed[account_id]

    base, realm, token = _connection(settings)
    purchase = preview["purchase"]
    line = next(row for row in purchase["Line"] if row.get("AccountBasedExpenseLineDetail"))
    before_ref = dict(line["AccountBasedExpenseLineDetail"].get("AccountRef") or {})
    line["AccountBasedExpenseLineDetail"]["AccountRef"] = {
        "value": account_id, "name": account_name,
    }
    response = requests.post(
        f"{base}/{realm}/purchase",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
        params={"operation": "update", "minorversion": "70"},
        json=purchase, timeout=30,
    )
    response.raise_for_status()
    updated = response.json().get("Purchase") or {}
    now = datetime.now(timezone.utc)
    evidence = {
        "qbo_purchase_id": str(purchase.get("Id") or ""),
        "amount_cents": int(preview["event"].get("amount_cents") or 0),
        "before_account": before_ref,
        "after_account": {"value": account_id, "name": account_name},
        "qbo_sync_token": str(updated.get("SyncToken") or ""),
    }
    with get_engine().begin() as connection:
        connection.execute(text("""
            INSERT INTO finance_action_audit (
                id, scope_key, action_type, entity_type, entity_id, actor,
                idempotency_key, evidence_json, created_at
            ) VALUES (
                :id, 'default', 'qbo_expense_categorized', 'cash_event', :event_id,
                :actor, :key, :evidence, :now
            )
        """), {
            "id": str(uuid4()), "event_id": event_id, "actor": actor,
            "key": f"qbo-expense:{purchase.get('Id')}:{updated.get('SyncToken')}",
            "evidence": json.dumps(evidence), "now": now,
        })
        connection.execute(text("""
            UPDATE cash_events SET subcategory=:account, updated_at=:now WHERE id=:id
        """), {"account": account_name[:64], "now": now, "id": event_id})
    return {"event_id": event_id, "account_name": account_name, "evidence": evidence}


def list_ready_for_qbo(limit: int = 50) -> list[dict[str, Any]]:
    """Possible QBO cleanup candidates, confirmed remotely only at preview."""
    with get_engine().connect() as connection:
        rows = connection.execute(text("""
            SELECT id, name, description, amount_cents, category,
                   COALESCE(effective_date, due_date) AS posted_on, subcategory
            FROM cash_events
            WHERE source='qbo_bank' AND record_kind='transaction'
              AND event_type='outflow' AND source_id LIKE 'purchase-%'
              AND LOWER(COALESCE(category,'')) NOT IN (
                '', 'uncategorized', 'other', 'transfer', 'revenue', 'loan', 'debt'
              )
              AND (
                COALESCE(subcategory,'')='' OR LOWER(subcategory) LIKE '%uncategor%'
                OR LOWER(subcategory) LIKE '%suspense%'
                OR LOWER(subcategory) LIKE '%ask my accountant%'
              )
            ORDER BY COALESCE(effective_date, due_date) DESC
            LIMIT :limit
        """), {"limit": limit}).fetchall()
    return [dict(row._mapping) for row in rows]
