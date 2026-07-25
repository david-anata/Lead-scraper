"""Automated bookkeeping: file bank transactions without hand-sorting them all.

Filing order for each unfiled transaction:

1. a rule the operator taught ("always do this") wins;
2. otherwise the built-in keyword categorizer;
3. anything still unknown goes to a short review queue.

Confirming one transaction can create a rule, so the same merchant is never
asked about twice. Nothing here writes to QuickBooks: that is a separate,
outward-facing step that stays off until it is explicitly specced and enabled.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Optional
from uuid import uuid4

from sqlalchemy import text

from sales_support_agent.models.database import get_engine

UNFILED_CATEGORIES = {"", "uncategorized", "other"}
QBO_WRITEBACK_STATUS = "not_connected"


def _normalise(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def suggest_rule_pattern(description: str) -> str:
    """Derive a reusable merchant pattern from one transaction description.

    Card and ACH descriptors carry per-transaction noise (store numbers, dates,
    trailing reference digits), so the pattern keeps the leading words that
    identify the merchant and drops the rest.
    """
    cleaned = re.sub(r"[^a-z0-9 ]+", " ", _normalise(description))
    words = [word for word in cleaned.split() if not word.isdigit()]
    return " ".join(words[:3])[:255]


def _rules() -> list[dict[str, Any]]:
    with get_engine().connect() as connection:
        rows = connection.execute(text(
            "SELECT * FROM finance_booking_rules ORDER BY LENGTH(match_pattern) DESC"
        )).fetchall()
    return [dict(row._mapping) for row in rows]


def _unfiled_transactions(limit: int = 500) -> list[dict[str, Any]]:
    placeholders = ",".join(f"'{value}'" for value in sorted(UNFILED_CATEGORIES))
    with get_engine().connect() as connection:
        rows = connection.execute(text(f"""
            SELECT id, name, description, vendor_or_customer, category, amount_cents,
                   COALESCE(effective_date, due_date) AS posted_on
            FROM cash_events
            WHERE record_kind='transaction'
              AND LOWER(COALESCE(category,'')) IN ({placeholders})
              AND COALESCE(amount_cents,0) > 0
            ORDER BY COALESCE(effective_date, due_date) DESC
            LIMIT :limit
        """), {"limit": limit}).fetchall()  # noqa: S608 - fixed internal allowlist
    return [dict(row._mapping) for row in rows]


def _haystack(row: dict[str, Any]) -> str:
    return _normalise(" ".join([
        str(row.get("name") or ""),
        str(row.get("vendor_or_customer") or ""),
        str(row.get("description") or ""),
    ]))


def file_transactions(*, limit: int = 500) -> dict[str, Any]:
    """File what can be filed confidently. Returns counts, writes categories."""
    from sales_support_agent.services.cashflow.categorizer import categorize

    rules = _rules()
    pending = _unfiled_transactions(limit=limit)
    filed_by_rule = 0
    filed_by_keyword = 0
    still_unknown = 0
    now = datetime.now(timezone.utc)

    for row in pending:
        haystack = _haystack(row)
        category = ""
        rule_id = ""
        for rule in rules:
            pattern = _normalise(rule.get("match_pattern") or "")
            if pattern and pattern in haystack:
                category = str(rule.get("category") or "")
                rule_id = str(rule.get("id"))
                break
        if category:
            filed_by_rule += 1
        else:
            guess = categorize(str(row.get("description") or row.get("name") or ""))
            if guess and guess.lower() not in UNFILED_CATEGORIES:
                category = guess
                filed_by_keyword += 1
            else:
                still_unknown += 1
                continue

        with get_engine().begin() as connection:
            connection.execute(text(
                "UPDATE cash_events SET category=:category, updated_at=:now WHERE id=:id"
            ), {"category": category, "now": now, "id": str(row["id"])})
            if rule_id:
                connection.execute(text(
                    "UPDATE finance_booking_rules SET hit_count=hit_count+1, updated_at=:now WHERE id=:id"
                ), {"now": now, "id": rule_id})

    return {
        "filed_by_rule": filed_by_rule,
        "filed_by_keyword": filed_by_keyword,
        "needs_decision": still_unknown,
        "reviewed": len(pending),
    }


def list_needs_decision(*, limit: int = 50) -> list[dict[str, Any]]:
    """Transactions the app could not file confidently, with its best guess."""
    from sales_support_agent.services.cashflow.categorizer import categorize

    items = []
    for row in _unfiled_transactions(limit=limit):
        description = str(row.get("description") or row.get("name") or "")
        guess = categorize(description)
        items.append({
            "id": str(row["id"]),
            "name": str(row.get("name") or row.get("vendor_or_customer") or "Transaction"),
            "amount_cents": int(row.get("amount_cents") or 0),
            "posted_on": str(row.get("posted_on") or "")[:10],
            "guess": guess if guess and guess.lower() not in UNFILED_CATEGORIES else "",
            "suggested_pattern": suggest_rule_pattern(description or str(row.get("name") or "")),
        })
    return items


def file_transaction(
    event_id: str, category: str, *, always: bool = False, actor: str = "system",
) -> dict[str, Any]:
    """File one transaction, optionally teaching a rule so it is never asked again."""
    category = _normalise(category)
    if not category:
        raise ValueError("a category is required")
    now = datetime.now(timezone.utc)
    rule_created = ""
    with get_engine().begin() as connection:
        row = connection.execute(text(
            "SELECT name, description, vendor_or_customer FROM cash_events WHERE id=:id"
        ), {"id": event_id}).fetchone()
        if row is None:
            raise ValueError("transaction not found")
        connection.execute(text(
            "UPDATE cash_events SET category=:category, updated_at=:now WHERE id=:id"
        ), {"category": category, "now": now, "id": event_id})

        if always:
            source_text = str(row._mapping.get("description") or row._mapping.get("name") or "")
            pattern = suggest_rule_pattern(source_text)
            if pattern:
                existing = connection.execute(text(
                    "SELECT id FROM finance_booking_rules WHERE match_pattern=:pattern"
                ), {"pattern": pattern}).fetchone()
                if existing:
                    connection.execute(text(
                        "UPDATE finance_booking_rules SET category=:category, updated_at=:now WHERE id=:id"
                    ), {"category": category, "now": now, "id": str(existing._mapping["id"])})
                    rule_created = str(existing._mapping["id"])
                else:
                    rule_id = str(uuid4())
                    connection.execute(text("""
                        INSERT INTO finance_booking_rules (
                            id, scope_key, match_pattern, category, created_by,
                            hit_count, created_at, updated_at
                        ) VALUES (:id, 'default', :pattern, :category, :actor, 0, :now, :now)
                    """), {"id": rule_id, "pattern": pattern, "category": category,
                           "actor": actor or "system", "now": now})
                    rule_created = rule_id
    return {"event_id": event_id, "category": category, "rule_id": rule_created}


def list_rules() -> list[dict[str, Any]]:
    return _rules()


def delete_rule(rule_id: str) -> None:
    with get_engine().begin() as connection:
        connection.execute(
            text("DELETE FROM finance_booking_rules WHERE id=:id"), {"id": rule_id}
        )


def bookkeeping_summary() -> dict[str, Any]:
    """Counts for the bookkeeping panel, including the honest write-back state."""
    with get_engine().connect() as connection:
        total = int(connection.execute(text(
            "SELECT COUNT(*) FROM cash_events WHERE record_kind='transaction'"
        )).scalar() or 0)
        rule_count = int(connection.execute(text(
            "SELECT COUNT(*) FROM finance_booking_rules"
        )).scalar() or 0)
    needs = len(_unfiled_transactions(limit=1000))
    return {
        "total_transactions": total,
        "needs_decision": needs,
        "filed": max(0, total - needs),
        "rule_count": rule_count,
        "qbo_writeback": QBO_WRITEBACK_STATUS,
    }
