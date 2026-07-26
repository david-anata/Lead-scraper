"""Automated bookkeeping: file bank transactions without hand-sorting them all.

Only money going out is filed here. Money arriving is not an expense, so asking
which expense category a deposit belongs to is a question with no answer; the
incoming side is handled where income and who-owes-you live.

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
from sales_support_agent.services.cashflow.transfers import is_internal_transfer

UNFILED_CATEGORIES = {"", "uncategorized", "other"}
QBO_WRITEBACK_STATUS = "not_connected"

# Shown when something tries to file a deposit. Plain enough for the redirect
# banner the page puts it in.
MONEY_IN_NOT_FILED_HERE = (
    "money coming in is not filed here, it belongs with the rest of your income"
)


def _normalise(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def _is_money_in(row: dict[str, Any]) -> bool:
    """True when this row is money arriving rather than money going out."""
    # Rows with nothing recorded are spend: that is the column's own default.
    return str(row.get("event_type") or "").strip().lower() == "inflow"


# Bank wording that wraps the real merchant name. Stripping it first stops
# every ACH line collapsing into one meaningless "ach withdrawal company" group.
_BANK_NOISE = (
    r"\bach (?:withdrawal|deposit)\b", r"\bdraft withdrawal\b", r"\bfee withdrawal\b",
    r"\bhome banking\b", r"\bwithdrawal\b", r"\bdeposit\b", r"\bpurchase\b",
    r"\bcompany:", r"\bentry:", r"\btracer:", r"\bcomment:", r"\bco:", r"\btype:",
    r"\bdraft #\S*", r"\bcheck #\s*\S*", r"\bcard\b", r"\bdebit\b", r"\bdc\b",
    r"\banata(?: llc| inc\.?| entry)?\b",  # the operator's own entity, not a merchant
    r"\bllc\b", r"\binc\.?\b", r"\bcorp\b", r"\bentry class code\b",
    r"\binternational service fee\b", r"\bisa\b",
    r"\*+\d+", r"#+\d+", r"\b\d{4,}\b", r"\bx{2,}\d*\b",
)
_BANK_NOISE_RE = re.compile("|".join(_BANK_NOISE), re.IGNORECASE)


def merchant_key(description: str) -> str:
    """The merchant inside a bank descriptor, used to group the filing queue.

    Real descriptors bury the payee in boilerplate: "Ach Withdrawal Company:
    Anata Entry: Stripe Cap David Narayan". Grouping on the raw leading words
    would file unrelated payments together, so the boilerplate is removed before
    the identifying words are taken.
    """
    cleaned = _BANK_NOISE_RE.sub(" ", _normalise(description))
    cleaned = re.sub(r"[^a-z0-9 ]+", " ", cleaned)
    words = [word for word in cleaned.split() if len(word) > 1 and not word.isdigit()]
    # Per-charge reference codes ("AMAZON *2K4L9") would otherwise split one
    # merchant into a group per charge, which is the opposite of the point.
    # Names that genuinely contain digits (Helium10) are kept when dropping
    # them would leave nothing behind.
    without_codes = [word for word in words if not any(ch.isdigit() for ch in word)]
    words = without_codes or words
    return " ".join(words[:3])[:255]


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


def _unfiled_transactions(limit: int = 500, *, money_in: bool = False) -> list[dict[str, Any]]:
    """The rows the filing queue can offer, money going out only.

    Deposits are filtered in the query, not afterwards, so the row limit is
    spent on transactions the operator can actually decide about. Pass
    money_in=True only to recognise a deposit a stale page is trying to file.
    """
    placeholders = ",".join(f"'{value}'" for value in sorted(UNFILED_CATEGORIES))
    direction = "=" if money_in else "<>"
    with get_engine().connect() as connection:
        rows = connection.execute(text(f"""
            SELECT id, name, description, vendor_or_customer, category, amount_cents,
                   event_type, COALESCE(effective_date, due_date) AS posted_on
            FROM cash_events
            WHERE record_kind='transaction'
              AND LOWER(COALESCE(category,'')) IN ({placeholders})
              AND COALESCE(amount_cents,0) > 0
              AND LOWER(COALESCE(event_type,'outflow')) {direction} 'inflow'
            ORDER BY COALESCE(effective_date, due_date) DESC
            LIMIT :limit
        """), {"limit": limit}).fetchall()  # noqa: S608 - fixed internal allowlist
    # Moving money between the operator's own accounts is not an expense, so it
    # must never sit in a queue asking what category it belongs to. The
    # direction check is repeated here so the one Python answer stays the same
    # whatever the column happens to hold.
    return [
        dict(row._mapping) for row in rows
        if not is_internal_transfer(dict(row._mapping))
        and _is_money_in(dict(row._mapping)) == money_in
    ]


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


def group_needs_decision(*, limit: int = 3000, top: int = 60) -> dict[str, Any]:
    """The filing queue collapsed by merchant, biggest pile first.

    Nine hundred one-at-a-time decisions is a wall nobody climbs. The same
    transactions are usually a much smaller number of merchants, so deciding
    once per merchant clears most of the queue. Sample descriptions travel with
    each group so the operator can see what they are about to file together.
    """
    from sales_support_agent.services.cashflow.categorizer import categorize

    pending = _unfiled_transactions(limit=limit)
    groups: dict[str, dict[str, Any]] = {}
    for row in pending:
        description = str(row.get("description") or row.get("name") or "")
        key = merchant_key(description) or _normalise(row.get("name") or "") or "unknown"
        group = groups.setdefault(key, {
            "key": key,
            # The cleaned merchant reads better than a raw descriptor with a
            # per-charge reference code in it; the samples show the real text.
            "label": key.title() if key else str(row.get("name") or "Unknown"),
            "count": 0,
            "amount_cents": 0,
            "samples": [],
            "guess": "",
        })
        group["count"] += 1
        group["amount_cents"] += int(row.get("amount_cents") or 0)
        if len(group["samples"]) < 3:
            sample = str(row.get("name") or description)[:90]
            if sample not in group["samples"]:
                group["samples"].append(sample)
        if not group["guess"]:
            guess = categorize(description)
            if guess and guess.lower() not in UNFILED_CATEGORIES:
                group["guess"] = guess

    ordered = sorted(groups.values(), key=lambda item: -item["count"])
    covered = sum(item["count"] for item in ordered[:top])
    return {
        "groups": ordered[:top],
        "group_count": len(ordered),
        "transaction_count": len(pending),
        "covered_by_shown": covered,
    }


def file_merchant(
    key: str, category: str, *, always: bool = True, actor: str = "system", limit: int = 3000,
) -> dict[str, Any]:
    """File every unfiled transaction from one merchant in a single decision."""
    category = _normalise(category)
    if not category or category in UNFILED_CATEGORIES:
        raise ValueError("choose a category first")
    key = _normalise(key)
    if not key:
        raise ValueError("no merchant given")

    matches = [
        row for row in _unfiled_transactions(limit=limit)
        if merchant_key(str(row.get("description") or row.get("name") or "")) == key
    ]
    if not matches:
        # A page opened before deposits left the queue can still post one of
        # them. Say why instead of quietly filing nothing and teaching a rule
        # that would file every future deposit as spend.
        incoming = [
            row for row in _unfiled_transactions(limit=limit, money_in=True)
            if merchant_key(str(row.get("description") or row.get("name") or "")) == key
        ]
        if incoming:
            raise ValueError(MONEY_IN_NOT_FILED_HERE)
        return {"filed": 0, "rule_id": ""}

    now = datetime.now(timezone.utc)
    with get_engine().begin() as connection:
        for row in matches:
            connection.execute(text(
                "UPDATE cash_events SET category=:category, updated_at=:now WHERE id=:id"
            ), {"category": category, "now": now, "id": str(row["id"])})

    rule_id = ""
    if always:
        # Teach it once so this merchant files itself from now on.
        with get_engine().begin() as connection:
            existing = connection.execute(text(
                "SELECT id FROM finance_booking_rules WHERE match_pattern=:pattern"
            ), {"pattern": key}).fetchone()
            if existing:
                connection.execute(text(
                    "UPDATE finance_booking_rules SET category=:category, updated_at=:now WHERE id=:id"
                ), {"category": category, "now": now, "id": str(existing._mapping["id"])})
                rule_id = str(existing._mapping["id"])
            else:
                rule_id = str(uuid4())
                connection.execute(text("""
                    INSERT INTO finance_booking_rules (
                        id, scope_key, match_pattern, category, created_by,
                        hit_count, created_at, updated_at
                    ) VALUES (:id, 'default', :pattern, :category, :actor, :hits, :now, :now)
                """), {"id": rule_id, "pattern": key, "category": category,
                       "actor": actor or "system", "hits": len(matches), "now": now})
    return {"filed": len(matches), "rule_id": rule_id, "category": category}


def file_transaction(
    event_id: str, category: str, *, always: bool = False, actor: str = "system",
) -> dict[str, Any]:
    """File one transaction, optionally teaching a rule so it is never asked again."""
    category = _normalise(category)
    if not category or category in UNFILED_CATEGORIES:
        raise ValueError("choose a category first")
    now = datetime.now(timezone.utc)
    rule_created = ""
    with get_engine().begin() as connection:
        row = connection.execute(text(
            "SELECT name, description, vendor_or_customer, event_type"
            " FROM cash_events WHERE id=:id"
        ), {"id": event_id}).fetchone()
        if row is None:
            raise ValueError("transaction not found")
        if _is_money_in(dict(row._mapping)):
            raise ValueError(MONEY_IN_NOT_FILED_HERE)
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
    """Counts for the bookkeeping panel, including the honest write-back state.

    The split between "QuickBooks already booked this" and "filed only here"
    matters: the first needs nothing, while the second is spend the operator's
    real books still do not have, because nothing is written back.
    """
    # Every number is counted in one pass using the same checks the queue uses,
    # so the nav badge, the queue and these totals cannot drift apart. Money
    # arriving and moving your own money are both outside all of it, including
    # the denominator, because neither is spend waiting for an expense
    # category. A QuickBooks account of "Uncategorized Expense" is the books
    # saying they do not know either, so those rows count as needing a decision
    # rather than as booked. That makes the three states add up to the total,
    # which is the point of showing them.
    with get_engine().connect() as connection:
        rows = connection.execute(text("""
            SELECT name, description, vendor_or_customer, category, source, subcategory,
                   event_type
            FROM cash_events
            WHERE record_kind='transaction' AND COALESCE(amount_cents,0) > 0
        """)).fetchall()
        rule_count = int(connection.execute(text(
            "SELECT COUNT(*) FROM finance_booking_rules"
        )).scalar() or 0)

    total = needs = booked_in_quickbooks = 0
    for raw in rows:
        row = dict(raw._mapping)
        if is_internal_transfer(row) or _is_money_in(row):
            continue
        total += 1
        if str(row.get("category") or "").lower() in UNFILED_CATEGORIES:
            needs += 1
        elif row.get("source") == "qbo_bank" and str(row.get("subcategory") or "").strip():
            booked_in_quickbooks += 1

    return {
        "total_transactions": total,
        "needs_decision": needs,
        "filed": total - needs,
        "booked_in_quickbooks": booked_in_quickbooks,
        "filed_here_only": total - needs - booked_in_quickbooks,
        "rule_count": rule_count,
        "qbo_writeback": QBO_WRITEBACK_STATUS,
    }
