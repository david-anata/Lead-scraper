"""Collections: who owes you, oldest first, with ready-to-send draft messages.

Reads overdue receivables from the ledger, groups them by customer, and writes
a polite email and text draft for each. The app only drafts and tracks status;
it never sends a message to a customer. Sending stays a human action.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timezone
from typing import Any, Optional
from uuid import uuid4

from sqlalchemy import text

from sales_support_agent.models.database import get_engine

OPEN_INFLOW_STATUSES = ("planned", "overdue", "pending")
VALID_CHANNELS = ("email", "sms")
VALID_STATUSES = ("draft", "sent", "skipped")


def _customer_key(name: str) -> str:
    return re.sub(r"\s+", " ", str(name or "").strip().lower())[:255]


def _as_date(value: Any) -> Optional[date]:
    if not value:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _dollars(cents: int) -> str:
    return f"${cents / 100:,.2f}"


def list_overdue_receivables(*, as_of: Optional[date] = None) -> list[dict[str, Any]]:
    """Group open, past-due money owed to us by customer, oldest first."""
    as_of = as_of or date.today()
    placeholders = ",".join(f"'{status}'" for status in OPEN_INFLOW_STATUSES)
    with get_engine().connect() as connection:
        rows = connection.execute(text(f"""
            SELECT vendor_or_customer, amount_cents, due_date
            FROM cash_events
            WHERE event_type='inflow'
              AND LOWER(COALESCE(status,'')) IN ({placeholders})
              AND COALESCE(amount_cents,0) > 0
              AND record_kind <> 'transaction'
        """)).fetchall()  # noqa: S608 - statuses are a fixed internal allowlist

    grouped: dict[str, dict[str, Any]] = {}
    for raw in rows:
        row = dict(raw._mapping)
        due = _as_date(row.get("due_date"))
        if due is None or due >= as_of:
            continue  # not past due yet
        customer = str(row.get("vendor_or_customer") or "").strip() or "Customer"
        key = _customer_key(customer)
        entry = grouped.setdefault(key, {
            "customer_key": key, "customer": customer,
            "owed_cents": 0, "invoice_count": 0, "oldest_due": due,
        })
        entry["owed_cents"] += int(row.get("amount_cents") or 0)
        entry["invoice_count"] += 1
        if due < entry["oldest_due"]:
            entry["oldest_due"] = due

    result = []
    for entry in grouped.values():
        entry["days_late"] = (as_of - entry["oldest_due"]).days
        entry["oldest_due"] = entry["oldest_due"].isoformat()
        result.append(entry)
    result.sort(key=lambda item: item["days_late"], reverse=True)
    return result


def email_draft(entry: dict[str, Any]) -> dict[str, str]:
    owed = _dollars(int(entry["owed_cents"]))
    customer = entry["customer"]
    plural = "invoice" if entry["invoice_count"] == 1 else "invoices"
    subject = f"Friendly reminder: {owed} outstanding"
    body = (
        f"Hi {customer},\n\n"
        f"I hope you are well. Our records show {owed} outstanding across "
        f"{entry['invoice_count']} {plural}, the oldest {entry['days_late']} days past due.\n\n"
        "Could you let me know the expected payment date, or reply here if anything "
        "needs sorting out on our end? Happy to resend the invoices if helpful.\n\n"
        "Thank you,\nAnata"
    )
    return {"subject": subject, "body": body}


def sms_draft(entry: dict[str, Any]) -> dict[str, str]:
    owed = _dollars(int(entry["owed_cents"]))
    body = (
        f"Hi {entry['customer']}, a quick reminder that {owed} is outstanding "
        f"({entry['days_late']} days past due). Can you share an expected payment date? "
        "Thanks, Anata"
    )
    return {"body": body}


def _load_statuses() -> dict[tuple[str, str], str]:
    with get_engine().connect() as connection:
        rows = connection.execute(text(
            "SELECT customer_key, channel, status FROM finance_collection_drafts"
        )).fetchall()
    return {(str(r._mapping["customer_key"]), str(r._mapping["channel"])): str(r._mapping["status"]) for r in rows}


def build_collections(*, as_of: Optional[date] = None) -> dict[str, Any]:
    """Overdue customers with their drafts and any recorded send/skip status."""
    receivables = list_overdue_receivables(as_of=as_of)
    statuses = _load_statuses()
    total_owed = 0
    customers = []
    for entry in receivables:
        total_owed += int(entry["owed_cents"])
        key = entry["customer_key"]
        customers.append({
            **entry,
            "email": email_draft(entry),
            "sms": sms_draft(entry),
            "email_status": statuses.get((key, "email"), "draft"),
            "sms_status": statuses.get((key, "sms"), "draft"),
        })
    return {
        "total_owed_cents": total_owed,
        "customer_count": len(customers),
        "customers": customers,
    }


def set_draft_status(customer_key: str, channel: str, status: str, *, amount_cents: Optional[int] = None) -> None:
    """Record that the operator sent or skipped a customer/channel message."""
    channel = str(channel or "").strip().lower()
    status = str(status or "").strip().lower()
    if channel not in VALID_CHANNELS:
        raise ValueError("channel must be email or sms")
    if status not in VALID_STATUSES:
        raise ValueError("status must be draft, sent, or skipped")
    key = _customer_key(customer_key)
    now = datetime.now(timezone.utc)
    with get_engine().begin() as connection:
        existing = connection.execute(text(
            "SELECT id FROM finance_collection_drafts WHERE scope_key='default' "
            "AND customer_key=:key AND channel=:channel"
        ), {"key": key, "channel": channel}).fetchone()
        if existing:
            connection.execute(text(
                "UPDATE finance_collection_drafts SET status=:status, amount_cents=:amount, "
                "updated_at=:now WHERE id=:id"
            ), {"status": status, "amount": amount_cents, "now": now, "id": str(existing._mapping["id"])})
        else:
            connection.execute(text("""
                INSERT INTO finance_collection_drafts (
                    id, scope_key, customer_key, channel, status, amount_cents, created_at, updated_at
                ) VALUES (:id, 'default', :key, :channel, :status, :amount, :now, :now)
            """), {"id": str(uuid4()), "key": key, "channel": channel,
                   "status": status, "amount": amount_cents, "now": now})
