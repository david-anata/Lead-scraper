"""Vendors: declared payment terms with payoff progress from real bank spend.

Each vendor carries operator-declared terms (how much, how often, total owed,
end date) plus keywords used to match the actual outflows in the ledger. Paid
amount and payoff date are computed at read time from those matched payments;
this module never moves money.
"""

from __future__ import annotations

import math
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional
from uuid import uuid4

from sqlalchemy import text

from sales_support_agent.models.database import get_engine

VALID_TERMS = ("one_off", "recurring")
VALID_FREQUENCY = ("week", "biweekly", "month", "quarter", "year", "once")
_FREQUENCY_DAYS = {"week": 7, "biweekly": 14, "month": 30, "quarter": 91, "year": 365, "once": 0}


def _match_terms_list(raw: str) -> list[str]:
    """Split the operator's keyword blob into normalized, non-empty terms."""
    parts: list[str] = []
    for chunk in str(raw or "").replace("\n", ",").split(","):
        term = chunk.strip().lower()
        if term:
            parts.append(term)
    return parts


def _term_matcher(terms: list[str]) -> Optional[re.Pattern[str]]:
    """Compile terms into a whole-word matcher.

    Plain substring matching over-matches badly on short terms: "von" would
    claim VONAGE, and "rent" would claim PARENT, silently inflating what a
    vendor looks like it has been paid. Word boundaries keep "von" matching
    VON HILL while leaving VONAGE alone.
    """
    if not terms:
        return None
    return re.compile(
        "|".join(r"\b" + re.escape(term) + r"\b" for term in terms),
        re.IGNORECASE,
    )


def _parse_date(value: Any) -> Optional[date]:
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


def _parse_cents(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def create_vendor(data: dict[str, Any]) -> str:
    vendor_id = str(uuid4())
    now = datetime.now(timezone.utc)
    terms_type = str(data.get("terms_type") or "recurring").strip().lower()
    if terms_type not in VALID_TERMS:
        raise ValueError("terms_type must be one_off or recurring")
    frequency = str(data.get("frequency") or "month").strip().lower()
    if frequency not in VALID_FREQUENCY:
        frequency = "month"
    name = str(data.get("name") or "").strip()
    if not name:
        raise ValueError("vendor name is required")
    with get_engine().begin() as connection:
        connection.execute(text("""
            INSERT INTO finance_vendors (
                id, scope_key, name, terms_type, payment_amount_cents, frequency,
                total_committed_cents, start_date, end_date, match_terms, notes,
                running_account, active, created_at, updated_at
            ) VALUES (
                :id, 'default', :name, :terms_type, :payment, :frequency,
                :total, :start_date, :end_date, :match_terms, :notes,
                :running_account, TRUE, :now, :now
            )
        """), {
            "id": vendor_id, "name": name, "terms_type": terms_type,
            "payment": _parse_cents(data.get("payment_amount_cents")),
            "frequency": frequency,
            "total": _parse_cents(data.get("total_committed_cents")),
            "start_date": _parse_date(data.get("start_date")),
            "end_date": _parse_date(data.get("end_date")),
            "match_terms": str(data.get("match_terms") or "").strip(),
            "notes": str(data.get("notes") or "").strip(),
            "running_account": bool(data.get("running_account")), "now": now,
        })
    return vendor_id


def update_vendor(vendor_id: str, data: dict[str, Any]) -> None:
    now = datetime.now(timezone.utc)
    terms_type = str(data.get("terms_type") or "recurring").strip().lower()
    if terms_type not in VALID_TERMS:
        raise ValueError("terms_type must be one_off or recurring")
    frequency = str(data.get("frequency") or "month").strip().lower()
    if frequency not in VALID_FREQUENCY:
        frequency = "month"
    name = str(data.get("name") or "").strip()
    if not name:
        raise ValueError("vendor name is required")
    with get_engine().begin() as connection:
        result = connection.execute(text("""
            UPDATE finance_vendors SET
                name=:name, terms_type=:terms_type, payment_amount_cents=:payment,
                frequency=:frequency, total_committed_cents=:total,
                start_date=:start_date, end_date=:end_date, match_terms=:match_terms,
                notes=:notes, running_account=:running_account, updated_at=:now
            WHERE id=:id
        """), {
            "id": vendor_id, "name": name, "terms_type": terms_type,
            "payment": _parse_cents(data.get("payment_amount_cents")),
            "frequency": frequency,
            "total": _parse_cents(data.get("total_committed_cents")),
            "start_date": _parse_date(data.get("start_date")),
            "end_date": _parse_date(data.get("end_date")),
            "match_terms": str(data.get("match_terms") or "").strip(),
            "notes": str(data.get("notes") or "").strip(),
            "running_account": bool(data.get("running_account")), "now": now,
        })
    if result.rowcount == 0:
        raise ValueError("vendor not found")


def deactivate_vendor(vendor_id: str) -> None:
    now = datetime.now(timezone.utc)
    with get_engine().begin() as connection:
        connection.execute(
            text("UPDATE finance_vendors SET active=FALSE, updated_at=:now WHERE id=:id"),
            {"now": now, "id": vendor_id},
        )


def _matched_outflows(connection: Any) -> list[dict[str, Any]]:
    """Posted outflows available for vendor attribution."""
    rows = connection.execute(text("""
        SELECT amount_cents, name, vendor_or_customer, description,
               COALESCE(effective_date, due_date) AS paid_on
        FROM cash_events
        WHERE event_type='outflow'
          AND LOWER(COALESCE(status,'')) IN ('posted','matched','completed')
          AND COALESCE(amount_cents, 0) > 0
    """)).fetchall()
    return [dict(row._mapping) for row in rows]


def _vendor_progress(vendor: dict[str, Any], outflows: list[dict[str, Any]]) -> dict[str, Any]:
    terms = _match_terms_list(vendor.get("match_terms", ""))
    matcher = _term_matcher(terms)
    start = _parse_date(vendor.get("start_date"))
    paid_cents = 0
    matched_count = 0
    last_paid: Optional[date] = None
    if matcher is not None:
        for row in outflows:
            haystack = " ".join([
                str(row.get("name") or ""),
                str(row.get("vendor_or_customer") or ""),
                str(row.get("description") or ""),
            ]).lower()
            if not matcher.search(haystack):
                continue
            paid_on = _parse_date(row.get("paid_on"))
            if start and paid_on and paid_on < start:
                continue
            paid_cents += int(row.get("amount_cents") or 0)
            matched_count += 1
            if paid_on and (last_paid is None or paid_on > last_paid):
                last_paid = paid_on

    is_running = bool(vendor.get("running_account"))
    total = vendor.get("total_committed_cents")
    remaining_cents: Optional[int] = None
    percent_bps: Optional[int] = None
    if total:
        remaining_cents = max(0, int(total) - paid_cents)
        percent_bps = min(10000, round(paid_cents * 10000 / int(total))) if int(total) else 0

    payoff_date = _parse_date(vendor.get("end_date"))
    if payoff_date is None and remaining_cents and remaining_cents > 0:
        payment = vendor.get("payment_amount_cents")
        if vendor.get("terms_type") == "recurring" and payment and int(payment) > 0:
            periods = math.ceil(remaining_cents / int(payment))
            cadence_days = _FREQUENCY_DAYS.get(str(vendor.get("frequency") or "month"), 30)
            if cadence_days:
                anchor = last_paid or date.today()
                payoff_date = anchor + timedelta(days=periods * cadence_days)

    return {
        "running_account": is_running,
        "paid_cents": paid_cents,
        "matched_count": matched_count,
        "remaining_cents": remaining_cents,
        "percent_bps": percent_bps,
        "payoff_date": payoff_date.isoformat() if payoff_date else "",
        "last_paid": last_paid.isoformat() if last_paid else "",
    }


def list_vendors_with_progress() -> list[dict[str, Any]]:
    with get_engine().connect() as connection:
        rows = connection.execute(text(
            "SELECT * FROM finance_vendors WHERE active=TRUE ORDER BY name"
        )).fetchall()
        vendors = [dict(row._mapping) for row in rows]
        outflows = _matched_outflows(connection) if vendors else []
    for vendor in vendors:
        vendor.update(_vendor_progress(vendor, outflows))
    return vendors


def get_vendor(vendor_id: str) -> Optional[dict[str, Any]]:
    with get_engine().connect() as connection:
        row = connection.execute(
            text("SELECT * FROM finance_vendors WHERE id=:id"), {"id": vendor_id}
        ).fetchone()
    return dict(row._mapping) if row else None
