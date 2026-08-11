"""Vendors: declared payment terms with payoff progress from real bank spend.

Each vendor carries operator-declared terms (how much, how often, total owed,
end date) plus keywords used to match the actual outflows in the ledger. Paid
amount and payoff date are computed at read time from those matched payments;
this module never moves money.
"""

from __future__ import annotations

import math
import calendar
import hashlib
import json
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional
from uuid import uuid4

from sqlalchemy import text

from sales_support_agent.models.database import get_engine

VALID_TERMS = ("one_off", "recurring")
VALID_FREQUENCY = ("week", "biweekly", "month", "quarter", "year", "once")
VALID_AGREEMENT_STATUSES = ("draft", "active", "ending", "ended", "disputed")
VALID_TERM_TYPES = ("month_to_month", "fixed_term", "evergreen", "one_time")
VALID_AMOUNT_TYPES = ("fixed", "variable", "minimum")
VALID_AUTO_RENEWAL = ("yes", "no", "unknown")
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


def _choice(data: dict[str, Any], key: str, allowed: tuple[str, ...], default: str) -> str:
    value = str(data.get(key) or default).strip().lower()
    if value not in allowed:
        raise ValueError(f"{key} must be one of {', '.join(allowed)}")
    return value


def _notice_days(value: Any) -> int:
    try:
        return min(730, max(0, int(value or 0)))
    except (TypeError, ValueError) as exc:
        raise ValueError("cancellation notice days must be a whole number") from exc


def cancellation_deadline(vendor: dict[str, Any]) -> Optional[date]:
    renewal = _parse_date(vendor.get("renewal_date"))
    if renewal is None:
        return None
    return renewal - timedelta(days=_notice_days(vendor.get("cancellation_notice_days")))


def _agreement_values(data: dict[str, Any], *, actor: str) -> dict[str, Any]:
    return {
        "agreement_name": str(data.get("agreement_name") or "").strip(),
        "agreement_reference_url": str(data.get("agreement_reference_url") or "").strip(),
        "agreement_status": _choice(data, "agreement_status", VALID_AGREEMENT_STATUSES, "active"),
        "term_type": _choice(data, "term_type", VALID_TERM_TYPES, "month_to_month"),
        "amount_type": _choice(data, "amount_type", VALID_AMOUNT_TYPES, "fixed"),
        "payment_account_label": str(data.get("payment_account_label") or "").strip()[:128],
        "renewal_date": _parse_date(data.get("renewal_date")),
        "auto_renewal": _choice(data, "auto_renewal", VALID_AUTO_RENEWAL, "unknown"),
        "cancellation_notice_days": _notice_days(data.get("cancellation_notice_days")),
        "owner": str(data.get("owner") or "").strip()[:255],
        "evidence_note": str(data.get("evidence_note") or "").strip(),
        "updated_by": actor or "system",
    }


def _audit(connection: Any, *, action: str, vendor_id: str, actor: str, evidence: dict[str, Any]) -> None:
    connection.execute(text("""
        INSERT INTO finance_action_audit (
            id, scope_key, action_type, entity_type, entity_id, actor,
            evidence_json, created_at
        ) VALUES (
            :id, 'default', :action, 'vendor_agreement', :vendor_id, :actor,
            :evidence, :now
        )
    """), {
        "id": str(uuid4()), "action": action, "vendor_id": vendor_id,
        "actor": actor or "system", "evidence": json.dumps(evidence, default=str),
        "now": datetime.now(timezone.utc),
    })


def create_vendor(data: dict[str, Any], *, actor: str = "system") -> str:
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
    agreement = _agreement_values(data, actor=actor)
    with get_engine().begin() as connection:
        connection.execute(text("""
            INSERT INTO finance_vendors (
                id, scope_key, name, terms_type, payment_amount_cents, frequency,
                total_committed_cents, start_date, end_date, match_terms, notes,
                running_account, payment_method, agreement_name,
                agreement_reference_url, agreement_status, term_type, amount_type,
                payment_account_label, renewal_date, auto_renewal,
                cancellation_notice_days, owner, evidence_note, created_by,
                updated_by, active, created_at, updated_at
            ) VALUES (
                :id, 'default', :name, :terms_type, :payment, :frequency,
                :total, :start_date, :end_date, :match_terms, :notes,
                :running_account, :payment_method, :agreement_name,
                :agreement_reference_url, :agreement_status, :term_type, :amount_type,
                :payment_account_label, :renewal_date, :auto_renewal,
                :cancellation_notice_days, :owner, :evidence_note, :created_by,
                :updated_by, TRUE, :now, :now
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
            "running_account": bool(data.get("running_account")),
            "payment_method": ("auto" if str(data.get("payment_method", "")).lower() == "auto" else "manual"),
            "now": now,
            "created_by": actor or "system",
            **agreement,
        })
        _audit(connection, action="vendor_agreement_created", vendor_id=vendor_id,
               actor=actor, evidence={"name": name, **agreement})
    return vendor_id


def update_vendor(vendor_id: str, data: dict[str, Any], *, actor: str = "system") -> None:
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
    agreement = _agreement_values(data, actor=actor)
    with get_engine().begin() as connection:
        previous = connection.execute(
            text("SELECT * FROM finance_vendors WHERE id=:id"), {"id": vendor_id}
        ).fetchone()
        if previous is None:
            raise ValueError("vendor not found")
        result = connection.execute(text("""
            UPDATE finance_vendors SET
                name=:name, terms_type=:terms_type, payment_amount_cents=:payment,
                frequency=:frequency, total_committed_cents=:total,
                start_date=:start_date, end_date=:end_date, match_terms=:match_terms,
                notes=:notes, running_account=:running_account,
                payment_method=:payment_method, agreement_name=:agreement_name,
                agreement_reference_url=:agreement_reference_url,
                agreement_status=:agreement_status, term_type=:term_type,
                amount_type=:amount_type, payment_account_label=:payment_account_label,
                renewal_date=:renewal_date, auto_renewal=:auto_renewal,
                cancellation_notice_days=:cancellation_notice_days,
                owner=:owner, evidence_note=:evidence_note,
                updated_by=:updated_by, updated_at=:now
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
            "running_account": bool(data.get("running_account")),
            "payment_method": ("auto" if str(data.get("payment_method", "")).lower() == "auto" else "manual"),
            "now": now,
            **agreement,
        })
        _audit(connection, action="vendor_agreement_updated", vendor_id=vendor_id,
               actor=actor, evidence={"before": dict(previous._mapping), "after": {"name": name, **agreement}})


def deactivate_vendor(vendor_id: str, *, actor: str = "system") -> None:
    now = datetime.now(timezone.utc)
    with get_engine().begin() as connection:
        result = connection.execute(
            text("UPDATE finance_vendors SET active=FALSE, agreement_status='ended', updated_by=:actor, updated_at=:now WHERE id=:id"),
            {"now": now, "id": vendor_id, "actor": actor or "system"},
        )
        if result.rowcount:
            _audit(connection, action="vendor_agreement_ended", vendor_id=vendor_id,
                   actor=actor, evidence={"active": False, "agreement_status": "ended"})


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


def agreement_mismatches(
    vendor: dict[str, Any], outflows: list[dict[str, Any]], *, as_of: date | None = None,
) -> list[dict[str, Any]]:
    """Explain posted charges that disagree with declared agreement terms."""
    today = as_of or date.today()
    matcher = _term_matcher(_match_terms_list(vendor.get("match_terms", "")))
    if matcher is None:
        return []
    expected = max(0, int(vendor.get("payment_amount_cents") or 0))
    end = _parse_date(vendor.get("end_date"))
    matches: list[dict[str, Any]] = []
    for row in outflows:
        description = " ".join(str(row.get(key) or "") for key in (
            "name", "vendor_or_customer", "description",
        ))
        if matcher.search(description):
            matches.append(row)
    findings: list[dict[str, Any]] = []
    seen: set[tuple[str, int]] = set()
    for row in matches:
        paid_on = _parse_date(row.get("paid_on"))
        amount = max(0, int(row.get("amount_cents") or 0))
        signature = ((paid_on or today).isoformat(), amount)
        if signature in seen:
            findings.append({"kind": "duplicate_charge", "amount_cents": amount,
                             "date": signature[0], "message": "Possible duplicate posted charge."})
        seen.add(signature)
        if end and paid_on and paid_on > end:
            findings.append({"kind": "charge_after_end", "amount_cents": amount,
                             "date": paid_on.isoformat(), "message": "Charge posted after the agreement end date."})
        if expected and str(vendor.get("amount_type") or "fixed") == "fixed":
            tolerance = max(100, expected * 5 // 100)
            if abs(amount - expected) > tolerance:
                findings.append({"kind": "amount_changed", "amount_cents": amount,
                                 "date": paid_on.isoformat() if paid_on else "",
                                 "message": "Posted amount differs from the fixed agreement amount."})
    return findings


def list_vendors_with_progress() -> list[dict[str, Any]]:
    with get_engine().connect() as connection:
        rows = connection.execute(text(
            "SELECT * FROM finance_vendors WHERE active=TRUE ORDER BY name"
        )).fetchall()
        vendors = [dict(row._mapping) for row in rows]
        outflows = _matched_outflows(connection) if vendors else []
    for vendor in vendors:
        vendor.update(_vendor_progress(vendor, outflows))
        vendor["review_items"] = agreement_mismatches(vendor, outflows)
        deadline = cancellation_deadline(vendor)
        vendor["cancellation_deadline"] = deadline.isoformat() if deadline else ""
    return vendors


def _add_months(day: date, months: int) -> date:
    month_index = day.month - 1 + months
    year = day.year + month_index // 12
    month = month_index % 12 + 1
    return date(year, month, min(day.day, calendar.monthrange(year, month)[1]))


def preview_agreement_obligations(
    vendor: dict[str, Any], *, as_of: date | None = None, horizon_days: int = 400,
) -> list[dict[str, Any]]:
    """Return dated forecast obligations; never posted transactions."""
    today = as_of or date.today()
    end = today + timedelta(days=max(1, horizon_days))
    if str(vendor.get("agreement_status") or "active") not in {"active", "ending"}:
        return []
    amount = max(0, int(vendor.get("payment_amount_cents") or 0))
    if amount <= 0 or str(vendor.get("amount_type") or "fixed") == "variable":
        return []
    due = _parse_date(vendor.get("start_date")) or today
    explicit_end = _parse_date(vendor.get("end_date"))
    frequency = str(vendor.get("frequency") or "month")
    while due < today:
        if frequency == "month": due = _add_months(due, 1)
        elif frequency == "quarter": due = _add_months(due, 3)
        elif frequency == "year": due = _add_months(due, 12)
        elif frequency in {"week", "biweekly"}: due += timedelta(days=7 if frequency == "week" else 14)
        else: return []
    rows: list[dict[str, Any]] = []
    while due <= end and (explicit_end is None or due <= explicit_end):
        identity = hashlib.sha256(f"{vendor.get('id')}|{due.isoformat()}".encode()).hexdigest()[:24]
        rows.append({
            "id": f"agreement-{identity}", "source": "vendor_agreement",
            "source_id": str(vendor.get("id") or ""), "record_kind": "obligation",
            "event_type": "outflow", "category": "vendor_contract",
            "commitment_type": "recurring", "name": str(vendor.get("name") or "Vendor"),
            "vendor_or_customer": str(vendor.get("name") or "Vendor"),
            "amount_cents": amount, "due_date": due, "status": "planned",
            "confidence": "confirmed", "authority": "vendor_agreement",
            "agreement_status": vendor.get("agreement_status"),
        })
        if frequency == "once": break
        if frequency == "month": due = _add_months(due, 1)
        elif frequency == "quarter": due = _add_months(due, 3)
        elif frequency == "year": due = _add_months(due, 12)
        else: due += timedelta(days=7 if frequency == "week" else 14)
    return rows


def get_vendor(vendor_id: str) -> Optional[dict[str, Any]]:
    with get_engine().connect() as connection:
        row = connection.execute(
            text("SELECT * FROM finance_vendors WHERE id=:id"), {"id": vendor_id}
        ).fetchone()
    return dict(row._mapping) if row else None
