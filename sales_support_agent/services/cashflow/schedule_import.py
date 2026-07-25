"""Rebuild the billing schedule natively from what ClickUp already told us.

ClickUp was a scheduler, not a data source, and its entries are already in the
ledger: name, amount, cadence, and whether the payment is automatic. This module
reads them back, proposes a native schedule plus the vendors behind it, and
sorts the leftovers so nothing is guessed:

* recurring series  -> a proposed schedule and vendor
* overdue one-offs  -> checked against the bank feed: paid (archive) or not (keep)
* undated items     -> listed for the operator to pick from

Nothing is written until ``apply_import`` is called with explicit selections.
"""

from __future__ import annotations

import re
import statistics
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

from sqlalchemy import text

from sales_support_agent.models.database import get_engine

SOURCE = "clickup"
# ClickUp cadences the sync already recorded, mapped to template frequencies.
_CADENCE_TO_FREQUENCY = {
    "weekly": "weekly",
    "biweekly": "biweekly",
    "bi-weekly": "biweekly",
    "monthly": "monthly",
    "quarterly": "quarterly",
    "annual": "annual",
    "yearly": "annual",
}
_CADENCE_DAYS = {"weekly": 7, "biweekly": 14, "monthly": 30, "quarterly": 91, "annual": 365}
_WEEKDAYS = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")


def _series_name(name: str) -> str:
    """Strip per-occurrence noise so occurrences of one bill group together."""
    cleaned = re.sub(r"\s*\(week\s*\d+\)\s*", " ", str(name or ""), flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*[-–]\s*\d{1,2}[/-]\d{1,2}([/-]\d{2,4})?\s*$", " ", cleaned)
    cleaned = re.sub(r"\s+\d{1,2}[/-]\d{1,2}([/-]\d{2,4})?\s*$", " ", cleaned)
    return re.sub(r"\s+", " ", cleaned).strip()


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


def _payment_method(rows: list[dict[str, Any]]) -> str:
    """Auto only when the source consistently said so; otherwise manual.

    The ClickUp sync recorded the Man/Auto flag as confidence: 'confirmed' for
    Auto. Anything mixed or unclear resolves to manual, because a wrong 'manual'
    costs a wasted minute while a wrong 'auto' means a missed payment.
    """
    confidences = {str(row.get("confidence") or "").lower() for row in rows}
    return "auto" if confidences == {"confirmed"} else "manual"


def _infer_cadence(dates: list[date]) -> str:
    if len(dates) < 3:
        return ""
    ordered = sorted(dates)
    gaps = [(right - left).days for left, right in zip(ordered, ordered[1:])]
    median = int(statistics.median(gaps)) if gaps else 0
    for frequency, days in _CADENCE_DAYS.items():
        if abs(median - days) <= max(3, days * 0.2):
            return frequency
    return ""


def _typical_day_of_month(dates: list[date]) -> Optional[int]:
    """The day these bills actually land on, ignoring one-off shifts."""
    if not dates:
        return None
    days = [d.day for d in dates]
    return int(statistics.mode(days)) if len(set(days)) < len(days) else days[-1]


def _next_due(last_due: date, frequency: str, *, as_of: date,
              day_of_month: Optional[int] = None) -> date:
    """First occurrence at or after today.

    Monthly cadences advance by calendar month, not by 30 days: stepping 30 days
    walks the date backwards (Jul 9 becomes Aug 8), which would rename a bill the
    operator knows as "the 5th" to the 4th and keep drifting every month.
    """
    from sales_support_agent.services.cashflow.obligations import _next_occurrence

    candidate = last_due
    guard = 0
    while candidate < as_of and guard < 500:
        candidate = _next_occurrence(candidate, frequency, day_of_month)
        guard += 1
    return candidate


def _load_clickup_obligations() -> list[dict[str, Any]]:
    with get_engine().connect() as connection:
        rows = connection.execute(text("""
            SELECT id, name, vendor_or_customer, amount_cents, due_date, category,
                   event_type, recurring_rule, confidence, commitment_type, status
            FROM cash_events
            WHERE LOWER(COALESCE(source,'')) = :source
              AND record_kind <> 'transaction'
              AND archived_at IS NULL
            ORDER BY due_date
        """), {"source": SOURCE}).fetchall()
    return [dict(row._mapping) for row in rows]


def propose_schedules(*, as_of: Optional[date] = None) -> list[dict[str, Any]]:
    """Recurring series found in the ClickUp data, as proposed native schedules."""
    as_of = as_of or date.today()
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in _load_clickup_obligations():
        if _as_date(row.get("due_date")) is None:
            continue
        if str(row.get("event_type") or "outflow") != "outflow":
            continue
        grouped[_series_name(row.get("name") or row.get("vendor_or_customer") or "")].append(row)

    proposals = []
    for name, rows in grouped.items():
        if not name:
            continue
        dates = [d for d in (_as_date(r.get("due_date")) for r in rows) if d]
        declared = {str(r.get("recurring_rule") or "").lower() for r in rows} - {""}
        frequency = ""
        for rule in declared:
            if rule in _CADENCE_TO_FREQUENCY:
                frequency = _CADENCE_TO_FREQUENCY[rule]
                break
        if not frequency:
            frequency = _infer_cadence(dates)
        if not frequency or len(rows) < 2:
            continue  # not enough evidence of a schedule

        amounts = [int(r.get("amount_cents") or 0) for r in rows if int(r.get("amount_cents") or 0) > 0]
        if not amounts:
            continue
        amount = int(statistics.median(amounts))
        last_due = max(dates)
        monthly = frequency in {"monthly", "quarterly", "annual"}
        day_of_month = _typical_day_of_month(dates) if monthly else None
        next_due = _next_due(last_due, frequency, as_of=as_of, day_of_month=day_of_month)
        proposals.append({
            "key": _series_name(name),
            "name": name,
            "vendor": str(rows[0].get("vendor_or_customer") or name),
            "amount_cents": amount,
            "amount_varies": len(set(amounts)) > 1,
            "frequency": frequency,
            "day_of_month": day_of_month,
            "weekday": _WEEKDAYS[next_due.weekday()] if frequency in {"weekly", "biweekly"} else "",
            "next_due": next_due.isoformat(),
            "payment_method": _payment_method(rows),
            "category": str(rows[0].get("category") or "other"),
            "commitment_type": str(rows[0].get("commitment_type") or "general"),
            "occurrence_count": len(rows),
            "source_ids": [str(r["id"]) for r in rows],
            "is_estimate": str(rows[0].get("commitment_type") or "").lower() == "payroll",
        })
    proposals.sort(key=lambda item: -item["occurrence_count"])
    return proposals


def propose_overdue_disposition(*, as_of: Optional[date] = None) -> list[dict[str, Any]]:
    """Each overdue ClickUp bill, checked against the bank feed for a payment."""
    from sales_support_agent.services.cashflow.payment_finder import find_payment_candidates

    as_of = as_of or date.today()
    scheduled_ids = {
        source_id
        for proposal in propose_schedules(as_of=as_of)
        for source_id in proposal["source_ids"]
    }

    results = []
    for row in _load_clickup_obligations():
        due = _as_date(row.get("due_date"))
        if due is None or due >= as_of:
            continue
        if str(row.get("status") or "").lower() not in {"planned", "pending", "overdue"}:
            continue
        if str(row["id"]) in scheduled_ids:
            continue  # covered by a proposed schedule instead
        try:
            found = find_payment_candidates(str(row["id"]))
            candidates = found["candidates"]
        except Exception:
            candidates = []
        best = candidates[0] if candidates else None
        results.append({
            "id": str(row["id"]),
            "name": str(row.get("name") or row.get("vendor_or_customer") or "Bill"),
            "amount_cents": int(row.get("amount_cents") or 0),
            "due_date": due.isoformat(),
            "days_overdue": (as_of - due).days,
            "looks_paid": best is not None,
            "match_name": str(best["name"]) if best else "",
            "match_date": str(best["paid_on"]) if best else "",
            "match_exact": bool(best and best["exact_amount"]),
            "recommendation": "archive" if best else "keep",
        })
    results.sort(key=lambda item: (item["recommendation"] != "archive", -item["days_overdue"]))
    return results


def propose_unscheduled() -> list[dict[str, Any]]:
    """ClickUp items with no date. They cannot be a schedule, so the operator picks."""
    items = []
    for row in _load_clickup_obligations():
        if _as_date(row.get("due_date")) is not None:
            continue
        items.append({
            "id": str(row["id"]),
            "name": str(row.get("name") or row.get("vendor_or_customer") or "Item"),
            "amount_cents": int(row.get("amount_cents") or 0),
            "event_type": str(row.get("event_type") or "outflow"),
        })
    return items


def build_import_plan(*, as_of: Optional[date] = None) -> dict[str, Any]:
    """Everything the review page needs, computed without writing anything."""
    schedules = propose_schedules(as_of=as_of)
    overdue = propose_overdue_disposition(as_of=as_of)
    unscheduled = propose_unscheduled()
    return {
        "schedules": schedules,
        "overdue": overdue,
        "unscheduled": unscheduled,
        "schedule_count": len(schedules),
        "monthly_estimate_cents": sum(
            int(item["amount_cents"] * (30 / _CADENCE_DAYS.get(item["frequency"], 30)))
            for item in schedules
        ),
        "archive_suggested": sum(1 for item in overdue if item["recommendation"] == "archive"),
        "keep_suggested": sum(1 for item in overdue if item["recommendation"] == "keep"),
        "unscheduled_count": len(unscheduled),
    }


def apply_import(
    *,
    schedule_keys: list[str],
    archive_ids: list[str],
    keep_ids: list[str],
    actor: str = "system",
    as_of: Optional[date] = None,
) -> dict[str, Any]:
    """Create the chosen vendors and schedules, then archive what was selected."""
    from sales_support_agent.services.cashflow.bulk_resolve import apply_bulk_action
    from sales_support_agent.services.cashflow.obligations import create_recurring_template
    from sales_support_agent.services.cashflow.vendors import create_vendor

    as_of = as_of or date.today()
    chosen = {key for key in schedule_keys}
    proposals = [item for item in propose_schedules(as_of=as_of) if item["key"] in chosen]

    created_templates = 0
    created_vendors = 0
    source_ids_to_archive: list[str] = []

    for proposal in proposals:
        create_recurring_template(
            name=proposal["name"],
            vendor_or_customer=proposal["vendor"],
            event_type="outflow",
            category=proposal["category"],
            amount_cents=int(proposal["amount_cents"]),
            frequency=proposal["frequency"],
            next_due_date=date.fromisoformat(proposal["next_due"]),
            day_of_month=proposal["day_of_month"],
            confidence="confirmed" if proposal["payment_method"] == "auto" else "estimated",
            notes=(
                "Payroll estimate until HR provides real hours"
                if proposal["is_estimate"] else
                f"Imported from the ClickUp schedule ({proposal['payment_method']})"
            ),
        )
        created_templates += 1

        try:
            create_vendor({
                "name": proposal["vendor"],
                "terms_type": "recurring",
                "payment_amount_cents": int(proposal["amount_cents"]),
                "frequency": "month" if proposal["frequency"] == "monthly" else "week",
                "match_terms": proposal["vendor"],
                "payment_method": proposal["payment_method"],
            })
            created_vendors += 1
        except Exception:
            pass  # a vendor may already exist; the schedule is what matters

        source_ids_to_archive.extend(proposal["source_ids"])

    # Superseded schedule rows and the operator's archive picks leave together.
    archive_all = sorted({*source_ids_to_archive, *[str(i) for i in archive_ids]} - {str(i) for i in keep_ids})
    archived = 0
    if archive_all:
        result = apply_bulk_action(
            archive_all, "archive_historical",
            reason="Replaced by native recurring schedule", actor=actor,
        )
        archived = int(result.get("applied") or 0)

    return {
        "templates_created": created_templates,
        "vendors_created": created_vendors,
        "archived": archived,
        "kept": len(keep_ids),
    }
