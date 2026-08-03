"""Evidence-bound budgeting and spending review for Finance.

Dollar amounts are deterministic. The LLM may explain and prioritize those
amounts, but it cannot invent savings or alter bank, accounting, or budget data.
"""

from __future__ import annotations

import hashlib
import html
import json
import logging
import os
import re
from calendar import monthrange
from collections import defaultdict
from datetime import date, datetime, timedelta
from statistics import median
from typing import Any, Iterable, Mapping

from sales_support_agent.models.database import kv_get_json, kv_set_json
from sales_support_agent.services.cashflow.cashflow_helpers import _page_shell
from sales_support_agent.services.cashflow.categorizer import categorize
from sales_support_agent.services.cashflow.finance_nav import render_finance_nav
from sales_support_agent.services.cashflow.obligations import list_obligations
from sales_support_agent.services.cashflow.vendor_aliases import (
    alias_map,
    clean_vendor_display_name,
)


logger = logging.getLogger(__name__)

_CACHE_KEY = "finance_budget_spending_review"
_PROMPT_VERSION = "budget-review-v2-six-month"
_DEFAULT_MODEL = "claude-haiku-4-5-20251001"
_SOURCE_PRIORITY = ("plaid", "qbo_bank", "csv")
_TRANSFER_CATEGORIES = {
    "account_transfer", "bank_transfer", "card_payment", "credit_card",
    "credit_card_payment",
    "internal_transfer", "transfer", "transfers",
}
_TRANSFER_TEXT_MARKERS = (
    "a2a transfer",
    "account transfer",
    "home banking withdrawal anata llc",
    "internal transfer",
    "online transfer",
    "payment to chase",
    "payment to jpmorganchase",
    "transfer between",
    "transfer from share",
    "transfer to share",
    "withdrawal trans to share",
    "withdrawal trans",
    "withdrawal transfer to",
    "withdrawal transfer to share",
    "xfer from",
    "xfer to",
)
_PROTECTED_CATEGORIES = {
    "debt", "debt_service", "insurance", "payroll", "rent", "tax", "taxes",
    "utilities", "critical_utilities", "revenue", "manual_check",
}
_HIGH_CONTROL_CATEGORIES = {
    "advertising", "bank_fees", "entertainment", "fees", "meals",
    "office", "software", "subscriptions", "travel",
}


class BudgetReviewProviderError(RuntimeError):
    """Raised when the advisory model cannot complete a spending review."""


def _slug(value: Any) -> str:
    return "_".join(
        part for part in "".join(
            char.lower() if char.isalnum() else " " for char in str(value or "")
        ).split()
    )


def _event_date(row: Mapping[str, Any]) -> date | None:
    for key in ("posted_date", "effective_date", "due_date", "transaction_date"):
        value = row.get(key)
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        try:
            if value:
                return date.fromisoformat(str(value)[:10])
        except ValueError:
            continue
    return None


def _money(cents: int, *, exact: bool = False) -> str:
    value = int(cents) / 100
    return f"${value:,.2f}" if exact else f"${value:,.0f}"


def _category(row: Mapping[str, Any]) -> str:
    transaction_type = str(row.get("bank_transaction_type") or "").strip().lower()
    description = " ".join(
        str(row.get(field) or "")
        for field in ("friendly_name", "vendor_or_customer", "name", "description")
    )
    # A bare check number is not a vendor. It may be payroll, a contractor,
    # rent, or another protected payment that needs payee evidence before a
    # savings decision. Keep it visible in spending, but out of the trim list.
    if transaction_type == "check" or re.search(
        r"\b(?:check|chk)\s*(?:#|no\.?|number)?\s*\d+\b", description, re.IGNORECASE
    ):
        return "manual_check"
    raw = (
        row.get("category")
        or row.get("personal_finance_category")
        or row.get("transaction_category")
        or "uncategorized"
    )
    key = _slug(raw)
    if key in {"", "other", "uncategorized", "unknown"}:
        key = _slug(categorize(description, str(raw or "")))
    return key or "uncategorized"


def _merchant(row: Mapping[str, Any]) -> str:
    return str(
        row.get("friendly_name")
        or row.get("vendor_or_customer")
        or row.get("name")
        or row.get("description")
        or "Unassigned"
    ).strip()[:120]


def _merchant_identity(
    row: Mapping[str, Any], aliases: Mapping[str, Mapping[str, str]] | None = None,
) -> tuple[str, str]:
    """Resolve an audited merchant identity while preserving raw bank wording."""
    raw = _merchant(row)
    cleaned = clean_vendor_display_name(raw)
    base_key = _slug(cleaned) or _slug(raw) or "unassigned"
    try:
        loaded_aliases = dict(aliases) if aliases is not None else alias_map()
        key = base_key
        seen: set[str] = set()
        while key not in seen:
            seen.add(key)
            matched = key if key in loaded_aliases else next((
                alias for alias in sorted(loaded_aliases, key=len, reverse=True)
                if key.startswith(alias + " ")
            ), "")
            if not matched:
                break
            key = str(loaded_aliases[matched].get("canonical_key") or key)
        display = next((
            str(value.get("canonical_name") or "")
            for value in loaded_aliases.values()
            if str(value.get("canonical_key") or "") == key
            and str(value.get("canonical_name") or "")
        ), cleaned)
    except RuntimeError:
        # Pure calculation tests and offline previews intentionally have no DB.
        key, display = base_key, cleaned
    return key, display


def _account_label(row: Mapping[str, Any]) -> str:
    """Return a safe account hint, never credentials or a full account number."""
    for field in ("account_name", "plaid_account_name", "account_mask", "bank_reference"):
        value = str(row.get(field) or "").strip()
        if value:
            return value[-80:]
    notes = str(row.get("notes") or "")
    match = re.search(r"(?:account|acct)[=: ]+([^;|\n]+)", notes, re.IGNORECASE)
    return match.group(1).strip()[-80:] if match else "Connected bank account"


def _recurrence_classification(
    occurrences: list[dict[str, Any]], *, latest_complete_month: str,
) -> dict[str, Any]:
    """Classify vendor cadence from transaction dates, not monthly totals alone."""
    ordered = sorted(occurrences, key=lambda row: row["date"])
    dates = [row["date"] for row in ordered]
    amounts = [int(row["amount_cents"]) for row in ordered]
    unique_months = sorted({_month_key(day) for day in dates})
    duplicate_signatures: dict[tuple[str, int, str], int] = defaultdict(int)
    for row in ordered:
        duplicate_signatures[(
            row["date"].isoformat(), int(row["amount_cents"]), str(row["account"]),
        )] += 1
    probable_duplicate_cents = sum(
        (count - 1) * signature[1]
        for signature, count in duplicate_signatures.items() if count > 1
    )
    intervals = [(later - earlier).days for earlier, later in zip(dates, dates[1:])]
    typical_interval = int(round(median(intervals))) if intervals else 0
    cadence = "one_time"
    confidence = "low"
    exclusion_reason = "Only one isolated posted charge is available."
    if probable_duplicate_cents:
        cadence = "uncertain"
        exclusion_reason = "Same-day, same-amount charges need duplicate review first."
    elif len(ordered) >= 3 and 20 <= typical_interval <= 40:
        cadence = "monthly" if latest_complete_month in unique_months else "inactive"
        confidence = "high" if len(ordered) >= 5 else "medium"
        exclusion_reason = "" if cadence == "monthly" else "No charge posted in the latest complete month."
    elif len(ordered) >= 2 and 300 <= typical_interval <= 430:
        cadence = "annual"
        confidence = "medium"
        exclusion_reason = ""
    elif len(unique_months) >= 2:
        cadence = "irregular" if latest_complete_month in unique_months else "inactive"
        confidence = "medium"
        exclusion_reason = (
            "Timing does not form a reliable monthly or annual cycle."
            if cadence == "irregular" else "No charge posted in the latest complete month."
        )
    stable_amount = int(median(amounts[-3:])) if amounts else 0
    earlier_amount = int(median(amounts[:-3])) if len(amounts) >= 6 else stable_amount
    increase_cents = max(0, stable_amount - earlier_amount)
    price_increase = bool(
        earlier_amount and increase_cents >= 1_000
        and stable_amount * 100 >= earlier_amount * 110
    )
    expected_next = dates[-1] + timedelta(days=(365 if cadence == "annual" else typical_interval or 30))
    return {
        "cadence": cadence,
        "confidence": confidence,
        "typical_interval_days": typical_interval,
        "baseline_amount_cents": stable_amount,
        "prior_amount_cents": earlier_amount,
        "price_increase": price_increase,
        "price_increase_cents": increase_cents if price_increase else 0,
        "probable_duplicate_cents": probable_duplicate_cents,
        "exclusion_reason": exclusion_reason,
        "last_charge_date": dates[-1].isoformat() if dates else "",
        "next_expected_date": expected_next.isoformat() if dates else "",
    }


def _is_transfer(row: Mapping[str, Any], category: str) -> bool:
    """Identify internal money movement even when Plaid leaves it uncategorized."""
    if category in _TRANSFER_CATEGORIES:
        return True
    text = " ".join(
        str(row.get(key) or "")
        for key in (
            "friendly_name",
            "vendor_or_customer",
            "name",
            "description",
            "subcategory",
            "bank_transaction_type",
            "bank_reference",
        )
    )
    normalized = " ".join(
        "".join(char if char.isalnum() else " " for char in text.lower()).split()
    )
    return any(marker in normalized for marker in _TRANSFER_TEXT_MARKERS)


def _month_key(value: date) -> str:
    return f"{value.year:04d}-{value.month:02d}"


def _previous_months(today: date, count: int = 3) -> list[str]:
    year, month = today.year, today.month
    values: list[str] = []
    for _ in range(count):
        month -= 1
        if month == 0:
            month = 12
            year -= 1
        values.append(f"{year:04d}-{month:02d}")
    return list(reversed(values))


def _canonical_transactions(
    rows: Iterable[Mapping[str, Any]], *, as_of: date
) -> tuple[str, list[dict[str, Any]]]:
    """Choose one posted source so mirrored providers cannot double-count."""
    candidates: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for raw in rows:
        row = dict(raw)
        source = str(row.get("source") or "").lower()
        if source not in _SOURCE_PRIORITY:
            continue
        if str(row.get("event_type") or "").lower() != "outflow":
            continue
        if str(row.get("status") or "").lower() not in {"posted", "matched"}:
            continue
        occurred = _event_date(row)
        amount = int(row.get("amount_cents") or 0)
        category = _category(row)
        if (
            occurred is None
            or occurred > as_of
            or amount <= 0
            or _is_transfer(row, category)
        ):
            continue
        row["_budget_date"] = occurred
        row["_budget_category"] = category
        row["_budget_merchant"] = _merchant(row)
        candidates[source].append(row)
    for source in _SOURCE_PRIORITY:
        if candidates[source]:
            candidates[source].sort(
                key=lambda item: (
                    item["_budget_date"],
                    str(item.get("source_id") or item.get("id") or ""),
                )
            )
            return source, candidates[source]
    return "unavailable", []


def build_budget_view(
    rows: Iterable[Mapping[str, Any]], *, as_of: date | None = None
) -> dict[str, Any]:
    """Build the monthly budget from one canonical posted-transaction source."""
    today = as_of or date.today()
    source, transactions = _canonical_transactions(rows, as_of=today)
    comparison_months = _previous_months(today, count=6)
    current_month = _month_key(today)
    category_months: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    category_merchants: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    merchant_months: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    merchant_meta: dict[str, dict[str, str]] = {}
    merchant_occurrences: dict[str, list[dict[str, Any]]] = defaultdict(list)
    merchant_charge_signatures: dict[str, dict[tuple[str, int], int]] = defaultdict(
        lambda: defaultdict(int)
    )
    latest_date: date | None = None
    earliest_date: date | None = None
    try:
        aliases = alias_map()
    except RuntimeError:
        aliases = {}
    for row in transactions:
        occurred = row["_budget_date"]
        key = row["_budget_category"]
        month = _month_key(occurred)
        merchant_key, merchant_display = _merchant_identity(row, aliases)
        merchant_meta[merchant_key] = {
            "name": merchant_display, "category": key,
        }
        if occurred >= today - timedelta(days=430) and month != current_month:
            merchant_occurrences[merchant_key].append({
                "transaction_id": str(row.get("source_id") or row.get("id") or ""),
                "date": occurred,
                "amount_cents": int(row.get("amount_cents") or 0),
                "account": _account_label(row),
                "raw_description": row["_budget_merchant"],
            })
        if month in comparison_months or month == current_month:
            category_months[key][month] += int(row.get("amount_cents") or 0)
            category_merchants[key][row["_budget_merchant"]] += int(
                row.get("amount_cents") or 0
            )
            merchant_months[merchant_key][month] += int(row.get("amount_cents") or 0)
            if month in comparison_months:
                merchant_charge_signatures[merchant_key][
                    (occurred.isoformat(), int(row.get("amount_cents") or 0))
                ] += 1
        latest_date = max(latest_date or occurred, occurred)
        earliest_date = min(earliest_date or occurred, occurred)

    days_in_month = monthrange(today.year, today.month)[1]
    elapsed_days = max(1, today.day)
    recurrence_facts = {
        merchant_key: _recurrence_classification(
            occurrences, latest_complete_month=comparison_months[-1]
        )
        for merchant_key, occurrences in merchant_occurrences.items()
    }
    recurring_category_average: dict[str, int] = defaultdict(int)
    for merchant_key, facts in recurrence_facts.items():
        if facts["cadence"] not in {"monthly", "annual"}:
            continue
        monthly_value = int(facts["baseline_amount_cents"])
        if facts["cadence"] == "annual":
            monthly_value //= 12
        recurring_category_average[merchant_meta[merchant_key]["category"]] += monthly_value
    categories: list[dict[str, Any]] = []
    for key in sorted(category_months):
        monthly = category_months[key]
        historical = [int(monthly.get(month) or 0) for month in comparison_months]
        average = sum(historical) // len(comparison_months)
        earlier_average = sum(historical[:3]) // 3
        recent_average = sum(historical[3:]) // 3
        trend_cents = recent_average - earlier_average
        trend_bps = trend_cents * 10_000 // max(earlier_average, 1)
        trend_direction = (
            "up" if trend_bps >= 500 else "down" if trend_bps <= -500 else "flat"
        )
        current = int(monthly.get(current_month) or 0)
        projected = current * days_in_month // elapsed_days
        protected = key in _PROTECTED_CATEGORIES
        reduction_bps = 0 if protected else 1_500 if key in _HIGH_CONTROL_CATEGORIES else 1_000
        target = average * (10_000 - reduction_bps) // 10_000
        potential = 0 if protected else max(0, projected - target)
        recurring_average = int(recurring_category_average.get(key) or 0)
        historical_reduction = (
            0 if protected else recurring_average * reduction_bps // 10_000
        )
        recurring_saving = min(historical_reduction, potential)
        variance = projected - target
        merchants = sorted(
            category_merchants[key].items(), key=lambda item: (-item[1], item[0].casefold())
        )[:3]
        categories.append(
            {
                "key": key,
                "label": key.replace("_", " ").title(),
                "protected": protected,
                "historical_months": dict(zip(comparison_months, historical)),
                "average_cents": average,
                "current_cents": current,
                "projected_cents": projected,
                "target_cents": target,
                "potential_saving_cents": potential,
                "historical_reduction_cents": historical_reduction,
                "recurring_average_cents": recurring_average,
                "recurring_saving_cents": recurring_saving,
                "variance_cents": variance,
                "earlier_average_cents": earlier_average,
                "recent_average_cents": recent_average,
                "trend_cents": trend_cents,
                "trend_bps": trend_bps,
                "trend_direction": trend_direction,
                "top_merchants": [
                    {"name": name, "amount_cents": amount} for name, amount in merchants
                ],
            }
        )
    categories.sort(
        key=lambda item: (
            -max(item["variance_cents"], 0),
            -item["potential_saving_cents"],
            -item["average_cents"],
            item["label"],
        )
    )
    totals = {
        "average_cents": sum(item["average_cents"] for item in categories),
        "current_cents": sum(item["current_cents"] for item in categories),
        "projected_cents": sum(item["projected_cents"] for item in categories),
        "target_cents": sum(item["target_cents"] for item in categories),
        "potential_saving_cents": sum(
            item["potential_saving_cents"] for item in categories
        ),
        "recurring_saving_cents": sum(
            item["recurring_saving_cents"] for item in categories
        ),
    }
    monthly_totals = [
        {
            "month": month,
            "amount_cents": sum(
                int(item["historical_months"].get(month) or 0) for item in categories
            ),
        }
        for month in comparison_months
    ]
    investigations: list[dict[str, Any]] = []
    for merchant_key, monthly in merchant_months.items():
        meta = merchant_meta[merchant_key]
        if meta["category"] in _PROTECTED_CATEGORIES:
            continue
        earlier = [int(monthly.get(month) or 0) for month in comparison_months[:3]]
        recent = [int(monthly.get(month) or 0) for month in comparison_months[3:]]
        earlier_average = sum(earlier) // 3
        recent_average = sum(recent) // 3
        active_recent_months = sum(1 for amount in recent if amount > 0)
        if earlier_average == 0 and active_recent_months >= 2 and recent_average >= 2_500:
            investigations.append({
                "kind": "new_recurring", "merchant": meta["name"],
                "category": meta["category"], "headline": "New recurring cost",
                "monthly_review_cents": recent_average, "one_time_review_cents": 0,
                "evidence": f"Appeared in {active_recent_months} of the last 3 complete months after no spend in the earlier 3.",
                "action": "Confirm the service is still needed, then cancel, downgrade, or set an owner.",
            })
        elif earlier_average >= 2_500 and recent_average - earlier_average >= 5_000 and recent_average * 100 >= earlier_average * 125:
            investigations.append({
                "kind": "rising_vendor", "merchant": meta["name"],
                "category": meta["category"], "headline": "Vendor cost increased",
                "monthly_review_cents": recent_average - earlier_average,
                "one_time_review_cents": 0,
                "evidence": f"Recent 3-month average {_money(recent_average, exact=True)} versus {_money(earlier_average, exact=True)} in the earlier 3 months.",
                "action": "Check usage, seats, rate changes, and contract terms before the next renewal.",
            })
        duplicate_cents = sum(
            max(0, count - 1) * amount
            for (_posted_on, amount), count in merchant_charge_signatures[merchant_key].items()
            if count > 1
        )
        if duplicate_cents >= 1_000:
            duplicate_sets = sum(1 for count in merchant_charge_signatures[merchant_key].values() if count > 1)
            investigations.append({
                "kind": "duplicate_looking", "merchant": meta["name"],
                "category": meta["category"], "headline": "Duplicate-looking charges",
                "monthly_review_cents": 0, "one_time_review_cents": duplicate_cents,
                "evidence": f"{duplicate_sets} same-day, same-amount charge set(s) appeared in the six complete months. These may be valid and require receipt review.",
                "action": "Match the charges to invoices or receipts and dispute only confirmed duplicates.",
            })
    investigations.sort(key=lambda item: (
        -int(item["monthly_review_cents"]), -int(item["one_time_review_cents"]),
        str(item["merchant"]).casefold(),
    ))
    trim_items: list[dict[str, Any]] = []
    for merchant_key, monthly in merchant_months.items():
        meta = merchant_meta[merchant_key]
        if meta["category"] in _PROTECTED_CATEGORIES:
            continue
        history = {month: int(monthly.get(month) or 0) for month in comparison_months}
        total = sum(history.values())
        if total <= 0:
            continue
        occurrence_evidence = [
            {
                "transaction_id": item["transaction_id"],
                "date": item["date"].isoformat(),
                "amount_cents": item["amount_cents"],
                "account": item["account"],
            }
            for item in merchant_occurrences.get(merchant_key, [])
        ]
        evidence = {
            "merchant": merchant_key, "category": meta["category"],
            "months": history, "comparison_months": comparison_months,
            "transactions": occurrence_evidence,
        }
        opportunity_key = hashlib.sha256(f"budget-trim-v1|{merchant_key}".encode()).hexdigest()
        evidence_hash = hashlib.sha256(
            json.dumps(evidence, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        recent_average = sum(history[month] for month in comparison_months[3:]) // 3
        active_months = sum(1 for value in history.values() if value > 0)
        facts = recurrence_facts.get(merchant_key) or {
            "cadence": "one_time", "confidence": "low", "baseline_amount_cents": 0,
            "probable_duplicate_cents": 0, "exclusion_reason": "Insufficient history.",
            "last_charge_date": "", "next_expected_date": "", "price_increase": False,
            "price_increase_cents": 0, "typical_interval_days": 0,
        }
        cadence = str(facts["cadence"])
        is_recurring = cadence in {"monthly", "annual"}
        baseline = int(facts["baseline_amount_cents"])
        monthly_potential = baseline // 12 if cadence == "annual" else baseline if cadence == "monthly" else 0
        occurrences = merchant_occurrences.get(merchant_key, [])
        trim_items.append({
            "opportunity_key": opportunity_key, "key": opportunity_key,
            "evidence_hash": evidence_hash, "display_name": meta["name"],
            "normalized_merchant": merchant_key, "category": meta["category"],
            "cadence": cadence,
            "monthly_potential_cents": monthly_potential,
            "baseline_amount_cents": baseline if is_recurring else 0,
            "six_month_total_cents": total, "monthly_average_cents": total // 6,
            "recent_average_cents": recent_average,
            "active_months": active_months, "monthly_history": history,
            "reason": (
                f"{cadence.title()} controllable cost supported by posted charges"
                if is_recurring
                else "Previously recurring but absent from the latest complete month"
                if cadence == "inactive"
                else "Duplicate-looking activity needs review before savings analysis"
                if cadence == "uncertain"
                else "One-time or irregular historical purchase review"
            ),
            "limitations": str(facts.get("exclusion_reason") or "Usage, contract terms, and replacement cost require operator review."),
            "evidence_dates": [item["date"].isoformat() for item in occurrences],
            "transactions": [{
                **item, "date": item["date"].isoformat(),
            } for item in occurrences],
            "last_charge_date": facts.get("last_charge_date"),
            "next_expected_date": facts.get("next_expected_date"),
            "confidence": facts.get("confidence"),
            "typical_interval_days": facts.get("typical_interval_days"),
            "probable_duplicate_cents": facts.get("probable_duplicate_cents"),
            "price_increase": facts.get("price_increase"),
            "price_increase_cents": facts.get("price_increase_cents"),
            "review_state": "unknown", "review_note": "",
        })
    trim_items.sort(key=lambda item: (
        {"monthly": 0, "annual": 1, "inactive": 2, "irregular": 3, "one_time": 4, "uncertain": 5}.get(item["cadence"], 6),
        -int(item["six_month_total_cents"]), str(item["display_name"]).casefold(),
    ))
    proof = {
        "source": source,
        "as_of": today.isoformat(),
        "earliest_date": earliest_date.isoformat() if earliest_date else "",
        "latest_date": latest_date.isoformat() if latest_date else "",
        "coverage_days": (
            (latest_date - earliest_date).days + 1
            if latest_date is not None and earliest_date is not None
            else 0
        ),
        "comparison_months": comparison_months,
        "totals": totals,
        "monthly_totals": monthly_totals,
        "categories": categories,
        "investigations": investigations[:12],
        "trim_items": trim_items[:100],
    }
    return {
        **proof,
        "calculation_id": hashlib.sha256(
            json.dumps(proof, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()[:16],
        "transaction_count": len(transactions),
        "status": "ready" if categories else "empty",
    }


def load_budget_view() -> dict[str, Any]:
    try:
        source_rows = list_obligations(limit=10_000)
        view = build_budget_view(source_rows)
        from sales_support_agent.services.cashflow.savings_reviews import load_savings_reviews
        reviews = load_savings_reviews()
        try:
            aliases = alias_map()
        except RuntimeError:
            aliases = {}
        current_day = date.today()
        for item in view.get("trim_items") or []:
            review = reviews.get(str(item.get("opportunity_key") or ""))
            if review:
                item["review_state"] = str(review.get("state") or "unknown")
                item["review_note"] = str(review.get("reason") or "")
                item["review"] = {
                    key: review.get(key) for key in (
                        "owner", "action_type", "cancellation_started_at",
                        "cancellation_confirmed_at", "effective_date",
                        "expected_verification_date", "proof_note",
                        "realized_monthly_cents", "updated_at",
                    )
                }
                effective = review.get("effective_date")
                expected = review.get("expected_verification_date")
                try:
                    effective_day = effective if isinstance(effective, date) else date.fromisoformat(str(effective)[:10])
                except (TypeError, ValueError):
                    effective_day = None
                try:
                    expected_day = expected if isinstance(expected, date) else date.fromisoformat(str(expected)[:10])
                except (TypeError, ValueError):
                    expected_day = None
                later_matches = []
                if effective_day:
                    for raw in source_rows:
                        occurred = _event_date(raw)
                        if (
                            occurred and occurred > effective_day
                            and str(raw.get("source") or "").lower() == "plaid"
                            and str(raw.get("event_type") or "").lower() == "outflow"
                            and str(raw.get("status") or "").lower() in {"posted", "matched"}
                            and _merchant_identity(raw, aliases)[0] == item["normalized_merchant"]
                        ):
                            later_matches.append({
                                "date": occurred.isoformat(),
                                "amount_cents": int(raw.get("amount_cents") or 0),
                            })
                materially_returned = any(
                    int(row["amount_cents"]) > int(item.get("baseline_amount_cents") or 0) * 20 // 100
                    for row in later_matches
                )
                item["verification_ready"] = bool(
                    item["review_state"] == "verifying" and expected_day
                    and current_day >= expected_day and not materially_returned
                )
                item["charge_returned"] = bool(
                    item["review_state"] == "realized" and materially_returned
                )
                item["verification_matches"] = later_matches
        recurring_items = [
            item for item in view.get("trim_items") or []
            if item.get("cadence") in {"monthly", "annual"}
        ]
        view["savings_summary"] = {
            "potential_monthly_cents": sum(
                int(item.get("monthly_potential_cents") or 0)
                for item in recurring_items
                if item.get("review_state") in {"unknown", "investigate"}
            ),
            "committed_monthly_cents": sum(
                int(item.get("monthly_potential_cents") or 0)
                for item in recurring_items
                if item.get("review_state") in {"waste", "cancellation_started", "verifying"}
            ),
            "realized_monthly_cents": sum(
                int((item.get("review") or {}).get("realized_monthly_cents") or 0)
                for item in recurring_items
                if item.get("review_state") == "realized" and not item.get("charge_returned")
            ),
        }
        return view
    except Exception:
        return build_budget_view([])


def _review_packet(view: Mapping[str, Any]) -> dict[str, Any]:
    eligible = [
        item
        for item in view.get("categories") or []
        if (
            not item.get("protected")
            and int(item.get("recurring_saving_cents") or 0) > 0
        )
    ][:12]
    return {
        "prompt_version": _PROMPT_VERSION,
        "calculation_id": view.get("calculation_id"),
        "source": view.get("source"),
        "as_of": view.get("as_of"),
        "comparison_months": view.get("comparison_months"),
        "totals": view.get("totals"),
        "categories": [
            {
                "key": item["key"],
                "average_cents": item["average_cents"],
                "current_cents": item["current_cents"],
                "projected_cents": item["projected_cents"],
                "target_cents": item["target_cents"],
                "deterministic_potential_saving_cents": item["potential_saving_cents"],
                "deterministic_recurring_saving_cents": item[
                    "recurring_saving_cents"
                ],
                "trend_direction": item["trend_direction"],
                "trend_bps": item["trend_bps"],
                "historical_months": item["historical_months"],
                "top_merchants": item["top_merchants"],
            }
            for item in eligible
        ],
    }


def _qualitative_text(value: Any, fallback: str, *, limit: int = 500) -> str:
    """Keep model prose qualitative; every displayed financial figure is deterministic."""
    text = str(value or "").strip()
    if not text or any(char.isdigit() for char in text) or any(
        marker in text for marker in ("$", "%")
    ):
        return fallback
    return text[:limit]


def run_budget_review(settings: Any, *, force: bool = False) -> dict[str, Any]:
    """Run a high-level LLM review over deterministic category evidence."""
    view = load_budget_view()
    packet = _review_packet(view)
    packet_hash = hashlib.sha256(
        json.dumps(packet, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    cached = kv_get_json(_CACHE_KEY) or {}
    if not force and cached.get("packet_hash") == packet_hash:
        return {**cached, "cached": True}
    if not packet["categories"]:
        stored = {
            "status": "empty",
            "packet_hash": packet_hash,
            "calculation_id": view["calculation_id"],
            "recommendations": [],
            "summary": "There is not enough eligible posted spending to run a savings review.",
        }
        kv_set_json(_CACHE_KEY, stored)
        return stored
    api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        return {
            "status": "not_configured",
            "packet_hash": packet_hash,
            "calculation_id": view["calculation_id"],
            "recommendations": [],
        }
    model = (
        os.getenv("FINANCE_BUDGET_REVIEW_MODEL")
        or os.getenv("FINANCE_SMART_CFO_MODEL")
        or _DEFAULT_MODEL
    ).strip()
    result = _call_anthropic(api_key, model, packet)
    valid_keys = {item["key"]: item for item in packet["categories"]}
    recommendations: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for raw in list(result.get("recommendations") or [])[:6]:
        if not isinstance(raw, Mapping):
            continue
        key = str(raw.get("category_key") or "")
        evidence = valid_keys.get(key)
        if not evidence or key in seen_keys:
            continue
        seen_keys.add(key)
        label = key.replace("_", " ").title()
        merchant_names = [
            str(item.get("name") or "").strip()
            for item in evidence.get("top_merchants") or []
            if str(item.get("name") or "").strip()
        ]
        merchant_note = ", ".join(merchant_names[:3]) or f"current {label.lower()} vendors"
        confidence = str(raw.get("confidence") or "").strip().lower()
        if confidence not in {"high", "medium", "low"}:
            confidence = "medium"
        recommendations.append(
            {
                "headline": _qualitative_text(
                    raw.get("headline"), f"Review {label.lower()} commitments"
                ),
                "reason": (
                    f"The six-month {label.lower()} average supports a recurring "
                    f"{_money(evidence['deterministic_recurring_saving_cents'], exact=True)} "
                    f"monthly reduction target. Recent spending is "
                    f"{str(evidence['trend_direction']).replace('_', ' ')} versus the earlier "
                    f"three months. The largest posted names are {merchant_note}."
                ),
                "next_action": _qualitative_text(
                    raw.get("next_action"),
                    f"Confirm which {label.lower()} costs are still necessary and remove or renegotiate avoidable commitments.",
                ),
                "confidence": confidence,
                "category_key": key,
                "category_label": label,
                "potential_saving_cents": evidence[
                    "deterministic_potential_saving_cents"
                ],
                "recurring_saving_cents": evidence[
                    "deterministic_recurring_saving_cents"
                ],
                "average_cents": evidence["average_cents"],
                "projected_cents": evidence["projected_cents"],
                "top_merchants": evidence["top_merchants"],
            }
        )
    total_saving = int(packet.get("totals", {}).get("recurring_saving_cents") or 0)
    category_labels = ", ".join(
        str(item.get("category_label") or "") for item in recommendations
    )
    stored = {
        "status": "ready",
        "packet_hash": packet_hash,
        "calculation_id": view["calculation_id"],
        "created_at": datetime.utcnow().isoformat(),
        "model": model,
        "summary": (
            f"The six-month review supports {_money(total_saving, exact=True)} in recurring "
            f"monthly savings targets across controllable spending. Start with "
            f"{category_labels or 'the reviewed categories'} below."
        ),
        "recommendations": recommendations,
        "cached": False,
    }
    kv_set_json(_CACHE_KEY, stored)
    return stored


def _call_anthropic(
    api_key: str, model: str, packet: Mapping[str, Any]
) -> Mapping[str, Any]:
    import anthropic

    schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "summary": {"type": "string"},
            "recommendations": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "category_key": {
                            "type": "string",
                            "enum": [item["key"] for item in packet["categories"]],
                        },
                        "headline": {"type": "string"},
                        "reason": {"type": "string"},
                        "next_action": {"type": "string"},
                        "confidence": {
                            "type": "string",
                            "enum": ["high", "medium", "low"],
                        },
                    },
                    "required": [
                        "category_key", "headline", "reason", "next_action", "confidence"
                    ],
                },
            },
        },
        "required": ["summary", "recommendations"],
    }
    try:
        message = anthropic.Anthropic(api_key=api_key).messages.create(
            model=model,
            max_tokens=1800,
            system=(
                "You are a cost-control CFO. Analyze only the supplied posted-spending "
                "trends. Prioritize practical cuts that improve end-of-month cash. "
                "Never invent a dollar amount, merchant, category, contract, or cancellation. "
                "Dollar savings are calculated outside the model. Do not recommend reducing "
                "payroll, tax, debt, rent, insurance, or utilities. Do not include any numbers, "
                "currency, percentages, or calculated comparisons in your prose. Return concise "
                "qualitative advice only."
            ),
            messages=[
                {
                    "role": "user",
                    "content": json.dumps(packet, separators=(",", ":")),
                }
            ],
            tools=[
                {
                    "name": "submit_budget_review",
                    "description": "Submit evidence-bound spending reduction priorities.",
                    "input_schema": schema,
                }
            ],
            tool_choice={"type": "tool", "name": "submit_budget_review"},
        )
    except Exception as exc:
        logger.warning("Budget review failed for model %s: %s", model, type(exc).__name__)
        raise BudgetReviewProviderError("The high spending review could not finish.") from exc
    for block in list(getattr(message, "content", None) or []):
        if (
            getattr(block, "type", "") == "tool_use"
            and getattr(block, "name", "") == "submit_budget_review"
            and isinstance(getattr(block, "input", None), Mapping)
        ):
            return getattr(block, "input")
    raise BudgetReviewProviderError("The high spending review returned no usable analysis.")


def load_budget_review() -> dict[str, Any]:
    try:
        return kv_get_json(_CACHE_KEY) or {"status": "empty", "recommendations": []}
    except Exception:
        return {"status": "empty", "recommendations": []}


def render_budget_vendor_page(
    view: Mapping[str, Any], opportunity_key: str, *, flash: str = ""
) -> str:
    """Render one evidence-first savings decision as a normal page."""
    item = next(
        (
            dict(candidate) for candidate in view.get("trim_items") or []
            if str(candidate.get("opportunity_key") or "") == opportunity_key
        ),
        None,
    )
    if item is None:
        body = f"""<div class="money-brief">{render_finance_nav("budget", counts={})}
        <div class="money-empty"><h1>This cost is no longer in the current review</h1>
        <p>Its bank evidence may have changed. Return to Budget &amp; savings for the current list.</p>
        <a class="btn btn-primary" href="/admin/finances/budget">Return to Budget &amp; savings</a></div></div>"""
        return _page_shell("Cost review", "budget", body, flash=flash)
    item["realization_ready"] = bool(item.get("verification_ready"))
    payload = html.escape(json.dumps(item, separators=(",", ":"), default=str), quote=True)
    history = "".join(
        f"<tr><td>{html.escape(str(tx.get('date') or ''))}</td>"
        f"<td>{html.escape(str(tx.get('account') or 'Connected bank account'))}</td>"
        f"<td>{html.escape(str(tx.get('raw_description') or item['display_name']))}</td>"
        f"<td>{_money(int(tx.get('amount_cents') or 0), exact=True)}</td></tr>"
        for tx in item.get("transactions") or []
    )
    state = str(item.get("review_state") or "unknown")
    state_label = {
        "unknown": "Needs a decision", "investigate": "Needs investigation",
        "needed": "Kept", "waste": "Ready to cut",
        "cancellation_started": "Cancellation started", "verifying": "Verifying the charge stopped",
        "realized": "Savings confirmed", "cannot_cancel": "Cannot cancel",
    }.get(state, state.replace("_", " ").title())
    common = f"""
      <input type="hidden" name="opportunity_json" value="{payload}">
      <input type="hidden" name="evidence_hash" value="{html.escape(str(item['evidence_hash']), quote=True)}">
      <input type="hidden" name="return_to" value="/admin/finances/budget/vendor/{html.escape(opportunity_key, quote=True)}">
    """
    if state == "waste":
        action_panel = f"""<form class="cost-action-form" method="post" action="/admin/finances/savings/{html.escape(opportunity_key, quote=True)}/review">
          {common}<input type="hidden" name="action" value="start_cancellation">
          <h2>Start the cost-cutting action</h2><p>This records the work. It does not contact the vendor.</p>
          <div class="cost-action-grid"><label>Owner<input name="owner" value="David Narayan" required></label>
          <label>Action<select name="action_type" required><option value="cancel">Cancel</option><option value="downgrade">Downgrade</option><option value="renegotiate">Renegotiate</option><option value="dispute">Dispute duplicate</option><option value="investigate_duplicate">Investigate duplicate</option></select></label></div>
          <label>Working note<textarea name="proof_note" rows="3" placeholder="Vendor login, contract detail, or next step"></textarea></label>
          <button class="btn btn-primary" type="submit">Start cancellation work</button></form>"""
    elif state == "cancellation_started":
        action_panel = f"""<form class="cost-action-form" method="post" action="/admin/finances/savings/{html.escape(opportunity_key, quote=True)}/review">
          {common}<input type="hidden" name="action" value="confirm_cancellation">
          <h2>Record the vendor confirmation</h2><p>Finance will wait for the next expected charge window before calling this saved.</p>
          <label>Effective date<input type="date" name="effective_date" required></label>
          <label>Confirmation or proof<textarea name="proof_note" rows="4" required placeholder="Confirmation number, email summary, or downgrade details"></textarea></label>
          <button class="btn btn-primary" type="submit">Confirm cancellation details</button></form>"""
    elif state == "verifying":
        review = item.get("review") or {}
        verify_on = html.escape(str(review.get("expected_verification_date") or "the next expected charge window"))
        if item.get("verification_ready"):
            action_panel = f"""<form class="cost-action-form" method="post" action="/admin/finances/savings/{html.escape(opportunity_key, quote=True)}/review">
              {common}<input type="hidden" name="action" value="confirm_realized">
              <h2>Plaid verifies the charge stopped</h2><p>No comparable posted charge appeared through {verify_on}. Confirm this evidence to count the saving.</p>
              <button class="btn btn-primary" type="submit">Record bank-verified savings</button></form>"""
        else:
            action_panel = f"""<div class="cost-action-form"><h2>Waiting for bank proof</h2>
              <p>Finance will check posted Plaid activity through {verify_on}. Potential savings are not included in verified cash.</p></div>"""
    elif state == "realized":
        warning = "<div class=\"money-alert money-alert--danger\"><strong>A matching charge returned.</strong><p>Reopen this action and remove it from confirmed savings.</p></div>" if item.get("charge_returned") else ""
        action_panel = f"""{warning}<form class="cost-action-form" method="post" action="/admin/finances/savings/{html.escape(opportunity_key, quote=True)}/review">
          {common}<input type="hidden" name="action" value="reopen"><h2>Savings confirmed</h2>
          <p>{_money(int((item.get('review') or {}).get('realized_monthly_cents') or 0), exact=True)} per month is supported by later Plaid evidence.</p>
          <button class="btn btn-secondary" type="submit">Reopen this cost</button></form>"""
    else:
        action_panel = f"""<form class="cost-action-form" method="post" action="/admin/finances/savings/{html.escape(opportunity_key, quote=True)}/review">
          {common}<h2>Choose what this cost means</h2><p>Nothing changes until you confirm one answer.</p>
          <label>Decision<select name="action"><option value="needed">Needed</option><option value="investigate">Investigate</option><option value="waste">Waste — prepare to cut</option><option value="unknown">Unknown</option></select></label>
          <label>Note<textarea name="reason" rows="3" placeholder="Why you chose this"></textarea></label>
          <button class="btn btn-primary" type="submit">Save this decision</button></form>"""
    body = f"""
    <div class="money-brief cost-review-page">
      {render_finance_nav("budget", counts={})}
      <a class="money-back-link" href="/admin/finances/budget#trim-title">← Back to Budget &amp; savings</a>
      <header class="money-page-header"><div><p class="finance-eyebrow">Cost review</p>
      <h1>{html.escape(item['display_name'])}</h1><p class="money-page-subtitle">See the bank evidence, then take one clear next step.</p></div>
      <div class="money-page-status"><span class="money-status money-status--review">{html.escape(state_label)}</span>
      <span>{html.escape(str(item.get('confidence') or 'low').title())} evidence confidence</span></div></header>
      <section class="cost-evidence-summary" aria-label="Cost evidence summary">
        <article><span>Current frequency</span><strong>{html.escape(str(item.get('cadence') or 'uncertain').replace('_', ' ').title())}</strong></article>
        <article><span>Estimated monthly cost</span><strong>{_money(int(item.get('monthly_potential_cents') or 0), exact=True)}</strong></article>
        <article><span>Last posted charge</span><strong>{html.escape(str(item.get('last_charge_date') or 'Unavailable'))}</strong></article>
        <article><span>Next expected</span><strong>{html.escape(str(item.get('next_expected_date') or 'Uncertain'))}</strong></article>
      </section>
      <section class="budget-workspace"><div class="money-section-heading"><div><p class="finance-eyebrow">Why this is here</p><h2>{html.escape(str(item.get('reason') or 'Review posted cost'))}</h2></div></div>
      <p>{html.escape(str(item.get('limitations') or 'Verify usage and contract terms before acting.'))}</p>
      {f'<div class="money-alert money-alert--warning"><strong>Possible duplicate</strong><p>{_money(int(item.get("probable_duplicate_cents") or 0), exact=True)} requires receipt or invoice review and is excluded from recurring savings.</p></div>' if item.get('probable_duplicate_cents') else ''}
      <div class="money-table-wrap"><table class="budget-table"><thead><tr><th>Date</th><th>Account</th><th>Bank description</th><th>Posted amount</th></tr></thead><tbody>{history}</tbody></table></div></section>
      {action_panel}
      <p class="budget-rule-note">This workflow never cancels a service, moves money, runs payroll, or edits QuickBooks.</p>
    </div>"""
    return _page_shell(f"Review {item['display_name']}", "budget", body, flash=flash)


def render_budget_page(
    view: Mapping[str, Any], review: Mapping[str, Any], *, flash: str = ""
) -> str:
    """Render budgets and LLM advice without any financial mutation controls."""
    if view.get("status") != "ready":
        body = f"""
        <div class="money-brief">
          {render_finance_nav("budget", counts={})}
          <header class="money-page-header"><div><p class="finance-eyebrow">Budget</p>
          <h1>Build a budget from posted spending</h1>
          <p class="money-page-subtitle">Finance needs posted bank history before it can calculate a trustworthy monthly budget.</p></div></header>
          <div class="money-empty"><h2>No eligible spending history is available</h2>
          <p>Refresh Accounts, then return here. Finance will never create a budget from forecast obligations.</p>
          <a class="btn btn-primary" href="/admin/finances/accounts">Check accounts</a></div>
        </div>"""
        return _page_shell("Budget", "budget", body, flash=flash)

    totals = view["totals"]
    month_totals = list(view.get("monthly_totals") or [])
    trend_max = max(
        [int(item.get("amount_cents") or 0) for item in month_totals] or [1]
    )
    trend_columns = "".join(
        f"""
        <div class="budget-trend-column">
          <strong>{_money(int(item['amount_cents']))}</strong>
          <div class="budget-trend-track"><span style="height:{max(8, int(item['amount_cents']) * 100 // trend_max)}%"></span></div>
          <span>{html.escape(date.fromisoformat(str(item['month']) + '-01').strftime('%b'))}</span>
        </div>"""
        for item in month_totals
    )
    rows = "".join(
        f"""
        <tr>
          <td><strong>{html.escape(item['label'])}</strong>
          <span>{'Protected cost' if item['protected'] else ', '.join(html.escape(m['name']) for m in item['top_merchants'][:2]) or 'Posted transactions'}</span></td>
          <td>{_money(item['average_cents'])}</td>
          <td><span class="budget-trend-label budget-trend-label--{item['trend_direction']}">{item['trend_direction'].title()}</span></td>
          <td>{_money(item['projected_cents'])}</td>
          <td>{_money(item['target_cents'])}</td>
          <td class="{'is-over' if item['variance_cents'] > 0 else 'is-under'}">{'+' if item['variance_cents'] > 0 else ''}{_money(item['variance_cents'])}</td>
        </tr>"""
        for item in view["categories"]
    )
    investigation_rows = "".join(
        f"""
        <article class="budget-investigation">
          <div><span>{html.escape(item['headline'])} · {html.escape(str(item['category']).replace('_', ' ').title())}</span>
          <h3>{html.escape(item['merchant'])}</h3><p>{html.escape(item['evidence'])}</p></div>
          <div class="budget-investigation__amount"><span>{'Monthly increase to review' if item['monthly_review_cents'] else 'One-time charges to verify'}</span>
          <strong>{_money(int(item['monthly_review_cents'] or item['one_time_review_cents']), exact=True)}</strong></div>
          <p class="budget-saving-action"><strong>Do next:</strong> {html.escape(item['action'])}</p>
        </article>"""
        for item in view.get("investigations") or []
    )
    trim_items = list(view.get("trim_items") or [])
    recurring_trim_count = sum(1 for item in trim_items if item.get("cadence") in {"monthly", "annual"})
    inactive_trim_count = sum(1 for item in trim_items if item.get("cadence") == "inactive")
    one_time_trim_count = sum(1 for item in trim_items if item.get("cadence") in {"one_time", "irregular", "uncertain"})
    trim_counts = {
        state: sum(1 for item in trim_items if str(item.get("review_state") or "unknown") == state)
        for state in ("unknown", "needed", "investigate", "waste")
    }
    actionable_items = [
        item for item in trim_items
        if (
            item.get("cadence") in {"monthly", "annual"}
            and str(item.get("review_state") or "unknown") in {"unknown", "investigate"}
        )
    ][:5]
    actionable_keys = {
        str(item.get("opportunity_key") or "") for item in actionable_items
    }
    ready_to_cut_count = sum(
        1 for item in trim_items
        if item.get("cadence") in {"monthly", "annual"} and item.get("review_state") == "waste"
    )
    cancellation_started_count = sum(1 for item in trim_items if item.get("review_state") == "cancellation_started")
    verifying_count = sum(1 for item in trim_items if item.get("review_state") == "verifying")
    realized_count = sum(1 for item in trim_items if item.get("review_state") == "realized" and not item.get("charge_returned"))
    brief_items: list[dict[str, str]] = []
    for item in trim_items:
        if item.get("charge_returned"):
            brief_items.append({
                "label": "Charge returned", "name": str(item["display_name"]),
                "detail": "A cost previously counted as saved charged again.",
                "href": f"/admin/finances/budget/vendor/{item['opportunity_key']}",
            })
    for item in trim_items:
        if item.get("review_state") == "waste":
            brief_items.append({
                "label": "Ready to cut", "name": str(item["display_name"]),
                "detail": f"{_money(int(item.get('monthly_potential_cents') or 0), exact=True)} per month awaits cancellation work.",
                "href": f"/admin/finances/budget/vendor/{item['opportunity_key']}",
            })
    for finding in view.get("investigations") or []:
        brief_items.append({
            "label": str(finding.get("headline") or "Review spending"),
            "name": str(finding.get("merchant") or "Posted spending"),
            "detail": str(finding.get("evidence") or "Review current bank evidence."),
            "href": "/admin/finances/budget#budget-investigation-title",
        })
    brief_items = brief_items[:5]
    monthly_brief_rows = "".join(
        f"<li><div><span>{html.escape(item['label'])}</span><strong>{html.escape(item['name'])}</strong>"
        f"<p>{html.escape(item['detail'])}</p></div><a href=\"{html.escape(item['href'], quote=True)}\">Review</a></li>"
        for item in brief_items
    )
    trim_rows = "".join(
        f"""
        <tr {'hidden' if str(item.get('opportunity_key') or '') not in actionable_keys else ''} data-trim-row data-trim-actionable="{'true' if str(item.get('opportunity_key') or '') in actionable_keys else 'false'}" data-trim-cadence="{html.escape(str(item.get('cadence') or 'one_time'), quote=True)}" data-trim-state="{html.escape(str(item.get('review_state') or 'unknown'), quote=True)}" data-trim-original-state="{html.escape(str(item.get('review_state') or 'unknown'), quote=True)}" data-trim-original-note="{html.escape(str(item.get('review_note') or ''), quote=True)}" data-trim-opportunity="{html.escape(json.dumps(item, separators=(',', ':'), default=str), quote=True)}">
          <td><strong><a href="/admin/finances/budget/vendor/{html.escape(item['opportunity_key'], quote=True)}">{html.escape(item['display_name'])}</a></strong><span>{html.escape(str(item.get('cadence') or 'uncertain').replace('_', ' ').title())} · {html.escape(str(item['category']).replace('_', ' ').title())} · {item['active_months']} of 6 months</span>
          <span class="trim-month-history">{' · '.join(f"{date.fromisoformat(month + '-01').strftime('%b')} {_money(int(amount), exact=True)}" for month, amount in item['monthly_history'].items())}</span></td>
          <td>{_money(int(item['monthly_potential_cents']), exact=True) if item.get('cadence') in {'monthly', 'annual'} else '<span class="trim-not-recurring">No recent charge</span>' if item.get('cadence') == 'inactive' else '<span class="trim-not-recurring">Not recurring</span>'}</td>
          <td>{_money(int(item['six_month_total_cents']), exact=True)}</td>
          <td><span class="trim-state trim-state--{html.escape(str(item.get('review_state') or 'unknown'), quote=True)}">{html.escape(str(item.get('review_state') or 'unknown').title())}</span></td>
          <td><div class="trim-form" {'hidden' if item.get('review_state') in {'cancellation_started', 'verifying', 'realized', 'cannot_cancel'} else ''}>
            <label class="sr-only" for="trim-note-{html.escape(item['opportunity_key'], quote=True)}">Note for {html.escape(item['display_name'])}</label>
            <input id="trim-note-{html.escape(item['opportunity_key'], quote=True)}" data-trim-note value="{html.escape(str(item.get('review_note') or ''), quote=True)}" placeholder="Optional note">
            <div class="trim-actions" role="group" aria-label="Classify {html.escape(item['display_name'], quote=True)}">
              <button class="trim-choice is-needed{' is-selected' if item.get('review_state') == 'needed' else ''}" data-trim-choice="needed" type="button">Needed</button>
              <button class="trim-choice is-unknown{' is-selected' if item.get('review_state') == 'unknown' else ''}" data-trim-choice="unknown" type="button">Unknown</button>
              <button class="trim-choice is-investigate{' is-selected' if item.get('review_state') == 'investigate' else ''}" data-trim-choice="investigate" type="button">Investigate</button>
              <button class="trim-choice is-waste{' is-selected' if item.get('review_state') == 'waste' else ''}" data-trim-choice="waste" type="button">Waste</button>
            </div>
          </div><a class="trim-review-link" href="/admin/finances/budget/vendor/{html.escape(item['opportunity_key'], quote=True)}">Review evidence and next step</a></td>
        </tr>"""
        for item in trim_items
    )
    review_status = str(review.get("status") or "empty")
    review_matches = review.get("calculation_id") == view.get("calculation_id")
    if review_status == "ready" and review_matches:
        recommendation_html = "".join(
            f"""
            <article class="budget-saving-item">
              <div><span>{html.escape(str(item['confidence']).title())} confidence · {html.escape(item['category_label'])}</span>
              <h3>{html.escape(item['headline'])}</h3><p>{html.escape(item['reason'])}</p></div>
              <div class="budget-saving-impact"><span>Recurring monthly target</span>
              <strong>{_money(int(item['recurring_saving_cents']), exact=True)}</strong></div>
              <p class="budget-saving-action"><strong>Do next:</strong> {html.escape(item['next_action'])}</p>
            </article>"""
            for item in review.get("recommendations") or []
        )
        review_content = f"""
          <p class="budget-review-summary">{html.escape(str(review.get('summary') or ''))}</p>
          <div class="budget-saving-list">{recommendation_html or '<div class="money-empty"><h3>No high-confidence cuts were supported</h3><p>The model did not find a recommendation strong enough to show.</p></div>'}</div>
          <form method="post" action="/admin/finances/budget/review"><button class="btn btn-secondary" type="submit">Run again on current spending</button></form>"""
    elif review_status == "not_configured":
        review_content = """
          <div class="money-empty"><h3>The high spending review is not configured</h3>
          <p>The deterministic budget still works. Add the approved Anthropic key before requesting an LLM review.</p></div>"""
    else:
        review_content = """
          <div class="budget-review-ready"><div><h3>Get a CFO-level read on the spending trend</h3>
          <p>The model reviews aggregated posted spending and can only prioritize savings amounts already calculated here.</p></div>
          <form method="post" action="/admin/finances/budget/review">
          <button class="btn btn-primary" type="submit">Run high spending review</button></form></div>"""

    body = f"""
    <div class="money-brief">
      {render_finance_nav("budget", counts={})}
      <header class="money-page-header"><div><p class="finance-eyebrow">Budget</p>
      <h1>Stop the monthly cash leak</h1>
      <p class="money-page-subtitle">A working budget built from posted spending—not forecasts—and a focused plan to improve end-of-month cash.</p></div>
      <div class="money-page-status"><span class="money-status money-status--review">Savings target</span>
      <span>Calculation {html.escape(str(view['calculation_id']))}</span></div></header>

      <section class="budget-summary" aria-label="Monthly budget and savings summary">
        <article><span>Projected spending this month</span><strong>{_money(totals['projected_cents'], exact=True)}</strong></article>
        <article><span>Six-month monthly average</span><strong>{_money(totals['average_cents'], exact=True)}</strong></article>
        <article><span>Suggested monthly budget</span><strong>{_money(totals['target_cents'], exact=True)}</strong></article>
        <article><span>Possible EOM improvement</span><strong>{_money(totals['potential_saving_cents'], exact=True)}</strong></article>
        <article><span>Potential monthly savings</span><strong>{_money(int((view.get('savings_summary') or {}).get('potential_monthly_cents') or 0), exact=True)}</strong></article>
        <article><span>Cancellation in progress</span><strong>{_money(int((view.get('savings_summary') or {}).get('committed_monthly_cents') or 0), exact=True)}</strong></article>
        <article class="budget-summary__saving"><span>Bank-verified monthly savings</span><strong>{_money(int((view.get('savings_summary') or {}).get('realized_monthly_cents') or 0), exact=True)}</strong></article>
      </section>
      <p class="budget-proof">Source: {html.escape(str(view['source']).replace('_', ' ').title())} posted transactions · {int(view.get('transaction_count') or 0)} transactions available from {html.escape(str(view.get('earliest_date') or 'unavailable'))} through {html.escape(str(view.get('latest_date') or 'unavailable'))} · Six complete months reviewed: {html.escape(', '.join(view['comparison_months']))}. Mirrored sources and internal transfers are excluded.</p>

      <section class="monthly-trim-brief" aria-labelledby="monthly-trim-title">
        <div class="money-section-heading"><div><p class="finance-eyebrow">Monthly trim brief</p>
        <h2 id="monthly-trim-title">The next five cost decisions</h2></div><span class="money-section-state">First working-day review</span></div>
        <ul>{monthly_brief_rows or '<li><div><strong>No urgent cost exceptions</strong><p>Continue with the five current recurring costs below.</p></div></li>'}</ul>
      </section>

      <section class="savings-cash-bridge" aria-labelledby="savings-cash-title">
        <div><p class="finance-eyebrow">Cash impact</p><h2 id="savings-cash-title">What may improve month-end cash</h2>
        <p>Potential and in-progress cuts remain scenarios. Only later Plaid evidence can move a cost into bank-verified savings.</p></div>
        <dl><div><dt>Potential</dt><dd>{_money(int((view.get('savings_summary') or {}).get('potential_monthly_cents') or 0), exact=True)}</dd></div>
        <div><dt>Committed, not verified</dt><dd>{_money(int((view.get('savings_summary') or {}).get('committed_monthly_cents') or 0), exact=True)}</dd></div>
        <div><dt>Verified monthly</dt><dd>{_money(int((view.get('savings_summary') or {}).get('realized_monthly_cents') or 0), exact=True)}</dd></div></dl>
      </section>

      <section class="budget-workspace trim-workspace" aria-labelledby="trim-title">
        <div class="money-section-heading"><div><p class="finance-eyebrow">Trim list</p>
        <h2 id="trim-title">Decide what stays and what goes</h2></div>
        <span class="money-section-state">{len(trim_items)} controllable vendors</span></div>
        <p class="budget-review-summary">Start with five current costs. Saved keep decisions leave this queue; Waste moves to Ready to cut. A saving counts only after later Plaid activity verifies that the charge stopped.</p>
        <div class="trim-summary" aria-label="Trim review progress">
          <button type="button" data-trim-filter="needs_decision" class="is-active">Needs decision <strong>{len(actionable_items)}</strong></button>
          <button type="button" data-trim-filter="waste">Ready to cut <strong>{ready_to_cut_count}</strong></button>
          <button type="button" data-trim-filter="cancellation_started">Cancellation started <strong>{cancellation_started_count}</strong></button>
          <button type="button" data-trim-filter="verifying">Verifying <strong>{verifying_count}</strong></button>
          <button type="button" data-trim-filter="realized">Savings confirmed <strong>{realized_count}</strong></button>
          <button type="button" data-trim-filter="recurring">All recent <strong>{recurring_trim_count}</strong></button>
          <button type="button" data-trim-filter="inactive">Inactive/history <strong>{inactive_trim_count}</strong></button>
          <button type="button" data-trim-filter="one_time">One-time <strong>{one_time_trim_count}</strong></button>
          <button type="button" data-trim-filter="all">All <strong>{len(trim_items)}</strong></button>
          <button type="button" data-trim-filter="needed">Kept <strong>{trim_counts['needed']}</strong></button>
        </div>
        <form method="post" action="/admin/finances/savings/reviews/batch" data-trim-batch-form data-trim-calculation="{html.escape(str(view['calculation_id']), quote=True)}">
        <input type="hidden" name="changes_json" data-trim-changes value="[]">
        <div class="trim-savebar"><span data-trim-unsaved>No unsaved changes</span><div class="trim-savebar__actions"><button class="btn btn-secondary" type="button" data-trim-discard disabled>Discard draft</button><button class="btn btn-primary" type="submit" data-trim-save disabled>Save all changes</button></div></div>
        <div class="money-table-wrap trim-table-wrap"><table class="budget-table trim-table"><thead><tr>
          <th>Vendor</th><th>Monthly average</th><th>Six-month spend</th><th>Status</th><th>Decision and note</th>
        </tr></thead><tbody>{trim_rows}</tbody></table></div>
        <p class="budget-rule-note" data-trim-result-count>Showing {len(actionable_items)} highest-impact current vendors that still need a decision.</p>
        </form>
      </section>

      <section class="budget-trend" aria-labelledby="budget-trend-title">
        <div class="money-section-heading"><div><p class="finance-eyebrow">Six-month trend</p>
        <h2 id="budget-trend-title">What operating spending is doing</h2></div>
        <span class="money-section-state">Complete months only</span></div>
        <div class="budget-trend-scroll"><div class="budget-trend-chart" role="img" aria-label="Posted operating spending for each of the last six complete months">{trend_columns}</div></div>
        <p class="budget-rule-note">The current partial month is kept out of this trend. It appears only as a separate projection above.</p>
      </section>

      <section class="budget-workspace" aria-labelledby="budget-table-title">
        <div class="money-section-heading"><div><p class="finance-eyebrow">Six-month controls</p>
        <h2 id="budget-table-title">Where recurring savings can come from</h2></div>
        <span class="money-section-state">{len(view['categories'])} categories</span></div>
        <div class="money-table-wrap"><table class="budget-table"><thead><tr>
        <th>Category</th><th>6-month average</th><th>Recent trend</th><th>Projected this month</th><th>Suggested budget</th><th>Over / under</th>
        </tr></thead><tbody>{rows}</tbody></table></div>
        <p class="budget-rule-note">Suggested budgets keep protected costs unchanged and target 10–15% reductions only in controllable categories. These are planning targets, not changes to your bank or books.</p>
      </section>

      <section class="budget-review" aria-labelledby="budget-investigation-title">
        <div class="money-section-heading"><div><p class="finance-eyebrow">Merchant-level investigation</p>
        <h2 id="budget-investigation-title">Where the deeper savings may be hiding</h2></div>
        <span class="money-section-state">{len(view.get('investigations') or [])} evidence checks</span></div>
        <p class="budget-review-summary">These are not booked savings. They identify rising vendors, newly recurring costs, and duplicate-looking charges that deserve receipt, usage, or contract review.</p>
        <div class="budget-saving-list">{investigation_rows or '<div class="money-empty"><h3>No merchant-level leaks crossed the evidence threshold</h3><p>The category budget still applies, but no specific vendor pattern is strong enough to call out.</p></div>'}</div>
      </section>

      <section class="budget-review" aria-labelledby="budget-review-title">
        <div class="money-section-heading"><div><p class="finance-eyebrow">Savings review</p>
        <h2 id="budget-review-title">What should we cut or renegotiate?</h2></div><span class="money-status money-status--ready">Advice only</span></div>
        {review_content}
      </section>
    </div>"""
    body += """
    <script>
    (() => {
      const filters = [...document.querySelectorAll('[data-trim-filter]')];
      const rows = [...document.querySelectorAll('[data-trim-row]')];
      const count = document.querySelector('[data-trim-result-count]');
      const form = document.querySelector('[data-trim-batch-form]');
      const changesInput = document.querySelector('[data-trim-changes]');
      const saveButton = document.querySelector('[data-trim-save]');
      const discardButton = document.querySelector('[data-trim-discard]');
      const unsaved = document.querySelector('[data-trim-unsaved]');
      let submitting = false;
      const draftKey = `anata-finance-trim-draft:${form?.dataset.trimCalculation || 'current'}`;
      const storage = {
        get: () => { try { return JSON.parse(localStorage.getItem(draftKey) || 'null'); } catch (_) { return null; } },
        set: value => { try { localStorage.setItem(draftKey, JSON.stringify(value)); } catch (_) {} },
        clear: () => { try { localStorage.removeItem(draftKey); } catch (_) {} },
      };
      const stagedChanges = () => rows.flatMap(row => {
        const note = row.querySelector('[data-trim-note]')?.value.trim() || '';
        const changed = row.dataset.trimState !== row.dataset.trimOriginalState || note !== row.dataset.trimOriginalNote;
        if (!changed) return [];
        return [{action: row.dataset.trimState, reason: note, opportunity: JSON.parse(row.dataset.trimOpportunity)}];
      });
      const updateSaveState = () => {
        const changes = stagedChanges();
        if (changesInput) changesInput.value = JSON.stringify(changes);
        if (saveButton) saveButton.disabled = changes.length === 0;
        if (discardButton) discardButton.disabled = changes.length === 0;
        if (unsaved) unsaved.textContent = changes.length ? `${changes.length} unsaved change${changes.length === 1 ? '' : 's'}` : 'No unsaved changes';
        if (changes.length) storage.set({changes, savedAt: new Date().toISOString()}); else storage.clear();
      };
      const showRowState = (row, state) => {
        row.dataset.trimState = state;
        const badge = row.querySelector('.trim-state');
        if (badge) {
          badge.textContent = state.charAt(0).toUpperCase() + state.slice(1);
          badge.className = `trim-state trim-state--${state}`;
        }
        row.querySelectorAll('[data-trim-choice]').forEach(item => {
          const selected = item.dataset.trimChoice === state;
          item.classList.toggle('is-selected', selected);
          item.setAttribute('aria-pressed', selected ? 'true' : 'false');
        });
      };
      rows.forEach(row => {
        row.querySelectorAll('[data-trim-choice]').forEach(button => button.addEventListener('click', () => {
          const state = button.dataset.trimChoice;
          showRowState(row, state);
          updateSaveState();
        }));
        row.querySelector('[data-trim-note]')?.addEventListener('input', updateSaveState);
      });
      filters.forEach(button => button.addEventListener('click', () => {
        const wanted = button.dataset.trimFilter;
        let shown = 0;
        rows.forEach(row => {
          const visible = wanted === 'all' || (wanted === 'needs_decision' && row.dataset.trimActionable === 'true') || row.dataset.trimState === wanted || row.dataset.trimCadence === wanted;
          row.hidden = !visible;
          if (visible) shown += 1;
        });
        filters.forEach(item => item.classList.toggle('is-active', item === button));
        filters.forEach(item => item.setAttribute('aria-pressed', item === button ? 'true' : 'false'));
        if (count) count.textContent = `Showing ${shown} vendor${shown === 1 ? '' : 's'}.`;
      }));
      form?.addEventListener('submit', event => {
        const changes = stagedChanges();
        if (!changes.length) { event.preventDefault(); return; }
        changesInput.value = JSON.stringify(changes);
        submitting = true;
        saveButton.disabled = true;
        saveButton.textContent = 'Saving changes…';
      });
      discardButton?.addEventListener('click', () => {
        rows.forEach(row => {
          showRowState(row, row.dataset.trimOriginalState);
          const note = row.querySelector('[data-trim-note]');
          if (note) note.value = row.dataset.trimOriginalNote;
        });
        storage.clear();
        updateSaveState();
      });
      const flash = new URLSearchParams(window.location.search).get('flash') || '';
      if (flash.startsWith('ok:Saved ')) storage.clear();
      const draft = storage.get();
      if (draft && Array.isArray(draft.changes)) {
        const byKey = new Map(rows.map(row => [JSON.parse(row.dataset.trimOpportunity).opportunity_key, row]));
        let restored = 0;
        draft.changes.forEach(change => {
          const key = change?.opportunity?.opportunity_key;
          const row = byKey.get(key);
          if (!row || !['needed', 'unknown', 'investigate', 'waste'].includes(change.action)) return;
          showRowState(row, change.action);
          const note = row.querySelector('[data-trim-note]');
          if (note) note.value = String(change.reason || '');
          restored += 1;
        });
        updateSaveState();
        if (restored && unsaved) unsaved.textContent = `Recovered ${restored} unsaved change${restored === 1 ? '' : 's'}`;
      } else updateSaveState();
      window.addEventListener('beforeunload', event => {
        if (submitting || !stagedChanges().length) return;
        event.preventDefault();
        event.returnValue = '';
      });
    })();
    </script>"""
    return _page_shell("Budget & savings", "budget", body, flash=flash)
