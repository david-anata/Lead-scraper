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
from datetime import date, datetime
from typing import Any, Iterable, Mapping

from sales_support_agent.models.database import kv_get_json, kv_set_json
from sales_support_agent.services.cashflow.cashflow_helpers import _page_shell
from sales_support_agent.services.cashflow.categorizer import categorize
from sales_support_agent.services.cashflow.finance_nav import render_finance_nav
from sales_support_agent.services.cashflow.obligations import list_obligations


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
    merchant_charge_signatures: dict[str, dict[tuple[str, int], int]] = defaultdict(
        lambda: defaultdict(int)
    )
    latest_date: date | None = None
    earliest_date: date | None = None
    for row in transactions:
        occurred = row["_budget_date"]
        key = row["_budget_category"]
        month = _month_key(occurred)
        if month in comparison_months or month == current_month:
            category_months[key][month] += int(row.get("amount_cents") or 0)
            category_merchants[key][row["_budget_merchant"]] += int(
                row.get("amount_cents") or 0
            )
            merchant_key = _slug(row["_budget_merchant"]) or "unassigned"
            merchant_meta[merchant_key] = {
                "name": row["_budget_merchant"], "category": key,
            }
            merchant_months[merchant_key][month] += int(row.get("amount_cents") or 0)
            if month in comparison_months:
                merchant_charge_signatures[merchant_key][
                    (occurred.isoformat(), int(row.get("amount_cents") or 0))
                ] += 1
        latest_date = max(latest_date or occurred, occurred)
        earliest_date = min(earliest_date or occurred, occurred)

    days_in_month = monthrange(today.year, today.month)[1]
    elapsed_days = max(1, today.day)
    recurring_category_average: dict[str, int] = defaultdict(int)
    for merchant_key, monthly in merchant_months.items():
        historical = [int(monthly.get(month) or 0) for month in comparison_months]
        if (
            sum(1 for amount in historical if amount > 0) >= 2
            and historical[-1] > 0
        ):
            recurring_category_average[merchant_meta[merchant_key]["category"]] += (
                sum(historical) // len(comparison_months)
            )
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
        evidence = {
            "merchant": merchant_key, "category": meta["category"],
            "months": history, "comparison_months": comparison_months,
        }
        opportunity_key = hashlib.sha256(f"budget-trim-v1|{merchant_key}".encode()).hexdigest()
        evidence_hash = hashlib.sha256(
            json.dumps(evidence, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        recent_average = sum(history[month] for month in comparison_months[3:]) // 3
        active_months = sum(1 for value in history.values() if value > 0)
        cadence = (
            "recurring"
            if active_months >= 2 and history[comparison_months[-1]] > 0
            else "inactive"
            if active_months >= 2
            else "one_time"
        )
        trim_items.append({
            "opportunity_key": opportunity_key, "key": opportunity_key,
            "evidence_hash": evidence_hash, "display_name": meta["name"],
            "normalized_merchant": merchant_key, "category": meta["category"],
            "cadence": cadence,
            "monthly_potential_cents": recent_average if cadence == "recurring" else 0,
            "baseline_amount_cents": recent_average if cadence == "recurring" else 0,
            "six_month_total_cents": total, "monthly_average_cents": total // 6,
            "recent_average_cents": recent_average,
            "active_months": active_months, "monthly_history": history,
            "reason": (
                "Recurring controllable vendor review"
                if cadence == "recurring"
                else "Previously recurring but absent from the latest complete month"
                if cadence == "inactive"
                else "One-time or irregular historical purchase review"
            ),
            "limitations": "Usage, contract terms, and replacement cost require operator review.",
            "evidence_dates": comparison_months, "review_state": "unknown", "review_note": "",
        })
    trim_items.sort(key=lambda item: (
        {"recurring": 0, "inactive": 1, "one_time": 2}.get(item["cadence"], 3),
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
        view = build_budget_view(list_obligations(limit=10_000))
        from sales_support_agent.services.cashflow.savings_reviews import load_savings_reviews
        reviews = load_savings_reviews()
        for item in view.get("trim_items") or []:
            review = reviews.get(str(item.get("opportunity_key") or ""))
            if review:
                item["review_state"] = str(review.get("state") or "unknown")
                item["review_note"] = str(review.get("reason") or "")
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
    recurring_trim_count = sum(1 for item in trim_items if item.get("cadence") == "recurring")
    inactive_trim_count = sum(1 for item in trim_items if item.get("cadence") == "inactive")
    one_time_trim_count = sum(1 for item in trim_items if item.get("cadence") == "one_time")
    trim_counts = {
        state: sum(1 for item in trim_items if str(item.get("review_state") or "unknown") == state)
        for state in ("unknown", "needed", "investigate", "waste")
    }
    actionable_items = [
        item for item in trim_items
        if (
            item.get("cadence") == "recurring"
            and str(item.get("review_state") or "unknown") in {"unknown", "investigate"}
        )
    ][:15]
    actionable_keys = {
        str(item.get("opportunity_key") or "") for item in actionable_items
    }
    ready_to_cut_count = sum(
        1 for item in trim_items
        if item.get("cadence") == "recurring" and item.get("review_state") == "waste"
    )
    trim_rows = "".join(
        f"""
        <tr {'hidden' if str(item.get('opportunity_key') or '') not in actionable_keys else ''} data-trim-row data-trim-actionable="{'true' if str(item.get('opportunity_key') or '') in actionable_keys else 'false'}" data-trim-cadence="{html.escape(str(item.get('cadence') or 'one_time'), quote=True)}" data-trim-state="{html.escape(str(item.get('review_state') or 'unknown'), quote=True)}" data-trim-original-state="{html.escape(str(item.get('review_state') or 'unknown'), quote=True)}" data-trim-original-note="{html.escape(str(item.get('review_note') or ''), quote=True)}" data-trim-opportunity="{html.escape(json.dumps(item, separators=(',', ':')), quote=True)}">
          <td><strong>{html.escape(item['display_name'])}</strong><span>{'Recently recurring' if item.get('cadence') == 'recurring' else 'Inactive / historical' if item.get('cadence') == 'inactive' else 'One-time / irregular'} · {html.escape(str(item['category']).replace('_', ' ').title())} · {item['active_months']} of 6 months</span>
          <span class="trim-month-history">{' · '.join(f"{date.fromisoformat(month + '-01').strftime('%b')} {_money(int(amount), exact=True)}" for month, amount in item['monthly_history'].items())}</span></td>
          <td>{_money(int(item['monthly_average_cents']), exact=True) if item.get('cadence') == 'recurring' else '<span class="trim-not-recurring">No recent charge</span>' if item.get('cadence') == 'inactive' else '<span class="trim-not-recurring">Not recurring</span>'}</td>
          <td>{_money(int(item['six_month_total_cents']), exact=True)}</td>
          <td><span class="trim-state trim-state--{html.escape(str(item.get('review_state') or 'unknown'), quote=True)}">{html.escape(str(item.get('review_state') or 'unknown').title())}</span></td>
          <td><div class="trim-form">
            <label class="sr-only" for="trim-note-{html.escape(item['opportunity_key'], quote=True)}">Note for {html.escape(item['display_name'])}</label>
            <input id="trim-note-{html.escape(item['opportunity_key'], quote=True)}" data-trim-note value="{html.escape(str(item.get('review_note') or ''), quote=True)}" placeholder="Optional note">
            <div class="trim-actions" role="group" aria-label="Classify {html.escape(item['display_name'], quote=True)}">
              <button class="trim-choice is-needed{' is-selected' if item.get('review_state') == 'needed' else ''}" data-trim-choice="needed" type="button">Needed</button>
              <button class="trim-choice is-unknown{' is-selected' if item.get('review_state') == 'unknown' else ''}" data-trim-choice="unknown" type="button">Unknown</button>
              <button class="trim-choice is-investigate{' is-selected' if item.get('review_state') == 'investigate' else ''}" data-trim-choice="investigate" type="button">Investigate</button>
              <button class="trim-choice is-waste{' is-selected' if item.get('review_state') == 'waste' else ''}" data-trim-choice="waste" type="button">Waste</button>
            </div>
          </div></td>
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

      <section class="budget-summary" aria-label="Monthly budget summary">
        <article><span>Projected spending this month</span><strong>{_money(totals['projected_cents'], exact=True)}</strong></article>
        <article><span>Six-month monthly average</span><strong>{_money(totals['average_cents'], exact=True)}</strong></article>
        <article><span>Suggested monthly budget</span><strong>{_money(totals['target_cents'], exact=True)}</strong></article>
        <article><span>Possible EOM improvement</span><strong>{_money(totals['potential_saving_cents'], exact=True)}</strong></article>
        <article class="budget-summary__saving"><span>Recurring savings still to capture</span><strong>{_money(totals['recurring_saving_cents'], exact=True)}</strong></article>
      </section>
      <p class="budget-proof">Source: {html.escape(str(view['source']).replace('_', ' ').title())} posted transactions · {int(view.get('transaction_count') or 0)} transactions available from {html.escape(str(view.get('earliest_date') or 'unavailable'))} through {html.escape(str(view.get('latest_date') or 'unavailable'))} · Six complete months reviewed: {html.escape(', '.join(view['comparison_months']))}. Mirrored sources and internal transfers are excluded.</p>

      <section class="budget-workspace trim-workspace" aria-labelledby="trim-title">
        <div class="money-section-heading"><div><p class="finance-eyebrow">Trim list</p>
        <h2 id="trim-title">Decide what stays and what goes</h2></div>
        <span class="money-section-state">{len(trim_items)} controllable vendors</span></div>
        <p class="budget-review-summary">Review only the highest-impact vendors that still need an answer. Saved keep decisions leave this queue; confirmed waste moves to Ready to cut. No service is cancelled here.</p>
        <div class="trim-summary" aria-label="Trim review progress">
          <button type="button" data-trim-filter="needs_decision" class="is-active">Needs decision <strong>{len(actionable_items)}</strong></button>
          <button type="button" data-trim-filter="waste">Ready to cut <strong>{ready_to_cut_count}</strong></button>
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
        <p class="budget-rule-note" data-trim-result-count>Showing {len(actionable_items)} highest-impact recent vendors that still need a decision.</p>
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
