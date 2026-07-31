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
    "utilities", "critical_utilities", "revenue",
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
    raw = (
        row.get("category")
        or row.get("personal_finance_category")
        or row.get("transaction_category")
        or "uncategorized"
    )
    key = _slug(raw)
    if key in {"", "other", "uncategorized", "unknown"}:
        description = " ".join(
            str(row.get(field) or "")
            for field in (
                "friendly_name",
                "vendor_or_customer",
                "name",
                "description",
            )
        )
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
    latest_date: date | None = None
    for row in transactions:
        occurred = row["_budget_date"]
        key = row["_budget_category"]
        month = _month_key(occurred)
        if month in comparison_months or month == current_month:
            category_months[key][month] += int(row.get("amount_cents") or 0)
            category_merchants[key][row["_budget_merchant"]] += int(
                row.get("amount_cents") or 0
            )
        latest_date = max(latest_date or occurred, occurred)

    days_in_month = monthrange(today.year, today.month)[1]
    elapsed_days = max(1, today.day)
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
        historical_reduction = 0 if protected else max(0, average - target)
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
    proof = {
        "source": source,
        "as_of": today.isoformat(),
        "latest_date": latest_date.isoformat() if latest_date else "",
        "comparison_months": comparison_months,
        "totals": totals,
        "monthly_totals": monthly_totals,
        "categories": categories,
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
        return build_budget_view(list_obligations(limit=10_000))
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
      <p class="budget-proof">Source: {html.escape(str(view['source']).replace('_', ' ').title())} posted transactions · Six complete months: {html.escape(', '.join(view['comparison_months']))} · Latest evidence {html.escape(str(view.get('latest_date') or 'unavailable'))}. Mirrored sources and internal transfers are excluded.</p>

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

      <section class="budget-review" aria-labelledby="budget-review-title">
        <div class="money-section-heading"><div><p class="finance-eyebrow">Savings review</p>
        <h2 id="budget-review-title">What should we cut or renegotiate?</h2></div><span class="money-status money-status--ready">Advice only</span></div>
        {review_content}
      </section>
    </div>"""
    return _page_shell("Budget & savings", "budget", body, flash=flash)
