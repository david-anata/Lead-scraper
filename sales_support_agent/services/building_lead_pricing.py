"""Per-lead pricing.

Standard rate plans are the starting point, not the answer. Each lead carries
its own pricing, seeded from the approved plan and adjustable for that customer
alone. Changing a lead never changes the standard, and never changes another
lead.

Pricing stays editable at every stage by design. Each generated contract keeps
its own copy of the numbers it was built from, so editing afterwards is allowed
and still leaves a record of what a customer was actually sent.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional


#: Owner-approved baseline, used when a lead has no pricing and no rate plan.
DEFAULT_HOURLY_RATE_CENTS = 17_500
DEFAULT_MINIMUM_HOURS = 6
DEFAULT_CLEANING_FEE_CENTS = 25_000
DEFAULT_DEPOSIT_PERCENT_BPS = 5_000
MAX_ADDONS = 20


class LeadPricingError(ValueError):
    """Raised when submitted pricing cannot be stored."""


def _cents(value: Any, *, field: str, allow_negative: bool = False) -> int:
    text = str(value if value is not None else "").strip().replace(",", "").replace("$", "")
    if not text:
        return 0
    try:
        amount = round(float(text) * 100)
    except ValueError as exc:
        raise LeadPricingError(f"{field} must be a number.") from exc
    if amount < 0 and not allow_negative:
        raise LeadPricingError(f"{field} cannot be negative.")
    return int(amount)


def default_pricing(rate_plan: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    """Seed a lead from the approved plan so it opens filled in, not blank."""

    plan = dict(rate_plan or {})
    return {
        "rate_plan_id": str(plan.get("id") or ""),
        "rate_plan_name": str(plan.get("name") or "Owner-approved baseline"),
        "currency": str(plan.get("currency") or "USD"),
        "hourly_rate_cents": int(plan.get("unit_amount_cents") or DEFAULT_HOURLY_RATE_CENTS),
        "hours": int(plan.get("minimum_units") or DEFAULT_MINIMUM_HOURS),
        "cleaning_fee_cents": DEFAULT_CLEANING_FEE_CENTS,
        "addons": [],
        "discount_cents": 0,
        "discount_reason": "",
        "deposit_percent_bps": int(
            plan.get("deposit_percent_bps") or DEFAULT_DEPOSIT_PERCENT_BPS
        ),
        "updated_by": "",
        "updated_at": "",
    }


def compute_totals(pricing: dict[str, Any]) -> dict[str, int]:
    """Return the derived money for a lead. Never stored; always recomputed."""

    hours = max(0, int(pricing.get("hours") or 0))
    rate = max(0, int(pricing.get("hourly_rate_cents") or 0))
    venue = hours * rate
    cleaning = max(0, int(pricing.get("cleaning_fee_cents") or 0))
    addons = sum(
        max(0, int(item.get("amount_cents") or 0))
        for item in list(pricing.get("addons") or [])
    )
    subtotal = venue + cleaning + addons
    discount = min(max(0, int(pricing.get("discount_cents") or 0)), subtotal)
    total = subtotal - discount
    deposit = (total * max(0, int(pricing.get("deposit_percent_bps") or 0)) + 5_000) // 10_000
    return {
        "venue_cents": venue,
        "cleaning_cents": cleaning,
        "addons_cents": addons,
        "subtotal_cents": subtotal,
        "discount_cents": discount,
        "total_cents": total,
        "deposit_cents": min(deposit, total),
    }


def parse_pricing_form(form: Any, *, existing: dict[str, Any], actor: str) -> dict[str, Any]:
    """Build stored pricing from a submitted form, keeping what was not sent."""

    pricing = dict(existing or {})
    pricing["hourly_rate_cents"] = _cents(form.get("hourly_rate"), field="Hourly rate")
    try:
        hours = int(str(form.get("hours") or "0").strip() or 0)
    except ValueError as exc:
        raise LeadPricingError("Hours must be a whole number.") from exc
    if hours < 0:
        raise LeadPricingError("Hours cannot be negative.")
    pricing["hours"] = hours
    pricing["cleaning_fee_cents"] = _cents(form.get("cleaning_fee"), field="Cleaning fee")
    pricing["discount_cents"] = _cents(form.get("discount"), field="Discount")
    pricing["discount_reason"] = str(form.get("discount_reason") or "").strip()[:300]

    try:
        percent = float(str(form.get("deposit_percent") or "0").strip() or 0)
    except ValueError as exc:
        raise LeadPricingError("Deposit percent must be a number.") from exc
    if not 0 <= percent <= 100:
        raise LeadPricingError("Deposit percent must be between 0 and 100.")
    pricing["deposit_percent_bps"] = int(round(percent * 100))

    addons: list[dict[str, Any]] = []
    index = 0
    while index < MAX_ADDONS:
        name = form.get(f"addon_name_{index}")
        amount = form.get(f"addon_amount_{index}")
        if name is None and amount is None:
            break
        label = str(name or "").strip()
        if label:
            addons.append({
                "name": label[:120],
                "amount_cents": _cents(amount, field=f"Add-on '{label}' amount"),
            })
        index += 1
    pricing["addons"] = addons

    if pricing["discount_cents"] and not pricing["discount_reason"]:
        raise LeadPricingError("A discount needs a reason recorded against it.")

    pricing["updated_by"] = actor
    pricing["updated_at"] = datetime.now(timezone.utc).isoformat()
    return pricing


def merge_values_from_pricing(pricing: dict[str, Any]) -> dict[str, Any]:
    """Map lead pricing onto the contract merge fields."""

    totals = compute_totals(pricing)
    return {
        "currency": str(pricing.get("currency") or "USD"),
        "subtotal_before_discount": totals["subtotal_cents"],
        "discount_amount": totals["discount_cents"],
        "discount_reason": str(pricing.get("discount_reason") or ""),
        "quote_total": totals["total_cents"],
        "deposit_amount": totals["deposit_cents"],
        "deposit_type": "percent",
        "addons": [item.get("name", "") for item in list(pricing.get("addons") or [])],
    }
