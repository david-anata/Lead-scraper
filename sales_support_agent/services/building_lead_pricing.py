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

from sales_support_agent.services.building_event_policy import (
    DAY_PORTION_LABELS,
    LEGACY_HOURLY_RATE_CENTS,
    RESERVATION_PERCENT_BPS,
    quote_duration,
)


#: Retired hourly baseline. Kept so a lead saved before November 10, 2026 can
#: still be recognised. New quotes use the day card in building_event_policy.
DEFAULT_HOURLY_RATE_CENTS = LEGACY_HOURLY_RATE_CENTS
DEFAULT_MINIMUM_HOURS = 6
DEFAULT_CLEANING_FEE_CENTS = 0
DEFAULT_DEPOSIT_PERCENT_BPS = RESERVATION_PERCENT_BPS
#: No security deposit. The field stays so a manual exception can be recorded,
#: and so an older quote that still has one is not folded into the total.
DEFAULT_SECURITY_DEPOSIT_CENTS = 0
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
    hours = DEFAULT_MINIMUM_HOURS
    portion, _amount = quote_duration(hours=hours)
    return {
        "rate_plan_id": str(plan.get("id") or ""),
        "rate_plan_name": str(plan.get("name") or "November 2026 day rates"),
        "currency": str(plan.get("currency") or "USD"),
        "pricing_model": "day_portion",
        "day_portion": portion or "half",
        "hourly_rate_cents": 0,
        "hours": hours,
        "cleaning_fee_cents": DEFAULT_CLEANING_FEE_CENTS,
        "addons": [],
        "discount_cents": 0,
        "discount_reason": "",
        "deposit_percent_bps": int(
            plan.get("deposit_percent_bps") or DEFAULT_DEPOSIT_PERCENT_BPS
        ),
        "tax_status": str(plan.get("tax_status") or "review_required"),
        "tax_rate_bps": int(plan.get("tax_rate_bps") or 0),
        "security_deposit_cents": DEFAULT_SECURITY_DEPOSIT_CENTS,
        "updated_by": "",
        "updated_at": "",
    }


def _venue_cents(pricing: dict[str, Any]) -> tuple[int, str]:
    """Price the room from the day card unless this lead is an explicit hourly override."""

    model = str(pricing.get("pricing_model") or "")
    if model == "unquoted":
        return 0, ""
    if model == "hourly":
        hours = max(0, int(pricing.get("hours") or 0))
        rate = max(0, int(pricing.get("hourly_rate_cents") or 0))
        return hours * rate, ""
    portion = str(pricing.get("day_portion") or "").strip()
    days = pricing.get("days")
    if model == "day_portion" or portion or days not in (None, ""):
        resolved, amount = quote_duration(
            hours=pricing.get("hours"),
            days=None if days in (None, "") else days,
            day_portion=portion or None,
        )
        return int(amount or 0), resolved or ""
    rate = int(pricing.get("hourly_rate_cents") or 0)
    hours = max(0, int(pricing.get("hours") or 0))
    if rate and rate != LEGACY_HOURLY_RATE_CENTS:
        return hours * max(rate, 0), ""
    resolved, amount = quote_duration(hours=hours or None)
    return int(amount or 0), resolved or ""


def venue_label(pricing: dict[str, Any]) -> str:
    """Customer-facing name for the venue line."""

    _amount, portion = _venue_cents(pricing)
    if portion:
        hours = int(pricing.get("hours") or 0)
        label = DAY_PORTION_LABELS[portion]
        return f"{label} ({hours} hours)" if hours else label
    hours = int(pricing.get("hours") or 0)
    if str(pricing.get("pricing_model") or "") == "hourly" and hours:
        return f"{hours} hour{'s' if hours != 1 else ''} of venue time"
    if hours:
        return f"{hours} hour{'s' if hours != 1 else ''} of venue time"
    return "Venue rental"


def compute_totals(pricing: dict[str, Any]) -> dict[str, int]:
    """Return the derived money for a lead. Never stored; always recomputed."""

    hours = max(0, int(pricing.get("hours") or 0))
    venue, _portion = _venue_cents(pricing)
    cleaning = max(0, int(pricing.get("cleaning_fee_cents") or 0))
    addons = sum(
        max(0, int(item.get("amount_cents") or 0))
        for item in list(pricing.get("addons") or [])
    )
    subtotal = venue + cleaning + addons
    discount = min(max(0, int(pricing.get("discount_cents") or 0)), subtotal)
    taxable = subtotal - discount
    tax_rate_bps = max(0, int(pricing.get("tax_rate_bps") or 0))
    tax = (taxable * tax_rate_bps + 5_000) // 10_000
    total = taxable + tax
    deposit = (total * max(0, int(pricing.get("deposit_percent_bps") or 0)) + 5_000) // 10_000
    booking_deposit = min(deposit, total)
    # Refundable and excluded from the booking-deposit percentage, per the
    # approved terms, so it is added to what is collected but never to the
    # contract total.
    security = max(0, int(pricing.get("security_deposit_cents") or 0))
    return {
        "venue_cents": venue,
        "cleaning_cents": cleaning,
        "addons_cents": addons,
        "subtotal_cents": subtotal,
        "discount_cents": discount,
        "taxable_cents": taxable,
        "tax_cents": tax,
        "total_cents": total,
        "deposit_cents": booking_deposit,
        "security_deposit_cents": security,
        "due_to_book_cents": booking_deposit + security,
        "balance_cents": total - booking_deposit,
    }


def parse_pricing_form(form: Any, *, existing: dict[str, Any], actor: str) -> dict[str, Any]:
    """Build stored pricing from a submitted form, keeping what was not sent."""

    pricing = dict(existing or {})
    try:
        hours = int(str(form.get("hours") or "0").strip() or 0)
    except ValueError as exc:
        raise LeadPricingError("Hours must be a whole number.") from exc
    if hours < 0:
        raise LeadPricingError("Hours cannot be negative.")
    pricing["hours"] = hours
    portion = str(form.get("day_portion") or "").strip()
    if portion == "unquoted":
        pricing["pricing_model"] = "unquoted"
        pricing["day_portion"] = ""
        pricing["hourly_rate_cents"] = 0
    elif portion in DAY_PORTION_LABELS:
        pricing["pricing_model"] = "day_portion"
        pricing["day_portion"] = portion
        pricing["hourly_rate_cents"] = 0
    else:
        resolved, _amount = quote_duration(hours=hours)
        pricing["pricing_model"] = "day_portion"
        pricing["day_portion"] = resolved or ""
        pricing["hourly_rate_cents"] = 0
    pricing["cleaning_fee_cents"] = _cents(form.get("cleaning_fee"), field="Cleaning fee")
    pricing["security_deposit_cents"] = _cents(
        form.get("security_deposit"), field="Security deposit"
    )
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
