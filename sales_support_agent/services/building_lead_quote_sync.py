"""Write a lead's pricing into its frozen quote.

Lead pricing is what an operator edits; the quote is what billing, invoicing,
and the QuickBooks handoff already read. Feeding one into the other keeps a
single source of money without rebuilding the payment path.

The quote keeps its existing shape exactly — a total, itemised lines, and a
rate-plan snapshot — so nothing downstream has to know pricing now starts on the
lead.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional
from uuid import uuid4

from sqlalchemy import select

from sales_support_agent.models.entities import (
    BuildingProposal,
    BuildingRatePlan,
    BuildingReservation,
)
from sales_support_agent.services.building_lead_pricing import compute_totals


def _line_items(pricing: dict[str, Any], totals: dict[str, int]) -> list[dict[str, Any]]:
    """Itemise the quote so an invoice can be read without the lead."""

    items: list[dict[str, Any]] = []
    hours = int(pricing.get("hours") or 0)
    if totals["venue_cents"]:
        items.append({
            "type": "base",
            "name": "Venue rental",
            "description": f"{hours} hour{'s' if hours != 1 else ''} of venue time",
            "quantity": hours,
            "amount_cents": totals["venue_cents"],
        })
    if totals["cleaning_cents"]:
        items.append({
            "type": "fee",
            "name": "Routine cleaning",
            "quantity": 1,
            "amount_cents": totals["cleaning_cents"],
        })
    for addon in list(pricing.get("addons") or []):
        amount = int(addon.get("amount_cents") or 0)
        if amount:
            items.append({
                "type": "addon",
                "name": str(addon.get("name") or "Add-on"),
                "quantity": 1,
                "amount_cents": amount,
            })
    if totals["discount_cents"]:
        # A discount is its own line with its reason, because that is where the
        # contract and the invoice both read it from.
        items.append({
            "type": "discount",
            "name": "Discount",
            "description": str(pricing.get("discount_reason") or ""),
            "quantity": 1,
            "amount_cents": -totals["discount_cents"],
        })
    return items


def _rate_snapshot(
    pricing: dict[str, Any],
    plan: Optional[BuildingRatePlan],
    previous: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Freeze the commercial terms this contract is built on.

    Only the money comes from the lead. The policy terms come from the approved
    plan, and when that cannot be resolved they are carried forward from the
    quote already on the booking — editing a price must never quietly strip a
    cancellation policy out of a contract.
    """

    prior = dict(previous or {})

    def carried(key: str, planned: Any) -> Any:
        text = str(planned or "").strip()
        return text or prior.get(key) or ""

    included = list((plan.included_json or []) if plan else [])
    if not included and plan is not None:
        included = list((plan.commercial_terms_json or {}).get("included") or [])
    if not included:
        included = list(prior.get("included") or [])
    return {
        "id": str(pricing.get("rate_plan_id") or (plan.id if plan else "") or prior.get("id") or ""),
        "version": int(plan.version) if plan else int(prior.get("version") or 1),
        "source": "lead_pricing",
        "hourly_rate_cents": int(pricing.get("hourly_rate_cents") or 0),
        "hours": int(pricing.get("hours") or 0),
        "cleaning_fee_cents": int(pricing.get("cleaning_fee_cents") or 0),
        "security_deposit_cents": int(pricing.get("security_deposit_cents") or 0),
        "deposit_type": "percent",
        "deposit_percent_bps": int(pricing.get("deposit_percent_bps") or 0),
        "cancellation_policy": carried(
            "cancellation_policy", plan.cancellation_policy if plan else ""
        ),
        "tax_status": (
            carried("tax_status", plan.tax_status if plan else "") or "review_required"
        ),
        "tax_rate_bps": 0,
        "tax_note": carried("tax_note", plan.tax_note if plan else ""),
        "included": included,
        "addons": [
            str(item.get("name") or "")
            for item in list(pricing.get("addons") or [])
            if str(item.get("name") or "").strip()
        ],
    }


def sync_quote_from_lead_pricing(
    session: Any,
    *,
    reservation: BuildingReservation,
    pricing: dict[str, Any],
    actor: str,
) -> BuildingProposal:
    """Create or update this booking's quote from the lead's pricing.

    Returns the quote the contract will be prepared from. A quote that has been
    accepted is never rewritten in place: a new version is created instead, so
    what a customer already agreed to stays readable.
    """

    totals = compute_totals(pricing)
    plan = (
        session.get(BuildingRatePlan, str(pricing.get("rate_plan_id")))
        if pricing.get("rate_plan_id")
        else None
    )
    existing_quote = session.execute(
        select(BuildingProposal)
        .where(
            BuildingProposal.reservation_id == reservation.id,
            BuildingProposal.proposal_type == "quote",
        )
        .order_by(BuildingProposal.version.desc())
    ).scalars().first()
    existing = existing_quote
    prior_snapshot = dict(
        (existing.rate_plan_snapshot_json or {}) if existing is not None else {}
    )
    if plan is None and existing is not None and existing.rate_plan_id:
        # The hold already bound the one approved plan effective for this date.
        # Reuse it rather than producing an unbound quote.
        plan = session.get(BuildingRatePlan, str(existing.rate_plan_id))

    now = datetime.now(timezone.utc)
    fields = {
        "currency": str(pricing.get("currency") or "USD"),
        "amount_cents": totals["total_cents"],
        "line_items_json": _line_items(pricing, totals),
        "rate_plan_id": str(
            pricing.get("rate_plan_id")
            or (plan.id if plan is not None else "")
            or (existing.rate_plan_id if existing is not None else "")
            or ""
        ),
        "rate_plan_snapshot_json": _rate_snapshot(pricing, plan, prior_snapshot),
        "terms_summary": (
            f"{pricing.get('hours') or 0} hours at "
            f"{int(pricing.get('hourly_rate_cents') or 0) / 100:,.2f}, "
            f"{int(pricing.get('deposit_percent_bps') or 0) / 100:g}% booking deposit, "
            f"{totals['security_deposit_cents'] / 100:,.2f} refundable security deposit."
        ),
        "updated_at": now,
    }

    if existing is None:
        quote = BuildingProposal(
            id=str(uuid4()),
            reservation_id=reservation.id,
            version=1,
            proposal_type="quote",
            status="draft",
            created_by=actor,
            **fields,
        )
        session.add(quote)
        return quote

    if existing.status in {"accepted", "voided"}:
        quote = BuildingProposal(
            id=str(uuid4()),
            reservation_id=reservation.id,
            version=existing.version + 1,
            proposal_type="quote",
            status="draft",
            created_by=actor,
            **fields,
        )
        session.add(quote)
        return quote

    for key, value in fields.items():
        setattr(existing, key, value)
    session.add(existing)
    return existing
