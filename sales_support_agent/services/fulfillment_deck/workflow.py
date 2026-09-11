"""Versioned proposal state and customer-safe publication contracts."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import math

from .pricing_rules import merge_fee_rows, read_cost_rules
from .quote import BASELINE_RATES, compute_margin, quote_multiplier
from .schema import ProspectProfile, RateMatrix

INPUT_KEYS = frozenset({
    "prospect_profile", "origin_zip", "rate_matrix", "quote_margin_override",
    "rate_overrides", "rate_card_note", "sales_pricing", "fulfillment_actual_costs",
    "fulfillment_cost_submissions", "document_kind", "segment",
})
SNAPSHOT_KEYS = frozenset({
    "deck_html", "document_kind", "segment", "suppress_fulfillment_pricing",
    "prospect", "design_title", "origin_zip", "rates_source", "rate_matrix",
    "blended_rate", "blend_method", "avg_transit_days", "savings", "narrative",
    "rate_card_note", "sections_included", "renderer_version", "view_path",
    "resolved_customer_rates",
})


class RevisionConflict(ValueError):
    """The operator is acting on a version that is no longer current."""


def document_kind(summary: dict) -> str:
    """Infer legacy mode without automatically exposing any hidden fees."""
    if summary.get("segment") == "diy":
        return "shipping_teaser"
    return summary.get("document_kind") or (
        "shipping_teaser" if summary.get("suppress_fulfillment_pricing") else "fulfillment_proposal"
    )


def resolved_rates(summary: dict) -> dict:
    """Resolve final customer prices once, including explicit zero and waivers."""
    profile = ProspectProfile.from_dict(summary.get("prospect_profile") or {})
    multiplier = quote_multiplier(profile, summary.get("quote_margin_override"))
    marked_up = {
        "dtc_base_per_order", "dtc_additional_item", "receiving_per_pallet",
        "storage_short_per_pallet_mo", "wholesale_per_unit", "special_handling_per_unit",
    }
    rates = {key: float(value) * (multiplier if key in marked_up else 1)
             for key, value in BASELINE_RATES.items() if isinstance(value, (int, float))}
    for key, value in (summary.get("rate_overrides") or {}).items():
        if key in rates:
            rates[key] = float(value)
    for row in merge_fee_rows((summary.get("sales_pricing") or {}).get("fee_rows") or []):
        key = row["fee_key"]
        if key in rates and row.get("waived"):
            rates[key] = 0.0
    if any(not math.isfinite(value) or value < 0 for value in rates.values()):
        raise ValueError("Customer prices must be finite, non-negative numbers.")
    return rates


def normalized_costs(costs: dict) -> dict:
    """Ignore unfilled fields when comparing a signed cost submission."""
    return {key: float(value) for key, value in costs.items() if value is not None}


def signed_costs_current(summary: dict) -> bool:
    """Only a signature covering these actual costs is applicable."""
    try:
        costs = normalized_costs(summary.get("fulfillment_actual_costs") or {})
    except (TypeError, ValueError):
        return False
    if not costs:
        return False
    return any(
        row.get("name") and row.get("email") and normalized_costs(row.get("costs") or {}) == costs
        for row in summary.get("fulfillment_cost_submissions") or [] if isinstance(row, dict)
    )


def customer_snapshot(summary: dict) -> dict:
    """Allowlist published content; never serialize an internal summary."""
    result = {key: deepcopy(summary[key]) for key in SNAPSHOT_KEYS if key in summary}
    if not result.get("resolved_customer_rates"):
        try:
            result["resolved_customer_rates"] = resolved_rates(summary)
        except (ValueError, TypeError):
            pass  # Preserve legacy HTML; invalid prices must be corrected in the draft.
    profile = dict(summary.get("prospect_profile") or {})
    # Package and volume inputs used by the public calculator, no contacts or provenance.
    result["prospect_profile"] = {key: deepcopy(profile[key]) for key in (
        "brand", "company", "products", "monthly_order_volume", "destinations_note",
        "current_costs_note", "current_cost_per_parcel_usd", "volume_basis",
    ) if key in profile}
    quote = dict(summary.get("fulfillment_quote") or {})
    result["fulfillment_quote"] = {key: deepcopy(quote[key]) for key in (
        "orders", "units_total", "avg_items_per_order", "units_per_pallet", "pallets",
        "pallets_per_month", "pallet_breakdown", "blended_rate", "monthly_total",
        "fixed_monthly", "variable_monthly", "effective_per_order", "assumptions", "one_time",
    ) if key in quote}
    result["fulfillment_quote"]["lines"] = [
        {key: deepcopy(value) for key, value in row.items() if key != "multiplier"}
        for row in quote.get("lines") or []
    ]
    result["revision"] = int(summary.get("draft_revision") or 1)
    result["published_at"] = summary.get("published_at") or ""
    return result


def migrate(summary: dict, *, published: bool) -> dict:
    """Lazy, idempotent backfill performed under the run's database lock."""
    result = deepcopy(summary)
    result.setdefault("document_kind", document_kind(result))
    result["suppress_fulfillment_pricing"] = document_kind(result) == "shipping_teaser"
    result.setdefault("draft_revision", 1)
    result.setdefault("rendered_revision", 1 if result.get("deck_html") else 0)
    if published and "published_snapshot" not in result:
        result["published_snapshot"] = customer_snapshot(result)
        result["published_calculation"] = {"quote_margin_override": result.get("quote_margin_override")}
    return result


def public_summary(summary: dict) -> dict:
    """Legacy read fallback is safe until the first locked migration/write."""
    if "published_snapshot" in summary:
        return deepcopy(summary.get("published_snapshot") or {})
    return customer_snapshot(summary)


def publication_errors(summary: dict, *, require_review: bool = True) -> list[str]:
    """Standalone proposal guards: deliberately no CRM imports or requirements."""
    errors: list[str] = []
    revision = int(summary.get("draft_revision") or 1)
    if not summary.get("deck_html") or int(summary.get("rendered_revision") or 0) != revision:
        errors.append("Save draft to rebuild the stale customer preview.")
    if document_kind(summary) == "shipping_teaser":
        return errors
    try:
        resolved_rates(summary)
    except (ValueError, TypeError):
        errors.append("Correct invalid customer prices in Pricing & Cost Lines.")
    if not (summary.get("fulfillment_quote") or {}).get("orders"):
        errors.append("Enter monthly order volume and save draft.")
    if summary.get("rates_source") != "wms" or RateMatrix.from_dict(summary.get("rate_matrix") or {}).source != "wms":
        errors.append("Request live carrier rates before publishing the fulfillment proposal.")
    if not signed_costs_current(summary):
        errors.append("Collect a signed fulfillment cost form for the current warehouse costs.")
    pricing = summary.get("sales_pricing") or {}
    if require_review and (summary.get("pricing_review") or {}).get("revision") != revision:
        errors.append("Review the saved customer preview, then approve pricing for this draft.")
    for row in merge_fee_rows(pricing.get("fee_rows") or []):
        if row.get("waived") and not str(row.get("waiver_reason") or pricing.get("waiver_reason") or "").strip():
            errors.append(f"Add a waiver reason for {row.get('label') or row['fee_key']}.")
    quote = summary.get("fulfillment_quote") or {}
    costs = summary.get("fulfillment_actual_costs") or {}
    try:
        if any(not math.isfinite(v) or v < 0 for v in normalized_costs(costs).values()):
            raise ValueError()
        margin = compute_margin(float(quote.get("monthly_total") or 0), costs,
            ProspectProfile.from_dict(summary.get("prospect_profile") or {}),
            sum(float(row.get("monthly") or 0) for row in quote.get("lines") or [] if row.get("key") == "shipping"))
        if margin["margin_pct"] < float(read_cost_rules().get("minimum_margin_pct", 15)) and not pricing.get("margin_approved"):
            errors.append("Margin is below the configured minimum. Record margin approval before publishing.")
    except (TypeError, ValueError, OverflowError):
        errors.append("Correct invalid warehouse cost values before publishing.")
    return errors


def history_event(event: str, actor: str, revision: int) -> dict:
    """Small audit event without copying prices or private payloads into logs."""
    return {"at": datetime.now(timezone.utc).isoformat(), "event": event,
            "detail": f"Draft revision {revision}", "user_email": actor}
