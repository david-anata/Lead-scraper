"""Fulfillment quote engine — Anata customer rate defaults x category margin.

The customer defaults below come from Anata's template fulfillment agreement.
Blank sales fields quote ``default x multiplier`` where the multiplier comes
from the product category table (plus a fragile bump, hard-capped for
competitiveness), or a flat margin override. Admin-entered rate overrides are
treated as final customer prices and are not multiplied again. The public
sheet renders quoted rates only; internal costs and multipliers stay in the
admin/workflow record.

Pure module: no I/O, deterministic, returns plain dicts the service stores
in the run summary as ``fulfillment_quote``.
"""

from __future__ import annotations

import math
import re
from typing import Optional

from sales_support_agent.services.fulfillment_deck.schema import (
    ProspectProfile,
    RateMatrix,
)

# Fulfillment manager internal baseline costs. These are warehouse-only values
# used by the admin cost form and margin review, not customer-facing defaults.
INTERNAL_COST_BASELINES = {
    "receiving_precounted_box": 2.00,
    "receiving_count_per_item": 0.15,
    "receiving_per_pallet": 2.00,
    "storage_short_per_pallet_mo": 30.00,
    "storage_cubic_foot_mo": 0.45,
    "dtc_base_per_order": 0.80,
    "dtc_additional_item": 0.15,
    "pallet_order_per_pallet": 20.00,
    "kitting_per_unit": 0.15,
    "labeling_per_unit": 0.15,
    "bagging_labeling_per_unit": 0.25,
    "returns_receive_per_unit": 1.00,
    "returns_examination_per_unit": 1.00,
    "returns_custom_steps_per_unit": 2.00,
    "special_projects_per_hour": 40.00,
    "packaging_markup_pct": 5.00,
    "monthly_tech_fee": 50.00,
    "customer_service_monthly": 200.00,
}

# Customer-facing template agreement defaults (USD). Keys mirror the rate card
# language. Some lines are optional/custom in the agreement but remain
# chargeable here so sales can quote or waive them intentionally.
BASELINE_RATES = {
    "receiving_precounted_box": 2.00,
    "receiving_count_per_item": 0.15,
    # Back-compat key used by the existing receiving estimate UI.
    "receiving_per_pallet": 20.00,
    "uro_fee": 35.00,
    "storage_short_per_pallet_mo": 35.00,
    "storage_long_per_pallet_mo": 45.00,
    "storage_cubic_foot_mo": 0.45,
    "dtc_base_per_order": 1.60,
    "dtc_additional_item": 0.15,
    "special_handling_per_unit": 0.50,
    "wholesale_per_unit": 0.15,
    "pallet_order_min": 20.00,
    "pallet_order_per_pallet": 20.00,
    "pallet_per_unit": 0.80,
    "kitting_per_unit": 0.15,
    "labeling_per_unit": 0.25,
    "bagging_labeling_per_unit": 0.25,
    "returns_receive_per_unit": 1.00,
    "returns_examination_per_unit": 1.00,
    "returns_custom_steps_per_unit": 2.00,
    "returns_per_unit": 2.00,
    "special_projects_per_hour": 40.00,
    "monthly_tech_fee": 75.00,
    "integration_setup_fee": 2000.00,
    "customer_service_monthly": 200.00,
    "monthly_minimum": 500.00,
    "packaging_markup_pct": 10.00,
    "packaging": "at cost + 10%",
    "late_fee_pct_per_7_days": 2.00,
    "fba_prep_per_unit": 0.75,
    "fnsku_labeling_per_unit": 0.25,
    "bundle_2pack_per_bundle": 1.00,
    "custom_kitting_per_kit": 1.50,
    "carton_labeling_per_carton": 1.00,
    "label_printing_per_label": 0.10,
}

# Category -> margin multiplier over the baseline floors.
CATEGORY_MULTIPLIERS = {
    "beauty": 1.15,
    "food": 1.15,
    "electronics": 1.12,
    "supplements": 1.10,
    "home": 1.08,
    "apparel": 1.05,
    "other": 1.10,
}
FRAGILE_BUMP = 0.05
MULTIPLIER_CAP = 1.25  # competitive hard cap

# Product names that look like wholesale/freight-shaped volume (shared with
# rendering's monthly-math caveat — single source of truth lives here).
WHOLESALE_RE = re.compile(r"b2b|wholesale|pallet|case", re.IGNORECASE)

# Pallet math: 48x40 pallet stacked to 60in at 65% cube utilization.
_PALLET_CUBE_IN3 = 48 * 40 * 60 * 0.65
_UNITS_PER_PALLET_MIN = 50
_UNITS_PER_PALLET_MAX = 2000
_UNITS_PER_PALLET_DEFAULT = 500  # used when no product has dims

# Packaging size classes (estimate per order, billed at cost + 10%). Class is
# decided by the largest single dimension among DTC products (and weight for
# the poly-mailer cut).
PACKAGING_CLASSES = (
    # (class name, est. cost per order, max dim in, max weight lb)
    ("poly mailer", 0.35, 9.0, 1.0),
    ("small box", 0.65, 14.0, None),
    ("medium box", 0.95, None, None),
)

# One-time fees — listed transparently below the monthly estimate, NEVER
# counted into the monthly total or the per-order effective number.
ONE_TIME_FEES = (
    {
        "key": "implementation",
        "label": "Implementation & onboarding",
        "amount": BASELINE_RATES["integration_setup_fee"],
        "unit": "one-time",
        "note": "dedicated onboarding specialist",
    },
    {
        "key": "uro",
        "label": "Unidentified receiving order (URO)",
        "amount": 35.00,
        "unit": "per occurrence",
        "note": "only when inventory arrives unannounced — avoidable",
    },
)


def _product_multiplier(product) -> float:
    base = CATEGORY_MULTIPLIERS.get(
        (product.product_category or "other"), CATEGORY_MULTIPLIERS["other"]
    )
    if product.fragile:
        base += FRAGILE_BUMP
    return min(base, MULTIPLIER_CAP)


def quote_multiplier(profile: ProspectProfile, margin_override: Optional[float] = None) -> float:
    """The margin multiplier for this prospect's quote.

    ``margin_override`` is a flat percentage (12 -> x1.12) that replaces the
    category table entirely. Otherwise: units-weighted average of per-product
    (category multiplier + fragile bump), equal weights when no product has
    units, hard-capped at MULTIPLIER_CAP.
    """
    if margin_override is not None:
        try:
            return round(min(1.0 + float(margin_override) / 100.0, MULTIPLIER_CAP), 4)
        except (TypeError, ValueError):
            pass
    products = list(profile.products)
    if not products:
        return CATEGORY_MULTIPLIERS["other"]
    any_units = any(p.monthly_units for p in products)
    total_weight = 0.0
    total = 0.0
    for product in products:
        weight = float(product.monthly_units or 0) if any_units else 1.0
        if weight <= 0:
            continue
        total += _product_multiplier(product) * weight
        total_weight += weight
    if not total_weight:
        return CATEGORY_MULTIPLIERS["other"]
    return round(min(total / total_weight, MULTIPLIER_CAP), 4)


def _product_units_per_pallet(product) -> Optional[int]:
    """clamp(int(pallet cube / unit volume), 50, 2000) from THIS product's
    dims; None when the product has no usable dims."""
    if None in (product.length_in, product.width_in, product.height_in):
        return None
    volume = product.length_in * product.width_in * product.height_in
    if volume <= 0:
        return None
    return max(
        _UNITS_PER_PALLET_MIN,
        min(int(_PALLET_CUBE_IN3 / volume), _UNITS_PER_PALLET_MAX),
    )


def _dominant_category(profile: ProspectProfile) -> str:
    """Units-weighted dominant product category for the margin-basis bullet
    ("rates reflect beauty-category handling"). "standard" when no product
    claims a category — the multiplier number itself is never exposed."""
    weights: dict[str, float] = {}
    any_units = any(p.monthly_units for p in profile.products)
    for product in profile.products:
        category = product.product_category or ""
        if not category:
            continue
        weight = float(product.monthly_units or 0) if any_units else 1.0
        if weight <= 0:
            continue
        weights[category] = weights.get(category, 0.0) + weight
    if not weights:
        return "standard"
    return max(sorted(weights), key=lambda c: weights[c])


def _units_per_pallet(profile: ProspectProfile) -> int:
    """Average of per-product units/pallet (legacy fallback when no product
    has units); default when none have dims."""
    per_product = [
        upp for upp in (_product_units_per_pallet(p) for p in profile.products)
        if upp is not None
    ]
    if not per_product:
        return _UNITS_PER_PALLET_DEFAULT
    return int(sum(per_product) / len(per_product))


def _pallet_breakdown(profile: ProspectProfile) -> list[dict]:
    """Per-product pallet math from each product's OWN dims (not an average).

    Only products with stated monthly_units contribute rows; products without
    dims use the default units/pallet. Returns
    [{name, units, units_per_pallet, pallets}].
    """
    rows: list[dict] = []
    for product in profile.products:
        units = product.monthly_units or 0
        if units <= 0:
            continue
        upp = _product_units_per_pallet(product) or _UNITS_PER_PALLET_DEFAULT
        rows.append({
            "name": product.name or "(unnamed product)",
            "units": int(units),
            "units_per_pallet": int(upp),
            "pallets": int(math.ceil(units / upp)),
        })
    return rows


def _packaging_class(profile: ProspectProfile) -> tuple[str, float, str]:
    """(class name, est. fulfillment cost+markup per order, why)."""
    dtc = [
        p for p in profile.products
        if not (p.name and WHOLESALE_RE.search(p.name))
    ] or list(profile.products)
    dims = [
        max(p.length_in, p.width_in, p.height_in)
        for p in dtc
        if None not in (p.length_in, p.width_in, p.height_in)
    ]
    if not dims:
        name, cost, _d, _w = PACKAGING_CLASSES[1]  # small box
        return name, _with_packaging_markup(cost), "package size unconfirmed → small box class assumed"
    max_dim = max(dims)
    weights = [p.weight_lb for p in dtc if p.weight_lb is not None]
    max_weight = max(weights) if weights else None
    driver = max(
        (p for p in dtc if None not in (p.length_in, p.width_in, p.height_in)),
        key=lambda p: max(p.length_in, p.width_in, p.height_in),
    )
    dims_label = f"{driver.length_in:g}×{driver.width_in:g}×{driver.height_in:g}in"
    poly_name, poly_cost, poly_dim, poly_weight = PACKAGING_CLASSES[0]
    small_name, small_cost, small_dim, _sw = PACKAGING_CLASSES[1]
    medium_name, medium_cost, _md, _mw = PACKAGING_CLASSES[2]
    if max_dim <= poly_dim and max_weight is not None and max_weight <= poly_weight:
        return poly_name, _with_packaging_markup(poly_cost), f"{dims_label} parcel → poly mailer class"
    if max_dim <= small_dim:
        return small_name, _with_packaging_markup(small_cost), f"{dims_label} parcel → small box class"
    return medium_name, _with_packaging_markup(medium_cost), f"{dims_label} parcel → medium box class"


def _with_packaging_markup(cost: float) -> float:
    return cost * (1 + float(BASELINE_RATES["packaging_markup_pct"]) / 100.0)


def _effective_rates(rate_overrides: Optional[dict] = None) -> dict:
    rates = dict(BASELINE_RATES)
    for key, value in (rate_overrides or {}).items():
        if key not in rates:
            continue
        try:
            rates[key] = float(value)
        except (TypeError, ValueError):
            continue
    return rates


def _valid_override_keys(rate_overrides: Optional[dict] = None) -> set[str]:
    keys: set[str] = set()
    for key, value in (rate_overrides or {}).items():
        if key not in BASELINE_RATES:
            continue
        try:
            float(value)
        except (TypeError, ValueError):
            continue
        keys.add(key)
    return keys


def _customer_rate(
    key: str,
    rates: dict,
    multiplier: float,
    overridden_keys: set[str],
    *,
    marginable: bool = True,
) -> tuple[float, float]:
    """Return the quoted customer rate and multiplier metadata.

    Admin-entered customer rates are final prices. Blank fields use agreement
    defaults and apply the product/category margin multiplier.
    """
    rate = float(rates[key])
    if key in overridden_keys or not marginable:
        return rate, 1.0
    return rate * multiplier, multiplier


def _line(key: str, label: str, qty: float, unit: str, rate: float,
          monthly: float, *, multiplier: float = 1.0,
          scales_with_orders: bool = False, note: str = "") -> dict:
    return {
        "key": key,
        "label": label,
        "qty": qty,
        "unit": unit,
        "rate": round(rate, 4),
        "monthly": round(monthly, 2),
        "multiplier": round(multiplier, 4),  # internal — never rendered publicly
        "scales_with_orders": scales_with_orders,
        "note": note,
    }


def build_fulfillment_quote(
    profile: ProspectProfile,
    matrix: RateMatrix,
    blended_rate: Optional[float],
    *,
    margin_override: Optional[float] = None,
    rate_overrides: Optional[dict] = None,
) -> Optional[dict]:
    """Directional monthly fulfillment invoice for the prospect.

    Returns None when neither orders nor units are known (the quote section
    is omitted, monthly-math style). All rates are baseline x multiplier;
    tech fee and shipping carry no margin.
    """
    units_total = sum(p.monthly_units or 0 for p in profile.products)
    orders = profile.monthly_order_volume or units_total
    if not orders:
        return None
    if not units_total:
        units_total = orders  # assume one unit per order when units unknown

    br = _effective_rates(rate_overrides)
    overridden_keys = _valid_override_keys(rate_overrides)
    m = quote_multiplier(profile, margin_override)
    units_per_pallet = _units_per_pallet(profile)
    # PER-PRODUCT pallet math: each product's pallets from ITS dims; the
    # receiving/storage qty is the SUM of per-product pallet counts. Falls
    # back to the pooled average only when no product states units.
    pallet_rows = _pallet_breakdown(profile)
    if pallet_rows:
        pallets = max(1, sum(row["pallets"] for row in pallet_rows))
    else:
        pallets = max(1, math.ceil(units_total / units_per_pallet))

    # Average items per order, clamped 1..5 — drives the additional-item fee.
    avg_items = max(1.0, min(5.0, units_total / orders))
    extra_items = max(avg_items - 1.0, 0.0)
    pick_pack_base_rate, pick_pack_base_multiplier = _customer_rate(
        "dtc_base_per_order", br, m, overridden_keys
    )
    additional_item_rate, additional_item_multiplier = _customer_rate(
        "dtc_additional_item", br, m, overridden_keys
    )
    pick_pack_rate = pick_pack_base_rate + extra_items * additional_item_rate
    pick_pack_multiplier = pick_pack_base_multiplier
    if extra_items > 0:
        pick_pack_multiplier = max(pick_pack_base_multiplier, additional_item_multiplier)
    receiving_rate, receiving_multiplier = _customer_rate(
        "receiving_per_pallet", br, m, overridden_keys
    )
    storage_rate, storage_multiplier = _customer_rate(
        "storage_short_per_pallet_mo", br, m, overridden_keys
    )

    lines = [
        _line(
            "receiving", "Receiving", pallets, "pallets",
            receiving_rate,
            pallets * receiving_rate,
            multiplier=receiving_multiplier,
        ),
        _line(
            "storage", "Storage", pallets, "pallets",
            storage_rate,
            pallets * storage_rate,
            multiplier=storage_multiplier,
        ),
        _line(
            "pick_pack", "Pick & pack (DTC)", orders, "orders",
            pick_pack_rate,
            orders * pick_pack_rate,
            multiplier=pick_pack_multiplier,
            scales_with_orders=True,
        ),
    ]

    # Wholesale fulfillment: only for products whose name smells wholesale.
    wholesale_units = sum(
        p.monthly_units or 0
        for p in profile.products
        if p.name and WHOLESALE_RE.search(p.name)
    )
    if wholesale_units:
        wholesale_rate, wholesale_multiplier = _customer_rate(
            "wholesale_per_unit", br, m, overridden_keys
        )
        lines.append(
            _line(
                "wholesale", "Wholesale fulfillment", wholesale_units, "units",
                wholesale_rate,
                wholesale_units * wholesale_rate,
                multiplier=wholesale_multiplier,
                scales_with_orders=True,
            )
        )

    # Packaging: size-class estimate per order, billed at package cost + 10%,
    # then marked up by the sales margin multiplier.
    packaging_class, packaging_cost, packaging_why = _packaging_class(profile)
    packaging_rate = packaging_cost * m
    lines.append(
        _line(
            "packaging", "Packaging (est., cost +10% before margin)",
            orders, "orders",
            packaging_rate,
            orders * packaging_rate,
            multiplier=m,
            scales_with_orders=True,
        )
    )

    # Fragile special handling: only when a flagged product carries units.
    fragile_products = [p for p in profile.products if p.fragile]
    fragile_units = sum(p.monthly_units or 0 for p in fragile_products)
    if fragile_units:
        fragile_rate, fragile_multiplier = _customer_rate(
            "special_handling_per_unit", br, m, overridden_keys
        )
        lines.append(
            _line(
                "fragile", "Special handling (fragile)",
                fragile_units, "units",
                fragile_rate,
                fragile_units * fragile_rate,
                multiplier=fragile_multiplier,
                scales_with_orders=True,
            )
        )

    lines.append(
        _line(
            "tech", "Account & tech", 1, "flat",
            br["monthly_tech_fee"],
            br["monthly_tech_fee"],
        )
    )
    if blended_rate:
        lines.append(
            _line(
                "shipping", "Shipping", orders, "orders",
                blended_rate,
                orders * blended_rate,
                scales_with_orders=True,
                note="at the carrier rates above",
            )
        )

    sum_lines = round(sum(line["monthly"] for line in lines), 2)
    # Anata's $500 monthly minimum: a tiny-volume account that prices out below
    # the floor is topped up to $500 with a visible adjustment line (counted as
    # FIXED). Above the floor, the total is the line sum as-is.
    monthly_minimum = br["monthly_minimum"]
    floor_applied = sum_lines < monthly_minimum
    if floor_applied:
        lines.append({
            "label": "Monthly minimum adjustment",
            "qty_label": "",
            "monthly": round(monthly_minimum - sum_lines, 2),
            "scales_with_orders": False,
        })
        monthly_total = round(monthly_minimum, 2)
    else:
        monthly_total = sum_lines

    variable = round(
        sum(line["monthly"] for line in lines if line["scales_with_orders"]), 2
    )
    fixed = round(monthly_total - variable, 2)

    # HOW-DETERMINED assumptions: every line item maps to at least one bullet
    # explaining its derivation. The margin basis bullet names the category
    # WITHOUT ever exposing multiplier numbers.
    assumptions: list[str] = []
    if profile.volume_basis.strip():
        assumptions.append(
            f"Order volume: {profile.volume_basis.strip()} = {orders:,} orders/month"
        )
    else:
        assumptions.append(f"Order volume: {orders:,} orders/month, as stated")
    if pallet_rows:
        for row in pallet_rows:
            assumptions.append(
                f"{row['name']}: ~{row['units_per_pallet']:,} units/pallet → "
                f"{row['pallets']} pallet{'s' if row['pallets'] != 1 else ''}/mo"
            )
        assumptions.append(
            f"Receiving & storage billed on {pallets} pallet"
            f"{'s' if pallets != 1 else ''}/month total (48×40 pallet, 60in "
            f"stack, 65% cube; one month of inventory on hand)"
        )
    else:
        assumptions.append(
            f"~{units_per_pallet:,} units per pallet (48×40 pallet, 60in stack, "
            f"65% cube) -> {pallets} pallet{'s' if pallets != 1 else ''}/month, "
            "one month of inventory on hand"
        )
    assumptions.append(f"Packaging: {packaging_why}, billed at cost + 10% before sales margin")
    if fragile_units:
        names = ", ".join(
            p.name or "(unnamed product)" for p in fragile_products
        )
        assumptions.append(
            f"Special handling applied for fragile product"
            f"{'s' if len(fragile_products) != 1 else ''}: {names}"
        )
    dominant = _dominant_category(profile)
    assumptions.append(f"Rates reflect {dominant}-category handling")
    if floor_applied:
        assumptions.append(
            "Anata's $500 monthly minimum applied (added as an adjustment above)"
        )
    else:
        assumptions.append("Anata's $500 monthly minimum applies to all accounts")
    assumptions.append(
        "Final pricing after a scoping call — this is a directional estimate"
    )

    # C6: the headline pallet stat reconciles with the BILLED pallet count.
    # ``pallets`` (total) is the unambiguous stat across mixed products — it
    # equals the sum of the per-product breakdown that receiving/storage bill
    # against. ``units_per_pallet`` is now the effective overall figure derived
    # from that billed count (units_total / billed pallets), so it can no
    # longer drift away from the breakdown the way the legacy pooled average
    # did; the exact per-product units/pallet still live in pallet_breakdown
    # and the assumption bullets.
    headline_units_per_pallet = int(round(units_total / pallets)) if pallets else units_per_pallet

    one_time = []
    for fee in ONE_TIME_FEES:
        item = dict(fee)
        if item.get("key") == "implementation":
            item["amount"] = float(br["integration_setup_fee"])
        one_time.append(item)

    return {
        "orders": int(orders),
        "units_total": int(units_total),
        "avg_items_per_order": round(avg_items, 2),
        "units_per_pallet": headline_units_per_pallet,
        "pallets": int(pallets),
        "pallets_per_month": pallets,
        "pallet_breakdown": pallet_rows,
        "packaging_class": packaging_class,
        "multiplier": m,  # internal — never rendered on the public sheet
        "margin_override_pct": margin_override,
        "blended_rate": round(float(blended_rate), 4) if blended_rate else None,
        "lines": lines,
        "monthly_total": monthly_total,
        "fixed_monthly": fixed,
        "variable_monthly": variable,
        "effective_per_order": round(monthly_total / orders, 2),
        "assumptions": assumptions,
        # One-time fees: listed transparently, never in the monthly total.
        "one_time": one_time,
    }


# ---------------------------------------------------------------------------
# Pipeline margin helpers (two-sided pricing: what we pitch vs. actual cost)
# ---------------------------------------------------------------------------


def estimate_pallets_mo(profile: ProspectProfile) -> float:
    """Estimate pallets stored per month from the prospect profile.

    Uses the same units-per-pallet logic as the quote engine so numbers
    are consistent across the quote and the pipeline margin view.
    """
    total_units = sum((p.monthly_units or 0) for p in profile.products)
    if not total_units and profile.monthly_order_volume:
        total_units = profile.monthly_order_volume
    if not total_units:
        return 0.0
    upp = _units_per_pallet(profile)
    return round(total_units / upp, 2) if upp else 0.0


def estimate_storage_cuft_mo(profile: ProspectProfile) -> float:
    total = 0.0
    for product in profile.products:
        units = product.monthly_units or 0
        if units <= 0 or None in (product.length_in, product.width_in, product.height_in):
            continue
        total += (product.length_in * product.width_in * product.height_in / 1728.0) * units
    return round(total, 2)


def compute_margin(
    pitched_monthly: float,
    actual_costs: dict,
    profile: ProspectProfile,
    pass_through_monthly: float = 0.0,
) -> dict:
    """Compute monthly margin after pass-through revenue and warehouse cost.

    ``pitched_monthly`` is the customer-facing monthly estimate. Carrier /
    shipping pass-through is revenue shown to the prospect but not profit that
    Anata earns, so it is excluded before subtracting fulfillment costs.
    Receiving is returned as a one-time estimate and excluded from monthly
    margin. Returns a dict with the breakdown plus margin_pct and annual_margin.
    """
    pass_through_monthly = max(float(pass_through_monthly or 0), 0.0)
    marginable_revenue = max(round(float(pitched_monthly or 0) - pass_through_monthly, 2), 0.0)
    pick_pack = float(actual_costs.get("pick_pack_per_order") or 0)
    additional_item = float(actual_costs.get("pick_pack_additional_item") or 0)
    storage = float(actual_costs.get("storage_per_pallet_mo") or 0)
    storage_cuft = float(actual_costs.get("storage_cubic_foot_mo") or 0)
    tech_fee = float(actual_costs.get("monthly_tech_fee") or 0)
    customer_service = float(actual_costs.get("customer_service_monthly") or 0)
    kitting = float(actual_costs.get("kitting_per_item") or 0)
    labeling = float(actual_costs.get("labeling_per_item") or 0)
    bagging_labeling = float(actual_costs.get("bagging_labeling_per_item") or 0)
    pallet_order = float(actual_costs.get("pallet_order_per_pallet") or 0)
    returns_receive = float(actual_costs.get("returns_receive_per_unit") or 0)
    returns_exam = float(actual_costs.get("returns_examination_per_unit") or 0)
    returns_custom = float(actual_costs.get("returns_custom_steps_per_unit") or 0)
    returns_units = float(actual_costs.get("returns_units_mo") or 0)
    special_project_hour = float(actual_costs.get("special_projects_per_hour") or 0)
    special_project_hours = float(actual_costs.get("special_project_hours_mo") or 0)
    orders = profile.monthly_order_volume or 0
    units_total = sum((p.monthly_units or 0) for p in profile.products) or orders
    avg_items = max(1.0, units_total / orders) if orders else 1.0
    extra_items = max(avg_items - 1.0, 0.0)
    pallets = estimate_pallets_mo(profile)
    storage_cuft_mo = estimate_storage_cuft_mo(profile)
    actual_pp = round((pick_pack * orders) + (additional_item * extra_items * orders), 2)
    pallet_storage = round(storage * pallets, 2)
    cubic_storage = round(storage_cuft * storage_cuft_mo, 2)
    actual_st = max(pallet_storage, cubic_storage)
    actual_kitting = round(kitting * units_total, 2)
    actual_labeling = round(labeling * units_total, 2)
    actual_bagging_labeling = round(bagging_labeling * units_total, 2)
    actual_pallet_orders = round(pallet_order * pallets, 2)
    actual_returns = round((returns_receive + returns_exam + returns_custom) * returns_units, 2)
    actual_special_projects = round(special_project_hour * special_project_hours, 2)
    optional_monthly = round(
        actual_kitting + actual_labeling + actual_bagging_labeling
        + actual_pallet_orders + actual_returns + actual_special_projects
        + customer_service,
        2,
    )
    actual_monthly = round(actual_pp + actual_st + tech_fee + optional_monthly, 2)
    monthly_margin = round(marginable_revenue - actual_monthly, 2)
    margin_pct = (
        round(monthly_margin / marginable_revenue * 100, 1) if marginable_revenue > 0 else 0.0
    )
    return {
        "pitched_monthly": round(float(pitched_monthly or 0), 2),
        "pass_through_monthly": round(pass_through_monthly, 2),
        "marginable_revenue": marginable_revenue,
        "actual_pick_pack": actual_pp,
        "actual_storage": actual_st,
        "actual_storage_pallet": pallet_storage,
        "actual_storage_cubic": cubic_storage,
        "actual_tech_fee": round(tech_fee, 2),
        "actual_customer_service": round(customer_service, 2),
        "actual_kitting": actual_kitting,
        "actual_labeling": actual_labeling,
        "actual_bagging_labeling": actual_bagging_labeling,
        "actual_pallet_orders": actual_pallet_orders,
        "actual_returns": actual_returns,
        "actual_special_projects": actual_special_projects,
        "actual_optional_monthly": optional_monthly,
        "actual_monthly": actual_monthly,
        "monthly_margin": monthly_margin,
        "annual_margin": round(monthly_margin * 12, 2),
        "margin_pct": margin_pct,
        "orders": orders,
        "units_total": units_total,
        "pallets_mo": pallets,
        "storage_cuft_mo": storage_cuft_mo,
    }
