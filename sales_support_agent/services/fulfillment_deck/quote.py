"""Fulfillment quote engine — Anata's contract baseline x category margin.

The baseline numbers below are Anata's contract FLOORS. Every quoted rate is
``baseline x multiplier`` where the multiplier comes from the product
category table (plus a fragile bump, hard-capped for competitiveness), or a
flat admin override. The public sheet renders QUOTED rates only — the
baseline and multiplier never appear in the rendered HTML (the quote dict
keeps them for the admin/History record).

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

# Anata contract baseline (floors, USD). Keys mirror the rate card language.
BASELINE_RATES = {
    "receiving_per_pallet": 20.00,
    "storage_short_per_pallet_mo": 35.00,
    "dtc_base_per_order": 1.60,
    "dtc_additional_item": 0.25,
    "special_handling_per_unit": 0.50,
    "wholesale_per_unit": 0.15,
    "pallet_order_min": 20.00,
    "pallet_per_unit": 0.80,
    "kitting_per_unit": 0.15,
    "returns_per_unit": 2.00,
    "labeling_per_unit": 0.25,
    "monthly_tech_fee": 75.00,
    "monthly_minimum": 500.00,
    "packaging": "at cost + 10%",
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


def _units_per_pallet(profile: ProspectProfile) -> int:
    """clamp(int(pallet cube / unit volume), 50, 2000) averaged across
    products with full dims; default when none have dims."""
    per_product = []
    for product in profile.products:
        if None in (product.length_in, product.width_in, product.height_in):
            continue
        volume = product.length_in * product.width_in * product.height_in
        if volume <= 0:
            continue
        per_product.append(
            max(_UNITS_PER_PALLET_MIN,
                min(int(_PALLET_CUBE_IN3 / volume), _UNITS_PER_PALLET_MAX))
        )
    if not per_product:
        return _UNITS_PER_PALLET_DEFAULT
    return int(sum(per_product) / len(per_product))


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

    m = quote_multiplier(profile, margin_override)
    units_per_pallet = _units_per_pallet(profile)
    pallets = max(1, math.ceil(units_total / units_per_pallet))

    # Average items per order, clamped 1..5 — drives the additional-item fee.
    avg_items = max(1.0, min(5.0, units_total / orders))
    extra_items = max(avg_items - 1.0, 0.0)
    pick_pack_rate = (
        BASELINE_RATES["dtc_base_per_order"]
        + extra_items * BASELINE_RATES["dtc_additional_item"]
    ) * m

    lines = [
        _line(
            "receiving", "Receiving", pallets, "pallets",
            BASELINE_RATES["receiving_per_pallet"] * m,
            pallets * BASELINE_RATES["receiving_per_pallet"] * m,
            multiplier=m,
        ),
        _line(
            "storage", "Storage", pallets, "pallets",
            BASELINE_RATES["storage_short_per_pallet_mo"] * m,
            pallets * BASELINE_RATES["storage_short_per_pallet_mo"] * m,
            multiplier=m,
        ),
        _line(
            "pick_pack", "Pick & pack (DTC)", orders, "orders",
            pick_pack_rate,
            orders * pick_pack_rate,
            multiplier=m,
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
        lines.append(
            _line(
                "wholesale", "Wholesale fulfillment", wholesale_units, "units",
                BASELINE_RATES["wholesale_per_unit"] * m,
                wholesale_units * BASELINE_RATES["wholesale_per_unit"] * m,
                multiplier=m,
                scales_with_orders=True,
            )
        )

    lines.append(
        _line(
            "tech", "Account & tech", 1, "flat",
            BASELINE_RATES["monthly_tech_fee"],
            BASELINE_RATES["monthly_tech_fee"],
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

    monthly_total = round(sum(line["monthly"] for line in lines), 2)
    variable = round(
        sum(line["monthly"] for line in lines if line["scales_with_orders"]), 2
    )
    fixed = round(monthly_total - variable, 2)

    assumptions = [
        (
            f"~{units_per_pallet:,} units per pallet (48×40 pallet, 60in stack, "
            f"65% cube) -> {pallets} pallet{'s' if pallets != 1 else ''}/month"
        ),
        "One month of inventory on hand (storage billed on the same pallet count)",
        "Packaging materials at cost + 10%, billed separately",
        "Anata's $500 monthly minimum applies",
        "Final pricing after a scoping call — this is a directional estimate",
    ]

    return {
        "orders": int(orders),
        "units_total": int(units_total),
        "avg_items_per_order": round(avg_items, 2),
        "units_per_pallet": units_per_pallet,
        "pallets_per_month": pallets,
        "multiplier": m,  # internal — never rendered on the public sheet
        "margin_override_pct": margin_override,
        "blended_rate": round(float(blended_rate), 4) if blended_rate else None,
        "lines": lines,
        "monthly_total": monthly_total,
        "fixed_monthly": fixed,
        "variable_monthly": variable,
        "effective_per_order": round(monthly_total / orders, 2),
        "assumptions": assumptions,
    }
