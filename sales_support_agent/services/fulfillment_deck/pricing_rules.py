"""Sales pricing and quote guard helpers for fulfillment rate sheets."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


_CONFIG_PATH = Path(__file__).resolve().parents[3] / "config" / "fulfillment_cost_rules.json"
_MARKET_CONFIG_PATH = Path(__file__).resolve().parents[3] / "config" / "fulfillment_market_pricing.json"


def read_cost_rules() -> dict[str, Any]:
    try:
        return json.loads(_CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"minimum_margin_pct": 15, "fees": [], "volume_tiers": []}


def read_market_pricing() -> dict[str, Any]:
    try:
        return json.loads(_MARKET_CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"market_ranges": {}, "source_note": ""}


def _money(value: float) -> str:
    if abs(value - round(value)) < 0.005:
        return f"${value:,.0f}"
    return f"${value:,.2f}"


def suggest_customer_price(
    fee_key: str,
    *,
    internal_cost: float | None,
    agreement_default: float | None,
    margin_override_pct: float | None,
) -> dict[str, Any]:
    """Build a market-aware customer price suggestion for one line item.

    The result is guidance only. Sales still owns the final customer price and
    can intentionally match a line to cost to close the deal.
    """
    market = (read_market_pricing().get("market_ranges") or {}).get(fee_key) or {}
    low = market.get("low")
    high = market.get("high")
    markup_pct = 20.0 if margin_override_pct is None else float(margin_override_pct or 0)
    candidates: list[float] = []
    if internal_cost is not None:
        candidates.append(max(float(internal_cost), 0.0) * (1 + max(markup_pct, 0.0) / 100.0))
    if agreement_default is not None:
        candidates.append(max(float(agreement_default), 0.0))
    if low is not None:
        candidates.append(float(low))
    if not candidates:
        return {"price": None, "rationale": "No suggestion yet.", "market": None}

    suggested = max(candidates)
    if low is not None and high is not None:
        suggested = max(float(low), suggested)
        if suggested > float(high) and (internal_cost is None or float(internal_cost) <= float(high)):
            suggested = float(high)

    reasons: list[str] = []
    if low is not None and high is not None:
        reasons.append(f"Market {_money(float(low))}-{_money(float(high))}")
    if agreement_default is not None:
        reasons.append(f"Agreement {_money(float(agreement_default))}")
    if internal_cost is not None:
        reasons.append(f"Cost+target {_money(float(internal_cost) * (1 + max(markup_pct, 0.0) / 100.0))}")
    note = str(market.get("note") or "").strip()
    if note:
        reasons.append(note)
    reasons.append("Sales can match cost to close; call it out as a concession.")
    return {
        "price": round(float(suggested), 2),
        "rationale": " · ".join(reasons),
        "market": market or None,
    }


def default_fee_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for fee in read_cost_rules().get("fees") or []:
        if not isinstance(fee, dict):
            continue
        key = str(fee.get("fee_key") or "").strip()
        if not key:
            continue
        baseline = fee.get("baseline_cost")
        suggested = fee.get("suggested_customer_price", baseline)
        rows.append({
            "fee_key": key,
            "label": str(fee.get("label") or key),
            "baseline_cost": baseline,
            "suggested_customer_price": suggested,
            "customer_price": suggested,
            "unit": str(fee.get("unit") or ""),
            "quantity": 1,
            "waivable": bool(fee.get("waivable", True)),
            "waived": False,
            "waiver_reason": "",
            "sales_override_price": None,
            "internal_notes": "",
            "prospect_notes": "",
        })
    return rows


def merge_fee_rows(stored: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    by_key = {
        str(row.get("fee_key") or ""): dict(row)
        for row in (stored or [])
        if isinstance(row, dict) and str(row.get("fee_key") or "").strip()
    }
    merged: list[dict[str, Any]] = []
    for row in default_fee_rows():
        key = row["fee_key"]
        if key in by_key:
            copy = dict(row)
            copy.update(by_key[key])
            merged.append(copy)
        else:
            merged.append(row)
    return merged


def validate_quote_readiness(summary: dict[str, Any], *, published: bool) -> list[str]:
    """Return blocking reasons before a HubSpot quote can be created."""
    errors: list[str] = []
    if not published:
        errors.append("Publish the rate sheet before creating a HubSpot quote.")
    if not str(summary.get("hubspot_deal_id") or "").strip():
        errors.append("Select or confirm the HubSpot deal before creating a quote.")
    if not str(summary.get("view_path") or "").strip():
        errors.append("Rate sheet public link is missing.")
    if str(summary.get("rates_source") or "").strip().lower() == "mock":
        errors.append("Configure live WMS carrier rates before creating a quote.")
    signed_costs = [
        s for s in (summary.get("fulfillment_cost_submissions") or [])
        if isinstance(s, dict) and str(s.get("name") or "").strip() and str(s.get("email") or "").strip()
    ]
    if not signed_costs:
        errors.append("Collect a signed fulfillment cost submission before creating a quote.")
    sales_pricing = dict(summary.get("sales_pricing") or {})
    if not sales_pricing.get("reviewed"):
        errors.append("Review sales pricing and waivers before creating a quote.")
    rows = merge_fee_rows(sales_pricing.get("fee_rows") or summary.get("pricing_fee_rows") or [])
    waived_without_reason = [
        str(row.get("label") or row.get("fee_key"))
        for row in rows
        if row.get("waived") and not str(row.get("waiver_reason") or sales_pricing.get("waiver_reason") or "").strip()
    ]
    if waived_without_reason:
        errors.append("Add waiver reasons for: " + ", ".join(waived_without_reason[:4]))
    margin = sales_pricing.get("margin_pct")
    minimum = read_cost_rules().get("minimum_margin_pct", 15)
    try:
        if margin is not None and float(margin) < float(minimum) and not sales_pricing.get("margin_approved"):
            errors.append(f"Margin is below {minimum:g}% and needs approval.")
    except (TypeError, ValueError):
        pass
    return errors
