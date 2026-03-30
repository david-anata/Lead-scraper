from __future__ import annotations

import re
from typing import Any


def _flatten_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        return " ".join(_flatten_text(item) for item in value.values())
    if isinstance(value, list):
        return " ".join(_flatten_text(item) for item in value)
    return ""


def _extract_first_text(value: Any) -> str:
    flattened = _flatten_text(value)
    return re.sub(r"\s+", " ", flattened).strip()


def extract_money_amount(value: Any) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)

    text = _extract_first_text(value)
    if not text:
        return None

    normalized = text.strip()
    try:
        return float(normalized.replace("$", "").replace(",", ""))
    except (TypeError, ValueError):
        pass

    matches = re.findall(r"(\d+(?:,\d{3})*(?:\.\d+)?)\s*([kmb])?", normalized.lower())
    if not matches:
        return None

    amounts: list[float] = []
    for amount_text, suffix in matches:
        try:
            amount = float(amount_text.replace(",", ""))
        except ValueError:
            continue
        multiplier = 1.0
        if suffix == "k":
            multiplier = 1_000.0
        elif suffix == "m":
            multiplier = 1_000_000.0
        elif suffix == "b":
            multiplier = 1_000_000_000.0
        amounts.append(amount * multiplier)

    return max(amounts) if amounts else None


def parse_monthly_sales(store: dict[str, Any]) -> float | None:
    monthly_candidates = (
        "estimated_sales",
        "estimated_monthly_revenue",
        "monthly_revenue",
        "revenue",
        "sales",
        "gmv",
        "monthly_sales",
    )
    for field_name in monthly_candidates:
        value = extract_money_amount(store.get(field_name))
        if value is not None:
            return value

    annual_candidates = (
        "estimated_annual_revenue",
        "annual_revenue",
        "organization_estimated_annual_revenue",
        "organization_annual_revenue",
        "estimated_revenue_range",
    )
    for field_name in annual_candidates:
        value = extract_money_amount(store.get(field_name))
        if value is not None:
            return value / 12.0

    return None


def format_money_exact(amount: float | None) -> str:
    if amount is None:
        return ""
    normalized_amount = int(round(max(amount, 0)))
    return f"${normalized_amount:,}"


def format_money_compact(amount: float | None) -> str:
    if amount is None:
        return ""

    normalized_amount = int(round(max(amount, 0)))
    if normalized_amount == 0:
        return "$0"
    if normalized_amount < 1_000:
        return f"${normalized_amount}"

    if normalized_amount < 10_000:
        bucket_size = 1_000
    elif normalized_amount < 100_000:
        bucket_size = 5_000
    elif normalized_amount < 1_000_000:
        bucket_size = 25_000
    elif normalized_amount < 10_000_000:
        bucket_size = 100_000
    else:
        bucket_size = 1_000_000

    bucketed_amount = max(bucket_size, (normalized_amount // bucket_size) * bucket_size)

    if bucketed_amount < 1_000_000:
        return f"${bucketed_amount // 1_000}K"

    millions = bucketed_amount / 1_000_000
    if millions.is_integer():
        return f"${int(millions)}M"
    return f"${millions:.1f}M"


def build_revenue_fields(store: dict[str, Any]) -> dict[str, str]:
    revenue = parse_monthly_sales(store)
    return {
        "revenue": format_money_exact(revenue),
        "estimated_revenue": format_money_compact(revenue),
    }
