"""Pure formatting helpers — string normalization, label formatting, value coercion, chart-data transforms, default offer cards. No internal deck/* dependencies."""

from __future__ import annotations

import base64
import csv
import html
import io
import json
import mimetypes
import re
import secrets
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse
from typing import Any

import requests
from sqlalchemy import select
from sqlalchemy.orm import Session

from sales_support_agent.config import Settings
from sales_support_agent.integrations.amazon_sp_api import AmazonSpApiClient
from sales_support_agent.integrations.shopify import ShopifyStorefrontClient
from sales_support_agent.models.entities import AutomationRun
from sales_support_agent.services.audit import AuditService
from sales_support_agent.services.helium10 import (
    CerebroKeywordInsight,
    DistributionSlice,
    Helium10CerebroReport,
    Helium10KeywordReport,
    Helium10XrayReport,
    KeywordInsight,
    WordFrequencyReport,
    XrayProduct,
    parse_cerebro_csv,
    parse_keyword_csv,
    parse_keyword_csvs,
    parse_word_frequency_csv,
    parse_xray_csv,
    parse_xray_csvs,
)
from sales_support_agent.services.product_research import EnrichedHeroProduct, ProductResearchService



DEFAULT_CUSTOM_OFFERS: tuple[dict[str, str], ...] = (
    {
        "title": "Channel management",
        "description": "Full-service Amazon marketing and operations support, including graphic designers, advertising management, and more.",
        "price": "$3,000",
        "price_label": "Monthly retainer fee",
        "commission": "5%",
        "commission_label": "Commission on growth",
        "baseline": "$10,000",
        "baseline_label": "Commission baseline",
        "bonus": "+TikTok Shop Support",
    },
    {
        "title": "Commission Model + Shipping OS",
        "description": "A performance-based growth model that aligns marketing, inventory, and fulfillment under one operating system - ensuring every dollar of demand can be fulfilled profitably.",
        "price": "$0",
        "price_label": "Monthly retainer fee",
        "commission": "10%",
        "commission_label": "Commission over baseline",
        "baseline": "$TBD",
        "baseline_label": "Commission baseline",
        "bonus": "Shipping OS | Required (* Order Min.)",
    },
)
DEFAULT_CASE_STUDY_URL = (
    "https://www.canva.com/design/DAHEy6FPsSw/NDWUOqpKXYu4YPxNWfieUg/view"
    "?utm_content=DAHEy6FPsSw&utm_campaign=designshare&utm_medium=embeds&utm_source=link"
)
DEFAULT_SERVICE_TABS: tuple[str, ...] = ("amazon", "tiktok_shop", "shopify", "3pl", "shipping_os")
COPY_TERM_STOP_WORDS: frozenset[str] = frozenset(
    {
        "a",
        "an",
        "and",
        "by",
        "for",
        "from",
        "in",
        "of",
        "on",
        "or",
        "the",
        "to",
        "with",
    }
)
def _normalize_channels(channels: list[str]) -> list[str]:
    allowed = {
        "amazon": "amazon",
        "shopify": "shopify",
        "tiktok_shop": "tiktok_shop",
        "tiktok": "tiktok_shop",
        "3pl": "3pl",
        "shipping_os": "shipping_os",
        "shipping-os": "shipping_os",
    }
    normalized: list[str] = []
    seen: set[str] = set()
    for value in channels:
        key = _normalize_key(str(value or ""))
        mapped = allowed.get(key)
        if not mapped or mapped in seen:
            continue
        seen.add(mapped)
        normalized.append(mapped)
    if normalized:
        return normalized
    return list(DEFAULT_SERVICE_TABS)
def _target_reference_label(target: dict[str, Any]) -> str:
    source_url = str(target.get("source_url") or "").strip()
    if source_url:
        parsed = urlparse(source_url)
        host = (parsed.netloc or "").strip()
        if host:
            return host
        return "Prospect URL"
    title = str(target.get("title") or "").strip()
    if title:
        return _trim_text(title, 36)
    return "Prospect product"
def _looks_like_raw_asin_label(value: str) -> bool:
    cleaned = str(value or "").strip()
    return bool(re.fullmatch(r"(ASIN\s+)?[A-Z0-9]{10}", cleaned, flags=re.IGNORECASE))
def _preferred_target_title(*candidates: str) -> str:
    cleaned_candidates = [str(value or "").strip() for value in candidates if str(value or "").strip()]
    for candidate in cleaned_candidates:
        if not _looks_like_raw_asin_label(candidate):
            return candidate
    return cleaned_candidates[0] if cleaned_candidates else "Prospect product"
def _is_generic_brand_name(value: str) -> bool:
    cleaned = _normalize_key(str(value or ""))
    return cleaned in {"amazon", "amazon_brand", "brand", "prospect_brand"}
def _preferred_brand_name(*candidates: str) -> str:
    cleaned_candidates = [str(value or "").strip() for value in candidates if str(value or "").strip()]
    for candidate in cleaned_candidates:
        if not _is_generic_brand_name(candidate):
            return candidate
    return cleaned_candidates[0] if cleaned_candidates else "Prospect brand"
def _infer_brand_from_title(title: str) -> str:
    cleaned = _clean_listing_title(str(title or ""))
    if not cleaned:
        return ""
    token = cleaned.split()[0]
    token = re.sub(r"[^A-Za-z0-9&'-]", "", token).strip()
    if not token or len(token) <= 1:
        return ""
    return token
def _slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().lower()).strip("-")
def _normalize_product_url(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    parsed = urlparse(raw if "://" in raw else f"https://{raw}")
    host = re.sub(r"^www\.", "", (parsed.netloc or "").lower())
    path = (parsed.path or "").rstrip("/").lower()
    if "amazon." in host:
        asin_match = re.search(r"/(?:dp|gp/product)/([a-z0-9]{10})", path, flags=re.IGNORECASE)
        if asin_match:
            return f"amazon:{asin_match.group(1).upper()}"
    return f"{host}{path}"
def _title_token_set(value: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-z0-9]+", _clean_listing_title(value).lower())
        if len(token) > 2 and token not in {"with", "from", "pack", "product", "amazon", "brand"}
    }
def _variant_tokens(value: str) -> set[str]:
    return {
        token
        for token in re.findall(r"(?:\d+(?:\.\d+)?(?:oz|ml|lb|ct|pack|pk|count|inch|in)|\d+)", _clean_listing_title(value).lower())
        if token
    }
def _build_competitor_strength(product: XrayProduct) -> str:
    return f"{product.title} converts enough demand to produce {product.revenue_label} in 30-day revenue with {product.bsr_label} BSR."
def _build_competitor_gap(product: XrayProduct, target_row: XrayProduct | None) -> str:
    if (product.review_count or 0) < 75:
        return "This listing is still winning with a relatively thin review moat, which makes it a useful target for differentiation."
    if target_row and product.price and target_row.price and product.price > target_row.price:
        return "The price anchor is higher than the target listing, which creates room for a sharper value story."
    return "Use this listing as a benchmark for claim clarity, review depth, and imagery sequence."
def _build_offer_cards(offers: list[str]) -> list[dict[str, Any]]:
    catalog = {
        "channel_management": {
            **DEFAULT_CUSTOM_OFFERS[0],
        },
        "commission_model_shipping_os": {
            **DEFAULT_CUSTOM_OFFERS[1],
        },
    }
    return [catalog[key] for key in offers if key in catalog]
def _extract_first(content: str, *patterns: str) -> str:
    for pattern in patterns:
        match = re.search(pattern, content, flags=re.IGNORECASE | re.DOTALL)
        if match:
            return str(match.group(1) or "").strip()
    return ""
def _clean_scraped_text(value: str) -> str:
    cleaned = html.unescape(str(value or ""))
    cleaned = re.sub(r"<!--.*?-->", " ", cleaned, flags=re.DOTALL)
    cleaned = re.sub(r"<script[^>]*>.*?</script>", " ", cleaned, flags=re.IGNORECASE | re.DOTALL)
    cleaned = re.sub(r"<style[^>]*>.*?</style>", " ", cleaned, flags=re.IGNORECASE | re.DOTALL)
    cleaned = re.sub(r"<[^>]+>", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned
def _format_metric_delta(
    target_value: float | None,
    benchmark_value: float | None,
    *,
    inverse: bool,
    unit: str = "number",
) -> str:
    """
    Format a target-vs-benchmark delta as a human-readable string.

    `unit` controls how the absolute delta is rendered:
    - "currency": prefixed with `$` and 0 decimal places when >= $1000, else 2 dp
    - "percent":  suffixed with `%`, 1 decimal place
    - "float":    1 decimal place (e.g. ratings)
    - "number":   integer with thousand separators (default; for BSR, review counts,
                  and other integer-only metrics — no spurious decimals)
    """
    if target_value is None or benchmark_value is None:
        return "Benchmark only"
    delta = target_value - benchmark_value
    if inverse:
        delta *= -1
    if abs(delta) < 0.01:
        return "In line"
    sign = "+" if delta > 0 else "-"
    abs_delta = abs(delta)

    if unit == "currency":
        if abs_delta >= 1000:
            magnitude = f"${abs_delta:,.0f}"
        else:
            magnitude = f"${abs_delta:,.2f}"
    elif unit == "percent":
        magnitude = f"{abs_delta:,.1f}%"
    elif unit == "float":
        magnitude = f"{abs_delta:,.1f}"
    else:  # "number" → always integer, no decimals
        magnitude = f"{abs_delta:,.0f}"

    return f"{sign}{magnitude} vs best seller"
def _bounded_ratio(value: float, ceiling: float) -> float:
    if ceiling <= 0:
        return 0.0
    return max(0.0, min(value / ceiling, 1.0))
def _inverse_bounded_ratio(value: float, ceiling: float) -> float:
    if ceiling <= 0:
        return 0.0
    return max(0.0, min(1.0 - (min(value, ceiling) / ceiling), 1.0))
def _label_integer(value: float | int | None) -> str:
    if value is None:
        return "n/a"
    return f"{int(round(value)):,}"
def _label_float(value: float | None, digits: int) -> str:
    if value is None:
        return "n/a"
    return f"{value:,.{digits}f}"
def _label_money_value(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"${value:,.2f}"
def _label_share(value: float | None, total: float) -> str:
    if value is None or total <= 0:
        return "n/a"
    return f"{((value / total) * 100):.1f}%"
def _format_metric_display(value: str) -> str:
    cleaned = str(value or "").strip()
    if re.fullmatch(r"\d{4,}", cleaned):
        try:
            return f"{int(cleaned):,}"
        except ValueError:
            return cleaned
    if re.fullmatch(r"\d{4,}\.0+", cleaned):
        try:
            return f"{int(float(cleaned)):,}"
        except ValueError:
            return cleaned
    if re.fullmatch(r"\$?\d{4,}(?:\.\d+)?", cleaned):
        prefix = "$" if cleaned.startswith("$") else ""
        numeric = cleaned.lstrip("$")
        try:
            value_number = float(numeric)
        except ValueError:
            return cleaned
        if value_number.is_integer():
            return f"{prefix}{int(value_number):,}"
        return f"{prefix}{value_number:,.2f}"
    return cleaned
def _coverage_terms(value: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-z0-9]+", value.lower())
        if len(token) > 2 and token not in {"with", "from", "that", "this", "your", "into", "pack", "product"}
    }
def _rank_keyword_terms(phrases: list[str]) -> list[str]:
    counts: dict[str, int] = {}
    for phrase in phrases:
        seen: set[str] = set()
        for token in _coverage_terms(phrase):
            if token in seen:
                continue
            counts[token] = counts.get(token, 0) + 1
            seen.add(token)
    ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return [term for term, _ in ranked[:8]]
def _format_channel_label(value: str) -> str:
    mapping = {
        "amazon": "Amazon",
        "shopify": "Shopify",
        "tiktok_shop": "TikTok Shop",
        "3pl": "3PL",
        "shipping_os": "Shipping OS",
    }
    key = _normalize_key(value)
    return mapping.get(key, value.replace("_", " ").title())
def _format_display_date(value: date | datetime | str | None) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).strftime("%m/%d/%Y")
    if isinstance(value, date):
        return value.strftime("%m/%d/%Y")
    raw = str(value).strip()
    if not raw:
        return ""
    try:
        if "T" in raw:
            return datetime.fromisoformat(raw.replace("Z", "+00:00")).strftime("%m/%d/%Y")
        return date.fromisoformat(raw).strftime("%m/%d/%Y")
    except ValueError:
        return raw
def _trim_text(value: str, limit: int) -> str:
    cleaned = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: max(limit - 1, 1)].rstrip() + "…"
def _clean_listing_title(value: str) -> str:
    cleaned = re.sub(r"<!--.*?-->", " ", str(value or ""), flags=re.DOTALL)
    cleaned = re.sub(r"<[^>]+>", " ", cleaned)
    cleaned = html.unescape(cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned
def _brand_possessive(brand_name: str) -> str:
    cleaned = _trim_text(re.sub(r"\s+", " ", str(brand_name or "")).strip(), 40)
    if not cleaned:
        return "The prospect's"
    if cleaned.lower().endswith("s"):
        return f"{cleaned}'"
    return f"{cleaned}'s"
def _brand_product_reference(brand_name: str) -> str:
    return f"{_brand_possessive(brand_name)} product"
def _normalize_offer_text(value: Any, default: str = "") -> str:
    cleaned = re.sub(r"\s+", " ", str(value or default)).strip()
    return cleaned
def _default_offer_cards() -> list[dict[str, str]]:
    return [dict(card) for card in DEFAULT_CUSTOM_OFFERS]
def _normalize_custom_offer_cards(*, offer_payload_json: str, offers: list[str]) -> list[dict[str, str]]:
    payload = str(offer_payload_json or "").strip()
    if payload:
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError:
            parsed = []
        cards: list[dict[str, str]] = []
        for raw_card in parsed if isinstance(parsed, list) else []:
            if not isinstance(raw_card, dict):
                continue
            enabled = bool(raw_card.get("enabled", True))
            if not enabled:
                continue
            title = _normalize_offer_text(raw_card.get("title"))
            description = _normalize_offer_text(raw_card.get("description"))
            if not title and not description:
                continue
            cards.append(
                {
                    "title": title or "Recommended offer",
                    "description": description,
                    "price": _normalize_offer_text(raw_card.get("price"), "$TBD"),
                    "price_label": _normalize_offer_text(raw_card.get("price_label"), "Monthly retainer fee"),
                    "commission": _normalize_offer_text(raw_card.get("commission"), "TBD"),
                    "commission_label": _normalize_offer_text(raw_card.get("commission_label"), "Commission"),
                    "baseline": _normalize_offer_text(raw_card.get("baseline"), "TBD"),
                    "baseline_label": _normalize_offer_text(raw_card.get("baseline_label"), "Baseline"),
                    "bonus": _normalize_offer_text(raw_card.get("bonus")),
                }
            )
        if cards:
            return cards
    fallback = _build_offer_cards(offers)
    return fallback or _default_offer_cards()
def _extract_listing_copy_points(description: str, *, limit: int = 4) -> list[str]:
    cleaned = _clean_listing_title(description)
    cleaned = re.sub(r"(?i)^about this item[:\s-]*", "", cleaned)
    cleaned = re.sub(r"(?<=[^A-Z ][ ])(?=[A-Z]{2,}(?: [A-Z]{2,})*:)", "\n", cleaned)
    parts = re.split(r"\n+|[•●▪]+", cleaned)
    bullets: list[str] = []
    for part in parts:
        fragment = part.strip(" -;")
        if not fragment:
            continue
        if ":" in fragment:
            prefix, remainder = fragment.split(":", 1)
            if prefix.isupper() and len(prefix) > 4:
                fragment = f"{prefix.title()}: {remainder.strip()}"
        fragment = _trim_text(fragment, 170)
        if fragment not in bullets:
            bullets.append(fragment)
        if len(bullets) >= limit:
            break
    return bullets
def _build_price_comparison_summary(*, hero_price: str, market_average_price: float, best_seller_price: str) -> str:
    hero_value = _coerce_number(hero_price or "")
    if hero_value is None:
        return ""
    benchmark_bits: list[str] = []
    if market_average_price and market_average_price > 0:
        delta = ((hero_value - market_average_price) / market_average_price) * 100
        direction = "above" if delta > 0 else "below"
        if abs(delta) < 5:
            benchmark_bits.append(f"roughly in line with the market average at {_label_money_value(market_average_price)}")
        else:
            benchmark_bits.append(f"{abs(delta):.0f}% {direction} the market average of {_label_money_value(market_average_price)}")
    best_value = _coerce_number(best_seller_price or "")
    if best_value is not None:
        benchmark_bits.append(f"against the best seller at {_label_money_value(best_value)}")
    if not benchmark_bits:
        return ""
    return f"The current price point is {hero_price} and compares {' '.join(benchmark_bits)}."
def _normalize_offers(values: list[str]) -> list[str]:
    allowed = {
        "channel_management": "channel_management",
        "commission_model_shipping_os": "commission_model_shipping_os",
    }
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        key = allowed.get(_normalize_key(value))
        if key and key not in seen:
            normalized.append(key)
            seen.add(key)
    if not normalized:
        normalized.append("channel_management")
    return normalized
def _normalize_key(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", (value or "").strip().lower()).strip("_")
    return normalized
def _normalize_competitor_inputs(values: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_value in values:
        for fragment in re.split(r"[\n,]+", str(raw_value or "")):
            cleaned = fragment.strip()
            if not cleaned:
                continue
            lowered = cleaned.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            normalized.append(cleaned)
    return normalized
def _titleize_slug(value: str) -> str:
    cleaned = re.sub(r"[_\-]+", " ", str(value or "").strip())
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if not cleaned:
        return ""
    return " ".join(token.capitalize() for token in cleaned.split(" "))
def _normalize_sales_rows(values: list[list[str]]) -> tuple[list[list[str]], dict[str, str]]:
    scalar_fields: dict[str, str] = {}
    first_row = values[0]
    if len(first_row) == 2 and all(len(row) >= 2 for row in values):
        normalized_rows = [["Metric", "Value"]]
        for source_index, row in enumerate(values, start=1):
            label = str(row[0] or "").strip()
            value = str(row[1] or "").strip()
            if source_index == 1 and _normalize_key(label) in {"metric", "label", "name"} and _normalize_key(value) in {"value", "amount"}:
                continue
            normalized_rows.append([label, value])
            if label:
                scalar_fields[f"sales_{_normalize_key(label)}"] = value
        return normalized_rows, scalar_fields

    headers = [_normalize_key(cell) or f"column_{index + 1}" for index, cell in enumerate(first_row)]
    normalized_rows = [[str(cell or "").strip() for cell in first_row]]
    for row_index, row in enumerate(values[1:], start=1):
        padded = [str(row[column_index] or "").strip() if column_index < len(row) else "" for column_index in range(len(headers))]
        normalized_rows.append(padded)
        for header, value in zip(headers, padded):
            scalar_fields[f"sales_row_{row_index}_{header}"] = value
        if row_index == 1:
            for header, value in zip(headers, padded):
                scalar_fields[f"sales_{header}"] = value
    return normalized_rows, scalar_fields
def _build_chart_data(rows: list[list[str]]) -> dict[str, Any]:
    clipped_rows = rows[:100]
    if clipped_rows:
        clipped_rows = [row[:20] for row in clipped_rows]
    return {
        "rows": [
            {
                "cells": [_build_chart_cell(cell, is_header=row_index == 0) for cell in row]
            }
            for row_index, row in enumerate(clipped_rows)
        ]
    }
def _build_top_products_by_bsr_rows(rows: list[list[str]]) -> list[list[str]] | None:
    if len(rows) < 2:
        return None

    header_row = rows[0]
    normalized_headers = [_normalize_key(cell) for cell in header_row]

    def _find_index(*candidates: str) -> int | None:
        for candidate in candidates:
            if candidate in normalized_headers:
                return normalized_headers.index(candidate)
        return None

    product_idx = _find_index("product_name", "product", "title", "item_name", "name")
    bsr_idx = _find_index("bsr", "best_seller_rank", "bestseller_rank", "sales_rank")
    sales_idx = _find_index("sales", "revenue", "sales_total", "sales_amount")
    units_idx = _find_index("units", "unit_sales", "ordered_units", "qty", "quantity")
    change_idx = _find_index(
        "change_from_previous_period",
        "change_vs_previous_period",
        "previous_period_change",
        "period_change",
        "sales_change",
        "mom_change",
        "change",
    )

    required_indexes = (product_idx, bsr_idx, sales_idx, units_idx, change_idx)
    if any(index is None for index in required_indexes):
        return None

    ranked_rows: list[tuple[float, list[str]]] = []
    for row in rows[1:]:
        padded = [str(cell or "").strip() for cell in row]
        max_index = max(index for index in required_indexes if index is not None)
        if len(padded) <= max_index:
            continue
        bsr_value = _coerce_number(padded[bsr_idx]) if bsr_idx is not None else None
        if bsr_value is None:
            continue
        ranked_rows.append(
            (
                bsr_value,
                [
                    padded[product_idx] if product_idx is not None else "",
                    padded[bsr_idx] if bsr_idx is not None else "",
                    padded[sales_idx] if sales_idx is not None else "",
                    padded[units_idx] if units_idx is not None else "",
                    padded[change_idx] if change_idx is not None else "",
                ],
            )
        )

    if not ranked_rows:
        return None

    ranked_rows.sort(key=lambda item: (item[0], item[1][0].lower()))
    top_rows = [["Product name", "BSR", "Sales", "Units", "Change from previous period"]]
    top_rows.extend(values for _, values in ranked_rows[:10])
    return top_rows
def _build_chart_cell(value: str, *, is_header: bool) -> dict[str, Any]:
    cleaned = str(value or "").strip()
    if is_header:
        return {"type": "string", "value": cleaned}
    lowered = cleaned.lower()
    if lowered in {"true", "false", "yes", "no"}:
        return {"type": "boolean", "value": lowered in {"true", "yes"}}
    number_value = _coerce_number(cleaned)
    if number_value is not None:
        return {"type": "number", "value": number_value}
    timestamp_value = _coerce_date_timestamp(cleaned)
    if timestamp_value is not None:
        return {"type": "date", "value": timestamp_value}
    return {"type": "string", "value": cleaned}
def _coerce_number(value: str) -> float | None:
    cleaned = value.replace(",", "").replace("$", "").replace("%", "").strip()
    if not cleaned:
        return None
    multiplier = 1.0
    if cleaned.lower().endswith("k"):
        multiplier = 1000.0
        cleaned = cleaned[:-1]
    elif cleaned.lower().endswith("m"):
        multiplier = 1_000_000.0
        cleaned = cleaned[:-1]
    if not re.fullmatch(r"-?\d+(\.\d+)?", cleaned):
        return None
    return float(cleaned) * multiplier
def _coerce_date_timestamp(value: str) -> int | None:
    if not value:
        return None
    for pattern in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return int(datetime.strptime(value, pattern).replace(tzinfo=timezone.utc).timestamp())
        except ValueError:
            continue
    return None
