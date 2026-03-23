"""Helium 10 CSV parsing helpers for the Amazon-first deck flow."""

from __future__ import annotations

import csv
import io
import math
import re
from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class XrayProduct:
    display_order: int
    title: str
    asin: str
    url: str
    image_url: str
    brand: str
    price: float | None
    price_label: str
    revenue: float | None
    revenue_label: str
    units_sold: float | None
    units_label: str
    bsr: float | None
    bsr_label: str
    rating: float | None
    rating_label: str
    review_count: int | None
    category: str
    seller_country: str
    size_tier: str
    fulfillment: str
    dimensions: str
    weight: str


@dataclass(frozen=True)
class KeywordInsight:
    phrase: str
    search_volume: int | None
    search_volume_label: str
    keyword_sales: int | None
    keyword_sales_label: str
    suggested_ppc_bid: float | None
    competing_products: int | None
    title_density: int | None
    competitor_rank_avg: float | None


@dataclass(frozen=True)
class DistributionSlice:
    label: str
    count: int
    share: float


@dataclass(frozen=True)
class Helium10XrayReport:
    products: list[XrayProduct]
    total_revenue: float
    total_units_sold: float
    average_bsr: float | None
    average_price: float | None
    average_rating: float | None
    search_results_count: int
    revenue_over_5000_count: int
    under_75_reviews_count: int
    seller_country_distribution: list[DistributionSlice]
    size_tier_distribution: list[DistributionSlice]
    fulfillment_distribution: list[DistributionSlice]
    warnings: list[str]

    def find_by_asin(self, asin: str) -> XrayProduct | None:
        normalized = asin.strip().upper()
        if not normalized:
            return None
        for product in self.products:
            if product.asin.upper() == normalized:
                return product
        return None


@dataclass(frozen=True)
class Helium10KeywordReport:
    keywords: list[KeywordInsight]
    total_search_volume: int
    average_search_volume: float | None
    top_search_volume: int | None
    average_competing_products: float | None
    average_title_density: float | None
    warnings: list[str]


def parse_xray_csv(content: bytes) -> Helium10XrayReport:
    decoded = content.decode("utf-8-sig").strip()
    if not decoded:
        raise RuntimeError("Competitor Xray CSV is empty.")

    reader = csv.DictReader(io.StringIO(decoded))
    headers = {str(header or "").strip().lower(): header for header in (reader.fieldnames or [])}
    required = {
        "product details",
        "asin",
        "url",
        "image url",
        "brand",
        "price  $",
        "asin revenue",
        "asin sales",
        "bsr",
        "ratings",
        "review count",
    }
    missing = sorted(field for field in required if field not in headers)
    if missing:
        raise RuntimeError(f"Competitor Xray CSV is missing columns: {', '.join(missing)}")

    products: list[XrayProduct] = []
    warnings: list[str] = []
    for index, row in enumerate(reader, start=1):
        title = _clean_text(row.get(headers["product details"], ""))
        asin = _clean_text(row.get(headers["asin"], "")).upper()
        if not title or not asin:
            continue
        products.append(
            XrayProduct(
                display_order=int(_parse_number(row.get(headers.get("display order", ""), "")) or index),
                title=title,
                asin=asin,
                url=_clean_text(row.get(headers["url"], "")),
                image_url=_clean_text(row.get(headers["image url"], "")),
                brand=_clean_text(row.get(headers["brand"], "")),
                price=_parse_number(row.get(headers["price  $"], "")),
                price_label=_label_money(row.get(headers["price  $"], "")),
                revenue=_parse_number(row.get(headers["asin revenue"], "")),
                revenue_label=_label_money(row.get(headers["asin revenue"], "")),
                units_sold=_parse_number(row.get(headers["asin sales"], "")),
                units_label=_clean_text(row.get(headers["asin sales"], "")),
                bsr=_parse_number(row.get(headers["bsr"], "")),
                bsr_label=_clean_text(row.get(headers["bsr"], "")),
                rating=_parse_number(row.get(headers["ratings"], "")),
                rating_label=_clean_text(row.get(headers["ratings"], "")),
                review_count=_parse_int(row.get(headers["review count"], "")),
                category=_clean_text(row.get(headers.get("category", ""), "")),
                seller_country=_clean_text(row.get(headers.get("seller country/region", ""), "")) or "N/A",
                size_tier=_clean_text(row.get(headers.get("size tier", ""), "")) or "Unknown",
                fulfillment=_clean_text(row.get(headers.get("fulfillment", ""), "")) or "Unknown",
                dimensions=_clean_text(row.get(headers.get("dimensions", ""), "")),
                weight=_clean_text(row.get(headers.get("weight", ""), "")),
            )
        )

    if not products:
        raise RuntimeError("Competitor Xray CSV did not contain any usable product rows.")

    total_revenue = sum(product.revenue or 0.0 for product in products)
    total_units = sum(product.units_sold or 0.0 for product in products)
    prices = [product.price for product in products if product.price is not None]
    bsrs = [product.bsr for product in products if product.bsr is not None]
    ratings = [product.rating for product in products if product.rating is not None]

    return Helium10XrayReport(
        products=sorted(products, key=lambda item: (-(item.revenue or 0.0), item.display_order, item.title.lower())),
        total_revenue=total_revenue,
        total_units_sold=total_units,
        average_bsr=_avg(bsrs),
        average_price=_avg(prices),
        average_rating=_avg(ratings),
        search_results_count=len(products),
        revenue_over_5000_count=sum(1 for product in products if (product.revenue or 0.0) >= 5000),
        under_75_reviews_count=sum(1 for product in products if (product.review_count or 0) < 75),
        seller_country_distribution=_build_distribution(product.seller_country for product in products),
        size_tier_distribution=_build_distribution(product.size_tier for product in products),
        fulfillment_distribution=_build_distribution(product.fulfillment for product in products),
        warnings=warnings,
    )


def parse_keyword_csv(content: bytes | None) -> Helium10KeywordReport | None:
    if content is None:
        return None
    decoded = content.decode("utf-8-sig").strip()
    if not decoded:
        return None

    reader = csv.DictReader(io.StringIO(decoded))
    headers = {str(header or "").strip().lower(): header for header in (reader.fieldnames or [])}
    required = {"keyword phrase", "search volume"}
    missing = sorted(field for field in required if field not in headers)
    if missing:
        raise RuntimeError(f"Keyword Xray CSV is missing columns: {', '.join(missing)}")

    keywords: list[KeywordInsight] = []
    for row in reader:
        phrase = _clean_text(row.get(headers["keyword phrase"], ""))
        if not phrase:
            continue
        keywords.append(
            KeywordInsight(
                phrase=phrase,
                search_volume=_parse_int(row.get(headers["search volume"], "")),
                search_volume_label=_clean_text(row.get(headers["search volume"], "")),
                keyword_sales=_parse_int(row.get(headers.get("keyword sales", ""), "")),
                keyword_sales_label=_clean_text(row.get(headers.get("keyword sales", ""), "")),
                suggested_ppc_bid=_parse_number(row.get(headers.get("suggested ppc bid", ""), "")),
                competing_products=_parse_int(row.get(headers.get("competing products", ""), "")),
                title_density=_parse_int(row.get(headers.get("title density", ""), "")),
                competitor_rank_avg=_parse_number(row.get(headers.get("competitor rank (avg)", ""), "")),
            )
        )

    if not keywords:
        return None

    sorted_keywords = sorted(keywords, key=lambda item: (-(item.search_volume or 0), item.phrase.lower()))
    volumes = [keyword.search_volume for keyword in sorted_keywords if keyword.search_volume is not None]
    competing = [keyword.competing_products for keyword in sorted_keywords if keyword.competing_products is not None]
    title_density = [keyword.title_density for keyword in sorted_keywords if keyword.title_density is not None]
    return Helium10KeywordReport(
        keywords=sorted_keywords,
        total_search_volume=sum(volumes),
        average_search_volume=_avg(volumes),
        top_search_volume=max(volumes) if volumes else None,
        average_competing_products=_avg(competing),
        average_title_density=_avg(title_density),
        warnings=[],
    )


def _build_distribution(values: Iterable[str]) -> list[DistributionSlice]:
    counts: dict[str, int] = {}
    total = 0
    for value in values:
        label = _clean_text(value) or "Unknown"
        counts[label] = counts.get(label, 0) + 1
        total += 1
    if total <= 0:
        return []
    ordered = sorted(counts.items(), key=lambda item: (-item[1], item[0].lower()))
    return [
        DistributionSlice(label=label, count=count, share=(count / total))
        for label, count in ordered
    ]


def _avg(values: Iterable[float]) -> float | None:
    materialized = [value for value in values if value is not None]
    if not materialized:
        return None
    return sum(materialized) / len(materialized)


def _parse_int(value: str) -> int | None:
    number = _parse_number(value)
    if number is None:
        return None
    try:
        return int(round(number))
    except Exception:
        return None


def _parse_number(value: str) -> float | None:
    cleaned = _clean_text(value)
    if not cleaned:
        return None
    cleaned = cleaned.replace("$", "").replace(",", "").replace("%", "").strip()
    multiplier = 1.0
    if cleaned.lower().endswith("k"):
        multiplier = 1000.0
        cleaned = cleaned[:-1]
    elif cleaned.lower().endswith("m"):
        multiplier = 1_000_000.0
        cleaned = cleaned[:-1]
    if not re.fullmatch(r"-?\d+(\.\d+)?", cleaned):
        return None
    try:
        return float(cleaned) * multiplier
    except ValueError:
        return None


def _label_money(value: str) -> str:
    number = _parse_number(value)
    if number is None:
        return _clean_text(value)
    if math.isfinite(number):
        return f"${number:,.2f}"
    return _clean_text(value)


def _clean_text(value: object) -> str:
    return str(value or "").strip()
