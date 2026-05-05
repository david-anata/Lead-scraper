"""Deck dataset assembly — _build_amazon_first_dataset and helpers that compose the payload the renderer consumes."""

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

from sales_support_agent.services.deck.formatting import (  # noqa: F401
    COPY_TERM_STOP_WORDS,
    _brand_product_reference,
    _build_price_comparison_summary,
    _clean_listing_title,
    _coverage_terms,
    _label_float,
    _label_integer,
    _label_money_value,
    _normalize_key,
    _normalize_product_url,
    _rank_keyword_terms,
    _title_token_set,
    _trim_text,
    _variant_tokens,
)


@dataclass(frozen=True)
class DeckDataset:
    text_fields: dict[str, str]
    chart_fields: dict[str, dict[str, Any]]
    warnings: list[str]
    sales_row_count: int
    competitor_row_count: int
    deck_payload: dict[str, Any]
@dataclass(frozen=True)
class DeckGenerationResult:
    run_id: int
    status: str
    message: str
    output_type: str
    design_id: str
    design_title: str
    edit_url: str
    view_url: str
    warnings: list[str]
    sales_row_count: int
    competitor_row_count: int
    template_fields: int
@dataclass(frozen=True)
class TargetRowMatch:
    product: XrayProduct | None
    provenance: str
    confidence: int
def _find_target_row(
    xray_report: Helium10XrayReport,
    *,
    asin: str,
    candidate_asin: str,
    source_url: str,
    title: str,
    brand_name: str,
) -> TargetRowMatch:
    if asin:
        exact = xray_report.find_by_asin(asin)
        if exact:
            return TargetRowMatch(product=exact, provenance="verified_asin", confidence=100)

    normalized_url = _normalize_product_url(source_url)
    normalized_title = _normalize_key(_clean_listing_title(title))
    normalized_brand = _normalize_key(brand_name)
    title_tokens = _title_token_set(title)
    best_match: TargetRowMatch | None = None
    best_score = 0

    if candidate_asin:
        candidate = xray_report.find_by_asin(candidate_asin)
        if candidate:
            candidate_score = _score_target_match(
                candidate,
                normalized_url=normalized_url,
                normalized_title=normalized_title,
                normalized_brand=normalized_brand,
                title_tokens=title_tokens,
            )
            if candidate_score >= 75:
                return TargetRowMatch(product=candidate, provenance="candidate_asin_verified", confidence=candidate_score)

    for product in xray_report.products:
        score = _score_target_match(
            product,
            normalized_url=normalized_url,
            normalized_title=normalized_title,
            normalized_brand=normalized_brand,
            title_tokens=title_tokens,
        )
        if score > best_score:
            best_score = score
            best_match = TargetRowMatch(product=product, provenance="identity_match", confidence=score)

    return best_match if best_match and best_score >= 75 else TargetRowMatch(product=None, provenance="", confidence=0)
def _score_target_match(
    product: XrayProduct,
    *,
    normalized_url: str,
    normalized_title: str,
    normalized_brand: str,
    title_tokens: set[str],
) -> int:
    score = 0
    product_title = _normalize_key(_clean_listing_title(product.title))
    product_brand = _normalize_key(product.brand)
    product_tokens = _title_token_set(product.title)
    input_variants = _variant_tokens(normalized_title)
    product_variants = _variant_tokens(product.title)
    if normalized_url and normalized_url == _normalize_product_url(product.url):
        score += 120
    if normalized_title and normalized_title == product_title:
        score += 100
    elif normalized_title and product_title and (normalized_title in product_title or product_title in normalized_title):
        score += 55
    if normalized_brand and normalized_brand == product_brand:
        score += 30
    if title_tokens and product_tokens:
        overlap = len(title_tokens & product_tokens)
        if overlap:
            score += overlap * 6
        overlap_ratio = overlap / max(1, min(len(title_tokens), len(product_tokens)))
        if overlap_ratio >= 0.8:
            score += 30
        elif overlap_ratio >= 0.6:
            score += 18
    if input_variants != product_variants and (input_variants or product_variants):
        score -= 35
    return score
def _resolve_target_state(source_type: str, *, target_row: XrayProduct | None, hero_product: EnrichedHeroProduct) -> str:
    if target_row:
        return "matched_market"
    if source_type == "amazon" or hero_product.asin:
        return "live_unmatched"
    if hero_product.source_url:
        return "concept_only"
    return "concept_only"
def _build_target_snapshot_text(
    target_title: str,
    brand_name: str,
    target_row: XrayProduct | None,
    *,
    comparison_mode: str,
    hero_product: EnrichedHeroProduct,
) -> str:
    if comparison_mode == "matched_market" and target_row:
        revenue_text = target_row.revenue_label if target_row.revenue_label and target_row.revenue_label != "n/a" else ""
        bsr_text = target_row.bsr_label if target_row.bsr_label and target_row.bsr_label != "n/a" else ""
        metrics_clause = ""
        if revenue_text or bsr_text:
            metrics_parts = [part for part in (f"{revenue_text} in revenue" if revenue_text else "", f"{bsr_text} BSR" if bsr_text else "") if part]
            metrics_clause = f" In the current niche export it shows {' and '.join(metrics_parts)}."
        return f"{_brand_product_reference(brand_name)} is the benchmark listing for this prospect.{metrics_clause}"
    if comparison_mode == "live_unmatched":
        bsr_text = _label_float(hero_product.bsr, 0) if hero_product.bsr is not None else ""
        rating_text = _label_float(hero_product.rating, 1) if hero_product.rating is not None else ""
        review_text = _label_integer(hero_product.review_count) if hero_product.review_count is not None else ""
        details = []
        if bsr_text and bsr_text != "n/a":
            details.append(f"{bsr_text} BSR")
        if rating_text and rating_text != "n/a":
            details.append(f"{rating_text} rating")
        if review_text and review_text != "n/a":
            details.append(f"{review_text} reviews")
        metrics_clause = f" Current direct target data shows {' / '.join(details)}." if details else ""
        return (
            f"{_brand_product_reference(brand_name)} is live, but it was not matched to the current niche export.{metrics_clause} "
            "Use this deck to compare the PDP and offer against page-one leaders while the market benchmark is refined."
        )
    return (
        f"{_brand_product_reference(brand_name)} is not yet established in the current market set. "
        "Use this deck to benchmark the product concept against niche leaders and shape the launch-ready PDP, offer, and positioning."
    )
def _build_market_summary(
    brand_name: str,
    xray_report: Helium10XrayReport,
    keyword_report: Helium10KeywordReport | None,
) -> str:
    lead_keyword = keyword_report.keywords[0].phrase if keyword_report and keyword_report.keywords else "the niche"
    search_volume = keyword_report.top_search_volume if keyword_report and keyword_report.top_search_volume else None
    search_text = f" The leading search term in the current dataset is {lead_keyword} with {search_volume:,} monthly searches." if search_volume else ""
    return (
        f"The current {lead_keyword} market shows {xray_report.search_results_count} comparable listings and "
        f"{_label_money_value(xray_report.total_revenue)} in 30-day competitor revenue across the visible market set."
        f"{search_text}"
    )
def _build_executive_summary(
    target_title: str,
    brand_name: str,
    xray_report: Helium10XrayReport,
    keyword_report: Helium10KeywordReport | None,
) -> str:
    keyword_text = ""
    if keyword_report and keyword_report.keywords:
        keyword_text = f" The leading search objective in the dataset is {keyword_report.keywords[0].phrase}."
    return (
        f"This deck benchmarks {_brand_product_reference(brand_name)} against the live market set and translates the data into an offer, PDP, SEO, and service plan for {brand_name}."
        f"{keyword_text}"
    )
def _build_advertising_summary(xray_report: Helium10XrayReport, keyword_report: Helium10KeywordReport | None) -> str:
    top_keyword = keyword_report.keywords[0].phrase if keyword_report and keyword_report.keywords else "the primary search terms"
    return (
        f"Advertising should follow listing cleanup. Once the PDP is aligned, lean into {top_keyword} and the adjacent high-volume terms while exploiting low-review competitors in the category."
    )
def _build_plan_summary(offer_cards: list[dict[str, str]], channels: list[str]) -> str:
    if offer_cards:
        return " / ".join(str(card.get("title", "")).strip() for card in offer_cards if str(card.get("title", "")).strip())
    services = []
    if "amazon" in channels:
        services.append("fix the Amazon PDP, imagery, and search coverage")
    if "shopify" in channels:
        services.append("align the Shopify storefront and conversion flow")
    if "tiktok_shop" in channels:
        services.append("package the TikTok Shop offer and creative")
    return "Phase 1: " + "; ".join(services[:3]) + ". Phase 2: launch the first measurement sprint and tune against live market response."
def _build_expected_impact_summary(xray_report: Helium10XrayReport) -> str:
    return (
        f"The niche is large enough to justify a tighter positioning and conversion sprint. {xray_report.under_75_reviews_count} listings are still competing with under 75 reviews, which leaves room for differentiated creative and offer design."
    )
def _build_why_anata_summary(channels: list[str]) -> str:
    scope = []
    if "amazon" in channels:
        scope.append("Amazon growth execution")
    if "shopify" in channels:
        scope.append("Shopify conversion systems")
    if "tiktok_shop" in channels:
        scope.append("TikTok Shop go-to-market support")
    if "3pl" in channels:
        scope.append("3PL operations")
    if "shipping_os" in channels:
        scope.append("Shipping OS")
    return "Anata can own " + ", ".join(scope) + " without splitting CRO, creative, fulfillment, and acquisition across separate vendors."
def _build_market_metric_cards(
    xray_report: Helium10XrayReport,
    keyword_report: Helium10KeywordReport | None,
) -> list[dict[str, str]]:
    average_revenue_per_listing = (xray_report.total_revenue / xray_report.search_results_count) if xray_report.search_results_count else 0.0
    average_units_per_listing = (xray_report.total_units_sold / xray_report.search_results_count) if xray_report.search_results_count else 0.0
    return [
        {
            "label": "30-day revenue",
            "value": _label_money_value(xray_report.total_revenue),
            "meta": f"Avg per listing { _label_money_value(average_revenue_per_listing) }",
        },
        {
            "label": "30-day units sold",
            "value": _label_integer(xray_report.total_units_sold),
            "meta": f"Avg per listing { _label_integer(average_units_per_listing) }",
        },
        {"label": "Average BSR", "value": _label_float(xray_report.average_bsr, 0), "meta": "Lower is stronger"},
        {"label": "Average price", "value": _label_money_value(xray_report.average_price or 0.0), "meta": "From the current market set"},
        {"label": "Average rating", "value": _label_float(xray_report.average_rating, 1), "meta": "Competitive review signal"},
        {
            "label": "Open opportunity",
            "value": f"{xray_report.under_75_reviews_count}/{xray_report.search_results_count}",
            "meta": f"{xray_report.revenue_over_5000_count} listings clear $5k revenue while {xray_report.under_75_reviews_count} stay under 75 reviews.",
        },
    ]
def _build_keyword_metric_cards(keyword_report: Helium10KeywordReport | None) -> list[dict[str, str]]:
    if keyword_report is None:
        return []
    return [
        {"label": "Keywords parsed", "value": str(len(keyword_report.keywords)), "meta": "Rows loaded from the current keyword dataset"},
        {"label": "Total search volume", "value": _label_integer(keyword_report.total_search_volume), "meta": "Summed across the current keyword dataset"},
        {"label": "Average competing products", "value": _label_float(keyword_report.average_competing_products, 0), "meta": "Competitive density"},
        {"label": "Average title density", "value": _label_float(keyword_report.average_title_density, 0), "meta": "How crowded the SERP language is"},
    ]
def _build_cerebro_metric_cards(
    cerebro_report: Helium10CerebroReport | None,
    word_frequency_report: WordFrequencyReport | None,
) -> list[dict[str, str]]:
    cards: list[dict[str, str]] = []
    if cerebro_report:
        cards.extend(
            [
                {
                    "label": "Top-20 impression proxy",
                    "value": _label_integer(cerebro_report.impression_proxy),
                    "meta": "Summed search volume where the target ranks 1-20 in the Cerebro set",
                },
                {
                    "label": "Tracked ranking keywords",
                    "value": _label_integer(cerebro_report.top_20_ranked_keywords),
                    "meta": "Keywords where the target currently ranks inside positions 1-20",
                },
                {
                    "label": "Competitor overlays",
                    "value": _label_integer(len(cerebro_report.competitor_asins)),
                    "meta": "ASIN columns compared against the target listing",
                },
            ]
        )
    if word_frequency_report:
        cards.append(
            {
                "label": "Support terms parsed",
                "value": _label_integer(len(word_frequency_report.words)),
                "meta": "Individual terms available for bullet and copy coverage checks",
            }
        )
    return cards
def _build_keyword_rows(
    keyword_report: Helium10KeywordReport | None,
    cerebro_report: Helium10CerebroReport | None,
) -> list[list[str]]:
    if cerebro_report and cerebro_report.keywords:
        rows: list[list[str]] = [["Keyword", "Search volume", "Keyword sales", "Target rank", "Impression proxy"]]
        for keyword in cerebro_report.keywords[:10]:
            rows.append(
                [
                    keyword.phrase,
                    _label_integer(keyword.search_volume),
                    _label_integer(keyword.keyword_sales),
                    _label_integer(keyword.target_rank),
                    _label_integer(keyword.target_impression_proxy),
                ]
            )
        return rows
    rows = [["Keyword", "Search volume", "Sales", "Competing products", "Title density"]]
    if keyword_report:
        for keyword in keyword_report.keywords[:10]:
            rows.append(
                [
                    keyword.phrase,
                    keyword.search_volume_label,
                    keyword.keyword_sales_label,
                    str(keyword.competing_products or ""),
                    str(keyword.title_density or ""),
                ]
            )
    return rows
def _build_seo_recommendations(
    keyword_report: Helium10KeywordReport | None,
    cerebro_report: Helium10CerebroReport | None,
    xray_report: Helium10XrayReport,
    search_insights: dict[str, list[str]],
    *,
    brand_name: str,
    target_title: str,
) -> list[str]:
    branded_reference = _brand_product_reference(brand_name)
    cleaned_title = _trim_text(_clean_listing_title(target_title), 60) or branded_reference
    title_misses = list(search_insights.get("title_misses", []))
    copy_misses = list(search_insights.get("copy_misses", []))
    if keyword_report and keyword_report.keywords:
        keyword_phrases = [keyword.phrase.strip() for keyword in keyword_report.keywords[:10] if keyword.phrase.strip()]
        title_misses = [phrase for phrase in keyword_phrases if phrase.lower() not in cleaned_title.lower()]
        copy_misses = [target["label"] for target in _build_copy_targets(keyword_phrases, keyword_report=keyword_report, word_frequency_report=None)]
    recommendations = [
        f"Rewrite the title and first bullets around the highest-intent keyword cluster instead of broad category language. Current title direction: '{cleaned_title}'.",
        "Use the lowest-title-density terms as the first indexing gap before scaling paid traffic or broadening the PDP message.",
    ]
    if keyword_report and keyword_report.keywords:
        top_term = keyword_report.keywords[0].phrase
        recommendations.insert(
            0,
            f"Lead the SEO rewrite with '{top_term}' and adjacent long-tail terms with meaningful search volume. Example title direction: '{top_term}' + core benefit + format / pack detail.",
        )
    elif cerebro_report and cerebro_report.keywords:
        top_term = cerebro_report.keywords[0]
        recommendations.insert(
            0,
            f"Lead the SEO rewrite with '{top_term.phrase}' and adjacent long-tail terms. In Cerebro this term carries {_label_integer(top_term.search_volume)} search volume and the target currently ranks {_label_integer(top_term.target_rank)}.",
        )
    if title_misses:
        example_title_terms = ", ".join(f"'{item}'" for item in title_misses[:3])
        recommendations.append(
            "Add missing title keywords first: "
            + example_title_terms
            + f". Suggested result: move those terms into the first 80-100 characters of {branded_reference} so the listing states the use case earlier."
        )
    if copy_misses:
        example_copy_terms = ", ".join(f"'{item}'" for item in copy_misses[:3])
        recommendations.append(
            "Use bullets / description to pick up the next keyword layer: "
            + example_copy_terms
            + ". Suggested result: dedicate one bullet to problem / solution language and another to proof so those targets index without stuffing."
        )
    if xray_report.under_75_reviews_count:
        recommendations.append("Push harder into keyword relevance while review barriers are still low across several competitors.")
    return recommendations[:4]
def _build_cro_recommendations(target_row: XrayProduct | None, competitors: list[XrayProduct]) -> list[str]:
    recommendations = [
        "Simplify the above-the-fold value proposition so the primary outcome is readable in under five seconds.",
        "Tighten the PDP information hierarchy: hero image, proof, ingredient context, usage, and trust markers should read in that order.",
    ]
    if target_row and (target_row.review_count or 0) < 75:
        recommendations.append("Compensate for the lighter review base with stronger proof blocks, FAQ coverage, and comparison framing.")
    if competitors:
        recommendations.append(f"{_trim_text(competitors[0].title, 30)} does a better job of showing proof and hierarchy early; replicate that clarity in the first image stack.")
    return recommendations[:4]
def _build_creative_recommendations(target_row: XrayProduct | None, competitors: list[XrayProduct]) -> list[str]:
    recommendations = [
        "Rebuild the hero image sequence so each panel communicates one claim, one proof point, or one use case.",
        "Add visual comparison and product-context frames instead of relying only on clinical or generic packaging shots.",
    ]
    if competitors:
        recommendations.append(f"{_trim_text(competitors[0].title, 30)} uses a stronger visual proof sequence; mirror that pacing in the refreshed creative set.")
    if target_row and not target_row.image_url:
        recommendations.append("Capture a clean primary listing image before the creative refresh so the deck has a stable hero asset.")
    return recommendations[:4]
def _build_channel_sections(channels: list[str]) -> list[dict[str, Any]]:
    rail = [
        {"label": "Amazon", "key": "amazon"},
        {"label": "TikTok Shop", "key": "tiktok_shop"},
        {"label": "Shopify (DTC)", "key": "shopify"},
        {"label": "3PL", "key": "3pl"},
        {"label": "Shipping OS", "key": "shipping_os"},
    ]
    sections: list[dict[str, Any]] = []
    if "amazon" in channels:
        sections.append(
            {
                "eyebrow": "Amazon offering",
                "title": "Amazon growth support",
                "summary": "Structured the way the sales call walks through delivery, not as a generic checklist.",
                "active_key": "amazon",
                "rail": rail,
                "items": [
                    {"title": "PPC Ad Management", "description": "Lower wasted spend and scale the search terms that survive the conversion benchmark."},
                    {"title": "SEO Listing Optimization", "description": "Align titles, bullets, and imagery around the highest-intent keyword cluster."},
                    {"title": "Graphic Design & Copywriting", "description": "Refresh the image stack, comparison frames, and conversion copy with a tighter story."},
                    {"title": "Brand Registry & IP Protection", "description": "Protect the listing from copycats, bad actors, and content theft as it scales."},
                    {"title": "Marketing Strategy", "description": "Use niche data and category intelligence to prioritize the next 90 days of execution."},
                ],
            }
        )
    if "tiktok_shop" in channels:
        sections.append(
            {
                "eyebrow": "TikTok Shop offering",
                "title": "TikTok Shop support",
                "summary": "Position TikTok as an offer extension with creator, paid, and fulfillment coverage.",
                "active_key": "tiktok_shop",
                "rail": rail,
                "items": [
                    {"title": "Shop Setup & Compliance", "description": "Handle product setup, policy compliance, and shipping configuration so the shop launches correctly."},
                    {"title": "Creator Seeding Campaigns", "description": "Source and manage creators to generate authentic UGC that drives discovery and conversions."},
                    {"title": "Paid TikTok Shop Ads", "description": "Run Spark Ads and GMV-focused campaigns to scale winning content and products."},
                    {"title": "TikTok Fulfillment Strategy", "description": "Implement compliant fulfillment workflows so orders ship correctly and on time."},
                    {"title": "Inventory & Replenishment", "description": "Forecast and manage inventory before paid demand outruns operational capacity."},
                ],
            }
        )
    if "shopify" in channels:
        sections.append(
            {
                "eyebrow": "Shopify offering",
                "title": "Shopify / DTC support",
                "summary": "Keep the DTC slide offer-led and consistent with the Amazon and TikTok visual structure.",
                "active_key": "shopify",
                "rail": rail,
                "items": [
                    {"title": "Store Setup & Optimization", "description": "Build and optimize storefronts that are clean, fast, and designed to convert visitors into customers."},
                    {"title": "Paid Media (Meta & Google)", "description": "Manage paid traffic campaigns that drive qualified demand and scale revenue profitably."},
                    {"title": "Email & SMS Automation", "description": "Create automated flows that increase repeat purchases, retention, and lifetime value."},
                    {"title": "Shopify ↔ Amazon Integration", "description": "Connect Shopify and Amazon to streamline inventory, fulfillment, and order routing."},
                    {"title": "Landing Pages & Funnels", "description": "Design focused landing pages and funnels that increase conversion rate and average order value."},
                ],
            }
        )
    if "3pl" in channels:
        sections.append(
            {
                "eyebrow": "3PL offering",
                "title": "3PL support",
                "summary": "Warehouse, replenishment, and downstream execution support for brands that need tighter order accuracy and throughput.",
                "active_key": "3pl",
                "rail": rail,
                "items": [
                    {"title": "Warehouse onboarding", "description": "Map SKUs, receiving flows, storage logic, and SOPs before volume ramps."},
                    {"title": "Inventory controls", "description": "Set reorder logic, variance checks, and exception handling before stockouts become a growth blocker."},
                    {"title": "Order accuracy", "description": "Reduce mis-picks, label issues, and preventable support tickets with better operational controls."},
                    {"title": "Returns workflow", "description": "Tighten return disposition and feedback loops so damaged margin is visible and recoverable."},
                    {"title": "Client reporting", "description": "Keep the brand team close to fulfillment performance with practical weekly reporting."},
                ],
            }
        )
    if "shipping_os" in channels:
        sections.append(
            {
                "eyebrow": "Shipping OS offering",
                "title": "Shipping OS",
                "summary": "A margin-first shipping operating system that brings routing, cost control, and service levels under one model.",
                "active_key": "shipping_os",
                "rail": rail,
                "items": [
                    {"title": "Carrier optimization", "description": "Route orders against service-level goals and margin thresholds instead of defaulting to static rules."},
                    {"title": "Rate visibility", "description": "Surface shipping cost leakage by order profile, carrier, and region before it compounds."},
                    {"title": "Operational scorecards", "description": "Track SLA performance, exception rates, and shipping cost trends in one operating view."},
                    {"title": "Workflow automation", "description": "Automate repetitive fulfillment decisions so support, warehouse, and finance stay aligned."},
                    {"title": "Margin guardrails", "description": "Keep growth channels profitable by matching promo strategy with fulfillment economics."},
                ],
            }
        )
    return sections
def _build_search_insights(
    *,
    title: str,
    description: str,
    keyword_report: Helium10KeywordReport | None,
    cerebro_report: Helium10CerebroReport | None,
    word_frequency_report: WordFrequencyReport | None,
) -> dict[str, list[str]]:
    keywords = _build_title_targets(keyword_report, cerebro_report)
    title_lc = _clean_listing_title(title).lower()
    description_lc = _clean_listing_title(description).lower()
    copy_targets = _build_copy_targets(
        keywords,
        keyword_report=keyword_report,
        word_frequency_report=word_frequency_report,
    )
    return {
        "title_hits": [phrase for phrase in keywords if phrase.lower() in title_lc],
        "title_misses": [phrase for phrase in keywords if phrase.lower() not in title_lc],
        "copy_hits": [target["label"] for target in copy_targets if _copy_target_is_present(target, description_lc)],
        "copy_misses": [target["label"] for target in copy_targets if not _copy_target_is_present(target, description_lc)],
    }
def _build_target_opportunities(
    *,
    comparison_mode: str,
    brand_name: str,
    target_row: XrayProduct | None,
    hero_product: EnrichedHeroProduct,
    search_insights: dict[str, list[str]],
    market_average_price: float,
    best_seller: XrayProduct | None,
) -> tuple[list[str], list[str]]:
    strengths: list[str] = []
    gaps: list[str] = []
    if hero_product.image_url:
        strengths.append("The listing already has a usable primary image, so the creative refresh can focus on sequencing and proof.")
    price_summary = _build_price_comparison_summary(
        hero_price=hero_product.price,
        market_average_price=market_average_price,
        best_seller_price=(best_seller.price_label if best_seller else ""),
    )
    if price_summary:
        strengths.append(price_summary)
    if target_row and (target_row.rating or 0) >= 4.2:
        strengths.append(f"The listing already carries a credible review signal at {target_row.rating_label}.")
    if search_insights.get("title_hits"):
        strengths.append("The title already captures some high-intent search terms: " + ", ".join(search_insights["title_hits"][:3]) + ".")
    if comparison_mode == "live_unmatched":
        gaps.append("The live Amazon target was not matched in the current niche export, so the benchmark column uses the top page-one competitor while direct target data fills identity, BSR, rating, and review signals where available.")
    elif comparison_mode == "concept_only":
        gaps.append("The prospect is not currently visible in the Amazon market set, so the benchmark column uses the top page-one competitor instead.")
    if not hero_product.description:
        gaps.append("The current listing copy does not expose enough product-story detail; bullets and support content need to be rebuilt.")
    if search_insights.get("title_misses"):
        gaps.append("Important title keywords are still missing: " + ", ".join(search_insights["title_misses"][:3]) + ".")
    if search_insights.get("copy_misses"):
        gaps.append("The bullets / description are not covering: " + ", ".join(search_insights["copy_misses"][:3]) + ".")
    if target_row and (target_row.review_count or 0) < 75:
        gaps.append("The review moat is still light, so the PDP needs stronger proof blocks and comparison framing.")
    if not strengths:
        strengths.append(f"{_brand_product_reference(brand_name)} already gives us a clear product to benchmark against the current niche leaders.")
    if not gaps:
        gaps.append("The next gap is to tighten the claim hierarchy, comparison frames, and conversion proof above the fold.")
    return strengths[:4], gaps[:4]
def _build_title_targets(
    keyword_report: Helium10KeywordReport | None,
    cerebro_report: Helium10CerebroReport | None,
) -> list[str]:
    if cerebro_report and cerebro_report.keywords:
        ordered = sorted(
            cerebro_report.keywords,
            key=lambda item: (-(item.search_volume or 0), 0 if item.target_rank is not None else 1, item.phrase.lower()),
        )
        return [item.phrase.strip() for item in ordered if item.phrase.strip()][:10]
    return [keyword.phrase.strip() for keyword in (keyword_report.keywords[:10] if keyword_report else []) if keyword.phrase.strip()]
def _build_copy_targets(
    phrases: list[str],
    *,
    keyword_report: Helium10KeywordReport | None,
    word_frequency_report: WordFrequencyReport | None,
) -> list[dict[str, Any]]:
    if word_frequency_report and word_frequency_report.words:
        phrase_terms = {term for phrase in phrases for term in _coverage_terms(phrase)}
        ranked_terms = [
            insight.word
            for insight in word_frequency_report.words
            if insight.word not in COPY_TERM_STOP_WORDS and len(insight.word) > 2 and insight.word not in phrase_terms
        ]
        targets = [{"label": term, "terms": (term,)} for term in ranked_terms[:8]]
        if targets:
            return targets
    head_terms = {term for term in _rank_keyword_terms(phrases)[:2]}
    targets: list[dict[str, Any]] = []
    seen: set[str] = set()
    for phrase in phrases:
        ordered_terms = [token for token in re.findall(r"[a-z0-9]+", phrase.lower()) if token in _coverage_terms(phrase)]
        support_terms = [token for token in ordered_terms if token not in head_terms]
        chosen_terms = support_terms or ordered_terms
        if not chosen_terms:
            continue
        label = " ".join(chosen_terms)
        if label in seen:
            continue
        seen.add(label)
        targets.append({"label": label, "terms": tuple(chosen_terms)})
    if not targets and keyword_report and keyword_report.keywords:
        fallback_terms = _rank_keyword_terms([keyword.phrase for keyword in keyword_report.keywords[:10]])
        targets = [{"label": term, "terms": (term,)} for term in fallback_terms[:8]]
    return targets[:8]
def _copy_target_is_present(target: dict[str, Any], description_lc: str) -> bool:
    terms = [str(term) for term in target.get("terms", ()) if str(term)]
    if not terms:
        return False
    description_terms = _coverage_terms(description_lc)
    return all(term in description_terms for term in terms)
