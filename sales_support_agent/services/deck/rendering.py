"""HTML rendering helpers for deck slides."""

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

from sales_support_agent.services.deck.formatting import (  # noqa: F401
    _build_competitor_gap,
    _clean_scraped_text,
    _coerce_number,
    _extract_first,
    _format_channel_label,
    _format_metric_delta,
    _format_metric_display,
    _label_integer,
    _label_share,
    _normalize_key,
    _trim_text,
)


def _render_metric_card(card: dict[str, str]) -> str:
    label = str(card.get("label", "") or "")
    label_html = html.escape(label)
    if _normalize_key(label) == "open_opportunity":
        label_html += " " + _render_help_badge("This compares low-review listings against those already generating meaningful revenue to estimate how much whitespace is still available in the niche.")
    return (
        "<article class='metric-card'>"
        f"<span>{label_html}</span>"
        f"<strong>{html.escape(card.get('value', ''))}</strong>"
        f"<small>{html.escape(card.get('meta', ''))}</small>"
        "</article>"
    )
def _render_competitor_card(product: XrayProduct, total_revenue: float) -> str:
    image = f"<img src='{html.escape(product.image_url)}' alt='{html.escape(product.title)}' />" if product.image_url else "<div class='image-fallback'>No image</div>"
    share = _label_share(product.revenue, total_revenue)
    return (
        "<article class='competitor-card'>"
        f"<div class='competitor-media'>{image}</div>"
        "<div class='competitor-body'>"
        f"<h3>{html.escape(_trim_text(product.title, 40))}</h3>"
        f"<p class='muted'>{html.escape(product.brand)}<br>{html.escape(product.asin)}</p>"
        f"<p><strong>Revenue</strong><br>{html.escape(product.revenue_label)}</p>"
        f"<p><strong>Market share</strong><br>{html.escape(share)}</p>"
        f"<p><strong>BSR</strong><br>{html.escape(product.bsr_label)}</p>"
        f"<p><strong>Reviews</strong><br>{html.escape(str(product.review_count or 'n/a'))}</p>"
        f"<p>{html.escape(_build_competitor_gap(product, None))}</p>"
        "</div>"
        "</article>"
    )
def _render_keyword_row(keyword: KeywordInsight) -> str:
    return (
        "<tr>"
        f"<td>{html.escape(keyword.phrase)}</td>"
        f"<td>{html.escape(keyword.search_volume_label)}</td>"
        f"<td>{html.escape(keyword.keyword_sales_label)}</td>"
        f"<td>{html.escape(str(keyword.competing_products or ''))}</td>"
        f"<td>{html.escape(str(keyword.title_density or ''))}</td>"
        "</tr>"
    )
def _render_keyword_table(rows: list[list[str]]) -> str:
    if not rows:
        return (
            "<thead><tr><th>Keyword</th><th>Search volume</th><th>Sales</th><th>Competing products</th><th>Title density</th></tr></thead>"
            "<tbody><tr><td colspan='5' class='muted'>No keyword data available.</td></tr></tbody>"
        )
    headers = rows[0]
    body = rows[1:]
    header_html = "".join(f"<th>{html.escape(str(header))}</th>" for header in headers)
    if not body:
        return f"<thead><tr>{header_html}</tr></thead><tbody><tr><td colspan='{len(headers)}' class='muted'>No keyword data available.</td></tr></tbody>"
    body_html = "".join(
        "<tr>" + "".join(f"<td>{html.escape(str(cell))}</td>" for cell in row) + "</tr>"
        for row in body
    )
    return f"<thead><tr>{header_html}</tr></thead><tbody>{body_html}</tbody>"
def _render_cerebro_rank_summary(cerebro_report: Helium10CerebroReport | None) -> str:
    if not cerebro_report or not cerebro_report.keywords:
        return ""
    buckets = [
        ("Rank 1-5", 0, 0),
        ("Rank 6-10", 0, 0),
        ("Rank 11-20", 0, 0),
        ("Rank 21-50", 0, 0),
        ("Unranked", 0, 0),
    ]
    for keyword in cerebro_report.keywords:
        rank = keyword.target_rank
        volume = keyword.search_volume or 0
        if rank is not None and 1 <= rank <= 5:
            index = 0
        elif rank is not None and 6 <= rank <= 10:
            index = 1
        elif rank is not None and 11 <= rank <= 20:
            index = 2
        elif rank is not None and 21 <= rank <= 50:
            index = 3
        else:
            index = 4
        label, count, proxy = buckets[index]
        buckets[index] = (label, count + 1, proxy + volume)
    max_count = max((count for _, count, _ in buckets), default=1) or 1
    rows = "".join(
        "<li>"
        f"<div><strong>{html.escape(label)}</strong><span>{_label_integer(count)} keywords</span></div>"
        f"<div class='rank-track'><span style='width:{max(8, int((count / max_count) * 100))}%'></span></div>"
        f"<small>{_label_integer(proxy)} search volume</small>"
        "</li>"
        for label, count, proxy in buckets
    )
    return (
        "<div class='dashboard-card'>"
        "<div class='card-head'><h3>Ranking path</h3>"
        "<span class='muted'>Where the target already ranks and where the next keyword lifts can come from</span></div>"
        f"<ul class='rank-summary-list'>{rows}</ul>"
        "</div>"
    )
def _render_word_frequency_bubbles(report: Any) -> str:
    if not isinstance(report, WordFrequencyReport) or not report.words:
        return ""
    words = report.words[:12]
    max_frequency = max((item.frequency for item in words), default=1) or 1
    bubbles = "".join(
        "<li class='term-bubble' "
        f"style='--bubble-size:{72 + int((item.frequency / max_frequency) * 84)}px'>"
        f"<strong>{html.escape(item.word)}</strong>"
        f"<span>{_label_integer(item.frequency)}</span>"
        "</li>"
        for item in words
    )
    return (
        "<div class='dashboard-card'>"
        "<div class='card-head'><h3>Support-term demand</h3>"
        "<span class='muted'>Single-word demand from the word-frequency file to guide bullet and copy expansion</span></div>"
        f"<ul class='bubble-cloud'>{bubbles}</ul>"
        "</div>"
    )
def _render_revenue_bar(product: XrayProduct, total_revenue: float) -> str:
    share = 0.0 if total_revenue <= 0 else ((product.revenue or 0.0) / total_revenue)
    width = max(6, min(int(round(share * 100)), 100))
    return (
        f"<article class='revenue-row' title='{html.escape(f'{product.title}: {product.revenue_label} ({share * 100:.1f}% share)')}' >"
        "<div class='revenue-labels'>"
        f"<strong>{html.escape(_trim_text(product.title, 40))}</strong>"
        f"<span>{html.escape(product.revenue_label)}</span>"
        "</div>"
        f"<div class='revenue-track'><div class='revenue-fill' style='width:{width}%'></div></div>"
        "</article>"
    )
def _render_niche_summary_row(product: XrayProduct, total_revenue: float) -> str:
    share = 0.0 if total_revenue <= 0 else ((product.revenue or 0.0) / total_revenue) * 100
    image_html = (
        f"<img src='{html.escape(product.image_url)}' alt='{html.escape(product.title)}' />"
        if product.image_url
        else "<div class='image-fallback compact'>No image</div>"
    )
    return (
        "<tr>"
        f"<td>{html.escape(str(product.display_order))}</td>"
        "<td>"
        "<div class='niche-product-cell'>"
        f"<div class='niche-product-thumb'>{image_html}</div>"
        "<div>"
        f"<strong>{html.escape(_trim_text(product.title, 40))}</strong>"
        f"<div class='muted'>{html.escape(product.asin)} · {html.escape(product.brand)}</div>"
        "</div>"
        "</div>"
        "</td>"
        f"<td>{html.escape(product.price_label)}</td>"
        f"<td>{html.escape(product.revenue_label)}</td>"
        f"<td>{share:.1f}%</td>"
        "</tr>"
    )
def _render_target_comparison_table(target: dict[str, Any], best_seller: XrayProduct | None, missing_image_asset: str = "") -> str:
    comparison_mode = str(target.get("comparison_mode", "") or "")
    launch_mode = comparison_mode == "concept_only"
    direct_metric_reason = "Unavailable from direct target data."
    benchmark_metric_reason = "Not present in the current niche export."
    if comparison_mode == "live_unmatched":
        benchmark_metric_reason = "Not matched in the current niche export."
    target_price_number = _coerce_number(str(target.get("price", "") or ""))
    target_bsr_number = _coerce_number(str(target.get("bsr", "") or ""))
    target_revenue_number = _coerce_number(str(target.get("revenue", "") or ""))
    target_rating_number = _coerce_number(str(target.get("rating", "") or ""))
    target_reviews_number = _coerce_number(str(target.get("review_count", "") or ""))
    rows = [
        (
            "Listing",
            _render_comparison_listing_cell(
                image_url=str(target.get("image_url", "") or ""),
                title=_trim_text(str(target.get("title", "") or ("Target product" if launch_mode else "Target listing")), 40),
                brand=str(target.get("brand_name", "") or "Prospect brand"),
                emphasized=True,
                missing_image_asset=missing_image_asset,
            ),
            _render_comparison_listing_cell(
                image_url=str(best_seller.image_url or "") if best_seller else "",
                title=_trim_text(best_seller.title, 40) if best_seller else "Best seller",
                brand=str(best_seller.brand or "Benchmark") if best_seller else "Benchmark",
                emphasized=False,
                missing_image_asset=missing_image_asset,
            ),
        ),
        (
            "Price",
            _render_target_metric(
                str(target.get("price", "") or ""),
                target_price_number,
                best_seller.price if best_seller else None,
                inverse=False,
                missing_reason="Unavailable from the target page.",
                unit="currency",
            ),
            _render_plain_metric(best_seller.price_label if best_seller else "n/a"),
        ),
        (
            "BSR",
            _render_target_metric(
                str(target.get("bsr", "") or ""),
                target_bsr_number,
                best_seller.bsr if best_seller else None,
                inverse=True,
                missing_reason=direct_metric_reason if comparison_mode == "live_unmatched" else benchmark_metric_reason,
                unit="number",
            ),
            _render_plain_metric(best_seller.bsr_label if best_seller else "n/a"),
        ),
        (
            "Revenue",
            _render_target_metric(
                str(target.get("revenue", "") or ""),
                target_revenue_number,
                best_seller.revenue if best_seller else None,
                inverse=False,
                missing_reason=benchmark_metric_reason,
                unit="currency",
            ),
            _render_plain_metric(best_seller.revenue_label if best_seller else "n/a"),
        ),
        (
            "Rating",
            _render_target_metric(
                str(target.get("rating", "") or ""),
                target_rating_number,
                best_seller.rating if best_seller else None,
                inverse=False,
                missing_reason=direct_metric_reason if comparison_mode == "live_unmatched" else benchmark_metric_reason,
                unit="float",
            ),
            _render_plain_metric(best_seller.rating_label if best_seller else "n/a"),
        ),
        (
            "Reviews",
            _render_target_metric(
                str(target.get("review_count", "") or ""),
                target_reviews_number,
                float(best_seller.review_count or 0) if best_seller else None,
                inverse=False,
                missing_reason=direct_metric_reason if comparison_mode == "live_unmatched" else benchmark_metric_reason,
                unit="number",
            ),
            _render_plain_metric(str(best_seller.review_count or "n/a") if best_seller else "n/a"),
        ),
        (
            "Dims",
            _render_target_plain_metric(
                str(target.get("dimensions", "") or ""),
                missing_reason="Unavailable from the target page.",
            ),
            _render_plain_metric(str(best_seller.dimensions or "Unavailable") if best_seller else "Unavailable"),
        ),
    ]
    body = "".join(
        "<tr>"
        f"<td>{html.escape(label)}</td>"
        f"<td class='target-column'>{target_value}</td>"
        f"<td class='benchmark-column'>{best_value}</td>"
        "</tr>"
        for label, target_value, best_value in rows
    )
    return (
        "<div class='comparison-table-wrap'>"
        "<table class='comparison-table comparison-table-structured'>"
        "<colgroup><col class='comparison-metric-col'><col class='comparison-target-col'><col class='comparison-benchmark-col'></colgroup>"
        f"<thead><tr><th>Metric</th><th>{'Target product' if launch_mode else 'Target listing'}</th><th>Best seller</th></tr></thead>"
        f"<tbody>{body}</tbody>"
        "</table>"
        "</div>"
    )
def _render_competitor_landscape_table(products: list[XrayProduct], total_revenue: float) -> str:
    rows = []
    for product in products:
        image_html = (
            f"<img src='{html.escape(product.image_url)}' alt='{html.escape(product.title)}' />"
            if product.image_url
            else "<div class='image-fallback compact'>No image</div>"
        )
        rows.append(
            "<tr>"
            f"<td><div class='table-product-cell'><div class='table-product-thumb'>{image_html}</div><div><strong>{html.escape(_trim_text(product.title, 40))}</strong><div class='muted'>{html.escape(product.asin)}</div></div></div></td>"
            f"<td>{html.escape(product.price_label)}</td>"
            f"<td>{html.escape(product.revenue_label)}</td>"
            f"<td>{html.escape(_label_share(product.revenue, total_revenue))}</td>"
            f"<td>{html.escape(product.bsr_label)}</td>"
            f"<td>{html.escape(product.rating_label)}</td>"
            f"<td>{html.escape(str(product.review_count or ''))}</td>"
            "</tr>"
        )
    return (
        "<table class='landscape-table'>"
        "<thead><tr><th>Product</th><th>Price</th><th>Revenue</th><th>Market share</th><th>BSR</th><th>Rating</th><th>Reviews</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        "</table>"
    )
def _render_distribution_card(title: str, slices: list[DistributionSlice]) -> str:
    donut = _render_donut(slices)
    palette = ["#d39a49", "#8d4e54", "#85bbda", "#c3a46d", "#d26b36", "#cdd7e3"]
    items = "".join(
        f"<li title='{html.escape(f'{item.label}: {item.count} listings ({item.share * 100:.1f}%)')}' style='--legend-color:{palette[index % len(palette)]}'><span>{html.escape(item.label)}</span><strong>{item.count}</strong></li>"
        for index, item in enumerate(slices[:6])
    )
    return (
        "<article class='distribution-card'>"
        f"<h3>{html.escape(title)}</h3>"
        f"{donut}"
        f"<ul>{items}</ul>"
        "</article>"
    )
def _render_donut(slices: list[DistributionSlice]) -> str:
    palette = ["#d39a49", "#8d4e54", "#85bbda", "#c3a46d", "#d26b36", "#cdd7e3"]
    stops: list[str] = []
    start = 0.0
    for index, item in enumerate(slices[:6]):
        end = start + (item.share * 100)
        stops.append(f"{palette[index % len(palette)]} {start:.2f}% {end:.2f}%")
        start = end
    if start < 100:
        stops.append(f"#edf2f7 {start:.2f}% 100%")
    style = f"background: conic-gradient({', '.join(stops)});"
    tooltip = ", ".join(f"{item.label}: {item.count} ({item.share * 100:.1f}%)" for item in slices[:6])
    return f"<div class='donut-chart'><div class='donut-visual' title=\"{html.escape(tooltip)}\" style=\"{style}\"></div></div>"
def _render_offering_tabs(sections: list[dict[str, Any]]) -> str:
    if not sections:
        return ""
    tabs = []
    panels = []
    for index, section in enumerate(sections):
        active_class = " is-active" if index == 0 else ""
        hidden_attr = "" if index == 0 else " hidden"
        key = html.escape(str(section.get("active_key", "")))
        tabs.append(
            f"<button class='offering-tab{active_class}' type='button' data-tab='{key}'>{html.escape(_format_channel_label(str(section.get('active_key', ''))))}</button>"
        )
        items = "".join(
            "<article class='service-card'>"
            f"<h3>{html.escape(str(item.get('title', '')))}</h3>"
            f"<p>{html.escape(str(item.get('description', '')))}</p>"
            "</article>"
            for item in section.get("items", [])
        )
        panels.append(
            "<div class='offering-panel{active_class}' data-panel='{key}'{hidden_attr}>"
            .format(active_class=active_class, key=key, hidden_attr=hidden_attr)
            + "<div class='offering-panel-head'>"
            + f"<h3>{html.escape(str(section.get('title', '')))}</h3>"
            + "</div>"
            + f"<div class='service-grid'>{items}</div></div>"
        )
    return (
        "<section class='slide'>"
        "<div class='slide-head'><div><p class='eyebrow'>Service offerings</p><h2>Integrated support model</h2></div>"
        "<p class='muted'>Marketplace, DTC, fulfillment, and shipping support are organized into one operating model with service-specific workstreams.</p></div>"
        f"<div class='offering-tabs'>{''.join(tabs)}</div>"
        f"<div class='offering-panels'>{''.join(panels)}</div>"
        "</section>"
    )
def _render_hero_media(target: dict[str, Any], missing_image_asset: str) -> str:
    image_url = str(target.get("image_url", "") or "").strip()
    if image_url:
        return (
            "<div class='hero-media'>"
            f"<img src='{html.escape(image_url)}' alt='{html.escape(str(target.get('title', 'Target product')))}' />"
            "</div>"
        )
    fallback_media = missing_image_asset if missing_image_asset else ""
    label = "" if fallback_media else "<span class='image-fallback-label'>No product image available</span>"
    return f"<div class='hero-media fallback'>{fallback_media}{label}</div>"
def _product_to_gallery_item(product: XrayProduct) -> dict[str, str]:
    return {
        "title": product.title,
        "subtitle": product.brand,
        "image_url": product.image_url,
        "meta": f"{product.revenue_label} revenue · {product.bsr_label} BSR",
    }
def _render_gallery_card(item: dict[str, Any]) -> str:
    image_url = str(item.get("image_url", "") or "").strip()
    media = f"<img src='{html.escape(image_url)}' alt='{html.escape(str(item.get('title', 'Listing image')))}' />" if image_url else "<div class='image-fallback'>Image unavailable</div>"
    return (
        "<article class='gallery-card'>"
        f"<div class='gallery-media'>{media}</div>"
        f"<strong>{html.escape(_trim_text(str(item.get('title', '')), 40))}</strong>"
        f"<p>{html.escape(str(item.get('subtitle', '')))}</p>"
        f"<small>{html.escape(str(item.get('meta', '')))}</small>"
        "</article>"
    )
def _render_signal_list(title: str, hits: list[str], misses: list[str], miss_label: str) -> str:
    hit_items = "".join(
        f"<li><span class='signal-icon positive'>+</span><span>{html.escape(item)}</span></li>"
        for item in hits[:5]
    ) or "<li><span class='signal-icon positive'>+</span><span>None identified yet.</span></li>"
    miss_items = "".join(
        f"<li><span class='signal-icon negative'>+</span><span>{html.escape(item)}</span></li>"
        for item in misses[:5]
    ) or "<li><span class='signal-icon negative'>+</span><span>No immediate gaps from the current keyword dataset.</span></li>"
    help_text = (
        "Title coverage checks whether exact high-intent keyword phrases are already present in the title."
        if "title" in title.lower()
        else "Bullet / copy coverage checks whether the supporting concepts, modifiers, and use-case terms appear across bullets and descriptive copy."
    )
    return (
        f"<h3>{html.escape(title)} {_render_help_badge(help_text)}</h3>"
        f"<div class='signal-list'><strong>Already covered</strong><ul class='signal-bullets'>{hit_items}</ul></div>"
        f"<div class='signal-list'><strong>{html.escape(miss_label)}</strong><ul class='signal-bullets'>{miss_items}</ul></div>"
    )
def _render_resource_card(title: str, description: str, url: str) -> str:
    safe_url = html.escape(url, quote=True)
    return (
        "<article class='resource-card'>"
        f"<h3>{html.escape(title)}</h3>"
        f"<p>{html.escape(description)}</p>"
        f"<a href='{safe_url}' target='_blank' rel='noreferrer'>Open link</a>"
        "</article>"
    )
def _render_embedded_resource_tabs(*, case_study_url: str, creative_mockup_url: str) -> str:
    resources: list[tuple[str, str, str]] = []
    if case_study_url:
        resources.append(("case-studies", "Case studies", case_study_url))
    if creative_mockup_url:
        resources.append(("listing-mockup", "Creative mockup", creative_mockup_url))
    if not resources:
        return ""
    tabs: list[str] = []
    panels: list[str] = []
    for index, (key, label, url) in enumerate(resources):
        active_class = " is-active" if index == 0 else ""
        hidden_attr = "" if index == 0 else " hidden"
        tabs.append(f"<button class='embedded-tab{active_class}' type='button' data-tab='{html.escape(key)}'>{html.escape(label)}</button>")
        if key == "case-studies" and "canva.com" in urlparse(url).netloc.lower():
            panels.append(
                f"<div class='embedded-panel{active_class}' data-panel='{html.escape(key)}'{hidden_attr}>"
                + _render_canva_embed(label=label, url=url)
                + "</div>"
            )
        else:
            preview = _fetch_embed_preview(url)
            panels.append(
                f"<div class='embedded-panel{active_class}' data-panel='{html.escape(key)}'{hidden_attr}>"
                + _render_resource_embed(label=label, url=url, preview=preview)
                + "</div>"
            )
    return "<div class='embedded-resource-section'><div class='embedded-tabs'>" + "".join(tabs) + "</div><div class='embedded-panels'>" + "".join(panels) + "</div></div>"
def _fetch_embed_preview(url: str) -> dict[str, str]:
    """Fetch a Canva/case-study URL and pull og:title + og:image for the embed card.

    Hard-capped at 5s so a slow/down third-party host can't stall an entire
    deck render. Any network/HTTP/parse error is silently treated as "no
    preview available" — the renderer falls back to a plain link card.
    """
    safe_url = str(url or "").strip()
    if not safe_url:
        return {}
    try:
        response = requests.get(
            safe_url,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            },
            timeout=5,
        )
        response.raise_for_status()
    except (requests.RequestException, ValueError):
        return {}
    content = response.text or ""
    title = _extract_first(
        content,
        r'<meta\s+property="og:title"\s+content="([^"]+)"',
        r"<title>\s*(.*?)\s*</title>",
    ).strip()
    image_url = _extract_first(
        content,
        r'<meta\s+property="og:image"\s+content="([^"]+)"',
        r'<meta\s+name="twitter:image"\s+content="([^"]+)"',
    ).replace("\\u0026", "&").replace("\\/", "/").strip()
    return {
        "title": _clean_scraped_text(title),
        "image_url": image_url,
    }
def _render_resource_embed(*, label: str, url: str, preview: dict[str, str]) -> str:
    image_url = str(preview.get("image_url", "") or "").strip()
    preview_title = str(preview.get("title", "") or label).strip()
    safe_url = html.escape(url, quote=True)
    if "canva.com" in urlparse(url).netloc.lower():
        media = (
            f"<img src='{html.escape(image_url, quote=True)}' alt='{html.escape(preview_title)}' />"
            if image_url
            else "<div class='image-fallback'>Preview unavailable</div>"
        )
        return (
            "<div class='resource-preview-card'>"
            f"<div class='resource-preview-media'>{media}</div>"
            f"<div class='resource-preview-copy'><h3>{html.escape(label)}</h3><p>{html.escape(preview_title)}</p>"
            f"<a class='resource-link' href='{safe_url}' target='_blank' rel='noreferrer'>Open in Canva</a></div>"
            "</div>"
        )
    return f"<iframe src='{safe_url}' title='{html.escape(label)}' loading='lazy' referrerpolicy='no-referrer-when-downgrade'></iframe>"
def _render_canva_embed(*, label: str, url: str) -> str:
    safe_url = html.escape(url, quote=True)
    parsed = urlparse(url)
    embed_src = f"{parsed.scheme}://{parsed.netloc}{parsed.path}?embed"
    safe_embed = html.escape(embed_src, quote=True)
    return (
        "<div class='canva-embed-wrap'>"
        "<div class='canva-embed-frame'>"
        f"<iframe loading='lazy' src='{safe_embed}' title='{html.escape(label)}' allowfullscreen='allowfullscreen' allow='fullscreen'></iframe>"
        "</div>"
        f"<a class='resource-link' href='{safe_url}' target='_blank' rel='noopener'>Open in Canva</a>"
        "</div>"
    )
def _render_offer_card(card: dict[str, Any]) -> str:
    return (
        "<article class='offer-card'>"
        f"<h3>{html.escape(str(card.get('title', '')))}</h3>"
        f"<p>{html.escape(str(card.get('description', '')))}</p>"
        "<div class='offer-stats'>"
        f"<div><span>{html.escape(str(card.get('price_label', '')))}</span><strong>{html.escape(str(card.get('price', '')))}</strong></div>"
        f"<div><span>{html.escape(str(card.get('commission_label', '')))}</span><strong>{html.escape(str(card.get('commission', '')))}</strong></div>"
        f"<div><span>{html.escape(str(card.get('baseline_label', '')))}</span><strong>{html.escape(str(card.get('baseline', '')))}</strong></div>"
        "</div>"
        f"<small>{html.escape(str(card.get('bonus', '')))}</small>"
        "<a class='offer-link' href='https://anatainc.com/contact' target='_blank' rel='noreferrer'>Get started</a>"
        "</article>"
    )
def _render_comparison_listing_cell(*, image_url: str, title: str, brand: str, emphasized: bool, missing_image_asset: str = "") -> str:
    media = (
        f"<img src='{html.escape(image_url, quote=True)}' alt='{html.escape(title)}' />"
        if image_url
        else (missing_image_asset if missing_image_asset else "<div class='image-fallback compact'>No image</div>")
    )
    return (
        f"<div class='comparison-listing{' is-target' if emphasized else ''}'>"
        f"<div class='comparison-thumb'>{media}</div>"
        f"<div><strong>{html.escape(title)}</strong><div class='muted'>{html.escape(brand)}</div></div>"
        "</div>"
    )
def _render_plain_metric(value: str) -> str:
    return f"<div class='comparison-metric'><strong>{html.escape(_format_metric_display(value))}</strong></div>"
def _render_target_plain_metric(value: str, *, missing_reason: str) -> str:
    cleaned = str(value or "").strip()
    if cleaned and cleaned.lower() not in {"n/a", "na", "unavailable"}:
        return _render_plain_metric(cleaned)
    return (
        "<div class='comparison-metric comparison-metric-missing'>"
        "<strong>Unavailable</strong>"
        f"<span class='metric-delta'>{html.escape(missing_reason)}</span>"
        "</div>"
    )
def _render_metric_with_delta(display_value: str, target_value: float | None, benchmark_value: float | None, *, inverse: bool, unit: str = "number") -> str:
    delta = _format_metric_delta(target_value, benchmark_value, inverse=inverse, unit=unit)
    delta_html = f"<span class='metric-delta'>{html.escape(delta)}</span>" if delta else ""
    return f"<div class='comparison-metric'><strong>{html.escape(_format_metric_display(display_value))}</strong>{delta_html}</div>"
def _render_target_metric(
    display_value: str,
    target_value: float | None,
    benchmark_value: float | None,
    *,
    inverse: bool,
    missing_reason: str,
    unit: str = "number",
) -> str:
    cleaned = str(display_value or "").strip()
    if cleaned and cleaned.lower() not in {"n/a", "na", "unavailable"}:
        return _render_metric_with_delta(cleaned, target_value, benchmark_value, inverse=inverse, unit=unit)
    return (
        "<div class='comparison-metric comparison-metric-missing'>"
        "<strong>Unavailable</strong>"
        f"<span class='metric-delta'>{html.escape(missing_reason)}</span>"
        "</div>"
    )
def _render_help_badge(text: str) -> str:
    return f"<span class='help-badge' title='{html.escape(text, quote=True)}'>?</span>"
def _render_emphasis_list_item(text: str) -> str:
    highlighted = html.escape(text)
    highlighted = re.sub(r"'([^']+)'", r"<mark>\1</mark>", highlighted)
    return f"<li>{highlighted}</li>"
def _render_recommendation_item(text: str) -> str:
    return f"<li>{_highlight_competitor_names(html.escape(text))}</li>"
def _render_action_item(text: str) -> str:
    safe = html.escape(text)
    safe = re.sub(r"^([^:;]{3,80}[:;])", r"<strong>\1</strong>", safe)
    safe = re.sub(r"(Important title keywords are still missing:|The bullets / description are not covering:|The prospect is not currently visible in the Amazon market set, so|The current listing copy does not expose enough product-story detail;|The review moat is still light, so)", r"<strong>\1</strong>", safe)
    return f"<li>{safe}</li>"
def _highlight_competitor_names(text: str) -> str:
    return re.sub(r"([A-Z][A-Za-z0-9&' -]{2,})", r"<strong>\1</strong>", text, count=1)
