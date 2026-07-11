"""Rainforest API client for automatic Amazon competitor discovery.

Replaces the manual Helium 10 CSV upload workflow for the Digital Shelf
intake path. Given a single ASIN, discovers the top competitors in the
same BSR category and builds a Helium10XrayReport ready to feed into
the existing deck generation pipeline.

Revenue and unit figures are estimated from BSR using the same heuristic
already in service.py (_estimate_target_units): min(50_000, 75_000 / BSR).
Actual Helium 10 proprietary sales data is not available via public scraping.
"""

from __future__ import annotations

import concurrent.futures
import dataclasses
import logging
import os
import re
from typing import Any

import requests

from sales_support_agent.services.helium10 import (
    DistributionSlice,
    Helium10XrayReport,
    XrayProduct,
)

logger = logging.getLogger(__name__)

_RAINFOREST_BASE = "https://api.rainforestapi.com/request"
_AMAZON_DOMAIN = "amazon.com"
# Per-call timeout: a single slow Rainforest request fails fast (the competitor
# is skipped) instead of pinning a worker for 30s and dragging the whole batch
# toward the 120s proxy timeout.
_DEFAULT_TIMEOUT = 20
# Fetch the competitor set in a single parallel wave (see _DEFAULT_COMPETITOR_LIMIT).
_MAX_CONCURRENT_FETCHES = 12
_BSR_ESTIMATE_CAP = 50_000
# The deck surfaces the top ~10 competitors (niche table [:10], gallery [:4]),
# so discovering 12 keeps full display coverage while cutting generation time
# roughly in half versus 20 (one fetch wave instead of three).
_DEFAULT_COMPETITOR_LIMIT = 12


def _bsr_to_units(bsr: float | None) -> int:
    """Estimate monthly unit sales from BSR. Same formula as service.py:180.

    Only used as a fallback when Amazon's real ``recent_sales`` badge
    ("X+ bought in past month") is absent from the listing.
    """
    if not bsr or bsr <= 0:
        return 0
    return min(_BSR_ESTIMATE_CAP, max(1, int(round(75_000.0 / bsr))))


def _parse_recent_sales(value: str | None) -> int | None:
    """Parse Amazon's real "X+ bought in past month" badge into a unit floor.

    Rainforest surfaces this verbatim in the ``recent_sales`` field, e.g.
    "50+ bought in past month" or "1K+ bought in past month". This is real
    Amazon data (a lower bound), not a BSR estimate. Returns the integer
    floor, or None when the field is absent/unparseable.
    """
    if not value:
        return None
    m = re.search(r"([\d,.]+)\s*([KkMm]?)\s*\+?\s*bought", value)
    if not m:
        return None
    try:
        num = float(m.group(1).replace(",", ""))
    except ValueError:
        return None
    suffix = m.group(2).lower()
    if suffix == "k":
        num *= 1_000
    elif suffix == "m":
        num *= 1_000_000
    return int(num) if num > 0 else None


def _extract_asin_from_url(url: str) -> str:
    """Pull ASIN from an Amazon product URL, or return empty string."""
    m = re.search(r"/dp/([A-Z0-9]{10})", url or "")
    return m.group(1) if m else ""


def _normalize_asin(value: str) -> str:
    """Return the bare 10-char ASIN whether the user pasted an ASIN or URL."""
    value = (value or "").strip()
    if re.match(r"^[A-Z0-9]{10}$", value):
        return value
    extracted = _extract_asin_from_url(value)
    return extracted


class RainforestClient:
    """Thin wrapper around the Rainforest API for product + bestseller data."""

    def __init__(self, api_key: str | None = None, *, base_url: str = _RAINFOREST_BASE):
        self.api_key = (api_key or os.getenv("RAINFOREST_API_KEY", "")).strip()
        self._base_url = base_url
        self._session = requests.Session()

    def _get(self, params: dict[str, Any]) -> dict[str, Any]:
        params = {
            "api_key": self.api_key,
            "amazon_domain": _AMAZON_DOMAIN,
            **params,
        }
        resp = self._session.get(self._base_url, params=params, timeout=_DEFAULT_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        if data.get("request_info", {}).get("success") is False:
            raise RuntimeError(f"Rainforest API error: {data.get('request_info', {}).get('message', 'unknown')}")
        return data

    def get_product(self, asin: str) -> dict[str, Any]:
        return self._get({"type": "product", "asin": asin})

    def get_bestsellers(self, url: str) -> dict[str, Any]:
        return self._get({"type": "bestsellers", "url": url})

    def search(self, search_term: str, *, page: int = 1) -> dict[str, Any]:
        return self._get({"type": "search", "search_term": search_term, "page": page})

    # ------------------------------------------------------------------
    # Higher-level methods
    # ------------------------------------------------------------------

    def _competitor_asins_from_bestsellers(
        self,
        target_product: dict[str, Any],
        target_asin: str,
        *,
        limit: int,
    ) -> list[str]:
        """Discover competitors via the target product's BSR category bestsellers list."""
        bestsellers_rank = target_product.get("bestsellers_rank") or []
        asins: list[str] = []

        # Walk the BSR list from most specific (deepest) to least specific.
        # Avoid the root category (e.g. "Books" — too broad).
        for rank_entry in reversed(bestsellers_rank):
            # Rainforest's real field is `link`; `category_url` kept as a
            # defensive fallback in case the schema differs by account.
            cat_url = rank_entry.get("link") or rank_entry.get("category_url") or ""
            lc = cat_url.lower()
            if not cat_url or not (
                "best-sellers" in lc or "bestsellers" in lc or "/zgbs/" in lc
            ):
                continue
            try:
                bs_data = self.get_bestsellers(cat_url)
                for item in bs_data.get("bestsellers") or []:
                    asin = (item.get("asin") or "").strip()
                    if asin and asin != target_asin and asin not in asins:
                        asins.append(asin)
                        if len(asins) >= limit:
                            return asins
                if asins:
                    return asins
            except Exception as exc:
                logger.warning("Rainforest bestsellers error for %s: %s", cat_url, exc)

        return asins

    def _competitor_asins_from_search(
        self,
        target_product: dict[str, Any],
        target_asin: str,
        *,
        limit: int,
    ) -> list[str]:
        """Fallback: derive competitors from a keyword search on the product title."""
        title = target_product.get("title", "")
        # Strip size/count/flavor modifiers — keep 3-5 meaningful nouns
        words = [w for w in re.sub(r"[^a-zA-Z0-9 ]", " ", title).split() if len(w) > 3][:5]
        if not words:
            return []
        search_term = " ".join(words)
        asins: list[str] = []
        try:
            data = self.search(search_term)
            for item in data.get("search_results") or []:
                asin = (item.get("asin") or "").strip()
                if asin and asin != target_asin and asin not in asins:
                    asins.append(asin)
                    if len(asins) >= limit:
                        break
        except Exception as exc:
            logger.warning("Rainforest search error for %r: %s", search_term, exc)
        return asins

    def _product_to_xray(self, data: dict[str, Any], *, display_order: int) -> XrayProduct | None:
        """Convert a type=product API response to an XrayProduct."""
        product = data.get("product") or {}
        if not product:
            return None

        asin = (product.get("asin") or "").strip()
        title = (product.get("title") or "").strip()
        brand = (product.get("brand") or "").strip()
        link = product.get("link") or f"https://www.amazon.com/dp/{asin}"
        main_image = (product.get("main_image") or {}).get("link", "")

        # Price: prefer buybox price
        price_obj = (
            (product.get("buybox_winner") or {}).get("price")
            or product.get("price")
            or {}
        )
        price_val: float | None = price_obj.get("value")
        currency_symbol = price_obj.get("symbol", "$")
        price_label = f"{currency_symbol}{price_val:.2f}" if price_val else "N/A"

        # BSR + category from bestsellers_rank array
        bestsellers_rank = product.get("bestsellers_rank") or []
        bsr: float | None = None
        category = ""
        if bestsellers_rank:
            first = bestsellers_rank[0]
            bsr = first.get("rank")
            category = first.get("category", "")

        rating = product.get("rating")
        ratings_total = product.get("ratings_total")

        # Units: prefer Amazon's REAL "X+ bought in past month" badge when
        # present; only fall back to the BSR estimate when it's absent.
        real_units = _parse_recent_sales(product.get("recent_sales"))
        units = real_units if real_units is not None else _bsr_to_units(bsr)
        units_are_real = real_units is not None
        revenue = (units * price_val) if (units and price_val) else None
        # Real badges are a floor ("50+"), so mark units/revenue with a "+".
        _suffix = "+" if units_are_real else ""

        # Real fulfillment (FBA/FBM) from the buybox winner, not a hardcode.
        fulfillment_obj = (product.get("buybox_winner") or {}).get("fulfillment") or {}
        if fulfillment_obj.get("is_fulfilled_by_amazon") or fulfillment_obj.get("is_sold_by_amazon"):
            fulfillment = "FBA"
        elif fulfillment_obj.get("is_sold_by_third_party") or fulfillment_obj.get("third_party_seller"):
            fulfillment = "FBM"
        else:
            fulfillment = ""

        return XrayProduct(
            display_order=display_order,
            title=title,
            asin=asin,
            url=link,
            image_url=main_image,
            brand=brand,
            price=price_val,
            price_label=price_label,
            revenue=revenue,
            revenue_label=f"${revenue:,.0f}{_suffix}" if revenue else "N/A",
            units_sold=float(units) if units else None,
            units_label=f"{units:,}{_suffix}" if units else "N/A",
            bsr=float(bsr) if bsr else None,
            bsr_label=f"{int(bsr):,}" if bsr else "N/A",
            rating=float(rating) if rating else None,
            rating_label=f"{rating:.1f}" if rating else "N/A",
            review_count=int(ratings_total) if ratings_total else None,
            category=category,
            seller_country="",
            size_tier="",
            fulfillment=fulfillment,
            dimensions=str(product.get("dimensions") or ""),
            weight=str(product.get("weight") or ""),
        )

    def build_xray_report(
        self,
        asin_or_url: str,
        *,
        competitor_limit: int = _DEFAULT_COMPETITOR_LIMIT,
    ) -> tuple[Helium10XrayReport, dict[str, Any]]:
        """
        Core Digital Shelf builder.

        1. Fetch target product detail.
        2. Discover competitors via bestseller category (fallback: keyword search).
        3. Fetch all competitor products in parallel.
        4. Build Helium10XrayReport with BSR-estimated units/revenue.

        Returns:
            (xray_report, target_raw) — the report ready for the deck pipeline,
            and the raw Rainforest product response for the target (so the caller
            can populate hero_product enrichment without a second API call).
        """
        if not self.api_key:
            raise RuntimeError("RAINFOREST_API_KEY is not configured.")

        target_asin = _normalize_asin(asin_or_url)
        if not target_asin:
            raise RuntimeError(f"Could not extract a valid Amazon ASIN from {asin_or_url!r}.")

        # 1. Target product
        target_data = self.get_product(target_asin)
        target_product = target_data.get("product") or {}

        # 2. Discover competitor ASINs
        competitor_asins = self._competitor_asins_from_bestsellers(
            target_product, target_asin, limit=competitor_limit
        )
        if not competitor_asins:
            competitor_asins = self._competitor_asins_from_search(
                target_product, target_asin, limit=competitor_limit
            )

        if not competitor_asins:
            logger.warning("No competitor ASINs found for %s; report will be empty.", target_asin)

        # 3. Parallel product fetches
        def _safe_fetch(asin: str) -> dict[str, Any] | None:
            try:
                return self.get_product(asin)
            except Exception as exc:
                logger.warning("Rainforest product fetch failed for %s: %s", asin, exc)
                return None

        competitor_raw: list[dict[str, Any]] = []
        if competitor_asins:
            with concurrent.futures.ThreadPoolExecutor(max_workers=_MAX_CONCURRENT_FETCHES) as pool:
                futures = {pool.submit(_safe_fetch, a): a for a in competitor_asins}
                for fut in concurrent.futures.as_completed(futures):
                    result = fut.result()
                    if result:
                        competitor_raw.append(result)

        # 4. Convert to XrayProduct
        products: list[XrayProduct] = []
        for i, raw in enumerate(competitor_raw[:competitor_limit]):
            xp = self._product_to_xray(raw, display_order=i + 1)
            if xp:
                products.append(xp)

        # Sort by BSR ascending (better rank = higher on list)
        products.sort(key=lambda p: (p.bsr or 999_999, p.display_order))
        products = [
            dataclasses.replace(p, display_order=i + 1)
            for i, p in enumerate(products)
        ]

        # 5. Aggregate report fields
        total_revenue = sum(p.revenue for p in products if p.revenue)
        total_units = sum(p.units_sold for p in products if p.units_sold)
        bsrs = [p.bsr for p in products if p.bsr]
        prices = [p.price for p in products if p.price]
        ratings = [p.rating for p in products if p.rating]

        distinct_brands = len({(p.brand or "").lower() for p in products if p.brand})

        # Real fulfillment distribution from the parsed FBA/FBM labels.
        # Seller "country of origin" is NOT reliably available from Rainforest,
        # so we leave it empty (the deck suppresses empty donuts) rather than
        # fabricate "100% US". Size tier is likewise left to real data only.
        fulfillment_counts: dict[str, int] = {}
        for p in products:
            if p.fulfillment:
                fulfillment_counts[p.fulfillment] = fulfillment_counts.get(p.fulfillment, 0) + 1
        _ff_total = sum(fulfillment_counts.values())
        fulfillment_dist = (
            [
                DistributionSlice(label, count, count / _ff_total)
                for label, count in sorted(fulfillment_counts.items(), key=lambda kv: -kv[1])
            ]
            if _ff_total
            else []
        )
        seller_dist: list[DistributionSlice] = []

        # Honest sourcing note: how many listings carried Amazon's real
        # "bought in past month" badge vs. fell back to a BSR estimate.
        real_units_count = sum(
            1 for raw in competitor_raw
            if _parse_recent_sales((raw.get("product") or {}).get("recent_sales")) is not None
        )
        if real_units_count and real_units_count == len(products):
            _sales_warning = (
                "Unit/revenue figures use Amazon's real \"bought in past month\" "
                "data for every listing (a reported floor)."
            )
        elif real_units_count:
            _sales_warning = (
                f"Unit/revenue figures use Amazon's real \"bought in past month\" "
                f"data where available ({real_units_count} of {len(products)} listings); "
                f"the rest are BSR-based estimates."
            )
        else:
            _sales_warning = (
                "Sales estimates are BSR-based (Amazon did not expose a "
                "\"bought in past month\" figure for these listings)."
            )

        xray_report = Helium10XrayReport(
            products=products,
            total_revenue=total_revenue,
            total_units_sold=total_units,
            average_bsr=sum(bsrs) / len(bsrs) if bsrs else None,
            average_price=sum(prices) / len(prices) if prices else None,
            average_rating=sum(ratings) / len(ratings) if ratings else None,
            search_results_count=len(products),
            revenue_over_5000_count=sum(1 for p in products if (p.revenue or 0) > 5_000),
            under_75_reviews_count=sum(1 for p in products if (p.review_count or 0) < 75),
            seller_country_distribution=seller_dist,
            size_tier_distribution=[],
            fulfillment_distribution=fulfillment_dist,
            warnings=[_sales_warning],
            distinct_brand_count=distinct_brands,
        )

        return xray_report, target_data
