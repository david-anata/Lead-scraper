"""Product enrichment helpers for automation-first deck generation."""

from __future__ import annotations

import html as html_lib
import json
import re
from dataclasses import dataclass
from typing import Any

import requests

from sales_support_agent.integrations.amazon_sp_api import AmazonCatalogSnapshot, AmazonSpApiClient
from sales_support_agent.integrations.shopify import ShopifyProductSnapshot, ShopifyStorefrontClient


@dataclass(frozen=True)
class EnrichedHeroProduct:
    brand_name: str
    title: str
    source_url: str
    description: str
    price: str
    dimensions: str
    image_url: str
    product_type: str
    tags: tuple[str, ...]
    warnings: tuple[str, ...]


@dataclass(frozen=True)
class EnrichedCompetitorProduct:
    name: str
    identifier: str
    source_url: str
    asin: str
    brand: str
    category: str
    bsr: str
    estimated_sales: str
    estimated_units: str
    dimensions: str
    package_dimensions: str
    strength: str
    gap: str
    warnings: tuple[str, ...]


class ProductResearchService:
    def __init__(
        self,
        *,
        shopify_client: ShopifyStorefrontClient,
        amazon_client: AmazonSpApiClient,
    ) -> None:
        self.shopify_client = shopify_client
        self.amazon_client = amazon_client

    def enrich_hero_product(self, product_url: str) -> EnrichedHeroProduct:
        warnings: list[str] = []
        snapshot = self.shopify_client.fetch_product(product_url)
        if not snapshot.description:
            warnings.append("Shopify description was empty, so the hero-product slide will need a manually written summary.")
        if not snapshot.price:
            warnings.append("Shopify price was unavailable from storefront data.")
        return EnrichedHeroProduct(
            brand_name=snapshot.brand_name,
            title=snapshot.title,
            source_url=snapshot.source_url,
            description=snapshot.description,
            price=snapshot.price,
            dimensions="Pending SP-API enrichment",
            image_url=snapshot.image_url,
            product_type=snapshot.product_type,
            tags=snapshot.tags,
            warnings=tuple(warnings),
        )

    def enrich_target_product(self, target: dict[str, str]) -> EnrichedHeroProduct:
        source_type = target.get("source_type", "")
        if source_type == "shopify":
            try:
                return self.enrich_hero_product(target.get("source_url", ""))
            except Exception as exc:
                return EnrichedHeroProduct(
                    brand_name=target.get("brand_name", ""),
                    title=target.get("product_name", ""),
                    source_url=target.get("source_url", ""),
                    description="",
                    price="",
                    dimensions="",
                    image_url="",
                    product_type="",
                    tags=(),
                    warnings=(f"Shopify enrichment failed for {target.get('source_url', '')}: {exc}",),
                )
        if source_type == "amazon":
            return self._enrich_amazon_target(target)
        raise RuntimeError("Target product must be a Shopify product URL or an Amazon ASIN/URL.")

    def enrich_competitor_product(self, competitor: dict[str, str]) -> EnrichedCompetitorProduct:
        warnings: list[str] = []
        catalog: AmazonCatalogSnapshot | None = None
        if competitor.get("asin") and self.amazon_client.is_configured():
            try:
                catalog = self.amazon_client.get_catalog_item(
                    competitor["asin"],
                    source_url=competitor.get("source_url", ""),
                )
            except Exception as exc:
                warnings.append(f"Amazon SP-API enrichment failed for {competitor.get('asin')}: {exc}")
        elif competitor.get("asin"):
            warnings.append("Amazon SP-API credentials are not configured, so competitor enrichment is limited.")
        else:
            warnings.append("Competitor input did not include a valid ASIN, so Amazon catalog enrichment was skipped.")

        name = catalog.title if catalog and catalog.title else competitor.get("name", "")
        category = catalog.category if catalog else ""
        bsr = catalog.bsr if catalog else ""
        dimensions = catalog.dimensions if catalog else ""
        package_dimensions = catalog.package_dimensions if catalog else ""
        estimated_units = _estimate_units_from_bsr(bsr)
        estimated_sales = _estimate_sales_from_bsr(bsr)

        return EnrichedCompetitorProduct(
            name=name,
            identifier=competitor.get("identifier", ""),
            source_url=(catalog.source_url if catalog else competitor.get("source_url", "")),
            asin=competitor.get("asin", ""),
            brand=(catalog.brand if catalog else ""),
            category=category,
            bsr=bsr,
            estimated_sales=estimated_sales,
            estimated_units=estimated_units,
            dimensions=dimensions,
            package_dimensions=package_dimensions,
            strength=_build_strength(name, bsr, category),
            gap=_build_gap(catalog),
            warnings=tuple(warnings),
        )

    def _enrich_amazon_target(self, target: dict[str, str]) -> EnrichedHeroProduct:
        warnings: list[str] = []
        asin = target.get("asin", "")
        if not asin:
            raise RuntimeError("Amazon target product input did not include a valid ASIN.")
        catalog: AmazonCatalogSnapshot | None = None
        page_data = _fetch_amazon_page_data(target.get("source_url", "") or f"https://www.amazon.com/dp/{asin}")
        warnings.extend(page_data.get("warnings", []))
        if self.amazon_client.is_configured():
            try:
                catalog = self.amazon_client.get_catalog_item(asin, source_url=target.get("source_url", ""))
            except Exception as exc:
                warnings.append(f"Amazon catalog enrichment failed for {asin}: {exc}")
        try:
            source_url = (catalog.source_url if catalog else "") or page_data.get("source_url", "") or target.get("source_url", "")
            title = (page_data.get("title", "") or (catalog.title if catalog else "") or target.get("product_name", "")).strip()
            brand_name = (page_data.get("brand_name", "") or (catalog.brand if catalog else "") or target.get("brand_name", "")).strip()
            description = (page_data.get("description", "") or "").strip()
            price = (page_data.get("price", "") or "").strip()
            dimensions = (
                (catalog.dimensions if catalog else "")
                or (catalog.package_dimensions if catalog else "")
                or page_data.get("dimensions", "")
                or ""
            ).strip()
            image_url = (page_data.get("image_url", "") or "").strip()
            product_type = ((catalog.category if catalog else "") or page_data.get("category", "") or "").strip()
        except Exception as exc:
            raise RuntimeError(f"Amazon target-product enrichment failed for {asin}: {exc}") from exc

        if catalog and not catalog.bsr:
            warnings.append("Amazon target product returned no BSR.")
        if not image_url:
            warnings.append("Amazon target product image was unavailable from the product page.")
        if not price:
            warnings.append("Amazon target product price was unavailable from the product page.")

        return EnrichedHeroProduct(
            brand_name=brand_name,
            title=title,
            source_url=source_url,
            description=description,
            price=price,
            dimensions=dimensions,
            image_url=image_url,
            product_type=product_type,
            tags=(),
            warnings=tuple(warnings),
        )


def _estimate_units_from_bsr(bsr: str) -> str:
    if not bsr:
        return ""
    try:
        rank = max(float(str(bsr).replace(",", "")), 1.0)
    except ValueError:
        return ""
    units = max(int(round(75000 / rank)), 1)
    return str(units)


def _estimate_sales_from_bsr(bsr: str) -> str:
    units = _estimate_units_from_bsr(bsr)
    if not units:
        return ""
    try:
        revenue = int(units) * 25
    except ValueError:
        return ""
    return f"${revenue:,}"


def _build_strength(name: str, bsr: str, category: str) -> str:
    if bsr and category:
        return f"{name} is visible in {category} with a current rank signal of {bsr}. Use it as a benchmark for demand and positioning."
    if bsr:
        return f"{name} has a current rank signal of {bsr}. Use it as a benchmark for demand and positioning."
    return f"Use {name} as a live benchmark once rank, reviews, and price are confirmed."


def _build_gap(catalog: AmazonCatalogSnapshot | None) -> str:
    if catalog is None:
        return "Catalog data is incomplete. Validate title quality, creative depth, and conversion proof manually."
    if not catalog.dimensions:
        return "Catalog dimensions are missing. Validate pack size, positioning, and PDP content manually."
    return "Use the catalog details to compare claim clarity, content depth, and conversion proof against the hero product."


def _fetch_amazon_page_data(source_url: str) -> dict[str, Any]:
    if not source_url:
        return {"warnings": ["Amazon target product URL was missing."]}
    warnings: list[str] = []
    try:
        response = requests.get(
            source_url,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=20,
        )
        response.raise_for_status()
    except Exception as exc:
        return {
            "source_url": source_url,
            "warnings": [f"Amazon product-page enrichment failed for {source_url}: {exc}"],
        }

    content = response.text or ""
    title = _extract_first(
        content,
        r'id="productTitle"[^>]*>\s*(.*?)\s*</span>',
        r"<title>\s*(.*?)\s*</title>",
    )
    title = html_lib.unescape(re.sub(r"\s+", " ", title)).replace(": Amazon.com", "").strip()
    image_url = _extract_first(
        content,
        r'<meta\s+property="og:image"\s+content="([^"]+)"',
        r'"hiRes":"([^"]+)"',
        r'"large":"([^"]+)"',
    ).replace("\\u0026", "&").replace("\\/", "/")
    price = _extract_first(
        content,
        r'<span class="a-offscreen">\s*([$][^<]+)\s*</span>',
        r'"priceAmount":"([^"]+)"',
    ).strip()
    if price and not price.startswith("$") and re.fullmatch(r"\d+(\.\d+)?", price):
        price = f"${price}"
    description = html_lib.unescape(
        _extract_first(
            content,
            r'<div id="feature-bullets"[^>]*>(.*?)</div>',
            r'<meta\s+name="description"\s+content="([^"]+)"',
        )
    )
    description = re.sub(r"<[^>]+>", " ", description)
    description = re.sub(r"\s+", " ", description).strip()
    brand_name = html_lib.unescape(
        _extract_first(
            content,
            r'id="bylineInfo"[^>]*>\s*(.*?)\s*</a>',
            r'"brand":"([^"]+)"',
        )
    ).replace("Visit the ", "").replace(" Store", "").strip()
    category = html_lib.unescape(_extract_first(content, r'"productGroup":"([^"]+)"')).strip()
    dimensions = html_lib.unescape(_extract_first(content, r'"itemDimensions":"([^"]+)"')).strip()
    if not title:
        warnings.append("Amazon product page did not expose a parseable title.")
    return {
        "source_url": source_url,
        "title": title,
        "image_url": image_url,
        "price": price,
        "description": description,
        "brand_name": brand_name,
        "category": category,
        "dimensions": dimensions,
        "warnings": warnings,
    }


def _extract_first(content: str, *patterns: str) -> str:
    for pattern in patterns:
        match = re.search(pattern, content, flags=re.IGNORECASE | re.DOTALL)
        if match:
            return str(match.group(1) or "").strip()
    return ""
