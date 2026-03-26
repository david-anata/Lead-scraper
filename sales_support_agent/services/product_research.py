"""Product enrichment helpers for automation-first deck generation."""

from __future__ import annotations

import html as html_lib
import json
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qs, quote_plus, unquote, urlparse

import requests

from sales_support_agent.integrations.amazon_sp_api import AmazonCatalogSnapshot, AmazonSpApiClient
from sales_support_agent.integrations.shopify import ShopifyProductSnapshot, ShopifyStorefrontClient


@dataclass(frozen=True)
class EnrichedHeroProduct:
    asin: str
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
        amazon_reference = _fetch_public_amazon_reference(_build_public_product_query(snapshot.title, snapshot.brand_name))
        if not snapshot.description:
            warnings.append("Shopify description was empty, so the hero-product slide will need a manually written summary.")
        if not snapshot.price:
            warnings.append("Shopify price was unavailable from storefront data.")
        return EnrichedHeroProduct(
            asin=amazon_reference.get("asin", ""),
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
                    asin="",
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
        if source_type == "website":
            return self._enrich_generic_target(target)
        if source_type == "amazon":
            return self._enrich_amazon_target(target)
        raise RuntimeError("Target product must be a website URL, Shopify product URL, or Amazon ASIN/URL.")

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
        search_data = _fetch_amazon_search_data(asin)
        public_data = _fetch_public_asin_fallback(asin)
        warnings.extend(page_data.get("warnings", []))
        if self.amazon_client.is_configured():
            try:
                catalog = self.amazon_client.get_catalog_item(asin, source_url=target.get("source_url", ""))
            except Exception as exc:
                warnings.append(f"Amazon catalog enrichment failed for {asin}: {exc}")
        try:
            resolved_catalog_title = _clean_scraped_text(catalog.title if catalog else "")
            source_url = (
                (catalog.source_url if catalog else "")
                or page_data.get("source_url", "")
                or public_data.get("source_url", "")
                or target.get("source_url", "")
            )
            title = (
                resolved_catalog_title
                or page_data.get("title", "")
                or search_data.get("title", "")
                or public_data.get("title", "")
                or target.get("product_name", "")
            ).strip()
            brand_name = (
                _clean_scraped_text(catalog.brand if catalog else "")
                or page_data.get("brand_name", "")
                or search_data.get("brand_name", "")
                or public_data.get("brand_name", "")
                or target.get("brand_name", "")
                or _infer_brand_from_title(title)
            ).strip()
            description = (page_data.get("description", "") or "").strip()
            price = (page_data.get("price", "") or "").strip()
            dimensions = (
                (catalog.dimensions if catalog else "")
                or (catalog.package_dimensions if catalog else "")
                or page_data.get("dimensions", "")
                or ""
            ).strip()
            image_url = (page_data.get("image_url", "") or search_data.get("image_url", "") or "").strip()
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
            asin=asin,
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

    def _enrich_generic_target(self, target: dict[str, str]) -> EnrichedHeroProduct:
        source_url = target.get("source_url", "")
        if not source_url:
            raise RuntimeError("Target product URL was missing.")
        page_data = _fetch_generic_page_data(source_url)
        amazon_reference = _fetch_public_amazon_reference(
            _build_public_product_query(
                page_data.get("title", "") or target.get("product_name", ""),
                page_data.get("brand_name", "") or target.get("brand_name", ""),
            )
        )
        warnings = list(page_data.get("warnings", []))
        return EnrichedHeroProduct(
            asin=amazon_reference.get("asin", ""),
            brand_name=(page_data.get("brand_name", "") or target.get("brand_name", "")).strip(),
            title=(page_data.get("title", "") or target.get("product_name", "")).strip(),
            source_url=page_data.get("source_url", "") or source_url,
            description=(page_data.get("description", "") or "").strip(),
            price=(page_data.get("price", "") or "").strip(),
            dimensions="",
            image_url=(page_data.get("image_url", "") or "").strip(),
            product_type=(page_data.get("category", "") or "").strip(),
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
    if _is_amazon_block_page(content):
        return {
            "source_url": source_url,
            "warnings": ["Amazon product page returned an anti-bot response, so public-page enrichment was skipped."],
        }
    title = _extract_first(
        content,
        r'<meta\s+property="og:title"\s+content="([^"]+)"',
        r'"title"\s*:\s*"([^"]+)"',
        r'"name"\s*:\s*"([^"]+)"',
        r'id="productTitle"[^>]*>\s*(.*?)\s*</span>',
        r"<title>\s*(.*?)\s*</title>",
    )
    title = _clean_scraped_text(title).replace(": Amazon.com", "").strip()
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
    description = _clean_scraped_text(description)
    brand_name = _clean_scraped_text(
        _extract_first(
            content,
            r'id="bylineInfo"[^>]*>\s*(.*?)\s*</a>',
            r'"brand":"([^"]+)"',
        )
    ).replace("Visit the ", "").replace(" Store", "").strip()
    category = _clean_scraped_text(_extract_first(content, r'"productGroup":"([^"]+)"'))
    dimensions = _clean_scraped_text(_extract_first(content, r'"itemDimensions":"([^"]+)"'))
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


def _fetch_amazon_search_data(asin: str) -> dict[str, str]:
    if not asin:
        return {}
    try:
        response = requests.get(
            f"https://www.amazon.com/s?k={asin}",
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=20,
        )
        response.raise_for_status()
    except Exception:
        return {}
    content = response.text or ""
    if _is_amazon_block_page(content):
        return {}
    title = _clean_scraped_text(
        _extract_first(
            content,
            r'<img[^>]+class="s-image"[^>]+alt="([^"]+)"',
            r'<h2[^>]*>\s*<a[^>]*>\s*<span[^>]*>(.*?)</span>',
        )
    )
    image_url = _extract_first(
        content,
        r'<img[^>]+class="s-image"[^>]+src="([^"]+)"',
    ).replace("\\u0026", "&").replace("\\/", "/")
    return {
        "title": title,
        "brand_name": _infer_brand_from_title(title),
        "image_url": image_url,
    }


def _infer_brand_from_title(title: str) -> str:
    cleaned = _clean_scraped_text(title)
    if not cleaned:
        return ""
    token = cleaned.split()[0]
    token = re.sub(r"[^A-Za-z0-9&'-]", "", token).strip()
    if not token or len(token) <= 1:
        return ""
    return token


def _fetch_public_asin_fallback(asin: str) -> dict[str, str]:
    return _fetch_public_amazon_reference(asin)


def _fetch_public_amazon_reference(query: str) -> dict[str, str]:
    cleaned_query = _clean_scraped_text(query)
    if not cleaned_query:
        return {}
    try:
        response = requests.get(
            f"https://duckduckgo.com/html/?q={quote_plus(cleaned_query)}",
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=20,
        )
        response.raise_for_status()
    except Exception:
        return {}

    best: dict[str, str] = {}
    best_score = -10**9
    for raw_href, raw_title in re.findall(r'<a rel="nofollow" class="result__a" href="([^"]+)">(.*?)</a>', response.text or ""):
        resolved_url = _resolve_duckduckgo_link(raw_href)
        if "amazon." not in resolved_url:
            continue
        title = _clean_public_amazon_result_title(raw_title)
        if not title:
            continue
        asin = _extract_asin_from_text(resolved_url)
        score = _title_score(title) + _query_match_score(cleaned_query, title)
        if asin:
            score += 25
        if score <= best_score:
            continue
        best_score = score
        best = {
            "asin": asin,
            "title": title,
            "brand_name": _infer_brand_from_title(title),
            "source_url": resolved_url,
        }
    return best


def _resolve_duckduckgo_link(value: str) -> str:
    raw = html_lib.unescape(str(value or "")).strip()
    if not raw:
        return ""
    if raw.startswith("//"):
        raw = f"https:{raw}"
    parsed = urlparse(raw)
    if "duckduckgo.com" in (parsed.netloc or ""):
        redirect = parse_qs(parsed.query).get("uddg", [""])[0]
        if redirect:
            return unquote(redirect)
    return raw


def _build_public_product_query(title: str, brand_name: str) -> str:
    pieces = [piece.strip() for piece in (brand_name, title) if piece and piece.strip()]
    if not pieces:
        return ""
    return f"{' '.join(pieces)} site:amazon.com"


def _clean_public_amazon_result_title(value: str) -> str:
    cleaned = _clean_scraped_text(re.sub(r"<.*?>", " ", html_lib.unescape(str(value or ""))))
    cleaned = re.sub(r"^\s*Amazon\.com:\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*-\s*Amazon(?:\.[A-Za-z.]+)?\s*$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" -")
    if re.fullmatch(r"(ASIN\s+)?[A-Z0-9]{10}", cleaned, flags=re.IGNORECASE):
        return ""
    return cleaned


def _title_score(value: str) -> int:
    cleaned = _clean_scraped_text(value)
    if not cleaned:
        return 0
    score = len(cleaned)
    if "Amazon.com:" in value:
        score -= 25
    if re.search(r"\b[A-Z][A-Za-z0-9&'-]{2,}\b", cleaned):
        score += 10
    if "..." in cleaned:
        score -= 5
    return score


def _query_match_score(query: str, title: str) -> int:
    query_tokens = {token for token in re.findall(r"[a-z0-9]+", query.lower()) if len(token) > 2 and token != "amazon"}
    title_tokens = {token for token in re.findall(r"[a-z0-9]+", title.lower()) if len(token) > 2}
    if not query_tokens or not title_tokens:
        return 0
    shared = len(query_tokens & title_tokens)
    return shared * 8


def _extract_asin_from_text(value: str) -> str:
    match = re.search(r"\b([A-Z0-9]{10})\b", str(value or "").upper())
    return match.group(1) if match else ""


def _is_amazon_block_page(content: str) -> bool:
    cleaned = str(content or "")
    lower = cleaned.lower()
    return (
        "opfcaptcha" in lower
        or "automated access to amazon data" in lower
        or "sorry! something went wrong!" in lower
        or "<title dir=\"ltr\">amazon.com</title>" in lower
    )


def _fetch_generic_page_data(source_url: str) -> dict[str, Any]:
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
            "warnings": [f"Website target-page enrichment failed for {source_url}: {exc}"],
        }

    content = response.text or ""
    title = _clean_scraped_text(
        _extract_first(
            content,
            r'<meta\s+property="og:title"\s+content="([^"]+)"',
            r"<title>\s*(.*?)\s*</title>",
            r"<h1[^>]*>\s*(.*?)\s*</h1>",
        )
    )
    image_url = _extract_first(
        content,
        r'<meta\s+property="og:image"\s+content="([^"]+)"',
        r'"image":"([^"]+)"',
    ).replace("\\u0026", "&").replace("\\/", "/")
    description = _clean_scraped_text(
        _extract_first(
            content,
            r'<meta\s+name="description"\s+content="([^"]+)"',
            r'<meta\s+property="og:description"\s+content="([^"]+)"',
            r'"description":"([^"]+)"',
        )
    )
    brand_name = _clean_scraped_text(
        _extract_first(
            content,
            r'<meta\s+property="og:site_name"\s+content="([^"]+)"',
            r'"brand"\s*:\s*"([^"]+)"',
        )
    )
    price = _clean_scraped_text(
        _extract_first(
            content,
            r'<meta\s+property="product:price:amount"\s+content="([^"]+)"',
            r'"price"\s*:\s*"([^"]+)"',
            r'[$]\s?(\d+(?:\.\d{2})?)',
        )
    )
    if price and not price.startswith("$") and re.fullmatch(r"\d+(?:\.\d{2})?", price):
        price = f"${price}"
    category = _clean_scraped_text(_extract_first(content, r'"category"\s*:\s*"([^"]+)"'))
    if not title:
        warnings.append("Website target page did not expose a parseable title.")
    return {
        "source_url": source_url,
        "title": title,
        "image_url": image_url,
        "price": price,
        "description": description,
        "brand_name": brand_name,
        "category": category,
        "warnings": warnings,
    }


def _extract_first(content: str, *patterns: str) -> str:
    for pattern in patterns:
        match = re.search(pattern, content, flags=re.IGNORECASE | re.DOTALL)
        if match:
            return str(match.group(1) or "").strip()
    return ""


def _clean_scraped_text(value: str) -> str:
    cleaned = html_lib.unescape(str(value or ""))
    cleaned = re.sub(r"<!--.*?-->", " ", cleaned, flags=re.DOTALL)
    cleaned = re.sub(r"<script[^>]*>.*?</script>", " ", cleaned, flags=re.IGNORECASE | re.DOTALL)
    cleaned = re.sub(r"<style[^>]*>.*?</style>", " ", cleaned, flags=re.IGNORECASE | re.DOTALL)
    cleaned = re.sub(r"<[^>]+>", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned
