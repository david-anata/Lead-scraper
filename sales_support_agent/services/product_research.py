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


@dataclass(frozen=True)
class EnrichedHeroProduct:
    asin: str
    candidate_asin: str
    brand_name: str
    title: str
    source_url: str
    description: str
    price: str
    dimensions: str
    image_url: str
    product_type: str
    bsr: float | None
    rating: float | None
    review_count: int | None
    identity_source: str
    market_metrics_source: str
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
        amazon_client: AmazonSpApiClient,
    ) -> None:
        self.amazon_client = amazon_client

    def enrich_target_product(self, target: dict[str, str]) -> EnrichedHeroProduct:
        source_type = target.get("source_type", "")
        if source_type == "website":
            return self._enrich_generic_target(target)
        if source_type == "amazon":
            return self._enrich_amazon_target(target)
        raise RuntimeError("Target product must be an Amazon ASIN/URL or a generic product URL.")

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
        remote_catalog = _fetch_remote_catalog_data(asin)
        page_data = _fetch_amazon_page_data(target.get("source_url", "") or f"https://www.amazon.com/dp/{asin}")
        search_data = _fetch_amazon_search_data(asin)
        public_data = _fetch_public_asin_fallback(asin)
        public_page_data = _fetch_amazon_page_data(public_data.get("source_url", "")) if public_data.get("source_url") else {}
        warnings.extend(page_data.get("warnings", []))
        warnings.extend(public_page_data.get("warnings", []))
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
            title = ""
            identity_source = ""
            for value, source_name in (
                (resolved_catalog_title, "amazon_catalog"),
                (remote_catalog.get("title", ""), "remote_catalog"),
                (page_data.get("title", ""), "amazon_page"),
                (public_page_data.get("title", ""), "public_page"),
                (search_data.get("title", ""), "amazon_search"),
                (public_data.get("title", ""), "public_result"),
                (target.get("product_name", ""), "input"),
            ):
                if str(value or "").strip():
                    title = str(value).strip()
                    identity_source = source_name
                    break
            brand_name = (
                _clean_scraped_text(catalog.brand if catalog else "")
                or remote_catalog.get("brand_name", "")
                or page_data.get("brand_name", "")
                or public_page_data.get("brand_name", "")
                or search_data.get("brand_name", "")
                or public_data.get("brand_name", "")
                or target.get("brand_name", "")
                or _infer_brand_from_title(title)
            ).strip()
            description = (page_data.get("description", "") or public_page_data.get("description", "") or "").strip()
            price = (page_data.get("price", "") or remote_catalog.get("price", "") or public_page_data.get("price", "") or "").strip()
            dimensions = (
                (catalog.dimensions if catalog else "")
                or (catalog.package_dimensions if catalog else "")
                or page_data.get("dimensions", "")
                or remote_catalog.get("dimensions", "")
                or public_page_data.get("dimensions", "")
                or ""
            ).strip()
            image_url = (page_data.get("image_url", "") or search_data.get("image_url", "") or remote_catalog.get("image_url", "") or public_page_data.get("image_url", "") or "").strip()
            product_type = ((catalog.category if catalog else "") or page_data.get("category", "") or remote_catalog.get("category", "") or public_page_data.get("category", "") or "").strip()
            bsr_raw = ""
            bsr_source = ""
            for value, source_name in (
                ((catalog.bsr if catalog else ""), "amazon_catalog"),
                (page_data.get("bsr", ""), "amazon_page"),
                (remote_catalog.get("bsr", ""), "remote_catalog"),
                (public_page_data.get("bsr", ""), "public_page"),
            ):
                if str(value or "").strip():
                    bsr_raw = str(value).strip()
                    bsr_source = source_name
                    break
            bsr = _parse_numeric_value(bsr_raw)
            rating_raw = ""
            rating_source = ""
            for value, source_name in (
                (page_data.get("rating", ""), "amazon_page"),
                (public_page_data.get("rating", ""), "public_page"),
            ):
                if str(value or "").strip():
                    rating_raw = str(value).strip()
                    rating_source = source_name
                    break
            rating = _parse_numeric_value(rating_raw)
            review_raw = ""
            review_source = ""
            for value, source_name in (
                (page_data.get("review_count", ""), "amazon_page"),
                (public_page_data.get("review_count", ""), "public_page"),
            ):
                if str(value or "").strip():
                    review_raw = str(value).strip()
                    review_source = source_name
                    break
            review_count = _parse_int_value(review_raw)
            if identity_source == "remote_catalog" and not page_data.get("title", "") and not any(public_page_data.get(key) for key in ("rating", "review_count")):
                rating = None
                review_count = None
        except Exception as exc:
            raise RuntimeError(f"Amazon target-product enrichment failed for {asin}: {exc}") from exc

        if title and image_url and price:
            warnings = [
                warning
                for warning in warnings
                if "Amazon product page returned an anti-bot response" not in warning
                and "Amazon product page did not expose a parseable title." not in warning
            ]
        if catalog and not catalog.bsr:
            warnings.append("Amazon target product returned no BSR.")
        if not image_url:
            warnings.append("Amazon target product image was unavailable from the product page.")
        if not price:
            warnings.append("Amazon target product price was unavailable from the product page.")

        deduped_warnings = tuple(dict.fromkeys(warnings))
        metric_sources: list[str] = []
        for source_name in (bsr_source, rating_source, review_source):
            if source_name and source_name not in metric_sources:
                metric_sources.append(source_name)
        return EnrichedHeroProduct(
            asin=asin,
            candidate_asin="",
            brand_name=brand_name,
            title=title,
            source_url=source_url,
            description=description,
            price=price,
            dimensions=dimensions,
            image_url=image_url,
            product_type=product_type,
            bsr=bsr,
            rating=rating,
            review_count=review_count,
            identity_source=identity_source or "amazon",
            market_metrics_source="+".join(metric_sources),
            tags=(),
            warnings=deduped_warnings,
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
        candidate_asin = ""
        if _is_verified_public_reference(
            amazon_reference,
            title=page_data.get("title", "") or target.get("product_name", ""),
            brand_name=page_data.get("brand_name", "") or target.get("brand_name", ""),
        ):
            candidate_asin = amazon_reference.get("asin", "")
        return EnrichedHeroProduct(
            asin="",
            candidate_asin=candidate_asin,
            brand_name=(page_data.get("brand_name", "") or target.get("brand_name", "")).strip(),
            title=(page_data.get("title", "") or target.get("product_name", "")).strip(),
            source_url=page_data.get("source_url", "") or source_url,
            description=(page_data.get("description", "") or "").strip(),
            price=(page_data.get("price", "") or "").strip(),
            dimensions="",
            image_url=(page_data.get("image_url", "") or "").strip(),
            product_type=(page_data.get("category", "") or "").strip(),
            bsr=None,
            rating=None,
            review_count=None,
            identity_source="website",
            market_metrics_source="",
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


_AMAZON_DESKTOP_USER_AGENTS: tuple[str, ...] = (
    # Recent Chrome on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    # Recent Chrome on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    # Recent Safari on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    # Recent Firefox on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.5; rv:127.0) Gecko/20100101 Firefox/127.0",
)
_AMAZON_MOBILE_USER_AGENT = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) "
    "Version/17.5 Mobile/15E148 Safari/604.1"
)


def _amazon_browser_headers(user_agent: str, *, referer: str = "https://www.amazon.com/") -> dict[str, str]:
    """Headers that match what a real Chrome/Safari/Firefox sends to Amazon.

    A bare User-Agent is the #1 scraper fingerprint Amazon flags. Sending the
    same Accept / Accept-Language / Sec-Fetch-* / Sec-Ch-Ua quartet a real
    browser would send substantially raises the success rate without paid
    proxies.
    """
    is_chrome = "Chrome/" in user_agent and "Chromium" not in user_agent
    headers = {
        "User-Agent": user_agent,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Upgrade-Insecure-Requests": "1",
        "DNT": "1",
        "Connection": "keep-alive",
        "Referer": referer,
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin" if "amazon.com" in referer else "none",
        "Sec-Fetch-User": "?1",
    }
    if is_chrome:
        headers["Sec-Ch-Ua"] = '"Chromium";v="126", "Not.A/Brand";v="24", "Google Chrome";v="126"'
        headers["Sec-Ch-Ua-Mobile"] = "?1" if "iPhone" in user_agent else "?0"
        headers["Sec-Ch-Ua-Platform"] = (
            '"iOS"' if "iPhone" in user_agent
            else '"Windows"' if "Windows" in user_agent
            else '"macOS"'
        )
    return headers


def _amazon_session_get(url: str, *, attempts: int = 3) -> tuple[str, list[str]]:
    """GET an Amazon URL with browser-realistic headers, rotating UA on each
    retry. Returns (html_content, warnings). Empty content means we gave up.

    Rotation strategy:
      attempt 1: desktop UA (Chrome macOS)
      attempt 2: alternate desktop UA (Chrome Windows)
      attempt 3: mobile Safari → mobile.amazon.com if path supports it
    """
    import random
    warnings: list[str] = []
    session = requests.Session()
    # Warm up cookies by hitting the homepage first — gives us
    # session-id / csm-hit cookies that Amazon expects.
    try:
        session.get(
            "https://www.amazon.com/",
            headers=_amazon_browser_headers(_AMAZON_DESKTOP_USER_AGENTS[0], referer="https://www.google.com/"),
            timeout=10,
        )
    except Exception:
        pass  # Warmup is best-effort; the real fetch can still succeed.

    last_block = False
    for attempt in range(attempts):
        if attempt < 2:
            ua = _AMAZON_DESKTOP_USER_AGENTS[attempt % len(_AMAZON_DESKTOP_USER_AGENTS)]
            target_url = url
        else:
            # Mobile fallback — often less aggressive bot detection.
            ua = _AMAZON_MOBILE_USER_AGENT
            target_url = url.replace("www.amazon.com", "www.amazon.com")  # mobile UA is enough; same URL
        try:
            response = session.get(
                target_url,
                headers=_amazon_browser_headers(ua),
                timeout=20,
                allow_redirects=True,
            )
            response.raise_for_status()
        except Exception as exc:
            warnings.append(f"Amazon page fetch attempt {attempt + 1} failed: {exc}")
            continue
        content = response.text or ""
        if _is_amazon_block_page(content):
            last_block = True
            warnings.append(f"Amazon page fetch attempt {attempt + 1}: anti-bot challenge")
            continue
        return content, warnings
    if last_block:
        warnings.append("All Amazon page fetch attempts hit the anti-bot challenge.")
    return "", warnings


def _fetch_amazon_page_data(source_url: str) -> dict[str, Any]:
    if not source_url:
        return {"warnings": ["Amazon target product URL was missing."]}
    content, fetch_warnings = _amazon_session_get(source_url)
    if not content:
        return {
            "source_url": source_url,
            "warnings": fetch_warnings or [
                "Amazon product page returned an anti-bot response, so public-page enrichment was skipped."
            ],
        }
    warnings: list[str] = list(fetch_warnings)
    title = _extract_first(
        content,
        r'<meta\s+property="og:title"\s+content="([^"]+)"',
        r'<span[^>]+id="productTitle"[^>]*>\s*(.*?)\s*</span>',
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
        r'"priceAmount":\s*([0-9]+(?:\.[0-9]+)?)',
    ).strip()
    if price and not price.startswith("$") and re.fullmatch(r"\d+(\.\d+)?", price):
        price = f"${price}"
    feature_bullets = _clean_scraped_text(
        html_lib.unescape(
            _extract_first(
                content,
                r'<div id="feature-bullets"[^>]*>(.*?)</div>',
            )
        )
    )
    product_description = _clean_scraped_text(
        html_lib.unescape(
            _extract_first(
                content,
                r'<div id="productDescription"[^>]*>(.*?)</div>',
                r'<div id="productDescription_feature_div"[^>]*>(.*?)</div>',
                r'"productDescription":"([^"]+)"',
            )
        )
    )
    meta_description = _clean_scraped_text(
        html_lib.unescape(
            _extract_first(
                content,
                r'<meta\s+name="description"\s+content="([^"]+)"',
            )
        )
    )
    description = _merge_listing_copy_segments(feature_bullets, product_description, meta_description)
    brand_name = _clean_scraped_text(
        _extract_first(
            content,
            r'id="bylineInfo"[^>]*>\s*(.*?)\s*</a>',
            r'"brand":"([^"]+)"',
        )
    ).replace("Visit the ", "").replace(" Store", "").replace("Brand:", "").strip()
    category = _clean_scraped_text(_extract_first(content, r'"productGroup":"([^"]+)"'))
    dimensions = _clean_scraped_text(_extract_first(content, r'"itemDimensions":"([^"]+)"'))
    bsr = _clean_scraped_text(
        _extract_first(
            content,
            r'"rank"\s*:\s*"?(?:#)?([\d,]+)"?',
            r'Best Sellers Rank[^#]*#([\d,]+)',
        )
    )
    rating = _clean_scraped_text(
        _extract_first(
            content,
            r'id="acrPopover"[^>]+title="([^"]+)"',
            r'"rating"\s*:\s*"([^"]+)"',
        )
    )
    rating = _extract_first(rating, r"(\d+(?:\.\d+)?)")
    review_count = _clean_scraped_text(
        _extract_first(
            content,
            r'id="acrCustomerReviewText"[^>]*>\s*([\d,]+)',
            r'acrCustomerReviewText"[^>]+aria-label="([\d,]+)\s+ratings"',
            r'([\d,]+)\s+ratings',
            r'"reviewCount"\s*:\s*"([^"]+)"',
        )
    )
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
        "bsr": bsr,
        "rating": rating,
        "review_count": review_count,
        "warnings": warnings,
    }


def _fetch_remote_catalog_data(asin: str) -> dict[str, str]:
    if not asin:
        return {}
    try:
        response = requests.get(
            f"https://amazon-sp-api-platform.onrender.com/api/amazon/catalog/{asin}",
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                "Accept": "application/json",
            },
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json() if response.content else {}
    except Exception:
        return {}
    dimensions = payload.get("dimensions") or {}
    dimension_parts = []
    for key in ("length", "width", "height"):
        value = dimensions.get(key)
        if value in (None, ""):
            continue
        unit = dimensions.get("unit") or ""
        dimension_parts.append(f"{value} {unit}".strip())
    price = payload.get("buy_box_price") or payload.get("price")
    price_label = ""
    if isinstance(price, (int, float)):
        price_label = f"${price:,.2f}"
    elif price not in (None, ""):
        price_label = str(price).strip()
    images = payload.get("images") or []
    return {
        "title": _clean_scraped_text(str(payload.get("title", "") or "")),
        "brand_name": _clean_scraped_text(str(payload.get("brand", "") or "")),
        "price": price_label,
        "bsr": str(payload.get("bsr") or "").strip(),
        "image_url": str(images[0] if images else ""),
        "category": _clean_scraped_text(str(payload.get("category_label", "") or "")),
        "dimensions": " x ".join(dimension_parts),
    }


def _fetch_amazon_search_data(asin: str) -> dict[str, str]:
    if not asin:
        return {}
    content, _ = _amazon_session_get(f"https://www.amazon.com/s?k={asin}")
    if not content:
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


def _parse_numeric_value(value: str) -> float | None:
    cleaned = _clean_scraped_text(value)
    if not cleaned:
        return None
    match = re.search(r"(\d+(?:,\d{3})*(?:\.\d+)?)", cleaned)
    if not match:
        return None
    try:
        return float(match.group(1).replace(",", ""))
    except ValueError:
        return None


def _parse_int_value(value: str) -> int | None:
    numeric = _parse_numeric_value(value)
    if numeric is None:
        return None
    return int(round(numeric))


def _normalized_identity_tokens(value: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-z0-9]+", _clean_scraped_text(value).lower())
        if len(token) > 2 and token not in {"with", "from", "pack", "amazon", "site", "com"}
    }


def _variant_tokens(value: str) -> set[str]:
    return {
        token
        for token in re.findall(r"(?:\d+(?:\.\d+)?(?:oz|ml|lb|ct|pack|pk|count|inch|in)|\d+)", _clean_scraped_text(value).lower())
        if token
    }


def _is_verified_public_reference(reference: dict[str, str], *, title: str, brand_name: str) -> bool:
    asin = str(reference.get("asin", "") or "").strip()
    ref_title = str(reference.get("title", "") or "").strip()
    ref_brand = str(reference.get("brand_name", "") or "").strip()
    if not asin or not ref_title:
        return False
    input_brand = _clean_scraped_text(brand_name).lower()
    if input_brand:
        normalized_reference_brand = _clean_scraped_text(ref_brand).lower()
        if normalized_reference_brand and normalized_reference_brand != input_brand:
            return False
    input_tokens = _normalized_identity_tokens(title)
    reference_tokens = _normalized_identity_tokens(ref_title)
    if not input_tokens or not reference_tokens:
        return False
    input_variants = _variant_tokens(title)
    reference_variants = _variant_tokens(ref_title)
    if input_variants != reference_variants and (input_variants or reference_variants):
        return False
    overlap = len(input_tokens & reference_tokens)
    required_overlap = max(2, min(len(input_tokens), len(reference_tokens)) // 2)
    return overlap >= required_overlap


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


def _merge_listing_copy_segments(*segments: str) -> str:
    merged: list[str] = []
    seen: set[str] = set()
    for segment in segments:
        cleaned = _clean_scraped_text(segment)
        if not cleaned:
            continue
        normalized = re.sub(r"\s+", " ", cleaned).strip().lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        merged.append(cleaned)
    return "\n".join(merged)


def _clean_scraped_text(value: str) -> str:
    cleaned = html_lib.unescape(str(value or ""))
    cleaned = re.sub(r"<!--.*?-->", " ", cleaned, flags=re.DOTALL)
    cleaned = re.sub(r"<script[^>]*>.*?</script>", " ", cleaned, flags=re.IGNORECASE | re.DOTALL)
    cleaned = re.sub(r"<style[^>]*>.*?</style>", " ", cleaned, flags=re.IGNORECASE | re.DOTALL)
    cleaned = re.sub(r"<[^>]+>", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned
