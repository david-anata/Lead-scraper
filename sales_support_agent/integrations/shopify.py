"""Minimal Shopify storefront client for deck enrichment."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

import requests

from sales_support_agent.config import Settings


@dataclass(frozen=True)
class ShopifyProductSnapshot:
    source_url: str
    domain: str
    handle: str
    brand_name: str
    title: str
    description: str
    price: str
    currency: str
    image_url: str
    product_type: str
    tags: tuple[str, ...]
    vendor: str


class ShopifyStorefrontClient:
    def __init__(self, settings: Settings):
        self.settings = settings

    def fetch_product(self, product_url: str) -> ShopifyProductSnapshot:
        parsed = self._parse_product_url(product_url)
        if not parsed["domain"] or not parsed["handle"]:
            raise RuntimeError("Shopify product URL must include a /products/{handle} path.")

        payload = self._fetch_product_json(parsed["domain"], parsed["handle"])
        if payload is None:
            payload = self._fetch_product_json_ld(parsed["source_url"])
        if payload is None:
            raise RuntimeError("Could not extract Shopify product data from the provided URL.")

        return ShopifyProductSnapshot(
            source_url=parsed["source_url"],
            domain=parsed["domain"],
            handle=parsed["handle"],
            brand_name=str(payload.get("vendor") or parsed["brand_name"] or "Brand").strip(),
            title=str(payload.get("title") or parsed["product_name"] or "Hero Product").strip(),
            description=_strip_html(str(payload.get("body_html") or payload.get("description") or "")).strip(),
            price=str(payload.get("price") or payload.get("price_text") or "").strip(),
            currency=str(payload.get("currency") or "").strip(),
            image_url=str(payload.get("image_url") or "").strip(),
            product_type=str(payload.get("product_type") or "").strip(),
            tags=tuple(payload.get("tags") or ()),
            vendor=str(payload.get("vendor") or "").strip(),
        )

    def _fetch_product_json(self, domain: str, handle: str) -> dict[str, Any] | None:
        response = requests.get(
            f"https://{domain}/products/{handle}.js",
            headers={"User-Agent": self.settings.shopify_user_agent, "Accept": "application/json"},
            timeout=self.settings.shopify_request_timeout_seconds,
        )
        if not response.ok or not response.content:
            return None
        payload = response.json()
        variant = dict((payload.get("variants") or [{}])[0] or {})
        image = payload.get("featured_image") or payload.get("image") or ""
        image_url = ""
        if isinstance(image, dict):
            image_url = str(image.get("src") or image.get("url") or "").strip()
        elif isinstance(image, str):
            image_url = image.strip()
        price_text = ""
        price_value = variant.get("price") or payload.get("price")
        if price_value not in (None, ""):
            try:
                price_text = f"${float(price_value) / 100:.2f}" if isinstance(price_value, int) or str(price_value).isdigit() else str(price_value)
            except Exception:
                price_text = str(price_value)
        tags = payload.get("tags")
        normalized_tags: tuple[str, ...]
        if isinstance(tags, str):
            normalized_tags = tuple(tag.strip() for tag in tags.split(",") if tag.strip())
        elif isinstance(tags, list):
            normalized_tags = tuple(str(tag).strip() for tag in tags if str(tag).strip())
        else:
            normalized_tags = ()
        return {
            "title": payload.get("title"),
            "body_html": payload.get("description") or payload.get("body_html"),
            "price": price_text,
            "currency": variant.get("currency") or "",
            "image_url": image_url,
            "product_type": payload.get("type") or payload.get("product_type") or "",
            "tags": normalized_tags,
            "vendor": payload.get("vendor") or "",
        }

    def _fetch_product_json_ld(self, source_url: str) -> dict[str, Any] | None:
        response = requests.get(
            source_url,
            headers={"User-Agent": self.settings.shopify_user_agent},
            timeout=self.settings.shopify_request_timeout_seconds,
        )
        if not response.ok or not response.text:
            return None
        matches = re.findall(
            r'<script[^>]+type="application/ld\+json"[^>]*>\s*(.*?)\s*</script>',
            response.text,
            flags=re.IGNORECASE | re.DOTALL,
        )
        for raw_block in matches:
            try:
                payload = json.loads(raw_block)
            except json.JSONDecodeError:
                continue
            candidates = payload if isinstance(payload, list) else [payload]
            for candidate in candidates:
                if not isinstance(candidate, dict):
                    continue
                if str(candidate.get("@type") or "").lower() != "product":
                    continue
                offers = candidate.get("offers") or {}
                if isinstance(offers, list):
                    offers = offers[0] if offers else {}
                image = candidate.get("image") or ""
                image_url = image[0] if isinstance(image, list) and image else image
                return {
                    "title": candidate.get("name"),
                    "description": candidate.get("description"),
                    "price_text": str((offers or {}).get("price") or "").strip(),
                    "currency": str((offers or {}).get("priceCurrency") or "").strip(),
                    "image_url": str(image_url or "").strip(),
                    "product_type": "",
                    "tags": (),
                    "vendor": str(candidate.get("brand", {}).get("name") if isinstance(candidate.get("brand"), dict) else candidate.get("brand") or "").strip(),
                }
        return None

    @staticmethod
    def _parse_product_url(product_url: str) -> dict[str, str]:
        cleaned = str(product_url or "").strip()
        if not cleaned:
            return {"source_url": "", "domain": "", "handle": "", "brand_name": "", "product_name": ""}
        parsed = urlparse(cleaned if "://" in cleaned else f"https://{cleaned}")
        domain = (parsed.netloc or parsed.path).strip().lower().split("/")[0]
        path = parsed.path or ""
        handle_match = re.search(r"/products/([^/?#]+)", path, flags=re.IGNORECASE)
        handle = handle_match.group(1).strip() if handle_match else ""
        brand_token = domain.split(".")[-2] if len(domain.split(".")) >= 2 else domain
        brand_name = _titleize_slug(brand_token) or "Brand"
        product_name = _titleize_slug(handle) or f"{brand_name} Hero Product"
        source_url = cleaned if "://" in cleaned else f"https://{cleaned}"
        return {
            "source_url": source_url,
            "domain": domain,
            "handle": handle,
            "brand_name": brand_name,
            "product_name": product_name,
        }


def _strip_html(value: str) -> str:
    return re.sub(r"<[^>]+>", " ", str(value or "")).replace("&nbsp;", " ").strip()


def _titleize_slug(value: str) -> str:
    cleaned = re.sub(r"[_\-]+", " ", str(value or "").strip())
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if not cleaned:
        return ""
    return " ".join(token.capitalize() for token in cleaned.split(" "))
