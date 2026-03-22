"""Amazon SP-API catalog client used for deck enrichment."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests

from sales_support_agent.config import Settings


@dataclass(frozen=True)
class AmazonCatalogSnapshot:
    asin: str
    title: str
    brand: str
    category: str
    bsr: str
    dimensions: str
    package_dimensions: str
    marketplace_id: str
    source_url: str
    raw_payload: dict[str, Any]


class AmazonSpApiClient:
    LWA_TOKEN_URL = "https://api.amazon.com/auth/o2/token"

    def __init__(self, settings: Settings):
        self.settings = settings
        self._cached_access_token = ""

    def is_configured(self) -> bool:
        return bool(
            self.settings.amazon_sp_api_lwa_client_id
            and self.settings.amazon_sp_api_lwa_client_secret
            and self.settings.amazon_sp_api_refresh_token
            and self.settings.amazon_sp_api_marketplace_id
        )

    def get_catalog_item(self, asin: str, *, source_url: str = "") -> AmazonCatalogSnapshot:
        if not self.is_configured():
            raise RuntimeError("Amazon SP-API credentials are not configured.")
        payload = self._request(
            method="GET",
            path=f"/catalog/2022-04-01/items/{asin}",
            query={
                "marketplaceIds": self.settings.amazon_sp_api_marketplace_id,
                "includedData": "summaries,attributes,dimensions,salesRanks",
            },
        )
        summaries = list(payload.get("summaries") or [])
        summary = summaries[0] if summaries else {}
        sales_ranks = payload.get("salesRanks") or []
        if isinstance(sales_ranks, list) and sales_ranks:
            rank_groups = sales_ranks[0].get("classificationRanks") or []
        else:
            rank_groups = []
        bsr_value = ""
        category_name = ""
        if rank_groups:
            first_rank = rank_groups[0]
            bsr_value = str(first_rank.get("rank") or "").strip()
            category_name = str(first_rank.get("title") or "").strip()
        item_dimensions = _extract_dimensions(payload.get("dimensions") or [], dimension_key="item")
        package_dimensions = _extract_dimensions(payload.get("dimensions") or [], dimension_key="package")
        return AmazonCatalogSnapshot(
            asin=asin,
            title=str(summary.get("itemName") or "").strip(),
            brand=str(summary.get("brandName") or "").strip(),
            category=category_name,
            bsr=bsr_value,
            dimensions=item_dimensions,
            package_dimensions=package_dimensions,
            marketplace_id=self.settings.amazon_sp_api_marketplace_id,
            source_url=source_url or f"https://www.amazon.com/dp/{asin}",
            raw_payload=payload,
        )

    def _request(self, *, method: str, path: str, query: dict[str, str]) -> dict[str, Any]:
        access_token = self._get_lwa_access_token()
        amz_date = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        request_headers = {
            "x-amz-access-token": access_token,
            "x-amz-date": amz_date,
            "user-agent": "anata-deck-generator/1.0 (Language=Python)",
        }

        response = requests.request(
            method=method,
            url=f"{self.settings.amazon_sp_api_base_url.rstrip('/')}{path}",
            params=query,
            headers=request_headers,
            timeout=30,
        )
        if not response.ok:
            raise RuntimeError(f"Amazon SP-API request failed ({response.status_code}): {response.text}")
        return response.json() if response.content else {}

    def _get_lwa_access_token(self) -> str:
        if self._cached_access_token:
            return self._cached_access_token
        response = requests.post(
            self.LWA_TOKEN_URL,
            data={
                "grant_type": "refresh_token",
                "refresh_token": self.settings.amazon_sp_api_refresh_token,
                "client_id": self.settings.amazon_sp_api_lwa_client_id,
                "client_secret": self.settings.amazon_sp_api_lwa_client_secret,
            },
            timeout=30,
        )
        if not response.ok:
            raise RuntimeError(f"Amazon LWA token refresh failed ({response.status_code}): {response.text}")
        payload = response.json()
        token = str(payload.get("access_token") or "").strip()
        if not token:
            raise RuntimeError(f"Amazon LWA token refresh returned no access token: {json.dumps(payload)}")
        self._cached_access_token = token
        return token


def _extract_dimensions(dimension_sets: list[dict[str, Any]], *, dimension_key: str) -> str:
    for candidate in dimension_sets:
        if not isinstance(candidate, dict):
            continue
        dimensions = candidate.get(dimension_key) or {}
        if not isinstance(dimensions, dict):
            continue
        width = _dimension_value(dimensions.get("width"))
        length = _dimension_value(dimensions.get("length"))
        height = _dimension_value(dimensions.get("height"))
        parts = [part for part in (length, width, height) if part]
        if parts:
            return " x ".join(parts)
    return ""


def _dimension_value(value: Any) -> str:
    if not isinstance(value, dict):
        return ""
    number = value.get("value")
    unit = value.get("unit")
    if number in (None, ""):
        return ""
    return f"{number} {unit}".strip()

