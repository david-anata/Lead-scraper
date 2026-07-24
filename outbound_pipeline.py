"""StoreLeads -> Clay outbound pipeline.

Pulls ecommerce brands that match Anata's ICP from the StoreLeads API, dedupes
against brands already contacted, and pushes the fresh ones into a Clay table
(via Clay's inbound webhook). Clay then finds the decision-maker, verifies the
email, qualifies, personalizes, and hands off to Instantly.

This module deliberately does NOT send any email. It only sources brands and
hands them to Clay. The ICP gate here is the authoritative filter, matched to
the target already defined in the app and in docs/outbound/03-storeleads-filter-recipe.md.

Network calls (fetch_storeleads_page, push_to_clay) are thin and injectable so
the ICP logic can be tested without a live key. Live behaviour is verified once
STORELEADS_API_KEY and CLAY_WEBHOOK_URL are set on Render.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Optional

import requests

logger = logging.getLogger(__name__)

STORELEADS_BASE_URL = "https://storeleads.app/json/api/v1/all"
STORELEADS_DOMAIN_PATH = "/domain"

# ---- ICP (mirrors the coded target + docs/outbound/03) -----------------------
ICP_MIN_YEARLY_SALES_CENTS = 1_000_000_00      # ~$1M/yr floor
ICP_MAX_YEARLY_SALES_CENTS = 15_000_000_00     # ~$15M/yr ceiling
ICP_ALLOWED_COUNTRIES = {"US", "GB", "UK", "CA", "AU"}
ICP_ALLOWED_PLATFORMS = {"shopify"}

# StoreLeads category text -> our niche label (keyword match, lowercased)
ICP_NICHE_KEYWORDS: dict[str, tuple[str, ...]] = {
    "beauty_wellness": ("beauty", "skincare", "cosmetic", "wellness", "supplement", "personal care"),
    "food_beverage": ("food", "beverage", "drink", "snack", "coffee", "tea"),
    "apparel_accessories": ("apparel", "fashion", "clothing", "footwear", "jewelry", "accessories"),
    "home_lifestyle": ("home", "furniture", "decor", "lifestyle", "kitchen", "bedding"),
    "pets": ("pet", "pets", "animal"),
    "family_gifts": ("baby", "kids", "toys", "gift", "stationery"),
}

# Words that disqualify a store outright (agencies, resellers, etc.)
ICP_EXCLUDE_KEYWORDS = (
    "agency", "consulting", "consultancy", "software", "saas", "wholesale",
    "distributor", "manufacturer", "b2b", "dropship", "drop shipping",
    "print on demand", "print-on-demand", "printful", "printify",
    "etsy seller", "reseller", "marketplace",
)


def _normalize_country(code: str) -> str:
    code = (code or "").strip().upper()
    return "GB" if code == "UK" else code


def store_niche(store: dict[str, Any]) -> str:
    """Return the ICP niche label for a store, or '' if it fits none."""
    text = " ".join(
        str(part) for part in (
            store.get("categories") or "",
            store.get("tags") or "",
            store.get("merchant_name") or "",
            store.get("name") or "",
        )
    ).lower()
    for niche, keywords in ICP_NICHE_KEYWORDS.items():
        if any(kw in text for kw in keywords):
            return niche
    return ""


def store_matches_icp(store: dict[str, Any]) -> bool:
    """Authoritative ICP gate applied to a StoreLeads store record."""
    domain = str(store.get("name") or "").strip().lower()
    if not domain:
        return False

    platform = str(store.get("platform") or "").strip().lower()
    if ICP_ALLOWED_PLATFORMS and platform and platform not in ICP_ALLOWED_PLATFORMS:
        return False

    country = _normalize_country(str(store.get("country_code") or ""))
    if country and country not in ICP_ALLOWED_COUNTRIES:
        return False

    yearly = store.get("estimated_sales_yearly")
    if isinstance(yearly, (int, float)) and yearly > 0:
        if yearly < ICP_MIN_YEARLY_SALES_CENTS or yearly > ICP_MAX_YEARLY_SALES_CENTS:
            return False

    searchable = " ".join(
        str(part) for part in (
            domain,
            store.get("merchant_name") or "",
            store.get("categories") or "",
            store.get("tags") or "",
        )
    ).lower()
    if any(bad in searchable for bad in ICP_EXCLUDE_KEYWORDS):
        return False

    # Must land in one of the six niches, and must have a contact email listed
    # (StoreLeads has no emails itself, but a listed email means a live domain
    # Clay can work from).
    if not store_niche(store):
        return False
    if not _has_email(store):
        return False

    return True


def _has_email(store: dict[str, Any]) -> bool:
    for item in store.get("contact_info") or []:
        if isinstance(item, dict) and str(item.get("type", "")).lower() == "email":
            if str(item.get("value", "")).strip():
                return True
    return False


def to_clay_lead(store: dict[str, Any]) -> dict[str, Any]:
    """Shape a matched store into the payload Clay's table expects."""
    return {
        "domain": str(store.get("name") or "").strip().lower(),
        "brand": str(store.get("merchant_name") or store.get("name") or "").strip(),
        "niche": store_niche(store),
        "country": _normalize_country(str(store.get("country_code") or "")),
        "estimated_sales_yearly_cents": store.get("estimated_sales_yearly"),
        "categories": store.get("categories"),
        "apps": store.get("apps"),
    }


# ---- thin network layer (injectable for tests) -------------------------------

def fetch_storeleads_page(
    api_key: str,
    *,
    page: int = 0,
    page_size: int = 50,
    timeout: int = 30,
) -> list[dict[str, Any]]:
    """Fetch one page of Shopify stores from StoreLeads, newest-ranked first.

    Server-side we narrow to the platform and sort by rank; the authoritative
    ICP filtering (revenue, country, category, tags, email) happens client-side
    in store_matches_icp so it stays exact regardless of StoreLeads filter quirks.
    """
    params = {
        "f:p": "shopify",
        "page": page,
        "page_size": page_size,
        "sort": "-rank",
        "fields": ",".join((
            "name", "merchant_name", "platform", "country_code",
            "estimated_sales_yearly", "categories", "tags", "contact_info", "apps",
        )),
    }
    resp = requests.get(
        f"{STORELEADS_BASE_URL}{STORELEADS_DOMAIN_PATH}",
        headers={"Authorization": f"Bearer {api_key}"},
        params=params,
        timeout=timeout,
    )
    resp.raise_for_status()
    payload = resp.json()
    # StoreLeads returns the store array under "domains" (list endpoint).
    if isinstance(payload, dict):
        return payload.get("domains") or payload.get("results") or []
    if isinstance(payload, list):
        return payload
    return []


def push_to_clay(webhook_url: str, leads: list[dict[str, Any]], *, timeout: int = 30) -> dict[str, Any]:
    """Post a batch of leads to the Clay table's inbound webhook."""
    if not webhook_url:
        return {"pushed": 0, "skipped": True, "reason": "no Clay webhook configured"}
    if not leads:
        return {"pushed": 0, "skipped": True, "reason": "no leads to push"}
    resp = requests.post(webhook_url, json={"leads": leads}, timeout=timeout)
    resp.raise_for_status()
    return {"pushed": len(leads)}


# ---- orchestration -----------------------------------------------------------

@dataclass
class PipelineResult:
    scanned: int = 0
    matched_icp: int = 0
    fresh: int = 0
    pushed: int = 0
    skipped_already_contacted: int = 0
    leads: list[dict[str, Any]] = field(default_factory=list)
    dry_run: bool = False


def run_storeleads_to_clay(
    *,
    api_key: str,
    clay_webhook_url: str,
    processed_domains: set[str],
    max_new: int = 100,
    max_pages: int = 40,
    dry_run: bool = False,
    fetch_page: Optional[Callable[..., list[dict[str, Any]]]] = None,
    push: Optional[Callable[..., dict[str, Any]]] = None,
) -> PipelineResult:
    """Pull ICP brands from StoreLeads, drop already-contacted ones, push the
    rest to Clay. With dry_run=True (or no Clay webhook) nothing is pushed; the
    matched leads are returned for preview.
    """
    fetch_page = fetch_page or fetch_storeleads_page
    push = push or push_to_clay

    result = PipelineResult(dry_run=dry_run or not clay_webhook_url)
    seen_this_run: set[str] = set()

    for page in range(max_pages):
        if result.fresh >= max_new:
            break
        stores = fetch_page(api_key, page=page)
        if not stores:
            break
        for store in stores:
            result.scanned += 1
            if not store_matches_icp(store):
                continue
            result.matched_icp += 1
            lead = to_clay_lead(store)
            domain = lead["domain"]
            if not domain or domain in seen_this_run:
                continue
            if domain in processed_domains:
                result.skipped_already_contacted += 1
                continue
            seen_this_run.add(domain)
            result.leads.append(lead)
            result.fresh += 1
            if result.fresh >= max_new:
                break

    if not result.dry_run and result.leads:
        outcome = push(clay_webhook_url, result.leads)
        result.pushed = int(outcome.get("pushed", 0) or 0)

    logger.info(
        "[outbound] scanned=%s matched=%s fresh=%s pushed=%s skipped_contacted=%s dry_run=%s",
        result.scanned, result.matched_icp, result.fresh, result.pushed,
        result.skipped_already_contacted, result.dry_run,
    )
    return result


def load_config_from_env() -> tuple[str, str]:
    """Read the two values David sets on Render. Never hardcoded."""
    api_key = (os.getenv("STORELEADS_API_KEY") or "").strip()
    clay_webhook_url = (os.getenv("CLAY_WEBHOOK_URL") or "").strip()
    return api_key, clay_webhook_url
