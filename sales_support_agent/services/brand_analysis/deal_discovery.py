"""Deal Discovery — scrapes BizBuySell and Flippa for Amazon FBA listings,
qualifies them against Ascend's criteria, and prepares pipeline entries.

Usage:
    listings = scrape_listings("bizbuysell", min_revenue_cents=1_000_000_00)
    for listing in listings:
        q = qualify_listing(listing)
        if q["qualified"]:
            create_pipeline_entry(listing, q)
"""

from __future__ import annotations

import json
import logging
import re
from typing import Optional
from urllib.parse import urlencode

logger = logging.getLogger(__name__)

_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

SOURCES: dict[str, str] = {
    "bizbuysell": "https://www.bizbuysell.com/amazon-fba-businesses-for-sale/",
    "flippa": "https://flippa.com/search?" + urlencode({
        "filter[site_type]": "content",
        "filter[monetization][]": "amazon-fba",
    }),
}

# Ascend hard criteria (from business plan)
ASCEND_CRITERIA = {
    "min_revenue_usd": 1_000_000,
    "min_ebitda_margin_pct": 35,
    "platforms": ["amazon", "fba"],
}


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------

def _fetch(url: str, timeout: int = 12) -> str:
    try:
        import urllib.request
        req = urllib.request.Request(
            url,
            headers={"User-Agent": _UA, "Accept-Language": "en-US,en;q=0.9"},
        )
        with urllib.request.urlopen(req, timeout=timeout) as r:  # noqa: S310
            return r.read(1_200_000).decode("utf-8", errors="replace")
    except Exception:
        logger.debug("[deal_discovery] fetch failed: %s", url[:80])
        return ""


def _claude(prompt: str, html: str, max_tokens: int = 1024) -> Optional[dict]:
    if not html:
        return None
    try:
        import anthropic
        client = anthropic.Anthropic()
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=max_tokens,
            messages=[{
                "role": "user",
                "content": (
                    f"{prompt}\n\nHTML (first 10000 chars):\n{html[:10000]}\n\n"
                    "Return ONLY valid JSON, no markdown fences."
                ),
            }],
        )
        text = (msg.content[0].text if msg.content else "").strip()
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
        return json.loads(text)
    except Exception:
        logger.debug("[deal_discovery] claude call failed", exc_info=True)
        return None


def _parse_usd(value) -> Optional[int]:
    """Convert '$1.2M', '1200000', 1200000 etc. to cents."""
    if value is None:
        return None
    s = str(value).replace(",", "").strip()
    multiplier = 1
    if s.upper().endswith("M"):
        multiplier = 1_000_000
        s = s[:-1]
    elif s.upper().endswith("K"):
        multiplier = 1_000
        s = s[:-1]
    s = s.lstrip("$").strip()
    try:
        return int(float(s) * multiplier * 100)
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Scrape listings
# ---------------------------------------------------------------------------

def scrape_listings(
    source: str,
    min_revenue_cents: int = 100_000_00,
    max_price_cents: int = 20_000_000_00,
) -> list[dict]:
    """Fetch a search results page and extract listing cards via Claude Haiku.
    Returns a list of raw listing dicts; never raises.
    """
    url = SOURCES.get(source)
    if not url:
        logger.warning("[deal_discovery] unknown source: %s", source)
        return []

    html = _fetch(url)
    if not html:
        return []

    result = _claude(
        "Extract ALL business listing cards from this HTML page. "
        "For each listing return: "
        '{"name": str, "asking_price_usd": str_or_null, '
        '"annual_revenue_usd": str_or_null, "annual_cashflow_usd": str_or_null, '
        '"description": str (first 200 chars), "listing_url": str, '
        '"business_type": str}. '
        'Return a JSON array: [{"name":...}, ...]',
        html,
        max_tokens=2048,
    )
    if not isinstance(result, list):
        result = result.get("listings") or [] if isinstance(result, dict) else []

    # Filter to Amazon/FBA and within price range
    filtered = []
    for item in result:
        btype = (item.get("business_type") or "").lower()
        if not any(kw in btype for kw in ["amazon", "fba", "ecommerce", "e-commerce"]):
            desc = (item.get("description") or "").lower()
            if not any(kw in desc for kw in ["amazon", "fba"]):
                continue
        price_cents = _parse_usd(item.get("asking_price_usd"))
        rev_cents = _parse_usd(item.get("annual_revenue_usd"))
        if price_cents and price_cents > max_price_cents:
            continue
        if rev_cents and rev_cents < min_revenue_cents:
            continue
        item["_source"] = source
        item["_asking_price_cents"] = price_cents
        item["_revenue_cents"] = rev_cents
        item["_cashflow_cents"] = _parse_usd(item.get("annual_cashflow_usd"))
        filtered.append(item)

    return filtered


# ---------------------------------------------------------------------------
# Qualify a listing
# ---------------------------------------------------------------------------

def qualify_listing(listing: dict) -> dict:
    """Assess a listing against Ascend's criteria.

    Returns:
        {
          qualified: bool,
          score: int (0-100),
          criteria: {revenue_ok, margin_ok, fba_confirmed, trademark_hint, brand_registry_hint},
          gaps: list[str],
          extracted: {ebitda_margin_pct, channel_mix_hint, sku_count_hint}
        }
    """
    rev_cents = listing.get("_revenue_cents") or 0
    cashflow_cents = listing.get("_cashflow_cents") or 0
    description = listing.get("description") or ""
    name = listing.get("name") or ""

    # Deterministic checks first (fast path for obvious failures)
    revenue_ok = rev_cents >= ASCEND_CRITERIA["min_revenue_usd"] * 100
    margin_pct = (cashflow_cents / rev_cents * 100) if rev_cents else 0
    margin_ok = margin_pct >= ASCEND_CRITERIA["min_ebitda_margin_pct"]
    desc_lower = description.lower()
    fba_confirmed = any(k in desc_lower for k in ["amazon fba", "fulfillment by amazon", "fba business"])
    trademark_hint = any(k in desc_lower for k in ["trademark", "registered trademark", "tm "])
    brand_registry_hint = "brand registry" in desc_lower

    # Claude scoring for nuance
    prompt = (
        "You are evaluating an Amazon FBA brand acquisition for a private equity firm. "
        f"Ascend's criteria: ${ASCEND_CRITERIA['min_revenue_usd']:,}+ revenue, "
        f"{ASCEND_CRITERIA['min_ebitda_margin_pct']}%+ EBITDA, FBA model, "
        "Brand Registry preferred, trademark preferred, 4.3+ rating preferred.\n\n"
        f"Listing name: {name}\n"
        f"Revenue: ${rev_cents/100:,.0f}\n"
        f"Cashflow/EBITDA: ${cashflow_cents/100:,.0f}\n"
        f"Description: {description[:500]}\n\n"
        "Score this deal 0-100 and identify acquisition gaps. Return JSON:\n"
        '{"score": int, "gaps": ["Missing ACOS data", ...], '
        '"extracted": {"ebitda_margin_pct": float_or_null, '
        '"channel_mix_hint": str_or_null, "sku_count_hint": str_or_null}}'
    )
    # We pass an empty html string since all context is in the prompt
    try:
        import anthropic
        client = anthropic.Anthropic()
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=512,
            messages=[{
                "role": "user",
                "content": prompt + "\n\nReturn ONLY valid JSON, no markdown fences.",
            }],
        )
        text = (msg.content[0].text if msg.content else "").strip()
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
        ai_result = json.loads(text)
    except Exception:
        ai_result = {"score": 0, "gaps": ["Qualification unavailable"], "extracted": {}}

    score = ai_result.get("score", 0)
    gaps = ai_result.get("gaps") or []
    if not revenue_ok:
        gaps.insert(0, f"Revenue ${rev_cents/100:,.0f} below $1M threshold")
    if not margin_ok and cashflow_cents > 0:
        gaps.insert(0, f"EBITDA margin {margin_pct:.0f}% below 35% target")

    return {
        "qualified": revenue_ok and score >= 40,
        "score": score,
        "criteria": {
            "revenue_ok": revenue_ok,
            "margin_ok": margin_ok,
            "fba_confirmed": fba_confirmed,
            "trademark_hint": trademark_hint,
            "brand_registry_hint": brand_registry_hint,
        },
        "gaps": gaps[:8],
        "extracted": ai_result.get("extracted") or {},
    }


# ---------------------------------------------------------------------------
# Create pipeline entry
# ---------------------------------------------------------------------------

def create_pipeline_entry(listing: dict, qualified: dict) -> Optional[str]:
    """Insert a BrandAnalysisReport placeholder row for a discovered deal.
    Returns the new report_id, or None if dedup skipped it.
    """
    from sales_support_agent.services.brand_analysis.storage import (
        create_placeholder_entry,
    )

    listing_url = listing.get("listing_url") or ""
    brand_name = listing.get("name") or "Unknown Brand"
    ask_price_cents = listing.get("_asking_price_cents")
    rev_cents = listing.get("_revenue_cents")

    gaps_text = "; ".join(qualified.get("gaps") or [])
    score = qualified.get("score", 0)
    notes = (
        f"Source: {listing.get('_source', 'unknown')} | "
        f"URL: {listing_url} | "
        f"Score: {score}/100"
        + (f" | Gaps: {gaps_text}" if gaps_text else "")
    )

    return create_placeholder_entry(
        brand_name=brand_name,
        ask_price_cents=ask_price_cents,
        notes=notes,
        brand_website=listing_url,
        category="Amazon FBA",
        revenue_cents=rev_cents,
    )
