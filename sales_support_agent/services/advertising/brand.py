"""Brand focus — scope an audit to a single brand instead of the whole account.

A brand is matched as a case-insensitive substring of the campaign/ad-group name
(for ad rows) or the product title/SKU (for sales rows). `detect_brand_candidates`
mines campaign names for likely brand tokens to suggest in the UI.
"""

from __future__ import annotations

import re
from collections import Counter

from sales_support_agent.services.advertising.schema import AdRow, SalesRow

# Tokens that show up in campaign names but are never the brand.
_STOPWORDS = {
    "quartile", "non", "branded", "brand", "sp", "sb", "sd", "dsp", "ras",
    "auto", "manual", "exact", "broad", "phrase", "competitor", "competitors",
    "category", "cat", "generic", "defensive", "prospecting", "retargeting",
    "remarketing", "awareness", "conversion", "kw", "keyword", "keywords",
    "product", "products", "asin", "test", "new", "the", "and", "for", "of",
    "ad", "ads", "campaign", "us", "usa", "fba", "performance", "ow", "atm",
    "tos", "top", "search", "rest", "collection", "productcollection", "video",
}
_ASIN_RE = re.compile(r"^b0[a-z0-9]{8}$", re.IGNORECASE)


def matches_brand(brand: str, *texts: str) -> bool:
    b = (brand or "").strip().lower()
    if not b:
        return True
    return any(b in (t or "").lower() for t in texts)


def _ad_asin(row: AdRow) -> str:
    return (row.raw.get("Advertised product ID") or row.raw.get("Advertised product Id") or "").strip()


def filter_by_brand(
    ad_rows: list[AdRow], sales_rows: list[SalesRow], brand: str
) -> tuple[list[AdRow], list[SalesRow]]:
    """Return only the rows belonging to `brand`. Empty brand -> unchanged.

    Brand campaigns are frequently named by ASIN (e.g. ``..._B07NXN4F7X_...``)
    rather than the brand word, so a plain keyword match misses most of the
    spend. We scope by the brand's ASINs (from the Business Report) as well as
    the keyword: keep an ad row if it advertises a brand ASIN, sits in a campaign
    that advertises one, names the brand/ASIN, or the keyword appears in it."""
    if not (brand or "").strip():
        return ad_rows, sales_rows

    brand_sales = [s for s in sales_rows if matches_brand(brand, s.title, s.sku, s.asin)]
    brand_asins = {s.asin for s in brand_sales if s.asin}
    asin_lowers = {a.lower() for a in brand_asins}

    # Campaigns proven to advertise a brand ASIN, plus campaigns named for the brand.
    brand_campaigns = {
        r.campaign_name for r in ad_rows
        if r.entity_level == "product_ad" and _ad_asin(r) in brand_asins and r.campaign_name
    }
    brand_campaigns |= {
        r.campaign_name for r in ad_rows
        if r.campaign_name and matches_brand(brand, r.campaign_name)
    }

    def _keep(r: AdRow) -> bool:
        if r.entity_level == "product_ad":
            return _ad_asin(r) in brand_asins or matches_brand(brand, r.campaign_name, r.ad_group_name)
        if r.campaign_name in brand_campaigns:
            return True
        if matches_brand(brand, r.campaign_name, r.ad_group_name, r.entity_text):
            return True
        name = (r.campaign_name or "").lower()
        return any(a in name for a in asin_lowers)

    return [r for r in ad_rows if _keep(r)], brand_sales


def detect_brand_candidates(
    ad_rows: list[AdRow], sales_rows: "list[SalesRow] | None" = None, limit: int = 10
) -> list[str]:
    """Surface likely brand tokens, ranked. Product titles usually LEAD with the
    brand ("Zantrex SkinnyStix …"), so the first significant title token is the
    strongest signal; campaign-name tokens are a weaker secondary source.
    Heuristic — UI suggestions, not a hard constraint."""
    counts: Counter[str] = Counter()

    for s in (sales_rows or []):
        for raw in re.split(r"[^A-Za-z0-9]+", s.title or ""):
            tok = raw.strip()
            low = tok.lower()
            if len(tok) < 3 or not tok.isalpha() or low in _STOPWORDS or _ASIN_RE.match(low):
                continue
            counts[_titlecase(tok)] += 3  # leading title token = brand
            break

    campaigns = {r.campaign_name.strip() for r in ad_rows if r.campaign_name.strip()}
    for name in campaigns:
        seen: set[str] = set()
        for raw in re.split(r"[^A-Za-z0-9]+", name):
            tok = raw.strip()
            low = tok.lower()
            # Brand names are alphabetic; skip codes/ASINs/IDs (APRQ8M…, B0…).
            if len(tok) < 3 or not tok.isalpha() or low in _STOPWORDS or _ASIN_RE.match(low):
                continue
            if low in seen:
                continue
            seen.add(low)
            counts[_titlecase(tok)] += 1
    # Prefer tokens appearing in 2+ campaigns; fall back to most common.
    ranked = [tok for tok, n in counts.most_common() if n >= 2] or [tok for tok, _ in counts.most_common()]
    return ranked[:limit]


def _titlecase(token: str) -> str:
    # Preserve existing capitalization if it looks intentional (e.g. SkinnyStix).
    if any(c.isupper() for c in token[1:]):
        return token
    return token[:1].upper() + token[1:].lower()
