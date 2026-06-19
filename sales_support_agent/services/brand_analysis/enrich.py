"""Auto-enrichment for Brand Analysis — Zone G (Social Metrics) and Zone H
(Competitive Signals) prefill.

Fetches missing data from public social profile pages and Amazon search
results so analysts don't have to look up follower counts manually.
Never raises — always returns a partial dict with whatever succeeded.

All platforms are scraped via regex against the embedded page JSON first
(YouTube ytInitialData, Instagram edge_followed_by, TikTok followerCount,
Facebook fan_count), with a Claude Haiku fallback for anything that slips
through. No external API keys required.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Optional
from urllib.parse import quote_plus

logger = logging.getLogger(__name__)

_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------


def _fetch(url: str, timeout: int = 8) -> str:
    """GET a URL with a browser User-Agent. Returns raw HTML or ''."""
    try:
        import urllib.request
        req = urllib.request.Request(
            url,
            headers={"User-Agent": _UA, "Accept-Language": "en-US,en;q=0.9"},
        )
        with urllib.request.urlopen(req, timeout=timeout) as r:  # noqa: S310
            return r.read(800_000).decode("utf-8", errors="replace")
    except Exception:
        logger.debug("[enrich] fetch failed: %s", url[:80])
        return ""


def _claude_extract(html_text: str, prompt: str) -> Optional[dict]:
    """Ask Claude Haiku to extract structured data from HTML. ~200 tokens/call."""
    if not html_text:
        return None
    try:
        import anthropic
        client = anthropic.Anthropic()
        snippet = html_text[:8000]
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=256,
            messages=[{
                "role": "user",
                "content": (
                    f"{prompt}\n\n"
                    f"HTML snippet:\n{snippet}\n\n"
                    "Return ONLY valid JSON, no markdown fences or other text."
                ),
            }],
        )
        text = (msg.content[0].text if msg.content else "").strip()
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
        return json.loads(text)
    except Exception:
        logger.debug("[enrich] claude extraction failed", exc_info=True)
        return None


def _parse_count_text(text: str) -> Optional[int]:
    """Parse '1.23M', '5.4K', '120,000', '1.2M subscribers' → int."""
    if not text:
        return None
    text = text.replace(",", "").strip()
    m = re.search(r"([\d.]+)\s*([KkMmBbGg]?)", text)
    if not m:
        return None
    try:
        val = float(m.group(1))
        suffix = m.group(2).upper()
        multipliers = {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000, "G": 1_000_000_000}
        return int(val * multipliers.get(suffix, 1))
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Per-platform scraping  (regex-first, Claude Haiku fallback)
# ---------------------------------------------------------------------------


def _yt_subscribers(yt_url: str) -> Optional[int]:
    """YouTube subscriber count via ytInitialData embedded JSON (no API key needed)."""
    page_html = _fetch(yt_url)
    if not page_html:
        return None

    # Exact count — YouTube embeds the raw number in ytInitialData
    m = re.search(r'"subscriberCount":"(\d+)"', page_html)
    if m:
        return int(m.group(1))

    # Human-readable text like "1.23M subscribers"
    m2 = re.search(r'"subscriberCountText":\{"simpleText":"([^"]+)"', page_html)
    if m2:
        count = _parse_count_text(m2.group(1))
        if count:
            return count

    # Also try simpleText variant inside accessibilityData
    m3 = re.search(r'"subscribers"[^}]*"simpleText":"([^"]+)"', page_html)
    if m3:
        count = _parse_count_text(m3.group(1))
        if count:
            return count

    # Haiku fallback
    result = _claude_extract(
        page_html,
        "Extract the YouTube channel subscriber count from this page HTML. "
        'Return JSON: {"subscribers": <integer or null>}',
    )
    if result and result.get("subscribers"):
        return int(result["subscribers"])
    return None


def _ig_followers(page_html: str) -> Optional[int]:
    """Extract Instagram follower count from page HTML."""
    for pattern in [
        r'"edge_followed_by":\{"count":(\d+)',
        r'"followerCount":(\d+)',
        r'"followers":(\d+)',
        r'"userInteractionCount":(\d+)',
    ]:
        m = re.search(pattern, page_html)
        if m:
            val = int(m.group(1))
            if val > 0:
                return val
    return None


def _tt_followers(page_html: str) -> Optional[int]:
    """Extract TikTok follower count from page HTML."""
    for pattern in [
        r'"followerCount":(\d+)',
        r'"fans":(\d+)',
        r'"authorStats":\{[^}]*"followerCount":(\d+)',
    ]:
        m = re.search(pattern, page_html)
        if m:
            val = int(m.group(1))
            if val > 0:
                return val
    return None


def _fb_followers(page_html: str) -> Optional[int]:
    """Extract Facebook follower/like count from page HTML."""
    for pattern in [
        r'"follower_count":(\d+)',
        r'"fan_count":(\d+)',
        r'"interactionStatistic"[^}]*"userInteractionCount":(\d+)',
    ]:
        m = re.search(pattern, page_html)
        if m:
            val = int(m.group(1))
            if val > 0:
                return val
    # Human-readable in visible text like "12,345 people follow this"
    m2 = re.search(r'([\d,]+)\s+people\s+follow', page_html, re.IGNORECASE)
    if m2:
        count = _parse_count_text(m2.group(1))
        if count:
            return count
    return None


_REGEX_EXTRACTORS = {
    "instagram": _ig_followers,
    "tiktok": _tt_followers,
    "facebook": _fb_followers,
}


def _social_followers(platform: str, url: str) -> Optional[int]:
    """Extract follower/subscriber count for a social URL."""
    if platform == "youtube":
        return _yt_subscribers(url)

    page_html = _fetch(url)
    if not page_html:
        return None

    # Try fast regex first
    extractor = _REGEX_EXTRACTORS.get(platform)
    if extractor:
        count = extractor(page_html)
        if count:
            return count

    # Haiku fallback
    labels = {
        "instagram": "follower",
        "tiktok": "follower",
        "facebook": "follower or like",
        "twitter": "follower",
        "linkedin": "follower",
    }
    label = labels.get(platform, "follower")
    result = _claude_extract(
        page_html,
        f"Extract the {platform} {label} count from this profile page HTML. "
        f'Return JSON: {{"followers": <integer or null>}}',
    )
    if result and result.get("followers"):
        return int(result["followers"])
    return None


# ---------------------------------------------------------------------------
# Amazon competitive data
# ---------------------------------------------------------------------------


def _amazon_competitive(brand_name: str) -> dict:
    """Search Amazon.com for the brand and extract competitive data from first 2 results."""
    if not brand_name:
        return {}
    search_url = f"https://www.amazon.com/s?k={quote_plus(brand_name)}"
    page_html = _fetch(search_url, timeout=10)
    if not page_html:
        return {}
    result = _claude_extract(
        page_html,
        "From this Amazon search results page, extract data for the first two products shown. "
        "Return JSON: "
        '{"brand": {"bsr_rank": int_or_null, "rating": float_or_null, '
        '"review_count": int_or_null, "price_cents": int_or_null, '
        '"category": str_or_null}, '
        '"competitor": {"name": str_or_null, "bsr_rank": int_or_null, '
        '"review_count": int_or_null}}',
    )
    return result or {}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def auto_enrich(
    brand_name: str,
    brand_website: str,
    existing_handles: dict,
) -> dict:
    """Return a prefill dict for Zone G + Zone H fields. Never raises.

    Keys returned (all optional — only present when successfully extracted):
      Social:   instagram_url, ig_followers, tiktok_url, tt_followers,
                facebook_url, fb_followers, youtube_url, yt_subscribers,
                review_rating, review_count
      Competitive: bsr_rank, brand_price_cents, category_name,
                   top_competitor_name, competitor_reviews, competitor_bsr
      Meta:     _sources (field → source URL), _errors (list of failure notes)
    """
    out: dict = {"_sources": {}, "_errors": []}

    # ── Step 1: discover social URLs if not already supplied ─────────────────
    from sales_support_agent.services.brand_analysis.social import discover_socials
    handles = dict(existing_handles or {})
    if brand_website and not handles:
        try:
            handles.update(discover_socials(brand_website))
        except Exception as exc:
            out["_errors"].append(f"discover_socials: {str(exc)[:80]}")

    # ── Step 2: follower counts for each discovered platform ─────────────────
    platform_map = {
        "instagram": ("instagram_url", "ig_followers"),
        "tiktok":    ("tiktok_url",    "tt_followers"),
        "facebook":  ("facebook_url",  "fb_followers"),
        "youtube":   ("youtube_url",   "yt_subscribers"),
    }
    for platform, (url_field, fol_field) in platform_map.items():
        url = handles.get(platform)
        if not url:
            continue
        out[url_field] = url
        out["_sources"][fol_field] = url
        try:
            count = _social_followers(platform, url)
            if count and count > 0:
                out[fol_field] = count
        except Exception as exc:
            out["_errors"].append(f"{platform}: {str(exc)[:80]}")

    # ── Step 3: Amazon competitive data ─────────────────────────────────────
    try:
        comp = _amazon_competitive(brand_name)
        brand_data = comp.get("brand") or {}
        competitor_data = comp.get("competitor") or {}
        field_map = {
            "review_rating":       brand_data.get("rating"),
            "review_count":        brand_data.get("review_count"),
            "bsr_rank":            brand_data.get("bsr_rank"),
            "brand_price_cents":   brand_data.get("price_cents"),
            "category_name":       brand_data.get("category"),
            "top_competitor_name": competitor_data.get("name"),
            "competitor_reviews":  competitor_data.get("review_count"),
            "competitor_bsr":      competitor_data.get("bsr_rank"),
        }
        for field, value in field_map.items():
            if value:
                out[field] = value
                out["_sources"][field] = "amazon.com/s"
    except Exception as exc:
        out["_errors"].append(f"amazon: {str(exc)[:80]}")

    return out
