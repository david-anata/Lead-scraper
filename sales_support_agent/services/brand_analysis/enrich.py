"""Auto-enrichment for Brand Analysis — Zone G (Social Metrics) and Zone H
(Competitive Signals) prefill.

Fetches missing data from public social profile pages and Amazon search
results so analysts don't have to look up follower counts manually.
Never raises — always returns a partial dict with whatever succeeded.
"""

from __future__ import annotations

import json
import logging
import os
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


# ---------------------------------------------------------------------------
# Per-platform follower extraction
# ---------------------------------------------------------------------------


def _yt_subscribers(yt_url: str) -> Optional[int]:
    """YouTube subscriber count via Data API v3 (if GOOGLE_API_KEY set) or page scrape."""
    google_key = os.environ.get("GOOGLE_API_KEY", "")
    if google_key:
        handle_m = re.search(r"youtube\.com/@([\w.-]+)", yt_url)
        channel_m = re.search(r"youtube\.com/channel/([\w-]+)", yt_url)
        try:
            if handle_m:
                api_url = (
                    "https://www.googleapis.com/youtube/v3/channels"
                    f"?part=statistics&forHandle=%40{handle_m.group(1)}&key={google_key}"
                )
            elif channel_m:
                api_url = (
                    "https://www.googleapis.com/youtube/v3/channels"
                    f"?part=statistics&id={channel_m.group(1)}&key={google_key}"
                )
            else:
                api_url = ""
            if api_url:
                raw = _fetch(api_url)
                data = json.loads(raw)
                items = data.get("items") or []
                if items:
                    return int(items[0].get("statistics", {}).get("subscriberCount") or 0) or None
        except Exception:
            logger.debug("[enrich] YouTube Data API failed", exc_info=True)

    # Fallback: scrape the page
    page_html = _fetch(yt_url)
    result = _claude_extract(
        page_html,
        'Extract the YouTube channel subscriber count. '
        'Return JSON: {"subscribers": <integer or null>}',
    )
    if result and result.get("subscribers"):
        return int(result["subscribers"])
    return None


def _social_followers(platform: str, url: str) -> Optional[int]:
    """Extract follower/subscriber count for a social URL."""
    if platform == "youtube":
        return _yt_subscribers(url)

    page_html = _fetch(url)
    if not page_html:
        return None

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
        f"Extract the {platform} {label} count from this profile page. "
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
