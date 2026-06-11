"""Visitor metadata helpers shared by hosted-document heartbeat endpoints
(sales decks, fulfillment rate sheets): lightweight UA parsing, referrer
categorization, and free geo capture. Extracted from api/router.py (PR54) so
the in-process rate-sheet router can reuse them without importing the full
backend router module.
"""

from __future__ import annotations

from fastapi import Request

MAX_SESSION_SECONDS = 6 * 60 * 60


def parse_user_agent(ua_raw: str) -> dict[str, str]:
    """Lightweight UA parser — no extra dep. Returns {device, os, browser}.
    Catches the 95% case (mainstream desktop + iOS/Android mobile);
    everything else falls back to "other"."""
    ua = (ua_raw or "")[:512]
    ua_lc = ua.lower()
    if not ua:
        return {"device": "", "os": "", "browser": ""}
    # Device tier: mobile/tablet/desktop. iPad reports as Mac on iOS 13+,
    # but we don't need that level of precision for the dashboard.
    if "ipad" in ua_lc or ("tablet" in ua_lc and "android" in ua_lc):
        device = "tablet"
    elif "mobile" in ua_lc or "iphone" in ua_lc or "ipod" in ua_lc or "android" in ua_lc:
        device = "mobile"
    else:
        device = "desktop"
    # OS — pick the most specific match first.
    if "windows" in ua_lc:
        os_name = "Windows"
    elif "iphone" in ua_lc or "ipad" in ua_lc or "ipod" in ua_lc:
        os_name = "iOS"
    elif "android" in ua_lc:
        os_name = "Android"
    elif "mac os x" in ua_lc or "macos" in ua_lc or "macintosh" in ua_lc:
        os_name = "macOS"
    elif "linux" in ua_lc:
        os_name = "Linux"
    elif "cros" in ua_lc:
        os_name = "ChromeOS"
    else:
        os_name = "other"
    # Browser — order matters (Edge contains "Chrome", Chrome contains "Safari").
    if "edg/" in ua_lc or "edge/" in ua_lc:
        browser = "Edge"
    elif "opr/" in ua_lc or "opera" in ua_lc:
        browser = "Opera"
    elif "firefox/" in ua_lc:
        browser = "Firefox"
    elif "chrome/" in ua_lc or "crios/" in ua_lc:
        browser = "Chrome"
    elif "safari/" in ua_lc:
        browser = "Safari"
    else:
        browser = "other"
    return {"device": device, "os": os_name, "browser": browser}


def categorize_referrer(referrer_url: str) -> tuple[str, str]:
    """Returns (host, category) for a referer URL.
    Categories: direct/email/social/search/other. Used for the source
    breakdown card in the analytics modal."""
    raw = (referrer_url or "").strip()
    if not raw:
        return ("", "direct")
    try:
        from urllib.parse import urlparse
        parsed = urlparse(raw)
        host = (parsed.hostname or "").lower().lstrip("www.")
    except Exception:
        return ("", "direct")
    if not host:
        return ("", "direct")
    # Mail clients & email services.
    if any(m in host for m in ("mail.google", "outlook.live", "outlook.office", "mail.yahoo", "mail.proton")):
        return (host, "email")
    # Social.
    social_hosts = ("linkedin.com", "twitter.com", "x.com", "facebook.com", "instagram.com",
                    "t.co", "lnkd.in", "fb.com", "reddit.com", "youtube.com")
    if any(s in host for s in social_hosts):
        return (host, "social")
    # Messaging that often gets pasted in.
    if any(m in host for m in ("slack.com", "discord.com", "teams.microsoft", "telegram.org", "wa.me", "whatsapp.com")):
        return (host, "social")
    # Search engines.
    if any(s in host for s in ("google.com", "bing.com", "duckduckgo.com", "yahoo.com")):
        # google.com hosts gmail too (mail.google), but we matched mail.* above.
        return (host, "search")
    return (host, "other")


def extract_visitor_geo(request: Request) -> dict[str, str]:
    """PR54: free geo capture. Uses Cloudflare's CF-IPCountry header when
    present (free if Cloudflare proxies the origin). Region/city are
    deferred (would need MaxMind GeoLite2 DB). Returns
    {country, region, city} — region/city are "" for now."""
    country = (request.headers.get("cf-ipcountry") or "").strip().upper()[:8]
    # Cloudflare uses "XX" for unknown; treat as missing.
    if country in ("", "XX", "T1"):
        country = ""
    return {"country": country, "region": "", "city": ""}
