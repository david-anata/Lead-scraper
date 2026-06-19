"""Brand & Social Opportunity track — a SEPARATE A–F alongside the financial grade.

Framing: Ascend grades on ACQUISITION OPPORTUNITY, not current presence.
- No social footprint = "A" (maximum build runway — Ascend launches TikTok,
  Instagram, Facebook, YouTube from Day 1 per the integration playbook).
- No email/DTC list = "B" (Ascend builds via Shopify + email from Month 6;
  clean-slate is expected for Amazon-only brands).
- Strong existing social/email = still positive (transferable asset) but less
  incremental upside for Ascend to capture.
- Reviews/reputation: always grades current state — high reviews = proven
  product-market fit, which IS the signal Ascend wants.

Auto-discovery scrapes social links from the brand site (best-effort, never
raises); analyst can supply email-list size + review numbers.
"""

from __future__ import annotations

import logging
import re
from typing import Optional

from sales_support_agent.services.brand_analysis.schema import (
    BRAND_SOCIAL_DIMENSIONS,
    GRADE_POINTS,
    NOT_ASSESSED,
    DimensionGrade,
    Metrics,
    PeriodFinancials,
    Scorecard,
    fmt_pct,
    letter_from_score,
)

logger = logging.getLogger(__name__)

_SOCIAL_PATTERNS = {
    "instagram": re.compile(r"https?://(?:www\.)?instagram\.com/[^\s\"'<>?]+", re.I),
    "tiktok": re.compile(r"https?://(?:www\.)?tiktok\.com/@[^\s\"'<>?]+", re.I),
    "facebook": re.compile(r"https?://(?:www\.)?facebook\.com/[^\s\"'<>?]+", re.I),
    "youtube": re.compile(r"https?://(?:www\.)?youtube\.com/[^\s\"'<>?]+", re.I),
    "twitter": re.compile(r"https?://(?:www\.)?(?:twitter|x)\.com/[^\s\"'<>?]+", re.I),
    "linkedin": re.compile(r"https?://(?:www\.)?linkedin\.com/(?:company|in)/[^\s\"'<>?]+", re.I),
}
_SKIP = ("/sharer", "/share", "intent", "/plugins", "/tr?", "sharer.php")


def discover_socials(website: str) -> dict:
    """Best-effort: fetch the brand site and pull social profile links. Returns
    ``{platform: url}``. Never raises (branding is a nice-to-have)."""
    url = (website or "").strip()
    if not url:
        return {}
    if not url.startswith("http"):
        url = "https://" + url
    try:
        import urllib.request

        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (brand-analysis)"})
        with urllib.request.urlopen(req, timeout=8) as resp:  # noqa: S310 — fixed scheme above
            html = resp.read(600_000).decode("utf-8", errors="replace")
    except Exception:  # noqa: BLE001
        logger.warning("[brand_analysis] social discovery failed for %s", url[:80], exc_info=True)
        return {}
    found: dict = {}
    for platform, pat in _SOCIAL_PATTERNS.items():
        for m in pat.findall(html):
            link = m.rstrip("/\"'")
            if any(s in link.lower() for s in _SKIP):
                continue
            found.setdefault(platform, link)
            break
    return found


# ---------------------------------------------------------------------------
# Per-dimension grading (letter, reason, assessed)
# ---------------------------------------------------------------------------


def _grade_brand_equity(metrics: Metrics, period: PeriodFinancials) -> tuple[str, str, bool]:
    """Defensibility & moat — high margin + owned channel = brand can hold price."""
    pts, proxies = [], []
    if metrics.owned_pct_bps is not None:
        pts.append(3.0 if metrics.owned_pct_bps >= 2000 else 1.5)
        proxies.append(f"owned-channel {fmt_pct(metrics.owned_pct_bps)} of revenue")
    if metrics.product_gm_bps is not None:
        pts.append(3.0 if metrics.product_gm_bps >= 5500 else 1.5)
        proxies.append(f"gross margin {fmt_pct(metrics.product_gm_bps)}")
    if not pts:
        return NOT_ASSESSED, "Defensibility proxies (owned reach, pricing power) not supplied.", False
    avg = sum(pts) / len(pts)
    letter = "A" if avg >= 3.0 else "B" if avg >= 2.5 else "C" if avg >= 1.8 else "D"
    return letter, "Defensibility proxies: " + ", ".join(proxies) + ".", True


def _grade_dtc_opportunity(email_list_size: int) -> tuple[str, str, bool]:
    """DTC & email build opportunity — for Ascend, ABSENCE of a list is HIGH upside.
    No list = Ascend launches Shopify + list-build from scratch at Month 6 (expected
    state for Amazon-only brands). A large existing list is also an asset but means
    less incremental build opportunity for Ascend."""
    n = int(email_list_size or 0)
    if n <= 0:
        return (
            "A",
            "No email/SMS list yet — Ascend launches DTC Shopify + list-build at Month 6. "
            "Clean-slate, full-control audience acquisition opportunity.",
            True,
        )
    if n >= 100_000:
        return "B", f"Substantial owned list of {n:,} contacts — transferable DTC asset, less incremental build upside.", True
    if n >= 50_000:
        return "A", f"Strong owned list of {n:,} contacts — DTC retention program ready to launch.", True
    if n >= 10_000:
        return "B", f"Growing owned list of {n:,} contacts — foundation for Shopify email flows.", True
    if n >= 1_000:
        return "C", f"Small owned list of {n:,} contacts — Ascend builds from here.", True
    return "B", f"Minimal list ({n:,}) — effectively a clean-slate build opportunity.", True


def _grade_social_oppty(social_handles: dict, signals: dict) -> tuple[str, str, bool]:
    """Social channel opportunity — for Ascend, NO social = MAXIMUM opportunity.
    Ascend's integration playbook launches Instagram, TikTok, Facebook, YouTube
    from Day 1. No legacy accounts means full brand voice control and no handover
    complexity. Existing active social is still positive (transferable audience)
    but represents less incremental opportunity for Ascend to capture."""
    handles = {k: v for k, v in (social_handles or {}).items() if v}
    s = signals or {}
    _ASCEND_PLATFORMS = {"instagram", "tiktok", "facebook", "youtube"}

    # Aggregate known follower counts for audience sizing
    _FOLLOWER_KEYS = {
        "instagram": "ig_followers",
        "tiktok":    "tt_followers",
        "facebook":  "fb_followers",
        "youtube":   "yt_subscribers",
    }

    def _fmt(n: int) -> str:
        if n >= 1_000_000:
            return f"{n/1_000_000:.1f}M"
        if n >= 1_000:
            return f"{n//1_000}K"
        return str(n)

    if not handles:
        to_build = sorted(_ASCEND_PLATFORMS)
        return (
            "A",
            f"No social footprint — Ascend builds {', '.join(p.title() for p in to_build)} "
            "from Day 1. Full brand voice control, zero legacy management overhead.",
            True,
        )

    existing = set(handles.keys())
    missing = sorted(_ASCEND_PLATFORMS - existing)
    n = len(handles)
    note = f"{n} platform(s) exist: {', '.join(sorted(handles))}"

    # Append per-platform follower counts when available
    follower_parts = []
    total_followers = 0
    for platform in sorted(existing):
        fol_key = _FOLLOWER_KEYS.get(platform)
        if fol_key and s.get(fol_key):
            count = int(s[fol_key])
            total_followers += count
            follower_parts.append(f"{platform.title()} {_fmt(count)}")
    if follower_parts:
        note += f" ({', '.join(follower_parts)}; {_fmt(total_followers)} total audience)"

    recency = s.get("posting_recency_days")
    if isinstance(recency, (int, float)):
        if recency <= 7:
            note += "; active posting — transferable audience"
        elif recency > 60:
            note += f"; last post ~{int(recency)}d ago (stale, needs reactivation)"
    if missing:
        note += f". Ascend adds {', '.join(p.title() for p in missing)}."
    letter = "B" if n >= 3 else "A" if n < 2 else "B"
    return letter, note + ".", True


def _grade_product_signal(signals: dict) -> tuple[str, str, bool]:
    """Product-market fit — reviews & demand signal. Strong reviews prove the
    product has real demand before Ascend deploys capital. Always graded on
    current state (high reviews = A, no data = not assessed)."""
    s = signals or {}
    rating = s.get("review_rating")
    count = s.get("review_count")
    if rating is None and count is None:
        return NOT_ASSESSED, "No review volume/rating supplied — product-market fit not verified.", False
    try:
        rating = float(rating) if rating is not None else None
        count = int(count) if count is not None else 0
    except (TypeError, ValueError):
        return NOT_ASSESSED, "Review data not parseable.", False
    if rating is None:
        return "C", f"{count:,} reviews (no average rating supplied — verify before LOI).", True
    if rating >= 4.5 and count >= 500:
        letter = "A"
    elif rating >= 4.3 and count >= 100:
        letter = "B"
    elif rating >= 4.0:
        letter = "C"
    elif rating >= 3.5:
        letter = "D"
    else:
        letter = "F"
    return letter, f"{rating:.1f}/5 across {count:,} reviews — {'strong' if letter in ('A','B') else 'weak'} product-market fit signal.", True


_LETTERS = ("A", "B", "C", "D", "F")


def _step_down(letter: str) -> str:
    i = min(len(_LETTERS) - 1, _LETTERS.index(letter) + 1) if letter in _LETTERS else 3
    return _LETTERS[i]


# ---------------------------------------------------------------------------
# Track assembly
# ---------------------------------------------------------------------------


def build_brand_social(
    metrics: Metrics,
    period: PeriodFinancials,
    *,
    email_list_size: int = 0,
    social_handles: Optional[dict] = None,
    social_signals: Optional[dict] = None,
) -> dict:
    """Compute the Acquisition Opportunity scorecard (Brand & Social track).
    Returns a dict with dimensions, score_100, letter, confidence, and caveats.

    High score = HIGH acquisition opportunity for Ascend.
    Absence of social/email is graded as OPPORTUNITY, not as a deficit.
    """
    social_handles = social_handles or {}
    social_signals = social_signals or {}
    graders = {
        "brand_equity":    lambda: _grade_brand_equity(metrics, period),
        "dtc_opportunity": lambda: _grade_dtc_opportunity(email_list_size),
        "social_oppty":    lambda: _grade_social_oppty(social_handles, social_signals),
        "product_signal":  lambda: _grade_product_signal(social_signals),
    }
    dims: list[DimensionGrade] = []
    weighted = 0.0
    assessed_weight = 0.0
    for key, label, weight in BRAND_SOCIAL_DIMENSIONS:
        letter, reason, assessed = graders[key]()
        points = GRADE_POINTS[letter] if assessed else 0.0
        weighted += points * weight
        if assessed:
            assessed_weight += weight
        dims.append(DimensionGrade(key=key, label=label, weight=weight, letter=letter,
                                   points=points, reason=reason, assessed=assessed))
    score_100 = int(round(weighted / 4.0 * 100))
    letter = letter_from_score(score_100)
    confidence = "High" if assessed_weight >= 0.85 else "Medium" if assessed_weight >= 0.5 else "Low"
    caveats = [
        "Social Opportunity grade is separate from the financial acquisition grade — "
        "it does NOT affect the composite A–F score.",
        "High score = maximum growth opportunity for Ascend. No social footprint is "
        "graded as an OPPORTUNITY (Ascend builds it), not a deficit.",
    ]
    sc = Scorecard(dimensions=dims, score_100=score_100, letter=letter)
    out = sc.to_dict()
    out["confidence"] = confidence
    out["assessed_weight_pct"] = int(round(assessed_weight * 100))
    out["caveats"] = caveats
    return out
