"""Brand & Social track — a SEPARATE A–F shown alongside the financial grade.

Soft, often-incomplete signals (scraped + analyst-supplied), deliberately kept
out of the deterministic financial grade. Same penalise-unknowns rule: a
dimension we can't measure scores zero (assessed=False) so a thin social
footprint reads honestly, with the confidence meter carrying the uncertainty.
Every signal is tagged measured-vs-estimated; nothing is fabricated.

Auto-discovery scrapes social profile links from the brand site (best-effort,
never raises); the analyst can override and supply the email-list size + review
numbers that public pages don't reliably expose.
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


def _grade_owned_audience(email_list_size: int) -> tuple[str, str, bool]:
    n = int(email_list_size or 0)
    if n <= 0:
        return NOT_ASSESSED, "Email/SMS list size not supplied — owned-audience strength unknown.", False
    if n >= 100_000:
        letter = "A"
    elif n >= 50_000:
        letter = "B"
    elif n >= 10_000:
        letter = "C"
    elif n >= 1_000:
        letter = "D"
    else:
        letter = "F"
    return letter, f"Owned list of {n:,} contacts (analyst-supplied).", True


def _grade_social_presence(social_handles: dict, signals: dict) -> tuple[str, str, bool]:
    handles = {k: v for k, v in (social_handles or {}).items() if v}
    if not handles:
        return NOT_ASSESSED, "No social profiles found or supplied — presence unknown.", False
    n = len(handles)
    letter = "A" if n >= 4 else "B" if n == 3 else "C" if n == 2 else "D"
    note = f"{n} platform(s): {', '.join(sorted(handles))}"
    recency = (signals or {}).get("posting_recency_days")
    if isinstance(recency, (int, float)):
        if recency <= 7:
            note += "; posting in the last week"
        elif recency > 60:
            letter = _step_down(letter)
            note += f"; last post ~{int(recency)}d ago (stale)"
    return letter, note + ".", True


def _grade_social_reputation(signals: dict) -> tuple[str, str, bool]:
    s = signals or {}
    rating = s.get("review_rating")
    count = s.get("review_count")
    if rating is None and count is None:
        return NOT_ASSESSED, "No review volume/rating supplied (Yotpo/Okendo/Trustpilot etc.).", False
    try:
        rating = float(rating) if rating is not None else None
        count = int(count) if count is not None else 0
    except (TypeError, ValueError):
        return NOT_ASSESSED, "Review data not parseable.", False
    if rating is None:
        return "C", f"{count:,} reviews (no average rating supplied).", True
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
    return letter, f"{rating:.1f}/5 across {count:,} reviews.", True


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
    """Compute the separate Brand & Social scorecard. Returns a dict with
    dimensions, score_100, letter, confidence, and caveats."""
    social_handles = social_handles or {}
    social_signals = social_signals or {}
    graders = {
        "brand_equity": lambda: _grade_brand_equity(metrics, period),
        "owned_audience": lambda: _grade_owned_audience(email_list_size),
        "social_presence": lambda: _grade_social_presence(social_handles, social_signals),
        "social_reputation": lambda: _grade_social_reputation(social_signals),
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
        "Brand & Social is a separate read — it does NOT affect the financial "
        "acquisition grade.",
        "Social metrics are best-effort: public follower/engagement numbers are "
        "unreliable, so this leans on owned-list size and review data; verify "
        "anything marked estimated.",
    ]
    sc = Scorecard(dimensions=dims, score_100=score_100, letter=letter)
    out = sc.to_dict()
    out["confidence"] = confidence
    out["assessed_weight_pct"] = int(round(assessed_weight * 100))
    out["caveats"] = caveats
    return out
