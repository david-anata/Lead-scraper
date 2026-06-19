"""Indicative valuation ranges for the investor package.

Deterministic, transparent, and explicitly caveated — this is *analysis*, not a
formal valuation or investment advice. We apply category-typical multiples to
two independent bases and report a blended band:

  * Revenue multiple  × net revenue
  * Earnings multiple  × a normalised earnings proxy (operating result
    excluding non-recurring other income, falling back to net earnings)

The band is shifted by the letter grade (quality premium/discount) and widened
when data completeness is low, so a thin dataset yields a wider, softer range
with a louder caveat. Every output carries the caveats list verbatim.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from sales_support_agent.services.brand_analysis.schema import (
    CATEGORY_DTC,
    Metrics,
    fmt_money,
)

# Base multiple bands by category: (revenue_low, revenue_high), (earn_low, earn_high).
# Earnings multiple is applied to an EBITDA/SDE-style proxy.
# Ascend acquires DTC/FBA brands at 2–3.5× SDE — use that range for the
# earnings multiple. Revenue multiple stays as a secondary check.
_MULTIPLES = {
    CATEGORY_DTC:    {"rev": (0.8, 2.0), "earn": (2.0, 3.5)},
    "retail":        {"rev": (0.5, 1.2), "earn": (3.0, 4.5)},
    "saas":          {"rev": (3.0, 8.0), "earn": (8.0, 15.0)},
    "other":         {"rev": (0.6, 1.5), "earn": (2.0, 3.5)},
}

# Quality premium/discount applied to both ends of every band.
_GRADE_FACTOR = {"A": 1.20, "B": 1.05, "C": 0.90, "D": 0.70, "F": 0.50}

# Below this completeness the band is widened and flagged low-confidence.
_THIN_DATA_PCT = 60
_THIN_WIDEN = 0.25  # ±25% on each end


@dataclass
class ValuationRange:
    primary_basis: str = "revenue"            # "revenue" | "earnings" | "blended"
    ev_low_cents: Optional[int] = None        # blended enterprise-value band
    ev_high_cents: Optional[int] = None
    rev_multiple_low: Optional[float] = None
    rev_multiple_high: Optional[float] = None
    rev_ev_low_cents: Optional[int] = None
    rev_ev_high_cents: Optional[int] = None
    earn_multiple_low: Optional[float] = None
    earn_multiple_high: Optional[float] = None
    earn_ev_low_cents: Optional[int] = None
    earn_ev_high_cents: Optional[int] = None
    earnings_basis_label: str = ""            # what the earnings proxy was
    earnings_basis_cents: Optional[int] = None
    inventory_cents: Optional[int] = None     # balance-sheet inventory at cost (additive to EV)
    confidence: str = "Low"                   # mirrors data confidence
    caveats: list = field(default_factory=list)

    def is_meaningful(self) -> bool:
        return self.ev_low_cents is not None and self.ev_high_cents is not None

    def headline(self) -> str:
        if not self.is_meaningful():
            return "Insufficient data for an indicative range"
        return f"{fmt_money(self.ev_low_cents)} – {fmt_money(self.ev_high_cents)}"

    def to_dict(self) -> dict:
        return dict(self.__dict__)

    @classmethod
    def from_dict(cls, data: dict) -> "ValuationRange":
        obj = cls()
        for k, v in (data or {}).items():
            if hasattr(obj, k):
                setattr(obj, k, v)
        return obj


def _round_cents(x: float) -> int:
    """Round an EV to a tidy 2 significant-ish figures so we never imply false
    precision (e.g. $1,284,113 -> $1,300,000)."""
    if x <= 0:
        return 0
    import math
    digits = int(math.floor(math.log10(abs(x))))
    step = 10 ** max(digits - 1, 2)  # round to nearest $1k at minimum
    return int(round(x / step) * step)


def estimate(
    metrics: Metrics,
    *,
    category: str = CATEGORY_DTC,
    grade: str = "C",
    data_completeness_pct: int = 0,
) -> ValuationRange:
    cat = (category or CATEGORY_DTC).lower()
    bands = _MULTIPLES.get(cat, _MULTIPLES[CATEGORY_DTC])
    gf = _GRADE_FACTOR.get((grade or "C").upper(), 0.90)
    thin = data_completeness_pct < _THIN_DATA_PCT
    widen = _THIN_WIDEN if thin else 0.0

    out = ValuationRange(confidence=_confidence(data_completeness_pct))
    caveats = [
        "Indicative only — a directional range from category multiples, not a "
        "formal valuation, fairness opinion, or investment advice.",
    ]

    rev = metrics.net_revenue_cents
    # --- Revenue-multiple basis ---------------------------------------------
    if rev and rev > 0:
        lo = bands["rev"][0] * gf * (1 - widen)
        hi = bands["rev"][1] * gf * (1 + widen)
        out.rev_multiple_low, out.rev_multiple_high = round(lo, 2), round(hi, 2)
        out.rev_ev_low_cents = _round_cents(rev * lo)
        out.rev_ev_high_cents = _round_cents(rev * hi)
    else:
        caveats.append("Net revenue not established — revenue-multiple basis omitted.")

    # --- Earnings-multiple basis (normalised) -------------------------------
    proxy, proxy_label = _earnings_proxy(metrics)
    out.earnings_basis_label = proxy_label
    out.earnings_basis_cents = proxy
    if proxy is not None and proxy > 0:
        lo = bands["earn"][0] * gf * (1 - widen)
        hi = bands["earn"][1] * gf * (1 + widen)
        out.earn_multiple_low, out.earn_multiple_high = round(lo, 2), round(hi, 2)
        out.earn_ev_low_cents = _round_cents(proxy * lo)
        out.earn_ev_high_cents = _round_cents(proxy * hi)
    elif proxy is not None and proxy <= 0:
        caveats.append(
            f"{proxy_label} is negative ({fmt_money(proxy)}) — earnings-multiple "
            "basis is not meaningful; range leans on the revenue basis with a "
            "turnaround discount."
        )

    # --- Blend ---------------------------------------------------------------
    rev_pair = (out.rev_ev_low_cents, out.rev_ev_high_cents)
    earn_pair = (out.earn_ev_low_cents, out.earn_ev_high_cents)
    have_rev = all(v is not None for v in rev_pair)
    have_earn = all(v is not None for v in earn_pair)

    if have_rev and have_earn:
        out.primary_basis = "blended"
        out.ev_low_cents = _round_cents(min(rev_pair[0], earn_pair[0]))
        out.ev_high_cents = _round_cents((rev_pair[1] + earn_pair[1]) / 2)
    elif have_earn:
        out.primary_basis = "earnings"
        out.ev_low_cents, out.ev_high_cents = earn_pair
    elif have_rev:
        out.primary_basis = "revenue"
        out.ev_low_cents, out.ev_high_cents = rev_pair
    else:
        caveats.append("No usable revenue or earnings basis — provide a P&L to size a range.")

    # Inventory is additive to EV (negotiated at close, typically at cost)
    if metrics.inventory_cents and metrics.inventory_cents > 0:
        out.inventory_cents = metrics.inventory_cents

    if thin and out.is_meaningful():
        caveats.append(
            f"Data completeness is {data_completeness_pct}% — band widened ±25% and "
            "should tighten materially with prior-year P&L, channel-level "
            "marketing, and cohort/LTV data."
        )
    out.caveats = caveats
    return out


def _earnings_proxy(metrics: Metrics) -> tuple[Optional[int], str]:
    """For FBA acquisitions the primary valuation basis is SDE (Seller
    Discretionary Earnings = net earnings + owner comp + D&A + addbacks).
    Falls back to operating result ex-other, then reported net earnings."""
    if metrics.sde_cents is not None and metrics.sde_cents > 0:
        return metrics.sde_cents, "SDE (net earnings + owner addbacks)"
    if metrics.operating_result_ex_other_cents is not None:
        return metrics.operating_result_ex_other_cents, "Operating result ex-other-income"
    if metrics.net_earnings_cents is not None:
        return metrics.net_earnings_cents, "Reported net earnings"
    return None, "Earnings (not supplied)"


def _confidence(pct: int) -> str:
    if pct >= 85:
        return "High"
    if pct >= 60:
        return "Medium"
    return "Low"


def deal_value(ask_price_cents: int, ev_low_cents: int, ev_high_cents: int) -> dict:
    """Price vs. Ascend EV range. Returns ratio, label, and tone."""
    ev_mid = (ev_low_cents + ev_high_cents) / 2
    ratio = round(ask_price_cents / ev_mid, 2)
    if ratio <= 0.50:
        return {"ratio": ratio, "label": "Deep Value", "tone": "great"}
    if ratio <= 0.70:
        return {"ratio": ratio, "label": "Discounted", "tone": "good"}
    if ratio <= 1.00:
        return {"ratio": ratio, "label": "Fair Value", "tone": "neutral"}
    if ratio <= 1.25:
        return {"ratio": ratio, "label": "At Premium", "tone": "caution"}
    return {"ratio": ratio, "label": "Overpriced", "tone": "bad"}


def deal_recommendation(grade: str, ratio: float) -> str:
    """Grade + price combined acquisition recommendation."""
    if ratio <= 0.50:
        return "Strong Buy" if grade in ("A", "B", "C") else "Distressed Offer"
    if ratio <= 0.70:
        if grade == "A": return "Strong Buy"
        if grade in ("B", "C"): return "Buy"
        return "Distressed Offer"
    if ratio <= 1.00:
        return {"A": "Strong Buy", "B": "Buy", "C": "Conditional",
                "D": "Proceed Carefully", "F": "Pass"}.get(grade, "Pass")
    if ratio <= 1.25:
        if grade == "A": return "Buy"
        if grade == "B": return "Conditional"
        return "Pass"
    return "Conditional" if grade == "A" else "Pass"
