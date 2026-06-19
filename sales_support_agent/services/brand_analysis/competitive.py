"""Amazon competitive position grader — third A–F track alongside financial
and social/DTC opportunity.

All signals are analyst-supplied (BSR, review counts, price vs. category
median). Returns a dict mirroring the brand_social structure so the share
page can render it uniformly.

Framing: high grade = STRONG COMPETITIVE POSITION (moat, pricing power, rank).
This differs from the social/DTC track (high = more build opportunity) — here
high = the brand already dominates its shelf space.
"""

from __future__ import annotations

from sales_support_agent.services.brand_analysis.schema import (
    CompetitiveSignals,
    GRADE_POINTS,
    letter_from_score,
)


def build_competitive_grade(signals: CompetitiveSignals) -> dict:
    """Compute competitive position scorecard from analyst-supplied signals.

    Returns a dict with: letter, score_100, confidence, assessed_weight_pct,
    dimensions, competitors, analyst_notes.
    """
    dims: list[dict] = []
    NA = "NA"

    # 1. Review moat — brand vs. top competitor (30% weight)
    bc = signals.brand_review_count
    tc = signals.top_competitor_review_count
    if bc is not None and tc is not None and tc > 0:
        ratio = bc / tc
        if ratio >= 2.0:
            letter = "A"
            reason = (f"Brand has {bc:,} reviews vs. top competitor {tc:,} "
                      f"({ratio:.1f}× moat) — dominant position, hard to dislodge.")
        elif ratio >= 1.0:
            letter = "B"
            reason = (f"Brand has {bc:,} reviews vs. top competitor {tc:,} "
                      f"(~parity) — strong presence, maintain post-acquisition.")
        elif ratio >= 0.5:
            letter = "C"
            reason = (f"Brand has {bc:,} reviews vs. top competitor {tc:,} "
                      f"({ratio:.1f}×) — building the moat; PPC + product seeding will close gap.")
        else:
            letter = "D"
            reason = (f"Brand has {bc:,} reviews vs. top competitor {tc:,} "
                      f"({ratio:.1f}×) — significant review gap; needs multi-year program to close.")
        dims.append(_dim("review_moat", "Review volume moat", 0.30, letter, reason, True))
    elif bc is not None:
        letter = "C" if bc >= 500 else "D" if bc >= 100 else "F"
        reason = f"Brand has {bc:,} reviews; no top-competitor count to compare."
        dims.append(_dim("review_moat", "Review volume moat", 0.30, letter, reason, True))
    else:
        dims.append(_dim("review_moat", "Review volume moat", 0.30, NA,
                         "Review count not supplied.", False))

    # 2. Product rating quality (25% weight)
    r = signals.brand_review_rating
    if r is not None:
        try:
            r = float(r)
            if r >= 4.5:
                letter, qual = "A", "best-in-class product-market fit"
            elif r >= 4.3:
                letter, qual = "B", "meets Ascend minimum threshold"
            elif r >= 4.0:
                letter, qual = "C", "acceptable but below Ascend's 4.3 target"
            elif r >= 3.5:
                letter, qual = "D", "below Ascend minimum — product quality risk"
            else:
                letter, qual = "F", "critical — hard disqualifier for Ascend"
            reason = f"{r:.1f}/5 average rating — {qual}."
            dims.append(_dim("rating_quality", "Product rating quality", 0.25, letter, reason, True))
        except (TypeError, ValueError):
            dims.append(_dim("rating_quality", "Product rating quality", 0.25, NA,
                             "Rating value not parseable.", False))
    else:
        dims.append(_dim("rating_quality", "Product rating quality", 0.25, NA,
                         "Review rating not supplied.", False))

    # 3. Price positioning vs. category median (25% weight)
    bp = signals.brand_price_cents
    mp = signals.category_median_price_cents
    if bp is not None and mp is not None and mp > 0:
        premium_bps = int((bp - mp) / mp * 10000)
        if premium_bps >= 2000:
            letter = "A"
            reason = f"Brand prices {premium_bps / 100:.0f}% above category median — strong pricing power."
        elif premium_bps >= 0:
            letter = "B"
            reason = f"Brand prices at or slight premium to category median — healthy positioning."
        elif premium_bps >= -1000:
            letter = "C"
            reason = (f"Brand prices {abs(premium_bps) / 100:.0f}% below category median "
                      "— some discount dependency, monitor post-acquisition.")
        else:
            letter = "D"
            reason = (f"Brand prices {abs(premium_bps) / 100:.0f}% below category median "
                      "— potential race-to-bottom risk; price recovery plan needed.")
        dims.append(_dim("price_positioning", "Price positioning vs. category", 0.25, letter, reason, True))
    else:
        dims.append(_dim("price_positioning", "Price positioning vs. category", 0.25, NA,
                         "Brand price or category median not supplied.", False))

    # 4. Amazon BSR rank (20% weight)
    bsr = signals.brand_bsr
    if bsr is not None:
        if bsr <= 100:
            letter = "A"
            reason = f"BSR #{bsr:,} — top-tier category rank."
        elif bsr <= 500:
            letter = "B"
            reason = f"BSR #{bsr:,} — strong subcategory position."
        elif bsr <= 2_000:
            letter = "C"
            reason = f"BSR #{bsr:,} — established brand with room to grow."
        elif bsr <= 10_000:
            letter = "D"
            reason = f"BSR #{bsr:,} — moderate subcategory rank; PPC + review program can improve."
        else:
            letter = "F"
            reason = f"BSR #{bsr:,} — weak rank; high PPC investment needed to gain shelf presence."
        dims.append(_dim("bsr_rank", "Amazon BSR rank", 0.20, letter, reason, True))
    else:
        dims.append(_dim("bsr_rank", "Amazon BSR rank", 0.20, NA,
                         "BSR not supplied.", False))

    # Aggregate
    weighted = sum(d["weight"] * GRADE_POINTS.get(d["letter"], 0.0) for d in dims)
    assessed_w = sum(d["weight"] for d in dims if d["assessed"])
    score_100 = int(round(weighted / 4.0 * 100))
    letter = letter_from_score(score_100)
    confidence = "High" if assessed_w >= 0.85 else "Medium" if assessed_w >= 0.5 else "Low"

    return {
        "letter": letter,
        "score_100": score_100,
        "confidence": confidence,
        "assessed_weight_pct": int(round(assessed_w * 100)),
        "dimensions": dims,
        "competitors": signals.competitors or [],
        "top_competitor_name": signals.top_competitor_name or "",
        "analyst_notes": signals.analyst_notes or "",
    }


def _dim(key, label, weight, letter, reason, assessed):
    return {
        "key": key,
        "label": label,
        "weight": weight,
        "letter": letter,
        "reason": reason,
        "assessed": assessed,
    }
