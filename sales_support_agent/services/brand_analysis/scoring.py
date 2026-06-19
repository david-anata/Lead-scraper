"""Deterministic grading engine for Brand Analysis.

Pure functions over the parsed ``PeriodFinancials`` — no LLM, no I/O — so the
same inputs always yield the same grade (the property the tests pin). Derives
the KPI set, grades each of the 8 weighted dimensions against the category
benchmarks *and direction of travel*, rebases to /100, and produces the
scorecard, ranked red flags, and the PASS/FAIL benchmark table.
"""

from __future__ import annotations

from typing import Optional

from sales_support_agent.services.brand_analysis.schema import (
    Benchmarks,
    BenchmarkRow,
    DimensionGrade,
    DIMENSIONS,
    GRADE_POINTS,
    Metrics,
    NOT_ASSESSED,
    PeriodFinancials,
    RedFlag,
    Scorecard,
    SEV_CRITICAL,
    SEV_HIGH,
    SEV_MEDIUM,
    benchmarks_for,
    fmt_money,
    fmt_mult,
    fmt_pct,
    letter_from_score,
    margin_bps,
    safe_div,
    sort_red_flags,
)

_LETTERS = ("A", "B", "C", "D", "F")


def _step(letter: str, down: int) -> str:
    """Move a letter grade `down` notches toward F (negative = toward A)."""
    idx = max(0, min(len(_LETTERS) - 1, _LETTERS.index(letter) + down))
    return _LETTERS[idx]


# ---------------------------------------------------------------------------
# Metric derivation
# ---------------------------------------------------------------------------


def derive_metrics(p: PeriodFinancials) -> Metrics:
    m = Metrics()
    net_rev = p.net_revenue_or_derived()
    m.net_revenue_cents = net_rev
    m.cogs_cents = p.cogs_cents
    if net_rev is not None and p.cogs_cents is not None:
        m.product_gross_profit_cents = net_rev - p.cogs_cents
        m.product_gm_bps = margin_bps(m.product_gross_profit_cents, net_rev)
    m.marketing_total_cents = p.marketing_total_cents
    m.marketing_pct_bps = margin_bps(p.marketing_total_cents, net_rev)
    m.blended_mer = safe_div(net_rev, p.marketing_total_cents)
    m.reported_gross_profit_cents = p.reported_gross_profit_cents
    m.contribution_margin_bps = margin_bps(p.reported_gross_profit_cents, net_rev)
    m.opex_cents = p.opex_cents
    m.net_earnings_cents = p.net_earnings_cents
    m.net_margin_bps = margin_bps(p.net_earnings_cents, net_rev)
    if p.net_earnings_cents is not None:
        m.operating_result_ex_other_cents = p.net_earnings_cents - (p.other_income_cents or 0)
    m.discount_rate_bps = margin_bps(p.discounts_cents, p.gross_sales_cents)
    m.return_rate_bps = margin_bps(p.returns_cents, p.gross_sales_cents)
    m.owned_pct_bps = margin_bps(p.owned_channel_revenue_cents, net_rev)
    # SDE = net earnings + owner compensation + depreciation + one-time addbacks
    if p.net_earnings_cents is not None:
        m.sde_cents = (
            p.net_earnings_cents
            + (p.owner_compensation_cents or 0)
            + (p.depreciation_cents or 0)
            + (p.addback_items_cents or 0)
        )
        m.sde_margin_bps = margin_bps(m.sde_cents, net_rev)
    return m


def yoy_growth_bps(current: Metrics, prior: Optional[Metrics]) -> Optional[int]:
    if prior is None:
        return None
    return margin_bps(
        (current.net_revenue_cents or 0) - (prior.net_revenue_cents or 0),
        prior.net_revenue_cents,
    )


# ---------------------------------------------------------------------------
# Per-dimension grading. Each returns (letter, reason).
# ---------------------------------------------------------------------------


def _grade_band(value: Optional[int], low: int, high: int) -> Optional[str]:
    """Higher-is-better band: >=high A, [low,high) B, [low/2,low) C, [0,low/2) D, <0 F."""
    if value is None:
        return None
    if value >= high:
        return "A"
    if value >= low:
        return "B"
    if value >= low / 2:
        return "C"
    if value >= 0:
        return "D"
    return "F"


def _grade_revenue(cur: Metrics, prior: Optional[Metrics], growth_bps: Optional[int], bm: Benchmarks) -> tuple[str, str]:
    if growth_bps is None:
        return NOT_ASSESSED, "No prior-year revenue supplied — trajectory not assessable (penalised pending data)."
    g = growth_bps / 100
    if growth_bps >= 2000:
        letter = "A"
    elif growth_bps >= 1000:
        letter = "B"
    elif growth_bps >= bm.yoy_growth_min_bps:
        letter = "C"
    elif growth_bps >= -1000:
        letter = "D"
    else:
        letter = "F"
    direction = "growing" if growth_bps >= 0 else "declining"
    return letter, f"Net revenue {direction} {g:+.1f}% YoY ({fmt_money(prior.net_revenue_cents)} → {fmt_money(cur.net_revenue_cents)})."


def _grade_margin_like(value_bps: Optional[int], low: int, high: int, prior_bps: Optional[int], label: str) -> tuple[str, str]:
    letter = _grade_band(value_bps, low, high)
    if letter is None:
        return NOT_ASSESSED, f"{label} not derivable from the supplied data (penalised pending data)."
    reason = f"{label} {fmt_pct(value_bps)} vs healthy {low/100:.0f}–{high/100:.0f}%."
    # Direction of travel: a declining-but-positive metric grades worse than stable.
    if prior_bps is not None and value_bps is not None and value_bps < prior_bps - 100:
        letter = _step(letter, 1)
        reason += f" Declining from {fmt_pct(prior_bps)} (down-weighted)."
    elif prior_bps is not None and value_bps is not None and value_bps > prior_bps + 100:
        reason += f" Improving from {fmt_pct(prior_bps)}."
    return letter, reason


def _grade_marketing(cur: Metrics, prior: Optional[Metrics], bm: Benchmarks) -> tuple[str, str]:
    mer = cur.blended_mer
    if mer is None:
        return NOT_ASSESSED, "Blended MER not derivable (marketing spend or revenue missing) — penalised pending data."
    if mer >= bm.blended_mer_min * 1.5:
        letter = "A"
    elif mer >= bm.blended_mer_min:
        letter = "B"
    elif mer >= bm.blended_mer_min * 0.7:
        letter = "C"
    elif mer >= bm.blended_mer_min * 0.5:
        letter = "D"
    else:
        letter = "F"
    reason = f"Blended MER {fmt_mult(mer)} vs ≥{bm.blended_mer_min:.1f}x healthy"
    if cur.marketing_pct_bps is not None:
        reason += f"; marketing {fmt_pct(cur.marketing_pct_bps)} of revenue."
    else:
        reason += "."
    if prior and prior.blended_mer is not None and mer < prior.blended_mer - 0.2:
        letter = _step(letter, 1)
        reason += f" Efficiency declining from {fmt_mult(prior.blended_mer)} (down-weighted)."
    return letter, reason


def _grade_acquisition(cur: Metrics, period: PeriodFinancials, bm: Benchmarks) -> tuple[str, str]:
    # Ascend grading: owned-channel % is NOT scored here — 0% owned is the
    # expected Amazon FBA baseline that Ascend's DTC playbook builds post-acquisition.
    # Only hard quality signals are used: return rate, discount rate, repeat buyers.
    signals = []
    score = 0.0
    n = 0
    if cur.return_rate_bps is not None:
        n += 1
        score += 3.0 if cur.return_rate_bps <= bm.return_rate_max_bps else 1.0
        signals.append(f"return rate {fmt_pct(cur.return_rate_bps)}")
    if cur.discount_rate_bps is not None:
        n += 1
        lo, hi = bm.discount_rate_bps
        score += 3.0 if cur.discount_rate_bps <= hi else 1.0
        signals.append(f"discount rate {fmt_pct(cur.discount_rate_bps)}")
    if period.new_customer_revenue_cents is not None and period.returning_customer_revenue_cents is not None:
        n += 1
        total = period.new_customer_revenue_cents + period.returning_customer_revenue_cents
        ret = safe_div(period.returning_customer_revenue_cents, total) or 0
        score += 3.0 if ret >= 0.3 else 1.5
        signals.append(f"returning-customer share {ret*100:.0f}%")
    if n == 0:
        return NOT_ASSESSED, "Return rate, discount rate, and repeat-customer split not supplied — product quality signals not assessable (penalised pending data)."
    avg = score / n
    letter = "A" if avg >= 3.0 else "B" if avg >= 2.5 else "C" if avg >= 1.8 else "D" if avg >= 1.0 else "F"
    return letter, "Product quality signals: " + ", ".join(signals) + "."


def _grade_media(period: PeriodFinancials, bm: Benchmarks) -> tuple[str, str]:
    # Ascend framing: Amazon-only ad concentration is an OPPORTUNITY (Ascend's
    # integration playbook adds TikTok, DTC, Walmart post-acquisition). Single-
    # channel = maximum expansion runway = A. Multi-channel = also good (B).
    channels = period.marketing_by_channel or {}
    real_channels = {k: v for k, v in channels.items() if v > 0}
    if not real_channels:
        # No channel data — grade neutral rather than penalise; Amazon FBA brands
        # often have only a single "Amazon PPC" line, not per-channel breakdowns.
        return "B", "Channel-level ad data not supplied — graded neutral. Ascend adds TikTok, DTC, and Walmart channels post-acquisition regardless of current mix."
    n = len(real_channels)
    if n == 1:
        ch = next(iter(real_channels))
        return "A", (
            f"Single channel ({ch}) — maximum expansion runway for Ascend. "
            "Integration playbook: add TikTok + DTC at Month 6, Walmart at Year 1."
        )
    top_channel, top_spend = max(real_channels.items(), key=lambda kv: kv[1])
    total = sum(real_channels.values()) or 1
    share = top_spend / total
    return "B", (
        f"{n} channels; top '{top_channel}' = {share*100:.0f}% of spend. "
        "Already multi-channel — Ascend expands further post-acquisition."
    )


def _grade_balance(period: PeriodFinancials, cur: Metrics, bm: Benchmarks) -> tuple[str, str]:
    if period.total_assets_cents is None and period.total_equity_cents is None and not period.related_party_flag:
        return NOT_ASSESSED, "Balance sheet not supplied — earnings quality not assessable (penalised pending data)."
    letter = "B"
    notes = []
    if period.total_equity_cents is not None and period.total_equity_cents < 0:
        letter = "F"
        notes.append(f"negative equity ({fmt_money(period.total_equity_cents)})")
    if period.intercompany_cents and period.total_assets_cents:
        ic_share = abs(period.intercompany_cents) / max(1, period.total_assets_cents)
        if ic_share >= 0.2:
            letter = _step(letter, 2)
            notes.append(f"intercompany balances {ic_share*100:.0f}% of assets")
        elif ic_share >= 0.05:
            letter = _step(letter, 1)
            notes.append(f"intercompany balances {ic_share*100:.0f}% of assets")
    if period.related_party_flag:
        letter = _step(letter, 1)
        notes.append("related-party items present")
    if period.dividends_cents and cur.net_earnings_cents and period.dividends_cents > cur.net_earnings_cents > 0:
        letter = _step(letter, 1)
        notes.append("distributions exceed net earnings")
    if not notes:
        notes.append("no related-party or equity flags detected")
    return letter, "Earnings quality: " + ", ".join(notes) + "."


def _grade_brand(cur: Metrics, period: PeriodFinancials) -> tuple[str, str]:
    # Defensibility: gross margin (pricing power) + returning-customer share (loyalty).
    # owned_pct_bps is NOT used — 0% owned is the expected Amazon-only baseline
    # and is graded as opportunity in the social track, not penalised here.
    proxies = []
    pts = []
    if cur.product_gm_bps is not None:
        pts.append(3.0 if cur.product_gm_bps >= 5500 else 2.0 if cur.product_gm_bps >= 4000 else 1.0)
        proxies.append(f"gross margin {fmt_pct(cur.product_gm_bps)}")
    if period.new_customer_revenue_cents is not None and period.returning_customer_revenue_cents is not None:
        total = period.new_customer_revenue_cents + period.returning_customer_revenue_cents
        ret = safe_div(period.returning_customer_revenue_cents, total) or 0
        pts.append(3.0 if ret >= 0.35 else 2.0 if ret >= 0.20 else 1.0)
        proxies.append(f"returning-customer share {ret*100:.0f}%")
    if not pts:
        return "C", "Brand defensibility signals (gross margin, repeat rate) not supplied — graded neutral."
    avg = sum(pts) / len(pts)
    letter = "A" if avg >= 3.0 else "B" if avg >= 2.5 else "C" if avg >= 1.8 else "D"
    return letter, "Brand defensibility: " + ", ".join(proxies) + "."


# ---------------------------------------------------------------------------
# Scorecard assembly
# ---------------------------------------------------------------------------


def build_scorecard(current: Metrics, prior: Optional[Metrics], period: PeriodFinancials,
                    growth_bps: Optional[int], bm: Benchmarks) -> Scorecard:
    prior = prior or Metrics()
    graders = {
        "revenue": lambda: _grade_revenue(current, prior, growth_bps, bm),
        "profitability": lambda: _grade_margin_like(current.net_margin_bps, *bm.net_margin_bps, prior.net_margin_bps, "Net margin"),
        "marketing": lambda: _grade_marketing(current, prior, bm),
        "acquisition": lambda: _grade_acquisition(current, period, bm),
        "media": lambda: _grade_media(period, bm),
        "contribution": lambda: _grade_margin_like(current.contribution_margin_bps, *bm.contribution_margin_bps, prior.contribution_margin_bps, "Contribution margin"),
        "balance": lambda: _grade_balance(period, current, bm),
        "brand": lambda: _grade_brand(current, period),
    }
    dims: list[DimensionGrade] = []
    weighted_points = 0.0
    for key, label, weight in DIMENSIONS:
        letter, reason = graders[key]()
        assessed = letter != NOT_ASSESSED
        # Unassessed dimensions score ZERO (penalty) but keep their weight in the
        # denominator, so incomplete data drags the grade toward F. The
        # data-completeness meter explains *why* the grade is low.
        points = GRADE_POINTS[letter] if assessed else 0.0
        weighted_points += points * weight
        dims.append(DimensionGrade(key=key, label=label, weight=weight, letter=letter,
                                   points=points, reason=reason, assessed=assessed))
    score_100 = int(round(weighted_points / 4.0 * 100))
    letter = letter_from_score(score_100)
    return Scorecard(dimensions=dims, score_100=score_100, letter=letter)


# ---------------------------------------------------------------------------
# Red flags + benchmark table
# ---------------------------------------------------------------------------


def build_red_flags(current: Metrics, period: PeriodFinancials, growth_bps: Optional[int],
                    bm: Benchmarks, *, social_signals: Optional[dict] = None) -> list:
    flags: list[RedFlag] = []

    def add(sev, title, detail=""):
        flags.append(RedFlag(severity=sev, title=title, detail=detail))

    # ---- Ascend hard-criteria disqualifiers (checked first; any Critical = Pass) ----
    if current.net_revenue_cents is not None and current.net_revenue_cents < 100_000_000:
        add(SEV_CRITICAL, "Below minimum revenue ($1M+ required)",
            f"Net revenue {fmt_money(current.net_revenue_cents)} — Ascend requires $1M+ LTM net revenue.")
    ss = social_signals or {}
    rating = ss.get("review_rating")
    if rating is not None:
        try:
            rating = float(rating)
            if rating < 4.3:
                add(SEV_CRITICAL, "Review rating below Ascend minimum (4.3+ required)",
                    f"{rating:.1f}/5 average rating — poor reviews indicate product-market fit issues "
                    "that are difficult to reverse post-acquisition.")
        except (TypeError, ValueError):
            pass
    if period.tacos_bps is not None and period.tacos_bps > 1500:
        add(SEV_HIGH, "Total ACoS above Ascend threshold (<15% required)",
            f"TACoS {fmt_pct(period.tacos_bps)} — Ascend's in-house PPC targets <15%. "
            "High ad dependency reduces post-acquisition margin runway.")
    if period.sku_count is not None and period.sku_count < 5:
        add(SEV_HIGH, "Below minimum SKU count (5+ required)",
            f"Only {period.sku_count} active SKU(s) — Ascend requires 5+. "
            "Single-SKU concentration creates supply and demand fragility.")
    if period.has_trademark is False:
        add(SEV_HIGH, "Trademark not confirmed",
            "Trademark filed/registered status not confirmed — required for Brand Registry, "
            "brand protection, and sponsor brand ads.")
    if period.has_brand_registry is False:
        add(SEV_HIGH, "Amazon Brand Registry not enrolled",
            "Brand Registry enrollment required for A+ content, brand analytics, "
            "and counterfeit protection.")
    # ---- Standard financial flags ------------------------------------------
    if current.net_earnings_cents is not None and current.net_earnings_cents < 0:
        add(SEV_CRITICAL, "Net loss for the period", f"Net earnings {fmt_money(current.net_earnings_cents)}.")
    if period.total_equity_cents is not None and period.total_equity_cents < 0:
        add(SEV_CRITICAL, "Negative equity", f"Total equity {fmt_money(period.total_equity_cents)}.")
    if current.blended_mer is not None and current.blended_mer < bm.blended_mer_min * 0.5:
        add(SEV_CRITICAL, "Marketing efficiency far below viable", f"Blended MER {fmt_mult(current.blended_mer)} (healthy ≥{bm.blended_mer_min:.1f}x).")
    if growth_bps is not None and growth_bps <= -2500:
        add(SEV_CRITICAL, "Severe revenue decline", f"Net revenue down {abs(growth_bps)/100:.0f}% YoY.")

    if growth_bps is not None and -2500 < growth_bps < 0:
        add(SEV_HIGH, "Declining revenue", f"Net revenue down {abs(growth_bps)/100:.1f}% YoY.")
    if current.net_margin_bps is not None and current.net_margin_bps < bm.net_margin_bps[0] / 2 and current.net_margin_bps >= 0:
        add(SEV_HIGH, "Thin net margin", f"Net margin {fmt_pct(current.net_margin_bps)} (healthy {bm.net_margin_bps[0]/100:.0f}–{bm.net_margin_bps[1]/100:.0f}%).")
    if current.return_rate_bps is not None and current.return_rate_bps > bm.return_rate_max_bps:
        add(SEV_HIGH, "Elevated return rate", f"Returns {fmt_pct(current.return_rate_bps)} of gross sales (healthy <{bm.return_rate_max_bps/100:.0f}%).")
    if period.intercompany_cents and period.total_assets_cents and abs(period.intercompany_cents) / max(1, period.total_assets_cents) >= 0.2:
        add(SEV_HIGH, "Large intercompany balances", f"Intercompany {fmt_money(period.intercompany_cents)} is {abs(period.intercompany_cents)/period.total_assets_cents*100:.0f}% of assets — collectability unproven.")
    if current.marketing_pct_bps is not None and current.marketing_pct_bps > bm.marketing_pct_bps[1]:
        add(SEV_MEDIUM, "Marketing spend above healthy band", f"Marketing {fmt_pct(current.marketing_pct_bps)} of revenue (healthy {bm.marketing_pct_bps[0]/100:.0f}–{bm.marketing_pct_bps[1]/100:.0f}%).")
    if current.product_gm_bps is not None and current.product_gm_bps < bm.product_gm_bps[0]:
        add(SEV_MEDIUM, "Product gross margin below band", f"Product GM {fmt_pct(current.product_gm_bps)} (healthy {bm.product_gm_bps[0]/100:.0f}–{bm.product_gm_bps[1]/100:.0f}%).")
    if current.discount_rate_bps is not None and current.discount_rate_bps > bm.discount_rate_bps[1]:
        add(SEV_MEDIUM, "Heavy discounting", f"Discount rate {fmt_pct(current.discount_rate_bps)} (healthy {bm.discount_rate_bps[0]/100:.0f}–{bm.discount_rate_bps[1]/100:.0f}%).")
    if period.other_income_cents and current.net_earnings_cents and period.other_income_cents > 0:
        share = period.other_income_cents / max(1, current.net_earnings_cents)
        if share >= 0.2:
            add(SEV_MEDIUM, "Earnings lean on non-recurring income", f"Other income {fmt_money(period.other_income_cents)} is {share*100:.0f}% of net earnings; operating result ex-other is {fmt_money(current.operating_result_ex_other_cents)}.")
    if period.dividends_cents and current.net_earnings_cents and period.dividends_cents > current.net_earnings_cents > 0:
        add(SEV_MEDIUM, "Distributions exceed earnings", f"Dividends/draws {fmt_money(period.dividends_cents)} vs net earnings {fmt_money(current.net_earnings_cents)}.")
    if period.related_party_flag:
        add(SEV_MEDIUM, "Related-party items present", "Related-party / intercompany activity detected — request agreements and collectability evidence.")

    return sort_red_flags(flags)


def _band_label(low: int, high: int) -> str:
    return f"{low/100:.0f}–{high/100:.0f}%"


def build_benchmarks(current: Metrics, bm: Benchmarks, growth_bps: Optional[int]) -> list:
    rows: list[BenchmarkRow] = []

    def in_band(v, lo, hi):
        return None if v is None else (lo <= v <= hi)

    rows.append(BenchmarkRow("Product gross margin", _band_label(*bm.product_gm_bps), fmt_pct(current.product_gm_bps),
                             None if current.product_gm_bps is None else current.product_gm_bps >= bm.product_gm_bps[0]))
    rows.append(BenchmarkRow("Marketing % of revenue", _band_label(*bm.marketing_pct_bps), fmt_pct(current.marketing_pct_bps),
                             in_band(current.marketing_pct_bps, *bm.marketing_pct_bps)))
    rows.append(BenchmarkRow("Blended MER", f"≥ {bm.blended_mer_min:.1f}x", fmt_mult(current.blended_mer),
                             None if current.blended_mer is None else current.blended_mer >= bm.blended_mer_min))
    rows.append(BenchmarkRow("Contribution margin", _band_label(*bm.contribution_margin_bps), fmt_pct(current.contribution_margin_bps),
                             None if current.contribution_margin_bps is None else current.contribution_margin_bps >= bm.contribution_margin_bps[0]))
    rows.append(BenchmarkRow("Net margin", _band_label(*bm.net_margin_bps), fmt_pct(current.net_margin_bps),
                             None if current.net_margin_bps is None else current.net_margin_bps >= bm.net_margin_bps[0]))
    rows.append(BenchmarkRow("DTC / owned % of revenue (expansion opportunity)", "0% → Ascend builds", fmt_pct(current.owned_pct_bps),
                             None))
    rows.append(BenchmarkRow("Discount rate", _band_label(*bm.discount_rate_bps), fmt_pct(current.discount_rate_bps),
                             None if current.discount_rate_bps is None else current.discount_rate_bps <= bm.discount_rate_bps[1]))
    rows.append(BenchmarkRow("Return rate", f"< {bm.return_rate_max_bps/100:.0f}%", fmt_pct(current.return_rate_bps),
                             None if current.return_rate_bps is None else current.return_rate_bps < bm.return_rate_max_bps))
    rows.append(BenchmarkRow("YoY revenue growth", "≥ 0%", "—" if growth_bps is None else fmt_pct(growth_bps),
                             None if growth_bps is None else growth_bps >= bm.yoy_growth_min_bps))
    return rows


# ---------------------------------------------------------------------------
# Top-level entry
# ---------------------------------------------------------------------------


def score(current_period: PeriodFinancials, prior_period: Optional[PeriodFinancials], *,
          category: str = "dtc", social_signals: Optional[dict] = None) -> dict:
    """Run the full deterministic pass. Returns the pieces the report assembler
    needs (metrics, scorecard, red flags, benchmarks, yoy growth)."""
    bm = benchmarks_for(category)
    current = derive_metrics(current_period)
    prior = derive_metrics(prior_period) if prior_period is not None else None
    growth = yoy_growth_bps(current, prior)
    scorecard = build_scorecard(current, prior, current_period, growth, bm)
    red_flags = build_red_flags(current, current_period, growth, bm, social_signals=social_signals)
    benchmarks = build_benchmarks(current, bm, growth)
    return {
        "current": current,
        "prior": prior,
        "growth_bps": growth,
        "scorecard": scorecard,
        "red_flags": red_flags,
        "benchmarks": benchmarks,
    }
