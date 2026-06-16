"""Orchestrator: file dump -> BrandReport.

Threads the package together — intake (parse) -> scoring (deterministic
metrics + grade) -> confidence (missing-data) -> llm (narrative) — and packs
the result into the single ``BrandReport`` the page renderer, .docx exporter,
and history all consume. Deterministic except for the optional LLM narrative.
"""

from __future__ import annotations

from typing import Optional

from sales_support_agent.services.brand_analysis import confidence as confidence_mod
from sales_support_agent.services.brand_analysis import intake as intake_mod
from sales_support_agent.services.brand_analysis import llm as llm_mod
from sales_support_agent.services.brand_analysis import scoring as scoring_mod
from sales_support_agent.services.brand_analysis import valuation as valuation_mod
from sales_support_agent.services.brand_analysis.schema import (
    CATEGORY_DTC,
    BrandReport,
    fmt_money,
    fmt_mult,
    fmt_pct,
)


def build_report(
    files: list[tuple[str, bytes]],
    *,
    brand: str = "",
    category: str = CATEGORY_DTC,
    prepared_date: str = "",
    use_llm: bool = True,
    context_notes: str = "",
    brand_website: str = "",
    logo_data_uri: str = "",
    brand_tagline: str = "",
    product_images: Optional[list] = None,
) -> BrandReport:
    category = (category or CATEGORY_DTC).lower()
    intake = intake_mod.parse_dump(files, category=category, use_llm=use_llm,
                                   context_notes=context_notes)

    scored = scoring_mod.score(intake.current, intake.prior, category=category)
    current = scored["current"]
    prior = scored["prior"]
    growth = scored["growth_bps"]
    scorecard = scored["scorecard"]
    red_flags = scored["red_flags"]
    benchmarks = scored["benchmarks"]

    conf = confidence_mod.evaluate(intake.current, intake.has_yoy)

    narrative = llm_mod.generate_narrative(
        brand or (intake.detected_brands[0] if intake.detected_brands else ""),
        category,
        current,
        growth,
        scorecard,
        red_flags,
        conf["confidence"],
        intake.has_yoy,
        context_notes=context_notes,
    ) if use_llm else llm_mod.build_deterministic(
        brand or (intake.detected_brands[0] if intake.detected_brands else ""),
        current, growth, scorecard, red_flags, intake.has_yoy,
    )

    scorecard.verdict = narrative.verdict_text

    completeness = conf.get("completeness_pct", 0)
    valuation = valuation_mod.estimate(
        current, category=category, grade=scorecard.letter,
        data_completeness_pct=completeness,
    )
    info_ribbon = _build_ribbon(scorecard, current, growth, narrative.recommendation,
                                valuation, intake.has_yoy)

    report = BrandReport(
        brand=brand or (intake.detected_brands[0] if intake.detected_brands else "Brand"),
        detected_brands=intake.detected_brands,
        category=category,
        prepared_date=prepared_date,
        period_current_label=intake.current.period_label,
        period_prior_label=(intake.prior.period_label if intake.prior else ""),
        has_yoy=intake.has_yoy,
        current=current,
        prior=prior or scoring_mod.Metrics(),
        yoy_revenue_growth_bps=growth,
        monthly_revenue=intake.current.monthly_revenue,
        media_mix=dict(intake.current.marketing_by_channel),
        media_mix_prior=dict(intake.prior.marketing_by_channel) if intake.prior else {},
        balance_sheet=_balance_lines(intake.current),
        acquisition_current=_acquisition_data(intake.current),
        acquisition_prior=_acquisition_data(intake.prior) if intake.prior else {},
        related_party_flag=intake.current.related_party_flag,
        scorecard=scorecard,
        red_flags=red_flags,
        benchmarks=benchmarks,
        missing_data=conf["missing_short"],
        confidence=conf["confidence"],
        data_sufficient=conf["data_sufficient"],
        data_gaps=conf["data_gaps"],
        executive_summary=narrative.executive_summary,
        stands_out=narrative.stands_out,
        verdict_text=narrative.verdict_text,
        recommendation=narrative.recommendation,
        narrative_model=narrative.model,
        intake_summary=intake.summary(),
        data_completeness_pct=completeness,
        account_mappings=intake.account_mappings,
        unmapped_accounts=intake.unmapped_accounts,
        classifier_model=intake.classifier_model,
        valuation=valuation.to_dict(),
        investment_thesis=narrative.investment_thesis,
        key_risks=narrative.key_risks,
        info_ribbon=info_ribbon,
        brand_website=brand_website,
        logo_data_uri=logo_data_uri,
        product_images=list(product_images or []),
        brand_tagline=brand_tagline,
        context_notes=context_notes,
    )
    return report


_GRADE_TONE = {"A": "good", "B": "good", "C": "warn", "D": "warn", "F": "bad"}


def _build_ribbon(scorecard, current, growth_bps, recommendation, valuation, has_yoy) -> list:
    """Quick-glance KPI chips for the exec-summary callout ribbon.
    Each chip: {label, value, tone}. tone in good|warn|bad|neutral."""
    chips: list = []
    chips.append({"label": "Grade", "value": f"{scorecard.letter} · {scorecard.score_100}/100",
                  "tone": _GRADE_TONE.get(scorecard.letter, "neutral")})
    chips.append({"label": "Recommendation", "value": recommendation or "—",
                  "tone": _GRADE_TONE.get(scorecard.letter, "neutral")})
    if current.net_revenue_cents is not None:
        chips.append({"label": "Net revenue", "value": fmt_money(current.net_revenue_cents), "tone": "neutral"})
    if growth_bps is not None:
        chips.append({"label": "YoY growth", "value": fmt_pct(growth_bps),
                      "tone": "good" if growth_bps >= 0 else "bad"})
    elif not has_yoy:
        chips.append({"label": "YoY growth", "value": "No prior year", "tone": "warn"})
    if current.net_margin_bps is not None:
        chips.append({"label": "Net margin", "value": fmt_pct(current.net_margin_bps),
                      "tone": "good" if current.net_margin_bps >= 0 else "bad"})
    if current.blended_mer is not None:
        chips.append({"label": "Blended MER", "value": fmt_mult(current.blended_mer),
                      "tone": "good" if current.blended_mer >= 3.0 else "warn"})
    if valuation.is_meaningful():
        chips.append({"label": "Indicative range", "value": valuation.headline(), "tone": "neutral"})
    return chips


def _acquisition_data(period) -> dict:
    """Raw PeriodFinancials acquisition fields not in Metrics — passed through to the report."""
    keys = ("new_customer_revenue_cents", "returning_customer_revenue_cents",
            "aov_cents", "cac_cents", "ltv_cents")
    return {k: getattr(period, k) for k in keys if getattr(period, k) is not None}


def _balance_lines(period) -> list:
    """The balance-sheet lines present in the parsed current period, for the
    Balance Sheet & Earnings Quality section."""
    labelled = [
        ("Total assets", period.total_assets_cents),
        ("Cash & equivalents", period.cash_cents),
        ("Inventory", period.inventory_cents),
        ("Intercompany balances", period.intercompany_cents),
        ("Total equity", period.total_equity_cents),
        ("Dividends / distributions", period.dividends_cents),
    ]
    return [[label, cents] for label, cents in labelled if cents is not None]


def report_to_history_meta(report: BrandReport) -> dict:
    """The slim fields History lists by (brand, date, grade, confidence)."""
    return {
        "brand": report.brand,
        "grade": report.scorecard.letter,
        "score_100": report.scorecard.score_100,
        "confidence": report.confidence,
        "category": report.category,
        "recommendation": report.recommendation,
        "has_yoy": report.has_yoy,
    }
