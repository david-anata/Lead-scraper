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
from sales_support_agent.services.brand_analysis.schema import (
    CATEGORY_DTC,
    BrandReport,
)


def build_report(
    files: list[tuple[str, bytes]],
    *,
    brand: str = "",
    category: str = CATEGORY_DTC,
    prepared_date: str = "",
    use_llm: bool = True,
) -> BrandReport:
    category = (category or CATEGORY_DTC).lower()
    intake = intake_mod.parse_dump(files, category=category)

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
    ) if use_llm else llm_mod.build_deterministic(
        brand or (intake.detected_brands[0] if intake.detected_brands else ""),
        current, growth, scorecard, red_flags, intake.has_yoy,
    )

    scorecard.verdict = narrative.verdict_text

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
    )
    return report


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
