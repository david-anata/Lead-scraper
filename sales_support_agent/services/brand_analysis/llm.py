"""LLM-augmented narrative for the Brand Analysis report.

The deterministic engine (scoring.py) does all the math and grading; this layer
writes the plain-English prose over the *already-computed* metrics — Executive
Summary, the "what stands out" bullets, and the acquisition verdict. Mirrors
advertising/llm.py: same key/model convention and a graceful deterministic
fallback so the report is never left blank without an API key.

Numbers are never invented or recalculated here.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from typing import Optional

from sales_support_agent.services.brand_analysis.schema import (
    Metrics,
    Scorecard,
    fmt_money,
    fmt_mult,
    fmt_pct,
)

logger = logging.getLogger(__name__)

_RECOMMENDATION_BY_LETTER = {
    "A": "Strong Buy",
    "B": "Buy",
    "C": "Conditional Buy",
    "D": "Proceed with Caution",
    "F": "Pass",
}


@dataclass
class NarrativeResult:
    executive_summary: str = ""
    stands_out: list = field(default_factory=list)
    verdict_text: str = ""
    recommendation: str = ""
    investment_thesis: list = field(default_factory=list)  # bull case (for)
    key_risks: list = field(default_factory=list)          # bear case (against)
    model: str = "none"
    input_tokens: int = 0
    output_tokens: int = 0


_SYSTEM = (
    "You are a senior M&A analyst writing the narrative for an executive "
    "acquisition report on a consumer brand. You are given ALREADY-COMPUTED "
    "metrics, a letter grade, a weighted scorecard, and ranked red flags — do "
    "NOT recalculate or invent any numbers; cite only the figures provided. "
    "Write in crisp, direct prose for a buy-side decision-maker. Return ONLY a "
    "JSON object with keys: executive_summary (2-4 sentence paragraph), "
    "stands_out (array of 3-5 short bullet strings of things that stand out "
    "beyond standard KPIs), verdict (2-3 sentence buy/pass paragraph that "
    "restates the grade and gives a recommendation), investment_thesis (array "
    "of 3-5 short bullets — the bull case / reasons this is an attractive "
    "opportunity, grounded only in the supplied figures), key_risks (array of "
    "3-5 short bullets — the bear case / what must be diligenced before close). "
    "No markdown, no prose outside the JSON."
)


def _facts(brand: str, category: str, current: Metrics, growth_bps: Optional[int],
           scorecard: Scorecard, red_flags: list, confidence: str, has_yoy: bool) -> str:
    lines = [
        f"BRAND: {brand or 'Unknown'}  CATEGORY: {category}",
        f"OVERALL GRADE: {scorecard.letter} ({scorecard.score_100}/100)  CONFIDENCE: {confidence}",
        f"YoY revenue growth: {'—' if growth_bps is None else fmt_pct(growth_bps)} (prior year {'present' if has_yoy else 'ABSENT'})",
        "METRICS:",
        f"  Net revenue: {fmt_money(current.net_revenue_cents)}",
        f"  Product gross margin: {fmt_pct(current.product_gm_bps)}",
        f"  Marketing % of revenue: {fmt_pct(current.marketing_pct_bps)}  Blended MER: {fmt_mult(current.blended_mer)}",
        f"  Contribution margin: {fmt_pct(current.contribution_margin_bps)}  Net margin: {fmt_pct(current.net_margin_bps)}",
        f"  Discount rate: {fmt_pct(current.discount_rate_bps)}  Return rate: {fmt_pct(current.return_rate_bps)}",
        f"  Operating result ex-other-income: {fmt_money(current.operating_result_ex_other_cents)}",
        "WEIGHTED SCORECARD:",
    ]
    for d in scorecard.dimensions:
        lines.append(f"  [{d.letter}] {d.label} ({int(d.weight*100)}%): {d.reason}")
    if red_flags:
        lines.append("RED FLAGS (ranked):")
        for f in red_flags[:8]:
            lines.append(f"  [{f.severity}] {f.title} — {f.detail}")
    else:
        lines.append("RED FLAGS: none material.")
    lines.append("\nWrite the JSON narrative now.")
    return "\n".join(lines)


def build_deterministic(brand: str, current: Metrics, growth_bps: Optional[int],
                        scorecard: Scorecard, red_flags: list, has_yoy: bool) -> NarrativeResult:
    """Always-available narrative computed from the numbers — the baseline the
    LLM enriches when a key is present, so the report is never blank."""
    rec = _RECOMMENDATION_BY_LETTER.get(scorecard.letter, "Conditional Buy")
    brand_name = brand or "The brand"

    growth_clause = (
        f"revenue { 'grew' if (growth_bps or 0) >= 0 else 'declined' } {abs((growth_bps or 0))/100:.1f}% YoY"
        if growth_bps is not None else "no prior-year data was supplied to assess revenue trajectory"
    )
    summary = (
        f"{brand_name} earns an overall {scorecard.letter} ({scorecard.score_100}/100) on net revenue of "
        f"{fmt_money(current.net_revenue_cents)}, where {growth_clause}. Product gross margin is "
        f"{fmt_pct(current.product_gm_bps)}, blended MER {fmt_mult(current.blended_mer)}, and net margin "
        f"{fmt_pct(current.net_margin_bps)}."
    )

    stands_out = []
    if current.operating_result_ex_other_cents is not None and current.net_earnings_cents is not None and \
            current.operating_result_ex_other_cents != current.net_earnings_cents:
        stands_out.append(
            f"Operating result excluding other income is {fmt_money(current.operating_result_ex_other_cents)} "
            f"vs reported net earnings {fmt_money(current.net_earnings_cents)} — check recurrence."
        )
    if current.blended_mer is not None:
        stands_out.append(f"Every $1 of marketing returns {fmt_mult(current.blended_mer)} in revenue (blended).")
    if current.contribution_margin_bps is not None:
        stands_out.append(f"Reported contribution margin sits at {fmt_pct(current.contribution_margin_bps)}.")
    crit = [f for f in red_flags if f.severity == "Critical"]
    if crit:
        stands_out.append("Critical flag(s): " + "; ".join(f.title for f in crit) + ".")
    if not stands_out:
        stands_out.append("No standout items beyond the headline KPIs in the supplied data.")

    verdict = (
        f"Recommendation: {rec}. The weighted composite grades the business {scorecard.letter} "
        f"({scorecard.score_100}/100). "
        + ("Material red flags should be diligenced before close. " if red_flags else "No material red flags surfaced. ")
        + ("Single-period data limits confidence — request prior-year and the latest quarter." if not has_yoy else "")
    ).strip()

    # Bull case: the strongest scorecard dimensions (A/B) become thesis points.
    thesis = []
    for d in sorted(scorecard.dimensions, key=lambda x: x.points, reverse=True):
        if d.letter in ("A", "B") and len(thesis) < 4:
            thesis.append(f"{d.label}: {d.reason}")
    if current.blended_mer is not None and current.blended_mer >= 3.0:
        thesis.append(f"Efficient acquisition — every $1 of marketing returns {fmt_mult(current.blended_mer)}.")
    if not thesis:
        thesis.append("No standout strengths in the supplied data — thesis depends on diligence upside.")

    # Bear case: critical/high red flags + the weakest dimensions.
    risks = [f"{f.title}: {f.detail}" if f.detail else f.title
             for f in red_flags if f.severity in ("Critical", "High")][:4]
    for d in sorted(scorecard.dimensions, key=lambda x: x.points):
        if d.letter in ("D", "F") and len(risks) < 5:
            risks.append(f"{d.label}: {d.reason}")
    if not has_yoy:
        risks.append("Single-period data — no prior-year trend to confirm trajectory.")
    if not risks:
        risks.append("No material risks surfaced in the supplied data.")

    return NarrativeResult(
        executive_summary=summary,
        stands_out=stands_out,
        verdict_text=verdict,
        recommendation=rec,
        investment_thesis=thesis,
        key_risks=risks,
        model="deterministic",
    )


def generate_narrative(
    brand: str,
    category: str,
    current: Metrics,
    growth_bps: Optional[int],
    scorecard: Scorecard,
    red_flags: list,
    confidence: str,
    has_yoy: bool,
    *,
    context_notes: str = "",
    api_key: Optional[str] = None,
    model: str = "claude-haiku-4-5-20251001",
) -> NarrativeResult:
    baseline = build_deterministic(brand, current, growth_bps, scorecard, red_flags, has_yoy)
    key = api_key or os.environ.get("ANTHROPIC_API_KEY", "")
    if not key:
        return baseline

    prompt = _facts(brand, category, current, growth_bps, scorecard, red_flags, confidence, has_yoy)
    if context_notes and context_notes.strip():
        prompt += (
            "\n\nANALYST CONTEXT (incorporate where relevant; do not invent numbers):\n"
            + context_notes.strip()
        )
    try:
        import anthropic

        client = anthropic.Anthropic(api_key=key)
        message = client.messages.create(
            model=model,
            max_tokens=700,
            system=_SYSTEM,
            messages=[{"role": "user", "content": prompt}],
        )
        text = (message.content[0].text if message.content else "").strip()
        data = _parse_json(text)
        if not data:
            return NarrativeResult(**{**baseline.__dict__, "model": "deterministic-fallback"})
        return NarrativeResult(
            executive_summary=str(data.get("executive_summary") or baseline.executive_summary).strip(),
            stands_out=[str(s).strip() for s in (data.get("stands_out") or baseline.stands_out) if str(s).strip()],
            verdict_text=str(data.get("verdict") or baseline.verdict_text).strip(),
            recommendation=baseline.recommendation,  # recommendation stays deterministic (tied to grade)
            investment_thesis=[str(s).strip() for s in (data.get("investment_thesis") or baseline.investment_thesis) if str(s).strip()],
            key_risks=[str(s).strip() for s in (data.get("key_risks") or baseline.key_risks) if str(s).strip()],
            model=message.model,
            input_tokens=message.usage.input_tokens,
            output_tokens=message.usage.output_tokens,
        )
    except Exception:  # noqa: BLE001 — never leave the report blank
        logger.warning("[brand_analysis] narrative LLM call failed; using deterministic read", exc_info=True)
        return NarrativeResult(**{**baseline.__dict__, "model": "deterministic-fallback"})


def _parse_json(text: str) -> Optional[dict]:
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # Tolerate code fences / leading prose: grab the outermost {...}.
        start, end = text.find("{"), text.rfind("}")
        if start >= 0 and end > start:
            try:
                return json.loads(text[start:end + 1])
            except json.JSONDecodeError:
                return None
    return None
