"""Strategic narrative for the advertising audit via the Claude API.

The deterministic engine does the math and produces the ranked burn list; this
layer writes a short plain-English strategic read over the *already-computed*
metrics and top recommendations. Mirrors cashflow/ai_summary.py: same key/model
convention and graceful fallback so the page never hard-errors without a key.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

from sales_support_agent.services.advertising.schema import (
    Goals,
    Recommendation,
    fmt_money,
    fmt_pct,
)


@dataclass
class NarrativeResult:
    text: str
    model: str = "none"
    input_tokens: int = 0
    output_tokens: int = 0


_SYSTEM = (
    "You are a senior Amazon advertising strategist advising a brand owner on their "
    "weekly account audit. You are given already-computed metrics and a ranked list of "
    "recommended actions — do not recalculate or invent numbers. Write a concise, direct "
    "strategic read: where the account stands versus goal, the 2-3 highest-leverage moves "
    "this week, and any risk in scaling. Plain English, no markdown headers, no bullet "
    "lists, under 180 words. Refer to total/blended marketing cost (including off-Amazon "
    "spend) when discussing efficiency, not Amazon ACoS alone."
)


def _build_prompt(summary: dict, recs: list[Recommendation], goals: Goals, prior_summary: Optional[dict]) -> str:
    g = goals or Goals()
    lines = [
        "ACCOUNT METRICS (this audit):",
        f"  Total sales: {fmt_money(summary.get('total_sales_cents'))}",
        f"  Amazon ad spend: {fmt_money(summary.get('ad_spend_cents'))}  ad sales: {fmt_money(summary.get('ad_sales_cents'))}",
        f"  External spend (Meta/TikTok/influencer): {fmt_money(summary.get('external_spend_cents'))}",
        f"  ACoS: {fmt_pct(summary.get('acos_bps'))}  TACoS: {fmt_pct(summary.get('tacos_bps'))}  "
        f"Blended TACoS: {fmt_pct(summary.get('blended_tacos_bps'))}",
        f"  Units: {summary.get('total_units')}  Sessions: {summary.get('total_sessions')}",
    ]
    targets = []
    if g.revenue_target_cents:
        targets.append(f"revenue {fmt_money(g.revenue_target_cents)}")
    if g.acos_target_bps:
        targets.append(f"ACoS {fmt_pct(g.acos_target_bps)}")
    if g.tacos_target_bps:
        targets.append(f"TACoS {fmt_pct(g.tacos_target_bps)}")
    if g.units_target:
        targets.append(f"units {g.units_target}")
    lines.append("GOALS (" + (g.period or "monthly") + "): " + (", ".join(targets) or "none set"))

    if prior_summary:
        lines.append(
            "PRIOR AUDIT: sales " + fmt_money(prior_summary.get("total_sales_cents"))
            + ", blended TACoS " + fmt_pct(prior_summary.get("blended_tacos_bps"))
        )

    lines.append("\nTOP RECOMMENDED ACTIONS (already ranked):")
    for i, r in enumerate(recs[:8], start=1):
        lines.append(f"  {i}. [{r.severity}] {r.title}")

    lines.append(
        "\nWrite the strategic read now: standing vs goal, the highest-leverage moves this week, "
        "and any scaling risk."
    )
    return "\n".join(lines)


def build_deterministic_read(summary: dict, recs: list[Recommendation], goals: Optional[Goals]) -> str:
    """A real strategic read computed straight from the numbers — no API needed.
    Used as the always-available baseline; the LLM only enriches it when a key
    is configured. The Exec Brief is therefore never left blank."""
    from collections import Counter

    g = goals or Goals()
    summary = summary or {}
    gap = summary.get("gap", {}) or {}
    parts: list[str] = []

    # Scope + data-sanity first, so the reader trusts the numbers.
    if summary.get("brand"):
        scope = f"Scoped to {summary.get('brand_asin_count', 0)} {summary['brand']} ASIN(s)"
        if summary.get("excluded_mixed_campaigns"):
            scope += f"; {summary['excluded_mixed_campaigns']} cross-brand campaign(s) excluded from edits (their spend still counts)"
        parts.append(scope + ".")
    windows = summary.get("data_windows") or []
    if len(windows) > 1:
        parts.append("⚠ Data note: your reports cover different date windows (" + "; ".join(windows) +
                     ") — use one trailing window, ending yesterday, for comparable metrics.")

    rev = summary.get("total_sales_cents")
    if gap.get("revenue_gap_cents", 0) and gap["revenue_gap_cents"] > 0:
        parts.append(
            f"Revenue is {fmt_money(rev)}, {fmt_money(gap['revenue_gap_cents'])} short of the "
            f"{fmt_money(gap.get('revenue_target_cents'))} goal ({fmt_pct(gap.get('revenue_attainment_bps'))} attained)."
        )
    elif rev is not None:
        parts.append(f"Revenue is {fmt_money(rev)}.")

    bt, tt = summary.get("blended_tacos_bps"), g.tacos_target_bps
    if bt is not None and tt:
        if bt < tt:
            parts.append(f"Blended TACoS is {fmt_pct(bt)} versus a {fmt_pct(tt)} target — there's headroom to invest in growth.")
        else:
            parts.append(f"Blended TACoS is {fmt_pct(bt)}, above the {fmt_pct(tt)} target — tighten efficiency before scaling.")
    elif bt is not None:
        parts.append(f"Blended TACoS is {fmt_pct(bt)}.")

    acos, at = summary.get("acos_bps"), g.acos_target_bps
    if acos is not None and at and acos > at:
        parts.append(f"Ad ACoS is {fmt_pct(acos)} against the {fmt_pct(at)} target.")

    cats = Counter(r.category for r in recs)
    moves = []
    if cats.get("negative_keyword"):
        moves.append(f"negate {cats['negative_keyword']} wasted-spend terms")
    if cats.get("new_keyword"):
        moves.append(f"harvest {cats['new_keyword']} converting search terms into exact keywords")
    if cats.get("bid_down"):
        moves.append(f"trim bids on {cats['bid_down']} over-target keywords")
    if cats.get("bid_up"):
        moves.append(f"scale bids on {cats['bid_up']} efficient winners")
    if moves:
        parts.append("Highest-leverage moves this week: " + ", ".join(moves) +
                     " — all in the Burn List and pre-loaded in the bulk apply-sheet.")
    return " ".join(parts) or "Upload your ad + sales reports to generate a fuller read."


def generate_narrative(
    summary: dict,
    recs: list[Recommendation],
    goals: Optional[Goals] = None,
    *,
    prior_summary: Optional[dict] = None,
    api_key: Optional[str] = None,
    model: str = "claude-haiku-4-5-20251001",
) -> NarrativeResult:
    baseline = build_deterministic_read(summary, recs, goals)
    key = api_key or os.environ.get("ANTHROPIC_API_KEY", "")
    if not key:
        return NarrativeResult(text=baseline, model="deterministic")

    prompt = _build_prompt(summary, recs, goals or Goals(), prior_summary)
    try:
        import anthropic

        client = anthropic.Anthropic(api_key=key)
        message = client.messages.create(
            model=model,
            max_tokens=320,
            system=_SYSTEM,
            messages=[{"role": "user", "content": prompt}],
        )
        text = (message.content[0].text if message.content else "").strip()
        return NarrativeResult(
            text=text or baseline,
            model=message.model,
            input_tokens=message.usage.input_tokens,
            output_tokens=message.usage.output_tokens,
        )
    except Exception:  # noqa: BLE001 - fall back to the computed read, never a placeholder
        return NarrativeResult(text=baseline, model="deterministic-fallback")
