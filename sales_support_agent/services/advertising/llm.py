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


def generate_narrative(
    summary: dict,
    recs: list[Recommendation],
    goals: Optional[Goals] = None,
    *,
    prior_summary: Optional[dict] = None,
    api_key: Optional[str] = None,
    model: str = "claude-haiku-4-5-20251001",
) -> NarrativeResult:
    key = api_key or os.environ.get("ANTHROPIC_API_KEY", "")
    if not key:
        return NarrativeResult(
            text=(
                "AI strategic read unavailable — set ANTHROPIC_API_KEY to enable. "
                "The ranked burn list below is fully computed and ready to act on."
            )
        )

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
        text = message.content[0].text if message.content else ""
        return NarrativeResult(
            text=text.strip(),
            model=message.model,
            input_tokens=message.usage.input_tokens,
            output_tokens=message.usage.output_tokens,
        )
    except Exception as exc:  # noqa: BLE001
        return NarrativeResult(
            text=f"AI strategic read temporarily unavailable ({exc}). The ranked burn list below is computed and ready.",
            model="error",
        )
