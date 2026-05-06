"""Story markdown generator — text-based deck companion.

Produces a markdown file that mirrors the visual deck but is optimized for
reading aloud during a sales call. The markdown gets persisted into
AutomationRun.summary_json["story_markdown"] at deck-creation time and is
exposed via two routes:

  GET /decks/{slug}/{run_id}/{token}/story     → HTML viewer
  GET /decks/{slug}/{run_id}/{token}/story.md  → raw download

Sections:
  1. Cover / Executive Summary
  2. Market & Competitive Analysis
  3. Search Behavior & Keyword Opportunities
  4. Conversion / PDP Recommendations
  5. Growth Plan Synopsis (math + per-channel campaign / outcome / why)
  6. Implementation Roadmap (4-phase, citation-backed)
  7. Proposed Offers & Next Step
"""

from __future__ import annotations

from typing import Any

from sales_support_agent.services.deck.growth_plan import (
    PHASES,
    PHASE_CITATIONS,
    GrowthPlan,
    _cumulative_active_keys,
)


def _money(value: float) -> str:
    if value is None:
        return "—"
    if abs(value) >= 1000:
        return f"${value:,.0f}"
    return f"${value:,.2f}"


def _int(value: Any) -> str:
    try:
        return f"{int(round(float(value))):,}"
    except (TypeError, ValueError):
        return str(value or "—")


def _section_cover(payload: dict[str, Any], target_brand: str) -> str:
    target = payload.get("target", {}) or {}
    text_fields = payload.get("text_fields", {}) or {}
    title = target.get("title") or "the prospect listing"
    asin = target.get("asin") or ""
    niche = payload.get("niche_keyword") or ""

    exec_summary = text_fields.get("executive_summary") or ""
    market_summary = text_fields.get("market_summary") or ""

    lines = [
        f"# {target_brand} × Anata Strategy",
        "",
        f"**Target listing:** {title}" + (f" (ASIN `{asin}`)" if asin else ""),
        f"**Niche keyword:** _{niche}_" if niche else "",
        "",
        "## Executive summary",
        "",
        exec_summary or market_summary or "",
        "",
    ]
    return "\n".join(line for line in lines if line is not None)


def _section_market(payload: dict[str, Any]) -> str:
    text_fields = payload.get("text_fields", {}) or {}
    xray = payload.get("xray_report")
    products = list(getattr(xray, "products", []) or [])[:5]

    parts = [
        "## Market & competitive landscape",
        "",
        text_fields.get("market_summary") or "",
        "",
    ]

    if products:
        total_rev = getattr(xray, "total_revenue", 0.0) or 0.0
        parts.append("**Top 5 listings on the visible market set:**")
        parts.append("")
        for product in products:
            share = (
                (product.revenue or 0.0) / total_rev * 100.0
                if total_rev > 0 else 0.0
            )
            parts.append(
                f"- **{product.brand or 'Unbranded'}** — _{product.title[:60]}_  \n"
                f"  ASIN `{product.asin}` · {product.price_label} · "
                f"{product.revenue_label} ({share:.1f}% share) · "
                f"BSR {product.bsr_label}"
            )
        parts.append("")

    parts.append(text_fields.get("advertising_summary") or "")
    parts.append("")
    return "\n".join(parts)


def _section_search(payload: dict[str, Any]) -> str:
    text_fields = payload.get("text_fields", {}) or {}
    keyword_report = payload.get("keyword_report")
    cerebro_report = payload.get("cerebro_report")
    seo_recs = list(payload.get("seo_recommendations", []) or [])
    search_insights = payload.get("search_insights", {}) or {}

    parts = [
        "## Search behavior & keyword opportunities",
        "",
        text_fields.get("seo_summary") or "",
        "",
    ]

    title_misses = search_insights.get("title_misses") or []
    copy_misses = search_insights.get("copy_misses") or []
    if title_misses:
        parts.append("**Missing title keywords (highest-value gaps):**")
        parts.append("")
        for term in title_misses[:5]:
            parts.append(f"- {term}")
        parts.append("")
    if copy_misses:
        parts.append("**Missing bullet/copy keywords:**")
        parts.append("")
        for term in copy_misses[:5]:
            parts.append(f"- {term}")
        parts.append("")

    if seo_recs:
        parts.append("**Recommended SEO actions:**")
        parts.append("")
        for rec in seo_recs[:5]:
            parts.append(f"- {rec}")
        parts.append("")

    if keyword_report and getattr(keyword_report, "keywords", None):
        parts.append("**Top keyword opportunities by search volume:**")
        parts.append("")
        for kw in keyword_report.keywords[:5]:
            sv = _int(kw.search_volume)
            parts.append(f"- **{kw.phrase}** — {sv} monthly searches")
        parts.append("")

    return "\n".join(parts)


def _section_conversion(payload: dict[str, Any]) -> str:
    cro = list(payload.get("cro_recommendations", []) or [])
    creative = list(payload.get("creative_recommendations", []) or [])

    parts = [
        "## Conversion & PDP — where the listing needs to improve",
        "",
    ]
    if cro:
        parts.append("**CRO recommendations:**")
        parts.append("")
        for rec in cro[:5]:
            parts.append(f"- {rec}")
        parts.append("")
    if creative:
        parts.append("**Creative recommendations:**")
        parts.append("")
        for rec in creative[:5]:
            parts.append(f"- {rec}")
        parts.append("")
    return "\n".join(parts)


def _section_growth_plan(plan: GrowthPlan, target_aov: float) -> str:
    cvr = max(plan.cvr_pct, 0.01)
    expected_units = int(round(plan.total_sessions_delivered * cvr / 100.0))
    expected_revenue = expected_units * max(target_aov, 0.0)

    parts = [
        "## Growth plan synopsis — closing the gap",
        "",
        f"- **Current sessions:** {plan.current_sessions:,}  "
        f"(_= {plan.target_units:,} units ÷ {plan.cvr_pct:.1f}% CVR_)",
        f"- **Goal sessions:** {plan.goal_sessions:,}",
        f"- **Sessions delta:** {plan.delta_sessions:,}",
        "",
        "### Growth path — how sessions ramp from today to goal",
        "",
        "Each phase brings a new channel online. Sessions accumulate as "
        "the funnel widens; we don't reach the full goal on day one.",
        "",
    ]

    # Per-phase ramp table (matches the new visual on the deck slide).
    label_map = {
        "organic": "Organic",
        "on_channel_paid": "On-channel paid",
        "off_channel_paid": "Off-channel paid",
        "affiliate": "Affiliate",
        "retargeting": "Retargeting",
    }
    current = max(0, plan.current_sessions)
    goal = max(plan.goal_sessions, current + 1)
    parts.append(f"- **Today (starting point):** {current:,} sessions "
                 f"({(current / goal * 100):.0f}% of goal)")
    for phase in PHASES:
        active_keys = _cumulative_active_keys(phase.id)
        cumulative_added = sum(c.sessions for c in plan.channels if c.key in active_keys)
        cumulative_total = current + cumulative_added
        pct_of_goal = min(100.0, (cumulative_total / goal) * 100.0)
        added_labels = ", ".join(label_map.get(k, k) for k in phase.channels_added) or "—"
        parts.append(
            f"- **Phase {phase.id} — {phase.label} ({phase.window_label}):** "
            f"{cumulative_total:,} sessions ({pct_of_goal:.0f}% of goal) · "
            f"+ {added_labels}"
        )
    parts.append("")

    parts.extend([
        "### Funnel math",
        "",
        f"5 traffic sources converge on PDP visits → CVR conversion → AOV multiplier → revenue.",
        "",
        f"- **PDP visits per month (steady state):** {plan.total_sessions_delivered:,}",
        f"- **Expected units (at {plan.cvr_pct:.1f}% CVR):** {expected_units:,}",
        f"- **Projected monthly revenue (at {_money(target_aov)} AOV):** "
        f"**{_money(expected_revenue)}**",
        f"- **Total monthly spend:** {_money(plan.total_monthly_spend)} "
        f"({_money(plan.total_monthly_spend / 30.0)}/day)",
        "",
        "### Channel mix — what runs and why",
        "",
    ])

    for channel in plan.channels:
        cost_line = (
            "SEO investment, no paid spend"
            if channel.key == "organic"
            else f"{_money(channel.monthly_cost)} / month"
        )
        outcome_line = (
            f"{channel.sessions:,} sessions → "
            f"{channel.expected_units:,} units → "
            f"{_money(channel.expected_revenue)} / mo"
        )
        parts.append(f"#### {channel.label}")
        parts.append("")
        parts.append(f"_{channel.mix_pct:.0f}% of mix · {cost_line} · Phase {channel.first_active_phase}_")
        parts.append("")
        parts.append(f"**Expected:** {outcome_line}")
        parts.append("")
        if channel.campaign_description:
            parts.append(f"**Campaign:** {channel.campaign_description}")
            parts.append("")
        if channel.strategic_why:
            parts.append(f"**Why this channel:** {channel.strategic_why}")
            parts.append("")
        if channel.is_directional:
            parts.append(
                "> _Directional — calibrate with first-party data after 30 days._"
            )
            parts.append("")

    if plan.shortfall_sessions > 0:
        parts.append(
            f"⚠️ Channel mix delivers {plan.total_sessions_delivered:,} sessions; "
            f"goal needs {plan.delta_sessions:,}. "
            f"Shortfall: {plan.shortfall_sessions:,}. Increase mix or budget."
        )
        parts.append("")

    return "\n".join(parts)


def _section_roadmap(plan: GrowthPlan) -> str:
    parts = [
        "## Implementation roadmap",
        "",
        "_4-phase rollout from launch to LTV. Each milestone cited below; "
        "see Sources at the bottom of this story for the full list._",
        "",
    ]

    # Channel-key → label for readability in "New this phase"
    label_map = {ch.key: ch.label for ch in plan.channels}

    for phase in PHASES:
        added = ", ".join(label_map.get(k, k) for k in phase.channels_added)
        parts.append(f"### Phase {phase.id} — {phase.label}  _({phase.window_label})_")
        parts.append("")
        parts.append(f"**Summary:** {phase.summary}")
        parts.append("")
        parts.append(f"**New this phase:** {added}")
        parts.append("")
        if phase.milestones:
            parts.append("**Milestones:**")
            parts.append("")
            for m in phase.milestones:
                parts.append(f"- {m}")
            parts.append("")

    parts.append("### Sources")
    parts.append("")
    parts.append(
        "_Timeline claims above cite the following published references:_"
    )
    parts.append("")
    for label, url in PHASE_CITATIONS:
        parts.append(f"- [{label}]({url})")
    parts.append("")
    return "\n".join(parts)


def _section_offers(payload: dict[str, Any], plan_summary: str = "") -> str:
    text_fields = payload.get("text_fields", {}) or {}
    offer_cards = list(payload.get("offer_cards", []) or [])
    parts = [
        "## Proposed offers & next step",
        "",
        text_fields.get("recommended_plan_summary")
            or plan_summary
            or "Choose the operating model, then move directly into the first growth sprint with clear ownership.",
        "",
    ]
    if offer_cards:
        for card in offer_cards:
            title = card.get("title", "Offer")
            desc = card.get("description", "")
            price = card.get("price", "")
            price_label = card.get("price_label", "")
            commission = card.get("commission", "")
            commission_label = card.get("commission_label", "")
            baseline = card.get("baseline", "")
            baseline_label = card.get("baseline_label", "")
            bonus = card.get("bonus", "")
            parts.append(f"### {title}")
            parts.append("")
            if desc:
                parts.append(desc)
                parts.append("")
            stats = []
            if price:
                stats.append(f"**{price_label or 'Fee'}:** {price}")
            if commission:
                stats.append(f"**{commission_label or 'Commission'}:** {commission}")
            if baseline:
                stats.append(f"**{baseline_label or 'Baseline'}:** {baseline}")
            if stats:
                parts.append(" · ".join(stats))
                parts.append("")
            if bonus:
                parts.append(f"_+ {bonus}_")
                parts.append("")

    parts.append("---")
    parts.append("")
    parts.append("**Next action:** schedule a meeting to align on the operating "
                 "model, the first growth sprint, and the next execution window.")
    parts.append("")
    parts.append(
        f"**Why now:** {text_fields.get('expected_impact_summary', '')}".strip()
    )
    parts.append("")
    parts.append(
        f"**Recommended next step:** {text_fields.get('why_anata_summary', '')}".strip()
    )
    parts.append("")
    return "\n".join(parts)


def build_story_markdown(
    *,
    payload: dict[str, Any],
    plan: GrowthPlan | None,
    target_brand: str,
    target_aov: float,
) -> str:
    """Compose the full story markdown from a deck payload.

    `payload` is the same `deck_payload` dict the renderer consumes.
    `plan` is the GrowthPlan or None when the user disabled the section.
    """
    sections = [
        _section_cover(payload, target_brand),
        _section_market(payload),
        _section_search(payload),
        _section_conversion(payload),
    ]
    if plan is not None:
        sections.append(_section_growth_plan(plan, target_aov))
        sections.append(_section_roadmap(plan))
    sections.append(_section_offers(payload))
    return "\n".join(s for s in sections if s)
