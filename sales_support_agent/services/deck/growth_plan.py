"""Growth Plan Synopsis — sessions-gap math + 5-channel breakdown.

Plan B from /Users/davidnarayan/.claude/plans/let-s-get-you-caught-goofy-lampson.md.
Reverse-engineers current sessions from `units / CVR`, derives the gap to a
user-supplied goal, and routes the delta across five channels (organic,
on-channel paid, off-channel paid, affiliate, retargeting/LTV).

Defaults are calibrated against cited 2025–2026 industry benchmarks; each card
in the rendered slide carries a small Source line so a prospect sees the
citation.

Anata-specific: the off-channel paid CPC defaults to $0.15 because Anata runs
storefront-link traffic optimized for Amazon's external-traffic signal (which
lifts organic rank on adjacent keywords), not direct-response performance.
The methodology footnote on the slide makes this explicit.
"""

from __future__ import annotations

import html
import math
from dataclasses import dataclass, field
from typing import Any


# ---------------------------------------------------------------------------
# Defaults (cited in the slide methodology footnote)
# ---------------------------------------------------------------------------

DEFAULT_CONVERSION_RATE_PCT: float = 15.0
DEFAULT_GOAL_MULTIPLIER: float = 3.0  # if user doesn't set a goal, multiply current sessions by this

# Channel mix defaults (must sum to 100). Mirrors the napkin-math the user
# walks through with prospects today.
DEFAULT_MIX = {
    "organic": 25.0,
    "on_channel_paid": 25.0,
    "off_channel_paid": 25.0,
    "affiliate": 15.0,
    "retargeting": 10.0,
}

# Channel-cost defaults
# - on_channel_cpc: Pacvue Q1 2026 supplements range $2.50–$7.00, anchor low end
# - off_channel_cpc: $0.15 is the Anata storefront-traffic strategy CPC, NOT
#   the WordStream direct-PDP supplements CPC ($1.81); see methodology footnote.
# - dsp_prospecting_cpm: $5–$25 typical (Sequence Commerce 2026); anchor mid-low
# - dsp_retargeting_cpm: supplements toward higher end of the range
# - dsp_avg_ctr_pct: Sequence Commerce 2026 (0.42% baseline)
# - retargeting_ctr_multiplier: 2–10x lift typical, anchor 3x mid
DEFAULT_ON_CHANNEL_CPC: float = 3.00
DEFAULT_OFF_CHANNEL_CPC: float = 0.15
DEFAULT_DSP_PROSPECTING_CPM: float = 10.00
DEFAULT_DSP_RETARGETING_CPM: float = 13.00
DEFAULT_DSP_AVG_CTR_PCT: float = 0.42
DEFAULT_RETARGETING_CTR_MULTIPLIER: float = 3.0

# Affiliate sub-model defaults
DEFAULT_VIDEOS_PER_MONTH: int = 8
DEFAULT_AVG_IMPRESSIONS_PER_VIDEO: int = 50_000
DEFAULT_SHOPPABLE_CTR_PCT: float = 2.0
DEFAULT_TIKTOK_PLATFORM_COMMISSION_PCT: float = 7.0
DEFAULT_CREATOR_COMMISSION_PCT: float = 13.0
DEFAULT_HYBRID_FLAT_FEE_PER_VIDEO: float = 0.0
DEFAULT_TIKTOK_TO_AMAZON_CVR_UPLIFT: float = 0.85  # directional drag vs cold paid

# Retargeting sub-model defaults
DEFAULT_AUDIENCE_WINDOW_DAYS: int = 60
DEFAULT_FREQUENCY_CAP: int = 4
DEFAULT_REPEAT_CVR_MULTIPLIER: float = 2.5  # directional
DEFAULT_BTP_REDEMPTION_PCT: float = 5.0  # directional


# ---------------------------------------------------------------------------
# Inputs / output dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class GrowthPlanInputs:
    """Form-supplied inputs. All fields have defaults so callers can pass
    sparse partial inputs and rely on the defaults for the rest."""

    # Plan-level
    conversion_rate_pct: float = DEFAULT_CONVERSION_RATE_PCT
    goal_monthly_sessions: int | None = None  # None → derive from goal_multiplier or top-3 avg
    goal_multiplier: float = DEFAULT_GOAL_MULTIPLIER
    average_order_value: float | None = None  # None → use target listing price

    # Channel mix (percentages, must sum to 100)
    mix_organic: float = DEFAULT_MIX["organic"]
    mix_on_channel_paid: float = DEFAULT_MIX["on_channel_paid"]
    mix_off_channel_paid: float = DEFAULT_MIX["off_channel_paid"]
    mix_affiliate: float = DEFAULT_MIX["affiliate"]
    mix_retargeting: float = DEFAULT_MIX["retargeting"]

    # Channel-cost inputs
    on_channel_cpc: float = DEFAULT_ON_CHANNEL_CPC
    off_channel_cpc: float = DEFAULT_OFF_CHANNEL_CPC
    dsp_prospecting_cpm: float = DEFAULT_DSP_PROSPECTING_CPM
    dsp_retargeting_cpm: float = DEFAULT_DSP_RETARGETING_CPM
    retargeting_ctr_pct: float = field(
        default=DEFAULT_DSP_AVG_CTR_PCT * DEFAULT_RETARGETING_CTR_MULTIPLIER
    )

    # Affiliate sub-model
    videos_per_month: int = DEFAULT_VIDEOS_PER_MONTH
    avg_impressions_per_video: int = DEFAULT_AVG_IMPRESSIONS_PER_VIDEO
    shoppable_ctr_pct: float = DEFAULT_SHOPPABLE_CTR_PCT
    tiktok_platform_commission_pct: float = DEFAULT_TIKTOK_PLATFORM_COMMISSION_PCT
    creator_commission_pct: float = DEFAULT_CREATOR_COMMISSION_PCT
    hybrid_flat_fee_per_video: float = DEFAULT_HYBRID_FLAT_FEE_PER_VIDEO
    cogs_per_unit: float = 0.0
    shipping_per_unit: float = 0.0
    tiktok_to_amazon_cvr_uplift: float = DEFAULT_TIKTOK_TO_AMAZON_CVR_UPLIFT

    # Retargeting sub-model
    audience_window_days: int = DEFAULT_AUDIENCE_WINDOW_DAYS
    frequency_cap: int = DEFAULT_FREQUENCY_CAP
    repeat_cvr_multiplier: float = DEFAULT_REPEAT_CVR_MULTIPLIER
    btp_redemption_pct: float = DEFAULT_BTP_REDEMPTION_PCT

    def mix_total_pct(self) -> float:
        return (
            self.mix_organic
            + self.mix_on_channel_paid
            + self.mix_off_channel_paid
            + self.mix_affiliate
            + self.mix_retargeting
        )

    def validate(self) -> list[str]:
        """Return list of human-readable validation errors; empty list = OK."""
        errors: list[str] = []
        if not (0 < self.conversion_rate_pct <= 100):
            errors.append("Conversion rate must be between 0 and 100 (exclusive of 0).")
        total = self.mix_total_pct()
        if abs(total - 100.0) > 0.01:
            errors.append(f"Channel mix percentages must sum to 100; got {total:.1f}.")
        if self.mix_affiliate > 0:
            if self.cogs_per_unit <= 0:
                errors.append("COGS per unit is required when affiliate mix > 0.")
            if self.shipping_per_unit < 0:
                errors.append("Shipping per unit cannot be negative.")
        return errors


@dataclass(frozen=True)
class GrowthChannel:
    """One row in the channel-mix table."""

    key: str  # 'organic' | 'on_channel_paid' | 'off_channel_paid' | 'affiliate' | 'retargeting'
    label: str
    mix_pct: float
    sessions: int
    monthly_cost: float
    detail: str  # short subtitle line ("@ $0.15 CPC", "8 videos × 50,000 imps × 2% CTR", etc.)
    source_label: str
    is_directional: bool = False


@dataclass(frozen=True)
class GrowthPlan:
    current_sessions: int
    goal_sessions: int
    delta_sessions: int
    target_units: int
    cvr_pct: float
    channels: list[GrowthChannel]
    total_monthly_spend: float
    total_sessions_delivered: int
    shortfall_sessions: int
    methodology_lines: list[str]


# ---------------------------------------------------------------------------
# Math
# ---------------------------------------------------------------------------


def build_growth_plan(
    *,
    inputs: GrowthPlanInputs,
    target_units: int,
    top3_competitor_avg_sessions: int | None = None,
) -> GrowthPlan:
    """Compute the full plan from inputs.

    target_units: monthly units sold by the target listing (from Helium 10 Xray
    or hero product enrichment). Used to reverse-engineer current sessions.
    top3_competitor_avg_sessions: optional, used as a fallback goal default
    when neither goal_monthly_sessions nor goal_multiplier are useful.
    """
    cvr = inputs.conversion_rate_pct / 100.0
    if cvr <= 0:
        cvr = DEFAULT_CONVERSION_RATE_PCT / 100.0

    current_sessions = int(round(target_units / cvr)) if target_units > 0 else 0

    if inputs.goal_monthly_sessions and inputs.goal_monthly_sessions > 0:
        goal_sessions = int(inputs.goal_monthly_sessions)
    elif top3_competitor_avg_sessions and top3_competitor_avg_sessions > current_sessions:
        goal_sessions = int(top3_competitor_avg_sessions)
    else:
        goal_sessions = int(round(current_sessions * inputs.goal_multiplier))

    delta_sessions = max(0, goal_sessions - current_sessions)

    channels = [
        _build_organic_channel(inputs, delta_sessions),
        _build_on_channel_paid_channel(inputs, delta_sessions),
        _build_off_channel_paid_channel(inputs, delta_sessions),
        _build_affiliate_channel(inputs, delta_sessions, cvr),
        _build_retargeting_channel(inputs, delta_sessions, cvr, current_sessions),
    ]

    total_monthly_spend = sum(c.monthly_cost for c in channels)
    total_sessions_delivered = sum(c.sessions for c in channels)
    shortfall_sessions = max(0, delta_sessions - total_sessions_delivered)

    methodology_lines = [
        f"Conversion rate: {inputs.conversion_rate_pct:.1f}% (form input; supplements typically 7–15% on Amazon).",
        "Sponsored Products / Brands CPC: Pacvue Q1 2026 Health & Household ($2.17 avg; supplements $2.50–$7.00).",
        "Off-channel paid CPC at $0.15 reflects storefront-link traffic optimized for Amazon's external-traffic signal "
        "(lifts organic rank on adjacent keywords), NOT direct-response. Direct-to-PDP supplements CPC is "
        "~$1.20–$1.81 (WordStream 2025 / Digital Applied 2026) and is modeled separately when ROAS-on-the-click is the objective.",
        "DSP CPM: Sequence Commerce / Amazon Ads 2026 ($5–$25 typical).",
        "Retargeting CTR lift, ROAS: Sequence Commerce 2026; Tinuiti Q1 2026 Digital Ads Benchmark.",
        "TikTok Shop creator commission: Shortform Nation 2026 (US average 13%).",
        "Per-video impressions, shoppable CTR, repeat-purchase CVR multiplier, and Brand-Tailored promo redemption are "
        "directional starters; calibrate with first-party data after the first 30 days.",
    ]

    return GrowthPlan(
        current_sessions=current_sessions,
        goal_sessions=goal_sessions,
        delta_sessions=delta_sessions,
        target_units=target_units,
        cvr_pct=inputs.conversion_rate_pct,
        channels=channels,
        total_monthly_spend=total_monthly_spend,
        total_sessions_delivered=total_sessions_delivered,
        shortfall_sessions=shortfall_sessions,
        methodology_lines=methodology_lines,
    )


def _alloc(delta: int, mix_pct: float) -> int:
    return int(round(delta * mix_pct / 100.0))


def _build_organic_channel(inputs: GrowthPlanInputs, delta: int) -> GrowthChannel:
    sessions = _alloc(delta, inputs.mix_organic)
    return GrowthChannel(
        key="organic",
        label="Organic",
        mix_pct=inputs.mix_organic,
        sessions=sessions,
        monthly_cost=0.0,
        detail="SEO listing optimization; 60–90 day ramp",
        source_label="No paid spend — investment in title/bullet/imagery work",
    )


def _build_on_channel_paid_channel(inputs: GrowthPlanInputs, delta: int) -> GrowthChannel:
    sessions = _alloc(delta, inputs.mix_on_channel_paid)
    cost = sessions * inputs.on_channel_cpc
    return GrowthChannel(
        key="on_channel_paid",
        label="On-channel paid (SP / SB / DSP cold)",
        mix_pct=inputs.mix_on_channel_paid,
        sessions=sessions,
        monthly_cost=cost,
        detail=f"@ ${inputs.on_channel_cpc:,.2f} CPC",
        source_label="Source: Pacvue Q1 2026 Health & Household",
    )


def _build_off_channel_paid_channel(inputs: GrowthPlanInputs, delta: int) -> GrowthChannel:
    sessions = _alloc(delta, inputs.mix_off_channel_paid)
    cost = sessions * inputs.off_channel_cpc
    return GrowthChannel(
        key="off_channel_paid",
        label="Off-channel paid (Meta / TikTok storefront-link)",
        mix_pct=inputs.mix_off_channel_paid,
        sessions=sessions,
        monthly_cost=cost,
        detail=f"@ ${inputs.off_channel_cpc:,.2f} CPC, routed to storefront for Amazon external-traffic signal",
        source_label="Anata storefront-link strategy (see methodology footnote)",
    )


def _build_affiliate_channel(
    inputs: GrowthPlanInputs,
    delta: int,
    cvr: float,
) -> GrowthChannel:
    sessions = _alloc(delta, inputs.mix_affiliate)
    if sessions <= 0:
        return GrowthChannel(
            key="affiliate",
            label="Affiliate (TikTok creators)",
            mix_pct=inputs.mix_affiliate,
            sessions=0,
            monthly_cost=0.0,
            detail="Skipped — mix set to 0%",
            source_label="Directional — calibrate with first-party data",
            is_directional=True,
        )

    shoppable_ctr = max(inputs.shoppable_ctr_pct, 0.01) / 100.0
    required_impressions = sessions / shoppable_ctr
    required_videos = math.ceil(
        required_impressions / max(inputs.avg_impressions_per_video, 1)
    )

    units_from_affiliate = sessions * cvr * inputs.tiktok_to_amazon_cvr_uplift
    aov = inputs.average_order_value or 0.0

    unit_economics_cost = units_from_affiliate * (
        inputs.cogs_per_unit
        + inputs.shipping_per_unit
        + aov * inputs.tiktok_platform_commission_pct / 100.0
        + aov * inputs.creator_commission_pct / 100.0
    )
    flat_fee_cost = required_videos * inputs.hybrid_flat_fee_per_video
    total_cost = unit_economics_cost + flat_fee_cost

    detail = (
        f"{required_videos} videos × {inputs.avg_impressions_per_video:,} imps × "
        f"{inputs.shoppable_ctr_pct:.1f}% CTR → ~{int(round(units_from_affiliate)):,} units"
    )
    return GrowthChannel(
        key="affiliate",
        label="Affiliate (TikTok creators)",
        mix_pct=inputs.mix_affiliate,
        sessions=sessions,
        monthly_cost=total_cost,
        detail=detail,
        source_label="Directional — calibrate with first-party data",
        is_directional=True,
    )


def _build_retargeting_channel(
    inputs: GrowthPlanInputs,
    delta: int,
    cvr: float,
    current_sessions: int,
) -> GrowthChannel:
    sessions = _alloc(delta, inputs.mix_retargeting)
    if sessions <= 0 or current_sessions <= 0:
        return GrowthChannel(
            key="retargeting",
            label="Retargeting / LTV (DSP retargeting + Brand Tailored)",
            mix_pct=inputs.mix_retargeting,
            sessions=0,
            monthly_cost=0.0,
            detail="Needs current sessions > 0 and retargeting mix > 0",
            source_label="Directional — calibrate with first-party data",
            is_directional=True,
        )

    eligible_audience = current_sessions * (inputs.audience_window_days / 30.0)
    impressions = eligible_audience * inputs.frequency_cap
    spend = impressions / 1000.0 * inputs.dsp_retargeting_cpm
    returning_sessions = impressions * inputs.retargeting_ctr_pct / 100.0
    repeat_units = returning_sessions * cvr * inputs.repeat_cvr_multiplier
    btp_redemptions = eligible_audience * inputs.btp_redemption_pct / 100.0

    detail = (
        f"audience ~{int(round(eligible_audience)):,} × {inputs.frequency_cap} freq → "
        f"~{int(round(returning_sessions)):,} returning sessions, ~{int(round(repeat_units)):,} repeat units, "
        f"~{int(round(btp_redemptions)):,} BTP redemptions @ ${inputs.dsp_retargeting_cpm:.2f} CPM"
    )
    return GrowthChannel(
        key="retargeting",
        label="Retargeting / LTV (DSP retargeting + Brand Tailored)",
        mix_pct=inputs.mix_retargeting,
        sessions=sessions,
        monthly_cost=spend,
        detail=detail,
        source_label="Repeat CVR + BTP redemption are directional; calibrate with first-party data",
        is_directional=True,
    )


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def _money(value: float) -> str:
    if abs(value) >= 1000:
        return f"${value:,.0f}"
    return f"${value:,.2f}"


def _render_funnel_svg(plan: GrowthPlan, *, target_aov: float) -> str:
    """Render the customer-funnel SVG: traffic sources → PDP visits →
    units → revenue. Channel boxes at the top are sized proportional to
    their session share. All flow lines drawn in pure SVG so it prints
    crisp from the browser.
    """
    # Pull data
    cvr = max(plan.cvr_pct, 0.01)
    delivered = max(plan.total_sessions_delivered, 1)
    # Use total sessions delivered (delta-driven) as the funnel mouth
    pdp_visits = plan.total_sessions_delivered
    expected_units = int(round(pdp_visits * cvr / 100.0))
    expected_revenue = expected_units * max(target_aov, 0.0)

    # Layout constants (viewBox units)
    VB_W = 1000
    VB_H = 540
    MARGIN_X = 24
    GAP = 8
    BOX_HEIGHT = 110
    Y_TOP = 16

    # Filter to non-zero channels for the funnel. Even if a channel has a
    # mix% but zero sessions (e.g. organic with delta=0) keep it; visual
    # presence matters for the "5 channels feeding the funnel" story.
    channels = [c for c in plan.channels if c.mix_pct > 0]
    if not channels:
        return ""

    total_pct = sum(c.mix_pct for c in channels) or 100.0
    inner_w = VB_W - 2 * MARGIN_X - GAP * (len(channels) - 1)
    # Minimum visual width per box so the label remains readable
    min_box = 110

    # Compute widths: proportional but no smaller than min_box. If
    # proportional widths sum < inner_w (because of mins), distribute
    # the slack proportionally on top.
    raw_widths = [inner_w * (c.mix_pct / total_pct) for c in channels]
    widths = [max(w, min_box) for w in raw_widths]
    overflow = sum(widths) - inner_w
    if overflow > 0:
        # Shrink the largest boxes proportionally to fit
        shrinkable = [(i, w) for i, w in enumerate(widths) if w > min_box]
        shrinkable_total = sum(w - min_box for _, w in shrinkable) or 1
        for i, w in shrinkable:
            widths[i] = max(min_box, w - overflow * (w - min_box) / shrinkable_total)

    # X positions
    x_positions: list[float] = []
    cursor = MARGIN_X
    for w in widths:
        x_positions.append(cursor)
        cursor += w + GAP

    # Channel colors (CSS-var-friendly fallbacks too)
    color_map = {
        "organic":          ("#dceaf5", "#4f84c4", "#1d2d44"),  # bg, border, text
        "on_channel_paid":  ("#cfe1ee", "#3f6da6", "#1d2d44"),
        "off_channel_paid": ("#bcd6e9", "#2c5d99", "#0f1d33"),
        "affiliate":        ("#f1ead8", "#bfa889", "#3a3528"),
        "retargeting":      ("#cfd6e1", "#33445c", "#1d2d44"),
    }
    # Short labels for the funnel boxes — full labels are tooltip / detail copy.
    short_label_map = {
        "organic":          "Organic",
        "on_channel_paid":  "On-channel paid",
        "off_channel_paid": "Off-channel paid",
        "affiliate":        "Affiliate",
        "retargeting":      "Retargeting",
    }

    # Build top-row channel boxes
    top_boxes_svg = ""
    flow_paths_svg = ""
    pdp_x = MARGIN_X + 220  # left edge of the merge box
    pdp_w = VB_W - 2 * (MARGIN_X + 220)
    pdp_cx = VB_W / 2
    pdp_y = Y_TOP + BOX_HEIGHT + 80  # 80px of flow runway

    for w, x, ch in zip(widths, x_positions, channels):
        bg, border, text = color_map.get(ch.key, ("#e9eef4", "#85bbda", "#1d2d44"))
        cx = x + w / 2
        cy = Y_TOP + BOX_HEIGHT
        # Box rectangle
        top_boxes_svg += (
            f'<rect x="{x:.1f}" y="{Y_TOP}" width="{w:.1f}" height="{BOX_HEIGHT}" '
            f'rx="14" fill="{bg}" stroke="{border}" stroke-width="1.5"/>'
        )
        # Channel name (top line) — short label so it fits the box
        short_label = short_label_map.get(ch.key, ch.label.split(" (")[0])
        top_boxes_svg += (
            f'<text x="{cx:.1f}" y="{Y_TOP + 26}" text-anchor="middle" '
            f'font-size="13" font-weight="700" fill="{text}">'
            f'{html.escape(short_label)}</text>'
        )
        # Mix percentage
        top_boxes_svg += (
            f'<text x="{cx:.1f}" y="{Y_TOP + 50}" text-anchor="middle" '
            f'font-size="11" font-weight="600" fill="{text}" opacity="0.78">'
            f'{ch.mix_pct:.0f}% of mix</text>'
        )
        # Sessions
        top_boxes_svg += (
            f'<text x="{cx:.1f}" y="{Y_TOP + 76}" text-anchor="middle" '
            f'font-size="20" font-weight="800" fill="{text}">'
            f'{ch.sessions:,}</text>'
        )
        # Cost line
        if ch.key == "organic":
            cost_label = "SEO investment"
        else:
            cost_label = f"${ch.monthly_cost:,.0f}/mo"
        top_boxes_svg += (
            f'<text x="{cx:.1f}" y="{Y_TOP + 98}" text-anchor="middle" '
            f'font-size="10" font-weight="500" fill="{text}" opacity="0.72">'
            f'{html.escape(cost_label)}</text>'
        )
        # Flow path: cubic bezier from box bottom-center down to PDP top-center
        target_x = pdp_cx
        target_y = pdp_y
        # Control points create a smooth converge into the merge point
        c1y = cy + 30
        c2y = target_y - 30
        flow_paths_svg += (
            f'<path d="M {cx:.1f} {cy} C {cx:.1f} {c1y}, {target_x:.1f} {c2y}, {target_x:.1f} {target_y}" '
            f'stroke="{border}" stroke-width="2" fill="none" opacity="0.55"/>'
        )

    # PDP visits middle box
    pdp_h = 64
    pdp_box_svg = (
        f'<rect x="{pdp_x}" y="{pdp_y}" width="{pdp_w:.1f}" height="{pdp_h}" rx="14" '
        f'fill="#10233d" stroke="#10233d"/>'
        f'<text x="{pdp_cx:.1f}" y="{pdp_y + 22}" text-anchor="middle" '
        f'font-size="11" font-weight="600" fill="#85bbda" letter-spacing="0.06em" '
        f'text-transform="uppercase">PDP VISITS</text>'
        f'<text x="{pdp_cx:.1f}" y="{pdp_y + 50}" text-anchor="middle" '
        f'font-size="26" font-weight="800" fill="#fffdf9">{pdp_visits:,} sessions</text>'
    )

    # Arrow + multiplier label between PDP and Units
    arrow1_y_top = pdp_y + pdp_h + 8
    arrow1_y_bot = arrow1_y_top + 36
    arrow1_svg = (
        f'<line x1="{pdp_cx:.1f}" y1="{arrow1_y_top}" x2="{pdp_cx:.1f}" y2="{arrow1_y_bot - 8}" '
        f'stroke="#33445c" stroke-width="2"/>'
        f'<polygon points="{pdp_cx - 6:.1f},{arrow1_y_bot - 8} {pdp_cx + 6:.1f},{arrow1_y_bot - 8} '
        f'{pdp_cx:.1f},{arrow1_y_bot}" fill="#33445c"/>'
        f'<text x="{pdp_cx + 18:.1f}" y="{arrow1_y_top + 24}" font-size="11" font-weight="600" '
        f'fill="#33445c">× {plan.cvr_pct:.1f}% CVR</text>'
    )

    # Units sold box
    units_y = arrow1_y_bot + 8
    units_h = 56
    units_w = pdp_w * 0.75
    units_x = pdp_cx - units_w / 2
    units_box_svg = (
        f'<rect x="{units_x:.1f}" y="{units_y}" width="{units_w:.1f}" height="{units_h}" rx="12" '
        f'fill="#fffdf9" stroke="#bfa889" stroke-width="2"/>'
        f'<text x="{pdp_cx:.1f}" y="{units_y + 20}" text-anchor="middle" '
        f'font-size="10" font-weight="600" fill="#33445c" letter-spacing="0.06em" '
        f'text-transform="uppercase">UNITS SOLD</text>'
        f'<text x="{pdp_cx:.1f}" y="{units_y + 44}" text-anchor="middle" '
        f'font-size="22" font-weight="800" fill="#1d2d44">{expected_units:,} units</text>'
    )

    # Arrow + AOV multiplier
    arrow2_y_top = units_y + units_h + 8
    arrow2_y_bot = arrow2_y_top + 32
    aov_label = f"× ${target_aov:,.2f} AOV" if target_aov > 0 else "× AOV"
    arrow2_svg = (
        f'<line x1="{pdp_cx:.1f}" y1="{arrow2_y_top}" x2="{pdp_cx:.1f}" y2="{arrow2_y_bot - 8}" '
        f'stroke="#33445c" stroke-width="2"/>'
        f'<polygon points="{pdp_cx - 6:.1f},{arrow2_y_bot - 8} {pdp_cx + 6:.1f},{arrow2_y_bot - 8} '
        f'{pdp_cx:.1f},{arrow2_y_bot}" fill="#33445c"/>'
        f'<text x="{pdp_cx + 18:.1f}" y="{arrow2_y_top + 22}" font-size="11" font-weight="600" '
        f'fill="#33445c">{html.escape(aov_label)}</text>'
    )

    # Revenue box
    rev_y = arrow2_y_bot + 8
    rev_h = 56
    rev_w = pdp_w * 0.6
    rev_x = pdp_cx - rev_w / 2
    rev_box_svg = (
        f'<rect x="{rev_x:.1f}" y="{rev_y}" width="{rev_w:.1f}" height="{rev_h}" rx="12" '
        f'fill="#10233d" stroke="#10233d"/>'
        f'<text x="{pdp_cx:.1f}" y="{rev_y + 20}" text-anchor="middle" '
        f'font-size="10" font-weight="600" fill="#85bbda" letter-spacing="0.06em" '
        f'text-transform="uppercase">PROJECTED REVENUE</text>'
        f'<text x="{pdp_cx:.1f}" y="{rev_y + 44}" text-anchor="middle" '
        f'font-size="22" font-weight="800" fill="#fffdf9">${expected_revenue:,.0f}/mo</text>'
    )

    return (
        f'<div class="growth-funnel">'
        f'<svg viewBox="0 0 {VB_W} {VB_H}" xmlns="http://www.w3.org/2000/svg" '
        f'role="img" aria-label="Customer funnel: traffic sources to revenue">'
        f'{flow_paths_svg}'
        f'{top_boxes_svg}'
        f'{pdp_box_svg}'
        f'{arrow1_svg}'
        f'{units_box_svg}'
        f'{arrow2_svg}'
        f'{rev_box_svg}'
        f'</svg>'
        f'</div>'
    )


def render_growth_plan_section(
    plan: GrowthPlan,
    *,
    target_brand: str,
    target_aov: float = 0.0,
) -> str:
    """Emit the HTML for the single 'Growth plan synopsis' slide."""
    if plan.delta_sessions <= 0:
        gap_caption = (
            f"{html.escape(target_brand)} is already at or above the goal "
            f"({plan.current_sessions:,} sessions vs goal of {plan.goal_sessions:,})."
        )
    else:
        gap_caption = (
            f"Reverse-engineered from {html.escape(target_brand)}'s units, "
            f"benchmarked against the visible market, and routed across paid + organic "
            f"to land at {plan.goal_sessions:,} monthly sessions."
        )

    kpi_strip = (
        "<div class='growth-kpis'>"
        "<div class='growth-kpi'>"
        "<span class='label'>Current sessions</span>"
        f"<strong>{plan.current_sessions:,}</strong>"
        f"<small>= {plan.target_units:,} units ÷ {plan.cvr_pct:.1f}%</small>"
        "</div>"
        "<div class='growth-kpi'>"
        "<span class='label'>Goal sessions</span>"
        f"<strong>{plan.goal_sessions:,}</strong>"
        "</div>"
        "<div class='growth-kpi'>"
        "<span class='label'>Sessions delta</span>"
        f"<strong>{plan.delta_sessions:,}</strong>"
        "</div>"
        "</div>"
    )

    cards_html = "".join(_render_channel_card(ch) for ch in plan.channels)

    daily_spend = plan.total_monthly_spend / 30.0
    if plan.shortfall_sessions > 0:
        shortfall_html = (
            f"<div class='growth-shortfall'>Channel mix delivers "
            f"{plan.total_sessions_delivered:,} sessions; goal needs {plan.delta_sessions:,}. "
            f"Shortfall: {plan.shortfall_sessions:,}. Increase mix or budget.</div>"
        )
    else:
        shortfall_html = ""

    methodology_lis = "".join(
        f"<li>{html.escape(line)}</li>" for line in plan.methodology_lines
    )

    funnel_svg = _render_funnel_svg(plan, target_aov=target_aov) if plan.delta_sessions > 0 else ""

    return f"""
    <section class="slide growth-plan-slide">
      <div class="slide-head">
        <div>
          <p class="eyebrow">Growth plan</p>
          <h2>Closing the sessions gap</h2>
        </div>
        <p class="muted">{gap_caption}</p>
      </div>
      {kpi_strip}
      {funnel_svg}
      <div class="channel-grid">{cards_html}</div>
      <div class="growth-summary">
        <div><strong>Total monthly spend:</strong> {_money(plan.total_monthly_spend)}
        <span class='muted'>({_money(daily_spend)}/day)</span></div>
        <div><strong>Total sessions delivered:</strong> {plan.total_sessions_delivered:,}</div>
      </div>
      {shortfall_html}
      <details class="growth-methodology">
        <summary>Methodology and sources</summary>
        <ul>{methodology_lis}</ul>
      </details>
    </section>
    """


def _render_channel_card(channel: GrowthChannel) -> str:
    directional_badge = (
        "<small class='directional'>Directional — calibrate with first-party data</small>"
        if channel.is_directional
        else ""
    )
    cost_line = (
        "<div class='card-cost'>SEO investment</div>"
        if channel.key == "organic"
        else f"<div class='card-cost'><strong>{_money(channel.monthly_cost)}</strong> / month</div>"
    )
    return (
        f"<article class='channel-card channel-{channel.key}'>"
        f"<div class='card-head'>"
        f"<h3>{html.escape(channel.label)}</h3>"
        f"<span class='card-mix'>{channel.mix_pct:.0f}% of mix</span>"
        f"</div>"
        f"<div class='card-sessions'>{channel.sessions:,} sessions</div>"
        f"{cost_line}"
        f"<p class='card-detail'>{html.escape(channel.detail)}</p>"
        f"<small class='card-source'>{html.escape(channel.source_label)}</small>"
        f"{directional_badge}"
        f"</article>"
    )


# ---------------------------------------------------------------------------
# Form-input parser (dict → GrowthPlanInputs)
# ---------------------------------------------------------------------------


def parse_growth_plan_inputs(form: dict[str, Any]) -> GrowthPlanInputs:
    """Build a GrowthPlanInputs from raw form values. Tolerates missing keys
    (defaults apply) and string-typed numeric values."""

    def _f(key: str, default: float) -> float:
        v = form.get(key)
        if v is None or v == "":
            return default
        try:
            return float(v)
        except (TypeError, ValueError):
            return default

    def _i(key: str, default: int) -> int:
        v = form.get(key)
        if v is None or v == "":
            return default
        try:
            return int(float(v))
        except (TypeError, ValueError):
            return default

    def _i_or_none(key: str) -> int | None:
        v = form.get(key)
        if v is None or v == "":
            return None
        try:
            n = int(float(v))
            return n if n > 0 else None
        except (TypeError, ValueError):
            return None

    def _f_or_none(key: str) -> float | None:
        v = form.get(key)
        if v is None or v == "":
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    return GrowthPlanInputs(
        conversion_rate_pct=_f("growth_cvr_pct", DEFAULT_CONVERSION_RATE_PCT),
        goal_monthly_sessions=_i_or_none("growth_goal_sessions"),
        goal_multiplier=_f("growth_goal_multiplier", DEFAULT_GOAL_MULTIPLIER),
        average_order_value=_f_or_none("growth_aov"),
        mix_organic=_f("growth_mix_organic", DEFAULT_MIX["organic"]),
        mix_on_channel_paid=_f("growth_mix_on_channel_paid", DEFAULT_MIX["on_channel_paid"]),
        mix_off_channel_paid=_f("growth_mix_off_channel_paid", DEFAULT_MIX["off_channel_paid"]),
        mix_affiliate=_f("growth_mix_affiliate", DEFAULT_MIX["affiliate"]),
        mix_retargeting=_f("growth_mix_retargeting", DEFAULT_MIX["retargeting"]),
        on_channel_cpc=_f("growth_on_channel_cpc", DEFAULT_ON_CHANNEL_CPC),
        off_channel_cpc=_f("growth_off_channel_cpc", DEFAULT_OFF_CHANNEL_CPC),
        dsp_prospecting_cpm=_f("growth_dsp_prospecting_cpm", DEFAULT_DSP_PROSPECTING_CPM),
        dsp_retargeting_cpm=_f("growth_dsp_retargeting_cpm", DEFAULT_DSP_RETARGETING_CPM),
        retargeting_ctr_pct=_f(
            "growth_retargeting_ctr_pct",
            DEFAULT_DSP_AVG_CTR_PCT * DEFAULT_RETARGETING_CTR_MULTIPLIER,
        ),
        videos_per_month=_i("growth_videos_per_month", DEFAULT_VIDEOS_PER_MONTH),
        avg_impressions_per_video=_i(
            "growth_avg_impressions_per_video", DEFAULT_AVG_IMPRESSIONS_PER_VIDEO
        ),
        shoppable_ctr_pct=_f("growth_shoppable_ctr_pct", DEFAULT_SHOPPABLE_CTR_PCT),
        tiktok_platform_commission_pct=_f(
            "growth_tiktok_platform_commission_pct", DEFAULT_TIKTOK_PLATFORM_COMMISSION_PCT
        ),
        creator_commission_pct=_f("growth_creator_commission_pct", DEFAULT_CREATOR_COMMISSION_PCT),
        hybrid_flat_fee_per_video=_f(
            "growth_hybrid_flat_fee_per_video", DEFAULT_HYBRID_FLAT_FEE_PER_VIDEO
        ),
        cogs_per_unit=_f("growth_cogs_per_unit", 0.0),
        shipping_per_unit=_f("growth_shipping_per_unit", 0.0),
        tiktok_to_amazon_cvr_uplift=_f(
            "growth_tiktok_to_amazon_cvr_uplift", DEFAULT_TIKTOK_TO_AMAZON_CVR_UPLIFT
        ),
        audience_window_days=_i("growth_audience_window_days", DEFAULT_AUDIENCE_WINDOW_DAYS),
        frequency_cap=_i("growth_frequency_cap", DEFAULT_FREQUENCY_CAP),
        repeat_cvr_multiplier=_f("growth_repeat_cvr_multiplier", DEFAULT_REPEAT_CVR_MULTIPLIER),
        btp_redemption_pct=_f("growth_btp_redemption_pct", DEFAULT_BTP_REDEMPTION_PCT),
    )
