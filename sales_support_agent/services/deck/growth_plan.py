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
    # Richer per-channel detail (PR22):
    campaign_description: str = ""  # what tactics actually run (2–3 lines)
    strategic_why: str = ""  # why this channel exists in the mix (1–2 lines)
    expected_units: int = 0
    expected_revenue: float = 0.0
    is_directional: bool = False
    # PR26: which phase the channel first comes online in (1–4).
    # Used to filter the funnel visual per phase tab and to order the
    # implementation roadmap.
    first_active_phase: int = 1


# ---------------------------------------------------------------------------
# Implementation phases (PR26)
# ---------------------------------------------------------------------------
# Maps the 5-channel mix onto a 4-phase rollout. Phase mapping derives from
# typical setup time + dependency requirements per channel:
#   Phase 1 (Foundation, D0–14):  organic, on_channel_paid (SP launches first)
#   Phase 2 (Acceleration, W3–8): + off_channel_paid
#   Phase 3 (Scale, W8–16):       + affiliate (creator outreach lead time)
#   Phase 4 (LTV, M4+):           + retargeting (audience build prerequisite)
#
# Sources / citations populated by the research agent (PR27 will replace
# placeholder citations with real ones).


@dataclass(frozen=True)
class GrowthPhase:
    id: int
    key: str  # e.g. "foundation", "acceleration"
    label: str
    window_label: str  # human-readable window: "Days 0–14"
    summary: str  # one-line summary of phase intent
    channels_added: list[str]  # channel keys that come online THIS phase
    milestones: list[str] = field(default_factory=list)  # week-by-week tasks


PHASES: list[GrowthPhase] = [
    GrowthPhase(
        id=1,
        key="foundation",
        label="Foundation",
        window_label="Days 0–14",
        summary="Brand Registry + listing optimization + first paid demand capture.",
        channels_added=["organic", "on_channel_paid"],
        milestones=[
            "Wk 1: Submit/confirm Brand Registry (~10 business days approval).",
            "Wk 1: Rewrite title (≤200 chars), 5 bullets, 7 backend keywords, image stack on hero SKU.",
            "Wk 1–2: Publish A+ Content (auto-eligible once Brand Registry approves); build Storefront v1 (4–8 hr build, 24–72 hr Amazon review).",
            "Wk 2: Launch Sponsored Products auto + manual exact campaigns. First signal in 3–7 days; trends stable in 2–4 weeks.",
            "Wk 2: Sponsored Display Views retargeting on (no minimum spend; uses 30-day view audience).",
        ],
    ),
    GrowthPhase(
        id=2,
        key="acceleration",
        label="Acceleration",
        window_label="Weeks 3–8",
        summary="Defend brand search + open the external-traffic flywheel.",
        channels_added=["off_channel_paid"],
        milestones=[
            "Wk 3: Sponsored Brands live on the brand search term (requires Brand Registry + active Storefront as landing page).",
            "Wk 3–4: Generate Amazon Attribution tags; launch Meta + TikTok Ads pointing to Storefront / PDP. 14-day last-touch attribution window.",
            "Wk 4: Submit TikTok Shop seller application (1–3 business days approval).",
            "Wk 5–6: First creator outreach wave — assume 4–6 weeks end-to-end before videos go live.",
            "Wk 7–8: Read Attribution data; reallocate budget to top-converting creatives.",
        ],
    ),
    GrowthPhase(
        id=3,
        key="scale",
        label="Scale",
        window_label="Weeks 8–16",
        summary="Layer DSP cold prospecting + ramp the creator program.",
        channels_added=["affiliate"],
        milestones=[
            "Wk 8–10: DSP onboarding — agency self-service path (~$10K/mo practical floor) or Amazon-managed ($50K/mo minimum).",
            "Wk 10–12: Launch DSP cold prospecting against in-market + lifestyle audiences. Build 30-day audience windows.",
            "Wk 11–14: Scale TikTok Shop creator roster from pilot (~8 creators) to 15–30 creators; layer commission tiers.",
            "Wk 14–16: First read on DSP-assisted new-to-brand rate vs SP/SB-only baseline.",
        ],
    ),
    GrowthPhase(
        id=4,
        key="ltv",
        label="LTV",
        window_label="Months 4+",
        summary="Compound past-viewer + past-purchaser audiences.",
        channels_added=["retargeting"],
        milestones=[
            "Mo 4 wk 1: Confirm Brand-Tailored Promotion audience pools ≥ 1,000 customers (BTP eligibility floor).",
            "Mo 4 wk 2: First BTP coupon to repeat-buyer audience. Featured Offer status required for badge display.",
            "Mo 4 wk 3: DSP retargeting layered on top of cold prospecting using PDP-viewer + cart-abandon audiences.",
            "Mo 5+: Quarterly creator refresh; DSP audience expansion; Premium A+ Content evaluation (Amazon re-checks eligibility monthly).",
        ],
    ),
]


# Citations for the timelines above (used by Story / methodology footnote)
PHASE_CITATIONS: list[tuple[str, str]] = [
    ("Amazon Sell — Brand Registry requirements", "https://sell.amazon.com/blog/brand-registry-requirements"),
    ("Amazon Sell — Brand Registry main", "https://sell.amazon.com/brand-registry"),
    ("Trellis — Storefront setup guide", "https://gotrellis.com/resources/blog/amazon-storefront-setup-guide/"),
    ("Helium 10 — Listing optimization guide", "https://www.helium10.com/blog/amazon-listing-optimization-guide/"),
    ("Amazon Ads — First-30-days SP tips", "https://advertising.amazon.com/library/case-studies/tips-for-first-30-days-on-sponsored-products"),
    ("BeBold — How long does Amazon PPC take", "https://www.bebolddigital.com/blog/how-long-does-it-take-for-amazon-ppc-to-work"),
    ("Amazon Ads — SB eligibility", "https://advertising.amazon.com/help/G5DAD7ZM3N639QF4"),
    ("Tinuiti — Sponsored Display ads guide", "https://tinuiti.com/blog/amazon/amazon-sponsored-display-ads-guide/"),
    ("Amazon Ads — Attribution guide", "https://advertising.amazon.com/library/guides/basics-of-amazon-attribution"),
    ("Canopy Mgmt — TikTok Shop eligibility 2026", "https://canopymanagement.com/tiktok-shop-eligibility-what-you-need-to-get-started/"),
    ("Later — Influencer marketing campaign timeline", "https://later.com/blog/timeline-for-influencer-marketing-campaigns/"),
    ("Amazon Ads — DSP", "https://advertising.amazon.com/solutions/products/amazon-dsp"),
    ("Trellis — Amazon DSP cost", "https://gotrellis.com/resources/blog/amazon-dsp-cost/"),
    ("Amazon Sell — Brand-Tailored Promotions", "https://sell.amazon.com/blog/brand-tailored-promotions"),
    ("Amazon Ads — Display purchases remarketing", "https://advertising.amazon.com/library/guides/display-ads-purchases-remarketing"),
]


def _cumulative_active_keys(through_phase: int) -> list[str]:
    """Return all channel keys active through a given phase (cumulative)."""
    keys: list[str] = []
    for phase in PHASES:
        if phase.id <= through_phase:
            keys.extend(phase.channels_added)
    return keys


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


def _channel_outcome(sessions: int, cvr: float, aov: float, cvr_mult: float = 1.0) -> tuple[int, float]:
    """Compute (expected_units, expected_revenue) for a channel given its sessions.
    `cvr_mult` lets specific channels (off-channel storefront drag, retargeting lift)
    deviate from the baseline CVR without changing the math elsewhere.
    """
    units = int(round(sessions * cvr * cvr_mult))
    revenue = units * max(aov, 0.0)
    return units, revenue


def _build_organic_channel(inputs: GrowthPlanInputs, delta: int) -> GrowthChannel:
    sessions = _alloc(delta, inputs.mix_organic)
    aov = inputs.average_order_value or 0.0
    cvr = max(inputs.conversion_rate_pct / 100.0, 0.001)
    units, revenue = _channel_outcome(sessions, cvr, aov)
    return GrowthChannel(
        key="organic",
        label="Organic",
        mix_pct=inputs.mix_organic,
        sessions=sessions,
        monthly_cost=0.0,
        detail="SEO listing optimization; 60–90 day ramp",
        source_label="No paid spend — investment in title/bullet/imagery work",
        first_active_phase=1,
        campaign_description=(
            "Listing optimization (title, bullets, A+ content), brand story refresh, "
            "indexed-keyword expansion, and Q&A injection. SEO investment, no paid spend."
        ),
        strategic_why=(
            "Compounding equity. Every organic session won here is sticky and "
            "reduces ACoS pressure on every other paid channel. Expect 60–90 day ramp."
        ),
        expected_units=units,
        expected_revenue=revenue,
    )


def _build_on_channel_paid_channel(inputs: GrowthPlanInputs, delta: int) -> GrowthChannel:
    sessions = _alloc(delta, inputs.mix_on_channel_paid)
    cost = sessions * inputs.on_channel_cpc
    aov = inputs.average_order_value or 0.0
    cvr = max(inputs.conversion_rate_pct / 100.0, 0.001)
    units, revenue = _channel_outcome(sessions, cvr, aov)
    return GrowthChannel(
        key="on_channel_paid",
        label="On-channel paid (SP / SB / DSP cold)",
        mix_pct=inputs.mix_on_channel_paid,
        sessions=sessions,
        monthly_cost=cost,
        detail=f"@ ${inputs.on_channel_cpc:,.2f} CPC",
        source_label="Source: Pacvue Q1 2026 Health & Household",
        first_active_phase=1,
        campaign_description=(
            f"Sponsored Products on top-30 niche keywords @ ${inputs.on_channel_cpc:,.2f} CPC, "
            "Sponsored Brands defending the brand search term, Sponsored Display "
            "retargeting cart abandoners. Lower-funnel intent."
        ),
        strategic_why=(
            "Captures in-market buyers searching today — fastest channel to convert. "
            "Defends brand search from competitor conquesting."
        ),
        expected_units=units,
        expected_revenue=revenue,
    )


def _build_off_channel_paid_channel(inputs: GrowthPlanInputs, delta: int) -> GrowthChannel:
    sessions = _alloc(delta, inputs.mix_off_channel_paid)
    cost = sessions * inputs.off_channel_cpc
    aov = inputs.average_order_value or 0.0
    cvr = max(inputs.conversion_rate_pct / 100.0, 0.001)
    # Off-channel storefront-link traffic converts at a slight drag vs cold paid
    # because the engagement ask is lighter (storefront before PDP).
    units, revenue = _channel_outcome(sessions, cvr, aov, cvr_mult=inputs.tiktok_to_amazon_cvr_uplift)
    return GrowthChannel(
        key="off_channel_paid",
        label="Off-channel paid (Meta / TikTok storefront-link)",
        mix_pct=inputs.mix_off_channel_paid,
        sessions=sessions,
        monthly_cost=cost,
        detail=f"@ ${inputs.off_channel_cpc:,.2f} CPC, routed to storefront for Amazon external-traffic signal",
        source_label="Anata storefront-link strategy (see methodology footnote)",
        first_active_phase=2,
        campaign_description=(
            f"Meta and TikTok video ads driving to the brand's Amazon storefront @ "
            f"~${inputs.off_channel_cpc:,.2f} CPC. Lightweight engagement ask keeps "
            "click cost low. Optimized for traffic volume + acceptable CTR, not direct ROAS."
        ),
        strategic_why=(
            "Amazon's algorithm rewards external-traffic signal with stronger organic "
            "rank on adjacent keywords. The flywheel compounds with #1 (Organic). "
            "Direct conversions are a bonus, not the goal."
        ),
        expected_units=units,
        expected_revenue=revenue,
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
    affiliate_revenue = int(round(units_from_affiliate)) * aov
    return GrowthChannel(
        key="affiliate",
        label="Affiliate (TikTok creators)",
        mix_pct=inputs.mix_affiliate,
        sessions=sessions,
        monthly_cost=total_cost,
        detail=detail,
        source_label="Directional — calibrate with first-party data",
        first_active_phase=3,
        campaign_description=(
            f"{required_videos} mid-tier TikTok creators per month (10K–100K followers), "
            "shoppable affiliate links direct to PDP. Hybrid model: "
            f"{inputs.creator_commission_pct:.0f}% creator commission + "
            f"{inputs.tiktok_platform_commission_pct:.0f}% TikTok Shop platform fee on each sale, "
            f"plus COGS + shipping per unit."
        ),
        strategic_why=(
            "Creator-driven social proof at lower CAC than paid. Trust signal "
            "compounds — prospect trust transfers to brand without buying-intent "
            "fatigue. Calibrate impressions and CTR against first 30 days of real data."
        ),
        expected_units=int(round(units_from_affiliate)),
        expected_revenue=affiliate_revenue,
        is_directional=True,
    )


def _build_retargeting_channel(
    inputs: GrowthPlanInputs,
    delta: int,
    cvr: float,
    current_sessions: int,
) -> GrowthChannel:
    sessions = _alloc(delta, inputs.mix_retargeting)
    if sessions <= 0:
        return GrowthChannel(
            key="retargeting",
            label="Retargeting / LTV (DSP retargeting + Brand Tailored)",
            mix_pct=inputs.mix_retargeting,
            sessions=0,
            monthly_cost=0.0,
            detail="Mix set to 0% — channel skipped",
            source_label="Directional — calibrate with first-party data",
            is_directional=True,
        )

    # When current_sessions is 0 (target units unknown — e.g., user didn't
    # upload Target Xray CSV and Amazon's public page doesn't expose units),
    # we still allocate sessions to retargeting from the delta_sessions pool.
    # The audience-math just uses `sessions` as a proxy for the future
    # eligible audience after the first 30 days of paid + organic acquisition.
    audience_basis = current_sessions if current_sessions > 0 else sessions
    audience_basis_note = (
        ""
        if current_sessions > 0
        else " (basis: projected post-30-day acquisition; calibrate after first month)"
    )

    eligible_audience = audience_basis * (inputs.audience_window_days / 30.0)
    impressions = eligible_audience * inputs.frequency_cap
    spend = impressions / 1000.0 * inputs.dsp_retargeting_cpm
    returning_sessions = impressions * inputs.retargeting_ctr_pct / 100.0
    repeat_units = returning_sessions * cvr * inputs.repeat_cvr_multiplier
    btp_redemptions = eligible_audience * inputs.btp_redemption_pct / 100.0

    detail = (
        f"audience ~{int(round(eligible_audience)):,} × {inputs.frequency_cap} freq → "
        f"~{int(round(returning_sessions)):,} returning sessions, ~{int(round(repeat_units)):,} repeat units, "
        f"~{int(round(btp_redemptions)):,} BTP redemptions @ ${inputs.dsp_retargeting_cpm:.2f} CPM"
        f"{audience_basis_note}"
    )
    aov = inputs.average_order_value or 0.0
    retarget_revenue = int(round(repeat_units)) * aov
    return GrowthChannel(
        key="retargeting",
        label="Retargeting / LTV (DSP retargeting + Brand Tailored)",
        mix_pct=inputs.mix_retargeting,
        sessions=sessions,
        monthly_cost=spend,
        detail=detail,
        source_label="Repeat CVR + BTP redemption are directional; calibrate with first-party data",
        first_active_phase=4,
        campaign_description=(
            f"Amazon DSP retargeting past-{inputs.audience_window_days}-day PDP viewers, "
            f"frequency cap {inputs.frequency_cap} @ ${inputs.dsp_retargeting_cpm:.2f} CPM. "
            "Brand Tailored Promotions to past purchasers. Sponsored Display Product "
            "Retargeting audiences for cart-abandoners and category browsers."
        ),
        strategic_why=(
            f"Past viewers convert {inputs.repeat_cvr_multiplier:.1f}× higher than cold "
            "traffic — cheapest CAC in the mix. Compounds the harder you push the top "
            "of the funnel: every new session today seeds tomorrow's retargeting pool."
        ),
        expected_units=int(round(repeat_units)),
        expected_revenue=retarget_revenue,
        is_directional=True,
    )


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def _money(value: float) -> str:
    if abs(value) >= 1000:
        return f"${value:,.0f}"
    return f"${value:,.2f}"


def _render_funnel_svg(
    plan: GrowthPlan,
    *,
    target_aov: float,
    active_keys: list[str] | None = None,
) -> str:
    """Render the customer-funnel SVG: traffic sources → PDP visits →
    units → revenue. Channel boxes at the top are sized proportional to
    their session share. All flow lines drawn in pure SVG so it prints
    crisp from the browser.

    PR26: when `active_keys` is provided, only those channels contribute
    to PDP / units / revenue totals. Inactive channels still render in
    the top row but dimmed, so the layout doesn't reflow when the user
    toggles between phase tabs.
    """
    # Pull data
    cvr = max(plan.cvr_pct, 0.01)

    # Cumulative sessions delivered = sum across active channels only.
    if active_keys is None:
        active_set = {ch.key for ch in plan.channels}
    else:
        active_set = set(active_keys)
    pdp_visits = sum(ch.sessions for ch in plan.channels if ch.key in active_set)
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
        is_active = ch.key in active_set
        # Inactive channels render dimmed; active in full color.
        if is_active:
            bg, border, text = color_map.get(ch.key, ("#e9eef4", "#85bbda", "#1d2d44"))
            box_opacity = 1.0
            text_opacity_label = 0.78
            text_opacity_cost = 0.72
        else:
            # Same color family but desaturated + transparent so the box still
            # holds the layout but reads as "future phase".
            bg, border, text = ("#f1f3f6", "#c8cfd9", "#7c8696")
            box_opacity = 0.45
            text_opacity_label = 0.5
            text_opacity_cost = 0.5
        cx = x + w / 2
        cy = Y_TOP + BOX_HEIGHT
        # Box rectangle
        top_boxes_svg += (
            f'<rect x="{x:.1f}" y="{Y_TOP}" width="{w:.1f}" height="{BOX_HEIGHT}" '
            f'rx="14" fill="{bg}" stroke="{border}" stroke-width="1.5" '
            f'opacity="{box_opacity}"/>'
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
            f'font-size="11" font-weight="600" fill="{text}" opacity="{text_opacity_label}">'
            f'{ch.mix_pct:.0f}% of mix</text>'
        )
        # Sessions
        top_boxes_svg += (
            f'<text x="{cx:.1f}" y="{Y_TOP + 76}" text-anchor="middle" '
            f'font-size="20" font-weight="800" fill="{text}">'
            f'{ch.sessions:,}</text>'
        )
        # Cost line — show "Phase N" tag for inactive channels instead of cost
        if not is_active:
            cost_label = f"Phase {ch.first_active_phase}"
        elif ch.key == "organic":
            cost_label = "SEO investment"
        else:
            cost_label = f"${ch.monthly_cost:,.0f}/mo"
        top_boxes_svg += (
            f'<text x="{cx:.1f}" y="{Y_TOP + 98}" text-anchor="middle" '
            f'font-size="10" font-weight="500" fill="{text}" opacity="{text_opacity_cost}">'
            f'{html.escape(cost_label)}</text>'
        )
        # Flow path: cubic bezier from box bottom-center down to PDP top-center
        # Only draw flow paths for ACTIVE channels — inactive ones don't feed
        # the funnel yet.
        if is_active:
            target_x = pdp_cx
            target_y = pdp_y
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
    )


def _render_funnel_with_tabs(plan: GrowthPlan, *, target_aov: float) -> str:
    """Wrap the funnel in a tabbed control — one tab per implementation phase.
    Each tab shows the cumulative funnel state at that phase (which channels
    are lit, what PDP/units/revenue accumulate). Defaults to the last phase
    (steady state — all channels active)."""
    if not plan.channels:
        return ""

    tab_buttons = ""
    panels = ""
    last_phase_id = PHASES[-1].id
    for phase in PHASES:
        active_keys = _cumulative_active_keys(phase.id)
        # Sum the active channels for the tab summary line
        tab_sessions = sum(c.sessions for c in plan.channels if c.key in active_keys)
        tab_spend = sum(c.monthly_cost for c in plan.channels if c.key in active_keys)
        is_default = phase.id == last_phase_id  # default to steady state
        active_class = " is-active" if is_default else ""
        aria_pressed = "true" if is_default else "false"
        tab_buttons += (
            f'<button type="button" class="growth-funnel-tab{active_class}" '
            f'data-phase="{phase.id}" aria-pressed="{aria_pressed}">'
            f'<span class="tab-num">Phase {phase.id}</span>'
            f'<span class="tab-label">{html.escape(phase.label)}</span>'
            f'<span class="tab-window">{html.escape(phase.window_label)}</span>'
            f'<span class="tab-metric">{tab_sessions:,} sessions · {_money(tab_spend)}/mo</span>'
            f'</button>'
        )
        panel_hidden = "" if is_default else " hidden"
        funnel_svg = _render_funnel_svg(plan, target_aov=target_aov, active_keys=active_keys)
        # Per-phase summary copy
        added_labels = ", ".join(
            short
            for short in (
                {
                    "organic": "Organic",
                    "on_channel_paid": "On-channel paid",
                    "off_channel_paid": "Off-channel paid",
                    "affiliate": "Affiliate",
                    "retargeting": "Retargeting",
                }.get(k, k)
                for k in phase.channels_added
            )
        )
        panel_caption = (
            f"<p class='funnel-tab-caption'>"
            f"<strong>{html.escape(phase.summary)}</strong>"
            f" New this phase: <em>{html.escape(added_labels)}</em>."
            f"</p>"
        )
        default_attr = ' data-default="1"' if is_default else ''
        panels += (
            f'<div class="growth-funnel-panel" data-phase="{phase.id}"{default_attr}{panel_hidden}>'
            f'{panel_caption}'
            f'{funnel_svg}'
            f'</div>'
        )

    return (
        '<div class="growth-funnel growth-funnel-tabbed">'
        f'<div class="growth-funnel-tabs" role="tablist" aria-label="Implementation phases">'
        f'{tab_buttons}'
        f'</div>'
        f'{panels}'
        '</div>'
    )


def _render_growth_ramp(plan: GrowthPlan) -> str:
    """Print-friendly per-phase ramp showing how sessions accumulate from
    `current_sessions` to `goal_sessions` as channels come online phase by
    phase. Each step shows: phase label, channels NEW this phase, cumulative
    sessions delivered, % of the way to goal.

    This complements the (interactive) tabbed funnel above by giving a single
    glanceable "growth path" view that prints cleanly on a single page."""
    if plan.delta_sessions <= 0 or not plan.channels:
        return ""

    label_map = {
        "organic": "Organic",
        "on_channel_paid": "On-channel paid",
        "off_channel_paid": "Off-channel paid",
        "affiliate": "Affiliate",
        "retargeting": "Retargeting",
    }
    current = max(0, plan.current_sessions)
    goal = max(plan.goal_sessions, current + 1)  # avoid div by zero

    # Starting point is the "Today" tile — current sessions, 0% added.
    steps_html: list[str] = []
    today_pct = int(round(min(100.0, (current / goal) * 100.0))) if goal else 0
    steps_html.append(
        "<li class='growth-ramp-step is-today'>"
        "<div class='ramp-step-head'>"
        "<span class='ramp-step-num'>Today</span>"
        "<span class='ramp-step-label'>Starting point</span>"
        "</div>"
        f"<div class='ramp-step-sessions'><strong>{current:,}</strong> sessions</div>"
        "<div class='ramp-step-bar'>"
        f"<span class='ramp-step-bar-fill' style='width:{today_pct}%'></span>"
        "</div>"
        f"<div class='ramp-step-pct'>{today_pct}% of goal</div>"
        "</li>"
    )

    for phase in PHASES:
        active_keys = _cumulative_active_keys(phase.id)
        cumulative_added = sum(c.sessions for c in plan.channels if c.key in active_keys)
        cumulative_total = current + cumulative_added
        pct_of_goal = min(100.0, (cumulative_total / goal) * 100.0) if goal else 0.0
        added_labels = ", ".join(
            label_map.get(k, k) for k in phase.channels_added
        ) or "—"
        steps_html.append(
            "<li class='growth-ramp-step'>"
            "<div class='ramp-step-head'>"
            f"<span class='ramp-step-num'>Phase {phase.id}</span>"
            f"<span class='ramp-step-label'>{html.escape(phase.label)}</span>"
            f"<span class='ramp-step-window'>{html.escape(phase.window_label)}</span>"
            "</div>"
            f"<div class='ramp-step-sessions'><strong>{cumulative_total:,}</strong> sessions"
            f" <small class='muted'>(+{cumulative_added:,} from delta)</small></div>"
            "<div class='ramp-step-bar'>"
            f"<span class='ramp-step-bar-fill' style='width:{pct_of_goal:.1f}%'></span>"
            "</div>"
            f"<div class='ramp-step-pct'>{pct_of_goal:.0f}% of goal</div>"
            f"<div class='ramp-step-new'>+ {html.escape(added_labels)}</div>"
            "</li>"
        )

    return (
        "<div class='growth-ramp'>"
        "<div class='growth-ramp-head'>"
        "<h3>Growth path — how sessions ramp from today to goal</h3>"
        f"<p class='muted'>Each phase brings a new channel online. Cumulative session "
        f"delivery climbs from <strong>{current:,}</strong> to "
        f"<strong>{plan.current_sessions + plan.total_sessions_delivered:,}</strong> "
        f"against the <strong>{goal:,}</strong> goal.</p>"
        "</div>"
        f"<ol class='growth-ramp-steps'>{''.join(steps_html)}</ol>"
        "</div>"
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

    # PR26: tabbed funnel by implementation phase. Default tab is steady-state
    # (all channels active) so the static deck PDF still shows the full picture.
    funnel_svg = _render_funnel_with_tabs(plan, target_aov=target_aov) if plan.delta_sessions > 0 else ""

    # PR28: print-friendly per-phase ramp — shows cumulative session delivery
    # climbing from current → goal as channels come online. Always rendered on
    # paper (the tabbed funnel is interactive only).
    ramp_html = _render_growth_ramp(plan)

    return f"""
    <section class="slide growth-plan-slide">
      <div class="slide-head">
        <div>
          <p class="eyebrow">Growth plan</p>
          <h2>Closing the gap</h2>
        </div>
        <p class="muted">{gap_caption}</p>
      </div>
      {kpi_strip}
      {ramp_html}
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
    cost_text = (
        "SEO investment, no paid spend"
        if channel.key == "organic"
        else f"{_money(channel.monthly_cost)} / month"
    )

    # Outcome line: sessions → units → revenue
    if channel.expected_revenue > 0:
        outcome_line = (
            f"<div class='card-outcome'>"
            f"<strong>{channel.sessions:,}</strong> sessions "
            f"→ <strong>{channel.expected_units:,}</strong> units "
            f"→ <strong>{_money(channel.expected_revenue)}</strong> / mo"
            f"</div>"
        )
    else:
        outcome_line = f"<div class='card-outcome'><strong>{channel.sessions:,}</strong> sessions</div>"

    campaign_block = (
        f"<div class='card-block'>"
        f"<span class='card-block-label'>Campaign</span>"
        f"<p>{html.escape(channel.campaign_description)}</p>"
        f"</div>"
        if channel.campaign_description
        else ""
    )
    why_block = (
        f"<div class='card-block'>"
        f"<span class='card-block-label'>Why this channel</span>"
        f"<p>{html.escape(channel.strategic_why)}</p>"
        f"</div>"
        if channel.strategic_why
        else ""
    )

    return (
        f"<article class='channel-card channel-{channel.key}'>"
        f"<div class='card-head'>"
        f"<h3>{html.escape(channel.label)}</h3>"
        f"<span class='card-mix'>{channel.mix_pct:.0f}% of mix</span>"
        f"</div>"
        f"<div class='card-cost'>{html.escape(cost_text)}</div>"
        f"{outcome_line}"
        f"{campaign_block}"
        f"{why_block}"
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
