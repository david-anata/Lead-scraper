"""Pure advertising-audit analytics engine — no DB, no IO.

Takes normalized rows (schema.py dataclasses) + Goals and produces:
  * compute_summary(...) -> dict of headline metrics (ACoS, TACoS, blended
    TACoS incl. external+influencer spend, gap-to-goal)
  * build_recommendations(...) -> ranked list[Recommendation]

The rules encode standard Amazon advertising playbooks: kill wasted spend with
negatives, trim bids on over-target converters, scale bids on under-target
winners, harvest converting search terms into exact keywords, and flag
structural / external-channel issues as manual tasks.
"""

from __future__ import annotations

import re
from typing import Optional

from sales_support_agent.services.advertising.schema import (
    AdRow,
    CAT_BID_DOWN,
    CAT_BID_UP,
    CAT_EXTERNAL,
    CAT_MANUAL,
    CAT_NEGATIVE,
    CAT_NEW_KEYWORD,
    CAT_STRUCTURE,
    BULK_SUPPORTED,
    ExternalCostRow,
    Goals,
    MarketRow,
    Recommendation,
    SalesRow,
    SEV_HIGH,
    SEV_LOW,
    SEV_MEDIUM,
    Thresholds,
    acos_bps,
    fmt_money,
    fmt_pct,
    parse_int,
    tacos_bps,
)


# ---------------------------------------------------------------------------
# Summary metrics
# ---------------------------------------------------------------------------


def compute_summary(
    ad_rows: list[AdRow],
    sales_rows: list[SalesRow],
    external_rows: Optional[list[ExternalCostRow]] = None,
    goals: Optional[Goals] = None,
) -> dict:
    """Headline metrics. Ad totals are computed from the highest-fidelity level
    available so we don't double-count: campaign rows if present, else the sum
    of the finest level we have."""
    external_rows = external_rows or []
    goals = goals or Goals()

    ad_spend = _sum_spend(ad_rows)
    ad_sales = _sum_sales(ad_rows)
    impressions = sum(r.impressions for r in _dominant_rows(ad_rows))
    clicks = sum(r.clicks for r in _dominant_rows(ad_rows))
    ad_orders = sum(r.orders for r in _dominant_rows(ad_rows))

    total_sales = sum(r.ordered_product_sales_cents for r in sales_rows) or ad_sales
    total_units = sum(r.units for r in sales_rows)
    total_sessions = sum(r.sessions for r in sales_rows)

    external_spend = sum(r.amount_cents for r in external_rows)
    influencer_spend = sum(r.amount_cents for r in external_rows if r.cost_type == "commission" or r.channel == "influencer")

    summary = {
        "ad_spend_cents": ad_spend,
        "ad_sales_cents": ad_sales,
        "total_sales_cents": total_sales,
        "external_spend_cents": external_spend,
        "influencer_spend_cents": influencer_spend,
        "total_marketing_spend_cents": ad_spend + external_spend,
        "impressions": impressions,
        "clicks": clicks,
        "ad_orders": ad_orders,
        "total_units": total_units,
        "total_sessions": total_sessions,
        "acos_bps": acos_bps(ad_spend, ad_sales),
        "tacos_bps": tacos_bps(ad_spend, total_sales),
        "blended_tacos_bps": tacos_bps(ad_spend + external_spend, total_sales),
        "ctr_bps": round(clicks * 10000 / impressions) if impressions else None,
        "cpc_cents": round(ad_spend / clicks) if clicks else None,
        "goal": goals.to_dict(),
    }
    summary["gap"] = _goal_gap(summary, goals)
    return summary


def _goal_gap(summary: dict, goals: Goals) -> dict:
    gap: dict = {}
    if goals.revenue_target_cents:
        gap["revenue_target_cents"] = goals.revenue_target_cents
        gap["revenue_actual_cents"] = summary["total_sales_cents"]
        gap["revenue_gap_cents"] = goals.revenue_target_cents - summary["total_sales_cents"]
        gap["revenue_attainment_bps"] = (
            round(summary["total_sales_cents"] * 10000 / goals.revenue_target_cents)
            if goals.revenue_target_cents else None
        )
    if goals.acos_target_bps and summary["acos_bps"] is not None:
        gap["acos_delta_bps"] = summary["acos_bps"] - goals.acos_target_bps
    if goals.tacos_target_bps and summary["blended_tacos_bps"] is not None:
        gap["tacos_delta_bps"] = summary["blended_tacos_bps"] - goals.tacos_target_bps
    if goals.units_target:
        gap["units_delta"] = summary["total_units"] - goals.units_target
    return gap


# search_term rows are a redundant *view* of spend (a diagnostic breakdown), so
# they never count toward totals unless they're the only thing we have for an ad
# type. campaign/ad_group/product_ad are each complete, non-overlapping views of
# the same spend, so for totals we pick exactly one per ad type. (keyword/target
# rows from the bulk file are a sub-view but don't out-spend product_ad, so the
# max-spend selection already prefers product_ad — no need to mark them here.)
_DIAGNOSTIC_LEVELS = ("search_term",)


def _dominant_rows(ad_rows: list[AdRow]) -> list[AdRow]:
    """Pick one non-overlapping level *per ad type* for additive totals, then
    union across ad types.

    Real uploads contain several *alternate breakdowns* of the same ad spend
    (advertised-product, ad-group and search-term reports all sum to the same
    account total) and some are partial (a one-row ad-group export). Choosing by
    a fixed level rank breaks when the coarse level is the partial one, so we
    instead pick, per ad type, the level group whose rows sum to the **highest
    spend** — i.e. the most complete view — excluding diagnostic levels."""
    by_type: dict[str, list[AdRow]] = {}
    for r in ad_rows:
        by_type.setdefault(r.ad_type, []).append(r)

    chosen: list[AdRow] = []
    for rows in by_type.values():
        by_level: dict[str, list[AdRow]] = {}
        for r in rows:
            by_level.setdefault(r.entity_level, []).append(r)
        candidate_levels = [lvl for lvl in by_level if lvl not in _DIAGNOSTIC_LEVELS] or list(by_level)
        best = max(candidate_levels, key=lambda lvl: sum(r.spend_cents for r in by_level[lvl]))
        chosen.extend(by_level[best])
    return chosen


def _sum_spend(ad_rows: list[AdRow]) -> int:
    return sum(r.spend_cents for r in _dominant_rows(ad_rows))


def _sum_sales(ad_rows: list[AdRow]) -> int:
    return sum(r.sales_cents for r in _dominant_rows(ad_rows))


# ---------------------------------------------------------------------------
# Recommendation rules
# ---------------------------------------------------------------------------


def build_recommendations(
    ad_rows: list[AdRow],
    sales_rows: list[SalesRow],
    market_rows: Optional[list[MarketRow]] = None,
    external_rows: Optional[list[ExternalCostRow]] = None,
    goals: Optional[Goals] = None,
    thresholds: Optional[Thresholds] = None,
) -> list[Recommendation]:
    market_rows = market_rows or []
    external_rows = external_rows or []
    goals = goals or Goals()
    thr = thresholds or Thresholds()
    target_acos = goals.effective_acos_bps(thr)

    recs: list[Recommendation] = []
    # Brand's own category vocabulary (from product titles) — protects core terms
    # from being auto-negated; they're surfaced for review instead.
    vocab = _brand_vocabulary(sales_rows)
    recs += _rule_wasted_spend_negatives(ad_rows, thr, vocab)
    recs += _rule_bid_to_target(ad_rows, target_acos, thr)

    # A proven search term can be either harvested as an exact keyword into its
    # existing ad group OR promoted into its own dedicated campaign — never both
    # (that would make two of your own campaigns compete on the same exact term).
    # The higher-bar promotion claims the term; suppress the in-place harvest.
    harvest = _rule_harvest_keywords(ad_rows, thr)
    promote = _rule_promote_to_campaign(ad_rows, target_acos, thr)
    promoted = {
        (p.bulk_row.get("ad_type"), (p.bulk_row.get("keyword_text") or "").strip().lower())
        for p in promote
    }
    harvest = [
        h for h in harvest
        if (h.bulk_row.get("ad_type"), (h.bulk_row.get("keyword_text") or "").strip().lower()) not in promoted
    ]
    recs += harvest
    recs += promote

    recs += _rule_external_efficiency(external_rows, sales_rows, goals)
    recs += _rule_strategic_gap(ad_rows, sales_rows, external_rows, goals, thr)

    return rank_recommendations(recs)


def _campaign_slug(text: str) -> str:
    """A short, deterministic, file-safe token used for the placeholder Campaign/
    Ad Group IDs that link a new campaign's rows together in one bulk file."""
    import re
    s = re.sub(r"[^a-z0-9]+", "-", (text or "").lower()).strip("-")
    return (s[:40] or "term")


def _rule_promote_to_campaign(ad_rows: list[AdRow], target_acos: int, thr: Thresholds) -> list[Recommendation]:
    """Proven search terms (≥promote_campaign_min_orders, ACoS within
    max_acos_ratio of target, enough clicks) → propose isolating each into its own
    dedicated exact-match SP campaign for independent budget + bid control. The
    rec carries the whole campaign spec in bulk_row; bulk_sheets expands it into
    the linked Campaign→AdGroup→ProductAd→Keyword rows. Target SKUs are filled in
    later by normalizers.resolve_promotion_targets (needs the bulk file), so these
    start is_bulk_actionable=False (review-only until a SKU resolves)."""
    # A term already running as exact (in the account or marked exact in the
    # search-term report) is not a promotion candidate — it already exists.
    existing = {
        (r.ad_type, r.entity_text.strip().lower())
        for r in ad_rows
        if r.entity_level == "keyword"
    }
    existing |= {
        (r.ad_type, r.entity_text.strip().lower())
        for r in ad_rows
        if r.entity_level == "search_term" and (r.match_type or "").strip().lower() == "exact"
    }
    ceiling = round(target_acos * thr.promote_campaign_max_acos_ratio)
    out: list[Recommendation] = []
    seen: set = set()
    for r in ad_rows:
        if r.entity_level != "search_term" or not _bulk_ok(r.ad_type):
            continue
        if r.orders < thr.promote_campaign_min_orders or r.clicks < thr.min_clicks_significant:
            continue
        ra = r.acos_bps
        if ra is None or ra > ceiling:
            continue  # not efficient enough to scale into its own campaign
        key = (r.ad_type, r.entity_text.strip().lower())
        if key in existing or key in seen:
            continue
        seen.add(key)
        term = r.entity_text.strip()
        bid = _clamp_bid(r.cpc_cents or thr.min_bid_cents, thr)
        slug = _campaign_slug(term)
        campaign_name = f"SP | {term} | Exact"[:128]
        ad_group_name = f"{term} | Exact"[:128]
        rec = Recommendation(
            category=CAT_STRUCTURE,
            ad_type=r.ad_type,
            severity=SEV_MEDIUM,
            title=f"New campaign for '{term}' — {r.orders} orders at {fmt_pct(ra)} ACoS",
            detail=(
                f"Proven winner: {r.orders} orders / {fmt_money(r.sales_cents)} sales / {r.clicks} clicks "
                f"in '{r.campaign_name}'. Isolate it in its own exact campaign for budget + bid control."
            ),
            rationale="A search term beating target deserves its own campaign so it isn't capped by a shared budget or diluted by other terms' bids.",
            entity_ref=f"NEW › {campaign_name} › {term}",
            current_value=f"in '{r.campaign_name}'",
            proposed_value=f"new exact campaign @ {fmt_money(bid)}",
            projected_impact={"orders": r.orders, "sales_cents": r.sales_cents, "acos_bps": ra},
            bulk_row={
                "action": "create_campaign",
                "ad_type": r.ad_type,
                "slug": slug,
                "campaign_name": campaign_name,
                "ad_group_name": ad_group_name,
                "keyword_text": term,
                "match_type": "exact",
                "new_bid_cents": bid,
                "daily_budget_cents": thr.default_new_campaign_budget_cents,
                "targeting_type": "MANUAL",
                "bidding_strategy": "Dynamic bids - down only",
                "state": "enabled",
                "products": [],  # [{sku, asin}] — filled by resolve_promotion_targets
                "source_campaign_id": r.campaign_id,
                "source_ad_group_id": r.ad_group_id,
                "source_campaign_name": r.campaign_name,
                "source_ad_group_name": r.ad_group_name,
            },
            is_bulk_actionable=False,  # becomes True once a SKU resolves
        )
        rec.score = float(r.sales_cents) * 0.6  # rank above plain harvests
        out.append(rec)
    return out


def _bulk_ok(ad_type: str) -> bool:
    return ad_type in BULK_SUPPORTED


_NEG_STOPWORDS = {
    "with", "your", "this", "that", "from", "more", "best", "and", "the", "for", "men", "women",
    "daily", "day", "days", "supply", "count", "pack", "formula", "support", "supplement",
    "supplements", "natural", "plus", "extra", "maximum", "strength", "servings", "serving",
    "single", "made", "free", "premium", "advanced", "complex", "kit", "bundle", "pack",
}


def _brand_vocabulary(sales_rows: list[SalesRow]) -> set:
    """Content words from the brand's OWN product titles — the category vocabulary
    used to tell a relevant query (don't auto-negate) from an off-target one."""
    vocab: set = set()
    for s in sales_rows or []:
        for tok in re.findall(r"[a-z0-9]+", (s.title or "").lower()):
            if len(tok) >= 4 and tok not in _NEG_STOPWORDS:
                vocab.add(tok)
    return vocab


def _is_category_relevant(text: str, vocab: set) -> bool:
    """True when a search term shares a meaningful word with the brand's catalog —
    i.e. it's in your category, so it's a bid/relevance call, not a blanket negate."""
    if not vocab:
        return False
    toks = [t for t in re.findall(r"[a-z0-9]+", (text or "").lower())
            if len(t) >= 4 and t not in _NEG_STOPWORDS]
    return any(t in vocab for t in toks)


def _rule_wasted_spend_negatives(ad_rows: list[AdRow], thr: Thresholds,
                                 vocab: Optional[set] = None) -> list[Recommendation]:
    """Off-target search terms burning real spend with no orders -> negatives.

    Two guards keep this from killing good keywords:
      • Evidence bar — needs ≥negative_min_clicks AND ≥wasted_spend_cents with 0
        orders (not a few dollars of noise).
      • Relevance — a term in the brand's own category (shares catalog vocabulary)
        is NEVER auto-negated; it's surfaced for review (bid-down vs negate is a
        judgment call). Only clearly off-target terms become apply-ready negatives.
    """
    vocab = vocab or set()
    out: list[Recommendation] = []
    for r in ad_rows:
        if r.entity_level not in ("search_term", "keyword", "target"):
            continue
        if r.orders > 0 or r.clicks < thr.negative_min_clicks or r.spend_cents < thr.wasted_spend_cents:
            continue
        relevant = r.entity_level != "target" and _is_category_relevant(r.entity_text, vocab)
        bulk = (not relevant) and _bulk_ok(r.ad_type) and r.entity_level in ("search_term", "keyword")
        rec = Recommendation(
            category=CAT_NEGATIVE,
            ad_type=r.ad_type,
            severity=SEV_HIGH if (not relevant and r.spend_cents >= thr.wasted_spend_cents * 3) else SEV_MEDIUM,
            title=(
                f"Review '{r.entity_text}' — {fmt_money(r.spend_cents)} / {r.clicks} clicks, 0 orders"
                if relevant else
                f"Negate '{r.entity_text}' — {fmt_money(r.spend_cents)} spent, 0 orders"
            ),
            detail=(
                f"{r.clicks} clicks, {r.impressions} impressions, no conversions in '{r.campaign_name}'. "
                + ("Core category term — bid-down or negate by hand, not auto-applied."
                   if relevant else "Off-target query; a negative exact stops the bleed.")
            ),
            rationale=(
                "One of your category terms — flagged for review, not auto-negated; it may just need a lower bid."
                if relevant else
                "Spend with zero orders on an off-target query is pure waste; a negative exact stops the bleed."
            ),
            entity_ref=f"{r.campaign_name} › {r.ad_group_name} › {r.entity_text}",
            current_value=fmt_money(r.spend_cents),
            proposed_value=("review — bid-down or negate?" if relevant else "negative exact"),
            projected_impact={"spend_saved_cents": r.spend_cents},
            bulk_row={
                "action": "create_negative",
                "ad_type": r.ad_type,
                "campaign_id": r.campaign_id,
                "ad_group_id": r.ad_group_id,
                "campaign_name": r.campaign_name,
                "ad_group_name": r.ad_group_name,
                "keyword_text": r.entity_text,
                "match_type": "negative exact",
            },
            is_bulk_actionable=bulk,
        )
        rec.score = float(r.spend_cents)
        out.append(rec)
    return out


def _bid_row(r, new_bid):
    return {
        "action": "set_bid", "ad_type": r.ad_type,
        "campaign_id": r.campaign_id, "ad_group_id": r.ad_group_id,
        "keyword_id": r.keyword_id, "target_id": r.target_id,
        "targeting_expression": r.entity_text, "bulk_sheet": r.bulk_sheet,
        "campaign_name": r.campaign_name, "ad_group_name": r.ad_group_name,
        "keyword_text": r.entity_text, "match_type": r.match_type,
        "new_bid_cents": new_bid,
    }


def _rule_bid_to_target(ad_rows: list[AdRow], target_acos: int, thr: Thresholds) -> list[Recommendation]:
    """Move every converting keyword/target with enough data TOWARD the target
    ACoS, sized to the gap (not a flat nudge). Over target -> trim the bid to the
    break-even-for-target CPC; under target -> raise it toward that CPC, capped at
    a controlled step up so winners scale without overshooting. Skips anything
    already within the deadband of target (no churn). Direction sets the category."""
    out: list[Recommendation] = []
    for r in ad_rows:
        if r.entity_level not in ("keyword", "target"):
            continue
        if r.orders < 1 or r.clicks < thr.min_clicks_significant:
            continue
        ra = r.acos_bps
        base = r.bid_cents or r.cpc_cents
        target_cpc = _target_cpc_cents(r, target_acos)
        if ra is None or not base or target_cpc is None:
            continue
        if target_cpc >= base:
            # scaling a winner up — controlled step, never overshoot target_cpc.
            new_bid = _clamp_bid(min(target_cpc, round(base * thr.bid_up_max_multiple)), thr)
        else:
            # trimming an over-target keyword down to its target CPC.
            new_bid = _clamp_bid(target_cpc, thr)
        if base <= 0 or abs(new_bid - base) / base < thr.bid_change_deadband:
            continue  # already near target — leave it alone

        if new_bid < base:
            saved = _estimated_spend_delta(r, new_bid)
            rec = Recommendation(
                category=CAT_BID_DOWN, ad_type=r.ad_type,
                severity=SEV_HIGH if ra > target_acos * 2 else SEV_MEDIUM,
                title=f"Lower bid on '{r.entity_text}' — ACoS {fmt_pct(ra)} vs target {fmt_pct(target_acos)}",
                detail=f"{r.orders} orders / {r.clicks} clicks in '{r.campaign_name}'.",
                rationale="Bid above the break-even CPC for target ACoS; trim to target protects margin while keeping the keyword live.",
                entity_ref=f"{r.campaign_name} › {r.ad_group_name} › {r.entity_text}",
                current_value=fmt_money(r.bid_cents) if r.bid_cents else f"CPC {fmt_money(r.cpc_cents)}",
                proposed_value=fmt_money(new_bid),
                projected_impact={"spend_saved_cents": saved, "current_acos_bps": ra, "target_acos_bps": target_acos},
                bulk_row=_bid_row(r, new_bid), is_bulk_actionable=_bulk_ok(r.ad_type),
            )
            rec.score = float(max(saved, 0))
        else:
            extra_sales = round(r.sales_cents * (new_bid / base - 1))
            rec = Recommendation(
                category=CAT_BID_UP, ad_type=r.ad_type, severity=SEV_MEDIUM,
                title=f"Raise bid on '{r.entity_text}' — ACoS {fmt_pct(ra)} under target {fmt_pct(target_acos)}",
                detail=f"Efficient winner: {r.orders} orders at {fmt_money(r.sales_cents)} sales in '{r.campaign_name}'.",
                rationale="Profitable keyword under the ACoS target; raising the bid toward target CPC wins more impressions to scale revenue.",
                entity_ref=f"{r.campaign_name} › {r.ad_group_name} › {r.entity_text}",
                current_value=fmt_money(r.bid_cents) if r.bid_cents else f"CPC {fmt_money(r.cpc_cents)}",
                proposed_value=fmt_money(new_bid),
                projected_impact={"sales_upside_cents": extra_sales, "current_acos_bps": ra, "target_acos_bps": target_acos},
                bulk_row=_bid_row(r, new_bid), is_bulk_actionable=_bulk_ok(r.ad_type),
            )
            rec.score = float(max(extra_sales, 0)) * 0.5
        out.append(rec)
    return out


def _rule_harvest_keywords(ad_rows: list[AdRow], thr: Thresholds) -> list[Recommendation]:
    """Converting search terms not yet exact keywords -> promote to exact."""
    existing = {
        (r.ad_type, r.entity_text.strip().lower())
        for r in ad_rows
        if r.entity_level == "keyword"
    }
    # A search term that matched via EXACT is, by definition, already an exact
    # keyword — harvesting it would be rejected by Amazon as "already exists!".
    # The uploaded bulk often misses these (a partial/short-window export lists
    # only entities with activity that day), so the search-term report's own
    # match type is the more reliable existence signal. Treat them as existing.
    existing |= {
        (r.ad_type, r.entity_text.strip().lower())
        for r in ad_rows
        if r.entity_level == "search_term" and (r.match_type or "").strip().lower() == "exact"
    }
    out: list[Recommendation] = []
    for r in ad_rows:
        if r.entity_level != "search_term" or r.orders < thr.promote_keyword_min_orders:
            continue
        if (r.ad_type, r.entity_text.strip().lower()) in existing:
            continue
        rec = Recommendation(
            category=CAT_NEW_KEYWORD,
            ad_type=r.ad_type,
            severity=SEV_MEDIUM,
            title=f"Harvest '{r.entity_text}' as exact keyword — {r.orders} orders",
            detail=f"Converting search term ({fmt_money(r.sales_cents)} sales) not yet a managed keyword.",
            rationale="Promoting a proven search term to its own exact keyword gives bid control and isolates the winner.",
            entity_ref=f"{r.campaign_name} › {r.ad_group_name} › {r.entity_text}",
            current_value="auto/broad discovery",
            proposed_value="exact keyword",
            projected_impact={"orders": r.orders, "sales_cents": r.sales_cents},
            bulk_row={
                "action": "create_keyword",
                "ad_type": r.ad_type,
                "campaign_id": r.campaign_id,
                "ad_group_id": r.ad_group_id,
                "campaign_name": r.campaign_name,
                "ad_group_name": r.ad_group_name,
                "keyword_text": r.entity_text,
                "match_type": "exact",
                "new_bid_cents": (r.cpc_cents or thr.min_bid_cents),
            },
            is_bulk_actionable=_bulk_ok(r.ad_type),
        )
        rec.score = float(r.sales_cents) * 0.4
        out.append(rec)
    return out


def _rule_external_efficiency(
    external_rows: list[ExternalCostRow], sales_rows: list[SalesRow], goals: Goals
) -> list[Recommendation]:
    """Surface off-Amazon spend (Meta/TikTok/influencer) as blended-efficiency
    context. These are manual tasks — no Amazon bulk operation applies."""
    if not external_rows:
        return []
    total_external = sum(r.amount_cents for r in external_rows)
    if total_external <= 0:
        return []
    total_sales = sum(r.ordered_product_sales_cents for r in sales_rows)
    by_channel: dict[str, int] = {}
    for r in external_rows:
        by_channel[r.channel] = by_channel.get(r.channel, 0) + r.amount_cents
    breakdown = ", ".join(f"{ch}: {fmt_money(amt)}" for ch, amt in sorted(by_channel.items(), key=lambda x: -x[1]))
    blended = tacos_bps(total_external, total_sales) if total_sales else None
    rec = Recommendation(
        category=CAT_EXTERNAL,
        ad_type="",
        severity=SEV_LOW,
        title=f"External marketing spend {fmt_money(total_external)} — fold into blended TACoS",
        detail=f"Off-Amazon spend ({breakdown}) contributes {fmt_pct(blended)} of total sales on its own.",
        rationale="Scaling decisions should weigh total marketing cost, not Amazon ACoS alone; confirm these channels drive incremental Amazon demand.",
        entity_ref="External channels",
        current_value=fmt_money(total_external),
        proposed_value="review attribution",
        projected_impact={"external_spend_cents": total_external, "by_channel": by_channel},
        is_bulk_actionable=False,
    )
    rec.score = float(total_external) * 0.2
    return [rec]


def _rule_strategic_gap(
    ad_rows: list[AdRow],
    sales_rows: list[SalesRow],
    external_rows: list[ExternalCostRow],
    goals: Goals,
    thr: Thresholds,
) -> list[Recommendation]:
    """High-level gap-to-goal note + a manual nudge for ad types we can't bulk."""
    out: list[Recommendation] = []
    summary = compute_summary(ad_rows, sales_rows, external_rows, goals)
    gap = summary.get("gap", {})
    if gap.get("revenue_gap_cents", 0) and gap["revenue_gap_cents"] > 0:
        rec = Recommendation(
            category=CAT_MANUAL,
            severity=SEV_HIGH,
            title=f"Revenue {fmt_money(gap['revenue_gap_cents'])} short of {goals.period} goal",
            detail=(
                f"At {fmt_money(summary['total_sales_cents'])} vs {fmt_money(gap['revenue_target_cents'])} "
                f"target ({fmt_pct(gap.get('revenue_attainment_bps'))} attained). Blended TACoS "
                f"{fmt_pct(summary['blended_tacos_bps'])}."
            ),
            rationale="Closing the gap means scaling the under-target winners above and reallocating budget from wasted spend.",
            entity_ref="Account",
            current_value=fmt_money(summary["total_sales_cents"]),
            proposed_value=fmt_money(gap["revenue_target_cents"]),
            projected_impact=gap,
            is_bulk_actionable=False,
        )
        rec.score = float(gap["revenue_gap_cents"]) * 0.1
        out.append(rec)

    # Flag ad types present in the data that we can't emit bulk sheets for.
    manual_types = {r.ad_type for r in ad_rows if r.ad_type not in BULK_SUPPORTED and r.spend_cents > 0}
    for ad_type in sorted(manual_types):
        spend = sum(r.spend_cents for r in ad_rows if r.ad_type == ad_type)
        rec = Recommendation(
            category=CAT_MANUAL,
            ad_type=ad_type,
            severity=SEV_LOW,
            title=f"{ad_type}: {fmt_money(spend)} spend — bulk apply not supported, review manually",
            detail="Amazon does not expose a full bulk-operations sheet for this ad type; apply changes in console.",
            rationale="Recommendations for this ad type are listed but cannot be exported as a bulk file.",
            entity_ref=ad_type,
            is_bulk_actionable=False,
        )
        rec.score = float(spend) * 0.05
        out.append(rec)
    return out


# ---------------------------------------------------------------------------
# Bid math + ranking
# ---------------------------------------------------------------------------


def _target_cpc_cents(row: AdRow, target_acos_bps: int) -> Optional[int]:
    """Max CPC that holds this keyword at the target ACoS, given its observed
    revenue-per-click. target_cpc = revenue_per_click * target_acos."""
    if row.clicks <= 0 or row.sales_cents <= 0:
        return None
    revenue_per_click = row.sales_cents / row.clicks
    return round(revenue_per_click * target_acos_bps / 10000)


def _clamp_bid(cents: int, thr: Thresholds) -> int:
    return max(thr.min_bid_cents, min(thr.max_bid_cents, cents))


def _proposed_bid_down(row: AdRow, target_acos: int, thr: Thresholds) -> Optional[int]:
    base = row.bid_cents or row.cpc_cents
    if not base:
        return None
    target_cpc = _target_cpc_cents(row, target_acos)
    candidate = round(base * thr.bid_down_factor)
    if target_cpc is not None:
        candidate = min(candidate, target_cpc)
    new_bid = _clamp_bid(candidate, thr)
    return new_bid if new_bid < base else None


def _proposed_bid_up(row: AdRow, thr: Thresholds) -> Optional[int]:
    base = row.bid_cents or row.cpc_cents
    if not base:
        return None
    new_bid = _clamp_bid(round(base * thr.bid_up_factor), thr)
    return new_bid if new_bid > base else None


def _estimated_spend_delta(row: AdRow, new_bid_cents: int) -> int:
    """Rough realized-spend reduction if CPC moves to new_bid, holding clicks.
    Conservative: assumes clicks stay flat (they usually fall, saving more)."""
    base = row.bid_cents or row.cpc_cents
    if not base or row.clicks <= 0:
        return 0
    return max(0, round((base - new_bid_cents) * row.clicks))


_CATEGORY_PRIORITY = {
    CAT_NEGATIVE: 5,
    CAT_BID_DOWN: 4,
    CAT_NEW_KEYWORD: 3,
    CAT_BID_UP: 3,
    CAT_MANUAL: 2,
    CAT_EXTERNAL: 1,
}
_SEVERITY_WEIGHT = {SEV_HIGH: 3, SEV_MEDIUM: 2, SEV_LOW: 1}


def rank_recommendations(recs: list[Recommendation]) -> list[Recommendation]:
    """Sort by projected dollar impact, weighted by severity, then category
    priority as a tiebreak. Highest-leverage actions float to the top."""
    def sort_key(rec: Recommendation):
        return (
            rec.score * _SEVERITY_WEIGHT.get(rec.severity, 1),
            _CATEGORY_PRIORITY.get(rec.category, 0),
            rec.score,
        )

    return sorted(recs, key=sort_key, reverse=True)
