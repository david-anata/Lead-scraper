"""Audit orchestrator — the one entry point a request handler calls.

Ties the whole vertical together: parse the uploaded CSVs/XLSX, persist
snapshots, compute the summary, build + rank recommendations, round-trip the
bulk sheet, write the strategic narrative, and finalize the run. Pure-ish: all
IO goes through storage.py and the normalizers; safe to call from a route.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from sales_support_agent.services.advertising import normalizers as N
from sales_support_agent.services.advertising import storage
from sales_support_agent.services.advertising.brand import (
    detect_brand_candidates,
    detect_primary_brand,
    filter_by_brand,
    matches_brand,
    mixed_campaigns,
)
from sales_support_agent.services.advertising.bulk_sheets import (
    BulkBuildResult,
    build_apply_sheet,
    build_bulk_workbook,
)
from sales_support_agent.services.advertising.deliverable import build_growth_plan
from sales_support_agent.services.advertising.engine import build_recommendations, compute_summary
from sales_support_agent.services.advertising.llm import generate_narrative
from sales_support_agent.services.advertising.schema import (
    AdRow,
    ExternalCostRow,
    Goals,
    MarketRow,
    SalesRow,
)

logger = logging.getLogger(__name__)


@dataclass
class AuditInputs:
    bulk_xlsx: Optional[bytes] = None
    search_term_csv: Optional[bytes] = None
    business_report_csv: Optional[bytes] = None
    sqp_csv: Optional[bytes] = None
    dsp_csv: Optional[bytes] = None
    external_costs_csv: Optional[bytes] = None
    external_costs_manual: list[ExternalCostRow] = field(default_factory=list)
    cogs_csv: Optional[bytes] = None
    # New-console Amazon Ads performance reports (search-term / advertised-product
    # / targeting / ad-group / campaign), each parsed by normalize_ads_report_csv.
    ads_report_csvs: list[bytes] = field(default_factory=list)

    def any_data(self) -> bool:
        return any([
            self.bulk_xlsx, self.search_term_csv, self.business_report_csv,
            self.sqp_csv, self.dsp_csv, self.external_costs_csv,
            self.external_costs_manual, self.ads_report_csvs, self.cogs_csv,
        ])


@dataclass
class AuditResult:
    run_id: str
    status: str
    summary: dict = field(default_factory=dict)
    counts: dict = field(default_factory=dict)
    narrative: str = ""
    bulk: Optional[BulkBuildResult] = None
    error: str = ""


def run_audit(
    inputs: AuditInputs,
    *,
    goals: Optional[Goals] = None,
    label: str = "",
    brand: str = "",
    week_start: Optional[datetime] = None,
    week_end: Optional[datetime] = None,
) -> AuditResult:
    """Execute one audit run end to end. Never raises on bad input data — a
    malformed file yields fewer rows, not a crash. Hard failures are recorded on
    the run and returned with status='error'."""
    goals = goals or storage.get_active_goals() or Goals()
    brand = (brand or "").strip()
    run_label = f"{brand} — {label}".strip(" —") if brand else label
    run_id = storage.create_run(label=run_label, goals=goals, week_start=week_start, week_end=week_end)

    try:
        # --- Parse (inside the try so any malformed file fails gracefully) ---
        # Business Report first — its ASINs define brand scope for the bulk file.
        sales_rows: list[SalesRow] = (
            N.normalize_business_report_csv(inputs.business_report_csv) if inputs.business_report_csv else []
        )

        ad_rows: list[AdRow] = []
        if inputs.search_term_csv:
            ad_rows += N.normalize_ads_report_csv(inputs.search_term_csv)
        for report_csv in inputs.ads_report_csvs:
            ad_rows += N.normalize_ads_report_csv(report_csv)
        if inputs.dsp_csv:
            ad_rows += N.normalize_dsp_csv(inputs.dsp_csv)

        # Bulk Operations file → existing keyword rows (Keyword ID + bid + perf),
        # scoped to brand-only campaigns. Enables bid-change apply rows.
        if inputs.bulk_xlsx:
            all_asins = {s.asin.upper() for s in sales_rows if s.asin}
            b_asins = (
                {s.asin.upper() for s in sales_rows if s.asin and matches_brand(brand, s.title, s.sku, s.asin)}
                if brand else all_asins
            )
            ad_rows += N.normalize_bulk_keywords(inputs.bulk_xlsx, b_asins, all_asins - b_asins)
            # Sponsored Brands: the bulk file is the only source of SB keyword /
            # product-targeting performance (its reports are campaign-level), so
            # SB bid changes come from here. Same cross-brand ASIN scoping.
            ad_rows += N.normalize_bulk_sb(inputs.bulk_xlsx, b_asins, all_asins - b_asins)
            # Performance reports (esp. legacy .xlsx exports) carry no entity IDs,
            # so most harvests/negatives/bid changes couldn't be written to the
            # apply sheet. The bulk file holds every name→ID — backfill them so the
            # apply sheet reflects the full burn list, not just the handful of rows
            # that came straight off the bulk file.
            backfilled = N.backfill_entity_ids(ad_rows, N.bulk_name_id_map(inputs.bulk_xlsx))
            if backfilled:
                logger.info("[advertising] backfilled entity IDs on %d report rows from the bulk file", backfilled)

        market_rows: list[MarketRow] = (
            N.normalize_sqp_csv(inputs.sqp_csv) if inputs.sqp_csv else []
        )
        external_rows: list[ExternalCostRow] = list(inputs.external_costs_manual)
        if inputs.external_costs_csv:
            external_rows += N.normalize_external_costs_csv(inputs.external_costs_csv)

        # COGS is standing reference data — merge any new upload, then load the full map.
        if inputs.cogs_csv:
            parsed = N.normalize_cogs_csv(inputs.cogs_csv, sales_rows=sales_rows)
            storage.save_cogs(parsed.get("asin", {}), parsed.get("sku", {}), parsed.get("source", {}))
        cogs = storage.get_cogs()

        # Brand focus: detect candidates from the full account, then scope the audit.
        brand_candidates = detect_brand_candidates(ad_rows, sales_rows)
        excluded_mixed = mixed_campaigns(ad_rows, sales_rows, brand) if brand else set()
        if brand:
            ad_rows, sales_rows = filter_by_brand(ad_rows, sales_rows, brand)

        counts = storage.save_snapshots(run_id, ad_rows, sales_rows, market_rows)
        if external_rows:
            storage.save_external_costs(external_rows, run_id=run_id)
        counts["external"] = len(external_rows)

        # Data-sanity: warn when uploaded ads reports cover different date windows
        # (the dashboard will auto-sync these via the API; the manual flow can't).
        windows = set()
        for csv_bytes in ([inputs.search_term_csv] if inputs.search_term_csv else []) + list(inputs.ads_report_csvs):
            rng = N.report_date_range(csv_bytes)
            if rng:
                windows.add(rng)

        summary = compute_summary(ad_rows, sales_rows, external_rows, goals)
        summary["brand"] = brand
        summary["brand_candidates"] = brand_candidates
        # For an un-scoped run, surface the account's common brand name (e.g.
        # "Number 4") so the page/history shows it instead of "Full account".
        summary["detected_brand"] = "" if brand else detect_primary_brand(sales_rows, ad_rows)
        summary["excluded_mixed_campaigns"] = len(excluded_mixed)
        summary["brand_asin_count"] = len([s for s in sales_rows if s.asin]) if brand else 0
        summary["data_windows"] = sorted(windows)
        recs = build_recommendations(ad_rows, sales_rows, market_rows, external_rows, goals)
        # Harvests discovered in Sponsored Brands (or otherwise unresolved)
        # campaigns are routed into the Sponsored Products campaign that
        # advertises the same ASIN, so they become apply-ready instead of dropped.
        if inputs.bulk_xlsx:
            redirected = N.redirect_harvests_to_sp(recs, N.bulk_sp_home_by_asin(inputs.bulk_xlsx))
            if redirected:
                logger.info("[advertising] routed %d cross-channel harvest(s) into their SP home", redirected)
            # Keep keyword harvests in keyword ad groups and ASIN harvests in
            # product-targeting ad groups (never an auto ad group) — Amazon rejects
            # a mixed-targeting or auto ad group.
            retyped = N.enforce_targeting_type(recs, inputs.bulk_xlsx)
            if retyped:
                logger.info("[advertising] re-homed/dropped %d harvest(s) to match ad-group targeting type", retyped)
        storage.save_recommendations(run_id, recs)
        counts["recommendations"] = len(recs)
        summary["recommendation_count"] = len(recs)

        # Apply-sheet: populate Amazon's official template directly from the
        # Campaign/Ad Group IDs carried on the reports — upload-ready, no manual
        # editing. Falls back to round-tripping an uploaded bulk workbook (which
        # also enables bid updates) only if the template path yields nothing.
        bulk_result: Optional[BulkBuildResult] = build_apply_sheet(recs)
        if bulk_result.has_file:
            storage.save_bulk_file(run_id, "combined", bulk_result.xlsx_bytes)
        elif inputs.bulk_xlsx:
            bulk_result = build_bulk_workbook(inputs.bulk_xlsx, recs)
            if bulk_result.has_file:
                storage.save_bulk_file(run_id, "combined", bulk_result.xlsx_bytes)

        prior = storage.get_prior_run(run_id)
        narrative = generate_narrative(
            summary, recs, goals, prior_summary=(prior or {}).get("summary") if prior else None
        ).text

        # Strategic deliverable — the multi-tab growth-plan workbook.
        try:
            plan_bytes = build_growth_plan(
                brand=brand, summary=summary, recommendations=recs,
                ad_rows=ad_rows, sales_rows=sales_rows, goals=goals, narrative=narrative,
                cogs=cogs, has_cogs=bool(cogs.get("asin") or cogs.get("sku")),
            )
            storage.save_bulk_file(run_id, "growth_plan", plan_bytes)
        except Exception:  # noqa: BLE001
            logger.exception("[advertising] growth-plan generation failed (non-fatal)")

        storage.finalize_run(run_id, status="complete", summary=summary, narrative=narrative)
        return AuditResult(
            run_id=run_id, status="complete", summary=summary, counts=counts,
            narrative=narrative, bulk=bulk_result,
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("[advertising] audit run failed")
        storage.finalize_run(run_id, status="error", error=str(exc))
        return AuditResult(run_id=run_id, status="error", error=str(exc))
