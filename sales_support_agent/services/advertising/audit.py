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
from sales_support_agent.services.advertising.bulk_sheets import BulkBuildResult, build_bulk_workbook
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
    # New-console Amazon Ads performance reports (search-term / advertised-product
    # / targeting / ad-group / campaign), each parsed by normalize_ads_report_csv.
    ads_report_csvs: list[bytes] = field(default_factory=list)

    def any_data(self) -> bool:
        return any([
            self.bulk_xlsx, self.search_term_csv, self.business_report_csv,
            self.sqp_csv, self.dsp_csv, self.external_costs_csv,
            self.external_costs_manual, self.ads_report_csvs,
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
    week_start: Optional[datetime] = None,
    week_end: Optional[datetime] = None,
) -> AuditResult:
    """Execute one audit run end to end. Never raises on bad input data — a
    malformed file yields fewer rows, not a crash. Hard failures are recorded on
    the run and returned with status='error'."""
    goals = goals or storage.get_active_goals() or Goals()

    # --- Parse ---
    ad_rows: list[AdRow] = []
    if inputs.bulk_xlsx:
        ad_rows += N.normalize_bulk_xlsx(inputs.bulk_xlsx)
    if inputs.search_term_csv:
        ad_rows += N.normalize_ads_report_csv(inputs.search_term_csv)
    for report_csv in inputs.ads_report_csvs:
        ad_rows += N.normalize_ads_report_csv(report_csv)
    if inputs.dsp_csv:
        ad_rows += N.normalize_dsp_csv(inputs.dsp_csv)

    sales_rows: list[SalesRow] = (
        N.normalize_business_report_csv(inputs.business_report_csv) if inputs.business_report_csv else []
    )
    market_rows: list[MarketRow] = (
        N.normalize_sqp_csv(inputs.sqp_csv) if inputs.sqp_csv else []
    )
    external_rows: list[ExternalCostRow] = list(inputs.external_costs_manual)
    if inputs.external_costs_csv:
        external_rows += N.normalize_external_costs_csv(inputs.external_costs_csv)

    run_id = storage.create_run(label=label, goals=goals, week_start=week_start, week_end=week_end)

    try:
        counts = storage.save_snapshots(run_id, ad_rows, sales_rows, market_rows)
        if external_rows:
            storage.save_external_costs(external_rows, run_id=run_id)
        counts["external"] = len(external_rows)

        summary = compute_summary(ad_rows, sales_rows, external_rows, goals)
        recs = build_recommendations(ad_rows, sales_rows, market_rows, external_rows, goals)
        storage.save_recommendations(run_id, recs)
        counts["recommendations"] = len(recs)

        # Round-trip the uploaded bulk workbook (covers all SP/SB/SD sheets it contains).
        bulk_result: Optional[BulkBuildResult] = None
        if inputs.bulk_xlsx:
            bulk_result = build_bulk_workbook(inputs.bulk_xlsx, recs)
            if bulk_result.has_file:
                storage.save_bulk_file(run_id, "combined", bulk_result.xlsx_bytes)

        prior = storage.get_prior_run(run_id)
        narrative = generate_narrative(
            summary, recs, goals, prior_summary=(prior or {}).get("summary") if prior else None
        ).text

        storage.finalize_run(run_id, status="complete", summary=summary, narrative=narrative)
        return AuditResult(
            run_id=run_id, status="complete", summary=summary, counts=counts,
            narrative=narrative, bulk=bulk_result,
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("[advertising] audit run failed")
        storage.finalize_run(run_id, status="error", error=str(exc))
        return AuditResult(run_id=run_id, status="error", error=str(exc))
