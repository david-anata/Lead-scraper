"""Persistence + read-back for the advertising audit.

Uses the shared global engine (database.get_engine()) via short-lived ORM
Sessions, mirroring how the cashflow service reaches the DB. Generated bulk
workbooks are written to a per-run directory on disk (downloaded right after a
run in the manual v1 flow); the DB holds the durable run/snapshot/rec records.
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from contextlib import contextmanager
from datetime import datetime
from typing import Iterable, Optional

from sqlalchemy.orm import Session

from sales_support_agent.models.database import get_engine
from sales_support_agent.models.entities import (
    AdGoal,
    AdSnapshot,
    AuditRun,
    ExternalCost,
    MarketSnapshot,
    Recommendation as RecommendationRow,
    SalesSnapshot,
)
from sales_support_agent.services.advertising.schema import (
    AdRow,
    ExternalCostRow,
    Goals,
    MarketRow,
    Recommendation,
    SalesRow,
)

logger = logging.getLogger(__name__)

# Where generated bulk workbooks land. Ephemeral on Render, which is fine for
# the manual upload→run→download flow; overridable via env for other hosts.
BULK_RUNS_DIR = os.environ.get(
    "ADVERTISING_RUNS_DIR",
    os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), "runtime", "advertising_runs"),
)


def _new_id() -> str:
    return str(uuid.uuid4())


@contextmanager
def _session():
    session = Session(get_engine(), expire_on_commit=False)
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Goals
# ---------------------------------------------------------------------------


def save_goals(goals: Goals, *, label: str = "") -> str:
    """Upsert the single active goal set. Deactivates prior active goals."""
    with _session() as s:
        for row in s.query(AdGoal).filter(AdGoal.is_active.is_(True)).all():
            row.is_active = False
        gid = _new_id()
        s.add(
            AdGoal(
                id=gid,
                label=label or goals.label,
                period=goals.period,
                revenue_target_cents=goals.revenue_target_cents,
                acos_target_bps=goals.acos_target_bps,
                tacos_target_bps=goals.tacos_target_bps,
                units_target=goals.units_target,
                is_active=True,
            )
        )
        return gid


def get_active_goals() -> Optional[Goals]:
    with _session() as s:
        row = (
            s.query(AdGoal)
            .filter(AdGoal.is_active.is_(True))
            .order_by(AdGoal.updated_at.desc())
            .first()
        )
        if not row:
            return None
        return Goals(
            revenue_target_cents=row.revenue_target_cents,
            acos_target_bps=row.acos_target_bps,
            tacos_target_bps=row.tacos_target_bps,
            units_target=row.units_target,
            period=row.period,
            label=row.label,
        )


# ---------------------------------------------------------------------------
# External costs
# ---------------------------------------------------------------------------


def save_external_costs(rows: Iterable[ExternalCostRow], *, run_id: Optional[str] = None) -> int:
    rows = list(rows)
    if not rows:
        return 0
    with _session() as s:
        for r in rows:
            s.add(
                ExternalCost(
                    id=_new_id(),
                    run_id=run_id,
                    channel=r.channel,
                    cost_type=r.cost_type,
                    label=r.label,
                    amount_cents=r.amount_cents,
                    note=r.note,
                )
            )
    return len(rows)


def get_external_costs(run_id: Optional[str] = None) -> list[ExternalCostRow]:
    with _session() as s:
        q = s.query(ExternalCost)
        if run_id is not None:
            q = q.filter(ExternalCost.run_id == run_id)
        return [
            ExternalCostRow(
                channel=row.channel,
                cost_type=row.cost_type,
                label=row.label,
                amount_cents=row.amount_cents,
                note=row.note,
            )
            for row in q.all()
        ]


# ---------------------------------------------------------------------------
# Audit runs + snapshots + recommendations
# ---------------------------------------------------------------------------


def create_run(*, label: str = "", goals: Optional[Goals] = None,
               week_start: Optional[datetime] = None, week_end: Optional[datetime] = None) -> str:
    rid = _new_id()
    with _session() as s:
        s.add(
            AuditRun(
                id=rid,
                label=label,
                week_start=week_start,
                week_end=week_end,
                status="draft",
                goal_snapshot_json=(goals.to_dict() if goals else {}),
            )
        )
    return rid


def save_snapshots(run_id: str, ad_rows: list[AdRow], sales_rows: list[SalesRow],
                   market_rows: Optional[list[MarketRow]] = None) -> dict:
    market_rows = market_rows or []
    with _session() as s:
        for r in ad_rows:
            s.add(AdSnapshot(
                id=_new_id(), run_id=run_id, ad_type=r.ad_type, entity_level=r.entity_level,
                campaign_name=r.campaign_name[:512], ad_group_name=r.ad_group_name[:512],
                entity_text=r.entity_text[:1024], match_type=r.match_type[:32],
                impressions=r.impressions, clicks=r.clicks, spend_cents=r.spend_cents,
                sales_cents=r.sales_cents, orders=r.orders, units=r.units, bid_cents=r.bid_cents,
                raw_json=r.raw,
            ))
        for r in sales_rows:
            s.add(SalesSnapshot(
                id=_new_id(), run_id=run_id, asin=r.asin[:32], sku=r.sku[:64], title=r.title[:512],
                sessions=r.sessions, page_views=r.page_views, units=r.units,
                ordered_product_sales_cents=r.ordered_product_sales_cents,
                buy_box_pct_bps=r.buy_box_pct_bps, conversion_bps=r.conversion_bps, raw_json=r.raw,
            ))
        for r in market_rows:
            s.add(MarketSnapshot(
                id=_new_id(), run_id=run_id, search_query=r.search_query[:512], asin=r.asin[:32],
                search_query_volume=r.search_query_volume, impressions_total=r.impressions_total,
                impression_share_bps=r.impression_share_bps, clicks_total=r.clicks_total,
                click_share_bps=r.click_share_bps, purchases_total=r.purchases_total,
                purchase_share_bps=r.purchase_share_bps, raw_json=r.raw,
            ))
    return {"ad": len(ad_rows), "sales": len(sales_rows), "market": len(market_rows)}


def save_recommendations(run_id: str, recs: list[Recommendation]) -> int:
    with _session() as s:
        for rank, rec in enumerate(recs, start=1):
            s.add(RecommendationRow(
                id=_new_id(), run_id=run_id, rank=rank, category=rec.category, ad_type=rec.ad_type,
                severity=rec.severity, title=rec.title[:512], detail=rec.detail, rationale=rec.rationale,
                entity_ref=rec.entity_ref[:1024], current_value=rec.current_value[:128],
                proposed_value=rec.proposed_value[:128], projected_impact_json=rec.projected_impact,
                bulk_row_json=rec.bulk_row, is_bulk_actionable=rec.is_bulk_actionable, status="open",
            ))
    return len(recs)


def finalize_run(run_id: str, *, status: str, summary: Optional[dict] = None,
                 narrative: str = "", error: str = "") -> None:
    with _session() as s:
        run = s.get(AuditRun, run_id)
        if not run:
            return
        run.status = status
        if summary is not None:
            run.summary_json = summary
        if narrative:
            run.narrative = narrative
        if error:
            run.error = error
        run.updated_at = datetime.utcnow()


def get_run(run_id: str) -> Optional[dict]:
    with _session() as s:
        run = s.get(AuditRun, run_id)
        if not run:
            return None
        return _run_to_dict(run)


def list_runs(limit: int = 25) -> list[dict]:
    with _session() as s:
        runs = s.query(AuditRun).order_by(AuditRun.created_at.desc()).limit(limit).all()
        return [_run_to_dict(r) for r in runs]


def get_recommendations(run_id: str) -> list[dict]:
    with _session() as s:
        rows = (
            s.query(RecommendationRow)
            .filter(RecommendationRow.run_id == run_id)
            .order_by(RecommendationRow.rank.asc())
            .all()
        )
        return [_rec_to_dict(r) for r in rows]


def get_prior_run(before_run_id: str) -> Optional[dict]:
    """The run created immediately before this one — for week-over-week deltas."""
    with _session() as s:
        cur = s.get(AuditRun, before_run_id)
        if not cur:
            return None
        prev = (
            s.query(AuditRun)
            .filter(AuditRun.created_at < cur.created_at, AuditRun.status == "complete")
            .order_by(AuditRun.created_at.desc())
            .first()
        )
        return _run_to_dict(prev) if prev else None


def _run_to_dict(run: AuditRun) -> dict:
    return {
        "id": run.id,
        "label": run.label,
        "status": run.status,
        "week_start": run.week_start.isoformat() if run.week_start else None,
        "week_end": run.week_end.isoformat() if run.week_end else None,
        "goal_snapshot": run.goal_snapshot_json or {},
        "summary": run.summary_json or {},
        "narrative": run.narrative or "",
        "error": run.error or "",
        "created_at": run.created_at.isoformat() if run.created_at else None,
    }


def _rec_to_dict(r: RecommendationRow) -> dict:
    return {
        "rank": r.rank, "category": r.category, "ad_type": r.ad_type, "severity": r.severity,
        "title": r.title, "detail": r.detail, "rationale": r.rationale, "entity_ref": r.entity_ref,
        "current_value": r.current_value, "proposed_value": r.proposed_value,
        "projected_impact": r.projected_impact_json or {}, "bulk_row": r.bulk_row_json or {},
        "is_bulk_actionable": r.is_bulk_actionable, "status": r.status,
    }


# ---------------------------------------------------------------------------
# Generated bulk-file storage (on disk)
# ---------------------------------------------------------------------------


def save_bulk_file(run_id: str, ad_type: str, xlsx_bytes: bytes) -> str:
    run_dir = os.path.join(BULK_RUNS_DIR, run_id)
    os.makedirs(run_dir, exist_ok=True)
    path = os.path.join(run_dir, f"{ad_type}_bulk.xlsx")
    with open(path, "wb") as fh:
        fh.write(xlsx_bytes)
    return path


def get_bulk_file(run_id: str, ad_type: str) -> Optional[bytes]:
    path = os.path.join(BULK_RUNS_DIR, run_id, f"{ad_type}_bulk.xlsx")
    if not os.path.exists(path):
        return None
    with open(path, "rb") as fh:
        return fh.read()


def list_bulk_files(run_id: str) -> list[str]:
    run_dir = os.path.join(BULK_RUNS_DIR, run_id)
    if not os.path.isdir(run_dir):
        return []
    return sorted(
        f.split("_bulk.xlsx")[0]
        for f in os.listdir(run_dir)
        if f.endswith("_bulk.xlsx")
    )
