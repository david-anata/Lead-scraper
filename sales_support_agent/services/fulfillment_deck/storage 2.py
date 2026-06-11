"""Persistence + read-back for Fulfillment Rate Sheets.

Mirrors brand_analysis/storage.py: short-lived ORM Sessions on the shared
global engine. Rate sheets ride on the existing `automation_runs` table
(run_type="fulfillment_rate_sheet") with the full result — including the
rendered HTML — in summary_json, exactly like sales decks. Engagement reuses
the deck tables (DeckVisitSession / DeckSectionView key only on run_id).
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from sales_support_agent.models.database import get_engine
from sales_support_agent.models.entities import (
    AutomationRun,
    DeckSectionView,
    DeckVisitSession,
)

logger = logging.getLogger(__name__)

RUN_TYPE = "fulfillment_rate_sheet"


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


def create_run(*, trigger: str, metadata: Optional[dict] = None) -> int:
    with _session() as s:
        run = AutomationRun(
            run_type=RUN_TYPE,
            status="running",
            trigger=trigger,
            started_at=datetime.now(timezone.utc),
            metadata_json=metadata or {},
            summary_json={},
        )
        s.add(run)
        s.flush()
        return int(run.id)


def complete_run(run_id: int, summary: dict) -> None:
    with _session() as s:
        run = s.get(AutomationRun, run_id)
        if run is None:
            return
        run.status = "completed"
        run.completed_at = datetime.now(timezone.utc)
        run.summary_json = summary
        s.add(run)


def fail_run(run_id: int, error: str) -> None:
    with _session() as s:
        run = s.get(AutomationRun, run_id)
        if run is None:
            return
        run.status = "failed"
        run.completed_at = datetime.now(timezone.utc)
        summary = dict(run.summary_json or {})
        summary["error"] = str(error)[:2000]
        run.summary_json = summary
        s.add(run)


def get_run(run_id: int) -> Optional[AutomationRun]:
    with _session() as s:
        run = s.execute(
            select(AutomationRun).where(
                AutomationRun.id == run_id,
                AutomationRun.run_type == RUN_TYPE,
            )
        ).scalar_one_or_none()
        return run


def list_runs(limit: int = 100) -> list[dict]:
    """Slim rows for the History table, newest first."""
    with _session() as s:
        rows = (
            s.execute(
                select(AutomationRun)
                .where(AutomationRun.run_type == RUN_TYPE)
                .order_by(AutomationRun.started_at.desc())
                .limit(limit)
            )
            .scalars()
            .all()
        )
        out: list[dict] = []
        for r in rows:
            summary = dict(r.summary_json or {})
            out.append(
                {
                    "id": int(r.id),
                    "status": r.status,
                    "started_at": r.started_at.isoformat() if r.started_at else None,
                    "design_title": str(summary.get("design_title") or ""),
                    "prospect": str(summary.get("prospect") or ""),
                    "origin_zip": str(summary.get("origin_zip") or ""),
                    "rates_source": str(summary.get("rates_source") or ""),
                    "sections_included": list(summary.get("sections_included") or []),
                    "view_path": str(summary.get("view_path") or ""),
                    "warnings": list(summary.get("warnings") or []),
                    "error": str(summary.get("error") or ""),
                }
            )
        return out


def delete_run(run_id: int) -> bool:
    """Delete a run plus its engagement rows. Returns True if a row existed."""
    with _session() as s:
        run = s.execute(
            select(AutomationRun).where(
                AutomationRun.id == run_id,
                AutomationRun.run_type == RUN_TYPE,
            )
        ).scalar_one_or_none()
        if run is None:
            return False
        session_rows = (
            s.execute(select(DeckVisitSession).where(DeckVisitSession.run_id == run_id))
            .scalars()
            .all()
        )
        for visit in session_rows:
            for sec in (
                s.execute(select(DeckSectionView).where(DeckSectionView.session_id == visit.id))
                .scalars()
                .all()
            ):
                s.delete(sec)
            s.delete(visit)
        s.delete(run)
        return True


def engagement_for(run_ids: list[int]) -> dict[int, dict]:
    """Per-run engagement rollup for the History table.

    Returns {run_id: {external_sessions, internal_sessions, total_seconds,
    max_scroll_pct, last_viewed_at}} — external-only for the time/scroll
    numbers so admin previews don't inflate prospect engagement.
    """
    if not run_ids:
        return {}
    with _session() as s:
        rows = (
            s.execute(select(DeckVisitSession).where(DeckVisitSession.run_id.in_(run_ids)))
            .scalars()
            .all()
        )
    out: dict[int, dict] = {}
    for row in rows:
        bucket = out.setdefault(
            int(row.run_id),
            {
                "external_sessions": 0,
                "internal_sessions": 0,
                "total_seconds": 0,
                "max_scroll_pct": 0,
                "last_viewed_at": None,
            },
        )
        if row.is_internal:
            bucket["internal_sessions"] += 1
        else:
            bucket["external_sessions"] += 1
            bucket["total_seconds"] += int(row.total_seconds or 0)
            bucket["max_scroll_pct"] = max(bucket["max_scroll_pct"], int(row.max_scroll_pct or 0))
        hb = row.last_heartbeat_at.isoformat() if row.last_heartbeat_at else None
        if hb and (bucket["last_viewed_at"] is None or hb > bucket["last_viewed_at"]):
            bucket["last_viewed_at"] = hb
    return out
