"""Persistence + read-back for Brand Analysis (global, shared history).

Mirrors advertising/storage.py: short-lived ORM Sessions on the shared global
engine for the row metadata + full report JSON, and durable base64 blobs in
kv_store for the uploaded source files and the generated .docx — so History,
re-open, and re-download survive Render's ephemeral filesystem across deploys.
"""

from __future__ import annotations

import base64
import logging
import uuid
from contextlib import contextmanager
from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session

from sales_support_agent.models.database import get_engine, kv_get_json, kv_set_json
from sales_support_agent.models.entities import BrandAnalysisReport as ReportRow
from sales_support_agent.services.brand_analysis.schema import BrandReport

logger = logging.getLogger(__name__)


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


def _sources_key(report_id: str) -> str:
    return f"brand_analysis:sources:{report_id}"


def _docx_key(report_id: str) -> str:
    return f"brand_analysis:docx:{report_id}"


# ---------------------------------------------------------------------------
# Save / finalize
# ---------------------------------------------------------------------------


def save_report(report: BrandReport, *, label: str = "",
                source_files: Optional[list[tuple[str, bytes]]] = None,
                docx_bytes: Optional[bytes] = None) -> str:
    """Persist a completed report and its artifacts; returns the report id."""
    rid = _new_id()
    with _session() as s:
        s.add(ReportRow(
            id=rid,
            label=label or report.brand,
            brand=report.brand[:255],
            category=report.category,
            status="complete",
            grade=report.scorecard.letter,
            score_100=report.scorecard.score_100,
            confidence=report.confidence,
            period_current=report.period_current_label[:64],
            period_prior=report.period_prior_label[:64],
            report_json=report.to_dict(),
        ))
    if source_files:
        _save_sources(rid, source_files)
    if docx_bytes:
        save_docx(rid, docx_bytes)
    return rid


def save_error(brand: str, error: str, *, label: str = "") -> str:
    rid = _new_id()
    with _session() as s:
        s.add(ReportRow(id=rid, label=label or brand, brand=brand[:255], status="error", error=error[:2000]))
    return rid


def _save_sources(report_id: str, files: list[tuple[str, bytes]]) -> None:
    try:
        payload = {name: base64.b64encode(data).decode("ascii") for name, data in files if data}
        kv_set_json(_sources_key(report_id), payload)
    except Exception:  # noqa: BLE001
        logger.exception("[brand_analysis] failed to persist source files to kv_store")


def get_sources(report_id: str) -> list[tuple[str, bytes]]:
    payload = kv_get_json(_sources_key(report_id), {}) or {}
    out: list[tuple[str, bytes]] = []
    for name, b64 in payload.items():
        try:
            out.append((name, base64.b64decode(b64)))
        except Exception:  # noqa: BLE001
            continue
    return out


def save_docx(report_id: str, docx_bytes: bytes) -> None:
    try:
        kv_set_json(_docx_key(report_id), {"docx": base64.b64encode(docx_bytes).decode("ascii")})
    except Exception:  # noqa: BLE001
        logger.exception("[brand_analysis] failed to persist docx to kv_store")


def get_docx(report_id: str) -> Optional[bytes]:
    data = (kv_get_json(_docx_key(report_id), {}) or {}).get("docx")
    if not data:
        return None
    try:
        return base64.b64decode(data)
    except Exception:  # noqa: BLE001
        return None


# ---------------------------------------------------------------------------
# Read-back
# ---------------------------------------------------------------------------


def get_report(report_id: str) -> Optional[BrandReport]:
    with _session() as s:
        row = s.get(ReportRow, report_id)
        if not row or not row.report_json:
            return None
        return BrandReport.from_dict(row.report_json)


def list_reports(limit: int = 50) -> list[dict]:
    """Slim list for History: brand, date, grade, confidence."""
    with _session() as s:
        rows = s.query(ReportRow).order_by(ReportRow.created_at.desc()).limit(limit).all()
        return [
            {
                "id": r.id,
                "label": r.label,
                "brand": r.brand,
                "category": r.category,
                "status": r.status,
                "grade": r.grade,
                "score_100": r.score_100,
                "confidence": r.confidence,
                "period_current": r.period_current,
                "period_prior": r.period_prior,
                "created_at": r.created_at.isoformat() if r.created_at else None,
            }
            for r in rows
        ]
