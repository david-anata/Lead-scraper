"""Persistence + read-back for Brand Analysis (global, shared history).

Mirrors advertising/storage.py: short-lived ORM Sessions on the shared global
engine for the row metadata + full report JSON, and durable base64 blobs in
kv_store for the uploaded source files and the generated .docx — so History,
re-open, and re-download survive Render's ephemeral filesystem across deploys.
"""

from __future__ import annotations

import base64
import logging
import re
import secrets
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


def _slugify(text: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "-", (text or "").lower()).strip("-")
    return (s[:60] or "brand")


def share_path(row_or_dict) -> str:
    """Public token-gated share URL path for a report row/dict, or '' if not
    publishable yet."""
    g = (lambda k: getattr(row_or_dict, k, None)) if not isinstance(row_or_dict, dict) else row_or_dict.get
    slug, rid, token = g("slug"), g("id"), g("share_token")
    if not (rid and token):
        return ""
    return f"/brand/{slug or 'brand'}/{rid}/{token}"


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
                docx_bytes: Optional[bytes] = None,
                report_html: str = "") -> str:
    """Persist a completed report and its artifacts; returns the report id.
    Mints the slug + share token for the public landing page."""
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
            slug=_slugify(report.brand),
            share_token=secrets.token_urlsafe(18),
            report_html=report_html or "",
            brand_website=report.brand_website[:512],
            context_notes=report.context_notes,
        ))
    if source_files:
        _save_sources(rid, source_files)
    if docx_bytes:
        save_docx(rid, docx_bytes)
    return rid


def update_report(report_id: str, report: BrandReport, *,
                  label: Optional[str] = None,
                  source_files: Optional[list[tuple[str, bytes]]] = None,
                  docx_bytes: Optional[bytes] = None,
                  report_html: str = "") -> bool:
    """Overwrite an existing report in place (edit + rerun). Keeps the same id,
    slug, and share token so a link already shared stays live. Snapshots the
    prior grade into version history first, so progression stays visible."""
    _snapshot_version(report_id)
    with _session() as s:
        row = s.get(ReportRow, report_id)
        if row is None:
            return False
        if label is not None:
            row.label = label or report.brand
        row.brand = report.brand[:255]
        row.category = report.category
        row.status = "complete"
        row.grade = report.scorecard.letter
        row.score_100 = report.scorecard.score_100
        row.confidence = report.confidence
        row.period_current = report.period_current_label[:64]
        row.period_prior = report.period_prior_label[:64]
        row.report_json = report.to_dict()
        row.brand_website = report.brand_website[:512]
        row.context_notes = report.context_notes
        if report_html:
            row.report_html = report_html
        if not row.slug:
            row.slug = _slugify(report.brand)
        if not row.share_token:
            row.share_token = secrets.token_urlsafe(18)
        row.updated_at = datetime.utcnow()
    if source_files:
        _save_sources(report_id, source_files)
    if docx_bytes:
        save_docx(report_id, docx_bytes)
    return True


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


def list_source_names(report_id: str) -> list[str]:
    """Filenames of the persisted source uploads (for the file manager)."""
    payload = kv_get_json(_sources_key(report_id), {}) or {}
    return list(payload.keys())


def _versions_key(report_id: str) -> str:
    return f"brand_analysis:versions:{report_id}"


def _snapshot_version(report_id: str) -> None:
    """Append the report's current grade/score to a capped version history,
    so a rerun (overwrite-in-place) doesn't erase how the grade evolved."""
    try:
        with _session() as s:
            row = s.get(ReportRow, report_id)
            if row is None or row.status != "complete":
                return
            snap = {"grade": row.grade, "score_100": row.score_100,
                    "confidence": row.confidence, "period_current": row.period_current,
                    "at": (row.updated_at.isoformat() if row.updated_at else None)}
        history = kv_get_json(_versions_key(report_id), []) or []
        history.append(snap)
        kv_set_json(_versions_key(report_id), history[-10:])  # keep last 10
    except Exception:  # noqa: BLE001 — history is a nicety, never block a rerun
        logger.exception("[brand_analysis] version snapshot failed")


def list_versions(report_id: str) -> list[dict]:
    return kv_get_json(_versions_key(report_id), []) or []


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


def get_report_row(report_id: str) -> Optional[dict]:
    """Row-level fields (slug, token, website, context, html) for the edit
    form prefill, the share link, and the public route."""
    with _session() as s:
        row = s.get(ReportRow, report_id)
        if not row:
            return None
        return {
            "id": row.id, "label": row.label, "brand": row.brand, "category": row.category,
            "status": row.status, "slug": row.slug, "share_token": row.share_token,
            "report_html": row.report_html, "brand_website": row.brand_website,
            "context_notes": row.context_notes,
        }


def get_share_html(report_id: str, token: str) -> Optional[str]:
    """Pre-rendered standalone HTML for the public route — only when the token
    matches. Returns None on mismatch/missing (the route 404s)."""
    with _session() as s:
        row = s.get(ReportRow, report_id)
        if not row or row.status != "complete":
            return None
        if not token or not row.share_token or token != row.share_token:
            return None
        return row.report_html or None


def set_share_html(report_id: str, report_html: str) -> None:
    with _session() as s:
        row = s.get(ReportRow, report_id)
        if row:
            row.report_html = report_html or ""


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
                "stage": r.stage,
                "grade": r.grade,
                "score_100": r.score_100,
                "confidence": r.confidence,
                "period_current": r.period_current,
                "period_prior": r.period_prior,
                "slug": r.slug,
                "share_token": r.share_token,
                "share_path": share_path(r),
                "created_at": r.created_at.isoformat() if r.created_at else None,
            }
            for r in rows
        ]


def list_pipeline_reports(limit: int = 200) -> list[dict]:
    """Full pipeline CRM list — includes report_json for financial columns."""
    with _session() as s:
        rows = (
            s.query(ReportRow)
            .order_by(ReportRow.updated_at.desc())
            .limit(limit)
            .all()
        )
        out = []
        for r in rows:
            rj: dict = r.report_json or {}
            current: dict = rj.get("current") or {}
            out.append({
                "id": r.id,
                "label": r.label,
                "brand": r.brand,
                "category": r.category,
                "status": r.status,
                "stage": r.stage,
                "grade": r.grade,
                "score_100": r.score_100,
                "confidence": r.confidence,
                "period_current": r.period_current,
                "period_prior": r.period_prior,
                "slug": r.slug,
                "share_token": r.share_token,
                "share_path": share_path(r),
                "updated_at": r.updated_at.isoformat() if r.updated_at else None,
                "created_at": r.created_at.isoformat() if r.created_at else None,
                # Financial columns extracted from report_json
                "recommendation": rj.get("recommendation") or "",
                "net_revenue_cents": current.get("net_revenue_cents"),
                "net_margin_bps": current.get("net_margin_bps"),
                "contribution_margin_bps": current.get("contribution_margin_bps"),
                "blended_mer": current.get("blended_mer"),
                "yoy_revenue_growth_bps": rj.get("yoy_revenue_growth_bps"),
                # Expand-panel data
                "scorecard_dimensions": (rj.get("scorecard") or {}).get("dimensions") or [],
                "investment_thesis": rj.get("investment_thesis") or [],
                "key_risks": rj.get("key_risks") or [],
                "red_flags": rj.get("red_flags") or [],
                # Social track
                "social_grade": (rj.get("brand_social") or {}).get("letter") or "",
                "social_score_100": (rj.get("brand_social") or {}).get("score_100") or 0,
                "social_confidence": (rj.get("brand_social") or {}).get("confidence") or "",
                "social_dimensions": (rj.get("brand_social") or {}).get("dimensions") or [],
                "email_list_size": rj.get("email_list_size") or 0,
                "social_handles": rj.get("social_handles") or {},
                "social_signals": rj.get("social_signals") or {},
                # Deal metadata
                "notes": getattr(r, "notes", "") or "",
                "ask_price_cents": getattr(r, "ask_price_cents", None),
                "contact_name": getattr(r, "contact_name", "") or "",
                "contact_email": getattr(r, "contact_email", "") or "",
            })
        return out


def set_stage(report_id: str, stage: str) -> bool:
    """Update the pipeline stage for a report. Returns False if not found."""
    from datetime import timezone
    with _session() as s:
        row = s.get(ReportRow, report_id)
        if row is None:
            return False
        row.stage = stage
        row.updated_at = datetime.now(timezone.utc)
        return True


def set_social_data(
    report_id: str,
    email_list_size: int,
    social_handles: dict,
    social_signals: dict,
) -> Optional[dict]:
    """Re-run the social opportunity track with updated inputs and persist.
    Returns the new brand_social dict (with grade/score/confidence), or None if
    the report doesn't exist.  Clears report_html so the share page is
    re-rendered on next view with the fresh social section."""
    from datetime import timezone
    with _session() as s:
        row = s.get(ReportRow, report_id)
        if row is None:
            return None
        rj: dict = dict(row.report_json or {})
        # Merge new inputs into report_json (keep existing fields untouched).
        rj["email_list_size"] = email_list_size
        rj["social_handles"] = social_handles
        rj["social_signals"] = social_signals
        # Recompute social track using the already-computed financial metrics.
        from sales_support_agent.services.brand_analysis.schema import (
            BrandReport, Metrics, PeriodFinancials,
        )
        from sales_support_agent.services.brand_analysis.social import build_brand_social
        try:
            report_obj = BrandReport.from_dict(rj)
            metrics = report_obj.current
            period = PeriodFinancials()
        except Exception:
            metrics = Metrics()
            period = PeriodFinancials()
        brand_social = build_brand_social(
            metrics, period,
            email_list_size=email_list_size,
            social_handles=social_handles,
            social_signals=social_signals,
        )
        rj["brand_social"] = brand_social
        row.report_json = rj
        row.report_html = ""  # force re-render on next share-page view
        row.updated_at = datetime.now(timezone.utc)
        return brand_social


def set_contact(report_id: str, name: str, email: str) -> bool:
    """Persist seller/broker contact details. Returns False if not found."""
    from datetime import timezone
    with _session() as s:
        row = s.get(ReportRow, report_id)
        if row is None:
            return False
        row.contact_name = name[:255]
        row.contact_email = email[:255]
        row.updated_at = datetime.now(timezone.utc)
        return True


def set_notes(report_id: str, notes: str) -> bool:
    """Persist analyst deal notes. Returns False if not found."""
    from datetime import timezone
    with _session() as s:
        row = s.get(ReportRow, report_id)
        if row is None:
            return False
        row.notes = notes
        row.updated_at = datetime.now(timezone.utc)
        return True


def set_ask_price(report_id: str, cents: Optional[int]) -> bool:
    """Persist proposed ask price in cents. Returns False if not found."""
    from datetime import timezone
    with _session() as s:
        row = s.get(ReportRow, report_id)
        if row is None:
            return False
        row.ask_price_cents = cents
        row.updated_at = datetime.now(timezone.utc)
        return True


def delete_report(report_id: str) -> bool:
    """Delete a report row and its kv_store artifacts. Returns False if not found."""
    with _session() as s:
        row = s.get(ReportRow, report_id)
        if row is None:
            return False
        s.delete(row)
    # Best-effort kv_store cleanup (don't crash if keys are absent).
    for suffix in ("sources", "docx", "versions"):
        try:
            kv_set_json(f"brand_analysis:{report_id}:{suffix}", None)
        except Exception:  # noqa: BLE001
            pass
    return True
