"""Executive > Brand Analysis controller — /admin/executive/brand-analysis.

Self-contained backend router (same pattern as advertising_router): renders its
own HTML pages, handles the multi-file financial upload, runs the deterministic
analysis + LLM classifier/narrative, renders a branded investor landing page,
persists each run to a global History with a token-gated public share link, and
supports edit + rerun (overwrite in place) plus the .docx export.

The public landing page is served by ``public_router`` at /brand/{slug}/{id}/
{token} — outside the /admin prefix, so it bypasses the RBAC middleware and
needs no session (the unguessable token is the gate).
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import quote_plus

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from sales_support_agent.services.auth_deps import (
    get_session_user_from_request,
    require_tool,
)
from sales_support_agent.services.brand_analysis import storage
from sales_support_agent.services.brand_analysis.docx_export import build_docx
from sales_support_agent.services.brand_analysis.report import build_report
from sales_support_agent.services.brand_analysis.report_page import (
    render_admin_view,
    render_brand_analysis_page,
    render_edit_page,
    render_pipeline_page,
    _STAGE_META,
)
from sales_support_agent.services.brand_analysis.share_page import render_share_page
from sales_support_agent.services.brand_analysis.intake_guide_page import render_intake_guide
from sales_support_agent.services.brand_analysis.schema import CATEGORY_DTC

logger = logging.getLogger(__name__)

_DOCX_MEDIA = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
_BASE = "/admin/executive/brand-analysis"


router = APIRouter(
    prefix=_BASE,
    tags=["brand-analysis"],
    dependencies=[Depends(require_tool("executive.brand_analysis"))],
)

# Public, token-gated landing page — no auth (mounted at root, off /admin).
public_router = APIRouter(tags=["brand-analysis-public"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fetch_brand_assets(website: str) -> dict:
    """Best-effort logo + tagline + product imagery from the brand's site.
    Reuses the fulfillment deck's extractor; never raises."""
    if not website:
        return {}
    try:
        from sales_support_agent.services.fulfillment_deck.intake import fetch_brand_assets
        return fetch_brand_assets(website) or {}
    except Exception:  # noqa: BLE001 — branding is a nice-to-have, never block a run
        logger.warning("[brand_analysis] brand asset fetch failed for %s", website[:80], exc_info=True)
        return {}


async def _collect_files(files) -> list[tuple[str, bytes]]:
    batch: list[tuple[str, bytes]] = []
    for f in files or []:
        if f is not None and f.filename:
            data = await f.read()
            if data:
                batch.append((f.filename, data))
    return batch


def _parse_social_urls(text: str) -> dict:
    """Manual social URLs (one per line / space-separated) -> {platform: url}."""
    from sales_support_agent.services.brand_analysis.social import _SOCIAL_PATTERNS
    out: dict = {}
    for tok in re.split(r"[\s,]+", text or ""):
        tok = tok.strip()
        if not tok:
            continue
        for platform, pat in _SOCIAL_PATTERNS.items():
            if pat.match(tok if tok.startswith("http") else "https://" + tok):
                out.setdefault(platform, tok if tok.startswith("http") else "https://" + tok)
                break
    return out


_OVERRIDE_FORM_MAP = {
    "override_net_revenue": "net_revenue_cents",
    "override_cogs": "cogs_cents",
    "override_marketing_total": "marketing_total_cents",
    "override_reported_gross_profit": "reported_gross_profit_cents",
    "override_opex": "opex_cents",
    "override_net_earnings": "net_earnings_cents",
}


def _collect_overrides(form_values: dict) -> dict:
    out: dict = {}
    for form_key, field_name in _OVERRIDE_FORM_MAP.items():
        v = str(form_values.get(form_key) or "").strip()
        if v:
            out[field_name] = v
    return out


def _run_and_render(batch, *, brand, category, context_notes, brand_website, prepared,
                    email_list_size=0, social_urls="", review_rating=None, review_count=None,
                    overrides=None):
    """Build the report (with branding + social + overrides) and render share HTML."""
    assets = _fetch_brand_assets(brand_website)
    # Auto-discover socials from the site, then let manual URLs override.
    from sales_support_agent.services.brand_analysis.social import discover_socials
    handles = discover_socials(brand_website)
    handles.update(_parse_social_urls(social_urls))
    signals: dict = {}
    if review_rating is not None:
        signals["review_rating"] = review_rating
    if review_count is not None:
        signals["review_count"] = review_count
    report = build_report(
        batch, brand=brand, category=category, prepared_date=prepared,
        context_notes=context_notes, brand_website=brand_website,
        logo_data_uri=assets.get("logo_data_uri", ""),
        brand_tagline=assets.get("tagline", ""),
        product_images=assets.get("product_images", []),
        email_list_size=email_list_size, social_handles=handles, social_signals=signals,
        overrides=overrides or {},
    )
    share_html = render_share_page(report)
    return report, share_html


# ---------------------------------------------------------------------------
# Landing + run
# ---------------------------------------------------------------------------


@router.get("", response_class=HTMLResponse)
def landing(request: Request, msg: str = "", detail: str = "") -> HTMLResponse:
    runs = storage.list_reports()
    return HTMLResponse(render_brand_analysis_page(
        runs=runs, user=get_session_user_from_request(request), flash=msg, detail=detail,
    ))


@router.get("/pipeline", response_class=HTMLResponse)
def pipeline(request: Request) -> HTMLResponse:
    runs = storage.list_pipeline_reports()
    return HTMLResponse(render_pipeline_page(runs, user=get_session_user_from_request(request)))


def _opt_int(v) -> Optional[int]:
    try:
        s = str(v or "").replace(",", "").strip()
        return int(float(s)) if s else None
    except (TypeError, ValueError):
        return None


def _opt_float(v) -> Optional[float]:
    try:
        s = str(v or "").strip()
        return float(s) if s else None
    except (TypeError, ValueError):
        return None


@router.post("/run")
async def run(
    request: Request,
    files: list[UploadFile] = File(default=[]),
    brand: str = Form(default=""),
    category: str = Form(default=CATEGORY_DTC),
    label: str = Form(default=""),
    context_notes: str = Form(default=""),
    brand_website: str = Form(default=""),
    email_list_size: str = Form(default=""),
    social_urls: str = Form(default=""),
    review_rating: str = Form(default=""),
    review_count: str = Form(default=""),
) -> RedirectResponse:
    overrides = _collect_overrides(await request.form())
    batch = await _collect_files(files)
    if not batch:
        return RedirectResponse(
            f"{_BASE}?msg=" + quote_plus("Upload at least one financial file to run an analysis."),
            status_code=303,
        )
    prepared = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        report, share_html = _run_and_render(
            batch, brand=brand, category=category, context_notes=context_notes,
            brand_website=brand_website, prepared=prepared,
            email_list_size=_opt_int(email_list_size) or 0, social_urls=social_urls,
            review_rating=_opt_float(review_rating), review_count=_opt_int(review_count),
            overrides=overrides)
        docx_bytes = build_docx(report)
        report_id = storage.save_report(
            report, label=label, source_files=batch, docx_bytes=docx_bytes, report_html=share_html)
    except Exception as exc:  # noqa: BLE001
        logger.exception("[brand_analysis] run failed")
        storage.save_error(brand or "Brand", str(exc), label=label)
        return RedirectResponse(
            f"{_BASE}?msg=" + quote_plus(f"Analysis failed: {str(exc)[:120]}"), status_code=303)
    return RedirectResponse(f"{_BASE}/{report_id}", status_code=303)


# ---------------------------------------------------------------------------
# Edit + rerun (overwrite in place — keeps the same share link)
# ---------------------------------------------------------------------------


@router.get("/{report_id}/edit", response_class=HTMLResponse)
def edit(request: Request, report_id: str) -> HTMLResponse:
    row = storage.get_report_row(report_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Report not found.")
    # Social inputs live in the report JSON (no row column) — prefill from there.
    rep = storage.get_report(report_id)
    if rep is not None:
        row = {**row,
               "email_list_size": rep.email_list_size or "",
               "social_urls": " ".join((rep.social_handles or {}).values()),
               "review_rating": (rep.social_signals or {}).get("review_rating") or "",
               "review_count": (rep.social_signals or {}).get("review_count") or ""}
        ov = rep.overrides or {}
        for form_key, field_name in _OVERRIDE_FORM_MAP.items():
            row[form_key] = ov.get(field_name, "")
    return HTMLResponse(render_edit_page(
        row, user=get_session_user_from_request(request),
        source_names=storage.list_source_names(report_id),
        versions=storage.list_versions(report_id)))


@router.post("/{report_id}/rerun")
async def rerun(
    request: Request,
    report_id: str,
    files: list[UploadFile] = File(default=[]),
    brand: str = Form(default=""),
    category: str = Form(default=CATEGORY_DTC),
    context_notes: str = Form(default=""),
    brand_website: str = Form(default=""),
    remove_files: list[str] = Form(default=[]),
    email_list_size: str = Form(default=""),
    social_urls: str = Form(default=""),
    review_rating: str = Form(default=""),
    review_count: str = Form(default=""),
) -> RedirectResponse:
    row = storage.get_report_row(report_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Report not found.")
    overrides = _collect_overrides(await request.form())
    # Kept originals (minus any the analyst removed) + newly added files.
    drop = {n for n in (remove_files or [])}
    kept = [(n, d) for (n, d) in storage.get_sources(report_id) if n not in drop]
    batch = kept + await _collect_files(files)
    if not batch:
        return RedirectResponse(
            f"{_BASE}/{report_id}/edit?msg=" + quote_plus("No files to analyse."),
            status_code=303)
    prepared = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        report, share_html = _run_and_render(
            batch, brand=brand or row.get("brand", ""), category=category,
            context_notes=context_notes, brand_website=brand_website, prepared=prepared,
            email_list_size=_opt_int(email_list_size) or 0, social_urls=social_urls,
            review_rating=_opt_float(review_rating), review_count=_opt_int(review_count),
            overrides=overrides)
        docx_bytes = build_docx(report)
        storage.update_report(
            report_id, report, source_files=batch, docx_bytes=docx_bytes, report_html=share_html)
    except Exception as exc:  # noqa: BLE001
        logger.exception("[brand_analysis] rerun failed")
        return RedirectResponse(
            f"{_BASE}/{report_id}/edit?msg=" + quote_plus(f"Rerun failed: {str(exc)[:120]}"),
            status_code=303)
    return RedirectResponse(f"{_BASE}/{report_id}", status_code=303)


# ---------------------------------------------------------------------------
# Pipeline PATCH/DELETE helpers
# ---------------------------------------------------------------------------

from pydantic import BaseModel as _BaseModel


class _StageBody(_BaseModel):
    stage: str


@router.patch("/{report_id}/stage")
def update_stage(report_id: str, body: _StageBody) -> dict:
    if body.stage not in _STAGE_META:
        raise HTTPException(status_code=422, detail=f"Unknown stage '{body.stage}'.")
    ok = storage.set_stage(report_id, body.stage)
    if not ok:
        raise HTTPException(status_code=404, detail="Report not found.")
    return {"ok": True}


@router.delete("/{report_id}")
def delete_report(report_id: str) -> dict:
    ok = storage.delete_report(report_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Report not found.")
    return {"ok": True}


# ---------------------------------------------------------------------------
# View (admin) + download
# ---------------------------------------------------------------------------


@router.get("/{report_id}", response_class=HTMLResponse)
def view(request: Request, report_id: str) -> HTMLResponse:
    report = storage.get_report(report_id)
    row = storage.get_report_row(report_id)
    if report is None or row is None:
        raise HTTPException(status_code=404, detail="Report not found.")
    share_html = row.get("report_html") or ""
    if not share_html:
        # Legacy rows (pre-redesign) have no pre-rendered HTML — render + cache now.
        share_html = render_share_page(report)
        storage.set_share_html(report_id, share_html)
    return HTMLResponse(render_admin_view(
        report, report_id=report_id, share_html=share_html,
        share_path=storage.share_path(row), user=get_session_user_from_request(request)))


@router.get("/{report_id}/download")
def download(report_id: str) -> Response:
    data = storage.get_docx(report_id)
    if not data:
        report = storage.get_report(report_id)
        if report is None:
            raise HTTPException(status_code=404, detail="Report not found.")
        data = build_docx(report)
        storage.save_docx(report_id, data)
    report = storage.get_report(report_id)
    brand_slug = "".join(ch for ch in (report.brand if report else "brand") if ch.isalnum() or ch in "-_") or "brand"
    filename = f"brand-analysis-{brand_slug}-{report_id[:8]}.docx"
    return Response(
        content=data, media_type=_DOCX_MEDIA,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ---------------------------------------------------------------------------
# Public token-gated landing page
# ---------------------------------------------------------------------------


@public_router.get("/brand/{slug}/{report_id}/{token}", response_class=HTMLResponse)
def public_brand_page(slug: str, report_id: str, token: str) -> HTMLResponse:
    html = storage.get_share_html(report_id, token)
    if html is None:
        # Render on demand if the cache is empty but the token is valid.
        row = storage.get_report_row(report_id)
        if row and row.get("share_token") and token == row["share_token"]:
            report = storage.get_report(report_id)
            if report is not None:
                html = render_share_page(report)
                storage.set_share_html(report_id, html)
    if not html:
        return HTMLResponse("Brief not found.", status_code=404)
    return HTMLResponse(html)


@public_router.get("/brand-intake", response_class=HTMLResponse)
def public_intake_guide(print: bool = False) -> HTMLResponse:
    return HTMLResponse(render_intake_guide(print_mode=print))
