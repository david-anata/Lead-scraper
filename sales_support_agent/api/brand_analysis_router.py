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
)
from sales_support_agent.services.brand_analysis.share_page import render_share_page
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


def _run_and_render(batch, *, brand, category, context_notes, brand_website, prepared):
    """Build the report (with branding) and render its standalone share HTML."""
    assets = _fetch_brand_assets(brand_website)
    report = build_report(
        batch, brand=brand, category=category, prepared_date=prepared,
        context_notes=context_notes, brand_website=brand_website,
        logo_data_uri=assets.get("logo_data_uri", ""),
        brand_tagline=assets.get("tagline", ""),
        product_images=assets.get("product_images", []),
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


@router.post("/run")
async def run(
    files: list[UploadFile] = File(default=[]),
    brand: str = Form(default=""),
    category: str = Form(default=CATEGORY_DTC),
    label: str = Form(default=""),
    context_notes: str = Form(default=""),
    brand_website: str = Form(default=""),
) -> RedirectResponse:
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
            brand_website=brand_website, prepared=prepared)
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
    return HTMLResponse(render_edit_page(
        row, user=get_session_user_from_request(request),
        source_names=storage.list_source_names(report_id)))


@router.post("/{report_id}/rerun")
async def rerun(
    report_id: str,
    files: list[UploadFile] = File(default=[]),
    brand: str = Form(default=""),
    category: str = Form(default=CATEGORY_DTC),
    context_notes: str = Form(default=""),
    brand_website: str = Form(default=""),
    remove_files: list[str] = Form(default=[]),
) -> RedirectResponse:
    row = storage.get_report_row(report_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Report not found.")
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
            context_notes=context_notes, brand_website=brand_website, prepared=prepared)
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
