"""Executive > Brand Analysis controller — routes under /admin/executive/brand-analysis.

Self-contained backend router (same pattern as advertising_router): renders its
own HTML pages, handles the multi-file financial upload, runs the deterministic
analysis + LLM narrative, persists each run to a global History, and serves the
.docx export. Mounted in-process by the frontend via include_router.
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
    is_authenticated,
)
from sales_support_agent.services.brand_analysis import storage
from sales_support_agent.services.brand_analysis.docx_export import build_docx
from sales_support_agent.services.brand_analysis.report import build_report
from sales_support_agent.services.brand_analysis.report_page import (
    render_brand_analysis_page,
    render_history_page,
    render_report,
)
from sales_support_agent.services.brand_analysis.schema import CATEGORY_DTC

logger = logging.getLogger(__name__)

_DOCX_MEDIA = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"


def _check_admin_access(request: Request) -> None:
    if not is_authenticated(request):
        raise HTTPException(status_code=303, headers={"Location": "/admin/login"})


router = APIRouter(
    prefix="/admin/executive/brand-analysis",
    tags=["brand-analysis"],
    dependencies=[Depends(_check_admin_access)],
)


# ---------------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------------


@router.get("", response_class=HTMLResponse)
def landing(request: Request, msg: str = "", detail: str = "") -> HTMLResponse:
    runs = storage.list_reports()
    return HTMLResponse(render_brand_analysis_page(
        runs=runs, user=get_session_user_from_request(request), flash=msg, detail=detail,
    ))


@router.get("/history", response_class=HTMLResponse)
def history(request: Request) -> HTMLResponse:
    runs = storage.list_reports()
    return HTMLResponse(render_history_page(runs=runs, user=get_session_user_from_request(request)))


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------


@router.post("/run")
async def run(
    files: list[UploadFile] = File(default=[]),
    brand: str = Form(default=""),
    category: str = Form(default=CATEGORY_DTC),
    label: str = Form(default=""),
) -> RedirectResponse:
    batch: list[tuple[str, bytes]] = []
    for f in files or []:
        if f is not None and f.filename:
            data = await f.read()
            if data:
                batch.append((f.filename, data))

    if not batch:
        return RedirectResponse(
            "/admin/executive/brand-analysis?msg=" + quote_plus("Upload at least one financial file to run an analysis."),
            status_code=303,
        )

    prepared = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        report = build_report(batch, brand=brand, category=category, prepared_date=prepared)
        docx_bytes = build_docx(report)
        report_id = storage.save_report(report, label=label, source_files=batch, docx_bytes=docx_bytes)
    except Exception as exc:  # noqa: BLE001
        logger.exception("[brand_analysis] run failed")
        rid = storage.save_error(brand or "Brand", str(exc), label=label)
        return RedirectResponse(
            f"/admin/executive/brand-analysis?msg=" + quote_plus(f"Analysis failed: {str(exc)[:120]}"),
            status_code=303,
        )

    return RedirectResponse(f"/admin/executive/brand-analysis/{report_id}", status_code=303)


# ---------------------------------------------------------------------------
# View + download a saved report
# ---------------------------------------------------------------------------


@router.get("/{report_id}", response_class=HTMLResponse)
def view(request: Request, report_id: str) -> HTMLResponse:
    report = storage.get_report(report_id)
    if report is None:
        raise HTTPException(status_code=404, detail="Report not found.")
    return HTMLResponse(render_report(report, report_id=report_id, user=get_session_user_from_request(request)))


@router.get("/{report_id}/download")
def download(report_id: str) -> Response:
    data = storage.get_docx(report_id)
    if not data:
        # Re-generate from the saved report if the blob is missing.
        report = storage.get_report(report_id)
        if report is None:
            raise HTTPException(status_code=404, detail="Report not found.")
        data = build_docx(report)
        storage.save_docx(report_id, data)

    report = storage.get_report(report_id)
    brand_slug = "".join(ch for ch in (report.brand if report else "brand") if ch.isalnum() or ch in "-_") or "brand"
    filename = f"brand-analysis-{brand_slug}-{report_id[:8]}.docx"
    return Response(
        content=data,
        media_type=_DOCX_MEDIA,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
