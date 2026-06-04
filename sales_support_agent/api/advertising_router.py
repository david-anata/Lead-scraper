"""Advertising > Audit controller — routes under /admin/advertising.

Self-contained backend router (same pattern as cashflow_router): renders its own
HTML pages, handles multipart CSV/XLSX uploads, runs the audit, and serves the
round-tripped bulk sheet. Mounted in-process by the frontend via include_router.
"""

from __future__ import annotations

import logging
from typing import Optional
from urllib.parse import quote_plus

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from sales_support_agent.services.advertising import storage
from sales_support_agent.services.advertising.audit import AuditInputs, run_audit
from sales_support_agent.services.advertising.audit_page import render_audit_page
from sales_support_agent.services.advertising.intake import route_files
from sales_support_agent.services.advertising.schema import ExternalCostRow, Goals
from sales_support_agent.services.auth_deps import get_session_user_from_request, is_authenticated

logger = logging.getLogger(__name__)


def _check_admin_access(request: Request) -> None:
    if not is_authenticated(request):
        raise HTTPException(status_code=303, headers={"Location": "/admin/login"})


router = APIRouter(
    prefix="/admin/advertising",
    tags=["advertising"],
    dependencies=[Depends(_check_admin_access)],
)


def _dollars_to_cents(raw: str) -> Optional[int]:
    raw = (raw or "").strip().replace("$", "").replace(",", "")
    if not raw:
        return None
    try:
        return int(round(float(raw) * 100))
    except ValueError:
        return None


def _pct_to_bps(raw: str) -> Optional[int]:
    raw = (raw or "").strip().replace("%", "")
    if not raw:
        return None
    try:
        return int(round(float(raw) * 100))
    except ValueError:
        return None


def _int_or_none(raw: str) -> Optional[int]:
    raw = (raw or "").strip().replace(",", "")
    if not raw:
        return None
    try:
        return int(round(float(raw)))
    except ValueError:
        return None


async def _read_upload(f: Optional[UploadFile]) -> Optional[bytes]:
    if f is not None and f.filename:
        data = await f.read()
        return data or None
    return None


# ---------------------------------------------------------------------------
# Page
# ---------------------------------------------------------------------------


@router.get("/audit", response_class=HTMLResponse)
def audit_page(request: Request, run: str = "", msg: str = "", detail: str = "") -> HTMLResponse:
    user = get_session_user_from_request(request)
    runs = storage.list_runs()

    latest = None
    if run:
        latest = storage.get_run(run)
    elif runs:
        latest = storage.get_run(runs[0]["id"])

    recommendations: list[dict] = []
    bulk_available = False
    plan_available = False
    if latest:
        recommendations = storage.get_recommendations(latest["id"])
        files = storage.list_bulk_files(latest["id"])
        bulk_available = "combined" in files
        plan_available = "growth_plan" in files

    html = render_audit_page(
        goals=storage.get_active_goals(),
        latest=latest,
        recommendations=recommendations,
        bulk_available=bulk_available,
        plan_available=plan_available,
        runs=runs,
        user=user,
        flash=msg,
        detail=detail,
    )
    return HTMLResponse(html)


# ---------------------------------------------------------------------------
# Goals
# ---------------------------------------------------------------------------


@router.post("/audit/goals")
def save_goals(
    revenue_target: str = Form(default=""),
    acos_target: str = Form(default=""),
    tacos_target: str = Form(default=""),
    units_target: str = Form(default=""),
    period: str = Form(default="monthly"),
) -> RedirectResponse:
    goals = Goals(
        revenue_target_cents=_dollars_to_cents(revenue_target),
        acos_target_bps=_pct_to_bps(acos_target),
        tacos_target_bps=_pct_to_bps(tacos_target),
        units_target=_int_or_none(units_target),
        period=period or "monthly",
    )
    storage.save_goals(goals)
    return RedirectResponse("/admin/advertising/audit?msg=Goals+saved.", status_code=303)


# ---------------------------------------------------------------------------
# Run audit
# ---------------------------------------------------------------------------


@router.post("/audit/run")
async def run(
    files: list[UploadFile] = File(default=[]),
    bulk_xlsx: Optional[UploadFile] = File(default=None),
    search_term_csv: Optional[UploadFile] = File(default=None),
    business_report_csv: Optional[UploadFile] = File(default=None),
    sqp_csv: Optional[UploadFile] = File(default=None),
    dsp_csv: Optional[UploadFile] = File(default=None),
    external_costs_csv: Optional[UploadFile] = File(default=None),
    cogs_csv: Optional[UploadFile] = File(default=None),
    ext_channel: list[str] = Form(default=[]),
    ext_label: list[str] = Form(default=[]),
    ext_amount: list[str] = Form(default=[]),
    label: str = Form(default=""),
    brand: str = Form(default=""),
) -> RedirectResponse:
    # Mass-upload path: auto-detect + route every dropped file by its headers.
    batch: list[tuple[str, bytes]] = []
    for f in files or []:
        if f is not None and f.filename:
            data = await f.read()
            if data:
                batch.append((f.filename, data))
    inputs, report = route_files(batch)

    # Labeled-slot path (still supported) takes precedence over auto-detected.
    for attr, upload in (
        ("bulk_xlsx", bulk_xlsx),
        ("search_term_csv", search_term_csv),
        ("business_report_csv", business_report_csv),
        ("sqp_csv", sqp_csv),
        ("dsp_csv", dsp_csv),
        ("external_costs_csv", external_costs_csv),
        ("cogs_csv", cogs_csv),
    ):
        data = await _read_upload(upload)
        if data is not None:
            setattr(inputs, attr, data)

    # Unlimited external-channel rows (channel/label/amount arrays, zipped by index).
    for i, channel in enumerate(ext_channel):
        amount = ext_amount[i] if i < len(ext_amount) else ""
        label = ext_label[i] if i < len(ext_label) else ""
        cents = _dollars_to_cents(amount)
        if channel and cents:
            inputs.external_costs_manual.append(
                ExternalCostRow(
                    channel=channel,
                    cost_type="commission" if channel == "influencer" else "ad_spend",
                    label=(label or channel),
                    amount_cents=cents,
                )
            )

    if not inputs.any_data():
        return RedirectResponse(
            "/admin/advertising/audit?msg=Upload+at+least+one+report+to+run+an+audit.",
            status_code=303,
        )

    result = run_audit(inputs, label=label, brand=brand)
    if result.status == "error":
        return RedirectResponse(
            f"/admin/advertising/audit?run={result.run_id}&msg=Audit+failed:+{result.error[:80]}",
            status_code=303,
        )
    applied = result.bulk.applied if result.bulk else 0
    detect = quote_plus(report.summary()) if batch else ""
    msg = quote_plus(
        f"Audit complete: {result.counts.get('recommendations', 0)} recommendations, {applied} bulk changes."
    )
    suffix = f"&detail={detect}" if detect else ""
    return RedirectResponse(f"/admin/advertising/audit?run={result.run_id}&msg={msg}{suffix}", status_code=303)


# ---------------------------------------------------------------------------
# Bulk sheet download
# ---------------------------------------------------------------------------


@router.get("/audit/{run_id}/bulk/{ad_type}.xlsx")
def download_bulk(run_id: str, ad_type: str) -> Response:
    data = storage.get_bulk_file(run_id, ad_type)
    if not data:
        raise HTTPException(status_code=404, detail="Bulk sheet not found — re-run the audit to regenerate it.")
    filename = f"amazon-bulk-{ad_type}-{run_id[:8]}.xlsx"
    return Response(
        content=data,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/audit/{run_id}/plan.xlsx")
def download_plan(run_id: str) -> Response:
    data = storage.get_bulk_file(run_id, "growth_plan")
    if not data:
        raise HTTPException(status_code=404, detail="Growth plan not found — re-run the audit to regenerate it.")
    filename = f"growth-plan-{run_id[:8]}.xlsx"
    return Response(
        content=data,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
