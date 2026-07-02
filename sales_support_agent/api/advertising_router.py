"""Advertising > Audit controller — routes under /admin/advertising.

Self-contained backend router (same pattern as cashflow_router): renders its own
HTML pages, handles multipart CSV/XLSX uploads, runs the audit, and serves the
round-tripped bulk sheet. Mounted in-process by the frontend via include_router.
"""

from __future__ import annotations

import base64
import logging
import re
from typing import Optional
from urllib.parse import quote_plus

import requests
from fastapi import APIRouter, BackgroundTasks, Depends, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response

from sales_support_agent.services.advertising import storage
from sales_support_agent.services.advertising.audit import (
    AuditInputs,
    detect_brand_in_business_report,
    run_audit,
)
from sales_support_agent.services.advertising.audit_page import (
    render_audit_page,
    render_brand_mismatch_page,
)
from sales_support_agent.services.advertising.bulk_profitability_page import (
    render_bulk_profitability_app_page,
    render_bulk_profitability_host_page,
)
from sales_support_agent.services.advertising.clients_page import render_clients_page
from sales_support_agent.services.advertising.profit_calculator_page import (
    render_profit_calculator_app_page,
    render_profit_calculator_host_page,
)
from sales_support_agent.services.advertising.intake import route_files
from sales_support_agent.services.advertising.schema import ExternalCostRow, Goals
from sales_support_agent.services.auth_deps import get_session_user_from_request, require_tool
from sales_support_agent.services.access.pages import render_forbidden_page

logger = logging.getLogger(__name__)

_advertising_user = require_tool("advertising.audit")

router = APIRouter(
    prefix="/admin/advertising",
    tags=["advertising"],
    dependencies=[Depends(_advertising_user)],
)

public_router = APIRouter(tags=["advertising-public"])

_PUBLIC_CALCULATOR_PATH = "/amazon-profit-calculator/runtime"
_PUBLIC_CALCULATOR_API_BASE = "/api/public/amazon-profit-calculator"
_PUBLIC_BULK_PROFITABILITY_PATH = "/amazon-bulk-profitability/runtime"
_PUBLIC_BULK_PROFITABILITY_API_BASE = "/api/public/amazon-bulk-profitability"


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


def _profit_api_base_url(request: Request) -> str:
    app_state = request.app.state
    for settings_obj in (
        getattr(app_state, "settings", None),
        getattr(app_state, "agent_settings", None),
        getattr(app_state, "admin_dashboard_settings", None),
    ):
        value = getattr(settings_obj, "amazon_profit_api_base_url", "") if settings_obj is not None else ""
        if value:
            return str(value).rstrip("/")
    return ""


def _profit_api_error(response: requests.Response) -> HTTPException:
    detail = f"Profit API request failed with status {response.status_code}."
    try:
        payload = response.json()
    except ValueError:
        payload = None
    if isinstance(payload, dict):
        detail = str(payload.get("detail") or detail)
    elif response.text:
        detail = response.text.strip()[:400] or detail
    return HTTPException(status_code=response.status_code or 502, detail=detail)


def _calculator_embed_headers() -> dict[str, str]:
    return {
        "Cache-Control": "public, max-age=300",
        "Content-Security-Policy": "default-src 'self' 'unsafe-inline' data: https:; img-src 'self' data: https:; media-src https: data:; frame-ancestors *;",
    }


# ---------------------------------------------------------------------------
# Page
# ---------------------------------------------------------------------------


def _with_files(run_dict: Optional[dict]) -> Optional[dict]:
    """Annotate a run dict with which download files exist for it."""
    if not run_dict:
        return run_dict
    files = storage.list_bulk_files(run_dict["id"])
    run_dict["has_plan"] = "growth_plan" in files
    run_dict["has_bids"] = "bids" in files
    run_dict["has_additions"] = "additions" in files
    # back-compat with older runs that stored a single combined file
    run_dict["has_apply"] = "combined" in files
    return run_dict


def _visible_run(run_dict: Optional[dict]) -> bool:
    """Only finalized runs belong in operator-facing history.

    A deploy or worker restart can interrupt an audit after create_run() has
    already persisted a draft row. Those rows have no summary/download content
    and should not replace the last good run in the page strip/history.
    """
    return bool(run_dict) and str((run_dict or {}).get("status") or "").strip().lower() != "draft"


@router.get("/audit", response_class=HTMLResponse)
def audit_page(request: Request, run: str = "", msg: str = "", detail: str = "") -> HTMLResponse:
    user = get_session_user_from_request(request)

    runs = [_with_files(r) for r in storage.list_runs() if _visible_run(r)]

    # Slim last-run strip: the ?run= run if given, else the most recent.
    latest = None
    if run:
        selected = _with_files(storage.get_run(run))
        latest = selected if _visible_run(selected) else None
    elif runs:
        latest = runs[0]

    html = render_audit_page(
        goals=storage.get_active_goals(),
        runs=runs,
        latest=latest,
        user=user,
        flash=msg,
        detail=detail,
        clients=storage.list_clients(),
        client_goals_map=storage.get_client_goals_map(),
    )
    return HTMLResponse(html)


# ---------------------------------------------------------------------------
# Clients — repository of advertising clients + per-client goals & history
# ---------------------------------------------------------------------------


@router.get("/clients", response_class=HTMLResponse)
def clients_page(request: Request, msg: str = "") -> HTMLResponse:
    user = get_session_user_from_request(request)
    clients = storage.list_clients()
    for c in clients:
        c["goals"] = storage.get_active_goals(client_id=c["id"])
        c["runs"] = [_with_files(r) for r in storage.list_runs(client_id=c["id"]) if _visible_run(r)]
    return HTMLResponse(render_clients_page(clients, user=user, flash=msg))


@router.get("/profit-calculator", response_class=HTMLResponse)
def profit_calculator_page(request: Request, user: dict = Depends(_advertising_user)) -> HTMLResponse:
    if not user or not user.get("is_superadmin"):
        return HTMLResponse(
            render_forbidden_page(user=user, tool_label="Super-admin only"),
            status_code=403,
        )
    html = render_profit_calculator_host_page(
        app_src=_PUBLIC_CALCULATOR_PATH,
        user=user,
    )
    return HTMLResponse(html)


@router.get("/bulk-profitability", response_class=HTMLResponse)
def bulk_profitability_page(request: Request, user: dict = Depends(_advertising_user)) -> HTMLResponse:
    if not user or not user.get("is_superadmin"):
        return HTMLResponse(
            render_forbidden_page(user=user, tool_label="Super-admin only"),
            status_code=403,
        )
    html = render_bulk_profitability_host_page(
        app_src=_PUBLIC_BULK_PROFITABILITY_PATH,
        user=user,
    )
    return HTMLResponse(html)


@public_router.get(_PUBLIC_CALCULATOR_PATH, response_class=HTMLResponse)
def profit_calculator_app() -> HTMLResponse:
    html = render_profit_calculator_app_page(api_base=_PUBLIC_CALCULATOR_API_BASE)
    return HTMLResponse(html, headers=_calculator_embed_headers())


@public_router.get(f"{_PUBLIC_CALCULATOR_API_BASE}/catalog/{{asin}}")
def profit_calculator_catalog_proxy(asin: str, request: Request) -> JSONResponse:
    normalized_asin = (asin or "").strip().upper()
    if not normalized_asin:
        raise HTTPException(status_code=400, detail="ASIN is required.")
    upstream_base = _profit_api_base_url(request)
    if not upstream_base:
        raise HTTPException(status_code=503, detail="Profit API base URL is not configured.")
    response = requests.get(
        f"{upstream_base}/api/public/amazon/catalog/{normalized_asin}",
        headers={"accept": "application/json"},
        timeout=20,
    )
    if not response.ok:
        raise _profit_api_error(response)
    return JSONResponse(response.json())


@public_router.get(_PUBLIC_BULK_PROFITABILITY_PATH, response_class=HTMLResponse)
def bulk_profitability_app() -> HTMLResponse:
    html = render_bulk_profitability_app_page(api_base=_PUBLIC_BULK_PROFITABILITY_API_BASE)
    return HTMLResponse(html, headers=_calculator_embed_headers())


@public_router.get(f"{_PUBLIC_BULK_PROFITABILITY_API_BASE}/catalog/{{asin}}")
def bulk_profitability_catalog_proxy(asin: str, request: Request) -> JSONResponse:
    normalized_asin = (asin or "").strip().upper()
    if not normalized_asin:
        raise HTTPException(status_code=400, detail="ASIN is required.")
    upstream_base = _profit_api_base_url(request)
    if not upstream_base:
        raise HTTPException(status_code=503, detail="Profit API base URL is not configured.")
    response = requests.get(
        f"{upstream_base}/api/public/amazon/catalog/{normalized_asin}",
        headers={"accept": "application/json"},
        timeout=20,
    )
    if not response.ok:
        raise _profit_api_error(response)
    return JSONResponse(response.json())


@public_router.post(f"{_PUBLIC_BULK_PROFITABILITY_API_BASE}/profitability/estimate")
async def bulk_profitability_estimate_proxy(request: Request) -> JSONResponse:
    upstream_base = _profit_api_base_url(request)
    if not upstream_base:
        raise HTTPException(status_code=503, detail="Profit API base URL is not configured.")
    payload = await request.json()
    response = requests.post(
        f"{upstream_base}/api/public/amazon/profitability/estimate",
        headers={"accept": "application/json", "content-type": "application/json"},
        json=payload,
        timeout=20,
    )
    if not response.ok:
        raise _profit_api_error(response)
    return JSONResponse(response.json())


@public_router.post(f"{_PUBLIC_CALCULATOR_API_BASE}/profitability/estimate")
async def profit_calculator_estimate_proxy(request: Request) -> JSONResponse:
    upstream_base = _profit_api_base_url(request)
    if not upstream_base:
        raise HTTPException(status_code=503, detail="Profit API base URL is not configured.")
    payload = await request.json()
    response = requests.post(
        f"{upstream_base}/api/public/amazon/profitability/estimate",
        headers={"accept": "application/json", "content-type": "application/json"},
        json=payload,
        timeout=20,
    )
    if not response.ok:
        raise _profit_api_error(response)
    return JSONResponse(response.json())


@router.post("/clients/new")
def clients_new(
    name: str = Form(default=""),
    objectives: str = Form(default=""),
) -> RedirectResponse:
    name = (name or "").strip()
    if not name:
        return RedirectResponse(
            "/admin/advertising/clients?msg=Enter+a+client+name.", status_code=303
        )
    storage.create_client(name, objectives=objectives or "")
    return RedirectResponse(
        f"/admin/advertising/clients?msg={quote_plus(f'Client “{name}” added.')}", status_code=303
    )


@router.post("/clients/{client_id}")
def clients_save(
    client_id: str,
    name: str = Form(default=""),
    objectives: str = Form(default=""),
    revenue_target: str = Form(default=""),
    acos_target: str = Form(default=""),
    tacos_target: str = Form(default=""),
    units_target: str = Form(default=""),
    period: str = Form(default="monthly"),
) -> RedirectResponse:
    if not storage.get_client(client_id):
        return RedirectResponse(
            "/admin/advertising/clients?msg=Client+not+found.", status_code=303
        )
    storage.update_client(client_id, name=(name or "").strip() or None, objectives=objectives or "")
    goals = Goals(
        revenue_target_cents=_dollars_to_cents(revenue_target),
        acos_target_bps=_pct_to_bps(acos_target),
        tacos_target_bps=_pct_to_bps(tacos_target),
        units_target=_int_or_none(units_target),
        period=period or "monthly",
    )
    storage.save_goals(goals, client_id=client_id)
    return RedirectResponse(
        "/admin/advertising/clients?msg=Saved.", status_code=303
    )


@router.post("/clients/{client_id}/archive")
def clients_archive(client_id: str) -> RedirectResponse:
    if not storage.get_client(client_id):
        return RedirectResponse(
            "/admin/advertising/clients?msg=Client+not+found.", status_code=303
        )
    client = storage.get_client(client_id)
    name = (client or {}).get("name") or "Client"
    storage.archive_client(client_id)
    return RedirectResponse(
        f"/admin/advertising/clients?msg={quote_plus(f'{name} archived.')}", status_code=303
    )


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


def _norm_brand(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def _brands_match(a: str, b: str) -> bool:
    na, nb = _norm_brand(a), _norm_brand(b)
    if not na or not nb:
        return False
    return na == nb or na in nb or nb in na


def _truthy(value: str) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _resolved_client_id(client_id: str) -> Optional[str]:
    cid = (client_id or "").strip() or None
    if cid and not storage.get_client(cid):
        return None
    return cid


def _goals_from_form(goals_form: dict) -> Goals:
    return Goals(
        revenue_target_cents=_dollars_to_cents(goals_form.get("revenue_target", "")),
        acos_target_bps=_pct_to_bps(goals_form.get("acos_target", "")),
        tacos_target_bps=_pct_to_bps(goals_form.get("tacos_target", "")),
        units_target=_int_or_none(goals_form.get("units_target", "")),
        period=goals_form.get("period") or "monthly",
    )


def _has_goal_values(goals: Optional[Goals]) -> bool:
    return bool(goals) and any([
        goals.revenue_target_cents,
        goals.acos_target_bps,
        goals.tacos_target_bps,
        goals.units_target,
    ])


def _inputs_for_mismatch_check(batch: list[tuple[str, bytes]], labeled: dict) -> AuditInputs:
    inputs, _ = route_files(batch)
    for attr, data in (labeled or {}).items():
        if data:
            setattr(inputs, attr, data)
    return inputs


def _maybe_render_brand_mismatch(
    *,
    batch: list[tuple[str, bytes]],
    labeled: dict,
    ext_channel: list[str],
    ext_label: list[str],
    ext_amount: list[str],
    label: str,
    brand: str,
    client_id: Optional[str],
    goals_form: dict,
) -> Optional[HTMLResponse]:
    """Preflight the client/file brand mismatch before a background run starts.

    Async runs must not create a persistent `running` row and then bail out for
    confirmation, otherwise the UI is left showing a run that can never finish.
    """
    if not client_id:
        return None
    inputs = _inputs_for_mismatch_check(batch, labeled)
    if not inputs.business_report_csv:
        return None
    known = _client_known_brands(client_id)
    if not known:
        return None
    detected = detect_brand_in_business_report(inputs.business_report_csv)
    if not detected or any(_brands_match(detected, k) for k in known):
        return None
    token = storage.stage_pending_upload(_encode_pending(
        batch, labeled, ext_channel, ext_label, ext_amount,
        label, brand, client_id, goals_form,
    ))
    client = storage.get_client(client_id) or {}
    return HTMLResponse(render_brand_mismatch_page(
        client_name=client.get("name") or "this client",
        detected=detected,
        known=sorted(known),
        token=token,
        user=None,
    ))


def _client_known_brands(client_id: str) -> set[str]:
    """Brands this client has audited before (typed brand + auto-detected),
    drawn from completed run summaries — the basis for the mismatch check."""
    out: set[str] = set()
    for r in storage.list_runs(client_id=client_id):
        if r.get("status") != "complete":
            continue
        s = r.get("summary") or {}
        for key in ("brand", "detected_brand"):
            v = (s.get(key) or "").strip()
            if v:
                out.add(v)
    return out


def _do_run(
    *,
    batch: list[tuple[str, bytes]],
    labeled: dict,
    ext_channel: list[str],
    ext_label: list[str],
    ext_amount: list[str],
    label: str,
    brand: str,
    client_id: str,
    goals_form: dict,
    confirmed: bool = False,
    run_id: Optional[str] = None,
):
    """Shared run pipeline used by both the direct POST and the confirm
    round-trip. Rebuilds AuditInputs from already-read bytes, applies the
    block/confirm brand-mismatch gate, then runs the audit."""
    inputs, report = route_files(batch)
    for attr, data in (labeled or {}).items():
        if data:
            setattr(inputs, attr, data)

    for i, channel in enumerate(ext_channel):
        amount = ext_amount[i] if i < len(ext_amount) else ""
        ext_lbl = ext_label[i] if i < len(ext_label) else ""
        cents = _dollars_to_cents(amount)
        if channel and cents:
            inputs.external_costs_manual.append(
                ExternalCostRow(
                    channel=channel,
                    cost_type="commission" if channel == "influencer" else "ad_spend",
                    label=(ext_lbl or channel),
                    amount_cents=cents,
                )
            )

    if not inputs.any_data():
        return RedirectResponse(
            "/admin/advertising/audit?msg=Upload+at+least+one+report+to+run+an+audit.",
            status_code=303,
        )

    # A client may be selected — scope goals + history to it. Blank = ad-hoc
    # (the existing global goal set), preserving the prior behavior.
    cid = _resolved_client_id(client_id)

    # --- The catch: block/confirm before running on a brand mismatch ---
    # If this client has run audits before and the uploaded Business Report's
    # detected brand matches none of them, stop and require confirmation so the
    # wrong files can't be silently run under the wrong client.
    if cid and not confirmed and inputs.business_report_csv:
        known = _client_known_brands(cid)
        if known:
            detected = detect_brand_in_business_report(inputs.business_report_csv)
            if detected and not any(_brands_match(detected, k) for k in known):
                token = storage.stage_pending_upload(_encode_pending(
                    batch, labeled, ext_channel, ext_label, ext_amount,
                    label, brand, cid, goals_form,
                ))
                client = storage.get_client(cid) or {}
                return HTMLResponse(render_brand_mismatch_page(
                    client_name=client.get("name") or "this client",
                    detected=detected, known=sorted(known), token=token, user=None,
                ))

    # Goals are part of the run form: save them (per-client when one is selected,
    # "tweaks save back to the client"), and run against them.
    goals = _goals_from_form(goals_form)
    if _has_goal_values(goals):
        storage.save_goals(goals, client_id=cid)
    else:
        goals = storage.get_active_goals(client_id=cid)  # fall back to saved targets

    result = run_audit(inputs, goals=goals, label=label, brand=brand, client_id=cid, run_id=run_id)
    if result.status == "error":
        return RedirectResponse(
            f"/admin/advertising/audit?run={result.run_id}&msg=Audit+failed:+{result.error[:80]}",
            status_code=303,
        )
    applied = result.bulk.applied if result.bulk else 0
    detect = quote_plus(report.summary()) if batch else ""
    ncc = (result.summary or {}).get("new_campaign_count") or 0
    extra = f" ⚠ {ncc} NEW campaign(s) in the Additions file — live on upload." if ncc else ""
    msg = quote_plus(
        f"Audit complete: {result.counts.get('recommendations', 0)} recommendations, {applied} bulk changes.{extra}"
    )
    suffix = f"&detail={detect}" if detect else ""
    return RedirectResponse(f"/admin/advertising/audit?run={result.run_id}&msg={msg}{suffix}", status_code=303)


def _do_run_background(**kwargs) -> None:
    run_id = str(kwargs.get("run_id") or "").strip()
    try:
        response = _do_run(**kwargs)
        if run_id and isinstance(response, HTMLResponse):
            storage.finalize_run(
                run_id,
                status="error",
                error="Audit needs confirmation before it can run. Re-open the audit page and confirm the client/brand match.",
            )
        elif run_id and isinstance(response, RedirectResponse):
            location = str(response.headers.get("location") or "")
            if "Upload+at+least+one+report" in location:
                storage.finalize_run(run_id, status="error", error="No valid report files were uploaded.")
    except Exception as exc:
        logger.exception("[advertising] background audit run failed")
        if run_id:
            storage.finalize_run(run_id, status="error", error=str(exc))


def _encode_pending(batch, labeled, ext_channel, ext_label, ext_amount,
                    label, brand, client_id, goals_form) -> dict:
    return {
        "batch": [[name, base64.b64encode(data).decode("ascii")] for name, data in batch],
        "labeled": {k: base64.b64encode(v).decode("ascii") for k, v in (labeled or {}).items()},
        "ext_channel": list(ext_channel), "ext_label": list(ext_label), "ext_amount": list(ext_amount),
        "label": label, "brand": brand, "client_id": client_id, "goals_form": goals_form,
    }


def _decode_pending(p: dict):
    batch = [(name, base64.b64decode(b64)) for name, b64 in p.get("batch", [])]
    labeled = {k: base64.b64decode(v) for k, v in (p.get("labeled") or {}).items()}
    return {
        "batch": batch, "labeled": labeled,
        "ext_channel": p.get("ext_channel", []), "ext_label": p.get("ext_label", []),
        "ext_amount": p.get("ext_amount", []), "label": p.get("label", ""),
        "brand": p.get("brand", ""), "client_id": p.get("client_id") or "",
        "goals_form": p.get("goals_form") or {},
    }


@router.post("/audit/run")
async def run(
    background_tasks: BackgroundTasks,
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
    client_id: str = Form(default=""),
    revenue_target: str = Form(default=""),
    acos_target: str = Form(default=""),
    tacos_target: str = Form(default=""),
    units_target: str = Form(default=""),
    period: str = Form(default="monthly"),
    run_async: str = Form(default="true"),
):
    # Read every dropped file once (auto-detect path), plus the labeled slots.
    batch: list[tuple[str, bytes]] = []
    for f in files or []:
        if f is not None and f.filename:
            data = await f.read()
            if data:
                batch.append((f.filename, data))
    labeled: dict = {}
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
            labeled[attr] = data

    goals_form = {
        "revenue_target": revenue_target, "acos_target": acos_target,
        "tacos_target": tacos_target, "units_target": units_target, "period": period,
    }
    if _truthy(run_async):
        preflight_inputs = _inputs_for_mismatch_check(batch, labeled)
        if not preflight_inputs.any_data():
            return RedirectResponse(
                "/admin/advertising/audit?msg=Upload+at+least+one+report+to+run+an+audit.",
                status_code=303,
            )
        cid = _resolved_client_id(client_id)
        mismatch = _maybe_render_brand_mismatch(
            batch=batch,
            labeled=labeled,
            ext_channel=ext_channel,
            ext_label=ext_label,
            ext_amount=ext_amount,
            label=label,
            brand=brand,
            client_id=cid,
            goals_form=goals_form,
        )
        if mismatch is not None:
            return mismatch
        pending_goals = _goals_from_form(goals_form)
        if not _has_goal_values(pending_goals):
            pending_goals = storage.get_active_goals(client_id=cid)
        pending_run_id = storage.create_run(
            label=(f"{(brand or '').strip()} — {label}".strip(" —") if (brand or "").strip() else label),
            client_id=cid,
            goals=pending_goals,
            status="running",
        )
        background_tasks.add_task(
            _do_run_background,
            batch=batch, labeled=labeled, ext_channel=ext_channel, ext_label=ext_label,
            ext_amount=ext_amount, label=label, brand=brand, client_id=client_id,
            goals_form=goals_form, confirmed=False,
            run_id=pending_run_id,
        )
        return RedirectResponse(
            f"/admin/advertising/audit?run={pending_run_id}&msg=Audit+started.+Refresh+this+page+in+a+minute+to+see+downloads.",
            status_code=303,
        )
    return _do_run(
        batch=batch, labeled=labeled, ext_channel=ext_channel, ext_label=ext_label,
        ext_amount=ext_amount, label=label, brand=brand, client_id=client_id,
        goals_form=goals_form, confirmed=False,
    )


@router.post("/audit/run/confirm")
def run_confirm(confirm_token: str = Form(default="")):
    """Second half of the block/confirm gate — re-run from the staged upload the
    user explicitly confirmed despite the brand mismatch."""
    payload = storage.get_pending_upload(confirm_token)
    if not payload:
        return RedirectResponse(
            "/admin/advertising/audit?msg=That+confirmation+expired+—+please+re-upload.",
            status_code=303,
        )
    storage.clear_pending_upload(confirm_token)
    return _do_run(confirmed=True, **_decode_pending(payload))


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
