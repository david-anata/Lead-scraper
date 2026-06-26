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

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, Response

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
from sales_support_agent.services.advertising.clients_page import render_clients_page
from sales_support_agent.services.advertising.intake import route_files
from sales_support_agent.services.advertising.schema import ExternalCostRow, Goals
from sales_support_agent.services.auth_deps import get_session_user_from_request, require_tool

logger = logging.getLogger(__name__)


router = APIRouter(
    prefix="/admin/advertising",
    tags=["advertising"],
    dependencies=[Depends(require_tool("advertising.audit"))],
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


@router.get("/audit", response_class=HTMLResponse)
def audit_page(request: Request, run: str = "", msg: str = "", detail: str = "") -> HTMLResponse:
    user = get_session_user_from_request(request)

    runs = [_with_files(r) for r in storage.list_runs()]

    # Slim last-run strip: the ?run= run if given, else the most recent.
    latest = None
    if run:
        latest = _with_files(storage.get_run(run))
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
        c["runs"] = [_with_files(r) for r in storage.list_runs(client_id=c["id"])]
    return HTMLResponse(render_clients_page(clients, user=user, flash=msg))


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
    cid = (client_id or "").strip() or None
    if cid and not storage.get_client(cid):
        cid = None  # stale/unknown id — treat as ad-hoc rather than 500

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
    goals = Goals(
        revenue_target_cents=_dollars_to_cents(goals_form.get("revenue_target", "")),
        acos_target_bps=_pct_to_bps(goals_form.get("acos_target", "")),
        tacos_target_bps=_pct_to_bps(goals_form.get("tacos_target", "")),
        units_target=_int_or_none(goals_form.get("units_target", "")),
        period=goals_form.get("period") or "monthly",
    )
    if any([goals.revenue_target_cents, goals.acos_target_bps, goals.tacos_target_bps, goals.units_target]):
        storage.save_goals(goals, client_id=cid)
    else:
        goals = storage.get_active_goals(client_id=cid)  # fall back to saved targets

    result = run_audit(inputs, goals=goals, label=label, brand=brand, client_id=cid)
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
