"""Finance / Cashflow Controller — all routes under /admin/finance."""

from __future__ import annotations

import asyncio

from fastapi import APIRouter, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse

from sales_support_agent.services.cashflow.alerts import render_risk_alerts_page
from sales_support_agent.services.cashflow.ap import (
    parse_obligation_form,
    render_ap_edit_page,
    render_ap_new_page,
    render_upcoming_ap_page,
)
from sales_support_agent.services.cashflow.ar import (
    render_ar_edit_page,
    render_ar_new_page,
    render_expected_ar_page,
)
from sales_support_agent.services.cashflow.forecast import render_weekly_forecast_page
from sales_support_agent.services.cashflow.obligations import (
    create_obligation,
    create_recurring_template,
    delete_obligation,
    delete_recurring_template,
    generate_upcoming_from_templates,
    update_obligation,
    update_recurring_template,
)
from sales_support_agent.services.cashflow.overview import render_cashflow_overview_page
from sales_support_agent.services.cashflow.recurring import (
    parse_template_form,
    render_recurring_edit_page,
    render_recurring_new_page,
    render_recurring_page,
)
from sales_support_agent.services.cashflow.scenario import render_scenario_page
from sales_support_agent.services.cashflow.upload import run_csv_upload
from sales_support_agent.services.cashflow.upload_page import (
    render_upload_page,
    render_upload_result,
)
from sales_support_agent.services.auth_deps import has_finance_access
from sales_support_agent.services.cashflow.clickup_sync import sync_clickup_finance

router = APIRouter(prefix="/admin/finances", tags=["finance"])


# ---------------------------------------------------------------------------
# Auth guard helper
# ---------------------------------------------------------------------------

def _redirect_login() -> RedirectResponse:
    return RedirectResponse("/admin/login", status_code=303)


# ---------------------------------------------------------------------------
# Health check — no auth, used for post-deploy self-testing
# ---------------------------------------------------------------------------

@router.get("/health")
async def cashflow_health():
    """
    Self-test endpoint.  Returns JSON with DB column state and static INSERT
    coverage for every cashflow write path.  No session cookie required.
    """
    import importlib as _importlib
    import inspect as _inspect
    from fastapi.responses import JSONResponse

    # NOT NULL columns that every INSERT must explicitly provide
    REQUIRED_COLUMNS: set = {
        "id", "source", "source_id", "event_type", "category",
        "subcategory", "description", "name", "vendor_or_customer",
        "amount_cents", "status", "confidence",
        "recurring_rule", "clickup_task_id",
        "bank_transaction_type", "bank_reference", "notes",
        "created_at", "updated_at",
    }

    checks: dict = {}
    db_columns: list = []
    missing_columns: list = []
    overall = "ok"

    # -- Live DB check -------------------------------------------------------
    try:
        from sales_support_agent.models.database import engine
        from sqlalchemy import inspect as _sainsp

        insp = _sainsp(engine)
        tables = set(insp.get_table_names())
        checks["cash_events_table_exists"] = "cash_events" in tables

        if checks["cash_events_table_exists"]:
            db_columns = sorted(c["name"] for c in insp.get_columns("cash_events"))
            missing_columns = sorted(REQUIRED_COLUMNS - set(db_columns))
            checks["all_required_columns_present"] = len(missing_columns) == 0
        else:
            checks["all_required_columns_present"] = False
            missing_columns = sorted(REQUIRED_COLUMNS)
            overall = "degraded"

        if missing_columns:
            overall = "degraded"

    except Exception as exc:
        return JSONResponse(status_code=200, content={
            "status": "error", "detail": str(exc),
        })

    # -- Static INSERT coverage check ----------------------------------------
    def _coverage(module_path: str) -> dict:
        try:
            src = _inspect.getsource(_importlib.import_module(module_path))
            missing = sorted(c for c in REQUIRED_COLUMNS if c not in src)
            return {"covered": not missing, "missing": missing}
        except Exception as exc:
            return {"covered": False, "error": str(exc)}

    checks["upload_insert_coverage"]       = _coverage("sales_support_agent.services.cashflow.upload")
    checks["clickup_sync_insert_coverage"] = _coverage("sales_support_agent.services.cashflow.clickup_sync")
    checks["obligations_insert_coverage"]  = _coverage("sales_support_agent.services.cashflow.obligations")

    if any(
        not v.get("covered", False)
        for k, v in checks.items()
        if k.endswith("_insert_coverage")
    ):
        overall = "degraded"

    return JSONResponse(status_code=200, content={
        "status": overall,
        "db_columns": db_columns,
        "missing_columns": missing_columns,
        "checks": checks,
    })


# ---------------------------------------------------------------------------
# Overview
# ---------------------------------------------------------------------------

@router.get("", response_class=HTMLResponse)
@router.get("/", response_class=HTMLResponse)
async def finance_overview(request: Request, flash: str = ""):
    if not has_finance_access(request):
        return _redirect_login()
    return await render_cashflow_overview_page(flash=flash)


# ---------------------------------------------------------------------------
# Forecast
# ---------------------------------------------------------------------------

@router.get("/forecast", response_class=HTMLResponse)
async def finance_forecast(request: Request):
    if not has_finance_access(request):
        return _redirect_login()
    return render_weekly_forecast_page()


# ---------------------------------------------------------------------------
# AP (Payables)
# ---------------------------------------------------------------------------

@router.get("/ap", response_class=HTMLResponse)
async def ap_list(request: Request, flash: str = ""):
    if not has_finance_access(request):
        return _redirect_login()
    return render_upcoming_ap_page(flash=flash)


@router.get("/ap/new", response_class=HTMLResponse)
async def ap_new_form(request: Request):
    if not has_finance_access(request):
        return _redirect_login()
    return render_ap_new_page()


@router.post("/ap/new", response_class=HTMLResponse)
async def ap_new_submit(request: Request):
    if not has_finance_access(request):
        return _redirect_login()
    form = dict(await request.form())
    kwargs = parse_obligation_form(form)
    try:
        create_obligation(event_type="outflow", **kwargs)
        return RedirectResponse("/admin/finances/ap?flash=ok:Payable+added", status_code=303)
    except Exception as exc:
        return render_ap_new_page(flash=f"err:{exc}")


@router.get("/ap/{event_id}/edit", response_class=HTMLResponse)
async def ap_edit_form(request: Request, event_id: str):
    if not has_finance_access(request):
        return _redirect_login()
    return render_ap_edit_page(event_id)


@router.post("/ap/{event_id}/edit", response_class=HTMLResponse)
async def ap_edit_submit(request: Request, event_id: str):
    if not has_finance_access(request):
        return _redirect_login()
    form = dict(await request.form())
    kwargs = parse_obligation_form(form)
    try:
        update_obligation(event_id, **kwargs)
        return RedirectResponse("/admin/finances/ap?flash=ok:Payable+updated", status_code=303)
    except Exception as exc:
        return render_ap_edit_page(event_id, flash=f"err:{exc}")


@router.post("/ap/{event_id}/delete")
async def ap_delete(request: Request, event_id: str):
    if not has_finance_access(request):
        return _redirect_login()
    delete_obligation(event_id)
    return RedirectResponse("/admin/finances/ap?flash=ok:Deleted", status_code=303)


# ---------------------------------------------------------------------------
# AR (Receivables)
# ---------------------------------------------------------------------------

@router.get("/ar", response_class=HTMLResponse)
async def ar_list(request: Request, flash: str = ""):
    if not has_finance_access(request):
        return _redirect_login()
    return render_expected_ar_page(flash=flash)


@router.get("/ar/new", response_class=HTMLResponse)
async def ar_new_form(request: Request):
    if not has_finance_access(request):
        return _redirect_login()
    return render_ar_new_page()


@router.post("/ar/new", response_class=HTMLResponse)
async def ar_new_submit(request: Request):
    if not has_finance_access(request):
        return _redirect_login()
    form = dict(await request.form())
    kwargs = parse_obligation_form(form)
    try:
        create_obligation(event_type="inflow", **kwargs)
        return RedirectResponse("/admin/finances/ar?flash=ok:Receivable+added", status_code=303)
    except Exception as exc:
        return render_ar_new_page(flash=f"err:{exc}")


@router.get("/ar/{event_id}/edit", response_class=HTMLResponse)
async def ar_edit_form(request: Request, event_id: str):
    if not has_finance_access(request):
        return _redirect_login()
    return render_ar_edit_page(event_id)


@router.post("/ar/{event_id}/edit", response_class=HTMLResponse)
async def ar_edit_submit(request: Request, event_id: str):
    if not has_finance_access(request):
        return _redirect_login()
    form = dict(await request.form())
    kwargs = parse_obligation_form(form)
    try:
        update_obligation(event_id, **kwargs)
        return RedirectResponse("/admin/finances/ar?flash=ok:Receivable+updated", status_code=303)
    except Exception as exc:
        return render_ar_edit_page(event_id, flash=f"err:{exc}")


@router.post("/ar/{event_id}/delete")
async def ar_delete(request: Request, event_id: str):
    if not has_finance_access(request):
        return _redirect_login()
    delete_obligation(event_id)
    return RedirectResponse("/admin/finances/ar?flash=ok:Deleted", status_code=303)


# ---------------------------------------------------------------------------
# Alerts
# ---------------------------------------------------------------------------

@router.get("/alerts", response_class=HTMLResponse)
async def finance_alerts(request: Request):
    if not has_finance_access(request):
        return _redirect_login()
    return render_risk_alerts_page()


# ---------------------------------------------------------------------------
# Scenario
# ---------------------------------------------------------------------------

@router.get("/scenario", response_class=HTMLResponse)
async def scenario_get(request: Request):
    if not has_finance_access(request):
        return _redirect_login()
    return render_scenario_page()


@router.post("/scenario", response_class=HTMLResponse)
async def scenario_post(request: Request):
    if not has_finance_access(request):
        return _redirect_login()
    form = dict(await request.form())
    adj = {
        "event_id": form.get("event_id", ""),
        "new_amount_dollars": form.get("new_amount_dollars") or None,
        "new_due_date": form.get("new_due_date") or None,
        "remove": bool(form.get("remove")),
    }
    return render_scenario_page(adjustments=[adj] if adj["event_id"] else None)


# ---------------------------------------------------------------------------
# Upload CSV
# ---------------------------------------------------------------------------

@router.get("/upload", response_class=HTMLResponse)
async def upload_form(request: Request):
    if not has_finance_access(request):
        return _redirect_login()
    return render_upload_page()


@router.post("/upload", response_class=HTMLResponse)
async def upload_submit(request: Request, csv_file: UploadFile = File(...)):
    if not has_finance_access(request):
        return _redirect_login()
    form = dict(await request.form())
    merge_mode = str(form.get("merge_mode", "append"))
    csv_bytes = await csv_file.read()
    result = run_csv_upload(csv_bytes, merge_mode=merge_mode)
    result_html = render_upload_result(result)
    flash = f"ok:{result.summary()}" if result.success else f"err:{'; '.join(result.errors[:2])}"
    return render_upload_page(result_html=result_html, flash=flash)


# ---------------------------------------------------------------------------
# ClickUp sync
# ---------------------------------------------------------------------------

@router.post("/sync-clickup", response_class=HTMLResponse)
async def sync_clickup(request: Request):
    if not has_finance_access(request):
        return _redirect_login()
    settings = (
        getattr(request.app.state, "agent_settings", None)
        or getattr(request.app.state, "admin_dashboard_settings", None)
        or request.app.state.settings
    )
    try:
        result = await asyncio.to_thread(sync_clickup_finance, settings)
        flash = f"ok:Synced from ClickUp — {result['created']} added · {result['updated']} updated · {result['skipped']} skipped"
    except Exception as exc:
        flash = f"err:ClickUp sync failed: {exc}"
    from urllib.parse import quote
    return RedirectResponse(f"/admin/finances?flash={quote(flash)}", status_code=303)


# ---------------------------------------------------------------------------
# Recurring templates
# ---------------------------------------------------------------------------

@router.get("/recurring", response_class=HTMLResponse)
async def recurring_list(request: Request, flash: str = ""):
    if not has_finance_access(request):
        return _redirect_login()
    return render_recurring_page(flash=flash)


@router.get("/recurring/new", response_class=HTMLResponse)
async def recurring_new_form(request: Request):
    if not has_finance_access(request):
        return _redirect_login()
    return render_recurring_new_page()


@router.post("/recurring/new", response_class=HTMLResponse)
async def recurring_new_submit(request: Request):
    if not has_finance_access(request):
        return _redirect_login()
    form = dict(await request.form())
    kwargs = parse_template_form(form)
    try:
        create_recurring_template(**kwargs)
        return RedirectResponse("/admin/finances/recurring?flash=ok:Template+created", status_code=303)
    except Exception as exc:
        return render_recurring_new_page(flash=f"err:{exc}")


@router.get("/recurring/{template_id}/edit", response_class=HTMLResponse)
async def recurring_edit_form(request: Request, template_id: str):
    if not has_finance_access(request):
        return _redirect_login()
    return render_recurring_edit_page(template_id)


@router.post("/recurring/{template_id}/edit", response_class=HTMLResponse)
async def recurring_edit_submit(request: Request, template_id: str):
    if not has_finance_access(request):
        return _redirect_login()
    form = dict(await request.form())
    kwargs = parse_template_form(form)
    try:
        update_recurring_template(template_id, **kwargs)
        return RedirectResponse("/admin/finances/recurring?flash=ok:Template+updated", status_code=303)
    except Exception as exc:
        return render_recurring_edit_page(template_id, flash=f"err:{exc}")


@router.post("/recurring/{template_id}/delete")
async def recurring_delete(request: Request, template_id: str):
    if not has_finance_access(request):
        return _redirect_login()
    delete_recurring_template(template_id)
    return RedirectResponse("/admin/finances/recurring?flash=ok:Deleted", status_code=303)


@router.post("/recurring/generate", response_class=HTMLResponse)
async def recurring_generate(request: Request):
    if not has_finance_access(request):
        return _redirect_login()
    created = generate_upcoming_from_templates(horizon_days=90)
    return RedirectResponse(
        f"/admin/finances/recurring?flash=ok:{len(created)}+obligations+generated",
        status_code=303,
    )
