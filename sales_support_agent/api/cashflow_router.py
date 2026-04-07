"""Finance / Cashflow Controller — all routes under /admin/finance."""

from __future__ import annotations

import asyncio
from datetime import datetime

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

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
from sales_support_agent.services.cashflow.qbo_sync import sync_qbo_invoices
from sales_support_agent.services.cashflow.qbo_bank_sync import sync_qbo_bank_transactions

def _check_finance_access(request: Request) -> None:
    """FastAPI dependency that enforces finance access.
    Raises HTTPException(303) redirecting to login if not authorized.
    """
    if not has_finance_access(request):
        raise HTTPException(
            status_code=303,
            headers={"Location": "/admin/login"},
        )


router = APIRouter(
    prefix="/admin/finances",
    tags=["finance"],
    dependencies=[Depends(_check_finance_access)],
)


# ---------------------------------------------------------------------------
# Auth guard helper (kept for compatibility)
# ---------------------------------------------------------------------------

def _redirect_login() -> RedirectResponse:
    return RedirectResponse("/admin/login", status_code=303)


# ---------------------------------------------------------------------------
# Health check — no auth, used for post-deploy self-testing
# ---------------------------------------------------------------------------

@router.get("/health")
async def cashflow_health(request: Request):
    """
    Self-test endpoint.  Returns JSON with DB column state and static INSERT
    coverage for every cashflow write path.
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
        from sales_support_agent.models.database import get_engine
        from sqlalchemy import inspect as _sainsp

        insp = _sainsp(get_engine())
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
    # Ensure future recurring events exist — runs fast after first call
    # (subsequent calls just verify existing rows and return quickly).
    async def _expand():
        try:
            created = await asyncio.to_thread(
                generate_upcoming_from_templates, horizon_days=400, advance_template=True
            )
            if created:
                _forecast_logger.debug("[overview] template expansion: %d events created", len(created))
        except Exception as exc:
            _forecast_logger.error("[overview] template expansion failed: %s", exc, exc_info=True)
    asyncio.create_task(_expand())
    return await render_cashflow_overview_page(flash=flash)


@router.get("/chart-data")
async def chart_data(request: Request, weeks: int = 12):
    from sales_support_agent.services.cashflow.overview import _build_chart_data
    return JSONResponse(_build_chart_data(period_weeks=weeks))


@router.get("/chart-data-daily")
async def chart_data_daily(request: Request, days_back: int = 14, days_forward: int = 42):
    """Daily bar+line chart data — 14 days actual + 42 days forecast."""
    from sales_support_agent.services.cashflow.overview import _build_daily_chart_data
    return JSONResponse(await asyncio.to_thread(
        _build_daily_chart_data, days_back, days_forward,
    ))


@router.patch("/events/{event_id}")
async def patch_event(event_id: str, request: Request):
    """Update friendly_name or notes on a cash event. Called by inline edit JS."""
    from sales_support_agent.models.database import get_engine
    from sqlalchemy import text

    # Coerce to int to reject non-numeric IDs early (defence-in-depth; the
    # parameterised query below already prevents SQL injection).
    try:
        int(event_id)
    except ValueError:
        return JSONResponse({"error": "invalid event_id"}, status_code=422)

    body = await request.json()
    allowed_fields = {"friendly_name", "notes"}
    updates = {k: v for k, v in body.items() if k in allowed_fields}

    if not updates:
        return JSONResponse({"error": "no valid fields"}, status_code=400)

    now = datetime.utcnow().isoformat()
    set_clauses = ", ".join(f"{k} = :{k}" for k in updates)
    updates["event_id"] = event_id
    updates["now"] = now

    with get_engine().begin() as conn:
        result = conn.execute(
            text(f"UPDATE cash_events SET {set_clauses}, updated_at = :now WHERE id = :event_id"),
            updates
        )
        if result.rowcount == 0:
            return JSONResponse({"error": "not found"}, status_code=404)

    return JSONResponse({"ok": True})


# ---------------------------------------------------------------------------
# Forecast
# ---------------------------------------------------------------------------

import logging as _logging
_forecast_logger = _logging.getLogger(__name__)


@router.get("/forecast", response_class=HTMLResponse)
async def finance_forecast(request: Request):
    # Fire-and-forget: expand recurring templates in the background so the
    # page load is never blocked by template generation.
    async def _expand_templates():
        try:
            created = await asyncio.to_thread(
                generate_upcoming_from_templates, horizon_days=400, advance_template=True
            )
            _forecast_logger.debug(
                "[forecast] background template expansion: %d obligations created", len(created)
            )
        except Exception as exc:
            _forecast_logger.error(
                "[forecast] background template expansion failed: %s", exc, exc_info=True
            )

    asyncio.create_task(_expand_templates())
    return render_weekly_forecast_page()


# ---------------------------------------------------------------------------
# AP (Payables)
# ---------------------------------------------------------------------------

@router.get("/ap", response_class=HTMLResponse)
async def ap_list(request: Request, flash: str = ""):
    return render_upcoming_ap_page(flash=flash)


@router.get("/ap/new", response_class=HTMLResponse)
async def ap_new_form(request: Request):
    return render_ap_new_page()


@router.post("/ap/new", response_class=HTMLResponse)
async def ap_new_submit(request: Request):
    form = dict(await request.form())
    kwargs = parse_obligation_form(form)
    try:
        create_obligation(event_type="outflow", **kwargs)
        return RedirectResponse("/admin/finances/ap?flash=ok:Payable+added", status_code=303)
    except Exception as exc:
        return render_ap_new_page(flash=f"err:{exc}")


@router.get("/ap/{event_id}/edit", response_class=HTMLResponse)
async def ap_edit_form(request: Request, event_id: str):
    return render_ap_edit_page(event_id)


@router.post("/ap/{event_id}/edit", response_class=HTMLResponse)
async def ap_edit_submit(request: Request, event_id: str):
    form = dict(await request.form())
    kwargs = parse_obligation_form(form)
    try:
        update_obligation(event_id, **kwargs)
        return RedirectResponse("/admin/finances/ap?flash=ok:Payable+updated", status_code=303)
    except Exception as exc:
        return render_ap_edit_page(event_id, flash=f"err:{exc}")


@router.post("/ap/{event_id}/delete")
async def ap_delete(request: Request, event_id: str):
    delete_obligation(event_id)
    return RedirectResponse("/admin/finances/ap?flash=ok:Deleted", status_code=303)


# ---------------------------------------------------------------------------
# AR (Receivables)
# ---------------------------------------------------------------------------

@router.get("/ar", response_class=HTMLResponse)
async def ar_list(request: Request, flash: str = ""):
    return render_expected_ar_page(flash=flash)


@router.get("/ar/new", response_class=HTMLResponse)
async def ar_new_form(request: Request):
    return render_ar_new_page()


@router.post("/ar/new", response_class=HTMLResponse)
async def ar_new_submit(request: Request):
    form = dict(await request.form())
    kwargs = parse_obligation_form(form)
    try:
        create_obligation(event_type="inflow", **kwargs)
        return RedirectResponse("/admin/finances/ar?flash=ok:Receivable+added", status_code=303)
    except Exception as exc:
        return render_ar_new_page(flash=f"err:{exc}")


@router.get("/ar/{event_id}/edit", response_class=HTMLResponse)
async def ar_edit_form(request: Request, event_id: str):
    return render_ar_edit_page(event_id)


@router.post("/ar/{event_id}/edit", response_class=HTMLResponse)
async def ar_edit_submit(request: Request, event_id: str):
    form = dict(await request.form())
    kwargs = parse_obligation_form(form)
    try:
        update_obligation(event_id, **kwargs)
        return RedirectResponse("/admin/finances/ar?flash=ok:Receivable+updated", status_code=303)
    except Exception as exc:
        return render_ar_edit_page(event_id, flash=f"err:{exc}")


@router.post("/ar/{event_id}/delete")
async def ar_delete(request: Request, event_id: str):
    delete_obligation(event_id)
    return RedirectResponse("/admin/finances/ar?flash=ok:Deleted", status_code=303)


# ---------------------------------------------------------------------------
# Alerts
# ---------------------------------------------------------------------------

@router.get("/alerts", response_class=HTMLResponse)
async def finance_alerts(request: Request, flash: str = ""):
    params = dict(request.query_params)
    severity = params.get("severity", "all")
    from sales_support_agent.services.cashflow.alerts_view import render_alerts_view_page
    return HTMLResponse(render_alerts_view_page(flash=flash, severity_filter=severity))


# ---------------------------------------------------------------------------
# Scenario
# ---------------------------------------------------------------------------

@router.get("/scenario", response_class=HTMLResponse)
async def scenario_get(request: Request):
    return render_scenario_page()


@router.post("/scenario", response_class=HTMLResponse)
async def scenario_post(request: Request):
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
    return render_upload_page()


@router.post("/upload", response_class=HTMLResponse)
async def upload_submit(request: Request, csv_file: UploadFile = File(...)):
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
    settings = (
        getattr(request.app.state, "agent_settings", None)
        or getattr(request.app.state, "admin_dashboard_settings", None)
        or request.app.state.settings
    )
    try:
        result = await asyncio.to_thread(sync_clickup_finance, settings)
        flash = f"ok:Synced from ClickUp — {result.rows_inserted} added · {result.rows_skipped_duplicate} updated/skipped"
        if result.errors:
            flash = f"err:ClickUp sync errors: {'; '.join(result.errors[:2])}"
    except Exception as exc:
        flash = f"err:ClickUp sync failed: {exc}"
    from urllib.parse import quote
    return RedirectResponse(f"/admin/finances?flash={quote(flash)}", status_code=303)


# ---------------------------------------------------------------------------
# Recurring templates
# ---------------------------------------------------------------------------

@router.get("/recurring", response_class=HTMLResponse)
async def recurring_list(request: Request, flash: str = ""):
    return render_recurring_page(flash=flash)


@router.get("/recurring/new", response_class=HTMLResponse)
async def recurring_new_form(request: Request):
    return render_recurring_new_page()


@router.post("/recurring/new", response_class=HTMLResponse)
async def recurring_new_submit(request: Request):
    form = dict(await request.form())
    kwargs = parse_template_form(form)
    try:
        create_recurring_template(**kwargs)
        return RedirectResponse("/admin/finances/recurring?flash=ok:Template+created", status_code=303)
    except Exception as exc:
        return render_recurring_new_page(flash=f"err:{exc}")


@router.get("/recurring/{template_id}/edit", response_class=HTMLResponse)
async def recurring_edit_form(request: Request, template_id: str):
    return render_recurring_edit_page(template_id)


@router.post("/recurring/{template_id}/edit", response_class=HTMLResponse)
async def recurring_edit_submit(request: Request, template_id: str):
    form = dict(await request.form())
    kwargs = parse_template_form(form)
    try:
        update_recurring_template(template_id, **kwargs)
        return RedirectResponse("/admin/finances/recurring?flash=ok:Template+updated", status_code=303)
    except Exception as exc:
        return render_recurring_edit_page(template_id, flash=f"err:{exc}")


@router.post("/recurring/{template_id}/delete")
async def recurring_delete(request: Request, template_id: str):
    delete_recurring_template(template_id)
    return RedirectResponse("/admin/finances/recurring?flash=ok:Deleted", status_code=303)


# ---------------------------------------------------------------------------
# QBO invoice sync
# ---------------------------------------------------------------------------

@router.post("/sync-qbo", response_class=HTMLResponse)
async def sync_qbo(request: Request):
    """Full finance sync: ClickUp templates → template expansion → QBO invoices → QBO bank."""
    settings = (
        getattr(request.app.state, "agent_settings", None)
        or getattr(request.app.state, "admin_dashboard_settings", None)
        or request.app.state.settings
    )
    parts: list[str] = []
    errors: list[str] = []

    # 1. ClickUp sync → recurring templates
    try:
        from sales_support_agent.services.cashflow.clickup_sync import sync_clickup_finance
        cu = await asyncio.to_thread(sync_clickup_finance, settings)
        parts.append(f"ClickUp {cu.rows_inserted} new")
        errors.extend(cu.errors[:1])
    except Exception as exc:
        errors.append(f"ClickUp: {exc}")

    # 2. Template expansion → fill 400-day horizon
    try:
        expanded = await asyncio.to_thread(
            generate_upcoming_from_templates, horizon_days=400, advance_template=True,
        )
        parts.append(f"{len(expanded)} events")
    except Exception as exc:
        errors.append(f"Templates: {exc}")

    # 3. QBO invoice sync (AR planned events)
    try:
        inv = await asyncio.to_thread(sync_qbo_invoices, settings)
        parts.append(f"Invoices {inv.rows_inserted} new")
        errors.extend(inv.errors[:1])
    except Exception as exc:
        errors.append(f"Invoices: {exc}")

    # 4. QBO bank sync (posted actuals — replaces manual CSV upload)
    try:
        bank = await asyncio.to_thread(sync_qbo_bank_transactions, settings)
        parts.append(f"Bank {bank.rows_inserted} new")
        errors.extend(bank.errors[:1])
    except Exception as exc:
        errors.append(f"Bank: {exc}")

    if errors:
        flash = f"err:Sync issues: {'; '.join(errors[:2])}"
    else:
        flash = f"ok:Synced — {' · '.join(parts)}"

    from urllib.parse import quote
    return RedirectResponse(f"/admin/finances?flash={quote(flash)}", status_code=303)


@router.post("/recurring/generate", response_class=HTMLResponse)
async def recurring_generate(request: Request):
    created = generate_upcoming_from_templates(horizon_days=90)
    return RedirectResponse(
        f"/admin/finances/recurring?flash=ok:{len(created)}+obligations+generated",
        status_code=303,
    )


# ---------------------------------------------------------------------------
# QuickBooks settings / connection status
# ---------------------------------------------------------------------------

@router.get("/qbo", response_class=HTMLResponse)
async def qbo_settings_page(request: Request, flash: str = ""):
    from sales_support_agent.services.cashflow.qbo_settings import render_qbo_settings_page
    return HTMLResponse(render_qbo_settings_page(flash=flash))


# ---------------------------------------------------------------------------
# Reconcile — Actuals vs Planned + trend suggestions
# ---------------------------------------------------------------------------

@router.get("/reconcile", response_class=HTMLResponse)
async def reconcile_page(request: Request, flash: str = ""):
    from sales_support_agent.services.cashflow.reconcile import render_reconcile_page
    return HTMLResponse(render_reconcile_page(flash=flash))


@router.post("/reconcile/accept-pattern", response_class=HTMLResponse)
async def reconcile_accept_pattern(request: Request):
    """Turn a detected recurring pattern into a recurring_template."""
    from sales_support_agent.services.cashflow.trend_detector import accept_pattern_as_template
    from urllib.parse import quote
    form = dict(await request.form())
    try:
        accept_pattern_as_template(form)
        flash = f"ok:Template created for {form.get('normalized_vendor','pattern')}. Edit it to fine-tune the amount and due date."
    except Exception as exc:
        flash = f"err:Could not create template: {exc}"
    return RedirectResponse(
        f"/admin/finances/reconcile?flash={quote(flash)}", status_code=303
    )


# ---------------------------------------------------------------------------
# Ledger
# ---------------------------------------------------------------------------

@router.get("/ledger", response_class=HTMLResponse)
async def ledger_page(request: Request, **kwargs):
    from sales_support_agent.services.cashflow.ledger import render_ledger_page
    params = dict(request.query_params)
    return HTMLResponse(render_ledger_page(
        from_date=params.get("from"),
        to_date=params.get("to"),
        filter_type=params.get("filter", "all"),
    ))


@router.get("/ledger/export")
async def ledger_export(request: Request):
    from sales_support_agent.services.cashflow.obligations import list_obligations
    from sales_support_agent.services.cashflow.cashflow_helpers import _display_name
    import csv, io

    params = dict(request.query_params)
    from_date = params.get("from") or datetime.utcnow().date().replace(day=1).isoformat()
    to_date = params.get("to") or datetime.utcnow().date().isoformat()
    filter_type = params.get("filter", "all")

    all_rows = list_obligations(limit=5000)
    filtered = [
        r for r in all_rows
        if str(r.get("due_date",""))[:10] >= from_date
        and str(r.get("due_date",""))[:10] <= to_date
    ]
    if filter_type == "income":
        filtered = [r for r in filtered if r.get("event_type") == "inflow"]
    elif filter_type == "expenses":
        filtered = [r for r in filtered if r.get("event_type") == "outflow"]
    filtered.sort(key=lambda r: str(r.get("due_date","")))

    csv_rows_sorted = sorted(
        [r for r in all_rows if r.get("source")=="csv" and r.get("account_balance_cents") is not None
         and str(r.get("due_date",""))[:10] <= from_date],
        key=lambda r: str(r.get("due_date",""))
    )
    running = int(csv_rows_sorted[-1].get("account_balance_cents",0)) if csv_rows_sorted else 0

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Date","Income","Expenses","Description","Running Total","Notes"])
    for row in filtered:
        is_in = row.get("event_type") == "inflow"
        amt = row.get("amount_cents",0) / 100
        running += int(row.get("amount_cents",0)) if is_in else -int(row.get("amount_cents",0))
        writer.writerow([
            str(row.get("due_date",""))[:10],
            f"{amt:.2f}" if is_in else "",
            f"{amt:.2f}" if not is_in else "",
            _display_name(row),
            f"{running/100:.2f}",
            row.get("notes",""),
        ])

    from fastapi.responses import Response
    return Response(
        content=output.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="ledger-{from_date}-to-{to_date}.csv"'}
    )


# ---------------------------------------------------------------------------
# Calendar
# ---------------------------------------------------------------------------

@router.get("/calendar", response_class=HTMLResponse)
async def calendar_page(request: Request):
    from sales_support_agent.services.cashflow.calendar_view import render_calendar_page
    # Kick off template expansion so the current and upcoming months are populated.
    async def _expand():
        try:
            await asyncio.to_thread(
                generate_upcoming_from_templates, horizon_days=400, advance_template=True
            )
        except Exception as exc:
            _forecast_logger.error("[calendar] template expansion failed: %s", exc, exc_info=True)
    asyncio.create_task(_expand())
    params = dict(request.query_params)
    year = int(params["year"]) if params.get("year") else None
    month = int(params["month"]) if params.get("month") else None
    return HTMLResponse(render_calendar_page(
        year=year,
        month=month,
        filter_type=params.get("filter", "all"),
    ))


# ---------------------------------------------------------------------------
# Alert dismiss
# ---------------------------------------------------------------------------

@router.post("/alerts/dismiss/{alert_id}", response_class=HTMLResponse)
async def dismiss_alert(alert_id: str, request: Request):
    from sales_support_agent.models.database import get_engine
    from sqlalchemy import text
    now = datetime.utcnow().isoformat()
    with get_engine().begin() as conn:
        conn.execute(text("""
            INSERT INTO kv_store (key, value, updated_at) VALUES (:key, 'dismissed', :now)
            ON CONFLICT(key) DO UPDATE SET value='dismissed', updated_at=excluded.updated_at
        """), {"key": f"alert_dismissed:{alert_id}", "now": now})
    from urllib.parse import quote
    return RedirectResponse(f"/admin/finances/alerts?flash={quote('ok:Alert dismissed')}", status_code=303)


@router.post("/alerts/dismiss-all", response_class=HTMLResponse)
async def dismiss_all_alerts(request: Request):
    from sales_support_agent.models.database import get_engine
    from sqlalchemy import text
    now = datetime.utcnow().isoformat()
    with get_engine().begin() as conn:
        conn.execute(text("""
            INSERT INTO kv_store (key, value, updated_at) VALUES ('alerts_bulk_dismissed_at', :now, :now)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
        """), {"now": now})
    from urllib.parse import quote
    return RedirectResponse(f"/admin/finances/alerts?flash={quote('ok:All alerts dismissed')}", status_code=303)
