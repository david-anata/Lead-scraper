"""Finance / Cashflow Controller — all routes under /admin/finance."""

from __future__ import annotations

import asyncio
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from urllib.parse import quote
from uuid import uuid4

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from sales_support_agent.services.cashflow.ap import (
    parse_obligation_form,
    render_ap_edit_page,
    render_ap_new_page,
)
from sales_support_agent.services.cashflow.ar import (
    render_ar_edit_page,
    render_ar_new_page,
)
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
)
from sales_support_agent.services.cashflow.upload import run_csv_upload
from sales_support_agent.services.cashflow.upload_page import render_upload_result
from sales_support_agent.services.auth_deps import get_current_user, require_tool
from sales_support_agent.services.cashflow.cashflow_helpers import _finance_nav_user


async def _set_finance_nav_user(request: Request) -> None:
    _finance_nav_user.set(get_current_user(request))
from sales_support_agent.services.cashflow.clickup_sync import sync_clickup_finance
from sales_support_agent.services.cashflow.qbo_sync import sync_qbo_invoices


router = APIRouter(
    prefix="/admin/finances",
    tags=["finance"],
    dependencies=[Depends(require_tool("finance")), Depends(_set_finance_nav_user)],
)


# ---------------------------------------------------------------------------
# Auth guard helper (kept for compatibility)
# ---------------------------------------------------------------------------

def _redirect_login() -> RedirectResponse:
    return RedirectResponse("/admin/login", status_code=303)


def _redirect_finance_home(message: str = "Finance now lives on one control page.") -> RedirectResponse:
    return RedirectResponse(f"/admin/finances?flash={quote(f'ok:{message}')}", status_code=303)


def _redirect_finance_error(message: str) -> RedirectResponse:
    return RedirectResponse(f"/admin/finances?flash={quote(f'err:{message}')}", status_code=303)


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
    missing_v2_columns: list = []
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
            missing_v2_columns = sorted(
                {"record_kind", "pay_priority", "minimum_payment_cents", "flexibility"}
                - set(db_columns)
            )
            checks["finance_v2_columns_present"] = not missing_v2_columns
        else:
            checks["all_required_columns_present"] = False
            checks["finance_v2_columns_present"] = False
            missing_columns = sorted(REQUIRED_COLUMNS)
            missing_v2_columns = [
                "flexibility", "minimum_payment_cents", "pay_priority", "record_kind"
            ]
            overall = "degraded"

        finance_v2_tables = {
            "payment_installments", "settlement_allocations", "finance_source_records",
            "finance_import_batches", "finance_import_rows",
        }
        checks["finance_v2_tables_present"] = finance_v2_tables.issubset(tables)

        if missing_columns or missing_v2_columns or not checks["finance_v2_tables_present"]:
            overall = "degraded"

    except Exception as exc:
        return JSONResponse(status_code=200, content={
            "status": "error", "detail": str(exc),
        })

    # -- Static INSERT coverage check ----------------------------------------
    def _coverage(*module_paths: str) -> dict:
        try:
            src = "\n".join(
                _inspect.getsource(_importlib.import_module(module_path))
                for module_path in module_paths
            )
            missing = sorted(c for c in REQUIRED_COLUMNS if c not in src)
            return {
                "covered": not missing,
                "missing": missing,
                "modules": list(module_paths),
            }
        except Exception as exc:
            return {
                "covered": False,
                "error": str(exc),
                "modules": list(module_paths),
            }

    # Bank CSV parsing lives in upload.py; the canonical staged INSERT lives in imports.py.
    bank_import_coverage = _coverage("sales_support_agent.services.cashflow.imports")
    checks["bank_import_insert_coverage"] = bank_import_coverage
    checks["upload_insert_coverage"] = bank_import_coverage
    checks["clickup_sync_insert_coverage"] = _coverage("sales_support_agent.services.cashflow.clickup_sync")
    checks["obligations_insert_coverage"]  = _coverage("sales_support_agent.services.cashflow.obligations")

    from sales_support_agent.services.cashflow.settings import get_cash_floor_health

    checks["cash_floor_settings"] = get_cash_floor_health()
    if not checks["cash_floor_settings"]["available"]:
        overall = "degraded"

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
        "missing_v2_columns": missing_v2_columns,
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


def _money_to_cents(raw_amount: str) -> int:
    """Parse an operator-entered dollar amount without float rounding."""
    try:
        amount = Decimal(str(raw_amount).replace("$", "").replace(",", "").strip())
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("Enter a valid positive amount") from exc
    cents = int((amount * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    if cents <= 0:
        raise ValueError("Amount must be greater than zero")
    return cents


@router.post("/settings/cash-floor", response_class=HTMLResponse)
async def update_cash_floor(request: Request, cash_floor: str = Form(...)):
    """Persist the minimum reserve used by every Finance calculation."""
    from sales_support_agent.services.cashflow.settings import set_cash_floor_cents

    try:
        cents = _money_to_cents(cash_floor)
        current_user = get_current_user(request)
        actor = "finance-operator"
        if isinstance(current_user, dict):
            actor = str(
                current_user.get("email")
                or current_user.get("name")
                or actor
            )
        await asyncio.to_thread(set_cash_floor_cents, cents, actor=actor)
    except ValueError as exc:
        return _redirect_finance_error(str(exc))
    except Exception:
        return _redirect_finance_error("Cash floor could not be updated")
    return _redirect_finance_home("Cash floor updated")


@router.post("/income-patterns/{pattern_key}/decision", response_class=HTMLResponse)
async def update_income_pattern_decision(
    request: Request,
    pattern_key: str,
    decision: str = Form(...),
):
    """Persist an operator decision without creating or mutating cash events."""
    from sales_support_agent.services.cashflow.income_decisions import (
        record_income_pattern_decision,
    )

    current_user = get_current_user(request)
    actor = "finance-operator"
    if isinstance(current_user, dict):
        actor = str(current_user.get("email") or current_user.get("name") or actor)

    form = await request.form()
    evidence: dict[str, object] = {}
    for field, raw_value in form.multi_items():
        value = str(raw_value).strip()
        if not value:
            continue
        if field == "evidence":
            evidence["note"] = value
        elif field.startswith("evidence_") and len(field) > len("evidence_"):
            evidence[field[len("evidence_"):]] = value

    try:
        request_id = request.headers.get("Idempotency-Key") or uuid4().hex
        await asyncio.to_thread(
            record_income_pattern_decision,
            pattern_key,
            decision,
            actor,
            evidence,
            request_id=request_id,
        )
    except ValueError as exc:
        return _redirect_finance_error(str(exc))
    except Exception:
        return _redirect_finance_error("Income pattern decision could not be recorded")
    return _redirect_finance_home("Income pattern decision recorded")


@router.post("/savings/{opportunity_key}/review", response_class=HTMLResponse)
async def record_savings_review_action(
    request: Request,
    opportunity_key: str,
    action: str = Form(...),
    evidence_hash: str = Form(...),
    opportunity_json: str = Form(...),
    reason: str = Form(""),
):
    """Store a confirmed savings disposition without mutating cash facts."""
    import json
    from sales_support_agent.services.cashflow.savings_reviews import (
        create_clickup_savings_review_task,
        record_savings_review,
    )

    current_user = get_current_user(request)
    actor = "finance-operator"
    if isinstance(current_user, dict):
        actor = str(current_user.get("email") or current_user.get("name") or actor)
    try:
        opportunity = json.loads(opportunity_json)
        if not isinstance(opportunity, dict):
            raise ValueError("Savings evidence is invalid; refresh Finance and try again")
        if opportunity.get("opportunity_key") != opportunity_key or opportunity.get("evidence_hash") != evidence_hash:
            raise ValueError("Savings evidence is stale; refresh Finance and try again")
        task = None
        if action == "follow_up":
            task = await asyncio.to_thread(create_clickup_savings_review_task, opportunity)
        result = await asyncio.to_thread(
            record_savings_review,
            opportunity,
            action,
            actor,
            reason=reason,
            request_id=request.headers.get("Idempotency-Key") or uuid4().hex,
            clickup_task=task,
        )
    except ValueError as exc:
        return _redirect_finance_error(str(exc))
    except Exception:
        return _redirect_finance_error("Savings review could not be recorded")
    messages = {
        "keep": "Savings opportunity kept for 90 days.",
        "dismiss": "Savings opportunity dismissed for 90 days.",
        "follow_up": "Savings review task created; Finance will wait for bank evidence before counting a saving.",
        "confirm_realized": "Bank-verified savings recorded.",
    }
    return _redirect_finance_home(messages.get(action, "Savings review recorded."))


@router.post("/actions/{event_id}/partial", response_class=HTMLResponse)
async def record_partial_payment(
    request: Request,
    event_id: str,
    amount: str = Form(...),
    allocation_date: str = Form(""),
    idempotency_key: str = Form(""),
):
    """Record explicitly confirmed settlement evidence for part of an obligation."""
    from sales_support_agent.services.cashflow.settlements import create_settlement_allocation

    try:
        cents = _money_to_cents(amount)
        if not idempotency_key.strip():
            raise ValueError("Confirmation token is missing; reopen the preview and try again")
        settled_on = datetime.fromisoformat(allocation_date).date() if allocation_date else None
        create_settlement_allocation(
            obligation_event_id=event_id,
            amount_cents=cents,
            allocation_date=settled_on,
            source="manual_operator",
            confidence="confirmed",
            notes="Confirmed from Finance Control",
            idempotency_key=idempotency_key,
        )
        return _redirect_finance_home("Partial payment recorded; remaining balance recalculated.")
    except Exception as exc:
        return _redirect_finance_error(f"Could not record partial payment: {exc}")


@router.post("/actions/{event_id}/installment", response_class=HTMLResponse)
async def schedule_installment(
    request: Request,
    event_id: str,
    amount: str = Form(...),
    due_date: str = Form(...),
    idempotency_key: str = Form(""),
):
    """Create one explicitly confirmed installment without changing the face amount."""
    from sales_support_agent.services.cashflow.settlements import create_payment_installment

    try:
        cents = _money_to_cents(amount)
        if not idempotency_key.strip():
            raise ValueError("Confirmation token is missing; reopen the preview and try again")
        scheduled_for = datetime.fromisoformat(due_date).date()
        create_payment_installment(
            obligation_event_id=event_id,
            amount_cents=cents,
            due_date=scheduled_for,
            idempotency_key=idempotency_key,
        )
        return _redirect_finance_home("Installment scheduled; cash paths recalculated.")
    except Exception as exc:
        return _redirect_finance_error(f"Could not schedule installment: {exc}")


# ---------------------------------------------------------------------------
# Forecast
# ---------------------------------------------------------------------------

import logging as _logging
_forecast_logger = _logging.getLogger(__name__)


@router.get("/forecast", response_class=HTMLResponse)
async def finance_forecast(request: Request):
    return _redirect_finance_home()


# ---------------------------------------------------------------------------
# AP (Payables)
# ---------------------------------------------------------------------------

@router.get("/ap", response_class=HTMLResponse)
async def ap_list(request: Request, flash: str = ""):
    return _redirect_finance_home()


@router.get("/ap/new", response_class=HTMLResponse)
async def ap_new_form(request: Request):
    return render_ap_new_page()


@router.post("/ap/new", response_class=HTMLResponse)
async def ap_new_submit(request: Request):
    form = dict(await request.form())
    kwargs = parse_obligation_form(form)
    try:
        create_obligation(event_type="outflow", **kwargs)
        return RedirectResponse("/admin/finances?flash=ok:Payable+added", status_code=303)
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
        return RedirectResponse("/admin/finances?flash=ok:Payable+updated", status_code=303)
    except Exception as exc:
        return render_ap_edit_page(event_id, flash=f"err:{exc}")


@router.post("/ap/{event_id}/delete")
async def ap_delete(request: Request, event_id: str):
    delete_obligation(event_id)
    return RedirectResponse("/admin/finances?flash=ok:Deleted", status_code=303)


# ---------------------------------------------------------------------------
# AR (Receivables)
# ---------------------------------------------------------------------------

@router.get("/ar", response_class=HTMLResponse)
async def ar_list(request: Request, flash: str = ""):
    return _redirect_finance_home()


@router.get("/ar/new", response_class=HTMLResponse)
async def ar_new_form(request: Request):
    return render_ar_new_page()


@router.post("/ar/new", response_class=HTMLResponse)
async def ar_new_submit(request: Request):
    form = dict(await request.form())
    kwargs = parse_obligation_form(form)
    try:
        create_obligation(event_type="inflow", **kwargs)
        return RedirectResponse("/admin/finances?flash=ok:Receivable+added", status_code=303)
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
        return RedirectResponse("/admin/finances?flash=ok:Receivable+updated", status_code=303)
    except Exception as exc:
        return render_ar_edit_page(event_id, flash=f"err:{exc}")


@router.post("/ar/{event_id}/delete")
async def ar_delete(request: Request, event_id: str):
    delete_obligation(event_id)
    return RedirectResponse("/admin/finances?flash=ok:Deleted", status_code=303)


# ---------------------------------------------------------------------------
# Alerts
# ---------------------------------------------------------------------------

@router.get("/alerts", response_class=HTMLResponse)
async def finance_alerts(request: Request, flash: str = ""):
    return _redirect_finance_home()


# ---------------------------------------------------------------------------
# Scenario
# ---------------------------------------------------------------------------

@router.get("/scenario", response_class=HTMLResponse)
async def scenario_get(request: Request):
    return _redirect_finance_home()


@router.post("/scenario", response_class=HTMLResponse)
async def scenario_post(request: Request):
    return _redirect_finance_home()


# ---------------------------------------------------------------------------
# Upload CSV
# ---------------------------------------------------------------------------

@router.get("/upload", response_class=HTMLResponse)
async def upload_form(request: Request):
    return _redirect_finance_home()


@router.post("/upload", response_class=HTMLResponse)
async def upload_submit(request: Request, csv_file: UploadFile = File(...)):
    form = dict(await request.form())
    merge_mode = str(form.get("merge_mode", "append"))
    csv_bytes = await csv_file.read()
    result = run_csv_upload(csv_bytes, merge_mode=merge_mode)
    result_html = render_upload_result(result)
    flash = f"ok:{result.summary()}" if result.success else f"err:{'; '.join(result.errors[:2])}"
    return await render_cashflow_overview_page(flash=flash, inline_result_html=result_html)


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
        if result.source_exceptions:
            flash += f" · {result.source_exceptions} source exception(s) need review"
        if result.errors:
            flash = f"err:ClickUp sync errors: {'; '.join(result.errors[:2])}"
    except Exception as exc:
        flash = f"err:ClickUp sync failed: {exc}"
    return RedirectResponse(f"/admin/finances?flash={quote(flash)}", status_code=303)


# ---------------------------------------------------------------------------
# Recurring templates
# ---------------------------------------------------------------------------

@router.get("/recurring", response_class=HTMLResponse)
async def recurring_list(request: Request, flash: str = ""):
    return _redirect_finance_home()


@router.get("/recurring/new", response_class=HTMLResponse)
async def recurring_new_form(request: Request):
    return render_recurring_new_page()


@router.post("/recurring/new", response_class=HTMLResponse)
async def recurring_new_submit(request: Request):
    form = dict(await request.form())
    kwargs = parse_template_form(form)
    try:
        create_recurring_template(**kwargs)
        return RedirectResponse("/admin/finances?flash=ok:Template+created", status_code=303)
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
        return RedirectResponse("/admin/finances?flash=ok:Template+updated", status_code=303)
    except Exception as exc:
        return render_recurring_edit_page(template_id, flash=f"err:{exc}")


@router.post("/recurring/{template_id}/delete")
async def recurring_delete(request: Request, template_id: str):
    delete_recurring_template(template_id)
    return RedirectResponse("/admin/finances?flash=ok:Deleted", status_code=303)


# ---------------------------------------------------------------------------
# QBO invoice sync
# ---------------------------------------------------------------------------

@router.post("/sync-qbo", response_class=HTMLResponse)
async def sync_qbo(request: Request):
    """Compatibility sync using Finance Control's canonical source policy.

    Cash on hand is only refreshed from the bank CSV.  QBO contributes dated
    receivables, while ClickUp contributes planned work.  This endpoint remains
    for old links but must not silently reintroduce QBO bank transactions.
    """
    settings = (
        getattr(request.app.state, "agent_settings", None)
        or getattr(request.app.state, "admin_dashboard_settings", None)
        or request.app.state.settings
    )
    parts: list[str] = []
    errors: list[str] = []

    # 1. ClickUp planned AP/AR.
    try:
        from sales_support_agent.services.cashflow.clickup_sync import sync_clickup_finance
        cu = await asyncio.to_thread(sync_clickup_finance, settings)
        parts.append(f"ClickUp {cu.rows_inserted} new")
        errors.extend(cu.errors[:1])
    except Exception as exc:
        errors.append(f"ClickUp: {exc}")

    # 2. QBO dated receivables. No QBO bank sync: CSV remains cash truth.
    try:
        inv = await asyncio.to_thread(sync_qbo_invoices, settings)
        parts.append(f"Invoices {inv.rows_inserted} new")
        errors.extend(inv.errors[:1])
    except Exception as exc:
        errors.append(f"Invoices: {exc}")

    if errors:
        flash = f"err:Sync issues: {'; '.join(errors[:2])}"
    else:
        flash = f"ok:Synced — {' · '.join(parts)}"

    return RedirectResponse(f"/admin/finances?flash={quote(flash)}", status_code=303)


@router.post("/sync-qbo-invoices", response_class=HTMLResponse)
async def sync_qbo_invoices_only(request: Request):
    """Refresh QBO receivables without changing the bank-CSV cash position."""
    settings = (
        getattr(request.app.state, "agent_settings", None)
        or getattr(request.app.state, "admin_dashboard_settings", None)
        or request.app.state.settings
    )
    try:
        result = await asyncio.to_thread(sync_qbo_invoices, settings)
    except Exception as exc:
        return _redirect_finance_error(f"QuickBooks receivables sync failed: {exc}")

    if result.errors:
        return _redirect_finance_error(f"QuickBooks receivables sync: {result.errors[0]}")
    return _redirect_finance_home(
        f"QuickBooks receivables refreshed: {result.rows_inserted} new, {result.rows_skipped_duplicate} unchanged."
    )


@router.post("/recurring/generate", response_class=HTMLResponse)
async def recurring_generate(request: Request):
    created = generate_upcoming_from_templates(horizon_days=90)
    return RedirectResponse(
        f"/admin/finances?flash=ok:{len(created)}+obligations+generated",
        status_code=303,
    )


# ---------------------------------------------------------------------------
# QuickBooks settings / connection status
# ---------------------------------------------------------------------------

@router.get("/qbo", response_class=HTMLResponse)
async def qbo_settings_page(request: Request, flash: str = ""):
    return _redirect_finance_home()


@router.post("/qbo/disconnect", response_class=HTMLResponse)
async def qbo_disconnect(request: Request):
    """Clear the current QBO authorization from the one-page Finance control."""
    from sales_support_agent.api.qbo_auth_router import _do_disconnect

    _do_disconnect()
    return _redirect_finance_home("QuickBooks disconnected. Reconnect the intended company when ready.")


# ---------------------------------------------------------------------------
# Reconcile — Actuals vs Planned + trend suggestions
# ---------------------------------------------------------------------------

@router.get("/reconcile", response_class=HTMLResponse)
async def reconcile_page(request: Request, flash: str = ""):
    return _redirect_finance_home()


@router.post("/reconcile/accept-pattern", response_class=HTMLResponse)
async def reconcile_accept_pattern(request: Request):
    """Turn a detected recurring pattern into a recurring_template."""
    from sales_support_agent.services.cashflow.trend_detector import accept_pattern_as_template
    form = dict(await request.form())
    try:
        accept_pattern_as_template(form)
        flash = f"ok:Template created for {form.get('normalized_vendor','pattern')}. Edit it to fine-tune the amount and due date."
    except Exception as exc:
        flash = f"err:Could not create template: {exc}"
    return RedirectResponse(
        f"/admin/finances?flash={quote(flash)}", status_code=303
    )


# ---------------------------------------------------------------------------
# Ledger
# ---------------------------------------------------------------------------

@router.get("/ledger", response_class=HTMLResponse)
async def ledger_page(request: Request):
    return _redirect_finance_home()


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
    return _redirect_finance_home()


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
    return RedirectResponse(f"/admin/finances?flash={quote('ok:Alert dismissed')}", status_code=303)


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
    return RedirectResponse(f"/admin/finances?flash={quote('ok:All alerts dismissed')}", status_code=303)
