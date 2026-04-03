"""Finance / Cashflow Controller — all routes under /admin/finance."""

from __future__ import annotations

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
    delete_obligation,
    update_obligation,
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

router = APIRouter(prefix="/admin/finances", tags=["finance"])


# ---------------------------------------------------------------------------
# Auth guard helper
# ---------------------------------------------------------------------------

def _check_auth(request: Request) -> bool:
    from sales_support_agent.config import load_settings
    from sales_support_agent.services.admin_auth import validate_admin_session_token
    settings = load_settings()
    # Try every cookie value — handles mismatched cookie-name defaults across host apps.
    # validate_admin_session_token returns False (never raises) for invalid tokens.
    return any(
        validate_admin_session_token(settings, token)
        for token in request.cookies.values()
    )


def _redirect_login() -> RedirectResponse:
    return RedirectResponse("/admin/login", status_code=303)


# ---------------------------------------------------------------------------
# Overview
# ---------------------------------------------------------------------------

@router.get("", response_class=HTMLResponse)
@router.get("/", response_class=HTMLResponse)
async def finance_overview(request: Request, flash: str = ""):
    if not _check_auth(request):
        return _redirect_login()
    return render_cashflow_overview_page(flash=flash)


# ---------------------------------------------------------------------------
# Forecast
# ---------------------------------------------------------------------------

@router.get("/forecast", response_class=HTMLResponse)
async def finance_forecast(request: Request):
    if not _check_auth(request):
        return _redirect_login()
    return render_weekly_forecast_page()


# ---------------------------------------------------------------------------
# AP (Payables)
# ---------------------------------------------------------------------------

@router.get("/ap", response_class=HTMLResponse)
async def ap_list(request: Request, flash: str = ""):
    if not _check_auth(request):
        return _redirect_login()
    return render_upcoming_ap_page(flash=flash)


@router.get("/ap/new", response_class=HTMLResponse)
async def ap_new_form(request: Request):
    if not _check_auth(request):
        return _redirect_login()
    return render_ap_new_page()


@router.post("/ap/new", response_class=HTMLResponse)
async def ap_new_submit(request: Request):
    if not _check_auth(request):
        return _redirect_login()
    form = dict(await request.form())
    kwargs = parse_obligation_form(form)
    try:
        create_obligation(db=None, event_type="outflow", **kwargs)  # type: ignore[arg-type]
        return RedirectResponse("/admin/finances/ap?flash=ok:Payable+added", status_code=303)
    except Exception as exc:
        return render_ap_new_page(flash=f"err:{exc}")


@router.get("/ap/{event_id}/edit", response_class=HTMLResponse)
async def ap_edit_form(request: Request, event_id: str):
    if not _check_auth(request):
        return _redirect_login()
    return render_ap_edit_page(event_id)


@router.post("/ap/{event_id}/edit", response_class=HTMLResponse)
async def ap_edit_submit(request: Request, event_id: str):
    if not _check_auth(request):
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
    if not _check_auth(request):
        return _redirect_login()
    delete_obligation(event_id)
    return RedirectResponse("/admin/finances/ap?flash=ok:Deleted", status_code=303)


# ---------------------------------------------------------------------------
# AR (Receivables)
# ---------------------------------------------------------------------------

@router.get("/ar", response_class=HTMLResponse)
async def ar_list(request: Request, flash: str = ""):
    if not _check_auth(request):
        return _redirect_login()
    return render_expected_ar_page(flash=flash)


@router.get("/ar/new", response_class=HTMLResponse)
async def ar_new_form(request: Request):
    if not _check_auth(request):
        return _redirect_login()
    return render_ar_new_page()


@router.post("/ar/new", response_class=HTMLResponse)
async def ar_new_submit(request: Request):
    if not _check_auth(request):
        return _redirect_login()
    form = dict(await request.form())
    kwargs = parse_obligation_form(form)
    try:
        create_obligation(db=None, event_type="inflow", **kwargs)  # type: ignore[arg-type]
        return RedirectResponse("/admin/finances/ar?flash=ok:Receivable+added", status_code=303)
    except Exception as exc:
        return render_ar_new_page(flash=f"err:{exc}")


@router.get("/ar/{event_id}/edit", response_class=HTMLResponse)
async def ar_edit_form(request: Request, event_id: str):
    if not _check_auth(request):
        return _redirect_login()
    return render_ar_edit_page(event_id)


@router.post("/ar/{event_id}/edit", response_class=HTMLResponse)
async def ar_edit_submit(request: Request, event_id: str):
    if not _check_auth(request):
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
    if not _check_auth(request):
        return _redirect_login()
    delete_obligation(event_id)
    return RedirectResponse("/admin/finances/ar?flash=ok:Deleted", status_code=303)


# ---------------------------------------------------------------------------
# Alerts
# ---------------------------------------------------------------------------

@router.get("/alerts", response_class=HTMLResponse)
async def finance_alerts(request: Request):
    if not _check_auth(request):
        return _redirect_login()
    return render_risk_alerts_page()


# ---------------------------------------------------------------------------
# Scenario
# ---------------------------------------------------------------------------

@router.get("/scenario", response_class=HTMLResponse)
async def scenario_get(request: Request):
    if not _check_auth(request):
        return _redirect_login()
    return render_scenario_page()


@router.post("/scenario", response_class=HTMLResponse)
async def scenario_post(request: Request):
    if not _check_auth(request):
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
    if not _check_auth(request):
        return _redirect_login()
    return render_upload_page()


@router.post("/upload", response_class=HTMLResponse)
async def upload_submit(request: Request, csv_file: UploadFile = File(...)):
    if not _check_auth(request):
        return _redirect_login()
    form = dict(await request.form())
    merge_mode = str(form.get("merge_mode", "append"))
    csv_bytes = await csv_file.read()
    result = run_csv_upload(csv_bytes, merge_mode=merge_mode)
    result_html = render_upload_result(result)
    flash = f"ok:{result.summary()}" if result.success else f"err:{'; '.join(result.errors[:2])}"
    return render_upload_page(result_html=result_html, flash=flash)


# ---------------------------------------------------------------------------
# Recurring templates
# ---------------------------------------------------------------------------

@router.get("/recurring", response_class=HTMLResponse)
async def recurring_list(request: Request, flash: str = ""):
    if not _check_auth(request):
        return _redirect_login()
    return render_recurring_page(flash=flash)


@router.get("/recurring/new", response_class=HTMLResponse)
async def recurring_new_form(request: Request):
    if not _check_auth(request):
        return _redirect_login()
    return render_recurring_new_page()


@router.post("/recurring/new", response_class=HTMLResponse)
async def recurring_new_submit(request: Request):
    if not _check_auth(request):
        return _redirect_login()
    form = dict(await request.form())
    kwargs = parse_template_form(form)
    from sales_support_agent.services.cashflow.obligations import create_recurring_template
    try:
        create_recurring_template(**kwargs)
        return RedirectResponse("/admin/finances/recurring?flash=ok:Template+created", status_code=303)
    except Exception as exc:
        return render_recurring_new_page(flash=f"err:{exc}")


@router.get("/recurring/{template_id}/edit", response_class=HTMLResponse)
async def recurring_edit_form(request: Request, template_id: str):
    if not _check_auth(request):
        return _redirect_login()
    return render_recurring_edit_page(template_id)


@router.post("/recurring/{template_id}/edit", response_class=HTMLResponse)
async def recurring_edit_submit(request: Request, template_id: str):
    if not _check_auth(request):
        return _redirect_login()
    form = dict(await request.form())
    kwargs = parse_template_form(form)
    from sales_support_agent.services.cashflow.obligations import update_recurring_template
    try:
        update_recurring_template(template_id, **kwargs)
        return RedirectResponse("/admin/finances/recurring?flash=ok:Template+updated", status_code=303)
    except Exception as exc:
        return render_recurring_edit_page(template_id, flash=f"err:{exc}")


@router.post("/recurring/{template_id}/delete")
async def recurring_delete(request: Request, template_id: str):
    if not _check_auth(request):
        return _redirect_login()
    from sales_support_agent.services.cashflow.obligations import delete_recurring_template
    delete_recurring_template(template_id)
    return RedirectResponse("/admin/finances/recurring?flash=ok:Deleted", status_code=303)


@router.post("/recurring/generate", response_class=HTMLResponse)
async def recurring_generate(request: Request):
    if not _check_auth(request):
        return _redirect_login()
    from sales_support_agent.services.cashflow.obligations import generate_upcoming_from_templates
    created = generate_upcoming_from_templates(horizon_days=90)
    return RedirectResponse(
        f"/admin/finances/recurring?flash=ok:{len(created)}+obligations+generated",
        status_code=303,
    )
