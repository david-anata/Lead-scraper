"""Finance / Cashflow Controller — all routes under /admin/finance."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from functools import lru_cache
from typing import Any
from contextvars import ContextVar
from urllib.parse import quote, urlparse
from uuid import uuid4

from fastapi import APIRouter, BackgroundTasks, Depends, File, Form, HTTPException, Request, UploadFile
import requests
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response

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
from sales_support_agent.services.cashflow.qbo_bank_sync import sync_qbo_bank_transactions
from sales_support_agent.services.cashflow.qbo_sync import sync_qbo_invoices


logger = logging.getLogger(__name__)


def _finance_settings(request: Request) -> Any:
    """Return the full agent settings used by Finance integrations.

    The root application keeps its legacy lead-scraper settings in
    ``app.state.settings`` and the modular agent settings in
    ``app.state.agent_settings``. Finance integrations (including Plaid) are
    configured on the latter. The fallback preserves compatibility with the
    standalone sales-support app and focused route tests.
    """
    return getattr(request.app.state, "agent_settings", None) or request.app.state.settings


def _plaid_client_user_id(user: Any) -> str:
    """Return a stable opaque Plaid user ID without transmitting PII."""
    if isinstance(user, dict):
        identity = str(user.get("id") or user.get("email") or "finance-operator")
    else:
        identity = str(getattr(user, "id", "") or getattr(user, "email", "") or "finance-operator")
    digest = hashlib.sha256(identity.strip().lower().encode("utf-8")).hexdigest()
    return f"finance-{digest[:32]}"


_CURRENT_REQUEST: ContextVar[Optional[Request]] = ContextVar("finance_current_request", default=None)


async def _track_finance_request(request: Request) -> None:
    """Remember the request so a redirect can return to the page it came from."""
    _CURRENT_REQUEST.set(request)


router = APIRouter(
    prefix="/admin/finances",
    tags=["finance"],
    dependencies=[
        Depends(require_tool("finance")),
        Depends(_set_finance_nav_user),
        Depends(_track_finance_request),
    ],
)

plaid_webhook_router = APIRouter(tags=["plaid"])

_PLAID_LINK_SDK_URL = "https://cdn.plaid.com/link/v2/stable/link-initialize.js"


@lru_cache(maxsize=1)
def _load_plaid_link_sdk() -> bytes:
    """Fetch and cache Plaid's official Link loader for first-party delivery."""
    response = requests.get(_PLAID_LINK_SDK_URL, timeout=20)
    response.raise_for_status()
    return response.content


@plaid_webhook_router.get("/api/integrations/plaid/link-initialize.js")
async def plaid_link_sdk() -> Response:
    """Serve Plaid Link through Agent when privacy tools block the CDN URL."""
    try:
        content = await asyncio.to_thread(_load_plaid_link_sdk)
    except requests.RequestException as exc:
        logger.warning("Plaid Link loader could not be fetched: %s", exc)
        raise HTTPException(status_code=503, detail="Secure bank connection loader is unavailable") from exc
    return Response(
        content=content,
        media_type="text/javascript",
        headers={"Cache-Control": "public, max-age=3600, stale-if-error=86400"},
    )


@plaid_webhook_router.post("/api/integrations/plaid/webhook")
async def plaid_webhook(request: Request, background_tasks: BackgroundTasks):
    """Authenticate a Plaid webhook, record it, and queue an idempotent sync."""
    from sales_support_agent.services.cashflow.plaid import (
        PlaidClient,
        PlaidError,
        record_webhook,
        sync_item,
        verify_webhook,
    )

    settings = _finance_settings(request)
    raw_body = await request.body()
    signed_jwt = request.headers.get("Plaid-Verification", "")
    client = PlaidClient(settings)
    try:
        verify_webhook(raw_body, signed_jwt, client=client)
        payload = json.loads(raw_body)
    except (PlaidError, json.JSONDecodeError) as exc:
        code = exc.code if isinstance(exc, PlaidError) else "invalid_json"
        raise HTTPException(status_code=401, detail={"code": code, "message": "Webhook verification failed"}) from exc
    payload_environment = str(payload.get("environment") or "").lower()
    expected_environment = str(settings.plaid_environment or "sandbox").lower()
    if payload_environment and payload_environment != expected_environment:
        raise HTTPException(status_code=401, detail={"code": "environment_mismatch", "message": "Webhook verification failed"})
    webhook_type = str(payload.get("webhook_type") or "").upper()
    webhook_code = str(payload.get("webhook_code") or "").upper()
    external_item_id = str(payload.get("item_id") or "")
    error = payload.get("error") or {}
    error_code = str(error.get("error_code") or "") if isinstance(error, dict) else ""
    if webhook_type == "ITEM" and webhook_code in {"PENDING_DISCONNECT", "PENDING_EXPIRATION"}:
        error_code = webhook_code
    local_item_id = record_webhook(external_item_id, error_code=error_code) if external_item_id else None
    should_sync = webhook_type == "TRANSACTIONS" or (
        webhook_type == "ITEM" and webhook_code in {"LOGIN_REPAIRED", "NEW_ACCOUNTS_AVAILABLE"}
    )
    if local_item_id and should_sync:
        background_tasks.add_task(sync_item, local_item_id, settings=settings)
    return JSONResponse({"status": "accepted"})


# ---------------------------------------------------------------------------
# Auth guard helper (kept for compatibility)
# ---------------------------------------------------------------------------

def _redirect_login() -> RedirectResponse:
    return RedirectResponse("/admin/login", status_code=303)


def _safe_return_path(default: str = "/admin/finances") -> str:
    """Where to send the operator after an action: back where they were.

    Every action used to bounce to the finance home page, so dismissing one
    audit finding meant navigating back and scrolling again. The destination is
    taken from an explicit return_to field, or the page that submitted the form,
    and is only honoured when it points inside Finance on this host. Anything
    else falls back, so this cannot be used to redirect somewhere unexpected.
    """
    request = _CURRENT_REQUEST.get()
    if request is None:
        return default

    candidates: list[str] = []
    explicit = ""
    try:
        explicit = str(getattr(request.state, "finance_return_to", "") or "")
    except Exception:
        explicit = ""
    if explicit:
        candidates.append(str(explicit))
    candidates.append(str(request.headers.get("referer") or ""))

    for candidate in candidates:
        if not candidate:
            continue
        parsed = urlparse(candidate)
        if parsed.scheme and parsed.netloc and parsed.netloc != request.url.netloc:
            continue  # another host, never follow it
        path = parsed.path or ""
        if not path.startswith("/admin/finances"):
            continue
        fragment = f"#{parsed.fragment}" if parsed.fragment else ""
        return f"{path}{fragment}"
    return default


def _redirect_with_flash(message: str, *, level: str = "ok") -> RedirectResponse:
    """Flash a message and return to the originating page, keeping any anchor."""
    target = _safe_return_path()
    path, _, fragment = target.partition("#")
    separator = "&" if "?" in path else "?"
    suffix = f"#{fragment}" if fragment else ""
    return RedirectResponse(
        f"{path}{separator}flash={quote(f'{level}:{message}')}{suffix}",
        status_code=303,
    )


def _redirect_finance_home(message: str = "Finance now lives on one control page.") -> RedirectResponse:
    return _redirect_with_flash(message)


def _redirect_review_home(message: str) -> RedirectResponse:
    return _redirect_with_flash(message)


def _redirect_review_error(message: str) -> RedirectResponse:
    return _redirect_with_flash(message, level="err")


def _redirect_finance_error(message: str) -> RedirectResponse:
    return _redirect_with_flash(message, level="err")


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
        from sales_support_agent.models.database import (
            _ensure_finance_settlement_tables,
            ensure_finance_trust_schema,
            get_engine,
        )
        from sqlalchemy import inspect as _sainsp

        db_engine = get_engine()
        _ensure_finance_settlement_tables(db_engine)
        ensure_finance_trust_schema(db_engine)
        insp = _sainsp(db_engine)
        tables = set(insp.get_table_names())
        checks["cash_events_table_exists"] = "cash_events" in tables

        if checks["cash_events_table_exists"]:
            db_columns = sorted(c["name"] for c in insp.get_columns("cash_events"))
            missing_columns = sorted(REQUIRED_COLUMNS - set(db_columns))
            checks["all_required_columns_present"] = len(missing_columns) == 0
            missing_v2_columns = sorted(
                {
                    "record_kind", "pay_priority", "minimum_payment_cents", "flexibility",
                    "commitment_type", "workflow_status", "owner", "approval_status",
                    "created_by", "archived_at",
                }
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
        savings_review_tables = {"finance_savings_reviews", "finance_savings_review_events"}
        checks["savings_review_tables_present"] = savings_review_tables.issubset(tables)
        checks["native_commitment_columns_present"] = not any(
            column in missing_v2_columns for column in {
                "commitment_type", "workflow_status", "owner", "approval_status",
                "created_by", "archived_at",
            }
        )
        checks["plaid_tables_present"] = {"plaid_items", "plaid_accounts"}.issubset(tables)

        if (
            missing_columns
            or missing_v2_columns
            or not checks["finance_v2_tables_present"]
            or not checks["savings_review_tables_present"]
            or not checks["native_commitment_columns_present"]
            or not checks["plaid_tables_present"]
        ):
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
                generate_upcoming_from_templates, horizon_days=45, advance_template=True
            )
            if created:
                _forecast_logger.debug("[overview] template expansion: %d events created", len(created))
        except Exception as exc:
            _forecast_logger.error("[overview] template expansion failed: %s", exc, exc_info=True)
    asyncio.create_task(_expand())
    async def _refresh_stale_plaid():
        try:
            from sales_support_agent.services.cashflow.plaid import (
                stale_connected_item_ids,
                sync_connected_items,
            )
            item_ids = await asyncio.to_thread(stale_connected_item_ids, max_age_hours=6)
            if item_ids:
                await asyncio.to_thread(
                    sync_connected_items,
                    settings=_finance_settings(request), item_ids=item_ids,
                )
        except Exception as exc:
            _forecast_logger.warning("[overview] background Plaid refresh failed: %s", exc)
    asyncio.create_task(_refresh_stale_plaid())
    return await render_cashflow_overview_page(flash=flash, settings=_finance_settings(request))


@router.get("/plaid/oauth-return", response_class=HTMLResponse)
async def plaid_oauth_return(request: Request):
    """Render Finance at Plaid's exact OAuth return URL so Link can resume."""
    return await render_cashflow_overview_page(settings=_finance_settings(request))


@router.post("/plaid/link-token")
async def plaid_link_token(request: Request):
    """Create a short-lived, Transactions-only Plaid Link token."""
    from sales_support_agent.services.cashflow.plaid import PlaidClient, PlaidError

    user = get_current_user(request) or {}
    client_user_id = _plaid_client_user_id(user)
    try:
        token = await asyncio.to_thread(
            PlaidClient(_finance_settings(request)).create_link_token,
            client_user_id=client_user_id,
        )
    except PlaidError as exc:
        raise HTTPException(status_code=503, detail={"code": exc.code, "message": str(exc)}) from exc
    logger.info("Plaid Link token created mode=create actor=%s", _plaid_client_user_id(user))
    return JSONResponse({"link_token": token})


@router.post("/plaid/exchange")
async def plaid_exchange(request: Request):
    """Exchange Link's public token, seal it, and perform the first sync."""
    from sales_support_agent.services.cashflow.plaid import (
        PlaidClient, PlaidError, store_item, sync_item,
    )

    body = await request.json()
    public_token = str(body.get("public_token") or "").strip()
    if not public_token:
        raise HTTPException(status_code=400, detail="public_token is required")
    settings = _finance_settings(request)
    user = get_current_user(request) or {}
    actor = str(user.get("email") or user.get("id") or "finance-operator")
    link_session_id = str(body.get("link_session_id") or "")
    client = PlaidClient(settings)
    try:
        exchanged = await asyncio.to_thread(client.exchange_public_token, public_token)
        local_item_id = await asyncio.to_thread(
            store_item,
            item_id=exchanged["item_id"], access_token=exchanged["access_token"],
            token_secret=settings.plaid_token_secret, actor=actor,
            institution_id=str(body.get("institution_id") or ""),
            display_name=str(body.get("institution_name") or ""),
        )
        result = await asyncio.to_thread(sync_item, local_item_id, settings=settings, client=client)
    except PlaidError as exc:
        raise HTTPException(status_code=503, detail={"code": exc.code, "message": str(exc)}) from exc
    logger.info(
        "Plaid Link exchange complete item_id=%s link_session_id=%s actor=%s",
        local_item_id, link_session_id, actor,
    )
    return JSONResponse({"status": "ok", "item_id": local_item_id, "sync": result})


@router.post("/plaid/items/{item_id}/refresh")
async def plaid_refresh_item(request: Request, item_id: str):
    from sales_support_agent.services.cashflow.plaid import PlaidError, sync_item

    try:
        result = await asyncio.to_thread(sync_item, item_id, settings=_finance_settings(request))
    except PlaidError as exc:
        raise HTTPException(status_code=503, detail={"code": exc.code, "message": str(exc)}) from exc
    return JSONResponse({"status": "ok", "sync": result})


@router.post("/plaid/refresh")
async def plaid_refresh_all(request: Request):
    """Refresh every healthy bank connection without letting one block another."""
    from sales_support_agent.services.cashflow.plaid import sync_connected_items

    result = await asyncio.to_thread(
        sync_connected_items, settings=_finance_settings(request),
    )
    status = "ok" if not result["failed"] else "attention"
    return JSONResponse({"status": status, **result})


@router.post("/plaid/items/{item_id}/link-token")
async def plaid_update_link_token(request: Request, item_id: str):
    """Create a Plaid update-mode session for an expired bank login."""
    from sales_support_agent.services.cashflow.plaid import (
        PlaidError,
        create_update_link_token,
    )

    user = get_current_user(request) or {}
    try:
        token = await asyncio.to_thread(
            create_update_link_token,
            item_id,
            client_user_id=_plaid_client_user_id(user),
            settings=_finance_settings(request),
        )
    except PlaidError as exc:
        raise HTTPException(status_code=503, detail={"code": exc.code, "message": str(exc)}) from exc
    return JSONResponse({"link_token": token, "mode": "update", "item_id": item_id})


@router.post("/plaid/items/{item_id}/disconnect")
async def plaid_disconnect_item(
    request: Request, item_id: str, force: bool = Form(False), return_to: str = Form(""),
):
    """Revoke a bank connection and destroy its reusable Plaid credential.

    ``force`` clears a stuck connection locally without waiting for Plaid to
    confirm removal, for the case where Plaid can never confirm (an invalid or
    wrong-environment credential).
    """
    from sales_support_agent.services.cashflow.plaid import PlaidError, disconnect_item

    request.state.finance_return_to = return_to
    user = get_current_user(request) or {}
    actor = str(user.get("email") or user.get("id") or "finance-operator")
    try:
        await asyncio.to_thread(
            disconnect_item, item_id, settings=_finance_settings(request),
            actor=actor, force=force,
        )
    except PlaidError as exc:
        return _redirect_finance_error(
            "Plaid could not confirm removal, so the bank was kept. If it stays "
            "stuck, use Force remove to clear it here."
        )
    return _redirect_finance_home(
        "Bank disconnected. Its Plaid credential was removed and it will no longer refresh."
    )


@router.post("/plaid/accounts/{account_id}/cash-role")
async def plaid_set_account_cash_role(
    request: Request, account_id: str, role: str = Form(...), return_to: str = Form(""),
):
    """Reclassify one bank account as spendable cash, reserve, or excluded."""
    from sales_support_agent.services.cashflow.accounts_view import set_cash_role

    request.state.finance_return_to = return_to

    user = get_current_user(request) or {}
    actor = str(user.get("email") or user.get("id") or "finance-operator")
    try:
        applied = await asyncio.to_thread(set_cash_role, account_id, role, actor=actor)
    except ValueError as exc:
        return _redirect_finance_error(f"That account could not be updated: {exc}")
    label = {"spendable": "spendable cash", "reserve": "savings/reserve", "excluded": "not counted"}.get(applied, applied)
    return _redirect_finance_home(f"Account updated. It now counts as {label}.")


def _vendor_form_data(form: Any) -> dict:
    return {
        "name": form.get("name", ""),
        "terms_type": form.get("terms_type", "recurring"),
        "payment_amount_cents": _money_to_cents(form.get("payment_amount", "") or "") or None,
        "frequency": form.get("frequency", "month"),
        "total_committed_cents": _money_to_cents(form.get("total_committed", "") or "") or None,
        "start_date": form.get("start_date", ""),
        "end_date": form.get("end_date", ""),
        "match_terms": form.get("match_terms", ""),
        "running_account": str(form.get("running_account", "")).lower() in {"true", "on", "1", "yes"},
        "notes": form.get("notes", ""),
    }


@router.post("/vendors")
async def create_vendor_endpoint(request: Request):
    from sales_support_agent.services.cashflow.vendors import create_vendor
    form = await request.form()
    try:
        await asyncio.to_thread(create_vendor, _vendor_form_data(form))
    except ValueError as exc:
        return _redirect_finance_error(f"Vendor could not be saved: {exc}")
    return _redirect_finance_home("Vendor added.")


@router.post("/vendors/{vendor_id}")
async def update_vendor_endpoint(request: Request, vendor_id: str):
    from sales_support_agent.services.cashflow.vendors import update_vendor
    form = await request.form()
    try:
        await asyncio.to_thread(update_vendor, vendor_id, _vendor_form_data(form))
    except ValueError as exc:
        return _redirect_finance_error(f"Vendor could not be updated: {exc}")
    return _redirect_finance_home("Vendor updated.")


@router.post("/vendors/{vendor_id}/delete")
async def delete_vendor_endpoint(request: Request, vendor_id: str):
    from sales_support_agent.services.cashflow.vendors import deactivate_vendor
    await asyncio.to_thread(deactivate_vendor, vendor_id)
    return _redirect_finance_home("Vendor removed.")


@router.post("/collections/contact")
async def collections_contact_endpoint(
    request: Request,
    customer_key: str = Form(...),
    email: str = Form(""),
    phone: str = Form(""),
):
    """Save where to reach one customer about overdue money."""
    from sales_support_agent.services.cashflow.collections import set_contact

    try:
        await asyncio.to_thread(set_contact, customer_key, email=email, phone=phone)
    except ValueError as exc:
        return _redirect_finance_error(f"Could not save that contact: {exc}")
    return _redirect_finance_home("Contact saved.")


@router.post("/collections/send")
async def collections_send_endpoint(
    request: Request,
    customer_key: str = Form(...),
    subject: str = Form(...),
    body: str = Form(...),
    mode: str = Form("real"),
):
    """Send one reminder email. One customer per click; never in bulk."""
    from sales_support_agent.services.cashflow.collections import send_email_reminder

    user = get_current_user(request) or {}
    actor = str(user.get("email") or user.get("id") or "finance-operator")
    to_override = actor if str(mode) == "test" and "@" in actor else ""
    if str(mode) == "test" and not to_override:
        return _redirect_finance_error("No address to send your test to.")
    try:
        result = await asyncio.to_thread(
            send_email_reminder,
            customer_key,
            subject=subject,
            body=body,
            settings=_finance_settings(request),
            actor=actor,
            to_override=to_override,
            force=str(mode) == "resend",
        )
    except ValueError as exc:
        return _redirect_finance_error(str(exc))
    if result.get("test"):
        return _redirect_finance_home(f"Test sent to {result['recipient']}.")
    return _redirect_finance_home(f"Reminder sent to {result['recipient']}.")


@router.post("/bookkeeping/file-all")
async def bookkeeping_file_all_endpoint(request: Request):
    """File every transaction that can be filed confidently."""
    from sales_support_agent.services.cashflow.bookkeeping import file_transactions

    result = await asyncio.to_thread(file_transactions)
    filed = result["filed_by_rule"] + result["filed_by_keyword"]
    return _redirect_finance_home(
        f"Filed {filed} transaction(s); {result['needs_decision']} still need a decision."
    )


@router.post("/bookkeeping/file")
async def bookkeeping_file_endpoint(
    request: Request,
    event_id: str = Form(...),
    category: str = Form(...),
    always: bool = Form(False),
):
    """File one transaction, optionally teaching a rule for next time."""
    from sales_support_agent.services.cashflow.bookkeeping import file_transaction

    user = get_current_user(request) or {}
    actor = str(user.get("email") or user.get("id") or "finance-operator")
    try:
        result = await asyncio.to_thread(
            file_transaction, event_id, category, always=always, actor=actor,
        )
    except ValueError as exc:
        return _redirect_finance_error(f"Could not file that: {exc}")
    if result["rule_id"]:
        return _redirect_finance_home("Filed, and it will file itself from now on.")
    return _redirect_finance_home("Filed.")


@router.post("/bookkeeping/file-merchant")
async def bookkeeping_file_merchant_endpoint(
    request: Request, key: str = Form(...), category: str = Form(...),
):
    """File every unfiled transaction from one merchant in a single decision."""
    from sales_support_agent.services.cashflow.bookkeeping import file_merchant

    user = get_current_user(request) or {}
    actor = str(user.get("email") or user.get("id") or "finance-operator")
    try:
        result = await asyncio.to_thread(file_merchant, key, category, actor=actor)
    except ValueError as exc:
        return _redirect_finance_error(f"Could not file those: {exc}")
    return _redirect_finance_home(
        f"Filed {result['filed']} transaction(s) as {result['category']}; "
        "this merchant will file itself from now on."
    )


@router.post("/bookkeeping/rules/{rule_id}/delete")
async def bookkeeping_rule_delete_endpoint(request: Request, rule_id: str):
    from sales_support_agent.services.cashflow.bookkeeping import delete_rule

    await asyncio.to_thread(delete_rule, rule_id)
    return _redirect_finance_home("Rule removed.")


@router.post("/plan/move")
async def plan_move_endpoint(
    request: Request, event_id: str = Form(...), direction: str = Form(...),
):
    """Move one bill up or down in the suggested pay order."""
    from sales_support_agent.services.cashflow.todays_plan import move_in_pay_order

    try:
        await asyncio.to_thread(move_in_pay_order, event_id, direction)
    except ValueError as exc:
        return _redirect_finance_error(f"Could not reorder: {exc}")
    return _redirect_finance_home("Pay order updated.")


@router.post("/plan/order/reset")
async def plan_order_reset_endpoint(request: Request):
    """Go back to the automatic pay order."""
    from sales_support_agent.services.cashflow.todays_plan import clear_manual_pay_order

    cleared = await asyncio.to_thread(clear_manual_pay_order)
    return _redirect_finance_home(f"Back to the automatic order ({cleared} cleared).")


@router.get("/audit", response_class=HTMLResponse)
async def finance_audit_page(request: Request, flash: str = ""):
    from sales_support_agent.services.cashflow.finance_pages import render_audit_page

    return HTMLResponse(await asyncio.to_thread(render_audit_page, flash=flash))


@router.get("/bookkeeping", response_class=HTMLResponse)
async def finance_bookkeeping_page(request: Request, flash: str = ""):
    from sales_support_agent.services.cashflow.finance_pages import render_bookkeeping_page

    return HTMLResponse(await asyncio.to_thread(render_bookkeeping_page, flash=flash))


@router.get("/collections", response_class=HTMLResponse)
async def finance_collections_page(request: Request, flash: str = ""):
    from sales_support_agent.services.cashflow.finance_pages import render_collections_page

    return HTMLResponse(await asyncio.to_thread(render_collections_page, flash=flash))


@router.get("/setup", response_class=HTMLResponse)
async def finance_setup_page(request: Request, flash: str = ""):
    from sales_support_agent.services.cashflow.finance_pages import render_setup_page

    return HTMLResponse(await asyncio.to_thread(render_setup_page, flash=flash))


def _render_cutover_page(flash: str = "") -> str:
    """The cutover verdict card wrapped in the standard Finance page."""
    from sales_support_agent.services.cashflow.cutover import render_cutover_readiness
    from sales_support_agent.services.cashflow.finance_nav import render_finance_nav
    from sales_support_agent.services.cashflow.overview import _page_shell

    title = "Switching off the old bill list"
    body = (
        render_finance_nav("setup")
        + f"<h1>{title}</h1>"
        + '<p class="page-sub">What would happen to your forecast if the old ClickUp bill '
        "list went away today. Nothing on this page switches anything off.</p>"
        + render_cutover_readiness()
    )
    return _page_shell(title, "setup", body, flash=flash)


@router.get("/cutover", response_class=HTMLResponse)
async def finance_cutover_page(request: Request, flash: str = ""):
    """Whether the old ClickUp bill list can be switched off yet."""
    return HTMLResponse(await asyncio.to_thread(_render_cutover_page, flash))


@router.post("/audit/clear-dismissals")
async def audit_clear_dismissals_endpoint(request: Request):
    """Forget dismissals made against the old, broken rules."""
    from sales_support_agent.services.cashflow.bill_audit import clear_dismissals

    cleared = await asyncio.to_thread(clear_dismissals)
    return RedirectResponse(
        f"/admin/finances/audit?flash={quote(f'ok:{cleared} old dismissal(s) cleared.')}",
        status_code=303,
    )


@router.get("/import", response_class=HTMLResponse)
async def schedule_import_page(request: Request, flash: str = ""):
    """Review the schedule rebuilt from existing ClickUp data. Writes nothing."""
    from sales_support_agent.services.cashflow.import_page import render_import_page

    return HTMLResponse(await asyncio.to_thread(render_import_page, flash=flash))


@router.post("/import/apply")
async def schedule_import_apply(request: Request):
    """Create the chosen schedules and vendors, then archive what was selected."""
    from sales_support_agent.services.cashflow.schedule_import import apply_import

    form = await request.form()
    user = get_current_user(request) or {}
    actor = str(user.get("email") or user.get("id") or "finance-operator")
    try:
        result = await asyncio.to_thread(
            apply_import,
            schedule_keys=[str(v) for v in form.getlist("schedule_key")],
            archive_ids=[str(v) for v in form.getlist("archive_id")],
            keep_ids=[str(v) for v in form.getlist("keep_id")],
            actor=actor,
        )
    except ValueError as exc:
        return _redirect_finance_error(f"The import could not run: {exc}")
    return _redirect_finance_home(
        f"Created {result['templates_created']} schedule(s) and {result['vendors_created']} vendor(s); "
        f"archived {result['archived']} old item(s)."
    )


@router.get("/review", response_class=HTMLResponse)
async def review_page_endpoint(request: Request, flash: str = ""):
    """The grouped review list for clearing blocked obligations in batches."""
    from sales_support_agent.services.cashflow.review_page import render_review_page

    return HTMLResponse(await asyncio.to_thread(render_review_page, flash=flash))


@router.post("/review/preview", response_class=HTMLResponse)
async def review_preview_endpoint(request: Request):
    """Show exactly what a bulk action would change. Writes nothing."""
    from sales_support_agent.services.cashflow.bulk_resolve import preview_bulk_action
    from sales_support_agent.services.cashflow.review_page import render_review_preview

    form = await request.form()
    event_ids = [str(value) for value in form.getlist("event_id")]
    action = str(form.get("action") or "write_off")
    if not event_ids:
        return _redirect_review_error("Tick at least one item first.")
    try:
        preview = await asyncio.to_thread(preview_bulk_action, event_ids, action)
    except ValueError as exc:
        return _redirect_review_error(str(exc))
    return HTMLResponse(await asyncio.to_thread(render_review_preview, preview))


@router.post("/review/cleanup-preview", response_class=HTMLResponse)
async def review_cleanup_preview_endpoint(
    request: Request, older_than_days: int = Form(90), event_type: str = Form("outflow"),
):
    """Preview archiving everything older than the cutoff. Writes nothing."""
    from sales_support_agent.services.cashflow.bulk_resolve import (
        list_historical_backlog,
        preview_bulk_action,
    )
    from sales_support_agent.services.cashflow.review_page import render_review_preview

    backlog = await asyncio.to_thread(
        list_historical_backlog, older_than_days=int(older_than_days), event_type=str(event_type),
    )
    if not backlog["actionable_ids"]:
        return _redirect_review_error(
            f"Nothing older than {int(older_than_days)} days needs archiving."
        )
    preview = await asyncio.to_thread(
        preview_bulk_action, backlog["actionable_ids"], "archive_historical",
    )
    preview["cutoff_note"] = (
        f"Everything dated before {backlog['cutoff_date']} with no linked payment"
    )
    return HTMLResponse(await asyncio.to_thread(render_review_preview, preview))


@router.post("/review/snooze")
async def review_snooze_endpoint(
    request: Request, until: str = Form(...), event_id: list[str] = Form(default=[]),
):
    """Hide the ticked items until a date."""
    from datetime import date as _date

    from sales_support_agent.services.cashflow.bulk_resolve import snooze_events

    user = get_current_user(request) or {}
    actor = str(user.get("email") or user.get("id") or "finance-operator")
    try:
        until_date = _date.fromisoformat(str(until)[:10])
    except ValueError:
        return _redirect_review_error("That snooze date could not be read.")
    result = await asyncio.to_thread(snooze_events, list(event_id), until=until_date, actor=actor)
    return _redirect_review_home(f"{result['snoozed']} item(s) hidden until {until_date.isoformat()}.")


@router.post("/review/follow-up")
async def review_follow_up_endpoint(
    request: Request, follow_up_on: str = Form(...), event_id: list[str] = Form(default=[]),
):
    """Keep chasing the ticked items and come back on a date."""
    from datetime import date as _date

    from sales_support_agent.services.cashflow.bulk_resolve import set_follow_up

    user = get_current_user(request) or {}
    actor = str(user.get("email") or user.get("id") or "finance-operator")
    try:
        follow_date = _date.fromisoformat(str(follow_up_on)[:10])
    except ValueError:
        return _redirect_review_error("That follow-up date could not be read.")
    result = await asyncio.to_thread(set_follow_up, list(event_id), follow_up_on=follow_date, actor=actor)
    return _redirect_review_home(
        f"{result['scheduled']} item(s) kept open with a follow-up on {follow_date.isoformat()}."
    )


@router.post("/review/apply")
async def review_apply_endpoint(request: Request):
    """Apply a previewed bulk action with a required reason."""
    from sales_support_agent.services.cashflow.bulk_resolve import apply_bulk_action

    form = await request.form()
    event_ids = [str(value) for value in form.getlist("event_id")]
    action = str(form.get("action") or "")
    reason = str(form.get("reason") or "")
    user = get_current_user(request) or {}
    actor = str(user.get("email") or user.get("id") or "finance-operator")
    try:
        result = await asyncio.to_thread(
            apply_bulk_action, event_ids, action, reason=reason, actor=actor,
        )
    except ValueError as exc:
        return _redirect_review_error(str(exc))
    message = f"{result['applied']} item(s) resolved."
    if result["skipped"]:
        message += f" {result['skipped']} skipped."
    return _redirect_review_home(message)


@router.post("/review/undo/{batch_id}")
async def review_undo_endpoint(request: Request, batch_id: str):
    """Put every item in a batch back exactly as it was."""
    from sales_support_agent.services.cashflow.bulk_resolve import undo_batch

    user = get_current_user(request) or {}
    actor = str(user.get("email") or user.get("id") or "finance-operator")
    result = await asyncio.to_thread(undo_batch, batch_id, actor=actor)
    return _redirect_review_home(f"{result['restored']} item(s) put back.")


@router.post("/matches/confirm")
async def matches_confirm_endpoint(request: Request):
    """Confirm selected bank-payment-to-bill matches as one undoable batch."""
    from sales_support_agent.services.cashflow.plaid_match import confirm_matches

    form = await request.form()
    user = get_current_user(request) or {}
    actor = str(user.get("email") or user.get("id") or "finance-operator")
    pairs: list[tuple[str, str]] = []
    for raw in form.getlist("pair"):
        transaction_id, _, obligation_id = str(raw).partition("|")
        if transaction_id and obligation_id:
            pairs.append((transaction_id, obligation_id))
    if not pairs:
        return _redirect_finance_error("No matches were selected.")
    result = await asyncio.to_thread(confirm_matches, pairs, actor=actor)
    if result["failed"]:
        return _redirect_finance_home(
            f"Matched {result['confirmed']} payment(s); {result['failed']} could not be matched."
        )
    return _redirect_finance_home(f"Matched {result['confirmed']} payment(s) to their bills.")


@router.post("/matches/undo/{run_id}")
async def matches_undo_endpoint(request: Request, run_id: str):
    """Reverse every match made by one batch."""
    from sales_support_agent.services.cashflow.plaid_match import undo_run

    user = get_current_user(request) or {}
    actor = str(user.get("email") or user.get("id") or "finance-operator")
    result = await asyncio.to_thread(undo_run, run_id, actor=actor)
    return _redirect_finance_home(f"Undid {result['reversed']} match(es).")


@router.post("/audit/dismiss")
async def audit_dismiss_endpoint(request: Request, fingerprint: str = Form(...)):
    from sales_support_agent.services.cashflow.bill_audit import dismiss_finding
    await asyncio.to_thread(dismiss_finding, fingerprint)
    return _redirect_finance_home("Audit item dismissed. It will stay quiet next time.")


@router.post("/collections/mark")
async def collections_mark_endpoint(
    request: Request,
    customer_key: str = Form(...),
    channel: str = Form(...),
    status: str = Form(...),
):
    """Record that a collection message was sent or skipped. Never sends it."""
    from sales_support_agent.services.cashflow.collections import set_draft_status
    try:
        await asyncio.to_thread(set_draft_status, customer_key, channel, status)
    except ValueError as exc:
        return _redirect_finance_error(f"Could not update that message: {exc}")
    verb = {"sent": "marked as sent", "skipped": "skipped", "draft": "reset to draft"}.get(status, status)
    return _redirect_finance_home(f"Reminder {verb}.")


@router.post("/assistant/preview")
async def finance_assistant_preview(request: Request):
    """Turn plain English into a server-side draft; this never writes money."""
    from sales_support_agent.services.cashflow.assistant import (
        FinanceAssistantError,
        create_preview,
    )

    body = await request.json()
    user = get_current_user(request) or {}
    actor = str(user.get("email") or user.get("id") or "finance-operator")
    try:
        preview = await asyncio.to_thread(
            create_preview,
            str(body.get("prompt") or ""), actor=actor, settings=_finance_settings(request),
        )
    except (ValueError, FinanceAssistantError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(preview)


@router.post("/assistant/confirm")
async def finance_assistant_confirm(request: Request):
    """Confirm one unexpired assistant draft after explicit user review."""
    from sales_support_agent.services.cashflow.assistant import confirm_preview

    body = await request.json()
    user = get_current_user(request) or {}
    actor = str(user.get("email") or user.get("id") or "finance-operator")
    try:
        commitment = await asyncio.to_thread(
            confirm_preview, str(body.get("preview_id") or ""), actor=actor,
        )
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse({"status": "ok", "commitment": commitment})


@router.post("/commitments/{commitment_id}/transition-preview")
async def commitment_transition_preview(request: Request, commitment_id: str):
    """Preview a native workflow change without changing the commitment."""
    from sales_support_agent.services.cashflow.commitments import preview_transition
    from sales_support_agent.services.cashflow.obligations import get_obligation

    body = await request.json()
    commitment = await asyncio.to_thread(get_obligation, commitment_id)
    if not commitment:
        raise HTTPException(status_code=404, detail="Commitment not found")
    try:
        preview = preview_transition(commitment, str(body.get("target_status") or ""))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(preview)


@router.post("/commitments/{commitment_id}/transition-confirm")
async def commitment_transition_confirm(request: Request, commitment_id: str):
    """Confirm an explicitly reviewed native workflow change."""
    from sales_support_agent.services.cashflow.commitments import confirm_transition

    body = await request.json()
    user = get_current_user(request) or {}
    actor = str(user.get("email") or user.get("id") or "finance-operator")
    try:
        commitment = await asyncio.to_thread(
            confirm_transition,
            commitment_id,
            str(body.get("target_status") or ""),
            actor=actor,
            idempotency_key=str(body.get("idempotency_key") or ""),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse({"status": "ok", "commitment": commitment})


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


@router.post("/smart-review", response_class=HTMLResponse)
async def run_smart_cfo_review(request: Request):
    """Generate advisory-only Smart CFO recommendations from the full ledger."""
    from sales_support_agent.services.cashflow.smart_cfo import SmartCfoProviderError, run_smart_cfo

    try:
        result = await asyncio.to_thread(run_smart_cfo, _finance_settings(request))
    except (TypeError, ValueError, json.JSONDecodeError):
        logger.exception("Smart CFO returned invalid advice")
        return _redirect_finance_error("Smart review returned invalid advice; no finance data changed")
    except SmartCfoProviderError:
        logger.exception("Smart CFO provider request failed")
        return _redirect_finance_error("Smart review provider request failed. Check the configured Anthropic model; no finance data changed")
    except Exception:
        logger.exception("Smart CFO review failed")
        return _redirect_finance_error("Smart review could not be completed; no finance data changed")
    if result.get("status") == "not_configured":
        return _redirect_finance_error("Smart review needs ANTHROPIC_API_KEY on the production service")
    adjective = "reused" if result.get("cached") else "completed"
    return _redirect_finance_home(f"Smart review {adjective} across {result.get('record_count', 0)} finance records")


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
    from sales_support_agent.services.cashflow.savings_reviews import record_savings_review

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
        result = await asyncio.to_thread(
            record_savings_review,
            opportunity,
            action,
            actor,
            reason=reason,
            request_id=request.headers.get("Idempotency-Key") or uuid4().hex,
            clickup_task=None,
        )
    except ValueError as exc:
        return _redirect_finance_error(str(exc))
    except Exception:
        return _redirect_finance_error("Savings review could not be recorded")
    messages = {
        "keep": "Savings opportunity kept for 90 days.",
        "dismiss": "Savings opportunity dismissed for 90 days.",
        "follow_up": "Savings review added to Anata; Finance will wait for bank evidence before counting a saving.",
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
    if bool(getattr(settings, "disable_clickup_finance_sync", False)):
        # Switching the old list off has to hold here too, or one click on this
        # button pulls every retired bill straight back into the forecast.
        return RedirectResponse(
            "/admin/finances?flash="
            + quote(
                "ok:The old ClickUp bill list is switched off, so nothing was brought in. "
                "The schedules in this app are in charge now."
            ),
            status_code=303,
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
    """The schedules that build the next 14 and 30 days."""
    from sales_support_agent.services.cashflow.recurring import render_recurring_page

    return HTMLResponse(await asyncio.to_thread(render_recurring_page, flash=flash))


@router.get("/recurring/new", response_class=HTMLResponse)
async def recurring_new_form(request: Request):
    return render_recurring_new_page()


@router.post("/recurring/new", response_class=HTMLResponse)
async def recurring_new_submit(request: Request):
    form = dict(await request.form())
    try:
        kwargs = parse_template_form(form)
        create_recurring_template(**kwargs)
    except ValueError as exc:
        # A ValueError here is a problem with what was typed in, so the wording
        # is already meant for the operator.
        return render_recurring_new_page(flash=f"err:{exc}")
    except Exception:
        # Anything else is our fault, not theirs. Showing them the Python text
        # tells them nothing they can act on.
        logger.exception("Creating a schedule failed")
        return render_recurring_new_page(
            flash="err:That could not be saved. Nothing was changed. Please try again."
        )
    return RedirectResponse(
        "/admin/finances/recurring?flash=ok:Schedule+added", status_code=303
    )


@router.get("/recurring/{template_id}/edit", response_class=HTMLResponse)
async def recurring_edit_form(request: Request, template_id: str):
    return render_recurring_edit_page(template_id)


@router.post("/recurring/{template_id}/edit", response_class=HTMLResponse)
async def recurring_edit_submit(request: Request, template_id: str):
    form = dict(await request.form())
    try:
        kwargs = parse_template_form(form)
        update_recurring_template(template_id, **kwargs)
    except ValueError as exc:
        return render_recurring_edit_page(template_id, flash=f"err:{exc}")
    except Exception:
        logger.exception("Saving a schedule failed template_id=%s", template_id)
        return render_recurring_edit_page(
            template_id,
            flash="err:That could not be saved. Nothing was changed. Please try again.",
        )
    # Back to the list, not the finance home: being thrown to a different page
    # after every action means finding your place again each time.
    return RedirectResponse(
        "/admin/finances/recurring?flash=ok:Schedule+saved", status_code=303
    )


@router.post("/recurring/roll-forward")
async def recurring_roll_forward(request: Request, return_to: str = Form("")):
    """Move dates that have passed on to the next one in the same series."""
    from sales_support_agent.services.cashflow.obligations import (
        supersede_stale_template_occurrences,
    )

    request.state.finance_return_to = return_to or "/admin/finances/recurring"
    try:
        rolled = await asyncio.to_thread(supersede_stale_template_occurrences)
    except Exception:
        logger.exception("Rolling stale schedule dates forward failed")
        return _redirect_finance_error(
            "Those dates could not be moved on, so nothing changed."
        )
    if not rolled:
        return _redirect_finance_home(
            "Nothing to move on. Every schedule date is already current."
        )
    dates = "date" if len(rolled) == 1 else "dates"
    return _redirect_finance_home(
        f"Moved {len(rolled)} old schedule {dates} on, so they stop counting as money you owe."
    )


@router.post("/recurring/{template_id}/delete")
async def recurring_delete(request: Request, template_id: str):
    try:
        delete_recurring_template(template_id)
    except Exception:
        logger.exception("Deleting a schedule failed template_id=%s", template_id)
        return _redirect_finance_error("That schedule could not be removed, so nothing changed.")
    return RedirectResponse(
        "/admin/finances/recurring?flash=ok:Schedule+removed", status_code=303
    )


# ---------------------------------------------------------------------------
# What is coming: bills predicted from the bank history itself
# ---------------------------------------------------------------------------

_WHATS_COMING_PATH = "/admin/finances/whats-coming"

# What each answer did, in the words the operator picked it with. The tracking
# message deliberately does not promise the 14 day figure will move: a bill due in
# four weeks does not land in a fortnight, and claiming otherwise made a correct
# result look like a broken button.
_BILL_DECISION_FLASH = {
    "track": "Tracking that one. It counts from the date it is next due, shown on its card.",
    "not_a_bill": "Noted, that is not a bill. We will stop asking about it.",
    "snooze": "Left for now. We will ask about it again in a week.",
}


@router.get("/whats-coming", response_class=HTMLResponse)
async def whats_coming_page(request: Request, flash: str = ""):
    """Bills found in the bank history that no schedule covers yet."""
    from sales_support_agent.services.cashflow.whats_coming_page import (
        render_whats_coming_page,
    )

    return HTMLResponse(await asyncio.to_thread(render_whats_coming_page, flash=flash))


@router.post("/whats-coming/decide")
async def whats_coming_decide(
    request: Request,
    pattern_key: str = Form(""),
    decision: str = Form(""),
    return_to: str = Form(""),
):
    """Record one answer about a predicted bill and come back to this page.

    A missing or unrecognised answer is a message on the page, never a validation
    error page, so a stale form cannot dead-end the operator.
    """
    from sales_support_agent.services.cashflow.bill_patterns import (
        pattern_exists,
        record_bill_pattern_decision,
    )

    # This page is a list of questions, so an answer has to land back on it
    # rather than on the finance home with the list scrolled away.
    request.state.finance_return_to = return_to or _WHATS_COMING_PATH

    user = get_current_user(request) or {}
    actor = str(user.get("email") or user.get("id") or "finance-operator")

    # A key can be well formed and still name nothing, which is what a stale tab
    # or a re-submitted form sends. Recording it would report success for a bill
    # that was never tracked.
    if not await asyncio.to_thread(pattern_exists, pattern_key):
        return _redirect_finance_error(
            "That bill is no longer in the list, so nothing changed. "
            "The page below is up to date."
        )
    try:
        await asyncio.to_thread(
            record_bill_pattern_decision,
            pattern_key,
            decision,
            actor=actor,
            request_id=request.headers.get("Idempotency-Key") or uuid4().hex,
        )
    except ValueError:
        return _redirect_finance_error(
            "That answer could not be read, so nothing changed. Try the buttons again."
        )
    except Exception:
        logger.exception("A predicted bill decision could not be recorded")
        return _redirect_finance_error(
            "That answer could not be saved, so nothing changed."
        )
    return _redirect_finance_home(_BILL_DECISION_FLASH.get(decision, "Answer saved."))


# ---------------------------------------------------------------------------
# QBO invoice sync
# ---------------------------------------------------------------------------

async def _refresh_connected_finance_sources(request: Request) -> RedirectResponse:
    """Refresh all connected non-CSV Finance sources in source-of-truth order."""
    settings = (
        getattr(request.app.state, "agent_settings", None)
        or getattr(request.app.state, "admin_dashboard_settings", None)
        or request.app.state.settings
    )
    parts: list[str] = []
    errors: list[str] = []

    # ClickUp is a migration archive now. It is refreshed only through the
    # explicit legacy control so native Anata work cannot be repopulated from
    # an old task list during a normal source refresh.

    try:
        result = await asyncio.to_thread(sync_qbo_invoices, settings)
        parts.append(
            f"QBO receivables {result.rows_inserted} new, {result.rows_skipped_duplicate} unchanged"
        )
        errors.extend(f"QBO receivables: {error}" for error in result.errors[:1])
    except Exception as exc:
        errors.append(f"QBO receivables: {exc}")

    try:
        result = await asyncio.to_thread(
            sync_qbo_bank_transactions, settings, lookback_days=365
        )
        parts.append(
            f"QBO actuals {result.rows_inserted} imported, {result.rows_skipped_duplicate} unchanged"
        )
        errors.extend(f"QBO actuals: {error}" for error in result.errors[:1])
    except Exception as exc:
        errors.append(f"QBO actuals: {exc}")

    summary = " · ".join(parts) or "No connected source completed"
    if errors:
        return _redirect_finance_error(
            f"Connected refresh completed with issues: {summary}. {'; '.join(errors[:2])}"
        )
    return _redirect_finance_home(
        f"Accounting sources refreshed: {summary}. Connected bank data was not changed."
    )


@router.post("/sync-connected-sources", response_class=HTMLResponse)
async def sync_connected_sources(request: Request):
    """One-click refresh for current accounting data; ClickUp stays archival."""
    return await _refresh_connected_finance_sources(request)

@router.post("/sync-qbo", response_class=HTMLResponse)
async def sync_qbo(request: Request):
    """Compatibility alias for the canonical connected-sources refresh."""
    return await _refresh_connected_finance_sources(request)


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


@router.post("/sync-qbo-actuals", response_class=HTMLResponse)
async def sync_qbo_actuals(request: Request):
    """Refresh posted QBO activity for settlement matching, not cash balance."""
    settings = (
        getattr(request.app.state, "agent_settings", None)
        or getattr(request.app.state, "admin_dashboard_settings", None)
        or request.app.state.settings
    )
    try:
        # Historical reconciliation needs more than the short startup window.
        result = await asyncio.to_thread(
            sync_qbo_bank_transactions, settings, lookback_days=365
        )
    except Exception as exc:
        return _redirect_finance_error(f"QuickBooks actuals sync failed: {exc}")

    if result.errors:
        return _redirect_finance_error(f"QuickBooks actuals sync: {result.errors[0]}")
    return _redirect_finance_home(
        f"QuickBooks actuals refreshed: {result.rows_inserted} imported, "
        f"{result.rows_skipped_duplicate} unchanged. Bank CSV remains cash-on-hand truth."
    )


@router.post("/recurring/generate", response_class=HTMLResponse)
async def recurring_generate(request: Request):
    created = generate_upcoming_from_templates(horizon_days=45)
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
    """An old link. The bills it used to guess at now live on What is coming."""
    target = _WHATS_COMING_PATH
    if flash:
        target = f"{target}?flash={quote(flash)}"
    return RedirectResponse(target, status_code=303)


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
