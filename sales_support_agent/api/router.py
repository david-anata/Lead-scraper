"""API routes."""

from __future__ import annotations

import logging
import secrets
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import re
from urllib.parse import parse_qs

import requests
from fastapi import APIRouter, File, Form, Header, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from sqlalchemy import func, select

from main import (
    ICPBuildRequest as LeadBuildRequest,
    enqueue_lead_build,
    fetch_lead_run_status,
    get_lead_run_csv,
    get_missing_required_settings as get_missing_lead_builder_settings,
    load_settings as load_lead_builder_settings,
)

from sales_support_agent.config import get_missing_runtime_settings
from sales_support_agent.integrations.canva import CanvaClient
from sales_support_agent.integrations.clickup import ClickUpClient
from sales_support_agent.integrations.gmail import GmailClient, GmailIntegrationError
from sales_support_agent.integrations.slack import SlackClient
from sales_support_agent.jobs.daily_digest import DailyDigestJob
from sales_support_agent.jobs.mailbox_sync import GmailMailboxSyncJob
from sales_support_agent.jobs.stale_leads import StaleLeadJob
from sales_support_agent.models.database import session_scope
from sales_support_agent.models.entities import AutomationRun, LeadMirror
from sales_support_agent.models.schemas import (
    ApiMessage,
    CommunicationEventRequest,
    DailyDigestRunRequest,
    DiscoveryRequest,
    GmailSyncRequest,
    StaleLeadRunRequest,
    SyncRequest,
)
from sales_support_agent.services.communications import CommunicationService
from sales_support_agent.services.admin_auth import (
    admin_login_enabled,
    create_admin_session_token,
    create_signed_state_token,
    read_signed_state_token,
    validate_admin_session_token,
    verify_admin_password,
)
from sales_support_agent.services.admin_dashboard import (
    build_dashboard_data,
    build_executive_data,
    dashboard_data_to_dict,
    executive_data_to_dict,
    render_dashboard_page,
    render_executive_page,
    render_login_page,
)
from sales_support_agent.services.discovery import ClickUpDiscoveryService
from sales_support_agent.services.deck_generator import DeckGenerationService
from sales_support_agent.services.gmail_drafts import create_bulk_draft_payloads
from sales_support_agent.services.instantly_webhooks import InstantlyWebhookService
from sales_support_agent.services.sync import ClickUpSyncService
from sales_support_agent.config import normalize_status_key


router = APIRouter()
logger = logging.getLogger(__name__)


def _parse_competitor_inputs(value: str) -> list[str]:
    items: list[str] = []
    seen: set[str] = set()
    for fragment in re.split(r"[\n,]+", str(value or "")):
        cleaned = fragment.strip()
        if not cleaned:
            continue
        lowered = cleaned.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        items.append(cleaned)
    return items


def _enforce_api_key(request: Request, internal_api_key: str | None) -> None:
    configured = request.app.state.settings.internal_api_key
    if configured and internal_api_key != configured:
        raise HTTPException(status_code=401, detail="Invalid internal API key.")


def _enforce_api_key_from_header_or_query(request: Request, internal_api_key: str | None) -> None:
    provided = internal_api_key or request.query_params.get("token") or ""
    _enforce_api_key(request, provided)


def _validate_runtime(request: Request) -> None:
    missing = get_missing_runtime_settings(request.app.state.settings)
    if missing:
        raise HTTPException(
            status_code=500,
            detail=f"Missing required environment variables for sales support agent: {', '.join(missing)}",
        )


def _lead_builder_status(settings: object | None = None) -> dict[str, object]:
    try:
        lead_settings = load_lead_builder_settings()
        missing = get_missing_lead_builder_settings(lead_settings)
        if not missing:
            return {"ready": True, "missing": [], "mode": "local"}
        remote_url = getattr(settings, "lead_build_url", "") if settings is not None else ""
        if remote_url:
            return {"ready": True, "missing": missing, "mode": "remote", "lead_build_url": str(remote_url)}
        return {"ready": False, "missing": missing, "mode": "local"}
    except Exception as exc:
        remote_url = getattr(settings, "lead_build_url", "") if settings is not None else ""
        if remote_url:
            return {"ready": True, "missing": [str(exc)], "mode": "remote", "lead_build_url": str(remote_url)}
        return {"ready": False, "missing": [str(exc)], "mode": "local"}


def _require_admin_enabled(request: Request) -> None:
    if not admin_login_enabled(request.app.state.settings):
        raise HTTPException(
            status_code=503,
            detail="Admin dashboard is not configured. Set ADMIN_DASHBOARD_PASSWORD first.",
        )


def _is_admin_authenticated(request: Request) -> bool:
    settings = request.app.state.settings
    token = request.cookies.get(settings.admin_cookie_name, "")
    return validate_admin_session_token(settings, token)


def _admin_cookie_options(request: Request) -> dict[str, object]:
    settings = request.app.state.settings
    return {
        "key": settings.admin_cookie_name,
        "httponly": True,
        "secure": request.url.scheme == "https",
        "samesite": "lax",
        "max_age": settings.admin_session_ttl_hours * 3600,
        "path": "/",
    }


def _canva_oauth_cookie_name(request: Request) -> str:
    return f"{request.app.state.settings.admin_cookie_name}_canva_oauth"


def _normalize_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _latest_clickup_sync_at(request: Request) -> datetime | None:
    with session_scope(request.app.state.session_factory) as session:
        latest_sync = session.execute(select(func.max(LeadMirror.last_sync_at))).scalar_one_or_none()
    return _normalize_utc(latest_sync)


def _dashboard_sync_is_stale(request: Request, latest_sync_at: datetime | None) -> bool:
    if latest_sync_at is None:
        return True
    max_age = max(1, request.app.state.settings.dashboard_auto_sync_max_age_minutes)
    return datetime.now(timezone.utc) - latest_sync_at >= timedelta(minutes=max_age)


def _dashboard_sync_details(request: Request) -> dict[str, object]:
    latest_sync_at = _latest_clickup_sync_at(request)
    with request.app.state.dashboard_sync_lock:
        future = request.app.state.dashboard_sync_future
        running = bool(future and not future.done())
        last_started_at = request.app.state.dashboard_sync_last_started_at
        last_completed_at = request.app.state.dashboard_sync_last_completed_at
        last_error = request.app.state.dashboard_sync_last_error
    stale = _dashboard_sync_is_stale(request, latest_sync_at)
    message = (
        f"Sync running in the background. Last full board refresh was {_normalize_utc(last_started_at).isoformat() if last_started_at else 'recently queued'}."
        if running
        else (
            f"Board cache is stale. Auto-refresh kicks in after {request.app.state.settings.dashboard_auto_sync_max_age_minutes} minutes."
            if stale
            else "Board cache is fresh."
        )
    )
    if last_error and not running:
        message = f"Last sync failed: {last_error}"
    return {
        "running": running,
        "stale": stale,
        "latest_sync_at": latest_sync_at.isoformat() if latest_sync_at else "",
        "last_started_at": _normalize_utc(last_started_at).isoformat() if last_started_at else "",
        "last_completed_at": _normalize_utc(last_completed_at).isoformat() if last_completed_at else "",
        "last_error": last_error,
        "auto_sync_enabled": request.app.state.settings.dashboard_auto_sync_enabled,
        "max_age_minutes": max(1, request.app.state.settings.dashboard_auto_sync_max_age_minutes),
        "message": message,
    }


def _remote_lead_builder_url(request: Request) -> str:
    return str(getattr(request.app.state.settings, "lead_build_url", "") or "").rstrip("/")


def _queue_remote_lead_build(request: Request, payload: dict[str, object]) -> dict[str, object]:
    lead_build_url = _remote_lead_builder_url(request)
    if not lead_build_url:
        raise HTTPException(status_code=503, detail="LEAD_BUILD_URL is not configured on this service.")
    response = requests.post(
        f"{lead_build_url}/run-lead-build?async=true",
        headers={"Content-Type": "application/json", "X-Scheduler-Source": "admin_dashboard"},
        json=payload,
        timeout=60,
    )
    payload_json = response.json()
    if response.status_code >= 400:
        raise HTTPException(status_code=response.status_code, detail=payload_json.get("detail") or payload_json.get("message") or "Remote lead build failed.")
    details = dict(payload_json.get("details") or {})
    run_id = str(details.get("run_id") or "")
    return {
        "run_id": run_id,
        "poll_url": f"/admin/api/lead-runs/{run_id}",
        "download_url": f"/admin/api/lead-runs/{run_id}/download",
        "remote": True,
    }


def _fetch_remote_lead_run_status(request: Request, run_id: str) -> dict[str, object] | None:
    lead_build_url = _remote_lead_builder_url(request)
    if not lead_build_url:
        return None
    response = requests.get(f"{lead_build_url}/lead-runs/{run_id}", timeout=30)
    if response.status_code == 404:
        return None
    response.raise_for_status()
    payload = response.json()
    return dict(payload.get("details") or {})


def _download_remote_lead_run(request: Request, run_id: str) -> Response | None:
    lead_build_url = _remote_lead_builder_url(request)
    if not lead_build_url:
        return None
    response = requests.get(f"{lead_build_url}/lead-runs/{run_id}/download", timeout=60)
    if response.status_code == 404:
        return None
    if response.status_code == 409:
        return JSONResponse(status_code=409, content={"detail": "Lead run is not complete yet."})
    if response.status_code >= 400:
        try:
            payload = response.json()
        except Exception:
            payload = {"detail": "Remote lead run download failed."}
        return JSONResponse(status_code=response.status_code, content=payload)
    return Response(
        content=response.content,
        media_type=response.headers.get("content-type", "text/csv"),
        headers={"Content-Disposition": response.headers.get("content-disposition", 'attachment; filename="instantly_upload.csv"')},
    )


def _run_dashboard_sync(app: object, *, trigger: str) -> dict[str, object]:
    settings = app.state.settings
    with session_scope(app.state.session_factory) as session:
        clickup_summary = ClickUpSyncService(settings, ClickUpClient(settings), session).sync_list(include_closed=True)
        stale_summary = StaleLeadJob(settings, ClickUpClient(settings), SlackClient(settings), session).run(dry_run=True)
        mirrored_leads = list(
            session.execute(
                select(LeadMirror).where(LeadMirror.list_id == settings.clickup_list_id)
            ).scalars()
        )
    active_leads = sum(
        1
        for lead in mirrored_leads
        if normalize_status_key(lead.status or "") in settings.active_statuses
    )
    synced_tasks = int(clickup_summary.get("synced_tasks", 0) or 0)
    if synced_tasks == 0:
        message = "Dashboard sync finished, but ClickUp returned 0 tasks. Check CLICKUP_LIST_ID and ClickUp token access."
    elif active_leads == 0:
        message = "Dashboard sync finished, but 0 tasks are in tracked active statuses. Check your ClickUp status names."
    else:
        message = f"Dashboard sync finished. Synced {synced_tasks} tasks and found {active_leads} active leads."
    return {
        "clickup_sync": clickup_summary,
        "stale_lead_scan": stale_summary,
        "gmail_sync": {"status": "skipped", "reason": "enable once Gmail OAuth is fixed"},
        "trigger": trigger,
        "mirrored_leads": len(mirrored_leads),
        "active_leads": active_leads,
        "message": message,
    }


def _dashboard_sync_worker(app: object, *, trigger: str) -> None:
    try:
        _run_dashboard_sync(app, trigger=trigger)
        with app.state.dashboard_sync_lock:
            app.state.dashboard_sync_last_completed_at = datetime.now(timezone.utc)
            app.state.dashboard_sync_last_error = ""
        logger.info("dashboard sync completed trigger=%s", trigger)
    except Exception as exc:
        logger.exception("dashboard sync failed trigger=%s", trigger)
        with app.state.dashboard_sync_lock:
            app.state.dashboard_sync_last_completed_at = datetime.now(timezone.utc)
            app.state.dashboard_sync_last_error = str(exc)
    finally:
        with app.state.dashboard_sync_lock:
            app.state.dashboard_sync_future = None


def _start_dashboard_sync(request: Request, *, trigger: str, force: bool) -> dict[str, object]:
    latest_sync_at = _latest_clickup_sync_at(request)
    stale = _dashboard_sync_is_stale(request, latest_sync_at)
    status: str
    message: str
    with request.app.state.dashboard_sync_lock:
        future = request.app.state.dashboard_sync_future
        if future and not future.done():
            status = "running"
            message = "Dashboard sync is already running in the background."
        elif not force and not stale:
            status = "skipped"
            message = "Board cache is still fresh. No sync was needed."
        else:
            request.app.state.dashboard_sync_last_started_at = datetime.now(timezone.utc)
            request.app.state.dashboard_sync_last_error = ""
            request.app.state.dashboard_sync_future = request.app.state.dashboard_sync_executor.submit(
                _dashboard_sync_worker,
                request.app,
                trigger=trigger,
            )
            status = "running"
            message = "Dashboard sync started in the background."
    details = _dashboard_sync_details(request)
    details["status"] = status
    details["message"] = message
    return details


def _canva_oauth_cookie_options(request: Request) -> dict[str, object]:
    return {
        **_admin_cookie_options(request),
        "max_age": 900,
    }


def _enforce_instantly_webhook_auth(request: Request) -> None:
    settings = request.app.state.settings
    if settings.instantly_webhook_secret:
        header_name = settings.instantly_webhook_secret_header
        provided = request.headers.get(header_name) or request.query_params.get("token") or ""
        if provided != settings.instantly_webhook_secret:
            raise HTTPException(status_code=401, detail="Invalid Instantly webhook secret.")
        return

    configured = settings.internal_api_key
    if configured:
        provided = request.headers.get("X-Internal-Api-Key") or request.query_params.get("token") or ""
        if provided != configured:
            raise HTTPException(status_code=401, detail="Invalid internal API key.")


@router.get("/", response_model=ApiMessage)
def root() -> ApiMessage:
    return ApiMessage(status="ok", message="Sales support agent is running.")


@router.get("/health", response_model=ApiMessage)
def health(request: Request) -> ApiMessage:
    settings = request.app.state.settings
    brand_package_path = Path(str(getattr(settings, "shared_brand_package_path", "") or "")).expanduser()
    return ApiMessage(
        status="ok",
        message="healthy",
        details={
            "clickup_configured": bool(settings.clickup_api_token and settings.clickup_list_id),
            "slack_configured": bool(settings.slack_bot_token and settings.slack_channel_id),
            "discovery_snapshot_path": str(settings.discovery_snapshot_path),
            "deck_generator_configured": brand_package_path.exists(),
            "deck_brand_package_path": str(brand_package_path),
        },
    )


@router.get("/admin/login", response_class=HTMLResponse)
def admin_login_page(request: Request) -> HTMLResponse:
    _require_admin_enabled(request)
    if _is_admin_authenticated(request):
        return HTMLResponse("", status_code=302, headers={"Location": "/admin"})
    return HTMLResponse(render_login_page())


@router.post("/admin/login", response_class=HTMLResponse)
async def admin_login_submit(request: Request) -> Response:
    _require_admin_enabled(request)
    body = (await request.body()).decode("utf-8")
    password = parse_qs(body).get("password", [""])[0]
    if not verify_admin_password(request.app.state.settings, password):
        return HTMLResponse(render_login_page(error_message="Incorrect password."), status_code=401)

    response = RedirectResponse(url="/admin", status_code=302)
    response.set_cookie(
        value=create_admin_session_token(request.app.state.settings),
        **_admin_cookie_options(request),
    )
    return response


@router.get("/admin/logout")
def admin_logout(request: Request) -> RedirectResponse:
    response = RedirectResponse(url="/admin/login", status_code=302)
    response.delete_cookie(request.app.state.settings.admin_cookie_name, path="/")
    return response


@router.get("/admin", response_class=HTMLResponse)
def admin_dashboard(request: Request) -> Response:
    _require_admin_enabled(request)
    if not _is_admin_authenticated(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    settings = request.app.state.settings
    with session_scope(request.app.state.session_factory) as session:
        dashboard = build_dashboard_data(
            settings=settings,
            session=session,
            lead_builder_status=_lead_builder_status(settings),
            clickup_client=ClickUpClient(settings),
        )
    if settings.dashboard_auto_sync_enabled:
        _start_dashboard_sync(request, trigger="admin_page_load", force=False)
    return HTMLResponse(render_dashboard_page(dashboard))


@router.get("/admin/executive", response_class=HTMLResponse)
def admin_executive_dashboard(request: Request) -> Response:
    _require_admin_enabled(request)
    if not _is_admin_authenticated(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    settings = request.app.state.settings
    with session_scope(request.app.state.session_factory) as session:
        executive = build_executive_data(
            settings=settings,
            session=session,
            clickup_client=ClickUpClient(settings),
        )
    return HTMLResponse(render_executive_page(executive))


@router.get("/api/admin/dashboard-data", response_model=ApiMessage)
def admin_dashboard_data(
    request: Request,
    x_internal_api_key: str | None = Header(default=None),
) -> ApiMessage:
    _enforce_api_key(request, x_internal_api_key)
    settings = request.app.state.settings
    with session_scope(request.app.state.session_factory) as session:
        dashboard = build_dashboard_data(
            settings=settings,
            session=session,
            lead_builder_status=_lead_builder_status(settings),
            clickup_client=ClickUpClient(settings),
        )
    return ApiMessage(status="ok", message="Admin dashboard data loaded.", details=dashboard_data_to_dict(dashboard))


@router.get("/api/admin/executive-data", response_model=ApiMessage)
def admin_executive_data(
    request: Request,
    x_internal_api_key: str | None = Header(default=None),
) -> ApiMessage:
    _enforce_api_key(request, x_internal_api_key)
    settings = request.app.state.settings
    with session_scope(request.app.state.session_factory) as session:
        executive = build_executive_data(
            settings=settings,
            session=session,
            clickup_client=ClickUpClient(settings),
        )
    return ApiMessage(
        status="ok",
        message="Executive summary data loaded.",
        details=executive_data_to_dict(executive),
    )


@router.post("/admin/api/run-lead-build", response_model=None)
async def admin_run_lead_build(request: Request) -> Response:
    _require_admin_enabled(request)
    if not _is_admin_authenticated(request):
        return JSONResponse(status_code=401, content={"detail": "Admin login required."})
    lead_builder_status = _lead_builder_status(request.app.state.settings)
    if not lead_builder_status.get("ready"):
        return JSONResponse(
            status_code=503,
            content={
                "detail": "Lead builder is not configured on this service.",
                "missing": lead_builder_status.get("missing", []),
            },
        )

    payload = await request.json()
    build_request = LeadBuildRequest(**payload)
    if lead_builder_status.get("mode") == "remote":
        remote_details = _queue_remote_lead_build(request, build_request.model_dump())
        return JSONResponse(
            status_code=202,
            content={
                "status": "queued",
                "message": "Lead build queued on the remote lead engine.",
                "details": remote_details,
            },
        )

    run_id = enqueue_lead_build(build_request, scheduler_source="admin_dashboard")
    return JSONResponse(
        status_code=202,
        content={
            "status": "queued",
            "message": "Lead build queued.",
            "details": {
                "run_id": run_id,
                "poll_url": f"/admin/api/lead-runs/{run_id}",
                "download_url": f"/admin/api/lead-runs/{run_id}/download",
            },
        },
    )


@router.get("/admin/api/lead-runs/{run_id}", response_model=None)
def admin_lead_run_status(request: Request, run_id: str) -> JSONResponse:
    _require_admin_enabled(request)
    if not _is_admin_authenticated(request):
        return JSONResponse(status_code=401, content={"detail": "Admin login required."})

    payload = fetch_lead_run_status(run_id)
    if payload is None:
        payload = _fetch_remote_lead_run_status(request, run_id)
    if payload is None:
        return JSONResponse(status_code=404, content={"detail": "Lead run not found."})
    return JSONResponse(status_code=200, content={"status": "ok", "message": "Lead run status loaded.", "details": payload})


@router.get("/admin/api/lead-runs/{run_id}/download", response_model=None)
def admin_lead_run_download(request: Request, run_id: str) -> Response:
    _require_admin_enabled(request)
    if not _is_admin_authenticated(request):
        return JSONResponse(status_code=401, content={"detail": "Admin login required."})

    payload = fetch_lead_run_status(run_id)
    if payload is None:
        remote_response = _download_remote_lead_run(request, run_id)
        if remote_response is not None:
            return remote_response
    if payload is None:
        return JSONResponse(status_code=404, content={"detail": "Lead run not found."})
    if payload.get("status") != "completed":
        return JSONResponse(status_code=409, content={"detail": "Lead run is not complete yet."})

    csv_content = get_lead_run_csv(run_id)
    if not csv_content:
        return JSONResponse(status_code=404, content={"detail": "No CSV was produced for this run."})

    filename_date = str(payload.get("run_date") or "lead_run")
    return Response(
        content=csv_content,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename=\"instantly_upload_{filename_date}.csv\"'},
    )


@router.post("/admin/api/sync-dashboard", response_model=None)
def admin_sync_dashboard(
    request: Request,
    background: bool = True,
    only_if_stale: bool = False,
) -> JSONResponse:
    _require_admin_enabled(request)
    if not _is_admin_authenticated(request):
        return JSONResponse(status_code=401, content={"detail": "Admin login required."})

    if background:
        details = _start_dashboard_sync(
            request,
            trigger="admin_manual_sync" if not only_if_stale else "admin_auto_sync",
            force=not only_if_stale,
        )
        status_code = 202 if details.get("running") else 200
        return JSONResponse(
            status_code=status_code,
            content={
                "status": details.get("status", "ok"),
                "message": str(details.get("message", "Dashboard sync requested.")),
                "details": details,
            },
        )

    details = _run_dashboard_sync(request.app, trigger="admin_manual_sync")
    return JSONResponse(
        status_code=200,
        content={
            "status": "ok",
            "message": str(details.get("message", "Dashboard sync completed.")),
            "details": details,
        },
    )


@router.get("/admin/api/sync-dashboard/status", response_model=None)
def admin_sync_dashboard_status(request: Request) -> JSONResponse:
    _require_admin_enabled(request)
    if not _is_admin_authenticated(request):
        return JSONResponse(status_code=401, content={"detail": "Admin login required."})

    details = _dashboard_sync_details(request)
    return JSONResponse(
        status_code=200,
        content={
            "status": "ok",
            "message": str(details.get("message", "Dashboard sync status loaded.")),
            "details": details,
        },
    )


@router.post("/api/admin/gmail-drafts", response_model=None)
async def admin_create_gmail_drafts(
    request: Request,
    x_internal_api_key: str | None = Header(default=None),
    contacts_csv: UploadFile = File(...),
    sales_objective: str = Form(default=""),
    subject_template: str = Form(default=""),
    body_template: str = Form(default=""),
    dry_run: bool = Form(default=False),
) -> JSONResponse:
    _enforce_api_key(request, x_internal_api_key)
    file_bytes = await contacts_csv.read()
    if not file_bytes:
        return JSONResponse(status_code=400, content={"detail": "Upload a CSV file with at least one contact row."})

    try:
        payload = create_bulk_draft_payloads(
            csv_bytes=file_bytes,
            sales_objective=sales_objective,
            subject_template=subject_template,
            body_template=body_template,
        )
    except ValueError as exc:
        return JSONResponse(status_code=400, content={"detail": str(exc)})

    gmail_client = GmailClient(request.app.state.settings)
    prepared_rows = payload["prepared_rows"]
    failed_rows = list(payload["failed_rows"])
    created_rows: list[dict[str, str]] = []

    if not dry_run:
        try:
            for prepared in prepared_rows:
                draft = gmail_client.create_draft(
                    to=(prepared["email"],),
                    subject=prepared["subject"],
                    text=prepared["body"],
                )
                created_rows.append(
                    {
                        "row_number": prepared["row_number"],
                        "email": prepared["email"],
                        "subject": prepared["subject"],
                        "draft_id": str(draft.get("id") or ""),
                        "message_id": str((draft.get("message") or {}).get("id") or ""),
                    }
                )
        except GmailIntegrationError as exc:
            return JSONResponse(
                status_code=502,
                content={
                    "status": "failed",
                    "message": "Gmail draft creation failed.",
                    "details": exc.as_dict(),
                },
            )

    preview_rows = [
        {
            "row_number": row["row_number"],
            "email": row["email"],
            "subject": row["subject"],
            "body": row["body"],
            "body_preview": row["body"][:240],
            "body_length": len(row["body"]),
            "first_name": row.get("first_name", ""),
            "last_name": row.get("last_name", ""),
            "company": row.get("company", ""),
        }
        for row in prepared_rows[:10]
    ]
    details = {
        "dry_run": dry_run,
        "rows_total": payload["rows_total"],
        "prepared": len(prepared_rows),
        "created": len(created_rows),
        "failed": len(failed_rows),
        "preview_limit": 10,
        "previewed": len(preview_rows),
        "available_placeholders": payload["available_placeholders"],
        "drafts_url": "https://mail.google.com/mail/u/0/#drafts",
        "previews": preview_rows,
        "created_rows": created_rows[:25],
        "failed_rows": failed_rows[:25],
    }
    return JSONResponse(
        status_code=200,
        content={
            "status": "ok",
            "message": "Gmail draft preview completed." if dry_run else "Gmail drafts created.",
            "details": details,
        },
    )


@router.get("/admin/api/canva/connect", response_model=None)
def admin_canva_connect(request: Request) -> Response:
    _require_admin_enabled(request)
    if not _is_admin_authenticated(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    settings = request.app.state.settings
    missing = [
        env_name
        for env_name, attr_name in (
            ("CANVA_CLIENT_ID", "canva_client_id"),
            ("CANVA_CLIENT_SECRET", "canva_client_secret"),
            ("CANVA_REDIRECT_URI", "canva_redirect_uri"),
        )
        if not getattr(settings, attr_name, "")
    ]
    if missing:
        return JSONResponse(
            status_code=503,
            content={"detail": f"Canva OAuth is missing environment variables: {', '.join(missing)}"},
        )

    state = secrets.token_urlsafe(18)
    code_verifier = secrets.token_urlsafe(64)
    signed_state = create_signed_state_token(
        settings.admin_session_secret,
        {
            "state": state,
            "code_verifier": code_verifier,
            "issued_at": str(int(datetime.now(timezone.utc).timestamp())),
            "return_to": str(request.query_params.get("return_to", "") or "").strip(),
        },
    )
    authorize_url = CanvaClient(settings).build_authorize_url(
        state=state,
        code_verifier=code_verifier,
    )
    response = RedirectResponse(url=authorize_url, status_code=302)
    response.set_cookie(
        value=signed_state,
        **{
            **_canva_oauth_cookie_options(request),
            "key": _canva_oauth_cookie_name(request),
        },
    )
    return response


@router.get("/api/admin/canva/connect", response_model=None)
def internal_canva_connect(
    request: Request,
    x_internal_api_key: str | None = Header(default=None),
) -> Response:
    _enforce_api_key_from_header_or_query(request, x_internal_api_key)

    settings = request.app.state.settings
    missing = [
        env_name
        for env_name, attr_name in (
            ("CANVA_CLIENT_ID", "canva_client_id"),
            ("CANVA_CLIENT_SECRET", "canva_client_secret"),
            ("CANVA_REDIRECT_URI", "canva_redirect_uri"),
        )
        if not getattr(settings, attr_name, "")
    ]
    if missing:
        return JSONResponse(
            status_code=503,
            content={"detail": f"Canva OAuth is missing environment variables: {', '.join(missing)}"},
        )

    state = secrets.token_urlsafe(18)
    code_verifier = secrets.token_urlsafe(64)
    signed_state = create_signed_state_token(
        settings.admin_session_secret,
        {
            "state": state,
            "code_verifier": code_verifier,
            "issued_at": str(int(datetime.now(timezone.utc).timestamp())),
            "return_to": str(request.query_params.get("return_to", "") or "").strip(),
        },
    )
    authorize_url = CanvaClient(settings).build_authorize_url(
        state=state,
        code_verifier=code_verifier,
    )
    response = RedirectResponse(url=authorize_url, status_code=302)
    response.set_cookie(
        value=signed_state,
        **{
            **_canva_oauth_cookie_options(request),
            "key": _canva_oauth_cookie_name(request),
        },
    )
    return response


@router.get("/admin/api/canva/callback", response_model=None)
def admin_canva_callback(
    request: Request,
    code: str = "",
    state: str = "",
    error: str = "",
    error_description: str = "",
) -> Response:
    if error:
        return JSONResponse(status_code=400, content={"detail": error_description or error})

    signed_state = request.cookies.get(_canva_oauth_cookie_name(request), "")
    state_payload = read_signed_state_token(request.app.state.settings.admin_session_secret, signed_state)
    if not state_payload or state_payload.get("state") != state or not code:
        return JSONResponse(status_code=400, content={"detail": "Canva OAuth state validation failed."})

    issued_at = int(state_payload.get("issued_at", "0") or 0)
    if issued_at and datetime.now(timezone.utc) > datetime.fromtimestamp(issued_at, tz=timezone.utc) + timedelta(minutes=15):
        return JSONResponse(status_code=400, content={"detail": "Canva OAuth state expired. Start the connection flow again."})

    settings = request.app.state.settings
    with session_scope(request.app.state.session_factory) as session:
        DeckGenerationService(settings, session).connect_canva(
            code=code,
            code_verifier=state_payload.get("code_verifier", ""),
        )

    redirect_target = str(state_payload.get("return_to", "") or "").strip() or "/admin"
    response = RedirectResponse(url=redirect_target, status_code=302)
    response.delete_cookie(_canva_oauth_cookie_name(request), path="/")
    return response


def _render_deck_export(request: Request, run_id: int, token: str) -> Response:
    with session_scope(request.app.state.session_factory) as session:
        run = session.execute(
            select(AutomationRun).where(
                AutomationRun.id == run_id,
                AutomationRun.run_type == "deck_generation",
            )
        ).scalar_one_or_none()
        if run is None:
            return HTMLResponse("Deck export not found.", status_code=404)
        summary = dict(run.summary_json or {})
        if summary.get("export_token") != token:
            return HTMLResponse("Deck export not found.", status_code=404)
        deck_html = str(summary.get("deck_html") or "")
        if not deck_html:
            return HTMLResponse("Deck export not found.", status_code=404)
        now_iso = datetime.now(timezone.utc).isoformat()
        summary["view_count"] = int(summary.get("view_count", 0) or 0) + 1
        if not summary.get("first_viewed_at"):
            summary["first_viewed_at"] = now_iso
        summary["last_viewed_at"] = now_iso
        run.summary_json = summary
        session.add(run)
        return HTMLResponse(deck_html)


@router.get("/deck-exports/{run_id}/{token}", response_class=HTMLResponse)
def deck_export_view(request: Request, run_id: int, token: str) -> Response:
    return _render_deck_export(request, run_id, token)


@router.get("/decks/{deck_slug}/{run_id}/{token}", response_class=HTMLResponse)
def deck_export_slug_view(request: Request, deck_slug: str, run_id: int, token: str) -> Response:
    return _render_deck_export(request, run_id, token)


@router.post("/admin/api/generate-deck", response_model=ApiMessage)
async def admin_generate_deck(
    request: Request,
    competitor_xray_csv: UploadFile = File(...),
    keyword_xray_csv: UploadFile | None = File(default=None),
    target_product_input: str = Form(default=""),
    channels: list[str] = Form(default=[]),
    creative_mockup_url: str = Form(default=""),
    case_study_url: str = Form(default=""),
    offers: list[str] = Form(default=[]),
    include_recommended_plan: bool = Form(default=True),
) -> ApiMessage:
    _require_admin_enabled(request)
    if not _is_admin_authenticated(request):
        raise HTTPException(status_code=401, detail="Admin login required.")
    competitor_bytes = await competitor_xray_csv.read()
    keyword_bytes = await keyword_xray_csv.read() if keyword_xray_csv is not None else None
    settings = request.app.state.settings
    try:
        with session_scope(request.app.state.session_factory) as session:
            result = DeckGenerationService(settings, session).generate_deck(
                competitor_xray_csv_bytes=competitor_bytes,
                competitor_xray_filename=competitor_xray_csv.filename or "competitors.csv",
                keyword_xray_csv_bytes=keyword_bytes,
                keyword_xray_filename=keyword_xray_csv.filename if keyword_xray_csv is not None else "",
                target_product_input=target_product_input,
                channels=channels,
                creative_mockup_url=creative_mockup_url,
                case_study_url=case_study_url,
                offers=offers,
                include_recommended_plan=include_recommended_plan,
            )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return ApiMessage(
        status="ok",
        message=result.message,
        details={
            "run_id": result.run_id,
            "status": result.status,
            "output_type": result.output_type,
            "design_id": result.design_id,
            "design_title": result.design_title,
            "edit_url": result.edit_url,
            "view_url": result.view_url,
            "warnings": result.warnings,
            "sales_row_count": result.sales_row_count,
            "competitor_row_count": result.competitor_row_count,
            "template_fields": result.template_fields,
        },
    )


@router.post("/api/admin/generate-deck", response_model=ApiMessage)
async def internal_admin_generate_deck(
    request: Request,
    x_internal_api_key: str | None = Header(default=None),
    competitor_xray_csv: UploadFile = File(...),
    keyword_xray_csv: UploadFile | None = File(default=None),
    target_product_input: str = Form(default=""),
    channels: list[str] = Form(default=[]),
    creative_mockup_url: str = Form(default=""),
    case_study_url: str = Form(default=""),
    offers: list[str] = Form(default=[]),
    include_recommended_plan: bool = Form(default=True),
) -> ApiMessage:
    _enforce_api_key(request, x_internal_api_key)
    competitor_bytes = await competitor_xray_csv.read()
    keyword_bytes = await keyword_xray_csv.read() if keyword_xray_csv is not None else None
    settings = request.app.state.settings
    try:
        with session_scope(request.app.state.session_factory) as session:
            result = DeckGenerationService(settings, session).generate_deck(
                competitor_xray_csv_bytes=competitor_bytes,
                competitor_xray_filename=competitor_xray_csv.filename or "competitors.csv",
                keyword_xray_csv_bytes=keyword_bytes,
                keyword_xray_filename=keyword_xray_csv.filename if keyword_xray_csv is not None else "",
                target_product_input=target_product_input,
                channels=channels,
                creative_mockup_url=creative_mockup_url,
                case_study_url=case_study_url,
                offers=offers,
                include_recommended_plan=include_recommended_plan,
                trigger="internal_api",
            )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return ApiMessage(
        status="ok",
        message=result.message,
        details={
            "run_id": result.run_id,
            "status": result.status,
            "output_type": result.output_type,
            "design_id": result.design_id,
            "design_title": result.design_title,
            "edit_url": result.edit_url,
            "view_url": result.view_url,
            "warnings": result.warnings,
            "sales_row_count": result.sales_row_count,
            "competitor_row_count": result.competitor_row_count,
            "template_fields": result.template_fields,
        },
    )


@router.get("/admin/api/deck-runs", response_model=ApiMessage)
def admin_deck_runs(request: Request) -> ApiMessage:
    _require_admin_enabled(request)
    if not _is_admin_authenticated(request):
        raise HTTPException(status_code=401, detail="Admin login required.")

    settings = request.app.state.settings
    with session_scope(request.app.state.session_factory) as session:
        service = DeckGenerationService(settings, session)
        details = {"runs": service.list_recent_runs(limit=10)}
    return ApiMessage(status="ok", message="Deck generation runs loaded.", details=details)


@router.post("/api/discovery/clickup-schema", response_model=ApiMessage)
def discover_clickup_schema(
    payload: DiscoveryRequest,
    request: Request,
    x_internal_api_key: str | None = Header(default=None),
) -> ApiMessage:
    _enforce_api_key(request, x_internal_api_key)
    _validate_runtime(request)
    settings = request.app.state.settings
    discovery = ClickUpDiscoveryService(settings, ClickUpClient(settings)).run(sample_size=payload.sample_size)
    return ApiMessage(status="ok", message="Discovery snapshot captured.", details=discovery)


@router.post("/api/clickup/sync", response_model=ApiMessage)
def sync_clickup_tasks(
    payload: SyncRequest,
    request: Request,
    x_internal_api_key: str | None = Header(default=None),
) -> ApiMessage:
    _enforce_api_key(request, x_internal_api_key)
    _validate_runtime(request)
    settings = request.app.state.settings
    with session_scope(request.app.state.session_factory) as session:
        summary = ClickUpSyncService(settings, ClickUpClient(settings), session).sync_list(
            include_closed=payload.include_closed,
            max_tasks=payload.max_tasks,
        )
    return ApiMessage(status="ok", message="ClickUp sync completed.", details=summary)


@router.post("/api/jobs/stale-leads/run", response_model=ApiMessage)
def run_stale_lead_job(
    payload: StaleLeadRunRequest,
    request: Request,
    x_internal_api_key: str | None = Header(default=None),
) -> ApiMessage:
    _enforce_api_key(request, x_internal_api_key)
    _validate_runtime(request)
    settings = request.app.state.settings
    with session_scope(request.app.state.session_factory) as session:
        result = StaleLeadJob(settings, ClickUpClient(settings), SlackClient(settings), session).run(
            dry_run=payload.dry_run,
            as_of_date=payload.as_of_date,
            max_tasks=payload.max_tasks,
        )
    return ApiMessage(status="ok", message="Stale lead scan completed.", details=result)


@router.post("/api/jobs/gmail-sync/run", response_model=ApiMessage)
def run_gmail_sync_job(
    payload: GmailSyncRequest,
    request: Request,
    x_internal_api_key: str | None = Header(default=None),
) -> ApiMessage:
    _enforce_api_key(request, x_internal_api_key)
    settings = request.app.state.settings
    with session_scope(request.app.state.session_factory) as session:
        result = GmailMailboxSyncJob(
            settings,
            ClickUpClient(settings),
            SlackClient(settings),
            GmailClient(settings),
            session,
        ).run(
            dry_run=payload.dry_run,
            query=payload.query or None,
            max_messages=payload.max_messages,
        )
    return ApiMessage(status="ok", message="Gmail mailbox sync completed.", details=result)


@router.post("/api/jobs/daily-digest/run", response_model=ApiMessage)
def run_daily_digest_job(
    payload: DailyDigestRunRequest,
    request: Request,
    x_internal_api_key: str | None = Header(default=None),
) -> ApiMessage:
    _enforce_api_key(request, x_internal_api_key)
    settings = request.app.state.settings
    with session_scope(request.app.state.session_factory) as session:
        result = DailyDigestJob(
            settings,
            ClickUpClient(settings),
            GmailClient(settings),
            session,
        ).run(
            as_of_date=payload.as_of_date,
            include_stale=payload.include_stale,
            include_mailbox=payload.include_mailbox,
            max_items=payload.max_items,
        )
    return ApiMessage(status="ok", message="Daily digest completed.", details=result)


@router.post("/api/communications/events", response_model=ApiMessage)
def ingest_communication_event(
    payload: CommunicationEventRequest,
    request: Request,
    x_internal_api_key: str | None = Header(default=None),
) -> ApiMessage:
    _enforce_api_key(request, x_internal_api_key)
    _validate_runtime(request)
    settings = request.app.state.settings
    with session_scope(request.app.state.session_factory) as session:
        result = CommunicationService(settings, ClickUpClient(settings), SlackClient(settings), session).process_event(payload)
    return ApiMessage(status="ok", message="Communication event processed.", details=result)


@router.post("/api/integrations/instantly/webhook", response_model=ApiMessage)
async def ingest_instantly_webhook(request: Request) -> ApiMessage:
    _enforce_instantly_webhook_auth(request)
    _validate_runtime(request)
    payload = await request.json()
    settings = request.app.state.settings
    with session_scope(request.app.state.session_factory) as session:
        result = InstantlyWebhookService(settings, ClickUpClient(settings), SlackClient(settings), session).process_webhook(payload)
    message = "Instantly webhook processed." if result.get("status") == "processed" else "Instantly webhook ignored."
    return ApiMessage(status="ok", message=message, details=result)
