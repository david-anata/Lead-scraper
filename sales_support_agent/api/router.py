"""API routes."""

from __future__ import annotations

import logging
import secrets
import hashlib
from typing import Optional
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import re
from urllib.parse import parse_qs

import requests
from fastapi import APIRouter, File, Form, Header, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse, Response
from sqlalchemy import func, inspect, select

from main import (
    ICPBuildRequest as LeadBuildRequest,
    enqueue_lead_build,
    fetch_lead_run_status,
    get_lead_run_csv,
    get_missing_required_settings as get_missing_lead_builder_settings,
    load_settings as load_lead_builder_settings,
)

from sales_support_agent.config import get_missing_runtime_settings
from sales_support_agent.integrations.clickup import ClickUpAPIError, ClickUpClient
from sales_support_agent.integrations.gmail import GmailClient, GmailIntegrationError
from sales_support_agent.integrations.slack import SlackClient
from sales_support_agent.jobs.daily_digest import DailyDigestJob
from sales_support_agent.jobs.mailbox_sync import GmailMailboxSyncJob
from sales_support_agent.jobs.stale_leads import StaleLeadJob
from sales_support_agent.models.database import session_scope
from sales_support_agent.models.entities import (
    AutomationRun,
    DeckSectionView,
    DeckVisitSession,
    LeadMirror,
)
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
    get_session_user,
    read_signed_state_token,
    validate_admin_session_token,
    verify_admin_password,
)
from sales_support_agent.services.admin_auth_google import google_oauth_enabled
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
from sales_support_agent.services.fulfillment_dashboard import (
    fulfillment_report_entries,
    latest_fulfillment_report_entry,
    load_fulfillment_report_artifact,
    load_fulfillment_report_by_slug,
    load_latest_fulfillment_report,
    render_fulfillment_dashboard_page,
    render_fulfillment_not_found_page,
    render_fulfillment_report_detail_page,
    render_fulfillment_reports_page,
)
from sales_support_agent.services.gmail_drafts import create_bulk_draft_payloads
from sales_support_agent.services.instantly_webhooks import InstantlyWebhookService
from sales_support_agent.services.sync import ClickUpSyncService
from sales_support_agent.services.website_ops import (
    get_feedback_record,
    latest_report_entry,
    render_dashboard_page as render_website_ops_dashboard_page,
    render_feedback_detail_page,
    render_queue_page as render_website_ops_queue_page,
    render_report_page,
    render_reports_page,
    review_feedback_record,
    run_website_ops,
    save_feedback_record,
)
from sales_support_agent.config import is_active_pipeline_status, normalize_status_key
from sales_support_agent.services.auth_deps import get_session_user_from_request, is_authenticated


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


def _enforce_api_key(request: Request, internal_api_key: Optional[str]) -> None:
    configured = request.app.state.settings.internal_api_key
    if configured and internal_api_key != configured:
        raise HTTPException(status_code=401, detail="Invalid internal API key.")


def _enforce_api_key_from_header_or_query(request: Request, internal_api_key: Optional[str]) -> None:
    provided = internal_api_key or request.query_params.get("token") or ""
    _enforce_api_key(request, provided)


def _validate_runtime(request: Request) -> None:
    missing = get_missing_runtime_settings(request.app.state.settings)
    if missing:
        raise HTTPException(
            status_code=500,
            detail=f"Missing required environment variables for sales support agent: {', '.join(missing)}",
        )


def _lead_builder_status(settings: Optional[object] = None) -> dict[str, object]:
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
    return is_authenticated(request)


def _get_request_user(request: Request) -> Optional[dict]:
    return get_session_user_from_request(request)


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


def _normalize_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _latest_clickup_sync_at(request: Request) -> Optional[datetime]:
    with session_scope(request.app.state.session_factory) as session:
        latest_sync = session.execute(select(func.max(LeadMirror.last_sync_at))).scalar_one_or_none()
    return _normalize_utc(latest_sync)


def _dashboard_sync_is_stale(request: Request, latest_sync_at: Optional[datetime]) -> bool:
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


def _dashboard_sync_error_message(request: Request) -> str:
    with request.app.state.dashboard_sync_lock:
        return str(request.app.state.dashboard_sync_last_error or "").strip()


def _clickup_probe_error(settings) -> str:
    if not settings.clickup_api_token or not settings.clickup_list_id:
        return ""
    try:
        ClickUpClient(settings).get_list(settings.clickup_list_id)
    except ClickUpAPIError as exc:
        return str(exc)
    return ""


def _dashboard_needs_inline_sync(details: dict[str, object]) -> bool:
    return not str(details.get("latest_sync_at") or "").strip() and int(details.get("total_active_leads", 0) or 0) == 0


def _executive_needs_inline_sync(details: dict[str, object]) -> bool:
    kpis = dict(details.get("kpis", {}) or {})
    return not str(details.get("latest_sync_at") or "").strip() and int(kpis.get("active_leads", 0) or 0) == 0


def _inline_sync_dashboard_data(request: Request, settings) -> dict[str, object]:
    with session_scope(request.app.state.session_factory) as session:
        ClickUpSyncService(settings, ClickUpClient(settings), session).sync_list(include_closed=True)
        dashboard = build_dashboard_data(
            settings=settings,
            session=session,
            lead_builder_status=_lead_builder_status(settings),
            clickup_client=ClickUpClient(settings),
        )
    return dashboard_data_to_dict(dashboard)


def _inline_sync_executive_data(request: Request, settings) -> dict[str, object]:
    with session_scope(request.app.state.session_factory) as session:
        ClickUpSyncService(settings, ClickUpClient(settings), session).sync_list(include_closed=True)
        executive = build_executive_data(
            settings=settings,
            session=session,
            clickup_client=ClickUpClient(settings),
        )
    return executive_data_to_dict(executive)


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


def _fetch_remote_lead_run_status(request: Request, run_id: str) -> Optional[dict[str, object]]:
    lead_build_url = _remote_lead_builder_url(request)
    if not lead_build_url:
        return None
    response = requests.get(f"{lead_build_url}/lead-runs/{run_id}", timeout=30)
    if response.status_code == 404:
        return None
    response.raise_for_status()
    payload = response.json()
    return dict(payload.get("details") or {})


def _download_remote_lead_run(request: Request, run_id: str) -> Optional[Response]:
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
        if is_active_pipeline_status(
            lead.status or "",
            active_statuses=settings.active_statuses,
            inactive_statuses=settings.inactive_statuses,
        )
    )
    synced_tasks = int(clickup_summary.get("synced_tasks", 0) or 0)
    if synced_tasks == 0:
        message = "Dashboard sync finished, but ClickUp returned 0 tasks. Check CLICKUP_LIST_ID and ClickUp token access."
    elif active_leads == 0:
        message = "Dashboard sync finished, but 0 tasks are still classified as in-flight opportunities. Check your ClickUp status names."
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
    ticket1_details: dict[str, object] = {}
    db_details: dict[str, object] = {}
    try:
        with session_scope(request.app.state.session_factory) as session:
            bind = session.get_bind()
            db_details = {
                "database_backend": bind.dialect.name,
                "sales_agent_db_url_configured": bool(str(getattr(settings, "sales_agent_db_url", "") or "").strip()),
            }
            inspector = inspect(bind)
            existing_columns = {
                column["name"]
                for column in inspector.get_columns("lead_mirrors")
            }
            latest_sync_at = session.execute(select(func.max(LeadMirror.last_sync_at))).scalar_one_or_none()
            ticket1_details = {
                "lead_mirror_ticket1_columns_present": {
                    "status_key": "status_key" in existing_columns,
                    "is_closed": "is_closed" in existing_columns,
                    "is_active": "is_active" in existing_columns,
                    "task_updated_at": "task_updated_at" in existing_columns,
                },
                "lead_mirror_record_count": int(
                    session.execute(select(func.count()).select_from(LeadMirror)).scalar_one() or 0
                ),
                "lead_mirror_current_list_count": int(
                    session.execute(
                        select(func.count()).select_from(LeadMirror).where(LeadMirror.list_id == settings.clickup_list_id)
                    ).scalar_one()
                    or 0
                ),
                "lead_mirror_other_list_count": int(
                    session.execute(
                        select(func.count()).select_from(LeadMirror).where(LeadMirror.list_id != settings.clickup_list_id)
                    ).scalar_one()
                    or 0
                ),
                "lead_mirror_ticket1_populated_counts": {
                    "status_key_nonempty": int(
                        session.execute(
                            select(func.count()).select_from(LeadMirror).where(LeadMirror.status_key != "")
                        ).scalar_one()
                        or 0
                    ),
                    "is_active_true": int(
                        session.execute(
                            select(func.count()).select_from(LeadMirror).where(LeadMirror.is_active.is_(True))
                        ).scalar_one()
                        or 0
                    ),
                    "is_closed_true": int(
                        session.execute(
                            select(func.count()).select_from(LeadMirror).where(LeadMirror.is_closed.is_(True))
                        ).scalar_one()
                        or 0
                    ),
                    "task_updated_at_nonnull": int(
                        session.execute(
                            select(func.count()).select_from(LeadMirror).where(LeadMirror.task_updated_at.is_not(None))
                        ).scalar_one()
                        or 0
                    ),
                    "raw_task_payload_present": int(
                        session.execute(
                            select(func.count()).select_from(LeadMirror).where(LeadMirror.raw_task_payload.is_not(None))
                        ).scalar_one()
                        or 0
                    ),
                },
                "lead_mirror_latest_sync_at": latest_sync_at.isoformat() if latest_sync_at else "",
            }
    except Exception as exc:
        ticket1_details = {"lead_mirror_ticket1_validation_error": str(exc)}
    return ApiMessage(
        status="ok",
        message="healthy",
        details={
            "clickup_configured": bool(settings.clickup_api_token and settings.clickup_list_id),
            "slack_configured": bool(settings.slack_bot_token and settings.slack_channel_id),
            "discovery_snapshot_path": str(settings.discovery_snapshot_path),
            "deck_generator_configured": brand_package_path.exists(),
            "deck_brand_package_path": str(brand_package_path),
            **db_details,
            **ticket1_details,
        },
    )


@router.get("/admin/login", response_class=HTMLResponse)
def admin_login_page(request: Request) -> HTMLResponse:
    _require_admin_enabled(request)
    if _is_admin_authenticated(request):
        return HTMLResponse("", status_code=302, headers={"Location": "/admin"})
    settings = request.app.state.settings
    return HTMLResponse(render_login_page(show_google_button=google_oauth_enabled(settings)))


@router.post("/admin/login", response_class=HTMLResponse)
async def admin_login_submit(request: Request) -> Response:
    _require_admin_enabled(request)
    body = (await request.body()).decode("utf-8")
    password = parse_qs(body).get("password", [""])[0]
    if not verify_admin_password(request.app.state.settings, password):
        settings = request.app.state.settings
        return HTMLResponse(render_login_page(error_message="Incorrect password.", show_google_button=google_oauth_enabled(settings)), status_code=401)

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
    return HTMLResponse(render_dashboard_page(dashboard, user=_get_request_user(request)))


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
    return HTMLResponse(render_executive_page(executive, user=_get_request_user(request)))


@router.get("/admin/fulfillment", response_class=HTMLResponse)
def admin_fulfillment_root(request: Request) -> Response:
    _require_admin_enabled(request)
    if not _is_admin_authenticated(request):
        return RedirectResponse(url="/admin/login", status_code=302)
    return RedirectResponse(url="/admin/fulfillment/sales", status_code=302)


@router.get("/admin/fulfillment-cs{rest:path}")
def admin_fulfillment_cs_legacy_redirect(rest: str) -> Response:
    # The CS pages moved under the renamed Fulfillment section; keep old links working.
    return RedirectResponse(url=f"/admin/fulfillment/cs{rest}", status_code=301)


@router.get("/admin/fulfillment/cs", response_class=HTMLResponse)
def admin_fulfillment_cs_root(request: Request) -> Response:
    _require_admin_enabled(request)
    if not _is_admin_authenticated(request):
        return RedirectResponse(url="/admin/login", status_code=302)
    return RedirectResponse(url="/admin/fulfillment/cs/", status_code=302)


@router.get("/admin/fulfillment/cs/", response_class=HTMLResponse)
def admin_fulfillment_cs_dashboard(request: Request) -> Response:
    _require_admin_enabled(request)
    if not _is_admin_authenticated(request):
        return RedirectResponse(url="/admin/login", status_code=302)
    reports_dir = request.app.state.settings.fulfillment_cs_reports_dir
    latest_report = load_latest_fulfillment_report(reports_dir)
    entries = fulfillment_report_entries(reports_dir)
    return HTMLResponse(render_fulfillment_dashboard_page(latest_report, entries))


@router.get("/admin/fulfillment/cs/reports", response_class=HTMLResponse)
def admin_fulfillment_cs_reports_root(request: Request) -> Response:
    _require_admin_enabled(request)
    if not _is_admin_authenticated(request):
        return RedirectResponse(url="/admin/login", status_code=302)
    return RedirectResponse(url="/admin/fulfillment/cs/reports/", status_code=302)


@router.get("/admin/fulfillment/cs/reports/", response_class=HTMLResponse)
def admin_fulfillment_cs_reports(request: Request) -> Response:
    _require_admin_enabled(request)
    if not _is_admin_authenticated(request):
        return RedirectResponse(url="/admin/login", status_code=302)
    reports_dir = request.app.state.settings.fulfillment_cs_reports_dir
    return HTMLResponse(render_fulfillment_reports_page(fulfillment_report_entries(reports_dir)))


@router.get("/admin/fulfillment/cs/reports/latest", response_class=HTMLResponse)
def admin_fulfillment_cs_latest_report(request: Request) -> Response:
    _require_admin_enabled(request)
    if not _is_admin_authenticated(request):
        return RedirectResponse(url="/admin/login", status_code=302)
    latest_entry = latest_fulfillment_report_entry(request.app.state.settings.fulfillment_cs_reports_dir)
    if latest_entry is None:
        return RedirectResponse(url="/admin/fulfillment/cs/reports/", status_code=302)
    return RedirectResponse(url=f"/admin/fulfillment/cs/reports/{latest_entry.slug}", status_code=302)


@router.get("/admin/fulfillment/cs/reports/{report_slug}.json")
def admin_fulfillment_cs_report_json(request: Request, report_slug: str) -> Response:
    _require_admin_enabled(request)
    if not _is_admin_authenticated(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    artifact = load_fulfillment_report_artifact(request.app.state.settings.fulfillment_cs_reports_dir, report_slug, "json")
    if artifact is None:
        return JSONResponse(status_code=404, content={"detail": "The requested fulfillment report JSON was not found."})
    body, content_type = artifact
    return Response(content=body, media_type=content_type)


@router.get("/admin/fulfillment/cs/reports/{report_slug}.md", response_class=PlainTextResponse)
def admin_fulfillment_cs_report_markdown(request: Request, report_slug: str) -> Response:
    _require_admin_enabled(request)
    if not _is_admin_authenticated(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    artifact = load_fulfillment_report_artifact(request.app.state.settings.fulfillment_cs_reports_dir, report_slug, "md")
    if artifact is None:
        return PlainTextResponse("The requested fulfillment report markdown was not found.", status_code=404)
    body, content_type = artifact
    return Response(content=body, media_type=content_type)


@router.get("/admin/fulfillment/cs/reports/{report_slug}.html", response_class=HTMLResponse)
def admin_fulfillment_cs_report_html(request: Request, report_slug: str) -> Response:
    _require_admin_enabled(request)
    if not _is_admin_authenticated(request):
        return RedirectResponse(url="/admin/login", status_code=302)

    artifact = load_fulfillment_report_artifact(request.app.state.settings.fulfillment_cs_reports_dir, report_slug, "html")
    if artifact is None:
        return HTMLResponse(render_fulfillment_not_found_page("The requested fulfillment report HTML was not found."), status_code=404)
    body, content_type = artifact
    return Response(content=body, media_type=content_type)


@router.get("/admin/fulfillment/cs/reports/{report_slug}", response_class=HTMLResponse)
def admin_fulfillment_cs_report_detail(request: Request, report_slug: str) -> Response:
    _require_admin_enabled(request)
    if not _is_admin_authenticated(request):
        return RedirectResponse(url="/admin/login", status_code=302)
    report = load_fulfillment_report_by_slug(request.app.state.settings.fulfillment_cs_reports_dir, report_slug)
    if report is None:
        return HTMLResponse(render_fulfillment_not_found_page("The requested fulfillment report was not found."), status_code=404)
    return HTMLResponse(render_fulfillment_report_detail_page(report))


@router.get("/admin/website-ops", response_class=HTMLResponse)
def admin_website_ops(request: Request) -> Response:
    _require_admin_enabled(request)
    if not _is_admin_authenticated(request):
        return RedirectResponse(url="/admin/login", status_code=302)
    return HTMLResponse(render_website_ops_dashboard_page(request.app.state.settings))


@router.get("/admin/website-ops/queue", response_class=HTMLResponse)
def admin_website_ops_queue(request: Request, status: str = "") -> Response:
    _require_admin_enabled(request)
    if not _is_admin_authenticated(request):
        return RedirectResponse(url="/admin/login", status_code=302)
    return HTMLResponse(render_website_ops_queue_page(request.app.state.settings, status_filter=status))


@router.get("/admin/website-ops/reports", response_class=HTMLResponse)
def admin_website_ops_reports(request: Request) -> Response:
    _require_admin_enabled(request)
    if not _is_admin_authenticated(request):
        return RedirectResponse(url="/admin/login", status_code=302)
    return HTMLResponse(render_reports_page(request.app.state.settings))


@router.get("/admin/website-ops/reports/latest")
def admin_website_ops_reports_latest(request: Request) -> Response:
    _require_admin_enabled(request)
    if not _is_admin_authenticated(request):
        return RedirectResponse(url="/admin/login", status_code=302)
    latest = latest_report_entry(request.app.state.settings)
    if not latest:
        return RedirectResponse(url="/admin/website-ops/reports", status_code=302)
    return RedirectResponse(url=f"/admin/website-ops/reports/{latest['mode']}/{latest['slug']}", status_code=302)


@router.get("/admin/website-ops/reports/{mode}/{slug}", response_class=HTMLResponse)
def admin_website_ops_report_detail(request: Request, mode: str, slug: str) -> Response:
    _require_admin_enabled(request)
    if not _is_admin_authenticated(request):
        return RedirectResponse(url="/admin/login", status_code=302)
    return HTMLResponse(render_report_page(request.app.state.settings, mode, slug))


@router.get("/admin/website-ops/feedback/{feedback_id}", response_class=HTMLResponse)
def admin_website_ops_feedback_detail(request: Request, feedback_id: str) -> Response:
    _require_admin_enabled(request)
    if not _is_admin_authenticated(request):
        return RedirectResponse(url="/admin/login", status_code=302)
    return HTMLResponse(render_feedback_detail_page(request.app.state.settings, feedback_id))


@router.get("/api/admin/dashboard-data", response_model=ApiMessage)
def admin_dashboard_data(
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
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
    details = dashboard_data_to_dict(dashboard)
    sync_error = _dashboard_sync_error_message(request)
    if not sync_error and _dashboard_needs_inline_sync(details):
        try:
            details = _inline_sync_dashboard_data(request, settings)
        except ClickUpAPIError as exc:
            sync_error = str(exc)
        except Exception as exc:
            sync_error = f"Inline dashboard sync failed: {exc}"
    if not sync_error and _dashboard_needs_inline_sync(details):
        sync_error = _clickup_probe_error(settings)
    if sync_error:
        latest_run_summary = dict(details.get("latest_run_summary", {}) or {})
        latest_run_summary.setdefault("dashboard_error", sync_error)
        details["latest_run_summary"] = latest_run_summary
    return ApiMessage(status="ok", message="Admin dashboard data loaded.", details=details)


@router.post("/admin/api/website-ops/run")
def admin_website_ops_run(request: Request, mode: str = Form(default="daily")) -> Response:
    _require_admin_enabled(request)
    if not _is_admin_authenticated(request):
        return JSONResponse(status_code=401, content={"detail": "Admin login required."})
    normalized_mode = (mode or "daily").strip().lower()
    if normalized_mode not in {"daily", "weekly", "monthly"}:
        return JSONResponse(status_code=400, content={"detail": "Unsupported run mode."})
    result = run_website_ops(request.app.state.settings, mode=normalized_mode)
    return RedirectResponse(url="/admin/website-ops", status_code=302)


@router.post("/admin/api/website-ops/feedback")
def admin_website_ops_feedback_submit(
    request: Request,
    category: str = Form(default="SEO"),
    priority: str = Form(default="Medium"),
    page_url: str = Form(default=""),
    page_title: str = Form(default=""),
    summary: str = Form(default=""),
    details: str = Form(default=""),
    desired_outcome: str = Form(default=""),
    recommended_fix: str = Form(default=""),
    reporter_name: str = Form(default=""),
    reporter_email: str = Form(default=""),
) -> Response:
    _require_admin_enabled(request)
    if not _is_admin_authenticated(request):
        return JSONResponse(status_code=401, content={"detail": "Admin login required."})
    record = save_feedback_record(
        request.app.state.settings,
        {
            "category": category,
            "priority": priority,
            "page_url": page_url,
            "page_title": page_title,
            "summary": summary,
            "details": details,
            "desired_outcome": desired_outcome,
            "recommended_fix": recommended_fix,
            "reporter_name": reporter_name,
            "reporter_email": reporter_email,
        },
    )
    return RedirectResponse(url=f"/admin/website-ops/feedback/{record['feedback_id']}", status_code=302)


@router.post("/admin/api/website-ops/feedback/{feedback_id}/review")
def admin_website_ops_feedback_review(
    request: Request,
    feedback_id: str,
    status: str = Form(default="new"),
    reviewer_name: str = Form(default=""),
    review_notes: str = Form(default=""),
    action_type: str = Form(default=""),
    action_value: str = Form(default=""),
    target_post_id: str = Form(default=""),
) -> Response:
    _require_admin_enabled(request)
    if not _is_admin_authenticated(request):
        return JSONResponse(status_code=401, content={"detail": "Admin login required."})
    result = review_feedback_record(
        request.app.state.settings,
        feedback_id,
        {
            "status": status,
            "reviewer_name": reviewer_name,
            "review_notes": review_notes,
            "action_type": action_type,
            "action_value": action_value,
            "target_post_id": target_post_id,
        },
    )
    if not result.ok and not result.record:
        return JSONResponse(status_code=404, content={"detail": result.message})
    return RedirectResponse(url=f"/admin/website-ops/feedback/{feedback_id}", status_code=302)


@router.get("/api/admin/executive-data", response_model=ApiMessage)
def admin_executive_data(
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> ApiMessage:
    _enforce_api_key(request, x_internal_api_key)
    settings = request.app.state.settings
    with session_scope(request.app.state.session_factory) as session:
        executive = build_executive_data(
            settings=settings,
            session=session,
            clickup_client=ClickUpClient(settings),
        )
    details = executive_data_to_dict(executive)
    sync_error = _dashboard_sync_error_message(request)
    if not sync_error and _executive_needs_inline_sync(details):
        try:
            details = _inline_sync_executive_data(request, settings)
        except ClickUpAPIError as exc:
            sync_error = str(exc)
        except Exception as exc:
            sync_error = f"Inline executive sync failed: {exc}"
    if not sync_error and _executive_needs_inline_sync(details):
        sync_error = _clickup_probe_error(settings)
    if sync_error:
        latest_run_summary = dict(details.get("latest_run_summary", {}) or {})
        latest_run_summary.setdefault("executive_error", sync_error)
        details["latest_run_summary"] = latest_run_summary
    return ApiMessage(
        status="ok",
        message="Executive summary data loaded.",
        details=details,
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
    x_internal_api_key: Optional[str] = Header(default=None),
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


def _hash_deck_visitor_key(request: Request) -> str:
    client_host = (request.client.host if request.client else "") or ""
    user_agent = request.headers.get("user-agent", "") or ""
    source = f"{client_host}|{user_agent}"
    return hashlib.sha256(source.encode("utf-8")).hexdigest()[:16]


# ============================================================
# PR54: deck-engagement analytics helpers (UA / geo / referrer)
# ============================================================

# Cap a session at 6h. Anything beyond is almost certainly a stuck tab,
# not real engagement, and we don't want a misbehaving client (or hostile
# one) to inflate "time spent" beyond physical reality.
# Shared with the fulfillment rate-sheet router — see services/visitor_meta.py.
from sales_support_agent.services.visitor_meta import (
    MAX_SESSION_SECONDS as _MAX_SESSION_SECONDS,
    categorize_referrer as _categorize_referrer,
    extract_visitor_geo as _extract_visitor_geo,
    parse_user_agent as _parse_user_agent,
)


def _extract_client_ip(request: Request) -> str:
    """Pull the visitor IP through proxy headers in trust order:
    CF-Connecting-IP > X-Forwarded-For (first hop) > request.client.host.
    Used only for diagnostics; we don't store IP."""
    cf = (request.headers.get("cf-connecting-ip") or "").strip()
    if cf:
        return cf
    fwd = (request.headers.get("x-forwarded-for") or "").strip()
    if fwd:
        return fwd.split(",")[0].strip()
    return (request.client.host if request.client else "") or ""


def _summarize_deck_view_events(events: list[dict[str, str]]) -> dict[str, object]:
    now = datetime.now(timezone.utc)
    grouped: dict[str, dict[str, object]] = {}
    for viewer_type in ("internal", "external"):
        filtered = [event for event in events if str(event.get("viewer_type") or "external") == viewer_type]
        unique_visitors = {str(event.get("visitor_key") or "") for event in filtered if str(event.get("visitor_key") or "")}
        parsed_dates: list[datetime] = []
        daily_rows: list[tuple[datetime, str]] = []
        for event in filtered:
            raw = str(event.get("viewed_at") or "").strip()
            if not raw:
                continue
            try:
                parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            except ValueError:
                continue
            parsed_dates.append(parsed)
            daily_rows.append((parsed, parsed.date().isoformat()))

        daily_counts: dict[str, dict[str, int]] = {}
        for window_name, days in (("7", 7), ("30", 30), ("90", 90), ("all", None)):
            counter: dict[str, int] = {}
            for parsed, day_key in daily_rows:
                if days is not None and (now - parsed).days >= days:
                    continue
                counter[day_key] = counter.get(day_key, 0) + 1
            daily_counts[window_name] = dict(sorted(counter.items()))

        grouped[viewer_type] = {
            "unique_visitors": len(unique_visitors),
            "total_visits": len(filtered),
            "first_viewed_at": min(parsed_dates).isoformat() if parsed_dates else "",
            "last_viewed_at": max(parsed_dates).isoformat() if parsed_dates else "",
            "daily_counts": daily_counts,
        }
    return grouped


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
        viewer_type = "internal" if str(request.query_params.get("viewer") or "").strip().lower() == "internal" else "external"
        view_events = list(summary.get("view_events", []) or [])
        view_events.append(
            {
                "viewed_at": now_iso,
                "viewer_type": viewer_type,
                "visitor_key": _hash_deck_visitor_key(request),
                "path": str(request.url.path),
            }
        )
        summary["view_events"] = view_events[-500:]
        analytics = _summarize_deck_view_events(summary["view_events"])
        summary["view_analytics"] = analytics
        external_summary = dict(analytics.get("external", {}) or {})
        summary["view_count"] = int(external_summary.get("total_visits", 0) or 0)
        summary["first_viewed_at"] = str(external_summary.get("first_viewed_at", "") or "")
        summary["last_viewed_at"] = str(external_summary.get("last_viewed_at", "") or "")
        run.summary_json = summary
        session.add(run)
        return HTMLResponse(deck_html)


@router.get("/deck-exports/{run_id}/{token}", response_class=HTMLResponse)
def deck_export_view(request: Request, run_id: int, token: str) -> Response:
    return _render_deck_export(request, run_id, token)


@router.get("/decks/{deck_slug}/{run_id}/{token}", response_class=HTMLResponse)
def deck_export_slug_view(request: Request, deck_slug: str, run_id: int, token: str) -> Response:
    return _render_deck_export(request, run_id, token)


# PR54: deck-engagement heartbeat endpoint.
# Client posts here every 15s active / 60s idle with current session state.
# Server upserts the session row + per-section dwell rows. No response body
# beyond status — keeps the wire small.
@router.post("/decks/{deck_slug}/{run_id}/{token}/heartbeat")
async def deck_heartbeat(
    request: Request,
    deck_slug: str,
    run_id: int,
    token: str,
) -> JSONResponse:
    # Validate deck identity (token must match) before doing any DB work.
    with session_scope(request.app.state.session_factory) as session:
        run = session.execute(
            select(AutomationRun).where(
                AutomationRun.id == run_id,
                AutomationRun.run_type == "deck_generation",
            )
        ).scalar_one_or_none()
        if run is None:
            return JSONResponse(status_code=404, content={"detail": "Deck not found."})
        summary = dict(run.summary_json or {})
        if summary.get("export_token") != token:
            return JSONResponse(status_code=404, content={"detail": "Deck not found."})

        try:
            payload = await request.json()
        except Exception:
            payload = {}
        visitor_token = str(payload.get("visitor_token") or "").strip()[:64]
        if not visitor_token:
            return JSONResponse(status_code=400, content={"detail": "visitor_token required."})

        is_internal = bool(payload.get("is_internal", False))
        # Client-tracked cumulative seconds (capped server-side so a hostile
        # or buggy client can't claim absurd durations).
        client_total_seconds = int(payload.get("total_seconds", 0) or 0)
        client_total_seconds = max(0, min(client_total_seconds, _MAX_SESSION_SECONDS))
        max_scroll = int(payload.get("max_scroll_pct", 0) or 0)
        max_scroll = max(0, min(max_scroll, 100))

        # Section dwell — dict of {section_id: total_seconds_visible}.
        # Capped per-section at the same ceiling so one section can't
        # individually exceed the session ceiling.
        sections_payload: dict[str, int] = {}
        raw_sections = payload.get("sections") or {}
        if isinstance(raw_sections, dict):
            for sec_id, secs in raw_sections.items():
                sec_id_clean = str(sec_id)[:64]
                if not sec_id_clean:
                    continue
                try:
                    secs_int = int(secs)
                except (TypeError, ValueError):
                    continue
                sections_payload[sec_id_clean] = max(0, min(secs_int, _MAX_SESSION_SECONDS))

        now = datetime.now(timezone.utc)

        # Upsert by (run_id, visitor_token). One session row per
        # visitor-cookie+deck pair; reopening the deck after weeks updates
        # the same row's last_heartbeat_at and accumulates seconds.
        # (Future: split into multiple sessions if the gap exceeds a
        # threshold like 30 min — for PR54 a single rolling session is fine.)
        existing = session.execute(
            select(DeckVisitSession).where(
                DeckVisitSession.run_id == run_id,
                DeckVisitSession.visitor_token == visitor_token,
            )
        ).scalar_one_or_none()

        if existing is None:
            # First heartbeat — capture identity (geo, UA, referrer).
            ua_raw = (request.headers.get("user-agent") or "")[:512]
            ua_parts = _parse_user_agent(ua_raw)
            geo = _extract_visitor_geo(request)
            referrer_url = str(payload.get("referrer") or request.headers.get("referer") or "")
            ref_host, ref_cat = _categorize_referrer(referrer_url)
            existing = DeckVisitSession(
                run_id=run_id,
                visitor_token=visitor_token,
                is_internal=is_internal,
                started_at=now,
                last_heartbeat_at=now,
                total_seconds=client_total_seconds,
                max_scroll_pct=max_scroll,
                ip_country=geo["country"],
                ip_region=geo["region"],
                ip_city=geo["city"],
                device=ua_parts["device"],
                os=ua_parts["os"],
                browser=ua_parts["browser"],
                user_agent_raw=ua_raw,
                referrer_host=ref_host[:128],
                referrer_category=ref_cat,
            )
            session.add(existing)
            session.flush()  # populate existing.id for the section rows below
        else:
            # Subsequent heartbeat — bump cumulative fields.
            existing.last_heartbeat_at = now
            # Trust the client's cumulative count (already capped). Use
            # max() so a stale heartbeat (out-of-order delivery) can't
            # decrease the stored value.
            if client_total_seconds > existing.total_seconds:
                existing.total_seconds = client_total_seconds
            if max_scroll > existing.max_scroll_pct:
                existing.max_scroll_pct = max_scroll
            session.add(existing)

        # Section dwell — upsert each section's row.
        for sec_id, secs in sections_payload.items():
            sec_row = session.execute(
                select(DeckSectionView).where(
                    DeckSectionView.session_id == existing.id,
                    DeckSectionView.section_id == sec_id,
                )
            ).scalar_one_or_none()
            if sec_row is None:
                sec_row = DeckSectionView(
                    session_id=existing.id,
                    section_id=sec_id,
                    first_seen_at=now,
                    last_seen_at=now,
                    total_seconds=secs,
                )
                session.add(sec_row)
            else:
                sec_row.last_seen_at = now
                if secs > sec_row.total_seconds:
                    sec_row.total_seconds = secs
                session.add(sec_row)

        session.commit()
        return JSONResponse(status_code=200, content={"status": "ok", "session_id": existing.id})


def _load_story_markdown(
    request: Request, run_id: int, token: str
) -> tuple[str, str, bool] | None:
    """Return (markdown, deck_title, is_fallback) for the run, or None if the
    run/token isn't valid. `is_fallback=True` means the run pre-dates the Story
    feature and we synthesized a placeholder pointing at re-generation."""
    with session_scope(request.app.state.session_factory) as session:
        run = session.execute(
            select(AutomationRun).where(
                AutomationRun.id == run_id,
                AutomationRun.run_type == "deck_generation",
            )
        ).scalar_one_or_none()
        if run is None:
            return None
        summary = dict(run.summary_json or {})
        if summary.get("export_token") != token:
            return None
        deck_title = str(summary.get("design_title") or "Anata Sales Story")
        markdown_text = str(summary.get("story_markdown") or "").strip()
        if markdown_text:
            return markdown_text, deck_title, False

        # Fallback for decks generated BEFORE the Story feature shipped:
        # craft a minimal markdown with the data we still have on the run row,
        # plus a clear instruction to re-generate the deck for the full Story.
        view_url = str(summary.get("view_url") or "").strip()
        target_id = str(summary.get("target_product_identifier") or "").strip()
        channels = list(summary.get("channels") or [])
        bullets: list[str] = []
        if target_id:
            bullets.append(f"- **Target listing:** `{target_id}`")
        if channels:
            bullets.append(f"- **Channels in scope:** {', '.join(str(c) for c in channels)}")
        if view_url:
            bullets.append(f"- **Open the deck:** [{view_url}]({view_url})")
        bullets_block = "\n".join(bullets) if bullets else ""

        fallback_md = (
            f"# {deck_title}\n\n"
            "## Story not yet generated for this deck\n\n"
            "This deck was created before the Story markdown companion was added. "
            "Re-generate the deck from the admin dashboard with the same inputs "
            "and the new Story will be saved automatically — including the full "
            "executive summary, market & competitive landscape, search behavior, "
            "conversion recommendations, growth-plan synopsis, 4-phase implementation "
            "roadmap with cited sources, and proposed offers.\n\n"
            f"{bullets_block}\n"
        )
        return fallback_md, deck_title, True


@router.get("/decks/{deck_slug}/{run_id}/{token}/story", response_class=HTMLResponse)
def deck_story_view(request: Request, deck_slug: str, run_id: int, token: str) -> Response:
    loaded = _load_story_markdown(request, run_id, token)
    if loaded is None:
        return HTMLResponse("Story not found.", status_code=404)
    markdown_text, deck_title, _is_fallback = loaded
    try:
        import markdown as _markdown  # type: ignore

        body_html = _markdown.markdown(
            markdown_text,
            extensions=["extra", "sane_lists", "toc"],
        )
    except Exception:
        # Fallback: plain <pre> rendering if the markdown package is unavailable.
        from html import escape as _escape

        body_html = f"<pre>{_escape(markdown_text)}</pre>"

    download_url = f"/decks/{deck_slug}/{run_id}/{token}/story.md"
    page = f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>{deck_title} — Story</title>
  <style>
    :root {{
      --ink: #0d1f24;
      --muted: #5f6f73;
      --accent: #1a4f4a;
      --bg: #f6f3ec;
      --card: #ffffff;
      --rule: #e3decf;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: var(--bg);
      color: var(--ink);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
      line-height: 1.6;
    }}
    .story-shell {{
      max-width: 820px;
      margin: 0 auto;
      padding: 56px 32px 96px;
    }}
    .story-toolbar {{
      display: flex;
      justify-content: flex-end;
      gap: 12px;
      margin-bottom: 24px;
    }}
    .story-toolbar a {{
      font-size: 13px;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      color: var(--accent);
      border: 1px solid var(--accent);
      padding: 8px 14px;
      border-radius: 999px;
      text-decoration: none;
    }}
    .story-toolbar a:hover {{ background: var(--accent); color: #fff; }}
    .story-body {{
      background: var(--card);
      padding: 48px 56px;
      border-radius: 14px;
      box-shadow: 0 18px 40px -28px rgba(13,31,36,0.35);
      border: 1px solid var(--rule);
    }}
    .story-body h1 {{ font-size: 30px; margin-top: 0; }}
    .story-body h2 {{ font-size: 22px; margin-top: 36px; border-top: 1px solid var(--rule); padding-top: 28px; }}
    .story-body h3 {{ font-size: 17px; margin-top: 24px; }}
    .story-body h4 {{ font-size: 15px; margin-top: 18px; color: var(--accent); }}
    .story-body code {{ background: var(--bg); padding: 2px 6px; border-radius: 4px; font-size: 0.9em; }}
    .story-body blockquote {{
      border-left: 3px solid var(--accent);
      margin: 16px 0;
      padding: 4px 16px;
      color: var(--muted);
      background: rgba(26,79,74,0.04);
    }}
    .story-body ul, .story-body ol {{ padding-left: 22px; }}
    .story-body li {{ margin: 6px 0; }}
    .story-body a {{ color: var(--accent); }}
    @media (max-width: 640px) {{
      .story-shell {{ padding: 24px 16px 64px; }}
      .story-body {{ padding: 28px 22px; }}
    }}
  </style>
</head>
<body>
  <div class=\"story-shell\">
    <div class=\"story-toolbar\">
      <a href=\"{download_url}\" download>Download .md</a>
    </div>
    <article class=\"story-body\">
      {body_html}
    </article>
  </div>
</body>
</html>
"""
    return HTMLResponse(page)


@router.get("/decks/{deck_slug}/{run_id}/{token}/story.md")
def deck_story_download(request: Request, deck_slug: str, run_id: int, token: str) -> Response:
    loaded = _load_story_markdown(request, run_id, token)
    if loaded is None:
        return PlainTextResponse("Story not found.", status_code=404)
    markdown_text, _, _is_fallback = loaded
    safe_slug = deck_slug or f"deck-{run_id}"
    filename = f"{safe_slug}-story.md"
    return PlainTextResponse(
        markdown_text,
        media_type="text/markdown; charset=utf-8",
        headers={"Content-Disposition": f"attachment; filename=\"{filename}\""},
    )


_GROWTH_PLAN_FORM_KEYS = {
    "growth_cvr_pct", "growth_goal_sessions", "growth_goal_multiplier", "growth_aov",
    "growth_mix_organic", "growth_mix_on_channel_paid", "growth_mix_off_channel_paid",
    "growth_mix_affiliate", "growth_mix_retargeting",
    "growth_on_channel_cpc", "growth_off_channel_cpc",
    "growth_dsp_prospecting_cpm", "growth_dsp_retargeting_cpm", "growth_retargeting_ctr_pct",
    "growth_videos_per_month", "growth_avg_impressions_per_video", "growth_shoppable_ctr_pct",
    "growth_tiktok_platform_commission_pct", "growth_creator_commission_pct",
    "growth_hybrid_flat_fee_per_video", "growth_cogs_per_unit", "growth_shipping_per_unit",
    "growth_tiktok_to_amazon_cvr_uplift",
    "growth_audience_window_days", "growth_frequency_cap",
    "growth_repeat_cvr_multiplier", "growth_btp_redemption_pct",
}


async def _run_generate_deck(
    request: Request,
    *,
    competitor_xray_csv: list[UploadFile],
    target_xray_csv: Optional[UploadFile],
    keyword_xray_csv: list[UploadFile],
    cerebro_csv: Optional[UploadFile],
    word_frequency_csv: Optional[UploadFile],
    csv_files: list[UploadFile] | None = None,
    target_product_input: str,
    channels: list[str],
    creative_mockup_url: str,
    case_study_url: str,
    offers: list[str],
    offer_payload_json: str,
    include_recommended_plan: bool,
    include_growth_plan: bool = True,
    trigger: str = "admin_dashboard",
) -> ApiMessage:
    """Shared body for the two generate-deck routes.

    Both `/admin/api/generate-deck` (cookie-auth web admin) and
    `/api/admin/generate-deck` (internal-key) share this implementation;
    only the auth gate and `trigger` label differ at the route level.

    PR40: When the form posts the unified `csv_files` field, auto-detect
    each file's type (target_xray / competitor_xray / keyword / cerebro /
    word_frequency) and route into the right slot. Legacy per-type fields
    are still accepted for backward-compat with internal callers.
    """
    competitor_files: list[UploadFile] = [file for file in competitor_xray_csv if file.filename]
    keyword_files: list[UploadFile] = [file for file in keyword_xray_csv if file.filename]
    settings = request.app.state.settings

    # PR40: auto-route unified upload into the appropriate per-type slots.
    # Buffer each upload's bytes once so we can both detect AND parse without
    # consuming the underlying stream twice.
    auto_target_xray_bytes: bytes | None = None
    auto_target_xray_filename: str = ""
    auto_cerebro_bytes: bytes | None = None
    auto_cerebro_filename: str = ""
    auto_word_freq_bytes: bytes | None = None
    auto_word_freq_filename: str = ""
    # Buffered (filename, bytes) tuples for the multi-file slots.
    auto_competitor_payloads: list[tuple[str, bytes]] = []
    auto_keyword_payloads: list[tuple[str, bytes]] = []
    autodetect_log: list[str] = []
    for upload in (csv_files or []):
        if not upload or not upload.filename:
            continue
        raw = await upload.read()
        from sales_support_agent.services.helium10 import detect_csv_kind
        kind = detect_csv_kind(raw)
        autodetect_log.append(f"{upload.filename} → {kind}")
        if kind == "target_xray":
            auto_target_xray_bytes = raw
            auto_target_xray_filename = upload.filename
        elif kind == "competitor_xray":
            auto_competitor_payloads.append((upload.filename, raw))
        elif kind == "keyword":
            auto_keyword_payloads.append((upload.filename, raw))
        elif kind == "cerebro":
            auto_cerebro_bytes = raw
            auto_cerebro_filename = upload.filename
        elif kind == "word_frequency":
            auto_word_freq_bytes = raw
            auto_word_freq_filename = upload.filename
        # "unknown" is silently skipped — surface in the warnings list
        # below so the AE knows.

    # Pull growth-plan inputs out of the request form. We don't define them as
    # FastAPI Form() params to keep the function signature manageable; instead
    # we read them straight from the form payload and forward as a dict.
    growth_plan_inputs: Optional[dict[str, str]] = None
    category_label_input: str = ""
    if include_growth_plan:
        try:
            form_data = await request.form()
            growth_plan_inputs = {
                key: str(form_data.get(key))
                for key in _GROWTH_PLAN_FORM_KEYS
                if form_data.get(key) not in (None, "")
            }
        except Exception:
            growth_plan_inputs = {}
    # PR47: read the optional category_label override (works whether or not
    # growth_plan is included). Falls back to "" → auto-derive in the dataset
    # builder.
    try:
        form_payload = await request.form()
        category_label_input = str(form_payload.get("category_label") or "").strip()
    except Exception:
        category_label_input = ""

    try:
        with session_scope(request.app.state.session_factory) as session:
            # Legacy per-type uploads (still supported for the internal API
            # callers that wire them explicitly).
            legacy_target_xray_bytes = (
                await target_xray_csv.read()
                if target_xray_csv and target_xray_csv.filename
                else None
            )
            legacy_target_xray_filename = (
                target_xray_csv.filename or "" if target_xray_csv else ""
            )
            legacy_competitor_payloads = [
                (file.filename or "competitors.csv", await file.read())
                for file in competitor_files
            ]
            legacy_keyword_payloads = [
                (file.filename or "keywords.csv", await file.read())
                for file in keyword_files
            ]
            legacy_cerebro_bytes = (await cerebro_csv.read()) if cerebro_csv and cerebro_csv.filename else None
            legacy_cerebro_filename = (cerebro_csv.filename or "") if cerebro_csv else ""
            legacy_word_freq_bytes = (await word_frequency_csv.read()) if word_frequency_csv and word_frequency_csv.filename else None
            legacy_word_freq_filename = (word_frequency_csv.filename or "") if word_frequency_csv else ""

            # PR40: prefer auto-detected uploads when they're present;
            # legacy per-type fields fill any slot the unified upload
            # didn't cover (so internal API callers and the new web flow
            # both work without changes to the service).
            target_xray_csv_bytes = auto_target_xray_bytes or legacy_target_xray_bytes
            target_xray_filename = auto_target_xray_filename or legacy_target_xray_filename
            competitor_payloads = auto_competitor_payloads or legacy_competitor_payloads
            keyword_payloads = auto_keyword_payloads or legacy_keyword_payloads
            cerebro_bytes = auto_cerebro_bytes if auto_cerebro_bytes is not None else legacy_cerebro_bytes
            cerebro_filename = auto_cerebro_filename or legacy_cerebro_filename
            word_freq_bytes = auto_word_freq_bytes if auto_word_freq_bytes is not None else legacy_word_freq_bytes
            word_freq_filename = auto_word_freq_filename or legacy_word_freq_filename

            # PR40: target ASIN becomes optional when a target Xray is
            # uploaded — derive it from the single product row in that file.
            effective_target_input = target_product_input.strip()
            if not effective_target_input and target_xray_csv_bytes:
                from sales_support_agent.services.helium10 import extract_target_asin_from_xray
                derived_asin = extract_target_asin_from_xray(target_xray_csv_bytes)
                if derived_asin:
                    effective_target_input = derived_asin

            if not effective_target_input:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Target product is required. Provide an ASIN/URL OR "
                        "upload a target Xray CSV (the single-row export of "
                        "your prospect's listing) — the ASIN will be read "
                        "from the file."
                    ),
                )

            result = DeckGenerationService(settings, session).generate_deck(
                competitor_xray_csv_payloads=competitor_payloads,
                target_xray_csv_bytes=target_xray_csv_bytes,
                target_xray_filename=target_xray_filename,
                keyword_xray_csv_payloads=keyword_payloads,
                cerebro_csv_bytes=cerebro_bytes,
                cerebro_filename=cerebro_filename,
                word_frequency_csv_bytes=word_freq_bytes,
                word_frequency_filename=word_freq_filename,
                target_product_input=effective_target_input,
                channels=channels,
                creative_mockup_url=creative_mockup_url,
                case_study_url=case_study_url,
                offers=offers,
                offer_payload_json=offer_payload_json,
                include_recommended_plan=include_recommended_plan,
                growth_plan_inputs=growth_plan_inputs,
                category_label=category_label_input,
                trigger=trigger,
            )
            # Surface the auto-detection result so the AE can see what
            # was routed where in the deck's warnings list.
            # PR40: result is a frozen dataclass — can't mutate `warnings`
            # in place. We return the autodetect log alongside the result
            # via the API response below instead.
    except HTTPException:
        # PR40: pass our deliberate 400/4xx through unchanged. Without this
        # the catch-all below was turning the "Target product is required"
        # 400 into a 500.
        raise
    except Exception as exc:
        # PR41: log the full traceback so failures don't disappear into a
        # bare 500 with a cryptic detail string. Auto-detect log included
        # so we can reproduce the exact upload partition that failed.
        logger.exception(
            "[generate_deck] failed: trigger=%s target_input=%r autodetect=%s",
            trigger,
            target_product_input,
            autodetect_log or "(no auto-detected uploads)",
        )
        raise HTTPException(status_code=500, detail=str(exc) or "Deck generation failed.") from exc
    # PR40: append the auto-detect log so the AE sees what was routed where.
    response_warnings = list(result.warnings or [])
    if autodetect_log:
        response_warnings.append("Auto-detected uploads: " + "; ".join(autodetect_log))
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
            "warnings": response_warnings,
            "sales_row_count": result.sales_row_count,
            "competitor_row_count": result.competitor_row_count,
            "template_fields": result.template_fields,
        },
    )


@router.post("/admin/api/generate-deck", response_model=ApiMessage)
async def admin_generate_deck(
    request: Request,
    # PR40: unified upload — drop ALL Helium 10 CSVs into one field, server
    # auto-routes by header signature. Legacy per-type fields stay so the
    # /api/admin/generate-deck flow doesn't change for existing callers.
    csv_files: list[UploadFile] = File(default=[]),
    competitor_xray_csv: list[UploadFile] = File(default=[]),
    target_xray_csv: Optional[UploadFile] = File(default=None),
    keyword_xray_csv: list[UploadFile] = File(default=[]),
    cerebro_csv: Optional[UploadFile] = File(default=None),
    word_frequency_csv: Optional[UploadFile] = File(default=None),
    target_product_input: str = Form(default=""),
    channels: list[str] = Form(default=[]),
    creative_mockup_url: str = Form(default=""),
    case_study_url: str = Form(default=""),
    offers: list[str] = Form(default=[]),
    offer_payload_json: str = Form(default=""),
    include_recommended_plan: bool = Form(default=True),
    include_growth_plan: bool = Form(default=True),
) -> ApiMessage:
    _require_admin_enabled(request)
    if not _is_admin_authenticated(request):
        raise HTTPException(status_code=401, detail="Admin login required.")
    return await _run_generate_deck(
        request,
        csv_files=csv_files,
        competitor_xray_csv=competitor_xray_csv,
        target_xray_csv=target_xray_csv,
        keyword_xray_csv=keyword_xray_csv,
        cerebro_csv=cerebro_csv,
        word_frequency_csv=word_frequency_csv,
        target_product_input=target_product_input,
        channels=channels,
        creative_mockup_url=creative_mockup_url,
        case_study_url=case_study_url,
        offers=offers,
        offer_payload_json=offer_payload_json,
        include_recommended_plan=include_recommended_plan,
        include_growth_plan=include_growth_plan,
        trigger="admin_dashboard",
    )


@router.post("/api/admin/generate-deck", response_model=ApiMessage)
async def internal_admin_generate_deck(
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
    csv_files: list[UploadFile] = File(default=[]),
    competitor_xray_csv: list[UploadFile] = File(default=[]),
    target_xray_csv: Optional[UploadFile] = File(default=None),
    keyword_xray_csv: list[UploadFile] = File(default=[]),
    cerebro_csv: Optional[UploadFile] = File(default=None),
    word_frequency_csv: Optional[UploadFile] = File(default=None),
    target_product_input: str = Form(default=""),
    channels: list[str] = Form(default=[]),
    creative_mockup_url: str = Form(default=""),
    case_study_url: str = Form(default=""),
    offers: list[str] = Form(default=[]),
    offer_payload_json: str = Form(default=""),
    include_recommended_plan: bool = Form(default=True),
    include_growth_plan: bool = Form(default=True),
) -> ApiMessage:
    _enforce_api_key(request, x_internal_api_key)
    return await _run_generate_deck(
        request,
        csv_files=csv_files,
        competitor_xray_csv=competitor_xray_csv,
        target_xray_csv=target_xray_csv,
        keyword_xray_csv=keyword_xray_csv,
        cerebro_csv=cerebro_csv,
        word_frequency_csv=word_frequency_csv,
        target_product_input=target_product_input,
        channels=channels,
        creative_mockup_url=creative_mockup_url,
        case_study_url=case_study_url,
        offers=offers,
        offer_payload_json=offer_payload_json,
        include_recommended_plan=include_recommended_plan,
        include_growth_plan=include_growth_plan,
        trigger="internal_api",
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


@router.delete("/admin/api/deck-runs/{run_id}", response_model=ApiMessage)
def admin_delete_deck_run(request: Request, run_id: int) -> ApiMessage:
    """PR52: hard-delete a single deck-generation AutomationRun.
    Admin-auth gated. The frontend confirms with a modal before calling
    this; backend trusts that confirmation and deletes immediately."""
    _require_admin_enabled(request)
    if not _is_admin_authenticated(request):
        raise HTTPException(status_code=401, detail="Admin login required.")
    with session_scope(request.app.state.session_factory) as session:
        run = session.get(AutomationRun, run_id)
        if run is None:
            raise HTTPException(status_code=404, detail=f"Deck run {run_id} not found.")
        if run.run_type != "deck_generation":
            # Defensive: don't let this endpoint nuke other run types.
            raise HTTPException(status_code=400, detail=f"Run {run_id} is not a deck generation.")
        session.delete(run)
        session.commit()
    return ApiMessage(status="ok", message=f"Deck run {run_id} deleted.")


@router.post("/api/admin/deck-runs/{run_id}/delete", response_model=ApiMessage)
def internal_admin_delete_deck_run(
    request: Request,
    run_id: int,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> ApiMessage:
    """PR52: internal-API mirror of the admin DELETE so the frontend
    proxy can forward via POST (proxy uses POST for cross-service calls
    and avoids sending DELETE through the redirect chain). Same effect."""
    _enforce_api_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        run = session.get(AutomationRun, run_id)
        if run is None:
            raise HTTPException(status_code=404, detail=f"Deck run {run_id} not found.")
        if run.run_type != "deck_generation":
            raise HTTPException(status_code=400, detail=f"Run {run_id} is not a deck generation.")
        session.delete(run)
        session.commit()
    return ApiMessage(status="ok", message=f"Deck run {run_id} deleted.")


@router.post("/api/discovery/clickup-schema", response_model=ApiMessage)
def discover_clickup_schema(
    payload: DiscoveryRequest,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
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
    x_internal_api_key: Optional[str] = Header(default=None),
) -> ApiMessage:
    _enforce_api_key(request, x_internal_api_key)
    _validate_runtime(request)
    settings = request.app.state.settings
    with request.app.state.dashboard_sync_lock:
        request.app.state.dashboard_sync_last_started_at = datetime.now(timezone.utc)
    try:
        with session_scope(request.app.state.session_factory) as session:
            summary = ClickUpSyncService(settings, ClickUpClient(settings), session).sync_list(
                include_closed=payload.include_closed,
                max_tasks=payload.max_tasks,
            )
    except ClickUpAPIError as exc:
        with request.app.state.dashboard_sync_lock:
            request.app.state.dashboard_sync_last_completed_at = datetime.now(timezone.utc)
            request.app.state.dashboard_sync_last_error = str(exc)
        return ApiMessage(
            status="error",
            message=str(exc),
            details={
                "dashboard_error": str(exc),
                "http_status": exc.status_code,
                "error_code": "clickup_auth_error" if exc.status_code in {401, 403} else "clickup_api_error",
                "path": exc.path,
            },
        )
    with request.app.state.dashboard_sync_lock:
        request.app.state.dashboard_sync_last_completed_at = datetime.now(timezone.utc)
        request.app.state.dashboard_sync_last_error = ""
    return ApiMessage(status="ok", message="ClickUp sync completed.", details=summary)


@router.post("/api/jobs/stale-leads/run", response_model=ApiMessage)
def run_stale_lead_job(
    payload: StaleLeadRunRequest,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
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
    x_internal_api_key: Optional[str] = Header(default=None),
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
    x_internal_api_key: Optional[str] = Header(default=None),
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
    x_internal_api_key: Optional[str] = Header(default=None),
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
