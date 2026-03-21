"""API routes."""

from __future__ import annotations

import secrets
from datetime import date, datetime, timedelta, timezone
from urllib.parse import parse_qs

from fastapi import APIRouter, File, Form, Header, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response

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


router = APIRouter()


def _enforce_api_key(request: Request, internal_api_key: str | None) -> None:
    configured = request.app.state.settings.internal_api_key
    if configured and internal_api_key != configured:
        raise HTTPException(status_code=401, detail="Invalid internal API key.")


def _validate_runtime(request: Request) -> None:
    missing = get_missing_runtime_settings(request.app.state.settings)
    if missing:
        raise HTTPException(
            status_code=500,
            detail=f"Missing required environment variables for sales support agent: {', '.join(missing)}",
        )


def _lead_builder_status() -> dict[str, object]:
    try:
        lead_settings = load_lead_builder_settings()
        missing = get_missing_lead_builder_settings(lead_settings)
        return {"ready": not missing, "missing": missing}
    except Exception as exc:
        return {"ready": False, "missing": [str(exc)]}


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
    return ApiMessage(
        status="ok",
        message="healthy",
        details={
            "clickup_configured": bool(settings.clickup_api_token and settings.clickup_list_id),
            "slack_configured": bool(settings.slack_bot_token and settings.slack_channel_id),
            "discovery_snapshot_path": str(settings.discovery_snapshot_path),
            "deck_generator_configured": bool(
                settings.google_sheets_spreadsheet_id
                and settings.google_sheets_sales_range
                and settings.google_service_account_json
                and settings.canva_client_id
                and settings.canva_client_secret
                and settings.canva_redirect_uri
                and settings.canva_brand_template_id
            ),
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
            lead_builder_status=_lead_builder_status(),
            clickup_client=ClickUpClient(settings),
        )
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
            lead_builder_status=_lead_builder_status(),
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
    lead_builder_status = _lead_builder_status()
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
        return JSONResponse(status_code=404, content={"detail": "Lead run not found."})
    return JSONResponse(status_code=200, content={"status": "ok", "message": "Lead run status loaded.", "details": payload})


@router.get("/admin/api/lead-runs/{run_id}/download", response_model=None)
def admin_lead_run_download(request: Request, run_id: str) -> Response:
    _require_admin_enabled(request)
    if not _is_admin_authenticated(request):
        return JSONResponse(status_code=401, content={"detail": "Admin login required."})

    payload = fetch_lead_run_status(run_id)
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
def admin_sync_dashboard(request: Request) -> JSONResponse:
    _require_admin_enabled(request)
    if not _is_admin_authenticated(request):
        return JSONResponse(status_code=401, content={"detail": "Admin login required."})

    settings = request.app.state.settings
    with session_scope(request.app.state.session_factory) as session:
        clickup_summary = ClickUpSyncService(settings, ClickUpClient(settings), session).sync_list(include_closed=True)
        stale_summary = StaleLeadJob(settings, ClickUpClient(settings), SlackClient(settings), session).run(dry_run=True)
    return JSONResponse(
        status_code=200,
        content={
            "status": "ok",
            "message": "Dashboard sync completed.",
            "details": {
                "clickup_sync": clickup_summary,
                "stale_lead_scan": stale_summary,
                "gmail_sync": {"status": "skipped", "reason": "enable once Gmail OAuth is fixed"},
            },
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
        },
    )
    authorize_url = CanvaClient(settings).build_authorize_url(
        state=state,
        code_verifier=code_verifier,
    )
    response = RedirectResponse(url=authorize_url, status_code=302)
    response.set_cookie(
        key=_canva_oauth_cookie_name(request),
        value=signed_state,
        **_canva_oauth_cookie_options(request),
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
    _require_admin_enabled(request)
    if not _is_admin_authenticated(request):
        return RedirectResponse(url="/admin/login", status_code=302)
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

    response = RedirectResponse(url="/admin", status_code=302)
    response.delete_cookie(_canva_oauth_cookie_name(request), path="/")
    return response


@router.post("/admin/api/generate-deck", response_model=ApiMessage)
async def admin_generate_deck(
    request: Request,
    competitor_csv: UploadFile = File(...),
    run_label: str = Form(default=""),
    reporting_period: str = Form(default=""),
    report_date: str = Form(default=""),
) -> ApiMessage:
    _require_admin_enabled(request)
    if not _is_admin_authenticated(request):
        raise HTTPException(status_code=401, detail="Admin login required.")

    try:
        parsed_report_date = date.fromisoformat(report_date) if report_date else None
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Report date must use YYYY-MM-DD format.") from exc
    competitor_bytes = await competitor_csv.read()
    settings = request.app.state.settings
    with session_scope(request.app.state.session_factory) as session:
        result = DeckGenerationService(settings, session).generate_deck(
            competitor_csv_bytes=competitor_bytes,
            competitor_filename=competitor_csv.filename or "competitor.csv",
            run_label=run_label,
            report_date=parsed_report_date,
            reporting_period=reporting_period,
        )
    return ApiMessage(
        status="ok",
        message=result.message,
        details={
            "run_id": result.run_id,
            "status": result.status,
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
        details = {
            "connection": service.get_connection_summary(),
            "runs": service.list_recent_runs(limit=10),
        }
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
