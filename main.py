from __future__ import annotations

import csv
import io
import json
import logging
import math
import os
import re
import secrets
import time
import threading
import unicodedata
from concurrent.futures import Future, ThreadPoolExecutor
from urllib.parse import parse_qs, quote, urlparse
from base64 import b64decode, b64encode
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional, Union
from zoneinfo import ZoneInfo

import requests
from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from sales_support_agent.services.admin_auth import (
    admin_login_enabled,
    create_admin_session_token,
    create_user_session_token,
    password_login_enabled,
    validate_admin_session_token,
    verify_admin_password,
)
from sales_support_agent.services.admin_dashboard import (
    DashboardData,
    ExecutiveData,
    dashboard_data_from_dict,
    executive_data_from_dict,
    render_dashboard_page,
    render_executive_page,
    render_login_page,
    render_sales_deck_page,
)
from sales_support_agent.services.admin_home import (
    render_admin_home_page,
    render_service_status_page,
)
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
from sales_support_agent.services.website_ops import (
    get_website_ops_run_state,
    latest_report_entry as latest_website_ops_report_entry,
    render_content_page as render_website_ops_content_page,
    render_dashboard_page as render_website_ops_dashboard_page,
    render_feedback_detail_page as render_website_ops_feedback_detail_page,
    render_indexing_page as render_website_ops_indexing_page,
    render_queue_page as render_website_ops_queue_page,
    render_report_page as render_website_ops_report_page,
    render_reports_page as render_website_ops_reports_page,
    render_site_health_page as render_website_ops_site_health_page,
    review_feedback_record as review_website_ops_feedback_record,
    run_website_ops as run_website_ops_pipeline,
    save_feedback_record as save_website_ops_feedback_record,
    send_website_ops_failure_email,
    website_ops_run_is_due,
    write_website_ops_run_state,
)


logger = logging.getLogger(__name__)

app = FastAPI()

# Mount static files so finance.css (and future assets) are served at /static/*
_static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sales_support_agent", "static")
_brand_static_dir = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "shared",
    "anata_brand",
    "assets",
)
os.makedirs(_static_dir, exist_ok=True)
app.mount("/static", StaticFiles(directory=_static_dir), name="static")
app.mount(
    "/brand-static",
    StaticFiles(directory=_brand_static_dir),
    name="brand-static",
)

from sales_support_agent.api.assets_router import router as _assets_router  # noqa: E402
app.include_router(_assets_router)

from sales_support_agent.api.cashflow_router import plaid_webhook_router as _plaid_webhook_router, router as _cashflow_router  # noqa: E402
app.include_router(_cashflow_router)
app.include_router(_plaid_webhook_router)

# Advertising > Audit — self-contained router serving /admin/advertising/* in
# process (same pattern as the cashflow router above).
from sales_support_agent.api.advertising_router import public_router as _advertising_public_router, router as _advertising_router  # noqa: E402
app.include_router(_advertising_router)
app.include_router(_advertising_public_router)

# Executive > Brand Analysis — self-contained router serving
# /admin/executive/brand-analysis/* in process (same pattern as above).
from sales_support_agent.api.brand_analysis_router import (  # noqa: E402
    public_router as _brand_analysis_public_router,
    router as _brand_analysis_router,
)
app.include_router(_brand_analysis_router)
app.include_router(_brand_analysis_public_router)

# Fulfillment > Sales Deck (rate sheets) — admin generator/history plus the
# public token-gated /rate-sheets/* hosted views (same in-process pattern).
from sales_support_agent.api.fulfillment_deck_router import (  # noqa: E402
    admin_router as _fulfillment_deck_admin_router,
    public_router as _fulfillment_deck_public_router,
)
app.include_router(_fulfillment_deck_admin_router)
app.include_router(_fulfillment_deck_public_router)

# Fulfillment public self-serve funnel — /api/public/fulfillment/* (taste +
# unlock), shared-secret gated like the marketing intake routes.
from sales_support_agent.api.fulfillment_public_router import router as _fulfillment_public_router  # noqa: E402
app.include_router(_fulfillment_public_router)

# Access admin UI — /admin/access (users list, role CRUD, guards all behind access.manage).
from sales_support_agent.api.access_router import router as _access_router, _settings_router as _settings_router_  # noqa: E402
app.include_router(_access_router)
app.include_router(_settings_router_)

# HR / payroll section — /admin/hr/* (employees, teams; payroll/settings later).
from sales_support_agent.api.hr_router import router as _hr_router  # noqa: E402
app.include_router(_hr_router)
from sales_support_agent.api.employee_app_router import router as _employee_app_router  # noqa: E402
app.include_router(_employee_app_router)

# Sales Priorities — HubSpot-backed deal board (/admin/sales/*).
from sales_support_agent.api.sales_router import router as _sales_router  # noqa: E402
app.include_router(_sales_router)

# Google OAuth start + callback — /admin/auth/google and /admin/auth/callback.
# The login page's "Sign in with Google" button and the invite-accept flow both
# bounce here; without this router those paths 404 on the root app. The router
# resolves its config from app.state.agent_settings (see _auth_settings).
from sales_support_agent.api.auth_router import router as _google_auth_router  # noqa: E402
app.include_router(_google_auth_router)

# QuickBooks OAuth routes — NO auth guard on /connect, /callback, /disconnect
# so Intuit's reviewer can complete the flow without an Anata session.
# Prefix: /admin/finances/qbo — redirect URI must be registered in Intuit portal as
#   https://agent.anatainc.com/admin/finances/qbo/callback
from sales_support_agent.api.qbo_auth_router import router as _qbo_auth_router  # noqa: E402
app.include_router(_qbo_auth_router, prefix="/admin/finances/qbo")

# Building OS — keep both production entrypoints on the same complete route
# registrar so the live root app cannot silently drift behind the modular app.
from sales_support_agent.api.building_routes import include_building_routers as _include_building_routers  # noqa: E402
_include_building_routers(app)

# Access control (RBAC) — MUST be registered at construction time, not in the
# startup event: once the app starts, Starlette freezes the middleware stack and
# add_middleware() raises, which was being swallowed — leaving the root app with
# NO access middleware and NO ToolForbidden handler. Router-level
# require_tool() then 500'd (Finance/Advertising/Fulfillment/Brand Analysis)
# instead of rendering the friendly 403.
try:
    from sales_support_agent.services.access.middleware import install_access_middleware  # noqa: E402
    from sales_support_agent.services.auth_deps import ToolForbidden, render_forbidden_response  # noqa: E402
    from sales_support_agent.services.performance import install_performance_middleware  # noqa: E402
    from sales_support_agent.services.website_ops_storage import WebsiteOpsStorageMiddleware  # noqa: E402
    install_performance_middleware(app)
    install_access_middleware(app)
    app.add_middleware(WebsiteOpsStorageMiddleware)
    app.add_exception_handler(ToolForbidden, render_forbidden_response)
except Exception as _e:  # noqa: BLE001
    logger.warning("Could not install RBAC middleware: %s", _e)


async def _run_finance_sync(settings, *, label: str = "manual") -> None:
    """Run the full finance data pipeline in sequence:

    1. ClickUp sync      → upserts RecurringTemplates for recurring AP/AR tasks
    2. Template expand   → fills CashEvent rows for the next 400 days
    3. QBO invoice sync  → AR events from open QBO invoices
    4. QBO bank sync     → posted Purchase / Deposit / Payment actuals (replaces CSV)
    5. QB token expiry   → logs a warning if token expires within 24 h

    All steps are non-fatal: failures are logged but do not crash the app.
    """
    import asyncio as _asyncio

    logger.info("[Finance sync/%s] Starting...", label)

    # 1. ClickUp — skipped once native recurring schedules replace it
    if getattr(settings, "disable_clickup_finance_sync", False):
        logger.info("[Finance sync/%s] ClickUp: disabled, native schedules are the source", label)
    else:
        try:
            from sales_support_agent.services.cashflow.clickup_sync import sync_clickup_finance
            cu_result = await _asyncio.to_thread(sync_clickup_finance, settings)
            logger.info(
                "[Finance sync/%s] ClickUp: %d inserted, %d skipped, %d errors",
                label, cu_result.rows_inserted, cu_result.rows_skipped_duplicate, len(cu_result.errors),
            )
        except Exception as exc:
            logger.warning("[Finance sync/%s] ClickUp sync failed: %s", label, exc)

    # 2. Template expansion (pick up new templates from ClickUp sync above)
    try:
        from sales_support_agent.services.cashflow.obligations import generate_upcoming_from_templates
        created = await _asyncio.to_thread(
            generate_upcoming_from_templates,
            # Enough to forecast the quarter without manufacturing a year of
            # obligations that would age into debt.
            horizon_days=60,
            advance_template=True,
        )
        logger.info("[Finance sync/%s] Template expansion: %d events created/verified", label, len(created))
        from sales_support_agent.services.cashflow.obligations import (
            supersede_stale_template_occurrences,
        )
        rolled = await _asyncio.to_thread(supersede_stale_template_occurrences)
        if rolled:
            logger.info("[Finance sync/%s] Recurring rolled forward: %d superseded", label, len(rolled))
    except Exception as exc:
        logger.warning("[Finance sync/%s] Template expansion failed: %s", label, exc)

    # 3. QBO invoice sync (open AR invoices → planned inflow events)
    try:
        from sales_support_agent.services.cashflow.qbo_sync import sync_qbo_invoices
        inv_result = await _asyncio.to_thread(sync_qbo_invoices, settings)
        logger.info(
            "[Finance sync/%s] QBO invoices: %d inserted, %d skipped, %d errors",
            label, inv_result.rows_inserted, inv_result.rows_skipped_duplicate, len(inv_result.errors),
        )
    except Exception as exc:
        logger.warning("[Finance sync/%s] QBO invoice sync failed: %s", label, exc)

    # 4. QBO bank sync (posted transactions → actuals, replaces manual CSV upload)
    try:
        from sales_support_agent.services.cashflow.qbo_bank_sync import sync_qbo_bank_transactions
        bank_result = await _asyncio.to_thread(sync_qbo_bank_transactions, settings)
        logger.info(
            "[Finance sync/%s] QBO bank: %d inserted, %d skipped, %d errors",
            label, bank_result.rows_inserted, bank_result.rows_skipped_duplicate, len(bank_result.errors),
        )
    except Exception as exc:
        logger.warning("[Finance sync/%s] QBO bank sync failed: %s", label, exc)

    # 4b. Persist sync audit record to kv_store (non-fatal)
    try:
        from sales_support_agent.models.database import kv_set_json
        kv_set_json("last_sync", {
            "label":     label,
            "synced_at": datetime.utcnow().isoformat(),
        })
    except Exception as _e:
        pass

    # 5. QB token expiry warning
    try:
        from sales_support_agent.api.qbo_auth_router import _load_tokens
        token_row = await _asyncio.to_thread(_load_tokens)
        if token_row and token_row.get("expires_at"):
            exp_str = token_row["expires_at"]
            exp = datetime.fromisoformat(exp_str)
            if exp.tzinfo is None:
                exp = exp.replace(tzinfo=timezone.utc)
            remaining = exp - datetime.now(timezone.utc)
            if remaining < timedelta(hours=24):
                logger.warning(
                    "[Finance sync/%s] QB access token expires in %s — "
                    "visit /admin/finances/qbo/connect to re-authenticate.",
                    label, str(remaining).split(".")[0],
                )
            else:
                logger.info(
                    "[Finance sync/%s] QB access token valid for %s.",
                    label, str(remaining).split(".")[0],
                )
    except Exception as exc:
        logger.debug("[Finance sync/%s] QB token expiry check skipped: %s", label, exc)

    logger.info("[Finance sync/%s] Done.", label)


@app.on_event("startup")
async def _startup_init():
    """Ensure cashflow DB tables exist, then kick off background ClickUp sync."""
    import asyncio as _asyncio
    from sales_support_agent.models.database import init_cashflow_db
    from sales_support_agent.config import load_settings
    settings = load_settings()
    init_cashflow_db(settings.sales_agent_db_url)

    # --- Step 1: Expand recurring templates SYNCHRONOUSLY before first request.
    # This ensures the calendar, overview, and forecast all have populated
    # cash_events rows on first page load — no more blank future weeks.
    try:
        from sales_support_agent.services.cashflow.obligations import generate_upcoming_from_templates
        created = await _asyncio.to_thread(
            generate_upcoming_from_templates,
            horizon_days=400,
            advance_template=True,
        )
        logger.info("[Finance startup] Templates expanded: %d obligations created/verified", len(created))
    except Exception as exc:
        logger.warning("[Finance startup] Template expansion failed (non-fatal): %s", exc)

    # --- Step 2: Initial background sync (runs 5 s after boot, non-blocking).
    async def _background_finance_sync():
        await _asyncio.sleep(5)   # give the server time to fully start
        await _run_finance_sync(settings, label="startup")

    _asyncio.create_task(_background_finance_sync())

    # --- Step 3: Automated periodic sync every 2 hours.
    async def _periodic_finance_sync_loop():
        """Run ClickUp → QBO invoices → QBO bank → template expansion every 2 h."""
        sync_interval_seconds = int(os.getenv("FINANCE_SYNC_INTERVAL_SECONDS", "7200"))
        await _asyncio.sleep(sync_interval_seconds)   # first run offset so startup completes
        while True:
            try:
                await _run_finance_sync(settings, label="periodic")
            except Exception as exc:
                logger.error("[Finance periodic sync] Unhandled error: %s", exc)
            await _asyncio.sleep(sync_interval_seconds)

    _asyncio.create_task(_periodic_finance_sync_loop())


ADMIN_SYNC_LOCK = threading.Lock()

# ========= RUNTIME CONFIG =========
REQUEST_TIMEOUT_SECONDS = 60
ADMIN_REMOTE_TIMEOUT_SECONDS = int((os.getenv("ADMIN_REMOTE_TIMEOUT_SECONDS", "8") or "8").strip())
DECK_PROXY_TIMEOUT_SECONDS = 10
DECK_PROXY_RETRY_DELAYS_SECONDS = (0.5, 1.0)
RENDER_GIT_COMMIT = os.getenv("RENDER_GIT_COMMIT", "").strip()
RENDER_GIT_BRANCH = os.getenv("RENDER_GIT_BRANCH", "").strip()


# ========= REQUEST / SETTINGS MODELS =========
@dataclass(frozen=True)
class AdminDashboardSettings:
    admin_username: str
    admin_password: str
    admin_session_secret: str
    admin_cookie_name: str
    admin_session_ttl_hours: int
    admin_auto_sync_max_age_minutes: int
    sales_support_agent_url: str
    sales_agent_internal_api_key: str


@dataclass(frozen=True)
class WebsiteOpsHostSettings:
    website_ops_root: Path
    website_ops_site_urls: tuple[str, ...]
    website_ops_execute_approved: bool
    website_ops_sitemap_url: str
    website_ops_allowed_host: str
    website_ops_report_email_to: tuple[str, ...]
    website_ops_email_from: str
    resend_api_key: str
    resend_from: str


@dataclass(frozen=True)
class FulfillmentCSHostSettings:
    fulfillment_cs_reports_dir: Path


# ========= CONFIGURATION =========
def configure_logging() -> None:
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO)


def load_admin_dashboard_settings() -> AdminDashboardSettings:
    return AdminDashboardSettings(
        admin_username=os.getenv("ADMIN_DASHBOARD_USERNAME", "admin").strip() or "admin",
        admin_password=os.getenv("ADMIN_DASHBOARD_PASSWORD", "").strip(),
        admin_session_secret=(
            os.getenv("ADMIN_DASHBOARD_SESSION_SECRET", "").strip()
            or os.getenv("SALES_AGENT_INTERNAL_API_KEY", "").strip()
            or "lead-scraper-admin-session-secret"
        ),
        admin_cookie_name=os.getenv("ADMIN_DASHBOARD_COOKIE_NAME", "lead_scraper_admin_session").strip() or "lead_scraper_admin_session",
        admin_session_ttl_hours=int((os.getenv("ADMIN_DASHBOARD_SESSION_TTL_HOURS", "24") or "24").strip()),
        admin_auto_sync_max_age_minutes=int((os.getenv("ADMIN_DASHBOARD_AUTO_SYNC_MAX_AGE_MINUTES", "30") or "30").strip()),
        sales_support_agent_url=os.getenv("SALES_SUPPORT_AGENT_URL", "https://sales-support-agent.onrender.com").strip().rstrip("/"),
        sales_agent_internal_api_key=os.getenv("SALES_AGENT_INTERNAL_API_KEY", "").strip(),
    )


def _parse_bool(value: str, *, default: bool = False) -> bool:
    normalized = (value or "").strip().lower()
    if not normalized:
        return default
    return normalized in {"1", "true", "yes", "on"}


def _parse_csv_tuple(value: str, *, default: tuple[str, ...]) -> tuple[str, ...]:
    items = tuple(part.strip() for part in (value or "").split(",") if part.strip())
    return items or default


def load_website_ops_settings() -> WebsiteOpsHostSettings:
    return WebsiteOpsHostSettings(
        website_ops_root=Path(os.getenv("WEBSITE_OPS_ROOT", "runtime/website_ops").strip() or "runtime/website_ops"),
        website_ops_site_urls=_parse_csv_tuple(
            os.getenv(
                "WEBSITE_OPS_URLS",
                "https://anatainc.com/",
            ),
            default=("https://anatainc.com/",),
        ),
        website_ops_execute_approved=_parse_bool(os.getenv("WEBSITE_OPS_EXECUTE_APPROVED", "true"), default=True),
        website_ops_sitemap_url=(
            os.getenv("WEBSITE_OPS_SITEMAP_URL", "https://anatainc.com/sitemap.xml").strip()
            or "https://anatainc.com/sitemap.xml"
        ),
        website_ops_allowed_host=(
            os.getenv("WEBSITE_OPS_ALLOWED_HOST", "anatainc.com").strip().lower()
            or "anatainc.com"
        ),
        website_ops_report_email_to=_parse_csv_tuple(
            os.getenv("WEBSITE_OPS_REPORT_EMAIL_TO", "david@anatainc.com"),
            default=("david@anatainc.com",),
        ),
        website_ops_email_from=(
            os.getenv("WEBSITE_OPS_EMAIL_FROM", "").strip()
            or os.getenv("RESEND_FROM", "").strip()
            or "Anata Agent <noreply@anatainc.com>"
        ),
        resend_api_key=os.getenv("RESEND_API_KEY", "").strip(),
        resend_from=(
            os.getenv("RESEND_FROM", "").strip()
            or "Anata Agent <noreply@anatainc.com>"
        ),
    )


def load_fulfillment_cs_settings() -> FulfillmentCSHostSettings:
    return FulfillmentCSHostSettings(
        fulfillment_cs_reports_dir=Path(
            os.getenv("FULFILLMENT_CS_REPORTS_DIR", "runtime/fulfillment_cs_reports").strip()
            or "runtime/fulfillment_cs_reports"
        )
    )


def _configure_agent_runtime_settings(app_instance: FastAPI) -> None:
    """Expose the Agent settings contract to in-process routers."""
    from sales_support_agent.config import load_settings as _load_agent_settings

    agent_settings = _load_agent_settings()
    app_instance.state.agent_settings = agent_settings
    app_instance.state.settings = agent_settings


@app.on_event("startup")
def startup() -> None:
    configure_logging()
    app.state.ready = False
    app.state.render_git_commit = RENDER_GIT_COMMIT or "unknown"
    _configure_agent_runtime_settings(app)
    app.state.website_ops_settings = load_website_ops_settings()
    app.state.admin_dashboard_last_auto_sync_at = None
    app.state.admin_dashboard_last_auto_sync_result = {
        "status": "idle",
        "running": False,
        "message": "Dashboard sync has not run in this session yet.",
    }
    app.state.admin_dashboard_sync_future = None
    app.state.website_ops_run_lock = threading.Lock()
    app.state.website_ops_run_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="website-ops")
    app.state.website_ops_run_futures = {}
    _cf_db_url = os.getenv("SALES_AGENT_DB_URL", "").strip()
    from sales_support_agent.models.database import create_session_factory, init_database, init_cashflow_db
    if not _cf_db_url:
        # Fall back to the same SQLite default that sales_support_agent/main.py uses
        # so the session factory is always available on app.state.
        from pathlib import Path as _Path
        _runtime = _Path("/tmp/anata-agent") if os.getenv("VERCEL") else _Path("runtime")
        _runtime.mkdir(parents=True, exist_ok=True)
        _cf_db_url = f"sqlite:///{_runtime / 'sales_support_agent.sqlite3'}"
    _sf = create_session_factory(_cf_db_url)
    prepare_database_on_startup = os.getenv(
        "AGENT_PREPARE_DATABASE_ON_STARTUP",
        "true",
    ).strip().lower() in {"1", "true", "yes", "on"}
    if prepare_database_on_startup:
        init_database(_sf)
        init_cashflow_db(_cf_db_url)
    app.state.session_factory = _sf  # required by sales_router and cashflow_router
    if os.getenv("WEBSITE_OPS_DATABASE_MIRROR", "false").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }:
        from sales_support_agent.services.website_ops_storage import (
            synchronize_website_ops_cache,
        )

        synchronize_website_ops_cache(
            app.state.website_ops_settings,
            _sf.kw["bind"],
        )
    # Store AdminDashboardSettings so cashflow auth_deps can always find
    # admin_cookie_name even when agent_settings fails to load.
    app.state.admin_dashboard_settings = load_admin_dashboard_settings()
    # RBAC: seed the never-lockable super-admin(s) and install the per-tool gate.
    try:
        if _cf_db_url and app.state.agent_settings is not None:
            from sales_support_agent.services.access import store as _access_store
            _access_store.seed_superadmins(getattr(app.state.agent_settings, "rbac_superadmin_emails", ()))
    except Exception as _e:
        logger.warning("Could not seed RBAC super-admins: %s", _e)
    # NOTE: RBAC middleware + ToolForbidden handler are installed at module
    # construction time (above), NOT here — add_middleware() fails once the app
    # has started.
    logger.info("[Startup] render_git_branch=%s render_git_commit=%s", RENDER_GIT_BRANCH or "unknown", RENDER_GIT_COMMIT or "unknown")
    app.state.ready = True


def _admin_cookie_options(request: Request, admin_settings: AdminDashboardSettings) -> dict[str, Any]:
    return {
        "key": admin_settings.admin_cookie_name,
        "httponly": True,
        "secure": request.url.scheme == "https",
        "samesite": "lax",
        "max_age": admin_settings.admin_session_ttl_hours * 3600,
        "path": "/",
    }


def _build_empty_dashboard(*, error_message: str = "") -> DashboardData:
    summary = {"dashboard_error": error_message} if error_message else {}
    return DashboardData(
        as_of_date=date.today(),
        total_active_leads=0,
        stale_counts={"overdue": 0, "needs_immediate_review": 0, "follow_up_due": 0},
        mailbox_findings=0,
        owner_queues=[],
        latest_sync_at=None,
        latest_run_summary=summary,
        sync_auto_enabled=False,
        sync_stale_after_minutes=0,
        deck_generator_ready=False,
        deck_generator_missing=[],
        recent_deck_runs=[],
    )


def _build_empty_executive(*, error_message: str = "") -> ExecutiveData:
    return ExecutiveData(
        as_of_date=date.today(),
        latest_sync_at=None,
        latest_run_summary={"executive_error": error_message} if error_message else {},
        summary_text=error_message or "No executive data is currently available.",
        kpis={
            "active_leads": 0,
            "overdue": 0,
            "review": 0,
            "due": 0,
            "untouched_7_plus": 0,
            "late_stage_stale": 0,
        },
        owner_scorecards=[],
        status_distribution=[],
        source_distribution=[],
        aging_buckets=[],
        late_stage_distribution=[],
        risk_leads=[],
        inbound_replies_by_owner=[],
        mailbox_signals_by_owner=[],
        hygiene_counts={
            "missing_next_action": 0,
            "missing_meeting_outcome": 0,
            "untouched_new_or_contacted": 0,
            "inbound_replies_last_7_days": 0,
            "mailbox_signals_last_7_days": 0,
        },
        filters={"owners": [], "statuses": [], "sources": [], "urgencies": ["overdue", "needs_immediate_review", "follow_up_due"]},
        lead_records=[],
    )


def dashboard_needs_auto_sync(
    dashboard: DashboardData,
    admin_settings: AdminDashboardSettings,
    *,
    now: Optional[datetime] = None,
) -> bool:
    max_age_minutes = max(admin_settings.admin_auto_sync_max_age_minutes, 0)
    if max_age_minutes == 0:
        return False

    if dashboard.latest_sync_at is None:
        return True

    current_time = now or datetime.now(timezone.utc)
    latest_sync_at = dashboard.latest_sync_at
    if latest_sync_at.tzinfo is None:
        latest_sync_at = latest_sync_at.replace(tzinfo=timezone.utc)

    return current_time - latest_sync_at >= timedelta(minutes=max_age_minutes)


def latest_sync_is_stale(
    latest_sync_at: Optional[datetime],
    admin_settings: AdminDashboardSettings,
    *,
    now: Optional[datetime] = None,
) -> bool:
    max_age_minutes = max(admin_settings.admin_auto_sync_max_age_minutes, 0)
    if max_age_minutes == 0:
        return False
    if latest_sync_at is None:
        return True

    current_time = now or datetime.now(timezone.utc)
    normalized_latest_sync = latest_sync_at
    if normalized_latest_sync.tzinfo is None:
        normalized_latest_sync = normalized_latest_sync.replace(tzinfo=timezone.utc)

    return current_time - normalized_latest_sync >= timedelta(minutes=max_age_minutes)


def fetch_remote_dashboard_data() -> DashboardData:
    admin_settings = load_admin_dashboard_settings()
    if not admin_settings.sales_support_agent_url or not admin_settings.sales_agent_internal_api_key:
        return _build_empty_dashboard(
            error_message="Sales support dashboard feed is not configured on this service.",
        )

    try:
        response = requests.get(
            f"{admin_settings.sales_support_agent_url}/api/admin/dashboard-data",
            headers={"X-Internal-Api-Key": admin_settings.sales_agent_internal_api_key},
            timeout=ADMIN_REMOTE_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        payload = response.json()
        details = payload.get("details") or {}
        return dashboard_data_from_dict(details)
    except Exception as exc:
        logger.exception("[AdminDashboard] remote data fetch failed")
        return _build_empty_dashboard(
            error_message=f"Sales support dashboard feed unavailable: {exc}",
        )


def fetch_remote_executive_data() -> ExecutiveData:
    admin_settings = load_admin_dashboard_settings()
    if not admin_settings.sales_support_agent_url or not admin_settings.sales_agent_internal_api_key:
        return _build_empty_executive(
            error_message="Sales support executive feed is not configured on this service.",
        )

    try:
        response = requests.get(
            f"{admin_settings.sales_support_agent_url}/api/admin/executive-data",
            headers={"X-Internal-Api-Key": admin_settings.sales_agent_internal_api_key},
            timeout=ADMIN_REMOTE_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        payload = response.json()
        details = payload.get("details") or {}
        return executive_data_from_dict(details)
    except Exception as exc:
        logger.exception("[ExecutiveDashboard] remote data fetch failed")
        return _build_empty_executive(
            error_message=f"Sales support executive feed unavailable: {exc}",
        )


def _post_sales_support_job(path: str, payload: dict[str, Any]) -> dict[str, Any]:
    admin_settings = load_admin_dashboard_settings()
    if not admin_settings.sales_support_agent_url or not admin_settings.sales_agent_internal_api_key:
        raise RuntimeError("Sales support agent URL or internal API key is not configured on this service.")

    response = requests.post(
        f"{admin_settings.sales_support_agent_url}{path}",
        headers={
            "Content-Type": "application/json",
            "X-Internal-Api-Key": admin_settings.sales_agent_internal_api_key,
        },
        json=payload,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return response.json()


def _post_sales_support_multipart(
    path: str,
    *,
    data_items: list[tuple[str, Any]],
    files_payload: Optional[list[tuple[str, tuple[str, bytes, str]]]] = None,
) -> tuple[int, dict[str, Any]]:
    admin_settings = load_admin_dashboard_settings()
    if not admin_settings.sales_support_agent_url or not admin_settings.sales_agent_internal_api_key:
        raise RuntimeError("Sales support agent URL or internal API key is not configured on this service.")

    response = requests.post(
        f"{admin_settings.sales_support_agent_url}{path}",
        headers={"X-Internal-Api-Key": admin_settings.sales_agent_internal_api_key},
        data=data_items,
        files=files_payload,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    if response.content:
        try:
            payload = response.json()
        except ValueError:
            payload = {"detail": (response.text or "Sales support agent returned a non-JSON response.").strip()}
    else:
        payload = {}
    return response.status_code, payload


def _rewrite_sales_support_url_for_agent(request: Request, value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return raw
    parsed = urlparse(raw)
    if not parsed.scheme or not parsed.netloc:
        return raw
    request_base = urlparse(str(request.base_url))
    admin_settings = load_admin_dashboard_settings()
    backend_base = urlparse(admin_settings.sales_support_agent_url)
    if backend_base.netloc and parsed.netloc == backend_base.netloc:
        return parsed._replace(scheme=request_base.scheme, netloc=request_base.netloc).geturl()
    return raw


def _deck_proxy_headers(content_type: str) -> dict[str, str]:
    return {
        "Content-Type": content_type,
        "Cache-Control": "private, max-age=300",
        "Content-Security-Policy": "default-src 'self' 'unsafe-inline' data: https:; img-src 'self' data: https:; media-src https: data:; frame-ancestors *;",
    }


def _deck_proxy_error_response() -> HTMLResponse:
    return HTMLResponse(
        """
        <!doctype html>
        <html lang="en">
          <head>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <title>Deck temporarily unavailable</title>
          </head>
          <body style="font-family: Inter, Arial, sans-serif; background: #f7f4ed; color: #1f3550; padding: 40px;">
            <main style="max-width: 720px; margin: 0 auto;">
              <h1 style="font-size: 2rem; margin-bottom: 0.5rem;">Deck is temporarily unavailable.</h1>
              <p style="font-size: 1rem; line-height: 1.6;">Retry in a few seconds. If this keeps happening, regenerate the deck or check the sales-support-agent service.</p>
            </main>
          </body>
        </html>
        """,
        status_code=502,
        headers=_deck_proxy_headers("text/html; charset=utf-8"),
    )


def sync_remote_dashboard_sources() -> dict[str, Any]:
    clickup_sync = _post_sales_support_job(
        "/api/clickup/sync",
        {"include_closed": True},
    )
    clickup_status = str(clickup_sync.get("status") or "ok").strip().lower()
    stale_scan = _post_sales_support_job(
        "/api/jobs/stale-leads/run",
        {"dry_run": True},
    ) if clickup_status == "ok" else {
        "status": "skipped",
        "message": "Stale lead scan skipped because ClickUp sync failed.",
        "details": {"status": "skipped"},
    }
    clickup_details = clickup_sync.get("details", clickup_sync)
    stale_details = stale_scan.get("details", stale_scan)
    if clickup_status != "ok":
        message = str(clickup_sync.get("message") or clickup_details.get("dashboard_error") or "Dashboard sync failed.")
        return {
            "clickup_sync": clickup_details,
            "stale_lead_scan": stale_details,
            "gmail_sync": {"status": "skipped", "reason": "enable once Gmail OAuth is fixed"},
            "dashboard_error": message,
            "message": message,
        }
    synced_tasks = int(clickup_details.get("synced_tasks", 0) or 0)
    inspected = int(stale_details.get("inspected", 0) or 0)
    if synced_tasks == 0:
        message = "Dashboard sync finished, but ClickUp returned 0 tasks. Check CLICKUP_LIST_ID and ClickUp token access."
    elif inspected == 0:
        message = "Dashboard sync finished, but 0 synced tasks matched the tracked active statuses."
    else:
        message = f"Dashboard sync finished. Synced {synced_tasks} tasks and found {inspected} active leads."

    return {
        "clickup_sync": clickup_details,
        "stale_lead_scan": stale_details,
        "gmail_sync": {"status": "skipped", "reason": "enable once Gmail OAuth is fixed"},
        "message": message,
    }


def _dashboard_has_sync_gap(dashboard: DashboardData) -> bool:
    latest_run_summary = dict(dashboard.latest_run_summary or {})
    return bool(str(latest_run_summary.get("dashboard_error", "") or "").strip())


def _executive_has_sync_gap(executive: ExecutiveData, *, dashboard: DashboardData) -> bool:
    latest_run_summary = dict(executive.latest_run_summary or {})
    if str(latest_run_summary.get("executive_error", "") or "").strip():
        return True
    if executive.latest_sync_at is None:
        return True
    return dashboard.total_active_leads > 0 and int(executive.kpis.get("active_leads", 0) or 0) == 0


def should_run_auto_dashboard_sync(
    request: Request,
    dashboard: DashboardData,
    admin_settings: AdminDashboardSettings,
    *,
    max_age_minutes_override: Optional[int] = None,
) -> bool:
    effective_settings = admin_settings
    if max_age_minutes_override is not None:
        effective_settings = replace(
            admin_settings,
            admin_auto_sync_max_age_minutes=max(0, int(max_age_minutes_override)),
        )

    if not latest_sync_is_stale(dashboard.latest_sync_at, effective_settings) and not _dashboard_has_sync_gap(dashboard):
        return False

    last_attempt = getattr(request.app.state, "admin_dashboard_last_auto_sync_at", None)
    if isinstance(last_attempt, datetime):
        current_time = datetime.now(timezone.utc)
        if last_attempt.tzinfo is None:
            last_attempt = last_attempt.replace(tzinfo=timezone.utc)
        if current_time - last_attempt < timedelta(minutes=5):
            return False

    return True


def _refresh_remote_dashboard_cache_for_app(app_instance: FastAPI, *, force: bool = False) -> dict[str, Any]:
    admin_settings = load_admin_dashboard_settings()
    current_time = datetime.now(timezone.utc)
    last_attempt = getattr(app_instance.state, "admin_dashboard_last_auto_sync_at", None)
    if not force and isinstance(last_attempt, datetime):
        normalized_last_attempt = last_attempt if last_attempt.tzinfo else last_attempt.replace(tzinfo=timezone.utc)
        if current_time - normalized_last_attempt < timedelta(minutes=5):
            result = {"status": "skipped", "running": False, "message": "Dashboard sync was attempted recently."}
            app_instance.state.admin_dashboard_last_auto_sync_result = result
            return result

    app_instance.state.admin_dashboard_last_auto_sync_at = current_time
    app_instance.state.admin_dashboard_last_auto_sync_result = {
        "status": "running",
        "running": True,
        "message": "Syncing dashboard data now...",
    }
    try:
        result = sync_remote_dashboard_sources()
        app_instance.state.admin_dashboard_last_auto_sync_at = datetime.now(timezone.utc)
        final_result = {
            "status": "error" if str(result.get("dashboard_error") or "").strip() else "ok",
            "running": False,
            **result,
        }
    except Exception as exc:
        final_result = {
            "status": "error",
            "running": False,
            "message": "Dashboard sync failed.",
            "error": str(exc),
        }
        app_instance.state.admin_dashboard_last_auto_sync_at = datetime.now(timezone.utc)
        app_instance.state.admin_dashboard_last_auto_sync_result = final_result
        raise

    app_instance.state.admin_dashboard_last_auto_sync_result = final_result
    return final_result


def _refresh_remote_dashboard_cache(request: Request, *, force: bool = False) -> dict[str, Any]:
    return _refresh_remote_dashboard_cache_for_app(request.app, force=force)


def _start_remote_dashboard_sync(request: Request, *, force: bool = False) -> dict[str, Any]:
    with ADMIN_SYNC_LOCK:
        current_future = getattr(request.app.state, "admin_dashboard_sync_future", None)
        if isinstance(current_future, Future) and not current_future.done():
            current_result = getattr(request.app.state, "admin_dashboard_last_auto_sync_result", None) or {}
            return {
                "status": str(current_result.get("status") or "running"),
                "running": True,
                "message": str(current_result.get("message") or "Syncing dashboard data now..."),
            }

        request.app.state.admin_dashboard_last_auto_sync_result = {
            "status": "running",
            "running": True,
            "message": "Syncing dashboard data in the background...",
        }
        request.app.state.admin_dashboard_sync_future = LEAD_RUN_EXECUTOR.submit(
            _refresh_remote_dashboard_cache_for_app,
            request.app,
            force=force,
        )
        return dict(request.app.state.admin_dashboard_last_auto_sync_result)


# ========= ROUTES =========
def _current_nav_user(request: Request) -> Optional[dict]:
    """Resolve the enriched RBAC user (permissions, is_superadmin) for the
    top-nav account chip. Never raises — nav rendering must not break pages."""
    try:
        from sales_support_agent.services.auth_deps import get_current_user
        return get_current_user(request)
    except Exception:
        return None


def _require_admin_tool(request: Request, tool_key: str, *, json_response: bool = False) -> tuple[Optional[dict], Optional[Response]]:
    """Imperative RBAC guard for admin handlers that cannot use FastAPI deps."""
    try:
        from sales_support_agent.services.auth_deps import require_tool_inline
        user, response = require_tool_inline(request, tool_key)
    except Exception:
        logger.exception("[AdminAuth] tool guard failed for %s", tool_key)
        return None, JSONResponse(status_code=401, content={"detail": "Admin login required."}) if json_response else RedirectResponse(url="/admin/login", status_code=302)
    if response is None:
        return user, None
    if json_response:
        status = getattr(response, "status_code", 403)
        if status in {301, 302, 303, 307, 308}:
            return None, JSONResponse(status_code=401, content={"detail": "Admin login required."})
        return None, JSONResponse(status_code=status, content={"detail": "Access denied."})
    return None, response


@app.get("/admin/login", response_class=HTMLResponse)
def admin_login_page(request: Request) -> Response:
    admin_settings = load_admin_dashboard_settings()
    if not admin_login_enabled(admin_settings):
        raise HTTPException(status_code=503, detail="Admin dashboard is not configured. Set ADMIN_DASHBOARD_PASSWORD.")
    token = request.cookies.get(admin_settings.admin_cookie_name, "")
    if validate_admin_session_token(admin_settings, token):
        return RedirectResponse(url="/admin", status_code=302)
    _agent_settings = getattr(request.app.state, "agent_settings", None)
    try:
        from sales_support_agent.services.admin_auth_google import google_oauth_enabled as _goe
        _show_google = bool(_agent_settings and _goe(_agent_settings))
    except Exception:
        _show_google = False
    try:
        from sales_support_agent.services.access.notify import email_delivery_configured
        _show_email = email_delivery_configured(_agent_settings)
    except Exception:
        _show_email = False
    _show_password = password_login_enabled(admin_settings)
    _oauth_errors = {
        "domain_not_allowed": ("This dashboard is for Anata Google accounts. "
                               "If you received an invite, open the invite link to sign in "
                               "with the email it was sent to."),
        "invalid_state": "Google sign-in expired — please try again.",
        "no_code": "Google sign-in was cancelled — please try again.",
        "token_exchange": "Google sign-in failed — please try again.",
    }
    _err = _oauth_errors.get(request.query_params.get("error", ""), "")
    if not (_show_google or _show_email or _show_password):
        _err = "No sign-in method is configured. Configure email delivery, Google OAuth, or the administrator fallback."
    elif not _err and not (_show_google or _show_email) and _show_password:
        _err = (
            "Email and Google sign-in are currently unavailable. Use the shared fallback "
            "password only if you already have break-glass access."
        )
    return HTMLResponse(render_login_page(show_google_button=_show_google,
                                          show_email_form=_show_email,
                                          show_password_form=_show_password,
                                          error_message=_err))


@app.post("/admin/login", response_class=HTMLResponse)
async def admin_login_submit(request: Request) -> Response:
    admin_settings = load_admin_dashboard_settings()
    if not password_login_enabled(admin_settings):
        raise HTTPException(status_code=404, detail="Password login is disabled. Use Google sign-in.")
    body = (await request.body()).decode("utf-8")
    parsed = parse_qs(body)
    password = parsed.get("password", [""])[0]
    email = (parsed.get("email", [""])[0] or "").strip().lower()
    _agent_settings_post = getattr(request.app.state, "agent_settings", None)
    try:
        from sales_support_agent.services.admin_auth_google import google_oauth_enabled as _goe2
        _show_google_post = bool(_agent_settings_post and _goe2(_agent_settings_post))
    except Exception:
        _show_google_post = False
    try:
        from sales_support_agent.services.access.notify import email_delivery_configured
        _show_email_post = email_delivery_configured(_agent_settings_post)
    except Exception:
        _show_email_post = False
    if not verify_admin_password(admin_settings, password):
        return HTMLResponse(
            render_login_page(
                error_message="Incorrect password.",
                show_google_button=_show_google_post,
                show_email_form=_show_email_post,
            ),
            status_code=401,
        )

    # Mint a 5-part identity token (same format as Google SSO) so the session
    # carries the submitter's email rather than the generic admin username.
    _name = email
    _role = "admin"
    try:
        from sales_support_agent.services.access import store as _access_store
        _u = _access_store.get_user_by_email(email)
        if _u:
            _name = _u.get("name") or email
            _role = _u.get("role") or "admin"
    except Exception:
        pass
    # Sign with admin_settings — the same settings object every /admin route
    # in this app validates against (the RBAC middleware tries all secrets).
    response = RedirectResponse(url="/admin", status_code=302)
    response.set_cookie(
        value=create_user_session_token(admin_settings, email=email or admin_settings.admin_username, name=_name, role=_role),
        **_admin_cookie_options(request, admin_settings),
    )
    try:
        _start_remote_dashboard_sync(request, force=True)
    except Exception:
        logger.exception("[AdminDashboard] login-triggered sync failed")
    return response


@app.get("/admin/logout")
def admin_logout(request: Request) -> RedirectResponse:
    admin_settings = load_admin_dashboard_settings()
    response = RedirectResponse(url="/admin/login", status_code=302)
    response.delete_cookie(admin_settings.admin_cookie_name, path="/")
    return response


@app.get("/admin", response_class=HTMLResponse)
def admin_dashboard(request: Request) -> Response:
    admin_settings = load_admin_dashboard_settings()
    if not admin_login_enabled(admin_settings):
        raise HTTPException(status_code=503, detail="Admin dashboard is not configured. Set ADMIN_DASHBOARD_PASSWORD.")
    token = request.cookies.get(admin_settings.admin_cookie_name, "")
    if not validate_admin_session_token(admin_settings, token):
        return RedirectResponse(url="/admin/login", status_code=302)
    return HTMLResponse(render_admin_home_page(user=_current_nav_user(request)))


@app.get("/admin/sales-decks", response_class=HTMLResponse)
@app.get("/admin/sales/decks/", response_class=HTMLResponse)
def admin_sales_decks(request: Request) -> Response:
    admin_settings = load_admin_dashboard_settings()
    if not admin_login_enabled(admin_settings):
        raise HTTPException(status_code=503, detail="Admin dashboard is not configured. Set ADMIN_DASHBOARD_PASSWORD.")
    token = request.cookies.get(admin_settings.admin_cookie_name, "")
    if not validate_admin_session_token(admin_settings, token):
        return RedirectResponse(url="/admin/login", status_code=302)
    dashboard = fetch_remote_dashboard_data()
    return HTMLResponse(render_sales_deck_page(dashboard, user=_current_nav_user(request)))


@app.get("/admin/sales/decks")
def admin_sales_decks_canonical_redirect(request: Request) -> Response:
    admin_settings = load_admin_dashboard_settings()
    if not admin_login_enabled(admin_settings):
        raise HTTPException(status_code=503, detail="Admin dashboard is not configured. Set ADMIN_DASHBOARD_PASSWORD.")
    token = request.cookies.get(admin_settings.admin_cookie_name, "")
    if not validate_admin_session_token(admin_settings, token):
        return RedirectResponse(url="/admin/login", status_code=302)
    return RedirectResponse(url="/admin/sales/decks/", status_code=303)


@app.get("/admin/executive", response_class=HTMLResponse)
def admin_executive_dashboard(request: Request) -> Response:
    admin_settings = load_admin_dashboard_settings()
    if not admin_login_enabled(admin_settings):
        raise HTTPException(status_code=503, detail="Admin dashboard is not configured. Set ADMIN_DASHBOARD_PASSWORD first.")

    token = request.cookies.get(admin_settings.admin_cookie_name, "")
    if not validate_admin_session_token(admin_settings, token):
        return RedirectResponse(url="/admin/login", status_code=302)
    dashboard = fetch_remote_dashboard_data()
    executive = fetch_remote_executive_data()
    should_refresh_executive = should_run_auto_dashboard_sync(
        request,
        dashboard,
        admin_settings,
        max_age_minutes_override=max(admin_settings.admin_auto_sync_max_age_minutes, 60),
    )
    if not should_refresh_executive and _executive_has_sync_gap(executive, dashboard=dashboard):
        should_refresh_executive = True
    if should_refresh_executive:
        try:
            _refresh_remote_dashboard_cache(request, force=False)
            dashboard = fetch_remote_dashboard_data()
            executive = fetch_remote_executive_data()
        except Exception:
            logger.exception("[ExecutiveDashboard] auto sync on page load failed")
    return HTMLResponse(render_executive_page(executive, user=_current_nav_user(request)))


@app.get("/admin/fulfillment", response_class=HTMLResponse)
def admin_fulfillment_root(request: Request) -> Response:
    admin_settings = load_admin_dashboard_settings()
    if not admin_login_enabled(admin_settings):
        raise HTTPException(status_code=503, detail="Admin dashboard is not configured. Set ADMIN_DASHBOARD_PASSWORD.")
    token = request.cookies.get(admin_settings.admin_cookie_name, "")
    if not validate_admin_session_token(admin_settings, token):
        return RedirectResponse(url="/admin/login", status_code=302)
    from sales_support_agent.services.auth_deps import has_tool
    if has_tool(request, "fulfillment.rate_sheets"):
        return RedirectResponse(url="/admin/fulfillment/sales", status_code=302)
    if has_tool(request, "fulfillment.dashboard"):
        return RedirectResponse(url="/admin/fulfillment/cs/", status_code=302)
    return RedirectResponse(url="/admin/fulfillment/sales", status_code=302)


@app.get("/admin/fulfillment-cs{rest:path}")
def admin_fulfillment_cs_legacy_redirect(rest: str) -> Response:
    # The CS pages moved under the renamed Fulfillment section; keep old links working.
    return RedirectResponse(url=f"/admin/fulfillment/cs{rest}", status_code=301)


@app.get("/admin/fulfillment/cs", response_class=HTMLResponse)
def admin_fulfillment_cs_root(request: Request) -> Response:
    admin_settings = load_admin_dashboard_settings()
    if not admin_login_enabled(admin_settings):
        raise HTTPException(status_code=503, detail="Admin dashboard is not configured. Set ADMIN_DASHBOARD_PASSWORD.")
    token = request.cookies.get(admin_settings.admin_cookie_name, "")
    if not validate_admin_session_token(admin_settings, token):
        return RedirectResponse(url="/admin/login", status_code=302)
    return RedirectResponse(url="/admin/fulfillment/cs/", status_code=302)


@app.get("/admin/fulfillment/cs/", response_class=HTMLResponse)
def admin_fulfillment_cs_dashboard(request: Request) -> Response:
    admin_settings = load_admin_dashboard_settings()
    if not admin_login_enabled(admin_settings):
        raise HTTPException(status_code=503, detail="Admin dashboard is not configured. Set ADMIN_DASHBOARD_PASSWORD.")
    token = request.cookies.get(admin_settings.admin_cookie_name, "")
    if not validate_admin_session_token(admin_settings, token):
        return RedirectResponse(url="/admin/login", status_code=302)
    settings = load_fulfillment_cs_settings()
    latest_report = load_latest_fulfillment_report(settings.fulfillment_cs_reports_dir)
    entries = fulfillment_report_entries(settings.fulfillment_cs_reports_dir)
    from sales_support_agent.services.auth_deps import get_current_user
    return HTMLResponse(render_fulfillment_dashboard_page(latest_report, entries, user=get_current_user(request)))


@app.get("/admin/fulfillment/cs/reports", response_class=HTMLResponse)
def admin_fulfillment_cs_reports_root(request: Request) -> Response:
    admin_settings = load_admin_dashboard_settings()
    if not admin_login_enabled(admin_settings):
        raise HTTPException(status_code=503, detail="Admin dashboard is not configured. Set ADMIN_DASHBOARD_PASSWORD.")
    token = request.cookies.get(admin_settings.admin_cookie_name, "")
    if not validate_admin_session_token(admin_settings, token):
        return RedirectResponse(url="/admin/login", status_code=302)
    return RedirectResponse(url="/admin/fulfillment/cs/reports/", status_code=302)


@app.get("/admin/fulfillment/cs/reports/", response_class=HTMLResponse)
def admin_fulfillment_cs_reports(request: Request) -> Response:
    admin_settings = load_admin_dashboard_settings()
    if not admin_login_enabled(admin_settings):
        raise HTTPException(status_code=503, detail="Admin dashboard is not configured. Set ADMIN_DASHBOARD_PASSWORD.")
    token = request.cookies.get(admin_settings.admin_cookie_name, "")
    if not validate_admin_session_token(admin_settings, token):
        return RedirectResponse(url="/admin/login", status_code=302)
    settings = load_fulfillment_cs_settings()
    from sales_support_agent.services.auth_deps import get_current_user
    return HTMLResponse(render_fulfillment_reports_page(fulfillment_report_entries(settings.fulfillment_cs_reports_dir), user=get_current_user(request)))


@app.get("/admin/fulfillment/cs/reports/latest")
def admin_fulfillment_cs_reports_latest(request: Request) -> Response:
    admin_settings = load_admin_dashboard_settings()
    if not admin_login_enabled(admin_settings):
        raise HTTPException(status_code=503, detail="Admin dashboard is not configured. Set ADMIN_DASHBOARD_PASSWORD.")
    token = request.cookies.get(admin_settings.admin_cookie_name, "")
    if not validate_admin_session_token(admin_settings, token):
        return RedirectResponse(url="/admin/login", status_code=302)
    latest_entry = latest_fulfillment_report_entry(load_fulfillment_cs_settings().fulfillment_cs_reports_dir)
    if latest_entry is None:
        return RedirectResponse(url="/admin/fulfillment/cs/reports/", status_code=302)
    return RedirectResponse(url=f"/admin/fulfillment/cs/reports/{latest_entry.slug}", status_code=302)


def _fulfillment_report_artifact_response(
    request: Request,
    *,
    report_slug: str,
    extension: str,
) -> Response:
    admin_settings = load_admin_dashboard_settings()
    if not admin_login_enabled(admin_settings):
        raise HTTPException(status_code=503, detail="Admin dashboard is not configured. Set ADMIN_DASHBOARD_PASSWORD.")
    token = request.cookies.get(admin_settings.admin_cookie_name, "")
    if not validate_admin_session_token(admin_settings, token):
        return RedirectResponse(url="/admin/login", status_code=302)
    artifact = load_fulfillment_report_artifact(load_fulfillment_cs_settings().fulfillment_cs_reports_dir, report_slug, extension)
    if artifact is None:
        from sales_support_agent.services.auth_deps import get_current_user
        return HTMLResponse(render_fulfillment_not_found_page("The requested fulfillment report artifact was not found.", user=get_current_user(request)), status_code=404)
    body, media_type = artifact
    return Response(content=body, media_type=media_type)


@app.get("/admin/fulfillment/cs/reports/{report_slug}.json")
def admin_fulfillment_cs_report_json(request: Request, report_slug: str) -> Response:
    return _fulfillment_report_artifact_response(request, report_slug=report_slug, extension="json")


@app.get("/admin/fulfillment/cs/reports/{report_slug}.md")
def admin_fulfillment_cs_report_markdown(request: Request, report_slug: str) -> Response:
    return _fulfillment_report_artifact_response(request, report_slug=report_slug, extension="md")


@app.get("/admin/fulfillment/cs/reports/{report_slug}.html")
def admin_fulfillment_cs_report_html(request: Request, report_slug: str) -> Response:
    return _fulfillment_report_artifact_response(request, report_slug=report_slug, extension="html")


@app.get("/admin/fulfillment/cs/reports/{report_slug}", response_class=HTMLResponse)
def admin_fulfillment_cs_report_detail(request: Request, report_slug: str) -> Response:
    admin_settings = load_admin_dashboard_settings()
    if not admin_login_enabled(admin_settings):
        raise HTTPException(status_code=503, detail="Admin dashboard is not configured. Set ADMIN_DASHBOARD_PASSWORD.")
    token = request.cookies.get(admin_settings.admin_cookie_name, "")
    if not validate_admin_session_token(admin_settings, token):
        return RedirectResponse(url="/admin/login", status_code=302)
    report = load_fulfillment_report_by_slug(load_fulfillment_cs_settings().fulfillment_cs_reports_dir, report_slug)
    if report is None:
        from sales_support_agent.services.auth_deps import get_current_user
        return HTMLResponse(render_fulfillment_not_found_page("The requested fulfillment report was not found.", user=get_current_user(request)), status_code=404)
    from sales_support_agent.services.auth_deps import get_current_user
    return HTMLResponse(
        render_fulfillment_report_detail_page(
            report,
            report_slug=report_slug,
            user=get_current_user(request),
        )
    )


def _website_ops_run_status(request: Request, *, mode: str = "daily") -> dict[str, Any]:
    settings = load_website_ops_settings()
    state = get_website_ops_run_state(settings, mode)
    with request.app.state.website_ops_run_lock:
        future = request.app.state.website_ops_run_futures.get(mode)
        running = bool(future and not future.done())
    if state.get("status") in {"queued", "running"} and not running:
        state = write_website_ops_run_state(
            settings,
            mode,
            {
                "status": "failed",
                "last_completed_at": datetime.now(timezone.utc).isoformat(),
                "last_error": state.get("last_error") or "Background run was interrupted before completion.",
            },
        )
    return {
        **state,
        "running": running,
        "due": website_ops_run_is_due(settings, mode),
    }


def _website_ops_run_worker(app_instance: FastAPI, settings: WebsiteOpsHostSettings, *, mode: str, trigger: str) -> None:
    now = datetime.now(timezone.utc)
    run_date = now.date().isoformat()
    write_website_ops_run_state(
        settings,
        mode,
        {
            "mode": mode,
            "status": "running",
            "run_date": run_date,
            "trigger": trigger,
            "last_started_at": now.isoformat(),
            "last_error": "",
        },
    )
    try:
        run_website_ops_pipeline(settings, mode=mode)
    except Exception as exc:
        logger.exception("[WebsiteOps] %s run failed trigger=%s", mode, trigger)
        send_website_ops_failure_email(settings, mode=mode, error=str(exc))
        write_website_ops_run_state(
            settings,
            mode,
            {
                "mode": mode,
                "status": "failed",
                "run_date": run_date,
                "trigger": trigger,
                "last_completed_at": datetime.now(timezone.utc).isoformat(),
                "last_error": str(exc),
            },
        )
    else:
        write_website_ops_run_state(
            settings,
            mode,
            {
                "mode": mode,
                "status": "succeeded",
                "run_date": run_date,
                "trigger": trigger,
                "last_completed_at": datetime.now(timezone.utc).isoformat(),
                "last_successful_date": run_date,
                "last_error": "",
            },
        )
    finally:
        with app_instance.state.website_ops_run_lock:
            app_instance.state.website_ops_run_futures.pop(mode, None)


def _start_website_ops_run(request: Request, *, mode: str, force: bool, trigger: str) -> dict[str, Any]:
    settings = load_website_ops_settings()
    with request.app.state.website_ops_run_lock:
        future = request.app.state.website_ops_run_futures.get(mode)
        if future and not future.done():
            status = "running"
            message = f"{mode.title()} Website Ops run is already running."
        elif not force and not website_ops_run_is_due(settings, mode):
            status = "skipped"
            message = f"{mode.title()} Website Ops run already completed for today."
        else:
            now = datetime.now(timezone.utc)
            write_website_ops_run_state(
                settings,
                mode,
                {
                    "mode": mode,
                    "status": "queued",
                    "run_date": now.date().isoformat(),
                    "trigger": trigger,
                    "last_started_at": now.isoformat(),
                    "last_error": "",
                },
            )
            request.app.state.website_ops_run_futures[mode] = request.app.state.website_ops_run_executor.submit(
                _website_ops_run_worker,
                request.app,
                settings,
                mode=mode,
                trigger=trigger,
            )
            status = "queued"
            message = f"{mode.title()} Website Ops run queued."
    return {
        **_website_ops_run_status(request, mode=mode),
        "status": status,
        "message": message,
    }


@app.get("/admin/website-ops", response_class=HTMLResponse)
def admin_website_ops(request: Request) -> Response:
    admin_settings = load_admin_dashboard_settings()
    if not admin_login_enabled(admin_settings):
        raise HTTPException(status_code=503, detail="Admin dashboard is not configured. Set ADMIN_DASHBOARD_PASSWORD.")
    token = request.cookies.get(admin_settings.admin_cookie_name, "")
    if not validate_admin_session_token(admin_settings, token):
        return RedirectResponse(url="/admin/login", status_code=302)
    return HTMLResponse(render_website_ops_dashboard_page(load_website_ops_settings(), user=_current_nav_user(request)))


@app.get("/admin/website-ops/queue", response_class=HTMLResponse)
def admin_website_ops_queue(request: Request, status: str = "") -> Response:
    admin_settings = load_admin_dashboard_settings()
    if not admin_login_enabled(admin_settings):
        raise HTTPException(status_code=503, detail="Admin dashboard is not configured. Set ADMIN_DASHBOARD_PASSWORD.")
    token = request.cookies.get(admin_settings.admin_cookie_name, "")
    if not validate_admin_session_token(admin_settings, token):
        return RedirectResponse(url="/admin/login", status_code=302)
    return HTMLResponse(render_website_ops_queue_page(load_website_ops_settings(), status_filter=status, user=_current_nav_user(request)))


@app.get("/admin/website-ops/content", response_class=HTMLResponse)
def admin_website_ops_content(request: Request) -> Response:
    admin_settings = load_admin_dashboard_settings()
    if not admin_login_enabled(admin_settings):
        raise HTTPException(status_code=503, detail="Admin dashboard is not configured. Set ADMIN_DASHBOARD_PASSWORD.")
    token = request.cookies.get(admin_settings.admin_cookie_name, "")
    if not validate_admin_session_token(admin_settings, token):
        return RedirectResponse(url="/admin/login", status_code=302)
    return HTMLResponse(render_website_ops_content_page(load_website_ops_settings(), user=_current_nav_user(request)))


@app.get("/admin/website-ops/site-health", response_class=HTMLResponse)
def admin_website_ops_site_health(request: Request) -> Response:
    admin_settings = load_admin_dashboard_settings()
    if not admin_login_enabled(admin_settings):
        raise HTTPException(status_code=503, detail="Admin dashboard is not configured. Set ADMIN_DASHBOARD_PASSWORD.")
    token = request.cookies.get(admin_settings.admin_cookie_name, "")
    if not validate_admin_session_token(admin_settings, token):
        return RedirectResponse(url="/admin/login", status_code=302)
    return HTMLResponse(render_website_ops_site_health_page(load_website_ops_settings(), user=_current_nav_user(request)))


@app.get("/admin/website-ops/indexing", response_class=HTMLResponse)
def admin_website_ops_indexing(request: Request) -> Response:
    admin_settings = load_admin_dashboard_settings()
    if not admin_login_enabled(admin_settings):
        raise HTTPException(status_code=503, detail="Admin dashboard is not configured. Set ADMIN_DASHBOARD_PASSWORD.")
    token = request.cookies.get(admin_settings.admin_cookie_name, "")
    if not validate_admin_session_token(admin_settings, token):
        return RedirectResponse(url="/admin/login", status_code=302)
    return HTMLResponse(render_website_ops_indexing_page(load_website_ops_settings(), user=_current_nav_user(request)))


@app.get("/admin/website-ops/reports", response_class=HTMLResponse)
def admin_website_ops_reports(request: Request) -> Response:
    admin_settings = load_admin_dashboard_settings()
    if not admin_login_enabled(admin_settings):
        raise HTTPException(status_code=503, detail="Admin dashboard is not configured. Set ADMIN_DASHBOARD_PASSWORD.")
    token = request.cookies.get(admin_settings.admin_cookie_name, "")
    if not validate_admin_session_token(admin_settings, token):
        return RedirectResponse(url="/admin/login", status_code=302)
    return HTMLResponse(render_website_ops_reports_page(load_website_ops_settings(), user=_current_nav_user(request)))


@app.get("/admin/website-ops/reports/latest")
def admin_website_ops_reports_latest(request: Request) -> Response:
    admin_settings = load_admin_dashboard_settings()
    if not admin_login_enabled(admin_settings):
        raise HTTPException(status_code=503, detail="Admin dashboard is not configured. Set ADMIN_DASHBOARD_PASSWORD.")
    token = request.cookies.get(admin_settings.admin_cookie_name, "")
    if not validate_admin_session_token(admin_settings, token):
        return RedirectResponse(url="/admin/login", status_code=302)
    settings = load_website_ops_settings()
    latest = latest_website_ops_report_entry(settings)
    if not latest:
        return RedirectResponse(url="/admin/website-ops/reports", status_code=302)
    return RedirectResponse(url=f"/admin/website-ops/reports/{latest['mode']}/{latest['slug']}", status_code=302)


@app.get("/admin/website-ops/reports/{mode}/{slug}", response_class=HTMLResponse)
def admin_website_ops_report_detail(request: Request, mode: str, slug: str) -> Response:
    admin_settings = load_admin_dashboard_settings()
    if not admin_login_enabled(admin_settings):
        raise HTTPException(status_code=503, detail="Admin dashboard is not configured. Set ADMIN_DASHBOARD_PASSWORD.")
    token = request.cookies.get(admin_settings.admin_cookie_name, "")
    if not validate_admin_session_token(admin_settings, token):
        return RedirectResponse(url="/admin/login", status_code=302)
    return HTMLResponse(render_website_ops_report_page(load_website_ops_settings(), mode, slug, user=_current_nav_user(request)))


@app.get("/admin/website-ops/feedback/{feedback_id}", response_class=HTMLResponse)
def admin_website_ops_feedback_detail(request: Request, feedback_id: str) -> Response:
    admin_settings = load_admin_dashboard_settings()
    if not admin_login_enabled(admin_settings):
        raise HTTPException(status_code=503, detail="Admin dashboard is not configured. Set ADMIN_DASHBOARD_PASSWORD.")
    token = request.cookies.get(admin_settings.admin_cookie_name, "")
    if not validate_admin_session_token(admin_settings, token):
        return RedirectResponse(url="/admin/login", status_code=302)
    return HTMLResponse(render_website_ops_feedback_detail_page(load_website_ops_settings(), feedback_id, user=_current_nav_user(request)))



@app.get("/admin/api/outbound/brands.csv", response_model=None)
def admin_outbound_brands_csv(request: Request, max_new: int = 100) -> Response:
    """Pull ICP-matched, not-yet-contacted brands from StoreLeads and return them
    as a CSV to import into Clay. Sends nothing and pushes nothing. Used on the
    Launch plan before Clay webhooks unlock on Growth.
    """
    admin_settings = load_admin_dashboard_settings()
    token = request.cookies.get(admin_settings.admin_cookie_name, "")
    if not validate_admin_session_token(admin_settings, token):
        return JSONResponse(status_code=401, content={"detail": "Admin login required."})

    import outbound_pipeline as _op

    api_key, _clay_webhook = _op.load_config_from_env()
    if not api_key:
        return JSONResponse(status_code=400, content={"detail": "STORELEADS_API_KEY is not set on the server."})

    from sales_support_agent.services import outbound_memory

    engine = request.app.state.session_factory.kw.get("bind")
    processed = outbound_memory.load_contacted(engine)
    try:
        result = _op.run_storeleads_to_clay(
            api_key=api_key,
            clay_webhook_url="",  # CSV mode: always dry-run, never push
            processed_domains=processed,
            max_new=max(1, min(int(max_new or 100), 500)),
            dry_run=True,
        )
    except Exception as exc:  # noqa: BLE001 — surface a clean error to the operator
        logger.exception("[outbound] StoreLeads CSV build failed")
        return JSONResponse(status_code=502, content={"detail": f"StoreLeads fetch failed: {exc}"})

    csv_text = _op.leads_to_csv(result.leads)
    return Response(
        content=csv_text,
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="anata_clay_brands.csv"'},
    )


@app.get("/admin/api/sync-dashboard/status")
def admin_sync_dashboard_status(request: Request) -> JSONResponse:
    admin_settings = load_admin_dashboard_settings()
    token = request.cookies.get(admin_settings.admin_cookie_name, "")
    if not validate_admin_session_token(admin_settings, token):
        return JSONResponse(status_code=401, content={"detail": "Admin login required."})

    dashboard = fetch_remote_dashboard_data()
    details = getattr(request.app.state, "admin_dashboard_last_auto_sync_result", None) or {
        "status": "idle",
        "running": False,
        "message": "Dashboard sync has not run in this session yet.",
    }
    details = {
        **details,
        "stale": latest_sync_is_stale(dashboard.latest_sync_at, admin_settings) or _dashboard_has_sync_gap(dashboard),
    }
    return JSONResponse(status_code=200, content={"status": "ok", "details": details})


@app.post("/admin/api/sync-dashboard")
def admin_sync_dashboard(request: Request) -> JSONResponse:
    admin_settings = load_admin_dashboard_settings()
    token = request.cookies.get(admin_settings.admin_cookie_name, "")
    if not validate_admin_session_token(admin_settings, token):
        return JSONResponse(status_code=401, content={"detail": "Admin login required."})

    background = str(request.query_params.get("background", "false")).strip().lower() == "true"
    only_if_stale = str(request.query_params.get("only_if_stale", "false")).strip().lower() == "true"
    if only_if_stale:
        dashboard = fetch_remote_dashboard_data()
        if not should_run_auto_dashboard_sync(request, dashboard, admin_settings):
            result = {"status": "skipped", "running": False, "message": "Board cache is still fresh."}
            request.app.state.admin_dashboard_last_auto_sync_result = result
            return JSONResponse(status_code=200, content={"status": "ok", "message": result["message"], "details": result})

    try:
        result = _start_remote_dashboard_sync(request, force=not only_if_stale) if background else _refresh_remote_dashboard_cache(request, force=not only_if_stale)
    except Exception as exc:
        logger.exception("[AdminDashboard] sync failed")
        request.app.state.admin_dashboard_last_auto_sync_result = {
            "status": "error",
            "running": False,
            "message": "Dashboard sync failed.",
            "error": str(exc),
        }
        return JSONResponse(
            status_code=500,
            content={"detail": "Dashboard sync failed.", "error": str(exc)},
        )
    return JSONResponse(
        status_code=200,
        content={
            "status": "ok",
            "message": str(result.get("message", "Dashboard sync completed.")),
            "details": {**result, "requested_background": background},
        },
    )


@app.post("/admin/api/website-ops/run")
async def admin_website_ops_run(request: Request, mode: str = Form(default="daily")) -> Response:
    admin_settings = load_admin_dashboard_settings()
    if not admin_login_enabled(admin_settings):
        raise HTTPException(status_code=503, detail="Admin dashboard is not configured. Set ADMIN_DASHBOARD_PASSWORD.")
    _, auth_response = _require_admin_tool(request, "website_ops.seo", json_response=True)
    if auth_response is not None:
        return auth_response

    normalized_mode = (mode or "daily").strip().lower()
    if normalized_mode not in {"daily", "weekly", "monthly"}:
        return JSONResponse(status_code=400, content={"detail": "Unsupported run mode."})
    _start_website_ops_run(request, mode=normalized_mode, force=True, trigger="manual")
    return RedirectResponse(url="/admin/website-ops", status_code=302)


@app.post("/api/jobs/website-ops/run")
async def scheduled_website_ops_run(request: Request) -> JSONResponse:
    """Authenticated Render cron entrypoint for Website Ops."""

    expected = os.getenv("SALES_AGENT_INTERNAL_API_KEY", "").strip()
    supplied = request.headers.get("X-Internal-Api-Key", "").strip()
    if not expected or not secrets.compare_digest(supplied, expected):
        raise HTTPException(status_code=401, detail="Invalid internal API key.")
    try:
        payload = await request.json()
    except (ValueError, json.JSONDecodeError):
        payload = {}
    mode = str(payload.get("mode", "scheduled") or "scheduled").strip().lower()
    if mode not in {"scheduled", "daily", "weekly", "monthly"}:
        raise HTTPException(status_code=400, detail="Unsupported run mode.")
    local_now = datetime.now(ZoneInfo("America/Denver"))
    if mode == "scheduled" and local_now.hour != 8:
        return JSONResponse(
            status_code=200,
            content={
                "status": "skipped",
                "message": "Website Ops scheduler is waiting for 8:00 AM America/Denver.",
                "local_time": local_now.isoformat(),
            },
        )

    modes = [mode] if mode != "scheduled" else ["daily"]
    if mode == "scheduled" and local_now.weekday() == 0:
        modes.append("weekly")
        if local_now.day <= 7:
            modes.append("monthly")
    details = {
        selected_mode: _start_website_ops_run(
            request,
            mode=selected_mode,
            force=False,
            trigger="render_cron",
        )
        for selected_mode in modes
    }
    return JSONResponse(status_code=202, content={"status": "ok", "details": details})


@app.get("/admin/api/website-ops/status")
def admin_website_ops_status(request: Request, mode: str = "daily") -> JSONResponse:
    admin_settings = load_admin_dashboard_settings()
    if not admin_login_enabled(admin_settings):
        raise HTTPException(status_code=503, detail="Admin dashboard is not configured. Set ADMIN_DASHBOARD_PASSWORD.")
    _, auth_response = _require_admin_tool(request, "website_ops.seo", json_response=True)
    if auth_response is not None:
        return auth_response

    normalized_mode = (mode or "daily").strip().lower()
    if normalized_mode not in {"daily", "weekly", "monthly"}:
        return JSONResponse(status_code=400, content={"detail": "Unsupported run mode."})
    return JSONResponse(status_code=200, content={"status": "ok", "details": _website_ops_run_status(request, mode=normalized_mode)})


@app.post("/admin/api/website-ops/feedback")
async def admin_website_ops_feedback_submit(
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
    admin_settings = load_admin_dashboard_settings()
    if not admin_login_enabled(admin_settings):
        raise HTTPException(status_code=503, detail="Admin dashboard is not configured. Set ADMIN_DASHBOARD_PASSWORD.")
    user, auth_response = _require_admin_tool(request, "website_ops.queue", json_response=True)
    if auth_response is not None:
        return auth_response

    record = save_website_ops_feedback_record(
        load_website_ops_settings(),
        {
            "category": category,
            "priority": priority,
            "page_url": page_url,
            "page_title": page_title,
            "summary": summary,
            "details": details,
            "desired_outcome": desired_outcome,
            "recommended_fix": recommended_fix,
            "reporter_name": reporter_name or str((user or {}).get("name") or ""),
            "reporter_email": reporter_email or str((user or {}).get("email") or ""),
        },
    )
    return RedirectResponse(url=f"/admin/website-ops/feedback/{record['feedback_id']}", status_code=302)


@app.post("/admin/api/website-ops/feedback/{feedback_id}/review")
async def admin_website_ops_feedback_review(
    request: Request,
    feedback_id: str,
    status: str = Form(default="new"),
    reviewer_name: str = Form(default=""),
    review_notes: str = Form(default=""),
    action_type: str = Form(default=""),
    action_value: str = Form(default=""),
    target_post_id: str = Form(default=""),
) -> Response:
    admin_settings = load_admin_dashboard_settings()
    if not admin_login_enabled(admin_settings):
        raise HTTPException(status_code=503, detail="Admin dashboard is not configured. Set ADMIN_DASHBOARD_PASSWORD.")
    user, auth_response = _require_admin_tool(request, "website_ops.queue", json_response=True)
    if auth_response is not None:
        return auth_response

    result = review_website_ops_feedback_record(
        load_website_ops_settings(),
        feedback_id,
        {
            "status": status,
            "reviewer_name": reviewer_name,
            "review_notes": review_notes,
            "action_type": action_type,
            "action_value": action_value,
            "target_post_id": target_post_id,
        },
        reviewer=user,
    )
    if not result.ok and not result.record:
        return JSONResponse(status_code=404, content={"detail": result.message})
    return RedirectResponse(url=f"/admin/website-ops/feedback/{feedback_id}", status_code=302)


@app.get("/admin/api/canva/connect", response_model=None)
def admin_canva_connect_proxy(request: Request) -> Response:
    admin_settings = load_admin_dashboard_settings()
    token = request.cookies.get(admin_settings.admin_cookie_name, "")
    if not validate_admin_session_token(admin_settings, token):
        return RedirectResponse(url="/admin/login", status_code=302)
    if not admin_settings.sales_support_agent_url:
        return JSONResponse(status_code=500, content={"detail": "Sales support agent URL is not configured on this service."})

    return_to = str(request.base_url).rstrip("/") + "/admin"
    redirect_url = (
        f"{admin_settings.sales_support_agent_url}/api/admin/canva/connect"
        f"?token={quote(admin_settings.sales_agent_internal_api_key, safe='')}"
        f"&return_to={quote(return_to, safe='')}"
    )
    return RedirectResponse(url=redirect_url, status_code=302)


@app.delete("/admin/api/deck-runs/{run_id}", response_model=None)
def admin_delete_deck_run_proxy(request: Request, run_id: int) -> JSONResponse:
    """PR52: proxy DELETE for past-decks-table delete button. Admin-cookie
    gated on the frontend, then forwarded to the backend internal API
    (POST /api/admin/deck-runs/{run_id}/delete) which is keyed by the
    internal API key. Backend deletes the AutomationRun row."""
    admin_settings = load_admin_dashboard_settings()
    token = request.cookies.get(admin_settings.admin_cookie_name, "")
    if not validate_admin_session_token(admin_settings, token):
        return JSONResponse(status_code=401, content={"detail": "Admin login required."})
    if not admin_settings.sales_support_agent_url or not admin_settings.sales_agent_internal_api_key:
        return JSONResponse(
            status_code=500,
            content={"detail": "Sales support agent URL or internal API key is not configured."},
        )
    try:
        response = requests.post(
            f"{admin_settings.sales_support_agent_url}/api/admin/deck-runs/{run_id}/delete",
            headers={"X-Internal-Api-Key": admin_settings.sales_agent_internal_api_key},
            timeout=30,
        )
    except requests.RequestException as exc:
        return JSONResponse(
            status_code=502,
            content={"detail": f"Failed to reach backend: {exc}"},
        )
    try:
        body = response.json()
    except ValueError:
        body = {"detail": response.text or "Unexpected backend response."}
    return JSONResponse(status_code=response.status_code, content=body)


@app.post("/admin/api/digital-shelf/generate-deck")
async def admin_digital_shelf_generate_deck_proxy(request: Request) -> JSONResponse:
    """Digital Shelf proxy: forward JSON body to the backend, no file uploads."""
    admin_settings = load_admin_dashboard_settings()
    token = request.cookies.get(admin_settings.admin_cookie_name, "")
    if not validate_admin_session_token(admin_settings, token):
        return JSONResponse(status_code=401, content={"detail": "Admin login required."})
    if not admin_settings.sales_support_agent_url:
        return JSONResponse(status_code=500, content={"detail": "Sales support agent URL not configured."})

    try:
        body_bytes = await request.body()
    except Exception:
        body_bytes = b"{}"

    try:
        response = requests.post(
            f"{admin_settings.sales_support_agent_url}/admin/api/digital-shelf/generate-deck",
            data=body_bytes,
            headers={
                "Content-Type": "application/json",
                "Cookie": f"{admin_settings.admin_cookie_name}={token}",
            },
            timeout=120,
        )
    except requests.RequestException as exc:
        return JSONResponse(status_code=502, content={"detail": f"Failed to reach backend: {exc}"})

    try:
        payload = response.json()
    except ValueError:
        payload = {"detail": response.text or "Unexpected backend response."}
    return JSONResponse(status_code=response.status_code, content=payload)


@app.post("/admin/api/generate-deck")
async def admin_generate_deck_proxy(
    request: Request,
    # PR42: accept the unified upload field PR40 added to the form. Also
    # made every per-type field optional + added target_xray_csv (which
    # was missing from the proxy entirely so the previous "Target Xray"
    # input was being silently dropped).
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
) -> JSONResponse:
    admin_settings = load_admin_dashboard_settings()
    token = request.cookies.get(admin_settings.admin_cookie_name, "")
    if not validate_admin_session_token(admin_settings, token):
        return JSONResponse(status_code=401, content={"detail": "Admin login required."})

    csv_unified_files = [file for file in (csv_files or []) if file and file.filename]
    competitor_files = [file for file in competitor_xray_csv if file.filename]
    keyword_files = [file for file in keyword_xray_csv if file.filename]
    cerebro_bytes = await cerebro_csv.read() if cerebro_csv and cerebro_csv.filename else b""
    word_frequency_bytes = await word_frequency_csv.read() if word_frequency_csv and word_frequency_csv.filename else b""
    target_xray_bytes = (
        await target_xray_csv.read()
        if target_xray_csv and target_xray_csv.filename
        else b""
    )

    # PR42: forward the growth-plan form fields too. The backend reads
    # them off `request.form()` so we need to pass them through.
    form_data = await request.form()

    try:
        status_code, payload = _post_sales_support_multipart(
            "/api/admin/generate-deck",
            data_items=[
                ("target_product_input", target_product_input),
                *[("channels", channel) for channel in channels],
                ("creative_mockup_url", creative_mockup_url),
                ("case_study_url", case_study_url),
                *[("offers", offer) for offer in offers],
                ("offer_payload_json", offer_payload_json),
                ("include_recommended_plan", "true" if include_recommended_plan else "false"),
                ("include_growth_plan", "true" if include_growth_plan else "false"),
                # Pass through every growth_* form key the backend expects.
                *[
                    (key, str(form_data.get(key)))
                    for key in form_data.keys()
                    if isinstance(key, str) and key.startswith("growth_") and form_data.get(key) not in (None, "")
                ],
                # PR47: forward the optional category_label override.
                *(
                    [("category_label", str(form_data.get("category_label")))]
                    if form_data.get("category_label") not in (None, "")
                    else []
                ),
            ],
            files_payload=[
                # PR42: forward the unified `csv_files` upload first — the
                # backend auto-routes each by header signature.
                *[
                    (
                        "csv_files",
                        (
                            file.filename or "upload.csv",
                            await file.read(),
                            file.content_type or "text/csv",
                        ),
                    )
                    for file in csv_unified_files
                ],
                *[
                    (
                        "competitor_xray_csv",
                        (
                            file.filename or "competitors.csv",
                            await file.read(),
                            file.content_type or "text/csv",
                        ),
                    )
                    for file in competitor_files
                ],
                *[
                    (
                        "keyword_xray_csv",
                        (
                            file.filename or "keywords.csv",
                            await file.read(),
                            file.content_type or "text/csv",
                        ),
                    )
                    for file in keyword_files
                ],
                *(
                    [
                        (
                            "target_xray_csv",
                            (
                                target_xray_csv.filename or "target.csv",
                                target_xray_bytes,
                                target_xray_csv.content_type or "text/csv",
                            ),
                        )
                    ]
                    if target_xray_csv and target_xray_csv.filename and target_xray_bytes
                    else []
                ),
                *(
                    [
                        (
                            "cerebro_csv",
                            (
                                cerebro_csv.filename or "cerebro.csv",
                                cerebro_bytes,
                                cerebro_csv.content_type or "text/csv",
                            ),
                        )
                    ]
                    if cerebro_csv and cerebro_csv.filename and cerebro_bytes
                    else []
                ),
                *(
                    [
                        (
                            "word_frequency_csv",
                            (
                                word_frequency_csv.filename or "word-frequency.csv",
                                word_frequency_bytes,
                                word_frequency_csv.content_type or "text/csv",
                            ),
                        )
                    ]
                    if word_frequency_csv and word_frequency_csv.filename and word_frequency_bytes
                    else []
                ),
            ],
        )
    except Exception as exc:
        logger.exception("[AdminDashboard] deck generation failed")
        return JSONResponse(
            status_code=500,
            content={"detail": "Deck generation failed.", "error": str(exc)},
        )

    details = payload.get("details") if isinstance(payload, dict) else None
    if isinstance(details, dict):
        for key in ("edit_url", "view_url"):
            if key in details:
                details[key] = _rewrite_sales_support_url_for_agent(request, str(details.get(key) or ""))

    return JSONResponse(status_code=status_code, content=payload)


def _proxy_deck_subpath(
    request: Request,
    deck_slug: str,
    run_id: int,
    token: str,
    *,
    suffix: str = "",
) -> Response:
    """Forward a deck request to the backend sales-support-agent service.

    `suffix` is appended after `/decks/{slug}/{run_id}/{token}` — empty for
    the main deck view, "/story" for the HTML story viewer, "/story.md" for
    the raw markdown download. Same retry / error / passthrough behavior for
    all three so the public host doesn't need to know which sub-path the
    backend supports.
    """
    admin_settings = load_admin_dashboard_settings()
    if not admin_settings.sales_support_agent_url:
        return JSONResponse(
            status_code=500,
            content={"detail": "Sales support agent URL is not configured on this service."},
        )
    backend_url = (
        f"{admin_settings.sales_support_agent_url}"
        f"/decks/{quote(deck_slug, safe='')}/{run_id}/{quote(token, safe='')}"
        f"{suffix}"
    )
    if request.url.query:
        backend_url = f"{backend_url}?{request.url.query}"
    attempt_count = 1 + len(DECK_PROXY_RETRY_DELAYS_SECONDS)
    for attempt in range(1, attempt_count + 1):
        try:
            response = requests.get(backend_url, timeout=DECK_PROXY_TIMEOUT_SECONDS)
        except requests.RequestException as exc:
            logger.warning(
                "[DeckProxy] upstream request failed attempt=%s slug=%s run_id=%s suffix=%s url=%s error=%s",
                attempt,
                deck_slug,
                run_id,
                suffix or "(none)",
                backend_url,
                exc,
            )
            if attempt < attempt_count:
                time.sleep(DECK_PROXY_RETRY_DELAYS_SECONDS[attempt - 1])
                continue
            return _deck_proxy_error_response()

        if response.status_code in {502, 503, 504}:
            logger.warning(
                "[DeckProxy] upstream retryable status attempt=%s slug=%s run_id=%s suffix=%s url=%s status=%s",
                attempt,
                deck_slug,
                run_id,
                suffix or "(none)",
                backend_url,
                response.status_code,
            )
            if attempt < attempt_count:
                time.sleep(DECK_PROXY_RETRY_DELAYS_SECONDS[attempt - 1])
                continue
            return _deck_proxy_error_response()

        # Pass through the upstream's Content-Type — backend uses
        # text/html for /story, text/markdown for /story.md, etc.
        content_type = response.headers.get("Content-Type", "text/html; charset=utf-8")
        passthrough_headers = _deck_proxy_headers(content_type)
        # Preserve Content-Disposition (attachment filename) on /story.md
        # downloads so the browser triggers a save dialog.
        upstream_disposition = response.headers.get("Content-Disposition")
        if upstream_disposition:
            passthrough_headers["Content-Disposition"] = upstream_disposition
        return Response(
            content=response.content,
            status_code=response.status_code,
            media_type=content_type.split(";")[0],
            headers=passthrough_headers,
        )

    return _deck_proxy_error_response()


@app.get("/decks/{deck_slug}/{run_id}/{token}")
def public_deck_proxy(request: Request, deck_slug: str, run_id: int, token: str) -> Response:
    return _proxy_deck_subpath(request, deck_slug, run_id, token)


@app.get("/decks/{deck_slug}/{run_id}/{token}/story")
def public_deck_story_proxy(request: Request, deck_slug: str, run_id: int, token: str) -> Response:
    """HTML viewer for the markdown story companion (PR27/PR29)."""
    return _proxy_deck_subpath(request, deck_slug, run_id, token, suffix="/story")


@app.get("/decks/{deck_slug}/{run_id}/{token}/preview.png")
def public_deck_preview_proxy(request: Request, deck_slug: str, run_id: int, token: str) -> Response:
    return _proxy_deck_subpath(request, deck_slug, run_id, token, suffix="/preview.png")


@app.get("/decks/{deck_slug}/{run_id}/{token}/story.md")
def public_deck_story_md_proxy(request: Request, deck_slug: str, run_id: int, token: str) -> Response:
    """Raw markdown download — backend sets Content-Disposition: attachment."""
    return _proxy_deck_subpath(request, deck_slug, run_id, token, suffix="/story.md")


@app.post("/decks/{deck_slug}/{run_id}/{token}/heartbeat")
async def public_deck_heartbeat_proxy(
    request: Request,
    deck_slug: str,
    run_id: int,
    token: str,
) -> Response:
    """PR54: forward the deck-engagement heartbeat from the prospect's
    browser to the backend. Body passed through as-is (JSON). We add
    X-Forwarded-For and CF headers if present so the backend can capture
    the real visitor IP / country instead of seeing this proxy's IP."""
    admin_settings = load_admin_dashboard_settings()
    if not admin_settings.sales_support_agent_url:
        return JSONResponse(status_code=500, content={"detail": "Backend URL not configured."})
    body = await request.body()
    headers = {
        "Content-Type": request.headers.get("content-type", "application/json"),
        # Trust order matches the backend's _extract_client_ip helper.
        "X-Forwarded-For": (
            request.headers.get("cf-connecting-ip")
            or request.headers.get("x-forwarded-for")
            or (request.client.host if request.client else "")
            or ""
        ),
    }
    cf_country = request.headers.get("cf-ipcountry")
    if cf_country:
        headers["CF-IPCountry"] = cf_country
    cf_ip = request.headers.get("cf-connecting-ip")
    if cf_ip:
        headers["CF-Connecting-IP"] = cf_ip
    referer = request.headers.get("referer")
    if referer:
        headers["Referer"] = referer
    user_agent = request.headers.get("user-agent")
    if user_agent:
        headers["User-Agent"] = user_agent
    upstream_url = (
        f"{admin_settings.sales_support_agent_url}/decks/{deck_slug}/{run_id}/{token}/heartbeat"
    )
    try:
        upstream = requests.post(upstream_url, data=body, headers=headers, timeout=10)
    except requests.RequestException as exc:
        return JSONResponse(status_code=502, content={"detail": f"Backend unreachable: {exc}"})
    try:
        return JSONResponse(status_code=upstream.status_code, content=upstream.json())
    except ValueError:
        return Response(
            status_code=upstream.status_code,
            content=upstream.text,
            media_type="text/plain",
        )


@app.get("/", response_class=HTMLResponse)
def home(request: Request) -> HTMLResponse:
    """Human-readable service status; machine probes remain under /health/*."""

    return HTMLResponse(
        render_service_status_page(ready=bool(getattr(request.app.state, "ready", False)))
    )


@app.get("/api/status")
def api_status() -> dict[str, str]:
    return {"status": "agent running"}


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/health/live")
def health_live() -> dict[str, str]:
    """Process-only liveness probe with no database or external calls."""

    return {
        "status": "live",
        "render_git_commit": RENDER_GIT_COMMIT or "unknown",
    }


@app.get("/health/ready")
def health_ready(request: Request) -> JSONResponse:
    """Fail closed until startup completes and PostgreSQL answers one query."""

    commit = RENDER_GIT_COMMIT or "unknown"
    if not bool(getattr(request.app.state, "ready", False)):
        return JSONResponse(
            {
                "status": "not_ready",
                "render_git_commit": commit,
                "reason": "application_initializing",
            },
            status_code=503,
        )
    try:
        from sqlalchemy import text as sql_text

        with request.app.state.session_factory() as session:
            session.execute(sql_text("SELECT 1"))
    except Exception:  # noqa: BLE001
        logger.exception("readiness status=failed commit=%s", commit)
        return JSONResponse(
            {
                "status": "not_ready",
                "render_git_commit": commit,
                "reason": "database_unavailable",
            },
            status_code=503,
        )
    return JSONResponse(
        {"status": "ready", "render_git_commit": commit}
    )


@app.get("/health/storage")
def health_storage(request: Request) -> JSONResponse:
    """Non-sensitive proof that Website Ops files are durable in PostgreSQL."""

    from sales_support_agent.services.website_ops_storage import (
        database_mirror_enabled,
        website_ops_storage_status,
    )

    if not database_mirror_enabled():
        return JSONResponse(
            {"status": "disabled", "files": 0, "bytes": 0}
        )
    try:
        payload = website_ops_storage_status(
            request.app.state.session_factory.kw["bind"]
        )
    except Exception:  # noqa: BLE001
        return JSONResponse(
            {"status": "unavailable", "files": 0, "bytes": 0},
            status_code=503,
        )
    return JSONResponse({"status": "ready", **payload})
