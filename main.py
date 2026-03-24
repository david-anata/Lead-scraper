import csv
import io
import json
import logging
import math
import os
import re
import time
import unicodedata
from concurrent.futures import Future, ThreadPoolExecutor
from urllib.parse import parse_qs, quote, urlparse
from base64 import b64decode, b64encode
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response, StreamingResponse
from pydantic import BaseModel, Field

from sales_support_agent.services.admin_auth import (
    admin_login_enabled,
    create_admin_session_token,
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
)
from sales_support_agent.services.revenue_ops import (
    append_daily_import_count_db,
    append_processed_domains_db,
    append_processed_heyreach_leads_db,
    complete_lead_run,
    create_lead_run,
    fail_lead_run,
    get_lead_run,
    get_lead_run_csv,
    load_apollo_attempts_db,
    load_daily_import_counts_db,
    load_processed_domains_db,
    load_processed_heyreach_leads_db,
    load_source_cursor_db,
    mark_lead_run_started,
    record_lead_run_item,
    save_source_cursor_db,
    update_lead_run_stage,
    upsert_apollo_attempts_db,
    upsert_lead_rows,
)


logger = logging.getLogger(__name__)

app = FastAPI()
LEAD_RUN_EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix="lead-build")
ACTIVE_LEAD_RUNS: dict[str, Future[Any]] = {}


# ========= EXTERNAL ENDPOINTS =========
APOLLO_ORG_SEARCH_URL = "https://api.apollo.io/api/v1/mixed_companies/search"
APOLLO_PEOPLE_SEARCH_URL = "https://api.apollo.io/api/v1/mixed_people/api_search"
APOLLO_BULK_PEOPLE_MATCH_URL = "https://api.apollo.io/api/v1/people/bulk_match"
SLACK_CHAT_POST_MESSAGE_URL = "https://slack.com/api/chat.postMessage"
SLACK_GET_UPLOAD_URL = "https://slack.com/api/files.getUploadURLExternal"
SLACK_COMPLETE_UPLOAD_URL = "https://slack.com/api/files.completeUploadExternal"
INSTANTLY_ADD_LEADS_URL = "https://api.instantly.ai/api/v2/leads/add"
HEYREACH_CHECK_API_KEY_URL = "https://api.heyreach.io/api/public/auth/CheckApiKey"
HEYREACH_ADD_LEADS_TO_CAMPAIGN_URL = os.getenv(
    "HEYREACH_ADD_LEADS_TO_CAMPAIGN_URL",
    "https://api.heyreach.io/api/public/campaign/AddLeadsToCampaignV2",
).strip()


# ========= RUNTIME CONFIG =========
REQUEST_TIMEOUT_SECONDS = 60
ADMIN_REMOTE_TIMEOUT_SECONDS = int((os.getenv("ADMIN_REMOTE_TIMEOUT_SECONDS", "8") or "8").strip())
MAX_APOLLO_ORG_PAGES = int((os.getenv("MAX_APOLLO_ORG_PAGES", "25") or "25").strip())
APOLLO_ORG_PAGE_SIZE = min(int((os.getenv("APOLLO_ORG_PAGE_SIZE", "100") or "100").strip()), 100)
MAX_APOLLO_DOMAINS_PER_RUN = int((os.getenv("MAX_APOLLO_DOMAINS_PER_RUN", "60") or "60").strip())
APOLLO_MIN_DOMAINS_PER_RUN = int((os.getenv("APOLLO_MIN_DOMAINS_PER_RUN", "20") or "20").strip())
APOLLO_DOMAINS_PER_TARGET_LEAD = float((os.getenv("APOLLO_DOMAINS_PER_TARGET_LEAD", "3.0") or "3.0").strip())
APOLLO_MIN_ORG_PAGES_PER_RUN = int((os.getenv("APOLLO_MIN_ORG_PAGES_PER_RUN", "4") or "4").strip())
APOLLO_ORG_PAGES_PER_REQUESTED_PAGE = int((os.getenv("APOLLO_ORG_PAGES_PER_REQUESTED_PAGE", "4") or "4").strip())
APOLLO_SLEEP_SECONDS = 1.2
MAX_CONTACTS_PER_DOMAIN = 2
APOLLO_SEARCH_CANDIDATES_PER_DOMAIN = 10
APOLLO_ATTEMPT_COOLDOWN_DAYS = int((os.getenv("APOLLO_ATTEMPT_COOLDOWN_DAYS", "60") or "60").strip())
TARGET_ACCEPTED_LEADS_PER_RUN = int((os.getenv("TARGET_ACCEPTED_LEADS_PER_RUN", "0") or "0").strip())

GENERIC_EMAIL_PREFIXES = (
    "info@",
    "support@",
    "hello@",
    "contact@",
    "admin@",
    "team@",
    "sales@",
    "marketing@",
    "office@",
    "care@",
    "service@",
    "help@",
    "noreply@",
    "no-reply@",
)

TARGET_CAMPAIGN_NAME = "Amazon | DTC Brands | Performance Marketing | Mar 2026"
APOLLO_TARGET_TITLES = (
    "founder",
    "co-founder",
    "owner",
    "ceo",
    "chief executive officer",
    "chief operating officer",
    "coo",
    "chief of staff",
    "president",
    "founding team",
    "founding member",
    "head of operations",
    "head of ecommerce",
    "head of e-commerce",
    "head of growth",
    "head of brand",
    "vp of ecommerce",
    "vp of e-commerce",
    "vp of operations",
    "director of ecommerce",
    "director of e-commerce",
    "director of operations",
    "ecommerce manager",
    "e-commerce manager",
    "operations manager",
    "general manager",
    "operator",
    "ecommerce",
    "e-commerce",
)
APOLLO_TARGET_SENIORITIES = (
    "c_suite",
    "vp",
    "head",
    "director",
    "manager",
)
APOLLO_DEBUG_RAW = os.getenv("APOLLO_DEBUG_RAW", "").strip().lower() in {"1", "true", "yes", "on"}
APP_VERSION = os.getenv("APP_VERSION", "apollo-org-sourcing-v1")
RENDER_GIT_COMMIT = os.getenv("RENDER_GIT_COMMIT", "").strip()
RENDER_GIT_BRANCH = os.getenv("RENDER_GIT_BRANCH", "").strip()
PROCESSED_DOMAINS_FILE = os.getenv("PROCESSED_DOMAINS_FILE", "processed_domains.csv").strip()
DAILY_IMPORT_LOG_FILE = os.getenv("DAILY_IMPORT_LOG_FILE", "daily_import_counts.csv").strip()
DAILY_NEW_LEAD_LIMIT = int((os.getenv("DAILY_NEW_LEAD_LIMIT", "0") or "0").strip() or 0)
ENABLE_WEEKDAY_ONLY_IMPORTS = os.getenv("ENABLE_WEEKDAY_ONLY_IMPORTS", "").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
STATE_BACKEND = os.getenv("STATE_BACKEND", "").strip().lower() or "local"
GITHUB_STATE_TOKEN = os.getenv("GITHUB_STATE_TOKEN", "").strip()
GITHUB_STATE_REPO = os.getenv("GITHUB_STATE_REPO", "").strip()
GITHUB_STATE_BRANCH = os.getenv("GITHUB_STATE_BRANCH", "state").strip() or "state"
GITHUB_STATE_BASE_BRANCH = os.getenv("GITHUB_STATE_BASE_BRANCH", "main").strip() or "main"
GITHUB_STATE_PROCESSED_DOMAINS_PATH = (
    os.getenv("GITHUB_STATE_PROCESSED_DOMAINS_PATH", "state/processed_domains.csv").strip()
    or "state/processed_domains.csv"
)
GITHUB_STATE_DAILY_IMPORTS_PATH = (
    os.getenv("GITHUB_STATE_DAILY_IMPORTS_PATH", "state/daily_import_counts.csv").strip()
    or "state/daily_import_counts.csv"
)
APOLLO_ATTEMPTS_FILE = os.getenv("APOLLO_ATTEMPTS_FILE", "apollo_attempts.csv").strip()
APOLLO_ORG_CURSOR_FILE = (
    os.getenv("APOLLO_ORG_CURSOR_FILE")
    or os.getenv("STORELEADS_CURSOR_FILE")
    or "apollo_org_cursor.csv"
).strip()
GITHUB_STATE_APOLLO_ATTEMPTS_PATH = (
    os.getenv("GITHUB_STATE_APOLLO_ATTEMPTS_PATH", "state/apollo_attempts.csv").strip()
    or "state/apollo_attempts.csv"
)
GITHUB_STATE_APOLLO_ORG_CURSOR_PATH = (
    os.getenv("GITHUB_STATE_APOLLO_ORG_CURSOR_PATH")
    or os.getenv("GITHUB_STATE_STORELEADS_CURSOR_PATH")
    or "state/apollo_org_cursor.csv"
).strip()
INSTANTLY_CAMPAIGN_ROUTING_JSON = os.getenv("INSTANTLY_CAMPAIGN_ROUTING_JSON", "").strip()
APOLLO_MIN_EMPLOYEE_COUNT = int((os.getenv("APOLLO_MIN_EMPLOYEE_COUNT", "2") or "2").strip())
APOLLO_MAX_EMPLOYEE_COUNT = int((os.getenv("APOLLO_MAX_EMPLOYEE_COUNT", "80") or "80").strip())
APOLLO_MAX_ANNUAL_REVENUE = int((os.getenv("APOLLO_MAX_ANNUAL_REVENUE", "15000000") or "15000000").strip())
APOLLO_ALLOWED_COUNTRIES = {"US", "GB", "UK", "CA", "AU", "UNITED STATES", "UNITED KINGDOM", "CANADA", "AUSTRALIA"}
APOLLO_EXCLUDED_KEYWORDS = (
    "agency",
    "consulting",
    "consultancy",
    "software",
    "saas",
    "printing",
    "publisher",
    "publishing",
    "industrial",
    "construction",
    "pharmaceutical",
    "church",
    "politics",
    "charity",
    "hotel",
    "consultant",
    "services",
    "service",
    "enterprise",
    "global",
    "wholesale",
    "distributor",
    "b2b",
    "manufacturer",
    "private label",
    "amazon services",
    "marketplace services",
    "mail delivery",
    "public relations",
    "printful",
    "printify",
    "etsy seller",
    "amazon seller services",
    "dropshipping",
    "drop shipping",
)
APOLLO_ECOMMERCE_INCLUDE_KEYWORDS = (
    "ecommerce",
    "e-commerce",
    "shopify",
    "woocommerce",
    "bigcommerce",
    "magento",
    "direct to consumer",
    "dtc",
    "consumer goods",
    "retail",
    "apparel",
    "fashion",
    "beauty",
    "cosmetics",
    "skincare",
    "food",
    "beverage",
    "snack",
    "supplement",
    "pet",
    "jewelry",
    "home goods",
    "home decor",
    "furniture",
    "baby",
    "gift",
    "wellness",
    "sporting goods",
    "toys",
)
ORG_CATEGORY_KEYWORDS: dict[str, tuple[str, ...]] = {
    "beauty_wellness": ("beauty", "skincare", "cosmetic", "wellness", "supplement", "personal care"),
    "food_beverage": ("food", "beverage", "drink", "snack", "coffee", "tea"),
    "apparel_accessories": ("apparel", "fashion", "clothing", "footwear", "jewelry", "accessories"),
    "home_lifestyle": ("home", "furniture", "decor", "lifestyle", "kitchen", "bedding"),
    "pets": ("pet", "pets", "animal"),
    "family_gifts": ("baby", "kids", "toys", "gift", "stationery"),
}
APOLLO_CATEGORY_SEARCH_KEYWORDS: dict[str, tuple[str, ...]] = {
    "beauty_wellness": ("beauty", "skincare", "cosmetics", "wellness", "supplement", "personal care"),
    "food_beverage": ("food", "beverage", "snack", "coffee", "tea"),
    "apparel_accessories": ("apparel", "fashion", "clothing", "jewelry", "accessories"),
    "home_lifestyle": ("home goods", "home decor", "furniture", "kitchen", "bedding"),
    "pets": ("pet", "pets"),
    "family_gifts": ("baby", "kids", "toys", "gift", "stationery"),
}
GITHUB_STATE_STORELEADS_CURSOR_PATH = GITHUB_STATE_APOLLO_ORG_CURSOR_PATH
STORELEADS_CURSOR_FILE = APOLLO_ORG_CURSOR_FILE
HEYREACH_PROCESSED_LEADS_FILE = os.getenv("HEYREACH_PROCESSED_LEADS_FILE", "heyreach_processed_leads.csv").strip()
GITHUB_STATE_HEYREACH_LEADS_PATH = (
    os.getenv("GITHUB_STATE_HEYREACH_LEADS_PATH", "state/heyreach_processed_leads.csv").strip()
    or "state/heyreach_processed_leads.csv"
)
LEAD_ENGINE_USE_DB_STATE = os.getenv("LEAD_ENGINE_USE_DB_STATE", "true").strip().lower() in {"1", "true", "yes", "on"}
LEAD_ENGINE_DB_CONFIGURED = bool(
    os.getenv("LEAD_ENGINE_DB_URL", "").strip() or os.getenv("SALES_AGENT_DB_URL", "").strip()
)


# ========= REQUEST / SETTINGS MODELS =========
class ICPBuildRequest(BaseModel):
    date: str
    max_domains: int = Field(default=150)


@dataclass(frozen=True)
class Settings:
    apollo_api_key: str
    slack_bot_token: str
    slack_channel_id: str
    instantly_campaign_id: str
    instantly_api_key: str
    heyreach_api_key: str
    heyreach_campaign_id: str


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
class LeadBuildExecutionResult:
    instantly_csv: str
    instantly_rows: list[dict[str, Any]]
    raw_scanned: int
    qualified_domains_count: int
    qualified_matches_total: int
    previously_processed_domains: int
    skipped_apollo_cooldown_domains: int
    storeleads_start_page: int
    storeleads_end_page: int
    storeleads_pages_scanned: int
    apollo_domains_queried: int
    accepted_lead_target: int
    apollo_hits: int
    successful_contacts: int
    scheduler_status: dict[str, Any]
    instantly_import_result: dict[str, Any]
    heyreach_import_result: dict[str, Any]


@dataclass(frozen=True)
class StoreLeadsCollectionResult:
    domains: list[dict[str, Any]]
    raw_scanned: int
    qualified_matches_total: int
    skipped_processed_domains: int
    skipped_apollo_cooldown_domains: int
    start_page: int
    end_page: int
    pages_scanned: int
    next_page: int


@dataclass(frozen=True)
class LeadBuildRowsResult:
    instantly_rows: list[dict[str, Any]]
    linkedin_rows: list[dict[str, Any]]
    successful_contacts: int
    apollo_hits: int
    apollo_domains_attempted: int
    apollo_attempt_rows: list[dict[str, str]]


def detect_scheduler_source(request: Request) -> str:
    query_source = (request.query_params.get("scheduler_source") or "").strip().lower()
    if query_source:
        return query_source

    header_source = (request.headers.get("X-Scheduler-Source") or "").strip().lower()
    if header_source:
        return header_source

    return "manual"


# ========= CONFIGURATION =========
def configure_logging() -> None:
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO)


def load_settings() -> Settings:
    return Settings(
        apollo_api_key=os.getenv("APOLLO_API_KEY", "").strip(),
        slack_bot_token=os.getenv("SLACK_BOT_TOKEN", "").strip(),
        slack_channel_id=os.getenv("SLACK_CHANNEL_ID", "").strip(),
        instantly_campaign_id=os.getenv("INSTANTLY_CAMPAIGN_ID", "").strip(),
        instantly_api_key=(os.getenv("INSTANTLY_API_KEY") or os.getenv("INSTANTLY_AI") or "").strip(),
        heyreach_api_key=os.getenv("HEYREACH_API_KEY", "").strip(),
        heyreach_campaign_id=os.getenv("HEYREACH_CAMPAIGN_ID", "").strip(),
    )


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


def get_missing_required_settings(settings: Settings) -> list[str]:
    missing: list[str] = []

    if not settings.apollo_api_key:
        missing.append("APOLLO_API_KEY")
    if not settings.slack_bot_token:
        missing.append("SLACK_BOT_TOKEN")
    if not settings.slack_channel_id:
        missing.append("SLACK_CHANNEL_ID")

    return missing


def build_missing_settings_message(missing: list[str]) -> str:
    return (
        "Missing required environment variables: "
        + ", ".join(missing)
        + ". Set them before starting the API."
    )


def validate_required_settings(settings: Settings) -> None:
    missing = get_missing_required_settings(settings)
    if missing:
        raise HTTPException(status_code=500, detail=build_missing_settings_message(missing))


def validate_settings_on_startup(settings: Settings) -> None:
    missing = get_missing_required_settings(settings)
    if missing:
        message = build_missing_settings_message(missing)
        logger.error(message)
        raise RuntimeError(message)


@app.on_event("startup")
def startup() -> None:
    configure_logging()
    settings = load_settings()
    app.state.settings = settings
    validate_settings_on_startup(settings)
    logger.info(
        "[Startup] app_version=%s render_git_branch=%s render_git_commit=%s apollo_mode=org_search_plus_people_search state_backend=%s github_state_repo=%s github_state_branch=%s",
        APP_VERSION,
        RENDER_GIT_BRANCH or "unknown",
        RENDER_GIT_COMMIT or "unknown",
        STATE_BACKEND,
        GITHUB_STATE_REPO or "n/a",
        GITHUB_STATE_BRANCH,
    )


def _admin_cookie_options(request: Request, admin_settings: AdminDashboardSettings) -> dict[str, Any]:
    return {
        "key": admin_settings.admin_cookie_name,
        "httponly": True,
        "secure": request.url.scheme == "https",
        "samesite": "lax",
        "max_age": admin_settings.admin_session_ttl_hours * 3600,
        "path": "/",
    }


def _build_empty_dashboard(*, lead_builder_missing: list[str], error_message: str = "") -> DashboardData:
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
        lead_builder_ready=not lead_builder_missing,
        lead_builder_missing=lead_builder_missing,
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
    now: datetime | None = None,
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
    latest_sync_at: datetime | None,
    admin_settings: AdminDashboardSettings,
    *,
    now: datetime | None = None,
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
    lead_builder_missing = get_missing_required_settings(load_settings())
    if not admin_settings.sales_support_agent_url or not admin_settings.sales_agent_internal_api_key:
        return _build_empty_dashboard(
            lead_builder_missing=lead_builder_missing,
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
        dashboard = dashboard_data_from_dict(details)
        return replace(
            dashboard,
            lead_builder_ready=not lead_builder_missing,
            lead_builder_missing=lead_builder_missing,
        )
    except Exception as exc:
        logger.exception("[AdminDashboard] remote data fetch failed")
        return _build_empty_dashboard(
            lead_builder_missing=lead_builder_missing,
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
    files_payload: list[tuple[str, tuple[str, bytes, str]]] | None = None,
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


def sync_remote_dashboard_sources() -> dict[str, Any]:
    clickup_sync = _post_sales_support_job(
        "/api/clickup/sync",
        {"include_closed": True},
    )
    stale_scan = _post_sales_support_job(
        "/api/jobs/stale-leads/run",
        {"dry_run": True},
    )
    clickup_details = clickup_sync.get("details", clickup_sync)
    stale_details = stale_scan.get("details", stale_scan)
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


def should_run_auto_dashboard_sync(request: Request, dashboard: DashboardData, admin_settings: AdminDashboardSettings) -> bool:
    if not latest_sync_is_stale(dashboard.latest_sync_at, admin_settings):
        return False

    last_attempt = getattr(request.app.state, "admin_dashboard_last_auto_sync_at", None)
    if isinstance(last_attempt, datetime):
        current_time = datetime.now(timezone.utc)
        if last_attempt.tzinfo is None:
            last_attempt = last_attempt.replace(tzinfo=timezone.utc)
        if current_time - last_attempt < timedelta(minutes=5):
            return False

    return True


# ========= GENERAL HELPERS =========
def normalize_domain(domain: str) -> str:
    return (
        str(domain or "")
        .replace("https://", "")
        .replace("http://", "")
        .replace("www.", "")
        .strip("/")
        .lower()
    )


def parse_monthly_sales(store: dict[str, Any]) -> float | None:
    monthly_candidates = (
        "estimated_sales",
        "estimated_monthly_revenue",
        "monthly_revenue",
    )
    for field_name in monthly_candidates:
        try:
            value = store.get(field_name)
            if value is None:
                continue
            return float(value)
        except (TypeError, ValueError):
            continue

    annual_candidates = (
        "estimated_annual_revenue",
        "annual_revenue",
        "organization_estimated_annual_revenue",
        "organization_annual_revenue",
    )
    for field_name in annual_candidates:
        try:
            value = store.get(field_name)
            if value is None:
                continue
            return float(value) / 12.0
        except (TypeError, ValueError):
            continue

    return None


def parse_average_product_price_usd(store: dict[str, Any]) -> float | None:
    for field_name in ("avg_price_usd", "average_product_price_usd", "avgppusd"):
        try:
            value = store.get(field_name)
            if value is None:
                continue
            parsed_value = float(value)
            # StoreLeads price values are in minor currency units.
            return parsed_value / 100.0
        except (TypeError, ValueError):
            continue
    return None


def split_full_name(full_name: str) -> tuple[str, str]:
    name_parts = full_name.split()
    first_name = name_parts[0] if name_parts else ""
    last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""
    return first_name, last_name


def rows_to_csv(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return ""

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


def github_state_enabled() -> bool:
    return STATE_BACKEND == "github" and bool(GITHUB_STATE_TOKEN and GITHUB_STATE_REPO)


def github_api_headers() -> dict[str, str]:
    return {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {GITHUB_STATE_TOKEN}",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def ensure_github_state_branch() -> None:
    ref_url = f"https://api.github.com/repos/{GITHUB_STATE_REPO}/git/ref/heads/{GITHUB_STATE_BRANCH}"
    response = requests.get(ref_url, headers=github_api_headers(), timeout=REQUEST_TIMEOUT_SECONDS)
    if response.status_code == 200:
        return
    if response.status_code != 404:
        response.raise_for_status()

    base_ref_url = f"https://api.github.com/repos/{GITHUB_STATE_REPO}/git/ref/heads/{GITHUB_STATE_BASE_BRANCH}"
    base_response = requests.get(base_ref_url, headers=github_api_headers(), timeout=REQUEST_TIMEOUT_SECONDS)
    base_response.raise_for_status()
    base_sha = ((base_response.json() or {}).get("object") or {}).get("sha")
    if not base_sha:
        raise HTTPException(status_code=500, detail="GitHub state branch creation failed: missing base branch SHA")

    create_response = requests.post(
        f"https://api.github.com/repos/{GITHUB_STATE_REPO}/git/refs",
        headers=github_api_headers(),
        json={"ref": f"refs/heads/{GITHUB_STATE_BRANCH}", "sha": base_sha},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    if create_response.status_code not in (200, 201, 422):
        create_response.raise_for_status()


def load_github_state_file(path: str) -> tuple[str, str | None]:
    ensure_github_state_branch()
    response = requests.get(
        f"https://api.github.com/repos/{GITHUB_STATE_REPO}/contents/{path}",
        headers=github_api_headers(),
        params={"ref": GITHUB_STATE_BRANCH},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    if response.status_code == 404:
        return "", None

    response.raise_for_status()
    payload = response.json() or {}
    encoded_content = (payload.get("content") or "").replace("\n", "")
    decoded_content = b64decode(encoded_content).decode("utf-8") if encoded_content else ""
    return decoded_content, payload.get("sha")


def write_github_state_file(path: str, content: str, message: str) -> None:
    ensure_github_state_branch()
    _, current_sha = load_github_state_file(path)
    response = requests.put(
        f"https://api.github.com/repos/{GITHUB_STATE_REPO}/contents/{path}",
        headers=github_api_headers(),
        json={
            "message": message,
            "content": b64encode(content.encode("utf-8")).decode("utf-8"),
            "branch": GITHUB_STATE_BRANCH,
            **({"sha": current_sha} if current_sha else {}),
        },
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()


def parse_csv_text(content: str) -> list[dict[str, str]]:
    if not content.strip():
        return []

    buffer = io.StringIO(content)
    return [dict(row) for row in csv.DictReader(buffer)]


def write_csv_text(rows: list[dict[str, Any]], fieldnames: list[str]) -> str:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


def current_utc_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def parse_iso_datetime(value: str) -> datetime | None:
    normalized = (value or "").strip()
    if not normalized:
        return None

    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"

    try:
        parsed_value = datetime.fromisoformat(normalized)
    except ValueError:
        return None

    if parsed_value.tzinfo is None:
        parsed_value = parsed_value.replace(tzinfo=timezone.utc)

    return parsed_value.astimezone(timezone.utc)


def processed_domains_path() -> Path:
    configured_path = Path(PROCESSED_DOMAINS_FILE)
    if configured_path.is_absolute():
        return configured_path
    return Path(__file__).resolve().parent / configured_path


def apollo_attempts_path() -> Path:
    configured_path = Path(APOLLO_ATTEMPTS_FILE)
    if configured_path.is_absolute():
        return configured_path
    return Path(__file__).resolve().parent / configured_path


def apollo_org_cursor_path() -> Path:
    configured_path = Path(APOLLO_ORG_CURSOR_FILE)
    if configured_path.is_absolute():
        return configured_path
    return Path(__file__).resolve().parent / configured_path


def db_state_enabled() -> bool:
    return LEAD_ENGINE_USE_DB_STATE and LEAD_ENGINE_DB_CONFIGURED


def _legacy_load_processed_domains() -> set[str]:
    if github_state_enabled():
        try:
            content, _ = load_github_state_file(GITHUB_STATE_PROCESSED_DOMAINS_PATH)
        except requests.RequestException as exc:
            raise HTTPException(status_code=500, detail=f"GitHub processed-domain state read failed: {exc}") from exc

        processed_domains = {
            normalize_domain((row or {}).get("domain", ""))
            for row in parse_csv_text(content)
            if normalize_domain((row or {}).get("domain", ""))
        }
        logger.info("[State] backend=github processed_domains=%s", len(processed_domains))
        return processed_domains

    path = processed_domains_path()
    if not path.exists():
        return set()

    processed_domains: set[str] = set()
    with path.open("r", encoding="utf-8", newline="") as file_obj:
        reader = csv.DictReader(file_obj)
        for row in reader:
            domain = normalize_domain((row or {}).get("domain", ""))
            if domain:
                processed_domains.add(domain)

    return processed_domains


def load_processed_domains() -> set[str]:
    if db_state_enabled():
        processed_domains = load_processed_domains_db()
        if processed_domains:
            logger.info("[State] backend=db processed_domains=%s", len(processed_domains))
            return processed_domains
        legacy_domains = _legacy_load_processed_domains()
        if legacy_domains:
            append_processed_domains_db(legacy_domains, "legacy-import")
            logger.info("[State] backend=db migrated_processed_domains=%s", len(legacy_domains))
        return legacy_domains
    return _legacy_load_processed_domains()


def _legacy_append_processed_domains(domains: set[str], run_date: str) -> None:
    if not domains:
        return

    if github_state_enabled():
        try:
            content, _ = load_github_state_file(GITHUB_STATE_PROCESSED_DOMAINS_PATH)
            existing_rows = parse_csv_text(content)
            existing_domains = {
                normalize_domain((row or {}).get("domain", ""))
                for row in existing_rows
                if normalize_domain((row or {}).get("domain", ""))
            }
            new_rows = list(existing_rows)
            for domain in sorted(domains):
                normalized_domain = normalize_domain(domain)
                if normalized_domain and normalized_domain not in existing_domains:
                    new_rows.append({"domain": normalized_domain, "date_added": run_date})
                    existing_domains.add(normalized_domain)

            write_github_state_file(
                GITHUB_STATE_PROCESSED_DOMAINS_PATH,
                write_csv_text(new_rows, ["domain", "date_added"]),
                f"Update processed domains for {run_date}",
            )
            logger.info("[State] backend=github appended_processed_domains=%s", len(domains))
            return
        except requests.RequestException as exc:
            raise HTTPException(status_code=500, detail=f"GitHub processed-domain state write failed: {exc}") from exc

    path = processed_domains_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = path.exists()

    with path.open("a", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=["domain", "date_added"])
        if not file_exists:
            writer.writeheader()

        for domain in sorted(domains):
            writer.writerow({"domain": domain, "date_added": run_date})


def append_processed_domains(domains: set[str], run_date: str) -> None:
    if not domains:
        return
    if db_state_enabled():
        append_processed_domains_db(domains, run_date)
        logger.info("[State] backend=db appended_processed_domains=%s", len(domains))
        return
    _legacy_append_processed_domains(domains, run_date)


def _legacy_load_apollo_attempts() -> dict[str, dict[str, str]]:
    if github_state_enabled():
        try:
            content, _ = load_github_state_file(GITHUB_STATE_APOLLO_ATTEMPTS_PATH)
        except requests.RequestException as exc:
            raise HTTPException(status_code=500, detail=f"GitHub Apollo state read failed: {exc}") from exc

        attempts = {
            normalize_domain((row or {}).get("domain", "")): dict(row)
            for row in parse_csv_text(content)
            if normalize_domain((row or {}).get("domain", ""))
        }
        logger.info("[State] backend=github apollo_attempt_domains=%s", len(attempts))
        return attempts

    path = apollo_attempts_path()
    if not path.exists():
        return {}

    attempts: dict[str, dict[str, str]] = {}
    with path.open("r", encoding="utf-8", newline="") as file_obj:
        reader = csv.DictReader(file_obj)
        for row in reader:
            domain = normalize_domain((row or {}).get("domain", ""))
            if domain:
                attempts[domain] = dict(row or {})

    return attempts


def load_apollo_attempts() -> dict[str, dict[str, str]]:
    if db_state_enabled():
        attempts = load_apollo_attempts_db()
        if attempts:
            logger.info("[State] backend=db apollo_attempt_domains=%s", len(attempts))
            return attempts
        legacy_attempts = _legacy_load_apollo_attempts()
        if legacy_attempts:
            upsert_apollo_attempts_db(list(legacy_attempts.values()))
            logger.info("[State] backend=db migrated_apollo_attempts=%s", len(legacy_attempts))
        return legacy_attempts
    return _legacy_load_apollo_attempts()


def _legacy_upsert_apollo_attempts(attempt_rows: list[dict[str, str]]) -> None:
    if not attempt_rows:
        return

    fieldnames = ["domain", "last_attempted_at", "result", "cooldown_until"]

    if github_state_enabled():
        try:
            content, _ = load_github_state_file(GITHUB_STATE_APOLLO_ATTEMPTS_PATH)
            existing_rows = parse_csv_text(content)
            attempts_by_domain = {
                normalize_domain((row or {}).get("domain", "")): dict(row)
                for row in existing_rows
                if normalize_domain((row or {}).get("domain", ""))
            }
            for row in attempt_rows:
                domain = normalize_domain(row.get("domain", ""))
                if not domain:
                    continue
                attempts_by_domain[domain] = {
                    "domain": domain,
                    "last_attempted_at": row.get("last_attempted_at", ""),
                    "result": row.get("result", ""),
                    "cooldown_until": row.get("cooldown_until", ""),
                }

            ordered_rows = [attempts_by_domain[domain] for domain in sorted(attempts_by_domain)]
            write_github_state_file(
                GITHUB_STATE_APOLLO_ATTEMPTS_PATH,
                write_csv_text(ordered_rows, fieldnames),
                "Update Apollo attempts state",
            )
            logger.info("[State] backend=github upserted_apollo_attempts=%s", len(attempt_rows))
            return
        except requests.RequestException as exc:
            raise HTTPException(status_code=500, detail=f"GitHub Apollo state write failed: {exc}") from exc

    path = apollo_attempts_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    attempts_by_domain = load_apollo_attempts()
    for row in attempt_rows:
        domain = normalize_domain(row.get("domain", ""))
        if not domain:
            continue
        attempts_by_domain[domain] = {
            "domain": domain,
            "last_attempted_at": row.get("last_attempted_at", ""),
            "result": row.get("result", ""),
            "cooldown_until": row.get("cooldown_until", ""),
        }

    ordered_rows = [attempts_by_domain[domain] for domain in sorted(attempts_by_domain)]
    path.write_text(write_csv_text(ordered_rows, fieldnames), encoding="utf-8")


def upsert_apollo_attempts(attempt_rows: list[dict[str, str]]) -> None:
    if not attempt_rows:
        return
    if db_state_enabled():
        upsert_apollo_attempts_db(attempt_rows)
        logger.info("[State] backend=db upserted_apollo_attempts=%s", len(attempt_rows))
        return
    _legacy_upsert_apollo_attempts(attempt_rows)


def _legacy_load_apollo_org_cursor() -> int:
    default_page = 1

    if github_state_enabled():
        try:
            content, _ = load_github_state_file(GITHUB_STATE_APOLLO_ORG_CURSOR_PATH)
        except requests.RequestException as exc:
            raise HTTPException(status_code=500, detail=f"GitHub Apollo org cursor read failed: {exc}") from exc

        rows = parse_csv_text(content)
        if not rows:
            return default_page
        try:
            next_page = int((rows[0] or {}).get("next_page", default_page) or default_page)
        except (TypeError, ValueError):
            next_page = default_page
        next_page = min(max(next_page, 1), MAX_APOLLO_ORG_PAGES)
        logger.info("[State] backend=github apollo_org_next_page=%s", next_page)
        return next_page

    path = apollo_org_cursor_path()
    if not path.exists():
        return default_page

    with path.open("r", encoding="utf-8", newline="") as file_obj:
        rows = list(csv.DictReader(file_obj))
    if not rows:
        return default_page
    try:
        next_page = int((rows[0] or {}).get("next_page", default_page) or default_page)
    except (TypeError, ValueError):
        next_page = default_page
    return min(max(next_page, 1), MAX_APOLLO_ORG_PAGES)


def load_apollo_org_cursor() -> int:
    default_page = 1
    if db_state_enabled():
        next_page = load_source_cursor_db("apollo_org_search", default_page)
        if next_page != default_page:
            logger.info("[State] backend=db apollo_org_next_page=%s", next_page)
            return min(max(next_page, 1), MAX_APOLLO_ORG_PAGES)
        legacy_page = _legacy_load_apollo_org_cursor()
        if legacy_page != default_page:
            save_source_cursor_db("apollo_org_search", legacy_page, {"migrated_from": "legacy"})
            logger.info("[State] backend=db migrated_apollo_org_next_page=%s", legacy_page)
        return min(max(legacy_page, 1), MAX_APOLLO_ORG_PAGES)
    return _legacy_load_apollo_org_cursor()


def _legacy_save_apollo_org_cursor(next_page: int) -> None:
    normalized_next_page = min(max(next_page, 1), MAX_APOLLO_ORG_PAGES)
    rows = [{"next_page": normalized_next_page, "last_updated": current_utc_timestamp()}]
    fieldnames = ["next_page", "last_updated"]

    if github_state_enabled():
        try:
            write_github_state_file(
                GITHUB_STATE_APOLLO_ORG_CURSOR_PATH,
                write_csv_text(rows, fieldnames),
                "Update Apollo org cursor",
            )
            logger.info("[State] backend=github saved_apollo_org_next_page=%s", normalized_next_page)
            return
        except requests.RequestException as exc:
            raise HTTPException(status_code=500, detail=f"GitHub Apollo org cursor write failed: {exc}") from exc

    path = apollo_org_cursor_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(write_csv_text(rows, fieldnames), encoding="utf-8")


def save_apollo_org_cursor(next_page: int) -> None:
    normalized_next_page = min(max(next_page, 1), MAX_APOLLO_ORG_PAGES)
    if db_state_enabled():
        save_source_cursor_db(
            "apollo_org_search",
            normalized_next_page,
            {"last_updated": current_utc_timestamp()},
        )
        logger.info("[State] backend=db saved_apollo_org_next_page=%s", normalized_next_page)
        return
    _legacy_save_apollo_org_cursor(normalized_next_page)


def domains_in_apollo_cooldown(apollo_attempts: dict[str, dict[str, str]], current_time: datetime) -> set[str]:
    cooldown_domains: set[str] = set()
    for domain, record in apollo_attempts.items():
        result = ((record or {}).get("result") or "").strip().lower()
        cooldown_until = parse_iso_datetime((record or {}).get("cooldown_until", ""))
        if result == "no_usable_contacts" and cooldown_until and cooldown_until > current_time:
            cooldown_domains.add(domain)
    return cooldown_domains


def daily_import_log_path() -> Path:
    configured_path = Path(DAILY_IMPORT_LOG_FILE)
    if configured_path.is_absolute():
        return configured_path
    return Path(__file__).resolve().parent / configured_path


def heyreach_processed_leads_path() -> Path:
    configured_path = Path(HEYREACH_PROCESSED_LEADS_FILE)
    if configured_path.is_absolute():
        return configured_path
    return Path(__file__).resolve().parent / configured_path


def normalize_linkedin_url(linkedin_url: str) -> str:
    normalized = (linkedin_url or "").strip()
    if not normalized:
        return ""

    normalized = normalized.split("?", 1)[0].strip().rstrip("/")
    normalized = re.sub(r"^http://", "https://", normalized, flags=re.IGNORECASE)
    return normalized.lower()


def build_heyreach_lead_key(campaign_id: str, linkedin_url: str) -> str:
    normalized_url = normalize_linkedin_url(linkedin_url)
    if not normalized_url:
        return ""
    return f"{campaign_id.strip()}::{normalized_url}"


def _legacy_load_processed_heyreach_leads() -> set[str]:
    if github_state_enabled():
        try:
            content, _ = load_github_state_file(GITHUB_STATE_HEYREACH_LEADS_PATH)
        except requests.RequestException as exc:
            raise HTTPException(status_code=500, detail=f"GitHub HeyReach state read failed: {exc}") from exc

        processed_leads = {
            ((row or {}).get("lead_key") or "").strip()
            for row in parse_csv_text(content)
            if ((row or {}).get("lead_key") or "").strip()
        }
        logger.info("[State] backend=github heyreach_processed_leads=%s", len(processed_leads))
        return processed_leads

    path = heyreach_processed_leads_path()
    if not path.exists():
        return set()

    processed_leads: set[str] = set()
    with path.open("r", encoding="utf-8", newline="") as file_obj:
        reader = csv.DictReader(file_obj)
        for row in reader:
            lead_key = ((row or {}).get("lead_key") or "").strip()
            if lead_key:
                processed_leads.add(lead_key)

    return processed_leads


def load_processed_heyreach_leads() -> set[str]:
    if db_state_enabled():
        processed_leads = load_processed_heyreach_leads_db()
        if processed_leads:
            logger.info("[State] backend=db heyreach_processed_leads=%s", len(processed_leads))
            return processed_leads
        legacy_leads = _legacy_load_processed_heyreach_leads()
        if legacy_leads:
            append_processed_heyreach_leads_db(
                [{"lead_key": lead_key, "campaign_id": "", "linkedin_url": ""} for lead_key in sorted(legacy_leads)],
                "legacy-import",
            )
            logger.info("[State] backend=db migrated_heyreach_processed_leads=%s", len(legacy_leads))
        return legacy_leads
    return _legacy_load_processed_heyreach_leads()


def _legacy_append_processed_heyreach_leads(lead_rows: list[dict[str, str]], run_date: str) -> None:
    if not lead_rows:
        return

    if github_state_enabled():
        try:
            content, _ = load_github_state_file(GITHUB_STATE_HEYREACH_LEADS_PATH)
            existing_rows = parse_csv_text(content)
            existing_keys = {
                ((row or {}).get("lead_key") or "").strip()
                for row in existing_rows
                if ((row or {}).get("lead_key") or "").strip()
            }
            new_rows = list(existing_rows)
            appended_count = 0

            for row in lead_rows:
                lead_key = (row.get("lead_key") or "").strip()
                if not lead_key or lead_key in existing_keys:
                    continue
                new_rows.append(
                    {
                        "lead_key": lead_key,
                        "campaign_id": row.get("campaign_id", ""),
                        "linkedin_url": row.get("linkedin_url", ""),
                        "date_added": run_date,
                    }
                )
                existing_keys.add(lead_key)
                appended_count += 1

            write_github_state_file(
                GITHUB_STATE_HEYREACH_LEADS_PATH,
                write_csv_text(new_rows, ["lead_key", "campaign_id", "linkedin_url", "date_added"]),
                f"Update HeyReach processed leads for {run_date}",
            )
            logger.info("[State] backend=github appended_heyreach_processed_leads=%s", appended_count)
            return
        except requests.RequestException as exc:
            raise HTTPException(status_code=500, detail=f"GitHub HeyReach state write failed: {exc}") from exc

    path = heyreach_processed_leads_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = path.exists()

    with path.open("a", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=["lead_key", "campaign_id", "linkedin_url", "date_added"])
        if not file_exists:
            writer.writeheader()
        for row in lead_rows:
            writer.writerow(
                {
                    "lead_key": row.get("lead_key", ""),
                    "campaign_id": row.get("campaign_id", ""),
                    "linkedin_url": row.get("linkedin_url", ""),
                    "date_added": run_date,
                }
            )


def append_processed_heyreach_leads(lead_rows: list[dict[str, str]], run_date: str) -> None:
    if not lead_rows:
        return
    if db_state_enabled():
        append_processed_heyreach_leads_db(lead_rows, run_date)
        logger.info("[State] backend=db appended_heyreach_processed_leads=%s", len(lead_rows))
        return
    _legacy_append_processed_heyreach_leads(lead_rows, run_date)


def _legacy_load_daily_import_counts() -> dict[str, int]:
    if github_state_enabled():
        try:
            content, _ = load_github_state_file(GITHUB_STATE_DAILY_IMPORTS_PATH)
        except requests.RequestException as exc:
            raise HTTPException(status_code=500, detail=f"GitHub daily import state read failed: {exc}") from exc

        counts_by_date: dict[str, int] = {}
        for row in parse_csv_text(content):
            date_key = ((row or {}).get("date") or "").strip()
            try:
                imported_count = int((row or {}).get("imported_count", 0) or 0)
            except (TypeError, ValueError):
                imported_count = 0

            if date_key:
                counts_by_date[date_key] = counts_by_date.get(date_key, 0) + max(imported_count, 0)

        logger.info("[State] backend=github daily_import_dates=%s", len(counts_by_date))
        return counts_by_date

    path = daily_import_log_path()
    if not path.exists():
        return {}

    counts_by_date: dict[str, int] = {}
    with path.open("r", encoding="utf-8", newline="") as file_obj:
        reader = csv.DictReader(file_obj)
        for row in reader:
            date_key = ((row or {}).get("date") or "").strip()
            try:
                imported_count = int((row or {}).get("imported_count", 0) or 0)
            except (TypeError, ValueError):
                imported_count = 0

            if date_key:
                counts_by_date[date_key] = counts_by_date.get(date_key, 0) + max(imported_count, 0)

    return counts_by_date


def load_daily_import_counts() -> dict[str, int]:
    if db_state_enabled():
        counts = load_daily_import_counts_db()
        if counts:
            logger.info("[State] backend=db daily_import_dates=%s", len(counts))
            return counts
        legacy_counts = _legacy_load_daily_import_counts()
        if legacy_counts:
            for date_key, imported_count in legacy_counts.items():
                append_daily_import_count_db(date_key, imported_count)
            logger.info("[State] backend=db migrated_daily_import_dates=%s", len(legacy_counts))
        return legacy_counts
    return _legacy_load_daily_import_counts()


def _legacy_append_daily_import_count(run_date: str, imported_count: int) -> None:
    if imported_count <= 0:
        return

    if github_state_enabled():
        try:
            content, _ = load_github_state_file(GITHUB_STATE_DAILY_IMPORTS_PATH)
            rows = parse_csv_text(content)
            rows.append({"date": run_date, "imported_count": imported_count})
            write_github_state_file(
                GITHUB_STATE_DAILY_IMPORTS_PATH,
                write_csv_text(rows, ["date", "imported_count"]),
                f"Update daily import counts for {run_date}",
            )
            logger.info("[State] backend=github appended_daily_import_count=%s", imported_count)
            return
        except requests.RequestException as exc:
            raise HTTPException(status_code=500, detail=f"GitHub daily import state write failed: {exc}") from exc

    path = daily_import_log_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = path.exists()

    with path.open("a", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=["date", "imported_count"])
        if not file_exists:
            writer.writeheader()

        writer.writerow({"date": run_date, "imported_count": imported_count})


def append_daily_import_count(run_date: str, imported_count: int, run_id: str = "") -> None:
    if imported_count <= 0:
        return
    if db_state_enabled():
        append_daily_import_count_db(run_date, imported_count, run_id=run_id)
        logger.info("[State] backend=db appended_daily_import_count=%s", imported_count)
        return
    _legacy_append_daily_import_count(run_date, imported_count)


def clean_company_name(company_name: str) -> str:
    cleaned = (company_name or "").strip()
    if not cleaned:
        return ""

    mojibake_replacements = {
        "‚Ä¢": " • ",
        "â€¢": " • ",
        "Â®": "",
        "Â™": "",
        "â„¢": "",
        "Ã©": "e",
        "Ã¨": "e",
        "Ã": "",
    }

    for source, target in mojibake_replacements.items():
        cleaned = cleaned.replace(source, target)

    cleaned = unicodedata.normalize("NFKC", cleaned)

    for separator in (" • ", " | ", " — ", " – ", " :: ", " - "):
        if separator in cleaned:
            cleaned = cleaned.split(separator, 1)[0]
            break

    cleaned = cleaned.replace("&amp;", "&")
    cleaned = re.sub(r"[^\w\s&'\-.,]", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" -|•,")
    return cleaned


def clean_role_name(role_name: str) -> str:
    cleaned = (role_name or "").strip()
    if not cleaned:
        return ""

    cleaned = unicodedata.normalize("NFKC", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    cleaned = cleaned.title()

    acronym_replacements = {
        "Ceo": "CEO",
        "Coo": "COO",
        "Cfo": "CFO",
        "Cmo": "CMO",
        "Cto": "CTO",
        "Cro": "CRO",
        "Cso": "CSO",
        "Cpo": "CPO",
        "Vp": "VP",
    }
    for source, target in acronym_replacements.items():
        cleaned = re.sub(rf"\b{source}\b", target, cleaned)

    cleaned = re.sub(r"\bEcommerce\b", "Ecommerce", cleaned)
    cleaned = re.sub(r"\bE-Commerce\b", "E-commerce", cleaned)
    return cleaned.strip()


def clean_platform_name(platform_name: str) -> str:
    cleaned = (platform_name or "").strip()
    if not cleaned:
        return ""

    normalized = cleaned.lower()
    if "shopify" in normalized:
        return "Shopify"
    if "woocommerce" in normalized:
        return "WooCommerce"
    if "bigcommerce" in normalized:
        return "BigCommerce"
    if "magento" in normalized:
        return "Magento"
    if "wordpress" in normalized:
        return "WordPress"

    cleaned = unicodedata.normalize("NFKC", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.title().strip()


def derive_department(role_name: str) -> str:
    normalized_role = (role_name or "").strip().lower()
    if not normalized_role:
        return ""

    department_keywords = (
        ("operations", "Operations"),
        ("operator", "Operations"),
        ("supply chain", "Operations"),
        ("logistics", "Operations"),
        ("fulfillment", "Operations"),
        ("ecommerce", "Ecommerce"),
        ("e-commerce", "Ecommerce"),
        ("digital", "Ecommerce"),
        ("growth", "Growth"),
        ("marketing", "Marketing"),
        ("brand", "Brand"),
        ("partnership", "Partnerships"),
        ("sales", "Sales"),
        ("revenue", "Revenue"),
        ("founder", "Leadership"),
        ("owner", "Leadership"),
        ("chief", "Leadership"),
        ("president", "Leadership"),
        ("ceo", "Leadership"),
        ("coo", "Leadership"),
    )

    for keyword, department in department_keywords:
        if keyword in normalized_role:
            return department

    return "Leadership"


def format_money_bucket(amount: float | None) -> str:
    if amount is None:
        return ""

    normalized_amount = int(round(max(amount, 0)))
    if normalized_amount == 0:
        return "$0"

    if normalized_amount < 1_000:
        bucket_size = 100
    elif normalized_amount < 100_000:
        bucket_size = 10_000
    elif normalized_amount < 1_000_000:
        bucket_size = 100_000
    elif normalized_amount < 10_000_000:
        bucket_size = 1_000_000
    else:
        bucket_size = 10_000_000

    bucketed_amount = round(normalized_amount / bucket_size) * bucket_size
    bucketed_amount = max(bucketed_amount, bucket_size)

    return f"${int(bucketed_amount):,}"


def estimate_monthly_orders(revenue: float | None, average_product_price_usd: float | None) -> int | None:
    if revenue is None or average_product_price_usd is None or average_product_price_usd <= 0:
        return None

    return max(int(round(revenue / average_product_price_usd)), 0)


def format_orders_bucket(order_count: int | None) -> str:
    if order_count is None:
        return ""

    if order_count == 0:
        return "0"

    if order_count < 100:
        bucket_size = 10
    elif order_count < 1_000:
        bucket_size = 100
    elif order_count < 10_000:
        bucket_size = 1_000
    else:
        bucket_size = 10_000

    bucketed_orders = round(order_count / bucket_size) * bucket_size
    bucketed_orders = max(bucketed_orders, bucket_size)

    return f"{int(bucketed_orders):,}"


def build_location_name(city: str, state: str, country_code: str) -> str:
    parts = [part.strip() for part in (city, state, country_code) if str(part or "").strip()]
    if not parts:
        return ""

    if len(parts) >= 2 and parts[0].lower() == parts[1].lower():
        parts = [parts[0]] + parts[2:]

    return ", ".join(parts)


def apply_daily_import_limit(
    rows: list[dict[str, Any]],
    run_date: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    scheduler_status = {
        "enabled": False,
        "weekday_only": ENABLE_WEEKDAY_ONLY_IMPORTS,
        "daily_limit": DAILY_NEW_LEAD_LIMIT,
        "already_imported_today": 0,
        "remaining_capacity": None,
        "run_date": run_date,
        "status": "disabled",
    }

    if not rows:
        scheduler_status["status"] = "no_rows"
        return rows, scheduler_status

    if DAILY_NEW_LEAD_LIMIT <= 0 and not ENABLE_WEEKDAY_ONLY_IMPORTS:
        return rows, scheduler_status

    scheduler_status["enabled"] = True

    current_date = datetime.now().date()
    if ENABLE_WEEKDAY_ONLY_IMPORTS and current_date.weekday() >= 5:
        scheduler_status["status"] = "weekend_blocked"
        scheduler_status["remaining_capacity"] = 0
        return [], scheduler_status

    if DAILY_NEW_LEAD_LIMIT <= 0:
        scheduler_status["status"] = "weekday_only_passthrough"
        return rows, scheduler_status

    daily_counts = load_daily_import_counts()
    today_key = current_date.isoformat()
    already_imported_today = daily_counts.get(today_key, 0)
    remaining_capacity = max(DAILY_NEW_LEAD_LIMIT - already_imported_today, 0)

    scheduler_status["already_imported_today"] = already_imported_today
    scheduler_status["remaining_capacity"] = remaining_capacity

    if remaining_capacity <= 0:
        scheduler_status["status"] = "daily_capacity_reached"
        return [], scheduler_status

    scheduler_status["status"] = "limited" if len(rows) > remaining_capacity else "within_capacity"
    return rows[:remaining_capacity], scheduler_status


# ========= APOLLO ORGANIZATION SEARCH =========
def _flatten_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        return " ".join(_flatten_text(item) for item in value.values())
    if isinstance(value, list):
        return " ".join(_flatten_text(item) for item in value)
    return ""


def _extract_first_text(value: Any) -> str:
    flattened = _flatten_text(value)
    return re.sub(r"\s+", " ", flattened).strip()


def _extract_country_code(value: Any) -> str:
    normalized = _extract_first_text(value).upper()
    if normalized in {"UNITED STATES", "USA"}:
        return "US"
    if normalized in {"UNITED KINGDOM", "GREAT BRITAIN"}:
        return "GB"
    if normalized in {"CANADA"}:
        return "CA"
    if normalized in {"AUSTRALIA"}:
        return "AU"
    return normalized


def _parse_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    digits = re.sub(r"[^\d]", "", str(value))
    if not digits:
        return None
    try:
        return int(digits)
    except ValueError:
        return None


def _extract_revenue_ceiling(value: Any) -> int | None:
    if isinstance(value, (int, float)):
        return int(value)
    text = _extract_first_text(value).lower()
    if not text:
        return None
    amounts = re.findall(r"(\d+(?:\.\d+)?)\s*([mbk]?)", text)
    if not amounts:
        return None
    max_value = 0
    for amount_text, suffix in amounts:
        amount = float(amount_text)
        multiplier = 1
        if suffix == "k":
            multiplier = 1_000
        elif suffix == "m":
            multiplier = 1_000_000
        elif suffix == "b":
            multiplier = 1_000_000_000
        max_value = max(max_value, int(amount * multiplier))
    return max_value or None


def build_apollo_org_search_params(page: int) -> dict[str, Any]:
    categories = list(APOLLO_CATEGORY_SEARCH_KEYWORDS.keys())
    category_key = categories[(page - 1) % len(categories)] if categories else ""
    category_terms = APOLLO_CATEGORY_SEARCH_KEYWORDS.get(category_key, ())
    return {
        "page": page,
        "per_page": APOLLO_ORG_PAGE_SIZE,
        "organization_num_employees_ranges[]": [f"{APOLLO_MIN_EMPLOYEE_COUNT},{APOLLO_MAX_EMPLOYEE_COUNT}"],
        "organization_locations[]": ["United States", "United Kingdom", "Canada", "Australia"],
        "q_organization_keyword_tags[]": list(category_terms),
    }


def normalize_apollo_organization(org: dict[str, Any]) -> dict[str, Any]:
    domain = normalize_domain(
        org.get("website_url")
        or org.get("primary_domain")
        or org.get("domain")
        or org.get("organization_website_url")
        or org.get("organization_primary_domain")
        or ""
    )
    technologies_text = _extract_first_text(
        org.get("organization_technology_names")
        or org.get("technology_names")
        or org.get("organization_technologies")
        or org.get("technologies")
    )
    industry_text = _extract_first_text(
        org.get("industry")
        or org.get("industry_tag")
        or org.get("industry_tags")
        or org.get("industry_keywords")
        or org.get("keywords")
        or org.get("organization_keywords")
    )
    market_segment_text = _extract_first_text(
        org.get("market_segment")
        or org.get("market_segments")
        or org.get("organization_market_segments")
    )
    sic_text = _extract_first_text(org.get("sic") or org.get("sic_codes"))
    naics_text = _extract_first_text(org.get("naics") or org.get("naics_codes"))
    country_code = _extract_country_code(
        org.get("country")
        or org.get("country_code")
        or org.get("organization_country")
        or org.get("organization_country_code")
    )

    return {
        "name": domain,
        "title": clean_company_name(org.get("name") or org.get("organization_name") or domain),
        "website_url": org.get("website_url") or org.get("organization_website_url") or "",
        "platform": technologies_text,
        "country_code": country_code,
        "state": _extract_first_text(org.get("state") or org.get("organization_state")),
        "city": _extract_first_text(org.get("city") or org.get("organization_city")),
        "estimated_sales": parse_monthly_sales(org),
        "avg_price_usd": "",
        "industry": industry_text,
        "market_segment": market_segment_text,
        "sic": sic_text,
        "naics": naics_text,
        "employee_count": _parse_int(
            org.get("estimated_num_employees")
            or org.get("employee_count")
            or org.get("employees")
            or org.get("organization_num_employees")
        ),
        "annual_revenue": _extract_revenue_ceiling(
            org.get("estimated_annual_revenue")
            or org.get("annual_revenue")
            or org.get("organization_annual_revenue")
            or org.get("organization_estimated_annual_revenue")
            or org.get("estimated_revenue_range")
        ),
        "source_provider": "apollo_org_search",
        "raw_keywords": " ".join(
            part for part in (industry_text, market_segment_text, sic_text, naics_text, technologies_text) if part
        ),
    }


def derive_org_category(org: dict[str, Any]) -> str:
    searchable_text = " ".join(
        part
        for part in (
            org.get("market_segment", ""),
            org.get("industry", ""),
            org.get("sic", ""),
            org.get("naics", ""),
            org.get("platform", ""),
            org.get("raw_keywords", ""),
            org.get("title", ""),
        )
        if part
    ).lower()

    for category, keywords in ORG_CATEGORY_KEYWORDS.items():
        if any(keyword in searchable_text for keyword in keywords):
            return category
    return "general_dtc"


def load_campaign_routing() -> dict[str, dict[str, str]]:
    if not INSTANTLY_CAMPAIGN_ROUTING_JSON:
        return {}
    try:
        payload = json.loads(INSTANTLY_CAMPAIGN_ROUTING_JSON)
    except json.JSONDecodeError:
        logger.warning("[Routing] invalid INSTANTLY_CAMPAIGN_ROUTING_JSON")
        return {}
    if not isinstance(payload, dict):
        return {}

    routing: dict[str, dict[str, str]] = {}
    for key, value in payload.items():
        if not isinstance(value, dict):
            continue
        routing[str(key)] = {
            "campaign_id": str(value.get("campaign_id", "")).strip(),
            "campaign_name": str(value.get("campaign_name", key)).strip() or str(key),
        }
    return routing


def route_campaign_for_org(org: dict[str, Any], settings: Settings) -> tuple[str, str, str]:
    category = derive_org_category(org)
    routing = load_campaign_routing()
    route = routing.get(category)
    if route and route.get("campaign_id"):
        return category, route["campaign_id"], route.get("campaign_name") or category
    return category, settings.instantly_campaign_id, TARGET_CAMPAIGN_NAME


def matches_apollo_org_icp(org: dict[str, Any]) -> bool:
    domain = normalize_domain(org.get("name", ""))
    if not domain:
        return False

    employee_count = org.get("employee_count")
    if employee_count is not None and employee_count < APOLLO_MIN_EMPLOYEE_COUNT:
        return False
    if employee_count is not None and employee_count > APOLLO_MAX_EMPLOYEE_COUNT:
        return False

    annual_revenue = org.get("annual_revenue")
    if annual_revenue is not None and annual_revenue > APOLLO_MAX_ANNUAL_REVENUE:
        return False

    country_code = _extract_country_code(org.get("country_code", ""))
    if country_code and country_code not in APOLLO_ALLOWED_COUNTRIES:
        return False

    searchable_text = " ".join(
        part
        for part in (
            domain,
            org.get("title", ""),
            org.get("platform", ""),
            org.get("industry", ""),
            org.get("market_segment", ""),
            org.get("sic", ""),
            org.get("naics", ""),
            org.get("raw_keywords", ""),
        )
        if part
    ).lower()
    if any(keyword in searchable_text for keyword in APOLLO_EXCLUDED_KEYWORDS):
        return False

    matched_categories = [category for category, keywords in ORG_CATEGORY_KEYWORDS.items() if any(keyword in searchable_text for keyword in keywords)]
    if matched_categories:
        return True

    return any(keyword in searchable_text for keyword in APOLLO_ECOMMERCE_INCLUDE_KEYWORDS)


def fetch_apollo_org_page(page: int, settings: Settings) -> list[dict[str, Any]]:
    preferred_params = build_apollo_org_search_params(page)
    try:
        response = requests.post(
            APOLLO_ORG_SEARCH_URL,
            headers={
                "Content-Type": "application/json",
                "X-Api-Key": settings.apollo_api_key,
            },
            params=preferred_params,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
    except requests.RequestException as exc:
        logger.warning("[ApolloOrg] organization search request error for page=%s: %s", page, exc)
        raise HTTPException(status_code=502, detail=f"Apollo organization search failed on page {page}") from exc

    if response.status_code in {401, 403, 422}:
        logger.warning(
            "[ApolloOrg] preferred search params rejected status=%s body=%s; retrying with basic paging only",
            response.status_code,
            response.text,
        )
        try:
            response = requests.post(
                APOLLO_ORG_SEARCH_URL,
                headers={
                    "Content-Type": "application/json",
                    "X-Api-Key": settings.apollo_api_key,
                },
                params={"page": page, "per_page": APOLLO_ORG_PAGE_SIZE},
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
        except requests.RequestException as exc:
            logger.warning("[ApolloOrg] fallback organization search request error for page=%s: %s", page, exc)
            raise HTTPException(status_code=502, detail=f"Apollo organization search failed on page {page}") from exc

    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        logger.warning("[ApolloOrg] organization search non-200 page=%s status=%s body=%s", page, response.status_code, response.text)
        raise HTTPException(status_code=502, detail=f"Apollo organization search failed on page {page}") from exc

    try:
        data = response.json()
    except ValueError as exc:
        logger.warning("[ApolloOrg] invalid JSON for page=%s body=%s", page, response.text)
        raise HTTPException(status_code=502, detail=f"Apollo organization search returned invalid JSON on page {page}") from exc

    organizations = data.get("organizations") or data.get("accounts") or []
    logger.info("[ApolloOrg] page=%s returned %s organizations", page, len(organizations))
    return [normalize_apollo_organization(org) for org in organizations if isinstance(org, dict)]


def collect_domains(
    max_domains: int,
    settings: Settings,
    processed_domains: set[str] | None = None,
) -> StoreLeadsCollectionResult:
    start_page = load_apollo_org_cursor()
    max_pages_this_run = effective_max_apollo_org_pages(max_domains)
    qualified_domains: list[dict[str, Any]] = []
    seen_domains: set[str] = set()
    processed_domains = processed_domains or set()
    apollo_attempts = load_apollo_attempts()
    apollo_cooldown_domains = domains_in_apollo_cooldown(apollo_attempts, datetime.now(timezone.utc))
    raw_scanned = 0
    qualified_matches_total = 0
    skipped_processed_domains = 0
    skipped_apollo_cooldown_domains = 0
    pages_scanned = 0
    last_scanned_page = start_page

    logger.info(
        "[ApolloOrg] start_page=%s requested_domains=%s max_pages_this_run=%s page_size=%s",
        start_page,
        max_domains,
        max_pages_this_run,
        APOLLO_ORG_PAGE_SIZE,
    )

    for page_offset in range(max_pages_this_run):
        page = ((start_page - 1 + page_offset) % MAX_APOLLO_ORG_PAGES) + 1
        organizations = fetch_apollo_org_page(page, settings)
        pages_scanned += 1
        last_scanned_page = page

        if not organizations:
            continue

        raw_scanned += len(organizations)

        for org in organizations:
            normalized_domain = normalize_domain(org.get("name", ""))
            if not normalized_domain or normalized_domain in seen_domains:
                continue

            seen_domains.add(normalized_domain)

            if not matches_apollo_org_icp(org):
                continue

            qualified_matches_total += 1

            if normalized_domain in processed_domains:
                skipped_processed_domains += 1
                continue

            if normalized_domain in apollo_cooldown_domains:
                skipped_apollo_cooldown_domains += 1
                continue

            qualified_domains.append(org)

            if len(qualified_domains) >= max_domains:
                next_page = (page % MAX_APOLLO_ORG_PAGES) + 1
                save_apollo_org_cursor(next_page)
                return StoreLeadsCollectionResult(
                    domains=qualified_domains,
                    raw_scanned=raw_scanned,
                    qualified_matches_total=qualified_matches_total,
                    skipped_processed_domains=skipped_processed_domains,
                    skipped_apollo_cooldown_domains=skipped_apollo_cooldown_domains,
                    start_page=start_page,
                    end_page=page,
                    pages_scanned=pages_scanned,
                    next_page=next_page,
                )

    next_page = (last_scanned_page % MAX_APOLLO_ORG_PAGES) + 1
    save_apollo_org_cursor(next_page)
    return StoreLeadsCollectionResult(
        domains=qualified_domains,
        raw_scanned=raw_scanned,
        qualified_matches_total=qualified_matches_total,
        skipped_processed_domains=skipped_processed_domains,
        skipped_apollo_cooldown_domains=skipped_apollo_cooldown_domains,
        start_page=start_page,
        end_page=last_scanned_page,
        pages_scanned=pages_scanned,
        next_page=next_page,
    )


# ========= APOLLO =========
def is_personal_email(email: str) -> bool:
    normalized_email = email.strip().lower()
    if not normalized_email:
        return False

    return not any(normalized_email.startswith(prefix) for prefix in GENERIC_EMAIL_PREFIXES)


def extract_contact_email(contact: dict[str, Any]) -> str:
    direct_email = (contact.get("email") or "").strip().lower()
    if direct_email:
        return direct_email

    emails = contact.get("emails") or []
    if emails and isinstance(emails, list):
        first_email = ((emails[0] or {}).get("email") or "").strip().lower()
        if first_email:
            return first_email

    return ""


def extract_contact_name(contact: dict[str, Any]) -> str:
    full_name = (contact.get("name") or "").strip()
    if full_name:
        return full_name

    first_name = (contact.get("first_name") or "").strip()
    last_name = (contact.get("last_name") or "").strip()
    return " ".join(part for part in (first_name, last_name) if part).strip()


def email_matches_store(email: str, store_domain: str) -> bool:
    email_domain = normalize_domain(email.split("@")[-1])
    normalized_store_domain = normalize_domain(store_domain)
    return email_domain == normalized_store_domain or email_domain.endswith("." + normalized_store_domain)


def score_contact_title(title: str) -> int:
    normalized_title = (title or "").strip().lower()
    if not normalized_title:
        return 0

    weighted_keywords = (
        ("founder", 100),
        ("co-founder", 100),
        ("owner", 90),
        ("chief executive officer", 85),
        ("ceo", 85),
        ("president", 80),
        ("chief operating officer", 75),
        ("coo", 75),
        ("chief of staff", 70),
        ("head of", 65),
        ("vp", 55),
        ("vice president", 55),
        ("director", 45),
        ("operations", 40),
        ("operator", 40),
        ("manager", 30),
    )

    for keyword, score in weighted_keywords:
        if keyword in normalized_title:
            return score

    return 10


def search_apollo_people(
    domain: str,
    settings: Settings,
    *,
    max_results: int = APOLLO_SEARCH_CANDIDATES_PER_DOMAIN,
) -> tuple[list[dict[str, Any]], str]:
    search_params = {
        "page": 1,
        "per_page": max_results,
        "include_similar_titles": "true",
        "person_titles[]": list(APOLLO_TARGET_TITLES),
        "person_seniorities[]": list(APOLLO_TARGET_SENIORITIES),
        "q_organization_domains_list[]": [domain],
    }

    try:
        response = requests.post(
            APOLLO_PEOPLE_SEARCH_URL,
            headers={
                "Content-Type": "application/json",
                "X-Api-Key": settings.apollo_api_key,
            },
            params=search_params,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
    except requests.RequestException as exc:
        logger.warning("[Apollo] people search request error for domain=%s: %s", domain, exc)
        return [], "request_error"

    if response.status_code != 200:
        failure_class = "request_shape_or_unknown"
        if response.status_code in (401, 403):
            failure_class = "api_key_permission_or_scope"
        elif response.status_code == 422:
            failure_class = "request_shape"
        elif response.status_code >= 500:
            failure_class = "apollo_server_error"
        logger.warning(
            "[Apollo] people search non-200 for domain=%s status=%s failure_class=%s body=%s",
            domain,
            response.status_code,
            failure_class,
            response.text,
        )
        if APOLLO_DEBUG_RAW:
            logger.debug("[Apollo] people search params for domain=%s: %s", domain, search_params)
        return [], "request_error"

    try:
        data = response.json()
    except ValueError:
        logger.warning("[Apollo] invalid people search JSON for domain=%s", domain)
        return [], "request_error"

    if APOLLO_DEBUG_RAW:
        logger.debug("[Apollo] people search response for domain=%s: %s", domain, data)

    people = data.get("people", []) or []
    logger.info("[Apollo] raw people for domain=%s: %s", domain, len(people))
    return people, "ok"


def enrich_apollo_people(people: list[dict[str, Any]], settings: Settings) -> tuple[list[dict[str, Any]], str]:
    if not people:
        return [], "ok"

    details = [{"id": person["id"]} for person in people if person.get("id")]
    if not details:
        return [], "ok"

    enrich_payload = {"details": details}

    try:
        response = requests.post(
            APOLLO_BULK_PEOPLE_MATCH_URL,
            headers={
                "Content-Type": "application/json",
                "X-Api-Key": settings.apollo_api_key,
            },
            params={
                "reveal_personal_emails": "false",
                "reveal_phone_number": "false",
            },
            json=enrich_payload,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
    except requests.RequestException as exc:
        logger.warning("[Apollo] people enrichment request error: %s", exc)
        return [], "request_error"

    if response.status_code != 200:
        failure_class = "request_shape_or_unknown"
        if response.status_code in (401, 403):
            failure_class = "api_key_permission_or_scope"
        elif response.status_code == 422:
            failure_class = "request_shape"
        elif response.status_code >= 500:
            failure_class = "apollo_server_error"
        logger.warning(
            "[Apollo] people enrichment non-200 status=%s failure_class=%s body=%s",
            response.status_code,
            failure_class,
            response.text,
        )
        if APOLLO_DEBUG_RAW:
            logger.debug("[Apollo] people enrichment payload: %s", enrich_payload)
        return [], "request_error"

    try:
        data = response.json()
    except ValueError:
        logger.warning("[Apollo] invalid people enrichment JSON")
        return [], "request_error"

    if APOLLO_DEBUG_RAW:
        logger.debug("[Apollo] people enrichment response: %s", data)

    matches = [match for match in (data.get("matches", []) or []) if isinstance(match, dict)]
    logger.info("[Apollo] enriched people returned: %s", len(matches))
    return matches, "ok"


def search_apollo_contacts(
    domain: str,
    settings: Settings,
    *,
    max_per_domain: int = MAX_CONTACTS_PER_DOMAIN,
) -> tuple[list[dict[str, Any]], dict[str, int | str]]:
    people, people_status = search_apollo_people(domain, settings)
    if people_status != "ok":
        logger.info(
            "[ApolloPipeline] domain=%s stage=people_search result=request_error likely_root_cause=request_or_api_error",
            domain,
        )
        return [], {
            "domain": domain,
            "people_search_candidates": 0,
            "enrichment_matches": 0,
            "candidates_with_any_email": 0,
            "candidates_with_brand_domain_email": 0,
            "attempt_status": "request_error",
        }
    if not people:
        logger.info(
            "[ApolloPipeline] domain=%s stage=people_search result=empty likely_root_cause=request_shape_permission_or_no_results",
            domain,
        )
        return [], {
            "domain": domain,
            "people_search_candidates": 0,
            "enrichment_matches": 0,
            "candidates_with_any_email": 0,
            "candidates_with_brand_domain_email": 0,
            "attempt_status": "ok",
        }

    enriched_people, enrichment_status = enrich_apollo_people(people, settings)
    if enrichment_status != "ok":
        logger.info(
            "[ApolloPipeline] domain=%s stage=enrichment result=request_error likely_root_cause=permission_shape_or_api_error people_search_candidates=%s",
            domain,
            len(people),
        )
        return [], {
            "domain": domain,
            "people_search_candidates": len(people),
            "enrichment_matches": 0,
            "candidates_with_any_email": 0,
            "candidates_with_brand_domain_email": 0,
            "attempt_status": "request_error",
        }
    if not enriched_people:
        logger.info(
            "[ApolloPipeline] domain=%s stage=enrichment result=empty likely_root_cause=permission_shape_or_no_matches people_search_candidates=%s",
            domain,
            len(people),
        )
        return [], {
            "domain": domain,
            "people_search_candidates": len(people),
            "enrichment_matches": 0,
            "candidates_with_any_email": 0,
            "candidates_with_brand_domain_email": 0,
            "attempt_status": "ok",
        }

    candidates_with_any_email = sum(1 for person in enriched_people if extract_contact_email(person))

    filtered_people = [
        person
        for person in enriched_people
        if email_matches_store(extract_contact_email(person), domain)
    ]
    filtered_people.sort(key=lambda person: score_contact_title(person.get("title", "")), reverse=True)
    return filtered_people[: max_per_domain * 6], {
        "domain": domain,
        "people_search_candidates": len(people),
        "enrichment_matches": len(enriched_people),
        "candidates_with_any_email": candidates_with_any_email,
        "candidates_with_brand_domain_email": len(filtered_people),
        "attempt_status": "ok",
    }


# ========= LEAD OUTPUT BUILDERS =========
def accepted_lead_target_per_run() -> int:
    if TARGET_ACCEPTED_LEADS_PER_RUN > 0:
        return TARGET_ACCEPTED_LEADS_PER_RUN
    if DAILY_NEW_LEAD_LIMIT > 0:
        return DAILY_NEW_LEAD_LIMIT
    return 15


def effective_max_apollo_domains_per_run() -> int:
    target = max(1, accepted_lead_target_per_run())
    target_based_budget = max(APOLLO_MIN_DOMAINS_PER_RUN, int(math.ceil(target * APOLLO_DOMAINS_PER_TARGET_LEAD)))
    return max(1, min(MAX_APOLLO_DOMAINS_PER_RUN, target_based_budget))


def effective_max_apollo_org_pages(max_domains: int) -> int:
    requested_pages = max(1, int(math.ceil(max(1, max_domains) / max(APOLLO_ORG_PAGE_SIZE, 1))))
    target_pages = requested_pages * max(1, APOLLO_ORG_PAGES_PER_REQUESTED_PAGE)
    return max(1, min(MAX_APOLLO_ORG_PAGES, max(APOLLO_MIN_ORG_PAGES_PER_RUN, target_pages)))


def determine_offer(revenue: float | None) -> str:
    if revenue and revenue >= 150000:
        return "Fulfillment"
    return "Shipping Optimization"


def build_csv_rows(
    domains: list[dict[str, Any]],
    run_date: str,
    settings: Settings,
) -> LeadBuildRowsResult:
    instantly_rows: list[dict[str, Any]] = []
    linkedin_rows: list[dict[str, Any]] = []
    successful_contacts = 0
    apollo_hits = 0
    apollo_domains_attempted = 0
    apollo_attempt_rows: list[dict[str, str]] = []
    seen_emails_global: set[str] = set()
    accepted_target = accepted_lead_target_per_run()
    max_apollo_domains_this_run = effective_max_apollo_domains_per_run()

    for store in domains:
        domain = normalize_domain(store.get("name", ""))
        if not domain:
            continue
        if apollo_domains_attempted >= max_apollo_domains_this_run:
            break
        if successful_contacts >= accepted_target:
            break

        contacts, apollo_debug_stats = search_apollo_contacts(
            domain,
            settings,
            max_per_domain=MAX_CONTACTS_PER_DOMAIN,
        )
        apollo_domains_attempted += 1
        time.sleep(APOLLO_SLEEP_SECONDS)

        if contacts:
            apollo_hits += 1

        revenue = parse_monthly_sales(store)
        formatted_revenue = format_money_bucket(revenue)
        average_product_price_usd = parse_average_product_price_usd(store)
        formatted_average_product_price = format_money_bucket(average_product_price_usd)
        estimated_monthly_orders = estimate_monthly_orders(revenue, average_product_price_usd)
        formatted_estimated_monthly_orders = format_orders_bucket(estimated_monthly_orders)
        offer = determine_offer(revenue)
        org_category, campaign_id, campaign_name = route_campaign_for_org(store, settings)
        accepted_for_domain = 0

        for contact in contacts:
            if accepted_for_domain >= MAX_CONTACTS_PER_DOMAIN:
                break

            full_name = extract_contact_name(contact)
            email = extract_contact_email(contact)

            if not email:
                continue
            if not is_personal_email(email):
                continue
            if not email_matches_store(email, domain):
                continue
            if email in seen_emails_global:
                continue

            seen_emails_global.add(email)
            accepted_for_domain += 1

            first_name, last_name = split_full_name(full_name)
            linkedin_url = contact.get("linkedin_url", "") or ""
            store_title = clean_company_name(store.get("title", "") or "")
            role = clean_role_name(contact.get("title", "") or "")
            department = derive_department(role)
            platform = clean_platform_name(store.get("platform", "") or "")
            location = build_location_name(
                store.get("city", "") or "",
                store.get("state", "") or "",
                store.get("country_code", "") or "",
            )

            instantly_rows.append(
                {
                    "first_name": first_name,
                    "last_name": last_name,
                    "email": email,
                    "role": role,
                    "department": department,
                    "linkedin_url": linkedin_url,
                    "company_name": store_title,
                    "website": domain,
                    "platform": platform,
                    "location": location,
                    "city": store.get("city", ""),
                    "state": store.get("state", ""),
                    "revenue": formatted_revenue,
                    "average_product_price": formatted_average_product_price,
                    "estimated_monthly_orders": formatted_estimated_monthly_orders,
                    "market_segment": store.get("market_segment", ""),
                    "industry": store.get("industry", ""),
                    "org_category": org_category,
                    "campaign_name": campaign_name,
                    "campaign_id": campaign_id,
                    "custom_offer": offer,
                }
            )

            linkedin_rows.append(
                {
                    "name": full_name,
                    "role": role,
                    "department": department,
                    "linkedin_url": linkedin_url,
                    "company": store_title,
                    "website": domain,
                    "email": email,
                    "platform": platform,
                    "location": location,
                    "city": store.get("city", ""),
                    "state": store.get("state", ""),
                    "revenue": formatted_revenue,
                    "average_product_price": formatted_average_product_price,
                    "estimated_monthly_orders": formatted_estimated_monthly_orders,
                    "market_segment": store.get("market_segment", ""),
                    "industry": store.get("industry", ""),
                    "org_category": org_category,
                    "date_added": run_date,
                }
            )

            successful_contacts += 1

        attempt_status = str(apollo_debug_stats.get("attempt_status", "ok"))
        cooldown_until = ""
        attempt_result = "accepted_contacts" if accepted_for_domain > 0 else "no_usable_contacts"
        if attempt_status == "request_error":
            attempt_result = "request_error"
        elif accepted_for_domain == 0:
            cooldown_until = (datetime.now(timezone.utc) + timedelta(days=APOLLO_ATTEMPT_COOLDOWN_DAYS)).isoformat()

        apollo_attempt_rows.append(
            {
                "domain": domain,
                "last_attempted_at": current_utc_timestamp(),
                "result": attempt_result,
                "cooldown_until": cooldown_until,
            }
        )

        logger.info(
            "[ApolloPipeline] domain=%s people_search_candidates=%s enrichment_matches=%s "
            "candidates_with_any_email=%s candidates_with_brand_domain_email=%s "
            "final_contacts_selected=%s",
            apollo_debug_stats["domain"],
            apollo_debug_stats["people_search_candidates"],
            apollo_debug_stats["enrichment_matches"],
            apollo_debug_stats["candidates_with_any_email"],
            apollo_debug_stats["candidates_with_brand_domain_email"],
            accepted_for_domain,
        )

    logger.info(
        "[ApolloBudget] accepted_target=%s max_apollo_domains_this_run=%s attempted=%s successful_contacts=%s",
        accepted_target,
        max_apollo_domains_this_run,
        apollo_domains_attempted,
        successful_contacts,
    )

    return LeadBuildRowsResult(
        instantly_rows=instantly_rows,
        linkedin_rows=linkedin_rows,
        successful_contacts=successful_contacts,
        apollo_hits=apollo_hits,
        apollo_domains_attempted=apollo_domains_attempted,
        apollo_attempt_rows=apollo_attempt_rows,
    )


# ========= SLACK =========
def build_slack_headers(settings: Settings) -> dict[str, str]:
    return {"Authorization": f"Bearer {settings.slack_bot_token}"}


def build_heyreach_headers(settings: Settings) -> dict[str, str]:
    return {
        "X-API-KEY": settings.heyreach_api_key,
        "Content-Type": "application/json",
    }


def normalize_external_identifier(value: str) -> int | str:
    normalized = (value or "").strip()
    if normalized.isdigit():
        return int(normalized)
    return normalized


def validate_heyreach_api_key(settings: Settings) -> tuple[bool, str]:
    try:
        response = requests.get(
            HEYREACH_CHECK_API_KEY_URL,
            headers={"X-API-KEY": settings.heyreach_api_key},
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
    except requests.RequestException as exc:
        logger.warning("[HeyReach] API key check request failed: %s", exc)
        return False, "request_failed"

    if response.status_code != 200:
        logger.warning(
            "[HeyReach] API key check non-200 status=%s body=%s",
            response.status_code,
            response.text,
        )
        return False, "invalid_api_key"

    try:
        payload = response.json()
    except ValueError:
        logger.warning("[HeyReach] API key check returned invalid JSON")
        return False, "invalid_api_key"

    payload_status = str(payload.get("status") or payload.get("message") or "ok").lower()
    logger.info("[HeyReach] API key check status=%s", payload_status)
    return True, "ok"


def upload_file_to_slack(filename: str, content: str, settings: Settings) -> dict[str, Any]:
    if not content:
        return {"ok": True, "skipped": True, "reason": "empty_file"}

    content_bytes = content.encode("utf-8")
    if len(content_bytes) <= 1:
        return {"ok": True, "skipped": True, "reason": "empty_file"}

    upload_details_response = requests.post(
        SLACK_GET_UPLOAD_URL,
        headers=build_slack_headers(settings),
        data={
            "filename": filename,
            "length": str(len(content_bytes)),
        },
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    upload_details_response.raise_for_status()
    upload_details = upload_details_response.json()

    if not upload_details.get("ok"):
        raise HTTPException(
            status_code=500,
            detail=f"Slack getUploadURLExternal failed: {upload_details}",
        )

    upload_url = upload_details.get("upload_url")
    file_id = upload_details.get("file_id")
    if not upload_url or not file_id:
        raise HTTPException(
            status_code=500,
            detail=f"Slack upload URL missing from response: {upload_details}",
        )

    upload_response = requests.post(
        upload_url,
        data=content_bytes,
        headers={"Content-Type": "application/octet-stream"},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    upload_response.raise_for_status()

    completion_response = requests.post(
        SLACK_COMPLETE_UPLOAD_URL,
        headers=build_slack_headers(settings),
        data={
            "files": json.dumps([{"id": file_id, "title": filename}]),
            "channel_id": settings.slack_channel_id,
        },
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    completion_response.raise_for_status()
    completion = completion_response.json()

    if not completion.get("ok"):
        raise HTTPException(
            status_code=500,
            detail=f"Slack completeUploadExternal failed: {completion}",
        )

    return completion


def import_leads_to_instantly(rows: list[dict[str, Any]], settings: Settings) -> dict[str, Any]:
    if not rows:
        return {"status": "skipped", "reason": "no_rows", "created_count": 0, "skipped_count": 0}

    if not settings.instantly_campaign_id and not any(str(row.get("campaign_id", "")).strip() for row in rows):
        logger.warning("[Instantly] import skipped: INSTANTLY_CAMPAIGN_ID missing")
        return {"status": "skipped", "reason": "missing_campaign_id", "created_count": 0, "skipped_count": 0}

    if not settings.instantly_api_key:
        logger.warning("[Instantly] import skipped: INSTANTLY_API_KEY missing")
        return {"status": "skipped", "reason": "missing_api_key", "created_count": 0, "skipped_count": 0}

    rows_by_campaign: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        campaign_id = str(row.get("campaign_id", "")).strip() or settings.instantly_campaign_id
        if not campaign_id:
            continue
        rows_by_campaign.setdefault(campaign_id, []).append(row)

    created_count = 0
    skipped_count = 0
    total_sent = 0
    statuses: list[str] = []
    campaign_results: list[dict[str, Any]] = []

    for campaign_id, campaign_rows in rows_by_campaign.items():
        leads = []
        for row in campaign_rows:
            leads.append(
                {
                    "email": row.get("email", ""),
                    "first_name": row.get("first_name", ""),
                    "last_name": row.get("last_name", ""),
                    "company_name": row.get("company_name", ""),
                    "website": row.get("website", ""),
                    "custom_variables": {
                        "custom_offer": row.get("custom_offer", ""),
                        "linkedin_url": row.get("linkedin_url", ""),
                        "role": row.get("role", ""),
                        "department": row.get("department", ""),
                        "platform": row.get("platform", ""),
                        "location": row.get("location", ""),
                        "city": row.get("city", ""),
                        "state": row.get("state", ""),
                        "revenue": row.get("revenue", ""),
                        "average_product_price": row.get("average_product_price", ""),
                        "estimated_monthly_orders": row.get("estimated_monthly_orders", ""),
                        "market_segment": row.get("market_segment", ""),
                        "industry": row.get("industry", ""),
                        "org_category": row.get("org_category", ""),
                    },
                }
            )

        response = requests.post(
            INSTANTLY_ADD_LEADS_URL,
            headers={
                "Authorization": f"Bearer {settings.instantly_api_key}",
                "Content-Type": "application/json",
            },
            json={
                "campaign_id": campaign_id,
                "leads": leads,
                "verify_leads_on_import": False,
                "skip_if_in_workspace": True,
            },
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        result = response.json()
        result_status = (result.get("status") or "unknown").lower()
        if result.get("error") or result_status in {"error", "failed"}:
            raise HTTPException(status_code=500, detail=f"Instantly import failed: {result}")

        batch_created_count = len(result.get("created_leads", []) or [])
        batch_skipped_count = int(result.get("skipped_count", 0) or 0)
        batch_total_sent = int(result.get("total_sent", len(campaign_rows)) or len(campaign_rows))
        created_count += batch_created_count
        skipped_count += batch_skipped_count
        total_sent += batch_total_sent
        statuses.append(result_status)
        campaign_results.append(
            {
                "campaign_id": campaign_id,
                "campaign_name": campaign_rows[0].get("campaign_name", ""),
                "created_count": batch_created_count,
                "skipped_count": batch_skipped_count,
                "total_sent": batch_total_sent,
                "status": result_status,
            }
        )

    overall_status = "success" if all(status == "success" for status in statuses) else ",".join(sorted(set(statuses)))
    logger.info(
        "[Instantly] status=%s campaigns=%s total_sent=%s created_count=%s skipped_count=%s",
        overall_status,
        len(rows_by_campaign),
        total_sent,
        created_count,
        skipped_count,
    )
    return {
        "status": overall_status,
        "reason": "",
        "created_count": created_count,
        "skipped_count": skipped_count,
        "total_sent": total_sent,
        "campaign_results": campaign_results,
    }


def import_leads_to_heyreach(rows: list[dict[str, Any]], run_date: str, settings: Settings) -> dict[str, Any]:
    if not rows:
        return {
            "status": "skipped",
            "reason": "no_rows",
            "attempted_count": 0,
            "created_count": 0,
            "skipped_count": 0,
            "missing_linkedin_url_count": 0,
        }

    if not settings.heyreach_campaign_id:
        logger.info("[HeyReach] import skipped: HEYREACH_CAMPAIGN_ID missing")
        return {
            "status": "skipped",
            "reason": "missing_campaign_id",
            "attempted_count": 0,
            "created_count": 0,
            "skipped_count": 0,
            "missing_linkedin_url_count": 0,
        }

    if not settings.heyreach_api_key:
        logger.info("[HeyReach] import skipped: HEYREACH_API_KEY missing")
        return {
            "status": "skipped",
            "reason": "missing_api_key",
            "attempted_count": 0,
            "created_count": 0,
            "skipped_count": 0,
            "missing_linkedin_url_count": 0,
        }

    api_key_valid, api_key_reason = validate_heyreach_api_key(settings)
    if not api_key_valid:
        return {
            "status": "error",
            "reason": api_key_reason,
            "attempted_count": 0,
            "created_count": 0,
            "skipped_count": 0,
            "missing_linkedin_url_count": 0,
        }

    processed_leads = load_processed_heyreach_leads()
    prepared_leads: list[dict[str, Any]] = []
    processed_rows_to_append: list[dict[str, str]] = []
    skipped_count = 0
    missing_linkedin_url_count = 0

    for row in rows:
        linkedin_url = normalize_linkedin_url(row.get("linkedin_url", ""))
        if not linkedin_url:
            missing_linkedin_url_count += 1
            continue

        lead_key = build_heyreach_lead_key(settings.heyreach_campaign_id, linkedin_url)
        if not lead_key or lead_key in processed_leads:
            skipped_count += 1
            continue

        prepared_leads.append(
            {
                "firstName": row.get("first_name", ""),
                "lastName": row.get("last_name", ""),
                "email": row.get("email", ""),
                "company": row.get("company_name", ""),
                "position": row.get("role", ""),
                "linkedinUrl": linkedin_url,
            }
        )
        processed_rows_to_append.append(
            {
                "lead_key": lead_key,
                "campaign_id": settings.heyreach_campaign_id,
                "linkedin_url": linkedin_url,
            }
        )

    if not prepared_leads:
        return {
            "status": "skipped",
            "reason": "no_eligible_leads",
            "attempted_count": 0,
            "created_count": 0,
            "skipped_count": skipped_count,
            "missing_linkedin_url_count": missing_linkedin_url_count,
        }

    payload = {
        "campaignId": normalize_external_identifier(settings.heyreach_campaign_id),
        "leads": prepared_leads,
    }

    try:
        response = requests.post(
            HEYREACH_ADD_LEADS_TO_CAMPAIGN_URL,
            headers=build_heyreach_headers(settings),
            json=payload,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        result = response.json()
    except requests.HTTPError as exc:
        response = exc.response
        logger.warning(
            "[HeyReach] import request failed status=%s body=%s payload=%s",
            response.status_code if response is not None else "unknown",
            response.text if response is not None else "",
            payload,
        )
        return {
            "status": "error",
            "reason": "http_error",
            "attempted_count": len(prepared_leads),
            "created_count": 0,
            "skipped_count": skipped_count,
            "missing_linkedin_url_count": missing_linkedin_url_count,
        }
    except requests.RequestException as exc:
        logger.warning("[HeyReach] import request failed: %s payload=%s", exc, payload)
        return {
            "status": "error",
            "reason": "request_failed",
            "attempted_count": len(prepared_leads),
            "created_count": 0,
            "skipped_count": skipped_count,
            "missing_linkedin_url_count": missing_linkedin_url_count,
        }
    except ValueError:
        logger.warning("[HeyReach] import returned invalid JSON")
        return {
            "status": "error",
            "reason": "invalid_json",
            "attempted_count": len(prepared_leads),
            "created_count": 0,
            "skipped_count": skipped_count,
            "missing_linkedin_url_count": missing_linkedin_url_count,
        }

    result_status = str(result.get("status") or "success").lower()
    if result.get("error") or result_status in {"error", "failed"}:
        logger.warning("[HeyReach] import failed response: %s", result)
        return {
            "status": "error",
            "reason": "api_error",
            "attempted_count": len(prepared_leads),
            "created_count": 0,
            "skipped_count": skipped_count,
            "missing_linkedin_url_count": missing_linkedin_url_count,
        }

    created_count = len(prepared_leads)
    append_processed_heyreach_leads(processed_rows_to_append, run_date)
    logger.info(
        "[HeyReach] status=%s attempted_count=%s created_count=%s skipped_count=%s missing_linkedin_url_count=%s",
        result_status,
        len(prepared_leads),
        created_count,
        skipped_count,
        missing_linkedin_url_count,
    )
    return {
        "status": result_status,
        "reason": "",
        "attempted_count": len(prepared_leads),
        "created_count": created_count,
        "skipped_count": skipped_count,
        "missing_linkedin_url_count": missing_linkedin_url_count,
    }


def post_slack_summary(
    run_date: str,
    scheduler_source: str,
    raw_scanned: int,
    storeleads_start_page: int,
    storeleads_end_page: int,
    storeleads_pages_scanned: int,
    qualified_domains: int,
    new_domains_considered: int,
    previously_processed_domains: int,
    skipped_apollo_cooldown_domains: int,
    apollo_domains_queried: int,
    accepted_lead_target: int,
    apollo_hits: int,
    successful_contacts: int,
    instantly_import_result: dict[str, Any],
    heyreach_import_result: dict[str, Any],
    scheduler_status: dict[str, Any],
    settings: Settings,
) -> None:
    scheduler_lines: list[str] = []
    if scheduler_status.get("enabled"):
        scheduler_lines = [
            f"Limit {scheduler_status.get('daily_limit', 0)}",
            f"Used {scheduler_status.get('already_imported_today', 0)}",
            f"Remaining {scheduler_status.get('remaining_capacity', 0)}",
        ]

    sourcing_parts = [
        f"Scanned {raw_scanned}",
        f"Qualified {qualified_domains}",
        f"Fresh {new_domains_considered}",
    ]
    if previously_processed_domains:
        sourcing_parts.append(f"Skipped processed {previously_processed_domains}")
    if skipped_apollo_cooldown_domains:
        sourcing_parts.append(f"Skipped cooldown {skipped_apollo_cooldown_domains}")
    if storeleads_pages_scanned > 1 or storeleads_start_page or storeleads_end_page:
        sourcing_parts.append(
            f"Pages {storeleads_start_page}->{storeleads_end_page} ({storeleads_pages_scanned})"
        )

    contact_parts = [
        f"Apollo queried {apollo_domains_queried}",
        f"Positive domains {apollo_hits}",
        f"Contacts selected {successful_contacts}/{accepted_lead_target}",
    ]

    instantly_parts = [
        f"Status {instantly_import_result.get('status', 'unknown')}",
        f"Created {instantly_import_result.get('created_count', 0)}",
        f"Skipped {instantly_import_result.get('skipped_count', 0)}",
    ]

    lines = [
        "<!channel>",
        f"Lead scrape complete | {run_date} | {scheduler_source}",
        f"Apollo: {' | '.join(sourcing_parts)}",
        f"Contacts: {' | '.join(contact_parts)}",
        f"Instantly: {' | '.join(instantly_parts)}",
    ]
    if heyreach_import_result.get("status") not in {None, "disabled"} or heyreach_import_result.get("attempted_count", 0):
        heyreach_parts = [
            f"Status {heyreach_import_result.get('status', 'unknown')}",
            f"Attempted {heyreach_import_result.get('attempted_count', 0)}",
            f"Added {heyreach_import_result.get('created_count', 0)}",
        ]
        missing_linkedin = heyreach_import_result.get("missing_linkedin_url_count", 0)
        if missing_linkedin:
            heyreach_parts.append(f"Missing LinkedIn {missing_linkedin}")
        lines.append(f"HeyReach: {' | '.join(heyreach_parts)}")
    if scheduler_lines:
        lines.append(f"Pacing: {' | '.join(scheduler_lines)}")
    lines.append("CSV attached below.")

    message_text = "\n".join(lines)

    response = requests.post(
        SLACK_CHAT_POST_MESSAGE_URL,
        headers={**build_slack_headers(settings), "Content-Type": "application/json"},
        json={
            "channel": settings.slack_channel_id,
            "text": message_text,
        },
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()


def execute_lead_build(payload: ICPBuildRequest, *, scheduler_source: str, run_id: str = "") -> LeadBuildExecutionResult:
    settings = load_settings()
    validate_required_settings(settings)

    processed_domains = load_processed_domains()
    if run_id:
        update_lead_run_stage(run_id, "collecting_domains")
    collection_result = collect_domains(
        payload.max_domains,
        settings,
        processed_domains,
    )
    accepted_lead_target = accepted_lead_target_per_run()
    max_apollo_domains_this_run = effective_max_apollo_domains_per_run()
    max_apollo_org_pages_this_run = effective_max_apollo_org_pages(payload.max_domains)

    logger.info(
        "[Run] scheduler_source=%s date=%s max_domains=%s raw_scanned=%s start_page=%s end_page=%s pages_scanned=%s icp_matches=%s new_domains_considered=%s skipped_processed=%s skipped_apollo_cooldown=%s accepted_lead_target=%s max_apollo_domains=%s daily_new_lead_limit=%s weekday_only=%s",
        scheduler_source,
        payload.date,
        payload.max_domains,
        collection_result.raw_scanned,
        collection_result.start_page,
        collection_result.end_page,
        collection_result.pages_scanned,
        collection_result.qualified_matches_total,
        len(collection_result.domains),
        collection_result.skipped_processed_domains,
        collection_result.skipped_apollo_cooldown_domains,
        accepted_lead_target,
        max_apollo_domains_this_run,
        DAILY_NEW_LEAD_LIMIT,
        ENABLE_WEEKDAY_ONLY_IMPORTS,
    )

    if run_id:
        update_lead_run_stage(
            run_id,
            "enriching_contacts",
            summary_patch={
                "organizations_scanned": collection_result.raw_scanned,
                "icp_matches": collection_result.qualified_matches_total,
                "new_domains_considered": len(collection_result.domains),
            },
        )
    rows_result = build_csv_rows(
        collection_result.domains,
        payload.date,
        settings,
    )
    upsert_apollo_attempts(rows_result.apollo_attempt_rows)

    if run_id:
        update_lead_run_stage(
            run_id,
            "distributing_leads",
            summary_patch={
                "apollo_domains_attempted": rows_result.apollo_domains_attempted,
                "apollo_contacts_found": rows_result.apollo_hits,
                "personal_contacts_found": rows_result.successful_contacts,
            },
        )
    instantly_rows, scheduler_status = apply_daily_import_limit(rows_result.instantly_rows, payload.date)
    allowed_emails = {row.get("email", "") for row in instantly_rows if row.get("email")}
    linkedin_rows = [row for row in rows_result.linkedin_rows if row.get("email", "") in allowed_emails]
    successful_contacts = len(instantly_rows)
    logger.info(
        "[Scheduler] source=%s run_date=%s max_domains=%s status=%s daily_limit=%s already_imported_today=%s remaining_capacity=%s selected_contacts=%s accepted_lead_target=%s apollo_domains_attempted=%s apollo_org_page_budget=%s",
        scheduler_source,
        payload.date,
        payload.max_domains,
        scheduler_status.get("status", "unknown"),
        scheduler_status.get("daily_limit", 0),
        scheduler_status.get("already_imported_today", 0),
        scheduler_status.get("remaining_capacity", 0),
        successful_contacts,
        accepted_lead_target,
        rows_result.apollo_domains_attempted,
        max_apollo_org_pages_this_run,
    )

    instantly_csv = rows_to_csv(instantly_rows)
    exported_domains = {normalize_domain(row.get("website", "")) for row in instantly_rows if row.get("website")}
    instantly_import_result = import_leads_to_instantly(instantly_rows, settings)
    heyreach_import_result = import_leads_to_heyreach(instantly_rows, payload.date, settings)
    heyreach_lead_rows = [
        {
            "lead_key": build_heyreach_lead_key(settings.heyreach_campaign_id, row.get("linkedin_url", "")),
            "campaign_id": settings.heyreach_campaign_id,
            "linkedin_url": row.get("linkedin_url", ""),
        }
        for row in instantly_rows
        if build_heyreach_lead_key(settings.heyreach_campaign_id, row.get("linkedin_url", ""))
    ]
    upsert_lead_rows(run_id or f"inline-{payload.date}", instantly_rows, heyreach_lead_rows)
    if instantly_rows and instantly_import_result.get("status") != "error":
        append_processed_domains(exported_domains, payload.date)
    created_count = int(instantly_import_result.get("created_count", 0) or 0)
    if created_count > 0:
        append_daily_import_count(datetime.now().date().isoformat(), created_count, run_id=run_id)

    post_slack_summary(
        run_date=payload.date,
        scheduler_source=scheduler_source,
        raw_scanned=collection_result.raw_scanned,
        storeleads_start_page=collection_result.start_page,
        storeleads_end_page=collection_result.end_page,
        storeleads_pages_scanned=collection_result.pages_scanned,
        qualified_domains=collection_result.qualified_matches_total,
        new_domains_considered=len(collection_result.domains),
        previously_processed_domains=collection_result.skipped_processed_domains,
        skipped_apollo_cooldown_domains=collection_result.skipped_apollo_cooldown_domains,
        apollo_domains_queried=rows_result.apollo_domains_attempted,
        accepted_lead_target=accepted_lead_target,
        apollo_hits=rows_result.apollo_hits,
        successful_contacts=successful_contacts,
        instantly_import_result=instantly_import_result,
        heyreach_import_result=heyreach_import_result,
        scheduler_status=scheduler_status,
        settings=settings,
    )

    if instantly_rows:
        upload_file_to_slack(f"instantly_upload_{payload.date}.csv", instantly_csv, settings)

    return LeadBuildExecutionResult(
        instantly_csv=instantly_csv,
        instantly_rows=instantly_rows,
        raw_scanned=collection_result.raw_scanned,
        qualified_domains_count=len(collection_result.domains),
        qualified_matches_total=collection_result.qualified_matches_total,
        previously_processed_domains=collection_result.skipped_processed_domains,
        skipped_apollo_cooldown_domains=collection_result.skipped_apollo_cooldown_domains,
        storeleads_start_page=collection_result.start_page,
        storeleads_end_page=collection_result.end_page,
        storeleads_pages_scanned=collection_result.pages_scanned,
        apollo_domains_queried=rows_result.apollo_domains_attempted,
        accepted_lead_target=accepted_lead_target,
        apollo_hits=rows_result.apollo_hits,
        successful_contacts=successful_contacts,
        scheduler_status=scheduler_status,
        instantly_import_result=instantly_import_result,
        heyreach_import_result=heyreach_import_result,
    )


def lead_run_summary_from_result(result: LeadBuildExecutionResult) -> dict[str, Any]:
    return {
        "organizations_scanned": result.raw_scanned,
        "domains_scanned": result.raw_scanned,
        "icp_matches": result.qualified_matches_total,
        "new_domains_considered": result.qualified_domains_count,
        "previously_processed_domains": result.previously_processed_domains,
        "skipped_apollo_cooldown_domains": result.skipped_apollo_cooldown_domains,
        "apollo_org_start_page": result.storeleads_start_page,
        "apollo_org_end_page": result.storeleads_end_page,
        "apollo_org_pages_scanned": result.storeleads_pages_scanned,
        "apollo_domains_attempted": result.apollo_domains_queried,
        "accepted_lead_target": result.accepted_lead_target,
        "apollo_contacts_found": result.apollo_hits,
        "personal_contacts_found": result.successful_contacts,
        "instantly_import_result": result.instantly_import_result,
        "heyreach_import_result": result.heyreach_import_result,
        "scheduler_status": result.scheduler_status,
        "has_csv": bool(result.instantly_rows),
    }


def enqueue_lead_build(payload: ICPBuildRequest, *, scheduler_source: str) -> str:
    run_id = create_lead_run(
        trigger_source=scheduler_source,
        run_date=payload.date,
        max_domains=payload.max_domains,
        request_payload=payload.model_dump(),
    )

    def _worker() -> None:
        try:
            mark_lead_run_started(run_id, "collecting_domains")
            result = execute_lead_build(payload, scheduler_source=scheduler_source, run_id=run_id)
            summary = lead_run_summary_from_result(result)
            complete_lead_run(run_id, summary=summary, csv_content=result.instantly_csv if result.instantly_rows else "")
        except Exception as exc:
            logger.exception("[LeadRun] run failed run_id=%s", run_id)
            fail_lead_run(
                run_id,
                stage="failed",
                error_message=str(exc),
                summary_patch={"error_type": type(exc).__name__},
            )
            raise
        finally:
            ACTIVE_LEAD_RUNS.pop(run_id, None)

    future = LEAD_RUN_EXECUTOR.submit(_worker)
    ACTIVE_LEAD_RUNS[run_id] = future
    return run_id


def fetch_lead_run_status(run_id: str) -> dict[str, Any] | None:
    return get_lead_run(run_id)


# ========= ROUTES =========
@app.get("/admin/login", response_class=HTMLResponse)
def admin_login_page(request: Request) -> Response:
    admin_settings = load_admin_dashboard_settings()
    if not admin_login_enabled(admin_settings):
        raise HTTPException(status_code=503, detail="Admin dashboard is not configured. Set ADMIN_DASHBOARD_PASSWORD.")
    token = request.cookies.get(admin_settings.admin_cookie_name, "")
    if validate_admin_session_token(admin_settings, token):
        return RedirectResponse(url="/admin", status_code=302)
    return HTMLResponse(render_login_page())


@app.post("/admin/login", response_class=HTMLResponse)
async def admin_login_submit(request: Request) -> Response:
    admin_settings = load_admin_dashboard_settings()
    if not admin_login_enabled(admin_settings):
        raise HTTPException(status_code=503, detail="Admin dashboard is not configured. Set ADMIN_DASHBOARD_PASSWORD.")
    body = (await request.body()).decode("utf-8")
    password = parse_qs(body).get("password", [""])[0]
    if not verify_admin_password(admin_settings, password):
        return HTMLResponse(render_login_page(error_message="Incorrect password."), status_code=401)

    response = RedirectResponse(url="/admin", status_code=302)
    response.set_cookie(
        value=create_admin_session_token(admin_settings),
        **_admin_cookie_options(request, admin_settings),
    )
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
    dashboard = fetch_remote_dashboard_data()
    return HTMLResponse(render_dashboard_page(dashboard))


@app.get("/admin/executive", response_class=HTMLResponse)
def admin_executive_dashboard(request: Request) -> Response:
    admin_settings = load_admin_dashboard_settings()
    if not admin_login_enabled(admin_settings):
        raise HTTPException(status_code=503, detail="Admin dashboard is not configured. Set ADMIN_DASHBOARD_PASSWORD first.")

    token = request.cookies.get(admin_settings.admin_cookie_name, "")
    if not validate_admin_session_token(admin_settings, token):
        return RedirectResponse(url="/admin/login", status_code=302)

    executive = fetch_remote_executive_data()
    return HTMLResponse(render_executive_page(executive))


@app.post("/admin/api/run-lead-build", response_model=None)
async def admin_run_lead_build(request: Request) -> Response:
    admin_settings = load_admin_dashboard_settings()
    token = request.cookies.get(admin_settings.admin_cookie_name, "")
    if not validate_admin_session_token(admin_settings, token):
        return JSONResponse(status_code=401, content={"detail": "Admin login required."})

    payload = await request.json()
    build_request = ICPBuildRequest(**payload)
    try:
        result = execute_lead_build(build_request, scheduler_source="admin_dashboard")
    except HTTPException as exc:
        return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

    if not result.instantly_rows:
        return JSONResponse(
            status_code=200,
            content={
                "status": "ok",
                "message": "No valid personal contacts found for this run.",
                "organizations_scanned": result.raw_scanned,
                "domains_scanned": result.raw_scanned,
                "icp_matches": result.qualified_matches_total,
                "new_domains_considered": result.qualified_domains_count,
                "previously_processed_domains": result.previously_processed_domains,
                "skipped_apollo_cooldown_domains": result.skipped_apollo_cooldown_domains,
                "apollo_org_start_page": result.storeleads_start_page,
                "apollo_org_end_page": result.storeleads_end_page,
                "apollo_org_pages_scanned": result.storeleads_pages_scanned,
                "storeleads_start_page": result.storeleads_start_page,
                "storeleads_end_page": result.storeleads_end_page,
                "storeleads_pages_scanned": result.storeleads_pages_scanned,
                "apollo_domains_attempted": result.apollo_domains_queried,
                "accepted_lead_target": result.accepted_lead_target,
                "apollo_contacts_found": result.apollo_hits,
                "personal_contacts_found": result.successful_contacts,
            },
        )

    return Response(
        content=result.instantly_csv,
        media_type="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="instantly_upload_{build_request.date}.csv"'
        },
    )


@app.post("/admin/api/sync-dashboard")
def admin_sync_dashboard(request: Request) -> JSONResponse:
    admin_settings = load_admin_dashboard_settings()
    token = request.cookies.get(admin_settings.admin_cookie_name, "")
    if not validate_admin_session_token(admin_settings, token):
        return JSONResponse(status_code=401, content={"detail": "Admin login required."})

    try:
        result = sync_remote_dashboard_sources()
    except Exception as exc:
        logger.exception("[AdminDashboard] sync failed")
        return JSONResponse(
            status_code=500,
            content={"detail": "Dashboard sync failed.", "error": str(exc)},
        )
    return JSONResponse(
        status_code=200,
        content={"status": "ok", "message": str(result.get("message", "Dashboard sync completed.")), "details": result},
    )


@app.post("/admin/api/create-gmail-drafts")
async def admin_create_gmail_drafts(
    request: Request,
    contacts_csv: UploadFile = File(...),
    sales_objective: str = Form(default=""),
    subject_template: str = Form(default=""),
    body_template: str = Form(default=""),
    dry_run: bool = Form(default=False),
) -> JSONResponse:
    admin_settings = load_admin_dashboard_settings()
    token = request.cookies.get(admin_settings.admin_cookie_name, "")
    if not validate_admin_session_token(admin_settings, token):
        return JSONResponse(status_code=401, content={"detail": "Admin login required."})

    file_bytes = await contacts_csv.read()
    if not file_bytes:
        return JSONResponse(status_code=400, content={"detail": "Upload a CSV file with at least one contact row."})

    try:
        status_code, payload = _post_sales_support_multipart(
            "/api/admin/gmail-drafts",
            data_items=[
                ("sales_objective", sales_objective),
                ("subject_template", subject_template),
                ("body_template", body_template),
                ("dry_run", "true" if dry_run else "false"),
            ],
            files_payload=[
                (
                    "contacts_csv",
                    (
                        contacts_csv.filename or "contacts.csv",
                        file_bytes,
                        contacts_csv.content_type or "text/csv",
                    ),
                )
            ],
        )
    except Exception as exc:
        logger.exception("[AdminDashboard] gmail draft creation failed")
        return JSONResponse(
            status_code=500,
            content={"detail": "Bulk draft creation failed.", "error": str(exc)},
        )

    return JSONResponse(status_code=status_code, content=payload)


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


@app.post("/admin/api/generate-deck")
async def admin_generate_deck_proxy(
    request: Request,
    competitor_xray_csv: UploadFile = File(...),
    keyword_xray_csv: UploadFile | None = File(default=None),
    target_product_input: str = Form(default=""),
    channels: list[str] = Form(default=[]),
    creative_mockup_url: str = Form(default=""),
    case_study_url: str = Form(default=""),
    offers: list[str] = Form(default=[]),
    offer_payload_json: str = Form(default=""),
    include_recommended_plan: bool = Form(default=True),
) -> JSONResponse:
    admin_settings = load_admin_dashboard_settings()
    token = request.cookies.get(admin_settings.admin_cookie_name, "")
    if not validate_admin_session_token(admin_settings, token):
        return JSONResponse(status_code=401, content={"detail": "Admin login required."})

    competitor_file_bytes = await competitor_xray_csv.read()
    keyword_file_bytes = await keyword_xray_csv.read() if keyword_xray_csv is not None else None
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
            ],
            files_payload=[
                (
                    "competitor_xray_csv",
                    (
                        competitor_xray_csv.filename or "competitors.csv",
                        competitor_file_bytes,
                        competitor_xray_csv.content_type or "text/csv",
                    ),
                ),
                *(
                    [
                        (
                            "keyword_xray_csv",
                            (
                                keyword_xray_csv.filename or "keywords.csv",
                                keyword_file_bytes or b"",
                                keyword_xray_csv.content_type or "text/csv",
                            ),
                        )
                    ]
                    if keyword_xray_csv is not None
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


@app.get("/decks/{deck_slug}/{run_id}/{token}")
def public_deck_proxy(request: Request, deck_slug: str, run_id: int, token: str) -> Response:
    admin_settings = load_admin_dashboard_settings()
    if not admin_settings.sales_support_agent_url:
        return JSONResponse(status_code=500, content={"detail": "Sales support agent URL is not configured on this service."})
    backend_url = f"{admin_settings.sales_support_agent_url}/decks/{quote(deck_slug, safe='')}/{run_id}/{quote(token, safe='')}"
    if request.url.query:
        backend_url = f"{backend_url}?{request.url.query}"
    response = requests.get(backend_url, timeout=REQUEST_TIMEOUT_SECONDS)
    content_type = response.headers.get("Content-Type", "text/html; charset=utf-8")
    return Response(
        content=response.content,
        status_code=response.status_code,
        media_type=content_type.split(";")[0],
        headers={
            "Content-Type": content_type,
            "Cache-Control": "private, max-age=300",
            "Content-Security-Policy": "default-src 'self' 'unsafe-inline' data: https:; img-src 'self' data: https:; media-src https: data:; frame-ancestors *;",
        },
    )


@app.get("/")
def home() -> RedirectResponse:
    return RedirectResponse(url="/admin/login", status_code=302)


@app.get("/api/status")
def api_status() -> dict[str, str]:
    return {"status": "lead engine running"}


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/run-lead-build", response_model=None)
def run(payload: ICPBuildRequest, request: Request) -> JSONResponse | StreamingResponse:
    scheduler_source = detect_scheduler_source(request)
    enqueue_requested = (request.query_params.get("async") or "").strip().lower() in {"1", "true", "yes", "on"}

    if enqueue_requested:
        run_id = enqueue_lead_build(payload, scheduler_source=scheduler_source)
        return JSONResponse(
            status_code=202,
            content={
                "status": "queued",
                "message": "Lead build queued.",
                "details": {
                    "run_id": run_id,
                    "poll_url": f"/lead-runs/{run_id}",
                    "download_url": f"/lead-runs/{run_id}/download",
                },
            },
        )

    try:
        result = execute_lead_build(payload, scheduler_source=scheduler_source)

        if not result.instantly_rows:
            return JSONResponse(
                status_code=200,
                content={
                    "status": "ok",
                    "message": "No valid personal contacts found for this run.",
                    "organizations_scanned": result.raw_scanned,
                    "domains_scanned": result.raw_scanned,
                    "icp_matches": result.qualified_matches_total,
                    "new_domains_considered": result.qualified_domains_count,
                    "previously_processed_domains": result.previously_processed_domains,
                    "skipped_apollo_cooldown_domains": result.skipped_apollo_cooldown_domains,
                    "apollo_org_start_page": result.storeleads_start_page,
                    "apollo_org_end_page": result.storeleads_end_page,
                    "apollo_org_pages_scanned": result.storeleads_pages_scanned,
                    "storeleads_start_page": result.storeleads_start_page,
                    "storeleads_end_page": result.storeleads_end_page,
                    "storeleads_pages_scanned": result.storeleads_pages_scanned,
                    "apollo_domains_attempted": result.apollo_domains_queried,
                    "accepted_lead_target": result.accepted_lead_target,
                    "apollo_contacts_found": result.apollo_hits,
                    "personal_contacts_found": result.successful_contacts,
                },
            )

        return StreamingResponse(
            iter([result.instantly_csv]),
            media_type="text/csv",
            headers={
                "Content-Disposition": f'attachment; filename="instantly_upload_{payload.date}.csv"'
            },
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("[Run] unexpected error")
        return JSONResponse(
            status_code=500,
            content={"error_type": type(exc).__name__, "detail": str(exc)},
        )


@app.get("/lead-runs/{run_id}")
def lead_run_status(run_id: str) -> JSONResponse:
    payload = fetch_lead_run_status(run_id)
    if payload is None:
        return JSONResponse(status_code=404, content={"detail": "Lead run not found."})
    return JSONResponse(status_code=200, content={"status": "ok", "message": "Lead run status loaded.", "details": payload})


@app.get("/lead-runs/{run_id}/download", response_model=None)
def lead_run_download(run_id: str) -> Response:
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
