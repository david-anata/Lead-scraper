"""Configuration for the sales support agent."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional


ACTIVE_FOLLOW_UP_STATUSES = (
    "new lead",
    "CONTACTED COLD",
    "CONTACTED WARM",
    "WORKING QUALIFIED",
    "WORKING NEEDS OFFER",
    "WORKING OFFERED",
    "WORKING NEGOTIATING",
)

INACTIVE_STATUSES = (
    "WON - ONBOARDING",
    "WON - ACTIVE",
    "LOST",
    "LOST - NOT QUALIFIED",
    "WON - CANCELED",
)


@dataclass(frozen=True)
class StatusPolicy:
    first_action_days: Optional[int] = None
    due_days: Optional[int] = None
    overdue_days: Optional[int] = None
    use_follow_up_date: bool = False


DEFAULT_STATUS_POLICIES: dict[str, StatusPolicy] = {
    "new lead": StatusPolicy(first_action_days=1, due_days=2, overdue_days=3),
    "CONTACTED COLD": StatusPolicy(first_action_days=1, due_days=2, overdue_days=3),
    "CONTACTED WARM": StatusPolicy(first_action_days=1, due_days=1, overdue_days=2),
    "WORKING QUALIFIED": StatusPolicy(due_days=2, overdue_days=3),
    "WORKING NEEDS OFFER": StatusPolicy(due_days=1, overdue_days=2),
    "WORKING OFFERED": StatusPolicy(due_days=4, overdue_days=5),
    "WORKING NEGOTIATING": StatusPolicy(due_days=2, overdue_days=3),
    "FOLLOW UP": StatusPolicy(due_days=0, overdue_days=1, use_follow_up_date=True),
}

DEFAULT_CANVA_SCOPES = (
    "design:content:write",
    "design:meta:read",
    "brandtemplate:content:read",
    "profile:read",
)


@dataclass(frozen=True)
class ManagedFieldSettings:
    next_follow_up_date_field_id: str = ""
    communication_summary_field_id: str = ""
    last_meeting_outcome_field_id: str = ""
    recommended_next_action_field_id: str = ""
    last_meaningful_touch_field_id: str = ""
    last_outbound_field_id: str = ""
    last_inbound_field_id: str = ""


@dataclass(frozen=True)
class GmailMailboxAccount:
    account_key: str
    label: str
    access_token: str = ""
    client_id: str = ""
    client_secret: str = ""
    refresh_token: str = ""
    user_id: str = "me"
    poll_query: str = "newer_than:2d"
    poll_max_messages: int = 25
    source_domains: tuple[str, ...] = ()


@dataclass(frozen=True)
class Settings:
    app_name: str
    admin_username: str
    admin_password: str
    admin_session_secret: str
    admin_cookie_name: str
    admin_session_ttl_hours: int
    dashboard_auto_sync_enabled: bool
    dashboard_auto_sync_max_age_minutes: int
    clickup_api_token: str
    clickup_base_url: str
    clickup_list_id: str
    clickup_request_timeout_seconds: int
    clickup_discovery_sample_size: int
    stale_lead_scan_max_tasks: int
    stale_lead_scan_sync_max_tasks: int
    stale_lead_slack_digest_enabled: bool
    stale_lead_slack_digest_mention_channel: bool
    stale_lead_slack_digest_max_items: int
    stale_lead_immediate_alert_urgencies: tuple[str, ...]
    daily_digest_enabled: bool
    daily_digest_email_to: tuple[str, ...]
    daily_digest_email_cc: tuple[str, ...]
    daily_digest_subject_prefix: str
    daily_digest_max_items: int
    slack_bot_token: str
    slack_channel_id: str
    slack_assignee_map: dict[str, str]
    slack_immediate_event_types: tuple[str, ...]
    gmail_api_base_url: str
    gmail_oauth_token_url: str
    gmail_access_token: str
    gmail_client_id: str
    gmail_client_secret: str
    gmail_refresh_token: str
    gmail_user_id: str
    gmail_poll_query: str
    gmail_poll_max_messages: int
    gmail_source_domains: tuple[str, ...]
    gmail_mailbox_accounts: tuple[GmailMailboxAccount, ...]
    lead_build_url: str
    sales_agent_db_url: str
    internal_api_key: str
    discovery_snapshot_path: Path
    fulfillment_cs_reports_dir: Path
    website_ops_root: Path
    website_ops_site_urls: tuple[str, ...]
    website_ops_execute_approved: bool
    use_due_date_for_follow_up: bool
    openai_api_key: str
    openai_model: str
    instantly_webhook_secret: str
    instantly_webhook_secret_header: str
    instantly_webhook_allowed_event_types: tuple[str, ...]
    google_sheets_api_base_url: str
    google_sheets_spreadsheet_id: str
    google_sheets_sales_range: str
    google_service_account_json: str
    canva_api_base_url: str
    canva_authorize_url: str
    canva_token_url: str
    canva_client_id: str
    canva_client_secret: str
    canva_redirect_uri: str
    canva_brand_template_id: str
    canva_scopes: tuple[str, ...]
    canva_token_secret: str
    deck_canva_poll_interval_seconds: int
    deck_canva_poll_attempts: int
    deck_competitor_required_columns: tuple[str, ...]
    deck_competitor_allowed_columns: tuple[str, ...]
    deck_required_template_fields: tuple[str, ...]
    shared_brand_package_path: Path
    deck_public_base_url: str
    shopify_request_timeout_seconds: int
    shopify_user_agent: str
    amazon_sp_api_base_url: str
    amazon_sp_api_region: str
    amazon_sp_api_marketplace_id: str
    amazon_sp_api_lwa_client_id: str
    amazon_sp_api_lwa_client_secret: str
    amazon_sp_api_refresh_token: str
    amazon_sp_api_aws_access_key_id: str
    amazon_sp_api_aws_secret_access_key: str
    amazon_sp_api_aws_session_token: str
    google_oauth_client_id: str = ""
    google_oauth_client_secret: str = ""
    google_oauth_allowed_domain: str = "anatainc.com"
    admin_role_map: dict[str, str] = field(default_factory=dict)
    admin_default_role: str = "ops"
    active_statuses: tuple[str, ...] = field(default_factory=lambda: ACTIVE_FOLLOW_UP_STATUSES)
    inactive_statuses: tuple[str, ...] = field(default_factory=lambda: INACTIVE_STATUSES)
    managed_fields: ManagedFieldSettings = field(default_factory=ManagedFieldSettings)
    status_policies: dict[str, StatusPolicy] = field(default_factory=lambda: DEFAULT_STATUS_POLICIES)


def _parse_bool(value: str, default: bool = False) -> bool:
    if not value:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _parse_json_object(value: str) -> dict[str, str]:
    raw = (value or "").strip()
    if not raw:
        return {}
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError("Expected a JSON object.")
    return {str(key): str(val) for key, val in parsed.items()}


def _parse_csv_tuple(value: str, default: tuple[str, ...] = ()) -> tuple[str, ...]:
    raw = (value or "").strip()
    if not raw:
        return default
    return tuple(item.strip() for item in raw.split(",") if item.strip())


def _parse_source_domains(raw_value: Any, default: tuple[str, ...]) -> tuple[str, ...]:
    if isinstance(raw_value, (list, tuple)):
        values = tuple(str(item).strip() for item in raw_value if str(item).strip())
        return values or default
    return _parse_csv_tuple(str(raw_value or ""), default=default)


def _default_gmail_mailbox_accounts(
    *,
    access_token: str,
    client_id: str,
    client_secret: str,
    refresh_token: str,
    user_id: str,
    poll_query: str,
    poll_max_messages: int,
    source_domains: tuple[str, ...],
) -> tuple[GmailMailboxAccount, ...]:
    if not any([access_token, client_id, client_secret, refresh_token]):
        return ()
    label = user_id if user_id and user_id != "me" else "Primary inbox"
    account_key = re.sub(r"[^a-z0-9]+", "-", label.lower()).strip("-") or "primary"
    return (
        GmailMailboxAccount(
            account_key=account_key,
            label=label,
            access_token=access_token,
            client_id=client_id,
            client_secret=client_secret,
            refresh_token=refresh_token,
            user_id=user_id or "me",
            poll_query=poll_query or "newer_than:2d",
            poll_max_messages=max(1, poll_max_messages),
            source_domains=source_domains,
        ),
    )


def _parse_gmail_mailbox_accounts(
    raw_value: str,
    *,
    fallback_accounts: tuple[GmailMailboxAccount, ...],
    default_poll_query: str,
    default_poll_max_messages: int,
    default_source_domains: tuple[str, ...],
) -> tuple[GmailMailboxAccount, ...]:
    raw = (raw_value or "").strip()
    if not raw:
        return fallback_accounts

    parsed = json.loads(raw)
    if not isinstance(parsed, list):
        raise ValueError("GMAIL_INBOXES_JSON must be a JSON array.")

    accounts: list[GmailMailboxAccount] = []
    for index, item in enumerate(parsed):
        if not isinstance(item, dict):
            raise ValueError("Each GMAIL_INBOXES_JSON item must be a JSON object.")
        label = str(item.get("label") or item.get("owner_name") or item.get("email") or item.get("user_id") or f"Inbox {index + 1}").strip()
        account_key = str(item.get("account_key") or item.get("key") or "").strip()
        if not account_key:
            account_key = re.sub(r"[^a-z0-9]+", "-", label.lower()).strip("-") or f"inbox-{index + 1}"
        accounts.append(
            GmailMailboxAccount(
                account_key=account_key,
                label=label,
                access_token=str(item.get("access_token") or "").strip(),
                client_id=str(item.get("client_id") or "").strip(),
                client_secret=str(item.get("client_secret") or "").strip(),
                refresh_token=str(item.get("refresh_token") or "").strip(),
                user_id=str(item.get("user_id") or "me").strip() or "me",
                poll_query=str(item.get("poll_query") or default_poll_query).strip() or default_poll_query,
                poll_max_messages=max(1, int(item.get("poll_max_messages", default_poll_max_messages) or default_poll_max_messages)),
                source_domains=_parse_source_domains(item.get("source_domains"), default_source_domains),
            )
        )
    return tuple(accounts)


def normalize_status_key(value: str) -> str:
    return " ".join((value or "").strip().lower().split())


def build_normalized_status_policies(status_policies: dict[str, StatusPolicy]) -> dict[str, StatusPolicy]:
    return {normalize_status_key(status): policy for status, policy in status_policies.items()}


def is_closed_pipeline_status(status_key: str, inactive_statuses: tuple[str, ...]) -> bool:
    normalized = normalize_status_key(status_key)
    if not normalized:
        return False
    if normalized in inactive_statuses:
        return True
    closed_markers = ("won", "lost", "canceled", "cancelled", "closed", "archive", "archived")
    return any(marker in normalized for marker in closed_markers)


def is_active_pipeline_status(status: str, *, active_statuses: tuple[str, ...], inactive_statuses: tuple[str, ...]) -> bool:
    normalized = normalize_status_key(status)
    if not normalized:
        return False
    if normalized in active_statuses:
        return True
    return not is_closed_pipeline_status(normalized, inactive_statuses)


def status_policy_for(status: str, status_policies: dict[str, StatusPolicy]) -> StatusPolicy:
    normalized = normalize_status_key(status)
    if normalized in status_policies:
        return status_policies[normalized]

    if "follow up" in normalized:
        return status_policies[normalize_status_key("FOLLOW UP")]
    if "negoti" in normalized:
        return status_policies[normalize_status_key("WORKING NEGOTIATING")]
    if "needs offer" in normalized or ("offer" in normalized and "need" in normalized):
        return status_policies[normalize_status_key("WORKING NEEDS OFFER")]
    if "offered" in normalized or "offer sent" in normalized:
        return status_policies[normalize_status_key("WORKING OFFERED")]
    if "qualif" in normalized:
        return status_policies[normalize_status_key("WORKING QUALIFIED")]
    if "contacted" in normalized or "contact" in normalized:
        return status_policies[normalize_status_key("CONTACTED COLD")]
    if "new" in normalized or "lead" in normalized:
        return status_policies[normalize_status_key("new lead")]
    return StatusPolicy(due_days=2, overdue_days=3)


def _default_db_url() -> str:
    runtime_dir = Path("runtime")
    runtime_dir.mkdir(parents=True, exist_ok=True)
    return f"sqlite:///{runtime_dir / 'sales_support_agent.sqlite3'}"


def load_settings() -> Settings:
    gmail_access_token = os.getenv("GMAIL_ACCESS_TOKEN", "").strip()
    gmail_client_id = os.getenv("GMAIL_CLIENT_ID", "").strip()
    gmail_client_secret = os.getenv("GMAIL_CLIENT_SECRET", "").strip()
    gmail_refresh_token = os.getenv("GMAIL_REFRESH_TOKEN", "").strip()
    gmail_user_id = os.getenv("GMAIL_USER_ID", "me").strip() or "me"
    gmail_poll_query = os.getenv("GMAIL_POLL_QUERY", "newer_than:2d").strip() or "newer_than:2d"
    gmail_poll_max_messages = int((os.getenv("GMAIL_POLL_MAX_MESSAGES", "25") or "25").strip())
    gmail_source_domains = _parse_csv_tuple(os.getenv("GMAIL_SOURCE_DOMAINS", "fulfil.com"), default=("fulfil.com",))
    fallback_gmail_accounts = _default_gmail_mailbox_accounts(
        access_token=gmail_access_token,
        client_id=gmail_client_id,
        client_secret=gmail_client_secret,
        refresh_token=gmail_refresh_token,
        user_id=gmail_user_id,
        poll_query=gmail_poll_query,
        poll_max_messages=gmail_poll_max_messages,
        source_domains=gmail_source_domains,
    )
    gmail_mailbox_accounts = _parse_gmail_mailbox_accounts(
        os.getenv("GMAIL_INBOXES_JSON", ""),
        fallback_accounts=fallback_gmail_accounts,
        default_poll_query=gmail_poll_query,
        default_poll_max_messages=gmail_poll_max_messages,
        default_source_domains=gmail_source_domains,
    )

    return Settings(
        app_name="sales-support-agent",
        admin_username=os.getenv("ADMIN_DASHBOARD_USERNAME", "admin").strip() or "admin",
        admin_password=os.getenv("ADMIN_DASHBOARD_PASSWORD", "").strip(),
        admin_session_secret=(
            os.getenv("ADMIN_DASHBOARD_SESSION_SECRET", "").strip()
            or os.getenv("SALES_AGENT_INTERNAL_API_KEY", "").strip()
            or "sales-support-agent-session-secret"
        ),
        admin_cookie_name=os.getenv("ADMIN_DASHBOARD_COOKIE_NAME", "sales_support_admin_session").strip() or "sales_support_admin_session",
        admin_session_ttl_hours=int((os.getenv("ADMIN_DASHBOARD_SESSION_TTL_HOURS", "24") or "24").strip()),
        dashboard_auto_sync_enabled=_parse_bool(os.getenv("ADMIN_DASHBOARD_AUTO_SYNC_ENABLED", "true"), default=True),
        dashboard_auto_sync_max_age_minutes=int((os.getenv("ADMIN_DASHBOARD_AUTO_SYNC_MAX_AGE_MINUTES", "30") or "30").strip()),
        clickup_api_token=(os.getenv("CLICKUP_API_TOKEN") or os.getenv("CLICKUP_API_KEY") or "").strip(),
        clickup_base_url=(os.getenv("CLICKUP_BASE_URL", "https://api.clickup.com/api/v2").strip() or "https://api.clickup.com/api/v2"),
        clickup_list_id=os.getenv("CLICKUP_LIST_ID", "").strip(),
        clickup_request_timeout_seconds=int((os.getenv("CLICKUP_REQUEST_TIMEOUT_SECONDS", "30") or "30").strip()),
        clickup_discovery_sample_size=int((os.getenv("CLICKUP_DISCOVERY_SAMPLE_SIZE", "10") or "10").strip()),
        stale_lead_scan_max_tasks=int((os.getenv("STALE_LEAD_SCAN_MAX_TASKS", "50") or "50").strip()),
        stale_lead_scan_sync_max_tasks=int((os.getenv("STALE_LEAD_SCAN_SYNC_MAX_TASKS", "100") or "100").strip()),
        stale_lead_slack_digest_enabled=_parse_bool(os.getenv("STALE_LEAD_SLACK_DIGEST_ENABLED", "false"), default=False),
        stale_lead_slack_digest_mention_channel=_parse_bool(
            os.getenv("STALE_LEAD_SLACK_DIGEST_MENTION_CHANNEL", "false"),
            default=False,
        ),
        stale_lead_slack_digest_max_items=int((os.getenv("STALE_LEAD_SLACK_DIGEST_MAX_ITEMS", "20") or "20").strip()),
        stale_lead_immediate_alert_urgencies=_parse_csv_tuple(
            os.getenv("STALE_LEAD_IMMEDIATE_ALERT_URGENCIES", ""),
            default=(),
        ),
        daily_digest_enabled=_parse_bool(os.getenv("DAILY_DIGEST_ENABLED", "true"), default=True),
        daily_digest_email_to=_parse_csv_tuple(os.getenv("DAILY_DIGEST_EMAIL_TO", ""), default=()),
        daily_digest_email_cc=_parse_csv_tuple(os.getenv("DAILY_DIGEST_EMAIL_CC", ""), default=()),
        daily_digest_subject_prefix=os.getenv("DAILY_DIGEST_SUBJECT_PREFIX", "[SDR Support]").strip() or "[SDR Support]",
        daily_digest_max_items=int((os.getenv("DAILY_DIGEST_MAX_ITEMS", "25") or "25").strip()),
        slack_bot_token=os.getenv("SLACK_BOT_TOKEN", "").strip(),
        slack_channel_id=os.getenv("SLACK_CHANNEL_ID", "").strip(),
        slack_assignee_map=_parse_json_object(os.getenv("SLACK_AE_MAP_JSON", "{}")),
        slack_immediate_event_types=_parse_csv_tuple(
            os.getenv("SLACK_IMMEDIATE_EVENT_TYPES", "inbound_reply_received,meeting_notes_missing"),
            default=("inbound_reply_received", "meeting_notes_missing"),
        ),
        gmail_api_base_url=os.getenv("GMAIL_API_BASE_URL", "https://gmail.googleapis.com/gmail/v1").strip() or "https://gmail.googleapis.com/gmail/v1",
        gmail_oauth_token_url=os.getenv("GMAIL_OAUTH_TOKEN_URL", "https://oauth2.googleapis.com/token").strip() or "https://oauth2.googleapis.com/token",
        gmail_access_token=gmail_access_token,
        gmail_client_id=gmail_client_id,
        gmail_client_secret=gmail_client_secret,
        gmail_refresh_token=gmail_refresh_token,
        gmail_user_id=gmail_user_id,
        gmail_poll_query=gmail_poll_query,
        gmail_poll_max_messages=gmail_poll_max_messages,
        gmail_source_domains=gmail_source_domains,
        gmail_mailbox_accounts=gmail_mailbox_accounts,
        lead_build_url=os.getenv("LEAD_BUILD_URL", "").strip().rstrip("/"),
        sales_agent_db_url=(os.getenv("SALES_AGENT_DB_URL", "").strip() or _default_db_url()),
        internal_api_key=os.getenv("SALES_AGENT_INTERNAL_API_KEY", "").strip(),
        discovery_snapshot_path=Path(
            os.getenv("CLICKUP_DISCOVERY_SNAPSHOT_PATH", "runtime/clickup_schema_snapshot.json").strip()
            or "runtime/clickup_schema_snapshot.json"
        ),
        fulfillment_cs_reports_dir=Path(
            os.getenv("FULFILLMENT_CS_REPORTS_DIR", "runtime/fulfillment_cs_reports").strip()
            or "runtime/fulfillment_cs_reports"
        ),
        website_ops_root=Path(
            os.getenv("WEBSITE_OPS_ROOT", "runtime/website_ops").strip()
            or "runtime/website_ops"
        ),
        website_ops_site_urls=_parse_csv_tuple(
            os.getenv(
                "WEBSITE_OPS_URLS",
                "https://anatainc.com/,https://anatainc.com/services/,https://anatainc.com/services/fulfillment/,https://anatainc.com/services/shipping/,https://anatainc.com/services/ai/,https://anatainc.com/services/advertising/,https://anatainc.com/contact/",
            ),
            default=(
                "https://anatainc.com/",
                "https://anatainc.com/services/",
                "https://anatainc.com/services/fulfillment/",
                "https://anatainc.com/services/shipping/",
                "https://anatainc.com/services/ai/",
                "https://anatainc.com/services/advertising/",
                "https://anatainc.com/contact/",
            ),
        ),
        website_ops_execute_approved=_parse_bool(os.getenv("WEBSITE_OPS_EXECUTE_APPROVED", "true"), default=True),
        use_due_date_for_follow_up=_parse_bool(os.getenv("CLICKUP_USE_DUE_DATE_FOR_FOLLOW_UP", "")),
        openai_api_key=os.getenv("OPENAI_API_KEY", "").strip(),
        openai_model=os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip() or "gpt-4o-mini",
        instantly_webhook_secret=os.getenv("INSTANTLY_WEBHOOK_SECRET", "").strip(),
        instantly_webhook_secret_header=(
            os.getenv("INSTANTLY_WEBHOOK_SECRET_HEADER", "X-Instantly-Webhook-Secret").strip()
            or "X-Instantly-Webhook-Secret"
        ),
        instantly_webhook_allowed_event_types=tuple(
            event_type.strip()
            for event_type in (
                os.getenv(
                    "INSTANTLY_WEBHOOK_ALLOWED_EVENT_TYPES",
                    "email_sent,reply_received,lead_meeting_booked,lead_meeting_completed,lead_interested,lead_not_interested,lead_neutral",
                )
                or ""
            ).split(",")
            if event_type.strip()
        ),
        google_sheets_api_base_url=(
            os.getenv("GOOGLE_SHEETS_API_BASE_URL", "https://sheets.googleapis.com/v4").strip()
            or "https://sheets.googleapis.com/v4"
        ),
        google_sheets_spreadsheet_id=os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID", "").strip(),
        google_sheets_sales_range=os.getenv("GOOGLE_SHEETS_SALES_RANGE", "").strip(),
        google_service_account_json=os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip(),
        canva_api_base_url=(os.getenv("CANVA_API_BASE_URL", "https://api.canva.com/rest/v1").strip() or "https://api.canva.com/rest/v1"),
        canva_authorize_url=(
            os.getenv("CANVA_AUTHORIZE_URL", "https://www.canva.com/api/oauth/authorize").strip()
            or "https://www.canva.com/api/oauth/authorize"
        ),
        canva_token_url=(os.getenv("CANVA_TOKEN_URL", "https://api.canva.com/rest/v1/oauth/token").strip() or "https://api.canva.com/rest/v1/oauth/token"),
        canva_client_id=os.getenv("CANVA_CLIENT_ID", "").strip(),
        canva_client_secret=os.getenv("CANVA_CLIENT_SECRET", "").strip(),
        canva_redirect_uri=os.getenv("CANVA_REDIRECT_URI", "").strip(),
        canva_brand_template_id=os.getenv("CANVA_BRAND_TEMPLATE_ID", "").strip(),
        canva_scopes=_parse_csv_tuple(
            os.getenv("CANVA_SCOPES", ",".join(DEFAULT_CANVA_SCOPES)),
            default=DEFAULT_CANVA_SCOPES,
        ),
        canva_token_secret=os.getenv("CANVA_TOKEN_SECRET", "").strip(),
        deck_canva_poll_interval_seconds=int((os.getenv("DECK_CANVA_POLL_INTERVAL_SECONDS", "2") or "2").strip()),
        deck_canva_poll_attempts=int((os.getenv("DECK_CANVA_POLL_ATTEMPTS", "15") or "15").strip()),
        deck_competitor_required_columns=_parse_csv_tuple(
            os.getenv("DECK_COMPETITOR_REQUIRED_COLUMNS", ""),
            default=(),
        ),
        deck_competitor_allowed_columns=_parse_csv_tuple(
            os.getenv("DECK_COMPETITOR_ALLOWED_COLUMNS", ""),
            default=(),
        ),
        deck_required_template_fields=_parse_csv_tuple(
            os.getenv("DECK_REQUIRED_TEMPLATE_FIELDS", ""),
            default=(),
        ),
        shared_brand_package_path=Path(
            os.getenv(
                "SHARED_BRAND_PACKAGE_PATH",
                str(Path(__file__).resolve().parents[1] / "shared" / "anata_brand"),
            ).strip()
            or str(Path(__file__).resolve().parents[1] / "shared" / "anata_brand")
        ),
        deck_public_base_url=(
            os.getenv("DECK_PUBLIC_BASE_URL", "https://agent.anatainc.com").strip().rstrip("/")
        ),
        shopify_request_timeout_seconds=int((os.getenv("SHOPIFY_REQUEST_TIMEOUT_SECONDS", "20") or "20").strip()),
        shopify_user_agent=(
            os.getenv("SHOPIFY_USER_AGENT", "anata-deck-generator/1.0").strip()
            or "anata-deck-generator/1.0"
        ),
        amazon_sp_api_base_url=(
            os.getenv("AMAZON_SP_API_BASE_URL", "https://sellingpartnerapi-na.amazon.com").strip()
            or "https://sellingpartnerapi-na.amazon.com"
        ),
        amazon_sp_api_region=(os.getenv("AMAZON_SP_API_REGION", "us-east-1").strip() or "us-east-1"),
        amazon_sp_api_marketplace_id=(os.getenv("AMAZON_SP_API_MARKETPLACE_ID", "ATVPDKIKX0DER").strip() or "ATVPDKIKX0DER"),
        amazon_sp_api_lwa_client_id=os.getenv("AMAZON_SP_API_LWA_CLIENT_ID", "").strip(),
        amazon_sp_api_lwa_client_secret=os.getenv("AMAZON_SP_API_LWA_CLIENT_SECRET", "").strip(),
        amazon_sp_api_refresh_token=os.getenv("AMAZON_SP_API_REFRESH_TOKEN", "").strip(),
        amazon_sp_api_aws_access_key_id=os.getenv("AMAZON_SP_API_AWS_ACCESS_KEY_ID", "").strip(),
        amazon_sp_api_aws_secret_access_key=os.getenv("AMAZON_SP_API_AWS_SECRET_ACCESS_KEY", "").strip(),
        amazon_sp_api_aws_session_token=os.getenv("AMAZON_SP_API_AWS_SESSION_TOKEN", "").strip(),
        google_oauth_client_id=os.getenv("GOOGLE_OAUTH_CLIENT_ID", "").strip(),
        google_oauth_client_secret=os.getenv("GOOGLE_OAUTH_CLIENT_SECRET", "").strip(),
        google_oauth_allowed_domain=(os.getenv("GOOGLE_OAUTH_ALLOWED_DOMAIN", "anatainc.com").strip() or "anatainc.com"),
        admin_role_map=_parse_json_object(os.getenv("ADMIN_ROLE_MAP", "{}")),
        admin_default_role=(os.getenv("ADMIN_DEFAULT_ROLE", "ops").strip() or "ops"),
        active_statuses=tuple(
            normalize_status_key(status)
            for status in ACTIVE_FOLLOW_UP_STATUSES
        ),
        inactive_statuses=tuple(
            normalize_status_key(status)
            for status in INACTIVE_STATUSES
        ),
        status_policies=build_normalized_status_policies(DEFAULT_STATUS_POLICIES),
        managed_fields=ManagedFieldSettings(
            next_follow_up_date_field_id=os.getenv("CLICKUP_NEXT_FOLLOW_UP_FIELD_ID", "").strip(),
            communication_summary_field_id=os.getenv("CLICKUP_COMMUNICATION_SUMMARY_FIELD_ID", "").strip(),
            last_meeting_outcome_field_id=os.getenv("CLICKUP_LAST_MEETING_OUTCOME_FIELD_ID", "").strip(),
            recommended_next_action_field_id=os.getenv("CLICKUP_RECOMMENDED_NEXT_ACTION_FIELD_ID", "").strip(),
            last_meaningful_touch_field_id=os.getenv("CLICKUP_LAST_MEANINGFUL_TOUCH_FIELD_ID", "").strip(),
            last_outbound_field_id=os.getenv("CLICKUP_LAST_OUTBOUND_FIELD_ID", "").strip(),
            last_inbound_field_id=os.getenv("CLICKUP_LAST_INBOUND_FIELD_ID", "").strip(),
        ),
    )


def get_missing_runtime_settings(settings: Settings) -> list[str]:
    missing: list[str] = []
    if not settings.clickup_api_token:
        missing.append("CLICKUP_API_TOKEN")
    if not settings.clickup_list_id:
        missing.append("CLICKUP_LIST_ID")
    return missing


def get_missing_deck_generator_settings(settings: Settings, *, include_google_sheets: bool = True) -> list[str]:
    del settings, include_google_sheets
    return []
