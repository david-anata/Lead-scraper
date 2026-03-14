"""Configuration for the sales support agent."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path


ACTIVE_FOLLOW_UP_STATUSES = (
    "CONTACTED COLD",
    "CONTACTED WARM",
    "WORKING QUALIFIED",
    "WORKING NEEDS OFFER",
    "WORKING OFFERED",
    "WORKING NEGOTIATING",
    "FOLLOW UP",
)

INACTIVE_STATUSES = (
    "WON - ACTIVE",
    "LOST",
    "LOST - NOT QUALIFIED",
    "WON - CANCELED",
)


@dataclass(frozen=True)
class StatusPolicy:
    first_action_days: int | None = None
    due_days: int | None = None
    overdue_days: int | None = None
    use_follow_up_date: bool = False


DEFAULT_STATUS_POLICIES: dict[str, StatusPolicy] = {
    "CONTACTED COLD": StatusPolicy(first_action_days=1, due_days=2, overdue_days=3),
    "CONTACTED WARM": StatusPolicy(first_action_days=1, due_days=1, overdue_days=2),
    "WORKING QUALIFIED": StatusPolicy(due_days=2, overdue_days=3),
    "WORKING NEEDS OFFER": StatusPolicy(due_days=1, overdue_days=2),
    "WORKING OFFERED": StatusPolicy(due_days=4, overdue_days=5),
    "WORKING NEGOTIATING": StatusPolicy(due_days=2, overdue_days=3),
    "FOLLOW UP": StatusPolicy(due_days=0, overdue_days=1, use_follow_up_date=True),
}


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
class Settings:
    app_name: str
    clickup_api_token: str
    clickup_base_url: str
    clickup_list_id: str
    clickup_request_timeout_seconds: int
    clickup_discovery_sample_size: int
    slack_bot_token: str
    slack_channel_id: str
    slack_assignee_map: dict[str, str]
    sales_agent_db_url: str
    internal_api_key: str
    discovery_snapshot_path: Path
    use_due_date_for_follow_up: bool
    openai_api_key: str
    openai_model: str
    instantly_webhook_secret: str
    instantly_webhook_secret_header: str
    instantly_webhook_allowed_event_types: tuple[str, ...]
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


def _default_db_url() -> str:
    runtime_dir = Path("runtime")
    runtime_dir.mkdir(parents=True, exist_ok=True)
    return f"sqlite:///{runtime_dir / 'sales_support_agent.sqlite3'}"


def load_settings() -> Settings:
    return Settings(
        app_name="sales-support-agent",
        clickup_api_token=(os.getenv("CLICKUP_API_TOKEN") or os.getenv("CLICKUP_API_KEY") or "").strip(),
        clickup_base_url=(os.getenv("CLICKUP_BASE_URL", "https://api.clickup.com/api/v2").strip() or "https://api.clickup.com/api/v2"),
        clickup_list_id=os.getenv("CLICKUP_LIST_ID", "").strip(),
        clickup_request_timeout_seconds=int((os.getenv("CLICKUP_REQUEST_TIMEOUT_SECONDS", "30") or "30").strip()),
        clickup_discovery_sample_size=int((os.getenv("CLICKUP_DISCOVERY_SAMPLE_SIZE", "10") or "10").strip()),
        slack_bot_token=os.getenv("SLACK_BOT_TOKEN", "").strip(),
        slack_channel_id=os.getenv("SLACK_CHANNEL_ID", "").strip(),
        slack_assignee_map=_parse_json_object(os.getenv("SLACK_AE_MAP_JSON", "{}")),
        sales_agent_db_url=(os.getenv("SALES_AGENT_DB_URL", "").strip() or _default_db_url()),
        internal_api_key=os.getenv("SALES_AGENT_INTERNAL_API_KEY", "").strip(),
        discovery_snapshot_path=Path(
            os.getenv("CLICKUP_DISCOVERY_SNAPSHOT_PATH", "runtime/clickup_schema_snapshot.json").strip()
            or "runtime/clickup_schema_snapshot.json"
        ),
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
