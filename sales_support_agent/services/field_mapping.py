"""Helpers for resolving existing ClickUp custom fields safely."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sales_support_agent.config import Settings


FIELD_NAME_CANDIDATES = {
    "next_follow_up_date": {"next follow up date", "next follow-up date", "follow up date", "next touch date"},
    "communication_summary": {"communication summary", "latest communication summary", "summary"},
    "last_meeting_outcome": {"last meeting outcome", "meeting outcome"},
    "recommended_next_action": {"recommended next action", "next action", "recommended action"},
    "last_meaningful_touch": {"last meaningful touch", "last contacted", "last touch"},
    "last_outbound": {"last outbound", "last outbound date"},
    "last_inbound": {"last inbound", "last inbound date"},
}


@dataclass(frozen=True)
class ManagedFieldMap:
    next_follow_up_date: str = ""
    communication_summary: str = ""
    last_meeting_outcome: str = ""
    recommended_next_action: str = ""
    last_meaningful_touch: str = ""
    last_outbound: str = ""
    last_inbound: str = ""


def normalize_field_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (value or "").strip().lower()).strip()


def _match_field(custom_fields: list[dict[str, Any]], explicit_id: str, field_key: str) -> str:
    if explicit_id:
        return explicit_id

    candidates = FIELD_NAME_CANDIDATES[field_key]
    for field in custom_fields:
        if normalize_field_name(str(field.get("name") or "")) in candidates:
            return str(field.get("id") or "")
    return ""


def resolve_managed_fields(settings: Settings, custom_fields: list[dict[str, Any]]) -> ManagedFieldMap:
    return ManagedFieldMap(
        next_follow_up_date=_match_field(
            custom_fields,
            settings.managed_fields.next_follow_up_date_field_id,
            "next_follow_up_date",
        ),
        communication_summary=_match_field(
            custom_fields,
            settings.managed_fields.communication_summary_field_id,
            "communication_summary",
        ),
        last_meeting_outcome=_match_field(
            custom_fields,
            settings.managed_fields.last_meeting_outcome_field_id,
            "last_meeting_outcome",
        ),
        recommended_next_action=_match_field(
            custom_fields,
            settings.managed_fields.recommended_next_action_field_id,
            "recommended_next_action",
        ),
        last_meaningful_touch=_match_field(
            custom_fields,
            settings.managed_fields.last_meaningful_touch_field_id,
            "last_meaningful_touch",
        ),
        last_outbound=_match_field(
            custom_fields,
            settings.managed_fields.last_outbound_field_id,
            "last_outbound",
        ),
        last_inbound=_match_field(
            custom_fields,
            settings.managed_fields.last_inbound_field_id,
            "last_inbound",
        ),
    )


def parse_clickup_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    raw = str(value).strip()
    if not raw:
        return None
    if raw.isdigit():
        timestamp = int(raw)
        if timestamp > 10_000_000_000:
            timestamp = timestamp / 1000
        return datetime.fromtimestamp(timestamp, tz=timezone.utc)
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def serialize_clickup_date(value: datetime | None) -> int | None:
    if value is None:
        return None
    normalized = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return int(normalized.timestamp() * 1000)


def extract_field_value(task: dict[str, Any], field_map: ManagedFieldMap, attr_name: str) -> Any:
    field_id = getattr(field_map, attr_name)
    if not field_id:
        return None
    for field in task.get("custom_fields", []) or []:
        if str(field.get("id") or "") == field_id:
            return field.get("value")
    return None

