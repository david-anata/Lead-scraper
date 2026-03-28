"""Sync ClickUp tasks into a local mirror for auditing and rule evaluation."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from sales_support_agent.config import (
    ACTIVE_FOLLOW_UP_STATUSES,
    INACTIVE_STATUSES,
    Settings,
    is_active_pipeline_status,
    is_closed_pipeline_status,
    normalize_status_key,
)
from sales_support_agent.integrations.clickup import ClickUpClient
from sales_support_agent.models.entities import LeadMirror
from sales_support_agent.services.field_mapping import (
    extract_field_value,
    parse_clickup_datetime,
    resolve_managed_fields,
)


VISIBLE_FIELD_NAMES = {
    "product": {"product"},
    "source": {"source"},
    "email": {"email"},
    "value": {"value"},
    "phone_number": {"phone number", "phone", "phone #"},
}


def _normalize(value: str) -> str:
    return (value or "").strip().lower()


def _extract_named_field(task: dict[str, Any], candidates: set[str]) -> str:
    for field in task.get("custom_fields", []) or []:
        if _normalize(str(field.get("name") or "")) in candidates:
            value = field.get("value")
            if isinstance(value, dict):
                return str(value.get("name") or value.get("label") or "")
            return "" if value is None else str(value)
    return ""


def _extract_priority(task: dict[str, Any]) -> str:
    priority = task.get("priority") or {}
    if isinstance(priority, dict):
        return str(priority.get("priority") or "")
    return str(priority or "")


def _extract_owner(task: dict[str, Any]) -> tuple[str, str]:
    assignees = task.get("assignees", []) or []
    if isinstance(assignees, dict):
        assignees = [assignees]
    if not assignees:
        return "", ""
    assignee = assignees[0] or {}
    return str(assignee.get("id") or ""), str(assignee.get("username") or assignee.get("email") or assignee.get("initials") or "")


def _extract_task_dates(task: dict[str, Any]) -> tuple[datetime | None, datetime | None, datetime | None]:
    created_at = parse_clickup_datetime(task.get("date_created"))
    updated_at = parse_clickup_datetime(task.get("date_updated"))
    due_date = parse_clickup_datetime(task.get("due_date"))
    return created_at, updated_at, due_date


def _normalized_status_sets(settings: Settings) -> tuple[tuple[str, ...], tuple[str, ...]]:
    active_statuses = tuple(
        normalize_status_key(status)
        for status in getattr(settings, "active_statuses", ACTIVE_FOLLOW_UP_STATUSES)
        if normalize_status_key(status)
    )
    inactive_statuses = tuple(
        normalize_status_key(status)
        for status in getattr(settings, "inactive_statuses", INACTIVE_STATUSES)
        if normalize_status_key(status)
    )
    return active_statuses, inactive_statuses


def _extract_status(task: dict[str, Any], settings: Settings) -> tuple[str, str, bool, bool]:
    status_value = task.get("status") or {}
    if isinstance(status_value, dict):
        raw_status = str(status_value.get("status") or "")
    else:
        raw_status = str(status_value or "")
    status_key = normalize_status_key(raw_status)
    active_statuses, inactive_statuses = _normalized_status_sets(settings)
    is_closed = is_closed_pipeline_status(status_key, inactive_statuses)
    is_active = is_active_pipeline_status(
        raw_status,
        active_statuses=active_statuses,
        inactive_statuses=inactive_statuses,
    )
    return raw_status, status_key, is_closed, is_active


class ClickUpSyncService:
    def __init__(self, settings: Settings, clickup_client: ClickUpClient, session: Session):
        self.settings = settings
        self.clickup_client = clickup_client
        self.session = session

    def sync_list(self, *, include_closed: bool = True, max_tasks: int | None = None) -> dict[str, Any]:
        custom_fields = self.clickup_client.get_accessible_custom_fields(self.settings.clickup_list_id)
        field_map = resolve_managed_fields(self.settings, custom_fields)

        page = 0
        synced = 0
        while True:
            tasks = self.clickup_client.get_tasks(self.settings.clickup_list_id, include_closed=include_closed, page=page)
            if not tasks:
                break
            for task in tasks:
                self._upsert_task(task, field_map)
                synced += 1
                if max_tasks and synced >= max_tasks:
                    return {"synced_tasks": synced, "field_map": field_map.__dict__}
            page += 1
        return {"synced_tasks": synced, "field_map": field_map.__dict__}

    def sync_task(self, task: dict[str, Any]) -> LeadMirror:
        custom_fields = self.clickup_client.get_accessible_custom_fields(self.settings.clickup_list_id)
        field_map = resolve_managed_fields(self.settings, custom_fields)
        return self._upsert_task(task, field_map)

    def _upsert_task(self, task: dict[str, Any], field_map) -> LeadMirror:
        task_id = str(task.get("id") or "")
        if not task_id:
            raise ValueError("ClickUp task payload is missing an id.")

        lead = self.session.get(LeadMirror, task_id) or LeadMirror(
            clickup_task_id=task_id,
            list_id=self.settings.clickup_list_id,
            task_name=str(task.get("name") or ""),
            status="",
        )
        lead.list_id = self.settings.clickup_list_id
        assignee_id, assignee_name = _extract_owner(task)
        created_at, updated_at, due_date = _extract_task_dates(task)
        raw_status, status_key, is_closed, is_active = _extract_status(task, self.settings)
        lead.task_name = str(task.get("name") or "")
        lead.task_url = str(task.get("url") or "")
        lead.status = raw_status
        lead.status_key = status_key
        lead.is_closed = is_closed
        lead.is_active = is_active
        lead.assignee_id = assignee_id
        lead.assignee_name = assignee_name
        lead.priority = _extract_priority(task)
        lead.product = _extract_named_field(task, VISIBLE_FIELD_NAMES["product"])
        lead.source = _extract_named_field(task, VISIBLE_FIELD_NAMES["source"])
        lead.email = _extract_named_field(task, VISIBLE_FIELD_NAMES["email"])
        lead.phone_number = _extract_named_field(task, VISIBLE_FIELD_NAMES["phone_number"])
        lead.value = _extract_named_field(task, VISIBLE_FIELD_NAMES["value"])
        lead.created_at = created_at
        lead.updated_at = updated_at
        lead.task_updated_at = updated_at
        lead.due_date = due_date
        lead.last_meaningful_touch_at = parse_clickup_datetime(extract_field_value(task, field_map, "last_meaningful_touch"))
        lead.last_outbound_at = parse_clickup_datetime(extract_field_value(task, field_map, "last_outbound"))
        lead.last_inbound_at = parse_clickup_datetime(extract_field_value(task, field_map, "last_inbound"))
        lead.next_follow_up_at = parse_clickup_datetime(extract_field_value(task, field_map, "next_follow_up_date"))
        lead.communication_summary = str(extract_field_value(task, field_map, "communication_summary") or "")
        lead.last_meeting_outcome = str(extract_field_value(task, field_map, "last_meeting_outcome") or "")
        lead.recommended_next_action = str(extract_field_value(task, field_map, "recommended_next_action") or "")
        lead.last_sync_at = datetime.utcnow()
        lead.raw_task_payload = task
        self.session.add(lead)
        self.session.flush()
        return lead
