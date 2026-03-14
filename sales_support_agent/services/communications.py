"""Manual communication ingest and ClickUp task updates."""

from __future__ import annotations

from datetime import date, datetime, time, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from sales_support_agent.config import Settings, normalize_status_key
from sales_support_agent.integrations.clickup import ClickUpClient
from sales_support_agent.integrations.slack import SlackClient
from sales_support_agent.models.entities import CommunicationEvent
from sales_support_agent.models.schemas import CommunicationEventRequest
from sales_support_agent.rules.business_days import add_business_days
from sales_support_agent.services.audit import AuditService
from sales_support_agent.services.field_mapping import (
    resolve_managed_fields,
    serialize_clickup_date,
)
from sales_support_agent.services.notification_policy import build_clickup_owner_reference
from sales_support_agent.services.reply_templates import format_date_label, trim_for_slack
from sales_support_agent.services.sync import ClickUpSyncService


EVENT_LABELS = {
    "outbound_email_sent": "Outbound email sent",
    "inbound_reply_received": "Inbound reply received",
    "call_completed": "Call completed",
    "meeting_completed": "Meeting completed",
    "offer_sent": "Offer sent",
    "note_logged": "Note logged",
}


class CommunicationService:
    def __init__(
        self,
        settings: Settings,
        clickup_client: ClickUpClient,
        slack_client: SlackClient,
        session: Session,
    ):
        self.settings = settings
        self.clickup_client = clickup_client
        self.slack_client = slack_client
        self.session = session
        self.audit = AuditService(session)

    def process_event(self, payload: CommunicationEventRequest) -> dict[str, Any]:
        if payload.external_event_key:
            existing_event = self.session.execute(
                select(CommunicationEvent).where(CommunicationEvent.external_event_key == payload.external_event_key).limit(1)
            ).scalar_one_or_none()
            if existing_event is not None:
                return {
                    "task_id": payload.task_id,
                    "event_id": existing_event.id,
                    "status": "skipped_duplicate",
                    "reason": "external_event_key_already_processed",
                }

        task = self.clickup_client.get_task(payload.task_id)
        sync_service = ClickUpSyncService(self.settings, self.clickup_client, self.session)
        lead = sync_service.sync_task(task)
        field_map = resolve_managed_fields(self.settings, self.clickup_client.get_accessible_custom_fields(self.settings.clickup_list_id))

        occurred_at = payload.occurred_at or datetime.now(timezone.utc)
        event = CommunicationEvent(
            clickup_task_id=payload.task_id,
            event_type=payload.event_type,
            external_event_key=payload.external_event_key,
            source=payload.source,
            summary=payload.summary,
            outcome=payload.outcome,
            recommended_next_action=payload.recommended_next_action,
            occurred_at=occurred_at,
            raw_payload=payload.model_dump(mode="json"),
        )
        self.session.add(event)
        self.session.flush()

        comment_text = self._build_comment_text(task, payload, occurred_at)
        comment_result = self.clickup_client.create_task_comment(payload.task_id, comment_text)
        self.audit.record_action(
            run_id=None,
            clickup_task_id=payload.task_id,
            system="clickup",
            action_type="append_comment",
            before={"task_status": lead.status},
            after=comment_result,
        )

        update_summary = self._apply_field_updates(task, payload, occurred_at, field_map)
        sync_service.sync_task(self.clickup_client.get_task(payload.task_id))

        slack_result: dict[str, Any] = {"ok": False, "skipped": True}
        if payload.event_type == "inbound_reply_received" and self._should_send_immediate_event_alert("inbound_reply_received"):
            slack_result = self._notify_reply_received(task, payload, occurred_at)
        elif (
            payload.event_type == "meeting_completed"
            and not (payload.summary or payload.outcome)
            and self._should_send_immediate_event_alert("meeting_notes_missing")
        ):
            slack_result = self._notify_meeting_notes_missing(task)
        if not slack_result.get("skipped"):
            self.audit.record_action(
                run_id=None,
                clickup_task_id=payload.task_id,
                system="slack",
                action_type="event_notification",
                before={"event_type": payload.event_type},
                after=slack_result,
            )

        return {
            "task_id": payload.task_id,
            "event_id": event.id,
            "comment_posted": bool(comment_result),
            "field_updates": update_summary,
            "slack_notification": slack_result,
        }

    def _apply_field_updates(self, task: dict[str, Any], payload: CommunicationEventRequest, occurred_at: datetime, field_map) -> dict[str, Any]:
        updates: dict[str, Any] = {}
        if field_map.last_meaningful_touch:
            result = self.clickup_client.set_custom_field_value(payload.task_id, field_map.last_meaningful_touch, serialize_clickup_date(occurred_at))
            updates["last_meaningful_touch"] = occurred_at.isoformat()
            self._record_clickup_write(payload.task_id, "set_last_meaningful_touch", result, updates)
        if payload.event_type == "outbound_email_sent" and field_map.last_outbound:
            result = self.clickup_client.set_custom_field_value(payload.task_id, field_map.last_outbound, serialize_clickup_date(occurred_at))
            updates["last_outbound"] = occurred_at.isoformat()
            self._record_clickup_write(payload.task_id, "set_last_outbound", result, updates)
        if payload.event_type == "inbound_reply_received" and field_map.last_inbound:
            result = self.clickup_client.set_custom_field_value(payload.task_id, field_map.last_inbound, serialize_clickup_date(occurred_at))
            updates["last_inbound"] = occurred_at.isoformat()
            self._record_clickup_write(payload.task_id, "set_last_inbound", result, updates)
        if payload.summary and field_map.communication_summary:
            result = self.clickup_client.set_custom_field_value(payload.task_id, field_map.communication_summary, payload.summary)
            updates["communication_summary"] = payload.summary
            self._record_clickup_write(payload.task_id, "set_communication_summary", result, updates)
        if payload.outcome and payload.event_type == "meeting_completed" and field_map.last_meeting_outcome:
            result = self.clickup_client.set_custom_field_value(payload.task_id, field_map.last_meeting_outcome, payload.outcome)
            updates["last_meeting_outcome"] = payload.outcome
            self._record_clickup_write(payload.task_id, "set_last_meeting_outcome", result, updates)
        if payload.recommended_next_action and field_map.recommended_next_action:
            result = self.clickup_client.set_custom_field_value(payload.task_id, field_map.recommended_next_action, payload.recommended_next_action)
            updates["recommended_next_action"] = payload.recommended_next_action
            self._record_clickup_write(payload.task_id, "set_recommended_next_action", result, updates)

        next_follow_up = self._resolve_next_follow_up_date(task, payload, occurred_at.date())
        if next_follow_up:
            next_follow_up_dt = datetime.combine(next_follow_up, time.min, tzinfo=timezone.utc)
            if field_map.next_follow_up_date:
                result = self.clickup_client.set_custom_field_value(payload.task_id, field_map.next_follow_up_date, serialize_clickup_date(next_follow_up_dt))
                updates["next_follow_up_date"] = next_follow_up.isoformat()
                self._record_clickup_write(payload.task_id, "set_next_follow_up_date", result, updates)
            if self.settings.use_due_date_for_follow_up:
                result = self.clickup_client.update_task(payload.task_id, {"due_date": serialize_clickup_date(next_follow_up_dt)})
                updates["due_date"] = next_follow_up.isoformat()
                self._record_clickup_write(payload.task_id, "set_due_date", result, updates)

        if payload.event_type == "offer_sent":
            current_status = str(((task.get("status") or {}).get("status")) or "")
            if current_status in {
                "CONTACTED COLD",
                "CONTACTED WARM",
                "WORKING QUALIFIED",
                "WORKING NEEDS OFFER",
                "FOLLOW UP",
            }:
                result = self.clickup_client.update_task(payload.task_id, {"status": "WORKING OFFERED"})
                updates["status"] = "WORKING OFFERED"
                self._record_clickup_write(payload.task_id, "set_status_working_offered", result, updates)

        return updates

    def _resolve_next_follow_up_date(
        self,
        task: dict[str, Any],
        payload: CommunicationEventRequest,
        occurred_date: date,
    ) -> date | None:
        if payload.next_follow_up_date:
            return payload.next_follow_up_date

        current_status = str(((task.get("status") or {}).get("status")) or "")
        if payload.event_type == "offer_sent":
            return add_business_days(occurred_date, 4)
        policy = self.settings.status_policies.get(normalize_status_key(current_status))
        if policy and policy.due_days is not None and not policy.use_follow_up_date:
            return add_business_days(occurred_date, policy.due_days)
        return None

    def _build_comment_text(self, task: dict[str, Any], payload: CommunicationEventRequest, occurred_at: datetime) -> str:
        assignee = (task.get("assignees") or [{}])[0] or {}
        assignee_name = str(assignee.get("username") or assignee.get("email") or "")
        owner_reference = build_clickup_owner_reference(assignee_name)
        classification = str(payload.metadata.get("classification") or payload.event_type)
        parts = [
            f"[Sales Support Agent] {owner_reference} {EVENT_LABELS[payload.event_type].lower()}.",
            f"Date: {format_date_label(occurred_at)}",
        ]
        if payload.summary:
            parts.append(f"Action summary: {payload.summary}")
        if payload.outcome:
            parts.append(f"Outcome: {payload.outcome}")
        if payload.recommended_next_action:
            parts.append(f"Next step: {payload.recommended_next_action}")
        if payload.suggested_reply_draft:
            parts.append(f"Suggested reply: {payload.suggested_reply_draft}")
        if payload.suggested_status:
            parts.append(f"Suggested status: {payload.suggested_status}")
        parts.append(f"Classification: {classification}")
        return "\n".join(parts)

    def _notify_reply_received(self, task: dict[str, Any], payload: CommunicationEventRequest, occurred_at: datetime) -> dict[str, Any]:
        assignee = (task.get("assignees") or [{}])[0] or {}
        assignee_id = str(assignee.get("id") or "")
        assignee_name = str(assignee.get("username") or assignee.get("email") or "Assigned AE")
        slack_user = self.settings.slack_assignee_map.get(assignee_id) or self.settings.slack_assignee_map.get(assignee_name)
        mention = f"<@{slack_user}>" if slack_user else assignee_name
        date_label = format_date_label(occurred_at)
        action_summary = payload.summary or payload.recommended_next_action or "Lead activity needs review."
        draft = trim_for_slack(payload.suggested_reply_draft or "", limit=140)
        text = (
            f"{mention} {date_label} | {task.get('name', 'Lead')} | "
            f"{trim_for_slack(action_summary, limit=140)} | "
            f"next: {payload.recommended_next_action or 'Review and respond.'}"
            f"{f' | draft: {draft}' if draft else ''} "
            f"{task.get('url', '')}"
        )
        return self.slack_client.post_message(text=text)

    def _notify_meeting_notes_missing(self, task: dict[str, Any]) -> dict[str, Any]:
        assignee = (task.get("assignees") or [{}])[0] or {}
        assignee_id = str(assignee.get("id") or "")
        assignee_name = str(assignee.get("username") or assignee.get("email") or "Assigned AE")
        slack_user = self.settings.slack_assignee_map.get(assignee_id) or self.settings.slack_assignee_map.get(assignee_name)
        mention = f"<@{slack_user}>" if slack_user else assignee_name
        text = (
            f"{mention} {format_date_label(datetime.now(timezone.utc))} | {task.get('name', 'Lead')} | meeting notes still missing. "
            f"Next step: log the meeting outcome and next follow-up in ClickUp. {task.get('url', '')}"
        )
        return self.slack_client.post_message(text=text)

    def _should_send_immediate_event_alert(self, event_type: str) -> bool:
        return event_type in self.settings.slack_immediate_event_types

    def _record_clickup_write(self, task_id: str, action_type: str, result: dict[str, Any], updates: dict[str, Any]) -> None:
        self.audit.record_action(
            run_id=None,
            clickup_task_id=task_id,
            system="clickup",
            action_type=action_type,
            before={},
            after={"result": result, "updates": dict(updates)},
        )
