"""Normalize Instantly webhook payloads into internal communication events."""

from __future__ import annotations

import json
from typing import Any

from sqlalchemy.orm import Session

from sales_support_agent.config import Settings
from sales_support_agent.integrations.clickup import ClickUpClient
from sales_support_agent.integrations.instantly_payloads import (
    SUPPORTED_EVENT_MAP,
    build_external_event_key,
    build_next_follow_up_date,
    build_outcome,
    build_recommended_next_action,
    build_suggested_reply_draft,
    build_summary,
    build_suggested_status,
    extract_email,
    parse_occurred_at,
)
from sales_support_agent.integrations.slack import SlackClient
from sales_support_agent.models.schemas import CommunicationEventRequest
from sales_support_agent.services.audit import AuditService
from sales_support_agent.services.communications import CommunicationService
from sales_support_agent.services.matching import LeadMatchingService


class InstantlyWebhookService:
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

    def process_webhook(self, payload: dict[str, Any]) -> dict[str, Any]:
        event_type = str(payload.get("event_type") or "").strip()
        if not event_type:
            return {"status": "ignored", "reason": "missing_event_type"}
        if event_type not in self.settings.instantly_webhook_allowed_event_types:
            return {"status": "ignored", "reason": "event_type_not_enabled", "event_type": event_type}
        if event_type not in SUPPORTED_EVENT_MAP:
            return {"status": "ignored", "reason": "event_type_not_supported", "event_type": event_type}

        lead_email = extract_email(payload)
        if not lead_email:
            return {"status": "ignored", "reason": "missing_lead_email", "event_type": event_type}

        matcher = LeadMatchingService(self.settings, self.clickup_client, self.session)
        lead = matcher.find_by_email(lead_email, sync_on_miss=True)
        if lead is None:
            self.audit.record_integration_log(
                run_id=None,
                provider="instantly",
                operation="webhook_unmatched",
                status_code=202,
                success=False,
                request_json=payload,
                response_json={"reason": "no_matching_clickup_task", "lead_email": lead_email},
            )
            return {"status": "ignored", "reason": "no_matching_clickup_task", "lead_email": lead_email}

        normalized_event = self._normalize_payload(payload, lead.clickup_task_id)
        self.audit.record_integration_log(
            run_id=None,
            provider="instantly",
            operation="webhook_received",
            status_code=200,
            success=True,
            request_json=payload,
            response_json={"task_id": lead.clickup_task_id, "event_type": normalized_event.event_type},
        )
        result = CommunicationService(self.settings, self.clickup_client, self.slack_client, self.session).process_event(normalized_event)
        return {
            "status": "processed",
            "event_type": event_type,
            "task_id": lead.clickup_task_id,
            "lead_email": lead_email,
            "result": result,
        }

    def _normalize_payload(self, payload: dict[str, Any], task_id: str) -> CommunicationEventRequest:
        return CommunicationEventRequest(
            task_id=task_id,
            event_type=SUPPORTED_EVENT_MAP[str(payload.get("event_type") or "")],
            external_event_key=build_external_event_key(payload),
            occurred_at=parse_occurred_at(payload),
            summary=build_summary(payload),
            outcome=build_outcome(payload),
            recommended_next_action=build_recommended_next_action(payload),
            suggested_reply_draft=build_suggested_reply_draft(payload),
            next_follow_up_date=build_next_follow_up_date(payload),
            suggested_status=build_suggested_status(payload),
            source="instantly_webhook",
            metadata={"raw_payload": payload, "classification": "reply_received" if str(payload.get("event_type") or "") == "reply_received" else "instantly_activity"},
        )


def serialize_payload(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, default=str)
