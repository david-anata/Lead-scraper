"""Stale-lead evaluation and Slack formatting."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from sales_support_agent.config import Settings, normalize_status_key
from sales_support_agent.models.entities import CommunicationEvent, LeadMirror
from sales_support_agent.rules.follow_up import FollowUpAssessment, assess_status_follow_up


AGENT_COMMENT_PREFIX = "[Sales Support Agent]"


@dataclass(frozen=True)
class LeadEvaluation:
    lead: LeadMirror
    assessment: FollowUpAssessment
    last_meaningful_touch_at: datetime | None
    has_work_signal: bool


class ReminderService:
    def __init__(self, settings: Settings, session: Session):
        self.settings = settings
        self.session = session

    def evaluate_lead(
        self,
        lead: LeadMirror,
        *,
        as_of_date: date,
        comments: list[dict[str, Any]] | None = None,
    ) -> LeadEvaluation | None:
        status = (lead.status or "").strip()
        status_key = normalize_status_key(status)
        if status_key in self.settings.inactive_statuses or status_key not in self.settings.active_statuses:
            return None

        last_event = self._latest_meaningful_event(lead.clickup_task_id)
        comment_touch = self._latest_meaningful_comment(comments or [])
        last_touch = self._max_datetime(lead.last_meaningful_touch_at, lead.last_outbound_at, lead.last_inbound_at, last_event, comment_touch)
        has_work_signal = bool(
            last_touch
            or ((lead.next_follow_up_at is not None) and (lead.communication_summary or lead.recommended_next_action))
        )
        policy = self.settings.status_policies[status_key]
        created_at = lead.created_at or lead.last_sync_at or datetime.utcnow()
        assessment = assess_status_follow_up(
            status=status,
            policy=policy,
            created_at=created_at,
            meaningful_touch_at=last_touch,
            next_follow_up_date=lead.next_follow_up_at.date() if lead.next_follow_up_at else None,
            as_of_date=as_of_date,
            has_work_signal=has_work_signal,
        )
        if assessment is None:
            return None
        return LeadEvaluation(lead=lead, assessment=assessment, last_meaningful_touch_at=last_touch, has_work_signal=has_work_signal)

    def build_dedupe_key(self, evaluation: LeadEvaluation) -> str:
        anchor = evaluation.assessment.anchor_date.isoformat()
        return f"{evaluation.lead.clickup_task_id}:{evaluation.lead.status}:{evaluation.assessment.state}:{anchor}"

    def build_slack_message(self, evaluation: LeadEvaluation) -> dict[str, Any]:
        lead = evaluation.lead
        mention = self._format_assignee_mention(lead.assignee_id, lead.assignee_name)
        last_touch = evaluation.last_meaningful_touch_at.date().isoformat() if evaluation.last_meaningful_touch_at else "none"
        text = (
            f"{mention} {evaluation.assessment.state.replace('_', ' ')}: {lead.task_name} "
            f"({lead.status}) needs attention. Last meaningful touch: {last_touch}. "
            f"Next action: {evaluation.assessment.recommended_next_action} {lead.task_url}"
        )
        blocks = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"{mention} *{lead.task_name}*\n"
                        f"*Status:* {lead.status}\n"
                        f"*Created:* {(lead.created_at.date().isoformat() if lead.created_at else 'unknown')}\n"
                        f"*Last meaningful touch:* {last_touch}\n"
                        f"*Enforcement state:* {evaluation.assessment.state.replace('_', ' ')}\n"
                        f"*Recommended next action:* {evaluation.assessment.recommended_next_action}\n"
                        f"<{lead.task_url}|Open ClickUp task>"
                    ),
                },
            }
        ]
        return {"text": text, "blocks": blocks}

    def build_agent_comment(self, evaluation: LeadEvaluation) -> str:
        return (
            f"{AGENT_COMMENT_PREFIX} {evaluation.assessment.state.replace('_', ' ').title()}: "
            f"{evaluation.assessment.reason} Recommended next action: {evaluation.assessment.recommended_next_action}"
        )

    def _latest_meaningful_event(self, task_id: str) -> datetime | None:
        query = (
            select(CommunicationEvent)
            .where(
                CommunicationEvent.clickup_task_id == task_id,
                CommunicationEvent.event_type.in_(
                    [
                        "outbound_email_sent",
                        "inbound_reply_received",
                        "call_completed",
                        "meeting_completed",
                        "offer_sent",
                        "note_logged",
                    ]
                ),
            )
            .order_by(CommunicationEvent.occurred_at.desc())
            .limit(1)
        )
        row = self.session.execute(query).scalar_one_or_none()
        return row.occurred_at if row else None

    def _latest_meaningful_comment(self, comments: list[dict[str, Any]]) -> datetime | None:
        latest: datetime | None = None
        for comment in comments:
            raw_text = str(comment.get("comment_text") or comment.get("comment") or "")
            if not raw_text or raw_text.startswith(AGENT_COMMENT_PREFIX):
                continue
            timestamp = comment.get("date") or comment.get("date_created")
            if timestamp is None:
                continue
            parsed = datetime.fromtimestamp(int(str(timestamp)) / 1000) if str(timestamp).isdigit() else None
            if parsed and (latest is None or parsed > latest):
                latest = parsed
        return latest

    def _max_datetime(self, *values: datetime | None) -> datetime | None:
        filtered = [value for value in values if value is not None]
        if not filtered:
            return None
        return max(filtered)

    def _format_assignee_mention(self, assignee_id: str, assignee_name: str) -> str:
        slack_user_id = self.settings.slack_assignee_map.get(assignee_id) or self.settings.slack_assignee_map.get(assignee_name)
        if slack_user_id:
            return f"<@{slack_user_id}>"
        return assignee_name or "Assigned AE"
