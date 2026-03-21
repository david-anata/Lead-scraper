"""Stale-lead evaluation and Slack formatting."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from sales_support_agent.config import Settings, normalize_status_key
from sales_support_agent.models.entities import CommunicationEvent, LeadMirror
from sales_support_agent.rules.follow_up import FollowUpAssessment, assess_status_follow_up
from sales_support_agent.services.notification_policy import (
    STALE_URGENCY_LABELS,
    STALE_URGENCY_ORDER,
    classify_stale_assessment_state,
    build_clickup_owner_reference,
    determine_stale_notification_mode,
)
from sales_support_agent.services.reply_templates import (
    build_stale_action_summary,
    build_stale_reply_draft,
    format_date_label,
    trim_for_slack,
)


AGENT_COMMENT_PREFIX = "[Sales Support Agent]"
AGENT_COMMENT_SIGNATURE_PREFIX = "Signature:"


@dataclass(frozen=True)
class LeadEvaluation:
    lead: LeadMirror
    assessment: FollowUpAssessment
    last_meaningful_touch_at: datetime | None
    has_work_signal: bool


@dataclass(frozen=True)
class StaleDigestItem:
    evaluation: LeadEvaluation
    urgency: str
    urgency_label: str
    owner_label: str
    owner_display: str
    last_touch_label: str
    action_summary: str
    suggested_reply_draft: str
    notification_mode: str


class ReminderService:
    _SLACK_SECTION_TEXT_LIMIT = 2800
    _SLACK_OWNER_SUMMARY_LIMIT = 8

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

    def build_immediate_stale_slack_message(self, evaluation: LeadEvaluation) -> dict[str, Any]:
        lead = evaluation.lead
        mention = self._format_assignee_mention(lead.assignee_id, lead.assignee_name)
        urgency_label = self._urgency_label(evaluation)
        last_touch = self._last_touch_label(evaluation.last_meaningful_touch_at)
        action_summary = build_stale_action_summary(
            task_name=lead.task_name,
            status=lead.status,
            last_touch_label=last_touch,
            next_step=evaluation.assessment.recommended_next_action,
            urgency_label=urgency_label,
        )
        suggested_reply = build_stale_reply_draft(
            task_name=lead.task_name,
            status=lead.status,
            next_step=evaluation.assessment.recommended_next_action,
            as_of_date=evaluation.assessment.anchor_date,
        )
        text = (
            f"{mention} {format_date_label(evaluation.assessment.anchor_date)} | {lead.task_name} | "
            f"{trim_for_slack(action_summary)} | draft: {trim_for_slack(suggested_reply)} {lead.task_url}"
        )
        blocks = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"{mention} *{urgency_label}: {lead.task_name}*\n"
                        f"*Date:* {format_date_label(evaluation.assessment.anchor_date)}\n"
                        f"*Status:* {lead.status}\n"
                        f"*Last meaningful touch:* {last_touch}\n"
                        f"*Action summary:* {action_summary}\n"
                        f"*Suggested reply:* {suggested_reply}\n"
                        f"<{lead.task_url}|Open ClickUp task>"
                    ),
                },
            }
        ]
        return {"text": text, "blocks": blocks}

    def build_digest_item(self, evaluation: LeadEvaluation) -> StaleDigestItem:
        owner_display = self._format_assignee_mention(evaluation.lead.assignee_id, evaluation.lead.assignee_name)
        urgency = self._urgency_key(evaluation)
        return StaleDigestItem(
            evaluation=evaluation,
            urgency=urgency,
            urgency_label=STALE_URGENCY_LABELS[urgency],
            owner_label=evaluation.lead.assignee_name or "Assigned AE",
            owner_display=owner_display,
            last_touch_label=self._last_touch_label(evaluation.last_meaningful_touch_at),
            action_summary=build_stale_action_summary(
                task_name=evaluation.lead.task_name,
                status=evaluation.lead.status,
                last_touch_label=self._last_touch_label(evaluation.last_meaningful_touch_at),
                next_step=evaluation.assessment.recommended_next_action,
                urgency_label=STALE_URGENCY_LABELS[urgency],
            ),
            suggested_reply_draft=build_stale_reply_draft(
                task_name=evaluation.lead.task_name,
                status=evaluation.lead.status,
                next_step=evaluation.assessment.recommended_next_action,
                as_of_date=evaluation.assessment.anchor_date,
            ),
            notification_mode=determine_stale_notification_mode(urgency, self.settings.stale_lead_immediate_alert_urgencies),
        )

    def build_stale_digest_message(self, items: list[StaleDigestItem]) -> dict[str, Any] | None:
        if not items:
            return None

        ordered = sorted(
            items,
            key=lambda item: (
                STALE_URGENCY_ORDER.index(item.urgency),
                item.owner_label.lower(),
                item.evaluation.lead.task_name.lower(),
            ),
        )
        total_items = len(ordered)
        visible_items = ordered[: self.settings.stale_lead_slack_digest_max_items]
        truncated = total_items - len(visible_items)
        urgency_counts = Counter(item.urgency for item in ordered)
        assignee_counts = Counter(item.owner_display for item in ordered)

        intro_prefix = "<!channel> " if self.settings.stale_lead_slack_digest_mention_channel else ""
        intro = (
            f"{intro_prefix}*SDR Support Digest*\n"
            f"{total_items} leads need attention from the latest stale scan."
        )
        summary_lines = [
            f"*{STALE_URGENCY_LABELS[urgency]}:* {urgency_counts.get(urgency, 0)}"
            for urgency in STALE_URGENCY_ORDER
            if urgency_counts.get(urgency, 0)
        ]
        ranked_owners = sorted(assignee_counts.items(), key=lambda item: (-item[1], item[0].lower()))
        visible_owners = ranked_owners[: self._SLACK_OWNER_SUMMARY_LIMIT]
        owner_summary = ", ".join(f"{owner}: {count}" for owner, count in visible_owners) or "No owners assigned"
        if len(ranked_owners) > len(visible_owners):
            owner_summary += f", +{len(ranked_owners) - len(visible_owners)} more"

        sections = [
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": intro},
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*Summary*\n" + "\n".join(summary_lines) + f"\n*By owner:* {owner_summary}",
                },
            },
        ]

        for urgency in STALE_URGENCY_ORDER:
            urgency_items = [item for item in visible_items if item.urgency == urgency]
            if not urgency_items:
                continue
            lines = [
                (
                    f"- {item.owner_display} | *{item.evaluation.lead.task_name}* | {item.evaluation.lead.status} | "
                    f"{format_date_label(item.evaluation.assessment.anchor_date)} | {item.action_summary} | "
                    f"draft: {trim_for_slack(item.suggested_reply_draft, limit=120)} "
                    f"<{item.evaluation.lead.task_url}|Open>"
                )
                for item in urgency_items
            ]
            for section_text in self._chunk_digest_lines(STALE_URGENCY_LABELS[urgency], lines):
                sections.append(
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": section_text,
                        },
                    }
                )

        if truncated > 0:
            sections.append(
                {
                    "type": "context",
                    "elements": [{"type": "mrkdwn", "text": f"_Digest truncated. {truncated} additional leads were omitted from the message._"}],
                }
            )

        fallback = intro_prefix + f"SDR Support Digest: {total_items} leads need attention."
        return {"text": fallback, "blocks": sections}

    def build_agent_comment(self, evaluation: LeadEvaluation) -> str:
        owner_reference = build_clickup_owner_reference(evaluation.lead.assignee_name)
        last_touch = self._last_touch_label(evaluation.last_meaningful_touch_at)
        urgency_label = self._urgency_label(evaluation)
        signature = self.build_agent_comment_signature(evaluation)
        return (
            f"{AGENT_COMMENT_PREFIX} {owner_reference} {urgency_label.lower()} for this lead.\n"
            f"Date: {format_date_label(evaluation.assessment.anchor_date)}\n"
            f"Why it matters: {evaluation.assessment.reason}\n"
            f"Last meaningful touch: {last_touch}\n"
            f"Next step: {evaluation.assessment.recommended_next_action}\n"
            f"Suggested reply: {build_stale_reply_draft(task_name=evaluation.lead.task_name, status=evaluation.lead.status, next_step=evaluation.assessment.recommended_next_action, as_of_date=evaluation.assessment.anchor_date)}\n"
            f"{AGENT_COMMENT_SIGNATURE_PREFIX} {signature}"
        )

    def build_agent_comment_signature(self, evaluation: LeadEvaluation) -> str:
        parts = (
            normalize_status_key(evaluation.lead.status or ""),
            self._urgency_key(evaluation),
            " ".join((evaluation.assessment.reason or "").lower().split()),
            " ".join((evaluation.assessment.recommended_next_action or "").lower().split()),
        )
        return " | ".join(parts)

    def should_skip_agent_comment(
        self,
        *,
        evaluation: LeadEvaluation,
        comments: list[dict[str, Any]] | None,
        recency_days: int = 5,
    ) -> bool:
        candidate_signature = self.build_agent_comment_signature(evaluation)
        candidate_next_step = " ".join((evaluation.assessment.recommended_next_action or "").lower().split())
        candidate_reason = " ".join((evaluation.assessment.reason or "").lower().split())
        cutoff_date = evaluation.assessment.anchor_date - timedelta(days=recency_days)

        for comment in comments or []:
            raw_text = str(comment.get("comment_text") or comment.get("comment") or "")
            if not raw_text.startswith(AGENT_COMMENT_PREFIX):
                continue
            comment_timestamp = self._parse_comment_timestamp(comment.get("date") or comment.get("date_created"))
            if comment_timestamp and comment_timestamp.date() < cutoff_date:
                continue

            signature = self._extract_agent_comment_signature(raw_text)
            if signature:
                if signature == candidate_signature:
                    return True
                continue

            normalized_text = " ".join(raw_text.lower().split())
            urgency_phrase = f"{self._urgency_label(evaluation).lower()} for this lead."
            if urgency_phrase in normalized_text and f"next step: {candidate_next_step}" in normalized_text and candidate_reason in normalized_text:
                return True

        return False

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
            parsed = self._parse_comment_timestamp(comment.get("date") or comment.get("date_created"))
            if parsed and (latest is None or parsed > latest):
                latest = parsed
        return latest

    def _parse_comment_timestamp(self, raw_value: Any) -> datetime | None:
        if raw_value is None:
            return None
        if isinstance(raw_value, datetime):
            return raw_value
        value = str(raw_value).strip()
        if not value:
            return None
        if value.isdigit():
            return datetime.fromtimestamp(int(value) / 1000)
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None

    def _extract_agent_comment_signature(self, raw_text: str) -> str:
        for line in raw_text.splitlines():
            if line.startswith(AGENT_COMMENT_SIGNATURE_PREFIX):
                return line.split(":", 1)[1].strip()
        return ""

    def _max_datetime(self, *values: datetime | None) -> datetime | None:
        filtered = [value for value in values if value is not None]
        if not filtered:
            return None
        return max(filtered)

    def _urgency_key(self, evaluation: LeadEvaluation) -> str:
        return classify_stale_assessment_state(evaluation.assessment.state)

    def _urgency_label(self, evaluation: LeadEvaluation) -> str:
        return STALE_URGENCY_LABELS[self._urgency_key(evaluation)]

    def _last_touch_label(self, value: datetime | None) -> str:
        return value.date().isoformat() if value else "none recorded"

    def _format_assignee_mention(self, assignee_id: str, assignee_name: str) -> str:
        slack_user_id = self.settings.slack_assignee_map.get(assignee_id) or self.settings.slack_assignee_map.get(assignee_name)
        if slack_user_id:
            return f"<@{slack_user_id}>"
        return assignee_name or "Assigned AE"

    def _chunk_digest_lines(self, heading: str, lines: list[str]) -> list[str]:
        chunks: list[str] = []
        current = f"*{heading}*"
        for line in lines:
            candidate = f"{current}\n{line}"
            if len(candidate) > self._SLACK_SECTION_TEXT_LIMIT and current != f"*{heading}*":
                chunks.append(current)
                current = f"*{heading}*\n{line}"
            else:
                current = candidate
        if current:
            chunks.append(current)
        return chunks
