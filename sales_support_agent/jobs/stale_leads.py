"""Stale-lead scanning job."""

from __future__ import annotations

from datetime import date

from sqlalchemy import select
from sqlalchemy.orm import Session

from sales_support_agent.config import Settings
from sales_support_agent.integrations.clickup import ClickUpClient
from sales_support_agent.integrations.slack import SlackClient
from sales_support_agent.models.entities import LeadMirror
from sales_support_agent.services.audit import AuditService
from sales_support_agent.services.reminders import ReminderService
from sales_support_agent.services.sync import ClickUpSyncService


class StaleLeadJob:
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
        self.reminders = ReminderService(settings, session)

    def run(self, *, dry_run: bool = False, as_of_date: date | None = None, max_tasks: int | None = None) -> dict[str, int | str]:
        effective_date = as_of_date or date.today()
        run = self.audit.start_run("stale_lead_scan", trigger="manual", metadata={"dry_run": dry_run, "as_of_date": effective_date.isoformat()})
        sync_summary = ClickUpSyncService(self.settings, self.clickup_client, self.session).sync_list(include_closed=True, max_tasks=max_tasks)
        query = select(LeadMirror).where(LeadMirror.list_id == self.settings.clickup_list_id)
        leads = list(self.session.execute(query).scalars())

        inspected = 0
        alerted = 0
        commented = 0
        skipped = 0
        for lead in leads:
            if max_tasks and inspected >= max_tasks:
                break
            inspected += 1
            comments = self.clickup_client.get_task_comments(lead.clickup_task_id)
            evaluation = self.reminders.evaluate_lead(lead, as_of_date=effective_date, comments=comments)
            if evaluation is None:
                continue

            dedupe_key = self.reminders.build_dedupe_key(evaluation)
            if self.audit.has_successful_action(dedupe_key):
                skipped += 1
                continue

            if dry_run:
                continue

            slack_payload = self.reminders.build_slack_message(evaluation)
            slack_result = self.slack_client.post_message(**slack_payload)
            alerted += 1
            self.audit.record_action(
                run_id=run.id,
                clickup_task_id=lead.clickup_task_id,
                system="slack",
                action_type=evaluation.assessment.state,
                dedupe_key=dedupe_key,
                before={"status": lead.status, "follow_up_state": lead.follow_up_state},
                after=slack_result,
            )

            comment_text = self.reminders.build_agent_comment(evaluation)
            comment_result = self.clickup_client.create_task_comment(lead.clickup_task_id, comment_text)
            commented += 1
            self.audit.record_action(
                run_id=run.id,
                clickup_task_id=lead.clickup_task_id,
                system="clickup",
                action_type="append_reminder_comment",
                dedupe_key=f"{dedupe_key}:comment",
                before={"status": lead.status},
                after=comment_result,
            )
            lead.follow_up_state = evaluation.assessment.state
            self.session.add(lead)

        self.audit.finish_run(
            run,
            status="success",
            summary={
                "inspected": inspected,
                "alerted": alerted,
                "commented": commented,
                "skipped_deduped": skipped,
                "synced_tasks": sync_summary.get("synced_tasks", 0),
            },
        )
        return {
            "status": "ok",
            "inspected": inspected,
            "alerted": alerted,
            "commented": commented,
            "skipped_deduped": skipped,
            "synced_tasks": int(sync_summary.get("synced_tasks", 0)),
        }

