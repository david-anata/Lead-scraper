"""Stale-lead scanning job."""

from __future__ import annotations

import logging
from datetime import date

from sqlalchemy import select
from sqlalchemy.orm import Session

from sales_support_agent.config import Settings, normalize_status_key
from sales_support_agent.integrations.clickup import ClickUpClient
from sales_support_agent.integrations.slack import SlackClient
from sales_support_agent.models.entities import LeadMirror
from sales_support_agent.services.audit import AuditService
from sales_support_agent.services.reminders import ReminderService
from sales_support_agent.services.sync import ClickUpSyncService


logger = logging.getLogger(__name__)


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

    def run(self, *, dry_run: bool = False, as_of_date: date | None = None, max_tasks: int | None = None) -> dict[str, int | str | bool]:
        effective_date = as_of_date or date.today()
        processing_limit = max_tasks if max_tasks is not None else self.settings.stale_lead_scan_max_tasks
        sync_limit = max_tasks if max_tasks is not None else self.settings.stale_lead_scan_sync_max_tasks
        run = self.audit.start_run("stale_lead_scan", trigger="manual", metadata={"dry_run": dry_run, "as_of_date": effective_date.isoformat()})
        sync_summary: dict[str, int | str] = {"synced_tasks": 0}
        sync_failed = False
        try:
            sync_summary = ClickUpSyncService(self.settings, self.clickup_client, self.session).sync_list(
                include_closed=True,
                max_tasks=sync_limit,
            )
        except Exception as exc:
            sync_failed = True
            logger.exception("stale lead sync refresh failed")
            self.audit.record_action(
                run_id=run.id,
                clickup_task_id="",
                system="sales_support_agent",
                action_type="stale_lead_sync_failed",
                success=False,
                error_message=str(exc),
                before={"sync_limit": sync_limit},
                after={},
            )
        query = (
            select(LeadMirror)
            .where(LeadMirror.list_id == self.settings.clickup_list_id)
            .order_by(LeadMirror.updated_at.asc(), LeadMirror.last_sync_at.asc())
        )
        leads = list(self.session.execute(query).scalars())

        inspected = 0
        alerted = 0
        immediate_alerted = 0
        commented = 0
        comment_skipped_duplicate = 0
        skipped = 0
        failed = 0
        digest_items = []
        digest_posted = False
        urgency_counts: dict[str, int] = {}
        assignee_counts: dict[str, int] = {}
        for lead in leads:
            if processing_limit and inspected >= processing_limit:
                break
            status_key = normalize_status_key(lead.status or "")
            if status_key not in self.settings.active_statuses:
                continue
            inspected += 1
            try:
                comments = self.clickup_client.get_task_comments(lead.clickup_task_id)
                evaluation = self.reminders.evaluate_lead(lead, as_of_date=effective_date, comments=comments)
                if evaluation is None:
                    continue

                dedupe_key = self.reminders.build_dedupe_key(evaluation)
                if self.audit.has_successful_action(dedupe_key):
                    skipped += 1
                    continue

                digest_item = self.reminders.build_digest_item(evaluation)
                digest_items.append(digest_item)
                if dry_run:
                    continue

                if digest_item.notification_mode == "immediate_alert":
                    slack_payload = self.reminders.build_immediate_stale_slack_message(evaluation)
                    slack_result = self.slack_client.post_message(**slack_payload)
                    if not slack_result.get("skipped"):
                        alerted += 1
                        immediate_alerted += 1
                        self.audit.record_action(
                            run_id=run.id,
                            clickup_task_id=lead.clickup_task_id,
                            system="slack",
                            action_type="stale_lead_immediate_alert",
                            dedupe_key=dedupe_key,
                            before={"status": lead.status, "follow_up_state": lead.follow_up_state},
                            after=slack_result,
                        )

                comment_text = self.reminders.build_agent_comment(evaluation)
                if self.reminders.should_skip_agent_comment(evaluation=evaluation, comments=comments):
                    comment_skipped_duplicate += 1
                else:
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
            except Exception as exc:
                failed += 1
                logger.exception("stale lead processing failed for task %s", lead.clickup_task_id)
                self.audit.record_action(
                    run_id=run.id,
                    clickup_task_id=lead.clickup_task_id,
                    system="sales_support_agent",
                    action_type="stale_lead_processing_failed",
                    success=False,
                    error_message=str(exc),
                    before={"status": lead.status, "follow_up_state": lead.follow_up_state},
                    after={},
                )

        urgency_counts = {
            urgency: sum(1 for item in digest_items if item.urgency == urgency)
            for urgency in ("overdue", "needs_immediate_review", "follow_up_due")
            if any(item.urgency == urgency for item in digest_items)
        }
        assignee_counts = {}
        for item in digest_items:
            assignee_counts[item.owner_display] = assignee_counts.get(item.owner_display, 0) + 1

        if not dry_run and self.settings.stale_lead_slack_digest_enabled and digest_items:
            digest_dedupe_key = f"stale_lead_digest:{run.id}"
            if not self.audit.has_successful_action(digest_dedupe_key):
                try:
                    digest_payload = self.reminders.build_stale_digest_message(digest_items)
                    if digest_payload:
                        digest_result = self.slack_client.post_message(**digest_payload)
                        if not digest_result.get("skipped"):
                            digest_posted = True
                            alerted += 1
                            self.audit.record_action(
                                run_id=run.id,
                                clickup_task_id="",
                                system="slack",
                                action_type="stale_lead_digest",
                                dedupe_key=digest_dedupe_key,
                                before={
                                    "digest_items": len(digest_items),
                                    "urgency_counts": urgency_counts,
                                    "assignee_counts": assignee_counts,
                                },
                                after=digest_result,
                            )
                except Exception as exc:
                    failed += 1
                    logger.exception("stale lead digest notification failed")
                    self.audit.record_action(
                        run_id=run.id,
                        clickup_task_id="",
                        system="sales_support_agent",
                        action_type="stale_lead_digest_failed",
                        dedupe_key=digest_dedupe_key,
                        success=False,
                        error_message=str(exc),
                        before={
                            "digest_items": len(digest_items),
                            "urgency_counts": urgency_counts,
                            "assignee_counts": assignee_counts,
                        },
                        after={},
                    )

        self.audit.finish_run(
            run,
            status="success",
            summary={
                "inspected": inspected,
                "alerted": alerted,
                "immediate_alerted": immediate_alerted,
                "commented": commented,
                "comment_skipped_duplicate": comment_skipped_duplicate,
                "skipped_deduped": skipped,
                "failed": failed,
                "digest_posted": digest_posted,
                "digest_items": len(digest_items),
                "urgency_counts": urgency_counts,
                "assignee_counts": assignee_counts,
                "sync_failed": sync_failed,
                "synced_tasks": sync_summary.get("synced_tasks", 0),
            },
        )
        return {
            "status": "ok",
            "inspected": inspected,
            "alerted": alerted,
            "immediate_alerted": immediate_alerted,
            "commented": commented,
            "comment_skipped_duplicate": comment_skipped_duplicate,
            "skipped_deduped": skipped,
            "failed": failed,
            "digest_posted": digest_posted,
            "digest_items": len(digest_items),
            "urgency_counts": urgency_counts,
            "assignee_counts": assignee_counts,
            "sync_failed": sync_failed,
            "synced_tasks": int(sync_summary.get("synced_tasks", 0)),
        }
