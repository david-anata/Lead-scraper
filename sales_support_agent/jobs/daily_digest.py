"""Daily SDR email digest job."""

from __future__ import annotations

import logging
from datetime import date

from sqlalchemy import select
from sqlalchemy.orm import Session

from sales_support_agent.config import Settings, normalize_status_key
from sales_support_agent.integrations.clickup import ClickUpClient
from sales_support_agent.integrations.gmail import GmailClient
from sales_support_agent.models.entities import LeadMirror
from sales_support_agent.services.audit import AuditService
from sales_support_agent.services.daily_digest import (
    build_daily_digest_subject,
    build_daily_digest_text,
    fetch_mailbox_signals,
)
from sales_support_agent.services.reminders import ReminderService
from sales_support_agent.services.sync import ClickUpSyncService


logger = logging.getLogger(__name__)


class DailyDigestJob:
    def __init__(
        self,
        settings: Settings,
        clickup_client: ClickUpClient,
        gmail_client: GmailClient,
        session: Session,
    ):
        self.settings = settings
        self.clickup_client = clickup_client
        self.gmail_client = gmail_client
        self.session = session
        self.audit = AuditService(session)
        self.reminders = ReminderService(settings, session)

    def run(
        self,
        *,
        as_of_date: date | None = None,
        include_stale: bool = True,
        include_mailbox: bool = True,
        max_items: int | None = None,
    ) -> dict[str, int | str | bool]:
        effective_date = as_of_date or date.today()
        run = self.audit.start_run(
            "daily_email_digest",
            trigger="manual",
            metadata={
                "as_of_date": effective_date.isoformat(),
                "include_stale": include_stale,
                "include_mailbox": include_mailbox,
            },
        )
        if not self.settings.daily_digest_enabled:
            self.audit.finish_run(run, status="success", summary={"status": "skipped", "reason": "daily_digest_disabled"})
            return {"status": "skipped", "reason": "daily_digest_disabled"}
        if not self.gmail_client.is_configured():
            self.audit.finish_run(run, status="success", summary={"status": "skipped", "reason": "gmail_not_configured"})
            return {"status": "skipped", "reason": "gmail_not_configured"}
        if not self.settings.daily_digest_email_to:
            self.audit.finish_run(run, status="success", summary={"status": "skipped", "reason": "missing_daily_digest_email_to"})
            return {"status": "skipped", "reason": "missing_daily_digest_email_to"}

        dedupe_key = f"daily_email_digest:{effective_date.isoformat()}"
        if self.audit.has_successful_action(dedupe_key):
            self.audit.finish_run(run, status="success", summary={"status": "skipped_duplicate"})
            return {"status": "skipped", "reason": "already_sent_for_date"}

        digest_limit = max_items if max_items is not None else self.settings.daily_digest_max_items
        stale_items = self._collect_stale_digest_items(effective_date) if include_stale else []
        mailbox_signals = fetch_mailbox_signals(self.session, as_of_date=effective_date, max_items=digest_limit) if include_mailbox else []
        if not stale_items and not mailbox_signals:
            self.audit.finish_run(run, status="success", summary={"status": "skipped", "reason": "no_items"})
            return {"status": "skipped", "reason": "no_items"}

        subject = build_daily_digest_subject(prefix=self.settings.daily_digest_subject_prefix, as_of_date=effective_date)
        text = build_daily_digest_text(
            as_of_date=effective_date,
            stale_items=stale_items,
            mailbox_signals=mailbox_signals,
            max_items=digest_limit,
        )
        result = self.gmail_client.send_message(
            to=self.settings.daily_digest_email_to,
            cc=self.settings.daily_digest_email_cc,
            subject=subject,
            text=text,
        )
        self.audit.record_action(
            run_id=run.id,
            clickup_task_id="",
            system="gmail",
            action_type="daily_digest_sent",
            dedupe_key=dedupe_key,
            before={
                "stale_items": len(stale_items),
                "mailbox_signals": len(mailbox_signals),
            },
            after=result,
        )
        summary = {
            "status": "ok",
            "stale_items": len(stale_items),
            "mailbox_signals": len(mailbox_signals),
            "email_sent": True,
        }
        self.audit.finish_run(run, status="success", summary=summary)
        return summary

    def _collect_stale_digest_items(self, effective_date: date):
        ClickUpSyncService(self.settings, self.clickup_client, self.session).sync_list(
            include_closed=True,
            max_tasks=self.settings.stale_lead_scan_sync_max_tasks,
        )
        query = (
            select(LeadMirror)
            .where(LeadMirror.list_id == self.settings.clickup_list_id)
            .order_by(LeadMirror.updated_at.asc(), LeadMirror.last_sync_at.asc())
        )
        items = []
        for lead in self.session.execute(query).scalars():
            status_key = normalize_status_key(lead.status or "")
            if status_key not in self.settings.active_statuses:
                continue
            comments = self.clickup_client.get_task_comments(lead.clickup_task_id)
            evaluation = self.reminders.evaluate_lead(lead, as_of_date=effective_date, comments=comments)
            if evaluation is None:
                continue
            items.append(self.reminders.build_digest_item(evaluation))
        return items
