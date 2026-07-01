"""Scheduled sales operator review job."""

from __future__ import annotations

import logging
from typing import Any

from sales_support_agent.config import Settings
from sales_support_agent.integrations.clickup import ClickUpClient
from sales_support_agent.integrations.gmail import GmailClient
from sales_support_agent.integrations.hubspot import HubSpotClient
from sales_support_agent.integrations.slack import SlackClient
from sales_support_agent.jobs.mailbox_sync import GmailMailboxSyncJob
from sales_support_agent.models.database import session_scope
from sales_support_agent.services.audit import AuditService
from sales_support_agent.services.hubspot_sync.service import sync_hubspot_sales
from sales_support_agent.services.sales.operator_dashboard import (
    get_operator_snapshot,
    run_writeback,
)


logger = logging.getLogger(__name__)


class SalesOperatorReviewJob:
    def __init__(self, settings: Settings, session_factory: Any):
        self.settings = settings
        self.session_factory = session_factory

    def run(
        self,
        *,
        dry_run: bool = False,
        limit: int = 25,
        run_hubspot_sync: bool = True,
        run_mailbox_sync: bool = False,
        max_messages: int | None = None,
        trigger: str = "manual",
    ) -> dict[str, Any]:
        metadata = {
            "dry_run": dry_run,
            "limit": limit,
            "run_hubspot_sync": run_hubspot_sync,
            "run_mailbox_sync": run_mailbox_sync,
            "max_messages": max_messages,
        }
        with session_scope(self.session_factory) as session:
            audit = AuditService(session)
            run = audit.start_run("sales_operator_review", trigger=trigger, metadata=metadata)
            try:
                hubspot_sync_summary: dict[str, Any] = {"status": "skipped", "reason": "disabled"}
                if run_hubspot_sync:
                    sync_result = sync_hubspot_sales(session, HubSpotClient(self.settings), self.settings)
                    hubspot_sync_summary = {
                        "status": "completed" if sync_result.as_dict().get("ok") else "completed_with_errors",
                        **sync_result.as_dict(),
                    }

                mailbox_sync_summary: dict[str, Any] = {"status": "skipped", "reason": "disabled"}
                if run_mailbox_sync:
                    mailbox_sync_summary = GmailMailboxSyncJob(
                        self.settings,
                        ClickUpClient(self.settings),
                        SlackClient(self.settings),
                        GmailClient(self.settings),
                        session,
                    ).run(
                        dry_run=dry_run,
                        max_messages=max_messages,
                        trigger=trigger,
                    )

                writeback = run_writeback(
                    self.settings,
                    session_factory=self.session_factory,
                    mode="preview" if dry_run else "apply",
                    limit=max(1, min(limit, 25)),
                )
                snapshot = get_operator_snapshot(
                    self.settings,
                    session_factory=self.session_factory,
                    force_refresh=True,
                )
                next_action = ""
                recent_deals = list(snapshot.get("recentDeals") or [])
                if recent_deals:
                    actions = list((recent_deals[0].get("proposedActions") or []))
                    if actions:
                        next_action = str(actions[0].get("title") or "").strip()
                summary = {
                    "status": "completed",
                    "hubspot_sync": hubspot_sync_summary,
                    "mailbox_sync": mailbox_sync_summary,
                    "writeback": dict(writeback.get("summary") or {}),
                    "next_action": next_action,
                    "candidate_deals": int((writeback.get("summary") or {}).get("candidateDeals") or 0),
                    "applied_actions": int((writeback.get("summary") or {}).get("appliedActions") or 0),
                    "deferred_actions": int((writeback.get("summary") or {}).get("deferredActions") or 0),
                }
                audit.finish_run(run, status="success", summary=summary)
                return summary
            except Exception as exc:  # noqa: BLE001
                logger.exception("[sales_operator_review] run failed")
                summary = {"status": "failed", "error": str(exc)}
                audit.finish_run(run, status="failed", summary=summary)
                return summary
