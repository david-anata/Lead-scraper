"""Gmail mailbox polling job."""

from __future__ import annotations

import logging

from sqlalchemy.orm import Session

from sales_support_agent.config import Settings
from sales_support_agent.integrations.clickup import ClickUpClient
from sales_support_agent.integrations.gmail import GmailClient, GmailIntegrationError
from sales_support_agent.integrations.gmail_payloads import normalize_gmail_message
from sales_support_agent.integrations.slack import SlackClient
from sales_support_agent.models.entities import MailboxSignal
from sales_support_agent.models.schemas import CommunicationEventRequest
from sales_support_agent.services.audit import AuditService
from sales_support_agent.services.communications import CommunicationService
from sales_support_agent.services.matching import LeadMatchingService


logger = logging.getLogger(__name__)


class GmailMailboxSyncJob:
    def __init__(
        self,
        settings: Settings,
        clickup_client: ClickUpClient,
        slack_client: SlackClient,
        gmail_client: GmailClient,
        session: Session,
    ):
        self.settings = settings
        self.clickup_client = clickup_client
        self.slack_client = slack_client
        self.gmail_client = gmail_client
        self.session = session
        self.audit = AuditService(session)

    def run(self, *, dry_run: bool = False, query: str | None = None, max_messages: int | None = None) -> dict[str, int | str | bool]:
        run = self.audit.start_run(
            "gmail_mailbox_sync",
            trigger="manual",
            metadata={"dry_run": dry_run, "query": query or self.settings.gmail_poll_query},
        )
        if not self.gmail_client.is_configured():
            summary = {
                "status": "skipped",
                "reason": "gmail_not_configured",
                "missing_configuration": list(self.gmail_client.missing_configuration()),
            }
            self.audit.finish_run(run, status="success", summary=summary)
            return summary

        fetched = 0
        processed = 0
        matched = 0
        unmatched = 0
        skipped = 0
        failed = 0
        max_results = max_messages if max_messages is not None else self.settings.gmail_poll_max_messages
        mailbox_query = query or self.settings.gmail_poll_query
        preflight: dict[str, object] = {}
        matcher = LeadMatchingService(self.settings, self.clickup_client, self.session)
        communication_service = CommunicationService(
            self.settings,
            self.clickup_client,
            self.slack_client,
            self.session,
        )

        try:
            preflight = self.gmail_client.debug_preflight()
            message_refs = self.gmail_client.list_messages(query=mailbox_query, max_results=max_results)
        except GmailIntegrationError as exc:
            summary = {
                "status": "failed",
                "stage": exc.stage,
                "query": mailbox_query,
                "max_messages": max_results,
                **exc.as_dict(),
            }
            self.audit.finish_run(run, status="failed", summary=summary)
            return summary
        except Exception as exc:
            summary = {
                "status": "failed",
                "stage": "mailbox_sync",
                "query": mailbox_query,
                "max_messages": max_results,
                "error_code": "unexpected_error",
                "error": str(exc),
                "hint": "Inspect the sales-support-agent logs for the mailbox sync run and verify Gmail auth and message normalization.",
            }
            self.audit.finish_run(run, status="failed", summary=summary)
            return summary

        for message_ref in message_refs:
            fetched += 1
            message_id = str(message_ref.get("id") or "")
            dedupe_key = f"gmail_message:{message_id}"
            if self.audit.has_successful_action(dedupe_key):
                skipped += 1
                continue

            try:
                message_payload = self.gmail_client.get_message(message_id)
                initial = normalize_gmail_message(
                    message_payload,
                    configured_source_domains=self.settings.gmail_source_domains,
                    matched_task=False,
                )
                lead = matcher.find_mailbox_match(
                    sender_email=initial.sender_email,
                    sender_domain=initial.sender_domain,
                    candidate_emails=initial.candidate_emails,
                    sync_on_miss=True,
                )
                normalized = normalize_gmail_message(
                    message_payload,
                    configured_source_domains=self.settings.gmail_source_domains,
                    matched_task=lead is not None,
                )
                signal = self._build_signal(normalized, lead.clickup_task_id if lead else "", lead.task_name if lead else "", lead.task_url if lead else "", lead.status if lead else "", lead.assignee_id if lead else "", lead.assignee_name if lead else "")
                if not dry_run:
                    self.session.add(signal)
                    self.session.flush()
                    if lead is not None and normalized.classification in {"reply_received", "pricing_or_offer_request", "meeting_action_needed"}:
                        communication_service.process_event(
                            CommunicationEventRequest(
                                task_id=lead.clickup_task_id,
                                event_type="inbound_reply_received",
                                external_event_key=normalized.external_event_key,
                                occurred_at=normalized.occurred_at,
                                summary=normalized.action_summary,
                                recommended_next_action=normalized.recommended_next_action,
                                suggested_reply_draft=normalized.suggested_reply_draft,
                                source="gmail_mailbox",
                                metadata={
                                    "classification": normalized.classification,
                                    "sender_email": normalized.sender_email,
                                    "sender_name": normalized.sender_name,
                                    "sender_domain": normalized.sender_domain,
                                    "subject": normalized.subject,
                                    "snippet": normalized.snippet,
                                    "gmail_message_id": normalized.external_message_id,
                                    "gmail_thread_id": normalized.external_thread_id,
                                },
                            )
                        )
                        matched += 1
                    elif lead is None:
                        unmatched += 1
                else:
                    if lead is not None:
                        matched += 1
                    else:
                        unmatched += 1

                processed += 1
                self.audit.record_action(
                    run_id=run.id,
                    clickup_task_id=lead.clickup_task_id if lead else "",
                    system="gmail",
                    action_type="gmail_message_processed",
                    dedupe_key=dedupe_key,
                    before={"query": mailbox_query},
                    after={
                        "classification": normalized.classification,
                        "sender_email": normalized.sender_email,
                        "matched_task_id": lead.clickup_task_id if lead else "",
                    },
                )
            except Exception as exc:
                failed += 1
                logger.exception("gmail mailbox sync failed for message %s", message_id)
                self.audit.record_action(
                    run_id=run.id,
                    clickup_task_id="",
                    system="gmail",
                    action_type="gmail_message_failed",
                    dedupe_key=dedupe_key,
                    success=False,
                    error_message=str(exc),
                    before={"message_id": message_id},
                    after={},
                )

        summary = {
            "status": "ok",
            "query": mailbox_query,
            "max_messages": max_results,
            "preflight": preflight,
            "fetched": fetched,
            "processed": processed,
            "matched": matched,
            "unmatched": unmatched,
            "skipped_deduped": skipped,
            "failed": failed,
        }
        self.audit.finish_run(run, status="success", summary=summary)
        return summary

    def _build_signal(
        self,
        normalized,
        matched_task_id: str,
        task_name: str,
        task_url: str,
        task_status: str,
        owner_id: str,
        owner_name: str,
    ) -> MailboxSignal:
        return MailboxSignal(
            provider="gmail",
            external_message_id=normalized.external_message_id,
            external_thread_id=normalized.external_thread_id,
            dedupe_key=f"gmail_message:{normalized.external_message_id}",
            matched_task_id=matched_task_id,
            sender_name=normalized.sender_name,
            sender_email=normalized.sender_email,
            sender_domain=normalized.sender_domain,
            subject=normalized.subject,
            snippet=normalized.snippet,
            body_text=normalized.body_text,
            classification=normalized.classification,
            urgency=normalized.urgency,
            owner_id=owner_id,
            owner_name=owner_name,
            task_name=task_name,
            task_url=task_url,
            task_status=task_status,
            action_summary=normalized.action_summary,
            suggested_reply_draft=normalized.suggested_reply_draft,
            received_at=normalized.occurred_at,
            raw_payload=normalized.raw_payload,
        )
