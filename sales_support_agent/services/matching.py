"""Helpers for matching upstream events back to existing ClickUp tasks."""

from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from sales_support_agent.config import Settings
from sales_support_agent.integrations.clickup import ClickUpClient
from sales_support_agent.models.entities import LeadMirror
from sales_support_agent.services.sync import ClickUpSyncService


class LeadMatchingService:
    def __init__(self, settings: Settings, clickup_client: ClickUpClient, session: Session):
        self.settings = settings
        self.clickup_client = clickup_client
        self.session = session

    def find_by_email(self, email: str, *, sync_on_miss: bool = True) -> LeadMirror | None:
        normalized_email = (email or "").strip().lower()
        if not normalized_email:
            return None

        match = self._query_by_email(normalized_email)
        if match is not None or not sync_on_miss:
            return match

        ClickUpSyncService(self.settings, self.clickup_client, self.session).sync_list(include_closed=True)
        return self._query_by_email(normalized_email)

    def find_by_candidate_emails(self, emails: tuple[str, ...], *, sync_on_miss: bool = True) -> LeadMirror | None:
        candidates = tuple(email.strip().lower() for email in emails if email and email.strip())
        for candidate in candidates:
            match = self._query_by_email(candidate)
            if match is not None:
                return match

        if not candidates or not sync_on_miss:
            return None

        ClickUpSyncService(self.settings, self.clickup_client, self.session).sync_list(include_closed=True)
        for candidate in candidates:
            match = self._query_by_email(candidate)
            if match is not None:
                return match
        return None

    def find_mailbox_match(
        self,
        *,
        sender_email: str,
        sender_domain: str,
        candidate_emails: tuple[str, ...],
        sync_on_miss: bool = True,
    ) -> LeadMirror | None:
        normalized_sender = (sender_email or "").strip().lower()
        normalized_candidates = tuple(
            email.strip().lower()
            for email in candidate_emails
            if email and email.strip()
        )

        # Mail we sent is never a lead replying to us.
        #
        # The daily digest goes out through the same mailbox the sync polls, and the
        # poll query has no sender filter, so it comes straight back in as inbound.
        # When one of our own addresses is ALSO a lead in ClickUp - a test form
        # submission using your own address - the sender matched that lead directly
        # and every digest logged a "reply" and pinged the assignee in Slack.
        #
        # Guarding here rather than only in the Gmail query is deliberate: the query
        # is configurable and per-account, so a future account added without the
        # exclusion would silently reopen the loop. This is the backstop.
        self_addresses = {
            address.strip().lower()
            for address in getattr(self.settings, "gmail_self_addresses", ())
            if address and address.strip()
        }
        if normalized_sender and normalized_sender in self_addresses:
            return None

        def _usable(match: LeadMirror | None) -> LeadMirror | None:
            """Never attribute mail to a lead whose own address is one of ours.

            A test form submission made with an internal address becomes a magnet:
            every message that mentions or is sent from that address resolves to it.
            In production one such record collected wedding enquiries, supplier
            invoices, HR mail and fundraising threads, all logged as that lead
            replying, every 15 minutes.
            """
            if match is None:
                return None
            if (match.email or "").strip().lower() in self_addresses:
                return None
            return match

        sender_match = _usable(self._query_by_email(normalized_sender)) if normalized_sender else None
        if sender_match is not None:
            return sender_match

        source_domains = {domain.strip().lower() for domain in self.settings.gmail_source_domains if domain and domain.strip()}
        allow_body_fallback = (sender_domain or "").strip().lower() in source_domains
        fallback_candidates = tuple(candidate for candidate in normalized_candidates if candidate != normalized_sender)
        if allow_body_fallback:
            for candidate in fallback_candidates:
                match = _usable(self._query_by_email(candidate))
                if match is not None:
                    return match

        if not sync_on_miss:
            return None

        ClickUpSyncService(self.settings, self.clickup_client, self.session).sync_list(include_closed=True)

        sender_match = _usable(self._query_by_email(normalized_sender)) if normalized_sender else None
        if sender_match is not None:
            return sender_match

        if allow_body_fallback:
            for candidate in fallback_candidates:
                match = _usable(self._query_by_email(candidate))
                if match is not None:
                    return match
        return None

    def _query_by_email(self, email: str) -> LeadMirror | None:
        query = (
            select(LeadMirror)
            .where(func.lower(LeadMirror.email) == email)
            .order_by(LeadMirror.updated_at.desc().nullslast(), LeadMirror.last_sync_at.desc())
            .limit(1)
        )
        return self.session.execute(query).scalar_one_or_none()
