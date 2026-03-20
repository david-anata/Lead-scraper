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

        sender_match = self._query_by_email(normalized_sender) if normalized_sender else None
        if sender_match is not None:
            return sender_match

        source_domains = {domain.strip().lower() for domain in self.settings.gmail_source_domains if domain and domain.strip()}
        allow_body_fallback = (sender_domain or "").strip().lower() in source_domains
        fallback_candidates = tuple(candidate for candidate in normalized_candidates if candidate != normalized_sender)
        if allow_body_fallback:
            for candidate in fallback_candidates:
                match = self._query_by_email(candidate)
                if match is not None:
                    return match

        if not sync_on_miss:
            return None

        ClickUpSyncService(self.settings, self.clickup_client, self.session).sync_list(include_closed=True)

        sender_match = self._query_by_email(normalized_sender) if normalized_sender else None
        if sender_match is not None:
            return sender_match

        if allow_body_fallback:
            for candidate in fallback_candidates:
                match = self._query_by_email(candidate)
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
