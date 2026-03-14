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

    def _query_by_email(self, email: str) -> LeadMirror | None:
        query = (
            select(LeadMirror)
            .where(func.lower(LeadMirror.email) == email)
            .order_by(LeadMirror.updated_at.desc().nullslast(), LeadMirror.last_sync_at.desc())
            .limit(1)
        )
        return self.session.execute(query).scalar_one_or_none()

