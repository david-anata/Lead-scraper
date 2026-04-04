"""Pydantic schemas for API requests and responses."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field


CommunicationEventType = Literal[
    "outbound_email_sent",
    "inbound_reply_received",
    "call_completed",
    "meeting_completed",
    "offer_sent",
    "note_logged",
]


class DiscoveryRequest(BaseModel):
    sample_size: int = Field(default=10, ge=1, le=50)


class SyncRequest(BaseModel):
    max_tasks: Optional[int] = Field(default=None, ge=1, le=1000)
    include_closed: bool = True


class StaleLeadRunRequest(BaseModel):
    dry_run: bool = False
    as_of_date: Optional[date] = None
    max_tasks: Optional[int] = Field(default=None, ge=1, le=1000)


class GmailSyncRequest(BaseModel):
    dry_run: bool = False
    query: str = ""
    max_messages: Optional[int] = Field(default=None, ge=1, le=250)


class DailyDigestRunRequest(BaseModel):
    as_of_date: Optional[date] = None
    include_stale: bool = True
    include_mailbox: bool = True
    max_items: Optional[int] = Field(default=None, ge=1, le=250)


class CommunicationEventRequest(BaseModel):
    task_id: str = Field(min_length=1)
    event_type: CommunicationEventType
    external_event_key: str = ""
    occurred_at: Optional[datetime] = None
    summary: str = ""
    outcome: str = ""
    recommended_next_action: str = ""
    suggested_reply_draft: str = ""
    next_follow_up_date: Optional[date] = None
    suggested_status: str = ""
    source: str = "manual"
    metadata: dict[str, Any] = Field(default_factory=dict)


class ApiMessage(BaseModel):
    status: str
    message: str
    details: dict[str, Any] = Field(default_factory=dict)
