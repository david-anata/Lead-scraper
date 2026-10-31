"""Voice receptionist tables: call log, voice consent, and do-not-call list."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import JSON, DateTime, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from sales_support_agent.models.database import Base


class VoiceCall(Base):
    __tablename__ = "voice_calls"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    call_sid: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    direction: Mapped[str] = mapped_column(String(16), default="inbound", index=True)
    from_number: Mapped[str] = mapped_column(String(32), default="", index=True)
    to_number: Mapped[str] = mapped_column(String(32), default="")
    status: Mapped[str] = mapped_column(String(32), default="ringing", index=True)
    outcome: Mapped[str] = mapped_column(String(64), default="", index=True)
    disclosure_text: Mapped[str] = mapped_column(Text, default="")
    disclosure_played_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    matched_inquiry_id: Mapped[str] = mapped_column(String(64), default="", index=True)
    caller_name: Mapped[str] = mapped_column(String(255), default="")
    caller_email: Mapped[str] = mapped_column(String(255), default="")
    transcript_json: Mapped[list] = mapped_column(JSON, default=list)
    tool_calls_json: Mapped[list] = mapped_column(JSON, default=list)
    summary: Mapped[str] = mapped_column(Text, default="")
    tour_event_id: Mapped[str] = mapped_column(String(255), default="")
    tour_start_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    transferred: Mapped[bool] = mapped_column(default=False)
    summary_email_status: Mapped[str] = mapped_column(String(32), default="")
    error: Mapped[str] = mapped_column(Text, default="")
    duration_seconds: Mapped[int] = mapped_column(Integer, default=0)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, index=True)
    ended_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)


class VoiceConsent(Base):
    """Express consent to AI-voice calls, captured with its exact wording."""

    __tablename__ = "voice_consents"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    phone: Mapped[str] = mapped_column(String(32), index=True)
    email: Mapped[str] = mapped_column(String(255), default="", index=True)
    source: Mapped[str] = mapped_column(String(64), default="", index=True)
    source_ref: Mapped[str] = mapped_column(String(128), default="")
    consent_text: Mapped[str] = mapped_column(Text, default="")
    granted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    revoked_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    revoked_reason: Mapped[str] = mapped_column(String(128), default="")


class VoiceDoNotCall(Base):
    __tablename__ = "voice_dnc"

    phone: Mapped[str] = mapped_column(String(32), primary_key=True)
    reason: Mapped[str] = mapped_column(String(128), default="caller_request")
    source: Mapped[str] = mapped_column(String(64), default="voice_agent")
    call_sid: Mapped[str] = mapped_column(String(64), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
