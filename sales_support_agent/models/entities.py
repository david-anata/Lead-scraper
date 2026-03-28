"""SQLAlchemy entities for local auditability and task mirrors."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import JSON, Boolean, DateTime, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from sales_support_agent.models.database import Base


class LeadMirror(Base):
    __tablename__ = "lead_mirrors"

    clickup_task_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    list_id: Mapped[str] = mapped_column(String(64), index=True)
    task_name: Mapped[str] = mapped_column(String(255))
    task_url: Mapped[str] = mapped_column(String(1024), default="")
    status: Mapped[str] = mapped_column(String(128), index=True)
    status_key: Mapped[str] = mapped_column(String(128), default="", index=True)
    is_closed: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    assignee_id: Mapped[str] = mapped_column(String(64), default="")
    assignee_name: Mapped[str] = mapped_column(String(255), default="")
    priority: Mapped[str] = mapped_column(String(64), default="")
    product: Mapped[str] = mapped_column(String(255), default="")
    source: Mapped[str] = mapped_column(String(255), default="")
    email: Mapped[str] = mapped_column(String(255), default="")
    phone_number: Mapped[str] = mapped_column(String(128), default="")
    value: Mapped[str] = mapped_column(String(128), default="")
    created_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    task_updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    due_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_meaningful_touch_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_outbound_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_inbound_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    next_follow_up_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    communication_summary: Mapped[str] = mapped_column(Text, default="")
    last_meeting_outcome: Mapped[str] = mapped_column(Text, default="")
    recommended_next_action: Mapped[str] = mapped_column(Text, default="")
    follow_up_state: Mapped[str] = mapped_column(String(64), default="")
    last_sync_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    raw_task_payload: Mapped[dict] = mapped_column(JSON, default=dict)


class Company(Base):
    __tablename__ = "companies"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    domain: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    company_name: Mapped[str] = mapped_column(String(255), default="")
    normalized_name: Mapped[str] = mapped_column(String(255), default="", index=True)
    website: Mapped[str] = mapped_column(String(1024), default="")
    platform: Mapped[str] = mapped_column(String(128), default="")
    location: Mapped[str] = mapped_column(String(255), default="")
    market_segment: Mapped[str] = mapped_column(String(255), default="")
    industry: Mapped[str] = mapped_column(String(255), default="")
    org_category: Mapped[str] = mapped_column(String(128), default="", index=True)
    apollo_org_id: Mapped[str] = mapped_column(String(128), default="", index=True)
    source_system: Mapped[str] = mapped_column(String(64), default="apollo")
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, index=True)
    last_exported_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)


class Contact(Base):
    __tablename__ = "contacts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    company_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    email: Mapped[str] = mapped_column(String(255), default="", index=True)
    linkedin_url: Mapped[str] = mapped_column(String(1024), default="", index=True)
    apollo_person_id: Mapped[str] = mapped_column(String(128), default="", index=True)
    full_name: Mapped[str] = mapped_column(String(255), default="")
    first_name: Mapped[str] = mapped_column(String(255), default="")
    last_name: Mapped[str] = mapped_column(String(255), default="")
    role: Mapped[str] = mapped_column(String(255), default="")
    department: Mapped[str] = mapped_column(String(128), default="")
    source_system: Mapped[str] = mapped_column(String(64), default="apollo")
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class LeadRecord(Base):
    __tablename__ = "lead_records"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    lead_key: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    company_id: Mapped[int] = mapped_column(Integer, index=True)
    contact_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    channel: Mapped[str] = mapped_column(String(64), default="email", index=True)
    status: Mapped[str] = mapped_column(String(64), default="accepted", index=True)
    source_run_id: Mapped[str] = mapped_column(String(64), default="", index=True)
    last_skip_reason: Mapped[str] = mapped_column(String(255), default="")
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, index=True)
    last_qualified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class LeadRun(Base):
    __tablename__ = "lead_runs"

    run_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    status: Mapped[str] = mapped_column(String(32), default="queued", index=True)
    trigger_source: Mapped[str] = mapped_column(String(64), default="manual", index=True)
    current_stage: Mapped[str] = mapped_column(String(64), default="queued")
    run_date: Mapped[str] = mapped_column(String(32), default="", index=True)
    max_domains: Mapped[int] = mapped_column(Integer, default=150)
    request_json: Mapped[dict] = mapped_column(JSON, default=dict)
    summary_json: Mapped[dict] = mapped_column(JSON, default=dict)
    csv_content: Mapped[str] = mapped_column(Text, default="")
    error_message: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, index=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)


class LeadRunItem(Base):
    __tablename__ = "lead_run_items"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(64), index=True)
    company_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    contact_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    domain: Mapped[str] = mapped_column(String(255), default="", index=True)
    stage: Mapped[str] = mapped_column(String(64), default="", index=True)
    status: Mapped[str] = mapped_column(String(32), default="pending", index=True)
    reason: Mapped[str] = mapped_column(String(255), default="")
    payload_json: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class CampaignEnrollment(Base):
    __tablename__ = "campaign_enrollments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    enrollment_key: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    lead_record_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    company_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    contact_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    channel: Mapped[str] = mapped_column(String(64), default="", index=True)
    campaign_id: Mapped[str] = mapped_column(String(128), default="", index=True)
    campaign_name: Mapped[str] = mapped_column(String(255), default="")
    external_id: Mapped[str] = mapped_column(String(255), default="")
    status: Mapped[str] = mapped_column(String(64), default="created", index=True)
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict)
    enrolled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class SourceCursor(Base):
    __tablename__ = "source_cursors"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_key: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    next_cursor: Mapped[str] = mapped_column(String(255), default="")
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, index=True)


class Cooldown(Base):
    __tablename__ = "cooldowns"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    scope: Mapped[str] = mapped_column(String(128), index=True)
    entity_key: Mapped[str] = mapped_column(String(255), index=True)
    result: Mapped[str] = mapped_column(String(64), default="")
    cooldown_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    last_attempted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict)


class RevenueEvent(Base):
    __tablename__ = "events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_type: Mapped[str] = mapped_column(String(128), index=True)
    subject_type: Mapped[str] = mapped_column(String(64), default="", index=True)
    subject_key: Mapped[str] = mapped_column(String(255), default="", index=True)
    run_id: Mapped[str] = mapped_column(String(64), default="", index=True)
    payload_json: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, index=True)


class RevenueAction(Base):
    __tablename__ = "actions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    action_type: Mapped[str] = mapped_column(String(128), index=True)
    subject_type: Mapped[str] = mapped_column(String(64), default="", index=True)
    subject_key: Mapped[str] = mapped_column(String(255), default="", index=True)
    run_id: Mapped[str] = mapped_column(String(64), default="", index=True)
    success: Mapped[bool] = mapped_column(Boolean, default=True)
    payload_json: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, index=True)


class CommunicationEvent(Base):
    __tablename__ = "communication_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    clickup_task_id: Mapped[str] = mapped_column(String(64), index=True)
    event_type: Mapped[str] = mapped_column(String(64), index=True)
    external_event_key: Mapped[str] = mapped_column(String(255), default="", index=True)
    source: Mapped[str] = mapped_column(String(64), default="manual")
    summary: Mapped[str] = mapped_column(Text, default="")
    outcome: Mapped[str] = mapped_column(Text, default="")
    recommended_next_action: Mapped[str] = mapped_column(Text, default="")
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, index=True)
    raw_payload: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class MailboxSignal(Base):
    __tablename__ = "mailbox_signals"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    provider: Mapped[str] = mapped_column(String(64), default="gmail", index=True)
    external_message_id: Mapped[str] = mapped_column(String(255), default="", index=True)
    external_thread_id: Mapped[str] = mapped_column(String(255), default="", index=True)
    dedupe_key: Mapped[str] = mapped_column(String(255), default="", index=True)
    matched_task_id: Mapped[str] = mapped_column(String(64), default="", index=True)
    sender_name: Mapped[str] = mapped_column(String(255), default="")
    sender_email: Mapped[str] = mapped_column(String(255), default="", index=True)
    sender_domain: Mapped[str] = mapped_column(String(255), default="", index=True)
    subject: Mapped[str] = mapped_column(Text, default="")
    snippet: Mapped[str] = mapped_column(Text, default="")
    body_text: Mapped[str] = mapped_column(Text, default="")
    classification: Mapped[str] = mapped_column(String(64), default="", index=True)
    urgency: Mapped[str] = mapped_column(String(64), default="follow_up_due", index=True)
    owner_id: Mapped[str] = mapped_column(String(64), default="")
    owner_name: Mapped[str] = mapped_column(String(255), default="")
    task_name: Mapped[str] = mapped_column(String(255), default="")
    task_url: Mapped[str] = mapped_column(String(1024), default="")
    task_status: Mapped[str] = mapped_column(String(128), default="")
    action_summary: Mapped[str] = mapped_column(Text, default="")
    suggested_reply_draft: Mapped[str] = mapped_column(Text, default="")
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, index=True)
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    raw_payload: Mapped[dict] = mapped_column(JSON, default=dict)


class AutomationRun(Base):
    __tablename__ = "automation_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_type: Mapped[str] = mapped_column(String(64), index=True)
    status: Mapped[str] = mapped_column(String(32), default="running")
    trigger: Mapped[str] = mapped_column(String(64), default="manual")
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict)
    summary_json: Mapped[dict] = mapped_column(JSON, default=dict)


class AutomationAction(Base):
    __tablename__ = "automation_actions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    clickup_task_id: Mapped[str] = mapped_column(String(64), default="", index=True)
    system: Mapped[str] = mapped_column(String(64), index=True)
    action_type: Mapped[str] = mapped_column(String(64), index=True)
    dedupe_key: Mapped[str] = mapped_column(String(255), default="", index=True)
    success: Mapped[bool] = mapped_column(Boolean, default=True)
    error_message: Mapped[str] = mapped_column(Text, default="")
    before_json: Mapped[dict] = mapped_column(JSON, default=dict)
    after_json: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class IntegrationLog(Base):
    __tablename__ = "integration_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    provider: Mapped[str] = mapped_column(String(64), index=True)
    operation: Mapped[str] = mapped_column(String(128), index=True)
    request_json: Mapped[dict] = mapped_column(JSON, default=dict)
    response_json: Mapped[dict] = mapped_column(JSON, default=dict)
    status_code: Mapped[int] = mapped_column(Integer, default=0)
    success: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class CanvaConnection(Base):
    __tablename__ = "canva_connections"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    canva_user_id: Mapped[str] = mapped_column(String(128), default="", index=True)
    display_name: Mapped[str] = mapped_column(String(255), default="")
    scope: Mapped[str] = mapped_column(Text, default="")
    access_token_encrypted: Mapped[str] = mapped_column(Text, default="")
    refresh_token_encrypted: Mapped[str] = mapped_column(Text, default="")
    token_type: Mapped[str] = mapped_column(String(64), default="Bearer")
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    capabilities_json: Mapped[dict] = mapped_column(JSON, default=dict)
    last_validated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
