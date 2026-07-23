"""SQLAlchemy entities for local auditability and task mirrors."""

from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from sqlalchemy import JSON, Boolean, Date, DateTime, ForeignKey, Index, Integer, String, Text
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
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    task_updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    due_date: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_meaningful_touch_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_outbound_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_inbound_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    next_follow_up_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
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
    last_exported_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True, index=True)


class Contact(Base):
    __tablename__ = "contacts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    company_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, index=True)
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
    contact_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, index=True)
    channel: Mapped[str] = mapped_column(String(64), default="email", index=True)
    status: Mapped[str] = mapped_column(String(64), default="accepted", index=True)
    source_run_id: Mapped[str] = mapped_column(String(64), default="", index=True)
    last_skip_reason: Mapped[str] = mapped_column(String(255), default="")
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, index=True)
    last_qualified_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)


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
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True, index=True)


class LeadRunItem(Base):
    __tablename__ = "lead_run_items"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(64), index=True)
    company_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, index=True)
    contact_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, index=True)
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
    lead_record_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, index=True)
    company_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, index=True)
    contact_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, index=True)
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
    cooldown_until: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    last_attempted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
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
    # HubSpot deal this event belongs to (sales source of truth). Nullable for
    # back-compat with legacy ClickUp-keyed rows until ClickUp is retired.
    hubspot_deal_id: Mapped[str] = mapped_column(String(64), default="", index=True)
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
    # HubSpot deal this inbound signal was matched to (sales source of truth).
    matched_deal_id: Mapped[str] = mapped_column(String(64), default="", index=True)
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
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict)
    summary_json: Mapped[dict] = mapped_column(JSON, default=dict)


# PR54: deck-engagement analytics — proper relational tables instead of
# the JSON-blob view_events list inside AutomationRun.summary_json. JSON
# blobs can't safely handle session state that needs to update in place
# (every heartbeat would be a read-modify-write race).
class DeckVisitSession(Base):
    __tablename__ = "deck_visit_sessions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    # FK to automation_runs.id — but we don't enforce a real FK so old
    # AutomationRun rows can be deleted without orphan cleanup blocking.
    run_id: Mapped[int] = mapped_column(Integer, index=True)
    # Anonymous visitor cookie token — UUIDv4 minted client-side. Not a
    # fingerprint hash; user wants to add real auth later.
    visitor_token: Mapped[str] = mapped_column(String(64), index=True)
    # internal=admin previewing (?viewer=internal); external=prospect.
    is_internal: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, index=True
    )
    last_heartbeat_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow
    )
    # Cumulative seconds the page was foregrounded for this session.
    # Computed client-side and reported on each heartbeat (capped server-side
    # so a misbehaving client can't claim a 24-hour session).
    total_seconds: Mapped[int] = mapped_column(Integer, default=0)
    # Max scroll percentage observed during the session (0-100).
    max_scroll_pct: Mapped[int] = mapped_column(Integer, default=0)
    # Geo from CF-IPCountry header (free if Cloudflare proxies the host).
    # 2-letter ISO code; "" when unavailable.
    ip_country: Mapped[str] = mapped_column(String(8), default="")
    ip_region: Mapped[str] = mapped_column(String(64), default="")
    ip_city: Mapped[str] = mapped_column(String(96), default="")
    # User-agent parsed server-side (lightweight regex; no heavy lib).
    device: Mapped[str] = mapped_column(String(16), default="")  # desktop/mobile/tablet
    os: Mapped[str] = mapped_column(String(32), default="")
    browser: Mapped[str] = mapped_column(String(32), default="")
    user_agent_raw: Mapped[str] = mapped_column(String(512), default="")
    # Where they clicked from. Host extracted from `Referer` header,
    # categorized into direct/email/social/search/other for the dashboard.
    referrer_host: Mapped[str] = mapped_column(String(128), default="")
    referrer_category: Mapped[str] = mapped_column(String(16), default="direct")

    __table_args__ = (
        Index("ix_deck_sessions_run_visitor", "run_id", "visitor_token"),
    )


class DeckSectionView(Base):
    __tablename__ = "deck_section_views"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    session_id: Mapped[int] = mapped_column(Integer, index=True)
    # Section ID matches the deck's `id="sec-01"` etc. on each <section>.
    section_id: Mapped[str] = mapped_column(String(64), index=True)
    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow
    )
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow
    )
    # Cumulative seconds this section was visible during the session.
    total_seconds: Mapped[int] = mapped_column(Integer, default=0)

    __table_args__ = (
        Index("ix_deck_section_session_sec", "session_id", "section_id", unique=True),
    )


class AutomationAction(Base):
    __tablename__ = "automation_actions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, index=True)
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
    run_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, index=True)
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
    expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    capabilities_json: Mapped[dict] = mapped_column(JSON, default=dict)
    last_validated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


# ---------------------------------------------------------------------------
# Cashflow Controller
# ---------------------------------------------------------------------------


class CashEvent(Base):
    """A single financial cash event — either a planned obligation or a posted
    bank transaction.  All amounts are stored as integer cents to avoid
    floating-point arithmetic errors.

    source values:
        "manual"    — created by the operator inside this app
        "clickup"   — imported from a ClickUp task (transition period)
        "csv"       — parsed from an uploaded bank CSV
        "recurring" — auto-generated from a RecurringTemplate

    status values (planned side):
        "planned"   — scheduled but not yet due
        "pending"   — due within 7 days, not yet paid
        "overdue"   — past due_date, no matching bank transaction
        "paid"      — matched to a csv event or manually marked paid
        "cancelled" — removed from forecast

    status values (bank/actual side):
        "posted"    — appeared in bank CSV, not yet matched to a planned event
        "matched"   — linked to a planned obligation via matched_to_id

    confidence:
        "confirmed" — from a real source (bank CSV, QBO)
        "estimated" — manually entered or auto-generated
    """

    __tablename__ = "cash_events"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)

    # Origin
    source: Mapped[str] = mapped_column(String(32), default="manual", index=True)
    source_id: Mapped[str] = mapped_column(String(255), default="", index=True)

    # Classification
    record_kind: Mapped[str] = mapped_column(
        String(16), default="obligation", server_default="obligation", index=True
    )
    event_type: Mapped[str] = mapped_column(String(16), index=True)   # "inflow" | "outflow"
    category: Mapped[str] = mapped_column(String(64), default="uncategorized", index=True)
    subcategory: Mapped[str] = mapped_column(String(64), default="")

    # Identity
    name: Mapped[str] = mapped_column(String(255), default="")
    description: Mapped[str] = mapped_column(Text, default="")
    vendor_or_customer: Mapped[str] = mapped_column(String(255), default="")

    # Amount — stored as integer cents; use Decimal for all arithmetic
    amount_cents: Mapped[int] = mapped_column(Integer, default=0)

    # Dates
    due_date: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    effective_date: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    expected_date: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    # Lifecycle
    status: Mapped[str] = mapped_column(String(32), default="planned", index=True)
    confidence: Mapped[str] = mapped_column(String(16), default="estimated")  # "confirmed" | "estimated"

    # Native Anata commitment workflow.  These fields deliberately live on the
    # canonical obligation instead of in a second task system so Finance has one
    # operational source of truth.  Transaction rows retain inert defaults.
    commitment_type: Mapped[str] = mapped_column(
        String(32), default="general", server_default="general", index=True
    )
    workflow_status: Mapped[str] = mapped_column(
        String(32), default="draft", server_default="draft", index=True
    )
    owner: Mapped[str] = mapped_column(String(255), default="", server_default="", index=True)
    approval_status: Mapped[str] = mapped_column(
        String(32), default="not_required", server_default="not_required", index=True
    )
    created_by: Mapped[str] = mapped_column(String(255), default="system", server_default="system")
    archived_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True, index=True)

    # Provider-reported lifecycle is evidence, not canonical settlement truth.
    # ``status`` remains derived from allocations / explicit local decisions.
    source_status: Mapped[str] = mapped_column(
        String(32), default="", server_default="", index=True
    )
    source_open_amount_cents: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    source_updated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Obligation controls. Transaction rows retain these compatible defaults.
    pay_priority: Mapped[str] = mapped_column(
        String(16), default="review", server_default="review", index=True
    )
    minimum_payment_cents: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    flexibility: Mapped[str] = mapped_column(
        String(16), default="unknown", server_default="unknown", index=True
    )

    # Recurring linkage
    recurring_template_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    recurring_rule: Mapped[str] = mapped_column(String(64), default="")  # "weekly"|"biweekly"|"monthly"|"custom"

    # CSV ↔ planned obligation matching
    matched_to_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    match_status: Mapped[str] = mapped_column(
        String(16), default="", server_default="", index=True
    )
    match_candidates_json: Mapped[list] = mapped_column(JSON, default=list)

    # ClickUp transition
    clickup_task_id: Mapped[str] = mapped_column(String(64), default="", index=True)

    # Bank snapshot (from CSV rows)
    account_balance_cents: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    bank_transaction_type: Mapped[str] = mapped_column(String(32), default="")  # Card|Retail ACH|Check|POS
    bank_reference: Mapped[str] = mapped_column(String(128), default="")

    # Notes
    notes: Mapped[str] = mapped_column(Text, default="")

    # Audit
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, index=True)

    # Composite index for the most common forecast query: date range + status
    __table_args__ = (
        Index("ix_cash_events_due_date_status", "due_date", "status"),
        Index("ix_cash_events_source_source_id", "source", "source_id"),
        Index("ix_cash_events_native_queue", "workflow_status", "archived_at", "due_date"),
    )


class PaymentInstallment(Base):
    """A durable payment slice planned against one obligation."""

    __tablename__ = "payment_installments"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    obligation_event_id: Mapped[str] = mapped_column(ForeignKey("cash_events.id"), index=True)
    amount_cents: Mapped[int] = mapped_column(Integer)
    due_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    status: Mapped[str] = mapped_column(String(16), default="planned", index=True)
    idempotency_key: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class SettlementAllocation(Base):
    """Append-only evidence assigning actual cash movement to an obligation."""

    __tablename__ = "settlement_allocations"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    obligation_event_id: Mapped[str] = mapped_column(ForeignKey("cash_events.id"), index=True)
    transaction_event_id: Mapped[Optional[str]] = mapped_column(ForeignKey("cash_events.id"), nullable=True, index=True)
    installment_id: Mapped[Optional[str]] = mapped_column(ForeignKey("payment_installments.id"), nullable=True, index=True)
    amount_cents: Mapped[int] = mapped_column(Integer)
    allocation_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    source: Mapped[str] = mapped_column(String(32), default="manual")
    confidence: Mapped[str] = mapped_column(String(16), default="confirmed")
    idempotency_key: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    reversed_allocation_id: Mapped[Optional[str]] = mapped_column(ForeignKey("settlement_allocations.id"), nullable=True, unique=True)
    notes: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class FinanceSourceRecord(Base):
    """Stable source identity and hashes for a canonical cash event."""

    __tablename__ = "finance_source_records"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    cash_event_id: Mapped[str] = mapped_column(ForeignKey("cash_events.id"), index=True)
    source_system: Mapped[str] = mapped_column(String(64))
    scope_key: Mapped[str] = mapped_column(String(255), default="")
    entity_type: Mapped[str] = mapped_column(String(64))
    external_id: Mapped[str] = mapped_column(String(255))
    payload_hash: Mapped[str] = mapped_column(String(128), default="")
    soft_fingerprint: Mapped[str] = mapped_column(String(255), default="", index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    __table_args__ = (
        Index(
            "uq_finance_source_identity",
            "source_system",
            "scope_key",
            "entity_type",
            "external_id",
            unique=True,
        ),
    )


class FinanceImportBatch(Base):
    """One atomic finance import staged before canonical records are written."""

    __tablename__ = "finance_import_batches"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    source_type: Mapped[str] = mapped_column(String(64), index=True)
    file_hash: Mapped[str] = mapped_column(String(128), index=True)
    status: Mapped[str] = mapped_column(String(16), default="staged", index=True)
    ready_count: Mapped[int] = mapped_column(Integer, default=0)
    duplicate_count: Mapped[int] = mapped_column(Integer, default=0)
    review_count: Mapped[int] = mapped_column(Integer, default=0)
    invalid_count: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    posted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)


class FinanceImportRow(Base):
    """Staged source row and its deterministic import classification."""

    __tablename__ = "finance_import_rows"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    import_batch_id: Mapped[str] = mapped_column(ForeignKey("finance_import_batches.id"), index=True)
    row_number: Mapped[int] = mapped_column(Integer)
    raw_payload: Mapped[dict] = mapped_column(JSON, default=dict)
    normalized_payload: Mapped[dict] = mapped_column(JSON, default=dict)
    classification: Mapped[str] = mapped_column(String(16), index=True)
    reason: Mapped[str] = mapped_column(Text, default="")

    __table_args__ = (
        Index("uq_finance_import_batch_row", "import_batch_id", "row_number", unique=True),
    )


class FinanceSetting(Base):
    """Persisted operator controls shared by every Finance read model."""

    __tablename__ = "finance_settings"

    scope_key: Mapped[str] = mapped_column(String(255), primary_key=True)
    cash_floor_cents: Mapped[int] = mapped_column(Integer, default=1_000_000)
    active_actual_source: Mapped[str] = mapped_column(String(32), default="csv")
    updated_by: Mapped[str] = mapped_column(String(255), default="system")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class PlaidItem(Base):
    """One consented Plaid institution connection; access tokens are sealed."""

    __tablename__ = "plaid_items"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    scope_key: Mapped[str] = mapped_column(String(255), default="default", index=True)
    external_item_id: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    institution_id: Mapped[str] = mapped_column(String(255), default="", index=True)
    display_name: Mapped[str] = mapped_column(String(255), default="")
    sealed_access_token: Mapped[str] = mapped_column(Text, default="")
    status: Mapped[str] = mapped_column(String(32), default="connected", index=True)
    consent_expiration: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_success_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_webhook_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error_code: Mapped[str] = mapped_column(String(128), default="")
    transactions_cursor: Mapped[str] = mapped_column(Text, default="")
    created_by: Mapped[str] = mapped_column(String(255), default="system")
    disconnected_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class PlaidAccount(Base):
    """A masked bank account belonging to a Plaid Item."""

    __tablename__ = "plaid_accounts"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    plaid_item_id: Mapped[str] = mapped_column(ForeignKey("plaid_items.id"), index=True)
    external_account_id: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(255), default="")
    official_name: Mapped[str] = mapped_column(String(255), default="")
    mask: Mapped[str] = mapped_column(String(8), default="")
    account_type: Mapped[str] = mapped_column(String(32), default="")
    subtype: Mapped[str] = mapped_column(String(64), default="")
    currency: Mapped[str] = mapped_column(String(16), default="USD")
    current_balance_cents: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    available_balance_cents: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    balance_as_of: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class FinanceActionAudit(Base):
    """Append-only evidence for trust-sensitive Finance mutations."""

    __tablename__ = "finance_action_audit"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    scope_key: Mapped[str] = mapped_column(String(255), default="default", index=True)
    action_type: Mapped[str] = mapped_column(String(64), index=True)
    entity_type: Mapped[str] = mapped_column(String(64), default="")
    entity_id: Mapped[str] = mapped_column(String(255), default="", index=True)
    actor: Mapped[str] = mapped_column(String(255), default="system")
    idempotency_key: Mapped[Optional[str]] = mapped_column(String(128), nullable=True, unique=True, index=True)
    evidence_json: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class FinanceReconciliationReport(Base):
    """Immutable shadow-reconciliation result used before forecast promotion."""

    __tablename__ = "finance_reconciliation_reports"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    scope_key: Mapped[str] = mapped_column(String(255), default="default", index=True)
    as_of_date: Mapped[date] = mapped_column(Date, index=True)
    input_hash: Mapped[str] = mapped_column(String(128), index=True)
    status: Mapped[str] = mapped_column(String(16), default="complete", index=True)
    summary_json: Mapped[dict] = mapped_column(JSON, default=dict)
    report_json: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    __table_args__ = (
        Index(
            "uq_finance_reconciliation_report_input",
            "scope_key", "as_of_date", "input_hash", unique=True,
        ),
        Index("ix_finance_reconciliation_report_latest", "scope_key", "created_at"),
    )


class FinanceSavingsReview(Base):
    """Current operator state for one deterministic savings opportunity."""

    __tablename__ = "finance_savings_reviews"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    scope_key: Mapped[str] = mapped_column(String(255), default="default", index=True)
    opportunity_key: Mapped[str] = mapped_column(String(64), index=True)
    evidence_hash: Mapped[str] = mapped_column(String(64), default="", index=True)
    state: Mapped[str] = mapped_column(String(32), default="reviewing", index=True)
    display_name: Mapped[str] = mapped_column(String(255), default="")
    normalized_merchant: Mapped[str] = mapped_column(String(255), default="", index=True)
    cadence: Mapped[str] = mapped_column(String(32), default="")
    potential_monthly_cents: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    baseline_amount_cents: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    suppress_until: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    clickup_task_id: Mapped[str] = mapped_column(String(64), default="")
    clickup_task_url: Mapped[str] = mapped_column(String(1024), default="")
    reason: Mapped[str] = mapped_column(Text, default="")
    evidence_json: Mapped[dict] = mapped_column(JSON, default=dict)
    created_by: Mapped[str] = mapped_column(String(255), default="system")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    __table_args__ = (
        Index("uq_finance_savings_review_scope_opportunity", "scope_key", "opportunity_key", unique=True),
    )


class FinanceSavingsReviewEvent(Base):
    """Append-only audit log for savings review decisions and verification."""

    __tablename__ = "finance_savings_review_events"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    review_id: Mapped[str] = mapped_column(ForeignKey("finance_savings_reviews.id"), index=True)
    scope_key: Mapped[str] = mapped_column(String(255), default="default", index=True)
    event_type: Mapped[str] = mapped_column(String(64), index=True)
    prior_state: Mapped[str] = mapped_column(String(32), default="")
    next_state: Mapped[str] = mapped_column(String(32), default="")
    actor: Mapped[str] = mapped_column(String(255), default="system")
    idempotency_key: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    payload_json: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class RecurringTemplate(Base):
    """Defines a repeating financial obligation.  Each template generates new
    CashEvent rows (source="recurring") one period ahead on each app boot or
    manual refresh, keeping the forecast window populated.

    frequency values:
        "weekly" | "biweekly" | "monthly" | "quarterly" | "annual"
    """

    __tablename__ = "recurring_templates"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)

    # What
    name: Mapped[str] = mapped_column(String(255), default="")
    vendor_or_customer: Mapped[str] = mapped_column(String(255), default="")
    event_type: Mapped[str] = mapped_column(String(16), default="outflow")   # "inflow" | "outflow"
    category: Mapped[str] = mapped_column(String(64), default="uncategorized")
    amount_cents: Mapped[int] = mapped_column(Integer, default=0)
    confidence: Mapped[str] = mapped_column(String(16), default="estimated")
    notes: Mapped[str] = mapped_column(Text, default="")

    # When
    frequency: Mapped[str] = mapped_column(String(32), default="monthly", index=True)
    next_due_date: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    day_of_month: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)  # for monthly rules

    # Lifecycle
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    clickup_task_id: Mapped[str] = mapped_column(String(64), default="")

    # Audit
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


# ===========================================================================
# Advertising > Audit  (agent.anatainc.com — weekly Amazon ad/sales audit)
#
# Conventions mirror CashEvent: money is stored as integer cents, percentages
# as integer basis points (25.0% -> 2500 bps) so all arithmetic stays exact.
# Relationships to audit_runs are soft (indexed string columns, no FK
# constraints) so a run can be deleted without orphan-cleanup blocking — same
# rationale as the cashflow tables.
# ===========================================================================


class AdClient(Base):
    """An advertising client the audit is run for. Each client owns its own
    objectives + active goal set (AdGoal.client_id) and a history of runs
    (AuditRun.client_id), so the same engine optimizes toward per-client targets
    instead of one global set. Brand stays a free-text per-run scope — a client
    does NOT own a managed brand list.
    """

    __tablename__ = "ad_clients"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), default="", index=True)
    objectives: Mapped[str] = mapped_column(Text, default="")  # free-text strategy notes
    status: Mapped[str] = mapped_column(String(16), default="active", index=True)  # active|archived

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class AdGoal(Base):
    """A standing set of advertising/sales targets the audit scales toward.

    Snapshotted into each AuditRun.goal_snapshot_json at run time so historical
    runs reflect the goal that was active then, even after the goal is edited.
    Percentages are basis points; money is integer cents. Scoped to a client via
    client_id (NULL = the global/ad-hoc set used when no client is selected).
    """

    __tablename__ = "ad_goals"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    client_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    label: Mapped[str] = mapped_column(String(255), default="")
    period: Mapped[str] = mapped_column(String(32), default="monthly")  # weekly|monthly|quarterly

    revenue_target_cents: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    acos_target_bps: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)   # ad ACoS %
    tacos_target_bps: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)  # total ACoS %
    units_target: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    is_active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    notes: Mapped[str] = mapped_column(Text, default="")

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class ExternalCost(Base):
    """Off-Amazon marketing spend that belongs in the blended-TACoS denominator:
    Meta / TikTok ad spend, influencer commissions, agency fees, etc. Entered
    manually or via CSV. Tied to a run (run_id) or left standing (run_id NULL).
    """

    __tablename__ = "external_costs"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    run_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    channel: Mapped[str] = mapped_column(String(32), default="other", index=True)  # meta|tiktok|influencer|google|other
    cost_type: Mapped[str] = mapped_column(String(32), default="ad_spend")          # ad_spend|commission|fee
    label: Mapped[str] = mapped_column(String(255), default="")
    amount_cents: Mapped[int] = mapped_column(Integer, default=0)
    period_start: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    period_end: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    note: Mapped[str] = mapped_column(Text, default="")

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class AuditRun(Base):
    """One weekly advertising audit. Holds the goal snapshot, the computed
    summary metrics, and the LLM narrative. ad/sales/market snapshots and
    recommendations reference it by run_id.
    """

    __tablename__ = "audit_runs"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    client_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    label: Mapped[str] = mapped_column(String(255), default="")
    week_start: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    week_end: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(String(32), default="draft", index=True)  # draft|ingested|analyzed|complete|error

    goal_snapshot_json: Mapped[dict] = mapped_column(JSON, default=dict)
    summary_json: Mapped[dict] = mapped_column(JSON, default=dict)
    narrative: Mapped[str] = mapped_column(Text, default="")
    error: Mapped[str] = mapped_column(Text, default="")

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class AdSnapshot(Base):
    """A normalized row from an Amazon Ads report, at whatever entity level the
    report carries (campaign / ad group / keyword / target / search term /
    placement / product ad). One table spans all ad types via ad_type.
    """

    __tablename__ = "ad_snapshots"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(64), index=True)
    ad_type: Mapped[str] = mapped_column(String(16), index=True)       # SP|SB|SD|STV|DSP
    entity_level: Mapped[str] = mapped_column(String(32), index=True)  # campaign|ad_group|keyword|target|search_term|placement|product_ad

    campaign_name: Mapped[str] = mapped_column(String(512), default="")
    ad_group_name: Mapped[str] = mapped_column(String(512), default="")
    entity_text: Mapped[str] = mapped_column(String(1024), default="")  # keyword / search term / target expr / asin
    match_type: Mapped[str] = mapped_column(String(32), default="")

    impressions: Mapped[int] = mapped_column(Integer, default=0)
    clicks: Mapped[int] = mapped_column(Integer, default=0)
    spend_cents: Mapped[int] = mapped_column(Integer, default=0)
    sales_cents: Mapped[int] = mapped_column(Integer, default=0)
    orders: Mapped[int] = mapped_column(Integer, default=0)
    units: Mapped[int] = mapped_column(Integer, default=0)
    bid_cents: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    raw_json: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    __table_args__ = (
        Index("ix_ad_snapshots_run_type_level", "run_id", "ad_type", "entity_level"),
    )


class SalesSnapshot(Base):
    """A normalized row from the Amazon Business Report (Detail Page Sales &
    Traffic) — one row per child ASIN/SKU. Powers TACoS, sessions and CVR.
    """

    __tablename__ = "sales_snapshots"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(64), index=True)
    asin: Mapped[str] = mapped_column(String(32), default="", index=True)
    sku: Mapped[str] = mapped_column(String(64), default="")
    title: Mapped[str] = mapped_column(String(512), default="")

    sessions: Mapped[int] = mapped_column(Integer, default=0)
    page_views: Mapped[int] = mapped_column(Integer, default=0)
    units: Mapped[int] = mapped_column(Integer, default=0)
    ordered_product_sales_cents: Mapped[int] = mapped_column(Integer, default=0)
    buy_box_pct_bps: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    conversion_bps: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    raw_json: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class MarketSnapshot(Base):
    """A normalized row from Brand Analytics Search Query Performance — share of
    impressions/clicks/purchases for a search query. Gives market-share context.
    """

    __tablename__ = "market_snapshots"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(64), index=True)
    search_query: Mapped[str] = mapped_column(String(512), default="", index=True)
    asin: Mapped[str] = mapped_column(String(32), default="")

    search_query_volume: Mapped[int] = mapped_column(Integer, default=0)
    impressions_total: Mapped[int] = mapped_column(Integer, default=0)
    impression_share_bps: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    clicks_total: Mapped[int] = mapped_column(Integer, default=0)
    click_share_bps: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    purchases_total: Mapped[int] = mapped_column(Integer, default=0)
    purchase_share_bps: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    raw_json: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class Recommendation(Base):
    """One prioritized item in the audit's burn list. When is_bulk_actionable is
    true, bulk_row_json carries the change(s) to write into the round-tripped
    Amazon bulk sheet; otherwise it is a manual task in the burn list.
    """

    __tablename__ = "recommendations"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(64), index=True)
    rank: Mapped[int] = mapped_column(Integer, default=0, index=True)
    category: Mapped[str] = mapped_column(String(48), default="", index=True)  # bid_down|bid_up|negative_keyword|new_keyword|budget|structure|placement|dayparting|external|manual
    ad_type: Mapped[str] = mapped_column(String(16), default="")
    severity: Mapped[str] = mapped_column(String(16), default="medium")  # high|medium|low

    title: Mapped[str] = mapped_column(String(512), default="")
    detail: Mapped[str] = mapped_column(Text, default="")
    rationale: Mapped[str] = mapped_column(Text, default="")
    entity_ref: Mapped[str] = mapped_column(String(1024), default="")
    current_value: Mapped[str] = mapped_column(String(128), default="")
    proposed_value: Mapped[str] = mapped_column(String(128), default="")

    projected_impact_json: Mapped[dict] = mapped_column(JSON, default=dict)
    bulk_row_json: Mapped[dict] = mapped_column(JSON, default=dict)
    is_bulk_actionable: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    status: Mapped[str] = mapped_column(String(16), default="open", index=True)  # open|applied|dismissed

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    __table_args__ = (
        Index("ix_recommendations_run_rank", "run_id", "rank"),
    )


class BrandAnalysisReport(Base):
    """One Executive > Brand Analysis run. Holds the slim list fields (brand,
    grade, score, confidence) for History plus the full computed report as a
    JSON blob. Uploaded source files and the generated .docx live in kv_store
    (base64) so re-open + re-download survive Render's ephemeral disk.
    """

    __tablename__ = "brand_analysis_reports"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    label: Mapped[str] = mapped_column(String(255), default="")
    brand: Mapped[str] = mapped_column(String(255), default="", index=True)
    category: Mapped[str] = mapped_column(String(32), default="dtc")
    status: Mapped[str] = mapped_column(String(32), default="complete", index=True)  # complete|error

    grade: Mapped[str] = mapped_column(String(2), default="")          # A–F
    score_100: Mapped[int] = mapped_column(Integer, default=0)
    confidence: Mapped[str] = mapped_column(String(16), default="")    # High|Medium|Low
    period_current: Mapped[str] = mapped_column(String(64), default="")
    period_prior: Mapped[str] = mapped_column(String(64), default="")

    report_json: Mapped[dict] = mapped_column(JSON, default=dict)      # full BrandReport.to_dict()
    error: Mapped[str] = mapped_column(Text, default="")

    # Shareable hosted landing page: token-gated public URL + pre-rendered HTML
    # (same pattern as the sales/fulfillment decks). slug is the brand slug.
    slug: Mapped[str] = mapped_column(String(96), default="", index=True)
    share_token: Mapped[str] = mapped_column(String(64), default="", index=True)
    report_html: Mapped[str] = mapped_column(Text, default="")         # standalone branded LP
    # Inputs persisted for edit + rerun (overwrite-in-place).
    brand_website: Mapped[str] = mapped_column(String(512), default="")
    context_notes: Mapped[str] = mapped_column(Text, default="")
    stage: Mapped[str] = mapped_column(String(32), default="new", index=True)  # acquisition funnel stage
    notes: Mapped[str] = mapped_column(Text, default="")                       # analyst deal notes
    ask_price_cents: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)  # proposed ask price
    contact_name: Mapped[str] = mapped_column(String(255), default="")        # seller / broker contact
    contact_email: Mapped[str] = mapped_column(String(255), default="")

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


# ---------------------------------------------------------------------------
# Access control (RBAC) — multi-user, custom roles, per-tool permissions.
# ---------------------------------------------------------------------------


class AppRole(Base):
    """A custom role: a named set of per-tool permissions (catalog keys)."""

    __tablename__ = "app_roles"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    name: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    description: Mapped[str] = mapped_column(Text, default="")
    permissions_json: Mapped[list] = mapped_column(JSON, default=list)  # list[str] of tool keys

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class AppUser(Base):
    """A person who can sign in. Authorization is resolved from role_id ->
    AppRole.permissions_json on every request (the cookie is identity only).
    Super-admins bypass all per-tool checks and can never be locked out.
    """

    __tablename__ = "app_users"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    email: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(255), default="")
    picture_url: Mapped[str] = mapped_column(Text, default="")
    role_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)  # legacy; per-user permissions below supersede it
    permissions_json: Mapped[list] = mapped_column(JSON, default=list)  # list[str] of tool keys granted directly to this user
    status: Mapped[str] = mapped_column(String(16), default="active", index=True)  # active|suspended
    is_superadmin: Mapped[bool] = mapped_column(Boolean, default=False)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    last_login_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)


class InboxConnection(Base):
    """Persisted per-user inbox connection metadata for self-serve Gmail sync."""

    __tablename__ = "inbox_connections"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    provider: Mapped[str] = mapped_column(String(32), default="gmail", index=True)
    connection_source: Mapped[str] = mapped_column(String(32), default="user_oauth", index=True)
    account_key: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    account_label: Mapped[str] = mapped_column(String(255), default="")
    account_email: Mapped[str] = mapped_column(String(255), default="", index=True)
    owner_user_id: Mapped[str] = mapped_column(String(64), default="", index=True)
    owner_user_email: Mapped[str] = mapped_column(String(255), default="", index=True)
    owner_user_name: Mapped[str] = mapped_column(String(255), default="")
    gmail_user_id: Mapped[str] = mapped_column(String(64), default="me")
    sealed_access_token: Mapped[str] = mapped_column(Text, default="")
    sealed_refresh_token: Mapped[str] = mapped_column(Text, default="")
    poll_query: Mapped[str] = mapped_column(String(255), default="newer_than:2d")
    poll_max_messages: Mapped[int] = mapped_column(Integer, default=25)
    source_domains_json: Mapped[list] = mapped_column(JSON, default=list)
    status: Mapped[str] = mapped_column(String(32), default="connected", index=True)
    last_error: Mapped[str] = mapped_column(Text, default="")
    last_validated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_sync_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    disconnected_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class AppInvite(Base):
    """A direct invite to a specific email, granting a role on acceptance.
    The bearer token is stored hashed; the raw token lives only in the link."""

    __tablename__ = "app_invites"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    email: Mapped[str] = mapped_column(String(255), index=True)
    role_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    token_hash: Mapped[str] = mapped_column(String(128), index=True)
    invited_by: Mapped[str] = mapped_column(String(255), default="")
    status: Mapped[str] = mapped_column(String(16), default="pending", index=True)  # pending|accepted|revoked|expired

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    accepted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)


class AppAccessRequest(Base):
    """A self-service access request raised when an un-provisioned (but
    domain-allowed) user signs in. An admin approves with a role, or denies."""

    __tablename__ = "app_access_requests"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    email: Mapped[str] = mapped_column(String(255), index=True)
    name: Mapped[str] = mapped_column(String(255), default="")
    status: Mapped[str] = mapped_column(String(16), default="pending", index=True)  # pending|approved|denied
    assigned_role_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    decided_by: Mapped[str] = mapped_column(String(255), default="")

    requested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, index=True)
    decided_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)


# ---------------------------------------------------------------------------
# HubSpot sales mirror tables (read mirror first; HubSpot stays canonical).
# Money is stored in integer cents. These replace the ClickUp-shaped
# LeadMirror as the sales source of truth. Sync cursors live in kv_store.
# ---------------------------------------------------------------------------
class HubSpotCompany(Base):
    __tablename__ = "hubspot_companies"

    hubspot_company_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    name: Mapped[str] = mapped_column(String(512), default="", index=True)
    domain: Mapped[str] = mapped_column(String(255), default="", index=True)
    industry: Mapped[str] = mapped_column(String(255), default="")
    city: Mapped[str] = mapped_column(String(128), default="")
    state: Mapped[str] = mapped_column(String(128), default="")
    raw_properties: Mapped[dict] = mapped_column(JSON, default=dict)
    last_sync_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class HubSpotContact(Base):
    __tablename__ = "hubspot_contacts"

    hubspot_contact_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    hubspot_company_id: Mapped[str] = mapped_column(String(64), default="", index=True)
    first_name: Mapped[str] = mapped_column(String(255), default="")
    last_name: Mapped[str] = mapped_column(String(255), default="")
    email: Mapped[str] = mapped_column(String(255), default="", index=True)
    phone: Mapped[str] = mapped_column(String(128), default="")
    job_title: Mapped[str] = mapped_column(String(255), default="")
    raw_properties: Mapped[dict] = mapped_column(JSON, default=dict)
    last_sync_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class HubSpotDeal(Base):
    __tablename__ = "hubspot_deals"

    hubspot_deal_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    deal_name: Mapped[str] = mapped_column(String(512), default="", index=True)
    amount_cents: Mapped[int] = mapped_column(Integer, default=0)
    deal_stage: Mapped[str] = mapped_column(String(128), default="", index=True)
    # Human-readable stage label resolved from the HubSpot pipeline (raw
    # deal_stage is an internal id like "qualifiedtobuy").
    deal_stage_label: Mapped[str] = mapped_column(String(255), default="")
    pipeline: Mapped[str] = mapped_column(String(128), default="", index=True)
    close_date: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    owner_id: Mapped[str] = mapped_column(String(64), default="", index=True)
    owner_email: Mapped[str] = mapped_column(String(255), default="")
    hubspot_company_id: Mapped[str] = mapped_column(String(64), default="", index=True)
    is_closed: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    is_won: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    description: Mapped[str] = mapped_column(Text, default="")
    # Accountability/managed fields mirrored from LeadMirror so the staleness
    # engine can repoint onto HubSpot deals without losing its inputs.
    last_meaningful_touch_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_outbound_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_inbound_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    next_follow_up_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    follow_up_state: Mapped[str] = mapped_column(String(64), default="")
    communication_summary: Mapped[str] = mapped_column(Text, default="")
    recommended_next_action: Mapped[str] = mapped_column(Text, default="")
    raw_properties: Mapped[dict] = mapped_column(JSON, default=dict)
    last_sync_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class HubSpotLineItem(Base):
    __tablename__ = "hubspot_line_items"

    hubspot_line_item_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    hubspot_deal_id: Mapped[str] = mapped_column(String(64), default="", index=True)
    name: Mapped[str] = mapped_column(String(512), default="")
    quantity: Mapped[int] = mapped_column(Integer, default=0)
    unit_price_cents: Mapped[int] = mapped_column(Integer, default=0)
    amount_cents: Mapped[int] = mapped_column(Integer, default=0)
    raw_properties: Mapped[dict] = mapped_column(JSON, default=dict)
    last_sync_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class HubSpotDealContact(Base):
    """Deal <-> contact link mirror (a deal can have many contacts)."""

    __tablename__ = "hubspot_deal_contacts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    hubspot_deal_id: Mapped[str] = mapped_column(String(64), index=True)
    hubspot_contact_id: Mapped[str] = mapped_column(String(64), index=True)
    role: Mapped[str] = mapped_column(String(64), default="")

    __table_args__ = (
        Index("ix_hs_deal_contact_unique", "hubspot_deal_id", "hubspot_contact_id", unique=True),
    )


class HubSpotDealNote(Base):
    """Recent HubSpot notes mirrored locally for operator reasoning."""

    __tablename__ = "hubspot_deal_notes"

    hubspot_note_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    hubspot_deal_id: Mapped[str] = mapped_column(String(64), index=True)
    owner_id: Mapped[str] = mapped_column(String(64), default="", index=True)
    body_text: Mapped[str] = mapped_column(Text, default="")
    body_preview: Mapped[str] = mapped_column(String(512), default="")
    override_state: Mapped[str] = mapped_column(String(64), default="", index=True)
    override_reason: Mapped[str] = mapped_column(String(255), default="")
    note_timestamp: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    raw_properties: Mapped[dict] = mapped_column(JSON, default=dict)
    last_sync_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class SalesDealAsset(Base):
    """Links a HubSpot deal to a closing-tool run (deck / rate sheet / ads
    audit) so the deal board/detail can render the three CTAs as deep links."""

    __tablename__ = "sales_deal_assets"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    hubspot_deal_id: Mapped[str] = mapped_column(String(64), index=True)
    asset_type: Mapped[str] = mapped_column(String(32), index=True)  # deck|rate_sheet|ads_audit
    run_id: Mapped[str] = mapped_column(String(64), default="")
    url: Mapped[str] = mapped_column(String(1024), default="")
    label: Mapped[str] = mapped_column(String(255), default="")
    linked_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    __table_args__ = (
        Index("ix_sales_deal_asset_unique", "hubspot_deal_id", "asset_type", "run_id", unique=True),
    )


# ---------------------------------------------------------------------------
# Anata Building operations. Agent owns inventory and commercial availability;
# the public website receives only the explicitly published projection.
# ---------------------------------------------------------------------------
class BuildingSpace(Base):
    __tablename__ = "building_spaces"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    slug: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(255))
    space_type: Mapped[str] = mapped_column(String(64), index=True)
    floor: Mapped[str] = mapped_column(String(64), default="")
    capacity: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String(32), default="unavailable", index=True)
    public_description: Mapped[str] = mapped_column(Text, default="")
    internal_notes: Mapped[str] = mapped_column(Text, default="")
    features_json: Mapped[list] = mapped_column(JSON, default=list)
    media_json: Mapped[list] = mapped_column(JSON, default=list)
    is_public: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class BuildingOffering(Base):
    __tablename__ = "building_offerings"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    slug: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(255))
    offering_type: Mapped[str] = mapped_column(String(64), index=True)
    space_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("building_spaces.id"), nullable=True, index=True
    )
    public_description: Mapped[str] = mapped_column(Text, default="")
    price_display: Mapped[str] = mapped_column(String(128), default="")
    booking_unit: Mapped[str] = mapped_column(String(32), default="custom")
    call_to_action: Mapped[str] = mapped_column(String(64), default="inquire")
    features_json: Mapped[list] = mapped_column(JSON, default=list)
    is_published: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class BuildingAvailabilityBlock(Base):
    __tablename__ = "building_availability_blocks"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    space_id: Mapped[str] = mapped_column(ForeignKey("building_spaces.id"), index=True)
    state: Mapped[str] = mapped_column(String(32), index=True)
    starts_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    ends_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    source: Mapped[str] = mapped_column(String(64), default="agent")
    source_reference: Mapped[str] = mapped_column(String(255), default="", index=True)
    public_label: Mapped[str] = mapped_column(String(128), default="")
    notes: Mapped[str] = mapped_column(Text, default="")
    created_by: Mapped[str] = mapped_column(String(255), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class BuildingInquiry(Base):
    __tablename__ = "building_inquiries"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    idempotency_key: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    kind: Mapped[str] = mapped_column(String(32), index=True)
    source: Mapped[str] = mapped_column(String(64), default="anata-building", index=True)
    source_reference: Mapped[str] = mapped_column(String(255), default="")
    offering_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("building_offerings.id"), nullable=True, index=True
    )
    name: Mapped[str] = mapped_column(String(255))
    email: Mapped[str] = mapped_column(String(255), index=True)
    phone: Mapped[str] = mapped_column(String(128), default="")
    preferred_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True, index=True)
    status: Mapped[str] = mapped_column(String(32), default="new", index=True)
    assigned_owner: Mapped[str] = mapped_column(String(255), default="")
    consent_to_contact: Mapped[bool] = mapped_column(Boolean, default=False)
    consent_to_marketing: Mapped[bool] = mapped_column(Boolean, default=False)
    hubspot_contact_id: Mapped[str] = mapped_column(String(64), default="")
    payload_json: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class BuildingAuditEvent(Base):
    __tablename__ = "building_audit_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    entity_type: Mapped[str] = mapped_column(String(64), index=True)
    entity_id: Mapped[str] = mapped_column(String(64), index=True)
    action: Mapped[str] = mapped_column(String(64), index=True)
    actor: Mapped[str] = mapped_column(String(255), default="")
    before_json: Mapped[dict] = mapped_column(JSON, default=dict)
    after_json: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, index=True)


class BuildingContact(Base):
    __tablename__ = "building_contacts"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    email: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    full_name: Mapped[str] = mapped_column(String(255), default="")
    phone: Mapped[str] = mapped_column(String(128), default="")
    company_name: Mapped[str] = mapped_column(String(255), default="")
    hubspot_contact_id: Mapped[str] = mapped_column(String(64), default="", index=True)
    source: Mapped[str] = mapped_column(String(64), default="manual", index=True)
    status: Mapped[str] = mapped_column(String(32), default="active", index=True)
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class BuildingRelationship(Base):
    __tablename__ = "building_relationships"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    contact_id: Mapped[str] = mapped_column(ForeignKey("building_contacts.id"), index=True)
    relationship_type: Mapped[str] = mapped_column(String(64), index=True)
    status: Mapped[str] = mapped_column(String(32), default="active", index=True)
    organization: Mapped[str] = mapped_column(String(255), default="")
    starts_on: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    ends_on: Mapped[Optional[date]] = mapped_column(Date, nullable=True, index=True)
    source_reference: Mapped[str] = mapped_column(String(255), default="")
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    __table_args__ = (
        Index(
            "ix_building_relationship_unique",
            "contact_id",
            "relationship_type",
            "source_reference",
            unique=True,
        ),
    )


class BuildingCommunicationPreference(Base):
    __tablename__ = "building_communication_preferences"

    contact_id: Mapped[str] = mapped_column(ForeignKey("building_contacts.id"), primary_key=True)
    marketing_status: Mapped[str] = mapped_column(String(32), default="unknown", index=True)
    marketing_source: Mapped[str] = mapped_column(String(64), default="")
    marketing_changed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    transactional_allowed: Mapped[bool] = mapped_column(Boolean, default=True)
    updated_by: Mapped[str] = mapped_column(String(255), default="")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class BuildingSuppression(Base):
    __tablename__ = "building_suppressions"

    email: Mapped[str] = mapped_column(String(255), primary_key=True)
    scope: Mapped[str] = mapped_column(String(32), default="marketing", index=True)
    reason: Mapped[str] = mapped_column(String(64), default="unsubscribe", index=True)
    source: Mapped[str] = mapped_column(String(64), default="agent")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class BuildingSegment(Base):
    __tablename__ = "building_segments"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), unique=True)
    description: Mapped[str] = mapped_column(Text, default="")
    rules_json: Mapped[dict] = mapped_column(JSON, default=dict)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    created_by: Mapped[str] = mapped_column(String(255), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class BuildingCampaign(Base):
    __tablename__ = "building_campaigns"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    name: Mapped[str] = mapped_column(String(255))
    segment_id: Mapped[str] = mapped_column(ForeignKey("building_segments.id"), index=True)
    communication_class: Mapped[str] = mapped_column(String(32), default="marketing", index=True)
    subject: Mapped[str] = mapped_column(String(255))
    body_text: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(32), default="draft", index=True)
    preview_hash: Mapped[str] = mapped_column(String(128), default="")
    previewed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    test_sent_by: Mapped[str] = mapped_column(String(255), default="")
    test_sent_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    approved_by: Mapped[str] = mapped_column(String(255), default="")
    approved_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    sent_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_by: Mapped[str] = mapped_column(String(255), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class BuildingCampaignRecipient(Base):
    __tablename__ = "building_campaign_recipients"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    campaign_id: Mapped[str] = mapped_column(ForeignKey("building_campaigns.id"), index=True)
    contact_id: Mapped[str] = mapped_column(ForeignKey("building_contacts.id"), index=True)
    email: Mapped[str] = mapped_column(String(255), index=True)
    full_name: Mapped[str] = mapped_column(String(255), default="")
    inclusion_reason: Mapped[str] = mapped_column(String(255), default="")
    status: Mapped[str] = mapped_column(String(32), default="approved", index=True)
    exclusion_reason: Mapped[str] = mapped_column(String(255), default="")
    provider_message_id: Mapped[str] = mapped_column(String(255), default="")
    sent_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    __table_args__ = (
        Index("ix_building_campaign_recipient_unique", "campaign_id", "contact_id", unique=True),
    )


class BuildingReservation(Base):
    __tablename__ = "building_reservations"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    kind: Mapped[str] = mapped_column(String(32), index=True)
    status: Mapped[str] = mapped_column(String(32), default="inquiry", index=True)
    inquiry_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("building_inquiries.id"), nullable=True, index=True
    )
    contact_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("building_contacts.id"), nullable=True, index=True
    )
    offering_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("building_offerings.id"), nullable=True, index=True
    )
    space_id: Mapped[str] = mapped_column(ForeignKey("building_spaces.id"), index=True)
    starts_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    ends_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    hold_expires_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True, index=True
    )
    attendance: Mapped[int] = mapped_column(Integer, default=0)
    agreement_status: Mapped[str] = mapped_column(String(32), default="not_started", index=True)
    deposit_status: Mapped[str] = mapped_column(String(32), default="not_started", index=True)
    deposit_required: Mapped[bool] = mapped_column(Boolean, default=True)
    assigned_owner: Mapped[str] = mapped_column(String(255), default="")
    requirements_json: Mapped[dict] = mapped_column(JSON, default=dict)
    source: Mapped[str] = mapped_column(String(64), default="agent")
    source_reference: Mapped[str] = mapped_column(String(255), default="")
    calendar_event_id: Mapped[str] = mapped_column(String(255), default="")
    created_by: Mapped[str] = mapped_column(String(255), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class BuildingCalendarProjection(Base):
    """Outbox state for projecting authoritative Agent reservations to a calendar."""

    __tablename__ = "building_calendar_projections"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    reservation_id: Mapped[str] = mapped_column(
        ForeignKey("building_reservations.id"), unique=True, index=True
    )
    provider: Mapped[str] = mapped_column(String(32), default="google_calendar")
    desired_action: Mapped[str] = mapped_column(String(16), default="upsert", index=True)
    status: Mapped[str] = mapped_column(String(32), default="pending", index=True)
    provider_event_id: Mapped[str] = mapped_column(String(255), default="")
    payload_json: Mapped[dict] = mapped_column(JSON, default=dict)
    attempt_count: Mapped[int] = mapped_column(Integer, default=0)
    last_error: Mapped[str] = mapped_column(Text, default="")
    last_attempt_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    synced_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class BuildingOperationalChecklist(Base):
    """Audited event or tenant-operation checklist linked to one reservation."""

    __tablename__ = "building_operational_checklists"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    reservation_id: Mapped[str] = mapped_column(
        ForeignKey("building_reservations.id"), index=True
    )
    checklist_type: Mapped[str] = mapped_column(String(32), index=True)
    title: Mapped[str] = mapped_column(String(255))
    status: Mapped[str] = mapped_column(String(32), default="open", index=True)
    assigned_owner: Mapped[str] = mapped_column(String(255), default="")
    due_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True, index=True
    )
    created_by: Mapped[str] = mapped_column(String(255), default="")
    completed_by: Mapped[str] = mapped_column(String(255), default="")
    completed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    __table_args__ = (
        Index(
            "ix_building_operational_checklist_unique",
            "reservation_id",
            "checklist_type",
            unique=True,
        ),
    )


class BuildingOperationalChecklistItem(Base):
    """One required, completed, or explicitly waived operational action."""

    __tablename__ = "building_operational_checklist_items"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    checklist_id: Mapped[str] = mapped_column(
        ForeignKey("building_operational_checklists.id"), index=True
    )
    label: Mapped[str] = mapped_column(String(512))
    status: Mapped[str] = mapped_column(String(32), default="pending", index=True)
    is_required: Mapped[bool] = mapped_column(Boolean, default=True)
    sort_order: Mapped[int] = mapped_column(Integer, default=0)
    completion_reason: Mapped[str] = mapped_column(Text, default="")
    completed_by: Mapped[str] = mapped_column(String(255), default="")
    completed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class BuildingAgreement(Base):
    __tablename__ = "building_agreements"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    reservation_id: Mapped[str] = mapped_column(
        ForeignKey("building_reservations.id"), index=True
    )
    version: Mapped[int] = mapped_column(Integer, default=1)
    status: Mapped[str] = mapped_column(String(32), default="draft", index=True)
    provider: Mapped[str] = mapped_column(String(64), default="")
    provider_reference: Mapped[str] = mapped_column(String(255), default="")
    template_name: Mapped[str] = mapped_column(String(255), default="")
    document_url: Mapped[str] = mapped_column(String(1024), default="")
    sent_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    signed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    voided_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    evidence_json: Mapped[dict] = mapped_column(JSON, default=dict)
    created_by: Mapped[str] = mapped_column(String(255), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    __table_args__ = (
        Index("ix_building_agreement_version", "reservation_id", "version", unique=True),
    )


class BuildingDepositEvidence(Base):
    __tablename__ = "building_deposit_evidence"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    reservation_id: Mapped[str] = mapped_column(
        ForeignKey("building_reservations.id"), index=True
    )
    status: Mapped[str] = mapped_column(String(32), default="due", index=True)
    amount_cents: Mapped[int] = mapped_column(Integer, default=0)
    provider: Mapped[str] = mapped_column(String(64), default="")
    provider_reference: Mapped[str] = mapped_column(String(255), default="", index=True)
    evidence_json: Mapped[dict] = mapped_column(JSON, default=dict)
    recorded_by: Mapped[str] = mapped_column(String(255), default="")
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class BuildingBillingAccount(Base):
    __tablename__ = "building_billing_accounts"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    contact_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("building_contacts.id"), nullable=True, index=True
    )
    account_name: Mapped[str] = mapped_column(String(255))
    billing_email: Mapped[str] = mapped_column(String(255), index=True)
    status: Mapped[str] = mapped_column(String(32), default="active", index=True)
    stripe_customer_id: Mapped[str] = mapped_column(String(64), default="", index=True)
    qbo_customer_id: Mapped[str] = mapped_column(String(64), default="", index=True)
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class BuildingBillingSchedule(Base):
    __tablename__ = "building_billing_schedules"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    billing_account_id: Mapped[str] = mapped_column(
        ForeignKey("building_billing_accounts.id"), index=True
    )
    reservation_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("building_reservations.id"), nullable=True, index=True
    )
    schedule_type: Mapped[str] = mapped_column(String(32), index=True)
    description: Mapped[str] = mapped_column(String(512))
    amount_cents: Mapped[int] = mapped_column(Integer)
    currency: Mapped[str] = mapped_column(String(8), default="usd")
    collection_method: Mapped[str] = mapped_column(String(32), default="send_invoice")
    days_until_due: Mapped[int] = mapped_column(Integer, default=7)
    status: Mapped[str] = mapped_column(String(32), default="draft", index=True)
    starts_on: Mapped[date] = mapped_column(Date)
    ends_on: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    next_invoice_on: Mapped[Optional[date]] = mapped_column(Date, nullable=True, index=True)
    last_invoice_on: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    created_by: Mapped[str] = mapped_column(String(255), default="")
    approved_by: Mapped[str] = mapped_column(String(255), default="")
    approved_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class BuildingInvoice(Base):
    __tablename__ = "building_invoices"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    billing_account_id: Mapped[str] = mapped_column(
        ForeignKey("building_billing_accounts.id"), index=True
    )
    billing_schedule_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("building_billing_schedules.id"), nullable=True, index=True
    )
    reservation_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("building_reservations.id"), nullable=True, index=True
    )
    idempotency_key: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    provider: Mapped[str] = mapped_column(String(32), default="stripe")
    provider_invoice_id: Mapped[str] = mapped_column(String(128), default="", unique=True, index=True)
    qbo_invoice_id: Mapped[str] = mapped_column(String(64), default="", index=True)
    description: Mapped[str] = mapped_column(String(512))
    status: Mapped[str] = mapped_column(String(32), default="draft", index=True)
    accounting_status: Mapped[str] = mapped_column(
        String(32), default="pending_qbo", index=True
    )
    amount_due_cents: Mapped[int] = mapped_column(Integer)
    amount_paid_cents: Mapped[int] = mapped_column(Integer, default=0)
    currency: Mapped[str] = mapped_column(String(8), default="usd")
    due_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    hosted_invoice_url: Mapped[str] = mapped_column(String(1024), default="")
    provider_payload_json: Mapped[dict] = mapped_column(JSON, default=dict)
    created_by: Mapped[str] = mapped_column(String(255), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class BuildingPayment(Base):
    __tablename__ = "building_payments"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    invoice_id: Mapped[str] = mapped_column(ForeignKey("building_invoices.id"), index=True)
    provider: Mapped[str] = mapped_column(String(32), default="stripe")
    provider_payment_id: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    status: Mapped[str] = mapped_column(String(32), index=True)
    amount_cents: Mapped[int] = mapped_column(Integer)
    currency: Mapped[str] = mapped_column(String(8), default="usd")
    evidence_class: Mapped[str] = mapped_column(String(32), default="provider_confirmed")
    posted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    provider_payload_json: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class BuildingBillingAdjustment(Base):
    """Permissioned refund, credit, or write-off workflow with external evidence."""

    __tablename__ = "building_billing_adjustments"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    invoice_id: Mapped[str] = mapped_column(
        ForeignKey("building_invoices.id"), index=True
    )
    adjustment_type: Mapped[str] = mapped_column(String(32), index=True)
    amount_cents: Mapped[int] = mapped_column(Integer)
    currency: Mapped[str] = mapped_column(String(8), default="usd")
    status: Mapped[str] = mapped_column(String(32), default="requested", index=True)
    reason: Mapped[str] = mapped_column(Text)
    provider_reference: Mapped[str] = mapped_column(String(255), default="")
    qbo_reference: Mapped[str] = mapped_column(String(255), default="")
    evidence_json: Mapped[dict] = mapped_column(JSON, default=dict)
    requested_by: Mapped[str] = mapped_column(String(255))
    approved_by: Mapped[str] = mapped_column(String(255), default="")
    approved_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    confirmed_by: Mapped[str] = mapped_column(String(255), default="")
    confirmed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class BuildingStripeEvent(Base):
    __tablename__ = "building_stripe_events"

    event_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    event_type: Mapped[str] = mapped_column(String(128), index=True)
    status: Mapped[str] = mapped_column(String(32), default="received", index=True)
    payload_json: Mapped[dict] = mapped_column(JSON, default=dict)
    error_message: Mapped[str] = mapped_column(Text, default="")
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    processed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
