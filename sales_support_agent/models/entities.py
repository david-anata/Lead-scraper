"""SQLAlchemy entities for local auditability and task mirrors."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import JSON, Boolean, DateTime, Index, Integer, String, Text
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

    # Recurring linkage
    recurring_template_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    recurring_rule: Mapped[str] = mapped_column(String(64), default="")  # "weekly"|"biweekly"|"monthly"|"custom"

    # CSV ↔ planned obligation matching
    matched_to_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)

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
    )


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


class AdGoal(Base):
    """A standing set of advertising/sales targets the audit scales toward.

    Snapshotted into each AuditRun.goal_snapshot_json at run time so historical
    runs reflect the goal that was active then, even after the goal is edited.
    Percentages are basis points; money is integer cents.
    """

    __tablename__ = "ad_goals"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
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
    picture_url: Mapped[str] = mapped_column(String(512), default="")
    role_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    status: Mapped[str] = mapped_column(String(16), default="active", index=True)  # active|suspended
    is_superadmin: Mapped[bool] = mapped_column(Boolean, default=False)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    last_login_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)


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
