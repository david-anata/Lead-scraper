"""Durable records for the Content & Growth Engine."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import Boolean, JSON, DateTime, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from sales_support_agent.models.database import Base


class ContentJobRun(Base):
    """One idempotent scheduled or operator-triggered content job."""

    __tablename__ = "content_job_runs"
    __table_args__ = (
        UniqueConstraint("job_key", "run_key", name="uq_content_job_run_key"),
    )

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    job_key: Mapped[str] = mapped_column(String(64), index=True)
    run_key: Mapped[str] = mapped_column(String(160))
    trigger: Mapped[str] = mapped_column(String(32), default="scheduled")
    status: Mapped[str] = mapped_column(String(32), default="queued", index=True)
    scheduled_for: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True, index=True
    )
    started_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    completed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True, index=True
    )
    attempt_count: Mapped[int] = mapped_column(Integer, default=0)
    idempotency_key: Mapped[str] = mapped_column(String(160), default="", index=True)
    input_fingerprint: Mapped[str] = mapped_column(String(128), default="")
    safe_error_code: Mapped[str] = mapped_column(String(64), default="")
    safe_error_message: Mapped[str] = mapped_column(Text, default="")
    summary_json: Mapped[dict] = mapped_column(JSON, default=dict)
    created_by: Mapped[str] = mapped_column(String(255), default="system")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, index=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow
    )


class ContentSourceAsset(Base):
    """Normalized Riverside source media and transcript lineage."""

    __tablename__ = "content_source_assets"
    __table_args__ = (
        UniqueConstraint(
            "provider",
            "external_asset_id",
            name="uq_content_source_provider_asset",
        ),
    )

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    provider: Mapped[str] = mapped_column(String(32), default="riverside")
    episode_external_id: Mapped[str] = mapped_column(String(255), index=True)
    external_asset_id: Mapped[str] = mapped_column(String(255))
    asset_type: Mapped[str] = mapped_column(String(64), index=True)
    processing_status: Mapped[str] = mapped_column(
        String(32), default="ready", index=True
    )
    title: Mapped[str] = mapped_column(String(500), default="")
    speaker: Mapped[str] = mapped_column(String(255), default="")
    transcript_start_ms: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True
    )
    transcript_end_ms: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True
    )
    source_url: Mapped[str] = mapped_column(Text, default="")
    source_fingerprint: Mapped[str] = mapped_column(String(128), default="")
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict)
    ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, index=True
    )


class ContentPublication(Base):
    """A channel-specific publication attempt and verification record."""

    __tablename__ = "content_publications"
    __table_args__ = (
        UniqueConstraint(
            "channel",
            "destination",
            "content_fingerprint",
            name="uq_content_publication_destination_fingerprint",
        ),
    )

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(64), index=True)
    source_asset_id: Mapped[str] = mapped_column(String(64), default="", index=True)
    channel: Mapped[str] = mapped_column(String(32), index=True)
    destination: Mapped[str] = mapped_column(String(255), default="")
    playbook_version: Mapped[str] = mapped_column(String(32), default="v1")
    status: Mapped[str] = mapped_column(String(32), default="queued", index=True)
    content_fingerprint: Mapped[str] = mapped_column(String(128))
    provider_receipt: Mapped[str] = mapped_column(String(500), default="")
    public_url: Mapped[str] = mapped_column(Text, default="")
    quality_gate_json: Mapped[dict] = mapped_column(JSON, default=dict)
    attempt_count: Mapped[int] = mapped_column(Integer, default=0)
    published_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    verified_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, index=True
    )


class ContentPerformanceObservation(Base):
    """Comparable, evidence-labeled performance observed from a destination."""

    __tablename__ = "content_performance_observations"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    publication_id: Mapped[str] = mapped_column(String(64), index=True)
    platform: Mapped[str] = mapped_column(String(32), index=True)
    format: Mapped[str] = mapped_column(String(64), default="")
    objective: Mapped[str] = mapped_column(String(64), default="")
    window_started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    window_ended_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    metrics_json: Mapped[dict] = mapped_column(JSON, default=dict)
    data_availability: Mapped[str] = mapped_column(
        String(32), default="available"
    )
    sample_confidence: Mapped[str] = mapped_column(String(32), default="low")
    observed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, index=True
    )


class ContentAuditEvent(Base):
    """Append-only safe audit event for content operations."""

    __tablename__ = "content_audit_events"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(64), default="", index=True)
    actor_type: Mapped[str] = mapped_column(String(32), default="system")
    actor_id: Mapped[str] = mapped_column(String(255), default="system")
    event_type: Mapped[str] = mapped_column(String(64), index=True)
    object_type: Mapped[str] = mapped_column(String(64), default="")
    object_id: Mapped[str] = mapped_column(String(255), default="")
    details_json: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, index=True
    )


class ContentQueueItem(Base):
    """One Riverside-derived creative moving through the publishing queue."""

    __tablename__ = "content_queue_items"
    __table_args__ = (
        UniqueConstraint(
            "episode_external_id",
            "source_external_id",
            name="uq_content_queue_episode_source",
        ),
    )

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    episode_external_id: Mapped[str] = mapped_column(String(255), index=True)
    source_external_id: Mapped[str] = mapped_column(String(255))
    title: Mapped[str] = mapped_column(String(500))
    duration_ms: Mapped[int] = mapped_column(Integer, default=0)
    source_url: Mapped[str] = mapped_column(Text, default="")
    preview_url: Mapped[str] = mapped_column(Text, default="")
    transcript_excerpt: Mapped[str] = mapped_column(Text, default="")
    status: Mapped[str] = mapped_column(String(32), default="queued", index=True)
    rank: Mapped[int] = mapped_column(Integer, default=0)
    six_c_json: Mapped[dict] = mapped_column(JSON, default=dict)
    recycle_eligible: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, index=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow
    )


class ContentChannelVariant(Base):
    """A channel-native treatment and its independent delivery state."""

    __tablename__ = "content_channel_variants"
    __table_args__ = (
        UniqueConstraint(
            "queue_item_id",
            "channel",
            "cycle_key",
            name="uq_content_variant_item_channel_cycle",
        ),
    )

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    queue_item_id: Mapped[str] = mapped_column(String(64), index=True)
    channel: Mapped[str] = mapped_column(String(32), index=True)
    destination: Mapped[str] = mapped_column(String(255), default="")
    provider: Mapped[str] = mapped_column(String(32), default="")
    cycle_key: Mapped[str] = mapped_column(String(64), default="launch")
    title: Mapped[str] = mapped_column(String(500), default="")
    copy_text: Mapped[str] = mapped_column(Text, default="")
    status: Mapped[str] = mapped_column(String(32), default="queued", index=True)
    scheduled_for: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True, index=True
    )
    manual_only: Mapped[bool] = mapped_column(Boolean, default=False)
    attempt_count: Mapped[int] = mapped_column(Integer, default=0)
    idempotency_key: Mapped[str] = mapped_column(String(160), default="", index=True)
    provider_receipt: Mapped[str] = mapped_column(String(500), default="")
    public_url: Mapped[str] = mapped_column(Text, default="")
    safe_error_message: Mapped[str] = mapped_column(Text, default="")
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict)
    published_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, index=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow
    )


class ContentPersonalDraft(Base):
    """A performance-led draft that David posts manually on LinkedIn."""

    __tablename__ = "content_personal_drafts"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    queue_item_id: Mapped[str] = mapped_column(String(64), default="", index=True)
    title: Mapped[str] = mapped_column(String(500), default="")
    copy_text: Mapped[str] = mapped_column(Text, default="")
    status: Mapped[str] = mapped_column(String(32), default="draft", index=True)
    suggested_for: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True, index=True
    )
    evidence_json: Mapped[dict] = mapped_column(JSON, default=dict)
    posted_url: Mapped[str] = mapped_column(Text, default="")
    posted_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, index=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow
    )
