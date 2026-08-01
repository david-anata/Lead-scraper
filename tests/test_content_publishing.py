from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from sales_support_agent.integrations.content_relay import RelayResult
from sales_support_agent.models.content import ContentArtifact, ContentPublication
from sales_support_agent.models.database import Base
from sales_support_agent.services.content_publishing import (
    channel_publish_readiness,
    publish_daily_portfolio,
    publish_artifact,
)


def _session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return Session(engine)


def _artifact(session: Session, *, channel: str = "linkedin_company") -> ContentArtifact:
    now = datetime.now(timezone.utc)
    row = ContentArtifact(
        id="artifact-1",
        run_id="run-1",
        source_asset_id="source-1",
        artifact_type="native_candidate",
        channel=channel,
        playbook_version="v1",
        status="needs_review",
        title="A native lesson",
        body="One specific operator lesson.",
        content_fingerprint="f" * 64,
        quality_gate_json={"passed": True},
        lineage_json={"source_asset_id": "source-1"},
        created_at=now,
        updated_at=now,
    )
    session.add(row)
    session.commit()
    return row


def test_channel_readiness_requires_identity_and_live_mode(monkeypatch) -> None:
    monkeypatch.setenv("CONTENT_LINKEDIN_CONNECTOR_URL", "https://relay.example.com")
    monkeypatch.setenv("CONTENT_LINKEDIN_CONNECTOR_KEY", "secret")
    monkeypatch.setenv("CONTENT_LINKEDIN_CONNECTOR_VERIFIED", "true")
    monkeypatch.setenv("CONTENT_LINKEDIN_COMPANY_LIVE_APPROVED", "true")
    assert channel_publish_readiness("linkedin_company")["state"] == "not_connected"
    monkeypatch.setenv("CONTENT_LINKEDIN_COMPANY_ID", "company-1")
    assert channel_publish_readiness("linkedin_company")["state"] == "shadow"
    monkeypatch.setenv("CONTENT_PUBLISHING_MODE", "live")
    assert channel_publish_readiness("linkedin_company")["ready"] is True


def test_google_business_readiness_is_independent(monkeypatch) -> None:
    monkeypatch.setenv("CONTENT_GOOGLE_BUSINESS_CONNECTOR_URL", "https://relay.example.com")
    monkeypatch.setenv("CONTENT_GOOGLE_BUSINESS_CONNECTOR_KEY", "secret")
    monkeypatch.setenv("CONTENT_GOOGLE_BUSINESS_CONNECTOR_VERIFIED", "true")
    monkeypatch.setenv("CONTENT_GOOGLE_BUSINESS_PROFILE_ID", "anata-profile")
    monkeypatch.setenv("CONTENT_GOOGLE_BUSINESS_LIVE_APPROVED", "true")
    monkeypatch.setenv("CONTENT_PUBLISHING_MODE", "live")
    assert channel_publish_readiness("google_business")["ready"] is True


def test_publish_requires_explicit_confirmation() -> None:
    session = _session()
    row = _artifact(session)
    with pytest.raises(ValueError, match="confirmation"):
        publish_artifact(
            session,
            artifact_id=row.id,
            actor="operator@example.com",
            confirmed=False,
        )


def test_publish_records_verified_evidence(monkeypatch) -> None:
    session = _session()
    row = _artifact(session)
    monkeypatch.setenv("CONTENT_LINKEDIN_CONNECTOR_URL", "https://relay.example.com")
    monkeypatch.setenv("CONTENT_LINKEDIN_CONNECTOR_KEY", "secret")
    monkeypatch.setenv("CONTENT_LINKEDIN_CONNECTOR_VERIFIED", "true")
    monkeypatch.setenv("CONTENT_LINKEDIN_COMPANY_LIVE_APPROVED", "true")
    monkeypatch.setenv("CONTENT_LINKEDIN_COMPANY_ID", "company-1")
    monkeypatch.setenv("CONTENT_PUBLISHING_MODE", "live")

    def fake_execute(self, **kwargs):
        return RelayResult(
            accepted=True,
            verified=True,
            status="delivered",
            provider_receipt="provider-123",
            public_url="https://linkedin.example/post/123",
        )

    monkeypatch.setattr(
        "sales_support_agent.services.content_publishing.ContentRelayClient.execute",
        fake_execute,
    )
    publication = publish_artifact(
        session,
        artifact_id=row.id,
        actor="operator@example.com",
        confirmed=True,
    )
    assert publication.status == "delivered"
    assert publication.verified_at is not None
    assert session.query(ContentPublication).count() == 1
    session.refresh(row)
    assert row.status == "delivered"
    assert row.external_url == "https://linkedin.example/post/123"


def test_staging_only_channel_never_publishes() -> None:
    session = _session()
    row = _artifact(session, channel="x")
    with pytest.raises(ValueError, match="staging-only"):
        publish_artifact(
            session,
            artifact_id=row.id,
            actor="operator@example.com",
            confirmed=True,
        )


def test_daily_portfolio_isolates_one_channel_failure(monkeypatch) -> None:
    session = _session()
    now = datetime(2026, 7, 28, 16, 0, tzinfo=timezone.utc)  # Tuesday
    monkeypatch.setattr(
        "sales_support_agent.services.content_publishing.channel_publish_readiness",
        lambda channel: {"ready": True, "message": "ready"},
    )

    def fake_publish(session, *, channel, actor, now):
        if channel == "instagram":
            raise ConnectionError("provider unavailable")
        return None

    monkeypatch.setattr(
        "sales_support_agent.services.content_publishing.publish_best_candidate",
        fake_publish,
    )
    result = publish_daily_portfolio(session, actor="scheduler", now=now)
    assert result["linkedin_company"]["status"] == "not_eligible"
    assert result["instagram"]["status"] == "failed"
