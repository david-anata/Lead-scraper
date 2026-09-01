from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from sales_support_agent.integrations.content_relay import RelayResult
from sales_support_agent.models.content import ContentArtifact, ContentPublication
from sales_support_agent.models.database import Base
from sales_support_agent.services.content_publishing import (
    DAILY_PORTFOLIO,
    WEEKLY_CAPS,
    VERTICAL_SYNDICATION_CHANNELS,
    channel_publish_readiness,
    publish_daily_portfolio,
    publish_artifact,
)


def test_company_destinations_run_seven_days_per_week() -> None:
    assert WEEKLY_CAPS["linkedin_company"] == 7
    assert WEEKLY_CAPS["google_business"] == 7
    for channels in DAILY_PORTFOLIO.values():
        assert "linkedin_company" in channels
        assert "google_business" in channels


def test_every_vertical_destination_runs_daily_without_personal_channels() -> None:
    expected = {
        "tiktok",
        "instagram",
        "youtube",
        "linkedin_company",
        "google_business",
    }
    assert set(VERTICAL_SYNDICATION_CHANNELS) == expected
    assert all(set(channels) == expected for channels in DAILY_PORTFOLIO.values())
    assert "linkedin_personal" not in VERTICAL_SYNDICATION_CHANNELS
    assert "x" not in VERTICAL_SYNDICATION_CHANNELS


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


def test_vertical_publish_prefers_magic_clip_over_full_video(monkeypatch) -> None:
    session = _session()
    row = _artifact(session, channel="tiktok")
    row.lineage_json = {
        "media_assets": [
            {"asset_type": "video", "source_url": "https://riverside.example/full"},
            {"asset_type": "clip", "source_url": "https://riverside.example/clip"},
        ]
    }
    session.commit()
    for name, value in {
        "CONTENT_TIKTOK_CONNECTOR_URL": "https://relay.example.com",
        "CONTENT_TIKTOK_CONNECTOR_KEY": "secret",
        "CONTENT_TIKTOK_CONNECTOR_VERIFIED": "true",
        "CONTENT_TIKTOK_ACCOUNT_ID": "anata-tiktok",
        "CONTENT_TIKTOK_LIVE_APPROVED": "true",
        "CONTENT_PUBLISHING_MODE": "live",
        "RIVERSIDE_API_KEY": "riverside-secret",
    }.items():
        monkeypatch.setenv(name, value)
    resolved = []
    monkeypatch.setattr(
        "sales_support_agent.integrations.riverside.RiversideClient.resolve_download_url",
        lambda self, source_url: resolved.append(source_url) or "https://signed.example/clip.mp4",
    )
    monkeypatch.setattr(
        "sales_support_agent.services.content_publishing.ContentRelayClient.execute",
        lambda self, **kwargs: RelayResult(True, True, "delivered", "receipt"),
    )

    publish_artifact(session, artifact_id=row.id, actor="test", confirmed=True)

    assert resolved == ["https://riverside.example/clip"]


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
    for channel in VERTICAL_SYNDICATION_CHANNELS:
        monkeypatch.setenv(
            f"CONTENT_{'LINKEDIN_COMPANY' if channel == 'linkedin_company' else channel.upper()}_AUTO_PUBLISH_ENABLED",
            "true",
        )
    artifacts = {}
    for index, channel in enumerate(VERTICAL_SYNDICATION_CHANNELS):
        row = ContentArtifact(
            id=f"artifact-{index}",
            run_id="run-1",
            source_asset_id="shared-source",
            artifact_type="native_candidate",
            channel=channel,
            playbook_version="v2",
            status="needs_review",
            title=f"Native {channel}",
            body=f"Native copy for {channel}",
            content_fingerprint=str(index) * 64,
            quality_gate_json={"passed": True},
            lineage_json={},
        )
        session.add(row)
        artifacts[channel] = row
    session.commit()
    monkeypatch.setattr(
        "sales_support_agent.services.content_publishing.rank_publishable_artifacts",
        lambda session, *, channel, now: [(artifacts[channel], 100.0)],
    )

    def fake_publish(session, *, artifact_id, actor, confirmed):
        channel = session.get(ContentArtifact, artifact_id).channel
        if channel == "instagram":
            raise ConnectionError("provider unavailable")
        return ContentPublication(
            id=f"publication-{channel}",
            run_id="run-1",
            source_asset_id="shared-source",
            channel=channel,
            destination=f"destination-{channel}",
            playbook_version="v2",
            status="delivered",
            content_fingerprint=f"fingerprint-{channel}",
        )

    monkeypatch.setattr(
        "sales_support_agent.services.content_publishing.publish_artifact",
        fake_publish,
    )
    result = publish_daily_portfolio(session, actor="scheduler", now=now)
    assert result["linkedin_company"]["status"] == "delivered"
    assert result["instagram"]["status"] == "failed"
    delivered_sources = {
        row["source_asset_id"]
        for row in result.values()
        if row["status"] == "delivered"
    }
    assert delivered_sources == {"shared-source"}
