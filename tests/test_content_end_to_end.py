from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from sales_support_agent.integrations.content_relay import RelayResult
from sales_support_agent.integrations.riverside import RiversideEpisode
from sales_support_agent.models.content import (
    ContentArtifact,
    ContentAuditEvent,
    ContentPublication,
    ContentSourceAsset,
)
from sales_support_agent.models.database import init_database, session_scope
from sales_support_agent.services.content_automation import run_content_cycle


def _factory():
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
        future=True,
    )
    factory = sessionmaker(
        bind=engine,
        autoflush=False,
        autocommit=False,
        future=True,
        expire_on_commit=False,
    )
    init_database(factory)
    return factory


def _settings() -> SimpleNamespace:
    return SimpleNamespace(
        gmail_access_token="",
        gmail_client_id="",
        gmail_client_secret="",
        gmail_refresh_token="",
        slack_bot_token="",
        slack_channel_id="",
    )


def test_riverside_to_native_copy_to_verified_daily_publication(
    monkeypatch,
) -> None:
    factory = _factory()
    now = datetime(2026, 8, 3, 16, 0, tzinfo=timezone.utc)  # Monday 10:00 Denver
    monkeypatch.setenv("RIVERSIDE_API_KEY", "riverside-secret")
    monkeypatch.setenv("GOOGLE_SERVICE_ACCOUNT_JSON", '{"type":"service_account"}')
    monkeypatch.setenv("CONTENT_DRIVE_PARENT_ID", "drive-parent")
    monkeypatch.setenv("CONTENT_DRIVE_VERIFIED", "true")
    monkeypatch.setenv("CONTENT_PUBLISHING_MODE", "live")
    monkeypatch.setenv("CONTENT_LINKEDIN_CONNECTOR_URL", "https://relay.example.com")
    monkeypatch.setenv("CONTENT_LINKEDIN_CONNECTOR_KEY", "relay-secret")
    monkeypatch.setenv("CONTENT_LINKEDIN_CONNECTOR_VERIFIED", "true")
    monkeypatch.setenv("CONTENT_LINKEDIN_PERSON_ID", "david")
    monkeypatch.setenv("CONTENT_LINKEDIN_PERSONAL_LIVE_APPROVED", "true")
    monkeypatch.setenv("CONTENT_LINKEDIN_PERSONAL_AUTO_PUBLISH_ENABLED", "true")
    monkeypatch.setenv("CONTENT_YOUTUBE_CONNECTOR_URL", "https://relay.example.com")
    monkeypatch.setenv("CONTENT_YOUTUBE_CONNECTOR_KEY", "relay-secret")
    monkeypatch.setenv("CONTENT_YOUTUBE_CONNECTOR_VERIFIED", "true")
    monkeypatch.setenv("CONTENT_YOUTUBE_CHANNEL_ID", "anata-youtube")
    monkeypatch.setenv("CONTENT_YOUTUBE_LIVE_APPROVED", "true")
    monkeypatch.setenv("CONTENT_YOUTUBE_AUTO_PUBLISH_ENABLED", "true")
    monkeypatch.setenv("CONTENT_COPY_AI_ENABLED", "false")

    episode = RiversideEpisode(
        "episode-100",
        [
            {
                "asset_id": "video-100",
                "asset_type": "video",
                "status": "ready",
                "title": "Inventory accuracy protects cash",
                "source_url": (
                    "https://platform.riverside.fm/api/v3/download/file/video-100"
                ),
                "metadata": {},
            },
            {
                "asset_id": "transcript-100",
                "asset_type": "transcript",
                "status": "ready",
                "title": "Inventory accuracy protects cash",
                "source_url": (
                    "https://platform.riverside.fm/api/v3/download/transcription/"
                    "episode-100"
                ),
                "transcript_text": (
                    "Inventory accuracy is a cash discipline, not a warehouse "
                    "report. Operators need one trusted inventory position before "
                    "they make a purchase decision. The next action needs one owner "
                    "and the result must be verified."
                ),
                "metadata": {},
            },
        ],
    )
    monkeypatch.setattr(
        "sales_support_agent.integrations.riverside.RiversideClient.list_ready_recordings",
        lambda self, **kwargs: [episode],
    )
    monkeypatch.setattr(
        "sales_support_agent.integrations.riverside.RiversideClient.resolve_download_url",
        lambda self, source_url: "https://signed.example.com/video-100.mp4",
    )

    def fake_execute(self, **kwargs):
        action = kwargs["action_key"]
        return RelayResult(
            accepted=True,
            verified=True,
            status="delivered",
            provider_receipt=f"receipt-{action}",
            public_url=f"https://published.example.com/{action}",
        )

    monkeypatch.setattr(
        "sales_support_agent.services.content_publishing.ContentRelayClient.execute",
        fake_execute,
    )

    harvested = run_content_cycle(
        factory,
        _settings(),
        mode="episode_harvest",
        force=True,
        now=now,
    )
    assert harvested["details"]["episode_harvest"]["staged_candidates"] == {
            "created": 6,
        "existing": 0,
        "rejected": 0,
    }
    distributed = run_content_cycle(
        factory,
        _settings(),
        mode="daily_distribution",
        force=True,
        now=now,
    )
    portfolio = distributed["details"]["daily_distribution"]["daily_portfolio"]
    assert portfolio["linkedin_personal"]["status"] == "delivered"
    assert portfolio["youtube"]["status"] == "delivered"

    with session_scope(factory) as session:
        assert session.scalar(select(func.count()).select_from(ContentSourceAsset)) == 2
        artifacts = list(session.scalars(select(ContentArtifact)))
        assert len(artifacts) == 6
        assert len({item.body for item in artifacts}) == 6
        assert all(item.quality_gate_json["passed"] for item in artifacts)
        publications = list(session.scalars(select(ContentPublication)))
        assert len(publications) == 2
        assert all(item.provider_receipt for item in publications)
        assert all(item.public_url for item in publications)
        assert not any(item.channel == "x" for item in publications)
        assert session.scalar(
            select(func.count()).select_from(ContentAuditEvent)
        ) >= 10
