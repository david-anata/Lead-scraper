from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from sales_support_agent.models.content import (
    ContentAuditEvent,
    ContentChannelVariant,
    ContentJobRun,
    ContentQueueItem,
    ContentSourceAsset,
)
from sales_support_agent.models.database import (
    init_database,
    session_scope,
)
from sales_support_agent.services.access.catalog import ALL_TOOL_KEYS, grants_tool
from sales_support_agent.services.admin_nav import render_agent_nav
from sales_support_agent.services.content_engine import (
    dependency_health,
    ingest_source_assets,
    record_orchestration_check,
    render_content_control_room,
)
from sales_support_agent.services.content_workflow import (
    dispatch_due_variants,
    ingest_episode_package,
    queue_workspace,
    update_variant_state,
)
from sales_support_agent.api.content_router import CONTENT_VIEW, router


def _settings() -> SimpleNamespace:
    return SimpleNamespace(
        gmail_access_token="",
        gmail_client_id="",
        gmail_client_secret="",
        gmail_refresh_token="",
        slack_bot_token="",
        slack_channel_id="",
    )


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


def test_content_permissions_and_navigation_are_first_class() -> None:
    assert {"content.view", "content.operate", "content.admin"} <= ALL_TOOL_KEYS
    assert grants_tool({"content.operate"}, "content.view")
    assert grants_tool({"content.admin"}, "content.view")
    nav = render_agent_nav("content", permissions={"content.view"})
    assert 'href="/admin/content"' in nav
    assert 'class="top-link active"' in nav


def test_source_ingestion_is_idempotent_and_redacts_signed_urls() -> None:
    factory = _factory()
    payload = {
        "asset_id": "asset-1",
        "asset_type": "clip",
        "status": "ready",
        "title": "Operator lesson",
        "source_url": "https://cdn.example.com/clip.mp4?token=secret#frag",
        "metadata": {
            "duration_ms": 15000,
            "api_key": "must-not-persist",
            "authorization": "must-not-persist",
        },
    }
    with session_scope(factory) as session:
        first = ingest_source_assets(
            session,
            episode_id="episode-1",
            assets=[payload],
            actor="test-relay",
        )
        second = ingest_source_assets(
            session,
            episode_id="episode-1",
            assets=[payload],
            actor="test-relay",
        )
        assert first == {"created": 1, "existing": 0}
        assert second == {"created": 0, "existing": 1}
        asset = session.scalar(select(ContentSourceAsset))
        assert asset is not None
        assert asset.source_url == "https://cdn.example.com/clip.mp4"
        assert asset.metadata_json == {"duration_ms": 15000}
        assert len(list(session.scalars(select(ContentAuditEvent)))) == 1


def test_orchestration_preflight_is_idempotent_and_truthful() -> None:
    factory = _factory()
    with session_scope(factory) as session:
        first = record_orchestration_check(
            session,
            job_key="daily_brief",
            run_key="2026-07-28:daily_brief",
            trigger="scheduled",
            actor="test",
            blockers=["drive", "gmail"],
        )
        second = record_orchestration_check(
            session,
            job_key="daily_brief",
            run_key="2026-07-28:daily_brief",
            trigger="scheduled",
            actor="test",
            blockers=["drive", "gmail"],
        )
        assert first.id == second.id
        assert second.status == "blocked"
        assert second.summary_json["blockers"] == ["drive", "gmail"]
        assert len(list(session.scalars(select(ContentJobRun)))) == 1


def test_drive_requires_verified_access(monkeypatch) -> None:
    monkeypatch.setenv("GOOGLE_SERVICE_ACCOUNT_JSON", '{"type":"service_account"}')
    monkeypatch.setenv("CONTENT_DRIVE_PARENT_ID", "parent")
    monkeypatch.setenv("CONTENT_DRIVE_VERIFIED", "false")
    states = {item["key"]: item["status"] for item in dependency_health(_settings())}
    assert states["drive"] == "blocked"
    monkeypatch.setenv("CONTENT_DRIVE_VERIFIED", "true")
    states = {item["key"]: item["status"] for item in dependency_health(_settings())}
    assert states["drive"] == "ready"


def test_control_room_uses_canonical_structure_and_truthful_empty_state() -> None:
    factory = _factory()
    user = {
        "email": "david@anatainc.com",
        "name": "David",
        "permissions": {"content.view"},
        "is_superadmin": False,
    }
    with session_scope(factory) as session:
        page = render_content_control_room(session, _settings(), user=user)
    assert 'class="app-container app-page content-page"' in page
    assert "Content Control Room" in page
    assert "Riverside-to-growth production line" in page
    assert "No content runs yet" in page
    assert "Developer-only MCP access is not counted as production-ready." in page
    assert "The Six C's" in page


def test_content_routes_require_trusted_key_and_render_for_authorized_user() -> None:
    factory = _factory()
    settings = _settings()
    settings.internal_api_key = "internal-test-key"
    app = FastAPI()
    app.state.session_factory = factory
    app.state.settings = settings
    app.include_router(router)
    app.dependency_overrides[CONTENT_VIEW] = lambda: {
        "email": "david@anatainc.com",
        "name": "David",
        "permissions": {"content.view"},
        "is_superadmin": False,
    }
    client = TestClient(app)

    assert client.get("/admin/content").status_code == 200
    assert client.get("/admin/api/content/status").json()["status"] == "blocked"
    assert (
        client.post(
            "/api/jobs/content/source-assets",
            json={
                "episode_id": "episode-1",
                "assets": [{"asset_id": "asset-1", "asset_type": "transcript"}],
            },
        ).status_code
        == 401
    )
    ingested = client.post(
        "/api/jobs/content/source-assets",
        headers={"X-Internal-Api-Key": "internal-test-key"},
        json={
            "episode_id": "episode-1",
            "assets": [{"asset_id": "asset-1", "asset_type": "transcript"}],
        },
    )
    assert ingested.status_code == 200
    assert ingested.json()["created"] == 1

    run = client.post(
        "/api/jobs/content/run",
        headers={"X-Internal-Api-Key": "internal-test-key"},
        json={"mode": "social_distribution", "force": True},
    )
    assert run.status_code == 200
    assert run.json()["status"] == "blocked"
    run_id = run.json()["details"]["social_distribution"]["run_id"]
    assert client.get(f"/admin/content/runs/{run_id}").status_code == 200


def test_episode_package_creates_dense_independent_destination_schedule() -> None:
    factory = _factory()
    clips = [
        {
            "asset_id": f"clip-{index}",
            "title": f"Clip {index}",
            "duration_ms": 30_000,
            "preview_url": f"https://riverside.example/{index}",
            "media_url": f"https://media.example/{index}.mp4",
            "rank": index,
            "six_c": {"channel": 4, "credibility": 5},
            "channel_copy": {"tiktok": {"copy": f"TikTok copy {index}"}},
        }
        for index in range(1, 11)
    ]
    with session_scope(factory) as session:
        first = ingest_episode_package(
            session,
            episode_id="episode-2026-07-30",
            episode_title="Gabe & David",
            episode_url="https://riverside.example/episode",
            clips=clips,
            actor="test",
        )
        second = ingest_episode_package(
            session,
            episode_id="episode-2026-07-30",
            episode_title="Gabe & David",
            episode_url="https://riverside.example/episode",
            clips=clips,
            actor="test",
        )
        workspace = queue_workspace(session)
        assert first == {"queue_items_created": 10, "variants_created": 42}
        assert second == {"queue_items_created": 0, "variants_created": 0}
        assert workspace["counts"]["items"] == 10
        assert workspace["counts"]["scheduled"] == 42
        assert [item.rank for item in workspace["items"]] == list(range(1, 11))
        assert len(list(session.scalars(select(ContentChannelVariant)))) == 42


def test_one_destination_failure_does_not_stop_other_destinations(monkeypatch) -> None:
    factory = _factory()
    with session_scope(factory) as session:
        ingest_episode_package(
            session,
            episode_id="episode-1",
            episode_title="Episode",
            episode_url="https://riverside.example/episode",
            clips=[
                {
                    "asset_id": "clip-1",
                    "title": "Clip",
                    "duration_ms": 10_000,
                    "media_url": "https://media.example/clip.mp4",
                }
            ],
            actor="test",
        )
        variants = list(session.scalars(select(ContentChannelVariant)))
        for variant in variants:
            variant.scheduled_for = datetime.now(timezone.utc) - timedelta(minutes=1)
        session.flush()

        class _Result:
            def __init__(self, accepted: bool):
                self.accepted = accepted
                self.receipt = "receipt" if accepted else ""
                self.public_url = ""
                self.safe_error = "" if accepted else "failed safely"

        class _Connector:
            def __init__(self, accepted: bool):
                self.accepted = accepted

            def publish(self, payload, idempotency_key):
                return _Result(self.accepted)

        monkeypatch.setattr(
            "sales_support_agent.services.content_workflow._connector_for",
            lambda channel: _Connector(channel != "instagram"),
        )
        result = dispatch_due_variants(session)
        states = {row.channel: row.status for row in variants}
        assert result["attempted"] == 5
        assert result["accepted"] == 4
        assert result["failed"] == 1
        assert states["instagram"] == "failed"
        assert states["tiktok"] == "accepted"


def test_operator_can_pause_and_resume_one_destination() -> None:
    factory = _factory()
    with session_scope(factory) as session:
        item = ContentQueueItem(
            id="item",
            episode_external_id="episode",
            source_external_id="clip",
            title="Clip",
        )
        variant = ContentChannelVariant(
            id="variant",
            queue_item_id="item",
            channel="tiktok",
            destination="Anata",
            idempotency_key="key",
        )
        session.add_all([item, variant])
        session.flush()
        update_variant_state(session, variant_id="variant", action="pause", actor="operator")
        assert variant.status == "paused"
        update_variant_state(session, variant_id="variant", action="resume", actor="operator")
        assert variant.status == "queued"


def test_episode_package_route_is_idempotent() -> None:
    factory = _factory()
    settings = _settings()
    settings.internal_api_key = "internal-test-key"
    app = FastAPI()
    app.state.session_factory = factory
    app.state.settings = settings
    app.include_router(router)
    app.dependency_overrides[CONTENT_VIEW] = lambda: {
        "email": "david@anatainc.com",
        "permissions": {"content.view", "content.operate"},
    }
    client = TestClient(app)
    payload = {
        "episode_id": "episode-1",
        "episode_title": "Gabe & David",
        "episode_url": "https://riverside.example/episode",
        "clips": [{"asset_id": "clip-1", "title": "A clip"}],
    }
    response = client.post(
        "/api/jobs/content/episodes",
        headers={"X-Internal-Api-Key": "internal-test-key"},
        json=payload,
    )
    assert response.status_code == 200
    assert response.json()["queue_items_created"] == 1
