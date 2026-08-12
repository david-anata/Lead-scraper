from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from sales_support_agent.models.content import (
    ContentArtifact,
    ContentChannelPlaybook,
    ContentDependencyCheck,
    ContentJobRun,
    ContentSourceAsset,
    ContentTranscript,
)
from sales_support_agent.models.database import init_database, session_scope
from sales_support_agent.services.content_automation import (
    JOB_DEFINITIONS,
    due_scheduled_jobs,
    quality_gate,
    stage_native_candidates,
    stage_daily_brief,
    run_content_cycle,
    seed_default_playbooks,
)
from sales_support_agent.services.content_engine import (
    ingest_source_assets,
    record_orchestration_check,
)


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


def test_default_playbooks_are_versioned_and_idempotent() -> None:
    factory = _factory()
    with session_scope(factory) as session:
        assert seed_default_playbooks(session) == 6
        assert seed_default_playbooks(session) == 0
        rows = list(
            session.scalars(
                select(ContentChannelPlaybook).order_by(
                    ContentChannelPlaybook.channel
                )
            )
        )
        assert len(rows) == 6
        assert {row.version for row in rows} == {"v2"}
        linkedin = next(row for row in rows if row.channel == "linkedin_personal")
        assert linkedin.priority == "primary_authority"
        assert linkedin.format_rules_json["cross_post_copy"] is False
        x_playbook = next(row for row in rows if row.channel == "x")
        assert x_playbook.format_rules_json["publishing_enabled"] is False


def test_episode_harvest_does_not_require_optional_drive_archival() -> None:
    assert JOB_DEFINITIONS["episode_harvest"].required_dependencies == (
        "riverside",
    )


def test_scheduler_catches_up_after_six_and_deduplicates_the_day() -> None:
    factory = _factory()
    before = datetime(2026, 7, 29, 11, 59, tzinfo=timezone.utc)  # 05:59 Denver
    after = datetime(2026, 7, 29, 12, 1, tzinfo=timezone.utc)  # 06:01 Denver
    with session_scope(factory) as session:
        assert due_scheduled_jobs(session, now=before) == []
        assert due_scheduled_jobs(session, now=after) == [
            ("daily_brief", "2026-07-29:daily_brief"),
            ("episode_harvest", "2026-07-29:episode_harvest"),
        ]
        session.add(
            ContentJobRun(
                id="run-1",
                job_key="daily_brief",
                run_key="2026-07-29:daily_brief",
                idempotency_key="same-day",
            )
        )
        session.commit()
        assert due_scheduled_jobs(session, now=after) == [
            ("episode_harvest", "2026-07-29:episode_harvest")
        ]


def test_monday_scheduler_includes_comparable_weekly_retrospective() -> None:
    factory = _factory()
    monday = datetime(2026, 8, 3, 14, 0, tzinfo=timezone.utc)
    with session_scope(factory) as session:
        assert due_scheduled_jobs(session, now=monday) == [
            ("daily_brief", "2026-08-03:daily_brief"),
            ("episode_harvest", "2026-08-03:episode_harvest"),
            ("weekly_retrospective", "2026-W32:weekly_retrospective"),
        ]


def test_shadow_cycle_records_dependencies_and_cross_instance_deduplication() -> None:
    factory = _factory()
    now = datetime(2026, 7, 29, 14, 0, tzinfo=timezone.utc)
    first = run_content_cycle(
        factory,
        _settings(),
        mode="scheduled",
        now=now,
    )
    second = run_content_cycle(
        factory,
        _settings(),
        mode="scheduled",
        now=now,
    )
    assert first["status"] == "blocked"
    assert first["details"]["daily_brief"]["blockers"] == [
        "drive",
        "gmail",
        "slack",
    ]
    assert second["status"] == "not_due"
    with session_scope(factory) as session:
        assert (
            session.scalar(
                select(func.count()).select_from(ContentDependencyCheck)
            )
            == 24
        )
        run = session.scalar(select(ContentJobRun))
        assert run is not None
        assert run.summary_json["execution_mode"] == "shadow"
        assert run.summary_json["schedule"] == "daily_after_0600"


def test_social_cycle_stages_separate_native_candidates_with_lineage(
    monkeypatch,
) -> None:
    factory = _factory()
    monkeypatch.setenv("CONTENT_RIVERSIDE_RELAY_ENABLED", "true")
    with session_scope(factory) as session:
        ingest_source_assets(
            session,
            episode_id="episode-42",
            actor="test",
            assets=[
                {
                    "asset_id": "clip-42",
                    "asset_type": "clip",
                    "title": "Why inventory accuracy protects cash",
                    "status": "ready",
                    "transcript_start_ms": 1200,
                    "transcript_end_ms": 18000,
                    "transcript_text": (
                        "Inventory accuracy is a cash discipline, not a warehouse "
                        "report. Operators need one trusted inventory position."
                    ),
                }
            ],
        )

    result = run_content_cycle(
        factory,
        _settings(),
        mode="social_distribution",
        force=True,
        now=datetime(2026, 7, 29, 15, 0, tzinfo=timezone.utc),
    )
    assert result["status"] == "needs_review"
    assert result["details"]["social_distribution"]["staged_candidates"] == {
        "created": 6,
        "existing": 0,
        "rejected": 0,
    }
    with session_scope(factory) as session:
        artifacts = list(
            session.scalars(
                select(ContentArtifact).order_by(ContentArtifact.channel)
            )
        )
        assert {row.channel for row in artifacts} == {
            "google_business",
            "instagram",
            "linkedin_company",
            "linkedin_personal",
            "x",
            "youtube",
        }
        assert len({row.body for row in artifacts}) == 6
        assert {row.status for row in artifacts} == {"needs_review"}
        assert all(row.playbook_version == "v2" for row in artifacts)
        assert all(
            row.lineage_json["episode_external_id"] == "episode-42"
            for row in artifacts
        )
        assert all(row.quality_gate_json["passed"] for row in artifacts)
        assert all(
            not row.body.lower().startswith(("build ", "turn ", "write ", "create "))
            for row in artifacts
        )
        assert all(row.quality_gate_json["six_cs"]["collection"] for row in artifacts)
        assert session.scalar(
            select(func.count()).select_from(ContentTranscript)
        ) == 1


def test_clip_quality_gate_requires_transcript_interval_and_no_em_dash() -> None:
    source = ContentSourceAsset(
        id="source",
        episode_external_id="episode",
        external_asset_id="clip",
        asset_type="clip",
        processing_status="ready",
        source_fingerprint="fingerprint",
    )
    failed = quality_gate(
        channel="linkedin_company",
        body="A useful point \N{EM DASH} without lineage timing.",
        source_asset=source,
    )
    assert failed["passed"] is False
    assert failed["checks"]["no_em_dash"] is False
    assert failed["checks"]["transcript_interval_valid"] is False


def test_transformation_covers_every_untransformed_episode() -> None:
    factory = _factory()
    with session_scope(factory) as session:
        for index in range(2):
            ingest_source_assets(
                session,
                episode_id=f"episode-{index}",
                actor="test",
                assets=[
                    {
                        "asset_id": f"transcript-{index}",
                        "asset_type": "transcript",
                        "title": f"Episode {index} operating lesson",
                        "status": "ready",
                        "transcript_text": (
                            "Operators need a trusted source before they make a "
                            "decision. The next action needs one owner. The result "
                            "must be verified after the change."
                        ),
                    }
                ],
            )
        run = record_orchestration_check(
            session,
            job_key="social_distribution",
            run_key="coverage-test",
            trigger="test",
            actor="test",
            blockers=[],
        )
        first = stage_native_candidates(session, run=run, actor="test")
        second = stage_native_candidates(session, run=run, actor="test")
        assert first == {"created": 12, "existing": 0, "rejected": 0}
        assert second == {"created": 0, "existing": 12, "rejected": 0}
        assert session.scalar(select(func.count()).select_from(ContentArtifact)) == 12


def _daily_payload(theme: str = "Inventory accuracy protects cash") -> dict:
    return {
        "theme": theme,
        "recording_time": "10:00 AM America/Denver",
        "cold_open": "Most inventory problems first appear as cash problems.",
        "news_items": [
            {
                "title": f"Signal {index}",
                "what_happened": "A documented market change occurred.",
                "why_it_matters": "Operators need to update their decision model.",
                "anata_angle": "Connect the signal to one operating mechanism.",
                "talking_point": "What should an operator change this week?",
            }
            for index in range(1, 4)
        ],
        "deep_dives": [
            {
                "title": "The inventory cash loop",
                "skill": "Read inventory as working capital.",
                "common_mistake": "Treating the warehouse report as complete truth.",
                "framework": "Observe, reconcile, decide, and verify.",
                "questions": "Where does uncertainty enter? How is it resolved?",
                "keyword": "inventory accuracy",
            }
        ],
        "source_urls": ["https://example.com/evidence"],
    }


def test_daily_brief_stages_four_outputs_and_blocks_topic_reuse() -> None:
    factory = _factory()
    now = datetime(2026, 7, 29, 15, 0, tzinfo=timezone.utc)
    with session_scope(factory) as session:
        first_run = ContentJobRun(
            id="daily-1",
            job_key="daily_brief",
            run_key="2026-07-29:daily_brief",
            status="ready",
            idempotency_key="daily-1-key",
        )
        session.add(first_run)
        session.commit()
        first = stage_daily_brief(
            session,
            run=first_run,
            actor="test",
            now=now,
            **_daily_payload(),
        )
        replay = stage_daily_brief(
            session,
            run=first_run,
            actor="test",
            now=now,
            **_daily_payload(),
        )
        second_run = ContentJobRun(
            id="daily-2",
            job_key="daily_brief",
            run_key="2026-07-30:daily_brief",
            status="ready",
            idempotency_key="daily-2-key",
        )
        session.add(second_run)
        session.commit()
        duplicate_topic = stage_daily_brief(
            session,
            run=second_run,
            actor="test",
            now=now,
            **_daily_payload(),
        )

        assert first == {"status": "needs_review", "errors": [], "created": 4}
        assert replay == {"status": "needs_review", "errors": [], "created": 0}
        assert duplicate_topic["status"] == "rejected"
        assert "This topic was used within the last 14 days." in duplicate_topic["errors"]
        artifacts = list(session.scalars(select(ContentArtifact)))
        assert {item.artifact_type for item in artifacts} == {
            "podcast_brief",
            "topic_file",
            "gmail_draft",
            "slack_notice",
        }
        assert all("\N{EM DASH}" not in item.body for item in artifacts)


def test_daily_brief_rejects_em_dash_before_any_artifact_is_created() -> None:
    factory = _factory()
    payload = _daily_payload("Inventory accuracy \N{EM DASH} protects cash")
    with session_scope(factory) as session:
        run = ContentJobRun(
            id="daily-rejected",
            job_key="daily_brief",
            run_key="2026-07-29:daily_brief",
            status="ready",
            idempotency_key="daily-rejected-key",
        )
        session.add(run)
        session.commit()
        result = stage_daily_brief(
            session,
            run=run,
            actor="test",
            **payload,
        )
        assert result["status"] == "rejected"
        assert session.scalar(
            select(func.count()).select_from(ContentArtifact)
        ) == 0
