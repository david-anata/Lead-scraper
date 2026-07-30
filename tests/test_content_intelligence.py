from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from sales_support_agent.models.content import (
    ContentArtifact,
    ContentPerformanceObservation,
    ContentPublication,
)
from sales_support_agent.models.database import Base
from sales_support_agent.services.content_intelligence import (
    normalized_performance_score,
    personal_cadence_state,
    rank_publishable_artifacts,
    record_performance_observation,
)


def _session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return Session(engine)


def test_score_renormalizes_missing_signals_instead_of_treating_them_as_zero() -> None:
    assert normalized_performance_score({"start": 0.8, "stay": 0.6}) == 0.6857
    assert normalized_performance_score({}) == 0.0


def test_observed_performance_ranks_strongest_source() -> None:
    session = _session()
    now = datetime.now(timezone.utc)
    for index, source in enumerate(("weak", "strong")):
        artifact = ContentArtifact(
            id=f"artifact-{source}",
            run_id=f"run-{source}",
            source_asset_id=source,
            artifact_type="native_candidate",
            channel="linkedin_personal",
            status="needs_review",
            title=source,
            body="Native personal lesson",
            content_fingerprint=source * 12,
            quality_gate_json={"passed": True, "six_cs": {"channel": True}},
            created_at=now - timedelta(hours=index),
            updated_at=now,
        )
        publication = ContentPublication(
            id=f"publication-{source}",
            run_id=f"old-run-{source}",
            source_asset_id=source,
            channel="linkedin_personal",
            destination="david",
            status="delivered",
            content_fingerprint=f"old-{source}",
            verified_at=now - timedelta(days=1),
        )
        session.add_all([artifact, publication])
    session.commit()
    record_performance_observation(
        session,
        publication_id="publication-weak",
        platform="linkedin_personal",
        format_name="text",
        objective="authority",
        window_started_at=now - timedelta(days=2),
        window_ended_at=now - timedelta(days=1),
        metrics={"start": 0.2, "stay": 0.2, "signal": 0.2},
        sample_confidence="high",
        actor="test",
    )
    record_performance_observation(
        session,
        publication_id="publication-strong",
        platform="linkedin_personal",
        format_name="text",
        objective="authority",
        window_started_at=now - timedelta(days=2),
        window_ended_at=now - timedelta(days=1),
        metrics={"start": 0.9, "stay": 0.9, "signal": 0.9},
        sample_confidence="high",
        actor="test",
    )
    ranked = rank_publishable_artifacts(
        session, channel="linkedin_personal", now=now
    )
    assert ranked[0][0].source_asset_id == "strong"
    assert session.query(ContentPerformanceObservation).count() == 2


def test_personal_cadence_enforces_two_to_three_verified_posts() -> None:
    session = _session()
    now = datetime(2026, 7, 29, 18, 0, tzinfo=timezone.utc)
    for index in range(2):
        session.add(
            ContentPublication(
                id=f"pub-{index}",
                run_id=f"run-{index}",
                channel="linkedin_personal",
                destination="david",
                status="delivered",
                content_fingerprint=f"fingerprint-{index}",
                verified_at=now - timedelta(days=index),
            )
        )
    session.commit()
    state = personal_cadence_state(session, now=now)
    assert state["on_track"] is True
    assert state["delivered"] == 2
    assert state["at_cap"] is False
