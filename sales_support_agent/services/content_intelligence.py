"""Performance-led selection for channel-native content."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

from sales_support_agent.models.content import (
    ContentArtifact,
    ContentAuditEvent,
    ContentPerformanceObservation,
    ContentPublication,
)


SCORE_FIELDS = (
    "start",
    "stay",
    "signal",
    "business_impact",
    "credibility",
    "category_fit",
)
SCORE_WEIGHTS = {
    "start": 0.15,
    "stay": 0.20,
    "signal": 0.20,
    "business_impact": 0.25,
    "credibility": 0.10,
    "category_fit": 0.10,
}


def normalized_performance_score(metrics: dict[str, Any]) -> float:
    """Score comparable normalized signals without treating missing data as zero."""

    available = {
        key: max(0.0, min(1.0, float(metrics[key])))
        for key in SCORE_FIELDS
        if metrics.get(key) is not None
    }
    if not available:
        return 0.0
    weight = sum(SCORE_WEIGHTS[key] for key in available)
    return round(
        sum(available[key] * SCORE_WEIGHTS[key] for key in available) / weight,
        4,
    )


def record_performance_observation(
    session: Session,
    *,
    publication_id: str,
    platform: str,
    format_name: str,
    objective: str,
    window_started_at: datetime,
    window_ended_at: datetime,
    metrics: dict[str, Any],
    sample_confidence: str,
    actor: str,
) -> ContentPerformanceObservation:
    """Persist one provider observation and its computed calibration score."""

    publication = session.get(ContentPublication, publication_id)
    if publication is None:
        raise LookupError("Content publication not found.")
    if platform != publication.channel:
        raise ValueError("Performance platform must match the publication channel.")
    if window_ended_at <= window_started_at:
        raise ValueError("Performance observation window is invalid.")
    cleaned = {key: metrics.get(key) for key in SCORE_FIELDS if key in metrics}
    cleaned["selection_score"] = normalized_performance_score(cleaned)
    row = ContentPerformanceObservation(
        id=uuid4().hex,
        publication_id=publication.id,
        platform=platform,
        format=format_name,
        objective=objective,
        window_started_at=window_started_at,
        window_ended_at=window_ended_at,
        metrics_json=cleaned,
        data_availability="available" if cleaned else "unavailable",
        sample_confidence=sample_confidence,
    )
    session.add(row)
    session.add(
        ContentAuditEvent(
            id=uuid4().hex,
            run_id=publication.run_id,
            actor_type="provider",
            actor_id=actor[:255],
            event_type="content_performance_observed",
            object_type="content_publication",
            object_id=publication.id,
            details_json={
                "platform": platform,
                "selection_score": cleaned["selection_score"],
                "sample_confidence": sample_confidence,
            },
        )
    )
    session.commit()
    return row


def rank_publishable_artifacts(
    session: Session,
    *,
    channel: str,
    now: datetime | None = None,
) -> list[tuple[ContentArtifact, float]]:
    """Rank eligible originals using Six C quality plus observed source performance."""

    current = now or datetime.now(timezone.utc)
    candidates = list(
        session.scalars(
            select(ContentArtifact).where(
                ContentArtifact.channel == channel,
                ContentArtifact.artifact_type == "native_candidate",
                ContentArtifact.status.in_(("needs_review", "approved", "failed")),
            )
        )
    )
    ranked: list[tuple[ContentArtifact, float]] = []
    for artifact in candidates:
        observations = list(
            session.scalars(
                select(ContentPerformanceObservation)
                .join(
                    ContentPublication,
                    ContentPublication.id
                    == ContentPerformanceObservation.publication_id,
                )
                .where(
                    ContentPublication.source_asset_id == artifact.source_asset_id,
                    ContentPerformanceObservation.platform == channel,
                    ContentPerformanceObservation.data_availability == "available",
                )
            )
        )
        observed = max(
            (
                float((item.metrics_json or {}).get("selection_score") or 0.0)
                for item in observations
                if item.sample_confidence in {"medium", "high"}
            ),
            default=0.0,
        )
        gate = artifact.quality_gate_json or {}
        six_cs = gate.get("six_cs") or {}
        strategy = (
            sum(1 for value in six_cs.values() if bool(value)) / len(six_cs)
            if six_cs
            else (1.0 if gate.get("passed") else 0.0)
        )
        created_at = artifact.created_at
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=timezone.utc)
        age_days = max(0.0, (current - created_at).total_seconds() / 86400)
        freshness = max(0.0, 1.0 - min(age_days, 30.0) / 30.0)
        score = round((observed * 0.55) + (strategy * 0.35) + (freshness * 0.10), 4)
        ranked.append((artifact, score))
    return sorted(ranked, key=lambda item: (item[1], item[0].created_at), reverse=True)


def personal_cadence_state(
    session: Session,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return the mandatory 2–3/week personal LinkedIn delivery state."""

    current = now or datetime.now(timezone.utc)
    local_start = (current - timedelta(days=current.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    delivered = list(
        session.scalars(
            select(ContentPublication).where(
                ContentPublication.channel == "linkedin_personal",
                ContentPublication.verified_at >= local_start,
                ContentPublication.verified_at <= current,
            )
        )
    )
    return {
        "delivered": len(delivered),
        "minimum": 2,
        "maximum": 3,
        "on_track": 2 <= len(delivered) <= 3,
        "remaining_to_minimum": max(0, 2 - len(delivered)),
        "at_cap": len(delivered) >= 3,
    }
