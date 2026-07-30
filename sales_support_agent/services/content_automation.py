"""Durable, fail-closed orchestration for the Content & Growth Engine.

This module owns scheduling and production policy. Provider adapters remain
separate so a developer MCP session can never be mistaken for a production
publishing dependency.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable
from uuid import uuid4
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from sales_support_agent.models.content import (
    ContentArtifact,
    ContentAuditEvent,
    ContentChannelPlaybook,
    ContentDependencyCheck,
    ContentJobRun,
    ContentSourceAsset,
    ContentTranscript,
)
from sales_support_agent.models.database import session_scope
from sales_support_agent.services.content_engine import (
    control_room_data,
    dependency_health,
    record_orchestration_check,
)
from sales_support_agent.services.job_lease import (
    claim_scheduled_job,
    finish_scheduled_job,
)


DENVER = ZoneInfo("America/Denver")
EM_DASH = "\N{EM DASH}"


@dataclass(frozen=True)
class ContentJobDefinition:
    """One deterministic job contract in the content dependency graph."""

    key: str
    label: str
    required_dependencies: tuple[str, ...]
    upstream_jobs: tuple[str, ...] = ()
    schedule: str = "event"


JOB_DEFINITIONS: dict[str, ContentJobDefinition] = {
    "daily_brief": ContentJobDefinition(
        "daily_brief",
        "Daily signal and podcast brief",
        ("drive", "gmail", "slack"),
        schedule="daily_after_0600",
    ),
    "seo_blog": ContentJobDefinition(
        "seo_blog",
        "SEO blog",
        ("drive",),
        upstream_jobs=("daily_brief",),
        schedule="after_daily_brief",
    ),
    "episode_harvest": ContentJobDefinition(
        "episode_harvest",
        "Riverside episode harvest",
        ("riverside", "drive"),
        schedule="episode_ready",
    ),
    "social_distribution": ContentJobDefinition(
        "social_distribution",
        "Native social distribution",
        ("riverside",),
        upstream_jobs=("episode_harvest",),
        schedule="after_episode_harvest",
    ),
    "weekly_retrospective": ContentJobDefinition(
        "weekly_retrospective",
        "Weekly performance retrospective",
        ("youtube", "drive"),
        schedule="monday_after_0600",
    ),
    "newsletter_issue": ContentJobDefinition(
        "newsletter_issue",
        "Owned-audience issue",
        ("newsletter",),
        upstream_jobs=("weekly_retrospective",),
        schedule="approved_winner",
    ),
}


DEFAULT_PLAYBOOKS: tuple[dict[str, Any], ...] = (
    {
        "channel": "linkedin_personal",
        "version": "v1",
        "priority": "primary_authority",
        "cadence": {"max_per_week": 3, "minimum_spacing_hours": 36},
        "format": {
            "treatment": "first_person_operator_lesson",
            "cta_limit": 1,
            "cross_post_copy": False,
        },
        "quality": {
            "requires_original_experience": True,
            "requires_specific_takeaway": True,
            "prohibits_em_dash": True,
        },
        "metrics": {
            "primary": ["qualified_comments", "qualified_actions", "leads"],
            "minimum_sample": 3,
        },
    },
    {
        "channel": "linkedin_company",
        "version": "v1",
        "priority": "b2b_proof",
        "cadence": {"max_per_week": 2, "minimum_spacing_hours": 48},
        "format": {
            "treatment": "useful_operating_insight",
            "cta_limit": 1,
            "cross_post_copy": False,
        },
        "quality": {
            "requires_anata_evidence": True,
            "requires_specific_takeaway": True,
            "prohibits_em_dash": True,
        },
        "metrics": {
            "primary": ["engagement_rate", "qualified_visits", "leads"],
            "minimum_sample": 3,
        },
    },
    {
        "channel": "youtube",
        "version": "v1",
        "priority": "depth_and_search",
        "cadence": {
            "max_episodes_per_week": 1,
            "max_shorts_per_week": 3,
            "minimum_spacing_hours": 24,
        },
        "format": {
            "treatment": "story_led_education",
            "requires_thumbnail_contract": True,
            "cross_post_copy": False,
        },
        "quality": {
            "requires_source_lineage": True,
            "requires_clear_promise": True,
            "prohibits_em_dash": True,
        },
        "metrics": {
            "primary": ["ctr", "retention", "watch_time", "qualified_actions"],
            "minimum_sample": 3,
        },
    },
    {
        "channel": "instagram",
        "version": "v1",
        "priority": "discovery_and_relationship",
        "cadence": {"max_per_week": 3, "minimum_spacing_hours": 24},
        "format": {
            "treatment": "native_reel_or_carousel",
            "requires_visual_action": True,
            "cross_post_copy": False,
        },
        "quality": {
            "requires_source_lineage": True,
            "requires_native_caption": True,
            "prohibits_em_dash": True,
        },
        "metrics": {
            "primary": ["watch_percent", "shares", "saves", "qualified_actions"],
            "minimum_sample": 3,
        },
    },
    {
        "channel": "x",
        "version": "v1",
        "priority": "staging_only",
        "cadence": {"max_staged_per_week": 5, "minimum_spacing_hours": 12},
        "format": {
            "treatment": "sharp_opinion_or_question",
            "publishing_enabled": False,
            "cross_post_copy": False,
        },
        "quality": {
            "requires_original_point_of_view": True,
            "prohibits_em_dash": True,
        },
        "metrics": {"primary": ["conversation_quality"], "minimum_sample": 3},
    },
)


def seed_default_playbooks(session: Session) -> int:
    """Insert immutable v1 playbooks once and return the number created."""

    created = 0
    for item in DEFAULT_PLAYBOOKS:
        existing = session.scalar(
            select(ContentChannelPlaybook).where(
                ContentChannelPlaybook.channel == item["channel"],
                ContentChannelPlaybook.version == item["version"],
            )
        )
        if existing is not None:
            continue
        session.add(
            ContentChannelPlaybook(
                id=uuid4().hex,
                channel=item["channel"],
                version=item["version"],
                is_active=True,
                priority=item["priority"],
                cadence_json=item["cadence"],
                format_rules_json=item["format"],
                quality_rules_json=item["quality"],
                metric_rules_json=item["metrics"],
            )
        )
        created += 1
    if created:
        session.commit()
    return created


def record_dependency_snapshot(
    session: Session,
    dependencies: Iterable[dict[str, Any]],
    *,
    checked_at: datetime,
    ttl_minutes: int = 90,
) -> int:
    """Persist safe readiness evidence without credentials or provider payloads."""

    count = 0
    for item in dependencies:
        session.add(
            ContentDependencyCheck(
                id=uuid4().hex,
                dependency=str(item["key"])[:64],
                status=str(item["status"])[:32],
                message=str(item.get("message") or "")[:2000],
                evidence_json={"source": "production_preflight"},
                checked_at=checked_at,
                expires_at=checked_at + timedelta(minutes=max(5, ttl_minutes)),
            )
        )
        count += 1
    session.commit()
    return count


def _daily_run_key(now: datetime, job_key: str) -> str:
    local = now.astimezone(DENVER)
    return f"{local.date().isoformat()}:{job_key}"


def _weekly_run_key(now: datetime, job_key: str) -> str:
    local = now.astimezone(DENVER)
    year, week, _ = local.isocalendar()
    return f"{year}-W{week:02d}:{job_key}"


def due_scheduled_jobs(session: Session, *, now: datetime) -> list[tuple[str, str]]:
    """Return missed or currently due time-based jobs for restart catch-up."""

    local = now.astimezone(DENVER)
    if local.hour < 6:
        return []
    candidates = [("daily_brief", _daily_run_key(now, "daily_brief"))]
    if local.weekday() == 0:
        candidates.append(
            (
                "weekly_retrospective",
                _weekly_run_key(now, "weekly_retrospective"),
            )
        )
    due: list[tuple[str, str]] = []
    for job_key, run_key in candidates:
        exists = session.scalar(
            select(ContentJobRun.id).where(
                ContentJobRun.job_key == job_key,
                ContentJobRun.run_key == run_key,
            )
        )
        if not exists:
            due.append((job_key, run_key))
    return due


def quality_gate(
    *,
    channel: str,
    body: str,
    source_asset: ContentSourceAsset | None,
) -> dict[str, Any]:
    """Evaluate deterministic rules before a candidate can leave staging."""

    checks = {
        "no_em_dash": EM_DASH not in body,
        "has_content": bool(body.strip()),
        "native_channel": channel
        in {"linkedin_company", "linkedin_personal", "youtube", "instagram", "x"},
        "source_lineage": source_asset is not None,
        "transcript_interval_valid": bool(
            source_asset is not None
            and (
                source_asset.asset_type != "clip"
                or (
                    source_asset.transcript_start_ms is not None
                    and source_asset.transcript_end_ms is not None
                    and source_asset.transcript_end_ms
                    >= source_asset.transcript_start_ms
                )
            )
        ),
    }
    required = ["no_em_dash", "has_content", "native_channel", "source_lineage"]
    if source_asset is not None and source_asset.asset_type == "clip":
        required.append("transcript_interval_valid")
    return {
        "passed": all(checks[name] for name in required),
        "checks": checks,
        "required": required,
    }


def _normalize_topic(value: str) -> str:
    return " ".join(
        "".join(char.lower() if char.isalnum() else " " for char in value).split()
    )


def stage_daily_brief(
    session: Session,
    *,
    run: ContentJobRun,
    theme: str,
    recording_time: str,
    cold_open: str,
    news_items: list[dict[str, str]],
    deep_dives: list[dict[str, str]],
    source_urls: list[str],
    actor: str,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Validate and stage the four Job 1 outputs without external writes."""

    current = now or datetime.now(timezone.utc)
    clean_theme = " ".join(theme.split()).strip()
    topic_key = _normalize_topic(clean_theme)
    errors: list[str] = []
    if not clean_theme:
        errors.append("Theme is required.")
    if EM_DASH in " ".join(
        [
            clean_theme,
            recording_time,
            cold_open,
            *[str(item) for item in news_items],
            *[str(item) for item in deep_dives],
        ]
    ):
        errors.append("Generated copy must not contain em dashes.")
    if len(news_items) != 3:
        errors.append("Exactly three news items are required.")
    if not (1 <= len(deep_dives) <= 3):
        errors.append("One to three deep-dive options are required.")
    if not source_urls:
        errors.append("At least one evidence URL is required.")

    cutoff = current - timedelta(days=14)
    recent_topics = list(
        session.scalars(
            select(ContentArtifact).where(
                ContentArtifact.artifact_type == "topic_file",
                ContentArtifact.created_at >= cutoff,
            )
        )
    )
    if topic_key and any(
        _normalize_topic(item.title.removeprefix("Topic: ")) == topic_key
        for item in recent_topics
        if item.run_id != run.id
    ):
        errors.append("This topic was used within the last 14 days.")
    if errors:
        return {"status": "rejected", "errors": errors, "created": 0}

    news_markdown = "\n".join(
        (
            f"### {index}. {str(item.get('title') or '').strip()}\n"
            f"- What happened: {str(item.get('what_happened') or '').strip()}\n"
            f"- Why it matters: {str(item.get('why_it_matters') or '').strip()}\n"
            f"- Anata angle: {str(item.get('anata_angle') or '').strip()}\n"
            f"- Talking point: {str(item.get('talking_point') or '').strip()}"
        )
        for index, item in enumerate(news_items, start=1)
    )
    dives_markdown = "\n".join(
        (
            f"### {str(item.get('title') or '').strip()}\n"
            f"- The skill: {str(item.get('skill') or '').strip()}\n"
            f"- Common mistake: {str(item.get('common_mistake') or '').strip()}\n"
            f"- Anata framework: {str(item.get('framework') or '').strip()}\n"
            f"- Questions: {str(item.get('questions') or '').strip()}"
        )
        for item in deep_dives
    )
    sources_markdown = "\n".join(f"- {url}" for url in source_urls)
    brief_body = (
        f"# Anata Podcast Brief\n\n"
        f"## Episode Theme\n{clean_theme}\n\n"
        f"## Hosts\nDavid Narayan and Gabe\n\n"
        f"## Recording Time\n{recording_time.strip()}\n\n"
        f"## Cold Open Script\n{cold_open.strip()}\n\n"
        f"## News Rapid Fire\n{news_markdown}\n\n"
        f"## Deep Dive Options\n{dives_markdown}\n\n"
        f"## Best Clip Opportunities\n"
        f"Mark transcript intervals during the Riverside harvest.\n\n"
        f"## Sources\n{sources_markdown}\n"
    )
    primary_dive = deep_dives[0]
    topic_body = (
        f"Selected topic: {clean_theme}\n"
        f"Angle: {str(primary_dive.get('framework') or '').strip()}\n"
        f"Primary keyword: {str(primary_dive.get('keyword') or clean_theme).strip()}\n"
        f"Source section: Deep Dive 1\n"
        f"Run ID: {run.id}\n"
    )
    gmail_body = (
        f"Theme: {clean_theme}\n\n"
        "News:\n"
        + "\n".join(f"- {item.get('title', '').strip()}" for item in news_items)
        + "\n\nDeep dives:\n"
        + "\n".join(f"- {item.get('title', '').strip()}" for item in deep_dives)
        + f"\n\nRecording time: {recording_time.strip()}\n"
        + "Drive link: pending verified Drive write\n"
    )
    slack_body = (
        f"<!channel>\nPodcast brief staged: {clean_theme}\n"
        + "\n".join(f"• {item.get('title', '').strip()}" for item in news_items)
        + "\nDrive link: pending verified Drive write"
    )
    outputs = (
        ("podcast_brief", "drive", f"Podcast Brief: {clean_theme}", brief_body),
        ("topic_file", "drive", f"Topic: {clean_theme}", topic_body),
        (
            "gmail_draft",
            "gmail",
            f"Podcast Brief. {current.date().isoformat()}. {clean_theme}",
            gmail_body,
        ),
        ("slack_notice", "slack", f"Podcast brief: {clean_theme}", slack_body),
    )
    created = 0
    for artifact_type, channel, title, body in outputs:
        fingerprint = hashlib.sha256(
            f"{run.run_key}:{artifact_type}:{body}".encode("utf-8")
        ).hexdigest()
        existing = session.scalar(
            select(ContentArtifact).where(
                ContentArtifact.run_id == run.id,
                ContentArtifact.artifact_type == artifact_type,
                ContentArtifact.channel == channel,
                ContentArtifact.content_fingerprint == fingerprint,
            )
        )
        if existing is not None:
            continue
        artifact = ContentArtifact(
            id=uuid4().hex,
            run_id=run.id,
            artifact_type=artifact_type,
            channel=channel,
            status="needs_review",
            title=title[:500],
            body=body,
            content_fingerprint=fingerprint,
            lineage_json={
                "source_urls": source_urls,
                "topic_key": topic_key,
                "run_id": run.id,
            },
            quality_gate_json={
                "passed": True,
                "checks": {
                    "no_em_dash": True,
                    "three_news_items": True,
                    "has_deep_dive": True,
                    "has_sources": True,
                    "topic_not_used_in_14_days": True,
                },
            },
        )
        session.add(artifact)
        session.add(
            ContentAuditEvent(
                id=uuid4().hex,
                run_id=run.id,
                actor_type="connector",
                actor_id=actor[:255],
                event_type="daily_brief_artifact_staged",
                object_type="content_artifact",
                object_id=artifact.id,
                details_json={
                    "artifact_type": artifact_type,
                    "channel": channel,
                    "content_fingerprint": fingerprint,
                },
            )
        )
        created += 1
    run.status = "needs_review"
    run.safe_error_message = (
        "Daily brief outputs are staged. No Drive, Gmail, or Slack write was attempted."
    )
    summary = dict(run.summary_json or {})
    summary["staged_outputs"] = created
    summary["topic_key"] = topic_key
    summary["execution_mode"] = "shadow"
    run.summary_json = summary
    run.updated_at = current
    session.commit()
    return {"status": "needs_review", "errors": [], "created": created}


def _candidate_body(
    channel: str,
    asset: ContentSourceAsset,
    transcript_excerpt: str,
) -> str:
    topic = asset.title.strip() or "this operator lesson"
    evidence = " ".join(transcript_excerpt.split())[:240].replace(EM_DASH, "-")
    evidence_note = (
        f' Source evidence begins: "{evidence}"'
        if evidence
        else " Source transcript must be attached before approval."
    )
    treatments = {
        "linkedin_company": (
            f"Build an evidence-led operating insight from {topic}. "
            "Name the business problem, the Anata framework, and one useful next step."
        ),
        "youtube": (
            f"Build a clear search promise around {topic}. "
            "Open with the result, teach the mechanism, and preserve the source story."
        ),
        "instagram": (
            f"Build a native visual treatment from {topic}. "
            "Lead with visible action, one specific lesson, and a save-worthy takeaway."
        ),
        "x": (
            f"Stage one sharp, defensible point of view from {topic}. "
            "Use one idea and one conversation-opening question."
        ),
    }
    return treatments[channel] + evidence_note


def stage_native_candidates(
    session: Session,
    *,
    run: ContentJobRun,
    actor: str,
) -> dict[str, int]:
    """Create channel-specific candidate briefs without external writes."""

    source = session.scalar(
        select(ContentSourceAsset)
        .where(ContentSourceAsset.processing_status == "ready")
        .order_by(ContentSourceAsset.ingested_at.desc())
    )
    if source is None:
        return {"created": 0, "existing": 0, "rejected": 0}
    transcript = session.scalar(
        select(ContentTranscript)
        .where(ContentTranscript.source_asset_id == source.id)
        .order_by(ContentTranscript.created_at.desc())
    )
    transcript_excerpt = transcript.text if transcript is not None else ""

    created = 0
    existing = 0
    rejected = 0
    for channel in ("linkedin_company", "youtube", "instagram", "x"):
        body = _candidate_body(channel, source, transcript_excerpt)
        gate = quality_gate(channel=channel, body=body, source_asset=source)
        fingerprint = hashlib.sha256(
            f"{source.source_fingerprint}:{channel}:v1:{body}".encode("utf-8")
        ).hexdigest()
        duplicate = session.scalar(
            select(ContentArtifact).where(
                ContentArtifact.run_id == run.id,
                ContentArtifact.artifact_type == "native_candidate",
                ContentArtifact.channel == channel,
                ContentArtifact.content_fingerprint == fingerprint,
            )
        )
        if duplicate is not None:
            existing += 1
            continue
        status = "needs_review" if gate["passed"] else "failed"
        if not gate["passed"]:
            rejected += 1
        artifact = ContentArtifact(
            id=uuid4().hex,
            run_id=run.id,
            source_asset_id=source.id,
            artifact_type="native_candidate",
            channel=channel,
            playbook_version="v1",
            status=status,
            title=f"{source.title or 'Riverside source'}: {channel.replace('_', ' ')}",
            body=body,
            content_fingerprint=fingerprint,
            lineage_json={
                "provider": source.provider,
                "episode_external_id": source.episode_external_id,
                "source_asset_id": source.id,
                "source_fingerprint": source.source_fingerprint,
                "transcript_start_ms": source.transcript_start_ms,
                "transcript_end_ms": source.transcript_end_ms,
            },
            quality_gate_json=gate,
        )
        session.add(artifact)
        session.add(
            ContentAuditEvent(
                id=uuid4().hex,
                run_id=run.id,
                actor_type="scheduler",
                actor_id=actor,
                event_type="native_candidate_staged",
                object_type="content_artifact",
                object_id=artifact.id,
                details_json={
                    "channel": channel,
                    "playbook_version": "v1",
                    "source_asset_id": source.id,
                    "quality_passed": gate["passed"],
                },
            )
        )
        created += 1
    session.commit()
    return {"created": created, "existing": existing, "rejected": rejected}


def _job_blockers(
    definition: ContentJobDefinition,
    state_by_key: dict[str, str],
) -> list[str]:
    return [
        key
        for key in definition.required_dependencies
        if state_by_key.get(key) != "ready"
    ]


def run_content_cycle(
    session_factory: sessionmaker[Session],
    settings: Any,
    *,
    mode: str,
    force: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Claim, preflight, and safely stage due jobs across Render instances."""

    current = now or datetime.now(timezone.utc)
    engine = session_factory.kw.get("bind")
    if engine is None:
        raise RuntimeError("Content scheduler requires a bound database engine.")

    with session_scope(session_factory) as session:
        seed_default_playbooks(session)
        source_count = int(control_room_data(session, settings)["source_asset_count"])
        health = dependency_health(settings, source_asset_count=source_count)
        record_dependency_snapshot(session, health, checked_at=current)
        if mode == "scheduled":
            jobs = due_scheduled_jobs(session, now=current)
        else:
            if mode not in JOB_DEFINITIONS:
                raise ValueError(f"Unknown content job: {mode}")
            stable_key = (
                _weekly_run_key(current, mode)
                if mode == "weekly_retrospective"
                else _daily_run_key(current, mode)
            )
            jobs = [
                (
                    mode,
                    f"{current.astimezone(DENVER).isoformat()}:{mode}"
                    if force
                    else stable_key,
                )
            ]

    if not jobs:
        return {
            "status": "not_due",
            "message": "No scheduled content job is due. Catch-up remains active.",
            "details": {},
        }

    state_by_key = {item["key"]: item["status"] for item in health}
    results: dict[str, dict[str, Any]] = {}
    for job_key, run_key in jobs:
        lease = claim_scheduled_job(
            engine,
            job_key=f"content:{job_key}",
            run_key=run_key,
            lease_minutes=90,
        )
        if lease is None:
            results[job_key] = {
                "status": "duplicate",
                "run_key": run_key,
                "message": "This logical run is already owned or complete.",
            }
            continue

        definition = JOB_DEFINITIONS[job_key]
        blockers = _job_blockers(definition, state_by_key)
        try:
            with session_scope(session_factory) as session:
                row = record_orchestration_check(
                    session,
                    job_key=job_key,
                    run_key=run_key,
                    trigger="scheduled" if mode == "scheduled" else "internal",
                    actor="job:content-orchestrator",
                    blockers=blockers,
                )
                staged = (
                    stage_native_candidates(
                        session,
                        run=row,
                        actor="job:content-orchestrator",
                    )
                    if job_key == "social_distribution"
                    else {"created": 0, "existing": 0, "rejected": 0}
                )
                if (
                    job_key == "social_distribution"
                    and not blockers
                    and staged["created"] > 0
                ):
                    row.status = "needs_review"
                    row.safe_error_message = (
                        "Native candidates are staged for review. "
                        "No destination write was attempted."
                    )
                summary = dict(row.summary_json or {})
                summary.update(
                    {
                        "schedule": definition.schedule,
                        "upstream_jobs": list(definition.upstream_jobs),
                        "staged_candidates": staged,
                        "execution_mode": "shadow",
                    }
                )
                row.summary_json = summary
                row.updated_at = current
                session.commit()
                results[job_key] = {
                    "run_id": row.id,
                    "run_key": run_key,
                    "status": row.status,
                    "blockers": blockers,
                    "staged_candidates": staged,
                }
            finish_scheduled_job(
                engine,
                lease,
                status="succeeded",
                details={
                    "content_status": results[job_key]["status"],
                    "blockers": blockers,
                },
            )
        except Exception as exc:
            finish_scheduled_job(
                engine,
                lease,
                status="failed",
                details={"error_type": type(exc).__name__},
            )
            raise

    states = {item["status"] for item in results.values()}
    return {
        "status": (
            "blocked"
            if "blocked" in states
            else (
                "needs_review"
                if "needs_review" in states
                else ("duplicate" if states == {"duplicate"} else "ready")
            )
        ),
        "details": results,
    }
