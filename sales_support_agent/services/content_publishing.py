"""Approved, audited publication of staged content through verified relays."""

from __future__ import annotations

import hashlib
import os
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

from sales_support_agent.integrations.content_relay import (
    ALLOWED_ACTIONS,
    ContentRelayClient,
)
from sales_support_agent.models.content import (
    ContentArtifact,
    ContentAuditEvent,
    ContentPublication,
)
from sales_support_agent.services.content_intelligence import (
    rank_publishable_artifacts,
)


CHANNEL_CONFIG: dict[str, dict[str, str]] = {
    "linkedin_company": {
        "action": ALLOWED_ACTIONS["linkedin_company"],
        "url": "CONTENT_LINKEDIN_CONNECTOR_URL",
        "key": "CONTENT_LINKEDIN_CONNECTOR_KEY",
        "verified": "CONTENT_LINKEDIN_CONNECTOR_VERIFIED",
        "destination": "CONTENT_LINKEDIN_COMPANY_ID",
        "activation": "CONTENT_LINKEDIN_COMPANY_LIVE_APPROVED",
        "auto": "CONTENT_LINKEDIN_COMPANY_AUTO_PUBLISH_ENABLED",
    },
    "linkedin_personal": {
        "action": ALLOWED_ACTIONS["linkedin_personal"],
        "url": "CONTENT_LINKEDIN_CONNECTOR_URL",
        "key": "CONTENT_LINKEDIN_CONNECTOR_KEY",
        "verified": "CONTENT_LINKEDIN_CONNECTOR_VERIFIED",
        "destination": "CONTENT_LINKEDIN_PERSON_ID",
        "activation": "CONTENT_LINKEDIN_PERSONAL_LIVE_APPROVED",
        "auto": "CONTENT_LINKEDIN_PERSONAL_AUTO_PUBLISH_ENABLED",
    },
    "google_business": {
        "action": ALLOWED_ACTIONS["google_business"],
        "url": "CONTENT_GOOGLE_BUSINESS_CONNECTOR_URL",
        "key": "CONTENT_GOOGLE_BUSINESS_CONNECTOR_KEY",
        "verified": "CONTENT_GOOGLE_BUSINESS_CONNECTOR_VERIFIED",
        "destination": "CONTENT_GOOGLE_BUSINESS_PROFILE_ID",
        "activation": "CONTENT_GOOGLE_BUSINESS_LIVE_APPROVED",
        "auto": "CONTENT_GOOGLE_BUSINESS_AUTO_PUBLISH_ENABLED",
    },
    "youtube": {
        "action": ALLOWED_ACTIONS["youtube_upload"],
        "url": "CONTENT_YOUTUBE_CONNECTOR_URL",
        "key": "CONTENT_YOUTUBE_CONNECTOR_KEY",
        "verified": "CONTENT_YOUTUBE_CONNECTOR_VERIFIED",
        "destination": "CONTENT_YOUTUBE_CHANNEL_ID",
        "activation": "CONTENT_YOUTUBE_LIVE_APPROVED",
        "auto": "CONTENT_YOUTUBE_AUTO_PUBLISH_ENABLED",
    },
    "instagram": {
        "action": ALLOWED_ACTIONS["instagram_video"],
        "url": "CONTENT_INSTAGRAM_CONNECTOR_URL",
        "key": "CONTENT_INSTAGRAM_CONNECTOR_KEY",
        "verified": "CONTENT_INSTAGRAM_CONNECTOR_VERIFIED",
        "destination": "CONTENT_INSTAGRAM_ACCOUNT_ID",
        "activation": "CONTENT_INSTAGRAM_LIVE_APPROVED",
        "auto": "CONTENT_INSTAGRAM_AUTO_PUBLISH_ENABLED",
    },
    "tiktok": {
        "action": ALLOWED_ACTIONS["tiktok_video"],
        "url": "CONTENT_TIKTOK_CONNECTOR_URL",
        "key": "CONTENT_TIKTOK_CONNECTOR_KEY",
        "verified": "CONTENT_TIKTOK_CONNECTOR_VERIFIED",
        "destination": "CONTENT_TIKTOK_ACCOUNT_ID",
        "activation": "CONTENT_TIKTOK_LIVE_APPROVED",
        "auto": "CONTENT_TIKTOK_AUTO_PUBLISH_ENABLED",
    },
}

WEEKLY_CAPS = {
    "linkedin_personal": 3,
    "linkedin_company": 7,
    "google_business": 7,
    "youtube": 7,
    "instagram": 7,
    "tiktok": 7,
}
VERTICAL_SYNDICATION_CHANNELS = (
    "tiktok",
    "instagram",
    "youtube",
    "linkedin_company",
    "google_business",
)
DAILY_PORTFOLIO = {day: VERTICAL_SYNDICATION_CHANNELS for day in range(7)}


def _enabled(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


def channel_publish_readiness(channel: str) -> dict[str, Any]:
    """Return non-secret, channel-specific readiness and the exact next action."""

    config = CHANNEL_CONFIG.get(channel)
    if config is None:
        return {
            "ready": False,
            "state": "staging_only",
            "message": "This destination is intentionally staging-only.",
        }
    missing = [
        label
        for label, name in (
            ("relay URL", config["url"]),
            ("relay key", config["key"]),
            ("destination identity", config["destination"]),
        )
        if not os.getenv(name, "").strip()
    ]
    if missing:
        return {
            "ready": False,
            "state": "not_connected",
            "message": f"Connect {', '.join(missing)}.",
        }
    if not _enabled(config["verified"]):
        return {
            "ready": False,
            "state": "needs_verification",
            "message": "Run a destination identity check, then mark this connector verified.",
        }
    if not _enabled(config["activation"]):
        return {
            "ready": False,
            "state": "needs_activation",
            "message": (
                "Destination identity is verified. Approve the first live "
                "activation before any public write."
            ),
        }
    if os.getenv("CONTENT_PUBLISHING_MODE", "shadow").strip().lower() != "live":
        return {
            "ready": False,
            "state": "shadow",
            "message": "Connection is configured; live publishing is still switched off.",
        }
    return {
        "ready": True,
        "state": "ready",
        "message": "Approved content can publish to this verified destination.",
    }


def publish_best_personal_candidate(
    session: Session,
    *,
    actor: str,
    now: datetime | None = None,
) -> ContentPublication | None:
    """Publish the strongest eligible personal candidate within the 2–3/week policy."""

    return publish_best_candidate(
        session,
        channel="linkedin_personal",
        actor=actor,
        now=now,
    )


def _weekly_verified_count(
    session: Session,
    *,
    channel: str,
    now: datetime,
) -> int:
    week_start = (now - timedelta(days=now.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    return len(
        list(
            session.scalars(
                select(ContentPublication.id).where(
                    ContentPublication.channel == channel,
                    ContentPublication.verified_at >= week_start,
                    ContentPublication.verified_at <= now,
                )
            )
        )
    )


def publish_best_candidate(
    session: Session,
    *,
    channel: str,
    actor: str,
    now: datetime | None = None,
) -> ContentPublication | None:
    """Select and publish one strongest eligible candidate within channel cadence."""

    current = now or datetime.now(timezone.utc)
    config = CHANNEL_CONFIG.get(channel)
    if config is None or not _enabled(config["auto"]):
        return None
    delivered = _weekly_verified_count(session, channel=channel, now=current)
    if delivered >= WEEKLY_CAPS[channel]:
        return None
    ranked = rank_publishable_artifacts(
        session,
        channel=channel,
        now=current,
    )
    if not ranked:
        return None
    artifact, score = ranked[0]
    artifact.status = "approved"
    gate = dict(artifact.quality_gate_json or {})
    gate["selection"] = {
        "score": score,
        "policy": "observed_performance_six_cs_freshness_v1",
        "selected_at": current.isoformat(),
    }
    artifact.quality_gate_json = gate
    session.add(
        ContentAuditEvent(
            id=uuid4().hex,
            run_id=artifact.run_id,
            actor_type="scheduler",
            actor_id=actor[:255],
            event_type="content_candidate_auto_selected",
            object_type="content_artifact",
            object_id=artifact.id,
            details_json={
                "channel": artifact.channel,
                "selection_score": score,
                "weekly_delivered_before": delivered,
            },
        )
    )
    session.commit()
    return publish_artifact(
        session,
        artifact_id=artifact.id,
        actor=actor,
        confirmed=True,
    )


def publish_daily_portfolio(
    session: Session,
    *,
    actor: str,
    now: datetime | None = None,
) -> dict[str, dict[str, Any]]:
    """Publish one native vertical-content set across every available destination."""

    current = now or datetime.now(timezone.utc)
    results: dict[str, dict[str, Any]] = {}
    eligible_channels: list[str] = []
    for channel in DAILY_PORTFOLIO[current.weekday()]:
        readiness = channel_publish_readiness(channel)
        if not readiness["ready"]:
            results[channel] = {
                "status": "blocked",
                "message": readiness["message"],
            }
            continue
        config = CHANNEL_CONFIG[channel]
        if not _enabled(config["auto"]):
            results[channel] = {
                "status": "not_enabled",
                "message": "This ready destination is not enabled for automatic publishing.",
            }
            continue
        if _weekly_verified_count(session, channel=channel, now=current) >= WEEKLY_CAPS[channel]:
            results[channel] = {
                "status": "not_eligible",
                "message": "This destination has reached its weekly cadence cap.",
            }
            continue
        eligible_channels.append(channel)

    candidates_by_source: dict[str, dict[str, tuple[ContentArtifact, float]]] = {}
    for channel in eligible_channels:
        for artifact, score in rank_publishable_artifacts(
            session, channel=channel, now=current
        ):
            candidates_by_source.setdefault(artifact.source_asset_id, {})[channel] = (
                artifact,
                score,
            )
    complete_sets = [
        rows
        for rows in candidates_by_source.values()
        if all(channel in rows for channel in eligible_channels)
    ]
    selected = max(
        complete_sets,
        key=lambda rows: sum(rows[channel][1] for channel in eligible_channels),
        default=None,
    )
    if selected is None:
        for channel in eligible_channels:
            results[channel] = {
                "status": "not_eligible",
                "message": "No complete native syndication set is ready for every available destination.",
            }
        return results

    for channel in eligible_channels:
        try:
            artifact, score = selected[channel]
            artifact.status = "approved"
            gate = dict(artifact.quality_gate_json or {})
            gate["selection"] = {
                "score": score,
                "policy": "daily_vertical_syndication_set_v1",
                "selected_at": current.isoformat(),
                "source_asset_id": artifact.source_asset_id,
            }
            artifact.quality_gate_json = gate
            session.commit()
            publication = publish_artifact(
                session, artifact_id=artifact.id, actor=actor, confirmed=True
            )
            results[channel] = {
                "status": publication.status,
                "publication_id": publication.id,
                "public_url": publication.public_url,
                "source_asset_id": artifact.source_asset_id,
            }
        except Exception as exc:
            session.rollback()
            session.add(
                ContentAuditEvent(
                    id=uuid4().hex,
                    actor_type="scheduler",
                    actor_id=actor[:255],
                    event_type="content_channel_publish_failed",
                    object_type="content_channel",
                    object_id=channel,
                    details_json={
                        "channel": channel,
                        "safe_error_type": type(exc).__name__,
                    },
                )
            )
            session.commit()
            results[channel] = {
                "status": "failed",
                "message": "This channel failed independently and can be retried.",
            }
    return results


def publish_artifact(
    session: Session,
    *,
    artifact_id: str,
    actor: str,
    confirmed: bool,
) -> ContentPublication:
    """Publish one reviewed artifact, failing closed before any external write."""

    artifact = session.get(ContentArtifact, artifact_id)
    if artifact is None:
        raise LookupError("Content artifact not found.")
    if not confirmed:
        raise ValueError("Explicit publication confirmation is required.")
    if artifact.artifact_type != "native_candidate":
        raise ValueError("Only native channel candidates can be published.")
    if artifact.status not in {"needs_review", "approved", "failed"}:
        raise ValueError("This artifact is not in a publishable review state.")
    if not bool((artifact.quality_gate_json or {}).get("passed")):
        raise ValueError("The content quality gate has not passed.")

    config = CHANNEL_CONFIG.get(artifact.channel)
    if config is None:
        raise ValueError("This channel is staging-only.")
    readiness = channel_publish_readiness(artifact.channel)
    if not readiness["ready"]:
        raise RuntimeError(str(readiness["message"]))

    destination = os.getenv(config["destination"], "").strip()
    existing = session.scalar(
        select(ContentPublication).where(
            ContentPublication.channel == artifact.channel,
            ContentPublication.destination == destination,
            ContentPublication.content_fingerprint == artifact.content_fingerprint,
        )
    )
    if existing is not None and existing.status in {
        "queued",
        "running",
        "delivered",
        "confirmed",
    }:
        return existing

    publication = existing or ContentPublication(
        id=uuid4().hex,
        run_id=artifact.run_id,
        source_asset_id=artifact.source_asset_id,
        channel=artifact.channel,
        destination=destination,
        playbook_version=artifact.playbook_version,
        status="queued",
        content_fingerprint=artifact.content_fingerprint,
        quality_gate_json=artifact.quality_gate_json,
    )
    if existing is None:
        session.add(publication)
    publication.attempt_count = int(publication.attempt_count or 0) + 1
    idempotency_key = hashlib.sha256(
        f"{publication.id}:{artifact.content_fingerprint}:{destination}".encode()
    ).hexdigest()
    client = ContentRelayClient(
        base_url=os.environ[config["url"]],
        api_key=os.environ[config["key"]],
    )
    media_assets = list((artifact.lineage_json or {}).get("media_assets") or [])
    media_url = ""
    if media_assets:
        vertical_media = sorted(
            media_assets,
            key=lambda item: (
                {"clip": 0, "video": 1}.get(str(item.get("asset_type") or ""), 2),
                str(item.get("source_asset_id") or ""),
            ),
        )[0]
        stable_media_url = str(vertical_media.get("source_url") or "")
        if stable_media_url and os.getenv("RIVERSIDE_API_KEY", "").strip():
            from sales_support_agent.integrations.riverside import RiversideClient

            media_url = RiversideClient(
                api_key=os.environ["RIVERSIDE_API_KEY"]
            ).resolve_download_url(stable_media_url)
    if artifact.channel in {"youtube", "instagram", "tiktok"} and not media_url:
        raise RuntimeError(
            "This video destination needs a retrievable Riverside media asset."
        )
    result = client.execute(
        action_key=config["action"],
        destination_identity=destination,
        idempotency_key=idempotency_key,
        payload={
            "artifact_id": artifact.id,
            "title": artifact.title,
            "body": artifact.body,
            "source_asset_id": artifact.source_asset_id,
            "lineage": artifact.lineage_json,
            "media_url": media_url,
            "playbook_version": artifact.playbook_version,
        },
        allow_write=True,
    )
    now = datetime.now(timezone.utc)
    publication.status = result.status
    publication.provider_receipt = result.provider_receipt
    publication.public_url = result.public_url
    if result.accepted and publication.published_at is None:
        publication.published_at = now
    if result.verified:
        publication.verified_at = now
    artifact.status = "delivered" if result.verified else result.status
    artifact.provider_object_id = result.provider_receipt
    artifact.external_url = result.public_url
    artifact.updated_at = now
    session.add(
        ContentAuditEvent(
            id=uuid4().hex,
            run_id=artifact.run_id,
            actor_type="operator",
            actor_id=actor[:255],
            event_type="content_publication_attempted",
            object_type="content_publication",
            object_id=publication.id,
            details_json={
                "artifact_id": artifact.id,
                "channel": artifact.channel,
                "status": result.status,
                "accepted": result.accepted,
                "verified": result.verified,
                "safe_message": result.safe_message,
            },
        )
    )
    session.commit()
    return publication
