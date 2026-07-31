"""Deterministic queueing, scheduling, and connector orchestration for content."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from typing import Any
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4
from zoneinfo import ZoneInfo

import requests
from sqlalchemy import select
from sqlalchemy.orm import Session

from sales_support_agent.models.content import (
    ContentAuditEvent,
    ContentChannelVariant,
    ContentPersonalDraft,
    ContentQueueItem,
)


DENVER = ZoneInfo("America/Denver")
AUTOMATED_CHANNELS = ("tiktok", "instagram", "youtube_shorts", "linkedin_company", "google_business")
CHANNEL_WINDOWS = {
    "tiktok": time(12, 15),
    "instagram": time(10, 30),
    "youtube_shorts": time(12, 0),
    "linkedin_company": time(9, 10),
    "google_business": time(10, 0),
}
CHANNEL_DESTINATIONS = {
    "tiktok": "David | Anata Inc.",
    "instagram": "anatainc",
    "youtube_shorts": "anata inc.",
    "linkedin_company": "anata",
    "google_business": "anata inc. · Lehi",
}
CHANNEL_PROVIDERS = {
    "tiktok": "riverside",
    "instagram": "riverside",
    "youtube_shorts": "riverside",
    "linkedin_company": "buffer",
    "google_business": "buffer",
}


def _safe_url(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    parsed = urlsplit(raw)
    if parsed.scheme not in {"http", "https"}:
        return ""
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))


def _fingerprint(*parts: str) -> str:
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def _next_weekday(base: datetime, offset: int) -> datetime:
    cursor = base.astimezone(DENVER).replace(hour=0, minute=0, second=0, microsecond=0)
    if cursor.weekday() >= 5:
        cursor += timedelta(days=7 - cursor.weekday())
    count = 0
    while count < offset:
        cursor += timedelta(days=1)
        if cursor.weekday() < 5:
            count += 1
    return cursor


def _scheduled_at(base: datetime, index: int, channel: str) -> datetime:
    """Create a compact two-week cadence without weekend video gaps."""

    local_day = _next_weekday(base, index)
    if channel == "linkedin_company":
        # Three company posts each week. Lower-ranked clips remain active on
        # short-form destinations instead of being discarded.
        local_day = _next_weekday(base, (index // 3) * 5 + (index % 3) * 2)
    elif channel == "google_business":
        local_day = _next_weekday(base, (index // 3) * 5 + min((index % 3) * 2 + 1, 4))
    local = datetime.combine(local_day.date(), CHANNEL_WINDOWS[channel], tzinfo=DENVER)
    return local.astimezone(timezone.utc)


def ingest_episode_package(
    session: Session,
    *,
    episode_id: str,
    episode_title: str,
    episode_url: str,
    clips: list[dict[str, Any]],
    actor: str,
    schedule_from: datetime | None = None,
) -> dict[str, int]:
    """Idempotently create queue items and native destination variants."""

    base = schedule_from or datetime.now(timezone.utc)
    created_items = 0
    created_variants = 0
    for index, raw in enumerate(clips):
        external_id = str(raw.get("asset_id") or raw.get("id") or "").strip()
        title = str(raw.get("title") or "Untitled clip").strip()
        if not external_id:
            continue
        item = session.scalar(
            select(ContentQueueItem).where(
                ContentQueueItem.episode_external_id == episode_id,
                ContentQueueItem.source_external_id == external_id,
            )
        )
        if item is None:
            item = ContentQueueItem(
                id=uuid4().hex,
                episode_external_id=episode_id,
                source_external_id=external_id,
                title=title,
                duration_ms=max(0, int(raw.get("duration_ms") or 0)),
                source_url=_safe_url(str(raw.get("source_url") or episode_url)),
                preview_url=_safe_url(str(raw.get("preview_url") or raw.get("source_url") or episode_url)),
                transcript_excerpt=str(raw.get("transcript_excerpt") or "")[:8000],
                rank=max(1, int(raw.get("rank") or index + 1)),
                six_c_json=dict(raw.get("six_c") or {}),
                status="queued",
                recycle_eligible=bool(raw.get("recycle_eligible", True)),
            )
            session.add(item)
            session.flush()
            created_items += 1

        channel_copy = dict(raw.get("channel_copy") or {})
        for channel in AUTOMATED_CHANNELS:
            if channel in {"linkedin_company", "google_business"} and index >= 6:
                continue
            variant = session.scalar(
                select(ContentChannelVariant).where(
                    ContentChannelVariant.queue_item_id == item.id,
                    ContentChannelVariant.channel == channel,
                    ContentChannelVariant.cycle_key == "launch",
                )
            )
            if variant is not None:
                continue
            copy_value = channel_copy.get(channel) or {}
            if isinstance(copy_value, str):
                copy_value = {"copy": copy_value}
            scheduled_for = _scheduled_at(base, index, channel)
            variant = ContentChannelVariant(
                id=uuid4().hex,
                queue_item_id=item.id,
                channel=channel,
                destination=CHANNEL_DESTINATIONS[channel],
                provider=CHANNEL_PROVIDERS[channel],
                title=str(copy_value.get("title") or title)[:500],
                copy_text=str(copy_value.get("copy") or raw.get("default_copy") or "")[:10000],
                status="queued",
                scheduled_for=scheduled_for,
                manual_only=False,
                idempotency_key=_fingerprint(episode_id, external_id, channel, "launch"),
                metadata_json={
                    "episode_title": episode_title,
                    "aspect_ratio": str(raw.get("aspect_ratio") or "9:16"),
                    "media_url": _safe_url(str(raw.get("media_url") or "")),
                },
            )
            session.add(variant)
            created_variants += 1

        session.add(
            ContentAuditEvent(
                id=uuid4().hex,
                actor_type="relay",
                actor_id=actor,
                event_type="queue_item_ingested",
                object_type="content_queue_item",
                object_id=item.id,
                details_json={"episode_id": episode_id, "source_external_id": external_id},
            )
        )
    session.flush()
    return {"queue_items_created": created_items, "variants_created": created_variants}


def queue_workspace(session: Session) -> dict[str, Any]:
    items = list(
        session.scalars(
            select(ContentQueueItem).order_by(ContentQueueItem.rank, ContentQueueItem.created_at.desc())
        )
    )
    variants = list(
        session.scalars(
            select(ContentChannelVariant).order_by(
                ContentChannelVariant.scheduled_for.asc(),
                ContentChannelVariant.created_at.asc(),
            )
        )
    )
    drafts = list(
        session.scalars(
            select(ContentPersonalDraft).order_by(
                ContentPersonalDraft.suggested_for.asc(),
                ContentPersonalDraft.created_at.desc(),
            )
        )
    )
    by_item: dict[str, list[ContentChannelVariant]] = {}
    for variant in variants:
        by_item.setdefault(variant.queue_item_id, []).append(variant)
    return {
        "items": items,
        "variants": variants,
        "drafts": drafts,
        "variants_by_item": by_item,
        "counts": {
            "items": len(items),
            "scheduled": sum(v.status in {"queued", "scheduled"} for v in variants),
            "live": sum(v.status in {"published", "verified"} for v in variants),
            "attention": sum(v.status in {"failed", "blocked"} for v in variants),
            "manual": sum(d.status in {"draft", "ready"} for d in drafts),
        },
    }


def update_variant_state(
    session: Session,
    *,
    variant_id: str,
    action: str,
    actor: str,
    scheduled_for: datetime | None = None,
) -> ContentChannelVariant:
    variant = session.get(ContentChannelVariant, variant_id)
    if variant is None:
        raise LookupError("Content destination was not found.")
    allowed = {
        "pause": "paused",
        "resume": "queued",
        "skip": "skipped",
        "retry": "queued",
    }
    if action == "reschedule":
        if scheduled_for is None:
            raise ValueError("A new scheduled time is required.")
        variant.scheduled_for = scheduled_for
        variant.status = "queued"
    elif action in allowed:
        variant.status = allowed[action]
        if action == "retry":
            variant.safe_error_message = ""
    else:
        raise ValueError("Unsupported queue action.")
    variant.updated_at = datetime.now(timezone.utc)
    session.add(
        ContentAuditEvent(
            id=uuid4().hex,
            actor_type="operator",
            actor_id=actor,
            event_type=f"variant_{action}",
            object_type="content_channel_variant",
            object_id=variant.id,
            details_json={"channel": variant.channel, "scheduled_for": variant.scheduled_for.isoformat() if variant.scheduled_for else None},
        )
    )
    session.flush()
    return variant


@dataclass(frozen=True)
class ConnectorResult:
    accepted: bool
    receipt: str = ""
    public_url: str = ""
    safe_error: str = ""


class WebhookPublisher:
    """Small authenticated JSON adapter suitable for Zapier or provider relays."""

    def __init__(self, url: str, key: str, *, timeout_seconds: int = 20) -> None:
        self.url = url.strip()
        self.key = key.strip()
        self.timeout_seconds = timeout_seconds

    def publish(self, payload: dict[str, Any], idempotency_key: str) -> ConnectorResult:
        try:
            response = requests.post(
                self.url,
                json=payload,
                headers={
                    "Authorization": f"Bearer {self.key}",
                    "Idempotency-Key": idempotency_key,
                    "Content-Type": "application/json",
                },
                timeout=self.timeout_seconds,
            )
        except requests.RequestException:
            return ConnectorResult(False, safe_error="The publishing connector could not be reached.")
        if response.status_code >= 400:
            return ConnectorResult(False, safe_error=f"The publishing connector rejected the request ({response.status_code}).")
        try:
            body = response.json()
        except ValueError:
            body = {}
        return ConnectorResult(
            True,
            receipt=str(body.get("receipt") or body.get("id") or response.headers.get("X-Request-Id") or ""),
            public_url=_safe_url(str(body.get("public_url") or body.get("url") or "")),
        )


def _connector_for(channel: str) -> WebhookPublisher | None:
    prefix = {
        "linkedin_company": "CONTENT_LINKEDIN_COMPANY",
        "google_business": "CONTENT_GOOGLE_BUSINESS",
        "tiktok": "CONTENT_TIKTOK",
        "instagram": "CONTENT_INSTAGRAM",
        "youtube_shorts": "CONTENT_YOUTUBE_SHORTS",
    }.get(channel)
    if not prefix:
        return None
    url = os.getenv(f"{prefix}_CONNECTOR_URL", "").strip()
    key = os.getenv(f"{prefix}_CONNECTOR_KEY", "").strip()
    if channel == "linkedin_company" and not (url and key):
        url = os.getenv("CONTENT_LINKEDIN_CONNECTOR_URL", "").strip()
        key = os.getenv("CONTENT_LINKEDIN_CONNECTOR_KEY", "").strip()
    if channel == "youtube_shorts" and not (url and key):
        url = os.getenv("CONTENT_YOUTUBE_CONNECTOR_URL", "").strip()
        key = os.getenv("CONTENT_YOUTUBE_CONNECTOR_KEY", "").strip()
    return WebhookPublisher(url, key) if url and key else None


def dispatch_due_variants(session: Session, *, now: datetime | None = None) -> dict[str, int]:
    """Publish each due destination independently and preserve partial success."""

    current = now or datetime.now(timezone.utc)
    due = list(
        session.scalars(
            select(ContentChannelVariant).where(
                ContentChannelVariant.status.in_(("queued", "scheduled")),
                ContentChannelVariant.scheduled_for.is_not(None),
                ContentChannelVariant.scheduled_for <= current,
            )
        )
    )
    counts = {"attempted": 0, "accepted": 0, "blocked": 0, "failed": 0}
    for variant in due:
        item = session.get(ContentQueueItem, variant.queue_item_id)
        connector = _connector_for(variant.channel)
        if connector is None:
            variant.status = "blocked"
            variant.safe_error_message = "This destination does not have a verified production connector."
            counts["blocked"] += 1
            continue
        media_url = str((variant.metadata_json or {}).get("media_url") or "")
        if variant.channel != "google_business" and not media_url:
            variant.status = "blocked"
            variant.safe_error_message = "Riverside has not supplied a durable media URL."
            counts["blocked"] += 1
            continue
        counts["attempted"] += 1
        variant.attempt_count += 1
        result = connector.publish(
            {
                "channel": variant.channel,
                "destination": variant.destination,
                "title": variant.title,
                "copy": variant.copy_text,
                "scheduled_for": variant.scheduled_for.isoformat() if variant.scheduled_for else None,
                "media_url": media_url,
                "source_url": item.source_url if item else "",
                "queue_item_id": variant.queue_item_id,
            },
            variant.idempotency_key,
        )
        if result.accepted:
            variant.status = "accepted"
            variant.provider_receipt = result.receipt
            variant.public_url = result.public_url
            variant.safe_error_message = ""
            counts["accepted"] += 1
        else:
            variant.status = "failed"
            variant.safe_error_message = result.safe_error
            counts["failed"] += 1
        variant.updated_at = current
        session.add(
            ContentAuditEvent(
                id=uuid4().hex,
                actor_type="scheduler",
                actor_id="job:content-dispatch",
                event_type="variant_dispatch_attempted",
                object_type="content_channel_variant",
                object_id=variant.id,
                details_json={"channel": variant.channel, "accepted": result.accepted, "attempt": variant.attempt_count},
            )
        )
    session.flush()
    return counts


def poll_relay_payload() -> list[dict[str, Any]]:
    """Read normalized episode packages from an authenticated Riverside relay."""

    url = os.getenv("CONTENT_RIVERSIDE_RELAY_URL", "").strip()
    key = os.getenv("CONTENT_RIVERSIDE_RELAY_KEY", "").strip()
    if not url or not key:
        return []
    try:
        response = requests.get(
            url,
            headers={"Authorization": f"Bearer {key}", "Accept": "application/json"},
            timeout=30,
        )
        response.raise_for_status()
        body = response.json()
    except (requests.RequestException, ValueError):
        return []
    episodes = body.get("episodes") if isinstance(body, dict) else body
    return [item for item in (episodes or []) if isinstance(item, dict)]
