"""Live call sessions: signed tokens, session lifecycle, transcript lines.

Tape's server streams the call to Deepgram and posts finished lines here with
the live token as a Bearer credential. The agent signs and verifies the token
itself, so Tape never needs a shared secret.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from sales_support_agent.models.entities import (
    SalesCallCoachEvent,
    SalesCallLiveSession,
    SalesCallTracker,
    SalesCallTranscriptLine,
)

TOKEN_VERSION = "v1"
TOKEN_TTL_SECONDS = 4 * 3600
RELAY_URL_DEFAULT = "https://tape.anatainc.com"
MAX_CONTEXT_CHARS = 120_000
MAX_LINES_PER_POST = 100
MAX_LINE_CHARS = 2000
MAX_LINES_PER_STATE = 200
SPEAKERS = ("prospect", "rep")

STATUS_PENDING = "pending"
STATUS_LIVE = "live"
STATUS_ENDED = "ended"

KEYTERMS = (
    "ACoS", "TACoS", "ROAS", "ASIN", "Seller Central", "Sponsored Products",
    "Sponsored Brands", "Sponsored Display", "Buy Box", "Anata", "Anata Intelligence",
    "Intelligence", "TikTok Shop", "Shopify", "SKU", "PPC", "CTR", "CVR",
    "Subscribe and Save", "A+ content", "Brand Registry",
)


class LiveError(ValueError):
    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.status = status


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def aware(dt: Optional[datetime]) -> Optional[datetime]:
    """SQLite hands back naive datetimes; treat them as UTC."""
    if dt is None:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def iso(dt: Optional[datetime]) -> Optional[str]:
    dt = aware(dt)
    return dt.isoformat() if dt else None


# ---------------------------------------------------------------- tokens


def _signing_key() -> bytes:
    explicit = os.getenv("LIVE_SESSION_SECRET", "").strip()
    if explicit:
        return explicit.encode("utf-8")
    base = (
        os.getenv("ADMIN_DASHBOARD_SESSION_SECRET", "").strip()
        or os.getenv("SALES_AGENT_INTERNAL_API_KEY", "").strip()
    )
    if not base:
        raise LiveError("Live calls need ADMIN_DASHBOARD_SESSION_SECRET on the agent.", 503)
    return hmac.new(base.encode("utf-8"), b"anata-live-session-v1", hashlib.sha256).digest()


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _unb64(text: str) -> bytes:
    pad = "=" * (-len(text) % 4)
    return base64.urlsafe_b64decode(text + pad)


def sign_token(session_id: str, tracker_id: str, email: str, *, ttl: int = TOKEN_TTL_SECONDS) -> tuple[str, int]:
    exp = int(time.time()) + ttl
    payload = _b64(json.dumps({"sid": session_id, "tid": tracker_id, "em": email, "exp": exp}, separators=(",", ":")).encode())
    signing_input = f"{TOKEN_VERSION}.{payload}"
    sig = _b64(hmac.new(_signing_key(), signing_input.encode("ascii"), hashlib.sha256).digest())
    return f"{signing_input}.{sig}", exp


def verify_token(token: str, session_id: str) -> dict:
    """Return the payload or raise LiveError(401)."""
    try:
        version, payload_b64, sig = (token or "").split(".")
    except ValueError:
        raise LiveError("Invalid live token.", 401)
    if version != TOKEN_VERSION:
        raise LiveError("Invalid live token.", 401)
    expected = _b64(hmac.new(_signing_key(), f"{version}.{payload_b64}".encode("ascii"), hashlib.sha256).digest())
    if not hmac.compare_digest(expected, sig):
        raise LiveError("Invalid live token.", 401)
    try:
        payload = json.loads(_unb64(payload_b64))
    except (ValueError, UnicodeDecodeError):
        raise LiveError("Invalid live token.", 401)
    if payload.get("sid") != session_id:
        raise LiveError("Token does not match this session.", 401)
    if int(payload.get("exp") or 0) < int(time.time()):
        raise LiveError("Live token expired.", 401)
    return payload


def bearer(header: Optional[str]) -> str:
    value = (header or "").strip()
    if value.lower().startswith("bearer "):
        return value[7:].strip()
    return ""


# ---------------------------------------------------------------- sessions


def relay_url() -> str:
    return (os.getenv("SALES_CALL_RELAY_URL", "").strip() or RELAY_URL_DEFAULT).rstrip("/")


def open_session_for(session: Session, tracker_id: str) -> Optional[SalesCallLiveSession]:
    stmt = (
        select(SalesCallLiveSession)
        .where(SalesCallLiveSession.tracker_id == tracker_id)
        .where(SalesCallLiveSession.status != STATUS_ENDED)
        .order_by(SalesCallLiveSession.started_at.desc())
        .limit(1)
    )
    return session.scalars(stmt).first()


def latest_session_for(session: Session, tracker_id: str) -> Optional[SalesCallLiveSession]:
    stmt = (
        select(SalesCallLiveSession)
        .where(SalesCallLiveSession.tracker_id == tracker_id)
        .order_by(SalesCallLiveSession.started_at.desc())
        .limit(1)
    )
    return session.scalars(stmt).first()


def start_session(
    session: Session,
    tracker: SalesCallTracker,
    user: dict,
    *,
    coach_context: str,
    label: str = "",
) -> SalesCallLiveSession:
    """Reuse this tracker's open session, or end stale ones and start fresh."""
    email = str(user.get("email") or "").strip().lower()
    context = str(coach_context or "")[:MAX_CONTEXT_CHARS]
    existing = open_session_for(session, tracker.id)
    if existing is not None:
        if context:
            existing.coach_context = context
        if label:
            existing.label = str(label)[:255]
        session.flush()
        return existing
    live = SalesCallLiveSession(
        id=str(uuid.uuid4()),
        tracker_id=tracker.id,
        owner_email=email or tracker.owner_email,
        status=STATUS_PENDING,
        label=str(label or tracker.prospect_name or tracker.company)[:255],
        coach_context=context,
        started_at=now_utc(),
        last_coach_line_id=0,
        after_call_json={},
    )
    session.add(live)
    session.flush()
    return live


def get_session(session: Session, session_id: str) -> Optional[SalesCallLiveSession]:
    return session.get(SalesCallLiveSession, str(session_id or "")[:36])


def session_for_token(session: Session, session_id: str, token: str) -> SalesCallLiveSession:
    verify_token(token, session_id)
    live = get_session(session, session_id)
    if live is None:
        raise LiveError("Live session not found.", 404)
    if live.status == STATUS_ENDED:
        raise LiveError("This live call has ended.", 410)
    return live


def mark_consent(session: Session, live: SalesCallLiveSession) -> datetime:
    if live.consent_at is None:
        live.consent_at = now_utc()
        session.flush()
    return aware(live.consent_at)


def mark_tape_ended(session: Session, live: SalesCallLiveSession) -> None:
    live.tape_ended_at = now_utc()
    session.flush()


def end_session(session: Session, live: SalesCallLiveSession) -> datetime:
    if live.status != STATUS_ENDED:
        live.status = STATUS_ENDED
        live.ended_at = now_utc()
        session.flush()
    return aware(live.ended_at)


def minutes_elapsed(live: SalesCallLiveSession, at: Optional[datetime] = None) -> float:
    start = aware(live.first_audio_at) or aware(live.started_at)
    end = aware(live.ended_at) or (at or now_utc())
    if not start:
        return 0.0
    return max(0.0, (end - start).total_seconds() / 60.0)


# ---------------------------------------------------------------- lines


@dataclass(frozen=True)
class LineIn:
    speaker: str
    text: str
    start_ms: int
    end_ms: int


def parse_lines(raw: Any) -> list[LineIn]:
    if not isinstance(raw, list):
        raise LiveError("lines must be a list.")
    if len(raw) > MAX_LINES_PER_POST:
        raise LiveError(f"At most {MAX_LINES_PER_POST} lines per request.")
    out: list[LineIn] = []
    for item in raw:
        if not isinstance(item, dict):
            raise LiveError("Each line must be an object.")
        speaker = str(item.get("speaker") or "").lower()
        if speaker not in SPEAKERS:
            raise LiveError("speaker must be prospect or rep.")
        text = " ".join(str(item.get("text") or "").split())[:MAX_LINE_CHARS]
        if not text:
            continue
        try:
            start_ms = max(0, int(item.get("startMs") or 0))
            end_ms = max(start_ms, int(item.get("endMs") or start_ms))
        except (TypeError, ValueError):
            raise LiveError("startMs and endMs must be numbers.")
        out.append(LineIn(speaker, text, start_ms, end_ms))
    return out


def add_lines(session: Session, live: SalesCallLiveSession, lines: list[LineIn]) -> int:
    now = now_utc()
    last_id = 0
    for ln in lines:
        row = SalesCallTranscriptLine(
            session_id=live.id,
            speaker=ln.speaker,
            text=ln.text,
            start_ms=ln.start_ms,
            end_ms=ln.end_ms,
            created_at=now,
        )
        session.add(row)
        session.flush()
        last_id = row.id
    if lines:
        if live.first_audio_at is None:
            live.first_audio_at = now
        if live.status == STATUS_PENDING:
            live.status = STATUS_LIVE
        session.flush()
    return last_id or last_line_id(session, live.id)


def last_line_id(session: Session, session_id: str) -> int:
    stmt = (
        select(SalesCallTranscriptLine.id)
        .where(SalesCallTranscriptLine.session_id == session_id)
        .order_by(SalesCallTranscriptLine.id.desc())
        .limit(1)
    )
    return int(session.scalars(stmt).first() or 0)


def lines_after(session: Session, session_id: str, after: int, limit: int = MAX_LINES_PER_STATE) -> list[SalesCallTranscriptLine]:
    stmt = (
        select(SalesCallTranscriptLine)
        .where(SalesCallTranscriptLine.session_id == session_id)
        .where(SalesCallTranscriptLine.id > int(after or 0))
        .order_by(SalesCallTranscriptLine.id.asc())
        .limit(limit)
    )
    return list(session.scalars(stmt))


def all_lines(session: Session, session_id: str) -> list[SalesCallTranscriptLine]:
    stmt = (
        select(SalesCallTranscriptLine)
        .where(SalesCallTranscriptLine.session_id == session_id)
        .order_by(SalesCallTranscriptLine.start_ms.asc(), SalesCallTranscriptLine.id.asc())
    )
    return list(session.scalars(stmt))


def line_json(row: SalesCallTranscriptLine) -> dict:
    return {
        "id": row.id,
        "speaker": row.speaker,
        "text": row.text,
        "startMs": row.start_ms,
        "endMs": row.end_ms,
    }


def transcript_text(lines: list[SalesCallTranscriptLine]) -> str:
    """Merge both tracks into time order: 'Prospect: ...' / 'Rep: ...'."""
    out = []
    for row in sorted(lines, key=lambda r: (r.start_ms, r.id)):
        who = "Prospect" if row.speaker == "prospect" else "Rep"
        out.append(f"{who}: {row.text}")
    return "\n".join(out)


# ---------------------------------------------------------------- coach state


def latest_coach_event(session: Session, session_id: str) -> Optional[SalesCallCoachEvent]:
    stmt = (
        select(SalesCallCoachEvent)
        .where(SalesCallCoachEvent.session_id == session_id)
        .order_by(SalesCallCoachEvent.rev.desc())
        .limit(1)
    )
    return session.scalars(stmt).first()


def try_acquire_lease(session: Session, live: SalesCallLiveSession, seconds: int = 60) -> bool:
    """Atomic lease so two coach passes never run at once for one call."""
    now = now_utc()
    result = session.execute(
        update(SalesCallLiveSession)
        .where(SalesCallLiveSession.id == live.id)
        .where(
            (SalesCallLiveSession.coach_lease_until.is_(None))
            | (SalesCallLiveSession.coach_lease_until < now)
        )
        .values(coach_lease_until=now + timedelta(seconds=seconds), last_coach_at=now)
        .execution_options(synchronize_session=False)
    )
    session.commit()
    session.refresh(live)
    return result.rowcount == 1


def release_lease(session: Session, live: SalesCallLiveSession, *, line_id: Optional[int] = None) -> None:
    values: dict = {"coach_lease_until": None}
    if line_id is not None:
        values["last_coach_line_id"] = int(line_id)
    session.execute(
        update(SalesCallLiveSession)
        .where(SalesCallLiveSession.id == live.id)
        .values(**values)
        .execution_options(synchronize_session=False)
    )
    session.commit()
    session.refresh(live)


def coach_due(session: Session, live: SalesCallLiveSession, *, min_interval: int = 10) -> bool:
    if live.status != STATUS_LIVE:
        return False
    lease = aware(live.coach_lease_until)
    now = now_utc()
    if lease and lease > now:
        return False
    last = aware(live.last_coach_at)
    if last and (now - last).total_seconds() < min_interval:
        return False
    return last_line_id(session, live.id) > int(live.last_coach_line_id or 0)


def session_json(live: Optional[SalesCallLiveSession]) -> Optional[dict]:
    if live is None:
        return None
    return {
        "id": live.id,
        "status": live.status,
        "startedAt": iso(live.started_at),
        "firstAudioAt": iso(live.first_audio_at),
        "consentAt": iso(live.consent_at),
        "endedAt": iso(live.ended_at),
        "minutes": round(minutes_elapsed(live), 1),
    }
