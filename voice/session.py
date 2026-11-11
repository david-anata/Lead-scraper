"""In-memory state for one live call.

Render runs a single instance of this service because a transfer and the
media stream have to meet on the same process. The durable copy is the
Supabase row written when the call ends.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any

from voice.lines import LineContext


@dataclass
class CallSession:
    call_sid: str
    caller_number: str
    dialed_number: str
    line: LineContext
    started_at: float = field(default_factory=time.time)
    resume: str = ""
    category: str = "other"
    reason: str = ""
    notes: str = ""
    caller_name: str = ""
    company: str = ""
    email: str = ""
    phone: str = ""
    event_date: str = ""
    guest_count: str = ""
    event_or_interest_type: str = ""
    business_line: str = ""
    marketplaces: str = ""
    revenue_range: str = ""
    summary: str = ""
    action: str = ""
    callback_needed: bool = True
    transfer_requested: bool = False
    transfer_started: bool = False
    transfer_state: str = ""
    transfer_outcome: str = ""
    transfer_accepted: bool = False
    holding_for_transfer: bool = False
    agent_call_sid: str = ""
    voicemail_detected: bool = False
    asked_for_david: bool = False
    security_preference: str = ""
    turns: list[str] = field(default_factory=list)
    caller_partials: dict[str, str] = field(default_factory=dict)
    finalized: bool = False
    finalizing: bool = False
    inquiry_id: str = ""
    sms_notified: bool = False
    lock: threading.Lock = field(default_factory=threading.Lock)

    def remember(self, args: dict[str, Any]) -> None:
        """Merge tool fields. Later non-empty values replace earlier ones."""

        mapping = {
            "caller_name": "caller_name",
            "name": "caller_name",
            "company": "company",
            "email": "email",
            "phone": "phone",
            "event_date": "event_date",
            "guest_count": "guest_count",
            "event_or_interest_type": "event_or_interest_type",
            "business_line": "business_line",
            "marketplaces": "marketplaces",
            "revenue_range": "revenue_range",
            "reason": "reason",
            "summary": "summary",
        }
        preference = normalize_security_preference(args.get("security_preference"))
        if preference:
            self.security_preference = preference
        for source, target in mapping.items():
            value = args.get(source)
            if isinstance(value, str) and value.strip():
                setattr(self, target, value.strip()[:500])
        extra = args.get("notes")
        if isinstance(extra, str) and extra.strip():
            piece = extra.strip()[:1000]
            self.notes = f"{self.notes} {piece}".strip()[:2000]
        guests = args.get("guest_count")
        if isinstance(guests, (int, float)) and not isinstance(guests, bool):
            self.guest_count = str(int(guests))

    def add_turn(self, speaker: str, text: str) -> None:
        cleaned = " ".join(text.split())
        if cleaned:
            self.turns.append(f"{speaker}: {cleaned}")

    def transcript(self) -> str:
        """Full call transcript. Nothing is cut off to fit a preview."""

        lines = list(self.turns)
        for text in self.caller_partials.values():
            cleaned = " ".join(text.split())
            if cleaned and f"Caller: {cleaned}" not in lines:
                lines.append(f"Caller: {cleaned}")
        return "\n".join(lines)

    def lead_fields(self) -> dict[str, str]:
        fields = {
            "name": self.caller_name,
            "phone": self.phone or self.caller_number,
            "email": self.email,
            "company": self.company,
            "event_date": self.event_date,
            "guest_count": self.guest_count,
            "type": self.event_or_interest_type,
            "business_line": self.business_line,
            "marketplaces": self.marketplaces,
            "revenue_range": self.revenue_range,
            "notes": self.notes,
            "reason": self.reason,
        }
        if self.security_preference:
            fields["security_preference"] = self.security_preference
        source = self.line.source.strip()
        if source:
            fields["source"] = source
        return fields

    def duration_seconds(self, now: float | None = None) -> int:
        end = time.time() if now is None else now
        return max(0, int(end - self.started_at))


def normalize_security_preference(value: object) -> str:
    """Caller answer about optional event security: yes, no, or undecided."""

    text = str(value or "").strip().casefold()
    if text in {"yes", "no", "undecided"}:
        return text
    return ""


class SessionStore:
    """Process-local call table. Pruned so a long-lived worker cannot grow forever."""

    def __init__(self, ttl_seconds: int = 6 * 60 * 60) -> None:
        self._items: dict[str, CallSession] = {}
        self._lock = threading.Lock()
        self.ttl_seconds = ttl_seconds

    def put(self, session: CallSession) -> CallSession:
        with self._lock:
            self._prune_locked()
            self._items[session.call_sid] = session
            return session

    def get(self, call_sid: str) -> CallSession | None:
        with self._lock:
            return self._items.get(call_sid)

    def _prune_locked(self) -> None:
        cutoff = time.time() - self.ttl_seconds
        stale = [key for key, item in self._items.items() if item.started_at < cutoff]
        for key in stale:
            del self._items[key]
