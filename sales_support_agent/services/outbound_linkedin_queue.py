"""Decide when a contact who got an email becomes eligible for LinkedIn.

The two channels have to know about each other or they arrive as two strangers
at once. This is the piece that joins them: Instantly says what happened to the
email, and that decides whether a LinkedIn touch still makes sense, and when.

Driven by the email actually being SENT, not by a calendar. Instantly drips
sends across days on warmup and throttling, so "day 5" on a calendar fires
before the email has gone out for some people and long after for others.
Measuring from the send event makes the gap the one the person experienced.

Deliberately NOT driven by opens. Apple Mail pre-fetches images, so a large
share of opens are machines rather than people, and open tracking needs a pixel
in the message, which the copy playbook rules out of a first email. Every event
used here is something a person actually did.

The state machine is small on purpose:

    (nothing) --email_sent--> waiting (until = sent + N days)
    waiting   --any human reply--> stopped
    waiting   --time passes--> due      (surfaces on Lead Ops for review)
    due       --pushed to HeyReach--> sent

Nothing here sends anything. It decides eligibility; a person still presses the
button, because a LinkedIn request cannot be taken back.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)

# Days between the email landing and the LinkedIn touch becoming eligible.
# Long enough that the two do not read as one machine firing twice, short
# enough that the email is still why the name looks familiar.
DEFAULT_WAIT_DAYS = 5

# Anything a human did. All of these end the LinkedIn step: if someone has
# already answered, a connection request is not a follow-up, it is noise.
STOP_EVENTS = {
    "reply_received": "they replied",
    "lead_interested": "they said they are interested",
    "lead_meeting_booked": "a meeting is booked",
    "lead_meeting_completed": "the meeting happened",
    "lead_neutral": "they replied",
    "lead_not_interested": "they said no",
}

# The one stop that must also close the email channel, not just LinkedIn.
HARD_STOP = "lead_not_interested"

START_EVENT = "email_sent"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def wait_days(settings: dict[str, Any] | None = None) -> int:
    """How long to wait, from settings, with a floor and a ceiling.

    Bounded because this is reachable from the settings page: a zero would fire
    LinkedIn the same hour as the email, which is the exact thing the delay
    exists to prevent, and a huge value would silently park everyone forever.
    """
    raw = (settings or {}).get("outbound.linkedin_wait_days")
    try:
        days = int(raw)
    except (TypeError, ValueError):
        return DEFAULT_WAIT_DAYS
    return max(1, min(30, days))


def decide(event_type: str, *, now: datetime | None = None,
           settings: dict[str, Any] | None = None) -> dict[str, Any] | None:
    """What this Instantly event means for the LinkedIn step.

    Returns None for events that change nothing, so a caller can skip the write
    rather than churn a row on every webhook.
    """
    event = str(event_type or "").strip()
    now = now or _now()

    if event == START_EVENT:
        return {
            "state": "waiting",
            "eligible_at": (now + timedelta(days=wait_days(settings))).isoformat(timespec="seconds"),
            "reason": "",
            "block_email": False,
        }

    if event in STOP_EVENTS:
        return {
            "state": "stopped",
            "eligible_at": "",
            "reason": STOP_EVENTS[event],
            # A no closes both doors. Reaching someone on LinkedIn after they
            # declined by email is the fastest way to earn a complaint.
            "block_email": event == HARD_STOP,
        }

    return None


def is_due(row: dict[str, Any], *, now: datetime | None = None) -> bool:
    """Is this contact ready for a LinkedIn touch right now?

    Requires a real profile, so a contact with no LinkedIn URL can never become
    due and is never shown as work that cannot be done.
    """
    if str(row.get("state") or "") != "waiting":
        return False
    if not str(row.get("linkedin_url") or "").strip():
        return False
    stamp = str(row.get("eligible_at") or "").strip()
    if not stamp:
        return False
    try:
        when = datetime.fromisoformat(stamp)
    except ValueError:
        return False
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return when <= (now or _now())


def summarise(rows: list[dict[str, Any]], *, now: datetime | None = None) -> dict[str, int]:
    """Counts for the Lead Ops panel. Every row lands in exactly one bucket, so
    a total that does not add up is visible rather than assumed."""
    out = {"total": 0, "due": 0, "waiting": 0, "stopped": 0, "sent": 0, "no_profile": 0}
    for row in rows:
        out["total"] += 1
        state = str(row.get("state") or "")
        if state == "sent":
            out["sent"] += 1
        elif state == "stopped":
            out["stopped"] += 1
        elif not str(row.get("linkedin_url") or "").strip():
            out["no_profile"] += 1
        elif is_due(row, now=now):
            out["due"] += 1
        else:
            out["waiting"] += 1
    return out


def describe(row: dict[str, Any], *, now: datetime | None = None) -> str:
    """One plain line about where this contact stands."""
    state = str(row.get("state") or "")
    if state == "sent":
        return "Already sent on LinkedIn."
    if state == "stopped":
        return f"Stopped: {row.get('reason') or 'no longer eligible'}."
    if not str(row.get("linkedin_url") or "").strip():
        return "No LinkedIn profile, email only."
    if is_due(row, now=now):
        return "Ready for LinkedIn."
    stamp = str(row.get("eligible_at") or "").strip()
    if not stamp:
        return "Waiting on the first email."
    try:
        when = datetime.fromisoformat(stamp)
        if when.tzinfo is None:
            when = when.replace(tzinfo=timezone.utc)
    except ValueError:
        return "Waiting."
    delta = when - (now or _now())
    days = max(0, delta.days)
    if days == 0:
        return "Ready later today."
    return f"Ready in {days} day{'s' if days != 1 else ''}."
