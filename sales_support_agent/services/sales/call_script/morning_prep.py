"""Morning prep: one ClickUp prep comment per sales call scheduled today.

Uses the same facts as ss-prep (`prep.gather_context`): verified website and
Amazon links, prior-call / mail / Abby history, and the ClickUp task. Adds
disqualification flags. Posting is behind SALES_MORNING_PREP_ENABLED (off by
default); dry run builds the comments and posts nothing.
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Callable, Optional

from sales_support_agent.services.sales.call_script.bookings import Booking
from sales_support_agent.services.sales.call_script.prep import (
    AMAZON_UNVERIFIED,
    DENVER,
    _FREEMAIL,
    _history_line,
    format_when,
    no_em_dash,
)

FLAG_ENV = "SALES_MORNING_PREP_ENABLED"
_DQ_STATUSES = ("lost", "disqualified", "not a fit", "dq", "won canceled", "churn")
_DQ_WORDS = ("unsubscribe", "not interested", "remove me", "stop emailing", "do not contact", "spam")


def enabled() -> bool:
    return os.getenv(FLAG_ENV, "").strip().lower() in {"1", "true", "yes", "on"}


def todays_bookings(bookings: list[Booking], now: datetime) -> list[Booking]:
    day = now.astimezone(DENVER).date()
    return sorted(
        (b for b in bookings if b.starts_at and b.starts_at.astimezone(DENVER).date() == day),
        key=lambda b: b.starts_at,
    )


def disqualification_flags(booking: Booking, context: dict) -> list[str]:
    flags: list[str] = []
    links = context.get("links") or {}
    website = context.get("website") or {}
    domain = str(context.get("domain") or "")
    if not domain:
        flags.append("No email domain on the booking")
    elif domain in _FREEMAIL:
        flags.append(f"Personal email domain ({domain}), no company site to check")
    elif not website.get("verified"):
        flags.append(f"Website {domain} did not load")
    if not (links.get("amazon") or {}).get("verified"):
        flags.append(AMAZON_UNVERIFIED)
    status = str((context.get("clickup") or {}).get("status") or "").lower()
    if any(s in status for s in _DQ_STATUSES):
        flags.append(f"ClickUp status is '{status}'")
    contact = (context.get("outbound_contact") or {}).get("row") or {}
    if str(contact.get("state") or "").lower() in {"suppressed", "unsubscribed", "bounced", "do_not_contact"}:
        flags.append(f"Outbound contact state: {contact.get('state')}")
    blob = _history_line(context).lower()
    hits = [w for w in _DQ_WORDS if w in blob]
    if hits:
        flags.append("History mentions: " + ", ".join(hits))
    return flags


def _why_back(booking: Booking, context: dict) -> str:
    comments = (context.get("clickup") or {}).get("comments") or []
    abby = (context.get("abby_handled") or {}).get("rows") or []
    if not comments and not abby and not context.get("gmail") and not context.get("mailbox"):
        return "First call. No prior call or mail history on file."
    reason = booking.purpose or "Booking form answer was empty."
    return f"Returning. Booking says: {reason}"


def build_comment(booking: Booking, context: dict) -> str:
    links = context.get("links") or {}
    website = context.get("website") or {}
    amazon = links.get("amazon") or {}
    clickup = context.get("clickup") or {}
    history = _history_line(context)
    lines = [
        f"Morning prep: {booking.prospect_name or 'Unknown'} ({booking.prospect_email or 'no email'}) at {format_when(booking.starts_at)}",
        f"Company: {context.get('company') or 'Unknown'}",
        "Website: " + (website.get("url") + " (verified)" if website.get("verified") else "not verified"),
        "Amazon: " + (", ".join(amazon.get("urls") or []) + " (verified)" if amazon.get("verified") else "not verified"),
        f"Current ClickUp status: {clickup.get('status') or 'unknown'}",
        "",
        "Why they're back: " + _why_back(booking, context),
        "Prior history: " + (history[:1200] if history else "none on file"),
        "",
        "Disqualification flags:",
    ]
    flags = disqualification_flags(booking, context)
    lines += [f"- {f}" for f in flags] or ["- none"]
    if booking.meet_url:
        lines += ["", f"Meet: {booking.meet_url}"]
    return no_em_dash("\n".join(lines))[:9000]


def run_morning_prep(
    *,
    now: datetime,
    bookings: list[Booking],
    context_for: Callable[[Booking], dict],
    clickup: Any,
    dry_run: bool = True,
) -> dict:
    """Build a comment per call today. Posts only when enabled and not a dry run."""

    post = enabled() and not dry_run and clickup is not None
    rows = []
    for booking in todays_bookings(bookings, now):
        row: dict[str, Optional[str] | bool] = {"event_id": booking.event_id, "posted": False}
        try:
            context = context_for(booking)
            task_id = str((context.get("clickup") or {}).get("id") or "")
            row.update({"task_id": task_id, "comment": build_comment(booking, context)})
            if not task_id:
                row["skipped"] = "No matching ClickUp task"
            elif post:
                clickup.create_task_comment(task_id, row["comment"])
                row["posted"] = True
        except Exception as exc:  # one failure does not stop the rest
            row["error"] = type(exc).__name__
        rows.append(row)
    return {"enabled": enabled(), "dry_run": not post, "count": len(rows), "calls": rows}
