"""Close a call: write the log, then text the summary.

Spam calls are logged and not texted. Everything else is texted when a
notify number is configured. Twilio accepting the text is not delivery.
`sms_notified` stays false until the status callback says delivered.

A transfer parks the caller on hold. The first media socket closes then,
and this function must not log the call until that hold is over.
"""

from __future__ import annotations

import asyncio
import logging
from urllib.parse import quote

from voice.leads import SupabaseStore
from voice.notify import TwilioRest, call_summary_text, should_text_about_call
from voice.session import CallSession
from voice.settings import Settings

logger = logging.getLogger(__name__)


def begin_transfer(session: CallSession) -> None:
    """Mark the call as parked. The closing socket must not finalize it."""

    session.holding_for_transfer = True
    session.transfer_started = True
    session.transfer_state = "dialing"


def accept_transfer(session: CallSession) -> None:
    """The on-call phone pressed 1. Voicemail cannot reach this state."""

    with session.lock:
        if session.voicemail_detected:
            return
        session.transfer_accepted = True
        session.transfer_outcome = "accepted"
        session.transfer_state = "connected"


def fail_transfer(session: CallSession, outcome: str) -> bool:
    """Send the caller back to the assistant. A voicemail outcome is not an answer.

    Returns false when the transfer was already accepted or already handed back,
    so a late status callback cannot overwrite a real connect.
    """

    cleaned = (outcome or "no_answer").strip().casefold() or "no_answer"
    if cleaned in {"answered", "accepted", "completed"}:
        cleaned = "no_answer"
    with session.lock:
        if session.transfer_accepted or session.finalized or session.resume == "message":
            return False
        session.resume = "message"
        session.transfer_state = "messaging"
        session.transfer_requested = False
        session.transfer_outcome = cleaned[:200]
        session.callback_needed = True
        session.action = "transfer_no_answer_message"
        return True


async def finalize_call(
    session: CallSession,
    *,
    settings: Settings,
    store: SupabaseStore,
    twilio: TwilioRest,
) -> None:
    with session.lock:
        if session.finalized or session.finalizing:
            return
        if session.holding_for_transfer or session.transfer_state == "dialing":
            return
        session.finalizing = True
    try:
        if session.resume == "message" and session.action in {"", "transfer", "message"}:
            session.action = "transfer_no_answer_message"
            session.callback_needed = True
        if session.transfer_outcome == "voicemail":
            session.callback_needed = True
            if session.action in {"", "transfer", "transferred"}:
                session.action = "transfer_no_answer_message"
        if session.category == "spam":
            session.callback_needed = False
            session.action = "logged_only"
        # Delivery is unknown until the status callback. Never store a 2xx as delivered.
        session.sms_notified = False
        await asyncio.to_thread(store.save_call, session, sms_notified=False)
        if should_text_about_call(session.category) and settings.notify_sms_number:
            callback = _status_callback(settings, session.call_sid)
            accepted = await asyncio.to_thread(
                twilio.send_sms,
                from_number=session.dialed_number,
                to_number=settings.notify_sms_number,
                body=call_summary_text(session),
                status_callback=callback,
            )
            if not accepted:
                logger.warning("After-call text was not accepted for call %s", session.call_sid)
        session.finalized = True
    except Exception:
        logger.exception("Failed to finalize call %s", session.call_sid)
    finally:
        if not session.finalized:
            session.finalizing = False


def _status_callback(settings: Settings, call_sid: str) -> str:
    base = settings.http_base()
    if not base or not call_sid:
        return ""
    return f"{base}/voice/sms-status?call_sid={quote(call_sid, safe='')}"
