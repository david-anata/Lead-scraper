"""Close a call: text the summary, then write the log.

Spam calls are logged and not texted. Everything else is texted when a
notify number is configured.
"""

from __future__ import annotations

import asyncio
import logging

from voice.leads import SupabaseStore
from voice.notify import TwilioRest, call_summary_text, should_text_about_call
from voice.session import CallSession
from voice.settings import Settings

logger = logging.getLogger(__name__)


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
        if session.transfer_state == "dialing":
            return
        session.finalizing = True
    try:
        if session.resume == "message" and session.action in {"", "transfer", "message"}:
            session.action = "transfer_no_answer_message"
            session.callback_needed = True
        if session.category == "spam":
            session.callback_needed = False
            session.action = "logged_only"
        notified = False
        if should_text_about_call(session.category) and settings.notify_sms_number:
            notified = await asyncio.to_thread(
                twilio.send_sms,
                from_number=session.dialed_number,
                to_number=settings.notify_sms_number,
                body=call_summary_text(session),
            )
        session.sms_notified = notified
        await asyncio.to_thread(store.save_call, session, sms_notified=notified)
        session.finalized = True
    except Exception:
        logger.exception("Failed to finalize call %s", session.call_sid)
    finally:
        if not session.finalized:
            session.finalizing = False
