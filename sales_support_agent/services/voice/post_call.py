"""After a call: persist the log, add a CRM note, and email an internal summary."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Callable, Optional

import requests

from sales_support_agent.models.database import session_scope
from sales_support_agent.models.entities import BuildingAuditEvent
from sales_support_agent.models.voice import VoiceCall
from sales_support_agent.services.voice.config import VoiceConfig
from sales_support_agent.services.voice.tools import CallContext

logger = logging.getLogger(__name__)


def transcript_text(transcript: list[dict]) -> str:
    return "\n".join(f"{'Caller' if t['speaker'] == 'caller' else 'Agent'}: {t['text']}" for t in transcript)


def fallback_summary(ctx: CallContext) -> str:
    state = ctx.state
    parts = [f"Inbound call from {ctx.from_number or 'unknown number'}."]
    if state.get("caller_name"):
        parts.append(f"Caller: {state['caller_name']}.")
    if state.get("tour_start_at"):
        parts.append(f"Tour booked for {state['tour_start_at'].isoformat()}.")
    if state.get("transferred"):
        parts.append("Transferred to a person.")
    if state.get("opted_out"):
        parts.append("Caller asked not to be called again (added to do-not-call).")
    caller_lines = [t["text"] for t in state.get("transcript", []) if t["speaker"] == "caller"]
    if caller_lines:
        parts.append("Caller said: " + " / ".join(caller_lines[:3])[:400])
    return " ".join(parts)


def xai_summary(config: VoiceConfig, ctx: CallContext) -> str:
    transcript = transcript_text(ctx.state.get("transcript", []))
    if not (config.xai_api_key and transcript):
        return ""
    response = requests.post(
        "https://api.x.ai/v1/chat/completions",
        headers={"Authorization": f"Bearer {config.xai_api_key}"},
        json={
            "model": config.xai_summary_model,
            "messages": [
                {"role": "system", "content": "Summarize this receptionist call for the venue team in 3-5 short bullet points: who called, what they want (event type, date, size), what was booked or promised, and the next step. Do not invent details."},
                {"role": "user", "content": transcript[:20000]},
            ],
        },
        timeout=30,
    )
    if response.status_code >= 400:
        return ""
    return (response.json().get("choices") or [{}])[0].get("message", {}).get("content", "").strip()


def _outcome(ctx: CallContext, base: str) -> str:
    if base == "error":
        return "error"
    if ctx.state.get("opted_out"):
        return "opted_out"
    if ctx.state.get("tour_event_id"):
        return "tour_booked"
    if ctx.state.get("transferred"):
        return "transferred"
    return "completed"


def default_send_email(to: str, subject: str, body: str) -> str:
    from sales_support_agent.config import load_settings
    from sales_support_agent.integrations.gmail import GmailClient

    client = GmailClient(load_settings())
    if not client.is_configured():
        return "skipped_unconfigured"
    result = client.send_message(to=tuple(a.strip() for a in to.split(",") if a.strip()), subject=subject, text=body)
    return "sent" if result.get("ok", True) and not result.get("skipped") else "failed"


def finalize_call(
    ctx: CallContext,
    outcome: str,
    *,
    summarize: Optional[Callable[[VoiceConfig, CallContext], str]] = None,
    send_email: Optional[Callable[[str, str, str], str]] = None,
) -> str:
    summarize = summarize or xai_summary
    send_email = send_email or default_send_email
    state = ctx.state
    try:
        summary = summarize(ctx.config, ctx) or fallback_summary(ctx)
    except Exception as exc:  # noqa: BLE001
        logger.warning("voice summary failed: %s", exc)
        summary = fallback_summary(ctx)
    final_outcome = _outcome(ctx, outcome)
    now = datetime.now(timezone.utc)
    with session_scope(ctx.session_factory) as session:
        row = session.query(VoiceCall).filter_by(call_sid=ctx.call_sid).one_or_none()
        if row is None:
            return ""
        row.status = "ended"
        row.outcome = final_outcome
        row.ended_at = now
        started = row.started_at if row.started_at.tzinfo else row.started_at.replace(tzinfo=timezone.utc)
        row.duration_seconds = max(0, int((now - started).total_seconds()))
        row.disclosure_played_at = state.get("disclosure_played_at")
        row.transcript_json = state.get("transcript", [])
        row.tool_calls_json = state.get("tool_calls", [])
        row.summary = summary
        row.caller_name = str(state.get("caller_name", ""))[:255]
        row.caller_email = str(state.get("caller_email", ""))[:255]
        row.matched_inquiry_id = str(state.get("inquiry_id", ""))
        row.tour_event_id = str(state.get("tour_event_id", ""))
        row.tour_start_at = state.get("tour_start_at")
        row.transferred = bool(state.get("transferred"))
        row.error = str(state.get("error", ""))
        call_id = row.id
        if row.matched_inquiry_id:
            # CRM note on the inquiry's timeline (the Building CRM audit trail).
            session.add(
                BuildingAuditEvent(
                    entity_type="building_inquiry",
                    entity_id=row.matched_inquiry_id,
                    action="voice_call_note",
                    actor="voice_agent",
                    before_json={},
                    after_json={"voice_call_id": call_id, "outcome": final_outcome, "summary": summary[:4000]},
                    created_at=now,
                )
            )
    email_status = "skipped_no_recipient"
    if ctx.config.summary_email_to:
        body = (
            f"{summary}\n\nOutcome: {final_outcome}\nFrom: {ctx.from_number}\n"
            f"Call log: {ctx.config.public_base_url or ''}/admin/sales/voice-calls/{call_id}\n\n"
            f"Transcript\n{transcript_text(state.get('transcript', []))[:15000]}"
        )
        try:
            email_status = send_email(ctx.config.summary_email_to, f"Voice call summary: {state.get('caller_name') or ctx.from_number}", body)
        except Exception as exc:  # noqa: BLE001
            logger.warning("voice summary email failed: %s", exc)
            email_status = "failed"
    with session_scope(ctx.session_factory) as session:
        row = session.query(VoiceCall).filter_by(call_sid=ctx.call_sid).one()
        row.summary_email_status = email_status
    return call_id
