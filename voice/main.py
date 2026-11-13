"""FastAPI app for the Anata phone receptionist.

Webhook paths, all POST unless noted:

- `POST /voice/inbound` — Twilio Voice webhook
- `POST /voice/dial-result` — caller's conference leg ended
- `POST /voice/whisper` — spoken to the on-call phone, then waits for digit 1
- `POST /voice/accept` — on-call phone pressed 1 and joins the conference
- `POST /voice/agent-status` — on-call leg ended without a press 1
- `POST /voice/agent-amd` — voicemail detection on the on-call leg
- `POST /voice/sms-status` — delivery receipt for a summary text
- `GET  /voice/media` — Twilio Media Stream websocket
- `POST /voice/sms` — Twilio Messaging webhook
- `GET  /health` — Render health check
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any
from urllib.parse import quote

from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import PlainTextResponse, Response

from voice.bridge import CallBridge, whisper_line
from voice.known_callers import apply_known_caller
from voice.calendar_availability import CalendarAvailability
from voice.logging_store import LoggingStore
from voice.lifecycle import accept_transfer, fail_transfer, finalize_call
from voice.lines import LineContext, normalize_phone, resolve_line
from voice.notify import TwilioRest, sms_attention_text, sms_forward_text
from voice.security import public_request_url, valid_signature, valid_stream_token
from voice.session import CallSession, SessionStore
from voice.settings import Settings
from voice.sms_conversation import SmsThreadStore, handle_inbound_sms
from voice.sms_reply import classify_sms
from voice.twiml import (
    empty_twiml,
    goodbye_twiml,
    hangup,
    inbound_twiml,
    join_conference_twiml,
    resume_twiml,
    say,
    sms_twiml,
    whisper_twiml,
    xml_response,
)

logger = logging.getLogger(__name__)

XML = "application/xml"


def create_app(settings: Settings | None = None) -> FastAPI:
    logging.basicConfig(level=logging.INFO)
    current = settings or Settings.from_env()
    app = FastAPI(title="Anata voice receptionist", docs_url=None, redoc_url=None)
    app.state.settings = current
    app.state.sessions = SessionStore()
    app.state.sms_threads = SmsThreadStore()
    app.state.store = LoggingStore(
        database_url=current.voice_database_url,
        supabase_url=current.supabase_url,
        service_key=current.supabase_service_key,
        owner=current.lead_owner,
    )
    app.state.twilio = TwilioRest(current)
    app.state.calendar = CalendarAvailability(
        calendar_id=current.calendar_id,
        service_account_json=current.google_service_account_json,
        delegated_subject=current.google_delegated_subject,
    )
    if os.getenv("SMS_SETUP_RUN", "").strip() == "1":
        from voice.sms_setup import schedule_sms_setup

        schedule_sms_setup(current)

    @app.get("/", response_class=PlainTextResponse)
    def root() -> str:
        return "Anata voice receptionist"

    @app.get("/health")
    def health() -> dict[str, Any]:
        live = app.state.settings
        probed = app.state.store.probe_logging()
        known = app.state.store.probe_known_callers()
        if probed is None:
            logging_ready = live.logging_ready
            logging_probe = "not_probed"
        else:
            logging_ready = probed
            logging_probe = app.state.store.postgres.probe_status
        if known is None:
            known_ready = False
            known_probe = "not_probed"
        else:
            known_ready = known
            known_probe = app.state.store.postgres.known_probe_status
        return {
            "status": "ok",
            "service": "anata-voice-receptionist",
            "calls_ready": live.calls_ready,
            "logging_ready": logging_ready,
            "logging_probe": logging_probe,
            "calendar_ready": live.calendar_ready,
            "sms_autoreply": live.sms_autoreply,
            "recording_enabled": live.recording_enabled,
            "known_callers_ready": known_ready,
            "known_callers_probe": known_probe,
            "missing": live.missing(),
        }

    @app.api_route("/voice/inbound", methods=["GET", "POST"])
    async def inbound(request: Request) -> Response:
        form, ok = await _signed_form(request)
        if not ok:
            return Response(status_code=403)
        settings = request.app.state.settings
        call_sid = form.get("CallSid", "")
        caller = form.get("From", "")
        dialed = form.get("To", "")
        if call_sid and settings.calls_ready:
            line = resolve_line(settings.line_contexts_json, dialed)
            session = CallSession(
                call_sid=call_sid,
                caller_number=caller,
                dialed_number=dialed,
                line=line,
            )
            try:
                row = await asyncio.to_thread(request.app.state.store.resolve_caller, caller)
            except Exception:
                logger.exception("Known caller lookup failed")
                row = {}
            apply_known_caller(session, row)
            request.app.state.sessions.put(session)
        base = _base(request)
        xml = inbound_twiml(settings, http_base=base, call_sid=call_sid)
        return Response(xml, media_type=XML)

    @app.api_route("/voice/dial-result", methods=["GET", "POST"])
    async def dial_result(request: Request) -> Response:
        form, ok = await _signed_form(request)
        if not ok:
            return Response(status_code=403)
        session = request.app.state.sessions.get(form.get("CallSid", ""))
        if session is None:
            return Response(empty_twiml(), media_type=XML)
        status = form.get("DialCallStatus", "")
        call_status = form.get("CallStatus", "")
        # Hold time is not an answer. Only a press 1 counts as a connected transfer.
        if session.transfer_accepted:
            session.holding_for_transfer = False
            session.transfer_state = "done"
            session.transfer_outcome = "answered"
            session.action = "transferred"
            session.callback_needed = False
            await _finish(request, session)
            if call_status == "completed":
                return Response(empty_twiml(), media_type=XML)
            return Response(goodbye_twiml(), media_type=XML)
        if session.resume == "message" or session.transfer_state == "messaging":
            # The on-call leg already failed. If this Dial action still owns the
            # caller, hand them back to the assistant instead of hanging up.
            if call_status == "completed":
                return Response(empty_twiml(), media_type=XML)
            xml = resume_twiml(
                request.app.state.settings,
                http_base=_base(request),
                call_sid=session.call_sid,
            )
            return Response(xml, media_type=XML)
        if call_status == "completed":
            session.holding_for_transfer = False
            session.transfer_state = "done"
            raw = (status or "no_answer").strip().casefold()
            if raw in {"", "completed", "answered"}:
                raw = "no_answer"
            session.transfer_outcome = "voicemail" if session.voicemail_detected else raw
            session.action = "needs_callback" if session.asked_for_david else "transfer_no_answer_message"
            session.callback_needed = True
            await _drop_agent(request, session)
            await _finish(request, session)
            return Response(empty_twiml(), media_type=XML)
        outcome = "voicemail" if session.voicemail_detected else (status or "no_answer")
        if fail_transfer(session, outcome):
            await _drop_agent(request, session)
        xml = resume_twiml(
            request.app.state.settings,
            http_base=_base(request),
            call_sid=session.call_sid,
        )
        return Response(xml, media_type=XML)

    @app.api_route("/voice/whisper", methods=["GET", "POST"])
    async def whisper(request: Request) -> Response:
        form, ok = await _signed_form(request)
        if not ok:
            return Response(status_code=403)
        parent = _parent(request) or form.get("ParentCallSid") or ""
        session = request.app.state.sessions.get(parent) if parent else None
        if session is None:
            return Response(
                xml_response(say("This transfer is no longer available."), hangup()),
                media_type=XML,
            )
        accept_url = f"{_base(request)}/voice/accept?parent={quote(parent, safe='')}"
        return Response(whisper_twiml(whisper_line(session), accept_url), media_type=XML)

    @app.api_route("/voice/accept", methods=["GET", "POST"])
    async def accept(request: Request) -> Response:
        form, ok = await _signed_form(request)
        if not ok:
            return Response(status_code=403)
        parent = _parent(request)
        session = request.app.state.sessions.get(parent) if parent else None
        digit = form.get("Digits", "").strip()
        if digit == "1" and session is not None and not session.voicemail_detected:
            accept_transfer(session)
            if session.transfer_accepted:
                return Response(join_conference_twiml(parent), media_type=XML)
        if session is not None:
            outcome = "voicemail" if session.voicemail_detected else "no_answer"
            if fail_transfer(session, outcome):
                await _redirect_parent(request, session)
        return Response(xml_response(hangup()), media_type=XML)

    @app.api_route("/voice/agent-status", methods=["GET", "POST"])
    async def agent_status(request: Request) -> Response:
        form, ok = await _signed_form(request)
        if not ok:
            return Response(status_code=403)
        session = _parent_session(request)
        if session is None or session.transfer_accepted:
            return Response(empty_twiml(), media_type=XML)
        status = form.get("CallStatus", "").strip().casefold()
        if status in {"no-answer", "busy", "failed", "canceled", "completed"}:
            outcome = "voicemail" if session.voicemail_detected else "no_answer"
            if fail_transfer(session, outcome):
                await _redirect_parent(request, session)
        return Response(empty_twiml(), media_type=XML)

    @app.api_route("/voice/agent-amd", methods=["GET", "POST"])
    async def agent_amd(request: Request) -> Response:
        form, ok = await _signed_form(request)
        if not ok:
            return Response(status_code=403)
        session = _parent_session(request)
        answered = form.get("AnsweredBy", "").strip().casefold()
        if session is not None and session.transfer_accepted:
            return Response(empty_twiml(), media_type=XML)
        if session is not None and (answered.startswith("machine") or answered == "fax"):
            session.voicemail_detected = True
            if not session.transfer_accepted:
                session.transfer_outcome = "voicemail"
                session.callback_needed = True
                if session.action in {"", "transfer", "transferred"}:
                    session.action = "transfer_no_answer_message"
            agent_sid = form.get("CallSid", "") or session.agent_call_sid
            if agent_sid:
                await asyncio.to_thread(request.app.state.twilio.complete_call, agent_sid)
            if fail_transfer(session, "voicemail"):
                await _redirect_parent(request, session)
        return Response(empty_twiml(), media_type=XML)

    @app.api_route("/voice/sms-status", methods=["GET", "POST"])
    async def sms_status(request: Request) -> Response:
        form, ok = await _signed_form(request)
        if not ok:
            return Response(status_code=403)
        call_sid = request.query_params.get("call_sid", "").strip()
        status = form.get("MessageStatus", "").strip().casefold()
        error = form.get("ErrorCode", "").strip()
        if call_sid and (error == "30034" or status in {"undelivered", "failed"}):
            logger.warning("SMS not delivered status=%s error=%s", status, error)
            await asyncio.to_thread(request.app.state.store.set_sms_notified, call_sid, False)
        elif call_sid and status == "delivered" and error != "30034":
            await asyncio.to_thread(request.app.state.store.set_sms_notified, call_sid, True)
        return Response(empty_twiml(), media_type=XML)

    @app.api_route("/voice/sms", methods=["GET", "POST"])
    async def sms(request: Request) -> Response:
        form, ok = await _signed_form(request)
        if not ok:
            return Response(status_code=403)
        if request.app.state.settings.sms_autoreply:
            return await _sms_conversation(request, form)
        return await _sms_log_and_forward(request, form)

    @app.websocket("/voice/media")
    async def media(websocket: WebSocket) -> None:
        settings = websocket.app.state.settings
        signature = websocket.headers.get("x-twilio-signature", "")
        if settings.validate_signatures and signature:
            signed = public_request_url(
                _ws_base(_base_from_settings(settings)),
                websocket.url.path,
                "",
            )
            if not valid_signature(signed, [], signature, settings.twilio_auth_token):
                await websocket.close(code=1008)
                return
        await websocket.accept()
        start = await _read_stream_start(websocket)
        token = ""
        started_call = ""
        if start:
            body = start.get("start") or {}
            started_call = str(body.get("callSid") or "")
            token = str((body.get("customParameters") or {}).get("token") or "")
        ok, call_sid, resume = valid_stream_token(token, settings.twilio_auth_token)
        if not ok or started_call != call_sid:
            await websocket.close(code=1008)
            return
        sessions: SessionStore = websocket.app.state.sessions
        session = sessions.get(call_sid)
        if session is None:
            session = sessions.put(
                CallSession(
                    call_sid=call_sid,
                    caller_number="",
                    dialed_number="",
                    line=LineContext(id="anata", label="Anata"),
                    resume=resume,
                )
            )
        elif resume:
            session.resume = resume
        bridge = CallBridge(
            settings=settings,
            session=session,
            store=websocket.app.state.store,
            twilio=websocket.app.state.twilio,
            calendar=websocket.app.state.calendar,
            websocket=websocket,
        )
        await bridge.run(start)

    return app


async def _sms_log_and_forward(request: Request, form: dict[str, str]) -> Response:
    """The pre-conversation path: log the text and forward one line to David."""

    settings = request.app.state.settings
    body = form.get("Body", "")
    caller = form.get("From", "")
    dialed = form.get("To", "")
    category = classify_sms(body)
    line = resolve_line(settings.line_contexts_json, dialed)
    message_sid = form.get("MessageSid", "")
    await asyncio.to_thread(
        request.app.state.store.save_sms,
        message_sid=message_sid,
        caller_number=caller,
        dialed_number=dialed,
        body=body,
        category=category,
        summary=body,
        sms_notified=False,
        line_id=line.id,
        lead_fields={"source": line.source} if line.source else {},
    )
    if settings.notify_sms_number and not _same_party(caller, settings.notify_sms_number):
        callback = ""
        base = _base(request)
        if base and message_sid:
            callback = f"{base}/voice/sms-status?call_sid={quote(message_sid, safe='')}"
        accepted = await asyncio.to_thread(
            request.app.state.twilio.send_sms,
            from_number=dialed,
            to_number=settings.notify_sms_number,
            body=sms_forward_text(
                from_number=caller,
                to_number=dialed,
                body=body,
                reply_sent=False,
                category=category,
                source=line.source,
            ),
            status_callback=callback,
        )
        if not accepted:
            logger.warning("Inbound SMS forward was not accepted")
    return Response(sms_twiml(""), media_type=XML)


async def _sms_conversation(request: Request, form: dict[str, str]) -> Response:
    """Reply from the number that was texted, and text David once when it matters."""

    settings = request.app.state.settings
    try:
        outcome = await asyncio.to_thread(
            handle_inbound_sms,
            settings=settings,
            store=request.app.state.store,
            threads=request.app.state.sms_threads,
            sender=form.get("From", ""),
            recipient=form.get("To", ""),
            body=form.get("Body", ""),
            message_sid=form.get("MessageSid", ""),
        )
    except Exception:
        logger.exception("Inbound SMS conversation failed")
        return Response(
            sms_twiml("Sorry, something went wrong. Email hello@anatainc.com and the team will help."),
            media_type=XML,
        )
    reply = outcome.reply
    if reply and outcome.to_number and not _same_party(outcome.to_number, settings.notify_sms_number):
        accepted = await asyncio.to_thread(
            request.app.state.twilio.send_sms,
            from_number=outcome.from_number,
            to_number=outcome.to_number,
            body=reply,
            pin_from=True,
        )
        if accepted:
            reply = ""
        else:
            logger.warning("Inbound SMS reply was not accepted")
    if (
        outcome.notify
        and outcome.session is not None
        and settings.notify_sms_number
        and not _same_party(outcome.session.caller_number, settings.notify_sms_number)
    ):
        callback = ""
        base = _base(request)
        if base and outcome.session.call_sid:
            callback = f"{base}/voice/sms-status?call_sid={quote(outcome.session.call_sid, safe='')}"
        accepted = await asyncio.to_thread(
            request.app.state.twilio.send_sms,
            from_number=outcome.from_number,
            to_number=settings.notify_sms_number,
            body=sms_attention_text(outcome.session),
            status_callback=callback,
        )
        if not accepted:
            logger.warning("Inbound SMS alert was not accepted")
    return Response(sms_twiml(reply), media_type=XML)


def _same_party(left: str, right: str) -> bool:
    first = normalize_phone(left)
    second = normalize_phone(right)
    return bool(first) and first == second


async def _read_stream_start(websocket: WebSocket, timeout: float = 8.0) -> dict[str, Any] | None:
    """Read Twilio's `start` message. The token arrives there, not on the URL."""

    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        remaining = deadline - loop.time()
        if remaining <= 0:
            return None
        try:
            incoming = await asyncio.wait_for(websocket.receive(), timeout=remaining)
        except asyncio.TimeoutError:
            return None
        if incoming.get("type") == "websocket.disconnect":
            return None
        raw = incoming.get("text")
        if not raw:
            continue
        try:
            message = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if message.get("event") == "start":
            return message


def _ws_base(http_base: str) -> str:
    if http_base.startswith("https://"):
        return "wss://" + http_base[len("https://") :]
    if http_base.startswith("http://"):
        return "ws://" + http_base[len("http://") :]
    return http_base


def _base_from_settings(settings: Settings) -> str:
    return settings.http_base()


def _base(request: Request) -> str:
    settings: Settings = request.app.state.settings
    return settings.http_base(str(request.base_url))


async def _signed_form(request: Request) -> tuple[dict[str, str], bool]:
    settings: Settings = request.app.state.settings
    pairs: list[tuple[str, str]] = []
    data: dict[str, str] = {}
    if request.method == "POST":
        form = await request.form()
        for key, value in form.multi_items():
            if getattr(value, "filename", None) is not None and not isinstance(value, str):
                continue
            text = str(value)
            pairs.append((str(key), text))
            data[str(key)] = text
    else:
        for key, value in request.query_params.multi_items():
            data[str(key)] = str(value)
    if not settings.validate_signatures:
        return data, True
    base = settings.http_base()
    if not base or not settings.twilio_auth_token:
        logger.warning("Rejecting a Twilio webhook because signature validation is not configured")
        return data, False
    url = public_request_url(base, request.url.path, request.url.query)
    signed_params = pairs if request.method == "POST" else []
    signature = request.headers.get("x-twilio-signature", "")
    return data, valid_signature(url, signed_params, signature, settings.twilio_auth_token)


def _parent(request: Request) -> str:
    return request.query_params.get("parent", "").strip()


def _parent_session(request: Request) -> CallSession | None:
    parent = _parent(request)
    if not parent:
        return None
    return request.app.state.sessions.get(parent)


async def _redirect_parent(request: Request, session: CallSession) -> None:
    twiml = resume_twiml(
        request.app.state.settings,
        http_base=_base(request),
        call_sid=session.call_sid,
    )
    await asyncio.to_thread(request.app.state.twilio.update_call, session.call_sid, twiml)


async def _drop_agent(request: Request, session: CallSession) -> None:
    if not session.agent_call_sid:
        return
    await asyncio.to_thread(request.app.state.twilio.complete_call, session.agent_call_sid)


async def _finish(request: Request, session: CallSession) -> None:
    await finalize_call(
        session,
        settings=request.app.state.settings,
        store=request.app.state.store,
        twilio=request.app.state.twilio,
    )


app = create_app()
