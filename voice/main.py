"""FastAPI app for the Anata phone receptionist.

Webhook paths, all POST unless noted:

- `POST /voice/inbound` — Twilio Voice webhook
- `POST /voice/dial-result` — `<Dial action>` when the forward ends
- `POST /voice/whisper` — spoken to the on-call phone before it is connected
- `GET  /voice/media` — Twilio Media Stream websocket
- `POST /voice/sms` — Twilio Messaging webhook
- `GET  /health` — Render health check
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import PlainTextResponse, Response

from voice.bridge import CallBridge, whisper_line
from voice.calendar_availability import CalendarAvailability
from voice.logging_store import LoggingStore
from voice.lifecycle import finalize_call
from voice.lines import LineContext, resolve_line
from voice.notify import TwilioRest, sms_forward_text
from voice.security import public_request_url, valid_signature, valid_stream_token
from voice.session import CallSession, SessionStore
from voice.settings import Settings
from voice.sms_reply import classify_sms, reply_to_sms
from voice.twiml import (
    empty_twiml,
    inbound_twiml,
    resume_twiml,
    sms_twiml,
    whisper_twiml,
)

logger = logging.getLogger(__name__)

XML = "application/xml"


def create_app(settings: Settings | None = None) -> FastAPI:
    logging.basicConfig(level=logging.INFO)
    current = settings or Settings.from_env()
    app = FastAPI(title="Anata voice receptionist", docs_url=None, redoc_url=None)
    app.state.settings = current
    app.state.sessions = SessionStore()
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

    @app.get("/", response_class=PlainTextResponse)
    def root() -> str:
        return "Anata voice receptionist"

    @app.get("/health")
    def health() -> dict[str, Any]:
        live = app.state.settings
        return {
            "status": "ok",
            "service": "anata-voice-receptionist",
            "calls_ready": live.calls_ready,
            "logging_ready": live.logging_ready,
            "calendar_ready": live.calendar_ready,
            "sms_autoreply": live.sms_autoreply,
            "recording_enabled": live.recording_enabled,
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
            request.app.state.sessions.put(
                CallSession(
                    call_sid=call_sid,
                    caller_number=caller,
                    dialed_number=dialed,
                    line=line,
                )
            )
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
        try:
            duration = int(form.get("DialCallDuration") or "0")
        except ValueError:
            duration = 0
        if status == "completed" and duration > 0:
            session.transfer_state = "done"
            session.transfer_outcome = "answered"
            session.action = "transferred"
            session.callback_needed = False
            await _finish(request, session)
            return Response(empty_twiml(), media_type=XML)
        if call_status == "completed":
            session.transfer_state = "done"
            session.transfer_outcome = status or "missed"
            session.action = "transfer_missed"
            session.callback_needed = True
            await _finish(request, session)
            return Response(empty_twiml(), media_type=XML)
        session.transfer_state = "messaging"
        session.transfer_outcome = status or "no_answer"
        session.resume = "message"
        session.transfer_requested = False
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
        parent = form.get("ParentCallSid") or form.get("CallSid") or ""
        session = request.app.state.sessions.get(parent)
        text = whisper_line(session) if session is not None else "Anata transfer. Connecting you now."
        return Response(whisper_twiml(text), media_type=XML)

    @app.api_route("/voice/sms", methods=["GET", "POST"])
    async def sms(request: Request) -> Response:
        form, ok = await _signed_form(request)
        if not ok:
            return Response(status_code=403)
        settings = request.app.state.settings
        body = form.get("Body", "")
        caller = form.get("From", "")
        dialed = form.get("To", "")
        category = classify_sms(body)
        reply = reply_to_sms(body) if settings.sms_autoreply else ""
        notified = False
        if settings.notify_sms_number:
            notified = await asyncio.to_thread(
                request.app.state.twilio.send_sms,
                from_number=dialed,
                to_number=settings.notify_sms_number,
                body=sms_forward_text(
                    from_number=caller,
                    to_number=dialed,
                    body=body,
                    reply_sent=bool(reply),
                    category=category,
                ),
            )
        await asyncio.to_thread(
            request.app.state.store.save_sms,
            message_sid=form.get("MessageSid", ""),
            caller_number=caller,
            dialed_number=dialed,
            body=body,
            category=category,
            summary=body[:500],
            sms_notified=notified,
        )
        return Response(sms_twiml(reply), media_type=XML)

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


async def _finish(request: Request, session: CallSession) -> None:
    await finalize_call(
        session,
        settings=request.app.state.settings,
        store=request.app.state.store,
        twilio=request.app.state.twilio,
    )


app = create_app()
