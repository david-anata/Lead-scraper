"""Inbound AI voice receptionist: Twilio webhook, media stream, admin call log.

All public endpoints return 404 unless VOICE_AGENT_ENABLED is on.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Request, WebSocket
from fastapi.responses import HTMLResponse, Response
from sqlalchemy import select

from sales_support_agent.models.database import session_scope
from sales_support_agent.models.voice import VoiceCall
from sales_support_agent.services.auth_deps import require_tool
from sales_support_agent.services.voice import twilio
from sales_support_agent.services.voice.bridge import CallBridge
from sales_support_agent.services.voice.config import load_voice_config
from sales_support_agent.services.voice.pages import BASE_PATH, render_call_detail, render_call_list
from sales_support_agent.services.voice.post_call import finalize_call

logger = logging.getLogger(__name__)

router = APIRouter(tags=["voice-agent"])
admin_router = APIRouter(prefix=BASE_PATH, tags=["voice-agent-admin"])

INBOUND_PATH = "/voice/twilio/inbound"
STREAM_PATH = "/voice/twilio/stream"


def _public_url(request: Request, path: str, base: str) -> str:
    if base:
        return f"{base}{path}"
    proto = request.headers.get("x-forwarded-proto", request.url.scheme)
    host = request.headers.get("x-forwarded-host", request.url.netloc)
    return f"{proto}://{host}{path}"


@router.post(INBOUND_PATH)
async def twilio_inbound(request: Request) -> Response:
    config = load_voice_config()
    if not config.enabled:
        raise HTTPException(status_code=404)
    form = await request.form()
    params = {key: str(value) for key, value in form.items()}
    url = _public_url(request, INBOUND_PATH, config.public_base_url)
    if not twilio.valid_signature(config.twilio_auth_token, url, params, request.headers.get("x-twilio-signature", "")):
        raise HTTPException(status_code=403, detail="invalid signature")
    if config.missing_for_calls():
        logger.error("voice agent enabled but missing %s", config.missing_for_calls())
        twiml = (
            twilio.transfer_twiml(config.transfer_number)
            if config.transfer_number
            else twilio.unavailable_twiml("Sorry, no one is available right now. Please call back later.")
        )
        return Response(content=twiml, media_type="application/xml")
    call_sid = params.get("CallSid", "")
    stream_url = _public_url(request, STREAM_PATH, config.public_base_url).replace("https://", "wss://", 1).replace("http://", "ws://", 1)
    twiml = twilio.connect_stream_twiml(
        stream_url,
        {
            "token": twilio.stream_token(config.twilio_auth_token, call_sid),
            "from": params.get("From", ""),
            "to": params.get("To", ""),
        },
    )
    return Response(content=twiml, media_type="application/xml")


@router.websocket(STREAM_PATH)
async def twilio_stream(websocket: WebSocket) -> None:
    config = load_voice_config()
    if not config.enabled:
        await websocket.close(code=1008)
        return
    await websocket.accept()
    bridge = CallBridge(
        config=config,
        session_factory=websocket.app.state.session_factory,
        twilio_ws=websocket,
        finalize=lambda ctx, outcome: finalize_call(ctx, outcome),
    )
    await bridge.run()


@admin_router.get("", response_class=HTMLResponse)
def voice_call_list(request: Request, user: dict = Depends(require_tool("sales.call"))) -> HTMLResponse:
    config = load_voice_config()
    with session_scope(request.app.state.session_factory) as session:
        calls = session.execute(select(VoiceCall).order_by(VoiceCall.started_at.desc()).limit(200)).scalars().all()
        page = render_call_list(calls, user=user, enabled=config.enabled, missing=config.missing_for_calls())
    return HTMLResponse(page)


@admin_router.get("/{call_id}", response_class=HTMLResponse)
def voice_call_detail(call_id: str, request: Request, user: dict = Depends(require_tool("sales.call"))) -> HTMLResponse:
    with session_scope(request.app.state.session_factory) as session:
        call = session.get(VoiceCall, call_id)
        if call is None:
            raise HTTPException(status_code=404)
        page = render_call_detail(call, user=user)
    return HTMLResponse(page)
