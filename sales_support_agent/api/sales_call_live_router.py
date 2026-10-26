"""Public live-call endpoints for Tape's relay: /api/sales-call/live/*.

No login. Every request carries the agent-signed live token as
`Authorization: Bearer <token>`; the token names the session it is for and
expires with it.
"""

from __future__ import annotations

import json

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from sales_support_agent.models.database import session_scope
from sales_support_agent.services.sales.call_script import live as live_svc

router = APIRouter(prefix="/api/sales-call/live", tags=["sales-call-live"])


def _token(request: Request) -> str:
    token = live_svc.bearer(request.headers.get("authorization"))
    if not token:
        raise HTTPException(status_code=401, detail="Missing live token.")
    return token


async def _body(request: Request) -> dict:
    try:
        body = json.loads((await request.body()) or b"{}")
    except (ValueError, UnicodeDecodeError):
        raise HTTPException(status_code=400, detail="Body must be JSON.")
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Body must be a JSON object.")
    return body


@router.post("/{session_id}/verify")
def verify(session_id: str, request: Request) -> JSONResponse:
    token = _token(request)
    try:
        with session_scope(request.app.state.session_factory) as session:
            live = live_svc.session_for_token(session, session_id, token)
            payload = {"ok": True, "trackerId": live.tracker_id, "keyterms": list(live_svc.KEYTERMS)}
    except live_svc.LiveError as exc:
        raise HTTPException(status_code=exc.status, detail=str(exc))
    return JSONResponse(payload)


@router.post("/{session_id}/lines")
async def lines(session_id: str, request: Request) -> JSONResponse:
    token = _token(request)
    body = await _body(request)
    try:
        parsed = live_svc.parse_lines(body.get("lines"))
        with session_scope(request.app.state.session_factory) as session:
            live = live_svc.session_for_token(session, session_id, token)
            last_id = live_svc.add_lines(session, live, parsed)
    except live_svc.LiveError as exc:
        raise HTTPException(status_code=exc.status, detail=str(exc))
    return JSONResponse({"ok": True, "lastLineId": last_id})


@router.post("/{session_id}/ended")
def ended(session_id: str, request: Request) -> JSONResponse:
    token = _token(request)
    try:
        live_svc.verify_token(token, session_id)
        with session_scope(request.app.state.session_factory) as session:
            live = live_svc.get_session(session, session_id)
            if live is None:
                raise live_svc.LiveError("Live session not found.", 404)
            live_svc.mark_tape_ended(session, live)
    except live_svc.LiveError as exc:
        raise HTTPException(status_code=exc.status, detail=str(exc))
    return JSONResponse({"ok": True})
