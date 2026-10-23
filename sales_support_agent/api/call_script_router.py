"""Live call script routes: /admin/sales/call.

Pages:
  GET  /admin/sales/call                 list of call trackers + start form
  POST /admin/sales/call                 start a tracker for a lead
  GET  /admin/sales/call/{id}            the call script with this lead's state
  POST /admin/sales/call/{id}/status     archive or restore a tracker
  GET  /admin/sales/call/slides/{file}   pitch deck slide images

JSON (used by the script's runtime adapter):
  PUT  /admin/sales/call/{id}/state      save the tracker (POST for sendBeacon)
  GET  /admin/sales/call/api/reviews     the rep's call reviews
  GET|PUT|DELETE /admin/sales/call/api/reviews/{review_id}
  POST /admin/sales/call/api/sample      Ask Claude

Live calls (see docs/LIVE_CALL_COACH_SPEC.md):
  POST /admin/sales/call/{id}/live              start or reuse a live session
  GET  /admin/sales/call/{id}/live/state        new lines + latest coach update
  POST /admin/sales/call/{id}/live/consent      log "Said it"
  POST /admin/sales/call/{id}/live/end          end the session, return transcript
  GET  /admin/sales/call/{id}/live/transcript   transcript of the latest session
  POST /admin/sales/call/{id}/after-call        ClickUp, recap draft, day-10 task

Every route needs the `sales.call` tool. Writes require a same-origin request
and the session-bound CSRF token (form field, X-CSRF-Token header, or the
`csrf` query parameter for sendBeacon).
"""

from __future__ import annotations

import json
import logging
from urllib.parse import quote, urlparse

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse

from sales_support_agent.models.database import session_scope
from sales_support_agent.services.auth_deps import get_current_user, require_tool
from sales_support_agent.services.sales.call_script import after_call
from sales_support_agent.services.sales.call_script import ai as call_ai
from sales_support_agent.services.sales.call_script import coach
from sales_support_agent.services.sales.call_script import live as live_svc
from sales_support_agent.services.sales.call_script import service
from sales_support_agent.services.sales.call_script.pages import render_call_list_page
from sales_support_agent.services.sales.security import csrf_token, valid_csrf_token

logger = logging.getLogger(__name__)

TOOL_KEY = "sales.call"

router = APIRouter(
    prefix=service.BASE_PATH,
    tags=["sales-call-script"],
    dependencies=[Depends(require_tool(TOOL_KEY))],
)


def _same_origin(request: Request) -> None:
    if (request.headers.get("sec-fetch-site") or "").lower() == "cross-site":
        raise HTTPException(status_code=403, detail="Cross-site request rejected.")
    origin = request.headers.get("origin")
    if origin and urlparse(origin).netloc.lower() != request.url.netloc.lower():
        raise HTTPException(status_code=403, detail="Request origin does not match.")


def _is_browser_request(request: Request) -> bool:
    # Same rule as the other Sales forms: browsers always send Origin or
    # Sec-Fetch-Mode on writes, so those requests must carry the token.
    return bool(request.headers.get("origin") or request.headers.get("sec-fetch-mode"))


async def require_form_token(request: Request) -> None:
    _same_origin(request)
    if not _is_browser_request(request):
        return
    form = await request.form()
    if not valid_csrf_token(get_current_user(request), str(form.get("_csrf_token") or "")):
        raise HTTPException(status_code=403, detail="Security token is invalid. Reload the page.")


async def require_json_token(request: Request) -> None:
    _same_origin(request)
    if not _is_browser_request(request):
        return
    supplied = request.headers.get("x-csrf-token") or request.query_params.get("csrf") or ""
    if not valid_csrf_token(get_current_user(request), supplied):
        raise HTTPException(status_code=403, detail="Security token is invalid. Reload the page.")


async def _json_body(request: Request) -> dict:
    try:
        body = json.loads((await request.body()) or b"{}")
    except (ValueError, UnicodeDecodeError):
        raise HTTPException(status_code=400, detail="Body must be JSON.")
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Body must be a JSON object.")
    return body


def _factory(request: Request):
    return request.app.state.session_factory


# ---------------------------------------------------------------- pages


@router.get("", response_class=HTMLResponse)
@router.get("/", response_class=HTMLResponse)
def call_list(request: Request) -> HTMLResponse:
    user = get_current_user(request)
    show_archived = request.query_params.get("archived") == "1"
    with session_scope(_factory(request)) as session:
        trackers = service.list_trackers(session, user, include_archived=show_archived)
        if show_archived:
            trackers = [t for t in trackers if t.status == service.STATUS_ARCHIVED]
        summaries = [service.summarize(t) for t in trackers]
    return HTMLResponse(
        render_call_list_page(
            user=user,
            trackers=summaries,
            csrf=csrf_token(user),
            show_archived=show_archived,
            ai_enabled=call_ai.ai_enabled(),
            error=request.query_params.get("error", "")[:200],
        )
    )


@router.post("", dependencies=[Depends(require_form_token)])
@router.post("/", dependencies=[Depends(require_form_token)], include_in_schema=False)
async def start_call(request: Request):
    user = get_current_user(request)
    form = await request.form()
    try:
        with session_scope(_factory(request)) as session:
            tracker = service.create_tracker(
                session,
                user,
                prospect_name=str(form.get("prospect_name") or ""),
                company=str(form.get("company") or ""),
                lead_link=str(form.get("lead_link") or ""),
            )
            tracker_id = tracker.id
    except service.CallScriptError as exc:
        return RedirectResponse(
            url=f"{service.BASE_PATH}?error={quote(str(exc))}", status_code=303
        )
    return RedirectResponse(url=f"{service.BASE_PATH}/{tracker_id}", status_code=303)


# Static slide route must be declared before /{tracker_id}.
@router.get("/slides/{name}")
def slide(name: str) -> FileResponse:
    path = service.slide_path(name)
    if path is None:
        raise HTTPException(status_code=404, detail="Slide not found.")
    return FileResponse(path, media_type="image/jpeg", headers={"Cache-Control": "private, max-age=86400"})


# ---------------------------------------------------------------- reviews API


@router.get("/api/reviews")
def reviews(request: Request) -> JSONResponse:
    user = get_current_user(request)
    with session_scope(_factory(request)) as session:
        return JSONResponse({"reviews": service.list_reviews(session, user)})


@router.get("/api/reviews/{review_id}")
def review_get(review_id: str, request: Request) -> JSONResponse:
    user = get_current_user(request)
    with session_scope(_factory(request)) as session:
        return JSONResponse({"review": service.get_review(session, user, review_id)})


@router.put("/api/reviews/{review_id}", dependencies=[Depends(require_json_token)])
async def review_put(review_id: str, request: Request) -> JSONResponse:
    user = get_current_user(request)
    body = await _json_body(request)
    try:
        with session_scope(_factory(request)) as session:
            saved = service.upsert_review(
                session, user, review_id, body.get("review"), str(body.get("trackerId") or "")
            )
    except service.CallScriptError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return JSONResponse({"review": saved})


@router.delete("/api/reviews/{review_id}", dependencies=[Depends(require_json_token)])
def review_delete(review_id: str, request: Request) -> JSONResponse:
    user = get_current_user(request)
    with session_scope(_factory(request)) as session:
        deleted = service.delete_review(session, user, review_id)
    return JSONResponse({"deleted": deleted})


@router.post("/api/sample", dependencies=[Depends(require_json_token)])
async def sample(request: Request) -> JSONResponse:
    body = await _json_body(request)
    tier = "quick" if body.get("tier") == "quick" else "default"
    try:
        result = call_ai.run_sample(body.get("input"), tier=tier, want_json=bool(body.get("json")))
    except call_ai.SampleError as exc:
        raise HTTPException(status_code=exc.status, detail=str(exc))
    return JSONResponse({"text": result.text, "truncated": result.truncated})


# ---------------------------------------------------------------- one tracker


@router.get("/{tracker_id}", response_class=HTMLResponse)
def call_page(tracker_id: str, request: Request) -> HTMLResponse:
    user = get_current_user(request)
    with session_scope(_factory(request)) as session:
        tracker = service.get_tracker(session, user, tracker_id)
        if tracker is None:
            raise HTTPException(status_code=404, detail="Call not found.")
        page = service.render_call_page(
            tracker, user, csrf=csrf_token(user), ai_enabled=call_ai.ai_enabled()
        )
    return HTMLResponse(page, headers={"Cache-Control": "no-store"})


async def _save_state(tracker_id: str, request: Request) -> JSONResponse:
    user = get_current_user(request)
    body = await _json_body(request)
    try:
        with session_scope(_factory(request)) as session:
            tracker = service.get_tracker(session, user, tracker_id)
            if tracker is None:
                raise HTTPException(status_code=404, detail="Call not found.")
            service.save_state(session, tracker, body.get("state"))
            saved_at = tracker.updated_at.isoformat()
    except service.CallScriptError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return JSONResponse({"saved": True, "updatedAt": saved_at})


@router.put("/{tracker_id}/state", dependencies=[Depends(require_json_token)])
async def put_state(tracker_id: str, request: Request) -> JSONResponse:
    return await _save_state(tracker_id, request)


@router.post("/{tracker_id}/state", dependencies=[Depends(require_json_token)])
async def beacon_state(tracker_id: str, request: Request) -> JSONResponse:
    return await _save_state(tracker_id, request)


@router.post("/{tracker_id}/status", dependencies=[Depends(require_form_token)])
async def set_status(tracker_id: str, request: Request):
    user = get_current_user(request)
    form = await request.form()
    status = str(form.get("status") or "")
    try:
        with session_scope(_factory(request)) as session:
            tracker = service.get_tracker(session, user, tracker_id)
            if tracker is None:
                raise HTTPException(status_code=404, detail="Call not found.")
            service.set_status(session, tracker, status)
    except service.CallScriptError as exc:
        return RedirectResponse(url=f"{service.BASE_PATH}?error={quote(str(exc))}", status_code=303)
    suffix = "?archived=1" if status == service.STATUS_OPEN else ""
    return RedirectResponse(url=f"{service.BASE_PATH}{suffix}", status_code=303)


# ---------------------------------------------------------------- live calls


def _agent_settings(request: Request):
    settings = getattr(request.app.state, "agent_settings", None)
    if settings is not None and hasattr(settings, "clickup_api_token"):
        return settings
    from sales_support_agent.config import load_settings

    settings = load_settings()
    request.app.state.agent_settings = settings
    return settings


def _clickup_client(settings):
    from sales_support_agent.integrations.clickup import ClickUpClient

    return ClickUpClient(settings)


def _gmail_client(settings):
    from sales_support_agent.integrations.gmail import GmailClient

    return GmailClient(settings)


def _tracker_or_404(session, user, tracker_id):
    tracker = service.get_tracker(session, user, tracker_id)
    if tracker is None:
        raise HTTPException(status_code=404, detail="Call not found.")
    return tracker


def _live_error(exc: live_svc.LiveError) -> HTTPException:
    return HTTPException(status_code=exc.status, detail=str(exc))


@router.post("/{tracker_id}/live", dependencies=[Depends(require_json_token)])
async def live_start(tracker_id: str, request: Request) -> JSONResponse:
    user = get_current_user(request)
    body = await _json_body(request)
    try:
        with session_scope(_factory(request)) as session:
            tracker = _tracker_or_404(session, user, tracker_id)
            live = live_svc.start_session(
                session,
                tracker,
                user,
                coach_context=str(body.get("coachContext") or ""),
                label=str(body.get("label") or ""),
            )
            token, exp = live_svc.sign_token(live.id, tracker.id, live.owner_email)
            payload = {
                "sessionId": live.id,
                "token": token,
                "relayUrl": live_svc.relay_url(),
                "expiresAt": exp,
                "startedAt": live_svc.iso(live.started_at),
                "status": live.status,
            }
    except live_svc.LiveError as exc:
        raise _live_error(exc)
    return JSONResponse(payload)


@router.get("/{tracker_id}/live/state")
def live_state(tracker_id: str, request: Request) -> JSONResponse:
    user = get_current_user(request)
    try:
        after = int(request.query_params.get("after") or 0)
        rev = int(request.query_params.get("rev") or 0)
    except ValueError:
        raise HTTPException(status_code=400, detail="after and rev must be numbers.")
    with session_scope(_factory(request)) as session:
        tracker = _tracker_or_404(session, user, tracker_id)
        live = live_svc.latest_session_for(session, tracker.id)
        if live is None:
            return JSONResponse({"session": None, "lines": [], "lastLineId": 0, "coach": None, "coachRunning": False})
        if live_svc.coach_due(session, live):
            coach.run_pass(session, live, tracker)
        lines = live_svc.lines_after(session, live.id, after)
        event = live_svc.latest_coach_event(session, live.id)
        lease = live_svc.aware(live.coach_lease_until)
        payload = {
            "session": live_svc.session_json(live),
            "lines": [live_svc.line_json(r) for r in lines],
            "lastLineId": lines[-1].id if lines else max(after, 0),
            "coach": (
                {"rev": event.rev, "patch": event.patch_json, "createdAt": live_svc.iso(event.created_at)}
                if event is not None and event.rev > rev
                else None
            ),
            "coachRunning": bool(lease and lease > live_svc.now_utc()),
        }
    return JSONResponse(payload, headers={"Cache-Control": "no-store"})


@router.post("/{tracker_id}/live/consent", dependencies=[Depends(require_json_token)])
def live_consent(tracker_id: str, request: Request) -> JSONResponse:
    user = get_current_user(request)
    with session_scope(_factory(request)) as session:
        tracker = _tracker_or_404(session, user, tracker_id)
        live = live_svc.open_session_for(session, tracker.id)
        if live is None:
            raise HTTPException(status_code=409, detail="No live call is running.")
        at = live_svc.mark_consent(session, live)
    return JSONResponse({"consentAt": at.isoformat()})


@router.post("/{tracker_id}/live/end", dependencies=[Depends(require_json_token)])
def live_end(tracker_id: str, request: Request) -> JSONResponse:
    user = get_current_user(request)
    with session_scope(_factory(request)) as session:
        tracker = _tracker_or_404(session, user, tracker_id)
        live = live_svc.latest_session_for(session, tracker.id)
        if live is None:
            raise HTTPException(status_code=409, detail="No live call to end.")
        ended = live_svc.end_session(session, live)
        transcript = live_svc.transcript_text(live_svc.all_lines(session, live.id))
    return JSONResponse({"endedAt": ended.isoformat(), "transcript": transcript})


@router.get("/{tracker_id}/live/transcript")
def live_transcript(tracker_id: str, request: Request) -> JSONResponse:
    user = get_current_user(request)
    with session_scope(_factory(request)) as session:
        tracker = _tracker_or_404(session, user, tracker_id)
        live = live_svc.latest_session_for(session, tracker.id)
        if live is None:
            return JSONResponse({"transcript": "", "lines": []})
        rows = live_svc.all_lines(session, live.id)
        payload = {"transcript": live_svc.transcript_text(rows), "lines": [live_svc.line_json(r) for r in rows]}
    return JSONResponse(payload)


@router.post("/{tracker_id}/after-call", dependencies=[Depends(require_json_token)])
async def live_after_call(tracker_id: str, request: Request) -> JSONResponse:
    user = get_current_user(request)
    body = await _json_body(request)
    settings = _agent_settings(request)
    with session_scope(_factory(request)) as session:
        tracker = _tracker_or_404(session, user, tracker_id)
        live = live_svc.latest_session_for(session, tracker.id)
        if live is None:
            raise HTTPException(status_code=409, detail="No live call on this lead yet.")
        try:
            steps = after_call.run_after_call(
                session,
                live,
                tracker,
                body.get("analysis"),
                str(body.get("offer") or "none"),
                clickup_client=_clickup_client(settings),
                gmail_client=_gmail_client(settings),
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
    return JSONResponse({"steps": steps})
