"""Public front-desk review page. No login."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from starlette.concurrency import run_in_threadpool

from sales_support_agent.models.database import session_scope
from sales_support_agent.models.entities import BuildingVisitorReview
from sales_support_agent.services import building_visitor_reviews as reviews
from sales_support_agent.services.building_review_page import (
    render_choose,
    render_feedback_form,
    render_google,
    render_limited,
    render_thanks,
    render_too_large,
)
from sales_support_agent.services.public_request_guard import durable_rate_limited

logger = logging.getLogger(__name__)

router = APIRouter(tags=["building-front-desk-review"])

_MAX_BODY_BYTES = 20_000
_HONEYPOT_FIELDS = ("hp_company", "company_website", "website", "hp", "website_hp")
_NO_STORE = {
    "Cache-Control": "no-store",
    "X-Robots-Tag": "noindex",
    "Referrer-Policy": "strict-origin-when-cross-origin",
}


def _html(body: str, status: int = 200, **headers: str) -> HTMLResponse:
    return HTMLResponse(body, status_code=status, headers={**_NO_STORE, **headers})


def _text(form: Any, key: str) -> str:
    value = form.get(key)
    if value is None or hasattr(value, "filename"):
        return ""
    return str(value)


def _honeypot(form: Any) -> bool:
    return any(_text(form, key).strip() for key in _HONEYPOT_FIELDS)


def _too_large(request: Request) -> bool:
    declared = request.headers.get("content-length", "").strip()
    if not declared:
        return False
    try:
        return int(declared) > _MAX_BODY_BYTES
    except ValueError:
        return True


@router.get("/review", response_class=HTMLResponse)
@router.get("/building/review", response_class=HTMLResponse)
def review_page() -> HTMLResponse:
    """Star picker for the front-desk QR code."""

    return _html(render_choose())


@router.post("/review", response_class=HTMLResponse)
@router.post("/building/review", response_class=HTMLResponse)
async def submit_review(request: Request) -> HTMLResponse:
    """Record a star tap, or the private note that follows a low score."""

    if _too_large(request):
        return _html(render_too_large(), status=413)
    form = await request.form()
    feedback = _text(form, "step") == "feedback" or bool(_text(form, "review_id").strip())
    scope = "building-review:feedback" if feedback else "building-review:star"
    limit = reviews.FEEDBACK_RATE_LIMIT if feedback else reviews.STAR_RATE_LIMIT
    if durable_rate_limited(
        request,
        scope=scope,
        limit=limit,
        window_seconds=reviews.RATE_WINDOW_SECONDS,
        trust_supplied_client_key=False,
    ):
        return _html(
            render_limited(),
            status=429,
            **{"Retry-After": str(reviews.RATE_WINDOW_SECONDS)},
        )
    if _honeypot(form):
        return _html(render_thanks())
    # Slack and email use blocking HTTP. Keep that off the event loop, and
    # only after the note itself has been committed.
    if feedback:
        return await run_in_threadpool(_save_feedback, request, form)
    return await run_in_threadpool(_save_star, request, form)


def _save_star(request: Request, form: Any) -> HTMLResponse:
    stars = reviews.parse_stars(_text(form, "stars"))
    if stars is None:
        return _html(
            render_choose(error="Choose a star from 1 to 5."),
            status=400,
        )
    with session_scope(request.app.state.session_factory) as session:
        row = reviews.record_star(session, stars)
        review_id = row.id
    if reviews.is_public_google_score(stars):
        return _html(render_google())
    return _html(render_feedback_form(review_id=review_id, stars=stars))


def _form_values(form: Any) -> dict[str, Any]:
    reasons = [
        str(item)
        for item in form.getlist("visit_reason")
        if not hasattr(item, "filename")
    ]
    return {
        "review_id": _text(form, "review_id").strip(),
        "stars": reviews.parse_stars(_text(form, "stars")),
        "comment": _text(form, "comment"),
        "visitor_name": _text(form, "visitor_name"),
        "contact": _text(form, "contact"),
        "reasons": reviews.clean_reasons(reasons),
    }


def _deliver_feedback_alert(request: Request, review_id: str) -> None:
    """Send the staff alert after the note is committed.

    The provider calls block. They run in a worker thread, and they no longer
    hold the database transaction open while Slack or email responds.
    """

    failed = {
        "slack": "failed",
        "email": "failed",
        "slack_reference": "",
        "email_reference": "",
    }
    try:
        with session_scope(request.app.state.session_factory) as session:
            alert_row = session.get(BuildingVisitorReview, review_id)
            if alert_row is None:
                return
            session.expunge(alert_row)
        result = reviews.notify_visitor_feedback(request.app.state.settings, alert_row)
    except Exception:
        logger.exception("Front desk review alert failed for %s", review_id)
        result = failed
    try:
        with session_scope(request.app.state.session_factory) as session:
            reviews.record_notification(session, review_id, result)
    except Exception:
        logger.exception("Front desk review alert status was not saved for %s", review_id)


def _save_feedback(request: Request, form: Any) -> HTMLResponse:
    values = _form_values(form)
    stars = values["stars"]
    comment, comment_error = reviews.clean_text(
        values["comment"], limit=reviews.MAX_COMMENT_LENGTH
    )
    visitor_name, name_error = reviews.clean_text(
        values["visitor_name"], limit=reviews.MAX_NAME_LENGTH
    )
    contact, contact_error = reviews.clean_contact(values["contact"])
    shown_comment = (
        str(values["comment"] or "")[: reviews.MAX_COMMENT_LENGTH]
        if comment_error
        else comment
    )
    shown_name = (
        str(values["visitor_name"] or "")[: reviews.MAX_NAME_LENGTH]
        if name_error
        else visitor_name
    )
    shown_contact = (
        str(values["contact"] or "")[: reviews.MAX_CONTACT_LENGTH]
        if contact_error
        else contact
    )
    error = ""
    if stars is None:
        error = "Choose a star from 1 to 5."
    elif name_error:
        error = name_error
    elif contact_error:
        error = contact_error
    elif comment_error:
        error = comment_error
    elif stars is not None and not reviews.is_public_google_score(stars) and not comment:
        error = "Tell us what we could do better. A sentence is plenty."
    if error:
        return _html(
            render_feedback_form(
                review_id=str(values["review_id"]),
                stars=stars or 1,
                comment=shown_comment,
                visitor_name=shown_name,
                contact=shown_contact,
                reasons=list(values["reasons"]),
                error=error,
            ),
            status=422,
        )

    should_alert = False
    review_id = str(values["review_id"])
    with session_scope(request.app.state.session_factory) as session:
        row = reviews.load_review(session, review_id)
        if row is None:
            row = reviews.record_star(session, int(stars))
        should_alert = reviews.save_private_feedback(
            session,
            row,
            stars=int(stars),
            comment=comment,
            visitor_name=visitor_name,
            contact=contact,
            reasons=list(values["reasons"]),
        )
        review_id = row.id
    if should_alert:
        _deliver_feedback_alert(request, review_id)
    if reviews.is_public_google_score(int(stars)):
        return _html(render_google())
    return _html(render_thanks())
