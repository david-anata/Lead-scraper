"""Build call prep for a booked sales appointment and store it on the tracker.

One tracker per lead, the same `create_tracker` path the Call Script page uses.
Notes live in `state_json["ss-prep"]`. Facts come from sources the app can
already read. Anything missing is marked Unknown. The model is not allowed
to invent a number, a client, or a pain.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from sales_support_agent.models.entities import SalesCallTracker
from sales_support_agent.services.sales.call_script import service as call_service
from sales_support_agent.services.sales.call_script.after_call import (
    FIELD_COMPANY,
    FIELD_EMAIL,
    FIELD_NAME,
    agent_url,
    list_id,
)
from sales_support_agent.services.sales.call_script.bookings import LOOKAHEAD, LOOKBACK, Booking

logger = logging.getLogger(__name__)

DAVID_EMAIL = "david@anatainc.com"
DAVID_USER = {"email": DAVID_EMAIL, "name": "David Narayan", "is_superadmin": True}
SALES_SLACK_CHANNEL = "CQRMXS339"  # #sales
DENVER = ZoneInfo("America/Denver")
PITCH_POINTS = (
    "Free 14-day trial.",
    "Plans from $99/mo.",
    "Zantrex proof: ROAS went from 2x to 3.5x.",
)
_DASHES = re.compile(r"[‒–—―]")
_FREEMAIL = {
    "gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "icloud.com",
    "aol.com", "proton.me", "protonmail.com", "live.com", "me.com",
}
_FIRST = {
    "nick": "nicholas", "nicky": "nicholas", "nicolas": "nicholas",
    "nicholas": "nicholas", "nic": "nicholas",
    "rob": "robert", "robert": "robert", "bob": "robert",
    "bill": "william", "william": "william", "will": "william",
    "mike": "michael", "michael": "michael",
    "dave": "david", "david": "david",
}


def no_em_dash(value: str) -> str:
    return _DASHES.sub("-", value or "")


def _walk_dashes(value: Any) -> Any:
    if isinstance(value, str):
        return no_em_dash(value)
    if isinstance(value, list):
        return [_walk_dashes(item) for item in value]
    if isinstance(value, dict):
        return {key: _walk_dashes(item) for key, item in value.items()}
    return value


def names_match(left: str, right: str) -> bool:
    """True when two prospect labels are the same person, including Nick and Nicolas."""

    def parts(value: str) -> list[str]:
        return [part for part in re.split(r"\s+", (value or "").strip().lower()) if part]

    left_parts, right_parts = parts(left), parts(right)
    if len(left_parts) < 2 or len(right_parts) < 2:
        return False
    if left_parts[-1].strip(".") != right_parts[-1].strip("."):
        return False
    first_left = _FIRST.get(left_parts[0].strip("."), left_parts[0].strip("."))
    first_right = _FIRST.get(right_parts[0].strip("."), right_parts[0].strip("."))
    if first_left == first_right:
        return True
    shorter, longer = sorted((first_left, first_right), key=len)
    return len(shorter) >= 4 and longer.startswith(shorter)


def in_window(booking: Booking, now: datetime) -> bool:
    if booking.starts_at is None:
        return True
    start = booking.starts_at
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    return now - LOOKBACK <= start <= now + LOOKAHEAD


def format_when(moment: Optional[datetime]) -> str:
    if moment is None:
        return "Unknown"
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    local = moment.astimezone(DENVER)
    hour = int(local.strftime("%I"))
    return f"{local.strftime('%a %b')} {local.day}, {hour}:{local.strftime('%M %p')} MT"


def tracker_url(tracker_id: str) -> str:
    return f"{agent_url()}/admin/sales/call/{tracker_id}"


def _field(task: dict, field_id: str) -> Any:
    for field in task.get("custom_fields") or []:
        if isinstance(field, dict) and field.get("id") == field_id:
            return field.get("value")
    return None


def _domain(email: str) -> str:
    if "@" not in (email or ""):
        return ""
    host = email.rsplit("@", 1)[1].strip().lower()
    if host in _FREEMAIL or host == "anatainc.com":
        return ""
    return host


def _query(session: Optional[Session], sql: str, params: dict) -> tuple[bool, list[dict]]:
    """Read one source inside a savepoint so a missing table does not abort prep."""

    if session is None:
        return False, []
    try:
        with session.begin_nested():
            result = session.execute(text(sql), params)
            columns = list(result.keys())
            rows = [dict(zip(columns, row)) for row in result.fetchall()]
        return True, rows
    except Exception as exc:  # missing table or a down database
        logger.info("[call-prep] source query failed: %s", type(exc).__name__)
        return False, []


def _clickup_task(client: Any, booking: Booking) -> dict:
    if client is None or not getattr(client, "get_tasks", None):
        return {"available": False, "matched": False}
    email = (booking.prospect_email or "").lower()
    try:
        for page in range(3):
            tasks = client.get_tasks(list_id(), include_closed=True, page=page) or []
            for task in tasks:
                if not isinstance(task, dict):
                    continue
                task_email = str(_field(task, FIELD_EMAIL) or "").strip().lower()
                person = str(_field(task, FIELD_NAME) or "")
                title = str(task.get("name") or "")
                company = str(_field(task, FIELD_COMPANY) or "")
                email_hit = bool(email and task_email == email)
                name_hit = names_match(booking.prospect_name, title) or (
                    bool(person) and names_match(booking.prospect_name, person)
                )
                if not email_hit and not name_hit:
                    continue
                task_id = str(task.get("id") or "")
                return {
                    "available": True,
                    "matched": True,
                    "id": task_id,
                    "name": title,
                    "status": str((task.get("status") or {}).get("status") or task.get("status") or ""),
                    "company": company,
                    "email": task_email,
                    "person": person,
                    "url": str(task.get("url") or (f"https://app.clickup.com/t/{task_id}" if task_id else "")),
                }
            if len(tasks) < 100:
                break
    except Exception as exc:
        logger.info("[call-prep] clickup lookup failed: %s", type(exc).__name__)
        return {"available": False, "matched": False, "error": type(exc).__name__}
    return {"available": True, "matched": False}


def fetch_site(url: str) -> str:
    """Public homepage text, or an empty string when the fetch is unsafe or down."""

    from sales_support_agent.services.public_url_guard import public_http_url

    safe = public_http_url(url)
    if not safe:
        return ""
    try:
        import requests

        response = requests.get(
            safe,
            timeout=8,
            headers={"User-Agent": "Mozilla/5.0 (compatible; AnataCallPrep/1.0)"},
            allow_redirects=False,
        )
        if response.status_code in {301, 302, 303, 307, 308}:
            from urllib.parse import urljoin

            nxt = public_http_url(urljoin(safe, str(response.headers.get("location") or "")))
            if not nxt:
                return ""
            response = requests.get(
                nxt,
                timeout=8,
                headers={"User-Agent": "Mozilla/5.0 (compatible; AnataCallPrep/1.0)"},
                allow_redirects=False,
            )
        if not response.ok:
            return ""
        from sales_support_agent.services.sales.call_script.bookings import html_to_text

        return " ".join(html_to_text(response.text or "").split())[:3000]
    except Exception as exc:
        logger.info("[call-prep] website fetch failed: %s", type(exc).__name__)
        return ""


def gather_context(session: Optional[Session], clickup: Any, booking: Booking, *, fetch_url: Callable[[str], str] = fetch_site) -> dict:
    """Facts already on file for this prospect. Missing sources stay explicit."""

    email = (booking.prospect_email or "").lower()
    domain = _domain(email)
    contact_ok, contacts = _query(
        session,
        "SELECT email, first_name, last_name, company, linkedin_url, state, reason "
        "FROM outbound_contacts WHERE lower(email) = :e LIMIT 1",
        {"e": email},
    ) if email else (True, [])
    person_ok, people = _query(
        session,
        "SELECT email, domain, first_name, last_name, title, brand, category, recipe, tier, opener "
        "FROM outbound_enriched_contacts WHERE lower(email) = :e LIMIT 1",
        {"e": email},
    ) if email else (True, [])
    domain_ok, domains = _query(
        session,
        "SELECT domain, brand, niche, tier, signals, recipe, reason, categories "
        "FROM outbound_contacted_domains WHERE lower(domain) = :d LIMIT 1",
        {"d": domain},
    ) if domain else (True, [])
    abby_ok, abby_rows = _query(
        session,
        "SELECT category, action, detail, created_at FROM abby_handled "
        "WHERE lower(lead) = :e ORDER BY created_at DESC LIMIT 8",
        {"e": email},
    ) if email else (True, [])
    website = fetch_url(f"https://{domain}") if domain else ""
    clickup_task = _clickup_task(clickup, booking)
    company = ""
    if clickup_task.get("company"):
        company = str(clickup_task["company"])
    elif contacts and contacts[0].get("company"):
        company = str(contacts[0]["company"])
    elif people and people[0].get("brand"):
        company = str(people[0]["brand"])
    elif domains and domains[0].get("brand"):
        company = str(domains[0]["brand"])
    return {
        "booking": {
            "prospect_name": booking.prospect_name,
            "prospect_email": booking.prospect_email,
            "purpose": booking.purpose,
            "summary": booking.summary,
            "when": format_when(booking.starts_at),
            "meet_url": booking.meet_url,
            "source": booking.source,
        },
        "company": company,
        "outbound_contact": {"available": contact_ok, "row": contacts[0] if contacts else None},
        "outbound_enriched": {"available": person_ok, "row": people[0] if people else None},
        "outbound_domain": {"available": domain_ok, "row": domains[0] if domains else None},
        "abby_handled": {
            "available": abby_ok,
            "rows": [
                {key: (str(value)[:500] if value is not None else "") for key, value in row.items()}
                for row in abby_rows
            ],
        },
        "clickup": clickup_task,
        "website": {"domain": domain, "text": website} if website else {"domain": domain, "text": ""},
    }


def _questions(purpose: str) -> list[str]:
    opening = (
        f"You booked to {purpose[:180].rstrip('.')}. What do you want that conversation to answer?"
        if purpose
        else "What made you book this call?"
    )
    return [
        opening,
        "What does Amazon look like for the business today?",
        "Where is time going that you wish was not?",
        "Who else has to be comfortable before you would start a trial?",
        "What would make you decide, on this call, to connect Seller Central?",
    ]


def fallback_notes(booking: Booking, context: dict) -> dict:
    """Prep from the packet only, used when the model is down or unreadable."""

    company = str(context.get("company") or "").strip()
    email = booking.prospect_email or "Unknown"
    who = f"{booking.prospect_name or 'Unknown'} ({email})."
    who += f" Company: {company}." if company else " Company: Unknown."
    abby = (context.get("abby_handled") or {}).get("rows") or []
    outbound = (context.get("outbound_enriched") or {}).get("row") or (context.get("outbound_contact") or {}).get("row")
    if abby:
        latest = abby[0]
        how = (
            f"Abby history on file: {latest.get('category') or 'unspecified'} / "
            f"{latest.get('action') or 'unspecified'}."
        )
    elif outbound:
        how = "Matched an outbound contact already stored for this email."
    elif (context.get("clickup") or {}).get("matched"):
        how = "Matched an existing ClickUp Sales CRM task. No outbound or Abby row was on file."
    else:
        how = "Unknown. No outbound row, Abby history, or ClickUp task matched this email."
    needs = booking.purpose or "Unknown. The booking form answer was empty."
    unknowns = []
    if not company:
        unknowns.append("Company")
    if not booking.purpose:
        unknowns.append("Booking form answer")
    if not abby and not outbound:
        unknowns.append("How they came in")
    return {
        "who": who,
        "how_they_came_in": how,
        "likely_needs": needs,
        "discovery_questions": _questions(booking.purpose),
        "trial_pitch": list(PITCH_POINTS),
        "objections": [
            {
                "objection": "Prepared, not raised by them: we already have someone doing this.",
                "answer": "The 14-day trial sits beside that. It is free, and it starts when Seller Central connects.",
            },
            {
                "objection": "Prepared, not raised by them: $99 is too small to matter.",
                "answer": "Plans start at $99/mo. The first 14 days are free, so they can see the account before they pay.",
            },
            {
                "objection": "Prepared, not raised by them: prove it works.",
                "answer": "Zantrex proof: ROAS went from 2x to 3.5x. That is one account, not a promise for this prospect.",
            },
        ],
        "next_step": (
            "Ask them to start the free 14-day trial before the call ends, "
            "and connect Seller Central on the call."
        ),
        "unknowns": unknowns,
    }


def _ensure_shape(notes: dict, booking: Booking, context: dict) -> dict:
    base = fallback_notes(booking, context)
    if not isinstance(notes, dict):
        return base
    cleaned = _walk_dashes(notes)
    questions = cleaned.get("discovery_questions")
    if not isinstance(questions, list) or len([item for item in questions if str(item).strip()]) < 5:
        cleaned["discovery_questions"] = base["discovery_questions"]
    else:
        cleaned["discovery_questions"] = [no_em_dash(str(item)).strip() for item in questions[:5]]
    pitch = [no_em_dash(str(item)).strip() for item in (cleaned.get("trial_pitch") or []) if str(item).strip()]
    blob = " ".join(pitch).lower()
    for point in PITCH_POINTS:
        if point.lower().rstrip(".") not in blob and point.split(":")[0].lower() not in blob:
            pitch.insert(0, point)
            blob = " ".join(pitch).lower()
    cleaned["trial_pitch"] = pitch
    objections = cleaned.get("objections")
    if not isinstance(objections, list) or not objections:
        cleaned["objections"] = base["objections"]
    else:
        normalized = []
        for item in objections:
            if isinstance(item, dict):
                normalized.append({
                    "objection": no_em_dash(str(item.get("objection") or "Unknown")),
                    "answer": no_em_dash(str(item.get("answer") or "Unknown")),
                })
        cleaned["objections"] = normalized or base["objections"]
    for key in ("who", "how_they_came_in", "likely_needs", "next_step"):
        text_value = no_em_dash(str(cleaned.get(key) or "")).strip()
        cleaned[key] = text_value or base[key]
    unknowns = cleaned.get("unknowns")
    cleaned["unknowns"] = [no_em_dash(str(item)) for item in unknowns] if isinstance(unknowns, list) else base["unknowns"]
    return cleaned


def generate_notes(booking: Booking, context: dict) -> tuple[dict, str]:
    """Ask the existing gateway path. Fall back to the packet when it cannot answer."""

    from sales_support_agent.services.llm_client import llm_available, llm_client
    from sales_support_agent.services.sales.call_script.ai import _model_for

    packet = json.dumps(context, default=str)[:12000]
    if not llm_available():
        return fallback_notes(booking, context), "facts"
    system = (
        "You write prep notes for David's Anata sales call. Use only facts in the packet. "
        "If a fact is missing, write Unknown. Do not guess revenue, category, headcount, "
        "ad spend, tools, or pain. Do not use em dashes. "
        "The trial pitch must include these approved points and no other proof numbers: "
        "Free 14-day trial. Plans from $99/mo. Zantrex proof: ROAS went from 2x to 3.5x. "
        "Discovery questions are questions, not claims about the prospect. "
        "If the prospect has not raised an objection, label it Prepared, not raised by them. "
        "Reply with JSON only, no prose and no code fences, using keys who, how_they_came_in, "
        "likely_needs, discovery_questions (5 strings), trial_pitch, objections "
        "(list of {objection, answer}), next_step, unknowns."
    )
    user = "Packet:\n" + packet
    try:
        message = llm_client(caller="call_prep").messages.create(
            model=_model_for("default"),
            max_tokens=2500,
            system=system,
            messages=[{"role": "user", "content": user}],
        )
        raw = "".join(
            getattr(block, "text", "")
            for block in (message.content or [])
            if getattr(block, "type", "") == "text"
        ).strip()
        start, end = raw.find("{"), raw.rfind("}")
        parsed = json.loads(raw[start : end + 1] if start >= 0 and end > start else raw)
    except Exception as exc:
        logger.warning("[call-prep] notes model failed: %s", type(exc).__name__)
        return fallback_notes(booking, context), "facts"
    return _ensure_shape(parsed, booking, context), "llm"


def _stamp(moment: Optional[datetime]) -> str:
    if moment is None:
        return ""
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc).isoformat()


def _sort_key(tracker: SalesCallTracker) -> float:
    moment = tracker.updated_at
    if moment is None:
        return 0.0
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.timestamp()


def find_tracker(session: Session, booking: Booking) -> Optional[SalesCallTracker]:
    """Match a stored event id first, then the same person (Nick and Nicolas)."""

    rows = list(session.scalars(select(SalesCallTracker)))
    for row in rows:
        state = row.state_json if isinstance(row.state_json, dict) else {}
        prep = state.get("ss-prep") if isinstance(state.get("ss-prep"), dict) else {}
        if prep.get("event_id") and prep.get("event_id") == booking.event_id:
            return row
    people = []
    email = (booking.prospect_email or "").lower()
    for row in rows:
        state = row.state_json if isinstance(row.state_json, dict) else {}
        prep = state.get("ss-prep") if isinstance(state.get("ss-prep"), dict) else {}
        stored_email = str(prep.get("prospect_email") or "").lower()
        if email and stored_email and stored_email == email:
            people.append(row)
            continue
        if names_match(row.prospect_name, booking.prospect_name):
            people.append(row)
    if not people:
        return None
    open_rows = [row for row in people if row.status == call_service.STATUS_OPEN]
    pool = open_rows or people
    pool.sort(key=_sort_key, reverse=True)
    return pool[0]


def _store(session: Session, tracker: SalesCallTracker, prep: dict) -> None:
    state = dict(tracker.state_json or {})
    state["ss-prep"] = prep
    tracker.state_json = call_service.sanitize_state(state)
    tracker.updated_at = datetime.now(timezone.utc)
    session.flush()


def _company(context: dict) -> str:
    return " ".join(str(context.get("company") or "").split())[:255]


def _lead_link(context: dict) -> str:
    url = str((context.get("clickup") or {}).get("url") or "").strip()
    if url.lower().startswith(("https://", "http://")):
        return url[:1024]
    return ""


def slack_text(tracker: SalesCallTracker, prep: dict, timing_note: str) -> str:
    sections = prep.get("sections") if isinstance(prep.get("sections"), dict) else {}
    questions = sections.get("discovery_questions") or []
    pitch = sections.get("trial_pitch") or []
    objections = sections.get("objections") or []
    lines = [
        f"Call prep: {prep.get('prospect_name') or tracker.prospect_name or 'Unknown'}",
        f"When: {prep.get('when') or 'Unknown'}",
        f"Purpose: {prep.get('purpose') or 'Unknown'}",
        f"Tracker: {tracker_url(tracker.id)}",
    ]
    if timing_note:
        lines.append(timing_note)
    if prep.get("meet_url"):
        lines.append(f"Meet: {prep['meet_url']}")
    lines += [
        "",
        "Who: " + str(sections.get("who") or "Unknown"),
        "How they came in: " + str(sections.get("how_they_came_in") or "Unknown"),
        "Likely needs: " + str(sections.get("likely_needs") or "Unknown"),
        "",
        "Discovery questions:",
    ]
    for index, question in enumerate(questions, start=1):
        lines.append(f"{index}. {question}")
    lines.append("")
    lines.append("Trial pitch:")
    for point in pitch:
        lines.append(f"- {point}")
    lines.append("")
    lines.append("Objections:")
    for item in objections:
        if isinstance(item, dict):
            lines.append(f"- {item.get('objection') or 'Unknown'}: {item.get('answer') or 'Unknown'}")
    lines += ["", "Next step: " + str(sections.get("next_step") or "Unknown")]
    unknowns = prep.get("unknowns") or []
    if unknowns:
        lines += ["", "Unknown: " + ", ".join(str(item) for item in unknowns)]
    return no_em_dash("\n".join(lines))[:3500]


def _notify(slack: Any, gmail: Any, tracker: SalesCallTracker, prep: dict, now: datetime) -> dict:
    starts = None
    raw_start = str(prep.get("starts_at") or "")
    if raw_start:
        try:
            starts = datetime.fromisoformat(raw_start)
        except ValueError:
            starts = None
    slack_state = dict(prep.get("slack") or {})
    draft_state = dict(prep.get("draft") or {})
    post = True
    timing_note = ""
    if starts is not None:
        if starts.tzinfo is None:
            starts = starts.replace(tzinfo=timezone.utc)
        if starts < now:
            post = False
            timing_note = "The call has already started, so Slack was not posted."
        elif starts - now < timedelta(hours=1):
            timing_note = "This call starts in under an hour."
    if post and not slack_state.get("posted_at"):
        if slack is None or not slack.is_configured(SALES_SLACK_CHANNEL):
            slack_state["error"] = "Slack is not configured."
        else:
            try:
                slack.post_message(
                    text=slack_text(tracker, prep, timing_note),
                    channel=SALES_SLACK_CHANNEL,
                )
                slack_state["posted_at"] = datetime.now(timezone.utc).isoformat()
                slack_state["channel"] = SALES_SLACK_CHANNEL
                slack_state["note"] = timing_note
                slack_state["error"] = ""
            except Exception as exc:
                logger.warning("[call-prep] slack failed: %s", type(exc).__name__)
                slack_state["error"] = type(exc).__name__
    elif not post:
        slack_state["note"] = timing_note
    if not draft_state.get("id") and post:
        if gmail is None or not gmail.is_configured():
            draft_state["skipped"] = "Gmail draft path is not configured."
        else:
            try:
                result = gmail.create_draft(
                    to=(DAVID_EMAIL,),
                    subject=f"Call prep: {prep.get('prospect_name') or tracker.prospect_name}",
                    text=slack_text(tracker, prep, timing_note),
                )
                if isinstance(result, dict) and result.get("skipped"):
                    draft_state["skipped"] = str(result.get("reason") or "skipped")
                else:
                    draft_state["id"] = str((result or {}).get("id") or "")
                    draft_state["to"] = DAVID_EMAIL
                    draft_state["skipped"] = ""
            except Exception as exc:
                logger.warning("[call-prep] draft failed: %s", type(exc).__name__)
                draft_state["error"] = type(exc).__name__
    elif not post and not draft_state.get("id"):
        draft_state["skipped"] = "The call has already started, so no draft was created."
    prep["slack"] = slack_state
    prep["draft"] = draft_state
    return prep


def apply_booking(
    session: Session,
    booking: Booking,
    *,
    context_for: Callable[[Booking], dict],
    notes_for: Callable[[Booking, dict], tuple[dict, str]],
    slack: Any,
    gmail: Any,
    now: datetime,
) -> dict:
    """Create or reuse the lead's tracker, store ss-prep, then notify David."""

    context = context_for(booking)
    company = _company(context)
    link = _lead_link(context)
    existing = find_tracker(session, booking)
    created = False
    if existing is None:
        tracker = call_service.create_tracker(
            session,
            DAVID_USER,
            prospect_name=booking.prospect_name or booking.prospect_email or "Prospect",
            company=company,
            lead_link=link,
        )
        created = True
    else:
        tracker = existing
        if company and not tracker.company:
            tracker.company = company
        if link and not tracker.lead_link:
            tracker.lead_link = link
    state = tracker.state_json if isinstance(tracker.state_json, dict) else {}
    current = state.get("ss-prep") if isinstance(state.get("ss-prep"), dict) else {}
    same_event = bool(current.get("event_id") and current.get("event_id") == booking.event_id)
    if same_event and isinstance(current.get("sections"), dict):
        prep = dict(current)
        action = "reused"
    else:
        sections, notes_source = notes_for(booking, context)
        sections = _walk_dashes(sections)
        prep = {
            "event_id": booking.event_id,
            "source": booking.source,
            "starts_at": _stamp(booking.starts_at),
            "ends_at": _stamp(booking.ends_at),
            "when": format_when(booking.starts_at),
            "prospect_name": booking.prospect_name,
            "prospect_email": booking.prospect_email,
            "company": company or tracker.company or "",
            "purpose": booking.purpose,
            "meet_url": booking.meet_url,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "notes_source": notes_source,
            "sections": sections,
            "unknowns": sections.get("unknowns") or [],
            "slack": {},
            "draft": {},
        }
        action = "created" if created else "prepped"
    prep = _notify(slack, gmail, tracker, prep, now)
    _store(session, tracker, prep)
    return {
        "action": action,
        "created": created,
        "tracker_id": tracker.id,
        "event_id": booking.event_id,
        "prospect": booking.prospect_name,
        "slack": bool((prep.get("slack") or {}).get("posted_at")),
        "draft": bool((prep.get("draft") or {}).get("id")),
    }


def run_prep(
    session: Session,
    *,
    now: datetime,
    bookings: list[Booking],
    source: str,
    gap: str,
    context_for: Callable[[Booking], dict],
    notes_for: Callable[[Booking, dict], tuple[dict, str]],
    slack: Any,
    gmail: Any,
    attempts: tuple[str, ...] = (),
) -> dict:
    """Prep every new booking in the window. One failure does not stop the rest."""

    chosen = [booking for booking in bookings if in_window(booking, now)]
    rows = []
    errors = []
    for booking in chosen:
        try:
            rows.append(apply_booking(
                session, booking, context_for=context_for, notes_for=notes_for,
                slack=slack, gmail=gmail, now=now,
            ))
        except Exception as exc:
            logger.exception("[call-prep] booking %s failed", booking.event_id)
            errors.append({"event_id": booking.event_id, "error": type(exc).__name__})
    status = "gap" if not chosen and gap else "ok"
    if errors and not rows:
        status = "error"
    return {
        "status": status,
        "source": source,
        "gap": gap,
        "attempts": list(attempts),
        "seen": len(chosen),
        "created": sum(1 for row in rows if row["created"]),
        "reused": sum(1 for row in rows if row["action"] == "reused"),
        "prepped": sum(1 for row in rows if row["action"] == "prepped"),
        "slack": sum(1 for row in rows if row["slack"]),
        "drafts": sum(1 for row in rows if row["draft"]),
        "bookings": rows,
        "errors": errors,
    }
