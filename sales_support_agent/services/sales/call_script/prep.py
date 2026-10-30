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
    task_id_from_link,
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
DISCOVERY_STEPS = (
    ("s1", "Isolate the problem"),
    ("s2", "Background"),
    ("s3", "Current situation"),
    ("s4", "Assess current efforts"),
    ("s5", "The numbers"),
    ("s6", "How long"),
    ("s7", "Widen the gap"),
    ("s8", "Doubt"),
    ("s9", "Solutions tried"),
    ("s10", "Cost / why now"),
    ("s11", "Support"),
    ("s12", "Desired situation"),
    ("s13", "Tie down"),
)
AMAZON_UNVERIFIED = "Amazon presence: not verified"
_AMAZON_URL = re.compile(
    r"https?://(?:www\.)?(?:amazon\.[a-z.]+|amzn\.to)/(?:stores/|dp/|gp/)[^\s\"'<>\\]+",
    re.I,
)
_SENTENCE = re.compile(r"(?<=[.!?])\s+")
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


def as_list(value: Any) -> list:
    """Turn a model field into a list of items. Never walk a string character by character."""

    if isinstance(value, list):
        if value and all(isinstance(item, str) and len(item) == 1 for item in value):
            return as_list("".join(value))
        return [item for item in value if item not in ("", None)]
    if not isinstance(value, str):
        return []
    text_value = value.strip()
    if not text_value:
        return []
    lines = []
    for line in re.split(r"[\r\n]+", text_value):
        cleaned = re.sub(r"^[\s\-•*]+", "", line).strip()
        if cleaned:
            lines.append(cleaned)
    if len(lines) > 1:
        return lines
    sentences = [part.strip() for part in _SENTENCE.split(text_value) if part.strip()]
    if len(sentences) > 1:
        return sentences
    return [text_value]


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


def _task_blob(task: dict) -> str:
    parts = [
        task.get("name"),
        task.get("description"),
        task.get("text_content"),
        task.get("markdown_description"),
        _field(task, FIELD_EMAIL),
        _field(task, FIELD_NAME),
        _field(task, FIELD_COMPANY),
    ]
    return " ".join(str(part or "") for part in parts).lower()


def _comment_rows(client: Any, task_id: str) -> list[dict]:
    if not task_id or client is None or not getattr(client, "get_task_comments", None):
        return []
    try:
        comments = client.get_task_comments(task_id) or []
    except Exception as exc:
        logger.info("[call-prep] clickup comments failed: %s", type(exc).__name__)
        return []
    rows = []
    for comment in comments[:8]:
        if not isinstance(comment, dict):
            continue
        text_value = " ".join(str(comment.get("comment_text") or "").split())
        if not text_value:
            continue
        user = comment.get("user") if isinstance(comment.get("user"), dict) else {}
        rows.append({
            "by": str(user.get("username") or ""),
            "text": text_value[:500],
        })
    return rows


def _task_record(task: dict, client: Any) -> dict:
    task_id = str(task.get("id") or "")
    status = task.get("status")
    if isinstance(status, dict):
        status = status.get("status")
    return {
        "available": True,
        "matched": True,
        "id": task_id,
        "name": str(task.get("name") or ""),
        "status": str(status or ""),
        "company": str(_field(task, FIELD_COMPANY) or ""),
        "email": str(_field(task, FIELD_EMAIL) or "").strip().lower(),
        "person": str(_field(task, FIELD_NAME) or ""),
        "description": " ".join(str(task.get("text_content") or task.get("description") or "").split())[:800],
        "url": str(task.get("url") or (f"https://app.clickup.com/t/{task_id}" if task_id else "")),
        "comments": _comment_rows(client, task_id),
    }


def _task_matches(task: dict, booking: Booking) -> bool:
    email = (booking.prospect_email or "").lower()
    blob = _task_blob(task)
    if email and email in blob:
        return True
    title = str(task.get("name") or "")
    person = str(_field(task, FIELD_NAME) or "")
    if names_match(booking.prospect_name, title) or (person and names_match(booking.prospect_name, person)):
        return True
    company = str(_field(task, FIELD_COMPANY) or "")
    return bool(company and company.lower() in (booking.summary or "").lower())


def _clickup_task(client: Any, booking: Booking, lead_link: str = "") -> dict:
    if client is None:
        return {"available": False, "matched": False}
    linked = task_id_from_link(lead_link)
    if linked and getattr(client, "get_task", None):
        try:
            task = client.get_task(linked)
        except Exception as exc:
            logger.info("[call-prep] clickup task %s failed: %s", linked, type(exc).__name__)
            task = None
        if isinstance(task, dict) and task.get("id"):
            return _task_record(task, client)
    if not getattr(client, "get_tasks", None):
        return {"available": False, "matched": False}
    email = (booking.prospect_email or "").lower()
    try:
        for page in range(8):
            tasks = client.get_tasks(list_id(), include_closed=True, page=page) or []
            for task in tasks:
                if isinstance(task, dict) and _task_matches(task, booking):
                    full = task
                    if getattr(client, "get_task", None) and task.get("id"):
                        try:
                            loaded = client.get_task(str(task["id"]))
                            if isinstance(loaded, dict):
                                full = loaded
                        except Exception:
                            full = task
                    return _task_record(full, client)
            if len(tasks) < 100:
                break
    except Exception as exc:
        logger.info("[call-prep] clickup lookup failed: %s", type(exc).__name__)
        return {"available": False, "matched": False, "error": type(exc).__name__}
    if not email:
        return {"available": True, "matched": False}
    return {"available": True, "matched": False}


def fetch_public(url: str) -> dict:
    """GET one public page. ok is true only when the response is 2xx."""

    from urllib.parse import urljoin

    from sales_support_agent.services.public_url_guard import public_http_url
    from sales_support_agent.services.sales.call_script.bookings import html_to_text

    empty = {"ok": False, "url": "", "status": 0, "html": "", "text": ""}
    safe = public_http_url(url)
    if not safe:
        return empty
    try:
        import requests

        current = safe
        response = None
        for _ in range(3):
            response = requests.get(
                current,
                timeout=8,
                headers={"User-Agent": "Mozilla/5.0 (compatible; AnataCallPrep/1.0)"},
                allow_redirects=False,
            )
            if response.status_code not in {301, 302, 303, 307, 308}:
                break
            nxt = public_http_url(urljoin(current, str(response.headers.get("location") or "")))
            if not nxt:
                return empty
            current = nxt
        if response is None or not response.ok:
            return {**empty, "url": current, "status": getattr(response, "status_code", 0)}
        html = response.text or ""
        return {
            "ok": True,
            "url": current,
            "status": response.status_code,
            "html": html[:200000],
            "text": " ".join(html_to_text(html).split())[:3000],
        }
    except Exception as exc:
        logger.info("[call-prep] fetch failed: %s", type(exc).__name__)
        return empty


def verify_links(domain: str, *, fetch: Callable[[str], dict] = fetch_public) -> dict:
    """Website and Amazon URLs that this process actually fetched. Nothing else counts."""

    page = fetch(f"https://{domain}") if domain else {"ok": False, "url": "", "html": "", "text": ""}
    website = {
        "verified": bool(page.get("ok") and page.get("url")),
        "url": str(page.get("url") or ""),
    }
    html = str(page.get("html") or "")
    candidates: list[str] = []
    for match in _AMAZON_URL.findall(html):
        cleaned = match.rstrip(").,;\"'")
        if cleaned not in candidates:
            candidates.append(cleaned)
    resolved: list[str] = []
    for candidate in candidates[:3]:
        checked = fetch(candidate)
        final = str(checked.get("url") or "")
        if checked.get("ok") and _AMAZON_URL.search(final or candidate):
            resolved.append(final or candidate)
    amazon_line = AMAZON_UNVERIFIED if not resolved else "Amazon: " + ", ".join(resolved)
    lowered = html.lower()
    return {
        "website": website,
        "amazon": {"verified": bool(resolved), "urls": resolved, "line": amazon_line},
        "shopify": "cdn.shopify.com" in lowered or "myshopify.com" in lowered,
        "title": _page_title(html),
        "text": str(page.get("text") or ""),
    }


def _page_title(html: str) -> str:
    match = re.search(r"<title[^>]*>(.*?)</title>", html or "", re.I | re.S)
    if not match:
        return ""
    return " ".join(re.sub(r"<[^>]+>", " ", match.group(1)).split())[:160]


def _gmail_history(client: Any, email: str) -> list[dict]:
    if client is None or not email or not getattr(client, "list_messages", None):
        return []
    try:
        messages = client.list_messages(
            query=f"{email} -subject:\"Call prep:\"",
            max_results=6,
        )
    except Exception as exc:
        logger.info("[call-prep] gmail history failed: %s", type(exc).__name__)
        return []
    from sales_support_agent.integrations.gmail_payloads import extract_body_text, extract_header

    rows = []
    for item in messages or []:
        message_id = str((item or {}).get("id") or "")
        if not message_id or not getattr(client, "get_message", None):
            continue
        try:
            full = client.get_message(message_id)
        except Exception:
            continue
        payload = full.get("payload") if isinstance(full.get("payload"), dict) else {}
        headers = payload.get("headers") if isinstance(payload.get("headers"), list) else []
        subject = extract_header(headers, "subject")
        if subject.lower().startswith("call prep:"):
            continue
        body = extract_body_text(payload) or str(full.get("snippet") or "")
        rows.append({
            "from": extract_header(headers, "from"),
            "date": extract_header(headers, "date"),
            "subject": subject[:180],
            "snippet": " ".join(body.split())[:400],
        })
    return rows


def _instantly_history(email: str) -> dict:
    """Lead and recent thread from the Instantly client the app already uses."""

    if not email:
        return {"available": False, "lead": None, "emails": []}
    try:
        import abby_runner
        import outbound_instantly_push as instantly_push

        key = instantly_push.api_key()
        if not key:
            return {"available": False, "lead": None, "emails": []}
        client = abby_runner.Instantly(key)
        lead = client.lead(email) or {}
        status, data = client._call(
            "GET", "emails", params={"search": email, "limit": 5, "sort_order": "desc"}
        )
        items = (data or {}).get("items") if status == 200 and isinstance(data, dict) else []
    except Exception as exc:
        logger.info("[call-prep] instantly history failed: %s", type(exc).__name__)
        return {"available": False, "lead": None, "emails": []}
    emails = []
    for item in items or []:
        if not isinstance(item, dict):
            continue
        body = item.get("body") if isinstance(item.get("body"), dict) else {}
        text_value = str(body.get("text") or item.get("content_preview") or "")
        emails.append({
            "from": str(item.get("from_address_email") or item.get("lead") or ""),
            "subject": str(item.get("subject") or "")[:180],
            "snippet": " ".join(text_value.split())[:400],
            "timestamp": str(item.get("timestamp_created") or ""),
        })
    slim = {}
    if isinstance(lead, dict):
        for key in ("email", "company_name", "website", "first_name", "last_name", "campaign"):
            if lead.get(key):
                slim[key] = lead.get(key)
    return {"available": True, "lead": slim or None, "emails": emails}


def _mailbox_rows(session: Optional[Session], email: str) -> list[dict]:
    if not email:
        return []
    ok, rows = _query(
        session,
        "SELECT sender_email, subject, snippet, occurred_at FROM mailbox_signals "
        "WHERE lower(sender_email) = :e ORDER BY occurred_at DESC LIMIT 6",
        {"e": email},
    )
    if not ok:
        return []
    return [
        {
            "from": str(row.get("sender_email") or ""),
            "subject": str(row.get("subject") or "")[:180],
            "snippet": " ".join(str(row.get("snippet") or "").split())[:400],
            "date": str(row.get("occurred_at") or ""),
        }
        for row in rows
    ]


def gather_context(
    session: Optional[Session],
    clickup: Any,
    booking: Booking,
    *,
    fetch: Callable[[str], dict] = fetch_public,
    gmail: Any = None,
    tracker: Any = None,
) -> dict:
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
    lead_link = str(getattr(tracker, "lead_link", "") or "")
    links = verify_links(domain, fetch=fetch)
    clickup_task = _clickup_task(clickup, booking, lead_link)
    gmail_rows = _gmail_history(gmail, email)
    instantly = _instantly_history(email)
    mailbox = _mailbox_rows(session, email)
    company = ""
    if clickup_task.get("company"):
        company = str(clickup_task["company"])
    elif tracker is not None and getattr(tracker, "company", ""):
        company = str(tracker.company)
    elif contacts and contacts[0].get("company"):
        company = str(contacts[0]["company"])
    elif people and people[0].get("brand"):
        company = str(people[0]["brand"])
    elif domains and domains[0].get("brand"):
        company = str(domains[0]["brand"])
    elif links.get("title"):
        company = str(links["title"])
    elif clickup_task.get("name"):
        company = str(clickup_task["name"]).split("|")[0].strip()
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
        "domain": domain,
        "company": company,
        "links": links,
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
        "gmail": gmail_rows,
        "instantly": instantly,
        "mailbox": mailbox,
        "website": {
            "domain": domain,
            "verified": bool((links.get("website") or {}).get("verified")),
            "url": (links.get("website") or {}).get("url") or "",
            "shopify": bool(links.get("shopify")),
            "text": links.get("text") or "",
        },
    }


def _history_line(context: dict) -> str:
    parts = []
    for row in (context.get("abby_handled") or {}).get("rows") or []:
        parts.append(
            f"Abby {row.get('created_at') or ''}: {row.get('category') or 'unspecified'} / "
            f"{row.get('action') or 'unspecified'}. {row.get('detail') or ''}".strip()
        )
    for comment in (context.get("clickup") or {}).get("comments") or []:
        parts.append(f"ClickUp {comment.get('by') or ''}: {comment.get('text') or ''}")
    for note in list(context.get("gmail") or []) + list(context.get("mailbox") or []):
        parts.append(
            f"Mail {note.get('date') or ''} from {note.get('from') or 'unknown'}: "
            f"{note.get('subject') or ''} {note.get('snippet') or ''}".strip()
        )
    for note in ((context.get("instantly") or {}).get("emails") or []):
        parts.append(
            f"Instantly {note.get('timestamp') or ''} from {note.get('from') or ''}: "
            f"{note.get('subject') or ''} {note.get('snippet') or ''}".strip()
        )
    return " ".join(part for part in parts if part).strip()


def prospect_quotes(booking: Booking, context: dict) -> str:
    """Lines the prospect wrote, for the script's quotes field. Not claims we inferred."""

    email = (booking.prospect_email or "").lower()
    lines = []
    for comment in (context.get("clickup") or {}).get("comments") or []:
        text_value = str(comment.get("text") or "")
        marker = re.search(r"(?is)\b(?:said|wrote):\s*(.+)", text_value)
        if marker:
            lines.append(" ".join(marker.group(1).split())[:400])
    for note in list(context.get("gmail") or []) + list(context.get("mailbox") or []):
        sender = str(note.get("from") or "").lower()
        if email and email in sender and note.get("snippet"):
            lines.append(" ".join(str(note["snippet"]).split())[:400])
    seen = []
    for line in lines:
        if line and line not in seen:
            seen.append(line)
    return "\n".join(seen[:6])


def discovery_map(booking: Booking, context: dict) -> list[dict]:
    """Facts and script angles for sections s1 to s13. No new question bank."""

    links = context.get("links") or {}
    website = (links.get("website") or {})
    amazon = (links.get("amazon") or {})
    company = str(context.get("company") or "").strip() or "Unknown"
    site_line = website.get("url") if website.get("verified") else "Website: not verified"
    channel = "The fetched homepage is a Shopify store." if links.get("shopify") else ""
    amazon_line = str(amazon.get("line") or AMAZON_UNVERIFIED)
    history = _history_line(context)
    purpose = booking.purpose or "Unknown"
    known = {
        "s1": f"Booking form: {purpose}.",
        "s2": " ".join(
            part for part in (
                f"Company on file: {company}.",
                f"Website: {site_line}.",
                channel,
                amazon_line + ".",
            ) if part
        ),
        "s3": history or "Unknown. No Abby, ClickUp, Gmail, or Instantly history matched this email.",
        "s4": "Unknown. Ask section 4. Do not treat follow-up emails as their current ad setup.",
        "s5": "Unknown. No verified account numbers are on file. Use the script's number questions.",
        "s6": "Unknown. Use section 6.",
        "s7": "Unknown. Use section 7 after they name the gap.",
        "s8": "Unknown. Use section 8.",
        "s9": history or "Unknown. Use section 9.",
        "s10": f"They booked for: {purpose}.",
        "s11": "Unknown who else decides. Use section 11.",
        "s12": "Unknown. Use section 12.",
        "s13": "Use their words from the quotes field when it has a line. Otherwise use section 13.",
    }
    angles = {
        "s1": "Use section 1. Start from the booking reason, then the script's biggest-challenge question.",
        "s2": "Use section 2. Only the verified website and Amazon lines above are safe to say.",
        "s3": "Use section 3. Read back a fact from the history, then ask them to say it in their words.",
        "s4": "Use section 4. Do not assume an agency, a tool, or an Amazon account.",
        "s5": "Use section 5. Leave the calculators blank until they give a number.",
        "s6": "Use section 6.",
        "s7": "Use section 7.",
        "s8": "Use section 8.",
        "s9": "Use section 9. Prior emails are context, not a list of solutions they tried.",
        "s10": "Use section 10. The booking reason is why they took the meeting.",
        "s11": "Use section 11.",
        "s12": "Use section 12.",
        "s13": "Use section 13 with their words only.",
    }
    if not amazon.get("verified"):
        angles["s2"] += " Do not say they are on Amazon."
    return [
        {"id": step_id, "title": title, "known": known[step_id], "angle": angles[step_id]}
        for step_id, title in DISCOVERY_STEPS
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
    history = _history_line(context)
    if history and how.startswith("Unknown"):
        how = history[:700]
    elif history and "Abby history" in how:
        how = (how + " " + history)[:700]
    links = context.get("links") or {}
    amazon_line = str((links.get("amazon") or {}).get("line") or AMAZON_UNVERIFIED)
    if not (links.get("amazon") or {}).get("verified"):
        needs = needs.rstrip(".") + f". {amazon_line}."
    unknowns = []
    if not company:
        unknowns.append("Company")
    if not booking.purpose:
        unknowns.append("Booking form answer")
    if not history and not abby and not outbound:
        unknowns.append("How they came in")
    if not (links.get("amazon") or {}).get("verified"):
        unknowns.append(AMAZON_UNVERIFIED)
    return {
        "who": who,
        "how_they_came_in": how,
        "likely_needs": needs,
        "discovery": discovery_map(booking, context),
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
    cleaned.pop("discovery_questions", None)
    cleaned["discovery"] = base["discovery"]
    pitch = [
        no_em_dash(str(item)).strip()
        for item in as_list(cleaned.get("trial_pitch"))
        if not isinstance(item, dict) and str(item).strip()
    ]
    blob = " ".join(pitch).lower()
    for point in PITCH_POINTS:
        if point.lower().rstrip(".") not in blob and point.split(":")[0].lower() not in blob:
            pitch.insert(0, point)
            blob = " ".join(pitch).lower()
    cleaned["trial_pitch"] = pitch
    objections = as_list(cleaned.get("objections"))
    normalized = []
    for item in objections:
        if isinstance(item, dict):
            normalized.append({
                "objection": no_em_dash(str(item.get("objection") or "Unknown")),
                "answer": no_em_dash(str(item.get("answer") or "Unknown")),
            })
        elif str(item).strip():
            normalized.append({"objection": no_em_dash(str(item)), "answer": "Unknown"})
    cleaned["objections"] = normalized or base["objections"]
    for key in ("who", "how_they_came_in", "likely_needs", "next_step"):
        text_value = no_em_dash(str(cleaned.get(key) or "")).strip()
        if not text_value or text_value.lower().rstrip(".") == "unknown":
            text_value = base[key]
        cleaned[key] = text_value
    unknowns = as_list(cleaned.get("unknowns"))
    cleaned["unknowns"] = [no_em_dash(str(item)) for item in unknowns if not isinstance(item, dict)] or base["unknowns"]
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
        "Do not write discovery questions. The call script already has sections s1 to s13. "
        "trial_pitch must be a JSON array of strings, never one paragraph. "
        "Do not say the prospect is on Amazon unless the packet's amazon.verified is true, "
        "and then only repeat the resolved URLs. If it is not verified, write "
        "'Amazon presence: not verified'. "
        "If the prospect has not raised an objection, label it Prepared, not raised by them. "
        "Reply with JSON only, no prose and no code fences, using keys who, how_they_came_in, "
        "likely_needs, trial_pitch (array of strings), objections "
        "(array of {objection, answer}), next_step, unknowns (array of strings)."
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


def _store(session: Session, tracker: SalesCallTracker, prep: dict, quotes: str = "") -> None:
    state = dict(tracker.state_json or {})
    state["ss-prep"] = prep
    if quotes:
        gap = state.get("ss-gap") if isinstance(state.get("ss-gap"), dict) else {}
        gap = dict(gap)
        if not str(gap.get("quotes") or "").strip():
            gap["quotes"] = quotes
        state["ss-gap"] = gap
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
    questions = as_list(sections.get("discovery") or sections.get("discovery_questions"))
    pitch = as_list(sections.get("trial_pitch"))
    objections = as_list(sections.get("objections"))
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
        "Discovery, using the call script:",
    ]
    links = prep.get("links") if isinstance(prep.get("links"), dict) else {}
    website = (links.get("website") or {}) if isinstance(links, dict) else {}
    amazon = (links.get("amazon") or {}) if isinstance(links, dict) else {}
    if website.get("verified") and website.get("url"):
        lines.append(f"Website: {website['url']}")
    if isinstance(amazon, dict) and amazon.get("line"):
        lines.append(str(amazon["line"]))
    for item in questions:
        if isinstance(item, dict):
            lines.append(
                f"- {item.get('id') or ''} {item.get('title') or ''}: "
                f"{item.get('known') or 'Unknown'} Angle: {item.get('angle') or ''}"
            )
        else:
            lines.append(f"- {item}")
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
    context_for: Callable[..., dict],
    notes_for: Callable[[Booking, dict], tuple[dict, str]],
    slack: Any,
    gmail: Any,
    now: datetime,
) -> dict:
    """Create or reuse the lead's tracker, store ss-prep, then notify David.

    A tracker that already has prep for this event is left as-is, including
    its draft. New notes are not written over it.
    """

    existing = find_tracker(session, booking)
    if existing is not None:
        prior = existing.state_json if isinstance(existing.state_json, dict) else {}
        saved = prior.get("ss-prep") if isinstance(prior.get("ss-prep"), dict) else {}
        finished = (
            saved.get("event_id") == booking.event_id
            and isinstance(saved.get("sections"), dict)
            and (saved.get("slack") or {}).get("posted_at")
            and ((saved.get("draft") or {}).get("id") or (saved.get("draft") or {}).get("skipped"))
        )
        if finished:
            return {
                "action": "reused",
                "created": False,
                "tracker_id": existing.id,
                "event_id": booking.event_id,
                "prospect": booking.prospect_name,
                "slack": False,
                "draft": False,
            }
    context = context_for(booking, existing)
    company = _company(context)
    link = _lead_link(context)
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
            "links": {
                "website": (context.get("links") or {}).get("website") or {},
                "amazon": (context.get("links") or {}).get("amazon") or {"line": AMAZON_UNVERIFIED},
            },
            "unknowns": sections.get("unknowns") or [],
            "slack": {},
            "draft": {},
        }
        action = "created" if created else "prepped"
    quotes = "" if action == "reused" else prospect_quotes(booking, context)
    prep = _notify(slack, gmail, tracker, prep, now)
    _store(session, tracker, prep, quotes=quotes)
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
    context_for: Callable[..., dict],
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
