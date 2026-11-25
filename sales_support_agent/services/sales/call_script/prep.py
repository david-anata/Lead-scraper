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
# Published on https://anatainc.com/case-studies (SeroVital). No other client result.
PROOF_LINE = "SeroVital: Amazon from $12K/day to $33K/day, margins preserved."
PITCH_POINTS = (
    "Free 14-day trial.",
    "Plans from $99/mo.",
    PROOF_LINE,
)
_BANNED_PROOF = re.compile(r"\bzantrex\b", re.I)
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


_NOISE_SUBJECT = re.compile(
    r"(?i)^(?:(?:re|fw|fwd)\s*:\s*)*(?:appointment booked\b|call prep:|invitation:|"
    r"updated invitation:|accepted:|declined:)"
)
_HOME_PREFIX = re.compile(r"(?i)^(?:home|homepage|welcome)(?:\s+page)?\s*[-|:]\s*")
_HOME_SUFFIX = re.compile(r"(?i)\s*[-|:]\s*(?:home|homepage|welcome)(?:\s+page)?$")
_SIGNATURE_CUT = re.compile(r"(?i)\bthank you,\s|\bthanks,\s|phone\s*\(|fax\s*\(")
_THREAD_MARK = re.compile(
    r"(?i)join with google meet|appointment booked:|original message|begin forwarded message|"
    r"\bmeet\.google\.com\b|\bon [A-Za-z]{3},\s.{0,60}?wrote:|\bABBY:\s|\bcc:\s|\|\^{3,}"
)
_AMAZON_REPEAT = re.compile(r"(?i)\s*amazon presence: not verified\.?")
_PREPARED = re.compile(r"(?i)^prepared(?:,\s*not raised by them)?\s*:\s*")
_NUDGE_ONLY = re.compile(r"(?i)^nudge\s*\d+$")


def clean_company_name(value: str) -> str:
    """Drop homepage chrome such as 'Home - Brand' so the title is not the company."""

    text = no_em_dash(" ".join(str(value or "").split()))
    previous = None
    while text and text != previous:
        previous = text
        text = _HOME_PREFIX.sub("", text).strip()
        text = _HOME_SUFFIX.sub("", text).strip()
    if text.lower() in {"home", "homepage", "welcome"}:
        return ""
    return text.strip(" -|")


def clean_message_text(value: str) -> str:
    """Keep the prospect's own words. Cut quoted threads, signatures, invites, and art."""

    text = no_em_dash(str(value or "")).replace("\r", "\n")
    kept = []
    for line in text.split("\n"):
        stripped = line.strip()
        if not stripped or stripped.startswith((">", "|")):
            continue
        if re.match(r"(?i)^(cc|bcc|from|sent|subject)\s*:", stripped):
            continue
        kept.append(stripped)
    text = re.sub(r"\s+", " ", " ".join(kept)).strip()
    cut_at = None
    mark = _THREAD_MARK.search(text)
    if mark:
        cut_at = mark.start()
    signature = _SIGNATURE_CUT.search(text)
    if signature and signature.start() >= 40:
        cut_at = signature.start() if cut_at is None else min(cut_at, signature.start())
    art = text.find("|^^^")
    if art >= 0:
        cut_at = art if cut_at is None else min(cut_at, art)
    if cut_at is not None:
        text = text[:cut_at]
    text = text.strip(" \t,;|")
    text = text.rstrip("-")
    if not text or _NUDGE_ONLY.match(text) or _THREAD_MARK.search(text):
        return ""
    return text[:320]


def _https_url(value: str) -> str:
    url = str(value or "").strip()
    if url.startswith("https://") and " " not in url and len(url) < 500:
        return url
    return ""


def _run_id_from_deck_url(url: str) -> str:
    match = re.search(r"/decks/[^/]+/(\d+)(?:/|$)", url or "")
    return match.group(1) if match else ""


def _json_text(dialect: str, column: str, key: str) -> str:
    if dialect == "postgresql":
        return f"coalesce({column}->>'{key}', '')"
    return f"coalesce(json_extract({column}, '$.{key}'), '')"


def _host_matches(value: str, domain: str) -> bool:
    from urllib.parse import urlparse

    raw = str(value or "").strip().lower()
    host_name = (domain or "").strip().lower()
    if not raw or not host_name:
        return False
    if "://" not in raw:
        raw = "https://" + raw
    host = (urlparse(raw).hostname or "").lower()
    return host == host_name or host.endswith("." + host_name)


def _deck_from_sends(session: Optional[Session], email: str) -> dict:
    if not email:
        return {}
    ok, rows = _query(
        session,
        "SELECT deck_url, status FROM abby_deck_sends "
        "WHERE lower(lead) = :e AND trim(deck_url) <> '' "
        "ORDER BY CASE WHEN lower(status) = 'sent' THEN 0 ELSE 1 END, id DESC "
        "LIMIT 5",
        {"e": email},
    )
    if not ok:
        return {}
    for row in rows:
        if str(row.get("status") or "").strip().lower() != "sent":
            continue
        url = _https_url(row.get("deck_url") or "")
        if url:
            return {
                "url": url,
                "run_id": _run_id_from_deck_url(url),
                "source": "abby_deck_sends",
                "status": "sent",
            }
    return {}


def _deck_from_runs(session: Optional[Session], domain: str) -> dict:
    """Latest successful deck whose target site is this domain. Does not read deck HTML."""

    if session is None or not domain:
        return {}
    try:
        dialect = session.get_bind().dialect.name
    except Exception:
        return {}
    target = _json_text(dialect, "metadata_json", "target_product_input")
    site = _json_text(dialect, "metadata_json", "brand_website")
    view = _json_text(dialect, "summary_json", "view_url")
    ok, rows = _query(
        session,
        "SELECT id, "
        f"{view} AS view_url, {target} AS target, {site} AS site "
        "FROM automation_runs "
        "WHERE run_type = 'deck_generation' AND status = 'success' "
        f"AND {view} <> '' "
        f"AND (lower({target}) LIKE :pat OR lower({site}) LIKE :pat) "
        "ORDER BY id DESC LIMIT 8",
        {"pat": f"%{domain.lower()}%"},
    )
    if not ok:
        return {}
    for row in rows:
        if not (
            _host_matches(str(row.get("target") or ""), domain)
            or _host_matches(str(row.get("site") or ""), domain)
        ):
            continue
        url = _https_url(row.get("view_url") or "")
        if not url:
            continue
        return {
            "url": url,
            "run_id": str(row.get("id") or "") or _run_id_from_deck_url(url),
            "source": "deck_generation",
            "status": "success",
        }
    return {}


def lookup_deck(session: Optional[Session], email: str, domain: str) -> dict:
    """The strategy deck sent to this prospect, else the latest generated deck for the domain."""

    found = _deck_from_sends(session, email)
    if found.get("url"):
        return found
    return _deck_from_runs(session, domain)


def _scrub_repeat(value: str) -> str:
    text = _AMAZON_REPEAT.sub("", no_em_dash(str(value or "")))
    text = re.sub(r"[ \t]{2,}", " ", text)
    text = re.sub(r"\s+\.", ".", text)
    return text.strip()


def _team_written(detail: str) -> bool:
    return "written by our team" in str(detail or "").lower()


def _abby_rows(context: dict) -> list[dict]:
    return list((context.get("abby_handled") or {}).get("rows") or [])


def _nudge_count(rows: list[dict]) -> int:
    count = 0
    for row in rows:
        action = str(row.get("action") or "").lower()
        category = str(row.get("category") or "").lower()
        if action == "nudge" or category == "nudge":
            count += 1
    return count


def _reply_count(rows: list[dict]) -> int:
    count = 0
    for row in rows:
        if str(row.get("action") or "").lower() != "reply":
            continue
        if _team_written(str(row.get("detail") or "")):
            continue
        count += 1
    return count


def _prospect_lines(booking: Booking, context: dict) -> list[str]:
    """Lines the prospect wrote. Team mail, nudges, and quoted threads stay out."""

    email = (booking.prospect_email or "").lower()
    lines: list[str] = []
    seen: set[str] = set()

    def add(value: str) -> None:
        cleaned = clean_message_text(value)
        if not cleaned:
            return
        key = cleaned.casefold()
        if key in seen:
            return
        seen.add(key)
        lines.append(cleaned)

    for row in _abby_rows(context):
        action = str(row.get("action") or "").lower()
        detail = str(row.get("detail") or "")
        if action in {"nudge", "skip"} or _team_written(detail):
            continue
        match = re.search(r"(?is)\bTHEM:\s*(.+)", detail)
        if match:
            add(match.group(1))
    notes = list(context.get("gmail") or []) + list(context.get("mailbox") or [])
    notes += list(((context.get("instantly") or {}).get("emails") or []))
    for note in notes:
        subject = str(note.get("subject") or "")
        if _NOISE_SUBJECT.search(subject.strip()):
            continue
        sender = str(note.get("from") or "").lower()
        if email and email in sender:
            add(str(note.get("snippet") or ""))
    for comment in (context.get("clickup") or {}).get("comments") or []:
        text_value = str(comment.get("text") or "")
        marker = re.search(r"(?is)\b(?:said|wrote):\s*(.+)", text_value)
        if marker:
            add(marker.group(1))
    return lines


def _clickup_notes(context: dict) -> list[str]:
    notes = []
    for comment in (context.get("clickup") or {}).get("comments") or []:
        text_value = str(comment.get("text") or "")
        if re.search(r"(?is)\b(?:said|wrote):", text_value):
            continue
        cleaned = clean_message_text(text_value)
        if cleaned and len(cleaned) <= 180:
            notes.append(cleaned)
    return notes


def _first_sentences(lines: list[str], limit: int) -> list[str]:
    chosen: list[str] = []
    for line in lines:
        parts = [part.strip() for part in _SENTENCE.split(line) if part.strip()] or [line]
        for part in parts:
            if part[-1:] not in ".!?":
                part += "."
            chosen.append(part)
            if len(chosen) >= limit:
                return chosen
    return chosen


def _deck_was_sent(context: dict) -> bool:
    deck = context.get("deck") if isinstance(context.get("deck"), dict) else {}
    if not _https_url(deck.get("url") or ""):
        return False
    return (
        str(deck.get("source") or "") == "abby_deck_sends"
        and str(deck.get("status") or "").lower() == "sent"
    )


def _sent_sentence(context: dict) -> str:
    rows = _abby_rows(context)
    parts = []
    if _deck_was_sent(context):
        parts.append("a strategy deck")
    elif _reply_count(rows):
        parts.append("a reply")
    nudges = _nudge_count(rows)
    if nudges:
        parts.append(f"{nudges} follow-up nudge" + ("s" if nudges != 1 else ""))
    if not parts:
        return ""
    if len(parts) == 1:
        body = parts[0]
    elif len(parts) == 2:
        body = f"{parts[0]} and {parts[1]}"
    else:
        body = ", ".join(parts[:-1]) + ", and " + parts[-1]
    return f"We sent {body}."


def _how_they_came_in(rows: list[dict]) -> str:
    """A short source line. Skip nudge logs and mail our team wrote."""

    chosen = None
    for row in rows:
        action = str(row.get("action") or "").lower()
        category = str(row.get("category") or "").lower()
        if action in {"skip", "nudge"} or category == "nudge":
            continue
        if _team_written(str(row.get("detail") or "")):
            continue
        chosen = row
        break
    if chosen is None:
        return "Abby outreach is on file."
    category = str(chosen.get("category") or "unspecified")
    action = str(chosen.get("action") or "unspecified")
    if category.lower() == "interested" and action.lower() == "reply":
        how = "They replied to Abby's outreach."
    else:
        how = f"Abby history on file: {category} / {action}."
    detail = _short_abby_detail(chosen)
    if detail:
        how = f"{how} {detail}"
    return how


def _short_abby_detail(row: dict) -> str:
    detail = " ".join(str(row.get("detail") or "").split())
    if not detail or _team_written(detail) or re.search(r"(?i)\bTHEM:", detail):
        return ""
    if _NUDGE_ONLY.match(detail) or _THREAD_MARK.search(detail) or len(detail) > 120:
        return ""
    return detail


def situation_summary(booking: Booking, context: dict) -> str:
    """Two or three sentences: what the prospect said, and what we sent."""

    said = _first_sentences(_prospect_lines(booking, context), 2)
    sent = _sent_sentence(context)
    sentences = list(said)
    if sent:
        sentences.append(sent)
    deck = context.get("deck") if isinstance(context.get("deck"), dict) else {}
    if _https_url(deck.get("url") or "") and not _deck_was_sent(context) and len(sentences) < 3:
        sentences.append("A strategy deck is available.")
    if len(sentences) < 3:
        for note in _clickup_notes(context):
            sentence = note if note[-1:] in ".!?" else note + "."
            if sentence not in sentences:
                sentences.append("ClickUp note: " + sentence)
            if len(sentences) >= 3:
                break
    if not sentences:
        return "Unknown. No Abby, ClickUp, Gmail, or Instantly history matched this email."
    return no_em_dash(" ".join(sentences[:3]))


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
        if subject.lower().startswith("call prep:") or _NOISE_SUBJECT.search(subject.strip()):
            continue
        body = extract_body_text(payload) or str(full.get("snippet") or "")
        cleaned = clean_message_text(body)
        if not cleaned:
            continue
        rows.append({
            "from": extract_header(headers, "from"),
            "date": extract_header(headers, "date"),
            "subject": subject[:180],
            "snippet": cleaned[:400],
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
        subject = str(item.get("subject") or "")
        if _NOISE_SUBJECT.search(subject.strip()):
            continue
        cleaned = clean_message_text(text_value)
        if not cleaned:
            continue
        emails.append({
            "from": str(item.get("from_address_email") or item.get("lead") or ""),
            "subject": subject[:180],
            "snippet": cleaned[:400],
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
    company = clean_company_name(company)
    deck = lookup_deck(session, email, domain)
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
        "deck": deck,
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

    return "\n".join(_prospect_lines(booking, context)[:4])


def discovery_map(booking: Booking, context: dict) -> list[dict]:
    """Facts and script angles for sections s1 to s13. No new question bank."""

    links = context.get("links") or {}
    website = (links.get("website") or {})
    amazon = (links.get("amazon") or {})
    company = clean_company_name(str(context.get("company") or "")) or "Unknown"
    if website.get("verified") and website.get("url"):
        site_line = f"Website: {website.get('url')}."
    else:
        site_line = "Website: not verified."
    channel = "The fetched homepage is a Shopify store." if links.get("shopify") else ""
    purpose = booking.purpose or "Unknown"
    known = {
        "s1": f"Booking form: {purpose}.",
        "s2": " ".join(
            part for part in (
                f"Company on file: {company}.",
                site_line,
                channel,
            ) if part
        ),
        "s3": situation_summary(booking, context),
        "s4": "Unknown. Ask section 4. Do not treat follow-up emails as their current ad setup.",
        "s5": "Unknown. No verified account numbers are on file. Use the script's number questions.",
        "s6": "Unknown. Use section 6.",
        "s7": "Unknown. Use section 7 after they name the gap.",
        "s8": "Unknown. Use section 8.",
        "s9": "Unknown. Use section 9. Prior emails are context, not a list of solutions they tried.",
        "s10": f"They booked for: {purpose}.",
        "s11": "Unknown who else decides. Use section 11.",
        "s12": "Unknown. Use section 12.",
        "s13": "Use their words from the quotes field when it has a line. Otherwise use section 13.",
    }
    angles = {
        "s1": "Use section 1. Start from the booking reason, then the script's biggest-challenge question.",
        "s2": "Use section 2. Only the verified website and Amazon lines above are safe to say.",
        "s3": "Use section 3. Read back what they said, then ask them to say it in their words.",
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

    company = clean_company_name(str(context.get("company") or ""))
    email = booking.prospect_email or "Unknown"
    who = f"{booking.prospect_name or 'Unknown'} ({email})."
    who += f" Company: {company}." if company else " Company: Unknown."
    abby = (context.get("abby_handled") or {}).get("rows") or []
    outbound = (context.get("outbound_enriched") or {}).get("row") or (context.get("outbound_contact") or {}).get("row")
    summary = situation_summary(booking, context)
    if abby:
        how = _how_they_came_in(abby)
    elif outbound:
        how = "Matched an outbound contact already stored for this email."
    elif (context.get("clickup") or {}).get("matched"):
        how = "Matched an existing ClickUp Sales CRM task. No outbound or Abby row was on file."
    elif not summary.startswith("Unknown"):
        how = summary
    else:
        how = "Unknown. No outbound row, Abby history, or ClickUp task matched this email."
    needs = booking.purpose or "Unknown. The booking form answer was empty."
    links = context.get("links") or {}
    unknowns = []
    if not company:
        unknowns.append("Company")
    if not booking.purpose:
        unknowns.append("Booking form answer")
    if summary.startswith("Unknown") and not abby and not outbound:
        unknowns.append("How they came in")
    return {
        "who": who,
        "how_they_came_in": how,
        "likely_needs": needs,
        "discovery": discovery_map(booking, context),
        "trial_pitch": list(PITCH_POINTS),
        "objections": [
            {
                "objection": "Prepared, not raised by them: we already have someone doing this.",
                "answer": "The 14-day trial sits beside that. It is free for 14 days with a card on file, then $99/mo Starter.",
            },
            {
                "objection": "Prepared, not raised by them: $99 is too small to matter.",
                "answer": "Plans start at $99/mo. The first 14 days are free, so they can see the account before they pay.",
            },
            {
                "objection": "Prepared, not raised by them: prove it works.",
                "answer": (
                    f"{PROOF_LINE} That is one published account, not a promise for this prospect."
                ),
            },
        ],
        "next_step": (
            "Ask them to start the free 14-day trial before the call ends, "
            "and connect Seller Central on the call."
        ),
        "unknowns": unknowns,
    }


def _strip_banned_proof(value: str) -> str:
    """Drop any sentence that names a client we no longer use as proof."""

    text = str(value or "")
    if not _BANNED_PROOF.search(text):
        return text
    kept = [
        part.strip()
        for part in _SENTENCE.split(text)
        if part.strip() and not _BANNED_PROOF.search(part)
    ]
    return " ".join(kept).strip()


def prep_contains_banned_proof(value: Any) -> bool:
    return bool(_BANNED_PROOF.search(json.dumps(value, default=str)))


def _ensure_shape(notes: dict, booking: Booking, context: dict) -> dict:
    base = fallback_notes(booking, context)
    if not isinstance(notes, dict):
        return base
    cleaned = _walk_dashes(notes)
    cleaned.pop("discovery_questions", None)
    cleaned["discovery"] = base["discovery"]
    pitch = [
        no_em_dash(_strip_banned_proof(str(item))).strip()
        for item in as_list(cleaned.get("trial_pitch"))
        if not isinstance(item, dict) and str(item).strip() and not _BANNED_PROOF.search(str(item))
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
            objection = no_em_dash(_strip_banned_proof(str(item.get("objection") or "Unknown")))
            answer = no_em_dash(_strip_banned_proof(str(item.get("answer") or "Unknown")))
            if _BANNED_PROOF.search(objection) or _BANNED_PROOF.search(answer) or not objection.strip():
                continue
            normalized.append({"objection": objection.strip() or "Unknown", "answer": answer.strip() or "Unknown"})
        elif str(item).strip() and not _BANNED_PROOF.search(str(item)):
            normalized.append({"objection": no_em_dash(str(item)), "answer": "Unknown"})
    cleaned["objections"] = normalized or base["objections"]
    for key in ("who", "how_they_came_in", "likely_needs", "next_step"):
        text_value = no_em_dash(_strip_banned_proof(str(cleaned.get(key) or ""))).strip()
        if not text_value or text_value.lower().rstrip(".") == "unknown" or _BANNED_PROOF.search(text_value):
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
        "The trial pitch must include these approved points and no other client or proof number: "
        f"Free 14-day trial. Plans from $99/mo. {PROOF_LINE} "
        "Do not name any other client and do not invent a KPI. "
        "Do not write discovery questions. The call script already has sections s1 to s13. "
        "trial_pitch must be a JSON array of strings, never one paragraph. "
        "Do not say the prospect is on Amazon unless the packet's amazon.verified is true, "
        "and then only repeat the resolved URLs. Do not write 'Amazon presence: not verified' "
        "anywhere; that line is added once, outside these notes. "
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


def _objection_lines(objections: list) -> list[str]:
    """One prepared label for the whole group, then a short objection and answer."""

    prepared: list[tuple[str, str]] = []
    raised: list[tuple[str, str]] = []
    for item in objections:
        if not isinstance(item, dict):
            continue
        question = _scrub_repeat(str(item.get("objection") or "")).strip() or "Unknown"
        answer = _scrub_repeat(str(item.get("answer") or "")).strip()
        if _PREPARED.match(question):
            prepared.append((_PREPARED.sub("", question).strip() or "Unknown", answer))
        else:
            raised.append((question, answer))
    lines: list[str] = []

    def add(title: str, pairs: list[tuple[str, str]]) -> None:
        if not pairs:
            return
        lines.append(title)
        for question, answer in pairs:
            lines.append(f"- {question}")
            if answer and answer.lower() != "unknown":
                lines.append(f"  {answer}")

    add("Objections prepared, not raised by them:", prepared)
    add("Objections they raised:", raised)
    if not lines:
        lines.append("Objections:")
        lines.append("- Unknown")
    return lines


def _discovery_known(sections: dict, step_id: str) -> str:
    for item in as_list(sections.get("discovery")):
        if isinstance(item, dict) and str(item.get("id") or "") == step_id:
            return str(item.get("known") or "").strip()
    return ""


def slack_text(tracker: SalesCallTracker, prep: dict, timing_note: str) -> str:
    """Short brief for Slack and the Gmail draft. The call page keeps the full script."""

    sections = prep.get("sections") if isinstance(prep.get("sections"), dict) else {}
    pitch = [item for item in as_list(sections.get("trial_pitch")) if not _BANNED_PROOF.search(str(item))]
    objections = [
        item for item in as_list(sections.get("objections"))
        if not _BANNED_PROOF.search(json.dumps(item, default=str))
    ]
    lines = [
        f"Call prep: {prep.get('prospect_name') or tracker.prospect_name or 'Unknown'}",
        f"When: {prep.get('when') or 'Unknown'}",
        f"Purpose: {prep.get('purpose') or 'Unknown'}",
        f"Call script: {tracker_url(tracker.id)}",
    ]
    deck = prep.get("deck") if isinstance(prep.get("deck"), dict) else {}
    deck_url = _https_url(deck.get("url") or "")
    if deck_url:
        lines.append(f"Deck: {deck_url}")
    if timing_note:
        lines.append(timing_note)
    if prep.get("meet_url"):
        lines.append(f"Meet: {prep['meet_url']}")
    lines += [
        "",
        "Who: " + (_scrub_repeat(str(sections.get("who") or "")) or "Unknown"),
        "How they came in: " + (_scrub_repeat(str(sections.get("how_they_came_in") or "")) or "Unknown"),
        "Likely needs: " + (_scrub_repeat(str(sections.get("likely_needs") or "")) or "Unknown"),
        "",
    ]
    situation = _scrub_repeat(_discovery_known(sections, "s3"))
    if situation and not situation.lower().startswith("unknown"):
        lines += ["Current situation:", situation, ""]
    links = prep.get("links") if isinstance(prep.get("links"), dict) else {}
    website = links.get("website") if isinstance(links.get("website"), dict) else {}
    amazon = links.get("amazon") if isinstance(links.get("amazon"), dict) else {}
    if website.get("verified") and website.get("url"):
        lines.append(f"Website: {website['url']}")
    else:
        lines.append("Website: not verified")
    if amazon.get("line"):
        lines.append(str(amazon["line"]))
    elif not amazon.get("verified"):
        lines.append(AMAZON_UNVERIFIED)
    lines.append("")
    lines.append("Trial pitch:")
    for point in pitch:
        lines.append(f"- {point}")
    lines.append("")
    lines.extend(_objection_lines(objections))
    lines += ["", "Next step: " + (_scrub_repeat(str(sections.get("next_step") or "")) or "Unknown")]
    unknowns = [
        str(item) for item in (prep.get("unknowns") or [])
        if str(item).strip() and AMAZON_UNVERIFIED.casefold() not in str(item).casefold()
    ]
    if unknowns:
        lines += ["", "Unknown: " + ", ".join(unknowns)]
    text = no_em_dash("\n".join(lines))
    kept = [line for line in text.split("\n") if not _BANNED_PROOF.search(line)]
    return "\n".join(kept)[:3500]


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
    replace_id = str(prep.pop("replace_draft_id", "") or "")
    force_draft = bool(prep.pop("force_draft", False))
    draft_text = slack_text(tracker, prep, timing_note)
    draft_subject = f"Call prep: {prep.get('prospect_name') or tracker.prospect_name}"
    if replace_id and gmail is not None and gmail.is_configured() and getattr(gmail, "update_draft", None):
        try:
            result = gmail.update_draft(
                replace_id,
                to=(DAVID_EMAIL,),
                subject=draft_subject,
                text=draft_text,
            )
            if not (isinstance(result, dict) and result.get("skipped")):
                draft_state["id"] = str((result or {}).get("id") or replace_id)
                draft_state["to"] = DAVID_EMAIL
                draft_state["skipped"] = ""
                draft_state["updated"] = True
                replace_id = ""
        except Exception as exc:
            logger.warning("[call-prep] draft update failed: %s", type(exc).__name__)
            draft_state["error"] = type(exc).__name__
            replace_id = ""
    if not draft_state.get("id") and (post or force_draft):
        if gmail is None or not gmail.is_configured():
            draft_state["skipped"] = "Gmail draft path is not configured."
        else:
            try:
                result = gmail.create_draft(
                    to=(DAVID_EMAIL,),
                    subject=draft_subject,
                    text=draft_text,
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
    force: bool = False,
) -> dict:
    """Create or reuse the lead's tracker, store ss-prep, then notify David.

    A tracker that already has prep for this event is left as-is, including
    its draft. Prep that still names a banned client is rebuilt. The draft
    is updated for David only. Nothing is sent.
    """

    existing = find_tracker(session, booking)
    if existing is not None:
        prior = existing.state_json if isinstance(existing.state_json, dict) else {}
        saved = prior.get("ss-prep") if isinstance(prior.get("ss-prep"), dict) else {}
        stale = prep_contains_banned_proof(saved)
        finished = (
            saved.get("event_id") == booking.event_id
            and isinstance(saved.get("sections"), dict)
            and (saved.get("slack") or {}).get("posted_at")
            and ((saved.get("draft") or {}).get("id") or (saved.get("draft") or {}).get("skipped"))
            and not stale
            and not force
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
    refresh = force or prep_contains_banned_proof(current)
    if same_event and isinstance(current.get("sections"), dict) and not refresh:
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
            "deck": {
                "url": _https_url((context.get("deck") or {}).get("url") or ""),
                "run_id": str((context.get("deck") or {}).get("run_id") or ""),
                "source": str((context.get("deck") or {}).get("source") or ""),
            },
            "unknowns": sections.get("unknowns") or [],
            "slack": dict(current.get("slack") or {}) if refresh and not created else {},
            "draft": {},
        }
        if refresh and not created:
            prep["replace_draft_id"] = str((current.get("draft") or {}).get("id") or "")
            prep["force_draft"] = True
        action = "created" if created else ("refreshed" if refresh else "prepped")
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


def booking_from_tracker(tracker: SalesCallTracker) -> Optional[Booking]:
    """The booking stored on a tracker, so an existing prep can be rebuilt."""

    state = tracker.state_json if isinstance(tracker.state_json, dict) else {}
    prep = state.get("ss-prep") if isinstance(state.get("ss-prep"), dict) else {}
    if not prep:
        return None

    def parse(raw: Any) -> Optional[datetime]:
        if not raw:
            return None
        try:
            return datetime.fromisoformat(str(raw))
        except ValueError:
            return None

    return Booking(
        event_id=str(prep.get("event_id") or f"tracker:{tracker.id}"),
        summary=str(tracker.prospect_name or ""),
        prospect_name=str(prep.get("prospect_name") or tracker.prospect_name or ""),
        prospect_email=str(prep.get("prospect_email") or ""),
        purpose=str(prep.get("purpose") or ""),
        starts_at=parse(prep.get("starts_at")),
        ends_at=parse(prep.get("ends_at")),
        meet_url=str(prep.get("meet_url") or ""),
        source=str(prep.get("source") or "stored"),
    )


def regenerate_tracker(
    session: Session,
    tracker: SalesCallTracker,
    *,
    context_for: Callable[..., dict],
    notes_for: Callable[[Booking, dict], tuple[dict, str]],
    slack: Any,
    gmail: Any,
    now: datetime,
) -> dict:
    """Rebuild one tracker's prep and replace David's draft. Does not send mail."""

    booking = booking_from_tracker(tracker)
    if booking is None:
        raise ValueError("Tracker has no stored booking to rebuild.")
    return apply_booking(
        session,
        booking,
        context_for=context_for,
        notes_for=notes_for,
        slack=slack,
        gmail=gmail,
        now=now,
        force=True,
    )


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
        "prepped": sum(1 for row in rows if row["action"] in {"prepped", "refreshed"}),
        "slack": sum(1 for row in rows if row["slack"]),
        "drafts": sum(1 for row in rows if row["draft"]),
        "bookings": rows,
        "errors": errors,
    }
