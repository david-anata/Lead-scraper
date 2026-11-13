"""Inbound SMS conversation for the Anata receptionist.

Twilio posts each text to `/voice/sms`. When SMS auto-reply is on, this
module keeps one thread per sender and line, asks for the same details the
voice flow collects, and writes them through the call log. A building event
becomes a `building_inquiries` row when the details are complete. anata Inc.
threads stay on the call log, matching a voice lead for that line.

Replies are plain sentences. Prices are only the published ones. David's
alert is decided here and sent once by the webhook, not on every text.
"""

from __future__ import annotations

import logging
import re
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

from voice.attribution import prefers_spanish
from voice.knowledge import ADDRESS, PUBLISHED_EMAIL, RECOMMENDED_SECURITY_LINE, parse_iso_date
from voice.known_callers import apply_known_caller, first_name, looks_like_tenant_urgent
from voice.lines import LineContext, normalize_phone, resolve_line
from voice.screening import looks_like_emergency, looks_like_person_request, looks_like_solicitation
from voice.session import CallSession, normalize_security_preference
from voice.settings import Settings
from voice.sms_setup import RECEPTIONIST_NUMBERS

logger = logging.getLogger(__name__)

THREAD_WINDOW = timedelta(hours=18)
MAX_REPLIES = 12
# +18018752284 was already asked for VELORA event details on Nov 13, 2026.
CONTINUED_SENDER = "+18018752284"
CONTINUED_UNTIL = date(2026, 12, 13)

OPT_OUT = frozenset({"stop", "stopall", "unsubscribe", "cancel", "end", "quit"})
OPT_IN = frozenset({"start", "unstop"})
HELP_WORDS = frozenset({"help", "info"})

_AUTOMATED = (
    "auto-reply",
    "automatic reply",
    "this is an automated",
    "out of office",
    "do not reply",
    "don't reply",
    "this number is not monitored",
)

_LEGAL_CLAIMS = (
    "fondell",
    "boulder ranch",
    "this is mckay",
    "i'm mckay",
    "i am mckay",
    "from mckay",
    "mckay law",
    "mckay legal",
)

_MONTHS = {
    "january": 1,
    "jan": 1,
    "february": 2,
    "feb": 2,
    "march": 3,
    "mar": 3,
    "april": 4,
    "apr": 4,
    "may": 5,
    "june": 6,
    "jun": 6,
    "july": 7,
    "jul": 7,
    "august": 8,
    "aug": 8,
    "september": 9,
    "sep": 9,
    "sept": 9,
    "october": 10,
    "oct": 10,
    "november": 11,
    "nov": 11,
    "december": 12,
    "dec": 12,
}
_WEEKDAYS = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
}

_EMAIL = re.compile(r"\b[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}\b", re.I)
_NAMED_EVENT = re.compile(r"\b([A-Z][A-Za-z0-9&']{2,})\s+event\b")
_MONTH_DAY = re.compile(
    r"\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
    r"jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|"
    r"dec(?:ember)?)\s+(\d{1,2})(?:st|nd|rd|th)?(?:,?\s*(20\d{2}))?\b",
    re.I,
)
_NUMERIC_DATE = re.compile(r"\b(\d{1,2})[/-](\d{1,2})(?:[/-](\d{2,4}))?\b")
_ISO_DATE = re.compile(r"\b(20\d{2})-(\d{2})-(\d{2})\b")
_TIME = re.compile(
    r"\b(\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?)?)\s*(?:to|-|–|—|until)\s*"
    r"(\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?)?|midnight|noon)\b",
    re.I,
)
_GUESTS = re.compile(
    r"\b(\d{1,4})\s*(?:guests?|people|ppl|persons?|attendees)\b"
    r"|\b(?:guests?|people|group of|party of|team of)\s*(\d{1,4})\b",
    re.I,
)
_NAME = re.compile(
    r"\b(?:my name is|this is|i am|i'm|im|it's|it is|name is|name:)\s+"
    r"([A-Za-z][A-Za-z'’\-]+(?:\s+[A-Za-z][A-Za-z'’\-]+){0,3})",
    re.I,
)
_BRAND = re.compile(
    r"\b(?:company|brand|business)\s+(?:is|called|:)\s+([A-Za-z0-9][\w&'.\- ]{1,40})",
    re.I,
)

_EVENT_TYPES = (
    ("quinceañera", "quinceañera"),
    ("quinceanera", "quinceanera"),
    ("quince", "quince"),
    ("baby shower", "baby shower"),
    ("bridal", "bridal shower"),
    ("wedding", "wedding"),
    ("birthday", "birthday"),
    ("corporate", "corporate event"),
    ("conference", "conference"),
    ("workshop", "workshop"),
    ("bni", "BNI"),
    ("meeting", "meeting"),
    ("reception", "reception"),
    ("fundraiser", "fundraiser"),
    ("graduation", "graduation"),
    ("anniversary", "anniversary"),
    ("concert", "concert"),
    ("gala", "gala"),
    ("party", "party"),
)

_NOT_A_NAME = frozenset(
    {
        "yes",
        "no",
        "yeah",
        "yep",
        "nope",
        "stop",
        "help",
        "start",
        "skip",
        "google",
        "saturday",
        "sunday",
        "monday",
        "event",
        "party",
        "wedding",
        "anata",
        "david",
        "hi",
        "hello",
        "thanks",
        "thank",
    }
)


@dataclass
class SmsOutcome:
    """What the webhook should send after one inbound text."""

    reply: str = ""
    notify: bool = False
    from_number: str = ""
    to_number: str = ""
    session: CallSession | None = None


class SmsThreadStore:
    """In-memory threads for the life of this process. The call log is the copy that survives a restart."""

    def __init__(self) -> None:
        self._items: dict[str, CallSession] = {}
        self._locks: dict[str, threading.Lock] = {}
        self._guard = threading.Lock()

    def lock_for(self, key: str) -> threading.Lock:
        with self._guard:
            lock = self._locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._locks[key] = lock
            return lock

    def get(self, key: str) -> CallSession | None:
        with self._guard:
            return self._items.get(key)

    def put(self, key: str, session: CallSession) -> None:
        with self._guard:
            cutoff = time.time() - 24 * 60 * 60
            stale = [item for item, saved in self._items.items() if saved.started_at < cutoff]
            for item in stale:
                self._items.pop(item, None)
            self._items[key] = session


def handle_inbound_sms(
    *,
    settings: Settings,
    store: Any,
    threads: SmsThreadStore,
    sender: str,
    recipient: str,
    body: str,
    message_sid: str,
    now: datetime | None = None,
) -> SmsOutcome:
    """Update one text thread and decide the reply and whether David should hear about it."""

    moment = now or datetime.now(timezone.utc)
    sender_id = normalize_phone(sender)
    recipient_id = normalize_phone(recipient)
    text = " ".join((body or "").split())
    outcome = SmsOutcome(from_number=recipient_id or recipient, to_number=sender_id or sender)
    if _is_staff_number(sender_id, recipient_id, settings):
        _log_ignored(store, message_sid, sender, recipient, text, "Ignored a text from our own number")
        return outcome
    if not sender_id or not text:
        _log_ignored(store, message_sid, sender, recipient, text or "(empty)", "Ignored an unusable text")
        return outcome

    key = f"{sender_id}|{recipient_id}"
    with threads.lock_for(key):
        session = _load_thread(
            settings,
            store,
            threads,
            key=key,
            sender=sender_id,
            recipient=recipient_id,
            moment=moment,
        )
        outcome.session = session
        _turn(session, store, text, moment)
        outcome.reply = str(session.sms_state.get("pending_reply") or "")
        outcome.notify = bool(session.sms_state.get("pending_notify"))
        session.sms_state["pending_reply"] = ""
        session.sms_state["pending_notify"] = False
        if outcome.reply:
            session.sms_state["last_reply"] = outcome.reply
            session.sms_state["reply_count"] = int(session.sms_state.get("reply_count") or 0) + 1
            session.add_turn("Anata", outcome.reply)
        create = bool(session.sms_state.get("complete") and not session.inquiry_id)
        _save(store, session, create_inquiry=create)
        threads.put(key, session)
    return outcome


def _turn(session: CallSession, store: Any, text: str, moment: datetime) -> None:
    """Apply one inbound text to the session. The reply is left on `pending_reply`."""

    state = session.sms_state
    state["pending_reply"] = ""
    state["pending_notify"] = False
    state["last_at"] = moment.timestamp()
    session.add_turn("Caller", text)
    keyword = _keyword(text)
    if keyword in OPT_OUT:
        _opt_out(session, state)
        return
    if keyword in HELP_WORDS:
        state["pending_reply"] = (
            f"Anata, {ADDRESS}. Email {PUBLISHED_EMAIL}. "
            "Reply STOP to opt out. Msg and data rates may apply."
        )
        return
    if state.get("opted_out"):
        if keyword in OPT_IN:
            state["opted_out"] = False
            state["pending_reply"] = "You're opted back in. What can I help you with?"
            session.action = "sms_collecting"
        return
    if keyword in OPT_IN:
        state["pending_reply"] = "You're opted in. What can I help you with?"
        return

    if prefers_spanish(text):
        state["spanish"] = True
    if _is_automated(text) or _is_echo(state, text):
        session.action = "sms_ignored"
        return
    if _too_many(state, moment):
        state["stopped_loop"] = True
        session.summary = "Sent too many texts, so I stopped replying"
        session.callback_needed = True
        session.action = "needs_callback"
        _notify_once(session, state)
        return

    if not state.get("known_loaded"):
        _load_known(session, store)
        state["known_loaded"] = True
    if _claims_legal(text) and session.known_caller_category in {"", "unknown"}:
        session.known_caller_category = "landlord_legal"
        session.ring_policy = "always"

    if _route_known(session, state, text):
        return
    mode = _mode_for(session, text)
    if mode:
        state["mode"] = mode
    if _is_spam(text, state.get("mode") or ""):
        session.category = "spam"
        session.action = "logged_only"
        session.callback_needed = False
        session.summary = "Sales pitch"
        return
    if _route_emergency(session, state, text):
        return
    if looks_like_person_request(text):
        _needs_david(session, state, text or "Asked for David")
        state["pending_reply"] = _with_opt_out(
            state,
            "Sure. I'll have David get back to you.",
        )
        return

    if not state.get("mode"):
        state["mode"] = "either"
    _capture(session, state, text, moment.date())
    if state.get("mode") == "either":
        chosen = _choose_mode(text)
        if chosen:
            state["mode"] = chosen
        else:
            state["pending_reply"] = _with_opt_out(
                state,
                _join_reply(session, state, "Are you asking about the building and Arena, or anata Inc.?"),
            )
            session.action = "sms_collecting"
            return

    question = _next_question(session, state)
    if question is None:
        _complete(session, state)
        return
    state["expecting"] = question
    if question == "security":
        state["security_asks"] = int(state.get("security_asks") or 0) + 1
    if question == "email":
        state["email_asks"] = int(state.get("email_asks") or 0) + 1
    if question == "revenue":
        state["revenue_asked"] = True
    reply = _with_opt_out(state, _join_reply(session, state, _question_text(session, state, question), text))
    state["pending_reply"] = reply
    session.action = "sms_collecting"
    if session.category in {"", "other"} and state.get("mode") in {"event", "agency", "workspace"}:
        session.category = "lead"


def _route_known(session: CallSession, state: dict[str, Any], text: str) -> bool:
    category = session.known_caller_category
    if category in {"", "unknown"}:
        return False
    name = session.known_caller_name
    first = first_name(name)
    if category == "landlord_legal":
        session.category = "other"
        session.action = "needs_callback"
        session.callback_needed = True
        session.summary = _clip(text) or "Landlord or legal text"
        who = f"Hello, {first}." if first else "Hello."
        state["pending_reply"] = _with_opt_out(state, f"{who} I'll have David get back to you.")
        _notify_once(session, state)
        return True
    if category in {"client", "vip"}:
        session.category = "other"
        session.action = "needs_callback"
        session.callback_needed = True
        session.summary = _clip(text) or "Asked for David"
        who = name or "there"
        state["pending_reply"] = _with_opt_out(state, f"Hi {who}. I'll have David get back to you.")
        _notify_once(session, state)
        return True
    if category == "tenant":
        session.category = "tenant"
        session.action = "needs_callback"
        session.callback_needed = True
        session.summary = _clip(text) or "Tenant text"
        who = first or "there"
        if looks_like_tenant_urgent(text) or looks_like_emergency(text):
            state["pending_reply"] = _with_opt_out(state, f"Hi {who}. I'll get David right away.")
        else:
            state["pending_reply"] = _with_opt_out(state, f"Hi {who}. I'll pass that to the team.")
        _notify_once(session, state)
        return True
    if category == "vendor":
        session.category = "vendor"
        session.action = "message"
        session.callback_needed = True
        session.summary = _clip(text) or "Vendor text"
        who = name or "there"
        state["pending_reply"] = _with_opt_out(state, f"Hi {who}. I'll pass that along to the team.")
        _notify_once(session, state)
        return True
    return False


def _route_emergency(session: CallSession, state: dict[str, Any], text: str) -> bool:
    if not looks_like_emergency(text):
        return False
    session.category = "urgent"
    session.action = "needs_callback"
    session.callback_needed = True
    session.summary = _clip(text)
    state["pending_reply"] = _with_opt_out(state, "I'll get David right away.")
    _notify_once(session, state)
    return True


def _needs_david(session: CallSession, state: dict[str, Any], reason: str) -> None:
    session.action = "needs_callback"
    session.callback_needed = True
    session.asked_for_david = True
    if session.category in {"", "other"}:
        session.category = "other"
    session.summary = _clip(reason)
    _notify_once(session, state)


def _notify_once(session: CallSession, state: dict[str, Any]) -> None:
    if state.get("alerted"):
        return
    state["alerted"] = True
    state["pending_notify"] = True


def _complete(session: CallSession, state: dict[str, Any]) -> None:
    mode = state.get("mode") or "event"
    if mode == "agency":
        if session.business_line not in {"agency", "fulfillment", "software"}:
            session.business_line = "agency"
    elif mode == "workspace":
        session.business_line = "workspace"
    else:
        session.business_line = "event"
    session.category = "lead"
    session.action = "message"
    session.callback_needed = True
    session.summary = _lead_summary(session)
    state["complete"] = True
    state["expecting"] = ""
    state["pending_reply"] = _closing(session, state)
    _notify_once(session, state)


def _closing(session: CallSession, state: dict[str, Any]) -> str:
    who = first_name(session.caller_name)
    prefix = f"Thanks, {who}. " if who else "Thanks. "
    if state.get("spanish"):
        prefix = f"Gracias, {who}. " if who else "Gracias. "
    if session.business_line in {"agency", "fulfillment", "software"}:
        return prefix + "The team will follow up. I won't guess a fee that isn't published."
    if session.business_line == "workspace":
        return prefix + "The team will follow up about the space. I won't guess a price that isn't published."
    bits = [part for part in (
        session.event_or_interest_type,
        _pretty_date(session.event_date),
        session.time_window,
        f"{session.guest_count} guests" if session.guest_count else "",
    ) if part]
    detail = ", ".join(bits)
    if state.get("spanish"):
        return prefix + "El equipo dará seguimiento. Este texto no reserva la fecha."
    if detail:
        return prefix + f"The team has this ({detail}) and will follow up. This text doesn't reserve the date."
    return prefix + "The team will follow up. This text doesn't reserve the date."


def _next_question(session: CallSession, state: dict[str, Any]) -> str | None:
    mode = state.get("mode") or "event"
    if mode == "agency":
        return _next_agency(session, state)
    if mode == "workspace":
        return _next_workspace(session, state)
    return _next_event(session, state)


def _next_event(session: CallSession, state: dict[str, Any]) -> str | None:
    if not session.event_or_interest_type:
        return "type"
    if not session.event_date:
        return "date"
    if not session.time_window:
        return "time"
    if not session.guest_count:
        return "guests"
    if not session.caller_name:
        return "name"
    if not session.email and not state.get("email_skipped") and int(state.get("email_asks") or 0) < 2:
        return "email"
    if _needs_security(session) and not session.security_preference:
        if int(state.get("security_asks") or 0) >= 2:
            session.security_preference = "undecided"
        else:
            return "security"
    return None


def _next_agency(session: CallSession, state: dict[str, Any]) -> str | None:
    if session.business_line not in {"agency", "fulfillment", "software"}:
        return "need"
    if not session.company:
        return "brand"
    if session.business_line != "software" and not session.marketplaces and not state.get("markets_skipped"):
        return "markets"
    if not session.revenue_range and not state.get("revenue_asked"):
        return "revenue"
    if not session.time_window and not state.get("timing_skipped"):
        return "timing"
    if not session.caller_name:
        return "name"
    if not session.email and not state.get("email_skipped") and int(state.get("email_asks") or 0) < 2:
        return "email"
    return None


def _next_workspace(session: CallSession, state: dict[str, Any]) -> str | None:
    if not session.event_or_interest_type:
        return "space"
    if not session.time_window and not state.get("timing_skipped"):
        return "timing"
    if not session.company and not state.get("brand_skipped"):
        return "brand"
    if not session.guest_count and not state.get("team_skipped"):
        return "team"
    if not session.caller_name:
        return "name"
    if not session.email and not state.get("email_skipped") and int(state.get("email_asks") or 0) < 2:
        return "email"
    return None


def _question_text(session: CallSession, state: dict[str, Any], key: str) -> str:
    spanish = bool(state.get("spanish"))
    if key == "security":
        if spanish:
            return (
                "Para un evento así recomendamos mucho la seguridad. Es opcional, "
                "pero ayuda a aprobar el evento. Es la tarifa del proveedor más un 20%. "
                "¿La agrego? Responde sí, no, o no sé."
            )
        return RECOMMENDED_SECURITY_LINE + " Reply yes, no, or not sure."
    english = {
        "type": "What kind of event is it?",
        "date": "What date are you looking at?",
        "time": "What time window do you need, start to finish?",
        "guests": "About how many guests?",
        "name": "What's your name?",
        "email": "What's the best email for the details?",
        "which": "Are you asking about the building and Arena, or anata Inc.?",
        "need": "What do you need help with: marketing, fulfillment, or software?",
        "brand": "What's the brand or company name?",
        "markets": "Where do you sell: Amazon, Shopify, TikTok Shop, or somewhere else?",
        "revenue": "Roughly what are sales per year? A range is fine, or say skip.",
        "timing": "When are you hoping to start?",
        "space": "Are you looking at an office, a desk, or warehouse space?",
        "team": "How big is the team?",
    }
    spanish_text = {
        "type": "¿Qué tipo de evento es?",
        "date": "¿Qué fecha tienen en mente?",
        "time": "¿A qué hora empieza y termina?",
        "guests": "¿Cuántos invitados, más o menos?",
        "name": "¿Cómo se llaman?",
        "email": "¿Cuál es el mejor correo?",
        "need": "¿Qué necesitan: marketing, fulfillment, o software?",
        "brand": "¿Cómo se llama la marca?",
        "name_agency": "¿Cómo se llaman?",
    }
    if spanish and key in spanish_text:
        return spanish_text[key]
    if key == "date" and session.event_or_interest_type and not state.get("continued"):
        kind = session.event_or_interest_type
        if spanish:
            return f"¿Qué fecha tienen en mente para {kind}?"
        return f"What date are you looking at for the {kind}?"
    return english.get(key, "What else should I pass along?")


def _join_reply(session: CallSession, state: dict[str, Any], question: str, inbound: str = "") -> str:
    extra = _aside(inbound, state, session)
    if not state.get("greeted"):
        state["greeted"] = True
        if state.get("continued"):
            lowered = question[0].lower() + question[1:] if question else ""
            head = f"Thanks. For the VELORA event, {lowered}".strip()
        elif state.get("spanish"):
            head = f"Gracias por escribir a Anata. {question}".strip()
        else:
            head = f"Thanks for texting Anata. {question}".strip()
        if extra:
            return f"{extra} {head}".strip()
        return head
    if extra:
        return f"{extra} {question}".strip()
    if question:
        return f"Got it. {question}"
    return ""


def _aside(inbound: str, state: dict[str, Any], session: CallSession) -> str:
    """A published fact, or a refusal to invent one. Empty when they didn't ask."""

    text = (inbound or "").casefold()
    if not text:
        return ""
    if any(word in text for word in ("available", "availability", "open that", "is that open")):
        return "I can't confirm a date by text. The team will check it."
    if any(word in text for word in ("hour", "hours", "what time are you open", "when do you close")):
        return (
            "Front desk is 9:00 a.m. to 4:00 p.m. Saturday events may run 8:00 a.m. to midnight, "
            "and Sunday events may run 8:00 a.m. to 10:00 p.m. You set up and clean up inside that time."
        )
    return _price_reply(text, state.get("mode") or "", session)


def _price_reply(text: str, mode: str, session: CallSession) -> str:
    if not any(word in text for word in ("price", "cost", "rate", "how much", "pricing", "fee")):
        return ""
    if any(word in text for word in ("office", "suite", "cowork", "private office", "warehouse")):
        return "That price isn't published. The team will quote it. Nothing is reserved by text."
    if mode == "agency" or any(
        word in text for word in ("marketing", "fulfillment", "amazon ads", "agency fee", "3pl")
    ):
        if any(word in text for word in ("software", "intelligence", "shipping os", "plan")):
            return (
                "Anata Intelligence is $99, $199, or $399 a month after a 14-day trial. "
                "The Agency plan starts at $999 a month. Marketing and fulfillment fees aren't published."
            )
        return "Marketing and fulfillment fees aren't published, so I won't guess a number. The team sends a range."
    if "deposit" in text:
        return "There's no security deposit. Nothing is reserved by text."
    if _weekday_business(text, session):
        return "Weekday business events are priced by the team. I won't guess a number. Nothing is reserved by text."
    return (
        "Weekend Arena rental, plus 7.45% Utah tax: half day up to 6 hours $1,500, full day $2,000. "
        "Weekday business events are priced by the team. Nothing is reserved by text."
    )


def _weekday_business(text: str, session: CallSession) -> bool:
    if any(word in text for word in ("saturday", "sunday", "weekend", "party", "wedding")):
        return False
    if any(word in text for word in ("weekday", "monday", "tuesday", "wednesday", "thursday", "friday", "workshop", "meeting", "bni")):
        return True
    parsed = parse_iso_date(session.event_date)
    kind = f"{session.event_or_interest_type} {session.notes}".casefold()
    return bool(parsed and parsed.weekday() < 5 and any(word in kind for word in ("meeting", "workshop", "bni", "corporate")))


def _capture(session: CallSession, state: dict[str, Any], text: str, today: date) -> None:
    if not session.reason:
        session.reason = _clip(text, 240)
    email = _EMAIL.search(text)
    if email:
        session.email = email.group(0)
    found_date = _extract_date(text, today)
    if found_date:
        session.event_date = found_date
    found_time = _extract_time(text)
    if found_time:
        session.time_window = found_time
    guests = _extract_guests(text, state.get("expecting") or "")
    if guests:
        session.guest_count = guests
    kind = _extract_event_type(text)
    if kind and session.event_or_interest_type in {"", "event"}:
        session.event_or_interest_type = kind
    name = _extract_name(text, state.get("expecting") or "")
    if name and not session.caller_name:
        session.caller_name = name
    elif name and state.get("expecting") == "name":
        session.caller_name = name
    brand = _extract_brand(text, state.get("expecting") or "")
    if brand and not session.company:
        session.company = brand
    markets = _extract_markets(text)
    if markets:
        session.marketplaces = markets
    if state.get("expecting") == "revenue" or any(word in text.casefold() for word in ("revenue", "per year", "a year", "million")):
        revenue = _extract_revenue(text)
        if revenue:
            session.revenue_range = revenue
        elif state.get("expecting") == "revenue" and _declines(text):
            state["revenue_asked"] = True
    line = _service_line(text)
    if line:
        session.business_line = line
        if not session.event_or_interest_type:
            session.event_or_interest_type = line
    expecting = state.get("expecting") or ""
    if expecting == "security" or _mentions_security(text):
        answer = _security_answer(text)
        if answer:
            session.security_preference = answer
    if expecting == "email" and not email and (_declines(text) or int(state.get("email_asks") or 0) >= 2):
        state["email_skipped"] = True
    if expecting == "timing" and not found_time:
        if _declines(text):
            state["timing_skipped"] = True
        elif text and not _EMAIL.search(text):
            session.time_window = _clip(text, 80)
    if expecting == "markets" and not markets and _declines(text):
        state["markets_skipped"] = True
    if expecting == "team" and not guests:
        if _declines(text):
            state["team_skipped"] = True
        else:
            bare = _bare_count(text)
            if bare:
                session.guest_count = bare
    if expecting == "space":
        space = _space_type(text)
        if space:
            session.event_or_interest_type = space
    if expecting == "brand" and not session.company and not _declines(text):
        cleaned = _EMAIL.sub("", text).strip(" .,")
        if cleaned and len(cleaned) <= 80:
            session.company = cleaned
    if expecting == "heard":
        state["heard_asked"] = True
        if not _declines(text):
            session.heard_about_raw = _clip(text, 240)


def _needs_security(session: CallSession) -> bool:
    blob = f"{session.event_or_interest_type} {session.notes} {session.time_window} {session.reason}".casefold()
    party = any(
        word in blob
        for word in ("party", "wedding", "quince", "birthday", "shower", "gala", "reception", "concert", "night", "velora")
    )
    business = any(word in blob for word in ("meeting", "workshop", "bni", "offsite", "training"))
    if business and not party:
        return False
    if party:
        return True
    try:
        if int(session.guest_count) >= 50:
            return True
    except ValueError:
        pass
    parsed = parse_iso_date(session.event_date)
    return bool(parsed and parsed.weekday() >= 5)


def _security_answer(text: str) -> str:
    cleaned = " ".join(text.casefold().replace("'", "'").split())
    normalized = normalize_security_preference(cleaned)
    if normalized:
        return normalized
    if cleaned in {"y", "yeah", "yep", "yup", "sure", "ok", "okay", "please", "add it", "sí", "si"}:
        return "yes"
    if cleaned in {"n", "nope", "nah", "no thanks", "no thank you"}:
        return "no"
    if any(phrase in cleaned for phrase in ("not sure", "undecided", "maybe", "don't know", "dont know", "idk", "no sé", "no se")):
        return "undecided"
    if cleaned.startswith("yes"):
        return "yes"
    if cleaned.startswith("no "):
        return "no"
    return ""


def _mentions_security(text: str) -> bool:
    folded = text.casefold()
    return "security" in folded or "guard" in folded or "bouncer" in folded


def _opt_out(session: CallSession, state: dict[str, Any]) -> None:
    state["opted_out"] = True
    state["pending_notify"] = False
    session.category = session.category if session.category in {"lead", "tenant", "vendor", "urgent", "spam", "other"} else "other"
    if session.category == "lead" and not state.get("complete"):
        session.category = "other"
    session.action = "sms_opt_out"
    session.callback_needed = False
    if state.get("spanish"):
        state["pending_reply"] = "Listo. No enviaremos más textos. Responde START para volver a recibirlos."
    else:
        state["pending_reply"] = (
            "You're opted out of Anata texts. We won't text this number again. Reply START to opt back in."
        )


def _with_opt_out(state: dict[str, Any], reply: str) -> str:
    if state.get("opt_out_noted") or not reply:
        return reply
    state["opt_out_noted"] = True
    if "stop" in reply.casefold():
        return reply
    return f"{reply} Reply STOP to opt out."


def _load_thread(
    settings: Settings,
    store: Any,
    threads: SmsThreadStore,
    *,
    key: str,
    sender: str,
    recipient: str,
    moment: datetime,
) -> CallSession:
    current = threads.get(key)
    if current is not None:
        return current
    row = _latest(store, sender)
    if row and _same_line(row, recipient) and (_row_fresh(row, moment) or _row_opted_out(row)):
        restored = _session_from_row(settings, row, sender, recipient)
        if restored is not None:
            return restored
    line = resolve_line(settings.line_contexts_json, recipient)
    session = CallSession(
        call_sid=_thread_sid(sender, recipient, moment.timestamp()),
        caller_number=sender,
        dialed_number=recipient,
        line=line,
        started_at=moment.timestamp(),
        channel="sms",
        phone=sender,
    )
    _seed_continuation(session, moment.date())
    return session


def _session_from_row(settings: Settings, row: dict[str, Any], sender: str, recipient: str) -> CallSession | None:
    fields = _fields(row.get("lead_fields"))
    state = fields.get("_sms") if isinstance(fields.get("_sms"), dict) else {}
    started = _as_datetime(row.get("started_at")) or datetime.now(timezone.utc)
    line = resolve_line(settings.line_contexts_json, recipient or str(row.get("dialed_number") or ""))
    session = CallSession(
        call_sid=str(row.get("call_sid") or _thread_sid(sender, recipient, started.timestamp())),
        caller_number=sender,
        dialed_number=recipient,
        line=line,
        started_at=started.timestamp(),
        channel="sms",
        phone=sender,
        category=str(row.get("category") or "other"),
        caller_name=str(fields.get("name") or ""),
        company=str(fields.get("company") or ""),
        email=str(fields.get("email") or ""),
        event_date=str(fields.get("event_date") or ""),
        guest_count=str(fields.get("guest_count") or ""),
        event_or_interest_type=str(fields.get("type") or ""),
        business_line=str(fields.get("business_line") or ""),
        marketplaces=str(fields.get("marketplaces") or ""),
        revenue_range=str(fields.get("revenue_range") or ""),
        notes=str(fields.get("notes") or ""),
        reason=str(fields.get("reason") or ""),
        summary=str(row.get("transcript_summary") or ""),
        action=str(row.get("action_taken") or ""),
        inquiry_id=str(row.get("building_inquiry_id") or ""),
        known_caller_id=str(row.get("known_caller_id") or ""),
        known_caller_category=str(row.get("known_caller_category") or ""),
        heard_about_raw=str(fields.get("heard_about") or row.get("heard_about_raw") or ""),
        heard_about_source=str(fields.get("heard_about_source") or row.get("heard_about_source") or ""),
        time_window=str(fields.get("time_window") or ""),
        security_preference=str(fields.get("security_preference") or ""),
        sms_state=dict(state),
    )
    transcript = str(row.get("transcript") or "")
    session.turns = [line for line in transcript.splitlines() if line.strip()]
    if _row_opted_out(row):
        session.sms_state["opted_out"] = True
    session.sms_state["known_loaded"] = bool(session.known_caller_id or session.known_caller_category)
    return session


def _seed_continuation(session: CallSession, today: date) -> None:
    if session.caller_number != CONTINUED_SENDER or today > CONTINUED_UNTIL:
        return
    session.event_or_interest_type = "VELORA event"
    session.business_line = "event"
    session.category = "lead"
    session.reason = "Rent for a VELORA event"
    session.notes = "Texted on Nov 13 about renting for a VELORA event and was already asked for the details."
    session.sms_state["mode"] = "event"
    session.sms_state["continued"] = True


def _load_known(session: CallSession, store: Any) -> None:
    resolve = getattr(store, "resolve_caller", None)
    if resolve is None:
        return
    try:
        row = resolve(session.caller_number)
    except Exception:
        logger.exception("SMS known-caller lookup failed")
        return
    if isinstance(row, dict):
        apply_known_caller(session, row)


def _save(store: Any, session: CallSession, *, create_inquiry: bool) -> None:
    try:
        store.save_call(session, sms_notified=False, create_inquiry=create_inquiry)
    except Exception:
        logger.exception("SMS thread save failed for %s", session.call_sid)


def _log_ignored(store: Any, message_sid: str, sender: str, recipient: str, body: str, summary: str) -> None:
    save = getattr(store, "save_sms", None)
    if save is None or not message_sid:
        return
    try:
        save(
            message_sid=message_sid,
            caller_number=sender,
            dialed_number=recipient,
            body=body,
            category="other",
            summary=summary,
            sms_notified=False,
        )
    except Exception:
        logger.exception("Ignored SMS was not logged")


def _latest(store: Any, sender: str) -> dict[str, Any] | None:
    latest = getattr(store, "latest_sms", None)
    if latest is None:
        return None
    try:
        row = latest(sender)
    except Exception:
        logger.exception("SMS thread lookup failed")
        return None
    return row if isinstance(row, dict) else None


def intake_mode(line: LineContext) -> str:
    """event, agency, or either. Building and event lines collect an Arena lead."""

    ident = " ".join((line.id, line.source, line.label)).casefold()
    if line.id in {"agency", "fulfillment", "software"} or "track_inc" in ident or "anatainc" in ident:
        return "agency"
    building = (
        "event",
        "arena",
        "building",
        "peerspace",
        "eventective",
        "warehouse",
        "spaces",
        "front_desk",
        "gbp",
        "listing",
        "anata_services",
    )
    if line.id in {"front_desk", "anata_services"} or any(token in ident for token in building):
        return "event"
    return "either"


def _mode_for(session: CallSession, text: str) -> str:
    current = str(session.sms_state.get("mode") or "")
    if current in {"event", "agency", "workspace"}:
        return current
    content = _content_mode(text)
    line_mode = intake_mode(session.line)
    if line_mode == "agency":
        return "workspace" if content == "workspace" else "agency" if content != "event" else "event"
    if line_mode == "event":
        if content == "agency":
            return "agency"
        if content == "workspace":
            return "workspace"
        return "event"
    return content


def _content_mode(text: str) -> str:
    folded = text.casefold()
    if any(word in folded for word in ("office", "warehouse", "cowork", "a desk", "private office", "tour")):
        if not any(word in folded for word in ("wedding", "party", "arena", "guests")):
            return "workspace"
    if any(word in folded for word in ("amazon", "fulfillment", "fba", "shopify", "tiktok", "3pl", "software", "anata intelligence", "ppc")):
        return "agency"
    if any(word in folded for word in ("event", "arena", "wedding", "party", "guest", "rent", "rental", "venue", "quince", "birthday")):
        return "event"
    return ""


def _choose_mode(text: str) -> str:
    found = _content_mode(text)
    if found:
        return found
    folded = text.casefold()
    if any(word in folded for word in ("building", "arena", "event", "party")):
        return "event"
    if any(word in folded for word in ("anata inc", "inc", "marketing", "amazon", "software", "fulfillment")):
        return "agency"
    return ""


def _is_spam(text: str, mode: str) -> bool:
    return looks_like_solicitation(text, agency_lead=mode == "agency")


def _is_staff_number(sender: str, recipient: str, settings: Settings) -> bool:
    staff = {
        normalize_phone(settings.notify_sms_number),
        normalize_phone(settings.forward_to_number),
    }
    staff.discard("")
    if sender in staff or sender == recipient:
        return True
    return sender in {normalize_phone(number) for number in RECEPTIONIST_NUMBERS}


def _is_automated(text: str) -> bool:
    folded = text.casefold()
    return any(phrase in folded for phrase in _AUTOMATED)


def _is_echo(state: dict[str, Any], text: str) -> bool:
    last = str(state.get("last_reply") or "").strip()
    return bool(last) and text.strip() == last


def _too_many(state: dict[str, Any], moment: datetime) -> bool:
    if state.get("stopped_loop"):
        return True
    if int(state.get("reply_count") or 0) >= MAX_REPLIES:
        return True
    times = [item for item in state.get("inbound_times") or [] if isinstance(item, (int, float))]
    times.append(moment.timestamp())
    state["inbound_times"] = times[-12:]
    recent = [item for item in times if moment.timestamp() - item <= 30]
    return len(recent) >= 10


def _claims_legal(text: str) -> bool:
    folded = text.casefold()
    return any(phrase in folded for phrase in _LEGAL_CLAIMS)


def _keyword(text: str) -> str:
    return re.sub(r"[^a-z]", "", text.casefold())


def _declines(text: str) -> bool:
    folded = " ".join(text.casefold().split())
    return folded in {"no", "nope", "nah", "skip", "none", "n/a", "na", "pass", "not sure", "i don't know", "idk"} or folded.startswith("no ")


def _extract_event_type(text: str) -> str:
    named = _NAMED_EVENT.search(text)
    if named and named.group(1).casefold() not in {"the", "our", "this", "that", "new"}:
        return f"{named.group(1)} event"
    folded = text.casefold()
    for needle, label in _EVENT_TYPES:
        if needle in folded:
            return label
    return ""


def _extract_date(text: str, today: date) -> str:
    iso = _ISO_DATE.search(text)
    if iso:
        found = _safe_date(int(iso.group(1)), int(iso.group(2)), int(iso.group(3)))
        if found:
            return found.isoformat()
    named = _MONTH_DAY.search(text)
    if named:
        key = named.group(1).casefold()
        month = _MONTHS.get(key) or _MONTHS.get(key[:3], 0)
        day = int(named.group(2))
        year = int(named.group(3)) if named.group(3) else today.year
        found = _safe_date(year, month, day)
        if found and not named.group(3) and found < today:
            found = _safe_date(year + 1, month, day)
        if found:
            return found.isoformat()
    numeric = _NUMERIC_DATE.search(text)
    if numeric:
        month = int(numeric.group(1))
        day = int(numeric.group(2))
        raw_year = numeric.group(3)
        if raw_year:
            year = int(raw_year)
            if year < 100:
                year += 2000
        else:
            year = today.year
        found = _safe_date(year, month, day)
        if found and not raw_year and found < today:
            found = _safe_date(year + 1, month, day)
        if found:
            return found.isoformat()
    folded = text.casefold()
    if "tomorrow" in folded:
        return (today + timedelta(days=1)).isoformat()
    if re.search(r"\btoday\b", folded):
        return today.isoformat()
    for name, index in _WEEKDAYS.items():
        if name in folded:
            ahead = (index - today.weekday()) % 7
            if ahead == 0:
                ahead = 7
            return (today + timedelta(days=ahead)).isoformat()
    return ""


def _safe_date(year: int, month: int, day: int) -> date | None:
    try:
        return date(year, month, day)
    except ValueError:
        return None


def _extract_time(text: str) -> str:
    match = _TIME.search(text)
    if match:
        start = _clean_clock(match.group(1))
        end = _clean_clock(match.group(2))
        return f"{start} to {end}"
    folded = text.casefold()
    for label in ("half day", "full day", "morning", "afternoon", "evening", "all day"):
        if label in folded:
            return label
    return ""


def _clean_clock(value: str) -> str:
    return re.sub(r"\s+", "", value).replace(".", "").lower() or value.strip().lower()


def _extract_guests(text: str, expecting: str) -> str:
    match = _GUESTS.search(text)
    if match:
        return match.group(1) or match.group(2) or ""
    if expecting in {"guests", "team"}:
        return _bare_count(text)
    return ""


def _bare_count(text: str) -> str:
    match = re.fullmatch(r"\s*(?:about|around|roughly)?\s*(\d{1,4})\s*", text, re.I)
    if match:
        return match.group(1)
    return ""


def _extract_name(text: str, expecting: str) -> str:
    match = _NAME.search(text)
    if match:
        return _clean_name(match.group(1))
    if expecting != "name":
        return ""
    without = _EMAIL.sub(" ", text)
    parts = [part.strip(" .,") for part in without.replace(",", " ").split() if part.strip(" .,")]
    kept = [part for part in parts if re.fullmatch(r"[A-Za-z][A-Za-z'’\-]{0,30}", part)]
    if not kept or len(kept) > 4:
        return ""
    if kept[0].casefold() in _NOT_A_NAME:
        return ""
    return _clean_name(" ".join(kept))


def _clean_name(value: str) -> str:
    parts = [part for part in value.split() if part.casefold() not in _NOT_A_NAME]
    return " ".join(parts)[:80]


def _extract_brand(text: str, expecting: str) -> str:
    match = _BRAND.search(text)
    if not match:
        return ""
    return " ".join(match.group(1).split())[:80]


def _extract_markets(text: str) -> str:
    folded = text.casefold()
    found = [
        label
        for needle, label in (
            ("amazon", "Amazon"),
            ("tiktok", "TikTok Shop"),
            ("shopify", "Shopify"),
            ("walmart", "Walmart"),
            ("ebay", "eBay"),
            ("etsy", "Etsy"),
            ("instagram", "Instagram"),
        )
        if needle in folded
    ]
    return ", ".join(found)


def _extract_revenue(text: str) -> str:
    match = re.search(
        r"(\$?\s*\d[\d,.]*\s*(?:k|m|million|billion)?(?:\s*(?:a|per)\s*year)?)",
        text,
        re.I,
    )
    if not match:
        return ""
    return " ".join(match.group(1).split())[:80]


def _service_line(text: str) -> str:
    folded = text.casefold()
    if any(word in folded for word in ("fulfillment", "fba", "3pl", "prep center", "pick and pack")):
        return "fulfillment"
    if any(word in folded for word in ("software", "anata intelligence", "shipping os")):
        return "software"
    if any(word in folded for word in ("marketing", "amazon ads", "ppc", "listing")):
        return "agency"
    return ""


def _space_type(text: str) -> str:
    folded = text.casefold()
    if "warehouse" in folded:
        return "warehouse"
    if "desk" in folded or "bullpen" in folded or "cowork" in folded:
        return "desk"
    if "office" in folded:
        return "private office"
    return ""


def _lead_summary(session: CallSession) -> str:
    parts = [
        session.event_or_interest_type,
        _pretty_date(session.event_date),
        session.time_window,
        f"{session.guest_count} guests" if session.guest_count else "",
        session.company,
        session.marketplaces,
    ]
    text = ", ".join(part for part in parts if part)
    return text or session.reason or "Texted Anata"


def _pretty_date(value: str) -> str:
    parsed = parse_iso_date(value)
    if parsed is None:
        return ""
    return f"{parsed.strftime('%B')} {parsed.day}, {parsed.year}"


def _clip(text: str, limit: int = 180) -> str:
    cleaned = " ".join((text or "").split())
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: limit - 1].rstrip() + "…"


def _fields(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        import json

        try:
            loaded = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return loaded if isinstance(loaded, dict) else {}
    return {}


def _row_opted_out(row: dict[str, Any]) -> bool:
    state = _fields(row.get("lead_fields")).get("_sms")
    if isinstance(state, dict) and state.get("opted_out"):
        return True
    return str(row.get("action_taken") or "") == "sms_opt_out"


def _row_fresh(row: dict[str, Any], moment: datetime) -> bool:
    stamp = _as_datetime(row.get("updated_at") or row.get("started_at"))
    if stamp is None:
        return False
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    return moment - stamp <= THREAD_WINDOW


def _same_line(row: dict[str, Any], recipient: str) -> bool:
    saved = normalize_phone(str(row.get("dialed_number") or ""))
    return bool(saved) and saved == recipient


def _as_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    return None


def _thread_sid(sender: str, recipient: str, started: float) -> str:
    return f"sms:{sender.lstrip('+')}:{recipient.lstrip('+')}:{int(started)}"

