"""How the caller heard about Anata, kept apart from the dialed tracking line.

The dialed line (Peerspace, Eventective, the events page, and the rest) is
already stored on the call. This module is the caller's own answer. The
question is asked once, early, and skipped for indexed callers and emergencies.
The category is taken from the transcript after the call. A tool value is only
a fallback when the transcript never shows the question.
"""

from __future__ import annotations

import re

from voice.screening import looks_like_emergency, looks_like_solicitation
from voice.session import CallSession

HOW_HEARD_QUESTION = "By the way, how'd you hear about us?"
HOW_HEARD_QUESTION_ES = "Por cierto, ¿cómo se enteraron de nosotros?"

HEARD_ABOUT_SOURCES = (
    "google_search",
    "google_maps",
    "website",
    "peerspace",
    "eventective",
    "other_event_site",
    "referral",
    "social",
    "returning_customer",
    "drive_by",
    "other",
    "unknown",
)

# Indexed callers already know us. Family is stored as vip.
SKIP_KNOWN_CATEGORIES = frozenset({"tenant", "client", "landlord_legal", "vip"})

_ASK_RE = re.compile(
    r"how(?:'|’)?d you hear|how did you hear|where did you (?:hear|find)|"
    r"c[oó]mo (?:se enter|nos conoc|supieron|oyeron)",
    re.IGNORECASE,
)

_ASSISTANT_SPEAKERS = frozenset({"anata", "assistant", "receptionist"})

_SHRUGS = (
    "i don't know",
    "i dont know",
    "don't know",
    "dont know",
    "not sure",
    "no idea",
    "no clue",
    "can't remember",
    "cannot remember",
    "don't remember",
    "dont remember",
    "no comment",
    "rather not say",
    "rather not",
    "no sé",
    "no se",
    "ni idea",
)

_NON_ANSWERS = frozenset(
    {
        "what",
        "huh",
        "sorry",
        "excuse me",
        "come again",
        "pardon",
        "hm",
        "hmm",
        "uh",
        "um",
        "ok",
        "okay",
        "yeah",
        "yes",
        "no",
        "sure",
        "maybe",
        "i",
        "we",
    }
)

# Checked in order. More specific sources come before a bare "google" or "site".
_SOURCE_PATTERNS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("peerspace", ("peerspace",)),
    ("eventective", ("eventective",)),
    (
        "other_event_site",
        (
            "the knot",
            "weddingwire",
            "wedding wire",
            "tagvenue",
            "gigsalad",
            "eventup",
            "splacer",
            "here comes the guide",
            "event site",
            "venue listing",
            "venue site",
        ),
    ),
    (
        "google_maps",
        (
            "google maps",
            "google map",
            "on maps",
            "on the map",
            "google business",
            "business profile",
        ),
    ),
    (
        "google_search",
        (
            "googled",
            "google search",
            "searched google",
            "search google",
            "on google",
            "google",
        ),
    ),
    (
        "referral",
        (
            "word of mouth",
            "referred",
            "referral",
            "a friend",
            "my friend",
            "friend told",
            "told me",
            "told us",
            "recommended",
            "my sister",
            "my brother",
            "my mom",
            "my mother",
            "my dad",
            "my father",
            "coworker",
            "colleague",
            "neighbor",
            "someone i know",
        ),
    ),
    (
        "social",
        (
            "instagram",
            "facebook",
            "tiktok",
            "tik tok",
            "linkedin",
            "twitter",
            "social media",
            "on social",
        ),
    ),
    (
        "drive_by",
        (
            "drove by",
            "drive by",
            "drive-by",
            "drove past",
            "driving by",
            "saw the building",
            "saw your building",
            "saw the sign",
            "your sign",
            "walked by",
            "walking by",
        ),
    ),
    (
        "returning_customer",
        (
            "been here before",
            "been there before",
            "rented before",
            "used you before",
            "booked before",
            "we've rented",
            "we rented",
            "i rented",
            "last year",
            "last time",
            "returning",
            "came before",
            "existing customer",
            "already a client",
            "i'm a client",
            "im a client",
            "i am a client",
        ),
    ),
    (
        "website",
        (
            "your website",
            "your site",
            "the website",
            "the site",
            "web site",
            "website",
            "anatabuilding",
            "anatainc.com",
        ),
    ),
)

# Unsolicited mentions. Weak words like "friend" or a bare "google" wait for the question.
_STRONG_PATTERNS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("peerspace", ("peerspace",)),
    ("eventective", ("eventective",)),
    (
        "other_event_site",
        ("the knot", "weddingwire", "wedding wire", "tagvenue", "gigsalad"),
    ),
    ("google_maps", ("google maps", "google map")),
    ("google_search", ("googled", "on google", "google search")),
    ("social", ("instagram", "facebook", "tiktok", "tik tok", "linkedin")),
    ("drive_by", ("drove by", "drive by", "drive-by", "saw the sign", "saw the building")),
    ("referral", ("word of mouth", "referred me", "referred us", "a referral")),
    ("website", ("your website", "your site", "anatabuilding", "anatainc.com")),
    ("returning_customer", ("rented before", "been here before", "booked before")),
)

_URGENT_EXTRA = (
    "no heat",
    "no ac",
    "no a/c",
    "no air",
    "without heat",
    "heat is out",
    "heater is out",
    "security problem",
    "security issue",
    "ac is out",
    "a/c is out",
    "air conditioning is out",
    "no air conditioning",
)


def dialed_line_source(session: CallSession) -> str:
    """Tracking-line label when the number has one, otherwise the line id."""

    return (session.line.source or "").strip() or (session.line.id or "").strip()


def normalize_heard_about(raw: str) -> str:
    """Map the caller's words to one source category. Empty or a shrug is unknown."""

    text = _clean(raw)
    if not text:
        return "unknown"
    for phrase in sorted(_SHRUGS, key=len, reverse=True):
        text = text.replace(phrase, " ")
    text = _clean(text)
    if not text or text in _NON_ANSWERS:
        return "unknown"
    return _match(text, _SOURCE_PATTERNS)


def should_ask_how_heard(session: CallSession) -> bool:
    """True only after we know why they called, and never on an urgent or indexed call."""

    if (session.heard_about_raw or "").strip():
        return False
    if answer_after_question(session.transcript()) is not None:
        return False
    if (session.known_caller_category or "").strip().casefold() in SKIP_KNOWN_CATEGORIES:
        return False
    if session.category in {"spam", "urgent"}:
        return False
    blob = " ".join(
        (
            session.reason,
            session.notes,
            session.event_or_interest_type,
            session.transcript(),
        )
    )
    if looks_like_emergency(blob) or _urgent_extra(blob):
        return False
    if looks_like_solicitation(blob):
        return False
    return _knows_why(session)


def how_heard_nudge(session: CallSession) -> dict[str, object]:
    """Tool hint. The model asks this once, then continues."""

    if not should_ask_how_heard(session):
        return {"ask_how_heard": False}
    blob = " ".join((session.transcript(), session.notes, session.reason))
    question = HOW_HEARD_QUESTION_ES if prefers_spanish(blob) else HOW_HEARD_QUESTION
    return {
        "ask_how_heard": True,
        "say_next": question,
        "instruction": (
            "Ask that once, in that sentence, then continue. "
            "If they don't know or skip it, move on and do not ask again."
        ),
    }


def prefers_spanish(text: str) -> bool:
    sample = (text or "").casefold()
    if any(character in sample for character in "áéíóúñ¿¡"):
        return True
    markers = (
        "hola",
        "gracias",
        "por favor",
        "buenos días",
        "buenas tardes",
        "quiero",
        "necesito",
        "ustedes",
        "nosotros",
    )
    return sum(marker in sample for marker in markers) >= 2


def apply_heard_about(session: CallSession) -> None:
    """Fill the raw answer and the category before the call row is written."""

    raw, source = resolve_heard_about(session)
    session.heard_about_raw = raw
    session.heard_about_source = source if source in HEARD_ABOUT_SOURCES else "unknown"


def resolve_heard_about(session: CallSession) -> tuple[str, str]:
    """Prefer the caller's reply to the question, then a saved tool value, then a clear mention."""

    asked = answer_after_question(session.transcript())
    if asked is not None:
        cleaned = " ".join(asked.split())
        if not cleaned:
            return "", "unknown"
        return cleaned[:500], normalize_heard_about(cleaned)
    tool_raw = " ".join((session.heard_about_raw or "").split())
    if tool_raw:
        return tool_raw[:500], normalize_heard_about(tool_raw)
    volunteered = volunteered_source(session.transcript())
    if volunteered:
        return volunteered[:500], normalize_heard_about(volunteered)
    return "", "unknown"


def answer_after_question(transcript: str) -> str | None:
    """The next caller line after the question, '' if they never answered, None if it was not asked."""

    lines = [line.strip() for line in (transcript or "").splitlines() if line.strip()]
    for index, line in enumerate(lines):
        speaker, _, text = line.partition(":")
        if speaker.strip().casefold() not in _ASSISTANT_SPEAKERS:
            continue
        if not _ASK_RE.search(text):
            continue
        for later in lines[index + 1 :]:
            later_speaker, _, later_text = later.partition(":")
            who = later_speaker.strip().casefold()
            if who == "caller":
                return later_text.strip()
            if who in _ASSISTANT_SPEAKERS:
                return ""
        return ""
    return None


def volunteered_source(transcript: str) -> str:
    for line in (transcript or "").splitlines():
        speaker, _, text = line.partition(":")
        if speaker.strip().casefold() != "caller":
            continue
        if _match(text.casefold(), _STRONG_PATTERNS) != "other":
            return " ".join(text.split())
    return ""


def _clean(text: str) -> str:
    flattened = " ".join((text or "").casefold().replace("’", "'").split())
    return flattened.strip(" .,;:!?\"'")


def _knows_why(session: CallSession) -> bool:
    return bool(
        session.reason.strip()
        or session.event_or_interest_type.strip()
        or session.business_line.strip()
        or session.notes.strip()
    )


def _urgent_extra(text: str) -> bool:
    haystack = f" {(text or '').casefold()} "
    return any(phrase in haystack for phrase in _URGENT_EXTRA)


def _match(text: str, patterns: tuple[tuple[str, tuple[str, ...]], ...]) -> str:
    haystack = f" {text.casefold()} "
    for category, phrases in patterns:
        if any(phrase in haystack for phrase in phrases):
            return category
    return "other"
