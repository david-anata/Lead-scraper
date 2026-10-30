"""Playbook for Abby's reply follow-ups.

Immediate replies for wrong-person and referral use the approved templates in
``sales_support_agent.abby_followups`` when that module is present. The second
note for "already has a solution" is written here in the same style: no em
dashes, signed ``Abby\\nAccount Specialist, Anata``, and no invented claims.

Nothing in this module sends email.
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime, timedelta, timezone
from typing import Optional

logger = logging.getLogger(__name__)

SIGNATURE = "Abby\nAccount Specialist, Anata"
NOT_NOW_DAYS = (30, 75)
HAS_SOLUTION_SECOND_DAYS = 7
OOO_FALLBACK_DAYS = 7
_EM_DASH = "\u2014"
_EN_DASH = "\u2013"

_MONTHS = {
    "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
    "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
    "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9, "oct": 10,
    "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12,
}
_MONTH_RE = (
    r"jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|"
    r"aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?"
)
_ROLE_WORDS = (
    "design", "marketing", "finance", "accounting", "operations", "sales",
    "product", "engineering", "support", "ecommerce", "e-commerce", "creative",
)
_HEALTH_WORDS = (
    "sick", "illness", "hospital", "surgery", "family", "funeral", "bereave",
    "health", "passed away",
)
_VACATION_WORDS = ("vacation", "holiday", "pto", "time off")
_SHOW_WORDS = ("conference", "trade show", "tradeshow", "summit", "expo")
_SKIP_NAMES = {"unknown", "there", "friend", "none", "null"}


def _plain(text: str) -> str:
    return (text or "").replace(_EM_DASH, " - ").replace(_EN_DASH, " - ").strip()


def _sign(body: str, signature: str = SIGNATURE) -> str:
    text = _plain(body)
    if text.endswith(signature):
        return text
    return f"{text}\n\n{signature}"


def _first(name: str) -> str:
    cleaned = _plain(name)
    if not cleaned or cleaned.lower() in _SKIP_NAMES:
        return ""
    return cleaned.split()[0]


def _brand(company: str) -> str:
    cleaned = _plain(company)
    return cleaned or "your brand"


def _templates() -> dict:
    """Approved templates when the other module is importable. Empty if not."""

    try:
        from sales_support_agent.abby_followups import load_approved_followups

        return load_approved_followups()
    except Exception:  # noqa: BLE001
        logger.exception("[abby] approved follow-up templates are not available")
        return {}


def _booking_link() -> str:
    try:
        from sales_support_agent.abby_followups import DEFAULT_BOOKING_LINK

        return DEFAULT_BOOKING_LINK
    except Exception:  # noqa: BLE001
        return "https://calendar.app.google/5TwjbbpmKaGAQuBk8"


def _signature() -> str:
    try:
        from sales_support_agent.abby_followups import SIGNATURE as approved

        return approved
    except Exception:  # noqa: BLE001
        return SIGNATURE


def _own_text(text: str) -> str:
    return re.split(r"\n\s*On .{5,90} wrote:|\nFrom: ", text or "")[0]


def _one_date(month: int, day: int, year: Optional[int], today: date) -> Optional[date]:
    if year:
        try:
            return date(year, month, day)
        except ValueError:
            return None
    try:
        this_year = date(today.year, month, day)
    except ValueError:
        return None
    if this_year >= today - timedelta(days=1):
        return this_year
    try:
        upcoming = date(today.year + 1, month, day)
    except ValueError:
        return None
    if upcoming - today <= timedelta(days=180):
        return upcoming
    return None


def parse_return_date(text: str, today: date) -> Optional[date]:
    """The return date in an out-of-office note, or None when none is stated.

    Yearless dates use this year when they are still ahead, otherwise next
    year when that is within six months. A range keeps the later date.
    """

    own = _own_text(text)
    found: list[tuple[int, date]] = []

    def add(start: int, month: int, day: int, year: Optional[int]) -> None:
        parsed = _one_date(month, day, year, today)
        if parsed is not None:
            found.append((start, parsed))

    for match in re.finditer(r"\b(20\d{2})-(\d{1,2})-(\d{1,2})\b", own):
        add(match.start(), int(match.group(2)), int(match.group(3)), int(match.group(1)))
    for match in re.finditer(
        rf"\b({_MONTH_RE})\.?\s+(\d{{1,2}})(?:st|nd|rd|th)?(?:,?\s*(20\d{{2}}))?\b",
        own,
        re.I,
    ):
        year = int(match.group(3)) if match.group(3) else None
        add(match.start(), _MONTHS[match.group(1).lower()], int(match.group(2)), year)
    for match in re.finditer(
        rf"\b(\d{{1,2}})(?:st|nd|rd|th)?\s+({_MONTH_RE})\.?(?:,?\s*(20\d{{2}}))?\b",
        own,
        re.I,
    ):
        year = int(match.group(3)) if match.group(3) else None
        add(match.start(), _MONTHS[match.group(2).lower()], int(match.group(1)), year)
    for match in re.finditer(r"\b(\d{1,2})/(\d{1,2})(?:/(\d{2,4}))?\b", own):
        year_raw = match.group(3)
        year = None
        if year_raw:
            year = int(year_raw)
            if year < 100:
                year += 2000
        add(match.start(), int(match.group(1)), int(match.group(2)), year)

    if not found:
        return None
    cued = []
    for start, parsed in found:
        window = own[max(0, start - 48):start].lower()
        if re.search(r"until|till|through|thru|back|return|available|office", window):
            cued.append(parsed)
    pool = cued or [parsed for _start, parsed in found]
    return max(pool)


def ooo_due(text: str, now: datetime) -> datetime:
    """When to send the out-of-office follow-up.

    The day after the parsed return date, at 15:00 UTC. When the note has no
    return date, seven days from ``now``.
    """

    moment = now if now.tzinfo else now.replace(tzinfo=timezone.utc)
    parsed = parse_return_date(text, moment.date())
    if parsed is None:
        return moment + timedelta(days=OOO_FALLBACK_DAYS)
    due = datetime(parsed.year, parsed.month, parsed.day, 15, 0, tzinfo=timezone.utc) + timedelta(days=1)
    if due < moment:
        return moment
    return due


def ooo_personal_line(text: str) -> str:
    """One approved personal line, or the neutral health line. Never a guess."""

    low = re.sub(r"\s+", " ", _own_text(text)).lower()
    loaded = _templates()
    lines: dict[str, str] = {}
    step = (loaded.get("out_of_office").steps[0] if loaded.get("out_of_office") else None)
    if step is not None:
        lines = dict(step.personal_lines)
    if any(word in low for word in _HEALTH_WORDS):
        return "Hope all is well."
    if any(word in low for word in _VACATION_WORDS):
        return lines.get("vacation", "Hope you had a great vacation.")
    if any(word in low for word in _SHOW_WORDS):
        return lines.get("conference_or_trade_show", "Hope the show was worth the trip.")
    return lines.get("no_reason", "Hope your time away went well.")


def render_ooo_followup(*, first_name: str, brand: str, reply_text: str) -> str:
    """The approved out-of-office follow-up, with the personal line filled in."""

    personal = ooo_personal_line(reply_text)
    name = _first(first_name) or "there"
    company = _brand(brand)
    link = _booking_link()
    loaded = _templates()
    item = loaded.get("out_of_office")
    if item is not None:
        core = item.steps[0].body.format(
            first_name=name,
            personal_line=personal,
            brand=company,
            booking_link=link,
        )
        return _sign(core, item.signature)
    core = (
        f"Hi {name}, {personal} Circling back on my note about {company}'s "
        f"Amazon and Shopify growth. Worth a quick look this week? Here's my calendar: {link}"
    )
    return _sign(core)


def _role_personal_line(text: str) -> str:
    low = _own_text(text).lower()
    for role in _ROLE_WORDS:
        if re.search(rf"\b{re.escape(role)}\b", low):
            shown = "e-commerce" if role == "ecommerce" else role
            return f"makes sense since you're on the {shown} side."
    return ""


def _role_clause(text: str) -> str:
    match = re.search(
        r"\b((?:handles|runs|leads|owns|manages)\s+[A-Za-z][A-Za-z0-9 &/-]{1,40})",
        _own_text(text),
        re.I,
    )
    if not match:
        return ""
    phrase = re.split(r"[.,;\n]|\b(?:for|at|and|with|who|that)\b", match.group(1), maxsplit=1)[0]
    phrase = re.sub(r"\s+", " ", phrase).strip()
    verb, _, rest = phrase.partition(" ")
    mapped = {"handles": "handle", "runs": "run", "leads": "lead", "owns": "own", "manages": "manage"}
    verb = mapped.get(verb.lower(), verb.lower())
    rest = rest.strip()
    if not rest or len(rest) > 40:
        return ""
    return f"{verb} {rest}"


def render_wrong_person(*, first_name: str, brand: str, reply_text: str) -> str:
    """The approved harder ask: who handles Amazon or marketing growth."""

    name = _first(first_name)
    company = _brand(brand)
    personal = _role_personal_line(reply_text)
    signature = _signature()
    loaded = _templates()
    item = loaded.get("wrong_person")
    if personal and item is not None and name:
        core = item.steps[0].body.format(first_name=name, personal_line=personal, brand=company)
        return _sign(core, item.signature)
    if name and personal:
        core = f"Thanks {name}, {personal} Who handles Amazon or marketing growth for {company}?"
    elif name:
        core = f"Thanks {name}, who handles Amazon or marketing growth for {company}?"
    else:
        core = f"Thanks, who handles Amazon or marketing growth for {company}?"
    return _sign(core, signature)


def render_referral(
    *,
    first_name: str,
    new_name: str,
    brand: str,
    reply_text: str,
    storeleads_detail: str = "",
) -> str:
    """The approved note to the named colleague. Drops details we do not have."""

    colleague = _first(new_name) or "there"
    referrer = _first(first_name) or "A colleague"
    company = _brand(brand)
    role = _role_clause(reply_text)
    detail = _plain(storeleads_detail)
    if detail and not detail.startswith(","):
        detail = " " + detail
    mentioned = (
        f"{referrer} mentioned you {role} at {company}."
        if role
        else f"{referrer} mentioned you at {company}."
    )
    core = (
        f"Hi {colleague}, {mentioned} I reached out about growing {company}'s sales "
        f"on Amazon and Shopify{detail}. Open to a quick look? We offer a free 14-day trial."
    )
    loaded = _templates()
    signature = loaded["wrong_person"].signature if loaded.get("wrong_person") else _signature()
    return _sign(core, signature)


def render_immediate_reply(
    category: str,
    *,
    first_name: str = "",
    company: str = "",
    reply_text: str = "",
    referral_name: str = "",
    referral_email: str = "",
    storeleads_detail: str = "",
) -> str:
    """Approved immediate copy for wrong-person and referral. Empty to keep the model draft."""

    try:
        if category == "wrong_person":
            return render_wrong_person(first_name=first_name, brand=company, reply_text=reply_text)
        if category == "referral" and re.fullmatch(r"[^@\s]+@[^@\s]+\.[A-Za-z]{2,}", (referral_email or "").strip()):
            return render_referral(
                    first_name=first_name,
                    new_name=referral_name,
                    brand=company,
                    reply_text=reply_text,
                storeleads_detail=storeleads_detail,
            )
        if category == "referral":
            return render_wrong_person(first_name=first_name, brand=company, reply_text=reply_text)
    except Exception:  # noqa: BLE001
        logger.exception("[abby] could not render the approved reply")
    return ""


def render_not_now(day: int, first_name: str) -> str:
    """Fixed check-ins at 30 and 75 days. No booking link and no new claims."""

    name = _first(first_name)
    hi = f"Hi {name}," if name else "Hi,"
    if day == NOT_NOW_DAYS[0]:
        core = (
            f"{hi} checking back in as I said I would. Is Amazon growth on the table now? "
            "The free 14-day trial is there when the timing is better."
        )
    else:
        core = (
            f"{hi} one more check from me. If Amazon growth is on the table now, the free 14-day trial is there. "
            "If the timing is still off, I will leave it here."
        )
    return _sign(core)


def render_has_solution_second(first_name: str) -> str:
    """The one later note: the free trial can run alongside their current tool."""

    name = _first(first_name)
    hi = f"Hi {name}," if name else "Hi,"
    core = (
        f"{hi} you can run the free 14-day trial alongside what you already use. "
        "We build the Amazon strategy and test the ad automation without asking you to drop your current tool."
    )
    return _sign(core)
