"""Abby: answers replies to the outbound campaign inside Instantly.

Rules agreed with David on 2026-10-26. The AI only classifies the reply and
drafts text; this module decides what is allowed to happen, so a bad model
answer can never send to the wrong person, skip an opt-out, or promise
something outside the offer.

  interested / asks for the breakdown -> reply with the 14-day trial and the
      booking link, mark Interested. David is CC'd only when Abby sets cc_david.
  price question -> "plans start at $99/mo", trial, booking link. No automatic CC.
  not now -> short thanks, then check-ins at 30 and 75 days.
  already has an agency or tool -> ONE reply (operating cost, how do they know
      it covers everything), then one later note that the free trial can run
      alongside their current tool. Nothing after that.
  referral to a colleague -> only when a human wrote the reply and named a
      colleague at a personal address. The approved template asks them, and a
      referral without an email gets the harder "who handles Amazon or
      marketing growth" ask. David only if asked. This does not mark the lead
      Interested.
  send info to an address -> a human asked us to send information to a
      specific address, including a role inbox such as partnerships@. Hold
      and post to Slack. Do not email that address, and do not mark Interested.
  wrong person -> the same harder ask, from the approved template when present.
      Address the person who wrote, not the stored lead, when those differ.
      Post the send to Slack.
  opt-out -> no reply, block the whole company domain, mark not interested.
  question Abby is unsure about -> answer what she can, offer the call.
  hostile / legal -> no reply, blocklist the lead, mark not interested.
  low confidence -> no reply, ask David in Slack.
  asked whether she is AI -> she is an AI account specialist on David's team.
  out of office -> no reply now. A follow-up is scheduled for the day after
      the parsed return date, or 7 days when no date is found.
  departed employee / automated forward -> ignore. No reply, no David CC,
      no Interested status, no follow-up. A departed contact is recorded so
      the brand can be tried again later. Nothing is sent. "I have left" and
      "left the business" count, including when the subject starts with OOO.
      A subject that is only OOO, with no departed wording, stays out of office.
      If Instantly marked the message as human, the ignore is posted to Slack.
"""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass, field
from typing import Any, Callable, NamedTuple, Optional

logger = logging.getLogger(__name__)

CLASSIFY_MODEL = "claude-haiku-4-5-20251001"
DRAFT_MODEL = "claude-sonnet-4-6"
MODEL = DRAFT_MODEL
BOOKING_LINK = "https://calendar.app.google/5TwjbbpmKaGAQuBk8"
DAVID_EMAIL = "david@anatainc.com"
SIGNATURE = "Abby\nAccount Specialist, Anata"
AI_DISCLOSURE = "I'm an AI account specialist on David's team."
NOT_NOW_FOLLOW_UP_DAYS = (30, 75)
MIN_CONFIDENCE = 0.7
MAX_WORDS = 150

CATEGORIES = (
    "interested", "price", "not_now", "has_solution", "referral", "send_info",
    "opt_out", "hostile", "question", "ai_question", "auto_reply", "wrong_person",
    "other",
)
# Categories where Abby writes back. send_info is held for a person.
REPLY_CATEGORIES = {"interested", "price", "not_now", "has_solution", "referral",
                    "question", "ai_question", "wrong_person"}
# Instantly interest. 1 enrolls the "Interested - book with David" subsequence,
# so only a real interested or price reply may set it. Referrals, questions,
# and wrong-person notes must not.
INTEREST = {"interested": 1, "price": 1, "opt_out": -1}

FACTS = f"""About Anata (only use these facts):
- Anata Intelligence builds a brand's Amazon growth strategy and then runs the ad actions automatically to hit it.
- Offer: a free 14-day trial (card on file, nothing charged for 14 days), then the $99/mo Starter plan. Sign up: https://dashboard.anatainc.com/signup. We review results with them on day 10.
- During the trial we build their Amazon strategy and test full ad automation on their account.
- Never say no card is needed.
- David Narayan is the founder and owner; he takes every call. Anata is based in Lehi, Utah.
- Booking link for a call with David: {BOOKING_LINK}
- The cold emails offered a short Amazon breakdown. David records a custom video for interested brands."""

SYSTEM = f"""You are Abby from Anata, replying inside an existing cold email thread.
{FACTS}

Classify the prospect's latest message and draft Abby's reply.
Categories: {", ".join(CATEGORIES)}.
- interested: wants to learn more, see the breakdown/video, or talk.
- price: asks what it costs (you may say plans start at $99/month).
- not_now: timing is wrong, maybe later.
- has_solution: already has an agency, tool, or team for Amazon, or says they already sell on Amazon.
- referral: a human wrote this reply and recommends a named colleague at a personal email address. Put that person's email in referral_email if they gave one. A shared inbox is not a colleague. Never put partnerships@, info@, sales@, support@, or customerservice@ in referral_email. Use send_info for those.
- send_info: a human asks us to send information, details, or this email to a specific address, including a role inbox such as partnerships@, info@, sales@, support@, or customerservice@. Put that address in referral_email. A human wrote this. It is not auto_reply.
- opt_out: asks to stop, unsubscribe, remove them, is clearly annoyed by the emails, or the company has closed or ceased operations.
- A message that is only a signature or contact block with no request is auto_reply.
- hostile: insults, threats, legal language (spam laws, lawyers, reporting).
- question: a specific question you can partly answer from the facts.
- ai_question: asks if you are a bot, AI or a real person.
- auto_reply: out of office, an automated message, a satisfaction survey, a departed-employee notice, or a forwarding notice. This includes "no longer with", "no longer part of", "no longer work at", "I have left", "I have now left", "left the business", "has left the company", "out of the office", a subject that starts with OOO, "please direct inquiries", "concluded my role", and "no longer monitoring". A mailbox rule that only points at info@, sales@, support@, or customerservice@ is auto_reply. Naming a person or giving their email does not make these a referral. Leave referral_email null. Choose auto_reply only when the message is clearly one of these. If a human wrote it, or you are unsure, choose other.
- wrong_person: a human says this is not their area and names no colleague and no address to send information to.

Reply rules by category:
- interested: thank them, one line on the 14-day trial (we build their Amazon strategy and run the ad automation), invite them to grab a time with David at the booking link. Under 80 words. Do not say you are looping David in.
- price: plans start at $99/month, mention the free 14-day trial, booking link. Do not say you are looping David in.
- not_now: two short sentences: thanks, and you will check back in about a month. No link.
- has_solution: respectful, one short paragraph: ask what it costs them to run that setup today and how they know it is covering everything they need on Amazon; mention the 14-day trial can run alongside what they have. No pressure.
- referral: only when a human wrote the referral. Address the colleague by name if known, say who pointed you to them, one line on the trial, booking link.
- question: answer only from the facts; for anything else say David covers it on a quick call and give the booking link.
- ai_question: be honest. Say "{AI_DISCLOSURE}" Never say "virtual assistant". Then continue helpfully.
- wrong_person: thank them and ask who handles Amazon or marketing growth. No booking link. Address the person who wrote, not a different name on the stored lead.
- send_info, opt_out, hostile, auto_reply, other: reply must be null.
Bringing David in: he is CC'd only when you set cc_david to true, because he is the one managing the calls. Do that when a warm prospect is ready for a call, when they push back on something outside the facts, for a larger brand, or on a stalled thread. Frame it in the reply, for example "David, any thoughts on X?" Leave cc_david false otherwise, and do not mention looping him in.
Style: plain, warm, short, founder-to-founder, no em dashes, no exclamation marks overload, no claims about their numbers. Never say "virtual assistant". Sign exactly as:
{SIGNATURE}

Return ONLY JSON: {{"category": "...", "confidence": 0.0-1.0, "referral_email": null or "x@y.com", "referral_name": null or "Name", "cc_david": false, "reply": null or "text"}}"""


@dataclass
class Decision:
    category: str
    action: str                      # reply | hold | block | ignore | skip
    reply: str = ""
    cc: list[str] = field(default_factory=list)
    to_extra: list[str] = field(default_factory=list)
    interest: Optional[int] = None
    follow_up_days: Optional[int] = None
    block_domain: str = ""
    slack: str = ""                  # message for David, if any
    reason: str = ""
    note: str = ""                   # recorded on the handled row; never sent
    schedule_kind: str = ""          # out_of_office | not_now | has_solution | ""


def cc_david_requested(model_out: dict[str, Any]) -> bool:
    """True only when Abby explicitly chose to bring David in."""
    raw = (model_out or {}).get("cc_david")
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, (int, float)):
        return raw == 1
    if isinstance(raw, str):
        return raw.strip().lower() in {"true", "yes", "1"}
    return False


def clean(text: str) -> str:
    t = (text or "").replace("—", " - ").replace("–", " - ")
    t = re.sub(r"[ \t]+", " ", t)
    return t.strip()


def validate_reply(category: str, text: str) -> tuple[bool, str]:
    t = clean(text)
    if not t:
        return False, "empty"
    if len(t.split()) > MAX_WORDS:
        return False, "too long"
    links = re.findall(r"https?://\S+", t)
    if any(not l.rstrip(".,)").startswith(BOOKING_LINK) for l in links):
        return False, "unknown link"
    if category in {"interested", "price", "referral"} and BOOKING_LINK not in t:
        return False, "missing booking link"
    # Only the offer's own numbers may appear ($99, 14-day), never a made-up stat.
    bare = re.sub(r"https?://\S+", "", t)
    for num in re.findall(r"\d+(?:[.,]\d+)?", bare):
        if num not in {"99", "14"}:
            return False, f"unexpected number {num}"
    return True, ""


def _domain(email: str) -> str:
    return (email or "").split("@")[-1].strip().lower()


# Shared inboxes. customer.service@ and customer-service@ compact to the same slot.
_ROLE_LOCALS = frozenset({
    "info", "sales", "support", "customerservice", "customersupport", "customercare",
})
# Mailbox rules from the Oct 26-27 misses (Riomar, Garbarini, Bellocq) plus the
# standard departed / out-of-office lines. A person naming a colleague does not
# use these. Garbarini named Sierra and still was not a referral.
_DEPARTED_PHRASES = (
    "no longer with",
    "has left the company",
    "have left the company",
    "no longer be monitoring",
    "no longer monitoring this",
    "concluded my role",
    "concluded her role",
    "concluded their role",
    "no longer works here",
    "no longer works at",
    "no longer work at",
    "no longer work for",
    "i no longer work",
    "no longer employed",
    "no longer part of",
    "left the business",
    "no longer monitored",
    "email is no longer",
    "inbox is no longer",
    "i have moved on",
    "i've moved on",
)
# "I have now left Arena" and "Mo has left REFY", but not "left a message"
# or "left the office" (that one is out of office).
_LEFT_RE = re.compile(
    r"\b(?:i have|i've|have|has) (?:now )?left\b"
    r"(?!\s+(?:an?|you|this|feedback|the office|my office|a message|a voicemail|a note)\b)",
    re.I,
)
_SURVEY_RE = re.compile(
    r"satisfaction[- ]survey|how would you rate the help|gorgias\.com/satisfaction",
    re.I,
)
# "Send your information to partnerships@" is a routing request.
# "Send the breakdown" is the prospect asking us for the offer, not a destination.
_SEND_INFO_RE = re.compile(
    r"\bsend\b(?:\s+\w+){0,8}\s+(?:your|our)\s+(?:information|info|details|collaboration)\b"
    r"|\bsend\b(?:\s+(?:your|the|our|this))?\s+(?:information|info|details)\b"
    r"(?:\s+\w+){0,10}\s+\b(?:to|at)\b"
    r"|\bsend\b(?:\s+(?:this|it))?\s+\bto\b"
    r"|\bforward\b(?:\s+\w+){0,6}\s+\bto\b",
    re.I,
)
_OPT_OUT_RE = re.compile(
    r"\b(?:unsubscribe|remove me|stop emailing|do not contact|don't contact)\b",
    re.I,
)
_SIGN_OFF_RE = re.compile(
    r"^(?:thanks|thank you|best|regards|cheers|sincerely|warmly|all the best)\s*[,!]?\s*$",
    re.I,
)
_NAME_STOP = _ROLE_LOCALS | {
    "best", "thanks", "thank", "regards", "cheers", "sincerely", "team", "hello",
    "hi", "dear", "abby", "amazon", "marketing", "growth",
}
_OOO_PHRASES = (
    "out of the office",
    "out of office",
    "away from the office",
    "away from my office",
    "left the office",
)
_AUTO_PHRASES = (
    "please direct inquiries",
    "please direct your inquiries",
    "direct inquiries to",
    "for assistance moving forward",
    "automatic reply",
    "automatic response",
    "this is an automated",
    "this is an automatic",
    "automatically generated",
    "automated response",
    "automated message",
    "auto-reply",
    "autoreply",
    "do not reply to this",
)
_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")
# Forwarding language in front of a shared inbox. "Email: info@" in a signature
# is a label, not one of these, so a real reply with that signature still counts.
_ROLE_CUE = re.compile(
    r"please\s+(?:reach out|email|e-mail|contact|write|direct|forward)|"
    r"reach out(?: to)?|"
    r"direct inquiries|"
    r"for assistance|"
    r"with any inquiries|"
    r"can be reached at",
    re.I,
)
_QUOTE_CUT = re.compile(r"\n\s*(?:On .{5,160} wrote:|_{5,}|-{5,})\s*", re.I)


class Notice(NamedTuple):
    kind: str
    reason: str
    note: str


def is_role_address(address: str) -> bool:
    """True for a shared inbox, including numbered and dotted variants."""
    import outbound_contacts

    return outbound_contacts.is_role_mailbox(address)


def _body_text(message: Any) -> str:
    if isinstance(message, dict):
        body = message.get("body") or {}
        text = str(body.get("text") or "") if isinstance(body, dict) else str(body or "")
        if not text.strip() and isinstance(body, dict):
            text = re.sub(r"<[^>]+>", " ", str(body.get("html") or ""))
        if not text.strip():
            text = str(message.get("content_preview") or "")
        return text
    return str(message or "")


def _own_words(text: str) -> str:
    """Drop the quoted thread. The notice is the part they actually sent."""
    return _QUOTE_CUT.split(text or "", maxsplit=1)[0]


def _bare_subject(subject: str) -> str:
    return re.sub(r"^\s*((re|fw|fwd)\s*:\s*)+", "", subject or "", flags=re.I).strip().lower()


def _flag(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes"}
    return value is True or value == 1


def _header_map(headers: Any) -> dict[str, str]:
    if isinstance(headers, dict):
        items = headers.items()
    elif isinstance(headers, list):
        items = []
        for item in headers:
            if isinstance(item, dict):
                items.append((item.get("name") or item.get("key") or "", item.get("value") or ""))
            elif isinstance(item, (list, tuple)) and len(item) == 2:
                items.append((item[0], item[1]))
    else:
        return {}
    return {str(k).strip().lower(): str(v).strip().lower() for k, v in items}


def header_says_auto(headers: Any) -> bool:
    """Auto-Submitted, X-Autoreply, X-Autorespond, or Precedence: auto_reply."""
    for name, value in _header_map(headers).items():
        if name == "auto-submitted" and value not in {"", "no"}:
            return True
        if name in {"x-autoreply", "x-autorespond", "x-autoresponder"} and value not in {"no", "false", "0"}:
            return True
        if name == "x-auto-response-suppress":
            return True
        if name == "precedence" and value in {"auto_reply", "auto-reply"}:
            return True
    return False


def _subject_says_auto(subject: str) -> bool:
    subject = _bare_subject(subject)
    return subject.startswith((
        "automatic reply", "auto-reply", "autoreply", "auto reply",
        "out of office", "out of the office",
    ))


def _subject_ooo(subject: str) -> bool:
    """A subject that is only an out-of-office marker, including ``OOO - ...``."""

    return re.match(r"^ooo\b", _bare_subject(subject)) is not None


def instantly_marked_human(value: Any) -> bool:
    """True only when Instantly explicitly says this was not an auto-reply.

    Missing means unknown. ``0``, ``false``, and ``no`` are human.
    """

    if value is None or value == "":
        return False
    return not _flag(value)


def _message_parts(message: Any) -> tuple[str, Any, Any, str]:
    if isinstance(message, dict):
        headers = message.get("headers")
        if headers is None:
            headers = message.get("email_headers")
        return (
            str(message.get("subject") or ""),
            headers,
            message.get("is_auto_reply"),
            _body_text(message),
        )
    return "", None, None, _body_text(message)


def _suggested(text: str, lead_email: str, extra: str = "") -> list[str]:
    lead = (lead_email or "").strip().lower()
    found: list[str] = []
    for blob in (text or "", extra or ""):
        for match in _EMAIL_RE.finditer(blob):
            addr = match.group(0).lower()
            if addr == lead or "anata" in addr.split("@")[-1] or addr in found:
                continue
            found.append(addr)
    return found[:8]


def _role_contacts(text: str) -> list[str]:
    found: list[str] = []
    for match in _EMAIL_RE.finditer(text or ""):
        addr = match.group(0).lower()
        if not is_role_address(addr) or addr in found:
            continue
        window = (text or "")[max(0, match.start() - 96):match.start()]
        if _ROLE_CUE.search(window):
            found.append(addr)
    return found


def _says_departed(low: str) -> bool:
    if any(phrase in low for phrase in _DEPARTED_PHRASES):
        return True
    return _LEFT_RE.search(low) is not None


def _public_excerpt(text: str) -> str:
    """A short quote for Slack. Query strings on links are dropped."""

    cleaned = re.sub(r"https?://\S+", "[link]", text or "")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned[:220]


def _clean_person_name(value: str) -> str:
    value = re.sub(r"[^A-Za-z '\-]", "", value or "").strip()
    parts = [part for part in value.split() if part]
    if not parts or len(parts) > 3:
        return ""
    first = parts[0].strip(",.")
    if len(first) < 2 or first.lower() in _NAME_STOP or not first[0].isupper():
        return ""
    return first


def _from_display(message: dict) -> str:
    for key in ("from_name", "from_address_name", "sender_name"):
        if message.get(key):
            return str(message[key])
    raw = message.get("from_address_json")
    if isinstance(raw, str) and raw.strip().startswith("{"):
        try:
            raw = json.loads(raw)
        except ValueError:
            return ""
    if isinstance(raw, dict):
        return str(raw.get("name") or raw.get("first_name") or "")
    return ""


def _signature_name(text: str) -> str:
    """The first name on the line after Best, Thanks, or Regards."""

    lines = [line.strip() for line in _own_words(text).splitlines() if line.strip()]
    for index, line in enumerate(lines):
        head, sep, tail = line.partition(",")
        if sep and _SIGN_OFF_RE.match(head):
            found = _clean_person_name(tail)
            if found:
                return found
        if _SIGN_OFF_RE.match(line) and index + 1 < len(lines):
            found = _clean_person_name(lines[index + 1])
            if found:
                return found
    return ""


def greeting_name(message: Any, *, lead_email: str, lead_first_name: str) -> str:
    """Who to address. Never the stored lead when someone else wrote.

    A missing From address still yields the signature name when that name
    is not the lead's. No name is safer than the wrong name.
    """

    lead_first = (lead_first_name or "").strip().split(" ")[0] if lead_first_name else ""
    from_email = ""
    text = ""
    display = ""
    if isinstance(message, dict):
        from_email = str(message.get("from_address_email") or "").strip().lower()
        display = _from_display(message)
        text = _body_text(message)
    else:
        text = str(message or "")
    writer = _clean_person_name(display) or _signature_name(text)
    lead = (lead_email or "").strip().lower()
    if from_email and lead and from_email != lead:
        if writer:
            return writer
        return ""
    if writer and lead_first and writer.lower() != lead_first.lower():
        return writer
    return lead_first


def send_info_route(message: Any = "", *, lead_email: str = "",
                    referral_email: str = "") -> Optional[list[str]]:
    """Addresses a human asked us to send information to.

    Role inboxes count. A warm referral to a personal address does not: that
    stays on the approved referral path. None when this is not that request.
    """

    _subject, _headers, _flag_value, text = _message_parts(message)
    own = _own_words(text)
    found: list[str] = []
    ref = (referral_email or "").strip().lower()
    if ref and is_role_address(ref) and ref not in found:
        found.append(ref)
    send_match = _SEND_INFO_RE.search(own)
    if send_match:
        window = own[send_match.start():send_match.end() + 180]
        for addr in _suggested(window, lead_email, ref):
            if addr not in found:
                found.append(addr)
    for addr in _role_contacts(own):
        if addr not in found:
            found.append(addr)
    if not found:
        return None
    if send_match is None and not any(is_role_address(addr) for addr in found):
        return None
    return found


def _notice(kind: str, reason: str, lead: str, suggested: list[str]) -> Notice:
    dest = ", ".join(suggested) if suggested else "none"
    who = lead or "unknown"
    if kind == "departed":
        note = f"Departed contact {who}. Suggested for a later attempt: {dest}. Nothing sent."
    elif kind == "out_of_office":
        note = f"Out of office from {who}. Suggested for a later attempt: {dest}. Nothing sent."
    else:
        note = f"Automated notice from {who}. Suggested for a later attempt: {dest}. Nothing sent."
    return Notice(kind, reason, note)


def automated_notice(message: Any = "", *, lead_email: str = "", subject: str = "",
                     headers: Any = None, is_auto_reply: Any = None) -> Optional[Notice]:
    """Ignore departed notices, out-of-office mail, and automated forwards.

    A referral counts only when a human wrote it. These signals are not a
    referral, even when they name a person or give an address.
    """
    sub_m, hdr_m, flag_m, text = _message_parts(message)
    subject = subject or sub_m
    if headers is None:
        headers = hdr_m
    if is_auto_reply is None:
        is_auto_reply = flag_m
    own = _own_words(text)
    # Mail clients wrap the template mid-phrase ("concluded\nmy role").
    low = re.sub(r"\s+", " ", f"{_bare_subject(subject)}\n{own}").lower()
    suggested = _suggested(own, lead_email)
    if _says_departed(low):
        return _notice("departed", "automatic reply: departed employee", lead_email, suggested)
    if _SURVEY_RE.search(low):
        return _notice("auto_reply", "automatic reply: satisfaction survey", lead_email, suggested)
    if any(phrase in low for phrase in _OOO_PHRASES) or _subject_ooo(subject):
        return _notice("out_of_office", "automatic reply: out of office", lead_email, suggested)
    if _flag(is_auto_reply) or header_says_auto(headers) or _subject_says_auto(subject):
        return _notice("auto_reply", "automatic reply: auto-reply header", lead_email, suggested)
    if any(phrase in low for phrase in _AUTO_PHRASES):
        return _notice("auto_reply", "automatic reply: automated message", lead_email, suggested)
    return None


def _message_flag(message: Any, is_auto_reply: Any) -> Any:
    if is_auto_reply is not None:
        return is_auto_reply
    if isinstance(message, dict):
        return message.get("is_auto_reply")
    return None


def _ignore(notice: Notice, *, lead: str = "", human: bool = False) -> Decision:
    kind = "out_of_office" if notice.kind == "out_of_office" else ""
    slack = ""
    if human:
        slack = (
            f"Abby ignored a message Instantly marked as human from {lead or 'unknown'}. "
            f"{notice.reason}. Nothing sent."
        )
    return Decision(
        "auto_reply", "ignore", reason=notice.reason, note=notice.note,
        schedule_kind=kind, slack=slack,
    )


def _hold_send_info(addresses: list[str], *, lead_email: str, message: Any,
                    writer: str = "") -> Decision:
    """A person reviews this. Abby does not email the address."""

    dest = ", ".join(addresses) if addresses else "the address they named"
    who = writer or lead_email or "The sender"
    excerpt = _public_excerpt(_body_text(message) if not isinstance(message, str) else str(message or ""))
    slack = (
        f"Abby held a reply. {who} asked us to send info for {lead_email} to {dest}. "
        "Nothing was sent. Review before anyone emails that address."
    )
    if excerpt:
        slack = f"{slack}\n{excerpt}"
    return Decision(
        "send_info", "hold", slack=slack,
        reason="human asked to send info to an address",
        note=f"Held for review. Destination: {dest}. Nothing sent.",
    )


def decide(model_out: dict[str, Any], *, lead_email: str, thread_categories: list[str],
           message: Any = "", subject: str = "", headers: Any = None,
           is_auto_reply: Any = None, first_name: str = "", company: str = "") -> Decision:
    """Turn the model's classification into an allowed action. Pure function."""
    cat = str(model_out.get("category") or "other").strip().lower()
    if cat not in CATEGORIES:
        cat = "other"
    try:
        conf = float(model_out.get("confidence") or 0)
    except (TypeError, ValueError):
        conf = 0.0
    reply = clean(str(model_out.get("reply") or ""))
    flag = _message_flag(message, is_auto_reply)
    human = instantly_marked_human(flag)
    addressed = greeting_name(message, lead_email=lead_email, lead_first_name=first_name)

    # Code gate, ahead of the model. Departed notices, surveys, and real
    # auto-replies never send and never mark Interested.
    notice = automated_notice(message, lead_email=lead_email, subject=subject,
                              headers=headers, is_auto_reply=flag)
    if notice:
        return _ignore(notice, lead=lead_email, human=human)

    if cat == "opt_out":
        return Decision(cat, "block", interest=-1, block_domain=_domain(lead_email),
                        reason="opt-out: no reply, company blocked")

    own_text = _body_text(message) if not isinstance(message, str) else str(message or "")
    ref = str(model_out.get("referral_email") or "").strip().lower()
    route = None if _OPT_OUT_RE.search(own_text) else send_info_route(
        message, lead_email=lead_email, referral_email=ref,
    )
    if route or cat == "send_info":
        return _hold_send_info(route or ([ref] if ref else []), lead_email=lead_email,
                               message=message, writer=addressed or ref)

    if cat == "auto_reply":
        # No header, flag, or departed/OOO phrase. The model used to drop these.
        return Decision(
            cat, "hold",
            slack=(
                f"Abby held a message from {lead_email} that the model called an auto-reply. "
                "Instantly did not mark it as automatic, and the text is not a departed notice "
                "or an out-of-office. Nothing sent."
            ),
            reason="model said auto-reply without auto-reply evidence",
        )
    if cat == "hostile":
        return Decision(cat, "block", interest=-1, block_domain="",
                        slack=f"Hostile or legal reply from {lead_email}. The lead is blocked. Nothing sent.",
                        reason="hostile/legal: lead blocked")
    if cat == "other" or conf < MIN_CONFIDENCE:
        return Decision(cat, "hold", slack=f"Abby is not sure how to answer {lead_email} ({cat}, confidence {conf:.2f}). Nothing sent.",
                        reason="low confidence")
    if cat == "has_solution" and "has_solution" in thread_categories:
        return Decision(cat, "ignore", interest=-1, reason="already answered the agency objection once")
    if cat not in REPLY_CATEGORIES:
        return Decision(cat, "hold", slack=f"Unhandled reply type {cat} from {lead_email}.", reason="unhandled")

    from sales_support_agent.abby_playbook import render_immediate_reply

    own = ""
    if isinstance(message, str):
        own = message
    elif isinstance(message, dict):
        own = str((message.get("body") or {}).get("text") or message.get("content_preview") or "")
    approved = render_immediate_reply(
        cat,
        first_name=addressed,
        company=company,
        reply_text=own,
        referral_name=str(model_out.get("referral_name") or ""),
        referral_email=str(model_out.get("referral_email") or ""),
    )
    if approved:
        reply = approved
    else:
        ok, why = validate_reply(cat, reply)
        if not ok:
            return Decision(cat, "hold", slack=f"Abby drafted a reply to {lead_email} but it failed a check ({why}). Nothing sent.\n\n{reply}",
                            reason=f"draft failed: {why}")

    d = Decision(cat, "reply", reply=reply, interest=INTEREST.get(cat))
    # The runner sends this list and does not add David on its own.
    if cc_david_requested(model_out):
        d.cc.append(DAVID_EMAIL)
    if cat == "referral":
        ref = str(model_out.get("referral_email") or "").strip().lower()
        if re.fullmatch(r"[^@\s]+@[^@\s]+\.[a-z]{2,}", ref):
            d.to_extra.append(ref)
        else:
            d.slack = f"{lead_email} referred a colleague but gave no email; Abby asked who handles Amazon or marketing growth."
    if cat == "not_now":
        d.schedule_kind = "not_now"
    if cat == "has_solution":
        d.schedule_kind = "has_solution"
    if cat in {"interested", "price", "referral"} and not d.slack:
        cc_note = " and CC'd David" if DAVID_EMAIL in d.cc else ""
        if cat == "referral" and BOOKING_LINK not in d.reply:
            named = str(model_out.get("referral_name") or "the colleague")
            d.slack = f"Referral from {lead_email}. Abby wrote {named}."
        else:
            d.slack = f"Positive reply from {lead_email} ({cat}). Abby replied with the booking link{cc_note}."
    if cat == "wrong_person" and not d.slack:
        wrote = addressed or "them"
        d.slack = (
            f"Abby sent a wrong-person reply on {lead_email}, addressed to {wrote}. "
            "She asked who handles Amazon or marketing growth. Nothing was sent to a new address."
        )
    return d


def parse_model_json(raw: str) -> dict[str, Any]:
    m = re.search(r"\{.*\}", raw or "", re.S)
    if not m:
        return {"category": "other", "confidence": 0}
    try:
        data = json.loads(m.group(0))
        return data if isinstance(data, dict) else {"category": "other", "confidence": 0}
    except ValueError:
        return {"category": "other", "confidence": 0}


def build_prompt(thread: list[dict[str, Any]], *, company: str, first_name: str,
                 lead_email: str = "") -> str:
    latest = _latest_prospect(thread)
    writer = greeting_name(latest, lead_email=lead_email, lead_first_name=first_name)
    from_email = str(latest.get("from_address_email") or "").strip().lower()
    lines = [f"Prospect: {writer or first_name or 'unknown'} at {company or 'their company'}."]
    if from_email and lead_email and from_email != lead_email.strip().lower():
        stored = first_name or "the stored lead"
        lines.append(
            f"The stored lead is {stored} <{lead_email}>. "
            f"The latest message was written by {writer or 'someone else'} <{from_email}>. "
            f"Do not address {stored}."
        )
    if instantly_marked_human(latest.get("is_auto_reply")):
        lines.append(
            "Instantly marked the latest message as written by a human (is_auto_reply=0). "
            "Do not classify it as auto_reply."
        )
    lines.append("Thread, oldest first:")
    for msg in thread[-6:]:
        who = "PROSPECT" if msg.get("ue_type") == 2 else "ABBY/ANATA"
        body = clean(str((msg.get("body") or {}).get("text") or msg.get("content_preview") or ""))
        body = re.split(r"\n\s*On .{5,80} wrote:", body)[0][:1500]
        lines.append(f"--- {who}:\n{body}")
    return "\n".join(lines) + "\n\nClassify the prospect's LATEST message and draft Abby's reply."


def _cached_system(text: str) -> list[dict[str, Any]]:
    """Anthropic prompt cache marker for a fixed system prompt."""

    return [{"type": "text", "text": text, "cache_control": {"type": "ephemeral"}}]


CLASSIFY_SYSTEM = f"""You classify a prospect's latest email for Abby at Anata.
{FACTS}

Categories: {", ".join(CATEGORIES)}.
- interested: wants to learn more, see the breakdown/video, or talk.
- price: asks what it costs.
- not_now: timing is wrong, maybe later.
- has_solution: already has an agency, tool, or team for Amazon, or says they already sell on Amazon.
- referral: a human wrote this reply and recommends a named colleague at a personal email address. Put that person's email in referral_email if they gave one. A shared inbox is not a colleague. Never put partnerships@, info@, sales@, support@, or customerservice@ in referral_email. Use send_info for those.
- send_info: a human asks us to send information, details, or this email to a specific address, including a role inbox such as partnerships@, info@, sales@, support@, or customerservice@. Put that address in referral_email. A human wrote this. It is not auto_reply.
- opt_out: asks to stop, unsubscribe, remove them, is clearly annoyed by the emails, or the company has closed or ceased operations.
- A message that is only a signature or contact block with no request is auto_reply.
- hostile: insults, threats, legal language (spam laws, lawyers, reporting).
- question: a specific question you can partly answer from the facts.
- ai_question: asks if you are a bot, AI or a real person.
- auto_reply: out of office, an automated message, a satisfaction survey, a departed-employee notice, or a forwarding notice. This includes "no longer with", "no longer part of", "no longer work at", "I have left", "I have now left", "left the business", "has left the company", "out of the office", a subject that starts with OOO, "please direct inquiries", "concluded my role", and "no longer monitoring". A mailbox rule that only points at info@, sales@, support@, or customerservice@ is auto_reply. Naming a person or giving their email does not make these a referral. Leave referral_email null. Choose auto_reply only when the message is clearly one of these. If a human wrote it, or you are unsure, choose other.
- wrong_person: a human says this is not their area and names no colleague and no address to send information to.

Do not draft the reply. Set reply to null.
Return ONLY JSON: {{"category": "...", "confidence": 0.0-1.0, "referral_email": null or "x@y.com", "referral_name": null or "Name", "cc_david": false, "reply": null}}"""


def _message_text(msg: Any) -> str:
    return "".join(getattr(block, "text", "") for block in getattr(msg, "content", []) or [])


def call_model(prompt: str, api_key: Optional[str] = None) -> str:
    """Classify with Haiku, then draft a reply with Sonnet only when one is allowed."""

    from sales_support_agent.services.llm_client import llm_client

    classified = llm_client(caller="abby_classify", api_key=api_key).messages.create(
        model=CLASSIFY_MODEL,
        max_tokens=400,
        system=_cached_system(CLASSIFY_SYSTEM),
        messages=[{"role": "user", "content": prompt}],
    )
    data = parse_model_json(_message_text(classified))
    category = str(data.get("category") or "other")
    if category not in REPLY_CATEGORIES:
        data["reply"] = None
        return json.dumps(data)
    drafted = llm_client(caller="abby_draft", api_key=api_key).messages.create(
        model=DRAFT_MODEL,
        max_tokens=600,
        system=_cached_system(SYSTEM),
        messages=[{
            "role": "user",
            "content": prompt + f"\n\nThe category is {category}. Draft Abby's reply now.",
        }],
    )
    draft = parse_model_json(_message_text(drafted))
    if draft.get("reply"):
        data["reply"] = draft["reply"]
    if "cc_david" in draft:
        data["cc_david"] = draft["cc_david"]
    for key in ("referral_email", "referral_name"):
        if draft.get(key):
            data[key] = draft[key]
    return json.dumps(data)


def to_html(text: str) -> str:
    import html
    return "<br/>".join(html.escape(line) for line in text.split("\n"))


def _latest_prospect(thread: list[dict[str, Any]]) -> dict[str, Any]:
    inbound = [m for m in thread if m.get("ue_type") == 2]
    if inbound:
        return inbound[-1]
    return thread[-1] if thread else {}


def think(thread: list[dict[str, Any]], *, lead_email: str, company: str, first_name: str,
          thread_categories: list[str], llm: Optional[Callable[[str], str]] = None) -> Decision:
    """Classify + draft + decide. Never raises; a failure becomes a hold.

    Departed notices, out-of-office mail, surveys, and requests to send
    information to an address are decided in code, before the model is asked.
    A bad classification cannot send or mark the lead Interested.
    """
    latest = _latest_prospect(thread)
    human = instantly_marked_human(latest.get("is_auto_reply") if isinstance(latest, dict) else None)
    notice = automated_notice(latest, lead_email=lead_email)
    if notice:
        return _ignore(notice, lead=lead_email, human=human)
    body = _body_text(latest)
    if not _OPT_OUT_RE.search(body):
        route = send_info_route(latest, lead_email=lead_email)
        if route:
            writer = greeting_name(latest, lead_email=lead_email, lead_first_name=first_name)
            return _hold_send_info(route, lead_email=lead_email, message=latest, writer=writer)
    if not _own_words(body).strip():
        return Decision(
            "other", "hold",
            slack=f"Abby held an empty message from {lead_email}. Nothing sent.",
            reason="empty message",
        )
    try:
        raw = (llm or call_model)(build_prompt(
            thread, company=company, first_name=first_name, lead_email=lead_email,
        ))
        out = parse_model_json(raw)
    except Exception:  # noqa: BLE001
        logger.exception("[abby] model call failed")
        return Decision("other", "hold", slack=f"Abby could not reach the AI for {lead_email}. Nothing sent.",
                        reason="model error")
    return decide(out, lead_email=lead_email, thread_categories=thread_categories, message=latest,
                  first_name=first_name, company=company)
