"""Abby: answers replies to the outbound campaign inside Instantly.

Rules agreed with David on 2026-10-26. The AI only classifies the reply and
drafts text; this module decides what is allowed to happen, so a bad model
answer can never send to the wrong person, skip an opt-out, or promise
something outside the offer.

  interested / asks for the breakdown -> reply with the 14-day trial and the
      booking link, CC David ("Looping David, our owner, in"), mark Interested.
  price question -> "plans start at $99/mo", trial, booking link, CC David.
  not now -> short thanks, check in again in 60 days.
  already has an agency or tool -> ONE reply (operating cost, how do they know
      it covers everything), then stop.
  referral to a colleague -> only when a human wrote the reply and named a
      colleague. Write to that colleague, CC the referrer and David.
  opt-out -> no reply, block the whole company domain, mark not interested.
  question Abby is unsure about -> answer what she can, offer the call.
  hostile / legal / low confidence -> no reply, ask David in Slack.
  asked whether she is AI -> answer honestly.
  out of office / auto-reply / departed employee / automated forward -> ignore.
      No reply, no David CC, no Interested status. A departed contact is
      recorded so the brand can be tried again later. Nothing is sent.
"""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass, field
from typing import Any, Callable, NamedTuple, Optional

logger = logging.getLogger(__name__)

MODEL = "claude-sonnet-4-6"
BOOKING_LINK = "https://calendar.app.google/5TwjbbpmKaGAQuBk8"
DAVID_EMAIL = "david@anatainc.com"
FOLLOW_UP_DAYS_NOT_NOW = 60
MIN_CONFIDENCE = 0.7
MAX_WORDS = 150

CATEGORIES = (
    "interested", "price", "not_now", "has_solution", "referral", "opt_out",
    "hostile", "question", "ai_question", "auto_reply", "wrong_person", "other",
)
# Categories where Abby writes back.
REPLY_CATEGORIES = {"interested", "price", "not_now", "has_solution", "referral",
                    "question", "ai_question", "wrong_person"}
# Categories where David is CC'd.
CC_DAVID = {"interested", "price", "referral", "question"}
# Interest status values used by Instantly's Unibox (status=1 Interested,
# status=2 Meeting booked as seen in the Unibox filters; -1 Not interested).
INTEREST = {"interested": 1, "price": 1, "referral": 1, "question": 1, "opt_out": -1}

FACTS = f"""About Anata (only use these facts):
- Anata Intelligence builds a brand's Amazon growth strategy and then runs the ad actions automatically to hit it.
- Offer: a free 14-day trial. During it we build their Amazon strategy and test full ad automation on their account.
- Plans start at $99 a month after the trial.
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
- referral: a human wrote this reply and recommends a colleague. Put that person's email in referral_email if they gave one. A shared inbox is not a colleague. Never put info@, sales@, support@, or customerservice@ in referral_email.
- opt_out: asks to stop, unsubscribe, remove them, is clearly annoyed by the emails, or the company has closed or ceased operations.
- A message that is only a signature or contact block with no request is auto_reply.
- hostile: insults, threats, legal language (spam laws, lawyers, reporting).
- question: a specific question you can partly answer from the facts.
- ai_question: asks if you are a bot, AI or a real person.
- auto_reply: out of office, an automated message, a departed-employee notice, or a forwarding notice. This includes "no longer with", "has left the company", "out of the office", "please direct inquiries to", "concluded my role", and "no longer monitoring". A pointer to info@, sales@, support@, or customerservice@ is auto_reply. Naming a person or giving their email does not make these a referral. Leave referral_email null. If you are unsure a human wrote the message, choose auto_reply.
- wrong_person: a human says this is not their area and names no colleague.

Reply rules by category:
- interested: thank them, one line on the 14-day trial (we build their Amazon strategy and run the ad automation), invite them to grab a time with David at the booking link, and say "Looping David, our owner, in." Under 80 words.
- price: plans start at $99/month, mention the free 14-day trial, booking link, "Looping David, our owner, in."
- not_now: two short sentences: thanks, and you will check back in a couple of months. No link.
- has_solution: respectful, one short paragraph: ask what it costs them to run that setup today and how they know it is covering everything they need on Amazon; mention the 14-day trial can run alongside what they have. No pressure.
- referral: only when a human wrote the referral. Address the colleague by name if known, say who pointed you to them, one line on the trial, booking link.
- question: answer only from the facts; for anything else say David covers it on a quick call and give the booking link.
- ai_question: be honest: "I'm an AI assistant on David's team at Anata." then continue helpfully.
- wrong_person: thank them and ask who handles Amazon or marketing.
- opt_out, hostile, auto_reply, other: reply must be null.
Style: plain, warm, short, founder-to-founder, no em dashes, no exclamation marks overload, no claims about their numbers, sign as "Abby".

Return ONLY JSON: {{"category": "...", "confidence": 0.0-1.0, "referral_email": null or "x@y.com", "referral_name": null or "Name", "reply": null or "text"}}"""


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
    "no longer employed",
)
_OOO_PHRASES = (
    "out of the office",
    "out of office",
    "away from the office",
    "away from my office",
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
    """True for info@, sales@, support@, customerservice@, and the same local part with dots or dashes."""
    local = (address or "").split("@", 1)[0].strip().lower()
    compact = re.sub(r"[^a-z0-9]", "", local)
    return bool(compact) and compact in _ROLE_LOCALS


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
    if any(phrase in low for phrase in _DEPARTED_PHRASES):
        return _notice("departed", "automatic reply: departed employee", lead_email, suggested)
    if any(phrase in low for phrase in _OOO_PHRASES):
        return _notice("out_of_office", "automatic reply: out of office", lead_email, suggested)
    if _flag(is_auto_reply) or header_says_auto(headers) or _subject_says_auto(subject):
        return _notice("auto_reply", "automatic reply: auto-reply header", lead_email, suggested)
    roles = _role_contacts(own)
    if roles:
        ordered = roles + [addr for addr in suggested if addr not in roles]
        return _notice("role_inbox", "automatic reply: shared inbox is not a colleague", lead_email, ordered)
    if any(phrase in low for phrase in _AUTO_PHRASES):
        return _notice("auto_reply", "automatic reply: automated message", lead_email, suggested)
    return None


def _ignore(notice: Notice) -> Decision:
    return Decision("auto_reply", "ignore", reason=notice.reason, note=notice.note)


def decide(model_out: dict[str, Any], *, lead_email: str, thread_categories: list[str],
           message: Any = "", subject: str = "", headers: Any = None,
           is_auto_reply: Any = None) -> Decision:
    """Turn the model's classification into an allowed action. Pure function."""
    cat = str(model_out.get("category") or "other").strip().lower()
    if cat not in CATEGORIES:
        cat = "other"
    try:
        conf = float(model_out.get("confidence") or 0)
    except (TypeError, ValueError):
        conf = 0.0
    reply = clean(str(model_out.get("reply") or ""))

    # Code gate, ahead of the model. Departed notices and shared inboxes stay
    # auto-replies: no referral send, no Interested mark, no CC to David.
    notice = automated_notice(message, lead_email=lead_email, subject=subject,
                              headers=headers, is_auto_reply=is_auto_reply)
    ref = str(model_out.get("referral_email") or "").strip().lower()
    if notice is None and cat == "referral" and is_role_address(ref):
        notice = _notice("role_inbox", "automatic reply: shared inbox is not a colleague",
                         lead_email, _suggested("", lead_email, ref))
    if notice:
        return _ignore(notice)

    if cat == "opt_out":
        return Decision(cat, "block", interest=-1, block_domain=_domain(lead_email),
                        reason="opt-out: no reply, company blocked")
    if cat == "auto_reply":
        return Decision(cat, "ignore", reason="automatic reply")
    if cat == "hostile":
        return Decision(cat, "hold", interest=-1, slack=f"Hostile or legal reply from {lead_email}. Nothing sent. Please take a look.",
                        reason="hostile/legal")
    if cat == "other" or conf < MIN_CONFIDENCE:
        return Decision(cat, "hold", slack=f"Abby is not sure how to answer {lead_email} ({cat}, confidence {conf:.2f}). Nothing sent.",
                        reason="low confidence")
    if cat == "has_solution" and "has_solution" in thread_categories:
        return Decision(cat, "ignore", interest=-1, reason="already answered the agency objection once")
    if cat not in REPLY_CATEGORIES:
        return Decision(cat, "hold", slack=f"Unhandled reply type {cat} from {lead_email}.", reason="unhandled")

    ok, why = validate_reply(cat, reply)
    if not ok:
        return Decision(cat, "hold", slack=f"Abby drafted a reply to {lead_email} but it failed a check ({why}). Nothing sent.\n\n{reply}",
                        reason=f"draft failed: {why}")

    d = Decision(cat, "reply", reply=reply, interest=INTEREST.get(cat))
    if cat in CC_DAVID:
        d.cc.append(DAVID_EMAIL)
    if cat == "referral":
        ref = str(model_out.get("referral_email") or "").strip().lower()
        if re.fullmatch(r"[^@\s]+@[^@\s]+\.[a-z]{2,}", ref):
            d.to_extra.append(ref)
        else:
            d.slack = f"{lead_email} referred a colleague but gave no email; Abby asked for it."
    if cat == "not_now":
        d.follow_up_days = FOLLOW_UP_DAYS_NOT_NOW
    if cat in {"interested", "price", "referral"}:
        d.slack = f"Positive reply from {lead_email} ({cat}). Abby replied with the booking link and CC'd David."
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


def build_prompt(thread: list[dict[str, Any]], *, company: str, first_name: str) -> str:
    lines = [f"Prospect: {first_name or 'unknown'} at {company or 'their company'}.", "Thread, oldest first:"]
    for msg in thread[-6:]:
        who = "PROSPECT" if msg.get("ue_type") == 2 else "ABBY/ANATA"
        body = clean(str((msg.get("body") or {}).get("text") or msg.get("content_preview") or ""))
        body = re.split(r"\n\s*On .{5,80} wrote:", body)[0][:1500]
        lines.append(f"--- {who}:\n{body}")
    return "\n".join(lines) + "\n\nClassify the prospect's LATEST message and draft Abby's reply."


def call_model(prompt: str, api_key: Optional[str] = None) -> str:
    import anthropic

    client = anthropic.Anthropic(api_key=api_key or os.getenv("ANTHROPIC_API_KEY", ""))
    msg = client.messages.create(model=MODEL, max_tokens=600, system=SYSTEM,
                                 messages=[{"role": "user", "content": prompt}])
    return "".join(getattr(b, "text", "") for b in msg.content)


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

    Departed notices, out-of-office mail, and automated forwards are decided
    in code, before the model is asked. A bad classification cannot send.
    """
    latest = _latest_prospect(thread)
    notice = automated_notice(latest, lead_email=lead_email)
    if notice:
        return _ignore(notice)
    try:
        raw = (llm or call_model)(build_prompt(thread, company=company, first_name=first_name))
        out = parse_model_json(raw)
    except Exception:  # noqa: BLE001
        logger.exception("[abby] model call failed")
        return Decision("other", "hold", slack=f"Abby could not reach the AI for {lead_email}. Nothing sent.",
                        reason="model error")
    return decide(out, lead_email=lead_email, thread_categories=thread_categories, message=latest)
