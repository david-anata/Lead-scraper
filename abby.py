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
  referral to a colleague -> write to the colleague, CC the referrer and David.
  opt-out -> no reply, block the whole company domain, mark not interested.
  question Abby is unsure about -> answer what she can, offer the call.
  hostile / legal / low confidence -> no reply, ask David in Slack.
  asked whether she is AI -> answer honestly.
  out of office / auto-reply -> ignore.
"""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

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
- referral: points to a colleague; put that person's email in referral_email if given.
- opt_out: asks to stop, unsubscribe, remove them, is clearly annoyed by the emails, or the company has closed or ceased operations.
- A message that is only a signature or contact block with no request is auto_reply.
- hostile: insults, threats, legal language (spam laws, lawyers, reporting).
- question: a specific question you can partly answer from the facts.
- ai_question: asks if you are a bot, AI or a real person.
- auto_reply: out of office or automated message.
- wrong_person: not their area and no colleague named.

Reply rules by category:
- interested: thank them, one line on the 14-day trial (we build their Amazon strategy and run the ad automation), invite them to grab a time with David at the booking link, and say "Looping David, our owner, in." Under 80 words.
- price: plans start at $99/month, mention the free 14-day trial, booking link, "Looping David, our owner, in."
- not_now: two short sentences: thanks, and you will check back in a couple of months. No link.
- has_solution: respectful, one short paragraph: ask what it costs them to run that setup today and how they know it is covering everything they need on Amazon; mention the 14-day trial can run alongside what they have. No pressure.
- referral: address the colleague by name if known, say who pointed you to them, one line on the trial, booking link.
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


def decide(model_out: dict[str, Any], *, lead_email: str, thread_categories: list[str]) -> Decision:
    """Turn the model's classification into an allowed action. Pure function."""
    cat = str(model_out.get("category") or "other").strip().lower()
    if cat not in CATEGORIES:
        cat = "other"
    try:
        conf = float(model_out.get("confidence") or 0)
    except (TypeError, ValueError):
        conf = 0.0
    reply = clean(str(model_out.get("reply") or ""))

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


def think(thread: list[dict[str, Any]], *, lead_email: str, company: str, first_name: str,
          thread_categories: list[str], llm: Optional[Callable[[str], str]] = None) -> Decision:
    """Classify + draft + decide. Never raises; a failure becomes a hold."""
    try:
        raw = (llm or call_model)(build_prompt(thread, company=company, first_name=first_name))
        out = parse_model_json(raw)
    except Exception:  # noqa: BLE001
        logger.exception("[abby] model call failed")
        return Decision("other", "hold", slack=f"Abby could not reach the AI for {lead_email}. Nothing sent.",
                        reason="model error")
    return decide(out, lead_email=lead_email, thread_categories=thread_categories)
