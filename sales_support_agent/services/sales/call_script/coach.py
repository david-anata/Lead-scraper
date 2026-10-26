"""Live coach: one Claude pass over the call so far -> a tracker patch.

The fixed playbook context comes from the Call Script page (it already holds
the sections, beliefs, keys, offers and cases), stored on the live session and
sent as a cached system block. Claude's answer is never trusted as-is: the
rules below run in code before the page sees anything.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from typing import Any, Callable, Optional

from sqlalchemy.orm import Session

from sales_support_agent.models.entities import (
    SalesCallCoachEvent,
    SalesCallLiveSession,
    SalesCallTracker,
    SalesCallTranscriptLine,
)
from sales_support_agent.services.sales.call_script import live as live_svc

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "claude-haiku-4-5-20251001"

BELIEFS = ("Pain", "Doubt", "Cost", "Desire", "Support", "Trust", "Money")
PAIN_KEYS = ("acos", "flat", "rank", "ctr", "cvr", "creative", "repeat", "oneasin", "launch", "copycats", "cash", "time", "blind")
OBJ_KEYS = ("think", "partner", "price", "timing", "diy", "burned", "current", "trial", "proof")
RES_KEYS = ("fit", "spike", "control", "commit", "ai", "small", "comms")
SECTION_KEYS = tuple(f"s{i}" for i in range(1, 18))
CASE_KEYS = ("sonrei", "serovital", "oceanrx", "zantrex", "arcticcool", "novex", "trestique")
NUMBER_KEYS = ("total", "adsales", "spend", "goal", "months", "margin", "target", "asins", "ctr", "cvr", "price", "impr")
OFFERS = ("starter", "growth", "pro", "full")
MOVE_KINDS = ("ask", "handle", "tiedown", "moveon", "pitch", "close", "temperature")

COACH_INSTRUCTIONS = """You are the live sales coach inside Anata's Call Script. A rep is on a Google Meet call right now. You read the transcript so far and return ONE JSON object that updates the rep's tracker. The rep reads your nextMove line out loud, so write it as natural speech in the rep's voice.

How Anata sells (one-call close):
- Main offer: Anata Intelligence from $99/month (Starter), 14-day free trial, no card, starts when Seller Central connects. Close it on this call by getting them to connect Seller Central.
- Offer pick: "starter" by default. "growth" ($199) if they need more than 2 channels or more than 2 seats. "pro" ($399) if more than 4 channels or more than 5 seats. "full" (full service: $3,000/month + 5% of sales above their current baseline, Intelligence and seller support included) only if they have no time or team, want it done for them, or do more than about $80K/month on Amazon and want fast growth.
- Never invent numbers, clients or results. Proof only from the approved case keys: sonrei, serovital, oceanrx, zantrex, arcticcool, novex, trestique.

Coach rules:
- Stay in discovery at least 20 of a 45 minute call. Warn if the rep pitches before minute 20 or before the core beliefs (Pain, Cost, Doubt, Desire, Trust; Support too if a partner is involved) are said.
- Frame discovery as "checking the oil, a few quick metrics under the hood" so it never feels like an interview.
- When the prospect doesn't know a number, suggest the ballpark ladder ("Below 5%? Between 5 and 10? Not over 10?").
- Get the prospect to say three things: the problem, the gap between where they are and want to be, and the cost of doing nothing.
- A goal without a date is a wish: if a goal has no number or no date, the next move asks for both.
- Check the goal is realistic against the margins they shared; prompt the rep if it doesn't add up.
- Tie-downs: restate each named pain in the prospect's words 5 to 6 times through the pitch, then the exact words at the close. Count uses.
- Temperature check before closing: ask 1 to 10. Grade the ANSWER, not just the number: a clear 8 or higher with a reason means close; a vague or hedged answer, or under 8, means a clarifying follow-up ("What would make it a 10?") and back to objections.
- Price objection: "Compared to what?" then the cost of the problem vs the fee.
- If the prospect has already confirmed a point several times, the next move is to move on.
- A belief only counts when the PROSPECT says it. Quote the prospect's exact words.
- The transcript is live speech-to-text: expect small talk, off-topic chatter and rough wording. Never call it corrupted, fake or test data, and never tell the rep to reset. Coach from what is there.

Reply with only this JSON object (no prose, no code fences, no em dashes):
{"sections": {"s1": {"done": true, "quote": "..."}},
 "beliefs": {"Pain": {"status": "said|partial|none", "quote": "prospect's exact words"}},
 "numbers": {only numbers the prospect stated, keys: total adsales spend goal months margin target asins ctr cvr price impr; monthly dollars as plain numbers, percentages as numbers},
 "pains": [pain keys], "objs": [objection keys], "res": [reservation keys],
 "three": {"problem": bool, "gap": bool, "cost": bool},
 "quotes": [up to 6 verbatim prospect phrases],
 "personalGoal": "",
 "tieDowns": [{"pain": pain key, "line": "tie-down in their words", "uses": n}],
 "nextMove": {"kind": "ask|handle|tiedown|moveon|pitch|close|temperature", "say": "exact line to say", "why": "short reason", "section": "s1..s17"},
 "pitch": {"ready": bool, "offer": "starter|growth|pro|full", "bridge": "line that brings up the offer from their pain and goal", "slides": [slide numbers], "case": "case key"},
 "temperature": {"asked": bool, "score": number or null, "quality": "clear|vague|none", "followUp": "line"},
 "warnings": ["short strings"]}
Pain keys: acos flat rank ctr cvr creative repeat oneasin launch copycats cash time blind. Objection keys: think partner price timing diy burned current trial proof. Reservation keys: fit spike control commit ai small comms. Sections s1 to s17 as listed in the playbook below.

PLAYBOOK FROM THE CALL SCRIPT:
"""

_DASHES = re.compile("[‒–—―]")
_WS = re.compile(r"\s+")


class CoachError(RuntimeError):
    pass


def _clean(text: Any, limit: int = 600) -> str:
    value = _DASHES.sub("-", str(text or ""))
    return _WS.sub(" ", value).strip()[:limit]


def _norm(text: str) -> str:
    return _WS.sub(" ", re.sub(r"[^\w\s%$]", " ", str(text or "").lower().replace("\u2019", "").replace("'", ""))).strip()


def _num(v: Any) -> Optional[float]:
    try:
        f = float(str(v).replace(",", "").replace("$", "").replace("%", "").strip())
    except (TypeError, ValueError):
        return None
    if f != f or f in (float("inf"), float("-inf")):
        return None
    return f


def _keys(raw: Any, allowed: tuple) -> list[str]:
    if not isinstance(raw, list):
        return []
    out = []
    for k in raw:
        k = str(k or "").strip().lower()
        if k in allowed and k not in out:
            out.append(k)
    return out


def tracker_locks(tracker: SalesCallTracker) -> dict[str, bool]:
    """Beliefs the rep set by hand while live: {belief: value}."""
    state = tracker.state_json or {}
    live = state.get("ss-live") if isinstance(state.get("ss-live"), dict) else {}
    locks = live.get("locks") if isinstance(live.get("locks"), dict) else {}
    beliefs = locks.get("beliefs") if isinstance(locks.get("beliefs"), dict) else {}
    return {b: bool(beliefs[b]) for b in BELIEFS if b in beliefs}


def apply_rules(raw: Any, *, prospect_lines: list[str], locks: dict[str, bool]) -> dict:
    """Validate and clean Claude's patch. Pure function; easy to test."""
    if not isinstance(raw, dict):
        raise CoachError("Coach reply was not a JSON object.")
    prospect_norm = [_norm(t) for t in prospect_lines]
    prospect_blob = " \n ".join(prospect_norm)

    def from_prospect(quote: str) -> bool:
        q = _norm(quote)
        return bool(q) and len(q) >= 3 and q in prospect_blob

    patch: dict[str, Any] = {}

    sections = {}
    for key, val in (raw.get("sections") or {}).items() if isinstance(raw.get("sections"), dict) else []:
        if key in SECTION_KEYS and isinstance(val, dict) and val.get("done"):
            sections[key] = {"done": True, "quote": _clean(val.get("quote"), 300)}
    patch["sections"] = sections

    beliefs = {}
    raw_beliefs = raw.get("beliefs") if isinstance(raw.get("beliefs"), dict) else {}
    for b in BELIEFS:
        if b in locks:
            beliefs[b] = {"status": "said" if locks[b] else "none", "quote": "", "locked": True}
            continue
        val = raw_beliefs.get(b)
        if not isinstance(val, dict):
            continue
        status = str(val.get("status") or "none").lower()
        quote = _clean(val.get("quote"), 300)
        if status == "said":
            if not from_prospect(quote):
                status, quote = "partial", ""
        elif status == "partial":
            if quote and not from_prospect(quote):
                quote = ""
        else:
            status, quote = "none", ""
        if status != "none":
            beliefs[b] = {"status": status, "quote": quote}
    patch["beliefs"] = beliefs

    numbers = {}
    raw_numbers = raw.get("numbers") if isinstance(raw.get("numbers"), dict) else {}
    for k in NUMBER_KEYS:
        n = _num(raw_numbers.get(k)) if k in raw_numbers else None
        if n is not None and n >= 0:
            numbers[k] = n
    patch["numbers"] = numbers

    patch["pains"] = _keys(raw.get("pains"), PAIN_KEYS)
    patch["objs"] = _keys(raw.get("objs"), OBJ_KEYS)
    patch["res"] = _keys(raw.get("res"), RES_KEYS)

    three = raw.get("three") if isinstance(raw.get("three"), dict) else {}
    patch["three"] = {k: bool(three.get(k)) for k in ("problem", "gap", "cost")}

    quotes = []
    for q in raw.get("quotes") or [] if isinstance(raw.get("quotes"), list) else []:
        q = _clean(q, 300)
        if q and from_prospect(q) and q not in quotes:
            quotes.append(q)
    patch["quotes"] = quotes[:6]

    patch["personalGoal"] = _clean(raw.get("personalGoal"), 300)

    ties = []
    for t in raw.get("tieDowns") or [] if isinstance(raw.get("tieDowns"), list) else []:
        if not isinstance(t, dict):
            continue
        pain = str(t.get("pain") or "").lower()
        line = _clean(t.get("line"), 400)
        if pain in PAIN_KEYS and line:
            uses = _num(t.get("uses"))
            ties.append({"pain": pain, "line": line, "uses": int(uses) if uses is not None and uses >= 0 else 0})
    patch["tieDowns"] = ties[:8]

    nm = raw.get("nextMove") if isinstance(raw.get("nextMove"), dict) else {}
    kind = str(nm.get("kind") or "ask").lower()
    section = str(nm.get("section") or "")
    patch["nextMove"] = {
        "kind": kind if kind in MOVE_KINDS else "ask",
        "say": _clean(nm.get("say"), 500),
        "why": _clean(nm.get("why"), 200),
        "section": section if section in SECTION_KEYS else "",
    }

    pitch = raw.get("pitch") if isinstance(raw.get("pitch"), dict) else {}
    offer = str(pitch.get("offer") or "starter").lower()
    case = str(pitch.get("case") or "").lower()
    slides = []
    for s in pitch.get("slides") or [] if isinstance(pitch.get("slides"), list) else []:
        n = _num(s)
        if n is not None and 1 <= n <= 25 and int(n) not in slides:
            slides.append(int(n))
    patch["pitch"] = {
        "ready": bool(pitch.get("ready")),
        "offer": offer if offer in OFFERS else "starter",
        "bridge": _clean(pitch.get("bridge"), 500),
        "slides": slides[:4],
        "case": case if case in CASE_KEYS else "",
    }

    temp = raw.get("temperature") if isinstance(raw.get("temperature"), dict) else {}
    score = _num(temp.get("score"))
    quality = str(temp.get("quality") or "none").lower()
    patch["temperature"] = {
        "asked": bool(temp.get("asked")),
        "score": score if score is not None and 0 <= score <= 10 else None,
        "quality": quality if quality in ("clear", "vague", "none") else "none",
        "followUp": _clean(temp.get("followUp"), 300),
    }

    warnings = []
    for w in raw.get("warnings") or [] if isinstance(raw.get("warnings"), list) else []:
        w = _clean(w, 200)
        if w:
            warnings.append(w)
    patch["warnings"] = warnings[:5]
    return patch


def _parse_json(text: str) -> Any:
    t = (text or "").strip()
    t = re.sub(r"^```(?:json)?\s*", "", t)
    t = re.sub(r"```\s*$", "", t)
    try:
        return json.loads(t)
    except ValueError:
        a, b = t.find("{"), t.rfind("}")
        if a > -1 and b > a:
            return json.loads(t[a : b + 1])
        raise CoachError("Coach reply was not JSON.")


def _state_summary(tracker: SalesCallTracker) -> str:
    state = tracker.state_json or {}
    beliefs = state.get("ss-beliefs") if isinstance(state.get("ss-beliefs"), dict) else {}
    nums = state.get("ss-nums") if isinstance(state.get("ss-nums"), dict) else {}
    sel = state.get("ss-sel") if isinstance(state.get("ss-sel"), dict) else {}
    locks = tracker_locks(tracker)
    parts = [
        "Beliefs ticked: " + (", ".join(b for b in BELIEFS if beliefs.get(b)) or "none"),
        "Numbers entered: " + (json.dumps({k: v for k, v in nums.items() if str(v).strip()}) if nums else "none"),
        "Raised: pains=" + ",".join(sel.get("pains") or []) + " objections=" + ",".join(sel.get("objs") or []) + " reservations=" + ",".join(sel.get("res") or []),
        "Partner involved: " + ("yes" if sel.get("partner") else "no"),
        "Category: " + str(sel.get("cat") or ""),
    ]
    if locks:
        parts.append("Rep set by hand (do not change): " + ", ".join(f"{b}={'yes' if v else 'no'}" for b, v in locks.items()))
    return "\n".join(parts)


def build_messages(
    live: SalesCallLiveSession,
    tracker: SalesCallTracker,
    lines: list[SalesCallTranscriptLine],
    previous: Optional[dict],
) -> tuple[list[dict], list[dict]]:
    system = [
        {
            "type": "text",
            "text": COACH_INSTRUCTIONS + (live.coach_context or "(no playbook sent)"),
            "cache_control": {"type": "ephemeral"},
        }
    ]
    user = (
        f"CALL: {live.label or tracker.prospect_name or tracker.company}\n"
        f"Minutes since the call started: {live_svc.minutes_elapsed(live):.1f}\n\n"
        f"TRACKER RIGHT NOW\n{_state_summary(tracker)}\n\n"
        f"YOUR PREVIOUS UPDATE\n{json.dumps(previous) if previous else 'none yet'}\n\n"
        f"TRANSCRIPT SO FAR (Prospect = the prospect, Rep = Anata)\n{live_svc.transcript_text(lines) or '(nothing yet)'}\n\n"
        "Return the JSON update now."
    )
    return system, [{"role": "user", "content": user}]


def _call_claude(system: list[dict], messages: list[dict], model: str) -> str:
    key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not key:
        raise CoachError("ANTHROPIC_API_KEY is not set on the agent.")
    import anthropic

    client = anthropic.Anthropic(api_key=key)
    msg = client.messages.create(model=model, max_tokens=2000, system=system, messages=messages)
    return "".join(getattr(b, "text", "") for b in (msg.content or []) if getattr(b, "type", "") == "text")


def model_name() -> str:
    return os.getenv("SALES_CALL_LIVE_MODEL", "").strip() or DEFAULT_MODEL


def run_pass(
    session: Session,
    live: SalesCallLiveSession,
    tracker: SalesCallTracker,
    *,
    caller: Optional[Callable[[list, list, str], str]] = None,
) -> Optional[SalesCallCoachEvent]:
    """Run one pass if the lease is free. Returns the stored event or None."""
    if not live_svc.try_acquire_lease(session, live):
        return None
    upto = live_svc.last_line_id(session, live.id)
    try:
        lines = live_svc.all_lines(session, live.id)
        if not any(ln.speaker == "prospect" for ln in lines):
            # Nothing from the prospect yet: no beliefs to judge, so no pass.
            live_svc.release_lease(session, live, line_id=upto)
            return None
        prev_event = live_svc.latest_coach_event(session, live.id)
        previous = prev_event.patch_json if prev_event else None
        system, messages = build_messages(live, tracker, lines, previous)
        model = model_name()
        started = time.monotonic()
        text = (caller or _call_claude)(system, messages, model)
        latency = int((time.monotonic() - started) * 1000)
        patch = apply_rules(
            _parse_json(text),
            prospect_lines=[ln.text for ln in lines if ln.speaker == "prospect"],
            locks=tracker_locks(tracker),
        )
        event = SalesCallCoachEvent(
            session_id=live.id,
            rev=(prev_event.rev + 1) if prev_event else 1,
            patch_json=patch,
            model=model,
            latency_ms=latency,
            created_at=live_svc.now_utc(),
        )
        session.add(event)
        session.flush()
        live_svc.release_lease(session, live, line_id=upto)
        return event
    except Exception as exc:
        logger.warning("live coach pass failed for %s: %s", live.id, exc)
        session.rollback()
        live_svc.release_lease(session, live)
        return None
