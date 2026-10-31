"""Function tools exposed to the realtime voice model, and their dispatcher."""

from __future__ import annotations

import logging
import re
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Callable, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import select

from sales_support_agent.models.database import session_scope
from sales_support_agent.models.entities import BuildingContact, BuildingInquiry, BuildingOffering, BuildingSpace
from sales_support_agent.models.voice import VoiceConsent, VoiceDoNotCall
from sales_support_agent.services.voice import google_calendar, twilio
from sales_support_agent.services.voice.config import VoiceConfig

logger = logging.getLogger(__name__)


def phone_key(value: str) -> str:
    """Last ten digits, so +1 (801) 555-0100 and 8015550100 match."""
    digits = re.sub(r"\D", "", value or "")
    return digits[-10:]


@dataclass
class CallContext:
    config: VoiceConfig
    session_factory: Any
    call_sid: str
    from_number: str
    state: dict = field(default_factory=dict)
    transfer: Callable[[VoiceConfig, str], dict] = twilio.transfer_call


TOOL_SCHEMAS: list[dict] = [
    {
        "type": "function",
        "name": "lookup_caller",
        "description": "Look up whether the caller's phone number matches an existing inquiry or contact. Call once at the start.",
        "parameters": {"type": "object", "properties": {}},
    },
    {
        "type": "function",
        "name": "get_building_info",
        "description": "Get published facts about the building's spaces, capacity, pricing and offerings. Never guess facts.",
        "parameters": {
            "type": "object",
            "properties": {"topic": {"type": "string", "description": "What the caller asked about"}},
        },
    },
    {
        "type": "function",
        "name": "get_tour_slots",
        "description": "List open tour times. Dates are YYYY-MM-DD in the building's local time.",
        "parameters": {
            "type": "object",
            "properties": {
                "start_date": {"type": "string", "description": "First day to search (optional)"},
                "days": {"type": "integer", "description": "How many days to search, default 5"},
            },
        },
    },
    {
        "type": "function",
        "name": "book_tour",
        "description": "Book a tour at one of the slots returned by get_tour_slots. Confirm name and the time with the caller first.",
        "parameters": {
            "type": "object",
            "properties": {
                "start": {"type": "string", "description": "Slot start, ISO 8601 exactly as returned"},
                "name": {"type": "string"},
                "email": {"type": "string"},
                "notes": {"type": "string", "description": "Event type, size, date or other context"},
            },
            "required": ["start", "name"],
        },
    },
    {
        "type": "function",
        "name": "transfer_to_human",
        "description": "Transfer the caller to a person. Use when they ask for a human, are upset, or need something you cannot do.",
        "parameters": {"type": "object", "properties": {"reason": {"type": "string"}}},
    },
    {
        "type": "function",
        "name": "opt_out",
        "description": "Caller asked not to be called or contacted by phone again. Records them on the do-not-call list.",
        "parameters": {"type": "object", "properties": {}},
    },
]


def lookup_caller(ctx: CallContext, _args: dict) -> dict:
    key = phone_key(ctx.from_number)
    if len(key) < 10:
        return {"found": False}
    with session_scope(ctx.session_factory) as session:
        inquiries = session.execute(
            select(BuildingInquiry).where(BuildingInquiry.phone != "").order_by(BuildingInquiry.created_at.desc()).limit(500)
        ).scalars().all()
        match = next((row for row in inquiries if phone_key(row.phone) == key), None)
        if match:
            ctx.state.update(inquiry_id=match.id, caller_name=match.name, caller_email=match.email)
            return {
                "found": True,
                "first_name": (match.name or "").split(" ")[0],
                "last_inquiry": match.created_at.date().isoformat() if match.created_at else "",
                "inquiry_status": match.status,
                "event_type": (match.payload_json or {}).get("event_type", ""),
            }
        contacts = session.execute(
            select(BuildingContact).where(BuildingContact.phone != "").limit(1000)
        ).scalars().all()
        contact = next((row for row in contacts if phone_key(row.phone) == key), None)
        if contact:
            ctx.state.update(caller_name=contact.full_name, caller_email=contact.email)
            return {"found": True, "first_name": (contact.full_name or "").split(" ")[0]}
    return {"found": False}


def get_building_info(ctx: CallContext, args: dict) -> dict:
    with session_scope(ctx.session_factory) as session:
        spaces = session.execute(select(BuildingSpace).where(BuildingSpace.is_public.is_(True))).scalars().all()
        offerings = session.execute(select(BuildingOffering).where(BuildingOffering.is_published.is_(True))).scalars().all()
        info = {
            "spaces": [
                {"name": s.name, "type": s.space_type, "capacity": s.capacity, "description": s.public_description[:400]}
                for s in spaces
            ],
            "offerings": [
                {"name": o.name, "type": o.offering_type, "price": o.price_display, "description": o.public_description[:400]}
                for o in offerings
            ],
        }
    if ctx.config.building_facts:
        info["facts"] = ctx.config.building_facts
    if not (info["spaces"] or info["offerings"] or info.get("facts")):
        info["note"] = "No published details. Offer to take a message or transfer."
    return info


def _slot_label(slot: datetime, tz: str) -> str:
    local = slot.astimezone(ZoneInfo(tz))
    return local.strftime("%A %B %-d at %-I:%M %p")


def get_tour_slots(ctx: CallContext, args: dict) -> dict:
    if not ctx.config.calendar_ready:
        return {"available": False, "reason": "Tour booking is not set up. Offer to take a message or transfer."}
    start_day: Optional[date] = None
    if args.get("start_date"):
        try:
            start_day = date.fromisoformat(str(args["start_date"]))
        except ValueError:
            start_day = None
    slots = google_calendar.free_slots(ctx.config, start_day=start_day, days=int(args.get("days") or 5))
    ctx.state["offered_slots"] = [s.isoformat() for s in slots]
    return {
        "available": bool(slots),
        "slots": [{"start": s.isoformat(), "label": _slot_label(s, ctx.config.tour_timezone)} for s in slots],
    }


def book_tour(ctx: CallContext, args: dict) -> dict:
    if not ctx.config.calendar_ready:
        return {"booked": False, "reason": "calendar_not_configured"}
    raw = str(args.get("start") or "")
    try:
        start = datetime.fromisoformat(raw)
    except ValueError:
        return {"booked": False, "reason": "invalid_start"}
    if start.tzinfo is None:
        start = start.replace(tzinfo=ZoneInfo(ctx.config.tour_timezone))
    still_free = google_calendar.free_slots(ctx.config, start_day=start.astimezone(ZoneInfo(ctx.config.tour_timezone)).date(), days=1, limit=100)
    if not any(abs((slot - start).total_seconds()) < 60 for slot in still_free):
        return {"booked": False, "reason": "slot_taken", "hint": "Call get_tour_slots again."}
    name = str(args.get("name") or "Caller").strip()[:120]
    email = str(args.get("email") or "").strip()[:255]
    description = "\n".join(
        line
        for line in (
            "Booked by the AI voice receptionist.",
            f"Caller: {name}",
            f"Phone: {ctx.from_number}",
            f"Email: {email}" if email else "",
            f"Notes: {str(args.get('notes') or '')[:500]}" if args.get("notes") else "",
            f"Twilio call: {ctx.call_sid}",
        )
        if line
    )
    event = google_calendar.insert_tour_event(ctx.config, start=start, summary=f"Tour: {name}", description=description)
    ctx.state.update(tour_event_id=event.get("id", ""), tour_start_at=start, caller_name=name)
    if email:
        ctx.state["caller_email"] = email
    return {"booked": True, "when": _slot_label(start, ctx.config.tour_timezone)}


def transfer_to_human(ctx: CallContext, args: dict) -> dict:
    result = ctx.transfer(ctx.config, ctx.call_sid)
    if result.get("ok"):
        ctx.state["transferred"] = True
        ctx.state["transfer_reason"] = str(args.get("reason") or "")[:200]
        return {"transferred": True}
    return {"transferred": False, "reason": "No one is available. Offer to take a message."}


def opt_out(ctx: CallContext, _args: dict) -> dict:
    key = phone_key(ctx.from_number)
    if not key:
        return {"recorded": False}
    now = datetime.now(timezone.utc)
    with session_scope(ctx.session_factory) as session:
        if session.get(VoiceDoNotCall, key) is None:
            session.add(VoiceDoNotCall(phone=key, reason="caller_request", source="voice_agent", call_sid=ctx.call_sid, created_at=now))
        for consent in session.execute(select(VoiceConsent).where(VoiceConsent.phone == key, VoiceConsent.revoked_at.is_(None))).scalars():
            consent.revoked_at = now
            consent.revoked_reason = "caller_opt_out"
    ctx.state["opted_out"] = True
    return {"recorded": True}


HANDLERS: dict[str, Callable[[CallContext, dict], dict]] = {
    "lookup_caller": lookup_caller,
    "get_building_info": get_building_info,
    "get_tour_slots": get_tour_slots,
    "book_tour": book_tour,
    "transfer_to_human": transfer_to_human,
    "opt_out": opt_out,
}


def dispatch(ctx: CallContext, name: str, args: dict) -> dict:
    handler = HANDLERS.get(name)
    if handler is None:
        return {"error": "unknown_tool"}
    try:
        result = handler(ctx, args or {})
    except Exception as exc:  # noqa: BLE001 — never drop the caller on a tool failure
        logger.warning("voice tool %s failed: %s", name, exc)
        result = {"error": "tool_failed", "detail": type(exc).__name__}
    ctx.state.setdefault("tool_calls", []).append({"name": name, "args": args, "result": result, "at": datetime.now(timezone.utc).isoformat()})
    return result


def new_id() -> str:
    return uuid.uuid4().hex
