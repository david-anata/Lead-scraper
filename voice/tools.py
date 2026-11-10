"""Tools the receptionist can call, and the code that actually runs them.

Transfer is not a tool the model can force. `request_transfer` returns a
decision from `screening.decide`. The bridge places the Twilio Dial only
after that decision says approved.
"""

from __future__ import annotations

import json
from typing import Any

from voice.calendar_availability import CalendarAvailability
from voice.knowledge import building_facts, published_addons, published_rates
from voice.screening import (
    ScreenInput,
    apply_forward_availability,
    decide,
    looks_like_solicitation,
    normalize_category,
)
from voice.session import CallSession
from voice.settings import Settings


TOOLS: list[dict[str, Any]] = [
    {
        "type": "function",
        "name": "get_published_rates",
        "description": "Return the published Arena rental rates, deposit, payment, cancellation, and cleanup rules. Use this before saying any price.",
        "parameters": {"type": "object", "properties": {}, "required": []},
    },
    {
        "type": "function",
        "name": "get_published_addons",
        "description": "Return published add-on prices for setup, labor, and A/V, plus insurance and security rules. Use only when the caller asks about those.",
        "parameters": {"type": "object", "properties": {}, "required": []},
    },
    {
        "type": "function",
        "name": "get_building_facts",
        "description": "Return the address, workspace options, parking, tours, and what else is in the building. No office prices are included.",
        "parameters": {"type": "object", "properties": {}, "required": []},
    },
    {
        "type": "function",
        "name": "check_event_date",
        "description": "Check one Arena date against the events calendar. date is YYYY-MM-DD. style is business, party, or unsure.",
        "parameters": {
            "type": "object",
            "properties": {
                "date": {"type": "string", "description": "YYYY-MM-DD"},
                "style": {"type": "string", "enum": ["business", "party", "unsure"]},
            },
            "required": ["date"],
        },
    },
    {
        "type": "function",
        "name": "save_caller_details",
        "description": "Save lead or tour details as you learn them. Call this whenever you learn a new fact. This does not reserve a date.",
        "parameters": {
            "type": "object",
            "properties": {
                "caller_name": {"type": "string"},
                "phone": {"type": "string"},
                "email": {"type": "string"},
                "company": {"type": "string"},
                "event_date": {"type": "string", "description": "YYYY-MM-DD if known"},
                "guest_count": {"type": "string"},
                "event_or_interest_type": {"type": "string"},
                "notes": {"type": "string"},
            },
            "required": [],
        },
    },
    {
        "type": "function",
        "name": "request_transfer",
        "description": "Ask to connect the caller to a person. The server may decline. Use only for emergencies, urgent tenant issues, known customers who need someone now, or qualified event or lease prospects who want to talk now.",
        "parameters": {
            "type": "object",
            "properties": {
                "category": {
                    "type": "string",
                    "enum": ["lead", "tenant", "vendor", "urgent", "spam", "other"],
                },
                "reason": {"type": "string"},
                "notes": {"type": "string"},
                "wants_live_person": {"type": "boolean"},
                "is_building_emergency": {"type": "boolean"},
                "is_current_tenant": {"type": "boolean"},
                "is_known_customer": {"type": "boolean"},
                "qualified_opportunity": {"type": "boolean"},
                "caller_name": {"type": "string"},
                "company": {"type": "string"},
                "event_date": {"type": "string"},
                "guest_count": {"type": "string"},
                "event_or_interest_type": {"type": "string"},
            },
            "required": ["category", "reason", "wants_live_person"],
        },
    },
    {
        "type": "function",
        "name": "finish_call",
        "description": "Record the outcome just before saying goodbye. category is lead, tenant, vendor, urgent, spam, or other.",
        "parameters": {
            "type": "object",
            "properties": {
                "category": {
                    "type": "string",
                    "enum": ["lead", "tenant", "vendor", "urgent", "spam", "other"],
                },
                "summary": {"type": "string"},
                "callback_needed": {"type": "boolean"},
                "caller_name": {"type": "string"},
                "phone": {"type": "string"},
                "email": {"type": "string"},
                "company": {"type": "string"},
                "event_date": {"type": "string"},
                "guest_count": {"type": "string"},
                "event_or_interest_type": {"type": "string"},
                "notes": {"type": "string"},
            },
            "required": ["category", "summary", "callback_needed"],
        },
    },
]


def parse_arguments(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            loaded = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return loaded if isinstance(loaded, dict) else {}
    return {}


def execute_tool(
    name: str,
    arguments: Any,
    *,
    session: CallSession,
    settings: Settings,
    calendar: CalendarAvailability,
    known_numbers: frozenset[str],
) -> dict[str, Any]:
    args = parse_arguments(arguments)
    session.remember(args)
    if name == "get_published_rates":
        return published_rates()
    if name == "get_published_addons":
        return published_addons()
    if name == "get_building_facts":
        return building_facts()
    if name == "check_event_date":
        return calendar.check(str(args.get("date") or session.event_date), str(args.get("style") or "unsure"))
    if name == "save_caller_details":
        if session.category == "other":
            session.category = "lead"
        return {"saved": True, "fields": session.lead_fields()}
    if name == "request_transfer":
        return _transfer(args, session=session, settings=settings, known_numbers=known_numbers)
    if name == "finish_call":
        return _finish(args, session)
    return {"error": "unknown_tool", "instruction": "Apologize and take a message."}


def _transfer(
    args: dict[str, Any],
    *,
    session: CallSession,
    settings: Settings,
    known_numbers: frozenset[str],
) -> dict[str, Any]:
    if session.resume == "message" or session.transfer_outcome == "no_answer":
        session.transfer_requested = False
        return {
            "transfer": "declined",
            "category": session.category,
            "action": "message",
            "instruction": "The transfer already failed. Take a message. Do not request another transfer.",
        }
    decision = decide(
        ScreenInput(
            category=str(args.get("category") or session.category),
            reason=str(args.get("reason") or session.reason),
            notes=str(args.get("notes") or session.notes),
            wants_live_person=bool(args.get("wants_live_person")),
            is_building_emergency=bool(args.get("is_building_emergency")),
            is_current_tenant=bool(args.get("is_current_tenant")),
            is_known_customer=bool(args.get("is_known_customer")),
            qualified_opportunity=bool(args.get("qualified_opportunity")),
            caller_number=session.caller_number,
            caller_name=str(args.get("caller_name") or session.caller_name),
            company=str(args.get("company") or session.company),
            event_date=str(args.get("event_date") or session.event_date),
            guest_count=str(args.get("guest_count") or session.guest_count),
            event_or_interest_type=str(
                args.get("event_or_interest_type") or session.event_or_interest_type
            ),
            known_numbers=known_numbers,
        )
    )
    decision = apply_forward_availability(decision, bool(settings.forward_to_number))
    session.category = decision.category
    session.reason = str(args.get("reason") or session.reason)
    session.action = "transfer" if decision.allow_transfer else "message"
    session.transfer_requested = decision.allow_transfer
    if decision.category == "spam":
        session.callback_needed = False
    return decision.as_tool_result()


def _finish(args: dict[str, Any], session: CallSession) -> dict[str, Any]:
    category = normalize_category(str(args.get("category") or session.category))
    blob = " ".join(
        (
            session.reason,
            session.notes,
            str(args.get("summary") or ""),
            str(args.get("notes") or ""),
        )
    )
    if looks_like_solicitation(blob):
        category = "spam"
    # Solicitation always wins. A prior spam or vendor decision is not loosened
    # into a lead by the goodbye tool.
    if category == "spam" or session.category not in {"spam", "vendor"}:
        session.category = category
    session.summary = str(args.get("summary") or session.summary)[:500]
    if "callback_needed" in args and session.category != "spam":
        session.callback_needed = bool(args.get("callback_needed"))
    if session.category == "spam":
        session.callback_needed = False
        session.action = "logged_only"
    elif not session.action or session.action == "transfer":
        if not session.transfer_requested:
            session.action = "message"
    return {"saved": True, "category": session.category, "instruction": "Say a short goodbye."}


def session_update_event(settings: Settings, instructions: str) -> dict[str, Any]:
    """xAI session.update using native G.711 µ-law at 8 kHz on both sides."""

    return {
        "type": "session.update",
        "session": {
            "voice": settings.xai_voice,
            "instructions": instructions,
            "reasoning": {"effort": "none"},
            "turn_detection": {"type": "server_vad"},
            "audio": {
                "input": {
                    "format": {"type": "audio/pcmu"},
                    "transcription": {
                        "model": "grok-transcribe",
                        "language_hint": "en",
                        "keyterms": [
                            "Anata",
                            "Anata Building",
                            "Arena",
                            "Lehi",
                            "Bullpen",
                        ],
                    },
                },
                "output": {"format": {"type": "audio/pcmu"}},
            },
            "tools": TOOLS,
        },
    }
