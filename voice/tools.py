"""Tools the receptionist can call, and the code that actually runs them.

Transfer is not a tool the model can force. `request_transfer` returns a
decision from `screening.decide`. The bridge places the Twilio Dial only
after that decision says approved.
"""

from __future__ import annotations

import json
from typing import Any

from voice.attribution import how_heard_nudge
from voice.calendar_availability import CalendarAvailability
from voice.knowledge import anata_inc_facts, building_facts, published_addons, published_rates
from voice.screening import (
    ScreenInput,
    apply_forward_availability,
    decide,
    is_agency_lead,
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
        "description": "Return optional add-on prices for setup, labor, and A/V, plus insurance. Security is never required. Use setup, labor, or A/V only when the caller asks. Use security only when they ask, or once on a big party.",
        "parameters": {"type": "object", "properties": {}, "required": []},
    },
    {
        "type": "function",
        "name": "get_building_facts",
        "description": "Return the address, workspace options and published workspace prices (Bullpen desk, warehouse), parking, tours, tenant mail and battery rules, and what else is in the building. Private office prices are not published.",
        "parameters": {"type": "object", "properties": {}, "required": []},
    },
    {
        "type": "function",
        "name": "get_anata_inc_facts",
        "description": "Return anata Inc. facts: Amazon and marketplace marketing services, fulfillment and FBA prep, Anata Intelligence and Shipping OS software, published software plan prices, and how to qualify the caller. Use before answering any agency, fulfillment, or software question.",
        "parameters": {"type": "object", "properties": {}, "required": []},
    },
    {
        "type": "function",
        "name": "check_event_date",
        "description": "Check one Arena date against the events calendar. date is YYYY-MM-DD. style is business for a small business meeting (for example a BNI chapter), large for a big event such as a 200-person conference, party for a loud or social event, or unsure.",
        "parameters": {
            "type": "object",
            "properties": {
                "date": {"type": "string", "description": "YYYY-MM-DD"},
                "style": {"type": "string", "enum": ["business", "large", "party", "unsure"]},
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
                "business_line": {
                    "type": "string",
                    "enum": ["event", "workspace", "agency", "fulfillment", "software", "tenant", "other"],
                    "description": "Which part of Anata the call is about.",
                },
                "marketplaces": {"type": "string", "description": "Agency callers: where they sell, e.g. Amazon, TikTok Shop, Shopify."},
                "revenue_range": {"type": "string", "description": "Agency callers: rough annual or monthly sales range they gave."},
                "notes": {"type": "string"},
                "security_preference": {
                    "type": "string",
                    "enum": ["yes", "no", "undecided"],
                    "description": "Whether the caller wants optional event security. Only for a big party, or if they asked.",
                },
                "heard_about": {
                    "type": "string",
                    "description": "The caller's own words for how they heard about Anata, only if they answered. Do not guess.",
                },
            },
            "required": [],
        },
    },
    {
        "type": "function",
        "name": "request_transfer",
        "description": "Ask to connect the caller to a person. The server may decline sales pitches. Use when they ask for a person, the owner, or David, when they have a contract or price-exception question you cannot answer, for a building emergency, or when a real prospect wants to talk now.",
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
                "caller_asked_for_person": {
                    "type": "boolean",
                    "description": "True when they asked for a person, the owner, David, a contract answer you cannot give, or a pricing exception.",
                },
                "is_building_emergency": {"type": "boolean"},
                "is_current_tenant": {"type": "boolean"},
                "is_known_customer": {"type": "boolean"},
                "qualified_opportunity": {"type": "boolean"},
                "caller_name": {"type": "string"},
                "company": {"type": "string"},
                "event_date": {"type": "string"},
                "guest_count": {"type": "string"},
                "event_or_interest_type": {"type": "string"},
                "business_line": {
                    "type": "string",
                    "enum": ["event", "workspace", "agency", "fulfillment", "software", "tenant", "other"],
                },
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
                "business_line": {
                    "type": "string",
                    "enum": ["event", "workspace", "agency", "fulfillment", "software", "tenant", "other"],
                    "description": "Which part of Anata the call is about.",
                },
                "notes": {"type": "string"},
                "security_preference": {
                    "type": "string",
                    "enum": ["yes", "no", "undecided"],
                    "description": "The caller's answer about optional event security, if it came up.",
                },
                "heard_about": {
                    "type": "string",
                    "description": "The caller's own words for how they heard about Anata, only if they answered. Do not guess.",
                },
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
    if name == "get_anata_inc_facts":
        return anata_inc_facts()
    if name == "check_event_date":
        return calendar.check(str(args.get("date") or session.event_date), str(args.get("style") or "unsure"))
    if name == "save_caller_details":
        line = session.business_line.strip().casefold()
        if line == "tenant" and session.category in {"other", "lead"}:
            session.category = "tenant"
        elif session.category == "other" and line != "other":
            session.category = "lead"
        return {"saved": True, "fields": session.lead_fields(), **how_heard_nudge(session)}
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
            caller_asked_for_person=bool(args.get("caller_asked_for_person")),
            ring_policy=session.ring_policy,
            known_category=session.known_caller_category,
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
            business_line=str(args.get("business_line") or session.business_line),
        )
    )
    decision = apply_forward_availability(decision, bool(settings.forward_to_number))
    session.category = decision.category
    session.reason = str(args.get("reason") or session.reason)
    session.action = "transfer" if decision.allow_transfer else "message"
    session.transfer_requested = decision.allow_transfer
    if decision.asked_for_david and decision.allow_transfer:
        session.asked_for_david = True
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
    agency_lead = is_agency_lead(category, session.business_line)
    if looks_like_solicitation(blob, agency_lead=agency_lead):
        category = "spam"
    # Solicitation always wins. A prior spam or vendor decision is not loosened
    # into a lead by the goodbye tool.
    if category == "spam" or session.category not in {"spam", "vendor"}:
        session.category = category
    session.summary = str(args.get("summary") or session.summary)
    if "callback_needed" in args and session.category != "spam":
        session.callback_needed = bool(args.get("callback_needed"))
    if session.asked_for_david and not session.transfer_accepted and session.category != "spam":
        # The caller was already told David has a message. Do not let goodbye clear that.
        session.callback_needed = True
        session.action = "needs_callback"
    elif (
        session.ring_policy == "urgent_only"
        and not session.transfer_accepted
        and session.category != "spam"
    ):
        session.callback_needed = True
        session.action = "needs_callback"
    elif session.ring_policy == "never" and session.category != "spam":
        session.callback_needed = True
        if session.action in {"", "transfer"}:
            session.action = "message"
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
                        # Omit language_hint. xAI auto-detects when it is absent.
                        # A fixed "en" pins the transcript to English and drops Spanish.
                        "keyterms": [
                            "Anata",
                            "Anata Building",
                            "Arena",
                            "Lehi",
                            "Bullpen",
                            "BNI",
                            "anata Inc",
                            "Anata Intelligence",
                            "Shipping OS",
                            "FBA",
                            "TikTok Shop",
                            "Eventective",
                        ],
                    },
                },
                "output": {
                    "format": {"type": "audio/pcmu"},
                    # Slightly under 1.0 so the front desk does not rush.
                    "speed": 0.95,
                },
            },
            "tools": TOOLS,
        },
    }
