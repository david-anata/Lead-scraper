"""Decide whether a live call may be forwarded.

The model can ask. This module decides. Solicitors, robocalls, and vendor
pitches are never forwarded, even when the model marks them urgent or qualified.
"""

from __future__ import annotations

import re
from dataclasses import dataclass


SOLICITATION_PHRASES = (
    "seo",
    "search engine",
    "rank on google",
    "ranking on google",
    "backlink",
    "marketing agency",
    "marketing services",
    "lead generation",
    "merchant services",
    "credit card processing",
    "business loan",
    "we help businesses",
    "google listing",
    "google business profile",
    "review management",
    "web design",
    "website redesign",
    "social media management",
    "solar panel",
    "press 1",
    "warranty extension",
    "llc formation",
    "payroll services",
    "increase your revenue",
    "increase your sales",
    "robocall",
    "robo call",
    "final expense",
    "insurance quote",
)

EMERGENCY_PHRASES = (
    "fire",
    "flood",
    "flooding",
    "gas leak",
    "smell gas",
    "break-in",
    "break in",
    "broken into",
    "burglar",
    "intruder",
    "alarm",
    "locked out",
    "lockout",
    "can't get in",
    "cannot get in",
    "no access",
    "door code",
    "medical",
    "ambulance",
    "no power",
    "power out",
    "water leak",
    "leaking",
    "sprinkler",
    "smoke",
    "elevator stuck",
    "stuck in the elevator",
    "emergency",
    "security issue",
    "security emergency",
    "need security",
    "broken pipe",
    "ceiling leak",
)

CATEGORIES = frozenset({"lead", "tenant", "vendor", "urgent", "spam", "other"})


@dataclass(frozen=True)
class ScreenInput:
    category: str
    reason: str = ""
    notes: str = ""
    wants_live_person: bool = False
    is_building_emergency: bool = False
    is_current_tenant: bool = False
    is_known_customer: bool = False
    qualified_opportunity: bool = False
    caller_number: str = ""
    caller_name: str = ""
    company: str = ""
    event_date: str = ""
    guest_count: str = ""
    event_or_interest_type: str = ""
    known_numbers: frozenset[str] = frozenset()
    forward_configured: bool = True


@dataclass(frozen=True)
class ScreenDecision:
    allow_transfer: bool
    category: str
    action: str
    speak: str

    def as_tool_result(self) -> dict[str, object]:
        return {
            "transfer": "approved" if self.allow_transfer else "declined",
            "category": self.category,
            "action": self.action,
            "instruction": self.speak,
        }


def _blob(*parts: str) -> str:
    return " ".join(part.strip().casefold() for part in parts if part and part.strip())


def looks_like_solicitation(text: str) -> bool:
    haystack = f" {text.casefold()} "
    return any(phrase in haystack for phrase in SOLICITATION_PHRASES)


def looks_like_emergency(text: str) -> bool:
    haystack = f" {text.casefold()} "
    return any(phrase in haystack for phrase in EMERGENCY_PHRASES)


def has_sales_substance(screen: ScreenInput) -> bool:
    """A qualified live transfer needs a name and one concrete interest."""

    if not screen.caller_name.strip():
        return False
    return any(
        value.strip()
        for value in (
            screen.event_date,
            screen.guest_count,
            screen.event_or_interest_type,
            screen.company,
        )
    )


def normalize_category(category: str) -> str:
    cleaned = (category or "").strip().casefold()
    if cleaned in CATEGORIES:
        return cleaned
    return "other"


def decide(screen: ScreenInput) -> ScreenDecision:
    """Return the only transfer decision the bridge is allowed to act on."""

    text = _blob(screen.reason, screen.notes, screen.event_or_interest_type)
    category = normalize_category(screen.category)
    if looks_like_solicitation(text) or category == "spam":
        return ScreenDecision(
            allow_transfer=False,
            category="spam",
            action="message",
            speak=(
                "Do not transfer. This is a solicitation or an unwanted sales call. "
                "Politely decline, offer to take a short message, and do not collect "
                "a sales pitch. Then call finish_call."
            ),
        )
    if category == "vendor":
        return ScreenDecision(
            allow_transfer=False,
            category="vendor",
            action="message",
            speak=(
                "Do not transfer. Take a short message and say the team will review it. "
                "Then call finish_call."
            ),
        )

    emergency = screen.is_building_emergency or category == "urgent" or looks_like_emergency(text)
    if emergency and (screen.is_building_emergency or looks_like_emergency(text) or category == "urgent"):
        # A bare "urgent" category still has to look like a building, security,
        # access, or safety problem, or be explicitly flagged by the model as one.
        real_emergency = screen.is_building_emergency or looks_like_emergency(text)
        tenant_urgent = screen.is_current_tenant and real_emergency
        if real_emergency or tenant_urgent:
            return _approved("urgent" if not screen.is_current_tenant else "tenant")

    number = _normalize_known(screen.caller_number)
    known = screen.is_known_customer or (bool(number) and number in screen.known_numbers)
    if known and screen.wants_live_person:
        logged = "tenant" if screen.is_current_tenant else "other"
        return _approved(logged)

    if (
        category == "lead"
        and screen.wants_live_person
        and screen.qualified_opportunity
        and has_sales_substance(screen)
    ):
        return _approved("lead")

    if category == "tenant" and screen.is_current_tenant and looks_like_emergency(text):
        return _approved("tenant")

    return ScreenDecision(
        allow_transfer=False,
        category=category,
        action="message",
        speak=(
            "Do not transfer. Take a message with their name, number, and what they need. "
            "Say the team will follow up. Then call finish_call."
        ),
    )


def apply_forward_availability(decision: ScreenDecision, forward_configured: bool) -> ScreenDecision:
    if decision.allow_transfer and not forward_configured:
        return ScreenDecision(
            allow_transfer=False,
            category=decision.category,
            action="message",
            speak=(
                "Do not transfer. No on-call number is configured. Take a message and "
                "tell them the team will follow up. Then call finish_call."
            ),
        )
    return decision


def _approved(category: str) -> ScreenDecision:
    return ScreenDecision(
        allow_transfer=True,
        category=category,
        action="transfer",
        speak=(
            "Transfer approved. Say one short sentence that you are connecting them "
            "now, then stop talking. Do not ask another question."
        ),
    )


def _normalize_known(value: str) -> str:
    from voice.lines import normalize_phone

    return normalize_phone(value)


def callback_needed(decision: ScreenDecision, explicit: bool | None = None) -> bool:
    if explicit is not None:
        return explicit
    return not decision.allow_transfer and decision.category != "spam"


_WS_RE = re.compile(r"\s+")


def one_line(value: str, limit: int = 180) -> str:
    text = _WS_RE.sub(" ", (value or "").strip())
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"
