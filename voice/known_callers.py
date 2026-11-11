"""Route a call from the known_callers directory.

The category sets the default. A non-empty ring_policy on the row wins.
Unknown numbers keep the normal discovery and screening path, and the
logger may insert them so David can tag them later.
"""

from __future__ import annotations

from voice.lines import normalize_phone
from voice.session import CallSession

CATEGORIES = frozenset({"tenant", "client", "landlord_legal", "vendor", "vip", "unknown"})
RING_POLICIES = frozenset({"always", "urgent_only", "never"})

# Defaults when the row does not set ring_policy. Unknown stays on screening.
_CATEGORY_RING = {
    "tenant": "urgent_only",
    "client": "always",
    "landlord_legal": "always",
    "vendor": "never",
    "vip": "always",
}

# Tenant issues that may ring David. Anything else is a callback.
TENANT_URGENT_PHRASES = (
    "locked out",
    "lockout",
    "can't get in",
    "cannot get in",
    "flood",
    "flooding",
    "water leak",
    "leaking",
    "fire",
    "alarm",
    "no heat",
    "no ac",
    "no a/c",
    "no air",
    "air conditioning",
    "heater",
    "security",
    "break-in",
    "break in",
    "broken into",
)


def effective_ring_policy(category: str, override: str | None) -> str:
    """Return always, urgent_only, never, or '' when screening still decides."""

    chosen = (override or "").strip().casefold()
    if chosen in RING_POLICIES:
        return chosen
    return _CATEGORY_RING.get((category or "").strip().casefold(), "")


def looks_like_tenant_urgent(text: str) -> bool:
    haystack = f" {(text or '').casefold()} "
    return any(phrase in haystack for phrase in TENANT_URGENT_PHRASES)


def first_name(name: str) -> str:
    parts = (name or "").strip().split()
    return parts[0] if parts else ""


def apply_known_caller(session: CallSession, row: dict[str, str] | None) -> None:
    """Copy a directory row onto the live call. Empty means an unknown caller."""

    if not row:
        return
    category = (row.get("category") or "unknown").strip().casefold()
    if category not in CATEGORIES:
        category = "unknown"
    session.known_caller_id = (row.get("id") or "").strip()
    session.known_caller_name = (row.get("name") or "").strip()
    session.known_caller_company = (row.get("company") or "").strip()
    session.known_caller_category = category
    session.ring_policy = effective_ring_policy(category, row.get("ring_policy"))
    if category == "tenant" and session.category in {"", "other"}:
        session.category = "tenant"
    elif category == "vendor" and session.category in {"", "other"}:
        session.category = "vendor"
    if session.known_caller_name and not session.caller_name:
        session.caller_name = session.known_caller_name
    if session.known_caller_company and not session.company:
        session.company = session.known_caller_company


def known_greeting(session: CallSession) -> str:
    """A directory greeting, or '' when the normal line greeting should be used."""

    category = session.known_caller_category
    name = session.known_caller_name.strip()
    first = first_name(name)
    if category == "tenant":
        who = first or "there"
        return f"Hi {who}. You've reached Anata. What's going on?"
    if category == "client":
        who = name or "there"
        return f"Hi {who}. Let me see if I can grab David, one moment."
    if category == "vip":
        who = name or "there"
        return f"Hi {who}. Let me see if I can grab David, one moment."
    if category == "landlord_legal":
        if first:
            return f"Hello, {first}. Let me see if I can grab David, one moment."
        return "Hello. Let me see if I can grab David, one moment."
    if category == "vendor":
        who = name or "there"
        return f"Hi {who}. I can take a message."
    return ""


def known_caller_instructions(session: CallSession) -> str:
    """Extra prompt lines for a matched directory row. Empty for a normal caller."""

    category = session.known_caller_category
    if not category:
        return ""
    name = session.known_caller_name or "this caller"
    company = session.known_caller_company
    company_bit = f", {company}" if company else ""
    policy = session.ring_policy or "screening"
    header = (
        f"Known caller: {name}{company_bit}. Category: {category}. "
        f"Ring policy: {policy}. Id: {session.known_caller_id or 'new'}."
    )
    if category == "tenant":
        body = (
            "Greet them by first name. Help with maintenance and access. "
            "Ring David only for a lockout, flood, fire or alarm, no heat or AC, or a security problem. "
            "Anything else is needs_callback. Do not ring for a routine question."
        )
    elif category == "client":
        body = (
            "Greet them by name and ring David right away. "
            "No screening and no discovery questions."
        )
    elif category == "landlord_legal":
        body = (
            "Be polite and brief. Ring David right away. If he misses, take a message only and say he will call back. "
            "Never discuss the building, rent, money, leases, legal matters, or any case."
        )
    elif category == "vendor":
        body = (
            "Greet them by name and take the message. Do not ring. "
            "The call is logged for David to handle."
        )
    elif category == "vip":
        body = "Ring David right away."
    else:
        body = "This number is not tagged yet. Use the normal discovery and screening."
    return header + "\n" + body


def normalize_caller_phone(value: str) -> str:
    return normalize_phone(value)
