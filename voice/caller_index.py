"""Turn a finished call into a known_callers row.

Manual directory rows keep the name, company, and category David entered.
Auto rows, and empty inbound stubs, take the learned identity. Every match
still updates the last reason and the last time they called.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from voice.lines import normalize_phone
from voice.screening import is_agency_lead, looks_like_solicitation
from voice.session import CallSession

RELATIONSHIPS = frozenset(
    {"tenant", "client", "landlord_legal", "vendor", "vip", "spam", "lead"}
)
_PLACEHOLDER_NAMES = frozenset(
    {
        "someone",
        "unknown",
        "a caller",
        "caller",
        "phone caller",
        "text caller",
        "unknown caller",
    }
)


def infer_relationship(session: CallSession) -> str:
    """Map this call onto a directory category. Empty when it is still unclear."""

    blob = " ".join(
        (
            session.company,
            session.reason,
            session.notes,
            session.summary,
            session.event_or_interest_type,
        )
    )
    agency = is_agency_lead(session.category, session.business_line)
    if "truework" in blob.casefold() or looks_like_solicitation(blob, agency_lead=agency):
        return "spam"
    learned = (session.learned_relationship or "").strip().casefold()
    if learned in RELATIONSHIPS:
        return learned
    if session.category == "spam":
        return "spam"
    if session.category == "vendor":
        return "vendor"
    if session.category == "tenant" or session.business_line == "tenant":
        return "tenant"
    known = (session.known_caller_category or "").strip().casefold()
    if known in RELATIONSHIPS:
        return known
    if session.category == "lead" or session.business_line in {
        "event",
        "workspace",
        "agency",
        "fulfillment",
        "software",
    }:
        return "lead"
    return ""


def build_directory_record(session: CallSession) -> dict[str, Any] | None:
    """A learned directory payload, or None when the name or relationship is not clear."""

    phone = normalize_phone(session.caller_number or session.phone)
    name = " ".join((session.caller_name or session.known_caller_name or "").split())
    if not phone or not _clear_name(name):
        return None
    category = infer_relationship(session)
    if category not in RELATIONSHIPS:
        return None
    company = " ".join((session.company or "").split())
    if company.casefold() in {"personal", "tenant", "none"}:
        company = ""
    if not company and session.known_caller_company.strip():
        company = session.known_caller_company.strip()
    reason = " ".join((session.reason or session.summary or "").split())
    return {
        "phone": phone,
        "name": name[:200],
        "company": company[:200],
        "category": category,
        "last_reason": reason[:500],
        "last_called_at": datetime.now(timezone.utc),
    }


def merge_learned(existing: dict[str, Any] | None, learned: dict[str, Any]) -> dict[str, Any]:
    """Apply the manual-record rule. Auto rows take the new identity."""

    if not existing:
        return {
            "phone": learned["phone"],
            "name": learned["name"],
            "company": learned["company"],
            "category": learned["category"],
            "source": "auto",
            "last_reason": learned.get("last_reason") or "",
            "last_called_at": learned.get("last_called_at"),
        }
    source = str(existing.get("source") or "")
    name = str(existing.get("name") or "")
    company = str(existing.get("company") or "")
    category = str(existing.get("category") or "unknown")
    untagged_stub = source in {"", "inbound_call"} and not name and category in {"", "unknown"}
    if source == "auto" or untagged_stub:
        return {
            "phone": learned["phone"],
            "name": learned["name"] or name,
            "company": learned["company"] or company,
            "category": learned["category"] or category or "unknown",
            "source": "auto",
            "last_reason": learned.get("last_reason") or str(existing.get("last_reason") or ""),
            "last_called_at": learned.get("last_called_at"),
        }
    return {
        "phone": str(existing.get("phone") or learned["phone"]),
        "name": name or learned["name"],
        "company": company or learned["company"],
        "category": category if category not in {"", "unknown"} else learned["category"],
        "source": source,
        "last_reason": learned.get("last_reason") or str(existing.get("last_reason") or ""),
        "last_called_at": learned.get("last_called_at"),
    }


def _clear_name(name: str) -> bool:
    if len(name) < 2 or len(name) > 80:
        return False
    if name.casefold() in _PLACEHOLDER_NAMES:
        return False
    letters = [character for character in name if character.isalpha()]
    return len(letters) >= 2
