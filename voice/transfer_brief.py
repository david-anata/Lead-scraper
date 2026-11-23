"""What David hears and reads before a call is connected.

Every approved transfer needs a name, who the caller is with, and a reason.
A known caller already has the first two. A refusal transfers with whatever
was collected. The sentence is the same one David hears in the whisper and
reads in the text, except the whisper adds the instruction to press 1.
"""

from __future__ import annotations

from voice.session import CallSession

SMS_LIMIT = 160
_PERSONAL = frozenset(
    {"personal", "myself", "just me", "none", "no company", "no one", "private", "n/a", "na"}
)
_TENANT = frozenset({"tenant", "a tenant", "i'm a tenant", "im a tenant", "i am a tenant"})


def absorb_details(session: CallSession, args: dict[str, object]) -> None:
    """Copy affiliation and relationship off a tool call onto the session."""

    relationship = _relationship(args.get("relationship"))
    if relationship:
        session.learned_relationship = relationship
    explicit = args.get("affiliation")
    if isinstance(explicit, str) and explicit.strip():
        _set_affiliation(session, explicit)
        return
    if session.company.strip() and not session.affiliation:
        _set_affiliation(session, session.company)


def apply_known_defaults(session: CallSession) -> None:
    """Fill name and affiliation from the directory. Do not invent a reason."""

    if not session.caller_name.strip() and session.known_caller_name.strip():
        session.caller_name = session.known_caller_name.strip()
    if session.company.strip().casefold() in _PERSONAL | _TENANT:
        _set_affiliation(session, session.company)
        session.company = ""
    if session.affiliation.strip():
        return
    if session.company.strip():
        session.affiliation = session.company.strip()
        return
    if session.known_caller_company.strip():
        session.affiliation = session.known_caller_company.strip()
        if not session.company.strip():
            session.company = session.known_caller_company.strip()
        return
    if session.known_caller_category == "tenant" or session.learned_relationship == "tenant":
        session.affiliation = "tenant"
        return
    if session.known_caller_category in {"client", "vip", "landlord_legal", "vendor", "lead"}:
        session.affiliation = "personal"


def missing_transfer_facts(session: CallSession) -> list[str]:
    """Name, affiliation, and reason still needed before David is rung."""

    apply_known_defaults(session)
    missing: list[str] = []
    if not (session.caller_name or session.known_caller_name).strip():
        missing.append("name")
    if not session.affiliation.strip():
        missing.append("affiliation")
    if not _reason(session):
        missing.append("reason")
    return missing


def transfer_announcement(session: CallSession) -> str:
    """One plain sentence. No labels. This is the SMS."""

    apply_known_defaults(session)
    name = (session.caller_name or session.known_caller_name or "Someone").strip()
    who = session.affiliation.strip() or "personal"
    reason = _reason(session) or "a call"
    if who.casefold() in {"personal", "tenant"}:
        text = f"Call from {name}, {who}, about {reason}."
    else:
        text = f"Call from {name} with {who} about {reason}."
    if len(text) <= SMS_LIMIT:
        return text
    return text[: SMS_LIMIT - 1].rstrip() + "…"


def whisper_sentence(session: CallSession) -> str:
    """The same sentence, plus the instruction to press 1."""

    text = transfer_announcement(session).rstrip(".")
    return f"{text}. Press 1 to accept."


def _set_affiliation(session: CallSession, value: str) -> None:
    text = " ".join(value.split()).strip(" .")
    folded = text.casefold()
    if folded in _PERSONAL:
        session.affiliation = "personal"
        if session.company.strip().casefold() in _PERSONAL:
            session.company = ""
        return
    if folded in _TENANT:
        session.affiliation = "tenant"
        if session.company.strip().casefold() in _TENANT:
            session.company = ""
        return
    session.affiliation = text[:200]
    if not session.company.strip():
        session.company = text[:200]


def _relationship(value: object) -> str:
    text = str(value or "").strip().casefold().replace("-", "_").replace(" ", "_")
    aliases = {
        "prospect": "lead",
        "family": "vip",
        "landlord": "landlord_legal",
        "legal": "landlord_legal",
    }
    text = aliases.get(text, text)
    if text in {"tenant", "client", "vendor", "lead", "landlord_legal", "vip", "spam"}:
        return text
    return ""


def _reason(session: CallSession) -> str:
    for raw in (session.reason, session.summary):
        text = " ".join((raw or "").split()).strip(" .")
        if not text:
            continue
        folded = text.casefold()
        if folded.startswith("known ") and folded.endswith(" caller"):
            continue
        if folded in {"other", "no reason captured", "no details were captured"}:
            continue
        if text[:1].isupper() and not text[:2].isupper():
            text = text[:1].lower() + text[1:]
        return text
    return ""
