"""Push Clay's found contacts into a HeyReach campaign, so LinkedIn runs beside email.

There is already a HeyReach connector in this repo, but it lives in the root
retired outbound flow, which was a different service and lead
flow. This one belongs to the outbound system on the agent: the brands the
Amazon check has qualified, enriched by Clay into people, sent to LinkedIn.

The API contract uses the established shape that has
been using in production: X-API-KEY, AddLeadsToCampaignV2, and a lead shaped
firstName / lastName / email / company / position / linkedinUrl.

The one rule that matters more than any of that: never message the same person
twice. HeyReach dedupes within a campaign, but a profile moved between
campaigns, or a campaign rebuilt, would see them again. So we keep our own
record, keyed on campaign plus normalized profile URL, and we write it BEFORE
trusting the response - a push that half succeeded must not be replayable.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Iterable

import requests

logger = logging.getLogger(__name__)

CHECK_KEY_URL = "https://api.heyreach.io/api/public/auth/CheckApiKey"
ADD_LEADS_URL = "https://api.heyreach.io/api/public/campaign/AddLeadsToCampaignV2"

# HeyReach caps a single AddLeads call. Batched rather than truncated, because a
# silently dropped tail looks identical to a successful push.
_BATCH = 100
_TIMEOUT = 60

# A real profile URL, not a company page, a post, or a search result. Anything
# else would be accepted by the API and then quietly never message anyone.
_PROFILE_RE = re.compile(r"^https://([a-z0-9-]+\.)?linkedin\.com/in/[^/\s]+$", re.IGNORECASE)


def normalize_profile_url(url: str) -> str:
    """One canonical form per person, so dedupe actually dedupes.

    Normalize profile URLs consistently: the
    two systems may end up sharing a processed list, and two spellings of the
    same profile would defeat the whole point.
    """
    out = (url or "").strip()
    if not out:
        return ""
    out = out.split("?", 1)[0].strip().rstrip("/")
    out = re.sub(r"^http://", "https://", out, flags=re.IGNORECASE)
    return out.lower()


def is_profile_url(url: str) -> bool:
    return bool(_PROFILE_RE.match(normalize_profile_url(url)))


def lead_key(campaign_id: str, url: str) -> str:
    norm = normalize_profile_url(url)
    if not norm:
        return ""
    return f"{str(campaign_id).strip()}::{norm}"


def _text(row: dict[str, Any], *names: str) -> str:
    """First non-empty of several column spellings.

    Clay's exports are named by whoever built the table, so the same field
    arrives as "Work Email", "email", or "Email" depending on the day. Guessing
    a single spelling is how a column silently exports blank.
    """
    for n in names:
        for key in (n, n.lower(), n.title(), n.replace("_", " "), n.replace("_", " ").title()):
            v = row.get(key)
            if v not in (None, ""):
                return str(v).strip()
    return ""


def prepare(rows: Iterable[dict[str, Any]], *, campaign_id: str,
            already: set[str] | None = None) -> tuple[list[dict[str, str]], list[str], dict[str, int]]:
    """Turn exported contact rows into HeyReach leads, dropping what we must not send.

    Returns the leads, the dedupe keys to record for them, and counts that
    reconcile: every input row lands in exactly one of sent / duplicate /
    no_profile, so a number that does not add up is visible instead of assumed.
    """
    seen = set(already or ())
    leads: list[dict[str, str]] = []
    keys: list[str] = []
    stats = {"rows": 0, "queued": 0, "duplicate": 0, "no_profile": 0}
    # Within-batch dedupe too: Clay can return the same person twice when two
    # company rows resolve to one contact.
    in_batch: set[str] = set()

    for row in rows:
        stats["rows"] += 1
        url = _text(row, "linkedin_url", "linkedin", "profile_url", "LinkedIn Profile", "LinkedIn")
        if not is_profile_url(url):
            stats["no_profile"] += 1
            continue
        key = lead_key(campaign_id, url)
        if key in seen or key in in_batch:
            stats["duplicate"] += 1
            continue
        in_batch.add(key)
        leads.append({
            "firstName": _text(row, "first_name", "firstname", "First Name"),
            "lastName": _text(row, "last_name", "lastname", "Last Name"),
            "email": _text(row, "email", "work_email", "Work Email"),
            "company": _text(row, "company_name", "company", "brand", "Merchant Name"),
            "position": _text(row, "role", "title", "position", "Job Title"),
            "linkedinUrl": normalize_profile_url(url),
        })
        keys.append(key)
        stats["queued"] += 1

    return leads, keys, stats


def contacts_from(rows: Iterable[dict[str, Any]]) -> list[dict[str, str]]:
    """Every person in the file, whether or not they have a LinkedIn profile.

    Wider than prepare(): the email queue needs to know about email-only
    contacts too, so that when Instantly says one of them replied we can stop
    chasing them rather than never having heard of them.
    """
    out = []
    for row in rows:
        email = _text(row, "email", "work_email", "Work Email")
        if not email:
            continue
        url = _text(row, "linkedin_url", "linkedin", "profile_url", "LinkedIn Profile", "LinkedIn")
        out.append({
            "email": email,
            # Only a real profile is stored. A company page here would put
            # someone in the LinkedIn queue that nobody can actually message.
            "linkedin_url": normalize_profile_url(url) if is_profile_url(url) else "",
            "first_name": _text(row, "first_name", "firstname", "First Name"),
            "last_name": _text(row, "last_name", "lastname", "Last Name", "surname", "Surname"),
            "company": _text(row, "company_name", "company", "brand", "Merchant Name"),
        })
    return out


def check_key(api_key: str, *, session: Any = None) -> tuple[bool, str]:
    """Confirm the key works before sending anyone anywhere."""
    if not str(api_key or "").strip():
        return False, "HEYREACH_API_KEY is not set."
    http = session or requests
    try:
        r = http.get(CHECK_KEY_URL, headers={"X-API-KEY": api_key}, timeout=_TIMEOUT)
    except requests.RequestException as exc:
        logger.warning("[heyreach] key check failed: %s", exc)
        return False, "Could not reach HeyReach."
    if r.status_code == 200:
        return True, "Key accepted."
    if r.status_code in (401, 403):
        return False, "HeyReach rejected the key."
    return False, f"HeyReach returned {r.status_code}."


def _campaign_id(raw: str) -> Any:
    v = str(raw or "").strip()
    return int(v) if v.isdigit() else v


def push(rows: Iterable[dict[str, Any]], *, api_key: str, campaign_id: str,
         already: set[str] | None = None, record=None, session: Any = None) -> dict[str, Any]:
    """Send eligible contacts to the campaign. Never sends the same profile twice.

    `record` is called with the keys of a batch BEFORE the result is trusted,
    because HeyReach may have accepted leads on a call that then times out on
    the way back. Recording after the response would let a retry message those
    people a second time, which is the one failure with no undo.
    """
    campaign = str(campaign_id or "").strip()
    if not campaign:
        return {"ok": False, "reason": "HEYREACH_CAMPAIGN_ID is not set.", **_zero()}

    ok, why = check_key(api_key, session=session)
    if not ok:
        return {"ok": False, "reason": why, **_zero()}

    leads, keys, stats = prepare(rows, campaign_id=campaign, already=already)
    if not leads:
        return {"ok": True, "sent": 0, "reason": _nothing_reason(stats), **stats}

    http = session or requests
    sent = 0
    for start in range(0, len(leads), _BATCH):
        chunk, chunk_keys = leads[start:start + _BATCH], keys[start:start + _BATCH]
        if record is not None:
            record(chunk_keys)
        try:
            r = http.post(ADD_LEADS_URL,
                          headers={"X-API-KEY": api_key, "Content-Type": "application/json"},
                          json={"campaignId": _campaign_id(campaign), "leads": chunk},
                          timeout=_TIMEOUT)
            r.raise_for_status()
        except requests.RequestException as exc:
            # Deliberately not un-recording the keys. Those people may already be
            # in the campaign; leaving them marked costs us one missed contact,
            # un-marking them risks messaging someone twice.
            logger.warning("[heyreach] batch failed after %s sent: %s", sent, exc)
            return {"ok": False, "sent": sent,
                    "reason": f"HeyReach refused the batch after {sent} contact(s). "
                              f"Nobody was messaged twice.", **stats}
        sent += len(chunk)

    return {"ok": True, "sent": sent,
            "reason": f"Sent {sent} contact(s) to HeyReach.", **stats}


def _zero() -> dict[str, int]:
    return {"sent": 0, "rows": 0, "queued": 0, "duplicate": 0, "no_profile": 0}


def _nothing_reason(stats: dict[str, int]) -> str:
    if not stats["rows"]:
        return "That file had no rows."
    bits = []
    if stats["duplicate"]:
        bits.append(f"{stats['duplicate']} already sent before")
    if stats["no_profile"]:
        bits.append(f"{stats['no_profile']} with no LinkedIn profile")
    return "Nothing to send: " + ", ".join(bits) + "." if bits else "Nothing to send."
