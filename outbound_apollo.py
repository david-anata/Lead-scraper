"""Apollo fallback for the hourly contact finder.

Prospeo stays first. This module runs only when Prospeo is out of credits,
is rate limited, finds nobody, or finds a person with no verified email.
People search is the official Apollo search (no email, no credits). People
match then reveals one email at a time. A contact is kept only when Apollo
marks that email verified. Guessed, extrapolated, unavailable, and locked
placeholder addresses are dropped.

``APOLLO_API_KEY`` empty means the fallback is off. ``APOLLO_DAILY_CREDITS``
is a separate Denver-day cap (default 50). Search costs nothing. A match that
returns a person counts as one credit unless Apollo reports ``credits_consumed``.
Phone reveal and waterfall enrichment stay off, because a phone costs extra
credits and a guessed email is never sent.
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, time as dtime
from typing import Any, Callable, Optional
from zoneinfo import ZoneInfo

import requests

import outbound_contacts as contacts

logger = logging.getLogger(__name__)

APOLLO_SEARCH_URL = "https://api.apollo.io/api/v1/mixed_people/api_search"
APOLLO_MATCH_URL = "https://api.apollo.io/api/v1/people/match"

# One revealed email is one credit. 50 is enough to cover part of a day when
# Prospeo is empty, and small enough that an unset cap cannot spend a month
# of Apollo credits in one weekday. Raise it with APOLLO_DAILY_CREDITS.
DEFAULT_DAILY_CREDITS = 50

# Apollo's people search withholds emails. Match is what reveals them.
# ``email_not_unlocked@domain.com`` is the placeholder some Apollo responses
# use for an address that was not revealed. It is not a sendable mailbox.
_LOCKED_EMAIL = "email_not_unlocked@domain.com"
_VERIFIED = "VERIFIED"

_DENVER = ZoneInfo("America/Denver")


def load_api_key() -> str:
    """Apollo key. Empty when the fallback must stay off."""

    return (os.getenv("APOLLO_API_KEY") or "").strip()


def daily_credit_cap() -> int:
    """Denver-day Apollo credit ceiling. Unset or invalid values use the default."""

    raw = (os.getenv("APOLLO_DAILY_CREDITS") or "").strip()
    if not raw:
        return DEFAULT_DAILY_CREDITS
    try:
        return max(0, int(raw))
    except ValueError:
        return DEFAULT_DAILY_CREDITS


def denver_midnight_utc(now: Optional[datetime] = None) -> datetime:
    """Naive UTC timestamp for the start of the current America/Denver day."""

    moment = now or datetime.now(_DENVER)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=_DENVER)
    else:
        moment = moment.astimezone(_DENVER)
    local_midnight = datetime.combine(moment.date(), dtime.min, tzinfo=moment.tzinfo)
    return local_midnight.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)


def prospeo_out_of_credits(result: contacts.FindResult) -> bool:
    """True when Prospeo reported quota, HTTP 402, or insufficient credits."""

    if result.credits_exhausted:
        return True
    reason = (result.reason or "").lower()
    if reason.startswith("out of contact-finder credits"):
        return True
    if "insufficient credit" in reason or "insufficient_credits" in reason:
        return True
    if "quota" in reason and any(word in reason for word in ("credit", "exceed", "exhaust")):
        return True
    return "search returned 402" in reason or reason.strip() in {"402", "http 402"}


def prospeo_rate_limited(result: contacts.FindResult) -> bool:
    """True when Prospeo returned 429 after its own retries."""

    if result.rate_limited:
        return True
    reason = (result.reason or "").lower()
    return reason.startswith("rate limit") or reason.strip() in {"429", "http 429"}


def prospeo_fallback_reason(result: contacts.FindResult) -> str:
    """Why Apollo should run, or '' when Prospeo's result stands.

    A verified contact is enough. Our own Prospeo credit budget, an invalid
    filter, and a transport error are not Apollo triggers. No person, no
    verified email, out of credits, and rate limit are.
    """

    if result.contacts:
        return ""
    if prospeo_out_of_credits(result):
        return "prospeo_out_of_credits"
    if prospeo_rate_limited(result):
        return "prospeo_rate_limit"
    reason = (result.reason or "").strip()
    low = reason.lower()
    if low == "credit budget reached.":
        return ""
    if "invalid_filters" in low:
        return ""
    if low.startswith("search failed") or low.startswith("enrich failed"):
        return ""
    if "contact finder is not connected" in low or low == "no domain.":
        return ""
    if "platform subdomain" in low:
        return ""
    if low.startswith("search returned") and "no_results" not in low:
        return ""
    if not reason or "no decision-maker" in low or "no_results" in low:
        return "prospeo_no_person"
    if "verified email" in low or "personal verified" in low:
        return "prospeo_no_verified_email"
    if "buyer title" in low or "first name" in low:
        return "prospeo_no_person"
    return ""


def _redact(text: str, secret: str) -> str:
    out = str(text)
    if secret and len(secret) > 6:
        out = out.replace(secret, "<apollo-key>")
    return out


def _search_params(body: dict[str, Any]) -> list[tuple[str, str]]:
    """Query pairs Apollo's people search expects, including repeated keys."""

    params: list[tuple[str, str]] = [
        ("page", str(body.get("page") or 1)),
        ("per_page", str(body.get("per_page") or 25)),
        ("include_similar_titles", "true" if body.get("include_similar_titles", True) else "false"),
    ]
    for title in body.get("person_titles") or []:
        params.append(("person_titles[]", str(title)))
    for domain in body.get("q_organization_domains_list") or []:
        params.append(("q_organization_domains_list[]", str(domain)))
    for status in body.get("contact_email_status") or []:
        params.append(("contact_email_status[]", str(status)))
    return params


def _http_post(url: str, *, key: str, body: dict[str, Any], timeout: int = 25) -> tuple[int, dict[str, Any]]:
    """POST one Apollo call. Search filters are query params. Match is JSON."""

    headers = {
        "Content-Type": "application/json",
        "Cache-Control": "no-cache",
        "x-api-key": key,
    }
    if url == APOLLO_SEARCH_URL:
        response = requests.post(url, headers=headers, params=_search_params(body), timeout=timeout)
    else:
        payload = {
            key_name: value
            for key_name, value in body.items()
            if key_name not in {"reveal_personal_emails", "reveal_phone_number"}
        }
        response = requests.post(
            url,
            headers=headers,
            params={
                "reveal_personal_emails": "false",
                "reveal_phone_number": "false",
            },
            json=payload,
            timeout=timeout,
        )
    try:
        data = response.json()
    except Exception:  # noqa: BLE001
        data = {}
    return response.status_code, data if isinstance(data, dict) else {}


def _people(data: dict[str, Any]) -> list[dict[str, Any]]:
    raw = data.get("people")
    if not isinstance(raw, list):
        raw = data.get("contacts")
    if not isinstance(raw, list):
        return []
    return [person for person in raw if isinstance(person, dict)]


def _as_ranked_rows(people: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for person in people:
        person_id = str(person.get("id") or person.get("person_id") or "").strip()
        title = str(person.get("title") or person.get("current_job_title") or "").strip()
        rows.append({
            "person": {
                "person_id": person_id,
                "current_job_title": title,
                "has_email": person.get("has_email", True),
            },
        })
    return rows


def _email_and_status(person: dict[str, Any]) -> tuple[str, str]:
    raw = person.get("email")
    status = str(person.get("email_status") or "")
    if isinstance(raw, dict):
        status = status or str(raw.get("status") or "")
        raw = raw.get("email") or ""
    return str(raw or "").strip().lower(), status.strip().upper()


def _revealed_email(address: str) -> bool:
    """True when the address is a real mailbox, not Apollo's locked placeholder."""

    email = (address or "").strip().lower()
    if "@" not in email or email == _LOCKED_EMAIL:
        return False
    local = email.split("@", 1)[0]
    if not local or "not_unlocked" in local:
        return False
    return True


def _match_credits(data: dict[str, Any]) -> int:
    """Credits this match consumed.

    Apollo charges one credit when a match returns demographic or email data,
    and nothing when ``match_confidence`` is ``none``. ``credits_consumed``
    wins when the response includes it. Phone credits are not requested.
    """

    if "credits_consumed" in data:
        try:
            return max(0, int(data.get("credits_consumed") or 0))
        except (TypeError, ValueError):
            return 0
    person = data.get("person")
    if not isinstance(person, dict) or not person:
        return 0
    confidence = str(person.get("match_confidence") or data.get("match_confidence") or "").lower()
    if confidence == "none":
        return 0
    if person.get("id") or person.get("email") or person.get("first_name"):
        return 1
    return 0


def _failure_reason(status: int, data: dict[str, Any], api_key: str) -> str:
    code = (data or {}).get("error_code") or (data or {}).get("code") or status
    message = ""
    for key in ("error", "message", "error_message"):
        value = (data or {}).get(key)
        if isinstance(value, str) and value.strip():
            message = " ".join(value.split())
            break
    text = f"Apollo returned {code}"
    if message and message.lower() not in {"true", "false"}:
        text = f"{text}: {message}"
    return _redact(text, api_key)[:500]


def find_contacts(
    domain: str,
    *,
    api_key: str,
    max_contacts: int = 2,
    post: Optional[contacts.Post] = None,
    sleep: Callable[[float], None] = time.sleep,
    pace_seconds: float = 0.4,
    rank_title: Optional[Callable[[str], Optional[int]]] = None,
    credit_budget: Optional[int] = None,
) -> contacts.FindResult:
    """Return up to ``max_contacts`` verified people at ``domain``.

    Same acceptance rules as Prospeo: buyer title, a real first name, no role
    mailbox, and a verified email. ``credit_budget`` is how many Apollo
    credits this call may spend. None means no extra limit. A budget of zero
    does not call Apollo.
    """

    domain = contacts.normalize_domain(domain)
    result = contacts.FindResult(domain=domain)
    if not api_key:
        result.reason = "Apollo fallback is disabled (APOLLO_API_KEY missing)."
        return result
    if not domain:
        result.reason = "No domain."
        return result
    if contacts.is_platform_subdomain(domain):
        result.reason = "Platform subdomain is not searchable."
        return result
    if credit_budget is not None and credit_budget <= 0:
        result.reason = "Apollo credit budget reached."
        return result
    post = post or _http_post
    rank_title = rank_title or contacts.title_rank

    body = {
        "page": 1,
        "per_page": 25,
        "include_similar_titles": True,
        "person_titles": list(contacts.SEARCH_TITLES),
        "contact_email_status": ["verified"],
        "q_organization_domains_list": [domain],
    }
    sleep(pace_seconds)
    try:
        status, data = contacts._post_retrying(
            post, APOLLO_SEARCH_URL, key=api_key, body=body, sleep=sleep,
        )
    except Exception as exc:  # noqa: BLE001
        result.reason = _redact(f"search failed: {exc}", api_key)
        return result
    if "credits_consumed" in data:
        try:
            result.credits_used += max(0, int(data.get("credits_consumed") or 0))
        except (TypeError, ValueError):
            pass
    if status == 429:
        result.reason = "Rate limit reached."
        result.rate_limited = True
        return result
    if status in (401, 403):
        result.reason = "Apollo API key was rejected."
        result.auth_failed = True
        return result
    if contacts.provider_credit_failure(status, data):
        result.reason = "Out of Apollo credits."
        result.credits_exhausted = True
        return result
    if status != 200:
        result.reason = _failure_reason(status, data, api_key)
        return result

    found = _people(data)
    result.candidates_seen = len(found)
    ranked = contacts.rank_candidates(_as_ranked_rows(found), rank_title)
    ranked = [
        item for item in ranked
        if item["person"].get("has_email") is not False
    ]
    if not ranked:
        result.reason = "No decision-maker found at this domain."
        return result

    skipped_role = False
    skipped_title = False
    skipped_name = False
    for item in ranked[: max_contacts + 3]:
        if len(result.contacts) >= max_contacts:
            break
        if credit_budget is not None and result.credits_used >= credit_budget:
            result.reason = "Apollo credit budget reached."
            break
        person_id = item["person"]["person_id"]
        sleep(pace_seconds)
        match_body = {
            "id": person_id,
            "reveal_personal_emails": False,
            "reveal_phone_number": False,
        }
        try:
            status, data = contacts._post_retrying(
                post, APOLLO_MATCH_URL, key=api_key, body=match_body, sleep=sleep,
            )
        except Exception as exc:  # noqa: BLE001
            result.reason = _redact(f"enrich failed: {exc}", api_key)
            continue
        if status == 429:
            result.reason = "Rate limit reached."
            result.rate_limited = True
            break
        if status in (401, 403):
            result.reason = "Apollo API key was rejected."
            result.auth_failed = True
            break
        if contacts.provider_credit_failure(status, data):
            result.reason = "Out of Apollo credits."
            result.credits_exhausted = True
            break
        if status != 200:
            continue
        result.credits_used += _match_credits(data)
        person = data.get("person") if isinstance(data.get("person"), dict) else {}
        if not person:
            continue
        email, status_text = _email_and_status(person)
        if not _revealed_email(email) or status_text != _VERIFIED:
            result.unverified_skipped += 1
            continue
        enriched_title = str(person.get("title") or person.get("current_job_title") or "").strip()
        first_name = str(person.get("first_name") or "").strip()
        if contacts.is_role_mailbox(email):
            skipped_role = True
            continue
        if rank_title(enriched_title) is None:
            skipped_title = True
            continue
        if not contacts.acceptable_first_name(first_name):
            skipped_name = True
            continue
        last_name = str(person.get("last_name") or "").strip()
        if "*" in last_name:
            last_name = ""
        result.contacts.append(contacts.Contact(
            domain=domain,
            first_name=first_name,
            last_name=last_name,
            email=email,
            title=enriched_title,
            linkedin_url=str(person.get("linkedin_url") or "").strip(),
            person_id=str(person.get("id") or person_id),
            source="apollo",
            email_status=_VERIFIED,
        ))
    if not result.contacts and not result.reason:
        if result.unverified_skipped and not (skipped_role or skipped_title or skipped_name):
            result.reason = "Candidates found but none had a verified email."
        elif skipped_role:
            result.reason = "Candidates found but none had a personal verified email."
        elif skipped_title:
            result.reason = "Candidates found but none had a buyer title."
        elif skipped_name:
            result.reason = "Candidates found but none had a first name."
        else:
            result.reason = "Candidates found but none had a verified email."
    return result


@dataclass
class FallbackDecision:
    """What the enrich loop should do after one Prospeo attempt."""

    result: contacts.FindResult
    apollo_credits: int = 0
    prospeo_exhausted: bool = False
    apollo_stopped: bool = False
    leave_open: bool = False
    stop_run: bool = False
    stop_reason: str = ""
    fallback_reason: str = ""
    unverified_skipped: int = 0
    apollo_called: bool = False
    cap_blocked: bool = False


def decide_fallback(
    prospeo: contacts.FindResult,
    *,
    apollo_key: str,
    apollo_find: Callable[..., contacts.FindResult],
    apollo_credit_budget: int,
    apollo_spent: int,
    prospeo_exhausted: bool,
    apollo_stopped: bool,
    max_contacts: int,
    rank_title: Optional[Callable[[str], Optional[int]]] = None,
) -> FallbackDecision:
    """Choose Apollo or keep the Prospeo result.

    ``prospeo_exhausted`` means this run already saw an out-of-credits
    response, so the caller did not call Prospeo again. A verified Prospeo
    contact is never replaced. When the Apollo key is missing, credit and
    rate-limit misses still stop the run the way they did before this fallback.
    """

    decision = FallbackDecision(
        result=prospeo,
        prospeo_exhausted=prospeo_exhausted,
        apollo_stopped=apollo_stopped,
    )
    if prospeo_exhausted:
        reason = "prospeo_out_of_credits"
    else:
        reason = prospeo_fallback_reason(prospeo)
        if reason == "prospeo_out_of_credits":
            decision.prospeo_exhausted = True
    decision.fallback_reason = reason
    if not reason:
        return decision
    if not apollo_key:
        if reason in {"prospeo_out_of_credits", "prospeo_rate_limit"}:
            decision.stop_run = True
            decision.stop_reason = prospeo.reason or "Out of contact-finder credits."
        return decision
    if apollo_stopped:
        decision.leave_open = True
        if decision.prospeo_exhausted:
            decision.stop_run = True
            decision.stop_reason = "Apollo fallback is stopped for this run."
        return decision
    remaining = max(0, int(apollo_credit_budget) - int(apollo_spent))
    if remaining <= 0:
        decision.cap_blocked = True
        # A finished Prospeo miss stays finished, so the same domains do not
        # sit at the front of every later run. A credit outage or a 429 did
        # not finish the search, so the domain stays open.
        if decision.prospeo_exhausted or prospeo_rate_limited(prospeo):
            decision.leave_open = True
        if decision.prospeo_exhausted:
            decision.stop_run = True
            decision.stop_reason = "Apollo daily credit cap reached."
        return decision

    logger.info(
        "[outbound-apollo] fallback domain=%s reason=%s budget=%s",
        prospeo.domain, reason, remaining,
    )
    try:
        apollo_res = apollo_find(
            prospeo.domain,
            api_key=apollo_key,
            max_contacts=max_contacts,
            credit_budget=remaining,
            rank_title=rank_title,
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("[outbound-apollo] fallback failed for %s", prospeo.domain)
        decision.result = contacts.FindResult(
            domain=prospeo.domain,
            credits_used=prospeo.credits_used,
            reason=_redact(f"{prospeo.reason} Apollo: {exc}", apollo_key)[:500],
        )
        return decision
    if not isinstance(apollo_res, contacts.FindResult):
        decision.result = prospeo
        return decision
    decision.apollo_called = True
    decision.apollo_credits = max(0, int(apollo_res.credits_used or 0))
    decision.unverified_skipped = max(0, int(apollo_res.unverified_skipped or 0))
    if apollo_res.auth_failed or apollo_res.credits_exhausted:
        decision.apollo_stopped = True
    if apollo_res.contacts:
        decision.result = contacts.FindResult(
            domain=prospeo.domain,
            contacts=list(apollo_res.contacts),
            credits_used=prospeo.credits_used,
            candidates_seen=apollo_res.candidates_seen,
            reason="",
        )
        return decision

    blocked = bool(
        apollo_res.rate_limited
        or apollo_res.credits_exhausted
        or apollo_res.auth_failed
        or (apollo_res.reason or "") == "Apollo credit budget reached."
    )
    if blocked:
        decision.leave_open = True
        decision.cap_blocked = (apollo_res.reason or "") == "Apollo credit budget reached."
        decision.result = contacts.FindResult(
            domain=prospeo.domain,
            credits_used=prospeo.credits_used,
            candidates_seen=apollo_res.candidates_seen,
            reason=apollo_res.reason or prospeo.reason,
            credits_exhausted=apollo_res.credits_exhausted,
            rate_limited=apollo_res.rate_limited,
            auth_failed=apollo_res.auth_failed,
            unverified_skipped=apollo_res.unverified_skipped,
        )
        if decision.prospeo_exhausted and (apollo_res.credits_exhausted or apollo_res.auth_failed):
            decision.stop_run = True
            decision.stop_reason = apollo_res.reason or "Out of Apollo credits."
        return decision

    combined = (prospeo.reason or "").strip()
    apollo_reason = (apollo_res.reason or "").strip()
    if apollo_reason and apollo_reason not in combined:
        combined = f"{combined} Apollo: {apollo_reason}".strip()
    decision.result = contacts.FindResult(
        domain=prospeo.domain,
        credits_used=prospeo.credits_used,
        candidates_seen=apollo_res.candidates_seen,
        reason=combined[:500],
        unverified_skipped=apollo_res.unverified_skipped,
    )
    return decision
