"""Find the right people at a sourced brand (replaces Clay's contact step).

One brand domain in, up to N verified contacts out. Prospeo is the first
provider: a person search finds candidates at the domain (1 credit per search,
up to 25 results), then each chosen candidate is enriched to reveal a verified
email (1 credit when an email is found, nothing when not).

Rules this module enforces:
  * The API key is a secret. It is never logged, never put in an error string,
    never returned to a page.
  * Nothing is guessed. A candidate without a verified email is dropped, not
    padded with a pattern guess, because a bounce costs more than a missing lead.
  * Network is injectable (``post``) so every path is testable without credits.
  * Everything fails soft: a provider error yields an empty result with a reason,
    never an exception into the daily job.
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

import requests
import tldextract

logger = logging.getLogger(__name__)

PROSPEO_SEARCH_URL = "https://api.prospeo.io/search-person"
PROSPEO_ENRICH_URL = "https://api.prospeo.io/enrich-person"

# Offline public-suffix snapshot. suffix_list_urls=() means this never fetches.
_EXTRACT = tldextract.TLDExtract(suffix_list_urls=(), cache_dir=None)

# Titles we want, best first. Matching is substring, lowercase. Order is the
# ranking: owners and founders answer cold email about tools; ecommerce and
# Amazon leads are the next best buyer of Anata Intelligence.
TITLE_PRIORITY: tuple[tuple[str, ...], ...] = (
    ("founder", "co-founder", "cofounder", "owner", "ceo", "chief executive", "president"),
    ("ecommerce", "e-commerce", "amazon", "marketplace"),
    ("growth", "marketing", "cmo", "chief marketing", "digital"),
    ("coo", "chief operating", "operations", "general manager", "managing director"),
)

# Searched with CONTAINS so "Co-Founder & CEO" and "Head of Ecommerce" both hit.
SEARCH_TITLES = [
    "founder", "owner", "ceo", "president", "ecommerce", "e-commerce",
    "amazon", "marketplace", "growth", "marketing", "chief operating",
]

# Titles that match a keyword above but are never the buyer.
EXCLUDE_TITLES = ("intern", "assistant", "coordinator", "recruiter", "student")


@dataclass
class Contact:
    domain: str
    first_name: str
    last_name: str
    email: str
    title: str = ""
    linkedin_url: str = ""
    person_id: str = ""
    source: str = "prospeo"
    email_status: str = "VERIFIED"


@dataclass
class FindResult:
    domain: str
    contacts: list[Contact] = field(default_factory=list)
    credits_used: int = 0
    candidates_seen: int = 0
    reason: str = ""

    @property
    def ok(self) -> bool:
        return bool(self.contacts)


def _redact(text: str, secret: str) -> str:
    out = str(text)
    if secret and len(secret) > 6:
        out = out.replace(secret, "<prospeo-key>")
    return out


def title_rank(title: str) -> Optional[int]:
    """Lower is better. None means do not contact."""
    t = (title or "").lower()
    if not t or any(bad in t for bad in EXCLUDE_TITLES):
        return None
    for rank, keywords in enumerate(TITLE_PRIORITY):
        if any(k in t for k in keywords):
            return rank
    return None


def rank_candidates(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Order search results best-first and drop people we would never email."""
    scored: list[tuple[int, int, dict[str, Any]]] = []
    for idx, item in enumerate(results or []):
        person = (item or {}).get("person") or {}
        rank = title_rank(person.get("current_job_title") or "")
        if rank is None or not person.get("person_id"):
            continue
        scored.append((rank, idx, item))
    scored.sort(key=lambda s: (s[0], s[1]))
    return [s[2] for s in scored]


def normalize_host(value: str) -> str:
    """Lowercase host with scheme, path, port, leading www, and any mailbox stripped."""
    v = (value or "").strip().lower()
    if "://" in v:
        v = v.split("://", 1)[1]
    v = v.split("/")[0].split("?")[0].split("#")[0]
    if "@" in v:
        v = v.split("@", 1)[1]
    if ":" in v and not v.startswith("["):
        v = v.split(":", 1)[0]
    if v.startswith("www."):
        v = v[4:]
    return v.strip(".")


def registrable_domain(value: str) -> str:
    """eTLD+1, so ca.petlibro.com and shop.brand.co.uk search as the brand.

    A host we cannot reduce (single label, or no public suffix) is returned as-is.
    """
    host = normalize_host(value)
    if not host or "." not in host:
        return host
    try:
        ext = _EXTRACT(host)
    except Exception:  # noqa: BLE001
        return host
    if not ext.domain or not ext.suffix:
        return host
    return f"{ext.domain}.{ext.suffix}"


def normalize_domain(value: str) -> str:
    """The domain we search, store, and dedupe on: the registrable domain."""
    return registrable_domain(value)


def search_failure_reason(status: int, data: dict[str, Any], api_key: str) -> str:
    """Prospeo search error, including filter_error, with the key removed."""
    code = (data or {}).get("error_code") or status
    detail = (data or {}).get("filter_error") or ""
    if not isinstance(detail, str):
        detail = str(detail)
    detail = " ".join(detail.split())
    text = f"search returned {code}"
    if detail:
        text = f"{text}: {detail}"
    return _redact(text, api_key)[:500]


Post = Callable[..., tuple[int, dict[str, Any]]]


def _http_post(url: str, *, key: str, body: dict[str, Any], timeout: int = 25) -> tuple[int, dict[str, Any]]:
    resp = requests.post(
        url,
        json=body,
        headers={"X-KEY": key, "Content-Type": "application/json"},
        timeout=timeout,
    )
    try:
        data = resp.json()
    except Exception:  # noqa: BLE001
        data = {}
    return resp.status_code, data if isinstance(data, dict) else {}


RETRY_WAITS = (2.0, 5.0, 10.0, 20.0, 30.0)


def _post_retrying(post: Post, url: str, *, key: str, body: dict[str, Any],
                   sleep: Callable[[float], None]) -> tuple[int, dict[str, Any]]:
    """Call ``post``; on HTTP 429 wait and retry a few times before giving up."""
    status, data = post(url, key=key, body=body)
    for wait in RETRY_WAITS:
        if status != 429:
            break
        sleep(wait)
        status, data = post(url, key=key, body=body)
    return status, data


def find_contacts(
    domain: str,
    *,
    api_key: str,
    max_contacts: int = 2,
    post: Optional[Post] = None,
    sleep: Callable[[float], None] = time.sleep,
    pace_seconds: float = 1.1,
) -> FindResult:
    """Return up to ``max_contacts`` verified people at ``domain``."""
    domain = normalize_domain(domain)
    result = FindResult(domain=domain)
    if not api_key:
        result.reason = "Contact finder is not connected (PROSPEO_API_KEY missing)."
        return result
    if not domain:
        result.reason = "No domain."
        return result
    post = post or _http_post

    search_body = {
        "page": 1,
        "filters": {
            "company": {"websites": {"include": [domain]}},
            "person_job_title": {"include": SEARCH_TITLES, "match_mode": "CONTAINS"},
        },
    }
    sleep(pace_seconds)  # Prospeo allows about 1 request a second
    try:
        status, data = _post_retrying(post, PROSPEO_SEARCH_URL, key=api_key, body=search_body, sleep=sleep)
    except Exception as exc:  # noqa: BLE001
        result.reason = _redact(f"search failed: {exc}", api_key)
        return result
    if status == 429:
        result.reason = "Rate limit reached."
        return result
    if status != 200 or data.get("error"):
        result.reason = search_failure_reason(status, data, api_key)
        return result

    found = data.get("results") or []
    result.candidates_seen = len(found)
    if found and not data.get("free"):
        result.credits_used += 1
    ranked = rank_candidates(found)
    if not ranked:
        result.reason = "No decision-maker found at this domain."
        return result

    # Enrich best-first until we have enough, trying a couple of extras since
    # some people have no verified email.
    for item in ranked[: max_contacts + 3]:
        if len(result.contacts) >= max_contacts:
            break
        person = item["person"]
        sleep(pace_seconds)
        body = {
            "only_verified_email": True,
            "enrich_mobile": False,
            "data": {"person_id": person["person_id"]},
        }
        try:
            status, data = _post_retrying(post, PROSPEO_ENRICH_URL, key=api_key, body=body, sleep=sleep)
        except Exception as exc:  # noqa: BLE001
            result.reason = _redact(f"enrich failed: {exc}", api_key)
            continue
        if status == 429:
            result.reason = "Rate limit reached."
            break
        if status != 200 or data.get("error"):
            if (data.get("error_code") or "") == "INSUFFICIENT_CREDITS":
                result.reason = "Out of contact-finder credits."
                break
            continue  # NO_MATCH: not charged, try the next person
        p = data.get("person") or {}
        email = ((p.get("email") or {}).get("email") or "").strip().lower()
        status_text = (p.get("email") or {}).get("status") or ""
        if not email or status_text.upper() != "VERIFIED":
            continue
        if not data.get("free_enrichment"):
            result.credits_used += 1
        result.contacts.append(Contact(
            domain=domain,
            first_name=(p.get("first_name") or "").strip(),
            last_name=(p.get("last_name") or "").strip(),
            email=email,
            title=(p.get("current_job_title") or "").strip(),
            linkedin_url=(p.get("linkedin_url") or "").strip(),
            person_id=p.get("person_id") or "",
            email_status=status_text.upper(),
        ))
    if not result.contacts and not result.reason:
        result.reason = "Candidates found but none had a verified email."
    return result


def load_contact_config() -> str:
    """The one value David sets on Render. Never hardcoded, never rendered back."""
    for name in ("PROSPEO_API_KEY", "PROSPEO_KEY", "PROSPEO_TOKEN"):
        value = (os.getenv(name) or "").strip()
        if value:
            return value
    return ""
