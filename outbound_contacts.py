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
  * A verified role inbox (info@, sales@, noreply@, and the same list) is
    dropped. Only a named person's address is kept.
  * Network is injectable (``post``) so every path is testable without credits.
  * Everything fails soft: a provider error yields an empty result with a reason,
    never an exception into the daily job.
"""

from __future__ import annotations

import logging
import os
import re
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

import requests
import tldextract

logger = logging.getLogger(__name__)

PROSPEO_SEARCH_URL = "https://api.prospeo.io/search-person"
PROSPEO_ENRICH_URL = "https://api.prospeo.io/enrich-person"

# Bundled public-suffix snapshot only. Empty suffix_list_urls never fetches,
# and cache_dir=None does not write a cache (Vercel /var/task is read-only).
_EXTRACT = tldextract.TLDExtract(
    suffix_list_urls=(),
    cache_dir=None,
    fallback_to_snapshot=True,
)

# Titles we want, best first. Founders and presidents outrank the functional
# buyers. The second rank is a director, head, or VP in ecommerce, Amazon,
# marketplaces, growth, or marketing, plus CMO, COO, and ecommerce managers.
# Matching is not a raw substring: "president" must not promote "vice
# president", and "app" is not a title word.
TITLE_PRIORITY: tuple[tuple[str, ...], ...] = (
    ("founder", "co-founder", "cofounder", "owner", "ceo", "chief executive", "president"),
    ("director", "head", "vp", "vice president", "cmo", "chief marketing",
     "coo", "chief operating", "ecommerce manager", "e-commerce manager",
     "digital commerce manager"),
)

# Searched with CONTAINS so "Co-Founder & CEO" and "Head of Ecommerce" both hit.
# title_rank drops associates, specialists, and the other excluded roles.
# "coo" also matches "coordinator"; those titles are dropped after the search.
SEARCH_TITLES = [
    "founder", "owner", "ceo", "chief executive", "president",
    "head of", "director", "vice president", "vp",
    "ecommerce", "e-commerce", "amazon", "marketplace", "growth", "marketing",
    "cmo", "chief marketing", "coo", "chief operating", "digital commerce",
]

# Generic inboxes. A personal name in the local part is not on this list.
# Dots and dashes are ignored, so customer.service@ and no-reply@ match.
# Numbered and dotted variants (sales1, info.us, hello-team) are matched in
# is_role_mailbox, not by listing every spelling here.
ROLE_LOCALS = frozenset({
    "marketing", "info", "sales", "support", "hello", "hi", "contact", "team",
    "admin", "help", "orders", "order", "office", "ecommerce", "customerservice",
    "customercare", "customersupport", "noreply", "donotreply",
    "press", "pr", "media", "wholesale", "care", "shop", "store",
    "partnerships", "partnership", "billing", "returns", "careers", "career",
    "jobs", "job", "accounts", "accounting", "enquiries", "enquiry",
    "inquiries", "inquiry", "newsletter", "webmaster", "postmaster",
    "abuse", "feedback", "reception", "frontdesk", "ops", "operations",
    "hr", "legal", "finance", "purchasing", "procurement", "bookings",
    "reservations", "service", "services", "mail", "email", "inbox",
    "general", "hq", "news",
})
# A dotted suffix that does not make info.us or sales.uk a person.
_ROLE_SUFFIXES = frozenset({
    "us", "usa", "uk", "eu", "au", "ca", "nz", "de", "fr", "en", "global",
    "intl", "international", "team", "dept", "hq", "office", "group",
    "emea", "apac", "na", "latam",
})

# Titles that match a keyword above but are never the buyer.
EXCLUDE_TITLES = (
    "intern", "assistant", "coordinator", "recruiter", "student",
    "associate", "specialist", "customer service", "customer support",
    "project manager",
)

_EXCLUDE_RE = re.compile(
    r"\b(?:interns?|assistants?|coordinators?|recruiters?|students?|associates?|specialists?)\b"
    r"|\bproject managers?\b|\bproject management\b"
    r"|\bcustomer[\s-]?service\b|\bcustomer[\s-]?support\b",
    re.I,
)
_CREATIVE_RE = re.compile(
    r"\b(?:creatives?|designers?|copywriters?|photographers?|illustrators?|videographers?|art directors?)\b",
    re.I,
)
_FUNCTION_RE = re.compile(
    r"\b(?:e[\s-]?commerce|amazon|marketplaces?|growth|marketing)\b",
    re.I,
)
_VP_RE = re.compile(r"\b(?:vp|svp|evp|vice president)\b", re.I)
_LEADER_RE = re.compile(r"\b(?:heads?|directors?|vp|svp|evp|vice president)\b", re.I)
_TOP_RE = re.compile(
    r"\b(?:co-?founders?|founders?|co-?owners?|owners?|ceo|chief executive|presidents?)\b",
    re.I,
)
_CMO_RE = re.compile(r"\b(?:cmo|chief marketing(?: officer)?)\b", re.I)
_COO_RE = re.compile(r"\b(?:coo|chief operating(?: officer)?)\b", re.I)
_ECOM_MANAGER_RE = re.compile(
    r"\b(?:e[\s-]?commerce|digital[\s-]?commerce)\s+managers?\b"
    r"|\bmanagers?\b(?:\s+\w+){0,3}\s+(?:e[\s-]?commerce|digital[\s-]?commerce)\b",
    re.I,
)
# Amazon and marketplace managers are buyers. A generic marketing manager is not.
_CHANNEL_MANAGER_RE = re.compile(
    r"\b(?:amazon|marketplaces?)\s+managers?\b"
    r"|\bmanagers?\b(?:\s+\w+){0,3}\s+(?:amazon|marketplaces?)\b",
    re.I,
)
# "Product owner" is an agile role, not the owner of the company.
_PRODUCT_OWNER_RE = re.compile(
    r"\b(?:product|process|project|category)\s+owners?\b",
    re.I,
)
# Engineering, HR, and design are not buyers, even with a seniority word.
_NON_BUYER_RE = re.compile(
    r"\b(?:engineers?|engineering|developers?|software)\b"
    r"|\b(?:designers?|design)\b"
    r"|\b(?:human resources|people ops|talent acquisition)\b"
    r"|\bhr\b",
    re.I,
)


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


def _is_company_leader(title: str) -> bool:
    """Founder, owner, CEO, or president. Not a vice president or product owner."""

    if _VP_RE.search(title):
        return False
    stripped = _PRODUCT_OWNER_RE.sub(" ", title)
    return _TOP_RE.search(stripped) is not None


def title_rank(title: str) -> Optional[int]:
    """Lower is better. None means do not contact.

    Rank 0 is founder, owner, CEO, or president (not vice president, and not
    a product owner). Rank 1 is a director, head, or VP of ecommerce, Amazon,
    marketplaces, growth, or marketing, and also a CMO, COO, or ecommerce,
    Amazon, or marketplace manager. Associates, interns, assistants,
    coordinators, engineering, HR, design, and other non-buyer roles are
    excluded. A founder stays even when the title also says designer.
    """

    raw = (title or "").strip()
    if not raw or any(bad in raw.lower() for bad in EXCLUDE_TITLES) or _EXCLUDE_RE.search(raw):
        return None
    if _is_company_leader(raw):
        return 0
    if _NON_BUYER_RE.search(raw):
        return None
    if _CREATIVE_RE.search(raw) and not _FUNCTION_RE.search(raw):
        return None
    # Rank 1 is the second TITLE_PRIORITY group.
    if len(TITLE_PRIORITY) > 1 and (
        (_LEADER_RE.search(raw) and _FUNCTION_RE.search(raw))
        or _CMO_RE.search(raw)
        or _COO_RE.search(raw)
        or _ECOM_MANAGER_RE.search(raw)
        or _CHANNEL_MANAGER_RE.search(raw)
    ):
        return 1
    return None


def _role_segments(local: str) -> list[str]:
    return [part for part in re.split(r"[._\-]+", local) if part]


def is_role_mailbox(address: str) -> bool:
    """True when the mailbox is a shared role, not a named person.

    ``info@``, ``customer.service@``, ``no-reply@``, ``sales1@``, ``info.us@``,
    and ``hello-team@`` match. ``jane.smith@`` and ``priya@`` do not. The
    check is the local part only.
    """

    local = (address or "").strip().lower().split("@", 1)[0]
    local = local.split("+", 1)[0]
    compact = re.sub(r"[^a-z0-9]", "", local)
    if not compact:
        return False
    if compact in ROLE_LOCALS:
        return True
    for role in ROLE_LOCALS:
        rest = compact[len(role):]
        if compact.startswith(role) and rest.isdigit():
            return True
    segments = _role_segments(local)
    if not segments:
        return False
    if segments[0] in ROLE_LOCALS:
        return True
    if any(part in ROLE_LOCALS for part in segments) and all(
        part in ROLE_LOCALS or part in _ROLE_SUFFIXES for part in segments
    ):
        return True
    return False


def acceptable_first_name(name: str) -> bool:
    """True when the value is a person's first name, not a blank or a role word."""

    raw = (name or "").strip()
    if not raw or not re.search(r"[A-Za-z]", raw):
        return False
    compact = re.sub(r"[^a-z]", "", raw.lower())
    if not compact or compact in ROLE_LOCALS or compact in {"there", "friend", "hey"}:
        return False
    return True


def lead_quality_reason(
    *,
    email: str,
    first_name: str,
    title: str = "",
    require_title: bool = False,
) -> str:
    """Why this person should not be added, or '' when they pass.

    A missing title is rejected only when ``require_title`` is set. A title
    that is present must pass ``title_rank`` either way. Historical re-engage
    rows often have no title, and that absence is not itself a rejection.
    """

    if is_role_mailbox(email):
        return "role mailbox"
    if not acceptable_first_name(first_name):
        return "missing first name"
    present = (title or "").strip()
    if require_title or present:
        if title_rank(present) is None:
            return "title"
    return ""


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


# Storefront hosts on a platform are separate stores. Collapsing them to the
# platform apex would treat every Shopify store as one company.
PLATFORM_REGISTRABLE = frozenset({
    "myshopify.com",
    "shopify.com",
    "shopifypreview.com",
})


def company_domain(value: str) -> str:
    """Company key for search, storage, and dedupe.

    A custom host becomes its registrable domain, so us.brand.com and
    shop.brand.com are one company. A platform storefront such as
    glow.myshopify.com stays that full host. The platform apex itself is
    not a company.
    """

    host = normalize_host(value)
    if not host:
        return ""
    root = registrable_domain(host)
    if root in PLATFORM_REGISTRABLE:
        if host == root:
            return ""
        return host
    return root or host


def normalize_domain(value: str) -> str:
    """The domain we search, store, and dedupe on."""
    return company_domain(value)


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
    # some people have no verified email. The search title is not enough: the
    # enriched title is the one we store, and it can be a weaker role than the
    # search hit (for example "Ecommerce Manager" enriching to
    # "E-commerce Merchandise Associate").
    skipped_role = False
    skipped_title = False
    skipped_name = False
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
        enriched_title = (p.get("current_job_title") or "").strip()
        first_name = (p.get("first_name") or "").strip()
        if is_role_mailbox(email):
            skipped_role = True
            continue
        if title_rank(enriched_title) is None:
            skipped_title = True
            continue
        if not acceptable_first_name(first_name):
            skipped_name = True
            continue
        result.contacts.append(Contact(
            domain=domain,
            first_name=first_name,
            last_name=(p.get("last_name") or "").strip(),
            email=email,
            title=enriched_title,
            linkedin_url=(p.get("linkedin_url") or "").strip(),
            person_id=p.get("person_id") or "",
            email_status=status_text.upper(),
        ))
    if not result.contacts and not result.reason:
        if skipped_role:
            result.reason = "Candidates found but none had a personal verified email."
        elif skipped_title:
            result.reason = "Candidates found but none had a buyer title."
        elif skipped_name:
            result.reason = "Candidates found but none had a first name."
        else:
            result.reason = "Candidates found but none had a verified email."
    return result


def load_contact_config() -> str:
    """The one value David sets on Render. Never hardcoded, never rendered back."""
    for name in ("PROSPEO_API_KEY", "PROSPEO_KEY", "PROSPEO_TOKEN"):
        value = (os.getenv(name) or "").strip()
        if value:
            return value
    return ""
