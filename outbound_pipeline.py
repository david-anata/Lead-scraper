"""StoreLeads -> Clay outbound pipeline.

Pulls ecommerce brands that match Anata's ICP from the StoreLeads API, dedupes
against brands already contacted, and pushes the fresh ones into a Clay table
(via Clay's inbound webhook). Clay then finds the decision-maker, verifies the
email, qualifies, personalizes, and hands off to Instantly.

This module deliberately does NOT send any email. It only sources brands and
hands them to Clay. The ICP gate here is the authoritative filter, matched to
the target already defined in the app and in docs/outbound/03-storeleads-filter-recipe.md.

Network calls (fetch_storeleads_page, push_to_clay) are thin and injectable so
the ICP logic can be tested without a live key. Live behaviour is verified once
STORELEADS_API_KEY and CLAY_WEBHOOK_URL are set on Render.
"""

from __future__ import annotations

import dataclasses
import logging
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Optional

import requests

logger = logging.getLogger(__name__)

STORELEADS_BASE_URL = "https://storeleads.app/json/api/v1/all"
STORELEADS_DOMAIN_PATH = "/domain"
# List Domains documents cursor, and page_size up to 5000. page= stops around
# 11 pages (550 stores at the old page_size of 50). 100-row cursor pages walk
# past that window without a 40-request loop.
STORELEADS_PAGE_SIZE = 100
STORELEADS_CURSOR_PAGES = 15

# ---- ICP (mirrors the coded target + docs/outbound/03) -----------------------
ICP_MIN_YEARLY_SALES_CENTS = 1_000_000_00      # ~$1M/yr floor
ICP_MAX_YEARLY_SALES_CENTS = 20_000_000_00     # ~$20M/yr ceiling (David: "under 20M is who we want")
# Company fit is US only. UK/CA/AU used to pass this set; they no longer do.
ICP_ALLOWED_COUNTRIES = {"US"}
ICP_ALLOWED_PLATFORMS = {"shopify"}

# StoreLeads category text -> our niche label (keyword match, lowercased)
ICP_NICHE_KEYWORDS: dict[str, tuple[str, ...]] = {
    "beauty_wellness": ("beauty", "skincare", "cosmetic", "wellness", "supplement", "personal care"),
    "food_beverage": ("food", "beverage", "drink", "snack", "coffee", "tea"),
    "apparel_accessories": ("apparel", "fashion", "clothing", "footwear", "jewelry", "accessories"),
    "home_lifestyle": ("home", "furniture", "decor", "lifestyle", "kitchen", "bedding"),
    "pets": ("pet", "pets", "animal"),
    "family_gifts": ("baby", "kids", "toys", "gift", "stationery"),
}

# Words that disqualify a store outright (agencies, resellers, etc.)
ICP_EXCLUDE_KEYWORDS = (
    "agency", "consulting", "consultancy", "software", "saas", "wholesale",
    "distributor", "manufacturer", "b2b", "dropship", "drop shipping",
    "print on demand", "print-on-demand", "printful", "printify",
    "etsy seller", "reseller", "marketplace",
)


def _normalize_country(code: str) -> str:
    code = (code or "").strip().upper()
    return "GB" if code == "UK" else code


# A regional storefront (usa.brand.com, us.brand.com) is not the company.
# Its country_code is where that host sells, not where the brand is based.
_STOREFRONT_LABELS = frozenset({
    "us", "usa", "uk", "gb", "ca", "au", "eu", "de", "fr", "es", "it", "nl",
    "nz", "ie", "se", "no", "dk", "be", "ch", "at", "mx", "br", "jp", "kr",
    "en-us", "en-gb", "en-ca", "en-au", "en-nz", "fr-fr", "es-es", "de-de",
})
_ADDRESS_TYPES = frozenset({
    "address", "location", "postal", "postal_address", "street", "street_address",
})
_COUNTRY_NAMES = (
    ("united states of america", "US"),
    ("united states", "US"),
    ("u.s.a.", "US"),
    ("u.s.", "US"),
    ("usa", "US"),
    ("united kingdom", "GB"),
    ("great britain", "GB"),
    ("england", "GB"),
    ("scotland", "GB"),
    ("wales", "GB"),
    ("france", "FR"),
    ("spain", "ES"),
    ("canada", "CA"),
    ("germany", "DE"),
    ("australia", "AU"),
    ("italy", "IT"),
    ("mexico", "MX"),
    ("netherlands", "NL"),
    ("ireland", "IE"),
    ("new zealand", "NZ"),
    ("belgium", "BE"),
    ("switzerland", "CH"),
    ("austria", "AT"),
    ("sweden", "SE"),
    ("norway", "NO"),
    ("denmark", "DK"),
    ("portugal", "PT"),
    ("japan", "JP"),
    ("china", "CN"),
    ("india", "IN"),
    ("brazil", "BR"),
    ("south korea", "KR"),
)
_US_REGIONS = frozenset({
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado",
    "connecticut", "delaware", "florida", "georgia", "hawaii", "idaho",
    "illinois", "indiana", "iowa", "kansas", "kentucky", "louisiana", "maine",
    "maryland", "massachusetts", "michigan", "minnesota", "mississippi",
    "missouri", "montana", "nebraska", "nevada", "new hampshire", "new jersey",
    "new mexico", "new york", "north carolina", "north dakota", "ohio",
    "oklahoma", "oregon", "pennsylvania", "rhode island", "south carolina",
    "south dakota", "tennessee", "texas", "utah", "vermont", "virginia",
    "washington", "west virginia", "wisconsin", "wyoming", "district of columbia",
})
_CA_REGIONS = frozenset({
    "ontario", "quebec", "québec", "british columbia", "alberta", "manitoba",
    "saskatchewan", "nova scotia", "new brunswick", "newfoundland and labrador",
    "prince edward island", "northwest territories", "nunavut", "yukon",
})
_OTHER_REGIONS = {
    "ile-de-france": "FR",
    "île-de-france": "FR",
    "alava": "ES",
    "álava": "ES",
    "araba": "ES",
    "pais vasco": "ES",
    "país vasco": "ES",
    "new south wales": "AU",
}


def _place_phrases() -> tuple[tuple[str, str], ...]:
    """Longest place names first, so 'new mexico' is not read as Mexico."""

    items = [(name, "US") for name in _US_REGIONS]
    items.append(("new england", "US"))
    items.extend((name, "CA") for name in _CA_REGIONS)
    items.extend(_OTHER_REGIONS.items())
    items.sort(key=lambda item: len(item[0]), reverse=True)
    return tuple(items)


_PLACE_PHRASES = _place_phrases()

# Country nouns used as a store name ("Tweezerman Australia"). Adjectives are
# left out so a product word does not become a headquarters.
_STORE_NAME_COUNTRIES = (
    ("new zealand", "NZ"),
    ("australia", "AU"),
    ("sweden", "SE"),
    ("canada", "CA"),
    ("france", "FR"),
    ("germany", "DE"),
    ("united kingdom", "GB"),
    ("britain", "GB"),
    ("ireland", "IE"),
    ("mexico", "MX"),
    ("spain", "ES"),
    ("italy", "IT"),
    ("netherlands", "NL"),
    ("japan", "JP"),
    ("korea", "KR"),
    ("brazil", "BR"),
)
_US_NAME_MASKS = (
    "new mexico", "new england", "new york", "new hampshire", "new jersey",
    "new orleans",
)


def _host_name(value: Any) -> str:
    import outbound_contacts as contacts

    return contacts.normalize_host(str(value or ""))


def _is_storefront_host(host: str) -> bool:
    label = host.split(".", 1)[0] if host else ""
    return label in _STOREFRONT_LABELS


def _record_is_primary(store: dict[str, Any]) -> bool:
    """True when this object is the apex or cluster_best_ranked domain.

    A missing name is the caller's own record, so its country_code still
    counts. usa.brand.com and us.brand.com never do.
    """

    import outbound_contacts as contacts

    host = _host_name(store.get("name"))
    if not host:
        return True
    if _is_storefront_host(host):
        return False
    ranked = _host_name(store.get("cluster_best_ranked"))
    primary = contacts.company_domain(ranked) or ranked or contacts.company_domain(host) or host
    return host == primary or host == ranked


# These phrases contain a country or nation word. They have to win over that
# shorter name, and they are not street names.
_COMPOUND_PLACES = (
    ("new south wales", "AU"),
    ("new mexico", "US"),
    ("new england", "US"),
)


def _country_in_text(value: Any) -> str:
    """A country named in a headquarters location or contact address.

    Compound places are read first, so New Mexico is not Mexico and New
    England is not England. An explicit country then wins over a US state
    that only appears inside a street name. State names count when they are
    their own comma-separated part, as in "Seattle, Washington".
    """

    raw = " ".join(str(value or "").replace("–", " ").replace("—", " ").split()).lower()
    if not raw:
        return ""
    for name, code in _COMPOUND_PLACES:
        if re.search(rf"(?<![a-z]){re.escape(name)}(?![a-z])", raw):
            return code
    for name, code in _COUNTRY_NAMES:
        if re.search(rf"(?<![a-z]){re.escape(name)}(?![a-z])", raw):
            return code
    parts = [part.strip(" .") for part in raw.split(",") if part.strip(" .")]
    for part in reversed(parts):
        if part in _US_REGIONS:
            return "US"
        if part in _CA_REGIONS:
            return "CA"
        if part in _OTHER_REGIONS:
            return _OTHER_REGIONS[part]
    last = parts[-1] if parts else raw
    if last in {"us", "usa", "u.s.", "u.s.a."}:
        return "US"
    if last in {"uk", "gb"}:
        return "GB"
    if last in {"fr", "es"}:
        return last.upper()
    return ""


def _contact_addresses(store: dict[str, Any]) -> list[str]:
    found: list[str] = []
    for key in ("address", "contact_address"):
        text = str(store.get(key) or "").strip()
        if text:
            found.append(text)
    for item in store.get("contact_info") or []:
        if not isinstance(item, dict):
            continue
        kind = str(item.get("type") or "").strip().lower()
        if kind not in _ADDRESS_TYPES:
            continue
        text = str(item.get("value") or item.get("address") or "").strip()
        if text:
            found.append(text)
    return found


def _delaware_registration(text: str) -> bool:
    """True when the only US place in the text is Delaware.

    A registered-agent address is not the brand's headquarters.
    """

    raw = " ".join(str(text or "").replace("–", " ").replace("—", " ").split()).lower()
    if not raw or "new england" in raw or "new mexico" in raw:
        return False
    parts = [part.strip(" .") for part in raw.split(",") if part.strip(" .")]
    has_delaware = any(part in {"delaware", "de"} for part in parts) or bool(
        re.search(r"(?<![a-z])delaware(?![a-z])", raw)
    )
    if not has_delaware:
        return False
    for part in parts:
        if part in _US_REGIONS and part != "delaware":
            return False
        if part in _CA_REGIONS or part in _OTHER_REGIONS:
            return False
    return True


def _location_is_delaware_only(store: dict[str, Any]) -> bool:
    texts = [str(store.get(key) or "") for key in (
        "location", "city", "administrative_area_level_1", "address", "contact_address",
    )]
    texts.extend(_contact_addresses(store))
    saw_delaware = False
    for text in texts:
        if not str(text or "").strip():
            continue
        if _delaware_registration(text):
            saw_delaware = True
            continue
        if _country_in_text(text) == "US":
            return False
    return saw_delaware


def _foreign_store_name(store: dict[str, Any]) -> str:
    """Country implied by a store name such as 'Tweezerman Australia'."""

    text = f"{store.get('merchant_name') or ''} {store.get('title') or ''}".lower()
    text = " ".join(text.replace("–", " ").replace("—", " ").split())
    if re.search(r"(?<![a-z])new zealand(?![a-z])", text):
        return "NZ"
    for phrase in _US_NAME_MASKS:
        text = text.replace(phrase, " ")
    for label, code in _STORE_NAME_COUNTRIES:
        if label == "new zealand":
            continue
        if re.search(rf"(?<![a-z]){re.escape(label)}(?![a-z])", text):
            return code
    return ""


def headquarters_verdict(store: dict[str, Any]) -> tuple[str, str]:
    """(country code, reason). Reason is empty only when the code is US.

    A real location wins. 'New Mexico' and 'New England' are US places.
    A Delaware registration address is not a headquarters. A US
    country_code does not outvote a foreign country-code domain or a
    clearly foreign store name. No evidence means unknown.
    """

    evidence: list[tuple[str, str]] = []
    for source, text in (
        ("location", store.get("location")),
        ("location", store.get("city")),
        ("location", store.get("administrative_area_level_1")),
    ):
        if _delaware_registration(str(text or "")):
            continue
        code = _country_in_text(text)
        if code:
            evidence.append((code, source))
    city = str(store.get("city") or "").strip()
    region = str(store.get("administrative_area_level_1") or "").strip()
    if city and region:
        combined = f"{city}, {region}"
        if not _delaware_registration(combined):
            code = _country_in_text(combined)
            if code:
                evidence.append((code, "location"))
    for address in _contact_addresses(store):
        if _delaware_registration(address):
            continue
        code = _country_in_text(address)
        if code:
            evidence.append((code, "contact address"))
    foreign = [(code, source) for code, source in evidence if code != "US"]
    if foreign:
        code, source = foreign[0]
        return code, f"headquarters country is {code} from {source}"
    if any(code == "US" for code, _source in evidence):
        return "US", ""

    host = _primary_host(store)
    tld_country = _cctld_country(host)
    suffix = host.split(".", 1)[-1] if "." in host else host
    name_country = _foreign_store_name(store)
    if _record_is_primary(store):
        code = _normalize_country(str(store.get("country_code") or ""))
        if code == "US" and tld_country and tld_country != "US":
            return tld_country, f"headquarters country is {tld_country} from .{suffix} domain"
        if code == "US" and name_country:
            return name_country, f"headquarters country is {name_country} from store name"
        if code == "US" and not _location_is_delaware_only(store):
            return "US", ""
        if code and code != "US":
            return code, f"headquarters country is {code} from primary domain country_code"
    if name_country:
        return name_country, f"headquarters country is {name_country} from store name"
    return "", "headquarters country is unknown"


# Generic and US suffixes are not evidence of a foreign brand. .co is used
# by US companies, so it is not treated as Colombia.
_GENERIC_SUFFIXES = frozenset({
    "com", "net", "org", "io", "co", "shop", "store", "xyz", "online", "site",
    "app", "us", "biz", "info",
})
_CCTLD_COUNTRIES = {
    "uk": "GB", "co.uk": "GB", "de": "DE", "ca": "CA", "fr": "FR", "es": "ES",
    "com.au": "AU", "au": "AU", "it": "IT", "nl": "NL", "ie": "IE",
    "co.nz": "NZ", "nz": "NZ", "se": "SE", "no": "NO", "dk": "DK", "be": "BE",
    "ch": "CH", "at": "AT", "pt": "PT", "jp": "JP", "mx": "MX", "br": "BR",
    "pl": "PL", "fi": "FI", "kr": "KR", "co.jp": "JP", "com.br": "BR",
    "com.mx": "MX", "co.za": "ZA",
}
# Calling codes that are not the North American +1 plan. +1 is US and Canada,
# so it is not a clear non-US phone.
_CALLING_CODES = (
    ("+353", "IE"), ("+351", "PT"), ("+61", "AU"), ("+64", "NZ"), ("+49", "DE"),
    ("+44", "GB"), ("+43", "AT"), ("+41", "CH"), ("+39", "IT"), ("+34", "ES"),
    ("+33", "FR"), ("+32", "BE"), ("+31", "NL"), ("+46", "SE"), ("+47", "NO"),
    ("+45", "DK"), ("+81", "JP"), ("+52", "MX"), ("+55", "BR"), ("+86", "CN"),
    ("+91", "IN"), ("+82", "KR"), ("+48", "PL"), ("+358", "FI"), ("+27", "ZA"),
)


def _primary_host(store: dict[str, Any]) -> str:
    import outbound_contacts as contacts

    ranked = _host_name(store.get("cluster_best_ranked"))
    host = _host_name(store.get("name"))
    if ranked:
        return contacts.company_domain(ranked) or ranked
    if host and not _is_storefront_host(host):
        return host
    return contacts.company_domain(host) or host


def _cctld_country(host: str) -> str:
    """Country of a country-code suffix, or '' for .com, .us, .io, and .co."""

    labels = [part for part in host.split(".") if part]
    if len(labels) < 2:
        return ""
    for size in (2, 1):
        if len(labels) <= size:
            continue
        suffix = ".".join(labels[-size:])
        if suffix in _GENERIC_SUFFIXES:
            return ""
        if suffix in _CCTLD_COUNTRIES:
            return _CCTLD_COUNTRIES[suffix]
    return ""


def _primary_currency(store: dict[str, Any]) -> str:
    for key in ("currency_code", "currency", "price_currency"):
        raw = str(store.get(key) or "").strip().upper().replace("US$", "USD")
        if raw in {"USD", "$"}:
            return "USD"
        if raw:
            return raw
    return ""


def _phone_countries(store: dict[str, Any]) -> list[str]:
    found: list[str] = []
    for item in store.get("contact_info") or []:
        if not isinstance(item, dict):
            continue
        if str(item.get("type") or "").strip().lower() != "phone":
            continue
        raw = str(item.get("value") or "")
        digits = re.sub(r"[^\d+]", "", raw)
        if digits.startswith("00"):
            digits = "+" + digits[2:]
        if not digits.startswith("+") or digits.startswith("+1"):
            continue
        for prefix, code in _CALLING_CODES:
            if digits.startswith(prefix):
                found.append(code)
                break
    return found


def _locale_country(store: dict[str, Any]) -> str:
    raw = str(store.get("language_code") or store.get("locale") or "").strip().lower().replace("_", "-")
    if "-" not in raw:
        return ""
    region = raw.rsplit("-", 1)[-1]
    if len(region) != 2 or not region.isalpha() or region == "us":
        return ""
    return "GB" if region == "uk" else region.upper()


def _ship_countries(store: dict[str, Any]) -> list[str]:
    raw = store.get("ships_to_countries") or []
    if isinstance(raw, str):
        raw = [part.strip() for part in raw.split(",") if part.strip()]
    found: list[str] = []
    for item in raw:
        code = _normalize_country(str(item))
        if code and code not in found:
            found.append(code)
    return found


def _clear_non_us_reason(store: dict[str, Any]) -> str:
    """Why an unknown headquarters is still clearly not a US brand.

    A non-USD primary currency is enough. A country-code domain excludes
    when a non-US phone, a matching locale, or the primary domain's
    non-US shipping list agrees with it. USD, a bare TLD, a bare phone,
    or no signals at all stay in.
    """

    currency = _primary_currency(store)
    if currency and currency != "USD":
        return f"primary currency is {currency}"
    host = _primary_host(store)
    tld_country = _cctld_country(host)
    suffix = host.split(".", 1)[-1] if "." in host else host
    phones = [code for code in _phone_countries(store) if code and code != "US"]
    if tld_country and tld_country != "US" and phones:
        return f".{suffix} domain and phone country {phones[0]}"
    locale = _locale_country(store)
    if tld_country and locale and tld_country == locale:
        return f".{suffix} domain and language {locale}"
    if _record_is_primary(store) and tld_country and tld_country != "US":
        shipped = _ship_countries(store)
        if shipped and "US" not in shipped and tld_country in shipped:
            return f".{suffix} domain and ships to {tld_country}"
    if tld_country and tld_country != "US":
        return f".{suffix} domain is a {tld_country} storefront"
    named = _foreign_store_name(store)
    if named:
        return f"store name reads as {named}"
    return ""


def country_eligibility(store: dict[str, Any]) -> tuple[bool, str]:
    """Whether the country gate allows this store, and the log reason.

    A known non-US headquarters is excluded. A missing headquarters falls
    back to currency, language, phone, the country TLD, and the primary
    domain's shipping countries. The brand is kept when those are absent
    or ambiguous. The kept reason is logged by the caller.
    """

    code, reason = headquarters_verdict(store)
    if code == "US":
        return True, ""
    if code:
        return False, reason
    clear = _clear_non_us_reason(store)
    if clear:
        return False, f"headquarters country is unknown; {clear}"
    return True, "headquarters country is unknown; kept"


def eligibility_country_by_domain(
    rows: dict[str, dict[str, Any]],
) -> dict[str, tuple[str, str]]:
    """One headquarters verdict per company, keyed by the apex domain.

    A storefront row cannot outvote the primary domain. A known non-US
    headquarters beats a US storefront flag. When every row lacks a
    headquarters, currency, phone, and the country TLD decide. An
    ambiguous or empty fallback is kept, and the reason says so.
    """

    import outbound_contacts as contacts

    grouped: dict[str, list[dict[str, Any]]] = {}
    for name, row in rows.items():
        if not isinstance(row, dict):
            continue
        host = str(row.get("name") or name or "")
        key = contacts.company_domain(host) or _host_name(host)
        if not key:
            continue
        grouped.setdefault(key, []).append(row)
    verdicts: dict[str, tuple[str, str]] = {}
    for key, group in grouped.items():
        decisions = [(headquarters_verdict(row), country_eligibility(row)) for row in group]
        foreign = [hq for hq, _choice in decisions if hq[0] and hq[0] != "US"]
        if foreign:
            verdicts[key] = foreign[0]
            continue
        denied = [choice for _hq, choice in decisions if not choice[0]]
        if denied:
            verdicts[key] = ("", denied[0][1])
            continue
        domestic = [hq for hq, _choice in decisions if hq[0] == "US"]
        if domestic:
            verdicts[key] = domestic[0]
            continue
        verdicts[key] = ("", "headquarters country is unknown; kept")
    return verdicts


# Short words that must be whole words. "app" is not "apparel". "event" is not
# "prevent". Fitness apparel stays; a fitness program does not.
_COMPANY_FIT_EXCLUDE = re.compile(
    r"\b(?:apps?|services?|events?)\b"
    r"|\bmulti[\s-]?brand\b"
    r"|\bfitness programs?\b|\bfitness class(?:es)?\b"
    r"|\bworkout programs?\b|\bpersonal training\b",
    re.I,
)


def _yearly_cents(store: dict[str, Any]) -> Optional[float]:
    """StoreLeads yearly sales in cents, or None when the figure is missing."""

    yearly = store.get("estimated_sales_yearly")
    if isinstance(yearly, bool) or yearly is None:
        return None
    if isinstance(yearly, (int, float)):
        return float(yearly)
    if isinstance(yearly, str):
        raw = yearly.strip().replace(",", "")
        if not raw:
            return None
        try:
            return float(raw)
        except ValueError:
            return None
    return None


def _company_text(store: dict[str, Any]) -> str:
    parts = (
        store.get("name") or "",
        store.get("merchant_name") or "",
        store.get("categories") or "",
        store.get("tags") or "",
        store.get("description") or "",
        store.get("merchant_description") or "",
    )
    from outbound_personalization import as_text

    return " ".join(as_text(part) for part in parts).lower()


def passes_company_fit(store: dict[str, Any], *, require_niche: bool = True) -> bool:
    """US own-brand physical-product companies with revenue on file.

    Baseline pulls, signal pulls, campaign sourcing, and the sales-org
    segment all use this gate. The country that counts is the headquarters
    on the primary domain. A US storefront host, a shipping country, or a
    cluster sibling does not make a foreign brand eligible. A known non-US
    headquarters is excluded. A missing headquarters is kept unless the
    store currency is not USD, or a country-code domain and a non-US phone
    agree. The keep or the exclusion is logged. A missing or zero
    sales figure is not revenue data. Distributors, multi-brand resellers,
    services, events, apps, and fitness programs are out. The niche list
    is the physical product test. The sales-org segment passes
    require_niche=False because any US brand qualifies there.
    """

    allowed, reason = country_eligibility(store)
    label = store.get("name") or store.get("merchant_name") or "unknown domain"
    if not allowed:
        logger.info("[outbound] excluding %s: %s", label, reason)
        return False
    if reason:
        logger.info("[outbound] %s: %s", label, reason)
    # A brands page means the site sells other companies' brands. That is a
    # retailer, not a brand we want in these campaigns.
    if str(store.get("brands_page") or "").strip():
        return False
    yearly = _yearly_cents(store)
    if yearly is None or yearly <= 0:
        return False
    if require_niche and not store_niche(store):
        return False
    text = _company_text(store)
    if any(bad in text for bad in ICP_EXCLUDE_KEYWORDS):
        return False
    if _COMPANY_FIT_EXCLUDE.search(text):
        return False
    return True


def store_niche(store: dict[str, Any]) -> str:
    """Return the ICP niche label for a store, or '' if it fits none."""
    text = " ".join(
        str(part) for part in (
            store.get("categories") or "",
            store.get("tags") or "",
            store.get("merchant_name") or "",
            store.get("name") or "",
        )
    ).lower()
    for niche, keywords in ICP_NICHE_KEYWORDS.items():
        if any(kw in text for kw in keywords):
            return niche
    return ""


def store_matches_icp(store: dict[str, Any], settings: Optional[dict[str, Any]] = None) -> bool:
    """Authoritative ICP gate applied to a StoreLeads store record.

    The revenue band is tunable on the Lead Ops page; settings are in dollars a
    year, while StoreLeads reports cents, so the conversion happens here.
    ``passes_company_fit`` is the shared US / own-brand / physical-product /
    known-revenue screen used by baseline, signal, and campaign sourcing.
    """
    lo, hi = ICP_MIN_YEARLY_SALES_CENTS, ICP_MAX_YEARLY_SALES_CENTS
    if settings:
        try:
            lo = int(settings.get("icp.revenue_min_usd") or lo // 100) * 100
            hi = int(settings.get("icp.revenue_max_usd") or hi // 100) * 100
        except (TypeError, ValueError):
            pass
    domain = str(store.get("name") or "").strip().lower()
    if not domain:
        return False
    if not passes_company_fit(store):
        return False

    platform = str(store.get("platform") or "").strip().lower()
    if ICP_ALLOWED_PLATFORMS and platform and platform not in ICP_ALLOWED_PLATFORMS:
        return False

    yearly = _yearly_cents(store)
    if yearly is not None and yearly > 0:
        if yearly < lo or yearly > hi:
            return False

    # Headcount cross-check. StoreLeads call their sales figures directional
    # only, so a low revenue estimate must not be the sole thing keeping a
    # 500-person company out of the list.
    emp_lo, emp_hi = 2, 80
    if settings:
        try:
            emp_lo = int(settings.get("icp.employees_min") or emp_lo)
            emp_hi = int(settings.get("icp.employees_max") or emp_hi)
        except (TypeError, ValueError):
            pass
    employees = store.get("employee_count")
    if isinstance(employees, (int, float)) and employees > 0:
        if employees < emp_lo or employees > emp_hi:
            return False

    searchable = " ".join(
        str(part) for part in (
            domain,
            store.get("merchant_name") or "",
            store.get("categories") or "",
            store.get("tags") or "",
        )
    ).lower()
    if any(bad in searchable for bad in ICP_EXCLUDE_KEYWORDS):
        return False

    # Must land in one of the six niches, and must have a contact email listed
    # (StoreLeads has no emails itself, but a listed email means a live domain
    # Clay can work from).
    if not store_niche(store):
        return False
    if not _has_email(store):
        return False

    return True


def _has_email(store: dict[str, Any]) -> bool:
    for item in store.get("contact_info") or []:
        if isinstance(item, dict) and str(item.get("type", "")).lower() == "email":
            if str(item.get("value", "")).strip():
                return True
    return False


# ---- signal scoring (docs/outbound/08) ---------------------------------------
# Each brand gets a fit score from weighted signals, a Tier (A/B/C), and a
# human "why now" reason that also seeds Clay's personalization. Pure function of
# the store record so it is fully unit-testable without a live key.

ICP_SCORE_RECENT_DAYS = 45          # "installed recently" window for the trigger
ICP_SCORE_PLAN_CHANGE_DAYS = 180    # "upgraded plan recently" window
ICP_TIER_A_MIN = 8
ICP_TIER_B_MIN = 4

# Ad pixels that prove live paid spend (StoreLeads technology names, lowercased).
_AD_PIXEL_TECHS = (
    "facebook pixel", "google ads pixel", "tiktok pixel", "pinterest pixel",
    "bing ads", "criteo", "snap pixel",
)
# CRO / testing / analytics apps: brand already cares about conversion.
_CRO_APP_HINTS = (
    "intelligems", "rebuy", "triple whale", "triplewhale", "polar", "lucky orange",
    "hotjar", "clarity", "kno", "octane", "boost ai", "boost commerce",
)
# App-store categories that mean "acquisition budget" or "conversion focus".
_ADS_APP_CATS = ("ads", "affiliate")
_CRO_APP_CATS = (
    "analytics", "site optimization", "pricing optimization", "conversion",
    "upsell", "search and filters", "surveys",
)
# Enterprise analytics that signal an existing agency / in-house team (deprioritize).
_ENTERPRISE_TECH = ("monetate", "ometria", "bloomreach", "dynamic yield")


def _parse_sl_date(value: Any) -> Optional[datetime]:
    """Parse a StoreLeads date ('2026-07-07T00:00:00' or '...Z') to aware UTC."""
    if not value:
        return None
    raw = str(value).strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt


def _tech_names(store: dict[str, Any]) -> set[str]:
    out: set[str] = set()
    for t in store.get("technologies") or []:
        if isinstance(t, dict) and t.get("name"):
            out.add(str(t["name"]).strip().lower())
    return out


def _app_records(store: dict[str, Any]) -> list[dict[str, Any]]:
    return [a for a in (store.get("apps") or []) if isinstance(a, dict)]


def _app_text(app: dict[str, Any]) -> str:
    cats = app.get("categories") or []
    return " ".join([str(app.get("name") or ""), str(app.get("token") or ""),
                     " ".join(str(c) for c in cats)]).lower()


def _has_ad_pixels(store: dict[str, Any]) -> int:
    techs = _tech_names(store)
    return sum(1 for p in _AD_PIXEL_TECHS if any(p in t for t in techs))


def _has_cro_app(store: dict[str, Any]) -> bool:
    for app in _app_records(store):
        text = _app_text(app)
        if any(h in text for h in _CRO_APP_HINTS):
            return True
        cats = " ".join(str(c) for c in (app.get("categories") or [])).lower()
        if any(c in cats for c in _CRO_APP_CATS):
            return True
    return False


def _has_ads_or_affiliate_app(store: dict[str, Any]) -> bool:
    for app in _app_records(store):
        cats = " ".join(str(c) for c in (app.get("categories") or [])).lower()
        if any(c in cats for c in _ADS_APP_CATS):
            return True
    return False


def _recent_growth_app_install(store: dict[str, Any], now: datetime) -> bool:
    """Any ads / analytics / CRO app installed within the recent window = a fresh
    project and moving budget: the strongest 'why now' trigger."""
    for app in _app_records(store):
        installed = _parse_sl_date(app.get("installed_at"))
        if not installed:
            continue
        if (now - installed).days > ICP_SCORE_RECENT_DAYS or (now - installed).days < 0:
            continue
        cats = " ".join(str(c) for c in (app.get("categories") or [])).lower()
        text = _app_text(app)
        if any(c in cats for c in (*_ADS_APP_CATS, *_CRO_APP_CATS)) or any(h in text for h in _CRO_APP_HINTS):
            return True
    return False


def _recent_plan_upgrade(store: dict[str, Any], now: datetime) -> bool:
    changed = _parse_sl_date(store.get("last_plan_change_at"))
    if not changed or (now - changed).days > ICP_SCORE_PLAN_CHANGE_DAYS or (now - changed).days < 0:
        return False
    return "plus" in str(store.get("plan") or "").lower()


def _healthy_app_spend(store: dict[str, Any]) -> bool:
    spend = store.get("monthly_app_spend")
    return isinstance(spend, (int, float)) and 300_00 <= spend <= 3_000_00


def _has_trending_tag(store: dict[str, Any]) -> bool:
    tags = " ".join(str(t) for t in (store.get("tags") or [])).lower()
    return "trending" in tags


def _is_public_company(store: dict[str, Any]) -> bool:
    feats = " ".join(str(f) for f in (store.get("features") or [])).lower()
    return "public company" in feats


def _has_enterprise_stack(store: dict[str, Any]) -> bool:
    techs = _tech_names(store)
    return any(e in t for e in _ENTERPRISE_TECH for t in techs)


def score_store(store: dict[str, Any], *, now: Optional[datetime] = None) -> dict[str, Any]:
    """Return {score, tier, reason, signals[], excluded} for a store.

    Ordered so the most compelling 'why now' trigger becomes the reason/opener.
    """
    now = now or datetime.now(timezone.utc)
    score = 0
    signals: list[str] = []

    # Timing triggers first (they make the best opener).
    if _recent_growth_app_install(store, now):
        score += 3
        signals.append("Added a growth or CRO app in the last 45 days")
    if _recent_plan_upgrade(store, now):
        score += 2
        signals.append("Upgraded their store plan recently")
    if _has_trending_tag(store):
        score += 1
        signals.append("Trending on social right now")

    # Spend-is-happening (there is waste to audit).
    pixels = _has_ad_pixels(store)
    if pixels >= 2:
        score += 3
        signals.append("Runs Meta and Google ads")
    if pixels >= 3:
        score += 2
        signals.append("Advertises across three or more channels")
    if _has_ads_or_affiliate_app(store):
        score += 2
        signals.append("Has ad or affiliate apps installed")
    if _healthy_app_spend(store):
        score += 1
        signals.append("Invests in a healthy growth app stack")

    # Believes in CVR (easy conversation).
    if _has_cro_app(store):
        score += 3
        signals.append("Already uses conversion or testing tools")

    # Anti-signals.
    excluded = _is_public_company(store)
    if _has_enterprise_stack(store):
        score -= 2
        signals.append("Runs an enterprise analytics stack")
    if pixels == 0:
        score -= 2
        signals.append("No ad pixel found")

    if excluded:
        tier = "X"
    elif score >= ICP_TIER_A_MIN:
        tier = "A"
    elif score >= ICP_TIER_B_MIN:
        tier = "B"
    else:
        tier = "C"

    positive = [s for s in signals if s not in
                ("Runs an enterprise analytics stack", "No ad pixel found")]
    reason = positive[0] if positive else "Fits our ICP, thin buying signals"
    return {"score": score, "tier": tier, "reason": reason, "signals": signals, "excluded": excluded}


def _amazon_unknown_sellers(findings: Any) -> int:
    """Total unknown third-party sellers across the matched listings."""
    if not isinstance(findings, dict):
        return 0
    total = 0
    for listing in findings.get("listings") or []:
        if not isinstance(listing, dict):
            continue
        try:
            total += int(listing.get("sellers_unknown") or 0)
        except (TypeError, ValueError):
            continue
    return total


def _named_list(value: Any, *, limit: int = 30) -> list[str]:
    found: list[str] = []
    items = value if isinstance(value, list) else []
    for item in items:
        if isinstance(item, str):
            name = item.strip()
        elif isinstance(item, dict):
            name = str(item.get("name") or item.get("token") or "").strip()
        else:
            name = ""
        if name and name not in found:
            found.append(name)
        if len(found) >= limit:
            break
    return found


def _social_contacts(value: Any) -> list[dict[str, Any]]:
    """Follower counts only. Email addresses in contact_info are not copied."""

    kept: list[dict[str, Any]] = []
    for item in value if isinstance(value, list) else []:
        if not isinstance(item, dict):
            continue
        kind = str(item.get("type") or "").strip().lower()
        if kind not in {"tiktok", "youtube", "instagram", "pinterest", "twitter"}:
            continue
        kept.append({
            "type": kind,
            "followers": item.get("followers"),
            "followers_30d": item.get("followers_30d"),
            "followers_90d": item.get("followers_90d"),
            "description": " ".join(str(item.get("description") or "").split())[:240],
        })
    return kept[:8]


def _review_blob(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    return {
        "avg_rating": value.get("avg_rating"),
        "review_count": value.get("review_count"),
    }


def store_profile(store: dict[str, Any]) -> dict[str, Any]:
    """Compact StoreLeads facts for a later opener. Not a sentence."""

    product = store.get("most_recently_published_product")
    if not isinstance(product, dict):
        product = {}
    trust = store.get("trustpilot")
    if not isinstance(trust, dict):
        trust = {}
    description = " ".join(str(store.get("description") or "").split())[:300]
    return {
        "country_code": headquarters_verdict(store)[0],
        "sales_channels": _named_list(store.get("sales_channels"), limit=8),
        "apps": _named_list(store.get("apps")),
        "technologies": _named_list(store.get("technologies")),
        "contact_info": _social_contacts(store.get("contact_info")),
        "plan": str(store.get("plan") or "").strip(),
        "last_platform": str(store.get("last_platform") or "").strip(),
        "last_platform_change_at": str(store.get("last_platform_change_at") or "").strip(),
        "product_count": store.get("product_count"),
        "avg_price_usd": store.get("avg_price_usd"),
        "max_product_published_at": str(store.get("max_product_published_at") or "").strip(),
        "most_recently_published_product": {
            "title": str(product.get("title") or product.get("name") or "").strip(),
            "published_at": str(product.get("published_at") or "").strip(),
        },
        "description": description,
        "retailer_page": str(store.get("retailer_page") or "").strip(),
        "store_locator_page": str(store.get("store_locator_page") or "").strip(),
        "brands_page": str(store.get("brands_page") or "").strip(),
        "trustpilot": {
            "avg_rating": trust.get("avg_rating"),
            "review_count": trust.get("review_count"),
        },
        "yotpo": _review_blob(store.get("yotpo")),
        "okendo": _review_blob(store.get("okendo")),
        "judgeme": _review_blob(store.get("judgeme") or store.get("judge_me")),
        "loox": _review_blob(store.get("loox")),
        "title": " ".join(str(store.get("title") or "").split())[:200],
        "merchant_name": str(store.get("merchant_name") or "").strip(),
        "features": _named_list(store.get("features"), limit=12),
        "sales_channel_urls": _named_list(store.get("sales_channel_urls"), limit=8),
        "cluster_domains": _named_list(store.get("cluster_domains"), limit=8),
        "locs": _named_list(store.get("locs"), limit=8),
        "ships_to_countries": _named_list(store.get("ships_to_countries"), limit=12),
        "vendor_count": store.get("vendor_count"),
        "city": str(store.get("city") or "").strip(),
        "estimated_visits": store.get("estimated_visits"),
        "product_images_created_90": store.get("product_images_created_90"),
    }


def to_clay_lead(store: dict[str, Any], *, now: Optional[datetime] = None,
                 amazon: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    """Shape a matched store into the payload Clay's table expects, with the
    fit score, Tier, and 'why now' reason attached for ranking + personalization.

    amazon is the result of outbound_amazon.brand_control(). A brand losing
    control of its own Amazon listings beats any StoreLeads signal as an opener,
    so when that check produced a reason it takes over the reason line. When the
    check was skipped, switched off, or never run, the StoreLeads reason stands.
    """
    scored = score_store(store, now=now)
    signals = list(scored["signals"])
    import outbound_contacts as _company
    import outbound_instantly_push as _push
    for token in _push.route_tokens_for_store(store, now=now):
        if token not in signals:
            signals.append(token)
    lead = {
        "domain": _company.company_domain(str(store.get("name") or "")),
        "brand": str(store.get("merchant_name") or store.get("name") or "").strip(),
        "niche": store_niche(store),
        "country": headquarters_verdict(store)[0],
        "tier": scored["tier"],
        "score": scored["score"],
        "reason": scored["reason"],
        "signals": signals,
        "estimated_sales_yearly_cents": store.get("estimated_sales_yearly"),
        "categories": store.get("categories"),
        "apps": store.get("apps"),
        "store_profile": store_profile(store),
    }
    apply_amazon(lead, amazon)
    return lead


def apply_amazon(lead: dict[str, Any], amazon: Optional[dict[str, Any]]) -> dict[str, Any]:
    """Fold an Amazon brand-control result onto a lead, in place.

    Applied LAST in the pull, after the recipe has had its say, because a real
    finding about a brand's own listings beats a guess made from a store plan
    change. Getting this order wrong silently reverts every lead to the old
    reason and the Amazon work never reaches the email.
    """
    if not isinstance(amazon, dict):
        return lead
    findings = amazon.get("findings")
    lead["amazon_confidence"] = str(amazon.get("confidence") or "none")
    lead["amazon_marketplace"] = str(amazon.get("marketplace") or "")
    lead["amazon_checked_at"] = str(amazon.get("checked_at") or "")
    lead["amazon_absent"] = bool(findings.get("absent")) if isinstance(findings, dict) else False
    lead["amazon_sellers_unknown"] = _amazon_unknown_sellers(findings)
    lead["amazon_skipped_reason"] = str(amazon.get("skipped_reason") or "")
    amazon_reason = str(amazon.get("reason") or "").strip()
    if amazon_reason:
        lead["reason"] = amazon_reason
        lead["signals"] = [amazon_reason] + [
            s for s in lead.get("signals") or [] if s != amazon_reason
        ]

    # The bucketed facts Clay writes the real sentence from. Our reason above is
    # only the fallback for when Clay's column has not run.
    try:
        import json as _json

        from outbound_amazon import clay_facts
        facts = clay_facts(amazon)
        lead.update(facts)
        # Also as one blob, because this lead gets stored and re-read later and
        # the facts have to survive that round trip to reach Clay at all.
        lead["amazon_facts"] = _json.dumps(facts)
    except Exception:  # noqa: BLE001 — a missing fact must not lose the lead
        logger.warning("[outbound] could not build Clay facts for %s", lead.get("domain"))
    return lead


# ---- thin network layer (injectable for tests) -------------------------------

def _retry_after_seconds(resp: "requests.Response") -> Optional[float]:
    raw = resp.headers.get("Retry-After")
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


# Ask for the whole apps and contact_info objects. Naming a nested field
# (apps.removed_at) can limit the object to that field and drop the categories
# the scorer reads. last_platform is separate and is what the replatform recipe
# is now filtering on.
# most_recently_published_product is omitted from the default domain payload
# and must be named here. The other names are the List Domains attributes at
# https://storeleads.app/api (product_count, avg_price_usd, city, and so on).
_STORELEADS_FIELDS = ",".join((
    "name", "merchant_name", "platform", "country_code",
    "estimated_sales_yearly", "categories", "tags", "contact_info", "apps",
    # Richer fields the signal scorer reads (docs/outbound/08).
    "technologies", "monthly_app_spend", "last_plan_change_at", "last_platform",
    "last_platform_change_at", "plan", "created_at", "features", "employee_count",
    "sales_channels",
    # Same List Domains call. No extra request. Used for personalization and
    # to drop retailers (brands_page).
    "product_count", "avg_price_usd", "max_product_published_at",
    "most_recently_published_product", "description", "title",
    "retailer_page", "store_locator_page", "brands_page", "trustpilot",
    "city", "estimated_visits", "product_images_created_90",
    # Headquarters, not a regional host. ships_to_countries and
    # cluster_domains are requested so they are on the record, and they
    # are never used to decide that a brand is US.
    "location", "administrative_area_level_1", "cluster_best_ranked",
    "cluster_domains", "ships_to_countries", "currency_code", "language_code",
    "vendor_count", "locs", "sales_channel_urls",
    "yotpo", "okendo", "judgeme", "loox",
))
STORELEADS_BULK_URL = f"{STORELEADS_BASE_URL}{STORELEADS_DOMAIN_PATH}/bulk"


@dataclass
class StoreLeadsPage:
    """One List Domains response. next_cursor is empty at the end of the results."""

    domains: list[dict[str, Any]] = field(default_factory=list)
    next_cursor: Optional[str] = None
    status: Any = ""
    error: str = ""


def build_storeleads_query(
    *,
    page: int = 0,
    page_size: int = STORELEADS_PAGE_SIZE,
    cursor: Optional[str] = None,
    extra_params: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Query string for List Domains.

    Cursor is the documented paginator. page is only sent for a legacy offset
    above 0, which StoreLeads stops honoring after about 11 pages. The first
    request omits both and reads next_cursor from the response.
    """
    params: dict[str, Any] = {
        "f:p": "shopify",
        "page_size": page_size,
        "sort": "-rank",
        "fields": _STORELEADS_FIELDS,
    }
    if cursor:
        params["cursor"] = cursor
    elif page:
        params["page"] = page
    if extra_params:
        # Recipe filters win over the defaults (e.g. a recipe's own sort order).
        params.update({
            key: value for key, value in extra_params.items()
            if value is not None and not str(key).startswith("_")
        })
    # Every list call is US only. A stored campaign row can omit f:cc, and an
    # expanded recipe can set it to None, which used to drop the filter. The
    # client gate still rejects a non-US row that slips through.
    params["f:cc"] = "US"
    params["f:cc:op"] = "or"
    return params


def _domains_from_payload(payload: Any) -> StoreLeadsPage:
    if isinstance(payload, dict):
        domains = payload.get("domains") or payload.get("results") or []
        raw_cursor = payload.get("next_cursor") or ""
        cursor = str(raw_cursor).strip() or None
        return StoreLeadsPage(list(domains) if isinstance(domains, list) else [], cursor)
    if isinstance(payload, list):
        return StoreLeadsPage(payload, None)
    return StoreLeadsPage()


def fetch_storeleads_result(
    api_key: str,
    *,
    page: int = 0,
    page_size: int = STORELEADS_PAGE_SIZE,
    timeout: int = 30,
    max_retries: int = 4,
    cursor: Optional[str] = None,
    extra_params: Optional[dict[str, Any]] = None,
) -> StoreLeadsPage:
    """Fetch one List Domains page, including the cursor for the next page.

    extra_params carries a recipe's server-side filters (see outbound_recipes).
    The authoritative ICP filtering still happens client-side in store_matches_icp,
    so a StoreLeads filter quirk can never widen our target.

    Retries politely on 429 (rate limit), honoring Retry-After when present, with
    exponential backoff otherwise.
    """
    params = build_storeleads_query(
        page=page, page_size=page_size, cursor=cursor, extra_params=extra_params,
    )
    for attempt in range(max_retries + 1):
        resp = requests.get(
            f"{STORELEADS_BASE_URL}{STORELEADS_DOMAIN_PATH}",
            headers={"Authorization": f"Bearer {api_key}"},
            params=params,
            timeout=timeout,
        )
        if resp.status_code == 429 and attempt < max_retries:
            wait = _retry_after_seconds(resp) or (2 ** attempt)
            logger.warning("[outbound] StoreLeads rate-limited; waiting %ss", wait)
            time.sleep(min(wait, 30))
            continue
        resp.raise_for_status()
        page = _domains_from_payload(resp.json())
        page.status = resp.status_code
        return page
    return StoreLeadsPage()


def fetch_storeleads_page(
    api_key: str,
    *,
    page: int = 0,
    page_size: int = 50,
    timeout: int = 30,
    max_retries: int = 4,
    extra_params: Optional[dict[str, Any]] = None,
    cursor: Optional[str] = None,
) -> list[dict[str, Any]]:
    """Fetch one page of stores. Prefer fetch_storeleads_result for the cursor."""
    return fetch_storeleads_result(
        api_key, page=page, page_size=page_size, timeout=timeout,
        max_retries=max_retries, cursor=cursor, extra_params=extra_params,
    ).domains


def lookup_domains(
    api_key: str,
    domains: Iterable[str],
    *,
    post: Optional[Callable[..., Any]] = None,
    timeout: int = 30,
) -> dict[str, dict[str, Any]]:
    """Bulk Retrieve Domains, up to 100 names per call.

    Returns a map of the requested domain to the StoreLeads object. A missing
    key or a failed call returns what was already collected and does not raise.
    This does not write to Instantly.
    """

    names: list[str] = []
    seen: set[str] = set()
    for raw in domains:
        name = str(raw or "").strip().lower()
        if name and name not in seen:
            seen.add(name)
            names.append(name)
    if not api_key or not names:
        return {}
    post = post or requests.post
    found: dict[str, dict[str, Any]] = {}
    for start in range(0, len(names), 100):
        chunk = names[start:start + 100]
        try:
            response = post(
                STORELEADS_BULK_URL,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={"domains": chunk, "fields": _STORELEADS_FIELDS},
                timeout=timeout,
            )
            status = int(getattr(response, "status_code", 0) or 0)
            payload = response.json() if status == 200 and hasattr(response, "json") else {}
        except Exception:  # noqa: BLE001
            logger.warning("[outbound] StoreLeads bulk lookup failed")
            continue
        rows = []
        if isinstance(payload, dict):
            rows = payload.get("domains") or []
        elif isinstance(payload, list):
            rows = payload
        for row in rows:
            if not isinstance(row, dict):
                continue
            key = str(row.get("name") or "").strip().lower()
            if key:
                found[key] = row
    return found


def push_to_clay(webhook_url: str, leads: list[dict[str, Any]], *, timeout: int = 30) -> dict[str, Any]:
    """Post a batch of leads to the Clay table's inbound webhook."""
    if not webhook_url:
        return {"pushed": 0, "skipped": True, "reason": "no Clay webhook configured"}
    if not leads:
        return {"pushed": 0, "skipped": True, "reason": "no leads to push"}
    resp = requests.post(webhook_url, json={"leads": leads}, timeout=timeout)
    resp.raise_for_status()
    return {"pushed": len(leads)}


# ---- orchestration -----------------------------------------------------------

@dataclass
class PipelineResult:
    scanned: int = 0
    matched_icp: int = 0
    fresh: int = 0
    pushed: int = 0
    skipped_already_contacted: int = 0
    leads: list[dict[str, Any]] = field(default_factory=list)
    dry_run: bool = False
    partial: bool = False  # True if StoreLeads cut us off (rate limit) mid-run
    recipe: str = ""       # which pull recipe sourced this batch
    storeleads_status: Any = ""
    storeleads_error: str = ""


def storeleads_cursor_key(recipe_key: str, params: Optional[dict[str, Any]]) -> str:
    """Stable id for one List Domains query.

    Date-window bounds change every morning, so they are not part of the key.
    A saved cursor then keeps walking the same filter instead of restarting.
    """

    import hashlib
    import json

    stable = {
        str(key): value
        for key, value in (params or {}).items()
        if str(key) != "cursor"
        and not str(key).endswith(":min")
        and not str(key).endswith(":max")
    }
    raw = json.dumps(stable, sort_keys=True, default=str)
    digest = hashlib.sha256(raw.encode()).hexdigest()[:16]
    return f"storeleads:{recipe_key or 'list'}:{digest}"


def load_storeleads_cursor(engine: Any, key: str) -> Optional[str]:
    """Resume token for a previous pull. Missing storage starts at the first page."""

    if engine is None or not key:
        return None
    try:
        import outbound_suppression as suppression

        value = suppression.get_cursor(engine, key).strip()
    except Exception:  # noqa: BLE001
        logger.warning("[outbound] StoreLeads cursor could not be read; starting at the first page")
        return None
    return value or None


def save_storeleads_cursor(engine: Any, key: str, cursor: Optional[str]) -> None:
    """Remember the next page. A missing table does not fail the pull."""

    if engine is None or not key:
        return
    try:
        import outbound_suppression as suppression

        suppression.set_cursor(engine, key, cursor or "")
    except Exception:  # noqa: BLE001
        logger.warning("[outbound] StoreLeads cursor could not be saved")


def run_storeleads_to_clay(
    *,
    api_key: str,
    clay_webhook_url: str,
    processed_domains: set[str],
    max_new: int = 100,
    max_pages: int = STORELEADS_CURSOR_PAGES,
    dry_run: bool = False,
    throttle_seconds: float = 1.5,
    recipe: Optional[Any] = None,
    now: Optional[datetime] = None,
    settings: Optional[dict[str, Any]] = None,
    extra_params: Optional[dict[str, Any]] = None,
    fetch_page: Optional[Callable[..., list[dict[str, Any]]]] = None,
    fetch_result: Optional[Callable[..., StoreLeadsPage]] = None,
    push: Optional[Callable[..., dict[str, Any]]] = None,
    amazon_check: Optional[Callable[[dict[str, Any]], Optional[dict[str, Any]]]] = None,
    engine: Any = None,
) -> PipelineResult:
    """Pull ICP brands from StoreLeads, drop already-contacted ones, push the
    rest to Clay. With dry_run=True (or no Clay webhook) nothing is pushed; the
    matched leads are returned for preview.

    Paces requests (throttle_seconds between pages) to stay under StoreLeads'
    rate limit. If StoreLeads still cuts us off, returns the brands gathered so
    far and flags the result partial, instead of failing the whole pull.
    When ``engine`` is set, the next cursor is saved and the following run
    continues after the pages already read. The page cap does not change.
    """
    push = push or push_to_clay
    # Injected page fetchers (tests) stay on offset pages. Live pulls follow
    # next_cursor, which is what gets past the ~550 store offset cap.
    use_cursor = fetch_page is None
    if use_cursor:
        fetch_result = fetch_result or fetch_storeleads_result

    result = PipelineResult(dry_run=dry_run or not clay_webhook_url)
    result.recipe = getattr(recipe, "key", "") if recipe else ""
    seen_this_run: set[str] = set()

    # A recipe supplies the server-side filters that make this pull a timed
    # trigger (see outbound_recipes); without one we pull the broad ICP list.
    # extra_params wins when a caller is walking one query of a multi-query recipe.
    if extra_params is not None:
        extra = extra_params
    elif recipe is not None:
        extra = recipe.params(now, settings)
    else:
        extra = None

    cursor_key = ""
    cursor: Optional[str] = None
    if engine is not None and use_cursor:
        cursor_key = storeleads_cursor_key(result.recipe or "list", extra)
        cursor = load_storeleads_cursor(engine, cursor_key)
    stale_reset = False
    for page in range(max_pages):
        if result.fresh >= max_new:
            break
        if page > 0 and throttle_seconds:
            time.sleep(throttle_seconds)
        requested = cursor
        try:
            if use_cursor:
                assert fetch_result is not None
                fetched = fetch_result(
                    api_key, cursor=cursor, page_size=STORELEADS_PAGE_SIZE, extra_params=extra,
                )
                if isinstance(fetched, StoreLeadsPage):
                    stores = fetched.domains
                    cursor = fetched.next_cursor
                    if fetched.status or fetched.error:
                        result.storeleads_status = fetched.status
                        result.storeleads_error = fetched.error or ""
                elif isinstance(fetched, tuple) and len(fetched) == 2:
                    stores, cursor = fetched
                    cursor = str(cursor).strip() or None if cursor else None
                else:
                    stores, cursor = list(fetched or []), None
            else:
                assert fetch_page is not None
                stores = fetch_page(api_key, page=page, extra_params=extra)
                cursor = None
        except Exception as exc:  # noqa: BLE001 — return partial instead of failing
            if cursor_key and requested and not stale_reset:
                stale_reset = True
                cursor = None
                save_storeleads_cursor(engine, cursor_key, "")
                continue
            response = getattr(exc, "response", None)
            result.storeleads_status = getattr(response, "status_code", "") or ""
            result.storeleads_error = str(exc)[:300]
            logger.warning("[outbound] StoreLeads page %s failed, returning %s brands gathered: %s",
                           page, len(result.leads), exc)
            result.partial = True
            break
        stopped_early = False
        if not stores:
            if cursor_key and requested and not stale_reset:
                stale_reset = True
                cursor = None
                save_storeleads_cursor(engine, cursor_key, "")
                continue
            if cursor_key:
                save_storeleads_cursor(engine, cursor_key, "")
            break
        for store in stores:
            result.scanned += 1
            # A recipe may re-check the trigger here when StoreLeads cannot
            # express it as a single filter (see outbound_recipes.Recipe.keeps).
            if recipe is not None and not recipe.keeps(store, now, settings):
                continue
            if not store_matches_icp(store, settings):
                continue
            result.matched_icp += 1
            lead = to_clay_lead(store, now=now)
            if recipe is not None:
                # The recipe is why this brand surfaced now, so it leads the
                # reason and is carried through for per-recipe measurement.
                reason = recipe.reason_for(settings)
                lead["recipe"] = recipe.key
                lead["reason"] = reason
                lead["signals"] = [reason] + [
                    s for s in lead.get("signals", []) if s != reason
                ]
            # Amazon last, so a real finding about their own listings wins over
            # the recipe's guess at why now.
            if amazon_check is not None:
                try:
                    apply_amazon(lead, amazon_check(lead))
                except Exception as exc:  # noqa: BLE001
                    logger.warning("[outbound] amazon check failed for %s: %s",
                                   lead.get("domain"), exc)
            if lead.get("tier") == "X":
                continue  # public company / out of profile — drop
            domain = lead["domain"]
            if not domain or domain in seen_this_run:
                continue
            if domain in processed_domains:
                result.skipped_already_contacted += 1
                continue
            seen_this_run.add(domain)
            result.leads.append(lead)
            result.fresh += 1
            if result.fresh >= max_new:
                stopped_early = True
                break
        if use_cursor and cursor_key:
            # A full page advances. Stopping because the cap filled keeps the
            # cursor for this page so the unread stores are still read later.
            save_storeleads_cursor(
                engine, cursor_key, (requested if stopped_early else cursor) or "",
            )
        if stopped_early or (use_cursor and not cursor):
            break

    # Rank hottest first so Tier A brands lead the CSV / the push.
    result.leads.sort(key=lambda l: l.get("score", 0), reverse=True)

    if not result.dry_run and result.leads:
        outcome = push(clay_webhook_url, result.leads)
        result.pushed = int(outcome.get("pushed", 0) or 0)

    logger.info(
        "[outbound] scanned=%s matched=%s fresh=%s pushed=%s skipped_contacted=%s dry_run=%s",
        result.scanned, result.matched_icp, result.fresh, result.pushed,
        result.skipped_already_contacted, result.dry_run,
    )
    return result


def _absorb_pull(combined: PipelineResult, part: PipelineResult, seen: set[str]) -> None:
    """Fold one query's result into a multi-query recipe run."""
    combined.scanned += part.scanned
    combined.matched_icp += part.matched_icp
    combined.skipped_already_contacted += part.skipped_already_contacted
    combined.partial = combined.partial or part.partial
    if part.storeleads_status or part.storeleads_error:
        combined.storeleads_status = part.storeleads_status
        combined.storeleads_error = part.storeleads_error
    for lead in part.leads:
        domain = str(lead.get("domain") or "")
        if not domain or domain in seen:
            continue
        seen.add(domain)
        combined.leads.append(lead)
    combined.fresh = len(combined.leads)
    combined.leads.sort(key=lambda item: item.get("score", 0), reverse=True)


def run_recipe_pull(
    *,
    api_key: str,
    clay_webhook_url: str,
    processed_domains: set[str],
    max_new: int = 100,
    max_pages: int = STORELEADS_CURSOR_PAGES,
    dry_run: bool = False,
    throttle_seconds: float = 1.5,
    recipe: Optional[Any] = None,
    now: Optional[datetime] = None,
    settings: Optional[dict[str, Any]] = None,
    fetch_page: Optional[Callable[..., list[dict[str, Any]]]] = None,
    fetch_result: Optional[Callable[..., StoreLeadsPage]] = None,
    push: Optional[Callable[..., dict[str, Any]]] = None,
    amazon_check: Optional[Callable[[dict[str, Any]], Optional[dict[str, Any]]]] = None,
    engine: Any = None,
) -> PipelineResult:
    """Run every query a recipe expands to, then one Clay push.

    Install and uninstall filters take a single app id, and social growth is one
    network per call. Each call is a separate List Domains request. Parts are
    dry runs; this function pushes the combined list once.
    """
    push = push or push_to_clay
    if recipe is None:
        return run_storeleads_to_clay(
            api_key=api_key, clay_webhook_url=clay_webhook_url,
            processed_domains=processed_domains, max_new=max_new, max_pages=max_pages,
            dry_run=dry_run, throttle_seconds=throttle_seconds, now=now, settings=settings,
            fetch_page=fetch_page, fetch_result=fetch_result, push=push,
            amazon_check=amazon_check, engine=engine,
        )

    sets = recipe.param_sets(now, settings)
    combined = PipelineResult(dry_run=dry_run or not clay_webhook_url, recipe=recipe.key)
    seen = set(processed_domains)
    for params in sets:
        if combined.fresh >= max_new:
            break
        part = run_storeleads_to_clay(
            api_key=api_key, clay_webhook_url="", processed_domains=processed_domains,
            max_new=max_new - combined.fresh, max_pages=max_pages, dry_run=True,
            throttle_seconds=throttle_seconds, recipe=recipe, now=now, settings=settings,
            extra_params=params, fetch_page=fetch_page, fetch_result=fetch_result,
            push=push, amazon_check=amazon_check, engine=engine,
        )
        _absorb_pull(combined, part, seen)

    if getattr(recipe, "key", "") == "churned_tool" and combined.scanned == 0 and sets:
        # The uninstall filter returned an empty list and no error. Scan the ICP
        # and keep stores whose apps[].removed_at is inside the window.
        logger.warning(
            "[outbound] churned_tool uninstall filter returned no domains; "
            "filtering apps[].removed_at ourselves"
        )
        import outbound_recipes as recipes

        fallback = dict(sets[0])
        fallback.pop("f:app_uninstalled_at", None)
        fallback.pop("f:app_uninstalled_at:min", None)
        variant = dataclasses.replace(recipe, client_filter=recipes.churn_fallback_keeps)
        part = run_storeleads_to_clay(
            api_key=api_key, clay_webhook_url="", processed_domains=processed_domains,
            max_new=max(0, max_new - combined.fresh), max_pages=min(max_pages, 8),
            dry_run=True, throttle_seconds=throttle_seconds, recipe=variant, now=now,
            settings=settings, extra_params=fallback, fetch_page=fetch_page,
            fetch_result=fetch_result, push=push, amazon_check=amazon_check,
            engine=engine,
        )
        _absorb_pull(combined, part, seen)

    if combined.scanned == 0:
        logger.warning(
            "[outbound] recipe=%s scanned 0 status=%s error=%s",
            getattr(recipe, "key", "") or "",
            combined.storeleads_status,
            combined.storeleads_error,
        )

    if not combined.dry_run and combined.leads:
        outcome = push(clay_webhook_url, combined.leads)
        combined.pushed = int(outcome.get("pushed", 0) or 0)
    return combined


def load_config_from_env() -> tuple[str, str]:
    """Read the two values David sets on Render. Never hardcoded."""
    api_key = (os.getenv("STORELEADS_API_KEY") or "").strip()
    clay_webhook_url = (os.getenv("CLAY_WEBHOOK_URL") or "").strip()
    return api_key, clay_webhook_url


# What Clay's AI reads to write the opening sentence. Mirrors
# outbound_amazon.CLAY_FACT_COLUMNS; kept as a literal so this module never has to
# import outbound_amazon, which imports this one.
AMAZON_FACT_COLUMNS = (
    "amz_situation", "amz_product", "amz_sellers_band", "amz_undercut",
    "amz_rivals_on_name", "amz_marketplace", "amz_confidence",
)

# Column order for the CSV that gets imported into Clay (used on the Launch plan,
# before webhooks unlock on Growth). These are the fields Clay needs to run its
# enrichment and the two prompts.
#
# NEVER RENAME an existing entry here or in CLAY_CSV_HEADERS. The Clay table's import
# mapping is keyed on these exact headers, so a rename silently creates a second set
# of columns that the enrichment never reads.
#
# ADDING a column is a deliberate act: it appears as a new column to map, once, on the
# next Clay import. The amz_* facts below were added on purpose, because Clay writes
# the opening sentence now and needs something true to write it from.
#
# The facts are BUCKETED on our side ("a handful of other sellers", never 18). Once an
# AI is holding a number there is nothing left to stop it quoting one, and a brand that
# reads "19 sellers" and counts 14 stops reading. Send words, not counts.
CLAY_CSV_COLUMNS = (
    "tier", "brand", "domain", "niche", "country", "reason", "recipe", "score",
    "revenue_usd", "categories", "video_url",
) + AMAZON_FACT_COLUMNS

# The CSV header we WRITE for each field. These are named to match the Clay table's
# existing columns exactly, so Clay auto-maps on import instead of creating a second
# set of columns. Case matters: a header of "domain" makes a new text column, while
# "Domain" matches the existing one the enrichment actually reads from.
CLAY_CSV_HEADERS = {
    "brand": "Merchant Name",
    "domain": "Domain",
    "reason": "personalization",
    "revenue_usd": "revenue_usd",
}


def clay_header(field: str) -> str:
    return CLAY_CSV_HEADERS.get(field, field)


def leads_to_csv(leads: list[dict[str, Any]]) -> str:
    """Serialize matched leads to CSV text for manual import into Clay."""
    import csv
    import io

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=CLAY_CSV_COLUMNS, extrasaction="ignore")
    # Write Clay's own column names as the header row so the import auto-maps.
    writer.writerow({f: clay_header(f) for f in CLAY_CSV_COLUMNS})
    for lead in leads:
        row = dict(lead)
        # Revenue is stored in cents; a person reading the sheet needs dollars,
        # otherwise $8.4M reads as 835227012 and looks a hundred times too big.
        try:
            # Freshly pulled leads carry the StoreLeads field name; leads read back
            # out of our own table carry ours. Reading only one silently exports
            # every scanned brand at $0.
            cents = lead.get("estimated_sales_yearly_cents")
            if cents in (None, ""):
                cents = lead.get("revenue_cents")
            row["revenue_usd"] = int(cents or 0) // 100
        except (TypeError, ValueError):
            row["revenue_usd"] = 0
        # Flatten anything non-scalar so the CSV stays clean.
        cats = row.get("categories")
        if isinstance(cats, (list, tuple)):
            row["categories"] = ", ".join(str(c) for c in cats)
        writer.writerow(row)
    return buf.getvalue()
