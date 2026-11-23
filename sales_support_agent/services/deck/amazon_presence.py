"""Amazon presence for a prospect strategy deck.

A website with no attached ASIN is not evidence that the brand is off Amazon.
This module searches Amazon through the Rainforest client the repo already
uses, with the same brand-match rules as the outbound Amazon check.

The result is one of three states:

- ``present``: a brand store or a confident listing match. The deck builds an
  existing-seller plan from those listings.
- ``absent``: Amazon answered and none of the searches returned a listing or
  a brand store. Only this state may say the brand is not an Amazon seller.
- ``unknown``: no key, a failed call, or results we could not attribute. The
  deck makes no claim about Amazon presence.
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

PRESENT = "present"
ABSENT = "absent"
UNKNOWN = "unknown"

_CONFIDENT = {"medium", "high"}
_STORE_KEYS = ("brand_store", "store", "brand_header")


@dataclass(frozen=True)
class AmazonPresence:
    """What the Amazon check was able to confirm."""

    status: str
    source: str
    detail: str = ""
    listings: tuple[dict[str, Any], ...] = ()

    @property
    def absence_confirmed(self) -> bool:
        return self.status == ABSENT


def unknown_presence(source: str, detail: str = "") -> AmazonPresence:
    return AmazonPresence(status=UNKNOWN, source=source, detail=detail)


def attached_presence(asin: str = "") -> AmazonPresence:
    """An ASIN or Amazon URL was supplied, so the brand is on Amazon.

    The listing the operator already attached is rendered by the existing
    Amazon deck path. This does not replace that path with a search roster.
    """

    del asin
    return AmazonPresence(
        status=PRESENT,
        source="attached_listing",
        detail="An Amazon ASIN or URL was supplied.",
    )


def listing_plan_text(brand: str, listings: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> str:
    """Existing-seller plan copy from the listings the check actually returned."""

    name = (brand or "This brand").strip() or "This brand"
    rows = [item for item in listings if isinstance(item, dict)]
    if not rows:
        return (
            f"{name} already sells on Amazon. A brand store or listing match was confirmed, "
            "and the plan starts from that live presence rather than a first launch."
        )
    bits: list[str] = []
    for item in rows[:4]:
        parts: list[str] = []
        title = str(item.get("title") or "").strip()
        asin = str(item.get("asin") or "").strip()
        if title:
            parts.append(title)
        if asin:
            parts.append(f"ASIN {asin}")
        rating = item.get("rating")
        if isinstance(rating, (int, float)):
            parts.append(f"{float(rating):.1f} stars")
        reviews = item.get("review_count")
        if isinstance(reviews, int):
            parts.append(f"{reviews:,} reviews")
        price = str(item.get("price") or "").strip()
        if price:
            parts.append(price)
        images = item.get("image_count")
        if isinstance(images, int):
            parts.append(f"{images} images")
        if parts:
            bits.append(", ".join(parts))
    roster = "; ".join(bits)
    return (
        f"{name} already sells on Amazon. This existing-seller plan uses the live listings: {roster}. "
        "Next work is the live PDP, reviews, and offer, not a first launch."
    )


def lookup_brand_on_amazon(
    brand: str,
    *,
    domain: str = "",
    client: Any = None,
) -> AmazonPresence:
    """Search Amazon for the brand. Failures stay unknown and never raise."""

    try:
        return _lookup(brand, domain=domain, client=client)
    except Exception as exc:  # noqa: BLE001 — a provider failure must not invent an absence
        logger.warning("Amazon presence lookup failed: %s", exc)
        return unknown_presence("error", "Amazon search failed.")


def _lookup(brand: str, *, domain: str, client: Any) -> AmazonPresence:
    from outbound_amazon import _compact, brand_candidates

    terms = _search_terms(brand, domain)
    if not terms and not brand_candidates(brand, "", limit=1):
        return unknown_presence("no_brand", "No usable brand name to search.")
    if not terms:
        terms = brand_candidates(brand, "", limit=3)
    candidates = list(terms)
    for extra in brand_candidates(brand, "", limit=3):
        if extra not in candidates:
            candidates.append(extra)

    if client is None:
        if not (os.getenv("RAINFOREST_API_KEY", "") or "").strip():
            return unknown_presence("unconfigured", "Amazon search is not configured.")
        from sales_support_agent.services.rainforest import RainforestClient

        client = RainforestClient()
        if not str(getattr(client, "api_key", "") or "").strip():
            return unknown_presence("unconfigured", "Amazon search is not configured.")

    search = getattr(client, "search", None)
    if search is None:
        return unknown_presence("unconfigured", "Amazon search is not configured.")

    completed_empty = 0
    completed_with_rows = 0
    incomplete = False
    matched: list[dict[str, Any]] = []
    seen: set[str] = set()
    store_hit = False
    for term in terms:
        try:
            payload = search(term)
        except Exception as exc:  # noqa: BLE001 — one failed query is not a negative
            logger.info("Amazon search failed for %r: %s", term, exc)
            incomplete = True
            continue
        if not isinstance(payload, dict):
            incomplete = True
            continue
        info = payload.get("request_info") or {}
        if isinstance(info, dict) and info.get("success") is False:
            incomplete = True
            continue
        if _store_matches(payload, candidates):
            store_hit = True
        results = payload.get("search_results")
        if not isinstance(results, list):
            incomplete = True
            continue
        if not results:
            completed_empty += 1
            continue
        completed_with_rows += 1
        high = False
        for item in results:
            if not isinstance(item, dict):
                continue
            confidence = _listing_confidence(candidates, item)
            if confidence not in _CONFIDENT:
                continue
            key = str(item.get("asin") or "").strip() or _compact(item.get("title"))
            if not key or key in seen:
                continue
            seen.add(key)
            matched.append(item)
            if confidence == "high":
                high = True
            if len(matched) >= 4:
                break
        if high or len(matched) >= 4:
            break

    if matched or store_hit:
        listings = tuple(_enrich_listing(client, item) for item in matched[:4])
        source = "brand_store" if store_hit and not matched else "rainforest_search"
        return AmazonPresence(
            status=PRESENT,
            source=source,
            detail="Amazon search returned a brand store or a listing whose brand matches.",
            listings=listings,
        )
    if incomplete:
        return unknown_presence(
            "incomplete",
            "Amazon search did not finish for every name, so absence was not confirmed.",
        )
    if completed_with_rows == 0 and completed_empty > 0:
        return AmazonPresence(
            status=ABSENT,
            source="rainforest_search",
            detail="Every Amazon search finished with no listings and no brand store.",
        )
    if completed_with_rows:
        return unknown_presence(
            "unattributed",
            "Amazon returned listings, but none could be confirmed as this brand.",
        )
    return unknown_presence("error", "Amazon search did not answer.")


def _search_terms(brand: str, domain: str) -> list[str]:
    """Brand-led queries. A domain stem is added only when it agrees with the brand.

    fwee on benow.co.kr must search fwee. Searching the registrable domain
    first would look up a different name.
    """

    from outbound_amazon import _TRAILING_TOKENS, _clean_name, _compact, _domain_stem

    tokens = _clean_name(brand).split()
    while len(tokens) > 1 and tokens[-1] in _TRAILING_TOKENS:
        tokens.pop()
    terms: list[str] = []
    if tokens:
        terms.append(" ".join(tokens))
    stem = _domain_stem(domain)
    compact_brand = _compact(" ".join(tokens))
    if stem and (
        not compact_brand
        or stem == compact_brand
        or stem.startswith(compact_brand[:4])
        or compact_brand.startswith(stem[:4])
    ):
        if stem not in terms:
            terms.append(stem)
    return terms[:3]


def _store_matches(payload: dict[str, Any], candidates: list[str]) -> bool:
    from outbound_amazon import _compact

    blocks: list[dict[str, Any]] = []
    for key in _STORE_KEYS:
        block = payload.get(key)
        if isinstance(block, dict):
            blocks.append(block)
        elif isinstance(block, list):
            blocks.extend(item for item in block if isinstance(item, dict))
    for block in blocks:
        name = str(block.get("brand") or block.get("title") or block.get("name") or "")
        link = str(block.get("link") or block.get("url") or block.get("store_link") or "")
        if _listing_confidence(candidates, {"brand": name, "title": name}) in _CONFIDENT:
            return True
        if "/stores/" in link.lower():
            slug = link.lower().split("/stores/", 1)[1].split("/", 1)[0]
            if any(_compact(cand) and _compact(cand) in _compact(slug) for cand in candidates):
                return True
    return False


# Words that can trail a brand without naming a different company.
# "Xvive" is the Amazon brand for "Xvive Audio". "Earth" is not "Earth Moon Magick".
_GENERIC_BRAND_TOKENS = frozenset({
    "audio", "global", "official", "shop", "store", "inc", "llc", "co",
    "company", "ltd", "usa", "us", "the", "brand",
})


def _brand_field_aligns(candidates: list[str], brand_norm: str, title: str) -> bool:
    """True when the Amazon brand field is this prospect, not a shared first word."""

    from outbound_amazon import _clean_name, _compact

    if len(brand_norm) < 4:
        return False
    title_tokens = set(_clean_name(title).split())
    for cand in candidates:
        cleaned = _clean_name(cand)
        cand_norm = _compact(cleaned)
        if len(cand_norm) < 4:
            continue
        if brand_norm == cand_norm:
            return True
        tokens = cleaned.split()
        if not cand_norm.startswith(brand_norm):
            continue
        acc = ""
        used = 0
        for token in tokens:
            acc += token
            used += 1
            if acc == brand_norm:
                rest = tokens[used:]
                if rest and all(token in _GENERIC_BRAND_TOKENS or token in title_tokens for token in rest):
                    return True
                break
    return False


def _listing_confidence(candidates: list[str], item: dict[str, Any]) -> str:
    """Outbound match rules, plus a brand-field prefix that keeps the whole name.

    Amazon's brand field for Xvive Audio is often "Xvive". That matches because
    the leftover word is generic. A title that only shares the first word, such
    as "Earth Rated" for "Earth Moon Magick", does not.
    """

    from outbound_amazon import _as_text, _clean_name, _compact, _first_present, match_confidence

    base = match_confidence(candidates, item)
    if base in _CONFIDENT:
        return base
    brand_value, _ = _first_present(item, ("brand", "brand_name", "manufacturer"))
    brand_norm = _compact(_as_text(brand_value))
    title = _clean_name(_as_text(_first_present(item, ("title", "name"))[0]))
    if _brand_field_aligns(candidates, brand_norm, title):
        return "high"
    if brand_norm:
        return "low"
    for cand in candidates:
        cleaned = _clean_name(cand)
        if len(_compact(cleaned)) >= 4 and (title == cleaned or title.startswith(cleaned + " ")):
            return "medium"
    return "low"


def _price_label(value: Any) -> str:
    if isinstance(value, dict):
        raw = str(value.get("raw") or "").strip()
        if raw:
            return raw
        amount = value.get("value")
        if isinstance(amount, (int, float)):
            symbol = str(value.get("symbol") or "$")
            return f"{symbol}{float(amount):.2f}"
        return ""
    if isinstance(value, (int, float)):
        return f"${float(value):.2f}"
    return str(value or "").strip()


def _image_count(product: dict[str, Any], fallback_image: str) -> int | None:
    images = product.get("images")
    if isinstance(images, list):
        count = 0
        for image in images:
            if isinstance(image, str) and image.strip():
                count += 1
            elif isinstance(image, dict) and str(image.get("link") or image.get("url") or "").strip():
                count += 1
        if count:
            return count
    if isinstance(product.get("images_count"), int) and product["images_count"] > 0:
        return int(product["images_count"])
    if str((product.get("main_image") or {}).get("link") or fallback_image or "").strip():
        return 1
    return None


def _listing_from_search(item: dict[str, Any]) -> dict[str, Any]:
    rating = item.get("rating")
    reviews = item.get("ratings_total")
    if reviews is None:
        reviews = item.get("reviews") or item.get("review_count")
    return {
        "asin": str(item.get("asin") or "").strip(),
        "title": str(item.get("title") or item.get("name") or "").strip(),
        "rating": float(rating) if isinstance(rating, (int, float)) else None,
        "review_count": int(reviews) if isinstance(reviews, int) else None,
        "price": _price_label(item.get("price")),
        "image_count": 1 if str(item.get("image") or "").strip() else None,
    }


def _enrich_listing(client: Any, item: dict[str, Any]) -> dict[str, Any]:
    listing = _listing_from_search(item)
    asin = listing["asin"]
    getter = getattr(client, "get_product", None)
    if not asin or getter is None:
        return listing
    try:
        payload = getter(asin)
    except Exception as exc:  # noqa: BLE001 — search facts still stand
        logger.info("Amazon product lookup failed for %s: %s", asin, exc)
        return listing
    product = {}
    if isinstance(payload, dict):
        inner = payload.get("product")
        product = inner if isinstance(inner, dict) else payload
    if not isinstance(product, dict):
        return listing
    title = str(product.get("title") or "").strip()
    if title:
        listing["title"] = title
    rating = product.get("rating")
    if isinstance(rating, (int, float)):
        listing["rating"] = float(rating)
    reviews = product.get("ratings_total")
    if isinstance(reviews, int):
        listing["review_count"] = reviews
    price = _price_label((product.get("buybox_winner") or {}).get("price") if isinstance(product.get("buybox_winner"), dict) else None)
    if not price:
        price = _price_label(product.get("price"))
    if price:
        listing["price"] = price
    images = _image_count(product, str(item.get("image") or ""))
    if images is not None:
        listing["image_count"] = images
    return listing


_ABSENCE_RE = re.compile(
    r"(?:"
    r"not an Amazon seller"
    r"|does not sell on Amazon"
    r"|do not sell on Amazon"
    r"|not on Amazon yet"
    r"|no Amazon listing"
    r"|is not on Amazon"
    r"|isn't on Amazon"
    r")",
    re.IGNORECASE,
)


def asserts_amazon_absence(text: str) -> bool:
    return bool(_ABSENCE_RE.search(text or ""))
