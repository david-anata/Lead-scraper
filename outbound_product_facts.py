"""Product facts for an opener, from a public catalog before a page title.

Order:

1. Shopify ``/products.json`` on the storefront.
2. The StoreLeads domain record, only when ``STORELEADS_API_KEY`` is set.
   The key is sent as a header and is never logged.
3. The homepage ``og:description``, and only when it reads as a product
   description rather than a keyword string or a menu label.

The chosen products are written onto the lead's store profile so the opener
and later steps reuse them. This module does not write to a database.
"""

from __future__ import annotations

import html as html_lib
import logging
import os
import re
import time
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
SHOPIFY_LIMIT = 250
FETCH_TIMEOUT = 6.0
MIN_INTERVAL = 0.4

_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")
_SKIP_ITEM = re.compile(
    r"\b(?:gift\s*cards?|samples?|trial\s+packs?|shipping\s+protection|package\s+protection|"
    r"route\s+insurance|warrant(?:y|ies)|test(?:\s+item|\s+product)?s?|"
    r"order\s+fees?|shipping\s+fees?|handling\s+fees?|donations?|"
    r"replacements?(?:\s+parts?)?|refills?|playing\s+cards?|bundles?|"
    r"gift\s+wrap)\b",
    re.I,
)
_ADDON = re.compile(
    r"\b(?:bandanas?|bandages?|cleaning(?:\s+solutions?|\s+refills?)?|"
    r"stickers?|keychains?|lanyards?|patches)\b",
    re.I,
)
_LICENSED = re.compile(
    r"\b(?:disney|marvel|nfl|nba|mlb|blippi|pok[eé]mon|star\s+wars|hello\s+kitty|barbie)\b",
    re.I,
)
_NOT_FOR_SALE = re.compile(
    r"\b(?:adopt|adoption|donat(?:e|ion|ions)|fundrais(?:e|er|ing)|"
    r"personaliz(?:e|ed)|custom(?:er|ized)?|monogram|your name|"
    r"first baptist|baptist church|\bchurch\b)\b",
    re.I,
)
_COLOR_WORD = re.compile(
    r"\b(?:black|white|gold|silver|ecru|mocha|grey|gray|navy|kaki|khaki|"
    r"anthracite|rose|chestnut|powder|gloss|stretch|earth|bold|brushed)\b",
    re.I,
)
_FLUFF_WORD = {
    "ultra", "slick", "premium", "official", "best", "new", "single",
    "packaged", "classic", "classique", "large", "small", "mini",
}
_PACK_WORD = {"can", "cans", "pack", "packs", "bottle", "bottles", "jar", "jars", "box", "boxes"}
_GENERIC_TYPE = {"", "product", "products", "item", "items", "merchandise", "accessory", "accessories"}
_RESTRICTED_PRODUCT = re.compile(
    r"\b(?:wines?|alcohol|beer|spirits|liquor|cannabis|marijuana|cbd|cbda|thc|"
    r"kush|indica|sativa|granddaddy|tobacco|vapes?)\b",
    re.I,
)
_FOREIGN_WORD = re.compile(
    r"\b(?:para|piernas|comprimidos|naturales|pour|avec|fuer|della|degli)\b",
    re.I,
)
def _shouted(title: str) -> bool:
    letters = [char for char in title if char.isalpha()]
    if len(letters) < 8:
        return False
    return sum(char.isupper() for char in letters) / len(letters) >= 0.8


def _non_english(text: str) -> bool:
    return any(char.isalpha() and ord(char) > 127 for char in text)


_BLOCKED_CATEGORY = re.compile(
    r"\b(?:cbd|cbda|hemp|kratom|microdos\w*|psilocybin|psychedelic|"
    r"bpc-?157|semaglutide|tirzepatide|research use only|nicotine|vapes?|vaping)\b",
    re.I,
)
_MOOD_MUSHROOM = re.compile(r"functional mushroom", re.I)
_MOOD_CLAIM = re.compile(r"\b(?:mood|calm|focus|anxiety|psychoactive)\b", re.I)
_DRUG_CLAIM = re.compile(
    r"\b(?:cure|cures|treat|treats|treating|prevent|prevents)\b.{0,40}\b(?:disease|cancer|diabetes|alzheimer)\b"
    r"|\b(?:disease|cancer|diabetes|alzheimer)\b.{0,40}\b(?:cure|cures|treat|treats|treating|prevent|prevents)\b",
    re.I,
)
_PHRASE_JUNK = re.compile(
    r"\b(?:options|hidden|google shopping|membership|sku)\b",
    re.I,
)
_DANGLING_END = re.compile(r"\b(?:with|and|of|for|the|a|an)\s*$", re.I)
_SKU_TOKEN = re.compile(r"\b(?=[A-Za-z0-9]*\d)[A-Za-z0-9]{2,}\b|\b[A-Z]{2,5}\b")
_GROUND_STOP = {
    "the", "and", "with", "for", "from", "that", "this", "your", "their", "its",
    "into", "over", "than", "instead", "many", "brand", "brands", "people",
    "looks", "like", "one", "come", "back", "not", "something", "see", "saw",
    "love", "does", "flavors", "another", "plain", "caught", "eye",
}


def blocked_category_reason(*parts: Any) -> str:
    """A hard skip for drug, hemp, and research-peptide catalogs.

    A copper peptide serum is cosmetic and is not blocked. A research peptide,
    CBD or hemp product, kratom, nicotine, or a disease claim is.
    """

    text = " ".join(str(part or "") for part in parts)
    if _BLOCKED_CATEGORY.search(text) or _DRUG_CLAIM.search(text):
        return "blocked_category"
    if _MOOD_MUSHROOM.search(text) and _MOOD_CLAIM.search(text):
        return "blocked_category"
    return ""


def phrase_is_natural(phrase: str) -> bool:
    """True when the model named a product in plain English."""

    raw = " ".join(str(phrase or "").split())
    if not raw or _non_english(raw) or _FOREIGN_WORD.search(raw):
        return False
    if _PHRASE_JUNK.search(raw) or _DANGLING_END.search(raw) or _SKU_TOKEN.search(raw):
        return False
    words = re.findall(r"[A-Za-z][A-Za-z'-]*", raw)
    short_ok = {
        "the", "and", "for", "gel", "mix", "oil", "tea", "bar", "hat", "bag",
        "can", "set", "kit", "wax", "rye", "soy", "oat", "jam",
    }
    if any(2 <= len(word) <= 3 and word.lower() not in short_ok for word in words):
        return False
    return 2 <= len(words) <= 8


def phrase_is_grounded(phrase: str, products: list[dict[str, Any]]) -> bool:
    """True when the phrase's content words appear in the catalog we passed."""

    blob = " ".join(
        " ".join(str(item.get(key) or "") for key in ("title", "product_type", "body", "tags"))
        for item in products if isinstance(item, dict)
    ).lower()
    nouns = [
        word.lower() for word in re.findall(r"[A-Za-z][A-Za-z'-]*", phrase or "")
        if len(word) >= 3 and word.lower() not in _GROUND_STOP
    ]
    if not nouns or not blob:
        return False
    return all(noun in blob for noun in nouns)


def _off_core(title: str, product_type: str, core: str) -> bool:
    """True for gift cards, add-ons, licensed merch, and products off the core type."""

    blob = f"{title} {product_type}"
    if _SKIP_ITEM.search(blob) or _LICENSED.search(blob) or _RESTRICTED_PRODUCT.search(blob):
        return True
    if _NOT_FOR_SALE.search(blob) or _shouted(title):
        return True
    if _FOREIGN_WORD.search(title) or _non_english(title):
        return True
    if re.search(r"\b(?:it|me|my)\b", title, re.I) and len(title.split()) >= 4:
        return True
    if _ADDON.search(blob) and not _ADDON.search(core or ""):
        return True
    return False


def _content_words(text: str, brand: str) -> list[str]:
    raw = _text(re.sub(r"\([^)]*\)", " ", str(text or "")), 160)
    raw = re.split(r"\s+[–—-]\s+", raw, maxsplit=1)[0]
    brand_words = set(re.findall(r"[a-z0-9]+", brand.lower()))
    words: list[str] = []
    for word in re.findall(r"[A-Za-z][A-Za-z'-]*", raw):
        low = word.lower().strip("'")
        if not low or len(low) < 3 or low in brand_words or low in _FLUFF_WORD or low in _PACK_WORD:
            continue
        if _COLOR_WORD.fullmatch(low):
            continue
        if re.fullmatch(r"[A-Z]{2,5}", word) and not re.search(r"[AEIOU]", word):
            continue
        if words and words[-1] == low:
            continue
        words.append(low)
    return words


def short_product_phrase(title: str, product_type: str = "", brand: str = "") -> str:
    """A short noun phrase, such as ``the shave cream`` or ``the dosa mix``.

    The full catalog title, a colorway, a SKU, and a foreign title are empty.
    """

    if _off_core(title, product_type, product_type):
        return ""
    kind = _content_words(product_type, brand)
    if kind and len(kind) >= 2 and " ".join(kind) not in _GENERIC_TYPE and len(kind) <= 3:
        return "the " + " ".join(kind)
    words = _content_words(title, brand)
    if len(words) > 3:
        words = words[-3:]
    if len(words) == 1 and words[0] not in _PRODUCT_NOUNS:
        return ""
    if not words or len(words) > 3:
        return ""
    return "the " + " ".join(words)


def domain_brand_mismatch(domain: str, brand: str, vendor: str = "") -> bool:
    """True when the domain is a different company than the brand being emailed.

    ``Jay's Juniors`` on ``popfunk.com`` is a mismatch. ``Pedro García`` on
    ``pedrogarcia.com`` is not. A short acronym matches the vendor's initials.
    """

    folded = re.sub(r"[^a-z0-9]+", " ", str(brand or "").lower()).strip()
    tokens = [part for part in folded.split() if len(part) >= 4]
    host = re.sub(r"[^a-z0-9]+", "", str(domain or "").split("/")[0].split(".")[0].lower())
    vendor_folded = re.sub(r"[^a-z0-9]+", " ", str(vendor or "").lower()).strip()
    hay = host + vendor_folded.replace(" ", "")
    if tokens and any(token in hay for token in tokens):
        return False
    compact = folded.replace(" ", "")
    if compact and len(compact) <= 4 and compact in host:
        return False
    initials = "".join(part[0] for part in vendor_folded.split() if part)
    if compact and initials and compact == initials:
        return False
    if not tokens:
        # A short acronym such as RSL or GLO is not proof of a different company.
        return False
    return True
_STORE_WORD = re.compile(r"\b(?:stores?|shops?|official)\b", re.I)
_SLOGAN_START = re.compile(
    r"^(?:amplifying|discovering|redefining|empowering|inspiring|celebrating|"
    r"bringing|helping|reimagining|unlocking|elevating|welcome|shop)\b",
    re.I,
)
_TITLE_RUN = re.compile(r"(?:[A-Z][a-z]+)(?:\s+[A-Z][a-z]+){2,}")
_GRAMMAR_COMMA = re.compile(r",\s*(?:which|and|so|where|but|then)\b", re.I)
_FIRST_NAMES = {
    "alex", "amy", "anna", "anne", "ben", "brian", "chris", "daniel", "david",
    "eric", "emma", "james", "jane", "jason", "jennifer", "jessica", "john",
    "julia", "karen", "kate", "kevin", "laura", "linda", "lisa", "maria",
    "mark", "mary", "michael", "mike", "nancy", "nicole", "paul", "peter",
    "rachel", "robert", "ryan", "sarah", "steve", "susan", "tom", "will",
}
_SMALL = {
    "a", "an", "the", "and", "or", "for", "with", "from", "of", "to", "in",
    "on", "by", "our", "your", "its", "new", "best", "free",
}
_VERBS = {
    "is", "are", "was", "were", "makes", "make", "made", "sells", "selling",
    "designed", "crafted", "carved", "built", "has", "have", "helps",
}
_PRODUCT_NOUNS = {
    "bag", "bags", "bar", "bars", "board", "boards", "boot", "boots", "bottle",
    "bottles", "candle", "candles", "cream", "creams", "dress", "dresses",
    "gummy", "gummies", "hat", "hats", "jacket", "jackets", "meal", "meals",
    "oil", "oils", "ring", "rings", "serum", "serums", "shirt", "shirts",
    "shoe", "shoes", "snack", "snacks", "soap", "soaps", "sock", "socks",
    "tea", "toy", "toys", "vacuum", "vacuums",
}

_last_fetch_at = 0.0


def _text(value: Any, limit: int = 240) -> str:
    raw = html_lib.unescape(_TAG_RE.sub(" ", str(value or "")))
    raw = raw.replace("&amp;", " and ").replace("|", " ")
    return _WS_RE.sub(" ", raw).strip()[:limit]


def _tags(value: Any) -> list[str]:
    if isinstance(value, str):
        parts = re.split(r"[,|]", value)
    elif isinstance(value, (list, tuple)):
        parts = list(value)
    else:
        parts = []
    out: list[str] = []
    for part in parts:
        text = _text(part, 40)
        if text and text not in out:
            out.append(text)
    return out[:12]


def _in_stock(raw: dict[str, Any]) -> bool:
    variants = raw.get("variants") if isinstance(raw.get("variants"), list) else []
    known = [item for item in variants if isinstance(item, dict) and "available" in item]
    if not known:
        return True
    return any(item.get("available") is True for item in known)


def _skip_product(title: str, product_type: str, tags: list[str]) -> bool:
    blob = " ".join([title, product_type, " ".join(tags)])
    return bool(_SKIP_ITEM.search(blob))


def parse_shopify_products(payload: Any) -> list[dict[str, Any]]:
    """Title, type, tags, a short plain body, and stock, from products.json."""

    products = payload.get("products") if isinstance(payload, dict) else None
    if not isinstance(products, list):
        return []
    parsed: list[dict[str, Any]] = []
    for raw in products:
        if not isinstance(raw, dict):
            continue
        title = _text(raw.get("title"), 160)
        if not title:
            continue
        product_type = _text(raw.get("product_type"), 80)
        tags = _tags(raw.get("tags"))
        if _skip_product(title, product_type, tags):
            continue
        parsed.append({
            "title": title,
            "product_type": product_type,
            "tags": tags,
            "body": _text(raw.get("body_html"), 240),
            "in_stock": _in_stock(raw),
            "vendor": _text(raw.get("vendor"), 80),
        })
    return parsed


def pick_representative(products: list[dict[str, Any]], *, limit: int = 3) -> list[dict[str, Any]]:
    """Two or three products, preferring the common type and what is in stock.

    One product is returned when the catalog has only one usable title.
    """

    if not products:
        return []
    pool = [item for item in products if item.get("in_stock")] or list(products)
    counts: dict[str, int] = {}
    for item in pool:
        kind = str(item.get("product_type") or "").strip().lower()
        if kind:
            counts[kind] = counts.get(kind, 0) + 1
    if counts:
        common = max(counts, key=counts.get)
        preferred = [item for item in pool if str(item.get("product_type") or "").strip().lower() == common]
        preferred_ids = {id(item) for item in preferred}
        rest = [item for item in pool if id(item) not in preferred_ids]
        ordered = preferred + rest
    else:
        ordered = pool
    chosen: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in ordered:
        key = str(item.get("title") or "").strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        chosen.append(item)
        if len(chosen) >= limit:
            break
    return chosen


def _has_person_name(text: str) -> bool:
    words = re.findall(r"[a-z']+", text.lower())
    for first, second in zip(words, words[1:]):
        if first in _FIRST_NAMES and second not in _PRODUCT_NOUNS and second not in _SMALL and len(second) > 2:
            return True
    return False


def description_is_product(text: str) -> bool:
    """True when the homepage blurb describes a product, not a menu or keywords."""

    raw = _WS_RE.sub(" ", str(text or "")).strip()
    if not raw or "|" in raw or _STORE_WORD.search(raw) or _SLOGAN_START.search(raw):
        return False
    if raw.count(",") >= 2:
        return False
    if "," in raw:
        sides = [part.strip() for part in raw.split(",") if part.strip()]
        if sides and all(len(part.split()) <= 4 for part in sides):
            return False
    words = raw.split()
    if len(words) < 4 or len(words) > 40:
        return False
    titled = sum(1 for word in words if word[:1].isupper() and word[1:2].islower())
    if titled >= 3 and titled / len(words) > 0.6 and not re.search(
        r"\b(?:is|are|for|with|made|makes)\b", raw, re.I,
    ):
        return False
    if not re.search(r"\b(?:is|are|was|makes|made|make|designed|crafted|carved|built|for|with)\b", raw, re.I):
        return False
    return True


def natural_product_phrase(title: str, brand: str = "") -> str:
    """A product title as lowercase words with an article, or empty.

    ``Olive Wood Serving Boards`` becomes ``the olive wood serving boards``.
    A menu, a slogan, a person's name, or a keyword pile is empty.
    """

    original = str(title or "")
    raw = _text(title, 180)
    if not raw or "," in original or "|" in original:
        return ""
    if (
        _non_english(original)
        or _RESTRICTED_PRODUCT.search(original)
        or _SKIP_ITEM.search(original)
        or _FOREIGN_WORD.search(original)
    ):
        return ""
    if _STORE_WORD.search(raw) or _SLOGAN_START.search(raw) or _has_person_name(raw):
        return ""
    if _TITLE_RUN.search(raw) and not re.search(r"\b(?:for|with|and)\b", raw, re.I):
        # A title-case product name is fine. A title-case sentence fragment is not.
        if len(raw.split()) > 6:
            return ""
    # Drop a size or color after a dash when the left side is already a product.
    if " - " in raw or " – " in raw:
        left = re.split(r"\s+[–-]\s+", raw, maxsplit=1)[0].strip()
        if len(left.split()) >= 2:
            raw = left
    brand_words = set(re.findall(r"[a-z0-9]+", brand.lower()))
    skip = brand_words | {
        "best", "award", "winning", "official", "homepage", "site", "free",
        "new", "co", "inc", "llc", "usa", "us",
    }
    words: list[str] = []
    tokens = re.findall(r"[A-Za-z][A-Za-z'-]*", raw)
    for index, word in enumerate(tokens):
        low = word.lower().strip("'")
        if not low or low in skip:
            continue
        if len(low) < 3 and low not in {"de", "la"}:
            continue
        # RRS, MPB, TNT, and GHK are codes, not words a shopper says.
        vowelless = 2 <= len(low) <= 5 and not re.search(r"[aeiou]", low)
        if vowelless and (word.isupper() or index == len(tokens) - 1):
            continue
        if words and words[-1] == low:
            continue
        words.append(low)
    while words and words[0] in {"the", "and", "for", "bundle", "pack"}:
        words.pop(0)
    if len(words) < 2 or len(words) > 8:
        return ""
    if _has_person_name(" ".join(words)):
        return ""
    return "the " + " ".join(words)


def stuffed_copy_reason(text: str) -> str:
    """Why a line is keyword stuffing, or empty when the line can be sent.

    Rejects a noun pile with no verb or article, a title-case run used as the
    product, a pipe or a title comma, store/shop/official used as the product,
    and a person's name used as the product.
    """

    raw = _WS_RE.sub(" ", str(text or "")).strip()
    if not raw:
        return ""
    if "|" in raw:
        return "keyword string"
    bare_commas = _GRAMMAR_COMMA.sub("", raw)
    if "," in bare_commas:
        return "title punctuation"
    span = _product_span(raw)
    if not span:
        if _SLOGAN_START.search(raw):
            return "slogan"
        return ""
    if _STORE_WORD.search(span):
        return "store word used as the product"
    if _has_person_name(span):
        return "person name used as a product"
    if _SLOGAN_START.search(span):
        return "slogan"
    if _TITLE_RUN.search(span):
        return "title-case run"
    words = [word for word in re.findall(r"[a-z0-9']+", span.lower())]
    content = [word for word in words if word not in _SMALL and word not in _VERBS]
    has_article = any(word in {"a", "an", "the"} for word in words)
    has_verb = any(word in _VERBS for word in words)
    if len(content) >= 4 and not has_article and not has_verb:
        return "noun pile"
    return ""


def _product_span(text: str) -> str:
    patterns = (
        r"^(.*?) is the clearest\b",
        r"\bmakes (.+?),",
        r"\bthrough (.+?)(?:\.|$)",
        r"\bsells (.+?) on Amazon\b",
        r"^On Amazon, (.+?) should\b",
        r"\binto .+? is (.+?)(?:,|\.|$)",
        r"\bfirst image should be (.+?)(?:\.|$)",
    )
    for pattern in patterns:
        match = re.search(pattern, text, re.I)
        if match:
            return match.group(1).strip()
    return ""


def _pace(sleep: Callable[[float], None], min_interval: float) -> None:
    global _last_fetch_at
    wait = min_interval - (time.monotonic() - _last_fetch_at)
    if wait > 0:
        sleep(wait)
    _last_fetch_at = time.monotonic()


def fetch_shopify_path(
    domain: str,
    path: str,
    *,
    get: Optional[Callable[..., Any]] = None,
    sleep: Callable[[float], None] = time.sleep,
    guard: Optional[Callable[[str], str]] = None,
    timeout: float = FETCH_TIMEOUT,
    min_interval: float = MIN_INTERVAL,
    accept: str = "application/json",
    attempts: int = 2,
) -> Any:
    """One public Shopify URL, or None on 404, 401, or a non-Shopify response.

    ``attempts`` is 2 for a single retry. The dry-run catalog uses 1.
    """

    import requests

    host = str(domain or "").strip().lower().split("/")[0]
    if not host or " " in host or "." not in host:
        return None
    url = f"https://{host}{path}"
    if guard is None:
        from sales_support_agent.services.public_url_guard import public_http_url

        guard = public_http_url
    safe = guard(url)
    if not safe:
        return None
    getter = get or requests.get
    last_error = ""
    for attempt in range(max(1, attempts)):
        _pace(sleep, min_interval)
        try:
            response = getter(
                safe,
                timeout=timeout,
                headers={"User-Agent": USER_AGENT, "Accept": accept},
                allow_redirects=True,
            )
        except Exception as exc:  # noqa: BLE001
            last_error = exc.__class__.__name__
            if attempt == 0:
                continue
            logger.info("[outbound-products] shopify fetch failed for %s (%s)", host, last_error)
            return None
        status = int(getattr(response, "status_code", 0) or 0)
        if status in {401, 404}:
            return None
        if status in {429} or status >= 500:
            if attempt + 1 < attempts:
                continue
            return None
        if status != 200:
            return None
        if "json" in accept:
            try:
                return response.json()
            except Exception:  # noqa: BLE001
                return None
        return (getattr(response, "text", "") or "")[:150000]
    return None


def fetch_shopify_catalog(
    domain: str,
    *,
    get: Optional[Callable[..., Any]] = None,
    sleep: Callable[[float], None] = time.sleep,
    guard: Optional[Callable[[str], str]] = None,
    timeout: float = FETCH_TIMEOUT,
    min_interval: float = MIN_INTERVAL,
) -> Optional[list[dict[str, Any]]]:
    """Public products.json, or None on 404, 401, or a non-Shopify response."""

    payload = fetch_shopify_path(
        domain, f"/products.json?limit={SHOPIFY_LIMIT}",
        get=get, sleep=sleep, guard=guard, timeout=timeout, min_interval=min_interval,
    )
    if not isinstance(payload, dict):
        return None
    products = parse_shopify_products(payload)
    return products or None


def _core_type(products: list[dict[str, Any]]) -> str:
    counts: dict[str, int] = {}
    for item in products:
        kind = str(item.get("product_type") or "").strip()
        if not kind or kind.lower() in _GENERIC_TYPE:
            continue
        counts[kind] = counts.get(kind, 0) + 1
    if not counts:
        return ""
    return max(counts, key=counts.get)


def choose_products(
    products: list[dict[str, Any]],
    *,
    brand: str,
    method: str,
) -> dict[str, Any]:
    """The product the brand is known for, named as a short noun phrase."""

    core = _core_type(products)
    kept: list[dict[str, Any]] = []
    for item in products:
        title = str(item.get("title") or "")
        kind = str(item.get("product_type") or "")
        if _off_core(title, kind, core):
            continue
        phrase = short_product_phrase(title, kind, brand)
        if not phrase:
            continue
        kept.append({**item, "phrase": phrase})
        if len(kept) >= 3:
            break
    if not kept:
        return {}
    vendor = next((str(item.get("vendor") or "") for item in kept if item.get("vendor")), "")
    return {
        "source": "products.json",
        "pick": method,
        "products": [{key: value for key, value in item.items() if key != "vendor"} for item in kept],
        "phrase": kept[0]["phrase"],
        "product_title": kept[0].get("title") or "",
        "product_type": kept[0].get("product_type") or core,
        "description": next((item.get("body") or "" for item in kept if description_is_product(item.get("body") or "")), ""),
        "vendor": vendor,
    }


_CURRENCY_RE = re.compile(r'"active"\s*:\s*"([A-Z]{3})"')
_HTML_LANG_RE = re.compile(r"<html[^>]*\blang=[\"']([a-zA-Z-]{2,10})", re.I)
_COLLECTION_RE = re.compile(r"/collections/([a-z0-9][a-z0-9-]{2,})", re.I)
_SKIP_COLLECTION = {"all", "frontpage", "best-sellers", "bestsellers"}
_LANG_WORDS = {
    "es": re.compile(r"\b(?:para|con|los|las|del|una|tratamiento|dispositivo|oferta|precio|piernas)\b", re.I),
    "fr": re.compile(r"\b(?:pour|avec|les|des|une|pull|maille|pantalon|noir|blanc)\b", re.I),
}


def signals_from_storefront(html: str, titles: list[str]) -> dict[str, str]:
    """Currency and language a shopper sees on the storefront."""

    currency = ""
    match = _CURRENCY_RE.search(html or "")
    if match:
        currency = match.group(1)
    language = ""
    lang_match = _HTML_LANG_RE.search(html or "")
    if lang_match:
        language = lang_match.group(1).split("-", 1)[0].lower()
        if language == "en":
            language = ""
    if not language:
        blob = " ".join(titles)
        for code, pattern in _LANG_WORDS.items():
            if len(pattern.findall(blob)) >= 2:
                language = code
                break
    return {"currency_code": currency, "language_code": language}


def featured_collection(html: str) -> str:
    for match in _COLLECTION_RE.finditer(html or ""):
        slug = match.group(1).lower()
        if slug not in _SKIP_COLLECTION:
            return slug
    return ""


def fetch_storeleads_record(
    domain: str,
    *,
    api_key: str = "",
    get: Optional[Callable[..., Any]] = None,
    timeout: float = FETCH_TIMEOUT,
) -> Optional[dict[str, Any]]:
    """Description, title, and top products from one StoreLeads domain.

    The key is sent only as an Authorization header. It is not logged, and it
    is not copied onto the returned record. An empty key skips the call.
    """

    key = (api_key or os.getenv("STORELEADS_API_KEY") or "").strip()
    host = str(domain or "").strip().lower()
    if not key or not host:
        return None
    import requests

    getter = get or requests.get
    url = f"https://storeleads.app/json/api/v1/all/domain/{host}"
    try:
        response = getter(
            url,
            headers={"Authorization": f"Bearer {key}"},
            timeout=timeout,
        )
    except Exception as exc:  # noqa: BLE001
        logger.info("[outbound-products] storeleads lookup failed for %s (%s)", host, exc.__class__.__name__)
        return None
    status = int(getattr(response, "status_code", 0) or 0)
    logger.info("[outbound-products] storeleads lookup %s status %s", host, status)
    if status != 200:
        return None
    try:
        payload = response.json()
    except Exception:  # noqa: BLE001
        return None
    record = payload.get("domain") if isinstance(payload, dict) and isinstance(payload.get("domain"), dict) else payload
    if not isinstance(record, dict):
        return None
    # Drop anything that could echo the credential.
    safe = {k: v for k, v in record.items() if "key" not in str(k).lower() and "token" not in str(k).lower()}
    return safe


def _products_from_storeleads(record: dict[str, Any]) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    blobs: list[Any] = []
    for key in ("products", "top_products"):
        value = record.get(key)
        if isinstance(value, list):
            blobs.extend(value)
    recent = record.get("most_recently_published_product")
    if isinstance(recent, dict):
        blobs.append(recent)
    elif isinstance(recent, str):
        blobs.append({"title": recent})
    for item in blobs:
        if isinstance(item, str):
            title = _text(item, 160)
            product_type = ""
            tags: list[str] = []
            body = ""
        elif isinstance(item, dict):
            title = _text(item.get("title") or item.get("name"), 160)
            product_type = _text(item.get("product_type") or item.get("type"), 80)
            tags = _tags(item.get("tags"))
            body = _text(item.get("body_html") or item.get("description"), 240)
        else:
            continue
        if not title or _skip_product(title, product_type, tags):
            continue
        found.append({
            "title": title,
            "product_type": product_type,
            "tags": tags,
            "body": body,
            "in_stock": True,
        })
    return found


def phrase_from_description(text: str, brand: str = "") -> str:
    """A product phrase from a real description, or empty for a keyword string."""

    if not description_is_product(text):
        return ""
    raw = _text(text, 240)
    left = re.split(r"\b(?:for|with|made|designed|crafted|carved)\b", raw, maxsplit=1, flags=re.I)[0]
    phrase = natural_product_phrase(left, brand)
    if phrase:
        return phrase
    return natural_product_phrase(raw, brand)


def _catalog_rows(products: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Up to five real products, titles left as the shop wrote them."""

    core = _core_type(products)
    kept: list[dict[str, Any]] = []
    for item in products:
        title = str(item.get("title") or "").strip()
        kind = str(item.get("product_type") or "").strip()
        if not title or _off_core(title, kind, core):
            continue
        if _non_english(title) or _FOREIGN_WORD.search(title):
            continue
        tags = item.get("tags") if isinstance(item.get("tags"), list) else []
        kept.append({
            "title": title,
            "product_type": kind,
            "body": str(item.get("body") or "")[:240],
            "tags": [str(tag) for tag in tags[:8]],
        })
        if len(kept) >= 5:
            break
    return kept


def collect_product_facts(
    domain: str,
    *,
    brand: str = "",
    allow_storeleads: bool = True,
    api_key: str = "",
    homepage_html: str = "",
    fetch_homepage: Optional[Callable[[str], str]] = None,
    get: Optional[Callable[..., Any]] = None,
    sleep: Callable[[float], None] = time.sleep,
    guard: Optional[Callable[[str], str]] = None,
) -> dict[str, Any]:
    """The first usable product source for this domain.

    StoreLeads is skipped when ``allow_storeleads`` is false, which is how the
    dry run behaves. The dry run has no API key.
    """

    empty: dict[str, Any] = {
        "source": "", "products": [], "phrase": "", "description": "", "vendor": "",
        "pick": "", "currency_code": "", "language_code": "", "blocked": "",
    }
    # Homepage once for currency and language, then one best-selling fetch,
    # then one products page only if best-selling did not yield a catalog.
    # Each request is 5 seconds and is not retried.
    html = homepage_html
    if not html:
        page = fetch_shopify_path(
            domain, "/", get=get, sleep=sleep, guard=guard,
            accept="text/html", timeout=5, attempts=1,
        )
        html = page if isinstance(page, str) else ""
    fetched: list[tuple[str, list[dict[str, Any]]]] = []
    for method, path in (
        ("best-selling", "/collections/all/products.json?limit=30&sort_by=best-selling"),
    ):
        payload = fetch_shopify_path(
            domain, path, get=get, sleep=sleep, guard=guard, timeout=5, attempts=1,
        )
        rows = parse_shopify_products(payload) if isinstance(payload, dict) else []
        if rows:
            fetched.append((method, rows))
    if not fetched:
        featured = featured_collection(html)
        if featured:
            payload = fetch_shopify_path(
                domain, f"/collections/{featured}/products.json?limit=30",
                get=get, sleep=sleep, guard=guard, timeout=5, attempts=1,
            )
            rows = parse_shopify_products(payload) if isinstance(payload, dict) else []
            if rows:
                fetched.append(("featured", rows))
    if not fetched:
        payload = fetch_shopify_path(
            domain, "/products.json?limit=50",
            get=get, sleep=sleep, guard=guard, timeout=5, attempts=1,
        )
        rows = parse_shopify_products(payload) if isinstance(payload, dict) else []
        if rows:
            fetched.append(("product_type", rows))
    titles = [str(item.get("title") or "") for _method, rows in fetched for item in rows[:8]]
    signals = signals_from_storefront(html, titles)
    if fetched:
        method, rows = fetched[0]
        catalog = _catalog_rows(rows)
        blocked = blocked_category_reason(
            *(str(item.get("title") or "") for item in rows),
            *(str(item.get("product_type") or "") for item in rows),
            *(str(item.get("body") or "") for item in rows),
            *(" ".join(str(tag) for tag in (item.get("tags") or [])) for item in rows),
        )
        return {
            "source": "products.json" if catalog and not blocked else "",
            "pick": method if catalog and not blocked else "",
            "products": [] if blocked else catalog,
            "phrase": "",
            "description": "",
            "vendor": next((str(item.get("vendor") or "") for item in rows if item.get("vendor")), ""),
            "blocked": blocked,
            **signals,
        }
    if allow_storeleads:
        record = fetch_storeleads_record(domain, api_key=api_key, get=get)
        if record:
            products = _products_from_storeleads(record)
            phrase = ""
            kept = []
            for item in pick_representative(products):
                item_phrase = natural_product_phrase(item.get("title") or "", brand)
                if not item_phrase:
                    continue
                kept.append({**item, "phrase": item_phrase})
                phrase = phrase or item_phrase
            description = _text(record.get("description"), 300)
            if not phrase and description_is_product(description):
                phrase = phrase_from_description(description, brand)
            if phrase:
                return {
                    "source": "storeleads",
                    "pick": "storeleads",
                    "products": kept,
                    "phrase": phrase,
                    "description": description if description_is_product(description) else "",
                    "vendor": "",
                    **signals,
                }
    description = _og_description(html)
    phrase = phrase_from_description(description, brand)
    if phrase:
        return {
            "source": "og",
            "pick": "og",
            "products": [],
            "phrase": phrase,
            "description": description,
            "vendor": "",
            **signals,
        }
    empty.update(signals)
    return empty


_OG_DESC = re.compile(
    r"""<meta[^>]+property=["']og:description["'][^>]+content=["']([^"']+)["']"""
    r"""|<meta[^>]+content=["']([^"']+)["'][^>]+property=["']og:description["']""",
    re.I,
)


def _og_description(html: str) -> str:
    match = _OG_DESC.search(html or "")
    if not match:
        return ""
    return _WS_RE.sub(" ", html_lib.unescape(next(group for group in match.groups() if group))).strip()


def apply_product_facts(profile: dict[str, Any], facts: dict[str, Any]) -> dict[str, Any]:
    """Copy the chosen products onto the store profile the writer already reads."""

    merged = dict(profile or {})
    for key in ("currency_code", "language_code", "blocked"):
        if facts.get(key):
            merged[key] = facts[key]
    products = [item for item in (facts.get("products") or []) if isinstance(item, dict)]
    if products:
        merged["catalog_products"] = products
    if facts.get("pick"):
        merged["product_pick"] = facts["pick"]
    source = str((facts or {}).get("source") or "")
    if not source:
        return merged
    products = [item for item in (facts.get("products") or []) if isinstance(item, dict)]
    merged["product_fact_source"] = source
    merged["representative_products"] = products
    phrase = str(facts.get("phrase") or "")
    if phrase:
        merged["product_phrase"] = phrase
    if products:
        first = products[0]
        merged["most_recently_published_product"] = {
            "title": first.get("title") or "",
            "product_type": first.get("product_type") or "",
            "in_stock": first.get("in_stock"),
        }
    description = str(facts.get("description") or "")
    if description and description_is_product(description):
        merged["description"] = description[:300]
    elif source == "products.json":
        # A page title stored earlier is not a product. Drop it so the writer
        # cannot fall back to the keyword string.
        merged.pop("description", None)
        merged.pop("title", None)
    return merged
