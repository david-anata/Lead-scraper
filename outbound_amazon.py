"""Amazon brand control: is somebody else selling this brand, and cheaper?

The strongest cold opener we have for an ecommerce brand is a problem on their
own Amazon listing that they cannot see from Shopify: unauthorized third-party
sellers on their ASIN, somebody undercutting their own selling price,
competitors buying ads on their brand name, or nothing of theirs on Amazon at
all. This module finds that, one brand at a time, and turns it into a single
question-shaped opening line.

Two rules run through everything here.

First, we never guess which listing belongs to the brand. Merchant names carry
junk ("up Charge - WearForm.com"), so the domain stem leads the search, and a
match we cannot stand behind is skipped with a reason rather than reported.

Second, no precise figure ever reaches a live email. Amazon prices, seller
counts and stock levels move hourly, and a brittle number in the first line is
how a good opener becomes a wrong one. assert_no_precise_figures is the hard
gate: every generated line passes through it before it is returned.

Network calls go through an injectable client, and every one of them is wrapped
so a Rainforest failure degrades the check to a skip and never kills a pull.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from outbound_pipeline import ICP_NICHE_KEYWORDS
from outbound_recipes import setting

logger = logging.getLogger(__name__)

# ---- marketplaces ------------------------------------------------------------

AMAZON_MARKETPLACES: dict[str, str] = {
    "US": "amazon.com",
    "GB": "amazon.co.uk",
    "UK": "amazon.co.uk",
    "CA": "amazon.ca",
    "AU": "amazon.com.au",
}
DEFAULT_MARKETPLACE = "amazon.com"


def marketplace_for(country: str, settings: Optional[dict[str, Any]] = None) -> str:
    """Which Amazon we check for this brand.

    We check amazon.com by default even for an overseas brand, because the US
    marketplace is the one we are selling them on and the one we can actually
    serve. A UK brand that is absent from amazon.com is a lead, not a miss.

    Following the brand's home marketplace instead is a switch in the app, off
    by default, because it answers a different question: how are they doing
    where they already sell.
    """
    if not _bool_setting(settings, "amazon.follow_country_marketplace"):
        return DEFAULT_MARKETPLACE
    return AMAZON_MARKETPLACES.get(str(country or "").strip().upper(), DEFAULT_MARKETPLACE)


# ---- tunables ----------------------------------------------------------------
# Read through outbound_recipes.setting() so the Lead Ops page can retune them,
# with the shipped fallbacks here because these keys are ours, not the pull
# recipes'.
AMAZON_DEFAULT_SETTINGS: dict[str, Any] = {
    "amazon.enabled": True,
    "amazon.min_unknown_sellers": 3,      # below this, "a handful of sellers" is not true
    "amazon.max_listings_checked": 1,     # offers calls per brand; each costs ~30s
    "amazon.max_name_variants": 2,        # searches per brand; each costs ~30s
    "amazon.min_confidence": "high",      # anything weaker is skipped, never guessed
    # Off means always check amazon.com, even for an overseas brand. The US
    # marketplace is the one we sell and the one we can serve.
    "amazon.follow_country_marketplace": False,
    "amazon.price_erosion_min_pct": 5,
    "amazon.finding_max_age_days": 7,
}

AMAZON_TIER_A_MIN = 8
AMAZON_TIER_B_MIN = 4

_CONF_RANK = {"low": 0, "medium": 1, "high": 2}
_CONF_LABEL = {0: "low", 1: "medium", 2: "high"}


def _setting(settings: Optional[dict[str, Any]], key: str) -> Any:
    val = setting(settings, key)
    if val in (None, ""):
        return AMAZON_DEFAULT_SETTINGS.get(key)
    return val


def _bool_setting(settings: Optional[dict[str, Any]], key: str) -> bool:
    """Settings arrive from a web form as text, so "false", "0" and "off" have
    to read as off rather than as a non-empty string. A blank field falls back
    to the shipped default, which is what setting() already does.

    Defined once on purpose: a second copy of this lower down the file silently
    replaced this one and read every unset flag as on."""
    val = _setting(settings, key)
    if val is None:
        return bool(AMAZON_DEFAULT_SETTINGS.get(key))
    if isinstance(val, str):
        return val.strip().lower() not in ("", "0", "false", "no", "off")
    return bool(val)


def _int_setting(settings: Optional[dict[str, Any]], key: str) -> int:
    try:
        return int(_setting(settings, key))
    except (TypeError, ValueError):
        return int(AMAZON_DEFAULT_SETTINGS.get(key) or 0)


def _float_setting(settings: Optional[dict[str, Any]], key: str) -> float:
    try:
        return float(_setting(settings, key))
    except (TypeError, ValueError):
        return float(AMAZON_DEFAULT_SETTINGS.get(key) or 0.0)


# ---- names -------------------------------------------------------------------

# Domain prefixes brands bolt on when the bare name was taken ("behuppy.com").
_DOMAIN_PREFIXES = ("shop", "the", "get", "try", "be")
# Trailing words that are never part of the name people search for.
_TRAILING_TOKENS = frozenset({
    "uk", "us", "usa", "gb", "au", "ca",
    "ltd", "inc", "llc", "co", "company", "limited",
})


def _clean_name(name: Any) -> str:
    """Lowercase, punctuation to spaces, whitespace collapsed."""
    text = re.sub(r"[^A-Za-z0-9]+", " ", str(name or ""))
    return re.sub(r"\s+", " ", text).strip().lower()


def _compact(text: Any) -> str:
    return _clean_name(text).replace(" ", "")


def _domain_stem(domain: str) -> str:
    """wearform.com -> wearform. Tolerates a scheme, www and a path."""
    text = str(domain or "").strip().lower()
    text = re.sub(r"^[a-z][a-z0-9+.-]*://", "", text)
    text = text.split("/")[0].split("?")[0]
    if text.startswith("www."):
        text = text[4:]
    return re.sub(r"[^a-z0-9]", "", text.split(".")[0])


def brand_candidates(brand: str, domain: str, *, limit: int = 3) -> list[str]:
    """Search terms to try on Amazon, best first.

    The domain stem leads because merchant names carry junk: "up Charge -
    WearForm.com" on wearform.com has to search "wearform" first or the whole
    check reports on somebody else's listings.
    """
    out: list[str] = []

    stem = _domain_stem(domain)
    if stem:
        out.append(stem)
        for prefix in _DOMAIN_PREFIXES:
            if stem.startswith(prefix) and len(stem) - len(prefix) >= 4:
                out.append(stem[len(prefix):])
                break

    base = _clean_name(re.split(r"[-\u2010-\u2015]", str(brand or ""))[0])
    if base:
        out.append(base)
        tokens = base.split()
        while len(tokens) > 1 and tokens[-1] in _TRAILING_TOKENS:
            tokens.pop()
        out.append(" ".join(tokens))

    deduped: list[str] = []
    for cand in out:
        cand = cand.strip()
        if cand and cand not in deduped:
            deduped.append(cand)
    return deduped[:max(0, int(limit))]


# ---- sellers -----------------------------------------------------------------

# Legitimate retailers. A retailer on the listing is distribution, not a leak,
# so only "unknown" sellers ever count toward one.
KNOWN_RETAILERS: frozenset[str] = frozenset({
    "amazon", "amazon.com", "amazon warehouse", "whole foods",
    "walmart", "target", "costco", "sam's club", "kroger",
    "iherb", "vitamin shoppe", "gnc", "thrive market", "vitacost",
    "cvs", "walgreens", "rite aid",
    "sephora", "ulta",
    "boots", "holland & barrett", "superdrug", "lookfantastic",
    "chemist warehouse", "shoppers drug mart", "well.ca",
})


def classify_seller(seller_name: str, candidates: list[str], *, condition: str = "new") -> str:
    """Who is this offer: the brand, a real retailer, a used seller, or a leak.

    Only "unknown" counts toward a leak. Used offers are a different (and much
    weaker) conversation, so they are separated out rather than counted.
    """
    if not str(condition or "").strip().lower().startswith("new"):
        return "used"

    name = _compact(seller_name)
    if not name:
        return "unknown"

    for cand in candidates or []:
        cand_norm = _compact(cand)
        if cand_norm and (cand_norm in name or name in cand_norm):
            return "brand"

    for retailer in KNOWN_RETAILERS:
        retail_norm = _compact(retailer)
        if not retail_norm:
            continue
        if retail_norm in name or (len(name) >= 3 and name in retail_norm):
            return "retailer"

    return "unknown"


# ---- match confidence --------------------------------------------------------

# Words that carry no identity on their own. A candidate made only of these is
# never strong enough to claim a listing on a title match alone.
_COMMON_WORDS = frozenset({
    "the", "and", "for", "with", "your", "our", "my", "us", "we", "all", "new",
    "up", "go", "get", "try", "shop", "store", "co", "one", "two", "first",
    "best", "good", "great", "big", "little", "more", "just", "only", "every",
    "pure", "true", "real", "natural", "simple", "clean", "fresh", "daily",
    "gold", "green", "blue", "black", "white", "red", "bright", "smart", "plus",
    "health", "life", "live", "love", "care", "home", "body", "day", "night",
    "free", "well", "wild", "calm", "form", "wear", "charge", "kids", "baby",
})


def _needs_high(candidate: str) -> bool:
    words = _clean_name(candidate).split()
    if len(words) <= 1:
        return True
    return len(words) == 2 and all(w in _COMMON_WORDS for w in words)


def _niche_consistent(result: dict, niche: str) -> bool:
    """Only contradict when the listing clearly belongs to a different niche.

    Amazon titles rarely repeat the category word, so "no keyword found" is not
    evidence of a mismatch. A title that matches somebody else's niche and not
    ours is.
    """
    if not niche:
        return True
    text = " ".join(str(part) for part in (
        _as_text(result.get("title")),
        _as_text(result.get("category")),
        " ".join(str(c) for c in (result.get("categories") or []) if not isinstance(c, dict)),
        " ".join(_as_text(c) for c in (result.get("categories") or []) if isinstance(c, dict)),
    )).lower()
    if not text.strip():
        return True

    ours = ICP_NICHE_KEYWORDS.get(niche) or tuple(_clean_name(niche).split())
    if any(kw and kw in text for kw in ours):
        return True
    for label, keywords in ICP_NICHE_KEYWORDS.items():
        if label == niche:
            continue
        if any(kw in text for kw in keywords):
            return False
    return True


def match_confidence(candidates: list[str], result: dict, *, niche: str = "") -> str:
    """How sure are we that this Amazon listing is actually theirs."""
    result = result if isinstance(result, dict) else {}
    cands = [_clean_name(c) for c in (candidates or [])]
    cands = [c for c in cands if c]
    if not cands:
        return "low"

    brand_value, _ = _first_present(result, ("brand", "brand_name", "manufacturer"))
    brand_norm = _compact(_as_text(brand_value))
    exact = bool(brand_norm) and any(brand_norm == _compact(c) for c in cands)
    if exact and _niche_consistent(result, niche):
        return "high"

    # A brand field that names somebody else is a contradiction, and no title
    # match survives it.
    contradicts = bool(brand_norm) and not exact
    title = _clean_name(_as_text(_first_present(result, ("title", "name"))[0]))
    if title and not contradicts:
        for cand in cands:
            if (title == cand or title.startswith(cand + " ")) and not _needs_high(cand):
                return "medium"
    return "low"


# ---- the figure gate ---------------------------------------------------------

class PreciseFigureError(ValueError):
    """A generated line carried a figure we cannot stand behind."""


_CURRENCY_SYMBOLS = ("$", "£", "€")


def assert_no_precise_figures(line: str, *, product: str = "") -> None:
    """Raise if a line about to be sent carries a price or a count.

    The product name is removed first, so a product literally called "NAD+
    3-Pack" is fine while "3 sellers" is not. Small numbers ("a couple") are
    allowed because they survive a week of Amazon churn; anything above two
    does not.
    """
    text = str(line or "")
    product = str(product or "")
    if product:
        text = re.sub(re.escape(product), " ", text, flags=re.IGNORECASE)

    for symbol in _CURRENCY_SYMBOLS:
        if symbol in text:
            raise PreciseFigureError(f"currency symbol {symbol!r} in outbound line: {line!r}")

    for token in re.findall(r"\d+", text):
        try:
            value = int(token)
        except ValueError:
            continue
        if value > 2:
            raise PreciseFigureError(f"precise figure {token!r} in outbound line: {line!r}")


# ---- reasons -----------------------------------------------------------------
# Kept as templates so retuning a threshold regenerates the text instead of
# leaving a stale claim behind, exactly as outbound_recipes does.

_REASON_TEMPLATES: dict[str, str] = {
    "resellers": "There are a handful of other sellers sitting on your {product} listing. Are those all authorized?",
    "underpriced": "Someone is listing your {product} below your own price on Amazon. Is that an authorized seller?",
    "sponsored": "A few competitors come up on Amazon when you search your own brand name. Is that something you are watching?",
    "absent": "I could not find anything of yours on Amazon under your own name. Is that deliberate?",
}

_SIGNAL_TEXT: dict[str, str] = {
    "resellers": "Other sellers are sitting on their Amazon listings",
    "underpriced": "Someone is listing below their own Amazon price",
    "sponsored": "Competitors are sponsored on their own brand search",
    "absent": "Nothing of theirs comes up on Amazon",
    "low_stock": "A matched Amazon listing is not showing in stock",
}


def _reason(kind: str, product: str = "") -> str:
    """Build an opener and prove it carries no brittle figure. A template that
    cannot pass the gate returns nothing rather than shipping a number."""
    template = _REASON_TEMPLATES.get(kind, "")
    if not template:
        return ""
    for name in (product, "product"):
        line = template.format(product=name) if "{product}" in template else template
        try:
            assert_no_precise_figures(line, product=name)
        except PreciseFigureError as exc:
            logger.warning("[amazon] reason %r rejected by the figure gate: %s", kind, exc)
            continue
        return line
    return ""


def _short_product(title: Any) -> str:
    """A few words a founder would recognise, not a full Amazon title.

    Kept verbatim rather than tidied, because the figure gate removes this exact
    string from the line, and a tidied copy would leave the real one behind.
    """
    text = re.split(r"[,|(\[\u2013\u2014]| - ", str(title or ""))[0]
    short = " ".join(text.split()[:6]).strip(" -\u2013\u2014,:;")
    return short or "product"


# ---- defensive field reading -------------------------------------------------
# No live "offers" response from this account has been seen, so nothing here
# assumes a schema. Every read tries the plausible names and logs which one the
# account actually returned, so the first live run tells us the shape.

def _dig(data: Any, path: str) -> Any:
    cur = data
    for part in path.split("."):
        if cur is None or not hasattr(cur, "get"):
            return None
        try:
            cur = cur.get(part)
        except Exception:  # noqa: BLE001 - a hostile shape is just a miss
            return None
    return cur


def _first_present(data: Any, paths: tuple[str, ...]) -> tuple[Any, str]:
    for path in paths:
        value = _dig(data, path)
        if value not in (None, "", [], {}):
            return value, path
    return None, ""


def _as_text(value: Any) -> str:
    if isinstance(value, dict):
        for key in ("name", "title", "raw", "value"):
            if value.get(key):
                return str(value[key]).strip()
        return ""
    if value is None:
        return ""
    return str(value).strip()


def _as_price(value: Any) -> Optional[float]:
    if isinstance(value, dict):
        for key in ("value", "amount", "raw", "price"):
            if key in value:
                price = _as_price(value.get(key))
                if price is not None:
                    return price
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value) if value > 0 else None
    match = re.search(r"\d[\d,]*(?:\.\d+)?", str(value or ""))
    if not match:
        return None
    try:
        price = float(match.group(0).replace(",", ""))
    except ValueError:
        return None
    return price if price > 0 else None


_SELLER_PATHS = ("seller.name", "seller_name", "seller", "merchant.name",
                 "merchant_name", "sold_by", "offer.seller.name")
_OFFER_PRICE_PATHS = ("price.value", "price.raw", "price", "offer_price.value",
                      "offer_price", "total_price.value", "total_price")
_CONDITION_PATHS = ("condition.is_new", "condition.title", "condition",
                    "condition_title", "subcondition", "item_condition")
_SEARCH_RESULT_PATHS = ("search_results", "results", "organic_results", "products")
_OFFER_LIST_PATHS = ("offers", "product_offers", "offer_results", "results", "sellers")


def _condition_text(offer: dict) -> str:
    value, path = _first_present(offer, _CONDITION_PATHS)
    if value is None:
        # No condition field at all: Amazon's default listing is new, and
        # calling everything used would silently wipe out every finding.
        return "new"
    if path == "condition.is_new" or isinstance(value, bool):
        return "new" if value else "used"
    text = _as_text(value)
    return text or "new"


def _listed_items(payload: Any, paths: tuple[str, ...]) -> list[dict]:
    """The rows out of a response, whether it arrived wrapped in a key or bare."""
    if payload is None:
        return []
    if isinstance(payload, list):
        return [item for item in payload if hasattr(item, "get")]
    value, path = _first_present(payload, paths)
    if isinstance(value, list):
        logger.debug("[amazon] read list from %r", path)
        return [item for item in value if hasattr(item, "get")]
    return []


def _is_sponsored(item: dict) -> bool:
    value, _ = _first_present(item, ("sponsored", "is_sponsored", "ad", "is_ad"))
    return bool(value)


def _looks_ours(item: dict, candidates: list[str]) -> bool:
    haystack = _compact(_as_text(item.get("brand"))) + " " + _compact(_as_text(item.get("title")))
    return any(_compact(c) and _compact(c) in haystack for c in candidates)


# ---- network (never raises) --------------------------------------------------

def _default_client() -> Any:
    try:
        from sales_support_agent.services.rainforest import RainforestClient
        return RainforestClient()
    except Exception as exc:  # noqa: BLE001 - a missing key must not kill a pull
        logger.warning("[amazon] no Rainforest client available: %s", exc)
        return None


def _call(fn: Any, *args: Any, **kwargs: Any) -> Any:
    """Call Rainforest and turn any failure into None."""
    if fn is None:
        return None
    try:
        result = fn(*args, **kwargs)
    except TypeError:
        # Older client signatures do not take amazon_domain; the default
        # marketplace is still better than no answer.
        kwargs.pop("amazon_domain", None)
        try:
            result = fn(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001
            logger.warning("[amazon] call failed: %s", exc)
            return None
    except Exception as exc:  # noqa: BLE001
        logger.warning("[amazon] call failed: %s", exc)
        return None
    if isinstance(result, list) or hasattr(result, "get"):
        return result
    logger.warning("[amazon] unexpected response type %s", type(result).__name__)
    return None


def _product_body(payload: Any) -> Any:
    if payload is None or not hasattr(payload, "get"):
        return {}
    inner = payload.get("product")
    return inner if hasattr(inner, "get") else payload


def _in_stock(product: dict) -> Optional[bool]:
    value, path = _first_present(product, (
        "in_stock", "buybox_winner.availability.in_stock",
        "buybox_winner.availability.raw", "availability.raw", "availability",
        "stock_level",
    ))
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if path == "stock_level":
        try:
            return int(value) > 0
        except (TypeError, ValueError):
            return None
    text = _as_text(value).lower()
    if not text:
        return None
    if "out of stock" in text or "unavailable" in text or "currently unavailable" in text:
        return False
    return True


def _low_stock(product: dict) -> bool:
    """Not in stock, or Amazon warning that only a few are left. The count
    itself is deliberately never carried forward."""
    if _in_stock(product) is False:
        return True
    text = " ".join(_as_text(_dig(product, path)) for path in (
        "availability.raw", "buybox_winner.availability.raw", "availability",
    )).lower()
    return "left in stock" in text or bool(re.search(r"only\s+\d+\s+left", text))


def _inspect_listing(client: Any, item: dict, candidates: list[str],
                     *, marketplace: str) -> tuple[dict[str, Any], bool]:
    """One matched listing: who else is on it, and at what price."""
    asin = _as_text(item.get("asin"))
    title = _as_text(_first_present(item, ("title", "name"))[0])

    product = _product_body(_call(getattr(client, "get_product", None), asin,
                                  amazon_domain=marketplace)) if asin else {}
    if product.get("title"):
        title = _as_text(product.get("title"))

    # The brand's OWN current selling price, which is the buybox price. Never
    # the struck-through list price: Rho sells at 50.18 against a 55.95 list,
    # and comparing to the list price turns a 4% leak into a 14% scare.
    brand_price = _as_price(_first_present(product, (
        "buybox_winner.price.value", "buybox_winner.price", "price.value", "price",
    ))[0])
    if brand_price is None:
        brand_price = _as_price(_first_present(item, ("price.value", "price"))[0])

    offers = _listed_items(_call(getattr(client, "get_offers", None), asin,
                                 amazon_domain=marketplace), _OFFER_LIST_PATHS) if asin else []

    unknown = retailer = used = 0
    cheapest: Optional[float] = None
    for offer in offers:
        seller = _as_text(_first_present(offer, _SELLER_PATHS)[0])
        kind = classify_seller(seller, candidates, condition=_condition_text(offer))
        if kind == "used":
            used += 1
            continue
        if kind == "retailer":
            retailer += 1
            continue
        if kind != "unknown":
            continue
        unknown += 1
        # "cheapest" is the cheapest UNKNOWN offer, because that is the only
        # one the erosion comparison is allowed to use.
        price = _as_price(_first_present(offer, _OFFER_PRICE_PATHS)[0])
        if price is not None and (cheapest is None or price < cheapest):
            cheapest = price

    listing = {
        "asin": asin,
        "title": title,
        "brand_price": brand_price,
        "cheapest": cheapest,
        "sellers_unknown": unknown,
        "sellers_retailer": retailer,
        "sellers_used": used,
        "in_stock": _in_stock(product),
    }
    return listing, _low_stock(product)


def _erodes(listing: dict[str, Any], min_pct: float) -> bool:
    brand_price = listing.get("brand_price")
    cheapest = listing.get("cheapest")
    if not isinstance(brand_price, (int, float)) or not isinstance(cheapest, (int, float)):
        return False
    if brand_price <= 0 or cheapest >= brand_price:
        return False
    return (brand_price - cheapest) / brand_price * 100.0 >= min_pct


# ---- time --------------------------------------------------------------------

def _iso(when: datetime) -> str:
    return when.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_iso(value: Any) -> Optional[datetime]:
    raw = str(value or "").strip().replace("Z", "+00:00")
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed


def finding_is_fresh(checked_at: Any, *, settings: Optional[dict[str, Any]] = None,
                     now: Optional[datetime] = None) -> bool:
    """True while a stored finding is recent enough to say out loud. Amazon
    listings move, so an old finding is a wrong claim waiting to be sent."""
    stamp = _parse_iso(checked_at)
    if stamp is None:
        return False
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    return (now - stamp) <= timedelta(days=_int_setting(settings, "amazon.finding_max_age_days"))


# ---- the check ---------------------------------------------------------------

def _tier(score: int, excluded: bool) -> str:
    if excluded:
        return "X"
    if score >= AMAZON_TIER_A_MIN:
        return "A"
    if score >= AMAZON_TIER_B_MIN:
        return "B"
    return "C"


def _is_retailer_itself(brand: str, domain: str) -> bool:
    """A retailer has no listings of its own to lose, so this play does not
    apply to them. Exact names only, so "Target Nutrition" stays a brand."""
    names = {_compact(brand), _domain_stem(domain)}
    names.discard("")
    return bool(names & {_compact(r) for r in KNOWN_RETAILERS})


def brand_control(brand: str, domain: str, country: str, *, niche: str = "",
                  client: Any = None, settings: Optional[dict[str, Any]] = None,
                  now: Optional[datetime] = None) -> dict[str, Any]:
    """Check one brand's control of its own Amazon marketplace.

    Ordered so the most compelling trigger becomes the opener, the same way
    score_store() picks a "why now". Zero search results is a finding, not an
    error. Anything we cannot confirm is skipped with a reason attached.
    """
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    marketplace = marketplace_for(country, settings)

    def _out(**over: Any) -> dict[str, Any]:
        base: dict[str, Any] = {
            "score": 0,
            "tier": "C",
            "reason": "",
            "signals": [],
            "confidence": "none",
            "findings": {"listings": [], "sponsored_competitors": [], "absent": False},
            "excluded": False,
            "skipped_reason": "",
            "checked_at": _iso(now),
            "marketplace": marketplace,
        }
        base.update(over)
        return base

    if not _bool_setting(settings, "amazon.enabled"):
        return _out(skipped_reason="Amazon check is switched off")

    if _is_retailer_itself(brand, domain):
        return _out(tier="X", excluded=True,
                    skipped_reason="This is a retailer, not a brand with listings to protect")

    max_variants = max(1, _int_setting(settings, "amazon.max_name_variants"))
    candidates = brand_candidates(brand, domain, limit=max_variants)
    if not candidates:
        return _out(skipped_reason="No usable brand name to search")

    client = client if client is not None else _default_client()
    if client is None:
        return _out(skipped_reason="No Amazon data source configured")

    max_listings = max(1, _int_setting(settings, "amazon.max_listings_checked"))
    min_rank = _CONF_RANK.get(str(_setting(settings, "amazon.min_confidence") or "").lower(), 1)

    ok_searches = 0
    saw_results = False
    best_rank = -1
    matched: list[dict] = []
    seen_keys: set[str] = set()
    sponsored: list[str] = []

    for cand in candidates[:max_variants]:
        payload = _call(getattr(client, "search", None), cand, amazon_domain=marketplace)
        if payload is None:
            continue
        ok_searches += 1
        results = _listed_items(payload, _SEARCH_RESULT_PATHS)
        if results:
            saw_results = True
        for item in results:
            if _is_sponsored(item) and not _looks_ours(item, candidates):
                label = _as_text(_first_present(item, ("brand", "title"))[0])
                if label and label not in sponsored:
                    sponsored.append(label)
            rank = _CONF_RANK.get(match_confidence(candidates, item, niche=niche), 0)
            best_rank = max(best_rank, rank)
            if rank >= min_rank and len(matched) < max_listings:
                key = _as_text(item.get("asin")) or _compact(item.get("title"))
                if key and key not in seen_keys:
                    seen_keys.add(key)
                    matched.append(item)
        # A confident match is as good as it gets, so stop spending calls.
        if best_rank == _CONF_RANK["high"]:
            break

    confidence = _CONF_LABEL.get(best_rank, "none")

    if ok_searches == 0:
        # Never report "absent" off the back of a failed call.
        return _out(skipped_reason="Amazon search did not answer")

    if not saw_results:
        score = 4
        return _out(score=score, tier=_tier(score, False), reason=_reason("absent"),
                    signals=[_SIGNAL_TEXT["absent"]], confidence="none",
                    findings={"listings": [], "sponsored_competitors": sponsored, "absent": True})

    if not matched:
        return _out(confidence=confidence,
                    findings={"listings": [], "sponsored_competitors": sponsored, "absent": False},
                    skipped_reason="Could not confirm which Amazon listings are theirs")

    listings: list[dict[str, Any]] = []
    low_stock = False
    for item in matched[:max_listings]:
        listing, listing_low = _inspect_listing(client, item, candidates, marketplace=marketplace)
        listings.append(listing)
        low_stock = low_stock or listing_low

    min_unknown = max(1, _int_setting(settings, "amazon.min_unknown_sellers"))
    erosion_min = _float_setting(settings, "amazon.price_erosion_min_pct")
    leaking = [l for l in listings if int(l.get("sellers_unknown") or 0) >= min_unknown]
    underpriced = [l for l in listings if _erodes(l, erosion_min)]

    score = 0
    signals: list[str] = []
    if leaking:
        score += min(6, 2 * len(leaking))
        signals.append(_SIGNAL_TEXT["resellers"])
    if underpriced:
        score += 3
        signals.append(_SIGNAL_TEXT["underpriced"])
    if sponsored:
        score += 2
        signals.append(_SIGNAL_TEXT["sponsored"])
    if low_stock:
        score += 1
        signals.append(_SIGNAL_TEXT["low_stock"])

    if leaking:
        reason = _reason("resellers", _short_product(leaking[0].get("title")))
    elif underpriced:
        reason = _reason("underpriced", _short_product(underpriced[0].get("title")))
    elif sponsored:
        reason = _reason("sponsored")
    else:
        reason = ""

    return _out(score=score, tier=_tier(score, False), reason=reason, signals=signals,
                confidence=confidence,
                findings={"listings": listings, "sponsored_competitors": sponsored,
                          "absent": False})
