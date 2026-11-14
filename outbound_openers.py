"""One opening line per company, plus a short brand angle (replaces Clay's AI column).

``opener`` fills ``{{personalization}}``. ``brand_angle`` is pushed as the
Instantly custom variable ``brandAngle``. An opener is one plain,
shopper-visible fact: a real product, flavor, story, named retail store, or
review theme. A category label, an unnamed "sold in retail stores" line, and
strategy copy are not openers. If the model is missing, errors, or writes
something that breaks a rule, a plain sentence from a real fact is used.
When no clean fact exists, the line is left empty so the lead is skipped.

Hard rules, enforced in code not just in the prompt:
  * No em or en dashes.
  * No digits, so a made-up number can never reach a prospect.
  * No links, no email addresses, no cliches, no questions.
  * No category slugs and no unverified claims.
  * A confirmed Amazon product can be named. A rich line may also name Amazon
    as the marketplace opportunity when a positive StoreLeads fact supports it.
    Never use "does not sell on Amazon" as the hook.
  * A Trial Offer line must not claim or assume the brand already sells on Amazon.
    The brand angle may still mention Amazon as an opportunity for new shoppers.
  * One sentence. The opener aims for about 20 words and never passes 25.
    The brand angle aims for about 15 words and never passes 20.
    A line over that hard max is regenerated once, then the lead is held.
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)

MODEL = "claude-haiku-4-5-20251001"
# {{personalization}} aims for about 20 words. 25 is the hard max.
MAX_WORDS = 25
# {{brandAngle}} aims for about 15 words. 20 is the hard max.
ANGLE_MAX_WORDS = 20
# "Anata // Trial Offer // Oct 2026". Same id as the Instantly push constant.
TRIAL_OFFER_CAMPAIGN_ID = "e5f5046b-07e4-41ad-a395-79cf8766ccd0"

BANNED_PHRASES = (
    "hope this finds you", "hope you're well", "hope you are well", "game-changing",
    "game changing", "revolutionary", "cutting-edge", "cutting edge", "synergy",
    "i came across your", "touch base", "circle back", "reaching out",
    # Template tics the model fell into on the first live run.
    "i noticed", "we noticed", "building something", "something special",
    "i'm curious", "we're curious", "curious how", "ideal customer profile", "icp",
    "social momentum",
)

# StoreLeads and internal category slugs, written as words a founder would say.
CATEGORY_WORDS = {
    "apparel_accessories": "apparel and accessories",
    "food_beverage": "food and beverage",
    "beauty_wellness": "beauty and wellness",
    "health_wellness": "health and wellness",
    "home_garden": "home and garden",
    "sports_outdoors": "sports and outdoors",
    "baby_kids": "baby and kids",
    "pet_supplies": "pet supplies",
    "electronics": "electronics",
}

RECIPE_FACTS = {
    "replatformed": "recently moved their store to a new platform",
    "new_growth_app": "recently installed a growth app",
    "plan_upgrade": "recently upgraded their store plan",
}

_SLUG = re.compile(r"\b[a-z0-9]+_[a-z0-9_]+\b", re.I)
_AMAZON_WORD = re.compile(r"\b(?:amazon|amzn|asin|fba|seller central)\b", re.I)
# A current-selling claim. "new Amazon shoppers" is an opportunity, not a claim.
_TRIAL_AMAZON_CLAIM = re.compile(
    r"\byour amazon\b"
    r"|\btheir amazon\b"
    r"|\byour listings?\b"
    r"|\balready on amazon\b"
    r"|\b(?:sells|selling|sold) on amazon\b"
    r"|\b(?:lists|listed) (?:[\w']+ ){0,8}on amazon\b"
    r"|\b(?:is|are|was|were) on amazon\b"
    r"|\breorders? on amazon\b"
    r"|\bon amazon (?:already|now|today)\b",
    re.I,
)
# Strategy and filler the model has shipped when it had no product to name.
_GENERIC_COPY = re.compile(
    r"\b(?:positioned|positioning|strategic|channels?|automation|leverage|leveraging|"
    r"elevate|elevated|scalable)\b"
    r"|premium consumer|consumer channels|go[- ]to[- ]market|value proposition"
    r"|shoppers already compare|a category where"
    r"|\b(?:drive|unlock|for) growth\b"
    r"|\bgrowth (?:strategy|opportunity|potential|engine)\b",
    re.I,
)
_GENERIC_RETAIL = re.compile(
    r"\b(?:is |are )?sold in retail stores\b"
    r"|\bin retail stores\b",
    re.I,
)
_GENERIC_RETAIL_FACTS = frozenset({
    "has a store locator",
    "sold through retailers",
    "sold in stores",
    "sells wholesale",
    "is sold in retail stores",
    "sold in retail stores",
})
# Page chrome StoreLeads sometimes stores as the title. Not a product.
_PAGE_BOILERPLATE = frozenset({
    "home", "homepage", "home page", "front page", "main page", "index",
    "official website", "official site", "official web site", "official shop",
    "official store", "website", "web site", "site", "shop", "shop now",
    "shop all", "shop home", "our store", "online store", "the store", "store",
    "welcome", "welcome home", "coming soon", "new arrivals", "all products",
    "products", "product", "collections", "collection", "catalog", "catalogue",
    "menu", "cart", "my account", "account", "log in", "login", "sign in",
    "about", "about us", "contact", "contact us", "our story", "untitled",
    "just added", "best sellers", "bestsellers", "best seller", "sale",
    "on sale", "new", "new in", "explore", "discover", "learn more",
    "read more", "click here", "buy now", "add to cart", "free shipping",
})
_RETAILER_HOSTS = {
    "rei.com": "REI",
    "dickssportinggoods.com": "Dick's Sporting Goods",
    "target.com": "Target",
    "walmart.com": "Walmart",
    "wholefoodsmarket.com": "Whole Foods",
    "nordstrom.com": "Nordstrom",
    "sephora.com": "Sephora",
    "ulta.com": "Ulta",
    "gopuff.com": "Gopuff",
    "costco.com": "Costco",
    "bestbuy.com": "Best Buy",
    "kroger.com": "Kroger",
    "cvs.com": "CVS",
    "walgreens.com": "Walgreens",
    "academy.com": "Academy",
    "basspro.com": "Bass Pro",
    "cabelas.com": "Cabela's",
}
_OG_TITLE = re.compile(
    r"""<meta[^>]+property=["']og:title["'][^>]+content=["']([^"']+)["']"""
    r"""|<meta[^>]+content=["']([^"']+)["'][^>]+property=["']og:title["']""",
    re.I,
)
_OPENERS_BY_DOMAIN: dict[str, tuple[str, str]] = {}
_ANGLES_BY_DOMAIN: dict[str, tuple[str, str]] = {}

# Short, factual tie from a recipe we already recorded to how Anata helps.
_ANGLE_AFTER = {
    "replatformed": "a recent platform move",
    "new_growth_app": "a recent growth app install",
    "plan_upgrade": "a recent plan upgrade",
}

_SYSTEM = (
    "You write the first line of a cold email from Abby at Anata to a founder or "
    "marketing lead at a consumer brand. Anata Intelligence builds a brand's Amazon "
    "strategy and runs the ad actions automatically. Write ONE sentence, not a "
    "question, of about 20 words and never more than 25 words. Ground it in one real "
    "fact from the list: a real product, flavor, story, named store, or review theme. "
    "Say that fact plainly. A category label is not a fact. Do not say the brand is "
    "sold in retail stores unless a store name is in the facts. Do not write strategy, "
    "positioning, channels, automation, or guesses about what shoppers do. "
    "Only mention Amazon when an 'Amazon product' fact is listed; "
    "otherwise never say or imply they sell on Amazon. If the facts say this is a "
    "Trial Offer, do not claim or assume the brand already sells on Amazon. Never "
    "write 'your Amazon store', 'your listings', or 'already on Amazon'. "
    "Never print a slug with underscores. Do not claim social momentum, growth, or "
    "anything that is not in the facts. Do not start with 'I noticed' or 'We noticed'. "
    "If no concrete product, flavor, story, store name, or review theme is listed, "
    "write nothing. "
    "Never use the words 'curious', 'special', 'building something', or 'ideal customer "
    "profile'. No numbers. No links. No em dashes. No greeting, no sign-off."
)

_ANGLE_SYSTEM = (
    "You write one sentence for a cold email from Abby at Anata. Anata helps "
    "a consumer brand grow. Tie the line to one fact from the list and to how "
    "Anata helps that brand grow. Use only those facts. Aim for about 15 words "
    "and never more than 20 words. Only mention Amazon when an 'Amazon product' "
    "fact is listed, unless the facts say this is a Trial Offer. A Trial Offer angle "
    "may mention Amazon as an opportunity for new shoppers, for example a bundle that "
    "gives new Amazon shoppers an easy way to try the products. Do not claim or assume "
    "the brand already sells on Amazon. Never write 'your Amazon store', 'your listings', "
    "or 'already on Amazon'. No numbers. No links. No questions. No em dashes. "
    "No greeting, no sign-off."
)


def sentence_count(text: str) -> int:
    """How many sentences a line contains. A blank line counts as zero."""

    return sum(1 for part in re.split(r"[.!?]+", text or "") if part.strip())


def trial_offer_lead(lead: Optional[dict[str, Any]]) -> bool:
    """True when the line is for the Trial Offer sequence.

    Those lines must not claim or assume the brand already sells on Amazon.
    The brand angle may still mention Amazon as an opportunity. Other recipes
    keep the confirmed-product rule.
    """

    if not lead:
        return False
    recipe = str(lead.get("recipe") or "").strip().lower().replace("-", " ").replace("_", " ")
    if recipe in {"trial", "trial offer", "trial offer oct"} or recipe.startswith("trial offer"):
        return True
    for key in ("campaign_id", "campaign", "instantly_campaign_id"):
        if str(lead.get(key) or "").strip() == TRIAL_OFFER_CAMPAIGN_ID:
            return True
    blob = " ".join(
        str(lead.get(key) or "") for key in ("campaign_name", "sequence", "sequence_name")
    ).lower()
    return "trial offer" in blob


def clean(text: str) -> str:
    """Normalise punctuation and whitespace the way our emails require."""
    t = (text or "").strip().strip('"').strip()
    t = t.replace("—", ",").replace("–", ",").replace("--", ",")
    t = re.sub(r"\s+", " ", t)
    t = re.sub(r"\s+,", ",", t)
    t = re.sub(r"\s+-\s+", " - ", t)
    return t


def amazon_confirmed(lead: dict[str, Any]) -> bool:
    """True only when the Amazon check matched this brand with high confidence."""

    conf = str(lead.get("amz_confidence") or lead.get("amazon_confidence") or "").strip().lower()
    return conf == "high" and not lead.get("amazon_absent")


def trial_amazon_claim(text: str) -> bool:
    """True when a Trial Offer line treats Amazon as a store the brand already has."""

    return bool(_TRIAL_AMAZON_CLAIM.search(text or ""))


def validate(
    text: str,
    *,
    allow_amazon: bool = True,
    max_words: int = MAX_WORDS,
    trial_offer: bool = False,
) -> tuple[bool, str]:
    raw = (text or "").strip()
    if trial_offer and trial_amazon_claim(raw):
        return False, "claims the brand already sells on Amazon"
    if not trial_offer and not allow_amazon and _AMAZON_WORD.search(raw):
        return False, "claims Amazon without a confirmed match"
    if "—" in raw or "–" in raw:
        return False, "em dash"
    if _SLUG.search(raw):
        return False, "slug"
    if "?" in raw:
        return False, "question"
    t = clean(raw)
    if not t:
        return False, "empty"
    if len(t.split()) > max_words or len(t) > 220:
        return False, "too long"
    if sentence_count(t) > 1:
        return False, "more than one sentence"
    if re.search(r"\d", t):
        return False, "contains a number"
    if re.search(r"https?://|www\.|@", t):
        return False, "contains a link or address"
    low = t.lower()
    for phrase in BANNED_PHRASES:
        if phrase in low:
            return False, f"cliche: {phrase}"
    generic = generic_copy_reason(t)
    if generic:
        return False, generic
    return True, ""


def _boilerplate_words(value: str) -> list[str]:
    pieces = re.split(r"[|–—]| - ", str(value or ""))
    chunks: list[str] = []
    for piece in pieces:
        words = re.sub(r"[^a-z0-9]+", " ", piece.lower()).strip()
        if words:
            chunks.append(words)
    return chunks


def _boilerplate_chunk(words: str) -> bool:
    if words in _PAGE_BOILERPLATE:
        return True
    if re.fullmatch(
        r"(?:the |our |my |your )?(?:official )?(?:website|web site|site|store|shop|homepage|home page|home)",
        words,
    ):
        return True
    if words.startswith("welcome to ") and len(words.split()) <= 8:
        return True
    if re.fullmatch(r"(?:shop|visit|explore)(?: now| all| home| our store| the store)?", words):
        return True
    return False


def is_page_boilerplate(value: str, brand: str = "") -> bool:
    """True when the text is page chrome, not a product, story, or store name."""

    chunks = _boilerplate_words(value)
    if not chunks:
        return True
    brand_words = re.sub(r"[^a-z0-9]+", " ", (brand or "").lower()).strip()

    def empty(chunk: str) -> bool:
        return _boilerplate_chunk(chunk) or bool(brand_words and chunk == brand_words)

    return all(empty(chunk) for chunk in chunks)


def generic_copy_reason(text: str) -> str:
    """Why a line is filler. Empty when it can name a real shopper-visible fact."""

    raw = text or ""
    if _GENERIC_COPY.search(raw):
        return "generic business phrasing"
    if _GENERIC_RETAIL.search(raw) and not re.search(r"\bsold at\b", raw, re.I):
        return "generic retail filler"
    return ""


def plain_category(value: str) -> str:
    """Turn category slugs and path fragments into words."""

    seen: list[str] = []
    for piece in re.split(r"[,/|]", str(value or "")):
        slug = piece.strip().lower().replace("-", "_").replace(" ", "_").strip("_")
        if not slug or slug in {"and", "the"}:
            continue
        words = CATEGORY_WORDS.get(slug, slug.replace("_", " "))
        if words and words not in seen:
            seen.append(words)
    return ", ".join(seen[:3])


def _usable(line: str, *, allow_amazon: bool, max_words: int = MAX_WORDS) -> str:
    ok, _why = validate(line, allow_amazon=allow_amazon, max_words=max_words)
    return clean(line) if ok else ""


def _fact_phrase(value: Any) -> str:
    """A short noun phrase we can actually stand behind. Empty when it is not."""

    raw = clean(str(value or "")).strip(" .")
    if not raw or _AMAZON_WORD.search(raw):
        return ""
    if len(raw.split()) > 8 or len(raw) > 60:
        return ""
    return _usable(raw, allow_amazon=False)


def _specific_category(value: Any) -> str:
    """Prefer the most specific category segment we can say in plain words."""

    paths = [path for path in str(value or "").split(",") if path.strip()]
    broad = {"apparel", "beauty", "health", "home", "food", "electronics", "sports", "toys"}
    weak = {"jobs", "education", "business", "industrial"}

    def usable(phrase: str) -> bool:
        words = phrase.split()
        if not words or words[0] in weak:
            return False
        return not (len(words) == 1 and words[0] in broad)

    for path in paths:
        segments = [phrase for piece in path.split("/") if (phrase := _category_phrase(piece))]
        specific = [phrase for phrase in segments if usable(phrase)]
        if specific:
            return specific[-1].replace(" & ", " and ")
    fallback = _category_phrase(value)
    return fallback.replace(" & ", " and ") if fallback else ""


def _category_phrase(value: Any) -> str:
    words = plain_category(str(value or ""))
    if not words:
        return ""
    return _fact_phrase(words.split(",")[0].strip())


def _scraped_product(lead: dict[str, Any]) -> str:
    """Product words already scraped onto the lead. This does not fetch a site."""

    for key in ("site_product", "scraped_product"):
        phrase = _fact_phrase(lead.get(key))
        if phrase:
            return phrase
    for key in ("site_facts", "scraped_facts"):
        raw = lead.get(key)
        if isinstance(raw, (list, tuple)):
            raw = raw[0] if raw else ""
        phrase = _fact_phrase(raw)
        if phrase:
            return phrase
    html = str(lead.get("site_html") or lead.get("site_text") or "")
    if html:
        match = _OG_TITLE.search(html)
        if match:
            title = next(group for group in match.groups() if group)
            phrase = _fact_phrase(title)
            if phrase:
                return phrase
    return ""


@dataclass(frozen=True)
class LeadFacts:
    """Facts already on the lead. Empty strings are facts we do not have."""

    brand: str = ""
    amazon_product: str = ""
    product: str = ""
    category: str = ""
    signal: str = ""
    extra_anchors: tuple[str, ...] = ()
    rich_phrases: tuple[str, ...] = ()
    rich_opener: str = ""
    rich_angle: str = ""
    trial_offer: bool = False

    @property
    def solid(self) -> bool:
        return bool(
            self.amazon_product or self.product or self.category or self.signal
            or self.rich_phrases or self.rich_opener
        )


RICH_PERSONALIZATION_FLAG = "OUTBOUND_RICH_PERSONALIZATION_ENABLED"
_MONTHS = (
    "", "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
)
_SMALL_COUNTS = (
    "zero", "one", "two", "three", "four", "five", "six", "seven", "eight",
    "nine", "ten", "eleven", "twelve", "thirteen", "fourteen", "fifteen",
    "sixteen", "seventeen", "eighteen", "nineteen", "twenty",
)


def rich_personalization_enabled(lead: Optional[dict[str, Any]] = None) -> bool:
    """New opener facts stay off until this is turned on.

    A lead may force the flag for a before and after sample. Production
    leads do not set that key, so the environment variable decides.
    """

    if isinstance(lead, dict) and "rich_personalization" in lead:
        return bool(lead.get("rich_personalization"))
    return (os.getenv(RICH_PERSONALIZATION_FLAG) or "").strip().lower() in {"1", "true", "yes", "on"}


def _count_words(value: Any) -> str:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return ""
    if number < 0 or number > 40:
        return ""
    if number <= 20:
        return _SMALL_COUNTS[number]
    tens, ones = divmod(number, 10)
    ten_name = {2: "twenty", 3: "thirty", 4: "forty"}[tens]
    if ones == 0:
        return ten_name
    return f"{ten_name} {_SMALL_COUNTS[ones]}"


def _month_name(value: Any) -> str:
    match = re.search(r"(\d{4})-(\d{2})", str(value or ""))
    if not match:
        return ""
    month = int(match.group(2))
    if 1 <= month <= 12:
        return _MONTHS[month]
    return ""


def _signal_text(lead: dict[str, Any]) -> str:
    raw = lead.get("signals") or ""
    if isinstance(raw, (list, tuple)):
        raw = " ".join(str(item) for item in raw)
    return str(raw).lower()


def _label_text(items: Any) -> str:
    parts: list[str] = []
    for item in items or []:
        if isinstance(item, dict):
            parts.append(str(item.get("name") or item.get("title") or ""))
        else:
            parts.append(str(item))
    return " ".join(parts).lower()


def _profile_of(lead: dict[str, Any]) -> dict[str, Any]:
    raw = lead.get("store_profile")
    if isinstance(raw, str):
        try:
            import json
            raw = json.loads(raw)
        except ValueError:
            raw = {}
    return raw if isinstance(raw, dict) else {}


def _no_digits(phrase: str) -> str:
    text = clean(phrase).strip(" .")
    if not text or re.search(r"\d", text):
        return ""
    return text


def _host_name(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"^https?://", "", text)
    text = text.split("/")[0].split("?")[0].split(":")[0]
    if text.startswith("www."):
        text = text[4:]
    return text


def named_retailers(profile: dict[str, Any], *, domain: str = "") -> list[str]:
    """Retailer names present on the record. A locator URL is not a name."""

    if not isinstance(profile, dict):
        return []
    names: list[str] = []

    def add(label: Any) -> None:
        text = clean(str(label or "")).strip(" .")
        if not text or re.search(r"https?://|www\.", text):
            return
        if len(text.split()) > 6 or len(text) > 40:
            return
        if text.lower() in {
            "amazon", "ebay", "etsy", "walmart", "shopify", "online store",
            "retail", "retailers", "store", "stores", "shop",
        }:
            return
        if text not in names:
            names.append(text)

    for key in ("retailer_name", "retailers", "stockists"):
        raw = profile.get(key)
        if isinstance(raw, str):
            raw = [raw]
        elif isinstance(raw, dict):
            raw = [raw]
        for item in raw or []:
            if isinstance(item, dict):
                add(item.get("name") or item.get("title") or "")
            else:
                add(item)
    own = _host_name(domain or profile.get("name") or "")
    for key in ("retailer_page", "store_locator_page"):
        raw = str(profile.get(key) or "").strip()
        if not raw:
            continue
        if not re.search(r"https?://|www\.|\.", raw):
            add(raw)
            continue
        host = _host_name(raw)
        if not host or (own and (host == own or host.endswith("." + own))):
            continue
        for suffix, label in _RETAILER_HOSTS.items():
            if host == suffix or host.endswith("." + suffix):
                add(label)
                break
    return names


def store_fact_phrases(lead: dict[str, Any]) -> list[str]:
    """Factual clauses from a StoreLeads profile. No digits, no invented claims."""

    profile = _profile_of(lead)
    if not profile:
        return []
    phrases: list[str] = []
    product = profile.get("most_recently_published_product")
    if not isinstance(product, dict):
        product = {}
    title = _fact_phrase(product.get("title"))
    month = _month_name(product.get("published_at") or profile.get("max_product_published_at"))
    if title and month:
        phrases.append(_no_digits(f"added {title} in {month}"))
    elif title:
        phrases.append(_no_digits(f"added {title}"))
    elif month:
        phrases.append(f"added a new product in {month}")

    channels = [str(item).strip().lower() for item in (profile.get("sales_channels") or [])]
    absent = bool(lead.get("amazon_absent"))
    if "amazon" in channels and not absent:
        phrases.append("sells on Amazon")
    recipe = str(lead.get("recipe") or "").strip()
    if recipe == "amz_on" and "sells on Amazon" not in phrases and not absent and "amazon" in channels:
        phrases.append("sells on Amazon")

    for retailer in named_retailers(profile, domain=str(lead.get("domain") or "")):
        phrases.append(f"is sold at {retailer}")

    labels = _label_text((profile.get("apps") or []) + (profile.get("technologies") or []))
    compact = labels.replace(".", "")
    if "klaviyo" in compact:
        phrases.append("uses Klaviyo")
    for app_name, token in (
        ("Yotpo", "yotpo"), ("Okendo", "okendo"), ("Judge.me", "judgeme"),
        ("Stamped", "stamped"), ("Loox", "loox"),
    ):
        if token in compact:
            phrases.append(f"uses {app_name}")
            break
    if "facebook pixel" in labels or "meta pixel" in labels or "facebook ads" in labels:
        phrases.append("runs ads on Meta")
    if "google ads" in labels or "google adwords" in labels:
        phrases.append("runs ads on Google")
    if "tiktok pixel" in labels or "tiktok ads" in labels:
        phrases.append("runs ads on TikTok")

    social_names = {
        "tiktok": "TikTok", "youtube": "YouTube", "instagram": "Instagram",
        "pinterest": "Pinterest", "twitter": "Twitter",
    }
    for item in profile.get("contact_info") or []:
        if not isinstance(item, dict):
            continue
        label = social_names.get(str(item.get("type") or "").lower())
        try:
            delta = float(item.get("followers_30d") or 0)
        except (TypeError, ValueError):
            delta = 0
        if label and delta > 0:
            phrases.append(f"is growing on {label}")

    platform_names = {
        "woocommerce": "WooCommerce", "magento": "Magento",
        "bigcommerce": "BigCommerce", "shopify": "Shopify",
    }
    raw_platform = _no_digits(str(profile.get("last_platform") or ""))
    previous = platform_names.get(raw_platform.lower(), raw_platform)
    if previous:
        moved = _month_name(profile.get("last_platform_change_at"))
        if moved:
            phrases.append(f"moved from {previous} in {moved}")
        else:
            phrases.append(f"previously ran on {previous}")

    plan = str(profile.get("plan") or "")
    if "plus" in plan.lower():
        phrases.append("is on Shopify Plus")
    elif recipe == "plan_upgrade":
        phrases.append("recently upgraded their store plan")
    if recipe in {"replatform", "replatformed"}:
        phrases.append("recently moved their store to a new platform")

    counted = _count_words(profile.get("product_count"))
    if counted and counted != "zero":
        phrases.append(f"sells {counted} products")

    trust = profile.get("trustpilot") if isinstance(profile.get("trustpilot"), dict) else {}
    try:
        reviews = int((trust or {}).get("review_count") or 0)
    except (TypeError, ValueError):
        reviews = 0
    if reviews > 0:
        phrases.append("has Trustpilot reviews")

    try:
        photos = int(profile.get("product_images_created_90") or 0)
    except (TypeError, ValueError):
        photos = 0
    if photos > 0:
        phrases.append("added new product photos recently")

    city = _no_digits(str(profile.get("city") or ""))
    if city and len(city.split()) <= 3:
        phrases.append(f"is based in {city}")

    description = _fact_phrase(profile.get("description"))
    if description and description.lower() not in str(lead.get("brand") or "").lower():
        phrases.append(f"sells {description}")

    ordered: list[str] = []
    for phrase in phrases:
        if phrase and phrase not in ordered:
            ordered.append(phrase)
    return ordered


def _join_names(names: list[str]) -> str:
    if not names:
        return ""
    if len(names) == 1:
        return names[0]
    if len(names) == 2:
        return f"{names[0]} and {names[1]}"
    return f"{', '.join(names[:-1])}, and {names[-1]}"


def _ad_phrase(names: list[str]) -> str:
    joined = _join_names(names)
    if not joined:
        return ""
    return joined + " ads"


def rich_observations(lead: dict[str, Any]) -> dict[str, Any]:
    """Positive StoreLeads facts only. A missing Amazon channel is not a fact we state."""

    profile = _profile_of(lead)
    labels = _label_text((profile.get("apps") or []) + (profile.get("technologies") or []))
    channels = [str(item).strip().lower() for item in (profile.get("sales_channels") or [])]
    ads: list[str] = []
    if any(token in labels for token in ("facebook pixel", "meta pixel", "facebook ads")):
        ads.append("Meta")
    if "google ads" in labels or "google adwords" in labels:
        ads.append("Google")
    if "tiktok pixel" in labels or "tiktok ads" in labels:
        ads.append("TikTok")
    signal_text = _signal_text(lead)
    if "no ad pixel" not in signal_text:
        if "meta and google" in signal_text or "google and meta" in signal_text:
            for name in ("Meta", "Google"):
                if name not in ads:
                    ads.append(name)
        elif "runs meta" in signal_text or "meta ads" in signal_text:
            if "Meta" not in ads:
                ads.append("Meta")
        elif "runs google" in signal_text or "google ads" in signal_text:
            if "Google" not in ads:
                ads.append("Google")
    social: list[str] = []
    social_names = {
        "tiktok": "TikTok", "youtube": "YouTube", "instagram": "Instagram",
        "pinterest": "Pinterest", "twitter": "Twitter",
    }
    for item in profile.get("contact_info") or []:
        if not isinstance(item, dict):
            continue
        label = social_names.get(str(item.get("type") or "").lower())
        try:
            delta = float(item.get("followers_30d") or 0)
        except (TypeError, ValueError):
            delta = 0
        if label and delta > 0 and label not in social:
            social.append(label)
    product = profile.get("most_recently_published_product")
    if not isinstance(product, dict):
        product = {}
    title = _fact_phrase(product.get("title"))
    month = _month_name(product.get("published_at") or profile.get("max_product_published_at"))
    new_product = ""
    if title and month:
        new_product = _no_digits(f"{title} in {month}")
    elif title:
        new_product = title
    elif month:
        new_product = f"a new product in {month}"
    platform_names = {
        "woocommerce": "WooCommerce", "magento": "Magento",
        "bigcommerce": "BigCommerce", "shopify": "Shopify",
    }
    raw_platform = _no_digits(str(profile.get("last_platform") or ""))
    previous = platform_names.get(raw_platform.lower(), raw_platform)
    moved = ""
    if previous:
        moved_month = _month_name(profile.get("last_platform_change_at"))
        moved = f"{previous} in {moved_month}" if moved_month else previous
    trust = profile.get("trustpilot") if isinstance(profile.get("trustpilot"), dict) else {}
    try:
        reviews = int((trust or {}).get("review_count") or 0)
    except (TypeError, ValueError):
        reviews = 0
    compact = labels.replace(".", "")
    review_app = ""
    for app_name, token in (
        ("Yotpo", "yotpo"), ("Judge.me", "judgeme"), ("Okendo", "okendo"),
        ("Stamped", "stamped"), ("Loox", "loox"),
    ):
        if token in compact:
            review_app = app_name
            break
    subscription = ""
    for app_name, token in (("Recharge", "recharge"), ("Smartrr", "smartrr"), ("Skio", "skio")):
        if token in compact:
            subscription = app_name
            break
    cats = lead.get("categories") or ""
    if isinstance(cats, (list, tuple)):
        cats = ", ".join(str(item) for item in cats[:3])
    category = _specific_category(cats) or _specific_category(lead.get("niche") or "")
    confidence = str(lead.get("amz_confidence") or lead.get("amazon_confidence") or "").strip().lower()
    absent_raw = lead.get("amazon_absent")
    if isinstance(absent_raw, str):
        absent = absent_raw.strip().lower() in {"1", "true", "yes"}
    else:
        absent = bool(absent_raw)
    signal = str(lead.get("recipe") or "").strip()
    if signal not in {"plan_upgrade", "replatform", "replatformed"}:
        signal = ""
    city = _no_digits(str(profile.get("city") or ""))
    if city and len(city.split()) > 3:
        city = ""
    return {
        "brand": str(lead.get("brand") or lead.get("domain") or "").strip() or "This brand",
        "retail": bool(str(profile.get("retailer_page") or "").strip() or str(profile.get("store_locator_page") or "").strip()),
        "amazon": (not absent) and ("amazon" in channels or confidence == "high"),
        "ads": ads,
        "social": social,
        "reviews": reviews > 0,
        "review_app": review_app,
        "subscription": subscription,
        "signal": signal,
        "new_product": new_product,
        "count_words": _count_words(profile.get("product_count")),
        "plus": "plus" in str(profile.get("plan") or "").lower(),
        "moved": moved,
        "city": city,
        "category": category,
        "klaviyo": "klaviyo" in labels.replace(".", ""),
    }


def _why_for(obs: dict[str, Any], *, retail: bool, amazon: bool, ads: bool, social: bool) -> str:
    if amazon and (ads or retail):
        return "that demand is already showing up in more than one place"
    if ads and not amazon:
        return "paid demand is already there to meet on Amazon"
    if social and retail:
        return "that kind of demand usually shows up on Amazon too"
    if social:
        return "that attention is worth meeting on Amazon"
    if retail:
        return "the product is already proven beyond the website"
    return "there is demand worth meeting on the marketplace side"


def _repeats(line: str, other: str) -> bool:
    words = clean(line).lower().split()
    hay = clean(other).lower()
    for size in (5, 4):
        for index in range(0, max(0, len(words) - size + 1)):
            chunk = " ".join(words[index:index + size])
            if chunk and chunk in hay:
                return True
    return False


def human_opener(obs: dict[str, Any]) -> tuple[str, list[str]]:
    """One or two sentences a person could have written. No negative Amazon hook."""

    brand = obs["brand"]
    ads = _ad_phrase(list(obs["ads"]))
    social = _join_names(list(obs["social"]))
    used: list[str] = []
    clause = ""
    why = ""
    if obs["retail"] and obs["amazon"] and ads:
        clause = f"in retail stores and on Amazon while running {ads}"
        why = _why_for(obs, retail=True, amazon=True, ads=True, social=False)
        used = ["retail stores", "Amazon", ads]
    elif obs["amazon"] and ads:
        clause = f"on Amazon while running {ads}"
        why = _why_for(obs, retail=False, amazon=True, ads=True, social=False)
        used = ["Amazon", ads]
    elif obs["retail"] and ads:
        clause = f"in retail stores while running {ads}"
        why = _why_for(obs, retail=True, amazon=False, ads=True, social=False)
        used = ["retail stores", ads]
    elif obs["social"] and obs["retail"]:
        clause = f"growing on {social} and sold in retail stores"
        why = _why_for(obs, retail=True, amazon=False, ads=False, social=True)
        used = [f"growing on {social}", "retail stores"]
    elif obs["social"]:
        clause = f"growing on {social}"
        why = _why_for(obs, retail=False, amazon=False, ads=False, social=True)
        used = [f"growing on {social}"]
    elif ads and obs["category"]:
        clause = f"running {ads} in {obs['category']}"
        why = _why_for(obs, retail=False, amazon=False, ads=True, social=False)
        used = [ads, obs["category"]]
    elif ads:
        clause = f"running {ads}"
        why = _why_for(obs, retail=False, amazon=False, ads=True, social=False)
        used = [ads]
    elif obs["retail"] and obs["category"]:
        clause = f"in retail stores, selling {obs['category']}"
        why = _why_for(obs, retail=True, amazon=False, ads=False, social=False)
        used = ["retail stores", obs["category"]]
    elif obs["retail"]:
        clause = "in retail stores"
        why = _why_for(obs, retail=True, amazon=False, ads=False, social=False)
        used = ["retail stores"]
    elif obs["reviews"] and obs["category"]:
        clause = f"with Trustpilot reviews in {obs['category']}"
        why = "shoppers are already talking about the products"
        used = ["Trustpilot reviews", obs["category"]]
    elif obs["reviews"]:
        clause = "with Trustpilot reviews"
        why = "shoppers are already talking about the products"
        used = ["Trustpilot reviews"]
    elif obs["new_product"] and (ads or obs["retail"] or obs["amazon"]):
        extra = ads or ("retail stores" if obs["retail"] else "Amazon")
        clause = f"adding {obs['new_product']} while running {extra}" if ads else f"adding {obs['new_product']} with {extra} already in place"
        why = "the catalog is moving at the same time as the demand"
        used = [obs["new_product"], extra]
    elif obs["plus"]:
        clause = "on Shopify Plus"
        why = "the store is in a build moment and the marketplace side has to keep up"
        used = ["Shopify Plus"]
    elif obs["moved"]:
        clause = f"move off {obs['moved']}"
        why = "listings and ads get rebuilt and Amazon gets a fresh look"
        used = [obs["moved"]]
    elif obs["count_words"] and obs["count_words"] != "zero" and obs["category"]:
        clause = f"with a catalog of {obs['count_words']} products in {obs['category']}"
        why = "that is a set a team can merchandise tightly"
        used = [f"{obs['count_words']} products", obs["category"]]
    elif obs["amazon"] and obs["category"]:
        clause = f"selling on Amazon in {obs['category']}"
        why = "price and the buy box are already part of how the number moves"
        used = ["Amazon", obs["category"]]
    elif obs["amazon"]:
        clause = "selling on Amazon"
        why = "price and the buy box are already part of how the number moves"
        used = ["Amazon"]
    elif obs["review_app"] and obs["category"]:
        clause = f"running {obs['review_app']} in {obs['category']}"
        why = "the store is set up to collect what shoppers say"
        used = [obs["review_app"], obs["category"]]
    elif obs["review_app"]:
        clause = f"running {obs['review_app']}"
        why = "the store is set up to collect what shoppers say"
        used = [obs["review_app"]]
    elif obs["subscription"] and obs["klaviyo"]:
        clause = f"running {obs['subscription']} subscriptions and Klaviyo"
        why = "repeat purchase and email demand are already in place"
        used = [f"{obs['subscription']} subscriptions", "Klaviyo"]
    elif obs["subscription"] and obs["category"]:
        clause = f"running {obs['subscription']} subscriptions in {obs['category']}"
        why = "repeat purchase is already part of the business"
        used = [f"{obs['subscription']} subscriptions", obs["category"]]
    elif obs["signal"] == "plan_upgrade" and obs["category"]:
        clause = f"upgrading the store plan in {obs['category']}"
        why = "the site is in a build moment and the marketplace side has to keep up"
        used = ["store plan upgrade", obs["category"]]
    elif obs["signal"] == "plan_upgrade":
        clause = "upgrading the store plan"
        why = "the site is in a build moment and the marketplace side has to keep up"
        used = ["store plan upgrade"]
    elif obs["signal"] in {"replatform", "replatformed"} and obs["category"]:
        clause = f"moving the store onto a new platform in {obs['category']}"
        why = "listings get rebuilt and Amazon gets a fresh look"
        used = ["platform move", obs["category"]]
    elif obs["signal"] in {"replatform", "replatformed"}:
        clause = "moving the store onto a new platform"
        why = "listings get rebuilt and Amazon gets a fresh look"
        used = ["platform move"]
    elif obs["new_product"] and obs["category"]:
        clause = f"adding {obs['new_product']} in {obs['category']}"
        why = "the catalog is still moving"
        used = [obs["new_product"], obs["category"]]
    elif obs["new_product"]:
        clause = f"adding {obs['new_product']}"
        why = "the catalog is still moving"
        used = [obs["new_product"]]
    elif obs["klaviyo"] and obs["category"]:
        clause = f"running Klaviyo in {obs['category']}"
        why = "email demand is already there to meet on Amazon"
        used = ["Klaviyo", obs["category"]]
    elif obs["klaviyo"]:
        clause = "running Klaviyo"
        why = "email demand is already there to meet on Amazon"
        used = ["Klaviyo"]
    elif obs["city"] and obs["category"]:
        clause = f"in {obs['city']}, selling {obs['category']}"
        why = "that is a category where Amazon demand tends to sit next to the website"
        used = [obs["city"], obs["category"]]
    elif obs["category"]:
        return "", []
    else:
        return "", []
    line = f"Saw {brand} {clause}, so {why}."
    usable = _usable(line, allow_amazon=True, max_words=MAX_WORDS)
    if usable:
        return usable, used
    shorter = _usable(f"Saw {brand} {clause}.", allow_amazon=True, max_words=MAX_WORDS)
    return (shorter, used) if shorter else ("", [])


def human_angle(obs: dict[str, Any], opener: str) -> str:
    """A second line that does not restate the opener and does not pitch 'Anata helps X grow'."""

    candidates: list[str] = []
    if obs["retail"] and "retail" not in opener.lower():
        candidates.append("Retail distribution is a sign the product can hold a price, which is what the buy box depends on.")
    if obs["ads"] and not any(name.lower() in opener.lower() for name in obs["ads"]):
        candidates.append("Ad spend works harder when the listing price holds.")
    if obs["social"] and not any(name.lower() in opener.lower() for name in obs["social"]):
        candidates.append("That social attention fades if the listing is not ready when someone searches the brand.")
    if obs["reviews"] and "trustpilot" not in opener.lower():
        candidates.append("Those reviews are the proof a listing needs when a shopper compares prices.")
    if obs["review_app"] and obs["review_app"].lower() not in opener.lower() and not obs["reviews"]:
        candidates.append("A review tool pays off when the listing is ready for the same shopper.")
    if obs["subscription"] and obs["subscription"].lower() not in opener.lower():
        candidates.append("Subscriptions only hold if the listing price stays steady for the next order.")
    if obs["signal"] == "plan_upgrade" and "plan" not in opener.lower():
        candidates.append("A plan upgrade is a build moment, and the marketplace listing has to keep up with the site.")
    elif obs["signal"] in {"replatform", "replatformed"} and "platform" not in opener.lower():
        candidates.append("A platform move is when the Amazon listing has to be rebuilt with the new site, not left behind.")
    elif obs["signal"] in {"replatform", "replatformed"}:
        candidates.append("The listing has to match the new site once that move is done.")
    if obs["new_product"] and obs["new_product"].split()[0].lower() not in opener.lower():
        if obs["ads"]:
            candidates.append("A new product is the moment the listing and the ads have to say the same thing.")
        else:
            candidates.append("A new product is the moment the listing has to match what the site just added.")
    if obs["plus"] and "plus" not in opener.lower():
        candidates.append("A Plus plan is a lot of store to run if the marketplace side is still manual.")
    if obs["moved"] and obs["moved"].split()[0].lower() not in opener.lower():
        candidates.append("A platform move is when the Amazon listing has to be rebuilt with the new site, not left behind.")
    if obs["count_words"] and obs["count_words"] not in opener.lower():
        candidates.append(f"A catalog of {obs['count_words']} products is a manageable set to keep priced.")
    if obs["amazon"] and "amazon" not in opener.lower():
        candidates.append("Being on Amazon means the buy box is part of the number, not a side project.")
    if obs["category"]:
        candidates.append(f"{obs['category'].capitalize()} shoppers compare fast, so the listing has to be ready when they look.")
        candidates.append("Shoppers in that category compare fast, so the listing has to be ready when they look.")
    if obs["klaviyo"] and "klaviyo" not in opener.lower():
        candidates.append("Email demand only pays off if the listing is ready when someone searches the brand.")
    candidates.append("Price and the buy box are where that demand gets kept or given away.")
    for line in candidates:
        if line.lower().startswith("anata helps"):
            continue
        if _repeats(line, opener):
            continue
        usable = _usable(line, allow_amazon="amazon" in line.lower(), max_words=ANGLE_MAX_WORDS)
        if usable:
            return usable
    return ""


def _compose(brand: str, phrases: list[str]) -> str:
    if len(phrases) == 1:
        return f"{brand} {phrases[0]}."
    if len(phrases) == 2:
        return f"{brand} {phrases[0]} and {phrases[1]}."
    return f"{brand} {', '.join(phrases[:-1])}, and {phrases[-1]}."


def rich_sentence(brand: str, phrases: list[str], *, allow_amazon: bool, max_words: int = MAX_WORDS) -> str:
    """The longest factual sentence that still passes the opener rules."""

    picked: list[str] = []
    for phrase in phrases:
        trial = picked + [phrase]
        if _usable(_compose(brand, trial), allow_amazon=allow_amazon, max_words=max_words):
            picked = trial
        if len(picked) >= 3:
            break
    if not picked:
        return ""
    return _usable(_compose(brand, picked), allow_amazon=allow_amazon, max_words=max_words)


def collect_facts(lead: dict[str, Any]) -> LeadFacts:
    """StoreLeads category, recipe signal, confirmed Amazon product, scraped phrase."""

    from outbound_pipeline import company_label

    profile = _profile_of(lead)
    brand = company_label(
        lead.get("brand") or lead.get("domain") or "",
        title=str(profile.get("title") or ""),
        domain=lead.get("domain") or "",
    )
    from outbound_pipeline import company_blocked_reason

    if company_blocked_reason(lead):
        return LeadFacts(brand, "", "", "", "", (), (), "", "", trial_offer_lead(lead))
    amazon_product = ""
    if amazon_confirmed(lead):
        amazon_product = _fact_phrase(_AMAZON_WORD.sub(" ", str(lead.get("amz_product") or "")))
    product = _scraped_product(lead)
    if product and amazon_product and product.lower() == amazon_product.lower():
        product = ""
    cats = lead.get("categories") or ""
    if isinstance(cats, (list, tuple)):
        cats = ", ".join(str(item) for item in cats[:3])
    category = _category_phrase(cats) or _category_phrase(lead.get("niche") or "")
    niche = _category_phrase(lead.get("niche") or "")
    extras = tuple(item for item in (niche,) if item and item != category)
    signal = str(lead.get("recipe") or "").strip()
    if signal not in RECIPE_FACTS:
        signal = ""
    rich: tuple[str, ...] = ()
    rich_opener = ""
    rich_angle = ""
    if rich_personalization_enabled(lead):
        from outbound_personalization import fact_sheet

        sheet = fact_sheet(lead)
        rich = tuple(sheet.facts)
    return LeadFacts(
        brand, amazon_product, product, category, signal, extras, rich, rich_opener, rich_angle,
        trial_offer_lead(lead),
    )


def _anchors(facts: LeadFacts) -> list[str]:
    found = [facts.amazon_product, facts.product, facts.category, *facts.extra_anchors]
    if facts.signal:
        found.append(RECIPE_FACTS[facts.signal])
        found.append(_ANGLE_AFTER[facts.signal])
    found.extend(facts.rich_phrases)
    return [item for item in found if item]


def _anchored(text: str, anchors: list[str]) -> bool:
    low = clean(text).lower()
    for anchor in anchors:
        piece = clean(anchor).lower()
        if len(piece) >= 3 and piece in low:
            return True
        words = piece.split()
        if len(words) > 2:
            leading = " ".join(words[:2])
            if len(leading) >= 3 and leading in low:
                return True
    return False


def _named(brand: str) -> str:
    return (brand or "").strip() or "This brand"


def _generic_rich_phrase(phrase: str) -> bool:
    low = " ".join(str(phrase or "").lower().split())
    if low in _GENERIC_RETAIL_FACTS or low.startswith("sells on "):
        return True
    if "retail stores" in low and "sold at " not in low:
        return True
    return False


def opener_line(facts: LeadFacts) -> str:
    """Deterministic opener from a product, a named store, or a recipe signal.

    A category label is not a sentence. An unnamed retail page is not a sentence.
    """

    brand = _named(facts.brand)
    if facts.rich_opener:
        return facts.rich_opener
    allow_name_amazon = bool(facts.amazon_product) and not facts.trial_offer
    if allow_name_amazon:
        line = _usable(f"{brand} lists {facts.amazon_product} on Amazon.", allow_amazon=True)
        if line:
            return line
    if facts.amazon_product and facts.trial_offer:
        line = _usable(f"{brand} sells {facts.amazon_product}.", allow_amazon=False)
        if line:
            return line
    phrases = [phrase for phrase in facts.rich_phrases if phrase and not _generic_rich_phrase(phrase)]
    if phrases:
        allow_amazon = (
            not facts.trial_offer
            and any("amazon" in phrase.lower() for phrase in phrases)
        )
        line = rich_sentence(brand, phrases, allow_amazon=allow_amazon)
        if line:
            return line
    if facts.product:
        line = _usable(f"{brand} sells {facts.product}.", allow_amazon=False)
        if line:
            return line
    if facts.signal:
        line = _usable(f"{brand} {RECIPE_FACTS[facts.signal]}.", allow_amazon=False)
        if line:
            return line
    return ""


def brand_angle_line(facts: LeadFacts) -> str:
    """Tie one known fact to how Anata helps. Category is the neutral fallback."""

    brand = _named(facts.brand)
    if facts.rich_angle:
        return facts.rich_angle
    allow_name_amazon = bool(facts.amazon_product) and not facts.trial_offer
    if allow_name_amazon:
        line = _usable(
            f"Anata helps {brand} grow {facts.amazon_product} on Amazon.",
            allow_amazon=True, max_words=ANGLE_MAX_WORDS,
        )
        if line:
            return line
    if facts.rich_phrases:
        allow_amazon = (
            not facts.trial_offer
            and any("amazon" in phrase.lower() for phrase in facts.rich_phrases)
        )
        line = _usable(
            f"Anata helps {brand} grow. The brand {facts.rich_phrases[0]}.",
            allow_amazon=allow_amazon, max_words=ANGLE_MAX_WORDS,
        )
        if line:
            return line
    if facts.product:
        line = _usable(
            f"Anata helps {brand} grow {facts.product}.",
            allow_amazon=False, max_words=ANGLE_MAX_WORDS,
        )
        if line:
            return line
    if facts.signal:
        line = _usable(
            f"Anata helps {brand} grow after {_ANGLE_AFTER[facts.signal]}.",
            allow_amazon=False, max_words=ANGLE_MAX_WORDS,
        )
        if line:
            return line
    if facts.category:
        line = _usable(
            f"Anata helps {facts.category} brands grow.",
            allow_amazon=False, max_words=ANGLE_MAX_WORDS,
        )
        if line:
            return line
    return ""


def fallback_line(
    brand: str,
    niche: str = "",
    reason: str = "",
    product: str = "",
    recipe: str = "",
) -> str:
    """Deterministic opener. Only a product or a recipe signal.

    ``reason`` is an internal note and is deliberately never shown to a prospect.
    ``product`` is an Amazon product the caller has already decided is confirmed.
    A category niche is not turned into a sentence. The line is empty when no
    product or signal survives the rules.
    """

    signal = (recipe or "").strip()
    if signal not in RECIPE_FACTS:
        signal = ""
    amazon_product = _fact_phrase(_AMAZON_WORD.sub(" ", product)) if product else ""
    return opener_line(LeadFacts(
        brand=(brand or "").strip(),
        amazon_product=amazon_product,
        category=_category_phrase(niche),
        signal=signal,
    ))


def _build_prompt(facts: LeadFacts, *, closing: str) -> str:
    lines = [f"Brand: {facts.brand or 'This brand'}"]
    if facts.trial_offer:
        lines.append(
            "Trial Offer: do not claim or assume this brand already sells on Amazon. "
            "Never write 'your Amazon store', 'your listings', or 'already on Amazon'. "
            "The brand angle may mention Amazon as an opportunity for new shoppers."
        )
        if facts.amazon_product:
            lines.append(f"Product: {facts.amazon_product}")
    elif facts.amazon_product:
        lines.append(f"Amazon product: {facts.amazon_product}")
    if facts.product:
        lines.append(f"Product: {facts.product}")
    if facts.signal:
        lines.append(f"Recent signal: {RECIPE_FACTS[facts.signal]}")
    if facts.category:
        lines.append(f"Category label, not a product: {facts.category}")
    for phrase in facts.rich_phrases:
        lines.append(f"Fact: {phrase}")
    return "\n".join(lines) + "\n\n" + closing


def _call_anthropic(prompt: str, api_key: str, *, system: str = _SYSTEM) -> str:
    from sales_support_agent.services.llm_client import llm_client

    client = llm_client(caller="outbound_openers", api_key=api_key)
    msg = client.messages.create(
        model=MODEL, max_tokens=120, system=system,
        messages=[{"role": "user", "content": prompt}],
    )
    return "".join(getattr(b, "text", "") for b in msg.content)


def _domain_key(lead: dict[str, Any]) -> str:
    domain = str(lead.get("domain") or "").strip().lower()
    if not domain:
        return ""
    if rich_personalization_enabled(lead):
        return domain + "|rich"
    return domain


def _resolve_llm(
    llm: Optional[Callable[[str], str]],
    api_key: Optional[str],
    *,
    system: str,
) -> Optional[Callable[[str], str]]:
    key = api_key if api_key is not None else os.getenv("ANTHROPIC_API_KEY", "")
    from sales_support_agent.services.llm_client import llm_available

    if llm is None and llm_available(api_key=key):
        return lambda prompt: _call_anthropic(prompt, key, system=system)  # noqa: E731
    return llm


def _ensure_rich(
    lead: dict[str, Any],
    *,
    llm: Optional[Callable[[str], str]],
    api_key: Optional[str],
) -> None:
    """Fill both caches from one model pass. The flag-off path never calls this."""

    domain = _domain_key(lead)
    if domain and domain in _OPENERS_BY_DOMAIN and domain in _ANGLES_BY_DOMAIN:
        return
    from outbound_personalization import generate

    opener, angle, source = generate(lead, llm=llm, api_key=api_key)
    if domain:
        _OPENERS_BY_DOMAIN[domain] = (opener, source if opener else "none")
        _ANGLES_BY_DOMAIN[domain] = (angle, source if angle else "none")


def _from_model(
    llm: Callable[[str], str],
    prompt: str,
    *,
    allow_amazon: bool,
    anchors: list[str],
    max_words: int,
    trial_offer: bool = False,
    concrete: Optional[list[str]] = None,
) -> tuple[str, str]:
    """Return a clean line, or ('', reason) after one regeneration.

    ``concrete`` is the shopper-visible things the opener must name. When it
    is provided, a category label is not enough to keep the line.
    """

    why = ""
    for _attempt in range(2):
        raw = llm(prompt)
        ok, why = validate(
            raw, allow_amazon=allow_amazon, max_words=max_words, trial_offer=trial_offer,
        )
        if ok and concrete is not None and not _anchored(raw, concrete):
            ok, why = False, "does not name a concrete product"
        elif ok and concrete is None and not _anchored(raw, anchors):
            ok, why = False, "not grounded in a known fact"
        if ok:
            return clean(raw), ""
        logger.info("[outbound-openers] rejected AI line (%s)", why)
        prompt = (prompt + f"\n\nYour last line was rejected ({why}). "
                  "Write a different statement that follows every rule and names a concrete fact.")
    return "", why or "rejected"


def concrete_phrases(lead: dict[str, Any], facts: LeadFacts) -> list[str]:
    """Product, flavor, story, named store, or review theme already on the lead."""

    from outbound_pipeline import company_blocked_reason

    if company_blocked_reason(lead):
        return []
    found: list[str] = []
    categories = _category_phrases(lead)
    broad = {item.lower() for item in CATEGORY_WORDS.values()}

    def add(value: Any) -> None:
        text = clean(str(value or "")).strip(" .")
        words = text.split()
        if len(words) > 8:
            text = " ".join(words[:6])
        if len(text) < 3:
            return
        lowered = text.lower()
        if lowered in categories or lowered in broad or lowered in _GENERIC_RETAIL_FACTS:
            return
        if is_page_boilerplate(text, str(lead.get("brand") or "")):
            return
        if text not in found:
            found.append(text)

    if facts.amazon_product:
        add(facts.amazon_product)
    if facts.product:
        add(facts.product)
    profile = _profile_of(lead)
    product = profile.get("most_recently_published_product")
    if not isinstance(product, dict):
        product = lead.get("most_recently_published_product")
    if isinstance(product, dict):
        add(product.get("title"))
    add(profile.get("description") or lead.get("description"))
    for name in named_retailers(profile, domain=str(lead.get("domain") or "")):
        add(name)
    for phrase in facts.rich_phrases:
        low = phrase.lower()
        if low.startswith("recent product:"):
            add(phrase.split(":", 1)[1])
        elif " sold at " in f" {low}" or low.startswith("sold at "):
            add(phrase.split(" at ", 1)[-1])
        elif "reviews" in low:
            add("reviews")
        elif _generic_rich_phrase(phrase) or low.endswith("followers") or " has shoppers" in f" {low}":
            continue
        elif low.startswith("offers a subscribe") or low == "sells wholesale":
            continue
        else:
            add(phrase)
    trust = profile.get("trustpilot") if isinstance(profile.get("trustpilot"), dict) else {}
    try:
        reviews = int((trust or {}).get("review_count") or 0)
        average = float((trust or {}).get("avg_rating") or 0)
    except (TypeError, ValueError):
        reviews, average = 0, 0.0
    if reviews > 0 and average >= 4.5:
        add("reviews")
    return found


def _sendable(line: str, lead: dict[str, Any], concrete: list[str]) -> bool:
    """True when the line names a real fact and is allowed to reach Instantly."""

    if not line or not concrete or not _anchored(line, concrete):
        return False
    if generic_copy_reason(line) or opener_hold_reason(line, lead):
        return False
    return True


def write_opener(
    lead: dict[str, Any],
    contact: Optional[dict[str, Any]] = None,
    *,
    llm: Optional[Callable[[str], str]] = None,
    api_key: Optional[str] = None,
) -> tuple[str, str]:
    """Return (line, source) where source is "ai", "fallback", or "none".

    One line per company domain. A second contact at the same domain reuses it.
    ``contact`` is ignored so the line is not rewritten per person. The line
    names one concrete shopper-visible fact. It is empty when no such fact
    exists, which the enrich path records as a skip and does not push.
    """

    if rich_personalization_enabled(lead):
        _ensure_rich(lead, llm=llm, api_key=api_key)
    domain = _domain_key(lead)

    def finish(line: str, source: str) -> tuple[str, str]:
        result = (line, source)
        if domain:
            _OPENERS_BY_DOMAIN[domain] = result
        return result

    facts = collect_facts(lead)
    concrete = concrete_phrases(lead, facts)
    cached = _OPENERS_BY_DOMAIN.get(domain) if domain else None
    if cached and cached[0] and _sendable(cached[0], lead, concrete):
        return cached
    if facts.rich_opener and _sendable(facts.rich_opener, lead, concrete):
        return finish(facts.rich_opener, "fallback")
    if not facts.solid and not concrete:
        return finish("", "none")
    already_modeled = bool(cached) and rich_personalization_enabled(lead)
    model = None if already_modeled else _resolve_llm(llm, api_key, system=_SYSTEM)
    if model is not None:
        prompt = _build_prompt(
            facts,
            closing=(
                "Write one sentence of about 20 words, never more than 25. "
                "Name one concrete product, flavor, story, store name, or review theme "
                "from the facts. If none is listed, write nothing. "
                "Write the opening statement now."
            ),
        )
        try:
            line, _why = _from_model(
                model, prompt,
                allow_amazon=bool(facts.amazon_product) and not facts.trial_offer,
                anchors=_anchors(facts), max_words=MAX_WORDS,
                trial_offer=facts.trial_offer,
                concrete=concrete,
            )
            if line and _sendable(line, lead, concrete):
                return finish(line, "ai")
        except Exception:  # noqa: BLE001 - never block a lead on the AI
            logger.exception("[outbound-openers] AI call failed, using a plain fact")
    plain, _angle = shopper_lines(lead)
    if plain and _sendable(plain, lead, concrete):
        return finish(plain, "fallback")
    line = opener_line(facts)
    if line and _sendable(line, lead, concrete):
        return finish(line, "fallback")
    return finish("", "none")


def write_brand_angle(
    lead: dict[str, Any],
    contact: Optional[dict[str, Any]] = None,
    *,
    llm: Optional[Callable[[str], str]] = None,
    api_key: Optional[str] = None,
) -> tuple[str, str]:
    """Return (line, source) for the Instantly ``brandAngle`` variable.

    The line ties one known fact to how Anata helps the brand grow. A category
    is the neutral fallback. The line is empty when we do not have a category
    either. ``contact`` is ignored so every person at the company shares it.
    """

    if rich_personalization_enabled(lead):
        _ensure_rich(lead, llm=llm, api_key=api_key)
    domain = _domain_key(lead)
    if domain and domain in _ANGLES_BY_DOMAIN:
        return _ANGLES_BY_DOMAIN[domain]

    facts = collect_facts(lead)

    def finish(line: str, source: str) -> tuple[str, str]:
        result = (line, source)
        if domain:
            _ANGLES_BY_DOMAIN[domain] = result
        return result

    if not facts.solid:
        return finish("", "none")
    if facts.rich_angle:
        return finish(facts.rich_angle, "fallback")
    model = _resolve_llm(llm, api_key, system=_ANGLE_SYSTEM)
    if model is not None:
        prompt = _build_prompt(
            facts,
            closing=(
                "Write one sentence of about 15 words, never more than 20. "
                "On a Trial Offer you may mention Amazon as an opportunity for new shoppers. "
                "Do not claim they already sell there. Write the growth line now."
            ),
        )
        try:
            line, why = _from_model(
                model, prompt,
                allow_amazon=bool(facts.amazon_product) and not facts.trial_offer,
                anchors=_anchors(facts), max_words=ANGLE_MAX_WORDS,
                trial_offer=facts.trial_offer,
            )
            if line:
                return finish(line, "ai")
            if why == "too long":
                return finish("", "none")
        except Exception:  # noqa: BLE001 - never block a lead on the AI
            logger.exception("[outbound-openers] brand angle AI call failed, using fallback")
    line = brand_angle_line(facts)
    return finish(line, "fallback" if line else "none")


# A held contact stays out of Instantly until a later push can replace this.
OPENER_HOLD_PREFIX = "held:opener:"

# Backend facts are recipe context. They are not a cold-email sentence.
_BACKEND_COPY = re.compile(
    r"\b(?:shopify|woocommerce|woo commerce|magento|bigcommerce|big commerce|"
    r"klaviyo|yotpo|okendo|judge\.?me|loox|recharge|pixel|shopify plus|"
    r"upgraded|platform|app install|growth app|store plan|"
    r"moved (?:their|from|off)|previously ran on)\b",
    re.I,
)
_COPY_URL = re.compile(r"https?://|www\.", re.I)
_CATEGORY_SENTENCE = re.compile(r"^(.+?) sells ([^.]+)\.$", re.I)
_ANATA_CATEGORY = re.compile(r"^anata helps .+ brands grow\.?$", re.I)
_SAW_CATEGORY = re.compile(r"^saw .+ in .+\.", re.I)


def _category_phrases(lead: dict[str, Any]) -> set[str]:
    """Category wording the filler sentence would use. Not a product name."""

    found: set[str] = set()
    raw_values = (
        lead.get("categories"), lead.get("category"), lead.get("niche"),
    )
    for raw in raw_values:
        for phrase in (_category_phrase(raw), _specific_category(raw)):
            text = phrase.lower().replace("&", "and")
            text = " ".join(text.split())
            if text:
                found.add(text)
    try:
        from outbound_instantly_push import friendly_category

        friendly = friendly_category(lead.get("categories") or lead.get("category") or "")
    except Exception:  # noqa: BLE001
        friendly = ""
    if friendly:
        found.add(" ".join(friendly.lower().replace("&", "and").split()))
    return found


def _normalized_sale(opener: str) -> str:
    match = _CATEGORY_SENTENCE.fullmatch((opener or "").strip())
    if not match:
        return ""
    return " ".join(match.group(2).lower().replace("&", "and").split())


def opener_hold_reason(opener: str, lead: Optional[dict[str, Any]] = None) -> str:
    """Why this opener must not be sent. Empty when it can go to Instantly.

    A category sentence, a URL, or a backend detail is not a shopper-visible
    fact. The contact stays unpushed and a later run can replace the line.
    """

    text = (opener or "").strip()
    if not text:
        return "no shopper-visible opener"
    generic = generic_copy_reason(text)
    if generic:
        return f"opener is {generic}"
    if "shoppers already compare" in text.lower() or "a category where" in text.lower():
        return "opener is a category filler with no shopper-visible fact"
    if _COPY_URL.search(text):
        return "opener contains a url"
    if _BACKEND_COPY.search(text):
        return "opener names backend technology instead of a shopper-visible fact"
    if _ANATA_CATEGORY.match(text):
        return "opener is a category filler with no shopper-visible fact"
    if _SAW_CATEGORY.match(text) and "category where" in text.lower():
        return "opener is a category filler with no shopper-visible fact"
    sold = _normalized_sale(text)
    brand = str((lead or {}).get("brand") or "")
    if sold and is_page_boilerplate(sold, brand):
        return "opener is page boilerplate with no shopper-visible fact"
    broad = {value.lower() for value in CATEGORY_WORDS.values()}
    broad.update({"food & beverage", "food and drink", "food & drink"})
    if sold and (sold in _category_phrases(lead or {}) or sold in broad):
        return "opener is a category filler with no shopper-visible fact"
    if len(text.split()) > MAX_WORDS:
        return "opener is longer than 25 words"
    if trial_offer_lead(lead) and trial_amazon_claim(text):
        return "trial offer opener assumes the brand already sells on Amazon"
    return ""


def angle_hold_reason(angle: str, lead: Optional[dict[str, Any]] = None) -> str:
    """Why this brand angle must not be sent. Empty when it can, or when it is blank.

    A line over 20 words is held. A short category slogan is still blanked by
    ``angle_is_filler`` and does not hold the contact by itself.
    """

    text = (angle or "").strip()
    if not text:
        return ""
    if len(text.split()) > ANGLE_MAX_WORDS:
        return "brand angle is longer than 20 words"
    if trial_offer_lead(lead) and trial_amazon_claim(text):
        return "trial offer angle assumes the brand already sells on Amazon"
    return ""


def angle_is_filler(angle: str) -> bool:
    """True when brandAngle would ship a category slogan, a URL, or backend tech."""

    text = (angle or "").strip()
    if not text:
        return False
    if generic_copy_reason(text):
        return True
    if _ANATA_CATEGORY.match(text) or _COPY_URL.search(text) or _BACKEND_COPY.search(text):
        return True
    return False


def _shopper_sentence(brand: str, fact: str) -> str:
    """One email sentence from a shopper-visible fact. Empty when it is not one."""

    raw = " ".join((fact or "").split())
    low = raw.lower()
    if not raw or is_page_boilerplate(raw, brand):
        return ""
    if low.startswith("sells on ") or low.startswith("has shoppers") or low.startswith("offers a subscribe"):
        return ""
    if low.startswith("recent product:"):
        title = raw.split(":", 1)[1].strip()
        if is_page_boilerplate(title, brand):
            return ""
        return _usable(f"{brand} sells {title}.", allow_amazon=False)
    if "reviews average" in low or low.startswith("reviews "):
        return _usable(f"{brand} {raw}.", allow_amazon=False)
    if low.startswith("sold at ") or low.startswith("is sold at "):
        name = raw.split(" at ", 1)[-1].strip()
        return _usable(f"{brand} is sold at {name}.", allow_amazon=False)
    if low in _GENERIC_RETAIL_FACTS or "retail stores" in low:
        return ""
    if " bio:" in low or low.endswith("followers"):
        return _usable(f"{brand} {raw}.", allow_amazon=False)
    if raw and not _BACKEND_COPY.search(raw) and not _COPY_URL.search(raw):
        sentence = raw if raw.endswith(".") else f"{raw}."
        if sentence.lower().startswith(brand.lower()):
            return _usable(sentence, allow_amazon=False)
        return _usable(f"{brand} sells {sentence[0].lower() + sentence[1:]}", allow_amazon=False)
    return ""


def shopper_lines(lead: dict[str, Any]) -> tuple[str, str]:
    """(opener, angle) using only products, story, reviews, retail, or social.

    Category labels, platforms, plans, apps, pixels, and URLs are not used.
    Both strings are empty when the record has no shopper-visible fact.
    """

    from outbound_personalization import fact_sheet
    from outbound_pipeline import company_label

    sheet = fact_sheet(lead)
    if sheet.blocked or not sheet.facts:
        return "", ""
    profile = lead.get("store_profile") if isinstance(lead.get("store_profile"), dict) else {}
    brand = _named(company_label(
        lead.get("brand") or sheet.brand or "",
        title=str(profile.get("title") or ""),
        domain=lead.get("domain") or "",
    ))
    opener = ""
    angle = ""
    preferred = []
    rest = []
    for fact in sheet.facts:
        low = fact.lower()
        if low in _GENERIC_RETAIL_FACTS or low.startswith("sells on ") or "retail stores" in low:
            continue
        if (
            low.startswith("recent product")
            or low.startswith("sold at ")
            or "reviews average" in low
            or " bio:" in low
            or low.endswith("followers")
        ):
            preferred.append(fact)
        else:
            rest.append(fact)
    for fact in preferred + rest:
        sentence = _shopper_sentence(brand, fact)
        if not sentence or _BACKEND_COPY.search(sentence) or _COPY_URL.search(sentence):
            continue
        if opener_hold_reason(sentence, lead):
            continue
        if not opener:
            opener = sentence
            continue
        if sentence.lower() != opener.lower():
            angle = sentence
            break
    if angle_is_filler(angle):
        angle = ""
    return opener, angle
