"""One opening line per company, plus a short brand angle (replaces Clay's AI column).

``opener`` fills ``{{personalization}}``. ``brand_angle`` is pushed as the
Instantly custom variable ``brandAngle``. Both are one shopper-visible idea:
a product, the brand story, reviews, a retail store, or a social presence.
Platform, plan, app, pixel, and change-date facts can choose who is emailed.
They never appear in the copy. If the model is missing, errors, or writes
something that breaks a rule, the deterministic idea is used. When there is
no shopper-visible fact, the field is left empty.

Hard rules, enforced in code not just in the prompt:
  * No em dashes, en dashes, or spaced hyphens (`` - ``).
  * No digits, so a made-up number can never reach a prospect.
  * No links, no email addresses, no cliches, no questions.
  * No category slugs, no "stood out" template, no raw category codes.
  * Only the amz_on segment may say the brand is on Amazon.
  * A Trial Offer opener and angle must not mention Amazon. The angle says
    new shoppers.
  * No backend or consultant language: app installs, automation, bids,
    direct-to-consumer, platform names or moves, plan upgrades, pixels,
    or change dates.
  * One sentence. The opener aims for about 20 words and never passes 25.
    The brand angle aims for about 15 words and never passes 20.
    A line outside that range is regenerated once, then replaced by the
    deterministic idea when a shopper-visible fact exists.
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from typing import Any, Callable, Optional

from outbound_product_facts import natural_product_phrase, stuffed_copy_reason

logger = logging.getLogger(__name__)

MODEL = "claude-haiku-4-5-20251001"
# {{personalization}} aims for about 20 words. 25 is the hard max.
MAX_WORDS = 25
# {{brandAngle}} aims for about 15 words. 20 is the hard max.
ANGLE_MAX_WORDS = 20
# A shorter line is a fragment, not an idea.
MIN_WORDS = 6
# "Anata // Trial Offer // Oct 2026". Same id as the Instantly push constant.
TRIAL_OFFER_CAMPAIGN_ID = "e5f5046b-07e4-41ad-a395-79cf8766ccd0"

_COPY_TIC = re.compile(
    r"\bsmall set\b|\beasy to picture\b|\brecent\b|\bi noticed\b|"
    r"\bone clear start\b|\bsimple place to start\b",
    re.I,
)


def copy_tic(text: str) -> str:
    """A repeated tic the model must not send, or empty."""

    if _COPY_TIC.search(text or ""):
        return "repeated opener tic"
    return ""


def opening_words(text: str, count: int = 3) -> str:
    return " ".join((text or "").lower().split()[:count])


def closing_clause(text: str) -> str:
    """The last clause, which is what makes a batch sound like one template."""

    raw = (text or "").strip().rstrip(".").lower()
    if "," in raw:
        return raw.rsplit(",", 1)[-1].strip()
    words = raw.split()
    return " ".join(words[-6:])


def variety_clash(
    text: str,
    prior: list[str],
    *,
    angle: str = "",
    angles: Optional[list[str]] = None,
) -> str:
    """Reject a third opener start or a third angle shape in one batch.

    The first word and the first three words each count. Two "The ..."
    openers are allowed. A third is not. Angle shape is the first four words.
    """

    if text and prior:
        first = opening_words(text, 1)
        opening = opening_words(text, 3)
        if first and sum(1 for old in prior if opening_words(old, 1) == first) >= 2:
            return "repeats an opening"
        if opening and sum(1 for old in prior if opening_words(old, 3) == opening) >= 2:
            return "repeats an opening"
    if angle and angles:
        shape = opening_words(angle, 4)
        if shape and sum(1 for old in angles if opening_words(old, 4) == shape) >= 2:
            return "repeats an angle"
        low = angle.lower()
        for motif in ("main image", "clear product", "center on", "focus on", "clear enough"):
            if motif in low and sum(1 for old in angles if motif in old.lower()) >= 2:
                return "repeats an angle"
    return ""


def brand_named_twice(opener: str, angle: str, brand: str) -> bool:
    """True when both lines say the brand. One of them should leave it out."""

    tokens = [token for token in re.findall(r"[a-z0-9]+", (brand or "").lower()) if len(token) > 3]
    if not tokens:
        return False

    def hit(text: str) -> bool:
        folded = re.sub(r"[^a-z0-9]+", " ", (text or "").lower())
        return any(re.search(rf"\b{re.escape(token)}\b", folded) for token in tokens)

    return hit(opener) and hit(angle)


def missing_possessive(text: str, brand: str) -> bool:
    """True for ``Paramo Snacks baked`` where the brand needs an apostrophe."""

    name = " ".join((brand or "").split())
    if len(name.split()) < 2:
        return False
    return bool(re.search(rf"\b{re.escape(name)}\s+[a-z]+ed\b", text or "", re.I))


BANNED_PHRASES = (
    "hope this finds you", "hope you're well", "hope you are well", "game-changing",
    "game changing", "revolutionary", "cutting-edge", "cutting edge", "synergy",
    "i came across your", "touch base", "circle back", "reaching out",
    # Template tics the model fell into on the first live run.
    "i noticed", "we noticed", "small set", "easy to picture",
    "one clear start", "simple place to start",
    "building something", "something special",
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
# "on Amazon" itself says the brand is there: catalog on Amazon, live on Amazon.
_TRIAL_AMAZON_CLAIM = re.compile(
    r"\byour amazon\b"
    r"|\btheir amazon\b"
    r"|\byour listings?\b"
    r"|\balready on amazon\b"
    r"|\blive on amazon\b"
    r"|\bon amazon\b"
    r"|\b(?:sells|selling|sold) on amazon\b"
    r"|\b(?:lists|listed) (?:[\w']+ ){0,8}on amazon\b"
    r"|\b(?:is|are|was|were) on amazon\b"
    r"|\breorders? on amazon\b",
    re.I,
)
# Backend and consultant language. Platform data may route the lead. It must
# not be what the prospect reads.
BACKEND_LANGUAGE = re.compile(
    r"\b(?:app installs?|installed an? (?:growth )?app|growth app|"
    r"automation|automated|automatically|"
    r"bid adjustments?|bid management|bids?|"
    r"real-time optimization|real time optimization|"
    r"direct[- ]to[- ]consumer|direct consumer|\bdtc\b|"
    r"shopify|woocommerce|woo commerce|magento|bigcommerce|big commerce|"
    r"klaviyo|yotpo|okendo|judge\.?me|loox|recharge|skio|smartrr|"
    r"(?:facebook|meta|tiktok|tracking|ad)\s+pixels?|shopify plus|plan upgrades?|store plan|upgraded|"
    r"platforms?|replatform|previously ran|moved)\b"
    r"|\b(?:in|last|this|since|during)\s+"
    r"(?:january|february|march|april|may|june|july|august|september|"
    r"october|november|december)\b",
    re.I,
)
_STOOD_OUT = re.compile(r"\bstood out\b|\bwhile i was looking at\b", re.I)
_REPR = re.compile(r"\[|\]|\{|\}|\b(?:none|nan|null)\b|\{\{", re.I)
_QUESTION_START = re.compile(r"^(?:is|are|do|does|did|can|could|would|will)\b", re.I)
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
    "marketing lead at a consumer brand. Write ONE sentence, not a question, of "
    "about 20 words and never more than 25. Suggest one specific idea from a "
    "shopper-visible fact only: a product, the brand story, reviews, a retail "
    "store, or a social presence. Never mention app installs, automation, bids, "
    "direct-to-consumer, platform names, platform moves, plan upgrades, pixels, "
    "or change dates. Never print a category slug or the words 'stood out'. "
    "Only say the brand is on Amazon when the facts say the segment is amz_on. "
    "If the facts say this is a Trial Offer, do not mention Amazon at all. "
    "No numbers. No links. No em dashes. No spaced hyphens. No greeting, no sign-off."
)

_ANGLE_SYSTEM = (
    "You write one sentence for a cold email from Abby at Anata. Suggest one "
    "specific idea from a shopper-visible fact. Use only those facts. Aim for "
    "about 15 words and never more than 20. Do not start with 'Anata helps'. "
    "Only say the brand is on Amazon when the facts say the segment is amz_on. "
    "A Trial Offer angle says new shoppers and does not mention Amazon. "
    "Never write 'your Amazon store', 'your listings', 'on Amazon', or "
    "'already on Amazon' for a Trial Offer. "
    "Never mention app installs, automation, bids, direct-to-consumer, platform "
    "names, platform moves, plan upgrades, pixels, or change dates. "
    "No numbers. No links. No questions. No em dashes. No spaced hyphens. "
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
    t = t.replace("—", ", ").replace("–", ", ").replace("--", ", ")
    t = re.sub(r"\s+-\s+", ", ", t)
    t = re.sub(r"\s+", " ", t)
    t = re.sub(r"\s+,", ",", t)
    t = re.sub(r",\s*,", ",", t)
    return t


def amazon_presence_allowed(lead: Optional[dict[str, Any]]) -> bool:
    """Only the amz_on segment may say the brand is on Amazon."""

    if not lead or trial_offer_lead(lead):
        return False
    recipe = str(lead.get("recipe") or "").strip().lower().replace("-", " ").replace("_", " ")
    return recipe == "amz on"


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
    min_words: int = MIN_WORDS,
    trial_offer: bool = False,
    role: str = "opener",
) -> tuple[bool, str]:
    raw = (text or "").strip()
    if "—" in raw or "–" in raw or " - " in raw:
        return False, "em dash"
    if _REPR.search(raw) or "  " in raw:
        return False, "repr or placeholder"
    if _STOOD_OUT.search(raw):
        return False, "stood out template"
    if _SLUG.search(raw):
        return False, "slug"
    if "?" in raw or _QUESTION_START.search(raw):
        return False, "question"
    if BACKEND_LANGUAGE.search(raw):
        return False, "backend language"
    if trial_offer and _AMAZON_WORD.search(raw):
        return False, "claims the brand already sells on Amazon"
    if not trial_offer and not allow_amazon and _AMAZON_WORD.search(raw):
        return False, "claims Amazon without a confirmed match"
    if len(raw.split()) > max_words or len(raw) > 280:
        return False, "too long"
    if role == "angle" and raw.lower().startswith("anata helps"):
        return False, "anata helps"
    if re.search(r"https?://|www\.|@", raw):
        return False, "contains a link or address"
    stuffed = stuffed_copy_reason(raw)
    if stuffed:
        return False, stuffed
    tic = copy_tic(raw)
    if tic:
        return False, tic
    t = clean(raw)
    if not t:
        return False, "empty"
    if len(t.split()) < min_words:
        return False, "too short"
    if sentence_count(t) > 1:
        return False, "more than one sentence"
    if re.search(r"\d", t):
        return False, "contains a number"
    low = t.lower()
    for phrase in BANNED_PHRASES:
        if phrase in low:
            return False, f"cliche: {phrase}"
    return True, ""


def plain_category(value: Any) -> str:
    """Turn category slugs and path fragments into words."""

    from outbound_personalization import as_text

    seen: list[str] = []
    for piece in re.split(r"[,/|]", as_text(value)):
        slug = piece.strip().lower().replace("-", "_").replace(" ", "_").strip("_")
        if not slug or slug in {"and", "the"}:
            continue
        words = CATEGORY_WORDS.get(slug, slug.replace("_", " "))
        if words and words not in seen:
            seen.append(words)
    return ", ".join(seen[:3])


def _usable(
    line: str,
    *,
    allow_amazon: bool,
    max_words: int = MAX_WORDS,
    min_words: int = MIN_WORDS,
) -> str:
    ok, _why = validate(
        line, allow_amazon=allow_amazon, max_words=max_words, min_words=min_words,
    )
    return clean(line) if ok else ""


def _fact_phrase(value: Any) -> str:
    """A short noun phrase we can actually stand behind. Empty when it is not."""

    raw = clean(str(value or "")).strip(" .")
    if not raw or _AMAZON_WORD.search(raw):
        return ""
    if len(raw.split()) > 8 or len(raw) > 60:
        return ""
    return _usable(raw, allow_amazon=False, min_words=1)


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


def template_fallback_enabled(lead: Optional[dict[str, Any]] = None) -> bool:
    """Deterministic opener templates are off unless a test or env turns them on."""

    if isinstance(lead, dict) and lead.get("template_fallback"):
        return True
    return (os.getenv("OUTBOUND_OPENER_TEMPLATES") or "").strip().lower() in {"1", "true", "yes", "on"}


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

    if str(profile.get("retailer_page") or "").strip() or str(profile.get("store_locator_page") or "").strip():
        phrases.append("is sold in retail stores")

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
    """Retired. Ideas are built by idea_lines from shopper-visible facts only."""

    return "", []
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
        line = (
            f"Saw {brand} in {obs['category']}. "
            "That is a category where Amazon demand tends to sit next to the website."
        )
        usable = _usable(line, allow_amazon=True, max_words=MAX_WORDS)
        return (usable, [obs["category"]]) if usable else ("", [])
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

    from outbound_pipeline import clean_brand_name

    brand = clean_brand_name(lead.get("brand") or lead.get("domain") or "")
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
    # A recipe signal chooses who is emailed. It is not a phrase the line must repeat.
    found = [facts.amazon_product, facts.product, facts.category, *facts.extra_anchors]
    found.extend(
        phrase for phrase in facts.rich_phrases if not BACKEND_LANGUAGE.search(phrase)
    )
    return [item for item in found if item]


def _anchored(text: str, anchors: list[str]) -> bool:
    low = clean(text).lower()
    for anchor in anchors:
        piece = clean(anchor).lower()
        if len(piece) >= 3 and piece in low:
            return True
    return False


def _named(brand: str) -> str:
    return (brand or "").strip() or "This brand"


def _possessive(brand: str) -> str:
    text = (brand or "").strip()
    if not text:
        return "This brand's"
    if text.endswith("s"):
        return f"{text}'"
    return f"{text}'s"


def _clip_words(text: str, limit: int) -> str:
    words = clean(text).strip(" .,|&").split()[:limit]
    fillers = {
        "and", "or", "the", "a", "an", "of", "for", "to", "with", "by",
        "meets", "&", "free",
    }
    while words and (
        words[-1].lower().strip(".,&+") in fillers or words[-1] in {"&", ",", "|", "+"}
    ):
        words.pop()
    return " ".join(words)


def _shopper_phrase(text: str, brand: str) -> str:
    """A product or story phrase with the brand, pipes, and junk titles removed."""

    import html as html_lib

    raw = html_lib.unescape(str(text or ""))
    raw = (
        raw.replace("®", " ").replace("™", " ").replace("©", " ")
        .replace("&amp;", " and ").replace("+", " ")
    )
    if not raw or BACKEND_LANGUAGE.search(raw) or _SLUG.search(raw):
        return ""
    if re.search(
        r"\b(?:wines?|alcohol|beer|spirits|liquor|cannabis|marijuana|cbd|tobacco|vapes?)\b",
        raw, re.I,
    ):
        return ""
    if re.search(r"[àâäéèêëïîôùûüç]", raw, re.I):
        return ""
    brand_folded = re.sub(r"[^a-z0-9]+", " ", brand.lower()).strip()
    parts = [
        clean(part).strip(" .,")
        for part in re.split(r"\s*\|\s*|\s+[-–—]\s+", raw)
        if clean(part).strip(" .,")
    ]
    if not parts:
        parts = [clean(raw).strip(" .,")]
    kept: list[str] = []
    generic = {
        "home", "shop", "store", "official", "official site", "welcome", "site",
        "online", "us", "usa", "health", "beauty", "apparel", "food", "sale", "new",
    }
    for part in parts:
        folded = re.sub(r"[^a-z0-9]+", " ", part.lower()).strip()
        if not folded or folded == brand_folded or folded in generic:
            continue
        if brand_folded and (folded.startswith(brand_folded) or brand_folded.startswith(folded)):
            rest = folded[len(brand_folded):].strip() if folded.startswith(brand_folded) else ""
            if rest in {"", "us", "usa", "co", "inc", "llc", "io"} or brand_folded.startswith(folded):
                continue
            part = part[len(brand):].strip(" .,") if part.lower().startswith(brand.lower()) else part
        words = [
            word for word in part.split()
            if word.lower().strip(".,") not in {"official", "site", "homepage", "co", "inc", "llc", "io"}
        ]
        part = " ".join(words).strip(" .,")
        if len(part) < 3 or part.lower() in generic:
            continue
        if re.search(r"\b(?:company|incorporated)\b", part, re.I):
            continue
        if re.fullmatch(r"[A-Za-z]+ US", part) and part.split()[0].lower() in brand_folded:
            continue
        if part and part.lower() not in generic:
            kept.append(part)
    phrase = kept[0] if kept else ""
    phrase = " ".join(phrase.split())
    if not phrase or phrase.lower() in generic:
        return ""
    if brand_folded and re.sub(r"[^a-z0-9]+", " ", phrase.lower()).strip() == brand_folded:
        return ""
    return phrase


def _subject(brand: str, phrase: str) -> str:
    """The thing the sentence is about, without saying the brand twice."""

    if len(brand) >= 8 and brand.lower() in phrase.lower():
        return phrase
    return f"{_possessive(brand)} {phrase}"


def _fit_line(
    line: str,
    *,
    max_words: int,
    allow_amazon: bool,
    trial: bool,
    role: str,
) -> str:
    ok, _why = validate(
        line, allow_amazon=allow_amazon, max_words=max_words, trial_offer=trial, role=role,
    )
    return clean(line) if ok else ""


def _product_title(lead: dict[str, Any], facts: list[str], brand: str) -> str:
    """A product from the storefront. A confirmed Amazon listing is separate."""

    for fact in facts:
        if fact.lower().startswith("recent product:"):
            title = _shopper_phrase(fact.split(":", 1)[1], brand)
            if title:
                return title
    profile = lead.get("store_profile") if isinstance(lead.get("store_profile"), dict) else {}
    stored = profile.get("most_recently_published_product")
    if isinstance(stored, dict):
        title = _shopper_phrase(str(stored.get("title") or ""), brand)
        if title:
            return title
    return _shopper_phrase(_scraped_product(lead), brand)


def _story_phrase(facts: list[str], brand: str) -> str:
    for fact in facts:
        low = fact.lower()
        if low.startswith("recent product:") or low.startswith("sells on "):
            continue
        if low in {
            "has a store locator", "sold through retailers", "sold in stores",
            "sells wholesale", "offers a subscribe option",
        }:
            continue
        if "reviews average" in low or "followers" in low or " bio:" in low:
            continue
        if low.startswith("has shoppers"):
            continue
        if BACKEND_LANGUAGE.search(fact) or re.search(r"https?://|www\.", fact, re.I):
            continue
        if len(fact.split()) < 3:
            continue
        phrase = _shopper_phrase(fact, brand)
        if phrase and len(phrase.split()) >= 3:
            return phrase
    return ""


def _social_network(facts: list[str]) -> str:
    names = {
        "tiktok": "TikTok", "youtube": "YouTube", "instagram": "Instagram",
        "pinterest": "Pinterest", "twitter": "Twitter",
    }
    blob = " ".join(facts).lower()
    for key, label in names.items():
        if key in blob:
            return label
    return ""


def plain_product_phrase(text: str, brand: str) -> str:
    """Lowercase product words, or empty when the text is a menu or a fragment.

    A comma or a pipe means two labels. The phrase is kept whole or not used.
    It is never cut off at a comma.
    """

    import html as html_lib

    raw = html_lib.unescape(str(text or ""))
    for mark in ("®", "™", "©", "+", "|"):
        raw = raw.replace(mark, " ")
    raw = " ".join(raw.replace("&amp;", " and ").split())
    if not raw or "," in raw or "|" in str(text or ""):
        return ""
    if BACKEND_LANGUAGE.search(raw) or _SLUG.search(raw):
        return ""
    if re.search(
        r"\b(?:wines?|alcohol|beer|spirits|liquor|cannabis|marijuana|cbd|cbda|thc|tobacco|vapes?)\b",
        raw, re.I,
    ):
        return ""
    if re.search(r"[àâäéèêëïîôùûüçñ]", raw, re.I):
        return ""
    # A comma separates two labels. Use one complete side, never a cut ending in a comma.
    if "," in raw or "|" in str(text or ""):
        sides = [part.strip() for part in re.split(r"[,|]", raw) if part.strip()]
        if len(sides) != 2:
            return ""
        left, right = sides
        if len(left.split()) <= 2 and any(w.lower() in {"and", "with", "for"} for w in right.split()):
            raw = right
        else:
            return ""
    brand_words = set(re.findall(r"[a-z0-9]+", brand.lower()))
    display = str(brand or "")
    skip = brand_words | {
        "best", "award", "winning", "award-winning", "official", "homepage",
        "site", "free", "new", "the", "co", "inc", "llc", "usa", "us", "io",
        "s", "and", "for", "with", "from", "your", "our", "you", "help",
    }
    words: list[str] = []
    for word in re.findall(r"[A-Za-z0-9][A-Za-z0-9'-]*", raw):
        low = word.lower().strip("'")
        if not low or low in skip or len(low) < 3:
            continue
        if re.fullmatch(r"[A-Z]{2,3}", word):
            words.append(word)
        else:
            words.append(low)
    phrase = " ".join(words)
    folded = re.sub(r"[^a-z0-9]+", " ", phrase).strip()
    brand_folded = re.sub(r"[^a-z0-9]+", " ", display.lower()).strip()
    if not folded or folded == brand_folded or brand_folded.startswith(folded):
        return ""
    if len(words) < 2 or len(words) > 8:
        return ""
    return phrase


def idea_decision(lead: dict[str, Any]) -> tuple[str, str, str]:
    """Return ``(opener, angle, hold_reason)``.

    The angle talks about the listing or the ad. It does not restate the
    opener. A Trial Offer angle says new shoppers and does not mention Amazon.
    """

    import hashlib

    from outbound_personalization import fact_sheet
    from outbound_pipeline import clean_brand_name

    sheet = fact_sheet(lead)
    if sheet.blocked == "restricted category":
        return "", "", "restricted category"
    if sheet.blocked == "multi-brand retailer":
        return "", "", "multi-brand retailer"
    brand = clean_brand_name(
        lead.get("brand") or "",
        display_name=lead.get("display_brand") or "",
    )
    sheet_brand = sheet.brand if sheet.brand and sheet.brand != "This brand" else ""
    brand = brand or sheet_brand
    if not brand:
        return "", "", "no recognizable brand"
    trial = bool(sheet.trial_offer or trial_offer_lead(lead))
    allow_amazon = amazon_presence_allowed(lead)
    facts = list(sheet.facts)

    def _reject_brand_echo(picked: str) -> str:
        if not picked:
            return ""
        display_folded = re.sub(
            r"[^a-z0-9]+", " ", str(lead.get("display_brand") or "").lower(),
        ).strip()
        bare = picked[4:] if picked.startswith("the ") else picked
        if display_folded and bare == display_folded:
            return ""
        stem = re.sub(r"[^a-z0-9]+", "", str(lead.get("domain") or "").split(".")[0].lower())
        if stem and re.sub(r"[^a-z0-9]+", "", bare) == stem:
            return ""
        return picked

    # A catalog product is a name a shopper can buy. The homepage title is not.
    # Only the confirmed Amazon listing may be described as on Amazon.
    profile = lead.get("store_profile") if isinstance(lead.get("store_profile"), dict) else {}
    catalog_phrase = str(profile.get("product_phrase") or lead.get("product_phrase") or "").strip()
    if catalog_phrase and not catalog_phrase.startswith("the "):
        catalog_phrase = natural_product_phrase(catalog_phrase, brand)
    if not catalog_phrase:
        for item in profile.get("representative_products") or []:
            if not isinstance(item, dict):
                continue
            catalog_phrase = str(item.get("phrase") or "") or natural_product_phrase(
                str(item.get("title") or ""), brand,
            )
            if catalog_phrase:
                break
    amazon_phrase = ""
    if allow_amazon and amazon_confirmed(lead):
        amazon_phrase = _reject_brand_echo(
            natural_product_phrase(str(lead.get("amz_product") or ""), brand)
        )
    catalog_phrase = _reject_brand_echo(catalog_phrase)
    phrase = amazon_phrase or catalog_phrase or _reject_brand_echo(
        natural_product_phrase(_product_title(lead, facts, brand), brand)
    )
    if not phrase:
        from outbound_product_facts import phrase_from_description

        for fact in facts:
            if fact.lower().startswith("recent product:"):
                continue
            phrase = _reject_brand_echo(phrase_from_description(fact, brand))
            if phrase:
                break
    category = ""
    trial_signal = str(lead.get("trial_signal") or "")
    if not phrase and trial_signal in {"category", "churned_tool"}:
        from outbound_instantly_push import friendly_category

        category = friendly_category(lead.get("categories") or lead.get("category") or sheet.category)
        category = plain_product_phrase(category, brand)
    retail = any(fact.lower() in {
        "has a store locator", "sold through retailers", "sold in stores",
    } for fact in facts)
    social = _social_network(facts)
    if not phrase and not category and not retail and not social:
        return "", "", "no shopper-visible fact"

    domain = str(lead.get("domain") or brand)
    def choose(options: list[str], salt: str) -> str:
        digest = hashlib.sha256(f"{domain}|{salt}".encode()).hexdigest()
        return options[int(digest[:8], 16) % len(options)]

    if amazon_phrase:
        opener = choose([
            f"{brand} already sells {amazon_phrase} on Amazon, and the listing should lead with it.",
            f"On Amazon, {amazon_phrase} should be the first {brand} product a new shopper sees.",
            f"{brand} sells {amazon_phrase} on Amazon, and the listing should open on it.",
        ], "amz-opener")
    elif phrase:
        opener = choose([
            f"{brand} makes {phrase}, and that should be the first thing a new shopper sees.",
            f"A new shopper should meet {brand} through {phrase}.",
            f"The first image for {brand} should be {phrase}.",
            f"The simplest way into {brand} is {phrase}.",
        ], "product-opener")
    elif category:
        opener = choose([
            f"{brand} is a {category} brand, and one product should be the first impression.",
            f"For a {category} brand like {brand}, one product should do the introducing.",
        ], "category-opener")
    elif retail:
        opener = f"{brand} is already in retail stores, which is proof a new shopper can see."
    else:
        opener = f"{brand} already shows up on {social}, where a new shopper can see the products."

    shoppers = "a new shopper"
    angles = [
        f"The listing photos should show one {brand} product on its own.",
        f"An ad for {brand} works when that product is the whole message.",
        f"The first image should make one {brand} product obvious to {shoppers}.",
        f"A listing for {brand} should lead with one product and let the photos explain it.",
    ]
    angle = choose(angles, "angle")
    if _repeats(angle, opener):
        angle = angles[(angles.index(angle) + 1) % len(angles)]

    opener_text = _fit_line(
        opener, max_words=MAX_WORDS, allow_amazon=allow_amazon, trial=trial, role="opener",
    )
    angle_text = _fit_line(
        angle, max_words=ANGLE_MAX_WORDS, allow_amazon=False, trial=trial, role="angle",
    )
    if stuffed_copy_reason(opener):
        return "", "", stuffed_copy_reason(opener)
    if not opener_text:
        return "", "", "no shopper-visible fact"
    return opener_text, angle_text, ""


def idea_lines(lead: dict[str, Any]) -> tuple[str, str]:
    """One opener and one angle from a shopper-visible fact.

    Both strings are empty when the record has no product, story, review,
    retail store, or social presence. Category codes and backend signals
    are not enough. ``idea_decision`` also returns why a lead was held.
    """

    opener, angle, _reason = idea_decision(lead)
    return opener, angle


def opener_line(facts: LeadFacts) -> str:
    """Kept for callers that already hold a checked rich line.

    Category sentences and backend signals are not openers. New copy goes
    through ``idea_lines``.
    """

    if not facts.rich_opener:
        return ""
    ok, _why = validate(
        facts.rich_opener,
        allow_amazon=not facts.trial_offer,
        trial_offer=facts.trial_offer,
    )
    return clean(facts.rich_opener) if ok else ""


def brand_angle_line(facts: LeadFacts) -> str:
    """Kept for callers that already hold a checked rich angle.

    "Anata helps {category} brands grow" is not an angle. New copy goes
    through ``idea_lines``.
    """

    if not facts.rich_angle:
        return ""
    ok, _why = validate(
        facts.rich_angle,
        allow_amazon=not facts.trial_offer,
        max_words=ANGLE_MAX_WORDS,
        trial_offer=facts.trial_offer,
        role="angle",
    )
    return clean(facts.rich_angle) if ok else ""


def fallback_line(
    brand: str,
    niche: str = "",
    reason: str = "",
    product: str = "",
    recipe: str = "",
) -> str:
    """Deterministic opener from a product. A category or backend signal is not enough.

    ``reason`` is an internal note and is deliberately never shown to a prospect.
    ``product`` is a product name the caller has already decided is real.
    The line is empty when that product does not survive the rules.
    """

    lead: dict[str, Any] = {
        "brand": (brand or "").strip(),
        "recipe": (recipe or "").strip(),
        "niche": niche,
        "categories": niche,
    }
    if product:
        lead["store_profile"] = {
            "most_recently_published_product": {"title": product},
            "description": product,
        }
        if amazon_presence_allowed(lead):
            lead["amz_product"] = product
            lead["amz_confidence"] = "high"
    opener, _angle = idea_lines(lead)
    if reason and reason.lower() in opener.lower():
        return ""
    return opener


def _build_prompt(facts: LeadFacts, *, closing: str) -> str:
    lines = [f"Brand: {facts.brand or 'This brand'}"]
    if facts.trial_offer:
        lines.append(
            "Trial Offer: the opener and the angle must not mention Amazon and must "
            "not claim the brand already sells on Amazon. "
            "Never write 'on Amazon', 'your Amazon store', 'your listings', or "
            "'already on Amazon'. Say new shoppers."
        )
    elif facts.amazon_product:
        lines.append("Segment: amz_on. The line may say this product is on Amazon.")
        lines.append(f"Product: {facts.amazon_product}")
    if facts.product:
        lines.append(f"Product: {facts.product}")
    if facts.category:
        lines.append(f"What they sell, in words not a code: {facts.category}")
    for phrase in facts.rich_phrases:
        if BACKEND_LANGUAGE.search(phrase):
            continue
        lines.append(f"Fact: {phrase}")
    lines.append(
        "Do not mention platforms, plans, apps, pixels, bids, automation, "
        "direct-to-consumer, or dates. Suggest one specific idea."
    )
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
    if not opener and template_fallback_enabled(lead):
        return
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
    role: str = "opener",
) -> tuple[str, str]:
    """Return a clean line, or ('', reason) after one regeneration."""

    why = ""
    for _attempt in range(2):
        raw = llm(prompt)
        ok, why = validate(
            raw, allow_amazon=allow_amazon, max_words=max_words,
            trial_offer=trial_offer, role=role,
        )
        if ok and not _anchored(raw, anchors):
            ok, why = False, "not grounded in a known fact"
        if ok:
            return clean(raw), ""
        logger.info("[outbound-openers] rejected AI line (%s)", why)
        prompt = (prompt + f"\n\nYour last line was rejected ({why}). "
                  "Write a different statement that follows every rule.")
    return "", why or "rejected"


def write_opener(
    lead: dict[str, Any],
    contact: Optional[dict[str, Any]] = None,
    *,
    llm: Optional[Callable[[str], str]] = None,
    api_key: Optional[str] = None,
) -> tuple[str, str]:
    """Return (line, source) where source is "ai", "fallback", or "none".

    One line per company domain. A second contact at the same domain reuses it.
    ``contact`` is ignored so the line is not rewritten per person. The line is
    empty when we have no confirmed product, recipe signal, or category.
    """

    if rich_personalization_enabled(lead):
        _ensure_rich(lead, llm=llm, api_key=api_key)
    domain = _domain_key(lead)
    if domain and domain in _OPENERS_BY_DOMAIN:
        return _OPENERS_BY_DOMAIN[domain]

    facts = collect_facts(lead)

    def finish(line: str, source: str) -> tuple[str, str]:
        result = (line, source)
        if domain:
            _OPENERS_BY_DOMAIN[domain] = result
        return result

    if not facts.solid:
        if not template_fallback_enabled(lead):
            return finish("", "none")
        opener, angle = idea_lines(lead)
        if domain and opener:
            _ANGLES_BY_DOMAIN[domain] = (angle, "fallback" if angle else "none")
        if opener:
            return finish(opener, "fallback")
        return finish("", "none")
    if facts.rich_opener:
        return finish(facts.rich_opener, "fallback")
    model = _resolve_llm(llm, api_key, system=_SYSTEM)
    if model is not None:
        prompt = _build_prompt(
            facts,
            closing=(
                "Write one sentence of about 20 words, never more than 25. "
                "Write the opening statement now."
            ),
        )
        try:
            line, _why = _from_model(
                model, prompt,
                allow_amazon=amazon_presence_allowed(lead),
                anchors=_anchors(facts), max_words=MAX_WORDS,
                trial_offer=facts.trial_offer,
                role="opener",
            )
            if line and not opener_hold_reason(line, lead):
                return finish(line, "ai")
        except Exception:  # noqa: BLE001 - never block a lead on the AI
            logger.exception("[outbound-openers] AI call failed")
    if not template_fallback_enabled(lead):
        return finish("", "none")
    line, angle = idea_lines(lead)
    if domain and line:
        _ANGLES_BY_DOMAIN[domain] = (angle, "fallback" if angle else "none")
    return finish(line, "fallback" if line else "none")


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
        if not template_fallback_enabled(lead):
            return finish("", "none")
        _opener, angle = idea_lines(lead)
        if angle:
            return finish(angle, "fallback")
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
            line, _why = _from_model(
                model, prompt,
                allow_amazon=amazon_presence_allowed(lead),
                anchors=_anchors(facts), max_words=ANGLE_MAX_WORDS,
                trial_offer=facts.trial_offer,
                role="angle",
            )
            if line and not angle_hold_reason(line, lead) and not angle_is_filler(line):
                return finish(line, "ai")
        except Exception:  # noqa: BLE001 - never block a lead on the AI
            logger.exception("[outbound-openers] brand angle AI call failed")
    if not template_fallback_enabled(lead):
        return finish("", "none")
    _opener, line = idea_lines(lead)
    return finish(line, "fallback" if line else "none")


# A held contact stays out of Instantly until a later push can replace this.
OPENER_HOLD_PREFIX = "held:opener:"

# Backend facts are recipe context. They are not a cold-email sentence.
_BACKEND_COPY = BACKEND_LANGUAGE
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
    if _COPY_URL.search(text):
        return "opener contains a url"
    if "—" in text or "–" in text or " - " in text:
        return "opener contains a dash"
    if _STOOD_OUT.search(text):
        return "opener uses the stood out template"
    if _SLUG.search(text):
        return "opener contains a raw category code"
    if "?" in text:
        return "opener is a question"
    if _BACKEND_COPY.search(text):
        return "opener names backend technology instead of a shopper-visible fact"
    if copy_tic(text):
        return copy_tic(text)
    if _ANATA_CATEGORY.match(text):
        return "opener is a category filler with no shopper-visible fact"
    if _SAW_CATEGORY.match(text):
        return "opener is a category filler with no shopper-visible fact"
    sold = _normalized_sale(text)
    if sold and sold in _category_phrases(lead or {}):
        return "opener is a category filler with no shopper-visible fact"
    stuffed = stuffed_copy_reason(text)
    if stuffed:
        return stuffed
    if len(text.split()) > MAX_WORDS:
        return "opener is longer than 25 words"
    if trial_offer_lead(lead) and _AMAZON_WORD.search(text):
        return "trial offer opener assumes the brand already sells on Amazon"
    if lead and not amazon_presence_allowed(lead) and not trial_offer_lead(lead) and _AMAZON_WORD.search(text):
        return "opener mentions being on Amazon outside the amz_on segment"
    if len(text.split()) < MIN_WORDS:
        return "opener is shorter than 6 words"
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
    if lead and not amazon_presence_allowed(lead) and not trial_offer_lead(lead) and _AMAZON_WORD.search(text):
        return "angle mentions being on Amazon outside the amz_on segment"
    if len(text.split()) < MIN_WORDS:
        return "brand angle is shorter than 6 words"
    if _BACKEND_COPY.search(text) or _STOOD_OUT.search(text) or "?" in text or _SLUG.search(text):
        return "brand angle is not a shopper-visible idea"
    if "—" in text or "–" in text or " - " in text or _COPY_URL.search(text):
        return "brand angle is not a shopper-visible idea"
    return ""


def angle_is_filler(angle: str) -> bool:
    """True when brandAngle would ship a category slogan, a URL, or backend tech."""

    text = (angle or "").strip()
    if not text:
        return False
    if _ANATA_CATEGORY.match(text) or _COPY_URL.search(text) or _BACKEND_COPY.search(text):
        return True
    return False


def _shopper_sentence(brand: str, fact: str) -> str:
    """One email sentence from a shopper-visible fact. Empty when it is not one."""

    raw = " ".join((fact or "").split())
    low = raw.lower()
    if not raw:
        return ""
    if low.startswith("sells on ") or low.startswith("has shoppers") or low.startswith("offers a subscribe"):
        return ""
    if low.startswith("recent product:"):
        title = raw.split(":", 1)[1].strip()
        return _usable(f"{brand} sells {title}.", allow_amazon=False)
    if "reviews average" in low or low.startswith("reviews "):
        return _usable(f"{brand} {raw}.", allow_amazon=False)
    if low in {"has a store locator", "sold through retailers", "sold in stores"}:
        return _usable(f"{brand} is sold in retail stores.", allow_amazon=False)
    if low == "sells wholesale":
        return _usable(f"{brand} sells through wholesale.", allow_amazon=False)
    if " bio:" in low or low.endswith("followers"):
        return _usable(f"{brand} {raw}.", allow_amazon=False)
    if len(raw.split()) >= 4 and not _BACKEND_COPY.search(raw) and not _COPY_URL.search(raw):
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

    opener, angle = idea_lines(lead)
    if opener_hold_reason(opener, lead):
        return "", ""
    if angle_is_filler(angle) or angle_hold_reason(angle, lead):
        angle = ""
    return opener, angle
