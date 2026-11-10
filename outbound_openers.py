"""One opening line per company, plus a short brand angle (replaces Clay's AI column).

``opener`` fills ``{{personalization}}``. ``brand_angle`` is pushed as the
Instantly custom variable ``brandAngle``. Both are statements grounded in a
fact we already hold: a confirmed Amazon product, a scraped product phrase,
a recipe signal, or a StoreLeads category. If the model is missing, errors,
or writes something that breaks a rule, a plain deterministic line is used
instead. When even that line would not be a real fact, the field is left empty.

Hard rules, enforced in code not just in the prompt:
  * No em or en dashes.
  * No digits, so a made-up number can never reach a prospect.
  * No links, no email addresses, no cliches, no questions.
  * No category slugs and no unverified claims.
  * Amazon is named only when the Amazon check confirmed the product.
  * One or two short sentences, capped by length.
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)

MODEL = "claude-haiku-4-5-20251001"
MAX_WORDS = 34
ANGLE_MAX_WORDS = 22

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
    "strategy and runs the ad actions automatically. Write ONE short statement, not a "
    "question, under 30 words. Ground it in one real fact from the list: the Amazon "
    "product, a recent platform move, a recent app install, or a recent plan upgrade. "
    "If a plain category is given, you may say they sell that. Only mention Amazon "
    "when an 'Amazon product' fact is listed; otherwise never say or imply they sell "
    "on Amazon. Never print a slug with "
    "underscores. Do not claim social momentum, growth, or anything that is not in the "
    "facts. Do not start with 'I noticed' or 'We noticed'. Never use the words "
    "'curious', 'special', 'building something', or 'ideal customer profile'. "
    "No numbers. No links. No em dashes. No greeting, no sign-off."
)

_ANGLE_SYSTEM = (
    "You write one short line for a cold email from Abby at Anata. Anata helps "
    "a consumer brand grow. Tie the line to one fact from the list and to how "
    "Anata helps that brand grow. Use only those facts. Only mention Amazon when "
    "an 'Amazon product' fact is listed; otherwise never say or imply they sell "
    "on Amazon. One statement, under 20 words. No numbers. No links. No questions. "
    "No em dashes. No greeting, no sign-off."
)


def clean(text: str) -> str:
    """Normalise punctuation and whitespace the way our emails require."""
    t = (text or "").strip().strip('"').strip()
    t = t.replace("—", " - ").replace("–", " - ").replace("--", " - ")
    t = re.sub(r"\s+", " ", t)
    t = re.sub(r"\s+-\s+", " - ", t)
    return t


def amazon_confirmed(lead: dict[str, Any]) -> bool:
    """True only when the Amazon check matched this brand with high confidence."""

    conf = str(lead.get("amz_confidence") or lead.get("amazon_confidence") or "").strip().lower()
    return conf == "high" and not lead.get("amazon_absent")


def validate(text: str, *, allow_amazon: bool = True, max_words: int = MAX_WORDS) -> tuple[bool, str]:
    raw = (text or "").strip()
    if not allow_amazon and _AMAZON_WORD.search(raw):
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
    if re.search(r"\d", t):
        return False, "contains a number"
    if re.search(r"https?://|www\.|@", t):
        return False, "contains a link or address"
    low = t.lower()
    for phrase in BANNED_PHRASES:
        if phrase in low:
            return False, f"cliche: {phrase}"
    return True, ""


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

    @property
    def solid(self) -> bool:
        return bool(self.amazon_product or self.product or self.category or self.signal or self.rich_phrases)


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
    if recipe == "amz_off" and "amazon" not in channels:
        phrases.append("does not sell on Amazon")
    elif recipe == "amz_on" and "sells on Amazon" not in phrases and not absent:
        phrases.append("sells on Amazon")

    if str(profile.get("retailer_page") or "").strip() or str(profile.get("store_locator_page") or "").strip():
        phrases.append("is sold in retail stores")

    labels = " ".join(
        str(item) for item in ((profile.get("apps") or []) + (profile.get("technologies") or []))
    ).lower()
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

    brand = str(lead.get("brand") or lead.get("domain") or "").strip()
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
    if rich_personalization_enabled(lead):
        rich = tuple(store_fact_phrases(lead))
    return LeadFacts(brand, amazon_product, product, category, signal, extras, rich)


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
    return False


def _named(brand: str) -> str:
    return (brand or "").strip() or "This brand"


def opener_line(facts: LeadFacts) -> str:
    """Deterministic opener. Empty when we have no product, signal, or category."""

    brand = _named(facts.brand)
    if facts.amazon_product:
        line = _usable(f"{brand} lists {facts.amazon_product} on Amazon.", allow_amazon=True)
        if line:
            return line
    if facts.rich_phrases:
        allow_amazon = any("amazon" in phrase.lower() for phrase in facts.rich_phrases)
        line = rich_sentence(brand, list(facts.rich_phrases), allow_amazon=allow_amazon)
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
    if facts.category:
        line = _usable(f"{brand} sells {facts.category}.", allow_amazon=False)
        if line:
            return line
    return ""


def brand_angle_line(facts: LeadFacts) -> str:
    """Tie one known fact to how Anata helps. Category is the neutral fallback."""

    brand = _named(facts.brand)
    if facts.amazon_product:
        line = _usable(
            f"Anata helps {brand} grow {facts.amazon_product} on Amazon.",
            allow_amazon=True, max_words=ANGLE_MAX_WORDS,
        )
        if line:
            return line
    if facts.rich_phrases:
        allow_amazon = any("amazon" in phrase.lower() for phrase in facts.rich_phrases)
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
    """Deterministic opener. Only a product, a recipe signal, or a plain category.

    ``reason`` is an internal note and is deliberately never shown to a prospect.
    ``product`` is an Amazon product the caller has already decided is confirmed.
    The line is empty when none of those facts survive the rules.
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
    if facts.amazon_product:
        lines.append(f"Amazon product: {facts.amazon_product}")
    if facts.product:
        lines.append(f"Product: {facts.product}")
    if facts.signal:
        lines.append(f"Recent signal: {RECIPE_FACTS[facts.signal]}")
    if facts.category:
        lines.append(f"What they sell: {facts.category}")
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


def _from_model(
    llm: Callable[[str], str],
    prompt: str,
    *,
    allow_amazon: bool,
    anchors: list[str],
    max_words: int,
) -> tuple[str, str]:
    """Return a clean line, or ('', reason) after two rejects."""

    why = ""
    for _attempt in range(2):
        raw = llm(prompt)
        ok, why = validate(raw, allow_amazon=allow_amazon, max_words=max_words)
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
        return finish("", "none")
    model = _resolve_llm(llm, api_key, system=_SYSTEM)
    if model is not None:
        prompt = _build_prompt(facts, closing="Write the opening statement now.")
        try:
            line, _why = _from_model(
                model, prompt, allow_amazon=bool(facts.amazon_product),
                anchors=_anchors(facts), max_words=MAX_WORDS,
            )
            if line:
                return finish(line, "ai")
        except Exception:  # noqa: BLE001 - never block a lead on the AI
            logger.exception("[outbound-openers] AI call failed, using fallback")
    line = opener_line(facts)
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
    model = _resolve_llm(llm, api_key, system=_ANGLE_SYSTEM)
    if model is not None:
        prompt = _build_prompt(facts, closing="Write the growth line now.")
        try:
            line, _why = _from_model(
                model, prompt, allow_amazon=bool(facts.amazon_product),
                anchors=_anchors(facts), max_words=ANGLE_MAX_WORDS,
            )
            if line:
                return finish(line, "ai")
        except Exception:  # noqa: BLE001 - never block a lead on the AI
            logger.exception("[outbound-openers] brand angle AI call failed, using fallback")
    line = brand_angle_line(facts)
    return finish(line, "fallback" if line else "none")
