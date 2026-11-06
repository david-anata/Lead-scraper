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

    @property
    def solid(self) -> bool:
        return bool(self.amazon_product or self.product or self.category or self.signal)


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
    return LeadFacts(brand, amazon_product, product, category, signal, extras)


def _anchors(facts: LeadFacts) -> list[str]:
    found = [facts.amazon_product, facts.product, facts.category, *facts.extra_anchors]
    if facts.signal:
        found.append(RECIPE_FACTS[facts.signal])
        found.append(_ANGLE_AFTER[facts.signal])
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
    return str(lead.get("domain") or "").strip().lower()


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
