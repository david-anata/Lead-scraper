"""One opening line per company (replaces Clay's AI column).

The line fills ``{{personalization}}`` in the Instantly sequence. It is a
statement grounded in a fact we already hold. If the model is missing, errors,
or writes something that breaks a rule, a plain deterministic line is used
instead, so a lead is never blocked on the AI.

Hard rules, enforced in code not just in the prompt:
  * No em or en dashes.
  * No digits, so a made-up number can never reach a prospect.
  * No links, no email addresses, no cliches, no questions.
  * No category slugs and no unverified claims.
  * One or two short sentences, capped by length.
"""

from __future__ import annotations

import logging
import os
import re
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)

MODEL = "claude-haiku-4-5-20251001"
MAX_WORDS = 34

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
_OPENERS_BY_DOMAIN: dict[str, tuple[str, str]] = {}

_SYSTEM = (
    "You write the first line of a cold email from Abby at Anata to a founder or "
    "marketing lead at a consumer brand. Anata Intelligence builds a brand's Amazon "
    "strategy and runs the ad actions automatically. Write ONE short statement, not a "
    "question, under 30 words. Ground it in one real fact from the list: the Amazon "
    "product, a recent platform move, a recent app install, or a recent plan upgrade. "
    "If a plain category is given, you may say they sell that. Never print a slug with "
    "underscores. Do not claim social momentum, growth, or anything that is not in the "
    "facts. Do not start with 'I noticed' or 'We noticed'. Never use the words "
    "'curious', 'special', 'building something', or 'ideal customer profile'. "
    "No numbers. No links. No em dashes. No greeting, no sign-off."
)


def clean(text: str) -> str:
    """Normalise punctuation and whitespace the way our emails require."""
    t = (text or "").strip().strip('"').strip()
    t = t.replace("—", " - ").replace("–", " - ").replace("--", " - ")
    t = re.sub(r"\s+", " ", t)
    t = re.sub(r"\s+-\s+", " - ", t)
    return t


def validate(text: str) -> tuple[bool, str]:
    raw = (text or "").strip()
    if "—" in raw or "–" in raw:
        return False, "em dash"
    if _SLUG.search(raw):
        return False, "slug"
    if "?" in raw:
        return False, "question"
    t = clean(raw)
    if not t:
        return False, "empty"
    if len(t.split()) > MAX_WORDS or len(t) > 220:
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


def fallback_line(
    brand: str,
    niche: str = "",
    reason: str = "",
    product: str = "",
    recipe: str = "",
) -> str:
    """Deterministic statement. Only a product, a recipe signal, or a plain category.

    ``reason`` is an internal note and is deliberately never shown to a prospect.
    """

    name = (brand or "This brand").strip()
    sold = (product or "").strip()
    if sold:
        return clean(f"{name} lists {sold} on Amazon.")
    signal = RECIPE_FACTS.get((recipe or "").strip())
    if signal:
        return clean(f"{name} {signal}.")
    words = plain_category(niche)
    if words:
        return clean(f"{name} sells {words}.")
    return clean(f"{name} sells to consumers online.")


def _build_prompt(brand: str, niche: str, categories: str, product: str, recipe: str) -> str:
    facts = [f"Brand: {brand}"]
    if product:
        facts.append(f"Amazon product: {product}")
    signal = RECIPE_FACTS.get(recipe)
    if signal:
        facts.append(f"Recent signal: {signal}")
    words = plain_category(categories) or plain_category(niche)
    if words:
        facts.append(f"What they sell: {words}")
    return "\n".join(facts) + "\n\nWrite the opening statement now."


def _call_anthropic(prompt: str, api_key: str) -> str:
    import anthropic  # imported lazily so tests and fallbacks never need it

    client = anthropic.Anthropic(api_key=api_key)
    msg = client.messages.create(
        model=MODEL, max_tokens=120, system=_SYSTEM,
        messages=[{"role": "user", "content": prompt}],
    )
    return "".join(getattr(b, "text", "") for b in msg.content)


def _domain_key(lead: dict[str, Any]) -> str:
    return str(lead.get("domain") or "").strip().lower()


def write_opener(
    lead: dict[str, Any],
    contact: Optional[dict[str, Any]] = None,
    *,
    llm: Optional[Callable[[str], str]] = None,
    api_key: Optional[str] = None,
) -> tuple[str, str]:
    """Return (line, source) where source is "ai" or "fallback".

    One line per company domain. A second contact at the same domain reuses it.
    ``contact`` is ignored so the line is not rewritten per person.
    """

    domain = _domain_key(lead)
    if domain and domain in _OPENERS_BY_DOMAIN:
        return _OPENERS_BY_DOMAIN[domain]

    brand = str(lead.get("brand") or lead.get("domain") or "").strip()
    niche = str(lead.get("niche") or "").strip()
    cats = lead.get("categories") or ""
    if isinstance(cats, (list, tuple)):
        cats = ", ".join(str(c) for c in cats[:3])
    product = str(lead.get("amz_product") or "").strip()
    recipe = str(lead.get("recipe") or "").strip()

    def finish(line: str, source: str) -> tuple[str, str]:
        result = (line, source)
        if domain:
            _OPENERS_BY_DOMAIN[domain] = result
        return result

    key = api_key if api_key is not None else os.getenv("ANTHROPIC_API_KEY", "")
    if llm is None and key:
        llm = lambda p: _call_anthropic(p, key)  # noqa: E731
    if llm is not None:
        prompt = _build_prompt(brand, niche, str(cats), product, recipe)
        try:
            for _attempt in range(2):
                raw = llm(prompt)
                ok, why = validate(raw)
                if ok:
                    return finish(clean(raw), "ai")
                logger.info("[outbound-openers] rejected AI line (%s)", why)
                prompt = (prompt + f"\n\nYour last line was rejected ({why}). "
                          "Write a different statement that follows every rule.")
        except Exception:  # noqa: BLE001 - never block a lead on the AI
            logger.exception("[outbound-openers] AI call failed, using fallback")
    return finish(fallback_line(brand, niche, "", product, recipe), "fallback")
