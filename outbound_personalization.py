"""Shopper-visible personalization for the rich opener flag.

The template joiner cannot write the lines David approved. This module
builds a fact sheet from a StoreLeads record, asks the existing model
client to write an opener and a brand angle in Abby's voice, and rejects
anything the rules do not allow. The flag stays off until it is turned on.
Apps, plans, pixels, and platform moves are recipe context only. They are
not put on the fact sheet and the validator refuses them in the copy.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)

HIGH_RATING = 4.5
RETAILER_VENDOR_COUNT = 25
OPENER_MAX_WORDS = 80
ANGLE_MAX_WORDS = 45

_RESTRICTED = re.compile(
    r"\b(?:wines?|alcohol|beer|spirits|liquor|cannabis|marijuana|cbd|tobacco|vapes?|vaping|"
    r"firearms?|weapons?|ammunition|adult)\b",
    re.I,
)
_PROMO = re.compile(
    r"\b(?:free shipping|discount code|promo code|coupon|use code\s+\w+|%\s*off|\d+\s*%\s*off)\b",
    re.I,
)
_TECH = re.compile(
    r"\b(?:shopify|woocommerce|woo commerce|magento|bigcommerce|big commerce|klaviyo|"
    r"yotpo|okendo|judge\.?me|loox|recharge|skio|smartrr|facebook pixel|meta pixel|"
    r"google ads|tiktok pixel|ad pixel|shopify plus)\b",
    re.I,
)
_BACKEND = re.compile(r"\b(?:moved|upgraded|platform|app install)\b", re.I)
_HYPE = re.compile(r"\b(?:perfect moment|maximize|maximise|game-changing|revolutionary)\b", re.I)
_LOW_RATING = re.compile(
    r"\b(?:one|two|three|four) stars?\b|\b(?:poor|low|bad|negative) (?:rating|reviews)\b",
    re.I,
)
_NEGATIVE = re.compile(
    r"\b(?:does not|doesn't|do not|isn't on amazon|not on amazon|no amazon presence)\b",
    re.I,
)
_URL = re.compile(r"https?://|www\.", re.I)
_SUBSCRIBE_APPS = ("recharge", "skio", "smartrr", "subscription")

_EXAMPLES = """
Lovevery, selling on Amazon. Opener: Lovevery's stage-based Play Kits are a smart way to sell toys, and with more than half a million TikTok followers, a lot of parents probably discover you there and then go looking on Amazon. That search is the moment your Amazon store either catches them or loses them to a lookalike Montessori toy.
Angle: The play-stage videos you already make for YouTube and Pinterest could double as Amazon listing video, helping parents pick the right toy for their child's age without leaving the page.

REV Gum, selling on Amazon and in stores. Opener: Caffeine and electrolytes in a sugar-free gum is an easy thing to explain in one line, and I noticed REV is already on convenience store shelves and gopuff alongside your Amazon store. People who try a pack at the counter often go to reorder online, and Amazon is usually the first place they check.
Angle: Leaning into the electrolyte side and the multi-pack value on Amazon could set REV apart from shoppers who are only comparing plain caffeine gums.

Water.io, not yet using Amazon as the hook. Opener: A smart bottle that comes with free hydration coaching in an app is an easy idea to get behind, and your reviews show buyers are happy with it. Products like this, where people want to understand how it works before buying, do well on Amazon when the listing shows the app clearly.
Angle: Putting the companion app front and center in the Amazon images could set Water.io apart from the plain tracker bottles shoppers compare it against.

Happy Viking. Opener: A plant-based shake that covers protein, fiber and probiotics in one go is a simple promise, and thousands of reviews near five stars suggest Happy Viking keeps it. That kind of loyalty usually means there are a lot more people out there who would love it if they found it.
Angle: Plenty of protein shoppers start their search on Amazon, and a subscribe option there would let new fans settle into the same daily habit your current customers already have.

Joybees. Opener: The honeycomb footbed and Popinz charms make Joybees an easy yes for families, and it's great to see you in stores and on sites beyond the US. Shoes that are washable and easy to slip on are exactly what busy parents reach for.
Angle: Pairing Popinz charms with clogs or sandals as ready-made gift sets on Amazon could turn a single shoe purchase into a bigger, more fun first order.

Wove Made. Opener: Sending couples a replica ring to try on at home before you build the real one is such a thoughtful idea. It takes a lot of the nerves out of one of the biggest purchases people ever make.
Angle: Ready-to-ship wedding bands or fine jewelry pieces could be Wove's entry point on Amazon, introducing couples to the brand before they book a design consultation.

Krazy Kreations. Opener: Krazy Kreations is known among card makers for its vinyl outline stickers, and the handmade micro-fine glitter paper is a lovely detail. The crafting ideas you share on YouTube and Pinterest make it easy to see what people can make with them.
Angle: Low-priced craft supplies tend to work better on Amazon as themed or seasonal kits, which makes each order worth shipping and gives crafters a reason to come back.

JaxKelly. Opener: Shower Affirmations is such a memorable product, and thousands of reviews averaging close to five stars say your customers agree. You're already on Etsy and in wholesale, so Amazon would just be one more place where those reviews do the selling.
Angle: Shower Affirmations make an easy gift, so an Amazon setup built around gifting seasons could carry them next to the jewelry for shoppers who never come across your Etsy shop.
""".strip()


@dataclass
class FactSheet:
    """Shopper-visible facts only. Empty facts means the record is too thin."""

    brand: str = ""
    category: str = ""
    recipe: str = ""
    facts: list[str] = field(default_factory=list)
    blocked: str = ""
    high_rating: bool = False

    @property
    def thin(self) -> bool:
        return not self.facts


def as_text(value: Any) -> str:
    """A display string. Lists, dicts, and nulls never become a Python repr."""

    if value is None or isinstance(value, bool):
        return ""
    if isinstance(value, float):
        if value != value:
            return ""
        return ""
    if isinstance(value, int):
        return ""
    if isinstance(value, dict):
        for key in ("name", "title", "value", "description"):
            text = as_text(value.get(key))
            if text:
                return text
        return ""
    if isinstance(value, (list, tuple, set)):
        parts: list[str] = []
        for item in value:
            text = as_text(item)
            if text and text not in parts:
                parts.append(text)
        return ", ".join(parts)
    text = " ".join(str(value).replace("\x00", " ").split())
    if text.lower() in {"none", "nan", "null"}:
        return ""
    if text[:1] in "{[":
        return ""
    return text


def _words_for_count(value: Any) -> str:
    try:
        number = int(float(value))
    except (TypeError, ValueError):
        return ""
    if number >= 1_000_000:
        return "more than a million"
    if number >= 500_000:
        return "more than half a million"
    if number >= 100_000:
        return "more than a hundred thousand"
    if number >= 1_000:
        return "thousands"
    if number >= 100:
        return "hundreds"
    return ""


def _clean_blurb(text: str, brand: str) -> str:
    raw = " ".join(str(text or "").split())
    if not raw:
        return ""
    if _PROMO.search(raw):
        raw = _PROMO.sub(" ", raw)
        raw = " ".join(raw.split(" ,")).strip(" ,.-")
    folded = re.sub(r"[^a-z0-9]+", " ", raw.lower()).strip()
    brand_folded = re.sub(r"[^a-z0-9]+", " ", brand.lower()).strip()
    if not folded or folded == brand_folded:
        return ""
    if re.search(r"\d", raw):
        return ""
    return raw[:220]


def _record(lead: dict[str, Any]) -> dict[str, Any]:
    store: dict[str, Any] = {}
    raw = lead.get("store")
    if isinstance(raw, dict):
        store.update(raw)
    profile = lead.get("store_profile")
    if isinstance(profile, dict):
        for key, value in profile.items():
            store.setdefault(key, value)
    for key in (
        "description", "title", "merchant_name", "categories", "sales_channels",
        "features", "vendor_count", "cluster_domains", "locs", "ships_to_countries",
    ):
        if lead.get(key) not in (None, "", [], {}) and key not in store:
            store[key] = lead.get(key)
    store.setdefault("name", lead.get("domain") or "")
    store.setdefault("merchant_name", lead.get("brand") or "")
    store.setdefault("categories", lead.get("categories") or lead.get("niche") or "")
    return store


def _channel_names(store: dict[str, Any]) -> list[str]:
    names: list[str] = []
    for item in store.get("sales_channels") or []:
        text = item if isinstance(item, str) else ""
        if isinstance(item, dict):
            text = str(item.get("name") or "")
        label = text.strip()
        if label and label not in names:
            names.append(label)
    for item in store.get("sales_channel_urls") or []:
        text = item if isinstance(item, str) else ""
        if isinstance(item, dict):
            text = str(item.get("url") or item.get("name") or "")
        host = text.lower()
        if "amazon." in host and "Amazon" not in names:
            names.append("Amazon")
        elif "etsy." in host and "Etsy" not in names:
            names.append("Etsy")
        elif "faire." in host and "Faire" not in names:
            names.append("Faire")
    return names


def _high_review(store: dict[str, Any]) -> tuple[bool, str]:
    best = 0.0
    count_words = ""
    for key in ("yotpo", "okendo", "judgeme", "judge_me", "loox", "trustpilot"):
        blob = store.get(key)
        if not isinstance(blob, dict):
            continue
        try:
            average = float(blob.get("avg_rating") or 0)
        except (TypeError, ValueError):
            average = 0.0
        if average < HIGH_RATING or average <= best:
            continue
        best = average
        count_words = _words_for_count(blob.get("review_count"))
    if best < HIGH_RATING:
        return False, ""
    if count_words:
        return True, f"{count_words} of reviews average close to five stars"
    return True, "reviews average close to five stars"


def fact_sheet(lead: dict[str, Any]) -> FactSheet:
    """Curated shopper-visible facts. Tech and traffic stats are left out."""

    store = _record(lead)
    brand = as_text(store.get("merchant_name") or lead.get("brand") or store.get("name"))
    category = as_text(store.get("categories") or lead.get("categories") or lead.get("niche"))
    recipe = str(lead.get("recipe") or "").strip()
    sheet = FactSheet(brand=brand or "This brand", category=category, recipe=recipe)
    if _RESTRICTED.search(category) or _RESTRICTED.search(brand):
        sheet.blocked = "restricted category"
        return sheet
    try:
        vendors = int(float(store.get("vendor_count") or 0))
    except (TypeError, ValueError):
        vendors = 0
    if vendors >= RETAILER_VENDOR_COUNT:
        sheet.blocked = "multi-brand retailer"
        return sheet

    facts: list[str] = []
    for key in ("description", "title"):
        blurb = _clean_blurb(as_text(store.get(key)), brand)
        if blurb:
            facts.append(blurb)
            break
    product = store.get("most_recently_published_product")
    if isinstance(product, dict):
        title = _clean_blurb(as_text(product.get("title")), brand)
        if title:
            facts.append(f"recent product: {title}")
    channels = _channel_names(store)
    for name in ("Amazon", "Etsy", "Faire"):
        if any(name.lower() in item.lower() for item in channels):
            facts.append(f"sells on {name}")
    if str(store.get("store_locator_page") or "").strip():
        facts.append("has a store locator")
    if str(store.get("retailer_page") or "").strip():
        facts.append("sold through retailers")
    feature_text = as_text(store.get("features"))
    if re.search(r"wholesale", feature_text, re.I):
        facts.append("sells wholesale")
    if re.search(r"retail|store locator", feature_text, re.I) and "has a store locator" not in facts:
        facts.append("sold in stores")
    high, review_line = _high_review(store)
    sheet.high_rating = high
    if review_line:
        facts.append(review_line)
    social_bits: list[str] = []
    for item in store.get("contact_info") or []:
        if not isinstance(item, dict):
            continue
        kind = as_text(item.get("type"))
        bio = _clean_blurb(as_text(item.get("description")), brand)
        reach = _words_for_count(item.get("followers"))
        if bio:
            social_bits.append(f"{kind} bio: {bio}" if kind else bio)
        elif reach and kind:
            social_bits.append(f"{reach} {kind} followers")
    facts.extend(social_bits[:3])
    abroad = []
    for item in list(store.get("ships_to_countries") or []) + list(store.get("locs") or []):
        label = as_text(item)
        if label and label.upper() not in {"US", "USA"} and label not in abroad:
            abroad.append(label)
    for host in store.get("cluster_domains") or []:
        text = str(host or "")
        if re.search(r"\.(?:co\.uk|de|fr|ca|com\.au|es)(?:/|$)", text, re.I):
            abroad.append("an international site version")
            break
    if abroad:
        facts.append("has shoppers or site versions beyond the US")
    app_names = " ".join(str(item) for item in (store.get("apps") or [])).lower()
    if any(token in app_names for token in _SUBSCRIBE_APPS):
        facts.append("offers a subscribe option")
    # De-dupe while keeping order.
    seen: list[str] = []
    for fact in facts:
        if fact and fact not in seen:
            seen.append(fact)
    sheet.facts = seen
    if not seen:
        sheet.blocked = "thin record"
    return sheet


def style_violation(
    text: str,
    *,
    role: str = "opener",
    recipe: str = "",
    high_rating: bool = False,
) -> str:
    """Empty when the line is allowed. Otherwise a short reason."""

    raw = (text or "").strip()
    if not raw:
        return "empty"
    if "—" in raw or "–" in raw:
        return "em dash"
    if re.search(r"\d", raw):
        return "digit"
    if _URL.search(raw):
        return "url"
    if "?" in raw:
        return "question"
    if _HYPE.search(raw):
        return "hype"
    if _NEGATIVE.search(raw):
        return "negative lead"
    if _TECH.search(raw) or _BACKEND.search(raw):
        return "tech or backend change"
    if _LOW_RATING.search(raw):
        return "low rating"
    if re.search(r"\bstars?\b|\brating\b", raw, re.I) and not high_rating:
        return "low rating"
    if role == "angle" and raw.lower().startswith("anata helps"):
        return "anata helps"
    if any(mark in raw for mark in ("[", "]", "{", "}", "{{", "}}")):
        return "repr or placeholder"
    if re.search(r"\b(?:None|nan)\b", raw) or "  " in raw:
        return "repr or placeholder"
    words = raw.split()
    sentences = [part.strip() for part in re.split(r"[.!?]", raw) if part.strip()]
    if any(len(part.split()) < 6 for part in sentences):
        return "too short"
    limit = ANGLE_MAX_WORDS if role == "angle" else OPENER_MAX_WORDS
    if len(words) > limit:
        return "too long"
    if recipe:
        # Every recipe keeps the same ban. plan_upgrade and replatform are
        # the ones David called out; the check is not limited to them.
        if _TECH.search(raw) or _BACKEND.search(raw):
            return "tech or backend change"
    return ""


def _prompt(sheet: FactSheet) -> str:
    facts = "\n".join(f"- {fact}" for fact in sheet.facts)
    return (
        f"Brand: {sheet.brand}\n"
        f"Category: {sheet.category or 'unknown'}\n"
        "Shopper-visible facts:\n"
        f"{facts}\n\n"
        "Write JSON with keys opener and angle. Use only the facts above. "
        "Opener is one or two sentences in Abby's voice tying one fact to why Amazon matters. "
        "Angle is one sentence with a specific Amazon idea. It must not repeat the opener "
        "and must not start with Anata helps. No digits, no em dashes, no URLs, no hype, "
        "no platform, plan, app, pixel, or the words moved or upgraded. "
        "A subscribe option may be named. Never name a review app. "
        "Cite reviews only when the facts say the average is close to five stars. "
        "Do not invent products, stores, or follower counts.\n\n"
        f"Examples of the voice:\n{_EXAMPLES}"
    )


def _parse_pair(raw: str) -> tuple[str, str]:
    text = (raw or "").strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?", "", text).strip()
        text = re.sub(r"```$", "", text).strip()
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, re.S)
        if not match:
            return "", ""
        try:
            payload = json.loads(match.group(0))
        except json.JSONDecodeError:
            return "", ""
    if not isinstance(payload, dict):
        return "", ""
    return str(payload.get("opener") or "").strip(), str(payload.get("angle") or "").strip()


def _call_model(prompt: str, api_key: Optional[str]) -> str:
    from sales_support_agent.services.llm_client import llm_client

    from outbound_openers import MODEL

    client = llm_client(caller="outbound_personalization", api_key=api_key)
    message = client.messages.create(
        model=MODEL,
        max_tokens=500,
        messages=[{"role": "user", "content": prompt}],
    )
    return "".join(getattr(block, "text", "") for block in message.content)


def generate(
    lead: dict[str, Any],
    *,
    llm: Optional[Callable[[str], str]] = None,
    api_key: Optional[str] = None,
) -> tuple[str, str, str, str]:
    """Return (opener, angle, source, reason).

    Source is ``ai`` or ``none``. There is no category template. A missing
    model, a failed line, a non-US headquarters, or a thin record leaves both
    lines empty so the campaign keeps its existing default copy.
    """

    from outbound_pipeline import country_eligibility

    store = _record(lead)
    allowed, country_reason = country_eligibility(store)
    if not allowed:
        logger.info("[outbound-personalization] %s flagged: %s", store.get("name"), country_reason)
        return "", "", "none", country_reason
    sheet = fact_sheet(lead)
    if sheet.blocked:
        logger.info("[outbound-personalization] %s flagged: %s", sheet.brand, sheet.blocked)
        return "", "", "none", sheet.blocked
    prompt = _prompt(sheet)
    model = llm
    if model is None:
        from sales_support_agent.services.llm_client import llm_available

        if llm_available(api_key=api_key):
            model = lambda text: _call_model(text, api_key)  # noqa: E731
    if model is None:
        logger.info("[outbound-personalization] %s flagged: model unavailable", sheet.brand)
        return "", "", "none", "model unavailable"
    why = ""
    for _attempt in range(2):
        try:
            opener, angle = _parse_pair(model(prompt))
        except Exception:  # noqa: BLE001
            logger.exception("[outbound-personalization] model call failed")
            why = "model unavailable"
            break
        why = style_violation(
            opener, role="opener", recipe=sheet.recipe, high_rating=sheet.high_rating,
        ) or style_violation(
            angle, role="angle", recipe=sheet.recipe, high_rating=sheet.high_rating,
        )
        if opener and angle and not why and opener.strip() != angle.strip():
            return opener, angle, "ai", ""
        prompt = prompt + f"\n\nRejected ({why or 'empty'}). Write a different pair that follows every rule."
    reason = f"model line rejected: {why or 'empty'}"
    logger.info("[outbound-personalization] %s flagged: %s", sheet.brand, reason)
    return "", "", "none", reason
