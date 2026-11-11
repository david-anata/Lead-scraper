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
# Same hard caps as outbound_openers. About 20 and 15 is the aim.
OPENER_MAX_WORDS = 25
ANGLE_MAX_WORDS = 20

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
_REPR = re.compile(r"\[|\]|\{|\}|\b(?:none|nan|null)\b|\{\{", re.I)
_SUBSCRIBE_APPS = ("recharge", "skio", "smartrr", "subscription")

_EXAMPLES = """
amz_on. Opener: Lovevery's Play Kits are easy to picture, and a small age-based set would give a new shopper one clear way to try them.
Angle: A tighter set of images around Play Kits would make Lovevery easier to choose on Amazon.

Trial Offer. Opener: Happy Viking's plant-based shake is easy to picture, and a small set around it would give a new shopper one clear way to try it.
Angle: A plant-based shake set could give new shoppers an easy way to try Happy Viking.

plan_upgrade. Opener: Joybees is already in retail stores, so a small set of the same shoes would make a first order simple.
Angle: Shoppers who already see Joybees in stores need one easy set to start with.

amz_off. Opener: Wove's replica ring is easy to picture, and a small set around it would give a new shopper one clear way to try it.
Angle: Leading with the replica ring would give a new shopper a concrete reason to try Wove.
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
    trial_offer: bool = False

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
    raw = as_text(text)
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

    from outbound_pipeline import clean_brand_name

    store = _record(lead)
    brand = clean_brand_name(
        as_text(store.get("merchant_name") or lead.get("brand") or store.get("name"))
    )
    category = as_text(store.get("categories") or lead.get("categories") or lead.get("niche"))
    recipe = str(lead.get("recipe") or "").strip()
    from outbound_openers import trial_offer_lead

    trial = trial_offer_lead(lead)
    sheet = FactSheet(
        brand=brand or "This brand", category=category, recipe=recipe, trial_offer=trial,
    )
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
    representatives = store.get("representative_products")
    if not isinstance(representatives, list):
        representatives = []
    catalog_title = ""
    for item in representatives:
        if isinstance(item, dict) and as_text(item.get("title")):
            catalog_title = as_text(item.get("title"))
            break
    if catalog_title or store.get("product_phrase"):
        # A catalog product replaces the homepage title. The title is often
        # an SEO keyword string, not something a shopper buys.
        if catalog_title:
            facts.append(f"recent product: {catalog_title}")
    else:
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
    # Amazon presence is copy only for the amz_on segment. Other channel
    # names decide who is emailed and stay out of the fact sheet.
    if recipe == "amz_on" and not trial:
        channels = _channel_names(store)
        if any("amazon" in item.lower() for item in channels):
            facts.append("sells on Amazon")
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
    trial_offer: bool = False,
) -> str:
    """Empty when the line is allowed. Otherwise a short reason."""

    raw = (text or "").strip()
    if not raw:
        return "empty"
    if "—" in raw or "–" in raw or " - " in raw:
        return "em dash"
    if _REPR.search(raw) or "  " in raw:
        return "repr or placeholder"
    if re.search(r"\d", raw):
        return "digit"
    if _URL.search(raw):
        return "url"
    if "?" in raw or re.match(r"^(?:is|are|do|does|did|can|could|would|will)\b", raw, re.I):
        return "question"
    if re.search(r"\bstood out\b|\bwhile i was looking at\b", raw, re.I):
        return "stood out template"
    if re.search(r"\b[a-z0-9]+_[a-z0-9_]+\b", raw, re.I):
        return "slug"
    if _HYPE.search(raw):
        return "hype"
    if _NEGATIVE.search(raw):
        return "negative lead"
    from outbound_openers import BACKEND_LANGUAGE

    if _TECH.search(raw) or _BACKEND.search(raw) or BACKEND_LANGUAGE.search(raw):
        return "tech or backend change"
    if _LOW_RATING.search(raw):
        return "low rating"
    if re.search(r"\bstars?\b|\brating\b", raw, re.I) and not high_rating:
        return "low rating"
    from outbound_openers import (
        MIN_WORDS,
        _AMAZON_WORD,
        sentence_count,
        trial_amazon_claim,
        trial_offer_lead,
    )

    if role == "angle" and raw.lower().startswith("anata helps"):
        return "anata helps"
    trial = trial_offer or trial_offer_lead({"recipe": recipe})
    if trial and (_AMAZON_WORD.search(raw) or trial_amazon_claim(raw)):
        return "claims the brand already sells on Amazon"
    if (
        not trial
        and recipe not in {"amz_on", "amz on"}
        and _AMAZON_WORD.search(raw)
    ):
        return "claims the brand already sells on Amazon"
    words = raw.split()
    limit = ANGLE_MAX_WORDS if role == "angle" else OPENER_MAX_WORDS
    if len(words) > limit:
        return "too long"
    if len(words) < MIN_WORDS:
        return "too short"
    if sentence_count(raw) > 1:
        return "more than one sentence"
    if re.search(r"\bfirst image\b|\bshould be the first\b", raw, re.I):
        return "tells the brand what their first image should be"
    from outbound_openers import stuffed_copy_reason

    stuffed = stuffed_copy_reason(raw)
    if stuffed:
        return stuffed
    if recipe:
        # Every recipe keeps the same ban. plan_upgrade and replatform are
        # the ones David called out; the check is not limited to them.
        if _TECH.search(raw) or _BACKEND.search(raw):
            return "tech or backend change"
    return ""


def _prompt(sheet: FactSheet) -> str:
    facts = "\n".join(f"- {fact}" for fact in sheet.facts)
    from outbound_openers import plain_category

    category = plain_category(sheet.category) or "unknown"
    if sheet.trial_offer:
        amazon_rule = (
            "This is a Trial Offer. The opener and the angle must not mention Amazon "
            "and must not claim the brand already sells on Amazon. Never write "
            "'on Amazon', 'your Amazon store', 'your listings', or 'already on Amazon'. "
            "Say new shoppers. "
        )
    elif sheet.recipe == "amz_on":
        amazon_rule = (
            "This brand is in the amz_on segment, so the line may say it is on Amazon "
            "when a fact supports that. Never assume a product that is not in the facts. "
        )
    else:
        amazon_rule = (
            "Do not mention Amazon. Never assume they sell there. "
        )
    return (
        f"Brand: {sheet.brand}\n"
        f"Category: {category}\n"
        "Shopper-visible facts:\n"
        f"{facts}\n\n"
        "Write JSON with keys opener and angle. Use only the brand and the facts above. "
        "Do not invent a product, a store, or a number. "
        "Sound like a person who looked at their store. Plain and warm. "
        "Vary the sentence shape. Do not start both lines with the brand name. "
        f"{amazon_rule}"
        "Opener: one sentence of about 20 words, never more than 25, and at least 6 words. "
        "Angle: one sentence of about 15 words, never more than 20, and at least 6 words. "
        "The angle ties the brand to Amazon listings, images, or ads. "
        "It must not restate the opener, must not start with Anata helps, "
        "and must not tell them what their first image should be or critique the store. "
        "No digits, no em dashes, no spaced hyphens, no URLs, no hype, no questions, "
        "no category slugs, and never the words 'stood out'. "
        "No app installs, automation, bids, direct-to-consumer, platform names, "
        "platform moves, plan upgrades, pixels, or change dates. "
        "A subscribe option may be named. Never name a review app. "
        "Cite reviews only when the facts say the average is close to five stars. "
        "Do not invent products, stores, or follower counts.\n\n"
        f"Examples of the voice:\n{_EXAMPLES}\n\n"
        "The examples show voice only. Do not copy their length. "
        "The opener must be one sentence of about 20 words and never more than 25. "
        "The angle must be one sentence of about 15 words and never more than 20."
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
) -> tuple[str, str, str]:
    """Return (opener, angle, source). Source is ai, fallback, or none.

    Thin, restricted, and multi-brand records get no lines. A failed model
    line falls back to a shopper-visible idea when one exists. A category slug
    is not that idea. A line over the hard word max is regenerated once, then
    the same idea is used when the record has a product, story, review, store,
    or social fact.
    """

    sheet = fact_sheet(lead)
    if sheet.blocked:
        logger.info(
            "[outbound-personalization] %s flagged: %s",
            sheet.brand, sheet.blocked,
        )
        return "", "", "none"
    prompt = _prompt(sheet)
    model = llm
    if model is None:
        from sales_support_agent.services.llm_client import llm_available

        if llm_available(api_key=api_key):
            model = lambda text: _call_model(text, api_key)  # noqa: E731
    if model is not None:
        why = ""
        for _attempt in range(2):
            try:
                opener, angle = _parse_pair(model(prompt))
            except Exception:  # noqa: BLE001
                logger.exception("[outbound-personalization] model call failed")
                break
            why = style_violation(
                opener, role="opener", recipe=sheet.recipe,
                high_rating=sheet.high_rating, trial_offer=sheet.trial_offer,
            ) or style_violation(
                angle, role="angle", recipe=sheet.recipe,
                high_rating=sheet.high_rating, trial_offer=sheet.trial_offer,
            )
            if opener and angle and not why and opener.strip() != angle.strip():
                return opener, angle, "ai"
            prompt = (
                prompt
                + f"\n\nRejected ({why or 'empty'}). Write a different pair that follows every rule. "
                "Opener: one sentence, about 20 words, never more than 25. "
                "Angle: one sentence, about 15 words, never more than 20."
            )
        logger.info("[outbound-personalization] model line rejected (%s)", why or "empty")
    from outbound_openers import idea_lines, template_fallback_enabled

    if template_fallback_enabled(lead):
        opener, angle = idea_lines(lead)
        if opener:
            return opener, angle, "fallback"
    logger.info("[outbound-personalization] %s flagged: no safe line", sheet.brand)
    return "", "", "none"
