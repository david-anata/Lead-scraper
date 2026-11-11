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


class ModelTimeout(TimeoutError):
    """The opener model did not answer within the lead budget."""

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
Opener: Your adai dosa mix caught my eye. It's not something I see from many brands.
Angle: I would use one photo of the dosa mix and leave the rest of the page alone.

Opener: The chocolate chunk cookie mix is the one I would come back for.
Angle: The cookie mix is what I would put in the ad.

Opener: Love that Perfy does flavors like blood orange yuzu instead of another plain cola.
Angle: Show that flavor before any other can.

Opener: Inova's clarifying shampoo is the one I would tell a friend about.
Angle: A shopper can understand the shampoo from a single listing image.

Opener: RSL's shave cream is a specific thing, not a general grooming line.
Angle: The cream is a concrete product for the first listing.

Opener: Biggs and Featherbelle's bar soap has a scent a shopper can name.
Angle: That bar soap is what I would build the ad around.
""".strip()


@dataclass
class FactSheet:
    """Shopper-visible facts only. Empty facts means the record is too thin."""

    brand: str = ""
    category: str = ""
    recipe: str = ""
    product: str = ""
    catalog: list[dict[str, Any]] = field(default_factory=list)
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
    display = as_text(
        lead.get("display_brand") or store.get("site_name") or store.get("name")
    )
    brand = clean_brand_name(
        as_text(store.get("merchant_name") or lead.get("brand") or store.get("name")),
        display_name=display or lead.get("brand") or "",
    )
    if not brand or brand.lower() == "this brand":
        brand = clean_brand_name(lead.get("brand") or "", display_name=display) or display
    if brand.lower() == "this brand":
        brand = ""
    category = as_text(store.get("categories") or lead.get("categories") or lead.get("niche"))
    recipe = str(lead.get("recipe") or "").strip()
    from outbound_openers import trial_offer_lead

    trial = trial_offer_lead(lead)
    sheet = FactSheet(
        brand=brand, category=category, recipe=recipe, trial_offer=trial,
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
    catalog = store.get("catalog_products")
    if not isinstance(catalog, list):
        catalog = lead.get("catalog_products") if isinstance(lead.get("catalog_products"), list) else []
    catalog = [item for item in catalog if isinstance(item, dict) and item.get("title")]
    if catalog:
        from outbound_product_facts import catalog_blocked_reason

        blocked = catalog_blocked_reason(catalog, brand)
        if blocked:
            sheet.blocked = blocked
            return sheet
        sheet.catalog = catalog[:5]
        sheet.facts = [str(item.get("title") or "") for item in sheet.catalog]
        return sheet
    phrase = as_text(store.get("product_phrase") or lead.get("product_phrase"))
    if phrase:
        # The picker already chose the short noun phrase. A catalog title,
        # including the newest product, must not reach the model.
        sheet.product = phrase
        sheet.facts = [phrase]
        return sheet
    for key in ("description",):
        blurb = _clean_blurb(as_text(store.get(key)), brand)
        if blurb:
            facts.append(blurb)
            break
    product = store.get("most_recently_published_product")
    if isinstance(product, dict):
        title = _clean_blurb(as_text(product.get("title")), brand)
        if title and "recent" not in title.lower():
            facts.append(title)
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


def _bare_product_opening(text: str, phrase: str) -> bool:
    """True when the opener starts on the raw product title."""

    raw = (text or "").strip()
    named = " ".join((phrase or "").split())
    if not raw or not named:
        return False
    if re.match(r"^(?:the|a|an|i|i'd|i’d|your|love)\b", raw, re.I):
        return False
    return raw.lower().startswith(named.lower())


def _ungrounded_benefit(text: str, catalog_text: str) -> bool:
    """True when the line claims an effect the catalog text does not say."""

    blob = (catalog_text or "").lower()
    for word in re.findall(
        r"\b(?:cool|cools|soothe|soothes|heal|heals|relieve|relieves|boost|boosts|"
        r"fast|instantly|overnight)\b",
        text or "",
        re.I,
    ):
        if re.search(rf"\b{re.escape(word.lower())}\b", blob) is None:
            return True
    return False


def style_violation(
    text: str,
    *,
    role: str = "opener",
    recipe: str = "",
    high_rating: bool = False,
    trial_offer: bool = False,
    retail: bool = False,
    catalog_text: str = "",
    product_phrase: str = "",
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
    recipe_key = (recipe or "").strip().lower().replace("-", " ").replace("_", " ")
    if trial and (_AMAZON_WORD.search(raw) or trial_amazon_claim(raw)):
        return "claims the brand already sells on Amazon"
    if recipe_key == "amz off":
        if re.search(
            r"\balready (?:on amazon|sells? on amazon|selling on amazon)\b"
            r"|\byour amazon\b|\btheir amazon\b|\blive on amazon\b|\bsells on amazon\b",
            raw, re.I,
        ):
            return "claims the brand already sells on Amazon"
    elif recipe_key != "amz on" and (_AMAZON_WORD.search(raw) or trial_amazon_claim(raw)):
        return "claims the brand already sells on Amazon"
    words = raw.split()
    limit = ANGLE_MAX_WORDS if role == "angle" else OPENER_MAX_WORDS
    if len(words) > limit:
        return "too long"
    if len(words) < MIN_WORDS:
        return "too short"
    sentence_limit = 2 if role == "opener" else 1
    if sentence_count(raw) > sentence_limit:
        return "more than two sentences" if role == "opener" else "more than one sentence"
    if re.search(
        r"\bpick up right away\b|\bshoppers will\b|\bwill love\b|\bguaranteed to\b|"
        r"\bnew shoppers can\b|\bshoppers can (?:spot|picture)\b|\bcan picture\b",
        raw, re.I,
    ):
        return "guesses what shoppers will do"
    if re.search(
        r"\b(?:support|supports|improve|improves|relief|relieve|relieves)\w*\b.{0,40}"
        r"\b(?:health|wellness|pain|body)\b"
        r"|\btotal body health\b",
        raw, re.I,
    ):
        return "health claim"
    if re.search(r"\bplain snack\b|\benough of a product\b", raw, re.I):
        return "put-down"
    if re.search(r"\bhas that\b.{0,60}\blook\b", raw, re.I):
        return "garbled phrase"
    if _bare_product_opening(raw, product_phrase):
        return "bare product title"
    if catalog_text and _ungrounded_benefit(raw, catalog_text):
        return "guessed benefit"
    if re.search(r"\bthis brand\b|\blineup\b", raw, re.I):
        return "generic brand or category label"
    if re.search(r"\bin the snack aisle\b|\bon the shelf\b|\bat the store\b", raw, re.I):
        return "made-up sighting"
    if not retail and re.search(r"\bsaw\b.{0,30}\bat\b", raw, re.I):
        return "made-up sighting"
    if re.search(r"\bfirst image\b|\bshould be the first\b", raw, re.I):
        return "tells the brand what their first image should be"
    from outbound_openers import copy_tic

    tic = copy_tic(raw)
    if tic:
        return tic
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
    from outbound_openers import plain_category

    category = plain_category(sheet.category) or "unknown"
    if sheet.catalog:
        lines = []
        for index, item in enumerate(sheet.catalog, start=1):
            lines.append(
                f"{index}. Title: {item.get('title') or ''}. "
                f"Type: {item.get('product_type') or ''}. "
                f"Description: {item.get('body') or ''}"
            )
        facts = "\n".join(lines)
    elif sheet.product:
        facts = f"- {sheet.product}"
    else:
        facts = "\n".join(f"- {fact}" for fact in sheet.facts)
    if sheet.trial_offer:
        amazon_rule = (
            "This is a Trial Offer. The opener and the angle must not mention Amazon "
            "and must not claim the brand already sells on Amazon. Never write "
            "'on Amazon', 'your Amazon store', 'your listings', or 'already on Amazon'. "
            "The angle talks about the listing, a product image, or an ad, without naming Amazon. "
            "Say new shoppers, and do not guess what they can see or do. "
        )
    elif sheet.recipe in {"amz_on", "amz on"}:
        amazon_rule = (
            "This brand is in the amz_on segment, so the line may say it is on Amazon "
            "when a fact supports that. The angle may mention Amazon listings, images, or ads. "
            "Never assume a product that is not in the facts. "
        )
    elif sheet.recipe in {"amz_off", "amz off"}:
        amazon_rule = (
            "This brand is not on Amazon yet. The angle may talk about launching on Amazon. "
            "Do not claim they already sell there. "
        )
    else:
        amazon_rule = (
            "Do not mention Amazon. The angle talks about the listing, a product image, "
            "or an ad, without naming Amazon. "
        )
    return (
        f"Brand: {sheet.brand}\n"
        f"Category: {category}\n"
        + (
            "Products a shopper can see:\n"
            f"{facts}\n"
            "Pick ONE of these. Return JSON with keys product_phrase, opener, and angle. "
            "product_phrase is the natural way a shopper would name that product, using words "
            "from the title, type, or description. Do not shorten it into catalog junk.\n"
            if sheet.catalog else
            (
                f"Product, said the way a shopper would: {sheet.product}\n"
                if sheet.product else
                "Shopper-visible facts:\n"
                f"{facts}\n"
            )
        )
        + "\n"
        "Write JSON with keys product_phrase, opener, and angle. "
        "Use only the brand and one product from the list above. "
        "Do not invent a product, a store, a result, or a number. "
        "Do not say you saw the product in a store or a snack aisle unless the facts name a retailer. "
        "Name the brand in only one of the two lines, and use an apostrophe when the brand owns the next word. "
        "Do not say This brand, and do not call the products a lineup. "
        "The product phrase is the product name, not a tagline from the page. "
        "Do not guess what shoppers will do. Do not critique the store. "
        "Do not paraphrase the product description or list ingredients the way the label does. "
        "Say one plain thing a person would say after looking at that product. "
        "Do not say recent, newest, I noticed, small set, easy to picture, "
        "one clear start, or simple place to start. "
        "Do not start the opener on the raw product title. Start with 'The', 'Your', or a short human line. "
        "Do not claim a health benefit, relief, or an effect the catalog text does not state. "
        "Do not put the product down. Never write 'plain snack' or 'enough of a product'. "
        "Do not twist a phrase from the site into broken English. "
        "The angle must not copy one shape. Do not repeat 'main image' or 'clear product to center on'. "
        "Do not write 'an ad can stay on X so the photo carries the message'. "
        "Sound like a person who looked at their store. Plain and warm. "
        f"{amazon_rule}"
        "Opener: one or two short sentences, about 25 words max, and at least 6 words. "
        "Angle: one sentence of about 15 words, never more than 20, and at least 6 words. "
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
        "The opener must be one sentence of about 20 words, never more than 25, or two short sentences. "
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


def _parse_copy(raw: str) -> tuple[str, str, str]:
    """``(product_phrase, opener, angle)`` from the model JSON."""

    text = (raw or "").strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?", "", text).strip()
        text = re.sub(r"```$", "", text).strip()
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, re.S)
        if not match:
            return "", "", ""
        try:
            payload = json.loads(match.group(0))
        except json.JSONDecodeError:
            return "", "", ""
    if not isinstance(payload, dict):
        return "", "", ""
    return (
        str(payload.get("product_phrase") or "").strip(),
        str(payload.get("opener") or "").strip(),
        str(payload.get("angle") or "").strip(),
    )


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
    grader: Optional[Callable[[str], str]] = None,
    api_key: Optional[str] = None,
    batch: Optional[list[str]] = None,
    angles: Optional[list[str]] = None,
    allow_retry: Optional[Callable[[], bool]] = None,
) -> tuple[str, str, str]:
    """Return (line, unused angle, source).

    The code picks one product and one detail. The model writes one sentence.
    A grader may ask for one rewrite. There is no template line.
    ``batch``, ``angles``, and ``allow_retry`` are ignored. Variety is not scored.
    """

    from outbound_opener_line import compose_line, line_fact_candidates

    sheet = fact_sheet(lead)
    if sheet.blocked:
        logger.info(
            "[outbound-personalization] %s flagged: %s",
            sheet.brand, sheet.blocked,
        )
        if sheet.blocked == "blocked_category":
            return "", "", "skipped:blocked_category"
        return "", "", "none"
    store = lead.get("store_profile") if isinstance(lead.get("store_profile"), dict) else {}
    products = list(sheet.catalog)
    if not products and sheet.product:
        products = [{"title": sheet.product, "body": sheet.product}]
    candidates = line_fact_candidates(
        products,
        brand=sheet.brand,
        recipe=sheet.recipe,
        vendors=store.get("vendors") if isinstance(store.get("vendors"), list) else None,
        domain=str(lead.get("domain") or ""),
    )
    facts = candidates[0]
    lead["opener_facts"] = facts.as_text()
    if facts.skip:
        lead["grader_reason"] = facts.skip
        return "", "", f"skipped:{facts.skip}"
    writer = llm
    if writer is None:
        from sales_support_agent.services.llm_client import llm_available

        if llm_available(api_key=api_key):
            writer = lambda text: _call_model(text, api_key)  # noqa: E731
    if writer is None:
        return "", "", "skipped:no_model"
    result = compose_line(
        facts, writer=writer, grader=grader or writer, alternatives=candidates[1:],
    )
    lead["opener_facts"] = result.facts
    lead["grader_pass"] = result.grader_pass
    lead["grader_reason"] = result.grader_reason
    return result.line, "", result.source
