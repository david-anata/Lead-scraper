"""One opener line from one product fact.

The code picks the product and one shopper-visible detail. A model writes
one sentence from those facts. A second model grades it. The only code
checks are grounding, backend language, Amazon wording, health claims, and
URLs. A failed grade gets one rewrite, then the lead is skipped.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)

# Published non-reasoning model. GET /v1/models needs an API key, which this
# environment does not have. The public model list's non-reasoning chat model
# is this one, and xAI describes it as the speed variant (reasoning: no).
FAST_MODEL = "grok-4.20-0309-non-reasoning"
# A step up from the writer. Still inside the 8s call cap and the 35s lead budget.
GRADER_MODEL = "grok-4.3"

_EXAMPLES = """
Love that Perfy does flavors like blood orange yuzu and turmeric.
The olive wood mugs from ArtisRaw look sturdy enough for daily use.
Biggs makes a coffee bar soap with ground coffee in the bar.
""".strip()

_FLUFF = re.compile(
    r"\b(?:best|official|premium|luxury|authentic|ultimate|"
    r"top|rated|selling|quality|perfect|amazing|deluxe|exclusive)\b",
    re.I,
)
_PACK = re.compile(
    r"\([^)]{0,40}\b(?:cans?|packs?|oz|ml|count|ct)\b[^)]{0,20}\)"
    r"|\b\d+(?:\.\d+)?\s*(?:fl\.?\s*)?(?:oz|ml|cans?|count|ct|pk)\b"
    r"|\bSingle\b.{0,40}\bPackaged Bar\b",
    re.I,
)
_NOUN = re.compile(
    r"\b(?:cola|soda|soap|bar|serum|kit|mug|mugs|chess|bracelet|necklace|"
    r"pillow|chips|mix|cookie|shampoo|tile|oil|tea|gum|candle|balm|treatment|"
    r"set|frame|frames)\b",
    re.I,
)
_FILLER_WORD = {
    "this", "that", "these", "those", "unique", "very", "really", "just", "little",
    "some", "your", "with", "from", "made", "real", "fun", "nice", "great", "good",
    "experience", "special", "our", "its", "the", "and", "for", "into",
    "craft", "comes", "come",
}
_GENERIC_DETAIL = {
    "soda", "cola", "drink", "beverage", "product", "item", "flavor", "scent",
    "soap", "bar", "set", "piece", "kit", "serum", "oil",
}
_INVENTED = re.compile(
    r"\bi (?:just )?(?:bought|tried|ordered|added|used)\b"
    r"|\bi added\b"
    r"|\badded\b.*\bto my cart\b"
    r"|\bmy cart\b"
    r"|\bpicked up\b"
    r"|\blets me\b"
    r"|\bmy (?:skin|hips|hair|face)\b"
    r"|\b(?:bought|ordered|tried)\b.{0,80}\bon amazon\b"
    r"|\bon amazon\b.{0,40}\b(?:bought|ordered|cart)\b",
    re.I,
)
_BENEFIT_LIST = re.compile(
    r"\b(?:firming|elasticity|wrinkles?|fine lines?|reduce frizz|manageability|"
    r"enhance shine|anti-aging|anti aging|redness|soothing|sensitive skin|calming)\b",
    re.I,
)
_HYPE = re.compile(
    r"\b(?:perfect|amazing|obsessed|mesmerizing|exquisite|stunning|breathtaking|"
    r"premium|leading-edge|cutting-edge|elevate|luxurious|game-changer|"
    r"elegantly|gracefully|effortlessly)\b"
    r"|\bhigh[- ]quality\b",
    re.I,
)
_DIETARY = re.compile(
    r"\b(?:oil-free|gluten-free|non-gmo|vegan|sugar-free|no sugars?)\b",
    re.I,
)
_SKIN_FEEL = re.compile(
    r"\b(?:leaves skin|skin feeling|feeling fresh|astringent|slightly tight)\b",
    re.I,
)
_NONFOOD = re.compile(r"\b(?:soap|candle|balm|wax|lotion)\b", re.I)
_VENDOR_CATEGORY = re.compile(
    r"\b(?:tinted|polarized|eyeglasses|sunglasses|frames|optical|readers)\b"
    r"|^(?:women|men|kids|unisex|youth)\b",
    re.I,
)
_GENERIC_VENDOR = re.compile(r"^(?:vinyl|default|upload|builder|title)$", re.I)
_ADDON_VENDOR = re.compile(
    r"\b(?:lens replacement|lens coating|buy 1|buy one|add-?on|upsell)\b",
    re.I,
)
_MARKETING_OPEN = re.compile(
    r"^\s*(?:embrace|elevate|experience|easily make|discover|introducing|unlock|unleash)\b",
    re.I,
)
_SIZE_DETAIL = re.compile(
    r"\b\d+(?:\.\d+)?\s*(?:inches|inch|in\.?|cm|mm|ft)\b|\(\s*\d+(?:\.\d+)?\s*cm\s*\)",
    re.I,
)
_HOW_USED = re.compile(
    r"\b(?:brewed|carved|hand[- ]?carved|infused|steeped|insert|inserted|roasted|"
    r"ground|pressed|distilled|stitched|woven)\b",
    re.I,
)
_FLAVORS = (
    "blood orange", "yuzu", "turmeric", "peppermint", "strawberry", "raspberry",
    "blueberry", "grapefruit", "chocolate", "vanilla", "lavender", "cinnamon",
    "caramel", "coconut", "matcha", "pineapple", "cherry", "coffee", "chai",
    "mango", "peach", "lemon", "ginger", "honey", "mint", "citrus", "orange",
    "beet",
)
_MATERIALS = (
    "olive wood", "sterling silver", "copper peptide", "ceramic", "leather",
    "cotton", "linen", "marble", "bamboo", "acetate", "keratin", "brass",
    "wool", "silk", "glass", "gold", "silver",
)
_FORMATS = (
    "bar soap", "cookie mix", "serum", "capsules", "capsule", "gummies",
    "necklace", "bracelet", "pillow", "shampoo", "balm", "mugs", "mug",
    "chips", "soda", "cola", "tea",
)
_HEALTH = re.compile(
    r"\b(?:cure|cures|treat|treats|heal|heals|relief|relieve|diagnos\w*|clinical)\b"
    r"|\b(?:support|supports|improve|improves)\w*\b.{0,40}\b(?:health|pain|inflammation|immunity|symptoms?)\b"
    r"|\btotal body health\b",
    re.I,
)
_URL = re.compile(r"https?://|www\.", re.I)
_AMAZON = re.compile(r"\bamazon\b", re.I)
_ALREADY_AMAZON = re.compile(
    r"\balready\b.{0,30}\bamazon\b|\bamazon\b.{0,30}\b(?:already|store)\b"
    r"|\b(?:sells|selling|live) on amazon\b",
    re.I,
)
_STOP = {"the", "a", "an", "and", "with", "for", "from", "of", "by", "to"}


@dataclass
class LineFacts:
    """The only facts the model is allowed to use."""

    brand: str = ""
    product: str = ""
    detail: str = ""
    detail_kind: str = ""
    recipe: str = ""
    skip: str = ""
    summary: str = ""

    def as_text(self) -> str:
        if self.summary:
            return self.summary
        if not self.product:
            return ""
        kind = f" ({self.detail_kind})" if self.detail_kind else ""
        detail = f"; detail: {self.detail}{kind}" if self.detail else ""
        return f"{self.product}{detail}"


@dataclass
class LineResult:
    """One written line, or a skip. Angle is unused; the writer returns one line."""

    line: str = ""
    source: str = ""
    facts: str = ""
    grader_pass: bool = False
    grader_reason: str = ""
    calls: list[str] = field(default_factory=list)


def shopper_name(title: str) -> str:
    """A name a person would say, without the pack size or the SKU tail."""

    raw = re.sub(r"[®™]", "", str(title or ""))
    raw = raw.split(". ")[0]
    raw = _PACK.sub(" ", raw)
    halves = re.split(r"\s+[-–—]\s+", raw)
    if len(halves) >= 2:
        named = [half for half in halves if _NOUN.search(half)]
        raw = named[-1] if named else halves[-1]
    raw = re.sub(r"\bby\b.+$", "", raw, flags=re.I)
    model = re.search(r"\b([A-Z]\d+[A-Za-z0-9-]*\s+[A-Z][\w'-]+)\b", raw)
    if model:
        return model.group(1)
    words = raw.split()
    if len(words) >= 6:
        tail = re.search(r"((?:[A-Z][\w'-]*-){1,}[\w'-]+(?:\s+[A-Z][\w'-]+){0,3})$", raw)
        if tail:
            raw = tail.group(1)
    raw = _tighten_name(raw)
    if raw and raw[0].islower():
        raw = raw[0].upper() + raw[1:]
    return " ".join(raw.split())


def _tighten_name(raw: str) -> str:
    """Drop filler from a long title and keep the name a person would say."""

    words = raw.split()
    loose = {"handmade", "adjustable", "professional", "robot", "live", "edge"}
    kept = []
    for word in words:
        if word.lower() in loose:
            continue
        if "-" in word and len(words) > 3 and not _NOUN.search(word):
            continue
        kept.append(word)
    if len(kept) >= 2 and _NOUN.search(kept[-1]):
        return " ".join(kept)
    return " ".join(words)


def _clean(text: str) -> str:
    raw = _FLUFF.sub(" ", str(text or ""))
    return " ".join(raw.split())


def _find_phrase(text: str, phrases: tuple[str, ...]) -> str:
    low = f" {_clean(text).lower()} "
    found = [phrase for phrase in phrases if f" {phrase} " in low]
    if not found:
        return ""
    return max(found, key=len)


def _keyword_blob(text: str) -> bool:
    """A comma list with no sentence, not a normal product description."""

    raw = str(text or "").strip()
    if not raw or "." in raw or "!" in raw:
        return False
    return raw.count(",") >= 6


def _word_forms(word: str) -> set[str]:
    forms = {word}
    if len(word) <= 2:
        return forms
    if word.endswith("ies") and len(word) > 4:
        forms.add(word[:-3] + "y")
    elif word.endswith("es") and len(word) > 3:
        forms.add(word[:-2])
    elif word.endswith("s") and len(word) > 3:
        forms.add(word[:-1])
    else:
        forms.add(word + "s")
        if word.endswith("y") and len(word) > 3:
            forms.add(word[:-1] + "ies")
    return forms


def _known_words(*texts: str) -> set[str]:
    known: set[str] = set()
    for text in texts:
        for token in re.findall(r"[a-z0-9]+", (text or "").lower()):
            known |= _word_forms(token)
    return known


def _novel_words(text: str, product_words: set[str]) -> list[str]:
    found = []
    for word in re.findall(r"[a-z0-9']+", (text or "").lower()):
        word = word.strip("'")
        if len(word) < 3 or word in _FILLER_WORD or word in _GENERIC_DETAIL or word in product_words:
            continue
        found.append(word)
    return found


def _kind_for(text: str, product_words: set[str]) -> str:
    novel = " ".join(_novel_words(text, product_words))
    if _find_phrase(novel, _FLAVORS):
        return "flavor"
    if _find_phrase(novel, _MATERIALS):
        return "material"
    return "detail"


def _sentences(text: str) -> list[str]:
    """Clauses from a description. Domains stay intact. Bullets become sentences."""

    raw = str(text or "")
    raw = re.sub(r"[\U0001F300-\U0001FAFF\u2600-\u27BF\uFE0F🟦▪•●✅✨]", "\n", raw)
    raw = re.sub(r"(?m)^\s*[-*]+\s*", "", raw)
    raw = re.sub(r"\([^)]*\)", " ", raw)
    raw = re.sub(r"\[[^\]]*\]", " ", raw)
    raw = re.sub(
        r"\b([A-Za-z0-9-]+)\.(?=[A-Za-z])",
        lambda match: match.group(1) + "\u0000",
        raw,
    )
    if not re.search(r"[.!?]", raw):
        raw = re.sub(r"([a-z])\s+([A-Z])", r"\1. \2", raw)
    raw = raw.replace("\n", ". ")
    found: list[str] = []
    for part in re.split(r"[.!?;]+", raw):
        clause = " ".join(part.replace("\u0000", ".").split()).strip(" -,")
        if not clause or clause.startswith("(") or clause.startswith("["):
            continue
        found.append(clause)
    return found


def _plain_clause(text: str) -> str:
    raw = re.sub(r"[\U0001F300-\U0001FAFF\u2600-\u27BF\uFE0F\u200d]", " ", str(text or ""))
    raw = re.sub(r"[🟦▪•●✅✨]", " ", raw)
    raw = re.sub(r"(?m)^\s*[-*•]+\s*", "", raw)
    return " ".join(raw.split())


def _score_clause(clause: str, product_words: set[str]) -> int:
    """Higher when the clause is an ingredient, scent, material, or use."""

    if not clause or _MARKETING_OPEN.search(clause) or _SIZE_DETAIL.search(clause):
        return 0
    novel = _novel_words(clause, product_words)
    novel_text = " ".join(novel)
    concrete = bool(
        _HOW_USED.search(clause)
        or _find_phrase(novel_text, _MATERIALS)
        or _find_phrase(novel_text, _FLAVORS)
        or re.search(r"\b(?:acetate|stainless|yacon|ghk|collagen|hyaluronic|cedarwood|vetiver)\b", novel_text, re.I)
    )
    if _BENEFIT_LIST.search(clause) and not concrete:
        return 0
    if _DIETARY.search(clause) and not concrete:
        return 1
    if len(clause.split()) <= 3 and not concrete:
        return 0
    if _SKIN_FEEL.search(clause) and not concrete:
        return 1
    if not novel and not concrete:
        return 0
    score = len(novel) or 1
    if concrete:
        score += 6
    if _SKIN_FEEL.search(clause):
        score -= 4
    return score


def _detail_from_text(text: str, product_words: set[str]) -> str:
    """One short phrase from the body. Not a size, a slogan, or two clauses joined."""

    if not text or _keyword_blob(text):
        return ""
    best = ""
    best_score = 0
    for clause in _sentences(text):
        score = _score_clause(clause, product_words)
        if score > best_score:
            best = clause
            best_score = score
    return best if best_score else ""


def detail_sentences(text: str, product_words: set[str]) -> list[str]:
    """Every usable sentence, best first."""

    if not text or _keyword_blob(text):
        return []
    ranked = [
        ( _score_clause(clause, product_words), clause)
        for clause in _sentences(text)
    ]
    ranked = [(score, clause) for score, clause in ranked if score]
    ranked.sort(key=lambda item: item[0], reverse=True)
    return [clause for _score, clause in ranked]


def extract_detail(
    title: str,
    body: str = "",
    _options: Optional[list[dict[str, Any]]] = None,
    brand: str = "",
) -> tuple[str, str, str]:
    """Return (product, detail, kind) or three empty strings.

    The detail comes from the body only. Option values such as ``Default Title``
    and sizes are not details. A word already in the shopper name is not enough.
    """

    product = shopper_name(title)
    if not product or not re.search(r"[A-Za-z]", product):
        return "", "", ""
    known = _known_words(product, brand)
    detail = _detail_from_text(body or "", known)
    if not detail:
        return product, "", ""
    return product, detail, _kind_for(detail, known)


def _tokens(text: str) -> set[str]:
    return {
        token for token in re.findall(r"[a-z0-9]+", (text or "").lower())
        if len(token) > 2 and token not in {"and", "the", "llc", "inc", "usa", "shop", "store", "co"}
    }


def _initials(text: str) -> str:
    words = [word for word in re.findall(r"[A-Za-z]+", text or "") if word.lower() not in {"and", "the", "of"}]
    return "".join(word[0] for word in words).lower()


def _brand_match(vendor: str, brand: str) -> bool:
    vendor_tokens = _tokens(vendor)
    brand_tokens = _tokens(brand)
    if brand_tokens & vendor_tokens:
        return True
    if brand.lower() in vendor.lower() or vendor.lower() in brand.lower():
        return True
    initials = _initials(vendor)
    if initials and initials == re.sub(r"[^a-z0-9]", "", brand.lower()):
        return True
    return False


def _selling_vendor(vendor: str) -> str:
    """A brand name. A category stuffed into the vendor field is ignored."""

    text = str(vendor or "").strip()
    if text.endswith(":"):
        text = text[:-1].strip()
    if (
        not text
        or _GENERIC_VENDOR.match(text)
        or _VENDOR_CATEGORY.search(text)
        or _ADDON_VENDOR.search(text)
        or re.fullmatch(r"[A-Z0-9][A-Z0-9-]{1,7}", text)
    ):
        return ""
    return text


def _row_is_custom(item: dict[str, Any]) -> bool:
    blob = f"{item.get('title') or ''} {item.get('vendor') or ''} {item.get('product_type') or ''}"
    return bool(re.search(r"\b(?:personaliz\w*|monogram|logomats?|your logo)\b", blob, re.I))


def custom_catalog(products: list[dict[str, Any]], brand: str = "") -> bool:
    """True when at least half the rows are the customer's own goods.

    One personalized chess set does not make the shop a custom shop.
    ``brand`` is unused; the share of the rows decides.
    """

    rows = [item for item in products if isinstance(item, dict) and (item.get("title") or item.get("vendor"))]
    if not rows:
        return False
    return sum(1 for item in rows if _row_is_custom(item)) / len(rows) >= 0.5


def reseller_catalog(products: list[dict[str, Any]], brand: str) -> bool:
    """True when most real vendor names are a different company than the store."""

    vendors = [_selling_vendor(str(item.get("vendor") or "")) for item in products]
    vendors = [vendor for vendor in vendors if vendor]
    if len(vendors) < 2 or not _tokens(brand):
        return False
    matches = sum(1 for vendor in vendors if _brand_match(vendor, brand))
    return matches / len(vendors) < 0.5


def line_fact_candidates(
    products: list[dict[str, Any]],
    *,
    brand: str,
    recipe: str = "",
    vendors: Optional[list[str]] = None,
) -> list[LineFacts]:
    """Detail sentences in best-seller order. A near-duplicate retry can use the next one."""

    catalog = [item for item in products if isinstance(item, dict) and item.get("title")]
    vendor_rows = [{"vendor": vendor} for vendor in (vendors or [])] or catalog
    if custom_catalog(catalog, brand) or custom_catalog(vendor_rows, brand):
        return [LineFacts(brand=brand, recipe=recipe, skip="custom")]
    if reseller_catalog(vendor_rows, brand):
        return [LineFacts(brand=brand, recipe=recipe, skip="reseller")]
    from outbound_product_facts import product_drug_claim, product_pain_claim, spare_part

    found: list[LineFacts] = []
    for item in catalog:
        if product_pain_claim(item) or spare_part(item):
            continue
        if product_drug_claim(item):
            return [LineFacts(brand=brand, recipe=recipe, skip="blocked_category")]
        product = shopper_name(str(item.get("title") or ""))
        sentences = detail_sentences(str(item.get("body") or ""), _known_words(product, brand))
        for detail in sentences[:2]:
            facts = LineFacts(
                brand=brand, product=product, detail=detail,
                detail_kind=_kind_for(detail, _known_words(product, brand)), recipe=recipe,
            )
            facts.summary = facts.as_text()
            found.append(facts)
            if len(found) >= 4:
                return found
    return found or [LineFacts(brand=brand, recipe=recipe, skip="no_product_detail")]


def pick_line_facts(
    products: list[dict[str, Any]],
    *,
    brand: str,
    recipe: str = "",
    vendors: Optional[list[str]] = None,
) -> LineFacts:
    """The first real product that has one concrete detail, or a skip."""

    return line_fact_candidates(products, brand=brand, recipe=recipe, vendors=vendors)[0]


def _name_parts(product: str, brand: str = "") -> tuple[list[str], list[str]]:
    """Main nouns, then the other words that distinguish this product."""

    brand_words = set(re.findall(r"[a-z0-9]+", (brand or "").lower()))
    nouns: list[str] = []
    distinctive: list[str] = []
    for word in re.findall(r"[a-z0-9]+", (product or "").lower()):
        if word in _STOP or word in brand_words or word.isdigit() or len(word) < 3:
            continue
        if _NOUN.fullmatch(word) or word in {"serum", "kit", "cola", "soap", "bar", "chips", "pillow", "vacuum"}:
            nouns.append(word)
        else:
            distinctive.append(word)
    return nouns, distinctive


def main_nouns(product: str, brand: str = "") -> list[str]:
    """The product words a line has to include. Not the pack size or the brand."""

    nouns, distinctive = _name_parts(product, brand)
    return (nouns + distinctive)[:4]


def _grounded(line: str, product: str, brand: str = "") -> bool:
    """The main noun plus one distinguishing word. Not every word in the title."""

    nouns, distinctive = _name_parts(product, brand)
    low = line.lower()
    if nouns and distinctive:
        has_noun = any(word in low for word in nouns)
        has_extra = any(word in low for word in distinctive) or sum(word in low for word in nouns) >= 2
        return has_noun and has_extra
    needed = nouns or distinctive
    if len(needed) >= 2:
        return sum(1 for word in needed if word in low) >= 2
    if needed:
        return any(word in low for word in needed)
    return True


def hard_reason(line: str, facts: LineFacts) -> str:
    """A code failure, or empty when the line may be graded."""

    raw = " ".join((line or "").split())
    if not raw:
        return "empty"
    if re.match(r"^[a-z]", raw):
        return "lowercase opening"
    if len(raw.split()) > 22:
        return "too long"
    if re.search(r"\bblends?\b", raw, re.I) and not re.search(r"\bblend", facts.detail or "", re.I):
        return "unsupported verb"
    cause = bool(re.search(r"\b(?:donat\w*|gives?|giving)\b|\d+\s*%", facts.detail or "", re.I))
    brand_words = [word for word in re.findall(r"[a-z0-9]+", (facts.brand or "").lower()) if len(word) >= 4]
    names_brand = any(word in raw.lower() for word in brand_words)
    if not _grounded(raw, facts.product, facts.brand) and not (cause and names_brand):
        return "grounding"
    if _cause_miscast(raw, facts):
        return "cause actor"
    if _INVENTED.search(raw):
        return "invented action"
    hype_text = raw
    for word in re.findall(r"[A-Za-z0-9]+", f"{facts.brand} {facts.product}"):
        if len(word) >= 3:
            hype_text = re.sub(rf"\b{re.escape(word)}\b", " ", hype_text, flags=re.I)
    if _HYPE.search(hype_text):
        return "hype"
    food = re.search(r"\b(?:cola|soda|chips|cookie|tea|gum|candy|chocolate)\b", facts.product, re.I)
    if (
        re.search(r"\bflavou?r\b", raw, re.I)
        and not food
        and (_NONFOOD.search(f"{facts.product} {raw}") or re.search(r"\bbar\b", facts.product, re.I))
    ):
        return "flavor"
    from outbound_openers import BACKEND_LANGUAGE

    masked = raw
    for word in re.findall(r"[A-Za-z0-9]+", facts.product or ""):
        if len(word) >= 3:
            masked = re.sub(rf"\b{re.escape(word)}\b", " ", masked, flags=re.I)
    if BACKEND_LANGUAGE.search(masked) or re.search(
        r"\b(?:shopify|klaviyo|woocommerce|recharge|plan upgrade)\b"
        r"|\b(?:january|february|march|april|june|july|august|september|october|november|december)\b",
        masked, re.I,
    ):
        return "backend"
    if _HEALTH.search(raw):
        return "health claim"
    if _URL.search(raw):
        return "url"
    recipe = (facts.recipe or "").strip().lower().replace("-", " ").replace("_", " ")
    if _AMAZON.search(raw):
        if recipe == "amz on":
            return ""
        if recipe == "amz off":
            if _ALREADY_AMAZON.search(raw):
                return "amazon presence"
            if re.search(r"\blaunch", raw, re.I):
                return ""
            return "amazon presence"
        return "amazon presence"
    return ""


def _cause_miscast(line: str, facts: LineFacts) -> bool:
    """A donation is something the brand does, not the product."""

    if not re.search(r"\b(?:donat\w*|gives?|giving)\b|\d+\s*%", facts.detail or "", re.I):
        return False
    brand_words = [word for word in re.findall(r"[a-z0-9]+", (facts.brand or "").lower()) if len(word) >= 4]
    low = line.lower()
    if brand_words and not any(word in low for word in brand_words):
        return True
    nouns, _distinctive = _name_parts(facts.product, facts.brand)
    if not nouns:
        return False
    return bool(re.search(
        r"\b(?:" + "|".join(re.escape(word) for word in nouns) + r")\b.{0,40}\b(?:donat\w*|gives?|giving)\b",
        line,
        re.I,
    ))


def writer_prompt(facts: LineFacts, *, rejection: str = "", previous: str = "") -> str:
    """A short prompt. Style lives here. The code does not score variety."""

    recipe = (facts.recipe or "").strip().lower().replace("_", " ")
    if recipe == "amz on":
        campaign = "This brand sells on Amazon. The line may say so."
    elif recipe == "amz off":
        campaign = "This brand is not on Amazon. The line may mention launching there, and may not say they already sell there."
    else:
        campaign = "Do not mention Amazon."
    extra = ""
    if rejection:
        extra = (
            f"\nRejected line: {previous}\n"
            f"Reason: {rejection}\n"
            "Write a different sentence. Do not repeat the rejected line.\n"
        )
    return (
        f"Brand: {facts.brand}\n"
        f"Product: {facts.product}\n"
        f"Detail: {facts.detail} ({facts.detail_kind})\n"
        f"{campaign}\n"
        f"Include this short product name in the line: {facts.product}. "
        "Sound like a person who just looked at it and noticed one thing. "
        "About 20 words. Vary the opening: 'Love that...', 'Noticed...', "
        "'...looks sturdy enough for...', or 'Nice that...'. "
        "Use only facts that are in the source sentence. Do not add an adjective, "
        "a color, a feeling, or a sense that the source does not state. "
        f"If the source says the brand gives or donates, {facts.brand or 'the brand'} does it, not the product. "
        "Do not copy these words even if they are in the source: mesmerizing, exquisite, "
        "stunning, breathtaking, high quality, premium, leading-edge, cutting-edge, "
        "elevate, luxurious, game-changer, elegantly, gracefully, effortlessly. "
        "Do not claim you used or bought it.\n"
        f"Examples of the voice, each a different shape:\n{_EXAMPLES}\n"
        f"{extra}"
        "Return only the sentence."
    )


def grader_prompt(facts: LineFacts, line: str) -> str:
    return (
        "Grade this cold-email line in two checks.\n"
        "Facts: brand and product name are known. A plain paraphrase of the source "
        "is fine, such as 'splash' for a small amount. Fail a new fact, a new "
        "adjective, or a verb the source does not support. "
        "A sensory line such as 'leaves skin feeling fresh' is not a health claim. "
        "A medical or benefit claim, such as calming redness or easing pain, still fails. "
        "'Gentle' passes only when the source says it. A new adjective fails.\n"
        "Voice: fail hype, filler, medical or benefit claims, ad-copy, "
        "instruction tone, and a flat 'Product does X' catalog statement. "
        "Do not fail a sensory description the source already states.\n"
        f"Brand: {facts.brand}\n"
        f"Product: {facts.product}\n"
        f"Source: {facts.detail}\n"
        f"Line: {line}\n"
        "Pass:\n"
        "Love that Perfy does flavors like blood orange yuzu and turmeric.\n"
        "The olive wood mugs from ArtisRaw look sturdy enough for daily use.\n"
        "Chocolate Chunk Cookie Mixes from Autumn Sons Baking need just water and oil.\n"
        "Fail:\n"
        "The coffee bar soap is worth a closer look.\n"
        "The Hana Ear Climber creates a mesmerizing cascade of shimmer along your ear.\n"
        "The Dreame L10s Ultra Robot Vacuum delivers up to 60 days of hands-free cleaning.\n"
        'Return JSON {"pass": false, "reason": "short reason"}.'
    )


def parse_grade(raw: str) -> tuple[bool, str]:
    text = (raw or "").strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?", "", text).strip()
        text = re.sub(r"```$", "", text).strip()
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, re.S)
        if not match:
            return False, "grader unreadable"
        try:
            payload = json.loads(match.group(0))
        except json.JSONDecodeError:
            return False, "grader unreadable"
    if not isinstance(payload, dict):
        return False, "grader unreadable"
    reason = str(payload.get("reason") or "").strip()
    return bool(payload.get("pass")), reason or ("pass" if payload.get("pass") else "fail")


def _similar(left: str, right: str) -> float:
    import difflib

    folded = lambda text: " ".join(re.sub(r"[^a-z0-9]+", " ", (text or "").lower()).split())
    a, b = folded(left), folded(right)
    if not a or not b:
        return 0.0
    return difflib.SequenceMatcher(None, a, b).ratio()


def compose_line(
    facts: LineFacts,
    *,
    writer: Callable[[str], str],
    grader: Callable[[str], str],
    alternatives: Optional[list[LineFacts]] = None,
) -> LineResult:
    """Write, grade, and rewrite once. No template line on the way out."""

    result = LineResult(facts=facts.as_text())
    if facts.skip:
        result.source = f"skipped:{facts.skip}"
        return result
    queue = [facts] + [item for item in (alternatives or []) if item is not facts]
    hard = {
        "grounding", "invented action", "hype", "flavor", "backend", "url",
        "health claim", "amazon presence", "too long", "lowercase opening",
        "unsupported verb", "cause actor", "model_timeout", "model_error",
    }

    def _attempt(candidate: LineFacts, rejection: str, previous: str) -> tuple[str, str]:
        try:
            line = writer(writer_prompt(candidate, rejection=rejection, previous=previous)) or ""
            line = re.sub(r"[®™]", "", line)
            line = " ".join(line.split())
        except Exception as exc:  # noqa: BLE001
            name = exc.__class__.__name__
            if "timeout" in name.lower() or "timeout" in str(exc).lower():
                return "", "model_timeout"
            return "", "model_error"
        result.calls.append("writer")
        why = hard_reason(line, candidate)
        if why:
            return line, why
        for attempt in range(2):
            try:
                passed, reason = parse_grade(grader(grader_prompt(candidate, line)))
            except Exception as exc:  # noqa: BLE001
                name = exc.__class__.__name__
                timed_out = "timeout" in name.lower() or "timeout" in str(exc).lower()
                result.calls.append("grader")
                if timed_out and attempt == 0:
                    continue
                return line, "model_timeout" if timed_out else "model_error"
            result.calls.append("grader")
            if passed:
                return line, ""
            return line, reason or "grader"
        return line, "model_error"

    last_why = "empty"
    for candidate in queue:
        result.facts = candidate.as_text()
        line, why = _attempt(candidate, "", "")
        if not why:
            result.line = line
            result.source = "ai"
            result.grader_pass = True
            result.grader_reason = "pass"
            return result
        if why in {"model_timeout", "model_error"}:
            last_why = why
            continue
        retry, why = _attempt(candidate, why, line)
        if retry and _similar(retry, line) > 0.85 and why not in hard:
            last_why = "identical"
            continue
        if not why:
            result.line = retry
            result.source = "ai"
            result.grader_pass = True
            result.grader_reason = "pass"
            return result
        last_why = why
        result.line = ""
        result.grader_pass = False
        result.grader_reason = last_why
        result.source = f"skipped:{last_why}" if last_why in hard else "skipped:grader"
        return result
    result.line = ""
    result.grader_pass = False
    result.grader_reason = last_why
    result.source = f"skipped:{last_why}" if last_why in hard else "skipped:grader"
    return result
