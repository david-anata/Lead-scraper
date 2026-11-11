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
    r"\bi (?:just )?(?:bought|tried|ordered|added)\b"
    r"|\badded\b.*\bto my cart\b"
    r"|\bmy cart\b"
    r"|\b(?:bought|ordered|tried)\b.{0,80}\bon amazon\b"
    r"|\bon amazon\b.{0,40}\b(?:bought|ordered|cart)\b",
    re.I,
)
_HYPE = re.compile(r"\b(?:perfect|amazing|obsessed)\b", re.I)
_NONFOOD = re.compile(r"\b(?:soap|candle|balm|wax|lotion)\b", re.I)
_VENDOR_CATEGORY = re.compile(
    r"\b(?:tinted|polarized|eyeglasses|sunglasses|frames|optical|readers)\b"
    r"|^(?:women|men|kids|unisex|youth)\b",
    re.I,
)
_GENERIC_VENDOR = re.compile(r"^(?:vinyl|default|upload|builder|title)$", re.I)
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
    return " ".join(raw.split())


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
    raw = str(text or "").strip()
    if not raw:
        return False
    if raw.count(",") >= 4 and "." not in raw:
        return True
    words = raw.split()
    return len(words) >= 14 and raw.count(",") >= 3


def _product_words(product: str) -> set[str]:
    return set(re.findall(r"[a-z0-9]+", product.lower()))


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
    if not novel:
        return 0
    score = len(novel)
    if _HOW_USED.search(clause):
        score += 6
    if _find_phrase(clause, _MATERIALS) or _find_phrase(clause, _FLAVORS):
        score += 4
    return score


def _detail_from_text(text: str, product_words: set[str]) -> str:
    """One short phrase from the body. Not a size, a slogan, or two clauses joined."""

    text = _plain_clause(text)
    if not text or _keyword_blob(text):
        return ""
    best = ""
    best_score = 0
    for part in re.split(r"[.!?;]", text):
        clause = " ".join(part.strip(" -,").split()[:10])
        score = _score_clause(clause, product_words)
        if score > best_score:
            best = clause
            best_score = score
    return best if best_score else ""


def extract_detail(title: str, body: str = "", _options: Optional[list[dict[str, Any]]] = None) -> tuple[str, str, str]:
    """Return (product, detail, kind) or three empty strings.

    The detail comes from the body only. Option values such as ``Default Title``
    and sizes are not details. A word already in the shopper name is not enough.
    """

    product = shopper_name(title)
    if not product or not re.search(r"[A-Za-z]", product):
        return "", "", ""
    detail = _detail_from_text(body or "", _product_words(product))
    if not detail:
        return product, "", ""
    return product, detail, _kind_for(detail, _product_words(product))


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
    if not text or _GENERIC_VENDOR.match(text) or _VENDOR_CATEGORY.search(text):
        return ""
    return text


def custom_catalog(products: list[dict[str, Any]], brand: str) -> bool:
    """True for a shop that sells the customer's own logo or personalized goods."""

    if re.search(r"\b(?:personaliz\w*|custom)\b", brand or "", re.I):
        return True
    blob = " ".join(
        f"{item.get('title') or ''} {item.get('vendor') or ''} {item.get('product_type') or ''}"
        for item in products if isinstance(item, dict)
    )
    return bool(re.search(r"\b(?:personaliz\w*|monogram|logomats?|your logo)\b", blob, re.I))


def reseller_catalog(products: list[dict[str, Any]], brand: str) -> bool:
    """True when most real vendor names are a different company than the store."""

    vendors = [_selling_vendor(str(item.get("vendor") or "")) for item in products]
    vendors = [vendor for vendor in vendors if vendor]
    if len(vendors) < 2 or not _tokens(brand):
        return False
    matches = sum(1 for vendor in vendors if _brand_match(vendor, brand))
    return matches / len(vendors) < 0.5


def pick_line_facts(
    products: list[dict[str, Any]],
    *,
    brand: str,
    recipe: str = "",
    vendors: Optional[list[str]] = None,
) -> LineFacts:
    """The first real product that has one concrete detail, or a skip."""

    catalog = [item for item in products if isinstance(item, dict) and item.get("title")]
    vendor_rows = [{"vendor": vendor} for vendor in (vendors or [])] or catalog
    if custom_catalog(catalog, brand) or custom_catalog(vendor_rows, brand):
        return LineFacts(brand=brand, recipe=recipe, skip="custom")
    if reseller_catalog(vendor_rows, brand):
        return LineFacts(brand=brand, recipe=recipe, skip="reseller")
    for item in catalog:
        product, detail, kind = extract_detail(
            str(item.get("title") or ""),
            str(item.get("body") or ""),
            item.get("options") if isinstance(item.get("options"), list) else [],
        )
        if product and detail:
            facts = LineFacts(
                brand=brand, product=product, detail=detail, detail_kind=kind, recipe=recipe,
            )
            facts.summary = facts.as_text()
            return facts
    return LineFacts(brand=brand, recipe=recipe, skip="no_product_detail")


def main_nouns(product: str, brand: str = "") -> list[str]:
    """The product words a line has to include. Not the pack size or the brand."""

    brand_words = set(re.findall(r"[a-z0-9]+", (brand or "").lower()))
    words = [word for word in re.findall(r"[a-z0-9]+", (product or "").lower()) if word not in _STOP]
    nouns: list[str] = []
    for index, word in enumerate(words):
        if word in brand_words or word.isdigit():
            continue
        if _NOUN.fullmatch(word) or word in {"serum", "kit", "cola", "soap", "bar", "chips", "pillow", "bracelet"}:
            if index and words[index - 1] not in brand_words and words[index - 1] not in _STOP:
                nouns.append(words[index - 1])
            nouns.append(word)
    if not nouns:
        nouns = [word for word in words if len(word) >= 5 and word not in brand_words][:3]
    seen: list[str] = []
    for noun in nouns:
        if noun not in seen:
            seen.append(noun)
    return seen[:4]


def hard_reason(line: str, facts: LineFacts) -> str:
    """A code failure, or empty when the line may be graded."""

    raw = " ".join((line or "").split())
    if not raw:
        return "empty"
    needed = main_nouns(facts.product, facts.brand)
    low = raw.lower()
    if needed and not all(token in low for token in needed):
        return "grounding"
    if _INVENTED.search(raw):
        return "invented action"
    if _HYPE.search(raw):
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


def writer_prompt(facts: LineFacts, *, rejection: str = "") -> str:
    """A short prompt. Style lives here. The code does not score variety."""

    recipe = (facts.recipe or "").strip().lower().replace("_", " ")
    if recipe == "amz on":
        campaign = "This brand sells on Amazon. The line may say so."
    elif recipe == "amz off":
        campaign = "This brand is not on Amazon. The line may mention launching there, and may not say they already sell there."
    else:
        campaign = "Do not mention Amazon."
    extra = f"\nThe previous line was rejected ({rejection}). Write a different sentence.\n" if rejection else ""
    return (
        f"Brand: {facts.brand}\n"
        f"Product: {facts.product}\n"
        f"Detail: {facts.detail} ({facts.detail_kind})\n"
        f"{campaign}\n"
        "Write one sentence a person would send after looking at this product. "
        "Use the product name and the detail. Plain and specific. "
        "Do not guess what shoppers will do, do not critique the store, and do not invent a benefit.\n"
        f"Examples of the voice, each a different shape:\n{_EXAMPLES}\n"
        f"{extra}"
        "Return only the sentence."
    )


def grader_prompt(facts: LineFacts, line: str) -> str:
    return (
        "Grade this cold-email line. Pass only when it is specific, sounds human, "
        "and uses a detail that is not already the product name.\n"
        f"Facts: {facts.as_text()}\n"
        f"Line: {line}\n"
        "Pass:\n"
        "Love that Perfy does flavors like blood orange yuzu and turmeric.\n"
        "The olive wood mugs from ArtisRaw look sturdy enough for daily use.\n"
        "Fail:\n"
        "The coffee bar soap is worth a closer look.\n"
        "I like the cherry flavor in Perfy Cherry Cola (12 Cans).\n"
        "I just bought these chips on Amazon.\n"
        "Fail invented actions, hype, a detail that only restates the name, "
        "a raw catalog title, and empty filler.\n"
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


def compose_line(
    facts: LineFacts,
    *,
    writer: Callable[[str], str],
    grader: Callable[[str], str],
) -> LineResult:
    """Write, grade, and rewrite once. No template line on the way out."""

    result = LineResult(facts=facts.as_text())
    if facts.skip:
        result.source = f"skipped:{facts.skip}"
        return result

    def _one(rejection: str) -> tuple[str, str]:
        try:
            line = " ".join((writer(writer_prompt(facts, rejection=rejection)) or "").split())
        except Exception as exc:  # noqa: BLE001
            name = exc.__class__.__name__
            if "timeout" in name.lower() or "timeout" in str(exc).lower():
                return "", "model_timeout"
            return "", "model_error"
        result.calls.append("writer")
        why = hard_reason(line, facts)
        if why:
            return line, why
        why = "model_error"
        for attempt in range(2):
            try:
                passed, reason = parse_grade(grader(grader_prompt(facts, line)))
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
            why = reason or "grader"
            break
        return line, why

    line, why = _one("")
    if not why:
        result.line = line
        result.source = "ai"
        result.grader_pass = True
        result.grader_reason = "pass"
        return result
    if why in {"model_timeout", "model_error"}:
        result.source = f"skipped:{why}"
        result.grader_reason = why
        return result
    line, why = _one(why)
    if not why:
        result.line = line
        result.source = "ai"
        result.grader_pass = True
        result.grader_reason = "pass"
        return result
    result.line = ""
    result.grader_pass = False
    result.grader_reason = why
    kept = {
        "grounding", "reseller", "custom", "health claim", "amazon presence",
        "backend", "url", "invented action", "hype", "flavor",
    }
    result.source = "skipped:model_timeout" if why == "model_timeout" else (
        f"skipped:{why}" if why in kept else "skipped:grader"
    )
    return result
