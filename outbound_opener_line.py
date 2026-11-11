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

_EXAMPLES = """
Love that Perfy does flavors like blood orange yuzu and turmeric.
The olive wood mugs from ArtisRaw look sturdy enough for daily use.
Biggs makes a coffee bar soap with ground coffee in the bar.
""".strip()

_FLUFF = re.compile(
    r"\b(?:best|official|premium|organic|natural|luxury|authentic|ultimate|"
    r"top|rated|selling|quality|perfect|amazing|deluxe|exclusive|new)\b",
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


def extract_detail(title: str, body: str = "", options: Optional[list[dict[str, Any]]] = None) -> tuple[str, str, str]:
    """Return (product, detail, kind) or three empty strings.

    The product is a cleaned title. The detail is one flavor, scent,
    ingredient, material, color, or format from that product's own text.
    """

    product = _clean(title)
    product = re.split(r"\s+[–—|-]\s+", product, maxsplit=1)[0].strip()
    if len(product.split()) > 8:
        product = " ".join(product.split()[:8])
    if len(product.split()) < 1 or not re.search(r"[A-Za-z]", product):
        return "", "", ""
    sources = [product]
    if body and not _keyword_blob(body):
        sources.append(_clean(body))
    for option in options or []:
        if not isinstance(option, dict):
            continue
        name = str(option.get("name") or "")
        values = [str(value) for value in (option.get("values") or []) if str(value).strip()]
        if re.search(r"colou?r|size", name, re.I) and len(values) > 3:
            continue
        if re.search(r"flavor|scent|material|scent", name, re.I) and values:
            sources.append(values[0])
    blob = " ".join(sources)
    for kind, phrases in (("flavor", _FLAVORS), ("material", _MATERIALS), ("format", _FORMATS)):
        found = _find_phrase(blob, phrases)
        if found and found.lower() != product.lower():
            return product, found, kind
        if found:
            return product, found, kind
    made = re.search(
        r"\b(?:made with|made from|with|scent of|notes of)\s+([a-z][a-z' -]{2,40})",
        blob, re.I,
    )
    if made:
        detail = " ".join(made.group(1).split()[:4])
        kind = "scent" if re.search(r"scent|notes", made.group(0), re.I) else "ingredient"
        return product, detail, kind
    return "", "", ""


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


def reseller_catalog(products: list[dict[str, Any]], brand: str) -> bool:
    """True when most product vendors are a different company than the store."""

    vendors = [str(item.get("vendor") or "").strip() for item in products if str(item.get("vendor") or "").strip()]
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
    vendor_rows = [{"vendor": vendor} for vendor in (vendors or [])]
    if reseller_catalog(vendor_rows or catalog, brand):
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


def _content_tokens(phrase: str) -> list[str]:
    tokens = []
    for token in re.findall(r"[A-Za-z0-9]+", phrase or ""):
        if token.lower() in _STOP:
            continue
        if len(token) >= 3 or token.isupper():
            tokens.append(token.lower())
    return tokens


def hard_reason(line: str, facts: LineFacts) -> str:
    """A code failure, or empty when the line may be graded."""

    raw = " ".join((line or "").split())
    if not raw:
        return "empty"
    needed = _content_tokens(facts.product)
    low = raw.lower()
    if needed and not all(token in low for token in needed):
        return "grounding"
    from outbound_openers import BACKEND_LANGUAGE

    if BACKEND_LANGUAGE.search(raw) or re.search(
        r"\b(?:shopify|klaviyo|woocommerce|recharge|plan upgrade|pixel)\b"
        r"|\b(?:january|february|march|april|june|july|august|september|october|november|december)\b",
        raw, re.I,
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
        "stays true to the facts, and is not filler or a critique.\n"
        f"Facts: {facts.as_text()}\n"
        f"Line: {line}\n"
        "Examples of the bar:\n"
        f"{_EXAMPLES}\n"
        "Filler such as 'worth a closer look' or 'a fun option to try' fails.\n"
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
        try:
            passed, reason = parse_grade(grader(grader_prompt(facts, line)))
        except Exception as exc:  # noqa: BLE001
            name = exc.__class__.__name__
            if "timeout" in name.lower() or "timeout" in str(exc).lower():
                return line, "model_timeout"
            return line, "model_error"
        result.calls.append("grader")
        if passed:
            return line, ""
        return line, reason or "grader"

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
    result.source = "skipped:model_timeout" if why == "model_timeout" else f"skipped:{why if why in {'grounding', 'reseller', 'health claim', 'amazon presence', 'backend', 'url'} else 'grader'}"
    return result
