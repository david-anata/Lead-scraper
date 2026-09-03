"""Deterministic product-type resolution and competitor relevance checks.

The sales deck may use AI to explain evidence, but the evidence set itself is
qualified with inspectable rules.  This module intentionally has no network or
LLM dependencies so every inclusion/exclusion is repeatable and auditable.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from typing import Iterable

from sales_support_agent.services.helium10 import XrayProduct


_STOP_WORDS = {
    "and", "the", "for", "with", "from", "your", "company", "pack", "count",
    "ounce", "ounces", "fluid", "made", "more", "pure", "premium", "professional",
    "original", "new", "best", "all", "one", "two", "three", "four", "white",
    "black", "blue", "green", "red", "gray", "grey", "clear", "small", "large",
}

_TYPE_RULES: tuple[tuple[str, frozenset[str], frozenset[str]], ...] = (
    (
        "furniture_paint",
        frozenset({"paint", "furniture", "cabinet", "cabinets", "wood", "metal", "decor", "finish"}),
        frozenset({"paint", "furniture", "cabinet", "cabinets"}),
    ),
    (
        "athletic_chalk",
        frozenset({"gym", "climbing", "weightlifting", "magnesium", "carbonate", "chalkball", "crossfit"}),
        frozenset({"gym", "climbing", "weightlifting", "magnesium", "chalkball"}),
    ),
    (
        "classroom_chalk",
        frozenset({"chalkboard", "blackboard", "classroom", "teacher", "teachers", "sidewalk", "dustless", "crayola", "sticks"}),
        frozenset({"chalkboard", "classroom", "sidewalk", "sticks"}),
    ),
    (
        "tailors_chalk",
        frozenset({"tailor", "tailors", "sewing", "fabric", "garment", "triangle"}),
        frozenset({"tailor", "tailors", "sewing", "fabric"}),
    ),
)

_INCOMPATIBLE_TYPES = {
    "furniture_paint": {"athletic_chalk", "classroom_chalk", "tailors_chalk"},
    "athletic_chalk": {"furniture_paint", "classroom_chalk", "tailors_chalk"},
    "classroom_chalk": {"furniture_paint", "athletic_chalk", "tailors_chalk"},
    "tailors_chalk": {"furniture_paint", "athletic_chalk", "classroom_chalk"},
}


def _tokens(*values: str) -> set[str]:
    result: set[str] = set()
    for value in values:
        normalized = re.sub(r"[^a-z0-9]+", " ", str(value or "").casefold())
        result.update(
            token for token in normalized.split()
            if len(token) > 2 and token not in _STOP_WORDS and not token.isdigit()
        )
    return result


def _normalize_identity(value: str) -> str:
    return "".join(character for character in str(value or "").casefold() if character.isalnum())


def resolve_product_type(*, title: str, category: str = "", operator_label: str = "") -> str:
    """Return a stable product type from explicit, structured product evidence."""
    tokens = _tokens(operator_label, category, title)
    best_type = ""
    best_score = 0
    for product_type, signals, required_any in _TYPE_RULES:
        if not (tokens & required_any):
            continue
        score = len(tokens & signals)
        if score > best_score:
            best_type = product_type
            best_score = score
    if best_type:
        return best_type

    # A generic but deterministic fallback is safer than manufacturing a
    # taxonomy. Keep up to four meaningful terms for overlap scoring.
    generic = sorted(tokens)[:4]
    return "generic:" + ":".join(generic) if generic else "unknown"


def build_discovery_query(*, title: str, brand: str, category: str, operator_label: str) -> str:
    """Build a product-type query; never use a brand-heavy title prefix."""
    operator_tokens = _tokens(operator_label)
    category_tokens = _tokens(category)
    title_tokens = _tokens(title) - _tokens(brand)
    ordered: list[str] = []
    for source in (operator_tokens, category_tokens, title_tokens):
        for token in sorted(source):
            if token not in ordered:
                ordered.append(token)

    product_type = resolve_product_type(
        title=title,
        category=category,
        operator_label=operator_label,
    )
    preferred: dict[str, tuple[str, ...]] = {
        "furniture_paint": ("furniture", "paint", "cabinets", "wood"),
        "athletic_chalk": ("gym", "chalk", "climbing", "weightlifting"),
        "classroom_chalk": ("classroom", "chalk", "chalkboard"),
        "tailors_chalk": ("tailors", "chalk", "sewing", "fabric"),
    }
    selected = list(preferred.get(product_type, ()))
    selected.extend(token for token in ordered if token not in selected)
    return " ".join(selected[:6]).strip()


@dataclass(frozen=True)
class CompetitorDecision:
    asin: str
    title: str
    brand: str
    category: str
    discovery_source: str
    product_type: str
    relevance_score: float
    status: str
    reason_codes: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class EvidenceAssessment:
    status: str
    qualified_count: int
    candidate_count: int
    median_relevance: float
    reason: str

    @property
    def market_evidence_sufficient(self) -> bool:
        return self.status in {"ready", "directional"}


def qualify_competitors(
    *,
    target: XrayProduct,
    candidates: Iterable[XrayProduct],
    operator_category_label: str = "",
    discovery_sources: dict[str, str] | None = None,
    limit: int = 12,
) -> tuple[list[XrayProduct], list[CompetitorDecision], EvidenceAssessment]:
    """Score candidates and return only evidence-safe competitors.

    Hard exclusions protect against the reviewed failure where an ambiguous
    word ("chalk") mixed furniture paint with athletic/classroom products.
    """
    sources = discovery_sources or {}
    target_type = resolve_product_type(
        title=target.title,
        category=target.category,
        operator_label=operator_category_label,
    )
    target_tokens = _tokens(operator_category_label, target.category, target.title) - _tokens(target.brand)
    target_asin = str(target.asin or "").upper()
    target_brand = _normalize_identity(target.brand)
    qualified: list[XrayProduct] = []
    decisions: list[CompetitorDecision] = []
    seen_asins: set[str] = set()

    for candidate in candidates:
        asin = str(candidate.asin or "").upper()
        candidate_type = resolve_product_type(title=candidate.title, category=candidate.category)
        candidate_tokens = _tokens(candidate.category, candidate.title) - _tokens(candidate.brand)
        reasons: list[str] = []
        hard_excluded = False

        if not asin or not candidate.title.strip():
            reasons.append("missing_identity")
            hard_excluded = True
        elif asin == target_asin:
            reasons.append("subject_product")
            hard_excluded = True
        elif asin in seen_asins:
            reasons.append("duplicate_asin")
            hard_excluded = True
        elif target_brand and _normalize_identity(candidate.brand) == target_brand:
            reasons.append("subject_brand")
            hard_excluded = True
        if candidate_type in _INCOMPATIBLE_TYPES.get(target_type, set()):
            reasons.append(f"conflicting_product_type:{candidate_type}")
            hard_excluded = True

        intersection = target_tokens & candidate_tokens
        union = target_tokens | candidate_tokens
        overlap = len(intersection) / len(union) if union else 0.0
        if candidate_type == target_type and target_type != "unknown":
            type_score = 0.55
        elif target_type.startswith("generic:") and candidate_type.startswith("generic:"):
            # The fallback taxonomy must still work for categories that do not
            # have a named rule. Require meaningful title/category overlap;
            # never treat the word "generic" itself as evidence.
            type_score = min(0.45, overlap * 1.2)
        else:
            type_score = 0.0
        category_score = min(0.25, len(_tokens(target.category, operator_category_label) & candidate_tokens) * 0.08)
        title_score = min(0.20, overlap * 0.8)
        identity_score = 0.05 if candidate.brand.strip() and asin else 0.0
        score = round(min(1.0, type_score + category_score + title_score + identity_score), 3)

        if hard_excluded:
            status = "excluded"
        elif score >= 0.60:
            status = "qualified"
            reasons.append("product_type_and_intent_match")
        elif score >= 0.45:
            status = "review"
            reasons.append("partial_relevance")
        else:
            status = "excluded"
            reasons.append("insufficient_relevance")

        seen_asins.add(asin)
        decisions.append(
            CompetitorDecision(
                asin=asin,
                title=candidate.title,
                brand=candidate.brand,
                category=candidate.category,
                discovery_source=sources.get(asin, "unknown"),
                product_type=candidate_type,
                relevance_score=score,
                status=status,
                reason_codes=tuple(reasons),
            )
        )
        if status == "qualified" and len(qualified) < max(0, limit):
            qualified.append(candidate)

    scores = sorted(
        decision.relevance_score for decision in decisions if decision.status == "qualified"
    )
    if scores:
        midpoint = len(scores) // 2
        median = scores[midpoint] if len(scores) % 2 else (scores[midpoint - 1] + scores[midpoint]) / 2
    else:
        median = 0.0
    if len(qualified) >= 5 and median >= 0.70:
        status = "ready"
        reason = ""
    elif len(qualified) >= 3:
        status = "directional"
        reason = f"Limited comparison: {len(qualified)} qualified listings."
    else:
        status = "blocked"
        reason = (
            f"Market comparison needs review: {len(qualified)} of {len(decisions)} "
            "candidates match the target product type; at least 3 are required."
        )
    return qualified, decisions, EvidenceAssessment(
        status=status,
        qualified_count=len(qualified),
        candidate_count=len(decisions),
        median_relevance=round(median, 3),
        reason=reason,
    )
