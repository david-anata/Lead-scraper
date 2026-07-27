"""Evidence-labeled AEO diagnostics for Website Ops.

This module deliberately keeps simulated coverage prompts separate from observed
search and customer evidence. It does not call a model or infer search volume.
"""

from __future__ import annotations

import re
from typing import Any, Mapping, Sequence
from urllib.parse import urlparse


COMMERCIAL_FACETS = (
    ("fit", "Who is {topic} for?"),
    ("process", "How does {topic} work?"),
    ("comparison", "How should a buyer compare {topic} options?"),
    ("risk", "What should a buyer verify before choosing {topic}?"),
    ("proof", "What evidence supports a {topic} provider's claims?"),
    ("next-step", "What is the next step for evaluating {topic}?"),
)

INFORMATIONAL_FACETS = (
    ("definition", "What is {topic}?"),
    ("process", "How does {topic} work?"),
    ("decision", "When should a business use {topic}?"),
    ("risk", "What mistakes should a business avoid with {topic}?"),
)

TOOL_FACETS = (
    ("purpose", "What does the {topic} help calculate?"),
    ("inputs", "What inputs does the {topic} require?"),
    ("process", "How do you use the {topic}?"),
    ("interpretation", "How should a business interpret the {topic} results?"),
)

NAVIGATIONAL_FACETS = (
    ("brand", "What is {topic}?"),
    ("next-step", "How do I find {topic}?"),
)


def _normalize(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _slug_phrase(value: str) -> str:
    phrase = value.replace("-", " ").replace("_", " ").strip().title()
    for source, replacement in (
        ("Ppc", "PPC"),
        ("Fba", "FBA"),
        ("Asin", "ASIN"),
        ("Seo", "SEO"),
        ("Ga4", "GA4"),
        ("3Pl", "3PL"),
        ("B2B", "B2B"),
    ):
        phrase = re.sub(rf"\b{source}\b", replacement, phrase)
    return phrase


def _topic(observation: Mapping[str, Any]) -> str:
    path = urlparse(_normalize(observation.get("url"))).path.rstrip("/")
    segments = [segment for segment in path.split("/") if segment]
    if segments:
        section = segments[0].lower()
        leaf = _slug_phrase(segments[-1])
        if section == "services":
            return leaf
        if section == "platform":
            parent = _slug_phrase(segments[-2]) if len(segments) > 2 else ""
            return f"{parent} {leaf}".strip()
        if section == "tools":
            return leaf
        if section == "guides":
            return "Ecommerce and Amazon guides" if len(segments) == 1 else f"{leaf} guide"
        if section == "blog":
            return "Ecommerce operations insights" if len(segments) == 1 else leaf
        if section == "case-studies":
            return "Ecommerce case studies" if len(segments) == 1 else f"{leaf} case study"
        if section == "glossary":
            return "Ecommerce glossary"
        if section == "about":
            return "Anata ecommerce operations company"
        if section == "careers":
            return "Anata careers"
        if section == "contact":
            return "Anata contact information"
        return leaf
    title = _normalize(observation.get("title"))
    if title:
        cleaned_title = re.sub(
            r"\s*[|–-]\s*Anata(?: Inc\.?)?\s*$",
            "",
            title,
            flags=re.IGNORECASE,
        ).strip()
        if cleaned_title and cleaned_title.lower() not in {"anata", "anata inc.", "anata inc"}:
            return cleaned_title
    headings = observation.get("h1") or []
    if headings:
        heading = _normalize(headings[0])
        if heading:
            return heading
    return "Anata ecommerce operations"


def _facet_templates(observation: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    path = urlparse(_normalize(observation.get("url"))).path.rstrip("/")
    section = next((segment.lower() for segment in path.split("/") if segment), "")
    if section in {"services", "platform", "solutions"}:
        return COMMERCIAL_FACETS
    if section == "tools":
        return TOOL_FACETS
    if section in {"guides", "blog", "case-studies", "glossary", "resources"}:
        return INFORMATIONAL_FACETS
    if section in {"about", "careers", "contact", "privacy", "terms"}:
        return NAVIGATIONAL_FACETS
    return COMMERCIAL_FACETS


def simulated_fanout(observation: Mapping[str, Any]) -> list[dict[str, str]]:
    topic = _topic(observation)
    return [
        {"facet": facet, "prompt": template.format(topic=topic), "source": "simulated"}
        for facet, template in _facet_templates(observation)
    ]


def _technical_blockers(observation: Mapping[str, Any]) -> list[str]:
    blockers: list[str] = []
    status = observation.get("status_code")
    if status != 200:
        blockers.append(f"Page returned HTTP {status if status is not None else 'unknown'}.")
    if bool(observation.get("noindex")):
        blockers.append("Page is marked noindex.")
    if _normalize(observation.get("response_error")):
        blockers.append("Live fetch returned an error.")
    final_url = _normalize(observation.get("final_url"))
    requested_url = _normalize(observation.get("url"))
    if final_url and requested_url and final_url.rstrip("/") != requested_url.rstrip("/"):
        blockers.append("Requested URL resolves to a different final URL.")
    return blockers


def _answer_readiness_issues(observation: Mapping[str, Any]) -> list[str]:
    issues: list[str] = []
    h1 = [_normalize(item) for item in (observation.get("h1") or []) if _normalize(item)]
    h2 = [_normalize(item) for item in (observation.get("h2") or []) if _normalize(item)]
    description = _normalize(observation.get("meta_description"))
    text_length = int(observation.get("text_length", 0) or 0)

    if len(h1) != 1:
        issues.append("Use exactly one descriptive H1.")
    if not h2:
        issues.append("Add descriptive H2 sections that expose the page's main answers.")
    if not description:
        issues.append("Add a factual meta description that states the page's offer.")
    if text_length < 600:
        issues.append("Rendered text is too limited to demonstrate fit, process, risk, and next step.")
    if not any(
        marker in heading.lower()
        for heading in h2
        for marker in ("question", "faq", "how ", "who ", "what ", "compare", "proof")
    ):
        issues.append("No H2 clearly frames a direct buyer question or answer topic.")
    return issues


def build_aeo_assessment(
    observation: Mapping[str, Any],
    *,
    gsc: Mapping[str, Any] | None = None,
    customer_questions: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    blockers = _technical_blockers(observation)
    readiness_issues = _answer_readiness_issues(observation)
    observed_queries = [
        {
            "query": _normalize(item.get("query")),
            "clicks": float(item.get("clicks", 0) or 0),
            "impressions": float(item.get("impressions", 0) or 0),
            "source": "observed",
        }
        for item in ((gsc or {}).get("top_queries") or [])
        if _normalize(item.get("query"))
    ]
    observed_questions = [
        {
            "question": _normalize(item.get("question")),
            "frequency": int(item.get("frequency", 0) or 0),
            "source": "observed",
        }
        for item in customer_questions
        if _normalize(item.get("question"))
    ]
    return {
        "technical_eligibility": "blocked" if blockers else "eligible",
        "technical_eligibility_label": "Technically accessible to answer engines",
        "technical_blockers": blockers,
        "answer_readiness": "needs-work" if readiness_issues else "ready",
        "answer_readiness_label": "Structural answer readiness",
        "answer_readiness_issues": readiness_issues,
        "observed_demand_status": "observed" if observed_queries else "not-observed",
        "observed_queries": observed_queries,
        "observed_customer_questions": observed_questions,
        "simulated_coverage_prompts": simulated_fanout(observation),
        "method_note": (
            "Observed evidence comes from connected sources. Simulated prompts are coverage "
            "hypotheses only and carry no inferred demand, volume, ranking, or citation claim."
        ),
    }
