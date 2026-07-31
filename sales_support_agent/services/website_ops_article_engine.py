"""Evidence-gated article generation for autonomous Website Ops."""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import requests

from sales_support_agent.services.website_ops_query_intelligence import citation_config


DAILY_ARTICLE_MINIMUM = 8
DAILY_ARTICLE_TARGET = 8
PILLAR_DAILY_MINIMUM = 2
SERVICE_PILLARS = (
    "Ecommerce Marketing Management",
    "Fulfillment / 3PL",
    "Shipping OS",
    "Anata Intelligence",
)

EDITORIAL_TOPIC_SEEDS: tuple[dict[str, Any], ...] = (
    {
        "cluster_id": "editorial-amazon-tacos",
        "pillar": "Anata Intelligence",
        "label": "How to calculate and use Amazon TACoS",
        "normalized_query": "how to calculate amazon tacos",
        "owner_url": "https://anatainc.com/services/amazon-advertising",
    },
    {
        "cluster_id": "editorial-amazon-ppc-structure",
        "pillar": "Ecommerce Marketing Management",
        "label": "How to structure Amazon PPC campaigns without losing control",
        "normalized_query": "how to structure amazon ppc campaigns",
        "owner_url": "https://anatainc.com/services/amazon-ppc-management",
    },
    {
        "cluster_id": "editorial-amazon-listing-audit",
        "pillar": "Ecommerce Marketing Management",
        "label": "How to audit an Amazon product listing",
        "normalized_query": "how to audit an amazon product listing",
        "owner_url": "https://anatainc.com/services/amazon-listing-optimization",
    },
    {
        "cluster_id": "editorial-fba-prep-requirements",
        "pillar": "Fulfillment / 3PL",
        "label": "Amazon FBA prep requirements and common rejection risks",
        "normalized_query": "amazon fba prep requirements",
        "owner_url": "https://anatainc.com/services/amazon-fba-prep",
    },
    {
        "cluster_id": "editorial-ecommerce-fulfillment-costs",
        "pillar": "Fulfillment / 3PL",
        "label": "How to compare ecommerce fulfillment costs",
        "normalized_query": "how to compare ecommerce fulfillment costs",
        "owner_url": "https://anatainc.com/services/ecommerce-fulfillment",
    },
    {
        "cluster_id": "editorial-tiktok-shop-fees",
        "pillar": "Ecommerce Marketing Management",
        "label": "TikTok Shop seller fees, fulfillment costs, and margin planning",
        "normalized_query": "tiktok shop seller fees and fulfillment costs",
        "owner_url": "https://anatainc.com/services/tiktok-shop-management",
    },
    {
        "cluster_id": "editorial-shopify-cac",
        "pillar": "Anata Intelligence",
        "label": "How Shopify brands should evaluate customer acquisition cost",
        "normalized_query": "how to evaluate shopify customer acquisition cost",
        "owner_url": "https://anatainc.com/services/shopify-marketing-management",
    },
    {
        "cluster_id": "editorial-inventory-placement",
        "pillar": "Fulfillment / 3PL",
        "label": "How inventory placement affects ecommerce fulfillment speed and cost",
        "normalized_query": "how inventory placement affects ecommerce fulfillment",
        "owner_url": "https://anatainc.com/services/ecommerce-fulfillment",
    },
    {
        "cluster_id": "editorial-3pl-sla-scorecard",
        "pillar": "Fulfillment / 3PL",
        "label": "How to build a useful 3PL service-level scorecard",
        "normalized_query": "how to build a 3pl service level scorecard",
        "owner_url": "https://anatainc.com/services/ecommerce-fulfillment",
    },
    {
        "cluster_id": "editorial-fulfillment-rfp",
        "pillar": "Fulfillment / 3PL",
        "label": "What to include in an ecommerce fulfillment RFP",
        "normalized_query": "what to include in an ecommerce fulfillment rfp",
        "owner_url": "https://anatainc.com/services/ecommerce-fulfillment",
    },
    {
        "cluster_id": "editorial-returns-operations",
        "pillar": "Fulfillment / 3PL",
        "label": "How to evaluate ecommerce returns operations",
        "normalized_query": "how to evaluate ecommerce returns operations",
        "owner_url": "https://anatainc.com/services/ecommerce-fulfillment",
    },
    {
        "cluster_id": "editorial-shipping-zone-cost",
        "pillar": "Shipping OS",
        "label": "How shipping zones change parcel cost and delivery speed",
        "normalized_query": "how shipping zones affect parcel cost",
        "owner_url": "https://anatainc.com/platform/shipping",
    },
    {
        "cluster_id": "editorial-carrier-mix",
        "pillar": "Shipping OS",
        "label": "How to design a resilient parcel carrier mix",
        "normalized_query": "how to design a parcel carrier mix",
        "owner_url": "https://anatainc.com/platform/shipping",
    },
    {
        "cluster_id": "editorial-dimensional-weight",
        "pillar": "Shipping OS",
        "label": "Dimensional weight: how to calculate it and reduce its impact",
        "normalized_query": "how to calculate dimensional weight",
        "owner_url": "https://anatainc.com/platform/shipping",
    },
    {
        "cluster_id": "editorial-delivery-promise",
        "pillar": "Shipping OS",
        "label": "How ecommerce teams should set an accurate delivery promise",
        "normalized_query": "how to set an ecommerce delivery promise",
        "owner_url": "https://anatainc.com/platform/shipping",
    },
    {
        "cluster_id": "editorial-rate-shopping",
        "pillar": "Shipping OS",
        "label": "When parcel rate shopping helps and when it adds risk",
        "normalized_query": "when parcel rate shopping helps",
        "owner_url": "https://anatainc.com/platform/shipping",
    },
    {
        "cluster_id": "editorial-contribution-margin",
        "pillar": "Anata Intelligence",
        "label": "How ecommerce teams should calculate contribution margin",
        "normalized_query": "how to calculate ecommerce contribution margin",
        "owner_url": "https://anatainc.com/platform/intelligence",
    },
    {
        "cluster_id": "editorial-demand-forecast",
        "pillar": "Anata Intelligence",
        "label": "How to evaluate an ecommerce demand forecast",
        "normalized_query": "how to evaluate an ecommerce demand forecast",
        "owner_url": "https://anatainc.com/platform/intelligence",
    },
    {
        "cluster_id": "editorial-channel-profitability",
        "pillar": "Anata Intelligence",
        "label": "How to compare profitability across ecommerce channels",
        "normalized_query": "how to compare ecommerce channel profitability",
        "owner_url": "https://anatainc.com/platform/intelligence",
    },
    {
        "cluster_id": "editorial-inventory-turnover",
        "pillar": "Anata Intelligence",
        "label": "How to use inventory turnover without hiding stockout risk",
        "normalized_query": "how to use ecommerce inventory turnover",
        "owner_url": "https://anatainc.com/platform/intelligence",
    },
    {
        "cluster_id": "editorial-marketplace-channel-mix",
        "pillar": "Ecommerce Marketing Management",
        "label": "How to choose the right ecommerce marketplace channel mix",
        "normalized_query": "how to choose an ecommerce marketplace channel mix",
        "owner_url": "https://anatainc.com/services/ecommerce-marketing",
    },
    {
        "cluster_id": "editorial-product-page-conversion",
        "pillar": "Ecommerce Marketing Management",
        "label": "How to diagnose an ecommerce product page conversion problem",
        "normalized_query": "how to diagnose product page conversion problems",
        "owner_url": "https://anatainc.com/services/ecommerce-marketing",
    },
    {
        "cluster_id": "editorial-retail-media-budget",
        "pillar": "Ecommerce Marketing Management",
        "label": "How to allocate a retail media budget across products",
        "normalized_query": "how to allocate a retail media budget",
        "owner_url": "https://anatainc.com/services/ecommerce-marketing",
    },
)


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _json_object(text: str) -> dict[str, Any]:
    candidate = text.strip()
    if candidate.startswith("```"):
        candidate = re.sub(r"^```(?:json)?\s*", "", candidate, flags=re.IGNORECASE)
        candidate = re.sub(r"\s*```$", "", candidate)
    start, end = candidate.find("{"), candidate.rfind("}")
    if start < 0 or end <= start:
        raise RuntimeError("The article model did not return a JSON object.")
    payload = json.loads(candidate[start : end + 1])
    if not isinstance(payload, dict):
        raise RuntimeError("The article model response was not an object.")
    return payload


def _provider_text(payload: Mapping[str, Any], provider: str) -> str:
    if provider == "anthropic":
        return "\n".join(
            _clean(item.get("text"))
            for item in payload.get("content", []) or []
            if isinstance(item, Mapping) and item.get("type") == "text"
        )
    if _clean(payload.get("output_text")):
        return _clean(payload.get("output_text"))
    return "\n".join(
        _clean(content.get("text"))
        for item in payload.get("output", []) or []
        if isinstance(item, Mapping)
        for content in item.get("content", []) or []
        if isinstance(content, Mapping) and content.get("type") == "output_text"
    )


def _request_article(*, settings: Any, prompt: str) -> dict[str, Any]:
    config = citation_config(settings)
    if not config.api_key or config.provider not in {"openai", "anthropic"}:
        raise RuntimeError("Article generation needs an OpenAI or Anthropic API key.")
    if config.provider == "anthropic":
        response = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": config.api_key,
                "anthropic-version": "2023-06-01",
                "Content-Type": "application/json",
            },
            json={
                "model": config.model,
                "max_tokens": 5000,
                "messages": [{"role": "user", "content": prompt}],
                "tools": [{"type": "web_search_20250305", "name": "web_search", "max_uses": 8}],
            },
            timeout=150,
        )
    else:
        response = requests.post(
            "https://api.openai.com/v1/responses",
            headers={
                "Authorization": f"Bearer {config.api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": config.model,
                "input": prompt,
                "tools": [{"type": "web_search"}],
                "tool_choice": "required",
                "store": False,
            },
            timeout=150,
        )
    response.raise_for_status()
    payload = response.json()
    return _json_object(_provider_text(payload, config.provider))


def _eligible_cluster(
    query_intelligence: Mapping[str, Any],
    *,
    excluded_cluster_ids: set[str] | None = None,
) -> Mapping[str, Any] | None:
    excluded_cluster_ids = excluded_cluster_ids or set()
    for cluster in query_intelligence.get("clusters", []) or []:
        label = _clean(cluster.get("label"))
        if _clean(cluster.get("cluster_id")) in excluded_cluster_ids:
            continue
        if (
            len(label) > 140
            or re.search(r"(?:^|\s)-?(?:site|inurl|intitle|filetype):", label.lower())
        ):
            continue
        citation = dict(cluster.get("citation") or {})
        owner_path = urlparse(_clean(cluster.get("owner_url"))).path
        cited_urls = [
            item
            for item in citation.get("cited_urls", []) or []
            if isinstance(item, Mapping)
            and _clean(item.get("url")).startswith("https://")
            and (urlparse(_clean(item.get("url"))).hostname or "").removeprefix("www.")
            != "anatainc.com"
        ]
        if (
            cluster.get("validation_status") == "validated"
            and cluster.get("quality_status") == "eligible"
            and cluster.get("ownership_status") == "assigned"
            and cluster.get("intent") == "informational"
            and not owner_path.startswith(("/blog", "/guides", "/glossary"))
            and float(dict(cluster.get("alignment") or {}).get("composite", 1) or 1) < 0.35
            and len(cited_urls) >= 2
        ):
            return cluster
    return None


def _eligible_editorial_seed(
    excluded_cluster_ids: set[str],
    *,
    pillar: str | None = None,
) -> Mapping[str, Any] | None:
    for seed in EDITORIAL_TOPIC_SEEDS:
        if (
            _clean(seed.get("cluster_id")) not in excluded_cluster_ids
            and (not pillar or _clean(seed.get("pillar")) == pillar)
        ):
            return {
                **seed,
                "citation": {"cited_urls": []},
                "evidence_classes": ["editorial_backlog", "service_intent_map"],
                "source_kind": "editorial_backlog",
            }
    return None


def _daily_generation_path(settings: Any) -> Path | None:
    configured_root = getattr(settings, "website_ops_root", None)
    if configured_root is None:
        return None
    return (
        Path(configured_root)
        / "content-strategy"
        / "article-generation"
        / f"{datetime.now(ZoneInfo('America/Denver')).date().isoformat()}.json"
    )


def _historical_cluster_ids(settings: Any) -> set[str]:
    """Return every topic previously claimed on the durable Website Ops volume."""

    today = _daily_generation_path(settings)
    if today is None:
        return set()
    history: set[str] = set()
    for target in today.parent.glob("*.json"):
        try:
            payload = json.loads(target.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(payload, Mapping):
            continue
        history.update(
            _clean(value)
            for value in payload.get("cluster_ids", []) or []
            if _clean(value)
        )
        legacy = _clean(payload.get("cluster_id"))
        if legacy:
            history.add(legacy)
    return history


def article_generation_progress(settings: Any) -> dict[str, Any]:
    """Return today's bounded production quota and claimed topic IDs."""

    target = _daily_generation_path(settings)
    cluster_ids: list[str] = []
    claims: list[dict[str, str]] = []
    if target and target.exists():
        try:
            payload = json.loads(target.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            payload = {}
        if isinstance(payload, Mapping):
            cluster_ids = [
                _clean(value)
                for value in payload.get("cluster_ids", []) or []
                if _clean(value)
            ]
            legacy = _clean(payload.get("cluster_id"))
            if legacy and legacy not in cluster_ids:
                cluster_ids.append(legacy)
            claims = [
                {
                    "cluster_id": _clean(item.get("cluster_id")),
                    "pillar": _clean(item.get("pillar")),
                }
                for item in payload.get("claims", []) or []
                if isinstance(item, Mapping) and _clean(item.get("cluster_id"))
            ]
    pillar_counts = {
        pillar: sum(1 for claim in claims if claim["pillar"] == pillar)
        for pillar in SERVICE_PILLARS
    }
    return {
        "daily_minimum": DAILY_ARTICLE_MINIMUM,
        "daily_target": DAILY_ARTICLE_TARGET,
        "generated_today": len(cluster_ids),
        "remaining_to_minimum": max(0, DAILY_ARTICLE_MINIMUM - len(cluster_ids)),
        "remaining_to_target": max(0, DAILY_ARTICLE_TARGET - len(cluster_ids)),
        "cluster_ids": cluster_ids,
        "claims": claims,
        "pillar_daily_minimum": PILLAR_DAILY_MINIMUM,
        "pillar_counts": pillar_counts,
        "pillar_deficits": {
            pillar: max(0, PILLAR_DAILY_MINIMUM - count)
            for pillar, count in pillar_counts.items()
        },
    }


def _claim_daily_article_slot(settings: Any, cluster_id: str, pillar: str = "") -> bool:
    """Reserve one of today's production slots without duplicating a topic."""

    target = _daily_generation_path(settings)
    if target is None:
        return True
    progress = article_generation_progress(settings)
    claimed = list(progress["cluster_ids"])
    if cluster_id in claimed or len(claimed) >= DAILY_ARTICLE_TARGET:
        return False
    target.parent.mkdir(parents=True, exist_ok=True)
    claimed.append(cluster_id)
    claims = list(progress.get("claims") or [])
    claims.append({"cluster_id": cluster_id, "pillar": pillar})
    temporary = target.with_suffix(".tmp")
    temporary.write_text(
        json.dumps(
            {
                "cluster_ids": claimed,
                "claims": claims,
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "daily_minimum": DAILY_ARTICLE_MINIMUM,
                "daily_target": DAILY_ARTICLE_TARGET,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    temporary.replace(target)
    return True


def build_article_action(
    *,
    settings: Any,
    query_intelligence: Mapping[str, Any],
    requester: Callable[..., Mapping[str, Any]] | None = None,
) -> dict[str, Any] | None:
    """Create one bounded daily article action from a validated content gap."""

    if os.getenv("WEBSITE_OPS_ARTICLE_GENERATION_ENABLED", "true").lower() not in {
        "1",
        "true",
        "yes",
        "on",
    }:
        return None
    progress = article_generation_progress(settings)
    if int(progress["remaining_to_target"]) <= 0:
        return None
    excluded_cluster_ids = _historical_cluster_ids(settings) | set(progress["cluster_ids"])
    pillar_counts = dict(progress.get("pillar_counts") or {})
    selected_pillar = min(
        SERVICE_PILLARS,
        key=lambda pillar: (int(pillar_counts.get(pillar, 0)), SERVICE_PILLARS.index(pillar)),
    )
    cluster = _eligible_cluster(
        query_intelligence,
        excluded_cluster_ids=excluded_cluster_ids,
    )
    if not cluster:
        cluster = _eligible_editorial_seed(
            excluded_cluster_ids,
            pillar=selected_pillar,
        )
    if not cluster:
        cluster = _eligible_editorial_seed(excluded_cluster_ids)
    if not cluster:
        return None
    pillar = _clean(cluster.get("pillar")) or selected_pillar
    if not _claim_daily_article_slot(
        settings,
        _clean(cluster.get("cluster_id")),
        pillar,
    ):
        return None
    citations = [
        {"title": _clean(item.get("title")), "url": _clean(item.get("url"))}
        for item in dict(cluster.get("citation") or {}).get("cited_urls", []) or []
        if isinstance(item, Mapping)
        and _clean(item.get("url")).startswith("https://")
        and (urlparse(_clean(item.get("url"))).hostname or "").removeprefix("www.")
        != "anatainc.com"
    ][:6]
    known_internal_routes = list(
        dict.fromkeys(
            [
                "/services",
                "/guides",
                "/guides/amazon-advertising",
                "/guides/ecommerce-fulfillment",
                *[
                    urlparse(_clean(item.get("owner_url"))).path or "/"
                    for item in query_intelligence.get("clusters", []) or []
                    if _clean(item.get("owner_url")).startswith("https://anatainc.com")
                ],
                *[
                    urlparse(_clean(item.get("owner_url"))).path
                    for item in EDITORIAL_TOPIC_SEEDS
                ],
            ]
        )
    )[:20]
    publication_timestamp = datetime.now(timezone.utc).isoformat()
    prompt = f"""
Create a source-backed Anata blog article for the informational query:
{_clean(cluster.get("label"))}
Service pillar: {pillar}

This is a high-utility SEO/AEO publishing task. Search and verify the web. Use only
factual claims supported by authoritative HTTPS sources. Do not invent Anata results,
clients, metrics, prices, capabilities, testimonials, or proprietary data. Do not use
em dashes. Do not mention Basic Research, "reveal the gap", or any demo URL.
Write for an operator who needs a useful answer, not for a content quota. Ban filler,
generic scene-setting, fake urgency, repetition, vague superlatives, and phrases such
as "in today's fast-paced world", "game-changer", "unlock", "delve", "ever-evolving",
"seamlessly", and "robust". Explain tradeoffs, decision criteria, failure modes, and
specific next steps. Every claim that depends on external facts must be supported by
one of the visible sources.

Previously observed sources:
{json.dumps(citations)}

Existing Anata routes allowed for internal links:
{json.dumps(known_internal_routes)}

Return only one JSON object with this exact shape:
{{
  "slug": "lowercase-hyphenated",
  "primaryIntent": "{_clean(cluster.get("normalized_query"))}",
  "evidenceId": "{_clean(cluster.get("cluster_id"))}",
  "generatedAt": "{publication_timestamp}",
  "publishedAt": "{publication_timestamp}",
  "modifiedAt": "{publication_timestamp}",
  "author": {{
    "type": "Organization",
    "name": "Anata Inc.",
    "url": "https://anatainc.com"
  }},
  "title": "15 to 65 characters",
  "description": "50 to 155 characters",
  "content": {{
    "route": "/blog/SLUG",
    "eyebrow": "Topic label",
    "h1": "same as articleTitle",
    "tldr": {{"heading": "The short answer.", "answer": ["60 to 140 word direct answer"]}},
    "sections": [{{
      "heading": "Heading",
      "paragraphs": ["paragraph"],
      "citations": [
        {{"title": "Matching top-level source", "href": "https://authoritative-source.example/path"}}
      ],
      "internalLinks": [
        {{"title": "Relevant Anata resource", "href": "/approved-route", "note": "Why this helps next"}}
      ]
    }}],
    "breadcrumbs": [
      {{"name": "Home", "href": "/"}},
      {{"name": "Blog", "href": "/blog"}},
      {{"name": "Current article"}}
    ],
    "schemaType": "article",
    "articleTitle": "same as h1",
    "articleDescription": "factual summary",
    "related": [
      {{"title": "Relevant service", "href": "/services/relevant-service"}},
      {{"title": "Related educational resource", "href": "/guides/relevant-guide"}}
    ]
  }},
  "sources": [{{"title": "Source title", "url": "https://..."}}]
}}
Write at least 900 useful words across at least four substantive sections with at
least two paragraphs each. Cite top-level authoritative sources contextually in
at least two sections. Add useful internal links contextually in at least two
sections, using only approved Anata routes. Also include the related-resource
block. Do not pad the article to reach the word count.
"""
    article = dict((requester or _request_article)(settings=settings, prompt=prompt))
    slug = _clean(article.get("slug"))
    content = dict(article.get("content") or {})
    article["primaryIntent"] = _clean(cluster.get("normalized_query"))
    article["evidenceId"] = _clean(cluster.get("cluster_id"))
    article["generatedAt"] = publication_timestamp
    article["publishedAt"] = publication_timestamp
    article["modifiedAt"] = publication_timestamp
    article["author"] = {
        "type": "Organization",
        "name": "Anata Inc.",
        "url": "https://anatainc.com",
    }
    content["route"] = f"/blog/{slug}"
    article["content"] = content
    from_editorial_backlog = cluster.get("source_kind") == "editorial_backlog"
    return {
        "page_url": f"https://anatainc.com/blog/{slug}",
        "page_title": _clean(article.get("title")),
        "action_type": "publish_blog_article",
        "section_name": "Generated article registry",
        "before_state": (
            (
                "No dedicated educational article answers this service-adjacent operator "
                f"question; the closest owner is {_clean(cluster.get('owner_url'))}."
            )
            if from_editorial_backlog
            else (
                f"The informational query is landing on {_clean(cluster.get('owner_url'))}, "
                "where measured semantic alignment is weak."
            )
        ),
        "after_state": "A dedicated, source-backed answer page owns the informational intent.",
        "reason": (
            (
                "The approved editorial backlog covers a distinct educational question "
                "that supports a canonical service owner without replacing it."
            )
            if from_editorial_backlog
            else (
                "The query has repeated independent evidence, weak alignment with its current "
                "commercial page, and multiple observed external sources."
            )
        ),
        "insight_source": (
            "Approved editorial backlog and one-page-one-intent map"
            if from_editorial_backlog
            else "Validated query intelligence and answer-engine citations"
        ),
        "expected_impact": "Clearer intent ownership and stronger search and answer-engine retrieval.",
        "confidence": "high",
        "status": "recommended",
        "evidence": [
            (
                f"Approved editorial topic: {_clean(cluster.get('cluster_id'))}."
                if from_editorial_backlog
                else f"Validated cluster: {_clean(cluster.get('cluster_id'))}."
            ),
            "Independent evidence: "
            + ", ".join(_clean(value).replace("_", " ") for value in cluster.get("evidence_classes", []) or [])
            + ".",
            f"Observed authoritative sources: {len(citations)}.",
            (
                "The generated draft must independently verify at least two authoritative "
                "external sources before publication."
                if from_editorial_backlog
                else "The cluster has at least two independent evidence classes, including an observed signal."
            ),
        ],
        "confidence_basis": [
            "The intent is informational and is currently landing on a commercial route.",
            (
                "The topic is a distinct educational question in the approved service-aligned backlog."
                if from_editorial_backlog
                else "Semantic alignment is below the conservative article threshold."
            ),
            "The article includes visible citations and production rollback.",
        ],
        "execution_eligibility": "auto_execute",
        "requires_approval": False,
        "action_value": json.dumps(article, ensure_ascii=False),
    }
