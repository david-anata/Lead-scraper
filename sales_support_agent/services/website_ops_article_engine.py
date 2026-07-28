"""Evidence-gated article generation for autonomous Website Ops."""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from typing import Any, Callable, Mapping, Sequence
from urllib.parse import urlparse

import requests

from sales_support_agent.services.website_ops_query_intelligence import citation_config


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


def _eligible_cluster(query_intelligence: Mapping[str, Any]) -> Mapping[str, Any] | None:
    if int(dict(query_intelligence.get("summary") or {}).get("weekly_validation_cycles", 0) or 0) < 2:
        return None
    for cluster in query_intelligence.get("clusters", []) or []:
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


def build_article_action(
    *,
    settings: Any,
    query_intelligence: Mapping[str, Any],
    requester: Callable[..., Mapping[str, Any]] | None = None,
) -> dict[str, Any] | None:
    """Create one bounded article action from a repeatedly validated content gap."""

    if os.getenv("WEBSITE_OPS_ARTICLE_GENERATION_ENABLED", "true").lower() not in {
        "1",
        "true",
        "yes",
        "on",
    }:
        return None
    cluster = _eligible_cluster(query_intelligence)
    if not cluster:
        return None
    citations = [
        {"title": _clean(item.get("title")), "url": _clean(item.get("url"))}
        for item in dict(cluster.get("citation") or {}).get("cited_urls", []) or []
        if isinstance(item, Mapping)
        and _clean(item.get("url")).startswith("https://")
        and (urlparse(_clean(item.get("url"))).hostname or "").removeprefix("www.")
        != "anatainc.com"
    ][:6]
    prompt = f"""
Create a source-backed Anata blog article for the informational query:
{_clean(cluster.get("label"))}

This is a conservative SEO/AEO publishing task. Search and verify the web. Use only
factual claims supported by authoritative HTTPS sources. Do not invent Anata results,
clients, metrics, prices, capabilities, testimonials, or proprietary data. Do not use
em dashes. Do not mention Basic Research, "reveal the gap", or any demo URL.

Previously observed sources:
{json.dumps(citations)}

Return only one JSON object with this exact shape:
{{
  "slug": "lowercase-hyphenated",
  "primaryIntent": "{_clean(cluster.get("normalized_query"))}",
  "evidenceId": "{_clean(cluster.get("cluster_id"))}",
  "generatedAt": "{datetime.now(timezone.utc).isoformat()}",
  "title": "15 to 65 characters",
  "description": "50 to 155 characters",
  "content": {{
    "route": "/blog/SLUG",
    "eyebrow": "Topic label",
    "h1": "same as articleTitle",
    "tldr": {{"heading": "The short answer.", "answer": ["direct answer"]}},
    "sections": [{{"heading": "Heading", "paragraphs": ["paragraph"]}}],
    "breadcrumbs": [
      {{"name": "Home", "href": "/"}},
      {{"name": "Blog", "href": "/blog"}},
      {{"name": "Current article"}}
    ],
    "schemaType": "article",
    "articleTitle": "same as h1",
    "articleDescription": "factual summary",
    "related": [
      {{"title": "Amazon advertising", "href": "/services/amazon-advertising"}}
    ]
  }},
  "sources": [{{"title": "Source title", "url": "https://..."}}]
}}
Include at least three substantive sections and two distinct authoritative sources.
"""
    article = dict((requester or _request_article)(settings=settings, prompt=prompt))
    slug = _clean(article.get("slug"))
    content = dict(article.get("content") or {})
    article["primaryIntent"] = _clean(cluster.get("normalized_query"))
    article["evidenceId"] = _clean(cluster.get("cluster_id"))
    article["generatedAt"] = datetime.now(timezone.utc).isoformat()
    content["route"] = f"/blog/{slug}"
    article["content"] = content
    return {
        "page_url": f"https://anatainc.com/blog/{slug}",
        "page_title": _clean(article.get("title")),
        "action_type": "publish_blog_article",
        "section_name": "Generated article registry",
        "before_state": (
            f"The informational query is landing on {_clean(cluster.get('owner_url'))}, "
            "where measured semantic alignment is weak."
        ),
        "after_state": "A dedicated, source-backed answer page owns the informational intent.",
        "reason": (
            "The query has repeated independent evidence, weak alignment with its current "
            "commercial page, and multiple observed external sources."
        ),
        "insight_source": "Validated query intelligence and answer-engine citations",
        "expected_impact": "Clearer intent ownership and stronger search and answer-engine retrieval.",
        "confidence": "high",
        "status": "recommended",
        "evidence": [
            f"Validated cluster: {_clean(cluster.get('cluster_id'))}.",
            "Independent evidence: "
            + ", ".join(_clean(value).replace("_", " ") for value in cluster.get("evidence_classes", []) or [])
            + ".",
            f"Observed authoritative sources: {len(citations)}.",
            "At least two comparable weekly validation cycles completed.",
        ],
        "confidence_basis": [
            "The intent is informational and is currently landing on a commercial route.",
            "Semantic alignment is below the conservative article threshold.",
            "The article includes visible citations and production rollback.",
        ],
        "execution_eligibility": "auto_execute",
        "requires_approval": False,
        "action_value": json.dumps(article, ensure_ascii=False),
    }
