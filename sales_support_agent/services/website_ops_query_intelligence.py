"""Evidence-backed query, intent, citation, and outcome intelligence.

The module keeps immutable raw observations in JSONL and writes a reproducible
snapshot for the operator UI. Simulated prompts never become observed demand,
and unavailable providers never become zero-valued citation results.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
from typing import Any, Callable, Iterable, Mapping, Sequence
from urllib.parse import urlparse

import requests


METHODOLOGY_VERSION = "query-intelligence-v2"
PROMPT_TEMPLATE_VERSION = "commercial-citation-v1"
EVIDENCE_CLASSES = {
    "simulated",
    "observed_search",
    "observed_customer",
    "observed_answer_engine",
}
STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "best",
    "for",
    "how",
    "in",
    "is",
    "of",
    "on",
    "the",
    "to",
    "what",
    "when",
    "who",
    "why",
    "with",
}


@dataclass(frozen=True)
class CitationHarnessConfig:
    enabled: bool
    api_key: str
    model: str
    max_clusters: int


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clean(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _tokens(value: Any) -> list[str]:
    return [
        token
        for token in re.findall(r"[a-z0-9]+", _clean(value).lower())
        if token not in STOPWORDS and len(token) > 1
    ]


def normalize_query(value: Any) -> str:
    """Normalize superficial query variation while preserving raw evidence."""
    tokens = _tokens(value)
    replacements = {
        "advertising": "ads",
        "advertisement": "ads",
        "ecommerce": "commerce",
        "e-commerce": "commerce",
        "provider": "agency",
        "company": "agency",
        "consultant": "consulting",
        "services": "service",
    }
    return " ".join(replacements.get(token, token) for token in tokens)


def query_fingerprint(raw_query: str, page_url: str, evidence_class: str) -> str:
    material = "\n".join(
        (METHODOLOGY_VERSION, evidence_class, page_url.rstrip("/"), raw_query)
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:24]


def classify_intent(query: str) -> tuple[str, str]:
    lowered = _clean(query).lower()
    if any(marker in lowered for marker in ("price", "pricing", "cost", "quote", "hire")):
        return "transactional", "decision"
    if any(marker in lowered for marker in ("vs", "compare", "comparison", "alternative", "best")):
        return "comparison", "consideration"
    if any(marker in lowered for marker in ("agency", "service", "management", "consulting", "partner")):
        return "commercial", "consideration"
    return "informational", "awareness"


def _cluster_key(query: str) -> str:
    normalized = normalize_query(query)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]


def _append_jsonl(path: Path, records: Sequence[Mapping[str, Any]]) -> None:
    if not records:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(dict(record), sort_keys=True, ensure_ascii=True) + "\n")


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                records.append(value)
    return records


def _write_snapshot(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(
        json.dumps(dict(payload), indent=2, sort_keys=True, ensure_ascii=True),
        encoding="utf-8",
    )
    temporary.replace(path)


def query_intelligence_root(settings: Any) -> Path:
    return Path(settings.website_ops_root) / "query_intelligence"


def load_query_intelligence(settings: Any) -> dict[str, Any]:
    path = query_intelligence_root(settings) / "snapshot.json"
    if not path.exists():
        return {
            "status": "not-run",
            "methodology_version": METHODOLOGY_VERSION,
            "clusters": [],
            "recommendations": [],
            "citation_observations": [],
            "summary": {},
        }
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {
            "status": "unavailable",
            "methodology_version": METHODOLOGY_VERSION,
            "clusters": [],
            "recommendations": [],
            "citation_observations": [],
            "summary": {},
        }
    return value if isinstance(value, dict) else {}


def _observation(
    *,
    raw_query: str,
    page_url: str,
    evidence_class: str,
    source_detail: str,
    metrics: Mapping[str, Any] | None = None,
    facet: str = "",
) -> dict[str, Any]:
    if evidence_class not in EVIDENCE_CLASSES:
        raise ValueError(f"Unsupported query evidence class: {evidence_class}")
    intent, funnel_stage = classify_intent(raw_query)
    normalized = normalize_query(raw_query)
    return {
        "observation_id": query_fingerprint(raw_query, page_url, evidence_class),
        "observed_at": _now(),
        "methodology_version": METHODOLOGY_VERSION,
        "raw_query": _clean(raw_query),
        "normalized_query": normalized,
        "cluster_id": _cluster_key(raw_query),
        "page_url": page_url.rstrip("/"),
        "evidence_class": evidence_class,
        "source_detail": source_detail,
        "facet": facet,
        "intent": intent,
        "funnel_stage": funnel_stage,
        "metrics": dict(metrics or {}),
    }


def collect_query_observations(
    page_insights: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    seen: set[str] = set()
    for page in page_insights:
        page_url = _clean(page.get("page_url"))
        aeo = page.get("aeo") if isinstance(page.get("aeo"), Mapping) else {}
        page_observed: list[dict[str, Any]] = []
        for item in aeo.get("observed_queries", []) or []:
            raw_query = _clean(item.get("query"))
            if not raw_query:
                continue
            record = _observation(
                raw_query=raw_query,
                page_url=page_url,
                evidence_class="observed_search",
                source_detail="google_search_console",
                metrics={
                    "clicks": float(item.get("clicks", 0) or 0),
                    "impressions": float(item.get("impressions", 0) or 0),
                },
            )
            if record["observation_id"] not in seen:
                records.append(record)
                page_observed.append(record)
                seen.add(record["observation_id"])
        for item in aeo.get("observed_customer_questions", []) or []:
            raw_query = _clean(item.get("question"))
            if not raw_query:
                continue
            record = _observation(
                raw_query=raw_query,
                page_url=page_url,
                evidence_class="observed_customer",
                source_detail="sanitized_first_party_language",
                metrics={"frequency": int(item.get("frequency", 0) or 0)},
            )
            if page_observed:
                best = max(
                    page_observed,
                    key=lambda candidate: _similarity(
                        raw_query, candidate.get("raw_query")
                    ),
                )
                if _similarity(raw_query, best.get("raw_query")) >= 0.25:
                    record["cluster_id"] = best["cluster_id"]
            if record["observation_id"] not in seen:
                records.append(record)
                seen.add(record["observation_id"])
        for item in aeo.get("simulated_coverage_prompts", []) or []:
            raw_query = _clean(item.get("prompt"))
            if not raw_query:
                continue
            record = _observation(
                raw_query=raw_query,
                page_url=page_url,
                evidence_class="simulated",
                source_detail="deterministic_commercial_fanout",
                facet=_clean(item.get("facet")),
            )
            if page_observed:
                best = max(
                    page_observed,
                    key=lambda candidate: _similarity(
                        raw_query, candidate.get("raw_query")
                    ),
                )
                if _similarity(raw_query, best.get("raw_query")) >= 0.25:
                    record["cluster_id"] = best["cluster_id"]
            if record["observation_id"] not in seen:
                records.append(record)
                seen.add(record["observation_id"])
    return records


def _similarity(query: str, value: Any) -> float:
    query_tokens = set(_tokens(query))
    value_tokens = set(_tokens(value))
    if not query_tokens:
        return 0.0
    return round(len(query_tokens & value_tokens) / len(query_tokens), 3)


def _page_alignment(
    query: str,
    page_url: str,
    page: Mapping[str, Any],
) -> dict[str, Any]:
    aeo = page.get("aeo") if isinstance(page.get("aeo"), Mapping) else {}
    title = _clean(page.get("page_title"))
    slug = urlparse(page_url).path.replace("-", " ")
    simulated = " ".join(
        _clean(item.get("prompt")) for item in aeo.get("simulated_coverage_prompts", []) or []
    )
    title_score = _similarity(query, title)
    url_score = _similarity(query, slug)
    answer_score = _similarity(query, simulated)
    composite = round((title_score * 0.4) + (url_score * 0.25) + (answer_score * 0.35), 3)
    return {
        "title_alignment": title_score,
        "url_alignment": url_score,
        "answer_coverage": answer_score,
        "composite": composite,
        "answer_readiness": _clean(aeo.get("answer_readiness")) or "unknown",
    }


def build_clusters(
    records: Sequence[Mapping[str, Any]],
    page_insights: Sequence[Mapping[str, Any]],
    citation_records: Sequence[Mapping[str, Any]] = (),
) -> list[dict[str, Any]]:
    pages = {_clean(item.get("page_url")).rstrip("/"): item for item in page_insights}
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for record in records:
        grouped.setdefault(_clean(record.get("cluster_id")), []).append(record)

    latest_citations: dict[str, Mapping[str, Any]] = {}
    for citation in citation_records:
        cluster_id = _clean(citation.get("cluster_id"))
        if cluster_id:
            latest_citations[cluster_id] = citation

    clusters: list[dict[str, Any]] = []
    for cluster_id, items in grouped.items():
        raw_queries = list(dict.fromkeys(_clean(item.get("raw_query")) for item in items))
        evidence_classes = sorted({_clean(item.get("evidence_class")) for item in items})
        observed_pages: dict[str, float] = {}
        all_pages: set[str] = set()
        for item in items:
            page_url = _clean(item.get("page_url")).rstrip("/")
            all_pages.add(page_url)
            if item.get("evidence_class") == "observed_search":
                metrics = item.get("metrics") if isinstance(item.get("metrics"), Mapping) else {}
                observed_pages[page_url] = observed_pages.get(page_url, 0.0) + float(
                    metrics.get("impressions", 0) or 0
                )
        candidates = observed_pages or {
            page_url: _page_alignment(raw_queries[0], page_url, pages.get(page_url, {}))[
                "composite"
            ]
            for page_url in all_pages
        }
        owner_url = sorted(candidates, key=lambda url: (-candidates[url], url))[0] if candidates else ""
        conflict_urls = sorted(
            url
            for url, impressions in observed_pages.items()
            if url != owner_url and impressions > 0
        )
        citation = dict(latest_citations.get(cluster_id) or {})
        if citation and citation.get("status") in {"cited", "mentioned", "no-citation"}:
            evidence_classes = sorted(set(evidence_classes) | {"observed_answer_engine"})
        independent_signals = {
            "search" if value == "observed_search" else
            "customer" if value == "observed_customer" else
            "answer_engine" if value == "observed_answer_engine" else
            "simulation"
            for value in evidence_classes
        }
        validated = len(independent_signals) >= 2 and bool(
            independent_signals & {"search", "customer", "answer_engine"}
        )
        intent, funnel_stage = classify_intent(raw_queries[0])
        alignment = _page_alignment(raw_queries[0], owner_url, pages.get(owner_url, {}))
        clusters.append(
            {
                "cluster_id": cluster_id,
                "label": raw_queries[0],
                "raw_queries": raw_queries,
                "normalized_query": normalize_query(raw_queries[0]),
                "intent": intent,
                "funnel_stage": funnel_stage,
                "evidence_classes": evidence_classes,
                "validation_status": "validated" if validated else "hypothesis",
                "owner_url": owner_url,
                "owner_title": _clean(pages.get(owner_url, {}).get("page_title")),
                "conflict_urls": conflict_urls,
                "ownership_status": "conflict" if conflict_urls else "assigned",
                "alignment": alignment,
                "citation": citation,
                "observed_impressions": round(sum(observed_pages.values()), 2),
            }
        )
    return sorted(
        clusters,
        key=lambda item: (
            item["validation_status"] != "validated",
            item["ownership_status"] == "conflict",
            -float(item["observed_impressions"]),
            item["label"],
        ),
    )


def citation_config(settings: Any) -> CitationHarnessConfig:
    enabled = os.getenv("WEBSITE_OPS_CITATION_TESTING_ENABLED", "true").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    return CitationHarnessConfig(
        enabled=enabled,
        api_key=_clean(getattr(settings, "openai_api_key", "")),
        model=os.getenv("WEBSITE_OPS_CITATION_MODEL", "gpt-5-mini").strip() or "gpt-5-mini",
        max_clusters=max(
            1,
            min(int(os.getenv("WEBSITE_OPS_CITATION_MAX_CLUSTERS", "5") or "5"), 20),
        ),
    )


def _openai_web_search(
    *,
    config: CitationHarnessConfig,
    prompt: str,
) -> dict[str, Any]:
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
        timeout=60,
    )
    response.raise_for_status()
    payload = response.json()
    return payload if isinstance(payload, dict) else {}


def _citation_from_response(
    *,
    cluster: Mapping[str, Any],
    config: CitationHarnessConfig,
    prompt: str,
    response: Mapping[str, Any],
) -> dict[str, Any]:
    queries: list[str] = []
    citations: list[dict[str, str]] = []
    retrieval_used = False
    output_text = _clean(response.get("output_text"))
    for item in response.get("output", []) or []:
        if not isinstance(item, Mapping):
            continue
        if item.get("type") == "web_search_call":
            retrieval_used = True
            action = item.get("action") if isinstance(item.get("action"), Mapping) else {}
            queries.extend(_clean(value) for value in action.get("queries", []) or [] if _clean(value))
            if _clean(action.get("query")):
                queries.append(_clean(action.get("query")))
        for content in item.get("content", []) or []:
            if not isinstance(content, Mapping):
                continue
            if content.get("type") == "output_text":
                output_text = _clean(content.get("text")) or output_text
            for annotation in content.get("annotations", []) or []:
                if not isinstance(annotation, Mapping) or annotation.get("type") != "url_citation":
                    continue
                citations.append(
                    {
                        "url": _clean(annotation.get("url")),
                        "title": _clean(annotation.get("title")),
                    }
                )
    cited_anata = any(
        (urlparse(item["url"]).hostname or "").lower().removeprefix("www.") == "anatainc.com"
        for item in citations
        if item.get("url")
    )
    mentioned_anata = "anata" in output_text.lower()
    if cited_anata:
        status = "cited"
    elif mentioned_anata:
        status = "mentioned"
    elif retrieval_used:
        status = "no-citation"
    else:
        status = "no-retrieval"
    return {
        "citation_id": hashlib.sha256(
            f"{cluster.get('cluster_id')}|{config.model}|{prompt}|{_now()}".encode("utf-8")
        ).hexdigest()[:24],
        "observed_at": _now(),
        "methodology_version": METHODOLOGY_VERSION,
        "prompt_template_version": PROMPT_TEMPLATE_VERSION,
        "provider": "openai",
        "model": config.model,
        "cluster_id": _clean(cluster.get("cluster_id")),
        "parent_prompt": prompt,
        "status": status,
        "retrieval_used": retrieval_used,
        "fanout_queries": list(dict.fromkeys(queries)),
        "cited_urls": citations,
        "anata_cited": cited_anata,
        "anata_mentioned": mentioned_anata,
        "response_fingerprint": hashlib.sha256(output_text.encode("utf-8")).hexdigest()[:24],
    }


def run_citation_harness(
    *,
    settings: Any,
    clusters: Sequence[Mapping[str, Any]],
    run_mode: str,
    requester: Callable[..., Mapping[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    config = citation_config(settings)
    if run_mode not in {"weekly", "monthly"}:
        return []
    eligible = [
        item
        for item in clusters
        if item.get("validation_status") == "validated"
        and item.get("ownership_status") == "assigned"
    ][: config.max_clusters]
    if not config.enabled or not config.api_key:
        return [
            {
                "citation_id": hashlib.sha256(
                    f"{item.get('cluster_id')}|unavailable|{_now()}".encode("utf-8")
                ).hexdigest()[:24],
                "observed_at": _now(),
                "methodology_version": METHODOLOGY_VERSION,
                "prompt_template_version": PROMPT_TEMPLATE_VERSION,
                "provider": "openai",
                "model": config.model,
                "cluster_id": _clean(item.get("cluster_id")),
                "parent_prompt": "",
                "status": "unavailable",
                "retrieval_used": False,
                "fanout_queries": [],
                "cited_urls": [],
                "anata_cited": False,
                "anata_mentioned": False,
                "error": "Citation testing is not configured.",
            }
            for item in eligible
        ]
    request = requester or _openai_web_search
    results: list[dict[str, Any]] = []
    for cluster in eligible:
        prompt = (
            f"A business is evaluating {cluster.get('label')}. "
            "Explain what they should compare, what evidence they should verify, "
            "and which providers or resources are useful. Search the web and cite sources."
        )
        try:
            response = request(config=config, prompt=prompt)
            results.append(
                _citation_from_response(
                    cluster=cluster,
                    config=config,
                    prompt=prompt,
                    response=response,
                )
            )
        except Exception as exc:
            results.append(
                {
                    "citation_id": hashlib.sha256(
                        f"{cluster.get('cluster_id')}|error|{_now()}".encode("utf-8")
                    ).hexdigest()[:24],
                    "observed_at": _now(),
                    "methodology_version": METHODOLOGY_VERSION,
                    "prompt_template_version": PROMPT_TEMPLATE_VERSION,
                    "provider": "openai",
                    "model": config.model,
                    "cluster_id": _clean(cluster.get("cluster_id")),
                    "parent_prompt": prompt,
                    "status": "unavailable",
                    "retrieval_used": False,
                    "fanout_queries": [],
                    "cited_urls": [],
                    "anata_cited": False,
                    "anata_mentioned": False,
                    "error": f"Citation provider unavailable: {str(exc)[:180]}",
                }
            )
    return results


def _comparable_citation_changes(
    current: Sequence[Mapping[str, Any]],
    historical: Sequence[Mapping[str, Any]],
) -> dict[str, int]:
    gained = 0
    lost = 0
    prior_by_key: dict[tuple[str, str, str, str], Mapping[str, Any]] = {}
    for item in historical:
        key = (
            _clean(item.get("cluster_id")),
            _clean(item.get("provider")),
            _clean(item.get("model")),
            _clean(item.get("prompt_template_version")),
        )
        prior_by_key[key] = item
    for item in current:
        key = (
            _clean(item.get("cluster_id")),
            _clean(item.get("provider")),
            _clean(item.get("model")),
            _clean(item.get("prompt_template_version")),
        )
        prior = prior_by_key.get(key)
        if not prior:
            continue
        if item.get("status") == "cited" and prior.get("status") != "cited":
            gained += 1
        if item.get("status") != "cited" and prior.get("status") == "cited":
            lost += 1
    return {"gained": gained, "lost": lost}


def build_recommendations(
    *,
    clusters: Sequence[Mapping[str, Any]],
    decision_data_ready: bool,
    weekly_validation_cycles: int,
) -> list[dict[str, Any]]:
    recommendations: list[dict[str, Any]] = []
    for cluster in clusters:
        if cluster.get("validation_status") != "validated":
            continue
        alignment = cluster.get("alignment") if isinstance(cluster.get("alignment"), Mapping) else {}
        conflict = cluster.get("ownership_status") == "conflict"
        block_reasons: list[str] = []
        if not decision_data_ready:
            block_reasons.append("Search Console and GA4 decision data must both be ready.")
        if conflict:
            block_reasons.append("Multiple internal pages have observed ownership evidence.")
        if weekly_validation_cycles < 2:
            block_reasons.append("Two comparable weekly shadow-mode cycles have not completed.")
        if float(alignment.get("title_alignment", 0) or 0) < 0.5:
            action_type = "meta_title_update"
            target = "title"
        elif float(alignment.get("answer_coverage", 0) or 0) < 0.5:
            action_type = "expand_service_page_section"
            target = "visible answer section"
        else:
            continue
        current_state = _clean(cluster.get("owner_title"))
        proposed_state = ""
        action_value = ""
        if action_type == "meta_title_update":
            proposed_state = f"{_clean(cluster.get('label')).title()} | Anata"
            if not 15 <= len(proposed_state) <= 65:
                block_reasons.append("The deterministic title proposal is outside safe length limits.")
        else:
            proposed_state = (
                f"Add a concise, claim-supported answer for: {cluster.get('label')}"
            )
            block_reasons.append(
                "Visible-content copy requires a claim-approved exact proposal."
            )
        if action_type == "meta_title_update" and not block_reasons:
            action_value = json.dumps({"meta_title": proposed_state}, sort_keys=True)
        recommendation_id = hashlib.sha256(
            f"{cluster.get('cluster_id')}|{cluster.get('owner_url')}|{action_type}".encode("utf-8")
        ).hexdigest()[:24]
        recommendations.append(
            {
                "recommendation_id": recommendation_id,
                "cluster_id": cluster.get("cluster_id"),
                "query_cluster": cluster.get("label"),
                "page_url": cluster.get("owner_url"),
                "target": target,
                "action_type": action_type,
                "current_state": current_state,
                "proposed_state": proposed_state,
                "action_value": action_value,
                "reason": (
                    f"The validated {cluster.get('intent')} query cluster is not clearly aligned "
                    f"with the page's {target}."
                ),
                "evidence_classes": cluster.get("evidence_classes", []),
                "confidence": "high" if not block_reasons else "medium",
                "risk": "low",
                "execution_status": "eligible" if not block_reasons else "shadow",
                "block_reasons": block_reasons,
                "validation_method": (
                    "Build, rendered preview, production recrawl, comparable GSC window, "
                    "qualified GA4 leads, and the next comparable citation run."
                ),
                "rollback_method": "Revert only the responsible commit and verify the prior rendered state.",
            }
        )
    return recommendations


def _weekly_cycles(historical_citations: Sequence[Mapping[str, Any]]) -> int:
    weeks = {
        _clean(item.get("observed_at"))[:10]
        for item in historical_citations
        if item.get("status") in {"cited", "mentioned", "no-citation", "no-retrieval"}
    }
    return len(weeks)


def record_outcomes(
    *,
    settings: Any,
    page_insights: Sequence[Mapping[str, Any]],
    decision_data_ready: bool,
    run_mode: str,
) -> list[dict[str, Any]]:
    """Record observed page outcomes without asserting that a change caused them."""
    if not decision_data_ready:
        return [
            {
                "status": "unavailable",
                "reason": "Search Console and GA4 decision data are not both ready.",
                "association_only": True,
            }
        ]
    root = query_intelligence_root(settings)
    path = root / "outcome_observations.jsonl"
    historical = _load_jsonl(path)
    latest_by_page: dict[str, Mapping[str, Any]] = {}
    for item in historical:
        page_url = _clean(item.get("page_url")).rstrip("/")
        if page_url:
            latest_by_page[page_url] = item
    current: list[dict[str, Any]] = []
    for page in page_insights:
        page_url = _clean(page.get("page_url")).rstrip("/")
        gsc = page.get("search_console") if isinstance(page.get("search_console"), Mapping) else {}
        ga4 = page.get("ga4") if isinstance(page.get("ga4"), Mapping) else {}
        metrics = {
            "impressions": float(gsc.get("impressions", 0) or 0),
            "clicks": float(gsc.get("clicks", 0) or 0),
            "ctr": float(gsc.get("ctr", 0) or 0),
            "sessions": float(ga4.get("sessions", 0) or 0),
            "lead_conversions": float(ga4.get("lead_conversions", 0) or 0),
        }
        prior = latest_by_page.get(page_url)
        prior_metrics = (
            prior.get("metrics") if isinstance((prior or {}).get("metrics"), Mapping) else {}
        )
        deltas = {
            key: round(value - float(prior_metrics.get(key, value) or 0), 4)
            for key, value in metrics.items()
        } if prior else {}
        record = {
            "outcome_id": hashlib.sha256(
                f"{page_url}|{run_mode}|{json.dumps(metrics, sort_keys=True)}|{_now()}".encode("utf-8")
            ).hexdigest()[:24],
            "observed_at": _now(),
            "methodology_version": METHODOLOGY_VERSION,
            "page_url": page_url,
            "run_mode": run_mode,
            "status": "observed",
            "metrics": metrics,
            "deltas_from_previous_observation": deltas,
            "association_only": True,
            "method_note": (
                "These are observed before/after associations. Website Ops does not claim "
                "that a publication caused the movement."
            ),
        }
        current.append(record)
    _append_jsonl(path, current)
    return current


def build_query_intelligence(
    *,
    settings: Any,
    page_insights: Sequence[Mapping[str, Any]],
    decision_data_ready: bool,
    run_mode: str,
    citation_requester: Callable[..., Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    root = query_intelligence_root(settings)
    observation_log = root / "query_observations.jsonl"
    citation_log = root / "citation_observations.jsonl"
    historical_citations = _load_jsonl(citation_log)
    observations = collect_query_observations(page_insights)
    existing_ids = {
        _clean(item.get("observation_id")) for item in _load_jsonl(observation_log)
    }
    _append_jsonl(
        observation_log,
        [item for item in observations if item["observation_id"] not in existing_ids],
    )

    clusters = build_clusters(observations, page_insights, historical_citations)
    current_citations = run_citation_harness(
        settings=settings,
        clusters=clusters,
        run_mode=run_mode,
        requester=citation_requester,
    )
    _append_jsonl(citation_log, current_citations)
    all_citations = historical_citations + current_citations
    clusters = build_clusters(observations, page_insights, all_citations)
    comparable_changes = _comparable_citation_changes(
        current_citations,
        historical_citations,
    )
    weekly_cycles = _weekly_cycles(all_citations)
    recommendations = build_recommendations(
        clusters=clusters,
        decision_data_ready=decision_data_ready,
        weekly_validation_cycles=weekly_cycles,
    )
    outcomes = record_outcomes(
        settings=settings,
        page_insights=page_insights,
        decision_data_ready=decision_data_ready,
        run_mode=run_mode,
    )
    observed_outcomes = [item for item in outcomes if item.get("status") == "observed"]
    pages_with_lead_growth = sum(
        1
        for item in observed_outcomes
        if float(
            dict(item.get("deltas_from_previous_observation") or {}).get(
                "lead_conversions", 0
            )
            or 0
        )
        > 0
    )
    summary = {
        "total_clusters": len(clusters),
        "validated_clusters": sum(
            1 for item in clusters if item.get("validation_status") == "validated"
        ),
        "hypothesis_clusters": sum(
            1 for item in clusters if item.get("validation_status") == "hypothesis"
        ),
        "ownership_conflicts": sum(
            1 for item in clusters if item.get("ownership_status") == "conflict"
        ),
        "cited_clusters": sum(
            1 for item in clusters if item.get("citation", {}).get("status") == "cited"
        ),
        "citation_gains": comparable_changes["gained"],
        "citation_losses": comparable_changes["lost"],
        "weekly_validation_cycles": weekly_cycles,
        "shadow_recommendations": sum(
            1 for item in recommendations if item.get("execution_status") == "shadow"
        ),
        "eligible_recommendations": sum(
            1 for item in recommendations if item.get("execution_status") == "eligible"
        ),
        "observed_outcome_pages": len(observed_outcomes),
        "pages_with_associated_lead_growth": pages_with_lead_growth,
    }
    payload = {
        "status": "ready" if decision_data_ready else "partial",
        "generated_at": _now(),
        "methodology_version": METHODOLOGY_VERSION,
        "prompt_template_version": PROMPT_TEMPLATE_VERSION,
        "run_mode": run_mode,
        "summary": summary,
        "clusters": clusters,
        "recommendations": recommendations,
        "citation_observations": current_citations,
        "outcomes": outcomes,
        "policy": {
            "independent_signals_required": 2,
            "weekly_shadow_cycles_required": 2,
            "high_risk_url_actions_allowed": False,
            "simulated_evidence_can_autopublish": False,
        },
    }
    _write_snapshot(root / "snapshot.json", payload)
    return payload
