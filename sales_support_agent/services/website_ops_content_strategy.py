"""Durable content strategy and editorial work queue for Website Ops."""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.parse import urlparse


DAILY_ARTICLE_MINIMUM = 2
DAILY_ARTICLE_TARGET = 3


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _pillar(cluster: Mapping[str, Any]) -> str:
    value = " ".join(
        (
            _clean(cluster.get("label")),
            _clean(cluster.get("owner_url")),
        )
    ).lower()
    if any(token in value for token in ("fulfillment", "shipping", "warehouse", "3pl", "fba prep")):
        return "Fulfillment and Shipping OS"
    if any(token in value for token in ("intelligence", "tacos", "profit", "analytics")):
        return "Anata Intelligence"
    return "Ecommerce Marketing"


def _source_count(cluster: Mapping[str, Any]) -> int:
    sources = {
        _clean(item.get("url"))
        for item in dict(cluster.get("citation") or {}).get("cited_urls", []) or []
        if isinstance(item, Mapping)
        and _clean(item.get("url")).startswith("https://")
        and (urlparse(_clean(item.get("url"))).hostname or "").removeprefix("www.")
        != "anatainc.com"
    }
    return len(sources)


def _next_iso_week(today: date) -> date:
    days = 7 - today.weekday()
    return today + timedelta(days=days or 7)


def build_content_strategy(
    query_intelligence: Mapping[str, Any],
    *,
    today: date | None = None,
) -> dict[str, Any]:
    """Turn intent evidence into a prioritized, executable editorial program."""

    today = today or datetime.now(timezone.utc).date()
    summary = dict(query_intelligence.get("summary") or {})
    cycles = int(summary.get("weekly_validation_cycles", 0) or 0)
    briefs: list[dict[str, Any]] = []
    for cluster in query_intelligence.get("clusters", []) or []:
        validation = _clean(cluster.get("validation_status"))
        ownership = _clean(cluster.get("ownership_status"))
        quality = _clean(cluster.get("quality_status"))
        intent = _clean(cluster.get("intent"))
        alignment = float(dict(cluster.get("alignment") or {}).get("composite", 1) or 1)
        source_count = _source_count(cluster)
        owner_url = _clean(cluster.get("owner_url"))
        label = _clean(cluster.get("label") or cluster.get("normalized_query"))
        if not label:
            continue

        article_gap = (
            validation == "validated"
            and quality == "eligible"
            and ownership == "assigned"
            and intent == "informational"
            and not urlparse(owner_url).path.startswith(("/blog", "/guides", "/glossary"))
            and alignment < 0.35
        )
        if ownership == "conflict":
            stage = "blocked"
            next_operation = "Resolve the one-page-one-intent ownership conflict."
            earliest = ""
            content_type = "Ownership correction"
        elif validation != "validated":
            stage = "validating"
            next_operation = "Collect another independent observed query or answer-engine signal."
            earliest = ""
            content_type = "Evidence brief"
        elif article_gap and source_count < 2:
            stage = "researching"
            next_operation = "Find and verify at least two authoritative external sources."
            earliest = ""
            content_type = "New article"
        elif article_gap:
            stage = "ready"
            next_operation = "Generate, validate, publish, production-check, and schedule measurement."
            earliest = today.isoformat()
            content_type = "New article"
        else:
            stage = "improve_existing"
            next_operation = (
                "Improve the existing intent owner before creating another URL."
                if owner_url
                else "Assign one canonical owner before planning content."
            )
            earliest = today.isoformat()
            content_type = "Existing-page improvement"

        priority = (
            0 if stage == "ready"
            else 1 if stage in {"researching", "scheduled"}
            else 2 if stage == "improve_existing"
            else 3
        )
        briefs.append(
            {
                "brief_id": _clean(cluster.get("cluster_id")) or label,
                "pillar": _pillar(cluster),
                "topic": label,
                "intent": intent or "unknown",
                "content_type": content_type,
                "owner_url": owner_url,
                "stage": stage,
                "priority": priority,
                "source_count": source_count,
                "cycles_completed": cycles,
                "cycles_required": 0,
                "earliest_publish_date": earliest,
                "next_operation": next_operation,
                "internal_link_plan": (
                    "Link from the current owner and relevant pillar pages; link back to the canonical service owner."
                    if content_type == "New article"
                    else "Strengthen contextual links to this canonical owner from adjacent pages."
                ),
                "success_check": (
                    "Production 200, canonical, Article schema, sitemap discovery, internal links, then comparable GSC and GA4 observations."
                    if content_type == "New article"
                    else "Rendered copy and metadata match the intent, with production crawl and later outcome observation."
                ),
            }
        )

    briefs.sort(key=lambda item: (item["priority"], item["pillar"], item["topic"]))
    by_stage: dict[str, int] = {}
    for brief in briefs:
        by_stage[brief["stage"]] = by_stage.get(brief["stage"], 0) + 1
    next_brief = briefs[0] if briefs else {}
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "objective": (
            "Increase qualified organic discovery by strengthening canonical service owners, "
            "publishing differentiated source-backed answers, and measuring what earns visibility."
        ),
        "operating_rules": [
            "Improve an existing intent owner before creating a new URL.",
            "Publish at least two source-qualified educational articles per day and target three; missed quota remains visible and rolls into the next pulse.",
            "Every article needs two authoritative sources, two independent evidence classes including an observed signal, and a unique informational intent.",
            "Every article ships with internal links, canonical metadata, Article schema, production verification, rollback, and measurement.",
            "No invented facts, clients, results, prices, rankings, search volume, or keyword difficulty.",
        ],
        "daily_article_minimum": DAILY_ARTICLE_MINIMUM,
        "daily_article_target": DAILY_ARTICLE_TARGET,
        "weekly_article_budget": DAILY_ARTICLE_TARGET * 7,
        "summary": {
            "total_briefs": len(briefs),
            "ready_to_publish": by_stage.get("ready", 0),
            "researching_sources": by_stage.get("researching", 0),
            "scheduled_for_validation": by_stage.get("scheduled", 0),
            "improve_existing": by_stage.get("improve_existing", 0),
            "validating": by_stage.get("validating", 0),
            "blocked": by_stage.get("blocked", 0),
        },
        "next_operation": dict(next_brief),
        "briefs": briefs,
    }


def persist_content_strategy(root: Path, strategy: Mapping[str, Any]) -> None:
    directory = root / "content-strategy"
    directory.mkdir(parents=True, exist_ok=True)
    temporary = directory / "snapshot.tmp"
    target = directory / "snapshot.json"
    temporary.write_text(json.dumps(dict(strategy), indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(target)


def load_content_strategy(root: Path) -> dict[str, Any]:
    path = root / "content-strategy" / "snapshot.json"
    if not path.exists():
        return build_content_strategy({})
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return build_content_strategy({})
    return payload if isinstance(payload, dict) else build_content_strategy({})
