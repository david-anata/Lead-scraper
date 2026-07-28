"""Durable, non-overlapping Website Ops candidate and lane projections."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any, Mapping, Sequence


CANDIDATE_STATES = {
    "observed",
    "verifying",
    "disproved",
    "noise",
    "duplicate",
    "validated",
    "unsupported",
    "deferred",
    "queued",
    "producing",
    "validating",
    "publishing",
    "verifying_production",
    "completed",
    "failed",
    "rolling_back",
    "rolled_back",
    "measuring",
    "learned",
}


@dataclass(frozen=True)
class LaneDefinition:
    lane_id: str
    label: str
    executor_status: str
    run_budget: int
    concurrency: int
    risk: str
    validation: str


LANES: tuple[LaneDefinition, ...] = (
    LaneDefinition("broken_internal_links", "Broken internal links", "not_automated", 10, 3, "low", "crawl + render + repository"),
    LaneDefinition("redirect_cleanup", "Redirect-chain cleanup", "not_automated", 5, 1, "medium", "chain + destination + intent"),
    LaneDefinition("canonical_sitemap", "Canonical and sitemap consistency", "autonomous", 5, 1, "high", "desired state + repository + production"),
    LaneDefinition("metadata", "Metadata corrections", "autonomous", 10, 2, "low", "intent owner + observed evidence"),
    LaneDefinition("internal_links", "Contextual internal links", "not_automated", 10, 2, "low", "link graph + relevance + owner"),
    LaneDefinition("page_content", "Existing-page content improvements", "suggestion_only", 3, 1, "medium", "query gap + owner + claims"),
    LaneDefinition("faq_aeo", "FAQ and AEO answer blocks", "suggestion_only", 3, 1, "medium", "observed questions + visible answers"),
    LaneDefinition("structured_data", "Structured data", "not_automated", 5, 2, "low", "visible content + schema validation"),
    LaneDefinition("images", "Image semantics and delivery", "not_automated", 10, 3, "low", "rendered usage + asset evidence"),
    LaneDefinition("content_refresh", "Content refreshes", "not_automated", 3, 1, "medium", "decay or changed-source evidence"),
    LaneDefinition("new_article", "Validated new articles", "autonomous", 1, 1, "medium", "two weeks + two sources + owner check"),
    LaneDefinition("indexing", "Indexing reconciliation", "autonomous", 1000, 2, "variable", "desired search state + technical evidence"),
    LaneDefinition("technical_other", "Other technical observations", "not_automated", 10, 2, "variable", "lane-specific verification"),
)
LANE_BY_ID = {lane.lane_id: lane for lane in LANES}
_LEDGER_WRITE_LOCK = Lock()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _fingerprint(*parts: str) -> str:
    value = "\x1f".join(str(part or "").strip().casefold() for part in parts)
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:24]


def _warning_lane(report: str) -> str:
    normalized = str(report or "").casefold()
    if any(token in normalized for token in ("client_error", "broken", "no_response")):
        return "broken_internal_links"
    if "redirect" in normalized:
        return "redirect_cleanup"
    if any(token in normalized for token in ("canonical", "sitemap", "robots")):
        return "canonical_sitemap"
    if any(token in normalized for token in ("meta_description", "page_titles")):
        return "metadata"
    if any(token in normalized for token in ("internal_outlink", "inlinks", "orphan")):
        return "internal_links"
    if any(token in normalized for token in ("structured_data", "jsonld", "schema")):
        return "structured_data"
    if "image" in normalized:
        return "images"
    return "technical_other"


def action_lane(action_type: str) -> str:
    normalized = str(action_type or "").strip()
    if normalized in {"meta_update", "meta_title_update", "meta_description_update"}:
        return "metadata"
    if normalized == "canonical_update":
        return "canonical_sitemap"
    if normalized == "inject_faq_block":
        return "faq_aeo"
    if normalized == "expand_service_page_section":
        return "page_content"
    if normalized == "publish_blog_article":
        return "new_article"
    return "technical_other"


def select_bounded_actions(
    actions: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Apply lane budgets and serialize actions that share a target or lock key."""

    selected: list[dict[str, Any]] = []
    deferred: list[dict[str, Any]] = []
    lane_counts: dict[str, int] = {}
    locks: set[str] = set()
    for original in actions:
        item = dict(original)
        lane_id = action_lane(str(item.get("action_type", "") or ""))
        lane = LANE_BY_ID[lane_id]
        existing_eligibility = str(item.get("execution_eligibility", "") or "").strip()
        target = str(item.get("page_url", "") or "").strip().casefold()
        lock_key = str(item.get("lock_key", "") or target or item.get("feedback_id", "")).strip().casefold()
        if lane.executor_status != "autonomous":
            item["execution_eligibility"] = "suggestion_only"
            item["execution_reason"] = f"{lane.label} does not have a complete production executor."
            deferred.append(item)
            continue
        if existing_eligibility != "auto_execute":
            item["execution_eligibility"] = existing_eligibility or "approval_required"
            item["execution_reason"] = str(
                item.get("execution_reason", "")
                or "The action has not passed its execution eligibility gate."
            )
            deferred.append(item)
            continue
        if lane_counts.get(lane_id, 0) >= lane.run_budget:
            item["execution_eligibility"] = "deferred"
            item["execution_reason"] = f"{lane.label} reached its {lane.run_budget}-action run budget."
            deferred.append(item)
            continue
        if lock_key and lock_key in locks:
            item["execution_eligibility"] = "deferred"
            item["execution_reason"] = "Another action in this run holds the same page or repository lock."
            deferred.append(item)
            continue
        lane_counts[lane_id] = lane_counts.get(lane_id, 0) + 1
        if lock_key:
            locks.add(lock_key)
        item["lane_id"] = lane_id
        item["lane_budget"] = lane.run_budget
        item["lane_concurrency"] = lane.concurrency
        item["lock_key"] = lock_key
        selected.append(item)
    return selected, deferred


def _candidate(
    *,
    source_type: str,
    source_key: str,
    lane_id: str,
    target_url: str,
    intent_id: str = "",
    state: str,
    reason: str,
    required_gate: str = "",
    earliest_eligible_at: str = "",
    evidence: Sequence[str] = (),
) -> dict[str, Any]:
    if state not in CANDIDATE_STATES:
        raise ValueError(f"Unsupported candidate state: {state}")
    lane = LANE_BY_ID[lane_id]
    return {
        "candidate_id": _fingerprint(source_type, source_key, lane_id, target_url, intent_id),
        "source_type": source_type,
        "source_key": source_key,
        "lane_id": lane_id,
        "lane_label": lane.label,
        "target_url": target_url,
        "intent_id": intent_id,
        "state": state,
        "state_reason": reason,
        "risk": lane.risk,
        "executor_status": lane.executor_status,
        "required_gate": required_gate,
        "earliest_eligible_at": earliest_eligible_at,
        "evidence": [str(item) for item in evidence if str(item).strip()],
        "updated_at": _now(),
    }


def build_candidates(report: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Build one durable state per independently actionable candidate."""

    candidates: list[dict[str, Any]] = []
    crawl = dict(report.get("crawl_verification") or {})
    for record in crawl.get("records", []) or []:
        url = str(record.get("url", "") or "")
        for warning in record.get("warning_results", []) or []:
            verdict = str(warning.get("verdict", "pending") or "pending")
            state = {
                "confirmed": "validated",
                "pending": "verifying",
                "disproved": "disproved",
                "noise": "noise",
            }.get(verdict, "observed")
            warning_report = str(warning.get("report", "") or "")
            candidates.append(
                _candidate(
                    source_type="crawl_warning",
                    # Multiple Screaming Frog exports may report the same
                    # warning class for the same URL. Evidence belongs on the
                    # candidate; it must not create a second opportunity.
                    source_key=warning_report,
                    lane_id=_warning_lane(warning_report),
                    target_url=url,
                    state=state,
                    reason=str(warning.get("reason", "") or "Crawler evidence requires classification."),
                    required_gate=(
                        "Rendered-page and repository verification."
                        if state in {"observed", "verifying"}
                        else ""
                    ),
                    evidence=(str(warning.get("crawler_evidence", "") or ""),),
                )
            )

    query_intelligence = dict(report.get("query_intelligence") or {})
    article = dict(query_intelligence.get("article_pipeline") or {})
    for cluster in query_intelligence.get("clusters", []) or []:
        cluster_id = str(cluster.get("cluster_id", "") or "")
        owner = str(cluster.get("owner_url", "") or "")
        ownership = str(cluster.get("ownership_status", "") or "")
        quality = str(cluster.get("quality_status", "") or "")
        validation = str(cluster.get("validation_status", "") or "")
        intent = str(cluster.get("intent", "") or "")
        if ownership == "conflict":
            state, reason, gate = "deferred", "Intent ownership conflict blocks publishing.", "Resolve the canonical owner."
        elif quality == "quarantined":
            state, reason, gate = "unsupported", "The evidence cluster is quarantined.", "Pass evidence-quality policy."
        elif validation != "validated":
            state, reason, gate = "observed", "The query cluster remains a hypothesis.", "Collect a second independent signal."
        else:
            state, reason, gate = "validated", "The query cluster passed evidence validation.", ""
        lane_id = "new_article" if intent == "informational" else "metadata"
        candidates.append(
            _candidate(
                source_type="query_cluster",
                source_key=cluster_id,
                lane_id=lane_id,
                target_url=owner,
                intent_id=cluster_id,
                state=state,
                reason=reason,
                required_gate=gate,
                earliest_eligible_at=(
                    str(article.get("next_eligible_at", "") or "")
                    if lane_id == "new_article" and state == "validated"
                    else ""
                ),
            )
        )

    for action in report.get("action_queue", []) or []:
        action_type = str(action.get("action_type", "") or "")
        eligibility = str(action.get("execution_eligibility", "") or "")
        state = "queued" if eligibility == "auto_execute" else "deferred"
        candidates.append(
            _candidate(
                source_type="action",
                source_key=str(action.get("feedback_id", "") or action.get("fingerprint", "") or action_type),
                lane_id=action_lane(action_type),
                target_url=str(action.get("page_url", "") or ""),
                state=state,
                reason=str(action.get("execution_reason", "") or action.get("reason", "") or "Qualified action."),
                required_gate="" if state == "queued" else "Pass the remaining approval or claim gate.",
                evidence=tuple(str(item) for item in action.get("evidence", []) or []),
            )
        )

    for action in report.get("executed_actions", []) or []:
        action_type = str(action.get("action_type", "") or "")
        verification = str(action.get("verification_status", "") or "")
        state = "completed" if verification in {"verified", "passed", "success"} else "failed"
        candidates.append(
            _candidate(
                # Use the same identity namespace as the queued action so a
                # completed write replaces, rather than duplicates, its queue
                # state in the current non-overlapping projection.
                source_type="action",
                source_key=str(action.get("feedback_id", "") or action.get("commit_sha", "") or action_type),
                lane_id=action_lane(action_type),
                target_url=str(action.get("page_url", "") or ""),
                state=state,
                reason=str(action.get("message", "") or f"Production verification: {verification or 'unknown'}."),
            )
        )

    return sorted(
        {item["candidate_id"]: item for item in candidates}.values(),
        key=lambda item: (item["state"], item["lane_id"], item["target_url"], item["candidate_id"]),
    )


def candidate_summary(candidates: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    by_state = {state: 0 for state in sorted(CANDIDATE_STATES)}
    by_lane: dict[str, int] = {lane.lane_id: 0 for lane in LANES}
    for item in candidates:
        state = str(item.get("state", "observed"))
        lane_id = str(item.get("lane_id", "technical_other"))
        by_state[state] = by_state.get(state, 0) + 1
        by_lane[lane_id] = by_lane.get(lane_id, 0) + 1
    active_states = {"queued", "producing", "validating", "publishing", "verifying_production"}
    return {
        "total_candidates": len(candidates),
        "by_state": by_state,
        "by_lane": by_lane,
        "validated_candidates": by_state.get("validated", 0),
        "ready_candidates": by_state.get("queued", 0),
        "active_candidates": sum(by_state.get(state, 0) for state in active_states),
        "completed_candidates": by_state.get("completed", 0),
    }


def lane_registry(candidates: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    summary = candidate_summary(candidates)
    return [
        {
            **asdict(lane),
            "candidate_count": int(summary["by_lane"].get(lane.lane_id, 0) or 0),
            "paused": False,
            "pause_reason": "",
        }
        for lane in LANES
    ]


def persist_candidate_ledger(
    root: Path,
    *,
    candidates: Sequence[Mapping[str, Any]],
    run_id: str,
) -> dict[str, Any]:
    """Persist the current projection and append only actual state transitions."""

    with _LEDGER_WRITE_LOCK:
        directory = root / "candidates"
        directory.mkdir(parents=True, exist_ok=True)
        snapshot_path = directory / "snapshot.json"
        previous: dict[str, Any] = {}
        if snapshot_path.exists():
            try:
                previous_payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
                previous = {
                    str(item.get("candidate_id", "")): dict(item)
                    for item in previous_payload.get("candidates", []) or []
                }
            except (OSError, json.JSONDecodeError):
                previous = {}
        normalized = [dict(item) for item in candidates]
        generated_at = _now()
        transitions = []
        for item in normalized:
            old = previous.get(str(item.get("candidate_id", "")))
            old_state = str((old or {}).get("state", "") or "")
            new_state = str(item.get("state", "") or "")
            if old_state != new_state:
                transitions.append(
                    {
                        "candidate_id": item.get("candidate_id"),
                        "run_id": run_id,
                        "from_state": old_state or None,
                        "to_state": new_state,
                        "reason": item.get("state_reason", ""),
                        "transitioned_at": generated_at,
                    }
                )
        if transitions:
            with (directory / "transitions.jsonl").open("a", encoding="utf-8") as handle:
                for transition in transitions:
                    handle.write(json.dumps(transition, sort_keys=True) + "\n")
        payload = {
            "generated_at": generated_at,
            "run_id": run_id,
            "summary": candidate_summary(normalized),
            "lanes": lane_registry(normalized),
            "candidates": normalized,
        }
        temporary_path = directory / f"snapshot.{run_id}.tmp"
        temporary_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        temporary_path.replace(snapshot_path)
        return payload


def load_candidate_ledger(root: Path) -> dict[str, Any]:
    path = root / "candidates" / "snapshot.json"
    if not path.exists():
        return {
            "generated_at": "",
            "run_id": "",
            "summary": candidate_summary([]),
            "lanes": lane_registry([]),
            "candidates": [],
        }
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {
            "generated_at": "",
            "run_id": "",
            "summary": candidate_summary([]),
            "lanes": lane_registry([]),
            "candidates": [],
        }
    return payload if isinstance(payload, dict) else {}
