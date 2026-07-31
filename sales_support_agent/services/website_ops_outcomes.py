"""Truthful lifecycle outcomes for autonomous Website Ops runs."""

from __future__ import annotations

from typing import Any, Mapping

VERIFIED_OUTCOME = "production_verified"
ACTIVE_OUTCOME = "work_in_progress"
WAITING_OUTCOME = "evidenced_wait"
NO_OPPORTUNITY_OUTCOME = "no_qualified_opportunity"
FAILED_OUTCOME = "failed_outcome"


def _items(value: Any) -> list[dict[str, Any]]:
    return [dict(item) for item in (value or []) if isinstance(item, Mapping)]


def _verified_actions(report: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [
        item
        for item in _items(report.get("executed_actions"))
        if str(item.get("verification_status", "")).strip().lower() == "verified"
        and bool(str(item.get("commit_sha", "") or item.get("production_url", "")).strip())
    ]


def classify_run_outcome(report: Mapping[str, Any]) -> dict[str, Any]:
    """Classify the achieved outcome, never merely the completed process."""

    operations = dict(report.get("operations_summary") or {})
    article_status = str(operations.get("article_pipeline_status", "") or "").strip().lower()
    article_message = str(operations.get("article_pipeline_message", "") or "").strip()
    verified = _verified_actions(report)
    queue = _items(report.get("action_queue"))
    candidate_states = dict(operations.get("candidate_states") or {})
    deferred = _items(operations.get("deferred_reasons"))
    ready = int(operations.get("auto_ready_actions", 0) or 0)
    completed = int(operations.get("executed_actions", 0) or 0)
    failed_execution = any(
        str(item.get("status", "")).strip().lower() == "error" for item in queue
    )

    if verified:
        return {
            "status": VERIFIED_OUTCOME,
            "summary": f"{len(verified)} production change(s) were independently verified.",
            "expected_output": "A validated marketing-site improvement with durable production evidence.",
            "actual_output": f"{len(verified)} verified production change(s).",
            "production_delta_count": len(verified),
            "last_stage": "production_verification",
            "failure_stage": "",
            "next_operation": "Measure discovery, indexing, engagement, and conversion evidence.",
        }
    if failed_execution or completed:
        return {
            "status": FAILED_OUTCOME,
            "summary": "The run completed work but did not produce independently verified production evidence.",
            "expected_output": "A validated marketing-site improvement with durable production evidence.",
            "actual_output": "No verified production delta.",
            "production_delta_count": 0,
            "last_stage": "execution",
            "failure_stage": "production_verification",
            "next_operation": "Repair verification or publishing, then retry the same qualified action.",
        }
    active_states = sum(
        int(candidate_states.get(name, 0) or 0)
        for name in ("validated", "queued", "executing", "verifying")
    )
    if ready or queue or active_states:
        return {
            "status": ACTIVE_OUTCOME,
            "summary": "Qualified work is actively queued, executing, or being validated.",
            "expected_output": "Advance qualified work toward a verified production improvement.",
            "actual_output": f"{max(ready, active_states, len(queue))} active qualified item(s); no production delta yet.",
            "production_delta_count": 0,
            "last_stage": "validation",
            "failure_stage": "",
            "next_operation": "Execute the highest-value eligible item and verify the rendered production result.",
        }
    if article_status in {"waiting", "cooldown", "measuring", "scheduled"}:
        return {
            "status": WAITING_OUTCOME,
            "summary": article_message or "The next action is waiting on a documented evidence window.",
            "expected_output": "Wait only when the reason and next check are recorded.",
            "actual_output": "No production delta; an evidenced wait is active.",
            "production_delta_count": 0,
            "last_stage": "measurement",
            "failure_stage": "",
            "next_operation": article_message or "Recheck when the evidence window closes.",
        }
    deferred_count = sum(int(item.get("count", 0) or 0) for item in deferred)
    return {
        "status": NO_OPPORTUNITY_OUTCOME,
        "summary": (
            f"No change was authorized; {deferred_count} candidate(s) were deferred with evidence."
            if deferred_count
            else "No qualified opportunity was found in this run."
        ),
        "expected_output": "A verified change or a documented reason why no change is safe.",
        "actual_output": "No production delta and no qualified executable action.",
        "production_delta_count": 0,
        "last_stage": "decision",
        "failure_stage": "",
        "next_operation": "Collect fresh crawl, query, conversion, and customer-language evidence.",
    }
