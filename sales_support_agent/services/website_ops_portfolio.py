"""Truthful daily action portfolio for autonomous Website Ops."""

from __future__ import annotations

from collections import Counter
from typing import Any, Mapping, Sequence

DAILY_ACTION_TARGET = 8


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _pillar(item: Mapping[str, Any]) -> str:
    explicit = _clean(item.get("pillar") or item.get("service_pillar"))
    if explicit:
        return explicit
    value = " ".join(
        (
            _clean(item.get("page_url")),
            _clean(item.get("page_title")),
            _clean(item.get("primary_intent")),
        )
    ).lower()
    if any(token in value for token in ("fulfillment", "3pl", "warehouse", "fba prep")):
        return "Fulfillment / 3PL"
    if any(token in value for token in ("shipping", "carrier", "parcel", "delivery")):
        return "Shipping OS"
    if any(token in value for token in ("intelligence", "analytics", "tacos", "profit")):
        return "Anata Intelligence"
    return "Ecommerce Marketing Management"


def _priority(item: Mapping[str, Any]) -> tuple[int, int, str]:
    eligibility = _clean(item.get("execution_eligibility"))
    confidence = _clean(item.get("confidence")).lower()
    priority = _clean(item.get("priority")).lower()
    return (
        {"auto_execute": 0, "approved": 1, "approval_required": 2}.get(eligibility, 3),
        {"critical": 0, "high": 1, "medium": 2, "low": 3}.get(priority or confidence, 4),
        _clean(item.get("feedback_id") or item.get("candidate_id")),
    )


def build_daily_action_portfolio(
    *,
    action_queue: Sequence[Mapping[str, Any]],
    candidate_ledger: Mapping[str, Any],
) -> dict[str, Any]:
    """Select up to eight qualified actions and explain every empty slot."""

    eligible = [
        dict(item)
        for item in action_queue
        if _clean(item.get("execution_eligibility")) == "auto_execute"
    ]
    seen: set[tuple[str, str]] = set()
    selected: list[dict[str, Any]] = []
    for item in sorted(eligible, key=_priority):
        key = (
            _clean(item.get("page_url")).casefold(),
            _clean(item.get("action_type")).casefold(),
        )
        if key in seen:
            continue
        seen.add(key)
        selected.append({**item, "service_pillar": _pillar(item)})
        if len(selected) >= DAILY_ACTION_TARGET:
            break

    candidates = [
        dict(item)
        for item in candidate_ledger.get("candidates", []) or []
        if isinstance(item, Mapping)
    ]
    state_counts = Counter(_clean(item.get("state")) or "unknown" for item in candidates)
    lane_counts = Counter(_clean(item.get("lane_id")) or "unknown" for item in selected)
    pillar_counts = Counter(_pillar(item) for item in selected)
    missing = max(0, DAILY_ACTION_TARGET - len(selected))
    blockers = [
        {
            "state": state,
            "count": count,
            "reason": {
                "observed": "Evidence has been observed but not independently validated.",
                "verifying": "Rendered-page or repository verification is still running.",
                "validated": "The candidate is valid but lacks a complete eligible executor.",
                "deferred": "A documented gate, conflict, budget, or evidence window prevents execution.",
                "unsupported": "The current evidence does not support a safe marketing-site change.",
            }.get(state, "The candidate is not currently eligible for autonomous execution."),
        }
        for state, count in sorted(state_counts.items())
        if count and state not in {"queued", "completed", "learned"}
    ]
    return {
        "daily_action_target": DAILY_ACTION_TARGET,
        "qualified_actions": selected,
        "qualified_action_count": len(selected),
        "remaining_slots": missing,
        "status": "target_met" if not missing else "underfilled",
        "truthful_summary": (
            "Eight qualified actions are ready for autonomous execution."
            if not missing
            else f"{len(selected)} of 8 daily slots contain qualified actions; {missing} remain empty rather than forcing unsafe work."
        ),
        "lane_mix": dict(sorted(lane_counts.items())),
        "service_pillar_mix": dict(sorted(pillar_counts.items())),
        "candidate_state_counts": dict(sorted(state_counts.items())),
        "empty_slot_reasons": blockers,
        "next_operation": (
            "Execute and production-verify the selected portfolio."
            if selected
            else "Collect or verify evidence until at least one candidate becomes safely executable."
        ),
    }
