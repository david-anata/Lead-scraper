"""Ordered pipeline stage utilities for stage-move proposals.

Pipeline stage order is cached by the HubSpot sync service under
``hubspot:pipeline_stages`` as a dict keyed by pipeline_id → ordered list
of {id, label} dicts. This module reads that cache and returns the next
stage in the sequence.
"""

from __future__ import annotations

from typing import Optional

PIPELINE_STAGES_KEY = "hubspot:pipeline_stages"


def get_next_stage(
    pipeline_id: str,
    current_stage_id: str,
    *,
    pipeline_data: dict | None = None,
) -> Optional[tuple[str, str]]:
    """Return ``(next_stage_id, next_stage_label)`` or ``None``.

    ``pipeline_data`` is injected in tests to avoid a KV lookup.
    When ``None`` the live KV cache is read.
    """
    if pipeline_data is None:
        from sales_support_agent.models.database import kv_get_json
        pipeline_data = kv_get_json(PIPELINE_STAGES_KEY) or {}

    stages: list[dict] = (pipeline_data or {}).get(pipeline_id, [])
    if not stages:
        return None

    for i, stage in enumerate(stages):
        if str(stage.get("id", "")) == current_stage_id:
            if i + 1 < len(stages):
                nxt = stages[i + 1]
                nxt_id = str(nxt.get("id", "")).strip()
                nxt_label = str(nxt.get("label", "")).strip()
                if nxt_id:
                    return nxt_id, nxt_label
            return None  # already at the last stage
    return None
