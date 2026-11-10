"""Rules for outbound cold sequences.

A cold step is the first-touch email we generate for Instantly. It cannot
contain a URL. The booking link belongs in Abby's reply after a prospect
responds, not in these steps.
"""

from __future__ import annotations

import re
from typing import Any

_URL_RE = re.compile(r"https?://\S+|www\.\S+", re.I)


def cold_urls(text: str) -> list[str]:
    """URL-like tokens in one subject or body. Email addresses are not URLs."""

    return _URL_RE.findall(text or "")


def _chunks(step: dict[str, Any]) -> list[str]:
    found = [str(step.get("subject") or ""), str(step.get("body") or "")]
    for variant in step.get("variants") or []:
        if not isinstance(variant, dict):
            continue
        found.append(str(variant.get("subject") or ""))
        found.append(str(variant.get("body") or ""))
    return found


def check_cold_steps(steps: list[dict[str, Any]]) -> list[str]:
    """Every URL in a cold sequence. Empty means the steps are clean."""

    hits: list[str] = []
    for step in steps:
        if not isinstance(step, dict):
            continue
        for chunk in _chunks(step):
            hits.extend(cold_urls(chunk))
    return hits


def require_no_cold_urls(steps: list[dict[str, Any]]) -> None:
    """Refuse to build a cold sequence that contains a URL."""

    hits = check_cold_steps(steps)
    if hits:
        raise ValueError("A cold sequence step contains a URL.")


def known_cold_sequences() -> list[tuple[str, list[dict[str, Any]]]]:
    """Every cold sequence this repo generates."""

    import outbound_reengage as reengage
    import outbound_sales_org as sales

    live = {"campaign_schedule": {"schedules": []}, "email_list": []}
    reengage_steps = reengage.campaign_body(live)["sequences"][0]["steps"]
    return [
        ("reengage", reengage_steps),
        ("sales_org", list(sales.STEPS)),
    ]
