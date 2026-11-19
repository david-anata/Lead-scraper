"""Rules for outbound cold sequences.

A cold step is the first-touch email we generate for Instantly. It cannot
contain a URL. The ask is a small yes. The plan goes out in Abby's reply
after the prospect says yes, not in these steps.
"""

from __future__ import annotations

import re
from typing import Any

_URL_RE = re.compile(r"https?://\S+|www\.\S+", re.I)

# CAN-SPAM: a plain opt-out and a postal address. No link.
OPT_OUT_LINE = "Not a fit? Just let me know and I won't follow up."
POSTAL_ADDRESS = "anata Inc, 1657 N. State St., Lehi, UT 84043"


def has_footer(body: str) -> bool:
    """True when both footer lines are already in the step."""

    text = body or ""
    return OPT_OUT_LINE in text and POSTAL_ADDRESS in text


def append_footer(body: str) -> str:
    """Put the opt-out and the postal address under the signature."""

    if has_footer(body):
        return body or ""
    raw = body or ""
    if "<" in raw:
        return (
            f"{raw}<div><br /></div>"
            f"<div>{OPT_OUT_LINE}</div>"
            f"<div>{POSTAL_ADDRESS}</div>"
        )
    stripped = raw.rstrip()
    spacer = "\n\n" if stripped else ""
    return f"{stripped}{spacer}{OPT_OUT_LINE}\n{POSTAL_ADDRESS}"


def steps_with_footer(steps: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Same steps, with the footer on every variant body. Delays and subjects stay."""

    updated: list[dict[str, Any]] = []
    for step in steps or []:
        if not isinstance(step, dict):
            updated.append(step)
            continue
        copy = dict(step)
        variants = []
        for variant in copy.get("variants") or []:
            if not isinstance(variant, dict):
                variants.append(variant)
                continue
            variant_copy = dict(variant)
            variant_copy["body"] = append_footer(str(variant_copy.get("body") or ""))
            variants.append(variant_copy)
        if variants:
            copy["variants"] = variants
        elif "body" in copy:
            copy["body"] = append_footer(str(copy.get("body") or ""))
        updated.append(copy)
    return updated


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
    import outbound_signal_copy as signal

    live = {"campaign_schedule": {"schedules": []}, "email_list": []}
    reengage_steps = reengage.campaign_body(live)["sequences"][0]["steps"]
    sequences = [
        ("reengage", reengage_steps),
        ("sales_org", list(sales.STEPS)),
    ]
    for key, _campaign_id in signal.CAMPAIGNS:
        sequences.append((key, signal.steps_for(key)))
    return sequences
