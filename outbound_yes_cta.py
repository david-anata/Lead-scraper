"""Turn a cold-email ask into a small yes, and pause the calendar subsequence.

The opener and the personalization line stay. A line that asks for a call,
a calendar, or a "reply with X" answer is replaced. A URL line is removed.
Nothing here runs until ``apply_yes_cta`` is called.

The subsequence ``Interested - book with David`` is paused rather than
rewritten. A second sequence that also sends on a yes would race Abby's deck.
"""

from __future__ import annotations

import re
from html import unescape
from typing import Any, Callable, Optional

from outbound_cold_sequence import cold_urls

# Live campaigns Abby and the morning push already name.
LIVE_CAMPAIGNS: tuple[tuple[str, str], ...] = (
    ("Trial Offer", "e5f5046b-07e4-41ad-a395-79cf8766ccd0"),
    ("amz_on", "8e0df427-1f18-43ac-b0c6-e8ea787123d7"),
    ("amz_off v2", "138a162d-6654-4916-9959-20c11e31214b"),
    ("plan_upgrade", "4dc10f79-f7c9-43c5-9c3e-5e28e9cf4c72"),
    ("replatform v2", "d1c197e3-bf1f-4f41-bcb9-e30e73975a2c"),
    ("abby_reengage", "907d32fd-d6db-4ce6-8edc-485b78cce120"),
)
BOOKING_SUBSEQUENCE_ID = "0c2f113f-ebed-4890-92df-c241d925dcdf"

FIRST_ASK = "I found something that could help your Amazon growth. Can I send it to you?"
FOLLOW_ASK = "Mind if I put something together for you?"

_CTA_RE = re.compile(
    r"book(?:ing)?\s+(?:a\s+)?(?:call|time)|grab a time|calendar|reply\s+[\"']|"
    r"reply with|intro with david|open to a quick intro|want it\??|should i send|"
    r"which do you prefer|david'?s calendar|worth a look\??|want me to set it up|"
    r"just reply",
    re.I,
)
_TAG_RE = re.compile(r"<[^>]+>")
_ASK_ALREADY = re.compile(r"can i send it to you\?|mind if i put something together", re.I)


def visible_text(value: str) -> str:
    """Drop tags so a CTA can be seen inside an Instantly HTML body."""

    return unescape(_TAG_RE.sub("", value or "")).replace("\xa0", " ").strip()


def _line_is_cta(visible: str) -> bool:
    text = visible.strip()
    if not text:
        return False
    if cold_urls(text):
        return True
    return bool(_CTA_RE.search(text))


def _preserve_variables(original: str, ask: str) -> str:
    """Keep a company token when the old ask was about that brand."""

    if "{{companyName}}" in original and "{{companyName}}" not in ask:
        return ask.replace("your Amazon growth", "{{companyName}}'s Amazon growth").replace(
            "for you?", "for {{companyName}}?",
        )
    return ask


def rewrite_body(body: str, step_index: int) -> str:
    """Replace the ask. Personalization lines and the signature stay."""

    ask = FIRST_ASK if step_index <= 0 else FOLLOW_ASK
    raw = body or ""
    lines = raw.split("\n") if "\n" in raw else re.split(r"(?i)(?=<div)|(?=<br)", raw)
    if len(lines) == 1 and "<div" in raw.lower():
        lines = re.split(r"(?i)(?<=</div>)", raw)
    rewritten: list[str] = []
    replaced = False
    for line in lines:
        visible = visible_text(line)
        if not visible:
            rewritten.append(line)
            continue
        if "{{personalization}}" in line or "{{brandAngle}}" in line:
            rewritten.append(line)
            continue
        if _line_is_cta(visible):
            if replaced:
                continue
            replaced = True
            use = _preserve_variables(visible, ask)
            if "\u2014" in use or "\u2013" in use or " - " in use:
                raise ValueError("Rewritten cold copy contains a dash.")
            if line.strip().startswith("<"):
                rewritten.append(f"<div>{use}</div>")
            else:
                rewritten.append(use)
            continue
        rewritten.append(line)
    text = "\n".join(part for part in rewritten if part is not None)
    if not replaced and not _ASK_ALREADY.search(visible_text(text)):
        use = ask
        if "<div" in raw.lower():
            text = text.rstrip() + f"\n<div>{use}</div>"
        else:
            text = text.rstrip() + f"\n\n{use}"
    if cold_urls(visible_text(text)):
        raise ValueError("Rewritten cold copy still contains a URL.")
    return text


def rewrite_steps(steps: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    """New step dicts, and one before/after record for each body that changed."""

    changed: list[dict[str, str]] = []
    updated: list[dict[str, Any]] = []
    for index, step in enumerate(steps or []):
        if not isinstance(step, dict):
            updated.append(step)
            continue
        copy = dict(step)
        variants = []
        for variant_index, variant in enumerate(copy.get("variants") or []):
            if not isinstance(variant, dict):
                variants.append(variant)
                continue
            variant_copy = dict(variant)
            before = str(variant_copy.get("body") or "")
            after = rewrite_body(before, index)
            if after != before:
                variant_copy["body"] = after
                changed.append({
                    "step": str(index + 1),
                    "variant": str(variant_index + 1),
                    "before": before,
                    "after": after,
                })
            variants.append(variant_copy)
        if variants:
            copy["variants"] = variants
        elif "body" in copy:
            before = str(copy.get("body") or "")
            after = rewrite_body(before, index)
            if after != before:
                copy["body"] = after
                changed.append({"step": str(index + 1), "variant": "1", "before": before, "after": after})
        updated.append(copy)
    return updated, changed


def _sequences_with_steps(campaign: dict[str, Any], steps: list[dict[str, Any]]) -> list[dict[str, Any]]:
    sequences = [dict(item) for item in (campaign.get("sequences") or []) if isinstance(item, dict)]
    if not sequences:
        return [{"steps": steps}]
    sequences[0]["steps"] = steps
    return sequences


def apply_yes_cta(client: Any, *, campaigns: Optional[tuple[tuple[str, str], ...]] = None,
                  subsequence_id: str = BOOKING_SUBSEQUENCE_ID) -> dict[str, Any]:
    """Rewrite the named campaigns and pause the calendar subsequence.

    ``client`` needs ``get_campaign``, ``patch_campaign``, ``get_subsequence``,
    and ``pause_subsequence``. A campaign that already uses the small yes is
    not patched. The subsequence is paused even when its copy is left in place,
    so it cannot send a calendar email after a yes.
    """

    report: dict[str, Any] = {"campaigns": [], "subsequence": {}, "errors": []}
    for name, campaign_id in (campaigns or LIVE_CAMPAIGNS):
        try:
            status, payload = client.get_campaign(campaign_id)
        except Exception as exc:  # noqa: BLE001
            report["errors"].append({"campaign": name, "error": type(exc).__name__})
            continue
        if status != 200 or not isinstance(payload, dict):
            report["errors"].append({"campaign": name, "error": f"read {status}"})
            continue
        steps = ((payload.get("sequences") or [{}])[0].get("steps") or []) if payload.get("sequences") else []
        new_steps, changes = rewrite_steps(steps)
        patched = False
        if changes:
            try:
                patch_status, _body = client.patch_campaign(
                    campaign_id, {"sequences": _sequences_with_steps(payload, new_steps)},
                )
            except Exception as exc:  # noqa: BLE001
                report["errors"].append({"campaign": name, "error": type(exc).__name__})
                patch_status = 0
            patched = patch_status == 200
            if not patched:
                report["errors"].append({"campaign": name, "error": f"patch {patch_status}"})
        report["campaigns"].append({
            "name": name,
            "campaign_id": campaign_id,
            "changed": len(changes),
            "patched": patched,
            "steps": changes,
        })
    try:
        sub_status, sub = client.get_subsequence(subsequence_id)
    except Exception as exc:  # noqa: BLE001
        report["errors"].append({"subsequence": type(exc).__name__})
        return report
    already = str((sub or {}).get("status") or "").lower() in {"2", "paused"} if isinstance(sub, dict) else False
    paused = already
    if sub_status == 200 and not already:
        try:
            pause_status, _pause = client.pause_subsequence(subsequence_id)
        except Exception as exc:  # noqa: BLE001
            report["errors"].append({"subsequence": type(exc).__name__})
            pause_status = 0
        paused = pause_status == 200
        if not paused:
            report["errors"].append({"subsequence": f"pause {pause_status}"})
    report["subsequence"] = {
        "id": subsequence_id,
        "name": "Interested - book with David",
        "action": "already paused" if already else "pause",
        "paused": paused,
        "why": "A calendar email on Interested would race Abby's deck send. The subsequence is paused, not rewritten.",
    }
    return report


def apply_with_instantly(api_key: str, post: Optional[Callable[..., Any]] = None,
                         get: Optional[Callable[..., Any]] = None) -> dict[str, Any]:
    """Call Instantly. The key is a header and is not logged."""

    import requests

    key = (api_key or "").strip()
    if not key:
        return {"campaigns": [], "subsequence": {}, "errors": [{"error": "INSTANTLY_API_KEY is unset"}]}
    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    getter = get or requests.get
    sender = post or requests.request

    class _Client:
        def get_campaign(self, campaign_id: str) -> tuple[int, Any]:
            response = getter(
                f"https://api.instantly.ai/api/v2/campaigns/{campaign_id}",
                headers=headers, timeout=60,
            )
            return response.status_code, response.json() if response.content else None

        def patch_campaign(self, campaign_id: str, body: dict[str, Any]) -> tuple[int, Any]:
            response = sender(
                "PATCH", f"https://api.instantly.ai/api/v2/campaigns/{campaign_id}",
                headers=headers, json=body, timeout=60,
            )
            return response.status_code, response.json() if response.content else None

        def get_subsequence(self, subsequence_id: str) -> tuple[int, Any]:
            response = getter(
                f"https://api.instantly.ai/api/v2/subsequences/{subsequence_id}",
                headers=headers, timeout=60,
            )
            return response.status_code, response.json() if response.content else None

        def pause_subsequence(self, subsequence_id: str) -> tuple[int, Any]:
            response = sender(
                "POST", f"https://api.instantly.ai/api/v2/subsequences/{subsequence_id}/pause",
                headers=headers, json={}, timeout=60,
            )
            return response.status_code, response.json() if response.content else None

    return apply_yes_cta(_Client())
