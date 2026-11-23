"""Turn a cold-email ask into a small yes, and pause the calendar subsequence.

The four signal campaigns are replaced with the sequences in
``outbound_signal_copy``. abby_reengage keeps its current steps and gains
the CAN-SPAM footer. Trial Offer is not edited.
Any other campaign still has a call, a calendar, or a "reply with X" line
replaced, and a URL line removed. Nothing here runs until ``apply_yes_cta``
is called.

The subsequence ``Interested - book with David`` is paused rather than
rewritten. A second sequence that also sends on a yes would race Abby's deck.
"""

from __future__ import annotations

import os
import re
from html import unescape
from typing import Any, Callable, Optional

from sqlalchemy import text

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
# Trial Offer keeps the copy it already has.
SKIP_COPY_CAMPAIGN_IDS = frozenset({
    "e5f5046b-07e4-41ad-a395-79cf8766ccd0",
})
REENGAGE_CAMPAIGN_ID = "907d32fd-d6db-4ce6-8edc-485b78cce120"

FIRST_ASK = "I found something that could help your Amazon growth. Can I send it to you?"
FOLLOW_ASK = "Mind if I put something together for you?"

_CTA_RE = re.compile(
    r"book(?:ing)?\s+(?:a\s+)?(?:call|time)|grab a time|calendar|reply\s+[\"']|"
    r"reply with|intro with|worth a (?:quick )?intro|worth a chat|15-minute|"
    r"set one up|would a short|would that be more useful|open to a quick|"
    r"want it\??|should i send|which do you prefer|david'?s calendar|"
    r"worth a look\??|want me to set|just reply",
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
    lines = re.split(r"\n|(?<=</div>)|(?<=</DIV>)", raw)
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
    text = "\n".join(_place_ask(rewritten, ask, replaced, html="<" in raw.lower()))
    text = _drop_ask_below_signature(text)
    if cold_urls(visible_text(text)):
        raise ValueError("Rewritten cold copy still contains a URL.")
    return text


_STANDARD_ASKS = (
    "I found something that could help your Amazon growth. Can I send it to you?",
    "I found something that could help {{companyName}}'s Amazon growth. Can I send it to you?",
    "Mind if I put something together for you?",
    "Mind if I put something together for {{companyName}}?",
)


def _drop_ask_below_signature(text: str) -> str:
    """Remove a standard ask that sits under the sign-off. Keep the one above it."""

    cut = -1
    for marker in ("Account Specialist, Anata", ">Anna<", "\nAnna"):
        pos = text.rfind(marker)
        if pos > cut:
            cut = pos + len(marker)
    if cut < 0:
        return text
    head, tail = text[:cut], text[cut:]
    for ask in _STANDARD_ASKS:
        tail = tail.replace(f"<div>{ask}</div>", "")
        tail = tail.replace(ask, "")
    return head + tail


def _is_signature(visible: str) -> bool:
    text = visible.strip()
    return text in {"Abby", "Anna"} or text.startswith("Account Specialist")


def _is_standard_ask(visible: str) -> bool:
    return bool(_ASK_ALREADY.search(visible or ""))


def _place_ask(lines: list[str], ask: str, replaced: bool, *, html: bool) -> list[str]:
    """Keep one ask, and keep it above the signature.

    A previous run appended the ask under the signature. That copy is moved
    up. A step that already asks a small yes, such as "still on your list?",
    does not get a second question.
    """

    signature_at = next((i for i, line in enumerate(lines) if _is_signature(visible_text(line))), None)
    kept: list[str] = []
    for index, line in enumerate(lines):
        visible = visible_text(line)
        if signature_at is not None and index >= signature_at and _is_standard_ask(visible):
            continue
        kept.append(line)
    before = kept if signature_at is None else kept[:signature_at]
    already = any(_is_standard_ask(visible_text(line)) for line in before)
    soft = any("still on your list?" in visible_text(line).lower() for line in before)
    if replaced or already or soft:
        return kept
    block = f"<div>{ask}</div>" if html else ask
    if signature_at is None:
        return [*kept, "", block]
    return [*kept[:signature_at], block, *kept[signature_at:]]


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


def _variant_disabled(variant: dict[str, Any]) -> bool:
    """Instantly v2 pauses one variant with ``v_disabled``. Missing means live."""

    return variant.get("v_disabled") is True


def _subject_key(value: str) -> str:
    """Subject identity with the company token and extra space removed."""

    text = unescape(str(value or ""))
    text = _TAG_RE.sub(" ", text)
    text = re.sub(r"\{\{.*?\}\}", " ", text)
    return re.sub(r"\s+", " ", text).strip().lower()


def _step_variants(step: Any) -> tuple[bool, list[dict[str, Any]]]:
    """Whether this step names its variants, and the dict variants it has."""

    if not isinstance(step, dict) or not isinstance(step.get("variants"), list):
        return False, []
    return True, [item for item in step["variants"] if isinstance(item, dict)]


def _subject_overlap(model_vars: list[dict[str, Any]], live_vars: list[dict[str, Any]]) -> bool:
    model_keys = {_subject_key(item.get("subject")) for item in model_vars}
    live_keys = {_subject_key(item.get("subject")) for item in live_vars}
    model_keys.discard("")
    live_keys.discard("")
    return bool(model_keys & live_keys)


def _outgoing_variant(variant: dict[str, Any], *, disabled: bool) -> dict[str, Any]:
    """The fields Instantly should store. The restore flag stays in the repo."""

    return {
        "subject": str(variant.get("subject") or ""),
        "body": str(variant.get("body") or ""),
        "v_disabled": bool(disabled),
    }


def _strip_model_step(step: dict[str, Any]) -> dict[str, Any]:
    copy = dict(step)
    _present, variants = _step_variants(step)
    copy["variants"] = [_outgoing_variant(item, disabled=_variant_disabled(item)) for item in variants]
    return copy


def _align_by_subject(model_vars: list[dict[str, Any]], live_vars: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Live order. A model variant with no live subject is not put back."""

    unused = list(model_vars)
    chosen: list[dict[str, Any]] = []
    for live in live_vars:
        key = _subject_key(live.get("subject"))
        match = None
        if key:
            for index, model in enumerate(unused):
                if _subject_key(model.get("subject")) == key:
                    match = unused.pop(index)
                    break
        if match is None:
            chosen.append(_outgoing_variant(live, disabled=_variant_disabled(live)))
            continue
        chosen.append(_outgoing_variant(match, disabled=_variant_disabled(live) or _variant_disabled(match)))
    for model in unused:
        if model.get("restore_if_removed") is True:
            chosen.append(_outgoing_variant(model, disabled=_variant_disabled(model)))
    return chosen


# Words every follow-up shares. They are not enough to tell two variants apart.
_BODY_BOILERPLATE = frozenset({
    "hi", "i", "a", "an", "the", "and", "or", "to", "of", "for", "in", "on",
    "with", "your", "you", "it", "is", "we", "can", "if", "not", "me", "my",
    "from", "that", "this", "be", "at", "by", "as", "do", "our", "us", "put",
    "short", "plan", "together", "send", "amazon", "growth", "brand", "just",
    "let", "know", "follow", "up", "abby", "account", "specialist", "anata",
    "inc", "state", "st", "lehi", "ut",
})


def _body_tokens(value: str) -> tuple[str, ...]:
    """Words that can identify a variant after tags and tokens are removed."""

    text = unescape(str(value or ""))
    text = _TAG_RE.sub(" ", text)
    text = re.sub(r"\{%.*?%\}", " ", text, flags=re.S)
    text = re.sub(r"\{\{.*?\}\}", " ", text, flags=re.S)
    text = re.sub(r"[^a-z0-9]+", " ", text.lower())
    return tuple(tok for tok in text.split() if tok and tok not in _BODY_BOILERPLATE)


def _match_followups(
    model_vars: list[dict[str, Any]], live_vars: list[dict[str, Any]],
) -> list[Optional[int]]:
    """Pair each live follow-up with a model variant.

    An unchanged body wins. A shifted survivor still matches on the words
    that are unique to that variant, such as Sonrei after SeroVital was
    deleted. Position is only the leftover.
    """

    model_tokens = [_body_tokens(item.get("body")) for item in model_vars]
    if len(model_tokens) > 1:
        common = set.intersection(*(set(tokens) for tokens in model_tokens))
    else:
        common = set()
    distinctive = [set(tokens) - common for tokens in model_tokens]
    assignment: list[Optional[int]] = [None] * len(live_vars)
    unused = list(range(len(model_vars)))

    def take(live_index: int, model_index: int) -> None:
        assignment[live_index] = model_index
        unused.remove(model_index)

    for live_index, live in enumerate(live_vars):
        live_key = " ".join(_body_tokens(live.get("body")))
        if not live_key:
            continue
        for model_index in list(unused):
            if live_key == " ".join(model_tokens[model_index]):
                take(live_index, model_index)
                break

    for live_index, live in enumerate(live_vars):
        if assignment[live_index] is not None or not unused:
            continue
        live_fp = set(_body_tokens(live.get("body"))) - common
        scored = [(len(live_fp & distinctive[model_index]), model_index) for model_index in unused]
        scored = [item for item in scored if item[0] >= 2]
        scored.sort(reverse=True)
        if scored and (len(scored) == 1 or scored[0][0] > scored[1][0]):
            take(live_index, scored[0][1])

    for live_index in range(len(live_vars)):
        if assignment[live_index] is None and unused:
            take(live_index, unused[0])
    return assignment


def _align_by_index(model_vars: list[dict[str, Any]], live_vars: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Follow-ups have an empty subject. Match the body, then the position."""

    chosen: list[dict[str, Any]] = []
    used: set[int] = set()
    for live_index, model_index in enumerate(_match_followups(model_vars, live_vars)):
        live = live_vars[live_index]
        if model_index is None:
            chosen.append(_outgoing_variant(live, disabled=_variant_disabled(live)))
            continue
        used.add(model_index)
        model = model_vars[model_index]
        chosen.append(_outgoing_variant(
            model, disabled=_variant_disabled(live) or _variant_disabled(model),
        ))
    for model_index, model in enumerate(model_vars):
        if model_index not in used and model.get("restore_if_removed") is True:
            chosen.append(_outgoing_variant(model, disabled=_variant_disabled(model)))
    return chosen


def _align_variants(model_vars: list[dict[str, Any]], live_vars: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if _subject_overlap(model_vars, live_vars):
        return _align_by_subject(model_vars, live_vars)
    return _align_by_index(model_vars, live_vars)


def preserve_live_variant_state(
    model_steps: list[dict[str, Any]], live_steps: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Keep the live variant list, and never re-enable a paused variant.

    A campaign whose step 1 subjects do not match this copy is a first
    install, so the full model is sent. Once the subjects match, a variant
    that is not in Instantly stays out unless ``restore_if_removed`` is set,
    and ``v_disabled: true`` on the live variant stays true. A follow-up
    with an empty subject is matched on its body before its position, so
    deleting the first variant does not overwrite the one that remains.
    """

    model = [step for step in (model_steps or []) if isinstance(step, dict)]
    live = [step for step in (live_steps or []) if isinstance(step, dict)]
    model_openers = _step_variants(model[0])[1] if model else []
    live_openers = _step_variants(live[0])[1] if live else []
    known = bool(model and live and _subject_overlap(model_openers, live_openers))
    if not known:
        return [_strip_model_step(step) for step in model]

    aligned: list[dict[str, Any]] = []
    for index, model_step in enumerate(model):
        if index >= len(live):
            break
        present, live_vars = _step_variants(live[index])
        _model_present, model_vars = _step_variants(model_step)
        copy = dict(model_step)
        if not present:
            copy["variants"] = [_outgoing_variant(item, disabled=_variant_disabled(item)) for item in model_vars]
        elif not live_vars:
            copy["variants"] = [
                _outgoing_variant(item, disabled=_variant_disabled(item))
                for item in model_vars if item.get("restore_if_removed") is True
            ]
        else:
            copy["variants"] = _align_variants(model_vars, live_vars)
        aligned.append(copy)
    for extra in live[len(model):]:
        present, variants = _step_variants(extra)
        if not present:
            continue
        aligned.append({
            "type": str(extra.get("type") or "email"),
            "delay": int(extra.get("delay") or 0),
            "delay_unit": str(extra.get("delay_unit") or "days"),
            "variants": [_outgoing_variant(item, disabled=_variant_disabled(item)) for item in variants],
        })
    return aligned


def _normalized_steps(steps: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The fields this apply owns: type, delay, unit, subject, body, and v_disabled."""

    normalized: list[dict[str, Any]] = []
    for step in steps or []:
        if not isinstance(step, dict):
            continue
        variants = []
        for variant in step.get("variants") or []:
            if not isinstance(variant, dict):
                continue
            variants.append({
                "subject": str(variant.get("subject") or ""),
                "body": str(variant.get("body") or ""),
                "v_disabled": _variant_disabled(variant),
            })
        normalized.append({
            "type": str(step.get("type") or "email"),
            "delay": int(step.get("delay") or 0),
            "delay_unit": str(step.get("delay_unit") or "days"),
            "variants": variants,
        })
    return normalized


def _body_changes(before_steps: list[dict[str, Any]], after_steps: list[dict[str, Any]]) -> list[dict[str, str]]:
    changes: list[dict[str, str]] = []
    for index, step in enumerate(after_steps):
        previous = before_steps[index] if index < len(before_steps) and isinstance(before_steps[index], dict) else {}
        previous_variants = [item for item in (previous.get("variants") or []) if isinstance(item, dict)]
        for variant_index, variant in enumerate(step.get("variants") or []):
            if not isinstance(variant, dict):
                continue
            before = ""
            before_disabled = False
            if variant_index < len(previous_variants):
                before = str(previous_variants[variant_index].get("body") or "")
                before_disabled = _variant_disabled(previous_variants[variant_index])
            after = str(variant.get("body") or "")
            after_disabled = _variant_disabled(variant)
            if after != before or before_disabled != after_disabled:
                changes.append({
                    "step": str(index + 1),
                    "variant": str(variant_index + 1),
                    "before": before,
                    "after": after,
                })
    return changes


def _step_one_body(payload: dict[str, Any]) -> str:
    """The first variant of step 1, as Instantly returned it."""

    sequences = [item for item in (payload.get("sequences") or []) if isinstance(item, dict)]
    steps = sequences[0].get("steps") if sequences else []
    if not steps or not isinstance(steps[0], dict):
        return ""
    variants = [item for item in (steps[0].get("variants") or []) if isinstance(item, dict)]
    if variants:
        return str(variants[0].get("body") or "")
    return str(steps[0].get("body") or "")


def _read_back_step_one(client: Any, campaign_id: str) -> str:
    """Read step 1 after the patch. Empty when Instantly does not return it."""

    try:
        status, payload = client.get_campaign(campaign_id)
    except Exception:  # noqa: BLE001
        return ""
    if status != 200 or not isinstance(payload, dict):
        return ""
    return _step_one_body(payload)


def _sequences_with_steps(campaign: dict[str, Any], steps: list[dict[str, Any]]) -> list[dict[str, Any]]:
    sequences = [dict(item) for item in (campaign.get("sequences") or []) if isinstance(item, dict)]
    if not sequences:
        return [{"steps": steps}]
    sequences[0]["steps"] = steps
    return sequences


def apply_yes_cta(client: Any, *, campaigns: Optional[tuple[tuple[str, str], ...]] = None,
                  subsequence_id: str = BOOKING_SUBSEQUENCE_ID) -> dict[str, Any]:
    """Replace signal-campaign steps, add the reengage footer, leave Trial Offer alone, and pause the calendar subsequence.

    ``client`` needs ``get_campaign``, ``patch_campaign``, ``get_subsequence``,
    and ``pause_subsequence``. A signal campaign is not patched when the live
    variants already match the repo copy. A paused live variant stays paused,
    and a variant removed in Instantly is not added back. The subsequence is
    paused even when its copy is left in place, so it cannot send a calendar
    email after a yes.
    """

    from outbound_cold_sequence import steps_with_footer
    from outbound_signal_copy import steps_for_campaign_id

    report: dict[str, Any] = {"campaigns": [], "subsequence": {}, "errors": []}
    for name, campaign_id in (campaigns or LIVE_CAMPAIGNS):
        if campaign_id in SKIP_COPY_CAMPAIGN_IDS:
            report["campaigns"].append({
                "name": name,
                "campaign_id": campaign_id,
                "changed": 0,
                "patched": False,
                "skipped": True,
                "reason": "copy left unchanged",
                "steps": [],
            })
            continue
        try:
            status, payload = client.get_campaign(campaign_id)
        except Exception as exc:  # noqa: BLE001
            report["errors"].append({"campaign": name, "error": type(exc).__name__})
            continue
        if status != 200 or not isinstance(payload, dict):
            report["errors"].append({"campaign": name, "error": f"read {status}"})
            continue
        steps = ((payload.get("sequences") or [{}])[0].get("steps") or []) if payload.get("sequences") else []
        signal_steps = steps_for_campaign_id(campaign_id)
        if signal_steps is not None:
            replacement = preserve_live_variant_state(signal_steps, steps)
        elif campaign_id == REENGAGE_CAMPAIGN_ID:
            replacement = steps_with_footer(steps)
        else:
            replacement = None
        if replacement is not None:
            same = _normalized_steps(steps) == _normalized_steps(replacement)
            changes = [] if same else _body_changes(steps, replacement)
            if not same and not changes:
                changes = [{
                    "step": "0",
                    "variant": "0",
                    "before": "previous timing",
                    "after": "days 0, 3, 7, 11, 16",
                }]
            patched = False
            if not same:
                try:
                    patch_status, _body = client.patch_campaign(
                        campaign_id, {"sequences": _sequences_with_steps(payload, replacement)},
                    )
                except Exception as exc:  # noqa: BLE001
                    report["errors"].append({"campaign": name, "error": type(exc).__name__})
                    patch_status = 0
                patched = patch_status == 200
                if not patched and not any(item.get("campaign") == name for item in report["errors"]):
                    report["errors"].append({"campaign": name, "error": f"patch {patch_status}"})
            entry = {
                "name": name,
                "campaign_id": campaign_id,
                "changed": len(changes),
                "patched": patched,
                "steps": changes,
            }
            if patched:
                entry["read_back"] = _read_back_step_one(client, campaign_id)
            report["campaigns"].append(entry)
            continue
        try:
            new_steps, changes = rewrite_steps(steps)
        except ValueError as exc:
            report["errors"].append({"campaign": name, "error": str(exc)})
            continue
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


APPLY_KEY = "yes-cta-v5"
_APPLY_TABLE = "abby_copy_applies"


def ensure_apply_table(engine: Any) -> None:
    """The migration creates this on Vercel. Tests and local runs create it here."""

    import os

    dialect = str(getattr(getattr(engine, "dialect", None), "name", "") or "")
    if os.getenv("VERCEL") and dialect == "postgresql":
        return
    stamp = "TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP" if dialect == "postgresql" else "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
    with engine.begin() as conn:
        conn.execute(text(
            f"""
            CREATE TABLE IF NOT EXISTS {_APPLY_TABLE} (
                apply_key TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                report TEXT NOT NULL DEFAULT '',
                applied_at {stamp}
            )
            """
        ))


def maybe_apply_yes_cta(engine: Any, *, apply: Optional[Callable[[], dict[str, Any]]] = None) -> dict[str, Any]:
    """Rewrite the live asks once. A later tick does not call Instantly again.

    A failed attempt stays retryable. The report, including every before/after
    body, is stored on ``abby_copy_applies`` for the operator to read back.
    """

    from sqlalchemy import text as sql_text

    ensure_apply_table(engine)
    with engine.connect() as conn:
        row = conn.execute(sql_text(
            f"SELECT status FROM {_APPLY_TABLE} WHERE apply_key = :k"
        ), {"k": APPLY_KEY}).fetchone()
    if row and str(row[0]) == "applied":
        return {"ok": True, "skipped": True, "reason": "already applied"}

    from sales_support_agent.services.job_lease import (
        claim_scheduled_job,
        ensure_job_lease_schema,
        finish_scheduled_job,
    )

    if not os.getenv("VERCEL"):
        ensure_job_lease_schema(engine)
    lease = claim_scheduled_job(
        engine, job_key="abby_yes_cta", run_key=APPLY_KEY, lease_minutes=20,
    )
    if lease is None:
        return {"ok": True, "skipped": True, "reason": "apply already running"}

    runner = apply or _apply_with_configured_key
    try:
        report = runner()
    except Exception as exc:  # noqa: BLE001
        finish_scheduled_job(engine, lease, status="failed", details={"error": type(exc).__name__})
        _save_apply(engine, "failed", {"error": type(exc).__name__})
        return {"ok": False, "error": type(exc).__name__}
    errors = list((report or {}).get("errors") or [])
    status = "failed" if errors else "applied"
    _save_apply(engine, status, report or {})
    finish_scheduled_job(engine, lease, status="succeeded" if status == "applied" else "failed", details={"errors": len(errors)})
    if status == "applied":
        _keep_apply_lease(engine, lease.owner_token)
    return {"ok": status == "applied", "status": status, "report": report}


def _apply_with_configured_key() -> dict[str, Any]:
    import outbound_instantly_push as instantly_push

    return apply_with_instantly(instantly_push.api_key())


def _save_apply(engine: Any, status: str, report: dict[str, Any]) -> None:
    import json

    from sqlalchemy import text as sql_text

    payload = json.dumps(report, default=str)
    with engine.begin() as conn:
        conn.execute(sql_text(
            f"""
            INSERT INTO {_APPLY_TABLE} (apply_key, status, report)
            VALUES (:k, :s, :r)
            ON CONFLICT (apply_key) DO UPDATE SET status = excluded.status, report = excluded.report,
                applied_at = CURRENT_TIMESTAMP
            """
        ), {"k": APPLY_KEY, "s": status, "r": payload})


def _keep_apply_lease(engine: Any, owner_token: str) -> None:
    """A succeeded lease is reclaimable once it expires. Keep this one shut."""

    from sqlalchemy import text as sql_text

    with engine.begin() as conn:
        conn.execute(sql_text(
            """
            UPDATE scheduled_job_runs
               SET lease_expires_at = '2099-01-01T00:00:00+00:00'
             WHERE job_key = 'abby_yes_cta' AND run_key = :run AND owner_token = :owner
            """
        ), {"run": APPLY_KEY, "owner": owner_token})
