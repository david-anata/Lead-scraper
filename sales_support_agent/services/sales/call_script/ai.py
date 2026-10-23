"""Ask Claude for the call script, proxied server-side so no key reaches the browser."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

MAX_INPUT_CHARS = 400_000
DEFAULT_MODEL = "claude-sonnet-4-6"
QUICK_MODEL = "claude-haiku-4-5-20251001"


class SampleError(Exception):
    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.status = status


@dataclass(frozen=True)
class SampleResult:
    text: str
    truncated: bool
    model: str


def ai_enabled() -> bool:
    return bool(os.environ.get("ANTHROPIC_API_KEY", "").strip())


def _model_for(tier: str) -> str:
    if tier == "quick":
        return os.environ.get("SALES_CALL_MODEL_QUICK", "").strip() or QUICK_MODEL
    return os.environ.get("SALES_CALL_MODEL", "").strip() or DEFAULT_MODEL


def normalize_messages(raw: Any) -> list[dict]:
    """Accept the script's input shapes: a string, or [{role, content}] ending on user."""
    if isinstance(raw, str):
        messages = [{"role": "user", "content": raw}]
    elif isinstance(raw, list) and raw:
        messages = []
        for item in raw:
            if not isinstance(item, dict):
                raise SampleError("Each message must be an object.")
            role = item.get("role")
            content = item.get("content")
            if role not in ("user", "assistant") or not isinstance(content, str):
                raise SampleError("Messages need a user or assistant role and text content.")
            messages.append({"role": role, "content": content})
        if messages[-1]["role"] != "user":
            raise SampleError("The last message must come from the user.")
    else:
        raise SampleError("Send a prompt string or a list of messages.")
    total = sum(len(m["content"]) for m in messages)
    if total == 0:
        raise SampleError("The prompt is empty.")
    if total > MAX_INPUT_CHARS:
        raise SampleError("The prompt is too long. Trim the transcript and try again.", 413)
    return messages


def run_sample(raw_input: Any, *, tier: str = "default", want_json: bool = False) -> SampleResult:
    key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not key:
        raise SampleError("Ask Claude is not configured on this agent.", 503)
    messages = normalize_messages(raw_input)
    model = _model_for(tier)
    system = (
        "You are the sales coach inside Anata's call script. Follow the "
        "instructions in the user's message exactly. Never invent numbers, "
        "clients or results. Do not use em dashes."
    )
    if want_json:
        system += " Reply with valid JSON only, no prose and no code fences."
    try:
        import anthropic

        client = anthropic.Anthropic(api_key=key)
        msg = client.messages.create(
            model=model,
            max_tokens=4000 if want_json else 2000,
            system=system,
            messages=messages,
        )
    except Exception as exc:  # network, auth, rate limit
        status = getattr(exc, "status_code", None) or 502
        logger.warning("call script sample failed: %s", exc)
        if status == 429:
            raise SampleError("Claude is busy. Try again in a moment.", 429) from exc
        raise SampleError("Claude could not answer right now.", 502) from exc
    text = "".join(
        getattr(block, "text", "") for block in (msg.content or []) if getattr(block, "type", "") == "text"
    ).strip()
    return SampleResult(text=text, truncated=getattr(msg, "stop_reason", "") == "max_tokens", model=model)
