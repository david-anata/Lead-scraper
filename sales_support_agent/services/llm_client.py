"""One Anthropic client for every model call.

On Vercel, calls go to the AI Gateway (https://ai-gateway.vercel.sh) with
VERCEL_OIDC_TOKEN. Gateway model ids are the live provider/model slugs.
A cheaper model from another provider is configured as a gateway fallback.
If the gateway errors, or the OIDC token is missing, the same call is sent
to Anthropic with ANTHROPIC_API_KEY. If both fail, LLMError is raised so
each caller can keep its own fallback.
"""

from __future__ import annotations

import contextvars
import logging
import os
import threading
from typing import Any, Callable, Mapping, Optional

logger = logging.getLogger(__name__)

GATEWAY_BASE_URL = "https://ai-gateway.vercel.sh"
DIRECT_BASE_URL = "https://api.anthropic.com"

HAIKU = "claude-haiku-4-5-20251001"
SONNET = "claude-sonnet-4-6"

_GATEWAY_MODEL = {
    HAIKU: "anthropic/claude-haiku-4.5",
    SONNET: "anthropic/claude-sonnet-4.6",
}
# Cheaper model from another provider, tried after the Anthropic model fails.
_GATEWAY_FALLBACK = {
    "anthropic/claude-haiku-4.5": "google/gemini-2.5-flash-lite",
    "anthropic/claude-sonnet-4.6": "google/gemini-2.5-flash",
}


class LLMError(RuntimeError):
    """The gateway and the direct Anthropic key could not complete the call."""


# Request middleware stores the x-vercel-oidc-token header here. Raw threads
# must be started with start_background_thread so they copy this context.
_oidc_var: contextvars.ContextVar[str] = contextvars.ContextVar("vercel_oidc_token", default="")
_last_route: contextvars.ContextVar[str] = contextvars.ContextVar("llm_route", default="")


def bind_request_oidc(value: str) -> contextvars.Token:
    """Remember the request OIDC header for this context. Returns a reset token."""

    return _oidc_var.set((value or "").strip())


def reset_request_oidc(token: contextvars.Token) -> None:
    _oidc_var.reset(token)


def _header_from_vercel() -> str:
    """Read x-vercel-oidc-token from vercel.headers when that runtime module exists."""

    try:
        import vercel.headers as vercel_headers
    except Exception:  # noqa: BLE001
        return ""
    getter = getattr(vercel_headers, "get", None)
    if not callable(getter):
        return ""
    for name in ("x-vercel-oidc-token", "X-Vercel-Oidc-Token"):
        try:
            value = getter(name)
        except Exception:  # noqa: BLE001
            continue
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _oidc_token() -> str:
    """OIDC token from vercel.headers, then the request context, then the env var."""

    header = _header_from_vercel()
    if header:
        return header
    current = (_oidc_var.get() or "").strip()
    if current:
        return current
    return (os.getenv("VERCEL_OIDC_TOKEN") or "").strip()


def start_background_thread(
    target: Callable[..., Any],
    args: tuple = (),
    kwargs: Optional[dict[str, Any]] = None,
    *,
    name: Optional[str] = None,
    daemon: bool = True,
) -> threading.Thread:
    """Start a raw thread that keeps the caller's contextvars, including the OIDC token."""

    ctx = contextvars.copy_context()
    arguments = kwargs or {}

    def runner() -> None:
        ctx.run(target, *args, **arguments)

    thread = threading.Thread(target=runner, name=name, daemon=daemon)
    thread.start()
    return thread


def llm_available(*, api_key: Optional[str] = None) -> bool:
    """True when a gateway token or a direct Anthropic key is present."""

    if (api_key or "").strip():
        return True
    if (os.getenv("ANTHROPIC_API_KEY") or "").strip():
        return True
    return bool(os.getenv("VERCEL") and _oidc_token())


def _direct_key(api_key: Optional[str]) -> str:
    return (api_key if api_key is not None else os.getenv("ANTHROPIC_API_KEY") or "").strip()


def _gateway_model(model: str) -> str:
    if model in _GATEWAY_MODEL:
        return _GATEWAY_MODEL[model]
    if "/" in (model or ""):
        return model
    return _GATEWAY_MODEL.get(HAIKU, model)


def _tags(caller: str) -> list[str]:
    label = "".join(ch if ch.isalnum() or ch in "-_:" else "-" for ch in (caller or "llm"))
    label = label.strip("-")[:64] or "llm"
    return [f"caller:{label}"[:64]]


def _gateway_body(model: str, caller: str, extra_body: Optional[Mapping[str, Any]]) -> dict[str, Any]:
    body = dict(extra_body or {})
    options = dict(body.get("providerOptions") or {})
    gateway = dict(options.get("gateway") or {})
    fallback = _GATEWAY_FALLBACK.get(model)
    if fallback:
        models = list(gateway.get("models") or [])
        if fallback not in models:
            models.append(fallback)
        gateway["models"] = models
    tags = list(gateway.get("tags") or [])
    for tag in _tags(caller):
        if tag not in tags:
            tags.append(tag)
    gateway["tags"] = tags[:10]
    options["gateway"] = gateway
    body["providerOptions"] = options
    return body


class _Messages:
    def __init__(self, caller: str, api_key: Optional[str]) -> None:
        self.caller = caller
        self.api_key = api_key

    def create(self, **kwargs: Any) -> Any:
        return create_message(caller=self.caller, api_key=self.api_key, **kwargs)


class _LLMClient:
    def __init__(self, caller: str, api_key: Optional[str]) -> None:
        self.caller = caller
        self.messages = _Messages(caller, api_key)


def llm_client(*, caller: str, api_key: Optional[str] = None) -> _LLMClient:
    """Anthropic-shaped client tagged with ``caller`` for spend attribution."""

    return _LLMClient(caller, api_key)


def _invoke_sdk(*, credential: str, gateway: bool, caller: str, kwargs: dict[str, Any]) -> Any:
    import anthropic

    model = kwargs.pop("model")
    extra_body = kwargs.pop("extra_body", None)
    extra_headers = dict(kwargs.pop("extra_headers", None) or {})
    extra_headers["ai-reporting-tags"] = ",".join(_tags(caller))
    if gateway:
        model = _gateway_model(model)
        extra_body = _gateway_body(model, caller, extra_body)
        client = anthropic.Anthropic(api_key=credential, base_url=GATEWAY_BASE_URL)
    elif credential:
        client = anthropic.Anthropic(api_key=credential)
    else:
        # Lets the SDK read ANTHROPIC_API_KEY, and lets tests replace Anthropic.
        client = anthropic.Anthropic()
    return client.messages.create(
        model=model,
        extra_body=extra_body,
        extra_headers=extra_headers,
        **kwargs,
    )


def create_message(*, caller: str, api_key: Optional[str] = None, **kwargs: Any) -> Any:
    """Send one Messages request. Gateway first on Vercel, then the direct key."""

    oidc = _oidc_token()
    on_vercel = bool(os.getenv("VERCEL"))
    gateway_error: Optional[BaseException] = None
    if on_vercel and oidc:
        try:
            message = _invoke_sdk(
                credential=oidc,
                gateway=True,
                caller=caller,
                kwargs=dict(kwargs),
            )
            _last_route.set("gateway")
            return message
        except Exception as exc:  # noqa: BLE001
            gateway_error = exc
            logger.warning("[llm] gateway failed for %s: %s", caller, type(exc).__name__)
    elif on_vercel:
        logger.warning("[llm] OIDC token missing for %s", caller)

    key = _direct_key(api_key)
    try:
        message = _invoke_sdk(
            credential=key,
            gateway=False,
            caller=caller,
            kwargs=dict(kwargs),
        )
        _last_route.set("direct")
        return message
    except Exception as exc:  # noqa: BLE001
        if gateway_error is not None:
            raise LLMError("AI Gateway and the direct Anthropic key both failed.") from exc
        if not key and "could not resolve authentication" in str(exc).lower():
            raise LLMError("No AI Gateway token and no ANTHROPIC_API_KEY.") from exc
        if gateway_error is None and not key and not on_vercel:
            if "could not resolve authentication" in str(exc).lower():
                raise LLMError("No AI Gateway token and no ANTHROPIC_API_KEY.") from exc
        raise


def post_messages(
    *,
    caller: str,
    payload: Mapping[str, Any],
    api_key: Optional[str] = None,
    timeout: int = 90,
) -> dict[str, Any]:
    """POST /v1/messages for tool calls the SDK shape does not wrap.

    Same gateway-then-direct rule as create_message. Returns the JSON body.
    """

    import requests

    body = dict(payload)
    oidc = _oidc_token()
    on_vercel = bool(os.getenv("VERCEL"))
    gateway_error: Optional[BaseException] = None
    if on_vercel and oidc:
        gateway_body = dict(body)
        gateway_body["model"] = _gateway_model(str(body.get("model") or HAIKU))
        gateway_body.update({k: v for k, v in _gateway_body(gateway_body["model"], caller, None).items()})
        try:
            response = requests.post(
                f"{GATEWAY_BASE_URL}/v1/messages",
                headers={
                    "x-api-key": oidc,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                    "ai-reporting-tags": ",".join(_tags(caller)),
                },
                json=gateway_body,
                timeout=timeout,
            )
            response.raise_for_status()
            data = response.json()
            _last_route.set("gateway")
            return data if isinstance(data, dict) else {}
        except Exception as exc:  # noqa: BLE001
            gateway_error = exc
            logger.warning("[llm] gateway failed for %s: %s", caller, type(exc).__name__)
    elif on_vercel:
        logger.warning("[llm] OIDC token missing for %s", caller)

    key = _direct_key(api_key)
    if key:
        try:
            response = requests.post(
                f"{DIRECT_BASE_URL}/v1/messages",
                headers={
                    "x-api-key": key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                    "ai-reporting-tags": ",".join(_tags(caller)),
                },
                json=body,
                timeout=timeout,
            )
            response.raise_for_status()
            data = response.json()
            _last_route.set("direct")
            return data if isinstance(data, dict) else {}
        except Exception as exc:  # noqa: BLE001
            if gateway_error is None:
                raise
            raise LLMError("AI Gateway and the direct Anthropic key both failed.") from exc
    if gateway_error is not None:
        raise LLMError("AI Gateway failed and ANTHROPIC_API_KEY is not set.") from gateway_error
    raise LLMError("No AI Gateway token and no ANTHROPIC_API_KEY.")


def llm_ping() -> dict[str, Any]:
    """One Haiku call used to see whether this request took the gateway.

    No database writes and no Instantly calls.
    """

    try:
        message = create_message(
            caller="llm_ping",
            model=HAIKU,
            max_tokens=16,
            messages=[{"role": "user", "content": "Reply with the single word ok."}],
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("[llm] ping failed: %s", type(exc).__name__)
        return {"route": _last_route.get() or "", "model": "", "ok": False}
    model = str(getattr(message, "model", "") or "")
    return {"route": _last_route.get() or "", "model": model, "ok": True}
