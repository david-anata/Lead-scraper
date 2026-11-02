"""Instantly reply webhook for Abby.

The shared header value is HMAC-SHA256 of the literal ``instantly-webhook``
keyed by ``SALES_AGENT_INTERNAL_API_KEY``. No separate webhook secret is stored.
The handler and the register job both take that key from app settings, then
the environment. Registration lists existing webhooks first. A missing hook is
created. An existing hook whose header does not match the current key is
patched, and a hook Instantly disabled after failed deliveries (status -1)
is resumed once that header is current.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

logger = logging.getLogger(__name__)

WEBHOOK_HEADER = "X-Instantly-Webhook-Secret"
WEBHOOK_MESSAGE = b"instantly-webhook"
TARGET_URL = "https://agent.anatainc.com/api/webhooks/instantly/reply"
ACCEPTED_EVENTS = ("reply_received", "auto_reply_received")
MARKER_KEY = "abby_reply_webhook_v1"
# Hourly, so a hook Instantly disables is repaired by the register cron.
_OK_RECHECK = timedelta(hours=1)
_FAIL_RECHECK = timedelta(hours=1)


def derived_webhook_secret(api_key: str) -> str:
    """Header value Instantly must send. Empty when the internal key is unset."""

    key = (api_key or "").strip()
    if not key:
        return ""
    return hmac.new(key.encode("utf-8"), WEBHOOK_MESSAGE, hashlib.sha256).hexdigest()


def presented_secret(header_value: str) -> str:
    value = (header_value or "").strip()
    if value.lower().startswith("bearer "):
        value = value[7:].strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        value = value[1:-1].strip()
    return value


def webhook_authorized(header_value: str, api_key: str) -> bool:
    expected = derived_webhook_secret(api_key)
    provided = presented_secret(header_value)
    if not expected or not provided:
        return False
    return hmac.compare_digest(provided, expected)


def internal_api_key_from_env() -> str:
    return (os.getenv("SALES_AGENT_INTERNAL_API_KEY") or "").strip()


def resolve_internal_api_key(settings: Any = None) -> str:
    """The key both the handler and the register job derive the header from.

    Settings win when the app has already loaded them. Otherwise the
    environment is used. The two call sites must not read different sources.
    """

    configured = str(getattr(settings, "internal_api_key", "") or "").strip()
    return configured or internal_api_key_from_env()


def secret_fingerprint(secret: str) -> str:
    """Short hash of the header value, safe to store on the registration marker."""

    value = (secret or "").strip()
    if not value:
        return ""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


def redact(value: Any) -> Any:
    """Drop webhook header values before logs, markers, or API responses."""

    if isinstance(value, dict):
        cleaned = {}
        for key, item in value.items():
            if str(key).lower() in {"headers", "header"}:
                if isinstance(item, dict):
                    cleaned[key] = {str(name): "***" for name in item}
                else:
                    cleaned[key] = "***"
            else:
                cleaned[key] = redact(item)
        return cleaned
    if isinstance(value, list):
        return [redact(item) for item in value]
    return value


def email_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Map an Instantly reply webhook onto the email shape Abby already handles."""

    email_id = str(payload.get("email_id") or "").strip()
    lead = str(payload.get("lead_email") or payload.get("email") or "").strip().lower()
    text = str(payload.get("reply_text") or payload.get("reply_text_snippet") or payload.get("email_text") or "")
    subject = str(payload.get("reply_subject") or payload.get("email_subject") or "")
    auto = str(payload.get("event_type") or "") == "auto_reply_received"
    return {
        "id": email_id,
        "thread_id": str(payload.get("thread_id") or ""),
        "lead": lead,
        "from_address_email": str(payload.get("from_address_email") or lead),
        "eaccount": str(payload.get("email_account") or payload.get("eaccount") or ""),
        "subject": subject,
        "body": {"text": text},
        "content_preview": str(payload.get("reply_text_snippet") or text)[:300],
        "ue_type": 2,
        "timestamp_created": str(payload.get("timestamp") or ""),
        "campaign_id": str(payload.get("campaign_id") or ""),
        "is_auto_reply": 1 if auto else payload.get("is_auto_reply") or 0,
    }


def merge_fetched_email(payload_email: dict[str, Any], fetched: dict[str, Any]) -> dict[str, Any]:
    """Prefer the full Instantly email, and keep the webhook's auto-reply flag."""

    if not fetched or not fetched.get("id"):
        return payload_email
    merged = dict(fetched)
    if payload_email.get("is_auto_reply") and not merged.get("is_auto_reply"):
        merged["is_auto_reply"] = 1
    if not merged.get("campaign_id"):
        merged["campaign_id"] = payload_email.get("campaign_id") or ""
    if not merged.get("lead"):
        merged["lead"] = payload_email.get("lead") or ""
    if not merged.get("eaccount"):
        merged["eaccount"] = payload_email.get("eaccount") or ""
    return merged


def _items(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if isinstance(data, dict):
        for key in ("items", "data", "webhooks"):
            batch = data.get(key)
            if isinstance(batch, list):
                return [item for item in batch if isinstance(item, dict)]
    return []


def _same_hook(hook: dict[str, Any], event_type: str) -> bool:
    url = str(hook.get("target_hook_url") or hook.get("url") or "").rstrip("/")
    return url == TARGET_URL.rstrip("/") and str(hook.get("event_type") or "") == event_type


def _stored_secret(hook: dict[str, Any]) -> str:
    headers = hook.get("headers")
    if not isinstance(headers, dict):
        return ""
    for name, value in headers.items():
        if str(name).lower() == WEBHOOK_HEADER.lower():
            return presented_secret("" if value is None else str(value))
    return ""


def _header_matches(hook: dict[str, Any], secret: str) -> bool:
    expected = presented_secret(secret)
    provided = _stored_secret(hook)
    if not expected or not provided:
        return False
    return hmac.compare_digest(provided, expected)


def _webhook_disabled(hook: dict[str, Any]) -> bool:
    """Instantly sets status to -1 after repeated non-2xx deliveries."""

    status = hook.get("status")
    if status is None or status == "":
        return False
    try:
        return int(status) == -1
    except (TypeError, ValueError):
        return False


def _unsupported_event(status: int, body: Any) -> bool:
    """True when Instantly's create API rejects the event type itself.

    ``auto_reply_received`` is a documented delivery event, and it is not in
    the create-webhook enum. That 400 is not a bad header or a disabled hook.
    """

    if status != 400 or not isinstance(body, dict):
        return False
    message = str(body.get("message") or "")
    return "event_type" in message and "allowed values" in message


def _empty_registration(status: int, body: Any) -> dict[str, Any]:
    safe = redact(body)
    return {
        "ok": False,
        "step": "list",
        "status": status,
        "error": safe,
        "created": [],
        "existing": [],
        "updated": [],
        "unsupported": [],
        "errors": [{"event_type": "*", "status": status, "body": safe}],
    }


def register_abby_webhooks(client: Any, secret: str) -> dict[str, Any]:
    """Create missing Abby webhooks and repair ones Instantly already has.

    A matching url and event is left alone when its header equals ``secret``
    and it is not disabled. A different or missing header is patched. Status
    -1 is resumed after the header is current, via ``POST /webhooks/{id}/resume``.
    The returned dict never includes the secret.
    """

    if not (secret or "").strip():
        return _empty_registration(0, {"message": "missing webhook secret"})

    status, listed = client.list_webhooks()
    if status != 200:
        return _empty_registration(status, listed)
    existing = _items(listed)
    created: list[dict[str, Any]] = []
    kept: list[dict[str, Any]] = []
    updated: list[dict[str, Any]] = []
    unsupported: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for event_type in ACCEPTED_EVENTS:
        match = next((hook for hook in existing if _same_hook(hook, event_type)), None)
        if match is None:
            create_status, create_body = client.create_webhook({
                "target_hook_url": TARGET_URL,
                "event_type": event_type,
                "name": f"Abby {event_type}",
                "campaign": None,
                "headers": {WEBHOOK_HEADER: secret},
            })
            safe_body = redact(create_body)
            if _unsupported_event(create_status, create_body):
                unsupported.append({"event_type": event_type, "status": create_status, "body": safe_body})
                continue
            if create_status not in {200, 201}:
                errors.append({"event_type": event_type, "status": create_status, "body": safe_body})
                continue
            created.append({
                "id": (create_body or {}).get("id") if isinstance(create_body, dict) else None,
                "event_type": event_type,
                "status": create_status,
            })
            continue
        repaired = _repair_hook(client, match, event_type, secret)
        if repaired.get("error"):
            errors.append(repaired["error"])
            continue
        if repaired.get("unsupported"):
            unsupported.append(repaired["unsupported"])
            continue
        if repaired["summary"].get("header_updated") or repaired["summary"].get("resumed"):
            updated.append(repaired["summary"])
        else:
            kept.append(repaired["summary"])
    result = {
        "ok": not errors,
        "target_hook_url": TARGET_URL,
        "created": created,
        "existing": kept,
        "updated": updated,
        "unsupported": unsupported,
        "errors": errors,
    }
    if errors:
        result["plan_error"] = errors
    return result


def _repair_hook(client: Any, match: dict[str, Any], event_type: str, secret: str) -> dict[str, Any]:
    """Patch a stale header, then resume a disabled hook. Never returns the secret."""

    hook_id = str(match.get("id") or "").strip()
    summary: dict[str, Any] = {
        "id": match.get("id"),
        "event_type": event_type,
        "target_hook_url": TARGET_URL,
    }
    disabled = _webhook_disabled(match)
    if not _header_matches(match, secret):
        if not hook_id:
            return {"error": {"event_type": event_type, "status": 0, "body": {"message": "webhook id missing"}}}
        patch = getattr(client, "patch_webhook", None)
        if patch is None:
            return {"error": {"event_type": event_type, "status": 0, "body": {"message": "patch_webhook is not available"}}}
        patch_status, patch_body = patch(hook_id, {"headers": {WEBHOOK_HEADER: secret}})
        safe_body = redact(patch_body)
        if patch_status not in {200, 201}:
            return {"error": {"event_type": event_type, "status": patch_status, "body": safe_body}}
        summary["header_updated"] = True
        if isinstance(patch_body, dict) and patch_body.get("status") is not None:
            disabled = _webhook_disabled(patch_body)
    if disabled:
        if not hook_id:
            return {"error": {"event_type": event_type, "status": 0, "body": {"message": "webhook id missing"}}}
        resume = getattr(client, "resume_webhook", None)
        if resume is None:
            return {"error": {"event_type": event_type, "status": 0, "body": {"message": "resume_webhook is not available"}}}
        resume_status, resume_body = resume(hook_id)
        safe_body = redact(resume_body)
        if resume_status not in {200, 201}:
            return {
                "error": {
                    "event_type": event_type,
                    "status": resume_status,
                    "body": safe_body,
                    "header_updated": bool(summary.get("header_updated")),
                },
            }
        summary["resumed"] = True
    return {"summary": summary}


def _parse_marker(raw: str) -> dict[str, Any]:
    try:
        data = json.loads(raw or "")
    except ValueError:
        return {}
    return data if isinstance(data, dict) else {}


def _marker_due(raw: str, now: datetime, fingerprint: str = "") -> bool:
    """True when the last check is stale or was for a different header value.

    A marker written before the fingerprint existed is due immediately, so the
    first run after deploy repairs a hook that was registered under an older key.
    """

    data = _parse_marker(raw)
    if fingerprint and str(data.get("secret_fingerprint") or "") != fingerprint:
        return True
    checked = data.get("checked_at") or ""
    try:
        stamp = datetime.fromisoformat(str(checked).replace("Z", "+00:00"))
    except ValueError:
        return True
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    wait = _OK_RECHECK if data.get("ok") else _FAIL_RECHECK
    return now - stamp >= wait


def maybe_register_abby_webhook(now: Optional[datetime] = None, settings: Any = None) -> dict[str, Any]:
    """Register once the server has both keys. Safe to call from the Abby cron.

    Missing keys skip without calling Instantly. The header is derived from the
    same key the webhook handler uses. A check is repeated after an hour, and
    immediately when that key has changed. A failure, including a plan
    rejection, keeps the Instantly body on the marker.
    """

    secret_key = resolve_internal_api_key(settings)
    try:
        import outbound_instantly_push as instantly_push

        token = instantly_push.api_key()
    except Exception:  # noqa: BLE001
        logger.exception("[abby] could not read the Instantly key")
        token = ""
    if not secret_key or not token:
        return {"ok": False, "skipped": True, "reason": "missing key"}

    now = now or datetime.now(timezone.utc)
    header_secret = derived_webhook_secret(secret_key)
    fingerprint = secret_fingerprint(header_secret)
    engine = None
    try:
        from sales_support_agent.models.database import get_engine

        engine = get_engine()
        import outbound_suppression as suppression

        raw = suppression.get_cursor(engine, MARKER_KEY)
        if raw and not _marker_due(raw, now, fingerprint):
            data = _parse_marker(raw)
            return {"ok": bool(data.get("ok")), "skipped": True, "reason": "already checked", "result": redact(data)}
    except Exception:  # noqa: BLE001
        logger.exception("[abby] could not read the webhook registration marker")

    import abby_runner

    result = register_abby_webhooks(abby_runner.Instantly(token), header_secret)
    result["checked_at"] = now.isoformat()
    result["secret_fingerprint"] = fingerprint
    safe = redact(result)
    if engine is not None:
        try:
            import outbound_suppression as suppression

            suppression.set_cursor(engine, MARKER_KEY, json.dumps(safe, default=str)[:4000])
        except Exception:  # noqa: BLE001
            logger.exception("[abby] could not save the webhook registration marker")
    if safe.get("ok"):
        logger.info("[abby] instantly reply webhook registration: %s", json.dumps(safe, default=str)[:2000])
    else:
        logger.warning("[abby] instantly reply webhook registration failed: %s", json.dumps(safe, default=str)[:2000])
    return safe


def _watched_campaigns(client: Any) -> list[str]:
    import abby_runner

    reengage: list[str] = []
    named = getattr(client, "campaign_ids_named", None)
    if named is not None:
        try:
            import outbound_reengage as reengage_mod

            reengage = list(named(reengage_mod.NAME) or [])
        except Exception:  # noqa: BLE001
            logger.exception("[abby] could not resolve the re-engage campaign")
    return abby_runner.watched_campaign_ids(reengage)


def reply_state(engine: Any, email_id: str, *, now: Optional[datetime] = None) -> str:
    """``missing``, ``settled``, or ``stale`` for one abby_handled row."""

    from sqlalchemy import text

    now = now or datetime.now(timezone.utc)
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT action, created_at FROM abby_handled WHERE email_id = :i"),
            {"i": email_id},
        ).fetchone()
    if row is None:
        return "missing"
    action = str(row[0] or "")
    if action != "processing":
        return "settled"
    created = row[1]
    if isinstance(created, datetime):
        stamp = created if created.tzinfo else created.replace(tzinfo=timezone.utc)
    else:
        try:
            stamp = datetime.fromisoformat(str(created).replace("Z", "+00:00"))
            if stamp.tzinfo is None:
                stamp = stamp.replace(tzinfo=timezone.utc)
        except ValueError:
            return "stale"
    if now - stamp >= timedelta(minutes=15):
        return "stale"
    return "settled"


def process_reply(
    payload: dict[str, Any],
    *,
    engine: Any = None,
    client: Any = None,
    now: Optional[datetime] = None,
    think: Any = None,
    post_slack: Any = None,
    lead_info: Any = None,
) -> dict[str, Any]:
    """Handle one webhook payload. Sends only when ABBY_MODE and the daily cap allow it."""

    import abby_runner

    event_type = str(payload.get("event_type") or "")
    if event_type not in ACCEPTED_EVENTS:
        return {"status": "ignored", "reason": "event_type", "event_type": event_type}
    email = email_from_payload(payload)
    if not email.get("id"):
        return {"status": "ignored", "reason": "missing_email_id"}
    if abby_runner.mode() == "off":
        return {"status": "skipped", "reason": "ABBY_MODE is off", "email_id": email["id"]}

    if engine is None:
        from sales_support_agent.models.database import get_engine

        engine = get_engine()
    if client is None:
        import outbound_instantly_push as instantly_push

        client = abby_runner.Instantly(instantly_push.api_key())
    getter = getattr(client, "get_email", None)
    if getter is not None and not email.get("thread_id"):
        try:
            email = merge_fetched_email(email, getter(email["id"]) or {})
        except Exception:  # noqa: BLE001
            logger.exception("[abby] could not load the webhook email")

    campaign_id = str(email.get("campaign_id") or payload.get("campaign_id") or "")
    watched = _watched_campaigns(client)
    if campaign_id and watched and campaign_id not in watched:
        return {"status": "ignored", "reason": "campaign_not_watched", "campaign_id": campaign_id}

    abby_runner.ensure_tables(engine)
    state = reply_state(engine, email["id"], now=now)
    if state == "settled":
        return {"status": "duplicate", "email_id": email["id"]}

    counts = abby_runner.handle_email(
        engine,
        email,
        client=client,
        campaign_id=campaign_id,
        now=now,
        think=think,
        post_slack=post_slack,
        lead_info=lead_info,
    )
    return {"status": "processed", "email_id": email["id"], "counts": counts}
