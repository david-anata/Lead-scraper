#!/usr/bin/env python3
"""Safe execution helpers for approved website-ops actions."""

from __future__ import annotations

import base64
import json
import os
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from .core import WebsiteOpsConfig, collect_page_observation, load_config


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_BACKUPS_ROOT = ROOT_DIR / "website-ops" / "backups"


class ExecutionError(RuntimeError):
    """Raised when an approved action cannot be safely executed."""


def wp_site_url() -> str:
    return os.getenv("WP_SITE_URL", "").strip().rstrip("/")


def wp_username() -> str:
    return os.getenv("WP_USERNAME", "").strip()


def wp_application_password() -> str:
    return os.getenv("WP_APPLICATION_PASSWORD", "").strip()


def execution_enabled() -> bool:
    return os.getenv("WEBSITE_OPS_EXECUTE_APPROVED", "").strip().lower() in {"1", "true", "yes", "y"}


def backup_root() -> Path:
    configured = os.getenv("WEBSITE_OPS_BACKUPS_DIR", "").strip()
    return Path(configured).expanduser() if configured else DEFAULT_BACKUPS_ROOT


def wp_headers() -> Dict[str, str]:
    username = wp_username()
    password = wp_application_password()
    if not wp_site_url() or not username or not password:
        raise ExecutionError("Missing WP_SITE_URL, WP_USERNAME, or WP_APPLICATION_PASSWORD.")
    token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
    return {
        "Authorization": f"Basic {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def wp_request(path: str, *, method: str = "GET", payload: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    url = f"{wp_site_url()}{path}"
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(url, data=data, headers=wp_headers(), method=method)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = response.read().decode("utf-8", errors="replace")
            return json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise ExecutionError(f"WordPress API error {exc.code} for {path}: {body}") from exc
    except urllib.error.URLError as exc:
        raise ExecutionError(f"WordPress API request failed for {path}: {exc.reason}") from exc


def infer_slug_from_url(page_url: str) -> str:
    path = urllib.parse.urlparse(str(page_url)).path.strip("/")
    if not path:
        return "home"
    return path.split("/")[-1]


def resolve_page_record(feedback: Mapping[str, Any]) -> Dict[str, Any]:
    target_post_id = str(feedback.get("target_post_id", "")).strip()
    if target_post_id:
        record = wp_request(f"/wp-json/wp/v2/pages/{urllib.parse.quote(target_post_id)}?context=edit")
        if record:
            return record

    page_url = str(feedback.get("page_url", "")).strip()
    if not page_url:
        raise ExecutionError("Approved action is missing page_url or target_post_id.")
    slug = infer_slug_from_url(page_url)
    candidates = wp_request(f"/wp-json/wp/v2/pages?slug={urllib.parse.quote(slug)}&context=edit&per_page=50")
    if not isinstance(candidates, list) or not candidates:
        raise ExecutionError(f"No WordPress page found for {page_url}.")
    for candidate in candidates:
        if str(candidate.get("link", "")).rstrip("/") == page_url.rstrip("/"):
            return candidate
    return candidates[0]


def parse_elementor_data(record: Mapping[str, Any]) -> List[Dict[str, Any]]:
    meta = record.get("meta") or {}
    raw = meta.get("_elementor_data")
    if not raw:
        raise ExecutionError("Page does not expose Elementor data in REST meta.")
    if isinstance(raw, list):
        return raw
    return json.loads(str(raw))


def walk_elements(elements: Sequence[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
    for element in elements:
        yield element
        for child in walk_elements(element.get("elements") or []):
            yield child


def update_primary_heading(elements: List[Dict[str, Any]], new_text: str) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    first_heading: Optional[Dict[str, Any]] = None
    target_heading: Optional[Dict[str, Any]] = None
    for element in walk_elements(elements):
        if element.get("widgetType") != "heading":
            continue
        if first_heading is None:
            first_heading = element
        settings = element.get("settings") or {}
        if str(settings.get("header_size", "")).strip().lower() == "h1":
            target_heading = element
            break
    target_heading = target_heading or first_heading
    if target_heading is None:
        raise ExecutionError("No Elementor heading widget found to update.")
    settings = dict(target_heading.get("settings") or {})
    before_text = str(settings.get("title", ""))
    before_size = str(settings.get("header_size", ""))
    settings["title"] = new_text
    settings["header_size"] = "h1"
    target_heading["settings"] = settings
    return elements, {
        "before_text": before_text,
        "after_text": new_text,
        "before_header_size": before_size,
        "after_header_size": "h1",
        "widget_id": str(target_heading.get("id", "")),
    }


def backup_page_record(record: Mapping[str, Any], *, timestamp: datetime) -> Path:
    run_dir = backup_root() / f"{timestamp.date().isoformat()}-approved-actions"
    run_dir.mkdir(parents=True, exist_ok=True)
    path = run_dir / f"page-{record.get('id')}.json"
    path.write_text(json.dumps(record, indent=2, sort_keys=True))
    return path


def execute_feedback_action(
    feedback: Mapping[str, Any],
    *,
    config: Optional[WebsiteOpsConfig] = None,
    timestamp: Optional[datetime] = None,
) -> Dict[str, Any]:
    config = config or load_config()
    timestamp = timestamp or datetime.now(timezone.utc)
    action_type = str(feedback.get("action_type", "")).strip()
    if action_type != "replace_primary_heading":
        raise ExecutionError(f"Unsupported action_type: {action_type or 'missing'}")
    action_value = str(feedback.get("action_value", "")).strip()
    if not action_value:
        raise ExecutionError("replace_primary_heading requires action_value.")

    record = resolve_page_record(feedback)
    backup_path = backup_page_record(record, timestamp=timestamp)
    elementor_data = parse_elementor_data(record)
    updated_data, change_summary = update_primary_heading(elementor_data, action_value)
    updated_record = wp_request(
        f"/wp-json/wp/v2/pages/{record['id']}",
        method="POST",
        payload={"meta": {"_elementor_data": json.dumps(updated_data)}},
    )
    verification = collect_page_observation(str(feedback.get("page_url") or updated_record.get("link")), config=config)
    live_h1 = (verification.get("h1") or [""])[0]
    if live_h1.strip() != action_value.strip():
        raise ExecutionError(f"Verification failed. Expected H1 '{action_value}' but found '{live_h1}'.")
    return {
        "feedback_id": feedback.get("feedback_id"),
        "action_type": action_type,
        "page_url": str(feedback.get("page_url") or updated_record.get("link") or ""),
        "target_post_id": updated_record.get("id"),
        "backup_path": str(backup_path),
        "executed_at": timestamp.isoformat(),
        "verification_status": "verified",
        "summary": change_summary,
    }


def execute_approved_feedback(
    feedback_entries: Sequence[Mapping[str, Any]],
    *,
    config: Optional[WebsiteOpsConfig] = None,
) -> List[Dict[str, Any]]:
    config = config or load_config()
    results: List[Dict[str, Any]] = []
    for feedback in feedback_entries:
        status = str(feedback.get("status", "")).strip().lower()
        if status != "approved":
            continue
        if not str(feedback.get("action_type", "")).strip():
            continue
        results.append(execute_feedback_action(feedback, config=config))
    return results
