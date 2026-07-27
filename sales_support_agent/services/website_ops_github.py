"""Validated GitHub-backed metadata updates for the Anata marketing site."""

from __future__ import annotations

import base64
import json
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Mapping
from urllib.parse import urlparse

import requests

from sales_support_agent.services import website_ops_vendor as website_ops


METADATA_ACTION_TYPES = {
    "meta_update",
    "meta_title_update",
    "meta_description_update",
    "canonical_update",
}
EXCLUDED_PATH_PREFIXES = ("/api/", "/book", "/brand", "/preview", "/x/")


def github_metadata_is_configured() -> bool:
    return bool(
        os.getenv("WEBSITE_OPS_GITHUB_TOKEN", "").strip()
        and os.getenv("WEBSITE_OPS_GITHUB_REPOSITORY", "david-anata/anata-website").strip()
    )


def _action_payload(record: Mapping[str, Any]) -> dict[str, Any]:
    raw = str(record.get("action_value") or record.get("suggested_action_value") or "").strip()
    if not raw:
        raise website_ops.ExecutionError("Metadata action is missing an action value.")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise website_ops.ExecutionError("Metadata action value must be a JSON object.") from exc
    if not isinstance(payload, dict):
        raise website_ops.ExecutionError("Metadata action value must be a JSON object.")
    return payload


def route_source_path(page_url: str) -> str:
    parsed = urlparse(page_url)
    allowed_host = os.getenv("WEBSITE_OPS_ALLOWED_HOST", "anatainc.com").strip().lower()
    if parsed.scheme != "https" or parsed.hostname != allowed_host:
        raise website_ops.ExecutionError("Metadata autopush is restricted to the production marketing host.")
    route = parsed.path.rstrip("/") or "/"
    if route != "/" and (
        route.startswith(EXCLUDED_PATH_PREFIXES)
        or "[" in route
        or "]" in route
        or ".." in route
    ):
        raise website_ops.ExecutionError("This route is outside the metadata autopush scope.")
    if route == "/":
        return "src/app/page.tsx"
    return f"src/app{route}/page.tsx"


def validate_metadata_action(record: Mapping[str, Any]) -> dict[str, str]:
    action_type = str(record.get("action_type") or record.get("suggested_action_type") or "").strip()
    if action_type not in METADATA_ACTION_TYPES:
        raise website_ops.ExecutionError("Only SEO metadata actions can use the GitHub autopush path.")
    confidence = str(record.get("confidence", "")).strip().lower()
    if confidence != "high":
        raise website_ops.ExecutionError("Metadata autopush requires high-confidence evidence.")
    reason = str(
        record.get("reason")
        or record.get("details")
        or record.get("execution_reason")
        or ""
    ).strip()
    evidence = record.get("evidence")
    if not reason or not evidence:
        raise website_ops.ExecutionError("Metadata autopush requires a validated reason and evidence.")

    page_url = str(record.get("page_url", "")).strip()
    route_source_path(page_url)
    payload = _action_payload(record)
    allowed_keys = {"meta_title", "meta_description", "canonical_url"}
    unexpected = sorted(set(payload) - allowed_keys)
    if unexpected:
        raise website_ops.ExecutionError(
            f"Metadata action contains unsupported fields: {', '.join(unexpected)}."
        )
    title = str(payload.get("meta_title", "")).strip()
    description = str(payload.get("meta_description", "")).strip()
    canonical = str(payload.get("canonical_url", "")).strip()
    if action_type == "meta_title_update" and not title:
        raise website_ops.ExecutionError("Meta title update is missing meta_title.")
    if action_type == "meta_description_update" and not description:
        raise website_ops.ExecutionError("Meta description update is missing meta_description.")
    if action_type == "canonical_update" and not canonical:
        raise website_ops.ExecutionError("Canonical update is missing canonical_url.")
    if action_type in {"meta_update", "meta_title_update"} and title:
        if not 15 <= len(title) <= 65:
            raise website_ops.ExecutionError("Meta title must be between 15 and 65 characters.")
    if action_type in {"meta_update", "meta_description_update"} and description:
        if not 50 <= len(description) <= 170:
            raise website_ops.ExecutionError("Meta description must be between 50 and 170 characters.")
    if canonical:
        expected = page_url.rstrip("/")
        if canonical.rstrip("/") != expected:
            raise website_ops.ExecutionError("Canonical URL must match the scoped production page URL.")
    if not any((title, description, canonical)):
        raise website_ops.ExecutionError("Metadata action contains no requested change.")
    return {
        "meta_title": title,
        "meta_description": description,
        "canonical_url": canonical,
    }


def _metadata_object_span(source: str) -> tuple[int, int]:
    match = re.search(r"export\s+const\s+metadata(?:\s*:\s*Metadata)?\s*=\s*\{", source)
    if not match:
        raise website_ops.ExecutionError("The route does not use a static metadata object.")
    start = source.find("{", match.start())
    depth = 0
    quote = ""
    escaped = False
    for index in range(start, len(source)):
        char = source[index]
        if quote:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = ""
            continue
        if char in {'"', "'", "`"}:
            quote = char
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return start, index + 1
    raise website_ops.ExecutionError("The static metadata object could not be parsed safely.")


def update_static_metadata_source(source: str, requested: Mapping[str, str]) -> str:
    start, end = _metadata_object_span(source)
    block = source[start:end]

    def replace_string_property(value: str, names: tuple[str, ...]) -> None:
        nonlocal block
        if not value:
            return
        property_pattern = "|".join(re.escape(name) for name in names)
        pattern = re.compile(
            rf"(?P<prefix>\b(?:{property_pattern})\s*:\s*)(?P<value>[\"'](?:\\.|[^\"'])*?[\"'])",
            re.DOTALL,
        )
        match = pattern.search(block)
        if not match:
            raise website_ops.ExecutionError(
                f"The metadata object is missing the {names[0]} property."
            )
        block = block[: match.start("value")] + json.dumps(value) + block[match.end("value") :]

    replace_string_property(str(requested.get("meta_title", "")), ("title",))
    replace_string_property(str(requested.get("meta_description", "")), ("description",))
    canonical = str(requested.get("canonical_url", ""))
    if canonical:
        canonical_pattern = re.compile(
            r"(?P<prefix>\bcanonical\s*:\s*)(?P<value>[\"'](?:\\.|[^\"'])*?[\"'])",
            re.DOTALL,
        )
        match = canonical_pattern.search(block)
        if match:
            block = block[: match.start("value")] + json.dumps(canonical) + block[match.end("value") :]
        elif "alternates" in block:
            raise website_ops.ExecutionError("Existing metadata alternates require manual canonical review.")
        else:
            base = block[:-1].rstrip()
            separator = "" if base.endswith(",") else ","
            block = base + f"{separator}\n  alternates: {{ canonical: {json.dumps(canonical)} }},\n}}"
    updated = source[:start] + block + source[end:]
    _metadata_object_span(updated)
    return updated


class GitHubWebsiteClient:
    def __init__(self) -> None:
        self.token = os.getenv("WEBSITE_OPS_GITHUB_TOKEN", "").strip()
        self.repository = os.getenv(
            "WEBSITE_OPS_GITHUB_REPOSITORY", "david-anata/anata-website"
        ).strip()
        self.branch = os.getenv("WEBSITE_OPS_GITHUB_BRANCH", "main").strip() or "main"
        self.api_base = os.getenv("GITHUB_API_URL", "https://api.github.com").rstrip("/")
        if not self.token or not self.repository:
            raise website_ops.ExecutionError("GitHub metadata autopush is not configured.")

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

    def get_file(self, path: str) -> tuple[str, str]:
        response = requests.get(
            f"{self.api_base}/repos/{self.repository}/contents/{path}",
            headers=self._headers(),
            params={"ref": self.branch},
            timeout=20,
        )
        if response.status_code != 200:
            raise website_ops.ExecutionError(
                f"GitHub source lookup failed ({response.status_code}) for {path}."
            )
        payload = response.json()
        return (
            base64.b64decode(str(payload.get("content", ""))).decode("utf-8"),
            str(payload.get("sha", "")),
        )

    def put_file(self, path: str, source: str, sha: str, message: str) -> dict[str, Any]:
        response = requests.put(
            f"{self.api_base}/repos/{self.repository}/contents/{path}",
            headers=self._headers(),
            json={
                "message": message,
                "content": base64.b64encode(source.encode("utf-8")).decode("ascii"),
                "sha": sha,
                "branch": self.branch,
            },
            timeout=30,
        )
        if response.status_code not in {200, 201}:
            raise website_ops.ExecutionError(
                f"GitHub metadata commit failed ({response.status_code}): {response.text[:240]}"
            )
        return dict(response.json())


def _live_metadata_matches(page_url: str, requested: Mapping[str, str], config: Any) -> bool:
    observation = website_ops.collect_page_observation(page_url, config=config)
    expected_title = str(requested.get("meta_title", "")).strip()
    expected_description = str(requested.get("meta_description", "")).strip()
    expected_canonical = str(requested.get("canonical_url", "")).strip().rstrip("/")
    if expected_title and str(observation.get("title", "")).strip() != expected_title:
        return False
    if expected_description and str(observation.get("meta_description", "")).strip() != expected_description:
        return False
    if expected_canonical and str(observation.get("canonical_url", "")).strip().rstrip("/") != expected_canonical:
        return False
    return True


def execute_github_metadata_action(
    record: Mapping[str, Any],
    *,
    config: Any,
    timestamp: datetime | None = None,
) -> dict[str, Any]:
    timestamp = timestamp or datetime.now(timezone.utc)
    requested = validate_metadata_action(record)
    page_url = str(record.get("page_url", "")).strip()
    source_path = route_source_path(page_url)
    client = GitHubWebsiteClient()
    before_source, before_sha = client.get_file(source_path)
    after_source = update_static_metadata_source(before_source, requested)
    if after_source == before_source:
        raise website_ops.ExecutionError("The requested metadata already matches repository source.")
    feedback_id = str(record.get("feedback_id", "") or "unknown")
    commit = client.put_file(
        source_path,
        after_source,
        before_sha,
        f"SEO: update metadata for {urlparse(page_url).path or '/'} ({feedback_id})",
    )
    commit_sha = str((commit.get("commit") or {}).get("sha", ""))

    timeout_seconds = max(30, int(os.getenv("WEBSITE_OPS_DEPLOY_VERIFY_TIMEOUT_SECONDS", "300")))
    poll_seconds = max(5, int(os.getenv("WEBSITE_OPS_DEPLOY_VERIFY_POLL_SECONDS", "15")))
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            if _live_metadata_matches(page_url, requested, config):
                return {
                    "feedback_id": feedback_id,
                    "action_type": str(record.get("action_type", "")),
                    "page_url": page_url,
                    "source_path": source_path,
                    "repository": client.repository,
                    "branch": client.branch,
                    "commit_sha": commit_sha,
                    "executed_at": timestamp.isoformat(),
                    "verification_status": "verified",
                    "summary": {
                        "before_source_sha": before_sha,
                        "commit_sha": commit_sha,
                        "validated_reason": str(
                            record.get("reason")
                            or record.get("details")
                            or record.get("execution_reason")
                            or ""
                        ).strip(),
                        "evidence": list(record.get("evidence") or []),
                        "requested": requested,
                    },
                }
        except Exception:  # noqa: BLE001 - deployment can be briefly unavailable
            pass
        time.sleep(poll_seconds)

    try:
        current_source, current_sha = client.get_file(source_path)
        if current_source == after_source:
            client.put_file(
                source_path,
                before_source,
                current_sha,
                f"Rollback SEO metadata for {urlparse(page_url).path or '/'} ({feedback_id})",
            )
    except Exception:  # noqa: BLE001 - preserve the primary verification failure
        pass
    raise website_ops.ExecutionError(
        "Production metadata verification timed out; an automatic rollback was attempted."
    )
