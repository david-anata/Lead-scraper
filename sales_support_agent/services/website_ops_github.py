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
from sales_support_agent.services.website_ops_editorial_quality import (
    contextual_evidence_errors,
    repair_deterministic_article_defects,
)

from sales_support_agent.services import website_ops_vendor as website_ops


METADATA_ACTION_TYPES = {
    "meta_update",
    "meta_title_update",
    "meta_description_update",
    "canonical_update",
}
CONTENT_ACTION_TYPES = {"publish_blog_article"}
OFFICIAL_ARTICLE_SOURCE_DOMAINS = {
    "amazon.com",
    "census.gov",
    "dhl.com",
    "ebay.com",
    "fedex.com",
    "ftc.gov",
    "google.com",
    "irs.gov",
    "sba.gov",
    "shopify.com",
    "tiktok.com",
    "ups.com",
    "usps.com",
    "walmart.com",
}


def _official_article_source(hostname: str) -> bool:
    normalized = hostname.lower().strip(".").removeprefix("www.")
    return any(
        normalized == domain or normalized.endswith(f".{domain}")
        for domain in OFFICIAL_ARTICLE_SOURCE_DOMAINS
    )
GENERATED_ARTICLE_REGISTRY = "src/content/generated-articles/index.ts"
EXCLUDED_PATH_PREFIXES = ("/api/", "/book", "/brand", "/preview", "/x/")


def generated_article_identities(source: str) -> dict[str, set[str]]:
    """Read durable article identity from the generated registry source."""

    marker = "// WEBSITE_OPS_GENERATED_ARTICLES_START"
    end_marker = "// WEBSITE_OPS_GENERATED_ARTICLES_END"
    marker_index = source.find(marker)
    end = source.find(end_marker, marker_index)
    if marker_index < 0 or end < 0:
        raise website_ops.ExecutionError("Generated article registry markers are missing.")
    registry = source[marker_index:end]

    def values(field: str, *, normalize: bool = False) -> set[str]:
        extracted = {
            match.group(1).strip()
            for match in re.finditer(
                rf'["\']{re.escape(field)}["\']\s*:\s*["\']([^"\']+)["\']',
                registry,
            )
            if match.group(1).strip()
        }
        return {value.casefold() for value in extracted} if normalize else extracted

    return {
        "evidence_ids": values("evidenceId"),
        "primary_intents": values("primaryIntent", normalize=True),
        "slugs": values("slug"),
    }


def generated_article_records(source: str) -> list[dict[str, Any]]:
    """Parse the durable generated-article registry without executing TypeScript."""

    marker = "// WEBSITE_OPS_GENERATED_ARTICLES_START"
    end_marker = "// WEBSITE_OPS_GENERATED_ARTICLES_END"
    marker_index = source.find(marker)
    end = source.find(end_marker, marker_index)
    if marker_index < 0 or end < 0:
        raise website_ops.ExecutionError("Generated article registry markers are missing.")
    block = source[marker_index + len(marker) : end]
    match = re.search(
        r"export const GENERATED_ARTICLES: readonly GeneratedArticle\[\] = (?P<data>\[[\s\S]*\]);",
        block,
    )
    if not match:
        raise website_ops.ExecutionError("Generated article registry could not be parsed.")
    try:
        articles = json.loads(match.group("data"))
    except json.JSONDecodeError as exc:
        raise website_ops.ExecutionError(
            "Generated article registry contains invalid JSON."
        ) from exc
    if not isinstance(articles, list) or not all(
        isinstance(item, dict) for item in articles
    ):
        raise website_ops.ExecutionError(
            "Generated article registry must contain article objects."
        )
    return articles


def load_generated_article_identities() -> dict[str, set[str]]:
    source, _ = GitHubWebsiteClient().get_file(GENERATED_ARTICLE_REGISTRY)
    return generated_article_identities(source)


def github_metadata_is_configured() -> bool:
    return bool(
        os.getenv("WEBSITE_OPS_GITHUB_TOKEN", "").strip()
        and os.getenv("WEBSITE_OPS_GITHUB_REPOSITORY", "david-anata/anata-website").strip()
    )


def validate_generated_article(record: Mapping[str, Any]) -> dict[str, Any]:
    """Validate a source-backed article payload before it can reach GitHub."""

    raw = str(record.get("action_value") or record.get("suggested_action_value") or "").strip()
    try:
        article = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise website_ops.ExecutionError("Generated article payload must be valid JSON.") from exc
    if not isinstance(article, dict):
        raise website_ops.ExecutionError("Generated article payload must be an object.")
    required = (
        "slug",
        "primaryIntent",
        "evidenceId",
        "generatedAt",
        "publishedAt",
        "modifiedAt",
        "author",
        "title",
        "description",
        "content",
        "sources",
    )
    missing = [key for key in required if not article.get(key)]
    if missing:
        raise website_ops.ExecutionError(
            "Generated article is missing required fields: " + ", ".join(missing) + "."
        )
    slug = str(article["slug"]).strip()
    if not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", slug):
        raise website_ops.ExecutionError("Generated article slug is invalid.")
    timestamps: dict[str, datetime] = {}
    for field in ("generatedAt", "publishedAt", "modifiedAt"):
        try:
            timestamps[field] = datetime.fromisoformat(
                str(article[field]).strip().replace("Z", "+00:00")
            )
        except (TypeError, ValueError) as exc:
            raise website_ops.ExecutionError(
                f"Generated article {field} must be an ISO 8601 timestamp."
            ) from exc
        if timestamps[field].tzinfo is None:
            raise website_ops.ExecutionError(
                f"Generated article {field} must include a timezone."
            )
    if timestamps["modifiedAt"] < timestamps["publishedAt"]:
        raise website_ops.ExecutionError(
            "Generated article modifiedAt cannot precede publishedAt."
        )
    author = article.get("author")
    expected_author = {
        "type": "Organization",
        "name": "Anata Inc.",
        "url": "https://anatainc.com",
    }
    if author != expected_author:
        raise website_ops.ExecutionError(
            "Generated articles must use the verified Anata Inc. organization author."
        )
    title = str(article["title"]).strip()
    description = str(article["description"]).strip()
    if not 15 <= len(title) <= 65:
        raise website_ops.ExecutionError("Generated article title must be 15 to 65 characters.")
    if not 50 <= len(description) <= 155:
        raise website_ops.ExecutionError(
            "Generated article description must be 50 to 155 characters."
        )
    serialized = json.dumps(article, ensure_ascii=False)
    if "—" in serialized:
        raise website_ops.ExecutionError("Generated article contains a prohibited em dash.")
    blocked_claims = ("basic research", "reveal the gap", "app.anatainc.com/demo")
    slop_phrases = (
        "in today's fast-paced world",
        "game-changer",
        "unlock the power",
        "delve into",
        "ever-evolving landscape",
        "seamlessly",
        "robust solution",
    )
    lowered = serialized.lower()
    if any(value in lowered for value in blocked_claims):
        raise website_ops.ExecutionError("Generated article contains prohibited content.")
    if any(value in lowered for value in slop_phrases):
        raise website_ops.ExecutionError("Generated article contains prohibited generic filler.")
    sources = article.get("sources")
    if not isinstance(sources, list) or len(sources) < 2:
        raise website_ops.ExecutionError("Generated article requires at least two sources.")
    source_domains: set[str] = set()
    for source in sources:
        if not isinstance(source, Mapping):
            raise website_ops.ExecutionError("Generated article source is invalid.")
        parsed = urlparse(str(source.get("url", "")).strip())
        if parsed.scheme != "https" or not parsed.hostname or not str(source.get("title", "")).strip():
            raise website_ops.ExecutionError(
                "Every generated article source needs an HTTPS URL and title."
            )
        hostname = parsed.hostname.removeprefix("www.")
        if hostname == "anatainc.com":
            raise website_ops.ExecutionError(
                "External article sources cannot use anatainc.com."
            )
        if not _official_article_source(hostname):
            raise website_ops.ExecutionError(
                "Generated article sources must be first-party platform, carrier, or government documentation."
            )
        source_domains.add(hostname)
    content = article.get("content")
    if not isinstance(content, Mapping):
        raise website_ops.ExecutionError("Generated article content is invalid.")
    content_text = json.dumps(content, ensure_ascii=False).lower()
    platform_source_requirements = {
        "amazon": "amazon.com",
        "tiktok": "tiktok.com",
        "shopify": "shopify.com",
        "walmart": "walmart.com",
        "ebay": "ebay.com",
        "fedex": "fedex.com",
        "usps": "usps.com",
        "dhl": "dhl.com",
    }
    for platform, required_domain in platform_source_requirements.items():
        if re.search(rf"\b{re.escape(platform)}\b", content_text) and not any(
            hostname == required_domain
            or hostname.endswith(f".{required_domain}")
            for hostname in source_domains
        ):
            raise website_ops.ExecutionError(
                f"Generated article discusses {platform.title()} without an official {required_domain} source."
            )
    expected_route = f"/blog/{slug}"
    if content.get("route") != expected_route or content.get("schemaType") != "article":
        raise website_ops.ExecutionError(
            "Generated article route and Article schema must match its slug."
        )
    if str(content.get("h1", "")).strip() != str(content.get("articleTitle", "")).strip():
        raise website_ops.ExecutionError("Generated article H1 and Article title must agree.")
    sections = content.get("sections")
    if not isinstance(sections, list) or len(sections) < 4:
        raise website_ops.ExecutionError("Generated article requires at least four sections.")
    if not all(
        isinstance(section, Mapping)
        and str(section.get("heading", "")).strip()
        and isinstance(section.get("paragraphs"), list)
        and len(section.get("paragraphs")) >= 2
        for section in sections
    ):
        raise website_ops.ExecutionError(
            "Every generated article section needs a heading and at least two paragraphs."
        )
    contextual_errors = contextual_evidence_errors(
        sections=sections,
        sources=sources,
    )
    if contextual_errors:
        raise website_ops.ExecutionError(contextual_errors[0])
    paragraphs = [
        str(paragraph).strip()
        for section in sections
        for paragraph in section.get("paragraphs", [])
        if str(paragraph).strip()
    ]
    if any(re.search(r"\w;\w", paragraph) for paragraph in paragraphs):
        raise website_ops.ExecutionError(
            "Generated article contains malformed punctuation joins."
        )
    normalized_paragraphs = {
        re.sub(r"\s+", " ", paragraph).lower() for paragraph in paragraphs
    }
    if len(normalized_paragraphs) != len(paragraphs):
        raise website_ops.ExecutionError(
            "Generated article repeats a paragraph."
        )
    tldr = content.get("tldr")
    if not isinstance(tldr, Mapping):
        raise website_ops.ExecutionError("Generated article requires a direct-answer summary.")
    tldr_answer = tldr.get("answer")
    tldr_text = " ".join(
        str(value).strip()
        for value in (
            tldr_answer if isinstance(tldr_answer, list) else [tldr_answer]
        )
        if str(value or "").strip()
    )
    article_text = " ".join([tldr_text, *paragraphs])
    word_count = len(re.findall(r"\b[\w'-]+\b", article_text))
    if not 60 <= len(re.findall(r"\b[\w'-]+\b", tldr_text)) <= 160:
        raise website_ops.ExecutionError(
            "Generated article direct answer must be 60 to 160 words."
        )
    if word_count < 900:
        raise website_ops.ExecutionError(
            "Generated article must contain at least 900 useful words."
        )
    related = content.get("related")
    if not isinstance(related, list) or len(related) < 2:
        raise website_ops.ExecutionError(
            "Generated article requires at least two internal links."
        )
    internal_hrefs = {
        str(item.get("href", "")).strip()
        for item in related
        if isinstance(item, Mapping)
        and str(item.get("title", "")).strip()
        and str(item.get("href", "")).strip().startswith("/")
        and not str(item.get("href", "")).strip().startswith("//")
    }
    if len(internal_hrefs) < 2:
        raise website_ops.ExecutionError(
            "Generated article requires two distinct titled internal links."
        )
    breadcrumbs = content.get("breadcrumbs")
    if not isinstance(breadcrumbs, list) or len(breadcrumbs) < 3:
        raise website_ops.ExecutionError("Generated article requires a breadcrumb path.")
    reason = str(
        record.get("reason")
        or record.get("details")
        or record.get("execution_reason")
        or ""
    ).strip()
    evidence = list(record.get("evidence") or [])
    if str(record.get("confidence", "")).strip().lower() != "high" or not reason or len(evidence) < 2:
        raise website_ops.ExecutionError(
            "Article publishing requires high confidence, a reason, and independent evidence."
        )
    return article


def update_generated_article_registry(source: str, article: Mapping[str, Any]) -> str:
    start_marker = "// WEBSITE_OPS_GENERATED_ARTICLES_START"
    end_marker = "// WEBSITE_OPS_GENERATED_ARTICLES_END"
    start = source.find(start_marker)
    end = source.find(end_marker)
    if start < 0 or end < 0 or end <= start:
        raise website_ops.ExecutionError("Generated article registry markers are missing.")
    articles = generated_article_records(source)
    slug = str(article.get("slug", ""))
    if any(str(item.get("slug", "")) == slug for item in articles if isinstance(item, Mapping)):
        raise website_ops.ExecutionError("Generated article slug already exists.")
    primary_intent = str(article.get("primaryIntent", "")).strip().lower()
    if any(
        str(item.get("primaryIntent", "")).strip().lower() == primary_intent
        for item in articles
        if isinstance(item, Mapping)
    ):
        raise website_ops.ExecutionError("Generated article primary intent already has an owner.")
    articles.append(dict(article))
    replacement = (
        "\nexport const GENERATED_ARTICLES: readonly GeneratedArticle[] = "
        + json.dumps(articles, ensure_ascii=False, indent=2)
        + ";\n"
    )
    return source[: start + len(start_marker)] + replacement + source[end:]


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


def execute_github_article_action(
    record: Mapping[str, Any],
    *,
    config: Any,
    timestamp: datetime | None = None,
) -> dict[str, Any]:
    timestamp = timestamp or datetime.now(timezone.utc)
    try:
        article = validate_generated_article(record)
        deterministic_repairs: list[str] = []
    except website_ops.ExecutionError as original_error:
        try:
            raw_article = json.loads(str(record.get("action_value", "") or ""))
        except json.JSONDecodeError:
            raise original_error
        repaired_article, deterministic_repairs = repair_deterministic_article_defects(
            raw_article if isinstance(raw_article, Mapping) else {}
        )
        if not deterministic_repairs:
            raise original_error
        repaired_record = {
            **dict(record),
            "action_value": json.dumps(repaired_article, ensure_ascii=False),
        }
        article = validate_generated_article(repaired_record)
    client = GitHubWebsiteClient()
    before_source, before_sha = client.get_file(GENERATED_ARTICLE_REGISTRY)
    feedback_id = str(record.get("feedback_id", "") or "unknown")
    existing_article = next(
        (
            item
            for item in generated_article_records(before_source)
            if str(item.get("slug", "")) == str(article["slug"])
        ),
        None,
    )
    reconciled_existing = existing_article is not None
    if existing_article is not None:
        if existing_article != article:
            raise website_ops.ExecutionError(
                "Generated article slug already exists with a different payload."
            )
        commit_sha = ""
    else:
        after_source = update_generated_article_registry(before_source, article)
        commit = client.put_file(
            GENERATED_ARTICLE_REGISTRY,
            after_source,
            before_sha,
            f"SEO: publish {article['slug']} ({feedback_id})",
        )
        commit_sha = str((commit.get("commit") or {}).get("sha", ""))
    page_url = f"https://anatainc.com/blog/{article['slug']}"
    # Vercel production promotion can legitimately take longer than five minutes,
    # especially while another website deployment is already building.  A short
    # timeout previously caused Agent to commit an article, observe it live just
    # after the deadline, and then delete it again as part of an automatic
    # rollback.  Publication latency is not evidence that the source change is
    # unsafe, so keep the durable commit and allow a full deployment window.
    timeout_seconds = max(
        900,
        int(os.getenv("WEBSITE_OPS_DEPLOY_VERIFY_TIMEOUT_SECONDS", "900")),
    )
    poll_seconds = max(
        5,
        int(os.getenv("WEBSITE_OPS_DEPLOY_VERIFY_POLL_SECONDS", "15")),
    )
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            observation = website_ops.collect_page_observation(page_url, config=config)
            observed_title = str(observation.get("title", "")).strip()
            expected_title = str(article["title"]).strip()
            if (
                int(observation.get("status_code", 0) or 0) == 200
                and observed_title in {expected_title, f"{expected_title} | Anata"}
                and str(observation.get("canonical_url", "")).rstrip("/") == page_url
                and str(article["content"]["h1"]).strip()
                in [str(value).strip() for value in observation.get("h1", []) or []]
            ):
                return {
                    "feedback_id": feedback_id,
                    "action_type": "publish_blog_article",
                    "page_url": page_url,
                    "source_path": GENERATED_ARTICLE_REGISTRY,
                    "repository": client.repository,
                    "branch": client.branch,
                    "commit_sha": commit_sha,
                    "production_url": page_url,
                    "executed_at": timestamp.isoformat(),
                    "verification_status": "verified",
                    "summary": {
                        "before_source_sha": before_sha,
                        "commit_sha": commit_sha,
                        "evidence_id": article["evidenceId"],
                        "primary_intent": article["primaryIntent"],
                        "source_count": len(article["sources"]),
                        "deterministic_repairs": deterministic_repairs,
                        "reconciled_existing": reconciled_existing,
                    },
                }
        except Exception:  # noqa: BLE001 - deployment can be briefly unavailable
            pass
        time.sleep(poll_seconds)
    raise website_ops.ExecutionError(
        "Production article verification timed out; the durable publication commit was preserved for reconciliation."
    )
