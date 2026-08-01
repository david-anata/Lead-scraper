"""Analytics-driven autonomy layer for Website Ops."""

from __future__ import annotations

import json
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import quote, urlparse

import requests
from sales_support_agent.services import website_ops_vendor
from sales_support_agent.services.website_ops_content import (
    build_faq_payload,
    build_section_expansion_payload,
    save_content_tasks,
)
from sales_support_agent.services.website_ops_customer_language import collect_customer_questions
from sales_support_agent.services.website_ops_serp import build_blueprint
from sales_support_agent.services.website_ops_aeo import build_aeo_assessment
from sales_support_agent.services.website_ops_article_engine import (
    article_batch_size,
    article_generation_progress,
    build_article_action,
)
from sales_support_agent.services.website_ops_content_strategy import (
    build_content_strategy,
    persist_content_strategy,
)
from sales_support_agent.services.website_ops_query_intelligence import (
    build_query_intelligence,
)
from sales_support_agent.services.website_ops_program import (
    build_indexing_inventory,
    load_indexing_inventory,
    reconcile_indexing_inventory,
)

try:
    from google.auth.transport.requests import Request as GoogleAuthRequest
    from google.oauth2.service_account import Credentials as ServiceAccountCredentials

    GOOGLE_AUTH_AVAILABLE = True
except ModuleNotFoundError:  # pragma: no cover - environment dependent
    GOOGLE_AUTH_AVAILABLE = False
    GoogleAuthRequest = None
    ServiceAccountCredentials = None


SEARCH_CONSOLE_SCOPE = "https://www.googleapis.com/auth/webmasters.readonly"
SEARCH_CONSOLE_WRITE_SCOPE = "https://www.googleapis.com/auth/webmasters"
GA4_SCOPE = "https://www.googleapis.com/auth/analytics.readonly"
MVP_MODE_ACTIVE = True
MVP_ALLOWED_ACTION_TYPES = (
    "inject_faq_block",
    "expand_service_page_section",
    "meta_update",
    "meta_title_update",
    "meta_description_update",
    "canonical_update",
    "publish_blog_article",
)
MVP_SUGGESTION_ONLY_ACTION_TYPES = {"inject_faq_block", "expand_service_page_section"}
MVP_FAQ_IMPRESSIONS_THRESHOLD = 25.0
MVP_FAQ_CTR_THRESHOLD = 0.03
MVP_FAQ_FORCE_IMPRESSIONS_THRESHOLD = 100.0
MVP_FAQ_FORCE_CTR_THRESHOLD = 0.015
MVP_THIN_TEXT_THRESHOLD = 5000


@dataclass(frozen=True)
class AnalyticsConfig:
    service_account_json: str
    search_console_property: str
    ga4_property_id: str
    lookback_days: int
    primary_lead_event: str


def _setting(settings: Any, name: str, env_name: str, default: str = "") -> str:
    value = getattr(settings, name, "")
    if isinstance(value, str) and value.strip():
        return value.strip()
    return os.getenv(env_name, default).strip()


def _load_service_account_info(raw: str) -> dict[str, Any]:
    if not raw:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON is missing.")
    if raw.lstrip().startswith("{"):
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            # Render's multiline editor can preserve a credential copied from a
            # JavaScript object with unquoted keys or literal private-key line
            # breaks. Normalize that data-only shape without ever evaluating it.
            normalized = re.sub(
                r'(?P<prefix>[{,]\s*)(?P<key>[A-Za-z_][A-Za-z0-9_]*)(?P<suffix>\s*:)',
                r'\g<prefix>"\g<key>"\g<suffix>',
                raw,
            )
            normalized = re.sub(r",(\s*[}\]])", r"\1", normalized)
            payload = json.loads(normalized, strict=False)
    else:
        payload = json.loads(Path(raw).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON must resolve to an object.")
    required_fields = ("client_email", "private_key", "token_uri")
    missing_fields = [field for field in required_fields if not str(payload.get(field, "")).strip()]
    if missing_fields:
        raise RuntimeError(
            "GOOGLE_SERVICE_ACCOUNT_JSON is missing required fields: "
            + ", ".join(missing_fields)
            + "."
        )
    return payload


def analytics_configuration_status(settings: Any) -> dict[str, Any]:
    """Return secret-free readiness for ranking-decision data sources."""

    config = analytics_config_from_settings(settings)
    checks = {
        "google_service_account": False,
        "search_console_property": bool(config.search_console_property),
        "ga4_property": bool(config.ga4_property_id),
    }
    blockers: list[dict[str, str]] = []
    try:
        _load_service_account_info(config.service_account_json)
        checks["google_service_account"] = True
    except Exception as exc:  # noqa: BLE001 - converted to a safe operator message
        blockers.append(
            {
                "code": "GOOGLE_SERVICE_ACCOUNT_INVALID",
                "source": "google",
                "message": f"Google service-account configuration is invalid: {exc}",
            }
        )
    if not checks["search_console_property"]:
        blockers.append(
            {
                "code": "GSC_PROPERTY_MISSING",
                "source": "google_search_console",
                "message": "Search Console property is not configured.",
            }
        )
    if not checks["ga4_property"]:
        blockers.append(
            {
                "code": "GA4_PROPERTY_MISSING",
                "source": "google_analytics_4",
                "message": "GA4 property is not configured.",
            }
        )
    return {
        "status": "ready" if all(checks.values()) else "blocked",
        "checks": checks,
        "blockers": blockers,
    }


def _service_account_project_name(raw: str) -> str:
    if not raw:
        return "the configured project"
    try:
        return str(_load_service_account_info(raw).get("project_id") or "the configured project")
    except Exception:
        return "the configured project"


def _google_access_token(raw_service_account_json: str, scopes: list[str]) -> str:
    if not GOOGLE_AUTH_AVAILABLE:
        raise RuntimeError("google-auth is not installed.")
    credentials = ServiceAccountCredentials.from_service_account_info(
        _load_service_account_info(raw_service_account_json),
        scopes=scopes,
    )
    credentials.refresh(GoogleAuthRequest())
    token = str(credentials.token or "").strip()
    if not token:
        raise RuntimeError("Google auth did not return an access token.")
    return token


def _normalize_url(value: str) -> str:
    return value.strip().rstrip("/")


def _path_from_url(value: str) -> str:
    parsed = urlparse(value)
    path = parsed.path or "/"
    if not path.startswith("/"):
        path = "/" + path
    return path.rstrip("/") or "/"


def _humanize_slug(value: str) -> str:
    cleaned = re.sub(r"[_\-]+", " ", str(value or "").strip()).strip()
    return cleaned.title() if cleaned else ""


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().lower()).strip("-")
    return slug or "item"


def _website_ops_root(settings: Any) -> Path:
    root = Path(getattr(settings, "website_ops_root"))
    root.mkdir(parents=True, exist_ok=True)
    return root


def _save_blueprints(settings: Any, blueprints: list[Mapping[str, Any]]) -> None:
    root = _website_ops_root(settings) / "serp_blueprints"
    root.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": date.today().isoformat(),
        "blueprints": list(blueprints),
    }
    dated_path = root / f"serp_blueprints_{date.today().isoformat()}.json"
    latest_path = root / "latest.json"
    text = json.dumps(payload, indent=2, sort_keys=True)
    dated_path.write_text(text, encoding="utf-8")
    latest_path.write_text(text, encoding="utf-8")


def _page_service_slug(page: Mapping[str, Any]) -> str:
    parsed = urlparse(str(page.get("url", "")).strip())
    parts = [item for item in parsed.path.split("/") if item]
    if "services" in parts:
        try:
            index = parts.index("services")
            return parts[index + 1] if len(parts) > index + 1 else ""
        except ValueError:
            return ""
    return parts[-1] if parts else ""


def _matching_customer_questions(
    page: Mapping[str, Any],
    customer_questions: list[Mapping[str, Any]],
    top_queries: list[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    service_slug = _page_service_slug(page)
    title = str(page.get("title", "")).lower()
    query_terms = " ".join(str(item.get("query", "")).lower() for item in top_queries[:3]).strip()
    matches: list[dict[str, Any]] = []
    for item in customer_questions:
        related_service = str(item.get("related_service", "")).strip().lower()
        question = str(item.get("question", "")).lower()
        if related_service and related_service in {service_slug.lower(), title}:
            matches.append(dict(item))
            continue
        if service_slug and service_slug.lower().replace("-", " ") in question:
            matches.append(dict(item))
            continue
        if query_terms and any(token for token in query_terms.split() if len(token) > 3 and token in question):
            matches.append(dict(item))
    matches.sort(key=lambda item: (-int(item.get("frequency", 0) or 0), str(item.get("question", ""))))
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in matches:
        key = str(item.get("question_id", "") or item.get("question", "")).strip()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped[:4]


def _blueprint_missing_faq(blueprint: Mapping[str, Any]) -> bool:
    if list(blueprint.get("faq_patterns") or []):
        return True
    return any("faq" in str(item).lower() for item in (blueprint.get("content_gaps") or []))


def _page_has_faq_coverage(page: Mapping[str, Any]) -> bool:
    headings = []
    for key in ("h1", "h2", "h3"):
        headings.extend(str(item or "") for item in (page.get(key) or []))
    headings.extend(
        str(item.get("text", "") if isinstance(item, Mapping) else item or "")
        for item in (page.get("heading_structure") or [])
    )
    haystack = " ".join(headings).lower()
    return "faq" in haystack or "frequently asked" in haystack


def _page_thin_for_section(page: Mapping[str, Any], blueprint: Mapping[str, Any], matched_questions: list[Mapping[str, Any]]) -> bool:
    text_length = int(page.get("text_length", 0) or 0)
    gap_count = len(list(blueprint.get("content_gaps") or []))
    if text_length and text_length < MVP_THIN_TEXT_THRESHOLD:
        return True
    if gap_count >= 2:
        return True
    return bool(gap_count and matched_questions and text_length < (MVP_THIN_TEXT_THRESHOLD * 1.5))


def _content_task_action(
    *,
    page: Mapping[str, Any],
    gsc: Mapping[str, Any],
    ga4: Mapping[str, Any],
    primary_lead_event: str,
    action_type: str,
    section_name: str,
    before_state: str,
    after_state: str,
    reason: str,
    confidence: str,
    action_payload: Mapping[str, Any],
    confidence_basis: list[str],
    evidence: list[str],
    execution_eligibility: str,
    target_region: str,
    verification_requirements: list[str],
) -> dict[str, Any]:
    action = _base_action(
        page=page,
        gsc=gsc,
        ga4=ga4,
        action_type=action_type,
        section_name=section_name,
        before_state=before_state,
        after_state=after_state,
        reason=reason,
        insight_source="SERP + Customer Language",
        confidence=confidence,
        action_payload=action_payload,
        primary_lead_event=primary_lead_event,
        confidence_basis=confidence_basis,
    )
    action["evidence"] = evidence
    action["execution_eligibility"] = execution_eligibility
    action["requires_approval"] = execution_eligibility != "auto_execute"
    action["target_region"] = target_region
    action["verification_requirements"] = verification_requirements
    return action


def _service_focus(page: Mapping[str, Any], gsc: Mapping[str, Any]) -> str:
    top_queries = list(gsc.get("top_queries") or [])
    if top_queries:
        top_query = str(top_queries[0].get("query", "")).strip()
        if top_query:
            return top_query.title()
    title = str(page.get("title", "") or "").strip()
    if title:
        return title
    return _humanize_slug((_path_from_url(str(page.get("url", ""))) or "/").split("/")[-1])


def _is_branded_query(query: str) -> bool:
    tokens = re.findall(r"[a-z0-9]+", str(query).lower())
    if not tokens:
        return False
    if "anatainc" in tokens:
        return True
    return tokens[0] == "anata" and len(tokens) <= 3


def _content_route_is_eligible(page: Mapping[str, Any]) -> bool:
    path = urlparse(str(page.get("url", "")).strip()).path.rstrip("/").lower()
    return any(path.startswith(prefix) for prefix in ("/services/", "/guides/", "/glossary/", "/blog/"))


def _observed_question_queries(top_queries: list[Mapping[str, Any]]) -> list[str]:
    question_terms = {
        "how", "what", "why", "when", "which", "who", "can", "does", "do",
        "is", "are", "should", "cost", "price", "pricing", "vs", "versus",
        "compare", "comparison",
    }
    matches: list[str] = []
    for item in top_queries:
        query = str(item.get("query", "")).strip()
        tokens = set(re.findall(r"[a-z0-9]+", query.lower()))
        if query and not _is_branded_query(query) and tokens.intersection(question_terms):
            matches.append(query)
    return matches


def _service_cluster_map(urls: list[str]) -> dict[str, list[str]]:
    normalized = [_normalize_url(url) for url in urls if str(url).strip()]
    services = [url for url in normalized if "/services/" in url and url.rstrip("/").split("/")[-1] != "services"]
    hub = next((url for url in normalized if url.rstrip("/").endswith("/services")), "")
    mapping: dict[str, list[str]] = {}
    for url in services:
        peers = [candidate for candidate in services if candidate != url][:3]
        ordered = [item for item in ([hub] if hub else []) + peers if item]
        mapping[url] = ordered[:3]
    return mapping


def _mvp_filter_actions(actions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    allowed = set(MVP_ALLOWED_ACTION_TYPES)
    return [item for item in actions if str(item.get("action_type", "")).strip() in allowed]


def analytics_config_from_settings(settings: Any) -> AnalyticsConfig:
    return AnalyticsConfig(
        service_account_json=_setting(settings, "google_service_account_json", "GOOGLE_SERVICE_ACCOUNT_JSON"),
        search_console_property=_setting(settings, "website_ops_gsc_property", "WEBSITE_OPS_GSC_PROPERTY", "sc-domain:anatainc.com"),
        ga4_property_id=_setting(settings, "website_ops_ga4_property_id", "WEBSITE_OPS_GA4_PROPERTY_ID", "372887830"),
        lookback_days=max(int(_setting(settings, "website_ops_lookback_days", "WEBSITE_OPS_LOOKBACK_DAYS", "28") or "28"), 7),
        primary_lead_event=_setting(settings, "website_ops_ga4_primary_lead_event", "WEBSITE_OPS_GA4_PRIMARY_LEAD_EVENT", "generate_lead"),
    )


def _service_account_identity(raw_service_account_json: str) -> dict[str, str]:
    if not raw_service_account_json:
        return {}
    try:
        info = _load_service_account_info(raw_service_account_json)
    except Exception:
        return {}
    return {
        "project_id": str(info.get("project_id", "") or "").strip(),
        "client_email": str(info.get("client_email", "") or "").strip(),
    }


def _service_disabled_note(service_name: str, project_name: str) -> str:
    return f"{service_name} API is disabled in Google Cloud project {project_name}. Enable it in Google Cloud, then rerun Website Ops."


def _response_text(response: requests.Response) -> str:
    try:
        return response.text
    except Exception:
        return ""


def _search_console_failure_note(response: requests.Response, property_name: str, project_name: str) -> str:
    status_code = response.status_code
    body = _response_text(response)
    if "SERVICE_DISABLED" in body or "accessNotConfigured" in body:
        return _service_disabled_note("Search Console", project_name)
    if status_code == 403:
        return (
            "Search Console access is blocked. Grant the Website Ops service account Full access "
            f"to {property_name or 'the verified property'}, or update WEBSITE_OPS_GSC_PROPERTY to the exact verified property."
        )
    if status_code == 404:
        return (
            "Search Console property was not found. Verify WEBSITE_OPS_GSC_PROPERTY matches the verified domain or URL-prefix property."
        )
    return f"Search Console request failed ({status_code})."


def _ga4_failure_note(response: requests.Response, property_id: str, project_name: str) -> str:
    status_code = response.status_code
    body = _response_text(response)
    if "SERVICE_DISABLED" in body or "accessNotConfigured" in body:
        return _service_disabled_note("Google Analytics Data", project_name)
    if status_code == 403:
        return (
            "GA4 access is blocked. Grant the Website Ops service account access to the configured GA4 property "
            f"({property_id or 'missing property id'})."
        )
    if status_code == 404:
        return (
            "GA4 property was not found. Verify WEBSITE_OPS_GA4_PROPERTY_ID matches the numeric property ID in Google Analytics."
        )
    return f"GA4 request failed ({status_code})."


def fetch_search_console_snapshot(settings: Any, urls: list[str]) -> tuple[dict[str, dict[str, Any]], list[str]]:
    config = analytics_config_from_settings(settings)
    project_name = _service_account_project_name(config.service_account_json)
    if not config.service_account_json:
        return {}, ["Search Console unavailable: GOOGLE_SERVICE_ACCOUNT_JSON is not configured."]
    if not config.search_console_property:
        return {}, ["Search Console unavailable: WEBSITE_OPS_GSC_PROPERTY is not configured."]

    try:
        token = _google_access_token(config.service_account_json, [SEARCH_CONSOLE_SCOPE])
    except Exception as exc:  # pragma: no cover - exercised via env in production
        return {}, [f"Search Console unavailable: {exc}"]

    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=config.lookback_days - 1)
    encoded_property = quote(config.search_console_property, safe="")
    base_url = f"https://searchconsole.googleapis.com/webmasters/v3/sites/{encoded_property}/searchAnalytics/query"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    page_response = requests.post(
        base_url,
        headers=headers,
        json={
            "startDate": start_date.isoformat(),
            "endDate": end_date.isoformat(),
            "dimensions": ["page"],
            "rowLimit": 250,
        },
        timeout=30,
    )
    if not page_response.ok:
        return {}, [_search_console_failure_note(page_response, config.search_console_property, str(project_name))]

    query_response = requests.post(
        base_url,
        headers=headers,
        json={
            "startDate": start_date.isoformat(),
            "endDate": end_date.isoformat(),
            "dimensions": ["page", "query"],
            "rowLimit": 250,
        },
        timeout=30,
    )
    query_rows = query_response.json().get("rows", []) if query_response.ok else []

    metrics_by_url: dict[str, dict[str, Any]] = {}
    monitored = {_normalize_url(url): url for url in urls}
    for row in page_response.json().get("rows", []):
        keys = row.get("keys") or []
        if not keys:
            continue
        page_url = _normalize_url(str(keys[0]))
        if page_url not in monitored:
            continue
        metrics_by_url[page_url] = {
            "impressions": float(row.get("impressions", 0) or 0),
            "clicks": float(row.get("clicks", 0) or 0),
            "ctr": float(row.get("ctr", 0) or 0),
            "position": float(row.get("position", 0) or 0),
            "top_queries": [],
        }

    for row in query_rows:
        keys = row.get("keys") or []
        if len(keys) < 2:
            continue
        page_url = _normalize_url(str(keys[0]))
        if page_url not in metrics_by_url:
            continue
        query = str(keys[1]).strip()
        if not query:
            continue
        metrics_by_url[page_url]["top_queries"].append(
            {
                "query": query,
                "clicks": float(row.get("clicks", 0) or 0),
                "impressions": float(row.get("impressions", 0) or 0),
            }
        )
    for value in metrics_by_url.values():
        value["top_queries"] = value["top_queries"][:3]
    return metrics_by_url, []


def inspect_search_console_indexing(
    settings: Any,
    urls: list[str],
    *,
    requester: Any = requests.post,
) -> tuple[dict[str, Any], list[str]]:
    """Inspect the canonical marketing inventory through Google's URL Inspection API."""

    config = analytics_config_from_settings(settings)
    if not config.service_account_json:
        return build_indexing_inventory([]), [
            "Search Console URL inspection unavailable: Google credentials are not configured."
        ]
    if not config.search_console_property:
        return build_indexing_inventory([]), [
            "Search Console URL inspection unavailable: the property is not configured."
        ]
    try:
        token = _google_access_token(
            config.service_account_json,
            [SEARCH_CONSOLE_SCOPE],
        )
    except Exception as exc:
        return build_indexing_inventory([]), [
            f"Search Console URL inspection unavailable: {exc}"
        ]

    endpoint = "https://searchconsole.googleapis.com/v1/urlInspection/index:inspect"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    scoped_urls = sorted(
        {
            _normalize_url(url)
            for url in urls
            if urlparse(str(url)).scheme == "https"
            and (urlparse(str(url)).hostname or "").removeprefix("www.")
            == "anatainc.com"
        }
    )

    def inspect(url: str) -> tuple[dict[str, Any] | None, str]:
        try:
            response = requester(
                endpoint,
                headers=headers,
                json={
                    "inspectionUrl": url,
                    "siteUrl": config.search_console_property,
                    "languageCode": "en-US",
                },
                timeout=12,
            )
        except Exception as exc:
            return None, f"{url}: {type(exc).__name__}"
        if not response.ok:
            return None, f"{url}: HTTP {response.status_code}"
        try:
            payload = response.json()
        except ValueError:
            return None, f"{url}: invalid JSON"
        result = dict(
            dict(payload.get("inspectionResult") or {}).get("indexStatusResult")
            or {}
        )
        coverage_state = str(result.get("coverageState", "") or "").strip()
        verdict = str(result.get("verdict", "") or "").strip()
        if not coverage_state:
            coverage_state = (
                "Submitted and indexed" if verdict.upper() == "PASS" else "Unspecified"
            )
        return {
            "url": url,
            "reason": coverage_state,
            "last_crawled": str(result.get("lastCrawlTime", "") or ""),
            "source": "Google Search Console URL Inspection API",
            "observed_at": datetime.now(timezone.utc).isoformat(),
            "verdict": verdict,
            "robots_txt_state": str(result.get("robotsTxtState", "") or ""),
            "indexing_state": str(result.get("indexingState", "") or ""),
            "page_fetch_state": str(result.get("pageFetchState", "") or ""),
            "google_canonical": str(result.get("googleCanonical", "") or ""),
            "user_canonical": str(result.get("userCanonical", "") or ""),
            "crawled_as": str(result.get("crawledAs", "") or ""),
        }, ""

    records: list[dict[str, Any]] = []
    failures: list[str] = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(inspect, url): url for url in scoped_urls}
        for future in as_completed(futures):
            record, failure = future.result()
            if record:
                records.append(record)
            if failure:
                failures.append(failure)

    inventory = build_indexing_inventory(records)
    inventory["inspection"] = {
        "attempted": len(scoped_urls),
        "succeeded": len(records),
        "failed": len(failures),
        "failure_samples": sorted(failures)[:5],
    }
    notes = (
        [
            f"Search Console URL inspection completed with {len(failures)} failed URL(s)."
        ]
        if failures
        else []
    )
    return inventory, notes


def save_search_console_indexing_inventory(
    settings: Any,
    inventory: Mapping[str, Any],
) -> None:
    directory = _website_ops_root(settings) / "indexing"
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "inventory.json").write_text(
        json.dumps(dict(inventory), indent=2, sort_keys=True),
        encoding="utf-8",
    )


def submit_search_console_sitemap(
    settings: Any,
    *,
    sitemap_url: str = "https://anatainc.com/sitemap.xml",
    requester: Any = requests.put,
) -> dict[str, Any]:
    """Submit the canonical production sitemap through the supported GSC API."""

    config = analytics_config_from_settings(settings)
    try:
        token = _google_access_token(
            config.service_account_json,
            [SEARCH_CONSOLE_WRITE_SCOPE],
        )
    except Exception as exc:
        return {
            "status": "failed",
            "sitemap_url": sitemap_url,
            "reason": f"Search Console sitemap authorization failed: {exc}",
        }
    property_name = quote(config.search_console_property, safe="")
    feed_path = quote(sitemap_url, safe="")
    endpoint = (
        "https://www.googleapis.com/webmasters/v3/sites/"
        f"{property_name}/sitemaps/{feed_path}"
    )
    try:
        response = requester(
            endpoint,
            headers={"Authorization": f"Bearer {token}"},
            timeout=20,
        )
    except Exception as exc:
        return {
            "status": "failed",
            "sitemap_url": sitemap_url,
            "reason": f"Search Console sitemap submission failed: {type(exc).__name__}",
        }
    if not response.ok:
        return {
            "status": "failed",
            "sitemap_url": sitemap_url,
            "http_status": response.status_code,
            "reason": _search_console_failure_note(
                response,
                config.search_console_property,
                _service_account_project_name(config.service_account_json),
            ),
        }
    return {
        "status": "submitted",
        "sitemap_url": sitemap_url,
        "submitted_at": datetime.now(timezone.utc).isoformat(),
    }


def refresh_retained_indexing_inventory(
    settings: Any,
    observations: list[Mapping[str, Any]],
) -> tuple[dict[str, Any] | None, list[str]]:
    """Reconcile the last Google snapshot during inexpensive daily sweeps."""

    inventory = load_indexing_inventory(Path(settings.website_ops_root))
    if not list(inventory.get("records") or []):
        return None, []
    refreshed = reconcile_indexing_inventory(inventory, observations)
    submission = dict(refreshed.get("sitemap_submission") or {})
    notes: list[str] = []
    if submission.get("status") != "submitted":
        submission = submit_search_console_sitemap(settings)
        refreshed["sitemap_submission"] = submission
        if submission.get("status") != "submitted":
            notes.append(
                str(
                    submission.get("reason")
                    or "Search Console sitemap submission failed."
                )
            )
    save_search_console_indexing_inventory(settings, refreshed)
    return refreshed, notes


def fetch_ga4_snapshot(settings: Any, urls: list[str]) -> tuple[dict[str, dict[str, Any]], list[str]]:
    config = analytics_config_from_settings(settings)
    project_name = _service_account_project_name(config.service_account_json)
    if not config.service_account_json:
        return {}, ["GA4 unavailable: GOOGLE_SERVICE_ACCOUNT_JSON is not configured."]
    if not config.ga4_property_id:
        return {}, ["GA4 property ID is missing. Set WEBSITE_OPS_GA4_PROPERTY_ID in the agent service environment."]

    try:
        token = _google_access_token(config.service_account_json, [GA4_SCOPE])
    except Exception as exc:  # pragma: no cover - exercised via env in production
        return {}, [f"GA4 unavailable: {exc}"]

    response = requests.post(
        f"https://analyticsdata.googleapis.com/v1beta/properties/{config.ga4_property_id}:runReport",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={
            "dateRanges": [{"startDate": f"{config.lookback_days}daysAgo", "endDate": "yesterday"}],
            "dimensions": [{"name": "landingPagePlusQueryString"}],
            "metrics": [{"name": "sessions"}, {"name": "engagedSessions"}],
            "limit": 250,
        },
        timeout=30,
    )
    if not response.ok:
        return {}, [_ga4_failure_note(response, config.ga4_property_id, str(project_name))]

    lead_response = requests.post(
        f"https://analyticsdata.googleapis.com/v1beta/properties/{config.ga4_property_id}:runReport",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={
            "dateRanges": [{"startDate": f"{config.lookback_days}daysAgo", "endDate": "yesterday"}],
            "dimensions": [{"name": "landingPagePlusQueryString"}],
            "metrics": [{"name": "eventCount"}],
            "dimensionFilter": {
                "filter": {
                    "fieldName": "eventName",
                    "stringFilter": {"matchType": "EXACT", "value": config.primary_lead_event},
                }
            },
            "limit": 250,
        },
        timeout=30,
    )
    if not lead_response.ok:
        return {}, [_ga4_failure_note(lead_response, config.ga4_property_id, str(project_name))]

    monitored_paths = {_path_from_url(url): _normalize_url(url) for url in urls}
    metrics_by_url: dict[str, dict[str, Any]] = {}
    for row in response.json().get("rows", []):
        dimensions = row.get("dimensionValues") or []
        metrics = row.get("metricValues") or []
        if not dimensions or len(metrics) < 2:
            continue
        landing = str(dimensions[0].get("value", "")).strip()
        path = _path_from_url(landing)
        if path not in monitored_paths:
            continue
        url = monitored_paths[path]
        sessions = float(metrics[0].get("value", 0) or 0)
        engaged_sessions = float(metrics[1].get("value", 0) or 0)
        metrics_by_url[url] = {
            "sessions": sessions,
            "engaged_sessions": engaged_sessions,
            "lead_conversions": 0.0,
            "lead_conversion_rate": 0.0,
            "primary_lead_event": config.primary_lead_event,
            "trust_status": "partial",
        }
    for row in lead_response.json().get("rows", []):
        dimensions = row.get("dimensionValues") or []
        metrics = row.get("metricValues") or []
        if not dimensions or not metrics:
            continue
        landing = str(dimensions[0].get("value", "")).strip()
        path = _path_from_url(landing)
        if path not in monitored_paths:
            continue
        url = monitored_paths[path]
        metrics_by_url.setdefault(
            url,
            {
                "sessions": 0.0,
                "engaged_sessions": 0.0,
                "lead_conversions": 0.0,
                "lead_conversion_rate": 0.0,
                "primary_lead_event": config.primary_lead_event,
                "trust_status": "partial",
            },
        )
        metrics_by_url[url]["lead_conversions"] = float(metrics[0].get("value", 0) or 0)
    for value in metrics_by_url.values():
        sessions = float(value.get("sessions", 0) or 0)
        lead_conversions = float(value.get("lead_conversions", 0) or 0)
        value["lead_conversion_rate"] = (lead_conversions / sessions) if sessions else 0.0
        value["trust_status"] = "trusted" if lead_conversions > 0 else ("partial" if sessions > 0 else "missing")
    return metrics_by_url, []


def _confidence_level(value: str) -> str:
    return value if value in {"high", "medium", "low"} else "medium"


def _expected_impact(action_type: str) -> str:
    impacts = {
        "replace_primary_heading": "Stronger topic clarity for organic ranking and AI extraction.",
        "rewrite_title_and_intro": "Higher SERP click-through rate from existing impressions.",
        "strengthen_primary_cta": "Higher lead conversion rate from existing traffic.",
        "resolve_canonical_route": "Clearer authority consolidation and less route confusion.",
        "update_faq_ai_extraction": "Broader query coverage, stronger AI extraction, and better service-page depth.",
        "add_internal_links": "Stronger authority flow into commercial pages and clearer topical relationships.",
    }
    return impacts.get(action_type, "Improves page performance against the current growth goal.")


def _lead_trust_status(ga4_metrics: Mapping[str, Mapping[str, Any]], ga4_notes: list[str]) -> str:
    if ga4_notes:
        return "missing"
    total_sessions = sum(float(item.get("sessions", 0) or 0) for item in ga4_metrics.values())
    total_leads = sum(float(item.get("lead_conversions", 0) or 0) for item in ga4_metrics.values())
    if total_leads > 0:
        return "trusted"
    if total_sessions > 0:
        return "partial"
    return "missing"


def _content_opportunity(page: Mapping[str, Any], gsc: Mapping[str, Any], ga4: Mapping[str, Any]) -> dict[str, Any]:
    impressions = float(gsc.get("impressions", 0) or 0)
    top_queries = list(gsc.get("top_queries") or [])
    sessions = float(ga4.get("sessions", 0) or 0)
    low_demand = impressions < 25 and sessions < 15
    weak_ai_ready = bool(top_queries) or low_demand
    return {
        "faq_gap": weak_ai_ready and not page.get("issues"),
        "internal_link_gap": bool(top_queries) and impressions < 60,
        "weak_ai_ready": weak_ai_ready,
    }


def _evidence_lines(page: Mapping[str, Any], gsc: Mapping[str, Any], ga4: Mapping[str, Any], *, primary_lead_event: str) -> list[str]:
    evidence = []
    impressions = int(float(gsc.get("impressions", 0) or 0))
    clicks = int(float(gsc.get("clicks", 0) or 0))
    ctr = float(gsc.get("ctr", 0) or 0)
    position = float(gsc.get("position", 0) or 0)
    sessions = int(float(ga4.get("sessions", 0) or 0))
    leads = int(float(ga4.get("lead_conversions", 0) or 0))
    lead_rate = float(ga4.get("lead_conversion_rate", 0) or 0)
    if impressions:
        evidence.append(f"Search Console: {impressions} impressions, {clicks} clicks, {ctr:.2%} CTR.")
    if position:
        evidence.append(f"Average position is {position:.1f}.")
    if sessions:
        evidence.append(f"GA4: {sessions} sessions, {leads} {primary_lead_event} events, {lead_rate:.2%} conversion rate.")
    top_queries = list(gsc.get("top_queries") or [])
    if top_queries:
        query = str(top_queries[0].get("query", "")).strip()
        if query:
            evidence.append(f"Top query signal: {query}.")
    if page.get("issues"):
        evidence.append(f"Structural issues present: {len(page.get('issues') or [])}.")
    return evidence


def _execution_envelope(action: Mapping[str, Any], *, page_url: str) -> dict[str, Any]:
    details = website_ops_vendor.execution_target_details(
        {
            "page_url": page_url,
            "action_type": action.get("action_type", ""),
            "suggested_action_type": action.get("action_type", ""),
            "suggested_action_value": action.get("action_value", ""),
        }
    )
    confidence = _confidence_level(str(action.get("confidence", "medium")))
    executable = confidence == "high" and bool(details.get("eligible"))
    return {
        "execution_eligibility": "auto_execute" if executable else "approval_required",
        "target_region": str(details.get("target_region", "") or ""),
        "verification_requirements": list(details.get("verification_requirements") or []),
        "execution_reason": str(details.get("reason", "") or ""),
        "requires_approval": not executable,
    }


def _base_action(
    *,
    page: Mapping[str, Any],
    gsc: Mapping[str, Any],
    ga4: Mapping[str, Any],
    action_type: str,
    section_name: str,
    before_state: str,
    after_state: str,
    reason: str,
    insight_source: str,
    confidence: str,
    action_payload: Mapping[str, Any],
    primary_lead_event: str,
    confidence_basis: list[str],
) -> dict[str, Any]:
    action = {
        "page_url": page.get("url", ""),
        "page_title": page.get("title", ""),
        "action_type": action_type,
        "section_name": section_name,
        "before_state": before_state,
        "after_state": after_state,
        "reason": reason,
        "insight_source": insight_source,
        "expected_impact": _expected_impact(action_type),
        "confidence": _confidence_level(confidence),
        "status": "recommended",
        "evidence": _evidence_lines(page, gsc, ga4, primary_lead_event=primary_lead_event),
        "confidence_basis": confidence_basis,
        "ga4_trust_status": str(ga4.get("trust_status", "missing") or "missing"),
        "action_value": json.dumps(action_payload, sort_keys=True),
    }
    action.update(_execution_envelope(action, page_url=str(page.get("url", ""))))
    return action


def _structural_action_from_issue(page: Mapping[str, Any], issue: Mapping[str, Any]) -> dict[str, Any]:
    code = str(issue.get("code", ""))
    h1 = list(page.get("h1") or [])
    canonical = str(page.get("canonical_url", "") or "")
    if code == "MULTIPLE_H1":
        action = {
            "page_url": page.get("url", ""),
            "page_title": page.get("title", ""),
            "action_type": "replace_primary_heading",
            "section_name": "Primary heading structure",
            "before_state": " | ".join(h1),
            "after_state": "Keep one topic-specific H1 and demote the rest to H2.",
            "reason": issue.get("summary", ""),
            "insight_source": "Structural audit",
            "expected_impact": _expected_impact("replace_primary_heading"),
            "confidence": "high",
            "evidence": [str(issue.get("summary", "")).strip()],
            "confidence_basis": ["Multiple H1s are deterministic structural debt."],
        }
        action.update(_execution_envelope(action, page_url=str(page.get("url", ""))))
        return action
    if code == "MISSING_H1":
        action = {
            "page_url": page.get("url", ""),
            "page_title": page.get("title", ""),
            "action_type": "replace_primary_heading",
            "section_name": "Hero heading",
            "before_state": "No H1 exposed",
            "after_state": "Promote the hero heading to a single H1.",
            "reason": issue.get("summary", ""),
            "insight_source": "Structural audit",
            "expected_impact": _expected_impact("replace_primary_heading"),
            "confidence": "high",
            "evidence": [str(issue.get("summary", "")).strip()],
            "confidence_basis": ["Missing H1 can be resolved deterministically."],
        }
        action.update(_execution_envelope(action, page_url=str(page.get("url", ""))))
        return action
    if code in {"CANONICAL_MISMATCH", "REDIRECTED_URL"}:
        return {
            "page_url": page.get("url", ""),
            "page_title": page.get("title", ""),
            "action_type": "resolve_canonical_route",
            "section_name": "Route / canonical",
            "before_state": canonical or str(page.get("final_url", "") or ""),
            "after_state": "Align monitored route, redirect target, and canonical URL to one standard path.",
            "reason": issue.get("summary", ""),
            "insight_source": "Structural audit",
            "expected_impact": _expected_impact("resolve_canonical_route"),
            "confidence": "medium",
            "evidence": [str(issue.get("summary", "")).strip()],
            "confidence_basis": ["Canonical decisions can affect URL authority and should remain approval-first."],
            "execution_eligibility": "approval_required",
            "target_region": "Route / canonical",
            "verification_requirements": ["Canonical route aligns to one preferred URL"],
        }
    if code == "MISSING_CANONICAL":
        return {
            "page_url": page.get("url", ""),
            "page_title": page.get("title", ""),
            "action_type": "resolve_canonical_route",
            "section_name": "Canonical tag",
            "before_state": "No canonical tag",
            "after_state": "Declare the preferred URL for search engines.",
            "reason": issue.get("summary", ""),
            "insight_source": "Structural audit",
            "expected_impact": _expected_impact("resolve_canonical_route"),
            "confidence": "medium",
            "evidence": [str(issue.get("summary", "")).strip()],
            "confidence_basis": ["Canonical changes can affect indexation and should remain approval-first."],
            "execution_eligibility": "approval_required",
            "target_region": "Canonical tag",
            "verification_requirements": ["Canonical tag resolves to the preferred URL"],
        }
    return {
        "page_url": page.get("url", ""),
        "page_title": page.get("title", ""),
        "action_type": "manual_review",
        "section_name": "Page structure",
        "before_state": issue.get("summary", ""),
        "after_state": issue.get("recommendation", ""),
        "reason": issue.get("summary", ""),
        "insight_source": "Structural audit",
        "expected_impact": "Reduces structural SEO risk on this page.",
        "confidence": "medium",
        "evidence": [str(issue.get("summary", "")).strip()],
        "confidence_basis": ["Manual review is required for non-deterministic structural issues."],
        "execution_eligibility": "approval_required",
        "target_region": "Page structure",
        "verification_requirements": [],
    }


def _deterministic_metadata_actions(
    page: Mapping[str, Any],
    gsc: Mapping[str, Any],
    ga4: Mapping[str, Any],
    *,
    primary_lead_event: str,
) -> list[dict[str, Any]]:
    """Build only metadata corrections whose desired value is unambiguous."""

    page_url = str(page.get("final_url") or page.get("url") or "").strip()
    if not page_url:
        return []
    actions: list[dict[str, Any]] = []
    for issue in page.get("issues") or []:
        code = str(issue.get("code", "")).strip()
        if code not in {"MISSING_CANONICAL", "CANONICAL_MISMATCH"}:
            continue
        current = str(page.get("canonical_url", "") or "No canonical tag").strip()
        action = _base_action(
            page=page,
            gsc=gsc,
            ga4=ga4,
            action_type="canonical_update",
            section_name="Canonical metadata",
            before_state=current,
            after_state=page_url,
            reason=(
                "The rendered canonical is missing or differs from the final production URL; "
                "the final 2xx sitemap URL is the deterministic preferred URL."
            ),
            insight_source="Rendered crawl + production sitemap",
            confidence="high",
            action_payload={"canonical_url": page_url},
            primary_lead_event=primary_lead_event,
            confidence_basis=[
                "The URL is in the production sitemap.",
                "The page resolves successfully on the restricted marketing host.",
                "The requested canonical exactly matches the final production URL.",
            ],
        )
        action["evidence"] = [
            str(issue.get("summary", "")).strip(),
            f"Rendered canonical: {current}.",
            f"Final production URL: {page_url}.",
        ]
        actions.append(action)
    return actions


def _analytics_actions(
    page: Mapping[str, Any],
    gsc: Mapping[str, Any],
    ga4: Mapping[str, Any],
    *,
    primary_lead_event: str,
    cluster_map: Mapping[str, list[str]],
) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    impressions = float(gsc.get("impressions", 0) or 0)
    ctr = float(gsc.get("ctr", 0) or 0)
    sessions = float(ga4.get("sessions", 0) or 0)
    lead_conversions = float(ga4.get("lead_conversions", 0) or 0)
    lead_rate = float(ga4.get("lead_conversion_rate", 0) or 0)
    top_queries = list(gsc.get("top_queries") or [])
    has_issues = bool(page.get("issues"))
    focus = _service_focus(page, gsc)
    content_gap = _content_opportunity(page, gsc, ga4)
    top_query = str(top_queries[0].get("query", "")).strip() if top_queries else ""
    if impressions >= 40 and ctr < 0.03:
        title_text = f"{focus} Services | Anata"
        intro = (
            f"Anata helps brands improve {focus.lower()} with execution-first support, clearer operating visibility, "
            "and a faster path from audit findings to measurable growth."
        )
        actions.append(
            _base_action(
                page=page,
                gsc=gsc,
                ga4=ga4,
                action_type="rewrite_title_and_intro",
                section_name="Hero title and intro",
                before_state=f"{int(impressions)} impressions at {ctr:.1%} CTR",
                after_state="Rewrite the page title and hero intro around observed search demand and buyer language.",
                reason="The page is already surfacing in search, but weak CTR suggests the title and intro are underselling relevance.",
                insight_source="Google Search Console",
                confidence="high" if impressions >= 100 and ctr < 0.02 else "medium",
                action_payload={
                    "page_title": title_text,
                    "heading": focus,
                    "intro": intro,
                    "intro_html": f"<p>{intro}</p>",
                },
                primary_lead_event=primary_lead_event,
                confidence_basis=[
                    f"{int(impressions)} impressions crossed the title-test threshold.",
                    f"{ctr:.2%} CTR is below the target benchmark for a commercial service page.",
                    f"Top query language: {top_query or focus}.",
                ],
            )
        )
    if sessions >= 20 and lead_rate < 0.01:
        trust_status = str(ga4.get("trust_status", "missing") or "missing")
        confidence = "high" if trust_status == "trusted" and sessions >= 30 else "medium"
        cta_text = "Book a Free Analysis"
        proof_text = (
            "Get service-specific recommendations, channel priorities, and a practical execution plan tailored to your current operation."
        )
        actions.append(
            _base_action(
                page=page,
                gsc=gsc,
                ga4=ga4,
                action_type="strengthen_primary_cta",
                section_name="Hero CTA and proof block",
                before_state=f"{int(sessions)} sessions, {int(lead_conversions)} {primary_lead_event} events, {lead_rate:.2%} lead rate",
                after_state="Strengthen the CTA language and add proof that reduces hesitation on the primary lead path.",
                reason="Traffic is reaching the page, but the main conversion block is not turning enough visits into qualified lead submissions.",
                insight_source="Google Analytics 4",
                confidence=confidence if trust_status == "trusted" else "medium",
                action_payload={
                    "cta_text": cta_text,
                    "proof_text": proof_text,
                    "proof_html": f"<p>{proof_text}</p>",
                },
                primary_lead_event=primary_lead_event,
                confidence_basis=[
                    f"{int(sessions)} sessions have reached the page during the lookback window.",
                    f"Primary lead event trust is {trust_status}.",
                    f"{lead_rate:.2%} lead conversion rate is below target.",
                ],
            )
        )
    peers = list(cluster_map.get(_normalize_url(str(page.get("url", ""))), []) or [])
    if not has_issues and content_gap["internal_link_gap"] and peers:
        links = []
        for url in peers[:3]:
            anchor = _humanize_slug((_path_from_url(url).split("/")[-1] or "services"))
            if top_query:
                anchor = f"{anchor} services"
            links.append({"url": url, "anchor": anchor})
        actions.append(
            _base_action(
                page=page,
                gsc=gsc,
                ga4=ga4,
                action_type="add_internal_links",
                section_name="Internal links and cluster support",
                before_state=f"Search demand is emerging, but only {int(impressions)} impressions are reaching the page.",
                after_state="Add stronger internal links from adjacent service and hub pages using approved anchor language.",
                reason="The page needs stronger internal authority and clearer topical support from related services.",
                insight_source="Google Search Console",
                confidence="high" if len(links) >= 2 and impressions >= 10 else "medium",
                action_payload={"links": links, "section_label": "Related services"},
                primary_lead_event=primary_lead_event,
                confidence_basis=[
                    f"{len(top_queries)} query patterns are already visible in Search Console.",
                    f"{len(links)} approved cluster destinations are available.",
                    "Internal links are safe only within approved insertion zones.",
                ],
            )
        )
    if not has_issues and content_gap["faq_gap"]:
        query_seed = top_query or focus
        questions = [
            {
                "question": f"What is {query_seed}?",
                "answer": f"{focus} is the operational work required to improve execution, reporting clarity, and measurable growth around {query_seed.lower()}.",
            },
            {
                "question": f"When should a brand invest in {query_seed}?",
                "answer": "A brand should invest when performance is inconsistent, internal bandwidth is limited, or execution needs to move faster with clearer accountability.",
            },
            {
                "question": f"How does Anata approach {query_seed}?",
                "answer": "Anata focuses on practical implementation, measurable outcomes, and decision-ready reporting instead of generic recommendations.",
            },
        ]
        actions.append(
            _base_action(
                page=page,
                gsc=gsc,
                ga4=ga4,
                action_type="update_faq_ai_extraction",
                section_name="FAQ and AI extraction block",
                before_state=f"{int(impressions)} impressions, {int(sessions)} sessions, and limited structured answer coverage.",
                after_state="Add a standardized FAQ and AI extraction block tied to observed search intent and service entities.",
                reason="The page needs clearer definition statements, direct answers, and entity coverage to support both SEO and AI search extraction.",
                insight_source="Google Search Console",
                confidence="high" if top_queries else "medium",
                action_payload={
                    "heading": f"{focus} FAQ",
                    "definitions": [
                        f"{focus} should be tied to measurable growth, not generic activity.",
                        f"Strong {focus.lower()} combines execution, reporting clarity, and commercial intent.",
                    ],
                    "questions": questions,
                    "citable_sentences": [
                        f"{focus} should improve both discoverability and conversion clarity.",
                        f"Anata uses observed search demand to shape how service pages explain the offer.",
                    ],
                },
                primary_lead_event=primary_lead_event,
                confidence_basis=[
                    "Topical coverage and answer extraction can be expanded safely within a dedicated FAQ block.",
                    f"Search demand is {'present' if top_queries else 'still emerging'}, which supports structured Q&A content.",
                ],
            )
        )
    return actions


def _page_bucket(
    page: Mapping[str, Any],
    gsc: Mapping[str, Any],
    ga4: Mapping[str, Any],
    *,
    decision_data_ready: bool = True,
) -> str:
    issues = list(page.get("issues") or [])
    if issues:
        return "repair"
    if not decision_data_ready:
        return "data unavailable"
    impressions = float(gsc.get("impressions", 0) or 0)
    ctr = float(gsc.get("ctr", 0) or 0)
    sessions = float(ga4.get("sessions", 0) or 0)
    conversions = float(ga4.get("lead_conversions", 0) or 0)
    if sessions >= 10 and conversions == 0:
        return "convert"
    if impressions >= 20 and ctr < 0.03:
        return "repair"
    if impressions >= 50 and conversions > 0:
        return "scale"
    if impressions < 20 and sessions < 10:
        return "build"
    return "hold"


def _page_score(
    page: Mapping[str, Any],
    gsc: Mapping[str, Any],
    ga4: Mapping[str, Any],
    *,
    decision_data_ready: bool = True,
) -> int | None:
    if not decision_data_ready:
        return None
    score = 100
    for issue in page.get("issues") or []:
        priority = str(issue.get("priority", "P3"))
        if priority == "P0":
            score -= 40
        elif priority == "P1":
            score -= 25
        elif priority == "P2":
            score -= 12
        else:
            score -= 5
    impressions = float(gsc.get("impressions", 0) or 0)
    ctr = float(gsc.get("ctr", 0) or 0)
    sessions = float(ga4.get("sessions", 0) or 0)
    conversions = float(ga4.get("lead_conversions", 0) or 0)
    if impressions >= 20 and ctr < 0.03:
        score -= 12
    if sessions >= 10 and conversions == 0:
        score -= 15
    if impressions < 20 and sessions < 10:
        score -= 8
    if conversions > 0:
        score += 6
    return max(0, min(100, score))


def _content_actions(
    *,
    settings: Any,
    page: Mapping[str, Any],
    gsc: Mapping[str, Any],
    ga4: Mapping[str, Any],
    primary_lead_event: str,
    blueprint_cache: dict[str, dict[str, Any]],
    customer_questions: list[Mapping[str, Any]],
    search_console_ready: bool = True,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None, dict[str, Any]]:
    impressions = float(gsc.get("impressions", 0) or 0)
    ctr = float(gsc.get("ctr", 0) or 0)
    top_queries = list(gsc.get("top_queries") or [])
    debug_state: dict[str, Any] = {
        "customer_question_count": 0,
        "blueprint_found": False,
        "faq_demand_detected": False,
        "page_thin_enough": False,
        "task_block_reason": "",
        "top_query_count": len(top_queries),
        "query_seed": "",
        "query_pertinent": False,
        "content_route_eligible": _content_route_is_eligible(page),
        "page_has_faq_coverage": _page_has_faq_coverage(page),
    }
    if not search_console_ready:
        debug_state["task_block_reason"] = (
            "Search Console data is unavailable. Ranking-led content recommendations are suspended."
        )
        return [], None, debug_state
    if impressions < MVP_FAQ_IMPRESSIONS_THRESHOLD:
        debug_state["task_block_reason"] = f"Impressions below MVP threshold ({int(impressions)} < {int(MVP_FAQ_IMPRESSIONS_THRESHOLD)})."
        return [], None, debug_state
    if ctr >= MVP_FAQ_CTR_THRESHOLD:
        debug_state["task_block_reason"] = f"CTR is above the MVP intervention threshold ({ctr:.2%} >= {MVP_FAQ_CTR_THRESHOLD:.0%})."
        return [], None, debug_state

    query = str(top_queries[0].get("query", "")).strip() if top_queries else ""
    if not query:
        query = _service_focus(page, gsc)
    debug_state["query_seed"] = query
    if not debug_state["content_route_eligible"]:
        debug_state["task_block_reason"] = (
            "Content recommendations are limited to service, guide, glossary, and blog routes."
        )
        return [], None, debug_state
    if not query:
        debug_state["task_block_reason"] = "No query seed was available for SERP blueprint generation."
        return [], None, debug_state
    if _is_branded_query(query):
        debug_state["task_block_reason"] = (
            "The leading query is branded or navigational and does not support an FAQ or article recommendation."
        )
        return [], None, debug_state
    debug_state["query_pertinent"] = True
    blueprint = blueprint_cache.get(query)
    if blueprint is None:
        try:
            blueprint = build_blueprint(query)
        except Exception:
            blueprint = {
                "blueprint_id": f"bp_{_slugify(query)}_{date.today().isoformat()}",
                "query": query,
                "created_at": date.today().isoformat(),
                "source_urls": [],
                "topical_entities": [],
                "heading_structure": [],
                "faq_patterns": [],
                "content_gaps": [],
            }
        blueprint_cache[query] = blueprint
    debug_state["blueprint_found"] = bool(blueprint)

    matched_questions = _matching_customer_questions(page, customer_questions, top_queries)
    debug_state["customer_question_count"] = len(matched_questions)
    observed_question_queries = _observed_question_queries(top_queries)
    debug_state["observed_question_query_count"] = len(observed_question_queries)
    page_lacks_faq_coverage = not bool(debug_state["page_has_faq_coverage"])
    faq_demand_detected = bool(matched_questions) or (
        len(observed_question_queries) >= 2 and _blueprint_missing_faq(blueprint)
    )
    debug_state["faq_demand_detected"] = faq_demand_detected
    page_thin_enough = _page_thin_for_section(page, blueprint, matched_questions)
    debug_state["page_thin_enough"] = page_thin_enough

    actions: list[dict[str, Any]] = []
    common_evidence = _evidence_lines(page, gsc, ga4, primary_lead_event=primary_lead_event)
    question_count = len(matched_questions)
    supporting_signals = 0
    if impressions >= MVP_FAQ_IMPRESSIONS_THRESHOLD and ctr < MVP_FAQ_CTR_THRESHOLD:
        supporting_signals += 1
    if question_count > 0:
        supporting_signals += 1
    if observed_question_queries and _blueprint_missing_faq(blueprint):
        supporting_signals += 1
    if len(observed_question_queries) >= 2:
        supporting_signals += 1

    faq_payload = build_faq_payload(
        page={**page, "related_service": _page_service_slug(page)},
        blueprint=blueprint,
        customer_questions=matched_questions,
    )
    if faq_payload.get("questions") and faq_demand_detected:
        actions.append(
            _content_task_action(
                page=page,
                gsc=gsc,
                ga4=ga4,
                primary_lead_event=primary_lead_event,
                action_type="inject_faq_block",
                section_name="FAQ block",
                before_state="No structured FAQ block is present on the page.",
                after_state="Insert a structured FAQ block using repeated buyer questions and SERP patterns.",
                reason="Search demand exists, CTR is weak, and the page needs direct-answer content that matches buyer questions.",
                confidence="high" if supporting_signals >= 2 else "medium",
                action_payload=faq_payload,
                confidence_basis=[
                    f"{int(impressions)} impressions crossed the FAQ opportunity threshold.",
                    f"{ctr:.2%} CTR is below the service-page target.",
                    (
                        f"{question_count} matching customer questions were found."
                        if question_count
                        else f"{len(observed_question_queries)} non-branded question queries support direct-answer content."
                    ),
                ],
                evidence=common_evidence
                + ([f"Customer language: {question_count} repeated buyer questions matched this page."] if question_count else [])
                + ([f"Search Console: {len(observed_question_queries)} non-branded question queries were observed."] if observed_question_queries else [])
                + ([f"SERP blueprint: {len(list(blueprint.get('faq_patterns') or []))} repeated FAQ patterns."] if observed_question_queries and _blueprint_missing_faq(blueprint) else [])
                + (["The live page has no visible FAQ section despite strong CTR-loss signals."] if page_lacks_faq_coverage else []),
                execution_eligibility="suggestion_only",
                target_region="FAQ insertion zone",
                verification_requirements=[
                    "FAQ section exists after insert",
                    "No duplicate FAQ block was created",
                    "At least one generated question is visible",
                ],
            )
        )

    if list(blueprint.get("content_gaps") or []) and page_thin_enough and (
        matched_questions or len(observed_question_queries) >= 2
    ):
        section_payload = build_section_expansion_payload(
            page={**page, "related_service": _page_service_slug(page)},
            blueprint=blueprint,
            customer_questions=matched_questions,
        )
        actions.append(
            _content_task_action(
                page=page,
                gsc=gsc,
                ga4=ga4,
                primary_lead_event=primary_lead_event,
                action_type="expand_service_page_section",
                section_name=str(section_payload.get("heading", "") or "Service page section"),
                before_state="The page is missing depth on a repeated buyer topic from search and customer conversations.",
                after_state="Add one structured service-page section that closes the observed content gap.",
                reason="The page needs a deeper section tied to repeated buyer questions and missing SERP coverage.",
                confidence="medium",
                action_payload=section_payload,
                confidence_basis=[
                    f"{len(list(blueprint.get('content_gaps') or []))} content gaps were detected from SERP structure.",
                    "Section expansion remains approval-first until insertion coverage is proven stable.",
                ],
                evidence=common_evidence
                + [f"Blueprint gap: {str(list(blueprint.get('content_gaps') or [])[0])}"]
                + ([f"Customer language: {question_count} repeated buyer questions support this gap."] if question_count else []),
                execution_eligibility="suggestion_only",
                target_region="After first major section",
                verification_requirements=[
                    "New heading is visible on the live page",
                    "Section body renders under the inserted heading",
                ],
            )
        )
    if not actions:
        block_reasons: list[str] = []
        if not faq_demand_detected:
            block_reasons.append("No matched customer questions or FAQ demand signal was found.")
        if list(blueprint.get("content_gaps") or []) and not page_thin_enough:
            block_reasons.append("The page is not thin enough for MVP section expansion.")
        elif list(blueprint.get("content_gaps") or []) and not (matched_questions or len(observed_question_queries) >= 2):
            block_reasons.append("The content gap is not supported by current customer questions or non-branded question queries.")
        if not faq_payload.get("questions"):
            block_reasons.append("FAQ payload generation returned no usable questions.")
        debug_state["task_block_reason"] = " ".join(block_reasons) or "No MVP content task qualified for this page."
    return actions, blueprint, debug_state


def build_autonomy_overlay(
    *,
    settings: Any,
    report: Mapping[str, Any],
    observations: list[Mapping[str, Any]],
    feedback_entries: list[Mapping[str, Any]],
    run_mode: str = "daily",
) -> dict[str, Any]:
    urls = [str(item.get("url", "")) for item in observations]
    config = analytics_config_from_settings(settings)
    identity = _service_account_identity(config.service_account_json)
    gsc_metrics, gsc_notes = fetch_search_console_snapshot(settings, urls)
    ga4_metrics, ga4_notes = fetch_ga4_snapshot(settings, urls)
    search_console_ready = not gsc_notes
    ga4_ready = not ga4_notes
    decision_data_ready = search_console_ready and ga4_ready
    cluster_map = _service_cluster_map(urls)
    ga4_trust_status = _lead_trust_status(ga4_metrics, ga4_notes)
    customer_language_enabled = os.getenv(
        "WEBSITE_OPS_CUSTOMER_LANGUAGE_ENABLED",
        "false",
    ).strip().lower() in {"1", "true", "yes", "on"}
    customer_questions: list[dict[str, Any]] = []
    if customer_language_enabled:
        try:
            customer_questions = collect_customer_questions(
                settings,
                max_messages=int(getattr(settings, "gmail_poll_max_messages", 25) or 25),
            )
        except Exception:
            customer_questions = []
    blueprint_cache: dict[str, dict[str, Any]] = {}

    page_insights: list[dict[str, Any]] = []
    action_queue: list[dict[str, Any]] = []
    content_tasks: list[dict[str, Any]] = []
    support_requests: list[str] = []
    indexing_inventory: dict[str, Any] | None = None
    if run_mode in {"weekly", "monthly"} and search_console_ready:
        indexing_inventory, indexing_notes = inspect_search_console_indexing(
            settings,
            urls,
        )
        indexing_inventory = reconcile_indexing_inventory(
            indexing_inventory,
            observations,
        )
        sitemap_submission = submit_search_console_sitemap(settings)
        indexing_inventory["sitemap_submission"] = sitemap_submission
        if sitemap_submission.get("status") != "submitted":
            support_requests.append(
                str(
                    sitemap_submission.get("reason")
                    or "Search Console sitemap submission failed."
                )
            )
        inspection = dict(indexing_inventory.get("inspection") or {})
        if int(inspection.get("succeeded", 0) or 0):
            save_search_console_indexing_inventory(settings, indexing_inventory)
        else:
            retained_inventory = load_indexing_inventory(
                Path(settings.website_ops_root)
            )
            retained_inventory["inspection"] = inspection
            retained_inventory["sitemap_submission"] = sitemap_submission
            save_search_console_indexing_inventory(settings, retained_inventory)
        support_requests.extend(indexing_notes)
    elif search_console_ready:
        indexing_inventory, indexing_notes = refresh_retained_indexing_inventory(
            settings,
            observations,
        )
        support_requests.extend(indexing_notes)

    for observation in observations:
        url = _normalize_url(str(observation.get("url", "")))
        gsc = gsc_metrics.get(url, {})
        ga4 = ga4_metrics.get(url, {})
        bucket = _page_bucket(
            observation,
            gsc,
            ga4,
            decision_data_ready=decision_data_ready,
        )
        score = _page_score(
            observation,
            gsc,
            ga4,
            decision_data_ready=decision_data_ready,
        )
        insights: list[str] = []
        if observation.get("issues"):
            insights.extend(str(issue.get("summary", "")) for issue in observation.get("issues") or [])
        if float(gsc.get("impressions", 0) or 0) >= 50 and float(gsc.get("ctr", 0) or 0) < 0.02:
            insights.append("Search demand exists, but click-through rate is weak.")
        if float(ga4.get("sessions", 0) or 0) >= 20 and float(ga4.get("lead_conversions", 0) or 0) == 0:
            insights.append("Traffic is reaching the page, but the page is not generating trusted lead conversions.")

        generated_content_actions, blueprint, content_debug = _content_actions(
            settings=settings,
            page=observation,
            gsc=gsc,
            ga4=ga4,
            primary_lead_event=config.primary_lead_event,
            blueprint_cache=blueprint_cache,
            customer_questions=customer_questions,
            search_console_ready=search_console_ready,
        )
        if blueprint:
            blueprint_cache[str(blueprint.get("query", "")).strip()] = blueprint
        filtered_generated_content_actions = _mvp_filter_actions(generated_content_actions)
        action_queue.extend(filtered_generated_content_actions)
        content_tasks.extend(filtered_generated_content_actions)
        action_queue.extend(
            _deterministic_metadata_actions(
                observation,
                gsc,
                ga4,
                primary_lead_event=config.primary_lead_event,
            )
        )
        matched_customer_questions = _matching_customer_questions(
            observation,
            customer_questions,
            list(gsc.get("top_queries") or []),
        )
        aeo_assessment = build_aeo_assessment(
            observation,
            gsc=gsc,
            customer_questions=matched_customer_questions,
        )

        page_insights.append(
            {
                "page_url": url,
                "page_title": observation.get("title", ""),
                "bucket": bucket,
                "score": score,
                "search_console": gsc,
                "ga4": ga4,
                "metric_availability": {
                    "search_console": "observed" if search_console_ready else "unavailable",
                    "ga4": "observed" if ga4_ready else "unavailable",
                },
                "top_queries": gsc.get("top_queries", []),
                "insights": insights[:3],
                "why_this_page_now": insights[:2] or ["This page is part of the monitored commercial service set."],
                "customer_question_count": int(content_debug.get("customer_question_count", 0) or 0),
                "blueprint_found": bool(content_debug.get("blueprint_found")),
                "faq_demand_detected": bool(content_debug.get("faq_demand_detected")),
                "page_thin_enough": bool(content_debug.get("page_thin_enough")),
                "task_block_reason": str(content_debug.get("task_block_reason", "")),
                "query_seed": str(content_debug.get("query_seed", "")),
                "top_query_count": int(content_debug.get("top_query_count", 0) or 0),
                "page_has_faq_coverage": bool(content_debug.get("page_has_faq_coverage")),
                "ga4_trust_status": str(ga4.get("trust_status", ga4_trust_status)),
                "aeo": aeo_assessment,
            }
        )

    if gsc_notes:
        support_requests.extend(gsc_notes)
    if ga4_notes:
        support_requests.extend(ga4_notes)
    if ga4_trust_status != "trusted":
        support_requests.append(
            f"Define or verify the GA4 primary lead event ({config.primary_lead_event}) on real service-page submits so Website Ops can trust conversion-driven prioritization."
        )
    if any(action.get("action_type") == "resolve_canonical_route" for action in action_queue):
        support_requests.append("Standardize all active commercial services under /services/, then redirect legacy /ecommerce-services/ routes so Website Ops can consolidate authority on one canonical page family.")

    query_intelligence = build_query_intelligence(
        settings=settings,
        page_insights=page_insights,
        decision_data_ready=decision_data_ready,
        run_mode=run_mode,
    )
    content_strategy = build_content_strategy(query_intelligence)
    if run_mode in {"daily", "weekly", "monthly"}:
        for _ in range(article_batch_size(settings)):
            try:
                article_action = build_article_action(
                    settings=settings,
                    query_intelligence=query_intelligence,
                )
                if not article_action:
                    break
                action_queue.append(article_action)
                content_tasks.append(article_action)
            except Exception as exc:  # noqa: BLE001 - report provider failure with completed crawl
                support_requests.append(
                    "Autonomous article generation could not complete: "
                    f"{type(exc).__name__}: {exc}"
                )
                break
    content_strategy["production_quota"] = article_generation_progress(settings)
    content_strategy["summary"]["generated_today"] = int(
        content_strategy["production_quota"].get("generated_today", 0) or 0
    )
    content_strategy["summary"]["remaining_to_minimum"] = int(
        content_strategy["production_quota"].get("remaining_to_minimum", 0) or 0
    )
    persist_content_strategy(_website_ops_root(settings), content_strategy)
    page_by_url = {
        _normalize_url(str(item.get("url", ""))): item for item in observations
    }
    for recommendation in query_intelligence.get("recommendations", []) or []:
        if recommendation.get("execution_status") != "eligible":
            continue
        page_url = _normalize_url(str(recommendation.get("page_url", "")))
        page = page_by_url.get(page_url, {})
        action = {
            "page_url": page_url,
            "page_title": str(page.get("title", "") or recommendation.get("current_state", "")),
            "action_type": str(recommendation.get("action_type", "")),
            "section_name": str(recommendation.get("target", "")),
            "before_state": str(recommendation.get("current_state", "")),
            "after_state": str(recommendation.get("proposed_state", "")),
            "reason": str(recommendation.get("reason", "")),
            "insight_source": "Validated query intelligence",
            "expected_impact": (
                "Stronger semantic alignment between observed buyer demand, the search result, "
                "and answer-engine retrieval."
            ),
            "confidence": "high",
            "status": "recommended",
            "evidence": [
                f"Validated query cluster: {recommendation.get('query_cluster', '')}.",
                "Independent evidence: "
                + ", ".join(
                    str(value).replace("_", " ")
                    for value in recommendation.get("evidence_classes", []) or []
                )
                + ".",
                "Two comparable weekly shadow-mode cycles completed.",
            ],
            "confidence_basis": [
                "The query cluster has at least two independent evidence signals.",
                "Exactly one production marketing page owns the intent.",
                "The proposal is a deterministic low-risk metadata correction.",
            ],
            "ga4_trust_status": ga4_trust_status,
            "action_value": str(recommendation.get("action_value", "")),
        }
        action.update(_execution_envelope(action, page_url=page_url))
        if action.get("execution_eligibility") == "auto_execute":
            action_queue.append(action)

    approved_actions = [item for item in feedback_entries if str(item.get("status", "")).strip().lower() == "approved"]
    filtered_action_queue = _mvp_filter_actions(action_queue)
    filtered_content_tasks = _mvp_filter_actions(content_tasks)
    auto_executable_count = sum(1 for item in filtered_action_queue if str(item.get("execution_eligibility", "")) == "auto_execute")
    approval_required_count = sum(1 for item in filtered_action_queue if str(item.get("execution_eligibility", "")) != "auto_execute")
    action_type_coverage = sorted({str(item.get("action_type", "")).strip() for item in filtered_action_queue if str(item.get("action_type", "")).strip()})
    serp_blueprints = list(blueprint_cache.values())
    _save_blueprints(settings, serp_blueprints)
    save_content_tasks(settings, filtered_content_tasks)
    return {
        "goal": {
            "primary": "Increase qualified organic leads by improving the service pages with the strongest search opportunity, weakest conversion efficiency, and highest upside for Google and AI search visibility.",
            "success_metrics": [
                "More clicks from Search Console on priority service pages",
                "Higher landing-page conversion rate in GA4",
                "Fewer structural SEO issues on monitored URLs",
            ],
        },
        "analytics_status": {
            "search_console": not gsc_notes,
            "ga4": not ga4_notes,
            "decision_data_status": "ready" if decision_data_ready else "blocked",
            "operational_status": "operational" if decision_data_ready else "blocked",
            "notes": gsc_notes + ga4_notes,
            "project_id": identity.get("project_id", ""),
            "client_email": identity.get("client_email", ""),
            "search_console_property": config.search_console_property,
            "ga4_property_id": config.ga4_property_id,
            "ga4_trust_status": ga4_trust_status,
            "primary_lead_event": config.primary_lead_event,
            "conversion_weight_enabled": ga4_trust_status == "trusted",
            "search_console_freshness": "connected" if not gsc_notes else "degraded",
            "action_type_coverage": action_type_coverage,
            "auto_executed_today": sum(1 for item in feedback_entries if str(item.get("status", "")).strip().lower() == "done"),
            "approval_required_today": approval_required_count,
            "auto_executable_today": auto_executable_count,
            "mvp_mode_active": MVP_MODE_ACTIVE,
            "mvp_allowed_action_types": list(MVP_ALLOWED_ACTION_TYPES),
            "customer_language_status": "enabled" if customer_language_enabled else "quarantined",
        },
        "support_requests": list(dict.fromkeys(item for item in support_requests if item)),
        "page_insights": sorted(
            page_insights,
            key=lambda item: (
                item["score"] is None,
                item["score"] if item["score"] is not None else 101,
                item["page_url"],
            ),
        )[:20],
        "action_queue": filtered_action_queue[:25],
        "serp_blueprints": serp_blueprints[:10],
        "customer_questions": customer_questions[:12],
        "content_tasks": filtered_content_tasks[:25],
        "query_intelligence": query_intelligence,
        "content_strategy": content_strategy,
        "indexing_inventory": indexing_inventory or {},
        "approved_action_count": len(approved_actions),
        "mvp_mode_active": MVP_MODE_ACTIVE,
        "mvp_allowed_action_types": list(MVP_ALLOWED_ACTION_TYPES),
    }
