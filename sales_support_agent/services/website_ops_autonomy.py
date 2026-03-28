"""Analytics-driven autonomy layer for Website Ops."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import quote, urlparse

import requests
from sales_support_agent.services import website_ops_vendor

try:
    from google.auth.transport.requests import Request as GoogleAuthRequest
    from google.oauth2.service_account import Credentials as ServiceAccountCredentials

    GOOGLE_AUTH_AVAILABLE = True
except ModuleNotFoundError:  # pragma: no cover - environment dependent
    GOOGLE_AUTH_AVAILABLE = False
    GoogleAuthRequest = None
    ServiceAccountCredentials = None


SEARCH_CONSOLE_SCOPE = "https://www.googleapis.com/auth/webmasters.readonly"
GA4_SCOPE = "https://www.googleapis.com/auth/analytics.readonly"


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
        payload = json.loads(raw)
    else:
        payload = json.loads(Path(raw).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON must resolve to an object.")
    return payload


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
    project_name = _load_service_account_info(config.service_account_json).get("project_id", "the configured project") if config.service_account_json else "the configured project"
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


def fetch_ga4_snapshot(settings: Any, urls: list[str]) -> tuple[dict[str, dict[str, Any]], list[str]]:
    config = analytics_config_from_settings(settings)
    project_name = _load_service_account_info(config.service_account_json).get("project_id", "the configured project") if config.service_account_json else "the configured project"
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


def _page_bucket(page: Mapping[str, Any], gsc: Mapping[str, Any], ga4: Mapping[str, Any]) -> str:
    issues = list(page.get("issues") or [])
    if issues:
        return "repair"
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


def _page_score(page: Mapping[str, Any], gsc: Mapping[str, Any], ga4: Mapping[str, Any]) -> int:
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


def build_autonomy_overlay(
    *,
    settings: Any,
    report: Mapping[str, Any],
    observations: list[Mapping[str, Any]],
    feedback_entries: list[Mapping[str, Any]],
) -> dict[str, Any]:
    urls = [str(item.get("url", "")) for item in observations]
    config = analytics_config_from_settings(settings)
    identity = _service_account_identity(config.service_account_json)
    gsc_metrics, gsc_notes = fetch_search_console_snapshot(settings, urls)
    ga4_metrics, ga4_notes = fetch_ga4_snapshot(settings, urls)
    cluster_map = _service_cluster_map(urls)
    ga4_trust_status = _lead_trust_status(ga4_metrics, ga4_notes)

    page_insights: list[dict[str, Any]] = []
    action_queue: list[dict[str, Any]] = []
    support_requests: list[str] = []

    for observation in observations:
        url = _normalize_url(str(observation.get("url", "")))
        gsc = gsc_metrics.get(url, {})
        ga4 = ga4_metrics.get(url, {})
        bucket = _page_bucket(observation, gsc, ga4)
        score = _page_score(observation, gsc, ga4)
        insights: list[str] = []
        if observation.get("issues"):
            insights.extend(str(issue.get("summary", "")) for issue in observation.get("issues") or [])
        if float(gsc.get("impressions", 0) or 0) >= 50 and float(gsc.get("ctr", 0) or 0) < 0.02:
            insights.append("Search demand exists, but click-through rate is weak.")
        if float(ga4.get("sessions", 0) or 0) >= 20 and float(ga4.get("lead_conversions", 0) or 0) == 0:
            insights.append("Traffic is reaching the page, but the page is not generating trusted lead conversions.")

        for issue in observation.get("issues") or []:
            action_queue.append(_structural_action_from_issue(observation, issue))
        action_queue.extend(
            _analytics_actions(
                observation,
                gsc,
                ga4,
                primary_lead_event=config.primary_lead_event,
                cluster_map=cluster_map,
            )
        )

        page_insights.append(
            {
                "page_url": url,
                "page_title": observation.get("title", ""),
                "bucket": bucket,
                "score": score,
                "search_console": gsc,
                "ga4": ga4,
                "top_queries": gsc.get("top_queries", []),
                "insights": insights[:3],
                "why_this_page_now": insights[:2] or ["This page is part of the monitored commercial service set."],
                "ga4_trust_status": str(ga4.get("trust_status", ga4_trust_status)),
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

    approved_actions = [item for item in feedback_entries if str(item.get("status", "")).strip().lower() == "approved"]
    auto_executable_count = sum(1 for item in action_queue if str(item.get("execution_eligibility", "")) == "auto_execute")
    approval_required_count = sum(1 for item in action_queue if str(item.get("execution_eligibility", "")) != "auto_execute")
    action_type_coverage = sorted({str(item.get("action_type", "")).strip() for item in action_queue if str(item.get("action_type", "")).strip()})

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
        },
        "support_requests": list(dict.fromkeys(item for item in support_requests if item)),
        "page_insights": sorted(page_insights, key=lambda item: (item["score"], item["page_url"]))[:20],
        "action_queue": action_queue[:25],
        "approved_action_count": len(approved_actions),
    }
