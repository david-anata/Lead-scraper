"""Analytics-driven autonomy layer for Website Ops."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import quote, urlparse

import requests

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


def analytics_config_from_settings(settings: Any) -> AnalyticsConfig:
    return AnalyticsConfig(
        service_account_json=_setting(settings, "google_service_account_json", "GOOGLE_SERVICE_ACCOUNT_JSON"),
        search_console_property=_setting(settings, "website_ops_gsc_property", "WEBSITE_OPS_GSC_PROPERTY", "sc-domain:anatainc.com"),
        ga4_property_id=_setting(settings, "website_ops_ga4_property_id", "WEBSITE_OPS_GA4_PROPERTY_ID"),
        lookback_days=max(int(_setting(settings, "website_ops_lookback_days", "WEBSITE_OPS_LOOKBACK_DAYS", "28") or "28"), 7),
    )


def fetch_search_console_snapshot(settings: Any, urls: list[str]) -> tuple[dict[str, dict[str, Any]], list[str]]:
    config = analytics_config_from_settings(settings)
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
        return {}, [f"Search Console request failed ({page_response.status_code})."]

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
    if not config.service_account_json:
        return {}, ["GA4 unavailable: GOOGLE_SERVICE_ACCOUNT_JSON is not configured."]
    if not config.ga4_property_id:
        return {}, ["GA4 unavailable: WEBSITE_OPS_GA4_PROPERTY_ID is not configured."]

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
            "metrics": [{"name": "sessions"}, {"name": "engagedSessions"}, {"name": "conversions"}],
            "limit": 250,
        },
        timeout=30,
    )
    if not response.ok:
        return {}, [f"GA4 request failed ({response.status_code})."]

    monitored_paths = {_path_from_url(url): _normalize_url(url) for url in urls}
    metrics_by_url: dict[str, dict[str, Any]] = {}
    for row in response.json().get("rows", []):
        dimensions = row.get("dimensionValues") or []
        metrics = row.get("metricValues") or []
        if not dimensions or len(metrics) < 3:
            continue
        landing = str(dimensions[0].get("value", "")).strip()
        path = _path_from_url(landing)
        if path not in monitored_paths:
            continue
        url = monitored_paths[path]
        sessions = float(metrics[0].get("value", 0) or 0)
        engaged_sessions = float(metrics[1].get("value", 0) or 0)
        conversions = float(metrics[2].get("value", 0) or 0)
        metrics_by_url[url] = {
            "sessions": sessions,
            "engaged_sessions": engaged_sessions,
            "conversions": conversions,
            "conversion_rate": (conversions / sessions) if sessions else 0.0,
        }
    return metrics_by_url, []


def _confidence_level(value: str) -> str:
    return value if value in {"high", "medium", "low"} else "medium"


def _expected_impact(action_type: str) -> str:
    impacts = {
        "replace_primary_heading": "Stronger topic clarity for organic ranking and AI extraction.",
        "rewrite_title_and_intro": "Higher SERP click-through rate from existing impressions.",
        "strengthen_primary_cta": "Higher lead conversion rate from existing traffic.",
        "resolve_canonical_route": "Clearer authority consolidation and less route confusion.",
    }
    return impacts.get(action_type, "Improves page performance against the current growth goal.")


def _structural_action_from_issue(page: Mapping[str, Any], issue: Mapping[str, Any]) -> dict[str, Any]:
    code = str(issue.get("code", ""))
    h1 = list(page.get("h1") or [])
    canonical = str(page.get("canonical_url", "") or "")
    if code == "MULTIPLE_H1":
        return {
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
            "requires_approval": False,
            "status": "recommended",
        }
    if code == "MISSING_H1":
        return {
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
            "requires_approval": False,
            "status": "recommended",
        }
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
            "requires_approval": True,
            "status": "recommended",
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
            "requires_approval": True,
            "status": "recommended",
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
        "requires_approval": True,
        "status": "recommended",
    }


def _analytics_actions(page: Mapping[str, Any], gsc: Mapping[str, Any], ga4: Mapping[str, Any]) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    impressions = float(gsc.get("impressions", 0) or 0)
    ctr = float(gsc.get("ctr", 0) or 0)
    sessions = float(ga4.get("sessions", 0) or 0)
    conversions = float(ga4.get("conversions", 0) or 0)
    if impressions >= 50 and ctr < 0.02:
        actions.append(
            {
                "page_url": page.get("url", ""),
                "page_title": page.get("title", ""),
                "action_type": "rewrite_title_and_intro",
                "section_name": "Title / SERP framing",
                "before_state": f"{int(impressions)} impressions at {ctr:.1%} CTR",
                "after_state": "Rewrite title and top-of-page messaging to align with buyer intent and likely query language.",
                "reason": "This page is appearing in search often enough to matter, but too few searchers click through.",
                "insight_source": "Google Search Console",
                "expected_impact": _expected_impact("rewrite_title_and_intro"),
                "confidence": "medium",
                "requires_approval": True,
                "status": "recommended",
            }
        )
    if sessions >= 20 and conversions == 0:
        actions.append(
            {
                "page_url": page.get("url", ""),
                "page_title": page.get("title", ""),
                "action_type": "strengthen_primary_cta",
                "section_name": "Hero CTA / proof block",
                "before_state": f"{int(sessions)} sessions and {int(conversions)} conversions",
                "after_state": "Clarify the offer, add proof, and strengthen the primary conversion path.",
                "reason": "The page is attracting visits but not converting them into lead actions.",
                "insight_source": "Google Analytics 4",
                "expected_impact": _expected_impact("strengthen_primary_cta"),
                "confidence": "medium",
                "requires_approval": True,
                "status": "recommended",
            }
        )
    return actions


def _page_bucket(page: Mapping[str, Any], gsc: Mapping[str, Any], ga4: Mapping[str, Any]) -> str:
    issues = list(page.get("issues") or [])
    if issues:
        return "repair"
    impressions = float(gsc.get("impressions", 0) or 0)
    ctr = float(gsc.get("ctr", 0) or 0)
    sessions = float(ga4.get("sessions", 0) or 0)
    conversions = float(ga4.get("conversions", 0) or 0)
    if sessions >= 20 and conversions == 0:
        return "convert"
    if impressions >= 50 and ctr < 0.02:
        return "repair"
    if impressions >= 50 and conversions > 0:
        return "scale"
    if impressions < 20 and sessions < 10:
        return "hold"
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
    conversions = float(ga4.get("conversions", 0) or 0)
    if impressions >= 50 and ctr < 0.02:
        score -= 12
    if sessions >= 20 and conversions == 0:
        score -= 15
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
    gsc_metrics, gsc_notes = fetch_search_console_snapshot(settings, urls)
    ga4_metrics, ga4_notes = fetch_ga4_snapshot(settings, urls)

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
        if float(ga4.get("sessions", 0) or 0) >= 20 and float(ga4.get("conversions", 0) or 0) == 0:
            insights.append("Traffic is reaching the page, but the page is not generating conversions.")

        for issue in observation.get("issues") or []:
            action_queue.append(_structural_action_from_issue(observation, issue))
        action_queue.extend(_analytics_actions(observation, gsc, ga4))

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
            }
        )

    if gsc_notes:
        support_requests.extend(gsc_notes)
    if ga4_notes:
        support_requests.extend(ga4_notes)
    if ga4_metrics and all(float(item.get("conversions", 0) or 0) == 0 for item in ga4_metrics.values()):
        support_requests.append("Define or verify lead conversion events in GA4 so Website Ops can prioritize conversion fixes with confidence.")
    if any(action.get("action_type") == "resolve_canonical_route" for action in action_queue):
        support_requests.append("Resolve legacy versus current service routes so Website Ops can consolidate authority on one canonical page family.")

    approved_actions = [item for item in feedback_entries if str(item.get("status", "")).strip().lower() == "approved"]
    start_doing = [
        "Prioritize pages with search demand but weak CTR.",
        "Prioritize pages with traffic but no conversions.",
        "Approve high-confidence structural fixes quickly.",
    ]
    stop_doing = [
        "Stop splitting authority across legacy and current route families.",
        "Stop editing healthy pages without a clear search or conversion signal.",
        "Stop publishing service pages without proof and conversion intent.",
    ]
    do_more_of = [
        "Provide proof assets and case studies for priority service pages.",
        "Clarify conversion events in GA4 so low-converting pages can be fixed faster.",
        "Use the dashboard approval queue instead of one-off requests.",
    ]

    return {
        "goal": {
            "primary": "Increase qualified organic leads by improving service pages with the strongest search opportunity and weakest conversion efficiency.",
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
        },
        "start_doing": start_doing,
        "stop_doing": stop_doing,
        "do_more_of": do_more_of,
        "support_requests": list(dict.fromkeys(item for item in support_requests if item)),
        "page_insights": sorted(page_insights, key=lambda item: (item["score"], item["page_url"]))[:20],
        "action_queue": action_queue[:25],
        "approved_action_count": len(approved_actions),
    }

