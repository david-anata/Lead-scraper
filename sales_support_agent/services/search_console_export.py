"""Read-only Search Console export into a Google Sheet.

The job lists every property the service account can see, then pulls the last
28 days of top queries and top pages for the configured Anata property and,
when it is visible, the anatabuilding.com property. It overwrites a spreadsheet
and does not run Website Ops or change either website.

Search Console calls use the same readonly query contract as
``fetch_search_console_snapshot`` (webmasters.readonly, a 28-day window ending
yesterday, and a 250-row limit). This export asks for site-level ``query`` and
``page`` dimensions instead of the page-filtered snapshot the website pipeline
uses. The only additional scope is Google Sheets.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable
from urllib.parse import quote, urlparse

import requests

from sales_support_agent.services.website_ops_autonomy import (
    SEARCH_CONSOLE_SCOPE,
    _google_access_token,
    _load_service_account_info,
    _search_console_failure_note,
    _setting,
)

logger = logging.getLogger(__name__)

SHEETS_SCOPE = "https://www.googleapis.com/auth/spreadsheets"
EXPORT_SCOPES = (SEARCH_CONSOLE_SCOPE, SHEETS_SCOPE)
LOOKBACK_DAYS = 28
ROW_LIMIT = 250
PRIMARY_PROPERTY_DEFAULT = "sc-domain:anatainc.com"
BUILDING_HOST = "anatabuilding.com"
SITES_URL = "https://searchconsole.googleapis.com/webmasters/v3/sites"
SHEETS_API_ROOT = "https://sheets.googleapis.com/v4"
PROPERTIES_TAB = "properties"
_TAB_FORBIDDEN = re.compile(r"[:\\/?*\[\]]+")
Requester = Callable[..., Any]


class SearchConsoleExportError(RuntimeError):
    """Operator-facing export failure. The message must not contain secrets."""


def reporting_window(today: date) -> tuple[date, date]:
    """Return the inclusive 28-day window ending yesterday."""

    end = today - timedelta(days=1)
    start = end - timedelta(days=LOOKBACK_DAYS - 1)
    return start, end


def property_host(site_url: str) -> str:
    """Return the registrable host for a domain property or URL-prefix property."""

    value = str(site_url or "").strip()
    if value.lower().startswith("sc-domain:"):
        host = value.split(":", 1)[1]
    else:
        host = urlparse(value).hostname or ""
    return host.lower().removeprefix("www.").strip(".")


def property_label(site_url: str) -> str:
    """Return a spreadsheet-tab-safe label for a Search Console property."""

    label = property_host(site_url) or str(site_url or "").strip()
    cleaned = _TAB_FORBIDDEN.sub(" ", label)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return (cleaned or "property")[:80]


def dimension_tab_name(site_url: str, dimension: str) -> str:
    """Return ``anatainc.com queries`` or ``anatainc.com pages``."""

    suffix = "queries" if dimension == "query" else "pages"
    return f"{property_label(site_url)} {suffix}"[:100]


def select_building_property(properties: list[dict[str, str]]) -> str:
    """Pick the anatabuilding.com property when the service account can see one.

    Domain properties win, then an https URL-prefix property, then anything else
    on that host. ``www`` is kept only when no apex property is visible.
    """

    matches = [
        str(item.get("site_url") or "").strip()
        for item in properties
        if property_host(str(item.get("site_url") or "")) == BUILDING_HOST
    ]
    matches = [item for item in matches if item]
    if not matches:
        return ""

    def rank(site_url: str) -> tuple[int, int, int, str]:
        if site_url.lower().startswith("sc-domain:"):
            return (0, 0, 0, site_url)
        parsed = urlparse(site_url)
        scheme_rank = 0 if parsed.scheme == "https" else 1
        www_rank = 1 if (parsed.hostname or "").lower().startswith("www.") else 0
        return (1, scheme_rank, www_rank, site_url)

    return sorted(matches, key=rank)[0]


def normalize_sheet_id(value: str) -> str:
    """Accept a spreadsheet id or a Google Sheets URL."""

    raw = str(value or "").strip()
    marker = "/spreadsheets/d/"
    if marker in raw:
        raw = raw.split(marker, 1)[1].split("/", 1)[0].strip()
    if not raw or any(character in raw for character in " /?#"):
        raise SearchConsoleExportError(
            "GSC_EXPORT_SHEET_ID must be the spreadsheet id from the sheet URL."
        )
    return raw


def run_search_console_export(
    settings: Any,
    *,
    requester: Requester = requests.request,
    today: date | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Pull Search Console into the configured spreadsheet and overwrite it.

    Search Console is read with ``webmasters.readonly`` only. The spreadsheet
    write is the only mutation. Website files, GitHub, and the Website Ops
    pipeline are not touched.
    """

    started = now or datetime.now(timezone.utc)
    service_account_json = _setting(
        settings,
        "google_service_account_json",
        "GOOGLE_SERVICE_ACCOUNT_JSON",
    )
    client_email = _client_email(service_account_json)
    logger.info("Search Console export client_email=%s", client_email)
    sheet_id = _sheet_id(settings, client_email)
    primary = (
        _setting(
            settings,
            "website_ops_gsc_property",
            "WEBSITE_OPS_GSC_PROPERTY",
            PRIMARY_PROPERTY_DEFAULT,
        )
        or PRIMARY_PROPERTY_DEFAULT
    )
    window_start, window_end = reporting_window(today or started.date())
    logger.info(
        "Search Console export starting client_email=%s sheet_id=%s primary=%s",
        client_email,
        sheet_id,
        primary,
    )
    try:
        token = _google_access_token(service_account_json, list(EXPORT_SCOPES))
    except Exception as exc:
        raise SearchConsoleExportError(_safe_error(exc)) from None

    properties = _list_properties(requester, token)
    building = select_building_property(properties)
    targets = [primary]
    if building and building != primary:
        targets.append(building)

    tables: dict[str, list[list[Any]]] = {}
    row_counts: dict[str, int] = {}
    exported: set[str] = set()
    notes: dict[str, str] = {}
    for site_url in targets:
        if site_url == building:
            notes[site_url] = ""
        elif not any(item.get("site_url") == site_url for item in properties):
            notes[site_url] = "Not returned by sites.list; the query was still attempted."
        for dimension in ("query", "page"):
            rows = _query_dimension(
                requester,
                token,
                site_url,
                dimension,
                window_start,
                window_end,
            )
            title = dimension_tab_name(site_url, dimension)
            tables[title] = _metric_table(rows, dimension)
            row_counts[title] = max(len(tables[title]) - 1, 0)
        exported.add(site_url)

    finished = datetime.now(timezone.utc)
    tables = {
        PROPERTIES_TAB: _properties_table(
            properties,
            exported=exported,
            notes=notes,
            started=started,
            finished=finished,
            window_start=window_start,
            window_end=window_end,
            client_email=client_email,
            primary=primary,
        ),
        **tables,
    }
    _overwrite_spreadsheet(requester, token, sheet_id, tables, client_email)
    logger.info(
        "Search Console export wrote sheet_id=%s client_email=%s tabs=%s",
        sheet_id,
        client_email,
        ",".join(tables),
    )
    return {
        "ok": True,
        "client_email": client_email,
        "sheet_id": sheet_id,
        "sheet_url": f"https://docs.google.com/spreadsheets/d/{sheet_id}",
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "run_started_at": started.isoformat(),
        "run_finished_at": finished.isoformat(),
        "properties": properties,
        "exported_properties": targets,
        "tabs": list(tables),
        "row_counts": row_counts,
    }


def _sheet_id(settings: Any, client_email: str) -> str:
    raw = str(getattr(settings, "gsc_export_sheet_id", "") or "").strip()
    if not raw:
        raw = _setting(settings, "gsc_export_sheet_id", "GSC_EXPORT_SHEET_ID")
    if not raw:
        raise SearchConsoleExportError(
            "GSC_EXPORT_SHEET_ID is not configured. "
            f"Share the destination spreadsheet with {client_email} as Editor, then set the spreadsheet id."
        )
    return normalize_sheet_id(raw)


def _client_email(raw_service_account_json: str) -> str:
    try:
        info = _load_service_account_info(raw_service_account_json)
    except Exception as exc:
        raise SearchConsoleExportError(_safe_error(exc)) from None
    email = str(info.get("client_email") or "").strip()
    if not email:
        raise SearchConsoleExportError(
            "GOOGLE_SERVICE_ACCOUNT_JSON is missing client_email."
        )
    return email


def _safe_error(exc: Exception) -> str:
    if isinstance(exc, json.JSONDecodeError):
        return "GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON."
    text = str(exc).strip()
    lowered = text.lower()
    if any(
        marker in lowered
        for marker in ("private_key", "begin private", "bearer ", "ya29.")
    ):
        return "Google authentication failed."
    return text[:300] or "Google authentication failed."


def _headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _list_properties(requester: Requester, token: str) -> list[dict[str, str]]:
    response = requester(
        "GET",
        SITES_URL,
        headers=_headers(token),
        timeout=30,
    )
    if not getattr(response, "ok", False):
        raise SearchConsoleExportError(
            _search_console_failure_note(response, "", "")
        )
    try:
        payload = response.json()
    except ValueError as exc:
        raise SearchConsoleExportError(
            "Search Console sites.list returned invalid JSON."
        ) from exc
    if not isinstance(payload, dict):
        raise SearchConsoleExportError("Search Console sites.list returned an unexpected payload.")
    entries = payload.get("siteEntry") or []
    properties: list[dict[str, str]] = []
    for item in entries:
        if not isinstance(item, dict):
            continue
        site_url = str(item.get("siteUrl") or "").strip()
        if not site_url:
            continue
        properties.append(
            {
                "site_url": site_url,
                "permission_level": str(item.get("permissionLevel") or "").strip(),
            }
        )
    return properties


def _query_dimension(
    requester: Requester,
    token: str,
    site_url: str,
    dimension: str,
    start: date,
    end: date,
) -> list[dict[str, Any]]:
    encoded = quote(site_url, safe="")
    response = requester(
        "POST",
        f"https://searchconsole.googleapis.com/webmasters/v3/sites/{encoded}/searchAnalytics/query",
        headers={**_headers(token), "Content-Type": "application/json"},
        json={
            "startDate": start.isoformat(),
            "endDate": end.isoformat(),
            "dimensions": [dimension],
            "rowLimit": ROW_LIMIT,
        },
        timeout=30,
    )
    if not getattr(response, "ok", False):
        note = _search_console_failure_note(response, site_url, "")
        raise SearchConsoleExportError(f"{site_url} {dimension}: {note}")
    try:
        payload = response.json()
    except ValueError as exc:
        raise SearchConsoleExportError(
            f"{site_url} {dimension}: Search Console returned invalid JSON."
        ) from exc
    rows = payload.get("rows") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        return []
    return [row for row in rows[:ROW_LIMIT] if isinstance(row, dict)]


def _metric_table(rows: list[dict[str, Any]], dimension: str) -> list[list[Any]]:
    header = dimension if dimension in {"query", "page"} else "value"
    table: list[list[Any]] = [[header, "clicks", "impressions", "ctr", "position"]]
    for row in rows:
        keys = row.get("keys") or []
        if not keys:
            continue
        table.append(
            [
                str(keys[0]),
                _whole_number(row.get("clicks")),
                _whole_number(row.get("impressions")),
                round(float(row.get("ctr") or 0), 6),
                round(float(row.get("position") or 0), 2),
            ]
        )
    return table


def _whole_number(value: Any) -> int | float:
    number = float(value or 0)
    if number.is_integer():
        return int(number)
    return number


def _properties_table(
    properties: list[dict[str, str]],
    *,
    exported: set[str],
    notes: dict[str, str],
    started: datetime,
    finished: datetime,
    window_start: date,
    window_end: date,
    client_email: str,
    primary: str,
) -> list[list[Any]]:
    rows: list[list[Any]] = [
        ["run_started_at", started.isoformat()],
        ["run_finished_at", finished.isoformat()],
        ["window_start", window_start.isoformat()],
        ["window_end", window_end.isoformat()],
        ["client_email", client_email],
        ["primary_property", primary],
        [],
        ["site_url", "permission_level", "exported", "note"],
    ]
    seen: set[str] = set()
    for item in properties:
        site_url = item["site_url"]
        seen.add(site_url)
        rows.append(
            [
                site_url,
                item.get("permission_level", ""),
                "yes" if site_url in exported else "no",
                notes.get(site_url, ""),
            ]
        )
    for site_url in sorted(exported - seen):
        rows.append([site_url, "", "yes", notes.get(site_url, "")])
    return rows


def _overwrite_spreadsheet(
    requester: Requester,
    token: str,
    sheet_id: str,
    tables: dict[str, list[list[Any]]],
    client_email: str,
) -> None:
    root = f"{SHEETS_API_ROOT}/spreadsheets/{quote(sheet_id, safe='')}"
    existing = _spreadsheet_sheets(requester, token, root, client_email)
    desired = list(tables)
    structural: list[dict[str, Any]] = []
    for title in desired:
        if title not in existing:
            structural.append({"addSheet": {"properties": {"title": title}}})
    for title, numeric_id in existing.items():
        if title not in desired:
            structural.append({"deleteSheet": {"sheetId": numeric_id}})
    if structural:
        _sheets_json(
            requester,
            token,
            "POST",
            f"{root}:batchUpdate",
            {"requests": structural},
            client_email,
        )
        existing = _spreadsheet_sheets(requester, token, root, client_email)
    missing = [title for title in desired if title not in existing]
    if missing:
        raise SearchConsoleExportError(
            "Google Sheets did not create the "
            + ", ".join(missing)
            + " tab."
        )
    _sheets_json(
        requester,
        token,
        "POST",
        f"{root}/values:batchClear",
        {"ranges": [_a1(title, "A:Z") for title in desired]},
        client_email,
    )
    _sheets_json(
        requester,
        token,
        "POST",
        f"{root}/values:batchUpdate",
        {
            "valueInputOption": "RAW",
            "data": [
                {
                    "range": _a1(title, "A1"),
                    "majorDimension": "ROWS",
                    "values": rows,
                }
                for title, rows in tables.items()
            ],
        },
        client_email,
    )


def _spreadsheet_sheets(
    requester: Requester,
    token: str,
    root: str,
    client_email: str,
) -> dict[str, int]:
    payload = _sheets_json(
        requester,
        token,
        "GET",
        f"{root}?fields=sheets.properties",
        None,
        client_email,
    )
    sheets = payload.get("sheets") if isinstance(payload, dict) else None
    found: dict[str, int] = {}
    for sheet in sheets or []:
        props = sheet.get("properties") if isinstance(sheet, dict) else None
        if not isinstance(props, dict):
            continue
        title = str(props.get("title") or "").strip()
        if not title:
            continue
        try:
            found[title] = int(props.get("sheetId"))
        except (TypeError, ValueError):
            continue
    if not found:
        raise SearchConsoleExportError("Google Sheets did not return any tabs.")
    return found


def _sheets_json(
    requester: Requester,
    token: str,
    method: str,
    url: str,
    body: dict[str, Any] | None,
    client_email: str,
) -> dict[str, Any]:
    kwargs: dict[str, Any] = {"headers": _headers(token), "timeout": 30}
    if body is not None:
        kwargs["json"] = body
        kwargs["headers"] = {
            **_headers(token),
            "Content-Type": "application/json",
        }
    response = requester(method, url, **kwargs)
    if not getattr(response, "ok", False):
        raise SearchConsoleExportError(_sheets_failure(response, client_email))
    if method == "POST" and url.endswith(":batchClear"):
        return {}
    try:
        payload = response.json()
    except ValueError as exc:
        raise SearchConsoleExportError("Google Sheets returned invalid JSON.") from exc
    if not isinstance(payload, dict):
        raise SearchConsoleExportError("Google Sheets returned an unexpected payload.")
    return payload


def _sheets_failure(response: Any, client_email: str) -> str:
    status = int(getattr(response, "status_code", 0) or 0)
    if status in {403, 404}:
        return (
            "Google Sheets denied access to GSC_EXPORT_SHEET_ID. "
            f"Share the spreadsheet with {client_email} as Editor."
        )
    return f"Google Sheets request failed ({status})."


def _a1(title: str, cells: str) -> str:
    escaped = title.replace("'", "''")
    return f"'{escaped}'!{cells}"
