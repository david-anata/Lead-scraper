"""Read-only Search Console export: properties, 28-day query/page tabs, sheet overwrite."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from types import SimpleNamespace
from typing import Any
from unittest import mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from sales_support_agent.api.search_console_export_job import router
from sales_support_agent.services.search_console_export import (
    EXPORT_SCOPES,
    ROW_LIMIT,
    SHEETS_SCOPE,
    SearchConsoleExportError,
    normalize_sheet_id,
    reporting_window,
    run_search_console_export,
    select_building_property,
)
from sales_support_agent.services.website_ops_autonomy import SEARCH_CONSOLE_SCOPE


SERVICE_ACCOUNT = {
    "type": "service_account",
    "project_id": "anata-test",
    "client_email": "gsc-export@anata-test.iam.gserviceaccount.com",
    "private_key": "-----BEGIN PRIVATE KEY-----\nfake-key-material\n-----END PRIVATE KEY-----\n",
    "token_uri": "https://oauth2.googleapis.com/token",
}
SHEET_ID = "sheet-export-id"
TODAY = date(2026, 11, 12)
NOW = datetime(2026, 11, 12, 14, 0, tzinfo=timezone.utc)


class Response:
    def __init__(self, status_code: int, payload: Any):
        self.status_code = status_code
        self.ok = 200 <= status_code < 300
        self.text = json.dumps(payload)
        self._payload = payload

    def json(self) -> Any:
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class GoogleFake:
    def __init__(self, properties: list[dict[str, str]], rows: dict[tuple[str, str], list[dict]]):
        self.properties = properties
        self.rows = rows
        self.calls: list[dict[str, Any]] = []
        self.sheets = {"Sheet1": 0, "old notes": 4}
        self._next_id = 10

    def __call__(self, method: str, url: str, **kwargs: Any) -> Response:
        self.calls.append({"method": method, "url": url, "kwargs": kwargs})
        if url.endswith("/webmasters/v3/sites") and method == "GET":
            return Response(
                200,
                {
                    "siteEntry": [
                        {
                            "siteUrl": item["site_url"],
                            "permissionLevel": item["permission_level"],
                        }
                        for item in self.properties
                    ]
                },
            )
        if url.endswith("/searchAnalytics/query") and method == "POST":
            site_url = _property_from_query_url(url)
            dimension = kwargs["json"]["dimensions"][0]
            return Response(200, {"rows": self.rows.get((site_url, dimension), [])})
        if "/spreadsheets/" in url and method == "GET":
            return Response(200, {"sheets": _sheet_payload(self.sheets)})
        if url.endswith("/values:batchUpdate") and method == "POST":
            return Response(200, {"totalUpdatedSheets": len(kwargs["json"]["data"])})
        if url.endswith(":batchUpdate") and method == "POST":
            for request in kwargs["json"]["requests"]:
                if "addSheet" in request:
                    title = request["addSheet"]["properties"]["title"]
                    self.sheets[title] = self._next_id
                    self._next_id += 1
                elif "deleteSheet" in request:
                    sheet_id = request["deleteSheet"]["sheetId"]
                    self.sheets = {
                        title: numeric
                        for title, numeric in self.sheets.items()
                        if numeric != sheet_id
                    }
            return Response(200, {"replies": []})
        if url.endswith(":batchClear") and method == "POST":
            return Response(200, {"clearedRanges": kwargs["json"]["ranges"]})
        raise AssertionError(f"unexpected Google call {method} {url}")


def _sheet_payload(sheets: dict[str, int]) -> list[dict[str, Any]]:
    return [
        {"properties": {"sheetId": numeric, "title": title}}
        for title, numeric in sheets.items()
    ]


def _property_from_query_url(url: str) -> str:
    from urllib.parse import unquote

    marker = "/webmasters/v3/sites/"
    encoded = url.split(marker, 1)[1].split("/searchAnalytics/query", 1)[0]
    return unquote(encoded)


def _settings(**overrides: Any) -> SimpleNamespace:
    values = {
        "google_service_account_json": json.dumps(SERVICE_ACCOUNT),
        "gsc_export_sheet_id": SHEET_ID,
        "website_ops_gsc_property": "sc-domain:anatainc.com",
        "internal_api_key": "internal-test-key",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _rows(label: str) -> list[dict[str, Any]]:
    return [
        {
            "keys": [label],
            "clicks": 4,
            "impressions": 40,
            "ctr": 0.1,
            "position": 3.25,
        }
    ]


def _visible_properties() -> list[dict[str, str]]:
    return [
        {"site_url": "sc-domain:anatainc.com", "permission_level": "siteFullUser"},
        {"site_url": "https://www.anatabuilding.com/", "permission_level": "siteOwner"},
        {"site_url": "sc-domain:anatabuilding.com", "permission_level": "siteFullUser"},
        {"site_url": "https://example.com/", "permission_level": "siteUnverifiedUser"},
    ]


def _run(fake: GoogleFake, **overrides: Any) -> dict[str, Any]:
    with mock.patch(
        "sales_support_agent.services.search_console_export._google_access_token",
        return_value="test-token",
    ) as token:
        result = run_search_console_export(
            _settings(**overrides),
            requester=fake,
            today=TODAY,
            now=NOW,
        )
    result["_scopes"] = token.call_args.args[1]
    return result


def test_reporting_window_is_28_days_ending_yesterday() -> None:
    start, end = reporting_window(TODAY)
    assert (end - start).days == 27
    assert start.isoformat() == "2026-10-15"
    assert end.isoformat() == "2026-11-11"


def test_building_property_prefers_the_domain_property() -> None:
    selected = select_building_property(_visible_properties())
    assert selected == "sc-domain:anatabuilding.com"


def test_building_property_is_absent_when_the_host_is_not_visible() -> None:
    assert select_building_property(
        [{"site_url": "sc-domain:anatainc.com", "permission_level": "siteFullUser"}]
    ) == ""


def test_sheet_id_accepts_a_docs_url() -> None:
    assert normalize_sheet_id(
        "https://docs.google.com/spreadsheets/d/abc123_-XYZ/edit#gid=0"
    ) == "abc123_-XYZ"


def test_export_writes_one_tab_per_site_and_dimension_and_overwrites(caplog) -> None:
    fake = GoogleFake(
        _visible_properties(),
        {
            ("sc-domain:anatainc.com", "query"): _rows("anata fulfillment"),
            ("sc-domain:anatainc.com", "page"): _rows("https://anatainc.com/services"),
            ("sc-domain:anatabuilding.com", "query"): _rows("arena rental"),
            ("sc-domain:anatabuilding.com", "page"): _rows("https://anatabuilding.com/"),
        },
    )
    with caplog.at_level("INFO"):
        result = _run(fake)

    assert result["ok"] is True
    assert result["client_email"] == SERVICE_ACCOUNT["client_email"]
    assert result["_scopes"] == [SEARCH_CONSOLE_SCOPE, SHEETS_SCOPE]
    assert list(EXPORT_SCOPES) == [SEARCH_CONSOLE_SCOPE, SHEETS_SCOPE]
    assert result["window_start"] == "2026-10-15"
    assert result["window_end"] == "2026-11-11"
    assert result["run_started_at"] == NOW.isoformat()
    assert result["exported_properties"] == [
        "sc-domain:anatainc.com",
        "sc-domain:anatabuilding.com",
    ]
    assert result["tabs"] == [
        "properties",
        "anatainc.com queries",
        "anatainc.com pages",
        "anatabuilding.com queries",
        "anatabuilding.com pages",
    ]
    assert result["row_counts"] == {
        "anatainc.com queries": 1,
        "anatainc.com pages": 1,
        "anatabuilding.com queries": 1,
        "anatabuilding.com pages": 1,
    }
    assert "fake-key-material" not in caplog.text
    assert "BEGIN PRIVATE KEY" not in caplog.text
    assert SERVICE_ACCOUNT["client_email"] in caplog.text

    methods = {(call["method"], call["url"].split("?")[0]) for call in fake.calls}
    assert ("PUT",) not in {(call["method"],) for call in fake.calls}
    assert not any("urlInspection" in call["url"] or "/sitemaps/" in call["url"] for call in fake.calls)
    assert not any(call["method"] in {"PUT", "DELETE", "PATCH"} for call in fake.calls)
    query_calls = [
        call for call in fake.calls if call["url"].endswith("/searchAnalytics/query")
    ]
    assert len(query_calls) == 4
    for call in query_calls:
        assert call["kwargs"]["json"]["rowLimit"] == ROW_LIMIT
        assert call["kwargs"]["json"]["startDate"] == "2026-10-15"
        assert call["kwargs"]["json"]["endDate"] == "2026-11-11"
        assert call["kwargs"]["json"]["dimensions"] in (["query"], ["page"])
        assert call["kwargs"]["headers"]["Authorization"] == "Bearer test-token"
    assert "example.com" not in " ".join(call["url"] for call in query_calls)
    assert "www.anatabuilding.com" not in " ".join(call["url"] for call in query_calls)

    deleted = [
        request["deleteSheet"]["sheetId"]
        for call in fake.calls
        if call["url"].endswith(":batchUpdate") and "requests" in call["kwargs"]["json"]
        for request in call["kwargs"]["json"]["requests"]
        if "deleteSheet" in request
    ]
    assert deleted == [0, 4]
    written = [
        item
        for call in fake.calls
        if call["url"].endswith("/values:batchUpdate")
        for item in call["kwargs"]["json"]["data"]
    ]
    by_range = {item["range"]: item["values"] for item in written}
    properties = by_range["'properties'!A1"]
    assert properties[0] == ["run_started_at", NOW.isoformat()]
    assert ["client_email", SERVICE_ACCOUNT["client_email"]] in properties
    assert [
        "sc-domain:anatainc.com",
        "siteFullUser",
        "yes",
        "",
    ] in properties
    assert [
        "https://example.com/",
        "siteUnverifiedUser",
        "no",
        "",
    ] in properties
    assert by_range["'anatainc.com queries'!A1"] == [
        ["query", "clicks", "impressions", "ctr", "position"],
        ["anata fulfillment", 4, 40, 0.1, 3.25],
    ]
    assert by_range["'anatabuilding.com pages'!A1"][1][0] == "https://anatabuilding.com/"
    assert methods


def test_configured_property_is_queried_when_sites_list_omits_it() -> None:
    fake = GoogleFake(
        [{"site_url": "https://example.com/", "permission_level": "siteOwner"}],
        {
            ("sc-domain:anatainc.com", "query"): [],
            ("sc-domain:anatainc.com", "page"): [],
        },
    )
    result = _run(fake)
    assert result["exported_properties"] == ["sc-domain:anatainc.com"]
    written = [
        item
        for call in fake.calls
        if call["url"].endswith("/values:batchUpdate")
        for item in call["kwargs"]["json"]["data"]
    ]
    properties = next(item["values"] for item in written if item["range"] == "'properties'!A1")
    assert [
        "sc-domain:anatainc.com",
        "",
        "yes",
        "Not returned by sites.list; the query was still attempted.",
    ] in properties


def test_hidden_building_property_is_not_queried() -> None:
    fake = GoogleFake(
        [{"site_url": "sc-domain:anatainc.com", "permission_level": "siteFullUser"}],
        {
            ("sc-domain:anatainc.com", "query"): [],
            ("sc-domain:anatainc.com", "page"): [],
        },
    )
    result = _run(fake)
    assert result["exported_properties"] == ["sc-domain:anatainc.com"]
    assert result["tabs"] == [
        "properties",
        "anatainc.com queries",
        "anatainc.com pages",
    ]
    assert not any("anatabuilding" in call["url"] for call in fake.calls)


def test_failed_search_console_query_does_not_write_the_sheet() -> None:
    class Failing(GoogleFake):
        def __call__(self, method: str, url: str, **kwargs: Any) -> Response:
            self.calls.append({"method": method, "url": url, "kwargs": kwargs})
            if url.endswith("/searchAnalytics/query"):
                return Response(403, {"error": {"status": "PERMISSION_DENIED"}})
            return super().__call__(method, url, **kwargs)

    fake = Failing(
        [{"site_url": "sc-domain:anatainc.com", "permission_level": "siteFullUser"}],
        {},
    )
    try:
        _run(fake)
    except SearchConsoleExportError as exc:
        message = str(exc)
    else:
        raise AssertionError("export should fail when Search Console refuses the query")
    assert "fake-key-material" not in message
    assert "sc-domain:anatainc.com" in message
    assert not any("spreadsheets" in call["url"] for call in fake.calls)


def test_missing_sheet_id_fails_before_google_and_names_the_service_account(caplog) -> None:
    fake = GoogleFake([], {})
    with caplog.at_level("INFO"), mock.patch(
        "sales_support_agent.services.search_console_export._google_access_token"
    ) as token:
        try:
            run_search_console_export(
                _settings(gsc_export_sheet_id=""),
                requester=fake,
                today=TODAY,
            )
        except SearchConsoleExportError as exc:
            message = str(exc)
        else:
            raise AssertionError("missing sheet id should fail")
    assert "GSC_EXPORT_SHEET_ID is not configured" in message
    assert SERVICE_ACCOUNT["client_email"] in message
    assert "fake-key-material" not in message
    assert "fake-key-material" not in caplog.text
    token.assert_not_called()
    assert fake.calls == []


def test_invalid_service_account_json_does_not_echo_the_document() -> None:
    try:
        run_search_console_export(
            _settings(google_service_account_json="{not-json private_key: secret"),
            requester=GoogleFake([], {}),
        )
    except SearchConsoleExportError as exc:
        message = str(exc)
    else:
        raise AssertionError("invalid credentials should fail")
    assert "not valid JSON" in message
    assert "private_key" not in message
    assert "secret" not in message


def _client() -> TestClient:
    app = FastAPI()
    app.state.settings = _settings()
    app.include_router(router)
    return TestClient(app)


def test_route_requires_the_same_job_credentials() -> None:
    response = _client().post("/api/jobs/search-console/export")
    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid internal API key."


def test_route_accepts_cron_bearer_and_internal_key_without_running_website_ops() -> None:
    with mock.patch.dict("os.environ", {"CRON_SECRET": "cron-test-key"}), mock.patch(
        "sales_support_agent.api.search_console_export_job.run_search_console_export",
        return_value={"ok": True, "client_email": SERVICE_ACCOUNT["client_email"]},
    ) as export, mock.patch(
        "sales_support_agent.services.website_ops.run_website_ops"
    ) as website_ops:
        bearer = _client().get(
            "/api/jobs/search-console/export",
            headers={"Authorization": "Bearer cron-test-key"},
        )
        internal = _client().post(
            "/api/jobs/search-console/export",
            headers={"X-Internal-Api-Key": "internal-test-key"},
        )
    assert bearer.status_code == 200
    assert internal.status_code == 200
    assert bearer.json()["client_email"] == SERVICE_ACCOUNT["client_email"]
    assert export.call_count == 2
    website_ops.assert_not_called()


def test_route_is_mounted_on_the_agent_app() -> None:
    from fastapi.routing import iter_route_contexts

    from sales_support_agent.main import app

    paths = {ctx.path for ctx in iter_route_contexts(app.routes)}
    assert "/api/jobs/search-console/export" in paths


def test_export_source_does_not_request_search_console_write_access() -> None:
    from pathlib import Path

    source = (
        Path(__file__).parents[1]
        / "sales_support_agent"
        / "services"
        / "search_console_export.py"
    ).read_text(encoding="utf-8")
    router_source = (
        Path(__file__).parents[1]
        / "sales_support_agent"
        / "api"
        / "search_console_export_job.py"
    ).read_text(encoding="utf-8")
    assert "https://www.googleapis.com/auth/webmasters\"" not in source
    assert "urlInspection" not in source
    assert "sitemaps" not in source
    assert "run_website_ops" not in source
    assert "run_website_ops" not in router_source
    assert SHEETS_SCOPE in source
    assert SEARCH_CONSOLE_SCOPE
