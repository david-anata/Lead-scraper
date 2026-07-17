from __future__ import annotations

from datetime import date

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from sales_support_agent.api.cashflow_router import router as cashflow_router
from sales_support_agent.services.cashflow import smart_cfo


def _rows():
    return [
        {"id": "bank-1", "source": "csv", "event_type": "outflow", "status": "posted", "vendor_or_customer": "Tool Co", "category": "software", "amount_cents": 12_000, "due_date": "2026-07-01"},
        {"id": "bank-2", "source": "csv", "event_type": "outflow", "status": "posted", "vendor_or_customer": "Tool Co", "category": "software", "amount_cents": 12_000, "due_date": "2026-06-01"},
        {"id": "invoice-1", "source": "qbo", "event_type": "inflow", "status": "planned", "vendor_or_customer": "Client A", "category": "receivable", "amount_cents": 55_000, "due_date": "2026-07-20"},
    ]


def test_packet_rolls_up_every_event_and_keeps_record_evidence():
    packet = smart_cfo.build_ledger_packet(_rows())
    assert packet["record_count"] == 3
    assert {record_id for rollup in packet["merchant_rollups"] for record_id in rollup["record_ids"]} == {"bank-1", "bank-2", "invoice-1"}
    assert {rollup["evidence_ref"] for rollup in packet["merchant_rollups"]} == {"r1", "r2"}
    assert packet["totals_cents"]["outflow:posted"] == 24_000


def test_finance_packet_contains_reconciled_cfo_analysis_not_only_rollups():
    rows = _rows() + [{
        "id": "balance", "source": "csv", "record_kind": "transaction",
        "event_type": "inflow", "status": "posted", "category": "transfer",
        "amount_cents": 0, "account_balance_cents": 200_000,
        "due_date": "2026-07-15",
    }]
    packet = smart_cfo.build_finance_packet(rows, settlement_annotations=[], as_of=date(2026, 7, 15))
    summary = packet["analytical_summary"]
    assert summary["cash"]["cash_on_hand_cents"] == 200_000
    assert summary["receivables"]["total_cents"] == 55_000
    assert summary["payables"]["total_cents"] == 0
    assert {"cash", "forecast", "receivables", "payables", "reconciliation", "trust"} <= set(summary)


def test_smart_cfo_caches_exact_ledger_analysis(monkeypatch):
    store = {}
    calls = []
    monkeypatch.setattr(smart_cfo, "list_obligations", lambda limit: _rows())
    monkeypatch.setattr(smart_cfo, "kv_get_json", lambda key: store.get(key))
    monkeypatch.setattr(smart_cfo, "kv_set_json", lambda key, value: store.__setitem__(key, value))

    monkeypatch.setenv("ANTHROPIC_API_KEY", "key")
    monkeypatch.setenv("FINANCE_SMART_CFO_MODEL", "claude-test")
    monkeypatch.setattr(smart_cfo, "_call_anthropic", lambda key, model, packet: calls.append((key, model, packet)) or {"summary": "Review Tool Co.", "recommendations": [{"category": "savings", "priority": "medium", "title": "Review Tool Co", "reason": "Two posted software charges recur.", "next_action": "Confirm owner and renewal date.", "operator_question": "Is this still used?", "evidence_refs": ["r2"]}]})
    settings = object()
    first = smart_cfo.run_smart_cfo(settings)
    second = smart_cfo.run_smart_cfo(settings)
    assert first["status"] == "ready"
    assert first["recommendations"][0]["record_ids"] == ["bank-1", "bank-2"]
    assert second["cached"] is True
    assert len(calls) == 1
    assert calls[0][1] == "claude-test"


def test_unsupported_llm_evidence_is_removed(monkeypatch):
    packet = smart_cfo.build_ledger_packet(_rows())
    result = smart_cfo._validate_analysis({"summary": "x", "recommendations": [{"category": "savings", "priority": "high", "title": "Bad", "reason": "x", "next_action": "x", "operator_question": "x", "evidence_refs": ["invented"]}]}, packet)
    assert result["recommendations"][0]["category"] == "cash_risk"
    assert set(result["recommendations"][0]["record_ids"]) <= {"bank-1", "bank-2", "invoice-1"}


def test_missing_key_does_not_call_llm(monkeypatch):
    monkeypatch.setattr(smart_cfo, "list_obligations", lambda limit: _rows())
    monkeypatch.setattr(smart_cfo, "kv_get_json", lambda key: None)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    result = smart_cfo.run_smart_cfo(object())
    assert result["status"] == "not_configured"


def test_response_parser_accepts_json_wrapped_in_a_code_fence():
    value = smart_cfo._parse_response_json("```json\n{\"summary\": \"x\", \"recommendations\": []}\n```")
    assert value["summary"] == "x"


def test_response_extractor_prefers_anthropic_tool_use_input():
    class ToolBlock:
        type = "tool_use"
        name = "submit_finance_advice"
        input = {"summary": "x", "recommendations": []}

    class Message:
        content = [ToolBlock()]

    assert smart_cfo._extract_response_value(Message())["summary"] == "x"


def test_structured_advice_requires_an_evidence_backed_action():
    schema = smart_cfo._schema()
    assert schema["properties"]["recommendations"]["minItems"] == 1
    assert schema["properties"]["recommendations"]["items"]["properties"]["evidence_refs"]["minItems"] == 1


def test_llm_packet_uses_compact_evidence_references_only():
    llm_packet = smart_cfo._llm_packet(smart_cfo.build_ledger_packet(_rows()))
    assert "record_ids" not in llm_packet["merchant_rollups"][0]
    assert llm_packet["merchant_rollups"][0]["evidence_ref"] == "r1"


def test_provider_error_is_safe_and_does_not_expose_provider_detail(monkeypatch):
    class BrokenMessages:
        def create(self, **kwargs):
            raise RuntimeError("provider detail")

    class BrokenClient:
        messages = BrokenMessages()

    class BrokenAnthropic:
        @staticmethod
        def Anthropic(api_key):
            return BrokenClient()

    monkeypatch.setitem(__import__("sys").modules, "anthropic", BrokenAnthropic)
    with pytest.raises(smart_cfo.SmartCfoProviderError, match="Anthropic Smart CFO request failed"):
        smart_cfo._call_anthropic("key", "claude-test", {"merchant_rollups": []})


def test_smart_review_route_is_advisory_and_reports_ledger_scope(monkeypatch):
    app = FastAPI()
    app.state.settings = type("Settings", (), {"admin_session_secret": "test", "admin_cookie_name": "admin", "admin_session_ttl_hours": 1})()
    app.include_router(cashflow_router)
    monkeypatch.setattr("sales_support_agent.services.auth_deps.get_session_user_from_request", lambda request: {"email": "qa@example.com"})
    monkeypatch.setattr("sales_support_agent.services.auth_deps.get_current_user", lambda request: {"email": "qa@example.com", "permissions": {"finance"}})
    monkeypatch.setattr("sales_support_agent.services.cashflow.smart_cfo.run_smart_cfo", lambda *args, **kwargs: {"status": "ready", "record_count": 42, "cached": False})
    response = TestClient(app, follow_redirects=False).post("/admin/finances/smart-review")
    assert response.status_code == 303
    assert "Smart%20review%20completed%20across%2042" in response.headers["location"]


def test_qbo_actuals_refresh_is_visible_without_replacing_csv_cash_truth(monkeypatch):
    app = FastAPI()
    app.state.settings = type("Settings", (), {"admin_session_secret": "test", "admin_cookie_name": "admin", "admin_session_ttl_hours": 1})()
    app.include_router(cashflow_router)
    monkeypatch.setattr("sales_support_agent.services.auth_deps.get_session_user_from_request", lambda request: {"email": "qa@example.com"})
    monkeypatch.setattr("sales_support_agent.services.auth_deps.get_current_user", lambda request: {"email": "qa@example.com", "permissions": {"finance"}})
    result = type("Result", (), {"rows_inserted": 12, "rows_skipped_duplicate": 8, "errors": []})()
    called = {}
    monkeypatch.setattr(
        "sales_support_agent.api.cashflow_router.sync_qbo_bank_transactions",
        lambda settings, lookback_days: called.update({"lookback_days": lookback_days}) or result,
    )
    response = TestClient(app, follow_redirects=False).post("/admin/finances/sync-qbo-actuals")
    assert response.status_code == 303
    assert called["lookback_days"] == 365
    assert "QuickBooks%20actuals%20refreshed%3A%2012%20imported" in response.headers["location"]
