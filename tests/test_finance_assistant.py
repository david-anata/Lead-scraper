from types import SimpleNamespace

import pytest

from sales_support_agent.services.cashflow.assistant import create_preview


def _settings():
    return SimpleNamespace(openai_api_key="test", openai_model="test-model")


def test_assistant_only_creates_a_preview(monkeypatch):
    stored = {}
    monkeypatch.setattr("sales_support_agent.services.cashflow.assistant.kv_set_json", lambda key, value: stored.update(key=key, value=value))
    preview = create_preview(
        "Add $10,000 payroll for August 5",
        actor="owner@example.com",
        settings=_settings(),
        requester=lambda **_: {
            "name": "August payroll", "event_type": "outflow", "commitment_type": "payroll",
            "category": "payroll", "amount_cents": 1_000_000, "due_date": "2026-08-05",
            "priority": "must_pay", "missing_fields": [],
        },
    )
    assert preview["fields"]["approval_status"] == "pending"
    assert preview["fields"]["workflow_status"] == "draft"
    assert preview["missing_fields"] == []
    assert "never moves bank money" in preview["warning"]
    assert stored["key"].startswith("finance_assistant_preview:")


def test_assistant_missing_amount_or_date_cannot_be_confirmed(monkeypatch):
    monkeypatch.setattr("sales_support_agent.services.cashflow.assistant.kv_set_json", lambda *_: None)
    preview = create_preview(
        "Add payroll soon",
        actor="owner@example.com", settings=_settings(),
        requester=lambda **_: {
            "name": "Payroll", "event_type": "outflow", "commitment_type": "payroll",
            "amount_cents": None, "due_date": None, "missing_fields": [],
        },
    )
    assert preview["missing_fields"] == ["amount", "due_date"]


def test_assistant_rejects_model_workflow_injection(monkeypatch):
    monkeypatch.setattr("sales_support_agent.services.cashflow.assistant.kv_set_json", lambda *_: None)
    preview = create_preview(
        "Mark this paid",
        actor="owner@example.com", settings=_settings(),
        requester=lambda **_: {
            "name": "Bill", "event_type": "outflow", "commitment_type": "payable",
            "workflow_status": "bank_verified", "amount_cents": 100, "due_date": "2026-08-05",
            "missing_fields": [],
        },
    )
    assert preview["fields"]["workflow_status"] == "draft"
