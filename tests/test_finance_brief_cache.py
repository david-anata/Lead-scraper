"""Guards for the shared read-only Finance brief cache."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

from sales_support_agent.api import cashflow_router


def _request() -> SimpleNamespace:
    state = SimpleNamespace(
        agent_settings=SimpleNamespace(plaid_environment="production"),
        settings=SimpleNamespace(plaid_environment="production"),
        finance_brief_cache=None,
    )
    return SimpleNamespace(app=SimpleNamespace(state=state))


def test_brief_cache_reuses_one_source_build_and_reports_hit(monkeypatch) -> None:
    request = _request()
    calls: list[object] = []
    expected = object()

    def fake_load(settings):
        calls.append(settings)
        return expected

    monkeypatch.setattr(cashflow_router, "load_finance_brief", fake_load)

    first, first_hit = asyncio.run(
        cashflow_router._load_request_finance_brief(request)
    )
    second, second_hit = asyncio.run(
        cashflow_router._load_request_finance_brief(request)
    )

    assert first is expected
    assert second is expected
    assert first_hit is False
    assert second_hit is True
    assert len(calls) == 1


def test_finance_write_invalidation_forces_a_fresh_source_build(monkeypatch) -> None:
    request = _request()
    builds = iter(("first", "second"))
    monkeypatch.setattr(cashflow_router, "load_finance_brief", lambda _settings: next(builds))

    first, _ = asyncio.run(cashflow_router._load_request_finance_brief(request))
    cashflow_router.clear_finance_brief_cache(request.app)
    second, cache_hit = asyncio.run(
        cashflow_router._load_request_finance_brief(request)
    )

    assert first == "first"
    assert second == "second"
    assert cache_hit is False


def test_cash_plan_response_exposes_cache_hit_for_operations(monkeypatch) -> None:
    request = _request()
    monkeypatch.setattr(cashflow_router, "load_finance_brief", lambda _settings: "brief")
    monkeypatch.setattr(cashflow_router, "render_cash_plan_page", lambda _brief: "ok")

    first = asyncio.run(cashflow_router.finance_cash_plan(request))
    second = asyncio.run(cashflow_router.finance_cash_plan(request))

    assert first.headers["X-Finance-Brief-Cache"] == "miss"
    assert second.headers["X-Finance-Brief-Cache"] == "hit"
