"""The Calendar route must not build a second, weaker rent forecast."""

from __future__ import annotations

import asyncio

from sales_support_agent.api import cashflow_router
from sales_support_agent.services.cashflow import obligations, rent_paydown


def test_calendar_route_passes_the_exact_shared_snapshot_to_rent(monkeypatch):
    ledger = [{"id": "one"}]
    snapshot = {
        "status": "ready",
        "calculation_id": "shared-1",
        "days": [],
        "weeks": [],
        "totals": {},
    }
    seen = {}

    monkeypatch.setattr(obligations, "list_obligations", lambda **_kwargs: ledger)
    monkeypatch.setattr(cashflow_router, "load_cash_calendar", lambda **_kwargs: snapshot)

    def load_plan(**kwargs):
        seen.update(kwargs)
        return {"status": "nothing_spare", "calculation_id": "shared-1", "instalments": []}

    monkeypatch.setattr(rent_paydown, "load_paydown_plan", load_plan)
    monkeypatch.setattr(
        cashflow_router,
        "render_cash_calendar_page",
        lambda calendar, **_kwargs: str(calendar.get("calculation_id")),
    )

    response = asyncio.run(cashflow_router.finance_cash_calendar(object()))

    assert seen["calendar"] is snapshot
    assert seen["rows"] is ledger
    assert response.body == b"shared-1"
