"""The dry-run contact test route: refuses without a key, never pushes."""

import asyncio
from unittest.mock import MagicMock

from sales_support_agent.api import outbound_router as r


def _request(limit="5"):
    req = MagicMock()

    async def form():
        return {"limit": limit}
    req.form = form
    return req


def test_refuses_without_key(monkeypatch):
    monkeypatch.delenv("PROSPEO_API_KEY", raising=False)
    r._ENRICH_TEST.update(running=False)
    resp = asyncio.run(r.outbound_enrich_test(_request()))
    assert resp.status_code == 400


def test_starts_when_key_present_and_clamps_limit(monkeypatch):
    monkeypatch.setenv("PROSPEO_API_KEY", "k" * 20)
    r._ENRICH_TEST.update(running=False)
    resp = asyncio.run(r.outbound_enrich_test(_request("999")))
    assert resp.status_code == 200 and b'"started":50' in resp.body.replace(b" ", b"")
    r._ENRICH_TEST.update(running=False)


def test_second_start_while_running_is_409(monkeypatch):
    monkeypatch.setenv("PROSPEO_API_KEY", "k" * 20)
    r._ENRICH_TEST.update(running=True)
    resp = asyncio.run(r.outbound_enrich_test(_request()))
    assert resp.status_code == 409
    r._ENRICH_TEST.update(running=False)


def test_vercel_runs_inline_and_returns_report(monkeypatch):
    import sales_support_agent.api.outbound_router as r
    monkeypatch.setenv("VERCEL", "1")
    monkeypatch.setenv("PROSPEO_API_KEY", "k" * 20)
    ran = []

    def fake(limit):
        ran.append(limit)
        r._ENRICH_TEST.update(report={"companies_tried": limit}, error="", running=False)

    monkeypatch.setattr(r, "_run_enrich_test", fake)
    r._ENRICH_TEST.update(running=False)
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    app = FastAPI()
    app.include_router(r.router)
    resp = TestClient(app).post("/admin/api/outbound/enrich-test", data={"limit": "7"})
    assert ran == [7]
    assert resp.json()["report"]["companies_tried"] == 7
