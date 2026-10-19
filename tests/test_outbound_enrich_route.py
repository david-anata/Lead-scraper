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
