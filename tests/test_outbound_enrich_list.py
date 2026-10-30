"""POST /api/admin/outbound/enrich-list: internal key, dry run, redo_partner tag."""

from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

import outbound_contacts as oc
import outbound_enrich as oe
import outbound_openers as openers
from sales_support_agent.api import outbound_router as router_mod


def _client(monkeypatch, engine=None):
    app = FastAPI()
    app.state.settings = SimpleNamespace(internal_api_key="internal-test-key")
    app.include_router(router_mod.router)
    if engine is not None:
        monkeypatch.setattr("sales_support_agent.models.database.get_engine", lambda: engine)
    return TestClient(app)


def test_enrich_list_requires_the_internal_key(monkeypatch):
    client = _client(monkeypatch)
    missing = client.post("/api/admin/outbound/enrich-list", json={"domains": ["a.com"]})
    wrong = client.post(
        "/api/admin/outbound/enrich-list",
        json={"domains": ["a.com"]},
        headers={"X-Internal-Api-Key": "nope"},
    )
    assert missing.status_code == 401 and wrong.status_code == 401
    assert missing.json()["detail"] == "Invalid internal API key."


def test_enrich_list_rejects_an_unset_server_key(monkeypatch):
    app = FastAPI()
    app.state.settings = SimpleNamespace(internal_api_key="")
    app.include_router(router_mod.router)
    response = TestClient(app).post(
        "/api/admin/outbound/enrich-list",
        json={"domains": ["a.com"]},
        headers={"X-Internal-Api-Key": "internal-test-key"},
    )
    assert response.status_code == 401


def test_enrich_list_dry_run_is_the_default_and_does_not_search(monkeypatch):
    engine = create_engine("sqlite://")
    calls = []

    def find(domain, *, api_key, max_contacts=2, **kw):
        calls.append(domain)
        return oc.FindResult(domain=domain)

    monkeypatch.setattr(oc, "find_contacts", find)
    client = _client(monkeypatch, engine)
    response = client.post(
        "/api/admin/outbound/enrich-list",
        json={"domains": ["https://www.Example.com/shop", "other.com"]},
        headers={"X-Internal-Api-Key": "internal-test-key"},
    )
    body = response.json()
    assert response.status_code == 200
    assert body["dry_run"] is True and body["recipe"] == "redo_partner"
    assert body["domains"] == ["example.com", "other.com"]
    assert calls == []


def test_enrich_list_live_stores_redo_partner_and_does_not_push(monkeypatch):
    # The route runs enrichment on a worker thread. A plain memory database
    # would be a different database on that thread.
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool,
    )
    pushed = []

    def find(domain, *, api_key, max_contacts=2, **kw):
        return oc.FindResult(domain=domain, credits_used=1, contacts=[
            oc.Contact(domain=domain, first_name="A", last_name="B", email=f"a@{domain}", title="Founder")])

    def refuse_push(*args, **kwargs):
        pushed.append(1)
        raise AssertionError("enrich-list must not push")

    monkeypatch.setenv("PROSPEO_API_KEY", "k" * 16)
    monkeypatch.setattr(oc, "find_contacts", find)
    monkeypatch.setattr(openers, "write_opener", lambda lead, contact=None, **kw: ("A plain line.", "fallback"))
    monkeypatch.setattr(oe, "push_pending", refuse_push)
    client = _client(monkeypatch, engine)
    response = client.post(
        "/api/admin/outbound/enrich-list",
        json={"domains": "Redo.com\nsecond.com", "dry_run": False},
        headers={"X-Internal-Api-Key": "internal-test-key"},
    )
    body = response.json()
    assert response.status_code == 200 and body["dry_run"] is False
    assert body["contacts_found"] == 2 and pushed == []
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT domain, recipe FROM outbound_enriched_contacts ORDER BY domain"
        )).fetchall()
    assert rows == [("redo.com", "redo_partner"), ("second.com", "redo_partner")]


def test_enrich_list_live_without_a_prospeo_key_is_400(monkeypatch):
    monkeypatch.delenv("PROSPEO_API_KEY", raising=False)
    client = _client(monkeypatch)
    response = client.post(
        "/api/admin/outbound/enrich-list",
        json={"domains": ["a.com"], "dry_run": False},
        headers={"X-Internal-Api-Key": "internal-test-key"},
    )
    assert response.status_code == 400
    assert "PROSPEO_API_KEY" in response.json()["reason"]
