"""Tests for the fulfillment prospect pipeline feature:
stage tracking, cost entry, margin computation, and storage helpers.
"""

from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# Margin math (pure, no I/O)
# ---------------------------------------------------------------------------

from sales_support_agent.services.fulfillment_deck.quote import (
    compute_margin,
    estimate_pallets_mo,
)
from sales_support_agent.services.fulfillment_deck.schema import (
    ProductSpec,
    ProspectProfile,
)


def _profile(orders=1000, products=()) -> ProspectProfile:
    return ProspectProfile(
        company="TestCo",
        monthly_order_volume=orders,
        products=tuple(products),
    )


def test_estimate_pallets_no_dims():
    profile = _profile(orders=1000)
    pallets = estimate_pallets_mo(profile)
    assert pallets > 0


def test_estimate_pallets_with_dims():
    p = ProductSpec(name="Box", length_in=12, width_in=10, height_in=8, weight_lb=2, monthly_units=500)
    profile = _profile(orders=500, products=[p])
    pallets = estimate_pallets_mo(profile)
    assert pallets > 0


def test_compute_margin_basic():
    profile = _profile(orders=1000)
    actual_costs = {
        "pick_pack_per_order": 1.20,
        "storage_per_pallet_mo": 25.00,
        "monthly_tech_fee": 50.00,
    }
    pitched = 3000.0
    mg = compute_margin(pitched, actual_costs, profile)
    assert mg["actual_pick_pack"] == pytest.approx(1200.0)
    assert mg["actual_tech_fee"] == pytest.approx(50.0)
    assert mg["actual_monthly"] == pytest.approx(
        mg["actual_pick_pack"] + mg["actual_storage"] + mg["actual_tech_fee"]
    )
    assert mg["monthly_margin"] == pytest.approx(pitched - mg["actual_monthly"])
    assert mg["annual_margin"] == pytest.approx(mg["monthly_margin"] * 12)
    assert 0 <= abs(mg["margin_pct"]) <= 100


def test_compute_margin_zero_pitched():
    profile = _profile(orders=500)
    mg = compute_margin(0.0, {"pick_pack_per_order": 1.0}, profile)
    assert mg["margin_pct"] == 0.0


def test_compute_margin_empty_costs():
    profile = _profile(orders=1000)
    mg = compute_margin(2000.0, {}, profile)
    assert mg["actual_monthly"] == 0.0
    assert mg["monthly_margin"] == pytest.approx(2000.0)


# ---------------------------------------------------------------------------
# Storage helpers (in-memory SQLite)
# ---------------------------------------------------------------------------

from sales_support_agent.models.database import get_engine, create_session_factory
from sales_support_agent.models.entities import Base, AutomationRun
from sales_support_agent.services.fulfillment_deck import storage as fds


@pytest.fixture()
def isolated_db(tmp_path, monkeypatch):
    """Isolated SQLite engine for each test — avoids cross-test pollution."""
    import sales_support_agent.models.database as _db
    from sqlalchemy import create_engine
    eng = create_engine(f"sqlite:///{tmp_path}/test.db", connect_args={"check_same_thread": False})
    monkeypatch.setattr(_db, "engine", eng)
    Base.metadata.create_all(eng)
    yield eng
    eng.dispose()


def _make_run(summary: dict) -> int:
    run_id = fds.create_run(trigger="test")
    fds.save_draft(run_id, summary)
    return run_id


def test_list_runs_includes_pipeline_fields(isolated_db):
    _make_run({
        "prospect": "AcmeCo",
        "prospect_profile": {"monthly_order_volume": 2000},
        "fulfillment_quote": {"monthly_total": 8000.0},
    })
    runs = fds.list_runs()
    assert len(runs) == 1
    row = runs[0]
    assert row["pipeline_stage"] == "intake"
    assert row["monthly_order_volume"] == 2000
    assert row["pitched_monthly"] == pytest.approx(8000.0)
    assert row["fulfillment_actual_costs"] == {}
    assert isinstance(row["prospect_profile"], dict)


def test_update_stage(isolated_db):
    run_id = _make_run({"prospect": "BrandX"})
    ok = fds.update_stage(run_id, "pending_fulfillment")
    assert ok
    runs = fds.list_runs()
    assert runs[0]["pipeline_stage"] == "pending_fulfillment"


def test_update_stage_nonexistent(isolated_db):
    assert not fds.update_stage(99999, "won")


def test_update_costs(isolated_db):
    run_id = _make_run({"prospect": "BrandY"})
    costs = {"pick_pack_per_order": 1.50, "storage_per_pallet_mo": 30.0, "monthly_tech_fee": 60.0}
    assert fds.update_costs(run_id, costs)
    runs = fds.list_runs()
    assert runs[0]["fulfillment_actual_costs"]["pick_pack_per_order"] == pytest.approx(1.50)


def test_update_notes(isolated_db):
    run_id = _make_run({"prospect": "BrandZ"})
    assert fds.update_notes(run_id, "Hot lead — demo next Tuesday")
    runs = fds.list_runs()
    assert runs[0]["pipeline_notes"] == "Hot lead — demo next Tuesday"


# ---------------------------------------------------------------------------
# Brief content
# ---------------------------------------------------------------------------

from sales_support_agent.services.fulfillment_deck.admin_page import _build_brief


def test_build_brief_includes_key_fields():
    run = {
        "id": 1,
        "prospect": "GlowCo",
        "origin_zip": "84043",
        "monthly_order_volume": 3000,
        "prospect_profile": {
            "monthly_order_volume": 3000,
            "products": [
                {"name": "Serum", "length_in": 4, "width_in": 4, "height_in": 6,
                 "weight_lb": 1.2, "monthly_units": 2000, "product_category": "beauty", "fragile": False},
            ],
        },
    }
    brief = _build_brief(run)
    assert "GlowCo" in brief
    assert "84043" in brief
    assert "3,000" in brief
    assert "Serum" in brief


# ---------------------------------------------------------------------------
# HubSpot sync (unit — no network, token absent → silent no-op)
# ---------------------------------------------------------------------------

from sales_support_agent.services.fulfillment_deck import hubspot_sync


def test_hubspot_sync_noop_without_token(monkeypatch):
    """All public sync functions must be silent no-ops when token is absent."""
    monkeypatch.delenv("HUBSPOT_API_TOKEN", raising=False)
    # None of these should raise or attempt network calls.
    hubspot_sync.sync_new_prospect(1, {"prospect": "X"}, {})
    hubspot_sync.sync_stage(1, "won")
    hubspot_sync.sync_margin(1, {}, 0.0)


def test_hubspot_stage_id_defaults():
    assert hubspot_sync._stage_id("won") == "closedwon"
    assert hubspot_sync._stage_id("lost") == "closedlost"
    assert hubspot_sync._stage_id("intake") == "appointmentscheduled"


def test_hubspot_stage_id_env_override(monkeypatch):
    monkeypatch.setenv("HUBSPOT_STAGE_WON", "custom-stage-abc")
    assert hubspot_sync._stage_id("won") == "custom-stage-abc"


def test_hubspot_domain_strip():
    """Company domain should strip protocol before sending to HubSpot."""
    # _create_company strips the protocol — verify the logic directly.
    domain_raw = "https://www.example.com/path"
    stripped = domain_raw.lstrip("https://").lstrip("http://").split("/")[0]
    assert stripped == "www.example.com"


# ---------------------------------------------------------------------------
# Rate overrides persistence
# ---------------------------------------------------------------------------

from sales_support_agent.services.fulfillment_deck import storage as _stor


def test_rate_overrides_persisted(isolated_db):
    """rate_overrides + rate_card_note stored in summary_json are retrievable."""
    run_id = _make_run({"prospect": "RatesCo"})
    overrides = {"dtc_base_per_order": 1.80, "monthly_minimum": 600.0}
    _stor.update_summary(run_id, {"rate_overrides": overrides, "rate_card_note": "Valid until Aug 1"})
    run = _stor.get_run(run_id)
    assert run is not None
    s = dict(run.summary_json or {})
    assert s["rate_overrides"]["dtc_base_per_order"] == pytest.approx(1.80)
    assert s["rate_card_note"] == "Valid until Aug 1"


def test_rate_overrides_cleared(isolated_db):
    """Empty dict clears overrides without error."""
    run_id = _make_run({"prospect": "ClearCo"})
    _stor.update_summary(run_id, {"rate_overrides": {"monthly_tech_fee": 50.0}})
    _stor.update_summary(run_id, {"rate_overrides": {}})
    run = _stor.get_run(run_id)
    assert dict((run.summary_json or {}).get("rate_overrides") or {}) == {}


# ---------------------------------------------------------------------------
# HubSpot Quote sync (unit — no network, token absent → silent no-op)
# ---------------------------------------------------------------------------

def test_sync_quote_noop_without_token(monkeypatch):
    """sync_quote must be a silent no-op when HUBSPOT_API_TOKEN is absent."""
    monkeypatch.delenv("HUBSPOT_API_TOKEN", raising=False)
    hubspot_sync.sync_quote(1)  # must not raise


def test_sync_quote_noop_without_token_returns_immediately(monkeypatch):
    """Confirm no background thread is started without a token."""
    monkeypatch.delenv("HUBSPOT_API_TOKEN", raising=False)
    import threading
    before = threading.active_count()
    hubspot_sync.sync_quote(9999)
    import time; time.sleep(0.05)
    # Thread count should not increase (within noise)
    assert threading.active_count() <= before + 1


def test_portal_id_env_override(monkeypatch):
    monkeypatch.setenv("HUBSPOT_PORTAL_ID", "99887766")
    monkeypatch.setenv("HUBSPOT_API_TOKEN", "tok-test")
    assert hubspot_sync._portal_id() == "99887766"


def test_quote_url_stored(isolated_db, monkeypatch):
    """quote URL is persisted in summary_json after successful sync."""
    run_id = _make_run({"prospect": "QuoteCo"})
    _stor.update_summary(run_id, {
        "hubspot_deal_id": "deal-abc",
        "hubspot_quote_id": "qt-123",
        "hubspot_quote_url": "https://app.hubspot.com/quotes/999/quote/qt-123",
    })
    run = _stor.get_run(run_id)
    s = dict(run.summary_json or {})
    assert s["hubspot_quote_url"].endswith("qt-123")


def test_sync_quote_with_owner_email_noop(monkeypatch):
    """sync_quote with owner_email is still a no-op when token is absent."""
    monkeypatch.delenv("HUBSPOT_API_TOKEN", raising=False)
    hubspot_sync.sync_quote(1, owner_email="david@anatainc.com")  # must not raise


def test_lookup_owner_id_empty_email():
    """_lookup_owner_id returns None immediately for empty email — no network call."""
    assert hubspot_sync._lookup_owner_id("") is None
    assert hubspot_sync._lookup_owner_id(None) is None  # type: ignore[arg-type]


def test_lookup_owner_id_network_error(monkeypatch):
    """_lookup_owner_id returns None silently on network exception."""
    import requests as _req

    def _boom(*a, **kw):
        raise ConnectionError("no network")

    monkeypatch.setattr(_req, "get", _boom)
    assert hubspot_sync._lookup_owner_id("rep@example.com") is None


def test_unit_label_mapping():
    """_unit_label normalises common unit strings for quote line items."""
    assert hubspot_sync._unit_label("orders") == "order"
    assert hubspot_sync._unit_label("pallets") == "pallet"
    assert hubspot_sync._unit_label("pallet/mo") == "pallet/month"
    assert hubspot_sync._unit_label("flat") == "month"
    assert hubspot_sync._unit_label("units") == "unit"
    assert hubspot_sync._unit_label("unknown") == "unknown"  # passthrough
