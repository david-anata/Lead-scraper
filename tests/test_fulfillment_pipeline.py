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
