"""Standalone proposal lifecycle against real storage and HTTP routes."""

from copy import deepcopy
from unittest.mock import Mock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from sales_support_agent.api import fulfillment_deck_router as router
from sales_support_agent.config import load_settings
from sales_support_agent.models.entities import AutomationRun
from sales_support_agent.services.fulfillment_deck import service, storage, workflow
from sales_support_agent.services.fulfillment_deck.schema import (
    ProductSpec, ProductRates, ProspectProfile, RateMatrix, RateQuote, ZoneRates,
)


@pytest.fixture
def context(tmp_path, monkeypatch):
    engine = create_engine(f"sqlite:///{tmp_path / 'runs.db'}")
    AutomationRun.__table__.create(engine)
    monkeypatch.setattr(storage, "get_engine", lambda: engine)
    monkeypatch.setattr(router, "get_current_user", lambda request: {"email": "operator@example.test"})
    app = FastAPI()
    for dependency in router.admin_router.dependencies:
        app.dependency_overrides[dependency.dependency] = lambda: {"email": "operator@example.test"}
    app.include_router(router.admin_router)
    app.include_router(router.public_router)
    product = ProductSpec(name="Towel", length_in=6, width_in=4, height_in=2,
                          weight_lb=.15, monthly_units=1500)
    profile = ProspectProfile(brand="Test Towels", monthly_order_volume=1500, products=(product,))
    matrix = RateMatrix(origin_zip="84043", products=(ProductRates(product=product, zones=(
        ZoneRates(zone=1, dest_zip="84101", dest_label="Salt Lake City", quotes=(
            RateQuote(carrier="USPS", service="Ground", rate_usd=4, zone=1, source="wms"),
        )),
    )),))
    monkeypatch.setattr(service, "build_rate_matrix", lambda *args: (matrix, []))
    monkeypatch.setattr(service, "get_wms_client", lambda: None)
    monkeypatch.setattr(service, "_build_narrative", service._fallback_narrative)
    run_id = storage.create_run(trigger="test")
    path = f"/rate-sheets/test/{run_id}/test-token"
    summary = service._assemble(settings=load_settings(), profile=profile, origin="84043",
                                warnings=[], view_path=path, suppress_fulfillment_pricing=True)
    summary.update(view_path=path, export_token="test-token")
    storage.save_draft(run_id, summary)
    storage.publish_run(run_id)
    # All network is forbidden after fixture creation, including CRM and accidental WMS refresh.
    import requests
    monkeypatch.setattr(requests.sessions.Session, "request", Mock(side_effect=AssertionError("Unexpected network")))
    monkeypatch.setattr(service, "build_rate_matrix", Mock(side_effect=AssertionError("Unexpected rate refresh")))
    return TestClient(app), run_id, path, engine


def current(run_id):
    return dict(storage.get_run(run_id).summary_json)


def prepare(run_id):
    costs = {"pick_pack_per_order": .8, "monthly_tech_fee": 50}
    storage.update_summary(run_id, {"fulfillment_actual_costs": costs,
        "fulfillment_cost_submissions": [{"name": "Warehouse", "email": "warehouse@example.test", "costs": costs}]})
    return service.apply_profile_edits(run_id, {
        "document_kind": "fulfillment_proposal",
        "rate_overrides": {"dtc_base_per_order": 2.1, "receiving_per_pallet": 15, "dtc_additional_item": .15},
        "sales_pricing": {"fee_rows": [{"fee_key": "integration_setup_fee", "waived": True, "waiver_reason": "PRIVATE CONCESSION"}]},
    }, settings=load_settings(), expected_revision=current(run_id)["draft_revision"])


def test_conversion_save_review_publish_share_without_hubspot(context):
    client, run_id, path, _ = context
    original = client.get(path).text
    assert "Full rate card" not in original
    summary = prepare(run_id)
    assert client.get(path).text == original
    assert "Full rate card" in summary["deck_html"]
    assert "Estimated invoice" in summary["deck_html"]
    assert summary["resolved_customer_rates"]["dtc_base_per_order"] == 2.1
    assert summary["fulfillment_quote"]["one_time"][0]["amount"] == 0
    base = f"/admin/fulfillment/sales/runs/{run_id}"
    review = client.get(base + "/review")
    assert review.status_code == 200
    assert "HubSpot (optional)" in review.text
    assert "Create the HubSpot quote" not in review.text
    assert "Unpublished changes" in review.text
    revision = summary["draft_revision"]
    response = client.post(base + "/publish", data={"expected_revision": revision}, follow_redirects=False)
    assert "Review" in response.headers["location"]  # review is required
    assert client.get(path).text == original
    response = client.post(base + "/approve-pricing", data={"expected_revision": revision}, follow_redirects=False)
    assert response.status_code == 303
    assert current(run_id)["pricing_review"]["revision"] == revision
    response = client.post(base + "/publish", data={"expected_revision": revision}, follow_redirects=False)
    assert response.status_code == 303
    published = client.get(path).text
    assert published == summary["deck_html"]
    assert "PRIVATE CONCESSION" not in published
    assert "before sales margin" not in published
    assert "Ready to share" in client.get(base + "/review").text
    snapshot = current(run_id)["published_snapshot"]
    assert "fulfillment_actual_costs" not in snapshot
    assert "sales_pricing" not in snapshot
    assert "multiplier" not in snapshot["fulfillment_quote"]
    service.apply_profile_edits(run_id, {"rate_overrides": {"dtc_base_per_order": 3}}, settings=load_settings())
    assert client.get(path).text == published
    assert not current(run_id).get("pricing_review")


def test_stale_form_never_overwrites_or_publishes(context):
    client, run_id, path, _ = context
    old = current(run_id)["draft_revision"]
    prepare(run_id)
    for action in ("convert", "approve-pricing", "publish"):
        response = client.post(f"/admin/fulfillment/sales/runs/{run_id}/{action}", data={"expected_revision": old})
        assert response.status_code == 409
    before = current(run_id)
    with pytest.raises(workflow.RevisionConflict):
        service.apply_profile_edits(run_id, {"rate_overrides": {}}, settings=load_settings(), expected_revision=old)
    assert current(run_id) == before


def test_render_failure_preserves_inputs_and_live_snapshot(context, monkeypatch):
    _, run_id, _, _ = context
    before = deepcopy(current(run_id)["published_snapshot"])
    monkeypatch.setattr(service, "render_rate_sheet_html", Mock(side_effect=RuntimeError("render failed")))
    with pytest.raises(RuntimeError):
        service.apply_profile_edits(run_id, {"rate_overrides": {"dtc_base_per_order": 2.1}}, settings=load_settings())
    summary = current(run_id)
    assert summary["rate_overrides"]["dtc_base_per_order"] == 2.1
    assert summary["published_snapshot"] == before
    assert summary["rendered_revision"] != summary["draft_revision"]
    assert workflow.publication_errors(summary)


def test_cost_changes_invalidate_signature_and_review(context):
    _, run_id, _, _ = context
    summary = prepare(run_id)
    storage.approve_pricing(run_id, expected_revision=summary["draft_revision"], actor="test")
    storage.update_costs(run_id, {"pick_pack_per_order": 1.2})
    summary = current(run_id)
    assert not workflow.signed_costs_current(summary)
    assert not summary.get("pricing_review")


def test_legacy_backfill_keeps_original_live_content(context):
    _, run_id, _, engine = context
    with Session(engine) as session:
        run = session.get(AutomationRun, run_id)
        legacy = dict(run.summary_json)
        for key in ("published_snapshot", "draft_revision", "rendered_revision", "document_kind"):
            legacy.pop(key, None)
        run.summary_json = legacy
        session.commit()
    old = legacy["deck_html"]
    prepare(run_id)
    assert current(run_id)["published_snapshot"]["deck_html"] == old
    migrated = current(run_id)
    assert workflow.migrate(migrated, published=True) == migrated


def test_invalid_prices_and_optional_quote_are_safe(context):
    client, run_id, _, _ = context
    before = current(run_id)
    for value in (-1, float("nan"), float("inf")):
        with pytest.raises(ValueError):
            service.apply_profile_edits(run_id, {"rate_overrides": {"dtc_base_per_order": value}}, settings=load_settings())
        assert current(run_id) == before
    response = client.post(f"/admin/fulfillment/sales/runs/{run_id}/quote", follow_redirects=False)
    assert "temporarily" in response.headers["location"]
    assert current(run_id) == before


def test_public_requote_uses_published_prices_and_packages(context, monkeypatch):
    client, run_id, path, _ = context
    summary = prepare(run_id)
    storage.approve_pricing(run_id, expected_revision=summary["draft_revision"], actor="test")
    storage.publish_run(run_id)
    published = deepcopy(current(run_id)["published_snapshot"])
    service.apply_profile_edits(run_id, {"rate_overrides": {"dtc_base_per_order": 9}}, settings=load_settings())
    captured = []
    def quote_packages(products, origin, client):
        captured.extend(products)
        return RateMatrix.from_dict(published["rate_matrix"]), []
    monkeypatch.setattr(service, "build_rate_matrix", quote_packages)
    response = client.post(path + "/requote", json={"products": [{"name": "Towel", "length_in": 6,
        "width_in": 4, "height_in": 2, "weight_lb": .15}]})
    assert response.status_code == 200
    assert not response.json()["persisted"]
    assert "$2.10" in response.json()["fragments"]["quote"]
    assert "$9.00" not in response.json()["fragments"]["quote"]
    assert captured[0].name == "Towel"
    assert current(run_id)["published_snapshot"] == published


def test_missing_revision_and_diy_cannot_convert(context):
    client, run_id, _, _ = context
    base = f"/admin/fulfillment/sales/runs/{run_id}"
    assert client.post(base + "/convert").status_code == 422
    storage.update_summary(run_id, {"segment": "diy", "document_kind": "shipping_teaser"})
    before = current(run_id)
    response = client.post(base + "/convert", data={"expected_revision": before["draft_revision"]}, follow_redirects=False)
    assert "Shipping+OS" in response.headers["location"]
    assert current(run_id) == before
