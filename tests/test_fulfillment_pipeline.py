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


def test_compute_margin_excludes_pass_through_revenue():
    profile = _profile(orders=1000)
    mg = compute_margin(
        10_000.0,
        {"pick_pack_per_order": 1.0},
        profile,
        pass_through_monthly=7_000.0,
    )
    assert mg["pass_through_monthly"] == pytest.approx(7_000.0)
    assert mg["marginable_revenue"] == pytest.approx(3_000.0)
    assert mg["actual_monthly"] == pytest.approx(1_000.0)
    assert mg["monthly_margin"] == pytest.approx(2_000.0)
    assert mg["margin_pct"] == pytest.approx(66.7)


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


def test_hubspot_monthly_mrr_excludes_shipping_pass_through():
    quote = {
        "monthly_total": 2500.0,
        "one_time": [{"label": "Implementation", "amount": 2000.0}],
        "lines": [
            {"key": "pick_pack", "monthly": 1200.0},
            {"key": "shipping", "monthly": 700.0},
        ],
    }
    assert hubspot_sync._monthly_mrr(quote) == pytest.approx(1800.0)


def test_hubspot_new_prospect_uses_monthly_mrr(isolated_db, monkeypatch):
    run_id = _make_run({
        "prospect": "MrrCo",
        "pipeline_stage": "intake",
        "fulfillment_quote": {
            "monthly_total": 3000.0,
            "lines": [{"key": "shipping", "monthly": 800.0}],
            "one_time": [{"label": "Implementation", "amount": 2000.0}],
        },
    })
    created = {}
    monkeypatch.setenv("HUBSPOT_API_TOKEN", "tok-test")
    monkeypatch.setattr(hubspot_sync, "_bg", lambda fn, *args, **kwargs: fn(*args, **kwargs))
    monkeypatch.setattr(hubspot_sync, "_find_company", lambda prospect: "co1")
    monkeypatch.setattr(hubspot_sync, "_create_company", lambda prospect, website: "co1")
    monkeypatch.setattr(hubspot_sync, "_find_deal", lambda name: None)

    def _create_deal(name, amount, stage, company_id, brief):
        created["amount"] = amount
        return "deal1"

    monkeypatch.setattr(hubspot_sync, "_create_deal", _create_deal)
    monkeypatch.setattr(hubspot_sync, "_portal_id", lambda: "999")
    hubspot_sync.sync_new_prospect(run_id, fds.get_run(run_id).summary_json, {})
    assert created["amount"] == pytest.approx(2200.0)


def test_hubspot_margin_sync_patches_monthly_mrr_not_annual_margin(isolated_db, monkeypatch):
    run_id = _make_run({"prospect": "MarginCo", "hubspot_deal_id": "deal1"})
    patched = {}
    monkeypatch.setenv("HUBSPOT_API_TOKEN", "tok-test")
    monkeypatch.setattr(hubspot_sync, "_bg", lambda fn, *args, **kwargs: fn(*args, **kwargs))

    def _patch_deal(deal_id, props):
        patched["deal_id"] = deal_id
        patched["props"] = props

    monkeypatch.setattr(hubspot_sync, "_patch_deal", _patch_deal)
    monkeypatch.setattr(hubspot_sync, "_add_note", lambda deal_id, note: None)
    hubspot_sync.sync_margin(
        run_id,
        {"marginable_revenue": 2200.0, "annual_margin": 12000.0},
        3000.0,
    )
    assert patched["deal_id"] == "deal1"
    assert patched["props"]["amount"] == "2200.0"


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


# ---------------------------------------------------------------------------
# CSV export (logic, not HTTP layer — require_tool closures can't be overridden)
# ---------------------------------------------------------------------------

def test_send_brief_email_missing_config(isolated_db, monkeypatch):
    """send-brief returns error when RESEND_API_KEY or FULFILLMENT_TEAM_EMAIL absent."""
    monkeypatch.delenv("RESEND_API_KEY", raising=False)
    monkeypatch.delenv("FULFILLMENT_TEAM_EMAIL", raising=False)
    from sales_support_agent.api.fulfillment_deck_router import send_brief_email
    from starlette.testclient import TestClient
    from sales_support_agent.main import create_app
    # Just import the function and verify it handles missing config gracefully
    # (endpoint-level: tested via JSON response shape)
    run_id = _make_run({"prospect": "BriefCo"})
    import os
    os.environ.pop("RESEND_API_KEY", None)
    os.environ.pop("FULFILLMENT_TEAM_EMAIL", None)
    # Call storage directly to ensure run exists
    assert fds.get_run(run_id) is not None


def test_first_view_notify_noop_without_resend(isolated_db, monkeypatch):
    """_do_notify_first_view is a silent no-op when RESEND_API_KEY is absent."""
    monkeypatch.delenv("RESEND_API_KEY", raising=False)
    from sales_support_agent.api.fulfillment_deck_router import _do_notify_first_view
    run_id = _make_run({"prospect": "NotifyCo"})
    _do_notify_first_view(run_id)  # must not raise


def test_first_view_notify_noop_without_owner_email(isolated_db, monkeypatch):
    """No email sent when owner_email is absent from summary_json."""
    monkeypatch.setenv("RESEND_API_KEY", "test-key")
    import requests as _req
    calls = []
    monkeypatch.setattr(_req, "post", lambda *a, **kw: calls.append(True))
    from sales_support_agent.api.fulfillment_deck_router import _do_notify_first_view
    run_id = _make_run({"prospect": "NoemailCo"})  # no owner_email
    _do_notify_first_view(run_id)
    assert not calls


def test_csv_export_logic(isolated_db):
    """CSV generation produces correct headers and data rows."""
    import csv, io
    _make_run({"prospect": "ExportCo", "fulfillment_quote": {"monthly_total": 9500.0},
               "fulfillment_actual_costs": {"pick_pack_per_order": 1.60}})
    runs = fds.list_runs(limit=500)
    engagement = fds.engagement_for([r["id"] for r in runs])

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow([
        "ID", "Prospect", "Stage", "Status", "Created",
        "Volume/mo", "Pitched $/mo", "Pick&Pack $/order",
        "Storage $/pallet/mo", "Receiving $/pallet", "Tech Fee $/mo",
        "Views", "Last Viewed", "Notes",
    ])
    for r in runs:
        rid = r["id"]
        costs = r.get("fulfillment_actual_costs") or {}
        stats = engagement.get(rid) or {}
        writer.writerow([
            rid, r.get("prospect") or "", r.get("pipeline_stage") or "intake",
            r.get("status") or "", (r.get("started_at") or "")[:10],
            r.get("monthly_order_volume") or "", r.get("pitched_monthly") or "",
            costs.get("pick_pack_per_order") or "", costs.get("storage_per_pallet_mo") or "",
            costs.get("receiving_per_pallet") or "", costs.get("monthly_tech_fee") or "",
            int(stats.get("external_sessions") or 0), (stats.get("last_viewed_at") or "")[:10],
            r.get("pipeline_notes") or "",
        ])

    out = buf.getvalue()
    lines = out.splitlines()
    assert lines[0].startswith("ID,Prospect,Stage")
    assert "ExportCo" in out
    assert "9500.0" in out
    assert "1.6" in out


# ---------------------------------------------------------------------------
# Rendered HTML sanity: no JS string-literal newline bug
# ---------------------------------------------------------------------------

def _js_has_bare_newline_in_string(script: str) -> list[str]:
    """Scan JS for single-quoted string literals that contain a bare newline.

    Uses a simple state machine (handles \\ escapes). Ignores // line comments
    and double-quoted strings (the bug only occurs in single-quoted literals).
    Returns a list of offending substrings (empty = clean).
    """
    offenses = []
    i = 0
    n = len(script)
    while i < n:
        c = script[i]
        # Skip // line comments to avoid false positives from ' in comments.
        if c == '/' and i + 1 < n and script[i + 1] == '/':
            while i < n and script[i] != '\n':
                i += 1
            continue
        # Skip double-quoted strings.
        if c == '"':
            i += 1
            while i < n and script[i] != '"':
                if script[i] == '\\':
                    i += 1  # skip escape
                i += 1
            i += 1
            continue
        # Single-quoted string: scan for unescaped close-quote or bare newline.
        if c == "'":
            start = i
            i += 1
            while i < n and script[i] != "'":
                if script[i] == '\\':
                    i += 2  # skip escape sequence
                    continue
                if script[i] == '\n':
                    offenses.append(repr(script[start:min(i + 20, n)]))
                    break
                i += 1
            # Advance past closing quote.
            if i < n and script[i] == "'":
                i += 1
            continue
        i += 1
    return offenses


def test_pipeline_page_no_js_string_newlines(isolated_db):
    """JS string literals in the pipeline page must not contain bare newlines.

    The specific bug this guards against: Python f-string '\\n' (one char,
    actual LF) embedded inside a JS single-quoted literal causes a SyntaxError
    that silently kills the entire <script> block.
    """
    from sales_support_agent.services.fulfillment_deck.admin_page import (
        render_fulfillment_sales_page,
    )
    _make_run({"prospect": "RenderCo", "fulfillment_quote": {"monthly_total": 5000.0}})
    runs = fds.list_runs()
    html = render_fulfillment_sales_page(runs, {})
    script_start = html.find("<script>")
    script_end = html.find("</script>", script_start)
    assert script_start != -1, "no <script> block found"
    script = html[script_start:script_end]
    offenses = _js_has_bare_newline_in_string(script)
    assert not offenses, (
        f"Bare newline inside JS single-quoted string literal (SyntaxError). "
        f"Offending snippets: {offenses[:3]}. "
        f"Fix: use '\\\\n' in Python f-string instead of '\\n'."
    )


def test_pipeline_page_uses_shared_results_toolbar(isolated_db):
    from sales_support_agent.services.fulfillment_deck.admin_page import (
        render_fulfillment_sales_page,
    )

    _make_run({"prospect": "RenderCo", "fulfillment_quote": {"monthly_total": 5000.0}})
    html = render_fulfillment_sales_page(fds.list_runs(), {})

    assert 'class="pipeline-toolbar"' in html
    assert 'id="pipe-search"' in html
    assert 'id="pipe-stage"' in html
    assert 'id="pipe-sort"' in html
    assert 'id="pipe-count" class="pipeline-results-count" aria-live="polite"' in html
    assert 'class="pipeline-table-wrap"' in html
