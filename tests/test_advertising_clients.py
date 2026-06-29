"""Tests for Advertising > Clients: per-client goals isolation, run tagging,
the intake pre-fill map, and the HTTP routes (in-memory SQLite + TestClient)."""

from __future__ import annotations

import tempfile
import unittest

from tests.test_advertising_audit import (
    _bootstrap_db,
    _make_engine,
    _patch_global_engine,
)


class _Base(unittest.TestCase):
    def setUp(self):
        self.engine = _make_engine()
        _bootstrap_db(self.engine)
        self.old_engine = _patch_global_engine(self.engine)
        self.tmpdir = tempfile.mkdtemp()
        import sales_support_agent.services.advertising.storage as storage
        self.storage = storage
        self._old_bulk_dir = storage.BULK_RUNS_DIR
        storage.BULK_RUNS_DIR = self.tmpdir

    def tearDown(self):
        self.storage.BULK_RUNS_DIR = self._old_bulk_dir
        _patch_global_engine(self.old_engine)
        self.engine.dispose()


class ClientStorageTest(_Base):
    def test_client_crud(self):
        cid = self.storage.create_client("Alpha", objectives="grow fast")
        names = [c["name"] for c in self.storage.list_clients()]
        self.assertIn("Alpha", names)
        got = self.storage.get_client(cid)
        self.assertEqual(got["objectives"], "grow fast")

        self.storage.update_client(cid, name="Alpha Co", objectives="profit now")
        got = self.storage.get_client(cid)
        self.assertEqual(got["name"], "Alpha Co")
        self.assertEqual(got["objectives"], "profit now")

        self.storage.archive_client(cid)
        self.assertNotIn(cid, [c["id"] for c in self.storage.list_clients()])
        self.assertIn(cid, [c["id"] for c in self.storage.list_clients(include_archived=True)])

    def test_per_client_goals_are_isolated(self):
        from sales_support_agent.services.advertising.schema import Goals
        a = self.storage.create_client("Alpha")
        b = self.storage.create_client("Beta")
        self.storage.save_goals(Goals(revenue_target_cents=100000, acos_target_bps=3000), client_id=a)
        self.storage.save_goals(Goals(revenue_target_cents=500000, acos_target_bps=2000), client_id=b)

        self.assertEqual(self.storage.get_active_goals(client_id=a).revenue_target_cents, 100000)
        self.assertEqual(self.storage.get_active_goals(client_id=b).revenue_target_cents, 500000)
        # The global (no-client) set is untouched by per-client saves.
        self.assertIsNone(self.storage.get_active_goals())

        # Tweaks "save back" to the client without disturbing the other.
        self.storage.save_goals(Goals(revenue_target_cents=111111), client_id=a)
        self.assertEqual(self.storage.get_active_goals(client_id=a).revenue_target_cents, 111111)
        self.assertEqual(self.storage.get_active_goals(client_id=b).revenue_target_cents, 500000)

    def test_runs_are_tagged_and_filtered_by_client(self):
        a = self.storage.create_client("Alpha")
        b = self.storage.create_client("Beta")
        r1 = self.storage.create_run(label="r1", client_id=a)
        self.storage.create_run(label="r2", client_id=b)
        self.storage.create_run(label="adhoc")  # no client
        self.assertEqual([r["id"] for r in self.storage.list_runs(client_id=a)], [r1])
        self.assertEqual(len(self.storage.list_runs()), 3)
        self.assertEqual(self.storage.get_run(r1)["client_id"], a)

    def test_client_goals_map(self):
        from sales_support_agent.services.advertising.schema import Goals
        a = self.storage.create_client("Alpha")
        b = self.storage.create_client("Beta")
        self.storage.save_goals(Goals(revenue_target_cents=100000), client_id=a)
        self.storage.save_goals(Goals(revenue_target_cents=500000), client_id=b)
        m = self.storage.get_client_goals_map()
        self.assertEqual(set(m.keys()), {a, b})
        self.assertEqual(m[a]["revenue_target_cents"], 100000)


class ClientPageRenderTest(_Base):
    def test_clients_page_renders_accordion(self):
        from sales_support_agent.services.advertising.clients_page import render_clients_page
        from sales_support_agent.services.advertising.schema import Goals
        clients = [{
            "id": "c1", "name": "Alpha", "objectives": "grow", "status": "active",
            "goals": Goals(revenue_target_cents=100000, acos_target_bps=3000), "runs": [],
        }]
        html = render_clients_page(clients, user={"email": "x"})
        self.assertIn("Alpha", html)
        self.assertIn("client-acc", html)
        self.assertIn("/admin/advertising/clients/c1", html)
        self.assertIn("Add a client", html)
        self.assertIn("1000.00", html)  # goal pre-filled in the edit form

    def test_intake_form_has_client_dropdown_and_prefill(self):
        from sales_support_agent.services.advertising.audit_page import render_audit_page
        from sales_support_agent.services.advertising.schema import Goals
        clients = [{"id": "c1", "name": "Alpha"}]
        cgmap = {"c1": Goals(revenue_target_cents=100000, acos_target_bps=3000).to_dict()}
        html = render_audit_page(goals=None, runs=[], clients=clients, client_goals_map=cgmap, user=None)
        self.assertIn("adv-client", html)
        self.assertIn("No client (ad-hoc)", html)
        self.assertIn("__advClientGoals", html)
        self.assertIn("1000.00", html)  # display-ready prefill embedded

    def test_profit_calculator_host_page_embeds_isolated_runtime(self):
        from sales_support_agent.services.advertising.profit_calculator_page import render_profit_calculator_host_page

        html = render_profit_calculator_host_page(
            app_src="/amazon-profit-calculator/runtime",
            user={"email": "x"},
        )
        self.assertIn("Profit Calculator", html)
        self.assertIn('<iframe', html)
        self.assertIn('/amazon-profit-calculator/runtime', html)

    def test_profit_calculator_app_uses_local_proxy_base(self):
        from sales_support_agent.services.advertising.profit_calculator_page import render_profit_calculator_app_page

        html = render_profit_calculator_app_page(api_base="/api/public/amazon-profit-calculator")
        self.assertIn('data-apc-root', html)
        self.assertIn('data-api-base="/api/public/amazon-profit-calculator"', html)
        self.assertIn('data-action="lookup"', html)
        self.assertIn('/catalog/" + encodeURIComponent(asin)', html)
        self.assertIn('"/profitability/estimate"', html)
        self.assertNotIn('/api/public/amazon/catalog/', html)
        self.assertIn('/amazon-bulk-profitability/runtime', html)
        self.assertIn('Open Bulk Planner', html)

    def test_bulk_profitability_host_page_embeds_isolated_runtime(self):
        from sales_support_agent.services.advertising.bulk_profitability_page import render_bulk_profitability_host_page

        html = render_bulk_profitability_host_page(
            app_src="/amazon-bulk-profitability/runtime",
            user={"email": "x"},
        )
        self.assertIn("Bulk Planner", html)
        self.assertIn('<iframe', html)
        self.assertIn('/amazon-bulk-profitability/runtime', html)

    def test_bulk_profitability_app_uses_local_proxy_base(self):
        from sales_support_agent.services.advertising.bulk_profitability_page import render_bulk_profitability_app_page

        html = render_bulk_profitability_app_page(api_base="/api/public/amazon-bulk-profitability")
        self.assertIn('data-amazon-bulk-upload', html)
        self.assertIn('data-api-base="/api/public/amazon-bulk-profitability"', html)
        self.assertIn('data-action="run"', html)


def _make_test_client(*, is_superadmin: bool = True):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from types import SimpleNamespace
    from sales_support_agent.api import advertising_router as ar
    app = FastAPI()
    app.include_router(ar.router)
    app.include_router(ar.public_router)
    app.state.settings = SimpleNamespace(amazon_profit_api_base_url="https://profit.test")
    app.dependency_overrides[ar.router.dependencies[0].dependency] = lambda: {
        "email": "test@anatainc.com", "is_superadmin": is_superadmin, "permissions": {"advertising.audit"},
    }
    return TestClient(app)


class ClientHttpTest(_Base):
    def _client(self):
        return _make_test_client()

    def test_clients_page_ok(self):
        cid = self.storage.create_client("Zantrex", objectives="profit")
        client = self._client()
        resp = client.get("/admin/advertising/clients")
        self.assertEqual(resp.status_code, 200)
        self.assertIn("Zantrex", resp.text)
        self.assertIn(f"/admin/advertising/clients/{cid}", resp.text)

    def test_create_client_via_http(self):
        client = self._client()
        resp = client.post(
            "/admin/advertising/clients/new",
            data={"name": "NewCo", "objectives": "scale"},
            follow_redirects=False,
        )
        self.assertEqual(resp.status_code, 303)
        self.assertIn("NewCo", [c["name"] for c in self.storage.list_clients()])

    def test_save_client_goals_via_http(self):
        cid = self.storage.create_client("Alpha")
        client = self._client()
        resp = client.post(
            f"/admin/advertising/clients/{cid}",
            data={"name": "Alpha", "objectives": "grow", "revenue_target": "1000",
                  "acos_target": "30", "period": "monthly"},
            follow_redirects=False,
        )
        self.assertEqual(resp.status_code, 303)
        g = self.storage.get_active_goals(client_id=cid)
        self.assertEqual(g.revenue_target_cents, 100000)
        self.assertEqual(g.acos_target_bps, 3000)

    def test_run_for_client_tags_run_and_saves_goals(self):
        from tests.test_advertising_audit import _BUSINESS_CSV
        cid = self.storage.create_client("Alpha")
        client = self._client()
        resp = client.post(
            "/admin/advertising/audit/run",
            data={"client_id": cid, "label": "wk", "revenue_target": "2000", "acos_target": "25"},
            files={"business_report_csv": ("br.csv", _BUSINESS_CSV, "text/csv")},
            follow_redirects=False,
        )
        self.assertEqual(resp.status_code, 303)
        runs = self.storage.list_runs(client_id=cid)
        self.assertEqual(len(runs), 1)
        # Goals tweaked on the run were saved back to the client.
        self.assertEqual(self.storage.get_active_goals(client_id=cid).revenue_target_cents, 200000)

    def test_audit_page_shows_client_dropdown(self):
        self.storage.create_client("Alpha")
        client = self._client()
        resp = client.get("/admin/advertising/audit")
        self.assertEqual(resp.status_code, 200)
        self.assertIn("adv-client", resp.text)
        self.assertIn("Alpha", resp.text)

    def test_archive_client_via_http(self):
        cid = self.storage.create_client("ToRemove")
        client = self._client()
        resp = client.post(
            f"/admin/advertising/clients/{cid}/archive",
            follow_redirects=False,
        )
        self.assertEqual(resp.status_code, 303)
        self.assertNotIn(cid, [c["id"] for c in self.storage.list_clients()])
        # Archived clients are still retrievable with include_archived=True.
        self.assertIn(cid, [c["id"] for c in self.storage.list_clients(include_archived=True)])

    def test_archive_unknown_client_redirects_gracefully(self):
        client = self._client()
        resp = client.post(
            "/admin/advertising/clients/no-such-id/archive",
            follow_redirects=False,
        )
        self.assertEqual(resp.status_code, 303)
        self.assertIn("not+found", resp.headers.get("location", "").lower())

    def test_clients_page_has_archive_button(self):
        self.storage.create_client("Beta")
        client = self._client()
        resp = client.get("/admin/advertising/clients")
        self.assertEqual(resp.status_code, 200)
        self.assertIn("archive", resp.text.lower())

    def test_profit_calculator_page_renders(self):
        client = self._client()
        resp = client.get("/admin/advertising/profit-calculator")
        self.assertEqual(resp.status_code, 200)
        self.assertIn("Profit Calculator", resp.text)
        self.assertIn("/amazon-profit-calculator/runtime", resp.text)

    def test_profit_calculator_page_requires_superadmin(self):
        client = _make_test_client(is_superadmin=False)
        resp = client.get("/admin/advertising/profit-calculator")
        self.assertEqual(resp.status_code, 403)
        self.assertIn("Super-admin only", resp.text)

    def test_profit_calculator_app_renders(self):
        client = self._client()
        resp = client.get("/amazon-profit-calculator/runtime")
        self.assertEqual(resp.status_code, 200)
        self.assertIn("Amazon profit calculator", resp.text)
        self.assertIn('data-api-base="/api/public/amazon-profit-calculator"', resp.text)

    def test_bulk_profitability_page_renders(self):
        client = self._client()
        resp = client.get("/admin/advertising/bulk-profitability")
        self.assertEqual(resp.status_code, 200)
        self.assertIn("Bulk Planner", resp.text)
        self.assertIn("/amazon-bulk-profitability/runtime", resp.text)

    def test_bulk_profitability_page_requires_superadmin(self):
        client = _make_test_client(is_superadmin=False)
        resp = client.get("/admin/advertising/bulk-profitability")
        self.assertEqual(resp.status_code, 403)
        self.assertIn("Super-admin only", resp.text)

    def test_bulk_profitability_app_renders(self):
        client = self._client()
        resp = client.get("/amazon-bulk-profitability/runtime")
        self.assertEqual(resp.status_code, 200)
        self.assertIn("Bulk ASIN Profitability Upload", resp.text)
        self.assertIn('data-api-base="/api/public/amazon-bulk-profitability"', resp.text)

    def test_profit_calculator_catalog_proxy(self):
        from sales_support_agent.api import advertising_router as ar

        class _Resp:
            status_code = 200
            ok = True

            def json(self):
                return {"asin": "B08N5WRWNW", "title": "Sample", "images": []}

        original_get = ar.requests.get
        ar.requests.get = lambda *args, **kwargs: _Resp()
        try:
            client = self._client()
            resp = client.get("/api/public/amazon-profit-calculator/catalog/B08N5WRWNW")
        finally:
            ar.requests.get = original_get

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["asin"], "B08N5WRWNW")

    def test_profit_calculator_catalog_proxy_uses_agent_settings_fallback(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from types import SimpleNamespace
        from sales_support_agent.api import advertising_router as ar

        class _Resp:
            status_code = 200
            ok = True

            def json(self):
                return {"asin": "B08N5WRWNW", "title": "Agent Settings Fallback", "images": []}

        original_get = ar.requests.get
        ar.requests.get = lambda *args, **kwargs: _Resp()
        try:
            app = FastAPI()
            app.include_router(ar.public_router)
            app.state.settings = SimpleNamespace()
            app.state.agent_settings = SimpleNamespace(amazon_profit_api_base_url="https://profit.test")
            client = TestClient(app)
            resp = client.get("/api/public/amazon-profit-calculator/catalog/B08N5WRWNW")
        finally:
            ar.requests.get = original_get

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["asin"], "B08N5WRWNW")

    def test_profit_calculator_estimate_proxy(self):
        from sales_support_agent.api import advertising_router as ar

        class _Resp:
            status_code = 200
            ok = True

            def json(self):
                return {"net_profit": 12.34, "net_margin_pct": 18.2}

        original_post = ar.requests.post
        ar.requests.post = lambda *args, **kwargs: _Resp()
        try:
            client = self._client()
            resp = client.post(
                "/api/public/amazon-profit-calculator/profitability/estimate",
                json={"price": 49.99},
            )
        finally:
            ar.requests.post = original_post

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["net_profit"], 12.34)

    def test_bulk_profitability_catalog_proxy(self):
        from sales_support_agent.api import advertising_router as ar

        class _Resp:
            status_code = 200
            ok = True

            def json(self):
                return {"asin": "B08N5WRWNW", "title": "Sample Bulk", "images": []}

        original_get = ar.requests.get
        ar.requests.get = lambda *args, **kwargs: _Resp()
        try:
            client = self._client()
            resp = client.get("/api/public/amazon-bulk-profitability/catalog/B08N5WRWNW")
        finally:
            ar.requests.get = original_get

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["asin"], "B08N5WRWNW")

    def test_bulk_profitability_estimate_proxy(self):
        from sales_support_agent.api import advertising_router as ar

        class _Resp:
            status_code = 200
            ok = True

            def json(self):
                return {"net_profit": 22.15, "net_margin_pct": 25.4}

        original_post = ar.requests.post
        ar.requests.post = lambda *args, **kwargs: _Resp()
        try:
            client = self._client()
            resp = client.post(
                "/api/public/amazon-bulk-profitability/profitability/estimate",
                json={"price": 39.99},
            )
        finally:
            ar.requests.post = original_post

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["net_profit"], 22.15)

    def test_public_runtime_is_accessible_without_admin_auth(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from types import SimpleNamespace
        from sales_support_agent.api import advertising_router as ar

        app = FastAPI()
        app.include_router(ar.public_router)
        app.state.settings = SimpleNamespace(amazon_profit_api_base_url="https://profit.test")
        client = TestClient(app)

        resp = client.get("/amazon-profit-calculator/runtime")
        self.assertEqual(resp.status_code, 200)
        self.assertIn("Amazon profit calculator", resp.text)

    def test_bulk_public_runtime_is_accessible_without_admin_auth(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from types import SimpleNamespace
        from sales_support_agent.api import advertising_router as ar

        app = FastAPI()
        app.include_router(ar.public_router)
        app.state.settings = SimpleNamespace(amazon_profit_api_base_url="https://profit.test")
        client = TestClient(app)

        resp = client.get("/amazon-bulk-profitability/runtime")
        self.assertEqual(resp.status_code, 200)
        self.assertIn("Bulk ASIN Profitability Upload", resp.text)


class BrandMismatchGateTest(_Base):
    """Phase B — block/confirm before running when the uploaded files' detected
    brand doesn't match what this client has audited before."""

    def _client(self):
        return _make_test_client()

    def _seed_client_with_brand(self, brand: str) -> str:
        cid = self.storage.create_client("Alpha")
        rid = self.storage.create_run(label="prev", client_id=cid)
        self.storage.finalize_run(rid, status="complete", summary={"brand": brand})
        return cid

    def _post_run(self, client, cid):
        from tests.test_advertising_audit import _BUSINESS_CSV
        return client.post(
            "/admin/advertising/audit/run",
            data={"client_id": cid, "label": "wk"},
            files={"business_report_csv": ("br.csv", _BUSINESS_CSV, "text/csv")},
            follow_redirects=False,
        )

    def test_mismatch_blocks_with_confirm_page(self):
        # Business Report detects "Widget"; client's history is "Zantrex" -> block.
        cid = self._seed_client_with_brand("Zantrex")
        resp = self._post_run(self._client(), cid)
        self.assertEqual(resp.status_code, 200)
        self.assertIn("mismatch", resp.text.lower())
        self.assertIn("Run anyway", resp.text)
        self.assertIn("Widget", resp.text)
        self.assertIn("Zantrex", resp.text)
        # Nothing ran — only the seeded prior run exists.
        self.assertEqual(len(self.storage.list_runs(client_id=cid)), 1)

    def test_confirm_runs_the_audit(self):
        import re
        cid = self._seed_client_with_brand("Zantrex")
        client = self._client()
        resp = self._post_run(client, cid)
        token = re.search(r'name="confirm_token" value="([^"]+)"', resp.text).group(1)
        resp2 = client.post(
            "/admin/advertising/audit/run/confirm",
            data={"confirm_token": token}, follow_redirects=False,
        )
        self.assertEqual(resp2.status_code, 303)
        self.assertEqual(len(self.storage.list_runs(client_id=cid)), 2)  # seeded + confirmed

    def test_matching_brand_runs_without_gate(self):
        cid = self._seed_client_with_brand("Widget")  # matches detected brand
        resp = self._post_run(self._client(), cid)
        self.assertEqual(resp.status_code, 303)
        self.assertEqual(len(self.storage.list_runs(client_id=cid)), 2)

    def test_new_client_with_no_history_skips_gate(self):
        cid = self.storage.create_client("Fresh")  # no prior runs
        resp = self._post_run(self._client(), cid)
        self.assertEqual(resp.status_code, 303)
        self.assertEqual(len(self.storage.list_runs(client_id=cid)), 1)


if __name__ == "__main__":
    unittest.main()
