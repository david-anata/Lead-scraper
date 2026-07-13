"""Single-service merge: the root main.py app now hosts the sales_support_agent
backend routers in-process (one Render web service serves agent.anatainc.com).

Proves the three invariants the consolidation depends on:

1. Route topology — the two backend routers are mounted, so the three cron
   endpoints and the /api/admin/* data endpoints exist on the root app; and
   FastAPI first-match resolves inline admin pages to main.* while the
   formerly-proxied same-path routes (/decks/*, digital-shelf) resolve to the
   in-process backend handlers.
2. The old same-path deck proxy was removed (no self-referential loop once
   SALES_SUPPORT_AGENT_URL points at this service).
3. The internal-key guards on all three cron endpoints FAIL CLOSED when a key
   is configured — a request with no/incorrect X-Internal-Api-Key gets 401.
   This is the security-critical contract: startup() sets app.state.settings to
   the sales_support_agent Settings so the guards read a real internal_api_key.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

try:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    import main  # noqa: E402  (imports the merged root app)
    from sales_support_agent.api.router import router as core_api_router
    from sales_support_agent.api.sales_jobs_router import router as sales_jobs_router
    DEPS = True
except ModuleNotFoundError as exc:  # pragma: no cover - env guard
    if exc.name not in {"fastapi", "sqlalchemy", "requests"}:
        raise
    DEPS = False


CRON_ENDPOINTS = [
    "/api/jobs/stale-leads/run",   # sales-support-stale-scan
    "/api/jobs/gmail-sync/run",    # sales-support-gmail-sync
    "/api/jobs/sales-operator/run",  # sales-support-operator-review
]


def _route_map(app):
    out: dict[str, object] = {}
    for route in app.routes:
        path = getattr(route, "path", None)
        if path and path not in out:  # first registration wins (FastAPI match order)
            out[path] = getattr(route, "endpoint", None)
    return out


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy + requests required")
class RouteTopologyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.routes = _route_map(main.app)

    def test_cron_endpoints_are_mounted(self):
        for path in CRON_ENDPOINTS:
            self.assertIn(path, self.routes, f"{path} should be mounted on the merged app")

    def test_data_endpoints_are_mounted(self):
        for path in ("/api/admin/dashboard-data", "/api/admin/executive-data"):
            self.assertIn(path, self.routes)

    def test_inline_admin_pages_win_first_match(self):
        # The live UX must not flip to the backend's parallel page copies.
        for path in ("/admin", "/admin/login", "/health", "/"):
            ep = self.routes.get(path)
            self.assertIsNotNone(ep, f"{path} missing")
            self.assertEqual(
                ep.__module__, "main",
                f"{path} must resolve to the inline root handler, not {ep.__module__}",
            )

    def test_same_path_routes_serve_from_backend_in_process(self):
        # These had inline proxies removed; the mounted backend handler serves.
        for path in (
            "/decks/{deck_slug}/{run_id}/{token}",
            "/admin/api/digital-shelf/generate-deck",
        ):
            ep = self.routes.get(path)
            self.assertIsNotNone(ep, f"{path} missing")
            self.assertTrue(
                ep.__module__.startswith("sales_support_agent"),
                f"{path} should be served in-process by the backend, got {ep.__module__}",
            )

    def test_old_deck_proxy_removed(self):
        # The self-referential proxy would infinite-loop once the URL is self.
        self.assertFalse(hasattr(main, "public_deck_proxy"))
        self.assertFalse(hasattr(main, "_proxy_deck_subpath"))


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy + requests required")
class CronAuthFailClosedTests(unittest.TestCase):
    """Mount the two backend routers on a minimal app whose app.state mirrors
    exactly what the merged startup() provides (settings carrying a real
    internal_api_key), and prove the guards reject unauthenticated calls."""

    @classmethod
    def setUpClass(cls):
        app = FastAPI()
        app.state.settings = SimpleNamespace(
            internal_api_key="test-internal-key",
            dashboard_auto_sync_enabled=False,
            dashboard_auto_sync_max_age_minutes=30,
        )
        app.state.session_factory = None
        app.state.dashboard_sync_lock = None
        app.state.dashboard_sync_future = None
        app.include_router(core_api_router)
        app.include_router(sales_jobs_router)
        # We assert the AUTH outcome, not the job outcome — let a downstream
        # error surface as a 500 response (against the stub state) rather than
        # be re-raised, so a passed auth gate reads as "not 401".
        cls.client = TestClient(app, raise_server_exceptions=False)

    def test_all_cron_endpoints_reject_missing_key(self):
        for path in CRON_ENDPOINTS:
            resp = self.client.post(path, json={"dry_run": True})
            self.assertEqual(
                resp.status_code, 401,
                f"{path} must 401 without X-Internal-Api-Key (got {resp.status_code})",
            )

    def test_all_cron_endpoints_reject_wrong_key(self):
        for path in CRON_ENDPOINTS:
            resp = self.client.post(
                path,
                json={"dry_run": True},
                headers={"X-Internal-Api-Key": "wrong-key"},
            )
            self.assertEqual(
                resp.status_code, 401,
                f"{path} must 401 with a wrong key (got {resp.status_code})",
            )

    def test_correct_key_passes_auth_gate(self):
        # With the right key the guard must let the request THROUGH (it may then
        # fail downstream on the stubbed session_factory, but it must not 401).
        for path in CRON_ENDPOINTS:
            resp = self.client.post(
                path,
                json={"dry_run": True},
                headers={"X-Internal-Api-Key": "test-internal-key"},
            )
            self.assertNotEqual(
                resp.status_code, 401,
                f"{path} rejected the correct key (got {resp.status_code})",
            )


if __name__ == "__main__":
    unittest.main()
