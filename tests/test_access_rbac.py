"""RBAC phase 1 — permission resolution + per-tool enforcement.

Uses a temp SQLite DB (SALES_AGENT_DB_URL) and the real backend app, minting
signed session cookies to exercise the authorization middleware end-to-end.
"""

from __future__ import annotations

import os
import tempfile
import unittest

os.environ.setdefault("SALES_AGENT_DB_URL", "sqlite:///" + tempfile.gettempdir() + "/rbac_test.db")

try:
    from fastapi.testclient import TestClient
    from sales_support_agent.main import app
    from sales_support_agent.services.access import store
    from sales_support_agent.services.access.catalog import ALL_TOOL_KEYS, valid_keys
    from sales_support_agent.services.admin_auth import create_user_session_token
    DEPS = True
except ModuleNotFoundError as exc:
    if exc.name not in {"sqlalchemy", "fastapi"}:
        raise
    DEPS = False


def _settings():
    return app.state.agent_settings


def _cookie_for(email: str, name: str = "User", role: str = "member"):
    s = _settings()
    token = create_user_session_token(s, email=email, name=name, role=role)
    return s.admin_cookie_name, token


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class CatalogTests(unittest.TestCase):
    def test_catalog_nonempty_and_filters_junk(self) -> None:
        self.assertIn("finance", ALL_TOOL_KEYS)
        self.assertIn("executive.brand_analysis", ALL_TOOL_KEYS)
        self.assertEqual(valid_keys(["finance", "bogus.key", "advertising.audit"]),
                         ["finance", "advertising.audit"])


def _role_id(name: str, perms: list) -> str:
    """Get-or-create a role by name (the temp DB persists across tests/runs)."""
    existing = store.get_role_by_name(name)
    if existing:
        return existing["id"]
    return store.create_role(name, perms, description="")


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class StoreTests(unittest.TestCase):
    def test_superadmin_seeded(self) -> None:
        u = store.get_user_by_email("david@anatainc.com")
        self.assertIsNotNone(u)
        self.assertTrue(u["is_superadmin"])
        self.assertEqual(u["permissions"], set(ALL_TOOL_KEYS))

    def test_role_assignment_resolves_permissions(self) -> None:
        rid = _role_id("Finance Only", ["finance"])
        store.upsert_user("fin1@anatainc.com", "Fin", role_id=rid)
        u = store.get_user_by_email("fin1@anatainc.com")
        self.assertEqual(u["permissions"], {"finance"})
        self.assertFalse(u["is_superadmin"])

    def test_suspended_user_has_no_permissions(self) -> None:
        rid = _role_id("Ops", ["website_ops.seo"])
        uid = store.upsert_user("ops1@anatainc.com", "Ops", role_id=rid)
        store.set_user_status(uid, "suspended")
        access = store.resolve_access("ops1@anatainc.com")
        self.assertEqual(access["status"], "suspended")
        self.assertEqual(access["permissions"], set())

    def test_role_delete_blocked_while_assigned(self) -> None:
        rid = _role_id("Temp", ["finance"])
        store.upsert_user("temp_user@anatainc.com", role_id=rid)
        self.assertFalse(store.delete_role(rid))  # assigned -> blocked


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class EnforcementTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(app)
        # A finance-only user (idempotent — the temp DB persists across tests).
        existing = store.get_role_by_name("FinanceOnlyEnf")
        self.fin_role = existing["id"] if existing else store.create_role("FinanceOnlyEnf", ["finance"], description="")
        store.upsert_user("enf_fin@anatainc.com", "Fin", role_id=self.fin_role)

    def _get(self, path, email):
        name, token = _cookie_for(email)
        self.client.cookies.set(name, token)
        try:
            return self.client.get(path, follow_redirects=False)
        finally:
            self.client.cookies.clear()

    def test_unauthenticated_redirects_to_login(self) -> None:
        r = self.client.get("/admin/executive/brand-analysis", follow_redirects=False)
        self.assertEqual(r.status_code, 302)
        self.assertEqual(r.headers.get("location"), "/admin/login")

    def test_superadmin_allowed_everywhere(self) -> None:
        r = self._get("/admin/executive/brand-analysis", "david@anatainc.com")
        self.assertEqual(r.status_code, 200)

    def test_forbidden_tool_returns_403_page(self) -> None:
        # finance-only user cannot reach brand analysis
        r = self._get("/admin/executive/brand-analysis", "enf_fin@anatainc.com")
        self.assertEqual(r.status_code, 403)
        self.assertIn("No access", r.text)

    def test_permitted_tool_allowed(self) -> None:
        # finance-only user CAN reach the advertising tool? No — only finance.
        r_ok = self._get("/admin/finances", "enf_fin@anatainc.com")
        self.assertNotIn(r_ok.status_code, (302, 403))  # allowed through the gate
        r_no = self._get("/admin/advertising/audit", "enf_fin@anatainc.com")
        self.assertEqual(r_no.status_code, 403)

    def test_unprovisioned_user_denied(self) -> None:
        r = self._get("/admin/executive/brand-analysis", "stranger@anatainc.com")
        self.assertEqual(r.status_code, 403)

    def test_bypass_paths_not_tool_gated(self) -> None:
        # QBO OAuth + login must not be redirected to login *by the RBAC gate*.
        r = self.client.get("/admin/login", follow_redirects=False)
        self.assertNotEqual(r.headers.get("location"), "/admin/login")


if __name__ == "__main__":
    unittest.main()
