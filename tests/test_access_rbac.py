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


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class AccessUITests(unittest.TestCase):
    """Phase 2 — /admin/access users + roles pages."""

    def setUp(self) -> None:
        self.client = TestClient(app)
        # Superadmin cookie
        self.sa_name, self.sa_token = _cookie_for("david@anatainc.com", "David", "admin")
        # Finance-only user (no access.manage)
        self.fin_role = _role_id("FinanceOnlyUI", ["finance"])
        store.upsert_user("ui_fin@anatainc.com", "FinUI", role_id=self.fin_role)
        self.fin_name, self.fin_token = _cookie_for("ui_fin@anatainc.com", "FinUI")

    def _get(self, path, token_pair=None):
        if token_pair:
            name, token = token_pair
            self.client.cookies.set(name, token)
        try:
            return self.client.get(path, follow_redirects=False)
        finally:
            self.client.cookies.clear()

    def _post(self, path, data, token_pair=None):
        if token_pair:
            name, token = token_pair
            self.client.cookies.set(name, token)
        try:
            return self.client.post(path, data=data, follow_redirects=False)
        finally:
            self.client.cookies.clear()

    def test_access_page_requires_access_manage(self) -> None:
        # Finance user cannot reach /admin/access
        r = self._get("/admin/access", (self.fin_name, self.fin_token))
        self.assertEqual(r.status_code, 403)

    def test_superadmin_can_view_users_page(self) -> None:
        r = self._get("/admin/access", (self.sa_name, self.sa_token))
        self.assertEqual(r.status_code, 200)
        self.assertIn("Users", r.text)

    def test_superadmin_can_view_roles_page(self) -> None:
        r = self._get("/admin/access/roles", (self.sa_name, self.sa_token))
        self.assertEqual(r.status_code, 200)
        self.assertIn("Roles", r.text)

    def test_create_and_delete_role(self) -> None:
        # Create a fresh role through the API
        r = self._post("/admin/access/roles/new",
                       {"name": "UITestRole", "description": "test", "permissions": ["finance"]},
                       (self.sa_name, self.sa_token))
        self.assertEqual(r.status_code, 303)
        self.assertIn("/admin/access/roles", r.headers.get("location", ""))
        # Role exists in store
        role = store.get_role_by_name("UITestRole")
        self.assertIsNotNone(role)
        self.assertIn("finance", role["permissions"])
        # Delete it
        r2 = self._post(f"/admin/access/roles/{role['id']}/delete", {},
                        (self.sa_name, self.sa_token))
        self.assertEqual(r2.status_code, 303)
        self.assertIsNone(store.get_role_by_name("UITestRole"))

    def test_duplicate_role_name_returns_422(self) -> None:
        _role_id("DupRole", ["finance"])
        r = self._post("/admin/access/roles/new",
                       {"name": "DupRole", "description": "", "permissions": []},
                       (self.sa_name, self.sa_token))
        self.assertEqual(r.status_code, 422)

    def test_assign_role_via_post(self) -> None:
        # Create a test user and assign a role via POST
        test_uid = store.upsert_user("assign_test@anatainc.com", "AssignTest")
        rid = _role_id("AssignTestRole", ["finance"])
        r = self._post(f"/admin/access/users/{test_uid}/role",
                       {"role_id": rid}, (self.sa_name, self.sa_token))
        self.assertEqual(r.status_code, 303)
        u = store.get_user_by_email("assign_test@anatainc.com")
        self.assertEqual(u["role_id"], rid)

    def test_suspend_and_activate_user(self) -> None:
        uid = store.upsert_user("suspend_test@anatainc.com", "SuspendTest")
        # Suspend
        r = self._post(f"/admin/access/users/{uid}/status",
                       {"action": "suspend"}, (self.sa_name, self.sa_token))
        self.assertEqual(r.status_code, 303)
        u = store.get_user_by_email("suspend_test@anatainc.com")
        self.assertEqual(u["status"], "suspended")
        # Activate
        r2 = self._post(f"/admin/access/users/{uid}/status",
                        {"action": "activate"}, (self.sa_name, self.sa_token))
        self.assertEqual(r2.status_code, 303)
        u2 = store.get_user_by_email("suspend_test@anatainc.com")
        self.assertEqual(u2["status"], "active")


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class InviteRequestTests(unittest.TestCase):
    """Phase 3 — invite flow + access request flow."""

    def setUp(self) -> None:
        self.client = TestClient(app)
        self.sa_name, self.sa_token = _cookie_for("david@anatainc.com", "David", "admin")

    def _get(self, path, token_pair=None):
        if token_pair:
            name, token = token_pair
            self.client.cookies.set(name, token)
        try:
            return self.client.get(path, follow_redirects=False)
        finally:
            self.client.cookies.clear()

    def _post(self, path, data, token_pair=None):
        if token_pair:
            name, token = token_pair
            self.client.cookies.set(name, token)
        try:
            return self.client.post(path, data=data, follow_redirects=False)
        finally:
            self.client.cookies.clear()

    def test_invites_page_renders(self) -> None:
        r = self._get("/admin/access/invites", (self.sa_name, self.sa_token))
        self.assertEqual(r.status_code, 200)
        self.assertIn("Invites", r.text)

    def test_requests_page_renders(self) -> None:
        r = self._get("/admin/access/requests", (self.sa_name, self.sa_token))
        self.assertEqual(r.status_code, 200)
        self.assertIn("Access Requests", r.text)

    def test_create_invite_returns_link_page(self) -> None:
        rid = _role_id("InvTestRole", ["finance"])
        r = self._post("/admin/access/invites/new",
                       {"email": "invitee@anatainc.com", "role_id": rid},
                       (self.sa_name, self.sa_token))
        self.assertEqual(r.status_code, 200)
        self.assertIn("Invite created", r.text)
        self.assertIn("invitee@anatainc.com", r.text)
        # Token link should appear on the page
        self.assertIn("/admin/access/invite/", r.text)

    def test_invite_stored_in_db(self) -> None:
        rid = _role_id("InvTestRole2", ["advertising.audit"])
        self._post("/admin/access/invites/new",
                   {"email": "invitee2@anatainc.com", "role_id": rid},
                   (self.sa_name, self.sa_token))
        invites = store.list_pending_invites()
        emails = [i["email"] for i in invites]
        self.assertIn("invitee2@anatainc.com", emails)

    def test_invite_landing_invalid_token(self) -> None:
        r = self._get("/admin/access/invite/totally-bogus-token-xyz")
        self.assertEqual(r.status_code, 410)
        self.assertIn("Invalid invite", r.text)

    def test_invite_landing_valid_token_redirects(self) -> None:
        import secrets as _sec
        token = _sec.token_urlsafe(32)
        store.create_invite("bounce_test@anatainc.com", None, token=token, invited_by="david@anatainc.com")
        r = self._get(f"/admin/access/invite/{token}")
        self.assertEqual(r.status_code, 302)
        self.assertIn("/admin/auth/google", r.headers.get("location", ""))
        # Cookie should be set
        self.assertIn("pending_invite", r.headers.get("set-cookie", ""))

    def test_access_request_flow(self) -> None:
        # Directly test the store round-trip: create request → approve → user provisioned
        rid = _role_id("ReqApprovalRole", ["finance"])
        req_id = store.create_access_request("req_test@anatainc.com", "ReqTest")
        self.assertIsNotNone(req_id)
        # Pending
        pending = store.list_access_requests(status="pending")
        self.assertTrue(any(r["email"] == "req_test@anatainc.com" for r in pending))
        # Approve
        email_out = store.decide_access_request(req_id, approve=True, role_id=rid, decided_by="david@anatainc.com")
        self.assertEqual(email_out, "req_test@anatainc.com")
        # User now provisioned
        u = store.get_user_by_email("req_test@anatainc.com")
        self.assertIsNotNone(u)
        self.assertEqual(u["role_id"], rid)

    def test_deny_access_request(self) -> None:
        req_id = store.create_access_request("deny_test@anatainc.com", "DenyTest")
        result = store.decide_access_request(req_id, approve=False, decided_by="david@anatainc.com")
        self.assertIsNone(result)
        # Denied requests not in pending
        pending = store.list_access_requests(status="pending")
        self.assertFalse(any(r["email"] == "deny_test@anatainc.com" for r in pending))

    def test_approve_request_via_post(self) -> None:
        rid = _role_id("PostApproveRole", ["finance"])
        req_id = store.create_access_request("post_approve@anatainc.com", "PostApprove")
        r = self._post(f"/admin/access/requests/{req_id}/approve",
                       {"role_id": rid}, (self.sa_name, self.sa_token))
        self.assertEqual(r.status_code, 303)
        u = store.get_user_by_email("post_approve@anatainc.com")
        self.assertIsNotNone(u)
        self.assertEqual(u["role_id"], rid)


if __name__ == "__main__":
    unittest.main()
