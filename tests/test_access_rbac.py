"""RBAC phase 1 — permission resolution + per-tool enforcement.

Uses a temp SQLite DB (SALES_AGENT_DB_URL) and the real backend app, minting
signed session cookies to exercise the authorization middleware end-to-end.
"""

from __future__ import annotations

import os
import tempfile
import unittest
import uuid
from types import SimpleNamespace

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

    def test_per_user_permissions_resolve(self) -> None:
        uid = store.upsert_user("fin1@anatainc.com", "Fin")
        store.set_user_permissions(uid, ["finance", "not.a.real.key"])  # invalid filtered
        u = store.get_user_by_email("fin1@anatainc.com")
        self.assertEqual(u["permissions"], {"finance"})
        self.assertFalse(u["is_superadmin"])

    def test_suspended_user_has_no_permissions(self) -> None:
        uid = store.upsert_user("ops1@anatainc.com", "Ops")
        store.set_user_permissions(uid, ["website_ops.seo"])
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
        uid = store.upsert_user("enf_fin@anatainc.com", "Fin")
        store.set_user_permissions(uid, ["finance"])  # per-person grant (roles removed)

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

    def test_website_ops_run_api_requires_seo_permission(self) -> None:
        name, token = _cookie_for("enf_fin@anatainc.com")
        self.client.cookies.set(name, token)
        try:
            r = self.client.post("/admin/api/website-ops/run", data={"mode": "daily"}, follow_redirects=False)
        finally:
            self.client.cookies.clear()
        self.assertEqual(r.status_code, 403)

    def test_website_ops_feedback_api_stamps_reporter_from_session(self) -> None:
        from unittest import mock

        uid = store.upsert_user("website_queue@anatainc.com", "Website Queue")
        store.set_user_permissions(uid, ["website_ops.queue"])
        name, token = _cookie_for("website_queue@anatainc.com", "Website Queue")
        self.client.cookies.set(name, token)
        try:
            with mock.patch(
                "sales_support_agent.api.router.save_feedback_record",
                return_value={"feedback_id": "feedback-123"},
            ) as save_feedback:
                r = self.client.post(
                    "/admin/api/website-ops/feedback",
                    data={"summary": "Check hero copy"},
                    follow_redirects=False,
                )
        finally:
            self.client.cookies.clear()
        self.assertEqual(r.status_code, 302)
        payload = save_feedback.call_args.args[1]
        self.assertEqual(payload["reporter_email"], "website_queue@anatainc.com")
        self.assertEqual(payload["reporter_name"], "Website Queue")

    def test_website_ops_review_api_passes_reviewer_identity(self) -> None:
        from unittest import mock

        uid = store.upsert_user("website_reviewer@anatainc.com", "Website Reviewer")
        store.set_user_permissions(uid, ["website_ops.queue"])
        name, token = _cookie_for("website_reviewer@anatainc.com", "Website Reviewer")
        result = type("Result", (), {"ok": True, "record": {"feedback_id": "feedback-123"}, "message": "Review saved."})()
        self.client.cookies.set(name, token)
        try:
            with mock.patch("sales_support_agent.api.router.review_feedback_record", return_value=result) as review:
                r = self.client.post(
                    "/admin/api/website-ops/feedback/feedback-123/review",
                    data={"status": "approved"},
                    follow_redirects=False,
                )
        finally:
            self.client.cookies.clear()
        self.assertEqual(r.status_code, 302)
        self.assertEqual(review.call_args.kwargs["reviewer"]["email"], "website_reviewer@anatainc.com")

    def test_unprovisioned_user_denied(self) -> None:
        r = self._get("/admin/executive/brand-analysis", "stranger@anatainc.com")
        self.assertEqual(r.status_code, 403)

    def test_bypass_paths_not_tool_gated(self) -> None:
        # QBO OAuth + login must not be redirected to login *by the RBAC gate*.
        r = self.client.get("/admin/login", follow_redirects=False)
        self.assertNotEqual(r.headers.get("location"), "/admin/login")

    def test_cs_only_user_fulfillment_root_redirects_to_cs(self) -> None:
        # A CS-only user (fulfillment.dashboard) hitting /admin/fulfillment must be
        # routed to the CS dashboard, NOT /sales (which would 403). Regression for
        # the access-safe redirect handler.
        uid = store.upsert_user("enf_cs@anatainc.com", "CS")
        store.set_user_permissions(uid, ["fulfillment.dashboard"])
        r = self._get("/admin/fulfillment", "enf_cs@anatainc.com")
        self.assertEqual(r.status_code, 302)
        self.assertEqual(r.headers.get("location"), "/admin/fulfillment/cs/")
        # And the CS page itself must not 403 for this user.
        r_cs = self._get("/admin/fulfillment/cs/", "enf_cs@anatainc.com")
        self.assertNotIn(r_cs.status_code, (302, 403))


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
        self.assertIn("People", r.text)  # unified People page (users + invites + requests)

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


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class AccessFinalizeTests(unittest.TestCase):
    """Phase-3 finalizers: decision history, optional password login, notify fallbacks."""

    def test_decision_history_includes_decider_fields(self) -> None:
        rid = store.create_access_request("history-case@anatainc.com", "History Case")
        store.decide_access_request(rid, approve=True, decided_by="david@anatainc.com")
        approved = store.list_access_requests(status="approved")
        match = [r for r in approved if r["email"] == "history-case@anatainc.com"]
        self.assertTrue(match)
        self.assertEqual(match[0]["decided_by"], "david@anatainc.com")
        self.assertTrue(match[0]["decided_at"])
        self.assertEqual(match[0]["status"], "approved")

    def test_password_login_split_from_session_validity(self) -> None:
        from dataclasses import dataclass
        from sales_support_agent.services.admin_auth import admin_login_enabled, password_login_enabled

        @dataclass
        class S:
            admin_password: str
            admin_session_secret: str

        google_only = S(admin_password="", admin_session_secret="secret")
        self.assertTrue(admin_login_enabled(google_only))      # sessions still validate
        self.assertFalse(password_login_enabled(google_only))  # but no password form
        both = S(admin_password="pw", admin_session_secret="secret")
        self.assertTrue(password_login_enabled(both))

    def test_notify_returns_false_without_gmail_config(self) -> None:
        from sales_support_agent.services.access.notify import send_approval_email, send_invite_email
        self.assertFalse(send_invite_email(None, to_email="x@anatainc.com", invite_link="https://x"))
        self.assertFalse(send_approval_email(None, to_email="x@anatainc.com"))

        class Empty:  # no GMAIL_* attributes at all
            pass

        self.assertFalse(send_invite_email(Empty(), to_email="x@anatainc.com", invite_link="https://x"))
        self.assertFalse(send_approval_email(Empty(), to_email="x@anatainc.com"))

    def test_login_page_hides_password_form_when_disabled(self) -> None:
        from sales_support_agent.services.admin_dashboard import render_login_page
        google_only = render_login_page(show_google_button=True, show_password_form=False)
        self.assertNotIn('name="password"', google_only)
        self.assertIn("Sign in with Google", google_only)
        self.assertNotIn('<div class="login-divider">', google_only)
        both = render_login_page(show_google_button=True, show_password_form=True)
        self.assertIn('name="password"', both)
        self.assertIn('<div class="login-divider">', both)
        self.assertIn("Admin fallback", both)
        self.assertIn("break-glass admin access", both)
        self.assertIn("Continue with fallback", both)
        self.assertNotIn("GET STARTED", both)


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class ExternalInviteTests(unittest.TestCase):
    """Invited external (non-domain) accounts may sign in; uninvited may not."""

    def _req(self, cookies=None):
        from types import SimpleNamespace
        return SimpleNamespace(cookies=cookies or {})

    def test_uninvited_external_is_rejected(self) -> None:
        from sales_support_agent.api.auth_router import _external_login_allowed
        self.assertFalse(_external_login_allowed(self._req(), "stranger@gmail.com"))

    def test_external_with_matching_invite_cookie_is_allowed(self) -> None:
        from datetime import datetime, timedelta
        from sales_support_agent.api.auth_router import _external_login_allowed
        store.create_invite("contractor@gmail.com", None, token="ext-tok-1",
                            expires_at=datetime.utcnow() + timedelta(days=7))
        req = self._req({"pending_invite": "ext-tok-1"})
        self.assertTrue(_external_login_allowed(req, "contractor@gmail.com"))
        # Same cookie, different google account → still rejected.
        self.assertFalse(_external_login_allowed(req, "other@gmail.com"))

    def test_previously_invited_external_can_sign_in_again(self) -> None:
        from sales_support_agent.api.auth_router import _external_login_allowed
        store.upsert_user("returning-ext@gmail.com", "Returning Ext")
        self.assertTrue(_external_login_allowed(self._req(), "returning-ext@gmail.com"))


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class RootAppGuardsTest(unittest.TestCase):
    """Regression: the root app (main.py, serves agent.anatainc.com) must register
    the access middleware + ToolForbidden handler at CONSTRUCTION time. They were
    being installed in a startup event, where add_middleware() fails after the
    stack is frozen — leaving router-guarded sections (Finance/Advertising/
    Fulfillment/Brand Analysis) to 500 instead of rendering a 403."""

    def test_root_app_registers_rbac_middleware_and_handler(self) -> None:
        import main as rootmain
        from sales_support_agent.services.auth_deps import ToolForbidden
        self.assertIn(ToolForbidden, rootmain.app.exception_handlers,
                      "root app must register the ToolForbidden 403 handler")
        names = {m.cls.__name__ for m in rootmain.app.user_middleware}
        self.assertIn("AccessControlMiddleware", names,
                      "root app must install AccessControlMiddleware")

    def test_root_app_mounts_google_auth_routes(self) -> None:
        """The 'Sign in with Google' button and invite-accept both bounce to
        /admin/auth/google → /admin/auth/callback. Those routes live in
        auth_router and MUST be mounted on the root app, or both 404."""
        import main as rootmain
        paths = {r.path for r in rootmain.app.routes if hasattr(r, "path")}
        self.assertIn("/admin/auth/google", paths,
                      "root app must mount the Google login-start route")
        self.assertIn("/admin/auth/callback", paths,
                      "root app must mount the Google OAuth callback route")

    def test_both_entrypoints_mount_public_profit_calculator_routes(self) -> None:
        import main as rootmain
        import sales_support_agent.main as agentmain

        expected = {
            "/amazon-profit-calculator/runtime",
            "/api/public/amazon-profit-calculator/catalog/{asin}",
            "/api/public/amazon-profit-calculator/profitability/estimate",
        }
        root_paths = {r.path for r in rootmain.app.routes if hasattr(r, "path")}
        agent_paths = {r.path for r in agentmain.app.routes if hasattr(r, "path")}
        self.assertTrue(expected.issubset(root_paths))
        self.assertTrue(expected.issubset(agent_paths))


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class EmailSenderTests(unittest.TestCase):
    """Access-flow email picks Resend first, then Gmail, then falls back to the
    copyable link (returns False) when no provider is configured."""

    def test_no_provider_returns_false(self) -> None:
        from sales_support_agent.services.access import notify
        s = _settings()
        # Neutralize both providers regardless of ambient env.
        import sales_support_agent.integrations.resend as R
        import sales_support_agent.integrations.gmail as G
        orig_r, orig_g = R.ResendClient, G.GmailClient
        try:
            R.ResendClient = lambda settings: type("X", (), {"is_configured": lambda self: False})()
            G.GmailClient = lambda settings: type("X", (), {"is_configured": lambda self: False})()
            self.assertFalse(notify.send_invite_email(
                s, to_email="nobody@anatainc.com", invite_link="https://x/TOK"))
        finally:
            R.ResendClient, G.GmailClient = orig_r, orig_g

    def test_resend_preferred_over_gmail(self) -> None:
        from sales_support_agent.services.access import notify
        s = _settings()
        import sales_support_agent.integrations.resend as R
        import sales_support_agent.integrations.gmail as G
        sent = {}

        class FakeResend:
            def __init__(self, settings): pass
            def is_configured(self): return True
            def send_message(self, **kw): sent["resend"] = kw

        class FakeGmail:
            def __init__(self, settings): pass
            def is_configured(self): return True
            def send_message(self, **kw): sent["gmail"] = kw

        orig_r, orig_g = R.ResendClient, G.GmailClient
        try:
            R.ResendClient, G.GmailClient = FakeResend, FakeGmail
            ok = notify.send_approval_email(s, to_email="gabe@anatainc.com",
                                            base_url="https://agent.anatainc.com")
            self.assertTrue(ok)
            self.assertIn("resend", sent)
            self.assertNotIn("gmail", sent)  # Gmail not touched when Resend works
        finally:
            R.ResendClient, G.GmailClient = orig_r, orig_g


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class InviteTimezoneTests(unittest.TestCase):
    """Regression: invite lookup compared a naive datetime.utcnow() against the
    invite's expires_at. On Postgres (TIMESTAMPTZ) that column is tz-AWARE, so
    the comparison raised TypeError and 500'd the public invite-accept link.
    Lookup must be timezone-safe regardless of how 'now'/expires_at are stored."""

    def test_lookup_survives_aware_now(self) -> None:
        import secrets
        from datetime import datetime, timezone, timedelta
        tok = secrets.token_urlsafe(16)
        store.create_invite("tz-reg@anatainc.com", None, token=tok,
                            expires_at=datetime.utcnow() + timedelta(days=7))
        aware_now = datetime.now(timezone.utc)  # mimics a TIMESTAMPTZ-driven compare
        inv = store.get_pending_invite_by_token(tok, now=aware_now)  # must not raise
        self.assertIsNotNone(inv)
        self.assertEqual(inv["email"], "tz-reg@anatainc.com")

    def test_expired_invite_returns_none_not_crash(self) -> None:
        import secrets
        from datetime import datetime, timezone, timedelta
        tok = secrets.token_urlsafe(16)
        store.create_invite("tz-old@anatainc.com", None, token=tok,
                            expires_at=datetime.utcnow() - timedelta(days=1))
        self.assertIsNone(store.get_pending_invite_by_token(tok, now=datetime.now(timezone.utc)))


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class UnifiedPeoplePageTests(unittest.TestCase):
    """The /admin/access page combines pending requests, pending invites, and
    provisioned users into one table (+ the invite form + decision history)."""

    def setUp(self) -> None:
        self.client = TestClient(app)
        self.sa_name, self.sa_token = _cookie_for("david@anatainc.com", "David", "admin")

    def _get(self, path):
        self.client.cookies.set(self.sa_name, self.sa_token)
        try:
            return self.client.get(path, follow_redirects=False)
        finally:
            self.client.cookies.clear()

    def test_people_page_shows_requests_invites_and_users(self) -> None:
        import secrets
        store.create_invite("combined-invite@anatainc.com", None,
                            token=secrets.token_urlsafe(16), invited_by="david@anatainc.com")
        store.create_access_request("combined-request@anatainc.com", "Combined Req")

        r = self._get("/admin/access")
        self.assertEqual(r.status_code, 200)
        # One unified page
        self.assertIn("People", r.text)
        # All three kinds present in the same page
        self.assertIn("combined-request@anatainc.com", r.text)  # a request
        self.assertIn("combined-invite@anatainc.com", r.text)   # an invite
        self.assertIn("david@anatainc.com", r.text)             # a user (superadmin)
        # Status badges that distinguish the kinds
        self.assertIn("Requested", r.text)
        self.assertIn("Invited", r.text)
        # The invite form lives on the same page now
        self.assertIn("Send new invite", r.text)
        # Approve/Deny + Revoke actions are inline
        self.assertIn("/requests/", r.text)
        self.assertIn("/revoke", r.text)


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class PerPersonAccessTests(unittest.TestCase):
    """Roles are gone — access is granted per-person via the Manage-access editor;
    invites carry no role."""

    def setUp(self) -> None:
        self.client = TestClient(app)
        self.sa_name, self.sa_token = _cookie_for("david@anatainc.com", "David", "admin")

    def _get(self, path):
        self.client.cookies.set(self.sa_name, self.sa_token)
        try:
            return self.client.get(path, follow_redirects=False)
        finally:
            self.client.cookies.clear()

    def _post(self, path, data):
        self.client.cookies.set(self.sa_name, self.sa_token)
        try:
            return self.client.post(path, data=data, follow_redirects=False)
        finally:
            self.client.cookies.clear()

    def test_people_page_has_manage_access_not_roles(self) -> None:
        store.upsert_user("pp_user@anatainc.com", "PP")
        r = self._get("/admin/access")
        self.assertEqual(r.status_code, 200)
        self.assertIn("Manage access", r.text)
        self.assertNotIn("Manage roles", r.text)
        self.assertNotIn("No role (assign later)", r.text)  # invite form is email-only

    def test_user_access_editor_renders_and_saves(self) -> None:
        uid = store.upsert_user("editme@anatainc.com", "Edit Me")
        # Editor renders
        r = self._get(f"/admin/access/users/{uid}/access")
        self.assertEqual(r.status_code, 200)
        self.assertIn("editme@anatainc.com", r.text)
        self.assertIn('value="finance"', r.text)
        # Saving grants exactly the ticked tools
        r2 = self._post(f"/admin/access/users/{uid}/access",
                        {"permissions": ["finance", "advertising.audit"]})
        self.assertIn(r2.status_code, (302, 303))
        u = store.get_user_by_email("editme@anatainc.com")
        self.assertEqual(u["permissions"], {"finance", "advertising.audit"})

    def test_invite_without_role(self) -> None:
        r = self._post("/admin/access/invites/new", {"email": "norole@anatainc.com"})
        self.assertEqual(r.status_code, 200)
        self.assertIn("Invite created", r.text)
        inv = next(i for i in store.list_pending_invites() if i["email"] == "norole@anatainc.com")
        self.assertIsNone(inv["role_id"])


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class GoogleAuthUrlTests(unittest.TestCase):
    """The Google auth URL must NOT pin `hd` (hosted domain) — that would block
    invited external users (personal Gmail). Authorization is enforced app-side."""

    def test_auth_url_omits_hd(self) -> None:
        from sales_support_agent.services.admin_auth_google import google_auth_url
        s = _settings()
        url = google_auth_url(s, redirect_uri="https://agent.anatainc.com/admin/auth/callback", state="s")
        self.assertNotIn("hd=", url)
        self.assertIn("client_id=", url)
        self.assertIn("prompt=select_account", url)


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class GoogleSessionMintTests(unittest.TestCase):
    """Regression: Google login must mint the session cookie with the SAME
    settings the root app validates /admin against (admin_dashboard_settings),
    or the user is recognized by RBAC but bounced to login at /admin."""

    def test_google_session_validates_at_admin(self) -> None:
        import main as rootmain
        from sales_support_agent.api import auth_router
        from sales_support_agent.config import load_settings as load_agent
        from sales_support_agent.services.admin_auth import validate_admin_session_token

        admin_dash = rootmain.load_admin_dashboard_settings()
        agent = load_agent()

        class _URL:
            def __str__(self): return "https://agent.anatainc.com/"

        class _Req:
            def __init__(self):
                st = type("St", (), {"admin_dashboard_settings": admin_dash, "agent_settings": agent})()
                self.app = type("A", (), {"state": st})()
                self.base_url = _URL()

        # Session settings must resolve to admin_dashboard_settings on the root app
        self.assertIs(auth_router._session_settings(_Req()), admin_dash)

        resp = auth_router._mint_session(_Req(), agent, "ext-partner@gmail.com", "Ext")
        cookies = [v.decode() for k, v in resp.raw_headers if k.decode().lower() == "set-cookie"]
        sess = next((c for c in cookies if c.startswith(admin_dash.admin_cookie_name + "=")), None)
        self.assertIsNotNone(sess, "must mint a cookie named like the root app's session cookie")
        token = sess.split("=", 1)[1].split(";")[0]
        # The crux: /admin's strict validator accepts the Google-minted token
        self.assertTrue(validate_admin_session_token(admin_dash, token))

    def test_allowed_domain_login_auto_provisions_website_ops_review_tools(self) -> None:
        from sales_support_agent.api import auth_router

        settings = _settings()
        email = f"website-ops-review-{uuid.uuid4().hex}@anatainc.com"

        class _URL:
            def __str__(self): return "https://agent.anatainc.com/"

        class _Req:
            cookies = {}
            base_url = _URL()
            app = type("A", (), {"state": type("St", (), {"agent_settings": settings})()})()

        resp = auth_router._rbac_login(_Req(), settings, email, "Website Ops Reviewer")
        self.assertEqual(resp.status_code, 302)
        user = store.get_user_by_email(email)
        self.assertIsNotNone(user)
        self.assertEqual(
            user["permissions"],
            {"website_ops.seo", "website_ops.queue", "website_ops.reports"},
        )

    def test_unprovisioned_google_login_creates_request_and_redirects_to_pending(self) -> None:
        from sales_support_agent.api import auth_router

        settings = SimpleNamespace(
            admin_session_secret="pending-secret",
            admin_cookie_name="pending_session",
            admin_session_ttl_hours=24,
            google_oauth_allowed_domain="anatainc.com",
            rbac_auto_provision_domain_tools=(),
            rbac_superadmin_emails=(),
            admin_role_map={},
            admin_default_role="ops",
        )
        email = f"pending-review-{uuid.uuid4().hex}@anatainc.com"

        class _URL:
            def __str__(self): return "https://agent.anatainc.com/"

        class _Req:
            cookies = {}
            base_url = _URL()
            app = type("A", (), {"state": type("St", (), {"settings": settings})()})()

        resp = auth_router._rbac_login(_Req(), settings, email, "Pending Reviewer")
        self.assertEqual(resp.status_code, 302)
        self.assertEqual(resp.headers["location"], "/admin/pending")
        cookies = [value.decode() for key, value in resp.raw_headers if key.decode().lower() == "set-cookie"]
        self.assertTrue(any(cookie.startswith(settings.admin_cookie_name + "=") for cookie in cookies))
        pending = store.get_pending_access_request_for_email(email)
        self.assertIsNotNone(pending)
        self.assertEqual(pending["email"], email)

    def test_pending_page_rehydrates_existing_request_from_session(self) -> None:
        email = f"pending-page-{uuid.uuid4().hex}@anatainc.com"
        store.create_access_request(email, "Pending Page")
        client = TestClient(app)
        name, token = _cookie_for(email, "Pending Page")
        client.cookies.set(name, token)
        try:
            resp = client.get("/admin/pending", follow_redirects=False)
        finally:
            client.cookies.clear()
        self.assertEqual(resp.status_code, 200)
        self.assertIn("Access requested", resp.text)
        self.assertIn(email, resp.text)
        self.assertIn("Request received", resp.text)

    def test_pending_page_shows_unavailable_when_access_store_is_down(self) -> None:
        from unittest import mock

        email = f"pending-down-{uuid.uuid4().hex}@anatainc.com"
        client = TestClient(app)
        name, token = _cookie_for(email, "Pending Down")
        client.cookies.set(name, token)
        try:
            with mock.patch(
                "sales_support_agent.services.access.store.get_user_by_email",
                side_effect=RuntimeError("database unavailable"),
            ):
                resp = client.get("/admin/pending", follow_redirects=False)
        finally:
            client.cookies.clear()
        self.assertEqual(resp.status_code, 503)
        self.assertIn("Access system unavailable", resp.text)
        self.assertIn("access approval database is temporarily unavailable", resp.text)
        self.assertIn(email, resp.text)

    def test_existing_allowed_domain_user_gets_website_ops_review_tools(self) -> None:
        from sales_support_agent.api import auth_router

        settings = _settings()
        email = f"existing-review-{uuid.uuid4().hex}@anatainc.com"
        uid = store.upsert_user(email, "Existing Reviewer")
        store.set_user_permissions(uid, ["finance"])

        class _URL:
            def __str__(self): return "https://agent.anatainc.com/"

        class _Req:
            cookies = {}
            base_url = _URL()
            app = type("A", (), {"state": type("St", (), {"agent_settings": settings})()})()

        resp = auth_router._rbac_login(_Req(), settings, email, "Existing Reviewer")
        self.assertEqual(resp.status_code, 302)
        user = store.get_user_by_email(email)
        self.assertEqual(
            user["permissions"],
            {"finance", "website_ops.seo", "website_ops.queue", "website_ops.reports"},
        )


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class NavAccessSafetyTests(unittest.TestCase):
    """The regression guard: every href the nav renders must point at a page the
    user can actually open (no link to a tool they don't hold)."""

    @staticmethod
    def _hrefs(nav_html: str) -> list:
        import re
        return re.findall(r'href="([^"]+)"', nav_html)

    def _assert_all_hrefs_accessible(self, nav_html: str, granted: set, is_superadmin: bool = False) -> None:
        from sales_support_agent.services.access.middleware import _resolve_tool
        for href in self._hrefs(nav_html):
            if not href.startswith("/admin"):
                continue  # external / logout etc.
            if href in ("/admin", "/admin/logout", "/admin/access", "/admin/settings"):
                # /admin = the brandmark home link (always present, like a logo);
                # the rest are profile-dropdown links gated separately.
                continue
            tool = _resolve_tool(href)
            if tool is None:
                continue  # ungated page (e.g. /admin maps to sales.priorities, but None tool = fine)
            if is_superadmin:
                continue
            self.assertIn(
                tool.key, granted,
                f"nav exposed {href} (tool {tool.key}) the user does not hold",
            )

    def test_no_forbidden_link_for_any_permission_set(self) -> None:
        from sales_support_agent.services.admin_nav import render_agent_nav
        for perms in (
            {"fulfillment.dashboard"},
            {"fulfillment.rate_sheets"},
            {"finance"},
        ):
            nav = render_agent_nav(permissions=perms)
            self._assert_all_hrefs_accessible(nav, perms)
        # Superadmin: every link is fair game, just assert it renders without error.
        nav = render_agent_nav(is_superadmin=True)
        self._assert_all_hrefs_accessible(nav, set(), is_superadmin=True)

    def test_cs_only_user_primary_points_at_cs_not_sales(self) -> None:
        from sales_support_agent.services.admin_nav import render_agent_nav
        nav = render_agent_nav("fulfillment_dashboard", permissions={"fulfillment.dashboard"})
        # Primary (and only) fulfillment link is the CS dashboard, NOT /sales.
        self.assertIn('href="/admin/fulfillment/cs/"', nav)
        self.assertNotIn('href="/admin/fulfillment/sales"', nav)

    def test_cs_dashboard_plus_reports_renders_dropdown(self) -> None:
        from sales_support_agent.services.admin_nav import render_agent_nav
        nav = render_agent_nav(
            "fulfillment_dashboard",
            permissions={"fulfillment.dashboard", "fulfillment.reports"},
        )
        # >=2 accessible CS pages -> a dropdown and visible section row of pills,
        # none pointing at /sales.
        self.assertIn("nav-dropdown", nav)
        self.assertIn("topbar-section-row", nav)
        self.assertIn('href="/admin/fulfillment/cs/"', nav)
        self.assertIn('href="/admin/fulfillment/cs/reports/"', nav)
        self.assertNotIn('href="/admin/fulfillment/sales"', nav)

    def test_active_fulfillment_section_exposes_visible_subpage_row(self) -> None:
        from sales_support_agent.services.admin_nav import render_agent_nav

        nav = render_agent_nav(
            "fulfillment",
            fulfillment_section="fulfillment_sales",
            permissions={"fulfillment.rate_sheets", "fulfillment.dashboard", "fulfillment.reports"},
        )

        self.assertIn('aria-label="Fulfillment pages"', nav)
        self.assertIn(">Rate Sheets</a>", nav)
        self.assertIn(">CS Dashboard</a>", nav)
        self.assertIn(">CS Reports</a>", nav)
        self.assertIn(">Latest Report</a>", nav)

    def test_single_accessible_page_section_has_no_dropdown(self) -> None:
        from sales_support_agent.services.admin_nav import render_agent_nav
        # finance has exactly one subpage -> plain link, no .nav-dropdown markup.
        nav = render_agent_nav(permissions={"finance"})
        self.assertIn('href="/admin/finances"', nav)
        self.assertNotIn("nav-dropdown", nav)

    def test_multi_accessible_page_section_renders_dropdown(self) -> None:
        from sales_support_agent.services.admin_nav import render_agent_nav
        # website_ops with 2+ tools -> dropdown with the right pills.
        nav = render_agent_nav(permissions={"website_ops.seo", "website_ops.queue"})
        self.assertIn("nav-dropdown", nav)
        self.assertIn('href="/admin/website-ops"', nav)
        self.assertIn('href="/admin/website-ops/queue"', nav)
        # reports tool not held -> its pill must not appear.
        self.assertNotIn('href="/admin/website-ops/reports"', nav)

    def test_superadmin_only_advertising_subpage_is_hidden_from_non_superadmins(self) -> None:
        from sales_support_agent.services.admin_nav import render_agent_nav
        member_nav = render_agent_nav(permissions={"advertising.audit"})
        self.assertNotIn('href="/admin/advertising/profit-calculator"', member_nav)
        self.assertNotIn('href="/admin/advertising/bulk-profitability"', member_nav)

        super_nav = render_agent_nav(is_superadmin=True)
        self.assertIn('href="/admin/advertising/profit-calculator"', super_nav)
        self.assertIn('href="/admin/advertising/bulk-profitability"', super_nav)

    def test_section_with_zero_accessible_pages_is_hidden(self) -> None:
        from sales_support_agent.services.admin_nav import render_agent_nav
        nav = render_agent_nav(permissions={"finance"})
        # No fulfillment tools held -> Fulfillment section must not render at all.
        self.assertNotIn("Fulfillment", nav)
        self.assertNotIn('href="/admin/fulfillment/cs/"', nav)
