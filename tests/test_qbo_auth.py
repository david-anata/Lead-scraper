"""Tests for the QuickBooks OAuth 2.0 routes.

Covers the three routes Intuit reviews for production approval:
  GET  /connect      → 302 redirect to appcenter.intuit.com
  GET  /callback     → 400 on bad/expired state
  POST /disconnect   → 200 {"status":"not_connected"} when no tokens stored

Uses an in-memory SQLite DB via a monkeypatched get_engine() so no external
service or real DB is needed.
"""

from __future__ import annotations

import os
import sqlite3
import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# In-memory SQLite fixture
# ---------------------------------------------------------------------------

def _make_in_memory_engine():
    """Return a SQLAlchemy engine pointed at a fresh in-memory SQLite DB
    with the qb_oauth_state and quickbooks_tokens tables pre-created.

    StaticPool is required: without it, each engine.connect() call creates a
    *separate* in-memory database, so tables created in one connection are
    invisible to the next connection.
    """
    from sqlalchemy import create_engine, text
    from sqlalchemy.pool import StaticPool

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS qb_oauth_state (
                state      TEXT PRIMARY KEY,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                expires_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS quickbooks_tokens (
                id            TEXT PRIMARY KEY DEFAULT 'singleton',
                access_token  TEXT NOT NULL DEFAULT '',
                refresh_token TEXT NOT NULL DEFAULT '',
                realm_id      TEXT NOT NULL DEFAULT '',
                expires_at    TEXT NULL,
                created_at    TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at    TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """))
    return engine


class TestQBOAuthRoutes(unittest.TestCase):
    """Route-level tests via FastAPI TestClient with in-memory DB."""

    def setUp(self):
        self.engine = _make_in_memory_engine()
        # The helper functions in qbo_auth_router do `from sales_support_agent.models.database import get_engine`
        # at call time, so we patch the source module.
        self.engine_patcher = patch(
            "sales_support_agent.models.database.get_engine",
            return_value=self.engine,
        )
        self.engine_patcher.start()

        # Patch env vars so routes have something to work with
        os.environ.setdefault("QB_CLIENT_ID", "test-client-id")
        os.environ.setdefault("QB_CLIENT_SECRET", "test-client-secret")
        os.environ.setdefault("QB_REDIRECT_URI", "https://agent.anatainc.com/callback")

        # Import app AFTER patching
        from sales_support_agent.api.qbo_auth_router import router
        from fastapi import FastAPI
        self.app = FastAPI()
        self.app.include_router(router)
        self.client = TestClient(self.app, follow_redirects=False)

    def tearDown(self):
        self.engine_patcher.stop()

    # ── /connect ──────────────────────────────────────────────────────────

    def test_connect_returns_302(self):
        resp = self.client.get("/connect")
        self.assertEqual(resp.status_code, 302)

    def test_connect_redirects_to_intuit_auth_url(self):
        resp = self.client.get("/connect")
        location = resp.headers.get("location", "")
        self.assertIn("appcenter.intuit.com", location, f"Expected Intuit URL, got: {location}")

    def test_connect_includes_required_params(self):
        resp = self.client.get("/connect")
        location = resp.headers.get("location", "")
        self.assertIn("response_type=code", location)
        self.assertIn("scope=com.intuit.quickbooks.accounting", location)
        self.assertIn("state=", location)
        self.assertIn("client_id=", location)
        self.assertIn("redirect_uri=", location)

    def test_connect_stores_state_in_db(self):
        from sqlalchemy import text
        resp = self.client.get("/connect")
        self.assertEqual(resp.status_code, 302)
        with self.engine.connect() as conn:
            rows = conn.execute(text("SELECT state FROM qb_oauth_state")).fetchall()
        self.assertEqual(len(rows), 1, "State should be stored in DB after /connect")

    # ── /callback — bad state ─────────────────────────────────────────────

    def test_callback_rejects_missing_state(self):
        resp = self.client.get("/callback?code=abc123&state=&realmId=123")
        self.assertEqual(resp.status_code, 400)

    def test_callback_rejects_wrong_state(self):
        # First call /connect to store a real state
        self.client.get("/connect")
        # Now call /callback with a completely wrong state
        resp = self.client.get("/callback?code=abc123&state=wrongstate&realmId=123")
        self.assertEqual(resp.status_code, 400)

    def test_callback_rejects_expired_state(self):
        from sqlalchemy import text
        # Insert a pre-expired state directly
        past = (datetime.utcnow() - timedelta(minutes=15)).isoformat()
        with self.engine.begin() as conn:
            conn.execute(
                text("INSERT INTO qb_oauth_state (state, created_at, expires_at) VALUES (:s, :now, :exp)"),
                {"s": "expiredstate", "now": past, "exp": past},
            )
        resp = self.client.get("/callback?code=abc123&state=expiredstate&realmId=123")
        self.assertEqual(resp.status_code, 400)

    def test_callback_rejects_when_user_denies(self):
        """Intuit sends ?error=access_denied when user clicks 'No'."""
        resp = self.client.get("/callback?error=access_denied&error_description=User+denied")
        self.assertEqual(resp.status_code, 400)

    def test_callback_state_is_consumed_after_use(self):
        """State should be deleted from the DB after one successful validation attempt,
        preventing replay attacks."""
        from sqlalchemy import text
        # Insert a valid state
        future = (datetime.utcnow() + timedelta(minutes=9)).isoformat()
        now = datetime.utcnow().isoformat()
        with self.engine.begin() as conn:
            conn.execute(
                text("INSERT INTO qb_oauth_state (state, created_at, expires_at) VALUES (:s, :now, :exp)"),
                {"s": "oncestate", "now": now, "exp": future},
            )
        # First /callback call validates state → triggers token exchange (which will fail with test creds)
        # We don't care about the token exchange result, just that state is consumed
        with patch("sales_support_agent.api.qbo_auth_router.requests.post") as mock_post:
            mock_post.side_effect = Exception("Mocked token exchange failure")
            self.client.get("/callback?code=validcode&state=oncestate&realmId=123")

        # State row should be gone
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("SELECT state FROM qb_oauth_state WHERE state = 'oncestate'")
            ).fetchall()
        self.assertEqual(len(rows), 0, "State must be deleted after use (replay protection)")

    # ── /disconnect ───────────────────────────────────────────────────────

    def test_disconnect_post_requires_auth(self):
        """POST /disconnect must return 401 without an authenticated session."""
        resp = self.client.post("/disconnect")
        self.assertEqual(resp.status_code, 401)

    def test_disconnect_returns_200(self):
        """GET /disconnect (unguarded for Intuit reviewer compat) returns 200."""
        resp = self.client.get("/disconnect")
        self.assertEqual(resp.status_code, 200)

    def test_disconnect_not_connected_when_no_tokens(self):
        resp = self.client.get("/disconnect")
        body = resp.json()
        self.assertEqual(body.get("status"), "not_connected")

    def test_disconnect_get_also_works(self):
        """GET /disconnect must work — Intuit reviewer compatibility."""
        resp = self.client.get("/disconnect")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json().get("status"), "not_connected")

    def test_disconnect_returns_disconnected_after_revoke(self):
        """When tokens exist, GET /disconnect revokes and returns 'disconnected'."""
        from sqlalchemy import text
        now = datetime.utcnow().isoformat()
        # Insert a fake token row
        with self.engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO quickbooks_tokens
                        (id, access_token, refresh_token, realm_id, expires_at, created_at, updated_at)
                    VALUES ('singleton', 'fake-access', 'fake-refresh', 'realm123', :now, :now, :now)
                """),
                {"now": now},
            )
        # Mock the revoke HTTP call
        with patch("sales_support_agent.api.qbo_auth_router.requests.post") as mock_post:
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_post.return_value = mock_resp
            resp = self.client.get("/disconnect")

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json().get("status"), "disconnected")

        # Confirm token row was deleted
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("SELECT id FROM quickbooks_tokens WHERE id = 'singleton'")
            ).fetchall()
        self.assertEqual(len(rows), 0, "Token row should be deleted after disconnect")

    def test_disconnect_clears_tokens_even_when_revoke_fails(self):
        """Local tokens must be cleared even if Intuit's revoke endpoint errors."""
        from sqlalchemy import text
        now = datetime.utcnow().isoformat()
        with self.engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO quickbooks_tokens
                        (id, access_token, refresh_token, realm_id, expires_at, created_at, updated_at)
                    VALUES ('singleton', 'fake-access', 'fake-refresh', 'realm123', :now, :now, :now)
                """),
                {"now": now},
            )
        with patch("sales_support_agent.api.qbo_auth_router.requests.post") as mock_post:
            mock_post.side_effect = Exception("Network error")
            resp = self.client.get("/disconnect")

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json().get("status"), "disconnected")

        # Tokens should still be cleared locally despite the network error
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("SELECT id FROM quickbooks_tokens WHERE id = 'singleton'")
            ).fetchall()
        self.assertEqual(len(rows), 0, "Tokens must be cleared even when Intuit revoke fails")


if __name__ == "__main__":
    unittest.main()
