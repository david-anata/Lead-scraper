"""QuickBooks Online OAuth 2.0 routes.

Three endpoints required for Intuit production approval:

    GET  /connect      — starts the OAuth flow (redirects to Intuit)
    GET  /callback     — receives the auth code, exchanges for tokens
    POST /disconnect   — revokes tokens and clears local storage
    GET  /disconnect   — same, for Intuit reviewer compatibility

Security design:
    - State parameter stored in DB (qb_oauth_state table) with 10-min TTL;
      validated on /callback before code exchange (CSRF protection).
    - Tokens stored server-side only in quickbooks_tokens table (singleton row).
      Never exposed to the browser.
    - Basic Auth header used for token exchange (client_id:client_secret
      base64-encoded), NOT as body params — per Intuit spec.
    - /connect, /callback, /disconnect intentionally have NO auth guard so
      Intuit's reviewer can hit them without an Anata session.
"""

from __future__ import annotations

import base64
import logging
import os
import secrets
from datetime import datetime, timedelta, timezone
from typing import Union
from urllib.parse import urlencode

import requests
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Intuit endpoint constants
# ---------------------------------------------------------------------------
_QB_AUTH_BASE  = "https://appcenter.intuit.com/connect/oauth2"
_QB_TOKEN_URL  = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
_QB_REVOKE_URL = "https://developer.api.intuit.com/v2/oauth2/tokens/revoke"
_QB_SCOPE      = "com.intuit.quickbooks.accounting"
_STATE_TTL_MIN = 10   # OAuth state expires after 10 minutes

router = APIRouter(tags=["qbo-oauth"])


# ---------------------------------------------------------------------------
# GET /connect — entry point for OAuth flow
# ---------------------------------------------------------------------------

@router.get("/connect", response_model=None)
def qb_connect(request: Request) -> RedirectResponse:
    """Start the QuickBooks OAuth flow.

    Generates a CSRF state token, stores it in the DB, then redirects
    to Intuit's authorization URL.  No Anata login required — Intuit's
    reviewer must be able to reach this directly.
    """
    client_id = os.getenv("QB_CLIENT_ID", "").strip()
    redirect_uri = _redirect_uri()

    if not client_id:
        return HTMLResponse(
            "<h2>QB_CLIENT_ID is not configured on this server.</h2>",
            status_code=500,
        )

    state = secrets.token_hex(16)
    try:
        _store_oauth_state(state)
    except Exception as exc:
        logger.error("Could not store OAuth state: %s", exc)
        return HTMLResponse("<h2>Server error: could not initialise OAuth state.</h2>", status_code=500)

    params = {
        "client_id":     client_id,
        "redirect_uri":  redirect_uri,
        "response_type": "code",
        "scope":         _QB_SCOPE,
        "state":         state,
    }
    auth_url = f"{_QB_AUTH_BASE}?{urlencode(params)}"
    logger.info("QB OAuth: redirecting to Intuit authorization URL (state=%s)", state[:8] + "…")
    return RedirectResponse(auth_url, status_code=302)


# ---------------------------------------------------------------------------
# GET /callback — Intuit redirects here after user authorizes
# ---------------------------------------------------------------------------

@router.get("/callback", response_model=None)
def qb_callback(
    request: Request,
    code: str = "",
    state: str = "",
    realmId: str = "",
    error: str = "",
    error_description: str = "",
) -> Union[HTMLResponse, RedirectResponse]:
    """Handle the Intuit redirect after user authorization.

    Validates the CSRF state, exchanges the auth code for tokens, stores
    them server-side, then sends the user to /admin.
    """
    # Intuit sends error= when the user denies access
    if error:
        logger.warning("QB OAuth denied: %s — %s", error, error_description)
        return HTMLResponse(
            f"<h2>QuickBooks authorization denied.</h2>"
            f"<p>{error}: {error_description}</p>"
            f'<p><a href="/connect">Try again</a></p>',
            status_code=400,
        )

    if not code or not state:
        return HTMLResponse(
            "<h2>Missing code or state parameter.</h2>"
            '<p><a href="/connect">Start over</a></p>',
            status_code=400,
        )

    # CSRF validation — consumes the state (one-time use)
    if not _validate_and_consume_state(state):
        logger.warning("QB OAuth: state mismatch or expired (state=%s…)", state[:8])
        return HTMLResponse(
            "<h2>Invalid or expired OAuth state.</h2>"
            "<p>This link may have already been used or expired (10 min TTL).</p>"
            '<p><a href="/connect">Start the flow again</a></p>',
            status_code=400,
        )

    # Exchange authorization code → access + refresh tokens
    try:
        resp = requests.post(
            _QB_TOKEN_URL,
            headers={
                "Authorization": _basic_auth_header(),
                "Content-Type":  "application/x-www-form-urlencoded",
                "Accept":        "application/json",
            },
            data={
                "grant_type":   "authorization_code",
                "code":         code,
                "redirect_uri": _redirect_uri(),
            },
            timeout=20,
        )
        resp.raise_for_status()
        token_data = resp.json()
    except requests.HTTPError as exc:
        body = exc.response.text if exc.response else str(exc)
        logger.error("QB token exchange HTTP error %s: %s", exc.response.status_code if exc.response else "?", body)
        return HTMLResponse(
            f"<h2>Token exchange failed (HTTP {exc.response.status_code if exc.response else 'error'}).</h2>"
            f"<pre>{body[:500]}</pre>"
            '<p><a href="/connect">Try again</a></p>',
            status_code=400,
        )
    except Exception as exc:
        logger.error("QB token exchange error: %s", exc)
        return HTMLResponse(
            f"<h2>Token exchange failed: {exc}</h2>"
            '<p><a href="/connect">Try again</a></p>',
            status_code=400,
        )

    # Persist tokens server-side
    access_token  = token_data.get("access_token", "")
    refresh_token = token_data.get("refresh_token", "")
    expires_in    = int(token_data.get("expires_in", 3600))
    expires_at    = (datetime.now(timezone.utc) + timedelta(seconds=expires_in)).isoformat()

    try:
        _store_tokens(
            access_token=access_token,
            refresh_token=refresh_token,
            realm_id=realmId,
            expires_at=expires_at,
        )
    except Exception as exc:
        logger.error("QB token storage failed: %s", exc)
        return HTMLResponse(
            f"<h2>Tokens received but could not be stored: {exc}</h2>",
            status_code=500,
        )

    logger.info("QB OAuth complete: realm=%s, access_token expires in %ds", realmId, expires_in)
    return RedirectResponse("/admin", status_code=303)


# ---------------------------------------------------------------------------
# POST /disconnect  (also GET for Intuit reviewer compatibility)
# ---------------------------------------------------------------------------

@router.post("/disconnect")
@router.get("/disconnect")
def qb_disconnect(request: Request) -> JSONResponse:
    """Revoke QuickBooks tokens and clear local storage.

    Always returns 200.  If no tokens are stored, returns
    {"status": "not_connected"}.  On successful revoke (or even if Intuit's
    revoke call fails), returns {"status": "disconnected"}.
    """
    token_row = _load_tokens()
    if not token_row or not token_row.get("access_token"):
        return JSONResponse({"status": "not_connected"}, status_code=200)

    # Call Intuit revoke — failure is non-fatal; we clear locally regardless
    try:
        resp = requests.post(
            _QB_REVOKE_URL,
            headers={
                "Authorization": _basic_auth_header(),
                "Content-Type":  "application/x-www-form-urlencoded",
                "Accept":        "application/json",
            },
            data={"token": token_row["access_token"]},
            timeout=15,
        )
        if resp.status_code not in (200, 204):
            logger.warning("QB revoke returned %d: %s", resp.status_code, resp.text[:200])
        else:
            logger.info("QB OAuth revoked successfully")
    except Exception as exc:
        logger.warning("QB revoke request failed (clearing locally anyway): %s", exc)

    _clear_tokens()
    return JSONResponse({"status": "disconnected"}, status_code=200)


# ---------------------------------------------------------------------------
# DB helpers — isolated so they can be patched in tests
# ---------------------------------------------------------------------------

def _redirect_uri() -> str:
    return os.getenv("QB_REDIRECT_URI", "https://agent.anatainc.com/callback").strip()


def _basic_auth_header() -> str:
    client_id     = os.getenv("QB_CLIENT_ID", "").strip()
    client_secret = os.getenv("QB_CLIENT_SECRET", "").strip()
    encoded = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    return f"Basic {encoded}"


def _store_oauth_state(state: str) -> None:
    from sales_support_agent.models.database import get_engine
    from sqlalchemy import text

    now = datetime.utcnow().isoformat()
    expires = (datetime.utcnow() + timedelta(minutes=_STATE_TTL_MIN)).isoformat()
    with get_engine().begin() as conn:
        # Clean up expired states first
        conn.execute(text("DELETE FROM qb_oauth_state WHERE expires_at < :now"), {"now": now})
        conn.execute(
            text("INSERT INTO qb_oauth_state (state, created_at, expires_at) VALUES (:s, :now, :exp)"),
            {"s": state, "now": now, "exp": expires},
        )


def _validate_and_consume_state(state: str) -> bool:
    """Return True and delete the state row if valid and not expired."""
    from sales_support_agent.models.database import get_engine
    from sqlalchemy import text

    now = datetime.utcnow().isoformat()
    with get_engine().begin() as conn:
        row = conn.execute(
            text("SELECT state FROM qb_oauth_state WHERE state = :s AND expires_at > :now"),
            {"s": state, "now": now},
        ).fetchone()
        if row:
            conn.execute(text("DELETE FROM qb_oauth_state WHERE state = :s"), {"s": state})
    return row is not None


def _store_tokens(*, access_token: str, refresh_token: str, realm_id: str, expires_at: str) -> None:
    """Upsert the singleton quickbooks_tokens row."""
    from sales_support_agent.models.database import get_engine
    from sqlalchemy import text

    now = datetime.utcnow().isoformat()
    with get_engine().begin() as conn:
        existing = conn.execute(
            text("SELECT id FROM quickbooks_tokens WHERE id = 'singleton'")
        ).fetchone()
        if existing:
            conn.execute(
                text("""
                    UPDATE quickbooks_tokens
                    SET access_token=:at, refresh_token=:rt, realm_id=:rid,
                        expires_at=:exp, updated_at=:now
                    WHERE id = 'singleton'
                """),
                {"at": access_token, "rt": refresh_token, "rid": realm_id, "exp": expires_at, "now": now},
            )
        else:
            conn.execute(
                text("""
                    INSERT INTO quickbooks_tokens
                        (id, access_token, refresh_token, realm_id, expires_at, created_at, updated_at)
                    VALUES
                        ('singleton', :at, :rt, :rid, :exp, :now, :now)
                """),
                {"at": access_token, "rt": refresh_token, "rid": realm_id, "exp": expires_at, "now": now},
            )


def _load_tokens() -> dict | None:
    """Return the stored token row as a dict, or None if no tokens are stored."""
    try:
        from sales_support_agent.models.database import get_engine
        from sqlalchemy import text

        with get_engine().connect() as conn:
            row = conn.execute(
                text("SELECT access_token, refresh_token, realm_id, expires_at FROM quickbooks_tokens WHERE id = 'singleton'")
            ).fetchone()
        return dict(row._mapping) if row else None
    except Exception as exc:
        logger.warning("Could not load QB tokens: %s", exc)
        return None


def _clear_tokens() -> None:
    """Remove the stored token row."""
    try:
        from sales_support_agent.models.database import get_engine
        from sqlalchemy import text

        with get_engine().begin() as conn:
            conn.execute(text("DELETE FROM quickbooks_tokens WHERE id = 'singleton'"))
    except Exception as exc:
        logger.warning("Could not clear QB tokens: %s", exc)
