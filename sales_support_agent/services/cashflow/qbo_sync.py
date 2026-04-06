"""QuickBooks Online (QBO) invoice sync — pulls open AR invoices into cash_events.

Why this matters for forecast accuracy
---------------------------------------
QBO records customer *payments* as a single lump sum that references multiple
invoices.  When that payment hits the bank, the CSV row shows one deposit for
the combined amount.  Without invoice-level data the forecast only sees an
opaque blob; with invoice-level data every outstanding invoice becomes its own
AR event with the correct due date, customer, and amount.

What this module syncs
-----------------------
1.  Open/partially-paid invoices  → ``source='qbo'``, ``event_type='inflow'``
    ``amount_cents`` = outstanding *balance* (not the invoice total)
2.  Fully paid invoices          → status set to ``'paid'`` in cash_events
3.  Void / deleted invoices      → status set to ``'cancelled'``

Auto-match on upload
---------------------
After a bank CSV upload, ``upload.py`` already calls
``auto_match_transactions()``.  Because QBO AR events are in cash_events with
``event_type='inflow'`` and ``status='planned'``, the matcher will link bank
deposits to open QBO invoices automatically.

Setup (env vars)
-----------------
QBO_CLIENT_ID          Intuit app client ID
QBO_CLIENT_SECRET      Intuit app client secret
QBO_REFRESH_TOKEN      Long-lived OAuth 2.0 refresh token
QBO_REALM_ID           Company ID  (visible in QBO URL: /app/dashboard?cid=…)
QBO_SANDBOX            "true" to use sandbox endpoint (default: false)

Getting a refresh token
-----------------------
1.  Create an app at https://developer.intuit.com/
2.  Set redirect URI to https://agent.anatainc.com/admin/finances/qbo/callback
3.  Scope: ``com.intuit.quickbooks.accounting``
4.  Complete OAuth flow once manually (or use the Intuit OAuth playground)
5.  Store the refresh_token in the env var — it is valid for 100 days and
    auto-rotates on each use (new token is saved to DB for the next call).
"""

from __future__ import annotations

import base64
import logging
import uuid
from datetime import date, datetime
from typing import Any, Optional

import requests

logger = logging.getLogger(__name__)

QBO_PROD_BASE = "https://quickbooks.api.intuit.com/v3/company"
QBO_SAND_BASE = "https://sandbox-quickbooks.api.intuit.com/v3/company"
QBO_TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"

# Minimum balance to create an AR event (avoids $0.01 rounding artefacts)
MIN_BALANCE_CENTS = 50  # $0.50


# ---------------------------------------------------------------------------
# OAuth helpers
# ---------------------------------------------------------------------------

def _refresh_access_token(client_id: str, client_secret: str, refresh_token: str) -> dict:
    """Exchange a QBO refresh token for a new access + refresh token pair.

    Returns the full token response dict:
        {access_token, refresh_token, token_type, expires_in, ...}
    Raises requests.HTTPError on failure.
    """
    credentials = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    resp = requests.post(
        QBO_TOKEN_URL,
        headers={
            "Authorization": f"Basic {credentials}",
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        },
        data={"grant_type": "refresh_token", "refresh_token": refresh_token},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def _save_refresh_token(realm_id: str, new_refresh_token: str) -> None:
    """Persist the rotated refresh token so the next sync can use it.

    Stores in a simple key-value table ``kv_store`` (id=qbo_refresh_token).
    Falls back silently if the table doesn't exist yet (first run).
    """
    try:
        from sales_support_agent.models.database import get_engine
        from sqlalchemy import text

        # Ensure table exists
        with get_engine().begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS kv_store (
                    key   TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """))
            conn.execute(text("""
                INSERT INTO kv_store (key, value, updated_at)
                VALUES (:key, :value, :now)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
            """), {"key": f"qbo_refresh_token:{realm_id}", "value": new_refresh_token, "now": datetime.utcnow().isoformat()})
    except Exception as exc:
        logger.warning("Could not persist QBO refresh token: %s", exc)


def _load_refresh_token(realm_id: str, fallback: str) -> str:
    """Load a persisted refresh token from DB, falling back to env-var value."""
    try:
        from sales_support_agent.models.database import get_engine
        from sqlalchemy import text

        with get_engine().connect() as conn:
            row = conn.execute(
                text("SELECT value FROM kv_store WHERE key = :key"),
                {"key": f"qbo_refresh_token:{realm_id}"},
            ).fetchone()
        if row:
            return row[0]
    except Exception as exc:
        logger.warning("Could not load QBO refresh token from kv_store (using env fallback): %s", exc)
    return fallback


# ---------------------------------------------------------------------------
# QBO API helpers
# ---------------------------------------------------------------------------

def _query(base_url: str, realm_id: str, access_token: str, sql: str) -> list[dict]:
    """Run a QBO SQL query and return the entity rows."""
    url = f"{base_url}/{realm_id}/query"
    resp = requests.get(
        url,
        headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
        params={"query": sql, "minorversion": "70"},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    query_response = data.get("QueryResponse", {})
    # The key name matches the entity: Invoice, Customer, etc.
    for key, val in query_response.items():
        if isinstance(val, list):
            return val
    return []


def _parse_qbo_date(s: Any) -> Optional[date]:
    if not s:
        return None
    try:
        return date.fromisoformat(str(s)[:10])
    except Exception:
        return None


def _invoice_to_event(inv: dict) -> Optional[dict]:
    """Convert a raw QBO Invoice dict to a cash_events-compatible dict.

    Returns None if the invoice should be skipped (zero balance, void).
    """
    # QBO invoice status
    active = str(inv.get("Active", "true")).lower()
    doc_num = inv.get("DocNumber", "")
    inv_id = str(inv.get("Id", ""))

    if active == "false":
        # Void/deleted — mark as cancelled
        return {
            "qbo_invoice_id": inv_id,
            "status": "cancelled",
        }

    balance = float(inv.get("Balance", 0) or 0)
    total_amt = float(inv.get("TotalAmt", 0) or 0)

    balance_cents = round(balance * 100)
    total_cents = round(total_amt * 100)

    if balance_cents < MIN_BALANCE_CENTS:
        # Fully paid (balance rounds to zero)
        return {
            "qbo_invoice_id": inv_id,
            "status": "paid",
        }

    # Customer name
    customer_ref = inv.get("CustomerRef", {}) or {}
    customer_name = str(customer_ref.get("name", "") or "").strip()

    # Dates
    due_date = _parse_qbo_date(inv.get("DueDate") or inv.get("TxnDate"))
    txn_date = _parse_qbo_date(inv.get("TxnDate"))

    # Build description from line items
    lines: list[str] = []
    for line in inv.get("Line", []):
        if line.get("LineNum") is None:
            continue  # subtotal / discount lines
        detail = line.get("SalesItemLineDetail", {}) or {}
        item_ref = detail.get("ItemRef", {}) or {}
        item_name = item_ref.get("name", "") or line.get("Description", "") or ""
        line_amt = float(line.get("Amount", 0) or 0)
        if item_name or line_amt:
            lines.append(f"{item_name}: ${line_amt:,.2f}" if item_name else f"${line_amt:,.2f}")
    description = f"Invoice #{doc_num}" + (f" — {'; '.join(lines)}" if lines else "")

    # Notes includes payment terms if present
    terms_ref = inv.get("SalesTermRef", {}) or {}
    terms = str(terms_ref.get("name", "") or "").strip()
    notes = f"QBO Invoice #{doc_num}" + (f" | Terms: {terms}" if terms else "") + \
            (f" | Invoiced: ${total_amt:,.2f}" if total_cents != balance_cents else "")

    return {
        "qbo_invoice_id": inv_id,
        "source": "qbo",
        "source_id": f"qbo-inv-{inv_id}",
        "event_type": "inflow",
        "category": "revenue",
        "subcategory": "",
        "description": description[:500],
        "name": customer_name or f"Invoice #{doc_num}",
        "vendor_or_customer": customer_name,
        "amount_cents": balance_cents,
        "due_date": due_date or txn_date,
        "status": "planned",
        "confidence": "confirmed",   # real invoiced amount, not an estimate
        "recurring_rule": "",
        "clickup_task_id": "",
        "bank_transaction_type": "",
        "bank_reference": "",
        "notes": notes[:500],
    }


# ---------------------------------------------------------------------------
# Main sync function
# ---------------------------------------------------------------------------

def sync_qbo_invoices(settings):
    """Pull open QBO invoices into cash_events.

    Token resolution order:
      1. DB (quickbooks_tokens table) — populated by the web OAuth flow at
         /admin/finances/qbo/connect.  Tokens are auto-refreshed via
         get_valid_access_token() before every sync.
      2. Env-var fallback — if DB tokens are absent, falls back to the legacy
         QBO_REFRESH_TOKEN / QBO_CLIENT_ID / QBO_CLIENT_SECRET / QBO_REALM_ID
         env vars.  This keeps the app working for deployments that haven't
         completed the web OAuth flow yet.

    Returns UploadResult (rows_inserted=created, rows_skipped_duplicate=updated+paid+cancelled).
    """
    from sales_support_agent.models.database import get_engine, upsert_cash_event
    from sales_support_agent.services.cashflow.upload import UploadResult
    from sqlalchemy import text

    result = UploadResult()
    counts = {"created": 0, "updated": 0, "paid": 0, "cancelled": 0, "skipped": 0}

    try:
        # -- Resolve access token + realm ID (DB-first, env-var fallback) -------
        access_token: str = ""
        realm_id:     str = ""
        sandbox:      bool = False

        # Path 1: DB tokens (set via web OAuth flow at /admin/finances/qbo/connect)
        try:
            from sales_support_agent.api.qbo_auth_router import (
                get_valid_access_token as _get_token,
                _load_tokens,
            )
            token_row = _load_tokens()
            if token_row and token_row.get("access_token") and token_row.get("realm_id"):
                access_token = _get_token() or ""
                realm_id = token_row.get("realm_id", "")
                sandbox = getattr(settings, "qbo_sandbox", False)
                logger.info("QBO sync: using DB OAuth tokens (realm=%s)", realm_id)
        except Exception as exc:
            logger.debug("QBO DB token load skipped: %s", exc)

        # Path 2: env-var fallback
        if not access_token or not realm_id:
            client_id     = getattr(settings, "qbo_client_id", "") or ""
            client_secret = getattr(settings, "qbo_client_secret", "") or ""
            env_realm_id  = getattr(settings, "qbo_realm_id", "") or ""
            env_refresh   = getattr(settings, "qbo_refresh_token", "") or ""
            sandbox       = getattr(settings, "qbo_sandbox", False)

            if not all([client_id, client_secret, env_realm_id]):
                msg = (
                    "QBO not connected. Complete the OAuth flow at "
                    "/admin/finances/qbo/connect, or set QBO_CLIENT_ID, "
                    "QBO_CLIENT_SECRET, QBO_REALM_ID, and QBO_REFRESH_TOKEN env vars."
                )
                logger.warning(msg)
                result.errors.append(msg)
                return result

            realm_id = env_realm_id
            refresh_token = _load_refresh_token(realm_id, env_refresh)
            try:
                token_data = _refresh_access_token(client_id, client_secret, refresh_token)
            except Exception as exc:
                logger.error("QBO OAuth token refresh failed: %s", exc)
                raise
            access_token = token_data["access_token"]
            new_refresh = token_data.get("refresh_token", refresh_token)
            if new_refresh != refresh_token:
                _save_refresh_token(realm_id, new_refresh)
            logger.info("QBO sync: using env-var OAuth tokens (realm=%s)", realm_id)

        base_url = QBO_SAND_BASE if sandbox else QBO_PROD_BASE

        # -- Fetch invoices -------------------------------------------------------
        try:
            invoices = _query(
                base_url, realm_id, access_token,
                "SELECT * FROM Invoice MAXRESULTS 1000",
            )
        except Exception as exc:
            logger.error("QBO Invoice query failed: %s", exc)
            raise

        logger.info("QBO sync: fetched %d invoices", len(invoices))

        # -- Upsert into cash_events ----------------------------------------------
        now_str = datetime.utcnow().isoformat()

        for raw_inv in invoices:
            inv_id = str(raw_inv.get("Id", ""))
            if not inv_id:
                counts["skipped"] += 1
                continue

            parsed = _invoice_to_event(raw_inv)
            if parsed is None:
                counts["skipped"] += 1
                continue

            event_id = f"qbo-inv-{inv_id}"

            # Handle paid / cancelled — these are terminal status-only updates
            # (no "source" key means it's a paid/cancelled stub from _invoice_to_event)
            terminal_status = parsed.get("status") if "source" not in parsed else None
            if terminal_status in ("paid", "cancelled"):
                with get_engine().connect() as conn:
                    existing = conn.execute(
                        text("SELECT id FROM cash_events WHERE id = :id"),
                        {"id": event_id},
                    ).fetchone()
                if existing:
                    with get_engine().begin() as conn:
                        conn.execute(
                            text("UPDATE cash_events SET status=:s, updated_at=:now WHERE id=:id"),
                            {"s": terminal_status, "now": now_str, "id": event_id},
                        )
                    counts[terminal_status] += 1
                continue

            # Active invoice — use shared upsert helper
            parsed["id"] = event_id
            with get_engine().begin() as conn:
                upsert_result = upsert_cash_event(conn, parsed)
            if upsert_result == "created":
                counts["created"] += 1
            else:
                counts["updated"] += 1

        logger.info(
            "QBO sync complete: created=%d updated=%d paid=%d cancelled=%d skipped=%d",
            counts["created"], counts["updated"], counts["paid"], counts["cancelled"], counts["skipped"],
        )

    except Exception as exc:
        logger.error("QBO sync error: %s", exc)
        result.errors.append(str(exc))

    result.rows_inserted = counts["created"]
    result.rows_skipped_duplicate = counts["updated"] + counts["paid"] + counts["cancelled"] + counts["skipped"]
    return result
