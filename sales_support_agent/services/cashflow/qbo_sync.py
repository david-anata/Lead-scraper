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
        from sales_support_agent.models.database import engine
        from sqlalchemy import text

        # Ensure table exists
        with engine.begin() as conn:
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
        from sales_support_agent.models.database import engine
        from sqlalchemy import text

        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT value FROM kv_store WHERE key = :key"),
                {"key": f"qbo_refresh_token:{realm_id}"},
            ).fetchone()
        if row:
            return row[0]
    except Exception:
        pass
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

def sync_qbo_invoices(settings) -> dict[str, int]:
    """Pull open QBO invoices into cash_events.

    Returns {"created": N, "updated": N, "paid": N, "cancelled": N, "skipped": N}
    """
    from sales_support_agent.models.database import engine
    from sqlalchemy import text

    if not all([settings.qbo_client_id, settings.qbo_client_secret,
                settings.qbo_realm_id]):
        logger.warning("QBO credentials not configured — skipping QBO sync")
        return {"created": 0, "updated": 0, "paid": 0, "cancelled": 0, "skipped": 0}

    realm_id = settings.qbo_realm_id
    base_url = QBO_SAND_BASE if settings.qbo_sandbox else QBO_PROD_BASE
    counts = {"created": 0, "updated": 0, "paid": 0, "cancelled": 0, "skipped": 0}

    # -- OAuth: get fresh access token ----------------------------------------
    refresh_token = _load_refresh_token(realm_id, settings.qbo_refresh_token)
    try:
        token_data = _refresh_access_token(
            settings.qbo_client_id,
            settings.qbo_client_secret,
            refresh_token,
        )
    except Exception as exc:
        logger.error("QBO OAuth token refresh failed: %s", exc)
        raise

    access_token = token_data["access_token"]
    new_refresh_token = token_data.get("refresh_token", refresh_token)
    if new_refresh_token != refresh_token:
        _save_refresh_token(realm_id, new_refresh_token)

    # -- Fetch invoices -------------------------------------------------------
    # Pull all non-void invoices modified in last 365 days (captures new, updated, paid)
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

        with engine.connect() as conn:
            existing = conn.execute(
                text("SELECT id, status FROM cash_events WHERE id = :id"),
                {"id": event_id},
            ).fetchone()

        # Handle paid / cancelled updates
        terminal_status = parsed.get("status") if "source" not in parsed else None
        if terminal_status in ("paid", "cancelled"):
            if existing:
                with engine.begin() as conn:
                    conn.execute(
                        text("UPDATE cash_events SET status=:s, updated_at=:now WHERE id=:id"),
                        {"s": terminal_status, "now": now_str, "id": event_id},
                    )
                counts[terminal_status] += 1
            # If it was never in our DB, skip — no need to create a paid invoice
            continue

        due_date_val = parsed.get("due_date")
        due_str = due_date_val.isoformat() if isinstance(due_date_val, date) else None

        if existing:
            # Update outstanding balance and date
            with engine.begin() as conn:
                conn.execute(
                    text("""
                        UPDATE cash_events SET
                            amount_cents=:amount_cents,
                            due_date=:due_date,
                            description=:description,
                            notes=:notes,
                            vendor_or_customer=:vendor_or_customer,
                            name=:name,
                            status=:status,
                            updated_at=:now
                        WHERE id=:id
                    """),
                    {
                        "amount_cents": parsed["amount_cents"],
                        "due_date": due_str,
                        "description": parsed["description"],
                        "notes": parsed["notes"],
                        "vendor_or_customer": parsed["vendor_or_customer"],
                        "name": parsed["name"],
                        "status": parsed["status"],
                        "now": now_str,
                        "id": event_id,
                    },
                )
            counts["updated"] += 1
        else:
            with engine.begin() as conn:
                conn.execute(
                    text("""
                        INSERT INTO cash_events (
                            id, source, source_id, event_type, category,
                            subcategory, description, name, vendor_or_customer,
                            amount_cents, due_date, status, confidence,
                            recurring_rule, clickup_task_id,
                            bank_transaction_type, bank_reference, notes,
                            created_at, updated_at
                        ) VALUES (
                            :id, :source, :source_id, :event_type, :category,
                            :subcategory, :description, :name, :vendor_or_customer,
                            :amount_cents, :due_date, :status, :confidence,
                            :recurring_rule, :clickup_task_id,
                            :bank_transaction_type, :bank_reference, :notes,
                            :now, :now
                        )
                    """),
                    {
                        "id": event_id,
                        "source": parsed["source"],
                        "source_id": parsed["source_id"],
                        "event_type": parsed["event_type"],
                        "category": parsed["category"],
                        "subcategory": parsed["subcategory"],
                        "description": parsed["description"],
                        "name": parsed["name"],
                        "vendor_or_customer": parsed["vendor_or_customer"],
                        "amount_cents": parsed["amount_cents"],
                        "due_date": due_str,
                        "status": parsed["status"],
                        "confidence": parsed["confidence"],
                        "recurring_rule": parsed["recurring_rule"],
                        "clickup_task_id": parsed["clickup_task_id"],
                        "bank_transaction_type": parsed["bank_transaction_type"],
                        "bank_reference": parsed["bank_reference"],
                        "notes": parsed["notes"],
                        "now": now_str,
                    },
                )
            counts["created"] += 1

    logger.info(
        "QBO sync complete: created=%d updated=%d paid=%d cancelled=%d skipped=%d",
        counts["created"], counts["updated"], counts["paid"], counts["cancelled"], counts["skipped"],
    )
    return counts
