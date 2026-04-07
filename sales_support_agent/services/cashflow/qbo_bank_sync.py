"""QuickBooks Online bank transaction sync — pulls posted actuals into cash_events.

Why this replaces manual bank CSV upload
-----------------------------------------
QBO already has every posted transaction in real time (Purchase, Deposit,
BillPayment, etc.).  Pulling them directly eliminates the error-prone step
of downloading a bank CSV and uploading it through the UI.

What this module syncs
-----------------------
1.  ``Purchase``      → outflow, status=``posted``
      Covers checks, ACH debits, credit-card charges, cash purchases.
2.  ``Deposit``       → inflow, status=``posted``
      Covers bank deposits and customer payment deposits.
3.  ``Payment``       → inflow, status=``posted``
      A/R payments received from customers (separate from Deposits).

Auto-match on sync
------------------
After inserting posted actuals this module calls
``auto_match_transactions()`` so any posted transaction that matches an
open ``planned`` / ``overdue`` CashEvent is automatically linked.

Dedup strategy
--------------
Each event gets a deterministic ID:  ``qbo-{entity_type}-{qbo_id}``
(e.g. ``qbo-purchase-123``, ``qbo-deposit-456``).  Re-syncing is fully
idempotent — rows are updated in place if the amount or date changed.

Lookback window
---------------
Defaults to 90 days so that a re-sync doesn't re-import years of history
every time.  The first run will pick up 90 days; subsequent runs are
incremental because the upsert logic is idempotent.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Any, Optional

import requests

logger = logging.getLogger(__name__)

QBO_PROD_BASE = "https://quickbooks.api.intuit.com/v3/company"
QBO_SAND_BASE = "https://sandbox-quickbooks.api.intuit.com/v3/company"

# Minimum amount to bother importing (avoids $0.01 artefacts)
MIN_AMOUNT_CENTS = 50  # $0.50

# How far back to look on each sync
DEFAULT_LOOKBACK_DAYS = 90


# ---------------------------------------------------------------------------
# QBO API helpers (mirrors qbo_sync.py — kept local to avoid circular import)
# ---------------------------------------------------------------------------

def _qbo_query(base_url: str, realm_id: str, access_token: str, sql: str) -> list[dict]:
    url = f"{base_url}/{realm_id}/query"
    resp = requests.get(
        url,
        headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
        params={"query": sql, "minorversion": "70"},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    qr = data.get("QueryResponse", {})
    for key, val in qr.items():
        if isinstance(val, list):
            return val
    return []


def _parse_date(s: Any) -> Optional[date]:
    if not s:
        return None
    try:
        return date.fromisoformat(str(s)[:10])
    except Exception:
        return None


def _dollars_to_cents(val: Any) -> int:
    try:
        return round(float(val) * 100)
    except (TypeError, ValueError):
        return 0


# ---------------------------------------------------------------------------
# Entity → cash_event converters
# ---------------------------------------------------------------------------

def _purchase_to_event(p: dict) -> Optional[dict]:
    """Convert a QBO Purchase to a cash_event dict (outflow/posted)."""
    qbo_id = str(p.get("Id", ""))
    if not qbo_id:
        return None

    total = _dollars_to_cents(p.get("TotalAmt", 0))
    if total < MIN_AMOUNT_CENTS:
        return None

    txn_date = _parse_date(p.get("TxnDate"))
    payment_type = str(p.get("PaymentType", "") or "")  # Cash|Check|CreditCard

    # Vendor / payee name
    entity_ref = p.get("EntityRef") or {}
    vendor = str(entity_ref.get("name", "") or "").strip()

    # Description from first meaningful line item
    description = ""
    for line in p.get("Line", []):
        detail = line.get("AccountBasedExpenseLineDetail") or line.get("ItemBasedExpenseLineDetail") or {}
        item_ref = detail.get("ItemRef") or {}
        acct_ref = detail.get("AccountRef") or {}
        desc = (
            line.get("Description")
            or item_ref.get("name")
            or acct_ref.get("name")
            or ""
        )
        if desc:
            description = desc.strip()
            break

    memo = str(p.get("PrivateNote", "") or "").strip()
    name = vendor or description or f"QBO Purchase #{qbo_id}"

    notes = f"QBO Purchase | type:{payment_type}"
    if memo:
        notes += f" | {memo[:400]}"

    return {
        "id": f"qbo-purchase-{qbo_id}",
        "source": "qbo_bank",
        "source_id": f"purchase-{qbo_id}",
        "event_type": "outflow",
        "category": _infer_category(name),
        "subcategory": "",
        "description": description[:500],
        "name": name[:255],
        "vendor_or_customer": vendor[:255],
        "amount_cents": total,
        "due_date": txn_date,
        "status": "posted",
        "confidence": "confirmed",
        "recurring_rule": "",
        "clickup_task_id": "",
        "bank_transaction_type": payment_type[:32],
        "bank_reference": qbo_id[:128],
        "notes": notes[:500],
    }


def _deposit_to_event(dep: dict) -> Optional[dict]:
    """Convert a QBO Deposit to a cash_event dict (inflow/posted)."""
    qbo_id = str(dep.get("Id", ""))
    if not qbo_id:
        return None

    total = _dollars_to_cents(dep.get("TotalAmt", 0))
    if total < MIN_AMOUNT_CENTS:
        return None

    txn_date = _parse_date(dep.get("TxnDate"))

    # Try to find a customer/entity name from deposit lines
    customer = ""
    for line in dep.get("Line", []):
        detail = line.get("DepositLineDetail") or {}
        entity_ref = detail.get("Entity") or {}
        if entity_ref.get("name"):
            customer = str(entity_ref["name"]).strip()
            break

    memo = str(dep.get("PrivateNote", "") or "").strip()
    name = customer or memo[:60] or f"QBO Deposit #{qbo_id}"
    notes = "QBO Deposit"
    if memo:
        notes += f" | {memo[:440]}"

    return {
        "id": f"qbo-deposit-{qbo_id}",
        "source": "qbo_bank",
        "source_id": f"deposit-{qbo_id}",
        "event_type": "inflow",
        "category": "revenue",
        "subcategory": "",
        "description": memo[:500],
        "name": name[:255],
        "vendor_or_customer": customer[:255],
        "amount_cents": total,
        "due_date": txn_date,
        "status": "posted",
        "confidence": "confirmed",
        "recurring_rule": "",
        "clickup_task_id": "",
        "bank_transaction_type": "Deposit",
        "bank_reference": qbo_id[:128],
        "notes": notes[:500],
    }


def _payment_to_event(pay: dict) -> Optional[dict]:
    """Convert a QBO Payment (A/R payment received) to a cash_event dict."""
    qbo_id = str(pay.get("Id", ""))
    if not qbo_id:
        return None

    total = _dollars_to_cents(pay.get("TotalAmt", 0))
    if total < MIN_AMOUNT_CENTS:
        return None

    txn_date = _parse_date(pay.get("TxnDate"))

    customer_ref = pay.get("CustomerRef") or {}
    customer = str(customer_ref.get("name", "") or "").strip()

    memo = str(pay.get("PrivateNote", "") or "").strip()
    name = customer or f"QBO Payment #{qbo_id}"
    notes = "QBO A/R Payment"
    if memo:
        notes += f" | {memo[:440]}"

    return {
        "id": f"qbo-payment-{qbo_id}",
        "source": "qbo_bank",
        "source_id": f"payment-{qbo_id}",
        "event_type": "inflow",
        "category": "revenue",
        "subcategory": "",
        "description": memo[:500],
        "name": name[:255],
        "vendor_or_customer": customer[:255],
        "amount_cents": total,
        "due_date": txn_date,
        "status": "posted",
        "confidence": "confirmed",
        "recurring_rule": "",
        "clickup_task_id": "",
        "bank_transaction_type": "Payment",
        "bank_reference": qbo_id[:128],
        "notes": notes[:500],
    }


# ---------------------------------------------------------------------------
# Category inference (reuse keywords from clickup_sync)
# ---------------------------------------------------------------------------

_CATEGORY_KEYWORDS = {
    "payroll": ["payroll", "salary", "wages", "gusto", "adp"],
    "rent": ["rent", "lease"],
    "loan": ["loan", "capital", "fora", "stripe capital", "kyle loan"],
    "utilities": ["power", "electric", "gas", "water", "comcast", "enbridge", "lehi", "rocky mountain"],
    "insurance": ["insurance", "liberty mutual", "cincinnati", "bear river", "select benefits", "instamed"],
    "software": ["software", "google workspace", "quickbooks", "clickup", "zapier", "openai", "lovable"],
    "banking": ["chase", "citi", "american express", "capital one", "credit card"],
    "fulfillment": ["fulfillment", "von", "bonus"],
}


def _infer_category(name: str) -> str:
    name_lower = name.lower()
    for category, keywords in _CATEGORY_KEYWORDS.items():
        if any(kw in name_lower for kw in keywords):
            return category
    return "other"


# ---------------------------------------------------------------------------
# Main sync function
# ---------------------------------------------------------------------------

def sync_qbo_bank_transactions(
    settings,
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    run_auto_match: bool = True,
) -> "UploadResult":  # noqa: F821
    """Pull recent QBO Purchase, Deposit, and Payment transactions into cash_events.

    Token resolution follows the same two-path approach as ``qbo_sync.py``:
    DB tokens (web OAuth flow) take priority; env-var fallback for legacy.

    Args:
        settings: App settings object with QB env vars.
        lookback_days: How many days back to query (default 90).
        run_auto_match: If True, call auto_match_transactions() after insert
                        to link posted actuals to open planned events.

    Returns:
        UploadResult with rows_inserted / rows_skipped_duplicate / errors.
    """
    from sales_support_agent.models.database import get_engine, upsert_cash_event
    from sales_support_agent.services.cashflow.upload import UploadResult
    from sqlalchemy import text

    result = UploadResult()
    counts = {"created": 0, "updated": 0, "skipped": 0}

    try:
        # ---- Resolve access token (same two-path as qbo_sync.py) ----------
        access_token: str = ""
        realm_id: str = ""
        sandbox: bool = False

        # Path 1: DB tokens
        try:
            from sales_support_agent.api.qbo_auth_router import (
                get_valid_access_token as _get_token,
                _load_tokens,
            )
            token_row = _load_tokens()
            if token_row and token_row.get("access_token") and token_row.get("realm_id"):
                access_token = _get_token() or ""
                realm_id = token_row.get("realm_id", "")
                sandbox = False
        except Exception as exc:
            logger.debug("QBO bank sync: DB token load skipped: %s", exc)

        # Path 2: env-var fallback
        if not access_token or not realm_id:
            from sales_support_agent.services.cashflow.qbo_sync import (
                _refresh_access_token,
                _load_refresh_token,
                _save_refresh_token,
            )
            client_id     = getattr(settings, "qbo_client_id", "") or ""
            client_secret = getattr(settings, "qbo_client_secret", "") or ""
            env_realm_id  = getattr(settings, "qbo_realm_id", "") or ""
            env_refresh   = getattr(settings, "qbo_refresh_token", "") or ""
            sandbox       = getattr(settings, "qbo_sandbox", False)

            if not all([client_id, client_secret, env_realm_id]):
                msg = "QBO not connected — bank sync skipped."
                logger.warning(msg)
                result.errors.append(msg)
                return result

            realm_id = env_realm_id
            refresh_token = _load_refresh_token(realm_id, env_refresh)
            token_data = _refresh_access_token(client_id, client_secret, refresh_token)
            access_token = token_data["access_token"]
            new_refresh = token_data.get("refresh_token", refresh_token)
            if new_refresh != refresh_token:
                _save_refresh_token(realm_id, new_refresh)

        base_url = QBO_SAND_BASE if sandbox else QBO_PROD_BASE

        # ---- Build date range filter -------------------------------------
        since = (datetime.utcnow().date() - timedelta(days=lookback_days)).isoformat()

        # ---- Fetch each entity type -------------------------------------
        converters: list[tuple[str, str, Any]] = [
            (
                "Purchase",
                f"SELECT * FROM Purchase WHERE TxnDate >= '{since}' MAXRESULTS 1000",
                _purchase_to_event,
            ),
            (
                "Deposit",
                f"SELECT * FROM Deposit WHERE TxnDate >= '{since}' MAXRESULTS 1000",
                _deposit_to_event,
            ),
            (
                "Payment",
                f"SELECT * FROM Payment WHERE TxnDate >= '{since}' MAXRESULTS 1000",
                _payment_to_event,
            ),
        ]

        engine = get_engine()
        now_str = datetime.utcnow().isoformat()

        for entity_name, sql, converter in converters:
            try:
                rows = _qbo_query(base_url, realm_id, access_token, sql)
                logger.info("QBO bank sync: fetched %d %s rows", len(rows), entity_name)
            except Exception as exc:
                logger.error("QBO bank sync: %s query failed: %s", entity_name, exc)
                result.errors.append(f"{entity_name} query failed: {exc}")
                continue

            for raw in rows:
                parsed = converter(raw)
                if parsed is None:
                    counts["skipped"] += 1
                    continue
                with engine.begin() as conn:
                    op = upsert_cash_event(conn, parsed)
                if op == "created":
                    counts["created"] += 1
                else:
                    counts["updated"] += 1

        logger.info(
            "QBO bank sync complete: created=%d updated=%d skipped=%d",
            counts["created"], counts["updated"], counts["skipped"],
        )

        # ---- Auto-match posted actuals to open planned events -----------
        if run_auto_match and (counts["created"] + counts["updated"]) > 0:
            try:
                from sales_support_agent.services.cashflow.matcher import auto_match_transactions
                from sales_support_agent.services.cashflow.obligations import list_obligations

                posted = [r for r in list_obligations(status="posted") if r.get("source") == "qbo_bank"]
                planned_open = list_obligations(status="planned")

                if posted and planned_open:
                    now_str = datetime.utcnow().isoformat()
                    match_results = auto_match_transactions(posted, planned_open)
                    matched_count = 0
                    for mr in match_results:
                        if mr.planned_event_id is None:
                            continue
                        with engine.begin() as conn:
                            conn.execute(
                                text("""
                                    UPDATE cash_events
                                    SET status='matched', matched_to_id=:pid, updated_at=:now
                                    WHERE id=:cid
                                """),
                                {"pid": mr.planned_event_id, "cid": mr.csv_event_id, "now": now_str},
                            )
                            conn.execute(
                                text("""
                                    UPDATE cash_events
                                    SET status='matched', updated_at=:now
                                    WHERE id=:pid AND status IN ('planned', 'pending', 'overdue')
                                """),
                                {"pid": mr.planned_event_id, "now": now_str},
                            )
                        matched_count += 1
                    logger.info("QBO bank sync: auto-matched %d/%d transactions", matched_count, len(posted))
            except Exception as exc:
                logger.warning("QBO bank sync: auto-match failed: %s", exc)

    except Exception as exc:
        logger.error("QBO bank sync error: %s", exc)
        result.errors.append(str(exc))

    result.rows_inserted = counts["created"]
    result.rows_skipped_duplicate = counts["updated"] + counts["skipped"]
    return result
