"""Read-only Plaid banking integration for Finance.

This module intentionally exposes no payment or transfer operation.
"""

from __future__ import annotations

import hashlib
import hmac
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Mapping
from uuid import NAMESPACE_URL, uuid4, uuid5

import requests
import jwt
from sqlalchemy import text

from sales_support_agent.models.database import get_engine, insert_cash_event
from sales_support_agent.services.token_seal import seal_token, unseal_token


PLAID_BASE_URLS = {
    "sandbox": "https://sandbox.plaid.com",
    "development": "https://development.plaid.com",
    "production": "https://production.plaid.com",
}


class PlaidError(RuntimeError):
    def __init__(self, message: str, *, code: str = "plaid_error") -> None:
        super().__init__(message)
        self.code = code


class PlaidClient:
    def __init__(self, settings: Any, *, session: requests.Session | None = None) -> None:
        environment = str(settings.plaid_environment or "sandbox").lower()
        if environment not in PLAID_BASE_URLS:
            raise ValueError("PLAID_ENV must be sandbox, development, or production")
        self.base_url = PLAID_BASE_URLS[environment]
        self.client_id = str(settings.plaid_client_id or "")
        self.secret = str(settings.plaid_secret or "")
        self.webhook_url = str(settings.plaid_webhook_url or "")
        self.session = session or requests.Session()
        if not self.client_id or not self.secret:
            raise PlaidError("Plaid is not configured", code="not_configured")

    def post(self, path: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        body = {"client_id": self.client_id, "secret": self.secret, **dict(payload)}
        try:
            response = self.session.post(f"{self.base_url}{path}", json=body, timeout=30)
            data = response.json()
        except (requests.RequestException, ValueError) as exc:
            raise PlaidError("Plaid could not be reached", code="network_error") from exc
        if response.status_code >= 400 or data.get("error_code"):
            code = str(data.get("error_code") or f"http_{response.status_code}")
            message = str(data.get("display_message") or data.get("error_message") or "Plaid request failed")
            raise PlaidError(message, code=code)
        return data

    def create_link_token(self, *, client_user_id: str, access_token: str = "") -> str:
        payload: dict[str, Any] = {
            "user": {"client_user_id": client_user_id},
            "client_name": "Anata Finance",
            "country_codes": ["US"],
            "language": "en",
            "webhook": self.webhook_url,
        }
        if access_token:
            # Plaid update mode repairs credentials/consent. Products must be
            # omitted so the existing Item is updated rather than recreated.
            payload["access_token"] = access_token
        else:
            payload["products"] = ["transactions"]
            payload["transactions"] = {"days_requested": 365}
        data = self.post("/link/token/create", payload)
        return str(data["link_token"])

    def exchange_public_token(self, public_token: str) -> dict[str, str]:
        data = self.post("/item/public_token/exchange", {"public_token": public_token})
        return {"access_token": str(data["access_token"]), "item_id": str(data["item_id"])}

    def accounts_get(self, access_token: str) -> dict[str, Any]:
        return self.post("/accounts/get", {"access_token": access_token})

    def transactions_sync(self, access_token: str, *, cursor: str = "") -> dict[str, Any]:
        payload: dict[str, Any] = {"access_token": access_token, "count": 500}
        if cursor:
            payload["cursor"] = cursor
        return self.post("/transactions/sync", payload)

    def webhook_verification_key(self, key_id: str) -> dict[str, Any]:
        data = self.post("/webhook_verification_key/get", {"key_id": key_id})
        key = data.get("key")
        if not isinstance(key, dict):
            raise PlaidError("Plaid verification key is unavailable", code="verification_key_missing")
        return key


_WEBHOOK_KEY_CACHE: dict[str, dict[str, Any]] = {}


def verify_webhook(raw_body: bytes, signed_jwt: str, *, client: PlaidClient) -> dict[str, Any]:
    """Verify Plaid's ES256 signature, age, and exact raw-body digest."""
    if not signed_jwt:
        raise PlaidError("Plaid-Verification header is required", code="verification_missing")
    try:
        header = jwt.get_unverified_header(signed_jwt)
    except jwt.PyJWTError as exc:
        raise PlaidError("Plaid verification header is invalid", code="verification_invalid") from exc
    if header.get("alg") != "ES256" or not header.get("kid"):
        raise PlaidError("Plaid verification algorithm is invalid", code="verification_algorithm")
    key_id = str(header["kid"])
    jwk = _WEBHOOK_KEY_CACHE.get(key_id)
    if not jwk:
        jwk = client.webhook_verification_key(key_id)
        if jwk.get("alg") != "ES256" or jwk.get("kid") != key_id or jwk.get("kty") != "EC":
            raise PlaidError("Plaid verification key is invalid", code="verification_key_invalid")
        _WEBHOOK_KEY_CACHE[key_id] = jwk
    try:
        key = jwt.PyJWK.from_dict(jwk).key
        claims = jwt.decode(
            signed_jwt, key=key, algorithms=["ES256"],
            options={"require": ["iat", "request_body_sha256"]},
        )
    except jwt.PyJWTError as exc:
        # A rotated key may reuse a stale local cache only after a process has
        # lived through rollover. Retry once with Plaid's current JWK.
        _WEBHOOK_KEY_CACHE.pop(key_id, None)
        raise PlaidError("Plaid webhook signature is invalid", code="verification_signature") from exc
    issued_at = int(claims.get("iat") or 0)
    now = int(datetime.now(timezone.utc).timestamp())
    if issued_at > now + 30 or now - issued_at > 300:
        raise PlaidError("Plaid webhook is outside the five-minute window", code="verification_expired")
    expected_hash = str(claims.get("request_body_sha256") or "")
    actual_hash = hashlib.sha256(raw_body).hexdigest()
    if not hmac.compare_digest(expected_hash, actual_hash):
        raise PlaidError("Plaid webhook body does not match its signature", code="verification_body")
    return claims


def local_item_id_for_external(external_item_id: str) -> str | None:
    with get_engine().connect() as connection:
        row = connection.execute(
            text("SELECT id FROM plaid_items WHERE external_item_id=:external_id AND disconnected_at IS NULL"),
            {"external_id": external_item_id},
        ).fetchone()
    return str(row._mapping["id"]) if row else None


def record_webhook(external_item_id: str, *, error_code: str = "") -> str | None:
    """Record item state quickly; the receiver schedules heavier sync work."""
    local_id = local_item_id_for_external(external_item_id)
    if not local_id:
        return None
    now = datetime.now(timezone.utc)
    with get_engine().begin() as connection:
        connection.execute(text("""
            UPDATE plaid_items
            SET last_webhook_at=:now,
                status=CASE WHEN :error_code='' THEN status ELSE 'error' END,
                last_error_code=:error_code,
                updated_at=:now
            WHERE id=:id
        """), {"now": now, "error_code": error_code, "id": local_id})
    return local_id


def _cents(value: Any) -> int | None:
    if value is None:
        return None
    return int((Decimal(str(value)) * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def store_item(
    *, item_id: str, access_token: str, token_secret: str, actor: str,
    institution_id: str = "", display_name: str = "",
) -> str:
    if not token_secret:
        raise PlaidError("PLAID_TOKEN_SECRET is required", code="token_secret_missing")
    local_id = str(uuid5(NAMESPACE_URL, f"plaid-item:{item_id}"))
    now = datetime.now(timezone.utc)
    sealed = seal_token(token_secret, access_token)
    with get_engine().begin() as connection:
        connection.execute(text("""
            INSERT INTO plaid_items (
                id, scope_key, external_item_id, institution_id, display_name,
                sealed_access_token, status, created_by, created_at, updated_at
            ) VALUES (
                :id, 'default', :external_id, :institution_id, :display_name,
                :token, 'connected', :actor, :now, :now
            )
            ON CONFLICT(external_item_id) DO UPDATE SET
                institution_id=:institution_id, display_name=:display_name,
                sealed_access_token=:token, status='connected', last_error_code='',
                disconnected_at=NULL, updated_at=:now
        """), {
            "id": local_id, "external_id": item_id, "institution_id": institution_id,
            "display_name": display_name, "token": sealed, "actor": actor or "system", "now": now,
        })
        connection.execute(text("""
            INSERT INTO finance_settings (
                scope_key, cash_floor_cents, active_actual_source, updated_by,
                created_at, updated_at
            ) VALUES ('default', 1000000, 'plaid', :actor, :now, :now)
            ON CONFLICT(scope_key) DO UPDATE SET
                active_actual_source='plaid', updated_by=:actor, updated_at=:now
        """), {"actor": actor or "system", "now": now})
    return local_id


def _item_row(local_item_id: str) -> dict[str, Any]:
    with get_engine().connect() as connection:
        row = connection.execute(text("SELECT * FROM plaid_items WHERE id=:id"), {"id": local_item_id}).fetchone()
    if row is None:
        raise PlaidError("Plaid connection not found", code="item_not_found")
    return dict(row._mapping)


def connection_summary(*, settings: Any) -> dict[str, Any]:
    """Return a token-free view model for the Finance Source Center."""
    configured = bool(settings.plaid_client_id and settings.plaid_secret and settings.plaid_token_secret)
    items: list[dict[str, Any]] = []
    try:
        with get_engine().connect() as connection:
            rows = connection.execute(text("""
                SELECT item.id, item.display_name, item.status, item.last_success_at,
                       item.last_webhook_at, item.last_error_code,
                       COUNT(account.id) AS account_count,
                       MAX(account.balance_as_of) AS balance_as_of
                FROM plaid_items AS item
                LEFT JOIN plaid_accounts AS account
                  ON account.plaid_item_id=item.id AND account.active=TRUE
                WHERE item.disconnected_at IS NULL
                GROUP BY item.id, item.display_name, item.status,
                         item.last_success_at, item.last_webhook_at, item.last_error_code
                ORDER BY item.created_at
            """)).fetchall()
        items = [dict(row._mapping) for row in rows]
    except Exception:
        # A pre-migration boot should render setup, not crash Finance.
        items = []
    reconnect_codes = {
        "ITEM_LOGIN_REQUIRED", "PENDING_DISCONNECT", "PENDING_EXPIRATION",
        "ACCESS_NOT_GRANTED", "USER_PERMISSION_REVOKED",
    }
    for item in items:
        code = str(item.get("last_error_code") or "").upper()
        item["needs_reconnect"] = code in reconnect_codes
    connected = sum(1 for item in items if item.get("status") == "connected")
    last_successes = [item.get("last_success_at") for item in items if item.get("last_success_at")]
    return {
        "configured": configured,
        "environment": str(settings.plaid_environment or "sandbox"),
        "items": items,
        "connected_count": connected,
        "account_count": sum(int(item.get("account_count") or 0) for item in items),
        "last_success_at": max(last_successes) if last_successes else None,
        "needs_reconnect_count": sum(1 for item in items if item.get("needs_reconnect")),
    }


def create_update_link_token(
    local_item_id: str, *, client_user_id: str, settings: Any,
    client: PlaidClient | None = None,
) -> str:
    """Create a short-lived Link token that repairs one existing Item."""
    item = _item_row(local_item_id)
    access_token = unseal_token(settings.plaid_token_secret, str(item["sealed_access_token"]))
    return (client or PlaidClient(settings)).create_link_token(
        client_user_id=client_user_id, access_token=access_token,
    )


def stale_connected_item_ids(*, max_age_hours: int = 6) -> list[str]:
    """Return connected Items whose last successful refresh is old or missing."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
    with get_engine().connect() as connection:
        rows = connection.execute(text("""
            SELECT id FROM plaid_items
            WHERE disconnected_at IS NULL AND status='connected'
              AND (last_success_at IS NULL OR last_success_at < :cutoff)
            ORDER BY created_at
        """), {"cutoff": cutoff}).fetchall()
    return [str(row._mapping["id"]) for row in rows]


def sync_connected_items(*, settings: Any, item_ids: list[str] | None = None) -> dict[str, Any]:
    """Refresh connected Items independently so one bank cannot block another."""
    if item_ids is None:
        with get_engine().connect() as connection:
            rows = connection.execute(text("""
                SELECT id FROM plaid_items
                WHERE disconnected_at IS NULL AND status='connected'
                ORDER BY created_at
            """)).fetchall()
        item_ids = [str(row._mapping["id"]) for row in rows]
    result: dict[str, Any] = {"refreshed": 0, "failed": 0, "items": []}
    client = PlaidClient(settings)
    for item_id in item_ids:
        try:
            counts = sync_item(item_id, settings=settings, client=client)
            result["refreshed"] += 1
            result["items"].append({"item_id": item_id, "status": "ok", "sync": counts})
        except Exception as exc:
            result["failed"] += 1
            result["items"].append({
                "item_id": item_id,
                "status": "error",
                "code": exc.code if isinstance(exc, PlaidError) else "sync_error",
            })
    return result


def sync_item(local_item_id: str, *, settings: Any, client: PlaidClient | None = None) -> dict[str, int]:
    """Synchronize accounts and transactions idempotently into canonical Finance."""
    item = _item_row(local_item_id)
    access_token = unseal_token(settings.plaid_token_secret, str(item["sealed_access_token"]))
    api = client or PlaidClient(settings)
    now = datetime.now(timezone.utc)
    counts = {"accounts": 0, "added": 0, "modified": 0, "removed": 0}
    try:
        accounts_payload = api.accounts_get(access_token)
        with get_engine().begin() as connection:
            active_ids: set[str] = set()
            for account in accounts_payload.get("accounts", []):
                external_id = str(account.get("account_id") or "")
                if not external_id:
                    continue
                active_ids.add(external_id)
                balances = account.get("balances") or {}
                connection.execute(text("""
                    INSERT INTO plaid_accounts (
                        id, plaid_item_id, external_account_id, name, official_name, mask,
                        account_type, subtype, currency, current_balance_cents,
                        available_balance_cents, balance_as_of, active, created_at, updated_at
                    ) VALUES (
                        :id, :item_id, :external_id, :name, :official_name, :mask,
                        :account_type, :subtype, :currency, :current, :available,
                        :now, TRUE, :now, :now
                    ) ON CONFLICT(external_account_id) DO UPDATE SET
                        name=:name, official_name=:official_name, mask=:mask,
                        account_type=:account_type, subtype=:subtype, currency=:currency,
                        current_balance_cents=:current, available_balance_cents=:available,
                        balance_as_of=:now, active=TRUE, updated_at=:now
                """), {
                    "id": str(uuid5(NAMESPACE_URL, f"plaid-account:{external_id}")),
                    "item_id": local_item_id, "external_id": external_id,
                    "name": str(account.get("name") or ""),
                    "official_name": str(account.get("official_name") or ""),
                    "mask": str(account.get("mask") or "")[-4:],
                    "account_type": str(account.get("type") or ""),
                    "subtype": str(account.get("subtype") or ""),
                    "currency": str(balances.get("iso_currency_code") or "USD"),
                    "current": _cents(balances.get("current")),
                    "available": _cents(balances.get("available")), "now": now,
                })
                counts["accounts"] += 1
            if active_ids:
                placeholders = ",".join(f":account_{index}" for index, _ in enumerate(active_ids))
                params = {"item_id": local_item_id, **{f"account_{index}": value for index, value in enumerate(active_ids)}}
                connection.execute(text(f"UPDATE plaid_accounts SET active=FALSE WHERE plaid_item_id=:item_id AND external_account_id NOT IN ({placeholders})"), params)  # noqa: S608

        cursor = str(item.get("transactions_cursor") or "")
        while True:
            page = api.transactions_sync(access_token, cursor=cursor)
            with get_engine().begin() as connection:
                for group in ("added", "modified"):
                    for transaction in page.get(group, []):
                        _upsert_transaction(connection, transaction, now=now)
                        counts[group] += 1
                for transaction in page.get("removed", []):
                    external_id = str(transaction.get("transaction_id") or "")
                    if external_id:
                        connection.execute(text("UPDATE cash_events SET status='removed', updated_at=:now WHERE source='plaid' AND source_id=:id"), {"id": external_id, "now": now})
                        counts["removed"] += 1
            cursor = str(page.get("next_cursor") or cursor)
            if not page.get("has_more"):
                break
        with get_engine().begin() as connection:
            connection.execute(text("UPDATE plaid_items SET transactions_cursor=:cursor, status='connected', last_success_at=:now, last_error_code='', updated_at=:now WHERE id=:id"), {"cursor": cursor, "now": now, "id": local_item_id})
    except Exception as exc:
        code = exc.code if isinstance(exc, PlaidError) else "sync_error"
        with get_engine().begin() as connection:
            connection.execute(text("UPDATE plaid_items SET status='error', last_error_code=:code, updated_at=:now WHERE id=:id"), {"code": code, "now": now, "id": local_item_id})
        raise
    return counts


def _upsert_transaction(connection: Any, transaction: Mapping[str, Any], *, now: datetime) -> None:
    external_id = str(transaction.get("transaction_id") or "")
    if not external_id:
        raise PlaidError("Plaid transaction is missing its stable id", code="invalid_transaction")
    amount = _cents(transaction.get("amount")) or 0
    # Plaid uses positive for outflow and negative for inflow.
    event_type = "outflow" if amount >= 0 else "inflow"
    cents = abs(amount)
    pending = bool(transaction.get("pending"))
    event_id = str(uuid5(NAMESPACE_URL, f"plaid-transaction:{external_id}"))
    event = connection.execute(text("SELECT id FROM cash_events WHERE source='plaid' AND source_id=:source_id"), {"source_id": external_id}).fetchone()
    posted_date = str(transaction.get("date") or transaction.get("authorized_date") or "")[:10] or None
    description = str(transaction.get("merchant_name") or transaction.get("name") or "")[:255]
    if event:
        connection.execute(text("""
            UPDATE cash_events SET event_type=:event_type, amount_cents=:amount,
                due_date=:posted_date, effective_date=:posted_date, status=:status,
                name=:name, description=:description, vendor_or_customer=:name,
                confidence=:confidence, updated_at=:now
            WHERE id=:id
        """), {"event_type": event_type, "amount": cents, "posted_date": posted_date,
                "status": "pending" if pending else "posted", "name": description,
                "description": description, "confidence": "estimated" if pending else "confirmed",
                "now": now, "id": event_id})
    else:
        insert_cash_event(
            connection, id=event_id, source="plaid", source_id=external_id,
            record_kind="transaction", event_type=event_type, category="uncategorized",
            name=description, description=description, vendor_or_customer=description,
            amount_cents=cents, due_date=posted_date, status="pending" if pending else "posted",
            confidence="estimated" if pending else "confirmed", bank_reference=external_id,
            notes="Imported from Plaid; pending transactions are not settlement evidence.",
            created_at=now, updated_at=now,
        )
    payload_hash = hashlib.sha256(json.dumps(dict(transaction), sort_keys=True, default=str).encode()).hexdigest()
    connection.execute(text("""
        INSERT INTO finance_source_records (
            id, cash_event_id, source_system, scope_key, entity_type, external_id,
            payload_hash, soft_fingerprint, created_at, updated_at
        ) VALUES (
            :id, :event_id, 'plaid', 'default', 'transaction', :external_id,
            :payload_hash, '', :now, :now
        ) ON CONFLICT(source_system, scope_key, entity_type, external_id)
        DO UPDATE SET payload_hash=:payload_hash, updated_at=:now
    """), {"id": str(uuid4()), "event_id": event_id, "external_id": external_id, "payload_hash": payload_hash, "now": now})
