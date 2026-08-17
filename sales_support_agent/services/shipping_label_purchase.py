"""Persistent, fail-closed orchestration for public single-parcel label purchases."""
from __future__ import annotations

import hashlib
import os
import secrets
from datetime import datetime, timezone

from sales_support_agent.integrations.stripe_billing import StripeBillingClient
from sales_support_agent.models.database import kv_get_json, kv_set_json
from sales_support_agent.config import load_settings
from sales_support_agent.services.fulfillment_deck.wms_client import AnataWMSClient, get_wms_client


def _key(purchase_id: str) -> str:
    return f"shipping-label:{purchase_id}"


def token_matches(record: dict, token: str) -> bool:
    return secrets.compare_digest(record.get("token_hash", ""), hashlib.sha256(token.encode()).hexdigest())


def create_record(payload: dict, shipment_id: str, rates: list[dict]) -> tuple[dict, str]:
    purchase_id, token = secrets.token_urlsafe(18), secrets.token_urlsafe(32)
    record = {
        "purchase_id": purchase_id, "token_hash": hashlib.sha256(token.encode()).hexdigest(),
        "status": "rated", "shipment_id": shipment_id, "request": payload, "rates": rates,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    kv_set_json(_key(purchase_id), record)
    if not kv_get_json(_key(purchase_id)):
        raise RuntimeError("Shipping label purchase could not be persisted.")
    return record, token


def load_record(purchase_id: str) -> dict | None:
    return kv_get_json(_key(purchase_id))


def save_record(record: dict) -> None:
    record["updated_at"] = datetime.now(timezone.utc).isoformat()
    kv_set_json(_key(record["purchase_id"]), record)
    if not kv_get_json(_key(record["purchase_id"])):
        raise RuntimeError("Shipping label purchase could not be persisted.")


def stripe_client() -> StripeBillingClient:
    return StripeBillingClient(load_settings())


def publishable_key() -> str:
    return os.getenv("STRIPE_PUBLISHABLE_KEY", "").strip()


def live_wms() -> AnataWMSClient:
    client = get_wms_client()
    if not isinstance(client, AnataWMSClient):
        raise RuntimeError("Live label service is not configured.")
    return client
