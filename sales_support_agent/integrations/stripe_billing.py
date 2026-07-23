"""Minimal Stripe Invoicing adapter with explicit idempotency."""

from __future__ import annotations

import hashlib
import hmac
import json
import time
from typing import Any

import requests


class StripeBillingError(RuntimeError):
    """Provider failure with a public-safe summary."""


class StripeBillingClient:
    def __init__(self, settings):
        self.secret_key = str(getattr(settings, "stripe_secret_key", "") or "").strip()
        self.webhook_secret = str(
            getattr(settings, "stripe_webhook_secret", "") or ""
        ).strip()
        self.base_url = str(
            getattr(settings, "stripe_api_base_url", "https://api.stripe.com")
            or "https://api.stripe.com"
        ).rstrip("/")

    @property
    def is_configured(self) -> bool:
        return bool(self.secret_key)

    @property
    def webhook_is_configured(self) -> bool:
        return bool(self.webhook_secret)

    def _request(
        self,
        method: str,
        path: str,
        *,
        data: dict[str, Any] | None = None,
        idempotency_key: str = "",
    ) -> dict[str, Any]:
        if not self.is_configured:
            raise StripeBillingError("Stripe billing is not configured.")
        headers = {"Authorization": f"Bearer {self.secret_key}"}
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key
        response = requests.request(
            method,
            f"{self.base_url}/{path.lstrip('/')}",
            headers=headers,
            data=data,
            timeout=20,
        )
        try:
            payload = response.json() if response.content else {}
        except ValueError:
            payload = {}
        if response.status_code >= 300:
            provider_message = str(
                ((payload.get("error") or {}).get("message") if isinstance(payload, dict) else "")
                or ""
            ).strip()
            message = f"Stripe {method.upper()} {path} failed ({response.status_code})."
            if provider_message:
                message = f"{message} {provider_message}"
            raise StripeBillingError(message)
        return payload if isinstance(payload, dict) else {}

    def create_customer(
        self,
        *,
        email: str,
        name: str,
        internal_account_id: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        return self._request(
            "POST",
            "/v1/customers",
            data={
                "email": email,
                "name": name,
                "metadata[building_billing_account_id]": internal_account_id,
            },
            idempotency_key=idempotency_key,
        )

    def create_invoice(
        self,
        *,
        customer_id: str,
        amount_cents: int,
        currency: str,
        description: str,
        collection_method: str,
        days_until_due: int,
        internal_invoice_id: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        self._request(
            "POST",
            "/v1/invoiceitems",
            data={
                "customer": customer_id,
                "amount": amount_cents,
                "currency": currency,
                "description": description,
                "metadata[building_invoice_id]": internal_invoice_id,
            },
            idempotency_key=f"{idempotency_key}:item",
        )
        invoice_data: dict[str, Any] = {
            "customer": customer_id,
            "collection_method": collection_method,
            "auto_advance": "true",
            "metadata[building_invoice_id]": internal_invoice_id,
        }
        if collection_method == "send_invoice":
            invoice_data["days_until_due"] = max(1, days_until_due)
        return self._request(
            "POST",
            "/v1/invoices",
            data=invoice_data,
            idempotency_key=f"{idempotency_key}:invoice",
        )

    def verify_webhook(
        self,
        *,
        payload: bytes,
        signature_header: str,
        tolerance_seconds: int = 300,
        now: int | None = None,
    ) -> dict[str, Any]:
        if not self.webhook_is_configured:
            raise StripeBillingError("Stripe webhook verification is not configured.")
        parts: dict[str, list[str]] = {}
        for item in str(signature_header or "").split(","):
            key, separator, value = item.partition("=")
            if separator:
                parts.setdefault(key.strip(), []).append(value.strip())
        try:
            timestamp = int((parts.get("t") or [""])[0])
        except ValueError as exc:
            raise StripeBillingError("Stripe signature timestamp is invalid.") from exc
        signatures = parts.get("v1") or []
        if not signatures:
            raise StripeBillingError("Stripe signature is missing.")
        current = int(time.time()) if now is None else int(now)
        if abs(current - timestamp) > max(1, tolerance_seconds):
            raise StripeBillingError("Stripe signature timestamp is outside the allowed window.")
        signed = str(timestamp).encode() + b"." + payload
        expected = hmac.new(
            self.webhook_secret.encode(), signed, hashlib.sha256
        ).hexdigest()
        if not any(hmac.compare_digest(expected, candidate) for candidate in signatures):
            raise StripeBillingError("Stripe signature is invalid.")
        try:
            event = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise StripeBillingError("Stripe webhook payload is invalid.") from exc
        if not isinstance(event, dict) or not event.get("id") or not event.get("type"):
            raise StripeBillingError("Stripe webhook event is incomplete.")
        return event
