from types import SimpleNamespace
from datetime import datetime, timezone
import hashlib
import json

import pytest
import jwt
from cryptography.hazmat.primitives.asymmetric import ec

from sales_support_agent.services.cashflow.plaid import (
    PlaidClient, PlaidError, _WEBHOOK_KEY_CACHE, _cents, verify_webhook,
)


def _settings(**overrides):
    values = {
        "plaid_environment": "sandbox",
        "plaid_client_id": "client",
        "plaid_secret": "secret",
        "plaid_webhook_url": "https://example.test/plaid",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_money_conversion_uses_decimal_rounding():
    assert _cents("10.235") == 1024
    assert _cents(-3.1) == -310
    assert _cents(None) is None


def test_invalid_environment_is_rejected():
    with pytest.raises(ValueError, match="PLAID_ENV"):
        PlaidClient(_settings(plaid_environment="unknown"))


def test_missing_credentials_fail_closed():
    with pytest.raises(PlaidError) as error:
        PlaidClient(_settings(plaid_client_id=""))
    assert error.value.code == "not_configured"


def test_link_token_is_transactions_only():
    client = PlaidClient(_settings())
    captured = {}
    client.post = lambda path, payload: captured.update(path=path, payload=payload) or {"link_token": "link-sandbox"}
    assert client.create_link_token(client_user_id="finance-user") == "link-sandbox"
    assert captured["path"] == "/link/token/create"
    assert captured["payload"]["products"] == ["transactions"]
    assert "auth" not in captured["payload"]["products"]
    assert "transfer" not in captured["payload"]["products"]


def test_update_link_token_repairs_existing_item_without_reinitializing_products():
    client = PlaidClient(_settings())
    captured = {}
    client.post = lambda path, payload: captured.update(path=path, payload=payload) or {"link_token": "update-sandbox"}

    assert client.create_link_token(
        client_user_id="finance-user", access_token="access-sandbox",
    ) == "update-sandbox"
    assert captured["path"] == "/link/token/create"
    assert captured["payload"]["access_token"] == "access-sandbox"
    assert "products" not in captured["payload"]
    assert "transactions" not in captured["payload"]


def _signed_webhook(raw_body: bytes, *, issued_at: int | None = None):
    private_key = ec.generate_private_key(ec.SECP256R1())
    key_data = json.loads(jwt.algorithms.ECAlgorithm.to_jwk(private_key.public_key()))
    key_data.update({"kid": "test-key", "alg": "ES256", "use": "sig"})
    claims = {
        "iat": issued_at or int(datetime.now(timezone.utc).timestamp()),
        "request_body_sha256": hashlib.sha256(raw_body).hexdigest(),
    }
    token = jwt.encode(claims, private_key, algorithm="ES256", headers={"kid": "test-key"})
    return token, key_data


def test_webhook_verifies_signature_age_and_exact_body_hash():
    raw = b'{"webhook_type":"TRANSACTIONS"}'
    token, key = _signed_webhook(raw)
    _WEBHOOK_KEY_CACHE.clear()
    client = SimpleNamespace(webhook_verification_key=lambda key_id: key)
    claims = verify_webhook(raw, token, client=client)
    assert claims["request_body_sha256"] == hashlib.sha256(raw).hexdigest()


def test_webhook_rejects_tampered_body():
    raw = b'{"webhook_type":"TRANSACTIONS"}'
    token, key = _signed_webhook(raw)
    _WEBHOOK_KEY_CACHE.clear()
    client = SimpleNamespace(webhook_verification_key=lambda key_id: key)
    with pytest.raises(PlaidError) as error:
        verify_webhook(raw + b" ", token, client=client)
    assert error.value.code == "verification_body"


def test_webhook_rejects_replay_outside_five_minutes():
    raw = b"{}"
    now = int(datetime.now(timezone.utc).timestamp())
    token, key = _signed_webhook(raw, issued_at=now - 301)
    _WEBHOOK_KEY_CACHE.clear()
    client = SimpleNamespace(webhook_verification_key=lambda key_id: key)
    with pytest.raises(PlaidError) as error:
        verify_webhook(raw, token, client=client)
    assert error.value.code == "verification_expired"
