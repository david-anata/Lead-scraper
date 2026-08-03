from types import SimpleNamespace
from datetime import datetime, timezone
import hashlib
import json

import pytest
import jwt
from cryptography.hazmat.primitives.asymmetric import ec
from sqlalchemy import text

from sales_support_agent.models.database import (
    _ensure_plaid_environment_column,
    create_session_factory,
    init_database,
)
from sales_support_agent.services.cashflow.plaid import (
    PlaidClient, PlaidError, _WEBHOOK_KEY_CACHE, _cents, disconnect_item, store_item,
    verify_webhook,
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
    assert captured["payload"]["transactions"]["days_requested"] == 730
    assert "auth" not in captured["payload"]["products"]
    assert "transfer" not in captured["payload"]["products"]


def test_link_token_includes_configured_oauth_redirect():
    client = PlaidClient(_settings(plaid_redirect_uri="https://agent.example/plaid/oauth-return"))
    captured = {}
    client.post = lambda path, payload: captured.update(path=path, payload=payload) or {"link_token": "link-sandbox"}

    client.create_link_token(client_user_id="finance-user")

    assert captured["payload"]["redirect_uri"] == "https://agent.example/plaid/oauth-return"


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


def test_store_item_supplies_required_status_fields():
    factory = create_session_factory("sqlite:///:memory:")
    init_database(factory)
    finance_engine = factory.kw["bind"]
    local_id = store_item(
        item_id="sandbox-item",
        access_token="sandbox-access-token",
        token_secret="test-token-secret",
        actor="qa@example.com",
        institution_id="ins_test",
        display_name="First Platypus Bank",
    )

    with finance_engine.connect() as connection:
        row = connection.execute(
            text(
                "SELECT status, last_error_code, transactions_cursor "
                "FROM plaid_items WHERE id=:id"
            ),
            {"id": local_id},
        ).one()
    assert tuple(row) == ("connected", "", "")


def test_connection_summary_hides_items_from_other_environment():
    factory = create_session_factory("sqlite:///:memory:")
    init_database(factory)
    store_item(
        item_id="sandbox-only-item",
        access_token="sandbox-access-token",
        token_secret="test-token-secret",
        actor="qa@example.com",
        environment="sandbox",
    )

    summary = __import__(
        "sales_support_agent.services.cashflow.plaid",
        fromlist=["connection_summary"],
    ).connection_summary(
        settings=_settings(
            plaid_environment="production",
            plaid_token_secret="test-token-secret",
        )
    )

    assert summary["environment"] == "production"
    assert summary["items"] == []
    assert summary["connected_count"] == 0


def test_sync_rejects_item_from_other_environment():
    factory = create_session_factory("sqlite:///:memory:")
    init_database(factory)
    local_id = store_item(
        item_id="sandbox-sync-item",
        access_token="sandbox-access-token",
        token_secret="test-token-secret",
        actor="qa@example.com",
        environment="sandbox",
    )
    plaid = __import__(
        "sales_support_agent.services.cashflow.plaid",
        fromlist=["sync_item"],
    )

    with pytest.raises(PlaidError) as error:
        plaid.sync_item(
            local_id,
            settings=_settings(
                plaid_environment="production",
                plaid_token_secret="test-token-secret",
            ),
        )

    assert error.value.code == "environment_mismatch"


def test_production_cutover_promotes_only_post_approval_legacy_items():
    factory = create_session_factory("sqlite:///:memory:")
    init_database(factory)
    finance_engine = factory.kw["bind"]
    old_id = store_item(
        item_id="sandbox-before-approval",
        access_token="sandbox-access-token",
        token_secret="test-token-secret",
        actor="qa@example.com",
        environment="sandbox",
    )
    pilot_id = store_item(
        item_id="pilot-after-approval",
        access_token="production-access-token",
        token_secret="test-token-secret",
        actor="qa@example.com",
        environment="sandbox",
    )
    with finance_engine.begin() as connection:
        connection.execute(
            text("UPDATE plaid_items SET created_at=:created_at WHERE id=:id"),
            {"id": old_id, "created_at": datetime(2026, 7, 22, tzinfo=timezone.utc)},
        )
        connection.execute(
            text("UPDATE plaid_items SET created_at=:created_at WHERE id=:id"),
            {"id": pilot_id, "created_at": datetime(2026, 7, 24, tzinfo=timezone.utc)},
        )

    _ensure_plaid_environment_column(
        finance_engine,
        current_environment="production",
    )

    with finance_engine.connect() as connection:
        environments = dict(connection.execute(text(
            "SELECT external_item_id, environment FROM plaid_items"
        )).fetchall())
    assert environments["sandbox-before-approval"] == "sandbox"
    assert environments["pilot-after-approval"] == "production"


def test_disconnect_revokes_item_destroys_token_and_records_audit():
    factory = create_session_factory("sqlite:///:memory:")
    init_database(factory)
    finance_engine = factory.kw["bind"]
    local_id = store_item(
        item_id="sandbox-disconnect-item",
        access_token="sandbox-access-token",
        token_secret="test-token-secret",
        actor="qa@example.com",
    )
    client = SimpleNamespace(remove_item=lambda token: {"request_id": "request-safe"})

    disconnect_item(
        local_id,
        settings=SimpleNamespace(plaid_token_secret="test-token-secret"),
        actor="qa@example.com",
        client=client,
    )

    with finance_engine.connect() as connection:
        item = connection.execute(text(
            "SELECT status, sealed_access_token, disconnected_at FROM plaid_items WHERE id=:id"
        ), {"id": local_id}).one()
        audit = connection.execute(text(
            "SELECT action_type, actor FROM finance_action_audit WHERE entity_id=:id"
        ), {"id": local_id}).one()
    assert item.status == "disconnected"
    assert item.sealed_access_token == ""
    assert item.disconnected_at is not None
    assert tuple(audit) == ("plaid_disconnect", "qa@example.com")


def test_disconnect_failure_preserves_credential_and_connected_state():
    factory = create_session_factory("sqlite:///:memory:")
    init_database(factory)
    finance_engine = factory.kw["bind"]
    local_id = store_item(
        item_id="sandbox-removal-failure",
        access_token="sandbox-access-token",
        token_secret="test-token-secret",
        actor="qa@example.com",
    )
    client = SimpleNamespace(remove_item=lambda token: (_ for _ in ()).throw(
        PlaidError("Removal failed", code="ITEM_ERROR")
    ))

    with pytest.raises(PlaidError):
        disconnect_item(
            local_id,
            settings=SimpleNamespace(plaid_token_secret="test-token-secret"),
            actor="qa@example.com",
            client=client,
        )

    with finance_engine.connect() as connection:
        item = connection.execute(text(
            "SELECT status, sealed_access_token, disconnected_at FROM plaid_items WHERE id=:id"
        ), {"id": local_id}).one()
    assert item.status == "connected"
    assert item.sealed_access_token
    assert item.disconnected_at is None


def _seed_plaid_transaction(engine, *, source_id: str, status: str = "posted") -> str:
    from sales_support_agent.models.database import insert_cash_event
    now = datetime.now(timezone.utc)
    event_id = f"plaid-event-{source_id}"
    with engine.begin() as connection:
        insert_cash_event(
            connection, id=event_id, source="plaid", source_id=source_id,
            record_kind="transaction", event_type="outflow", category="uncategorized",
            name="First Platypus charge", description="First Platypus charge",
            vendor_or_customer="First Platypus charge", amount_cents=4200,
            status=status, confidence="confirmed", bank_reference=source_id,
            created_at=now, updated_at=now,
        )
    return event_id


def _event_status(engine, event_id: str) -> str:
    with engine.connect() as connection:
        return connection.execute(
            text("SELECT status FROM cash_events WHERE id=:id"), {"id": event_id}
        ).scalar_one()


def test_disconnect_releases_item_when_plaid_cannot_recognize_token():
    factory = create_session_factory("sqlite:///:memory:")
    init_database(factory)
    finance_engine = factory.kw["bind"]
    local_id = store_item(
        item_id="sandbox-stranded-item",
        access_token="sandbox-access-token",
        token_secret="test-token-secret",
        actor="qa@example.com",
        display_name="First Platypus Bank",
    )
    fake_txn = _seed_plaid_transaction(finance_engine, source_id="txn-fake-1")
    # Plaid rejects a Sandbox token used against Production with this code.
    client = SimpleNamespace(remove_item=lambda token: (_ for _ in ()).throw(
        PlaidError("access token invalid", code="INVALID_ACCESS_TOKEN")
    ))

    disconnect_item(
        local_id,
        settings=SimpleNamespace(plaid_token_secret="test-token-secret"),
        actor="qa@example.com",
        client=client,
    )

    with finance_engine.connect() as connection:
        item = connection.execute(text(
            "SELECT status, sealed_access_token, disconnected_at FROM plaid_items WHERE id=:id"
        ), {"id": local_id}).one()
        evidence = connection.execute(text(
            "SELECT evidence_json FROM finance_action_audit WHERE entity_id=:id"
        ), {"id": local_id}).scalar_one()
    assert item.status == "disconnected"
    assert item.sealed_access_token == ""
    assert item.disconnected_at is not None
    assert json.loads(evidence)["forced"] is True
    assert json.loads(evidence)["plaid_code"] == "INVALID_ACCESS_TOKEN"
    # The stranded connection's imported transactions stop counting.
    assert _event_status(finance_engine, fake_txn) == "removed"


def test_disconnect_keeps_transactions_while_another_bank_stays_connected():
    factory = create_session_factory("sqlite:///:memory:")
    init_database(factory)
    finance_engine = factory.kw["bind"]
    stranded = store_item(
        item_id="sandbox-stranded", access_token="sandbox-access-token",
        token_secret="test-token-secret", actor="qa@example.com",
        display_name="First Platypus Bank",
    )
    store_item(
        item_id="live-real-bank", access_token="real-access-token",
        token_secret="test-token-secret", actor="qa@example.com",
        display_name="Real Bank",
    )
    live_txn = _seed_plaid_transaction(finance_engine, source_id="txn-real-1")
    client = SimpleNamespace(remove_item=lambda token: (_ for _ in ()).throw(
        PlaidError("access token invalid", code="INVALID_ACCESS_TOKEN")
    ))

    disconnect_item(
        stranded,
        settings=SimpleNamespace(plaid_token_secret="test-token-secret"),
        actor="qa@example.com",
        client=client,
    )

    # A live bank remains, so no transactions are touched.
    assert _event_status(finance_engine, live_txn) == "posted"


def test_force_disconnect_releases_item_that_normal_disconnect_cannot():
    factory = create_session_factory("sqlite:///:memory:")
    init_database(factory)
    finance_engine = factory.kw["bind"]
    local_id = store_item(
        item_id="sandbox-force-item", access_token="sandbox-access-token",
        token_secret="test-token-secret", actor="qa@example.com",
        display_name="First Platypus Bank",
    )
    fake_txn = _seed_plaid_transaction(finance_engine, source_id="txn-force-1")
    # A code that is NOT auto-releasable: normal disconnect must keep blocking.
    client = SimpleNamespace(remove_item=lambda token: (_ for _ in ()).throw(
        PlaidError("removal failed", code="ITEM_ERROR")
    ))
    settings = SimpleNamespace(plaid_token_secret="test-token-secret")

    with pytest.raises(PlaidError):
        disconnect_item(local_id, settings=settings, actor="qa@example.com", client=client)

    # Force clears it locally regardless of what Plaid returns.
    disconnect_item(local_id, settings=settings, actor="qa@example.com", client=client, force=True)

    with finance_engine.connect() as connection:
        item = connection.execute(text(
            "SELECT status, sealed_access_token, disconnected_at FROM plaid_items WHERE id=:id"
        ), {"id": local_id}).one()
        evidence = connection.execute(text(
            "SELECT evidence_json FROM finance_action_audit WHERE entity_id=:id"
        ), {"id": local_id}).scalar_one()
    assert item.status == "disconnected"
    assert item.sealed_access_token == ""
    assert item.disconnected_at is not None
    assert json.loads(evidence)["forced"] is True
    assert _event_status(finance_engine, fake_txn) == "removed"


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
