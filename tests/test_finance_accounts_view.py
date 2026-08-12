from datetime import datetime, timezone

import pytest
from sqlalchemy import text

from sales_support_agent.models.database import create_session_factory, init_database
from sales_support_agent.services.cashflow.accounts_view import (
    load_accounts_overview,
    set_cash_role,
    spendable_cash_cents,
)
from sales_support_agent.services.cashflow.plaid import store_item


def _setup():
    factory = create_session_factory("sqlite:///:memory:")
    init_database(factory)
    engine = factory.kw["bind"]
    item_id = store_item(
        item_id="item-accts", access_token="tok", token_secret="secret",
        actor="qa@example.com", display_name="Mountain America",
    )
    return engine, item_id


def _add_account(engine, item_id, *, ext, name, subtype, cash_role,
                 available=None, current=None, mask="1234", active=True,
                 account_type="depository"):
    now = datetime.now(timezone.utc)
    with engine.begin() as connection:
        connection.execute(text("""
            INSERT INTO plaid_accounts (
                id, plaid_item_id, external_account_id, name, official_name, mask,
                account_type, subtype, cash_role, currency, current_balance_cents,
                available_balance_cents, balance_as_of, active, created_at, updated_at
            ) VALUES (
                :id, :item_id, :ext, :name, '', :mask, :account_type, :subtype, :cash_role,
                'USD', :current, :available, :now, :active, :now, :now
            )
        """), {
            "id": f"acct-{ext}", "item_id": item_id, "ext": ext, "name": name,
            "mask": mask, "subtype": subtype, "cash_role": cash_role,
            "account_type": account_type,
            "current": current, "available": available, "now": now, "active": active,
        })
    return f"acct-{ext}"


def test_spendable_counts_only_spendable_accounts_and_prefers_available():
    engine, item_id = _setup()
    _add_account(engine, item_id, ext="chk", name="Checking", subtype="checking",
                 cash_role="spendable", available=41000_00, current=41999_00)
    _add_account(engine, item_id, ext="sav", name="Savings", subtype="savings",
                 cash_role="reserve", available=132940_00, current=132940_00)

    overview = load_accounts_overview()

    # Spendable uses AVAILABLE (41000), not current (41999).
    assert overview["spendable_cents"] == 41000_00
    assert overview["reserve_cents"] == 132940_00
    assert overview["account_count"] == 2
    assert spendable_cash_cents() == 41000_00


def test_tax_account_is_protected_even_if_old_role_says_spendable():
    engine, item_id = _setup()
    _add_account(engine, item_id, ext="tax", name="TAX", subtype="checking",
                 cash_role="spendable", available=16567_03)

    result = load_accounts_overview()
    account = result["banks"][0]["accounts"][0]

    assert result["spendable_cents"] == 0
    assert result["reserve_cents"] == 16567_03
    assert account["cash_role"] == "reserve"
    assert account["tax_protected"] is True


def test_excluded_account_counts_toward_neither_total():
    engine, item_id = _setup()
    _add_account(engine, item_id, ext="chk", name="Checking", subtype="checking",
                 cash_role="spendable", available=5000_00)
    _add_account(engine, item_id, ext="cc", name="Card", subtype="credit card",
                 cash_role="excluded", current=9000_00)

    overview = load_accounts_overview()
    assert overview["spendable_cents"] == 5000_00
    assert overview["reserve_cents"] == 0


def test_credit_card_is_owed_money_and_never_cash():
    engine, item_id = _setup()
    _add_account(
        engine, item_id, ext="citi", name="Citi Simplicity", subtype="credit card",
        account_type="credit", cash_role="liability", current=268_78, mask="6352",
    )

    overview = load_accounts_overview()

    assert overview["liability_cents"] == 268_78
    assert overview["spendable_cents"] == 0
    assert overview["reserve_cents"] == 0


def test_existing_credit_account_migrates_from_reserve_to_liability():
    engine, item_id = _setup()
    account_id = _add_account(
        engine, item_id, ext="legacy-card", name="Citi Simplicity",
        subtype="credit card", account_type="credit", cash_role="reserve",
        current=268_78, mask="6352",
    )

    init_database(create_session_factory("sqlite:///:memory:"))
    from sales_support_agent.models.database import _ensure_plaid_account_columns
    _ensure_plaid_account_columns(engine)

    with engine.connect() as connection:
        role = connection.execute(
            text("SELECT cash_role FROM plaid_accounts WHERE id=:id"), {"id": account_id}
        ).scalar_one()
    assert role == "liability"


def test_accounts_grouped_by_bank():
    engine, item_id = _setup()
    other_item = store_item(
        item_id="item-2", access_token="tok2", token_secret="secret",
        actor="qa@example.com", display_name="Chase",
    )
    _add_account(engine, item_id, ext="a", name="MACU Checking", subtype="checking", cash_role="spendable", available=100)
    _add_account(engine, other_item, ext="b", name="Chase Checking", subtype="checking", cash_role="spendable", available=200)

    overview = load_accounts_overview()
    names = {bank["display_name"] for bank in overview["banks"]}
    assert names == {"Mountain America", "Chase"}
    assert overview["spendable_cents"] == 300


def test_reclassify_moves_balance_and_writes_audit():
    engine, item_id = _setup()
    acct = _add_account(engine, item_id, ext="sav", name="Savings", subtype="savings",
                        cash_role="reserve", available=10000_00)
    assert load_accounts_overview()["spendable_cents"] == 0

    applied = set_cash_role(acct, "spendable", actor="qa@example.com")
    assert applied == "spendable"
    assert load_accounts_overview()["spendable_cents"] == 10000_00

    with engine.connect() as connection:
        audit = connection.execute(text(
            "SELECT action_type FROM finance_action_audit WHERE entity_id=:id"
        ), {"id": acct}).scalar_one()
    assert audit == "plaid_account_reclassify"


def test_reclassify_rejects_unknown_role_and_account():
    engine, item_id = _setup()
    acct = _add_account(engine, item_id, ext="chk", name="Checking", subtype="checking",
                        cash_role="spendable", available=100)
    with pytest.raises(ValueError):
        set_cash_role(acct, "nonsense")
    with pytest.raises(ValueError):
        set_cash_role("does-not-exist", "reserve")


def test_disconnected_bank_accounts_are_excluded():
    engine, item_id = _setup()
    _add_account(engine, item_id, ext="chk", name="Checking", subtype="checking",
                 cash_role="spendable", available=7000_00)
    with engine.begin() as connection:
        connection.execute(text("UPDATE plaid_items SET disconnected_at=:now WHERE id=:id"),
                           {"now": datetime.now(timezone.utc), "id": item_id})

    overview = load_accounts_overview()
    assert overview["account_count"] == 0
    assert overview["spendable_cents"] == 0
