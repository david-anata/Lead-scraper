from datetime import date, datetime, timezone

import pytest
from sqlalchemy import text

from sales_support_agent.models.database import (
    create_session_factory,
    init_database,
    insert_cash_event,
)
from sales_support_agent.services.cashflow.plaid import store_item
from sales_support_agent.services.cashflow.todays_plan import (
    build_todays_plan,
    clear_manual_pay_order,
    move_in_pay_order,
)


def _setup():
    factory = create_session_factory("sqlite:///:memory:")
    init_database(factory)
    engine = factory.kw["bind"]
    item = store_item(item_id="i", access_token="t", token_secret="s", actor="qa", display_name="B")
    now = datetime.now(timezone.utc)
    with engine.begin() as connection:
        connection.execute(text("""
            INSERT INTO plaid_accounts (id, plaid_item_id, external_account_id, name,
              official_name, mask, account_type, subtype, cash_role, currency,
              current_balance_cents, available_balance_cents, balance_as_of, active,
              created_at, updated_at)
            VALUES ('a1',:item,'e1','Checking','','1234','depository','checking',
              'spendable','USD',:bal,:bal,:now,TRUE,:now,:now)
        """), {"item": item, "now": now, "bal": 100000_00})
    return engine


def _bill(engine, cid, name, amount, due):
    now = datetime.now(timezone.utc)
    with engine.begin() as connection:
        insert_cash_event(
            connection, id=cid, source="clickup", source_id=cid,
            record_kind="obligation", event_type="outflow", category="other",
            name=name, vendor_or_customer=name, amount_cents=amount,
            due_date=due, status="planned", confidence="estimated",
            created_at=now, updated_at=now,
        )


def _names():
    return [item["name"] for item in build_todays_plan()["items"]]


def test_default_order_is_soonest_due():
    engine = _setup()
    _bill(engine, "late", "Late bill", 100_00, date(2026, 8, 10))
    _bill(engine, "early", "Early bill", 100_00, date(2026, 8, 1))
    assert _names() == ["Early bill", "Late bill"]
    assert build_todays_plan()["order"] == "due"


def test_moving_a_bill_up_persists_and_wins_over_due_date():
    engine = _setup()
    _bill(engine, "a", "First by date", 100_00, date(2026, 8, 1))
    _bill(engine, "b", "Second by date", 100_00, date(2026, 8, 5))
    _bill(engine, "c", "Third by date", 100_00, date(2026, 8, 9))

    result = move_in_pay_order("c", "up")
    assert result["moved"] is True
    assert _names() == ["First by date", "Third by date", "Second by date"]
    assert build_todays_plan()["order"] == "manual"

    # Survives a fresh read (persisted, not in-memory).
    assert _names() == ["First by date", "Third by date", "Second by date"]


def test_moving_down_works_and_edges_are_no_ops():
    engine = _setup()
    _bill(engine, "a", "A", 100_00, date(2026, 8, 1))
    _bill(engine, "b", "B", 100_00, date(2026, 8, 2))

    move_in_pay_order("a", "down")
    assert _names() == ["B", "A"]

    # "b" is now first, so moving it up is a no-op rather than an error.
    assert move_in_pay_order("b", "up")["moved"] is False
    assert _names() == ["B", "A"]

    # The last item cannot move down either.
    last_id = build_todays_plan()["items"][-1]["id"]
    assert move_in_pay_order(last_id, "down")["moved"] is False
    assert _names() == ["B", "A"]


def test_reset_returns_to_automatic_order():
    engine = _setup()
    _bill(engine, "a", "Early", 100_00, date(2026, 8, 1))
    _bill(engine, "b", "Later", 100_00, date(2026, 8, 5))
    move_in_pay_order("b", "up")
    assert _names() == ["Later", "Early"]

    cleared = clear_manual_pay_order()
    assert cleared == 2
    assert _names() == ["Early", "Later"]
    assert build_todays_plan()["order"] == "due"


def test_positions_and_edge_flags_are_reported():
    engine = _setup()
    _bill(engine, "a", "A", 100_00, date(2026, 8, 1))
    _bill(engine, "b", "B", 100_00, date(2026, 8, 2))
    items = build_todays_plan()["items"]
    assert [i["position"] for i in items] == [1, 2]
    assert items[0]["is_first"] is True and items[0]["is_last"] is False
    assert items[-1]["is_last"] is True


def test_invalid_direction_and_unknown_bill_are_rejected():
    engine = _setup()
    _bill(engine, "a", "A", 100_00, date(2026, 8, 1))
    with pytest.raises(ValueError):
        move_in_pay_order("a", "sideways")
    with pytest.raises(ValueError):
        move_in_pay_order("nope", "up")
