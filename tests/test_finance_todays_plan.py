from datetime import date, datetime, timezone

from sqlalchemy import text

from sales_support_agent.models.database import (
    create_session_factory,
    init_database,
    insert_cash_event,
)
from sales_support_agent.services.cashflow.plaid import store_item
from sales_support_agent.services.cashflow.todays_plan import build_todays_plan


def _setup():
    factory = create_session_factory("sqlite:///:memory:")
    init_database(factory)
    return factory.kw["bind"]


def _set_spendable(engine, cents):
    item = store_item(item_id="i", access_token="t", token_secret="s",
                      actor="qa", display_name="Bank")
    now = datetime.now(timezone.utc)
    with engine.begin() as connection:
        connection.execute(text("""
            INSERT INTO plaid_accounts (
                id, plaid_item_id, external_account_id, name, official_name, mask,
                account_type, subtype, cash_role, currency, current_balance_cents,
                available_balance_cents, balance_as_of, active, created_at, updated_at
            ) VALUES (
                'a1', :item, 'ext-a1', 'Checking', '', '1234', 'depository', 'checking',
                'spendable', 'USD', :bal, :bal, :now, TRUE, :now, :now
            )
        """), {"item": item, "bal": cents, "now": now})


def _add_bill(engine, *, cid, name, amount, due, status="planned", priority="review"):
    now = datetime.now(timezone.utc)
    with engine.begin() as connection:
        insert_cash_event(
            connection, id=cid, source="clickup", source_id=cid,
            record_kind="obligation", event_type="outflow", category="rent",
            name=name, vendor_or_customer=name, amount_cents=amount,
            due_date=due, status=status, confidence="estimated",
            pay_priority=priority, created_at=now, updated_at=now,
        )


def test_coverage_flags_first_uncovered_and_shortfall():
    engine = _setup()
    _set_spendable(engine, 30000_00)
    _add_bill(engine, cid="b1", name="Rent", amount=12000_00, due=date(2026, 7, 25))
    _add_bill(engine, cid="b2", name="Payroll", amount=15000_00, due=date(2026, 7, 26))
    _add_bill(engine, cid="b3", name="Software", amount=8000_00, due=date(2026, 7, 27))

    plan = build_todays_plan(order="due")
    assert plan["total_due_cents"] == 35000_00
    assert plan["shortfall_cents"] == 5000_00           # 35000 - 30000
    # 12000 + 15000 = 27000 covered; +8000 = 35000 > 30000 -> third is short
    assert [i["covered"] for i in plan["items"]] == [True, True, False]
    assert plan["first_uncovered_index"] == 2


def test_everything_covered_when_checking_is_enough():
    engine = _setup()
    _set_spendable(engine, 50000_00)
    _add_bill(engine, cid="b1", name="Rent", amount=12000_00, due=date(2026, 7, 25))
    plan = build_todays_plan()
    assert plan["shortfall_cents"] == 0
    assert plan["covered_all"] is True
    assert plan["first_uncovered_index"] is None


def test_orders_by_due_date():
    engine = _setup()
    _set_spendable(engine, 100000_00)
    _add_bill(engine, cid="late", name="Late", amount=100_00, due=date(2026, 8, 1))
    _add_bill(engine, cid="early", name="Early", amount=100_00, due=date(2026, 7, 20))
    plan = build_todays_plan(order="due")
    assert [i["name"] for i in plan["items"]] == ["Early", "Late"]


def test_only_open_positive_bills_are_planned():
    engine = _setup()
    _set_spendable(engine, 100000_00)
    _add_bill(engine, cid="open", name="Open", amount=500_00, due=date(2026, 7, 25), status="planned")
    _add_bill(engine, cid="paid", name="Paid", amount=999_00, due=date(2026, 7, 25), status="posted")
    _add_bill(engine, cid="zero", name="Zero", amount=0, due=date(2026, 7, 25), status="planned")
    plan = build_todays_plan()
    assert [i["name"] for i in plan["items"]] == ["Open"]


def test_priority_order_puts_critical_first():
    engine = _setup()
    _set_spendable(engine, 100000_00)
    _add_bill(engine, cid="flex", name="Flexible", amount=100_00, due=date(2026, 7, 20), priority="flexible")
    _add_bill(engine, cid="crit", name="Critical", amount=100_00, due=date(2026, 7, 28), priority="critical")
    plan = build_todays_plan(order="priority")
    assert plan["items"][0]["name"] == "Critical"
