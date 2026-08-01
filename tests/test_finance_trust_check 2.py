from datetime import date, datetime, timezone

from sqlalchemy import text

from sales_support_agent.models.database import (
    create_session_factory,
    init_database,
    insert_cash_event,
)
from sales_support_agent.services.cashflow.plaid import store_item
from sales_support_agent.services.cashflow.trust_check import build_trust_check

AS_OF = date(2026, 7, 24)


def _setup():
    factory = create_session_factory("sqlite:///:memory:")
    init_database(factory)
    return factory.kw["bind"]


def _add_account(engine, item, ext, subtype, role, bal):
    now = datetime.now(timezone.utc)
    with engine.begin() as c:
        c.execute(text("""
            INSERT INTO plaid_accounts (id, plaid_item_id, external_account_id, name,
              official_name, mask, account_type, subtype, cash_role, currency,
              current_balance_cents, available_balance_cents, balance_as_of, active,
              created_at, updated_at)
            VALUES (:id,:item,:ext,'Acct','','1234','depository',:sub,:role,'USD',
              :bal,:bal,:now,TRUE,:now,:now)
        """), {"id": "ac-"+ext, "item": item, "ext": ext, "sub": subtype, "role": role,
               "bal": bal, "now": now})


def _add_receivable(engine, cid, customer, amount, due):
    now = datetime.now(timezone.utc)
    with engine.begin() as c:
        insert_cash_event(c, id=cid, source="qbo", source_id=cid, record_kind="obligation",
            event_type="inflow", category="revenue", name=customer, vendor_or_customer=customer,
            amount_cents=amount, due_date=due, status="overdue", confidence="estimated",
            created_at=now, updated_at=now)


def test_cash_ties_to_account_total_and_matches_flag():
    engine = _setup()
    item = store_item(item_id="i", access_token="t", token_secret="s", actor="qa", display_name="B")
    _add_account(engine, item, "chk", "checking", "spendable", 30000_00)
    _add_account(engine, item, "sav", "savings", "reserve", 132940_00)

    tc = build_trust_check(cash_on_hand_cents=162940_00, payable_issues=[], as_of=AS_OF)
    assert tc["spendable_cents"] == 30000_00
    assert tc["reserve_cents"] == 132940_00
    assert tc["account_total_cents"] == 162940_00
    assert tc["cash_matches"] is True
    assert tc["cash_gap_cents"] == 0


def test_cash_mismatch_is_flagged_with_gap():
    engine = _setup()
    item = store_item(item_id="i", access_token="t", token_secret="s", actor="qa", display_name="B")
    _add_account(engine, item, "chk", "checking", "spendable", 30000_00)
    tc = build_trust_check(cash_on_hand_cents=51603_44, payable_issues=[], as_of=AS_OF)
    assert tc["cash_matches"] is False
    assert tc["cash_gap_cents"] == 51603_44 - 30000_00


def test_receivables_total_and_count_from_books():
    engine = _setup()
    _add_receivable(engine, "r1", "Acme Co", 5000_00, date(2026, 6, 1))
    _add_receivable(engine, "r2", "Acme Co", 7000_00, date(2026, 6, 10))
    _add_receivable(engine, "r3", "Beta LLC", 8500_00, date(2026, 7, 12))
    tc = build_trust_check(payable_issues=[], as_of=AS_OF)
    assert tc["ar_total_cents"] == 20500_00
    assert tc["ar_count"] == 3


def test_obligation_reasons_aggregated_and_sorted():
    _setup()
    issues = [
        {"id": "1", "reason": "missing settlement evidence"},
        {"id": "2", "reason": "missing settlement evidence"},
        {"id": "3", "reason": "ambiguous match"},
        {"id": "4", "reason": "source conflict"},
        {"id": "5", "reason": "missing settlement evidence"},
    ]
    tc = build_trust_check(payable_issues=issues, as_of=AS_OF)
    assert tc["obligation_issue_count"] == 5
    reasons = list(tc["obligation_reason_counts"].items())
    assert reasons[0] == ("missing settlement evidence", 3)  # most common first
    assert tc["obligation_reason_counts"]["ambiguous match"] == 1
    assert tc["obligation_reason_counts"]["source conflict"] == 1


def test_no_cash_input_leaves_match_unknown():
    _setup()
    tc = build_trust_check(payable_issues=[], as_of=AS_OF)
    assert tc["cash_matches"] is None
    assert tc["cash_gap_cents"] is None
    assert tc["obligation_issue_count"] == 0
