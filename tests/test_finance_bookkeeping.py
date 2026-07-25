from datetime import date, datetime, timezone

import pytest
from sqlalchemy import text

from sales_support_agent.models.database import (
    create_session_factory,
    init_database,
    insert_cash_event,
)
from sales_support_agent.services.cashflow.bookkeeping import (
    bookkeeping_summary,
    delete_rule,
    file_transaction,
    file_transactions,
    list_needs_decision,
    list_rules,
    suggest_rule_pattern,
)


def _setup():
    factory = create_session_factory("sqlite:///:memory:")
    init_database(factory)
    return factory.kw["bind"]


def _txn(engine, *, cid, name, amount=100_00, category="uncategorized", description=None):
    now = datetime.now(timezone.utc)
    with engine.begin() as connection:
        insert_cash_event(
            connection, id=cid, source="plaid", source_id=cid,
            record_kind="transaction", event_type="outflow", category=category,
            name=name, vendor_or_customer=name, description=description or name,
            amount_cents=amount, due_date=date(2026, 7, 10), status="posted",
            confidence="confirmed", created_at=now, updated_at=now,
        )


def _category(engine, cid):
    with engine.connect() as connection:
        return connection.execute(
            text("SELECT category FROM cash_events WHERE id=:id"), {"id": cid}
        ).scalar_one()


def test_pattern_drops_transaction_noise():
    assert suggest_rule_pattern("AMZN MKTP US*2K4L9 12345") == "amzn mktp us"
    assert suggest_rule_pattern("  Google   Workspace  ") == "google workspace"


def test_keyword_categorizer_files_known_merchants():
    engine = _setup()
    # COMCAST is one of the built-in descriptors the categorizer recognises.
    _txn(engine, cid="t1", name="COMCAST CABLE COMM")
    result = file_transactions()
    assert result["filed_by_keyword"] >= 1
    assert _category(engine, "t1") == "utilities"


def test_merchant_the_categorizer_does_not_know_needs_a_decision():
    """The built-in rules are tuned to known bank descriptors, so common SaaS
    vendors fall through to the queue until a rule is taught."""
    engine = _setup()
    _txn(engine, cid="t1", name="GOOGLE WORKSPACE")
    result = file_transactions()
    assert result["needs_decision"] == 1
    assert [item["id"] for item in list_needs_decision()] == ["t1"]


def test_unknown_merchant_goes_to_the_decision_queue():
    engine = _setup()
    _txn(engine, cid="t1", name="VON HILL CONSULTING LLC")
    file_transactions()
    pending = list_needs_decision()
    assert [item["id"] for item in pending] == ["t1"]
    assert pending[0]["suggested_pattern"] == "von hill consulting"


def test_filing_with_always_teaches_a_rule_that_files_the_next_one():
    engine = _setup()
    _txn(engine, cid="t1", name="VON HILL CONSULTING LLC")

    result = file_transaction("t1", "contractor", always=True, actor="qa@example.com")
    assert result["rule_id"]
    assert _category(engine, "t1") == "contractor"
    assert len(list_rules()) == 1

    # A later transaction from the same merchant files itself.
    _txn(engine, cid="t2", name="VON HILL CONSULTING LLC 99")
    counts = file_transactions()
    assert counts["filed_by_rule"] == 1
    assert _category(engine, "t2") == "contractor"
    assert list_rules()[0]["hit_count"] == 1


def test_filing_without_always_teaches_nothing():
    engine = _setup()
    _txn(engine, cid="t1", name="ONE OFF VENDOR")
    file_transaction("t1", "supplies", always=False)
    assert _category(engine, "t1") == "supplies"
    assert list_rules() == []


def test_rule_can_be_removed():
    engine = _setup()
    _txn(engine, cid="t1", name="VON HILL CONSULTING")
    file_transaction("t1", "contractor", always=True)
    rule_id = list_rules()[0]["id"]
    delete_rule(str(rule_id))
    assert list_rules() == []


def test_summary_counts_and_writeback_is_honestly_off():
    engine = _setup()
    _txn(engine, cid="t1", name="COMCAST CABLE COMM")
    _txn(engine, cid="t2", name="MYSTERY VENDOR")
    file_transactions()
    summary = bookkeeping_summary()
    assert summary["total_transactions"] == 2
    assert summary["needs_decision"] == 1
    assert summary["filed"] == 1
    assert summary["qbo_writeback"] == "not_connected"


def test_filing_requires_a_category_and_a_real_transaction():
    engine = _setup()
    _txn(engine, cid="t1", name="X")
    with pytest.raises(ValueError):
        file_transaction("t1", "   ")
    with pytest.raises(ValueError):
        file_transaction("nope", "supplies")
