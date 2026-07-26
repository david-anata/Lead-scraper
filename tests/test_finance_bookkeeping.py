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


def _txn(engine, *, cid, name, amount=100_00, category="uncategorized", description=None,
         source="plaid", subcategory="", event_type="outflow"):
    now = datetime.now(timezone.utc)
    with engine.begin() as connection:
        insert_cash_event(
            connection, id=cid, source=source, source_id=cid,
            record_kind="transaction", event_type=event_type, category=category,
            subcategory=subcategory,
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


# --- transfers and the no-guess default -----------------------------------

def test_internal_transfers_never_reach_the_filing_queue():
    engine = _setup()
    _txn(engine, cid="t1", name="Home banking Withdrawal Transfer to S0050", amount=25000_00)
    _txn(engine, cid="t2", name="To Share 58", amount=144995_17)
    _txn(engine, cid="real", name="MYSTERY VENDOR", amount=500_00)

    ids = [item["id"] for item in list_needs_decision()]
    assert ids == ["real"], "moving your own money is not an expense to categorise"


def test_transfers_are_not_counted_as_needing_a_decision():
    engine = _setup()
    _txn(engine, cid="t1", name="Home banking Withdrawal Transfer to S0050", amount=25000_00)
    assert file_transactions()["needs_decision"] == 0


def test_an_unknown_merchant_gets_no_guess_so_nothing_is_preselected():
    engine = _setup()
    _txn(engine, cid="t1", name="Madison Bicycle Shop", amount=500_00)
    item = list_needs_decision()[0]
    assert item["guess"] == "", "a blank guess means the form must not preselect a category"


def test_filing_without_choosing_a_category_is_refused():
    engine = _setup()
    _txn(engine, cid="t1", name="KFC", amount=500_00)
    for bad in ("", "   ", "uncategorized", "other"):
        with pytest.raises(ValueError, match="choose a category"):
            file_transaction("t1", bad)


# --- merchant grouping: 935 decisions become a few dozen -------------------

def test_bank_boilerplate_is_stripped_so_real_merchants_group_apart():
    """Real descriptors bury the payee in wrapper text. Grouping on the raw
    leading words would file Stripe and Fora together as "ach withdrawal"."""
    from sales_support_agent.services.cashflow.bookkeeping import merchant_key

    stripe = merchant_key("Ach Withdrawal Company: Anata Entry: Stripe Cap David Narayan")
    fora = merchant_key("Ach Withdrawal Company: Forafinancial S6 Entry: Merchdebit Anata Inc.")
    ups = merchant_key("Ach Deposit Company: Ups General Serv Entry: Edi Paymts Anata Llc")

    assert stripe != fora != ups
    assert "withdrawal" not in stripe and "company" not in stripe
    assert "stripe" in stripe and "fora" in fora and "ups" in ups


def test_the_same_merchant_groups_despite_per_charge_noise():
    from sales_support_agent.services.cashflow.bookkeeping import merchant_key

    assert merchant_key("Amazon") == merchant_key("Amazon *2K4L9")
    assert merchant_key("Costco #1234") == merchant_key("Costco")
    assert merchant_key("Helium10"), "a name that genuinely contains digits survives"

    # Honest limitation: a card descriptor and a clean name are different keys,
    # so they appear as two groups. Both are still one decision each.
    assert merchant_key("GoDaddy") != merchant_key("DNH*GODADDY 480-505-8855 AZ")


def test_the_queue_collapses_to_merchants_biggest_first():
    from sales_support_agent.services.cashflow.bookkeeping import group_needs_decision

    engine = _setup()
    for index in range(12):
        _txn(engine, cid=f"a{index}", name="Madison Bicycle Shop", amount=100_00 + index)
    for index in range(3):
        _txn(engine, cid=f"k{index}", name="KFC", amount=50_00)

    grouped = group_needs_decision()
    assert grouped["transaction_count"] == 15
    assert grouped["group_count"] == 2
    assert grouped["groups"][0]["count"] == 12, "the biggest pile comes first"
    assert grouped["groups"][0]["samples"], "samples let the operator see what they are filing"


def test_filing_a_merchant_clears_every_one_and_teaches_a_rule():
    from sales_support_agent.services.cashflow.bookkeeping import (
        file_merchant,
        group_needs_decision,
    )

    engine = _setup()
    for index in range(12):
        _txn(engine, cid=f"a{index}", name="Madison Bicycle Shop", amount=100_00)
    _txn(engine, cid="other", name="KFC", amount=50_00)

    key = group_needs_decision()["groups"][0]["key"]
    result = file_merchant(key, "supplies", actor="qa@example.com")

    assert result["filed"] == 12
    assert _category(engine, "a0") == "supplies"
    assert _category(engine, "other") == "uncategorized", "other merchants are untouched"
    assert len(list_rules()) == 1, "one decision teaches one rule"

    # A later charge from that merchant files itself.
    _txn(engine, cid="later", name="Madison Bicycle Shop", amount=75_00)
    assert file_transactions()["filed_by_rule"] == 1
    assert _category(engine, "later") == "supplies"


def test_filing_a_merchant_without_a_category_is_refused():
    from sales_support_agent.services.cashflow.bookkeeping import file_merchant

    engine = _setup()
    _txn(engine, cid="a", name="KFC", amount=50_00)
    for bad in ("", "uncategorized", "other"):
        with pytest.raises(ValueError, match="choose a category"):
            file_merchant("kfc", bad)


# --- QuickBooks runs the books; this page only chases what it has not booked --

def test_a_transaction_quickbooks_booked_never_reaches_the_queue():
    engine = _setup()
    _txn(engine, cid="booked", name="Madison Bicycle Shop", source="qbo_bank",
         category="job materials", subcategory="Job Expenses:Job Materials")
    _txn(engine, cid="unbooked", name="Madison Bicycle Shop")

    assert [item["id"] for item in list_needs_decision()] == ["unbooked"]


def test_the_summary_separates_booked_in_quickbooks_from_filed_here_only():
    """Filing here does not touch QuickBooks, so the two are not the same
    thing and the page must not imply the books are up to date."""
    engine = _setup()
    _txn(engine, cid="booked", name="Vendor A", source="qbo_bank",
         category="job materials", subcategory="Job Expenses:Job Materials")
    _txn(engine, cid="here", name="COMCAST CABLE COMM")
    _txn(engine, cid="open", name="MYSTERY VENDOR")
    file_transactions()

    summary = bookkeeping_summary()
    assert summary["total_transactions"] == 3
    assert summary["booked_in_quickbooks"] == 1
    assert summary["filed_here_only"] == 1, "Comcast is filed here but missing from the books"
    assert summary["needs_decision"] == 1


def test_a_quickbooks_row_with_no_account_is_not_counted_as_booked():
    engine = _setup()
    _txn(engine, cid="a", name="Vendor A", source="qbo_bank", category="software")
    assert bookkeeping_summary()["booked_in_quickbooks"] == 0


def test_quickbooks_calling_it_uncategorised_is_not_counted_as_booked():
    """It claimed 61 of 71 booked while 15 still needed a decision. Three
    states that overlap are three numbers the operator cannot act on."""
    engine = _setup()
    for index in range(4):
        _txn(engine, cid=f"real{index}", name="Vendor A", source="qbo_bank",
             category="job materials", subcategory="Job Expenses:Job Materials")
    for index in range(6):
        _txn(engine, cid=f"dunno{index}", name="Weird Vendor", source="qbo_bank",
             category="other", subcategory="Uncategorized Expense")

    summary = bookkeeping_summary()
    assert summary["booked_in_quickbooks"] == 4
    assert summary["needs_decision"] == 6
    assert (
        summary["booked_in_quickbooks"]
        + summary["filed_here_only"]
        + summary["needs_decision"]
    ) == summary["total_transactions"], "the three states must account for every transaction"


def test_the_badge_count_matches_the_rows_the_queue_actually_offers():
    """The nav badge comes from this count and the queue comes from another
    query. Any row one includes and the other drops is a badge that never
    reaches zero however much the operator files."""
    engine = _setup()
    _txn(engine, cid="real", name="MYSTERY VENDOR", amount=500_00)
    _txn(engine, cid="zero", name="ZERO AMOUNT ROW", amount=0)
    _txn(engine, cid="moved", name="To Share 58", amount=25000_00)

    assert bookkeeping_summary()["needs_decision"] == len(list_needs_decision())


def test_transfers_are_outside_every_bookkeeping_number():
    """A transfer is not spend, so counting it as "filed here only" would claim
    the books are missing something that was never an expense."""
    engine = _setup()
    _txn(engine, cid="real", name="Vendor A", source="qbo_bank",
         category="software", subcategory="Dues and Subscriptions")
    for index in range(3):
        _txn(engine, cid=f"t{index}", name="Home banking Withdrawal Transfer to S0050",
             amount=25000_00)

    summary = bookkeeping_summary()
    assert summary["total_transactions"] == 1
    assert summary["booked_in_quickbooks"] == 1
    assert summary["filed_here_only"] == 0
    assert summary["needs_decision"] == 0


def test_internal_transfers_are_not_offered_as_a_merchant_group():
    from sales_support_agent.services.cashflow.bookkeeping import group_needs_decision

    engine = _setup()
    for index in range(5):
        _txn(engine, cid=f"t{index}", name="Home banking Withdrawal Transfer to S0050", amount=25000_00)
    assert group_needs_decision()["groups"] == []


# --- the queue is money going out only -------------------------------------

def test_money_coming_in_never_reaches_the_filing_queue():
    """184 Intuit deposits worth $495,932 sat here asking which expense
    category they belonged to. A deposit is not an expense, so there is no
    answer and the queue must not ask."""
    from sales_support_agent.services.cashflow.bookkeeping import group_needs_decision

    engine = _setup()
    for index in range(4):
        _txn(engine, cid=f"in{index}", name="Ach Deposit Company: Intuit Payment",
             amount=2695_00, event_type="inflow")
    _txn(engine, cid="spend", name="MYSTERY VENDOR", amount=500_00)

    assert [item["id"] for item in list_needs_decision()] == ["spend"]

    grouped = group_needs_decision()
    assert grouped["transaction_count"] == 1
    assert [group["count"] for group in grouped["groups"]] == [1]
    assert all("intuit" not in group["key"] for group in grouped["groups"])

    summary = bookkeeping_summary()
    assert summary["total_transactions"] == 1, "deposits are outside the denominator too"
    assert summary["needs_decision"] == 1
    assert file_transactions()["reviewed"] == 1


def test_the_four_summary_numbers_add_up_with_deposits_and_transfers_present():
    engine = _setup()
    _txn(engine, cid="booked", name="Vendor A", source="qbo_bank",
         category="software", subcategory="Dues and Subscriptions")
    _txn(engine, cid="here", name="COMCAST CABLE COMM")
    _txn(engine, cid="open", name="MYSTERY VENDOR")
    for index in range(3):
        _txn(engine, cid=f"in{index}", name="Ach Deposit Company: Intuit Payment",
             amount=2695_00, event_type="inflow")
    _txn(engine, cid="moved", name="Home banking Withdrawal Transfer to S0050", amount=25000_00)
    _txn(engine, cid="refund", name="Amazon Refund", amount=40_00, event_type="inflow",
         category="supplies")
    file_transactions()

    summary = bookkeeping_summary()
    assert summary["total_transactions"] == 3
    assert summary["booked_in_quickbooks"] == 1
    assert summary["filed_here_only"] == 1
    assert summary["needs_decision"] == 1
    assert (
        summary["booked_in_quickbooks"]
        + summary["filed_here_only"]
        + summary["needs_decision"]
    ) == summary["total_transactions"], "the three states must account for every transaction"


def test_the_badge_count_still_matches_the_queue_when_deposits_exist():
    engine = _setup()
    _txn(engine, cid="spend", name="MYSTERY VENDOR", amount=500_00)
    for index in range(6):
        _txn(engine, cid=f"in{index}", name="Ach Deposit Company: Intuit Payment",
             amount=2695_00, event_type="inflow")
    _txn(engine, cid="moved", name="To Share 58", amount=25000_00)

    assert bookkeeping_summary()["needs_decision"] == len(list_needs_decision())


def test_filing_money_coming_in_is_refused():
    """A page left open before deposits were removed can still post one."""
    from sales_support_agent.services.cashflow.bookkeeping import file_merchant

    engine = _setup()
    _txn(engine, cid="in", name="Ach Deposit Company: Intuit Payment", amount=2695_00,
         event_type="inflow")

    with pytest.raises(ValueError, match="money coming in"):
        file_transaction("in", "software")
    with pytest.raises(ValueError, match="money coming in"):
        file_merchant("intuit payment", "software")

    assert _category(engine, "in") == "uncategorized", "the deposit is left alone"
    assert list_rules() == [], "nothing was taught that would file future deposits as spend"


def test_a_merchant_with_both_directions_files_only_the_money_going_out():
    from sales_support_agent.services.cashflow.bookkeeping import (
        file_merchant,
        group_needs_decision,
    )

    engine = _setup()
    _txn(engine, cid="paid", name="Madison Bicycle Shop", amount=500_00)
    _txn(engine, cid="refunded", name="Madison Bicycle Shop", amount=120_00,
         event_type="inflow")

    key = group_needs_decision()["groups"][0]["key"]
    result = file_merchant(key, "supplies")

    assert result["filed"] == 1
    assert _category(engine, "paid") == "supplies"
    assert _category(engine, "refunded") == "uncategorized", "the money back is untouched"
