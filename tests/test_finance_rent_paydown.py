"""What can go toward a big recurring bill, and when.

The property that matters is not the size of any instalment. It is that paying
one can never take a later day below the cash floor. "Safe to commit" only looks
14 days out, so it is comfortable today and short on the 22nd; these tests exist
to stop this repeating that.
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from sales_support_agent.services.cashflow.rent_paydown import (
    build_paydown_plan,
    largest_recurring_outflow,
)

AS_OF = date(2026, 7, 6)
MONTH_END = date(2026, 7, 31)
FLOOR = 1_000_000  # $10,000


def _event(when: date, cents: int, *, kind: str = "planned", vendor: str = "Someone") -> dict:
    return {"date": when.isoformat(), "amount_cents": cents, "kind": kind,
            "name": vendor, "vendor_or_customer": vendor}


def _calendar(events: list[dict]) -> dict:
    """A calendar shaped like the real one, with one bucket per day."""
    days = []
    day = AS_OF
    while day <= MONTH_END + timedelta(days=7):
        days.append({
            "date": day.isoformat(),
            "events": [item for item in events if item["date"] == day.isoformat()],
        })
        day += timedelta(days=1)
    return {"days": days}


def _posted(vendor: str, cents: int, when: date) -> dict:
    return {"event_type": "outflow", "status": "posted", "due_date": when.isoformat(),
            "amount_cents": cents, "vendor_or_customer": vendor, "name": vendor}


def _incoming(cents: int, when: date, *, confidence: str = "confirmed") -> dict:
    return {"event_type": "inflow", "status": "planned", "due_date": when.isoformat(),
            "amount_cents": cents, "open_amount_cents": cents, "confidence": confidence,
            "vendor_or_customer": "A customer", "name": "A customer"}


def _plan(*, events=(), rows=(), spendable=5_000_000, reserve=0, monthly=3_000_000):
    return build_paydown_plan(
        calendar=_calendar(list(events)),
        rows=list(rows),
        spendable_cents=spendable,
        reserve_cents=reserve,
        floor_cents=FLOOR,
        vendor_key="boulder ranch",
        vendor_label="Boulder Ranch",
        monthly_cents=monthly,
        as_of=AS_OF,
    )


def _balance_after_each_day(plan, *, events, rows, spendable):
    """Replay the month applying the plan, and report the lowest balance reached."""
    paid = {item["date"]: item["amount_cents"] for item in plan["instalments"]}
    out: dict[date, int] = {}
    for item in events:
        when = date.fromisoformat(item["date"])
        out[when] = out.get(when, 0) + item["amount_cents"]
    incoming: dict[date, int] = {}
    for row in rows:
        if row["event_type"] == "inflow" and row.get("confidence") == "confirmed":
            when = date.fromisoformat(row["due_date"])
            incoming[when] = incoming.get(when, 0) + row["amount_cents"]

    balance = spendable
    lowest = balance
    day = AS_OF
    while day <= MONTH_END:
        balance = balance - out.get(day, 0) + incoming.get(day, 0) - paid.get(day, 0)
        lowest = min(lowest, balance)
        day += timedelta(days=1)
    return lowest


# --- the rule the whole thing exists to hold ------------------------------

def test_no_instalment_takes_a_later_day_below_the_floor():
    """The failure this prevents: comfortable today, short on the 22nd."""
    events = [
        _event(AS_OF + timedelta(days=3), 400_000),
        _event(AS_OF + timedelta(days=16), 2_600_000),   # the late hit
        _event(AS_OF + timedelta(days=20), 300_000),
    ]
    plan = _plan(events=events, spendable=5_000_000)

    lowest = _balance_after_each_day(plan, events=events, rows=[], spendable=5_000_000)
    assert lowest >= FLOOR, (
        f"the plan drives the balance to {lowest / 100:,.2f}, below the "
        f"{FLOOR / 100:,.2f} floor, on a day after the payment"
    )


def test_a_bill_late_in_the_month_reduces_what_can_be_paid_today():
    """A 14 day view cannot see this, which is why it over-promises."""
    quiet = _plan(events=[], spendable=5_000_000)
    with_late_bill = _plan(
        events=[_event(AS_OF + timedelta(days=20), 2_500_000)], spendable=5_000_000
    )

    assert quiet["planned_total_cents"] > with_late_bill["planned_total_cents"]


def test_instalments_never_exceed_what_is_left_to_pay():
    plan = _plan(events=[], spendable=90_000_000, monthly=3_000_000)

    assert plan["planned_total_cents"] <= plan["remaining_cents"]
    assert sum(item["amount_cents"] for item in plan["instalments"]) == plan["planned_total_cents"]


def test_what_has_already_been_paid_this_month_is_deducted():
    rows = [
        _posted("Boulder Ranch", 1_200_000, AS_OF - timedelta(days=4)),
        _posted("Boulder Ranch", 900_000, AS_OF.replace(day=1) - timedelta(days=9)),  # last month
    ]
    plan = _plan(rows=rows, monthly=3_000_000)

    assert plan["paid_this_month_cents"] == 1_200_000, "last month's payment must not count"
    assert plan["remaining_cents"] == 1_800_000


def test_operator_confirmed_balance_is_authoritative():
    rows = [_posted("Boulder Ranch", 1_200_000, AS_OF - timedelta(days=4))]
    plan = build_paydown_plan(
        calendar=_calendar([]), rows=rows, spendable_cents=5_000_000,
        reserve_cents=0, floor_cents=FLOOR, vendor_key="boulder ranch",
        vendor_label="Boulder Ranch", monthly_cents=4_000_000,
        authoritative_balance_cents=3_000_000, balance_as_of=AS_OF,
        as_of=AS_OF,
    )

    assert plan["remaining_cents"] == 3_000_000
    assert plan["balance_basis"] == "operator_confirmed"


def test_posted_payment_after_confirmed_balance_reduces_what_is_owed():
    rows = [_posted("Boulder Ranch", 500_000, AS_OF + timedelta(days=1))]
    plan = build_paydown_plan(
        calendar=_calendar([]), rows=rows, spendable_cents=5_000_000,
        reserve_cents=0, floor_cents=FLOOR, vendor_key="boulder ranch",
        vendor_label="Boulder Ranch", monthly_cents=4_000_000,
        authoritative_balance_cents=3_000_000, balance_as_of=AS_OF,
        as_of=AS_OF + timedelta(days=1),
    )

    assert plan["remaining_cents"] == 2_500_000
    assert plan["paid_since_balance_cents"] == 500_000


def test_mirrored_bank_sources_do_not_double_count_sent_amount():
    rows = [
        {**_posted("Boulder Ranch", 1_007_516, AS_OF), "source": "plaid", "source_id": "p1"},
        {**_posted("Boulder Ranch", 1_007_516, AS_OF), "source": "qbo_bank", "source_id": "q1"},
    ]
    plan = build_paydown_plan(
        calendar=_calendar([]), rows=rows, spendable_cents=5_000_000,
        reserve_cents=0, floor_cents=FLOOR, vendor_key="boulder ranch",
        vendor_label="Boulder Ranch", monthly_cents=4_000_000,
        authoritative_balance_cents=3_000_000, balance_as_of=AS_OF,
        as_of=AS_OF,
    )

    assert plan["paid_this_month_cents"] == 1_007_516
    assert plan["remaining_cents"] == 3_000_000


# --- the double counting trap --------------------------------------------

def test_the_bill_being_paid_is_not_also_reserved_against():
    """Its own future entries must be excluded, or the rent is counted twice and
    every instalment comes out roughly half what it should be."""
    rent_dated = [_event(AS_OF + timedelta(days=10), 1_500_000, vendor="Boulder Ranch")]
    someone_else = [_event(AS_OF + timedelta(days=10), 1_500_000, vendor="Other Vendor")]

    paying_own = _plan(events=rent_dated, spendable=5_000_000)
    paying_other = _plan(events=someone_else, spendable=5_000_000)

    assert paying_own["reserved_cents"] == 0, "its own bill must not be reserved against"
    assert paying_own["planned_total_cents"] > paying_other["planned_total_cents"]


# --- what counts as money ------------------------------------------------

def test_unconfirmed_bills_are_reserved_for_as_well():
    """Under-reserving bounces a payment. Over-reserving just means paying more
    next week, so the cautious side is the correct side."""
    plan = _plan(
        events=[_event(AS_OF + timedelta(days=5), 800_000, kind="history_warning")],
        spendable=5_000_000,
    )

    assert plan["reserved_cents"] == 800_000
    assert plan["unconfirmed_reserved_cents"] == 800_000


def test_money_owed_but_not_confirmed_is_not_counted_as_arriving():
    """A plan built on a wish is how a rent payment bounces."""
    confirmed = _plan(rows=[_incoming(2_000_000, AS_OF + timedelta(days=5))],
                      spendable=1_500_000, monthly=9_000_000)
    hoped = _plan(rows=[_incoming(2_000_000, AS_OF + timedelta(days=5), confidence="expected")],
                  spendable=1_500_000, monthly=9_000_000)

    assert confirmed["planned_total_cents"] > hoped["planned_total_cents"]


def test_confirmed_money_arriving_lets_a_later_instalment_be_larger():
    plan = _plan(
        rows=[_incoming(3_000_000, AS_OF + timedelta(days=8))],
        spendable=1_500_000, monthly=9_000_000,
    )

    assert len(plan["instalments"]) >= 2, "money arriving should open a second payment"
    assert plan["instalments"][-1]["date"] > AS_OF


# --- when it cannot answer ------------------------------------------------

def test_no_spare_cash_proposes_nothing_and_says_so():
    plan = _plan(events=[_event(AS_OF + timedelta(days=2), 4_500_000)], spendable=5_000_000)

    assert plan["instalments"] == []
    assert plan["status"] == "nothing_spare"


def test_savings_are_reported_separately_and_never_blended_in():
    """David asked for savings to be possible but wants to stop needing it, so it
    must never quietly prop up the headline."""
    plan = _plan(events=[_event(AS_OF + timedelta(days=2), 4_500_000)],
                 spendable=5_000_000, reserve=2_000_000, monthly=3_000_000)

    assert plan["planned_total_cents"] == 0, "savings must not appear in the plan itself"
    assert plan["savings_would_unlock_cents"] > 0
    assert plan["savings_would_unlock_cents"] <= plan["savings_available_cents"]


def test_savings_line_is_zero_when_there_is_nothing_to_move():
    plan = _plan(events=[_event(AS_OF + timedelta(days=2), 4_500_000)],
                 spendable=5_000_000, reserve=1_900, monthly=3_000_000)

    assert plan["savings_would_unlock_cents"] == 1_900


def test_a_plan_is_not_produced_for_a_bill_already_settled():
    rows = [_posted("Boulder Ranch", 3_000_000, AS_OF - timedelta(days=2))]
    plan = _plan(rows=rows, monthly=3_000_000, spendable=9_000_000)

    assert plan["remaining_cents"] == 0
    assert plan["instalments"] == []


# --- choosing the vendor --------------------------------------------------

def test_the_biggest_thing_actually_leaving_the_bank_is_chosen():
    rows = [
        _posted("Boulder Ranch", 2_000_000, AS_OF - timedelta(days=10)),
        _posted("Boulder Ranch", 2_000_000, AS_OF - timedelta(days=40)),
        _posted("Boulder Ranch", 2_000_000, AS_OF - timedelta(days=70)),
        _posted("Small Supplier", 20_000, AS_OF - timedelta(days=10)),
    ]

    chosen = largest_recurring_outflow(rows, as_of=AS_OF)

    assert chosen["vendor_key"] == "boulder ranch"
    assert chosen["monthly_cents"] == pytest.approx(2_000_000, abs=1)


def test_nothing_to_plan_around_says_so_rather_than_guessing():
    plan = build_paydown_plan(
        calendar=_calendar([]), rows=[], spendable_cents=5_000_000,
        reserve_cents=0, floor_cents=FLOOR, as_of=AS_OF,
    )

    assert plan["status"] == "no_vendor"
    assert plan["instalments"] == []


# --- shape ----------------------------------------------------------------

def test_small_amounts_do_not_become_their_own_line_unless_they_finish_the_bill():
    """A plan of eleven $40 instalments is not a plan."""
    plan = _plan(events=[], spendable=90_000_000, monthly=3_000_000)

    for item in plan["instalments"][:-1]:
        assert item["amount_cents"] >= 50_000

    assert plan["instalments"], "and it still has to propose something"


def test_every_instalment_says_what_it_is_waiting_for():
    plan = _plan(rows=[_incoming(3_000_000, AS_OF + timedelta(days=8))],
                 spendable=1_500_000, monthly=9_000_000)

    for item in plan["instalments"]:
        assert item["why"], "a date with no reason cannot be argued with"


# --- what the database actually hands back --------------------------------

def test_timestamps_from_the_database_are_handled_like_dates():
    """This is the bug that reached production. A date column comes back as a
    full timestamp, datetime is a subclass of date, so a naive check returns it
    untouched and the next comparison against a plain date raises TypeError.
    Every local fixture used plain dates and ISO strings, so nothing caught it.
    """
    from datetime import datetime

    rows = [
        {"event_type": "outflow", "status": "posted",
         "due_date": datetime(2026, 6, 6, 14, 30), "amount_cents": 2_000_000,
         "vendor_or_customer": "Boulder Ranch", "name": "Boulder Ranch"},
        {"event_type": "outflow", "status": "posted",
         "due_date": datetime(2026, 5, 6, 9, 15), "amount_cents": 2_000_000,
         "vendor_or_customer": "Boulder Ranch", "name": "Boulder Ranch"},
        {"event_type": "outflow", "status": "posted",
         "due_date": datetime(2026, 4, 6, 0, 0), "amount_cents": 2_000_000,
         "vendor_or_customer": "Boulder Ranch", "name": "Boulder Ranch"},
    ]

    chosen = largest_recurring_outflow(rows, as_of=AS_OF)

    assert chosen, "timestamps must not make the vendor search fall over"
    assert chosen["vendor_key"] == "boulder ranch"


def test_a_whole_plan_can_be_built_from_timestamped_rows():
    from datetime import datetime

    rows = [
        {"event_type": "outflow", "status": "posted",
         "due_date": datetime(2026, 7, 2, 11, 0), "amount_cents": 1_000_000,
         "vendor_or_customer": "Boulder Ranch", "name": "Boulder Ranch"},
        {"event_type": "inflow", "status": "planned", "confidence": "confirmed",
         "due_date": datetime(2026, 7, 20, 8, 0), "amount_cents": 500_000,
         "open_amount_cents": 500_000, "vendor_or_customer": "A customer",
         "name": "A customer"},
    ]

    plan = build_paydown_plan(
        calendar={"days": [{"date": datetime(2026, 7, 12, 6, 0), "events": [
            {"kind": "planned", "amount_cents": 200_000,
             "vendor_or_customer": "Someone", "name": "Someone"}]}]},
        rows=rows, spendable_cents=5_000_000, reserve_cents=0, floor_cents=FLOOR,
        vendor_key="boulder ranch", vendor_label="Boulder Ranch",
        monthly_cents=3_000_000, as_of=AS_OF,
    )

    assert plan["status"] in {"ok", "nothing_spare"}
    assert plan["paid_this_month_cents"] == 1_000_000
