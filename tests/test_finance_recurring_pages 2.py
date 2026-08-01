"""The Schedules page replaces a ClickUp list, so it has to answer the two
questions ClickUp answered: what is running, and what does it cost a month.

The pause and pieces controls matter just as much: without them nothing in the
app can stop a schedule from building the next 14 and 30 days.
"""

from __future__ import annotations

from unittest.mock import patch

from sales_support_agent.services.cashflow.recurring import (
    _monthly_cents,
    parse_template_form,
    render_recurring_new_page,
    render_recurring_page,
)


def _template(name, **overrides):
    row = {
        "id": name.lower().replace(" ", "-"),
        "name": name,
        "vendor_or_customer": name,
        "event_type": "outflow",
        "category": "software",
        "amount_cents": 100_000,
        "frequency": "monthly",
        "next_due_date": "2026-08-01",
        "is_active": True,
        "flexibility": "unknown",
    }
    row.update(overrides)
    return row


def _render(templates):
    with patch(
        "sales_support_agent.services.cashflow.recurring.list_recurring_templates",
        return_value=templates,
    ):
        return render_recurring_page()


def _realistic():
    return [
        _template("Office rent", amount_cents=450_000, frequency="monthly", category="rent"),
        _template("Payroll", amount_cents=600_000, frequency="biweekly", category="payroll"),
        _template("Cleaning", amount_cents=25_000, frequency="weekly", category="supplies"),
        _template("Insurance", amount_cents=180_000, frequency="quarterly", category="insurance"),
        _template("Filing fee", amount_cents=60_000, frequency="annual", category="tax"),
        _template("Old software", amount_cents=9_900, is_active=False, frequency="monthly"),
    ]


# ---------------------------------------------------------------------------
# Monthly normalisation
# ---------------------------------------------------------------------------

def test_every_frequency_is_reduced_to_one_month_of_cost():
    assert _monthly_cents(100_000, "monthly") == 100_000
    assert _monthly_cents(100_000, "weekly") == round(100_000 * 52 / 12)
    assert _monthly_cents(100_000, "biweekly") == round(100_000 * 26 / 12)
    assert _monthly_cents(300_000, "quarterly") == 100_000
    assert _monthly_cents(1_200_000, "annual") == 100_000


def test_an_unknown_frequency_still_counts_once():
    """Guessing zero would quietly shrink the monthly total he trusts."""
    assert _monthly_cents(75_000, "whenever") == 75_000
    assert _monthly_cents(None, None) == 0


# ---------------------------------------------------------------------------
# The list page
# ---------------------------------------------------------------------------

def test_the_page_is_named_for_what_it_holds_not_for_the_table():
    page = _render(_realistic())

    assert "Schedules" in page
    assert "Recurring rules" not in page
    assert "recurring obligations" not in page
    assert "+ Add a bill" in page
    assert "Create upcoming events" not in page


def test_the_summary_says_how_many_run_and_what_they_cost_a_month():
    page = _render(_realistic())

    expected = sum(
        _monthly_cents(t["amount_cents"], t["frequency"])
        for t in _realistic()
        if t["is_active"]
    )
    # 4,500 rent + 13,000 payroll + 1,083 cleaning + 600 insurance + 50 filing
    assert expected == 1_923_333, "five running bills normalised to one month"

    assert "5 running" in page
    assert "$19,233 a month going out" in page
    assert "1 paused" in page


def test_money_coming_in_is_reported_apart_from_the_cost():
    page = _render([
        _template("Office rent", amount_cents=450_000),
        _template("Retainer", amount_cents=800_000, event_type="inflow", category="revenue"),
    ])

    assert "$4,500 a month going out" in page
    assert "$8,000 a month coming in" in page


def test_a_paused_schedule_reads_as_paused_and_is_kept_out_of_the_total():
    page = _render([
        _template("Office rent", amount_cents=450_000),
        _template("Old software", amount_cents=9_900, is_active=False),
    ])

    assert "Paused" in page
    assert "1 running" in page
    assert "$4,500 a month going out" in page, "the paused one must not be counted"
    assert "Paused, so it is left out of your next 14 and 30 days." in page


def test_when_everything_is_paused_the_summary_says_nothing_is_running():
    page = _render([_template("Old software", is_active=False)])

    assert "Nothing running" in page
    assert "1 paused, so it is left out." in page


def test_a_schedule_paid_in_pieces_says_so_on_its_row():
    page = _render([_template("Card payment", flexibility="chunkable")])

    assert "You pay this one in pieces across the month." in page


def test_a_row_with_no_flexibility_answer_stays_quiet_about_it():
    page = _render([_template("Office rent")])

    assert "in pieces" not in page
    assert "1 running" in page


def test_a_missing_flexibility_column_does_not_break_the_page():
    """Rows written before the column existed come back without the key."""
    row = _template("Office rent", amount_cents=450_000)
    row.pop("flexibility")

    page = _render([row])

    assert "1 running" in page
    assert "$4,500 a month going out" in page


def test_the_empty_page_offers_the_bank_history_route():
    page = _render([])

    assert "Nothing repeating here yet." in page
    assert "/admin/finances/whats-coming" in page
    assert "bank" in page
    assert "0 running" not in page, "an empty page should read as empty, not as a zero"


# ---------------------------------------------------------------------------
# The form
# ---------------------------------------------------------------------------

def test_the_form_can_pause_a_schedule_and_mark_it_paid_in_pieces():
    page = render_recurring_new_page()

    assert 'name="is_active"' in page
    assert "Paused for now" in page
    assert 'name="flexibility"' in page
    assert 'value="chunkable"' in page
    assert "In pieces across the month" in page


def test_the_form_does_not_show_database_words():
    page = render_recurring_new_page()

    assert "is_active" not in page.replace('name="is_active"', "")
    assert "outflow" not in page.replace('value="outflow"', "")
    assert "Money going out" in page


# ---------------------------------------------------------------------------
# Reading the form back
# ---------------------------------------------------------------------------

def test_pausing_and_pieces_survive_the_round_trip():
    parsed = parse_template_form({
        "name": "Card payment",
        "amount_dollars": "1,200.50",
        "next_due_date": "2026-08-01",
        "frequency": "monthly",
        "is_active": "no",
        "flexibility": "chunkable",
    })

    assert parsed["is_active"] is False
    assert parsed["flexibility"] == "chunkable"
    assert parsed["amount_cents"] == 120_050


def test_a_form_that_says_nothing_leaves_the_schedule_running_and_unknown():
    parsed = parse_template_form({"name": "Office rent", "amount_dollars": "4500"})

    assert parsed["is_active"] is True, "silence must never pause a bill"
    assert parsed["flexibility"] == "unknown"


def test_a_running_answer_and_a_junk_flexibility_are_read_safely():
    parsed = parse_template_form({"is_active": "yes", "flexibility": "sideways"})

    assert parsed["is_active"] is True
    assert parsed["flexibility"] == "unknown"


def test_a_flexibility_answer_set_elsewhere_is_not_thrown_away():
    assert parse_template_form({"flexibility": "deferrable"})["flexibility"] == "deferrable"
    assert parse_template_form({"flexibility": "fixed"})["flexibility"] == "fixed"
