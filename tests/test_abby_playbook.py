"""Return dates, approved copy, and the has-solution second note."""

from datetime import date, datetime, timedelta, timezone

from sales_support_agent.abby_playbook import (
    HAS_SOLUTION_SECOND_DAYS,
    NOT_NOW_DAYS,
    SIGNATURE,
    ooo_due,
    ooo_personal_line,
    parse_return_date,
    render_has_solution_second,
    render_immediate_reply,
    render_not_now,
    render_ooo_followup,
    render_referral,
    render_wrong_person,
)

EM_DASH = "\u2014"
TODAY = date(2026, 10, 27)
NOW = datetime(2026, 10, 27, 16, 0, tzinfo=timezone.utc)


def test_return_date_is_the_later_cued_date_and_followup_is_the_next_day():
    text = "I am out of the office from October 28 through November 4."
    assert parse_return_date(text, TODAY) == date(2026, 11, 4)
    due = ooo_due(text, NOW)
    assert due == datetime(2026, 11, 5, 15, 0, tzinfo=timezone.utc)


def test_explicit_day_month_and_iso_dates():
    assert parse_return_date("I will be back on 4 November 2026.", TODAY) == date(2026, 11, 4)
    assert parse_return_date("Returning 2026-12-01.", TODAY) == date(2026, 12, 1)


def test_missing_return_date_waits_seven_days():
    assert parse_return_date("I am out of the office.", TODAY) is None
    assert ooo_due("I am out of the office.", NOW) == NOW + timedelta(days=7)


def test_personal_lines_use_approved_wording_and_do_not_invent():
    assert ooo_personal_line("Out of the office on vacation until November 4.") == "Hope you had a great vacation."
    assert ooo_personal_line("At a trade show all week.") == "Hope the show was worth the trip."
    assert ooo_personal_line("Out of the office.") == "Hope your time away went well."
    health = ooo_personal_line("Out of the office for a family emergency.")
    assert health == "Hope all is well."
    assert "emergency" not in health and "sick" not in health


def test_rendered_followups_have_no_em_dash_and_the_signature():
    copies = [
        render_ooo_followup(first_name="Ann", brand="Huppy", reply_text="Out of the office until November 4."),
        render_not_now(30, "Ann"),
        render_not_now(75, "Ann"),
        render_has_solution_second("Ann"),
        render_wrong_person(first_name="Ann", brand="Huppy", reply_text="I am on the design side."),
        render_wrong_person(first_name="Ann", brand="Huppy", reply_text="This is not my area."),
        render_referral(
            first_name="Ann", new_name="Sarah Chen", brand="Huppy",
            reply_text="Sarah handles marketing for us.",
        ),
    ]
    for copy in copies:
        assert EM_DASH not in copy
        assert copy.rstrip().endswith(SIGNATURE)
        assert "virtual assistant" not in copy.lower()
    assert NOT_NOW_DAYS == (30, 75)
    assert HAS_SOLUTION_SECOND_DAYS == 7


def test_wrong_person_uses_the_approved_design_line_and_drops_a_missing_role():
    design = render_wrong_person(first_name="Ann", brand="Huppy", reply_text="I am on the design side.")
    assert "makes sense since you're on the design side." in design
    assert "Who handles Amazon or marketing growth for Huppy?" in design
    plain = render_wrong_person(first_name="Ann", brand="Huppy", reply_text="Wrong person.")
    assert "makes sense" not in plain
    assert plain.startswith("Thanks Ann, who handles Amazon or marketing growth for Huppy?")


def test_referral_drops_storeleads_detail_we_do_not_have():
    copy = render_referral(first_name="James", new_name="Mike Alvarez", brand="Northwind", reply_text="Mike runs sales for us.")
    assert "Hi Mike, James mentioned you run sales at Northwind." in copy
    assert "recent launch" not in copy
    assert "free 14-day trial" in copy
    no_role = render_immediate_reply(
        "referral", first_name="Ann", company="Huppy", referral_name="Sarah Chen",
        referral_email="sarah@huppy.com", reply_text="Talk to Sarah.",
    )
    assert "Ann mentioned you at Huppy." in no_role
    assert "recent launch" not in no_role


def test_has_solution_second_note_does_not_invent_claims():
    copy = render_has_solution_second("Ann")
    assert "alongside what you already use" in copy
    assert "99" not in copy
    assert EM_DASH not in copy


def test_invalid_referral_email_falls_back_to_the_harder_ask():
    copy = render_immediate_reply(
        "referral", first_name="Ann", company="Huppy", referral_name="Sam",
        referral_email="not-an-email", reply_text="Ask Sam.",
    )
    assert "who handles Amazon or marketing growth for Huppy?" in copy
