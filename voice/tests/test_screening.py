"""Transfer decisions. The model can ask. These rules are what actually run."""

from voice.screening import ScreenInput, apply_forward_availability, decide, looks_like_solicitation


def _screen(**overrides: object) -> ScreenInput:
    base = dict(
        category="other",
        reason="",
        notes="",
        wants_live_person=False,
        is_building_emergency=False,
        is_current_tenant=False,
        is_known_customer=False,
        qualified_opportunity=False,
        caller_number="+14155550100",
        caller_name="",
        company="",
        event_date="",
        guest_count="",
        event_or_interest_type="",
        known_numbers=frozenset(),
        forward_configured=True,
    )
    base.update(overrides)
    return ScreenInput(**base)  # type: ignore[arg-type]


def test_spam_and_vendor_pitches_are_never_forwarded() -> None:
    spam = decide(_screen(category="spam", wants_live_person=True, is_building_emergency=True))
    vendor = decide(
        _screen(
            category="vendor",
            wants_live_person=True,
            qualified_opportunity=True,
            caller_name="Pat",
            company="RankFirst",
        )
    )
    seo = decide(
        _screen(
            category="lead",
            reason="We do SEO and can rank you on Google",
            wants_live_person=True,
            qualified_opportunity=True,
            caller_name="Pat",
            company="RankFirst",
            is_building_emergency=True,
        )
    )
    assert spam.allow_transfer is False
    assert vendor.allow_transfer is False
    assert seo.allow_transfer is False
    assert seo.category == "spam"
    assert looks_like_solicitation("press 1 for a business loan")


def test_building_emergency_is_forwarded() -> None:
    decision = decide(
        _screen(
            category="urgent",
            reason="There is a water leak on the second floor",
            is_building_emergency=True,
        )
    )
    assert decision.allow_transfer is True
    assert decision.category == "urgent"


def test_urgent_label_without_a_real_emergency_is_not_enough() -> None:
    decision = decide(_screen(category="urgent", reason="I want a better rate", wants_live_person=True))
    assert decision.allow_transfer is False


def test_active_tenant_urgent_issue_is_forwarded() -> None:
    decision = decide(
        _screen(
            category="tenant",
            reason="We are locked out of the office",
            is_current_tenant=True,
            wants_live_person=True,
        )
    )
    assert decision.allow_transfer is True
    assert decision.category == "tenant"


def test_tenant_with_a_routine_question_takes_a_message() -> None:
    decision = decide(
        _screen(
            category="tenant",
            reason="Asking where the mail room is",
            is_current_tenant=True,
            wants_live_person=True,
        )
    )
    assert decision.allow_transfer is False
    assert decision.action == "message"


def test_known_customer_who_wants_a_person_is_forwarded() -> None:
    decision = decide(
        _screen(
            category="other",
            reason="Existing customer about a delivery",
            is_known_customer=True,
            wants_live_person=True,
        )
    )
    allowlist = decide(
        _screen(
            category="other",
            wants_live_person=True,
            caller_number="+14155550123",
            known_numbers=frozenset({"+14155550123"}),
        )
    )
    assert decision.allow_transfer is True
    assert allowlist.allow_transfer is True


def test_qualified_event_prospect_who_wants_to_talk_is_forwarded() -> None:
    decision = decide(
        _screen(
            category="lead",
            reason="Wedding in June, wants to talk through the date now",
            wants_live_person=True,
            qualified_opportunity=True,
            caller_name="Jamie Lee",
            event_date="2026-06-14",
            guest_count="80",
            event_or_interest_type="wedding",
        )
    )
    assert decision.allow_transfer is True
    assert decision.category == "lead"


def test_lead_without_a_name_or_who_does_not_want_a_person_is_not_forwarded() -> None:
    no_name = decide(
        _screen(
            category="lead",
            wants_live_person=True,
            qualified_opportunity=True,
            event_date="2026-06-14",
            event_or_interest_type="wedding",
        )
    )
    later = decide(
        _screen(
            category="lead",
            wants_live_person=False,
            qualified_opportunity=True,
            caller_name="Jamie Lee",
            event_date="2026-06-14",
        )
    )
    assert no_name.allow_transfer is False
    assert later.allow_transfer is False


def test_event_client_mentioning_a_dj_is_not_treated_as_a_solicitor() -> None:
    decision = decide(
        _screen(
            category="lead",
            reason="Birthday party, they are bringing their own DJ",
            wants_live_person=True,
            qualified_opportunity=True,
            caller_name="Riley Chen",
            event_or_interest_type="birthday",
            guest_count="40",
        )
    )
    assert decision.allow_transfer is True


def test_missing_forward_number_turns_an_approval_into_a_message() -> None:
    decision = decide(_screen(category="urgent", reason="Fire alarm", is_building_emergency=True))
    blocked = apply_forward_availability(decision, forward_configured=False)
    assert decision.allow_transfer is True
    assert blocked.allow_transfer is False
    assert blocked.action == "message"
