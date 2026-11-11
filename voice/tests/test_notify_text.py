"""Texts to David are one plain sentence and one SMS segment."""

from voice.lines import LineContext
from voice.notify import SMS_SEGMENT, call_summary_text, format_phone, sms_forward_text
from voice.session import CallSession


def _call(**overrides: object) -> CallSession:
    session = CallSession(
        call_sid="CA1",
        caller_number="+18013193675",
        dialed_number="+13853363562",
        line=LineContext(id="track_inc", label="Anata Inc · anatainc.com", source="anatainc.com"),
        callback_needed=True,
        action="needs_callback",
    )
    for key, value in overrides.items():
        setattr(session, key, value)
    return session


def test_a_known_missed_call_is_one_plain_line() -> None:
    session = _call(
        known_caller_name="David's wife",
        known_caller_category="vip",
        known_caller_id="abc-123",
        caller_name="David's wife",
        reason="Known vip caller",
        notes=(
            "VIP caller, David's wife. Needs David at 6 AM tomorrow. "
            "Callback number on file: +18013193675. VIP caller. "
            "Message: she needs David at 6 AM tomorrow."
        ),
    )
    text = call_summary_text(session)
    assert text == (
        "Missed call: David's wife (801-319-3675). "
        "Needs David at 6 AM tomorrow. Call back."
    )
    assert "needs_callback" not in text
    assert "anatainc.com" not in text
    assert "vip" not in text.casefold()
    assert text.count("David's wife") == 1
    assert text.count("801-319-3675") == 1
    assert len(text) <= SMS_SEGMENT


def test_an_unknown_caller_is_named_by_number_once() -> None:
    session = _call(summary="Asked about a Saturday party")
    text = call_summary_text(session)
    assert text == "Missed call: 801-319-3675. Asked about a Saturday party. Call back."
    assert text.count("801-319-3675") == 1


def test_a_handled_call_does_not_ask_for_a_callback() -> None:
    session = _call(
        caller_name="Alex",
        summary="A Saturday party",
        callback_needed=False,
        action="transferred",
        transfer_accepted=True,
        transfer_outcome="answered",
    )
    text = call_summary_text(session)
    assert text == "Alex (801-319-3675) called about a Saturday party. Handled."
    assert "Call back" not in text


def test_a_long_reason_stays_inside_one_segment() -> None:
    session = _call(summary="Needs " + ("a very long explanation " * 30))
    text = call_summary_text(session)
    assert len(text) <= SMS_SEGMENT
    assert text.endswith("Call back.")
    assert "needs_callback" not in text


def test_phone_format_and_inbound_text_are_short() -> None:
    assert format_phone("+18013193675") == "801-319-3675"
    forwarded = sms_forward_text(
        from_number="+18013193675",
        to_number="+13853363562",
        body="How much is the Arena on Saturday?",
        reply_sent=True,
        category="other",
        source="anatainc.com",
    )
    assert forwarded == "Text from 801-319-3675: How much is the Arena on Saturday?"
    assert "anatainc.com" not in forwarded
    assert "Reply sent" not in forwarded
    long = sms_forward_text(
        from_number="+18013193675",
        to_number="+13853363562",
        body="word " * 80,
        reply_sent=False,
        category="other",
        source="track_eventective",
    )
    assert len(long) <= SMS_SEGMENT
    assert long.endswith("…")
    assert "track_eventective" not in long
