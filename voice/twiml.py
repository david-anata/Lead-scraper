"""TwiML documents for the inbound call, the forward, and SMS replies."""

from __future__ import annotations

from xml.sax.saxutils import escape

from voice.security import mint_stream_token
from voice.settings import Settings


RECORDING_NOTICE = "This call may be recorded."
NOT_READY = (
    "Thanks for calling Anata. This line is not ready yet. Please try again later."
)
APOLOGY = (
    "Sorry about that. Something went wrong on our side. "
    "Please call again, or email hello@anatainc.com."
)
GOODBYE = "Thanks for calling Anata. The team has your message."
# Twilio's standard hold-music twimlet. The caller hears this in the conference
# until the on-call phone presses 1. It replaces ringing.
HOLD_MUSIC_URL = "https://twimlets.com/holdmusic?Bucket=com.twilio.music.classical"
RING_TIMEOUT_SECONDS = 25
ACCEPT_TIMEOUT_SECONDS = 25


def xml_response(*inner: str) -> str:
    body = "".join(inner)
    return f'<?xml version="1.0" encoding="UTF-8"?><Response>{body}</Response>'


def say(text: str) -> str:
    return f"<Say>{escape(text)}</Say>"


def hangup() -> str:
    return "<Hangup/>"


def message(text: str) -> str:
    return f"<Message>{escape(text)}</Message>"


def _ws_base(http_base: str) -> str:
    if http_base.startswith("https://"):
        return "wss://" + http_base[len("https://") :]
    if http_base.startswith("http://"):
        return "ws://" + http_base[len("http://") :]
    return http_base


def media_ws_url(http_base: str) -> str:
    """Twilio does not allow query parameters on a Stream URL."""

    return f"{_ws_base(http_base)}/voice/media"


def connect_stream(url: str, token: str) -> str:
    """Pass the stream token as a custom parameter, which arrives in `start`."""

    safe_url = escape(url, {'"': "&quot;"})
    safe_token = escape(token, {'"': "&quot;"})
    return (
        f'<Connect><Stream url="{safe_url}">'
        f'<Parameter name="token" value="{safe_token}"/>'
        "</Stream></Connect>"
    )


def inbound_twiml(
    settings: Settings,
    *,
    http_base: str,
    call_sid: str,
    resume: str = "",
) -> str:
    """Answer with a media stream. The recording notice is spoken only when enabled."""

    if not settings.calls_ready or not http_base:
        return xml_response(say(NOT_READY), hangup())
    parts: list[str] = []
    if settings.recording_enabled and not resume:
        parts.append(say(RECORDING_NOTICE))
        parts.append(
            "<Start><Recording channels=\"mono\" trim=\"do-not-trim\"/></Start>"
        )
    token = mint_stream_token(call_sid, resume, settings.twilio_auth_token)
    parts.append(connect_stream(media_ws_url(http_base), token))
    return xml_response(*parts)


def conference_name(call_sid: str) -> str:
    return f"anata-{call_sid}"


def _attr(value: str) -> str:
    return escape(value, {'"': "&quot;"})


def caller_hold_twiml(*, http_base: str, call_sid: str) -> str:
    """Park the caller in a conference with hold music instead of ringback.

    The conference does not start until the on-call phone presses 1 and joins.
    `startConferenceOnEnter` stays false so `waitUrl` keeps playing.
    """

    action = _attr(f"{http_base}/voice/dial-result")
    wait = _attr(HOLD_MUSIC_URL)
    name = _attr(conference_name(call_sid))
    return xml_response(
        say("Please hold."),
        (
            f'<Dial action="{action}" method="POST">'
            f'<Conference waitUrl="{wait}" waitMethod="GET" '
            f'startConferenceOnEnter="false" endConferenceOnExit="true" beep="false">'
            f"{name}</Conference>"
            "</Dial>"
        ),
    )


def whisper_twiml(summary: str, accept_url: str) -> str:
    """Play the whisper, then wait for digit 1. Anything else hangs up."""

    text = summary.strip() or "Anata transfer. Press 1 to accept."
    if "press 1" not in text.casefold():
        text = text.rstrip(".") + ". Press 1 to accept."
    action = _attr(accept_url)
    return xml_response(
        (
            f'<Gather numDigits="1" timeout="{ACCEPT_TIMEOUT_SECONDS}" '
            f'action="{action}" method="POST">'
            f"{say(text)}</Gather>"
        ),
        hangup(),
    )


def join_conference_twiml(call_sid: str) -> str:
    """Connect the on-call phone after it presses 1. This starts the conference."""

    name = _attr(conference_name(call_sid))
    return xml_response(
        "<Dial>"
        f'<Conference startConferenceOnEnter="true" endConferenceOnExit="true" beep="false">'
        f"{name}</Conference>"
        "</Dial>"
    )


def resume_twiml(settings: Settings, *, http_base: str, call_sid: str) -> str:
    return inbound_twiml(settings, http_base=http_base, call_sid=call_sid, resume="message")


def apology_twiml() -> str:
    return xml_response(say(APOLOGY), hangup())


AI_FALLBACK_HOLD = "Please hold. I'm going to try David."
AI_FALLBACK_VOICEMAIL = (
    "Sorry, David couldn't pick up just now. "
    "Please leave your name and number after the tone, and he will call you back."
)


def ai_fallback_dial_twiml(*, http_base: str, call_sid: str, forward_to: str) -> str:
    """Ring David's existing transfer number. The action URL handles no-answer."""

    action = _attr(f"{http_base}/voice/ai-fallback?parent={quote_path(call_sid)}")
    number = escape(forward_to)
    return xml_response(
        say(AI_FALLBACK_HOLD),
        (
            f'<Dial timeout="20" action="{action}" method="POST">'
            f"<Number>{number}</Number>"
            "</Dial>"
        ),
    )


def ai_fallback_record_twiml(*, http_base: str, call_sid: str) -> str:
    """Record a message after the forward fails or goes unanswered."""

    action = _attr(f"{http_base}/voice/ai-fallback-recording?parent={quote_path(call_sid)}")
    return xml_response(
        say(AI_FALLBACK_VOICEMAIL),
        (
            f'<Record maxLength="120" playBeep="true" timeout="5" '
            f'action="{action}" method="POST"/>'
        ),
        say("Thanks. He will call you back."),
        hangup(),
    )


def quote_path(value: str) -> str:
    from urllib.parse import quote

    return quote(value, safe="")


def goodbye_twiml() -> str:
    return xml_response(say(GOODBYE), hangup())


def empty_twiml() -> str:
    return xml_response()


def sms_twiml(reply: str) -> str:
    if not reply.strip():
        return empty_twiml()
    return xml_response(message(reply))
