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


def dial_twiml(settings: Settings, *, http_base: str) -> str:
    action = escape(f"{http_base}/voice/dial-result", {'"': "&quot;"})
    whisper_url = escape(f"{http_base}/voice/whisper", {'"': "&quot;"})
    number = escape(settings.forward_to_number)
    # answerOnBridge keeps the caller on ringing until the on-call phone answers.
    return xml_response(
        say("Please hold."),
        (
            f'<Dial action="{action}" method="POST" timeout="20" answerOnBridge="true">'
            f'<Number url="{whisper_url}" method="POST">{number}</Number>'
            "</Dial>"
        ),
    )


def whisper_twiml(summary: str) -> str:
    text = summary.strip() or "Anata transfer. Connecting you now."
    return xml_response(say(text))


def resume_twiml(settings: Settings, *, http_base: str, call_sid: str) -> str:
    return inbound_twiml(settings, http_base=http_base, call_sid=call_sid, resume="message")


def apology_twiml() -> str:
    return xml_response(say(APOLOGY), hangup())


def goodbye_twiml() -> str:
    return xml_response(say(GOODBYE), hangup())


def empty_twiml() -> str:
    return xml_response()


def sms_twiml(reply: str) -> str:
    if not reply.strip():
        return empty_twiml()
    return xml_response(message(reply))
