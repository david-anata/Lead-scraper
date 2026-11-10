"""Short SMS answers drawn from the published facts.

Replies never confirm a date, a price that is not published, or a booking.
The caller-facing text is used only when SMS_AUTOREPLY is true.
"""

from __future__ import annotations

from voice.knowledge import ADDRESS, PUBLISHED_EMAIL
from voice.screening import looks_like_solicitation


RATES = (
    "Arena rental, plus 7.45% Utah tax: half day up to 6 hours $1,500; "
    "full day over 6 hours $2,000; full day plus a half day $3,000; "
    "two full days $3,500. 50% reserves the date after the agreement is signed "
    "and the payment clears. 50% is due before the event. No security deposit. "
    "This text does not reserve a date."
)

HOURS = (
    "Daytime is for small business meetings when Anata approves them. "
    "Larger weekday events run 5:00 p.m. to 11:00 p.m. "
    "Saturday events may run 8:00 a.m. to midnight. "
    "Sunday events may run 8:00 a.m. to 10:00 p.m. "
    "Setup and teardown fit inside the booked time."
)

PLACE = (
    f"Anata Building, {ADDRESS}. Shared on-site parking. "
    f"Tours are scheduled; email {PUBLISHED_EMAIL} with a time that works and the team will confirm."
)

DATE = (
    "I can't confirm a date by text. Send the date, event type, and guest count "
    "and the team will confirm. A text does not reserve the Arena."
)

DEFAULT = (
    f"Thanks for texting Anata Building. Tell us your name and what you need. "
    f"We are at {ADDRESS}. Email {PUBLISHED_EMAIL}. "
    "Event rates are the published Arena rates only, and nothing is reserved by text."
)


def classify_sms(body: str) -> str:
    if looks_like_solicitation(body):
        return "spam"
    return "other"


def reply_to_sms(body: str) -> str:
    text = (body or "").casefold()
    if looks_like_solicitation(text):
        return (
            "Thanks. Anata isn't looking for that. "
            f"For the building, email {PUBLISHED_EMAIL}."
        )
    if any(word in text for word in ("price", "cost", "rate", "how much", "pricing")):
        return RATES
    if any(word in text for word in ("hour", "open", "close", "what time")):
        return HOURS
    if any(word in text for word in ("address", "where", "parking", "located")):
        return PLACE
    if any(word in text for word in ("available", "availability", "book", "date")):
        return DATE
    return DEFAULT
