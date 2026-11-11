"""Short SMS answers drawn from the published facts.

Replies never confirm a date, a price that is not published, or a booking.
The caller-facing text is used only when SMS_AUTOREPLY is true.
"""

from __future__ import annotations

from voice.knowledge import ADDRESS, PUBLISHED_EMAIL
from voice.screening import looks_like_solicitation


RATES = (
    "Weekend and party Arena rental, plus 7.45% Utah tax: half day up to 6 hours $1,500; "
    "full day over 6 hours $2,000; full day plus a half day $3,000; "
    "two full days $3,500. Weekday business events are priced by the team. "
    "50% reserves the date after the agreement is signed "
    "and the payment clears. 50% is due before the event. No security deposit. "
    "You set up and reset the room inside the booked time. "
    "There is no free teardown after Saturday midnight or Sunday 10:00 p.m. "
    "This text does not reserve a date."
)

HOURS = (
    "Front desk and visitors: 9:00 a.m. to 4:00 p.m. "
    "Weekdays are for business events. "
    "Large events and parties are weekends only. "
    "Saturday events may run 8:00 a.m. to midnight. "
    "Sunday events may run 8:00 a.m. to 10:00 p.m. "
    "New Year's Eve is allowed on a weekday. "
    "You set up and clean up inside the booked time. "
    "There are no complimentary hours and no free teardown after close."
)

SECURITY = (
    "Security staffing is optional and never required. "
    "For a large party Anata can arrange it at the provider's rate plus a 20% coordination fee. "
    "Reply yes, no, or undecided and the team will follow up. "
    "This text does not reserve a date."
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
    if ("security" in text and "deposit" not in text) or any(
        word in text for word in ("guard", "bouncer")
    ):
        return SECURITY
    if any(word in text for word in ("price", "cost", "rate", "how much", "pricing")):
        return RATES
    if any(word in text for word in ("hour", "open", "close", "what time")):
        return HOURS
    if any(word in text for word in ("address", "where", "parking", "located")):
        return PLACE
    if any(word in text for word in ("available", "availability", "book", "date")):
        return DATE
    return DEFAULT
