"""Arena event policy effective November 10, 2026.

David Narayan confirmed this card. The voice receptionist knowledge in
``voice/data/anata_building.md`` matches it. Pricing, hours, deposits, and
the words a quote or auto-reply uses should come from here so they cannot
drift back to the retired hourly rate.

There is no hourly rate, no security deposit, and no upfront cleaning fee.
Weekday business events are not auto-quoted. Large parties are weekends only,
inside the posted hours, except New Year's Eve.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo


MOUNTAIN = ZoneInfo("America/Denver")

#: Published venue rental. Amounts are cents.
HALF_DAY_CENTS = 150_000
FULL_DAY_CENTS = 200_000
FULL_PLUS_HALF_CENTS = 300_000
TWO_FULL_DAYS_CENTS = 350_000

PUBLISHED_CARD_CENTS = {
    "half": HALF_DAY_CENTS,
    "full": FULL_DAY_CENTS,
    "full_plus_half": FULL_PLUS_HALF_CENTS,
    "two_full": TWO_FULL_DAYS_CENTS,
}

DAY_PORTION_LABELS = {
    "half": "Half day (up to 6 hours)",
    "full": "Full day",
    "full_plus_half": "Full day + second-day half",
    "two_full": "Two full days",
}

#: Representative hour counts the public estimator already used for the card.
#: A real start and end on one calendar day never uses these multi-day buckets.
HOUR_BUCKET_FULL_MAX = 12
HOUR_BUCKET_FULL_PLUS_HALF_MAX = 18
HOUR_BUCKET_TWO_FULL_MAX = 24

#: The retired card. Stored leads that still have exactly these figures are
#: restated on the new card. A different hourly rate was negotiated and stays.
LEGACY_HOURLY_RATE_CENTS = 17_500
LEGACY_CLEANING_FEE_CENTS = 25_000
LEGACY_SECURITY_DEPOSIT_CENTS = 50_000

RESERVATION_PERCENT_BPS = 5_000

CANCELLATION_POLICY = (
    "50% of the venue rental is due to book the date. The remaining 50% is "
    "due before the event. There is no security deposit and no upfront "
    "cleaning fee. If the client cancels within 7 days of a payment, that "
    "payment is refunded in full. If the client cancels more than 7 days "
    "after payment, the paid 50% is forfeited and the unpaid 50% is not owed. "
    "One date transfer may be requested at least 14 days before the event, "
    "to an available date within six months, at pricing current for the "
    "replacement date. If Anata cancels for a reason within its control, "
    "customer payments are refunded."
)

WEEKDAY_BUSINESS_MESSAGE = (
    "Weekdays are for business events. The team will put together pricing "
    "for this event and can offer a tour."
)

PARTY_HOURS_MESSAGE = (
    "Large events and parties are weekends only. Saturday guest hours are "
    "8:00 AM to midnight. Sunday guest hours are 8:00 AM to 10:00 PM. "
    "New Year's Eve is allowed on any day of the week, and the team confirms "
    "those hours."
)

PARTY_REVIEW_QUESTIONS = (
    "How many people are you expecting?",
    "Will there be drinks, and if so who is the licensed bartender or caterer serving them?",
    "What do you have in mind for security?",
    "What time do you hope to wrap up?",
    "Guests bag trash into the building's cans and take their decorations and food with them. Does that work?",
)

_PARTY_WORDS = (
    "party",
    "rager",
    "open bar",
    "wedding",
    "reception",
    "quince",
    "birthday",
    "celebration",
    "nightclub",
    "dance",
)
_BUSINESS_WORDS = (
    "meeting",
    "workshop",
    "bni",
    "corporate",
    "conference",
    "training",
    "seminar",
    "offsite",
    "business",
    "board",
)
_ALCOHOL_YES = {"yes", "y", "true", "alcohol", "bar", "open bar", "open-bar"}


def portion_for_hours(hours: float | int | None) -> str | None:
    """Map a bare hour count onto the published card.

    6 hours is a half day. 8 hours is a full day. 18 hours stands in for a
    day and a half, and 24 hours for two full days. Longer than that is not
    on the card.
    """

    if hours is None:
        return None
    amount = float(hours)
    if amount <= 0:
        return None
    if amount <= 6:
        return "half"
    if amount <= HOUR_BUCKET_FULL_MAX:
        return "full"
    if amount <= HOUR_BUCKET_FULL_PLUS_HALF_MAX:
        return "full_plus_half"
    if amount <= HOUR_BUCKET_TWO_FULL_MAX:
        return "two_full"
    return None


def portion_for_days(days: float | int | None) -> str | None:
    """Map 0.5, 1, 1.5, and 2 days onto the published card."""

    if days is None:
        return None
    amount = float(days)
    if amount <= 0:
        return None
    if amount <= 0.5:
        return "half"
    if amount <= 1:
        return "full"
    if amount <= 1.5:
        return "full_plus_half"
    if amount <= 2:
        return "two_full"
    return None


def quote_duration(
    *,
    hours: float | int | None = None,
    days: float | int | None = None,
    day_portion: str | None = None,
) -> tuple[str | None, int | None]:
    """Return ``(portion, cents)`` for one published block, or ``(None, None)``."""

    portion = str(day_portion or "").strip() or None
    if portion is None and days is not None:
        portion = portion_for_days(days)
    if portion is None and hours is not None:
        portion = portion_for_hours(hours)
    if portion not in PUBLISHED_CARD_CENTS:
        return None, None
    return portion, PUBLISHED_CARD_CENTS[portion]


def portion_for_span(starts_at: datetime, ends_at: datetime) -> str | None:
    """Price a real start and end by the calendar days it occupies.

    One calendar day is a half day up to 6 hours and a full day after that,
    including a Saturday that runs 8:00 AM to midnight. A second day is a
    half day when it is 6 hours or less and a full day when it is longer.
    """

    start = _aware(starts_at).astimezone(MOUNTAIN)
    end = _aware(ends_at).astimezone(MOUNTAIN)
    if end <= start:
        return None
    lengths = _hours_per_calendar_day(start, end)
    if not lengths or len(lengths) > 2:
        return None
    kinds = ["half" if hours <= 6 else "full" for hours in lengths]
    if kinds == ["half"]:
        return "half"
    if kinds == ["full"]:
        return "full"
    if sorted(kinds) == ["full", "half"]:
        return "full_plus_half"
    if kinds == ["full", "full"]:
        return "two_full"
    return None


def is_new_years_eve(day: date | None) -> bool:
    return day is not None and day.month == 12 and day.day == 31


def guest_hour_bounds(day: date) -> tuple[int, int]:
    """Inclusive picker hours. 24 means midnight at the end of Saturday."""

    if day.weekday() == 5:
        return 8, 24
    if day.weekday() == 6:
        return 8, 22
    return 8, 23


def parse_clock(value: Any) -> time | None:
    """Read ``17:00``, ``5:00 PM``, or ``24:00`` (midnight at the end of the day)."""

    text = str(value or "").strip()
    if not text:
        return None
    if text in {"24:00", "24:00:00"}:
        return time(0, 0)
    lowered = text.lower().replace(".", "")
    for pattern in ("%H:%M", "%H:%M:%S", "%I:%M %p", "%I %p", "%I:%M%p"):
        try:
            return datetime.strptime(lowered, pattern).time()
        except ValueError:
            continue
    try:
        return time.fromisoformat(text)
    except ValueError:
        return None


def classify_event(
    *,
    event_type: str = "",
    attendance: int | None = None,
    alcohol: str = "",
    notes: str = "",
) -> str:
    """Return ``party``, ``business``, or ``easy``.

    A party is a large headcount, alcohol outside a business meeting, an open
    bar, or rager-style wording. Business meetings and small gatherings stay
    easy so they are not run through the party questions.
    """

    text = " ".join(
        part for part in (event_type, notes, alcohol) if str(part or "").strip()
    ).casefold()
    business = any(word in text for word in _BUSINESS_WORDS)
    party_words = any(word in text for word in _PARTY_WORDS) or "rager" in text
    alcohol_yes = _alcohol_yes(alcohol, text)
    headcount = int(attendance or 0)
    if party_words or "open bar" in text:
        return "party"
    if headcount >= 100 and not business:
        return "party"
    if alcohol_yes and not business:
        return "party"
    if business:
        return "business"
    return "easy"


@dataclass(frozen=True)
class EventQuoteDecision:
    """What the estimator, the lead page, and the auto-reply should do."""

    kind: str
    quotable: bool
    rejected: bool
    review_required: bool
    message: str
    questions: tuple[str, ...]
    day_portion: str | None
    venue_cents: int | None

    @property
    def auto_confirm(self) -> bool:
        """Party events are reviewed by the team. Nothing is confirmed here."""

        return False


def evaluate_event_request(
    *,
    event_date: date | None = None,
    guest_start: Any = None,
    guest_end: Any = None,
    event_type: str = "",
    attendance: int | None = None,
    alcohol: str = "",
    notes: str = "",
    hours: float | int | None = None,
    days: float | int | None = None,
    day_portion: str | None = None,
) -> EventQuoteDecision:
    """Apply the November 10 policy to one request.

    A missing date does not invent a weekday. Hours are still priced from the
    card. A weekday business request produces no price. A party outside the
    posted weekend hours is rejected, except New Year's Eve.
    """

    kind = classify_event(
        event_type=event_type,
        attendance=attendance,
        alcohol=alcohol,
        notes=notes,
    )
    start_clock = parse_clock(guest_start)
    end_clock = parse_clock(guest_end)
    if event_date is not None and kind == "party" and not is_new_years_eve(event_date):
        problem = _party_schedule_problem(event_date, start_clock, end_clock)
        if problem:
            return EventQuoteDecision(
                kind="party",
                quotable=False,
                rejected=True,
                review_required=True,
                message=problem,
                questions=PARTY_REVIEW_QUESTIONS,
                day_portion=None,
                venue_cents=None,
            )
    if event_date is not None and event_date.weekday() < 5 and not is_new_years_eve(event_date):
        if kind == "party":
            return EventQuoteDecision(
                kind="party",
                quotable=False,
                rejected=True,
                review_required=True,
                message=PARTY_HOURS_MESSAGE,
                questions=PARTY_REVIEW_QUESTIONS,
                day_portion=None,
                venue_cents=None,
            )
        return EventQuoteDecision(
            kind="business" if kind == "business" else "easy",
            quotable=False,
            rejected=False,
            review_required=False,
            message=WEEKDAY_BUSINESS_MESSAGE,
            questions=(),
            day_portion=None,
            venue_cents=None,
        )

    portion, amount = _price_request(
        event_date=event_date,
        start_clock=start_clock,
        end_clock=end_clock,
        hours=hours,
        days=days,
        day_portion=day_portion,
    )
    review = kind == "party"
    if portion is None:
        message = (
            "This length is not on the published card. The team will put "
            "together pricing."
        )
        return EventQuoteDecision(
            kind=kind,
            quotable=False,
            rejected=False,
            review_required=review,
            message=message,
            questions=PARTY_REVIEW_QUESTIONS if review else (),
            day_portion=None,
            venue_cents=None,
        )
    message = ""
    if review:
        message = (
            "This celebration uses the published day rates. The team reviews "
            "headcount, alcohol service, security, end time, and cleanup "
            "before it is confirmed."
        )
    return EventQuoteDecision(
        kind=kind,
        quotable=True,
        rejected=False,
        review_required=review,
        message=message,
        questions=PARTY_REVIEW_QUESTIONS if review else (),
        day_portion=portion,
        venue_cents=amount,
    )


def customer_receipt_note(decision: EventQuoteDecision) -> str:
    """Extra sentences for the inquiry auto-reply. Never says the date is held."""

    if decision.rejected:
        return (
            "Large events and parties run on Saturday from 8:00 a.m. to "
            "midnight, or Sunday from 8:00 a.m. to 10:00 p.m. New Year's Eve "
            "can fall on a weekday. The team will follow up about a weekend "
            "that works. This email does not book the date."
        )
    if decision.message == WEEKDAY_BUSINESS_MESSAGE:
        return (
            f"{WEEKDAY_BUSINESS_MESSAGE} This email does not book the date."
        )
    if decision.kind == "party":
        return (
            "Weekend pricing is $1,500 for a half day (up to 6 hours), "
            "$2,000 for a full day, $3,000 for a full day plus a half day, "
            "and $3,500 for two full days. The team will ask about headcount, "
            "who serves any alcohol, security, the end time, and cleanup. "
            "This email does not book the date."
        )
    if decision.quotable:
        return (
            "Published pricing is $1,500 for a half day (up to 6 hours), "
            "$2,000 for a full day, $3,000 for a full day plus a half day, "
            "and $3,500 for two full days. Half is due to book the date and "
            "the rest is due before the event. There is no security deposit "
            "and no upfront cleaning fee. This email does not book the date."
        )
    return ""


def _price_request(
    *,
    event_date: date | None,
    start_clock: time | None,
    end_clock: time | None,
    hours: float | int | None,
    days: float | int | None,
    day_portion: str | None,
) -> tuple[str | None, int | None]:
    if day_portion:
        return quote_duration(day_portion=day_portion)
    if (
        event_date is not None
        and start_clock is not None
        and end_clock is not None
    ):
        start, end = _span(event_date, start_clock, end_clock)
        portion = portion_for_span(start, end)
        if portion is None:
            return None, None
        return portion, PUBLISHED_CARD_CENTS[portion]
    return quote_duration(hours=hours, days=days)


def _party_schedule_problem(
    day: date,
    start: time | None,
    end: time | None,
) -> str | None:
    if day.weekday() < 5:
        return PARTY_HOURS_MESSAGE
    if start is None or end is None:
        return None
    start_m, end_m = _minutes(start, end)
    open_m = 8 * 60
    close_m = 24 * 60 if day.weekday() == 5 else 22 * 60
    if start_m < open_m or end_m > close_m:
        return PARTY_HOURS_MESSAGE
    return None


def _minutes(start: time, end: time) -> tuple[int, int]:
    start_m = start.hour * 60 + start.minute
    end_m = end.hour * 60 + end.minute
    # Midnight at the end of the day is 24:00, not a zero-length event.
    if end == time(0, 0) and start != time(0, 0):
        end_m = 24 * 60
    elif end_m <= start_m:
        end_m += 24 * 60
    return start_m, end_m


def _span(day: date, start: time, end: time) -> tuple[datetime, datetime]:
    starts_at = datetime.combine(day, start, tzinfo=MOUNTAIN)
    ends_at = datetime.combine(day, end, tzinfo=MOUNTAIN)
    if end == time(0, 0) and start != time(0, 0):
        ends_at += timedelta(days=1)
    elif ends_at <= starts_at:
        ends_at += timedelta(days=1)
    return starts_at, ends_at


def _hours_per_calendar_day(start: datetime, end: datetime) -> list[float]:
    """Hours on each Mountain calendar day, ignoring an exact-midnight boundary."""

    lengths: list[float] = []
    cursor = start
    while cursor < end:
        next_midnight = datetime.combine(
            cursor.date() + timedelta(days=1), time.min, tzinfo=MOUNTAIN
        )
        stop = min(end, next_midnight)
        hours = (stop - cursor).total_seconds() / 3600
        if hours > 0:
            lengths.append(hours)
        cursor = stop
    return lengths


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=MOUNTAIN)
    return value


def _alcohol_yes(alcohol: str, text: str) -> bool:
    token = str(alcohol or "").strip().casefold()
    if token in {"no", "n", "false", "none", "no alcohol"}:
        return False
    if "no alcohol" in text or "without alcohol" in text:
        return False
    if token in _ALCOHOL_YES:
        return True
    return "open bar" in text


def attendance_count(value: Any) -> int | None:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    if not digits:
        return None
    return int(digits)
