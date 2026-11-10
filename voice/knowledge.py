"""Published Anata Building facts.

Rates and rules in this module are the only numbers the tools may speak.
They were checked against anatabuilding.com on November 10, 2026, including
the Arena policies page effective November 5, 2026. Office prices and live
availability are not published, so they are absent on purpose.
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo


DENVER = ZoneInfo("America/Denver")
KNOWLEDGE_PATH = Path(__file__).resolve().parent / "data" / "anata_building.md"

PUBLISHED_PHONE = "(801) 386-8104"
PUBLISHED_EMAIL = "hello@anatainc.com"
ADDRESS = "1657 N. State Street, Lehi, Utah 84043"

# Venue rental only. Exactly six hours is the half-day rate.
RENTAL_RATES = {
    "half_day_up_to_6_hours_usd": 1500,
    "full_day_more_than_6_hours_usd": 2000,
    "full_day_plus_next_half_day_usd": 3000,
    "two_full_days_usd": 3500,
    "security_deposit_usd": 0,
    "upfront_cleaning_fee_usd": 0,
    "reserve_percent": 50,
    "balance_due_before_event_percent": 50,
    "utah_tax_percent": 7.45,
}

ADDON_RATES = {
    "setup_and_reset_up_to_75_guests_usd": 250,
    "setup_and_reset_76_to_150_guests_usd": 400,
    "setup_and_reset_151_to_200_guests_usd": 550,
    "av_technician_per_hour_usd": 75,
    "av_technician_minimum_hours": 2,
    "event_labor_per_hour_per_person_usd": 125,
    "event_labor_minimum_hours": 2,
    "rush_addon_fee_percent_inside_7_days": 20,
    "security_coordination_fee_percent": 20,
    "insurance_may_be_required_from_usd": 1_000_000,
}


def load_knowledge_text() -> str:
    return KNOWLEDGE_PATH.read_text(encoding="utf-8")


def published_rates() -> dict[str, object]:
    """Venue rental facts. No computed tax-included total."""

    return {
        **RENTAL_RATES,
        "half_day_means": "Up to 6 hours, including setup and teardown.",
        "full_day_means": "More than 6 hours, including setup and teardown.",
        "security_deposit": "None.",
        "upfront_cleaning_fee": "None. Extra charges can apply after the event only if cleanup rules are not followed.",
        "payment": (
            "50% of the venue rental is due to reserve the date. The other 50% is due "
            "before the event. Setup access is not given until that second 50% has cleared."
        ),
        "cancellation": (
            "A payment is refunded in full if the client cancels within 7 days of making "
            "that payment. After that, the paid 50% reservation is forfeited and the unpaid "
            "50% is not owed. If Anata cancels for a reason within its control, customer "
            "payments are refunded."
        ),
        "confirmation": (
            "Nothing is reserved on this call. The date is confirmed only after the agreement "
            "is signed and the 50% reservation payment has cleared."
        ),
        "tax": "Add Utah tax of 7.45%. Do not quote a tax-included total.",
        "cleanup": (
            "Clients bag trash into the building's cans and do not take trash out of the "
            "building. Decorations and food they brought must be thrown away or removed "
            "before they leave."
        ),
        "setup": "Setup and teardown must fit inside the booked time. There are no free extra hours.",
    }


def published_addons() -> dict[str, object]:
    return {
        **ADDON_RATES,
        "included": (
            "Tables, chairs, stage, microphones, speakers, projector, screens, guest Wi-Fi, "
            "and the lounge are included, subject to the approved event plan. Customers set "
            "and reset tables and chairs unless they buy setup service."
        ),
        "not_published": (
            "Linens, decor, event lighting, specialty staging, and extra equipment are quoted "
            "separately. Do not invent those prices."
        ),
        "alcohol": (
            "Alcohol needs written approval and a licensed, insured server. Alcohol events "
            "require customer-paid security. Service ends 30 minutes before the event ends."
        ),
        "insurance": (
            "Anata may require a certificate of insurance for higher-risk events, beginning "
            "at $1 million per occurrence, with Anata named as an additional insured. Do not "
            "say every event needs it."
        ),
    }


def building_facts() -> dict[str, object]:
    return {
        "name": "Anata Building",
        "organization": "Anata / anata Inc., headquartered in the building",
        "address": ADDRESS,
        "email": PUBLISHED_EMAIL,
        "published_phone": PUBLISHED_PHONE,
        "arena": {
            "size_square_feet": 6000,
            "capacity": "Up to 200 guests. The final layout can reduce that.",
            "fits": "Gatherings, workshops, talks, celebrations, weddings, and community events.",
            "loading": "Side door. No elevators or loading docks. No prep kitchen. Outside food is allowed; cooking on site is not.",
        },
        "workspace": {
            "options": [
                "Private offices",
                "The Bullpen coworking desks",
                "Conference rooms",
                "Call rooms",
                "Warehouse and storage options",
            ],
            "included": "Secure access for offices, internet, utilities, mail, common areas, and on-site parking.",
            "pricing": "No office or coworking price is published. Do not guess.",
            "availability": "No office availability is published. Offer a tour and say the team will confirm.",
            "larger_teams": "A single organization with more than about 20 people needs a more specific conversation.",
            "terms": "Ask about current month-to-month options. Do not promise a term.",
        },
        "also_in_the_building": (
            "Boom Fitness Culture is an independently operated gym. Do not quote gym prices "
            "or sell memberships. A golf simulator is coming soon."
        ),
        "parking": (
            "On-site parking is shared, not exclusive, and not guaranteed. No overnight parking. "
            "The North Lehi park-and-ride can be used for overflow."
        ),
        "tours": "Tours are scheduled so someone is there. Do not promise a clock time on this call.",
    }


def hours_guidance(on: date, style: str) -> str:
    """Published hours for one date. Style is business, party, or unsure."""

    kind = (style or "unsure").strip().casefold()
    weekday = on.weekday()
    if kind == "business":
        return (
            "Small business meetings can use the building in the daytime only, and only "
            "when Anata approves the plan in advance. Do not invent the daytime hours."
        )
    if weekday == 5:
        return (
            "Saturday guest events may run 8:00 a.m. to midnight, and teardown ends by "
            "2:00 a.m. Setup and teardown still have to fit inside the booked time."
        )
    if weekday == 6:
        return (
            "Sunday guest events may run 8:00 a.m. to 10:00 p.m., and teardown ends by "
            "midnight. Setup and teardown still have to fit inside the booked time."
        )
    if kind == "party":
        return (
            "Larger or louder weekday events: quiet setup may begin at 3:00 p.m., no "
            "amplified sound before 5:00 p.m., guests 5:00 p.m. to 11:00 p.m., and "
            "teardown ends by 1:00 a.m. Setup and teardown must fit inside the booked time."
        )
    return (
        "Daytime use is for small business meetings when Anata approves them. Larger or "
        "louder weekday events run in the evening, 5:00 p.m. to 11:00 p.m., with teardown "
        "by 1:00 a.m. Saturday guest events may run 8:00 a.m. to midnight. Sunday guest "
        "events may run 8:00 a.m. to 10:00 p.m. Tell the caller which rule matches, and "
        "do not invent a different clock time."
    )


def parse_iso_date(value: str) -> date | None:
    text = (value or "").strip()
    if not text:
        return None
    try:
        return datetime.strptime(text[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def denver_today() -> date:
    return datetime.now(DENVER).date()
