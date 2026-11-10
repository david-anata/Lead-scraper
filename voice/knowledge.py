"""Published Anata Building and anata Inc. facts.

Rates and rules in this module are the only numbers the tools may speak.
They were checked against anatabuilding.com and anatainc.com on November 10,
2026, including the Arena policies page effective November 5, 2026. Private
office prices, marketing fees, fulfillment fees, and live availability are not
published, so they are absent on purpose.
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo


DENVER = ZoneInfo("America/Denver")
DATA_DIR = Path(__file__).resolve().parent / "data"
KNOWLEDGE_PATH = DATA_DIR / "anata_building.md"
INC_KNOWLEDGE_PATH = DATA_DIR / "anata_inc.md"
FRONT_DESK_PATH = DATA_DIR / "front_desk.md"
# Order matters: the front desk rules refer to the other two.
KNOWLEDGE_FILES = (FRONT_DESK_PATH, KNOWLEDGE_PATH, INC_KNOWLEDGE_PATH)

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

# anatabuilding.com/spaces. Private offices are quoted on the tour.
WORKSPACE_RATES = {
    "bullpen_desk_from_usd_per_month": 150,
    "warehouse_usd_per_sq_ft_low": 2,
    "warehouse_usd_per_sq_ft_high": 4,
}

# anatainc.com/trial and /platform/anata-intelligence. Software only.
ANATA_INTELLIGENCE_PLANS = {
    "brand_starter_usd_per_month": 99,
    "brand_growth_usd_per_month": 199,
    "brand_pro_usd_per_month": 399,
    "agency_from_usd_per_month": 999,
    "trial_days": 14,
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
    """Every knowledge file, in prompt order."""

    return "\n\n".join(path.read_text(encoding="utf-8").strip() for path in KNOWLEDGE_FILES) + "\n"


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
            "bullpen_coworking": (
                "From $150 per month for a desk in the shared open-plan office. Internet, "
                "utilities, parking, mail room, and shared conference and call rooms included."
            ),
            "private_offices": (
                "Priced by size. No published price. Quoted on the tour based on the room "
                "and the term. Do not guess a number."
            ),
            "warehouse": (
                "Warehouse South: $2 to $4 per square foot, depending on size and access. "
                "Do not say per month or per year; the team confirms the term."
            ),
            "availability": "No office, desk, or warehouse availability is published. Offer a tour and say the team will confirm.",
            "larger_teams": "A single organization with more than about 20 people needs a more specific conversation.",
            "terms": "Ask about current month-to-month options. Do not promise a term.",
        },
        "also_in_the_building": (
            "Boom Fitness Culture is an independently operated gym. Do not quote gym prices "
            "or sell memberships. A golf simulator is coming soon; it is not bookable yet."
        ),
        "current_tenants": (
            "Mail is dropped at the front desk and then goes to the mail room. Spare lock "
            "batteries are at the front desk and tenants swap their own. Anything else is "
            "a message for the team. Do not discuss rent, balances, or billing disputes."
        ),
        "parking": (
            "On-site parking is shared, not exclusive, and not guaranteed. No overnight parking. "
            "The North Lehi park-and-ride can be used for overflow."
        ),
        "tours": "Tours are scheduled so someone is there. Do not promise a clock time on this call.",
    }


def anata_inc_facts() -> dict[str, object]:
    """anata Inc. agency, fulfillment, and software facts from anatainc.com."""

    return {
        "who": (
            "anata Inc. is an ecommerce marketing, marketplace operations, and fulfillment "
            "company headquartered in the Anata Building in Lehi. Amazon Ads Verified Partner, "
            "10+ years. Typical marketing client: a brand doing about $1M to $20M a year."
        ),
        "marketing_services": [
            "Amazon advertising: PPC and DSP",
            "Amazon marketing, account, and brand management",
            "Amazon listing optimization",
            "TikTok Shop management and ads",
            "Shopify marketing management",
            "Facebook and Instagram ads",
        ],
        "marketing_pricing": (
            "Not published. It starts with a free market report on anatainc.com; the team "
            "then sends a recommended service mix and an estimated range. Do not quote a fee."
        ),
        "fulfillment": (
            "Full 3PL from the Lehi warehouse: receiving, pick, pack, ship, returns, a named "
            "account manager, Shopify, Amazon, and EDI connections, and carriers USPS, UPS, "
            "FedEx, DHL, GLS, and UniUni. Amazon FBA prep too. Onboarding usually under two "
            "weeks. Fees are not published; quoted after volume and product details."
        ),
        "software": {
            **ANATA_INTELLIGENCE_PLANS,
            "anata_intelligence": (
                "Profit and ad analytics plus Amazon ad automation. Brand Starter $99, Brand "
                "Growth $199, Brand Pro $399 a month after a 14-day trial. Agency plans start "
                "at $999 a month. Say Agency plan, never Portfolio. Do not say whether the "
                "trial needs a card; the team will confirm."
            ),
            "shipping_os": (
                "Software for brands and 3PLs that ship in-house on Anata's negotiated carrier "
                "rates. Its plan prices are not published here."
            ),
        },
        "qualify": (
            "Name, brand and website, marketplaces or stores, rough revenue range, what they "
            "need, timing, email, and callback number. Do not ask for passwords."
        ),
        "contact": "david@anatainc.com, (801) 386-8104, anatainc.com",
    }


_LARGE_EVENT_HOURS = (
    "Large events, roughly 50 or more guests, conferences, and parties, can only be "
    "booked on weekday evenings, 5:00 p.m. to 11:00 p.m. Quiet setup may begin at "
    "3:00 p.m., no amplified sound before 5:00 p.m., and teardown ends by 1:00 a.m. "
    "Do not quote Saturday or Sunday hours. If they asked for a weekend or a daytime "
    "slot, explain the rule, offer a weekday evening, and still take the lead."
)


def hours_guidance(on: date, style: str) -> str:
    """Published hours for one date. Style is business, large, party, or unsure."""

    kind = (style or "unsure").strip().casefold()
    weekday = on.weekday()
    if kind in {"party", "large"}:
        return _LARGE_EVENT_HOURS
    if kind == "business":
        return (
            "Daytime Arena use is only for small business meetings, such as a BNI chapter "
            "meeting, and only when Anata approves the plan in advance. Do not invent the "
            "daytime hours. A large event is not a daytime booking; it can only be a "
            "weekday evening, 5:00 p.m. to 11:00 p.m."
        )
    if weekday == 5:
        return (
            "These Saturday hours are not for large events. Saturday guest events may run "
            "8:00 a.m. to midnight, and teardown ends by 2:00 a.m. A large event, about "
            "50 or more guests, a conference, or a party, can only be a weekday evening, "
            "5:00 p.m. to 11:00 p.m. Do not quote these Saturday hours for that."
        )
    if weekday == 6:
        return (
            "These Sunday hours are not for large events. Sunday guest events may run "
            "8:00 a.m. to 10:00 p.m., and teardown ends by midnight. A large event, about "
            "50 or more guests, a conference, or a party, can only be a weekday evening, "
            "5:00 p.m. to 11:00 p.m. Do not quote these Sunday hours for that."
        )
    return (
        "Daytime use is only for small business meetings such as BNI, when Anata approves "
        "them. Large events, roughly 50 or more guests, conferences, and parties, can only "
        "be booked on weekday evenings, 5:00 p.m. to 11:00 p.m. Do not quote weekend hours "
        "for a large event. If they asked for a weekend or a daytime slot, offer a weekday "
        "evening and still take the lead."
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
