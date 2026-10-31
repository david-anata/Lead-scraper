"""David-approved follow-up copy Abby can load.

These templates are data only. Nothing here sends email, changes Abby's
mode, or writes to Instantly.
"""

from __future__ import annotations

from dataclasses import dataclass

PERSONALIZATION_RULE = (
    "Abby opens each follow-up with one personal line written from the "
    "recipient's actual reply text or StoreLeads data (category, recent "
    "product launch) when available, and drops the line when nothing is "
    "available. She never guesses or invents details. If the reply mentions "
    "health or family, she uses only neutral wording such as 'Hope all is well'."
)

SIGNATURE = "Abby\nAccount Specialist, Anata"
DEFAULT_BOOKING_LINK = "https://calendar.app.google/5TwjbbpmKaGAQuBk8"
APPROVED_BY = "David"
APPROVED_ON = "2026-10-30"
APPROVED_STATUS = "approved"


@dataclass(frozen=True)
class FollowupStep:
    """One sendable step of an approved follow-up."""

    key: str
    send_when: str
    body: str
    personal_line_source: str = ""
    personal_lines: tuple[tuple[str, str], ...] = ()
    example_personal_line: str = ""
    alternate_body: str = ""
    alternate_when: str = ""
    optional_detail: str = ""
    optional_detail_example: str = ""


@dataclass(frozen=True)
class ApprovedFollowup:
    """A follow-up David approved, with the signature attached to every step."""

    key: str
    status: str
    approved_by: str
    approved_on: str
    steps: tuple[FollowupStep, ...]
    signature: str = SIGNATURE

    def signed_copies(self) -> tuple[str, ...]:
        """Each approved body with the signature. Does not send."""

        copies: list[str] = []
        for step in self.steps:
            copies.append(_with_signature(step.body, self.signature))
            if step.alternate_body:
                copies.append(_with_signature(step.alternate_body, self.signature))
        return tuple(copies)

    def copy_parts(self) -> tuple[str, ...]:
        """Every approved string on this template, including the signature."""

        parts = [self.signature, self.key, self.status, self.approved_by, self.approved_on]
        for step in self.steps:
            parts.extend((
                step.key, step.send_when, step.body, step.personal_line_source,
                step.example_personal_line, step.alternate_body, step.alternate_when,
                step.optional_detail, step.optional_detail_example,
            ))
            parts.extend(reason for reason, _line in step.personal_lines)
            parts.extend(line for _reason, line in step.personal_lines)
        return tuple(part for part in parts if part)


def _with_signature(body: str, signature: str) -> str:
    text = body.strip()
    if text.endswith(signature):
        return text
    return f"{text}\n\n{signature}"


_OOO = ApprovedFollowup(
    key="out_of_office",
    status=APPROVED_STATUS,
    approved_by=APPROVED_BY,
    approved_on=APPROVED_ON,
    steps=(
        FollowupStep(
            key="after_return",
            send_when="the day after the return date parsed from the auto-reply",
            personal_line_source="the reason in the auto-reply",
            personal_lines=(
                ("vacation", "Hope you had a great vacation."),
                ("conference_or_trade_show", "Hope the show was worth the trip."),
                ("no_reason", "Hope your time away went well."),
            ),
            body=(
                "Hi {first_name}, {personal_line} Circling back on my note about "
                "{brand}'s Amazon and Shopify growth. Worth a quick look this week? "
                "Here's my calendar: {booking_link}"
            ),
        ),
    ),
)

_INTERESTED = ApprovedFollowup(
    key="interested_not_booked",
    status=APPROVED_STATUS,
    approved_by=APPROVED_BY,
    approved_on=APPROVED_ON,
    steps=(
        FollowupStep(
            key="step_1",
            send_when="2 days after their reply",
            personal_line_source=(
                "what they said they liked or asked about in the reply, or StoreLeads"
            ),
            body=(
                "Hi {first_name}, {personal_line} That's exactly what the free "
                "14-day trial is built for. Here's my calendar if a quick call works: "
                "{booking_link}. Or you can start the trial today here: "
                "https://dashboard.anatainc.com/signup. Which do you prefer?"
            ),
        ),
        FollowupStep(
            key="step_2",
            send_when="4 days after step 1",
            body=(
                "Hi {first_name}, should I hold a trial spot for {brand}, "
                "or is the timing off right now?"
            ),
            alternate_when="their reply gave a timing hint, such as 'after our launch'",
            alternate_body=(
                "Hi {first_name}, you mentioned {timing_hint}. Should I hold a trial "
                "spot for {brand}, or is the timing off right now?"
            ),
        ),
    ),
)

_WRONG_PERSON = ApprovedFollowup(
    key="wrong_person",
    status=APPROVED_STATUS,
    approved_by=APPROVED_BY,
    approved_on=APPROVED_ON,
    steps=(
        FollowupStep(
            key="ask_owner",
            send_when="immediately",
            personal_line_source="the role they mentioned",
            example_personal_line="makes sense since you're on the design side.",
            body=(
                "Thanks {first_name}, {personal_line} Who handles Amazon or "
                "marketing growth for {brand}?"
            ),
        ),
        FollowupStep(
            key="referral",
            send_when="immediately, to the named colleague",
            optional_detail="{storeleads_detail}",
            optional_detail_example=", especially with your recent launch of {product}",
            body=(
                "Hi {new_name}, {first_name} mentioned you {role_from_reply} at {brand}. "
                "I reached out about growing {brand}'s sales on Amazon and Shopify"
                "{storeleads_detail}. Open to a quick look? We offer a free 14-day trial."
            ),
        ),
    ),
)

APPROVED_FOLLOWUPS: tuple[ApprovedFollowup, ...] = (_OOO, _INTERESTED, _WRONG_PERSON)


def load_approved_followups() -> dict[str, ApprovedFollowup]:
    """Return the follow-ups marked approved. Loading does not send anything."""

    return {
        item.key: item
        for item in APPROVED_FOLLOWUPS
        if item.status == APPROVED_STATUS
    }
