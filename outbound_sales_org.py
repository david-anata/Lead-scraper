"""Sales-org segment. Draft only. Nothing here creates an Instantly campaign.

The recipe stays off until OUTBOUND_SALES_ORG_ENABLED is true and
OUTBOUND_SALES_ORG_CAMPAIGN_ID is set to a campaign David creates.
A person already in the workspace is not copied: the shared push uses
skip_if_in_workspace true.

Cold steps have no URL. The booking link is only for Abby's reply after
someone responds.
"""

from __future__ import annotations

from outbound_cold_sequence import require_no_cold_urls

RECIPE = "sales_org"
SIGNATURE = "Abby\nAccount Specialist, Anata"

EMAIL_1_SUBJECT = "{{companyName}} growth idea"
EMAIL_1 = """Hi {{firstName}},

{{personalization}}

{{brandAngle}}

At a lot of brands, Amazon sits with sales, and it's one of the quickest places to add revenue against the number. We handle the strategy and the day-to-day ads, and we keep an eye on price and the buy box so growth doesn't come at the cost of margin. You can try it free for 14 days, and plans start at $99 a month after that.

Open to a quick intro with David, or should I send a short strategy deck for {{companyName}}?

Abby
Account Specialist, Anata"""

EMAIL_2 = """Hi {{firstName}},

Quick follow-up. If Amazon is part of how {{companyName}} hits its number, we can usually find revenue that's being left on the table, whether that's listings, ads, or price and buy box issues. It's free to try for 14 days, then plans start at $99 a month.

Would a quick intro with David help, or should I send over a short strategy deck?

Abby
Account Specialist, Anata"""

EMAIL_3 = """Hi {{firstName}},

I'll leave it here. If Amazon isn't a priority for {{companyName}} right now, no worries at all. If it is, just reply "intro" or "deck" and I'll take care of the rest.

Abby
Account Specialist, Anata"""

STEPS = (
    {"delay": 3, "subject": EMAIL_1_SUBJECT, "body": EMAIL_1},
    {"delay": 4, "subject": "", "body": EMAIL_2},
    {"delay": 1, "subject": "", "body": EMAIL_3},
)

require_no_cold_urls(list(STEPS))


def sequence_text() -> str:
    """Every line David reviews, in send order."""

    return "\n\n".join(step["body"] for step in STEPS)
