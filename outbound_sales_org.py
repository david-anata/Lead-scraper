"""Sales-org segment. Draft only. Nothing here creates an Instantly campaign.

The recipe stays off until OUTBOUND_SALES_ORG_ENABLED is true and
OUTBOUND_SALES_ORG_CAMPAIGN_ID is set to a campaign David creates after
he approves this copy. A person already in the workspace is not copied:
the shared push uses skip_if_in_workspace true.
"""

from __future__ import annotations

from sales_support_agent.abby_followups import DEFAULT_BOOKING_LINK, SIGNATURE

RECIPE = "sales_org"
BOOKING_LINK = DEFAULT_BOOKING_LINK

# Three steps, threaded, same shape as the re-engage v2 sequence.
# Email 1 carries the one link. Steps 2 and 3 do not.
EMAIL_1_SUBJECT = "{{RANDOM | amazon and the number | quick note | {{companyName}} on Amazon}}"
EMAIL_1 = """{{RANDOM | Hi | Hey}} {{firstName}},

{{personalization}}

{{brandAngle}}

Amazon is one channel a sales team can use to hit the number. We build the strategy and run the ad actions, with an eye on incremental revenue, price, and the buy box. You can try it free for 14 days. After that, plans start at $99 a month.

{{RANDOM | Open to a quick intro with David | Want me to send a short strategy deck}}?

""" + BOOKING_LINK + """

""" + SIGNATURE

EMAIL_2 = """{{firstName}}, short version: free for 14 days, then plans from $99 a month. The work is incremental revenue, holding price, and protecting the buy box.

{{RANDOM | Worth a quick intro with David | Should I send the short deck}}?

""" + SIGNATURE

EMAIL_3 = """Last one from me. If Amazon is not how {{companyName}} hits the number right now, no worries. If it is, reply and I will set up the intro or send the deck.

""" + SIGNATURE

STEPS = (
    {"delay": 3, "subject": EMAIL_1_SUBJECT, "body": EMAIL_1},
    {"delay": 4, "subject": "", "body": EMAIL_2},
    {"delay": 1, "subject": "", "body": EMAIL_3},
)


def sequence_text() -> str:
    """Every line David reviews, in send order."""

    return "\n\n".join(step["body"] for step in STEPS)
