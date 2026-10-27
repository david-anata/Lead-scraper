"""Re-engagement campaign: every past lead who finished a sequence without
replying hears about the new offer (David approved 2026-10-26: his copy,
300 a day, same 18 inboxes, same schedule as the live campaign).

Created paused. Leads are COPIED (not moved) from the old campaigns with
Instantly's own "completed, no reply" filter; Instantly skips blocklisted
addresses and duplicates. Abby answers replies to it like the live campaign.
"""

from __future__ import annotations

from typing import Any

NAME = "Re-engage // New Offer (Oct 2026)"
SOURCE_CAMPAIGNS = (
    "4f118329-c07e-4279-9c08-12059f2a49f4",   # AGENT SDR // MARCH '26
    "63549149-3cbf-4447-9380-1a788605e79b",   # Amazon | Supplements | Feb 2026
)
DAILY_LIMIT = 300

EMAIL_1_SUBJECT = "{{RANDOM | new for {{companyName}} | quick update | changed how we do Amazon}}"
EMAIL_1 = """{{RANDOM | Hi | Hey}} {{firstName | there}},

I reached out a while back about {{companyName}} on Amazon. Since then we changed how we work with brands your size.

We now build your Amazon strategy and run the ad actions automatically, and you can try it free for 14 days. After that, plans start at $99 a month.

Worth a look for {{companyName}}?

{{RANDOM | Best | Cheers}},
{{sendingAccountFirstName}}"""
EMAIL_2 = """{{firstName | there}}, short version: free for 14 days, we build the strategy and run the ads. Want me to set it up?

{{sendingAccountFirstName}}"""
EMAIL_3 = """Last one from me. If Amazon isn't a priority for {{companyName}} right now, no worries. If it is, just reply "trial" and I'll get it started.

{{sendingAccountFirstName}}"""


def to_html(text: str) -> str:
    return "".join(f"<div>{line}</div>" if line.strip() else "<div><br /></div>" for line in text.split("\n"))


def campaign_body(live: dict[str, Any]) -> dict[str, Any]:
    """Same inboxes, schedule and safety settings as the live campaign."""
    steps = [
        {"type": "email", "delay": 3, "variants": [{"subject": EMAIL_1_SUBJECT, "body": to_html(EMAIL_1)}]},
        {"type": "email", "delay": 4, "variants": [{"subject": "", "body": to_html(EMAIL_2)}]},
        {"type": "email", "delay": 1, "variants": [{"subject": "", "body": to_html(EMAIL_3)}]},
    ]
    sched = live.get("campaign_schedule") or {}
    return {
        "name": NAME,
        "campaign_schedule": {"schedules": sched.get("schedules") or []},
        "sequences": [{"steps": steps}],
        "email_list": list(live.get("email_list") or []),
        "daily_limit": DAILY_LIMIT,
        "stop_on_reply": True,
        "stop_on_auto_reply": True,
        "text_only": True,
        "link_tracking": False,
        "open_tracking": False,
        "email_gap": live.get("email_gap") or 5,
        "random_wait_max": live.get("random_wait_max") or 2,
        "stop_for_company": True,
        "allow_risky_contacts": False,
        "prioritize_new_leads": False,
    }


def copy_body(source: str, target: str) -> dict[str, Any]:
    return {
        "campaign": source,
        "filter": "FILTER_VAL_COMPLETED_NO_REPLY",
        "to_campaign_id": target,
        "copy_leads": True,
        "check_duplicates": True,
        "reset_interest_status": True,
        "skip_invalid_emails": True,
    }
