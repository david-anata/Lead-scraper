"""Re-engagement campaign: every past lead who finished a sequence without
replying hears about the new offer (David approved 2026-10-26: his copy,
300 a day, same 18 inboxes, same schedule as the live campaign).

Created paused. Leads are COPIED (not moved) from the old campaigns with
Instantly's own "completed, no reply" filter; Instantly skips blocklisted
addresses and duplicates. Abby answers replies to it like the live campaign.
"""

from __future__ import annotations

from typing import Any, Optional

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


# Instantly lead status values observed on the list endpoint.
_STATUS_COMPLETED = 3
_STATUS_ACTIVE = 1
_NEGATIVE_INTEREST = frozenset({-1, -2, -3, "-1", "-2", "-3"})
_CLOSED_STATUSES = frozenset({-1, -2, -3})
TARGET_CAMPAIGN_ID = "907d32fd-d6db-4ce6-8edc-485b78cce120"
# Old completed sequences only. Trial Offer Oct is not a source: its
# current sends and follow-ups stay where they are.
TOPUP_SOURCE_CAMPAIGNS = SOURCE_CAMPAIGNS + (
    "8e0df427-1f18-43ac-b0c6-e8ea787123d7",
    "138a162d-6654-4916-9959-20c11e31214b",
    "4dc10f79-f7c9-43c5-9c3e-5e28e9cf4c72",
    "d1c197e3-bf1f-4f41-bcb9-e30e73975a2c",
)


def _interest(lead: dict[str, Any]) -> str:
    return str(lead.get("lt_interest_status") if lead.get("lt_interest_status") is not None else lead.get("interest_status") or "").strip().lower()


def _reply_count(lead: dict[str, Any]) -> int:
    for key in ("email_reply_count", "reply_count"):
        try:
            return int(lead.get(key) or 0)
        except (TypeError, ValueError):
            continue
    return 0


def rejection_reason(lead: dict[str, Any], *, suppressed: set[str],
                     in_reengage: set[str], active_elsewhere: set[str]) -> str:
    """Why this lead is not eligible, or '' when it is a completed no-reply.

    The caller already asked Instantly for FILTER_VAL_COMPLETED_NO_REPLY.
    These checks refuse unsubscribes, negative replies, open threads,
    anyone already in the re-engage campaign, and anyone still active in
    another campaign. They do not send anything.
    """

    email = str(lead.get("email") or "").strip().lower()
    if "@" not in email:
        return "no email"
    try:
        status = int(lead.get("status"))
    except (TypeError, ValueError):
        status = None
    if status in _CLOSED_STATUSES:
        return "closed status"
    if status not in (None, _STATUS_COMPLETED):
        return "not completed"
    interest = _interest(lead)
    if interest in {str(item) for item in _NEGATIVE_INTEREST} or interest in {
        "negative", "not interested", "not_interested", "unsubscribed", "unsubscribe",
    }:
        return "negative or unsubscribed"
    if _reply_count(lead) > 0 or str(lead.get("timestamp_last_reply") or "").strip():
        return "active thread"
    if email in suppressed or email in in_reengage:
        return "already suppressed or re-engaged"
    if email in active_elsewhere:
        return "active in another campaign"
    campaign = str(lead.get("campaign") or lead.get("campaign_id") or "").strip()
    if campaign == TARGET_CAMPAIGN_ID:
        return "already in re-engage"
    if campaign == "e5f5046b-07e4-41ad-a395-79cf8766ccd0":
        return "trial offer left unchanged"
    return ""


def select_topup(leads: list[dict[str, Any]], *, suppressed: Optional[set[str]] = None,
                 in_reengage: Optional[set[str]] = None,
                 active_elsewhere: Optional[set[str]] = None) -> dict[str, Any]:
    """Split a completed/no-reply page into eligible leads and rejection counts."""

    suppressed = {item.strip().lower() for item in (suppressed or set())}
    in_reengage = {item.strip().lower() for item in (in_reengage or set())}
    active_elsewhere = {item.strip().lower() for item in (active_elsewhere or set())}
    eligible: list[dict[str, str]] = []
    seen: set[str] = set()
    rejected: dict[str, int] = {}
    for lead in leads:
        if not isinstance(lead, dict):
            continue
        reason = rejection_reason(
            lead, suppressed=suppressed, in_reengage=in_reengage, active_elsewhere=active_elsewhere,
        )
        email = str(lead.get("email") or "").strip().lower()
        if reason:
            rejected[reason] = rejected.get(reason, 0) + 1
            continue
        if email in seen:
            rejected["repeat"] = rejected.get("repeat", 0) + 1
            continue
        seen.add(email)
        eligible.append({
            "email": email,
            "first_name": str(lead.get("first_name") or "").strip(),
            "last_name": str(lead.get("last_name") or "").strip(),
            "company_name": str(lead.get("company_name") or lead.get("company") or "").strip(),
        })
    return {"eligible": eligible, "rejected": rejected}


def topup_enabled() -> bool:
    """Live adds stay off until this is set. The Monday job then still adds
    leads only. It does not activate a campaign or send a reply."""

    import os
    return (os.getenv("OUTBOUND_REENGAGE_TOPUP_ENABLED") or "").strip().lower() == "true"


def run_weekly_topup(*, post: Any = None, suppressed: Optional[set[str]] = None,
                     active_elsewhere: Optional[set[str]] = None,
                     now: Any = None) -> dict[str, Any]:
    """Monday top-up. Dry by default. Never calls reply, send, or activate.

    ``post`` is the Instantly POST helper. When the switch is off, this
    returns without calling it. A live run lists completed/no-reply leads,
    applies ``select_topup``, and adds only the eligible rows to the
    existing abby_reengage campaign.
    """

    from datetime import datetime
    from zoneinfo import ZoneInfo

    moment = now or datetime.now(ZoneInfo("America/Denver"))
    out: dict[str, Any] = {
        "dry_run": True, "eligible": 0, "added": 0, "rejected": {},
        "reason": "", "sources": list(TOPUP_SOURCE_CAMPAIGNS),
    }
    if getattr(moment, "weekday", lambda: 0)() != 0:
        out["reason"] = "not Monday"
        return out
    if not topup_enabled():
        out["reason"] = "OUTBOUND_REENGAGE_TOPUP_ENABLED is not true"
        return out
    if post is None:
        out["reason"] = "no Instantly client"
        return out
    import outbound_instantly_push as push

    key = push.api_key()
    if not key:
        out["reason"] = "Instantly API key missing"
        return out
    collected: list[dict[str, Any]] = []
    in_reengage: set[str] = set()
    elsewhere = {item.strip().lower() for item in (active_elsewhere or set())}
    for source in TOPUP_SOURCE_CAMPAIGNS:
        try:
            response = post(
                "https://api.instantly.ai/api/v2/leads/list",
                headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                json={"campaign": source, "filter": "FILTER_VAL_COMPLETED_NO_REPLY", "limit": 100},
                timeout=30,
            )
        except Exception as exc:  # noqa: BLE001
            out["reason"] = f"list failed: {type(exc).__name__}"
            return out
        status = int(getattr(response, "status_code", 0) or 0)
        data = response.json() if status == 200 and hasattr(response, "json") else {}
        if status >= 300 or not isinstance(data, dict):
            out["reason"] = f"list returned {status}"
            return out
        for item in data.get("items") or []:
            if isinstance(item, dict):
                item = dict(item)
                item.setdefault("campaign", source)
                collected.append(item)
    try:
        response = post(
            "https://api.instantly.ai/api/v2/leads/list",
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            json={"campaign": TARGET_CAMPAIGN_ID, "limit": 100},
            timeout=30,
        )
        data = response.json() if int(getattr(response, "status_code", 0) or 0) == 200 else {}
        for item in (data or {}).get("items") or []:
            email = str((item or {}).get("email") or "").strip().lower()
            if email:
                in_reengage.add(email)
    except Exception:  # noqa: BLE001
        out["reason"] = "could not read the re-engage campaign"
        return out
    chosen = select_topup(
        collected, suppressed=suppressed, in_reengage=in_reengage, active_elsewhere=elsewhere,
    )
    out["rejected"] = chosen["rejected"]
    out["eligible"] = len(chosen["eligible"])
    if not chosen["eligible"]:
        out["dry_run"] = False
        out["reason"] = "nobody eligible"
        return out
    try:
        response = post(
            push.INSTANTLY_ADD_LEADS_URL,
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            json={
                "campaign_id": TARGET_CAMPAIGN_ID,
                "leads": chosen["eligible"],
                "skip_if_in_workspace": False,
                "verify_leads_on_import": False,
            },
            timeout=60,
        )
    except Exception as exc:  # noqa: BLE001
        out["reason"] = f"add failed: {type(exc).__name__}"
        return out
    status = int(getattr(response, "status_code", 0) or 0)
    if status >= 300:
        out["reason"] = f"add returned {status}"
        return out
    body = response.json() if hasattr(response, "json") else {}
    out["dry_run"] = False
    out["added"] = len((body or {}).get("created_leads") or []) or len(chosen["eligible"])
    out["reason"] = "added to the existing re-engage campaign; no reply and no activation"
    return out
