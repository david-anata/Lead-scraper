"""Re-engagement campaign: every past lead who finished a sequence without
replying hears about the new offer (David approved 2026-10-26: his copy,
300 a day, same 18 inboxes, same schedule as the live campaign).

The original create copied leads. The weekly top-up does not. It moves the
existing Instantly record into abby_reengage so a person has one lead, and
the reply history stays on that record. Someone who replied negatively,
unsubscribed, or bounced is never moved or added.
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


MOVE_URL = "https://api.instantly.ai/api/v2/leads/move"
LIST_URL = "https://api.instantly.ai/api/v2/leads/list"


def move_body(lead_ids: list[str], target: str) -> dict[str, Any]:
    """Move these lead records. copy_leads false keeps one record and its history."""

    return {
        "ids": list(lead_ids),
        "to_campaign_id": target,
        "copy_leads": False,
        "check_duplicates": True,
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
    if status == -1:
        return "bounced"
    if status == -2:
        return "unsubscribed"
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
    if "country_code" in lead or "country" in lead:
        code = str(lead.get("country_code") if lead.get("country_code") is not None else lead.get("country") or "").strip().upper()
        if code == "UK":
            code = "GB"
        if code != "US":
            return "not US"
    bounced = lead.get("email_bounce_count") or lead.get("bounce_count")
    try:
        if int(bounced or 0) > 0:
            return "bounced"
    except (TypeError, ValueError):
        pass
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
        import outbound_contacts as contacts

        title = _lead_title(lead)
        quality = contacts.lead_quality_reason(
            email=email,
            first_name=str(lead.get("first_name") or "").strip(),
            title=title,
            require_title=False,
        )
        if quality:
            rejected[quality] = rejected.get(quality, 0) + 1
            continue
        eligible.append({
            "id": str(lead.get("id") or lead.get("lead_id") or "").strip(),
            "email": email,
            "first_name": str(lead.get("first_name") or "").strip(),
            "last_name": str(lead.get("last_name") or "").strip(),
            "company_name": str(lead.get("company_name") or lead.get("company") or "").strip(),
            "title": title,
            "website": str(lead.get("website") or lead.get("company_domain") or "").strip(),
        })
    return {"eligible": eligible, "rejected": rejected}


def _lead_title(lead: dict[str, Any]) -> str:
    custom = lead.get("custom_variables") or lead.get("payload") or {}
    if not isinstance(custom, dict):
        custom = {}
    nested = custom.get("custom_variables") if isinstance(custom.get("custom_variables"), dict) else {}
    for value in (
        lead.get("title"),
        lead.get("job_title"),
        custom.get("contact_title"),
        custom.get("title"),
        nested.get("contact_title"),
    ):
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _negative_record(lead: dict[str, Any]) -> str:
    """'' when this workspace record can still be moved."""

    try:
        status = int(lead.get("status"))
    except (TypeError, ValueError):
        status = None
    if status == -1:
        return "bounced"
    if status == -2:
        return "unsubscribed"
    if status in _CLOSED_STATUSES:
        return "closed status"
    interest = _interest(lead)
    if interest in {str(item) for item in _NEGATIVE_INTEREST} or interest in {
        "negative", "not interested", "not_interested", "unsubscribed", "unsubscribe", "bounced",
    }:
        return "negative or unsubscribed"
    return ""


def plan_reengage_actions(
    eligible: list[dict[str, Any]],
    records_by_email: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    """Move one existing record per person. Add only when no record exists.

    A negative, unsubscribed, or bounced record anywhere in the workspace
    blocks the person. A record already in the re-engage campaign blocks
    them too. Moving uses the existing id so Instantly does not create a
    second lead.
    """

    moves: list[dict[str, str]] = []
    adds: list[dict[str, str]] = []
    rejected: dict[str, int] = {}
    for lead in eligible:
        email = str(lead.get("email") or "").strip().lower()
        records = [item for item in (records_by_email.get(email) or []) if isinstance(item, dict)]
        if not records and lead.get("id"):
            records = [lead]
        blocked = ""
        for record in records:
            blocked = _negative_record(record)
            if blocked:
                break
            campaign = str(record.get("campaign") or record.get("campaign_id") or "").strip()
            if campaign == TARGET_CAMPAIGN_ID:
                blocked = "already in re-engage"
                break
        if blocked:
            rejected[blocked] = rejected.get(blocked, 0) + 1
            continue
        chosen_id = str(lead.get("id") or "").strip()
        if not chosen_id:
            for record in records:
                chosen_id = str(record.get("id") or record.get("lead_id") or "").strip()
                if chosen_id:
                    break
        if chosen_id:
            moves.append({"id": chosen_id, "email": email})
            continue
        if records:
            rejected["existing record has no id"] = rejected.get("existing record has no id", 0) + 1
            continue
        adds.append({
            "email": email,
            "first_name": str(lead.get("first_name") or "").strip(),
            "last_name": str(lead.get("last_name") or "").strip(),
            "company_name": str(lead.get("company_name") or "").strip(),
        })
    return {"moves": moves, "adds": adds, "rejected": rejected}


def topup_enabled() -> bool:
    """Live adds stay off until this is set. The Monday job then still adds
    leads only. It does not activate a campaign or send a reply."""

    import os
    return (os.getenv("OUTBOUND_REENGAGE_TOPUP_ENABLED") or "").strip().lower() == "true"


def _list_leads(post: Any, key: str, body: dict[str, Any], *, pages: int = 30) -> tuple[list[dict[str, Any]], str]:
    """One Instantly list, following next_starting_after. ('', reason) on failure."""

    found: list[dict[str, Any]] = []
    payload = dict(body)
    payload.setdefault("limit", 100)
    for _ in range(pages):
        try:
            response = post(
                LIST_URL,
                headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                json=payload,
                timeout=30,
            )
        except Exception as exc:  # noqa: BLE001
            return [], f"list failed: {type(exc).__name__}"
        status = int(getattr(response, "status_code", 0) or 0)
        data = response.json() if status == 200 and hasattr(response, "json") else {}
        if status >= 300 or not isinstance(data, dict):
            return [], f"list returned {status}"
        for item in data.get("items") or []:
            if isinstance(item, dict):
                found.append(dict(item))
        nxt = str(data.get("next_starting_after") or "").strip()
        if not nxt:
            return found, ""
        payload["starting_after"] = nxt
    return found, ""


def _email_domain(email: str) -> str:
    host = email.split("@", 1)[-1].strip().lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def run_weekly_topup(*, post: Any = None, suppressed: Optional[set[str]] = None,
                     active_elsewhere: Optional[set[str]] = None,
                     now: Any = None,
                     lookup_countries: Any = None) -> dict[str, Any]:
    """Monday top-up. Dry by default. Never calls reply, send, or activate.

    ``post`` is the Instantly POST helper. When the switch is off, this
    returns without calling it. A live run lists completed/no-reply leads,
    drops role inboxes, missing names, weak titles, and non-US brands, then
    moves an existing record into abby_reengage. It adds a lead only when
    that person has no Instantly record, and then with skip_if_in_workspace
    true so a second record is not created.
    """

    from datetime import datetime
    from zoneinfo import ZoneInfo

    moment = now or datetime.now(ZoneInfo("America/Denver"))
    out: dict[str, Any] = {
        "dry_run": True, "eligible": 0, "added": 0, "moved": 0, "rejected": {},
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
    records_by_email: dict[str, list[dict[str, Any]]] = {}
    elsewhere = {item.strip().lower() for item in (active_elsewhere or set())}
    for source in TOPUP_SOURCE_CAMPAIGNS:
        page, reason = _list_leads(
            post, key, {"campaign": source, "filter": "FILTER_VAL_COMPLETED_NO_REPLY"},
        )
        if reason:
            out["reason"] = reason
            return out
        for item in page:
            item.setdefault("campaign", source)
            collected.append(item)
            email = str(item.get("email") or "").strip().lower()
            if email:
                records_by_email.setdefault(email, []).append(item)
    page, reason = _list_leads(post, key, {"campaign": TARGET_CAMPAIGN_ID})
    if reason:
        out["reason"] = "could not read the re-engage campaign"
        return out
    for item in page:
        item.setdefault("campaign", TARGET_CAMPAIGN_ID)
        email = str(item.get("email") or "").strip().lower()
        if email:
            in_reengage.add(email)
            records_by_email.setdefault(email, []).append(item)
    domains = sorted({
        _email_domain(str(item.get("email") or ""))
        for item in collected if "@" in str(item.get("email") or "")
    })
    countries: dict[str, str] = {}
    if lookup_countries is None:
        import os

        import outbound_pipeline as pipeline

        store_key = (os.getenv("STORELEADS_API_KEY") or "").strip()
        if not store_key:
            out["dry_run"] = False
            out["reason"] = "StoreLeads country could not be checked"
            return out
        import outbound_contacts as contacts

        found = pipeline.lookup_domains(store_key, domains)
        countries = {}
        for name, row in found.items():
            code = str((row or {}).get("country_code") or "").strip().upper()
            key = contacts.company_domain(name) or name
            countries[key] = code
    else:
        import outbound_contacts as contacts

        countries = {}
        for name, code in (lookup_countries(domains) or {}).items():
            key = contacts.company_domain(str(name)) or str(name).strip().lower()
            countries[key] = str(code or "").strip().upper()
    if domains and not countries:
        out["dry_run"] = False
        out["reason"] = "StoreLeads returned no country"
        return out
    import outbound_contacts as contacts

    for item in collected:
        domain = contacts.company_domain(_email_domain(str(item.get("email") or "")))
        item["country_code"] = countries.get(domain, "")
    chosen = select_topup(
        collected, suppressed=suppressed, in_reengage=in_reengage, active_elsewhere=elsewhere,
    )
    plan = plan_reengage_actions(chosen["eligible"], records_by_email)
    rejected = dict(chosen["rejected"])
    for reason, count in plan["rejected"].items():
        rejected[reason] = rejected.get(reason, 0) + count
    out["rejected"] = rejected
    out["eligible"] = len(plan["moves"]) + len(plan["adds"])
    if not plan["moves"] and not plan["adds"]:
        out["dry_run"] = False
        out["reason"] = "nobody eligible"
        return out
    moved = 0
    for start in range(0, len(plan["moves"]), 100):
        chunk = plan["moves"][start:start + 100]
        try:
            response = post(
                MOVE_URL,
                headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                json=move_body([item["id"] for item in chunk], TARGET_CAMPAIGN_ID),
                timeout=60,
            )
        except Exception as exc:  # noqa: BLE001
            out["reason"] = f"move failed: {type(exc).__name__}"
            out["moved"] = moved
            return out
        status = int(getattr(response, "status_code", 0) or 0)
        if status >= 300:
            out["reason"] = f"move returned {status}"
            out["moved"] = moved
            return out
        moved += len(chunk)
    added = 0
    if plan["adds"]:
        try:
            response = post(
                push.INSTANTLY_ADD_LEADS_URL,
                headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                json={
                    "campaign_id": TARGET_CAMPAIGN_ID,
                    "leads": plan["adds"],
                    "skip_if_in_workspace": True,
                    "verify_leads_on_import": False,
                },
                timeout=60,
            )
        except Exception as exc:  # noqa: BLE001
            out["reason"] = f"add failed: {type(exc).__name__}"
            out["moved"] = moved
            return out
        status = int(getattr(response, "status_code", 0) or 0)
        if status >= 300:
            out["reason"] = f"add returned {status}"
            out["moved"] = moved
            return out
        body = response.json() if hasattr(response, "json") else {}
        added = len((body or {}).get("created_leads") or []) or len(plan["adds"])
    out["dry_run"] = False
    out["moved"] = moved
    out["added"] = added
    out["reason"] = "moved existing leads into the re-engage campaign; no reply and no activation"
    return out
