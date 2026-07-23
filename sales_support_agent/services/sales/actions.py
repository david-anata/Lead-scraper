"""Confidence-tiered action suggestions for the Sales deal detail and cleanup queue.

Confidence levels
-----------------
mid  — writeable; shown as checkable items in the Cleanup Queue and as approve
       cards on the deal detail page.  Executes a HubSpot write on approval.
low  — informational; shown as nudges with an optional "Fix in HubSpot →" link.
       Never triggers a write.

Severity (display only — does not affect write behaviour)
---------------------------------------------------------
critical — red:    blocking deal progress or data is badly wrong
warning  — amber:  needs attention soon
hygiene  — blue:   nice-to-fix, won't block a deal
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

from sales_support_agent.models.entities import HubSpotDeal, MailboxSignal


@dataclass
class ContactInfo:
    """Minimal contact data passed to compute_pending_actions."""
    contact_id: str
    email: str
    hubspot_url: str = ""


@dataclass
class SalesAction:
    action_id: str
    action_type: str        # "update_deal" | "update_contact" | "create_note" | "flag"
    confidence: str         # "mid" | "low"
    severity: str           # "critical" | "warning" | "hygiene"
    category: str           # "close_date" | "amount" | "staleness" | "stage" | "hygiene" | "review"
    label: str
    description: str
    hubspot_object_type: str   # "deals" | "contacts"
    hubspot_object_id: str
    properties: dict[str, str] = field(default_factory=dict)
    note_body: str = ""        # content for create_note actions
    link_url: str = ""         # low-confidence / flag: "Fix in HubSpot →" destination


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _aware(dt: datetime) -> datetime:
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _days_ago(dt: Optional[datetime], as_of: datetime) -> Optional[int]:
    if dt is None:
        return None
    return (as_of - _aware(dt)).days


def _fmt_date(dt: Optional[datetime]) -> str:
    if dt is None:
        return "Never"
    value = _aware(dt)
    return f"{value.strftime('%b')} {value.day}, {value.year}"


def _fmt_long_date(dt: datetime) -> str:
    value = _aware(dt)
    return f"{value.strftime('%B')} {value.day}, {value.year}"


def _deal_portal_url(deal_id: str, portal_id: str) -> str:
    if portal_id:
        return f"https://app.hubspot.com/contacts/{portal_id}/deal/{deal_id}"
    return ""


def _build_review_note(
    deal: HubSpotDeal,
    as_of: datetime,
    issues: list[str],
    recommended: str = "",
) -> str:
    """Format a Sales Director review note body for writing to HubSpot."""
    stage = deal.deal_stage_label or deal.deal_stage or "Unknown"
    amount = f"${deal.amount_cents // 100:,}" if (deal.amount_cents or 0) > 0 else "Not set"
    close = _fmt_date(deal.close_date)
    if deal.close_date and _aware(deal.close_date) < as_of:
        overdue_days = (as_of - _aware(deal.close_date)).days
        close = f"{close} (overdue {overdue_days}d)"

    last_inbound = _fmt_date(deal.last_inbound_at)
    last_outbound = _fmt_date(deal.last_outbound_at)
    last_touch = _fmt_date(deal.last_meaningful_touch_at)

    lines = [
        f"📋 Sales Director Review — {_fmt_long_date(as_of)}",
        "",
        f"Stage: {stage}",
        f"Amount: {amount}",
        f"Close date: {close}",
        f"Last inbound: {last_inbound}",
        f"Last outbound: {last_outbound}",
        f"Last meaningful touch: {last_touch}",
    ]
    if deal.owner_email:
        lines.append(f"Owner: {deal.owner_email}")

    if issues:
        lines.append("")
        lines.append("Issues flagged:")
        for issue in issues:
            lines.append(f"  • {issue}")

    if recommended:
        lines.append("")
        lines.append(f"Recommended action: {recommended}")

    lines.append("")
    lines.append("—— Logged by Anata Sales Director ——")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def compute_pending_actions(
    deal: HubSpotDeal,
    recent_signals: list[MailboxSignal],
    *,
    line_item_total_cents: int = 0,
    contacts: list[ContactInfo] | None = None,
    portal_id: str = "",
    as_of: datetime | None = None,
) -> list[SalesAction]:
    """Return all suggested actions for a deal, tiered by confidence and severity."""
    if deal.is_closed:
        return []

    as_of = as_of or datetime.now(timezone.utc)
    actions: list[SalesAction] = []
    deal_url = _deal_portal_url(deal.hubspot_deal_id, portal_id)
    issues_for_note: list[str] = []

    # ------------------------------------------------------------------
    # CATEGORY: close_date
    # ------------------------------------------------------------------
    close = deal.close_date
    if close is not None and _aware(close) < as_of:
        overdue_days = (as_of - _aware(close)).days
        new_date = as_of + timedelta(days=30)
        new_ts = str(int(new_date.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000))
        actions.append(SalesAction(
            action_id=f"{deal.hubspot_deal_id}:push_close_date",
            action_type="update_deal",
            confidence="mid",
            severity="warning",
            category="close_date",
            label=f"Push close date → {_fmt_date(new_date)}",
            description=f"Close date was {_fmt_date(close)} — {overdue_days} days overdue. "
                        "Extend 30 days so this deal stays in the active pipeline.",
            hubspot_object_type="deals",
            hubspot_object_id=deal.hubspot_deal_id,
            properties={"closedate": new_ts},
        ))
        issues_for_note.append(f"Close date overdue by {overdue_days} days ({_fmt_date(close)})")

    elif deal.close_date is None:
        new_date = as_of + timedelta(days=30)
        new_ts = str(int(new_date.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000))
        actions.append(SalesAction(
            action_id=f"{deal.hubspot_deal_id}:set_close_date",
            action_type="update_deal",
            confidence="mid",
            severity="warning",
            category="close_date",
            label=f"Set close date → {_fmt_date(new_date)}",
            description="No close date set. Adding one keeps this deal correctly ordered on the board "
                        "and visible in forecasts.",
            hubspot_object_type="deals",
            hubspot_object_id=deal.hubspot_deal_id,
            properties={"closedate": new_ts},
        ))
        issues_for_note.append("No close date set")

    # ------------------------------------------------------------------
    # CATEGORY: amount
    # ------------------------------------------------------------------
    amount_cents = deal.amount_cents or 0

    if amount_cents <= 0 and line_item_total_cents > 0:
        amount_str = f"{line_item_total_cents / 100:.2f}"
        actions.append(SalesAction(
            action_id=f"{deal.hubspot_deal_id}:sync_amount",
            action_type="update_deal",
            confidence="mid",
            severity="warning",
            category="amount",
            label=f"Sync amount from line items (${line_item_total_cents // 100:,})",
            description="Deal amount is $0 but associated line items total a real value. "
                        "Sync it so forecasts and the board show the correct number.",
            hubspot_object_type="deals",
            hubspot_object_id=deal.hubspot_deal_id,
            properties={"amount": amount_str},
        ))
        issues_for_note.append(f"Amount unset — line items total ${line_item_total_cents // 100:,}")

    elif amount_cents <= 0 and line_item_total_cents == 0:
        actions.append(SalesAction(
            action_id=f"{deal.hubspot_deal_id}:missing_amount",
            action_type="flag",
            confidence="mid",
            severity="critical",
            category="amount",
            label="Deal has no amount and no line items",
            description="Add pricing in HubSpot — this deal is invisible in revenue forecasts and "
                        "can't be prioritised correctly without a value.",
            hubspot_object_type="deals",
            hubspot_object_id=deal.hubspot_deal_id,
            link_url=deal_url,
        ))
        issues_for_note.append("No amount set and no line items exist")

    # ------------------------------------------------------------------
    # CATEGORY: staleness (uses managed mirror fields + MailboxSignals)
    # ------------------------------------------------------------------
    touch_days = _days_ago(deal.last_meaningful_touch_at, as_of)
    inbound_days = _days_ago(deal.last_inbound_at, as_of)
    outbound_days = _days_ago(deal.last_outbound_at, as_of)

    # Fall back to MailboxSignal dates when managed fields are empty
    if touch_days is None and recent_signals:
        latest_signal = max(recent_signals, key=lambda s: s.received_at)
        touch_days = _days_ago(latest_signal.received_at, as_of)

    deal_age_days: Optional[int] = None
    if deal.created_at:
        deal_age_days = (as_of - _aware(deal.created_at)).days

    never_touched = (touch_days is None and not recent_signals)
    old_enough = (deal_age_days is not None and deal_age_days >= 7)

    if never_touched and old_enough:
        age_str = f"{deal_age_days} days" if deal_age_days else "unknown age"
        note = _build_review_note(
            deal, as_of,
            issues=["No inbound or outbound communication recorded since deal creation"],
            recommended="Send first outreach immediately or close as lost if not progressing.",
        )
        actions.append(SalesAction(
            action_id=f"{deal.hubspot_deal_id}:never_touched",
            action_type="create_note",
            confidence="mid",
            severity="critical",
            category="staleness",
            label=f"Never touched — log Sales Director review ({age_str} old)",
            description="No communication has ever been recorded for this deal. "
                        "Logs a timestamped review note in HubSpot as an audit trail.",
            hubspot_object_type="deals",
            hubspot_object_id=deal.hubspot_deal_id,
            note_body=note,
        ))
        issues_for_note.append(f"Never touched — deal is {age_str} old")

    elif touch_days is not None and touch_days >= 30:
        note = _build_review_note(
            deal, as_of,
            issues=[f"No meaningful communication for {touch_days} days"],
            recommended="Immediate follow-up required — call or email the contact today.",
        )
        actions.append(SalesAction(
            action_id=f"{deal.hubspot_deal_id}:stale_30d",
            action_type="create_note",
            confidence="mid",
            severity="critical",
            category="staleness",
            label=f"Stale {touch_days}d — log review note to HubSpot",
            description=f"Last meaningful touch was {touch_days} days ago. "
                        "Prospect may be going cold. Logs a review note as an audit trail.",
            hubspot_object_type="deals",
            hubspot_object_id=deal.hubspot_deal_id,
            note_body=note,
        ))
        issues_for_note.append(f"No meaningful touch for {touch_days} days")

    elif touch_days is not None and touch_days >= 14:
        note = _build_review_note(
            deal, as_of,
            issues=[f"No meaningful communication for {touch_days} days"],
            recommended="Schedule follow-up before this deal goes cold.",
        )
        actions.append(SalesAction(
            action_id=f"{deal.hubspot_deal_id}:stale_14d",
            action_type="create_note",
            confidence="mid",
            severity="warning",
            category="staleness",
            label=f"Going stale ({touch_days}d) — log review note",
            description=f"Last touch {touch_days} days ago. Log a review note "
                        "and schedule a follow-up this week.",
            hubspot_object_type="deals",
            hubspot_object_id=deal.hubspot_deal_id,
            note_body=note,
        ))
        issues_for_note.append(f"No meaningful touch for {touch_days} days")

    # No outbound ever (and not newly created)
    if outbound_days is None and old_enough and not never_touched:
        actions.append(SalesAction(
            action_id=f"{deal.hubspot_deal_id}:no_outbound",
            action_type="flag",
            confidence="mid",
            severity="critical",
            category="staleness",
            label="No outbound recorded — has this deal been contacted?",
            description="No outbound activity is on record. Verify the rep has made contact "
                        "and log outreach in HubSpot.",
            hubspot_object_type="deals",
            hubspot_object_id=deal.hubspot_deal_id,
            link_url=deal_url,
        ))
        issues_for_note.append("No outbound activity recorded")

    # ------------------------------------------------------------------
    # CATEGORY: stage — recent inbound suggests stage advance
    # ------------------------------------------------------------------
    signal_cutoff = as_of - timedelta(days=14)
    recent_inbound = [
        s for s in recent_signals
        if _aware(s.received_at) > signal_cutoff
        and getattr(s, "signal_type", "") in ("inbound_reply", "email_received", "")
    ]

    if recent_inbound:
        latest = max(recent_inbound, key=lambda s: s.received_at)
        late_stages = {"contractsent", "closedwon", "closedlost"}
        if (deal.deal_stage or "") not in late_stages:
            next_stage = _try_get_next_stage(
                getattr(deal, "pipeline", "") or "",
                deal.deal_stage or "",
            )
            if next_stage:
                next_id, next_label = next_stage
                actions.append(SalesAction(
                    action_id=f"{deal.hubspot_deal_id}:stage_move",
                    action_type="update_deal",
                    confidence="mid",
                    severity="warning",
                    category="stage",
                    label=f"Advance stage → {next_label}",
                    description=(
                        f"Prospect replied {_fmt_date(latest.received_at)}. "
                        f"Move to '{next_label}' to reflect deal progress."
                    ),
                    hubspot_object_type="deals",
                    hubspot_object_id=deal.hubspot_deal_id,
                    properties={"dealstage": next_id},
                ))
            else:
                actions.append(SalesAction(
                    action_id=f"{deal.hubspot_deal_id}:replied_note",
                    action_type="flag",
                    confidence="low",
                    severity="warning",
                    category="stage",
                    label=f"Prospect replied {_fmt_date(latest.received_at)} — update stage?",
                    description=(getattr(latest, "subject", "") or "(no subject)")[:100],
                    hubspot_object_type="deals",
                    hubspot_object_id=deal.hubspot_deal_id,
                    link_url=deal_url,
                ))

    # ------------------------------------------------------------------
    # CATEGORY: hygiene (low-confidence flags)
    # ------------------------------------------------------------------
    if contacts is not None and len(contacts) == 0:
        actions.append(SalesAction(
            action_id=f"{deal.hubspot_deal_id}:no_contacts",
            action_type="flag",
            confidence="low",
            severity="hygiene",
            category="hygiene",
            label="No contacts linked to this deal",
            description="Add the buyer contact in HubSpot so you can track who to follow up with "
                        "and log email history.",
            hubspot_object_type="deals",
            hubspot_object_id=deal.hubspot_deal_id,
            link_url=deal_url,
        ))
        issues_for_note.append("No contacts linked")

    if not (getattr(deal, "hubspot_company_id", "") or "").strip():
        actions.append(SalesAction(
            action_id=f"{deal.hubspot_deal_id}:no_company",
            action_type="flag",
            confidence="low",
            severity="hygiene",
            category="hygiene",
            label="No company linked",
            description="Associate a company in HubSpot to match inbound mail and get "
                        "company-level context on the deal.",
            hubspot_object_type="deals",
            hubspot_object_id=deal.hubspot_deal_id,
            link_url=deal_url,
        ))

    for ci in (contacts or []):
        if not (ci.email or "").strip():
            hs_url = ci.hubspot_url or (
                f"https://app.hubspot.com/contacts/{portal_id}/contact/{ci.contact_id}"
                if portal_id else ""
            )
            actions.append(SalesAction(
                action_id=f"{deal.hubspot_deal_id}:contact_no_email_{ci.contact_id}",
                action_type="flag",
                confidence="low",
                severity="hygiene",
                category="hygiene",
                label="Contact has no email address",
                description="Add an email address for this contact in HubSpot so you can track "
                            "and send follow-up directly.",
                hubspot_object_type="contacts",
                hubspot_object_id=ci.contact_id,
                link_url=hs_url,
            ))

    # ------------------------------------------------------------------
    # CATEGORY: review — offer a Sales Director note for any deal with issues
    # ------------------------------------------------------------------
    # Only offer if there are meaningful issues to document and no staleness note
    # already being written (which includes a full note body).
    staleness_note_written = any(
        a.category == "staleness" and a.action_type == "create_note"
        for a in actions
    )
    mid_issue_count = sum(1 for a in actions if a.confidence == "mid" and a.action_type != "flag")

    if issues_for_note and not staleness_note_written and mid_issue_count >= 1:
        note = _build_review_note(
            deal, as_of,
            issues=issues_for_note,
            recommended="Review issues above and action them in HubSpot.",
        )
        actions.append(SalesAction(
            action_id=f"{deal.hubspot_deal_id}:review_note",
            action_type="create_note",
            confidence="mid",
            severity="hygiene",
            category="review",
            label="Log Sales Director review note to HubSpot",
            description="Creates a timestamped audit note in HubSpot summarising the issues "
                        "flagged in this cleanup run.",
            hubspot_object_type="deals",
            hubspot_object_id=deal.hubspot_deal_id,
            note_body=note,
        ))

    return actions


def _try_get_next_stage(pipeline_id: str, stage_id: str):
    try:
        from sales_support_agent.services.sales.pipeline import get_next_stage
        return get_next_stage(pipeline_id, stage_id)
    except Exception:
        return None
