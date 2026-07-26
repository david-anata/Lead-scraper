"""Is it safe to switch the old ClickUp bill list off yet?

The bill schedule still arrives from ClickUp, typed in by hand and often stale.
Retiring it is the goal, but flipping the switch blind would quietly drop bills
out of the cash forecast, and a bill you cannot see is a bill you do not pay. So
every live ClickUp obligation has to be accounted for first, one of four ways:

* covered   -> a recurring schedule in this app already pays that vendor on that cadence
* settled   -> it is in the past and the payment was found, so it is history
* undated   -> no date, cancelled, or already sitting open in the ledger: nothing to lose
* uncovered -> nothing here would replace it, so the money would silently vanish

Only ``uncovered`` blocks the switch. The analysis reuses the importer's own
grouping and bank checks, so what this reports is exactly what the importer would
act on. Nothing is written and no external system is touched: this only reads.
"""

from __future__ import annotations

import html
import re
from datetime import date
from typing import Any, Optional

from sales_support_agent.services.cashflow.schedule_import import (
    _CADENCE_TO_FREQUENCY,
    _as_date,
    _load_clickup_obligations,
    _series_name,
    propose_overdue_disposition,
    propose_schedules,
)

# A ClickUp row in one of these states is already history in our own records, so
# no bank lookup is needed to prove it.
_SETTLED_STATUSES = {"paid", "posted", "matched", "settled"}
# Shortest name fragment allowed to match by containment. Below this, "ad" would
# match "Adobe" and coverage would be claimed where there is none.
_MIN_FRAGMENT = 4


def _dollars(cents: int) -> str:
    """Whole dollars for a sentence a person reads out loud."""
    sign = "-" if cents < 0 else ""
    return f"{sign}${abs(int(cents)) / 100:,.0f}"


def _bills(count: int) -> str:
    return "bill" if count == 1 else "bills"


def _norm(value: Any) -> str:
    cleaned = re.sub(r"[^a-z0-9 ]+", " ", str(value or "").lower())
    return re.sub(r"\s+", " ", cleaned).strip()


def _identity_keys(*values: Any) -> set[str]:
    """Comparable names for one bill: the series name wins over the occurrence."""
    keys = set()
    for value in values:
        for candidate in (_norm(_series_name(value)), _norm(value)):
            if len(candidate) >= 3:
                keys.add(candidate)
    return keys


def _same_bill(left: set[str], right: set[str]) -> bool:
    for one in left:
        for other in right:
            if one == other:
                return True
            if len(one) >= _MIN_FRAGMENT and len(other) >= _MIN_FRAGMENT and (
                one in other or other in one
            ):
                return True
    return False


def _active_templates() -> list[dict[str, Any]]:
    """Native recurring schedules that would keep running after the switch."""
    from sales_support_agent.services.cashflow.obligations import list_recurring_templates

    templates = []
    for template in list_recurring_templates(active_only=True):
        if str(template.get("id") or "").startswith("clickup-tmpl-"):
            continue  # the old sync's own templates cannot be the replacement for it
        frequency = str(template.get("frequency") or "").lower()
        templates.append({
            "name": str(template.get("name") or template.get("vendor_or_customer") or "Schedule"),
            "frequency": _CADENCE_TO_FREQUENCY.get(frequency, frequency),
            "event_type": str(template.get("event_type") or "outflow"),
            "keys": _identity_keys(template.get("name"), template.get("vendor_or_customer")),
        })
    return templates


def _covering_template(
    templates: list[dict[str, Any]],
    *,
    name: Any,
    vendor: Any,
    frequency: str,
    event_type: str,
) -> Optional[dict[str, Any]]:
    """The schedule here that already carries this bill, if there is one.

    An unknown ClickUp cadence matches any cadence: a bill with no stated rhythm
    is covered by whatever schedule already pays that vendor.
    """
    keys = _identity_keys(name, vendor)
    if not keys:
        return None
    wanted = _CADENCE_TO_FREQUENCY.get(frequency, frequency)
    for template in templates:
        if template["event_type"] != event_type:
            continue
        if wanted and template["frequency"] and template["frequency"] != wanted:
            continue
        if _same_bill(keys, template["keys"]):
            return template
    return None


def _row_cadence(row: dict[str, Any]) -> str:
    rule = str(row.get("recurring_rule") or "").lower()
    return _CADENCE_TO_FREQUENCY.get(rule, "")


def _status(row: dict[str, Any]) -> str:
    return str(row.get("status") or "").lower()


def _flag_is_set() -> bool:
    """Whether the ClickUp finance sync is already switched off in settings."""
    try:
        from sales_support_agent.config import load_settings

        return bool(getattr(load_settings(), "disable_clickup_finance_sync", False))
    except Exception:
        return False


def _summary(*, counts: dict[str, int], at_risk_cents: int, already_off: bool) -> str:
    if counts["total"] == 0:
        if already_off:
            return "The old bill list is switched off already and has nothing live left in it."
        return "Your old bill list has nothing live in it, so switching it off changes nothing."

    uncovered = counts["uncovered"]
    if not uncovered:
        accounted = counts["covered"] + counts["settled"] + counts["undated"]
        if already_off:
            return (
                "The old bill list is switched off already, and everything that was in it "
                "is either covered by a schedule here or finished with."
            )
        return (
            f"All {accounted} {_bills(accounted)} from your old list are either covered by a "
            "schedule here or finished with, so switching it off costs you nothing."
        )

    money = _dollars(at_risk_cents)
    if already_off:
        return (
            f"The old bill list is switched off already and {uncovered} {_bills(uncovered)} "
            f"worth {money} are missing from your forecast. Set each one up here to get them back."
        )
    return (
        f"{uncovered} {_bills(uncovered)} worth {money} would disappear from your forecast if "
        "you switched the old list off now. Set each one up here first."
    )


def assess_cutover_readiness(*, as_of: Optional[date] = None) -> dict[str, Any]:
    """Every live ClickUp obligation, sorted into what would happen if it went away.

    Recurring series count as one obligation, not one per occurrence, because the
    operator needs one schedule to replace a series, and repeating the same bill
    six times on screen would exaggerate what is at stake.
    """
    as_of = as_of or date.today()
    rows = _load_clickup_obligations()
    proposals = propose_schedules(as_of=as_of)
    templates = _active_templates()

    by_id = {str(row["id"]): row for row in rows}
    series_ids = {source_id for proposal in proposals for source_id in proposal["source_ids"]}
    paid_by_bank = {
        item["id"]: item
        for item in (propose_overdue_disposition(as_of=as_of) if rows else [])
        if item["looks_paid"]
    }

    covered: list[dict[str, Any]] = []
    settled: list[dict[str, Any]] = []
    undated: list[dict[str, Any]] = []
    uncovered: list[dict[str, Any]] = []

    for proposal in proposals:
        members = [by_id[source_id] for source_id in proposal["source_ids"] if source_id in by_id]
        item = {
            "name": str(proposal["name"]),
            "vendor": str(proposal["vendor"]),
            "amount_cents": int(proposal["amount_cents"]),
            "due_date": str(proposal["next_due"]),
            "repeats": str(proposal["frequency"]),
            "occurrences": len(proposal["source_ids"]),
        }
        template = _covering_template(
            templates, name=proposal["name"], vendor=proposal["vendor"],
            frequency=proposal["frequency"], event_type="outflow",
        )
        if template:
            covered.append({
                **item,
                "covered_by": template["name"],
                "reason": f"already on your {template['frequency'] or 'recurring'} schedule here",
            })
        elif members and all(_status(row) == "cancelled" for row in members):
            undated.append({**item, "reason": "cancelled, so it is not in your forecast"})
        else:
            uncovered.append({
                **item,
                "reason": "it keeps coming back and nothing here would carry it on",
            })

    for row in rows:
        row_id = str(row["id"])
        if row_id in series_ids:
            continue  # counted once as its series above
        due = _as_date(row.get("due_date"))
        event_type = str(row.get("event_type") or "outflow")
        cadence = _row_cadence(row)
        item = {
            "name": str(row.get("name") or row.get("vendor_or_customer") or "Bill"),
            "vendor": str(row.get("vendor_or_customer") or row.get("name") or ""),
            "amount_cents": int(row.get("amount_cents") or 0),
            "due_date": due.isoformat() if due else "",
            "repeats": cadence,
            "occurrences": 1,
        }
        status = _status(row)
        match = paid_by_bank.get(row_id)

        if due is None:
            undated.append({**item, "reason": "it has no date, so it was never in your forecast"})
        elif status == "cancelled":
            undated.append({**item, "reason": "cancelled, so it is not in your forecast"})
        elif status in _SETTLED_STATUSES:
            settled.append({**item, "reason": "your own records already show this one paid"})
        elif match:
            settled.append({
                **item,
                "reason": (
                    f"paid on {match['match_date']}"
                    + ("" if match["match_exact"] else " for a slightly different amount")
                ),
            })
        else:
            template = _covering_template(
                templates, name=row.get("name"), vendor=row.get("vendor_or_customer"),
                frequency=cadence, event_type=event_type,
            )
            if template:
                covered.append({
                    **item,
                    "covered_by": template["name"],
                    "reason": f"already on your {template['frequency'] or 'recurring'} schedule here",
                })
            elif cadence:
                uncovered.append({
                    **item,
                    "reason": "it keeps coming back and nothing here would carry it on",
                })
            elif due >= as_of:
                uncovered.append({
                    **item,
                    "reason": "it is still to come and nothing here would replace it",
                })
            else:
                undated.append({
                    **item,
                    "reason": "already past its date and open in your ledger, so it stays put",
                })

    uncovered.sort(key=lambda item: (item["due_date"], -item["amount_cents"]))
    counts = {
        "covered": len(covered),
        "settled": len(settled),
        "undated": len(undated),
        "uncovered": len(uncovered),
        "total": len(covered) + len(settled) + len(undated) + len(uncovered),
    }
    at_risk_cents = sum(int(item["amount_cents"]) for item in uncovered)
    already_off = _flag_is_set()
    return {
        "as_of": as_of.isoformat(),
        "ready": not uncovered,
        "already_switched_off": already_off,
        "summary": _summary(counts=counts, at_risk_cents=at_risk_cents, already_off=already_off),
        "counts": counts,
        "at_risk_cents": at_risk_cents,
        "clickup_row_count": len(rows),
        "covered": covered,
        "settled": settled,
        "undated": undated,
        "uncovered": uncovered,
    }


def render_cutover_readiness(assessment: Optional[dict[str, Any]] = None) -> str:
    """One card: the verdict, the four groups, and the bills that block the switch."""
    data = assessment or assess_cutover_readiness()
    counts = data["counts"]
    ready = bool(data["ready"])

    verdict_colour = "#1f7a34" if ready else "#a12020"
    verdict_word = "Safe to switch off" if ready else "Not safe yet"
    lines = [
        f"{counts['covered']} covered by a schedule here",
        f"{counts['settled']} already paid",
        f"{counts['undated']} with nothing to lose",
        f"{counts['uncovered']} that would disappear",
    ]
    counts_html = (
        '<ul style="margin:0 0 10px 18px;padding:0;font-size:13px;color:#6b7a8d">'
        + "".join(f"<li>{html.escape(line)}</li>" for line in lines)
        + "</ul>"
    )

    rows = []
    for item in data["uncovered"]:
        when = str(item["due_date"]) or "no date"
        rows.append(
            "<tr><td>" + html.escape(str(item["name"]))
            + '<br><small style="color:#6b7a8d">' + html.escape(str(item["reason"])) + "</small></td>"
            + "<td>" + html.escape(str(item["vendor"])) + "</td>"
            + '<td style="text-align:right">' + html.escape(_dollars(item["amount_cents"])) + "</td>"
            + "<td>" + html.escape(when) + "</td></tr>"
        )
    uncovered_html = (
        '<table class="finance-accounts-table">'
        "<thead><tr><th>Bill</th><th>Who to</th>"
        '<th style="text-align:right">Amount</th><th>Next due</th></tr></thead>'
        f"<tbody>{''.join(rows)}</tbody></table>"
        '<p style="font-size:13px;color:#6b7a8d;margin:10px 0 0">'
        "Set up a schedule here for each of these, then check this again."
        "</p>"
    ) if rows else ""

    state_html = (
        '<p style="font-size:13px;color:#6b7a8d;margin:10px 0 0">'
        + (
            "The old list is switched off already."
            if data["already_switched_off"]
            else "The old list is still running, so nothing has changed yet."
        )
        + " Switching it off is a settings change, not a button on this page.</p>"
    )

    return (
        '<div class="card">'
        "<h2>Can you switch the old bill list off?</h2>"
        f'<p style="margin:0 0 8px;font-weight:600;color:{verdict_colour}">'
        f"{html.escape(verdict_word)}</p>"
        f'<p style="margin:0 0 10px;font-size:13px">{html.escape(str(data["summary"]))}</p>'
        + counts_html
        + uncovered_html
        + state_html
        + "</div>"
    )
