"""Daily contact finding: the Clay replacement, run by the scheduler.

Approved by David on 2026-10-22: run into the live Instantly campaign, up to
120 companies and 250 Prospeo credits a day.

Vercel Cron calls this a few times each weekday morning. Each call takes the
newest sourced brands that have not been enriched, finds up to two verified
decision-makers per brand (Prospeo), writes an opening line, and adds them to
the live Instantly campaign (the one Clay used to feed). A Slack summary posts
after each call that did something.

Limits, all overridable with env vars:
  OUTBOUND_AUTO_ENRICH            "false" turns the whole job off (kill switch)
  OUTBOUND_ENRICH_DAILY_COMPANIES companies per day across all calls (120)
  OUTBOUND_ENRICH_DAILY_CREDITS   Prospeo credits per day (250)
  OUTBOUND_ENRICH_PER_RUN         companies per call (40), keeps a call short
Leads only reach Instantly when OUTBOUND_PUSH_ENABLED is "true"; otherwise the
contacts are found and stored and the summary says nothing was added.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, time as dtime
from typing import Any, Callable, Optional
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Request

from sales_support_agent.api.outbound_jobs import _require_scheduler_auth

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/jobs/outbound-enrich", tags=["outbound-jobs"])

_TZ = "America/Denver"
_TIME_BUDGET_SECONDS = 540  # well inside Vercel's 800s function limit

DEFAULT_DAILY_COMPANIES = 120
DEFAULT_DAILY_CREDITS = 250
DEFAULT_PER_RUN = 40


def _int_env(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return max(0, int(raw))
    except ValueError:
        return default


def _enabled() -> bool:
    return (os.getenv("OUTBOUND_AUTO_ENRICH") or "true").strip().lower() != "false"


def _start_of_day_utc(now: datetime) -> datetime:
    local_midnight = datetime.combine(now.date(), dtime.min, tzinfo=now.tzinfo)
    return local_midnight.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)


def summary_text(result: dict[str, Any]) -> str:
    rep = result.get("report") or {}
    lines = [
        "Outbound contacts (replaces Clay)",
        f"Companies checked: {rep.get('companies_tried', 0)} | with a decision-maker: "
        f"{rep.get('companies_with_contacts', 0)} | contacts: {rep.get('contacts_found', 0)}",
        f"Prospeo credits: {rep.get('credits_used', 0)} (today so far: {result.get('credits_today', 0)})",
        f"Added to Instantly: {result.get('pushed', 0)}"
        + ("" if result.get("push_enabled") else " (sending switch is off, nothing added)"),
    ]
    if rep.get("skipped_in_instantly"):
        lines.append(f"Skipped, company already in Instantly: {rep['skipped_in_instantly']}")
    if rep.get("skipped_check_failed"):
        lines.append(f"Skipped, Instantly check failed (retried next run): {rep['skipped_check_failed']}")
    if rep.get("stopped_reason"):
        lines.append(f"Stopped: {rep['stopped_reason']}")
    if result.get("push_problem"):
        lines.append(f"Instantly problem: {result.get('push_note', '')}")
    return "\n".join(lines)


def run_enrich_job(
    *,
    now: Optional[datetime] = None,
    engine: Any = None,
    leads: Optional[list[dict[str, Any]]] = None,
    find: Optional[Callable[..., Any]] = None,
    write: Optional[Callable[..., Any]] = None,
    push: Optional[Callable[..., Any]] = None,
    post_slack: Optional[Callable[[str], Any]] = None,
    in_instantly: Optional[Callable[[str], Optional[bool]]] = None,
) -> dict[str, Any]:
    """One scheduled call. Never raises."""
    import outbound_contacts as _oc
    import outbound_enrich as _oe
    import outbound_instantly_push as _ip

    now = now or datetime.now(ZoneInfo(_TZ))
    out: dict[str, Any] = {"ran": False, "reason": "", "pushed": 0}
    if not _enabled():
        out["reason"] = "OUTBOUND_AUTO_ENRICH is false"
        return out
    if now.weekday() >= 5:
        out["reason"] = "weekend"
        return out
    key = _oc.load_contact_config()
    if not key:
        out["reason"] = "PROSPEO_API_KEY is not set"
        return out
    try:
        if engine is None:
            from sales_support_agent.models.database import get_engine
            engine = get_engine()
        today = _oe.enriched_since(engine, _start_of_day_utc(now))
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-enrich-job] could not read today's totals")
        out["reason"] = "could not read today's totals"
        return out

    company_room = _int_env("OUTBOUND_ENRICH_DAILY_COMPANIES", DEFAULT_DAILY_COMPANIES) - today["companies"]
    credit_room = _int_env("OUTBOUND_ENRICH_DAILY_CREDITS", DEFAULT_DAILY_CREDITS) - today["credits"]
    out["credits_today"] = today["credits"]
    if company_room <= 0 or credit_room <= 0:
        out["reason"] = "daily cap reached"
        return out

    try:
        if leads is None:
            from sales_support_agent.services import outbound_memory
            leads = outbound_memory.load_leads(engine, limit=1000)

        push_on = _ip.push_enabled()
        campaign = os.getenv("OUTBOUND_INSTANTLY_CAMPAIGN_ID", "").strip() or _ip.LIVE_CAMPAIGN_ID
        report = _oe.run_enrichment(
            engine, leads, api_key=key,
            max_companies=min(company_room, _int_env("OUTBOUND_ENRICH_PER_RUN", DEFAULT_PER_RUN)),
            max_contacts=2, credit_budget=credit_room,
            campaign_id=campaign, dry_run=not push_on,
            find=find, write=write, push=push,
            deadline=time.monotonic() + _TIME_BUDGET_SECONDS,
            # Never touch a company Clay (or anything else) already put in Instantly.
            already_in_instantly=in_instantly or _ip.domain_in_instantly,
        )
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-enrich-job] run failed")
        out["reason"] = "run failed"
        return out

    out.update(
        ran=True, report=report.as_dict(), pushed=report.pushed,
        push_enabled=push_on, push_note=report.push_note,
        push_problem=bool(push_on and report.contacts_found and not report.pushed),
        credits_today=today["credits"] + report.credits_used,
        campaign_id=campaign,
    )

    if report.companies_tried or report.pushed:
        try:
            if post_slack is None:
                from sales_support_agent.config import load_settings
                from sales_support_agent.integrations.slack import SlackClient
                client = SlackClient(load_settings())
                post_slack = lambda text: client.post_message(text=text)  # noqa: E731
            post_slack(summary_text(out))
        except Exception:  # noqa: BLE001
            logger.exception("[outbound-enrich-job] Slack summary failed")
    logger.info("[outbound-enrich-job] %s", {k: v for k, v in out.items() if k != "report"})
    return out


@router.api_route("/run", methods=["GET", "POST"])
def run_scheduled_enrich(request: Request) -> dict:
    """Called by Vercel Cron (CRON_SECRET) or an internal operator key."""
    _require_scheduler_auth(request)
    return {"status": "ok", "details": run_enrich_job()}
