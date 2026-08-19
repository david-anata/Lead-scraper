"""Run the outbound morning routine without anyone pressing anything.

Lead Ops has always shown a schedule ("Today is Monday. 1 pull scheduled"), but
nothing ever executed it: the plan was advisory and a person still had to click
Pull now, then click the Amazon scan, then wait. That made whoever was driving
the tool the bottleneck.

This runs the whole unattended half in-process, once a day: pull the recipes due
today, then check the new brands on Amazon so the opening lines are already
written by the time anyone looks. What is left for a person is the part that
genuinely needs judgement - reading the lines and deciding to send.

Same shape as install_embedded_website_ops_scheduler: a daemon thread, a due
check on a clock, and a persisted marker so a redeploy mid-morning does not run
the day twice.
"""

from __future__ import annotations

import logging
import os
import secrets
from datetime import datetime
from threading import Event, Thread
from zoneinfo import ZoneInfo

from fastapi import APIRouter, FastAPI, HTTPException, Request

from sales_support_agent.services.job_lease import (
    claim_scheduled_job,
    finish_scheduled_job,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/jobs/outbound-morning", tags=["outbound-jobs"])

_TZ = "America/Denver"
_MARKER = "__morning_run__"             # recipe name used purely as a daily marker
_RUN_HOUR = 7                           # before the working day, so it is ready
_SCAN_PER_DAY = 12                      # ~30 min of Amazon checks, then it stops


def _today(now: datetime) -> str:
    return now.strftime("%Y-%m-%d")


def _note_for(now: datetime) -> str:
    """The marker text. Comparing this rather than the row timestamp keeps the
    check on our clock instead of the database's."""
    return f"Automatic morning run started {_today(now)}"


def _already_ran_today(engine, now: datetime) -> bool:
    """Has the morning routine already started today?

    Read off the pull-runs table rather than the settings store, because every
    settings write bumps the config version, and a daily marker would fill the
    change log with noise and make real retunes impossible to find.
    """
    from sales_support_agent.services import outbound_memory

    try:
        stamp = _note_for(now)
        for run in outbound_memory.load_runs(engine, limit=40):
            if str(run.get("recipe") or "") != _MARKER:
                continue
            # Match on OUR date written into the note, not the row's timestamp.
            # The database stamps UTC while the schedule thinks in Denver time,
            # so after ~6pm local those are different dates and the job would
            # forget it had run, restart, and pay for the Amazon checks again
            # every ten minutes.
            if str(run.get("note") or "") == stamp:
                return True
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-jobs] could not tell whether today already ran")
    return False


def _mark_ran(engine, now: datetime) -> None:
    from sales_support_agent.services import outbound_memory

    try:
        outbound_memory.record_run(
            engine, recipe=_MARKER, scanned=0, matched=0, fresh=0, skipped_seen=0,
            delivery="scheduled", delivered=0,
            note=_note_for(now),
        )
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-jobs] could not record that today's run started")


_APP_URL = "https://agent.anatainc.com"
_BATCH_LINK = f"{_APP_URL}/admin/api/outbound/brands.csv?max_new=200"


def _sendable_brands(engine) -> list[dict]:
    """Brands with a real Amazon finding: what actually goes to Clay."""
    from sales_support_agent.services import outbound_memory

    held = outbound_memory.load_leads(engine, limit=2000)
    ready = [l for l in held
             if str(l.get("amazon_checked_at") or "").strip()
             and not str(l.get("amazon_skipped_reason") or "").strip()
             and str(l.get("amz_situation") or "").strip()]
    ready.sort(key=lambda l: -int(l.get("score") or 0))
    return ready


def _email_the_batch(engine, summary: dict, *, force: bool = False) -> bool:
    """Tell David what is waiting, so he never has to go looking for it.

    A link rather than an attachment: the file is regenerated on download, so a
    link is always current while an attachment is stale the moment a scan runs.
    Sends only when there is something to act on - a daily "0 brands" email
    trains you to ignore the daily email.
    """
    from sales_support_agent.config import load_settings
    from sales_support_agent.services import outbound_memory
    from sales_support_agent.services.access import notify

    prefs = outbound_memory.load_delivery_settings(engine)
    if not force and (not prefs["enabled"] or prefs["frequency"] != "daily"):
        return False

    ready = _sendable_brands(engine)
    if not ready:
        logger.info("[outbound-jobs] nothing sendable today, no email")
        return False

    recipients = [x.strip() for x in prefs.get("email_recipients", "").replace(";", ",").split(",") if x.strip()]
    if force and not recipients:
        recipients = [os.getenv("OUTBOUND_DIGEST_TO", "david@anatainc.com").strip()]
    lines = [
        f"{len(ready)} brand(s) are ready to put into Clay.",
    ]
    if prefs.get("content_mode") == "link" or force:
        lines += ["", f"Download the file:  {_BATCH_LINK}"]
    lines += ["", "What is in it:"]
    for lead in ready[:40]:
        brand = str(lead.get("brand") or lead.get("domain") or "").strip()
        opener = str(lead.get("reason") or "").strip()
        lines.append(f"  - {brand}: {opener}")
    if len(ready) > 40:
        lines.append(f"  ...and {len(ready) - 40} more in the file.")

    lines += [
        "",
        "What to do with it:",
        "  1. Clay > Anata // Claude Table > Recent Store Leads tab > Import CSV",
        "  2. Run Find people at these companies, then Work Email",
        "  3. Run the opening line column",
        "  4. Export from Found Contacts and upload into Instantly",
        "",
        f"This morning: pulled {summary.get('pulled', 0)}, checked {summary.get('scanned', 0)} on Amazon.",
        "",
        "Nothing sends until you press Resume in Instantly.",
    ]

    try:
        settings = load_settings()
        sent = False
        if prefs.get("email_enabled") or force:
            from sales_support_agent.services.outbound_delivery import _csv_attachment
            attachments = _csv_attachment(ready, "anata-leads-daily-digest.csv")
            for recipient in recipients:
                ok = notify._send(settings, to_email=recipient,
                                  subject=f"Outbound: {len(ready)} brands ready for Clay",
                                  text="\n".join(lines), attachments=attachments)
                sent = ok or sent
                outbound_memory.record_delivery_attempt(
                    engine, recipe="daily digest", destination="email", target=recipient,
                    status="sent" if ok else "failed",
                    detail=f"CSV attached ({len(ready)} companies)" if attachments else "No lead rows available to attach",
                )
        if prefs.get("slack_enabled"):
            from sales_support_agent.integrations.slack import SlackClient
            try:
                slack_ok = bool(SlackClient(settings).post_message(text="\n".join(lines)).get("ok"))
            except Exception:  # noqa: BLE001
                logger.exception("[outbound-jobs] daily Slack delivery failed")
                slack_ok = False
            sent = slack_ok or sent
            outbound_memory.record_delivery_attempt(
                engine, recipe="daily digest", destination="slack",
                target=str(getattr(settings, "slack_channel_id", "") or "configured channel"),
                status="sent" if slack_ok else "failed",
            )
        return sent
    except Exception:  # noqa: BLE001 - a failed email must not fail the morning
        logger.exception("[outbound-jobs] could not send the morning digest")
        return False


def run_morning_routine(*, now: datetime | None = None, scan_limit: int = _SCAN_PER_DAY) -> dict:
    """Pull whatever is due today, then check those brands on Amazon.

    Returns a small summary so the health endpoint and the tests can see what
    happened. Never raises: a failed morning must not take the app down.
    """
    import outbound_pipeline as _op
    import outbound_recipes as _rx

    from sales_support_agent.services import outbound_memory

    now = now or datetime.now(ZoneInfo(_TZ))
    out: dict = {"ran": False, "recipes": [], "pulled": 0, "scanned": 0, "reason": ""}

    try:
        from sales_support_agent.models.database import get_engine
        engine = get_engine()
    except Exception:  # noqa: BLE001
        engine = None

    api_key, _clay = _op.load_config_from_env()
    if not api_key:
        out["reason"] = "STORELEADS_API_KEY is not set"
        return out

    from sales_support_agent.services import outbound_settings as _st
    tunables = _st.effective(engine, _rx.DEFAULT_SETTINGS) if engine is not None else _rx.DEFAULT_SETTINGS
    version = _st.config_version(engine) if engine is not None else 0

    due = _rx.recipes_for_day(now.weekday(), tunables)
    if not due:
        out["reason"] = "nothing scheduled today"
        return out

    already = outbound_memory.load_contacted(engine) if engine is not None else set()

    # 1. Pull. Fast and free; the Amazon work is deliberately not inline here,
    #    because at minutes per brand it would never finish inside one request.
    for recipe in due:
        try:
            result = _op.run_storeleads_to_clay(
                api_key=api_key, clay_webhook_url="", processed_domains=already,
                max_new=recipe.cap(tunables), dry_run=True,
                recipe=recipe, settings=tunables,
            )
        except Exception:  # noqa: BLE001
            logger.exception("[outbound-jobs] pull failed for %s", recipe.key)
            continue

        out["recipes"].append(recipe.key)
        out["pulled"] += len(result.leads)
        already |= {str(l.get("domain") or "") for l in result.leads}

        if engine is not None:
            if result.leads:
                outbound_memory.record_leads(engine, result.leads,
                                             source=result.recipe or "scheduled",
                                             config_version=version)
            run_id = outbound_memory.record_run(
                engine, recipe=result.recipe or recipe.key, scanned=result.scanned,
                matched=result.matched_icp, fresh=len(result.leads),
                skipped_seen=result.skipped_already_contacted,
                partial=bool(getattr(result, "partial", False)),
                config_version=version, delivery="scheduled",
                delivered=len(result.leads), note="automatic morning run",
            )
            outbound_memory.record_run_leads(engine, run_id, result.leads)
            try:
                from sales_support_agent.services.outbound_delivery import deliver_completed_pull
                deliver_completed_pull(engine, {
                    "id": run_id,
                    "recipe": result.recipe or recipe.key, "scanned": result.scanned,
                    "matched": result.matched_icp, "fresh": len(result.leads),
                    "skipped_seen": result.skipped_already_contacted,
                    "partial": bool(getattr(result, "partial", False)), "config_version": version,
                })
            except Exception:  # noqa: BLE001
                logger.exception("[outbound-jobs] automatic pull delivery failed")

    # 2. Check the best of them on Amazon, so the opening lines exist before
    #    anyone opens the page. Bounded, because each brand costs minutes.
    if engine is not None:
        from sales_support_agent.api.outbound_router import _amazon_checker

        check = _amazon_checker(tunables)
        if check is not None:
            import outbound_amazon as _az
            max_age = _az._int_setting(tunables, "amazon.finding_max_age_days")
            pending = outbound_memory.leads_needing_amazon(
                engine, limit=scan_limit, max_age_days=max_age)
            for lead in pending:
                domain = str(lead.get("domain") or "")
                if not domain:
                    continue
                try:
                    if outbound_memory.update_amazon_finding(engine, domain, check(lead)):
                        out["scanned"] += 1
                except Exception:  # noqa: BLE001
                    logger.exception("[outbound-jobs] amazon check failed for %s", domain)

    out["ran"] = True
    if engine is not None:
        out["emailed"] = _email_the_batch(engine, out)
    logger.info("[outbound-jobs] morning run: %s", out)
    return out
