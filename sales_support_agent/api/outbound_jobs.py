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
from uuid import uuid4
from datetime import datetime
from datetime import datetime as _wall_clock
from typing import Any
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
_morning_state: dict = {"running": False, "last_status": "", "last_reason": ""}
_SCAN_PER_DAY = 12                      # ~30 min of Amazon checks, then it stops
_DAILY_COMPANY_CAP = 825                # David 2026-11-13: room for 825 companies
_PULL_TIME_BUDGET_S = 420              # leave room for the Amazon checks inside 800s


# #sales. Same channel id the sales call prep posts to.
SALES_SLACK_CHANNEL = "CQRMXS339"


def morning_is_running() -> bool:
    return bool(_morning_state.get("running"))


def begin_morning_run() -> bool:
    """Claim the manual pull. False when one is already in this process."""

    if _morning_state.get("running"):
        return False
    _morning_state["running"] = True
    _morning_state["last_status"] = "running"
    return True


def finish_morning_run(result: dict | None) -> None:
    result = result or {}
    failed = result.get("ran") is False and bool(result.get("reason"))
    _morning_state["running"] = False
    _morning_state["last_status"] = "failed" if failed else "finished"
    _morning_state["last_reason"] = str(result.get("reason") or "")
    _morning_state["last"] = result


def morning_pull_status(engine, now: datetime) -> dict:
    """What the Daily Leads button should say, without starting a pull."""

    if morning_is_running():
        return {"ok": False, "status": "running", "reason": "Today's pull is already running."}
    today = _today(now)
    try:
        from sales_support_agent.services import outbound_batches

        rows, _total = outbound_batches.load_batches(engine, page=1, per_page=30)
    except Exception:  # noqa: BLE001
        rows = []
    match = next((row for row in rows if row.business_date == today), None)
    if match is not None and outbound_batches.batch_is_reopenable(match.status):
        return {
            "ok": False,
            "status": "failed",
            "already_ran": False,
            "batch_id": match.id,
            "unique_companies": match.unique_companies,
            "reason": (
                f"Today's pull did not finish. Batch {match.id} is still {match.status}, "
                "so it can be run again."
            ),
        }
    if match is not None:
        held = 0
        try:
            import outbound_enrich as enrich

            held = enrich._count_opener_holds(engine)
        except Exception:  # noqa: BLE001
            held = 0
        return {
            "ok": True,
            "status": "finished",
            "already_ran": True,
            "batch_id": match.id,
            "unique_companies": match.unique_companies,
            "scanned": match.scanned,
            "matched": match.matched,
            "completed_count": match.completed_count,
            "recipe_count": match.recipe_count,
            "held_openers": held,
            "reason": (
                f"Already ran today. Finished with {match.unique_companies} companies, "
                f"scanned {match.scanned}, matched {match.matched}, "
                f"{match.completed_count} of {match.recipe_count} recipes complete, "
                f"{held} held contacts."
            ),
        }
    if _morning_state.get("last_status") == "failed" and _morning_state.get("last_reason"):
        return {"ok": False, "status": "failed", "reason": f"Failed: {_morning_state['last_reason']}"}
    return {"ok": True, "status": "ready", "reason": "No pull yet today."}


def post_sales_alert(batch: Any, recipe_runs: list[dict], *, slack: Any, held: int = 0) -> dict:
    """Tell #sales when a daily batch delivered nothing or a recipe failed.

    A skipped held recipe is not an error. Slack being unconfigured does not
    raise. The message names the batch, the recipe, and the reason.
    """

    failures = [
        run for run in recipe_runs
        if str(run.get("status") or "") == "failed"
    ]
    companies = int(getattr(batch, "unique_companies", 0) or 0)
    if companies > 0 and not failures:
        return {"posted": False, "reason": ""}
    lines = [
        f"Daily Leads batch {getattr(batch, 'id', '')} for {getattr(batch, 'business_date', '')} needs a look.",
    ]
    if companies == 0:
        lines.append("Delivered 0 companies.")
    for run in failures:
        lines.append(
            f"Recipe {run.get('recipe_key') or 'unknown'} failed: {run.get('error') or 'no reason recorded'}."
        )
    if held:
        lines.append(f"Held contacts waiting for a shopper-visible opener: {held}.")
    text = " ".join(lines)
    try:
        result = slack.post_message(text=text, channel=SALES_SLACK_CHANNEL)
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-jobs] sales alert was not posted")
        return {"posted": False, "reason": "slack post failed", "text": text}
    return {"posted": bool(result.get("ok")), "reason": str(result.get("reason") or ""), "text": text}


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


_EXPECTED_SKIP_REASONS = frozenset({
    "nothing scheduled today",
    "today's daily batch already exists",
    "daily marker already exists",
})
_RECORDED_RUN_STATUSES = frozenset({"complete", "partial", "failed"})


def lease_status_for(result: dict | None) -> str:
    """Scheduler lease outcome for one morning-routine result."""

    result = result or {}
    reason = str(result.get("reason") or "")
    if result.get("ran") or reason in _EXPECTED_SKIP_REASONS:
        return "succeeded"
    return "failed"


def credit_recovered_runs(recipe_runs: list[dict], leads: list[dict]) -> None:
    """Count reattached companies on the recipe row that owns them."""

    counts: dict[str, int] = {}
    for lead in leads:
        key = str(lead.get("recipe") or "unknown").strip() or "unknown"
        counts[key] = counts.get(key, 0) + 1
    for key, count in counts.items():
        match = next((run for run in recipe_runs if str(run.get("recipe_key") or "") == key), None)
        if match is None:
            recipe_runs.append({
                "recipe_key": key,
                "recipe_label": key,
                "status": "complete",
                "fresh": count,
                "matched": count,
                "scanned": 0,
                "skipped_seen": 0,
                "error": "",
            })
            continue
        match["fresh"] = int(match.get("fresh") or 0) + count
        match["matched"] = int(match.get("matched") or 0) + count
        if str(match.get("status") or "") == "skipped":
            match["status"] = "complete"
            match["error"] = ""


def _rerun_requested(request: Request) -> bool:
    """True when an operator is replaying an unfinished business day.

    The date is America/Denver today. There is no business_date parameter.
    ``rerun=1`` skips the 7am hour check. It does not reopen a batch that
    already finalized, and it does not bypass scheduler auth.
    """

    raw = str(request.query_params.get("rerun") or "").strip().lower()
    return raw in {"1", "true", "yes"}


def _explicit_rerun_target(request: Request) -> tuple[str, int]:
    """``business_date`` or ``batch_id`` on a rerun. Both require ``rerun=1``.

    A normal cron call does not pass either. An explicit target can only reopen
    a running or failed scheduled batch; it does not create a new day.
    """

    business_date = str(request.query_params.get("business_date") or "").strip()
    raw_id = str(request.query_params.get("batch_id") or "").strip()
    if business_date and (
        len(business_date) != 10 or business_date[4] != "-" or business_date[7] != "-"
        or not business_date.replace("-", "").isdigit()
    ):
        raise HTTPException(status_code=400, detail="business_date must be YYYY-MM-DD.")
    batch_id = 0
    if raw_id:
        if not raw_id.isdigit() or int(raw_id) <= 0:
            raise HTTPException(status_code=400, detail="batch_id must be a positive whole number.")
        batch_id = int(raw_id)
    if (business_date or batch_id) and not _rerun_requested(request):
        raise HTTPException(
            status_code=400,
            detail="Pass rerun=1 to reopen a batch by business_date or batch_id.",
        )
    return business_date, batch_id


_APP_URL = "https://agent.anatainc.com"
_BATCH_LINK = f"{_APP_URL}/admin/api/outbound/brands.csv?scanned=1&max_new=200"


def _sendable_brands(engine) -> list[dict]:
    """Brands with a real Amazon finding."""
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
        f"{len(ready)} brand(s) have an Amazon finding.",
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
        "  Nothing. Contacts are found and added to Instantly automatically each",
        "  weekday morning (Prospeo daily job). Clay is no longer used.",
        "",
        f"This morning: pulled {summary.get('pulled', 0)}, checked {summary.get('scanned', 0)} on Amazon.",
        "",
    ]

    try:
        settings = load_settings()
        sent = False
        if prefs.get("email_enabled") or force:
            from sales_support_agent.services.outbound_delivery import _csv_attachment
            attachments = _csv_attachment(ready, "anata-leads-daily-digest.csv")
            for recipient in recipients:
                ok = notify._send(settings, to_email=recipient,
                                  subject=f"Outbound: {len(ready)} brands with an Amazon finding",
                                  text="\n".join(lines), attachments=attachments)
                sent = ok or sent
                outbound_memory.record_delivery_attempt(
                    engine, recipe="daily digest", destination="email", target=recipient,
                    status="sent" if ok else "failed",
                    detail=f"CSV attached ({len(ready)} companies)" if attachments else "No lead rows available to attach",
                )
        if prefs.get("slack_enabled"):
            logger.info(
                "[outbound-jobs] morning digest not posted to Slack\n%s",
                "\n".join(lines),
            )
            outbound_memory.record_delivery_attempt(
                engine, recipe="daily digest", destination="slack",
                target=str(getattr(settings, "slack_channel_id", "") or "configured channel"),
                status="logged", detail="Routine digest kept out of Slack",
            )
        return sent
    except Exception:  # noqa: BLE001 - a failed email must not fail the morning
        logger.exception("[outbound-jobs] could not send the morning digest")
        return False


def _pull_baseline_slices(_op, _rx, api_key, already, requested, recipe, tunables, now, started, engine=None):
    """Run the baseline once per revenue slice until the cap or time budget is hit.

    Returns one combined PipelineResult so the caller records a single run."""
    import dataclasses
    import time as _time

    combined = _op.PipelineResult(dry_run=True)
    combined.recipe = recipe.key
    seen = set(already)
    for override in _rx.baseline_slices_for(now, tunables):
        if combined.fresh >= requested or _time.monotonic() - started > _PULL_TIME_BUDGET_S:
            break
        sliced = dataclasses.replace(recipe, build=lambda _now, _s, o=override: dict(o))
        part = _op.run_storeleads_to_clay(
            api_key=api_key, clay_webhook_url="", processed_domains=seen,
            max_new=requested - combined.fresh, dry_run=True,
            recipe=sliced, settings=tunables, engine=engine,
        )
        combined.scanned += part.scanned
        combined.matched_icp += part.matched_icp
        combined.skipped_already_contacted += part.skipped_already_contacted
        combined.partial = combined.partial or part.partial
        for lead in part.leads:
            if lead["domain"] in seen:
                continue
            seen.add(lead["domain"])
            combined.leads.append(lead)
            combined.fresh += 1
    combined.leads.sort(key=lambda l: l.get("score", 0), reverse=True)
    return combined


def run_morning_routine(*, now: datetime | None = None, scan_limit: int = _SCAN_PER_DAY) -> dict:
    """Pull whatever is due today, then check those brands on Amazon.

    Returns a small summary so the health endpoint and the tests can see what
    happened. Pull failures are caught. A finalize failure is marked on the
    batch and raised, so the cron returns 503 instead of leaving the batch
    running. An unfinished batch for this business date is reopened.
    """
    import outbound_pipeline as _op
    import outbound_recipes as _rx

    from sales_support_agent.services import outbound_batches, outbound_memory

    now = now or datetime.now(ZoneInfo(_TZ))
    out: dict = {"ran": False, "recipes": [], "pulled": 0, "scanned": 0, "reason": ""}
    if now.weekday() >= 5:
        # Render's morning cron is every day. Vercel is weekdays. Either way,
        # Saturday and Sunday do not source. Campaign filters are not an exception.
        out["reason"] = "weekend"
        return out

    try:
        from sales_support_agent.models.database import get_engine
        engine = get_engine()
    except Exception:  # noqa: BLE001
        engine = None

    api_key, _clay = _op.load_config_from_env()
    if not api_key:
        out["reason"] = "STORELEADS_API_KEY is not set"
        return out
    health = outbound_memory.persistence_health(engine)
    if not health["ready"]:
        out["reason"] = "outbound persistence is unavailable; no StoreLeads pull started"
        return out

    from sales_support_agent.services import outbound_settings as _st
    tunables = _st.effective(engine, _rx.DEFAULT_SETTINGS) if engine is not None else _rx.DEFAULT_SETTINGS
    version = _st.config_version(engine) if engine is not None else 0

    outbound_batches.ensure_tables(engine)
    due = outbound_batches.materialize_recipes(engine, now.weekday(), tunables)
    if not due and engine is None:
        out["reason"] = "nothing scheduled today"
        return out

    batch_id, created = outbound_batches.create_batch(
        engine, business_date=_today(now), trigger="scheduled",
        recipe_count=len(due), correlation_id=str(uuid4()),
        resume_incomplete=True,
    )
    if not created:
        out["reason"] = "today's daily batch already exists"
        out["batch_id"] = batch_id
        return out

    already = outbound_memory.load_contacted(engine) if engine is not None else set()
    all_leads: list[dict] = []
    recipe_runs: list[dict] = []
    import outbound_campaign_source as _source
    daily_remaining = _source.sourcing_company_cap()
    import time as _time
    pull_started = _time.monotonic()
    # Signal-campaign filters claim the daily cap first (this is what fills
    # amz_on and amz_off). That room is shared across the active filter
    # campaigns so a later recipe is not left at zero. Scheduled recipes
    # follow. Baseline stays last.
    due = [r for r in due if r.key != "icp_baseline"] + [r for r in due if r.key == "icp_baseline"]
    if engine is not None and daily_remaining > 0:
        try:
            sourced = _source.source_active_campaigns(
                engine, api_key=api_key, now=now, contacted=already,
                company_room=daily_remaining,
            )
        except Exception:  # noqa: BLE001
            logger.exception("[outbound-jobs] campaign sourcing failed")
            sourced = {"recorded": 0, "leads": [], "recipes": [], "scanned": 0, "skipped_seen": 0}
        recorded = int(sourced.get("recorded") or 0)
        if recorded:
            sourced_leads = list(sourced.get("leads") or [])
            all_leads.extend(sourced_leads)
            already |= {str(lead.get("domain") or "") for lead in sourced_leads}
            daily_remaining = max(0, daily_remaining - recorded)
            out["pulled"] += recorded
            out["recipes"].extend(list(sourced.get("recipes") or []))
        for item in sourced.get("campaigns") or []:
            recipe_runs.append({
                "recipe_key": str(item.get("recipe_key") or ""),
                "recipe_label": str(item.get("recipe_key") or ""),
                "status": "failed" if item.get("error") else "complete",
                "fresh": int(item.get("recorded") or 0),
                "scanned": int(item.get("scanned") or 0),
                "matched": int(item.get("recorded") or 0),
                "skipped_seen": int(item.get("skipped_seen") or 0),
                "error": str(item.get("error") or ""),
            })
        if not sourced.get("campaigns") and sourced.get("stopped_reason"):
            recipe_runs.append({
                "recipe_key": "campaign_filters",
                "recipe_label": "Signal campaign filters",
                "status": "failed",
                "fresh": 0,
                "scanned": int(sourced.get("scanned") or 0),
                "matched": 0,
                "skipped_seen": int(sourced.get("skipped_seen") or 0),
                "error": str(sourced.get("stopped_reason") or "")[:500],
            })
        try:
            sales = _source.source_sales_org(
                engine, api_key=api_key, now=now, contacted=already,
                company_room=daily_remaining,
            )
        except Exception:  # noqa: BLE001
            logger.exception("[outbound-jobs] sales-org sourcing failed")
            sales = {"recorded": 0}
        sales_recorded = int(sales.get("recorded") or 0)
        if sales_recorded:
            sales_leads = list(sales.get("leads") or [])
            all_leads.extend(sales_leads)
            already |= {str(lead.get("domain") or "") for lead in sales_leads}
            daily_remaining = max(0, daily_remaining - sales_recorded)
            out["pulled"] += sales_recorded
            out["recipes"].extend(list(sales.get("recipes") or []))

    catalog = None
    if engine is not None:
        try:
            import outbound_campaigns as _campaigns

            catalog = _campaigns.list_campaigns(engine)
        except Exception:  # noqa: BLE001
            logger.exception("[outbound-jobs] could not read campaigns for the recipe gate")
            catalog = None
    if catalog:
        import outbound_campaign_source as _gate

        allowed_due = []
        for recipe in due:
            skip_reason = _gate.legacy_recipe_pull_reason(recipe.key, catalog, tunables)
            if not skip_reason:
                allowed_due.append(recipe)
                continue
            logger.info("[outbound-jobs] skipping %s: %s", recipe.key, skip_reason)
            # The campaign loop already appended this recipe. A second skipped
            # row uses the same recipe_key and the batch-run primary key rejects
            # the insert. replatformed is the alias of replatform, so its key
            # differs and would not collide, but it is the same sourcing decision
            # and must not become its own batch row either.
            if skip_reason == _gate.ALREADY_SOURCED_REASON:
                continue
            recipe_runs.append({
                "recipe_key": recipe.key,
                "recipe_label": recipe.label,
                "status": "skipped",
                "fresh": 0,
                "error": skip_reason,
            })
        due = allowed_due

    # 1. Pull. Fast and free; the Amazon work is deliberately not inline here,
    #    because at minutes per brand it would never finish inside one request.
    # A campaign row is already the batch record. Pulling the same key again
    # (the catalog read failed, or it came back empty) would insert a second
    # primary key and spend another StoreLeads query.
    recorded_keys = {
        str(run.get("recipe_key") or "")
        for run in recipe_runs
        if str(run.get("status") or "") in _RECORDED_RUN_STATUSES
    }
    for recipe in due:
        if recipe.key in recorded_keys:
            logger.info(
                "[outbound-jobs] not pulling %s: a campaign batch row is already recorded",
                recipe.key,
            )
            continue
        requested = min(recipe.cap(tunables), daily_remaining)
        if requested <= 0:
            recipe_runs.append({"recipe_key": recipe.key, "recipe_label": recipe.label,
                                "status": "complete", "fresh": 0})
            continue
        try:
            if recipe.key == "icp_baseline":
                result = _pull_baseline_slices(_op, _rx, api_key, already, requested,
                                               recipe, tunables, now, pull_started, engine)
            else:
                result = _op.run_recipe_pull(
                    api_key=api_key, clay_webhook_url="", processed_domains=already,
                    max_new=requested, dry_run=True,
                    recipe=recipe, settings=tunables, now=now, engine=engine,
                )
        except Exception:  # noqa: BLE001
            logger.exception("[outbound-jobs] pull failed for %s", recipe.key)
            recipe_runs.append({"recipe_key": recipe.key, "recipe_label": recipe.label,
                                "status": "failed", "error": "StoreLeads pull failed"})
            continue

        out["recipes"].append(recipe.key)
        out["pulled"] += len(result.leads)
        already |= {str(l.get("domain") or "") for l in result.leads}
        for lead in result.leads:
            lead.setdefault("recipe", recipe.key)
        all_leads.extend(result.leads)
        daily_remaining = max(0, daily_remaining - len(result.leads))

        if engine is not None:
            try:
                persisted = outbound_memory.persist_pull(
                    engine, leads=result.leads, recipe=result.recipe or recipe.key,
                    scanned=result.scanned, matched=result.matched_icp,
                    fresh=len(result.leads), skipped_seen=result.skipped_already_contacted,
                    partial=bool(getattr(result, "partial", False)), config_version=version,
                    delivery="scheduled", delivered=len(result.leads),
                    note="automatic morning run",
                )
                run_id = persisted.run_id
            except outbound_memory.OutboundPersistenceError:
                logger.exception("[outbound-jobs] pull persistence failed for %s", recipe.key)
                out["reason"] = f"persistence failed for {recipe.key}"
                recipe_runs.append({"recipe_key": recipe.key, "recipe_label": recipe.label,
                                    "status": "failed", "error": out["reason"]})
                break
            recipe_runs.append({
                "run_id": run_id, "recipe_key": recipe.key, "recipe_label": recipe.label,
                "status": "partial" if bool(getattr(result, "partial", False)) else "complete",
                "fresh": len(result.leads), "scanned": result.scanned,
                "matched": result.matched_icp,
                "skipped_seen": result.skipped_already_contacted,
            })

    if engine is not None:
        recovered, already_pushed = outbound_batches.orphan_leads_for_batch(engine, batch_id)
        have = {str(lead.get("domain") or "").strip().lower() for lead in all_leads}
        fresh_orphans = [
            lead for lead in recovered
            if str(lead.get("domain") or "").strip().lower() not in have
        ]
        if fresh_orphans:
            credit_recovered_runs(recipe_runs, fresh_orphans)
            all_leads.extend(fresh_orphans)
            out["recovered_companies"] = len(fresh_orphans)
            out["pulled"] += len(fresh_orphans)
            logger.info(
                "[outbound-jobs] reattached %s orphan companies onto batch %s",
                len(fresh_orphans), batch_id,
            )
        if already_pushed:
            out["skipped_already_pushed"] = already_pushed
    recipe_runs = outbound_batches.merge_recipe_runs(recipe_runs)
    batch = outbound_batches.finalize_batch(
        engine, batch_id=batch_id, recipe_runs=recipe_runs, leads=all_leads,
        filename=f"anata-daily-leads-{_today(now)}.csv",
    )
    held_openers = 0
    if engine is not None:
        try:
            import outbound_enrich as _enrich

            held_openers = _enrich._count_opener_holds(engine)
        except Exception:  # noqa: BLE001
            logger.exception("[outbound-jobs] could not count held openers")
    out["held_openers"] = held_openers
    try:
        from sales_support_agent.config import load_settings
        from sales_support_agent.integrations.slack import SlackClient

        alert = post_sales_alert(
            batch, recipe_runs, slack=SlackClient(load_settings()), held=held_openers,
        )
        out["sales_alert"] = alert.get("posted")
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-jobs] sales alert failed")
    try:
        from sales_support_agent.services.outbound_delivery import deliver_daily_batch
        delivery = deliver_daily_batch(engine, batch, outbound_batches.load_batch_leads(engine, [batch_id]))
        outbound_batches.update_delivery(
            engine, batch_id, email_status=delivery["email"], slack_status=delivery["slack"])
        out["delivery"] = delivery
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-jobs] daily batch delivery failed")
    out["batch_id"] = batch_id
    out["unique_companies"] = batch.unique_companies

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
    logger.info("[outbound-jobs] morning run: %s", out)
    return out


def _reengage_topup(now: datetime) -> dict:
    """Monday completed/no-reply top-up. Adds leads only when explicitly enabled.

    It does not activate a campaign, send a reply, or read Trial Offer leads.
    Suppression and contacts already pushed to another campaign are excluded.
    """

    import outbound_reengage as reengage

    suppressed: set[str] = set()
    elsewhere: set[str] = set()
    try:
        from sqlalchemy import text

        from sales_support_agent.models.database import get_engine
        import outbound_suppression as suppression

        engine = get_engine()
        emails, _domains = suppression.lists(engine)
        suppressed = set(emails)
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT email FROM outbound_enriched_contacts "
                "WHERE pushed_at IS NOT NULL AND coalesce(campaign_id, '') != :campaign"
            ), {"campaign": reengage.TARGET_CAMPAIGN_ID}).fetchall()
        elsewhere = {str(row[0]).strip().lower() for row in rows if row and row[0]}
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-jobs] re-engage eligibility lists could not be read")
        if reengage.topup_enabled():
            return {"dry_run": True, "added": 0, "eligible": 0, "reason": "could not read suppression"}
    post = None
    if reengage.topup_enabled():
        import requests

        def post(url, **kwargs):  # type: ignore[no-redef]
            return requests.post(url, **kwargs)
    return reengage.run_weekly_topup(
        post=post, suppressed=suppressed, active_elsewhere=elsewhere, now=now,
    )


def _require_scheduler_auth(request: Request) -> None:
    """Accept Vercel Cron auth or the existing internal scheduler key."""

    cron_secret = os.getenv("CRON_SECRET", "").strip()
    authorization = request.headers.get("Authorization", "").strip()
    if cron_secret and secrets.compare_digest(
        authorization,
        f"Bearer {cron_secret}",
    ):
        return

    internal_key = str(
        getattr(request.app.state.settings, "internal_api_key", "") or ""
    ).strip()
    supplied_internal_key = request.headers.get("X-Internal-Api-Key", "").strip()
    if internal_key and secrets.compare_digest(supplied_internal_key, internal_key):
        return

    raise HTTPException(status_code=401, detail="Invalid scheduler credentials.")


@router.api_route("/run", methods=["GET", "POST"])
def run_scheduled_outbound(request: Request) -> dict:
    """Run the daily outbound batch from Vercel Cron or an internal operator.

    ``?rerun=1`` replays outside the 7am window. ``business_date=YYYY-MM-DD``
    or ``batch_id=`` picks an unfinished scheduled batch. A finished batch
    is refused. Companies saved before that batch's finalize failed are
    attached to the file. Auth is unchanged. Do not delete the batch row first.
    """

    _require_scheduler_auth(request)
    target_date, target_batch_id = _explicit_rerun_target(request)
    now = datetime.now(ZoneInfo(_TZ))
    engine = None
    try:
        from sales_support_agent.models.database import get_engine
        from sales_support_agent.services.outbound_settings import morning_pull_hour

        engine = get_engine()
        hour = morning_pull_hour(engine, _RUN_HOUR)
    except Exception:  # noqa: BLE001 - a missing database still has to skip cleanly
        hour = _RUN_HOUR
        engine = None
    rerun = _rerun_requested(request)
    if not rerun and now.hour != hour:
        return {
            "status": "skipped",
            "message": f"Outbound morning routine is waiting for {hour}:00 America/Denver.",
            "local_time": now.isoformat(),
        }
    if rerun:
        logger.info("[outbound-jobs] rerun requested for %s", target_date or _today(now))

    from sales_support_agent.models.database import get_engine

    if engine is None:
        engine = get_engine()
    explicit = bool(target_date or target_batch_id)
    if explicit:
        from sales_support_agent.services import outbound_batches

        chosen = outbound_batches.scheduled_batch(
            engine, business_date=target_date, batch_id=target_batch_id,
        )
        if chosen is None:
            detail = "No scheduled batch matches that business_date or batch_id."
            if target_date and target_batch_id:
                detail = (
                    f"Batch {target_batch_id} is not the scheduled batch for {target_date}."
                )
            raise HTTPException(status_code=409, detail=detail)
        if not outbound_batches.batch_is_reopenable(chosen.status):
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Batch {chosen.id} for {chosen.business_date} is {chosen.status}. "
                    "Only a running or failed batch can be reopened."
                ),
            )
        year, month, day = (int(part) for part in chosen.business_date.split("-"))
        now = _wall_clock(year, month, day, hour, tzinfo=ZoneInfo(_TZ))
        run_key = chosen.business_date
    else:
        run_key = _today(now)
    lease = claim_scheduled_job(
        engine,
        job_key="outbound_morning",
        run_key=run_key,
        lease_minutes=240,
        recover_succeeded=rerun,
    )
    if lease is None:
        return {
            "status": "skipped",
            "message": "Today's outbound routine is already running or complete.",
            "run_key": run_key,
        }

    topup: dict = {"reason": "not Monday"}
    if now.weekday() == 0:
        try:
            topup = _reengage_topup(now)
        except Exception:  # noqa: BLE001
            logger.exception("[outbound-jobs] re-engage top-up failed")
            topup = {"reason": "topup failed", "added": 0, "dry_run": True}

    try:
        if not explicit and _already_ran_today(engine, now):
            result = {"ran": False, "reason": "daily marker already exists"}
        else:
            result = run_morning_routine(now=now)

        reason = str(result.get("reason") or "")
        expected_skip = reason in _EXPECTED_SKIP_REASONS
        if not result.get("ran") and not expected_skip:
            finish_scheduled_job(engine, lease, status="failed", details=result)
            raise HTTPException(
                status_code=503,
                detail=reason or "Outbound morning routine did not complete.",
            )

        if result.get("ran"):
            _mark_ran(engine, now)
        finish_scheduled_job(engine, lease, status="succeeded", details=result)
        return {
            "status": "skipped" if expected_skip else "ok",
            "details": result,
            "reengage_topup": topup,
            "run_key": run_key,
        }
    except HTTPException:
        raise
    except Exception as exc:
        finish_scheduled_job(
            engine,
            lease,
            status="failed",
            details={"error": str(exc)},
        )
        logger.exception("[outbound-jobs] scheduled run failed")
        raise HTTPException(
            status_code=503,
            detail="Outbound morning routine failed.",
        ) from exc
