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
from datetime import datetime
from threading import Event, Thread
from zoneinfo import ZoneInfo

from fastapi import FastAPI

logger = logging.getLogger(__name__)

_TZ = "America/Denver"
_MARKER = "__morning_run__"             # recipe name used purely as a daily marker
_RUN_HOUR = 7                           # before the working day, so it is ready
_SCAN_PER_DAY = 12                      # ~30 min of Amazon checks, then it stops


def _today(now: datetime) -> str:
    return now.strftime("%Y-%m-%d")


def _already_ran_today(engine, now: datetime) -> bool:
    """Has the morning routine already started today?

    Read off the pull-runs table rather than the settings store, because every
    settings write bumps the config version, and a daily marker would fill the
    change log with noise and make real retunes impossible to find.
    """
    from sales_support_agent.services import outbound_memory

    try:
        for run in outbound_memory.load_runs(engine, limit=40):
            if str(run.get("recipe") or "") != _MARKER:
                continue
            when = str(run.get("ran_at") or "")
            if when.startswith(_today(now)):
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
            note=f"Automatic morning run started {_today(now)}",
        )
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-jobs] could not record that today's run started")


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

        if engine is not None and result.leads:
            outbound_memory.record_leads(engine, result.leads,
                                         source=result.recipe or "scheduled",
                                         config_version=version)
            outbound_memory.record_run(
                engine, recipe=result.recipe or recipe.key, scanned=result.scanned,
                matched=result.matched_icp, fresh=len(result.leads),
                skipped_seen=result.skipped_already_contacted,
                partial=bool(getattr(result, "partial", False)),
                config_version=version, delivery="scheduled",
                delivered=len(result.leads), note="automatic morning run",
            )

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


def install_embedded_outbound_scheduler(app: FastAPI) -> None:
    """Run the morning routine in-process, once a day, after 7am Denver."""
    enabled = os.getenv("OUTBOUND_EMBEDDED_SCHEDULER", "true").strip().lower() in {
        "1", "true", "yes", "on",
    }
    if not enabled:
        return
    if getattr(app.state, "outbound_scheduler_thread", None):
        return

    stop_event = Event()

    def worker() -> None:
        while not stop_event.is_set():
            try:
                now = datetime.now(ZoneInfo(_TZ))
                if now.hour >= _RUN_HOUR:
                    try:
                        from sales_support_agent.models.database import get_engine
                        engine = get_engine()
                    except Exception:  # noqa: BLE001
                        engine = None
                    if engine is not None and not _already_ran_today(engine, now):
                        # Marked BEFORE the work, not after. The run takes half an
                        # hour and a redeploy partway through would otherwise start
                        # it again and pay for the same Amazon checks twice.
                        _mark_ran(engine, now)
                        run_morning_routine(now=now)
            except Exception:  # noqa: BLE001
                logger.exception("[outbound-jobs] embedded scheduler failed")
            stop_event.wait(600)

    thread = Thread(target=worker, name="outbound-scheduler", daemon=True)
    app.state.outbound_scheduler_stop = stop_event
    app.state.outbound_scheduler_thread = thread
    thread.start()
