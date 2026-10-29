"""Daily contact finding: the Clay replacement, run by the scheduler.

Approved by David on 2026-10-22: run into the live Instantly campaign, up to
120 companies and 250 Prospeo credits a day.

Vercel Cron calls this a few times each weekday morning. Each call takes the
newest sourced brands that have not been enriched, finds up to two verified
decision-makers per brand (Prospeo), writes an opening line, and adds them to
the Instantly campaign in ``OUTBOUND_INSTANTLY_CAMPAIGN_ID``. There is no
fallback campaign. A Slack summary posts after each call that did something.

Limits, all overridable with env vars. Defaults are the approved caps, so a
deploy changes nothing until the env vars are set:
  OUTBOUND_AUTO_ENRICH            must be "true" for the job to run (off by default)
  OUTBOUND_ENRICH_DAILY_COMPANIES companies per day across all calls (120)
  OUTBOUND_ENRICH_DAILY_CREDITS   Prospeo credits per day (250)
  OUTBOUND_ENRICH_PER_RUN         companies per call (40), keeps a call short
  OUTBOUND_ENRICH_MAX_CONTACTS    verified people per brand (2)
Leads only reach Instantly when OUTBOUND_PUSH_ENABLED is "true"; otherwise the
contacts are found and stored and the summary says nothing was added.

Recommended once David wants about 540 contacts on a weekday, across the four
cron runs (14:15, 15:15, 16:15, 17:15 UTC). Do not treat these as defaults.
Each hour is its own path, one cron per path, the same shape as Abby.
A single schedule of ``15 14,15,16,17 * * 1-5`` on ``/run`` was listed and
then never invoked, including on a deploy where the morning comma-hour cron
and the weekday ``1-5`` crons did run. ``/run`` stays as one temporary
20:30 UTC recovery (``30 20 * * 1-5``).

Those cron paths stay registered. Abby's every-5-minute job also calls
``maybe_run_enrich_from_abby`` directly, because Vercel has not been invoking
the enrich paths. The first Abby tick runs enrich when no attempt has
succeeded today or in the last 6 hours. After that, enrich runs at most once
per America/Denver hour on weekdays. ``OUTBOUND_AUTO_ENRICH`` is still required.

  OUTBOUND_ENRICH_DAILY_COMPANIES=300
  OUTBOUND_ENRICH_PER_RUN=75
  OUTBOUND_ENRICH_MAX_CONTACTS=2
  OUTBOUND_ENRICH_DAILY_CREDITS=1100

Prospeo charges 1 credit for a search that returns people and 1 credit for each
verified email. Misses are not charged. 300 companies that yield about 1.8
verified emails each is about 300 + 540 = 840 credits for about 540 contacts.
The 1100 cap is headroom so a search-heavy morning is not cut off early.
If yield stays closer to one verified email per company, 300 companies produce
about 300 contacts and about 600 credits, and the 540 second time budget (about
70 to 90 companies per run) is the real ceiling. In that case raise the daily
company cap toward 400 and the credit cap toward 1200.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, time as dtime, timedelta, timezone
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
DEFAULT_MAX_CONTACTS = 2


def _int_env(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return max(0, int(raw))
    except ValueError:
        return default


def _enabled() -> bool:
    # Off until David finishes his review and sets OUTBOUND_AUTO_ENRICH=true on Vercel.
    return (os.getenv("OUTBOUND_AUTO_ENRICH") or "").strip().lower() == "true"


def _in_enrich_hours(local: datetime) -> bool:
    """True from the start hour inclusive until the end hour exclusive.

    Defaults are 7 and 18 America/Denver, so 06:00 and 18:00 are outside.
    """

    start = _int_env("OUTBOUND_ENRICH_START_HOUR", 7)
    end = _int_env("OUTBOUND_ENRICH_END_HOUR", 18)
    return start <= local.hour < end


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
    if rep.get("suppressed"):
        lines.append(f"Skipped, on our suppression list: {rep['suppressed']}")
    if rep.get("stopped_reason"):
        lines.append(f"Stopped: {rep['stopped_reason']}")
    if result.get("campaign_missing"):
        lines.append("Instantly push skipped: OUTBOUND_INSTANTLY_CAMPAIGN_ID is unset.")
    if result.get("push_problem"):
        lines.append(f"Instantly problem: {result.get('push_note', '')}")
    bounce = result.get("bounce_today") or {}
    if bounce.get("sent"):
        rate = bounce.get("bounce_rate")
        rate_text = f"{rate}%" if rate is not None else "no rate"
        lines.append(
            f"Bounces today: {bounce.get('bounces', 0)} of {bounce.get('sent', 0)} sent ({rate_text})")
    elif bounce.get("bounces"):
        lines.append(f"Bounces today: {bounce.get('bounces', 0)} (no sends recorded yet, so no rate)")
    return "\n".join(lines)


def _refresh_delivery_state(engine) -> dict[str, Any]:
    """Sync the blocklist and backfill bounces. A failure here does not stop finding."""
    try:
        import outbound_bounces as _bounces
        import outbound_suppression as _suppression
        return {
            "blocklist": _suppression.sync_blocklist(engine),
            "polled": _bounces.poll_lead_outcomes(engine),
            "bounces": _bounces.bounce_report(engine, days=2),
        }
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-enrich-job] deliverability refresh failed")
        return {}


def _default_post_slack(text: str) -> None:
    """Post through the shared Slack client the other jobs already use."""

    from sales_support_agent.config import load_settings
    from sales_support_agent.integrations.slack import SlackClient

    SlackClient(load_settings()).post_message(text=text)


def _notify_slack(post_slack: Optional[Callable[[str], Any]], text: str) -> None:
    try:
        (post_slack or _default_post_slack)(text)
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-enrich-job] Slack alert failed")


def _enrich_run_log_count(engine: Any, since_utc: datetime) -> int:
    """Rows in the existing scheduled-job log for an enrich run since ``since_utc``.

    The table is optional. A missing table means there is no run log to count.
    """

    try:
        from sqlalchemy import text

        with engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT COUNT(*) FROM scheduled_job_runs
                    WHERE job_key IN ('outbound_enrich', 'outbound-enrich')
                      AND started_at >= :t
                    """
                ),
                {"t": since_utc.isoformat()},
            ).fetchone()
    except Exception:  # noqa: BLE001
        return 0
    return int(row[0] or 0) if row else 0


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
    if not _in_enrich_hours(_as_denver(now)):
        out["reason"] = "outside enrich hours"
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
    delivery = _refresh_delivery_state(engine)
    bounce_today = None
    try:
        import outbound_bounces as _bounces
        bounce_today = _bounces.today_slice(delivery.get("bounces") or {}, now)
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-enrich-job] could not summarize bounces")
    push_on = _ip.push_enabled()
    campaign = _ip.configured_campaign_id()
    campaign_missing = not campaign
    out["campaign_missing"] = campaign_missing
    out["campaign_id"] = campaign
    if campaign_missing:
        logger.error("[outbound-enrich-job] %s", _ip.MISSING_CAMPAIGN_MESSAGE)
        _notify_slack(post_slack, _ip.MISSING_CAMPAIGN_MESSAGE)
    if company_room <= 0 or credit_room <= 0:
        out["reason"] = "daily cap reached"
        return out

    try:
        if leads is None:
            from sales_support_agent.services import outbound_memory
            leads = outbound_memory.load_leads(engine, limit=1000)

        report = _oe.run_enrichment(
            engine, leads, api_key=key,
            max_companies=min(company_room, _int_env("OUTBOUND_ENRICH_PER_RUN", DEFAULT_PER_RUN)),
            max_contacts=_int_env("OUTBOUND_ENRICH_MAX_CONTACTS", DEFAULT_MAX_CONTACTS),
            credit_budget=credit_room,
            campaign_id=campaign, dry_run=not push_on or campaign_missing,
            find=find, write=write, push=push,
            deadline=time.monotonic() + _TIME_BUDGET_SECONDS,
            # Never touch a company Clay (or anything else) already put in Instantly.
            already_in_instantly=in_instantly or _ip.domain_in_instantly,
        )
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-enrich-job] run failed")
        out["reason"] = "run failed"
        return out

    if campaign_missing:
        report.push_note = _ip.MISSING_CAMPAIGN_MESSAGE
    out.update(
        ran=True, report=report.as_dict(), pushed=report.pushed,
        push_enabled=push_on and not campaign_missing, push_note=report.push_note,
        push_problem=bool(
            push_on and not campaign_missing
            and report.contacts_found and not report.pushed
            and report.suppressed < report.contacts_found
        ),
        credits_today=today["credits"] + report.credits_used,
        campaign_id=campaign,
        campaign_missing=campaign_missing,
        bounce_today=bounce_today,
    )

    if report.companies_tried or report.pushed:
        _notify_slack(post_slack, summary_text(out))
    logger.info("[outbound-enrich-job] %s", {k: v for k, v in out.items() if k != "report"})
    return out


def run_missed_enrich_check(
    *,
    now: Optional[datetime] = None,
    engine: Any = None,
    post_slack: Optional[Callable[[str], Any]] = None,
) -> dict[str, Any]:
    """Alert when today's Denver date has no enrich activity by mid-morning.

    Activity is a row in ``outbound_enriched`` or an outbound-enrich row in
    ``scheduled_job_runs``. Neither table is created here.
    """

    now = now or datetime.now(ZoneInfo(_TZ))
    if now.weekday() >= 5:
        return {"alerted": False, "reason": "weekend"}
    since = _start_of_day_utc(now)
    try:
        if engine is None:
            from sales_support_agent.models.database import get_engine
            engine = get_engine()
        import outbound_enrich as _oe
        today = _oe.enriched_since(engine, since)
        run_logs = _enrich_run_log_count(engine, since)
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-enrich-job] could not check whether enrich ran today")
        message = (
            "Outbound enrich watchdog could not read today's activity. "
            "Check /api/jobs/outbound-enrich/at-1415."
        )
        logger.error("[outbound-enrich-job] %s", message)
        _notify_slack(post_slack, message)
        return {"alerted": True, "reason": "could not read enrich activity"}
    if int(today["companies"]) > 0 or run_logs > 0:
        return {
            "alerted": False,
            "reason": "enrich activity recorded",
            "companies": today["companies"],
            "run_logs": run_logs,
        }
    message = (
        "Outbound enrich has not recorded a run today (America/Denver). "
        "outbound_enriched has no new rows since midnight, and scheduled_job_runs "
        "has no outbound-enrich row. Weekday crons: "
        "/api/jobs/outbound-enrich/at-1415, at-1515, at-1615, and at-1715 "
        "(14:15, 15:15, 16:15, and 17:15 UTC)."
    )
    logger.error("[outbound-enrich-job] %s", message)
    _notify_slack(post_slack, message)
    return {
        "alerted": True,
        "reason": "no enrich activity today",
        "companies": 0,
        "run_logs": 0,
    }


def run_scheduled_enrich(request: Request) -> dict:
    """Called by Vercel Cron (GET, ``Authorization: Bearer $CRON_SECRET``).

    The check is ``_require_scheduler_auth``, the same function Abby uses.
    ``VERCEL_CRON_ENABLED_JOBS`` is not read. A value there does not skip
    this route. POST accepts the same credentials for an operator trigger.
    """

    _require_scheduler_auth(request)
    return {"status": "ok", "details": run_enrich_job()}


# One route per method. A single route with methods ["GET", "POST"] reuses the
# GET operation id for POST, and FastAPI warns "Duplicate Operation ID
# run_scheduled_enrich..._get". /run is the temporary 20:30 UTC recovery.
# The at-* paths are the weekday hours, one Vercel cron per path.
_ENRICH_RUN_SUFFIXES = ("/run", "/at-1415", "/at-1515", "/at-1615", "/at-1715")


def _register_enrich_run_routes() -> None:
    for suffix in _ENRICH_RUN_SUFFIXES:
        slug = suffix.strip("/").replace("-", "_")
        for method in ("GET", "POST"):
            router.add_api_route(
                suffix,
                run_scheduled_enrich,
                methods=[method],
                name=f"outbound_enrich_{slug}_{method.lower()}",
                operation_id=f"outbound_enrich_{slug}_{method.lower()}",
            )


_register_enrich_run_routes()


@router.get("/missed-run", operation_id="outbound_enrich_missed_run_get")
def run_scheduled_missed_enrich_check(request: Request) -> dict:
    """Weekday 15:30 UTC check that enrich has already recorded work today."""

    _require_scheduler_auth(request)
    return {"status": "ok", "details": run_missed_enrich_check()}


@router.post("/missed-run", operation_id="outbound_enrich_missed_run_post")
def run_scheduled_missed_enrich_check_post(request: Request) -> dict:
    return run_scheduled_missed_enrich_check(request)


# Abby calls this. The lease table already exists (scheduled_job_runs). Nothing
# here creates a table or reads a new env var. 6 hours reaches across midnight
# so a late weekday run still counts as recent early the next morning.
_VIA_ABBY_JOB_KEY = "outbound_enrich_via_abby"
_VIA_ABBY_RECENT_HOURS = 6
_VIA_ABBY_LEASE_MINUTES = 60


def _as_denver(now: datetime) -> datetime:
    """Normalize ``now`` onto the enrich job's America/Denver clock."""

    if now.tzinfo is None:
        return now.replace(tzinfo=ZoneInfo(_TZ))
    return now.astimezone(ZoneInfo(_TZ))


def enrich_via_abby_window(now: datetime, *, recent_success: bool) -> Optional[str]:
    """Lease key for this Abby tick, or None when enrich must not start.

    Weekdays use one key per Denver hour inside the enrich hours (default 7
    inclusive through 18 exclusive), at any minute, so the first tick in that
    hour runs. On Saturday and Sunday, one catch-up key per Denver date runs
    only when nothing has succeeded today or in the last 6 hours.
    """

    local = _as_denver(now)
    if local.weekday() < 5:
        if not _in_enrich_hours(local):
            return None
        return local.strftime("abby-%Y-%m-%dT%H")
    if not recent_success:
        return local.strftime("abby-catchup-%Y-%m-%d")
    return None


def _recent_enrich_cutoff(now: datetime) -> datetime:
    """UTC instant: earlier of Denver midnight and N hours ago."""

    local = _as_denver(now)
    midnight = datetime.combine(local.date(), dtime.min, tzinfo=local.tzinfo)
    hours_ago = local - timedelta(hours=_VIA_ABBY_RECENT_HOURS)
    earlier = midnight if midnight <= hours_ago else hours_ago
    return earlier.astimezone(timezone.utc)


def _has_recent_enrich_success(engine: Any, now: datetime) -> bool:
    """True when this fallback already succeeded today or in the last 6 hours."""

    cutoff = _recent_enrich_cutoff(now).isoformat()
    try:
        from sqlalchemy import text

        with engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT COUNT(*) FROM scheduled_job_runs
                    WHERE job_key = :job_key
                      AND status = 'succeeded'
                      AND started_at >= :cutoff
                    """
                ),
                {"job_key": _VIA_ABBY_JOB_KEY, "cutoff": cutoff},
            ).fetchone()
    except Exception:  # noqa: BLE001
        logger.exception("[enrich-via-abby] could not read last enrich run")
        return False
    return bool(row and int(row[0] or 0) > 0)


def _enrich_run_in_flight(engine: Any, now: datetime) -> bool:
    """True when a claimed run has not finished and its lease is still live."""

    current = _as_denver(now).astimezone(timezone.utc).isoformat()
    try:
        from sqlalchemy import text

        with engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT COUNT(*) FROM scheduled_job_runs
                    WHERE job_key = :job_key
                      AND status = 'running'
                      AND lease_expires_at >= :now
                    """
                ),
                {"job_key": _VIA_ABBY_JOB_KEY, "now": current},
            ).fetchone()
    except Exception:  # noqa: BLE001
        logger.exception("[enrich-via-abby] could not read in-flight enrich run")
        return False
    return bool(row and int(row[0] or 0) > 0)


def _enrich_via_abby_result(result: dict[str, Any], window: str) -> dict[str, Any]:
    """One result line: counts, or the skip reason. Never raises."""

    report = result.get("report") if isinstance(result.get("report"), dict) else {}
    try:
        leads_attempted = int(report.get("companies_tried") or 0)
    except (TypeError, ValueError):
        leads_attempted = 0
    try:
        enriched = int(report.get("contacts_found") or 0)
    except (TypeError, ValueError):
        enriched = 0
    try:
        pushed = int(result.get("pushed") or 0)
    except (TypeError, ValueError):
        pushed = 0
    payload = {
        "ran": bool(result.get("ran")),
        "reason": str(result.get("reason") or ""),
        "window": window,
        "leads_attempted": leads_attempted,
        "enriched": enriched,
        "pushed": pushed,
    }
    logger.info(
        "[enrich-via-abby] result window=%s ran=%s leads_attempted=%s enriched=%s pushed=%s reason=%s",
        payload["window"],
        payload["ran"],
        payload["leads_attempted"],
        payload["enriched"],
        payload["pushed"],
        payload["reason"],
    )
    return payload


def maybe_run_enrich_from_abby(
    *,
    now: Optional[datetime] = None,
    engine: Any = None,
    run_job: Optional[Callable[..., Any]] = None,
) -> dict[str, Any]:
    """Run ``run_enrich_job`` from Abby's cron. Never raises.

    The ``OUTBOUND_AUTO_ENRICH`` gate is ``_enabled``, the same check
    ``run_enrich_job`` uses. A failed enrich is recorded for this window so
    the next Abby tick does not start a second attempt in the same hour.
    """

    try:
        return _maybe_run_enrich_from_abby(now=now, engine=engine, run_job=run_job)
    except Exception:  # noqa: BLE001
        logger.exception("[enrich-via-abby] hook failed")
        return _enrich_via_abby_result(
            {"ran": False, "reason": "hook failed", "pushed": 0},
            "error",
        )


def _maybe_run_enrich_from_abby(
    *,
    now: Optional[datetime],
    engine: Any,
    run_job: Optional[Callable[..., Any]],
) -> dict[str, Any]:
    moment = now or datetime.now(ZoneInfo(_TZ))
    if not _enabled():
        # Do not claim the hour. Turning the gate on still runs on the next tick.
        return _enrich_via_abby_result(
            {"ran": False, "reason": "OUTBOUND_AUTO_ENRICH is false", "pushed": 0},
            "gated",
        )

    if engine is None:
        from sales_support_agent.models.database import get_engine

        engine = get_engine()

    local = _as_denver(moment)
    recent = _has_recent_enrich_success(engine, local) if local.weekday() >= 5 else False
    window = enrich_via_abby_window(local, recent_success=recent)
    if window is None:
        return _enrich_via_abby_result(
            {"ran": False, "reason": "not due", "pushed": 0},
            "none",
        )
    if _enrich_run_in_flight(engine, local):
        return _enrich_via_abby_result(
            {"ran": False, "reason": "enrich already in progress", "pushed": 0},
            window,
        )

    from sales_support_agent.services.job_lease import claim_scheduled_job, finish_scheduled_job

    try:
        lease = claim_scheduled_job(
            engine,
            job_key=_VIA_ABBY_JOB_KEY,
            run_key=window,
            lease_minutes=_VIA_ABBY_LEASE_MINUTES,
        )
    except Exception:  # noqa: BLE001
        logger.exception("[enrich-via-abby] could not claim enrich window=%s", window)
        # Abby fires every 5 minutes. Minutes 0-4 contain one tick, so a missing
        # lease table still runs at most once per weekday hour.
        if local.weekday() < 5 and local.minute < 5:
            return _enrich_via_abby_result(
                _invoke_enrich(run_job, local, engine, window), window,
            )
        return _enrich_via_abby_result(
            {"ran": False, "reason": "could not claim enrich window", "pushed": 0},
            window,
        )
    if lease is None:
        return _enrich_via_abby_result(
            {"ran": False, "reason": "already ran this window", "pushed": 0},
            window,
        )

    result = _invoke_enrich(run_job, local, engine, window)
    try:
        finish_scheduled_job(
            engine,
            lease,
            status="succeeded",
            details={
                "ran": bool(result.get("ran")),
                "reason": str(result.get("reason") or ""),
                "pushed": result.get("pushed") or 0,
                "source": "abby",
            },
        )
    except Exception:  # noqa: BLE001
        logger.exception("[enrich-via-abby] could not record enrich window=%s", window)
    return _enrich_via_abby_result(result, window)


def _invoke_enrich(run_job, local: datetime, engine: Any, window: str) -> dict[str, Any]:
    logger.info("[enrich-via-abby] start window=%s", window)
    try:
        result = (run_job or run_enrich_job)(now=local, engine=engine)
        if not isinstance(result, dict):
            return {"ran": False, "reason": "enrich returned nothing", "pushed": 0}
        return result
    except Exception:  # noqa: BLE001
        logger.exception("[enrich-via-abby] enrich failed")
        return {"ran": False, "reason": "enrich failed", "pushed": 0}
