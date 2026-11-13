"""Scheduler entry point for Abby (see abby_runner). Vercel Cron, every 5 minutes.

After Abby finishes, the same tick may run outbound enrich in-process.
Enrich can take several minutes (its own budget is 540s, inside the 800s
function limit). It runs only after Abby's work so a long or failed enrich
cannot drop replies Abby already handled. See maybe_run_enrich_from_abby.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Request

from sales_support_agent.api.outbound_jobs import _require_scheduler_auth

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/jobs/abby", tags=["outbound-jobs"])


def lead_info_from_db(engine):
    def look(email: str) -> dict[str, Any]:
        from sqlalchemy import text
        try:
            with engine.connect() as conn:
                row = conn.execute(text(
                    "SELECT first_name, brand, domain FROM outbound_enriched_contacts WHERE email = :e"),
                    {"e": email}).fetchone()
            if row:
                return {"first_name": row[0] or "", "company": row[1] or "", "domain": row[2] or ""}
        except Exception:  # noqa: BLE001
            logger.exception("[abby] lead lookup failed")
        return {}
    return look


def slack_poster():
    from sales_support_agent.config import load_settings
    from sales_support_agent.integrations.slack import SlackClient
    from sales_support_agent.services.sales_slack_policy import failure_kind, post_failure_once

    client = SlackClient(load_settings())

    def post(text: str) -> None:
        kind = failure_kind(text)
        if kind:
            try:
                from sales_support_agent.models.database import get_engine

                post_failure_once(
                    get_engine(),
                    lambda body: client.post_message(text=body),
                    kind=kind,
                    text=text,
                )
            except Exception:  # noqa: BLE001
                logger.exception("[abby] failure alert was not posted")
            return
        try:
            client.post_message(text=text)
        except Exception:  # noqa: BLE001
            logger.exception("[abby] Slack post failed")
    return post


def run_abby() -> dict[str, Any]:
    import abby_runner
    import outbound_instantly_push as ip
    from sales_support_agent.models.database import get_engine

    try:
        import outbound_reengage as reng
        engine = get_engine()
        client = abby_runner.Instantly(ip.api_key())
        campaigns = abby_runner.watched_campaign_ids(
            client.campaign_ids_named(reng.NAME), engine,
        )
        out = {}
        post = slack_poster()
        info = lead_info_from_db(engine)
        for cid in campaigns:
            out[cid] = abby_runner.run(engine, client=client, campaign_id=cid,
                                       lead_info=info, post_slack=post)
        # Mail Instantly stored with no campaign id is invisible to the loop above.
        out["_unassigned"] = abby_runner.run_unassigned(
            engine, client=client, watched=campaigns, lead_info=info, post_slack=post,
        )
        return out
    except Exception as exc:  # noqa: BLE001
        logger.exception("[abby] run failed")
        return {"error": type(exc).__name__}


def _run_enrich_after_abby() -> dict[str, Any]:
    """Call enrich directly. Never raises into Abby's response."""

    try:
        from sales_support_agent.api.outbound_enrich_job import maybe_run_enrich_from_abby

        result = maybe_run_enrich_from_abby()
        if isinstance(result, dict):
            return result
    except Exception:  # noqa: BLE001
        logger.exception("[enrich-via-abby] hook failed")
    return {"ran": False, "reason": "hook failed", "pushed": 0}


def _maybe_analytics_backfill() -> dict[str, Any] | None:
    """On Vercel, the first Abby tick backfills analytics when the table is empty.

    Vercel Cron already authenticates this route. The backfill does not send mail.
    """

    if not os.getenv("VERCEL"):
        return None
    try:
        from sales_support_agent.models.database import get_engine
        from sales_support_agent.services.instantly_campaign_analytics import (
            maybe_backfill_if_empty,
        )

        return maybe_backfill_if_empty(get_engine())
    except Exception:  # noqa: BLE001
        logger.exception("[instantly-analytics] backfill skipped")
        return {"ok": False, "ran": False, "reason": "backfill skipped"}


def _maybe_register_webhook(request: Request) -> dict[str, Any]:
    """Register Abby's Instantly webhook after deploy. Missing keys skip quietly."""

    try:
        from sales_support_agent.services.instantly_abby_webhook import maybe_register_abby_webhook

        return maybe_register_abby_webhook(settings=getattr(request.app.state, "settings", None))
    except Exception:  # noqa: BLE001
        logger.exception("[abby] webhook registration hook failed")
        return {"ok": False, "skipped": True, "reason": "hook failed"}


@router.api_route("/run", methods=["GET", "POST"])
def run_scheduled_abby(request: Request) -> dict:
    _require_scheduler_auth(request)
    details = run_abby()
    payload = {
        "status": "ok",
        "details": details,
        "enrich_via_abby": _run_enrich_after_abby(),
        "webhook_registration": _maybe_register_webhook(request),
    }
    backfill = _maybe_analytics_backfill()
    if backfill is not None:
        payload["analytics_backfill"] = backfill
    return payload


@router.api_route("/followups", methods=["GET", "POST"])
def run_scheduled_followups(request: Request) -> dict[str, Any]:
    """Send follow-ups whose due time has passed. Guarded like the other jobs."""

    _require_scheduler_auth(request)
    import abby_runner
    import outbound_instantly_push as instantly_push
    from sales_support_agent.models.database import get_engine

    if abby_runner.mode() != "live":
        return {"status": "ok", "sent": 0, "reason": "ABBY_MODE is not live"}
    try:
        engine = get_engine()
        client = abby_runner.Instantly(instantly_push.api_key())
        post = slack_poster()
        cap = int(os.getenv("ABBY_DAILY_CAP") or 60)
        sent = abby_runner.send_due_scheduled(
            engine, client, datetime.now(timezone.utc), "live", post, cap,
        )
        sent += abby_runner._send_due_followups(engine, client, datetime.now(timezone.utc), "live", post)
    except Exception as exc:  # noqa: BLE001
        logger.exception("[abby] scheduled follow-ups failed")
        return {"status": "error", "error": type(exc).__name__}
    return {"status": "ok", "sent": sent}


def _require_internal_key(request: Request, provided: str) -> None:
    """The same ``X-Internal-Api-Key`` check the other internal routes use."""

    import secrets

    configured = str(getattr(request.app.state.settings, "internal_api_key", "") or "").strip()
    if not configured:
        raise HTTPException(status_code=503, detail="Internal API is not configured.")
    if not secrets.compare_digest(str(provided or "").strip(), configured):
        raise HTTPException(status_code=401, detail="Invalid internal API key.")


@router.api_route("/missed-replies", methods=["GET", "POST"])
def run_missed_replies(request: Request) -> dict[str, Any]:
    """Hourly pass for human replies Abby missed. It does not post to Slack."""

    _require_scheduler_auth(request)
    import abby_runner
    import outbound_instantly_push as instantly_push
    import outbound_reengage as reengage
    from sales_support_agent.models.database import get_engine

    if abby_runner.mode() == "off":
        return {"status": "ok", "sent": 0, "reason": "ABBY_MODE is off"}
    try:
        engine = get_engine()
        client = abby_runner.Instantly(instantly_push.api_key())
        campaigns = abby_runner.watched_campaign_ids(
            client.campaign_ids_named(reengage.NAME), engine,
        )
        summary = abby_runner.run_missed_replies(engine, client=client, campaign_ids=campaigns)
    except Exception as exc:  # noqa: BLE001
        logger.exception("[abby] missed-reply fix failed")
        return {"status": "error", "error": type(exc).__name__}
    return {"status": "ok", **summary}


@router.get("/deck-sends")
def list_deck_sends(
    request: Request,
    since: str = "",
    x_internal_api_key: str = Header(default=""),
) -> dict[str, Any]:
    """Deck sends since ``since`` that still have no founder reply on the thread."""

    _require_internal_key(request, x_internal_api_key)
    import abby_runner
    import outbound_instantly_push as instantly_push
    from datetime import datetime, timezone

    from sales_support_agent.models.database import get_engine

    raw = (since or "").strip()
    if not raw:
        raise HTTPException(status_code=400, detail="since is required.")
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="since must be an ISO timestamp.") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    def threads(thread_id: str) -> list[dict]:
        key = instantly_push.api_key()
        if not key or not thread_id:
            return []
        return abby_runner.Instantly(key).thread(thread_id)

    try:
        rows = abby_runner.list_deck_sends_since(get_engine(), parsed, threads)
    except Exception as exc:  # noqa: BLE001
        logger.exception("[abby] deck send list failed")
        raise HTTPException(status_code=500, detail="Could not list deck sends.") from exc
    return {"sends": rows}


@router.api_route("/apply-yes-cta", methods=["POST"])
def apply_yes_cta(request: Request) -> dict[str, Any]:
    """Rewrite live cold asks and pause the calendar subsequence.

    Not on the cron. Call it once after this code is deployed. The response
    lists every step that changed. The Instantly key is not returned.
    """

    _require_scheduler_auth(request)
    import outbound_instantly_push as instantly_push
    from outbound_yes_cta import apply_with_instantly

    report = apply_with_instantly(instantly_push.api_key())
    return {"status": "ok" if not report.get("errors") else "error", **report}
