"""Scheduler entry point for Abby (see abby_runner). Vercel Cron, every 5 minutes.

After Abby finishes, the same tick may run outbound enrich in-process.
Enrich can take several minutes (its own budget is 540s, inside the 800s
function limit). It runs only after Abby's work so a long or failed enrich
cannot drop replies Abby already handled. See maybe_run_enrich_from_abby.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from fastapi import APIRouter, Request

from sales_support_agent.api.outbound_jobs import _require_scheduler_auth

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/jobs/abby", tags=["outbound-jobs"])


def lead_info_from_db(engine):
    def look(email: str) -> dict[str, Any]:
        from sqlalchemy import text
        try:
            with engine.connect() as conn:
                row = conn.execute(text(
                    "SELECT first_name, brand FROM outbound_enriched_contacts WHERE email = :e"),
                    {"e": email}).fetchone()
            if row:
                return {"first_name": row[0] or "", "company": row[1] or ""}
        except Exception:  # noqa: BLE001
            logger.exception("[abby] lead lookup failed")
        return {}
    return look


def slack_poster():
    from sales_support_agent.config import load_settings
    from sales_support_agent.integrations.slack import SlackClient
    client = SlackClient(load_settings())

    def post(text: str) -> None:
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
        campaigns = abby_runner.watched_campaign_ids(client.campaign_ids_named(reng.NAME))
        out = {}
        post = slack_poster()
        for cid in campaigns:
            out[cid] = abby_runner.run(engine, client=client, campaign_id=cid,
                                       lead_info=lead_info_from_db(engine), post_slack=post)
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


@router.api_route("/run", methods=["GET", "POST"])
def run_scheduled_abby(request: Request) -> dict:
    _require_scheduler_auth(request)
    details = run_abby()
    payload = {"status": "ok", "details": details, "enrich_via_abby": _run_enrich_after_abby()}
    backfill = _maybe_analytics_backfill()
    if backfill is not None:
        payload["analytics_backfill"] = backfill
    return payload
