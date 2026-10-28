"""Scheduler entry point for Abby (see abby_runner). Vercel Cron, every 5 minutes."""

from __future__ import annotations

import logging
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


@router.api_route("/run", methods=["GET", "POST"])
def run_scheduled_abby(request: Request) -> dict:
    _require_scheduler_auth(request)
    return {"status": "ok", "details": run_abby()}
