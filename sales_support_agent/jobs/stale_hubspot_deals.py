"""Stale HubSpot deal scanning job.

Phase 6b of the Sales Operational Director. Scans every open HubSpotDeal for
touch-staleness (no inbound signal from the prospect in > stale_deal_days) and
sends a Slack digest without any ClickUp dependency.

This runs alongside (not instead of) StaleLeadJob until ClickUp is fully retired.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from sales_support_agent.config import Settings
from sales_support_agent.integrations.slack import SlackClient
from sales_support_agent.models.entities import HubSpotCompany, HubSpotDeal

logger = logging.getLogger(__name__)


@dataclass
class StaleDealsResult:
    total_open: int = 0
    stale_count: int = 0
    digest_posted: bool = False
    dry_run: bool = False


def _days_since(dt: datetime | None, as_of: datetime) -> int | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return max(0, int((as_of - dt).total_seconds() // 86400))


def _build_digest_blocks(
    stale_deals: list[dict[str, Any]],
    *,
    as_of: datetime,
    stale_days: int,
) -> list[dict[str, Any]]:
    def _touch(d: dict[str, Any]) -> str:
        n = d["days_since_inbound"]
        return "no inbound ever" if n is None else f"last inbound {n}d ago"

    lines = [
        f"*{d['name']}* — {d['stage'] or 'unknown stage'} | owner: {d['owner'] or '—'} | {_touch(d)}"
        for d in stale_deals[:20]
    ]
    more = len(stale_deals) - 20
    summary = f"{len(stale_deals)} open deal{'s' if len(stale_deals) != 1 else ''} with no inbound reply in >{stale_days} days."
    if more > 0:
        summary += f" (showing 20 of {len(stale_deals)})"
    return [
        {"type": "section", "text": {"type": "mrkdwn", "text": f":warning: *Stale HubSpot Deals — {summary}*"}},
        {"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(f"• {l}" for l in lines)}},
        {"type": "context", "elements": [{"type": "mrkdwn", "text": f"Sales Operational Director · stale threshold: {stale_days} days"}]},
    ]


class StaleHubSpotDealsJob:
    def __init__(self, settings: Settings, slack_client: SlackClient, session: Session):
        self.settings = settings
        self.slack_client = slack_client
        self.session = session

    def run(self, *, dry_run: bool = False, as_of: datetime | None = None) -> StaleDealsResult:
        as_of = (as_of or datetime.now(timezone.utc)).replace(tzinfo=timezone.utc) if (as_of is None or (as_of and as_of.tzinfo is None)) else as_of
        stale_days = self.settings.stale_deal_days
        stale_cutoff = as_of - timedelta(days=stale_days)

        company_names = dict(self.session.execute(
            select(HubSpotCompany.hubspot_company_id, HubSpotCompany.name)
        ).all())

        open_deals = list(self.session.scalars(
            select(HubSpotDeal)
            .where(HubSpotDeal.is_closed.is_(False))
            .order_by(HubSpotDeal.close_date.asc().nulls_last())
        ).all())

        stale: list[dict[str, Any]] = []
        for d in open_deals:
            li = d.last_inbound_at
            if li is not None and li.tzinfo is None:
                li = li.replace(tzinfo=timezone.utc)
            is_stale = li is None or li < stale_cutoff
            if is_stale:
                company = company_names.get(d.hubspot_company_id or "", "")
                stale.append({
                    "deal_id": d.hubspot_deal_id,
                    "name": f"{d.deal_name or d.hubspot_deal_id}{(' — ' + company) if company else ''}",
                    "stage": d.deal_stage_label or d.deal_stage or "",
                    "owner": d.owner_email or "",
                    "days_since_inbound": _days_since(li, as_of),
                })

        result = StaleDealsResult(
            total_open=len(open_deals),
            stale_count=len(stale),
            dry_run=dry_run,
        )

        if not stale:
            logger.info("[stale_hs_deals] no stale deals found (threshold: %d days)", stale_days)
            return result

        logger.info("[stale_hs_deals] %d/%d open deals are stale", len(stale), len(open_deals))

        if dry_run or not self.settings.stale_deal_slack_digest_enabled:
            return result

        if not self.slack_client.is_configured:
            logger.warning("[stale_hs_deals] Slack not configured; skipping digest")
            return result

        try:
            blocks = _build_digest_blocks(stale, as_of=as_of, stale_days=stale_days)
            text = f":warning: {len(stale)} stale HubSpot deal(s) (no inbound >{stale_days}d)"
            self.slack_client.post_message(text=text, blocks=blocks)
            result.digest_posted = True
        except Exception:
            logger.exception("[stale_hs_deals] Slack digest failed")

        return result
