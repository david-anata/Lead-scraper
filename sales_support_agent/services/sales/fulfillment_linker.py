"""Auto-link published fulfillment rate sheets to open HubSpot deals.

Scans all ``AutomationRun`` rows with ``run_type="fulfillment_rate_sheet"``
that have a published ``view_path``, then upserts a ``SalesDealAsset``
(``asset_type="rate_sheet"``) for each one.

Matching priority:
  1. ``summary_json.hubspot_deal_id`` — explicit manual link from the review UI.
  2. ``summary_json.prospect`` — fuzzy brand/company-name match using the same
     normalisation as ``asset_linker._normalize``.

Existing links are refreshed (URL updated) so a re-publish doesn't orphan the
deal card. Runs with no view_path (draft, failed, or not yet published) are
silently skipped.
"""

from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from sales_support_agent.models.entities import AutomationRun, SalesDealAsset
from sales_support_agent.services.sales.asset_linker import (
    link_asset_to_deal,
    try_link_asset,
)

logger = logging.getLogger(__name__)

_RATE_SHEET_RUN_TYPE = "fulfillment_rate_sheet"


def sync_fulfillment_links(session: Session) -> dict[str, int]:
    """Scan all rate-sheet runs and ensure each has a SalesDealAsset row.

    Returns a dict with counts: linked, refreshed, skipped.
    """
    runs = session.scalars(
        select(AutomationRun)
        .where(AutomationRun.run_type == _RATE_SHEET_RUN_TYPE)
        .order_by(AutomationRun.started_at.desc())
        .limit(500)
    ).all()

    linked = refreshed = skipped = 0

    for run in runs:
        summary = dict(run.summary_json or {})
        view_path = str(summary.get("view_path") or "").strip()
        if not view_path:
            skipped += 1
            continue

        prospect = str(summary.get("prospect") or "").strip()
        explicit_deal_id = str(summary.get("hubspot_deal_id") or "").strip()
        label = prospect or f"Rate Sheet #{run.id}"
        run_id = str(run.id)

        existing = (
            session.query(SalesDealAsset)
            .filter_by(asset_type="rate_sheet", run_id=run_id)
            .first()
        )

        if explicit_deal_id:
            result = link_asset_to_deal(
                session,
                hubspot_deal_id=explicit_deal_id,
                asset_type="rate_sheet",
                run_id=run_id,
                url=view_path,
                label=label,
                overwrite=True,
            )
            if result:
                if existing:
                    refreshed += 1
                else:
                    linked += 1
            else:
                skipped += 1
        elif prospect:
            result = try_link_asset(
                session,
                brand_name=prospect,
                run_id=run_id,
                url=view_path,
                asset_type="rate_sheet",
                label=label,
            )
            if result:
                if existing:
                    refreshed += 1
                else:
                    linked += 1
            else:
                skipped += 1
        else:
            skipped += 1

    logger.info(
        "[fulfillment_linker] sync complete: %d linked, %d refreshed, %d skipped",
        linked, refreshed, skipped,
    )
    return {"linked": linked, "refreshed": refreshed, "skipped": skipped}
