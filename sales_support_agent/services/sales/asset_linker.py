"""Auto-link generated assets (rate sheets, decks, ads audits) to open HubSpot deals.

When a rate sheet is published, call `try_link_rate_sheet`.  The function
does a case/punctuation-normalised containment match against open deal names
and their associated company names, then upserts a `SalesDealAsset` row.

Returns the matched deal_id string, or None if no match was found.
"""

from __future__ import annotations

import logging
import re
from typing import Optional

from sqlalchemy.orm import Session

from sales_support_agent.models.entities import HubSpotCompany, HubSpotDeal, SalesDealAsset

logger = logging.getLogger(__name__)

_STRIP_SUFFIXES = re.compile(
    r"\b(inc\.?|llc\.?|ltd\.?|corp\.?|co\.?|company|group|holdings|partners|llp)\b",
    re.IGNORECASE,
)
_NONALPHA = re.compile(r"[^a-z0-9 ]+")


def _normalize(name: str) -> str:
    name = name.lower().strip()
    name = _STRIP_SUFFIXES.sub("", name)
    name = _NONALPHA.sub(" ", name)
    return " ".join(name.split())


def _matches(brand: str, deal_name: str, company_name: str) -> bool:
    nb = _normalize(brand)
    nd = _normalize(deal_name)
    nc = _normalize(company_name)
    if not nb:
        return False
    nb_words = set(nb.split())
    nd_words = set(nd.split())
    nc_words = set(nc.split()) if nc else set()
    return nb_words.issubset(nd_words) or nb_words.issubset(nc_words)


def try_link_rate_sheet(
    session: Session,
    brand_name: str,
    run_id: int,
    url: str,
    label: str = "Rate Sheet",
) -> Optional[str]:
    """Find the best open deal for *brand_name* and upsert a rate_sheet asset link.

    Returns the deal_id of the matched deal, or None if no match was found.
    """
    if not brand_name.strip():
        return None

    open_deals = (
        session.query(HubSpotDeal)
        .filter(HubSpotDeal.is_closed.is_(False))
        .all()
    )

    company_names: dict[str, str] = {}
    for deal in open_deals:
        if deal.hubspot_company_id and deal.hubspot_company_id not in company_names:
            co = session.get(HubSpotCompany, deal.hubspot_company_id)
            company_names[deal.hubspot_company_id] = co.name if co else ""

    matched_deal_id: Optional[str] = None
    for deal in open_deals:
        co_name = company_names.get(deal.hubspot_company_id, "")
        if _matches(brand_name, deal.deal_name, co_name):
            matched_deal_id = deal.hubspot_deal_id
            break

    if matched_deal_id is None:
        logger.debug("[asset_linker] no open deal matched brand=%r", brand_name)
        return None

    existing = (
        session.query(SalesDealAsset)
        .filter_by(hubspot_deal_id=matched_deal_id, asset_type="rate_sheet", run_id=str(run_id))
        .first()
    )
    if existing:
        existing.url = url
        existing.label = label
    else:
        session.add(SalesDealAsset(
            hubspot_deal_id=matched_deal_id,
            asset_type="rate_sheet",
            run_id=str(run_id),
            url=url,
            label=label,
        ))

    logger.info("[asset_linker] linked rate_sheet run=%s to deal=%s", run_id, matched_deal_id)
    return matched_deal_id
