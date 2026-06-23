"""Deal-centric HubSpot → local mirror sync.

Strategy: iterate deals (the sales unit of work), and for each deal pull its
associated company, contacts, and line items. We deliberately do NOT mirror the
entire contact/company database — only what's attached to a deal — to keep the
sync bounded and relevant to the sales workflow.

Money is normalized to integer **cents**. Dates accept either ISO-8601 strings
or epoch-millisecond strings (HubSpot uses both depending on the property).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from sales_support_agent.config import Settings
from sales_support_agent.integrations.hubspot import HubSpotClient
from sales_support_agent.models.entities import (
    HubSpotCompany,
    HubSpotContact,
    HubSpotDeal,
    HubSpotDealContact,
    HubSpotLineItem,
)

logger = logging.getLogger(__name__)

SYNC_STATE_KEY = "hubspot:sync:sales:state"

# HubSpot deal stages that are not "open". Falls back to the hs_is_closed
# property when present; this list catches the standard pipeline labels.
_CLOSED_STAGE_HINTS = ("closedwon", "closedlost")


@dataclass
class HubSpotSyncResult:
    deals: int = 0
    companies: int = 0
    contacts: int = 0
    line_items: int = 0
    auto_amount_synced: int = 0
    errors: list[str] = field(default_factory=list)
    started_at: Optional[str] = None
    completed_at: Optional[str] = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "deals": self.deals,
            "companies": self.companies,
            "contacts": self.contacts,
            "line_items": self.line_items,
            "auto_amount_synced": self.auto_amount_synced,
            "errors": list(self.errors),
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "ok": not self.errors,
        }


def _to_cents(value: Any) -> int:
    """HubSpot amounts are decimal strings/numbers in dollars → integer cents."""
    if value in (None, ""):
        return 0
    try:
        return int(round(float(value) * 100))
    except (TypeError, ValueError):
        return 0


def _to_int(value: Any) -> int:
    if value in (None, ""):
        return 0
    try:
        return int(round(float(value)))
    except (TypeError, ValueError):
        return 0


def _parse_dt(value: Any) -> Optional[datetime]:
    """Parse a HubSpot timestamp: ISO-8601 string or epoch-ms string/number."""
    if value in (None, ""):
        return None
    # Epoch milliseconds (HubSpot returns these for closedate/createdate often).
    try:
        as_num = float(value)
        # Heuristic: values this large are epoch ms, not a year.
        if as_num > 1_000_000_000_000:
            return datetime.fromtimestamp(as_num / 1000.0, tz=timezone.utc)
        if as_num > 1_000_000_000:  # epoch seconds
            return datetime.fromtimestamp(as_num, tz=timezone.utc)
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        # Date-only ISO (YYYY-MM-DD)
        try:
            dt = datetime.strptime(text[:10], "%Y-%m-%d")
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            return None


def _props(obj: dict[str, Any]) -> dict[str, Any]:
    return dict(obj.get("properties") or {})


def _is_closed(props: dict[str, Any]) -> tuple[bool, bool]:
    """Return (is_closed, is_won) from HubSpot deal properties."""
    raw_closed = str(props.get("hs_is_closed") or "").strip().lower()
    raw_won = str(props.get("hs_is_closed_won") or "").strip().lower()
    stage = str(props.get("dealstage") or "").strip().lower()
    is_won = raw_won in ("true", "1", "yes") or stage == "closedwon"
    is_closed = (
        raw_closed in ("true", "1", "yes")
        or is_won
        or stage in _CLOSED_STAGE_HINTS
    )
    return is_closed, is_won


def _upsert_deal(
    session: Session,
    obj: dict[str, Any],
    owner_emails: dict[str, str],
    *,
    company_id: str = "",
    stage_labels: dict[str, str] | None = None,
) -> None:
    deal_id = str(obj.get("id") or "").strip()
    if not deal_id:
        return
    p = _props(obj)
    is_closed, is_won = _is_closed(p)
    owner_id = str(p.get("hubspot_owner_id") or "").strip()
    stage_id = str(p.get("dealstage") or "")
    row = session.get(HubSpotDeal, deal_id) or HubSpotDeal(hubspot_deal_id=deal_id)
    row.deal_name = str(p.get("dealname") or "")
    row.amount_cents = _to_cents(p.get("amount"))
    row.deal_stage = stage_id
    row.deal_stage_label = (stage_labels or {}).get(stage_id, "")
    row.pipeline = str(p.get("pipeline") or "")
    row.close_date = _parse_dt(p.get("closedate"))
    row.owner_id = owner_id
    row.owner_email = owner_emails.get(owner_id, row.owner_email or "")
    if company_id:
        row.hubspot_company_id = company_id
    row.is_closed = is_closed
    row.is_won = is_won
    row.created_at = _parse_dt(p.get("createdate"))
    row.updated_at = _parse_dt(p.get("hs_lastmodifieddate"))
    row.description = str(p.get("description") or "")
    row.raw_properties = p
    row.last_sync_at = datetime.now(timezone.utc)
    session.add(row)


def _upsert_company(session: Session, obj: dict[str, Any]) -> None:
    cid = str(obj.get("id") or "").strip()
    if not cid:
        return
    p = _props(obj)
    row = session.get(HubSpotCompany, cid) or HubSpotCompany(hubspot_company_id=cid)
    row.name = str(p.get("name") or "")
    row.domain = str(p.get("domain") or "")
    row.industry = str(p.get("industry") or "")
    row.city = str(p.get("city") or "")
    row.state = str(p.get("state") or "")
    row.raw_properties = p
    row.last_sync_at = datetime.now(timezone.utc)
    session.add(row)


def _upsert_contact(session: Session, obj: dict[str, Any]) -> None:
    cid = str(obj.get("id") or "").strip()
    if not cid:
        return
    p = _props(obj)
    row = session.get(HubSpotContact, cid) or HubSpotContact(hubspot_contact_id=cid)
    row.hubspot_company_id = str(p.get("associatedcompanyid") or "")
    row.first_name = str(p.get("firstname") or "")
    row.last_name = str(p.get("lastname") or "")
    row.email = str(p.get("email") or "")
    row.phone = str(p.get("phone") or "")
    row.job_title = str(p.get("jobtitle") or "")
    row.raw_properties = p
    row.last_sync_at = datetime.now(timezone.utc)
    session.add(row)


def _upsert_line_item(session: Session, obj: dict[str, Any], deal_id: str) -> None:
    lid = str(obj.get("id") or "").strip()
    if not lid:
        return
    p = _props(obj)
    row = session.get(HubSpotLineItem, lid) or HubSpotLineItem(hubspot_line_item_id=lid)
    row.hubspot_deal_id = deal_id
    row.name = str(p.get("name") or "")
    row.quantity = _to_int(p.get("quantity"))
    row.unit_price_cents = _to_cents(p.get("price"))
    row.amount_cents = _to_cents(p.get("amount"))
    row.raw_properties = p
    row.last_sync_at = datetime.now(timezone.utc)
    session.add(row)


def _maybe_sync_amount(
    session: Session,
    client: HubSpotClient,
    deal_id: str,
    result: HubSpotSyncResult,
) -> None:
    """High-confidence auto-fix: set deal amount from line items when it's $0."""
    from sqlalchemy import func as sa_func, select as sa_select

    deal = session.get(HubSpotDeal, deal_id)
    if deal is None or deal.is_closed or (deal.amount_cents or 0) > 0:
        return

    li_total = session.execute(
        sa_select(sa_func.sum(HubSpotLineItem.amount_cents))
        .where(HubSpotLineItem.hubspot_deal_id == deal_id)
    ).scalar() or 0

    if li_total <= 0:
        return

    amount_str = str(round(li_total / 100, 2))
    try:
        client.update_deal(deal_id, {"amount": amount_str})
        deal.amount_cents = int(li_total)
        logger.info("[hubspot_sync] auto-synced amount for deal %s → $%s", deal_id, amount_str)
        result.auto_amount_synced = getattr(result, "auto_amount_synced", 0) + 1
    except Exception as exc:  # noqa: BLE001
        logger.warning("[hubspot_sync] auto-sync amount failed for deal %s: %s", deal_id, exc)


def _replace_deal_contacts(session: Session, deal_id: str, contact_ids: list[str]) -> None:
    existing = session.scalars(
        select(HubSpotDealContact).where(HubSpotDealContact.hubspot_deal_id == deal_id)
    ).all()
    have = {r.hubspot_contact_id for r in existing}
    want = set(contact_ids)
    for row in existing:
        if row.hubspot_contact_id not in want:
            session.delete(row)
    for cid in want - have:
        session.add(HubSpotDealContact(hubspot_deal_id=deal_id, hubspot_contact_id=cid))


def sync_hubspot_sales(
    session: Session,
    client: HubSpotClient,
    settings: Settings,
    *,
    max_deals: int | None = None,
) -> HubSpotSyncResult:
    """Sync deals + their associated companies/contacts/line items into mirrors."""
    result = HubSpotSyncResult(started_at=datetime.now(timezone.utc).isoformat())
    if not client.is_configured:
        result.errors.append("HUBSPOT_API_TOKEN is not configured.")
        result.completed_at = datetime.now(timezone.utc).isoformat()
        return result

    # Owner id -> email map (best-effort; deal board shows owner email).
    owner_emails: dict[str, str] = {}
    try:
        for owner in client.list_owners():
            oid = str(owner.get("id") or "").strip()
            email = str(owner.get("email") or "").strip()
            if oid:
                owner_emails[oid] = email
    except Exception as exc:  # noqa: BLE001 — owners are non-critical
        logger.warning("[hubspot_sync] owners fetch failed: %s", exc)

    # Stage id -> human label map (best-effort; raw stage ids are unreadable).
    stage_labels: dict[str, str] = {}
    try:
        stage_labels = client.deal_stage_labels()
    except Exception as exc:  # noqa: BLE001 — labels are non-critical
        logger.warning("[hubspot_sync] pipeline labels fetch failed: %s", exc)

    company_ids: set[str] = set()
    contact_ids: set[str] = set()

    try:
        for deal in client.iter_deals(max_records=max_deals):
            deal_id = str(deal.get("id") or "").strip()
            if not deal_id:
                continue

            # Resolve associations first so the deal row is written with its
            # company in one pass: company (one), contacts (many), line items.
            try:
                deal_companies = client.list_associations("deals", deal_id, "companies")
            except Exception as exc:  # noqa: BLE001
                deal_companies = []
                result.errors.append(f"deal {deal_id} companies: {exc}")
            primary_company = deal_companies[0] if deal_companies else ""
            if primary_company:
                company_ids.add(primary_company)

            _upsert_deal(
                session, deal, owner_emails,
                company_id=primary_company, stage_labels=stage_labels,
            )
            result.deals += 1

            try:
                deal_contacts = client.list_associations("deals", deal_id, "contacts")
            except Exception as exc:  # noqa: BLE001
                deal_contacts = []
                result.errors.append(f"deal {deal_id} contacts: {exc}")
            contact_ids.update(deal_contacts)
            _replace_deal_contacts(session, deal_id, deal_contacts)

            try:
                li_ids = client.list_associations("deals", deal_id, "line_items")
            except Exception as exc:  # noqa: BLE001
                li_ids = []
                result.errors.append(f"deal {deal_id} line_items: {exc}")
            if li_ids:
                for li in client.get_line_items(li_ids):
                    _upsert_line_item(session, li, deal_id)
                    result.line_items += 1

            # High-confidence auto-fix: deal amount is $0 but line items exist.
            # This is unambiguous — write back without waiting for rep approval.
            session.flush()
            _maybe_sync_amount(session, client, deal_id, result)
    except Exception as exc:  # noqa: BLE001
        logger.exception("[hubspot_sync] deal iteration failed")
        result.errors.append(f"deal sync: {exc}")

    # Batch-read referenced companies and contacts (100 per call).
    try:
        for batch in _chunks(sorted(company_ids), 100):
            for obj in client.batch_read("companies", batch, properties=_company_props()):
                _upsert_company(session, obj)
                result.companies += 1
    except Exception as exc:  # noqa: BLE001
        result.errors.append(f"company sync: {exc}")

    try:
        for batch in _chunks(sorted(contact_ids), 100):
            for obj in client.batch_read("contacts", batch, properties=_contact_props()):
                _upsert_contact(session, obj)
                result.contacts += 1
    except Exception as exc:  # noqa: BLE001
        result.errors.append(f"contact sync: {exc}")

    result.completed_at = datetime.now(timezone.utc).isoformat()
    return result


def _company_props():
    from sales_support_agent.integrations.hubspot import COMPANY_PROPERTIES

    return COMPANY_PROPERTIES


def _contact_props():
    from sales_support_agent.integrations.hubspot import CONTACT_PROPERTIES

    return CONTACT_PROPERTIES


def _chunks(items: list[str], size: int):
    for i in range(0, len(items), size):
        yield items[i : i + size]
