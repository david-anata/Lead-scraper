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
    auto_close_dates_fixed: int = 0
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
            "auto_close_dates_fixed": self.auto_close_dates_fixed,
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


def _best_signal(timestamps: list[Optional[datetime]]) -> Optional[datetime]:
    """Return the most recent non-None timestamp from a list."""
    valids = [t for t in timestamps if t is not None]
    return max(valids) if valids else None


def _make_aware(dt: datetime) -> datetime:
    """Ensure a datetime is timezone-aware (UTC if naive)."""
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)


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

    # Native HubSpot email signals — used as fallbacks when Gmail matching hasn't
    # populated these fields yet. Never overwrite a more-recent value set by the
    # Gmail signal job.
    native_outbound = _parse_dt(p.get("hs_email_last_send_date"))
    native_inbound = _best_signal([
        _parse_dt(p.get("hs_email_last_replied")),
        _parse_dt(p.get("hs_sales_email_last_opened")),
    ])
    native_activity = _best_signal([
        _parse_dt(p.get("hs_last_sales_activity_date")),
        _parse_dt(p.get("notes_last_updated")),
    ])

    # Only backfill if the existing mirror value is None or the native value is more recent.
    if native_outbound and (
        row.last_outbound_at is None
        or native_outbound > _make_aware(row.last_outbound_at)
    ):
        row.last_outbound_at = native_outbound
    if native_inbound and (
        row.last_inbound_at is None
        or native_inbound > _make_aware(row.last_inbound_at)
    ):
        row.last_inbound_at = native_inbound

    # last_meaningful_touch_at = best of native activity, outbound, inbound
    native_touch = _best_signal([native_outbound, native_inbound, native_activity])
    if native_touch and (
        row.last_meaningful_touch_at is None
        or native_touch > _make_aware(row.last_meaningful_touch_at)
    ):
        row.last_meaningful_touch_at = native_touch

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

    amount_str = f"{li_total / 100:.2f}"
    try:
        client.update_deal(deal_id, {"amount": amount_str})
        deal.amount_cents = int(li_total)
        logger.info("[hubspot_sync] auto-synced amount for deal %s → $%s", deal_id, amount_str)
        result.auto_amount_synced += 1
    except Exception as exc:  # noqa: BLE001
        logger.warning("[hubspot_sync] auto-sync amount failed for deal %s: %s", deal_id, exc)


def _maybe_fix_close_date(
    session: Session,
    client: HubSpotClient,
    deal_id: str,
    as_of: datetime,
    result: HubSpotSyncResult,
) -> None:
    """Auto-push past-due or missing close dates. Safe: deterministic, reversible.

    HubSpot's closedate is the *expected* close date, not actual close. An open
    deal with a past close_date is always data hygiene — the rep forgot to push it.
    Grace period: ≤3 days overdue + touched within 3 days → skip (rep may be
    actively negotiating a verbal commitment and hasn't updated the date yet).
    """
    from datetime import timedelta

    deal = session.get(HubSpotDeal, deal_id)
    if deal is None or deal.is_closed:
        return

    new_date = (as_of + timedelta(days=30)).replace(hour=0, minute=0, second=0, microsecond=0)
    new_ts = str(int(new_date.timestamp() * 1000))

    if deal.close_date is None:
        reason = "missing"
    else:
        close = deal.close_date if deal.close_date.tzinfo else deal.close_date.replace(tzinfo=timezone.utc)
        overdue_days = (as_of - close).days
        if overdue_days <= 0:
            return  # Future or today — nothing to do

        # Grace: barely overdue + rep is actively working it → let them update manually
        if overdue_days <= 3 and deal.last_meaningful_touch_at is not None:
            last_touch = (
                deal.last_meaningful_touch_at
                if deal.last_meaningful_touch_at.tzinfo
                else deal.last_meaningful_touch_at.replace(tzinfo=timezone.utc)
            )
            if (as_of - last_touch).days <= 3:
                return

        reason = f"overdue {overdue_days}d"

    try:
        client.update_deal(deal_id, {"closedate": new_ts})
        deal.close_date = new_date
        logger.info(
            "[hubspot_sync] auto-fixed close date for deal %s (%s) → %s",
            deal_id, reason, new_date.strftime("%Y-%m-%d"),
        )
        result.auto_close_dates_fixed += 1
    except Exception as exc:  # noqa: BLE001
        logger.warning("[hubspot_sync] auto-fix close date failed for deal %s: %s", deal_id, exc)


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
    as_of = datetime.now(timezone.utc)
    result = HubSpotSyncResult(started_at=as_of.isoformat())
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

    # Cache ordered stage list for stage-move proposals (best-effort, separate call).
    try:
        _cache_pipeline_stage_order(client)
    except Exception as exc:  # noqa: BLE001
        logger.warning("[hubspot_sync] pipeline stage order cache failed: %s", exc)

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

            contacts_ok = True
            try:
                deal_contacts = client.list_associations("deals", deal_id, "contacts")
            except Exception as exc:  # noqa: BLE001
                contacts_ok = False
                deal_contacts = []
                result.errors.append(f"deal {deal_id} contacts: {exc}")
            contact_ids.update(deal_contacts)
            if contacts_ok:
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

            # High-confidence auto-fixes: deterministic data hygiene that never
            # requires business judgment. Flush first so reads see the upserted row.
            session.flush()
            _maybe_sync_amount(session, client, deal_id, result)
            _maybe_fix_close_date(session, client, deal_id, as_of, result)
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


def _cache_pipeline_stage_order(client: HubSpotClient) -> None:
    """Cache ordered pipeline stages to KV for stage-move proposals."""
    from sales_support_agent.models.database import kv_set_json

    raw_pipelines = client.list_deal_pipelines()
    pipeline_stage_order: dict[str, list[dict]] = {}
    for pipeline in raw_pipelines:
        pid = str(pipeline.get("id") or "").strip()
        if not pid:
            continue
        stages_sorted = sorted(
            pipeline.get("stages", []) or [],
            key=lambda s: int(s.get("displayOrder") or 9999),
        )
        pipeline_stage_order[pid] = [
            {"id": str(s.get("id") or "").strip(), "label": str(s.get("label") or "").strip()}
            for s in stages_sorted
            if str(s.get("id") or "").strip()
        ]
    if pipeline_stage_order:
        kv_set_json("hubspot:pipeline_stages", pipeline_stage_order)


def _company_props():
    from sales_support_agent.integrations.hubspot import COMPANY_PROPERTIES

    return COMPANY_PROPERTIES


def _contact_props():
    from sales_support_agent.integrations.hubspot import CONTACT_PROPERTIES

    return CONTACT_PROPERTIES


def _chunks(items: list[str], size: int):
    for i in range(0, len(items), size):
        yield items[i : i + size]
