"""HubSpot CRM sync for the fulfillment prospect pipeline.

When rate sheets are generated and prospects move through the pipeline,
this module keeps HubSpot companies + deals in sync — silently, in a
background thread, so HubSpot errors never break the main request flow.

Configuration (all optional — absent HUBSPOT_API_TOKEN = silent no-op):
  HUBSPOT_API_TOKEN          — private app token
  HUBSPOT_PIPELINE_ID        — deal pipeline ID (default: "default")
  HUBSPOT_STAGE_INTAKE       — stage ID for "intake"       (default: appointmentscheduled)
  HUBSPOT_STAGE_PENDING      — stage ID for "pending_fulfillment" (default: qualifiedtobuy)
  HUBSPOT_STAGE_COSTS        — stage ID for "costs_received"     (default: presentationscheduled)
  HUBSPOT_STAGE_PUBLISHED    — stage ID for "published"          (default: decisionmakerboughtin)
  HUBSPOT_STAGE_WON          — stage ID for "won"                (default: closedwon)
  HUBSPOT_STAGE_LOST         — stage ID for "lost"               (default: closedlost)
"""

from __future__ import annotations

import logging
import os
import threading
import time
from typing import Optional

import requests

logger = logging.getLogger(__name__)

_BASE = "https://api.hubapi.com"

# HubSpot association type IDs (v3 API, HubSpot-defined)
_ASSOC_DEAL_TO_COMPANY = "5"
_ASSOC_NOTE_TO_DEAL = "214"
_ASSOC_QUOTE_TO_DEAL = "64"
_ASSOC_LINE_ITEM_TO_QUOTE = "68"

# Our stage → HubSpot default stage ID + configurable override env var
_STAGE_DEFAULTS = {
    "intake":               ("HUBSPOT_STAGE_INTAKE",     "appointmentscheduled"),
    "pending_fulfillment":  ("HUBSPOT_STAGE_PENDING",    "qualifiedtobuy"),
    "costs_received":       ("HUBSPOT_STAGE_COSTS",      "presentationscheduled"),
    "published":            ("HUBSPOT_STAGE_PUBLISHED",  "decisionmakerboughtin"),
    "won":                  ("HUBSPOT_STAGE_WON",        "closedwon"),
    "lost":                 ("HUBSPOT_STAGE_LOST",       "closedlost"),
}


def _token() -> Optional[str]:
    return os.environ.get("HUBSPOT_API_TOKEN", "").strip() or None


def _headers() -> dict:
    return {"Authorization": f"Bearer {_token()}", "Content-Type": "application/json"}


def _pipeline_id() -> str:
    return os.environ.get("HUBSPOT_PIPELINE_ID", "default")


def _stage_id(stage: str) -> str:
    env_key, default = _STAGE_DEFAULTS.get(stage, ("", stage))
    return (os.environ.get(env_key, "").strip() or default) if env_key else default


# ---------------------------------------------------------------------------
# HubSpot API helpers
# ---------------------------------------------------------------------------

def _find_company(name: str) -> Optional[str]:
    try:
        r = requests.post(
            f"{_BASE}/crm/v3/objects/companies/search",
            headers=_headers(),
            json={
                "filterGroups": [{"filters": [{"propertyName": "name", "operator": "EQ", "value": name}]}],
                "properties": ["name"],
                "limit": 1,
            },
            timeout=10,
        )
        results = r.json().get("results") or []
        return results[0]["id"] if results else None
    except Exception:
        logger.exception("[hubspot] find_company failed for %r", name)
        return None


def _create_company(name: str, domain: str = "") -> Optional[str]:
    try:
        props: dict = {"name": name}
        if domain:
            props["domain"] = domain.lstrip("https://").lstrip("http://").split("/")[0]
        r = requests.post(
            f"{_BASE}/crm/v3/objects/companies",
            headers=_headers(),
            json={"properties": props},
            timeout=10,
        )
        return r.json().get("id")
    except Exception:
        logger.exception("[hubspot] create_company failed for %r", name)
        return None


def _find_deal(name: str) -> Optional[str]:
    try:
        r = requests.post(
            f"{_BASE}/crm/v3/objects/deals/search",
            headers=_headers(),
            json={
                "filterGroups": [{"filters": [{"propertyName": "dealname", "operator": "EQ", "value": name}]}],
                "properties": ["dealname"],
                "limit": 1,
            },
            timeout=10,
        )
        results = r.json().get("results") or []
        return results[0]["id"] if results else None
    except Exception:
        logger.exception("[hubspot] find_deal failed for %r", name)
        return None


def _create_deal(name: str, amount: float, stage: str, company_id: str, description: str = "") -> Optional[str]:
    try:
        props: dict = {
            "dealname": name,
            "amount": str(round(amount, 2)),
            "dealstage": _stage_id(stage),
            "pipeline": _pipeline_id(),
        }
        if description:
            props["description"] = description[:65_000]
        r = requests.post(
            f"{_BASE}/crm/v3/objects/deals",
            headers=_headers(),
            json={"properties": props},
            timeout=10,
        )
        deal_id = r.json().get("id")
        if deal_id and company_id:
            requests.put(
                f"{_BASE}/crm/v3/objects/deals/{deal_id}/associations/companies/{company_id}/{_ASSOC_DEAL_TO_COMPANY}",
                headers=_headers(),
                timeout=10,
            )
        return deal_id
    except Exception:
        logger.exception("[hubspot] create_deal failed for %r", name)
        return None


def _patch_deal(deal_id: str, props: dict) -> bool:
    try:
        r = requests.patch(
            f"{_BASE}/crm/v3/objects/deals/{deal_id}",
            headers=_headers(),
            json={"properties": props},
            timeout=10,
        )
        return r.status_code < 300
    except Exception:
        logger.exception("[hubspot] patch_deal failed for deal %s", deal_id)
        return False


def _add_note(deal_id: str, body: str) -> bool:
    try:
        r = requests.post(
            f"{_BASE}/crm/v3/objects/notes",
            headers=_headers(),
            json={"properties": {
                "hs_note_body": body[:65_000],
                "hs_timestamp": str(int(time.time() * 1000)),
            }},
            timeout=10,
        )
        note_id = r.json().get("id")
        if note_id:
            requests.put(
                f"{_BASE}/crm/v3/objects/notes/{note_id}/associations/deals/{deal_id}/{_ASSOC_NOTE_TO_DEAL}",
                headers=_headers(),
                timeout=10,
            )
        return bool(note_id)
    except Exception:
        logger.exception("[hubspot] add_note failed for deal %s", deal_id)
        return False


def _account_info() -> dict:
    """Fetch portal ID and UI domain from HubSpot account-info API."""
    try:
        r = requests.get(
            f"{_BASE}/account-info/v3/details",
            headers=_headers(),
            timeout=10,
        )
        return r.json()
    except Exception:
        logger.exception("[hubspot] failed to fetch account info")
        return {}


def _portal_id() -> Optional[str]:
    pid = os.environ.get("HUBSPOT_PORTAL_ID", "").strip()
    if pid:
        return pid
    return str(_account_info().get("portalId") or "") or None


def _hub_domain() -> str:
    """Return the correct HubSpot UI domain for this account (e.g. app-na2.hubspot.com)."""
    override = os.environ.get("HUBSPOT_UI_DOMAIN", "").strip()
    if override:
        return override
    return str(_account_info().get("uiDomain") or "app.hubspot.com")


def _create_line_item(label: str, qty: float, price: float, total: float, unit: str = "") -> Optional[str]:
    props: dict = {
        "name": label,
        "quantity": str(round(qty, 4)),
        "price": str(round(price, 4)),
        "amount": str(round(total, 2)),
    }
    if unit:
        props["description"] = f"per {unit}"
    try:
        r = requests.post(
            f"{_BASE}/crm/v3/objects/line_items",
            headers=_headers(),
            json={"properties": props},
            timeout=10,
        )
        lid = r.json().get("id")
        if not lid:
            logger.warning("[hubspot] line_item creation failed: %s", r.text[:200])
        return lid
    except Exception:
        logger.exception("[hubspot] create_line_item failed for %r", label)
        return None


def _create_quote(prospect: str, expiry: str, deal_id: str, line_item_ids: list) -> Optional[str]:
    # Include deal association directly in the creation payload — avoids
    # separate PUT calls and removes dependency on v3 association typeId guesses.
    payload: dict = {
        "properties": {
            "hs_title": f"{prospect} — 3PL Fulfillment Agreement",
            "hs_expiration_date": expiry,
            "hs_status": "DRAFT",
            "hs_esign_enabled": "true",
            "hs_template_type": "CUSTOMIZABLE_QUOTE_TEMPLATE",
            "hs_currency": "USD",
            "hs_language": "en",
        },
        "associations": [
            {
                "to": {"id": deal_id},
                "types": [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 64}],
            }
        ],
    }
    try:
        r = requests.post(
            f"{_BASE}/crm/v3/objects/quotes",
            headers=_headers(),
            json=payload,
            timeout=10,
        )
        quote_id = r.json().get("id")
        if not quote_id:
            logger.warning("[hubspot] quote creation returned no ID: %s", r.text[:500])
            return None
        # Associate line items via v4 API (explicit category avoids numeric typeId ambiguity)
        for li_id in line_item_ids:
            ar = requests.put(
                f"{_BASE}/crm/v4/objects/line_items/{li_id}/associations/quotes/{quote_id}",
                headers=_headers(),
                json=[{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": int(_ASSOC_LINE_ITEM_TO_QUOTE)}],
                timeout=10,
            )
            if ar.status_code >= 300:
                logger.warning("[hubspot] line_item→quote assoc failed li=%s q=%s: %s", li_id, quote_id, ar.text[:200])
        return quote_id
    except Exception:
        logger.exception("[hubspot] create_quote failed for %r", prospect)
        return None


def _get_deal_id(run_id: int) -> Optional[str]:
    from sales_support_agent.services.fulfillment_deck import storage
    run = storage.get_run(run_id)
    if run is None:
        return None
    return str((run.summary_json or {}).get("hubspot_deal_id") or "") or None


# ---------------------------------------------------------------------------
# Background executor
# ---------------------------------------------------------------------------

def _bg(fn, *args, **kwargs) -> None:
    threading.Thread(target=fn, args=args, kwargs=kwargs, daemon=True).start()


# ---------------------------------------------------------------------------
# Public sync functions
# ---------------------------------------------------------------------------

def _do_sync_new(run_id: int, prospect: str, website: str, stage: str, annual: float, brief: str) -> None:
    from sales_support_agent.services.fulfillment_deck import storage

    company_id = _find_company(prospect) or _create_company(prospect, website)
    deal_name = f"{prospect} — 3PL Fulfillment"
    deal_id = _find_deal(deal_name) or _create_deal(deal_name, annual, stage, company_id or "", brief)

    if deal_id:
        storage.update_summary(run_id, {
            "hubspot_deal_id": deal_id,
            "hubspot_company_id": company_id,
        })
        logger.info("[hubspot] synced run %d → deal %s company %s", run_id, deal_id, company_id)
    else:
        logger.warning("[hubspot] could not create deal for run %d", run_id)


def sync_new_prospect(run_id: int, summary: dict, prospect_profile: dict) -> None:
    """Call after a rate sheet is generated. Creates company + deal in HubSpot."""
    if not _token():
        return
    from sales_support_agent.services.fulfillment_deck.admin_page import _build_brief
    prospect = str(summary.get("prospect") or f"Run {run_id}")
    website = str(prospect_profile.get("website") or "")
    stage = str(summary.get("pipeline_stage") or "intake")
    pitched = float((summary.get("fulfillment_quote") or {}).get("monthly_total") or 0)
    brief = _build_brief({
        "id": run_id,
        "prospect": prospect,
        "origin_zip": summary.get("origin_zip"),
        "monthly_order_volume": prospect_profile.get("monthly_order_volume"),
        "prospect_profile": prospect_profile,
    })
    _bg(_do_sync_new, run_id, prospect, website, stage, round(pitched * 12, 2), brief)


def sync_stage(run_id: int, stage: str) -> None:
    """Call after a pipeline stage change. Updates the HubSpot deal stage."""
    if not _token():
        return

    def _do():
        deal_id = _get_deal_id(run_id)
        if not deal_id:
            logger.debug("[hubspot] no deal for run %d, skipping stage sync", run_id)
            return
        _patch_deal(deal_id, {"dealstage": _stage_id(stage)})
        logger.info("[hubspot] stage → %s for run %d deal %s", stage, run_id, deal_id)

    _bg(_do)


def _do_sync_quote(run_id: int) -> None:
    import datetime as _dt
    from sales_support_agent.services.fulfillment_deck import storage
    run = storage.get_run(run_id)
    if run is None:
        return
    summary = dict(run.summary_json or {})
    deal_id = str(summary.get("hubspot_deal_id") or "") or None

    # If no deal yet (run pre-dates HubSpot wiring), create one now then re-read.
    if not deal_id:
        prospect_profile = dict(summary.get("prospect_profile") or {})
        prospect = str(summary.get("prospect") or f"Run {run_id}")
        website = str(prospect_profile.get("website") or "")
        stage = str(summary.get("pipeline_stage") or "intake")
        pitched = float((summary.get("fulfillment_quote") or {}).get("monthly_total") or 0)
        from sales_support_agent.services.fulfillment_deck.admin_page import _build_brief
        brief = _build_brief({
            "id": run_id,
            "prospect": prospect,
            "origin_zip": summary.get("origin_zip"),
            "monthly_order_volume": prospect_profile.get("monthly_order_volume"),
            "prospect_profile": prospect_profile,
        })
        _do_sync_new(run_id, prospect, website, stage, round(pitched * 12, 2), brief)
        run = storage.get_run(run_id)
        if run is None:
            return
        summary = dict(run.summary_json or {})
        deal_id = str(summary.get("hubspot_deal_id") or "") or None

    if not deal_id:
        logger.warning("[hubspot] could not obtain deal for run %d, skipping quote", run_id)
        return

    fq = dict(summary.get("fulfillment_quote") or {})
    monthly_total = float(fq.get("monthly_total") or 0)
    if not monthly_total:
        logger.warning("[hubspot] no monthly total for run %d, skipping quote", run_id)
        return

    # Create one HubSpot line item per fulfillment quote line
    line_item_ids: list[str] = []
    for line in (fq.get("lines") or []):
        label = str(line.get("label") or "Service")
        qty = float(line.get("qty") or 1)
        monthly = float(line.get("monthly") or 0)
        rate = float(line.get("rate") or 0) or (monthly / qty if qty else monthly)
        unit = str(line.get("unit") or "")
        li_id = _create_line_item(label, qty, rate, monthly, unit)
        if li_id:
            line_item_ids.append(li_id)

    prospect = str(summary.get("prospect") or f"Run {run_id}")
    expiry_days = int(os.environ.get("HUBSPOT_QUOTE_EXPIRY_DAYS", "30"))
    expiry = (_dt.datetime.utcnow() + _dt.timedelta(days=expiry_days)).strftime("%Y-%m-%d")

    quote_id = _create_quote(prospect, expiry, deal_id, line_item_ids)
    if not quote_id:
        return

    portal_id = _portal_id() or ""
    hub_domain = _hub_domain()
    quote_url = (
        f"https://{hub_domain}/quotes/{portal_id}/quote/{quote_id}"
        if portal_id else ""
    )
    storage.update_summary(run_id, {
        "hubspot_quote_id": quote_id,
        "hubspot_quote_url": quote_url,
    })
    logger.info("[hubspot] quote %s for run %d url=%s", quote_id, run_id, quote_url)


def sync_quote(run_id: int) -> None:
    """Call after a rate sheet is published. Creates a HubSpot Quote with e-signature enabled."""
    if not _token():
        return
    _bg(_do_sync_quote, run_id)


def sync_margin(run_id: int, margin: dict, pitched: float) -> None:
    """Call after costs are saved. Updates deal amount + adds margin note."""
    if not _token():
        return

    def _do():
        deal_id = _get_deal_id(run_id)
        if not deal_id:
            return
        annual_margin = float(margin.get("annual_margin") or 0)
        if annual_margin > 0:
            _patch_deal(deal_id, {"amount": str(round(annual_margin, 2))})
        note = (
            f"Fulfillment cost analysis:\n"
            f"  Pitched: ${pitched:,.0f}/mo\n"
            f"  Pick & pack actual: −${margin.get('actual_pick_pack', 0):,.0f}\n"
            f"  Storage actual: −${margin.get('actual_storage', 0):,.0f}\n"
            f"  Tech fee actual: −${margin.get('actual_tech_fee', 0):,.0f}\n"
            f"  Monthly margin: ${margin.get('monthly_margin', 0):,.0f} "
            f"({margin.get('margin_pct', 0)}%)\n"
            f"  Annual margin: ${margin.get('annual_margin', 0):,.0f}"
        )
        _add_note(deal_id, note)
        logger.info("[hubspot] margin note added for run %d deal %s", run_id, deal_id)

    _bg(_do)
