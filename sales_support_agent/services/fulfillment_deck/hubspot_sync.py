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

# HubSpot association type IDs (v3 API)
_ASSOC_DEAL_TO_COMPANY = "5"
_ASSOC_NOTE_TO_DEAL = "214"

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
