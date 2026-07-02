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
    for name in ("HUBSPOT_API_TOKEN", "HUBSPOT_PRIVATE_APP_TOKEN", "HUBSPOT_ACCESS_TOKEN", "HS_PRIVATE_APP_TOKEN"):
        value = os.environ.get(name, "").strip()
        if value:
            return value
    return None


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


def _lookup_owner_id(email: str) -> Optional[str]:
    """Return the HubSpot owner ID for the given email, or None."""
    if not email:
        return None
    try:
        r = requests.get(
            f"{_BASE}/crm/v3/owners",
            headers=_headers(),
            params={"email": email, "limit": 1},
            timeout=10,
        )
        results = r.json().get("results") or []
        return str(results[0]["id"]) if results else None
    except Exception:
        logger.exception("[hubspot] owner lookup failed for %r", email)
        return None


def _hub_domain() -> str:
    """Return the correct HubSpot UI domain for this account (e.g. app-na2.hubspot.com)."""
    override = os.environ.get("HUBSPOT_UI_DOMAIN", "").strip()
    if override:
        return override
    return str(_account_info().get("uiDomain") or "app.hubspot.com")


# Normalise plural unit strings to readable "per X" descriptions
_UNIT_MAP: dict[str, str] = {
    "orders": "order",
    "pallets": "pallet",
    "pallet/mo": "pallet/month",
    "flat": "month",
    "units": "unit",
    "one-time": "one-time",
    "per occurrence": "occurrence",
}


def _unit_label(unit: str) -> str:
    return _UNIT_MAP.get(unit.strip(), unit.strip())


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


def _create_quote(
    prospect: str,
    expiry: str,
    deal_id: str,
    line_item_ids: list,
    owner_id: Optional[str] = None,
) -> Optional[str]:
    template_type = os.environ.get("HUBSPOT_QUOTE_TEMPLATE_TYPE", "DEFAULT_QUOTE_TEMPLATE").strip()
    props: dict = {
        "hs_title": f"{prospect} — 3PL Fulfillment Agreement",
        "hs_expiration_date": expiry,
        "hs_status": "DRAFT",
        "hs_esign_enabled": "true",
        "hs_template_type": template_type,
        "hs_currency": "USD",
        "hs_language": "en",
        "hs_sender_company_name": "Anata",
        "hs_sender_company_domain": "anatainc.com",
    }
    if owner_id:
        props["hubspot_owner_id"] = owner_id
    # Include deal association directly in the creation payload — avoids
    # separate PUT calls and removes dependency on v3 association typeId guesses.
    payload: dict = {
        "properties": props,
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


def _monthly_mrr(fulfillment_quote: dict) -> float:
    """Monthly recurring revenue for HubSpot deal amount.

    One-time fees are intentionally excluded. Carrier/shipping pass-through is
    also excluded because it is not marginable top-line revenue for the sales
    deal amount standard.
    """
    monthly_total = float((fulfillment_quote or {}).get("monthly_total") or 0)
    pass_through = 0.0
    for line in (fulfillment_quote or {}).get("lines") or []:
        if isinstance(line, dict) and str(line.get("key") or "") == "shipping":
            try:
                pass_through += float(line.get("monthly") or 0)
            except (TypeError, ValueError):
                pass
    return round(max(monthly_total - pass_through, 0.0), 2)


# ---------------------------------------------------------------------------
# Background executor
# ---------------------------------------------------------------------------

def _bg(fn, *args, **kwargs) -> None:
    threading.Thread(target=fn, args=args, kwargs=kwargs, daemon=True).start()


# ---------------------------------------------------------------------------
# Public sync functions
# ---------------------------------------------------------------------------

def _do_sync_new(run_id: int, prospect: str, website: str, stage: str, monthly_mrr: float, brief: str) -> None:
    from sales_support_agent.services.fulfillment_deck import storage

    run = storage.get_run(run_id)
    existing_summary = dict(run.summary_json or {}) if run is not None else {}
    existing_deal_id = str(existing_summary.get("hubspot_deal_id") or "").strip()
    if existing_deal_id:
        logger.info("[hubspot] run %d already linked to deal %s; skipping new deal create", run_id, existing_deal_id)
        return

    company_id = _find_company(prospect) or _create_company(prospect, website)
    deal_name = f"{prospect} — 3PL Fulfillment"
    deal_id = _find_deal(deal_name) or _create_deal(deal_name, monthly_mrr, stage, company_id or "", brief)

    if deal_id:
        hub_domain = os.environ.get("HUBSPOT_DOMAIN", "app-na2.hubspot.com").strip()
        portal_id = _portal_id() or ""
        deal_url = (
            f"https://{hub_domain}/contacts/{portal_id}/deal/{deal_id}"
            if portal_id else ""
        )
        storage.update_summary(run_id, {
            "hubspot_deal_id": deal_id,
            "hubspot_company_id": company_id,
            "hubspot_deal_url": deal_url,
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
    fq = dict(summary.get("fulfillment_quote") or {})
    pitched = _monthly_mrr(fq)
    brief = _build_brief({
        "id": run_id,
        "prospect": prospect,
        "origin_zip": summary.get("origin_zip"),
        "monthly_order_volume": prospect_profile.get("monthly_order_volume"),
        "prospect_profile": prospect_profile,
    })
    _bg(_do_sync_new, run_id, prospect, website, stage, round(pitched, 2), brief)


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


def _do_sync_quote(run_id: int, owner_email: str = "", force: bool = False) -> None:
    import datetime as _dt
    from sales_support_agent.services.fulfillment_deck import storage
    run = storage.get_run(run_id)
    if run is None:
        return
    summary = dict(run.summary_json or {})
    try:
        from sales_support_agent.services.fulfillment_deck.pricing_rules import validate_quote_readiness
        errors = validate_quote_readiness(summary, published=(run.status == "completed"))
        if errors:
            logger.warning("[hubspot] quote blocked for run %d: %s", run_id, "; ".join(errors))
            return
    except Exception:
        logger.exception("[hubspot] quote readiness validation failed for run %d", run_id)
        return

    # Skip if a quote already exists and this is not a forced re-creation (e.g. on re-publish).
    if not force and str(summary.get("hubspot_quote_id") or ""):
        logger.debug("[hubspot] quote already exists for run %d, skipping (use force=True to re-create)", run_id)
        return

    deal_id = str(summary.get("hubspot_deal_id") or "") or None

    # If no deal yet (run pre-dates HubSpot wiring), create one now then re-read.
    if not deal_id:
        prospect_profile = dict(summary.get("prospect_profile") or {})
        prospect = str(summary.get("prospect") or f"Run {run_id}")
        website = str(prospect_profile.get("website") or "")
        stage = str(summary.get("pipeline_stage") or "intake")
        fq = dict(summary.get("fulfillment_quote") or {})
        pitched = _monthly_mrr(fq)
        from sales_support_agent.services.fulfillment_deck.admin_page import _build_brief
        brief = _build_brief({
            "id": run_id,
            "prospect": prospect,
            "origin_zip": summary.get("origin_zip"),
            "monthly_order_volume": prospect_profile.get("monthly_order_volume"),
            "prospect_profile": prospect_profile,
        })
        _do_sync_new(run_id, prospect, website, stage, round(pitched, 2), brief)
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

    # Resolve the HubSpot owner ID from the logged-in rep's email.
    owner_id = _lookup_owner_id(owner_email) if owner_email else None

    # Build rate-card line items (qty=1, price=rate) — not volume-computed totals.
    # Skip carrier shipping and packaging lines (too variable; out-of-scope for rate card).
    line_item_ids: list[str] = []
    _SKIP_KEYS = {"shipping", "packaging"}
    for line in (fq.get("lines") or []):
        key = str(line.get("key") or "")
        if not key or key in _SKIP_KEYS:
            continue
        rate = float(line.get("rate") or 0)
        if not rate:
            continue
        label = str(line.get("label") or "Service")
        unit = _unit_label(str(line.get("unit") or ""))
        li_id = _create_line_item(label, 1, rate, rate, unit)
        if li_id:
            line_item_ids.append(li_id)
    for fee in (fq.get("one_time") or []):
        if not isinstance(fee, dict):
            continue
        amount = float(fee.get("amount") or 0)
        if amount <= 0:
            continue
        label = str(fee.get("label") or "One-time fulfillment fee")
        unit = _unit_label(str(fee.get("unit") or "one-time"))
        li_id = _create_line_item(label, 1, amount, amount, unit)
        if li_id:
            line_item_ids.append(li_id)

    prospect = str(summary.get("prospect") or f"Run {run_id}")
    expiry_days = int(os.environ.get("HUBSPOT_QUOTE_EXPIRY_DAYS", "30"))
    expiry = (_dt.datetime.utcnow() + _dt.timedelta(days=expiry_days)).strftime("%Y-%m-%d")

    quote_id = _create_quote(prospect, expiry, deal_id, line_item_ids, owner_id=owner_id)
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


def sync_quote(run_id: int, owner_email: str = "", force: bool = False) -> None:
    """Call after a rate sheet is published. Creates a HubSpot Quote with e-signature enabled.

    Pass force=True to re-create even when a quote already exists (e.g. from
    an explicit "Create Quote" button click — not from a background re-publish).
    """
    if not _token():
        return
    _bg(_do_sync_quote, run_id, owner_email, force)


def sync_margin(run_id: int, margin: dict, pitched: float) -> None:
    """Call after costs are saved. Updates deal MRR amount + adds margin note."""
    if not _token():
        return

    def _do():
        deal_id = _get_deal_id(run_id)
        if not deal_id:
            return
        monthly_mrr = float(margin.get("marginable_revenue") or pitched or 0)
        if monthly_mrr > 0:
            _patch_deal(deal_id, {"amount": str(round(monthly_mrr, 2))})
        note = (
            f"Fulfillment cost analysis:\n"
            f"  Pitched: ${pitched:,.0f}/mo\n"
            f"  Carrier/pass-through revenue: −${margin.get('pass_through_monthly', 0):,.0f}/mo\n"
            f"  Marginable revenue: ${margin.get('marginable_revenue', 0):,.0f}/mo\n"
            f"  Pick & pack actual: −${margin.get('actual_pick_pack', 0):,.0f}\n"
            f"  Storage actual: −${margin.get('actual_storage', 0):,.0f}\n"
            f"  Tech fee actual: −${margin.get('actual_tech_fee', 0):,.0f}\n"
            f"  Optional/service actuals: −${margin.get('actual_optional_monthly', 0):,.0f}\n"
            f"  Monthly margin: ${margin.get('monthly_margin', 0):,.0f} "
            f"({margin.get('margin_pct', 0)}%)\n"
            f"  Annual margin: ${margin.get('annual_margin', 0):,.0f}"
        )
        _add_note(deal_id, note)
        logger.info("[hubspot] margin note added for run %d deal %s", run_id, deal_id)

    _bg(_do)
