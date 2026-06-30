"""Validated HubSpot deal creation helpers for the sales operator flow."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from sqlalchemy.orm import Session

from sales_support_agent.config import Settings
from sales_support_agent.models.entities import HubSpotDeal, HubSpotDealContact


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_RULES_PATH = REPO_ROOT / "config" / "hubspot_sales_rules.json"
DEAL_TO_CONTACT_ASSOCIATION_TYPE_ID = 3
DEAL_TO_COMPANY_ASSOCIATION_TYPE_ID = 5


@dataclass(frozen=True)
class SalesDealCreateRequest:
    properties: dict[str, str]
    company_id: str = ""
    contact_id: str = ""


class SalesDealRulesError(RuntimeError):
    pass


def read_sales_rules(path: Path | str | None = None) -> dict[str, Any]:
    rules_path = Path(path).expanduser() if path else DEFAULT_RULES_PATH
    try:
        payload = json.loads(rules_path.read_text())
    except FileNotFoundError as exc:
        raise SalesDealRulesError(f"HubSpot sales rules file not found: {rules_path}") from exc
    except json.JSONDecodeError as exc:
        raise SalesDealRulesError(f"HubSpot sales rules file is not valid JSON: {rules_path}") from exc
    if not isinstance(payload, dict):
        raise SalesDealRulesError("HubSpot sales rules file must contain a JSON object.")
    return payload


def _env(name: str) -> str:
    return os.getenv(name, "").strip()


def _deal_rules(rules: Mapping[str, Any]) -> Mapping[str, Any]:
    objects = rules.get("objects", {})
    if not isinstance(objects, Mapping):
        return {}
    deal = objects.get("deal", {})
    return deal if isinstance(deal, Mapping) else {}


def required_deal_properties(rules: Mapping[str, Any]) -> list[str]:
    raw = _deal_rules(rules).get("required_properties", [])
    return [str(item).strip() for item in raw if str(item).strip()] if isinstance(raw, list) else []


def allowed_deal_properties(rules: Mapping[str, Any]) -> list[str]:
    deal = _deal_rules(rules)
    configured: list[str] = []
    for key in ("required_properties", "recommended_properties"):
        raw = deal.get(key, [])
        if isinstance(raw, list):
            configured.extend(str(item).strip() for item in raw if str(item).strip())
    configured.extend(item.strip() for item in _env("HUBSPOT_DEAL_EXTRA_PROPERTIES").split(",") if item.strip())
    seen: set[str] = set()
    allowed: list[str] = []
    for item in configured:
        if item not in seen:
            seen.add(item)
            allowed.append(item)
    return allowed


def required_deal_associations(rules: Mapping[str, Any]) -> list[str]:
    required: list[str] = []
    for rule in _deal_rules(rules).get("rules", []):
        if not isinstance(rule, Mapping):
            continue
        if str(rule.get("when", "")).strip() != "always":
            continue
        association = str(rule.get("require_association", "")).strip()
        if association and association not in required:
            required.append(association)
    return required


def normalize_deal_create_request(
    payload: Mapping[str, Any],
    rules: Mapping[str, Any],
    *,
    settings: Settings,
) -> SalesDealCreateRequest:
    allowed = set(allowed_deal_properties(rules))
    aliases = {
        "deal_name": "dealname",
        "owner_id": "hubspot_owner_id",
        "service_line": "anata_service_line",
        "lead_source_detail": "anata_lead_source_detail",
        "next_step": "anata_next_step",
        "next_step_due_at": "anata_next_step_due_at",
    }
    properties: dict[str, str] = {}
    nested = payload.get("properties", {})
    if isinstance(nested, Mapping):
        for key, value in nested.items():
            prop = aliases.get(str(key), str(key))
            if prop in allowed and str(value).strip():
                properties[prop] = str(value).strip()
    for key, value in payload.items():
        prop = aliases.get(str(key), str(key))
        if prop in allowed and str(value).strip():
            properties[prop] = str(value).strip()

    properties["pipeline"] = (
        properties.get("pipeline")
        or (settings.hubspot_sales_pipeline_id or "").strip()
        or _env("HUBSPOT_DEFAULT_DEAL_PIPELINE")
        or _env("HUBSPOT_PIPELINE_ID")
        or "default"
    )
    properties["dealstage"] = (
        properties.get("dealstage")
        or _env("HUBSPOT_DEFAULT_DEAL_STAGE")
        or _env("HUBSPOT_STAGE_INTAKE")
        or "appointmentscheduled"
    )
    properties["hubspot_owner_id"] = properties.get("hubspot_owner_id") or _env("HUBSPOT_DEFAULT_OWNER_ID")
    properties["anata_service_line"] = properties.get("anata_service_line") or _env("HUBSPOT_DEFAULT_SERVICE_LINE")
    properties["anata_lead_source_detail"] = (
        properties.get("anata_lead_source_detail") or _env("HUBSPOT_DEFAULT_LEAD_SOURCE_DETAIL")
    )
    properties = {key: value for key, value in properties.items() if str(value).strip()}

    company_id = str(
        payload.get("hubspot_company_id")
        or payload.get("company_id")
        or payload.get("associated_company_id")
        or ""
    ).strip()
    contact_id = str(
        payload.get("hubspot_contact_id")
        or payload.get("contact_id")
        or payload.get("associated_contact_id")
        or ""
    ).strip()
    return SalesDealCreateRequest(properties=properties, company_id=company_id, contact_id=contact_id)


def validate_deal_create_request(request: SalesDealCreateRequest, rules: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    for prop in required_deal_properties(rules):
        if not request.properties.get(prop):
            errors.append(f"Missing required deal property: {prop}")
    required_associations = set(required_deal_associations(rules))
    if "company" in required_associations and not request.company_id:
        errors.append("Missing required company association: hubspot_company_id")
    if "contact" in required_associations and not request.contact_id:
        errors.append("Missing required contact association: hubspot_contact_id")
    service_line = request.properties.get("anata_service_line", "")
    allowed_service_lines = rules.get("service_lines", [])
    if service_line and isinstance(allowed_service_lines, list):
        allowed = {str(item).strip() for item in allowed_service_lines if str(item).strip()}
        if allowed and service_line not in allowed:
            errors.append(f"Invalid service line: {service_line}")
    amount = request.properties.get("amount", "")
    if amount:
        try:
            float(amount.replace(",", ""))
        except ValueError:
            errors.append("Amount must be numeric.")
    closedate = request.properties.get("closedate", "")
    if closedate and _parse_dt(closedate) is None:
        errors.append("Close date must be a valid date.")
    return errors


def _association_type_id(env_name: str, default: int) -> int:
    try:
        return int(_env(env_name) or default)
    except ValueError:
        return default


def build_deal_associations(request: SalesDealCreateRequest) -> list[dict[str, Any]]:
    associations: list[dict[str, Any]] = []
    if request.contact_id:
        associations.append({
            "to": {"id": request.contact_id},
            "types": [{
                "associationCategory": "HUBSPOT_DEFINED",
                "associationTypeId": _association_type_id(
                    "HUBSPOT_DEAL_TO_CONTACT_ASSOCIATION_TYPE_ID",
                    DEAL_TO_CONTACT_ASSOCIATION_TYPE_ID,
                ),
            }],
        })
    if request.company_id:
        associations.append({
            "to": {"id": request.company_id},
            "types": [{
                "associationCategory": "HUBSPOT_DEFINED",
                "associationTypeId": _association_type_id(
                    "HUBSPOT_DEAL_TO_COMPANY_ASSOCIATION_TYPE_ID",
                    DEAL_TO_COMPANY_ASSOCIATION_TYPE_ID,
                ),
            }],
        })
    return associations


def _to_cents(value: Any) -> int:
    if value in (None, ""):
        return 0
    try:
        return int(round(float(str(value).replace(",", "")) * 100))
    except (TypeError, ValueError):
        return 0


def _parse_dt(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        if raw.isdigit():
            n = int(raw)
            if n > 10_000_000_000:
                n = n / 1000
            return datetime.fromtimestamp(n, tz=timezone.utc)
        if len(raw) == 10 and raw[4] == "-" and raw[7] == "-":
            return datetime.fromisoformat(raw).replace(tzinfo=timezone.utc)
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def mirror_created_deal(
    session: Session,
    created: Mapping[str, Any],
    request: SalesDealCreateRequest,
) -> str:
    deal_id = str(created.get("id") or "").strip()
    if not deal_id:
        return ""
    props = dict(created.get("properties") or request.properties)
    row = session.get(HubSpotDeal, deal_id) or HubSpotDeal(hubspot_deal_id=deal_id)
    row.deal_name = str(props.get("dealname") or request.properties.get("dealname") or "")
    row.amount_cents = _to_cents(props.get("amount") or request.properties.get("amount"))
    row.deal_stage = str(props.get("dealstage") or request.properties.get("dealstage") or "")
    row.pipeline = str(props.get("pipeline") or request.properties.get("pipeline") or "")
    row.close_date = _parse_dt(props.get("closedate") or request.properties.get("closedate"))
    row.owner_id = str(props.get("hubspot_owner_id") or request.properties.get("hubspot_owner_id") or "")
    row.hubspot_company_id = request.company_id
    row.is_closed = False
    row.is_won = False
    row.description = str(props.get("description") or request.properties.get("description") or "")
    row.raw_properties = props
    row.created_at = _parse_dt(created.get("createdAt"))
    row.updated_at = _parse_dt(created.get("updatedAt"))
    row.last_sync_at = datetime.now(timezone.utc)
    session.add(row)
    if request.contact_id:
        session.add(HubSpotDealContact(hubspot_deal_id=deal_id, hubspot_contact_id=request.contact_id))
    return deal_id
