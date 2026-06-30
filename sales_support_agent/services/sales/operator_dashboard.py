from __future__ import annotations

import html
import math
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from sales_support_agent.config import Settings
from sales_support_agent.integrations.hubspot import DEAL_PROPERTIES, HubSpotClient
from sales_support_agent.services.admin_nav import (
    render_agent_favicon_links,
    render_agent_nav,
    render_agent_nav_styles,
)

TARGET_STAGE_LABELS = [
    "New Lead",
    "Contacted",
    "Qualified",
    "Audit Or Deck In Progress",
    "Proposal Ready",
    "Proposal Sent",
    "Negotiation",
    "Closed Won",
    "Closed Lost",
    "Nurture",
]
OBJECT_DEFINITIONS = {
    "contact": {
        "system_of_record": "hubspot",
        "required_fields": ["name_or_firstname", "email_or_phone"],
    },
    "company": {
        "system_of_record": "hubspot",
        "required_fields": ["name"],
    },
    "deal": {
        "system_of_record": "hubspot",
        "required_fields": ["company", "contact", "service_interest", "source", "owner"],
        "rules": ["one_primary_offer_only", "create_second_linked_deal_on_second_offer_detection"],
    },
    "deck": {
        "system_of_record": "agent.anatainc.com",
        "notes": "live link, can belong to multiple deals",
    },
    "audit": {
        "system_of_record": "agent.anatainc.com",
        "notes": "ads audit belongs to exactly one deal",
    },
    "quote": {
        "system_of_record": "hubspot",
        "notes": "create when pricing is roughly known",
    },
    "task": {
        "system_of_record": "agent_and_hubspot",
        "notes": "create when confidence is insufficient",
    },
    "communication": {
        "system_of_record": "hubspot",
        "notes": "agent reads for state changes and can reply in user voice",
    },
}
AUTONOMY_POLICY = {
    "mode": "high_confidence_only",
    "agent_can": [
        "create_deal",
        "update_deal_stage",
        "update_deal_amount",
        "create_linked_deal",
        "update_internal_notes",
        "create_follow_up_task",
        "send_outbound_message",
        "update_artifact_status",
    ],
    "when_not_confident": [
        "write_internal_note",
        "create_internal_follow_up_task",
    ],
}
HIGH_CONFIDENCE_THRESHOLD = 0.85
MEDIUM_CONFIDENCE_THRESHOLD = 0.65
SNAPSHOT_TTL_SECONDS = 30

_cached_snapshot: Optional[dict[str, Any]] = None
_cached_snapshot_expires_at = 0.0


def _esc(value: object) -> str:
    return html.escape(str(value or ""))


def _normalize(value: str) -> str:
    return " ".join("".join(ch.lower() if ch.isalnum() else " " for ch in value).split())


def _parse_multi_value(value: Optional[str]) -> list[str]:
    return [item.strip() for item in (value or "").split(";") if item.strip()]


def _to_float(value: object) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _get_primary_pipeline(client: HubSpotClient, settings: Settings) -> dict[str, Any]:
    pipeline_id = (settings.hubspot_sales_pipeline_id or "").strip()
    if pipeline_id:
        return client.get_deal_pipeline(pipeline_id)
    pipelines = client.list_deal_pipelines()
    return pipelines[0] if pipelines else {"id": "", "label": "Unknown", "stages": []}


def _stage_probability(stage: dict[str, Any]) -> float:
    metadata = stage.get("metadata") or {}
    return _to_float(metadata.get("probability")) or 0.0


def get_stage_status(stage: dict[str, Any]) -> str:
    label = _normalize(str(stage.get("label") or ""))
    probability = _stage_probability(stage)
    if "nurture" in label or "follow up" in label:
        return "nurture"
    if "lost" in label or probability == 0:
        return "lost"
    if "won" in label or probability == 1:
        return "won"
    return "open"


def _offer_to_service_type(offer_id: str) -> Optional[str]:
    if offer_id == "amazon_marketing_service":
        return "Amazon"
    if offer_id == "fulfillment":
        return "Fulfillment"
    if offer_id in {"shipping_os", "anata_intelligence"}:
        return "Software"
    return None


def infer_offer(deal: dict[str, Any], company: Optional[dict[str, Any]]) -> dict[str, Any]:
    properties = deal.get("properties") or {}
    company_properties = (company or {}).get("properties") or {}
    labels = {
        "amazon_marketing_service": "Amazon Marketing Service",
        "fulfillment": "Fulfillment",
        "shipping_os": "Shipping OS",
        "anata_intelligence": "Anata Intelligence",
        "unknown": "Unclassified",
    }
    overlays = {
        "amazon_marketing_service": "Anata Intelligence",
        "fulfillment": "Shipping OS",
        "shipping_os": "Shipping OS",
        "anata_intelligence": "Anata Intelligence",
    }
    scores: dict[str, float] = {}
    reasons: dict[str, list[str]] = {}

    def add(offer_id: str, score: float, reason: str) -> None:
        scores[offer_id] = scores.get(offer_id, 0.0) + score
        reasons.setdefault(offer_id, []).append(reason)

    for value in _parse_multi_value(str(properties.get("service_type") or "")):
        normalized = _normalize(value)
        if normalized == "amazon":
            add("amazon_marketing_service", 0.42, "deal service_type already signals Amazon")
        if normalized == "fulfillment":
            add("fulfillment", 0.42, "deal service_type already signals Fulfillment")
        if normalized == "software":
            if str(properties.get("shipping_os") or "").strip():
                add("shipping_os", 0.48, "deal service_type plus shipping_os signals Shipping OS")
            else:
                add("anata_intelligence", 0.22, "deal service_type signals Software")

    for value in _parse_multi_value(str(company_properties.get("service_type") or "")):
        normalized = _normalize(value)
        if normalized == "amazon":
            add("amazon_marketing_service", 0.22, "company service_type signals Amazon")
        if normalized == "fulfillment":
            add("fulfillment", 0.22, "company service_type signals Fulfillment")
        if normalized == "software":
            if str(properties.get("shipping_os") or "").strip():
                add("shipping_os", 0.24, "company service_type plus shipping_os signals Shipping OS")
            else:
                add("anata_intelligence", 0.12, "company service_type signals Software")

    if str(properties.get("agency") or "").strip():
        add("amazon_marketing_service", 0.98, "agency progress field is populated")
    if str(properties.get("fulfillment") or "").strip():
        add("fulfillment", 0.98, "fulfillment progress field is populated")
    if str(properties.get("shipping_os") or "").strip():
        add("shipping_os", 0.99, "shipping_os progress field is populated")

    haystack = _normalize(" ".join(filter(None, [str(properties.get("dealname") or ""), str(company_properties.get("name") or "")])))
    keyword_sets = {
        "fulfillment": ["fulfillment", "3pl", "warehouse", "ship"],
        "amazon_marketing_service": ["amazon", "marketing", "ads", "advertising"],
        "shipping_os": ["shipping os", "shippingos"],
        "anata_intelligence": ["saas", "software", "intelligence"],
    }
    for offer_id, keywords in keyword_sets.items():
        count = sum(1 for keyword in keywords if keyword in haystack)
        if offer_id == "fulfillment":
            if count >= 2:
                add(offer_id, 0.93, "deal and company naming strongly imply Fulfillment")
            elif count == 1:
                add(offer_id, 0.63, "deal naming weakly implies Fulfillment")
        elif offer_id == "amazon_marketing_service":
            if count >= 2:
                add(offer_id, 0.91, "deal and company naming strongly imply Amazon Marketing Service")
            elif count == 1:
                add(offer_id, 0.58, "deal naming weakly implies Amazon Marketing Service")
        elif offer_id == "shipping_os" and count >= 1:
            add(offer_id, 0.94, "deal naming explicitly references Shipping OS")
        elif offer_id == "anata_intelligence" and count >= 1 and "shipping os" not in haystack:
            add(offer_id, 0.55, "deal naming implies a software or intelligence offer")

    ranked = sorted(scores.items(), key=lambda item: item[1], reverse=True)
    if not ranked:
        return {
            "primary_offer": "unknown",
            "primary_offer_label": labels["unknown"],
            "overlay": None,
            "signal_count": 0,
            "confidence": 0.0,
            "reasons": ["no deterministic service signals were found"],
            "deal_service_type_value": None,
        }
    primary_offer, top_score = ranked[0]
    second_score = ranked[1][1] if len(ranked) > 1 else 0.0
    confidence = max(0.0, min(0.99, top_score - (0.14 if top_score - second_score < 0.18 else 0.0)))
    signal_count = sum(1 for _, score in ranked if score >= 0.25)
    return {
        "primary_offer": primary_offer,
        "primary_offer_label": labels.get(primary_offer, labels["unknown"]),
        "overlay": overlays.get(primary_offer),
        "signal_count": signal_count,
        "confidence": confidence,
        "reasons": reasons.get(primary_offer, []),
        "deal_service_type_value": _offer_to_service_type(primary_offer),
    }


def build_suggested_next_step(stage: Optional[dict[str, Any]], inference: dict[str, Any]) -> dict[str, Any]:
    offer_text = "opportunity"
    if inference.get("primary_offer") != "unknown" and inference.get("primary_offer_label"):
        offer_text = str(inference["primary_offer_label"]).lower()
    label = _normalize(str((stage or {}).get("label") or ""))
    if "new lead" in label:
        return {"text": f"Make the first contact on this {offer_text}, confirm it is a real lead, and capture the opening context.", "confidence": 0.95}
    if "contacted" in label:
        return {"text": f"Confirm the response details for this {offer_text} lead and decide whether it should move into qualification.", "confidence": 0.95}
    if "qualified" in label:
        return {"text": f"Confirm requirements for the {offer_text}, fill the qualification gaps, and prepare the proposal path.", "confidence": 0.96}
    if "audit or deck in progress" in label:
        return {"text": f"Finish the {offer_text} proposal deck and audit so scope, pricing, and recommendations are ready.", "confidence": 0.96}
    if "proposal ready" in label:
        return {"text": f"Review the {offer_text} proposal internally, finalize the send package, and confirm it is ready to send.", "confidence": 0.95}
    if "proposal sent" in label or "offered" in label:
        return {"text": f"Follow up on the sent {offer_text} proposal and confirm questions, objections, and timeline.", "confidence": 0.95}
    if "negotiation" in label:
        return {"text": f"Resolve the open negotiation points on the {offer_text} proposal and confirm the path to close.", "confidence": 0.95}
    if "nurture" in label or "follow up" in label:
        return {"text": f"Send the next follow-up on this {offer_text} and confirm whether the deal stays active or remains in nurture.", "confidence": 0.94}
    return {"text": f"Review this {offer_text} and define the next commercial action.", "confidence": 0.90}


def _map_records(records: list[dict[str, Any]], key_name: str = "id") -> dict[str, dict[str, Any]]:
    return {str(item.get(key_name) or ""): item for item in records if str(item.get(key_name) or "").strip()}


def _format_owner(owner: dict[str, Any]) -> str:
    full_name = " ".join(part for part in [str(owner.get("firstName") or "").strip(), str(owner.get("lastName") or "").strip()] if part).strip()
    return full_name or str(owner.get("email") or "").strip() or f"Owner {owner.get('id', '')}"


def _property_summary(properties: list[dict[str, Any]]) -> dict[str, Any]:
    custom = [item for item in properties if not item.get("hubspotDefined")]
    return {
        "totalCount": len(properties),
        "customCount": len(custom),
    }


def _list_deals(client: HubSpotClient, *, limit: Optional[int] = None) -> list[dict[str, Any]]:
    deals = list(
        client.iter_objects(
            "deals",
            properties=DEAL_PROPERTIES,
            max_records=limit,
            sorts=[{"propertyName": "hs_lastmodifieddate", "direction": "DESCENDING"}],
            filter_groups=(
                [{"filters": [{"propertyName": "pipeline", "operator": "EQ", "value": client.settings.hubspot_sales_pipeline_id}]}]
                if (client.settings.hubspot_sales_pipeline_id or "").strip()
                else None
            ),
        )
    )
    return deals


def build_operator_snapshot(settings: Settings) -> dict[str, Any]:
    client = HubSpotClient(settings)
    if not client.is_configured:
        raise RuntimeError("HubSpot token is not configured for this environment.")
    pipeline = _get_primary_pipeline(client, settings)
    owners = client.list_owners()
    all_deals = _list_deals(client)
    recent_deals = _list_deals(client, limit=12)
    owner_map = {str(owner.get("id") or ""): _format_owner(owner) for owner in owners}
    stage_map = {str(stage.get("id") or ""): stage for stage in pipeline.get("stages", []) or []}

    company_ids = set()
    contact_ids = set()
    deal_company_ids: dict[str, str] = {}
    deal_contact_ids: dict[str, str] = {}
    for deal in recent_deals:
        deal_id = str(deal.get("id") or "")
        company_list = client.list_associations("deals", deal_id, "companies")
        contact_list = client.list_associations("deals", deal_id, "contacts")
        if company_list:
            deal_company_ids[deal_id] = company_list[0]
            company_ids.add(company_list[0])
        if contact_list:
            deal_contact_ids[deal_id] = contact_list[0]
            contact_ids.add(contact_list[0])
    companies = _map_records(client.batch_read("companies", sorted(company_ids), properties=("name", "service_type")))
    contacts = _map_records(client.batch_read("contacts", sorted(contact_ids), properties=("firstname", "lastname", "email")))

    open_deals = won_deals = lost_deals = nurture_deals = 0
    unclassified = missing_amount = missing_owner = missing_next = multi_offer = 0
    open_amount = 0.0
    stage_rows: list[dict[str, Any]] = []
    stage_summary_map: dict[str, dict[str, Any]] = {}
    for stage in pipeline.get("stages", []) or []:
        row = {
            "id": str(stage.get("id") or ""),
            "label": str(stage.get("label") or ""),
            "status": get_stage_status(stage),
            "dealCount": 0,
            "totalAmount": 0.0,
            "needsAttentionCount": 0,
        }
        stage_rows.append(row)
        stage_summary_map[row["id"]] = row

    for deal in all_deals:
        properties = deal.get("properties") or {}
        stage_id = str(properties.get("dealstage") or "")
        stage = stage_map.get(stage_id)
        summary = stage_summary_map.get(stage_id)
        amount = _to_float(properties.get("amount")) or 0.0
        inference = infer_offer(deal, None)
        if inference["primary_offer"] == "unknown":
            unclassified += 1
        if inference["signal_count"] > 1:
            multi_offer += 1
        if not _to_float(properties.get("amount")):
            missing_amount += 1
        if not str(properties.get("hubspot_owner_id") or "").strip():
            missing_owner += 1
        if not stage or not summary:
            continue
        status = get_stage_status(stage)
        summary["dealCount"] += 1
        summary["totalAmount"] += amount
        if status == "open":
            open_deals += 1
            open_amount += amount
            if not str(properties.get("hs_next_step") or "").strip():
                missing_next += 1
                summary["needsAttentionCount"] += 1
        elif status == "won":
            won_deals += 1
        elif status == "lost":
            lost_deals += 1
        elif status == "nurture":
            nurture_deals += 1
            if not str(properties.get("hs_next_step") or "").strip():
                summary["needsAttentionCount"] += 1

    recent_rows = []
    for deal in recent_deals:
        deal_id = str(deal.get("id") or "")
        properties = deal.get("properties") or {}
        stage = stage_map.get(str(properties.get("dealstage") or ""))
        company = companies.get(deal_company_ids.get(deal_id, ""))
        contact = contacts.get(deal_contact_ids.get(deal_id, ""))
        inference = infer_offer(deal, company)
        amount = _to_float(properties.get("amount"))
        stage_status = get_stage_status(stage) if stage else "open"
        missing_fields: list[str] = []
        if inference["primary_offer"] == "unknown":
            missing_fields.append("service classification")
        if amount is None:
            missing_fields.append("amount")
        if not str(properties.get("hubspot_owner_id") or "").strip():
            missing_fields.append("owner")
        if stage_status in {"open", "nurture"} and not str(properties.get("hs_next_step") or "").strip():
            missing_fields.append("next step")
        if not company:
            missing_fields.append("company link")
        if not contact:
            missing_fields.append("contact link")
        full_name = ""
        if contact:
            cp = contact.get("properties") or {}
            full_name = " ".join(part for part in [str(cp.get("firstname") or "").strip(), str(cp.get("lastname") or "").strip()] if part).strip() or str(cp.get("email") or "")
        recent_rows.append(
            {
                "id": deal_id,
                "name": str(properties.get("dealname") or "").strip() or "Unnamed deal",
                "amount": amount,
                "owner": owner_map.get(str(properties.get("hubspot_owner_id") or ""), "Unassigned"),
                "stage": str((stage or {}).get("label") or properties.get("dealstage") or "Unknown stage"),
                "stageStatus": stage_status,
                "company": str(((company or {}).get("properties") or {}).get("name") or "No company"),
                "contact": full_name or "No contact",
                "primaryOffer": inference["primary_offer_label"],
                "overlay": inference.get("overlay"),
                "updatedAt": str(deal.get("updatedAt") or ""),
                "nextStep": str(properties.get("hs_next_step") or "").strip() or None,
                "missingFields": missing_fields,
                "url": (
                    f"https://app.hubspot.com/contacts/{settings.hubspot_portal_id}/record/0-3/{deal_id}"
                    if settings.hubspot_portal_id else ""
                ),
            }
        )

    live_labels = [str(stage.get("label") or "") for stage in pipeline.get("stages", []) or []]
    normalized_live = {_normalize(label): label for label in live_labels}
    normalized_target = {_normalize(label): label for label in TARGET_STAGE_LABELS}
    return {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "portalId": settings.hubspot_portal_id,
        "pipeline": {
            "id": str(pipeline.get("id") or ""),
            "label": str(pipeline.get("label") or ""),
            "stages": stage_rows,
            "liveStageCount": len(live_labels),
            "targetStageCount": len(TARGET_STAGE_LABELS),
        },
        "summary": {
            "openDeals": open_deals,
            "wonDeals": won_deals,
            "lostDeals": lost_deals,
            "nurtureDeals": nurture_deals,
            "openAmount": open_amount,
            "unclassifiedDeals": unclassified,
            "dealsMissingAmount": missing_amount,
            "dealsMissingOwner": missing_owner,
            "openDealsMissingNextStep": missing_next,
            "multiOfferCandidates": multi_offer,
        },
        "directives": {
            "happening": [
                f"{open_deals} open opportunities are active in the live {pipeline.get('label') or 'HubSpot'} pipeline.",
                f"{won_deals} won records and {lost_deals} lost records are shaping current sales history.",
                f"{multi_offer} deals show multiple offer signals and are candidates for linked commercial records.",
            ],
            "broken": [
                f"{unclassified} deals still lack a confident primary service or software classification.",
                f"{missing_next} open deals do not have a next-step instruction.",
                f"{missing_owner} deals are unassigned and {missing_amount} deals are missing amount data.",
            ],
            "next": [
                "Normalize live HubSpot stages into the audited shared operating model without losing current pipeline history.",
                "Write service inference back into the deal model only when confidence is high enough to act safely.",
                "Use the deal board and cleanup layers as the current human review path while deck and audit sync are connected.",
            ],
        },
        "schema": {
            "owners": [{"id": str(owner.get("id") or ""), "name": _format_owner(owner), "email": owner.get("email")} for owner in owners],
            "properties": {
                "deals": _property_summary(client.list_properties("deals")),
                "companies": _property_summary(client.list_properties("companies")),
                "contacts": _property_summary(client.list_properties("contacts")),
            },
            "associationLabels": {
                "dealToCompany": [str(item.get("label") or f"Type {item.get('typeId')}") for item in client.list_association_labels("deal", "company")],
                "dealToContact": [str(item.get("label") or f"Type {item.get('typeId')}") for item in client.list_association_labels("deal", "contact")],
            },
            "confidencePolicy": {
                "highThreshold": HIGH_CONFIDENCE_THRESHOLD,
                "mediumThreshold": MEDIUM_CONFIDENCE_THRESHOLD,
                "duplicateSendWindowMinutes": 240,
                "outboundEmailEnabled": False,
                "linkedDealCreationEnabled": True,
            },
        },
        "objectDefinitions": OBJECT_DEFINITIONS,
        "autonomy": AUTONOMY_POLICY,
        "stageDrift": {
            "targetOnly": [label for label in TARGET_STAGE_LABELS if _normalize(label) not in normalized_live],
            "liveOnly": [label for label in live_labels if _normalize(label) not in normalized_target],
        },
        "recentDeals": recent_rows,
    }


def get_operator_snapshot(settings: Settings, *, force_refresh: bool = False) -> dict[str, Any]:
    global _cached_snapshot, _cached_snapshot_expires_at
    if not force_refresh and _cached_snapshot and _cached_snapshot_expires_at > time.time():
        return _cached_snapshot
    snapshot = build_operator_snapshot(settings)
    _cached_snapshot = snapshot
    _cached_snapshot_expires_at = time.time() + SNAPSHOT_TTL_SECONDS
    return snapshot


def invalidate_operator_snapshot() -> None:
    global _cached_snapshot, _cached_snapshot_expires_at
    _cached_snapshot = None
    _cached_snapshot_expires_at = 0.0


def run_writeback(settings: Settings, *, mode: str = "preview", limit: int = 10, deal_ids: Optional[list[str]] = None) -> dict[str, Any]:
    client = HubSpotClient(settings)
    pipeline = _get_primary_pipeline(client, settings)
    stage_map = {str(stage.get("id") or ""): stage for stage in pipeline.get("stages", []) or []}
    deals = client.batch_read("deals", deal_ids or [], properties=DEAL_PROPERTIES) if deal_ids else _list_deals(client, limit=100)
    if not deal_ids:
        filtered = []
        for deal in deals:
            properties = deal.get("properties") or {}
            stage = stage_map.get(str(properties.get("dealstage") or ""))
            if not stage:
                continue
            status = get_stage_status(stage)
            needs_classification = not str(properties.get("service_type") or "").strip()
            needs_next_step = status in {"open", "nurture"} and not str(properties.get("hs_next_step") or "").strip()
            if needs_classification or needs_next_step:
                filtered.append(deal)
        deals = filtered[: max(1, min(limit, 25))]
    results = []
    applied = deferred = note_count = task_count = 0
    for deal in deals:
        deal_id = str(deal.get("id") or "")
        properties = deal.get("properties") or {}
        stage = stage_map.get(str(properties.get("dealstage") or ""))
        stage_status = get_stage_status(stage) if stage else "open"
        company = None
        company_ids = client.list_associations("deals", deal_id, "companies")
        if company_ids:
            company_rows = client.batch_read("companies", [company_ids[0]], properties=("name", "service_type"))
            company = company_rows[0] if company_rows else None
        inference = infer_offer(deal, company)
        suggestion = build_suggested_next_step(stage, inference)
        actions = []
        high_conf = []
        if not str(properties.get("service_type") or "").strip() and inference.get("deal_service_type_value") and float(inference.get("confidence") or 0.0) >= HIGH_CONFIDENCE_THRESHOLD:
            high_conf.append({"type": "update_deal_service_type", "payload": {"service_type": inference["deal_service_type_value"]}, "reason": f"set deal service_type to {inference['deal_service_type_value']}", "confidence": inference["confidence"]})
        if stage_status in {"open", "nurture"} and not str(properties.get("hs_next_step") or "").strip() and float(suggestion.get("confidence") or 0.0) >= HIGH_CONFIDENCE_THRESHOLD:
            high_conf.append({"type": "update_next_step", "payload": {"hs_next_step": suggestion["text"]}, "reason": "set deterministic next step from current stage", "confidence": suggestion["confidence"]})
        medium_reasons = []
        if not high_conf and not str(properties.get("service_type") or "").strip() and inference.get("deal_service_type_value"):
            medium_reasons.append(f"deal service_type likely should be {inference['deal_service_type_value']} but confidence is only {round(float(inference.get('confidence') or 0.0) * 100)}%")
        if not high_conf and stage_status in {"open", "nurture"} and not str(properties.get("hs_next_step") or "").strip():
            medium_reasons.append(f"next step suggestion exists but confidence is only {round(float(suggestion.get('confidence') or 0.0) * 100)}%")
        if not high_conf and not medium_reasons:
            continue
        if mode == "apply" and high_conf:
            merged = {}
            for action in high_conf:
                merged.update(action["payload"])
            client.update_deal(deal_id, merged)
            applied += len(high_conf)
            client.create_note(deal_id=deal_id, body="<p><strong>Anata agent applied high-confidence deal updates.</strong></p><ul>" + "".join(f"<li>{_esc(action['reason'])}</li>" for action in high_conf) + "</ul>")
            note_count += 1
            applied += 1
            actions.extend([{**action, "status": "applied"} for action in high_conf])
            actions.append({"type": "create_internal_note", "status": "applied", "reason": "logged reasoning note for applied write-back actions"})
        else:
            actions.extend([{**action, "status": "preview"} for action in high_conf])
        if mode == "apply" and medium_reasons:
            client.create_note(deal_id=deal_id, body="<p><strong>Anata agent deferred write-back actions.</strong></p><ul>" + "".join(f"<li>{_esc(reason)}</li>" for reason in medium_reasons) + "</ul>")
            note_count += 1
            applied += 1
            owner_id = str(properties.get("hubspot_owner_id") or "").strip()
            client.create_task(
                deal_id=deal_id,
                subject=f"Review deferred sales write-back for {str(properties.get('dealname') or deal_id)}",
                body="\n".join(["The Anata agent found candidate updates that were below the high-confidence threshold.", *[f"- {reason}" for reason in medium_reasons]]),
                due_at_ms=int((time.time() + (2 if stage_status == "nurture" else 1) * 86400) * 1000),
                owner_id=owner_id,
            )
            task_count += 1
            applied += 1
            deferred += len(medium_reasons)
            actions.append({"type": "create_follow_up_task", "status": "applied", "reason": "created internal review task for medium-confidence actions"})
        else:
            deferred += len(medium_reasons)
            actions.extend([{"type": "create_internal_note", "status": "deferred", "reason": reason} for reason in medium_reasons])
        results.append(
            {
                "dealId": deal_id,
                "dealName": str(properties.get("dealname") or "").strip() or "Unnamed deal",
                "companyName": str(((company or {}).get("properties") or {}).get("name") or "No company"),
                "stage": str((stage or {}).get("label") or properties.get("dealstage") or "Unknown stage"),
                "stageStatus": stage_status,
                "current": {"serviceType": str(properties.get("service_type") or "").strip() or None, "nextStep": str(properties.get("hs_next_step") or "").strip() or None},
                "inference": {"primaryOffer": inference["primary_offer_label"], "confidence": inference["confidence"], "reasons": inference["reasons"], "targetDealServiceType": inference["deal_service_type_value"]},
                "actions": actions,
            }
        )
    if mode == "apply":
        invalidate_operator_snapshot()
    return {
        "mode": "apply" if mode == "apply" else "preview",
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "summary": {"candidateDeals": len(results), "appliedActions": applied, "deferredActions": deferred, "noteCount": note_count, "taskCount": task_count},
        "deals": results,
    }


def _fmt_money(value: Optional[float]) -> str:
    if value is None:
        return "Missing"
    if math.isclose(value, 0):
        return "$0"
    return f"${value:,.0f}"


def _fmt_relative(value: str) -> str:
    if not value:
        return "Unknown"
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        delta = datetime.now(timezone.utc) - parsed.astimezone(timezone.utc)
        if delta < timedelta(hours=24):
            return f"{max(int(delta.total_seconds() // 3600), 0)}h ago"
        return f"{max(delta.days, 0)}d ago"
    except ValueError:
        return value


def render_operator_page(snapshot: dict[str, Any], *, user: Optional[dict[str, Any]] = None, writeback: Optional[dict[str, Any]] = None, status_message: str = "") -> str:
    nav_styles = render_agent_nav_styles()
    nav = render_agent_nav("sales", sales_section="sales_operator", user=user)
    favicons = render_agent_favicon_links()
    summary = snapshot.get("summary", {})
    schema = snapshot.get("schema", {})
    stage_cards = "".join(
        f"""
        <article class="panel">
          <p class="eyebrow">{_esc(str(stage.get("status") or "").title())}</p>
          <h3>{_esc(stage.get("label"))}</h3>
          <p class="muted">{int(stage.get("dealCount") or 0)} deal(s) · {_fmt_money(float(stage.get("totalAmount") or 0.0))}</p>
          <p>{int(stage.get("needsAttentionCount") or 0)} need attention.</p>
        </article>
        """
        for stage in snapshot.get("pipeline", {}).get("stages", [])
    ) or "<p class='muted'>No live stage data returned.</p>"
    recent_cards = "".join(
        f"""
        <article class="panel">
          <p class="eyebrow">{_esc(deal.get("primaryOffer"))}</p>
          <h3><a href="{_esc(deal.get("url"))}" target="_blank" rel="noreferrer">{_esc(deal.get("name"))}</a></h3>
          <p class="muted">{_esc(deal.get("company"))} · {_esc(deal.get("contact"))}</p>
          <p>{_fmt_money(deal.get("amount"))} · {_esc(deal.get("stage"))} · {_esc(deal.get("owner"))}</p>
          <p><strong>Next step:</strong> {_esc(deal.get("nextStep") or "No next step")}</p>
          <p><strong>Missing:</strong> {_esc(", ".join(deal.get("missingFields") or []) or "No critical gaps detected.")}</p>
          <p class="muted">Updated {_fmt_relative(str(deal.get("updatedAt") or ""))}</p>
        </article>
        """
        for deal in snapshot.get("recentDeals", [])
    ) or "<p class='muted'>No recent deals returned.</p>"
    writeback_markup = ""
    if writeback:
        writeback_cards = "".join(
            f"""
            <article class="panel">
              <p class="eyebrow">{_esc(deal.get("stageStatus", "")).title()}</p>
              <h3>{_esc(deal.get("dealName"))}</h3>
              <p class="muted">{_esc(deal.get("companyName"))} · {_esc(deal.get("stage"))}</p>
              <p><strong>Current service type:</strong> {_esc(deal.get("current", {}).get("serviceType") or "Blank")}</p>
              <p><strong>Current next step:</strong> {_esc(deal.get("current", {}).get("nextStep") or "Blank")}</p>
              <ul class="list">{"".join(f"<li>{_esc(action.get('type'))} · {_esc(action.get('status'))} · {_esc(action.get('reason'))}</li>" for action in deal.get("actions", [])) or "<li>No actions recorded.</li>"}</ul>
            </article>
            """
            for deal in writeback.get("deals", [])
        ) or "<p class='muted'>No candidate deals returned.</p>"
        writeback_markup = f"""
        <section class="workspace section-gap">
          <h2>Write-back result</h2>
          <div class="stats">
            <div class="stat"><div class="n">{int(writeback.get("summary", {}).get("candidateDeals") or 0)}</div><div class="l">Candidates</div></div>
            <div class="stat"><div class="n">{int(writeback.get("summary", {}).get("appliedActions") or 0)}</div><div class="l">Applied</div></div>
            <div class="stat"><div class="n">{int(writeback.get("summary", {}).get("deferredActions") or 0)}</div><div class="l">Deferred</div></div>
          </div>
          <div class="grid">{writeback_cards}</div>
        </section>
        """
    status_html = f"<div class='flash'>{_esc(status_message)}</div>" if status_message else ""
    directives = snapshot.get("directives", {})
    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <title>agent | Sales Operator</title>
    {favicons}
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Montserrat:wght@700;800&display=swap" rel="stylesheet">
    <style>
      :root {{ --dark-blue:#2B3644; --light-blue:#85BBDA; --light-brown:#F9F7F3; --white:#FFF; --border:rgba(43,54,68,0.12); --shadow:rgba(43,54,68,0.10); }}
      * {{ box-sizing:border-box; }}
      body {{ margin:0; background:var(--light-brown); color:var(--dark-blue); font-family:"Inter","Segoe UI",sans-serif; }}
      a {{ color:var(--dark-blue); }}
      {nav_styles}
      .shell {{ max-width:1260px; margin:0 auto; padding:28px 18px 64px; }}
      .workspace {{ background:var(--white); border:1px solid var(--border); border-radius:20px; box-shadow:0 18px 40px var(--shadow); padding:26px 28px 30px; }}
      .hero {{ background:linear-gradient(135deg,#10233d 0%,#294566 100%); color:#fff; }}
      .hero .eyebrow,.hero .muted,.hero p,.hero a {{ color:#fff; }}
      h1,h2,h3,p {{ margin-top:0; }}
      h1 {{ font-family:"Montserrat",sans-serif; font-size:34px; margin-bottom:10px; }}
      h2 {{ font-family:"Montserrat",sans-serif; font-size:22px; margin-bottom:14px; }}
      h3 {{ font-size:18px; margin-bottom:8px; }}
      .eyebrow {{ font-family:"Montserrat",sans-serif; font-size:11px; letter-spacing:0.08em; text-transform:uppercase; color:rgba(43,54,68,0.56); margin:0 0 6px; }}
      .muted {{ color:rgba(43,54,68,0.68); }}
      .stats {{ display:flex; gap:14px; flex-wrap:wrap; margin:18px 0 0; }}
      .stat {{ background:rgba(255,255,255,0.1); border:1px solid rgba(255,255,255,0.16); border-radius:14px; padding:12px 16px; min-width:150px; }}
      .stat .n {{ font-family:"Montserrat",sans-serif; font-size:24px; }}
      .stat .l {{ font-size:11px; text-transform:uppercase; letter-spacing:0.08em; opacity:0.82; }}
      .grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(260px,1fr)); gap:16px; }}
      .panel {{ background:#fff; border:1px solid var(--border); border-radius:16px; padding:16px; }}
      .section-gap {{ margin-top:18px; }}
      .flash {{ margin:0 0 18px; padding:12px 14px; border-radius:14px; background:rgba(133,187,218,0.15); border:1px solid rgba(43,54,68,0.1); }}
      .toolbar {{ display:flex; gap:10px; flex-wrap:wrap; margin-top:16px; }}
      .btn {{ display:inline-flex; align-items:center; justify-content:center; padding:10px 14px; border-radius:999px; border:1px solid var(--border); background:#fff; color:var(--dark-blue); text-decoration:none; font-weight:700; cursor:pointer; }}
      .btn--dark {{ background:var(--dark-blue); color:#fff; border-color:var(--dark-blue); }}
      input[type="text"] {{ width:100%; padding:12px 13px; border-radius:14px; border:1px solid var(--border); font:inherit; }}
      .inline-form {{ display:grid; gap:12px; margin-top:12px; }}
      .inline-row {{ display:flex; gap:10px; flex-wrap:wrap; }}
      .list {{ margin:8px 0 0; padding-left:18px; }}
      .triple {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(240px,1fr)); gap:16px; }}
    </style>
  </head>
  <body>
    {nav}
    <main class="shell">
      <section class="workspace hero">
        <p class="eyebrow">Sales Operator</p>
        <h1>HubSpot visibility first. Autonomous action next.</h1>
        <p class="muted">HubSpot is the source of truth. This root sales page reads the live pipeline and property model, then exposes the first high-confidence write-back layer directly inside agent.anatainc.com.</p>
        <div class="stats">
          <div class="stat"><div class="n">{int(summary.get("openDeals") or 0)}</div><div class="l">Open deals</div></div>
          <div class="stat"><div class="n">{_fmt_money(summary.get("openAmount"))}</div><div class="l">Open value</div></div>
          <div class="stat"><div class="n">{int(summary.get("unclassifiedDeals") or 0)}</div><div class="l">Unclassified</div></div>
          <div class="stat"><div class="n">{int(summary.get("openDealsMissingNextStep") or 0)}</div><div class="l">Missing next step</div></div>
          <div class="stat"><div class="n">{int(summary.get("multiOfferCandidates") or 0)}</div><div class="l">Multi-offer</div></div>
        </div>
      </section>
      {status_html}
      <section class="workspace section-gap">
        <div class="toolbar">
          <a class="btn" href="/admin/sales/deals">Open deal board</a>
          <a class="btn" href="/admin/sales/deals/cleanup">Open cleanup review</a>
          <a class="btn" href="/admin/sales/snapshot">Open JSON snapshot</a>
          <form method="post" action="/admin/sales/deals/sync" style="margin:0">
            <button class="btn btn--dark" type="submit">Refresh HubSpot mirror</button>
          </form>
        </div>
      </section>
      <section class="workspace section-gap">
        <h2>What is happening / broken / next</h2>
        <div class="triple">
          <article class="panel"><p class="eyebrow">What Is Happening</p><ul class="list">{"".join(f"<li>{_esc(item)}</li>" for item in directives.get("happening", []))}</ul></article>
          <article class="panel"><p class="eyebrow">What Is Broken</p><ul class="list">{"".join(f"<li>{_esc(item)}</li>" for item in directives.get("broken", []))}</ul></article>
          <article class="panel"><p class="eyebrow">What Should Happen Next</p><ul class="list">{"".join(f"<li>{_esc(item)}</li>" for item in directives.get("next", []))}</ul></article>
        </div>
      </section>
      <section class="workspace section-gap">
        <h2>Live pipeline and object model</h2>
        <p class="muted">Portal {_esc(snapshot.get("portalId") or "Unknown")} · {_esc(snapshot.get("pipeline", {}).get("label") or "Unknown pipeline")} / {_esc(snapshot.get("pipeline", {}).get("id") or "")}</p>
        <p class="muted">{int(snapshot.get("pipeline", {}).get("liveStageCount") or 0)} live stages · {int(snapshot.get("pipeline", {}).get("targetStageCount") or 0)} target stages · {int(schema.get("properties", {}).get("deals", {}).get("customCount") or 0) + int(schema.get("properties", {}).get("companies", {}).get("customCount") or 0) + int(schema.get("properties", {}).get("contacts", {}).get("customCount") or 0)} custom core properties</p>
        <div class="grid section-gap">{stage_cards}</div>
      </section>
      <section class="workspace section-gap">
        <h2>First write-back action layer</h2>
        <p class="muted">Preview candidate actions first. Apply writes only high-confidence deal updates plus supporting notes and follow-up tasks.</p>
        <form method="post" action="/admin/sales/writeback" class="inline-form">
          <label for="limit">Candidate limit</label>
          <input id="limit" name="limit" type="text" value="10">
          <div class="inline-row">
            <button class="btn btn--dark" type="submit" name="mode" value="preview">Preview write-back</button>
            <button class="btn" type="submit" name="mode" value="apply">Apply high-confidence actions</button>
          </div>
        </form>
      </section>
      <section class="workspace section-gap">
        <h2>Object definitions and autonomy policy</h2>
        <div class="grid">
          {"".join(f"<article class='panel'><p class='eyebrow'>{_esc(name.title())}</p><h3>{_esc(name.title())}</h3><p class='muted'>Source: {_esc(defn.get('system_of_record') or '')}</p><p>{_esc(', '.join(defn.get('required_fields') or []) or defn.get('notes') or '')}</p></article>" for name, defn in OBJECT_DEFINITIONS.items())}
        </div>
      </section>
      <section class="workspace section-gap">
        <h2>Recent live deals</h2>
        <div class="grid">{recent_cards}</div>
      </section>
      {writeback_markup}
    </main>
  </body>
</html>"""
