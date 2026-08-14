"""Server-to-server endpoints for anatainc.com embedded label purchase."""
from __future__ import annotations

import logging
from typing import Any, Optional

from fastapi import APIRouter, Header, Request
from fastapi.responses import JSONResponse

from sales_support_agent.api.fulfillment_public_router import _enforce_intake_key, _json_body
from sales_support_agent.services.public_request_guard import durable_rate_limit_response
from sales_support_agent.services.shipping_label_purchase import (
    create_record, live_wms, load_record, publishable_key, save_record, stripe_client, token_matches,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/public/fulfillment/labels", tags=["shipping-label-public"])
_PUBLIC_CARRIERS = {"USPS", "UPS", "FEDEX", "DHL", "GLS", "UNIUNI"}


def _text(value: Any, limit: int = 120) -> str:
    return str(value or "").strip()[:limit]


def _address(raw: Any) -> dict | None:
    if not isinstance(raw, dict): return None
    address = {k: _text(raw.get(k), 100) for k in ("name", "company", "street_1", "street_2", "city", "state", "postal")}
    address["country"] = "US"
    if not all(address[k] for k in ("name", "street_1", "city", "state", "postal")): return None
    return address


def _auth(request: Request, key: Optional[str]):
    return _enforce_intake_key(request, key)


def _public(record: dict) -> dict:
    return {k: record.get(k) for k in ("purchase_id", "status", "amount_cents", "carrier", "service", "tracking", "label_image", "error")}


@router.post("/rates")
async def rates(request: Request, x_internal_api_key: Optional[str] = Header(default=None)):
    denied = _auth(request, x_internal_api_key)
    if denied: return denied
    limited = durable_rate_limit_response(request, scope="shipping-label:rates", limit=20)
    if limited: return limited
    body, bad = await _json_body(request)
    if bad: return bad
    origin, destination = _address(body.get("from_address")), _address(body.get("to_address"))
    package = body.get("package") if isinstance(body.get("package"), dict) else {}
    try:
        dims = {k: float(package.get(k)) for k in ("length", "width", "height", "weight")}
    except (TypeError, ValueError):
        dims = {}
    if not origin or not destination or not dims or any(v <= 0 for v in dims.values()):
        return JSONResponse(status_code=400, content={"detail": "Complete both addresses and the package details."})
    model = {
        "reference": _text(body.get("reference"), 80) or "Website label",
        "from_address": origin, "to_address": destination,
        "shipment_packages": [{"type": "Parcel", **dims, "weight": round(dims["weight"] * 16),
            "contents_value": max(0, float(package.get("value") or 0)),
            "shipment_items": [{"name": _text(package.get("contents"), 60) or "Merchandise", "sku": "website-label", "quantity": 1, "value": max(0, float(package.get("value") or 0))}]}],
    }
    try:
        data = live_wms().create_shipment_rates(model)
        shipment_id = _text((data.get("model") or {}).get("id"), 80)
        clean_rates = []
        for rate in data.get("rates") or []:
            rate_id, amount = _text(rate.get("id"), 100), round(float(rate.get("rate") or 0) * 100)
            carrier = _text(rate.get("carrier"), 60).upper()
            if rate_id and amount > 0 and carrier in _PUBLIC_CARRIERS:
                clean_rates.append({"id": rate_id, "carrier": carrier, "service": _text(rate.get("service"), 80), "amount_cents": amount, "delivery_days": rate.get("delivery_days")})
        clean_rates.sort(key=lambda item: item["amount_cents"])
        if not shipment_id or not clean_rates: raise RuntimeError("Incomplete carrier response")
        record, token = create_record(body, shipment_id, clean_rates)
        return {"purchase_id": record["purchase_id"], "access_token": token, "rates": clean_rates}
    except Exception:
        logger.exception("Shipping label rating failed")
        return JSONResponse(status_code=503, content={"detail": "Live carrier rates are temporarily unavailable."})


@router.post("/address/verify")
async def verify_address(request: Request, x_internal_api_key: Optional[str] = Header(default=None)):
    denied = _auth(request, x_internal_api_key)
    if denied: return denied
    limited = durable_rate_limit_response(request, scope="shipping-label:address-verify", limit=30)
    if limited: return limited
    body, bad = await _json_body(request)
    if bad: return bad
    address = _address(body.get("address"))
    if not address:
        return JSONResponse(status_code=400, content={"detail": "Complete the address before checking it."})
    try:
        data = live_wms().verify_address(address)
        verified = bool(data.get("verified"))
        raw_choices = [data.get("address")] if verified and isinstance(data.get("address"), dict) else data.get("suggestions") or []
        suggestions = []
        for raw in raw_choices[:5]:
            if not isinstance(raw, dict): continue
            suggestion = {
                "street_1": _text(raw.get("street_1"), 100), "street_2": _text(raw.get("street_2"), 100),
                "city": _text(raw.get("city"), 100), "state": _text(raw.get("state"), 2).upper(),
                "postal": _text(raw.get("postal"), 10), "postal_sub": _text(raw.get("postal_sub"), 10), "country": "US",
            }
            if all(suggestion[key] for key in ("street_1", "city", "state", "postal")):
                suggestions.append(suggestion)
        if verified and not suggestions:
            suggestions = [{key: address.get(key, "") for key in ("street_1", "street_2", "city", "state", "postal", "country")}]
        return {"verified": verified, "suggestions": suggestions}
    except Exception:
        logger.exception("Shipping address verification failed")
        return JSONResponse(status_code=503, content={"detail": "Address verification is temporarily unavailable. Try again."})


@router.post("/payment")
async def payment(request: Request, x_internal_api_key: Optional[str] = Header(default=None)):
    denied = _auth(request, x_internal_api_key)
    if denied: return denied
    body, bad = await _json_body(request)
    if bad: return bad
    record = load_record(_text(body.get("purchase_id"), 80))
    if not record or not token_matches(record, _text(body.get("access_token"), 200)): return JSONResponse(status_code=404, content={"detail": "Purchase not found."})
    chosen = next((r for r in record["rates"] if r["id"] == _text(body.get("rate_id"), 100)), None)
    if not chosen: return JSONResponse(status_code=400, content={"detail": "Choose an available rate."})
    key, stripe = publishable_key(), stripe_client()
    if not key or not stripe.is_configured: return JSONResponse(status_code=503, content={"detail": "Secure payment is not configured yet."})
    try:
        intent = stripe.create_payment_intent(amount_cents=chosen["amount_cents"], purchase_id=record["purchase_id"])
        record.update({"rate": chosen, "amount_cents": chosen["amount_cents"], "payment_intent_id": intent["id"], "status": "payment_pending"})
        save_record(record)
        return {"client_secret": intent["client_secret"], "publishable_key": key}
    except Exception:
        logger.exception("Shipping label payment setup failed")
        return JSONResponse(status_code=503, content={"detail": "Secure payment is temporarily unavailable."})


@router.post("/purchase")
async def purchase(request: Request, x_internal_api_key: Optional[str] = Header(default=None)):
    denied = _auth(request, x_internal_api_key)
    if denied: return denied
    body, bad = await _json_body(request)
    if bad: return bad
    record = load_record(_text(body.get("purchase_id"), 80))
    if not record or not token_matches(record, _text(body.get("access_token"), 200)): return JSONResponse(status_code=404, content={"detail": "Purchase not found."})
    if record.get("status") == "purchased": return _public(record)
    try:
        intent = stripe_client().retrieve_payment_intent(record["payment_intent_id"])
        if intent.get("status") != "succeeded" or int(intent.get("amount_received") or 0) != record["amount_cents"] or (intent.get("metadata") or {}).get("shipping_label_purchase_id") != record["purchase_id"]:
            return JSONResponse(status_code=409, content={"detail": "Payment has not completed."})
        data = live_wms().purchase_label(shipment_id=record["shipment_id"], rate_id=record["rate"]["id"])
        label = (data.get("models") or [{}])[0]
        images = label.get("images") or data.get("images") or []
        image = images[0] if isinstance(images, list) and images else images if isinstance(images, str) else ""
        record.update({"status": "purchased", "label_id": _text(label.get("id"), 100), "carrier": record["rate"]["carrier"], "service": record["rate"]["service"], "tracking": _text(label.get("tracking"), 120), "label_image": image})
        save_record(record)
        return _public(record)
    except Exception:
        logger.exception("Paid shipping label fulfillment failed")
        refunded = False
        try:
            stripe_client().refund_payment(payment_intent_id=record["payment_intent_id"], purchase_id=record["purchase_id"])
            refunded = True
        except Exception:
            logger.exception("Automatic refund after label failure also failed")
        record.update({"status": "refunded" if refunded else "fulfillment_failed", "error": "The label could not be created. Your payment was refunded." if refunded else "Payment succeeded, but the label could not be created. Contact support with your purchase ID."})
        save_record(record)
        return JSONResponse(status_code=502, content=_public(record))


@router.get("/{purchase_id}")
async def status(purchase_id: str, request: Request, token: str = "", x_internal_api_key: Optional[str] = Header(default=None)):
    denied = _auth(request, x_internal_api_key)
    if denied: return denied
    record = load_record(_text(purchase_id, 80))
    if not record or not token_matches(record, token): return JSONResponse(status_code=404, content={"detail": "Purchase not found."})
    return _public(record)


@router.post("/{purchase_id}/void")
async def void(purchase_id: str, request: Request, x_internal_api_key: Optional[str] = Header(default=None)):
    denied = _auth(request, x_internal_api_key)
    if denied: return denied
    body, bad = await _json_body(request)
    if bad: return bad
    record = load_record(_text(purchase_id, 80))
    if not record or not token_matches(record, _text(body.get("access_token"), 200)): return JSONResponse(status_code=404, content={"detail": "Purchase not found."})
    if record.get("status") == "voided": return _public(record)
    if record.get("status") != "purchased": return JSONResponse(status_code=409, content={"detail": "Only purchased labels can be voided."})
    try:
        live_wms().void_label(record["label_id"])
        stripe_client().refund_payment(payment_intent_id=record["payment_intent_id"], purchase_id=record["purchase_id"])
        record["status"] = "voided"; save_record(record); return _public(record)
    except Exception:
        logger.exception("Shipping label void/refund failed")
        return JSONResponse(status_code=502, content={"detail": "We could not complete the void and refund. Contact support with your purchase ID."})
