"""Rate-quote clients for the Fulfillment Rate Sheet generator.

``AnataWMSClient`` talks to Anata's EliteWorks white-label shipping API at
app.anatainc.com (docs: https://app.anatainc.com/apidocs). Flow:

  1. ``POST /api/auth/tokens/generate`` with
     ``Authorization: Basic base64(API_KEY:API_PASSWORD)`` -> short-lived
     access token (cached process-wide until ~60s before expiry).
  2. ``POST /api/account/add`` with ``Authorization: Bearer {token}`` +
     ``AccountID`` header and ``{"class_key": "shipment", "model": {...}}``
     where the model has ``rate: true`` and ``purchase: false`` — rating
     creates a shipment object for quoting only; nothing is ever purchased.
     Response ``data.rates[]``: carrier / service / rate / delivery_days.

Configuration env vars (read by :func:`get_wms_client`):

  * ``ANATA_WMS_BASE_URL``       — https://app.anatainc.com (presence
                                   selects the real client)
  * ``ANATA_WMS_ACCOUNT_NUMBER`` — AccountID header value
  * ``ANATA_WMS_API_KEY``        — API key
  * ``ANATA_WMS_API_PASSWORD``   — API password

``MockWMSClient`` produces deterministic, plausible small-parcel rates so
the rest of the pipeline (and the generated rate sheet) works end-to-end
without credentials. Every mock quote is tagged ``source="mock"`` so
downstream rendering can label sample data honestly.
"""

from __future__ import annotations

import base64
import logging
import math
import os
import threading
import time

from .schema import ANATA_HQ_ZIP, ProductSpec, RateQuote, RATE_SOURCE_MOCK, RATE_SOURCE_WMS
from .zones import REPRESENTATIVE_METROS, zone_for

logger = logging.getLogger(__name__)

# dest zip -> (city, state) for the representative metros we quote against.
_METRO_BY_ZIP: dict = {
    zip_code: (label.rsplit(",", 1)[0].strip(), label.rsplit(",", 1)[1].strip())
    for zip_code, label in REPRESENTATIVE_METROS
}

_FROM_ADDRESS = {
    "street_1": "1657 N. State Street",
    "city": "Lehi",
    "state": "UT",
    "postal": ANATA_HQ_ZIP,
    "country": "US",
    "name": "Anata Fulfillment",
    "company": "Anata",
}

# Divisor for dimensional weight (industry-standard retail divisor, in^3/lb).
_DIM_DIVISOR = 139.0

# carrier, service, base $, $ per billed lb, $ added per zone above zone 1
_MOCK_RATE_TABLE = (
    ("USPS", "Ground Advantage", 5.20, 0.55, 0.65),
    ("UPS", "Ground", 7.10, 0.70, 0.85),
    ("FedEx", "Home Delivery", 6.90, 0.68, 0.80),
    ("USPS", "Priority Mail", 8.10, 0.90, 1.05),
)

# zone -> transit days for ground-type services
_GROUND_TRANSIT = {1: 1, 2: 2, 3: 2, 4: 3, 5: 3, 6: 4, 7: 4, 8: 5}
# zone -> transit days for USPS Priority Mail (faster, flatter curve)
_PRIORITY_TRANSIT = {1: 1, 2: 1, 3: 2, 4: 2, 5: 2, 6: 3, 7: 3, 8: 3}


def _billed_weight_lb(package: ProductSpec) -> int:
    """Billed weight: max(actual, dimensional L*W*H/139), rounded up."""
    actual = package.weight_lb or 0.0
    dims = (package.length_in or 0.0) * (package.width_in or 0.0) * (package.height_in or 0.0)
    dim_weight = dims / _DIM_DIVISOR if dims else 0.0
    return max(1, math.ceil(max(actual, dim_weight)))


class AnataWMSClient:
    """Client for Anata's EliteWorks shipping API (rate quotes only)."""

    _token_lock = threading.Lock()
    _token_cache: dict = {}  # base_url+key -> {"token": str, "expires": epoch}

    def __init__(self, base_url: str, account_number: str, api_key: str, api_password: str):
        self.base_url = base_url.rstrip("/")
        self.account_number = account_number
        self.api_key = api_key
        self.api_password = api_password

    # -- auth ---------------------------------------------------------------

    def _access_token(self) -> str:
        cache_key = f"{self.base_url}:{self.api_key}"
        with self._token_lock:
            cached = self._token_cache.get(cache_key)
            if cached and cached["expires"] - time.time() > 60:
                return cached["token"]
        import requests

        basic = base64.b64encode(f"{self.api_key}:{self.api_password}".encode()).decode()
        response = requests.post(
            f"{self.base_url}/api/auth/tokens/generate",
            headers={"Authorization": f"Basic {basic}"},
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("result") != "success":
            raise RuntimeError(f"EliteWorks token generation failed: {payload.get('message', '')[:200]}")
        access = payload["data"]["tokens"]["access"]
        with self._token_lock:
            self._token_cache[cache_key] = {"token": access["token"], "expires": float(access["expires"])}
        return access["token"]

    # -- rating -------------------------------------------------------------

    def quote_rates(self, package: ProductSpec, origin_zip: str, dest_zip: str) -> list:
        import requests

        city_state = _METRO_BY_ZIP.get(dest_zip)
        if city_state is None:
            raise ValueError(f"No representative city configured for destination ZIP {dest_zip}")
        city, state = city_state

        from_address = dict(_FROM_ADDRESS)
        if origin_zip and origin_zip != ANATA_HQ_ZIP:
            # Custom origin: account defaults still apply server-side; we only
            # override the postal so zone math and rating use the right lane.
            from_address = {**from_address, "postal": origin_zip, "street_1": "Origin", "city": "", "state": ""}

        body = {
            "class_key": "shipment",
            "model": {
                "rate": True,
                "purchase": False,
                "reference": "rate-sheet-estimate",
                "to_address": {
                    "street_1": "100 Main St",
                    "city": city,
                    "state": state,
                    "postal": dest_zip,
                    "country": "US",
                    "name": "Rate Estimate",
                },
                "from_address": from_address,
                "shipment_packages": [
                    {
                        "type": "Parcel",
                        "length": package.length_in,
                        "width": package.width_in,
                        "height": package.height_in,
                        "weight": package.weight_lb,
                        "contents_value": 25,
                        "shipment_items": [
                            {
                                "name": (package.name or "Product")[:60],
                                "sku": "rate-estimate",
                                "quantity": 1,
                                "value": 25,
                            }
                        ],
                    }
                ],
            },
        }
        response = requests.post(
            f"{self.base_url}/api/account/add",
            headers={
                "Authorization": f"Bearer {self._access_token()}",
                "AccountID": self.account_number,
                "Content-Type": "application/json",
            },
            json=body,
            timeout=60,
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("result") != "success":
            raise RuntimeError(f"EliteWorks rating failed: {payload.get('message', '')[:200]}")

        zone = zone_for(origin_zip, dest_zip)
        quotes = []
        for rate in (payload.get("data") or {}).get("rates") or []:
            try:
                rate_usd = round(float(rate.get("rate")), 2)
            except (TypeError, ValueError):
                continue
            if rate_usd <= 0:
                continue
            transit = rate.get("delivery_days")
            try:
                transit = int(transit) if transit is not None else None
            except (TypeError, ValueError):
                transit = None
            quotes.append(
                RateQuote(
                    carrier=str(rate.get("carrier") or "").strip()[:60],
                    service=str(rate.get("service") or "").strip()[:80],
                    rate_usd=rate_usd,
                    transit_days=transit,
                    zone=zone,
                    source=RATE_SOURCE_WMS,
                )
            )
        return quotes


class MockWMSClient:
    """Deterministic sample rates — pure function of the inputs.

    No randomness, no clock reads: identical calls always return identical
    quotes, which keeps generated rate sheets reproducible and testable.
    """

    def quote_rates(self, package: ProductSpec, origin_zip: str, dest_zip: str) -> list:
        zone = zone_for(origin_zip, dest_zip) or 5
        billed = _billed_weight_lb(package)
        quotes = []
        for carrier, service, base, per_lb, zone_step in _MOCK_RATE_TABLE:
            rate = base + per_lb * billed + zone_step * (zone - 1)
            transit_table = _PRIORITY_TRANSIT if service == "Priority Mail" else _GROUND_TRANSIT
            quotes.append(
                RateQuote(
                    carrier=carrier,
                    service=service,
                    rate_usd=round(rate, 2),
                    transit_days=transit_table.get(zone, 5),
                    zone=zone,
                    source=RATE_SOURCE_MOCK,
                )
            )
        return quotes


def get_wms_client() -> object:
    """Real WMS client when ANATA_WMS_BASE_URL is configured, else mock."""
    base_url = os.environ.get("ANATA_WMS_BASE_URL")
    if base_url:
        return AnataWMSClient(
            base_url=base_url,
            account_number=os.environ.get("ANATA_WMS_ACCOUNT_NUMBER", ""),
            api_key=os.environ.get("ANATA_WMS_API_KEY", ""),
            api_password=os.environ.get("ANATA_WMS_API_PASSWORD", ""),
        )
    return MockWMSClient()
