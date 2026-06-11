"""Rate-quote clients for the Fulfillment Rate Sheet generator.

``AnataWMSClient`` is the placeholder for the real Anata WMS rate API —
the endpoint spec is still pending, so ``quote_rates`` raises
``NotImplementedError`` until API docs arrive. Configuration env vars
(read by :func:`get_wms_client`):

  * ``ANATA_WMS_BASE_URL``       — base URL of the WMS rate API (presence
                                   selects the real client)
  * ``ANATA_WMS_ACCOUNT_NUMBER`` — Anata account number
  * ``ANATA_WMS_API_KEY``        — API key
  * ``ANATA_WMS_API_PASSWORD``   — API password

``MockWMSClient`` produces deterministic, plausible small-parcel rates so
the rest of the pipeline (and the generated rate sheet) works end-to-end
before the real integration lands. Every mock quote is tagged
``source="mock"`` so downstream rendering can label sample data honestly.
"""

from __future__ import annotations

import math
import os

from .schema import ProductSpec, RateQuote, RATE_SOURCE_MOCK
from .zones import zone_for

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
    """Client for the real Anata WMS rate endpoint (spec pending)."""

    def __init__(self, base_url: str, account_number: str, api_key: str, api_password: str):
        self.base_url = base_url
        self.account_number = account_number
        self.api_key = api_key
        self.api_password = api_password

    def quote_rates(self, package: ProductSpec, origin_zip: str, dest_zip: str) -> list:
        raise NotImplementedError(
            "Anata WMS rate endpoint spec not yet configured — awaiting API docs; "
            "falling back to MockWMSClient is handled by get_wms_client()"
        )


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
