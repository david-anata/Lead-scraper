"""Allowlisted public payloads for the anatainc.com Rate Sheet experience.

The generator summary contains internal pricing, narrative, provider, and sales
fields.  This module is the only bridge from that summary to the marketing
site.  Keep the serializer explicit so adding an internal field upstream can
never make it public by accident.
"""

from __future__ import annotations

from typing import Any

from .schema import ANATA_HQ_ZIP, RATE_SOURCE_WMS, RateMatrix
from .zip3_centroids import ZIP3_CENTROIDS


def _coordinate(zip_code: str) -> dict[str, float] | None:
    point = ZIP3_CENTROIDS.get(str(zip_code or "")[:3])
    if point is None:
        return None
    return {"latitude": round(float(point[0]), 4), "longitude": round(float(point[1]), 4)}


def serialize_public_matrix(summary: dict[str, Any], *, preview: bool) -> dict[str, Any] | None:
    """Return the customer-safe live matrix, or ``None`` when it is not live."""
    if str(summary.get("rates_source") or "") != RATE_SOURCE_WMS:
        return None

    matrix = RateMatrix.from_dict(dict(summary.get("rate_matrix") or {}))
    if matrix.source != RATE_SOURCE_WMS or not matrix.products:
        return None

    profile = dict(summary.get("prospect_profile") or {})
    profile_products = [item for item in (profile.get("products") or []) if isinstance(item, dict)]
    products: list[dict[str, Any]] = []
    destinations_by_zone: dict[int, dict[str, Any]] = {}
    quotes: list[dict[str, Any]] = []

    for product_index, product_rates in enumerate(matrix.products, start=1):
        product_id = f"product-{product_index}"
        product = product_rates.product
        products.append(
            {
                "id": product_id,
                "name": product.name or f"Package {product_index}",
                "package": {
                    "length_in": product.length_in,
                    "width_in": product.width_in,
                    "height_in": product.height_in,
                    "weight_lb": product.weight_lb,
                    "estimated": bool(product.dims_estimated),
                },
            }
        )
        for zone_rates in product_rates.zones:
            zone = int(zone_rates.zone)
            destination_id = f"zone-{zone}"
            if zone not in destinations_by_zone:
                destination: dict[str, Any] = {
                    "id": destination_id,
                    "label": zone_rates.dest_label or f"Zone {zone}",
                    "zone": zone,
                }
                coordinate = _coordinate(zone_rates.dest_zip)
                if coordinate:
                    destination.update(coordinate)
                destinations_by_zone[zone] = destination

            live_quotes = [quote for quote in zone_rates.quotes if quote.source == RATE_SOURCE_WMS and quote.rate_usd > 0]
            if not live_quotes:
                continue
            lowest = min(quote.rate_usd for quote in live_quotes)
            transit_values = [quote.transit_days for quote in live_quotes if quote.transit_days is not None]
            fastest = min(transit_values) if transit_values else None
            for quote in live_quotes:
                quotes.append(
                    {
                        "product_id": product_id,
                        "destination_id": destination_id,
                        "zone": zone,
                        "carrier": quote.carrier,
                        "service": quote.service,
                        "rate_usd": round(float(quote.rate_usd), 2),
                        "transit_days": quote.transit_days,
                        "is_lowest_cost": quote.rate_usd == lowest,
                        "is_fastest": fastest is not None and quote.transit_days == fastest,
                    }
                )

    if not quotes:
        return None

    origin_zip = str(matrix.origin_zip or summary.get("origin_zip") or "")
    origin: dict[str, Any] = {
        "zip": origin_zip,
        "label": "Lehi, UT" if origin_zip == ANATA_HQ_ZIP else f"Your dock, ZIP {origin_zip}",
    }
    origin_coordinate = _coordinate(origin_zip)
    if origin_coordinate:
        origin.update(origin_coordinate)

    payload: dict[str, Any] = {
        "rates_source": "live",
        "brand_name": str(summary.get("prospect") or profile.get("brand") or profile.get("company") or "").strip(),
        "preview": bool(preview),
        "preview_product_count": len(products),
        "origin": origin,
        "products": products,
        "destinations": [destinations_by_zone[key] for key in sorted(destinations_by_zone)],
        "quotes": quotes,
        "excludes_3pl_fees": True,
    }
    if profile_products:
        payload["catalog_product_count"] = len(profile_products)
    generated_at = str(summary.get("published_at") or "").strip()
    if generated_at:
        payload["generated_at"] = generated_at
    return payload


PUBLIC_MATRIX_KEYS = {
    "rates_source", "brand_name", "preview", "preview_product_count",
    "catalog_product_count", "origin", "products", "destinations", "quotes",
    "excludes_3pl_fees", "generated_at",
}
