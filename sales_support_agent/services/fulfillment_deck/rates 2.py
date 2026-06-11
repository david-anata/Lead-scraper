"""Build the zone-by-zone RateMatrix for a prospect's products.

Resilient by design: bad origin ZIPs fall back to Anata HQ, products with
incomplete package specs are skipped with a warning, duplicate package
specs are quoted once, and a real WMS client that isn't wired up yet
(NotImplementedError) degrades to deterministic mock rates. Plain quote
failures warn and skip just that product x zone cell.
"""

from __future__ import annotations

from .schema import ANATA_HQ_ZIP, ProductRates, RateMatrix, ZoneRates, clean_zip
from .wms_client import MockWMSClient
from .zip3_centroids import ZIP3_CENTROIDS
from .zones import representative_destinations


def build_rate_matrix(products, origin_zip, client) -> tuple:
    """Quote every kept product against one representative metro per zone.

    Returns ``(RateMatrix, warnings)``. Never raises on per-cell quote
    failures — they are reported in the warnings list instead.
    """
    warnings: list = []

    origin = clean_zip(origin_zip)
    if origin is None or origin[:3] not in ZIP3_CENTROIDS:
        warnings.append(
            f"Origin ZIP {origin_zip!r} is invalid or unknown — using Anata HQ ({ANATA_HQ_ZIP})"
        )
        origin = ANATA_HQ_ZIP

    # Keep only fully-specified products, deduping identical package specs.
    kept: list = []
    seen_dims: dict = {}
    for product in products:
        if not product.has_full_package_spec:
            warnings.append(
                f"Product '{product.name}' missing dims/weight — excluded from rate matrix"
            )
            continue
        first = seen_dims.get(product.dims_key)
        if first is not None:
            warnings.append(
                f"Product '{product.name}' has identical package spec to '{first.name}'; "
                "rates shown once"
            )
            continue
        seen_dims[product.dims_key] = product
        kept.append(product)

    dests = representative_destinations(origin)

    active_client = client
    wms_fallback_done = False
    product_rates: list = []
    for product in kept:
        zone_rates: list = []
        for zone in sorted(dests):
            dest_zip, dest_label = dests[zone]
            try:
                quotes = active_client.quote_rates(product, origin, dest_zip)
            except NotImplementedError as exc:
                if not wms_fallback_done:
                    warnings.append(f"WMS client unavailable ({exc}) — using sample rates")
                    wms_fallback_done = True
                active_client = MockWMSClient()
                try:
                    quotes = active_client.quote_rates(product, origin, dest_zip)
                except Exception as exc2:  # pragma: no cover - mock never raises
                    warnings.append(
                        f"Rate quote failed for '{product.name}' zone {zone} ({dest_label}): {exc2}"
                    )
                    continue
            except Exception as exc:
                warnings.append(
                    f"Rate quote failed for '{product.name}' zone {zone} ({dest_label}): {exc}"
                )
                continue
            if not quotes:
                continue
            zone_rates.append(
                ZoneRates(
                    zone=zone,
                    dest_zip=dest_zip,
                    dest_label=dest_label,
                    quotes=tuple(sorted(quotes, key=lambda q: q.rate_usd)),
                )
            )
        zone_rates.sort(key=lambda z: z.zone)
        product_rates.append(ProductRates(product=product, zones=tuple(zone_rates)))

    return RateMatrix(origin_zip=origin, products=tuple(product_rates)), warnings
