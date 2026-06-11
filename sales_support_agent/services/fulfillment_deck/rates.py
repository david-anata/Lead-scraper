"""Build the zone-by-zone RateMatrix for a prospect's products.

Resilient by design: bad origin ZIPs fall back to Anata HQ, products with
incomplete package specs are skipped with a warning, duplicate package
specs are quoted once, and a real WMS client that isn't wired up yet
(NotImplementedError) degrades to deterministic mock rates. Plain quote
failures warn and skip just that product x zone cell.

The real EliteWorks client takes ~7s per quote call, so every
(product x zone) cell is quoted concurrently (ThreadPoolExecutor) and the
results are assembled deterministically afterwards. The raw carrier dump
(~27 services per call) never leaves this module: ``select_display_quotes``
trims each cell to the cheapest service per carrier and caps the carrier
count, so rendering stays tight.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from .schema import ANATA_HQ_ZIP, ProductRates, RateMatrix, ZoneRates, clean_zip
from .wms_client import MockWMSClient
from .zip3_centroids import ZIP3_CENTROIDS
from .zones import representative_destinations

_MAX_QUOTE_WORKERS = 6


def select_display_quotes(matrix: RateMatrix, max_carriers: int = 5) -> RateMatrix:
    """Trim a full rate matrix down to what the rate sheet displays.

    Per product: each zone keeps only the CHEAPEST quote per carrier, then
    the product is capped to at most ``max_carriers`` carriers, chosen by
    their average best-rate across zones (cheapest carriers first). Quotes
    in each rebuilt zone stay sorted by rate ascending. Cells left with no
    quotes are dropped (matching build_rate_matrix's empty-cell behaviour).
    """
    products_out = []
    for product_rates in matrix.products:
        # zone -> {carrier: cheapest quote for that carrier in that zone}
        per_zone_best: list[tuple[ZoneRates, dict]] = []
        for zone in product_rates.zones:
            best: dict = {}
            for quote in zone.quotes:
                current = best.get(quote.carrier)
                if current is None or quote.rate_usd < current.rate_usd:
                    best[quote.carrier] = quote
            per_zone_best.append((zone, best))

        totals: dict = {}
        counts: dict = {}
        for _zone, best in per_zone_best:
            for carrier, quote in best.items():
                totals[carrier] = totals.get(carrier, 0.0) + quote.rate_usd
                counts[carrier] = counts.get(carrier, 0) + 1
        keep = set(
            sorted(totals, key=lambda c: (totals[c] / counts[c], c))[:max_carriers]
        )

        zones_out = []
        for zone, best in per_zone_best:
            quotes = tuple(
                sorted(
                    (q for carrier, q in best.items() if carrier in keep),
                    key=lambda q: q.rate_usd,
                )
            )
            if not quotes:
                continue
            zones_out.append(
                ZoneRates(
                    zone=zone.zone,
                    dest_zip=zone.dest_zip,
                    dest_label=zone.dest_label,
                    quotes=quotes,
                )
            )
        products_out.append(
            ProductRates(product=product_rates.product, zones=tuple(zones_out))
        )
    return RateMatrix(origin_zip=matrix.origin_zip, products=tuple(products_out))


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
    zones_sorted = sorted(dests)

    # Quote every (product x zone) cell concurrently — the real client takes
    # ~7s per call, so a 2-product sheet would otherwise take minutes. Threads
    # only wrap client.quote_rates; everything else stays deterministic.
    cell_results: dict = {}
    cells = [(index, zone) for index in range(len(kept)) for zone in zones_sorted]
    if cells:
        with ThreadPoolExecutor(max_workers=_MAX_QUOTE_WORKERS) as pool:
            futures = {
                key: pool.submit(client.quote_rates, kept[key[0]], origin, dests[key[1]][0])
                for key in cells
            }
            for key, future in futures.items():
                try:
                    cell_results[key] = ("ok", future.result())
                except NotImplementedError as exc:
                    cell_results[key] = ("not_implemented", exc)
                except Exception as exc:  # noqa: BLE001 — per-cell resilience
                    cell_results[key] = ("error", exc)

    # Assemble in deterministic (product, zone) order, applying fallbacks.
    mock_client = None
    wms_fallback_done = False
    product_rates: list = []
    for index, product in enumerate(kept):
        zone_rates: list = []
        for zone in zones_sorted:
            dest_zip, dest_label = dests[zone]
            status, value = cell_results[(index, zone)]
            if status == "not_implemented":
                if not wms_fallback_done:
                    warnings.append(f"WMS client unavailable ({value}) — using sample rates")
                    wms_fallback_done = True
                if mock_client is None:
                    mock_client = MockWMSClient()
                try:
                    quotes = mock_client.quote_rates(product, origin, dest_zip)
                except Exception as exc2:  # pragma: no cover - mock never raises
                    warnings.append(
                        f"Rate quote failed for '{product.name}' zone {zone} ({dest_label}): {exc2}"
                    )
                    continue
            elif status == "error":
                warnings.append(
                    f"Rate quote failed for '{product.name}' zone {zone} ({dest_label}): {value}"
                )
                continue
            else:
                quotes = value
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

    matrix = RateMatrix(origin_zip=origin, products=tuple(product_rates))
    return select_display_quotes(matrix), warnings
