"""Interactive ZIP-level rate map for the rate sheet.

Per David: not state averages — distance and ZIP code. Every assigned US
3-digit ZIP prefix (~900) renders as its own cell, colored by what it costs
to ship the selected product there (zone-band rate from the live quotes).
v4: the lower-48 state outlines (Wikimedia public-domain paths in
us_states_svg) render BEHIND the dots for geographic orientation — the dots
are transformed from our Albers projection into the Wikimedia 959x593 space
with a least-squares affine fit (see _fit_affine). Concentric mileage rings
radiate from the ship-from origin. Hover any cell for the area name, ZIP
prefix, true straight-line miles, zone, and rate. A Cost / Transit-time
toggle recolors the dots; the carrier filter chips live in the map controls
and drive both the map repaint and the rate-table columns below. The viewer
can edit dims/weight and press "Request rates" — the whole sheet re-quotes
live and saves.

Alaska / Hawaii / Puerto Rico & USVI render as compact inset rows (top-right,
clear of the Wikimedia AK/HI inset outlines at bottom-left) — their prefixes
are still real, individually hoverable cells.

Data semantics are unchanged from v3: every dot keeps its exact per-zip
straight-line distance; the displayed rate is the zone-band rate (one quote
per zone), not a per-zip quote.
"""

from __future__ import annotations

import html as _html
import json
import math
import re

from sales_support_agent.services.fulfillment_deck.schema import RateMatrix, clean_zip
from sales_support_agent.services.fulfillment_deck.us_states_svg import (
    STATE_PATHS,
    VIEWBOX,
)
from sales_support_agent.services.fulfillment_deck.zip3_centroids import ZIP3_CENTROIDS
from sales_support_agent.services.fulfillment_deck.zip3_names import ZIP3_NAMES
from sales_support_agent.services.fulfillment_deck.zones import haversine_miles, zone_for

# state -> representative metro ZIP (still used for zone bucketing elsewhere).
STATE_REP_ZIPS: dict[str, str] = {
    "AL": "35203", "AK": "99501", "AZ": "85004", "AR": "72201", "CA": "90012",
    "CO": "80202", "CT": "06103", "DE": "19801", "DC": "20001", "FL": "33101",
    "GA": "30303", "HI": "96813", "ID": "83702", "IL": "60601", "IN": "46204",
    "IA": "50309", "KS": "67202", "KY": "40202", "LA": "70112", "ME": "04101",
    "MD": "21201", "MA": "02108", "MI": "48226", "MN": "55401", "MS": "39201",
    "MO": "63101", "MT": "59601", "NE": "68102", "NV": "89101", "NH": "03101",
    "NJ": "07102", "NM": "87102", "NY": "10001", "NC": "28202", "ND": "58102",
    "OH": "43215", "OK": "73102", "OR": "97201", "PA": "19102", "RI": "02903",
    "SC": "29201", "SD": "57104", "TN": "37203", "TX": "75201", "UT": "84101",
    "VT": "05401", "VA": "23219", "WA": "98101", "WV": "25301", "WI": "53202",
    "WY": "82001",
}

# Rate ramp: cheapest -> most expensive (brand sky into brand ink).
RATE_RAMP = ["#e7f1f9", "#cfe3f2", "#aed1e8", "#85bbda", "#5f9cc7", "#4f84c4", "#33598f", "#1d2d44"]

# Carrier wordmark chips: UPPERCASE carrier name -> (background, text color).
# Unknown carriers (YSP etc.) fall back to brand ink with white text.
CARRIER_BRAND_COLORS: dict[str, tuple[str, str]] = {
    "UPS": ("#351C15", "#FFB500"),
    "USPS": ("#004B87", "#FFFFFF"),
    "FEDEX": ("#4D148C", "#FFFFFF"),
    "GLS": ("#061AB1", "#FFD100"),
    "UNIUNI": ("#00B8A9", "#FFFFFF"),
    "DHL": ("#FFCC00", "#D40511"),
}
_CARRIER_CHIP_FALLBACK = ("var(--anata-ink, #1d2d44)", "#FFFFFF")


def carrier_chip(carrier: str) -> str:
    """Small rounded pill with the carrier wordmark on its brand color."""
    key = (carrier or "").strip().upper()
    bg, fg = CARRIER_BRAND_COLORS.get(key, _CARRIER_CHIP_FALLBACK)
    return (
        f'<span class="carrier-chip" style="background:{bg};color:{fg}">'
        f"{_html.escape(key or 'CARRIER')}</span>"
    )

# Mileage rings drawn around the origin (matches the zone band edges).
RING_MILES = (150, 300, 600, 1000, 1400, 1800)

# Inset groups: prefixes outside the projected lower-48 canvas.
_AK_PREFIXES = tuple(p for p in ZIP3_CENTROIDS if p.startswith("99") and p >= "995")
_HI_PREFIXES = ("967", "968")
_CARIB_PREFIXES = ("006", "007", "008", "009")
_INSET_PREFIXES = set(_AK_PREFIXES) | set(_HI_PREFIXES) | set(_CARIB_PREFIXES)

# Albers equal-area conic (the standard US choropleth projection).
_P1, _P2 = math.radians(29.5), math.radians(45.5)   # standard parallels
_LAT0, _LON0 = math.radians(23.0), math.radians(-96.0)
_N = (math.sin(_P1) + math.sin(_P2)) / 2.0
_C = math.cos(_P1) ** 2 + 2.0 * _N * math.sin(_P1)
_RHO0 = math.sqrt(_C - 2.0 * _N * math.sin(_LAT0)) / _N


def _albers(lat: float, lon: float) -> tuple[float, float]:
    """Project lat/lon to unitless Albers x,y (y grows northward)."""
    phi, lam = math.radians(lat), math.radians(lon)
    rho = math.sqrt(max(_C - 2.0 * _N * math.sin(phi), 0.0)) / _N
    theta = _N * (lam - _LON0)
    return rho * math.sin(theta), _RHO0 - rho * math.cos(theta)


# ---------------------------------------------------------------------------
# Albers -> Wikimedia-space affine fit (v4 state-outline background)
# ---------------------------------------------------------------------------

# Canvas layout: the Wikimedia state-outline space (us_states_svg.VIEWBOX).
_W, _H = 959, 593
_CELL = 8.0  # cell size in px (shrunk from 9 — outlines make density visible)

_STATE_ABBRS = frozenset(STATE_PATHS)  # 50 states + DC
_NON_LOWER48 = frozenset({"AK", "HI"})

_PATH_TOKEN_RE = re.compile(r"([MmLlHhVvZz])|(-?(?:\d+\.?\d*|\.\d+))")


def zip3_state(prefix: str) -> str | None:
    """State abbreviation for a zip3 prefix, parsed from the LAST token of
    its ZIP3_NAMES label ("Springfield MA" -> "MA"). Returns None for
    unparseable labels (e.g. "US Virgin Islands")."""
    name = ZIP3_NAMES.get(prefix, "")
    token = name.rsplit(" ", 1)[-1] if name else ""
    return token if token in _STATE_ABBRS else None


def _path_points(d: str) -> list[tuple[float, float]]:
    """Vertices of an SVG path, supporting M/m, L/l, H/h, V/v, Z/z with
    comma/space-separated numbers and implicit lineto repeats — exactly the
    command vocabulary the Wikimedia state paths use (all-relative m/l/h/v/z;
    absolute variants handled for robustness)."""
    seq: list = []
    for letter, num in _PATH_TOKEN_RE.findall(d):
        seq.append(letter if letter else float(num))
    points: list[tuple[float, float]] = []
    cx = cy = sx = sy = 0.0
    cmd = ""
    i = 0
    while i < len(seq):
        token = seq[i]
        if isinstance(token, str):
            cmd = token
            i += 1
            if cmd in "Zz":
                cx, cy = sx, sy
                points.append((cx, cy))
            continue
        if cmd in "mM":
            x, y = seq[i], seq[i + 1]
            i += 2
            if cmd == "m":
                cx, cy = cx + x, cy + y
            else:
                cx, cy = x, y
            sx, sy = cx, cy
            points.append((cx, cy))
            cmd = "l" if cmd == "m" else "L"  # implicit lineto after moveto
        elif cmd in "lL":
            x, y = seq[i], seq[i + 1]
            i += 2
            if cmd == "l":
                cx, cy = cx + x, cy + y
            else:
                cx, cy = x, y
            points.append((cx, cy))
        elif cmd in "hH":
            x = seq[i]
            i += 1
            cx = cx + x if cmd == "h" else x
            points.append((cx, cy))
        elif cmd in "vV":
            y = seq[i]
            i += 1
            cy = cy + y if cmd == "v" else y
            points.append((cx, cy))
        else:  # pragma: no cover — vocabulary checked above
            raise ValueError(f"Unsupported path command {cmd!r}")
    return points


def path_bbox(abbr: str) -> tuple[float, float, float, float]:
    """(min_x, min_y, max_x, max_y) of a state's outline path in the
    Wikimedia 959x593 space."""
    points = _path_points(STATE_PATHS[abbr][1])
    xs = [p[0] for p in points]
    ys = [p[1] for p in points]
    return (min(xs), min(ys), max(xs), max(ys))


def _solve3(matrix: list[list[float]], rhs: list[float]) -> list[float]:
    """Solve a 3x3 linear system by Gaussian elimination (pure Python)."""
    m = [row[:] + [rhs[index]] for index, row in enumerate(matrix)]
    for col in range(3):
        pivot = max(range(col, 3), key=lambda r: abs(m[r][col]))
        m[col], m[pivot] = m[pivot], m[col]
        for row in range(3):
            if row == col:
                continue
            factor = m[row][col] / m[col][col]
            for c in range(col, 4):
                m[row][c] -= factor * m[col][c]
    return [m[index][3] / m[index][index] for index in range(3)]


def _fit_affine() -> tuple[tuple[float, ...], tuple[float, ...], float]:
    """Least-squares 6-param affine from Albers space to Wikimedia px space.

    Anchors: per lower-48 state (+DC), the mean Albers position of its zip3
    centroids paired with the bbox center of its Wikimedia outline path.
    Two-pass fit: fit all ~49 anchors, drop outliers with residual > 30px
    (MI and CA — bbox centers far from their zip mass: MI's bbox spans the
    Upper Peninsula, CA's arc pulls its bbox center off-landmass), refit on
    the kept anchors. Measured max residual over kept anchors: 28.1px
    (worst: TX), comfortably under the 35px budget asserted in tests.

    Returns ((a, b, c), (d, e, f), max_kept_residual_px) for
    x' = a*x + b*y + c ; y' = d*x + e*y + f.
    """
    sums: dict[str, list[float]] = {}
    for prefix, (lat, lon) in ZIP3_CENTROIDS.items():
        state = zip3_state(prefix)
        if state is None or state in _NON_LOWER48:
            continue
        x, y = _albers(lat, lon)
        bucket = sums.setdefault(state, [0.0, 0.0, 0.0])
        bucket[0] += x
        bucket[1] += y
        bucket[2] += 1.0

    anchors = []
    for state in sorted(sums):
        sum_x, sum_y, count = sums[state]
        min_x, min_y, max_x, max_y = path_bbox(state)
        anchors.append((
            state, sum_x / count, sum_y / count,
            (min_x + max_x) / 2.0, (min_y + max_y) / 2.0,
        ))

    def fit(rows):
        sxx = sum(a[1] * a[1] for a in rows)
        sxy = sum(a[1] * a[2] for a in rows)
        syy = sum(a[2] * a[2] for a in rows)
        sx = sum(a[1] for a in rows)
        sy = sum(a[2] for a in rows)
        n = float(len(rows))
        lhs = [[sxx, sxy, sx], [sxy, syy, sy], [sx, sy, n]]
        coef_x = _solve3(lhs, [
            sum(a[1] * a[3] for a in rows),
            sum(a[2] * a[3] for a in rows),
            sum(a[3] for a in rows),
        ])
        coef_y = _solve3(lhs, [
            sum(a[1] * a[4] for a in rows),
            sum(a[2] * a[4] for a in rows),
            sum(a[4] for a in rows),
        ])
        return tuple(coef_x), tuple(coef_y)

    def residuals(rows, coef_x, coef_y):
        out = []
        for state, x, y, tx, ty in rows:
            px = coef_x[0] * x + coef_x[1] * y + coef_x[2]
            py = coef_y[0] * x + coef_y[1] * y + coef_y[2]
            out.append((math.dist((px, py), (tx, ty)), state))
        return out

    coef_x, coef_y = fit(anchors)
    first_pass = dict((state, r) for r, state in residuals(anchors, coef_x, coef_y))
    kept = [a for a in anchors if first_pass[a[0]] <= 30.0]
    if len(kept) >= 3:
        coef_x, coef_y = fit(kept)
    else:  # pragma: no cover — defensive
        kept = anchors
    max_residual = max(r for r, _state in residuals(kept, coef_x, coef_y))
    return coef_x, coef_y, max_residual


_AFFINE_X, _AFFINE_Y, AFFINE_MAX_RESIDUAL_PX = _fit_affine()


def albers_point_px(lat: float, lon: float) -> tuple[float, float]:
    """lat/lon -> Wikimedia-space pixel via the Albers projection + affine."""
    x, y = _albers(lat, lon)
    return (
        _AFFINE_X[0] * x + _AFFINE_X[1] * y + _AFFINE_X[2],
        _AFFINE_Y[0] * x + _AFFINE_Y[1] * y + _AFFINE_Y[2],
    )


def _px_per_mile() -> float:
    """Average affine scale: transform two Albers points 200 miles apart
    (UT origin latitude, due east) and measure the pixel distance."""
    lat, lon = ZIP3_CENTROIDS["841"]
    a = albers_point_px(lat, lon)
    b = albers_point_px(lat, lon + 200.0 / (69.172 * math.cos(math.radians(lat))))
    return math.dist(a, b) / 200.0


# Inset rows live top-right, clear of the Wikimedia AK/HI outlines that sit
# bottom-left in the 959x593 space.
_INSET_X = 786.0
_INSET_Y = 26.0
_INSET_LABEL_X = 754.0


def _build_cells(origin_zip: str) -> tuple[list[dict], dict, float]:
    """All zip3 cells with Wikimedia-space px coords, miles, zone. Returns
    (cells, origin_px, px_per_mile). Each dot keeps its EXACT per-zip
    straight-line distance — only the rate stays zone-band."""
    lower48 = {
        p: c for p, c in ZIP3_CENTROIDS.items() if p not in _INSET_PREFIXES
    }
    origin_centroid = ZIP3_CENTROIDS.get((clean_zip(origin_zip) or "841")[:3])
    if origin_centroid is None:
        origin_centroid = ZIP3_CENTROIDS["841"]
    o_lat, o_lon = origin_centroid

    cells: list[dict] = []
    for prefix, (lat, lon) in lower48.items():
        x, y = albers_point_px(lat, lon)
        cells.append({
            "p": prefix,
            "x": round(x, 1),
            "y": round(y, 1),
            "z": zone_for(origin_zip, prefix + "01"),
            "mi": int(round(haversine_miles(o_lat, o_lon, lat, lon))),
            "n": ZIP3_NAMES.get(prefix, ""),
        })

    # Insets: AK, HI, PR/VI as compact rows top-right.
    inset_groups = (("AK", _AK_PREFIXES), ("HI", _HI_PREFIXES), ("PR/VI", _CARIB_PREFIXES))
    iy = _INSET_Y
    for label, prefixes in inset_groups:
        ix = _INSET_X
        for prefix in prefixes:
            centroid = ZIP3_CENTROIDS.get(prefix)
            if centroid is None:
                continue
            cells.append({
                "p": prefix,
                "x": round(ix, 1),
                "y": round(iy, 1),
                "z": zone_for(origin_zip, prefix + "01"),
                "mi": int(round(haversine_miles(o_lat, o_lon, centroid[0], centroid[1]))),
                "n": ZIP3_NAMES.get(prefix, ""),
                "inset": label,
            })
            ix += _CELL + 2.5
        iy += _CELL + 7

    ox, oy = albers_point_px(o_lat, o_lon)
    return cells, {"x": round(ox, 1), "y": round(oy, 1)}, _px_per_mile()


def state_zone_map(origin_zip: str) -> dict[str, int]:
    """zone per state from the origin (legacy helper, still used in payload)."""
    zones: dict[str, int] = {}
    for abbr, rep_zip in STATE_REP_ZIPS.items():
        zone = zone_for(origin_zip, rep_zip)
        if zone is not None:
            zones[abbr] = zone
    return zones


def origin_state(origin_zip: str) -> str:
    prefix = (clean_zip(origin_zip) or "")[:3]
    for abbr, rep_zip in STATE_REP_ZIPS.items():
        if rep_zip[:3] == prefix:
            return abbr
    zones = state_zone_map(origin_zip)
    return min(zones, key=zones.get) if zones else ""


def map_payload(matrix: RateMatrix) -> dict:
    """The JSON the in-page JS needs: per-product, per-zone, PER-CARRIER
    cheapest quote — so the viewer-side carrier filter can recompute the
    best rate among enabled carriers without a round trip.

    Shape: products[].zoneRates = {"6": {"USPS": {rate, service,
    transit_days}, "UPS": {...}}} (cheapest quote per carrier per zone).
    """
    products = []
    for product_rates in matrix.products:
        product = product_rates.product
        zone_rates: dict[str, dict] = {}
        for zone in product_rates.zones:
            per_carrier: dict[str, dict] = {}
            for quote in zone.quotes:
                current = per_carrier.get(quote.carrier)
                if current is None or quote.rate_usd < current["rate"]:
                    per_carrier[quote.carrier] = {
                        "rate": quote.rate_usd,
                        "service": quote.service,
                        "transit_days": quote.transit_days,
                    }
            if per_carrier:
                zone_rates[str(zone.zone)] = per_carrier
        products.append({
            "name": product.name or "Product",
            "length_in": product.length_in,
            "width_in": product.width_in,
            "height_in": product.height_in,
            "weight_lb": product.weight_lb,
            "estimated": product.dims_estimated,
            "zoneRates": zone_rates,
        })
    return {
        "origin": matrix.origin_zip,
        "originState": origin_state(matrix.origin_zip),
        "stateZones": state_zone_map(matrix.origin_zip),
        "source": matrix.source,
        "products": products,
    }


def _matrix_carriers(matrix: RateMatrix) -> list[str]:
    """Every carrier present anywhere in the matrix, cheapest-average first."""
    totals: dict[str, float] = {}
    counts: dict[str, int] = {}
    for product_rates in matrix.products:
        for zone in product_rates.zones:
            best: dict[str, float] = {}
            for quote in zone.quotes:
                current = best.get(quote.carrier)
                if current is None or quote.rate_usd < current:
                    best[quote.carrier] = quote.rate_usd
            for carrier, rate in best.items():
                totals[carrier] = totals.get(carrier, 0.0) + rate
                counts[carrier] = counts.get(carrier, 0) + 1
    return sorted(totals, key=lambda c: (totals[c] / counts[c], c))


def render_carrier_filter(matrix: RateMatrix) -> str:
    """Viewer-local carrier toggle chips — one per carrier in the matrix.

    v4: rendered INSIDE the map section's controls (the map is the first
    thing a viewer plays with). All enabled by default; clicking toggles the
    carrier's table columns below AND the map's best-rate computation (one
    document-level delegated handler drives both). Never persisted, not
    admin-preselectable.
    """
    carriers = _matrix_carriers(matrix)
    if not carriers:
        return ""
    chips = "".join(
        f'<button type="button" class="cf-chip" data-carrier="{_html.escape(c, quote=True)}" '
        f'aria-pressed="true">{carrier_chip(c)}</button>'
        for c in carriers
    )
    return (
        f'<div class="carrier-filter" id="carrier-filter">'
        f'<span class="cf-label">Carriers:</span>{chips}</div>'
    )


def _render_mode_toggle() -> str:
    """Cost / Transit-time segmented toggle for the dot coloring."""
    return (
        '<div class="rm-mode" id="rm-mode" role="group" aria-label="Map coloring mode">'
        '<button type="button" class="active" data-mode="cost" aria-pressed="true">Cost</button>'
        '<button type="button" data-mode="transit" aria-pressed="false">Transit time</button>'
        "</div>"
    )


def render_interactive_rate_map(matrix: RateMatrix, origin_label: str,
                                requote_path: str = "") -> str:
    """The ZIP-level 'distance from our dock' interactive section body."""
    payload = map_payload(matrix)
    payload["requoteUrl"] = requote_path
    payload["carrierColors"] = {k: list(v) for k, v in CARRIER_BRAND_COLORS.items()}

    cells, origin_px, px_per_mile = _build_cells(matrix.origin_zip)

    # Lower-48 outlines behind the dots; AK/HI outlines kept at 50% opacity
    # (their dots live in the inset rows, not on the Wikimedia insets).
    state_paths = "".join(
        f'<path class="rm-state{" rm-state-faded" if abbr in _NON_LOWER48 else ""}" '
        f'd="{d}"><title>{_html.escape(name)}</title></path>'
        for abbr, (name, d) in sorted(STATE_PATHS.items())
    )

    cell_rects = "".join(
        f'<rect class="rm-cell" x="{c["x"] - _CELL / 2}" y="{c["y"] - _CELL / 2}" '
        f'width="{_CELL}" height="{_CELL}" rx="2.2" '
        f'data-p="{c["p"]}" data-z="{c["z"] if c["z"] is not None else ""}" '
        f'data-mi="{c["mi"]}" data-n="{_html.escape(c["n"], quote=True)}"></rect>'
        for c in cells
    )
    rings = "".join(
        f'<circle class="rm-ring" cx="{origin_px["x"]}" cy="{origin_px["y"]}" '
        f'r="{miles * px_per_mile:.1f}"></circle>'
        f'<text class="rm-ring-label" x="{origin_px["x"] + miles * px_per_mile * 0.7071 + 3:.1f}" '
        f'y="{origin_px["y"] - miles * px_per_mile * 0.7071 - 3:.1f}">{miles} mi</text>'
        for miles in RING_MILES
    )
    inset_labels = (
        f'<text class="rm-inset-label" x="{_INSET_LABEL_X}" y="{_INSET_Y + 3}">AK</text>'
        f'<text class="rm-inset-label" x="{_INSET_LABEL_X}" y="{_INSET_Y + 3 + (_CELL + 7)}">HI</text>'
        f'<text class="rm-inset-label" x="{_INSET_LABEL_X}" y="{_INSET_Y + 3 + 2 * (_CELL + 7)}">PR</text>'
    )
    origin_marker = (
        f'<circle cx="{origin_px["x"]}" cy="{origin_px["y"]}" r="5.5" fill="#bfa889" '
        f'stroke="#fffdf9" stroke-width="2"></circle>'
    )

    return f"""
      <div class="rm-wrap">
        <div class="rm-controls" id="rm-controls">
          <div class="rm-products" id="rm-products"></div>
          {render_carrier_filter(matrix)}
          {_render_mode_toggle()}
          <div class="rm-dims" id="rm-dims"></div>
          <div class="rm-status" id="rm-status"></div>
        </div>
        <div class="rm-map-holder">
          <svg id="rm-svg" viewBox="{VIEWBOX}" role="img"
               aria-label="Estimated shipping rates by ZIP prefix and distance from {_html.escape(origin_label)}">
            <g id="rm-states">{state_paths}</g>
            <g id="rm-rings">{rings}</g>
            <g id="rm-cells">{cell_rects}</g>
            {origin_marker}
            {inset_labels}
          </svg>
          <div class="rm-overlay" id="rm-overlay" hidden><div class="rm-spinner"></div></div>
          <div class="rm-tooltip" id="rm-tooltip" hidden></div>
          <div class="rm-legend" id="rm-legend"></div>
        </div>
      </div>
      <style>
        .rm-wrap {{ display: flex; flex-direction: column; gap: 14px; }}
        .rm-map-holder {{ position: relative; }}
        #rm-svg {{ width: 100%; height: auto; display: block; }}
        .rm-state {{ fill: #f3efe9; stroke: #fffdf9; stroke-width: 1.5; }}
        .rm-state-faded {{ opacity: 0.5; }}
        .rm-cell {{ fill: #eee9dc; cursor: pointer; }}
        .rm-cell:hover {{ stroke: #1d2d44; stroke-width: 1.5; }}
        .rm-mode {{ display: inline-flex; border: 1px solid var(--anata-line, rgba(29,45,68,0.18));
          border-radius: 999px; overflow: hidden; align-self: flex-start; }}
        .rm-mode button {{ border: none; background: #fff; padding: 6px 14px; font: inherit;
          font-size: 11.5px; font-weight: 700; cursor: pointer; color: var(--anata-ink, #1d2d44); }}
        .rm-mode button.active {{ background: var(--anata-ink, #1d2d44); color: #fffdf9; }}
        .rm-ring {{ fill: none; stroke: rgba(29,45,68,0.18); stroke-dasharray: 3 4; stroke-width: 1; }}
        .rm-ring-label {{ font-size: 10px; fill: rgba(29,45,68,0.45); font-weight: 600; }}
        .rm-inset-label {{ font-size: 10px; fill: #6b7688; font-weight: 700; }}
        .rm-tooltip {{ position: absolute; pointer-events: none; background: var(--anata-ink, #1d2d44);
          color: #fffdf9; border-radius: 10px; padding: 9px 13px; font-size: 12.5px; line-height: 1.45;
          box-shadow: 0 8px 22px rgba(29,45,68,0.35); max-width: 250px; z-index: 5; }}
        .rm-tooltip .rm-tt-state {{ font-weight: 700; font-size: 13px; }}
        .rm-tooltip .rm-tt-rate {{ font-size: 17px; font-weight: 700; color: #85bbda; }}
        .rm-overlay {{ position: absolute; inset: 0; background: rgba(255,253,249,0.6);
          display: flex; align-items: center; justify-content: center; z-index: 4; }}
        .rm-overlay[hidden] {{ display: none; }}
        .rm-spinner {{ width: 42px; height: 42px; border-radius: 50%; border: 4px solid rgba(29,45,68,0.15);
          border-top-color: #4f84c4; animation: rm-spin 0.9s linear infinite; }}
        @keyframes rm-spin {{ to {{ transform: rotate(360deg); }} }}
        .rm-products {{ display: flex; gap: 8px; flex-wrap: wrap; }}
        .rm-products button {{ border: 1px solid var(--anata-line, rgba(29,45,68,0.12)); background: #fff;
          border-radius: 999px; padding: 7px 16px; font: inherit; font-size: 12.5px; font-weight: 600;
          cursor: pointer; color: var(--anata-ink, #1d2d44); }}
        .rm-products button.active {{ background: var(--anata-ink, #1d2d44); color: #fffdf9;
          border-color: var(--anata-ink, #1d2d44); }}
        .rm-dims {{ display: flex; gap: 10px; flex-wrap: wrap; align-items: end; }}
        .rm-dims label {{ display: grid; gap: 3px; font-size: 11px; font-weight: 600;
          color: var(--anata-muted, #6b7688); }}
        .rm-dims input {{ width: 74px; padding: 7px 9px; border: 1px solid var(--anata-line, rgba(29,45,68,0.18));
          border-radius: 9px; font: inherit; font-size: 13px; }}
        .rm-dims .rm-est {{ background: #fff4d9; border: 1px solid #d2a94b; color: #7a5b14;
          border-radius: 999px; padding: 3px 10px; font-size: 10.5px; font-weight: 700;
          letter-spacing: 0.04em; text-transform: uppercase; align-self: center; }}
        .rm-dims .rm-request {{ background: var(--anata-ink, #1d2d44); color: #fffdf9; border: none;
          border-radius: 999px; padding: 9px 18px; font: inherit; font-size: 12.5px; font-weight: 700;
          cursor: pointer; }}
        .rm-dims .rm-request:disabled {{ opacity: 0.5; cursor: wait; }}
        .rm-dims .rm-reset {{ font-size: 11.5px; align-self: center; cursor: pointer;
          color: var(--anata-sky-deep, #4f84c4); text-decoration: underline; background: none;
          border: none; padding: 0; font-family: inherit; }}
        .rm-status {{ font-size: 12px; color: var(--anata-muted, #6b7688); min-height: 16px; }}
        .rm-legend {{ display: flex; align-items: center; gap: 4px; margin-top: 8px;
          font-size: 11px; color: var(--anata-muted, #6b7688); flex-wrap: wrap; }}
        .rm-legend .rm-chip {{ width: 34px; height: 12px; border-radius: 3px; display: inline-block; }}
        @media print {{
          .rm-controls {{ display: none !important; }}
          .rm-overlay {{ display: none !important; }}
        }}
      </style>
      <script>
        (function() {{
          var DATA = {json.dumps(payload)};
          var RAMP = {json.dumps(RATE_RAMP)};
          var ORIGINALS = JSON.parse(JSON.stringify(DATA.products));
          var selected = 0;
          var requoteUrl = DATA.requoteUrl || (window.location.pathname.replace(/\\/$/, '') + '/requote');
          var svg = document.getElementById('rm-svg');
          var tooltip = document.getElementById('rm-tooltip');
          var statusEl = document.getElementById('rm-status');
          var overlay = document.getElementById('rm-overlay');
          var edited = false;
          // Carrier filter state — viewer-local only, never persisted. Keys
          // are carrier names exactly as they appear in the rate data.
          var disabledCarriers = {{}};
          var CHIP_COLORS = DATA.carrierColors || {{}};
          // Dot coloring mode: 'cost' (best rate) or 'transit' (best days).
          var mode = 'cost';

          function fmt(rate) {{ return '$' + Number(rate).toFixed(2); }}

          function chipHtml(carrier) {{
            var key = String(carrier || '').toUpperCase().replace(/[<>&]/g, '');
            var col = CHIP_COLORS[key] || ['#1d2d44', '#ffffff'];
            return '<span style="display:inline-block;border-radius:999px;padding:3px 8px;'
              + 'font-size:10px;font-weight:800;letter-spacing:0.05em;'
              + 'background:' + col[0] + ';color:' + col[1] + '">' + key + '</span>';
          }}

          function bestForZone(perCarrier, honorFilter) {{
            var best = null, bestCarrier = null;
            Object.keys(perCarrier).forEach(function(carrier) {{
              if (honorFilter && disabledCarriers[carrier]) return;
              var q = perCarrier[carrier];
              if (!best || q.rate < best.rate) {{ best = q; bestCarrier = carrier; }}
            }});
            if (!best && honorFilter) return bestForZone(perCarrier, false);
            if (!best) return null;
            return {{ carrier: bestCarrier, rate: best.rate, service: best.service,
                     transit_days: best.transit_days }};
          }}

          function bestTransitForZone(perCarrier, honorFilter) {{
            var best = null;
            Object.keys(perCarrier).forEach(function(carrier) {{
              if (honorFilter && disabledCarriers[carrier]) return;
              var days = perCarrier[carrier].transit_days;
              if (days == null) return;
              if (best === null || days < best) best = days;
            }});
            if (best === null && honorFilter) return bestTransitForZone(perCarrier, false);
            return best;
          }}

          function zoneInfo(zone, productIdx) {{
            var product = DATA.products[productIdx];
            if (!product || !zone) return null;
            var perCarrier = product.zoneRates[String(zone)];
            if (!perCarrier) return null;
            return bestForZone(perCarrier, true);
          }}

          function zoneTransit(zone, productIdx) {{
            var product = DATA.products[productIdx];
            if (!product || !zone) return null;
            var perCarrier = product.zoneRates[String(zone)];
            if (!perCarrier) return null;
            return bestTransitForZone(perCarrier, true);
          }}

          function rateRange(productIdx) {{
            var product = DATA.products[productIdx];
            if (!product) return null;
            var rates = [];
            Object.keys(product.zoneRates).forEach(function(zone) {{
              var info = bestForZone(product.zoneRates[zone], true);
              if (info) rates.push(info.rate);
            }});
            if (!rates.length) return null;
            return [Math.min.apply(null, rates), Math.max.apply(null, rates)];
          }}

          function transitRange(productIdx) {{
            var product = DATA.products[productIdx];
            if (!product) return null;
            var days = [];
            Object.keys(product.zoneRates).forEach(function(zone) {{
              var d = bestTransitForZone(product.zoneRates[zone], true);
              if (d != null) days.push(d);
            }});
            if (!days.length) return null;
            return [Math.min.apply(null, days), Math.max.apply(null, days)];
          }}

          // One handler, both effects: the filter chips (rendered inside the
          // carrier-rates section) repaint the map AND show/hide the matching
          // rate-table columns. Delegated on document so chips swapped in by
          // the requote flow keep working without re-binding.
          function applyCarrierFilter() {{
            var anyOff = Object.keys(disabledCarriers).some(function(k) {{ return disabledCarriers[k]; }});
            document.querySelectorAll('.cf-chip').forEach(function(chip) {{
              var off = !!disabledCarriers[chip.getAttribute('data-carrier')];
              chip.classList.toggle('cf-off', off);
              chip.setAttribute('aria-pressed', off ? 'false' : 'true');
            }});
            document.querySelectorAll('table.data-table').forEach(function(table) {{
              table.classList.toggle('js-filtered', anyOff);
              table.querySelectorAll('th[data-carrier], td[data-carrier]').forEach(function(cell) {{
                cell.classList.toggle('cf-hidden', !!disabledCarriers[cell.getAttribute('data-carrier')]);
              }});
              table.querySelectorAll('tbody tr').forEach(function(row) {{
                var best = null;
                row.querySelectorAll('td[data-carrier]').forEach(function(cell) {{
                  cell.classList.remove('js-best');
                  if (disabledCarriers[cell.getAttribute('data-carrier')]) return;
                  var rate = parseFloat(cell.getAttribute('data-rate'));
                  if (isNaN(rate)) return;
                  if (!best || rate < parseFloat(best.getAttribute('data-rate'))) best = cell;
                }});
                if (best && anyOff) best.classList.add('js-best');
              }});
            }});
            paint();
          }}

          document.addEventListener('click', function(evt) {{
            var chip = evt.target.closest('.cf-chip');
            if (!chip) return;
            var carrier = chip.getAttribute('data-carrier');
            if (!carrier) return;
            if (!disabledCarriers[carrier]) {{
              var enabledCount = 0;
              document.querySelectorAll('#carrier-filter .cf-chip').forEach(function(c) {{
                if (!disabledCarriers[c.getAttribute('data-carrier')]) enabledCount += 1;
              }});
              if (enabledCount <= 1) return; // at least one carrier stays on
              disabledCarriers[carrier] = true;
            }} else {{
              delete disabledCarriers[carrier];
            }}
            applyCarrierFilter();
          }});

          function colorFor(rate, range) {{
            if (range[1] <= range[0]) return RAMP[3];
            var t = (rate - range[0]) / (range[1] - range[0]);
            return RAMP[Math.min(RAMP.length - 1, Math.floor(t * RAMP.length))];
          }}

          function paint() {{
            var legend = document.getElementById('rm-legend');
            var chips = RAMP.map(function(c) {{ return '<span class="rm-chip" style="background:' + c + '"></span>'; }}).join('');
            var suffix = (DATA.source === 'mock' ? ' · sample rates' : '')
              + (edited ? ' · live estimate for edited specs' : '');
            if (mode === 'transit') {{
              var trange = transitRange(selected);
              svg.querySelectorAll('.rm-cell').forEach(function(el) {{
                var zone = parseInt(el.getAttribute('data-z'), 10);
                var days = trange ? zoneTransit(zone, selected) : null;
                el.style.fill = days != null ? colorFor(days, trange) : '#eee9dc';
              }});
              if (trange) {{
                legend.innerHTML = '<span>' + trange[0] + (trange[0] === trange[1] ? '' : '–' + trange[1]) + ' days</span>' + chips
                  + '<span style="margin-left:10px">best transit time by ZIP area' + suffix + '</span>';
              }} else {{
                legend.innerHTML = '';
              }}
              return;
            }}
            var range = rateRange(selected);
            svg.querySelectorAll('.rm-cell').forEach(function(el) {{
              var zone = parseInt(el.getAttribute('data-z'), 10);
              var info = range ? zoneInfo(zone, selected) : null;
              el.style.fill = info ? colorFor(info.rate, range) : '#eee9dc';
            }});
            if (range) {{
              legend.innerHTML = '<span>' + fmt(range[0]) + '</span>' + chips + '<span>' + fmt(range[1]) + '</span>'
                + '<span style="margin-left:10px">estimated per-parcel rate by ZIP area' + suffix + '</span>';
            }} else {{
              legend.innerHTML = '';
            }}
          }}

          var modeWrap = document.getElementById('rm-mode');
          if (modeWrap) modeWrap.addEventListener('click', function(evt) {{
            var btn = evt.target.closest('button[data-mode]');
            if (!btn) return;
            mode = btn.getAttribute('data-mode') === 'transit' ? 'transit' : 'cost';
            modeWrap.querySelectorAll('button').forEach(function(b) {{
              var on = b === btn;
              b.classList.toggle('active', on);
              b.setAttribute('aria-pressed', on ? 'true' : 'false');
            }});
            paint();
          }});

          function setBusy(busy) {{
            overlay.hidden = !busy;
            document.querySelectorAll('#rm-dims input, #rm-dims button, #rm-products button').forEach(function(el) {{
              el.disabled = busy;
            }});
          }}

          function renderControls() {{
            var tabs = document.getElementById('rm-products');
            tabs.innerHTML = DATA.products.map(function(p, i) {{
              return '<button type="button" data-i="' + i + '"' + (i === selected ? ' class="active"' : '') + '>'
                + p.name.replace(/[<>&]/g, '') + '</button>';
            }}).join('');
            var p = DATA.products[selected];
            var dims = document.getElementById('rm-dims');
            if (!p) {{ dims.innerHTML = ''; return; }}
            function field(label, key, step) {{
              var v = p[key] == null ? '' : p[key];
              return '<label>' + label + '<input type="number" min="0.1" step="' + step + '" data-key="' + key + '" value="' + v + '"></label>';
            }}
            dims.innerHTML = field('L (in)', 'length_in', '0.5') + field('W (in)', 'width_in', '0.5')
              + field('H (in)', 'height_in', '0.5') + field('Weight (lb)', 'weight_lb', '0.1')
              + '<button type="button" class="rm-request" id="rm-request">Request rates</button>'
              + (p.estimated ? '<span class="rm-est">estimated</span>' : '')
              + '<button type="button" class="rm-reset" id="rm-reset">reset to quoted specs</button>';
            dims.querySelectorAll('input').forEach(function(input) {{
              input.addEventListener('input', function(evt) {{
                var key = evt.target.getAttribute('data-key');
                var v = parseFloat(evt.target.value);
                p[key] = isNaN(v) || v <= 0 ? null : v;
                statusEl.textContent = 'Press "Request rates" to re-quote with the new specs.';
              }});
            }});
            document.getElementById('rm-request').addEventListener('click', function() {{ requote(false); }});
            var reset = document.getElementById('rm-reset');
            if (reset) reset.addEventListener('click', function() {{
              DATA.products = JSON.parse(JSON.stringify(ORIGINALS));
              renderControls();
              requote(true);
            }});
          }}

          function swapFragments(fragments) {{
            if (!fragments) return;
            Object.keys(fragments).forEach(function(key) {{
              var section = document.querySelector('[data-key="' + key + '"]');
              if (!section) return;
              var html = fragments[key];
              if (html) {{ section.outerHTML = html; }}
              else {{ section.parentNode.removeChild(section); }}
            }});
          }}

          function requote(isReset) {{
            setBusy(true);
            statusEl.textContent = 'Requesting live rates — this takes ~30 seconds…';
            var body = {{
              origin_zip: DATA.origin,
              products: DATA.products.map(function(p) {{
                return {{ name: p.name, length_in: p.length_in, width_in: p.width_in,
                         height_in: p.height_in, weight_lb: p.weight_lb }};
              }})
            }};
            fetch(requoteUrl, {{
              method: 'POST',
              headers: {{ 'Content-Type': 'application/json' }},
              body: JSON.stringify(body)
            }}).then(function(r) {{ if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); }})
              .then(function(data) {{
                (data.products || []).forEach(function(rp) {{
                  var local = DATA.products.find(function(p) {{ return p.name === rp.name; }});
                  if (local) local.zoneRates = rp.zoneRates || {{}};
                }});
                edited = !isReset;
                swapFragments(data.fragments);
                statusEl.textContent = isReset
                  ? 'Restored the quoted specs — rates updated and saved.'
                  : 'Rates updated and saved to this report.';
                // Swapped fragments arrive unfiltered — re-apply the viewer's
                // carrier filter to the fresh chips/columns (also repaints).
                applyCarrierFilter();
              }})
              .catch(function() {{
                statusEl.textContent = 'Could not re-quote right now — showing the previous rates.';
              }})
              .then(function() {{ setBusy(false); }});
          }}

          document.getElementById('rm-products').addEventListener('click', function(evt) {{
            var btn = evt.target.closest('button[data-i]');
            if (!btn) return;
            selected = parseInt(btn.getAttribute('data-i'), 10) || 0;
            renderControls(); paint();
          }});

          svg.addEventListener('mousemove', function(evt) {{
            var el = evt.target.closest('.rm-cell');
            if (!el) {{ tooltip.hidden = true; return; }}
            var zone = parseInt(el.getAttribute('data-z'), 10);
            var miles = el.getAttribute('data-mi');
            var name = el.getAttribute('data-n') || 'ZIP area';
            var prefix = el.getAttribute('data-p');
            var info = zoneInfo(zone, selected);
            var inner = '<div class="rm-tt-state">' + name + '</div>'
              + 'ZIP ' + prefix + 'xx · ' + Number(miles).toLocaleString() + ' mi'
              + (zone ? ' · zone ' + zone : '') + '<br>';
            if (info) {{
              inner += '<span class="rm-tt-rate">' + fmt(info.rate) + '</span> per parcel<br>'
                + chipHtml(info.carrier) + ' ' + String(info.service || '').replace(/[<>&]/g, '')
                + (info.transit_days ? ' · ' + info.transit_days + ' day' + (info.transit_days > 1 ? 's' : '') : '');
            }} else {{
              inner += 'No estimate available';
            }}
            tooltip.innerHTML = inner;
            tooltip.hidden = false;
            var holder = svg.parentElement.getBoundingClientRect();
            var x = evt.clientX - holder.left + 14;
            var y = evt.clientY - holder.top + 14;
            if (x + 260 > holder.width) x -= 280;
            tooltip.style.left = x + 'px';
            tooltip.style.top = y + 'px';
          }});
          svg.addEventListener('mouseleave', function() {{ tooltip.hidden = true; }});

          renderControls();
          paint();
        }})();
      </script>
    """
