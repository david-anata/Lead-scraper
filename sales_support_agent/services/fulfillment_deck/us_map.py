"""Interactive ZIP-level rate map for the rate sheet.

Per David: not state averages — distance and ZIP code. Every assigned US
3-digit ZIP prefix (~900) renders as its own cell, positioned by an Albers
equal-area projection of its centroid and colored by what it costs to ship
the selected product there (zone-band rate from the live quotes). Concentric
mileage rings radiate from the ship-from origin. Hover any cell for the
area name, ZIP prefix, true straight-line miles, zone, and rate. The viewer
can edit dims/weight and press "Request rates" — the whole sheet re-quotes
live and saves (same flow as before; only the visualization changed).

Alaska / Hawaii / Puerto Rico & USVI render as compact inset grids — their
prefixes are still real, individually hoverable cells.
"""

from __future__ import annotations

import html as _html
import json
import math

from sales_support_agent.services.fulfillment_deck.schema import RateMatrix, clean_zip
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


# Canvas layout.
_W, _H = 940, 580
_PAD = 22
_CELL = 9.0  # cell size in px


def _build_cells(origin_zip: str) -> tuple[list[dict], dict, float]:
    """All zip3 cells with projected px coords, miles, zone. Returns
    (cells, origin_px, px_per_mile)."""
    lower48 = {
        p: c for p, c in ZIP3_CENTROIDS.items() if p not in _INSET_PREFIXES
    }
    projected = {p: _albers(lat, lon) for p, (lat, lon) in lower48.items()}
    xs = [xy[0] for xy in projected.values()]
    ys = [xy[1] for xy in projected.values()]
    min_x, max_x, min_y, max_y = min(xs), max(xs), min(ys), max(ys)
    span = max(max_x - min_x, 1e-9)
    scale = (_W - 2 * _PAD) / span
    # Vertical fit check (the lower 48 are wider than tall, so x drives scale).
    used_h = (max_y - min_y) * scale

    def to_px(x: float, y: float) -> tuple[float, float]:
        return (
            _PAD + (x - min_x) * scale,
            _PAD + (max_y - y) * scale + max(0.0, (_H - 130 - 2 * _PAD - used_h) / 2),
        )

    origin_centroid = ZIP3_CENTROIDS.get((clean_zip(origin_zip) or "841")[:3])
    if origin_centroid is None:
        origin_centroid = ZIP3_CENTROIDS["841"]
    o_lat, o_lon = origin_centroid

    cells: list[dict] = []
    for prefix, (lat, lon) in lower48.items():
        x, y = to_px(*projected[prefix])
        cells.append({
            "p": prefix,
            "x": round(x, 1),
            "y": round(y, 1),
            "z": zone_for(origin_zip, prefix + "01"),
            "mi": int(round(haversine_miles(o_lat, o_lon, lat, lon))),
            "n": ZIP3_NAMES.get(prefix, ""),
        })

    # Insets: AK, HI, PR/VI as compact rows bottom-left.
    inset_groups = (("AK", _AK_PREFIXES), ("HI", _HI_PREFIXES), ("PR/VI", _CARIB_PREFIXES))
    iy = _H - 96
    for label, prefixes in inset_groups:
        ix = _PAD + 34
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

    # Origin position + px/mile (project a point ~200 miles east of origin).
    ox, oy = to_px(*_albers(o_lat, o_lon))
    east = _albers(o_lat, o_lon + 200.0 / (69.172 * math.cos(math.radians(o_lat))))
    ex, ey = to_px(*east)
    px_per_mile = math.dist((ox, oy), (ex, ey)) / 200.0
    return cells, {"x": round(ox, 1), "y": round(oy, 1)}, px_per_mile


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


def render_interactive_rate_map(matrix: RateMatrix, origin_label: str,
                                requote_path: str = "") -> str:
    """The ZIP-level 'distance from our dock' interactive section body."""
    payload = map_payload(matrix)
    payload["requoteUrl"] = requote_path
    payload["carrierColors"] = {k: list(v) for k, v in CARRIER_BRAND_COLORS.items()}

    cells, origin_px, px_per_mile = _build_cells(matrix.origin_zip)

    cell_rects = "".join(
        f'<rect class="rm-cell" x="{c["x"] - _CELL / 2}" y="{c["y"] - _CELL / 2}" '
        f'width="{_CELL}" height="{_CELL}" rx="2.4" '
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
        f'<text class="rm-inset-label" x="{_PAD}" y="{_H - 92}">AK</text>'
        f'<text class="rm-inset-label" x="{_PAD}" y="{_H - 92 + (_CELL + 7)}">HI</text>'
        f'<text class="rm-inset-label" x="{_PAD}" y="{_H - 92 + 2 * (_CELL + 7)}">PR</text>'
    )
    origin_marker = (
        f'<circle cx="{origin_px["x"]}" cy="{origin_px["y"]}" r="5.5" fill="#bfa889" '
        f'stroke="#fffdf9" stroke-width="2"></circle>'
    )

    return f"""
      <div class="rm-wrap">
        <div class="rm-controls" id="rm-controls">
          <div class="rm-products" id="rm-products"></div>
          <div class="rm-dims" id="rm-dims"></div>
          <div class="rm-status" id="rm-status"></div>
        </div>
        <div class="rm-map-holder">
          <svg id="rm-svg" viewBox="0 0 {_W} {_H}" role="img"
               aria-label="Estimated shipping rates by ZIP prefix and distance from {_html.escape(origin_label)}">
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
        .rm-cell {{ fill: #eee9dc; cursor: pointer; }}
        .rm-cell:hover {{ stroke: #1d2d44; stroke-width: 1.5; }}
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

          function zoneInfo(zone, productIdx) {{
            var product = DATA.products[productIdx];
            if (!product || !zone) return null;
            var perCarrier = product.zoneRates[String(zone)];
            if (!perCarrier) return null;
            return bestForZone(perCarrier, true);
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
            var range = rateRange(selected);
            svg.querySelectorAll('.rm-cell').forEach(function(el) {{
              var zone = parseInt(el.getAttribute('data-z'), 10);
              var info = range ? zoneInfo(zone, selected) : null;
              el.style.fill = info ? colorFor(info.rate, range) : '#eee9dc';
            }});
            var legend = document.getElementById('rm-legend');
            if (range) {{
              var chips = RAMP.map(function(c) {{ return '<span class="rm-chip" style="background:' + c + '"></span>'; }}).join('');
              legend.innerHTML = '<span>' + fmt(range[0]) + '</span>' + chips + '<span>' + fmt(range[1]) + '</span>'
                + '<span style="margin-left:10px">estimated per-parcel rate by ZIP area'
                + (DATA.source === 'mock' ? ' · sample rates' : '') + (edited ? ' · live estimate for edited specs' : '') + '</span>';
            }} else {{
              legend.innerHTML = '';
            }}
          }}

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
