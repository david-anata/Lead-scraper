"""Interactive US rate map for the rate sheet.

Real state outlines (see us_states_svg.py, CC0), colored by what it costs to
ship the selected product to each state. Hover shows the estimated rate,
carrier, and transit; the viewer can edit each product's dims/weight right on
the proposal and the map re-quotes live via the token-gated /requote endpoint.
Viewer edits are ephemeral — they never change the published sheet.
"""

from __future__ import annotations

import html as _html
import json

from sales_support_agent.services.fulfillment_deck.schema import RateMatrix, clean_zip
from sales_support_agent.services.fulfillment_deck.us_states_svg import STATE_PATHS, VIEWBOX

# state -> representative metro ZIP used to bucket it into a shipping zone.
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


def state_zone_map(origin_zip: str) -> dict[str, int]:
    """zone per state from the origin (states with unknown zones omitted)."""
    from sales_support_agent.services.fulfillment_deck.zones import zone_for

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
    """The JSON the in-page JS needs: per-product best rate per zone."""
    products = []
    for product_rates in matrix.products:
        product = product_rates.product
        zone_rates: dict[str, dict] = {}
        for zone in product_rates.zones:
            best = min(zone.quotes, key=lambda q: q.rate_usd, default=None)
            if best is None:
                continue
            zone_rates[str(zone.zone)] = {
                "rate": best.rate_usd,
                "carrier": best.carrier,
                "service": best.service,
                "transit_days": best.transit_days,
            }
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
    """The full 'coverage + rates' interactive section body."""
    payload = map_payload(matrix)
    payload["requoteUrl"] = requote_path
    state_paths_svg = "".join(
        f'<path class="rm-state" data-state="{abbr}" d="{d}"></path>'
        for abbr, (_name, d) in STATE_PATHS.items()
    )
    state_names = {abbr: name for abbr, (name, _d) in STATE_PATHS.items()}

    return f"""
      <div class="rm-wrap">
        <div class="rm-controls" id="rm-controls">
          <div class="rm-products" id="rm-products"></div>
          <div class="rm-dims" id="rm-dims"></div>
          <div class="rm-status" id="rm-status"></div>
        </div>
        <div class="rm-map-holder">
          <svg id="rm-svg" viewBox="{VIEWBOX}" role="img"
               aria-label="Estimated shipping rates by state from {_html.escape(origin_label)}">
            <g stroke="#fffdf9" stroke-width="1" stroke-linejoin="round">{state_paths_svg}</g>
          </svg>
          <div class="rm-tooltip" id="rm-tooltip" hidden></div>
          <div class="rm-legend" id="rm-legend"></div>
        </div>
      </div>
      <style>
        .rm-wrap {{ display: flex; flex-direction: column; gap: 14px; }}
        .rm-map-holder {{ position: relative; }}
        #rm-svg {{ width: 100%; height: auto; display: block; }}
        .rm-state {{ fill: #eee9dc; cursor: pointer; transition: opacity 120ms ease; }}
        .rm-state:hover {{ opacity: 0.78; }}
        .rm-state.rm-origin {{ stroke: #bfa889; stroke-width: 2.5; }}
        .rm-tooltip {{ position: absolute; pointer-events: none; background: var(--anata-ink, #1d2d44);
          color: #fffdf9; border-radius: 10px; padding: 9px 13px; font-size: 12.5px; line-height: 1.45;
          box-shadow: 0 8px 22px rgba(29,45,68,0.35); max-width: 240px; z-index: 5; }}
        .rm-tooltip .rm-tt-state {{ font-weight: 700; font-size: 13px; }}
        .rm-tooltip .rm-tt-rate {{ font-size: 17px; font-weight: 700; color: #85bbda; }}
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
        .rm-dims .rm-reset {{ font-size: 11.5px; align-self: center; cursor: pointer;
          color: var(--anata-sky-deep, #4f84c4); text-decoration: underline; background: none;
          border: none; padding: 0; font-family: inherit; }}
        .rm-status {{ font-size: 12px; color: var(--anata-muted, #6b7688); min-height: 16px; }}
        .rm-legend {{ display: flex; align-items: center; gap: 4px; margin-top: 8px;
          font-size: 11px; color: var(--anata-muted, #6b7688); flex-wrap: wrap; }}
        .rm-legend .rm-chip {{ width: 34px; height: 12px; border-radius: 3px; display: inline-block; }}
        @media print {{
          .rm-controls {{ display: none !important; }}
          .rm-state {{ transition: none; }}
        }}
      </style>
      <script>
        (function() {{
          var DATA = {json.dumps(payload)};
          var STATE_NAMES = {json.dumps(state_names)};
          var RAMP = {json.dumps(RATE_RAMP)};
          var ORIGINALS = JSON.parse(JSON.stringify(DATA.products));
          var selected = 0;
          var requoteUrl = DATA.requoteUrl || (window.location.pathname.replace(/\\/$/, '') + '/requote');
          var svg = document.getElementById('rm-svg');
          var tooltip = document.getElementById('rm-tooltip');
          var statusEl = document.getElementById('rm-status');
          var debounceTimer = null;
          var edited = false;

          function fmt(rate) {{ return '$' + Number(rate).toFixed(2); }}

          function rateFor(stateAbbr, productIdx) {{
            var zone = DATA.stateZones[stateAbbr];
            if (!zone) return null;
            var product = DATA.products[productIdx];
            if (!product) return null;
            return product.zoneRates[String(zone)] || null;
          }}

          function rateRange(productIdx) {{
            var product = DATA.products[productIdx];
            if (!product) return null;
            var rates = Object.values(product.zoneRates).map(function(z) {{ return z.rate; }});
            if (!rates.length) return null;
            return [Math.min.apply(null, rates), Math.max.apply(null, rates)];
          }}

          function colorFor(rate, range) {{
            if (range[1] <= range[0]) return RAMP[3];
            var t = (rate - range[0]) / (range[1] - range[0]);
            return RAMP[Math.min(RAMP.length - 1, Math.floor(t * RAMP.length))];
          }}

          function paint() {{
            var range = rateRange(selected);
            svg.querySelectorAll('.rm-state').forEach(function(el) {{
              var abbr = el.getAttribute('data-state');
              var info = range ? rateFor(abbr, selected) : null;
              el.style.fill = info ? colorFor(info.rate, range) : '#eee9dc';
              el.classList.toggle('rm-origin', abbr === DATA.originState);
            }});
            var legend = document.getElementById('rm-legend');
            if (range) {{
              var chips = RAMP.map(function(c) {{ return '<span class="rm-chip" style="background:' + c + '"></span>'; }}).join('');
              legend.innerHTML = '<span>' + fmt(range[0]) + '</span>' + chips + '<span>' + fmt(range[1]) + '</span>'
                + '<span style="margin-left:10px">estimated per-parcel rate' + (DATA.source === 'mock' ? ' · sample rates' : '') + (edited ? ' · live estimate for edited specs' : '') + '</span>';
            }} else {{
              legend.innerHTML = '';
            }}
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
              + (p.estimated ? '<span class="rm-est">estimated</span>' : '')
              + '<button type="button" class="rm-reset" id="rm-reset">reset to quoted specs</button>';
            dims.querySelectorAll('input').forEach(function(input) {{
              input.addEventListener('input', onDimEdit);
            }});
            var reset = document.getElementById('rm-reset');
            if (reset) reset.addEventListener('click', function() {{
              DATA.products = JSON.parse(JSON.stringify(ORIGINALS));
              edited = false;
              statusEl.textContent = '';
              renderControls(); paint();
            }});
          }}

          function onDimEdit(evt) {{
            var p = DATA.products[selected];
            var key = evt.target.getAttribute('data-key');
            var v = parseFloat(evt.target.value);
            p[key] = isNaN(v) || v <= 0 ? null : v;
            clearTimeout(debounceTimer);
            statusEl.textContent = 'Re-quoting…';
            debounceTimer = setTimeout(requote, 600);
          }}

          function requote() {{
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
                edited = true;
                statusEl.textContent = 'Live estimate updated for your edited specs — the quoted tables above are unchanged.';
                paint();
              }})
              .catch(function() {{
                statusEl.textContent = 'Could not re-quote right now — showing the original rates.';
              }});
          }}

          document.getElementById('rm-products').addEventListener('click', function(evt) {{
            var btn = evt.target.closest('button[data-i]');
            if (!btn) return;
            selected = parseInt(btn.getAttribute('data-i'), 10) || 0;
            renderControls(); paint();
          }});

          svg.addEventListener('mousemove', function(evt) {{
            var el = evt.target.closest('.rm-state');
            if (!el) {{ tooltip.hidden = true; return; }}
            var abbr = el.getAttribute('data-state');
            var info = rateFor(abbr, selected);
            var zone = DATA.stateZones[abbr];
            var name = STATE_NAMES[abbr] || abbr;
            var inner = '<div class="rm-tt-state">' + name + (zone ? ' · zone ' + zone : '') + '</div>';
            if (abbr === DATA.originState) {{
              inner += 'Ships from here — Anata HQ';
            }} else if (info) {{
              inner += '<span class="rm-tt-rate">' + fmt(info.rate) + '</span> per parcel<br>'
                + info.carrier + ' ' + info.service
                + (info.transit_days ? ' · ' + info.transit_days + ' day' + (info.transit_days > 1 ? 's' : '') : '');
            }} else {{
              inner += 'No estimate available';
            }}
            tooltip.innerHTML = inner;
            tooltip.hidden = false;
            var holder = svg.parentElement.getBoundingClientRect();
            var x = evt.clientX - holder.left + 14;
            var y = evt.clientY - holder.top + 14;
            if (x + 250 > holder.width) x -= 270;
            tooltip.style.left = x + 'px';
            tooltip.style.top = y + 'px';
          }});
          svg.addEventListener('mouseleave', function() {{ tooltip.hidden = true; }});

          renderControls();
          paint();
        }})();
      </script>
    """
