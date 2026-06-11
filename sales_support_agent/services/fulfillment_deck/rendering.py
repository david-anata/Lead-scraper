"""Hosted Rate Sheet HTML — same style guide as the sales deck.

Reuses the brand package stylesheet (`deck.css`) and the deck's shell
vocabulary (.app / .rail / .slide / .eyebrow / .slide-title), so the rate
sheet looks like a sibling of the strategy deck David's prospects already
compliment. Per-product rate tabs reuse the deck's .off-tabs classes; a small
extra print rule expands every tab pane so the printed PDF shows all products.
"""

from __future__ import annotations

import html
from typing import Optional

from sales_support_agent.config import Settings
from sales_support_agent.services.deck.brand_assets import (
    load_brand_asset,
    load_brand_favicon_link,
    load_brand_stylesheet,
)
from sales_support_agent.services.fulfillment_deck.schema import (
    ANATA_HQ_ADDRESS,
    RATE_SOURCE_MOCK,
    ProductRates,
    ProspectProfile,
    RateMatrix,
    SectionFlags,
)
from sales_support_agent.services.fulfillment_deck.us_map import render_zone_tile_map

_SAMPLE_BADGE = (
    '<span style="display:inline-block;background:#fff4d9;border:1px solid #d2a94b;'
    'color:#7a5b14;border-radius:999px;padding:3px 12px;font-size:11px;font-weight:700;'
    'letter-spacing:0.04em;text-transform:uppercase;">Sample rates — illustrative</span>'
)


def _fmt_rate(value: float) -> str:
    return f"${value:,.2f}"


def _fmt_dims(product) -> str:
    if not product.has_full_package_spec:
        return "—"
    return (
        f"{product.length_in:g} × {product.width_in:g} × {product.height_in:g} in · "
        f"{product.weight_lb:g} lb"
    )


def _carrier_order(product_rates: ProductRates) -> list[tuple[str, str]]:
    """Stable (carrier, service) column order across all zones of a product."""
    seen: list[tuple[str, str]] = []
    for zone in product_rates.zones:
        for quote in zone.quotes:
            key = (quote.carrier, quote.service)
            if key not in seen:
                seen.append(key)
    return seen


def _render_rate_table(product_rates: ProductRates) -> str:
    carriers = _carrier_order(product_rates)
    if not carriers or not product_rates.zones:
        return '<p class="muted small">No rates available for this product.</p>'
    head_cells = "".join(
        f"<th>{html.escape(carrier)}<br><span style='font-weight:500;color:var(--anata-muted);"
        f"font-size:11px'>{html.escape(service)}</span></th>"
        for carrier, service in carriers
    )
    body_rows = []
    for zone in product_rates.zones:
        by_key = {(q.carrier, q.service): q for q in zone.quotes}
        cheapest: Optional[float] = min((q.rate_usd for q in zone.quotes), default=None)
        cells = []
        for key in carriers:
            quote = by_key.get(key)
            if quote is None:
                cells.append("<td>—</td>")
                continue
            is_best = cheapest is not None and abs(quote.rate_usd - cheapest) < 0.005
            style = "font-weight:700;color:var(--anata-sky-deep);" if is_best else ""
            transit = (
                f"<br><span style='font-size:11px;color:var(--anata-muted);font-weight:500'>"
                f"{quote.transit_days} day{'s' if quote.transit_days != 1 else ''}</span>"
                if quote.transit_days
                else ""
            )
            cells.append(f"<td style='{style}'>{_fmt_rate(quote.rate_usd)}{transit}</td>")
        body_rows.append(
            f"<tr><td><strong>Zone {zone.zone}</strong><br>"
            f"<span style='font-size:11px;color:var(--anata-muted)'>{html.escape(zone.dest_label)}"
            f" · {html.escape(zone.dest_zip)}</span></td>{''.join(cells)}</tr>"
        )
    return (
        "<table class='data-table' style='width:100%;border-collapse:collapse'>"
        f"<thead><tr><th>Destination</th>{head_cells}</tr></thead>"
        f"<tbody>{''.join(body_rows)}</tbody></table>"
    )


def _render_product_tabs(matrix: RateMatrix) -> str:
    """One labeled tab per product spec (deck's .off-tabs pattern)."""
    if not matrix.products:
        return ""
    tabs = []
    panes = []
    for index, product_rates in enumerate(matrix.products):
        product = product_rates.product
        key = f"prod-{index}"
        label = html.escape(product.name or f"Product {index + 1}")
        active_attr = ' class="active"' if index == 0 else ""
        hidden_attr = "" if index == 0 else " hidden"
        tabs.append(f'<button{active_attr} type="button" data-off="{key}">{label}</button>')
        units = (
            f" · ~{product.monthly_units:,} units/mo" if product.monthly_units else ""
        )
        panes.append(
            f'<div class="off-pane rate-pane" data-pane="{key}"{hidden_attr}>'
            f'<h3 style="font-size:18px;font-weight:700;margin:0 0 4px;letter-spacing:-0.015em">'
            f"{label}</h3>"
            f'<p class="muted small" style="margin:0 0 16px">{html.escape(_fmt_dims(product))}{units}</p>'
            f"{_render_rate_table(product_rates)}"
            f"</div>"
        )
    multi = len(matrix.products) > 1
    tabs_html = f'<div class="off-tabs" id="off-tabs">{"".join(tabs)}</div>' if multi else ""
    return tabs_html + "".join(panes)


def _render_cover(profile: ProspectProfile, matrix: RateMatrix, origin_label: str,
                  generated_on: str) -> str:
    facts = []
    if profile.monthly_order_volume:
        facts.append(("Monthly orders", f"{profile.monthly_order_volume:,}"))
    if matrix.products:
        facts.append(("Products quoted", str(len(matrix.products))))
    facts.append(("Ship-from", origin_label))
    if profile.current_carrier:
        facts.append(("Current carrier", profile.current_carrier))
    fact_tiles = "".join(
        f"<div class='off-block'><h4>{html.escape(label)}</h4><p>{html.escape(value)}</p></div>"
        for label, value in facts[:4]
    )
    return f"""
    <section class="slide" id="sec-01" data-screen-label="01 Overview">
      <header class="slide-head">
        <div class="heading-stack">
          <p class="eyebrow">Fulfillment rate sheet · {html.escape(generated_on)}</p>
          <h2 class="slide-title">{html.escape(profile.display_name)} × Anata</h2>
        </div>
        <p class="caption">Carrier rates, transit windows, and shipping zones for {html.escape(profile.display_name)}, shipped from Anata's fulfillment center. Built from your product specs — printable and shareable.</p>
      </header>
      <div class="off-grid">{fact_tiles}</div>
    </section>"""


def _render_zone_map_section(matrix: RateMatrix, origin_label: str) -> str:
    return f"""
    <section class="slide" id="sec-02" data-screen-label="02 Zone map">
      <header class="slide-head">
        <div class="heading-stack">
          <p class="eyebrow">Coverage</p>
          <h2 class="slide-title">Shipping zones from {html.escape(origin_label)}</h2>
        </div>
        <p class="caption">Every state's USPS-style zone from our dock. Lower zones mean cheaper, faster ground service — and our Utah origin puts the entire West in zones 1–5 with 2–4 day national ground coverage.</p>
      </header>
      <div style="display:flex;justify-content:center">{render_zone_tile_map(matrix.origin_zip)}</div>
    </section>"""


def _render_rates_section(matrix: RateMatrix) -> str:
    badge = _SAMPLE_BADGE if matrix.source == RATE_SOURCE_MOCK else ""
    return f"""
    <section class="slide" id="sec-03" data-screen-label="03 Carrier rates">
      <header class="slide-head">
        <div class="heading-stack">
          <p class="eyebrow">Carrier costs</p>
          <h2 class="slide-title">Your rates, by product and zone</h2>
        </div>
        <p class="caption">Rates per parcel for each of your product configurations, quoted to a representative city in every zone. Best rate per zone highlighted. {badge}</p>
      </header>
      {_render_product_tabs(matrix)}
    </section>"""


def _render_volume_section(profile: ProspectProfile, matrix: RateMatrix) -> str:
    volume = profile.monthly_order_volume or sum(
        p.product.monthly_units or 0 for p in matrix.products
    )
    if not volume:
        return ""
    # Blended average of the cheapest rate per zone across products — a
    # directional planning number, clearly labeled as such.
    cheapest_rates: list[float] = []
    for product_rates in matrix.products:
        for zone in product_rates.zones:
            best = min((q.rate_usd for q in zone.quotes), default=None)
            if best is not None:
                cheapest_rates.append(best)
    avg = sum(cheapest_rates) / len(cheapest_rates) if cheapest_rates else 0.0
    monthly = avg * volume
    tiles = "".join(
        f"<div class='off-block'><h4>{html.escape(label)}</h4><p>{html.escape(value)}</p></div>"
        for label, value in (
            ("Monthly orders", f"{volume:,}"),
            ("Blended best-rate average", f"{_fmt_rate(avg)} / parcel"),
            ("Directional monthly shipping", _fmt_rate(monthly)),
            ("Note", "Flat blended average across zones — actual mix depends on your destination distribution."),
        )
    )
    return f"""
    <section class="slide" id="sec-04" data-screen-label="04 Volume economics">
      <header class="slide-head">
        <div class="heading-stack">
          <p class="eyebrow">Volume economics</p>
          <h2 class="slide-title">What this looks like at your volume</h2>
        </div>
      </header>
      <div class="off-grid">{tiles}</div>
    </section>"""


def _render_context_section(profile: ProspectProfile) -> str:
    """Cost comparison + destinations notes, when we know them."""
    blocks = []
    if profile.current_costs_note:
        blocks.append(
            "<div class='off-block'><h4>Your current shipping costs</h4>"
            f"<p>{html.escape(profile.current_costs_note)}</p></div>"
        )
    if profile.current_carrier:
        blocks.append(
            "<div class='off-block'><h4>Current setup</h4>"
            f"<p>{html.escape(profile.current_carrier)}</p></div>"
        )
    if profile.destinations_note:
        blocks.append(
            "<div class='off-block'><h4>Where your orders go</h4>"
            f"<p>{html.escape(profile.destinations_note)}</p></div>"
        )
    if not blocks:
        return ""
    return f"""
    <section class="slide" id="sec-05" data-screen-label="05 Your context">
      <header class="slide-head">
        <div class="heading-stack">
          <p class="eyebrow">Your context</p>
          <h2 class="slide-title">What we're comparing against</h2>
        </div>
      </header>
      <div class="off-grid">{''.join(blocks)}</div>
    </section>"""


def _render_about_section() -> str:
    tiles = "".join(
        f"<div class='off-block'><h4>{html.escape(title)}</h4><p>{html.escape(body)}</p></div>"
        for title, body in (
            ("Same-day turnaround", "Orders received by 2pm MT ship the same business day."),
            ("Strategic origin", f"{ANATA_HQ_ADDRESS} — 2-4 day ground coverage to the entire continental US."),
            ("Rate shopping built in", "Every order is rate-shopped across carriers at label time, so you always ship at the price on this sheet or better."),
            ("People who answer", "A named account manager, not a ticket queue. Integrations with Shopify, Amazon, and EDI retail."),
        )
    )
    return f"""
    <section class="slide" id="sec-06" data-screen-label="06 Why Anata">
      <header class="slide-head">
        <div class="heading-stack">
          <p class="eyebrow">Why Anata</p>
          <h2 class="slide-title">Fulfillment that feels in-house</h2>
        </div>
      </header>
      <div class="off-grid">{tiles}</div>
      <div class="next-steps" style="margin-top:18px">
        <div class="next-step cta">
          <span class="num">Next step</span>
          <h4>Lock these rates in</h4>
          <p>Reply to the email this sheet came with, or grab time with the team — onboarding takes under two weeks.</p>
          <a class="link" href="https://anatainc.com/contact" target="_blank" rel="noreferrer">Talk to Anata →</a>
        </div>
      </div>
    </section>"""


_TABS_JS = """
  <script>
    (function() {
      var tabs = document.getElementById('off-tabs');
      if (!tabs) return;
      tabs.addEventListener('click', function(evt) {
        var btn = evt.target.closest('button[data-off]');
        if (!btn) return;
        var key = btn.getAttribute('data-off');
        tabs.querySelectorAll('button').forEach(function(b) {
          b.classList.toggle('active', b === btn);
        });
        document.querySelectorAll('.off-pane').forEach(function(pane) {
          pane.hidden = pane.getAttribute('data-pane') !== key;
        });
      });
    })();
  </script>
"""

# PR54-style engagement instrumentation, identical behaviour to the deck's:
# heartbeat URL derives from window.location.pathname so it posts to
# /rate-sheets/{slug}/{run_id}/{token}/heartbeat with no edits.
_ENGAGEMENT_JS = """
  <script>
    (function() {
      var IS_INTERNAL = /[?&]viewer=internal\\b/.test(window.location.search);
      var ACTIVE_INTERVAL = 15 * 1000;
      var IDLE_INTERVAL   = 60 * 1000;
      var IDLE_THRESHOLD  = 2  * 60 * 1000;
      var MAX_SECONDS     = 6  * 60 * 60;
      function getCookie(name) {
        var match = document.cookie.match(new RegExp('(?:^|; )' + name + '=([^;]*)'));
        return match ? decodeURIComponent(match[1]) : '';
      }
      function setCookie(name, value, days) {
        var d = new Date(); d.setTime(d.getTime() + days * 24 * 60 * 60 * 1000);
        document.cookie = name + '=' + encodeURIComponent(value) +
          '; expires=' + d.toUTCString() + '; path=/; samesite=lax';
      }
      function uuid4() {
        return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
          var r = Math.random() * 16 | 0;
          var v = c === 'x' ? r : (r & 0x3 | 0x8);
          return v.toString(16);
        });
      }
      var visitorToken = getCookie('anata_visitor_token');
      if (!visitorToken) {
        visitorToken = uuid4();
        setCookie('anata_visitor_token', visitorToken, 365);
      }
      var lastActivity = Date.now();
      var totalActiveMs = 0;
      var lastTickMs = Date.now();
      var maxScrollPct = 0;
      var sectionDwell = {};
      var visibleSections = new Map();
      function bumpActivity() { lastActivity = Date.now(); }
      ['mousemove', 'keydown', 'scroll', 'touchstart', 'click'].forEach(function(evt) {
        document.addEventListener(evt, bumpActivity, { passive: true });
      });
      setInterval(function() {
        var now = Date.now();
        var delta = now - lastTickMs;
        lastTickMs = now;
        if (document.hidden) return;
        if (now - lastActivity > IDLE_THRESHOLD) return;
        totalActiveMs = Math.min(totalActiveMs + delta, MAX_SECONDS * 1000);
        visibleSections.forEach(function(_, secId) {
          sectionDwell[secId] = (sectionDwell[secId] || 0) + Math.round(delta / 1000);
          if (sectionDwell[secId] > MAX_SECONDS) sectionDwell[secId] = MAX_SECONDS;
        });
      }, 1000);
      function updateScrollDepth() {
        var doc = document.documentElement;
        var winH = window.innerHeight || doc.clientHeight || 0;
        var scrollY = window.scrollY || doc.scrollTop || 0;
        var docH = Math.max(doc.scrollHeight, doc.offsetHeight) - winH;
        if (docH <= 0) return;
        var pct = Math.round((scrollY / docH) * 100);
        if (pct > maxScrollPct) maxScrollPct = Math.min(100, pct);
      }
      window.addEventListener('scroll', updateScrollDepth, { passive: true });
      updateScrollDepth();
      function setupSectionObserver() {
        if (!('IntersectionObserver' in window)) return;
        var observer = new IntersectionObserver(function(entries) {
          entries.forEach(function(entry) {
            var secId = entry.target.id;
            if (!secId) return;
            if (entry.isIntersecting && entry.intersectionRatio >= 0.25) {
              if (!visibleSections.has(secId)) visibleSections.set(secId, Date.now());
            } else {
              visibleSections.delete(secId);
            }
          });
        }, { threshold: [0, 0.25, 0.5, 0.75, 1] });
        document.querySelectorAll('section[id]').forEach(function(el) {
          if (el.id) observer.observe(el);
        });
      }
      if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', setupSectionObserver);
      } else {
        setupSectionObserver();
      }
      var heartbeatUrl = window.location.pathname.replace(/\\/$/, '') + '/heartbeat';
      function buildPayload() {
        return {
          visitor_token: visitorToken,
          is_internal: IS_INTERNAL,
          total_seconds: Math.floor(totalActiveMs / 1000),
          max_scroll_pct: maxScrollPct,
          sections: sectionDwell,
          referrer: document.referrer || ''
        };
      }
      function sendHeartbeat() {
        try {
          fetch(heartbeatUrl, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(buildPayload()),
            keepalive: true
          }).catch(function() {});
        } catch (_e) {}
      }
      function nextDelay() {
        var idleMs = Date.now() - lastActivity;
        return idleMs > IDLE_THRESHOLD ? IDLE_INTERVAL : ACTIVE_INTERVAL;
      }
      function scheduleNext() {
        setTimeout(function tick() {
          sendHeartbeat();
          setTimeout(tick, nextDelay());
        }, nextDelay());
      }
      setTimeout(function() { sendHeartbeat(); scheduleNext(); }, 5000);
      function flush() {
        try {
          var body = JSON.stringify(buildPayload());
          if (navigator.sendBeacon) {
            var blob = new Blob([body], { type: 'application/json' });
            navigator.sendBeacon(heartbeatUrl, blob);
          } else {
            sendHeartbeat();
          }
        } catch (_e) {}
      }
      window.addEventListener('pagehide', flush);
    })();
  </script>
"""


def render_rate_sheet_html(
    *,
    profile: ProspectProfile,
    matrix: RateMatrix,
    flags: SectionFlags,
    origin_label: str,
    generated_on: str,
    settings: Settings,
) -> str:
    monogram = load_brand_asset(settings, "assets/monogram.png")
    stylesheet = load_brand_stylesheet(settings)
    favicon_link = load_brand_favicon_link(settings)
    title = f"{profile.display_name} × Anata — Fulfillment Rate Sheet"

    sections: list[tuple[str, str, str]] = []  # (id, rail label, html)
    sections.append(("sec-01", "Overview", _render_cover(profile, matrix, origin_label, generated_on)))
    if flags.zone_map:
        sections.append(("sec-02", "Zone map", _render_zone_map_section(matrix, origin_label)))
    if flags.rate_matrix:
        sections.append(("sec-03", "Carrier rates", _render_rates_section(matrix)))
    if flags.volume_economics:
        volume_html = _render_volume_section(profile, matrix)
        if volume_html:
            sections.append(("sec-04", "Volume economics", volume_html))
    if flags.cost_comparison or flags.destinations:
        context_html = _render_context_section(profile)
        if context_html:
            sections.append(("sec-05", "Your context", context_html))
    if flags.about_anata:
        sections.append(("sec-06", "Why Anata", _render_about_section()))

    rail_items = "".join(
        f'<li><a class="rail-item{" active" if index == 0 else ""}" href="#{sec_id}">'
        f'<span class="num">{index + 1:02d}</span>{html.escape(label)}</a></li>'
        for index, (sec_id, label, _) in enumerate(sections)
    )
    body_sections = "".join(section_html for _, _, section_html in sections)

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="robots" content="noindex">
  <title>{html.escape(title)}</title>
  {favicon_link}
  <style>{stylesheet}</style>
  <style>
    .data-table th, .data-table td {{
      padding: 9px 12px;
      text-align: left;
      border-bottom: 1px solid var(--anata-line);
      font-size: 13px;
    }}
    .data-table thead th {{
      font-size: 12px;
      letter-spacing: 0.02em;
      color: var(--anata-ink-soft);
      border-bottom: 1px solid var(--anata-line-strong);
    }}
    @media print {{
      /* Show every product's rates in the printed PDF, labeled. */
      .off-pane.rate-pane {{ display: block !important; }}
      .off-pane.rate-pane[hidden] {{ display: block !important; }}
    }}
  </style>
</head>
<body>

<div class="app">

  <aside class="rail" id="rail">
    <div class="rail-brand">
      <div class="rail-logo">{monogram or 'a'}</div>
      <div>
        <div class="rail-brand-name">Anata</div>
        <div class="rail-brand-sub">Rate sheet</div>
      </div>
    </div>
    <div class="rail-eye">Contents</div>
    <ul class="rail-list">{rail_items}</ul>
    <div class="rail-foot">
      <a class="rail-util" id="rail-print" href="#" onclick="event.preventDefault();window.print();return false;">Print PDF <span class="arrow">↗</span></a>
      <a class="rail-util primary" href="#sec-06">Get started <span class="arrow">→</span></a>
    </div>
  </aside>

  <main class="content">
    {body_sections}
  </main>

</div>

{_TABS_JS}
{_ENGAGEMENT_JS}
</body>
</html>"""
