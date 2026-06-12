"""Hosted Rate Sheet HTML — same style guide as the sales deck.

Reuses the brand package stylesheet (`deck.css`) and the deck's shell
vocabulary (.app / .rail / .slide / .eyebrow / .slide-title), so the rate
sheet looks like a sibling of the strategy deck David's prospects already
compliment. Per-product rate tabs reuse the deck's .off-tabs classes; a small
extra print rule expands every tab pane so the printed PDF shows all products.

Story order (per David's redesign feedback): hero (narrative + stat strip +
prospect-specific bullets) -> interactive rate map -> carrier rate matrix
(with viewer-local carrier filter chips) -> the monthly math (volume +
savings merged) -> partner with Anata (Fulfillment + Shipping OS offers +
CTA). Every generic Anata claim lives ONLY in the partner section; the hero
bullets are prospect-specific narrative output.
"""

from __future__ import annotations

import html
import re
from typing import Optional

from sales_support_agent.config import Settings
from sales_support_agent.services.deck.brand_assets import (
    load_brand_asset,
    load_brand_favicon_link,
    load_brand_stylesheet,
)
from sales_support_agent.services.fulfillment_deck.schema import (
    RATE_SOURCE_MOCK,
    NarrativeBlock,
    ProductRates,
    ProspectProfile,
    RateMatrix,
    SectionFlags,
)
from sales_support_agent.services.fulfillment_deck.us_map import (
    carrier_chip,
    render_interactive_rate_map,
)

_SAMPLE_BADGE = (
    '<span style="display:inline-block;background:#fff4d9;border:1px solid #d2a94b;'
    'color:#7a5b14;border-radius:999px;padding:3px 12px;font-size:11px;font-weight:700;'
    'letter-spacing:0.04em;text-transform:uppercase;">Sample rates — illustrative</span>'
)

_ESTIMATED_PILL = (
    '<span style="display:inline-block;background:#fff4d9;border:1px solid #d2a94b;'
    'color:#7a5b14;border-radius:999px;padding:2px 10px;font-size:10px;font-weight:700;'
    'letter-spacing:0.04em;text-transform:uppercase;vertical-align:middle;">'
    "estimated — to be confirmed</span>"
)

# Product names that look like wholesale/freight-shaped volume — these are
# quoted at parcel rates with an explicit caveat, never silently blended.
_WHOLESALE_RE = re.compile(r"b2b|wholesale|pallet|case", re.IGNORECASE)

# Tab labels longer than this are ellipsized (full name goes in title=).
_TAB_LABEL_MAX = 26


def _fmt_rate(value: float) -> str:
    return f"${value:,.2f}"


def _fmt_dims(product) -> str:
    if not product.has_full_package_spec:
        return "—"
    return (
        f"{product.length_in:g} × {product.width_in:g} × {product.height_in:g} in · "
        f"{product.weight_lb:g} lb"
    )


def _carrier_order(product_rates: ProductRates) -> list[str]:
    """Carrier column order: cheapest average best-rate across zones first.

    Columns are CARRIERS (stable across zones) rather than (carrier, service)
    pairs — real WMS data has different cheapest services per zone, which
    would otherwise explode the column count.
    """
    totals: dict[str, float] = {}
    counts: dict[str, int] = {}
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


def _stat_strip(stats: list[tuple[str, str]], sage: bool = False) -> str:
    """Horizontal row of compact stats: number big, label tiny."""
    if not stats:
        return ""
    blocks = "".join(
        f'<div class="stat{" sage" if sage else ""}">'
        f'<div class="stat-num">{html.escape(number)}</div>'
        f'<div class="stat-label">{html.escape(label)}</div></div>'
        for number, label in stats
    )
    return f'<div class="stat-strip">{blocks}</div>'


def _render_carrier_filter(matrix: RateMatrix) -> str:
    """Viewer-local carrier toggle chips — one per carrier in the matrix.

    All enabled by default; clicking toggles the carrier's table columns and
    the map's best-rate computation (handled by the map section's JS via a
    document-level delegate). Never persisted, not admin-preselectable.
    """
    carriers = _matrix_carriers(matrix)
    if not carriers:
        return ""
    chips = "".join(
        f'<button type="button" class="cf-chip" data-carrier="{html.escape(c, quote=True)}" '
        f'aria-pressed="true">{carrier_chip(c)}</button>'
        for c in carriers
    )
    return (
        f'<div class="carrier-filter" id="carrier-filter">'
        f'<span class="cf-label">Carriers:</span>{chips}</div>'
    )


def _render_rate_table(product_rates: ProductRates) -> str:
    carriers = _carrier_order(product_rates)
    if not carriers or not product_rates.zones:
        return '<p class="muted small">No rates available for this product.</p>'
    head_cells = "".join(
        f'<th data-carrier="{html.escape(carrier, quote=True)}">{carrier_chip(carrier)}</th>'
        for carrier in carriers
    )
    body_rows = []
    for zone in product_rates.zones:
        by_carrier: dict[str, object] = {}
        for q in zone.quotes:
            current = by_carrier.get(q.carrier)
            if current is None or q.rate_usd < current.rate_usd:
                by_carrier[q.carrier] = q
        cheapest: Optional[float] = min((q.rate_usd for q in zone.quotes), default=None)
        cells = []
        for carrier in carriers:
            quote = by_carrier.get(carrier)
            esc_carrier = html.escape(carrier, quote=True)
            if quote is None:
                cells.append(f'<td class="rate-cell" data-carrier="{esc_carrier}">—</td>')
                continue
            is_best = cheapest is not None and abs(quote.rate_usd - cheapest) < 0.005
            transit = (
                f'<span class="rc-transit">{quote.transit_days}d</span>'
                if quote.transit_days
                else ""
            )
            cells.append(
                f'<td class="rate-cell{" best" if is_best else ""}" '
                f'data-carrier="{esc_carrier}" data-rate="{quote.rate_usd:.2f}">'
                f'<span class="rc-price">{_fmt_rate(quote.rate_usd)}</span>{transit}'
                f'<span class="rc-service">{html.escape(quote.service)}</span></td>'
            )
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
        full_name = product.name or f"Product {index + 1}"
        display = (
            full_name
            if len(full_name) <= _TAB_LABEL_MAX
            else full_name[: _TAB_LABEL_MAX - 1].rstrip() + "…"
        )
        active_attr = ' class="active"' if index == 0 else ""
        hidden_attr = "" if index == 0 else " hidden"
        tabs.append(
            f'<button{active_attr} type="button" data-off="{key}" '
            f'title="{html.escape(full_name, quote=True)}">{html.escape(display)}</button>'
        )
        units = (
            f" · ~{product.monthly_units:,} units/mo" if product.monthly_units else ""
        )
        # The "estimated" pill lives INSIDE the pane heading, next to the name.
        estimated = f" {_ESTIMATED_PILL}" if product.dims_estimated else ""
        panes.append(
            f'<div class="off-pane rate-pane" data-pane="{key}"{hidden_attr}>'
            f'<h3 style="font-size:18px;font-weight:700;margin:0 0 4px;letter-spacing:-0.015em">'
            f"{html.escape(full_name)}{estimated}</h3>"
            f'<p class="muted small" style="margin:0 0 16px">{html.escape(_fmt_dims(product))}{units}</p>'
            f"{_render_rate_table(product_rates)}"
            f"</div>"
        )
    multi = len(matrix.products) > 1
    tabs_html = f'<div class="off-tabs" id="off-tabs">{"".join(tabs)}</div>' if multi else ""
    return tabs_html + "".join(panes)


def _render_hero(
    profile: ProspectProfile,
    matrix: RateMatrix,
    narrative: NarrativeBlock,
    generated_on: str,
    sec: str = "01",
) -> str:
    """Cover + executive summary merged: narrative lead, stat strip,
    prospect-specific bullets, and a single muted "today" context line."""
    # Stat strip: cheapest rate, fastest transit, monthly orders, ship-from.
    stats: list[tuple[str, str]] = []
    all_quotes = [
        (zone.zone, quote)
        for product_rates in matrix.products
        for zone in product_rates.zones
        for quote in zone.quotes
    ]
    rates = [q.rate_usd for _z, q in all_quotes]
    if rates:
        stats.append((f"From {_fmt_rate(min(rates))}", "per parcel, your specs"))
    transit_pairs = [(q.transit_days, z) for z, q in all_quotes if q.transit_days]
    if transit_pairs:
        days, zone = min(transit_pairs)
        stats.append((f"{days}-day", f"delivery in zone {zone}"))
    volume = profile.monthly_order_volume or sum(
        p.monthly_units or 0 for p in profile.products
    )
    if volume:
        stats.append((f"{volume:,}", "orders / month"))
    stats.append(("Lehi, UT", "ship-from"))

    summary_html = (
        f'<p class="hero-narrative">{html.escape(narrative.executive_summary)}</p>'
        if narrative.executive_summary.strip()
        else ""
    )

    # Single muted context line replaces the old standalone "Your context"
    # section — what they pay today and where their orders go.
    context_parts = []
    if profile.current_costs_note:
        context_parts.append(f"Today: {profile.current_costs_note}")
    if profile.destinations_note:
        context_parts.append(f"Destinations: {profile.destinations_note}")
    context_html = (
        f'<p class="hero-context">{html.escape(" · ".join(context_parts))}</p>'
        if context_parts
        else ""
    )

    bullets_html = ""
    if narrative.bullets:
        items = "".join(
            f'<li><span class="hb-tick">✓</span><span>{html.escape(b)}</span></li>'
            for b in narrative.bullets[:4]
        )
        bullets_html = f'<ul class="hero-bullets">{items}</ul>'

    return f"""
    <section class="slide" id="sec-{sec}" data-key="hero" data-screen-label="{sec} Overview">
      <header class="slide-head">
        <div class="heading-stack">
          <p class="eyebrow">Fulfillment rate sheet · {html.escape(generated_on)}</p>
          <h2 class="slide-title">{html.escape(profile.display_name)} × Anata</h2>
        </div>
      </header>
      {summary_html}
      {context_html}
      {_stat_strip(stats[:4])}
      {bullets_html}
    </section>"""


def _render_rate_map_section(matrix: RateMatrix, origin_label: str, sec: str = "02",
                             requote_path: str = "") -> str:
    return f"""
    <section class="slide" id="sec-{sec}" data-key="rate-map" data-screen-label="{sec} Rate map">
      <header class="slide-head">
        <div class="heading-stack">
          <p class="eyebrow">Explore your rates</p>
          <h2 class="slide-title">What shipping costs, anywhere in the US</h2>
        </div>
        <p class="caption">Every ZIP area in the country, colored by what it costs to ship there from our dock — the rings mark real mileage bands. Hover anywhere for the exact distance and rate. Adjust a product's dims or weight and press “Request rates” — the whole sheet re-quotes with live rates and saves to this report.</p>
      </header>
      {render_interactive_rate_map(matrix, origin_label, requote_path)}
    </section>"""


def _render_rates_section(matrix: RateMatrix, sec: str = "03") -> str:
    badge = _SAMPLE_BADGE if matrix.source == RATE_SOURCE_MOCK else ""
    return f"""
    <section class="slide" id="sec-{sec}" data-key="carrier-rates" data-screen-label="{sec} Carrier rates">
      <header class="slide-head">
        <div class="heading-stack">
          <p class="eyebrow">Carrier costs</p>
          <h2 class="slide-title">Your rates, by product and zone</h2>
        </div>
        <p class="caption">Rates per parcel for each of your product configurations, quoted to a representative city in every zone. Best rate per zone highlighted — toggle carriers to compare. {badge}</p>
      </header>
      {_render_carrier_filter(matrix)}
      {_render_product_tabs(matrix)}
    </section>"""


def _render_monthly_math_section(
    profile: ProspectProfile,
    matrix: RateMatrix,
    narrative: NarrativeBlock,
    savings: Optional[dict],
    blended_rate: Optional[float],
    blend_method: str,
    sec: str = "04",
) -> str:
    """Volume economics + projected savings merged into one stat strip."""
    volume = profile.monthly_order_volume or sum(
        p.product.monthly_units or 0 for p in matrix.products
    )
    if not volume and not savings:
        return ""

    stats: list[tuple[str, str]] = []
    if volume:
        stats.append((f"{volume:,}", "orders / month"))
    if blended_rate:
        stats.append((_fmt_rate(blended_rate), "blended best rate / parcel"))
        if volume:
            stats.append((_fmt_rate(blended_rate * volume), "directional monthly shipping"))

    sage_stats: list[tuple[str, str]] = []
    caption = ""
    if savings:
        try:
            current = float(savings["current_per_parcel"])
            monthly_savings = float(savings["monthly_savings"])
            annual_savings = float(savings["annual_savings"])
        except (KeyError, TypeError, ValueError):
            current = monthly_savings = annual_savings = 0.0
        if monthly_savings:
            sage_stats = [
                (_fmt_rate(current), "your current cost / parcel"),
                (_fmt_rate(monthly_savings), "monthly savings"),
                (_fmt_rate(annual_savings), "annual savings"),
            ]
            if narrative.savings_text.strip():
                caption = f'<p class="caption">{html.escape(narrative.savings_text)}</p>'

    notes = []
    if blended_rate:
        method = blend_method or "flat average across zones"
        notes.append(
            f'<p class="muted small" style="margin-top:12px">Blended best-rate average, '
            f"{html.escape(method)}. Directional math — actual spend depends on your "
            f"destination mix.</p>"
        )
    for product_rates in matrix.products:
        name = product_rates.product.name
        if name and _WHOLESALE_RE.search(name):
            notes.append(
                f'<p class="muted small" style="margin-top:6px">Note: {html.escape(name)} '
                f"is quoted at parcel rates — wholesale volumes often move as freight; "
                f"we'll quote that separately.</p>"
            )

    return f"""
    <section class="slide" id="sec-{sec}" data-key="monthly-math" data-screen-label="{sec} The monthly math">
      <header class="slide-head">
        <div class="heading-stack">
          <p class="eyebrow">The monthly math</p>
          <h2 class="slide-title">What this means monthly</h2>
        </div>
        {caption}
      </header>
      {_stat_strip(stats)}
      {_stat_strip(sage_stats, sage=True)}
      {''.join(notes)}
    </section>"""


def _render_partner_section(sec: str = "05") -> str:
    """Two ways to ship on these rates: full 3PL or Anata Shipping OS."""
    return f"""
    <section class="slide" id="sec-{sec}" data-key="partner" data-screen-label="{sec} Partner with Anata">
      <header class="slide-head">
        <div class="heading-stack">
          <p class="eyebrow">Partner with Anata</p>
          <h2 class="slide-title">Two ways to ship on these rates</h2>
        </div>
      </header>
      <div class="offer-cards">
        <div class="offer-card">
          <h4>Anata Fulfillment</h4>
          <p>Full 3PL — we receive your inventory, pick and pack every order, and ship same-day for orders in by 2pm MT, with these carrier rates built in.</p>
          <ul>
            <li>Receiving, pick/pack, returns</li>
            <li>Named account manager — a person, not a ticket queue</li>
            <li>Shopify, Amazon, and EDI retail integrations</li>
          </ul>
        </div>
        <div class="offer-card">
          <h4>Anata Shipping OS</h4>
          <p>Keep fulfillment in-house and ship on these same negotiated rates through Anata's shipping platform.</p>
          <ul>
            <li>Label printing from your own dock</li>
            <li>Rate shopping across every carrier on this sheet</li>
            <li>Multi-channel order sync</li>
          </ul>
        </div>
      </div>
      <p class="coming-banner">Coming soon: additional Anata fulfillment locations — multi-node placement compresses your zones and lowers these rates further.</p>
      <div class="next-steps" style="margin-top:18px;grid-template-columns:1fr">
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
      // Delegated on document so sections swapped in by the live-requote
      // flow (data-key fragments) keep working without re-binding.
      document.addEventListener('click', function(evt) {
        var tabs = evt.target.closest('#off-tabs');
        if (!tabs) return;
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
    narrative: Optional[NarrativeBlock] = None,
    savings: Optional[dict] = None,
    requote_path: str = "",
    blended_rate: Optional[float] = None,
    blend_method: str = "",
) -> str:
    monogram = load_brand_asset(settings, "assets/monogram.png")
    stylesheet = load_brand_stylesheet(settings)
    favicon_link = load_brand_favicon_link(settings)
    title = f"{profile.display_name} × Anata — Fulfillment Rate Sheet"
    narrative = narrative or NarrativeBlock()

    sections: list[tuple[str, str, str]] = []  # (id, rail label, html)

    def _add(label: str, render) -> None:
        """Append a section, keeping sec-NN ids sequential in document order."""
        sec = f"{len(sections) + 1:02d}"
        block = render(sec)
        if block:
            sections.append((f"sec-{sec}", label, block))

    _add("Overview", lambda sec: _render_hero(profile, matrix, narrative, generated_on, sec))
    if flags.zone_map:
        _add("Rate map", lambda sec: _render_rate_map_section(matrix, origin_label, sec, requote_path))
    if flags.rate_matrix:
        _add("Carrier rates", lambda sec: _render_rates_section(matrix, sec))
    if flags.volume_economics or savings:
        _add("The monthly math", lambda sec: _render_monthly_math_section(
            profile, matrix, narrative, savings, blended_rate, blend_method, sec))
    if flags.about_anata:
        _add("Partner with Anata", lambda sec: _render_partner_section(sec))

    last_sec_id = sections[-1][0] if sections else "sec-01"
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
    /* Density pass: tighter slides, left-aligned body copy everywhere. */
    .slide {{ padding-top: 30px; padding-bottom: 30px; }}
    .slide-head {{ margin-bottom: 16px; }}
    .slide-head .caption {{ text-align: left; }}

    .hero-narrative {{
      max-width: 70ch;
      font-size: 15px;
      line-height: 1.65;
      color: var(--anata-ink-soft);
      text-align: left;
      margin: 0 0 10px;
    }}
    .hero-context {{
      font-size: 12px;
      color: var(--anata-muted);
      margin: 0 0 16px;
      max-width: 70ch;
    }}
    .stat-strip {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px 28px;
      align-items: baseline;
      margin: 14px 0;
      padding: 12px 16px;
      border: 1px solid var(--anata-line);
      border-radius: 12px;
      background: white;
    }}
    .stat-strip .stat {{ min-width: 110px; }}
    .stat-strip .stat-num {{
      font-size: 21px;
      font-weight: 700;
      letter-spacing: -0.015em;
      color: var(--anata-ink);
    }}
    .stat-strip .stat.sage .stat-num {{ color: var(--anata-sage); }}
    .stat-strip .stat-label {{
      font-size: 10px;
      font-weight: 700;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      color: var(--anata-muted);
      margin-top: 2px;
    }}
    .hero-bullets {{
      list-style: none;
      margin: 12px 0 0;
      padding: 0;
      max-width: 70ch;
    }}
    .hero-bullets li {{
      display: flex;
      gap: 8px;
      align-items: baseline;
      font-size: 13px;
      line-height: 1.5;
      color: var(--anata-ink-soft);
      margin-bottom: 6px;
    }}
    .hero-bullets .hb-tick {{ color: var(--anata-sage); font-weight: 800; }}

    .carrier-chip {{
      display: inline-block;
      border-radius: 999px;
      padding: 3px 8px;
      font-size: 10px;
      font-weight: 800;
      letter-spacing: 0.05em;
    }}
    .carrier-filter {{
      display: flex;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
      margin: 0 0 14px;
    }}
    .carrier-filter .cf-label {{
      font-size: 11px;
      font-weight: 700;
      letter-spacing: 0.06em;
      text-transform: uppercase;
      color: var(--anata-muted);
    }}
    .cf-chip {{
      border: 1px solid var(--anata-line);
      background: white;
      border-radius: 999px;
      padding: 4px 6px;
      cursor: pointer;
      line-height: 1;
    }}
    .cf-chip.cf-off {{ opacity: 0.35; }}

    .data-table th, .data-table td {{
      padding: 8px 12px;
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
    .data-table .cf-hidden {{ display: none; }}
    .rc-price {{ font-size: 14px; font-weight: 600; }}
    td.rate-cell.best .rc-price,
    td.rate-cell.js-best .rc-price {{ font-weight: 700; color: var(--anata-sky-deep); }}
    table.js-filtered td.rate-cell.best:not(.js-best) .rc-price {{
      font-weight: 600; color: var(--anata-ink);
    }}
    .rc-transit {{
      display: inline-block;
      margin-left: 6px;
      border-radius: 6px;
      padding: 1px 5px;
      font-size: 10px;
      font-weight: 700;
      background: var(--anata-line);
      color: var(--anata-ink-soft);
      vertical-align: middle;
    }}
    .rc-service {{
      display: block;
      font-size: 10px;
      color: var(--anata-muted);
      margin-top: 2px;
    }}

    .offer-cards {{ display: flex; gap: 14px; flex-wrap: wrap; }}
    .offer-card {{
      flex: 1 1 280px;
      border: 1px solid var(--anata-line);
      border-radius: 14px;
      padding: 16px 18px;
      background: white;
    }}
    .offer-card h4 {{
      font-size: 15px;
      font-weight: 700;
      margin: 0 0 6px;
      letter-spacing: -0.01em;
    }}
    .offer-card p {{
      margin: 0 0 8px;
      font-size: 12.5px;
      line-height: 1.5;
      color: var(--anata-ink-soft);
    }}
    .offer-card ul {{ margin: 0; padding-left: 18px; }}
    .offer-card li {{
      font-size: 12px;
      line-height: 1.6;
      color: var(--anata-ink-soft);
    }}
    .coming-banner {{
      margin: 14px 0 0;
      padding: 10px 14px;
      border: 1px dashed var(--anata-line-strong);
      border-radius: 10px;
      font-size: 12px;
      color: var(--anata-muted);
      background: rgba(255, 253, 249, 0.7);
    }}

    @media print {{
      /* Show every product's rates in the printed PDF, labeled. */
      .off-pane.rate-pane {{ display: block !important; }}
      .off-pane.rate-pane[hidden] {{ display: block !important; }}
      /* Hide interactive controls; tighten type ~10%. */
      .carrier-filter {{ display: none !important; }}
      .slide {{ padding-top: 22px; padding-bottom: 22px; page-break-inside: avoid; }}
      h2.slide-title {{ font-size: 26px; }}
      .hero-narrative {{ font-size: 13.5px; }}
      .data-table th, .data-table td {{ font-size: 11.5px; padding: 6px 10px; }}
      .stat-strip .stat-num {{ font-size: 18px; }}
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
      <a class="rail-util primary" href="#{last_sec_id}">Get started <span class="arrow">→</span></a>
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
