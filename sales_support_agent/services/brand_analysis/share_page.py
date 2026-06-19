"""Standalone, branded investor landing page for a Brand Analysis report.

Unlike report_page.py (which renders inside the admin chrome), this produces a
fully self-contained HTML document — its own styles, the brand's logo/colour,
Chart.js visualisations, and no admin nav — suitable for a token-gated public
share link an investor can open. The same HTML is stored on the report row and
served by both the admin "preview" and the public route.

Charts use Chart.js 4.4.0 (the app's charting library of record, already used
on the cashflow overview). All data is embedded inline so the page is portable.
"""

from __future__ import annotations

import html
import json

from sales_support_agent.services.brand_analysis.schema import (
    BrandReport,
    fmt_money,
    fmt_mult,
    fmt_pct,
)
from sales_support_agent.services.brand_analysis.valuation import ValuationRange

_CHART_CDN = "https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"

_GRADE_FILL = {"A": "#2E7D5B", "B": "#3F8F6E", "C": "#B8860B", "D": "#C2663B", "F": "#8B4C42"}
_TONE_COLOR = {"good": "#2E7D5B", "warn": "#B8860B", "bad": "#8B4C42", "neutral": "#2B3644"}
_SEV_COLOR = {"Critical": "#8B4C42", "High": "#C2663B", "Medium": "#B8860B"}


def _e(v: object) -> str:
    return html.escape(str(v) if v is not None else "")


# ---------------------------------------------------------------------------
# Chart data
# ---------------------------------------------------------------------------


def _chart_data(report: BrandReport) -> dict:
    cur, prior = report.current, report.prior

    def d(cents):
        return None if cents is None else round(cents / 100, 2)

    yoy_labels = ["Net revenue", "COGS", "Gross profit", "Marketing", "Net earnings"]
    cur_vals = [d(cur.net_revenue_cents), d(cur.cogs_cents), d(cur.reported_gross_profit_cents),
                d(cur.marketing_total_cents), d(cur.net_earnings_cents)]
    prior_vals = [d(prior.net_revenue_cents), d(prior.cogs_cents), d(prior.reported_gross_profit_cents),
                  d(prior.marketing_total_cents), d(prior.net_earnings_cents)]

    media = report.media_mix or {}
    media_labels = [k.replace("_", " ").title() for k in media.keys()]
    media_vals = [round(v / 100, 2) for v in media.values()]

    dims = report.scorecard.dimensions
    radar_labels = [getattr(dd, "label", dd.get("label") if isinstance(dd, dict) else "") for dd in dims]
    radar_vals = [getattr(dd, "points", dd.get("points") if isinstance(dd, dict) else 0) for dd in dims]

    monthly = report.monthly_revenue or []
    monthly_labels = [m[0] for m in monthly]
    monthly_vals = [round(m[1] / 100, 2) for m in monthly]

    return {
        "yoy": {"labels": yoy_labels, "current": cur_vals,
                "prior": prior_vals if report.has_yoy else None},
        "media": {"labels": media_labels, "values": media_vals},
        "radar": {"labels": radar_labels, "values": radar_vals},
        "monthly": {"labels": monthly_labels, "values": monthly_vals},
    }


# ---------------------------------------------------------------------------
# Section builders
# ---------------------------------------------------------------------------


def _cover(report: BrandReport) -> str:
    grade = report.scorecard.letter
    fill = _GRADE_FILL.get(grade, "#2B3644")
    logo = ""
    if report.logo_data_uri:
        logo = f'<img class="cover-logo" src="{_e(report.logo_data_uri)}" alt="{_e(report.brand)} logo">'
    tagline = f'<div class="cover-tagline">{_e(report.brand_tagline)}</div>' if report.brand_tagline else ""
    thesis_line = report.investment_thesis[0] if report.investment_thesis else report.recommendation
    periods = report.period_current_label or "Current period"
    if report.has_yoy and report.period_prior_label:
        periods += f" vs {report.period_prior_label}"
    return f"""
    <header class="cover" style="--grade:{fill}">
      <div class="cover-left">
        {logo}
        <div class="cover-eyebrow">Executive Acquisition Brief</div>
        <h1 class="cover-brand">{_e(report.brand)}</h1>
        {tagline}
        <p class="cover-thesis">{_e(thesis_line)}</p>
        <div class="cover-meta">{_e(periods)} · Prepared {_e(report.prepared_date)} · {_e(report.intake_summary)}</div>
      </div>
      <div class="cover-grade">
        <div class="grade-badge" style="background:{fill}">{_e(grade)}</div>
        <div class="grade-score">{report.scorecard.score_100}<span>/100</span></div>
        <div class="grade-rec">{_e(report.recommendation)}</div>
      </div>
    </header>"""


def _ribbon(report: BrandReport) -> str:
    chips = report.info_ribbon or []
    if not chips:
        return ""
    cells = "".join(
        f"""<div class="chip">
          <div class="chip-label">{_e(c.get('label'))}</div>
          <div class="chip-value" style="color:{_TONE_COLOR.get(c.get('tone'), '#2B3644')}">{_e(c.get('value'))}</div>
        </div>"""
        for c in chips
    )
    return f'<section class="ribbon">{cells}</section>'


def _completeness_meter(report: BrandReport) -> str:
    pct = max(0, min(100, report.data_completeness_pct))
    color = "#2E7D5B" if pct >= 85 else ("#B8860B" if pct >= 60 else "#C2663B")
    return f"""
    <div class="meter-wrap">
      <div class="meter-head">
        <span>Data completeness</span>
        <strong style="color:{color}">{pct}% · {_e(report.confidence)} confidence</strong>
      </div>
      <div class="meter-track"><div class="meter-fill" style="width:{pct}%;background:{color}"></div></div>
      <div class="meter-note">Share of the material input set actually supplied. Higher completeness tightens the grade and the valuation band.</div>
    </div>"""


def _exec_summary(report: BrandReport) -> str:
    bullets = "".join(f"<li>{_e(s)}</li>" for s in report.stands_out)
    return f"""
    <section id="summary" class="card">
      <h2>Executive Summary</h2>
      <p class="lead">{_e(report.executive_summary)}</p>
      {f'<ul class="stands-out">{bullets}</ul>' if bullets else ""}
    </section>"""


def _context_callout(report: BrandReport) -> str:
    notes = (report.context_notes or "").strip()
    if not notes:
        return ""
    return f"""
    <section class="card context-callout">
      <h3 style="margin:0 0 8px">Analyst context</h3>
      <p style="margin:0;white-space:pre-wrap">{_e(notes)}</p>
    </section>"""


def _charts_section(report: BrandReport) -> str:
    has_media = bool(report.media_mix)
    has_monthly = bool(report.monthly_revenue)
    media_card = (
        '<div class="chart-card"><h3>Media mix</h3><canvas id="mediaChart"></canvas></div>'
        if has_media else ""
    )
    monthly_card = (
        '<div class="chart-card"><h3>Monthly revenue trajectory</h3><canvas id="monthlyChart"></canvas></div>'
        if has_monthly else ""
    )
    return f"""
    <section id="charts" class="card">
      <h2>The numbers at a glance</h2>
      <div class="chart-grid">
        <div class="chart-card"><h3>Financial overview{' (YoY)' if report.has_yoy else ''}</h3><canvas id="yoyChart"></canvas></div>
        <div class="chart-card"><h3>Scorecard by dimension</h3><canvas id="radarChart"></canvas></div>
        {media_card}
        {monthly_card}
      </div>
    </section>"""


def _thesis_risks(report: BrandReport) -> str:
    thesis = "".join(f"<li>{_e(t)}</li>" for t in report.investment_thesis)
    risks = "".join(f"<li>{_e(r)}</li>" for r in report.key_risks)
    return f"""
    <section class="card">
      <h2>Investment thesis &amp; key risks</h2>
      <div class="two-col">
        <div class="col-good">
          <h3>Why it's interesting</h3>
          <ul>{thesis}</ul>
        </div>
        <div class="col-bad">
          <h3>What to diligence</h3>
          <ul>{risks}</ul>
        </div>
      </div>
    </section>"""


def _valuation_section(report: BrandReport) -> str:
    v = ValuationRange.from_dict(report.valuation)
    if not v.is_meaningful():
        caveats = "".join(f"<li>{_e(c)}</li>" for c in v.caveats)
        return f"""
        <section class="card">
          <h2>Indicative valuation</h2>
          <p class="lead">Insufficient established financials to size a range yet.</p>
          <ul class="caveats">{caveats}</ul>
        </section>"""

    rows = []
    if v.rev_ev_low_cents is not None:
        rows.append(("Revenue multiple", f"{v.rev_multiple_low:.2f}–{v.rev_multiple_high:.2f}x net revenue",
                     f"{fmt_money(v.rev_ev_low_cents)} – {fmt_money(v.rev_ev_high_cents)}"))
    if v.earn_ev_low_cents is not None:
        rows.append((f"Earnings multiple", f"{v.earn_multiple_low:.2f}–{v.earn_multiple_high:.2f}x {v.earnings_basis_label.lower()}",
                     f"{fmt_money(v.earn_ev_low_cents)} – {fmt_money(v.earn_ev_high_cents)}"))
    body_rows = "".join(f"<tr><td>{_e(a)}</td><td>{_e(b)}</td><td class='num'>{_e(c)}</td></tr>" for a, b, c in rows)

    # Inventory row — additive to EV, negotiated at close
    inv = v.inventory_cents
    if inv and inv > 0:
        total_lo = fmt_money((v.ev_low_cents or 0) + inv)
        total_hi = fmt_money((v.ev_high_cents or 0) + inv)
        inventory_block = f"""
      <div class="inv-note">
        <strong>+ Inventory at cost: {fmt_money(inv)}</strong>
        <span> — typically negotiated separately at close</span><br>
        <span class="muted">Total consideration range: {total_lo} – {total_hi}</span>
      </div>"""
    else:
        inventory_block = (
            '<p class="muted inv-note" style="font-size:12px">'
            'Inventory not established — confirm and add to total consideration at close.</p>'
        )

    caveats = "".join(f"<li>{_e(c)}</li>" for c in v.caveats)
    return f"""
    <section id="valuation" class="card valuation">
      <h2>Indicative valuation</h2>
      <div class="val-headline">
        <span class="val-band">{_e(v.headline())}</span>
        <span class="val-basis">indicative enterprise value · {_e(v.primary_basis)} basis · {_e(v.confidence)} confidence</span>
      </div>
      <table class="data-table">
        <thead><tr><th>Method</th><th>Multiple</th><th class="num">Implied EV</th></tr></thead>
        <tbody>{body_rows}</tbody>
      </table>
      {inventory_block}
      <ul class="caveats">{caveats}</ul>
    </section>"""


def _yoy_table(report: BrandReport) -> str:
    cur, prior = report.current, report.prior
    rows = [
        ("Net revenue", fmt_money(cur.net_revenue_cents), fmt_money(prior.net_revenue_cents)),
        ("COGS", fmt_money(cur.cogs_cents), fmt_money(prior.cogs_cents)),
        ("Product gross margin", fmt_pct(cur.product_gm_bps), fmt_pct(prior.product_gm_bps)),
        ("Marketing spend", fmt_money(cur.marketing_total_cents), fmt_money(prior.marketing_total_cents)),
        ("Marketing % of revenue", fmt_pct(cur.marketing_pct_bps), fmt_pct(prior.marketing_pct_bps)),
        ("Blended MER", fmt_mult(cur.blended_mer), fmt_mult(prior.blended_mer)),
        ("Contribution margin", fmt_pct(cur.contribution_margin_bps), fmt_pct(prior.contribution_margin_bps)),
        ("Net earnings", fmt_money(cur.net_earnings_cents), fmt_money(prior.net_earnings_cents)),
        ("Net margin", fmt_pct(cur.net_margin_bps), fmt_pct(prior.net_margin_bps)),
    ]
    head = "<th>Metric</th><th class='num'>Current</th>" + ("<th class='num'>Prior</th>" if report.has_yoy else "")
    body = ""
    for label, c, p in rows:
        body += f"<tr><td>{_e(label)}</td><td class='num'>{_e(c)}</td>" + (f"<td class='num'>{_e(p)}</td>" if report.has_yoy else "") + "</tr>"
    return f"""
    <section id="financials" class="card">
      <h2>Financial overview</h2>
      <table class="data-table"><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>
    </section>"""


def _benchmarks(report: BrandReport) -> str:
    if not report.benchmarks:
        return ""
    rows = ""
    for b in report.benchmarks:
        passed = getattr(b, "passed", b.get("passed") if isinstance(b, dict) else None)
        kpi = getattr(b, "kpi", b.get("kpi") if isinstance(b, dict) else "")
        healthy = getattr(b, "healthy", b.get("healthy") if isinstance(b, dict) else "")
        actual = getattr(b, "actual", b.get("actual") if isinstance(b, dict) else "")
        verdict = "PASS" if passed else ("FAIL" if passed is False else "—")
        color = "#2E7D5B" if passed else ("#8B4C42" if passed is False else "#7a8694")
        rows += (f"<tr><td>{_e(kpi)}</td><td>{_e(healthy)}</td><td class='num'>{_e(actual)}</td>"
                 f"<td style='color:{color};font-weight:700'>{verdict}</td></tr>")
    return f"""
    <section class="card">
      <h2>Category benchmarks</h2>
      <table class="data-table"><thead><tr><th>KPI</th><th>Healthy range</th><th class="num">This brand</th><th>Verdict</th></tr></thead>
      <tbody>{rows}</tbody></table>
    </section>"""


def _red_flags(report: BrandReport) -> str:
    if not report.red_flags:
        return ""
    items = ""
    for f in report.red_flags:
        sev = getattr(f, "severity", f.get("severity") if isinstance(f, dict) else "Medium")
        title = getattr(f, "title", f.get("title") if isinstance(f, dict) else "")
        detail = getattr(f, "detail", f.get("detail") if isinstance(f, dict) else "")
        items += (f'<div class="flag"><span class="flag-sev" style="background:{_SEV_COLOR.get(sev, "#B8860B")}">{_e(sev)}</span>'
                  f'<div><strong>{_e(title)}</strong>{f"<div class=flag-detail>{_e(detail)}</div>" if detail else ""}</div></div>')
    return f'<section class="card"><h2>Red flags</h2><div class="flags">{items}</div></section>'


def _brand_social(report: BrandReport) -> str:
    bs = report.brand_social or {}
    if not bs or not bs.get("dimensions"):
        return ""
    letter = bs.get("letter", "F")
    fill = _GRADE_FILL.get(letter, "#2B3644")
    rows = ""
    for d in bs.get("dimensions", []):
        assessed = d.get("assessed", True)
        dl = d.get("letter", "")
        shown = dl if assessed else "n/a"
        color = _GRADE_FILL.get(dl, "#7a8694") if assessed else "#7a8694"
        rows += (f"<tr><td>{_e(d.get('label'))}</td><td class='num'>{int(d.get('weight',0)*100)}%</td>"
                 f"<td style='color:{color};font-weight:800'>{_e(shown)}</td>"
                 f"<td class='reason'>{_e(d.get('reason'))}</td></tr>")
    caveats = "".join(f"<li>{_e(c)}</li>" for c in bs.get("caveats", []))
    return f"""
    <section id="social" class="card">
      <h2>Social &amp; DTC Opportunity <span style="font-size:13px;font-weight:600;color:rgba(43,54,68,.5)">— channel expansion upside (separate from financial grade)</span></h2>
      <div class="val-headline">
        <span class="grade-badge" style="background:{fill};width:54px;height:54px;font-size:30px;border-radius:14px">{_e(letter)}</span>
        <span class="val-band" style="font-size:22px">{bs.get('score_100',0)}/100</span>
        <span class="val-basis">{_e(bs.get('confidence','Low'))} confidence · {bs.get('assessed_weight_pct',0)}% of signals supplied</span>
      </div>
      <table class="data-table" style="margin-top:8px"><thead><tr><th>Dimension</th><th class="num">Weight</th><th>Grade</th><th>Signal</th></tr></thead>
      <tbody>{rows}</tbody></table>
      <ul class="caveats">{caveats}</ul>
    </section>"""


def _ascend_growth_playbook(report: BrandReport) -> str:
    """Deterministic Ascend integration roadmap and value-creation plan."""
    bs = report.brand_social or {}
    cur = report.current
    if not cur:
        return ""

    rev_cents = cur.net_revenue_cents or 0

    # Day 1 EBITDA lift: 10–15% of revenue from Anata 3PL + Shipping OS
    lo_str = fmt_money(int(rev_cents * 0.10)) if rev_cents else "—"
    hi_str = fmt_money(int(rev_cents * 0.15)) if rev_cents else "—"
    uplift_str = f"{lo_str} – {hi_str}/yr" if rev_cents else "—"

    # Exit range: Y5 revenue ≈ current × 2.5; use SDE if available, else net earnings, else 40% est.
    earnings_base = (cur.sde_cents or cur.net_earnings_cents or int(rev_cents * 0.40)) if rev_cents else 0
    if cur.sde_cents:
        earnings_basis_label = "Based on reported SDE"
    elif cur.net_earnings_cents:
        earnings_basis_label = "Based on net earnings"
    else:
        earnings_basis_label = "Estimated at 40% EBITDA"
    exit_lo = fmt_money(int(earnings_base * 2.5 * 5.0)) if rev_cents else "—"
    exit_hi = fmt_money(int(earnings_base * 2.5 * 6.5)) if rev_cents else "—"
    exit_range = f"{exit_lo} – {exit_hi}" if rev_cents else "—"

    # Social & DTC build plan from brand_social dimensions
    dims_by_key = {d.get("key"): d for d in (bs.get("dimensions") or [])}
    soc = dims_by_key.get("social_oppty", {})
    dtc = dims_by_key.get("dtc_opportunity", {})

    if soc.get("letter") == "A":
        social_note = "Ascend builds Instagram, TikTok, Facebook, and YouTube from Day 1 — full brand voice control, zero legacy overhead."
    else:
        social_note = soc.get("reason") or "Existing social presence — Ascend expands and reactivates stale channels."

    if dtc.get("letter") == "A":
        dtc_note = "Ascend launches Shopify store + email/SMS flows at Month 6 — clean-slate list build, full first-party data ownership."
    else:
        dtc_note = dtc.get("reason") or "Existing email list — Ascend activates DTC retention and win-back flows."

    # Work effort signal: high social opportunity score + few red flags = low integration overhead
    ss = bs.get("score_100", 0)
    crit_flags = len([f for f in (report.red_flags or [])
                      if (getattr(f, "severity", None) or (f.get("severity") if isinstance(f, dict) else "")) == "Critical"])
    if ss >= 75 and crit_flags == 0:
        effort = ("Low", "#2E7D5B", "Minimal legacy overhead — Ascend builds from a clean slate across social, DTC, and ops.")
    elif ss >= 50 and crit_flags <= 1:
        effort = ("Medium", "#B8860B", "Some existing channels to transition. Plan for 60-90 days of onboarding complexity.")
    else:
        effort = ("High", "#8B4C42", "Significant integration work — multiple existing channels to migrate and critical flags to resolve.")

    return f"""
    <section id="playbook" class="card">
      <h2>Ascend Growth Playbook <span style="font-size:13px;font-weight:600;color:rgba(43,54,68,.5)">— integration &amp; value-creation roadmap</span></h2>

      <h3 style="margin:0 0 10px">Integration Timeline</h3>
      <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:24px">
        <div style="background:rgba(43,54,68,.04);border-radius:12px;padding:14px">
          <div style="font-family:Montserrat;font-weight:800;font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#85BBDA;margin-bottom:4px">Phase 0 &middot; Day 1–60</div>
          <div style="font-weight:700;font-size:13px;margin-bottom:6px;color:#2B3644">Handoff</div>
          <ul style="margin:0;padding-left:16px;font-size:12px;color:rgba(43,54,68,.65);line-height:1.6">
            <li>Ops &amp; inventory transfer</li>
            <li>Anata 3PL + Shipping OS onboard</li>
            <li>PPC audit &amp; in-house takeover</li>
          </ul>
        </div>
        <div style="background:rgba(43,54,68,.04);border-radius:12px;padding:14px">
          <div style="font-family:Montserrat;font-weight:800;font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#85BBDA;margin-bottom:4px">Phase 1 &middot; Month 3–12</div>
          <div style="font-weight:700;font-size:13px;margin-bottom:6px;color:#2B3644">Optimization</div>
          <ul style="margin:0;padding-left:16px;font-size:12px;color:rgba(43,54,68,.65);line-height:1.6">
            <li>ACOS target &lt;15%</li>
            <li>Anata cost savings realized</li>
            <li>First EBITDA lift ~10–15%</li>
          </ul>
        </div>
        <div style="background:rgba(43,54,68,.04);border-radius:12px;padding:14px">
          <div style="font-family:Montserrat;font-weight:800;font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#85BBDA;margin-bottom:4px">Phase 2 &middot; Month 6–18</div>
          <div style="font-weight:700;font-size:13px;margin-bottom:6px;color:#2B3644">Channel Diversification</div>
          <ul style="margin:0;padding-left:16px;font-size:12px;color:rgba(43,54,68,.65);line-height:1.6">
            <li>Shopify + email list launch</li>
            <li>TikTok Shop activation</li>
            <li>Walmart listing</li>
          </ul>
        </div>
        <div style="background:rgba(43,54,68,.04);border-radius:12px;padding:14px">
          <div style="font-family:Montserrat;font-weight:800;font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#85BBDA;margin-bottom:4px">Phase 3 &middot; Year 1–3</div>
          <div style="font-weight:700;font-size:13px;margin-bottom:6px;color:#2B3644">Brand Expansion</div>
          <ul style="margin:0;padding-left:16px;font-size:12px;color:rgba(43,54,68,.65);line-height:1.6">
            <li>New SKU development</li>
            <li>Retail door expansion</li>
            <li>Adjacent categories</li>
          </ul>
        </div>
      </div>

      <div style="display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-bottom:22px">
        <div>
          <h3 style="margin:0 0 10px">Channel Revenue Mix Roadmap</h3>
          <table class="data-table" style="font-size:13px">
            <thead><tr><th>Year</th><th class="num">Amazon</th><th class="num">TikTok</th><th class="num">DTC</th><th class="num">Walmart</th></tr></thead>
            <tbody>
              <tr><td><strong>Y0</strong></td><td class="num">100%</td><td class="num">—</td><td class="num">—</td><td class="num">—</td></tr>
              <tr><td><strong>Y1</strong></td><td class="num">55%</td><td class="num">25%</td><td class="num">15%</td><td class="num">5%</td></tr>
              <tr><td><strong>Y3</strong></td><td class="num">25%</td><td class="num">25%</td><td class="num">25%</td><td class="num">25%</td></tr>
              <tr><td><strong>Y5</strong></td><td class="num">25%</td><td class="num">25%</td><td class="num">25%</td><td class="num">25%</td></tr>
            </tbody>
          </table>
        </div>
        <div>
          <h3 style="margin:0 0 10px">Day 1 EBITDA Uplift (Anata Advantage)</h3>
          <table class="data-table" style="font-size:13px">
            <tbody>
              <tr><td>Anata 3PL + Shipping OS savings</td><td class="num" style="color:#2E7D5B"><strong>{_e(uplift_str)}</strong></td></tr>
              <tr><td>PPC target (in-house)</td><td class="num">TACoS &lt;15%</td></tr>
              <tr><td>Industry avg TACoS baseline</td><td class="num" style="color:#8B4C42">~18–22%</td></tr>
              <tr><td>Estimated blended MER target (Y1)</td><td class="num">&gt;3.5×</td></tr>
            </tbody>
          </table>
        </div>
      </div>

      <h3 style="margin:0 0 10px">Year 1 Channel Launch Budget (Indicative)</h3>
      <table class="data-table" style="font-size:13px;margin-bottom:22px">
        <thead>
          <tr>
            <th>Channel</th>
            <th class="num">Target Revenue (Y1)</th>
            <th class="num">Est. Ad Spend</th>
            <th class="num">Start</th>
            <th>Notes</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td><strong>Amazon PPC</strong></td>
            <td class="num">{_e(fmt_money(int(rev_cents * 0.55))) if rev_cents else "—"}</td>
            <td class="num">{_e(fmt_money(int(rev_cents * 0.55 * 0.12))) if rev_cents else "—"}</td>
            <td class="num"><strong>Day 1</strong></td>
            <td>Target TACoS ≤12% (reduce from ~18–22%)</td>
          </tr>
          <tr>
            <td><strong>TikTok Shop</strong></td>
            <td class="num">{_e(fmt_money(int(rev_cents * 0.25))) if rev_cents else "—"}</td>
            <td class="num">{_e(fmt_money(int(rev_cents * 0.25 * 0.20))) if rev_cents else "—"}</td>
            <td class="num">Month 3</td>
            <td>Content creation + paid ads; ~20% blended</td>
          </tr>
          <tr>
            <td><strong>DTC (Shopify)</strong></td>
            <td class="num">{_e(fmt_money(int(rev_cents * 0.15))) if rev_cents else "—"}</td>
            <td class="num">{_e(fmt_money(int(rev_cents * 0.15 * 0.25))) if rev_cents else "—"}</td>
            <td class="num">Month 6</td>
            <td>Meta/Google + email/SMS flows; ~25% CAC</td>
          </tr>
          <tr>
            <td><strong>Walmart</strong></td>
            <td class="num">{_e(fmt_money(int(rev_cents * 0.05))) if rev_cents else "—"}</td>
            <td class="num">{_e(fmt_money(int(rev_cents * 0.05 * 0.05))) if rev_cents else "—"}</td>
            <td class="num">Month 6</td>
            <td>Listing fees + Promoted Listings; ~5%</td>
          </tr>
          <tr style="border-top:2px solid rgba(43,54,68,.15);font-weight:700">
            <td>Total</td>
            <td class="num">{_e(fmt_money(rev_cents)) if rev_cents else "—"}</td>
            <td class="num">{_e(fmt_money(int(rev_cents * (0.55*0.12 + 0.25*0.20 + 0.15*0.25 + 0.05*0.05)))) if rev_cents else "—"}</td>
            <td class="num"></td>
            <td></td>
          </tr>
        </tbody>
      </table>

      <div style="display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-bottom:22px">
        <div>
          <h3 style="margin:0 0 10px">Social &amp; DTC Build Plan</h3>
          <p style="font-size:13px;margin:0 0 8px;color:rgba(43,54,68,.8)"><strong>Social:</strong> {_e(social_note)}</p>
          <p style="font-size:13px;margin:0;color:rgba(43,54,68,.8)"><strong>DTC/Email:</strong> {_e(dtc_note)}</p>
        </div>
        <div>
          <h3 style="margin:0 0 10px">Exit Positioning</h3>
          <p style="font-size:13px;margin:0 0 6px"><strong>Target exit:</strong> Year 3–5 at 5–6.5× EBITDA</p>
          <p style="font-size:13px;margin:0 0 6px"><strong>Est. exit value range:</strong> <strong style="color:#2E7D5B">{_e(exit_range)}</strong></p>
          <p style="font-size:12px;color:rgba(43,54,68,.45);margin:0">{_e(earnings_basis_label)} × 2.5 (Y5 growth multiple) × 5–6.5× exit.</p>
        </div>
      </div>

      <div style="display:flex;align-items:center;gap:12px;padding:12px 16px;background:rgba(43,54,68,.04);border-radius:10px">
        <div style="font-family:Montserrat;font-weight:800;font-size:11px;text-transform:uppercase;letter-spacing:.06em;color:rgba(43,54,68,.5)">Integration Effort</div>
        <div style="font-family:Montserrat;font-weight:800;font-size:14px;color:{effort[1]}">{effort[0]}</div>
        <div style="font-size:12.5px;color:rgba(43,54,68,.65)">{_e(effort[2])}</div>
      </div>
    </section>"""


def _scorecard(report: BrandReport) -> str:
    rows = ""
    for d in report.scorecard.dimensions:
        letter = getattr(d, "letter", "")
        label = getattr(d, "label", "")
        weight = getattr(d, "weight", 0)
        reason = getattr(d, "reason", "")
        rows += (f"<tr><td>{_e(label)}</td><td class='num'>{int(weight*100)}%</td>"
                 f"<td style='color:{_GRADE_FILL.get(letter, '#2B3644')};font-weight:800'>{_e(letter)}</td>"
                 f"<td class='reason'>{_e(reason)}</td></tr>")
    return f"""
    <section id="scorecard-legacy" class="card">
      <h2>Weighted scorecard</h2>
      <table class="data-table"><thead><tr><th>Dimension</th><th class="num">Weight</th><th>Grade</th><th>Reasoning</th></tr></thead>
      <tbody>{rows}</tbody></table>
    </section>"""


def _flag_sev(f) -> str:
    return getattr(f, "severity", None) if not isinstance(f, dict) else f.get("severity", "")


def _flag_title(f) -> str:
    return getattr(f, "title", "") if not isinstance(f, dict) else f.get("title", "")


def _flag_detail(f) -> str:
    return getattr(f, "detail", "") if not isinstance(f, dict) else f.get("detail", "")


def _hard_disqualifier_banner(report: BrandReport) -> str:
    """Red banner listing Critical-severity flags right under the cover.
    Signals immediate pass criteria before the analyst reads any further."""
    criticals = [f for f in report.red_flags if _flag_sev(f) == "Critical"]
    if not criticals:
        return ""

    def _item(f) -> str:
        title = _e(_flag_title(f))
        detail = _flag_detail(f)
        detail_html = (
            '<span style="color:rgba(255,255,255,.75);font-size:13px"> &mdash; ' + _e(detail) + "</span>"
            if detail else ""
        )
        return f'<li style="margin:4px 0"><strong>{title}</strong>{detail_html}</li>'

    items = "".join(_item(f) for f in criticals)
    return f"""
    <section id="disqualifiers" style="background:#7B2D2D;color:#fff;border-radius:16px;padding:20px 24px;margin-bottom:18px;border-left:6px solid #C0392B">
      <div style="display:flex;align-items:center;gap:12px;margin-bottom:10px">
        <span style="font-size:24px;flex-shrink:0">⛔</span>
        <div>
          <div style="font-family:Montserrat;font-weight:800;font-size:16px">{len(criticals)} Hard Disqualifier{"s" if len(criticals)>1 else ""} — Review Before Proceeding</div>
          <div style="font-size:13px;color:rgba(255,255,255,.75);margin-top:2px">These criteria are Ascend minimums. Any one of them is sufficient to pass on this acquisition.</div>
        </div>
      </div>
      <ul style="margin:0;padding-left:20px;font-size:14px">{items}</ul>
    </section>"""


def _section_nav(report: BrandReport) -> str:
    """Sticky horizontal mini-nav bar linking to each major section."""
    has_flags = bool(report.red_flags)
    has_social = bool((report.brand_social or {}).get("dimensions"))
    has_comp = bool((report.brand_competitive or {}).get("dimensions"))
    links = [
        ("#summary", "Summary"),
        ("#flags", "Red Flags") if has_flags else None,
        ("#scorecard", "Scorecard"),
        ("#charts", "Charts"),
        ("#financials", "Financials"),
        ("#competitive", "Competitive") if has_comp else None,
        ("#social", "Social &amp; DTC") if has_social else None,
        ("#playbook", "Playbook"),
        ("#valuation", "Valuation"),
        ("#distressed-offer", "Distressed Offer") if report.scorecard.letter in ("D", "F") else None,
    ]
    items = "".join(
        f'<a href="{href}" style="text-decoration:none;color:rgba(43,54,68,.7);font-family:Montserrat;font-weight:700;'
        f'font-size:11px;text-transform:uppercase;letter-spacing:.06em;padding:6px 12px;border-radius:20px;'
        f'white-space:nowrap;transition:background .15s" '
        f'onmouseover="this.style.background=\'rgba(133,187,218,.18)\'" '
        f'onmouseout="this.style.background=\'transparent\'">{label}</a>'
        for href, label in (lnk for lnk in links if lnk is not None)
    )
    return f"""
    <nav style="position:sticky;top:0;z-index:100;background:rgba(249,247,243,.95);backdrop-filter:blur(8px);
      border-bottom:1px solid rgba(43,54,68,.08);margin:0 -20px 18px;padding:8px 20px;
      display:flex;align-items:center;gap:4px;overflow-x:auto">
      {items}
    </nav>"""


def _competitive_section(report: BrandReport) -> str:
    """Competitive position grade — third A–F track."""
    bc = report.brand_competitive or {}
    if not bc or not bc.get("dimensions"):
        return ""
    letter = bc.get("letter", "F")
    fill = _GRADE_FILL.get(letter, "#2B3644")
    rows = ""
    for d in bc.get("dimensions", []):
        assessed = d.get("assessed", True)
        dl = d.get("letter", "")
        shown = dl if assessed else "n/a"
        color = _GRADE_FILL.get(dl, "#7a8694") if assessed else "#7a8694"
        rows += (f"<tr><td>{_e(d.get('label'))}</td><td class='num'>{int(d.get('weight',0)*100)}%</td>"
                 f"<td style='color:{color};font-weight:800'>{_e(shown)}</td>"
                 f"<td class='reason'>{_e(d.get('reason'))}</td></tr>")

    # Competitor comparison table
    comp_table = ""
    competitors = bc.get("competitors") or []
    if competitors:
        def _fmt_reviews(c):
            return f"{int(c.get('reviews', 0)):,}" if c.get("reviews") else "—"

        def _fmt_price(c):
            return "${:.2f}".format(c.get("price_cents", 0) / 100) if c.get("price_cents") else "—"

        comp_rows = "".join(
            f"<tr><td>{_e(c.get('name', ''))}</td>"
            f"<td class='num'>{_e(str(c.get('bsr') or '—'))}</td>"
            f"<td class='num'>{_e(_fmt_reviews(c))}</td>"
            f"<td class='num'>{_e(_fmt_price(c))}</td></tr>"
            for c in competitors
        )
        comp_table = f"""
        <h3 style="margin:14px 0 8px">Known competitors</h3>
        <table class="data-table" style="font-size:13px">
          <thead><tr><th>Name</th><th class="num">BSR</th><th class="num">Reviews</th><th class="num">Price</th></tr></thead>
          <tbody>{comp_rows}</tbody>
        </table>"""

    notes = bc.get("analyst_notes") or ""
    notes_block = f'<p style="margin:12px 0 0;font-size:13px;color:rgba(43,54,68,.65)"><strong>Analyst notes:</strong> {_e(notes)}</p>' if notes else ""

    return f"""
    <section id="competitive" class="card">
      <h2>Competitive Position <span style="font-size:13px;font-weight:600;color:rgba(43,54,68,.5)">— Amazon shelf presence (separate track)</span></h2>
      <div class="val-headline">
        <span class="grade-badge" style="background:{fill};width:54px;height:54px;font-size:30px;border-radius:14px">{_e(letter)}</span>
        <span class="val-band" style="font-size:22px">{bc.get('score_100',0)}/100</span>
        <span class="val-basis">{_e(bc.get('confidence','Low'))} confidence · {bc.get('assessed_weight_pct',0)}% of signals supplied</span>
      </div>
      <table class="data-table" style="margin-top:8px"><thead><tr><th>Dimension</th><th class="num">Weight</th><th>Grade</th><th>Signal</th></tr></thead>
      <tbody>{rows}</tbody></table>
      {comp_table}
      {notes_block}
    </section>"""


def _scorecard_visual(report: BrandReport) -> str:
    """Redesigned scorecard: one row per dimension with grade badge, weight bar,
    and reason — easier to scan than a dense table."""
    rows = ""
    for d in report.scorecard.dimensions:
        letter = getattr(d, "letter", "")
        label = getattr(d, "label", "")
        weight = getattr(d, "weight", 0)
        reason = getattr(d, "reason", "")
        assessed = getattr(d, "assessed", True)
        fill = _GRADE_FILL.get(letter, "#7a8694") if assessed else "#b0b8c1"
        shown_letter = letter if assessed else "NA"
        weight_pct = int(weight * 100)
        rows += f"""
        <div style="display:grid;grid-template-columns:52px 1fr;gap:14px;align-items:start;
            padding:12px 0;border-bottom:1px solid rgba(43,54,68,.07)">
          <div style="text-align:center">
            <div style="width:46px;height:46px;border-radius:12px;background:{fill};color:#fff;
                font-family:Montserrat;font-weight:900;font-size:22px;
                display:flex;align-items:center;justify-content:center">{_e(shown_letter)}</div>
            <div style="font-size:10px;color:rgba(43,54,68,.4);margin-top:3px">{weight_pct}%</div>
          </div>
          <div>
            <div style="font-weight:700;font-size:14px;color:#2B3644;margin-bottom:3px">{_e(label)}</div>
            <div style="font-size:13px;color:rgba(43,54,68,.6);line-height:1.45">{_e(reason)}</div>
          </div>
        </div>"""
    return f"""
    <section id="scorecard" class="card">
      <h2>Weighted Scorecard <span style="font-size:13px;font-weight:600;color:rgba(43,54,68,.5)">— {report.scorecard.score_100}/100 composite</span></h2>
      <div>{rows}</div>
    </section>"""


def _red_flags_visual(report: BrandReport) -> str:
    """Redesigned red flags with severity tiers and visual weight."""
    if not report.red_flags:
        return """
        <section id="flags" class="card" style="border-left:4px solid #2E7D5B">
          <h2>Red Flags</h2>
          <p style="color:#2E7D5B;font-weight:600;margin:0">No material red flags from the supplied data.</p>
        </section>"""

    by_sev: dict[str, list] = {"Critical": [], "High": [], "Medium": []}
    for f in report.red_flags:
        sev = _flag_sev(f) or "Medium"
        by_sev.setdefault(sev, []).append(f)

    _SEV_CFG = {
        "Critical": ("#7B2D2D", "#C0392B", "⛔"),
        "High":     ("#5C3310", "#C2663B", "⚠️"),
        "Medium":   ("#4A3800", "#B8860B", "ℹ"),
    }
    sections = ""
    for sev, flags in by_sev.items():
        if not flags:
            continue
        bg, border, icon = _SEV_CFG[sev]
        items = ""
        for f in flags:
            title = _flag_title(f)
            detail = _flag_detail(f)
            detail_html = (
                "<div style='font-size:13px;color:rgba(43,54,68,.6);margin-top:3px'>" + _e(detail) + "</div>"
                if detail else ""
            )
            items += (
                "<div style='padding:10px 0;border-bottom:1px solid rgba(43,54,68,.07)'>"
                f"<div style='font-weight:700;font-size:14px;color:#2B3644'>{_e(title)}</div>"
                f"{detail_html}"
                "</div>"
            )
        sections += f"""
        <div style="margin-bottom:14px">
          <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px">
            <span style="font-size:16px">{icon}</span>
            <span style="font-family:Montserrat;font-weight:800;font-size:12px;text-transform:uppercase;
              letter-spacing:.06em;color:{_SEV_COLOR.get(sev,'#666')}">{sev}</span>
            <span style="font-size:11px;color:rgba(43,54,68,.4)">{len(flags)} flag{"s" if len(flags)>1 else ""}</span>
          </div>
          <div style="padding-left:28px">{items}</div>
        </div>"""

    return f"""
    <section id="flags" class="card" style="border-left:4px solid {_SEV_COLOR.get('Critical','#8B4C42')}">
      <h2>Red Flags</h2>
      {sections}
    </section>"""


def _distressed_offer(report: BrandReport) -> str:
    """Distressed/turnaround acquisition evaluation — shown only for D/F grade brands.
    Shows what price Ascend would offer after applying a turnaround discount to the
    standard valuation, and what would need to be fixed post-acquisition."""
    letter = report.scorecard.letter
    if letter not in ("D", "F"):
        return ""

    v = ValuationRange.from_dict(report.valuation)
    score = report.scorecard.score_100

    # Turnaround haircut: F = 35–50% of normal EV, D = 55–70%
    if letter == "F":
        low_pct, high_pct = 0.35, 0.50
        tier_label = "Deep Turnaround"
        tier_color = "#8B4C42"
        tier_note = "Significant operational and financial remediation required before Ascend can deploy its playbook."
    else:  # D
        low_pct, high_pct = 0.55, 0.70
        tier_label = "Turnaround Acquisition"
        tier_color = "#C2663B"
        tier_note = "Brand has structural weaknesses but is not a write-off — Ascend would require a meaningful discount."

    # Compute distressed range from the normal EV
    ev_low = v.ev_low_cents
    ev_high = v.ev_high_cents
    if ev_low and ev_high:
        distressed_lo = fmt_money(int(ev_low * low_pct))
        distressed_hi = fmt_money(int(ev_high * high_pct))
        price_html = f"""
        <div class="val-headline" style="margin-bottom:10px">
          <span class="val-band" style="font-size:24px;color:{tier_color}">{distressed_lo} – {distressed_hi}</span>
          <span class="val-basis">indicative distressed offer · {int(low_pct*100)}–{int(high_pct*100)}% of standard range</span>
        </div>
        <table class="data-table" style="font-size:13px;margin-bottom:0">
          <tbody>
            <tr><td>Standard indicative EV</td><td class="num">{fmt_money(ev_low)} – {fmt_money(ev_high)}</td></tr>
            <tr><td>Turnaround discount applied</td><td class="num" style="color:{tier_color}">{int((1-high_pct)*100)}–{int((1-low_pct)*100)}%</td></tr>
            <tr><td>Distressed offer range</td><td class="num" style="color:{tier_color};font-weight:700">{distressed_lo} – {distressed_hi}</td></tr>
          </tbody>
        </table>"""
    else:
        price_html = '<p class="muted">Insufficient financial data to size a distressed range.</p>'

    # What needs to change for a viable acquisition
    crit_flags = [f for f in report.red_flags
                  if _flag_sev(f) == "Critical"]
    high_flags = [f for f in report.red_flags
                  if _flag_sev(f) == "High"]

    fix_items = ""
    for f in (crit_flags + high_flags)[:6]:
        title = _e(_flag_title(f))
        fix_items += f'<li style="margin:4px 0">{title}</li>'
    fix_section = (
        f'<ul style="margin:8px 0 0;padding-left:18px;font-size:13px">{fix_items}</ul>'
        if fix_items else ""
    )

    return f"""
    <section id="distressed-offer" class="card" style="border-left:4px solid {tier_color}">
      <h2>{tier_label} <span style="font-size:13px;font-weight:600;color:rgba(43,54,68,.5)">— offer framework for grade {letter} brands</span></h2>
      <div style="background:rgba(139,76,66,0.06);border-radius:12px;padding:16px 18px;margin-bottom:14px">
        <div style="font-weight:700;font-size:13px;color:{tier_color};margin-bottom:4px">{_e(tier_label)}</div>
        <div style="font-size:13px;color:rgba(43,54,68,.7)">{_e(tier_note)}</div>
      </div>
      {price_html}
      {"<h3 style='margin:14px 0 6px'>Issues that must be remediated or priced in</h3>" + fix_section if fix_section else ""}
      <p style="font-size:12px;color:rgba(43,54,68,.4);margin:12px 0 0">
        Ascend would proceed only if seller accepts the distressed range AND provides representations
        on the critical flags above. LOI should include price adjustment clauses tied to trailing revenue verification.
      </p>
    </section>"""


def _provenance(report: BrandReport) -> str:
    if not report.account_mappings and not report.unmapped_accounts:
        return ""
    mapped = ""
    for field_name, info in (report.account_mappings or {}).items():
        sources = ", ".join(info.get("sources", [])) if isinstance(info, dict) else ""
        conf = info.get("confidence", "") if isinstance(info, dict) else ""
        pretty = field_name.replace("_cents", "").replace("_", " ").title()
        mapped += f"<tr><td>{_e(pretty)}</td><td>{_e(sources)}</td><td>{_e(conf)}</td></tr>"
    unmapped = ""
    if report.unmapped_accounts:
        unmapped = ("<p class='prov-note'>Accounts not auto-classified (excluded from totals): "
                    + _e(", ".join(report.unmapped_accounts[:20])) + "</p>")
    model_note = (f"<p class='prov-note'>Line items classified by {_e(report.classifier_model)}.</p>"
                  if report.classifier_model else "")
    mapped_table = (f"<table class='data-table'><thead><tr><th>Bucket</th><th>Source accounts</th><th>Confidence</th></tr></thead>"
                    f"<tbody>{mapped}</tbody></table>") if mapped else ""
    return f"""
    <details class="card prov">
      <summary>Data provenance &amp; account mapping</summary>
      {model_note}
      {mapped_table}
      {unmapped}
    </details>"""


def _data_gaps(report: BrandReport) -> str:
    if not report.data_gaps:
        return ""
    items = "".join(f"<li>{_e(g)}</li>" for g in report.data_gaps)
    return f'<section class="card"><h2>Data to close the gaps</h2><ul class="caveats">{items}</ul></section>'


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def render_share_page(report: BrandReport, *, public: bool = True) -> str:
    data = _chart_data(report)
    sections = "\n".join([
        _section_nav(report),
        _ribbon(report),
        _completeness_meter(report),
        _exec_summary(report),
        _context_callout(report),
        _thesis_risks(report),
        _red_flags_visual(report),
        _scorecard_visual(report),
        _charts_section(report),
        _yoy_table(report),
        _benchmarks(report),
        _competitive_section(report),
        _brand_social(report),
        _ascend_growth_playbook(report),
        _valuation_section(report),
        _distressed_offer(report),
        _data_gaps(report),
        _provenance(report),
    ])
    confidential = (
        '<div class="confidential">Confidential — prepared for evaluation purposes. '
        'Indicative analysis, not a formal valuation or investment advice.</div>'
    )
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="robots" content="noindex, nofollow">
<title>{_e(report.brand)} — Acquisition Brief</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Montserrat:wght@700;800;900&display=swap" rel="stylesheet">
<style>{_STYLES}</style>
</head>
<body>
<main class="page">
  {_cover(report)}
  {_hard_disqualifier_banner(report)}
  {sections}
  {confidential}
  <footer class="foot">Generated by the Anata agent · Brand Analysis · {_e(report.prepared_date)}</footer>
</main>
<script src="{_CHART_CDN}"></script>
<script>window.__BA = {json.dumps(data)};</script>
<script>{_CHART_JS}</script>
<script>{_PRINT_JS}</script>
</body>
</html>"""


# Opening the page with ?print=1 (the "Download PDF" button) auto-opens the
# browser's print dialog once the charts have drawn → Save as PDF gives a
# pixel-faithful copy of this branded page, no server-side PDF dependency.
_PRINT_JS = """
(function(){
  if(!/[?&]print=1/.test(location.search))return;
  var fire=function(){setTimeout(function(){window.print();}, 900);};
  if(document.readyState==='complete')fire(); else window.addEventListener('load', fire);
})();
"""


_STYLES = """
:root{--navy:#2B3644;--blue:#85BBDA;--brown:#BFA889;--cream:#F9F7F3;--white:#fff;
 --line:rgba(43,54,68,.10);--shadow:rgba(43,54,68,.10);--good:#2E7D5B;--bad:#8B4C42;}
*{box-sizing:border-box}
body{margin:0;background:var(--cream);color:var(--navy);font-family:"Inter","Segoe UI",sans-serif;line-height:1.5}
.page{max-width:1000px;margin:0 auto;padding:28px 20px 64px}
.cover{display:flex;justify-content:space-between;gap:28px;background:var(--white);border:1px solid var(--line);
 border-radius:24px;padding:40px;box-shadow:0 18px 40px var(--shadow);border-top:6px solid var(--grade);margin-bottom:20px}
.cover-logo{max-height:54px;max-width:200px;margin-bottom:18px;display:block;object-fit:contain}
.cover-eyebrow{font-family:"Montserrat";font-weight:800;font-size:11px;letter-spacing:.12em;text-transform:uppercase;color:var(--blue)}
.cover-brand{font-family:"Montserrat";font-weight:900;font-size:46px;line-height:1;margin:8px 0 6px;letter-spacing:-.02em}
.cover-tagline{color:rgba(43,54,68,.6);font-size:15px;margin-bottom:14px}
.cover-thesis{font-size:18px;font-weight:500;max-width:46ch;margin:10px 0 16px}
.cover-meta{font-size:12px;color:rgba(43,54,68,.45)}
.cover-grade{text-align:center;flex-shrink:0}
.grade-badge{width:96px;height:96px;border-radius:20px;color:#fff;font-family:"Montserrat";font-weight:900;
 font-size:56px;display:flex;align-items:center;justify-content:center;margin:0 auto 10px}
.grade-score{font-family:"Montserrat";font-weight:800;font-size:24px}.grade-score span{font-size:14px;color:rgba(43,54,68,.4)}
.grade-rec{font-size:13px;font-weight:700;color:var(--navy);margin-top:4px}
.ribbon{display:flex;flex-wrap:wrap;gap:12px;margin-bottom:20px}
.chip{flex:1;min-width:130px;background:var(--white);border:1px solid var(--line);border-radius:14px;padding:14px 16px}
.chip-label{font-size:11px;text-transform:uppercase;letter-spacing:.06em;color:rgba(43,54,68,.5);font-weight:700}
.chip-value{font-family:"Montserrat";font-weight:800;font-size:18px;margin-top:4px}
.card{background:var(--white);border:1px solid var(--line);border-radius:20px;padding:26px 28px;margin-bottom:18px;box-shadow:0 6px 20px var(--shadow)}
.card h2{font-family:"Montserrat";font-weight:800;font-size:20px;margin:0 0 14px}
.card h3{font-family:"Montserrat";font-weight:700;font-size:14px;margin:0 0 10px;color:rgba(43,54,68,.7)}
.lead{font-size:16px;margin:0 0 12px}
.stands-out{margin:8px 0 0;padding-left:18px}.stands-out li{margin:6px 0}
.meter-wrap{background:var(--white);border:1px solid var(--line);border-radius:16px;padding:18px 22px;margin-bottom:18px}
.meter-head{display:flex;justify-content:space-between;font-size:14px;font-weight:600;margin-bottom:8px}
.meter-track{height:10px;background:rgba(43,54,68,.08);border-radius:99px;overflow:hidden}
.meter-fill{height:100%;border-radius:99px;transition:width .4s}
.meter-note{font-size:12px;color:rgba(43,54,68,.45);margin-top:8px}
.chart-grid{display:grid;grid-template-columns:1fr 1fr;gap:20px}
.chart-card{min-width:0}.chart-card canvas{max-height:280px}
.two-col{display:grid;grid-template-columns:1fr 1fr;gap:22px}
.col-good h3{color:var(--good)}.col-bad h3{color:var(--bad)}
.two-col ul{margin:0;padding-left:18px}.two-col li{margin:8px 0}
.data-table{width:100%;border-collapse:collapse;font-size:14px}
.data-table th{text-align:left;font-family:"Montserrat";font-weight:700;font-size:12px;text-transform:uppercase;
 letter-spacing:.04em;color:rgba(43,54,68,.55);padding:8px 10px;border-bottom:2px solid var(--line)}
.data-table td{padding:9px 10px;border-bottom:1px solid var(--line)}
.data-table .num{text-align:right;font-variant-numeric:tabular-nums}
.data-table .reason{color:rgba(43,54,68,.6);font-size:13px}
.valuation .val-headline{display:flex;align-items:baseline;gap:12px;flex-wrap:wrap;margin-bottom:14px}
.val-band{font-family:"Montserrat";font-weight:900;font-size:30px;color:var(--navy)}
.val-basis{font-size:13px;color:rgba(43,54,68,.5)}
.caveats{margin:14px 0 0;padding-left:18px;font-size:12.5px;color:rgba(43,54,68,.55)}.caveats li{margin:5px 0}
.inv-note{margin:12px 0;padding:10px 14px;background:rgba(133,187,218,0.1);border:1px solid rgba(133,187,218,0.3);border-radius:8px;font-size:13px}
.flags{display:flex;flex-direction:column;gap:10px}
.flag{display:flex;gap:12px;align-items:flex-start}
.flag-sev{color:#fff;font-size:11px;font-weight:700;padding:3px 9px;border-radius:99px;flex-shrink:0;font-family:"Montserrat"}
.flag-detail{font-size:13px;color:rgba(43,54,68,.6);margin-top:2px}
.prov{padding:18px 22px}.prov summary{cursor:pointer;font-family:"Montserrat";font-weight:700;font-size:14px}
.prov-note{font-size:12px;color:rgba(43,54,68,.5);margin:10px 0}
.context-callout{background:rgba(133,187,218,.10);border-left:4px solid var(--blue)}
.context-callout h3{color:var(--navy)}
.confidential{text-align:center;font-size:12px;color:rgba(43,54,68,.4);margin:22px 0 8px}
@media print{
  body{background:#fff}
  .page{max-width:none;padding:0}
  .card,.cover,.chip,.meter-wrap{box-shadow:none;break-inside:avoid;page-break-inside:avoid}
  .cover{border-top-width:6px}
  section.card{margin-bottom:14px}
  .chart-card canvas{max-height:260px}
  @page{margin:14mm}
}
.foot{text-align:center;font-size:12px;color:rgba(43,54,68,.35);margin-top:10px}
@media(max-width:760px){.cover{flex-direction:column}.chart-grid,.two-col{grid-template-columns:1fr}.cover-brand{font-size:34px}}
"""

_CHART_JS = """
(function(){
  if(!window.Chart||!window.__BA)return;
  var D=window.__BA, navy="#2B3644", blue="#85BBDA", brown="#BFA889";
  var money=function(v){return "$"+Number(v).toLocaleString();};
  Chart.defaults.font.family='Inter, sans-serif';
  Chart.defaults.color='rgba(43,54,68,.7)';
  function el(id){return document.getElementById(id);}
  // YoY grouped bar
  if(el('yoyChart')){
    var ds=[{label:'Current',data:D.yoy.current,backgroundColor:blue,borderRadius:6}];
    if(D.yoy.prior){ds.push({label:'Prior',data:D.yoy.prior,backgroundColor:brown,borderRadius:6});}
    new Chart(el('yoyChart'),{type:'bar',data:{labels:D.yoy.labels,datasets:ds},
      options:{responsive:true,plugins:{legend:{display:!!D.yoy.prior},tooltip:{callbacks:{label:function(c){return c.dataset.label+': '+money(c.parsed.y);}}}},
      scales:{y:{ticks:{callback:function(v){return money(v);}}}}}});
  }
  // Scorecard radar (0-4 points per dimension)
  if(el('radarChart')){
    new Chart(el('radarChart'),{type:'radar',data:{labels:D.radar.labels,datasets:[{label:'Grade points',data:D.radar.values,
      backgroundColor:'rgba(133,187,218,.25)',borderColor:blue,pointBackgroundColor:navy}]},
      options:{responsive:true,plugins:{legend:{display:false}},scales:{r:{min:0,max:4,ticks:{stepSize:1,backdropColor:'transparent'}}}}});
  }
  // Media mix doughnut
  if(el('mediaChart')&&D.media.values.length){
    new Chart(el('mediaChart'),{type:'doughnut',data:{labels:D.media.labels,datasets:[{data:D.media.values,
      backgroundColor:['#85BBDA','#2B3644','#BFA889','#2E7D5B','#C2663B','#B8860B','#6c7a89']}]},
      options:{responsive:true,plugins:{legend:{position:'right'},tooltip:{callbacks:{label:function(c){return c.label+': '+money(c.parsed);}}}}}});
  }
  // Monthly revenue line
  if(el('monthlyChart')&&D.monthly.values.length){
    new Chart(el('monthlyChart'),{type:'line',data:{labels:D.monthly.labels,datasets:[{label:'Revenue',data:D.monthly.values,
      borderColor:blue,backgroundColor:'rgba(133,187,218,.2)',fill:true,tension:.3,pointRadius:2}]},
      options:{responsive:true,plugins:{legend:{display:false},tooltip:{callbacks:{label:function(c){return money(c.parsed.y);}}}},
      scales:{y:{ticks:{callback:function(v){return money(v);}}}}}});
  }
})();
"""
