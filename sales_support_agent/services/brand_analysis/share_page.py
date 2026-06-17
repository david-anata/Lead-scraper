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
    <section class="card">
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
    <section class="card">
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
    caveats = "".join(f"<li>{_e(c)}</li>" for c in v.caveats)
    return f"""
    <section class="card valuation">
      <h2>Indicative valuation</h2>
      <div class="val-headline">
        <span class="val-band">{_e(v.headline())}</span>
        <span class="val-basis">indicative enterprise value · {_e(v.primary_basis)} basis · {_e(v.confidence)} confidence</span>
      </div>
      <table class="data-table">
        <thead><tr><th>Method</th><th>Multiple</th><th class="num">Implied EV</th></tr></thead>
        <tbody>{body_rows}</tbody>
      </table>
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
    <section class="card">
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
    <section class="card">
      <h2>Brand &amp; Social <span style="font-size:13px;font-weight:600;color:rgba(43,54,68,.5)">— separate from the financial grade</span></h2>
      <div class="val-headline">
        <span class="grade-badge" style="background:{fill};width:54px;height:54px;font-size:30px;border-radius:14px">{_e(letter)}</span>
        <span class="val-band" style="font-size:22px">{bs.get('score_100',0)}/100</span>
        <span class="val-basis">{_e(bs.get('confidence','Low'))} confidence · {bs.get('assessed_weight_pct',0)}% of signals supplied</span>
      </div>
      <table class="data-table" style="margin-top:8px"><thead><tr><th>Dimension</th><th class="num">Weight</th><th>Grade</th><th>Signal</th></tr></thead>
      <tbody>{rows}</tbody></table>
      <ul class="caveats">{caveats}</ul>
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
    <section class="card">
      <h2>Weighted scorecard</h2>
      <table class="data-table"><thead><tr><th>Dimension</th><th class="num">Weight</th><th>Grade</th><th>Reasoning</th></tr></thead>
      <tbody>{rows}</tbody></table>
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
        _ribbon(report),
        _completeness_meter(report),
        _exec_summary(report),
        _context_callout(report),
        _charts_section(report),
        _thesis_risks(report),
        _brand_social(report),
        _valuation_section(report),
        _yoy_table(report),
        _benchmarks(report),
        _red_flags(report),
        _scorecard(report),
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
