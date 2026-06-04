"""HTML renderer for Advertising > Audit. Standalone module (like
fulfillment_dashboard.py) that emits a full page using the shared agent nav +
.shell/.workspace visual language."""

from __future__ import annotations

import html
from typing import Optional

from sales_support_agent.services.admin_nav import (
    render_agent_favicon_links,
    render_agent_nav,
    render_agent_nav_styles,
)
from sales_support_agent.services.advertising.schema import (
    AD_TYPE_LABELS,
    EXTERNAL_CHANNELS,
    Goals,
    fmt_money,
    fmt_pct,
)

_SEV_COLORS = {
    "high": ("#fdecec", "#c0392b"),
    "medium": ("#fff4d9", "#b9821f"),
    "low": ("#eef4f8", "#3d6b86"),
}


def _esc(value: object) -> str:
    return html.escape("" if value is None else str(value))


def _page(title: str, body: str, *, user: Optional[dict]) -> str:
    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{_esc(title)}</title>
    {render_agent_favicon_links()}
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@700;800&family=Inter:wght@400;500;600&display=swap" rel="stylesheet">
    <style>
      :root {{
        --dark-blue: #2B3644; --light-blue: #85BBDA; --light-brown: #F9F7F3;
        --white: #FFFFFF; --shadow: rgba(43, 54, 68, 0.10);
        --line: rgba(43, 54, 68, 0.12);
      }}
      * {{ box-sizing: border-box; }}
      body {{ margin: 0; background: var(--light-brown); color: var(--dark-blue); font-family: "Inter","Segoe UI",sans-serif; }}
      a {{ color: var(--dark-blue); }}
      {render_agent_nav_styles()}
      .shell {{ max-width: 1280px; margin: 0 auto; padding: 28px 18px 64px; }}
      .workspace {{ background: var(--white); border: 1px solid var(--line); border-radius: 26px; box-shadow: 0 18px 40px var(--shadow); padding: 26px; }}
      .page-header {{ padding-bottom: 18px; border-bottom: 1px solid var(--line); margin-bottom: 22px; }}
      .eyebrow {{ display: inline-block; padding: 10px 15px; border-radius: 6px; background: var(--dark-blue); color: var(--white);
        font-family: "Montserrat",sans-serif; font-weight: 700; font-size: 14px; line-height: 1; letter-spacing: 0.04em; text-transform: uppercase; margin-bottom: 14px; }}
      .page-title {{ margin: 0; font-family: "Montserrat",sans-serif; font-weight: 800; font-size: 44px; line-height: 0.98; letter-spacing: -0.03em; }}
      .highlight {{ color: var(--light-blue); }}
      .page-copy {{ font-size: 16px; line-height: 1.5; margin-top: 10px; max-width: 760px; color: rgba(43,54,68,0.85); }}
      .flash {{ margin-bottom: 18px; padding: 12px 16px; border-radius: 12px; background: #e8f4ea; border: 1px solid #8fbf9a; font-size: 14px; }}
      .flash-detail {{ margin-top: 6px; font-size: 13px; color: rgba(43,54,68,0.75); }}
      .dropzone {{ border: 2px dashed var(--light-blue); border-radius: 16px; padding: 22px; text-align: center; background: #f4f8fb; }}
      .dropzone input[type=file] {{ margin-top: 12px; }}
      details.guide {{ border: 1px solid var(--line); border-radius: 12px; padding: 4px 14px; background: var(--white); }}
      details.guide > summary {{ cursor: pointer; font-weight: 600; font-size: 14px; padding: 8px 0; }}
      details.guide table.burn {{ font-size: 13px; }}
      .plan-card {{ border: 2px solid var(--light-blue); background: #f4f8fb; }}
      .chips {{ margin-top: 8px; display: flex; flex-wrap: wrap; gap: 6px; align-items: center; }}
      .chip {{ padding: 4px 10px; border-radius: 999px; border: 1px solid var(--line); background: var(--white); cursor: pointer; font-size: 12px; font-family: inherit; }}
      .chip:hover {{ background: var(--light-blue); }}
      .card {{ border: 1px solid var(--line); border-radius: 18px; padding: 20px; margin-bottom: 22px; background: var(--white); }}
      .card h2 {{ font-family: "Montserrat",sans-serif; font-size: 18px; margin: 0 0 14px; }}
      .card h2 small {{ font-family: "Inter",sans-serif; font-weight: 500; font-size: 13px; color: rgba(43,54,68,0.6); }}
      .metrics {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 12px; }}
      .metric {{ border: 2px solid var(--line); border-radius: 16px; padding: 16px; display: grid; gap: 6px; }}
      .metric span {{ font-size: 11px; text-transform: uppercase; letter-spacing: 0.06em; color: rgba(43,54,68,0.6); font-family: "Montserrat",sans-serif; font-weight: 700; }}
      .metric strong {{ font-family: "Montserrat",sans-serif; font-size: 24px; }}
      .metric small {{ font-size: 12px; color: rgba(43,54,68,0.65); }}
      .narrative {{ background: #f4f8fb; border-left: 4px solid var(--light-blue); border-radius: 0 12px 12px 0; padding: 16px 18px; font-size: 15px; line-height: 1.55; white-space: pre-wrap; }}
      table.burn {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
      table.burn th {{ text-align: left; font-family: "Montserrat",sans-serif; font-size: 11px; text-transform: uppercase; letter-spacing: 0.05em; color: rgba(43,54,68,0.6); padding: 8px 10px; border-bottom: 2px solid var(--line); }}
      table.burn td {{ padding: 10px; border-bottom: 1px solid var(--line); vertical-align: top; }}
      table.burn tr:hover td {{ background: #fafbfc; }}
      .badge {{ display: inline-block; padding: 3px 9px; border-radius: 999px; font-size: 11px; font-weight: 700; font-family: "Montserrat",sans-serif; text-transform: uppercase; letter-spacing: 0.03em; }}
      .pill {{ display: inline-block; padding: 2px 8px; border-radius: 6px; font-size: 11px; background: #eef2f5; color: var(--dark-blue); }}
      .pill.bulk {{ background: #e2f0e6; color: #2f6b3f; }}
      .pill.manual {{ background: #f3eee2; color: #836a32; }}
      form.grid {{ display: grid; gap: 14px; }}
      .field {{ display: grid; gap: 5px; }}
      .field label {{ font-size: 13px; font-weight: 600; }}
      .field .hint {{ font-size: 12px; color: rgba(43,54,68,0.6); }}
      input[type=text], input[type=number], input[type=file], select {{ padding: 9px 11px; border: 1px solid var(--line); border-radius: 10px; font-size: 14px; font-family: inherit; background: var(--white); width: 100%; }}
      .row {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 14px; }}
      .btn {{ display: inline-block; padding: 11px 20px; border-radius: 10px; border: none; background: var(--dark-blue); color: var(--white);
        font-family: "Montserrat",sans-serif; font-weight: 700; font-size: 14px; cursor: pointer; text-decoration: none; }}
      .btn.secondary {{ background: var(--white); color: var(--dark-blue); border: 1px solid var(--dark-blue); }}
      .btn:hover {{ opacity: 0.92; }}
      .runs li {{ padding: 8px 0; border-bottom: 1px solid var(--line); font-size: 14px; display: flex; justify-content: space-between; }}
      .empty {{ color: rgba(43,54,68,0.6); font-size: 14px; }}
      .two-col {{ display: grid; grid-template-columns: minmax(0, 1.4fr) minmax(0, 1fr); gap: 22px; align-items: start; }}
      @media (max-width: 900px) {{ .two-col {{ grid-template-columns: 1fr; }} }}
    </style>
  </head>
  <body>
    {render_agent_nav("advertising", advertising_section="advertising_audit", user=user)}
    <div class="shell">
      <div class="workspace">
        {body}
      </div>
    </div>
  </body>
</html>"""


def _metric(label: str, value: str, note: str = "") -> str:
    note_html = f"<small>{_esc(note)}</small>" if note else ""
    return f'<div class="metric"><span>{_esc(label)}</span><strong>{_esc(value)}</strong>{note_html}</div>'


def _severity_badge(severity: str) -> str:
    bg, fg = _SEV_COLORS.get(severity, _SEV_COLORS["low"])
    return f'<span class="badge" style="background:{bg};color:{fg}">{_esc(severity)}</span>'


def _metrics_block(summary: dict) -> str:
    if not summary:
        return ""
    gap = summary.get("gap", {})
    attain = gap.get("revenue_attainment_bps")
    cards = [
        _metric("Total Sales", fmt_money(summary.get("total_sales_cents")),
                f"{fmt_pct(attain)} of goal" if attain is not None else ""),
        _metric("Ad Spend", fmt_money(summary.get("ad_spend_cents"))),
        _metric("External Spend", fmt_money(summary.get("external_spend_cents")), "Meta / TikTok / influencer"),
        _metric("ACoS", fmt_pct(summary.get("acos_bps"))),
        _metric("TACoS", fmt_pct(summary.get("tacos_bps"))),
        _metric("Blended TACoS", fmt_pct(summary.get("blended_tacos_bps")), "incl. off-Amazon"),
        _metric("Units", str(summary.get("total_units", 0))),
    ]
    return f'<div class="metrics">{"".join(cards)}</div>'


def _burn_table(recs: list[dict]) -> str:
    if not recs:
        return '<p class="empty">No recommendations yet — upload your reports and run an audit.</p>'
    rows = []
    for r in recs:
        impact = r.get("projected_impact", {}) or {}
        impact_cents = impact.get("spend_saved_cents") or impact.get("sales_upside_cents") or impact.get("sales_cents")
        impact_str = fmt_money(impact_cents) if impact_cents else "—"
        actionable = (
            '<span class="pill bulk">bulk sheet</span>' if r.get("is_bulk_actionable")
            else '<span class="pill manual">manual</span>'
        )
        change = ""
        if r.get("current_value") or r.get("proposed_value"):
            change = f'{_esc(r.get("current_value"))} → <strong>{_esc(r.get("proposed_value"))}</strong>'
        rows.append(
            f"<tr>"
            f"<td>{_esc(r.get('rank'))}</td>"
            f"<td>{_severity_badge(r.get('severity', 'low'))}</td>"
            f"<td><span class='pill'>{_esc(r.get('category'))}</span>{(' ' + _esc(r.get('ad_type'))) if r.get('ad_type') else ''}</td>"
            f"<td><strong>{_esc(r.get('title'))}</strong><br><span class='empty'>{_esc(r.get('entity_ref'))}</span></td>"
            f"<td>{change}</td>"
            f"<td>{_esc(impact_str)}</td>"
            f"<td>{actionable}</td>"
            f"</tr>"
        )
    return (
        '<table class="burn"><thead><tr>'
        "<th>#</th><th>Severity</th><th>Type</th><th>Action</th><th>Change</th><th>Est. impact</th><th>Apply via</th>"
        "</tr></thead><tbody>" + "".join(rows) + "</tbody></table>"
    )


def _goals_form(goals: Optional[Goals]) -> str:
    g = goals or Goals()
    rev = "" if g.revenue_target_cents is None else f"{g.revenue_target_cents / 100:.2f}"
    acos = "" if g.acos_target_bps is None else f"{g.acos_target_bps / 100:.1f}"
    tacos = "" if g.tacos_target_bps is None else f"{g.tacos_target_bps / 100:.1f}"
    units = "" if g.units_target is None else str(g.units_target)
    periods = "".join(
        f'<option value="{p}"{" selected" if g.period == p else ""}>{p.title()}</option>'
        for p in ("weekly", "monthly", "quarterly")
    )
    return f"""
    <form class="grid" method="post" action="/admin/advertising/audit/goals">
      <div class="row">
        <div class="field"><label>Revenue target ($)</label><input type="number" step="0.01" name="revenue_target" value="{_esc(rev)}" placeholder="100000"></div>
        <div class="field"><label>ACoS target (%)</label><input type="number" step="0.1" name="acos_target" value="{_esc(acos)}" placeholder="30"></div>
        <div class="field"><label>TACoS target (%)</label><input type="number" step="0.1" name="tacos_target" value="{_esc(tacos)}" placeholder="25"></div>
        <div class="field"><label>Units target</label><input type="number" name="units_target" value="{_esc(units)}" placeholder="500"></div>
        <div class="field"><label>Period</label><select name="period">{periods}</select></div>
      </div>
      <div><button class="btn" type="submit">Save goals</button></div>
    </form>
    """


_DOWNLOAD_GUIDE_ROWS = [
    ("Ads performance reports", ".csv", "Ads Console → <strong>Reports</strong> → run the <strong>Search term</strong>, <strong>Advertised product</strong> &amp; <strong>Targeting</strong> templates → export CSV",
     "<strong>Core.</strong> Drives the whole burn list + ACoS/TACoS. The Search term report powers negatives &amp; keyword harvest; Advertised product gives the spend totals. Drop all of them in."),
    ("Business Report — Sales &amp; Traffic", ".csv", "<strong>Seller Central</strong> → Reports → Business Reports → <strong>By ASIN → Detail Page Sales and Traffic By Child Item</strong>",
     "<strong>Core.</strong> Per child-ASIN sessions, units, conversion, total sales → TACoS + gap-to-goal. Use the Child Item one (not By Date / By Parent)."),
    ("Ads bulk-operations file", ".xlsx", "Ads Console → Sponsored ads → <strong>Bulk operations</strong> → Create spreadsheet (custom date range) → Download",
     "<em>Optional.</em> Only needed for the downloadable <strong>apply-sheet</strong> (round-tripped bid/negative changes). Its own area — not the Reports page. Skip it and the burn list is still fully usable."),
    ("Brand Analytics — Search Query Performance", ".csv", "Seller Central → Brands → <strong>Brand Analytics</strong> → Search Query Performance",
     "Optional — market-share context."),
    ("DSP performance", ".csv", "Amazon DSP console → Reports (name the file with “DSP”)",
     "Optional — campaign-level; lands as manual tasks."),
]


def _download_guide() -> str:
    rows = "".join(
        f"<tr><td><strong>{name}</strong> <span class='pill'>{ext}</span></td>"
        f"<td>{where}</td><td class='empty'>{why}</td></tr>"
        for name, ext, where, why in _DOWNLOAD_GUIDE_ROWS
    )
    return f"""
    <details class="guide">
      <summary>📥 What to download from Amazon (exact paths)</summary>
      <p class="empty" style="margin:10px 0;">Use the <strong>same trailing date window</strong> for every report (e.g. last 30 or 60 days)
      and <strong>end it yesterday, not today</strong> — today's data is incomplete and skews ACoS. Drop them all in the box above; the tool detects each.</p>
      <table class="burn"><thead><tr><th>Report</th><th>Where to get it</th><th>Why</th></tr></thead><tbody>{rows}</tbody></table>
    </details>
    """


def _brand_chips(latest: Optional[dict]) -> str:
    cands = ((latest or {}).get("summary") or {}).get("brand_candidates") or []
    if not cands:
        return ""
    chips = "".join(
        f'<button type="button" class="chip" onclick="document.getElementById(\'adv-brand\').value=this.textContent">{_esc(c)}</button>'
        for c in cands[:8]
    )
    return f'<div class="chips"><span class="empty">Detected:</span> {chips}</div>'


def _upload_form(latest: Optional[dict] = None) -> str:
    ext_channels = "".join(f'<option value="{c}">{c.title()}</option>' for c in EXTERNAL_CHANNELS)
    brand_chips = _brand_chips(latest)
    return f"""
    <form class="grid" method="post" action="/admin/advertising/audit/run" enctype="multipart/form-data">
      <div class="dropzone">
        <label for="adv-files"><strong>Drop all your Amazon exports here</strong><br>
        <span class="empty">Bulk file, Search Term, Business Report, SQP, DSP — in any order. The tool detects what each file is.</span></label>
        <input id="adv-files" type="file" name="files" accept=".csv,.xlsx" multiple>
      </div>
      {_download_guide()}
      <div class="card" style="margin:0;background:#fafbfc;">
        <h2 style="font-size:15px;">External marketing spend <small>— off-Amazon channels for blended TACoS</small></h2>
        <div id="ext-rows">
          <div class="row ext-row">
            <div class="field"><label>Channel</label><select name="ext_channel"><option value=""></option>{ext_channels}</select></div>
            <div class="field"><label>Label (optional)</label><input type="text" name="ext_label" placeholder="e.g. Meta prospecting / influencer Jane"></div>
            <div class="field"><label>Amount ($)</label><input type="number" step="0.01" name="ext_amount" placeholder="0.00"></div>
          </div>
        </div>
        <div style="margin-top:10px;"><button type="button" class="btn secondary" id="adv-add-ext">+ Add channel</button></div>
      </div>
      <details class="guide">
        <summary>Assign files individually instead (advanced)</summary>
        <div class="row" style="margin-top:12px;">
          <div class="field"><label>Bulk-operations (XLSX)</label><input type="file" name="bulk_xlsx" accept=".xlsx"></div>
          <div class="field"><label>Search Term (CSV)</label><input type="file" name="search_term_csv" accept=".csv"></div>
          <div class="field"><label>Business Report (CSV)</label><input type="file" name="business_report_csv" accept=".csv"></div>
          <div class="field"><label>Brand Analytics SQP (CSV)</label><input type="file" name="sqp_csv" accept=".csv"></div>
          <div class="field"><label>DSP (CSV)</label><input type="file" name="dsp_csv" accept=".csv"></div>
          <div class="field"><label>External costs (CSV)</label><input type="file" name="external_costs_csv" accept=".csv"></div>
        </div>
      </details>
      <div class="field" style="max-width:420px;">
        <label>Brand focus (optional)</label>
        <input id="adv-brand" type="text" name="brand" placeholder="e.g. Zantrex — leave blank for full account">
        <span class="hint">Scopes the whole audit + growth plan to one brand's campaigns &amp; ASINs.</span>
        {brand_chips}
      </div>
      <div class="field" style="max-width:320px;"><label>Run label (optional)</label><input type="text" name="label" placeholder="Week of Jun 2"></div>
      <div><button class="btn" type="submit">Run weekly audit</button></div>
    </form>
    <script>
    (function(){{
      var add = document.getElementById('adv-add-ext');
      var rows = document.getElementById('ext-rows');
      if (!add || !rows) return;
      add.addEventListener('click', function(){{
        var first = rows.querySelector('.ext-row');
        var clone = first.cloneNode(true);
        clone.querySelectorAll('input').forEach(function(i){{ i.value = ''; }});
        clone.querySelectorAll('select').forEach(function(s){{ s.selectedIndex = 0; }});
        rows.appendChild(clone);
      }});
    }})();
    </script>
    """


def _runs_list(runs: list[dict], current_id: Optional[str]) -> str:
    if not runs:
        return '<p class="empty">No prior audits yet.</p>'
    items = []
    for r in runs:
        s = r.get("summary", {}) or {}
        when = (r.get("created_at") or "")[:16].replace("T", " ")
        label = r.get("label") or "(unlabeled)"
        marker = " · current" if r.get("id") == current_id else ""
        items.append(
            f'<li><a href="/admin/advertising/audit?run={_esc(r.get("id"))}">{_esc(label)}</a>'
            f'<span class="empty">{_esc(when)} · {fmt_money(s.get("total_sales_cents"))} sales · '
            f'blended TACoS {fmt_pct(s.get("blended_tacos_bps"))}{marker}</span></li>'
        )
    return f'<ul class="runs" style="list-style:none;padding:0;margin:0;">{"".join(items)}</ul>'


def render_audit_page(
    *,
    goals: Optional[Goals],
    latest: Optional[dict],
    recommendations: list[dict],
    bulk_available: bool,
    runs: list[dict],
    plan_available: bool = False,
    user: Optional[dict] = None,
    flash: str = "",
    detail: str = "",
) -> str:
    flash_html = ""
    if flash:
        detail_html = f'<div class="flash-detail">{_esc(detail)}</div>' if detail else ""
        flash_html = f'<div class="flash">{_esc(flash)}{detail_html}</div>'

    narrative_html = ""
    metrics_html = ""
    bulk_html = ""
    plan_html = ""
    if latest:
        summary = latest.get("summary", {}) or {}
        brand = summary.get("brand") or ""
        scope = f' · {_esc(brand)}' if brand else " · full account"
        if plan_available:
            plan_html = (
                f'<div class="card plan-card"><h2>📊 Growth plan ready{scope}</h2>'
                f'<a class="btn" href="/admin/advertising/audit/{_esc(latest["id"])}/plan.xlsx">⬇ Download growth plan (XLSX)</a> '
                '<span class="empty">7 tabs: Exec Brief · Burn List · ASIN Scorecard · Campaign Actions · Negatives · Revenue Bridge · Data Requests.</span></div>'
            )
        if latest.get("narrative"):
            narrative_html = (
                f'<div class="card"><h2>Strategic read <small>· {_esc(latest.get("label") or "latest run")}</small></h2>'
                f'<div class="narrative">{_esc(latest["narrative"])}</div></div>'
            )
        metrics_html = f'<div class="card"><h2>Account vs goal{scope}</h2>{_metrics_block(summary)}</div>'
        if bulk_available:
            bulk_html = (
                f'<a class="btn secondary" href="/admin/advertising/audit/{_esc(latest["id"])}/bulk/combined.xlsx">'
                "⬇ Amazon bulk apply-sheet</a> "
                '<span class="empty">Round-tripped from your upload — review, then upload to Seller Central.</span>'
            )

    bulk_block = (bulk_html + '<div style="height:14px"></div>') if bulk_html else ""
    burn_html = (
        '<div class="card"><h2>Burn list <small>· prioritized optimizations</small></h2>'
        f'{bulk_block}{_burn_table(recommendations)}</div>'
    )

    body = f"""
      <section class="page-header">
        <span class="eyebrow">Advertising</span>
        <h1 class="page-title">Weekly <span class="highlight">Audit</span>.</h1>
        <p class="page-copy">Upload your Amazon advertising + sales exports and your goals. The audit compares
        where you are against target — using blended TACoS that includes off-Amazon spend — and produces a
        ranked burn list plus a ready-to-upload Amazon bulk sheet to apply the changes at scale.</p>
      </section>
      {flash_html}
      {plan_html}
      {narrative_html}
      {metrics_html}
      {burn_html}
      <div class="two-col">
        <div class="card"><h2>Run an audit <small>· upload CSV / XLSX exports</small></h2>{_upload_form(latest)}</div>
        <div>
          <div class="card"><h2>Goals</h2>{_goals_form(goals)}</div>
          <div class="card"><h2>History</h2>{_runs_list(runs, latest.get("id") if latest else None)}</div>
        </div>
      </div>
    """
    return _page("agent | Advertising Audit", body, user=user)
