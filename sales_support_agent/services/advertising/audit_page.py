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
      .strip {{ display: flex; flex-wrap: wrap; gap: 14px; align-items: center; justify-content: space-between; border: 2px solid var(--light-blue); background: #f4f8fb; }}
      .strip-info {{ font-size: 15px; }}
      .strip-actions {{ display: flex; gap: 8px; flex-wrap: wrap; }}
      .filelist {{ margin-top: 12px; display: flex; flex-wrap: wrap; gap: 6px; justify-content: center; }}
      .fchip {{ padding: 4px 10px; border-radius: 8px; background: #e2f0e6; color: #2f6b3f; font-size: 12px; }}
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
    ("Per-ASIN COGS", ".csv", "Your own file — two columns: <strong>ASIN, COGS</strong> (optionally FBA Fee, Referral Fee for true landed cost)",
     "Makes it <strong>profit-true</strong>: real break-even ACoS per SKU instead of an ACoS proxy. Persists across runs — upload once."),
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
        <div id="adv-filelist" class="filelist"></div>
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
          <div class="field"><label>Per-ASIN COGS (CSV)</label><input type="file" name="cogs_csv" accept=".csv"></div>
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
      if (add && rows) add.addEventListener('click', function(){{
        var first = rows.querySelector('.ext-row');
        var clone = first.cloneNode(true);
        clone.querySelectorAll('input').forEach(function(i){{ i.value = ''; }});
        clone.querySelectorAll('select').forEach(function(s){{ s.selectedIndex = 0; }});
        rows.appendChild(clone);
      }});
      var files = document.getElementById('adv-files');
      var list = document.getElementById('adv-filelist');
      if (files && list) files.addEventListener('change', function(){{
        list.innerHTML = '';
        for (var i = 0; i < files.files.length; i++) {{
          var chip = document.createElement('span');
          chip.className = 'fchip';
          chip.textContent = files.files[i].name;
          list.appendChild(chip);
        }}
      }});
    }})();
    </script>
    """


def _last_run_strip(latest: dict) -> str:
    """Compact one-line summary of the most recent run + its downloads."""
    s = latest.get("summary", {}) or {}
    brand = s.get("brand") or "Full account"
    when = (latest.get("created_at") or "")[:16].replace("T", " ")
    rid = latest.get("id")
    recs = s.get("recommendation_count")
    meta = f"blended TACoS {fmt_pct(s.get('blended_tacos_bps'))} · ACoS {fmt_pct(s.get('acos_bps'))}"
    if recs:
        meta += f" · {recs} actions"
    dls = []
    if latest.get("has_plan"):
        dls.append(f'<a class="btn" href="/admin/advertising/audit/{_esc(rid)}/plan.xlsx">⬇ Growth plan</a>')
    if latest.get("has_apply"):
        dls.append(f'<a class="btn secondary" href="/admin/advertising/audit/{_esc(rid)}/bulk/combined.xlsx">⬇ Apply sheet</a>')
    dl_html = " ".join(dls) or '<span class="empty">No downloads for this run.</span>'
    return (
        '<div class="card strip">'
        f'<div class="strip-info"><strong>{_esc(brand)}</strong> '
        f'<span class="empty">· {_esc(when)} · {meta}</span></div>'
        f'<div class="strip-actions">{dl_html}</div>'
        "</div>"
    )


def _history_table(runs: list[dict]) -> str:
    if not runs:
        return '<p class="empty">No audits yet — run one above.</p>'
    rows = []
    for r in runs:
        s = r.get("summary", {}) or {}
        brand = s.get("brand") or "Full account"
        when = (r.get("created_at") or "")[:16].replace("T", " ")
        rid = r.get("id")
        recs = s.get("recommendation_count")
        plan = (f'<a class="pill bulk" href="/admin/advertising/audit/{_esc(rid)}/plan.xlsx">⬇ Plan</a>'
                if r.get("has_plan") else "")
        apply = (f'<a class="pill" href="/admin/advertising/audit/{_esc(rid)}/bulk/combined.xlsx">⬇ Apply sheet</a>'
                 if r.get("has_apply") else "")
        downloads = (plan + " " + apply).strip() or '<span class="empty">—</span>'
        rows.append(
            f"<tr><td><strong>{_esc(brand)}</strong></td><td>{_esc(when)}</td>"
            f"<td>{fmt_money(s.get('total_sales_cents'))}</td>"
            f"<td>{fmt_pct(s.get('blended_tacos_bps'))}</td>"
            f"<td>{_esc(recs) if recs else '—'}</td><td>{downloads}</td></tr>"
        )
    return (
        '<table class="burn"><thead><tr>'
        "<th>Brand</th><th>Run</th><th>Sales</th><th>Blended TACoS</th><th>Actions</th><th>Downloads</th>"
        "</tr></thead><tbody>" + "".join(rows) + "</tbody></table>"
    )


def render_audit_page(
    *,
    goals: Optional[Goals],
    runs: list[dict],
    latest: Optional[dict] = None,
    user: Optional[dict] = None,
    flash: str = "",
    detail: str = "",
) -> str:
    flash_html = ""
    if flash:
        detail_html = f'<div class="flash-detail">{_esc(detail)}</div>' if detail else ""
        flash_html = f'<div class="flash">{_esc(flash)}{detail_html}</div>'

    strip_html = _last_run_strip(latest) if latest else ""

    body = f"""
      <section class="page-header">
        <span class="eyebrow">Advertising</span>
        <h1 class="page-title">Burn <span class="highlight">List</span>.</h1>
        <p class="page-copy">Generate a brand burn-list workbook from your Amazon exports. Drop in your ad +
        sales reports, pick a brand, and download a ready-to-act plan plus an upload-ready Amazon bulk sheet.
        The full analysis lives in the workbook — this page runs it and keeps your history.</p>
      </section>
      {flash_html}
      {strip_html}
      <div class="card"><h2>Run an audit <small>· drop your CSV / XLSX exports</small></h2>{_upload_form(latest)}</div>
      <div class="card"><h2>Goals <small>· targets the workbook measures against</small></h2>{_goals_form(goals)}</div>
      <div class="card"><h2>History <small>· past runs &amp; downloads</small></h2>{_history_table(runs)}</div>
    """
    return _page("agent | Advertising Burn List", body, user=user)
