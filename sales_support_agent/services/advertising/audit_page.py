"""HTML renderer for Advertising > Audit. Standalone module (like
fulfillment_dashboard.py) that emits a full page using the shared agent nav +
.shell/.workspace visual language."""

from __future__ import annotations

import html
import json
from datetime import date, timedelta
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


def _week_label() -> str:
    today = date.today()
    monday = today - timedelta(days=today.weekday())
    return f"Week of {monday.strftime('%b %-d')}"


def _page(
    title: str,
    body: str,
    *,
    user: Optional[dict],
    advertising_section: str = "advertising_audit",
) -> str:
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
      a.golink {{ display: inline-block; margin-left: 6px; padding: 1px 8px; border-radius: 6px; background: var(--dark-blue); color: var(--white); font-size: 12px; text-decoration: none; white-space: nowrap; }}
      a.golink:hover {{ background: var(--light-blue); color: var(--dark-blue); }}
      details.faq-item {{ border-bottom: 1px solid var(--line); padding: 4px 0; }}
      details.faq-item > summary {{ cursor: pointer; font-weight: 600; font-size: 15px; padding: 10px 2px; }}
      details.faq-item p {{ margin: 0 2px 12px; font-size: 14px; line-height: 1.55; color: rgba(43,54,68,0.85); }}
      .loading-overlay {{ display: none; position: fixed; inset: 0; background: rgba(43,54,68,0.55); z-index: 999; align-items: center; justify-content: center; }}
      .loading-overlay.show {{ display: flex; }}
      .loading-box {{ background: var(--white); border-radius: 18px; padding: 30px 36px; display: grid; gap: 10px; justify-items: center; text-align: center; max-width: 380px; box-shadow: 0 18px 50px rgba(0,0,0,0.3); }}
      .loading-box strong {{ font-family: "Montserrat",sans-serif; font-size: 18px; }}
      .spinner {{ width: 38px; height: 38px; border: 4px solid var(--line); border-top-color: var(--light-blue); border-radius: 50%; animation: adv-spin 0.8s linear infinite; }}
      @keyframes adv-spin {{ to {{ transform: rotate(360deg); }} }}
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
    {render_agent_nav("advertising", advertising_section=advertising_section, user=user)}
    <div class="shell">
      <div class="workspace">
        {body}
      </div>
    </div>
  </body>
</html>"""


def _goals_fields(goals: Optional[Goals]) -> str:
    """Just the goal inputs (no <form>) — embedded inside the Run form so the
    targets are set + saved as part of running, in one submit."""
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
      <div class="row">
        <div class="field"><label>Revenue target ($)</label><input type="number" step="0.01" name="revenue_target" value="{_esc(rev)}" placeholder="100000"></div>
        <div class="field"><label>ACoS target (%)</label><input type="number" step="0.1" name="acos_target" value="{_esc(acos)}" placeholder="30"></div>
        <div class="field"><label>TACoS target (%)</label><input type="number" step="0.1" name="tacos_target" value="{_esc(tacos)}" placeholder="25"></div>
        <div class="field"><label>Units target</label><input type="number" name="units_target" value="{_esc(units)}" placeholder="500"></div>
        <div class="field"><label>Period</label><select name="period">{periods}</select></div>
      </div>"""


# (priority, name, ext, where-path, why-one-liner, deep-link URL)
# Priority tiers drive the badge + sort order: required → recommended → optional.
_DOWNLOAD_GUIDE_ROWS = [
    ("required", "Ads performance reports", ".csv",
     "Ads Console → Reports → <strong>Search term</strong> + <strong>Advertised product</strong> templates",
     "Drives the burn list — negatives, harvests, ACoS/TACoS.", "https://advertising.amazon.com/reports"),
    ("required", "Business Report", ".csv",
     "Seller Central → Business Reports → <strong>By ASIN · Detail Page Sales &amp; Traffic By Child Item</strong>",
     "Per-ASIN sales + conversion → TACoS &amp; gap-to-goal.", "https://sellercentral.amazon.com/business-reports"),
    ("recommended", "Bulk-operations file", ".xlsx",
     "Ads Console → Sponsored ads → <strong>Bulk operations</strong> → Create spreadsheet",
     "Unlocks the ⬇ Bids &amp; ⬇ Additions apply sheets (carries the IDs).", "https://advertising.amazon.com/bulk-operations"),
    ("recommended", "Unit costs by ASIN", ".csv",
     "Your own <strong>ASIN, COGS</strong> file (or a margins sheet by product name)",
     "Profit-true break-even ACoS per SKU. Upload once.", ""),
    ("optional", "Targeting report", ".csv",
     "Ads Console → Reports → <strong>Targeting</strong> template (SP)",
     "Bid changes when you don't have the bulk file.", "https://advertising.amazon.com/reports"),
    ("optional", "Search Query Performance (SQP)", ".csv",
     "Seller Central → Brands → <strong>Brand Analytics</strong> → Search Query Performance",
     "Market-share context.", "https://sellercentral.amazon.com/brand-analytics"),
    ("optional", "DSP performance", ".csv",
     "Amazon DSP → Reports (name the file “DSP”)",
     "Campaign-level; lands as manual tasks.", "https://advertising.amazon.com/dsp"),
]

_DL_GUIDE_STYLES = """
<style>
  .dl-tip { margin: 8px 0 14px; font-size: 13px; color: rgba(43,54,68,0.72); line-height: 1.5; }
  .dl-list { list-style: none; margin: 0; padding: 0; display: grid; gap: 8px; }
  .dl-item { display: grid; grid-template-columns: 104px minmax(0,1fr) auto; gap: 12px; align-items: center;
    padding: 10px 14px; border: 1px solid var(--line); border-radius: 12px; background: var(--white); }
  .dl-item:hover { background: #fafbfc; }
  .dl-prio { justify-self: start; font-family: "Montserrat",sans-serif; font-size: 10px; font-weight: 700;
    text-transform: uppercase; letter-spacing: 0.03em; padding: 4px 9px; border-radius: 999px; white-space: nowrap; }
  .dl-prio.required { background: var(--dark-blue); color: #fff; }
  .dl-prio.recommended { background: var(--light-blue); color: var(--dark-blue); }
  .dl-prio.optional { background: #eef2f5; color: rgba(43,54,68,0.6); }
  .dl-name { font-weight: 600; font-size: 14px; }
  .dl-name .pill { font-size: 10px; margin-left: 2px; }
  .dl-why { font-size: 12.5px; color: rgba(43,54,68,0.7); margin-top: 1px; }
  .dl-where { font-size: 12px; color: rgba(43,54,68,0.55); margin-top: 3px; line-height: 1.45; }
  @media (max-width: 720px) {
    .dl-item { grid-template-columns: 1fr; gap: 4px; }
    .dl-open { justify-self: start; margin-top: 4px; }
  }
</style>
"""


def _download_guide() -> str:
    def _link(url):
        return (f'<a class="golink" href="{url}" target="_blank" rel="noopener">Open ↗</a>') if url else ""
    items = "".join(
        f'<li class="dl-item">'
        f'<span class="dl-prio {prio}">{prio}</span>'
        f'<div><div class="dl-name">{name} <span class="pill">{ext}</span></div>'
        f'<div class="dl-why">{why}</div>'
        f'<div class="dl-where">{where}</div></div>'
        f'<div class="dl-open">{_link(url)}</div>'
        f'</li>'
        for prio, name, ext, where, why, url in _DOWNLOAD_GUIDE_ROWS
    )
    return f"""
    <details class="guide">
      <summary>📥 What to download from Amazon</summary>
      {_DL_GUIDE_STYLES}
      <p class="dl-tip">Use the <strong>same trailing window</strong> for every file (e.g. last 30 days) and
      <strong>end it yesterday</strong> — today's data is incomplete. Drop them all in the box above; the tool detects each.</p>
      <ul class="dl-list">{items}</ul>
    </details>
    """


def _goals_display(raw: Optional[dict]) -> dict:
    """Turn a stored goals dict (cents / bps) into the display strings the goal
    inputs expect, so client-side pre-fill can set values directly."""
    g = raw or {}
    rev = g.get("revenue_target_cents")
    acos = g.get("acos_target_bps")
    tacos = g.get("tacos_target_bps")
    units = g.get("units_target")
    return {
        "revenue": "" if rev is None else f"{rev / 100:.2f}",
        "acos": "" if acos is None else f"{acos / 100:.1f}",
        "tacos": "" if tacos is None else f"{tacos / 100:.1f}",
        "units": "" if units is None else str(units),
        "period": g.get("period") or "monthly",
    }


def _client_select(clients: list[dict]) -> str:
    opts = ['<option value="">— No client (ad-hoc) —</option>']
    for c in clients or []:
        opts.append(f'<option value="{_esc(c.get("id"))}">{_esc(c.get("name") or "Untitled client")}</option>')
    return (
        '<div class="field" style="max-width:480px;">'
        '<label>Client</label>'
        '<select id="adv-client" name="client_id" onchange="advClientChange()">'
        + "".join(opts) +
        '</select>'
        '<span class="hint">Pick the client to run this audit for — its goals pre-fill below and the run is '
        'saved to its history. <a href="/admin/advertising/clients">+ Add a client</a>.</span>'
        '</div>'
    )


def _upload_form(latest: Optional[dict] = None, goals: Optional[Goals] = None,
                 clients: Optional[list[dict]] = None,
                 client_goals_map: Optional[dict] = None) -> str:
    ext_channels = "".join(f'<option value="{c}">{c.title()}</option>' for c in EXTERNAL_CHANNELS)
    # {client_id: display-ready goal strings} so picking a client fills the goal
    # inputs client-side with no extra round-trip.
    prefill = {cid: _goals_display(raw) for cid, raw in (client_goals_map or {}).items()}
    prefill_json = json.dumps(prefill)
    return f"""
    <form id="adv-run-form" class="grid" method="post" action="/admin/advertising/audit/run" enctype="multipart/form-data">
      {_client_select(clients or [])}
      <div class="dropzone">
        <label for="adv-files"><strong>Upload Amazon performance files</strong><br>
        <span class="empty">Bulk Operations, Search Term, Business Report, Search Query Performance (SQP), and DSP files. The tool identifies each file automatically.</span></label>
        <input id="adv-files" type="file" name="files" accept=".csv,.xlsx" multiple>
        <div id="adv-filelist" class="filelist"></div>
      </div>
      {_download_guide()}
      <div class="card" style="margin:0;background:#fafbfc;">
        <h2 style="font-size:15px;">Off-Amazon marketing spend <small>- added to blended TACoS</small></h2>
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
          <div class="field"><label>Amazon bulk operations file (XLSX)</label><input type="file" name="bulk_xlsx" accept=".xlsx"></div>
          <div class="field"><label>Search Term (CSV)</label><input type="file" name="search_term_csv" accept=".csv"></div>
          <div class="field"><label>Business Report (CSV)</label><input type="file" name="business_report_csv" accept=".csv"></div>
          <div class="field"><label>Search Query Performance (SQP) report (CSV)</label><input type="file" name="sqp_csv" accept=".csv"></div>
          <div class="field"><label>DSP (CSV)</label><input type="file" name="dsp_csv" accept=".csv"></div>
          <div class="field"><label>External costs (CSV)</label><input type="file" name="external_costs_csv" accept=".csv"></div>
          <div class="field"><label>Unit costs by ASIN (COGS CSV)</label><input type="file" name="cogs_csv" accept=".csv"></div>
        </div>
      </details>
      <div class="field" style="max-width:420px;">
        <label>Brand focus (optional)</label>
        <input id="adv-brand" type="text" name="brand" placeholder="e.g. Zantrex — leave blank for full account">
        <span class="hint">Type the client brand to scope the whole audit + plan to its campaigns &amp; ASINs.</span>
      </div>
      <div class="field" style="max-width:320px;"><label>Run label (optional)</label><input type="text" name="label" placeholder="{_week_label()}"></div>
      <div class="card" style="margin:6px 0 0;background:#fafbfc;">
        <h2 style="font-size:15px;">Goals <small>— targets the plan measures against (saved &amp; applied on run)</small></h2>
        {_goals_fields(goals)}
      </div>
      <div><button class="btn" type="submit">Generate burn-list workbook</button></div>
    </form>
    <script>
    window.__advClientGoals = {prefill_json};
    function advClientChange(){{
      var sel = document.getElementById('adv-client');
      var form = document.getElementById('adv-run-form');
      if (!sel || !form) return;
      var g = window.__advClientGoals[sel.value];
      if (!g) return;  // ad-hoc / unknown — leave whatever's typed
      if (form.revenue_target) form.revenue_target.value = g.revenue;
      if (form.acos_target) form.acos_target.value = g.acos;
      if (form.tacos_target) form.tacos_target.value = g.tacos;
      if (form.units_target) form.units_target.value = g.units;
      if (form.period) form.period.value = g.period;
    }}
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
      // Loading overlay while the audit runs (parsing + scoping + building can
      // take up to a minute on a large Bulk Operations file).
      var form = document.getElementById('adv-run-form');
      var overlay = document.getElementById('adv-loading');
      if (form && overlay) form.addEventListener('submit', function(){{
        overlay.classList.add('show');
        var steps = ['Parsing your reports…','Scoping to the brand…','Computing the burn list…','Building the workbook + apply sheet…'];
        var el = document.getElementById('adv-loading-step'), i = 0;
        if (el) {{ el.textContent = steps[0]; setInterval(function(){{ i=(i+1)%steps.length; el.textContent=steps[i]; }}, 4000); }}
      }});
    }})();
    </script>
    """


def _last_run_strip(latest: dict) -> str:
    """Compact one-line summary of the most recent run + its downloads."""
    s = latest.get("summary", {}) or {}
    brand = s.get("brand") or s.get("detected_brand") or "Full account"
    when = (latest.get("created_at") or "")[:16].replace("T", " ")
    rid = latest.get("id")
    recs = s.get("recommendation_count")
    meta = f"blended TACoS {fmt_pct(s.get('blended_tacos_bps'))} · ACoS {fmt_pct(s.get('acos_bps'))}"
    if recs:
        meta += f" · {recs} actions"
    dls = []
    if latest.get("has_plan"):
        dls.append(f'<a class="btn" href="/admin/advertising/audit/{_esc(rid)}/plan.xlsx">⬇ Growth plan</a>')
    if latest.get("has_bids"):
        dls.append(f'<a class="btn secondary" href="/admin/advertising/audit/{_esc(rid)}/bulk/bids.xlsx" title="Bid changes — uploads clean every time. Upload this first.">⬇ Bid changes</a>')
    if latest.get("has_additions"):
        dls.append(f'<a class="btn secondary" href="/admin/advertising/audit/{_esc(rid)}/bulk/additions.xlsx" title="New keywords + negatives. Upload after the bid file; some rows may already exist.">⬇ Additions</a>')
    if latest.get("has_apply"):  # legacy combined file
        dls.append(f'<a class="btn secondary" href="/admin/advertising/audit/{_esc(rid)}/bulk/combined.xlsx">⬇ Apply sheet</a>')
    dl_html = " ".join(dls) or '<span class="empty">No downloads for this run.</span>'
    ncc = s.get("new_campaign_count") or 0
    warn = ""
    if ncc:
        warn = (
            f'<div class="flash" style="background:#fdf6e9;border-color:#d9a441;margin:10px 0 0;">'
            f'⚠️ The <strong>Additions</strong> file creates <strong>{_esc(ncc)} new campaign'
            f'{"s" if ncc != 1 else ""}</strong> that go <strong>live on upload</strong> — review the '
            f'<strong>New Campaigns</strong> tab in the Growth plan first.</div>'
        )
    return (
        '<div class="card strip">'
        f'<div class="strip-info"><strong>{_esc(brand)}</strong> '
        f'<span class="empty">· {_esc(when)} · {meta}</span></div>'
        f'<div class="strip-actions">{dl_html}</div>'
        f'{warn}'
        "</div>"
    )


def _history_table(runs: list[dict]) -> str:
    if not runs:
        return '<p class="empty">No audits yet — run one above.</p>'
    rows = []
    for r in runs:
        s = r.get("summary", {}) or {}
        brand = s.get("brand") or s.get("detected_brand") or "Full account"
        when = (r.get("created_at") or "")[:16].replace("T", " ")
        rid = r.get("id")
        recs = s.get("recommendation_count")
        plan = (f'<a class="pill bulk" href="/admin/advertising/audit/{_esc(rid)}/plan.xlsx">⬇ Plan</a>'
                if r.get("has_plan") else "")
        bids = (f'<a class="pill" href="/admin/advertising/audit/{_esc(rid)}/bulk/bids.xlsx">⬇ Bids</a>'
                if r.get("has_bids") else "")
        adds = (f'<a class="pill" href="/admin/advertising/audit/{_esc(rid)}/bulk/additions.xlsx">⬇ Additions</a>'
                if r.get("has_additions") else "")
        apply = (f'<a class="pill" href="/admin/advertising/audit/{_esc(rid)}/bulk/combined.xlsx">⬇ Apply sheet</a>'
                 if r.get("has_apply") else "")
        downloads = " ".join(p for p in (plan, bids, adds, apply) if p).strip() or '<span class="empty">—</span>'
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
    clients: Optional[list[dict]] = None,
    client_goals_map: Optional[dict] = None,
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
      <div class="card"><h2>Run an audit <small>· drop your CSV / XLSX exports</small></h2>{_upload_form(latest, goals, clients, client_goals_map)}</div>
      <div class="card"><h2>History <small>· past runs &amp; downloads</small></h2>{_history_table(runs)}</div>
      {_how_to()}
      <div id="adv-loading" class="loading-overlay">
        <div class="loading-box">
          <div class="spinner"></div>
          <strong>Running your audit…</strong>
          <span id="adv-loading-step" class="empty">Parsing your reports…</span>
          <span class="empty" style="font-size:12px;">Large Bulk Operations files can take up to a minute — this page will refresh with your downloads when it's done.</span>
        </div>
      </div>
    """
    return _page("agent | Advertising Burn List", body, user=user)


def render_brand_mismatch_page(
    *,
    client_name: str,
    detected: str,
    known: list[str],
    token: str,
    user: Optional[dict] = None,
) -> str:
    """The block/confirm gate: the uploaded files' detected brand doesn't match
    anything this client has run before. Make the user confirm before running."""
    known_html = ", ".join(f"<strong>{_esc(k)}</strong>" for k in known) or "—"
    body = f"""
      <section class="page-header">
        <span class="eyebrow">Advertising</span>
        <h1 class="page-title">Hold on<span class="highlight">.</span></h1>
        <p class="page-copy">These files don't look like <strong>{_esc(client_name)}</strong>'s usual brand.
        Double-check you picked the right client before running.</p>
      </section>
      <div class="card" style="border:2px solid #d9a441;background:#fdf6e9;">
        <h2>⚠️ Possible client / file mismatch</h2>
        <p style="font-size:15px;line-height:1.6;">
          The uploaded Business Report looks like <strong>{_esc(detected or "an unknown brand")}</strong>,
          but <strong>{_esc(client_name)}</strong>'s past audits have been for {known_html}.
        </p>
        <p class="empty" style="font-size:14px;">If that's expected (a new brand for this client), run anyway.
        Otherwise cancel, pick the correct client, and re-upload.</p>
        <form method="post" action="/admin/advertising/audit/run/confirm" style="display:flex;gap:10px;flex-wrap:wrap;margin-top:8px;">
          <input type="hidden" name="confirm_token" value="{_esc(token)}">
          <button class="btn" type="submit">Run anyway for {_esc(client_name)}</button>
          <a class="btn secondary" href="/admin/advertising/audit">Cancel</a>
        </form>
      </div>
    """
    return _page("agent | Confirm client", body, user=user)


_FAQ = [
    ("What is this?",
     "A one-step burn-list generator. Upload a brand's Amazon ad + sales reports and it produces a downloadable "
     "<strong>growth-plan workbook</strong> (Exec Brief, Burn List, ASIN Scorecard, Campaign Actions, Negatives, "
     "Revenue Bridge, Data Requests) plus an <strong>upload-ready Amazon bulk apply-sheet</strong>."),
    ("Where do I get each report? (direct links)",
     'All Amazon, opens in a new tab: '
     '<a class="golink" href="https://advertising.amazon.com/reports" target="_blank" rel="noopener">Ads reports ↗</a> '
     '(Search term · Advertised product · <strong>Targeting</strong> · Placement) '
     '<a class="golink" href="https://advertising.amazon.com/bulk-operations" target="_blank" rel="noopener">Bulk operations ↗</a> '
     '(all campaign types — SP/SB/SD — with IDs &amp; current bids, for a complete apply sheet) '
     '<a class="golink" href="https://sellercentral.amazon.com/business-reports" target="_blank" rel="noopener">Business Reports ↗</a> '
     '<a class="golink" href="https://sellercentral.amazon.com/brand-analytics" target="_blank" rel="noopener">Brand Analytics ↗</a> '
     '<a class="golink" href="https://advertising.amazon.com/dsp" target="_blank" rel="noopener">DSP ↗</a>'),
    ("What do I upload? (the 3 core files)",
     "1) <strong>Ads performance reports</strong> (Search term + Advertised product, from Ads Console → Reports). "
     "2) <strong>Business Report</strong> — By ASIN → Detail Page Sales &amp; Traffic By Child Item (Seller Central). "
     "3) <em>(optional but recommended)</em> a <strong>COGS</strong> file for true break-even ACoS. "
     "Use the “📥 What to download” links above to jump straight to each."),
    ("How do I run it?",
     "Drop all the files in the box (any order — it auto-detects each), type the <strong>brand</strong> to focus on, "
     "set your <strong>Goals</strong>, and hit <strong>Generate burn-list workbook</strong>. The run appears in "
     "History with ⬇ Plan and ⬇ Apply-sheet downloads."),
    ("What are the two apply files — Bid changes vs Additions?",
     "The run produces <strong>two</strong> Amazon bulk files so a problem in one never blocks the other. "
     "<strong>⬇ Bid changes</strong> = raise/lower bids on existing keywords &amp; targets; it references existing IDs "
     "so it <strong>uploads cleanly every time — upload this one first</strong>. <strong>⬇ Additions</strong> = new "
     "keywords + negatives to create; upload it separately. Both go to Ads Console → Bulk operations → Upload, no editing."),
    ("My Additions file says “already exists!” — why?",
     "Amazon rejects creating a keyword/target that already exists, and it fails the whole Additions file on the first one. "
     "We drop everything we can see in your uploaded <strong>Bulk operations</strong> file — but that file is a snapshot, "
     "so anything added to the account since you exported it is invisible to us. <strong>Fix: re-download a FRESH Bulk "
     "operations export right before you run</strong>, then re-run. (Your <strong>Bid changes</strong> file is unaffected.)"),
    ("Same date window for every report",
     "Pick one trailing window (e.g. last 30 days) and <strong>end it yesterday, not today</strong> — today's data is "
     "incomplete and skews ACoS."),
    ("Do I need COGS with ASINs?",
     "No — upload a margins sheet keyed by product name and the tool auto-matches it to ASINs. Check the "
     "<strong>COGS Mapping</strong> tab in the workbook to verify, and override any miss with a 2-column "
     "<strong>ASIN, COGS</strong> file."),
]


def _how_to() -> str:
    items = "".join(
        f'<details class="faq-item"><summary>{q}</summary><p>{a}</p></details>'
        for q, a in _FAQ
    )
    return f'<div class="card"><h2>How to use <small>· quick FAQ for account managers</small></h2>{items}</div>'
