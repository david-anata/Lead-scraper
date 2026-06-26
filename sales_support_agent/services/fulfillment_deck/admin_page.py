"""Admin page for Fulfillment > Sales Deck (rate sheet generator + history).

Same admin shell vocabulary as the Brand Analysis page (nav + workspace card),
so it reads as a sibling tool.
"""

from __future__ import annotations

import html
from datetime import datetime
from typing import Optional

from sales_support_agent.services.admin_nav import (
    render_agent_favicon_links,
    render_agent_nav,
    render_agent_nav_styles,
)
from sales_support_agent.services.fulfillment_deck.quote import BASELINE_RATES
from sales_support_agent.services.fulfillment_deck.schema import (
    ANATA_HQ_ADDRESS,
    ANATA_HQ_ZIP,
    RATE_SOURCE_WMS,
)


def _esc(value: object) -> str:
    return html.escape(str(value or ""))


_STYLES = """
      :root {
        --dark-blue: #2B3644;
        --light-blue: #85BBDA;
        --light-brown: #F9F7F3;
        --white: #FFFFFF;
        --border: rgba(43, 54, 68, 0.12);
        --shadow: rgba(43, 54, 68, 0.10);
      }
      * { box-sizing: border-box; }
      body { margin: 0; background: var(--light-brown); color: var(--dark-blue);
        font-family: "Inter", "Segoe UI", sans-serif; }
      a { color: var(--dark-blue); }
      __NAV__
      .shell { max-width: 1180px; margin: 0 auto; padding: 28px 18px 64px; }
      .workspace { background: var(--white); border: 1px solid var(--border); border-radius: 20px;
        box-shadow: 0 18px 40px var(--shadow); padding: 26px 28px 30px; }
      h1 { font-family: "Montserrat", sans-serif; font-weight: 800; font-size: 26px; margin: 0 0 4px; }
      h2 { font-family: "Montserrat", sans-serif; font-weight: 800; font-size: 17px; margin: 26px 0 8px; }
      .eyebrow { font-family: "Montserrat", sans-serif; font-weight: 700; font-size: 11px;
        letter-spacing: 0.08em; text-transform: uppercase; color: rgba(43,54,68,0.55); margin: 0 0 4px; }
      .intro { font-size: 14px; color: rgba(43,54,68,0.75); margin: 0 0 18px; max-width: 760px; }
      .flash { background: rgba(133,187,218,0.18); border: 1px solid rgba(133,187,218,0.5);
        border-radius: 12px; padding: 12px 16px; margin-bottom: 14px; font-size: 13.5px; }
      .flash--warn { background: #fff4d9; border-color: #d2a94b; }
      .field { display: grid; gap: 5px; margin: 12px 0; }
      .field label { font-family: "Montserrat", sans-serif; font-weight: 700; font-size: 12px; }
      .field .hint { font-size: 12px; color: rgba(43,54,68,0.55); font-weight: 400; }
      .field input[type=text], .field input[type=url] { min-height: 40px; padding: 0 12px;
        border-radius: 10px; border: 1px solid var(--border); font-size: 14px; }
      .field textarea { min-height: 150px; padding: 10px 12px; border-radius: 10px;
        border: 1px solid var(--border); font-size: 14px; font-family: inherit; resize: vertical; }
      .drop { border: 2px dashed rgba(133,187,218,0.7); border-radius: 16px; padding: 22px;
        text-align: center; background: var(--light-brown); }
      .grid2 { display: grid; grid-template-columns: 2fr 1fr; gap: 14px; }
      .btn { display: inline-flex; align-items: center; gap: 8px; min-height: 44px; padding: 0 22px;
        border-radius: 999px; background: var(--dark-blue); color: #fff; font-family: "Montserrat", sans-serif;
        font-weight: 700; font-size: 13px; border: none; cursor: pointer; text-decoration: none; }
      .btn--ghost { background: #fff; color: var(--dark-blue); border: 1px solid var(--border); min-height: 34px; padding: 0 14px; font-size: 12px; }
      .btn--danger { background: #fff; color: #8b4c42; border: 1px solid rgba(139,76,66,0.4); min-height: 34px; padding: 0 14px; font-size: 12px; }
      table { width: 100%; border-collapse: collapse; font-size: 13.5px; margin: 6px 0 8px; }
      th, td { text-align: left; padding: 9px 11px; border-bottom: 1px solid var(--border); vertical-align: middle; }
      thead th { background: rgba(133,187,218,0.20); font-family: "Montserrat", sans-serif; font-size: 11px;
        letter-spacing: 0.04em; text-transform: uppercase; }
      .pill { display: inline-block; padding: 3px 10px; border-radius: 999px; font-size: 11px;
        font-weight: 700; font-family: "Montserrat", sans-serif; letter-spacing: 0.03em; }
      .pill--live { background: rgba(46,125,91,0.16); color: #2e7d5b; }
      .pill--sample { background: #fff4d9; color: #7a5b14; border: 1px solid #d2a94b; }
      .pill--failed { background: rgba(139,76,66,0.16); color: #8b4c42; }
      .pill--draft { background: rgba(43,54,68,0.10); color: rgba(43,54,68,0.65); }
      .pill--running { background: rgba(14,165,233,0.12); color: #0369a1; border: 1px solid rgba(14,165,233,0.3); }
      .pill--estimated { background: #fff4d9; color: #7a5b14; border: 1px solid #d2a94b; }
      .row-actions { display: flex; gap: 6px; flex-wrap: wrap; }
      .muted { color: rgba(43,54,68,0.55); font-size: 12px; }
      .empty { color: rgba(43,54,68,0.55); font-size: 13.5px; padding: 18px 0; }
      .edit-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 0 24px; }
      @media (max-width: 760px) { .grid2 { grid-template-columns: 1fr; } .edit-grid { grid-template-columns: 1fr; } }
      /* On narrow screens keep only Prospect, Stage, Margin, Actions */
      @media (max-width: 640px) {
        table th:nth-child(3), table td:nth-child(3),
        table th:nth-child(4), table td:nth-child(4),
        table th:nth-child(5), table td:nth-child(5),
        table th:nth-child(7), table td:nth-child(7) { display: none; }
        .row-actions { flex-direction: column; }
      }
      /* Pipeline summary bar */
      .pipeline-stats { display: flex; gap: 12px; margin: 0 0 16px; flex-wrap: wrap; }
      .pipeline-stat { background: #fff; border: 1px solid var(--border); border-radius: 12px;
        padding: 12px 16px; flex: 1; min-width: 130px; }
      .pipeline-stat__val { font-family: "Montserrat", sans-serif; font-weight: 800;
        font-size: 20px; color: var(--dark-blue); line-height: 1.1; }
      .pipeline-stat__label { font-size: 10px; font-weight: 700; font-family: "Montserrat", sans-serif;
        letter-spacing: 0.07em; text-transform: uppercase; color: rgba(43,54,68,0.5); margin-top: 3px; }
      .pipeline-stat__sub { font-size: 11px; color: rgba(43,54,68,0.5); margin-top: 2px; }
      .pipeline-stat--won .pipeline-stat__val { color: #15803d; }
      /* Pipeline table */
      .prospect-row { cursor: pointer; }
      .prospect-row:hover td { background: rgba(133,187,218,0.07); }
      .prospect-row td:first-child { border-left: 3px solid transparent; padding-left: 9px; transition: border-color 0.15s; }
      .prospect-row[data-stage="intake"] td:first-child { border-left-color: #94a3b8; }
      .prospect-row[data-stage="pending_fulfillment"] td:first-child { border-left-color: #38bdf8; }
      .prospect-row[data-stage="costs_received"] td:first-child { border-left-color: #a78bfa; }
      .prospect-row[data-stage="published"] td:first-child { border-left-color: #fbbf24; }
      .prospect-row[data-stage="won"] td:first-child { border-left-color: #4ade80; }
      .prospect-row[data-stage="lost"] td:first-child { border-left-color: #e2e8f0; }
      .row-chevron { display: inline-block; color: rgba(43,54,68,0.35); font-size: 13px;
        margin-right: 5px; transition: transform 0.15s; line-height: 1; vertical-align: middle; }
      .stage-select-wrap { position: relative; display: inline-block; }
      .stage-select-wrap::after {
        content: '▾'; position: absolute; right: 7px; top: 50%;
        transform: translateY(-50%); pointer-events: none;
        font-size: 9px; color: rgba(43,54,68,0.45); line-height: 1;
      }
      .stage-select {
        appearance: none; -webkit-appearance: none; border: none; border-radius: 999px;
        padding: 3px 20px 3px 10px; font-size: 11px; font-weight: 700;
        font-family: "Montserrat", sans-serif; letter-spacing: 0.03em; cursor: pointer;
      }
      .stage--intake        { background: #e2e8f0; color: #475569; }
      .stage--pending_fulfillment { background: #e0f2fe; color: #0369a1; }
      .stage--costs_received { background: #ede9fe; color: #6d28d9; }
      .stage--published     { background: #fef3c7; color: #b45309; }
      .stage--won           { background: #dcfce7; color: #15803d; }
      .stage--lost          { background: #f1f5f9; color: #94a3b8; }
      .expand-row td { padding: 0; border-bottom: 2px solid var(--border); }
      .expand-panel {
        padding: 18px 22px 22px; background: rgba(133,187,218,0.06);
        display: grid; grid-template-columns: 1fr 1fr; gap: 18px;
      }
      .expand-panel h3 { font-family: "Montserrat", sans-serif; font-size: 12px;
        font-weight: 700; letter-spacing: 0.06em; text-transform: uppercase;
        color: rgba(43,54,68,0.55); margin: 0 0 10px; }
      .cost-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 8px 14px; }
      .cost-grid label { font-size: 12px; font-weight: 600; display: block; margin-bottom: 2px; }
      .cost-grid input { width: 100%; min-height: 34px; padding: 0 10px;
        border-radius: 8px; border: 1px solid var(--border); font-size: 13px; }
      .margin-card { background: #fff; border: 1px solid var(--border); border-radius: 12px;
        padding: 12px 16px; margin-top: 10px; font-size: 13px; }
      .margin-card .big { font-size: 22px; font-weight: 800;
        font-family: "Montserrat", sans-serif; }
      .margin-card .big--pos { color: #15803d; }
      .margin-card .big--neg { color: #b91c1c; }
      .margin-line { display: flex; justify-content: space-between;
        padding: 3px 0; border-bottom: 1px solid rgba(43,54,68,0.07); font-size: 12px; }
      .margin-line:last-child { border: none; }
      .expand-notes { width: 100%; min-height: 70px; padding: 8px 10px;
        border-radius: 8px; border: 1px solid var(--border); font-size: 13px;
        font-family: inherit; resize: vertical; }
      @media (max-width: 900px) {
        .expand-panel { grid-template-columns: 1fr; }
        .grid2 { grid-template-columns: 1fr; }
      }
"""


def _fmt_duration(seconds: int) -> str:
    if seconds <= 0:
        return "—"
    if seconds < 60:
        return f"{seconds}s"
    minutes, secs = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m {secs:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes:02d}m"


_STAGE_LABELS = {
    "intake": "Intake",
    "pending_fulfillment": "Sent to Fulfillment",
    "costs_received": "Costs Received",
    "published": "Published",
    "won": "Won",
    "lost": "Lost",
}

_STAGE_OPTIONS = "".join(
    f'<option value="{k}">{v}</option>' for k, v in _STAGE_LABELS.items()
)


def _stage_select(run_id: int, current: str) -> str:
    options = "".join(
        f'<option value="{k}" {"selected" if k == current else ""}>{v}</option>'
        for k, v in _STAGE_LABELS.items()
    )
    return (
        f'<div class="stage-select-wrap">'
        f'<select class="stage-select stage--{_esc(current)}" '
        f'onclick="event.stopPropagation()" '
        f'onchange="pipelineStage(this,{run_id})">{options}</select>'
        f'</div>'
    )


def _fmt_usd(value) -> str:
    if value is None:
        return "—"
    try:
        v = float(value)
        return f"−${abs(v):,.0f}" if v < 0 else f"${v:,.0f}"
    except (TypeError, ValueError):
        return "—"


def _build_brief(run: dict) -> str:
    """Plain-text fulfillment brief for clipboard copy."""
    profile = run.get("prospect_profile") or {}
    name = run.get("prospect") or run.get("design_title") or f"Run {run.get('id')}"
    origin = run.get("origin_zip") or "—"
    vol = run.get("monthly_order_volume")
    vol_str = f"{vol:,} orders/mo" if vol else "—"
    products = profile.get("products") or []
    prod_lines = []
    for p in products[:6]:
        pname = p.get("name") or "Product"
        l_, w_, h_, wt = (
            p.get("length_in"), p.get("width_in"),
            p.get("height_in"), p.get("weight_lb"),
        )
        units = p.get("monthly_units")
        dims = f"{l_}×{w_}×{h_}in" if None not in (l_, w_, h_) else "dims unknown"
        weight = f"{wt}lb" if wt else ""
        u_str = f" × {units:,} units/mo" if units else ""
        prod_lines.append(f"  {pname} ({dims}{', ' + weight if weight else ''}{u_str})")
    products_str = "\n".join(prod_lines) if prod_lines else "  (no products)"
    fragile = any(p.get("fragile") for p in products)
    cat = (products[0].get("product_category") or "unknown") if products else "unknown"
    return (
        f"Prospect: {name} | Origin ZIP: {origin} | Volume: {vol_str}\n"
        f"Products:\n{products_str}\n"
        f"Category: {cat} | Fragile: {'yes' if fragile else 'no'}"
    )


def _expand_panel(run: dict) -> str:
    """Collapsible expand panel for cost entry, margin, notes, brief."""
    run_id = int(run.get("id") or 0)
    costs = run.get("fulfillment_actual_costs") or {}
    notes = _esc(run.get("pipeline_notes") or "")
    pitched = run.get("pitched_monthly")

    def _cv(key: str) -> str:
        v = costs.get(key)
        return f"{v:g}" if v is not None else ""

    # Pre-compute margin if costs are present
    margin_html = ""
    if costs and pitched and any(v for v in costs.values() if v):
        try:
            from sales_support_agent.services.fulfillment_deck.quote import compute_margin
            from sales_support_agent.services.fulfillment_deck.schema import ProspectProfile
            profile_obj = ProspectProfile.from_dict(run.get("prospect_profile") or {})
            mg = compute_margin(float(pitched), costs, profile_obj)
            sign = "pos" if mg["monthly_margin"] >= 0 else "neg"
            _rec_pp = float(costs.get("receiving_per_pallet") or 0)
            _rec_pallets = int(mg.get("pallets_mo") or 0)
            _rec_line = (
                f'<div class="margin-line" style="opacity:0.65"><span>Receiving one-time (~{_rec_pallets} pallets)</span>'
                f'<span>−{_fmt_usd(_rec_pp * _rec_pallets)}</span></div>'
                if _rec_pp and _rec_pallets else ""
            )
            margin_html = f"""
            <div class="margin-card" id="margin-{run_id}">
              <div class="big big--{sign}">{_fmt_usd(mg['monthly_margin'])}<span style="font-size:14px;font-weight:400">/mo ({mg['margin_pct']}%)</span></div>
              <div class="margin-line"><span>Pitched monthly</span><span>{_fmt_usd(pitched)}</span></div>
              <div class="margin-line"><span>Pick &amp; pack actual</span><span>−{_fmt_usd(mg['actual_pick_pack'])}</span></div>
              <div class="margin-line"><span>Storage actual</span><span>−{_fmt_usd(mg['actual_storage'])}</span></div>
              <div class="margin-line"><span>Tech fee actual</span><span>−{_fmt_usd(mg['actual_tech_fee'])}</span></div>
              {_rec_line}
              <div class="margin-line" style="font-weight:700"><span>Annual margin</span><span>{_fmt_usd(mg['annual_margin'])}</span></div>
            </div>"""
        except Exception:
            margin_html = f'<div class="margin-card" id="margin-{run_id}"></div>'
    else:
        margin_html = f'<div class="margin-card" id="margin-{run_id}" style="color:rgba(43,54,68,0.45);font-size:12px">Enter actual costs above to see margin.</div>'

    brief_attr = html.escape(_build_brief(run), quote=True)
    _view_path = _esc(str(run.get("view_path") or ""))
    _hs_quote_url = _esc(str(run.get("hubspot_quote_url") or ""))
    _hs_deal_url = _esc(str(run.get("hubspot_deal_url") or ""))
    _quick_links = ""
    if _view_path or _hs_deal_url:
        _btns = []
        if _view_path:
            _btns.append(
                f'<button class="btn btn--ghost" type="button" style="font-size:12px" '
                f"onclick=\"navigator.clipboard.writeText(window.location.origin+'{_view_path}');"
                f"this.textContent='Copied!';setTimeout(()=>this.textContent='Copy link',1800)\">Copy link</button>"
                f'<a class="btn btn--ghost" href="/admin/fulfillment/sales/runs/{run_id}/review" '
                f'target="_blank" rel="noreferrer" onclick="event.stopPropagation()" style="font-size:12px">Edit rate sheet →</a>'
            )
        if _hs_deal_url:
            _btns.append(
                f'<a class="btn btn--ghost" href="{_hs_deal_url}" target="_blank" rel="noreferrer" '
                f'onclick="event.stopPropagation()" style="font-size:12px;color:#FF7A59">HubSpot Deal</a>'
            )
        if _hs_quote_url:
            _btns.append(
                f'<a class="btn btn--ghost" href="{_hs_quote_url}" target="_blank" rel="noreferrer" '
                f'onclick="event.stopPropagation()" style="font-size:12px;color:#FF7A59">HubSpot Quote ✍</a>'
            )
        _quick_links = f'<div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:16px">{"".join(_btns)}</div>'

    return f"""
    <div class="expand-panel">
      <div>
        {_quick_links}
        <h3>Fulfillment Team Costs</h3>
        <div class="cost-grid">
          <div><label>Pick &amp; pack ($/order)</label>
            <input type="number" step="0.01" min="0" placeholder="{BASELINE_RATES['dtc_base_per_order']:.2f}"
              id="pp-{run_id}" value="{_cv('pick_pack_per_order')}"></div>
          <div><label>Storage ($/pallet/mo)</label>
            <input type="number" step="0.01" min="0" placeholder="{BASELINE_RATES['storage_short_per_pallet_mo']:.2f}"
              id="st-{run_id}" value="{_cv('storage_per_pallet_mo')}"></div>
          <div><label>Receiving ($/pallet) <span style="font-weight:400;font-size:11px;opacity:.6">— one-time</span></label>
            <input type="number" step="0.01" min="0" placeholder="{BASELINE_RATES['receiving_per_pallet']:.2f}"
              id="rc-{run_id}" value="{_cv('receiving_per_pallet')}"></div>
          <div><label>Tech fee ($/mo)</label>
            <input type="number" step="0.01" min="0" placeholder="{BASELINE_RATES['monthly_tech_fee']:.2f}"
              id="tf-{run_id}" value="{_cv('monthly_tech_fee')}"></div>
        </div>
        <button class="btn btn--ghost" style="margin-top:10px" type="button"
          onclick="pipelineCosts(this,{run_id})">Save costs</button>
        {margin_html}
      </div>
      <div>
        <h3>Internal Notes</h3>
        <textarea class="expand-notes" placeholder="Call notes, deal context, next steps…"
          oninput="pipelineNotesDebounce(this,{run_id})">{notes}</textarea>
        <div style="display:flex;gap:6px;margin-top:10px">
          <button class="btn" type="button"
            style="background:#15803d;min-height:34px;font-size:12px;padding:0 14px"
            onclick="quickStage(this,'won',{run_id})">Mark as Won ✓</button>
          <button class="btn btn--ghost" type="button"
            style="color:#94a3b8;border-color:#e2e8f0;font-size:12px"
            onclick="quickStage(this,'lost',{run_id})">Archive / Lost</button>
        </div>
        <h3 style="margin-top:14px">Fulfillment Brief</h3>
        <p class="muted" style="margin:0 0 6px">Copy and share with the warehouse team for costing.</p>
        <div style="display:flex;gap:8px;flex-wrap:wrap">
          <button class="btn btn--ghost" type="button"
            data-brief="{brief_attr}"
            onclick="navigator.clipboard.writeText(this.dataset.brief);this.textContent='Copied!';setTimeout(()=>this.textContent='Copy brief',2000)">Copy brief</button>
          <button class="btn btn--ghost" type="button" style="font-size:12px"
            onclick="sendBriefEmail(this,{run_id})">Send to warehouse →</button>
        </div>
      </div>
    </div>"""


def _pipeline_stats(runs: list[dict]) -> str:
    """Four-stat summary bar above the pipeline table."""
    active = [r for r in runs if r.get("pipeline_stage") not in ("won", "lost")]
    won = [r for r in runs if r.get("pipeline_stage") == "won"]

    pitched_active = sum(float(r.get("pitched_monthly") or 0) for r in active)
    pitched_won = sum(float(r.get("pitched_monthly") or 0) for r in won)

    margin_active = 0.0
    margin_runs = 0
    for r in active:
        costs = r.get("fulfillment_actual_costs") or {}
        pitched = r.get("pitched_monthly")
        if costs and pitched and any(v for v in costs.values() if v):
            try:
                from sales_support_agent.services.fulfillment_deck.quote import compute_margin
                from sales_support_agent.services.fulfillment_deck.schema import ProspectProfile
                mg = compute_margin(float(pitched), costs, ProspectProfile.from_dict(r.get("prospect_profile") or {}))
                margin_active += mg["monthly_margin"]
                margin_runs += 1
            except Exception:
                pass

    def _stat(label: str, val: str, sub: str = "", extra_cls: str = "") -> str:
        sub_html = f'<div class="pipeline-stat__sub">{_esc(sub)}</div>' if sub else ""
        return (
            f'<div class="pipeline-stat {extra_cls}">'
            f'<div class="pipeline-stat__val">{val}</div>'
            f'<div class="pipeline-stat__label">{_esc(label)}</div>'
            f'{sub_html}</div>'
        )

    active_str = str(len(active))
    pipeline_str = f"${pitched_active:,.0f}<span style='font-size:13px;font-weight:400'>/mo</span>" if pitched_active else "—"
    margin_str = f"${margin_active:,.0f}<span style='font-size:13px;font-weight:400'>/mo</span>" if margin_active else "—"
    margin_sub = f"{margin_runs} of {len(active)} with costs" if active else ""
    won_pct = round(len(won) / len(runs) * 100) if runs else 0
    won_str = str(len(won)) if won else "—"
    won_sub_parts = []
    if won and pitched_won:
        won_sub_parts.append(f"${pitched_won:,.0f}/mo booked")
    if runs:
        won_sub_parts.append(f"{won_pct}% conversion")
    won_sub = " · ".join(won_sub_parts) if won_sub_parts else "no wins yet"

    return (
        f'<div class="pipeline-stats">'
        f'{_stat("Active prospects", active_str)}'
        f'{_stat("Pitched pipeline", pipeline_str, f"${pitched_active * 12:,.0f}/yr potential" if pitched_active else "")}'
        f'<div class="pipeline-stat" id="stat-margin"><div class="pipeline-stat__val">{margin_str}</div>'
        f'<div class="pipeline-stat__label">Monthly margin</div>'
        f'<div class="pipeline-stat__sub">{_esc(margin_sub)}</div></div>'
        f'{_stat("Won", won_str, won_sub, "pipeline-stat--won" if won else "")}'
        f'</div>'
    )


def _history_rows(runs: list[dict], engagement: dict[int, dict]) -> str:
    rows = []
    for run in runs:
        run_id = int(run.get("id") or 0)
        started_raw = str(run.get("started_at") or "")[:10]
        published_raw = str(run.get("published_at") or "")[:10]
        try:
            _date_src = published_raw if published_raw else started_raw
            _date_lbl = "sent" if published_raw else "created"
            started = f"{_date_lbl} {datetime.strptime(_date_src, '%Y-%m-%d').strftime('%b %-d')}"
        except ValueError:
            started = started_raw
        prospect = _esc(run.get("prospect") or run.get("design_title") or f"Run {run_id}")
        status = str(run.get("status") or "")
        view_path = str(run.get("view_path") or "")
        hs_quote_url = str(run.get("hubspot_quote_url") or "")
        published = bool(run.get("published")) and status == "completed"
        review_path = f"/admin/fulfillment/sales/runs/{run_id}/review"
        stage = str(run.get("pipeline_stage") or "intake")
        vol = run.get("monthly_order_volume")
        pitched = run.get("pitched_monthly")
        costs = run.get("fulfillment_actual_costs") or {}

        # Stale indicator (actionable follow-up cue)
        _stale_badge = ""
        try:
            _today = datetime.utcnow().date()
            _age_src = published_raw if published_raw else started_raw
            _age_days = (_today - datetime.strptime(_age_src, "%Y-%m-%d").date()).days if _age_src else 0
            _ext_views = int((engagement.get(run_id) or {}).get("external_sessions") or 0)
            if stage == "intake" and _age_days > 7 and published:
                # Sheet was published but stage wasn't advanced — one-click advance to pending_fulfillment
                _stale_badge = (
                    f'<div style="font-size:11px;color:#b45309;margin-top:3px;font-weight:500">'
                    f'⚠ Sent {_age_days}d ago &nbsp;'
                    f'<button type="button" onclick="event.stopPropagation();quickStage(this,\'pending_fulfillment\',{run_id})" '
                    f'style="font-size:10px;padding:1px 7px;border-radius:999px;border:1px solid #b45309;'
                    f'background:transparent;color:#b45309;cursor:pointer;font-weight:600">→ Mark as Sent</button>'
                    f'</div>'
                )
            elif stage == "published" and _ext_views == 0 and _age_days > 5:
                _stale_badge = (
                    f'<div style="font-size:11px;color:#b45309;margin-top:3px;font-weight:500">'
                    f'⚠ Unopened after {_age_days}d — follow up</div>'
                )
        except Exception:
            pass

        # Rates source pill (small, inside prospect cell)
        if status == "running":
            source_pill = '<span class="pill pill--running" style="font-size:10px">Generating…</span>'
        elif status == "failed":
            source_pill = '<span class="pill pill--failed" style="font-size:10px">Failed</span>'
        elif status == "draft":
            source_pill = '<span class="pill pill--draft" style="font-size:10px">Draft</span>'
        elif str(run.get("rates_source")) == RATE_SOURCE_WMS:
            source_pill = '<span class="pill pill--live" style="font-size:10px">Live</span>'
        else:
            source_pill = '<span class="pill pill--sample" style="font-size:10px">Sample</span>'

        # Engagement
        stats = engagement.get(run_id) or {}
        ext = int(stats.get("external_sessions") or 0)
        last_viewed = stats.get("last_viewed_at") or ""
        if ext and last_viewed:
            try:
                lv_date = datetime.fromisoformat(last_viewed[:10]).date()
                today = datetime.utcnow().date()
                days = (today - lv_date).days
                ago = "today" if days == 0 else ("yesterday" if days == 1 else f"{days}d ago")
                views_str = (
                    f'<span title="{ext} prospect session{"s" if ext != 1 else ""}, last {ago}">'
                    f'{ext}v <span class="muted" style="font-size:11px">{ago}</span></span>'
                )
            except Exception:
                views_str = f"{ext}v"
        elif ext:
            views_str = f"{ext}v"
        else:
            views_str = "—"

        # Margin + actual cost columns (single compute_margin call per row)
        actual_cell = '<span class="muted">—</span>'
        margin_cell = '<span class="muted">—</span>'
        _raw_margin: float = 0.0
        if costs and any(v for v in costs.values() if v):
            try:
                from sales_support_agent.services.fulfillment_deck.quote import compute_margin
                from sales_support_agent.services.fulfillment_deck.schema import ProspectProfile
                profile_obj = ProspectProfile.from_dict(run.get("prospect_profile") or {})
                mg = compute_margin(float(pitched or 0), costs, profile_obj)
                actual_cell = _fmt_usd(mg["actual_monthly"])
                if pitched:
                    _raw_margin = float(mg["monthly_margin"])
                    sign_color = "#15803d" if _raw_margin >= 0 else "#b91c1c"
                    margin_cell = (
                        f'<span style="color:{sign_color};font-weight:700">'
                        f'{_fmt_usd(_raw_margin)}</span>'
                        f'<div class="muted">{mg["margin_pct"]}%</div>'
                    )
            except Exception:
                actual_cell = "—"

        actions = []
        if status == "running":
            pass  # just Delete below — page auto-refreshes
        elif status == "draft":
            actions.append(f'<a class="btn btn--ghost" href="{review_path}" target="_blank" rel="noreferrer" onclick="event.stopPropagation()">Review</a>')
        elif view_path and published:
            actions.append(f'<a class="btn btn--ghost" href="{_esc(view_path)}?viewer=internal" target="_blank" rel="noreferrer" onclick="event.stopPropagation()">Open</a>')
            actions.append(
                f'<button class="btn btn--ghost" type="button" onclick="event.stopPropagation();'
                f"navigator.clipboard.writeText(window.location.origin + '{_esc(view_path)}');this.textContent='Copied';\">Share</button>"
            )
            if hs_quote_url:
                actions.append(f'<a class="btn btn--ghost" href="{_esc(hs_quote_url)}" target="_blank" rel="noreferrer" onclick="event.stopPropagation()" title="Open e-signature quote in HubSpot" style="color:#FF7A59;border-color:rgba(255,122,89,0.4)">Open Quote ✍</a>')
            else:
                actions.append(
                    f'<form method="post" action="/admin/fulfillment/sales/runs/{run_id}/quote" '
                    f'style="display:inline" onclick="event.stopPropagation()">'
                    f'<button class="btn btn--ghost" type="submit" title="Create HubSpot e-signature quote">Create Quote ✍</button></form>'
                )
            actions.append(f'<a class="btn btn--ghost" href="{review_path}" target="_blank" rel="noreferrer" onclick="event.stopPropagation()">Edit</a>')
        actions.append(
            f'<form method="post" action="/admin/fulfillment/sales/runs/{run_id}/delete" '
            f'style="display:inline" onclick="event.stopPropagation()" '
            f"onsubmit=\"return confirm('Delete this rate sheet? The public link will stop working.');\">"
            f'<button class="btn btn--danger" type="submit">Delete</button></form>'
        )

        vol_str = f"{vol:,}" if vol else "—"
        notes_dot = (
            '<span title="Has internal notes" style="margin-left:4px;font-size:10px;'
            'opacity:0.5;vertical-align:middle">●</span>'
            if str(run.get("pipeline_notes") or "").strip() else ""
        )
        row_idx = len(rows)
        rows.append(
            f'<tr class="prospect-row" data-order="{row_idx}" data-stage="{_esc(stage)}" data-run="{run_id}" data-expand="expand-{run_id}" onclick="toggleExpand(event,\'expand-{run_id}\')">'
            f"<td><span class='row-chevron'>›</span><strong>{prospect}</strong>{notes_dot}{_stale_badge} {source_pill}"
            f"<div class='muted'>{started}</div></td>"
            f"<td>{_stage_select(run_id, stage)}</td>"
            f"<td>{vol_str}</td>"
            f"<td>{_fmt_usd(pitched)}</td>"
            f"<td>{actual_cell}</td>"
            f'<td data-margin="{_raw_margin}">{margin_cell}</td>'
            f"<td>{views_str}</td>"
            f"<td><div class='row-actions'>{''.join(actions)}</div></td></tr>"
            f'<tr class="expand-row" id="expand-{run_id}" style="display:none">'
            f'<td colspan="8">{_expand_panel(run)}</td></tr>'
        )
    return "".join(rows)


def render_fulfillment_sales_page(
    runs: list[dict],
    engagement: dict[int, dict],
    *,
    user: Optional[dict] = None,
    flash: str = "",
    flash_kind: str = "",
) -> str:
    flash_html = (
        f'<div class="flash{" flash--warn" if flash_kind == "warn" else ""}">{_esc(flash)}</div>'
        if flash
        else ""
    )
    has_running = any(r.get("status") == "running" for r in runs)
    # Embed per-run margin data for live stats-bar update
    import json as _json
    _margin_seed: dict = {}
    for _r in runs:
        _costs = _r.get("fulfillment_actual_costs") or {}
        _pitched = _r.get("pitched_monthly")
        _stage = _r.get("pipeline_stage") or "intake"
        if _costs and _pitched and any(v for v in _costs.values() if v):
            try:
                from sales_support_agent.services.fulfillment_deck.quote import compute_margin
                from sales_support_agent.services.fulfillment_deck.schema import ProspectProfile
                _mg = compute_margin(float(_pitched), _costs, ProspectProfile.from_dict(_r.get("prospect_profile") or {}))
                _margin_seed[str(_r["id"])] = {"m": _mg["monthly_margin"], "s": _stage}
            except Exception:
                pass
    _margin_json = _json.dumps(_margin_seed)
    table = (
        "<table><thead><tr>"
        "<th>Prospect</th><th>Stage</th><th>Vol/mo</th>"
        "<th>Pitched $/mo</th><th>Actual cost</th><th>Margin</th>"
        "<th>Views</th><th>Actions</th>"
        "</tr></thead>"
        f"<tbody>{_history_rows(runs, engagement)}</tbody></table>"
        if runs
        else '<p class="empty">No rate sheets generated yet — the first one will appear here with its shareable link.</p>'
    )
    styles = _STYLES.replace("__NAV__", render_agent_nav_styles())
    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>agent | Fulfillment Sales Deck</title>
    {render_agent_favicon_links()}
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Montserrat:wght@700;800&display=swap" rel="stylesheet">
    <style>{styles}</style>
  </head>
  <body>
    {render_agent_nav("fulfillment", website_ops_section="fulfillment_sales", user=user)}
    <main class="shell">
      <div class="workspace">
        <p class="eyebrow">Fulfillment — Sales</p>
        <h1>Rate <span style="color:var(--light-blue)">Sheets</span>.</h1>
        <p class="intro">Paste whatever you know about the prospect — call notes, an email thread, a spreadsheet of products — and the system extracts their profile, quotes carrier rates per zone for each product size, and builds a hosted, printable rate sheet you can send as a link.</p>
        {flash_html}
        <form method="post" action="/admin/fulfillment/sales/generate" enctype="multipart/form-data">
          <div class="field">
            <label for="notes">Prospect notes <span class="hint">— free-form; anything goes (call notes, emails, product dims, volumes, current costs)</span></label>
            <textarea id="notes" name="notes" placeholder="e.g. Spoke with Sarah at GlowCo — they sell two SKUs: a serum (4 x 4 x 6 in, 1.2 lb) and a kit (10 x 8 x 4 in, 2.5 lb). ~3,000 orders/mo, mostly West Coast, paying about $9.80/parcel with UPS today."></textarea>
          </div>
          <div class="grid2">
            <div class="field">
              <label>Files <span class="hint">— optional CSV / XLSX / TXT, brand PDFs, or product images (specs, order exports, rate cards, line sheets)</span></label>
              <div class="drop"><input type="file" name="files" multiple accept=".csv,.xlsx,.xlsm,.txt,.md,.pdf,.png,.jpg,.jpeg,.webp"></div>
            </div>
            <div>
              <div class="field">
                <label for="website_url">Website <span class="hint">— optional</span></label>
                <input type="text" id="website_url" name="website_url" placeholder="prospect.com">
              </div>
              <div class="field">
                <label for="brand">Brand name <span class="hint">— optional override</span></label>
                <input type="text" id="brand" name="brand" placeholder="Auto-detected from notes" list="existing-brands" autocomplete="off">
                <datalist id="existing-brands">{''.join(f'<option value="{_esc(r["prospect"])}" label="{_esc(r["prospect"])} ({r.get("pipeline_stage","intake").replace("_"," ")})"/>' for r in runs if r.get("prospect"))}</datalist>
              </div>
              <div class="field">
                <label for="origin_zip">Ship-from ZIP</label>
                <input type="text" id="origin_zip" name="origin_zip" value="{ANATA_HQ_ZIP}">
                <span class="hint">Anata HQ — {_esc(ANATA_HQ_ADDRESS)}</span>
              </div>
            </div>
          </div>
          <button class="btn" type="submit">Generate rate sheet</button>
          {'<a href="#pipeline" class="muted" style="margin-left:12px;font-size:13px;text-decoration:none;opacity:.65">↓ or jump to pipeline</a>' if runs else ''}
        </form>
        <h2 id="pipeline">Pipeline</h2>
        <p class="muted" style="margin:-6px 0 12px">Click a row to expand — enter fulfillment costs, track margin, update stage. Click again to close. Changes save automatically.</p>
        {_pipeline_stats(runs) if runs else ""}
        {'<div style="display:flex;gap:10px;margin:0 0 10px;flex-wrap:wrap;align-items:center"><input id="pipe-search" type="search" placeholder="Filter by prospect…" oninput="filterPipeline()" style="flex:1;min-width:160px;max-width:280px;padding:7px 12px;border-radius:999px;border:1px solid var(--border);font-size:13px"><select id="pipe-stage" onchange="filterPipeline()" style="padding:7px 12px;border-radius:999px;border:1px solid var(--border);font-size:13px;background:#fff"><option value="">All stages</option><option value="intake">Intake</option><option value="pending_fulfillment">Sent to Fulfillment</option><option value="costs_received">Costs Received</option><option value="published">Published</option><option value="won">Won</option><option value="lost">Lost</option></select><select id="pipe-sort" onchange="sortPipeline()" style="padding:7px 12px;border-radius:999px;border:1px solid var(--border);font-size:13px;background:#fff"><option value="">Sort: Newest</option><option value="volume">Sort: Volume ↓</option><option value="pitched">Sort: Pitched $ ↓</option><option value="margin">Sort: Margin ↓</option><option value="views">Sort: Views ↓</option></select><a href="/admin/fulfillment/sales/export.csv" class="btn btn--ghost" style="white-space:nowrap;font-size:12px" title="Download pipeline as CSV">⬇ Export CSV</a><span id="pipe-count" class="muted" style="font-size:12px;white-space:nowrap;opacity:.5">' + str(len(runs)) + ' prospect' + ("s" if len(runs) != 1 else "") + '</span></div>' if runs else ""}
        {table}
      </div>
    </main>
    <script>
    // Per-run margin data for live stats-bar refresh
    var _marginData = {_margin_json};
    function refreshStatsBar() {{
      var terminal = new Set(['won','lost']);
      var total = 0, count = 0;
      for (var id in _marginData) {{
        var d = _marginData[id];
        var pRow = document.querySelector('tr.prospect-row[data-run="' + id + '"]');
        var stage = (pRow && pRow.dataset && pRow.dataset.stage) || d.s;
        if (!terminal.has(stage)) {{ total += (d.m || 0); count++; }}
      }}
      var el = document.getElementById('stat-margin');
      if (!el) return;
      var valEl = el.querySelector('.pipeline-stat__val');
      if (valEl && count > 0) {{
        var sign = total < 0 ? '−' : '';
        valEl.innerHTML = sign + '$' + Math.round(Math.abs(total)).toLocaleString('en-US') +
          '<span style="font-size:13px;font-weight:400">/mo</span>';
      }}
    }}
    // Generate form: loading state + duplicate brand warning.
    (function() {{
      var form = document.querySelector('form[action$="/generate"]');
      if (!form) return;
      var brandInput = form.querySelector('#brand');
      var existingBrands = [...document.querySelectorAll('#existing-brands option')].map(o => o.value.toLowerCase());
      if (brandInput && existingBrands.length) {{
        brandInput.addEventListener('input', function() {{
          var warn = document.getElementById('brand-dup-warn');
          if (this.value && existingBrands.includes(this.value.toLowerCase())) {{
            if (!warn) {{
              warn = document.createElement('span');
              warn.id = 'brand-dup-warn';
              warn.style.cssText = 'color:#b45309;font-size:12px;margin-left:6px';
              warn.textContent = '⚠ Rate sheet already exists — generating a new one will add to pipeline';
              this.parentNode.appendChild(warn);
            }}
          }} else if (warn) warn.remove();
        }});
      }}
      form.addEventListener('submit', function() {{
        var btn = form.querySelector('button[type="submit"]');
        if (btn) {{ btn.textContent = 'Generating… this takes ~30 sec'; btn.disabled = true; }}
      }});
    }})();
    function toggleExpand(e, id) {{
      if (e.target.closest('select,button,a,form,input')) return;
      var row = document.getElementById(id);
      if (!row) return;
      var open = row.style.display === 'none';
      row.style.display = open ? '' : 'none';
      var chev = row.previousElementSibling && row.previousElementSibling.querySelector('.row-chevron');
      if (chev) chev.style.transform = open ? 'rotate(90deg)' : '';
      if (open) setTimeout(() => row.scrollIntoView({{behavior: 'smooth', block: 'nearest'}}), 40);
    }}
    function quickStage(btn, stage, runId) {{
      var prospectRow = document.querySelector('tr.prospect-row[data-run="' + runId + '"]');
      var sel = prospectRow && prospectRow.querySelector('select');
      if (sel) {{ sel.value = stage; pipelineStage(sel, runId); }}
      var expandRow = document.getElementById('expand-' + runId);
      if (expandRow) {{
        expandRow.style.display = 'none';
        var chev = prospectRow && prospectRow.querySelector('.row-chevron');
        if (chev) chev.style.transform = '';
      }}
    }}
    function pipelineStage(sel, runId) {{
      sel.className = 'stage-select stage--' + sel.value;
      // Update left-border stage color immediately
      var pRow = document.querySelector('tr.prospect-row[data-run="' + runId + '"]');
      if (pRow) pRow.dataset.stage = sel.value;
      fetch('/admin/fulfillment/sales/runs/' + runId + '/stage', {{
        method: 'PATCH', headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify({{stage: sel.value}})
      }}).then(() => {{
        sel.style.outline = '2px solid #15803d';
        setTimeout(() => sel.style.outline = '', 1400);
        filterPipeline(); refreshStatsBar(); // re-apply filter and update active-margin stat
        // When advancing to "Sent to Fulfillment", auto-copy the brief so the rep can paste immediately.
        if (sel.value === 'pending_fulfillment') {{
          var expandRow = document.getElementById('expand-' + runId);
          var briefBtn = expandRow && expandRow.querySelector('button[data-brief]');
          if (briefBtn) {{
            navigator.clipboard.writeText(briefBtn.dataset.brief).catch(() => {{}});
            briefBtn.textContent = 'Brief copied! ✓';
            setTimeout(() => briefBtn.textContent = 'Copy brief', 2500);
          }}
        }}
      }});
    }}
    function pipelineCosts(btn, runId) {{
      var costs = {{
        pick_pack_per_order:  parseFloat(document.getElementById('pp-'+runId).value) || null,
        storage_per_pallet_mo: parseFloat(document.getElementById('st-'+runId).value) || null,
        receiving_per_pallet: parseFloat(document.getElementById('rc-'+runId).value) || null,
        monthly_tech_fee:     parseFloat(document.getElementById('tf-'+runId).value) || null,
      }};
      btn.textContent = 'Saving…';
      fetch('/admin/fulfillment/sales/runs/' + runId + '/costs', {{
        method: 'PATCH', headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify(costs)
      }}).then(r => r.json()).then(data => {{
        btn.textContent = 'Saved ✓';
        var fmt = v => '$' + Math.abs(v).toLocaleString('en-US', {{maximumFractionDigits:0}});
        if (data.margin) {{
          var mg = data.margin;
          var sign = mg.monthly_margin >= 0 ? 'pos' : 'neg';
          // Update expand panel margin card
          var card = document.getElementById('margin-' + runId);
          if (card) {{
            var recLine = '';
            if (data.receiving_one_time && data.pallets_mo) {{
              recLine = '<div class="margin-line" style="opacity:0.65"><span>Receiving one-time (~' + data.pallets_mo + ' pallets)</span><span>−' + fmt(data.receiving_one_time) + '</span></div>';
            }}
            card.innerHTML =
              '<div class="big big--' + sign + '">' + (mg.monthly_margin < 0 ? '−' : '') + fmt(mg.monthly_margin) +
              '/mo (' + mg.margin_pct + '%)</div>' +
              '<div class="margin-line"><span>Pitched monthly</span><span>' + fmt(data.pitched) + '</span></div>' +
              '<div class="margin-line"><span>Pick &amp; pack actual</span><span>−' + fmt(mg.actual_pick_pack) + '</span></div>' +
              '<div class="margin-line"><span>Storage actual</span><span>−' + fmt(mg.actual_storage) + '</span></div>' +
              '<div class="margin-line"><span>Tech fee actual</span><span>−' + fmt(mg.actual_tech_fee) + '</span></div>' +
              recLine +
              '<div class="margin-line" style="font-weight:700"><span>Annual margin</span><span>' + (mg.annual_margin < 0 ? '−' : '') + fmt(mg.annual_margin) + '</span></div>';
          }}
          // Update table row cells (actual cost + margin columns)
          var expandRow = document.getElementById('expand-' + runId);
          var prospectRow = expandRow ? expandRow.previousElementSibling : null;
          if (prospectRow) {{
            var tds = prospectRow.querySelectorAll('td');
            if (tds[4] && data.actual_monthly != null) tds[4].textContent = fmt(data.actual_monthly);
            if (tds[5]) {{
              var sc = mg.monthly_margin >= 0 ? '#15803d' : '#b91c1c';
              tds[5].innerHTML = '<span style="color:' + sc + ';font-weight:700">' +
                (mg.monthly_margin < 0 ? '−' : '') + fmt(mg.monthly_margin) + '</span>' +
                '<div class="muted">' + mg.margin_pct + '%</div>';
              tds[5].dataset.margin = String(mg.monthly_margin);
            }}
            // Auto-advance stage to Costs Received if still at an early stage
            var stageSelect = prospectRow.querySelector('select');
            if (stageSelect && (stageSelect.value === 'intake' || stageSelect.value === 'pending_fulfillment')) {{
              stageSelect.value = 'costs_received';
              pipelineStage(stageSelect, runId);
            }}
          }}
        }}
        // Update live stats bar with new margin
        if (data.margin) {{ _marginData[String(runId)] = {{m: data.margin.monthly_margin, s: 'costs_received'}}; refreshStatsBar(); }}
        setTimeout(() => btn.textContent = 'Save costs', 2000);
      }}).catch(() => {{ btn.textContent = 'Error — retry'; setTimeout(() => btn.textContent = 'Save costs', 3500); }});
    }}
    function sendBriefEmail(btn, runId) {{
      btn.textContent = 'Sending…'; btn.disabled = true;
      fetch('/admin/fulfillment/sales/runs/' + runId + '/send-brief', {{method: 'POST'}})
        .then(r => r.json()).then(d => {{
          if (d.ok) {{
            btn.textContent = 'Sent ✓';
          }} else {{
            var errMsg = (d.error || '').includes('FULFILLMENT_TEAM_EMAIL') ? 'Not configured — set FULFILLMENT_TEAM_EMAIL in Render' : (d.error || 'Error sending');
            btn.textContent = errMsg;
          }}
          btn.disabled = false;
          setTimeout(() => {{ btn.textContent = 'Send to warehouse →'; }}, 5000);
        }}).catch(() => {{ btn.textContent = 'Error — try again'; btn.disabled = false; }});
    }}
    var _noteTimers = {{}};
    function pipelineNotesDebounce(el, runId) {{
      clearTimeout(_noteTimers[runId]);
      el.style.borderColor = '';
      _noteTimers[runId] = setTimeout(() => {{
        fetch('/admin/fulfillment/sales/runs/' + runId + '/notes', {{
          method: 'PATCH', headers: {{'Content-Type': 'application/json'}},
          body: JSON.stringify({{notes: el.value}})
        }}).then(() => {{
          el.style.borderColor = '#15803d';
          setTimeout(() => el.style.borderColor = '', 1400);
        }});
      }}, 900);
    }}
    {'if (true) { setTimeout(() => location.reload(), 8000); }' if has_running else ''}
    // Keyboard shortcuts
    document.addEventListener('keydown', function(e) {{
      // Escape: close any open expand panel
      if (e.key === 'Escape') {{
        document.querySelectorAll('tr.expand-row').forEach(function(row) {{
          if (row.style.display !== 'none') {{
            row.style.display = 'none';
            var chev = row.previousElementSibling && row.previousElementSibling.querySelector('.row-chevron');
            if (chev) chev.style.transform = '';
          }}
        }});
      }}
    }});
    // Ctrl/Cmd+Enter in the generate textarea submits the form
    (function() {{
      var ta = document.getElementById('notes');
      if (!ta) return;
      ta.addEventListener('keydown', function(e) {{
        if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {{
          var form = ta.closest('form');
          if (form) form.submit();
        }}
      }});
    }})();
    function filterPipeline() {{
      var q = (document.getElementById('pipe-search') || {{}}).value || '';
      var stageEl = document.getElementById('pipe-stage');
      var stage = (stageEl || {{}}).value || '';
      q = q.toLowerCase().trim();
      // Persist stage selection across page loads
      try {{ if (stage) localStorage.setItem('fsp_stage', stage); else localStorage.removeItem('fsp_stage'); }} catch(e) {{}}
      var tbody = document.querySelector('table tbody');
      if (!tbody) return;
      var rows = tbody.querySelectorAll('tr.prospect-row');
      var shown = 0;
      rows.forEach(function(row) {{
        var expRow = document.getElementById(row.getAttribute('data-expand') || '');
        var name = (row.querySelector('td strong') || {{}}).textContent || '';
        var stageVal = (row.querySelector('select') || {{}}).value || '';
        var show = (!q || name.toLowerCase().includes(q)) && (!stage || stageVal === stage);
        row.style.display = show ? '' : 'none';
        if (expRow) expRow.style.display = 'none'; // collapse on filter
        if (show) shown++;
      }});
      // Update count indicator
      var countEl = document.getElementById('pipe-count');
      if (countEl) {{
        var total = rows.length;
        countEl.textContent = (shown < total) ? shown + ' of ' + total : total + ' prospect' + (total !== 1 ? 's' : '');
        countEl.style.opacity = shown < total ? '1' : '0.5';
      }}
    }}
    // Restore persisted stage filter on load
    (function() {{
      try {{
        var saved = localStorage.getItem('fsp_stage');
        if (saved) {{
          var stageEl = document.getElementById('pipe-stage');
          if (stageEl) {{ stageEl.value = saved; filterPipeline(); }}
        }}
      }} catch(e) {{}}
    }})();
    function sortPipeline() {{
      var key = (document.getElementById('pipe-sort') || {{}}).value || '';
      var tbody = document.querySelector('table tbody');
      if (!tbody) return;
      // Collect prospect+expand row pairs
      var allRows = [...tbody.querySelectorAll('tr')];
      var pairs = [];
      for (var i = 0; i < allRows.length; i += 2) {{
        if (allRows[i] && allRows[i+1]) pairs.push([allRows[i], allRows[i+1]]);
      }}
      if (!key) {{
        // Restore server-side order (by data-order attr added at render time)
        pairs.sort((a, b) => parseInt(a[0].dataset.order||0) - parseInt(b[0].dataset.order||0));
      }} else {{
        function getVal(row) {{
          var tds = row.querySelectorAll('td');
          var txt = function(i) {{ return ((tds[i] || {{}}).textContent || '').trim(); }};
          if (key === 'volume') return parseInt(txt(2).replace(/[^0-9]/g,'')) || 0;
          if (key === 'pitched') return parseFloat(txt(3).replace(/[^0-9.]/g,'')) || 0;
          if (key === 'margin') {{
            return parseFloat((tds[5] || {{}}).dataset.margin || 0) || 0;
          }}
          if (key === 'views') return parseInt(txt(6)) || 0;
          return 0;
        }}
        pairs.sort((a, b) => getVal(b[0]) - getVal(a[0]));
      }}
      pairs.forEach(function(pair, idx) {{
        pair[0].dataset.order = pair[0].dataset.order || idx;
        tbody.appendChild(pair[0]);
        pair[1].style.display = 'none';
        tbody.appendChild(pair[1]);
      }});
      // Re-apply filter after sort
      filterPipeline();
    }}
    </script>
  </body>
</html>"""


def _num_input(name: str, value: object, *, width: str = "76px", step: str = "any") -> str:
    val = "" if value is None else f"{value:g}" if isinstance(value, float) else str(value)
    return (
        f'<input type="number" name="{name}" value="{_esc(val)}" step="{step}" min="0" '
        f'style="width:{width};min-height:32px;padding:0 8px;border-radius:8px;'
        f'border:1px solid var(--border);font-size:13px">'
    )


def _product_row(index: int, product: dict, *, template: bool = False) -> str:
    name = str(product.get("name") or "")
    estimated = bool(product.get("dims_estimated"))
    est_tag = ' <span class="pill pill--estimated">estimated</span>' if estimated else ""
    remove_cell = (
        f'<input type="checkbox" name="product_remove" value="{index}" title="Remove this product">'
        if not template
        else ""
    )
    name_hint = ' placeholder="Add a product…"' if template else ""
    return (
        f"<tr>"
        f'<td><input type="text" name="product_name" value="{_esc(name)}"{name_hint} '
        f'style="width:100%;min-width:140px;min-height:32px;padding:0 8px;border-radius:8px;'
        f'border:1px solid var(--border);font-size:13px">{est_tag}'
        f'<input type="hidden" name="product_estimated" value="{1 if estimated else 0}"></td>'
        f"<td>{_num_input('product_length', product.get('length_in'))}</td>"
        f"<td>{_num_input('product_width', product.get('width_in'))}</td>"
        f"<td>{_num_input('product_height', product.get('height_in'))}</td>"
        f"<td>{_num_input('product_weight', product.get('weight_lb'))}</td>"
        f"<td>{_num_input('product_units', product.get('monthly_units'), width='90px', step='1')}</td>"
        f"<td style='text-align:center'>{remove_cell}</td>"
        f"</tr>"
    )


def _assortment_hint(profile: dict) -> str:
    """Warehouse-approval vetting card: estimated SKU count + deterministic
    size variance, computed from the stored profile. Internal-only."""
    from sales_support_agent.services.fulfillment_deck.rendering import (
        assortment_profile,
    )
    from sales_support_agent.services.fulfillment_deck.schema import ProspectProfile

    info = assortment_profile(ProspectProfile.from_dict(profile))
    if not info["products_quoted"] and info["estimated_sku_count"] is None:
        return ""

    sku_count = info["estimated_sku_count"]
    sku_text = f"{sku_count:,}" if sku_count is not None else "not stated"
    basis = info["sku_count_basis"]
    bits = [
        f"<strong>Est. SKU count:</strong> {_esc(sku_text)}"
        + (f' <span class="muted">({_esc(basis)})</span>' if basis else ""),
        f"<strong>Products quoted:</strong> {info['products_quoted']}",
    ]
    if info["size_label"]:
        bits.append(f"<strong>Size range:</strong> {_esc(info['size_label'])}")
    if info["variance"]:
        bits.append(f"<strong>Size variance:</strong> {_esc(info['variance'])}")
    if info["any_fragile"]:
        bits.append('<strong>Fragile items:</strong> yes')
    items = "".join(f"<li>{b}</li>" for b in bits)
    return (
        '<div class="flash"><strong>Warehouse approval — assortment:</strong>'
        f'<ul style="margin:6px 0 0;padding-left:18px">{items}</ul>'
        '<p class="muted" style="margin:6px 0 0">Size figures computed from the '
        "product dims below — share with the warehouse team before publishing.</p>"
        "</div>"
    )


def render_rate_sheet_review_page(
    run: dict,
    summary: dict,
    *,
    user: Optional[dict] = None,
    flash: str = "",
) -> str:
    """Review-before-publish page: live preview iframe + profile edit form."""
    run_id = int(run.get("id") or 0)
    status = str(run.get("status") or "")
    published = status == "completed"
    base = "/admin/fulfillment/sales"
    profile = dict(summary.get("prospect_profile") or {})
    products = [p for p in (profile.get("products") or []) if isinstance(p, dict)]

    flash_html = f'<div class="flash">{_esc(flash)}</div>' if flash else ""

    def _sanitize_warning(w: str) -> str:
        # Collapse raw LLM API exceptions into a one-liner — never show request IDs
        # or credit-balance details to the admin.
        if "LLM extraction failed" in w:
            suffix = "used basic text parsing instead" if "basic text" in w else "extraction fell back to basic parser"
            return f"Product details extracted with fallback parser ({suffix}) — review fields below."
        # Truncate any other internal exception messages
        return w[:200] + ("…" if len(w) > 200 else "")

    warnings = [_sanitize_warning(str(w)) for w in (summary.get("warnings") or []) if str(w).strip()]
    warnings_html = ""
    if warnings:
        items = "".join(f"<li>{_esc(w)}</li>" for w in warnings[:12])
        warn_label = "Published — notes:" if published else "Check before publishing:"
        warnings_html = (
            f'<div class="flash flash--warn"><strong>{warn_label}</strong>'
            f'<ul style="margin:6px 0 0;padding-left:18px">{items}</ul></div>'
        )

    view_path = str(summary.get("view_path") or "")
    hs_quote_url = str(summary.get("hubspot_quote_url") or "")
    hs_quote_btn = (
        f'<a class="btn" href="{_esc(hs_quote_url)}" target="_blank" rel="noreferrer" '
        f'style="background:#ff7a59;border-color:#ff7a59;color:#fff">Open HubSpot Quote ✍</a>'
        if hs_quote_url else ""
    )
    hs_create_quote_btn = (
        f'<form method="post" action="{base}/runs/{run_id}/quote" style="display:inline">'
        f'<button class="btn" type="submit" style="background:#ff7a59;border-color:#ff7a59;color:#fff">Create HubSpot Quote ✍</button></form>'
    )
    prospect_name = str(summary.get("prospect") or summary.get("design_title") or "your brand")
    if published and view_path:
        _full_link_js = f"window.location.origin+'{_esc(view_path)}'"
        _subj_attr = html.escape(f"Anata 3PL — {prospect_name} Fulfillment Rate Sheet", quote=True)
        # Build the email body as a JS expression so the link is injected client-side.
        # Using string concatenation so no template literal escaping needed inside onclick.
        _copy_email_js = html.escape(
            "var l=window.location.origin+'" + _esc(view_path) + "';"
            "var s='Subject: Anata 3PL \\u2014 " + prospect_name.replace("'", "\\'") + " Fulfillment Rate Sheet';"
            "var b='Hi,\\n\\nI wanted to share a customized fulfillment rate sheet from Anata "
            "\\u2014 it includes pricing tailored to your order volume and product specs."
            "\\n\\nView your rate sheet here: '+l+'\\n\\nHappy to walk through it on a quick call "
            "whenever works for you.\\n\\nBest,';"
            "navigator.clipboard.writeText(s+'\\n\\n'+b);"
            "this.textContent='Email copied!';setTimeout(()=>this.textContent='Copy email',2000);",
            quote=True,
        )
        publish_block = f"""
        <div class="flash">
          <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-bottom:8px">
            <strong>Published.</strong>
            <code style="font-size:12px;flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{_esc(view_path)}</code>
          </div>
          <div style="display:flex;gap:8px;flex-wrap:wrap">
            <button class="btn btn--ghost" type="button"
              onclick="navigator.clipboard.writeText({_full_link_js});this.textContent='Copied!';">Copy link</button>
            <button class="btn btn--ghost" type="button"
              onclick="{_copy_email_js}">Copy email</button>
            <a class="btn btn--ghost" href="{_esc(view_path)}?viewer=internal" target="_blank" rel="noreferrer">Open</a>
            {hs_quote_btn if hs_quote_url else hs_create_quote_btn}
          </div>
        </div>"""
        publish_button = '<button class="btn" type="submit">Re-publish</button>'
    else:
        publish_block = ""
        publish_button = '<button class="btn" type="submit">Publish — get shareable link</button>'

    rows = "".join(_product_row(i, p) for i, p in enumerate(products))
    rows += _product_row(len(products), {}, template=True)

    # v7: warehouse-approval vetting hints — estimated SKU count + size
    # variance computed deterministically from the products. Warehouse sign-off
    # happens here, before publish.
    assortment_html = _assortment_hint(profile)

    # Fulfillment brief — shown on review page so the rep can send it to the
    # warehouse team right away, before or after publishing the rate sheet.
    review_brief_run = {
        "id": run_id,
        "prospect": summary.get("prospect") or summary.get("design_title"),
        "origin_zip": summary.get("origin_zip"),
        "monthly_order_volume": profile.get("monthly_order_volume"),
        "prospect_profile": profile,
    }
    review_brief_text = _build_brief(review_brief_run)
    review_brief_attr = html.escape(review_brief_text, quote=True)
    brief_block = f"""
    <div class="flash" style="background:rgba(133,187,218,0.08);border-color:rgba(133,187,218,0.4);margin-bottom:16px">
      <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:12px;margin-bottom:8px">
        <strong>Fulfillment Brief</strong>
        <button class="btn btn--ghost" type="button" style="flex-shrink:0"
          data-brief="{review_brief_attr}"
          onclick="navigator.clipboard.writeText(this.dataset.brief);this.textContent='Copied!';setTimeout(()=>this.textContent='Copy brief',2000)">Copy brief</button>
      </div>
      <pre style="margin:0;font-family:inherit;font-size:12.5px;white-space:pre-wrap;color:rgba(43,54,68,0.75);line-height:1.55">{_esc(review_brief_text)}</pre>
      <p class="muted" style="margin:8px 0 0;font-size:12px">Share with the warehouse team to get a cost quote — paste into email or Slack.</p>
    </div>"""

    monthly_volume = profile.get("monthly_order_volume")
    current_cost = profile.get("current_cost_per_parcel_usd")
    volume_basis = str(profile.get("volume_basis") or "").strip()
    volume_provenance = str(profile.get("volume_provenance") or "").strip()
    # Vetting hint: the arithmetic (basis) AND where the number came from
    # (provenance) — the public sheet only ever shows the basis.
    hint_parts = []
    if volume_basis:
        hint_parts.append(f"Basis: {_esc(volume_basis)}")
    if volume_provenance:
        hint_parts.append(f"Source: {_esc(volume_provenance)}")
    volume_basis_hint = (
        f'<span class="hint">{" · ".join(hint_parts)}</span>' if hint_parts else ""
    )
    margin_override = summary.get("quote_margin_override")
    margin_value = "" if margin_override is None else f"{margin_override:g}"

    from sales_support_agent.services.fulfillment_deck.quote import BASELINE_RATES
    _ro = dict(summary.get("rate_overrides") or {})

    def _rval(key: str) -> str:
        v = _ro.get(key)
        return f"{v:g}" if v is not None else ""

    rate_card_note_val = _esc(str(summary.get("rate_card_note") or ""))

    status_label = "Published" if published else "Draft — not publicly visible yet"
    status_pill_cls = "pill--live" if published else "pill--draft"

    styles = _STYLES.replace("__NAV__", render_agent_nav_styles())
    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>agent | Review Rate Sheet — {_esc(summary.get('prospect') or f'Run {run_id}')}</title>
    {render_agent_favicon_links()}
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Montserrat:wght@700;800&display=swap" rel="stylesheet">
    <style>{styles}
      .preview-frame {{ width: 100%; height: 70vh; border: 1px solid var(--border);
        border-radius: 16px; background: #fff; box-shadow: 0 12px 28px var(--shadow); }}
      .review-actions {{ display: flex; gap: 10px; flex-wrap: wrap; align-items: center; margin-top: 16px; }}
      .products-table input[type=number] {{ font-family: inherit; }}
    </style>
  </head>
  <body>
    {render_agent_nav("fulfillment", website_ops_section="fulfillment_sales", user=user)}
    <main class="shell">
      <div class="workspace">
        <p class="eyebrow"><a href="{base}" style="color:inherit;text-decoration:none;opacity:0.7">← Pipeline</a> · Review</p>
        <h1>{_esc(summary.get('prospect') or 'Rate sheet')} <span style="color:var(--light-blue)">rate sheet</span>.</h1>
        <p class="intro">{'Rate sheet is live — edit fields below and re-publish to update. Shareable link stays the same.' if published else 'Check the preview, fix anything the extraction got wrong, then publish to activate the shareable link.'} <span class="pill {status_pill_cls}">{_esc(status_label)}</span></p>
        {flash_html}
        {publish_block}
        {warnings_html}
        {assortment_html}
        {brief_block}
        {'<form method="post" action="' + base + '/runs/' + str(run_id) + '/publish" style="margin-bottom:10px"><button class="btn" type="submit" style="width:100%">Publish — get shareable link</button></form>' if not published else ''}
        <iframe class="preview-frame" id="preview" src="{base}/runs/{run_id}/preview" title="Rate sheet preview"></iframe>

        <h2>Prospect details</h2>
        <form method="post" action="{base}/runs/{run_id}/update">
          <div class="grid2">
            <div>
              <div class="field">
                <label for="brand">Brand</label>
                <input type="text" id="brand" name="brand" value="{_esc(profile.get('brand') or '')}">
              </div>
              <div class="field">
                <label for="destinations_note">Destinations note</label>
                <input type="text" id="destinations_note" name="destinations_note" value="{_esc(profile.get('destinations_note') or '')}">
              </div>
              <div class="field">
                <label for="current_costs_note">Current costs note</label>
                <input type="text" id="current_costs_note" name="current_costs_note" value="{_esc(profile.get('current_costs_note') or '')}">
              </div>
            </div>
            <div>
              <div class="field">
                <label for="origin_zip">Ship-from ZIP</label>
                <input type="text" id="origin_zip" name="origin_zip" value="{_esc(summary.get('origin_zip') or '')}">
              </div>
              <div class="field">
                <label for="monthly_order_volume">Monthly order volume</label>
                <input type="text" id="monthly_order_volume" name="monthly_order_volume" value="{_esc('' if monthly_volume is None else monthly_volume)}">
                {volume_basis_hint}
              </div>
              <div class="field">
                <label for="current_cost_per_parcel_usd">Current $/parcel</label>
                <input type="text" id="current_cost_per_parcel_usd" name="current_cost_per_parcel_usd" value="{_esc('' if current_cost is None else f'{current_cost:g}')}">
                <span class="hint">Drives the savings section — leave blank to omit.</span>
              </div>
              <div class="field">
                <label for="quote_margin_override">Quote margin override %</label>
                <input type="number" id="quote_margin_override" name="quote_margin_override" step="any" min="0" value="{_esc(margin_value)}">
                <span class="hint">Blank = automatic by product category. e.g. 12 quotes everything at baseline × 1.12.</span>
              </div>
            </div>
          </div>

          <h2>Fee Card Adjustments</h2>
          <p class="muted" style="margin-bottom:12px">Override any baseline rate for this prospect's deck only. Leave blank to use the standard rate shown in parentheses.</p>
          <div class="edit-grid">
            <div class="edit-col">
              <div class="field">
                <label for="rate_receiving">Receiving / pallet (baseline ${BASELINE_RATES['receiving_per_pallet']:g})</label>
                <input type="number" id="rate_receiving" name="rate_receiving" step="0.01" min="0" value="{_rval('receiving_per_pallet')}" placeholder="{BASELINE_RATES['receiving_per_pallet']:g}">
              </div>
              <div class="field">
                <label for="rate_storage">Storage / pallet/mo (baseline ${BASELINE_RATES['storage_short_per_pallet_mo']:g})</label>
                <input type="number" id="rate_storage" name="rate_storage" step="0.01" min="0" value="{_rval('storage_short_per_pallet_mo')}" placeholder="{BASELINE_RATES['storage_short_per_pallet_mo']:g}">
              </div>
              <div class="field">
                <label for="rate_pick_pack">DTC pick &amp; pack / order (baseline ${BASELINE_RATES['dtc_base_per_order']:g})</label>
                <input type="number" id="rate_pick_pack" name="rate_pick_pack" step="0.01" min="0" value="{_rval('dtc_base_per_order')}" placeholder="{BASELINE_RATES['dtc_base_per_order']:g}">
              </div>
              <div class="field">
                <label for="rate_additional_item">DTC additional item (baseline ${BASELINE_RATES['dtc_additional_item']:g})</label>
                <input type="number" id="rate_additional_item" name="rate_additional_item" step="0.01" min="0" value="{_rval('dtc_additional_item')}" placeholder="{BASELINE_RATES['dtc_additional_item']:g}">
              </div>
              <div class="field">
                <label for="rate_kitting">Kitting / unit (baseline ${BASELINE_RATES['kitting_per_unit']:g})</label>
                <input type="number" id="rate_kitting" name="rate_kitting" step="0.01" min="0" value="{_rval('kitting_per_unit')}" placeholder="{BASELINE_RATES['kitting_per_unit']:g}">
              </div>
            </div>
            <div class="edit-col">
              <div class="field">
                <label for="rate_labeling">Labeling / unit (baseline ${BASELINE_RATES['labeling_per_unit']:g})</label>
                <input type="number" id="rate_labeling" name="rate_labeling" step="0.01" min="0" value="{_rval('labeling_per_unit')}" placeholder="{BASELINE_RATES['labeling_per_unit']:g}">
              </div>
              <div class="field">
                <label for="rate_wholesale">Wholesale / unit (baseline ${BASELINE_RATES['wholesale_per_unit']:g})</label>
                <input type="number" id="rate_wholesale" name="rate_wholesale" step="0.01" min="0" value="{_rval('wholesale_per_unit')}" placeholder="{BASELINE_RATES['wholesale_per_unit']:g}">
              </div>
              <div class="field">
                <label for="rate_returns">Returns / unit (baseline ${BASELINE_RATES['returns_per_unit']:g})</label>
                <input type="number" id="rate_returns" name="rate_returns" step="0.01" min="0" value="{_rval('returns_per_unit')}" placeholder="{BASELINE_RATES['returns_per_unit']:g}">
              </div>
              <div class="field">
                <label for="rate_tech_fee">Monthly tech fee (baseline ${BASELINE_RATES['monthly_tech_fee']:g})</label>
                <input type="number" id="rate_tech_fee" name="rate_tech_fee" step="0.01" min="0" value="{_rval('monthly_tech_fee')}" placeholder="{BASELINE_RATES['monthly_tech_fee']:g}">
              </div>
              <div class="field">
                <label for="rate_minimum">Monthly minimum (baseline ${BASELINE_RATES['monthly_minimum']:g})</label>
                <input type="number" id="rate_minimum" name="rate_minimum" step="1" min="0" value="{_rval('monthly_minimum')}" placeholder="{BASELINE_RATES['monthly_minimum']:g}">
              </div>
            </div>
          </div>
          <div class="field" style="margin-top:8px">
            <label for="rate_card_note">Rate card note (shown at bottom of Full Rate Card section)</label>
            <textarea id="rate_card_note" name="rate_card_note" rows="2" style="width:100%;resize:vertical">{rate_card_note_val}</textarea>
            <span class="hint">Use to call out specials, volume commitments, expiry dates, etc.</span>
          </div>

          <h2>Products</h2>
          {'<div class="flash flash--warn" style="margin-bottom:12px"><strong>No products on file.</strong> Fill in at least one product row below (name + dimensions + units/mo) so the rate sheet shows accurate savings estimates, then save &amp; re-publish.</div>' if not products else ''}
          <table class="products-table">
            <thead><tr><th>Name</th><th>L (in)</th><th>W (in)</th><th>H (in)</th><th>Weight (lb)</th><th>Units / mo</th><th>Remove</th></tr></thead>
            <tbody>{rows}</tbody>
          </table>
          <p class="muted">Rows tagged <span class="pill pill--estimated">estimated</span> had dimensions guessed from the product type — confirm or correct them before sending. Editing a dimension clears the tag. Tick Remove to drop a product; fill the empty row to add one.</p>
          <div class="review-actions">
            <button class="btn" type="submit">Save &amp; re-render</button>
            <span class="muted" style="font-size:12px">Saving does not affect the public link.</span>
          </div>
        </form>
        <form method="post" action="{base}/runs/{run_id}/publish" style="margin-top:10px">
          <div class="review-actions">
            {publish_button}
            <a class="btn btn--ghost" href="{base}">← Pipeline</a>
          </div>
        </form>
      </div>
    </main>
    <script>
      // Editing any dim/weight input clears that row's "estimated" flag.
      document.querySelectorAll('.products-table tbody tr').forEach(function(tr) {{
        var hidden = tr.querySelector('input[name=product_estimated]');
        if (!hidden) return;
        tr.querySelectorAll('input[type=number]').forEach(function(inp) {{
          inp.addEventListener('change', function() {{
            hidden.value = '0';
            var tag = tr.querySelector('.pill--estimated');
            if (tag) tag.remove();
          }});
        }});
      }});
    </script>
  </body>
</html>"""
