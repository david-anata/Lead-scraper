"""Admin page for Fulfillment > Sales Deck (rate sheet generator + history).

Same admin shell vocabulary as the Brand Analysis page (nav + workspace card),
so it reads as a sibling tool.
"""

from __future__ import annotations

import html
from typing import Optional

from sales_support_agent.services.admin_nav import (
    render_agent_favicon_links,
    render_agent_nav,
    render_agent_nav_styles,
)
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
      .pill--estimated { background: #fff4d9; color: #7a5b14; border: 1px solid #d2a94b; }
      .row-actions { display: flex; gap: 6px; flex-wrap: wrap; }
      .muted { color: rgba(43,54,68,0.55); font-size: 12px; }
      .empty { color: rgba(43,54,68,0.55); font-size: 13.5px; padding: 18px 0; }
      @media (max-width: 760px) { .grid2 { grid-template-columns: 1fr; } }
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


def _history_rows(runs: list[dict], engagement: dict[int, dict]) -> str:
    rows = []
    for run in runs:
        run_id = int(run.get("id") or 0)
        started = str(run.get("started_at") or "")[:16].replace("T", " ")
        prospect = _esc(run.get("prospect") or run.get("design_title") or f"Run {run_id}")
        status = str(run.get("status") or "")
        view_path = str(run.get("view_path") or "")
        published = bool(run.get("published")) and status == "completed"
        review_path = f"/admin/fulfillment/sales/runs/{run_id}/review"
        if status == "failed":
            source_pill = '<span class="pill pill--failed">Failed</span>'
        elif status == "draft":
            source_pill = '<span class="pill pill--draft">Draft</span>'
        elif str(run.get("rates_source")) == RATE_SOURCE_WMS:
            source_pill = '<span class="pill pill--live">Live rates</span>'
        else:
            source_pill = '<span class="pill pill--sample">Sample rates</span>'
        stats = engagement.get(run_id) or {}
        ext = int(stats.get("external_sessions") or 0)
        total_secs = int(stats.get("total_seconds") or 0)
        views = f"{ext} visit{'s' if ext != 1 else ''} · {_fmt_duration(total_secs)}" if ext else "—"
        sections = len(run.get("sections_included") or [])
        actions = []
        if status == "draft":
            actions.append(f'<a class="btn btn--ghost" href="{review_path}">Review</a>')
        elif view_path and published:
            actions.append(f'<a class="btn btn--ghost" href="{_esc(view_path)}?viewer=internal" target="_blank" rel="noreferrer">Open</a>')
            actions.append(
                f'<button class="btn btn--ghost" type="button" '
                f"onclick=\"navigator.clipboard.writeText(window.location.origin + '{_esc(view_path)}');this.textContent='Copied';\">Copy link</button>"
            )
            actions.append(f'<a class="btn btn--ghost" href="{review_path}">Review / edit</a>')
        actions.append(
            f'<form method="post" action="/admin/fulfillment/sales/runs/{run_id}/delete" '
            f"style=\"display:inline\" onsubmit=\"return confirm('Delete this rate sheet? The public link will stop working.');\">"
            f'<button class="btn btn--danger" type="submit">Delete</button></form>'
        )
        rows.append(
            f"<tr><td>{_esc(started)}</td><td><strong>{prospect}</strong>"
            f"<div class='muted'>{sections} sections</div></td>"
            f"<td>{_esc(run.get('origin_zip') or '')}</td><td>{source_pill}</td>"
            f"<td>{_esc(views)}</td><td><div class='row-actions'>{''.join(actions)}</div></td></tr>"
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
    table = (
        "<table><thead><tr><th>Created</th><th>Prospect</th><th>Origin</th><th>Rates</th>"
        "<th>Engagement</th><th>Actions</th></tr></thead>"
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
                <input type="url" id="website_url" name="website_url" placeholder="https://prospect.com">
              </div>
              <div class="field">
                <label for="brand">Brand name <span class="hint">— optional override</span></label>
                <input type="text" id="brand" name="brand" placeholder="Auto-detected from notes">
              </div>
              <div class="field">
                <label for="origin_zip">Ship-from ZIP</label>
                <input type="text" id="origin_zip" name="origin_zip" value="{ANATA_HQ_ZIP}">
                <span class="hint">Anata HQ — {_esc(ANATA_HQ_ADDRESS)}</span>
              </div>
            </div>
          </div>
          <button class="btn" type="submit">Generate rate sheet</button>
        </form>
        <h2>History</h2>
        {table}
      </div>
    </main>
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

    warnings = [str(w) for w in (summary.get("warnings") or []) if str(w).strip()]
    warnings_html = ""
    if warnings:
        items = "".join(f"<li>{_esc(w)}</li>" for w in warnings[:12])
        warnings_html = (
            f'<div class="flash flash--warn"><strong>Check before publishing:</strong>'
            f'<ul style="margin:6px 0 0;padding-left:18px">{items}</ul></div>'
        )

    view_path = str(summary.get("view_path") or "")
    if published and view_path:
        publish_block = f"""
        <div class="flash"><strong>Published.</strong> Shareable link:
          <code id="public-link" style="font-size:12px">{_esc(view_path)}</code>
          <button class="btn btn--ghost" type="button"
            onclick="navigator.clipboard.writeText(window.location.origin + '{_esc(view_path)}');this.textContent='Copied';">Copy link</button>
          <a class="btn btn--ghost" href="{_esc(view_path)}?viewer=internal" target="_blank" rel="noreferrer">Open</a>
        </div>"""
        publish_button = '<button class="btn" type="submit">Re-publish</button>'
    else:
        publish_block = ""
        publish_button = '<button class="btn" type="submit">Publish — get shareable link</button>'

    rows = "".join(_product_row(i, p) for i, p in enumerate(products))
    rows += _product_row(len(products), {}, template=True)

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
        <p class="eyebrow">Fulfillment — Sales · Review</p>
        <h1>{_esc(summary.get('prospect') or 'Rate sheet')} <span style="color:var(--light-blue)">rate sheet</span>.</h1>
        <p class="intro">Check the preview, fix anything the extraction got wrong, then publish to activate the shareable link. <span class="pill {status_pill_cls}">{_esc(status_label)}</span></p>
        {flash_html}
        {publish_block}
        {warnings_html}
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

          <h2>Products</h2>
          <table class="products-table">
            <thead><tr><th>Name</th><th>L (in)</th><th>W (in)</th><th>H (in)</th><th>Weight (lb)</th><th>Units / mo</th><th>Remove</th></tr></thead>
            <tbody>{rows}</tbody>
          </table>
          <p class="muted">Rows tagged <span class="pill pill--estimated">estimated</span> had dimensions guessed from the product type — confirm or correct them before sending. Editing a dimension clears the tag. Tick Remove to drop a product; fill the empty row to add one.</p>
          <div class="review-actions">
            <button class="btn" type="submit">Save &amp; re-render</button>
          </div>
        </form>
        <form method="post" action="{base}/runs/{run_id}/publish" style="margin-top:10px">
          <div class="review-actions">
            {publish_button}
            <a class="btn btn--ghost" href="{base}">Back to history</a>
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
