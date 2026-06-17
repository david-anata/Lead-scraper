"""On-screen rendering for Brand Analysis — upload, report "sheet", history.

Raw-HTML strings (the house style in admin_dashboard.py), reusing the shared
agent nav + design tokens. The report view reproduces the exact spec section
order: grade banner, missing-data block directly under the grade, executive
summary, YoY table, monthly trajectory bars, acquisition/media mix,
contribution, balance sheet, ranked red flags, category benchmarks, weighted
scorecard, data gaps, verdict.
"""

from __future__ import annotations

import html
from typing import Optional

from sales_support_agent.services.admin_nav import (
    render_agent_favicon_links,
    render_agent_nav,
    render_agent_nav_styles,
)
from sales_support_agent.services.brand_analysis.schema import (
    CATEGORY_LABELS,
    BrandReport,
    benchmarks_for,
    fmt_money,
    fmt_mult,
    fmt_pct,
    safe_div,
)

_GRADE_COLORS = {
    "A": "#2e7d5b", "B": "#3f8f6e", "C": "#b8860b", "D": "#c2663b", "F": "#8b4c42",
}
_SEV_COLORS = {"Critical": "#8b4c42", "High": "#c2663b", "Medium": "#b8860b"}


def _esc(text: object) -> str:
    return html.escape(str(text if text is not None else ""))


def _styles() -> str:
    return """
      :root {
        --dark-blue: #2B3644; --alt-dark-blue: #33445C; --light-blue: #85BBDA;
        --brown: #BFA889; --light-brown: #F9F7F3; --white: #FFFFFF; --text: #2B3644;
        --border: rgba(43, 54, 68, 0.10); --shadow: rgba(43, 54, 68, 0.10);
      }
      * { box-sizing: border-box; }
      body { margin: 0; background: var(--light-brown); color: var(--text); font-family: "Inter","Segoe UI",sans-serif; }
      a { color: var(--dark-blue); text-decoration: none; }
      __NAV__
      .shell { max-width: 1080px; margin: 0 auto; padding: 32px 20px 72px; }
      .workspace { background: var(--white); border: 1px solid var(--border); border-radius: 28px; box-shadow: 0 18px 40px var(--shadow); padding: 32px; }
      h1, h2, h3 { font-family: "Montserrat", sans-serif; color: var(--dark-blue); }
      h1 { font-size: 30px; margin: 0 0 4px; }
      h2 { font-size: 19px; margin: 34px 0 12px; padding-bottom: 8px; border-bottom: 2px solid var(--light-blue); }
      .eyebrow { display: inline-block; padding: 6px 12px; border-radius: 6px; background: var(--dark-blue); color: #fff;
        font-family: "Montserrat",sans-serif; font-weight: 700; font-size: 11px; letter-spacing: 0.08em; text-transform: uppercase; }
      .muted { color: rgba(43,54,68,0.62); font-size: 13px; }
      table { width: 100%; border-collapse: collapse; font-size: 13.5px; margin: 6px 0 8px; }
      th, td { text-align: left; padding: 9px 11px; border-bottom: 1px solid var(--border); }
      thead th { background: rgba(133,187,218,0.20); font-family: "Montserrat",sans-serif; font-size: 11px;
        letter-spacing: 0.04em; text-transform: uppercase; color: var(--dark-blue); }
      td.num, th.num { text-align: right; font-variant-numeric: tabular-nums; }
      /* Grade banner */
      .grade-banner { display: grid; grid-template-columns: 132px 1fr; gap: 22px; align-items: center;
        border-radius: 18px; padding: 22px 24px; color: #fff; margin: 16px 0 10px; }
      .grade-letter { font-family: "Montserrat",sans-serif; font-weight: 800; font-size: 78px; line-height: 1;
        text-align: center; background: rgba(255,255,255,0.16); border-radius: 16px; padding: 8px 0; }
      .grade-score { font-size: 15px; opacity: 0.92; font-weight: 600; }
      .grade-verdict { font-size: 14.5px; margin-top: 6px; line-height: 1.5; }
      /* Missing-data block */
      .missing { background: var(--light-brown); border: 1px solid var(--border); border-radius: 14px; padding: 16px 18px; margin: 4px 0 8px; }
      .missing h3 { margin: 0 0 8px; font-size: 14px; }
      .missing ul { margin: 0; padding-left: 18px; }
      .missing li { margin: 3px 0; font-size: 13px; }
      .pill { display: inline-block; padding: 3px 10px; border-radius: 999px; font-size: 11px; font-weight: 700;
        font-family: "Montserrat",sans-serif; letter-spacing: 0.03em; }
      .conf-High { background: rgba(46,125,91,0.16); color: #2e7d5b; }
      .conf-Medium { background: rgba(184,134,11,0.16); color: #8a6508; }
      .conf-Low { background: rgba(139,76,66,0.16); color: #8b4c42; }
      .sufficient { color: #2e7d5b; font-weight: 600; }
      /* Bars */
      .bars { display: flex; align-items: flex-end; gap: 6px; height: 150px; padding: 12px 4px 0; border-bottom: 1px solid var(--border); }
      .bar { flex: 1; background: linear-gradient(180deg, var(--light-blue), var(--alt-dark-blue)); border-radius: 5px 5px 0 0; min-height: 3px; position: relative; }
      .bar-labels { display: flex; gap: 6px; padding-top: 6px; }
      .bar-labels span { flex: 1; text-align: center; font-size: 10px; color: rgba(43,54,68,0.6); }
      .pass { color: #2e7d5b; font-weight: 700; }
      .fail { color: #8b4c42; font-weight: 700; }
      .gap { color: rgba(43,54,68,0.45); }
      .sev { font-weight: 700; }
      .grade-cell { font-family: "Montserrat",sans-serif; font-weight: 800; }
      .btn { display: inline-flex; align-items: center; gap: 8px; min-height: 44px; padding: 0 20px; border-radius: 999px;
        background: var(--dark-blue); color: #fff; font-family: "Montserrat",sans-serif; font-weight: 700; font-size: 13px;
        border: none; cursor: pointer; }
      .btn--ghost { background: #fff; color: var(--dark-blue); border: 1px solid var(--border); }
      .btn-row { display: flex; gap: 10px; flex-wrap: wrap; margin: 6px 0 4px; }
      /* Upload */
      .drop { border: 2px dashed rgba(133,187,218,0.7); border-radius: 16px; padding: 30px; text-align: center; background: var(--light-brown); }
      .drop input[type=file] { margin-top: 10px; }
      .field { display: grid; gap: 5px; margin: 10px 0; }
      .field label { font-family: "Montserrat",sans-serif; font-weight: 700; font-size: 12px; color: var(--dark-blue); }
      .field input, .field select, .field textarea { min-height: 40px; padding: 8px 12px; border-radius: 10px; border: 1px solid var(--border); font-size: 14px; font-family: inherit; }
      .field textarea { resize: vertical; }
      .grid2 { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; }
      .flash { background: rgba(133,187,218,0.18); border: 1px solid rgba(133,187,218,0.5); border-radius: 12px; padding: 12px 16px; margin-bottom: 14px; font-size: 13.5px; }
      .share-frame { width: 100%; height: 1400px; border: 1px solid var(--border); border-radius: 16px; background: #fff; margin-top: 12px; }
      .file-list { display: grid; gap: 6px; }
      .file-row { display: flex; align-items: center; gap: 10px; padding: 8px 12px; border: 1px solid var(--border); border-radius: 10px; background: #fff; font-size: 13.5px; cursor: pointer; }
      .file-row:has(input:checked) { background: rgba(139,76,66,0.06); border-color: rgba(139,76,66,0.3); }
      .file-row:has(input:checked) .file-name { text-decoration: line-through; color: rgba(43,54,68,0.45); }
      .file-name { flex: 1; }
      .file-rm { font-size: 11px; color: #8b4c42; font-weight: 700; text-transform: uppercase; letter-spacing: 0.04em; }
      .social-block { border: 1px solid var(--border); border-radius: 12px; padding: 12px 16px; margin: 10px 0; background: rgba(133,187,218,0.05); }
      .social-block summary { font-family: "Montserrat",sans-serif; font-weight: 700; font-size: 13px; cursor: pointer; }
      .row-actions { white-space: nowrap; }
      .row-act { font-size: 12px; font-weight: 600; color: var(--dark-blue); text-decoration: none; background: rgba(43,54,68,0.06); border: 1px solid var(--border); border-radius: 8px; padding: 3px 9px; cursor: pointer; }
      .row-act:hover { background: rgba(43,54,68,0.11); }
      .stands-out { margin: 6px 0; padding-left: 18px; }
      .stands-out li { margin: 4px 0; }
      @media (max-width: 720px) { .grid2 { grid-template-columns: 1fr; } .grade-banner { grid-template-columns: 96px 1fr; } }
    """.replace("__NAV__", render_agent_nav_styles())


def _doc(title: str, body: str, *, user: Optional[dict], section: str = "brand_analysis") -> str:
    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>agent | {_esc(title)}</title>
    {render_agent_favicon_links()}
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Montserrat:wght@700;800&display=swap" rel="stylesheet">
    <style>{_styles()}</style>
  </head>
  <body>
    {render_agent_nav(section, executive_section=section, user=user)}
    <main class="shell">
      <div class="workspace">
        {body}
      </div>
    </main>
  </body>
</html>"""


# ---------------------------------------------------------------------------
# Upload + history strip
# ---------------------------------------------------------------------------


def render_brand_analysis_page(*, runs: list, user: Optional[dict] = None,
                               flash: str = "", detail: str = "") -> str:
    flash_html = f'<div class="flash">{_esc(flash)}{(" — " + _esc(detail)) if detail else ""}</div>' if flash else ""
    options = "".join(
        f'<option value="{k}">{_esc(v)}</option>' for k, v in CATEGORY_LABELS.items()
    )
    body = f"""
      <span class="eyebrow">Executive · Brand Analysis</span>
      <h1>Brand Analysis</h1>
      <p class="muted">Drop a brand's financial file dump — P&amp;L, Balance Sheet, Trial Balance, GL, prior-year — and get a graded executive acquisition report. .xlsx, .xls, .csv and .pdf are accepted.</p>
      {flash_html}
      <div style="display:flex;align-items:center;gap:12px;background:#f0f4ff;border:1px solid #c7d7f8;border-radius:8px;padding:12px 16px;margin-bottom:18px">
        <svg xmlns="http://www.w3.org/2000/svg" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="#1d4ed8" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="flex-shrink:0"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="12" y1="18" x2="12" y2="12"/><line x1="9" y1="15" x2="15" y2="15"/></svg>
        <div style="flex:1;min-width:0">
          <span style="font-weight:600;color:#1e3a8a;font-size:13px">Not sure what to upload?</span>
          <span style="color:#475569;font-size:13px;margin-left:4px">View our intake checklist — a full guide covering every file we need and why.</span>
        </div>
        <a href="/brand-intake" target="_blank"
           style="flex-shrink:0;background:#1d4ed8;color:#fff;font-size:12px;font-weight:600;padding:7px 14px;border-radius:6px;text-decoration:none;white-space:nowrap">
          View Checklist
        </a>
      </div>
      <form method="post" action="/admin/executive/brand-analysis/run" enctype="multipart/form-data">
        <div class="drop">
          <strong>Drop financial files here</strong>
          <div class="muted">Upload one or many at once.</div>
          <input type="file" name="files" multiple accept=".xlsx,.xls,.csv,.pdf">
        </div>
        <div class="grid2">
          <div class="field">
            <label for="brand">Brand name (optional)</label>
            <input id="brand" name="brand" placeholder="auto-detected from filenames if blank">
          </div>
          <div class="field">
            <label for="category">Category / business model</label>
            <select id="category" name="category">{options}</select>
          </div>
        </div>
        <div class="grid2">
          <div class="field">
            <label for="brand_website">Brand website (optional — pulls logo &amp; product imagery)</label>
            <input id="brand_website" name="brand_website" placeholder="luxmery.com">
          </div>
          <div class="field"></div>
        </div>
        <div class="field">
          <label for="context_notes">Context notes (optional — anything you know about the brand)</label>
          <textarea id="context_notes" name="context_notes" rows="3" placeholder="e.g. legal entity differs from brand; related-party loan is owner financing."></textarea>
        </div>
        {_social_fields()}
        {_override_fields()}
        <div class="btn-row"><button class="btn" type="submit">Run analysis</button></div>
      </form>
      {_history_table(runs, heading="Analysis history", empty="No analyses yet — run one above.")}
    """
    return _doc("Brand Analysis", body, user=user)


_OVERRIDE_INPUTS = (
    ("override_net_revenue", "Net revenue"),
    ("override_cogs", "COGS"),
    ("override_reported_gross_profit", "Reported gross profit"),
    ("override_marketing_total", "Marketing spend"),
    ("override_opex", "Operating expenses"),
    ("override_net_earnings", "Net earnings"),
)


def _version_history(versions: Optional[list]) -> str:
    """Prior grades captured before each rerun — shows how the analysis evolved
    even though the live report overwrites in place."""
    if not versions:
        return ""
    rows = ""
    for v in reversed(versions):
        when = (v.get("at") or "")[:16].replace("T", " ")
        grade = v.get("grade") or "—"
        color = _GRADE_COLORS.get(grade, "#666")
        rows += (f"<tr><td><span class='grade-cell' style='color:{color}'>{_esc(grade)}</span> "
                 f"<span class='muted'>{v.get('score_100', 0)}/100</span></td>"
                 f"<td><span class='pill conf-{_esc(v.get('confidence') or '')}'>{_esc(v.get('confidence') or '—')}</span></td>"
                 f"<td class='muted'>{_esc(v.get('period_current') or '')}</td>"
                 f"<td class='muted'>{_esc(when)}</td></tr>")
    return f"""
      <h2 style="margin-top:30px;font-size:17px">Version history</h2>
      <p class="muted" style="margin-top:-4px">Grades captured before each rerun (most recent first). The live link always shows the latest.</p>
      <table><thead><tr><th>Grade</th><th>Confidence</th><th>Period</th><th>Saved</th></tr></thead>
      <tbody>{rows}</tbody></table>
    """


def _override_fields(row: Optional[dict] = None) -> str:
    """Manual corrections — exact dollar values that win over the parsed numbers.
    The escape hatch when a figure is mis-parsed."""
    row = row or {}
    cells = ""
    for key, label in _OVERRIDE_INPUTS:
        cells += f"""
            <div class="field">
              <label for="{key}">{label}</label>
              <input id="{key}" name="{key}" inputmode="numeric" placeholder="$ — leave blank to keep parsed" value="{_esc(row.get(key) or '')}">
            </div>"""
    return f"""
        <details class="social-block">
          <summary>Corrections (optional — override a mis-parsed number)</summary>
          <p class="muted" style="margin:8px 0 4px">Enter exact dollar amounts to override what the parser found. Blank fields keep the parsed value.</p>
          <div class="grid2">{cells}</div>
        </details>"""


def _social_fields(*, email_list_size: object = "", social_urls: str = "",
                   review_rating: object = "", review_count: object = "") -> str:
    """Brand & Social inputs — separate A–F track. Socials auto-discover from the
    website; these let the analyst supply what public pages don't expose."""
    return f"""
        <details class="social-block">
          <summary>Brand &amp; Social signals (optional — scored as a separate grade)</summary>
          <p class="muted" style="margin:8px 0 4px">Social profiles auto-discover from the website; add anything below to sharpen the Brand &amp; Social score. Public follower counts are unreliable, so owned-list size and reviews carry the most weight.</p>
          <div class="grid2">
            <div class="field">
              <label for="email_list_size">Email/SMS list size</label>
              <input id="email_list_size" name="email_list_size" inputmode="numeric" placeholder="e.g. 45000" value="{_esc(email_list_size)}">
            </div>
            <div class="field">
              <label for="social_urls">Social profile URLs (override auto-detect)</label>
              <input id="social_urls" name="social_urls" placeholder="instagram.com/brand  tiktok.com/@brand" value="{_esc(social_urls)}">
            </div>
          </div>
          <div class="grid2">
            <div class="field">
              <label for="review_rating">Avg review rating (1–5)</label>
              <input id="review_rating" name="review_rating" inputmode="decimal" placeholder="e.g. 4.6" value="{_esc(review_rating)}">
            </div>
            <div class="field">
              <label for="review_count">Total review count</label>
              <input id="review_count" name="review_count" inputmode="numeric" placeholder="e.g. 1200" value="{_esc(review_count)}">
            </div>
          </div>
        </details>"""


def _history_table(runs: list, *, heading: str = "", empty: str = "No analyses yet.") -> str:
    head = f"<h2>{_esc(heading)}</h2>" if heading else ""
    if not runs:
        return head + f'<p class="muted">{_esc(empty)}</p>'
    rows = []
    for r in runs:
        when = (r.get("created_at") or "")[:10]
        grade = r.get("grade") or "—"
        color = _GRADE_COLORS.get(grade, "#666")
        status = r.get("status")
        link = f'/admin/executive/brand-analysis/{r["id"]}'
        if status == "error":
            grade_cell = '<span class="muted">error</span>'
            brand_cell = _esc(r.get("brand") or r.get("label"))
        else:
            grade_cell = f'<span class="grade-cell" style="color:{color}">{_esc(grade)}</span> <span class="muted">{r.get("score_100", 0)}/100</span>'
            brand_cell = f'<a href="{link}">{_esc(r.get("brand") or r.get("label") or "Brand")}</a>'
        conf = r.get("confidence") or "—"
        period = _esc(r.get("period_current") or "")
        if r.get("period_prior"):
            period += f' vs {_esc(r["period_prior"])}'
        actions = ""
        if status != "error":
            share = r.get("share_path") or ""
            actions = f'<a class="row-act" href="{link}">Open</a>'
            actions += f' <a class="row-act" href="{link}/edit">Edit</a>'
            if share:
                actions += (f' <a class="row-act" href="{_esc(share)}" target="_blank" rel="noreferrer">Public</a>'
                            f' <button type="button" class="row-act copy-link" data-path="{_esc(share)}">Copy link</button>')
        rows.append(
            f"<tr><td>{brand_cell}</td><td>{_esc(when)}</td><td>{grade_cell}</td>"
            f'<td><span class="pill conf-{_esc(conf)}">{_esc(conf)}</span></td><td>{period}</td>'
            f'<td class="row-actions">{actions}</td></tr>'
        )
    return head + f"""
      <table>
        <thead><tr><th>Brand</th><th>Date</th><th>Grade</th><th>Confidence</th><th>Periods</th><th>Actions</th></tr></thead>
        <tbody>{"".join(rows)}</tbody>
      </table>
      <script>
        document.querySelectorAll('.copy-link').forEach(function(b){{
          b.addEventListener('click', function(){{
            navigator.clipboard.writeText(window.location.origin + b.dataset.path);
            var t=b.textContent; b.textContent='Copied ✓'; setTimeout(function(){{b.textContent=t;}},1500);
          }});
        }});
      </script>
    """


# ---------------------------------------------------------------------------
# Full report sheet
# ---------------------------------------------------------------------------


def render_report(report: BrandReport, *, report_id: str = "", user: Optional[dict] = None) -> str:
    return _doc(f"Brand Analysis — {report.brand}", _report_body(report, report_id=report_id), user=user)


def render_admin_view(report: BrandReport, *, report_id: str, share_html: str,
                      share_path: str = "", user: Optional[dict] = None) -> str:
    """Admin view of a report: the exact investor landing page in a live
    preview frame, with a toolbar (Copy link, Open public, Edit & rerun,
    Download). Matches the deck review pattern."""
    import html as _html
    srcdoc = _html.escape(share_html, quote=True)
    # "Download PDF" opens the branded page in print mode (Save as PDF) — a
    # faithful copy of what's on screen, unlike the reflowed .docx.
    pdf_btn = (
        f'<a class="btn" href="{_esc(share_path)}?print=1" target="_blank" rel="noreferrer">Download PDF</a>'
        if share_path else ""
    )
    copy_btn = (
        f'<button type="button" class="btn btn--ghost copy-link" data-path="{_esc(share_path)}">Copy share link</button>'
        f'<a class="btn btn--ghost" href="{_esc(share_path)}" target="_blank" rel="noreferrer">Open public page</a>'
        if share_path else ""
    )
    body = f"""
      <span class="eyebrow">Executive Acquisition Brief</span>
      <h1>{_esc(report.brand)}</h1>
      <div class="btn-row">
        {pdf_btn}
        {copy_btn}
        <a class="btn btn--ghost" href="/admin/executive/brand-analysis/{report_id}/edit">Edit &amp; rerun</a>
        <a class="btn btn--ghost" href="/admin/executive/brand-analysis/{report_id}/download">Download .docx</a>
        <a class="btn btn--ghost" href="/admin/executive/brand-analysis">← Brand Analysis</a>
      </div>
      <iframe class="share-frame" srcdoc="{srcdoc}" title="Investor brief preview"></iframe>
      <script>
        document.querySelectorAll('.copy-link').forEach(function(b){{
          b.addEventListener('click', function(){{
            navigator.clipboard.writeText(window.location.origin + b.dataset.path);
            var t=b.textContent; b.textContent='Copied ✓'; setTimeout(function(){{b.textContent=t;}},1500);
          }});
        }});
      </script>
    """
    return _doc(f"Brand Analysis — {report.brand}", body, user=user)


def render_edit_page(row: dict, report: Optional[BrandReport] = None, *,
                     user: Optional[dict] = None, flash: str = "",
                     source_names: Optional[list] = None,
                     versions: Optional[list] = None) -> str:
    """Edit + rerun form, prefilled from the saved row. Lets the analyst manage
    the attached files (add new / remove existing) and accumulate context, then
    rerun in place (same share link)."""
    options = "".join(
        f'<option value="{k}"{" selected" if k == row.get("category") else ""}>{_esc(v)}</option>'
        for k, v in CATEGORY_LABELS.items()
    )
    flash_html = f'<div class="flash">{_esc(flash)}</div>' if flash else ""
    rid = row.get("id")

    # File manager: list the persisted uploads, each with a remove checkbox.
    files_block = ""
    if source_names:
        rows = "".join(
            f'<label class="file-row"><input type="checkbox" name="remove_files" value="{_esc(n)}"> '
            f'<span class="file-name">{_esc(n)}</span> <span class="file-rm">remove</span></label>'
            for n in source_names
        )
        files_block = f"""
        <div class="field">
          <label>Attached files ({len(source_names)})</label>
          <div class="muted" style="margin:-2px 0 8px">Tick a file to drop it from the next run. Untouched files are re-analysed automatically.</div>
          <div class="file-list">{rows}</div>
        </div>"""

    body = f"""
      <span class="eyebrow">Executive · Brand Analysis</span>
      <h1>Edit &amp; rerun — {_esc(row.get("brand") or "Brand")}</h1>
      <p class="muted">Manage the attached files, add context, then rerun. The analysis updates in place and your existing share link stays live.</p>
      {flash_html}
      <form method="post" action="/admin/executive/brand-analysis/{rid}/rerun" enctype="multipart/form-data">
        {files_block}
        <div class="drop">
          <strong>Add more financial files (optional)</strong>
          <div class="muted">New files are analysed together with the ones you keep.</div>
          <input type="file" name="files" multiple accept=".xlsx,.xls,.csv,.pdf">
        </div>
        <div class="grid2">
          <div class="field">
            <label for="brand">Brand name</label>
            <input id="brand" name="brand" value="{_esc(row.get("brand") or "")}">
          </div>
          <div class="field">
            <label for="category">Category / business model</label>
            <select id="category" name="category">{options}</select>
          </div>
        </div>
        <div class="grid2">
          <div class="field">
            <label for="brand_website">Brand website (for logo &amp; product imagery)</label>
            <input id="brand_website" name="brand_website" placeholder="luxmery.com" value="{_esc(row.get("brand_website") or "")}">
          </div>
          <div class="field"></div>
        </div>
        <div class="field">
          <label for="context_notes">Context notes (accumulates — what you've learned about this brand)</label>
          <textarea id="context_notes" name="context_notes" rows="5" placeholder="e.g. Doggyvers Ltd is the legal entity; related-party loan is owner financing; Q1 actuals pending.">{_esc(row.get("context_notes") or "")}</textarea>
        </div>
        {_social_fields(email_list_size=row.get("email_list_size") or "", social_urls=row.get("social_urls") or "", review_rating=row.get("review_rating") or "", review_count=row.get("review_count") or "")}
        {_override_fields(row)}
        <div class="btn-row">
          <button class="btn" type="submit">Rerun analysis</button>
          <a class="btn btn--ghost" href="/admin/executive/brand-analysis/{rid}">← Back to report</a>
        </div>
      </form>
      {_version_history(versions)}
    """
    return _doc("Edit — Brand Analysis", body, user=user)


def _report_body(r: BrandReport, *, report_id: str = "") -> str:
    color = _GRADE_COLORS.get(r.scorecard.letter, "#666")
    detected = ", ".join(r.detected_brands) if r.detected_brands else "—"
    periods = _esc(r.period_current_label)
    if r.has_yoy:
        periods += f" vs {_esc(r.period_prior_label)}"
    else:
        periods += " (single period — no prior year)"
    dl = f"""<a class="btn" href="/admin/executive/brand-analysis/{report_id}/download">Download .docx</a>""" if report_id else ""

    return f"""
      <span class="eyebrow">Executive Acquisition Report</span>
      <h1>{_esc(r.brand)}</h1>
      <p class="muted">Category: {_esc(CATEGORY_LABELS.get(r.category, r.category))} · Detected: {_esc(detected)} · Periods: {periods}{(' · Prepared ' + _esc(r.prepared_date)) if r.prepared_date else ''}</p>
      <div class="btn-row">{dl}<a class="btn btn--ghost" href="/admin/executive/brand-analysis">← Brand Analysis</a></div>

      <!-- 0. Grade banner -->
      <div class="grade-banner" style="background:{color}">
        <div class="grade-letter">{_esc(r.scorecard.letter)}</div>
        <div>
          <div class="grade-score">Weighted score {r.scorecard.score_100}/100 · Recommendation: {_esc(r.recommendation)}</div>
          <div class="grade-verdict">{_esc(r.verdict_text)}</div>
        </div>
      </div>
      {_missing_block(r)}

      <h2>1. Executive Summary</h2>
      <p>{_esc(r.executive_summary)}</p>
      <p class="muted" style="margin-bottom:4px;"><strong>What stands out beyond standard KPIs</strong></p>
      <ul class="stands-out">{"".join(f"<li>{_esc(s)}</li>" for s in r.stands_out)}</ul>

      <h2>2. Financial Overview (YoY)</h2>
      {_yoy_table(r)}
      {_monthly_bars(r)}

      <h2>3. Acquisition Evaluation</h2>
      {_acquisition_evaluation(r)}

      <h2>4. Media Mix</h2>
      {_media_table(r)}

      <h2>5. Contribution &amp; Unit Economics</h2>
      {_contribution_table(r)}

      <h2>6. Balance Sheet &amp; Earnings Quality</h2>
      {_balance_table(r)}

      <h2>7. Red Flags</h2>
      {_red_flags_table(r)}

      <h2>8. Category Benchmarks</h2>
      {_benchmarks_table(r)}

      <h2>Weighted Scorecard</h2>
      {_scorecard_table(r)}

      <h2>9. Data Gaps to Close</h2>
      <ul class="stands-out">{"".join(f"<li>{_esc(g)}</li>" for g in r.data_gaps)}</ul>

      <h2>10. Verdict</h2>
      <p><strong>Grade {_esc(r.scorecard.letter)} ({r.scorecard.score_100}/100) — {_esc(r.recommendation)}.</strong> {_esc(r.verdict_text)}</p>
      <p class="muted">Source: derived from uploaded financial statements. Narrative: {_esc(r.narrative_model)}. {_esc(r.intake_summary)}</p>
    """


def _missing_block(r: BrandReport) -> str:
    conf_pill = f'<span class="pill conf-{_esc(r.confidence)}">Confidence: {_esc(r.confidence)}</span>'
    if r.data_sufficient:
        return f'<div class="missing"><span class="sufficient">Data sufficient — grade reflects complete financial inputs.</span> &nbsp; {conf_pill}</div>'
    items = "".join(f"<li>{_esc(m)}</li>" for m in r.missing_data)
    return f"""
      <div class="missing">
        <h3>Missing data that would raise confidence &nbsp; {conf_pill}</h3>
        <ul>{items}</ul>
      </div>
    """


def _yoy_row(label: str, cur: str, prior: str, has_yoy: bool) -> str:
    prior_cell = f'<td class="num">{prior}</td>' if has_yoy else ""
    return f'<tr><td>{label}</td><td class="num">{cur}</td>{prior_cell}</tr>'


def _yoy_table(r: BrandReport) -> str:
    c, p = r.current, r.prior
    prior_head = f'<th class="num">{_esc(r.period_prior_label)}</th>' if r.has_yoy else ""
    growth = "—" if r.yoy_revenue_growth_bps is None else fmt_pct(r.yoy_revenue_growth_bps)
    rows = [
        _yoy_row("Net revenue", fmt_money(c.net_revenue_cents), fmt_money(p.net_revenue_cents), r.has_yoy),
        _yoy_row("COGS", fmt_money(c.cogs_cents), fmt_money(p.cogs_cents), r.has_yoy),
        _yoy_row("Product gross profit", fmt_money(c.product_gross_profit_cents), fmt_money(p.product_gross_profit_cents), r.has_yoy),
        _yoy_row("Product GM%", fmt_pct(c.product_gm_bps), fmt_pct(p.product_gm_bps), r.has_yoy),
        _yoy_row("Marketing spend", fmt_money(c.marketing_total_cents), fmt_money(p.marketing_total_cents), r.has_yoy),
        _yoy_row("Marketing % of revenue", fmt_pct(c.marketing_pct_bps), fmt_pct(p.marketing_pct_bps), r.has_yoy),
        _yoy_row("Blended MER", fmt_mult(c.blended_mer), fmt_mult(p.blended_mer), r.has_yoy),
        _yoy_row("Contribution (reported GP)", fmt_money(c.reported_gross_profit_cents), fmt_money(p.reported_gross_profit_cents), r.has_yoy),
        _yoy_row("Contribution margin", fmt_pct(c.contribution_margin_bps), fmt_pct(p.contribution_margin_bps), r.has_yoy),
        _yoy_row("Operating expenses", fmt_money(c.opex_cents), fmt_money(p.opex_cents), r.has_yoy),
        _yoy_row("Net earnings", fmt_money(c.net_earnings_cents), fmt_money(p.net_earnings_cents), r.has_yoy),
        _yoy_row("Net margin", fmt_pct(c.net_margin_bps), fmt_pct(p.net_margin_bps), r.has_yoy),
    ]
    yoy_note = f'<p class="muted">YoY net-revenue growth: <strong>{growth}</strong>.</p>' if r.has_yoy else '<p class="muted">Single period — no prior-year comparison available.</p>'
    return f"""
      <table>
        <thead><tr><th>Metric</th><th class="num">{_esc(r.period_current_label)}</th>{prior_head}</tr></thead>
        <tbody>{"".join(rows)}</tbody>
      </table>
      {yoy_note}
    """


def _monthly_bars(r: BrandReport) -> str:
    data = r.monthly_revenue or []
    if not data:
        return '<p class="muted">Monthly revenue trajectory: not derivable from the supplied data (no monthly GL/P&amp;L columns).</p>'
    vals = [v for _, v in data if isinstance(v, (int, float))]
    peak = max(vals) if vals else 1
    bars = "".join(
        f'<div class="bar" style="height:{max(3, round((v / peak) * 100))}%" title="{_esc(lbl)}: {fmt_money(v)}"></div>'
        for lbl, v in data
    )
    labels = "".join(f"<span>{_esc(str(lbl)[:3])}</span>" for lbl, _ in data)
    return f'<p class="muted" style="margin-bottom:2px;">Monthly revenue trajectory</p><div class="bars">{bars}</div><div class="bar-labels">{labels}</div>'


def _acquisition_evaluation(r: BrandReport) -> str:
    c = r.current
    p = r.prior
    bm = benchmarks_for(r.category)
    acq_cur = r.acquisition_current
    acq_pri = r.acquisition_prior

    def _vd(passed):
        if passed is True:
            return '<span class="pass">PASS</span>'
        if passed is False:
            return '<span class="fail">FAIL</span>'
        return '<span class="gap">—</span>'

    def _val(v):
        return _esc(str(v)) if v is not None else '<span class="gap">Data gap</span>'

    # Grade badge
    acq_dim = next((d for d in r.scorecard.dimensions if d.key == "acquisition"), None)
    color = _GRADE_COLORS.get(acq_dim.letter, "#666") if acq_dim else "#666"
    badge_html = ""
    if acq_dim:
        badge_html = (
            f'<div style="display:flex;align-items:flex-start;gap:14px;margin-bottom:18px;">'
            f'<div style="font-size:36px;font-weight:900;color:{color};font-family:Montserrat,sans-serif;'
            f'line-height:1;min-width:48px;text-align:center;">{_esc(acq_dim.letter)}</div>'
            f'<div><strong style="font-size:14px;">Acquisition mix &amp; dependency &nbsp;·&nbsp; 12% weight</strong>'
            f'<p class="muted" style="margin:4px 0 0;">{_esc(acq_dim.reason)}</p></div>'
            f'</div>'
        )

    # ── 3a. Customer Revenue Split ──────────────────────────────────────────
    new_r = acq_cur.get("new_customer_revenue_cents")
    ret_r = acq_cur.get("returning_customer_revenue_cents")
    new_r_p = acq_pri.get("new_customer_revenue_cents")
    ret_r_p = acq_pri.get("returning_customer_revenue_cents")
    coh_total = (new_r or 0) + (ret_r or 0) if (new_r is not None or ret_r is not None) else None
    coh_total_p = (new_r_p or 0) + (ret_r_p or 0) if (new_r_p is not None or ret_r_p is not None) else None
    ret_pct_bps = round(ret_r / coh_total * 10000) if (ret_r is not None and coh_total) else None
    ret_pct_bps_p = round(ret_r_p / coh_total_p * 10000) if (ret_r_p is not None and coh_total_p) else None
    new_pct_bps = round(new_r / coh_total * 10000) if (new_r is not None and coh_total) else None
    new_pct_bps_p = round(new_r_p / coh_total_p * 10000) if (new_r_p is not None and coh_total_p) else None

    yoy_col = r.has_yoy
    yoy_th = f'<th class="num">{_esc(r.period_prior_label)}</th>' if yoy_col else ""

    def _split_row(label, cur_v, pri_v, healthy, passed):
        yoy_td = f'<td class="num">{cur_v if pri_v is None else _esc(str(pri_v) if pri_v else "—")}</td>' if yoy_col else ""
        # re-order: cur_v is always current
        yoy_cell = f'<td class="num">{_esc(str(pri_v)) if pri_v is not None else "—"}</td>' if yoy_col else ""
        return (
            f"<tr><td>{label}</td>"
            f'<td class="num">{_esc(str(cur_v)) if cur_v is not None else "—"}</td>'
            + yoy_cell +
            f'<td>{_esc(str(healthy))}</td><td>{_vd(passed)}</td></tr>'
        )

    split_rows = (
        _split_row("New-customer revenue", fmt_money(new_r), fmt_money(new_r_p) if yoy_col else None, "—", None) +
        _split_row("Returning-customer revenue", fmt_money(ret_r), fmt_money(ret_r_p) if yoy_col else None, "—", None) +
        _split_row("Returning-customer share", fmt_pct(ret_pct_bps), fmt_pct(ret_pct_bps_p) if yoy_col else None,
                   "≥ 30%", None if ret_pct_bps is None else ret_pct_bps >= 3000) +
        _split_row("New-customer share", fmt_pct(new_pct_bps), fmt_pct(new_pct_bps_p) if yoy_col else None, "—", None) +
        _split_row("Owned-channel (email/SMS) %",
                   fmt_pct(c.owned_pct_bps), fmt_pct(p.owned_pct_bps) if yoy_col else None,
                   f"{bm.owned_pct_bps[0]//100}–{bm.owned_pct_bps[1]//100}%",
                   None if c.owned_pct_bps is None else c.owned_pct_bps >= bm.owned_pct_bps[0])
    )
    split_table = (
        f'<p style="font-weight:700;margin:16px 0 8px;">Customer Revenue Split</p>'
        f'<table><thead><tr><th>Metric</th><th class="num">{_esc(r.period_current_label)}</th>'
        f'{yoy_th}<th>Healthy</th><th>Verdict</th></tr></thead>'
        f'<tbody>{split_rows}</tbody></table>'
    )

    # ── 3b. Retention & Pricing Signals ────────────────────────────────────
    sig_rows = (
        _split_row("Discount rate", fmt_pct(c.discount_rate_bps), fmt_pct(p.discount_rate_bps) if yoy_col else None,
                   f"{bm.discount_rate_bps[0]//100}–{bm.discount_rate_bps[1]//100}%",
                   None if c.discount_rate_bps is None else c.discount_rate_bps <= bm.discount_rate_bps[1]) +
        _split_row("Return rate", fmt_pct(c.return_rate_bps), fmt_pct(p.return_rate_bps) if yoy_col else None,
                   f"< {bm.return_rate_max_bps//100}%",
                   None if c.return_rate_bps is None else c.return_rate_bps < bm.return_rate_max_bps) +
        _split_row("Blended MER", fmt_mult(c.blended_mer), fmt_mult(p.blended_mer) if yoy_col else None,
                   f"≥ {bm.blended_mer_min:.1f}x",
                   None if c.blended_mer is None else c.blended_mer >= bm.blended_mer_min) +
        _split_row("Marketing % of revenue", fmt_pct(c.marketing_pct_bps), fmt_pct(p.marketing_pct_bps) if yoy_col else None,
                   f"{bm.marketing_pct_bps[0]//100}–{bm.marketing_pct_bps[1]//100}%",
                   None if c.marketing_pct_bps is None else
                   bm.marketing_pct_bps[0] <= c.marketing_pct_bps <= bm.marketing_pct_bps[1])
    )
    sig_table = (
        f'<p style="font-weight:700;margin:16px 0 8px;">Retention &amp; Pricing Signals</p>'
        f'<table><thead><tr><th>Signal</th><th class="num">{_esc(r.period_current_label)}</th>'
        f'{yoy_th}<th>Healthy</th><th>Verdict</th></tr></thead>'
        f'<tbody>{sig_rows}</tbody></table>'
    )

    # ── 3c. Unit Economics ──────────────────────────────────────────────────
    aov = acq_cur.get("aov_cents")
    cac = acq_cur.get("cac_cents")
    ltv = acq_cur.get("ltv_cents")
    ltv_cac = safe_div(ltv, cac) if (ltv is not None and cac) else None
    _gap = '<span class="gap">Data gap — not supplied</span>'
    _gap_ltv = '<span class="gap">Data gap — need LTV and CAC</span>'
    _aov_v = fmt_money(aov) if aov is not None else _gap
    _cac_v = fmt_money(cac) if cac is not None else _gap
    _ltv_v = fmt_money(ltv) if ltv is not None else _gap
    if ltv_cac is not None:
        _ltv_badge = f'<span class="pass">≥ 3x ✓</span>' if ltv_cac >= 3 else '<span class="fail">(healthy ≥ 3x)</span>'
        _ltv_cac_v = f"{ltv_cac:.1f}x {_ltv_badge}"
    else:
        _ltv_cac_v = _gap_ltv
    ue_rows_html = (
        f"<tr><td>AOV (average order value)</td><td>{_aov_v}</td></tr>"
        f"<tr><td>CAC (customer acquisition cost)</td><td>{_cac_v}</td></tr>"
        f"<tr><td>LTV (customer lifetime value)</td><td>{_ltv_v}</td></tr>"
        f"<tr><td>LTV : CAC ratio</td><td>{_ltv_cac_v}</td></tr>"
    )
    ue_table = (
        f'<p style="font-weight:700;margin:16px 0 8px;">Unit Economics</p>'
        f'<table><thead><tr><th>Metric</th><th>Value</th></tr></thead>'
        f'<tbody>{ue_rows_html}</tbody></table>'
    )

    return badge_html + split_table + sig_table + ue_table


def _media_table(r: BrandReport) -> str:
    ch = r.media_mix or {}
    if not ch:
        return '<p class="muted">Channel-level media mix not supplied — request ad-platform exports (Meta, Google, TikTok) for spend by channel and concentration read-through.</p>'
    total = sum(ch.values()) or 1
    top_share = max(ch.values()) / total
    rows = "".join(
        f'<tr><td>{_esc(k)}</td><td class="num">{fmt_money(v)}</td><td class="num">{v/total*100:.0f}%</td></tr>'
        for k, v in sorted(ch.items(), key=lambda kv: kv[1], reverse=True)
    )
    note = f'<p class="muted">Top channel is {top_share*100:.0f}% of spend — {"high concentration risk" if top_share > 0.7 else "reasonably diversified"}.</p>'
    return f'<table><thead><tr><th>Channel</th><th class="num">Spend</th><th class="num">% allocation</th></tr></thead><tbody>{rows}</tbody></table>{note}'


def _contribution_table(r: BrandReport) -> str:
    c = r.current
    rows = [
        ("Contribution margin", fmt_pct(c.contribution_margin_bps)),
        ("Discount rate", fmt_pct(c.discount_rate_bps)),
        ("Operating result excl. other income", fmt_money(c.operating_result_ex_other_cents)),
        ("Net earnings (reported)", fmt_money(c.net_earnings_cents)),
        ("AOV / CAC / LTV / payback", "Data gap — not supplied"),
    ]
    body = "".join(f"<tr><td>{l}</td><td>{_esc(v)}</td></tr>" for l, v in rows)
    return f'<table><thead><tr><th>Metric</th><th>Value</th></tr></thead><tbody>{body}</tbody></table>'


def _balance_table(r: BrandReport) -> str:
    bs = r.balance_sheet or []
    if not bs:
        return '<p class="muted">Balance sheet not supplied — request assets, intercompany balances, cash/inventory, equity, dividends and related-party agreements to assess earnings quality.</p>'
    rows = "".join(f'<tr><td>{_esc(l)}</td><td class="num">{fmt_money(v)}</td></tr>' for l, v in bs)
    flag = '<p class="fail">Related-party / intercompany items detected — diligence collectability and agreements.</p>' if r.related_party_flag else ""
    return f'<table><thead><tr><th>Line</th><th class="num">Amount</th></tr></thead><tbody>{rows}</tbody></table>{flag}'


def _red_flags_table(r: BrandReport) -> str:
    if not r.red_flags:
        return '<p class="sufficient">No material red flags surfaced in the supplied data.</p>'
    rows = "".join(
        f'<tr><td class="sev" style="color:{_SEV_COLORS.get(f.severity, "#666")}">{_esc(f.severity)}</td>'
        f"<td><strong>{_esc(f.title)}</strong><br><span class=\"muted\">{_esc(f.detail)}</span></td></tr>"
        for f in r.red_flags
    )
    return f'<table><thead><tr><th>Severity</th><th>Finding</th></tr></thead><tbody>{rows}</tbody></table>'


def _benchmarks_table(r: BrandReport) -> str:
    rows = []
    for b in r.benchmarks:
        if b.passed is True:
            verdict = '<span class="pass">PASS</span>'
        elif b.passed is False:
            verdict = '<span class="fail">FAIL</span>'
        else:
            verdict = '<span class="gap">data gap</span>'
        rows.append(f"<tr><td>{_esc(b.kpi)}</td><td>{_esc(b.healthy)}</td><td>{_esc(b.actual)}</td><td>{verdict}</td></tr>")
    return f'<table><thead><tr><th>KPI</th><th>Healthy range</th><th>Brand actual</th><th>Verdict</th></tr></thead><tbody>{"".join(rows)}</tbody></table>'


def _scorecard_table(r: BrandReport) -> str:
    rows = []
    for d in r.scorecard.dimensions:
        color = _GRADE_COLORS.get(d.letter, "#666")
        rows.append(
            f'<tr><td>{_esc(d.label)}</td><td class="num">{int(d.weight*100)}%</td>'
            f'<td class="grade-cell" style="color:{color}">{_esc(d.letter)}</td><td>{_esc(d.reason)}</td></tr>'
        )
    return f"""
      <table>
        <thead><tr><th>Dimension</th><th class="num">Weight</th><th>Grade</th><th>Reason</th></tr></thead>
        <tbody>{"".join(rows)}</tbody>
      </table>
      <p class="muted">Weighted composite (A=4…F=0, rebased to 100): <strong>{r.scorecard.score_100}/100 → {_esc(r.scorecard.letter)}</strong>.</p>
    """
