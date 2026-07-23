"""Public, no-auth intake guide landing page for Brand Analysis.

Served at /brand-intake (and /brand-intake?print=1 for the browser-print PDF
flow). Self-contained HTML — no admin chrome, no login required.
Styled to match the Anata brand guide (share_page.py palette + typography).
"""

from __future__ import annotations

from sales_support_agent.services.public_report_ui import (
    PUBLIC_REPORT_DESIGN_VERSION,
    public_report_foundation_css,
)

import base64
import os

_FONTS = (
    "https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&"
    "family=Montserrat:wght@700;800;900&display=swap"
)


def _wordmark_src() -> str:
    """Return a data-URI for wordmark.png, or '' if the file is missing."""
    assets_dir = os.path.normpath(
        os.path.join(os.path.dirname(__file__), "..", "..", "..", "shared", "anata_brand", "assets")
    )
    path = os.path.join(assets_dir, "wordmark.png")
    try:
        with open(path, "rb") as fh:
            return f"data:image/png;base64,{base64.b64encode(fh.read()).decode()}"
    except OSError:
        return ""


_CSS = """
:root {
  --navy:   #2B3644;
  --blue:   #85BBDA;
  --blue-d: #4f84c4;
  --brown:  #BFA889;
  --cream:  #F9F7F3;
  --white:  #fff;
  --line:   rgba(43,54,68,.10);
  --shadow: rgba(43,54,68,.10);
  --muted:  rgba(43,54,68,.55);
  --good:   #1A7F4B;
  --rec:    #1565C0;
}
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
body {
  font-family: Inter, "Segoe UI", sans-serif;
  background: var(--cream);
  color: var(--navy);
  -webkit-font-smoothing: antialiased;
}

/* ── header ────────────────────────────────── */
.site-header {
  background: var(--navy);
  padding: 18px 40px;
  display: flex;
  align-items: center;
  justify-content: space-between;
}
.wordmark { height: 28px; display: block; filter: brightness(0) invert(1); }
.wordmark-fallback {
  font-family: Montserrat, sans-serif;
  font-weight: 900;
  font-size: 20px;
  letter-spacing: -.01em;
  color: #fff;
}
.wordmark-fallback span { color: var(--blue); }
.pdf-btn {
  display: inline-flex;
  align-items: center;
  gap: 7px;
  background: var(--blue-d);
  color: #fff;
  font-size: 13px;
  font-weight: 600;
  padding: 8px 18px;
  border-radius: 999px;
  text-decoration: none;
  letter-spacing: .01em;
  white-space: nowrap;
  transition: opacity .15s;
}
.pdf-btn:hover { opacity: .85; }

/* ── page wrapper ─────────────────────────── */
.page { max-width: 900px; margin: 0 auto; padding: 48px 28px 72px; }

/* ── hero ──────────────────────────────────── */
.eyebrow {
  font-family: Montserrat, sans-serif;
  font-weight: 800;
  font-size: 11px;
  letter-spacing: .14em;
  text-transform: uppercase;
  color: var(--blue-d);
  margin-bottom: 12px;
}
.hero-title {
  font-family: Montserrat, sans-serif;
  font-weight: 900;
  font-size: clamp(1.8rem, 3vw, 2.6rem);
  line-height: 1.12;
  letter-spacing: -.02em;
  color: var(--navy);
  margin-bottom: 14px;
}
.hero-sub {
  font-size: 15.5px;
  line-height: 1.65;
  color: var(--muted);
  max-width: 640px;
  margin-bottom: 36px;
}

/* ── section heading ──────────────────────── */
.section-block {
  background: var(--navy);
  border-radius: 10px 10px 0 0;
  padding: 12px 20px;
  display: flex;
  align-items: center;
  justify-content: space-between;
}
.section-block h2 {
  font-family: Montserrat, sans-serif;
  font-weight: 800;
  font-size: 14px;
  letter-spacing: .04em;
  text-transform: uppercase;
  color: #fff;
}
.badge {
  font-size: 11px;
  font-weight: 700;
  letter-spacing: .08em;
  text-transform: uppercase;
  padding: 3px 10px;
  border-radius: 999px;
}
.badge-req  { background: var(--good);  color: #fff; }
.badge-rec  { background: var(--rec);   color: #fff; }
.badge-opt  { background: rgba(255,255,255,.18); color: rgba(255,255,255,.85); }

/* ── card ─────────────────────────────────── */
.card {
  background: var(--white);
  border: 1px solid var(--line);
  border-top: none;
  border-radius: 0 0 16px 16px;
  padding: 26px 28px;
  box-shadow: 0 8px 24px var(--shadow);
  margin-bottom: 22px;
  page-break-inside: avoid;
}
.card-intro {
  font-size: 14px;
  line-height: 1.6;
  color: var(--muted);
  font-style: italic;
  margin-bottom: 18px;
}

/* ── tables ────────────────────────────────── */
.data-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 13.5px;
}
.data-table th {
  text-align: left;
  font-family: Montserrat, sans-serif;
  font-weight: 700;
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: .06em;
  color: #fff;
  background: #16213E;
  padding: 9px 12px;
  border: 1px solid rgba(255,255,255,.1);
}
.data-table td {
  padding: 9px 12px;
  border: 1px solid var(--line);
  vertical-align: top;
  line-height: 1.5;
}
.data-table tr:nth-child(odd) td { background: #fafafa; }
.data-table tr:nth-child(even) td { background: var(--white); }
.data-table td strong { color: var(--navy); }
.data-table td em { color: var(--muted); font-size: 12.5px; }

/* quick-checklist #, Required? columns */
.col-num  { width: 36px; text-align: center; color: var(--blue-d); font-weight: 700; }
.col-req  { width: 120px; text-align: center; }
.col-unlocks { color: var(--muted); font-style: italic; font-size: 12.5px; }

/* badge inside table cell */
.req-badge { display:inline-block; font-size:11px; font-weight:700; padding:2px 8px; border-radius:999px; white-space:nowrap; }
.req-yes  { background:#d1fae5; color:#065f46; }
.req-rec  { background:#dbeafe; color:#1e40af; }
.req-opt  { background:#f3f4f6; color:#6b7280; }

/* ── callout ───────────────────────────────── */
.callout {
  background: #EBF4FB;
  border-left: 4px solid var(--blue-d);
  border-radius: 0 8px 8px 0;
  padding: 14px 16px;
  margin-top: 16px;
}
.callout p {
  font-size: 13px;
  font-weight: 700;
  color: var(--navy);
  margin-bottom: 8px;
}
.callout ul {
  padding-left: 18px;
  margin: 0;
}
.callout li {
  font-size: 13px;
  color: var(--muted);
  line-height: 1.6;
  margin: 4px 0;
}

/* ── bullet list (sections 4, 5) ─────────────*/
.bullet-list {
  padding-left: 0;
  list-style: none;
  display: flex;
  flex-direction: column;
  gap: 8px;
}
.bullet-list li {
  display: flex;
  gap: 10px;
  font-size: 13.5px;
  line-height: 1.55;
}
.bullet-list li::before {
  content: "";
  display: block;
  width: 7px;
  height: 7px;
  border-radius: 50%;
  background: var(--blue-d);
  margin-top: 5px;
  flex-shrink: 0;
}

/* ── closing CTA ───────────────────────────── */
.cta-block {
  background: var(--white);
  border: 1px solid var(--line);
  border-top: 4px solid var(--blue-d);
  border-radius: 14px;
  padding: 24px 28px;
  box-shadow: 0 6px 20px var(--shadow);
  margin-top: 8px;
}
.cta-block p { font-size: 14px; line-height: 1.7; color: var(--navy); }
.cta-block a { color: var(--blue-d); font-weight: 600; text-decoration: none; }

/* ── footer ────────────────────────────────── */
.site-footer {
  background: var(--navy);
  color: rgba(255,255,255,.5);
  text-align: center;
  padding: 24px 40px;
  font-size: 12.5px;
  line-height: 1.8;
}
.site-footer a { color: var(--blue); text-decoration: none; }
.site-footer strong { color: rgba(255,255,255,.85); }

/* ── print ─────────────────────────────────── */
@media print {
  .no-print, .pdf-btn { display: none !important; }
  body { background: #fff; font-size: 11pt; }
  .site-header {
    background: var(--navy) !important;
    print-color-adjust: exact; -webkit-print-color-adjust: exact;
    padding: 12px 24px;
  }
  .section-block {
    print-color-adjust: exact; -webkit-print-color-adjust: exact;
  }
  .data-table th {
    print-color-adjust: exact; -webkit-print-color-adjust: exact;
  }
  .page { padding: 24px 18px 40px; }
  .card { box-shadow: none; margin-bottom: 14px; }
  .cta-block { box-shadow: none; }
  .site-footer { print-color-adjust: exact; -webkit-print-color-adjust: exact; }
  @page { margin: 14mm; }
}

@media (max-width: 620px) {
  .site-header { padding: 14px 18px; }
  .page { padding: 32px 16px 48px; }
  .card { padding: 18px 16px; }
  .data-table { font-size: 12px; }
  .data-table th, .data-table td { padding: 7px 8px; }
}
"""

_PDF_ICON = (
    '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" '
    'stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round">'
    '<path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/>'
    '<polyline points="14 2 14 8 20 8"/>'
    '<line x1="12" y1="18" x2="12" y2="12"/>'
    '<line x1="9" y1="15" x2="15" y2="15"/></svg>'
)


def _section(num: str, title: str, badge_label: str, badge_cls: str, intro: str, body: str) -> str:
    return f"""
<div>
  <div class="section-block">
    <h2>{num}&ensp;&ensp;{title}</h2>
    <span class="badge {badge_cls}">{badge_label}</span>
  </div>
  <div class="card">
    <p class="card-intro">{intro}</p>
    {body}
  </div>
</div>"""


def _table(headers: list[str], rows: list[list[str]], extra_class: str = "") -> str:
    ths = "".join(f"<th>{h}</th>" for h in headers)
    trs = ""
    for row in rows:
        tds = "".join(f"<td>{cell}</td>" for cell in row)
        trs += f"<tr>{tds}</tr>\n"
    return f'<table class="data-table {extra_class}"><thead><tr>{ths}</tr></thead><tbody>{trs}</tbody></table>'


def _callout(title: str, items: list[str]) -> str:
    lis = "".join(f"<li>{item}</li>" for item in items)
    return f'<div class="callout"><p>{title}</p><ul>{lis}</ul></div>'


def _bullets(items: list[str]) -> str:
    lis = "".join(f"<li>{item}</li>" for item in items)
    return f'<ul class="bullet-list">{lis}</ul>'


def _rb(label: str, cls: str) -> str:
    return f'<span class="req-badge {cls}">{label}</span>'


def render_intake_guide(print_mode: bool = False) -> str:
    wm = _wordmark_src()
    logo_html = (
        f'<img src="{wm}" alt="Anata" class="wordmark">'
        if wm else
        '<span class="wordmark-fallback">ANA<span>TA</span></span>'
    )
    pdf_btn = (
        "" if print_mode else
        f'<a href="/brand-intake?print=1" class="pdf-btn no-print" target="_blank">'
        f'{_PDF_ICON} Download PDF</a>'
    )
    print_script = (
        "<script>window.addEventListener('load',function(){window.print();});</script>"
        if print_mode else ""
    )

    # ── Section 1: Financial ─────────────────────────────────────────────────
    fin_table = _table(
        ["What to provide", "Format / Source", "Why it matters"],
        [
            [
                "<strong>Profit &amp; Loss Statement</strong> — current fiscal year",
                "QuickBooks Online, Xero, or any P&amp;L summary",
                "Revenue, profitability, gross margin, contribution margin",
            ],
            [
                "<strong>Profit &amp; Loss Statement</strong> — prior fiscal year",
                "Same source, prior year — or a two-year P&amp;L in one file",
                "Year-over-year revenue growth grade",
            ],
            [
                "<strong>Balance Sheet</strong> — most recent period",
                "QBO, Xero, or equivalent",
                "Earnings quality, equity position, related-party flags",
            ],
        ],
    )
    fin_callout = _callout(
        "Export tips",
        [
            "QBO users: Reports → Profit and Loss → set date range → Export to Excel. Include both years in one export if possible.",
            "Xero users: Accounting → Reports → Profit &amp; Loss → set date range → Export.",
            "If you only have a combined multi-year file (both years in one spreadsheet), that's fine — no need to export separately.",
            "General Ledger transaction exports are NOT needed; summary statements only.",
        ],
    )
    s1 = _section(
        "01", "Financial Documents", "Required", "badge-req",
        "These drive the core financial scorecard. We accept .xlsx, .xls, .csv, and .pdf.",
        fin_table + fin_callout,
    )

    # ── Section 2: Advertising ───────────────────────────────────────────────
    ads_table = _table(
        ["Platform", "What to export", "Notes"],
        [
            ["Meta (Facebook / Instagram)", "Ads Manager → Campaigns → Export CSV", "Full year spend by campaign"],
            ["Google Ads", "Reports → Campaigns → Download CSV", "Full year spend"],
            ["Amazon Advertising", "Sponsored Products / Brands report", "Annual totals or monthly"],
            ["TikTok Ads", "Campaigns → Export", "If applicable"],
            ["Klaviyo / Email revenue", "Revenue attribution report", "Optional but valued for owned-channel grade"],
        ],
    )
    ads_callout = _callout(
        "P&amp;L-only path",
        [
            "If your total ad spend appears as a single line in your P&amp;L (e.g. 'Total Marketing — $89,000'), you can skip individual platform exports. We'll use the P&amp;L total for efficiency scoring, but Media Mix concentration will remain N/A.",
        ],
    )
    s2 = _section(
        "02", "Advertising &amp; Marketing Data", "Recommended", "badge-rec",
        "Unlocks the Marketing Efficiency and Media Mix grades. Without these, both dimensions score N/A.",
        ads_table + ads_callout,
    )

    # ── Section 3: Brand & Social ────────────────────────────────────────────
    social_table = _table(
        ["Item", "Example / Notes"],
        [
            ["Brand website URL", "<em>https://yourbrand.com</em>"],
            ["Instagram handle", "<em>@yourbrand</em>"],
            ["TikTok handle", "<em>@yourbrand</em>"],
            ["Facebook page URL", "<em>facebook.com/yourbrand</em>"],
            ["YouTube channel URL", "<em>youtube.com/c/yourbrand</em>"],
            ["Pinterest URL", "<em>pinterest.com/yourbrand</em>"],
            ["<strong>Email list size</strong> (approximate)", "<em>45,000 subscribers</em>"],
            ["<strong>Review platform + rating + count</strong>", "<em>Amazon 4.6★ / 1,200 reviews</em>"],
            ["Trustpilot / Google rating + count", "<em>4.8★ / 320 reviews</em>"],
        ],
    )
    s3 = _section(
        "03", "Brand &amp; Social", "Recommended", "badge-rec",
        "Used for the Brand &amp; Social scorecard, which is graded separately from the financial track.",
        social_table,
    )

    # ── Section 4: Customer Data ─────────────────────────────────────────────
    cust_bullets = _bullets([
        "New customer revenue vs. returning customer revenue (annual split, $ or %)",
        "Owned / email channel revenue as % of total revenue",
        "Average Order Value (AOV)",
        "Customer Acquisition Cost (CAC) — blended or by channel",
        "Lifetime Value (LTV) — 12-month or full cohort",
        "Return / refund rate (% of gross sales)",
        "Discount / promotion rate (% of gross sales)",
    ])
    s4 = _section(
        "04", "Customer &amp; Acquisition Data", "Optional", "badge-opt",
        "This dimension is frequently N/A without a CRM or cohort export. Provide what you have — partial data is better than none.",
        cust_bullets,
    )

    # ── Section 5: Context Notes ─────────────────────────────────────────────
    ctx_bullets = _bullets([
        "Significant one-time events (warehouse move, product recall, major relaunch)",
        "Seasonality patterns (e.g. 70% of revenue in Q4)",
        "Reason for revenue change year-over-year",
        "Pending contracts, LOIs, or distribution deals not yet reflected in financials",
        "Ownership structure notes (family loans, intercompany transactions, related-party leases)",
    ])
    s5 = _section(
        "05", "Context Notes", "Optional", "badge-opt",
        "A few sentences from your team helps us interpret the numbers accurately. Useful things to note:",
        ctx_bullets,
    )

    # ── Section 6: Quick Checklist ───────────────────────────────────────────
    cl_table = _table(
        ["#", "Item", "Required?", "Unlocks"],
        [
            ["1",  "P&amp;L — current fiscal year",               _rb("✅ Required",    "req-yes"), "Revenue, Profit, Gross Margin, Contribution"],
            ["2",  "P&amp;L — prior fiscal year",                 _rb("✅ Required",    "req-yes"), "Year-over-year Revenue Growth grade"],
            ["3",  "Balance Sheet",                               _rb("✅ Required",    "req-yes"), "Earnings Quality / Balance grade"],
            ["4",  "Ad platform exports (Meta, Google, etc.)",    _rb("Recommended", "req-rec"),  "Marketing Efficiency + Media Mix grades"],
            ["5",  "Brand website URL",                           _rb("Recommended", "req-rec"),  "Social discovery, brand imagery"],
            ["6",  "Social handles + email list size",            _rb("Recommended", "req-rec"),  "Brand &amp; Social scorecard"],
            ["7",  "Review ratings + review count",               _rb("Recommended", "req-rec"),  "Social Reputation grade"],
            ["8",  "New vs. returning revenue split",             _rb("Optional",    "req-opt"),  "Acquisition Mix grade"],
            ["9",  "AOV, CAC, Lifetime Value",                    _rb("Optional",    "req-opt"),  "Acquisition Mix grade"],
            ["10", "Context notes",                               _rb("Optional",    "req-opt"),  "Analyst interpretation accuracy"],
        ],
        "checklist-tbl",
    )
    s6 = _section(
        "06", "Quick Checklist at a Glance", "", "badge-opt",
        "Everything at a glance. Required items are needed for a complete graded report.",
        cl_table,
    )

    cta = """
<div class="cta-block">
  <p>
    <strong>Ready to submit?</strong> Upload your files at
    <a href="https://agent.anatainc.com/admin/executive/brand-analysis" target="_blank">agent.anatainc.com</a>
    → Executive → Brand Analysis → Start new analysis
  </p>
  <p style="margin-top:8px">
    <strong>Questions?</strong> Contact your Anata analyst or email
    <a href="mailto:david@anatainc.com">david@anatainc.com</a>
  </p>
</div>"""

    return f"""<!DOCTYPE html>
<html lang="en" data-design-system="{PUBLIC_REPORT_DESIGN_VERSION}">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Brand Intake Checklist — Anata</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="stylesheet" href="{_FONTS}">
<style>{public_report_foundation_css()}</style>
<style>{_CSS}</style>
</head>
<body>
<a class="public-report-skip" href="#brand-intake">Skip to intake checklist</a>

<header class="site-header">
  {logo_html}
  {pdf_btn}
</header>

<main id="brand-intake" class="page">
  <div class="eyebrow">Brand Acquisition</div>
  <h1 class="hero-title">Brand Intake Checklist</h1>
  <p class="hero-sub">
    To complete your acquisition analysis, please provide the items below. Documents can be
    uploaded directly at <strong>agent.anatainc.com</strong> or sent to your Anata contact.
    The more complete your submission, the more dimensions of the scorecard we can grade —
    missing data is noted as N/A rather than penalized arbitrarily.
  </p>

  {s1}
  {s2}
  {s3}
  {s4}
  {s5}
  {s6}
  {cta}
</main>

<footer class="site-footer">
  <strong>Anata Inc.</strong><br>
  Questions? Email <a href="mailto:david@anatainc.com">david@anatainc.com</a>
  &nbsp;|&nbsp; Upload at <a href="https://agent.anatainc.com">agent.anatainc.com</a>
</footer>

{print_script}
</body>
</html>"""
