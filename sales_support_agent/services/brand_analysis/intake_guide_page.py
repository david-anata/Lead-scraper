"""Public, no-auth intake guide landing page for Brand Analysis.

Served at /brand-intake (and /brand-intake?print=1 for the browser-print PDF
flow). Self-contained HTML — no admin chrome, no login required.
"""

from __future__ import annotations

_FONTS = (
    "https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&"
    "family=Montserrat:wght@700;800;900&display=swap"
)

_CSS = """
:root {
  --ink:       #1d2d44;
  --ink-soft:  #314664;
  --sky:       #85bbda;
  --sky-deep:  #4f84c4;
  --sand:      #bfa889;
  --sand-soft: #f7f3ec;
  --paper:     #fffdf9;
  --muted:     #6b7688;
  --line:      rgba(29,45,68,.12);
  --shadow:    rgba(29,45,68,.10);
}
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
body {
  font-family: Inter, system-ui, sans-serif;
  background: linear-gradient(180deg, #eef5fb 0%, #f7f3ec 100%) fixed;
  color: var(--ink);
  min-height: 100vh;
  -webkit-font-smoothing: antialiased;
}

/* ── header ────────────────────────────────── */
.site-header {
  background: var(--ink);
  padding: 20px 40px;
  display: flex;
  align-items: center;
  justify-content: space-between;
}
.logo {
  font-family: Montserrat, sans-serif;
  font-weight: 900;
  font-size: 22px;
  letter-spacing: -0.02em;
  color: #fff;
  text-decoration: none;
}
.logo span { color: var(--sky); }
.header-cta {
  background: var(--sky-deep);
  color: #fff;
  font-size: 13px;
  font-weight: 600;
  padding: 9px 20px;
  border-radius: 999px;
  text-decoration: none;
  letter-spacing: 0.01em;
  transition: opacity .15s;
}
.header-cta:hover { opacity: .85; }

/* ── hero ──────────────────────────────────── */
.hero {
  max-width: 860px;
  margin: 0 auto;
  padding: 60px 40px 36px;
  text-align: center;
}
.eyebrow {
  font-size: 11px;
  font-weight: 800;
  letter-spacing: .18em;
  text-transform: uppercase;
  color: var(--sky-deep);
  margin-bottom: 14px;
}
.hero h1 {
  font-family: Montserrat, sans-serif;
  font-weight: 800;
  font-size: clamp(1.8rem, 3.5vw, 2.8rem);
  color: var(--ink);
  line-height: 1.15;
  margin-bottom: 18px;
}
.hero p {
  font-size: 16px;
  line-height: 1.65;
  color: var(--ink-soft);
  max-width: 620px;
  margin: 0 auto 32px;
}
.pdf-btn {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  background: var(--ink);
  color: #fff;
  font-size: 14px;
  font-weight: 600;
  padding: 12px 26px;
  border-radius: 999px;
  text-decoration: none;
  letter-spacing: 0.01em;
  box-shadow: 0 8px 24px var(--shadow);
  transition: opacity .15s;
}
.pdf-btn:hover { opacity: .85; }
.pdf-btn svg { flex-shrink: 0; }

/* ── content grid ─────────────────────────── */
.content {
  max-width: 860px;
  margin: 0 auto;
  padding: 8px 40px 60px;
}
.section-label {
  font-size: 11px;
  font-weight: 800;
  letter-spacing: .14em;
  text-transform: uppercase;
  color: var(--muted);
  margin: 36px 0 14px;
}

/* ── slide cards ──────────────────────────── */
.slide {
  background: #fff;
  border-radius: 20px;
  padding: 30px 32px;
  box-shadow: 0 6px 24px var(--shadow);
  margin-bottom: 16px;
  border: 1px solid var(--line);
  page-break-inside: avoid;
}
.slide-header {
  display: flex;
  align-items: center;
  gap: 14px;
  margin-bottom: 16px;
}
.slide-icon {
  width: 40px;
  height: 40px;
  border-radius: 10px;
  background: var(--sand-soft);
  display: flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
}
.slide-icon svg { color: var(--sky-deep); }
.slide h2 {
  font-family: Montserrat, sans-serif;
  font-weight: 700;
  font-size: 17px;
  color: var(--ink);
}
.slide p.desc {
  font-size: 13.5px;
  line-height: 1.6;
  color: var(--ink-soft);
  margin-bottom: 16px;
}
.file-list {
  list-style: none;
  display: flex;
  flex-direction: column;
  gap: 8px;
}
.file-list li {
  display: flex;
  gap: 10px;
  font-size: 13px;
  line-height: 1.5;
}
.file-list li::before {
  content: "";
  display: block;
  width: 6px;
  height: 6px;
  border-radius: 50%;
  background: var(--sky-deep);
  margin-top: 6px;
  flex-shrink: 0;
}
.file-list .item-name {
  font-weight: 600;
  color: var(--ink);
  margin-right: 4px;
}
.file-list .item-why {
  color: var(--muted);
}
.tag {
  display: inline-block;
  font-size: 10px;
  font-weight: 700;
  letter-spacing: .1em;
  text-transform: uppercase;
  padding: 2px 8px;
  border-radius: 999px;
  margin-left: 6px;
  vertical-align: middle;
}
.tag-required { background: #fef2f2; color: #991b1b; }
.tag-optional { background: #f0fdf4; color: #166534; }

/* ── quick checklist table ────────────────── */
.checklist-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 8px 24px;
  margin-top: 4px;
}
.checklist-item {
  display: flex;
  align-items: flex-start;
  gap: 9px;
  font-size: 12.5px;
  color: var(--ink-soft);
  line-height: 1.4;
}
.checklist-item .box {
  width: 15px;
  height: 15px;
  border: 2px solid var(--sky-deep);
  border-radius: 3px;
  flex-shrink: 0;
  margin-top: 1px;
}

/* ── footer ────────────────────────────────── */
.site-footer {
  background: var(--ink);
  color: rgba(255,255,255,.6);
  text-align: center;
  padding: 28px 40px;
  font-size: 13px;
  line-height: 1.8;
}
.site-footer a { color: var(--sky); text-decoration: none; }
.site-footer strong { color: #fff; }

/* ── print ─────────────────────────────────── */
@media print {
  .site-header .header-cta,
  .pdf-btn,
  .no-print { display: none !important; }

  body {
    background: #fff;
    font-size: 11pt;
  }
  .site-header {
    background: var(--ink) !important;
    print-color-adjust: exact;
    -webkit-print-color-adjust: exact;
    padding: 14px 24px;
  }
  .hero { padding: 28px 24px 20px; }
  .content { padding: 0 24px 32px; }
  .slide {
    box-shadow: none;
    border: 1px solid var(--line);
    padding: 20px 22px;
    margin-bottom: 12px;
  }
  .checklist-grid { gap: 6px 20px; }
  .site-footer { background: var(--ink) !important; print-color-adjust: exact; -webkit-print-color-adjust: exact; }
}
@media (max-width: 600px) {
  .site-header { padding: 16px 20px; }
  .hero { padding: 36px 20px 24px; }
  .content { padding: 0 20px 40px; }
  .slide { padding: 22px 20px; }
  .checklist-grid { grid-template-columns: 1fr; }
}
"""

_PDF_ICON = """<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="12" y1="18" x2="12" y2="12"/><line x1="9" y1="15" x2="15" y2="15"/></svg>"""


def _icon(paths: str) -> str:
    return (
        f'<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" '
        f'stroke-width="2" stroke-linecap="round" stroke-linejoin="round">{paths}</svg>'
    )


_ICON_FINANCE = _icon(
    '<rect x="2" y="3" width="20" height="14" rx="2" ry="2"/>'
    '<line x1="8" y1="21" x2="16" y2="21"/><line x1="12" y1="17" x2="12" y2="21"/>'
)
_ICON_ADS = _icon(
    '<polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/>'
)
_ICON_BRAND = _icon(
    '<path d="M20.84 4.61a5.5 5.5 0 0 0-7.78 0L12 5.67l-1.06-1.06a5.5 5.5 0 0 0-7.78 7.78l1.06 1.06L12 21.23l7.78-7.78 1.06-1.06a5.5 5.5 0 0 0 0-7.78z"/>'
)
_ICON_CUSTOMER = _icon(
    '<path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/>'
    '<circle cx="9" cy="7" r="4"/>'
    '<path d="M23 21v-2a4 4 0 0 0-3-3.87"/>'
    '<path d="M16 3.13a4 4 0 0 1 0 7.75"/>'
)
_ICON_CONTEXT = _icon(
    '<circle cx="12" cy="12" r="10"/>'
    '<line x1="12" y1="8" x2="12" y2="12"/>'
    '<line x1="12" y1="16" x2="12.01" y2="16"/>'
)
_ICON_CHECKLIST = _icon(
    '<polyline points="9 11 12 14 22 4"/>'
    '<path d="M21 12v7a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11"/>'
)


def _tag(kind: str, label: str) -> str:
    return f'<span class="tag tag-{kind}">{label}</span>'


def _slide(icon: str, title: str, desc: str, items: list[tuple[str, str, str]]) -> str:
    lis = ""
    for name, why, req in items:
        tag_html = _tag("required", "Required") if req == "R" else _tag("optional", "Optional")
        lis += f"<li><span class='item-name'>{name}{tag_html}</span><span class='item-why'>{why}</span></li>\n"
    return f"""
<div class="slide">
  <div class="slide-header">
    <div class="slide-icon">{icon}</div>
    <h2>{title}</h2>
  </div>
  <p class="desc">{desc}</p>
  <ul class="file-list">{lis}</ul>
</div>"""


def _checklist_grid(items: list[str]) -> str:
    cells = "".join(
        f'<div class="checklist-item"><div class="box"></div><span>{item}</span></div>'
        for item in items
    )
    return f'<div class="checklist-grid">{cells}</div>'


def render_intake_guide(print_mode: bool = False) -> str:
    pdf_button = (
        "" if print_mode else
        f'<a href="/brand-intake?print=1" class="pdf-btn no-print" target="_blank">'
        f'{_PDF_ICON} Download PDF</a>'
    )

    print_script = (
        "<script>window.addEventListener('load',function(){window.print();});</script>"
        if print_mode else ""
    )

    sections = (
        _slide(
            _ICON_FINANCE,
            "Financial Documents",
            "Your P&amp;L and balance sheet are the foundation of the analysis. "
            "Export directly from your accounting software — QuickBooks, Xero, or NetSuite. "
            "Multi-year comparisons (2+ years) unlock YoY trend scoring.",
            [
                ("Profit &amp; Loss (P&amp;L)", "Revenue, COGS, marketing, and net income by period", "R"),
                ("Balance Sheet", "Assets, liabilities, and equity snapshot", "R"),
                ("Prior-Year P&amp;L", "Enables YoY growth and trend grading", "O"),
                ("Trial Balance", "Full GL summary — useful if P&amp;L is unavailable", "O"),
                ("General Ledger (GL)", "Transaction-level detail for deeper margin decomposition", "O"),
            ],
        )
        + _slide(
            _ICON_ADS,
            "Advertising &amp; Channel Data",
            "Platform-level performance exports show where marketing spend is going and how "
            "efficiently it drives revenue. Download from each platform's reporting UI.",
            [
                ("Meta Ads export (CSV)", "Spend, impressions, ROAS by campaign", "O"),
                ("Google Ads export (CSV)", "Spend and conversion data", "O"),
                ("Amazon Ads report", "Sponsored Products / Brands / Display performance", "O"),
                ("TikTok / Snap / Pinterest", "Any additional paid-social exports", "O"),
                ("Email revenue report", "Klaviyo, Attentive, or similar — attributed revenue", "O"),
            ],
        )
        + _slide(
            _ICON_BRAND,
            "Brand &amp; Social",
            "We pull a lightweight brand signal from public channels. "
            "Providing direct handles is faster and more accurate than auto-detection.",
            [
                ("Instagram handle", "Follower count and engagement rate baseline", "O"),
                ("TikTok handle", "Organic reach signal", "O"),
                ("Amazon brand page URL", "Rating, review count, listing quality", "O"),
                ("Website URL", "We pull logo, product imagery, and SEO signals automatically", "O"),
            ],
        )
        + _slide(
            _ICON_CUSTOMER,
            "Customer Data",
            "Cohort and retention metrics unlock the acquisition and LTV scoring dimensions. "
            "These can come from your e-commerce platform (Shopify, WooCommerce) or CRM.",
            [
                ("New vs. returning revenue split", "Retention proxy if full cohorts unavailable", "O"),
                ("Cohort LTV CSV", "Customer acquisition month + cumulative spend over time", "O"),
                ("CAC by channel", "Blended or per-channel cost to acquire a customer", "O"),
                ("Repeat purchase rate", "% of customers who bought more than once", "O"),
            ],
        )
        + _slide(
            _ICON_CONTEXT,
            "Context Notes",
            "A few sentences from you can prevent misinterpretations that would otherwise "
            "surface as flags in the report. No structured format needed — plain text is fine.",
            [
                ("Legal entity vs. brand name", "If the filing name differs from the brand", "O"),
                ("Related-party transactions", "Owner loans, intercompany lines, non-arm's-length entries", "O"),
                ("Seasonality or one-time events", "e.g. launch year, supply disruption, clearance sale", "O"),
                ("Channel mix shifts", "e.g. moved from retail wholesale to DTC mid-year", "O"),
                ("Pending operational changes", "Warehouse moves, 3PL switches, pricing resets", "O"),
            ],
        )
        + f"""
<div class="slide">
  <div class="slide-header">
    <div class="slide-icon">{_ICON_CHECKLIST}</div>
    <h2>Quick Reference Checklist</h2>
  </div>
  <p class="desc">
    Everything at a glance. Required items are needed for a complete graded report;
    optional items improve scoring confidence.
  </p>
  {_checklist_grid([
      "P&L (current year)", "Balance sheet", "Prior-year P&L",
      "Trial balance (if no P&L)", "Meta Ads CSV", "Google Ads CSV",
      "Amazon Ads report", "Email revenue report", "Instagram handle",
      "Amazon brand URL", "New vs returning split", "Cohort LTV CSV",
      "CAC by channel", "Context notes (plain text)", "Website URL",
  ])}
</div>"""
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Brand Intake Checklist — Anata</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="stylesheet" href="{_FONTS}">
<style>{_CSS}</style>
</head>
<body>

<header class="site-header">
  <a class="logo" href="https://anatainc.com">ANA<span>TA</span></a>
  {pdf_button}
</header>

<section class="hero">
  <div class="eyebrow">Brand Acquisition</div>
  <h1>What We Need From You</h1>
  <p>
    This guide covers every file and data point we use to produce your brand acquisition report.
    The more complete the upload, the more dimensions we can grade — and the faster we can
    give you a confident recommendation.
  </p>
</section>

<main class="content">
  <div class="section-label">File-by-file breakdown</div>
  {sections}
</main>

<footer class="site-footer">
  <strong>Anata Inc.</strong><br>
  Questions? Reach us at <a href="mailto:david@anatainc.com">david@anatainc.com</a>
</footer>

{print_script}
</body>
</html>"""
