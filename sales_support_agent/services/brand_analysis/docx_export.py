"""Server-side .docx export of a BrandReport (python-docx).

Reproduces the on-screen report's section order and rating system in a
professionally styled Word document: navy headings, light-blue table header
rows, a color-coded grade banner and PASS/FAIL cells, the YoY table, monthly
trajectory, ranked red-flags table, category benchmarks, weighted scorecard,
the missing-data block under the grade, and the verdict.
"""

from __future__ import annotations

import io
from typing import Optional

from sales_support_agent.services.brand_analysis.schema import (
    CATEGORY_LABELS,
    BrandReport,
    fmt_money,
    fmt_mult,
    fmt_pct,
)

# Brand palette (hex, no #) — mirrors the app design tokens.
_NAVY = "2B3644"
_LIGHT_BLUE = "85BBDA"
_HEADER_FILL = "DCE9F2"  # light-blue table header row
_MISSING_FILL = "F9F7F3"
_GRADE_FILL = {"A": "2E7D5B", "B": "3F8F6E", "C": "B8860B", "D": "C2663B", "F": "8B4C42"}
_SEV_HEX = {"Critical": "8B4C42", "High": "C2663B", "Medium": "B8860B"}
_PASS_HEX = "2E7D5B"
_FAIL_HEX = "8B4C42"


def _shade(cell, hex_fill: str) -> None:
    from docx.oxml import OxmlElement
    from docx.oxml.ns import qn

    tc_pr = cell._tc.get_or_add_tcPr()
    shd = OxmlElement("w:shd")
    shd.set(qn("w:val"), "clear")
    shd.set(qn("w:color"), "auto")
    shd.set(qn("w:fill"), hex_fill)
    tc_pr.append(shd)


def _color_run(run, hex_color: str) -> None:
    from docx.shared import RGBColor

    run.font.color.rgb = RGBColor.from_string(hex_color)


def _heading(doc, text: str, level: int = 1) -> None:
    from docx.shared import RGBColor

    h = doc.add_heading(text, level=level)
    for run in h.runs:
        run.font.color.rgb = RGBColor.from_string(_NAVY)


def _kv_table(doc, headers: list[str], rows: list[list], *, align_right_from: int = 1) -> None:
    from docx.enum.text import WD_ALIGN_PARAGRAPH

    table = doc.add_table(rows=1, cols=len(headers))
    table.style = "Light Grid Accent 1"
    hdr = table.rows[0].cells
    for i, h in enumerate(headers):
        hdr[i].text = ""
        run = hdr[i].paragraphs[0].add_run(h)
        run.bold = True
        _shade(hdr[i], _HEADER_FILL)
    for row in rows:
        cells = table.add_row().cells
        for i, val in enumerate(row):
            cells[i].text = "" if val is None else str(val)
            if i >= align_right_from:
                cells[i].paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.RIGHT
    doc.add_paragraph()


def build_docx(report: BrandReport) -> bytes:
    from docx import Document
    from docx.enum.text import WD_ALIGN_PARAGRAPH
    from docx.shared import Pt, RGBColor

    r = report
    doc = Document()

    # Header
    title = doc.add_paragraph()
    run = title.add_run("EXECUTIVE ACQUISITION REPORT")
    run.bold = True
    run.font.size = Pt(11)
    _color_run(run, _LIGHT_BLUE)
    _heading(doc, r.brand or "Brand", level=0)
    detected = ", ".join(r.detected_brands) if r.detected_brands else "—"
    periods = r.period_current_label + (f" vs {r.period_prior_label}" if r.has_yoy else " (single period — no prior year)")
    meta = doc.add_paragraph()
    meta.add_run(
        f"Category: {CATEGORY_LABELS.get(r.category, r.category)}  ·  Detected: {detected}  ·  "
        f"Periods: {periods}" + (f"  ·  Prepared {r.prepared_date}" if r.prepared_date else "")
    ).italic = True

    # 0. Grade banner
    banner = doc.add_table(rows=1, cols=2)
    cell_grade, cell_text = banner.rows[0].cells
    cell_grade.text = ""
    g_run = cell_grade.paragraphs[0].add_run(r.scorecard.letter)
    g_run.bold = True
    g_run.font.size = Pt(36)
    g_run.font.color.rgb = RGBColor.from_string("FFFFFF")
    cell_grade.paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.CENTER
    _shade(cell_grade, _GRADE_FILL.get(r.scorecard.letter, _NAVY))
    cell_text.paragraphs[0].add_run(
        f"Weighted score {r.scorecard.score_100}/100 · Recommendation: {r.recommendation}"
    ).bold = True
    cell_text.add_paragraph(r.verdict_text)
    doc.add_paragraph()

    # Missing-data block directly under the grade
    mp = doc.add_paragraph()
    if r.data_sufficient:
        run = mp.add_run(f"Data sufficient — grade reflects complete financial inputs.  (Confidence: {r.confidence})")
        run.bold = True
        _color_run(run, _PASS_HEX)
    else:
        run = mp.add_run(f"Missing data that would raise confidence  (Confidence: {r.confidence})")
        run.bold = True
        for item in r.missing_data:
            doc.add_paragraph(item, style="List Bullet")

    # 1. Executive Summary
    _heading(doc, "1. Executive Summary", 1)
    doc.add_paragraph(r.executive_summary)
    doc.add_paragraph().add_run("What stands out beyond standard KPIs").bold = True
    for s in r.stands_out:
        doc.add_paragraph(s, style="List Bullet")

    # 2. Financial Overview (YoY)
    _heading(doc, "2. Financial Overview (YoY)", 1)
    c, p = r.current, r.prior
    headers = ["Metric", r.period_current_label] + ([r.period_prior_label] if r.has_yoy else [])

    def _row(label, cur, prior):
        return [label, cur] + ([prior] if r.has_yoy else [])

    rows = [
        _row("Net revenue", fmt_money(c.net_revenue_cents), fmt_money(p.net_revenue_cents)),
        _row("COGS", fmt_money(c.cogs_cents), fmt_money(p.cogs_cents)),
        _row("Product gross profit", fmt_money(c.product_gross_profit_cents), fmt_money(p.product_gross_profit_cents)),
        _row("Product GM%", fmt_pct(c.product_gm_bps), fmt_pct(p.product_gm_bps)),
        _row("Marketing spend", fmt_money(c.marketing_total_cents), fmt_money(p.marketing_total_cents)),
        _row("Marketing % of revenue", fmt_pct(c.marketing_pct_bps), fmt_pct(p.marketing_pct_bps)),
        _row("Blended MER", fmt_mult(c.blended_mer), fmt_mult(p.blended_mer)),
        _row("Contribution (reported GP)", fmt_money(c.reported_gross_profit_cents), fmt_money(p.reported_gross_profit_cents)),
        _row("Contribution margin", fmt_pct(c.contribution_margin_bps), fmt_pct(p.contribution_margin_bps)),
        _row("Operating expenses", fmt_money(c.opex_cents), fmt_money(p.opex_cents)),
        _row("Net earnings", fmt_money(c.net_earnings_cents), fmt_money(p.net_earnings_cents)),
        _row("Net margin", fmt_pct(c.net_margin_bps), fmt_pct(p.net_margin_bps)),
    ]
    _kv_table(doc, headers, rows)
    if r.has_yoy:
        growth = "—" if r.yoy_revenue_growth_bps is None else fmt_pct(r.yoy_revenue_growth_bps)
        doc.add_paragraph(f"YoY net-revenue growth: {growth}.")

    # Monthly trajectory
    if r.monthly_revenue:
        doc.add_paragraph().add_run("Monthly revenue trajectory").bold = True
        peak = max((v for _, v in r.monthly_revenue if isinstance(v, (int, float))), default=1) or 1
        mt = doc.add_table(rows=0, cols=3)
        mt.style = "Light List Accent 1"
        for lbl, v in r.monthly_revenue:
            cells = mt.add_row().cells
            cells[0].text = str(lbl)
            cells[1].text = fmt_money(v)
            blocks = max(1, round((v / peak) * 20)) if isinstance(v, (int, float)) else 0
            cells[2].text = "█" * blocks
        doc.add_paragraph()

    # 3. Acquisition Mix
    _heading(doc, "3. Acquisition Mix", 1)
    _kv_table(doc, ["Signal", "Value"], [
        ["New vs returning split", "Data gap — request cohort data"],
        ["Owned-channel (email/SMS) share", fmt_pct(c.owned_pct_bps) if c.owned_pct_bps is not None else "Data gap"],
        ["Discount rate", fmt_pct(c.discount_rate_bps) if c.discount_rate_bps is not None else "Data gap"],
        ["Return rate", fmt_pct(c.return_rate_bps) if c.return_rate_bps is not None else "Data gap"],
        ["Blended MER", fmt_mult(c.blended_mer) if c.blended_mer is not None else "Data gap"],
    ], align_right_from=99)

    # 4. Media Mix
    _heading(doc, "4. Media Mix", 1)
    if r.media_mix:
        total = sum(r.media_mix.values()) or 1
        mrows = [[k, fmt_money(v), f"{v/total*100:.0f}%"] for k, v in sorted(r.media_mix.items(), key=lambda kv: kv[1], reverse=True)]
        _kv_table(doc, ["Channel", "Spend", "% allocation"], mrows, align_right_from=1)
    else:
        doc.add_paragraph("Channel-level media mix not supplied — request ad-platform exports (Meta, Google, TikTok).")

    # 5. Contribution & Unit Economics
    _heading(doc, "5. Contribution & Unit Economics", 1)
    _kv_table(doc, ["Metric", "Value"], [
        ["Contribution margin", fmt_pct(c.contribution_margin_bps)],
        ["Discount rate", fmt_pct(c.discount_rate_bps)],
        ["Operating result excl. other income", fmt_money(c.operating_result_ex_other_cents)],
        ["Net earnings (reported)", fmt_money(c.net_earnings_cents)],
        ["AOV / CAC / LTV / payback", "Data gap — not supplied"],
    ], align_right_from=99)

    # 6. Balance Sheet & Earnings Quality
    _heading(doc, "6. Balance Sheet & Earnings Quality", 1)
    if r.balance_sheet:
        _kv_table(doc, ["Line", "Amount"], [[l, fmt_money(v)] for l, v in r.balance_sheet], align_right_from=1)
        if r.related_party_flag:
            warn = doc.add_paragraph().add_run("Related-party / intercompany items detected — diligence collectability and agreements.")
            _color_run(warn, _FAIL_HEX)
            warn.bold = True
    else:
        doc.add_paragraph("Balance sheet not supplied — request assets, intercompany balances, equity, dividends and related-party agreements.")

    # 7. Red Flags
    _heading(doc, "7. Red Flags", 1)
    if r.red_flags:
        table = doc.add_table(rows=1, cols=2)
        table.style = "Light Grid Accent 1"
        hdr = table.rows[0].cells
        for i, h in enumerate(["Severity", "Finding"]):
            hdr[i].paragraphs[0].add_run(h).bold = True
            _shade(hdr[i], _HEADER_FILL)
        for f in r.red_flags:
            cells = table.add_row().cells
            sev_run = cells[0].paragraphs[0].add_run(f.severity)
            sev_run.bold = True
            _color_run(sev_run, _SEV_HEX.get(f.severity, _NAVY))
            cells[1].paragraphs[0].add_run(f.title).bold = True
            if f.detail:
                cells[1].add_paragraph(f.detail)
        doc.add_paragraph()
    else:
        doc.add_paragraph("No material red flags surfaced in the supplied data.")

    # 8. Category Benchmarks
    _heading(doc, "8. Category Benchmarks", 1)
    table = doc.add_table(rows=1, cols=4)
    table.style = "Light Grid Accent 1"
    hdr = table.rows[0].cells
    for i, h in enumerate(["KPI", "Healthy range", "Brand actual", "Verdict"]):
        hdr[i].paragraphs[0].add_run(h).bold = True
        _shade(hdr[i], _HEADER_FILL)
    for b in r.benchmarks:
        cells = table.add_row().cells
        cells[0].text = b.kpi
        cells[1].text = b.healthy
        cells[2].text = b.actual
        if b.passed is True:
            vr = cells[3].paragraphs[0].add_run("PASS"); vr.bold = True; _color_run(vr, _PASS_HEX)
        elif b.passed is False:
            vr = cells[3].paragraphs[0].add_run("FAIL"); vr.bold = True; _color_run(vr, _FAIL_HEX)
        else:
            cells[3].text = "data gap"
    doc.add_paragraph()

    # Weighted Scorecard
    _heading(doc, "Weighted Scorecard", 1)
    table = doc.add_table(rows=1, cols=4)
    table.style = "Light Grid Accent 1"
    hdr = table.rows[0].cells
    for i, h in enumerate(["Dimension", "Weight", "Grade", "Reason"]):
        hdr[i].paragraphs[0].add_run(h).bold = True
        _shade(hdr[i], _HEADER_FILL)
    for d in r.scorecard.dimensions:
        cells = table.add_row().cells
        cells[0].text = d.label
        cells[1].text = f"{int(d.weight*100)}%"
        gr = cells[2].paragraphs[0].add_run(d.letter); gr.bold = True; _color_run(gr, _GRADE_FILL.get(d.letter, _NAVY))
        cells[3].text = d.reason
    doc.add_paragraph(f"Weighted composite (A=4…F=0, rebased to 100): {r.scorecard.score_100}/100 → {r.scorecard.letter}.")

    # 9. Data Gaps to Close
    _heading(doc, "9. Data Gaps to Close", 1)
    for g in r.data_gaps:
        doc.add_paragraph(g, style="List Bullet")

    # 10. Verdict
    _heading(doc, "10. Verdict", 1)
    vp = doc.add_paragraph()
    vp.add_run(f"Grade {r.scorecard.letter} ({r.scorecard.score_100}/100) — {r.recommendation}. ").bold = True
    vp.add_run(r.verdict_text)
    note = doc.add_paragraph()
    note.add_run(
        f"Source: derived from uploaded financial statements. Narrative: {r.narrative_model}. {r.intake_summary}"
    ).italic = True

    buf = io.BytesIO()
    doc.save(buf)
    return buf.getvalue()
