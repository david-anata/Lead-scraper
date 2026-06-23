"""
Brand Analysis – comprehensive parser + dimension audit.

Tests the intake engine against 12 synthetic file structures that cover the
real-world upload patterns we've seen and anticipate. Reports a per-file
completeness matrix showing which of the 8 dimensions compute vs stay N/A.

Run:  python scripts/brand_audit.py
"""
import sys, io, csv
sys.path.insert(0, ".")

import openpyxl
from sales_support_agent.services.brand_analysis.intake import parse_dump
from sales_support_agent.services.brand_analysis.scoring import score
from sales_support_agent.services.brand_analysis.schema import NOT_ASSESSED

# ---------------------------------------------------------------------------
# Helpers to build synthetic files
# ---------------------------------------------------------------------------

def _xlsx(sheets: dict) -> bytes:
    """sheets = {name: [[row, row, …]]}"""
    wb = openpyxl.Workbook()
    first = True
    for sheet_name, rows in sheets.items():
        ws = wb.active if first else wb.create_sheet()
        ws.title = sheet_name
        first = False
        for row in rows:
            ws.append(row)
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def _csv(rows: list) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerows(rows)
    return buf.getvalue().encode()


# ---------------------------------------------------------------------------
# Scenario definitions
# ---------------------------------------------------------------------------

# Common P&L structure: QBO-style (revenue spread, Total for Income row)
_QBO_PNL_ROWS = [
    ["Profit and Loss", "", "Jan - Dec 2025", "%", "Jan - Dec 2024", "%"],
    ["Income", "", "", "", "", ""],
    ["400011 · Product Sales", "", "850000", "75.9%", "900000", "78.3%"],
    ["400022 · Amazon Sales", "", "270483", "24.1%", "249567", "21.7%"],
    ["Total for Income", "", "1120483", "100.0%", "1149567", "100.0%"],
    ["Cost of Goods Sold", "", "", "", "", ""],
    ["500100 · Product Cost", "", "448193", "40.0%", "459827", "40.0%"],
    ["Gross Profit", "", "672290", "60.0%", "689740", "60.0%"],
    ["Expenses", "", "", "", "", ""],
    ["600100 · Marketing - Advertising", "", "0", "0.0%", "0", "0.0%"],
    ["600200 · Fulfillment/3PL", "", "89639", "8.0%", "80470", "7.0%"],
    ["600300 · SG&A", "", "112048", "10.0%", "103461", "9.0%"],
    ["Total Expenses", "", "201687", "18.0%", "183931", "16.0%"],
    ["Net Income", "", "470603", "42.0%", "505809", "44.0%"],
]

# QBO Balance Sheet
_QBO_BS_ROWS = [
    ["Balance Sheet", "", "Dec 31, 2025"],
    ["Assets", "", ""],
    ["Cash and Cash Equivalents", "", "250000"],
    ["Inventory", "", "180000"],
    ["Total Assets", "", "430000"],
    ["Liabilities", "", ""],
    ["Accounts Payable", "", "45000"],
    ["Total Liabilities", "", "45000"],
    ["Equity", "", ""],
    ["Total Equity", "", "385000"],
]

# Simple CSV P&L (Shopify/FreshBooks export style)
_SIMPLE_CSV_PNL = [
    ["Account", "Amount (2025)", "Amount (2024)"],
    ["Net Revenue", "1120483", "1149567"],
    ["Cost of Goods Sold", "448193", "459827"],
    ["Gross Profit", "672290", "689740"],
    ["Marketing", "89639", "80470"],
    ["General & Admin", "112048", "103461"],
    ["Net Income", "470603", "505809"],
]

# Trial Balance (account numbers, no subtotals)
_TRIAL_BALANCE_ROWS = [
    ["Trial Balance", "", "Dec 31, 2025", ""],
    ["Account", "Account Name", "Debit", "Credit"],
    ["1000", "Cash", "250000", ""],
    ["1200", "Inventory", "180000", ""],
    ["2000", "Accounts Payable", "", "45000"],
    ["3000", "Retained Earnings", "", "385000"],
    ["4000", "Product Sales", "", "1120483"],
    ["5000", "Cost of Goods Sold", "448193", ""],
    ["6000", "SG&A Expenses", "112048", ""],
    ["6100", "Fulfillment/3PL", "89639", ""],
]

# Meta Ads export (channel-level marketing spend)
_META_ADS_CSV = [
    ["Date", "Campaign", "Spend"],
    ["2025-01", "TOFU - Prospecting", "12000"],
    ["2025-02", "Retargeting", "8000"],
    ["2025-03", "BOFU", "5000"],
    ["Total Meta Ads", "", "25000"],
]

# Google Ads export
_GOOGLE_ADS_CSV = [
    ["Date", "Campaign Name", "Cost"],
    ["Jan 2025", "Brand Search", "3500"],
    ["Jan 2025", "Shopping", "6500"],
    ["Total", "", "10000"],
]

# Xero P&L export style (different header structure)
_XERO_PNL_ROWS = [
    ["Your Company", "", "", ""],
    ["Profit & Loss", "", "", ""],
    ["For the year ended 31 December 2025", "", "", ""],
    ["", "2025", "2024", ""],
    ["Revenue", "", "", ""],
    ["Sales Revenue", "1120483", "1149567", ""],
    ["Total Revenue", "1120483", "1149567", ""],
    ["Cost of Sales", "", "", ""],
    ["Purchases", "448193", "459827", ""],
    ["Total Cost of Sales", "448193", "459827", ""],
    ["Gross Profit", "672290", "689740", ""],
    ["Operating Expenses", "", "", ""],
    ["Advertising Expense", "89639", "80470", ""],
    ["General & Administrative", "112048", "103461", ""],
    ["Total Operating Expenses", "201687", "183931", ""],
    ["Net Profit", "470603", "505809", ""],
]

# Cohort/LTV data (best case for acquisition dimension)
_COHORT_CSV = [
    ["Cohort", "New Customer Revenue", "Returning Customer Revenue", "AOV"],
    ["2025 Annual", "560241", "560242", "85"],
    ["Totals", "560241", "560242", "85"],
]

# Prior-year only file (separate workbook pattern)
_PRIOR_YEAR_ONLY_PNL = [
    ["Profit and Loss", "Jan - Dec 2024"],
    ["Net Revenue", "1149567"],
    ["Cost of Goods Sold", "459827"],
    ["Gross Profit", "689740"],
    ["Expenses", "183931"],
    ["Net Income", "505809"],
]

# Current-year only (no YoY)
_CURRENT_YEAR_ONLY_PNL = [
    ["Profit and Loss", "Jan - Dec 2025"],
    ["Net Revenue", "1120483"],
    ["Cost of Goods Sold", "448193"],
    ["Gross Profit", "672290"],
    ["Expenses", "201687"],
    ["Net Income", "470603"],
]

# GL dump (should be excluded by triage)
_GL_DUMP = [["Date", "Account", "Description", "Debit", "Credit"]] + [
    [f"2025-{(i//100+1):02d}-01", "4000 Product Sales", f"Invoice {i}", "", f"{50 + i%200}"]
    for i in range(2100)  # > 2000 rows → safety-net triage
]


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------

SCENARIOS = [
    {
        "name": "QBO P&L only (YoY in same file)",
        "files": [("Profit_and_Loss_2025.xlsx", _xlsx({"Profit and Loss": _QBO_PNL_ROWS}))],
    },
    {
        "name": "QBO P&L + Balance Sheet (multi-sheet)",
        "files": [("Financials_2025.xlsx", _xlsx({"Profit and Loss": _QBO_PNL_ROWS, "Balance Sheet": _QBO_BS_ROWS}))],
    },
    {
        "name": "QBO P&L + BS + Meta Ads + Google Ads",
        "files": [
            ("Financials_2025.xlsx", _xlsx({"Profit and Loss": _QBO_PNL_ROWS, "Balance Sheet": _QBO_BS_ROWS})),
            ("Meta_Ads_Report.csv", _csv(_META_ADS_CSV)),
            ("Google_Ads_Report.csv", _csv(_GOOGLE_ADS_CSV)),
        ],
    },
    {
        "name": "QBO P&L + cohort LTV (acquisition dim)",
        "files": [
            ("Financials_2025.xlsx", _xlsx({"Profit and Loss": _QBO_PNL_ROWS})),
            ("Cohort_LTV_2025.csv", _csv(_COHORT_CSV)),
        ],
    },
    {
        "name": "Simple CSV P&L (single file, net_revenue row)",
        "files": [("PnL_2025.csv", _csv(_SIMPLE_CSV_PNL))],
    },
    {
        "name": "Xero P&L export (column header years)",
        "files": [("Xero_PnL.xlsx", _xlsx({"P&L": _XERO_PNL_ROWS}))],
    },
    {
        "name": "Trial Balance only",
        "files": [("Trial_Balance_2025.xlsx", _xlsx({"Trial Balance": _TRIAL_BALANCE_ROWS}))],
    },
    {
        "name": "Separate prior-year file (cross-file YoY)",
        "files": [
            ("Financials_2025.xlsx", _xlsx({"P&L": _CURRENT_YEAR_ONLY_PNL})),
            ("Financials_2024.xlsx", _xlsx({"P&L": _PRIOR_YEAR_ONLY_PNL})),
        ],
    },
    {
        "name": "Current year only (no prior)",
        "files": [("Profit_Loss_2025.csv", _csv(_CURRENT_YEAR_ONLY_PNL))],
    },
    {
        "name": "GL dump only (2100 rows → should be excluded)",
        "files": [("General_Ledger_2025.xlsx", _xlsx({"GL": _GL_DUMP}))],
    },
    {
        "name": "GL dump + P&L (GL excluded, P&L scored)",
        "files": [
            ("General_Ledger_2025.xlsx", _xlsx({"GL": _GL_DUMP})),
            ("Profit_and_Loss_2025.xlsx", _xlsx({"Profit and Loss": _QBO_PNL_ROWS})),
        ],
    },
    {
        "name": "Full kit: P&L + BS + Ads + Cohort + Prior year",
        "files": [
            ("Financials_2025.xlsx", _xlsx({"Profit and Loss": _QBO_PNL_ROWS, "Balance Sheet": _QBO_BS_ROWS})),
            ("Financials_2024.xlsx", _xlsx({"P&L": _PRIOR_YEAR_ONLY_PNL})),
            ("Meta_Ads.csv", _csv(_META_ADS_CSV)),
            ("Google_Ads.csv", _csv(_GOOGLE_ADS_CSV)),
            ("Cohort_LTV.csv", _csv(_COHORT_CSV)),
        ],
    },
]

DIMENSION_KEYS = ["revenue", "profitability", "marketing", "acquisition", "media", "contribution", "balance"]


# ---------------------------------------------------------------------------
# Run audit
# ---------------------------------------------------------------------------

def run_scenario(s):
    try:
        result = parse_dump(s["files"], use_llm=False)  # deterministic pass only
        scored = score(result.current, result.prior, category="dtc")
        sc = scored["scorecard"]
        dim_grades = {d.key: d.letter for d in sc.dimensions}
        notes = result.notes
        cur = result.current
        fields = {
            "net_revenue": cur.net_revenue_cents,
            "gross_profit": cur.reported_gross_profit_cents,
            "net_earnings": cur.net_earnings_cents,
            "marketing_total": cur.marketing_total_cents,
            "mktg_channels": len(cur.marketing_by_channel),
            "cogs": cur.cogs_cents,
            "total_assets": cur.total_assets_cents,
            "total_equity": cur.total_equity_cents,
            "new_cust_rev": cur.new_customer_revenue_cents,
            "has_prior": result.prior is not None,
        }
        return {
            "ok": True,
            "score": sc.score_100,
            "letter": sc.letter,
            "yoy": result.has_yoy,
            "dims": dim_grades,
            "fields": fields,
            "notes": notes,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ---------------------------------------------------------------------------
# Pretty print
# ---------------------------------------------------------------------------

DIM_SHORT = {
    "revenue": "Revenue↑",
    "profitability": "Profit",
    "marketing": "MktEff",
    "acquisition": "Acq",
    "media": "Media",
    "contribution": "ContMgn",
    "balance": "Balance",
    "brand": "Brand",
}

def grade_cell(letter):
    if letter == NOT_ASSESSED or letter == "NA":
        return "  N/A "
    return f"  {letter}  "


print("\n" + "=" * 100)
print("BRAND ANALYSIS PARSER AUDIT — deterministic pass (no LLM)")
print("=" * 100)
print(f"{'Scenario':<44} {'Score':>5} {'YoY':>4}  " + "  ".join(f"{DIM_SHORT.get(k, k):>6}" for k in DIMENSION_KEYS))
print("-" * 100)

issues = []

for s in SCENARIOS:
    r = run_scenario(s)
    if not r["ok"]:
        print(f"  {'ERROR: ' + s['name']:<43} {'ERROR':>5}  {r['error'][:50]}")
        issues.append(f"CRASH: {s['name']} → {r['error']}")
        continue

    dims = r["dims"]
    row = (
        f"  {s['name']:<43}"
        f" {r['letter']}/{r['score']:>3}  "
        f" {'Y' if r['yoy'] else 'N':>3}   "
        + "  ".join(f"{grade_cell(dims.get(k, '?')):>6}" for k in DIMENSION_KEYS)
    )
    print(row)

    # Flag unexpected N/As
    f = r["fields"]
    if f["net_revenue"] is None:
        issues.append(f"  [{s['name']}] revenue = None")
    if dims.get("profitability") == NOT_ASSESSED and f["net_earnings"] is None:
        issues.append(f"  [{s['name']}] profitability N/A (net_earnings missing)")
    if dims.get("contribution") == NOT_ASSESSED and f["gross_profit"] is None:
        issues.append(f"  [{s['name']}] contribution N/A (gross_profit missing)")
    if dims.get("balance") == NOT_ASSESSED and f["total_assets"] is None and f["total_equity"] is None:
        issues.append(f"  [{s['name']}] balance N/A (no BS data)")
    if dims.get("marketing") != NOT_ASSESSED and f["marketing_total"] is not None:
        rev = f["net_revenue"] or 1
        mer = rev / f["marketing_total"]
        if mer > 50:
            issues.append(f"  [{s['name']}] NOISE GUARD MISSED: MER = {mer:.0f}x")

print("-" * 100)
print()

# ---------------------------------------------------------------------------
# Field extraction summary per scenario
# ---------------------------------------------------------------------------
print("FIELD EXTRACTION DETAIL")
print("-" * 100)
print(f"  {'Scenario':<43} {'Revenue':>10} {'GrossProf':>10} {'NetEarn':>10} {'Marketing':>10} {'MktCh':>5} {'HasPrior':>8}")
print("-" * 100)
for s in SCENARIOS:
    r = run_scenario(s)
    if not r["ok"]:
        continue
    f = r["fields"]
    def _fmt(v): return f"${v//100:,}" if v is not None else "—"
    print(f"  {s['name']:<43} {_fmt(f['net_revenue']):>10} {_fmt(f['gross_profit']):>10} "
          f"{_fmt(f['net_earnings']):>10} {_fmt(f['marketing_total']):>10} {f['mktg_channels']:>5}  {'Y' if f['has_prior'] else 'N':>8}")

print()

# ---------------------------------------------------------------------------
# Issues found
# ---------------------------------------------------------------------------
print("ISSUES FOUND")
print("-" * 100)
if issues:
    for i in issues:
        print(f"  ⚠  {i}")
else:
    print("  None — all scenarios parsed cleanly.")

print()

# ---------------------------------------------------------------------------
# What data is needed for each dimension
# ---------------------------------------------------------------------------
print("DIMENSION DATA REQUIREMENTS")
print("-" * 100)
REQS = [
    ("Revenue↑",  "net_revenue in BOTH current AND prior period"),
    ("Profit",    "net_earnings (net_income) in P&L"),
    ("MktEff",    "marketing_total_cents + net_revenue (ad-platform CSV or P&L marketing line)"),
    ("Acq",       "new_cust_rev + returning_cust_rev OR owned_channel% OR return/discount rates"),
    ("Media",     "marketing_by_channel (2+ channels from ad-platform exports)"),
    ("ContMgn",   "reported_gross_profit (Gross Profit line in P&L)"),
    ("Balance",   "total_assets or total_equity (Balance Sheet)"),
]
for k, v in REQS:
    print(f"  {k:<10}  {v}")

print("=" * 100)
