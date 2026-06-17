"""Auto-detecting financial "file dump" intake.

Drop a brand's P&L, Balance Sheet, Trial Balance, GL and prior-year statements
at once; this layer reads every file (xlsx/xls via openpyxl, csv via stdlib,
pdf via pdfplumber), extracts label→value rows, maps common line items onto the
canonical ``PeriodFinancials`` fields, and detects current-vs-prior period.

It is intentionally forgiving: it never raises on a bad cell, maps what it can,
and leaves everything else as ``None`` so the scoring + missing-data layers can
report the gaps. Routing/parsing only — no grading here.
"""

from __future__ import annotations

import csv
import io
import logging
import re
from dataclasses import dataclass, field
from typing import Optional

from sales_support_agent.services.brand_analysis import intake_llm
from sales_support_agent.services.brand_analysis.schema import (
    PeriodFinancials,
    parse_cents,
)

logger = logging.getLogger(__name__)

_YEAR_RE = re.compile(r"\b(20[0-3]\d)\b")
_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


# ---------------------------------------------------------------------------
# Line-item synonym map. First match wins; order matters (most specific first).
# Each canonical field -> list of substrings tested against a normalized label.
# ---------------------------------------------------------------------------

_PNL_SYNONYMS: list[tuple[str, tuple[str, ...]]] = [
    ("reported_gross_profit_cents", ("gross profit", "gross margin", "gross income")),
    # "turnover" = UK/Sage term for revenue; "total income"/"income total" = Wave/FreshBooks
    ("net_revenue_cents", (
        "net revenue", "net sales", "total net revenue", "total revenue", "total sales",
        "revenue, net", "turnover", "net turnover", "total income", "income total",
        "total operating revenue",
    )),
    # "service revenue"/"product revenue" = FreshBooks sub-lines; kept under gross_sales
    # so they feed net_revenue_or_derived() when no explicit total is found
    ("gross_sales_cents", ("gross sales", "gross revenue", "product sales", "sales revenue",
                           "service revenue", "product revenue")),
    ("discounts_cents", ("discount", "promotions", "promotional", "markdown")),
    ("returns_cents", ("returns", "refunds", "allowances", "chargebacks")),
    # "purchases" = Xero (AU/NZ) term for COGS; "cost of materials"/"direct costs" = Sage
    ("cogs_cents", ("cogs", "cost of goods", "cost of sales", "cost of revenue", "product cost",
                    "purchases", "cost of materials", "materials cost", "direct costs",
                    "variable costs")),
    ("freight_3pl_cents", ("freight", "3pl", "fulfillment", "fulfilment", "shipping cost", "logistics", "warehous")),
    ("customer_support_cents", ("customer support", "customer service", "support cost", "cx ")),
    ("other_income_cents", ("other income", "non-operating", "non operating", "miscellaneous income", "interest income")),
    # "profit before tax" / "profit for the year" = UK GAAP; "operating result" = Sage
    ("net_earnings_cents", (
        "net earnings", "net income", "net profit", "net loss", "bottom line",
        "profit before tax", "profit for the year", "profit for period", "operating result",
        "profit after tax", "earnings after tax",
    )),
    # "admin expenses"/"staff costs" = Sage/UK; "total expenses" = Wave
    ("opex_cents", (
        "operating expense", "total opex", "opex", "sg&a", "sga", "general & admin", "overhead",
        "admin expenses", "administrative expenses", "staff costs", "total expenses",
        "general and administrative",
    )),
]

# Marketing: a single total OR per-channel lines that we also sum.
_MARKETING_TOTAL = ("total marketing", "total advertising", "marketing expense", "advertising expense", "marketing &", "marketing spend")
_MARKETING_CHANNELS: list[tuple[str, tuple[str, ...]]] = [
    ("meta", ("meta", "facebook", "instagram", "fb ads")),
    ("google", ("google", "adwords", "youtube", "search ads")),
    ("tiktok", ("tiktok", "tik tok")),
    ("amazon", ("amazon ads", "amazon advertising", "sponsored")),
    ("email_sms", ("email", "sms", "klaviyo", "owned channel", "lifecycle")),
    ("influencer", ("influencer", "affiliate", "creator")),
    ("other_marketing", ("marketing", "advertising", "paid media", "media spend")),
]

_BALANCE_SYNONYMS: list[tuple[str, tuple[str, ...]]] = [
    ("intercompany_cents", ("intercompany", "inter-company", "related party", "due from", "due to", "affiliate receivable")),
    ("total_assets_cents", ("total assets",)),
    ("cash_cents", ("cash and cash equivalents", "cash & equivalents", "cash equivalents", "cash")),
    ("inventory_cents", ("inventory", "stock on hand", "merchandise")),
    ("total_equity_cents", ("total equity", "shareholders equity", "stockholders equity", "owner's equity", "owners equity", "retained earnings")),
    ("dividends_cents", ("dividend", "distributions", "owner draw", "owner's draw", "shareholder distribution")),
]

_RELATED_PARTY_HINTS = ("related party", "intercompany", "inter-company", "affiliate", "owner loan", "shareholder loan")

# Acquisition / unit economics (best-effort; usually absent).
_ACQ_SYNONYMS: list[tuple[str, tuple[str, ...]]] = [
    ("new_customer_revenue_cents", ("new customer", "new-customer", "first-time", "acquisition revenue")),
    ("returning_customer_revenue_cents", ("returning customer", "repeat customer", "repeat purchase", "existing customer")),
    ("owned_channel_revenue_cents", ("email revenue", "sms revenue", "owned revenue", "owned-channel")),
    ("aov_cents", ("average order value", "aov")),
    ("cac_cents", ("customer acquisition cost", "cac", "blended cac")),
    ("ltv_cents", ("lifetime value", "ltv", "customer lifetime")),
]


def _norm(text: object) -> str:
    return re.sub(r"\s+", " ", str(text or "").strip().lower())


@dataclass
class _Table:
    """A header row + data rows extracted from one sheet/section."""

    source: str
    header: list[str] = field(default_factory=list)
    rows: list[list[str]] = field(default_factory=list)


# ---------------------------------------------------------------------------
# File readers — each returns a list of _Table (forgiving; never raises)
# ---------------------------------------------------------------------------


def _read_xlsx(source: str, data: bytes) -> list[_Table]:
    tables: list[_Table] = []
    try:
        import openpyxl
        wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
        for ws in wb.worksheets:
            try:
                ws.reset_dimensions()  # Amazon/QBO exports often zero the dimension
            except Exception:
                pass
            rows: list[list[str]] = []
            for row in ws.iter_rows(values_only=True):
                cells = ["" if c is None else str(c) for c in row]
                if any(c.strip() for c in cells):
                    rows.append(cells)
            if rows:
                tables.append(_Table(source=f"{source}::{ws.title}", header=rows[0], rows=rows))
    except Exception:  # noqa: BLE001
        logger.warning("[brand_analysis] failed to read xlsx %s", source, exc_info=True)
    return tables


def _read_csv(source: str, data: bytes) -> list[_Table]:
    try:
        text = data.decode("utf-8-sig", errors="replace")
        rows = [r for r in csv.reader(io.StringIO(text)) if any((c or "").strip() for c in r)]
    except Exception:  # noqa: BLE001
        return []
    if not rows:
        return []
    return [_Table(source=source, header=rows[0], rows=rows)]


def _read_pdf(source: str, data: bytes) -> list[_Table]:
    tables: list[_Table] = []
    try:
        import pdfplumber
        with pdfplumber.open(io.BytesIO(data)) as pdf:
            for page in pdf.pages:
                for tbl in page.extract_tables() or []:
                    rows = [["" if c is None else str(c) for c in r] for r in tbl if any(r)]
                    if rows:
                        tables.append(_Table(source=source, header=rows[0], rows=rows))
                # Text fallback: "Label .... 1,234.00  5,678.00"
                if not tables:
                    text = page.extract_text() or ""
                    rows = []
                    for line in text.splitlines():
                        line = line.strip()
                        if not line:
                            continue
                        m = re.match(r"^(.*?)\s+([\d.,$()\-\s]+)$", line)
                        if m and re.search(r"\d", m.group(2)):
                            label = m.group(1).strip()
                            nums = re.findall(r"\(?\$?[\d,]+(?:\.\d+)?\)?", m.group(2))
                            rows.append([label, *nums])
                    if rows:
                        tables.append(_Table(source=source, header=[], rows=rows))
    except Exception:  # noqa: BLE001
        logger.warning("[brand_analysis] failed to read pdf %s (image-only?)", source, exc_info=True)
    return tables


def _read_file(filename: str, data: bytes) -> list[_Table]:
    name = (filename or "").lower()
    if not data:
        return []
    if name.endswith((".xlsx", ".xlsm", ".xls")) or data[:2] == b"PK":
        return _read_xlsx(filename, data)
    if name.endswith(".pdf") or data[:5] == b"%PDF-":
        return _read_pdf(filename, data)
    return _read_csv(filename, data)


# ---------------------------------------------------------------------------
# Period / value-column resolution
# ---------------------------------------------------------------------------


def _value_columns(table: _Table) -> list[int]:
    """Column indices that carry numbers across the data rows (majority rule).

    Columns where the majority of non-empty cells contain "%" are excluded —
    QBO/Xero/Wave exports interleave "% of revenue" columns between monetary
    columns, and these must never be treated as prior-period dollar values.
    """
    width = max((len(r) for r in table.rows), default=0)
    hits = [0] * width      # cells that parse as monetary amounts
    pct_hits = [0] * width  # cells that look like percentages (contain %)
    counts = [0] * width
    for r in table.rows:
        for i, cell in enumerate(r):
            s = str(cell).strip()
            if not s:
                continue
            counts[i] += 1
            if "%" in s:
                pct_hits[i] += 1
            elif parse_cents(cell) is not None and re.search(r"\d", s):
                hits[i] += 1
    return [
        i for i in range(width)
        if counts[i]
        and hits[i] >= max(2, counts[i] * 0.5)
        and pct_hits[i] <= hits[i]
    ]


def _column_years(table: _Table, value_cols: list[int]) -> dict:
    """Map value-column index -> year, from header tokens when present.

    Scans the first 6 rows (covers QBO row-0 headers, Xero row-3/4 year rows,
    and custom export title rows). Only uses a candidate row when it looks like
    a label/title row — i.e. fewer than half of its value-column cells parse as
    a large monetary amount (rules out data rows that happen to contain a year
    like 2024 units-sold)."""
    years: dict[int, int] = {}
    candidate_rows = ([table.header] if table.header else []) + (table.rows[:6] if table.rows else [])
    for row in candidate_rows:
        # Skip rows that look like financial data (most value cells are large numbers).
        big_nums = sum(
            1 for i in value_cols
            if i < len(row) and parse_cents(row[i]) is not None and abs(parse_cents(row[i])) > 100_000
        )
        if big_nums > len(value_cols) // 2:
            continue
        found: dict[int, int] = {}
        for i in value_cols:
            cell = row[i] if i < len(row) else ""
            m = _YEAR_RE.search(str(cell))
            if m:
                found[i] = int(m.group(1))
        if found:
            for col, yr in found.items():
                if col not in years:
                    years[col] = yr
    return years


def _label_of(row: list[str], value_cols: list[int]) -> str:
    parts = [str(c).strip() for j, c in enumerate(row) if j not in value_cols and str(c).strip()]
    return _norm(" ".join(parts))


def _match_field(label: str, synonyms: list[tuple[str, tuple[str, ...]]]) -> Optional[str]:
    for field_name, subs in synonyms:
        if any(sub in label for sub in subs):
            return field_name
    return None


def _set_if_absent(period: PeriodFinancials, field_name: str, value: Optional[int]) -> None:
    if value is None:
        return
    if getattr(period, field_name, None) is None:
        setattr(period, field_name, value)


def _maybe_monthly(table: _Table, value_cols: list[int]) -> list:
    """If the header carries month names over the value columns and a revenue
    row exists, capture (month_label, cents) in calendar order."""
    header = table.header or []
    month_cols: list[tuple[int, int, str]] = []  # (col, month_num, label)
    for i in value_cols:
        cell = _norm(header[i]) if i < len(header) else ""
        for key, num in _MONTHS.items():
            if cell.startswith(key) or f" {key}" in cell:
                month_cols.append((i, num, str(header[i]).strip()))
                break
    if len(month_cols) < 6:
        return []
    for r in table.rows:
        label = _label_of(r, value_cols)
        if any(s in label for s in ("net revenue", "net sales", "total revenue", "total sales", "gross sales", "revenue")):
            out = []
            for col, _num, lbl in sorted(month_cols, key=lambda t: t[1]):
                if col < len(r):
                    cents = parse_cents(r[col])
                    if cents is not None:
                        out.append([lbl, cents])
            if len(out) >= 6:
                return out
    return []


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


@dataclass
class IntakeResult:
    current: PeriodFinancials
    prior: Optional[PeriodFinancials]
    detected_brands: list
    files_read: list           # [(filename, n_tables)]
    files_ignored: list        # filenames that yielded nothing
    notes: list                # human-readable detection notes
    account_mappings: dict = field(default_factory=dict)   # field -> {sources, confidence}
    unmapped_accounts: list = field(default_factory=list)  # accounts the classifier couldn't place
    classifier_model: str = ""                             # LLM model if classification ran

    @property
    def has_yoy(self) -> bool:
        return self.prior is not None

    def summary(self) -> str:
        parts = []
        if self.files_read:
            parts.append(f"Parsed {len(self.files_read)} file(s)")
        if self.detected_brands:
            parts.append(f"brand(s): {', '.join(self.detected_brands[:3])}")
        parts.append("YoY comparison" if self.has_yoy else "single-period (no prior year)")
        if self.files_ignored:
            parts.append(f"ignored {len(self.files_ignored)} unreadable file(s)")
        return "; ".join(parts) + "."


def _brand_from_filename(filename: str) -> str:
    stem = re.sub(r"\.[a-z0-9]+$", "", filename or "", flags=re.I)
    # Normalize separators to spaces FIRST so the statement-keyword strip below
    # sees real word boundaries (underscores are word chars and block \b).
    stem = re.sub(r"[-_]+", " ", stem)
    stem = re.sub(r"(?i)\b(p&l|pnl|profit and loss|balance sheet|trial balance|general ledger|gl|income statement|financials?|statement|fy|q[1-4]|20[0-3]\d|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec|export|report|final|copy|v\d)\b", " ", stem)
    stem = re.sub(r"\s+", " ", stem).strip()
    return stem.title() if stem and len(stem) > 1 else ""


# Document triage. Each parsed sheet/table is classified so transaction-level
# dumps (a General Ledger can be tens of thousands of rows) are kept OUT of the
# scoring/classifier input — the summary statements (P&L, Trial Balance, Balance
# Sheet) carry the account totals we actually grade on. Order matters: most
# specific first. GL is detected first so a GL sheet never falls through.
_DOC_PATTERNS: list[tuple[str, tuple[str, ...]]] = [
    ("general_ledger", ("general ledger", "genledger", "::gl", " gl ", "_gl ", "_gl.", "_gl_")),
    ("trial_balance", ("trial balance", "trial_balance", "trialbalance", "::tb")),
    ("balance_sheet", ("balance sheet", "balance_sheet", "::bs")),
    ("pnl", ("profit and loss", "profit n loss", "profit & loss", "p&l", "pnl", "income statement")),
    ("ad_platform", ("meta ads", "facebook ads", "google ads", "adwords", "tiktok ads", "klaviyo", "ad report", "ads report", "campaign report")),
    ("cohort_ltv", ("cohort", "ltv", "lifetime value", "retention", "repeat purchase")),
    ("inventory_supplier", ("inventory report", "supplier", "3pl", "purchase order", "stock on hand")),
]

# Doc types excluded from scoring/classification (kept only as reference).
_TRANSACTION_TYPES = ("general_ledger",)


def _table_doc_type(filename: str, table) -> str:
    """Best-effort statement type for a single sheet/table, from the filename,
    sheet name (table.source), and the first few title rows."""
    hay = f" {filename} {table.source} ".lower()
    for row in ([table.header or []] + (table.rows[:3] if table.rows else [])):
        hay += " " + " ".join(str(c).lower() for c in row)
    for dtype, pats in _DOC_PATTERNS:
        if any(p in hay for p in pats):
            return dtype
    # Safety net: an enormous table is a transaction dump regardless of naming.
    if len(table.rows) > 2000:
        return "general_ledger"
    return "other"


def _income_total_col(table, col: int) -> Optional[int]:
    """Extract the Total-for-Income value from a specific column index.
    Used for the prior-year column of a multi-year P&L where _income_total
    only captures the first (current) money column."""
    vcols = _value_columns(table)
    cands: list[tuple[int, bool]] = []
    for r in table.rows:
        label = _label_of(r, vcols)
        if "income" not in label or "total" not in label:
            continue
        if "other income" in label or "cost" in label or "interest" in label:
            continue
        if col >= len(r):
            continue
        c = parse_cents(r[col])
        if c is None or abs(c) <= 100_000:
            continue
        # Detect the 100%-of-income marker in any adjacent column
        pct100 = any(
            (ic < len(r) and (lambda v: v is not None and 95 <= abs(v) / 100 <= 105)(parse_cents(r[ic])))
            for ic in vcols if ic != col
        )
        cands.append((abs(c), pct100))
    if not cands:
        return None
    base = [m for m, p in cands if p]
    return base[-1] if base else cands[-1][0]


def _income_total(table) -> Optional[int]:
    """Net total income (revenue) from a P&L table, in cents. Picks the
    'Total … Income' row that is the 100%-of-income base (the %-of-income
    column reads ~100 on the true total); else the outermost such total.
    Excludes 'other income', cost, and interest lines. None if not found."""
    vcols = _value_columns(table)
    cands: list[tuple[int, bool]] = []
    for r in table.rows:
        label = _label_of(r, vcols)
        if "income" not in label or "total" not in label:
            continue
        if "other income" in label or "cost" in label or "interest" in label:
            continue
        money = None
        pct100 = False
        for i in vcols:
            if i >= len(r):
                continue
            c = parse_cents(r[i])
            if c is None:
                continue
            if money is None and abs(c) > 100_000:   # > $1,000 in cents = the money column
                money = abs(c)
            if 95 <= abs(c) / 100 <= 105:             # a bare ~100 = the %-of-income base
                pct100 = True
        if money is not None:
            cands.append((money, pct100))
    if not cands:
        return None
    base = [m for m, p in cands if p]
    return base[-1] if base else cands[-1][0]


def _years_of_file(filename: str, tables: list) -> set[int]:
    """All plausible fiscal years found in a file's name and title rows.
    Returns a set so multi-year exports (e.g. Xero P&L with 2025/2024
    columns) contribute both years to the period-detection logic."""
    years: set[int] = set()
    m = _YEAR_RE.findall(filename or "")
    years.update(int(y) for y in m)
    for t in tables:
        head_rows = [t.header or []] + (t.rows[:4] if t.rows else [])
        for row in head_rows:
            for cell in row:
                for y in _YEAR_RE.findall(str(cell)):
                    years.add(int(y))
    return years


def parse_dump(files: list[tuple[str, bytes]], *, category: str = "dtc",
               use_llm: bool = True, context_notes: str = "") -> IntakeResult:
    """Parse a batch of uploaded financial files into current/prior periods.

    Runs the fast deterministic substring mapper first; when it leaves a
    material P&L bucket empty (the tell-tale of a trial-balance / GL dump) and
    an LLM key is configured, an LLM classifier folds the raw GL accounts into
    the canonical buckets and fills the gaps (see intake_llm)."""
    current = PeriodFinancials(period_label="Current period")
    prior: Optional[PeriodFinancials] = None
    detected_brands: list[str] = []
    files_read: list = []
    files_ignored: list = []
    notes: list = []
    all_years: set[int] = set()
    all_tables: list = []

    # First pass: parse each file and tag it with a fiscal year (filename +
    # title rows + column headers), so prior-year data living in a *separate
    # file* is recognised for YoY — not just year-labelled columns.
    parsed_files: list[tuple[str, list[_Table]]] = []
    file_years: dict[str, Optional[int]] = {}
    excluded_ledger = 0
    for filename, data in files:
        tables = _read_file(filename, data)
        if not tables:
            files_ignored.append(filename)
            continue
        files_read.append((filename, len(tables)))
        # Triage: drop transaction-level General Ledger sheets from scoring —
        # the summary statements carry the account totals, and a GL can be tens
        # of thousands of rows that flood the classifier and bury revenue.
        kept = [t for t in tables if _table_doc_type(filename, t) not in _TRANSACTION_TYPES]
        excluded_ledger += len(tables) - len(kept)
        # _years_of_file returns ALL years found (filename + title rows), so a
        # Xero/multi-year P&L that puts both "2025" and "2024" in a title row
        # contributes both years — not just the max — to period detection.
        file_yr_set = _years_of_file(filename, tables)
        fy = max(file_yr_set) if file_yr_set else None
        file_years[filename] = fy
        all_years.update(file_yr_set)
        if not kept:
            continue  # whole file was a ledger — recorded, but not scored
        all_tables.extend(kept)
        parsed_files.append((filename, kept))
        brand = _brand_from_filename(filename)
        if brand and brand not in detected_brands:
            detected_brands.append(brand)
        for t in kept:
            for cell in (t.header or []):
                m = _YEAR_RE.search(str(cell))
                if m:
                    all_years.add(int(m.group(1)))

    current_year = max(all_years) if all_years else None
    prior_year = sorted(all_years)[-2] if len(all_years) >= 2 else None
    if current_year:
        current.year = current_year
        current.period_label = f"FY {current_year}"
    if prior_year:
        prior = PeriodFinancials(period_label=f"FY {prior_year}", year=prior_year)

    synonyms_all = _PNL_SYNONYMS + _BALANCE_SYNONYMS + _ACQ_SYNONYMS

    for filename, tables in parsed_files:
        # A whole-file prior year (e.g. a separate "Financials 2024" workbook)
        # routes its single value column into the PRIOR period.
        file_year = file_years.get(filename)
        file_is_prior = (
            prior is not None and prior_year is not None and current_year is not None
            and file_year == prior_year and file_year != current_year
        )
        # When 3+ years are present, skip whole files that belong to a year
        # outside the current/prior window — prevents a 3rd-year file from
        # silently backfilling current-period fields via _set_if_absent.
        if (
            file_year is not None
            and current_year is not None
            and prior_year is not None
            and file_year not in (current_year, prior_year)
        ):
            notes.append(
                f"Skipped '{filename}' (FY {file_year}) — outside the two-period "
                f"window ({prior_year}–{current_year})."
            )
            continue
        for t in tables:
            value_cols = _value_columns(t)
            if not value_cols:
                continue
            col_years = _column_years(t, value_cols)
            # Decide which value column feeds current vs prior.
            cur_col = prior_col = None
            if col_years:
                # Highest year -> current; next -> prior.
                ordered = sorted(col_years.items(), key=lambda kv: kv[1], reverse=True)
                cur_col = ordered[0][0]
                if len(ordered) >= 2:
                    prior_col = ordered[1][0]
            else:
                cur_col = value_cols[0]
                if len(value_cols) >= 2:
                    prior_col = value_cols[1]

            monthly = _maybe_monthly(t, value_cols)
            if monthly and not current.monthly_revenue:
                current.monthly_revenue = monthly

            for r in t.rows:
                label = _label_of(r, value_cols)
                if not label:
                    continue
                if any(h in label for h in _RELATED_PARTY_HINTS):
                    current.related_party_flag = True
                    if prior:
                        prior.related_party_flag = True

                field_name = _match_field(label, synonyms_all)
                channel = None
                if field_name is None:
                    if any(s in label for s in _MARKETING_TOTAL):
                        field_name = "marketing_total_cents"
                    else:
                        channel = _match_field(label, _MARKETING_CHANNELS)
                if field_name is None and channel is None:
                    continue

                def _val(col):
                    return parse_cents(r[col]) if col is not None and col < len(r) else None

                cur_v, prior_v = _val(cur_col), _val(prior_col)
                # When the whole file is the prior year, its primary column is
                # prior-period data (no in-table current column to compare).
                dest_primary = prior if file_is_prior else current
                other = None if file_is_prior else prior
                if field_name:
                    _set_if_absent(dest_primary, field_name, cur_v)
                    if other is not None:
                        _set_if_absent(other, field_name, prior_v)
                elif channel:
                    if cur_v is not None:
                        dest_primary.marketing_by_channel.setdefault(channel, cur_v)
                    if other is not None and prior_v is not None:
                        other.marketing_by_channel.setdefault(channel, prior_v)

    # Roll channel marketing into a total when no explicit total line was found.
    for period in (current, prior):
        if period is None:
            continue
        if period.marketing_total_cents is None and period.marketing_by_channel:
            period.marketing_total_cents = sum(period.marketing_by_channel.values())

    # Deterministic revenue fallback. Real QBO/Xero P&L exports spread revenue
    # across numbered income accounts with no "Net Revenue" row, but carry a
    # "Total for Income" line (the 100%-of-income base). Use it when revenue is
    # still missing — works without the LLM, so revenue lands even if the
    # classifier can't run or errors.
    for filename, tables in parsed_files:
        fy = file_years.get(filename)
        # Skip files outside the current/prior window (same guard as the main loop).
        if (
            fy is not None
            and current_year is not None
            and prior_year is not None
            and fy not in (current_year, prior_year)
        ):
            continue
        is_prior = (prior is not None and prior_year is not None and current_year is not None
                    and fy == prior_year and fy != current_year)
        target = prior if is_prior else current
        if target is None or target.net_revenue_cents is not None:
            continue
        for t in tables:
            if _table_doc_type(filename, t) == "pnl":
                tot = _income_total(t)
                if tot:
                    target.net_revenue_cents = tot
                    break

    # Prior-revenue fallback for multi-year P&Ls. QBO/Xero put both current and
    # prior columns in the SAME sheet. The loop above fills current; prior stays
    # None because the file isn't tagged as a prior-year file. Extract from the
    # prior column directly using _income_total_col.
    if prior is not None and prior.net_revenue_cents is None:
        for filename, tables in parsed_files:
            fy = file_years.get(filename)
            is_prior = (prior_year is not None and current_year is not None
                        and fy == prior_year and fy != current_year)
            if is_prior:
                continue  # already handled above
            for t in tables:
                if _table_doc_type(filename, t) != "pnl":
                    continue
                vcols = _value_columns(t)
                col_yrs = _column_years(t, vcols)
                if not col_yrs:
                    continue
                ordered = sorted(col_yrs.items(), key=lambda kv: kv[1], reverse=True)
                if len(ordered) < 2:
                    continue
                prior_col = ordered[1][0]
                tot = _income_total_col(t, prior_col)
                if tot:
                    prior.net_revenue_cents = tot
                    break
            if prior.net_revenue_cents is not None:
                break

    # Marketing-noise guard. A GL occasionally has a tiny "marketing" account
    # that gets matched, implying an absurd MER (e.g. 6969x) and a falsely good
    # marketing/media grade. Real DTC ad spend lives in ad-platform exports, not
    # the P&L — so when the implied MER is implausible (marketing < ~2% of
    # revenue), treat marketing as NOT supplied: grade it N/A, not a fake A/B.
    _flagged = False
    for period in (current, prior):
        if period is None:
            continue
        rev = period.net_revenue_or_derived()
        mkt = period.marketing_total_cents
        if rev and rev > 0 and mkt and mkt > 0 and rev / mkt > 50:
            period.marketing_total_cents = None
            period.marketing_by_channel = {}
            _flagged = True
    if _flagged:
        notes.append(
            "Marketing spend was implausibly small vs revenue (likely not in the "
            "supplied financials) — left unscored. Add ad-platform exports "
            "(Meta/Google/Klaviyo) for real marketing efficiency."
        )

    # LLM gap-fill for GL / trial-balance dumps the substring matcher can't fold.
    account_mappings: dict = {}
    unmapped_accounts: list = []
    classifier_model = ""
    if use_llm and all_tables and intake_llm.should_classify(current):
        file_groups = [(fn, file_years.get(fn), tbls) for fn, tbls in parsed_files]
        result = intake_llm.classify(
            all_tables, file_groups=file_groups, context_notes=context_notes,
            current_year=current_year, prior_year=prior_year)
        if result is not None:
            classifier_model = result.model
            intake_llm.merge_into(current, result, account_mappings, prior=False)
            if prior is not None:
                intake_llm.merge_into(prior, result, account_mappings, prior=True)
            elif result.prior:
                # Classifier found a prior period the year-scan missed.
                prior = PeriodFinancials(period_label=result.period_prior_label or "Prior period")
                intake_llm.merge_into(prior, result, account_mappings, prior=True)
            unmapped_accounts = list(result.unmapped)
            if classifier_model:
                notes.append(
                    f"LLM classifier folded {len(account_mappings)} GL bucket(s) the "
                    f"line-item matcher left empty."
                )

    if excluded_ledger:
        notes.append(
            f"Excluded {excluded_ledger} General Ledger sheet(s) from scoring "
            "(transaction detail) — used the summary statements for account totals."
        )
    if current_year and prior_year:
        notes.append(f"Detected periods {current_year} (current) vs {prior_year} (prior).")
    elif not all_years:
        notes.append("No year headers found — treated columns positionally (first = current).")
    if not detected_brands:
        notes.append("Brand name not detected from filenames — defaulted to 'Brand'.")

    return IntakeResult(
        current=current,
        prior=prior,
        detected_brands=detected_brands,
        files_read=files_read,
        files_ignored=files_ignored,
        notes=notes,
        account_mappings=account_mappings,
        unmapped_accounts=unmapped_accounts,
        classifier_model=classifier_model,
    )
