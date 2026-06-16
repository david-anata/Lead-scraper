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
    ("reported_gross_profit_cents", ("gross profit", "gross margin")),
    ("net_revenue_cents", ("net revenue", "net sales", "total net revenue", "total revenue", "total sales", "revenue, net")),
    ("gross_sales_cents", ("gross sales", "gross revenue", "product sales", "sales revenue")),
    ("discounts_cents", ("discount", "promotions", "promotional", "markdown")),
    ("returns_cents", ("returns", "refunds", "allowances", "chargebacks")),
    ("cogs_cents", ("cogs", "cost of goods", "cost of sales", "cost of revenue", "product cost")),
    ("freight_3pl_cents", ("freight", "3pl", "fulfillment", "fulfilment", "shipping cost", "logistics", "warehous")),
    ("customer_support_cents", ("customer support", "customer service", "support cost", "cx ")),
    ("other_income_cents", ("other income", "non-operating", "non operating", "miscellaneous income", "interest income")),
    ("net_earnings_cents", ("net earnings", "net income", "net profit", "net loss", "bottom line")),
    ("opex_cents", ("operating expense", "total opex", "opex", "sg&a", "sga", "general & admin", "overhead")),
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
    """Column indices that carry numbers across the data rows (majority rule)."""
    width = max((len(r) for r in table.rows), default=0)
    hits = [0] * width
    counts = [0] * width
    for r in table.rows:
        for i, cell in enumerate(r):
            if not str(cell).strip():
                continue
            counts[i] += 1
            if parse_cents(cell) is not None and re.search(r"\d", str(cell)):
                hits[i] += 1
    cols = [i for i in range(width) if counts[i] and hits[i] >= max(2, counts[i] * 0.5)]
    return cols


def _column_years(table: _Table, value_cols: list[int]) -> dict:
    """Map value-column index -> year, from header tokens when present."""
    years: dict[int, int] = {}
    header = table.header or (table.rows[0] if table.rows else [])
    for i in value_cols:
        cell = header[i] if i < len(header) else ""
        m = _YEAR_RE.search(str(cell))
        if m:
            years[i] = int(m.group(1))
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

    # First pass: collect every year token across all files to fix current/prior.
    parsed_files: list[tuple[str, list[_Table]]] = []
    for filename, data in files:
        tables = _read_file(filename, data)
        if not tables:
            files_ignored.append(filename)
            continue
        all_tables.extend(tables)
        parsed_files.append((filename, tables))
        files_read.append((filename, len(tables)))
        brand = _brand_from_filename(filename)
        if brand and brand not in detected_brands:
            detected_brands.append(brand)
        for t in tables:
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
                if field_name:
                    _set_if_absent(current, field_name, cur_v)
                    if prior is not None:
                        _set_if_absent(prior, field_name, prior_v)
                elif channel:
                    if cur_v is not None:
                        current.marketing_by_channel.setdefault(channel, cur_v)
                    if prior is not None and prior_v is not None:
                        prior.marketing_by_channel.setdefault(channel, prior_v)

    # Roll channel marketing into a total when no explicit total line was found.
    for period in (current, prior):
        if period is None:
            continue
        if period.marketing_total_cents is None and period.marketing_by_channel:
            period.marketing_total_cents = sum(period.marketing_by_channel.values())

    # LLM gap-fill for GL / trial-balance dumps the substring matcher can't fold.
    account_mappings: dict = {}
    unmapped_accounts: list = []
    classifier_model = ""
    if use_llm and all_tables and intake_llm.should_classify(current):
        result = intake_llm.classify(all_tables, context_notes=context_notes)
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
