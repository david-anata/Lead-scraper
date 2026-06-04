"""Auto-detecting mass upload — drop every Amazon export at once and let the
tool figure out what each file is.

`sniff_kind` classifies one file by its bytes/headers; `route_files` takes a
batch of (filename, bytes) and builds an AuditInputs plus a human-readable
detection report (what was recognized, what was ignored, what's still missing).
This is a routing layer only — the actual parsing stays in normalizers.py.
"""

from __future__ import annotations

import csv
import io
import logging
from dataclasses import dataclass, field

from sales_support_agent.services.advertising.audit import AuditInputs

logger = logging.getLogger(__name__)

# Detection kinds (mirror the AuditInputs fields, minus manual external rows).
KIND_BULK = "bulk"
KIND_ADS_REPORT = "ads_report"
KIND_BUSINESS = "business_report"
KIND_SQP = "sqp"
KIND_DSP = "dsp"
KIND_EXTERNAL = "external"
KIND_COGS = "cogs"
KIND_UNKNOWN = "unknown"

KIND_LABELS = {
    KIND_BULK: "Ads bulk-operations file",
    KIND_ADS_REPORT: "Ads performance report",
    KIND_BUSINESS: "Business Report (Sales & Traffic)",
    KIND_SQP: "Brand Analytics SQP",
    KIND_DSP: "DSP performance",
    KIND_EXTERNAL: "External costs",
    KIND_COGS: "Per-ASIN COGS",
}

# What a real audit needs: at least one ads performance report (for the burn
# list) and the Business Report (for TACoS / gap-to-goal). The bulk file is
# optional — it only powers the downloadable apply-sheet.
CORE_KINDS = (KIND_ADS_REPORT, KIND_BUSINESS)


@dataclass
class IntakeReport:
    detected: dict[str, list[str]] = field(default_factory=dict)  # kind -> [filenames]
    ignored: list[str] = field(default_factory=list)             # filenames we couldn't classify

    def add(self, kind: str, filename: str) -> None:
        self.detected.setdefault(kind, []).append(filename)

    def missing_core(self) -> list[str]:
        return [k for k in CORE_KINDS if k not in self.detected]

    def summary(self) -> str:
        if not self.detected and not self.ignored:
            return "No files uploaded."
        parts = []
        for kind, files in self.detected.items():
            parts.append(f"{KIND_LABELS.get(kind, kind)} ({len(files)})")
        msg = "Detected: " + ", ".join(parts) if parts else "Nothing recognized"
        if self.ignored:
            msg += f". Ignored {len(self.ignored)} unrecognized file(s): {', '.join(self.ignored[:3])}"
        missing = self.missing_core()
        if missing:
            msg += ". Missing for a full audit: " + ", ".join(KIND_LABELS[k] for k in missing)
        return msg


def _looks_like_xlsx(data: bytes) -> bool:
    # XLSX is a ZIP container; the magic bytes are PK\x03\x04.
    return data[:2] == b"PK"


def _header_tokens(data: bytes) -> set[str]:
    """Normalized tokens from the first plausible header row of a CSV, skipping
    any preamble lines. Returns lowercased column names."""
    text = data.decode("utf-8-sig", errors="replace")
    try:
        rows = list(csv.reader(io.StringIO(text)))
    except csv.Error:
        return set()
    known = (
        "search term", "search query", "asin", "sessions", "units ordered",
        "campaign", "ad group", "impressions", "clicks", "spend", "cost",
        "total cost", "channel", "amount", "targeting", "advertised product",
        "match type", "portfolio", "total sales", "purchases",
    )
    for line in rows[:15]:
        cells = [c.strip().lower() for c in line if c and c.strip()]
        if len(cells) >= 2 and any(any(k in c for c in cells) for k in known):
            return set(cells)
    return set()


def _classify_csv(tokens: set[str]) -> str:
    def has(*subs: str) -> bool:
        return any(any(sub in tok for tok in tokens) for sub in subs)

    # Per-ASIN COGS / cost sheet — ASIN/SKU + a cost column, no ad metrics or traffic.
    if (has("asin") or has("sku")) and has("cogs", "unit cost", "landed cost", "cost of goods", "cost") \
            and not has("impressions", "sessions", "clicks", "campaign", "total cost", "search"):
        return KIND_COGS
    # Brand Analytics Search Query Performance
    if has("search query volume") or (has("search query") and has("impressions: total", "purchases: total")):
        return KIND_SQP
    # Business Report: Detail Page Sales & Traffic (no ad metrics)
    if has("(child) asin", "(parent) asin", "child asin") or (has("sessions") and has("units ordered", "ordered product sales")):
        return KIND_BUSINESS
    # External marketing costs (channel + amount, not an ad report)
    if has("channel") and has("amount", "commission") and not has("impressions"):
        return KIND_EXTERNAL
    # Amazon Ads performance report — new reporting console (Total cost / Purchases
    # / Sales / Units sold) OR legacy per-entity export (Total cost (USD) etc.).
    # Signature: per-row ad metrics + a campaign/ad-group/entity column.
    if has("impressions") and has("total cost", "spend", "cpc", "cost per click") and \
            has("campaign", "ad group", "search term", "targeting", "advertised product"):
        return KIND_ADS_REPORT
    return KIND_UNKNOWN


def _xlsx_is_bulk(data: bytes) -> bool:
    """True only if the workbook actually carries SP/SB/SD bulk sheets — so a
    Portfolio Trends / generic .xlsx export isn't mistaken for a bulk file."""
    try:
        import openpyxl
        wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True)
        return any("sponsored" in s.lower() and "campaign" in s.lower() for s in wb.sheetnames)
    except Exception:
        return False


def sniff_kind(filename: str, data: bytes) -> str:
    """Best-effort classification of a single uploaded file."""
    if not data:
        return KIND_UNKNOWN
    name = (filename or "").lower()
    if "dsp" in name:
        return KIND_DSP
    if name.endswith(".xlsx") or _looks_like_xlsx(data):
        return KIND_BULK if _xlsx_is_bulk(data) else KIND_UNKNOWN
    return _classify_csv(_header_tokens(data))


def _merge_csv(existing: bytes, new: bytes) -> bytes:
    """Concatenate two CSVs of the same kind, keeping a single header row."""
    if not existing:
        return new
    tail = new.decode("utf-8-sig", errors="replace").splitlines()
    body = "\n".join(tail[1:]) if len(tail) > 1 else ""
    if not body.strip():
        return existing
    return existing + b"\n" + body.encode("utf-8")


def route_files(files: list[tuple[str, bytes]]) -> tuple[AuditInputs, IntakeReport]:
    """Classify and route a batch of uploaded files into an AuditInputs.

    Multiple CSVs of the same kind are concatenated; multiple XLSX bulk files
    keep the first (workbooks can't be safely concatenated) and the rest are
    noted as ignored so nothing is silently dropped.
    """
    inputs = AuditInputs()
    report = IntakeReport()
    # Single-slot kinds (one file each; extra CSVs of a kind are concatenated).
    field_for = {
        KIND_BULK: "bulk_xlsx",
        KIND_BUSINESS: "business_report_csv",
        KIND_SQP: "sqp_csv",
        KIND_DSP: "dsp_csv",
        KIND_EXTERNAL: "external_costs_csv",
        KIND_COGS: "cogs_csv",
    }
    for filename, data in files:
        if not data:
            continue
        kind = sniff_kind(filename, data)
        if kind == KIND_UNKNOWN:
            report.ignored.append(filename)
            continue
        if kind == KIND_ADS_REPORT:
            # Many ad reports can be uploaded at once (search-term, advertised-
            # product, targeting, …); each is parsed independently.
            inputs.ads_report_csvs.append(data)
            report.add(kind, filename)
            continue
        attr = field_for[kind]
        current = getattr(inputs, attr)
        if current is None:
            setattr(inputs, attr, data)
        elif kind == KIND_BULK:
            report.ignored.append(filename)  # already have a bulk file
            continue
        else:
            setattr(inputs, attr, _merge_csv(current, data))
        report.add(kind, filename)
    return inputs, report
