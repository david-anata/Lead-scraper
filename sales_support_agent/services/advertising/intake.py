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
KIND_SEARCH_TERM = "search_term"
KIND_BUSINESS = "business_report"
KIND_SQP = "sqp"
KIND_DSP = "dsp"
KIND_EXTERNAL = "external"
KIND_UNKNOWN = "unknown"

KIND_LABELS = {
    KIND_BULK: "Ads bulk-operations file",
    KIND_SEARCH_TERM: "Search Term report",
    KIND_BUSINESS: "Business Report (Sales & Traffic)",
    KIND_SQP: "Brand Analytics SQP",
    KIND_DSP: "DSP performance",
    KIND_EXTERNAL: "External costs",
}

# What a complete-enough audit wants; used to nudge for missing reports.
CORE_KINDS = (KIND_BULK, KIND_SEARCH_TERM, KIND_BUSINESS)


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
        "campaign", "impressions", "clicks", "spend", "cost", "channel",
        "amount", "targeting", "match type", "portfolio", "total sales",
    )
    for line in rows[:15]:
        cells = [c.strip().lower() for c in line if c and c.strip()]
        if len(cells) >= 2 and any(any(k in c for c in cells) for k in known):
            return set(cells)
    return set()


def _classify_csv(tokens: set[str]) -> str:
    def has(*subs: str) -> bool:
        return any(any(sub in tok for tok in tokens) for sub in subs)

    if has("customer search term"):
        return KIND_SEARCH_TERM
    if has("search query volume") or (has("search query") and has("impressions: total", "purchases: total")):
        return KIND_SQP
    if has("(child) asin", "child asin") or (has("sessions") and has("units ordered")):
        return KIND_BUSINESS
    if has("channel") and has("amount", "spend", "commission"):
        return KIND_EXTERNAL
    if has("total cost") and has("campaign"):
        return KIND_DSP
    # Generic SP/SB search-term-style report (Targeting/Search term variants) that
    # still carry a search term or targeting column with performance metrics.
    if has("search term") and has("impressions", "clicks"):
        return KIND_SEARCH_TERM
    return KIND_UNKNOWN


def sniff_kind(filename: str, data: bytes) -> str:
    """Best-effort classification of a single uploaded file."""
    if not data:
        return KIND_UNKNOWN
    name = (filename or "").lower()
    if name.endswith(".xlsx") or _looks_like_xlsx(data):
        # Any Amazon Ads workbook export — only bulk sheets carry SP/SB/SD tabs,
        # and the bulk normalizer safely returns [] for non-bulk workbooks.
        return KIND_BULK
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
    field_for = {
        KIND_BULK: "bulk_xlsx",
        KIND_SEARCH_TERM: "search_term_csv",
        KIND_BUSINESS: "business_report_csv",
        KIND_SQP: "sqp_csv",
        KIND_DSP: "dsp_csv",
        KIND_EXTERNAL: "external_costs_csv",
    }
    for filename, data in files:
        if not data:
            continue
        kind = sniff_kind(filename, data)
        if kind == KIND_UNKNOWN:
            report.ignored.append(filename)
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
