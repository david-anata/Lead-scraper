"""Free-form prospect intake for the Fulfillment Rate Sheet generator.

Sales reps paste notes, drop spreadsheets/CSVs, and point at a prospect's
website; this layer flattens all of it into one bounded text context for the
LLM extraction step (llm.py). Mirrors brand_analysis/intake.py conventions:
intentionally forgiving — every file is wrapped in try/except, anything
unreadable degrades to a warning, and nothing here ever raises.
"""

from __future__ import annotations

import csv
import io
import logging
import re

logger = logging.getLogger(__name__)

# Size caps (characters ~= bytes for our purposes).
_NOTES_CAP = 20_000
_TEXT_FILE_CAP = 20_000
_WEBSITE_CAP = 15_000
_TOTAL_CAP = 60_000
_MAX_ROWS = 200

_SCRIPT_STYLE_RE = re.compile(r"<(script|style)\b[^>]*>.*?</\1\s*>", re.IGNORECASE | re.DOTALL)
_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")


def _section(header: str, body: str) -> str:
    return f"=== {header} ===\n{body.strip()}\n"


def _read_csv(data: bytes) -> str:
    text = data.decode("utf-8", errors="replace")
    rows = []
    for i, row in enumerate(csv.reader(io.StringIO(text))):
        if i >= _MAX_ROWS:
            break
        rows.append(",".join("" if c is None else str(c) for c in row))
    return "\n".join(rows)


def _read_xlsx(data: bytes) -> str:
    import openpyxl

    wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    try:
        ws = wb.worksheets[0]
        try:
            ws.reset_dimensions()  # some exports zero the dimension
        except Exception:  # noqa: BLE001
            pass
        lines = []
        for i, row in enumerate(ws.iter_rows(values_only=True)):
            if i >= _MAX_ROWS:
                break
            lines.append("\t".join("" if c is None else str(c) for c in row))
        return "\n".join(lines)
    finally:
        try:
            wb.close()
        except Exception:  # noqa: BLE001
            pass


def _read_file(filename: str, data: bytes, warnings: list) -> str:
    """One file -> text body, or "" with a warning appended."""
    name = (filename or "").lower()
    try:
        if name.endswith(".csv"):
            return _read_csv(data)
        if name.endswith((".xlsx", ".xlsm")):
            return _read_xlsx(data)
        if name.endswith((".txt", ".md")):
            return data.decode("utf-8", errors="replace")[:_TEXT_FILE_CAP]
        warnings.append(f"Unsupported file type: {filename} — skipped")
        return ""
    except Exception:  # noqa: BLE001 — intake never raises
        logger.warning("[fulfillment_deck] failed to read file %s", filename, exc_info=True)
        warnings.append(f"Could not read file: {filename} — skipped")
        return ""


def _fetch_website(url: str, warnings: list) -> str:
    """Fetch + crudely de-tag a prospect website. Failure -> warning, never raises."""
    try:
        import requests

        resp = requests.get(
            url,
            timeout=10,
            headers={"User-Agent": "Mozilla/5.0 (compatible; AnataRateSheet/1.0)"},
        )
        html = resp.text or ""
        html = _SCRIPT_STYLE_RE.sub(" ", html)
        text = _TAG_RE.sub(" ", html)
        text = _WS_RE.sub(" ", text).strip()
        return text[:_WEBSITE_CAP]
    except Exception as exc:  # noqa: BLE001 — never raise on a flaky prospect site
        logger.warning("[fulfillment_deck] website fetch failed for %s", url, exc_info=True)
        warnings.append(f"Could not fetch website {url}: {exc.__class__.__name__}")
        return ""


def build_extraction_context(
    notes: str,
    files: list[tuple[str, bytes]],
    website_url: str,
) -> tuple[str, list[str]]:
    """Flatten notes + uploaded files + website into one bounded context string.

    Returns (context_text, warnings). Never raises.
    """
    warnings: list[str] = []
    sections: list[str] = []

    notes = (notes or "").strip()
    if notes:
        sections.append(_section("SALES NOTES", notes[:_NOTES_CAP]))

    for filename, data in files or []:
        body = _read_file(filename, data or b"", warnings)
        if body.strip():
            sections.append(_section(f"FILE: {filename}", body))

    url = (website_url or "").strip()
    if url:
        body = _fetch_website(url, warnings)
        if body:
            sections.append(_section(f"WEBSITE: {url}", body))

    context = "\n".join(sections)
    if len(context) > _TOTAL_CAP:
        context = context[:_TOTAL_CAP]
        warnings.append("Source material truncated to ~60KB for extraction.")
    return context, warnings
