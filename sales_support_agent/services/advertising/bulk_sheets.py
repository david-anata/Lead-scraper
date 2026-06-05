"""Amazon Ads bulk-sheet generation by ROUND-TRIPPING the user's own upload.

The safest way to emit a Seller-Central-valid bulk file is to take the bulk-
operations workbook the operator already downloaded and mutate it in place:
  * set_bid        -> find the matching keyword/target row, set its Bid +
                      Operation=update
  * create_negative-> append a Negative Keyword row (Operation=create), copying
                      Campaign/Ad Group IDs from a sibling row in that ad group
  * create_keyword -> append a Keyword row (Operation=create, exact match, bid)

Untouched rows keep a blank Operation, so Amazon ignores them. This guarantees
the exact per-ad-type column schema without us reconstructing it from memory.
Ad types with no uploaded bulk file (or unsupported, e.g. STV/DSP) stay manual
tasks in the burn list.

Returns BulkBuildResult: the new xlsx bytes plus per-rec applied/skipped status,
so the page can tell the operator exactly what made it into the sheet.
"""

from __future__ import annotations

import io
import logging
import os
from dataclasses import dataclass, field
from typing import Optional

from sales_support_agent.services.advertising.normalizers import _ad_type_from_sheet, _norm_key
from sales_support_agent.services.advertising.schema import (
    BULK_SUPPORTED,
    Recommendation,
)

logger = logging.getLogger(__name__)

_TEMPLATE_PATH = os.path.join(os.path.dirname(__file__), "templates", "amazon_bulk_template.xlsx")

# rec match-type text -> Amazon bulk value (from the template's Config sheet)
_MATCH_MAP = {
    "negative exact": "negativeExact", "negativeexact": "negativeExact",
    "negative phrase": "negativePhrase", "negativephrase": "negativePhrase",
    "exact": "exact", "phrase": "phrase", "broad": "broad",
}


@dataclass
class BulkBuildResult:
    xlsx_bytes: Optional[bytes] = None
    applied: int = 0
    skipped: int = 0
    applied_titles: list[str] = field(default_factory=list)
    skipped_titles: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    @property
    def has_file(self) -> bool:
        return bool(self.xlsx_bytes) and self.applied > 0


def build_apply_sheet(recommendations: list[Recommendation]) -> BulkBuildResult:
    """Populate Amazon's official bulk template with create-negative and
    create-keyword rows, using the Campaign ID / Ad Group ID carried on each
    recommendation (extracted from the uploaded report CSVs). Produces an
    upload-ready Sponsored Products bulk sheet WITHOUT the operator having to
    download or edit anything.

    Bid changes on existing keywords (set_bid) become Operation=update rows keyed
    by Keyword ID — available when an Amazon Bulk Operations file was uploaded.
    """
    result = BulkBuildResult()
    actionable = [
        r for r in recommendations
        if r.is_bulk_actionable and r.bulk_row.get("action") in ("create_negative", "create_keyword", "set_bid")
        and r.bulk_row.get("ad_type") == "SP"
    ]
    if not actionable:
        result.notes.append("No create-keyword / create-negative / bid-change actions to apply.")
        return result

    try:
        import openpyxl
        wb = openpyxl.load_workbook(_TEMPLATE_PATH)
    except Exception:
        logger.exception("[advertising] could not open bundled Amazon bulk template")
        result.notes.append("Amazon bulk template unavailable.")
        return result

    ws = None
    for sheet in wb.worksheets:
        if _ad_type_from_sheet(sheet.title) == "SP":
            ws = sheet
            break
    if ws is None:
        result.notes.append("Template is missing the Sponsored Products sheet.")
        return result

    header = [str(c.value).strip() if c.value is not None else "" for c in ws[1]]
    col = {name: _col_index(header, name) for name in
           ("Product", "Entity", "Operation", "Campaign ID", "Ad Group ID", "Keyword ID",
            "Campaign Name", "Ad Group Name", "Keyword Text", "Match Type", "Bid", "State")}

    for rec in actionable:
        br = rec.bulk_row
        action = br["action"]
        row = [""] * len(header)

        def put(name, value):
            i = col.get(name)
            if i is not None:
                row[i] = value

        if action == "set_bid":
            # Update an existing keyword's bid — keyed by Keyword ID.
            if not br.get("keyword_id") or not br.get("new_bid_cents"):
                result.skipped += 1
                result.skipped_titles.append(rec.title)
                continue
            put("Product", "Sponsored Products")
            put("Entity", "Keyword")
            put("Operation", "Update")
            put("Campaign ID", br.get("campaign_id", ""))
            put("Ad Group ID", br.get("ad_group_id", ""))
            put("Keyword ID", br["keyword_id"])
            put("Keyword Text", br.get("keyword_text", ""))
            put("Match Type", _MATCH_MAP.get(_norm_key(br.get("match_type")), "exact"))
            put("Bid", _dollars(br["new_bid_cents"]))
            put("State", "enabled")
            ws.append(row)
            result.applied += 1
            result.applied_titles.append(rec.title)
            continue

        # create_negative / create_keyword
        if not br.get("campaign_id") or not br.get("ad_group_id"):
            result.skipped += 1
            result.skipped_titles.append(rec.title)
            continue
        is_neg = action == "create_negative"
        put("Product", "Sponsored Products")
        put("Entity", "Negative Keyword" if is_neg else "Keyword")
        put("Operation", "Create")
        put("Campaign ID", br["campaign_id"])
        put("Ad Group ID", br["ad_group_id"])
        put("Campaign Name", br.get("campaign_name", ""))
        put("Ad Group Name", br.get("ad_group_name", ""))
        put("Keyword Text", br.get("keyword_text", ""))
        put("Match Type", _MATCH_MAP.get(_norm_key(br.get("match_type")), "negativeExact" if is_neg else "exact"))
        put("State", "enabled")
        if not is_neg and br.get("new_bid_cents"):
            put("Bid", _dollars(br["new_bid_cents"]))
        ws.append(row)
        result.applied += 1
        result.applied_titles.append(rec.title)

    if result.applied:
        buf = io.BytesIO()
        wb.save(buf)
        result.xlsx_bytes = buf.getvalue()
        result.notes.append(
            f"{result.applied} change(s) written into Amazon's bulk template — upload directly to "
            "Ads Console → Bulk operations → Upload. No manual editing needed."
        )
    return result


def _col_index(header: list[str], *aliases: str) -> Optional[int]:
    norm = [_norm_key(h) for h in header]
    for alias in aliases:
        target = _norm_key(alias)
        if target in norm:
            return norm.index(target)
    for alias in aliases:
        target = _norm_key(alias)
        for i, h in enumerate(norm):
            if target and target in h:
                return i
    return None


def _dollars(cents: int) -> float:
    return round(cents / 100, 2)


def build_bulk_workbook(uploaded_xlsx_bytes: bytes, recommendations: list[Recommendation]) -> BulkBuildResult:
    """Apply bulk-actionable recommendations to the uploaded workbook."""
    result = BulkBuildResult()

    actionable = [r for r in recommendations if r.is_bulk_actionable and r.bulk_row]
    if not actionable:
        result.notes.append("No bulk-actionable recommendations in this audit.")
        return result

    if not uploaded_xlsx_bytes:
        result.skipped = len(actionable)
        result.skipped_titles = [r.title for r in actionable]
        result.notes.append("No bulk-operations file was uploaded, so changes can't be round-tripped into a sheet.")
        return result

    try:
        import openpyxl
    except ImportError:  # pragma: no cover
        result.notes.append("openpyxl unavailable; cannot generate bulk sheet.")
        return result

    try:
        wb = openpyxl.load_workbook(io.BytesIO(uploaded_xlsx_bytes), data_only=False)
    except Exception:
        logger.exception("[advertising] failed to load uploaded bulk workbook")
        result.notes.append("Uploaded file could not be opened as an Amazon bulk workbook.")
        result.skipped = len(actionable)
        result.skipped_titles = [r.title for r in actionable]
        return result

    # Map each supported ad type to its sheet.
    sheet_for_type: dict[str, object] = {}
    for sheet in wb.worksheets:
        at = _ad_type_from_sheet(sheet.title)
        if at in BULK_SUPPORTED and at not in sheet_for_type:
            sheet_for_type[at] = sheet

    for rec in actionable:
        ad_type = rec.bulk_row.get("ad_type", "")
        sheet = sheet_for_type.get(ad_type)
        if sheet is None:
            result.skipped += 1
            result.skipped_titles.append(rec.title)
            continue
        ok = _apply_rec_to_sheet(sheet, rec)
        if ok:
            result.applied += 1
            result.applied_titles.append(rec.title)
        else:
            result.skipped += 1
            result.skipped_titles.append(rec.title)

    if result.applied:
        buf = io.BytesIO()
        wb.save(buf)
        result.xlsx_bytes = buf.getvalue()
    if result.skipped:
        result.notes.append(
            f"{result.skipped} change(s) could not be matched to a row in the uploaded sheet "
            "(campaign/ad-group/keyword names didn't line up) and remain manual tasks."
        )
    return result


def _sheet_header(sheet) -> list[str]:
    for row in sheet.iter_rows(min_row=1, max_row=1, values_only=True):
        return [str(c).strip() if c is not None else "" for c in row]
    return []


def _apply_rec_to_sheet(sheet, rec: Recommendation) -> bool:
    header = _sheet_header(sheet)
    if not header:
        return False

    idx = {
        "entity": _col_index(header, "Entity"),
        "operation": _col_index(header, "Operation"),
        "campaign_name": _col_index(header, "Campaign Name (Informational only)", "Campaign Name", "Campaign"),
        "ad_group_name": _col_index(header, "Ad Group Name (Informational only)", "Ad Group Name", "Ad Group"),
        "campaign_id": _col_index(header, "Campaign ID"),
        "ad_group_id": _col_index(header, "Ad Group ID"),
        "keyword_text": _col_index(header, "Keyword Text", "Product Targeting Expression"),
        "match_type": _col_index(header, "Match Type"),
        "bid": _col_index(header, "Bid"),
        "product": _col_index(header, "Product"),
    }
    if idx["operation"] is None or idx["entity"] is None:
        return False

    br = rec.bulk_row
    action = br.get("action")
    want_campaign = _norm_key(br.get("campaign_name"))
    want_ad_group = _norm_key(br.get("ad_group_name"))
    want_kw = _norm_key(br.get("keyword_text"))

    if action == "set_bid":
        return _set_bid(sheet, idx, want_campaign, want_ad_group, want_kw, br.get("new_bid_cents"))
    if action in ("create_negative", "create_keyword"):
        return _append_row(sheet, header, idx, rec)
    return False


def _cell_norm(row_cells, col: Optional[int]) -> str:
    if col is None or col >= len(row_cells):
        return ""
    v = row_cells[col]
    return _norm_key(v)


def _set_bid(sheet, idx, want_campaign, want_ad_group, want_kw, new_bid_cents) -> bool:
    if idx["bid"] is None or new_bid_cents is None:
        return False
    for row in sheet.iter_rows(min_row=2):
        cells = [c.value for c in row]
        entity = _cell_norm(cells, idx["entity"])
        if entity not in ("keyword", "product targeting"):
            continue
        if want_kw and _cell_norm(cells, idx["keyword_text"]) != want_kw:
            continue
        if want_campaign and idx["campaign_name"] is not None and _cell_norm(cells, idx["campaign_name"]) != want_campaign:
            continue
        if want_ad_group and idx["ad_group_name"] is not None and _cell_norm(cells, idx["ad_group_name"]) != want_ad_group:
            continue
        row[idx["bid"]].value = _dollars(new_bid_cents)
        row[idx["operation"]].value = "update"
        return True
    return False


def _find_sibling(sheet, idx, want_campaign, want_ad_group) -> Optional[list]:
    """Return cell-values of any row in the same ad group, to copy IDs from."""
    for row in sheet.iter_rows(min_row=2, values_only=True):
        if want_campaign and idx["campaign_name"] is not None and _cell_norm(row, idx["campaign_name"]) != want_campaign:
            continue
        if want_ad_group and idx["ad_group_name"] is not None and _cell_norm(row, idx["ad_group_name"]) != want_ad_group:
            continue
        return list(row)
    return None


def _append_row(sheet, header, idx, rec: Recommendation) -> bool:
    br = rec.bulk_row
    want_campaign = _norm_key(br.get("campaign_name"))
    want_ad_group = _norm_key(br.get("ad_group_name"))
    sibling = _find_sibling(sheet, idx, want_campaign, want_ad_group)
    if sibling is None:
        # Without the campaign/ad-group IDs we can't create a valid row.
        return False

    new_row = [""] * len(header)
    # Copy structural identity from the sibling (IDs, names, product).
    for key in ("campaign_id", "ad_group_id", "campaign_name", "ad_group_name", "product"):
        col = idx[key]
        if col is not None and col < len(sibling):
            new_row[col] = sibling[col]

    is_negative = br["action"] == "create_negative"
    if idx["entity"] is not None:
        new_row[idx["entity"]] = "Negative Keyword" if is_negative else "Keyword"
    if idx["operation"] is not None:
        new_row[idx["operation"]] = "create"
    if idx["keyword_text"] is not None:
        new_row[idx["keyword_text"]] = br.get("keyword_text", "")
    if idx["match_type"] is not None:
        new_row[idx["match_type"]] = "negativeExact" if is_negative else "exact"
    if not is_negative and idx["bid"] is not None and br.get("new_bid_cents"):
        new_row[idx["bid"]] = _dollars(br["new_bid_cents"])

    sheet.append(new_row)
    return True
