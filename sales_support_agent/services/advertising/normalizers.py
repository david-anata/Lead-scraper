"""Tolerant parsers for the Amazon CSV/XLSX exports the audit ingests.

Same philosophy as cashflow's normalizers: absorb varying header spellings via
alias lists, clean values with typed helpers, and NEVER raise — a malformed row
is skipped, not fatal. Each normalizer returns canonical dataclasses from
schema.py.

Supported inputs:
  * Amazon Ads bulk-operations file (XLSX, multi-sheet) -> SP/SB/SD AdRows
  * Sponsored Products Search Term report (CSV)         -> search_term AdRows
  * Business Report: Detail Page Sales & Traffic (CSV)  -> SalesRows
  * Brand Analytics Search Query Performance (CSV)       -> MarketRows
  * DSP performance export (CSV)                         -> DSP AdRows
  * External costs (CSV; Meta/TikTok/influencer)         -> ExternalCostRows
"""

from __future__ import annotations

import csv
import io
import logging
from typing import Iterable, Optional

from sales_support_agent.services.advertising.schema import (
    AdRow,
    ExternalCostRow,
    MarketRow,
    SalesRow,
    parse_bps,
    parse_cents,
    parse_int,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------


def _norm_key(key: object) -> str:
    return str(key or "").strip().lower().replace("﻿", "")


def _lookup(row: dict) -> dict:
    """Return a case/space-insensitive view of a row keyed by normalized name."""
    return {_norm_key(k): ("" if v is None else v) for k, v in row.items()}


def _get(view: dict, *names: str) -> str:
    """First non-empty value across candidate header spellings (substring-
    tolerant): an alias matches a column if it equals it or is contained in it.
    """
    for name in names:
        target = _norm_key(name)
        if target in view and str(view[target]).strip():
            return str(view[target]).strip()
    # substring fallback — handles "spend(usd)" vs "spend", date-suffixed cols
    for name in names:
        target = _norm_key(name)
        for key, value in view.items():
            if target and target in key and str(value).strip():
                return str(value).strip()
    return ""


def _decode(file_bytes: bytes) -> str:
    return file_bytes.decode("utf-8-sig", errors="replace")


def _read_csv_rows(file_bytes: bytes, header_hint: Iterable[str]) -> list[dict]:
    """Parse CSV bytes into a list of dict rows, tolerating a preamble before
    the real header. The header row is the first row containing any hint token.
    """
    text = _decode(file_bytes)
    if not text.strip():
        return []
    raw_lines = list(csv.reader(io.StringIO(text)))
    if not raw_lines:
        return []
    hints = {_norm_key(h) for h in header_hint}
    header_idx = 0
    for idx, line in enumerate(raw_lines[:15]):
        cells = {_norm_key(c) for c in line}
        if any(any(h in c for c in cells) for h in hints):
            header_idx = idx
            break
    header = [str(c).strip() for c in raw_lines[header_idx]]
    rows: list[dict] = []
    for line in raw_lines[header_idx + 1:]:
        if not any(str(c).strip() for c in line):
            continue
        # pad/truncate to header width
        padded = list(line) + [""] * (len(header) - len(line))
        rows.append(dict(zip(header, padded[: len(header)])))
    return rows


# ---------------------------------------------------------------------------
# Ads bulk-operations file (XLSX) — SP / SB / SD
# ---------------------------------------------------------------------------

# Amazon "Entity" column value -> our entity_level
_ENTITY_MAP = {
    "campaign": "campaign",
    "ad group": "ad_group",
    "keyword": "keyword",
    "product targeting": "target",
    "product ad": "product_ad",
    "product_ad": "product_ad",
    "negative keyword": "negative_keyword",
    "campaign negative keyword": "negative_keyword",
    "negative product targeting": "negative_target",
}


def _ad_type_from_sheet(sheet_name: str) -> Optional[str]:
    name = sheet_name.lower()
    if "sponsored product" in name:
        return "SP"
    if "sponsored brand" in name:
        return "SB"
    if "sponsored display" in name:
        return "SD"
    return None


def normalize_bulk_xlsx(file_bytes: bytes) -> list[AdRow]:
    """Parse an Amazon Ads bulk-operations workbook into AdRows across SP/SB/SD.

    Only performance-bearing entity rows (campaign/ad group/keyword/target/
    product ad) are returned; structural rows without metrics are skipped. The
    full original row is retained in AdRow.raw for traceability.
    """
    try:
        import openpyxl
    except ImportError:  # pragma: no cover - dependency guaranteed in prod
        logger.error("[advertising] openpyxl not installed; cannot parse bulk XLSX")
        return []

    try:
        wb = openpyxl.load_workbook(io.BytesIO(file_bytes), read_only=True, data_only=True)
    except Exception:
        logger.exception("[advertising] failed to open bulk XLSX")
        return []

    out: list[AdRow] = []
    for sheet in wb.worksheets:
        ad_type = _ad_type_from_sheet(sheet.title)
        if ad_type is None:
            continue
        rows_iter = sheet.iter_rows(values_only=True)
        try:
            header = next(rows_iter)
        except StopIteration:
            continue
        header = [str(h).strip() if h is not None else "" for h in header]
        for values in rows_iter:
            row = dict(zip(header, values))
            view = _lookup(row)
            entity_raw = _norm_key(_get(view, "Entity"))
            level = _ENTITY_MAP.get(entity_raw)
            if level in (None, "negative_keyword", "negative_target"):
                continue
            impressions = parse_int(_get(view, "Impressions"))
            clicks = parse_int(_get(view, "Clicks"))
            spend = parse_cents(_get(view, "Spend"))
            if impressions == 0 and clicks == 0 and spend == 0:
                # No performance signal at this row — skip for snapshots.
                continue
            bid_raw = _get(view, "Bid")
            out.append(
                AdRow(
                    ad_type=ad_type,
                    entity_level=level,
                    campaign_name=_get(view, "Campaign Name (Informational only)", "Campaign Name", "Campaign"),
                    ad_group_name=_get(view, "Ad Group Name (Informational only)", "Ad Group Name", "Ad Group"),
                    entity_text=_get(view, "Keyword Text", "Product Targeting Expression", "Resolved Product Targeting Expression", "ASIN", "SKU"),
                    match_type=_get(view, "Match Type"),
                    impressions=impressions,
                    clicks=clicks,
                    spend_cents=spend,
                    sales_cents=parse_cents(_get(view, "Sales", "14 Day Total Sales", "7 Day Total Sales")),
                    orders=parse_int(_get(view, "Orders", "14 Day Total Orders (#)", "7 Day Total Orders (#)")),
                    units=parse_int(_get(view, "Units", "14 Day Total Units (#)", "7 Day Total Units (#)")),
                    bid_cents=parse_cents(bid_raw) if bid_raw else None,
                    raw={str(k): ("" if v is None else str(v)) for k, v in row.items()},
                )
            )
    return out


# ---------------------------------------------------------------------------
# Sponsored Products Search Term report (CSV)
# ---------------------------------------------------------------------------


def normalize_search_term_csv(file_bytes: bytes, ad_type: str = "SP") -> list[AdRow]:
    rows = _read_csv_rows(file_bytes, header_hint=["Customer Search Term", "Search Term", "Targeting"])
    out: list[AdRow] = []
    for row in rows:
        view = _lookup(row)
        term = _get(view, "Customer Search Term", "Search Term")
        if not term:
            continue
        out.append(
            AdRow(
                ad_type=ad_type,
                entity_level="search_term",
                campaign_name=_get(view, "Campaign Name", "Campaign"),
                ad_group_name=_get(view, "Ad Group Name", "Ad Group"),
                entity_text=term,
                match_type=_get(view, "Match Type"),
                impressions=parse_int(_get(view, "Impressions")),
                clicks=parse_int(_get(view, "Clicks")),
                spend_cents=parse_cents(_get(view, "Spend", "Cost")),
                sales_cents=parse_cents(_get(view, "7 Day Total Sales", "14 Day Total Sales", "Total Sales", "Sales")),
                orders=parse_int(_get(view, "7 Day Total Orders (#)", "14 Day Total Orders (#)", "Orders")),
                units=parse_int(_get(view, "7 Day Total Units (#)", "14 Day Total Units (#)", "Units")),
                raw={str(k): str(v) for k, v in row.items()},
            )
        )
    return out


# ---------------------------------------------------------------------------
# Business Report: Detail Page Sales & Traffic (CSV)
# ---------------------------------------------------------------------------


def normalize_business_report_csv(file_bytes: bytes) -> list[SalesRow]:
    rows = _read_csv_rows(file_bytes, header_hint=["Sessions", "ASIN", "Units Ordered"])
    out: list[SalesRow] = []
    for row in rows:
        view = _lookup(row)
        asin = _get(view, "(Child) ASIN", "Child ASIN", "ASIN", "(Parent) ASIN")
        sku = _get(view, "SKU")
        if not asin and not sku:
            continue
        out.append(
            SalesRow(
                asin=asin,
                sku=sku,
                title=_get(view, "Title", "Product Name"),
                sessions=parse_int(_get(view, "Sessions - Total", "Sessions – Total", "Sessions")),
                page_views=parse_int(_get(view, "Page Views - Total", "Page Views – Total", "Page Views")),
                units=parse_int(_get(view, "Units Ordered")),
                ordered_product_sales_cents=parse_cents(_get(view, "Ordered Product Sales")),
                buy_box_pct_bps=parse_bps(_get(view, "Featured Offer (Buy Box) Percentage", "Buy Box Percentage")),
                conversion_bps=parse_bps(_get(view, "Unit Session Percentage")),
                raw={str(k): str(v) for k, v in row.items()},
            )
        )
    return out


# ---------------------------------------------------------------------------
# Brand Analytics Search Query Performance (CSV)
# ---------------------------------------------------------------------------


def normalize_sqp_csv(file_bytes: bytes) -> list[MarketRow]:
    rows = _read_csv_rows(file_bytes, header_hint=["Search Query", "Search Query Volume"])
    out: list[MarketRow] = []
    for row in rows:
        view = _lookup(row)
        query = _get(view, "Search Query")
        if not query:
            continue
        out.append(
            MarketRow(
                search_query=query,
                asin=_get(view, "ASIN"),
                search_query_volume=parse_int(_get(view, "Search Query Volume")),
                impressions_total=parse_int(_get(view, "Impressions: Total Count", "Impressions Total Count")),
                impression_share_bps=parse_bps(_get(view, "Impressions: ASIN Share %", "Impressions ASIN Share")),
                clicks_total=parse_int(_get(view, "Clicks: Total Count", "Clicks Total Count")),
                click_share_bps=parse_bps(_get(view, "Clicks: ASIN Share %", "Clicks ASIN Share")),
                purchases_total=parse_int(_get(view, "Purchases: Total Count", "Purchases Total Count")),
                purchase_share_bps=parse_bps(_get(view, "Purchases: ASIN Share %", "Purchases ASIN Share")),
                raw={str(k): str(v) for k, v in row.items()},
            )
        )
    return out


# ---------------------------------------------------------------------------
# DSP performance (CSV) — campaign-level
# ---------------------------------------------------------------------------


def normalize_dsp_csv(file_bytes: bytes) -> list[AdRow]:
    rows = _read_csv_rows(file_bytes, header_hint=["Campaign", "Total Cost", "Impressions"])
    out: list[AdRow] = []
    for row in rows:
        view = _lookup(row)
        campaign = _get(view, "Campaign Name", "Campaign", "Order")
        if not campaign:
            continue
        out.append(
            AdRow(
                ad_type="DSP",
                entity_level="campaign",
                campaign_name=campaign,
                entity_text=campaign,
                impressions=parse_int(_get(view, "Impressions")),
                clicks=parse_int(_get(view, "Clicks", "Click-throughs")),
                spend_cents=parse_cents(_get(view, "Total Cost", "Spend", "Cost")),
                sales_cents=parse_cents(_get(view, "Total Sales", "14 Day Total Sales", "Sales")),
                orders=parse_int(_get(view, "Total Orders", "Orders", "Purchases")),
                units=parse_int(_get(view, "Total Units", "Units")),
                raw={str(k): str(v) for k, v in row.items()},
            )
        )
    return out


# ---------------------------------------------------------------------------
# External costs (CSV) — Meta / TikTok / influencer commissions
# ---------------------------------------------------------------------------

_CHANNEL_ALIASES = {
    "meta": "meta", "facebook": "meta", "fb": "meta", "instagram": "meta",
    "tiktok": "tiktok", "tik tok": "tiktok",
    "influencer": "influencer", "creator": "influencer", "affiliate": "influencer",
    "google": "google", "adwords": "google",
}


def normalize_external_costs_csv(file_bytes: bytes) -> list[ExternalCostRow]:
    rows = _read_csv_rows(file_bytes, header_hint=["Channel", "Amount", "Spend", "Cost"])
    out: list[ExternalCostRow] = []
    for row in rows:
        view = _lookup(row)
        channel_raw = _norm_key(_get(view, "Channel", "Platform", "Source"))
        amount = parse_cents(_get(view, "Amount", "Spend", "Cost", "Commission"))
        if not channel_raw and not amount:
            continue
        channel = _CHANNEL_ALIASES.get(channel_raw, channel_raw or "other")
        cost_type = "commission" if channel == "influencer" else "ad_spend"
        out.append(
            ExternalCostRow(
                channel=channel if channel in {"meta", "tiktok", "influencer", "google"} else "other",
                cost_type=_get(view, "Cost Type", "Type") or cost_type,
                label=_get(view, "Label", "Name", "Campaign") or channel_raw,
                amount_cents=amount,
                note=_get(view, "Note", "Notes"),
            )
        )
    return out
