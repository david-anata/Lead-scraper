"""Growth-plan workbook generator — the strategic XLSX deliverable.

Turns the audit (metrics + ranked recommendations + the normalized rows) into a
multi-tab Excel workbook modeled on the target the user validated:
  Exec Brief · Burn List · ASIN Scorecard · Campaign Actions · Negatives to Add
  · Revenue Bridge · Data Requests

Profit-true numbers (break-even ACoS) require per-ASIN COGS; until that's
supplied the Data Requests tab flags it as the #1 gap, exactly like the target.
"""

from __future__ import annotations

import io
from collections import defaultdict
from typing import Optional

from sales_support_agent.services.advertising.schema import (
    AdRow,
    Goals,
    Recommendation,
    SalesRow,
    acos_bps,
)

# --- light styling helpers --------------------------------------------------

_TITLE_FILL = "2B3644"
_HEAD_FILL = "85BBDA"
_BAND_FILL = "F4F8FB"


def _styles():
    from openpyxl.styles import Alignment, Font, PatternFill
    return {
        "title": (Font(bold=True, size=15, color="FFFFFF"), PatternFill("solid", fgColor=_TITLE_FILL)),
        "head": (Font(bold=True, size=11, color="2B3644"), PatternFill("solid", fgColor=_HEAD_FILL)),
        "wrap": Alignment(wrap_text=True, vertical="top"),
    }


def _dollars(cents: Optional[int]) -> float:
    return round((cents or 0) / 100, 2)


def _pct(bps: Optional[int]) -> Optional[float]:
    return None if bps is None else round(bps / 10000, 4)


# --- aggregations -----------------------------------------------------------


def _asin_scorecard(ad_rows, sales_rows, target_acos_bps, cogs=None) -> list[dict]:
    cogs = cogs or {}
    cogs_asin = cogs.get("asin") or {}
    cogs_sku = cogs.get("sku") or {}
    ad_by_asin: dict[str, dict] = defaultdict(lambda: {"spend": 0, "sales": 0, "orders": 0})
    for r in ad_rows:
        if r.entity_level != "product_ad":
            continue
        asin = (r.raw.get("Advertised product ID") or r.raw.get("Advertised product Id") or "").strip()
        if not asin:
            continue
        a = ad_by_asin[asin]
        a["spend"] += r.spend_cents
        a["sales"] += r.sales_cents
        a["orders"] += r.orders

    out: list[dict] = []
    for s in sales_rows:
        if not s.asin:
            continue
        ad = ad_by_asin.get(s.asin, {})
        spend = ad.get("spend", 0)
        sales = ad.get("sales", 0)
        ad_acos = acos_bps(spend, sales) if spend else None

        cogs_cents = cogs_asin.get(s.asin) or (cogs_sku.get(s.sku) if s.sku else None)
        price = round(s.ordered_product_sales_cents / s.units) if s.units else None
        breakeven_bps = None
        if cogs_cents and price and price > cogs_cents:
            breakeven_bps = round((price - cogs_cents) / price * 10000)

        out.append({
            "asin": s.asin,
            "product": s.title[:60],
            "org_sales": s.ordered_product_sales_cents,
            "units": s.units,
            "sessions": s.sessions,
            "cvr_bps": s.conversion_bps,
            "buybox_bps": s.buy_box_pct_bps,
            "ad_spend": spend,
            "ad_sales": sales,
            "ad_acos_bps": ad_acos,
            "cogs_cents": cogs_cents,
            "breakeven_acos_bps": breakeven_bps,
            "verdict": _verdict(s.conversion_bps, s.sessions, ad_acos, target_acos_bps, breakeven_bps),
        })
    out.sort(key=lambda r: r["org_sales"], reverse=True)
    return out


def _verdict(cvr_bps, sessions, ad_acos_bps, target_acos_bps, breakeven_bps=None) -> str:
    # Profit-true when COGS is known: compare ACoS to true break-even.
    if breakeven_bps is not None and ad_acos_bps is not None:
        if ad_acos_bps > breakeven_bps:
            return "Unprofitable on ads — cut/fix to break-even"
        if cvr_bps is not None and cvr_bps < 1000 and sessions >= 3000:
            return "Profitable — but fix CVR (biggest lever)"
        return "Profitable — scale"
    if cvr_bps is not None and cvr_bps < 1000 and sessions >= 3000:
        return "Fix CVR — biggest lever"
    if cvr_bps is not None and cvr_bps >= 2000:  # strong converter deserves investment
        if ad_acos_bps is None or ad_acos_bps <= target_acos_bps * 1.7:
            return "Scale — strong converter"
        return "Scale, but tighten ACoS"
    if ad_acos_bps is not None and ad_acos_bps > target_acos_bps * 1.4:
        return "Tighten ACoS — over target"
    return "Hold / monitor"


def _campaign_actions(ad_rows: list[AdRow], target_acos_bps: int, limit: int = 15) -> list[dict]:
    by_campaign: dict[str, dict] = defaultdict(lambda: {"spend": 0, "sales": 0})
    for r in ad_rows:
        if r.entity_level not in ("product_ad", "campaign", "ad_group"):
            continue
        c = by_campaign[r.campaign_name or "(unnamed)"]
        c["spend"] += r.spend_cents
        c["sales"] += r.sales_cents
    rows = []
    for name, agg in by_campaign.items():
        a = acos_bps(agg["spend"], agg["sales"])
        rows.append({"campaign": name, "spend": agg["spend"], "sales": agg["sales"],
                     "acos_bps": a, **_campaign_move(a, target_acos_bps)})
    rows.sort(key=lambda r: r["spend"], reverse=True)
    return rows[:limit]


def _campaign_move(acos, target) -> dict:
    if acos is None:
        return {"action": "Review", "why": "No attributed sales in window"}
    if acos > 12000:
        return {"action": "PAUSE", "why": f"{acos/100:.0f}% ACoS — unprofitable"}
    if acos > target * 1.3:
        return {"action": "CUT / Tighten", "why": f"{acos/100:.0f}% ACoS over target — harvest winners, negate junk"}
    if acos < target * 0.7:
        return {"action": "SCALE", "why": f"{acos/100:.0f}% ACoS — headroom to push"}
    return {"action": "Hold", "why": "Near target"}


# --- burn-list phase mapping ------------------------------------------------

_ACTION_CATEGORIES = ("negative_keyword", "bid_down", "new_keyword", "bid_up", "budget", "placement", "dayparting")

_PHASE = {
    "negative_keyword": ("1 Stop bleed", "P0", "S"),
    "bid_down": ("1 Stop bleed", "P0", "S"),
    "new_keyword": ("2 Scale winners", "P1", "S"),
    "bid_up": ("2 Scale winners", "P1", "S"),
    "external": ("3 Strategic", "P2", "M"),
    "manual": ("3 Strategic", "P1", "M"),
}


def _impact_dollars(rec: Recommendation) -> float:
    imp = rec.projected_impact or {}
    cents = imp.get("spend_saved_cents") or imp.get("sales_upside_cents") or imp.get("sales_cents") or 0
    return _dollars(cents)


# --- workbook ---------------------------------------------------------------


def build_growth_plan(
    *,
    brand: str,
    summary: dict,
    recommendations: list[Recommendation],
    ad_rows: list[AdRow],
    sales_rows: list[SalesRow],
    goals: Goals,
    narrative: str = "",
    data_window: str = "trailing ~30 days",
    cogs: Optional[dict] = None,
    has_cogs: bool = False,
) -> bytes:
    import openpyxl

    st = _styles()
    wb = openpyxl.Workbook()
    label = (brand or "Account").strip() or "Account"
    target_acos = goals.acos_target_bps or 3000

    # ---- Exec Brief ----
    ws = wb.active
    ws.title = "Exec Brief"
    _title(ws, st, f"{label.upper()} — AMAZON GROWTH PLAN")
    ws.append([f"Executive Brief · Data window: {data_window}"])
    ws.append([])
    _head(ws, st, ["Metric", "Current", "Target", "Gap"])
    rev = summary.get("total_sales_cents", 0)
    rev_target = goals.revenue_target_cents
    _row(ws, ["Revenue", _dollars(rev), _dollars(rev_target) if rev_target else "—",
              _dollars(rev_target - rev) if rev_target else "—"], money_cols=(1, 2, 3))
    _row(ws, ["Ad spend", _dollars(summary.get("ad_spend_cents")), "", ""], money_cols=(1,))
    _row(ws, ["External spend", _dollars(summary.get("external_spend_cents")), "", ""], money_cols=(1,))
    _row(ws, ["ACoS", _pct(summary.get("acos_bps")), _pct(target_acos), ""], pct_cols=(1, 2))
    _row(ws, ["TACoS", _pct(summary.get("tacos_bps")), _pct(goals.tacos_target_bps), ""], pct_cols=(1, 2))
    _row(ws, ["Blended TACoS (incl. off-Amazon)", _pct(summary.get("blended_tacos_bps")), "", ""], pct_cols=(1,))
    _row(ws, ["Units", summary.get("total_units", 0), goals.units_target or "—", ""])
    ws.append([])
    if narrative:
        ws.append(["Strategic read:"])
        ws.append([narrative])
        ws.cell(ws.max_row, 1).alignment = st["wrap"]
    _widths(ws, [34, 16, 16, 16])

    # ---- Burn List ----
    ws = wb.create_sheet("Burn List")
    _title(ws, st, f"{label.upper()} GROWTH BURN LIST")
    ws.append(["Sequenced for impact. P0 = do this week. Est. $ = monthly revenue gained or spend recovered."])
    ws.append([])
    _head(ws, st, ["#", "Phase", "Action", "Detail / How", "Prio", "Effort", "Est. $/mo Impact", "Status"])
    # Lead with concrete actions (negatives / bids / harvests); strategic + manual
    # notes sort to the bottom, so #1 is always a real move.
    burn_ordered = [r for r in recommendations if r.category in _ACTION_CATEGORIES] + \
                   [r for r in recommendations if r.category not in _ACTION_CATEGORIES]
    for i, rec in enumerate(burn_ordered, start=1):
        phase, prio, effort = _PHASE.get(rec.category, ("3 Strategic", "P2", "M"))
        _row(ws, [i, phase, rec.title, rec.detail or rec.rationale, prio, effort,
                  _impact_dollars(rec), "Not started"], money_cols=(6,), wrap_cols=(2, 3))
    _widths(ws, [4, 14, 46, 60, 6, 7, 16, 12])

    # ---- ASIN Scorecard ----
    ws = wb.create_sheet("ASIN Scorecard")
    _title(ws, st, f"{label.upper()} ASIN SCORECARD")
    ws.append(["Organic from Business Report; ad data from Advertised Product report. Joined on ASIN."])
    ws.append([])
    _head(ws, st, ["ASIN", "Product", "Org Sales", "Units", "Sessions", "CVR", "Buy Box",
                   "Ad Spend", "Ad Sales", "Ad ACoS", "COGS/unit", "Break-even ACoS", "Verdict / Move"])
    for r in _asin_scorecard(ad_rows, sales_rows, target_acos, cogs):
        _row(ws, [r["asin"], r["product"], _dollars(r["org_sales"]), r["units"], r["sessions"],
                  _pct(r["cvr_bps"]), _pct(r["buybox_bps"]), _dollars(r["ad_spend"]),
                  _dollars(r["ad_sales"]), _pct(r["ad_acos_bps"]),
                  _dollars(r["cogs_cents"]) if r["cogs_cents"] else "—",
                  _pct(r["breakeven_acos_bps"]) if r["breakeven_acos_bps"] is not None else "—",
                  r["verdict"]],
             money_cols=(2, 7, 8, 10), pct_cols=(5, 6, 9, 11), wrap_cols=(1, 12))
    _widths(ws, [14, 32, 11, 7, 9, 7, 7, 10, 10, 8, 10, 13, 30])

    # ---- Campaign Actions ----
    ws = wb.create_sheet("Campaign Actions")
    _title(ws, st, f"{label.upper()} CAMPAIGN ACTIONS")
    ws.append(["Top campaigns by spend with the specific move."])
    ws.append([])
    _head(ws, st, ["Campaign", "Spend", "Sales", "ACoS", "Action", "Why"])
    for r in _campaign_actions(ad_rows, target_acos):
        _row(ws, [r["campaign"], _dollars(r["spend"]), _dollars(r["sales"]), _pct(r["acos_bps"]),
                  r["action"], r["why"]], money_cols=(1, 2), pct_cols=(3,), wrap_cols=(0, 5))
    _widths(ws, [46, 12, 12, 9, 14, 40])

    # ---- Negatives to Add ----
    ws = wb.create_sheet("Negatives to Add")
    _title(ws, st, "NEGATIVE KEYWORDS / TARGETS TO ADD")
    ws.append(["From the Search Term report — spend with zero or unprofitable sales."])
    ws.append([])
    _head(ws, st, ["Search term / target", "Wasted spend", "Match", "Add to campaign"])
    for rec in recommendations:
        if rec.category != "negative_keyword":
            continue
        br = rec.bulk_row or {}
        _row(ws, [br.get("keyword_text", rec.entity_ref), _impact_dollars(rec),
                  "Neg exact", br.get("campaign_name", "")], money_cols=(1,), wrap_cols=(0, 3))
    _widths(ws, [40, 14, 12, 40])

    # ---- Revenue Bridge ----
    ws = wb.create_sheet("Revenue Bridge")
    _title(ws, st, f"REVENUE BRIDGE TO {(_dollars(rev_target) if rev_target else 'TARGET')}")
    ws.append(["Editable planning model — adjust the assumptions as results come in."])
    ws.append([])
    _head(ws, st, ["Input", "Value", "Source"])
    _row(ws, ["Current monthly revenue", _dollars(rev), "Business Report"], money_cols=(1,))
    _row(ws, ["Current monthly ad spend", _dollars(summary.get("ad_spend_cents")), "Advertised Product"], money_cols=(1,))
    _row(ws, ["Target monthly revenue", _dollars(rev_target) if rev_target else "—", "Goal"], money_cols=(1,))
    gap = (rev_target - rev) if rev_target else 0
    _row(ws, ["Revenue gap to close", _dollars(gap) if rev_target else "—", "Computed"], money_cols=(1,))
    ws.append([])
    ws.append(["Levers (planning estimates):"])
    _row(ws, ["Recover wasted spend (negatives)", _sum_impact(recommendations, "negative_keyword"), "Burn List P0"], money_cols=(1,))
    _row(ws, ["Scale winners (bid-up + harvest)", _sum_impact(recommendations, "bid_up", "new_keyword"), "Burn List P1"], money_cols=(1,))
    _widths(ws, [38, 16, 22])

    # ---- Data Requests ----
    ws = wb.create_sheet("Data Requests")
    _title(ws, st, "REPORTS NEEDED TO MAKE THIS PROFIT-TRUE")
    ws.append(["What's missing, why it matters, and priority. COGS is the #1 gap for guaranteeing profitability."])
    ws.append([])
    _head(ws, st, ["#", "Report / data", "Why it matters", "Priority"])
    reqs = []
    if not has_cogs:
        reqs.append(("Per-ASIN COGS + FBA & referral fees",
                     "The #1 gap. Enables TRUE break-even ACoS per product — guarantee (not estimate) each SKU stays profitable. Today ACoS/TACoS are proxies.", "P0"))
    reqs += [
        ("Targeting / Keyword report", "Unlocks keyword-level bid moves (retune, not just harvest/negate).", "P0"),
        ("FBA Inventory & Days-of-Supply", "Can't scale if best-sellers stock out — sets reorder triggers + caps spend on low-stock SKUs.", "P1"),
        ("Placement report (TOS / Product / Rest)", "Unlocks placement bid modifiers — a fast 5–15% ACoS win by over-weighting Top-of-Search on high-CVR ASINs.", "P1"),
    ]
    for i, (name, why, prio) in enumerate(reqs, start=1):
        _row(ws, [i, name, why, prio], wrap_cols=(1, 2))
    _widths(ws, [4, 34, 64, 8])

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def _sum_impact(recs: list[Recommendation], *categories: str) -> float:
    total = 0
    for r in recs:
        if r.category in categories:
            imp = r.projected_impact or {}
            total += imp.get("spend_saved_cents") or imp.get("sales_upside_cents") or imp.get("sales_cents") or 0
    return _dollars(total)


# --- low-level sheet writers ------------------------------------------------


def _title(ws, st, text: str) -> None:
    ws.append([text])
    cell = ws.cell(ws.max_row, 1)
    cell.font, cell.fill = st["title"]


def _head(ws, st, headers: list[str]) -> None:
    ws.append(headers)
    for col in range(1, len(headers) + 1):
        cell = ws.cell(ws.max_row, col)
        cell.font, cell.fill = st["head"]


def _row(ws, values, *, money_cols=(), pct_cols=(), wrap_cols=()) -> None:
    from openpyxl.styles import Alignment
    ws.append(values)
    r = ws.max_row
    # money_cols / pct_cols / wrap_cols are 0-based indices into `values`.
    for c in money_cols:
        cell = ws.cell(r, c + 1)
        if isinstance(cell.value, (int, float)):
            cell.number_format = "$#,##0.00"
    for c in pct_cols:
        cell = ws.cell(r, c + 1)
        if isinstance(cell.value, (int, float)):
            cell.number_format = "0.0%"
    for c in wrap_cols:
        ws.cell(r, c + 1).alignment = Alignment(wrap_text=True, vertical="top")


def _widths(ws, widths: list[int]) -> None:
    from openpyxl.utils import get_column_letter
    for i, w in enumerate(widths, start=1):
        ws.column_dimensions[get_column_letter(i)].width = w
