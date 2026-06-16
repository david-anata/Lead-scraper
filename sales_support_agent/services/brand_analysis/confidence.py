"""Missing-data + confidence evaluation.

After scoring, look at which inputs were actually present and produce:
  * the short missing-data block that renders directly under the letter grade,
  * a confidence level (High / Medium / Low) from the share of material inputs,
  * the full "Data Gaps to Close" checklist (section 9).

If every material input is present, the short block collapses to a single
confident line instead of a gap list (per spec).
"""

from __future__ import annotations

from typing import Optional

from sales_support_agent.services.brand_analysis.schema import PeriodFinancials

# Material inputs that drive grade confidence. (label, predicate) — predicate
# takes the current period (and a has_yoy flag for the prior-year check).
_MATERIAL = [
    ("Net revenue", lambda p, yoy: p.net_revenue_or_derived() is not None),
    ("COGS / product cost", lambda p, yoy: p.cogs_cents is not None),
    ("Marketing / advertising spend", lambda p, yoy: p.marketing_total_cents is not None),
    ("Reported gross profit (contribution)", lambda p, yoy: p.reported_gross_profit_cents is not None),
    ("Net earnings", lambda p, yoy: p.net_earnings_cents is not None),
    ("Operating expenses", lambda p, yoy: p.opex_cents is not None),
    ("Prior-year financials (for YoY)", lambda p, yoy: yoy),
    ("Balance sheet (assets / equity)", lambda p, yoy: p.total_assets_cents is not None or p.total_equity_cents is not None),
    ("Discounts & gross sales", lambda p, yoy: p.discounts_cents is not None and p.gross_sales_cents is not None),
    ("Returns", lambda p, yoy: p.returns_cents is not None),
    ("Channel-level media mix", lambda p, yoy: bool(p.marketing_by_channel)),
    ("Owned-channel (email/SMS) revenue", lambda p, yoy: p.owned_channel_revenue_cents is not None),
    ("New-vs-returning revenue split", lambda p, yoy: p.new_customer_revenue_cents is not None and p.returning_customer_revenue_cents is not None),
]

# Recommended evidence to close gaps — always worth requesting for an
# acquisition, shown in the long checklist when the related input is absent.
_GAP_HINTS = {
    "Channel-level media mix": "Ad-platform exports (Meta, Google, TikTok) for true CAC / ROAS by channel",
    "New-vs-returning revenue split": "Cohort / repeat-purchase / LTV data and new-vs-returning AOV trend",
    "Owned-channel (email/SMS) revenue": "Email/SMS attributed revenue (Klaviyo or equivalent)",
    "Balance sheet (assets / equity)": "Full balance sheet — intercompany agreements & collectability evidence",
    "Prior-year financials (for YoY)": "Prior-year P&L plus the most recent quarter actuals to confirm trend",
    "Reported gross profit (contribution)": "Reported gross-profit line to compute true contribution margin",
}


def evaluate(current: PeriodFinancials, has_yoy: bool) -> dict:
    present = []
    missing = []
    for label, pred in _MATERIAL:
        (present if pred(current, has_yoy) else missing).append(label)

    share = len(present) / len(_MATERIAL)
    if share >= 0.85:
        confidence = "High"
    elif share >= 0.6:
        confidence = "Medium"
    else:
        confidence = "Low"

    data_sufficient = not missing

    # Short block under the grade.
    if data_sufficient:
        missing_short: list[str] = []
    else:
        missing_short = [_GAP_HINTS.get(m, m) for m in missing]

    # Long checklist (section 9): always-useful acquisition diligence items,
    # plus the specific missing inputs.
    gaps = list(dict.fromkeys(
        [_GAP_HINTS.get(m, m) for m in missing]
        + [
            "Supplier / 3PL contracts, inventory ageing, owner/key-person dependency",
            "Most recent quarter actuals to confirm trend",
        ]
    ))

    return {
        "missing_short": missing_short,
        "confidence": confidence,
        "data_sufficient": data_sufficient,
        "data_gaps": gaps,
        "inputs_present": present,
        "inputs_missing": missing,
        "completeness_pct": int(round(share * 100)),
    }
