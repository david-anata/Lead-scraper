"""Actuals vs Planned reconciliation page.

Three-section view:
  1. Matched pairs — planned obligation linked to its actual bank transaction,
     showing amount variance and timing delta.
  2. Unmatched planned — obligations expected but not yet seen in the bank
     (overdue flagged red, upcoming flagged amber).
  3. Trend suggestions — recurring patterns inferred from bank history that
     aren't yet captured as recurring templates, with one-click "Add" action.
     Prioritises AR (inflow) patterns since ClickUp AR recurring hasn't been
     set up yet.

All matching is run in-memory on page load using matcher.auto_match_transactions()
so the view is always current without a separate sync step.
"""

from __future__ import annotations

import html
import time as _time
from datetime import date, datetime, timedelta
from typing import Any

from sales_support_agent.services.cashflow.cashflow_helpers import (
    _dollar,
    _display_name,
    _page_shell,
)
from sales_support_agent.services.cashflow.matcher import auto_match_transactions, _vendor_similarity
from sales_support_agent.services.cashflow.obligations import list_obligations
from sales_support_agent.services.cashflow.trend_detector import (
    RecurringPattern,
    detect_recurring_patterns,
)


# ---------------------------------------------------------------------------
# Module-level cache — amortises the expensive DB + matching + trend pass
# ---------------------------------------------------------------------------

_REC_CACHE: dict | None = None
_REC_CACHE_TS: float = 0.0
_REC_CACHE_TTL: int = 600  # 10 minutes


# ---------------------------------------------------------------------------
# Public render function
# ---------------------------------------------------------------------------

def render_reconcile_page(*, flash: str = "") -> str:
    global _REC_CACHE, _REC_CACHE_TS

    today = datetime.utcnow().date()

    # ── Cache check ─────────────────────────────────────────────────────────
    # Skip the cache when flash is set — that means a form action (e.g. adding
    # a template) just completed and the user needs to see fresh results.
    cache_fresh = (
        not flash
        and _REC_CACHE is not None
        and (_time.monotonic() - _REC_CACHE_TS) < _REC_CACHE_TTL
    )

    if cache_fresh:
        c = _REC_CACHE
        matched_pairs     = c["matched_pairs"]
        unmatched_planned = c["unmatched_planned"]
        unmatched_posted  = c["unmatched_posted"]
        patterns          = c["patterns"]
    else:
        # ── Expensive DB + matching + trend pass ────────────────────────────
        lookback = today - timedelta(days=90)   # 90-day reconciliation window

        rows = list_obligations(limit=5000)

        # Split into planned (AP/AR obligations) and posted (bank actuals)
        planned = [
            r for r in rows
            if r.get("source") not in ("csv",)
            and r.get("status") in ("planned", "pending", "overdue")
            and r.get("amount_cents", 0) > 0
        ]
        posted = [
            r for r in rows
            if r.get("source") == "csv"
            and r.get("status") in ("posted", "matched")
            and _to_date(r.get("due_date")) is not None
            and _to_date(r.get("due_date")) >= lookback  # type: ignore[operator]
        ]

        # Run in-memory matching
        match_results = auto_match_transactions(posted, planned)

        # Build lookup maps
        planned_by_id: dict = {str(p["id"]): p for p in planned}
        posted_by_id:  dict = {str(p["id"]): p for p in posted}

        matched_pairs: list = []   # (posted, planned, score, reason)
        matched_planned_ids: set = set()
        matched_posted_ids:  set = set()

        for mr in match_results:
            if mr.planned_event_id is None:
                continue
            p_csv  = posted_by_id.get(str(mr.csv_event_id))
            p_plan = planned_by_id.get(str(mr.planned_event_id))
            if p_csv and p_plan:
                matched_pairs.append((p_csv, p_plan, mr.score, mr.reason))
                matched_planned_ids.add(str(mr.planned_event_id))
                matched_posted_ids.add(str(mr.csv_event_id))

        # Also pull DB-already-matched rows (matched_to_id set from prior uploads)
        for r in rows:
            if r.get("status") == "matched" and r.get("matched_to_id"):
                rid = str(r["id"])
                mid = str(r["matched_to_id"])
                if rid not in matched_posted_ids and mid in planned_by_id:
                    plan = planned_by_id.get(mid)
                    if plan:
                        matched_pairs.append((r, plan, 1.0, "db-matched"))
                        matched_planned_ids.add(mid)
                        matched_posted_ids.add(rid)

        unmatched_planned = [
            p for p in planned if str(p["id"]) not in matched_planned_ids
        ]
        unmatched_posted = [
            p for p in posted
            if str(p["id"]) not in matched_posted_ids
            and p.get("status") == "posted"
        ]

        # Detect trend patterns (AR-first)
        try:
            patterns = detect_recurring_patterns(min_occurrences=2, lookback_days=120)
        except Exception:
            patterns = []

        # Store in cache
        _REC_CACHE = {
            "matched_pairs":     matched_pairs,
            "unmatched_planned": unmatched_planned,
            "unmatched_posted":  unmatched_posted,
            "patterns":          patterns,
        }
        _REC_CACHE_TS = _time.monotonic()

    # Metrics (derived from the cached or freshly-computed data)
    matched_count = len(matched_pairs)
    unmatched_p_count = len(unmatched_planned)
    surprise_count = len(unmatched_posted)
    new_ar_suggestions = sum(
        1 for p in patterns
        if not p.already_tracked and p.event_type == "inflow" and p.confidence >= 0.40
    )

    # ── Build HTML sections ────────────────────────────────────────────────
    body = f"""
    <div>
      <p class="eyebrow" style="margin:0 0 10px;text-transform:uppercase;letter-spacing:.18em;font-size:12px;font-weight:800;color:var(--accent);font-family:'Montserrat',sans-serif;">Finance</p>
      <h1>Actuals vs Planned</h1>
      <p class="page-sub" style="margin-top:8px">
        90-day reconciliation · {today.strftime("%B %d, %Y")} ·
        Matching planned AP/AR against real bank transactions
      </p>
    </div>

    <div class="card-grid">
      <div class="metric-card">
        <div class="metric-label">Matched Pairs</div>
        <div class="metric-value positive">{matched_count}</div>
        <div class="metric-note">Obligations confirmed by bank</div>
      </div>
      <div class="metric-card">
        <div class="metric-label">Unmatched Planned</div>
        <div class="metric-value {"negative" if unmatched_p_count else ""}">{unmatched_p_count}</div>
        <div class="metric-note">Expected but not yet in bank</div>
      </div>
      <div class="metric-card">
        <div class="metric-label">Surprise Transactions</div>
        <div class="metric-value {"negative" if surprise_count else ""}">{surprise_count}</div>
        <div class="metric-note">Bank rows with no planned match</div>
      </div>
      <div class="metric-card">
        <div class="metric-label">New AR Suggestions</div>
        <div class="metric-value {"positive" if new_ar_suggestions else ""}">{new_ar_suggestions}</div>
        <div class="metric-note">Recurring inflows to track</div>
      </div>
    </div>

    {_render_matched_section(matched_pairs)}
    {_render_unmatched_planned_section(unmatched_planned, today)}
    {_render_surprise_section(unmatched_posted, today)}
    {_render_trend_section(patterns)}
    """

    return _page_shell("Actuals vs Planned", "reconcile", body, flash=flash)


# ---------------------------------------------------------------------------
# Section renderers
# ---------------------------------------------------------------------------

def _render_matched_section(pairs: list[tuple]) -> str:
    if not pairs:
        return """
        <div class="card">
          <h2>✅ Matched Pairs</h2>
          <div class="empty-state">No matched pairs yet — upload a bank CSV to begin matching.</div>
        </div>"""

    rows_html = ""
    for posted, planned, score, reason in sorted(
        pairs, key=lambda x: str(x[0].get("due_date", "")), reverse=True
    ):
        actual_amt  = int(posted.get("amount_cents") or 0)
        planned_amt = int(planned.get("amount_cents") or 0)
        variance    = actual_amt - planned_amt
        var_pct     = (variance / planned_amt * 100) if planned_amt else 0
        var_class   = "amount-out" if variance < -500 else ("amount-in" if variance > 500 else "")
        var_label   = f"{'+' if variance >= 0 else ''}{_dollar(variance)} ({var_pct:+.0f}%)"

        actual_date  = _to_date(posted.get("due_date"))
        planned_date = _to_date(planned.get("due_date"))
        days_delta   = (actual_date - planned_date).days if actual_date and planned_date else None
        date_label   = (
            f"{'+' if days_delta >= 0 else ''}{days_delta}d"
            if days_delta is not None else "—"
        )
        date_class   = "amount-out" if (days_delta is not None and days_delta > 3) else ""

        type_cls  = "amount-in" if posted.get("event_type") == "inflow" else "amount-out"
        conf_pct  = f"{score * 100:.0f}%"
        conf_cls  = "badge-ok" if score >= 0.8 else ("badge-warning" if score >= 0.5 else "badge-info")

        rows_html += f"""
        <tr>
          <td style="color:var(--muted);font-size:12px;white-space:nowrap">
            {actual_date.strftime("%b %d") if actual_date else "—"}
          </td>
          <td>
            <div style="font-weight:600">{html.escape(_display_name(planned))}</div>
            <div style="font-size:11px;color:var(--muted)">{html.escape(planned.get("category","") or "")}</div>
          </td>
          <td class="{type_cls}">{_dollar(planned_amt)}</td>
          <td class="{type_cls}">{_dollar(actual_amt)}</td>
          <td class="{var_class}" style="font-size:12px">{var_label}</td>
          <td class="{date_class}" style="font-size:12px">{date_label}</td>
          <td><span class="badge {conf_cls}" style="font-size:10px">{conf_pct}</span></td>
        </tr>"""

    return f"""
    <div class="card">
      <h2>✅ Matched — {len(pairs)} pairs</h2>
      <p class="page-sub" style="margin:6px 0 14px">Planned obligations confirmed by a real bank transaction</p>
      <table>
        <thead>
          <tr>
            <th>Date</th>
            <th>Obligation</th>
            <th>Planned $</th>
            <th>Actual $</th>
            <th>Variance</th>
            <th>Timing</th>
            <th>Confidence</th>
          </tr>
        </thead>
        <tbody>{rows_html}</tbody>
      </table>
    </div>"""


def _render_unmatched_planned_section(unmatched: list[dict], today: date) -> str:
    if not unmatched:
        return """
        <div class="card">
          <h2>📋 Unmatched Planned</h2>
          <div class="empty-state">All planned obligations have been matched to bank actuals.</div>
        </div>"""

    overdue = sorted(
        [r for r in unmatched if _to_date(r.get("due_date")) and _to_date(r.get("due_date")) < today],  # type: ignore
        key=lambda r: str(r.get("due_date", "")),
    )
    upcoming = sorted(
        [r for r in unmatched if not _to_date(r.get("due_date")) or _to_date(r.get("due_date")) >= today],  # type: ignore
        key=lambda r: str(r.get("due_date", "")),
    )

    def _rows(events: list[dict], overdue_flag: bool) -> str:
        out = ""
        for r in events:
            d = _to_date(r.get("due_date"))
            days = (today - d).days if d and overdue_flag else ((d - today).days if d else None)
            days_label = (f"{days}d overdue" if overdue_flag else f"in {days}d") if days is not None else "—"
            type_cls   = "amount-in" if r.get("event_type") == "inflow" else "amount-out"
            badge_cls  = "badge-critical" if overdue_flag else "badge-info"
            type_label = "AR" if r.get("event_type") == "inflow" else "AP"
            mark_paid_url = f"/admin/finances/ap/{r['id']}/edit" if r.get("event_type") == "outflow" else f"/admin/finances/ar/{r['id']}/edit"

            out += f"""
            <tr>
              <td style="color:var(--muted);font-size:12px;white-space:nowrap">
                {d.strftime("%b %d") if d else "—"}
              </td>
              <td>
                <div style="font-weight:600">{html.escape(_display_name(r))}</div>
                <div style="font-size:11px;color:var(--muted)">{html.escape(r.get("category","") or "")}</div>
              </td>
              <td><span class="badge {badge_cls}" style="font-size:10px">{type_label}</span></td>
              <td class="{type_cls}">{_dollar(int(r.get("amount_cents") or 0))}</td>
              <td style="font-size:12px;color:{"var(--bad)" if overdue_flag else "var(--muted)"}">{days_label}</td>
              <td>
                <a href="{mark_paid_url}" class="btn btn-secondary btn-sm">Edit</a>
              </td>
            </tr>"""
        return out

    overdue_html   = _rows(overdue, True)
    upcoming_html  = _rows(upcoming, False)
    all_rows_html  = overdue_html + upcoming_html

    return f"""
    <div class="card">
      <h2>⚠️ Unmatched Planned — {len(unmatched)}</h2>
      <p class="page-sub" style="margin:6px 0 14px">
        {len(overdue)} overdue · {len(upcoming)} upcoming — no bank transaction found yet
      </p>
      <table>
        <thead>
          <tr>
            <th>Due</th><th>Obligation</th><th>Type</th><th>Amount</th><th>Status</th><th></th>
          </tr>
        </thead>
        <tbody>{all_rows_html}</tbody>
      </table>
    </div>"""


def _render_surprise_section(unmatched_posted: list[dict], today: date) -> str:
    if not unmatched_posted:
        return """
        <div class="card">
          <h2>🔍 Surprise Transactions</h2>
          <div class="empty-state">No unplanned bank transactions — every actual has a matching obligation.</div>
        </div>"""

    inflows  = [r for r in unmatched_posted if r.get("event_type") == "inflow"]
    outflows = [r for r in unmatched_posted if r.get("event_type") == "outflow"]

    rows_html = ""
    for r in sorted(unmatched_posted, key=lambda r: str(r.get("due_date", "")), reverse=True):
        d = _to_date(r.get("due_date"))
        type_cls   = "amount-in" if r.get("event_type") == "inflow" else "amount-out"
        type_label = "Inflow" if r.get("event_type") == "inflow" else "Outflow"

        rows_html += f"""
        <tr>
          <td style="color:var(--muted);font-size:12px;white-space:nowrap">
            {d.strftime("%b %d") if d else "—"}
          </td>
          <td>
            <div style="font-weight:600">{html.escape(_display_name(r))}</div>
            <div style="font-size:11px;color:var(--muted)">{html.escape(r.get("category","") or "")}</div>
          </td>
          <td class="{type_cls}">{type_label}</td>
          <td class="{type_cls}">{_dollar(int(r.get("amount_cents") or 0))}</td>
          <td>
            <form method="post" action="/admin/finances/recurring/new"
                  style="display:inline" target="_blank">
              <input type="hidden" name="name"
                     value="{html.escape(_display_name(r))}">
              <input type="hidden" name="event_type"
                     value="{html.escape(str(r.get("event_type","outflow")))}">
              <input type="hidden" name="category"
                     value="{html.escape(str(r.get("category","other")))}">
              <input type="hidden" name="amount_dollars"
                     value="{int(r.get("amount_cents",0))/100:.2f}">
              <button type="submit" class="btn btn-secondary btn-sm">Track as Recurring</button>
            </form>
          </td>
        </tr>"""

    net_surprise = (
        sum(r.get("amount_cents", 0) for r in inflows)
        - sum(r.get("amount_cents", 0) for r in outflows)
    )
    net_cls = "amount-in" if net_surprise >= 0 else "amount-out"

    return f"""
    <div class="card">
      <h2>🔍 Surprise Transactions — {len(unmatched_posted)}</h2>
      <p class="page-sub" style="margin:6px 0 14px">
        {len(inflows)} inflows · {len(outflows)} outflows ·
        Net: <span class="{net_cls}">{_dollar(net_surprise)}</span>
        — bank transactions with no corresponding planned obligation
      </p>
      <table>
        <thead>
          <tr>
            <th>Date</th><th>Description</th><th>Direction</th><th>Amount</th><th></th>
          </tr>
        </thead>
        <tbody>{rows_html}</tbody>
      </table>
    </div>"""


def _render_trend_section(patterns: list[RecurringPattern]) -> str:
    # Separate into: untracked (need attention) and already tracked
    untracked = [p for p in patterns if not p.already_tracked]
    tracked   = [p for p in patterns if p.already_tracked]

    # For untracked: AR first (highest priority for the user), then AP
    ar_new = [p for p in untracked if p.event_type == "inflow"  and p.confidence >= 0.35]
    ap_new = [p for p in untracked if p.event_type == "outflow" and p.confidence >= 0.35]

    if not ar_new and not ap_new and not tracked:
        return """
        <div class="card">
          <h2>📈 Recurring Pattern Suggestions</h2>
          <div class="empty-state">Not enough bank history to detect patterns yet. Upload more bank CSVs to enable trend analysis.</div>
        </div>"""

    def _pattern_rows(ps: list[RecurringPattern], show_add: bool = True) -> str:
        out = ""
        for p in ps:
            type_cls  = "amount-in"  if p.event_type == "inflow"  else "amount-out"
            conf_pct  = f"{p.confidence * 100:.0f}%"
            conf_cls  = "badge-ok"      if p.confidence >= 0.75 else (
                        "badge-warning" if p.confidence >= 0.50 else "badge-info")
            tracked_badge = (
                '<span class="badge badge-ok" style="font-size:10px">Tracked ✓</span>'
                if p.already_tracked else ""
            )
            amount_range = (
                f"{_dollar(p.min_amount_cents)}–{_dollar(p.max_amount_cents)}"
                if p.min_amount_cents != p.max_amount_cents
                else _dollar(p.avg_amount_cents)
            )

            add_btn = ""
            if show_add and not p.already_tracked:
                add_btn = f"""
                <form method="post" action="/admin/finances/reconcile/accept-pattern" style="display:inline">
                  <input type="hidden" name="normalized_vendor"  value="{html.escape(p.normalized_vendor)}">
                  <input type="hidden" name="event_type"         value="{html.escape(p.event_type)}">
                  <input type="hidden" name="category"           value="{html.escape(p.category)}">
                  <input type="hidden" name="avg_amount_cents"   value="{p.avg_amount_cents}">
                  <input type="hidden" name="frequency"          value="{html.escape(p.frequency)}">
                  <input type="hidden" name="next_expected"      value="{p.next_expected.isoformat()}">
                  <button type="submit" class="btn btn-primary btn-sm">+ Add Template</button>
                </form>"""

            out += f"""
            <tr>
              <td>
                <div style="font-weight:600">{html.escape(p.normalized_vendor)}</div>
                <div style="font-size:11px;color:var(--muted)">
                  {html.escape(", ".join(p.raw_vendors[:2]))}
                </div>
              </td>
              <td class="{type_cls}">{"↑ AR" if p.event_type == "inflow" else "↓ AP"}</td>
              <td class="{type_cls}">{amount_range}</td>
              <td style="font-size:12px">{html.escape(p.frequency)}</td>
              <td style="font-size:12px;color:var(--muted)">{p.occurrence_count}×</td>
              <td style="font-size:12px;color:var(--muted)">
                {p.last_seen.strftime("%b %d")}
                → <strong>{p.next_expected.strftime("%b %d")}</strong>
              </td>
              <td><span class="badge {conf_cls}" style="font-size:10px">{conf_pct}</span></td>
              <td>{tracked_badge}{add_btn}</td>
            </tr>"""
        return out

    thead = """
        <thead>
          <tr>
            <th>Vendor / Customer</th>
            <th>Type</th>
            <th>Amount</th>
            <th>Cadence</th>
            <th>Seen</th>
            <th>Last → Next</th>
            <th>Confidence</th>
            <th></th>
          </tr>
        </thead>"""

    ar_section = ""
    if ar_new:
        ar_section = f"""
        <div style="margin-bottom:20px">
          <h3 style="font-size:14px;font-weight:700;margin:0 0 10px;color:var(--good)">
            💰 New AR Patterns — not yet in ClickUp ({len(ar_new)})
          </h3>
          <p class="page-sub" style="margin:0 0 12px">
            These inflows appear regularly in your bank but have no planned AR entry.
            Add them as recurring templates so they show in the forecast.
          </p>
          <table>
            {thead}
            <tbody>{_pattern_rows(ar_new)}</tbody>
          </table>
        </div>"""

    ap_section = ""
    if ap_new:
        ap_section = f"""
        <div style="margin-bottom:20px">
          <h3 style="font-size:14px;font-weight:700;margin:0 0 10px;color:var(--warn)">
            ↓ Untracked AP Patterns ({len(ap_new)})
          </h3>
          <p class="page-sub" style="margin:0 0 12px">
            Regular outflows not covered by any recurring template.
          </p>
          <table>
            {thead}
            <tbody>{_pattern_rows(ap_new)}</tbody>
          </table>
        </div>"""

    tracked_section = ""
    if tracked:
        tracked_section = f"""
        <details style="margin-top:12px">
          <summary style="cursor:pointer;font-size:13px;color:var(--muted);font-weight:600">
            Show {len(tracked)} already-tracked patterns ›
          </summary>
          <div style="margin-top:12px">
            <table>
              {thead}
              <tbody>{_pattern_rows(tracked, show_add=False)}</tbody>
            </table>
          </div>
        </details>"""

    return f"""
    <div class="card">
      <h2>📈 Recurring Pattern Suggestions</h2>
      <p class="page-sub" style="margin:6px 0 18px">
        Patterns detected from your last 120 days of bank transactions.
        AR suggestions are highest priority — add them as templates so they appear in your forecast.
      </p>
      {ar_section}
      {ap_section}
      {tracked_section}
    </div>"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(str(value)[:10])
        except ValueError:
            return None
    return None
