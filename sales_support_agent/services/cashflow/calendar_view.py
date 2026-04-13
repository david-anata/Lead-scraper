"""Finance Calendar view — monthly grid of cash events."""
from __future__ import annotations

import html
import logging
import calendar
from datetime import date, datetime, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

from sales_support_agent.services.cashflow.cashflow_helpers import (
    _dollar,
    _display_name,
    _page_shell,
)
from sales_support_agent.services.cashflow.obligations import get_events_for_range


def render_calendar_page(
    *,
    year: Optional[int] = None,
    month: Optional[int] = None,
    filter_type: str = "all",  # "all" | "income" | "expenses"
    flash: str = "",
) -> str:
    """Render the Finance Calendar page with monthly grid of cash events."""
    today = datetime.utcnow().date()
    if not year:
        year = today.year
    if not month:
        month = today.month

    # Month navigation
    first_day = date(year, month, 1)
    last_day = date(year, month, calendar.monthrange(year, month)[1])

    prev_month = (first_day - timedelta(days=1))
    next_month = (last_day + timedelta(days=1))

    # Fetch all events for this month
    month_start = first_day - timedelta(days=first_day.weekday())  # go back to Monday
    month_end = last_day + timedelta(days=6 - last_day.weekday())  # go forward to Sunday

    try:
        events = get_events_for_range(month_start, month_end)
    except Exception as exc:
        logger.warning("Failed to fetch calendar events for %s-%s: %s", month_start, month_end, exc)
        events = []

    # Apply filter
    if filter_type == "income":
        events = [e for e in events if e.get("event_type") == "inflow"]
    elif filter_type == "expenses":
        events = [e for e in events if e.get("event_type") == "outflow"]

    # Index events by date
    events_by_date: dict[date, list[dict]] = {}
    for ev in events:
        d_str = str(ev.get("due_date",""))[:10]
        if not d_str:
            continue
        try:
            d = date.fromisoformat(d_str)
            events_by_date.setdefault(d, []).append(ev)
        except Exception as exc:
            logger.debug("Skipping event with unparseable due_date: %s", exc)

    # Get starting balance — kv_store snapshot first, CSV scan fallback
    from sales_support_agent.services.cashflow.obligations import list_obligations
    from collections import defaultdict
    running = 0
    csv_rows = []
    is_est = True  # set False below if we find a real balance

    try:
        from sales_support_agent.models.database import kv_get_json
        snap = kv_get_json("balance_snapshot")
        if snap and snap.get("balance_cents") is not None:
            running = int(snap["balance_cents"])
            is_est = False
    except Exception:
        pass

    if running == 0:
        # Fallback: scan CSV rows up to first_day
        try:
            all_rows = list_obligations(limit=5000)
            csv_rows = sorted(
                [r for r in all_rows
                 if r.get("source") == "csv"
                 and r.get("account_balance_cents") is not None
                 and str(r.get("due_date", ""))[:10] <= first_day.isoformat()],
                key=lambda r: str(r.get("due_date", ""))
            )
            if csv_rows:
                running = int(csv_rows[-1].get("account_balance_cents", 0))
                is_est = False
        except Exception as exc:
            logger.warning("Failed to compute starting balance for calendar: %s", exc)

    # Build separate net maps for actuals (posted/matched) vs planned.
    # Past days use actual_net so posted transactions are the truth.
    # Future days use planned_net for the projection.
    # Mixing them on past days was causing double-counting when a planned
    # event and its matching posted transaction both existed for the same day.
    actual_net:  dict[date, int] = defaultdict(int)  # posted / matched
    planned_net: dict[date, int] = defaultdict(int)  # planned / pending / overdue

    for ev in events:
        d_str = str(ev.get("due_date", ""))[:10]
        if not d_str:
            continue
        try:
            d = date.fromisoformat(d_str)
        except Exception:
            continue
        amt = int(ev.get("amount_cents") or 0)
        signed = amt if ev.get("event_type") == "inflow" else -amt
        status = ev.get("status", "")
        if status in ("posted", "matched"):
            actual_net[d] += signed
        elif status in ("planned", "pending", "overdue"):
            planned_net[d] += signed

    # Pre-compute EOD balance: actual net for past days, planned net for future
    eod_balance: dict[date, int] = {}
    walk = month_start
    bal = running
    while walk <= month_end:
        if walk <= today:
            bal += actual_net.get(walk, 0)
        else:
            bal += planned_net.get(walk, 0)
        eod_balance[walk] = bal
        walk += timedelta(days=1)

    MIN_BALANCE_CENTS = 1_000_000  # $10k floor for color cues

    # Build calendar grid — weeks starting Monday
    cal = calendar.Calendar(firstweekday=0)  # Monday
    month_weeks = cal.monthdatescalendar(year, month)

    # Day headers
    day_headers = "".join(
        f'<th style="text-align:center;padding:6px 0;font-size:0.8rem;color:#6b7280;font-weight:600">{d}</th>'
        for d in ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    )
    day_headers += '<th style="text-align:center;padding:6px 0;font-size:0.8rem;color:#6b7280;font-weight:600;width:96px">Week Summary</th>'

    is_future_month = date(year, month, 1) > today.replace(day=1)

    def _status_badge(status: str, source: str) -> str:
        if status == "posted":
            return '<span style="font-size:0.6rem;background:#d1fae5;color:#065f46;padding:1px 4px;border-radius:3px;margin-left:3px">✓ Posted</span>'
        if status == "matched":
            return '<span style="font-size:0.6rem;background:#d1fae5;color:#065f46;padding:1px 4px;border-radius:3px;margin-left:3px">✓ Matched</span>'
        if status == "overdue":
            return '<span style="font-size:0.6rem;background:#fee2e2;color:#991b1b;padding:1px 4px;border-radius:3px;margin-left:3px">Overdue</span>'
        if source in ("clickup", "clickup-recurring", "qbo"):
            return '<span style="font-size:0.6rem;background:#dbeafe;color:#1d4ed8;padding:1px 4px;border-radius:3px;margin-left:3px">Auto</span>'
        return '<span style="font-size:0.6rem;background:#fef3c7;color:#92400e;padding:1px 4px;border-radius:3px;margin-left:3px">Manual</span>'

    # Build week rows
    grid_html = ""
    week_running = running  # tracks EOD balance at end of each week for summary

    for week in month_weeks:
        # For past days: count only actuals (posted/matched) to avoid double-counting
        # with planned items that have already cleared. For future days: count planned.
        week_in = week_out = 0
        for d in week:
            is_past_day = d <= today
            for e in events_by_date.get(d, []):
                status = e.get("status", "")
                is_actual = status in ("posted", "matched")
                is_planned = status in ("planned", "pending", "overdue")
                if is_past_day and not is_actual:
                    continue   # skip planned items on past days
                if not is_past_day and not is_planned:
                    continue   # skip actuals on future days (shouldn't happen)
                amt = e.get("amount_cents", 0)
                if e.get("event_type") == "inflow":
                    week_in  += amt
                else:
                    week_out += amt

        day_cells = ""
        for day_date in week:
            is_today = day_date == today
            is_other_month = day_date.month != month
            is_future = day_date > today
            day_events = events_by_date.get(day_date, [])
            day_eod = eod_balance.get(day_date)

            # Cell background: neutral for other-month, teal for today,
            # light green/yellow/red based on projected balance health
            if is_other_month:
                cell_bg = "#fafafa"
            elif is_today:
                cell_bg = "#eff6ff"
            elif day_eod is not None and day_eod < 0:
                cell_bg = "rgba(254,226,226,0.6)"    # red tint — going negative
            elif day_eod is not None and day_eod < MIN_BALANCE_CENTS:
                cell_bg = "rgba(254,243,199,0.6)"    # yellow tint — below $10k floor
            else:
                cell_bg = "#fff"

            cell_style = (
                f"border:1px solid #e5e7eb;vertical-align:top;padding:4px 5px;"
                f"min-height:88px;width:calc(100%/7);background:{cell_bg}"
                + (";outline:2px solid #2563EB;outline-offset:-2px" if is_today else "")
            )

            day_num_style = "font-size:0.8rem;font-weight:700;margin-bottom:2px;display:flex;justify-content:space-between;align-items:center"
            if is_today:
                day_color = "#2563EB"
            elif is_other_month:
                day_color = "#d1d5db"
            else:
                day_color = "#374151"

            # EOD balance mini-indicator
            if day_eod is not None and not is_other_month:
                bal_color = "#16a34a" if day_eod >= MIN_BALANCE_CENTS else ("#d97706" if day_eod >= 0 else "#dc2626")
                bal_str = f'<span style="font-size:0.6rem;color:{bal_color};font-weight:600">{_dollar(day_eod)}</span>'
            else:
                bal_str = ""

            # Separate actual (confirmed bank transactions) from planned
            actual_evs  = [e for e in day_events if e.get("status") in ("posted", "matched")]
            planned_evs = [e for e in day_events if e.get("status") not in ("posted", "matched")]

            # Sort each group: outflows first, then by amount descending
            def _ev_sort(e):
                return (e.get("event_type") != "outflow", -int(e.get("amount_cents") or 0))
            actual_evs.sort(key=_ev_sort)
            planned_evs.sort(key=_ev_sort)

            # Notification dot for days with actual transactions (past days)
            actual_notif = ""
            if actual_evs and not is_other_month:
                actual_out_total = sum(e.get("amount_cents", 0) for e in actual_evs if e.get("event_type") == "outflow")
                actual_in_total  = sum(e.get("amount_cents", 0) for e in actual_evs if e.get("event_type") == "inflow")
                parts = []
                if actual_out_total:
                    parts.append(f'<span style="color:#dc2626">▼{_dollar(actual_out_total)}</span>')
                if actual_in_total:
                    parts.append(f'<span style="color:#16a34a">▲{_dollar(actual_in_total)}</span>')
                actual_notif = (
                    f'<div style="font-size:0.58rem;background:#fff7ed;border:1px solid #fed7aa;border-radius:3px;'
                    f'padding:1px 4px;margin-bottom:2px;display:flex;align-items:center;gap:3px;flex-wrap:wrap">'
                    f'<span style="background:#ea580c;color:#fff;border-radius:2px;padding:0 3px;font-weight:700;font-size:0.55rem">ACTUAL</span>'
                    f'{" ".join(parts)}'
                    f'</div>'
                )

            def _render_ev(ev, is_actual_ev=False):
                is_in = ev.get("event_type") == "inflow"
                source = ev.get("source", "")
                status = ev.get("status", "")
                badge = _status_badge(status, source)
                if is_actual_ev:
                    # Actual (posted/matched) — stronger border, clear color
                    if is_in:
                        card_bg, card_color = "#f0fdf4", "#14532d"
                        card_border = "1.5px solid #4ade80"
                    else:
                        card_bg, card_color = "#fff1f2", "#7f1d1d"
                        card_border = "1.5px solid #f87171"
                elif is_in:
                    card_bg, card_color, card_border = "#ede9fe", "#5b21b6", "1px solid #ddd6fe"
                else:
                    card_bg, card_color, card_border = "#fafafa", "#374151", "1px solid #fecdd3"

                name = _display_name(ev)
                amt = _dollar(ev.get("amount_cents", 0))
                is_ev_future = not is_actual_ev and day_date > today
                est_marker = " <span style='color:#d97706;font-size:0.58rem'>est.</span>" if is_ev_future and ev.get("confidence") != "confirmed" else ""
                return (
                    f'<div style="background:{card_bg};color:{card_color};border:{card_border};border-radius:4px;'
                    f'padding:2px 5px;margin-bottom:2px;font-size:0.67rem;cursor:pointer;line-height:1.4"'
                    f' title="{html.escape(name)} | {amt} | {ev.get("category", "")} | {status}"'
                    f' onclick="showEventDetail(\'{ev.get("id", "")}\');">'
                    f'<div style="font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">'
                    f'{html.escape(name[:24])}{"…" if len(name)>24 else ""}{badge}</div>'
                    f'<div style="opacity:0.9">{"▲" if is_in else "▼"} {amt}{est_marker}</div>'
                    f'</div>'
                )

            # Render: actuals first (prominent), then planned (up to 3 total planned shown)
            events_html = actual_notif
            for ev in actual_evs:
                events_html += _render_ev(ev, is_actual_ev=True)
            shown_planned = 0
            max_planned = max(0, 3 - len(actual_evs))  # fewer planned slots if many actuals
            for ev in planned_evs[:max_planned]:
                events_html += _render_ev(ev, is_actual_ev=False)
                shown_planned += 1

            # "+N more" summary for overflow
            overflow_evs = planned_evs[max_planned:]
            if overflow_evs:
                more_out = sum(e.get("amount_cents", 0) for e in overflow_evs if e.get("event_type") == "outflow")
                more_in  = sum(e.get("amount_cents", 0) for e in overflow_evs if e.get("event_type") == "inflow")
                summary_parts = []
                if more_out:
                    summary_parts.append(f'<span style="color:#dc2626">▼{_dollar(more_out)}</span>')
                if more_in:
                    summary_parts.append(f'<span style="color:#16a34a">▲{_dollar(more_in)}</span>')
                events_html += f'<div style="font-size:0.63rem;color:#6b7280;margin-top:1px">+{len(overflow_evs)} planned {" ".join(summary_parts)}</div>'

            day_cells += f"""
            <td style="{cell_style}">
              <div style="{day_num_style}">
                <span style="color:{day_color}">{day_date.day}</span>
                {bal_str}
              </div>
              {events_html}
            </td>"""

        # Week summary column
        week_net = week_in - week_out
        net_color = "#16a34a" if week_net >= 0 else "#dc2626"
        week_running += week_net
        eow_bal = eod_balance.get(week[-1], week_running)
        eow_color = "#16a34a" if eow_bal >= MIN_BALANCE_CENTS else ("#d97706" if eow_bal >= 0 else "#dc2626")
        has_past  = any(d <= today for d in week)
        has_future = any(d > today for d in week)
        if has_past and has_future:
            week_label = '<span style="font-size:0.58rem;color:#9ca3af">partial</span>'
        elif has_future:
            week_label = '<span style="font-size:0.58rem;color:#6366f1">forecast</span>'
        else:
            week_label = '<span style="font-size:0.58rem;background:#ea580c;color:#fff;border-radius:2px;padding:0 3px">actual</span>'

        # Tally actual vs planned for the week summary
        week_actual_out = sum(
            e.get("amount_cents", 0)
            for d in week
            for e in events_by_date.get(d, [])
            if e.get("event_type") == "outflow" and e.get("status") in ("posted", "matched")
        )
        week_actual_in = sum(
            e.get("amount_cents", 0)
            for d in week
            for e in events_by_date.get(d, [])
            if e.get("event_type") == "inflow" and e.get("status") in ("posted", "matched")
        )
        actual_row = ""
        if week_actual_out or week_actual_in:
            actual_row = (
                f'<div style="border-top:1px solid #fed7aa;margin-top:3px;padding-top:3px">'
                f'<div style="font-size:0.58rem;color:#ea580c;font-weight:700;margin-bottom:1px">✓ Actual</div>'
                + (f'<div style="color:#16a34a;font-size:0.65rem">▲ {_dollar(week_actual_in)}</div>' if week_actual_in else '')
                + (f'<div style="color:#dc2626;font-size:0.65rem">▼ {_dollar(week_actual_out)}</div>' if week_actual_out else '')
                + '</div>'
            )

        week_summary = f"""
        <td style="border:1px solid #e5e7eb;vertical-align:middle;padding:6px 4px;background:#f8fafc;width:96px;font-size:0.72rem;text-align:center">
          <div style="margin-bottom:2px">{week_label}</div>
          <div style="color:#16a34a;font-weight:600;font-size:0.75rem">▲ {_dollar(week_in)}</div>
          <div style="color:#dc2626;font-weight:600;font-size:0.75rem">▼ {_dollar(week_out)}</div>
          <div style="color:{net_color};font-weight:700;margin:2px 0">{'+' if week_net>=0 else ''}{_dollar(week_net)}</div>
          <div style="border-top:1px solid #e5e7eb;margin-top:4px;padding-top:4px">
            <div style="font-size:0.65rem;color:#6b7280">EOW Balance</div>
            <div style="color:{eow_color};font-weight:700;font-size:0.8rem">{_dollar(eow_bal)}</div>
          </div>
          {actual_row}
        </td>"""

        grid_html += f"<tr>{day_cells}{week_summary}</tr>"

    # Filter toggle
    def ftab(t, label):
        active = "background:#0f172a;color:#fff" if filter_type == t else "background:#f1f5f9;color:#374151"
        return f'<a href="?year={year}&month={month}&filter={t}" style="text-decoration:none"><button style="padding:6px 14px;border-radius:20px;border:none;cursor:pointer;font-size:0.85rem;{active}">{label}</button></a>'

    # Build event data JS
    event_data_js = ""
    for e in events:
        ev_id = e.get("id","").replace("'", "\\'")
        ev_name = _display_name(e).replace("'", "\\'")
        ev_amt = _dollar(e.get("amount_cents",0))
        ev_cat = (e.get("category","") or "").replace("'", "\\'")
        ev_status = (e.get("status","") or "").replace("'", "\\'")
        ev_type = (e.get("event_type","") or "").replace("'", "\\'")
        ev_source = (e.get("source","") or "").replace("'", "\\'")
        ev_notes = ((e.get("notes") or "")[:200]).replace("'", "\\'")
        ev_desc = ((e.get("description") or "")[:200]).replace("'", "\\'")
        ev_date = str(e.get("due_date",""))[:10]
        event_data_js += f"eventData['{ev_id}'] = {{'name': '{ev_name}', 'amount': '{ev_amt}', 'category': '{ev_cat}', 'status': '{ev_status}', 'type': '{ev_type}', 'source': '{ev_source}', 'notes': '{ev_notes}', 'desc': '{ev_desc}', 'date': '{ev_date}'}};\n"

    # Opening balance display
    bal_color = "#16a34a" if running >= MIN_BALANCE_CENTS else ("#d97706" if running >= 0 else "#dc2626")

    body = f"""
    <div>
      <p class="eyebrow" style="margin:0 0 8px;text-transform:uppercase;letter-spacing:.18em;font-size:12px;font-weight:800;color:var(--accent);font-family:'Montserrat',sans-serif;">Finance</p>
      <h1>Cash Calendar</h1>
      <p class="page-sub">{first_day.strftime("%B %Y")} · Daily cash position &amp; scheduled transactions</p>
    </div>

    <!-- Balance legend + summary bar -->
    <div style="display:flex;gap:12px;flex-wrap:wrap;margin-bottom:1rem;align-items:center">
      <div style="background:#fff;border:1px solid #e5e7eb;border-radius:10px;padding:10px 16px;min-width:180px">
        <div style="font-size:0.75rem;color:#6b7280;font-weight:600;text-transform:uppercase;letter-spacing:.05em">Opening Balance</div>
        <div style="font-size:1.35rem;font-weight:700;color:{bal_color}">{_dollar(running)}</div>
        <div style="font-size:0.7rem;color:#9ca3af">{'Estimated — no bank data' if is_est else 'From latest CSV'}</div>
      </div>
      <div style="display:flex;gap:8px;flex-wrap:wrap;font-size:0.75rem;color:#6b7280;align-items:center">
        <span>Cell colors: </span>
        <span style="background:#fff1f2;border:1px solid #fecdd3;border-radius:4px;padding:2px 8px">▼ AP/Expense</span>
        <span style="background:#ede9fe;border:1px solid #ddd6fe;border-radius:4px;padding:2px 8px">▲ AR/Income</span>
        <span style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:4px;padding:2px 8px">✓ Posted/Actual</span>
        <span style="background:rgba(254,243,199,0.8);border:1px solid #fde68a;border-radius:4px;padding:2px 8px">⚠ Below $10k floor</span>
        <span style="background:rgba(254,226,226,0.8);border:1px solid #fca5a5;border-radius:4px;padding:2px 8px">🔴 Negative balance</span>
      </div>
    </div>

    <div class="card">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:1rem">
        <div style="display:flex;gap:4px;align-items:center">
          <a href="?year={prev_month.year}&month={prev_month.month}&filter={filter_type}"
             style="padding:6px 12px;border:1px solid #e5e7eb;border-radius:6px;text-decoration:none;color:#374151;font-size:0.9rem">&#8249;</a>
          <span style="padding:6px 16px;font-weight:700;font-size:1.1rem">{first_day.strftime("%B %Y")}</span>
          <a href="?year={next_month.year}&month={next_month.month}&filter={filter_type}"
             style="padding:6px 12px;border:1px solid #e5e7eb;border-radius:6px;text-decoration:none;color:#374151;font-size:0.9rem">&#8250;</a>
          <a href="?year={today.year}&month={today.month}&filter={filter_type}"
             style="padding:4px 10px;border:1px solid #e5e7eb;border-radius:6px;text-decoration:none;color:#374151;font-size:0.8rem;margin-left:4px">Today</a>
        </div>
        <div style="display:flex;gap:0.25rem;background:#f1f5f9;border-radius:24px;padding:3px">
          {ftab('all', 'All')}{ftab('income', 'Income')}{ftab('expenses', 'Expenses')}
        </div>
      </div>

      <div style="overflow-x:auto">
        <table style="width:100%;border-collapse:collapse;table-layout:fixed">
          <thead>
            <tr style="background:#f8fafc">
              {day_headers}
            </tr>
          </thead>
          <tbody>{grid_html}</tbody>
        </table>
      </div>
    </div>

    <!-- Event detail panel -->
    <div id="event-detail-overlay" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,0.3);z-index:100" onclick="closeDetail()"></div>
    <div id="event-detail-panel" style="display:none;position:fixed;right:0;top:0;bottom:0;width:340px;background:#fff;box-shadow:-4px 0 20px rgba(0,0,0,0.15);z-index:101;padding:1.5rem;overflow-y:auto">
      <button onclick="closeDetail()" style="float:right;background:none;border:none;font-size:1.2rem;cursor:pointer;color:#6b7280">✕</button>
      <div id="event-detail-content"></div>
    </div>

    <script>
    const eventData = {{}};
    {event_data_js}

    function showEventDetail(id) {{
      const e = eventData[id];
      if (!e) return;
      const typeColor = e.type === 'inflow' ? '#5b21b6' : '#9d174d';
      document.getElementById('event-detail-content').innerHTML = `
        <h3 style="margin:0 0 1rem;font-size:1rem">${{e.name}}</h3>
        <table style="width:100%;font-size:0.85rem">
          <tr><td style="color:#6b7280;padding:3px 0">Date</td><td>${{e.date}}</td></tr>
          <tr><td style="color:#6b7280;padding:3px 0">Amount</td><td style="color:${{typeColor}};font-weight:600">${{e.amount}}</td></tr>
          <tr><td style="color:#6b7280;padding:3px 0">Type</td><td>${{e.type}}</td></tr>
          <tr><td style="color:#6b7280;padding:3px 0">Category</td><td>${{e.category}}</td></tr>
          <tr><td style="color:#6b7280;padding:3px 0">Status</td><td>${{e.status}}</td></tr>
          <tr><td style="color:#6b7280;padding:3px 0">Source</td><td>${{e.source}}</td></tr>
          ${{e.desc ? `<tr><td style="color:#6b7280;padding:3px 0">Description</td><td style="font-size:0.8rem;color:#9ca3af">${{e.desc}}</td></tr>` : ''}}
          ${{e.notes ? `<tr><td style="color:#6b7280;padding:3px 0">Notes</td><td>${{e.notes}}</td></tr>` : ''}}
        </table>
        <div style="margin-top:1rem">
          <a href="/admin/finances/ap/edit/${{id}}" style="display:inline-block;padding:6px 14px;background:#0f172a;color:#fff;border-radius:6px;text-decoration:none;font-size:0.85rem">Edit</a>
        </div>
      `;
      document.getElementById('event-detail-overlay').style.display = 'block';
      document.getElementById('event-detail-panel').style.display = 'block';
    }}
    function closeDetail() {{
      document.getElementById('event-detail-overlay').style.display = 'none';
      document.getElementById('event-detail-panel').style.display = 'none';
    }}
    </script>"""

    return _page_shell("Finance Calendar", "calendar", body, flash=flash)
