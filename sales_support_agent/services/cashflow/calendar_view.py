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

    # Get starting balance for running totals
    from sales_support_agent.services.cashflow.obligations import list_obligations
    try:
        all_rows = list_obligations(limit=5000)
        csv_rows = sorted(
            [r for r in all_rows if r.get("source")=="csv" and r.get("account_balance_cents") is not None and str(r.get("due_date",""))[:10] <= first_day.isoformat()],
            key=lambda r: str(r.get("due_date",""))
        )
        running = int(csv_rows[-1].get("account_balance_cents",0)) if csv_rows else 0
    except Exception as exc:
        logger.warning("Failed to compute starting balance for calendar: %s", exc)
        running = 0

    # Build calendar grid — weeks starting Monday
    cal = calendar.Calendar(firstweekday=0)  # Monday
    month_weeks = cal.monthdatescalendar(year, month)

    # Day headers
    day_headers = "".join(
        f'<th style="text-align:center;padding:6px 0;font-size:0.8rem;color:#6b7280;font-weight:600">{d}</th>'
        for d in ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
    )

    is_future_month = date(year, month, 1) > today.replace(day=1)

    # Build week rows
    grid_html = ""
    for week in month_weeks:
        week_in = sum(
            sum(e.get("amount_cents",0) for e in events_by_date.get(d, []) if e.get("event_type")=="inflow")
            for d in week
        )
        week_out = sum(
            sum(e.get("amount_cents",0) for e in events_by_date.get(d, []) if e.get("event_type")=="outflow")
            for d in week
        )

        day_cells = ""
        for day_date in week:
            is_today = day_date == today
            is_other_month = day_date.month != month
            is_future = day_date > today

            day_events = events_by_date.get(day_date, [])

            cell_style = "border:1px solid #e5e7eb;vertical-align:top;padding:4px;min-height:80px;width:calc(100%/7)"
            if is_other_month:
                cell_style += ";background:#fafafa"
            if is_today:
                cell_style += ";background:#eff6ff"

            day_num_style = "font-size:0.8rem;font-weight:600;margin-bottom:2px"
            if is_today:
                day_num_style += ";color:#2563EB"
            elif is_other_month:
                day_num_style += ";color:#d1d5db"
            else:
                day_num_style += ";color:#374151"

            events_html = ""
            for ev in day_events[:4]:  # max 4 events per cell
                is_in = ev.get("event_type") == "inflow"
                source = ev.get("source","")
                status = ev.get("status","")

                # Badge
                if status == "posted":
                    badge = '<span style="font-size:0.6rem;background:#e5e7eb;color:#6b7280;padding:1px 4px;border-radius:3px;margin-left:3px">Posted</span>'
                elif source in ("clickup","clickup-recurring"):
                    badge = '<span style="font-size:0.6rem;background:#dbeafe;color:#1d4ed8;padding:1px 4px;border-radius:3px;margin-left:3px">Auto</span>'
                else:
                    badge = '<span style="font-size:0.6rem;background:#fef3c7;color:#92400e;padding:1px 4px;border-radius:3px;margin-left:3px">Manual</span>'

                if is_in:
                    card_bg = "#ede9fe"
                    card_color = "#5b21b6"
                elif status == "posted":
                    card_bg = "#f3f4f6"
                    card_color = "#374151"
                else:
                    card_bg = "#fce7f3"
                    card_color = "#9d174d"

                name = _display_name(ev)
                amt = _dollar(ev.get("amount_cents",0))
                est_marker = " *" if is_future and ev.get("confidence") != "confirmed" else ""

                events_html += f"""
                <div style="background:{card_bg};color:{card_color};border-radius:4px;padding:2px 4px;margin-bottom:2px;font-size:0.7rem;cursor:pointer"
                     title="{html.escape(name)} | {amt} | {ev.get('category','')} | {status}"
                     onclick="showEventDetail('{ev.get('id','')}')">
                  <div style="font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:120px">{html.escape(name[:22])}{'…' if len(name)>22 else ''}{badge}</div>
                  <div style="opacity:0.8">{amt}{est_marker}</div>
                </div>"""

            if len(day_events) > 4:
                events_html += f'<div style="font-size:0.7rem;color:#6b7280">+{len(day_events)-4} more</div>'

            day_cells += f"""
            <td style="{cell_style}">
              <div style="{day_num_style}">{day_date.day}</div>
              {events_html}
            </td>"""

        # Week summary (end of row)
        week_net = week_in - week_out
        net_color = "#16a34a" if week_net >= 0 else "#dc2626"
        running += week_net
        est_label = ' <span style="font-size:0.65rem;color:#9ca3af">(est.)</span>' if is_future_month else ""

        week_summary = f"""
        <td style="border:1px solid #e5e7eb;vertical-align:middle;padding:6px;background:#f8fafc;width:90px;font-size:0.72rem;color:#6b7280;text-align:center">
          <div style="color:#16a34a;font-weight:600">+{_dollar(week_in)}</div>
          <div style="color:#dc2626;font-weight:600">–{_dollar(week_out)}</div>
          <div style="color:{net_color};font-weight:700">{'+' if week_net>=0 else ''}{_dollar(week_net)}{est_label}</div>
          <div style="color:#374151;font-size:0.7rem;margin-top:2px">{_dollar(running)}</div>
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

    body = f"""
    <h1>Finance Calendar</h1>
    <p class="page-sub">{first_day.strftime("%B %Y")} · Scheduled & actual transactions</p>

    <div class="card">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:1rem">
        <div style="display:flex;gap:4px">
          <a href="?year={prev_month.year}&month={prev_month.month}&filter={filter_type}"
             style="padding:6px 12px;border:1px solid #e5e7eb;border-radius:6px;text-decoration:none;color:#374151;font-size:0.9rem">&#8249;</a>
          <span style="padding:6px 16px;font-weight:600;font-size:1rem">{first_day.strftime("%B %Y")}</span>
          <a href="?year={next_month.year}&month={next_month.month}&filter={filter_type}"
             style="padding:6px 12px;border:1px solid #e5e7eb;border-radius:6px;text-decoration:none;color:#374151;font-size:0.9rem">&#8250;</a>
        </div>
        <div style="display:flex;gap:0.25rem;background:#f1f5f9;border-radius:24px;padding:3px">
          {ftab('all','All')}{ftab('income','Income')}{ftab('expenses','Expenses')}
        </div>
      </div>

      <div style="overflow-x:auto">
        <table style="width:100%;border-collapse:collapse;table-layout:fixed">
          <thead>
            <tr>
              {day_headers}
              <th style="text-align:center;padding:6px 0;font-size:0.8rem;color:#6b7280;font-weight:600;width:90px">Week</th>
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
