"""Shared helpers for the cashflow Finance OS.

Extracted from overview.py so other modules can import without creating
circular dependencies.  overview.py re-exports everything defined here.
"""

from __future__ import annotations

import html as _html
from datetime import date, datetime
from typing import Any

from sales_support_agent.services.cashflow.engine import EventDTO


# ---------------------------------------------------------------------------
# Dollar formatter
# ---------------------------------------------------------------------------

def _dollar(cents: int) -> str:
    neg = cents < 0
    val = abs(cents) / 100
    s = f"${val:,.0f}"
    return f"-{s}" if neg else s


# ---------------------------------------------------------------------------
# Display name helper
# ---------------------------------------------------------------------------

def _display_name(row: dict) -> str:
    """Return the best display name for a cash event row.
    Priority: friendly_name > name > vendor_or_customer > description[:60]
    """
    return (
        (row.get("friendly_name") or "").strip()
        or (row.get("name") or "").strip()
        or (row.get("vendor_or_customer") or "").strip()
        or (row.get("description") or "")[:60]
        or "—"
    )


# ---------------------------------------------------------------------------
# EventDTO converter
# ---------------------------------------------------------------------------

def _events_to_dtos(rows: list[dict[str, Any]]) -> list[EventDTO]:
    out: list[EventDTO] = []
    for r in rows:
        raw_date = r.get("due_date")
        if isinstance(raw_date, str):
            try:
                due = date.fromisoformat(raw_date[:10])
            except ValueError:
                continue
        elif isinstance(raw_date, datetime):
            due = raw_date.date()
        elif isinstance(raw_date, date):
            due = raw_date
        else:
            continue
        out.append(
            EventDTO(
                id=str(r.get("id", "")),
                source=str(r.get("source", "")),
                event_type=str(r.get("event_type", "outflow")),
                category=str(r.get("category", "other")),
                name=str(r.get("name", "")),
                vendor_or_customer=str(r.get("vendor_or_customer", "")),
                amount_cents=int(r.get("amount_cents") or 0),
                due_date=due,
                status=str(r.get("status", "planned")),
                confidence=str(r.get("confidence", "estimated")),
                matched_to_id=r.get("matched_to_id"),
                recurring_rule=r.get("recurring_rule"),
            )
        )
    return out


# ---------------------------------------------------------------------------
# Category ordering constants
# ---------------------------------------------------------------------------

# Category display order and labels — mirrors ClickUp AP/AR list structure
CATEGORY_ORDER: list[tuple[str, str, str]] = [
    # (internal_key, display_label, event_type_filter)  "" = show both
    ("revenue",      "Revenue",                "inflow"),
    ("payroll",      "Payroll",                "outflow"),
    ("rent",         "Rent",                   "outflow"),
    ("debt",         "Debt / Loans",           "outflow"),
    ("software",     "Software & SaaS",        "outflow"),
    ("utilities",    "Utilities",              "outflow"),
    ("insurance",    "Insurance",              "outflow"),
    ("credit_card",  "Credit Card Payments",   "outflow"),
    ("tax",          "Tax",                    "outflow"),
    ("supplies",     "Supplies",               "outflow"),
    ("equipment",    "Equipment",              "outflow"),
    ("meals",        "Meals",                  "outflow"),
    ("fees",         "Bank Fees",              "outflow"),
    ("owner_draw",   "Owner Draw",             "outflow"),
    ("transfer",     "Transfers",              ""),
    ("uncategorized","Uncategorized",          ""),
]

_CAT_ORDER_INDEX: dict[str, int] = {cat: i for i, (cat, _, _) in enumerate(CATEGORY_ORDER)}

# Category icon mapping for visual variety
_CAT_ICON: dict[str, str] = {
    "revenue":      "💰",
    "payroll":      "👥",
    "rent":         "🏠",
    "debt":         "🏦",
    "software":     "💻",
    "utilities":    "⚡",
    "insurance":    "🛡️",
    "credit_card":  "💳",
    "tax":          "🧾",
    "supplies":     "📦",
    "equipment":    "🔧",
    "meals":        "🍽️",
    "fees":         "💸",
    "owner_draw":   "👤",
    "transfer":     "↔️",
    "uncategorized":"📌",
}


# ---------------------------------------------------------------------------
# Inline name cell with pencil edit support
# ---------------------------------------------------------------------------

def _name_cell(row: dict) -> str:
    """Render a name cell with inline edit support."""
    ev_id = row.get("id", "")
    fname = (row.get("friendly_name") or "").strip()
    raw = _display_name(row)
    field = "friendly_name"

    if fname:
        display = f'<span id="disp-{ev_id}">{_html.escape(fname)}</span>'
    else:
        display = f'<span id="disp-{ev_id}" class="unlabeled-badge">⚠ {_html.escape(raw)}</span>'

    return (
        f'<span class="inline-edit-wrap" id="wrap-{ev_id}">'
        f'{display}'
        f'<button class="pencil-btn" onclick="editName(\'{ev_id}\', \'{_html.escape(fname or raw)}\', \'{field}\')" title="Edit label">✏</button>'
        f'</span>'
    )


# ---------------------------------------------------------------------------
# Page shell
# ---------------------------------------------------------------------------

def _page_shell(title: str, active_section: str, body: str, *, flash: str = "") -> str:
    from sales_support_agent.services.admin_nav import render_agent_nav, render_agent_nav_styles

    flash_html = ""
    if flash.startswith("ok:"):
        flash_html = f'<div class="flash-success">{_html.escape(flash[3:])}</div>'
    elif flash.startswith("err:"):
        flash_html = f'<div class="flash-error">{_html.escape(flash[4:])}</div>'

    inline_edit_js = """
<script>
function editName(wrapId, currentVal, fieldName) {
  let evId = wrapId;
  let wrapEl = document.getElementById('wrap-' + wrapId);
  let dispEl = document.getElementById('disp-' + wrapId);
  if (!wrapEl) {
    wrapEl = document.getElementById('wrap-notes-' + wrapId.replace('-notes',''));
    dispEl = document.getElementById('disp-notes-' + wrapId.replace('-notes',''));
    evId = wrapId.replace('-notes','');
  }
  if (!wrapEl) return;
  const input = document.createElement('input');
  input.className = 'inline-edit-input';
  input.value = currentVal || '';
  input.placeholder = 'Enter label...';
  if (dispEl) dispEl.style.display = 'none';
  wrapEl.appendChild(input);
  input.focus();
  input.select();

  function save() {
    const val = input.value.trim();
    fetch('/admin/finances/events/' + evId, {
      method: 'PATCH',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({[fieldName]: val})
    }).then(r => r.json()).then(() => {
      if (dispEl) { dispEl.textContent = val || '⚠ Unlabeled'; dispEl.style.display = ''; }
      if (input.parentNode) input.parentNode.removeChild(input);
    });
  }

  input.addEventListener('blur', save);
  input.addEventListener('keydown', e => { if (e.key === 'Enter') { e.preventDefault(); save(); } });
}
</script>"""

    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>agent | {_html.escape(title)}</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Montserrat:wght@700;800;900&display=swap" rel="stylesheet">
    <style>
      {render_agent_nav_styles()}
    </style>
    <link rel="stylesheet" href="/static/finance.css">
    {inline_edit_js}
  </head>
  <body>
    {render_agent_nav(active="finance")}
    <div class="finance-subnav-bar">
      <div class="finance-subnav-inner">
        {_finance_subnav(active_section)}
      </div>
    </div>
    <div class="shell">
      {flash_html}
      {body}
    </div>
  </body>
</html>"""


def _finance_subnav(active: str) -> str:
    items = [
        ("Overview", "/admin/finances", "overview"),
        ("Forecast", "/admin/finances/forecast", "forecast"),
        ("Payables (AP)", "/admin/finances/ap", "ap"),
        ("Receivables (AR)", "/admin/finances/ar", "ar"),
        ("Ledger", "/admin/finances/ledger", "ledger"),
        ("Calendar", "/admin/finances/calendar", "calendar"),
        ("Alerts", "/admin/finances/alerts", "alerts"),
        ("Scenario", "/admin/finances/scenario", "scenario"),
        ("Upload CSV", "/admin/finances/upload", "upload"),
        ("Recurring", "/admin/finances/recurring", "recurring"),
        ("Reconcile", "/admin/finances/reconcile", "reconcile"),
        ("QuickBooks", "/admin/finances/qbo", "qbo"),
    ]
    links = "".join(
        f'<a href="{href}" class="subnav-link{"" if key != active else " active"}">{_html.escape(label)}</a>'
        for label, href, key in items
    )
    return f'<nav class="subnav">{links}</nav>'
