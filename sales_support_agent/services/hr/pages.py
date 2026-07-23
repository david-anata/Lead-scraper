"""HR section pages rendered inside the universal Agent navigation shell."""

from __future__ import annotations

import html
from typing import Optional

from sales_support_agent.services.admin_nav import (
    render_agent_favicon_links,
    render_agent_nav,
    render_agent_nav_styles,
)
from sales_support_agent.services.hr.store import HR_ROLES, EMPLOYEE_TYPES


def _esc(v) -> str:
    return html.escape("" if v is None else str(v))


_HR_STYLES = """
  .hr-main { width:min(100%,1320px); min-width:0; margin:0 auto; padding:32px 24px 64px; }
  .hr-h1 { font-family: Montserrat, Inter, sans-serif; font-weight: 800; font-size: 26px; margin: 0 0 4px; color: #1c2430; }
  .hr-sub { color: rgba(43,54,68,0.6); font-size: 14px; margin: 0 0 24px; }
  .hr-cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px,1fr)); gap: 16px; margin-bottom: 28px; }
  .hr-card { background: #fff; border: 1px solid rgba(43,54,68,0.1); border-radius: 14px; padding: 18px 20px; }
  .hr-card .n { font-size: 30px; font-weight: 800; color: #1c2430; font-family: Montserrat, Inter, sans-serif; }
  .hr-card .l { font-size: 13px; color: rgba(43,54,68,0.6); margin-top: 2px; }
  .hr-tbl { width: 100%; background: #fff; border: 1px solid rgba(43,54,68,0.1); border-radius: 14px; border-collapse: separate; border-spacing: 0; overflow: hidden; }
  .hr-tbl th { text-align: left; font-size: 11px; letter-spacing: .05em; text-transform: uppercase; color: rgba(43,54,68,0.5); padding: 12px 16px; border-bottom: 1px solid rgba(43,54,68,0.08); }
  .hr-tbl td { padding: 13px 16px; border-bottom: 1px solid rgba(43,54,68,0.06); font-size: 14px; }
  .hr-tbl tr:last-child td { border-bottom: none; }
  .hr-badge { display: inline-block; font-size: 11px; font-weight: 600; padding: 2px 9px; border-radius: 999px; border: 1px solid; }
  .hr-row-head { display: flex; align-items: center; justify-content: space-between; margin-bottom: 16px; }
  .hr-btn { display: inline-block; background: #2b3644; color: #fff; text-decoration: none; font-weight: 600; font-size: 14px; padding: 9px 16px; border-radius: 10px; border: none; cursor: pointer; }
  .hr-btn-light { background: #fff; color: #2b3644; border: 1px solid rgba(43,54,68,0.2); }
  .hr-form { background: #fff; border: 1px solid rgba(43,54,68,0.1); border-radius: 14px; padding: 24px; max-width: 640px; }
  .hr-form label { display: block; font-size: 13px; font-weight: 600; color: rgba(43,54,68,0.7); margin: 14px 0 6px; }
  .hr-form input, .hr-form select { width: 100%; padding: 10px 12px; border: 1px solid rgba(43,54,68,0.2); border-radius: 10px; font-size: 14px; font-family: inherit; box-sizing: border-box; }
  .hr-grid2 { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; }
  .hr-actions { margin-top: 22px; display: flex; gap: 10px; }
  .hr-flash { background: #e6f4ec; color: #2e7d5b; border: 1px solid #2e7d5b33; border-radius: 10px; padding: 10px 14px; margin-bottom: 18px; font-size: 14px; }
  .hr-empty { padding: 40px; text-align: center; color: rgba(43,54,68,0.5); }
  .hr-soon { background: #fff; border: 1px dashed rgba(43,54,68,0.25); border-radius: 14px; padding: 48px; text-align: center; color: rgba(43,54,68,0.6); }
  .hr-callout { background:#f3f8fb; border:1px solid #b8dce8; border-radius:14px; padding:18px 20px; margin-bottom:20px; }
  .hr-callout.warn { background:#fff8e8; border-color:#e6bd62; }
  .hr-kicker { font:700 11px Montserrat,Inter,sans-serif; letter-spacing:.06em; text-transform:uppercase; color:#52606d; }
  .hr-stack { display:grid; gap:14px; }
  .hr-inline { display:flex; align-items:center; gap:10px; flex-wrap:wrap; }
  .hr-btn-danger { background:#8b3a32; }
  @media (max-width: 768px) {
    .hr-main { padding:22px 16px 36px; }
    .hr-grid2 { grid-template-columns:1fr; }
    .hr-actions .hr-btn { width:100%; text-align:center; box-sizing:border-box; min-height:44px; }
    .hr-actions { flex-direction:column; }
    .hr-tbl, .hr-tbl tbody, .hr-tbl tr, .hr-tbl td { display:block; width:100%; box-sizing:border-box; }
    .hr-tbl thead { display:none; }
    .hr-tbl tr { padding:10px 0; border-bottom:1px solid rgba(43,54,68,.1); }
    .hr-tbl td { border:0; padding:5px 14px; }
  }
"""


def hr_shell(title: str, active: str, body: str, *, user: Optional[dict]) -> str:
    perms = (user or {}).get("permissions") or set()
    is_super = bool((user or {}).get("is_superadmin"))
    nav = render_agent_nav("hr", hr_section=active, permissions=perms, is_superadmin=is_super, user=user)
    styles = render_agent_nav_styles()
    return f"""<!doctype html>
<html lang="en"><head>
  <meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
  <title>agent | HR — {_esc(title)}</title>
  {render_agent_favicon_links()}
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Montserrat:wght@700;800;900&display=swap" rel="stylesheet">
  <style>{styles}{_HR_STYLES}</style>
</head><body>
  {nav}
  <main class="hr-main">{body}</main>
</body></html>"""


def _flash(flash: Optional[str]) -> str:
    msgs = {
        "created": "✓ Employee added.",
        "updated": "✓ Employee saved.",
        "team_created": "✓ Team created.",
        "exists": "That email already has an employee record.",
        "clocked_in": "Clocked in. Your paid time is running.",
        "clocked_out": "Clocked out. Your time entry was saved.",
        "already_clocked_in": "You are already clocked in.",
        "not_clocked_in": "No open time entry was found.",
        "pto_requested": "PTO request sent for approval.",
        "invalid_request": "Check the PTO dates and requested hours.",
    }
    if not flash:
        return ""
    return f'<div class="hr-flash">{_esc(msgs.get(flash, flash))}</div>'


def render_hr_dashboard(stats: dict, *, user, flash=None) -> str:
    body = f"""
    {_flash(flash)}
    <h1 class="hr-h1">HR Dashboard.</h1>
    <p class="hr-sub">People, time, and payroll for Anata — all in one place.</p>
    <div class="hr-cards">
      <div class="hr-card"><div class="n">{stats.get('active_employees',0)}</div><div class="l">Active employees</div></div>
      <div class="hr-card"><div class="n">{stats.get('teams',0)}</div><div class="l">Teams</div></div>
      <div class="hr-card"><div class="n">{stats.get('onboarding_incomplete',0)}</div><div class="l">Onboarding pending</div></div>
      <div class="hr-card"><div class="n">{stats.get('total_employees',0)}</div><div class="l">Total records</div></div>
    </div>
    <div class="hr-row-head"><div></div><a class="hr-btn" href="/admin/hr/employees/new">+ Add employee</a></div>
    <p class="hr-sub" style="margin-top:0">Time tracking, payroll runs, and reports arrive in the next phases — the
    menu on the left shows what's coming.</p>
    """
    return hr_shell("Dashboard", "dashboard", body, user=user)


def _role_badge(role: str) -> str:
    colors = {"owner": ("#efe7ff", "#5b3aa8"), "admin": ("#efe7ff", "#5b3aa8"),
              "manager": ("#e7f0ff", "#2456b8"), "employee": ("#eef1f4", "#42505f")}
    bg, fg = colors.get(role, ("#eef1f4", "#42505f"))
    return f'<span class="hr-badge" style="background:{bg};color:{fg};border-color:{fg}33">{_esc(role)}</span>'


def render_hr_employees(employees: list, *, user, flash=None) -> str:
    rows = ""
    for e in employees:
        status_dot = "🟢" if e["status"] == "active" else "⚪️"
        pay = (f"${e['hourly_rate']}/hr" if e["employee_type"] == "hourly"
               else f"${e['annual_salary']}/yr")
        rows += f"""<tr>
          <td><a href="/admin/hr/employees/{e['id']}" style="color:#2456b8;text-decoration:none;font-weight:600">{_esc(e['full_name'])}</a></td>
          <td class="hr-sub" style="margin:0">{_esc(e['email'])}</td>
          <td>{_role_badge(e['hr_role'])}</td>
          <td>{_esc(e['employee_type'])}</td>
          <td>{_esc(pay)}</td>
          <td>{status_dot} {_esc(e['status'])}</td>
        </tr>"""
    if not rows:
        rows = '<tr><td colspan="6" class="hr-empty">No employees yet. Add your first one.</td></tr>'
    body = f"""
    {_flash(flash)}
    <div class="hr-row-head">
      <div><h1 class="hr-h1">Employees</h1><p class="hr-sub" style="margin:0">{len(employees)} record(s)</p></div>
      <a class="hr-btn" href="/admin/hr/employees/new">+ Add employee</a>
    </div>
    <table class="hr-tbl">
      <thead><tr><th>Name</th><th>Email</th><th>Role</th><th>Type</th><th>Pay</th><th>Status</th></tr></thead>
      <tbody>{rows}</tbody>
    </table>
    """
    return hr_shell("Employees", "employees", body, user=user)


def render_hr_employee_form(employee: Optional[dict], teams: list, *, user, error=None) -> str:
    is_new = employee is None
    e = employee or {}
    action = "/admin/hr/employees/new" if is_new else f"/admin/hr/employees/{e['id']}"
    title = "Add employee" if is_new else f"Edit {e.get('full_name','')}"

    def _sel(name, options, current):
        opts = "".join(
            f'<option value="{_esc(o)}"{" selected" if o == current else ""}>{_esc(o.title())}</option>'
            for o in options)
        return f'<select name="{name}">{opts}</select>'

    team_opts = '<option value="">— No team —</option>' + "".join(
        f'<option value="{t["id"]}"{" selected" if str(t["id"]) == str(e.get("team_id") or "") else ""}>{_esc(t["name"])}</option>'
        for t in teams)
    err = f'<div class="hr-flash" style="background:#fdecea;color:#8b4c42;border-color:#8b4c4233">{_esc(error)}</div>' if error else ""
    email_field = (f'<input type="email" name="email" required value="{_esc(e.get("email",""))}" placeholder="name@anatainc.com">'
                   if is_new else f'<input type="email" value="{_esc(e.get("email",""))}" disabled>')
    body = f"""
    {err}
    <h1 class="hr-h1">{_esc(title)}</h1>
    <p class="hr-sub">Employee record. Pay is stored exactly; time, payroll and tax setup come in later phases.</p>
    <form class="hr-form" method="post" action="{action}">
      <div class="hr-grid2">
        <div><label>Email *</label>{email_field}</div>
        <div><label>Full name</label><input name="full_name" value="{_esc(e.get('full_name',''))}" placeholder="Jane Doe"></div>
      </div>
      <div class="hr-grid2">
        <div><label>HR role</label>{_sel("hr_role", HR_ROLES, e.get("hr_role","employee"))}</div>
        <div><label>Employee type</label>{_sel("employee_type", EMPLOYEE_TYPES, e.get("employee_type","hourly"))}</div>
      </div>
      <div class="hr-grid2">
        <div><label>Team</label><select name="team_id">{team_opts}</select></div>
        <div><label>Status</label>{_sel("status", ("active","inactive"), e.get("status","active"))}</div>
      </div>
      <div class="hr-grid2">
        <div><label>Hourly rate ($)</label><input name="hourly_rate" value="{_esc(e.get('hourly_rate','0.00'))}" placeholder="25.00"></div>
        <div><label>Annual salary ($)</label><input name="annual_salary" value="{_esc(e.get('annual_salary','0.00'))}" placeholder="85000.00"></div>
      </div>
      <label>Phone</label><input name="phone" value="{_esc(e.get('phone',''))}" placeholder="(555) 123-4567">
      <div class="hr-actions">
        <button type="submit" class="hr-btn">{"Add employee" if is_new else "Save changes"}</button>
        <a class="hr-btn hr-btn-light" href="/admin/hr/employees">Cancel</a>
      </div>
    </form>
    """
    return hr_shell(title, "employees", body, user=user)


def render_hr_teams(teams: list, *, user, flash=None) -> str:
    rows = "".join(
        f"""<tr><td style="font-weight:600">{_esc(t['name'])}</td>
            <td class="hr-sub" style="margin:0">{_esc(t['manager_email'] or '—')}</td>
            <td>{_esc(t['description'] or '')}</td></tr>"""
        for t in teams)
    if not rows:
        rows = '<tr><td colspan="3" class="hr-empty">No teams yet.</td></tr>'
    body = f"""
    {_flash(flash)}
    <h1 class="hr-h1">Teams</h1>
    <p class="hr-sub">Departments / teams employees belong to.</p>
    <table class="hr-tbl" style="margin-bottom:24px">
      <thead><tr><th>Team</th><th>Manager</th><th>Description</th></tr></thead>
      <tbody>{rows}</tbody>
    </table>
    <form class="hr-form" method="post" action="/admin/hr/teams" style="max-width:560px">
      <div style="font-weight:700;margin-bottom:6px">Add a team</div>
      <label>Name *</label><input name="name" required placeholder="Operations">
      <label>Manager email</label><input name="manager_email" placeholder="manager@anatainc.com">
      <label>Description</label><input name="description" placeholder="What this team does">
      <div class="hr-actions"><button class="hr-btn" type="submit">Add team</button></div>
    </form>
    """
    return hr_shell("Teams", "teams", body, user=user)


def render_hr_coming_soon(active: str, title: str, blurb: str, *, user) -> str:
    body = f"""
    <h1 class="hr-h1">{_esc(title)}</h1>
    <div class="hr-soon">
      <div style="font-size:34px">🚧</div>
      <p style="font-size:16px;font-weight:600;color:#2b3644;margin:12px 0 4px">Coming in a later phase</p>
      <p style="margin:0">{_esc(blurb)}</p>
    </div>
    """
    return hr_shell(title, active, body, user=user)


def render_hr_time(entries: list, pto: dict, pto_requests: list, current: Optional[dict], *, user, flash=None) -> str:
    punch_action = "out" if current else "in"
    punch_label = "Clock out" if current else "Clock in"
    can_review = bool((user or {}).get("is_superadmin") or "hr.payroll" in ((user or {}).get("permissions") or set()))
    rows = "".join(f"""<tr><td>{_esc(r['date'])}</td><td>{_esc(r['start_time'] or '—')}</td>
      <td>{_esc(r['stop_time'] or 'Open')}</td><td>{r['hours']:.2f}</td><td>{_esc(r['employee_email'])}</td></tr>""" for r in entries)
    if not rows:
        rows = '<tr><td colspan="5" class="hr-empty">No time recorded yet.</td></tr>'
    requests = "".join(f"""<tr><td>{_esc(r['employee_email'])}</td><td>{_esc(r['start_date'])}–{_esc(r['end_date'])}</td>
      <td>{r['hours']:.2f}</td><td>{_esc(r['status'])}</td><td>{_esc(r['reason'] or '—')}</td><td>
      {f'<form class="hr-inline" method="post" action="/admin/hr/time/pto/{r["id"]}/decision"><button class="hr-btn" name="decision" value="approved">Approve</button><button class="hr-btn hr-btn-light" name="decision" value="denied">Deny</button></form>' if can_review and r['status'] == 'pending' else '—'}</td></tr>""" for r in pto_requests)
    if not requests:
        requests = '<tr><td colspan="6" class="hr-empty">No PTO requests yet.</td></tr>'
    body = f"""
    {_flash(flash)}
    <h1 class="hr-h1">Time & PTO</h1>
    <p class="hr-sub">Simple daily punches. Paid breaks stay inside the workday; no location is collected.</p>
    <div class="hr-callout"><div class="hr-kicker">Your time clock</div>
      <p>{'Clocked in. Your time is running.' if current else 'You are currently clocked out.'}</p>
      <form method="post" action="/admin/hr/time/clock"><input type="hidden" name="action" value="{punch_action}">
        <button class="hr-btn" type="submit">{punch_label}</button></form></div>
    <div class="hr-cards">
      <div class="hr-card"><div class="n">{pto.get('available',0):.2f}</div><div class="l">PTO hours available</div></div>
      <div class="hr-card"><div class="n">{pto.get('accrued',0):.2f}</div><div class="l">PTO hours accrued</div></div>
      <div class="hr-card"><div class="n">{pto.get('used',0):.2f}</div><div class="l">PTO hours used</div></div>
    </div>
    <h2>Recent punches</h2><table class="hr-tbl"><thead><tr><th>Date</th><th>In</th><th>Out</th><th>Hours</th><th>Employee</th></tr></thead><tbody>{rows}</tbody></table>
    <h2 style="margin-top:28px">Request PTO</h2>
    <form class="hr-form" method="post" action="/admin/hr/time/pto">
      <div class="hr-grid2"><div><label for="pto-start">Start date</label><input id="pto-start" type="date" name="start_date" required></div>
      <div><label for="pto-end">End date</label><input id="pto-end" type="date" name="end_date" required></div></div>
      <label for="pto-hours">Hours requested</label><input id="pto-hours" type="number" min="0.25" max="40" step="0.25" name="hours" required>
      <label for="pto-reason">Note (optional)</label><input id="pto-reason" name="reason" maxlength="500">
      <div class="hr-actions"><button class="hr-btn" type="submit">Send request</button></div></form>
    <h2 style="margin-top:28px">PTO requests</h2><table class="hr-tbl"><thead><tr><th>Employee</th><th>Dates</th><th>Hours</th><th>Status</th><th>Note</th><th>Decision</th></tr></thead><tbody>{requests}</tbody></table>
    """
    return hr_shell("Time & PTO", "time", body, user=user)


def render_hr_payroll_control(*, user) -> str:
    body = """
    <h1 class="hr-h1">Payroll control room</h1>
    <p class="hr-sub">Prepare the August 1–15 payroll for payment on August 20.</p>
    <div class="hr-callout warn"><div class="hr-kicker">Not ready for final approval</div>
      <h2 style="margin:6px 0">Opening payroll balances are required</h2>
      <p style="margin-bottom:0">Import 2026 year-to-date wages and taxes, confirm the Utah unemployment rate, and establish a federal electronic payment route. Agent will not guess or mark taxes paid without a confirmation.</p></div>
    <div class="hr-cards">
      <div class="hr-card"><div class="n">Aug 20</div><div class="l">Next live payday</div></div>
      <div class="hr-card"><div class="n">1–15</div><div class="l">Pay-period dates</div></div>
      <div class="hr-card"><div class="n">Blocked</div><div class="l">Approval readiness</div></div>
    </div>
    <div class="hr-callout"><div class="hr-kicker">Approved operating rules</div>
      <ul><li>Semimonthly: 1st–15th paid the 20th; 16th–month end paid the following 5th.</li>
      <li>Saturday paydays move to Friday; Sunday paydays move to Monday.</li>
      <li>Sunday–Saturday overtime week; overtime requires advance approval but worked overtime remains payable.</li>
      <li>Printed/manual checks at launch. Each check number, void, and reissue is recorded.</li></ul></div>
    """
    return hr_shell("Payroll", "payroll", body, user=user)


def render_hr_settings(*, user) -> str:
    body = """
    <h1 class="hr-h1">HR & payroll settings</h1><p class="hr-sub">The policies currently approved for Anata.</p>
    <div class="hr-stack">
      <div class="hr-callout"><div class="hr-kicker">PTO</div><h2>40-hour combined PTO bank</h2><p>Accrues 1 hour per 52 paid hours, usable after 90 days, capped at 40 hours. No negative balance. Unused PTO is not paid at separation unless a written agreement requires it.</p></div>
      <div class="hr-callout"><div class="hr-kicker">Paid holidays</div><p>New Year's Day, Memorial Day, Independence Day, Labor Day, Thanksgiving, and Christmas. W-2 employees become eligible after 90 days.</p></div>
      <div class="hr-callout"><div class="hr-kicker">Tax operations</div><p>Utah TAP access confirmed. Federal deposit schedule: semiweekly. EFTPS and Utah unemployment portal access remain setup checks.</p></div>
    </div>"""
    return hr_shell("Settings", "settings", body, user=user)
