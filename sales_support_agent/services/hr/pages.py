"""HR section pages rendered inside the universal Agent navigation shell."""

from __future__ import annotations

import html
from datetime import date
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
  .hr-form input, .hr-form select, .hr-form textarea { width: 100%; padding: 10px 12px; border: 1px solid rgba(43,54,68,0.2); border-radius: 10px; font-size: 16px; font-family: inherit; box-sizing: border-box; }
  .hr-form textarea { min-height:92px; resize:vertical; }
  .hr-btn:focus-visible, .hr-form input:focus-visible, .hr-form select:focus-visible, .hr-form textarea:focus-visible, .hr-tbl a:focus-visible { outline:3px solid #79b8d1; outline-offset:2px; }
  .hr-btn:disabled { opacity:.5; cursor:not-allowed; }
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
  .hr-dashboard-action { display:flex; align-items:center; justify-content:space-between; gap:20px; padding:18px 20px; background:#fff; border:1px solid rgba(43,54,68,.1); border-radius:14px; }
  .hr-dashboard-action .hr-sub { margin:0; max-width:720px; }
  .hr-btn-danger { background:#8b3a32; }
  @media (max-width: 768px) {
    .hr-main { padding:22px 16px 36px; overflow-x:auto; }
    .hr-grid2 { grid-template-columns:1fr; }
    .hr-actions .hr-btn { width:100%; text-align:center; box-sizing:border-box; min-height:44px; }
    .hr-actions { flex-direction:column; }
    .hr-dashboard-action { align-items:stretch; flex-direction:column; }
    .hr-dashboard-action .hr-btn { min-height:44px; text-align:center; }
    .hr-btn { min-height:44px; }
    .hr-tbl { min-width:640px; }
    .hr-form { padding:18px 16px; }
    .hr-cards { grid-template-columns:1fr 1fr; gap:10px; }
    .hr-card { padding:14px; }
    .hr-card .n { font-size:22px; overflow-wrap:anywhere; }
  }
  @media (max-width: 420px) { .hr-cards { grid-template-columns:1fr; } }
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


def render_hr_employee_record_missing(*, user: Optional[dict]) -> str:
    """Render a recoverable onboarding state when no HR employee record exists."""
    return hr_shell(
        "Onboarding unavailable",
        "onboarding",
        """
        <section aria-labelledby="hr-onboarding-unavailable-title">
          <div class="hr-kicker">Secure onboarding</div>
          <h1 class="hr-h1" id="hr-onboarding-unavailable-title">Your employee record is not ready yet.</h1>
          <p class="hr-sub">
            HR must create your employee record before you can complete onboarding.
            No information was submitted or changed.
          </p>
          <div class="hr-callout warn" role="status">
            Ask an HR administrator to add your employee profile using the same email
            address you use to sign in.
          </div>
          <a class="hr-btn hr-btn-light" href="/admin/hr">Return to HR</a>
        </section>
        """,
        user=user,
    )


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
        "employment_saved": "Employment setup saved.",
        "profile_saved": "Personal and emergency information saved.",
        "w4_saved": "W-4 elections securely saved.",
        "attestations_saved": "Employee attestations saved.",
        "onboarding_complete": "Onboarding approved and activated.",
        "onboarding_incomplete": "Employer review saved; other onboarding steps remain.",
        "correction_requested": "Time correction sent for review.",
        "correction_approved": "Time correction approved.",
        "correction_denied": "Time correction denied.",
        "self_approval_blocked": "The requester cannot approve their own correction.",
        "timesheet_submitted": "Timesheet submitted for independent review.",
        "timesheet_already_approved": "This unchanged timesheet is already approved.",
        "timesheet_approved": "Timesheet independently approved for payroll.",
        "timesheet_rejected": "Timesheet returned to the employee for correction.",
        "timesheet_attestation_required": "Confirm that the timesheet is complete and accurate.",
        "timesheet_open_punch": "Clock out before submitting this timesheet.",
        "timesheet_correction_pending": "Resolve the pending time correction before submitting.",
        "timesheet_empty": "No closed time entries exist in this pay period.",
        "timesheet_review_invalid": "Add a review note and choose approve or reject.",
        "timesheet_review_not_found": "That submitted timesheet is no longer available for review.",
        "timesheet_changed": "Time changed after submission. The employee must submit again.",
        "pto_setup_required": "Your employment and PTO eligibility must be configured first.",
        "pto_not_eligible": "The requested date is before your PTO eligibility date.",
        "pto_insufficient": "The request exceeds your available PTO balance.",
        "pto_split_period_required": "Submit separate PTO requests on each side of the 15th/16th payroll boundary.",
        "pii_secret_missing": "Secure tax storage is not configured. Ask David or Val to finish setup.",
        "invalid_w4": "Review the SSN and W-4 selections.",
        "attestation_required": "You must complete and sign your own attestation.",
        "settings_saved": "Payroll setup saved.",
        "company_profile_saved": "Employer legal profile saved.",
        "company_profile_invalid": "Complete the employer profile and evidence note.",
        "opening_balance_saved": "Reviewed opening balance saved.",
        "opening_source_required": "Add the source used to verify the opening balance.",
        "input_added": "Payroll input added for another person's review.",
        "input_approved": "Payroll input approved.",
        "input_rejected": "Payroll input rejected.",
        "reimbursement_evidence_required": "Add the reimbursement receipt or evidence reference.",
        "duplicate_receipt": "That reimbursement evidence reference is already in payroll.",
        "recurring_input_invalid": "Only deductions and garnishments can carry forward.",
        "payroll_blocked": "Resolve every blocking item before preparing payroll.",
        "approval_attestation_required": "Type the approval statement exactly as shown.",
        "payroll_approved": "Payroll approved. No money or tax payment was sent.",
        "payroll_already_approved": "This exact payroll version was already approved.",
        "check_issued": "Manual check recorded and pay statement created.",
        "check_number_used": "That check number is already recorded.",
        "check_already_issued": "That check was already recorded; no duplicate was created.",
        "employee_check_already_issued": "This employee already has a different active check.",
        "new_check_number_required": "Use a new check number for the replacement.",
        "check_reissued": "Original check voided and replacement check recorded.",
        "void_reason_required": "Enter a void reason and replacement check number.",
        "payroll_closed": "Payroll closed after checks and liabilities reconciled.",
        "checks_not_complete": "Every employee must have one active issued check before closing.",
        "liabilities_not_reconciled": "Reconcile every tax payment and filing before closing.",
        "liability_paid": "Tax payment evidence recorded.",
        "liability_filed": "Tax filing evidence recorded.",
        "liability_reconciled": "Tax payment and filing reconciled.",
        "confirmation_required": "Enter the agency confirmation or filing ID.",
        "evidence_required": "Add a note describing the evidence you reviewed.",
        "payment_and_filing_required": "Record both payment and filing before reconciliation.",
        "liability_amount_mismatch": "The confirmed payment amount does not match the liability.",
        "correction_reason_required": "Explain what the employee needs to correct.",
        "onboarding_correction_requested": "Correction request sent without deleting prior signed records.",
        "payroll_inputs_changed": "Payroll inputs changed after preparation. Prepare a new version.",
        "negative_net_pay": "A deduction would make an employee's net pay negative.",
        "contractor_payment_prepared": "Contractor payment prepared for another person's approval.",
        "contractor_payment_approved": "Contractor payment approved; complete it in Wise and record the reference.",
        "contractor_payment_paid": "Wise payment evidence recorded.",
        "contractor_payment_reconciled": "Contractor payment reconciled.",
        "contractor_profile_saved": "Contractor tax-form and Wise readiness saved.",
        "contractor_profile_invalid": "Review the contractor status, dates, and note.",
        "wise_evidence_required": "Enter the Wise reference and evidence note.",
        "offboarding_started": "Offboarding checklist started.",
        "offboarding_saved": "Offboarding progress saved.",
        "offboarding_complete": "Offboarding completed and the employee was made inactive.",
        "policy_acknowledged": "Current policy version acknowledged.",
        "policy_already_acknowledged": "You already acknowledged this policy version.",
        "compliance_confirmed": "Compliance submission evidence recorded.",
        "compliance_reopened": "Compliance task reopened for follow-up.",
        "compliance_evidence_required": "Add a note describing the evidence reviewed.",
        "compliance_confirmation_required": "Enter the outside confirmation reference.",
        "compliance_task_not_found": "That compliance task was not found.",
        "qualified_review_saved": "Qualified payroll review evidence recorded.",
        "qualified_review_invalid": "Complete the reviewer, date, evidence, note, and attestation.",
    }
    if not flash:
        return ""
    return f'<div class="hr-flash">{_esc(msgs.get(flash, flash))}</div>'


def render_hr_dashboard(stats: dict, *, user, flash=None, manager_view=True) -> str:
    attention = "".join(
        f'<li><a href="{_esc(item["url"])}">{item["count"]} {_esc(item["label"])}</a></li>'
        for item in stats.get("attention", [])
    ) or "<li>No HR items currently need attention.</li>"
    manager_cards = f"""
    <div class="hr-cards">
      <div class="hr-card"><div class="n">{stats.get('active_employees',0)}</div><div class="l">Active employees</div></div>
      <div class="hr-card"><div class="n">{stats.get('teams',0)}</div><div class="l">Teams</div></div>
      <div class="hr-card"><div class="n">{stats.get('onboarding_incomplete',0)}</div><div class="l">Onboarding pending</div></div>
      <div class="hr-card"><div class="n">{stats.get('total_employees',0)}</div><div class="l">Total records</div></div>
    </div>
    <section class="hr-dashboard-action" aria-label="HR next action">
      <p class="hr-sub">Add an employee record, or review people, time, payroll readiness, and reporting.</p>
      <a class="hr-btn" href="/admin/hr/employees/new">+ Add employee</a>
    </section>
    <section class="hr-callout"><div class="hr-kicker">Action queue</div><ul>{attention}</ul></section>
    """
    employee_cards = f"""
    <div class="hr-cards">
      <div class="hr-card"><div class="n">{stats.get('onboarding_steps_complete',0)}/5</div><div class="l">Onboarding steps</div></div>
      <div class="hr-card"><div class="n">{stats.get('pto_available',0):.2f}</div><div class="l">PTO hours available</div></div>
      <div class="hr-card"><div class="n">{stats.get('pending_pto',0)}</div><div class="l">PTO requests pending</div></div>
      <div class="hr-card"><div class="n">{stats.get('pending_corrections',0)}</div><div class="l">Time corrections pending</div></div>
    </div>
    <section class="hr-dashboard-action" aria-label="Your next HR action">
      <p class="hr-sub">Complete your onboarding, clock your day, or review your own time and PTO.</p>
      <a class="hr-btn" href="/admin/hr/onboarding">Continue onboarding</a>
      <a class="hr-btn hr-btn-light" href="/admin/hr/time">Open time &amp; PTO</a>
    </section>
    """
    body = f"""
    {_flash(flash)}
    <h1 class="hr-h1">HR Dashboard.</h1>
    <p class="hr-sub">People, time, and payroll for Anata — all in one place.</p>
    {manager_cards if manager_view else employee_cards}
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
        employment = e.get("employment") or {}
        pay = (
            f"${employment.get('fixed_pay_per_period', '0.00')}/check"
            if employment.get("pay_basis") == "fixed_semimonthly"
            else f"${e['hourly_rate']}/hr"
        )
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
    employment = e.get("employment") or {}

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
    <p class="hr-sub">Employer-owned identity, employment, classification, and compensation setup.</p>
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
        <div><label>Fixed semimonthly pay ($)</label><input name="fixed_pay_per_period" value="{_esc(employment.get('fixed_pay_per_period','0.00'))}" placeholder="1000.00"></div>
      </div>
      <div class="hr-grid2">
        <div><label>Hire date</label><input type="date" name="hire_date" value="{_esc(employment.get('hire_date',''))}"></div>
        <div><label>Job title</label><input name="title" value="{_esc(employment.get('title',''))}"></div>
      </div>
      <div class="hr-grid2">
        <div><label>Manager email</label><input type="email" name="manager_email" value="{_esc(employment.get('manager_email',''))}"></div>
        <div><label>Overtime classification</label>{_sel("classification", ("nonexempt","exempt"), employment.get("classification","nonexempt"))}</div>
      </div>
      <div class="hr-grid2">
        <div><label>Pay basis</label>{_sel("pay_basis", ("hourly","fixed_semimonthly"), employment.get("pay_basis","hourly"))}</div>
        <div><label>Standard weekly hours</label><input type="number" min="0" step="0.01" name="standard_weekly_hours" value="{_esc(employment.get('standard_weekly_hours','40'))}"></div>
      </div>
      <label>Phone</label><input name="phone" value="{_esc(e.get('phone',''))}" placeholder="(555) 123-4567">
      <div class="hr-actions">
        <button type="submit" class="hr-btn">{"Add employee" if is_new else "Save changes"}</button>
        <a class="hr-btn hr-btn-light" href="/admin/hr/employees">Cancel</a>
      </div>
    </form>
    {"" if is_new else f'''
    <form class="hr-form" method="post" action="/admin/hr/employees/{e["id"]}/invite" style="margin-top:18px">
      <div class="hr-kicker">Secure onboarding</div>
      <p>Creates an expiring employee-only invitation. The employee completes personal, W-4, I-9 employee, and policy steps after signing in.</p>
      <button class="hr-btn" type="submit">Create secure invitation</button>
    </form>
    <form class="hr-form" method="post" action="/admin/hr/employees/{e["id"]}/onboarding-review" style="margin-top:18px">
      <div class="hr-kicker">Employer I-9 review</div>
      <p>Review acceptable documents directly. Do not ask the employee to email identity documents.</p>
      <label>Document type/category</label><input name="i9_document_type" required placeholder="List A, or List B + List C">
      <div class="hr-grid2"><div><label>Verified date</label><input type="date" name="i9_verified_date" required></div>
      <div><label>Expiration date, if applicable</label><input type="date" name="i9_expiration_date"></div></div>
      <button class="hr-btn" type="submit">Record employer verification</button>
    </form>
    <form class="hr-form" method="post" action="/admin/hr/employees/{e["id"]}/onboarding-correction" style="margin-top:18px">
      <div class="hr-kicker">Request a correction</div>
      <p>Returns the secure forms to the employee. Their earlier signed records stay in the audit history.</p>
      <label>What needs correction</label><textarea name="reason" required></textarea>
      <button class="hr-btn hr-btn-light" type="submit">Send correction request</button>
    </form>'''}
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


def render_hr_time(
    entries: list, pto: dict, pto_requests: list, current: Optional[dict],
    corrections: list, review_flags: list, timesheets: list, period,
    *, user, flash=None
) -> str:
    punch_action = "out" if current else "in"
    punch_label = "Clock out" if current else "Clock in"
    can_review = bool((user or {}).get("is_superadmin") or "hr.payroll" in ((user or {}).get("permissions") or set()))
    rows = "".join(f"""<tr><td>{_esc(r['date'])}</td><td>{_esc(r['start_time'] or '—')}</td>
      <td>{_esc(r['stop_time'] or 'Open')}</td><td>{r['hours']:.2f}</td><td>{_esc(r['employee_email'])}</td>
      <td>{f'<details><summary>Correct</summary><form method="post" action="/admin/hr/time/{r["id"]}/correction"><label>Correct start</label><input type="time" name="proposed_start" value="{_esc(r["start_time"])}" required><label>Correct end</label><input type="time" name="proposed_stop" value="{_esc(r["stop_time"])}" required><label>Reason</label><input name="reason" required maxlength="500"><button class="hr-btn" type="submit">Request correction</button></form></details>' if not r['is_open'] else 'Close the shift first'}</td></tr>""" for r in entries)
    if not rows:
        rows = '<tr><td colspan="6" class="hr-empty">No time recorded yet.</td></tr>'
    requests = "".join(f"""<tr><td>{_esc(r['employee_email'])}</td><td>{_esc(r['start_date'])}–{_esc(r['end_date'])}</td>
      <td>{r['hours']:.2f}</td><td>{_esc(r['status'])}</td><td>{_esc(r['reason'] or '—')}</td><td>
      {f'<form class="hr-inline" method="post" action="/admin/hr/time/pto/{r["id"]}/decision"><button class="hr-btn" name="decision" value="approved">Approve</button><button class="hr-btn hr-btn-light" name="decision" value="denied">Deny</button></form>' if can_review and r['status'] == 'pending' else '—'}</td></tr>""" for r in pto_requests)
    if not requests:
        requests = '<tr><td colspan="6" class="hr-empty">No PTO requests yet.</td></tr>'
    correction_rows = "".join(f"""<tr><td>{_esc(c['employee_email'])}</td><td>{_esc(c['original'].get('start_time'))}–{_esc(c['original'].get('stop_time'))}</td>
      <td>{_esc(c['proposed'].get('start_time'))}–{_esc(c['proposed'].get('stop_time'))}</td><td>{_esc(c['reason'])}</td><td>{_esc(c['status'])}</td>
      <td>{f'<form class="hr-inline" method="post" action="/admin/hr/time/corrections/{c["id"]}/decision"><input name="reviewer_reason" placeholder="Review note"><button class="hr-btn" name="decision" value="approved">Approve</button><button class="hr-btn hr-btn-light" name="decision" value="denied">Deny</button></form>' if can_review and c['status'] == 'requested' else '—'}</td></tr>""" for c in corrections)
    if not correction_rows:
        correction_rows = '<tr><td colspan="6" class="hr-empty">No time corrections.</td></tr>'
    timesheet_rows = "".join(
        f"""<tr><td>{_esc(item['employee_email'])}</td>
        <td>{_esc(item['period_start'])}–{_esc(item['period_end'])}</td>
        <td>{_esc(item['status'])}</td><td>{_esc(item.get('reviewed_by') or '—')}</td>
        <td>{f'<form class="hr-form" method="post" action="/admin/hr/time/timesheets/{item["id"]}/decision"><input type="hidden" name="period_start" value="{_esc(period.start_date)}"><label>Review note</label><input name="review_note" required maxlength="500"><div class="hr-inline"><button class="hr-btn" name="decision" value="approved">Approve</button><button class="hr-btn hr-btn-light" name="decision" value="rejected">Return for correction</button></div></form>' if can_review and item["status"] == "submitted" else _esc(item.get("review_note") or "—")}</td></tr>"""
        for item in timesheets
    ) or '<tr><td colspan="5" class="hr-empty">No timesheets submitted for this period.</td></tr>'
    flags_html = (
        '<div class="hr-callout warn"><div class="hr-kicker">Time review</div><ul>'
        + "".join(
            f"<li>{_esc(flag.get('employee_email'))}: {_esc(flag.get('message'))}</li>"
            for flag in review_flags
        ) + "</ul></div>"
    ) if review_flags else ""
    body = f"""
    {_flash(flash)}
    <h1 class="hr-h1">Time & PTO</h1>
    <p class="hr-sub">Simple daily punches. Paid breaks stay inside the workday; no location is collected.</p>
    <form class="hr-inline" method="get" action="/admin/hr/time">
      <label for="time-period">Choose a date inside the pay period</label>
      <input id="time-period" type="date" name="period_date" value="{_esc(period.start_date)}">
      <button class="hr-btn hr-btn-light" type="submit">Open period</button>
    </form>
    <div class="hr-callout"><div class="hr-kicker">Your time clock</div>
      <p>{'Clocked in. Your time is running.' if current else 'You are currently clocked out.'}</p>
      <form method="post" action="/admin/hr/time/clock"><input type="hidden" name="action" value="{punch_action}">
        <button class="hr-btn" type="submit">{punch_label}</button></form></div>
    {flags_html}
    <div class="hr-cards">
      <div class="hr-card"><div class="n">{pto.get('available',0):.2f}</div><div class="l">PTO hours available</div></div>
      <div class="hr-card"><div class="n">{pto.get('accrued',0):.2f}</div><div class="l">PTO hours accrued</div></div>
      <div class="hr-card"><div class="n">{pto.get('used',0):.2f}</div><div class="l">PTO hours used</div></div>
    </div>
    <p class="hr-sub">{'PTO is available for requests.' if pto.get('eligible') else f'PTO becomes usable on {_esc(pto.get("eligible_date") or "the configured eligibility date")}.'}</p>
    <h2>Recent punches</h2><table class="hr-tbl"><thead><tr><th>Date</th><th>In</th><th>Out</th><th>Hours</th><th>Employee</th><th>Correction</th></tr></thead><tbody>{rows}</tbody></table>
    <h2 style="margin-top:28px">Time corrections</h2><table class="hr-tbl"><thead><tr><th>Employee</th><th>Original</th><th>Requested</th><th>Reason</th><th>Status</th><th>Decision</th></tr></thead><tbody>{correction_rows}</tbody></table>
    <h2 style="margin-top:28px">Timesheet approval</h2>
    <p class="hr-sub">Period {_esc(period.start_date)}–{_esc(period.end_date)}. Hourly payroll remains blocked until the employee submits and another authorized person approves an unchanged timesheet.</p>
    <form class="hr-form" method="post" action="/admin/hr/time/timesheets/submit">
      <input type="hidden" name="period_start" value="{_esc(period.start_date)}">
      <input type="hidden" name="period_end" value="{_esc(period.end_date)}">
      <label><input type="checkbox" name="attested" value="true" required style="width:auto"> I confirm my time for this period is complete and accurate.</label>
      <div class="hr-actions"><button class="hr-btn" type="submit">Submit my timesheet</button></div>
    </form>
    <table class="hr-tbl" style="margin-top:18px"><thead><tr><th>Employee</th><th>Period</th><th>Status</th><th>Reviewer</th><th>Decision / note</th></tr></thead><tbody>{timesheet_rows}</tbody></table>
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


def render_hr_invitation(invite_link: str, employee: dict, *, user, email_sent: bool) -> str:
    body = f"""
    <h1 class="hr-h1">Secure invitation created</h1>
    <p class="hr-sub">This link expires and can be used only by {_esc(employee.get('email'))}.</p>
    <div class="hr-flash">{'Invitation email sent.' if email_sent else 'Email delivery is not configured or failed. Copy and send the secure link below.'}</div>
    <div class="hr-callout warn"><div class="hr-kicker">Copy once</div>
      <p>Send this link through a trusted channel. It contains no employee data, but anyone holding it can begin the sign-in flow for the invited email.</p>
      <input readonly value="{_esc(invite_link)}" aria-label="Secure invitation link" onclick="this.select()">
    </div>
    <a class="hr-btn" href="/admin/hr/employees/{employee.get('id')}">Return to employee</a>
    """
    return hr_shell("Invitation", "employees", body, user=user)


def render_hr_onboarding(
    employee: dict, onboarding: dict, *, tax_election: Optional[dict] = None,
    user, flash=None
) -> str:
    tax_election = tax_election or {}
    filing_status = tax_election.get("filing_status", "")
    selected = lambda value: " selected" if filing_status == value else ""
    checked = lambda value: " checked" if value else ""
    ssn_note = (
        f'Your current signed W-4 uses an SSN ending in '
        f'<strong>{_esc(tax_election.get("ssn_last4"))}</strong>. '
        "For security, the full number is never shown or prefilled. "
        "Re-enter it to sign a replacement."
        if tax_election.get("ssn_last4")
        else "Enter your Social Security number. It will be encrypted and will not "
             "be shown again."
    )
    address = ", ".join(filter(None, [
        employee.get("address_line1"), employee.get("address_line2"),
        employee.get("city"),
        " ".join(filter(None, [employee.get("state"), employee.get("zip_code")])),
    ]))
    correction = (
        f'''<div class="hr-callout warn"><div class="hr-kicker">Correction requested</div>
        <p>{_esc(onboarding.get("correction_reason"))}</p>
        <p>Update the relevant form below. Your previous signed submission remains in the audit history.</p></div>'''
        if onboarding.get("status") == "correction_requested" else ""
    )
    body = f"""
    {_flash(flash)}
    <h1 class="hr-h1">Your Anata onboarding</h1>
    <p class="hr-sub">Complete your information securely. David or Val completes the employer verification step separately.</p>
    {correction}
    <div class="hr-cards">
      <div class="hr-card"><div class="n">{'Done' if onboarding.get('profile_complete') else 'Needed'}</div><div class="l">Personal profile</div></div>
      <div class="hr-card"><div class="n">{'Done' if onboarding.get('w4_complete') else 'Needed'}</div><div class="l">Federal W-4</div></div>
      <div class="hr-card"><div class="n">{'Done' if onboarding.get('i9_employee_complete') else 'Needed'}</div><div class="l">I-9 employee step</div></div>
      <div class="hr-card"><div class="n">{'Done' if onboarding.get('policies_complete') else 'Needed'}</div><div class="l">Policies</div></div>
    </div>
    <form class="hr-form" method="post" action="/admin/hr/onboarding/profile">
      <div class="hr-kicker">Personal and emergency information</div>
      <label>Phone</label><input name="phone" value="{_esc(employee.get('phone'))}">
      <label>Address</label><input name="address_line1" value="{_esc(employee.get('address_line1'))}" required>
      <label>Address line 2</label><input name="address_line2" value="{_esc(employee.get('address_line2'))}">
      <div class="hr-grid2"><div><label>City</label><input name="city" value="{_esc(employee.get('city'))}" required></div><div><label>State</label><input name="state" value="{_esc(employee.get('state') or 'UT')}" required></div></div>
      <label>ZIP</label><input name="zip_code" value="{_esc(employee.get('zip_code'))}" required>
      <label>Emergency contact name</label><input name="emergency_name" value="{_esc(onboarding.get('emergency_contact_name'))}" required>
      <label>Relationship</label><input name="emergency_relationship" value="{_esc(onboarding.get('emergency_contact_relationship'))}" required>
      <label>Emergency phone</label><input name="emergency_phone" value="{_esc(onboarding.get('emergency_contact_phone'))}" required>
      <label>Emergency email (optional)</label><input type="email" name="emergency_email" value="{_esc(onboarding.get('emergency_contact_email'))}">
      <div class="hr-actions"><button class="hr-btn" type="submit">Save personal information</button></div>
    </form>
    <form class="hr-form" method="post" action="/admin/hr/onboarding/w4" style="margin-top:18px">
      <div class="hr-kicker">Federal W-4</div>
      <p>We prefill facts already in your profile and, for a correction, your current elections. Agent never chooses a tax election for you.</p>
      <div class="hr-callout"><div class="hr-kicker">Prefilled employee information</div>
        <p><strong>{_esc(employee.get("full_name"))}</strong><br>{_esc(address)}</p>
        <p class="hr-sub">Update your personal profile above if this is not correct.</p>
      </div>
      <label>Social Security number</label>
      <p class="hr-sub" id="w4-ssn-help">{ssn_note}</p>
      <input type="password" inputmode="numeric" autocomplete="new-password" name="ssn" minlength="9" maxlength="11" required aria-describedby="w4-ssn-help">
      <label>Filing status</label><select name="filing_status" required>
        <option value="">Choose your filing status</option>
        <option value="single"{selected("single")}>Single or married filing separately</option>
        <option value="married_joint"{selected("married_joint")}>Married filing jointly</option>
        <option value="head_household"{selected("head_household")}>Head of household</option>
      </select>
      <label><input type="checkbox" name="two_jobs" value="true" style="width:auto"{checked(tax_election.get("two_jobs"))}> Multiple jobs or spouse works</label>
      <div class="hr-grid2"><div><label>Dependent credit ($)</label><input name="dependents_credit" inputmode="decimal" value="{_esc(tax_election.get('dependents_credit', '0.00'))}"></div><div><label>Other income ($)</label><input name="other_income" inputmode="decimal" value="{_esc(tax_election.get('other_income', '0.00'))}"></div></div>
      <div class="hr-grid2"><div><label>Deductions ($)</label><input name="deductions" inputmode="decimal" value="{_esc(tax_election.get('deductions', '0.00'))}"></div><div><label>Extra withholding per check ($)</label><input name="extra_withholding" inputmode="decimal" value="{_esc(tax_election.get('extra_withholding', '0.00'))}"></div></div>
      <label><input type="checkbox" name="exempt" value="true" style="width:auto"{checked(tax_election.get("exempt_from_federal_withholding"))}> Exempt from federal withholding (choose only if you meet both IRS conditions shown on the official 2026 Form W-4)</label>
      <p><a href="https://www.irs.gov/pub/irs-pdf/fw4.pdf" target="_blank" rel="noopener">Review the official Form W-4 instructions and worksheets</a>. Agent does not choose an election for you.</p>
      <label><input type="checkbox" name="attested" value="true" required style="width:auto"> Under penalties of perjury, I declare that this certificate, to the best of my knowledge and belief, is true, correct, and complete.</label>
      <div class="hr-actions"><button class="hr-btn" type="submit">Sign and save W-4</button></div>
    </form>
    <form class="hr-form" method="post" action="/admin/hr/onboarding/attestations" style="margin-top:18px">
      <div class="hr-kicker">Employee attestations</div>
      <p><a href="https://www.uscis.gov/i-9" target="_blank" rel="noopener">Use the official USCIS Form I-9 and instructions</a>. Complete Section 1 no later than your first day of work. Agent does not choose your citizenship/immigration status or retain identity-document images.</p>
      <label><input type="checkbox" name="i9_attested" value="true" required style="width:auto"> I completed Section 1 of the official Form I-9 and will present acceptable documents directly to David or Val; I will not email identity documents.</label>
      <label><input type="checkbox" name="policies_attested" value="true" required style="width:auto"> I received and acknowledge the timekeeping, overtime, PTO, holiday, payroll, and privacy policies.</label>
      <div class="hr-actions"><button class="hr-btn" type="submit">Save attestations</button></div>
    </form>
    """
    return hr_shell("Onboarding", "employees", body, user=user)


def render_hr_payroll_control(control: dict, *, user, flash=None) -> str:
    period = control["period"]
    readiness = control["readiness"]
    blockers = "".join(
        f"<li><strong>{_esc(item.get('employee_email') or 'Company setup')}:</strong> "
        f"{_esc(item.get('message'))}</li>" for item in readiness["blockers"]
    ) or "<li>No blocking issues. Payroll can be prepared for approval.</li>"
    employee_options = "".join(
        f'<option value="{_esc(employee["email"])}">{_esc(employee["full_name"])}'
        f' — {_esc(employee["email"])}</option>' for employee in control["employees"]
    )
    input_rows = "".join(
        f"""<tr><td>{_esc(item['employee_email'])}</td><td>{_esc(item['input_type'])}</td>
        <td>${_esc(item['amount'])}</td><td>{'Taxable' if item['taxable'] else 'Non-taxable'}</td>
        <td>{_esc(item['status'])}{' · Review unusual change' if item.get('unusual_change') else ''}</td>
        <td>{_esc(item['description'] or '—')}<br><span class="hr-sub">{_esc(item.get('source_reference') or 'No separate evidence reference')}{' · recurring' if item.get('recurring') else ''}</span></td>
        <td>{f'<form class="hr-inline" method="post" action="/admin/hr/payroll/inputs/{item["id"]}/decision"><input type="hidden" name="period_date" value="{period.start_date}"><button class="hr-btn" name="decision" value="approved">Approve</button><button class="hr-btn hr-btn-light" name="decision" value="rejected">Reject</button></form>' if item["status"] == "pending" else '—'}</td></tr>"""
        for item in control["inputs"]
    ) or '<tr><td colspan="7" class="hr-empty">No bonus, commission, reimbursement, deduction, or fee inputs.</td></tr>'
    run_rows = "".join(
        f"""<tr><td>{_esc(run['id'])}</td><td>{_esc(run['status'])}</td>
        <td>${_esc(run['gross'])}{f'<br><span class="hr-sub">{run["gross_change_percent"]:+.1f}% vs prior</span>' if run.get("gross_change_percent") is not None else ''}</td>
        <td>${_esc(run['taxes'])}</td><td>${_esc(run.get('deductions'))}</td><td>${_esc(run['net'])}</td><td>${_esc(run['cash_impact'])}</td>
        <td>{run['employee_count']}</td><td>{_esc(run['initiated_by'])}</td>
        <td><a href="/admin/hr/payroll/runs/{_esc(run['id'])}">Review details</a>
        {f'<form method="post" action="/admin/hr/payroll/{run["id"]}/approve"><label>Type exactly: I approve this payroll</label><input name="approval_text" required autocomplete="off"><input type="hidden" name="period_date" value="{period.start_date}"><button class="hr-btn" type="submit">Approve prepared payroll</button></form>' if run["status"] == "prepared" else ''}</td></tr>"""
        for run in control["runs"]
    ) or '<tr><td colspan="10" class="hr-empty">No prepared versions for this period.</td></tr>'
    liability_rows = "".join(
        f"""<tr><td>{_esc(item['agency'])}</td><td>{_esc(item['liability_type'])}</td>
        <td>${_esc(item['amount'])}</td><td>{_esc(item['due_date'])}</td><td>{_esc(item['status'])}</td>
        <td><form class="hr-form" method="post" action="/admin/hr/payroll/liabilities/{item["id"]}">
        <input type="hidden" name="period_date" value="{period.start_date}">
        <label>Payment confirmation ID</label><input name="confirmation_number" value="{_esc(item['confirmation_number'])}">
        <label>Filing confirmation ID</label><input name="filing_confirmation_number" value="{_esc(item.get('filing_confirmation_number'))}">
        <label>Confirmed payment amount</label><input name="confirmed_amount" value="{_esc(item['amount'])}">
        <label>Evidence note</label><input name="evidence_note" value="{_esc(item['evidence_note'])}" required>
        <div class="hr-inline"><button class="hr-btn" name="action" value="paid">Record paid</button>
        <button class="hr-btn hr-btn-light" name="action" value="filed">Record filed</button>
        <button class="hr-btn hr-btn-light" name="action" value="reconciled">Reconcile</button></div>
        </form></td></tr>""" for item in control["liabilities"]
    ) or '<tr><td colspan="6" class="hr-empty">Liabilities appear only after a different person approves the prepared payroll.</td></tr>'
    status_label = "Ready to prepare" if readiness["ready"] else "Blocked"
    body = f"""
    {_flash(flash)}
    <h1 class="hr-h1">Payroll control room</h1>
    <p class="hr-sub">Prepare {_esc(period.start_date)}–{_esc(period.end_date)} for payment on {_esc(period.pay_date)}.</p>
    <form class="hr-inline" method="get" action="/admin/hr/payroll">
      <label for="period-date">Choose a date inside the pay period</label>
      <input id="period-date" type="date" name="period_date" value="{_esc(period.start_date)}">
      <button class="hr-btn hr-btn-light" type="submit">Open period</button>
    </form>
    <div class="hr-callout {'ok' if readiness['ready'] else 'warn'}"><div class="hr-kicker">Payroll readiness</div>
      <h2 style="margin:6px 0">{status_label}</h2><ul>{blockers}</ul>
      <p>No bank transfer, check, tax payment, or filing occurs from preparation.</p></div>
    <div class="hr-cards">
      <div class="hr-card"><div class="n">{_esc(period.pay_date)}</div><div class="l">Payday</div></div>
      <div class="hr-card"><div class="n">{len(control['employees'])}</div><div class="l">W-2 employees</div></div>
      <div class="hr-card"><div class="n">{status_label}</div><div class="l">Preparation readiness</div></div>
    </div>
    <h2>Other payroll inputs</h2>
    <table class="hr-tbl"><thead><tr><th>Employee</th><th>Type</th><th>Amount</th><th>Tax</th><th>Status</th><th>Description</th><th>Review</th></tr></thead><tbody>{input_rows}</tbody></table>
    <form class="hr-form" method="post" action="/admin/hr/payroll/inputs">
      <input type="hidden" name="period_date" value="{period.start_date}">
      <div class="hr-grid2"><div><label>Employee</label><select name="employee_email" required>{employee_options}</select></div>
      <div><label>Type</label><select name="input_type"><option value="bonus">Bonus</option><option value="commission">Commission</option><option value="reimbursement">Accountable reimbursement</option><option value="deduction">Voluntary/standard deduction</option><option value="garnishment">Mandatory garnishment</option><option value="holiday_adjustment">Holiday adjustment</option><option value="manual_correction">Manual pay correction</option></select></div></div>
      <div class="hr-grid2"><div><label>Amount</label><input name="amount" inputmode="decimal" required></div>
      <div><label><input type="checkbox" name="taxable" value="true" checked style="width:auto"> Taxable</label></div></div>
      <label>Business reason</label><input name="description" required maxlength="255">
      <label>Receipt, order, or evidence reference</label><input name="source_reference" maxlength="255" placeholder="Required for reimbursements">
      <label><input type="checkbox" name="recurring" value="true" style="width:auto"> Carry forward for review each period (deductions and garnishments only)</label>
      <button class="hr-btn" type="submit">Add for review</button>
    </form>
    <form method="post" action="/admin/hr/payroll/prepare" style="margin:24px 0">
      <input type="hidden" name="period_date" value="{period.start_date}">
      <button class="hr-btn" type="submit"{'' if readiness['ready'] else ' disabled'}>Prepare immutable payroll version</button>
    </form>
    <h2>Prepared and approved versions</h2>
    <table class="hr-tbl"><thead><tr><th>Version ID</th><th>Status</th><th>Gross</th><th>Tax liability</th><th>Deduction liability</th><th>Employee check cash</th><th>Total employer cost</th><th>People</th><th>Prepared by</th><th>Approval</th></tr></thead><tbody>{run_rows}</tbody></table>
    <h2>Tax payment and filing reconciliation</h2>
    <p class="hr-sub">Amounts are liabilities until confirmation evidence is recorded. Paid and filed are separate facts.</p>
    <table class="hr-tbl"><thead><tr><th>Agency</th><th>Type</th><th>Amount</th><th>Due</th><th>Status</th><th>Evidence action</th></tr></thead><tbody>{liability_rows}</tbody></table>
    <div class="hr-callout"><div class="hr-kicker">Approved operating rules</div>
      <ul><li>Semimonthly: 1st–15th paid the 20th; 16th–month end paid the following 5th.</li>
      <li>Saturday paydays move to Friday; Sunday paydays move to Monday.</li>
      <li>Sunday–Saturday overtime week; overtime requires advance approval but worked overtime remains payable.</li>
      <li>Printed/manual checks at launch. Each check number, void, and reissue is recorded.</li></ul></div>
    """
    return hr_shell("Payroll", "payroll", body, user=user)


def render_hr_payroll_run(run: dict, *, user, employee_view=False, flash=None) -> str:
    def money(cents):
        return f"{int(cents or 0) / 100:,.2f}"
    rows = ""
    for calculation in run["calculations"]:
        inputs, results = calculation["inputs"], calculation["results"]
        check_action = "—"
        if not employee_view and run["status"] in {"approved", "checks_issued"}:
            check_action = (
                f'''<div>Check {_esc(calculation['check_number'])}</div>
                <details><summary>Void and reissue</summary>
                <form class="hr-form" method="post" action="/admin/hr/payroll/runs/{_esc(run["id"])}/checks/reissue">
                <input type="hidden" name="employee_email" value="{_esc(calculation["employee_email"])}">
                <label>Reason</label><input name="reason" required>
                <label>New check number</label><input name="new_check_number" required>
                <button class="hr-btn hr-btn-danger" type="submit">Void and record replacement</button></form></details>'''
                if calculation["check_number"] else
                f'''<form class="hr-inline" method="post" action="/admin/hr/payroll/runs/{_esc(run["id"])}/checks">
                <input type="hidden" name="employee_email" value="{_esc(calculation["employee_email"])}">
                <label>Check number</label><input name="check_number" required>
                <button class="hr-btn" type="submit">Record issued check</button></form>'''
            )
        rows += f"""<tr><td>{_esc(calculation['employee_email'])}</td>
        <td>{_esc(inputs.get('regular_hours','0'))}</td><td>{_esc(inputs.get('overtime_hours','0'))}</td>
        <td>{_esc(inputs.get('holiday_hours','0'))}</td><td>{_esc(inputs.get('pto_hours','0'))}</td>
        <td>${money(results.get('taxable_gross_cents'))}</td>
        <td>${money(results.get('federal_cents'))}</td><td>${money(results.get('utah_cents'))}</td>
        <td>${money(results.get('social_security_cents'))}</td><td>${money(results.get('medicare_cents'))}</td>
        <td>${money(results.get('deductions_cents'))}</td><td>${money(results.get('reimbursements_cents'))}</td>
        <td>${money(results.get('net_cents'))}</td>
        <td>${money(results.get('employer_taxes_cents'))}</td>
        <td>${money(results.get('total_employer_cost_cents'))}</td><td>{check_action}</td></tr>"""
    body = f"""
    {_flash(flash)}
    <h1 class="hr-h1">{'Pay statement' if employee_view else 'Payroll version review'}</h1>
    <p class="hr-sub">{_esc(run['period_start'])}–{_esc(run['period_end'])}, payday {_esc(run['pay_date'])}. Status: {_esc(run['status'])}.</p>
    <div class="hr-cards"><div class="hr-card"><div class="n">${_esc(run['gross'])}</div><div class="l">Gross wages</div></div>
    <div class="hr-card"><div class="n">${_esc(run['taxes'])}</div><div class="l">Tax liability</div></div>
    <div class="hr-card"><div class="n">${_esc(run['deductions'])}</div><div class="l">Deduction liability</div></div>
    <div class="hr-card"><div class="n">${_esc(run['net'])}</div><div class="l">Employee check cash</div></div>
    <div class="hr-card"><div class="n">${_esc(run['cash_impact'])}</div><div class="l">Total employer cost</div></div></div>
    <div style="overflow-x:auto"><table class="hr-tbl"><thead><tr><th>Employee</th><th>Regular</th><th>OT</th><th>Holiday</th><th>PTO</th><th>Gross</th><th>Federal</th><th>Utah</th><th>Social Security</th><th>Medicare</th><th>Other deductions</th><th>Reimbursements</th><th>Net</th><th>Employer taxes</th><th>Employer cost</th><th>Payment record</th></tr></thead><tbody>{rows}</tbody></table></div>
    <p class="hr-sub">Calculation version: immutable snapshot. A recorded check is not marked cleared until separate reconciliation evidence exists.</p>
    {f'<form method="post" action="/admin/hr/payroll/runs/{_esc(run["id"])}/close"><button class="hr-btn" type="submit">Close fully reconciled payroll</button></form>' if not employee_view and run["status"] == "checks_issued" else ''}
    <a class="hr-btn hr-btn-light" href="{'/admin/hr/payroll' if not employee_view else '/admin/hr/pay-statements'}">Back</a>
    """
    return hr_shell(
        "Pay statement" if employee_view else "Payroll review",
        "pay_statements" if employee_view else "payroll", body, user=user,
    )


def render_hr_pay_statements(runs: list, *, user) -> str:
    rows = "".join(
        f"""<tr><td>{_esc(run['pay_date'])}</td><td>{_esc(run['period_start'])}–{_esc(run['period_end'])}</td>
        <td>{_esc(run['status'])}</td><td>${_esc(run['gross'])}</td><td>${_esc(run['net'])}</td>
        <td><a href="/admin/hr/pay-statements/{_esc(run['id'])}">View statement</a></td></tr>"""
        for run in runs
    ) or '<tr><td colspan="6" class="hr-empty">No issued pay statements yet.</td></tr>'
    body = f"""<h1 class="hr-h1">Your pay statements</h1>
    <p class="hr-sub">Only your own issued payroll records appear here.</p>
    <table class="hr-tbl"><thead><tr><th>Payday</th><th>Period</th><th>Status</th><th>Gross</th><th>Net</th><th>Statement</th></tr></thead><tbody>{rows}</tbody></table>"""
    return hr_shell("Pay statements", "pay_statements", body, user=user)


def render_hr_contractors(contractors: list, profiles: list, payments: list,
                          *, user, flash=None) -> str:
    options = "".join(
        f'<option value="{_esc(row["email"])}">{_esc(row["full_name"])} — {_esc(row["email"])}</option>'
        for row in contractors
    )
    rows = ""
    for item in payments:
        action = "—"
        if item["status"] == "draft":
            action = f'<form method="post" action="/admin/hr/contractors/payments/{item["id"]}"><button class="hr-btn" name="action" value="approve">Approve</button></form>'
        elif item["status"] == "approved":
            action = f'''<form class="hr-form" method="post" action="/admin/hr/contractors/payments/{item["id"]}">
            <label>Wise transfer reference</label><input name="wise_reference" required>
            <label>Evidence note</label><input name="evidence_note" required>
            <button class="hr-btn" name="action" value="record_paid">Record Wise payment</button></form>'''
        elif item["status"] == "paid":
            action = f'''<form method="post" action="/admin/hr/contractors/payments/{item["id"]}">
            <input name="evidence_note" value="{_esc(item['evidence_note'])}" required>
            <button class="hr-btn" name="action" value="reconcile">Reconcile</button></form>'''
        rows += f"""<tr><td>{_esc(item['contractor_email'])}</td><td>{_esc(item['service_start'])}–{_esc(item['service_end'])}</td>
        <td>{_esc(item['currency'])} {_esc(item['amount'])}</td><td>{_esc(item['due_date'])}</td>
        <td>{_esc(item['status'])}</td><td>{_esc(item['wise_transfer_reference'] or '—')}</td><td>{action}</td></tr>"""
    rows = rows or '<tr><td colspan="7" class="hr-empty">No contractor payment obligations yet.</td></tr>'
    profile_by_email = {row["contractor_email"]: row for row in profiles}
    profile_rows = "".join(
        f"""<tr><td>{_esc(contractor['full_name'])}<br><span class="hr-sub">{_esc(contractor['email'])}</span></td>
        <td>{_esc(profile_by_email.get(contractor['email'], {}).get('tax_form_type', 'undetermined'))}</td>
        <td>{_esc(profile_by_email.get(contractor['email'], {}).get('tax_form_status', 'missing'))}</td>
        <td>{_esc(profile_by_email.get(contractor['email'], {}).get('expiration_date') or '—')}</td>
        <td>{_esc(profile_by_email.get(contractor['email'], {}).get('wise_recipient_reference') or '—')}</td></tr>"""
        for contractor in contractors
    ) or '<tr><td colspan="5" class="hr-empty">Add a contractor employee record first.</td></tr>'
    body = f"""{_flash(flash)}<h1 class="hr-h1">Contractors & Wise</h1>
    <p class="hr-sub">Contractor obligations stay separate from W-2 payroll. Agent records approvals and Wise evidence; it does not initiate a transfer.</p>
    <h2>Contractor readiness</h2>
    <p class="hr-sub">Choose the tax-form type only after a qualified review. Agent tracks status but does not select or prepare a country-specific tax form.</p>
    <table class="hr-tbl"><thead><tr><th>Contractor</th><th>Tax form</th><th>Status</th><th>Expiry</th><th>Wise recipient</th></tr></thead><tbody>{profile_rows}</tbody></table>
    <form class="hr-form" method="post" action="/admin/hr/contractors/profile">
      <div class="hr-kicker">Review contractor profile</div>
      <label>Contractor</label><select name="contractor_email" required>{options}</select>
      <label>Tax form type (human selected)</label><select name="tax_form_type"><option value="undetermined">Not determined</option><option value="w9">W-9</option><option value="w8ben">W-8BEN</option><option value="w8bene">W-8BEN-E</option><option value="other">Other</option></select>
      <label>Tax form status</label><select name="tax_form_status"><option value="missing">Missing</option><option value="requested">Requested</option><option value="received">Received</option><option value="reviewed">Reviewed</option><option value="expired">Expired</option></select>
      <div class="hr-grid2"><div><label>Received date</label><input type="date" name="received_date"></div><div><label>Expiration date, if applicable</label><input type="date" name="expiration_date"></div></div>
      <label>Wise recipient reference</label><input name="wise_recipient_reference">
      <label>Review note</label><textarea name="review_note" required></textarea>
      <button class="hr-btn" type="submit">Save contractor readiness</button>
    </form>
    <h2>Payment obligations</h2>
    <table class="hr-tbl"><thead><tr><th>Contractor</th><th>Service period</th><th>Amount</th><th>Due</th><th>Status</th><th>Wise reference</th><th>Action</th></tr></thead><tbody>{rows}</tbody></table>
    <form class="hr-form" method="post" action="/admin/hr/contractors/payments">
      <div class="hr-kicker">Prepare contractor payment</div>
      <label>Contractor</label><select name="contractor_email" required>{options}</select>
      <div class="hr-grid2"><div><label>Service start</label><input type="date" name="service_start" required></div><div><label>Service end</label><input type="date" name="service_end" required></div></div>
      <div class="hr-grid2"><div><label>Due date</label><input type="date" name="due_date" required></div><div><label>Amount</label><input name="amount" required></div></div>
      <label>Currency</label><input name="currency" value="USD" minlength="3" maxlength="3" required>
      <label>Description</label><input name="description" required>
      <label>Invoice/reference</label><input name="invoice_reference">
      <button class="hr-btn" type="submit">Prepare for approval</button>
    </form>"""
    return hr_shell("Contractors", "contractors", body, user=user)


def render_hr_offboarding(employees: list, checklists: list, *, user, flash=None) -> str:
    options = "".join(
        f'<option value="{_esc(row["email"])}">{_esc(row["full_name"])} — {_esc(row["email"])}</option>'
        for row in employees if row.get("status") == "active"
    )
    labels = {
        "time_reviewed": "Time reviewed", "final_pay_confirmed": "Final pay confirmed",
        "pto_reviewed": "PTO policy reviewed", "company_property_returned": "Property returned",
        "app_access_removed": "App access removed", "records_retained": "Records retained",
    }
    cards = ""
    for row in checklists:
        checks = "".join(
            f'<label><input type="checkbox" name="completed_steps" value="{key}"'
            f'{" checked" if row["checklist"].get(key) else ""} style="width:auto"> {_esc(label)}</label>'
            for key, label in labels.items()
        )
        cards += f"""<form class="hr-form" method="post" action="/admin/hr/offboarding/{row['id']}">
        <div class="hr-kicker">{_esc(row['status'])}</div><h2>{_esc(row['employee_email'])}</h2>
        <p>Last day {_esc(row['last_working_day'])}; final pay {_esc(row['final_pay_date'])}. {_esc(row['reason'])}</p>
        {checks}<button class="hr-btn" type="submit">Save checklist</button></form>"""
    body = f"""{_flash(flash)}<h1 class="hr-h1">Offboarding</h1>
    <p class="hr-sub">The employee stays active until every final-pay, access, property, and record step is confirmed.</p>
    <div class="hr-stack">{cards or '<div class="hr-empty">No offboarding workflows.</div>'}</div>
    <form class="hr-form" method="post" action="/admin/hr/offboarding">
      <div class="hr-kicker">Start offboarding</div>
      <label>Person</label><select name="employee_email" required>{options}</select>
      <label>Type</label><select name="separation_type"><option value="resignation">Resignation</option><option value="termination">Termination</option><option value="contract_end">Contract end</option></select>
      <div class="hr-grid2"><div><label>Last working day</label><input type="date" name="last_working_day" required></div><div><label>Final pay date</label><input type="date" name="final_pay_date" required></div></div>
      <label>Reason/notes</label><textarea name="reason"></textarea>
      <button class="hr-btn" type="submit">Start checklist</button>
    </form>"""
    return hr_shell("Offboarding", "offboarding", body, user=user)


def render_hr_reports(*, user) -> str:
    exports = (
        ("employees", "Employee directory", "Employment status and setup; excludes SSNs."),
        ("time", "Time entries", "Exact punches, hours, and notes."),
        ("payroll", "Payroll register", "Versioned gross, taxes, deductions, net, and hashes."),
        ("liabilities", "Tax liabilities", "Due, paid, filed, and reconciled evidence states."),
        ("compliance", "Employer compliance", "New-hire and future filing tasks with confirmation evidence."),
        ("contractors", "Contractor payments", "Wise references and approval history."),
        ("audit", "HR audit trail", "Who changed or approved each material record."),
    )
    rows = "".join(
        f"""<tr><td><strong>{_esc(label)}</strong><br><span class="hr-sub">{_esc(description)}</span></td>
        <td><a class="hr-btn hr-btn-light" href="/admin/hr/reports/{kind}.csv">Download CSV</a></td></tr>"""
        for kind, label, description in exports
    )
    body = f"""<h1 class="hr-h1">HR reports & exports</h1>
    <p class="hr-sub">Portable records for review, backup, accountant handoff, or a future payroll-provider migration.</p>
    <div class="hr-callout warn"><div class="hr-kicker">Sensitive records</div>
    <p>Exports intentionally exclude full Social Security numbers and sealed tax-election data. Store downloaded files securely.</p></div>
    <table class="hr-tbl"><thead><tr><th>Export</th><th>Action</th></tr></thead><tbody>{rows}</tbody></table>"""
    return hr_shell("Reports", "reports", body, user=user)


def render_hr_compliance(
    tasks: list, calendar_rows: list, *, year: int, user, flash=None
) -> str:
    task_rows = "".join(
        f"""<tr><td>{_esc(item['employee_email'])}</td>
        <td>{'Utah new-hire report' if item['task_type'] == 'utah_new_hire_report' else _esc(item['task_type'])}</td>
        <td>{_esc(item['due_date'])}{' · OVERDUE' if item.get('overdue') else ''}</td>
        <td>{_esc(item['status'])}</td>
        <td>{_esc(item.get('confirmation_reference') or '—')}<br><span class="hr-sub">{_esc(item.get('evidence_note') or 'No evidence recorded')}</span></td>
        <td><form class="hr-form" method="post" action="/admin/hr/compliance/{item['id']}">
          <label>Outside confirmation/reference</label><input name="confirmation_reference" value="{_esc(item.get('confirmation_reference'))}">
          <label>Evidence note</label><input name="evidence_note" required maxlength="500" value="{_esc(item.get('evidence_note'))}">
          <div class="hr-inline"><button class="hr-btn" name="action" value="confirmed">Record confirmed</button>
          <button class="hr-btn hr-btn-light" name="action" value="reopened">Reopen</button></div>
        </form></td></tr>"""
        for item in tasks
    ) or '<tr><td colspan="6" class="hr-empty">No compliance tasks have been generated.</td></tr>'
    calendar_html = "".join(
        f"""<tr><td>{row['period_number']}</td><td>{_esc(row['start_date'])}</td>
        <td>{_esc(row['end_date'])}</td><td>{_esc(row['pay_date'])}</td></tr>"""
        for row in calendar_rows
    )
    body = f"""
    {_flash(flash)}
    <h1 class="hr-h1">Payroll & hiring compliance</h1>
    <p class="hr-sub">Track employer submissions separately from the employee’s onboarding. Recording a task never submits it to an agency.</p>
    <div class="hr-callout warn"><div class="hr-kicker">Outside action required</div>
      <p>Utah new hires must be reported outside Anata. Enter the agency confirmation only after the submission succeeds.</p></div>
    <h2>Employer compliance tasks</h2>
    <table class="hr-tbl"><thead><tr><th>Employee</th><th>Task</th><th>Due</th><th>Status</th><th>Evidence</th><th>Action</th></tr></thead><tbody>{task_rows}</tbody></table>
    <div class="hr-row-head" style="margin-top:30px"><div><h2 style="margin:0">{year} payroll calendar</h2>
      <p class="hr-sub" style="margin:4px 0 0">Authoritative semimonthly schedule: 24 periods.</p></div>
      <form class="hr-inline" method="get" action="/admin/hr/compliance"><input type="number" name="year" min="2026" max="{date.today().year + 2}" value="{year}"><button class="hr-btn hr-btn-light">Open year</button></form>
    </div>
    <table class="hr-tbl"><thead><tr><th>Period</th><th>Starts</th><th>Ends</th><th>Payday</th></tr></thead><tbody>{calendar_html}</tbody></table>
    """
    return hr_shell("Compliance", "compliance", body, user=user)


def render_hr_policies(policy: dict, *, user, flash=None) -> str:
    ack = (
        f"Acknowledged {_esc(policy.get('acknowledged_at'))}"
        if policy.get("acknowledged") else "Acknowledgement required"
    )
    body = f"""{_flash(flash)}<h1 class="hr-h1">{_esc(policy['title'])}</h1>
    <p class="hr-sub">Version {_esc(policy['version'])} · {ack}</p>
    <div class="hr-stack">
      <section class="hr-callout"><h2>Timekeeping and overtime</h2><p>Hourly employees clock in and out for the day using exact time. The workweek is Sunday through Saturday. Overtime should be approved in advance, but all time actually worked must be reported and will be paid.</p></section>
      <section class="hr-callout"><h2>PTO</h2><p>W-2 employees accrue one PTO hour for each 52 paid hours, up to 40 hours. Accrual starts on the hire date; use begins after 90 days. Balances cannot go negative. PTO is paid at the base rate and does not count as hours worked for overtime.</p></section>
      <section class="hr-callout"><h2>Paid holidays</h2><p>New Year's Day, Memorial Day, Independence Day, Labor Day, Thanksgiving, and Christmas are paid after 90 days of W-2 employment. Saturday holidays are observed Friday and Sunday holidays Monday. Holiday pay remains separate from worked time.</p></section>
      <section class="hr-callout"><h2>Payroll and corrections</h2><p>Pay periods are the 1st–15th, paid the 20th, and the 16th–month end, paid the following 5th. Employees should review statements and request corrections promptly. A second authorized person reviews time and payroll changes.</p></section>
      <section class="hr-callout"><h2>Privacy and records</h2><p>Use the secure HR forms for personal and tax information. Do not email Social Security numbers or identity documents. Anata records access and material approvals.</p></section>
    </div>
    {'' if policy.get('acknowledged') else f'''<form class="hr-form" method="post" action="/admin/hr/policies/acknowledge">
      <label><input type="checkbox" name="attested" value="true" required style="width:auto"> I received, read, and acknowledge policy version {_esc(policy["version"])}.</label>
      <button class="hr-btn" type="submit">Acknowledge this version</button></form>'''}"""
    return hr_shell("Policies", "policies", body, user=user)


def render_hr_settings(settings: dict, company: dict, employees: list, opening_balances: list,
                       *, user, flash=None) -> str:
    checked = lambda key: " checked" if settings.get(key) else ""
    review = settings.get("qualified_review") or {}
    balance_by_email = {row["employee_email"]: row for row in opening_balances}
    balance_forms = ""
    for employee in employees:
        if employee.get("employee_type") == "contractor":
            continue
        balance = balance_by_email.get(employee["email"], {})
        balance_forms += f"""
        <details class="hr-callout"><summary>{_esc(employee['full_name'])} — {_esc(employee['email'])}</summary>
        <form class="hr-form" method="post" action="/admin/hr/settings/opening-balance">
          <input type="hidden" name="employee_email" value="{_esc(employee['email'])}">
          <input type="hidden" name="tax_year" value="2026">
          <div class="hr-grid2"><div><label>YTD gross wages</label><input name="gross_wages" value="{_esc(balance.get('gross_wages','0.00'))}" required></div>
          <div><label>Social Security wages</label><input name="social_security_wages" value="{_esc(balance.get('social_security_wages','0.00'))}" required></div></div>
          <div class="hr-grid2"><div><label>Medicare wages</label><input name="medicare_wages" value="{_esc(balance.get('medicare_wages','0.00'))}" required></div>
          <div><label>FUTA wages</label><input name="futa_wages" value="{_esc(balance.get('futa_wages','0.00'))}" required></div></div>
          <div class="hr-grid2"><div><label>Utah UI wages</label><input name="utah_ui_wages" value="{_esc(balance.get('utah_ui_wages','0.00'))}" required></div>
          <div><label>Federal income tax withheld</label><input name="federal_withheld" value="{_esc(balance.get('federal_withheld','0.00'))}" required></div></div>
          <div class="hr-grid2"><div><label>Utah income tax withheld</label><input name="utah_withheld" value="{_esc(balance.get('utah_withheld','0.00'))}" required></div>
          <div><label>Employee Social Security withheld</label><input name="employee_ss_withheld" value="{_esc(balance.get('employee_ss_withheld','0.00'))}" required></div></div>
          <label>Employee Medicare withheld</label><input name="employee_medicare_withheld" value="{_esc(balance.get('employee_medicare_withheld','0.00'))}" required>
          <label>Source and verification note</label><textarea name="source_note" required>{_esc(balance.get('source_note',''))}</textarea>
          <button class="hr-btn" type="submit">Save reviewed opening balance</button>
        </form></details>"""
    body = f"""
    {_flash(flash)}
    <h1 class="hr-h1">HR & payroll settings</h1><p class="hr-sub">The policies currently approved for Anata.</p>
    <form class="hr-form" method="post" action="/admin/hr/settings/company">
      <div class="hr-kicker">Employer legal profile</div>
      <div class="hr-grid2"><div><label>Legal name</label><input name="legal_name" value="{_esc(company.get('legal_name'))}" required></div>
      <div><label>Trade name</label><input name="trade_name" value="{_esc(company.get('trade_name'))}"></div></div>
      <label>EIN last 4 only</label><input name="ein_last4" value="{_esc(company.get('ein_last4'))}" inputmode="numeric" minlength="4" maxlength="4" required>
      <label>Business address</label><input name="address_line1" value="{_esc(company.get('address_line1'))}" required>
      <label>Address line 2</label><input name="address_line2" value="{_esc(company.get('address_line2'))}">
      <div class="hr-grid2"><div><label>City</label><input name="city" value="{_esc(company.get('city'))}" required></div>
      <div><label>State</label><input name="state" value="UT" readonly></div></div>
      <label>ZIP</label><input name="zip_code" value="{_esc(company.get('zip_code'))}" required>
      <label>Payroll contact email</label><input type="email" name="payroll_contact_email" value="{_esc(company.get('payroll_contact_email'))}" required>
      <div class="hr-grid2"><div><label>Utah withholding account last 4</label><input name="utah_withholding_account_last4" value="{_esc(company.get('utah_withholding_account_last4'))}" maxlength="4"></div>
      <div><label>Utah UI account last 4</label><input name="utah_ui_account_last4" value="{_esc(company.get('utah_ui_account_last4'))}" maxlength="4"></div></div>
      <label>Federal deposit schedule confirmed from lookback evidence</label>
      <select name="federal_deposit_schedule" required><option value="">Choose…</option>
      <option value="monthly"{' selected' if company.get('federal_deposit_schedule') == 'monthly' else ''}>Monthly</option>
      <option value="semiweekly"{' selected' if company.get('federal_deposit_schedule') == 'semiweekly' else ''}>Semiweekly</option></select>
      <label>Utah withholding payment frequency shown in TAP</label>
      <select name="utah_withholding_payment_frequency" required><option value="">Choose…</option>
      <option value="quarterly"{' selected' if company.get('utah_withholding_payment_frequency') == 'quarterly' else ''}>Quarterly payment</option>
      <option value="monthly"{' selected' if company.get('utah_withholding_payment_frequency') == 'monthly' else ''}>Monthly payment, quarterly return</option></select>
      <label>Source/review note</label><textarea name="source_note" required>{_esc(company.get('source_note'))}</textarea>
      <button class="hr-btn" type="submit">Save employer profile</button>
    </form>
    <form class="hr-form" method="post" action="/admin/hr/settings">
      <div class="hr-kicker">Required payroll setup</div>
      <label>Utah unemployment rate from the employer notice (decimal)</label>
      <input name="utah_ui_rate" value="{_esc(settings.get('utah_ui_rate'))}" placeholder="0.001" required>
      <label><input type="checkbox" name="eftps_ready" value="true"{checked('eftps_ready')} style="width:auto"> EFTPS access tested</label>
      <label><input type="checkbox" name="utah_tap_ready" value="true"{checked('utah_tap_ready')} style="width:auto"> Utah TAP access tested</label>
      <label><input type="checkbox" name="utah_ui_ready" value="true"{checked('utah_ui_ready')} style="width:auto"> Utah unemployment portal access tested</label>
      <label><input type="checkbox" name="opening_balances_confirmed" value="true"{checked('opening_balances_confirmed')} style="width:auto"> 2026 opening wages, taxes, and prior payments are confirmed</label>
      <label>Opening-balance source / review note</label><textarea name="opening_balance_note" required>{_esc(settings.get('opening_balance_note'))}</textarea>
      <button class="hr-btn" type="submit">Save payroll setup</button>
    </form>
    <form class="hr-form" method="post" action="/admin/hr/settings/qualified-review" style="margin-top:18px">
      <div class="hr-kicker">Independent payroll calculation review</div>
      <p>{'Recorded for 2026 by ' + _esc(review.get('reviewer_name')) + ' on ' + _esc(review.get('reviewed_on')) + '.' if review else 'No qualified 2026 review evidence is recorded. Payroll remains blocked.'}</p>
      <input type="hidden" name="tax_year" value="2026">
      <div class="hr-grid2"><div><label>Reviewer name</label><input name="reviewer_name" value="{_esc(review.get('reviewer_name'))}" required></div>
      <div><label>Reviewer email</label><input type="email" name="reviewer_email" value="{_esc(review.get('reviewer_email'))}" required></div></div>
      <label>Date reviewed</label><input type="date" name="reviewed_on" value="{_esc(review.get('reviewed_on'))}" required>
      <label>Evidence/reference</label><input name="evidence_reference" value="{_esc(review.get('evidence_reference'))}" placeholder="Accountant workpaper, comparison file, or engagement reference" required>
      <label>What was independently checked?</label><textarea name="review_note" required>{_esc(review.get('review_note'))}</textarea>
      <label><input type="checkbox" name="attested" value="true" required style="width:auto"> I confirm the named qualified professional actually reviewed the 2026 calculations and opening setup.</label>
      <button class="hr-btn" type="submit">Record qualified review evidence</button>
    </form>
    <h2>2026 employee opening balances</h2>
    <p class="hr-sub">Enter totals from prior payroll records. Zero is valid only when the source confirms zero.</p>
    {balance_forms or '<div class="hr-empty">Add W-2 employees before entering balances.</div>'}
    <div class="hr-stack">
      <div class="hr-callout"><div class="hr-kicker">PTO</div><h2>40-hour combined PTO bank</h2><p>Accrues 1 hour per 52 paid hours, usable after 90 days, capped at 40 hours. No negative balance. Unused PTO is not paid at separation unless a written agreement requires it.</p></div>
      <div class="hr-callout"><div class="hr-kicker">Paid holidays</div><p>New Year's Day, Memorial Day, Independence Day, Labor Day, Thanksgiving, and Christmas. W-2 employees become eligible after 90 days.</p></div>
      <div class="hr-callout"><div class="hr-kicker">Tax operations</div><p>Utah TAP access confirmed. Federal deposit schedule: semiweekly. EFTPS and Utah unemployment portal access remain setup checks.</p></div>
    </div>"""
    return hr_shell("Settings", "settings", body, user=user)
