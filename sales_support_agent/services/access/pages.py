"""HTML pages for access control.

Phase 1: `render_forbidden_page` — the friendly 403.
Phase 2: `render_users_page`, `render_roles_page`, `render_role_form_page` — full
  Access admin UI at /admin/access.
"""

from __future__ import annotations

import html
from typing import Optional

from sales_support_agent.services.access.catalog import SECTIONS, label_for
from sales_support_agent.services.admin_nav import (
    render_agent_favicon_links,
    render_agent_nav,
    render_agent_nav_styles,
)


def _esc(text: object) -> str:
    return html.escape(str(text if text is not None else ""))


# ---------------------------------------------------------------------------
# Shared styles
# ---------------------------------------------------------------------------

_BASE_STYLES = """
  :root { --dark-blue:#2B3644; --light-blue:#85BBDA; --light-brown:#F9F7F3; --white:#fff;
    --text:#2B3644; --border:rgba(43,54,68,0.10); --shadow:rgba(43,54,68,0.10); }
  * { box-sizing:border-box; }
  body { margin:0; background:var(--light-brown); color:var(--text); font-family:"Inter","Segoe UI",sans-serif; }
  a { color:var(--dark-blue); }
  __NAV__
  .shell { max-width:760px; margin:0 auto; padding:48px 20px 72px; }
  .shell-wide { max-width:1140px; margin:0 auto; padding:40px 20px 72px; }
  .card { background:var(--white); border:1px solid var(--border); border-radius:24px;
    box-shadow:0 18px 40px var(--shadow); padding:40px; text-align:center; }
  h1 { font-family:"Montserrat",sans-serif; color:var(--dark-blue); font-size:26px; margin:8px 0; }
  .muted { color:rgba(43,54,68,0.62); font-size:14.5px; line-height:1.6; }
  .lock { font-size:44px; }
  .btn { display:inline-flex; align-items:center; min-height:44px; padding:0 20px; border-radius:999px;
    background:var(--dark-blue); color:#fff; font-family:"Montserrat",sans-serif; font-weight:700;
    font-size:13px; text-decoration:none; margin-top:18px; }
"""

_ADMIN_STYLES = """
  /* --- Admin table + form shared --- */
  .page-header { display:flex; align-items:center; justify-content:space-between;
    margin-bottom:24px; flex-wrap:wrap; gap:12px; }
  h2 { font-family:"Montserrat",sans-serif; font-size:24px; font-weight:800;
    color:#2B3644; margin:0; }
  .flash { padding:12px 18px; border-radius:12px; margin-bottom:20px;
    font-size:14px; font-weight:600; line-height:1.4; }
  .flash-ok { background:#e8f5e9; color:#2e7d32; border:1px solid #c8e6c9; }
  .flash-err { background:#fce4ec; color:#c62828; border:1px solid #f8bbd0; }
  .tbl-card { background:#fff; border:1px solid rgba(43,54,68,0.10);
    border-radius:20px; box-shadow:0 8px 32px rgba(43,54,68,0.07);
    overflow:hidden; margin-bottom:24px; }
  .tbl-card-header { padding:16px 22px; border-bottom:1px solid rgba(43,54,68,0.08);
    display:flex; align-items:center; justify-content:space-between; flex-wrap:wrap; gap:10px; }
  .tbl-card-title { font-family:"Montserrat",sans-serif; font-weight:800;
    font-size:14px; color:#2B3644; }
  table { width:100%; border-collapse:collapse; }
  th { padding:11px 18px; font-family:"Montserrat",sans-serif; font-size:11px;
    font-weight:700; text-transform:uppercase; letter-spacing:0.06em;
    color:rgba(43,54,68,0.45); text-align:left;
    border-bottom:1px solid rgba(43,54,68,0.07); background:rgba(249,247,243,0.5); }
  td { padding:13px 18px; font-size:14px;
    border-bottom:1px solid rgba(43,54,68,0.05); vertical-align:middle; }
  tr:last-child td { border-bottom:none; }
  tr:hover td { background:rgba(133,187,218,0.04); }
  .cell-email { font-weight:600; color:#2B3644; }
  .cell-muted { color:rgba(43,54,68,0.62); font-size:13.5px; }
  .cell-sm { font-size:13px; color:rgba(43,54,68,0.7); }
  .badge { display:inline-flex; align-items:center; padding:3px 10px;
    border-radius:999px; font-size:11px; font-weight:700;
    font-family:"Montserrat",sans-serif; letter-spacing:0.04em; }
  .badge-active   { background:rgba(76,175,80,0.12);  color:#2e7d32; }
  .badge-suspended{ background:rgba(198,40,40,0.10);  color:#c62828; }
  .badge-super    { background:rgba(133,187,218,0.22); color:#1a5f84; }
  .badge-count    { background:rgba(43,54,68,0.08);   color:rgba(43,54,68,0.6); }
  .acts { display:flex; align-items:center; gap:7px; flex-wrap:wrap; }
  .role-form { display:flex; align-items:center; gap:6px; }
  select.role-sel { padding:5px 10px; border:1px solid rgba(43,54,68,0.18);
    border-radius:8px; font-size:13px; font-family:"Inter",sans-serif;
    background:#fff; color:#2B3644; min-width:140px; cursor:pointer; }
  .btn-xs { display:inline-flex; align-items:center; min-height:30px; padding:0 12px;
    border-radius:999px; font-family:"Montserrat",sans-serif; font-weight:700;
    font-size:11px; border:none; cursor:pointer; text-decoration:none;
    white-space:nowrap; transition:background 100ms; }
  .btn-dark  { background:#2B3644; color:#fff; }
  .btn-dark:hover  { background:#3d4f63; }
  .btn-blue  { background:rgba(133,187,218,0.18); color:#1a5f84;
    border:1px solid rgba(133,187,218,0.35); }
  .btn-blue:hover  { background:rgba(133,187,218,0.28); }
  .btn-red   { background:rgba(198,40,40,0.08); color:#c62828;
    border:1px solid rgba(198,40,40,0.22); }
  .btn-red:hover { background:rgba(198,40,40,0.16); }
  .btn-ghost { background:transparent; color:rgba(43,54,68,0.65);
    border:1px solid rgba(43,54,68,0.15); }
  .btn-ghost:hover { background:rgba(43,54,68,0.05); }
  .btn-primary { display:inline-flex; align-items:center; min-height:42px;
    padding:0 22px; border-radius:999px; background:#2B3644; color:#fff;
    font-family:"Montserrat",sans-serif; font-weight:700; font-size:13px;
    border:none; cursor:pointer; text-decoration:none; }
  .btn-primary:hover { background:#3d4f63; }
  .empty-state { padding:40px; text-align:center; color:rgba(43,54,68,0.40);
    font-size:14px; font-style:italic; }
  /* Role form */
  .form-card { background:#fff; border:1px solid rgba(43,54,68,0.10);
    border-radius:20px; box-shadow:0 8px 32px rgba(43,54,68,0.07);
    padding:32px 36px; margin-bottom:24px; }
  .form-group { margin-bottom:22px; }
  .form-label { display:block; font-family:"Montserrat",sans-serif; font-weight:700;
    font-size:11px; text-transform:uppercase; letter-spacing:0.07em;
    color:rgba(43,54,68,0.65); margin-bottom:7px; }
  .form-input { width:100%; padding:10px 14px; border:1px solid rgba(43,54,68,0.18);
    border-radius:10px; font-size:14px; font-family:"Inter",sans-serif;
    color:#2B3644; background:#fff; box-sizing:border-box;
    transition:border-color 120ms; }
  .form-input:focus { outline:none; border-color:#85BBDA;
    box-shadow:0 0 0 3px rgba(133,187,218,0.18); }
  textarea.form-input { resize:vertical; min-height:74px; }
  .tool-grid { display:grid; grid-template-columns:repeat(auto-fill,minmax(230px,1fr));
    gap:14px; margin-top:10px; }
  .sec-block { background:rgba(133,187,218,0.06); border:1px solid rgba(133,187,218,0.20);
    border-radius:14px; padding:16px 18px; }
  .sec-name { font-family:"Montserrat",sans-serif; font-weight:800; font-size:11px;
    text-transform:uppercase; letter-spacing:0.08em; color:#1a5f84; margin-bottom:10px; }
  .tool-row { display:flex; align-items:center; gap:8px; margin-bottom:8px; cursor:pointer; }
  .tool-row:last-child { margin-bottom:0; }
  .tool-row input[type=checkbox] { width:15px; height:15px; accent-color:#2B3644;
    cursor:pointer; flex-shrink:0; }
  .tool-row-label { font-size:13.5px; color:#2B3644; cursor:pointer; line-height:1.3; }
  .form-actions { display:flex; gap:12px; align-items:center; margin-top:28px;
    padding-top:22px; border-top:1px solid rgba(43,54,68,0.07); }
  .btn-cancel { display:inline-flex; align-items:center; min-height:42px;
    padding:0 22px; border-radius:999px; background:transparent;
    color:rgba(43,54,68,0.7); font-family:"Montserrat",sans-serif; font-weight:700;
    font-size:13px; border:1px solid rgba(43,54,68,0.15); text-decoration:none; }
  .btn-cancel:hover { background:rgba(43,54,68,0.05); }
  .danger-note { font-size:12px; color:#c62828; margin-left:4px; }
"""


def _shell(title: str, body: str, *, user: Optional[dict], active: str = "",
           wide: bool = False) -> str:
    permissions = (user or {}).get("permissions") or set()
    is_superadmin = bool((user or {}).get("is_superadmin"))
    nav = render_agent_nav(active, permissions=permissions, is_superadmin=is_superadmin, user=user)
    container = "shell-wide" if wide else "shell"
    all_styles = (_BASE_STYLES + _ADMIN_STYLES).replace("__NAV__", render_agent_nav_styles())
    return f"""<!doctype html>
<html lang="en"><head>
  <meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
  <title>agent | {_esc(title)}</title>
  {render_agent_favicon_links()}
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Montserrat:wght@700;800;900&display=swap" rel="stylesheet">
  <style>{all_styles}</style>
</head><body>
  {nav}
  <main class="{container}">{body}</main>
</body></html>"""


# ---------------------------------------------------------------------------
# Phase 1: 403 page
# ---------------------------------------------------------------------------


def render_forbidden_page(*, user: Optional[dict], tool_label: str) -> str:
    email = _esc((user or {}).get("email") or "")
    unprovisioned = (user or {}).get("status") == "unprovisioned"
    if unprovisioned:
        sub = (
            "You're signed in, but an administrator hasn't granted you access to any "
            "tools yet. They can set this up under People → Manage access."
        )
    else:
        sub = (f"You don't have access to <strong>{_esc(tool_label)}</strong> yet. "
               "Ask an administrator to grant it under People → Manage access.")
    body = f"""
      <div class="card">
        <div class="lock">🔒</div>
        <h1>No access</h1>
        <p class="muted">{sub}</p>
        <p class="muted">Signed in as {email}.</p>
        <a class="btn" href="/admin">Go to your dashboard</a>
      </div>
    """
    return _shell("No access", body, user=user)


# ---------------------------------------------------------------------------
# Phase 2: Users page
# ---------------------------------------------------------------------------


def _flash_html(flash: Optional[str]) -> str:
    if not flash:
        return ""
    msgs = {
        "role":    ("✓ Role updated.", "ok"),
        "status":  ("✓ Status updated.", "ok"),
        "created": ("✓ Role created.", "ok"),
        "updated": ("✓ Role saved.", "ok"),
        "deleted": ("✓ Role deleted.", "ok"),
        "blocked": ("Cannot delete a role that's still assigned to users.", "err"),
        "taken":   ("A role with that name already exists.", "err"),
        "noname":  ("Role name is required.", "err"),
        "revoked": ("✓ Invite revoked.", "ok"),
        "approved": ("✓ Person approved — set their access with “Manage access”.", "ok"),
        "denied":  ("✓ Request denied.", "ok"),
        "noemail": ("An email address is required to send an invite.", "err"),
        "access":  ("✓ Access updated.", "ok"),
    }
    text, kind = msgs.get(flash, (f"Action: {_esc(flash)}.", "ok"))
    return f'<div class="flash flash-{kind}">{text}</div>'


def _people_badge(text: str, bg: str, fg: str) -> str:
    return (f'<span class="badge" style="background:{bg};color:{fg};'
            f'border:1px solid {fg}22">{_esc(text)}</span>')


def _access_summary(u: dict) -> str:
    """Short human label for a user's granted access, for the People table."""
    if u.get("is_superadmin"):
        return '<span class="badge badge-super">Full access</span>'
    perms = sorted(u.get("permissions") or [])
    if not perms:
        return '<span class="cell-muted">No access yet</span>'
    labels = [label_for(k) for k in perms]
    shown = ", ".join(labels[:3])
    if len(labels) > 3:
        shown += f" +{len(labels) - 3} more"
    return f'<span class="cell-muted">{_esc(shown)}</span>'


def render_users_page(users: list, roles: list, *, current_user: dict,
                      flash: Optional[str] = None,
                      invites: Optional[list] = None,
                      requests_list: Optional[list] = None,
                      history: Optional[list] = None) -> str:
    """Unified People page — pending access requests, pending invites, and
    provisioned users in one table. Access is granted per-person after a user
    exists (Manage access), not via roles: invites carry no role, and approving
    a request just creates the person with no access until you grant it."""
    current_email = (current_user or {}).get("email", "")
    invites = invites or []
    requests_list = requests_list or []

    # --- Pending access requests (need a decision — listed first) ---
    def _req_row(req: dict) -> str:
        rid = _esc(req["id"])
        requested = (req.get("requested_at") or "")[:10]
        return f"""<tr>
          <td class="cell-email">{_esc(req.get("email") or "")}</td>
          <td class="cell-muted">{_esc(req.get("name") or "")}</td>
          <td>{_people_badge("Requested", "#fff3e0", "#8a5a00")}</td>
          <td class="cell-muted">—</td>
          <td class="cell-sm">{_esc(requested)}</td>
          <td><div class="acts">
            <form method="post" action="/admin/access/requests/{rid}/approve">
              <button type="submit" class="btn-xs btn-dark">Approve</button>
            </form>
            <form method="post" action="/admin/access/requests/{rid}/deny">
              <button type="submit" class="btn-xs btn-red">Deny</button>
            </form>
          </div></td>
        </tr>"""

    # --- Pending invites ---
    def _inv_row(inv: dict) -> str:
        iid = _esc(inv["id"])
        created = (inv.get("created_at") or "")[:10]
        return f"""<tr>
          <td class="cell-email">{_esc(inv.get("email") or "")}</td>
          <td class="cell-muted">—</td>
          <td>{_people_badge("Invited", "#e7f0ff", "#2456b8")}</td>
          <td class="cell-muted">—</td>
          <td class="cell-sm">{_esc(created)}</td>
          <td><div class="acts">
            <form method="post" action="/admin/access/invites/{iid}/revoke">
              <button type="submit" class="btn-xs btn-red">Revoke</button>
            </form>
          </div></td>
        </tr>"""

    # --- Provisioned users (active / suspended / super-admin) ---
    def _user_row(u: dict) -> str:
        uid = _esc(u["id"])
        is_self = u["email"] == current_email
        is_super = bool(u.get("is_superadmin"))
        status = u.get("status", "active")

        if is_super:
            status_cell = _people_badge("Super-admin", "#efe7ff", "#5b3aa8")
        elif status == "active":
            status_cell = _people_badge("Active", "#e6f4ec", "#2e7d5b")
        else:
            status_cell = _people_badge("Suspended", "#fdecea", "#8b4c42")

        ll = u.get("last_login_at") or ""
        ll_display = ll[:10] if ll else "—"

        # Manage access — not shown for super-admins (always full) or self.
        manage = "" if (is_super or is_self) else (
            f'<a class="btn-xs btn-dark" href="/admin/access/users/{uid}/access">Manage access</a>')
        if is_super or is_self:
            status_action = ""
        elif status == "active":
            status_action = f"""<form method="post" action="/admin/access/users/{uid}/status">
              <input type="hidden" name="action" value="suspend">
              <button type="submit" class="btn-xs btn-red">Suspend</button>
            </form>"""
        else:
            status_action = f"""<form method="post" action="/admin/access/users/{uid}/status">
              <input type="hidden" name="action" value="activate">
              <button type="submit" class="btn-xs btn-blue">Activate</button>
            </form>"""

        return f"""<tr>
          <td class="cell-email">{_esc(u["email"])}</td>
          <td class="cell-muted">{_esc(u.get("name") or "")}</td>
          <td>{status_cell}</td>
          <td>{_access_summary(u)}</td>
          <td class="cell-sm">{_esc(ll_display)}</td>
          <td><div class="acts">{manage}{status_action}</div></td>
        </tr>"""

    rows = ("".join(_req_row(r) for r in requests_list)
            + "".join(_inv_row(i) for i in invites)
            + "".join(_user_row(u) for u in users))
    if not rows:
        rows = '<tr><td colspan="6" class="empty-state">No people yet.</td></tr>'

    pending_n = len(requests_list) + len(invites)
    pending_note = (f'<span class="tbl-card-title" style="font-weight:500;color:#8a5a00">'
                    f'{pending_n} pending</span>' if pending_n else "")

    body = f"""
    {_flash_html(flash)}
    <div class="page-header">
      <h2>People</h2>
    </div>
    <div class="tbl-card" style="margin-bottom:28px">
      <div class="tbl-card-header">
        <span class="tbl-card-title">Requests, invites &amp; users</span>
        {pending_note}
      </div>
      <table>
        <thead><tr>
          <th>Email</th><th>Name</th><th>Status</th>
          <th>Access</th><th>Date</th><th>Actions</th>
        </tr></thead>
        <tbody>{rows}</tbody>
      </table>
    </div>
    <div class="form-card" style="margin-bottom:28px">
      <div class="tbl-card-title" style="margin-bottom:6px">Send new invite</div>
      <p class="cell-muted" style="margin:0 0 16px;font-size:13px">
        Invite by email — once they accept, set their access with “Manage access”.
      </p>
      <form method="post" action="/admin/access/invites/new"
        style="display:flex;gap:12px;align-items:flex-end;flex-wrap:wrap">
        <div class="form-group" style="margin-bottom:0;flex:1;min-width:260px">
          <label class="form-label">Email address *</label>
          <input class="form-input" type="email" name="email" required
            placeholder="colleague@anatainc.com">
        </div>
        <button type="submit" class="btn-primary">Generate invite link</button>
      </form>
    </div>
    {_history_html(history)}
    """
    return _shell("Access — People", body, user=current_user, active="access_users", wide=True)


def render_user_access_page(user: dict, *, current_user: dict,
                            error: Optional[str] = None) -> str:
    """Per-person access editor — a checkbox grid of tools granted directly to
    one user (replaces the role system)."""
    uid = _esc(user.get("id", ""))
    email = _esc(user.get("email", ""))
    granted = set(user.get("permissions") or [])

    grid_blocks = []
    for section_name, tools in SECTIONS.items():
        checks = ""
        for t in tools:
            checked = ' checked' if t.key in granted else ""
            tid = f"tool_{t.key.replace('.', '_')}"
            checks += f"""<label class="tool-row" for="{tid}">
              <input type="checkbox" id="{tid}" name="permissions" value="{_esc(t.key)}"{checked}>
              <span class="tool-row-label">{_esc(t.label)}</span>
            </label>"""
        grid_blocks.append(f"""<div class="sec-block">
          <div class="sec-name">{_esc(section_name)}</div>
          {checks}
        </div>""")

    error_html = f'<div class="flash flash-err">{_esc(error)}</div>' if error else ""
    body = f"""
    {error_html}
    <div class="page-header">
      <h2>Access for {email}</h2>
    </div>
    <div class="form-card">
      <p class="cell-muted" style="margin:0 0 18px;font-size:13px">
        Tick the tools this person can use. Changes take effect on their next page load.
      </p>
      <form method="post" action="/admin/access/users/{uid}/access">
        <div class="tool-grid">{"".join(grid_blocks)}</div>
        <div class="form-actions">
          <button type="submit" class="btn-primary">Save access</button>
          <a class="btn-cancel" href="/admin/access">Cancel</a>
        </div>
      </form>
    </div>
    """
    return _shell(f"Access — {email}", body, user=current_user, active="access_users", wide=True)


# ---------------------------------------------------------------------------
# Phase 2: Roles list page
# ---------------------------------------------------------------------------


def render_roles_page(roles: list, user_counts: dict, *, current_user: dict,
                      flash: Optional[str] = None) -> str:
    def _role_row(r: dict) -> str:
        rid = _esc(r["id"])
        cnt = user_counts.get(r["id"], 0)
        tool_count = len(r.get("permissions") or [])
        edit_btn = f'<a class="btn-xs btn-blue" href="/admin/access/roles/{rid}/edit">Edit</a>'
        if cnt == 0:
            del_btn = f"""<form method="post" action="/admin/access/roles/{rid}/delete"
              onsubmit="return confirm('Delete role «{_esc(r["name"])}»?')">
              <button type="submit" class="btn-xs btn-red">Delete</button>
            </form>"""
        else:
            del_btn = f'<span style="font-size:12px;color:rgba(43,54,68,0.4);">{cnt} user{"s" if cnt != 1 else ""} assigned</span>'
        return f"""<tr>
          <td class="cell-email">{_esc(r["name"])}</td>
          <td class="cell-muted">{_esc(r.get("description") or "")}</td>
          <td><span class="badge badge-count">{tool_count} tool{"s" if tool_count != 1 else ""}</span></td>
          <td class="cell-sm">{cnt}</td>
          <td><div class="acts">{edit_btn}{del_btn}</div></td>
        </tr>"""

    rows = "".join(_role_row(r) for r in roles)
    if not rows:
        rows = '<tr><td colspan="5" class="empty-state">No roles yet — create one to get started.</td></tr>'

    body = f"""
    {_flash_html(flash)}
    <div class="page-header">
      <h2>Roles</h2>
      <a class="btn-primary" href="/admin/access/roles/new">+ New role</a>
    </div>
    <div class="tbl-card">
      <table>
        <thead><tr>
          <th>Role name</th><th>Description</th>
          <th>Tools</th><th>Users</th><th>Actions</th>
        </tr></thead>
        <tbody>{rows}</tbody>
      </table>
    </div>
    <p style="margin-top:0; font-size:13px; color:rgba(43,54,68,0.5);">
      ← <a href="/admin/access">Back to people</a>
    </p>
    """
    return _shell("Access — Roles", body, user=current_user, active="access_roles", wide=True)


# ---------------------------------------------------------------------------
# Phase 2: Role create / edit form
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Phase 3: standalone info pages (no cookie / no nav — shown to non-authed users)
# ---------------------------------------------------------------------------


def _standalone_page(title: str, icon: str, heading: str, body_html: str) -> str:
    styles = (_BASE_STYLES + _ADMIN_STYLES).replace("__NAV__", "")
    return f"""<!doctype html>
<html lang="en"><head>
  <meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
  <title>agent | {_esc(title)}</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Montserrat:wght@700;800;900&display=swap" rel="stylesheet">
  <style>{styles}</style>
</head><body>
  <div class="shell">
    <div class="card" style="margin-top:60px">
      <div class="lock">{icon}</div>
      <h1>{_esc(heading)}</h1>
      {body_html}
    </div>
  </div>
</body></html>"""


def render_access_pending_page(email: str) -> str:
    body = f"""
      <p class="muted">
        Your sign-in was received for <strong>{_esc(email)}</strong>, but your account
        hasn't been set up yet.<br><br>
        An administrator has been notified and will approve your access shortly.
        Try signing in again once you've been approved.
      </p>
      <a class="btn" href="/admin/login" style="margin-top:22px">Back to sign-in</a>
    """
    return _standalone_page("Access pending", "⏳", "Access requested", body)


def render_suspended_page(email: str) -> str:
    body = f"""
      <p class="muted">
        The account for <strong>{_esc(email)}</strong> has been suspended.<br><br>
        Contact your administrator if you believe this is a mistake.
      </p>
    """
    return _standalone_page("Account suspended", "🚫", "Account suspended", body)


def render_invite_invalid_page() -> str:
    body = """
      <p class="muted">
        This invite link is invalid, expired, or has already been used.<br><br>
        Ask your administrator to send a new one.
      </p>
      <a class="btn" href="/admin/login" style="margin-top:22px">Sign in</a>
    """
    return _standalone_page("Invalid invite", "🔗", "Invalid invite link", body)


# ---------------------------------------------------------------------------
# Phase 3: Invite created confirmation (shown inline after POST)
# ---------------------------------------------------------------------------


def render_invite_created_page(invite_link: str, email: str, *,
                                current_user: dict, email_sent: bool = False) -> str:
    if email_sent:
        sent_note = (
            '<p style="color:#2e7d5b;font-weight:600;font-size:13px;margin-bottom:14px">'
            '&#10003; Invite emailed to the recipient. You can also share the link below.</p>'
        )
    else:
        sent_note = ""
    body = f"""
    <div class="page-header">
      <h2>Invite created</h2>
    </div>
    <div class="form-card" style="text-align:center">
      <p style="font-size:15px;color:#2B3644;font-weight:600;margin-bottom:6px">
        Invite for <span style="color:#1a5f84">{_esc(email)}</span>
      </p>
      {sent_note}
      <p class="cell-muted" style="margin-bottom:18px">
        Copy the link below and send it to the recipient. It expires in 7 days
        and can only be used once.
      </p>
      <div style="display:flex;gap:8px;align-items:center;justify-content:center;flex-wrap:wrap">
        <input id="inv-link" type="text" readonly
          value="{_esc(invite_link)}"
          style="flex:1;min-width:240px;max-width:540px;padding:10px 14px;
            border:1px solid rgba(133,187,218,0.5);border-radius:10px;
            font-size:13px;font-family:monospace;background:rgba(133,187,218,0.06);
            color:#2B3644;cursor:text">
        <button onclick="navigator.clipboard.writeText(document.getElementById('inv-link').value);
          this.textContent='Copied ✓';setTimeout(()=>this.textContent='Copy',2000)"
          class="btn-primary">Copy</button>
      </div>
      <div style="margin-top:24px">
        <a class="btn-cancel" href="/admin/access">← Back to people</a>
      </div>
    </div>
    """
    return _shell("Invite created", body, user=current_user, active="access_invites", wide=True)


# ---------------------------------------------------------------------------
# Phase 3: Invites admin page
# ---------------------------------------------------------------------------


def render_invites_page(invites: list, roles: list, *, current_user: dict,
                        flash: Optional[str] = None) -> str:
    roles_opts = "".join(
        f'<option value="{_esc(r["id"])}">{_esc(r["name"])}</option>'
        for r in roles
    )

    def _inv_row(inv: dict) -> str:
        iid = _esc(inv["id"])
        created = (inv.get("created_at") or "")[:10]
        role_name = inv.get("role_name") or "—"
        return f"""<tr>
          <td class="cell-email">{_esc(inv.get("email") or "")}</td>
          <td class="cell-muted">{_esc(role_name)}</td>
          <td class="cell-sm">{_esc(created)}</td>
          <td><div class="acts">
            <form method="post" action="/admin/access/invites/{iid}/revoke">
              <button type="submit" class="btn-xs btn-red">Revoke</button>
            </form>
          </div></td>
        </tr>"""

    rows = "".join(_inv_row(i) for i in invites)
    if not rows:
        rows = '<tr><td colspan="4" class="empty-state">No pending invites.</td></tr>'

    body = f"""
    {_flash_html(flash)}
    <div class="page-header"><h2>Invites</h2></div>
    <div class="tbl-card" style="margin-bottom:28px">
      <div class="tbl-card-header">
        <span class="tbl-card-title">Pending invites</span>
      </div>
      <table>
        <thead><tr><th>Email</th><th>Role</th><th>Created</th><th>Actions</th></tr></thead>
        <tbody>{rows}</tbody>
      </table>
    </div>
    <div class="form-card">
      <div class="tbl-card-title" style="margin-bottom:18px">Send new invite</div>
      <form method="post" action="/admin/access/invites/new">
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:14px">
          <div class="form-group" style="margin-bottom:0">
            <label class="form-label">Email address *</label>
            <input class="form-input" type="email" name="email" required
              placeholder="colleague@anatainc.com">
          </div>
          <div class="form-group" style="margin-bottom:0">
            <label class="form-label">Role</label>
            <select class="form-input" name="role_id">
              <option value="">— No role (assign later) —</option>
              {roles_opts}
            </select>
          </div>
        </div>
        <button type="submit" class="btn-primary">Generate invite link</button>
      </form>
    </div>
    """
    return _shell("Access — Invites", body, user=current_user, active="access_invites", wide=True)


# ---------------------------------------------------------------------------
# Phase 3: Access requests admin page
# ---------------------------------------------------------------------------


def render_requests_page(requests_list: list, roles: list, *, current_user: dict,
                         flash: Optional[str] = None, history: Optional[list] = None) -> str:
    roles_opts = "".join(
        f'<option value="{_esc(r["id"])}">{_esc(r["name"])}</option>'
        for r in roles
    )

    def _req_row(req: dict) -> str:
        rid = _esc(req["id"])
        requested = (req.get("requested_at") or "")[:10]
        return f"""<tr>
          <td class="cell-email">{_esc(req.get("email") or "")}</td>
          <td class="cell-muted">{_esc(req.get("name") or "")}</td>
          <td class="cell-sm">{_esc(requested)}</td>
          <td><div class="acts">
            <form method="post" action="/admin/access/requests/{rid}/approve"
              style="display:flex;align-items:center;gap:6px">
              <select name="role_id" class="role-sel" style="min-width:130px">
                <option value="">— No role —</option>
                {roles_opts}
              </select>
              <button type="submit" class="btn-xs btn-dark">Approve</button>
            </form>
            <form method="post" action="/admin/access/requests/{rid}/deny">
              <button type="submit" class="btn-xs btn-red">Deny</button>
            </form>
          </div></td>
        </tr>"""

    rows = "".join(_req_row(r) for r in requests_list)
    if not rows:
        rows = '<tr><td colspan="4" class="empty-state">No pending access requests.</td></tr>'

    body = f"""
    {_flash_html(flash)}
    <div class="page-header"><h2>Access Requests</h2></div>
    <div class="tbl-card">
      <table>
        <thead><tr><th>Email</th><th>Name</th><th>Requested</th><th>Actions</th></tr></thead>
        <tbody>{rows}</tbody>
      </table>
    </div>
    {_history_html(history)}
    """
    return _shell("Access — Requests", body, user=current_user, active="access_requests", wide=True)


def _history_html(history: Optional[list]) -> str:
    if not history:
        return ""

    def _h_row(req: dict) -> str:
        status = req.get("status") or ""
        badge = ('<span style="color:#2e7d5b;font-weight:700">Approved</span>' if status == "approved"
                 else '<span style="color:#8b4c42;font-weight:700">Denied</span>')
        decided = (req.get("decided_at") or "")[:10]
        return f"""<tr>
          <td class="cell-email">{_esc(req.get("email") or "")}</td>
          <td class="cell-sm">{badge}</td>
          <td class="cell-muted">{_esc(req.get("decided_by") or "")}</td>
          <td class="cell-sm">{_esc(decided)}</td>
        </tr>"""

    rows = "".join(_h_row(r) for r in history)
    return f"""
    <div class="page-header" style="margin-top:34px"><h2 style="font-size:17px">Decision history</h2></div>
    <div class="tbl-card">
      <table>
        <thead><tr><th>Email</th><th>Decision</th><th>Decided by</th><th>Date</th></tr></thead>
        <tbody>{rows}</tbody>
      </table>
    </div>
    """


# ---------------------------------------------------------------------------
# Phase 2: Role create / edit form
# ---------------------------------------------------------------------------


def render_role_form_page(role: Optional[dict], *, current_user: dict,
                          new: bool = False, error: Optional[str] = None) -> str:
    title = "New role" if new else f'Edit role: {_esc((role or {}).get("name", ""))}'
    action = "/admin/access/roles/new" if new else f'/admin/access/roles/{_esc((role or {}).get("id", ""))}/edit'
    current_perms = set((role or {}).get("permissions") or [])
    name_val = _esc((role or {}).get("name") or "")
    desc_val = _esc((role or {}).get("description") or "")

    # Build checkbox grid, grouped by section
    grid_blocks = []
    for section_name, tools in SECTIONS.items():
        checks = ""
        for t in tools:
            checked = ' checked' if t.key in current_perms else ""
            tid = f"tool_{t.key.replace('.', '_')}"
            checks += f"""<label class="tool-row" for="{tid}">
              <input type="checkbox" id="{tid}" name="permissions"
                value="{_esc(t.key)}"{checked}>
              <span class="tool-row-label">{_esc(t.label)}</span>
            </label>"""
        grid_blocks.append(f"""<div class="sec-block">
          <div class="sec-name">{_esc(section_name)}</div>
          {checks}
        </div>""")

    error_html = f'<div class="flash flash-err">{_esc(error)}</div>' if error else ""

    body = f"""
    {error_html}
    <div class="page-header">
      <h2>{title}</h2>
    </div>
    <div class="form-card">
      <form method="post" action="{action}">
        <div class="form-group">
          <label class="form-label">Role name *</label>
          <input class="form-input" type="text" name="name" required
            value="{name_val}" placeholder="e.g. Advertising manager" maxlength="80">
        </div>
        <div class="form-group">
          <label class="form-label">Description</label>
          <textarea class="form-input" name="description"
            placeholder="Optional — what this role is for">{desc_val}</textarea>
        </div>
        <div class="form-group">
          <label class="form-label">Tool access</label>
          <div class="tool-grid">{"".join(grid_blocks)}</div>
        </div>
        <div class="form-actions">
          <button type="submit" class="btn-primary">
            {"Create role" if new else "Save changes"}
          </button>
          <a class="btn-cancel" href="/admin/access/roles">Cancel</a>
        </div>
      </form>
    </div>
    """
    active = "access_roles"
    return _shell(title, body, user=current_user, active=active, wide=True)
