"""HTML pages for access control.

Phase 1: `render_forbidden_page` — the friendly 403.
Phase 2: `render_users_page`, `render_roles_page`, `render_role_form_page` — full
  Access admin UI at /admin/access.
"""

from __future__ import annotations

import html
from typing import Optional

from sales_support_agent.services.access.catalog import SECTIONS
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
            "Your account isn't set up with any tools yet. An administrator needs to "
            "assign you a role before you can use the dashboard."
        )
    else:
        sub = (f"Your role doesn't include access to <strong>{_esc(tool_label)}</strong>. "
               "Ask an administrator to grant it.")
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
    }
    text, kind = msgs.get(flash, (f"Action: {_esc(flash)}.", "ok"))
    return f'<div class="flash flash-{kind}">{text}</div>'


def render_users_page(users: list, roles: list, *, current_user: dict,
                      flash: Optional[str] = None) -> str:
    current_email = (current_user or {}).get("email", "")
    roles_opts = "".join(
        f'<option value="{_esc(r["id"])}">{_esc(r["name"])}</option>'
        for r in roles
    )

    def _user_row(u: dict) -> str:
        uid = _esc(u["id"])
        is_self = u["email"] == current_email
        is_super = bool(u.get("is_superadmin"))

        # Role select
        opts = '<option value="">— No role —</option>'
        for r in roles:
            sel = ' selected' if u.get("role_id") == r["id"] else ""
            opts += f'<option value="{_esc(r["id"])}"{sel}>{_esc(r["name"])}</option>'

        role_cell = f"""<form class="role-form" method="post" action="/admin/access/users/{uid}/role">
          <select name="role_id" class="role-sel">{opts}</select>
          <button type="submit" class="btn-xs btn-dark">Save</button>
        </form>"""
        if is_super:
            role_cell = '<span class="badge badge-super">Super-admin</span>'

        # Status badge
        status = u.get("status", "active")
        badge_cls = "badge-active" if status == "active" else "badge-suspended"
        status_cell = f'<span class="badge {badge_cls}">{_esc(status)}</span>'

        # Last login
        ll = u.get("last_login_at") or ""
        ll_display = ll[:10] if ll else "—"

        # Action: suspend/activate — blocked for self and super-admins
        if is_super or is_self:
            action_cell = ""
        elif status == "active":
            action_cell = f"""<form method="post" action="/admin/access/users/{uid}/status">
              <input type="hidden" name="action" value="suspend">
              <button type="submit" class="btn-xs btn-red">Suspend</button>
            </form>"""
        else:
            action_cell = f"""<form method="post" action="/admin/access/users/{uid}/status">
              <input type="hidden" name="action" value="activate">
              <button type="submit" class="btn-xs btn-blue">Activate</button>
            </form>"""

        return f"""<tr>
          <td class="cell-email">{_esc(u["email"])}</td>
          <td class="cell-muted">{_esc(u.get("name") or "")}</td>
          <td>{role_cell}</td>
          <td>{status_cell}</td>
          <td class="cell-sm">{_esc(ll_display)}</td>
          <td><div class="acts">{action_cell}</div></td>
        </tr>"""

    rows = "".join(_user_row(u) for u in users)
    if not rows:
        rows = '<tr><td colspan="6" class="empty-state">No users provisioned yet.</td></tr>'

    body = f"""
    {_flash_html(flash)}
    <div class="page-header">
      <h2>Users</h2>
      <a class="btn-primary" href="/admin/access/roles">Manage roles →</a>
    </div>
    <div class="tbl-card">
      <table>
        <thead><tr>
          <th>Email</th><th>Name</th><th>Role</th>
          <th>Status</th><th>Last login</th><th>Actions</th>
        </tr></thead>
        <tbody>{rows}</tbody>
      </table>
    </div>
    """
    return _shell("Access — Users", body, user=current_user, active="access_users", wide=True)


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
      ← <a href="/admin/access">Back to users</a>
    </p>
    """
    return _shell("Access — Roles", body, user=current_user, active="access_roles", wide=True)


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
