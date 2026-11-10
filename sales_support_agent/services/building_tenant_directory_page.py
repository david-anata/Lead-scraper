"""Admin page for the Anata Building tenant directory."""

from __future__ import annotations

import html
from typing import Any, Optional
from urllib.parse import urlencode

from sales_support_agent.services.admin_nav import (
    render_agent_favicon_links,
    render_agent_nav,
    render_agent_nav_styles,
)
from sales_support_agent.services.building_tenant_directory import MAX_IMPORT_ROWS


def _esc(value: Any) -> str:
    return html.escape(str(value if value is not None else ""))


def _lock_label(value: str) -> str:
    if not value:
        return "No"
    if value == "yes":
        return "Yes"
    return value


def _status_filters(current: str, query: str) -> str:
    labels = (
        ("active", "Active"),
        ("notices", "Notice recipients"),
        ("inactive", "Inactive"),
        ("all", "All"),
    )
    links = []
    for key, label in labels:
        params = {"status": key}
        if query:
            params["q"] = query
        current_attr = ' aria-current="page"' if key == current else ""
        style = "admin-btn" if key == current else "admin-btn admin-btn--ghost"
        links.append(
            f'<a class="{style}" href="/admin/building/tenants?{urlencode(params)}"{current_attr}>{label}</a>'
        )
    return "".join(links)


def render_tenant_directory_page(
    *,
    user: dict,
    contacts: list[dict[str, Any]],
    matched: int,
    metrics: dict[str, int],
    query: str,
    status: str,
    edit_contact: Optional[dict[str, Any]],
    notice: str,
    error: str,
    preview: Optional[dict[str, Any]],
    table_ready: bool,
    csrf_token: str,
    recipient_emails: list[str],
) -> str:
    """Render the directory workspace with the shared Agent shell."""

    nav = render_agent_nav("building_tenants", user=user, include_content_target=False)
    editing = edit_contact or {}
    form_title = "Edit tenant contact" if editing else "Add a tenant contact"
    checked_active = "checked" if (not editing or editing.get("is_active", True)) else ""
    checked_notices = "checked" if (not editing or editing.get("receives_notices", True)) else ""
    lock_name = str(editing.get("august_lock") or "")
    checked_lock = "checked" if lock_name else ""
    if lock_name == "yes":
        lock_name = ""
    alerts = ""
    if notice:
        alerts += f'<div class="app-alert app-alert--notice" role="status"><p>{_esc(notice)}</p></div>'
    if error:
        alerts += f'<div class="app-alert app-alert--error" role="alert"><p>{_esc(error)}</p></div>'
    if not table_ready:
        alerts += (
            '<div class="app-alert app-alert--blocked" role="alert"><p>'
            "The tenant directory table is not in this database yet. Apply "
            "<code>supabase/migrations/20261110205000_building_tenant_directory.sql</code>. "
            "That migration only creates a new table."
            "</p></div>"
        )
    rows = []
    for item in contacts:
        active = bool(item.get("is_active"))
        notices_on = bool(item.get("receives_notices"))
        action = "reactivate" if not active else "deactivate"
        action_label = "Reactivate" if not active else "Deactivate"
        rows.append(
            f"""<tr>
              <td><strong>{_esc(item.get("full_name"))}</strong></td>
              <td>{_esc(item.get("company_name"))}</td>
              <td><a href="mailto:{_esc(item.get("email"))}">{_esc(item.get("email"))}</a></td>
              <td>{_esc(item.get("phone") or "—")}</td>
              <td>{_esc(item.get("office_suite") or "—")}</td>
              <td>{_esc(_lock_label(str(item.get("august_lock") or "")))}</td>
              <td>{"Yes" if notices_on else "No"}</td>
              <td><span class="app-status {"app-status--ready" if active else "app-status--blocked"}">{"Active" if active else "Inactive"}</span></td>
              <td>
                <a class="admin-btn admin-btn--ghost" href="/admin/building/tenants?{urlencode({"edit": item.get("id"), "status": status, "q": query})}">Edit</a>
                <form method="post" action="/admin/building/tenants/{_esc(item.get("id"))}/{action}">
                  <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
                  <button class="admin-btn admin-btn--ghost" type="submit">{action_label}</button>
                </form>
              </td>
            </tr>"""
        )
    if rows:
        table = (
            '<div class="app-data-workspace"><table class="app-table">'
            "<thead><tr><th>Name</th><th>Company</th><th>Email</th><th>Phone</th>"
            "<th>Office</th><th>August lock</th><th>Notices</th><th>Status</th><th>Actions</th></tr></thead>"
            f"<tbody>{''.join(rows)}</tbody></table></div>"
        )
    else:
        table = (
            '<div class="app-state-panel"><h2>No contacts in this view</h2>'
            "<p>Add a person, change the filter, or import a CSV.</p></div>"
        )
    shown = (
        f"Showing {len(contacts)} of {matched}. Refine the search to see the rest."
        if matched > len(contacts)
        else f"{matched} contact{'s' if matched != 1 else ''}"
    )
    preview_html = ""
    if preview:
        preview_rows = []
        labels = {
            "ready": "Ready to add",
            "duplicate": "Already saved",
            "duplicate_in_file": "Duplicate in file",
            "invalid": "Needs a fix",
        }
        for item in preview.get("rows") or []:
            preview_rows.append(
                "<tr>"
                f"<td>{_esc(labels.get(item.get('status'), item.get('status')))}</td>"
                f"<td>{_esc(item.get('full_name') or '—')}</td>"
                f"<td>{_esc(item.get('company_name') or '—')}</td>"
                f"<td>{_esc(item.get('email') or '—')}</td>"
                f"<td>{_esc(item.get('office_suite') or '—')}</td>"
                f"<td>{'Yes' if item.get('receives_notices') else 'No'}</td>"
                f"<td>{'Active' if item.get('is_active') else 'Inactive'}</td>"
                f"<td>{_esc(item.get('reason') or 'Will be added')}</td>"
                "</tr>"
            )
        summary = preview.get("summary") or {}
        confirm = ""
        if summary.get("ready"):
            confirm = f"""
            <form method="post" action="/admin/building/tenants/import/apply">
              <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
              <input type="hidden" name="preview_token" value="{_esc(preview.get("token"))}">
              <textarea name="csv_text" hidden>{_esc(preview.get("csv_text"))}</textarea>
              <button class="admin-btn" type="submit">Add {summary.get("ready")} new contact{"s" if summary.get("ready") != 1 else ""}</button>
            </form>
            """
        preview_html = f"""
        <section class="admin-panel" aria-labelledby="import-preview-title">
          <h2 id="import-preview-title">Import preview</h2>
          <p class="app-muted">{summary.get("ready", 0)} ready to add, {summary.get("duplicate", 0)} duplicates skipped, {summary.get("invalid", 0)} need a fix. Nothing is saved until you confirm.</p>
          <div class="app-data-workspace"><table class="app-table">
            <thead><tr><th>Result</th><th>Name</th><th>Company</th><th>Email</th><th>Office</th><th>Notices</th><th>Status</th><th>Detail</th></tr></thead>
            <tbody>{''.join(preview_rows)}</tbody>
          </table></div>
          <div class="app-form-grid__actions">{confirm}<a class="admin-btn admin-btn--ghost" href="/admin/building/tenants">Discard preview</a></div>
        </section>
        """
    disabled = "disabled" if not table_ready else ""
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Tenant directory · Anata Agent</title>
  {render_agent_favicon_links()}
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Montserrat:wght@700;800&display=swap" rel="stylesheet">
  <link rel="stylesheet" href="/static/admin.css?v=5">
  <style>
    {render_agent_nav_styles()}
    .tenant-page {{ padding-block: 28px 64px; }}
    .tenant-page h2 {{ margin: 0 0 8px; font: 800 1.25rem/1.25 "Montserrat", sans-serif; letter-spacing: -0.02em; }}
    .tenant-section {{ margin-top: 22px; }}
    .tenant-actions {{ display: flex; flex-wrap: wrap; gap: 8px; }}
    .tenant-actions form {{ margin: 0; }}
    .app-check {{ display: flex; align-items: center; gap: 8px; min-height: 42px; font-weight: 700; }}
    .tenant-recipients {{ width: 100%; min-height: 72px; }}
  </style>
</head>
<body>
  {nav}
  <main class="admin-shell app-page tenant-page" id="agent-main-content">
    {alerts}
    <header class="app-page-header">
      <div>
        <p class="admin-eyebrow">Building operations</p>
        <h1>Tenant directory</h1>
        <p>One editable list of people in the Anata Building. The building manager bot uses the notice recipients for event notices, quarterly lock-battery reminders, and mail-arrival alerts. Save the Fondell Industrial and Boulder Ranch owner team here with “Receives building notices” turned off.</p>
      </div>
      <div class="app-page-actions">
        <a class="admin-btn" href="#tenant-form">Add contact</a>
        <a class="admin-btn admin-btn--ghost" href="/admin/building">Building control</a>
      </div>
    </header>
    <section class="app-metric-strip" aria-label="Directory summary">
      <div class="app-metric"><div class="app-metric__value">{metrics.get("active", 0)}</div><div class="app-metric__label">Active contacts</div></div>
      <div class="app-metric"><div class="app-metric__value">{metrics.get("notices", 0)}</div><div class="app-metric__label">Notice recipients</div></div>
      <div class="app-metric"><div class="app-metric__value">{metrics.get("companies", 0)}</div><div class="app-metric__label">Companies</div></div>
      <div class="app-metric"><div class="app-metric__value">{metrics.get("inactive", 0)}</div><div class="app-metric__label">Inactive</div></div>
    </section>
    <form class="app-command-bar" method="get" action="/admin/building/tenants">
      <label class="app-field app-field--inline"><span>Search</span><input name="q" value="{_esc(query)}" placeholder="Name, company, email, office"></label>
      <input type="hidden" name="status" value="{_esc(status)}">
      <button class="admin-btn" type="submit">Search</button>
      <span class="app-muted">{_esc(shown)}</span>
    </form>
    <div class="tenant-actions" aria-label="Directory filters">{_status_filters(status, query)}</div>
    <section class="tenant-section" aria-labelledby="directory-title">
      <div class="content-section__head">
        <h2 id="directory-title">Contacts</h2>
        <p class="app-muted">Several people at one company are separate rows. Deactivate someone who leaves; their history stays.</p>
      </div>
      {table}
    </section>
    <section class="admin-panel tenant-section" aria-labelledby="recipients-title">
      <h2 id="recipients-title">Copy notice recipients</h2>
      <p class="app-muted">Active contacts with notices turned on. Inactive people and the owner team are left out.</p>
      <label class="app-field"><span>Recipient emails</span>
        <textarea class="tenant-recipients" id="notice-recipient-emails" readonly>{_esc(chr(10).join(recipient_emails))}</textarea>
      </label>
      <div class="app-form-grid__actions">
        <button class="admin-btn" type="button" id="copy-notice-recipients">Copy notice recipients</button>
        <p id="copy-notice-status" class="app-muted" role="status"></p>
      </div>
    </section>
    <section class="admin-panel tenant-section" id="tenant-form" aria-labelledby="form-title">
      <h2 id="form-title">{form_title}</h2>
      <form class="app-form-grid" method="post" action="/admin/building/tenants">
        <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
        <input type="hidden" name="contact_id" value="{_esc(editing.get("id") or "")}">
        <label class="app-field"><span>Name</span><input name="full_name" required maxlength="255" value="{_esc(editing.get("full_name") or "")}" {disabled}></label>
        <label class="app-field"><span>Company</span><input name="company_name" required maxlength="255" value="{_esc(editing.get("company_name") or "")}" {disabled}></label>
        <label class="app-field"><span>Email</span><input name="email" type="email" required maxlength="255" value="{_esc(editing.get("email") or "")}" {disabled}></label>
        <label class="app-field"><span>Phone</span><input name="phone" type="tel" maxlength="128" value="{_esc(editing.get("phone") or "")}" {disabled}></label>
        <label class="app-field"><span>Office or suite</span><input name="office_suite" maxlength="128" value="{_esc(editing.get("office_suite") or "")}" {disabled}></label>
        <label class="app-field"><span>August lock name</span><input name="august_lock_name" maxlength="255" value="{_esc(lock_name)}" placeholder="Optional. Leave blank if the lock has no name." {disabled}></label>
        <label class="app-check"><input type="checkbox" name="has_august_lock" value="true" {checked_lock} {disabled}> Has an August lock</label>
        <label class="app-check"><input type="checkbox" name="receives_notices" value="true" {checked_notices} {disabled}> Receives building notices</label>
        <label class="app-check"><input type="checkbox" name="is_active" value="true" {checked_active} {disabled}> Active</label>
        <label class="app-field" style="grid-column: 1 / -1"><span>Notes</span><textarea name="notes" maxlength="5000" {disabled}>{_esc(editing.get("notes") or "")}</textarea></label>
        <div class="app-form-grid__actions">
          <button class="admin-btn" type="submit" {disabled}>{"Save changes" if editing else "Add contact"}</button>
          {"<a class='admin-btn admin-btn--ghost' href='/admin/building/tenants'>Cancel edit</a>" if editing else ""}
        </div>
      </form>
    </section>
    <section class="admin-panel tenant-section" aria-labelledby="import-title">
      <h2 id="import-title">Import CSV</h2>
      <p class="app-muted">Use columns such as name, email, phone, company, and office. A UniFi Access export is fine: first and last name are combined, and user group is used as the company when no company column is present. Duplicate emails are shown and skipped. Up to {MAX_IMPORT_ROWS} rows.</p>
      <form class="app-form-grid" method="post" action="/admin/building/tenants/import/preview" enctype="multipart/form-data">
        <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
        <label class="app-field"><span>CSV file</span><input name="csv_file" type="file" accept=".csv,text/csv,text/plain" required {disabled}></label>
        <div class="app-form-grid__actions"><button class="admin-btn" type="submit" {disabled}>Preview import</button></div>
      </form>
    </section>
    {preview_html}
  </main>
  <script>
    document.getElementById("copy-notice-recipients").addEventListener("click", async function () {{
      const status = document.getElementById("copy-notice-status");
      try {{
        const response = await fetch("/admin/building/tenants/notice-recipients", {{
          headers: {{ "Accept": "application/json" }},
          credentials: "same-origin"
        }});
        if (!response.ok) throw new Error("unavailable");
        const data = await response.json();
        const text = (data.emails || []).join("\\n");
        await navigator.clipboard.writeText(text);
        const count = data.count || 0;
        status.textContent = count ? "Copied " + count + " recipient email" + (count === 1 ? "" : "s") + "." : "There are no notice recipients to copy.";
      }} catch (err) {{
        const fallback = document.getElementById("notice-recipient-emails");
        fallback.focus();
        fallback.select();
        status.textContent = "Select the emails above and copy them.";
      }}
    }});
  </script>
</body>
</html>"""
