"""Operator page: the list of call trackers and the form to start a new one."""

from __future__ import annotations

import html
from datetime import datetime, timezone
from typing import Iterable, Optional

from sales_support_agent.services.admin_nav import render_agent_nav
from sales_support_agent.services.sales.call_script.service import (
    BASE_PATH,
    STATUS_ARCHIVED,
    TrackerSummary,
)
from sales_support_agent.services.ui_shell import render_operator_document

_STAGE_STATUS = {
    "Not started": "queued",
    "In discovery": "running",
    "Pitched": "review",
    "Reviewed": "confirmed",
}

_FAVICON = (
    '<link rel="icon" type="image/svg+xml" href="/brand-static/call-script-favicon.svg">'
    '<link rel="icon" type="image/png" sizes="32x32" href="/brand-static/call-script-favicon-32.png">'
    '<link rel="apple-touch-icon" sizes="180x180" href="/brand-static/call-script-favicon-180.png">'
)

_STYLES = """<style>
.call-list td .app-table__sub{margin-top:2px}
.call-actions{display:flex;gap:8px;align-items:center}
.call-actions form{margin:0}
</style>"""


def _esc(value: object) -> str:
    return html.escape(str(value if value is not None else ""), quote=True)


def _ago(when: Optional[datetime], now: datetime) -> str:
    if when is None:
        return ""
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    minutes = int((now - when).total_seconds() // 60)
    if minutes < 1:
        return "just now"
    if minutes < 60:
        return f"{minutes} min ago"
    hours = minutes // 60
    if hours < 48:
        return f"{hours} h ago"
    return f"{hours // 24} days ago"


def render_call_list_page(
    *,
    user: dict,
    trackers: Iterable[TrackerSummary],
    csrf: str,
    show_archived: bool,
    ai_enabled: bool,
    error: str = "",
    now: Optional[datetime] = None,
) -> str:
    now = now or datetime.now(timezone.utc)
    trackers = list(trackers)
    show_owner = bool(user.get("is_superadmin"))

    if trackers:
        rows = []
        for t in trackers:
            stage_mod = _STAGE_STATUS.get(t.stage, "queued")
            archived = t.status == STATUS_ARCHIVED
            toggle_to = "open" if archived else "archived"
            toggle_label = "Restore" if archived else "Archive"
            owner = f'<td>{_esc(t.owner_email)}</td>' if show_owner else ""
            rows.append(
                f"""<tr>
              <td><a href="{BASE_PATH}/{_esc(t.id)}"><strong>{_esc(t.label)}</strong></a>
                <div class="app-table__sub">{_esc(t.company if t.company and t.company != t.label else "")}</div></td>
              <td><span class="app-status app-status--{stage_mod}">{_esc(t.stage)}</span></td>
              <td>{t.beliefs_said} of 7</td>
              {owner}
              <td>{_esc(_ago(t.updated_at, now))}</td>
              <td><div class="call-actions">
                <a class="admin-btn" href="{BASE_PATH}/{_esc(t.id)}">{'Open' if archived else 'Resume'}</a>
                <form method="post" action="{BASE_PATH}/{_esc(t.id)}/status">
                  <input type="hidden" name="_csrf_token" value="{_esc(csrf)}">
                  <input type="hidden" name="status" value="{toggle_to}">
                  <button class="admin-btn admin-btn--ghost" type="submit">{toggle_label}</button>
                </form></div></td>
            </tr>"""
            )
        owner_head = "<th>Rep</th>" if show_owner else ""
        workspace = (
            '<div class="app-data-workspace"><table class="app-table call-list">'
            f"<thead><tr><th>Lead</th><th>Stage</th><th>Beliefs said</th>{owner_head}<th>Last saved</th><th></th></tr></thead>"
            f"<tbody>{''.join(rows)}</tbody></table></div>"
        )
    else:
        workspace = (
            '<div class="app-state-panel"><h2>No calls yet</h2>'
            "<p>Start a call below. Each lead keeps one tracker, so you can reopen it "
            "and pick up exactly where the call left off.</p></div>"
        )

    error_html = (
        f'<div class="app-alert app-alert--error" role="alert">{_esc(error)}</div>' if error else ""
    )
    ai_note = (
        ""
        if ai_enabled
        else '<div class="app-alert app-alert--notice">Ask Claude is off because the agent has no Anthropic key. The script and tracker still work.</div>'
    )
    archived_link = (
        f'<a href="{BASE_PATH}">Show open calls</a>'
        if show_archived
        else f'<a href="{BASE_PATH}?archived=1">Show archived calls</a>'
    )

    body = f"""<header class="app-page-header">
      <div>
        <h1>Call Script</h1>
        <p>The Belief Ladder script and close tracker for live sales calls. One tracker per lead, saved as you go, built to close on the first call.</p>
      </div>
    </header>
    {error_html}{ai_note}
    <section class="admin-panel">
      <h2>Start a call</h2>
      <form class="app-form-grid" method="post" action="{BASE_PATH}">
        <input type="hidden" name="_csrf_token" value="{_esc(csrf)}">
        <label class="app-field"><span>Prospect name</span>
          <input name="prospect_name" maxlength="255" autocomplete="off" placeholder="Jordan Lee"></label>
        <label class="app-field"><span>Company</span>
          <input name="company" maxlength="255" autocomplete="off" placeholder="Brand name"></label>
        <label class="app-field"><span>Lead link (optional)</span>
          <input name="lead_link" type="url" maxlength="1024" placeholder="https://app.clickup.com/t/..."></label>
        <div class="app-form-grid__actions">
          <button class="admin-btn" type="submit">Start call</button>
          <span class="app-muted">Name or company is required.</span>
        </div>
      </form>
    </section>
    <div class="app-command-bar"><span class="app-muted">{len(trackers)} {'archived' if show_archived else 'open'} call{'s' if len(trackers) != 1 else ''}</span> {archived_link}</div>
    {workspace}"""

    return render_operator_document(
        title="Call Script · Sales · Anata Agent",
        navigation=render_agent_nav("sales", sales_section="sales_call", user=user),
        body=body,
        page_class="sales-call-page",
        extra_head=_FAVICON + _STYLES,
    )
