"""Admin call log pages (operator shell + design-system classes only)."""

from __future__ import annotations

import html
from typing import Iterable, Optional

from sales_support_agent.models.voice import VoiceCall
from sales_support_agent.services.admin_nav import render_agent_nav
from sales_support_agent.services.ui_shell import render_operator_document

BASE_PATH = "/admin/sales/voice-calls"


def _esc(value: object) -> str:
    return html.escape(str(value or ""))


def _when(value) -> str:
    return value.strftime("%Y-%m-%d %H:%M UTC") if value else "—"


def render_call_list(calls: Iterable[VoiceCall], *, user: Optional[dict], enabled: bool, missing: list[str]) -> str:
    rows = "".join(
        f"<tr><td><a href=\"{BASE_PATH}/{_esc(c.id)}\">{_when(c.started_at)}</a></td>"
        f"<td>{_esc(c.caller_name or c.from_number)}</td><td>{_esc(c.outcome or c.status)}</td>"
        f"<td>{c.duration_seconds or 0}s</td><td>{_esc((c.summary or '')[:140])}</td></tr>"
        for c in calls
    )
    if not rows:
        rows = '<tr><td colspan="5" class="app-muted">No calls yet.</td></tr>'
    status = "On" if enabled else "Off (VOICE_AGENT_ENABLED is not set)"
    missing_note = f"<p class=\"app-muted\">Missing configuration: {_esc(', '.join(missing))}</p>" if missing else ""
    body = f"""
    <section class="admin-panel">
      <h2>AI receptionist</h2>
      <p>Status: <strong>{_esc(status)}</strong></p>{missing_note}
    </section>
    <section class="admin-panel">
      <table class="app-table">
        <thead><tr><th>Started</th><th>Caller</th><th>Outcome</th><th>Length</th><th>Summary</th></tr></thead>
        <tbody>{rows}</tbody>
      </table>
    </section>"""
    return render_operator_document(
        title="Voice calls · Sales · Anata Agent",
        navigation=render_agent_nav("sales", sales_section="voice_calls", user=user),
        body=body,
        page_class="voice-calls-page",
    )


def render_call_detail(call: VoiceCall, *, user: Optional[dict]) -> str:
    lines = "".join(
        f"<tr><td>{'Caller' if t.get('speaker') == 'caller' else 'Agent'}</td><td>{_esc(t.get('text'))}</td></tr>"
        for t in (call.transcript_json or [])
    ) or '<tr><td colspan="2" class="app-muted">No transcript.</td></tr>'
    tool_rows = "".join(
        f"<tr><td>{_esc(t.get('name'))}</td><td><code>{_esc(t.get('result'))[:300]}</code></td></tr>"
        for t in (call.tool_calls_json or [])
    ) or '<tr><td colspan="2" class="app-muted">No tool calls.</td></tr>'
    body = f"""
    <p><a href="{BASE_PATH}">All calls</a></p>
    <section class="admin-panel">
      <h2>{_esc(call.caller_name or call.from_number or 'Unknown caller')}</h2>
      <p>{_when(call.started_at)} · {call.duration_seconds or 0}s · {_esc(call.outcome or call.status)}</p>
      <p>Disclosure played: {_when(call.disclosure_played_at)}</p>
      <p class="app-muted">“{_esc(call.disclosure_text)}”</p>
      <p>{_esc(call.summary)}</p>
      <p class="app-muted">Summary email: {_esc(call.summary_email_status or '—')}</p>
    </section>
    <section class="admin-panel"><h2>Transcript</h2>
      <table class="app-table"><tbody>{lines}</tbody></table></section>
    <section class="admin-panel"><h2>Tool calls</h2>
      <table class="app-table"><tbody>{tool_rows}</tbody></table></section>"""
    return render_operator_document(
        title="Voice call · Sales · Anata Agent",
        navigation=render_agent_nav("sales", sales_section="voice_calls", user=user),
        body=body,
        page_class="voice-calls-page",
    )
