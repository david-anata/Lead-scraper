"""HTML renderer for Advertising > Clients — a repository of advertising
clients, each an accordion you expand to edit its objectives + goals and review
its run history. Reuses the audit page's shell, CSS tokens, goal fields, and
history table so the two pages stay visually identical."""

from __future__ import annotations

from typing import Optional

from sales_support_agent.services.advertising.audit_page import (
    _esc,
    _goals_fields,
    _history_table,
    _page,
)
from sales_support_agent.services.advertising.schema import Goals


def _add_client_form() -> str:
    return f"""
    <form class="grid" method="post" action="/admin/advertising/clients/new">
      <div class="row">
        <div class="field"><label>Client name</label><input type="text" name="name" placeholder="e.g. Zantrex" required></div>
      </div>
      <div class="field"><label>Objectives <span class="hint">— free-text strategy notes (optional)</span></label>
        <textarea name="objectives" rows="2" placeholder="What success looks like for this client — growth vs. profit, key brands, constraints…"
          style="padding:9px 11px;border:1px solid var(--line);border-radius:10px;font-size:14px;font-family:inherit;background:var(--white);width:100%;resize:vertical;"></textarea></div>
      <div><button class="btn" type="submit">+ Add client</button></div>
    </form>"""


def _client_block(client: dict) -> str:
    cid = client.get("id")
    name = client.get("name") or "Untitled client"
    objectives = client.get("objectives") or ""
    goals: Optional[Goals] = client.get("goals")
    runs = client.get("runs") or []
    run_count = len([r for r in runs if (r.get("status") == "complete")]) or len(runs)
    summary = f'{run_count} run{"s" if run_count != 1 else ""}' if runs else "no runs yet"
    return f"""
    <details class="client-acc">
      <summary><span class="client-name">{_esc(name)}</span> <span class="empty">· {_esc(summary)}</span></summary>
      <div class="client-body">
        <form class="grid" method="post" action="/admin/advertising/clients/{_esc(cid)}">
          <div class="row">
            <div class="field"><label>Client name</label><input type="text" name="name" value="{_esc(name)}" required></div>
          </div>
          <div class="field"><label>Objectives</label>
            <textarea name="objectives" rows="3"
              style="padding:9px 11px;border:1px solid var(--line);border-radius:10px;font-size:14px;font-family:inherit;background:var(--white);width:100%;resize:vertical;">{_esc(objectives)}</textarea></div>
          <div class="card" style="margin:0;background:#fafbfc;">
            <h2 style="font-size:15px;">Goals <small>— targets every audit for this client measures against</small></h2>
            {_goals_fields(goals)}
          </div>
          <div style="display:flex;align-items:center;gap:10px">
            <button class="btn" type="submit">Save client</button>
          </div>
        </form>
        <form method="post" action="/admin/advertising/clients/{_esc(cid)}/archive"
              onsubmit="return confirm('Archive {_esc(name)}? It disappears from this list — run history is kept.')">
          <button class="btn btn--ghost" type="submit"
                  style="font-size:13px;color:#b94040;border-color:rgba(185,64,64,0.4);margin-top:8px">
            Archive client
          </button>
        </form>
        <div style="margin-top:18px;">
          <h2 style="font-family:'Montserrat',sans-serif;font-size:15px;margin:0 0 10px;">Run history</h2>
          {_history_table(runs)}
        </div>
      </div>
    </details>"""


def render_clients_page(
    clients: list[dict],
    *,
    user: Optional[dict] = None,
    flash: str = "",
) -> str:
    flash_html = f'<div class="flash">{_esc(flash)}</div>' if flash else ""
    if clients:
        blocks = "".join(_client_block(c) for c in clients)
    else:
        blocks = '<p class="empty">No clients yet — add your first one above.</p>'

    body = f"""
      <section class="page-header">
        <span class="eyebrow">Advertising</span>
        <h1 class="page-title">Clients<span class="highlight">.</span></h1>
        <p class="page-copy">Every advertising client and its goals in one place. Expand a client to update its
        objectives + targets and see its audit history. Each client's goals drive the burn list when you run an
        audit for it — no more re-keying the same targets every time.</p>
      </section>
      {flash_html}
      <div class="card"><h2>Add a client</h2>{_add_client_form()}</div>
      <div class="card"><h2>Clients <small>· expand to edit goals &amp; view history</small></h2>{blocks}</div>
      <style>
        details.client-acc {{ border: 1px solid var(--line); border-radius: 14px; margin-bottom: 12px; background: var(--white); }}
        details.client-acc > summary {{ cursor: pointer; font-weight: 700; font-family: "Montserrat",sans-serif; font-size: 16px; padding: 14px 18px; list-style: none; }}
        details.client-acc > summary::-webkit-details-marker {{ display: none; }}
        details.client-acc[open] > summary {{ border-bottom: 1px solid var(--line); }}
        details.client-acc .client-name {{ color: var(--dark-blue); }}
        details.client-acc .client-body {{ padding: 18px; }}
      </style>
    """
    return _page("agent | Advertising Clients", body, user=user)
