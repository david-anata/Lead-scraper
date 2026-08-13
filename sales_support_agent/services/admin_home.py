"""Human-facing service status and authenticated Agent workspace landing pages."""

from __future__ import annotations

import html
from collections.abc import Iterable

from sales_support_agent.services.access.catalog import grants_tool
from sales_support_agent.services.admin_nav import (
    render_agent_favicon_links,
    render_agent_nav,
    render_agent_stylesheet_links,
)


_WORKSPACES = (
    ("sales.deals", "Sales", "Move opportunities forward and keep follow-up work visible.", "/admin/sales"),
    ("website_ops.seo", "Website Ops", "Prioritize content, indexing, and site-health actions.", "/admin/website-ops"),
    ("content.view", "Content", "Plan, produce, and distribute the content engine.", "/admin/content"),
    ("finance", "Finance", "Review cash, obligations, evidence, and next actions.", "/admin/finances"),
    ("building.manage", "Building", "Manage active builds, agreements, and delivery readiness.", "/admin/building"),
    ("advertising.audit", "Advertising", "Audit accounts and resolve performance issues.", "/admin/advertising/audit"),
    ("executive.summary", "Executive", "See the cross-functional operating summary.", "/admin/executive"),
    ("fulfillment.rate_sheets", "Fulfillment", "Manage sales handoffs and customer-success work.", "/admin/fulfillment/sales"),
    ("hr.access", "HR", "Manage people, onboarding, time, and employee records.", "/admin/hr"),
)


def _document(*, title: str, body: str, description: str, nav: str = "") -> str:
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="description" content="{html.escape(description, quote=True)}">
  <title>{html.escape(title)}</title>
  {render_agent_favicon_links()}
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Montserrat:wght@700;800&display=swap" rel="stylesheet">
  {render_agent_stylesheet_links()}
  <style>
    .agent-home {{ padding-block: 42px 72px; }}
    .agent-home__header {{ max-width: 760px; margin-bottom: 28px; }}
    .agent-home__eyebrow {{ color: var(--agent-blue-strong); font: 800 .75rem/1.2 "Montserrat", sans-serif; letter-spacing: .08em; text-transform: uppercase; }}
    .agent-home h1 {{ margin: 8px 0 10px; color: var(--agent-ink); font: 800 clamp(2rem, 4vw, 3.25rem)/1.04 "Montserrat", sans-serif; letter-spacing: -.045em; }}
    .agent-home__intro {{ margin: 0; color: var(--agent-ink-muted); font-size: 1.05rem; line-height: 1.6; }}
    .workspace-grid {{ display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 16px; }}
    .workspace-card {{ min-width: 0; min-height: 190px; display: flex; flex-direction: column; padding: 22px; border: 1px solid var(--agent-border); border-radius: var(--agent-radius-card); background: var(--agent-surface); color: var(--agent-ink); text-decoration: none; box-shadow: 0 10px 28px var(--agent-shadow); transition: border-color 160ms ease, transform 160ms ease, box-shadow 160ms ease; }}
    .workspace-card:hover {{ border-color: var(--agent-blue); transform: translateY(-2px); box-shadow: 0 14px 34px rgba(43,54,68,.14); }}
    .workspace-card h2 {{ margin: 0 0 9px; font: 800 1.15rem/1.25 "Montserrat", sans-serif; }}
    .workspace-card p {{ margin: 0; color: var(--agent-ink-muted); line-height: 1.5; }}
    .workspace-card span {{ margin-top: auto; padding-top: 22px; color: var(--agent-blue-strong); font: 800 .78rem/1.2 "Montserrat", sans-serif; }}
    .status-shell {{ min-height: 100vh; display: grid; place-items: center; padding-block: 48px; }}
    .status-panel {{ width: min(720px, 100%); padding: clamp(28px, 6vw, 52px); border: 1px solid var(--agent-border); border-radius: var(--agent-radius-card); background: var(--agent-surface); box-shadow: 0 18px 50px var(--agent-shadow); }}
    .status-mark {{ width: 52px; height: 52px; display: grid; place-items: center; border-radius: 50%; background: var(--agent-blue-soft); color: var(--agent-good); font: 800 1.4rem/1 "Montserrat", sans-serif; }}
    .status-panel h1 {{ margin: 22px 0 8px; font: 800 clamp(2rem, 5vw, 3rem)/1.06 "Montserrat", sans-serif; letter-spacing: -.04em; }}
    .status-panel > p {{ color: var(--agent-ink-muted); line-height: 1.6; }}
    .status-list {{ display: grid; gap: 10px; margin: 28px 0; }}
    .status-row {{ display: flex; justify-content: space-between; gap: 20px; padding: 14px 0; border-bottom: 1px solid var(--agent-border); }}
    .status-row strong {{ font: 700 .85rem/1.3 "Montserrat", sans-serif; }}
    .status-actions {{ display: flex; flex-wrap: wrap; gap: 10px; }}
    .status-actions a {{ min-height: var(--agent-control-height); display: inline-flex; align-items: center; padding: 0 16px; border-radius: var(--agent-radius-control); font: 700 .82rem/1 "Montserrat", sans-serif; text-decoration: none; }}
    .status-actions__primary {{ background: var(--agent-ink); color: white; }}
    .status-actions__secondary {{ border: 1px solid var(--agent-border); color: var(--agent-ink); }}
    @media (max-width: 900px) {{ .workspace-grid {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }} }}
    @media (max-width: 620px) {{ .workspace-grid {{ grid-template-columns: 1fr; }} .agent-home {{ padding-block: 28px 48px; }} }}
    @media (prefers-reduced-motion: reduce) {{ .workspace-card {{ transition: none; }} .workspace-card:hover {{ transform: none; }} }}
  </style>
</head>
<body class="app">{nav}{body}</body>
</html>"""


def render_service_status_page(*, ready: bool) -> str:
    state = "Operational" if ready else "Starting"
    detail = "Agent is online and ready for authenticated work." if ready else "Agent is online and completing startup checks."
    mark = "✓" if ready else "…"
    body = f"""
    <main id="agent-main-content" class="app-container status-shell">
      <section class="status-panel" aria-labelledby="status-title">
        <div class="status-mark" aria-hidden="true">{mark}</div>
        <h1 id="status-title">Agent is {state.lower()}.</h1>
        <p>{html.escape(detail)}</p>
        <div class="status-list" aria-label="Service status">
          <div class="status-row"><span>Application</span><strong>{state}</strong></div>
          <div class="status-row"><span>Secure access</span><strong>Available</strong></div>
          <div class="status-row"><span>Environment</span><strong>Staging</strong></div>
        </div>
        <div class="status-actions">
          <a class="status-actions__primary" href="/admin">Open Agent</a>
          <a class="status-actions__secondary" href="/health/ready">Technical status</a>
        </div>
      </section>
    </main>"""
    return _document(title="Agent status", description="Current Anata Agent service status.", body=body)


def render_admin_home_page(*, user: dict | None = None) -> str:
    user = user or {}
    permissions: Iterable[str] = user.get("permissions") or ()
    is_superadmin = bool(user.get("is_superadmin"))
    cards = []
    for tool_key, title, description, href in _WORKSPACES:
        if not is_superadmin and not grants_tool(set(permissions), tool_key):
            continue
        cards.append(
            f'<a class="workspace-card" href="{html.escape(href, quote=True)}">'
            f'<h2>{html.escape(title)}</h2><p>{html.escape(description)}</p>'
            '<span>Open workspace →</span></a>'
        )
    if not cards:
        cards.append('<section class="app-state-panel"><h2>No workspaces assigned</h2><p>Ask an Agent administrator to update your access.</p></section>')
    name = (user.get("name") or "").strip().split(" ", 1)[0]
    greeting = f"Welcome back, {html.escape(name)}." if name else "Welcome to Agent."
    nav = render_agent_nav(user=user, include_content_target=False)
    body = f"""
    <main id="agent-main-content" class="app-container agent-home">
      <header class="agent-home__header">
        <div class="agent-home__eyebrow">Anata operating system</div>
        <h1>{greeting}</h1>
        <p class="agent-home__intro">Choose a workspace to review priorities, resolve issues, and move the business forward.</p>
      </header>
      <section class="workspace-grid" aria-label="Your Agent workspaces">{''.join(cards)}</section>
    </main>"""
    return _document(title="Agent workspace", description="Anata Agent workspace launcher.", nav=nav, body=body)
