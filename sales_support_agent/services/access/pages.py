"""HTML pages for access control. Phase 1: the friendly 403 ("no access") page.
Phase 2 extends this module with the users/roles admin UI.
"""

from __future__ import annotations

import html
from typing import Optional

from sales_support_agent.services.admin_nav import (
    render_agent_favicon_links,
    render_agent_nav,
    render_agent_nav_styles,
)


def _esc(text: object) -> str:
    return html.escape(str(text if text is not None else ""))


_BASE_STYLES = """
  :root { --dark-blue:#2B3644; --light-blue:#85BBDA; --light-brown:#F9F7F3; --white:#fff;
    --text:#2B3644; --border:rgba(43,54,68,0.10); --shadow:rgba(43,54,68,0.10); }
  * { box-sizing:border-box; }
  body { margin:0; background:var(--light-brown); color:var(--text); font-family:"Inter","Segoe UI",sans-serif; }
  a { color:var(--dark-blue); }
  __NAV__
  .shell { max-width:760px; margin:0 auto; padding:48px 20px 72px; }
  .card { background:var(--white); border:1px solid var(--border); border-radius:24px;
    box-shadow:0 18px 40px var(--shadow); padding:40px; text-align:center; }
  h1 { font-family:"Montserrat",sans-serif; color:var(--dark-blue); font-size:26px; margin:8px 0; }
  .muted { color:rgba(43,54,68,0.62); font-size:14.5px; line-height:1.6; }
  .lock { font-size:44px; }
  .btn { display:inline-flex; align-items:center; min-height:44px; padding:0 20px; border-radius:999px;
    background:var(--dark-blue); color:#fff; font-family:"Montserrat",sans-serif; font-weight:700;
    font-size:13px; text-decoration:none; margin-top:18px; }
"""


def _shell(title: str, body: str, *, user: Optional[dict]) -> str:
    permissions = (user or {}).get("permissions") or set()
    is_superadmin = bool((user or {}).get("is_superadmin"))
    nav = render_agent_nav("", permissions=permissions, is_superadmin=is_superadmin, user=user)
    return f"""<!doctype html>
<html lang="en"><head>
  <meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
  <title>agent | {_esc(title)}</title>
  {render_agent_favicon_links()}
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Montserrat:wght@700;800&display=swap" rel="stylesheet">
  <style>{_BASE_STYLES.replace("__NAV__", render_agent_nav_styles())}</style>
</head><body>
  {nav}
  <main class="shell">{body}</main>
</body></html>"""


def render_forbidden_page(*, user: Optional[dict], tool_label: str) -> str:
    email = _esc((user or {}).get("email") or "")
    unprovisioned = (user or {}).get("status") == "unprovisioned"
    if unprovisioned:
        sub = (
            "Your account isn't set up with any tools yet. An administrator needs to "
            "assign you a role before you can use the dashboard."
        )
    else:
        sub = f"Your role doesn't include access to <strong>{_esc(tool_label)}</strong>. "\
              "Ask an administrator to grant it."
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
