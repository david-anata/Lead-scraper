"""Settings page renderer — /admin/settings.

Displays five sections visible to superadmins / access.manage holders:
  1. Your Account   — profile info from the session
  2. Team           — user/invite/request counts + quick links
  3. Amazon         — SP-API config values (read-only, secrets masked)
  4. Notifications  — Slack config (read-only)
  5. Appearance     — branding placeholder
"""

from __future__ import annotations

import html
from typing import Optional

from sales_support_agent.services.admin_nav import (
    render_agent_favicon_links,
    render_agent_nav,
    render_agent_nav_styles,
)


def _esc(v: object) -> str:
    return html.escape(str(v) if v is not None else "")


def _masked(v: str) -> str:
    """Show first 6 chars then *** for secrets."""
    if not v:
        return '<span style="color:rgba(43,54,68,0.38);">Not configured</span>'
    return html.escape(v[:6]) + "••••••••"


def _connected(v: str) -> str:
    if v:
        return '<span style="color:#2e7d5b;font-weight:700;">&#10003; Connected</span>'
    return '<span style="color:#8b4c42;font-weight:700;">&#10007; Not configured</span>'


_STYLES = """
  :root { --dark-blue:#2B3644; --light-blue:#85BBDA; --light-brown:#F9F7F3; --white:#fff;
    --text:#2B3644; --border:rgba(43,54,68,0.10); --shadow:rgba(43,54,68,0.10); }
  * { box-sizing:border-box; }
  body { margin:0; background:var(--light-brown); color:var(--text);
    font-family:"Inter","Segoe UI",sans-serif; }
  __NAV__
  .settings-shell { max-width:1040px; margin:0 auto; padding:40px 20px 80px; }
  .settings-title { font-family:"Montserrat",sans-serif; font-size:22px; font-weight:800;
    color:#2B3644; margin:0 0 28px; }
  .settings-grid { display:grid; grid-template-columns:1fr 1fr; gap:20px; }
  .settings-grid-wide { grid-column:1/-1; }
  .settings-card { background:#fff; border:1px solid var(--border); border-radius:20px;
    box-shadow:0 6px 24px var(--shadow); padding:28px 30px; }
  .card-section-label { font-family:"Montserrat",sans-serif; font-weight:800;
    font-size:11px; text-transform:uppercase; letter-spacing:0.08em;
    color:rgba(43,54,68,0.42); margin-bottom:18px; }
  .card-title { font-family:"Montserrat",sans-serif; font-weight:800;
    font-size:17px; color:#2B3644; margin:0 0 18px; display:flex;
    align-items:center; gap:9px; }
  .card-title .card-icon { font-size:18px; }
  /* Account card */
  .account-row { display:flex; align-items:center; gap:18px; }
  .account-avatar { width:52px; height:52px; border-radius:50%; background:#85BBDA;
    display:flex; align-items:center; justify-content:center; font-family:"Montserrat",sans-serif;
    font-weight:900; font-size:18px; color:#fff; flex-shrink:0; }
  .account-name { font-family:"Montserrat",sans-serif; font-weight:800; font-size:16px;
    color:#2B3644; }
  .account-email { font-size:13px; color:rgba(43,54,68,0.6); margin:2px 0; }
  .account-role-badge { display:inline-flex; align-items:center; padding:3px 10px;
    border-radius:999px; font-size:10px; font-weight:700; font-family:"Montserrat",sans-serif;
    letter-spacing:0.05em; background:rgba(133,187,218,0.18); color:#1a5f84; margin-top:4px; }
  .account-last-login { font-size:12px; color:rgba(43,54,68,0.40); margin-top:12px; }
  /* Team stats */
  .stat-row { display:flex; gap:14px; flex-wrap:wrap; margin-bottom:20px; }
  .stat-chip { flex:1; min-width:80px; background:rgba(249,247,243,0.8);
    border:1px solid rgba(43,54,68,0.08); border-radius:14px; padding:14px 16px;
    text-align:center; }
  .stat-chip-num { font-family:"Montserrat",sans-serif; font-weight:900;
    font-size:24px; color:#2B3644; line-height:1; }
  .stat-chip-label { font-size:11px; color:rgba(43,54,68,0.50); margin-top:4px; }
  .stat-chip.attention .stat-chip-num { color:#c2663b; }
  .quick-links { display:flex; gap:8px; flex-wrap:wrap; }
  .quick-link { display:inline-flex; align-items:center; gap:6px; padding:0 14px;
    min-height:34px; border-radius:999px; font-family:"Montserrat",sans-serif;
    font-weight:700; font-size:12px; background:rgba(43,54,68,0.06);
    color:#2B3644; text-decoration:none; border:1px solid rgba(43,54,68,0.10);
    transition:background 100ms; }
  .quick-link:hover { background:rgba(43,54,68,0.11); }
  /* Config rows */
  .config-table { width:100%; border-collapse:collapse; margin-top:4px; }
  .config-table td { padding:9px 0; border-bottom:1px solid rgba(43,54,68,0.06);
    font-size:13.5px; vertical-align:top; }
  .config-table tr:last-child td { border-bottom:none; }
  .config-table td:first-child { color:rgba(43,54,68,0.58); width:52%; }
  .config-table td:last-child { font-weight:600; color:#2B3644; text-align:right; }
  /* Placeholder */
  .placeholder-note { color:rgba(43,54,68,0.42); font-size:14px;
    font-style:italic; padding:8px 0; }
  @media (max-width:700px) { .settings-grid { grid-template-columns:1fr; } }
"""


def render_settings_page(
    user: dict,
    *,
    team_counts: Optional[dict] = None,
    agent_settings=None,
) -> str:
    name = _esc(user.get("name") or user.get("email") or "User")
    email = _esc(user.get("email") or "")
    role_raw = user.get("role") or ""
    is_superadmin = bool(user.get("is_superadmin"))
    role_label = _esc(role_raw or ("Super-admin" if is_superadmin else "Member"))
    initials = "".join(p[0].upper() for p in (user.get("name") or email).split()[:2]) or "?"
    last_login = user.get("last_login_at") or ""
    last_login_str = f'<div class="account-last-login">Last login: {_esc(last_login[:16].replace("T", " "))}</div>' if last_login else ""

    # ── 1. Account card ────────────────────────────────────────────────────
    account_card = f"""
    <div class="settings-card">
      <div class="card-title"><span class="card-icon">&#128100;</span> Your Account</div>
      <div class="account-row">
        <div class="account-avatar">{initials}</div>
        <div>
          <div class="account-name">{name}</div>
          <div class="account-email">{email}</div>
          <div class="account-role-badge">{role_label}</div>
        </div>
      </div>
      {last_login_str}
    </div>"""

    # ── 2. Team card ───────────────────────────────────────────────────────
    tc = team_counts or {}
    active_users = tc.get("active_users", 0)
    total_users = tc.get("total_users", 0)
    pending_invites = tc.get("pending_invites", 0)
    pending_requests = tc.get("pending_requests", 0)
    invite_cls = " attention" if pending_invites > 0 else ""
    request_cls = " attention" if pending_requests > 0 else ""
    team_card = f"""
    <div class="settings-card">
      <div class="card-title"><span class="card-icon">&#128101;</span> Team</div>
      <div class="stat-row">
        <div class="stat-chip">
          <div class="stat-chip-num">{active_users}</div>
          <div class="stat-chip-label">Active users</div>
        </div>
        <div class="stat-chip">
          <div class="stat-chip-num">{total_users}</div>
          <div class="stat-chip-label">Total members</div>
        </div>
        <div class="stat-chip{invite_cls}">
          <div class="stat-chip-num">{pending_invites}</div>
          <div class="stat-chip-label">Pending invites</div>
        </div>
        <div class="stat-chip{request_cls}">
          <div class="stat-chip-num">{pending_requests}</div>
          <div class="stat-chip-label">Access requests</div>
        </div>
      </div>
      <div class="quick-links">
        <a class="quick-link" href="/admin/access">&#128101; Manage users</a>
        <a class="quick-link" href="/admin/access/roles">&#127890; Manage roles</a>
        <a class="quick-link" href="/admin/access/invites">&#9993; Send invite</a>
        {'<a class="quick-link" href="/admin/access/requests" style="color:#c2663b;border-color:rgba(194,102,59,0.25);">&#9888; Review requests</a>' if pending_requests > 0 else '<a class="quick-link" href="/admin/access/requests">&#9888; Requests</a>'}
      </div>
    </div>"""

    # ── 3. Amazon config card ──────────────────────────────────────────────
    s = agent_settings
    marketplace_id = _esc(getattr(s, "amazon_sp_api_marketplace_id", "") or "")
    region = _esc(getattr(s, "amazon_sp_api_region", "") or "")
    base_url = _esc(getattr(s, "amazon_sp_api_base_url", "") or "")
    lwa_id = getattr(s, "amazon_sp_api_lwa_client_id", "") or ""
    refresh = getattr(s, "amazon_sp_api_refresh_token", "") or ""
    aws_key = getattr(s, "amazon_sp_api_aws_access_key_id", "") or ""

    # Human-readable marketplace
    _mkt_labels = {
        "ATVPDKIKX0DER": "US (ATVPDKIKX0DER)",
        "A2EUQ1WTGCTBG2": "CA (A2EUQ1WTGCTBG2)",
        "A1AM78C64UM0Y8": "MX (A1AM78C64UM0Y8)",
    }
    mkt_display = _esc(_mkt_labels.get(getattr(s, "amazon_sp_api_marketplace_id", ""), marketplace_id))

    amazon_card = f"""
    <div class="settings-card settings-grid-wide">
      <div class="card-title"><span class="card-icon">&#128230;</span> Amazon Integration</div>
      <table class="config-table">
        <tr><td>SP-API endpoint</td><td>{base_url or "—"}</td></tr>
        <tr><td>Marketplace</td><td>{mkt_display or "—"}</td></tr>
        <tr><td>Region</td><td>{region or "—"}</td></tr>
        <tr><td>LWA client ID</td><td>{_masked(lwa_id)}</td></tr>
        <tr><td>Refresh token</td><td>{_connected(refresh)}</td></tr>
        <tr><td>AWS access key</td><td>{_masked(aws_key)}</td></tr>
      </table>
      <p style="font-size:12px;color:rgba(43,54,68,0.38);margin:14px 0 0;">
        Configuration is managed via Render environment variables.
        Contact your administrator to update credentials.
      </p>
    </div>"""

    # ── 4. Notifications card ──────────────────────────────────────────────
    slack_token = getattr(s, "slack_bot_token", "") or ""
    slack_channel = _esc(getattr(s, "slack_channel_id", "") or "")
    digest_enabled = getattr(s, "stale_lead_slack_digest_enabled", False)
    digest_max = getattr(s, "stale_lead_slack_digest_max_items", 20)

    def _bool_badge(v: bool) -> str:
        return ('<span style="color:#2e7d5b;font-weight:700;">Enabled</span>' if v
                else '<span style="color:rgba(43,54,68,0.40);">Disabled</span>')

    notifications_card = f"""
    <div class="settings-card">
      <div class="card-title"><span class="card-icon">&#128276;</span> Notifications</div>
      <table class="config-table">
        <tr><td>Slack bot</td><td>{_connected(slack_token)}</td></tr>
        <tr><td>Slack channel</td><td>{slack_channel or "—"}</td></tr>
        <tr><td>Weekly stale-lead digest</td><td>{_bool_badge(digest_enabled)}</td></tr>
        <tr><td>Digest max items</td><td>{digest_max}</td></tr>
      </table>
      <p style="font-size:12px;color:rgba(43,54,68,0.38);margin:14px 0 0;">
        Notification preferences are set via environment variables.
        Per-user alert routing is on the roadmap.
      </p>
    </div>"""

    # ── 5. Appearance card ─────────────────────────────────────────────────
    appearance_card = """
    <div class="settings-card">
      <div class="card-title"><span class="card-icon">&#127912;</span> Appearance</div>
      <p class="placeholder-note">
        Custom branding — logo, color overrides, and company name —
        is coming soon. The current design uses the Anata brand tokens.
      </p>
      <table class="config-table">
        <tr><td>Primary color</td><td>#2B3644 (Navy)</td></tr>
        <tr><td>Accent color</td><td>#85BBDA (Light blue)</td></tr>
        <tr><td>Background</td><td>#F9F7F3 (Warm white)</td></tr>
      </table>
    </div>"""

    nav = render_agent_nav("settings", is_superadmin=is_superadmin,
                           permissions=user.get("permissions"), user=user)
    styles = _STYLES.replace("__NAV__", render_agent_nav_styles())
    favicon = render_agent_favicon_links()

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>agent | Settings</title>
  {favicon}
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@700;800;900&family=Inter:wght@400;500;600&display=swap" rel="stylesheet">
  <style>{styles}</style>
</head>
<body>
  {nav}
  <div class="settings-shell">
    <div class="settings-title">Settings</div>
    <div class="settings-grid">
      {account_card}
      {team_card}
      {amazon_card}
      {notifications_card}
      {appearance_card}
    </div>
  </div>
</body>
</html>"""
