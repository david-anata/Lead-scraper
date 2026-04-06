"""QuickBooks Online settings & connection status page."""

from __future__ import annotations

import html
from datetime import datetime, timezone, timedelta

from sales_support_agent.services.cashflow.cashflow_helpers import _page_shell


def render_qbo_settings_page(*, flash: str = "") -> str:
    """Render the QB connection status page."""
    today = datetime.now(timezone.utc)

    # Load token state
    connected    = False
    realm_id     = ""
    expires_at   = None
    expiry_label = ""
    expiry_cls   = ""
    warning_msg  = ""

    try:
        from sales_support_agent.api.qbo_auth_router import _load_tokens
        token_row = _load_tokens()
        if token_row and token_row.get("access_token") and token_row.get("realm_id"):
            connected  = True
            realm_id   = token_row.get("realm_id", "")
            exp_str    = token_row.get("expires_at", "")
            if exp_str:
                try:
                    exp = datetime.fromisoformat(exp_str)
                    if exp.tzinfo is None:
                        exp = exp.replace(tzinfo=timezone.utc)
                    remaining = exp - today
                    if remaining.total_seconds() < 0:
                        expiry_label = "Expired"
                        expiry_cls   = "negative"
                        warning_msg  = "Access token has expired. Re-connect to restore sync."
                    elif remaining < timedelta(hours=1):
                        mins = int(remaining.total_seconds() // 60)
                        expiry_label = f"Expires in {mins} min"
                        expiry_cls   = "amount-out"
                    elif remaining < timedelta(hours=24):
                        hrs = int(remaining.total_seconds() // 3600)
                        expiry_label = f"Expires in {hrs}h"
                        expiry_cls   = "amount-out"
                        warning_msg  = "Access token expires soon. Re-connect or it will auto-refresh on next sync."
                    else:
                        days = remaining.days
                        expiry_label = f"Valid · {days}d remaining"
                        expiry_cls   = "positive"
                except (ValueError, TypeError):
                    expiry_label = "Unknown"
    except Exception:
        pass

    # -- Status card -----------------------------------------------------------
    if connected:
        status_badge = '<span class="badge badge-ok" style="font-size:13px;padding:4px 12px">● Connected</span>'
        realm_html   = f'<div class="metric-note">Company ID (realm): <code>{html.escape(realm_id)}</code></div>'
        expiry_html  = f'<div class="metric-note" style="margin-top:4px"><span class="{expiry_cls}">{html.escape(expiry_label)}</span></div>'
        action_html  = f"""
        <div class="action-row" style="margin-top:20px">
          <form method="post" action="/admin/finances/qbo/disconnect"
                onsubmit="return confirm('Disconnect QuickBooks? Synced invoice data will remain but new syncs will stop.')">
            <button type="submit" class="btn btn-secondary"
                    style="border-color:#dc2626;color:#dc2626">Disconnect QuickBooks</button>
          </form>
          <a href="/admin/finances/qbo/connect" class="btn btn-secondary">Re-authenticate</a>
          <form method="post" action="/admin/finances/sync-qbo" style="display:inline">
            <button type="submit" class="btn btn-primary">Sync Invoices Now</button>
          </form>
        </div>"""
    else:
        status_badge = '<span class="badge badge-critical" style="font-size:13px;padding:4px 12px">○ Not Connected</span>'
        realm_html   = '<div class="metric-note">No QuickBooks account linked.</div>'
        expiry_html  = ""
        action_html  = f"""
        <div class="action-row" style="margin-top:20px">
          <a href="/admin/finances/qbo/connect" class="btn btn-primary">Connect QuickBooks</a>
        </div>"""

    warning_html = (
        f'<div class="alert alert-warn" style="margin-top:12px;padding:10px 14px;'
        f'background:#fef3c7;border-left:3px solid #d97706;border-radius:6px;font-size:14px">'
        f'⚠ {html.escape(warning_msg)}</div>'
        if warning_msg else ""
    )

    # -- Setup instructions -------------------------------------------------------
    setup_html = "" if connected else """
    <div class="card" style="margin-top:0">
      <h2>How to Connect</h2>
      <ol style="line-height:2;color:var(--text);padding-left:1.2rem">
        <li>
          Set these environment variables on your server (or Render dashboard):
          <pre style="background:#1e293b;color:#e2e8f0;padding:12px;border-radius:6px;margin:8px 0;font-size:13px">QB_CLIENT_ID=your-intuit-client-id
QB_CLIENT_SECRET=your-intuit-client-secret
QB_REDIRECT_URI=https://agent.anatainc.com/admin/finances/qbo/callback
QB_TOKEN_SECRET=&lt;random 32+ char string&gt;</pre>
        </li>
        <li>In your <a href="https://developer.intuit.com" target="_blank">Intuit developer portal</a>,
          add <code>https://agent.anatainc.com/admin/finances/qbo/callback</code>
          as a redirect URI for your app.</li>
        <li>Click <strong>Connect QuickBooks</strong> above — you'll be redirected to Intuit
          to authorise the connection.</li>
        <li>Once connected, click <strong>Sync Invoices Now</strong> to pull open invoices
          into the forecast immediately.</li>
      </ol>
    </div>"""

    # -- What gets synced ---------------------------------------------------------
    sync_info_html = """
    <div class="card" style="margin-top:0">
      <h2>What Gets Synced</h2>
      <table>
        <thead><tr><th>QBO Data</th><th>→ Finance OS</th><th>Notes</th></tr></thead>
        <tbody>
          <tr>
            <td>Open invoices</td>
            <td>AR (Receivables)</td>
            <td>Outstanding balance per invoice, one row each</td>
          </tr>
          <tr>
            <td>Paid invoices</td>
            <td>Status → <em>paid</em></td>
            <td>Removed from forecast automatically</td>
          </tr>
          <tr>
            <td>Void / deleted</td>
            <td>Status → <em>cancelled</em></td>
            <td>Removed from forecast automatically</td>
          </tr>
        </tbody>
      </table>
      <p class="page-sub" style="margin-top:12px">
        After a bank CSV upload, each QBO invoice is auto-matched against the
        corresponding bank deposit — closing the loop between your invoices and
        your actual cash receipts.
      </p>
    </div>"""

    body = f"""
    <div>
      <p class="eyebrow" style="margin:0 0 10px;text-transform:uppercase;letter-spacing:.18em;font-size:12px;font-weight:800;color:var(--accent);font-family:'Montserrat',sans-serif;">Finance</p>
      <h1>QuickBooks Online</h1>
      <p class="page-sub" style="margin-top:8px">Connect QBO to pull live invoice data into your cash forecast.</p>
    </div>

    <div class="card">
      <h2>Connection Status</h2>
      <div style="margin-top:12px">
        {status_badge}
        {realm_html}
        {expiry_html}
        {warning_html}
        {action_html}
      </div>
    </div>

    {setup_html}
    {sync_info_html}
    """

    return _page_shell("QuickBooks Settings", "qbo", body, flash=flash)
