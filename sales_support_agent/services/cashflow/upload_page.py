"""CSV Upload page — file upload form + result display."""

from __future__ import annotations

import html

from sales_support_agent.services.cashflow.overview import _page_shell


def render_upload_page(*, result_html: str = "", flash: str = "") -> str:
    body = f"""
    <h1>Upload bank actuals CSV</h1>
    <p class="page-sub">Fallback import for bank transactions when live finance data is incomplete</p>

    <div class="card">
      <h2>Import transactions</h2>
      <form method="post" action="/admin/finances/upload" enctype="multipart/form-data">
        <div class="form-row">
          <div>
            <label>Bank transactions CSV</label>
            <input type="file" name="csv_file" accept=".csv" style="padding:6px 0">
          </div>
          <div>
            <label>How to merge</label>
            <select name="merge_mode">
              <option value="append">Append / merge by transaction ID</option>
              <option value="replace_range">Replace date range</option>
            </select>
          </div>
        </div>
        <div class="action-row">
          <button type="submit" class="btn btn-primary">Import bank actuals</button>
        </div>
      </form>
    </div>

    <div class="card" style="background:rgba(43,54,68,0.02)">
      <h2>Required CSV columns</h2>
      <p style="font-size:13px;color:#6b7a8d;margin:0">
        Export from your bank as a CSV. Required columns:
        <code>Transaction ID</code>, <code>Date</code>, <code>Description</code>,
        <code>Amount</code>, <code>Balance</code>.
        The uploader auto-detects Debits/Credits columns as well.
      </p>
    </div>

    {result_html}"""

    return _page_shell("Upload CSV", "upload", body, flash=flash)


def render_upload_result(result) -> str:
    """Convert an UploadResult to an HTML card."""
    if result.errors:
        error_list = "".join(
            f"<li>{html.escape(e)}</li>" for e in result.errors[:10]
        )
        error_block = f'<ul style="margin:8px 0 0;padding-left:18px;font-size:13px">{error_list}</ul>'
    else:
        error_block = ""

    status_cls = "flash-success" if result.success else "flash-error"
    icon = "✓" if result.success else "⚠"

    bal_line = ""
    if result.latest_balance_cents is not None:
        bal = result.latest_balance_cents / 100
        bal_line = f" · Balance: ${bal:,.2f}"

    return f"""
    <div class="card finance-inline-result">
      <h2>Upload Result</h2>
      <div class="{status_cls}">
        {icon} {html.escape(result.summary())}{bal_line}
      </div>
      {error_block}
      <div class="action-row" style="margin-top:16px">
        <a href="/admin/finances" class="btn btn-secondary btn-sm">Refresh page</a>
      </div>
    </div>"""
