"""Server-rendered preview and receipt pages for shared Finance batches."""

from __future__ import annotations

import html
from typing import Any, Mapping

from sales_support_agent.services.cashflow.cashflow_helpers import _dollar, _page_shell
from sales_support_agent.services.cashflow.finance_nav import render_finance_nav


def render_workspace_preview(preview: Mapping[str, Any], *, csrf_token: str, idempotency_key: str) -> str:
    rows = "".join(
        f"""<tr class="workspace-preview-row workspace-preview-row--{html.escape(str(item.get('status') or 'invalid'), quote=True)}">
          <td><strong>{html.escape(str(item.get('label') or item.get('object_type') or '').replace('_', ' ').title())}</strong><span>{html.escape(str(item.get('object_type') or '').replace('_', ' ').title())}</span></td>
          <td>{html.escape(str(item.get('action') or '').replace('_', ' ').title())}</td>
          <td>{html.escape(str(item.get('value') if item.get('value') is not None else ''))}</td>
          <td>{_dollar(abs(int(item.get('amount_cents') or 0)))}</td>
          <td><span class="workspace-preview-status">{html.escape(str(item.get('status') or 'invalid').title())}</span>{f'<small>{html.escape(str(item.get("reason") or ""))}</small>' if item.get('reason') else ''}</td>
        </tr>"""
        for item in preview.get("items") or []
    )
    body = f"""
    <div class="money-brief finance-workspace-review">
      {render_finance_nav('review', counts={})}
      <header class="money-page-header"><div><p class="finance-eyebrow">Save review</p>
      <h1>Review every change before saving</h1>
      <p class="money-page-subtitle">Nothing on this page moves money, runs payroll, cancels a vendor, or manufactures payment evidence.</p></div></header>
      <section class="workspace-preview-summary" aria-label="Batch preview summary">
        <article><span>Ready to save</span><strong>{int(preview.get('eligible_count') or 0)}</strong></article>
        <article><span>Protected</span><strong>{int(preview.get('protected_count') or 0)}</strong></article>
        <article><span>Conflicts</span><strong>{int(preview.get('conflict_count') or 0)}</strong></article>
        <article><span>Invalid</span><strong>{int(preview.get('invalid_count') or 0)}</strong></article>
      </section>
      <section class="budget-workspace"><div class="money-section-heading"><div><p class="finance-eyebrow">Exact preview</p><h2>What will change and what will be skipped</h2></div></div>
      <div class="money-table-wrap"><table class="budget-table"><thead><tr><th>Item</th><th>Change</th><th>New value</th><th>Affected value</th><th>Eligibility</th></tr></thead><tbody>{rows}</tbody></table></div></section>
      <form id="finance-workspace-confirm" class="workspace-confirm" method="post"
        action="/admin/finances/workspace/apply"
        data-finance-workspace-confirm
        data-finance-eligible-count="{int(preview.get('eligible_count') or 0)}">
        <input type="hidden" name="_csrf_token" value="{html.escape(csrf_token, quote=True)}">
        <input type="hidden" name="preview_token" value="{html.escape(str(preview.get('preview_token') or ''), quote=True)}">
        <input type="hidden" name="idempotency_key" value="{html.escape(idempotency_key, quote=True)}">
        <label for="workspace-reason">Batch note <span>Optional for these reversible classifications</span></label>
        <textarea id="workspace-reason" name="reason" maxlength="2000" placeholder="Why are you making these changes?"></textarea>
        <div><a class="btn btn-secondary" href="/admin/finances/budget">Keep editing</a><button class="btn btn-primary" type="submit" {'disabled' if not preview.get('eligible_count') else ''}>Save all eligible changes</button></div>
      </form>
    </div>"""
    return _page_shell("Review Finance changes", "review", body)


def render_workspace_receipt(receipt: Mapping[str, Any], *, csrf_token: str) -> str:
    rows = "".join(
        f"""<li><strong>{html.escape(str(item.get('action') or '').replace('_', ' ').title())}</strong>
        <span>{html.escape(str(item.get('object_type') or '').replace('_', ' ').title())} · {html.escape(str(item.get('object_id') or ''))}</span>
        <em>{html.escape(str(item.get('eligibility_result') or ''))}{f' — {html.escape(str(item.get("skip_reason") or ""))}' if item.get('skip_reason') else ''}{f' · {html.escape(str(item.get("new_state_json") or {}))}' if item.get('eligibility_result') == 'eligible' else ''}</em></li>"""
        for item in receipt.get("items") or []
    )
    undo = "" if receipt.get("undone_at") else f"""
      <form method="post" action="/admin/finances/workspace/batches/{html.escape(str(receipt.get('id') or ''), quote=True)}/undo">
        <input type="hidden" name="_csrf_token" value="{html.escape(csrf_token, quote=True)}">
        <button class="btn btn-secondary" type="submit">Undo this batch</button>
      </form>"""
    body = f"""<div class="money-brief finance-workspace-review">{render_finance_nav('review', counts={})}
      <header class="money-page-header"><div><p class="finance-eyebrow">Save receipt</p><h1>{html.escape(str(receipt.get('status') or 'Saved').replace('_', ' ').title())}</h1>
      <p class="money-page-subtitle">Batch {html.escape(str(receipt.get('id') or ''))} records who changed what and when.</p></div></header>
      <section class="workspace-receipt"><dl><div><dt>Saved</dt><dd>{int(receipt.get('applied_count') or 0)} change(s)</dd></div><div><dt>Skipped</dt><dd>{int(receipt.get('skipped_count') or 0)} change(s)</dd></div><div><dt>Actor</dt><dd>{html.escape(str(receipt.get('actor') or ''))}</dd></div><div><dt>Value reviewed</dt><dd>{_dollar(int(receipt.get('amount_cents') or 0))}</dd></div></dl><ul>{rows}</ul>{undo}<a class="btn btn-primary" href="/admin/finances/budget">Return to Budget & savings</a></section>
    </div>"""
    return _page_shell("Finance save receipt", "review", body)
