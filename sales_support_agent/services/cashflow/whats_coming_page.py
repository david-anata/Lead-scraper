"""Dense, progressively enhanced review table for bank-detected bills."""

from __future__ import annotations

import html
import json
from datetime import date
from typing import Any, Mapping, Sequence

from sales_support_agent.services.cashflow.finance_nav import render_finance_nav
from sales_support_agent.services.cashflow.overview import _money, _page_shell
from sales_support_agent.services.cashflow.recurring import _FREQUENCY_WORDS

NAV_KEY = "whats_coming"
PAGE_PATH = "/admin/finances/whats-coming"
BULK_ACTION = f"{PAGE_PATH}/bulk"


def _day(value: Any) -> str:
    if isinstance(value, date):
        return value.strftime("%b %d, %Y")
    try:
        return date.fromisoformat(str(value)[:10]).strftime("%b %d, %Y")
    except ValueError:
        return "Not sure yet"


def _frequency(value: Any) -> str:
    key = str(value or "").strip().lower()
    return _FREQUENCY_WORDS.get(key, key or "Not sure")


def _evidence(pattern: Mapping[str, Any]) -> str:
    rows = "".join(
        "<li>" + html.escape(_money(int(item.get("amount_cents") or 0)))
        + " on " + html.escape(_day(item.get("due_date"))) + "</li>"
        for item in list(pattern.get("evidence") or [])[:6]
    )
    why = html.escape(str(pattern.get("why") or ""))
    return (
        '<details class="bill-evidence"><summary>Evidence</summary>'
        f'<p>Why we think so: {why}.</p><p>Past payments:</p><ul>{rows}</ul></details>'
    )


def _row(pattern: Mapping[str, Any]) -> str:
    key = html.escape(str(pattern["pattern_key"]), quote=True)
    vendor = html.escape(str(pattern.get("vendor") or "Unknown vendor"))
    amount_cents = int(pattern.get("amount_cents") or 0)
    confidence = html.escape(str(pattern.get("confidence_label") or "Possible"))
    pieces = bool(pattern.get("paid_in_pieces"))
    return f"""
    <tr data-bill-row data-pattern-key="{key}" data-amount="{amount_cents}"
        data-confidence="{confidence.lower()}" data-pieces="{'true' if pieces else 'false'}"
        data-merchant-key="{html.escape(str(pattern.get('merchant_key') or ''), quote=True)}"
        data-vendor="{vendor}">
      <td class="bill-select"><input type="checkbox" name="pattern_keys" value="{key}"
          form="bill-bulk-form" aria-label="Select {vendor}"></td>
      <td><strong>{vendor}</strong>{'<span class="status-badge">Paid in pieces</span>' if pieces else ''}
          {_evidence(pattern)}<p class="row-error" role="alert" hidden></p></td>
      <td class="amount-out">{html.escape(_money(amount_cents))}</td>
      <td>{html.escape(_frequency(pattern.get("frequency")))}</td>
      <td>{html.escape(_day(pattern.get("next_due")))}</td>
      <td><span class="status-badge">{confidence}</span></td>
      <td class="bill-row-actions">
        <form method="post" action="{BULK_ACTION}" data-bill-action-form>
          <input type="hidden" name="pattern_keys" value="{key}">
          <button class="btn btn-primary" name="action" value="track">Track this</button>
          <button class="btn btn-secondary" name="action" value="not_a_bill">Not a bill</button>
          <button class="btn btn-secondary" name="action" value="snooze">Not now</button>
        </form>
      </td>
    </tr>"""


def _answered_section(patterns: Sequence[Mapping[str, Any]], tracked: Sequence[Mapping[str, Any]]) -> str:
    answered = [row for row in patterns if row.get("decision") == "track"]
    all_rows = [*answered, *tracked]
    if not all_rows:
        return ""
    rows = "".join(
        "<tr><td>" + html.escape(str(row.get("vendor") or ""))
        + "</td><td>" + html.escape(_money(int(row.get("amount_cents") or 0)))
        + "</td><td>" + html.escape(_frequency(row.get("frequency")))
        + "</td><td>" + html.escape(_day(row.get("next_due"))) + "</td></tr>"
        for row in all_rows
    )
    scheduled_only = not answered and bool(tracked)
    title = (
        f"Already on your schedule ({len(all_rows)})"
        if scheduled_only else f"You track this one / already scheduled ({len(all_rows)})"
    )
    tracked_phrase = f"{len(answered)} you already track" if answered else ""
    return f"""
    <details class="card bill-answered">
      <summary>{html.escape(title)}</summary>
      <span class="sr-only">{html.escape(tracked_phrase)}</span>
      <p>Answered items leave the working queue immediately. These remain visible here for auditability.</p>
      <table class="finance-accounts-table"><thead><tr><th>Vendor</th><th>Amount</th>
      <th>Frequency</th><th>Next due</th></tr></thead><tbody>{rows}</tbody></table>
    </details>"""


def _alias_history() -> str:
    from sales_support_agent.services.cashflow.vendor_aliases import list_vendor_aliases

    aliases = list_vendor_aliases()
    if not aliases:
        return ""
    rows = "".join(
        "<tr><td>" + html.escape(row["alias_key"].title()) + "</td><td>"
        + html.escape(row["canonical_name"]) + '</td><td><form method="post" action="'
        + PAGE_PATH + '/vendor-alias/revoke"><input type="hidden" name="alias_key" value="'
        + html.escape(row["alias_key"], quote=True)
        + '"><button class="btn btn-secondary">Separate</button></form></td></tr>'
        for row in aliases
    )
    return f"""
    <details class="card bill-alias-history"><summary>Combined vendor names ({len(aliases)})</summary>
    <p>These names share one raw payment history. Separate one here to restore its own grouping.</p>
    <table class="finance-accounts-table"><thead><tr><th>Bank name</th><th>Grouped as</th>
    <th>Action</th></tr></thead><tbody>{rows}</tbody></table></details>"""


def _script() -> str:
    return """
    <script>
    (() => {
      const table = document.querySelector('[data-bill-table]');
      const bulk = document.querySelector('#bill-bulk-form');
      if (!table || !bulk || !window.fetch) return;
      const count = document.querySelector('[data-bill-count]');
      const live = document.querySelector('[data-bill-live]');
      const undo = document.querySelector('[data-bill-undo]');
      const selected = () => [...table.querySelectorAll('input[name="pattern_keys"]:checked')];
      const updateBar = () => {
        const n = selected().length;
        bulk.classList.toggle('is-active', n > 0);
        bulk.querySelector('[data-selected-count]').textContent = n + ' selected';
      };
      table.addEventListener('change', updateBar);
      document.querySelector('[data-select-all]')?.addEventListener('change', e => {
        table.querySelectorAll('input[name="pattern_keys"]').forEach(box => box.checked = e.target.checked);
        updateBar();
      });
      document.querySelectorAll('[data-bill-filter]').forEach(button => button.addEventListener('click', () => {
        const mode = button.dataset.billFilter;
        document.querySelectorAll('[data-bill-filter]').forEach(b => b.setAttribute('aria-pressed', String(b === button)));
        table.querySelectorAll('[data-bill-row]').forEach(row => {
          row.hidden = mode === 'large' ? Number(row.dataset.amount) < 50000
            : mode === 'likely' ? row.dataset.confidence !== 'very likely'
            : mode === 'pieces' ? row.dataset.pieces !== 'true' : false;
        });
      }));
      async function submit(form, rows, action) {
        rows.forEach(row => row.classList.add('is-working'));
        const data = new FormData(form);
        data.set('action', action);
        if (form === bulk) selected().forEach(box => data.append('pattern_keys', box.value));
        try {
          const response = await fetch(form.action, {method:'POST', body:data, headers:{Accept:'application/json'}});
          const result = await response.json();
          if (!response.ok) throw new Error(result.detail || 'Nothing changed. Please try again.');
          if (action === 'combine') { location.reload(); return; }
          rows.forEach(row => row.remove());
          count.textContent = result.remaining;
          live.textContent = result.message || 'Saved.';
          if (result.batch_id) {
            undo.hidden = false; undo.dataset.batchId = result.batch_id;
          }
          updateBar();
        } catch (error) {
          rows.forEach(row => {
            row.classList.remove('is-working');
            const message = row.querySelector('.row-error');
            message.hidden = false; message.textContent = error.message;
          });
        }
      }
      table.addEventListener('submit', event => {
        const form = event.target.closest('[data-bill-action-form]');
        if (!form) return;
        event.preventDefault(); submit(form, [form.closest('[data-bill-row]')], event.submitter?.value || '');
      });
      bulk.addEventListener('submit', event => {
        event.preventDefault();
        const rows = selected().map(box => box.closest('[data-bill-row]'));
        const action = event.submitter?.value || '';
        if (!rows.length) return;
        if (action === 'combine') {
          if (rows.length < 2) { live.textContent = 'Select at least two vendors to combine.'; return; }
          const dialog = document.querySelector('#bill-combine-dialog');
          const select = dialog.querySelector('[name="canonical_key"]');
          select.innerHTML = rows.map(row => `<option value="${row.dataset.merchantKey}">${row.dataset.vendor}</option>`).join('');
          dialog.querySelector('[name="canonical_name"]').value = rows[0].dataset.vendor;
          dialog.showModal(); return;
        }
        submit(bulk, rows, action);
      });
      const dialog = document.querySelector('#bill-combine-dialog');
      dialog?.querySelector('[data-combine-cancel]').addEventListener('click', () => dialog.close());
      dialog?.querySelector('form').addEventListener('submit', async event => {
        event.preventDefault();
        const rows = selected().map(box => box.closest('[data-bill-row]'));
        const previewData = new FormData(dialog.querySelector('form'));
        selected().forEach(box => previewData.append('pattern_keys', box.value));
        const previewResponse = await fetch('/admin/finances/whats-coming/combine-preview', {
          method:'POST', body:previewData, headers:{Accept:'application/json'}
        });
        const preview = await previewResponse.json();
        if (!previewResponse.ok) {
          dialog.querySelector('[data-combine-error]').textContent = preview.detail;
          return;
        }
        dialog.querySelector('[data-combine-preview]').textContent =
          `${preview.before.length} rows become 1: ${preview.after.vendor}, $${(preview.after.amount_cents/100).toFixed(2)} ${preview.after.frequency}. ${preview.explanation}`;
        if (!dialog.dataset.confirmed) { dialog.dataset.confirmed = 'true'; return; }
        bulk.querySelector('[name="canonical_key"]').value = preview.after.merchant_key;
        bulk.querySelector('[name="canonical_name"]').value = preview.after.vendor;
        dialog.close(); submit(bulk, rows, 'combine');
      });
      undo.addEventListener('click', async () => {
        const data = new FormData(); data.append('batch_id', undo.dataset.batchId);
        const response = await fetch('/admin/finances/whats-coming/undo', {
          method:'POST', body:data, headers:{Accept:'application/json'}
        });
        if (response.ok) location.reload();
      });
    })();
    </script>"""


def render_whats_coming_page(*, flash: str = "") -> str:
    from sales_support_agent.services.cashflow.bill_patterns import list_bill_patterns

    try:
        listing = list_bill_patterns()
    except Exception as exc:
        body = render_finance_nav(NAV_KEY) + "<h1>What is coming</h1><div class=\"card\">" + html.escape(str(exc)) + "</div>"
        return _page_shell("What is coming", NAV_KEY, body, flash=flash)

    patterns = list(listing["patterns"])
    waiting = [row for row in patterns if not row.get("decision")]
    waiting.sort(key=lambda row: (
        -int(row.get("amount_cents") or 0), str(row.get("vendor") or "")
    ))
    rows = "".join(_row(row) for row in waiting)
    table = f"""
    <div class="bill-command-bar" aria-label="Bill filters">
      <div><button class="btn btn-secondary" data-bill-filter="all" aria-pressed="true">All</button>
      <button class="btn btn-secondary" data-bill-filter="large">Over $500/month</button>
      <button class="btn btn-secondary" data-bill-filter="likely">Very likely</button>
      <button class="btn btn-secondary" data-bill-filter="pieces">Paid in pieces</button></div>
      <strong><span data-bill-count>{len(waiting)}</span> {'bill needs' if len(waiting) == 1 else 'bills need'} an answer</strong>
      <span class="sr-only">{len(waiting)} {'bill needs' if len(waiting) == 1 else 'bills need'} an answer</span>
    </div>
    <div class="bill-table-wrap"><table class="finance-accounts-table bill-queue" data-bill-table>
      <thead><tr><th><input type="checkbox" data-select-all aria-label="Select all bills"></th>
      <th>Vendor</th><th>Amount</th><th>Frequency</th><th>Next due</th>
      <th>Confidence</th><th>Answer</th></tr></thead>
      <tbody>{rows}</tbody>
    </table></div>""" if waiting else """
    <div class="card"><p><strong>Nothing to add.</strong> Your bank history has no regular payment
    still waiting for an answer.</p></div>"""

    body = f"""
    {render_finance_nav(NAV_KEY)}
    <div class="finance-page-header"><div><h1>What is coming</h1>
    <p class="page-sub">Review regular payments found in posted bank history. Nothing enters the cash plan until you track it.</p>
    </div><a href="/admin/finances/recurring" class="btn btn-secondary">Schedules</a></div>
    <p class="sr-only" aria-live="polite" data-bill-live></p>
    {table}
    <form method="post" action="{BULK_ACTION}" id="bill-bulk-form" class="bill-bulk-bar">
      <input type="hidden" name="canonical_key"><input type="hidden" name="canonical_name">
      <strong data-selected-count>0 selected</strong>
      <button class="btn btn-primary" name="action" value="track">Track</button>
      <button class="btn btn-secondary" name="action" value="not_a_bill">Not a bill</button>
      <button class="btn btn-secondary" name="action" value="snooze">Not now</button>
      <button class="btn btn-secondary" name="action" value="combine">Combine</button>
      <label>Category <input name="category" placeholder="Optional"></label>
      <label><input type="checkbox" name="paid_in_pieces" value="true"> Paid in pieces</label>
      <label>Payment date <input type="date" name="payment_date"></label>
    </form>
    <dialog id="bill-combine-dialog" class="bill-combine-dialog">
      <form method="dialog">
        <h2>Combine vendor histories</h2>
        <p>Choose the vendor name to keep, or type a clearer name. We recalculate from all
        original payments; we do not add two forecasts together.</p>
        <label>Keep vendor <select name="canonical_key"></select></label>
        <label>Combined name <input name="canonical_name" required maxlength="255"></label>
        <p data-combine-preview>Continue once to preview the new estimate, then again to confirm.</p>
        <p class="row-error" data-combine-error role="alert"></p>
        <div class="action-row"><button class="btn btn-secondary" type="button" data-combine-cancel>Cancel</button>
        <button class="btn btn-primary" type="submit">Preview / confirm</button></div>
      </form>
    </dialog>
    <button class="btn btn-secondary bill-undo" data-bill-undo hidden>Undo last answer</button>
    {_answered_section(patterns, listing["tracked"])}
    {_alias_history()}
    {_script()}"""
    return _page_shell("What is coming", NAV_KEY, body, flash=flash)


__all__ = ["render_whats_coming_page"]
