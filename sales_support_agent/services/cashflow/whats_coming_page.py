"""Progressively enhanced operator queue for bank-detected bills."""

from __future__ import annotations

import html
from datetime import date, timedelta
from difflib import SequenceMatcher
from typing import Any, Mapping, Sequence
from uuid import uuid4

from sales_support_agent.services.cashflow.finance_nav import render_finance_nav
from sales_support_agent.services.cashflow.overview import _money, _page_shell
from sales_support_agent.services.cashflow.recurring import _FREQUENCY_WORDS
from sales_support_agent.services.cashflow.vendor_aliases import clean_vendor_display_name

NAV_KEY = "whats_coming"
PAGE_PATH = "/admin/finances/whats-coming"
BULK_ACTION = f"{PAGE_PATH}/bulk"


def _day(value: Any) -> str:
    try:
        parsed = value if isinstance(value, date) else date.fromisoformat(str(value)[:10])
        return parsed.strftime("%b %d, %Y")
    except (TypeError, ValueError):
        return "Not sure yet"


def _frequency(value: Any) -> str:
    key = str(value or "").strip().lower()
    return _FREQUENCY_WORDS.get(key, key or "Not sure")


def _evidence(pattern: Mapping[str, Any]) -> str:
    rows = "".join(
        "<li><strong>" + html.escape(_money(int(item.get("amount_cents") or 0)))
        + "</strong> on " + html.escape(_day(item.get("due_date")))
        + ("<br><small>Bank description: " + html.escape(str(item.get("raw_descriptor") or "Unavailable")) + "</small>")
        + "</li>"
        for item in list(pattern.get("evidence") or [])[:6]
    )
    return (
        '<details class="bill-evidence"><summary>Why we recognized this</summary>'
        f'<p>Why we think so: {html.escape(str(pattern.get("why") or ""))}.</p>'
        f'<p>Past payments:</p><ul>{rows}</ul></details>'
    )


def _row(pattern: Mapping[str, Any], suggestion: str = "") -> str:
    key = html.escape(str(pattern["pattern_key"]), quote=True)
    raw_vendor = str(pattern.get("vendor") or "Unknown vendor")
    vendor = html.escape(clean_vendor_display_name(raw_vendor))
    amount = int(pattern.get("amount_cents") or 0)
    monthly = int(pattern.get("monthly_cost_cents") or amount)
    confidence = html.escape(str(pattern.get("confidence_label") or "Possible"))
    pieces = bool(pattern.get("paid_in_pieces"))
    search_text = " ".join([
        raw_vendor, vendor,
        *[str(item.get("raw_descriptor") or "") for item in pattern.get("evidence") or []],
    ]).lower()
    return f"""
    <tr data-bill-row data-pattern-key="{key}" data-amount="{amount}"
        data-monthly="{monthly}" data-due="{html.escape(str(pattern.get('next_due') or ''))}"
        data-confidence="{confidence.lower()}" data-pieces="{'true' if pieces else 'false'}"
        data-merchant-key="{html.escape(str(pattern.get('merchant_key') or ''), quote=True)}"
        data-vendor="{vendor}" data-search="{html.escape(search_text, quote=True)}">
      <td class="bill-select"><input type="checkbox" name="pattern_keys" value="{key}"
          form="bill-bulk-form" aria-label="Select {vendor}"></td>
      <td data-label="Vendor"><strong>{vendor}</strong>
          {'<span class="status-badge">Paid in pieces</span>' if pieces else ''}
          {f'<p class="bill-suggestion">Possible same vendor as <strong>{html.escape(suggestion)}</strong>. Select both and preview Combine.</p>' if suggestion else ''}
          {_evidence(pattern)}<p class="row-error" role="alert" hidden></p></td>
      <td data-label="Typical payment" class="amount-out">{html.escape(_money(amount))}</td>
      <td data-label="Monthly cost"><strong>{html.escape(_money(monthly))}</strong></td>
      <td data-label="Frequency">{html.escape(_frequency(pattern.get("frequency")))}</td>
      <td data-label="Next due">{html.escape(_day(pattern.get("next_due")))}</td>
      <td data-label="Confidence"><span class="status-badge">{confidence}</span></td>
      <td class="bill-row-actions" data-label="Decision">
        <span class="sr-only">Track this · Not now</span>
        <form method="post" action="{BULK_ACTION}" data-bill-action-form>
          <input type="hidden" name="pattern_keys" value="{key}">
          <input type="hidden" name="request_id" value="{uuid4().hex}">
          <button class="btn btn-primary" name="action" value="track">Review &amp; track<span class="sr-only"> this</span></button>
          <button class="btn btn-secondary" name="action" value="not_a_bill">Not a bill</button>
          <button class="btn btn-secondary" name="action" value="snooze">Ask me next week<span class="sr-only"> (formerly Not now)</span></button>
        </form>
      </td>
    </tr>"""


def _answered_section(patterns: Sequence[Mapping[str, Any]], tracked: Sequence[Mapping[str, Any]]) -> str:
    rows_data = [row for row in patterns if row.get("decision") == "track"] + list(tracked)
    if not rows_data:
        return ""
    rows = "".join(
        f"<tr><td>{html.escape(clean_vendor_display_name(str(row.get('vendor') or '')))}</td>"
        f"<td>{html.escape(_money(int(row.get('monthly_cost_cents') or row.get('amount_cents') or 0)))}</td>"
        f"<td>{html.escape(_day(row.get('next_due')))}</td></tr>"
        for row in rows_data
    )
    answered = [row for row in patterns if row.get("decision") == "track"]
    title = f"You track this one / already scheduled ({len(rows_data)})" if answered else f"Already on your schedule ({len(rows_data)})"
    tracked_note = f'<span class="sr-only">{len(answered)} you already track</span>' if answered else ""
    return f"""<details class="card bill-answered"><summary>{html.escape(title)}</summary>{tracked_note}
      <p>These items are outside the working queue but remain visible for verification.</p>
      <div class="bill-table-wrap"><table class="finance-accounts-table"><thead><tr>
      <th>Vendor</th><th>Monthly cost</th><th>Next due</th></tr></thead><tbody>{rows}</tbody></table></div>
    </details>"""


def _alias_history() -> str:
    from sales_support_agent.services.cashflow.vendor_aliases import list_vendor_aliases
    aliases = list_vendor_aliases()
    if not aliases:
        return ""
    rows = "".join(
        f"<tr><td>{html.escape(row['alias_key'].title())}</td><td>{html.escape(row['canonical_name'])}</td>"
        f'<td><form method="post" action="{PAGE_PATH}/vendor-alias/revoke"><input type="hidden" '
        f'name="alias_key" value="{html.escape(row["alias_key"], quote=True)}">'
        '<button class="btn btn-secondary">Separate</button></form></td></tr>'
        for row in aliases
    )
    return f"""<details class="card bill-alias-history"><summary>Combined vendor names ({len(aliases)})</summary>
      <p>Grouping never changes posted bank records. Separate a name to restore its own history.</p>
      <div class="bill-table-wrap"><table class="finance-accounts-table"><thead><tr>
      <th>Original group</th><th>Shown as</th><th>Action</th></tr></thead><tbody>{rows}</tbody></table></div>
    </details>"""


def _activity(patterns: Sequence[Mapping[str, Any]]) -> str:
    from sales_support_agent.services.cashflow.bill_queue import list_queue_activity
    try:
        activity = list_queue_activity(limit=20)
    except Exception:
        return ""
    if not activity:
        return ""
    vendors_by_key = {
        str(row.get("pattern_key") or ""): clean_vendor_display_name(
            str(row.get("vendor") or "")
        )
        for row in patterns
    }
    items = []
    labels = {
        "track": "Tracked", "not_a_bill": "Marked not a bill",
        "snooze": "Asked again next week", "combine": "Combined vendors",
    }
    for entry in activity:
        payload = entry["payload"]
        action = str(payload.get("decision") or payload.get("action") or "")
        evidence = payload.get("evidence") or {}
        vendors = ", ".join(
            str(item.get("vendor") or "") for item in payload.get("vendors") or []
        )
        pattern_reference = str(
            payload.get("pattern_key") or entry.get("pattern_key") or ""
        )
        if not vendors:
            vendors = vendors_by_key.get(pattern_reference, "")
        legacy_note = ""
        if not vendors:
            vendors = (
                f"Legacy bill {pattern_reference[:8]} (vendor not captured)"
                if pattern_reference else "Legacy bill (vendor not captured)"
            )
            legacy_note = (
                "Legacy record · previous state, reason, and batch were not captured"
            )
        detail = ""
        if evidence.get("return_on"):
            detail = f" · returns {_day(evidence['return_on'])}"
        if evidence.get("reason"):
            detail += f" · reason: {str(evidence['reason'])}"
        changes = "; ".join(
            f"{item.get('before', {}).get('decision', 'unreviewed')} → "
            f"{item.get('after', {}).get('decision', action)}"
            for item in payload.get("vendors") or []
        ) or legacy_note
        batch_id = str(
            evidence.get("batch_id")
            or (entry["id"] if entry["action_type"] == "bill_queue_batch_recorded" else "")
        )
        undo = (
            f'<button class="btn btn-secondary" type="button" '
            f'data-activity-undo="{html.escape(batch_id, quote=True)}">Undo batch</button>'
            if batch_id and entry["action_type"] == "bill_queue_batch_recorded"
            and action != "combine" else ""
        )
        items.append(
            f"<li><strong>{html.escape(labels.get(action, action.replace('_', ' ').title() or 'Queue updated'))}"
            f"{': ' + html.escape(vendors) if vendors else ''}</strong>"
            f"{html.escape(detail)}<br><small>{html.escape(entry['actor'])} · "
            f"{html.escape(str(entry['created_at'])[:19].replace('T', ' '))} · "
            f"Audit {html.escape(entry['id'][:8])}"
            f"{' · Batch ' + html.escape(batch_id[:8]) if batch_id else ''}</small>"
            f"{f'<br><small>{html.escape(changes)}</small>' if changes else ''}{undo}</li>"
        )
    return f"""<details class="card bill-activity"><summary>Recent bill activity ({len(activity)})</summary>
      <p>This history comes from the authoritative finance audit log.</p><ol>{''.join(items)}</ol></details>"""


def _script() -> str:
    return r"""
    <script>
    (() => {
      const table = document.querySelector('[data-bill-table]');
      const bulk = document.querySelector('#bill-bulk-form');
      if (!table || !bulk || !window.fetch) return;
      const rows = () => [...table.querySelectorAll('[data-bill-row]')];
      const selected = () => [...table.querySelectorAll('input[name="pattern_keys"]:checked')];
      const count = document.querySelector('[data-bill-count]');
      const live = document.querySelector('[data-bill-live]');
      const empty = document.querySelector('[data-filter-empty]');
      const undo = document.querySelector('[data-bill-undo]');
      let filter = 'all';
      const money = cents => new Intl.NumberFormat('en-US',{style:'currency',currency:'USD'}).format(cents/100);
      const escapeText = value => String(value ?? '').replace(/[&<>"']/g, character => ({
        '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'
      })[character]);

      function visibleRows() { return rows().filter(row => !row.hidden); }
      function applyView() {
        const query = document.querySelector('[data-bill-search]').value.trim().toLowerCase();
        rows().forEach(row => {
          const matchesFilter = filter === 'large' ? Number(row.dataset.monthly) > 50000
            : filter === 'likely' ? row.dataset.confidence === 'very likely'
            : filter === 'pieces' ? row.dataset.pieces === 'true' : true;
          row.hidden = !matchesFilter || (query && !row.dataset.search.includes(query));
        });
        const sort = document.querySelector('[data-bill-sort]').value;
        const body = table.tBodies[0];
        rows().sort((a,b) => {
          if (sort === 'monthly') return Number(b.dataset.monthly)-Number(a.dataset.monthly);
          if (sort === 'due') return a.dataset.due.localeCompare(b.dataset.due);
          if (sort === 'confidence') return b.dataset.confidence.localeCompare(a.dataset.confidence);
          return a.dataset.vendor.localeCompare(b.dataset.vendor);
        }).forEach(row => body.appendChild(row));
        count.textContent = visibleRows().length;
        empty.hidden = visibleRows().length > 0;
      }
      function updateBar() {
        const n = selected().length;
        bulk.classList.toggle('is-active', n > 0);
        bulk.querySelector('[data-selected-count]').textContent = `${n} selected`;
      }
      table.addEventListener('change', updateBar);
      document.querySelector('[data-select-all]')?.addEventListener('change', e => {
        visibleRows().forEach(row => row.querySelector('input[name="pattern_keys"]').checked = e.target.checked);
        updateBar();
      });
      document.querySelectorAll('[data-bill-filter]').forEach(button => button.addEventListener('click', () => {
        filter = button.dataset.billFilter;
        document.querySelectorAll('[data-bill-filter]').forEach(b => b.setAttribute('aria-pressed', String(b === button)));
        applyView();
      }));
      document.querySelector('[data-bill-search]').addEventListener('input', applyView);
      document.querySelector('[data-bill-sort]').addEventListener('change', applyView);
      document.querySelector('[data-clear-view]').addEventListener('click', () => {
        filter='all'; document.querySelector('[data-bill-search]').value='';
        document.querySelectorAll('[data-bill-filter]').forEach(b => b.setAttribute('aria-pressed', String(b.dataset.billFilter==='all')));
        applyView();
      });

      function setBulkFields(action) {
        bulk.querySelectorAll('[data-for-action]').forEach(group => {
          group.hidden = group.dataset.forAction !== action;
          group.querySelectorAll('input').forEach(input => input.disabled = group.hidden);
        });
      }
      async function submit(form, affected, action) {
        affected.forEach(row => row.classList.add('is-working'));
        form.setAttribute('aria-busy','true');
        const data = new FormData(form); data.set('action', action);
        if (form === bulk) selected().forEach(box => data.append('pattern_keys', box.value));
        try {
          const response = await fetch(form.action,{method:'POST',body:data,headers:{Accept:'application/json'}});
          const result = await response.json();
          if (!response.ok) throw new Error(result.detail || 'Nothing changed. Please try again.');
          if (action === 'combine') return location.reload();
          affected.forEach(row => row.remove()); live.textContent = result.message || 'Saved.';
          if (result.batch_id) { undo.hidden=false; undo.dataset.batchId=result.batch_id; }
          updateBar(); applyView();
        } catch(error) {
          affected.forEach(row => { row.classList.remove('is-working'); const msg=row.querySelector('.row-error'); msg.hidden=false; msg.textContent=error.message; });
        } finally {
          form.removeAttribute('aria-busy');
        }
      }

      const review = document.querySelector('#bill-review-dialog');
      let pending = {form:null, rows:[], action:''};
      async function renderTrackPreview(affected) {
        const data = new FormData();
        affected.forEach(row => data.append('pattern_keys',row.dataset.patternKey));
        const chosenDate = review.querySelector('[name="payment_date"]').value;
        if (chosenDate) data.append('payment_date', chosenDate);
        const response=await fetch(`${location.pathname}/track-preview`,{method:'POST',body:data,headers:{Accept:'application/json'}});
        const result=await response.json();
        if (!response.ok) { live.textContent=result.detail; return false; }
        review.querySelector('[data-track-preview]').innerHTML=result.rows.map(row =>
          `<strong>${escapeText(row.vendor)}</strong><br>${money(row.amount_cents)} each · ${money(row.monthly_cost_cents)}/month · next ${escapeText(row.next_due)}<br>14-day effect: ${money(row.effect_14_cents)} · 30-day effect: ${money(row.effect_30_cents)}`
          + (row.possible_duplicate
            ? `<br><strong role="alert">Possible existing schedule: ${escapeText(row.possible_duplicate.vendor)} (${money(row.possible_duplicate.amount_cents)}, next ${escapeText(row.possible_duplicate.next_due)}). Review that schedule before tracking.</strong>`
            : '<br>No matching active schedule was found.')
        ).join('<hr>');
        const confirm=review.querySelector('[data-review-confirm]');
        confirm.disabled=Boolean(result.blocked);
        if(result.blocked) live.textContent='Tracking is paused because a possible matching schedule already exists.';
        return !result.blocked;
      }
      async function openReview(form, affected, action) {
        pending={form,rows:affected,action};
        review.querySelector('[data-review-title]').textContent = action === 'track' ? 'Preview tracking' : action === 'not_a_bill' ? 'Mark as not a bill' : 'Ask me next week';
        review.querySelectorAll('[data-review-fields]').forEach(group => group.hidden = group.dataset.reviewFields !== action);
        review.querySelector('[data-review-confirm]').textContent = action === 'track' ? 'Track bill' : action === 'not_a_bill' ? 'Confirm not a bill' : 'Ask again next week';
        review.querySelector('[data-review-confirm]').disabled = false;
        review.querySelector('[data-track-preview]').textContent = '';
        if (action === 'track') {
          await renderTrackPreview(affected);
        }
        review.showModal();
      }
      review.querySelector('[name="payment_date"]').addEventListener('change', () => {
        if (pending.action === 'track') renderTrackPreview(pending.rows);
      });
      table.addEventListener('submit', event => {
        const form=event.target.closest('[data-bill-action-form]'); if(!form)return;
        event.preventDefault(); openReview(form,[form.closest('[data-bill-row]')],event.submitter?.value||'');
      });
      bulk.addEventListener('submit', event => {
        event.preventDefault(); const action=event.submitter?.value||''; const affected=selected().map(box=>box.closest('[data-bill-row]'));
        if(!affected.length)return; setBulkFields(action);
        if(action==='combine')return openCombine(affected);
        openReview(bulk,affected,action);
      });
      review.querySelector('[data-review-cancel]').addEventListener('click',()=>review.close());
      review.querySelector('form').addEventListener('submit',event=>{
        event.preventDefault();
        const fields=new FormData(event.target);
        pending.form.querySelectorAll('[data-review-generated]').forEach(input => input.remove());
        for(const [key,value] of fields) {
          let input=pending.form.querySelector(`[name="${key}"]`);
          if(!input){
            input=document.createElement('input');input.type='hidden';input.name=key;
            input.dataset.reviewGenerated='true';pending.form.appendChild(input);
          }
          input.value=value;
        }
        review.close(); submit(pending.form,pending.rows,pending.action);
      });

      const combine=document.querySelector('#bill-combine-dialog');
      let combinePreviewValid=false;
      let combinePreviewToken='';
      function openCombine(affected){
        if(affected.length<2){live.textContent='Select at least two vendors to combine.';return;}
        const select=combine.querySelector('[name="canonical_key"]');
        select.innerHTML=affected.map(row=>`<option value="${row.dataset.merchantKey}">${row.dataset.vendor}</option>`).join('');
        combine.querySelector('[name="canonical_name"]').value=affected[0].dataset.vendor;
        combinePreviewValid=false; combinePreviewToken='';
        combine.querySelector('[data-combine-confirm]').hidden=true;
        combine.querySelector('[data-combine-confirm]').disabled=true;
        combine.querySelector('[data-combine-preview]').textContent='Preview the recalculated result before confirming.';
        combine.showModal();
      }
      combine.querySelectorAll('input,select').forEach(input=>input.addEventListener('input',()=>{
        combinePreviewValid=false;combinePreviewToken='';
        combine.querySelector('[data-combine-confirm]').hidden=true;
        combine.querySelector('[data-combine-confirm]').disabled=true;
      }));
      combine.querySelector('[data-combine-cancel]').addEventListener('click',()=>combine.close());
      combine.querySelector('[data-combine-preview-button]').addEventListener('click',async()=>{
        const data=new FormData(combine.querySelector('form'));selected().forEach(box=>data.append('pattern_keys',box.value));
        const response=await fetch(`${location.pathname}/combine-preview`,{method:'POST',body:data,headers:{Accept:'application/json'}});
        const result=await response.json();
        if(!response.ok){combine.querySelector('[data-combine-error]').textContent=result.detail;return;}
        combine.querySelector('[data-combine-preview]').textContent=`${result.before.length} histories become ${result.after.vendor}: ${money(result.after.amount_cents)} ${result.after.frequency}, next ${result.after.next_due}. ${result.explanation}`;
        combinePreviewToken=result.preview_token||'';
        combinePreviewValid=Boolean(combinePreviewToken);
        combine.querySelector('[data-combine-confirm]').hidden=!combinePreviewValid;
        combine.querySelector('[data-combine-confirm]').disabled=!combinePreviewValid;
      });
      combine.querySelector('[data-combine-confirm]').addEventListener('click',()=>{
        if(!combinePreviewValid)return;
        bulk.querySelector('[name="canonical_key"]').value=combine.querySelector('[name="canonical_key"]').value;
        bulk.querySelector('[name="canonical_name"]').value=combine.querySelector('[name="canonical_name"]').value;
        bulk.querySelector('[name="preview_token"]').value=combinePreviewToken;
        combine.close();submit(bulk,selected().map(box=>box.closest('[data-bill-row]')),'combine');
      });
      undo.addEventListener('click',async()=>{
        const data=new FormData();data.append('batch_id',undo.dataset.batchId);
        const response=await fetch(`${location.pathname}/undo`,{method:'POST',body:data,headers:{Accept:'application/json'}});
        if(response.ok)location.reload();
      });
      document.querySelectorAll('[data-activity-undo]').forEach(button => {
        button.addEventListener('click', async () => {
          const data=new FormData();data.append('batch_id',button.dataset.activityUndo);
          const response=await fetch(`${location.pathname}/undo`,{method:'POST',body:data,headers:{Accept:'application/json'}});
          if(response.ok)location.reload();
          else live.textContent='That batch could not be undone.';
        });
      });
      applyView();
    })();
    </script>"""


def render_whats_coming_page(*, flash: str = "") -> str:
    from sales_support_agent.services.cashflow.bill_patterns import list_bill_patterns
    try:
        listing = list_bill_patterns()
    except Exception as exc:
        body = render_finance_nav(NAV_KEY) + f'<h1>What is coming</h1><div class="card"><strong>Could not load bills.</strong><p>{html.escape(str(exc))}</p></div>'
        return _page_shell("What is coming", NAV_KEY, body, flash=flash)
    patterns = list(listing["patterns"])
    evidence_dates = [
        str(item.get("due_date") or "")[:10]
        for pattern in patterns
        for item in pattern.get("evidence") or []
        if item.get("due_date")
    ]
    freshness = _day(max(evidence_dates)) if evidence_dates else "No dated bank evidence"
    waiting = [row for row in patterns if not row.get("decision")]
    waiting.sort(key=lambda row: (-int(row.get("monthly_cost_cents") or row.get("amount_cents") or 0), str(row.get("vendor") or "")))
    suggestions: dict[str, str] = {}
    for index, left in enumerate(waiting):
        left_name = clean_vendor_display_name(str(left.get("vendor") or ""))
        for right in waiting[index + 1:]:
            right_name = clean_vendor_display_name(str(right.get("vendor") or ""))
            if SequenceMatcher(None, left_name.lower(), right_name.lower()).ratio() >= .78:
                suggestions[str(left["pattern_key"])] = right_name
                suggestions[str(right["pattern_key"])] = left_name
    rows = "".join(_row(row, suggestions.get(str(row["pattern_key"]), "")) for row in waiting)
    queue = f"""
      <div class="bill-tools"><label class="bill-search">Search vendors or bank descriptions
      <input type="search" data-bill-search placeholder="Search bills"></label>
      <label>Sort by <select data-bill-sort><option value="monthly">Monthly cost</option>
      <option value="due">Next due</option><option value="confidence">Confidence</option><option value="vendor">Vendor</option></select></label></div>
      <div class="bill-command-bar"><div><button class="btn btn-secondary" data-bill-filter="all" aria-pressed="true">All</button>
      <button class="btn btn-secondary" data-bill-filter="large">Over $500/month</button>
      <button class="btn btn-secondary" data-bill-filter="likely">Very likely</button>
      <button class="btn btn-secondary" data-bill-filter="pieces">Paid in pieces</button></div>
      <strong><span data-bill-count>{len(waiting)}</span> shown</strong></div>
      <div class="card bill-filter-empty" data-filter-empty hidden><strong>No bills match this view.</strong>
      <p>Clear the search and filters to see the full queue.</p><button class="btn btn-secondary" data-clear-view>Clear view</button></div>
      <div class="bill-table-wrap"><table class="finance-accounts-table bill-queue" data-bill-table><thead><tr>
      <th><input type="checkbox" data-select-all aria-label="Select all visible bills"></th><th>Vendor</th>
      <th>Payment</th><th>Monthly cost</th><th>Frequency</th><th>Next due</th><th>Confidence</th><th>Decision</th>
      </tr></thead><tbody>{rows}</tbody></table></div>
    """ if waiting else '<div class="card"><strong>Nothing to add.</strong><p>Your bank history has no regular payment waiting for review.</p></div>'
    body = f"""
    {render_finance_nav(NAV_KEY)}
    <div class="finance-page-header"><div><h1>What is coming</h1>
    <p class="page-sub">Decide which regular bank payments belong in the cash plan. Nothing is added until you confirm it.</p>
    <p class="page-sub"><strong>Bank evidence through {html.escape(freshness)}.</strong></p>
    </div><a href="/admin/finances/recurring" class="btn btn-secondary">Schedules</a></div>
    <p class="sr-only">{len(waiting)} {'bill needs' if len(waiting) == 1 else 'bills need'} an answer</p>
    <p class="sr-only" aria-live="polite" data-bill-live></p>{queue}
    <form method="post" action="{BULK_ACTION}" id="bill-bulk-form" class="bill-bulk-bar">
      <input type="hidden" name="canonical_key"><input type="hidden" name="canonical_name">
      <input type="hidden" name="preview_token"><input type="hidden" name="request_id" value="{uuid4().hex}">
      <strong data-selected-count>0 selected</strong><button class="btn btn-primary" name="action" value="track">Track</button>
      <button class="btn btn-secondary" name="action" value="not_a_bill">Not a bill</button>
      <button class="btn btn-secondary" name="action" value="snooze">Ask me next week</button>
      <button class="btn btn-secondary" name="action" value="combine">Combine</button>
    </form>
    <dialog id="bill-review-dialog" class="bill-review-dialog"><form method="dialog">
      <h2 data-review-title>Review decision</h2><div data-review-fields="track"><p data-track-preview></p>
      <label>Category (optional)<input name="category"></label><label><input type="checkbox" name="paid_in_pieces" value="true"> This bill is paid in pieces</label>
      <label>Next payment date (optional)<input type="date" name="payment_date"></label></div>
      <div data-review-fields="not_a_bill" hidden><label>Reason (optional)<input name="reason" maxlength="500" placeholder="Why this is not a bill"></label></div>
      <div data-review-fields="snooze" hidden><p>This will return to the queue on <strong>{_day(date.today() + timedelta(days=7))}</strong>.</p></div>
      <div class="action-row"><button class="btn btn-secondary" type="button" data-review-cancel>Cancel</button>
      <button class="btn btn-primary" type="submit" data-review-confirm>Confirm</button></div></form></dialog>
    <dialog id="bill-combine-dialog" class="bill-combine-dialog"><form method="dialog">
      <h2>Combine vendor histories</h2><p>Choose the name to keep. We recalculate from original payments and never add two forecasts together.</p>
      <label>Keep vendor<select name="canonical_key"></select></label><label>Combined name<input name="canonical_name" required maxlength="255"></label>
      <div class="bill-preview" data-combine-preview>Preview the recalculated result before confirming.</div>
      <p class="row-error" data-combine-error role="alert"></p><div class="action-row">
      <button class="btn btn-secondary" type="button" data-combine-cancel>Cancel</button>
      <button class="btn btn-secondary" type="button" data-combine-preview-button>Preview</button>
      <button class="btn btn-primary" type="button" data-combine-confirm hidden disabled>Confirm combine</button></div></form></dialog>
    <button class="btn btn-secondary bill-undo" data-bill-undo hidden>Undo last answer</button>
    {_answered_section(patterns, listing["tracked"])}{_alias_history()}{_activity(patterns)}{_script()}"""
    return _page_shell("What is coming", NAV_KEY, body, flash=flash)


__all__ = ["render_whats_coming_page"]
