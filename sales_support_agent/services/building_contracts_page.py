"""Operator pages for the Building contract workspace.

Rendering only. Every form posts to an existing guarded route and keeps its
typed confirmation, permission, and audit contract. No surface here sends a
contract, requests a signature, creates an invoice, or charges a card.
"""

from __future__ import annotations

import html
import json
from datetime import datetime
from typing import Any, Optional
from zoneinfo import ZoneInfo

from sales_support_agent.services.building_contracts import (
    CONTRACT_STATE_FILTERS,
    CONTRACT_TYPE_LABELS,
)
from sales_support_agent.services.ui_shell import render_operator_document


MOUNTAIN = ZoneInfo("America/Denver")
CONTRACTS_URL = "/admin/building/contracts"
TEMPLATES_URL = "/admin/building/contracts/templates"
_LAUNCH_DECISION_URL = "/admin/building#arena-launch-readiness"


def _esc(value: Any) -> str:
    return html.escape(str(value if value is not None else ""))


def _money(cents: int, currency: str = "USD") -> str:
    return f"{_esc(currency or 'USD')} {int(cents or 0) / 100:,.2f}"


def _when(value: Optional[datetime], *, with_time: bool = True) -> str:
    if value is None:
        return "—"
    local = value.astimezone(MOUNTAIN)
    if with_time:
        return local.strftime("%b %d, %Y · %I:%M %p MT")
    return local.strftime("%b %d, %Y")


def _status(label: str, modifier: str) -> str:
    return (
        f'<span class="app-status app-status--{_esc(modifier)}">{_esc(label)}</span>'
    )


def _metric(value: str, label: str) -> str:
    return (
        f'<div class="app-metric"><div class="app-metric__value">{value}</div>'
        f'<div class="app-metric__label">{_esc(label)}</div></div>'
    )


def _alert(kind: str, body: str) -> str:
    return f'<div class="app-alert app-alert--{_esc(kind)}">{body}</div>'


def _messages(notice: str, error: str) -> str:
    blocks = []
    if notice:
        blocks.append(_alert("notice", f"<p>{_esc(notice)}</p>"))
    if error:
        blocks.append(_alert("error", f"<p>{_esc(error)}</p>"))
    return "".join(blocks)


def _blocked_template_alert() -> str:
    return _alert(
        "blocked",
        "<p><strong>No approved agreement template exists.</strong> Contract "
        "preparation fails closed until a reusable template is authored, legally "
        "approved, and versioned. The 2025 Vivint agreement is customer-specific "
        "evidence and is not reusable.</p>"
        f'<p><a href="{TEMPLATES_URL}">Author a reusable template →</a> · '
        f'<a href="{_LAUNCH_DECISION_URL}">'
        "Review the contract-template launch decision →</a></p>",
    )


def _terms(snapshot: dict[str, Any], payment: Optional[dict[str, Any]] = None) -> str:
    """Render the frozen package snapshot as labeled terms, never raw JSON."""

    if not snapshot:
        return (
            '<div class="app-state-panel"><h3>No frozen terms</h3>'
            "<p>This contract has no prepared package snapshot. It is legacy "
            "free-text evidence and carries no verifiable terms.</p></div>"
        )
    quote = dict(snapshot.get("quote") or {})
    window = dict(snapshot.get("event_window") or {})
    template = dict(snapshot.get("template") or {})
    merge_values = dict(snapshot.get("merge_values") or {})
    currency = str(quote.get("currency") or "USD")
    rows: list[tuple[str, str]] = []
    if template:
        rows.append((
            "Template",
            f"{_esc(template.get('name'))} · v{_esc(template.get('version'))}",
        ))
        if template.get("approved_by"):
            rows.append((
                "Template approved by",
                f"{_esc(template.get('approved_by'))} on "
                f"{_esc(str(template.get('approved_at') or '')[:10])}",
            ))
    if quote:
        rows.append(("Quote total", _money(int(quote.get("amount_cents") or 0), currency)))
        if quote.get("version") is not None:
            rows.append(("Quote version", f"v{_esc(quote.get('version'))}"))
    for key, label in (
        ("setup_starts_at", "Setup starts"),
        ("guest_starts_at", "Guests arrive"),
        ("guest_ends_at", "Guests depart"),
        ("teardown_ends_at", "Teardown ends"),
    ):
        if window.get(key):
            rows.append((label, _esc(str(window.get(key)).replace("T", " ")[:16])))
    if payment:
        rows.append((
            "Required payment",
            _money(
                int(payment.get("amount_cents") or 0),
                str(payment.get("currency") or currency),
            )
            + f' <span class="app-table__sub">{_esc(str(payment.get("request_type") or "").replace("_", " "))}</span>',
        ))
    # Merge values are the allow-listed customer-facing terms. Money and window
    # fields are already rendered above, so only the remaining terms are shown.
    _rendered_above = {
        "quote_total",
        "currency",
        "deposit_amount",
        "setup_starts_at",
        "guest_starts_at",
        "guest_ends_at",
        "teardown_ends_at",
    }
    for key, value in merge_values.items():
        if key in _rendered_above:
            continue
        if isinstance(value, dict):
            rendered = ", ".join(
                f"{item_key.replace('_', ' ')}: {item_value}"
                for item_key, item_value in value.items()
                if item_value not in (None, "", 0)
            )
            rendered = _esc(rendered or json.dumps(value, sort_keys=True))
        elif isinstance(value, list):
            rendered = _esc(", ".join(str(item) for item in value)) if value else "None"
        else:
            rendered = _esc(value)
        rows.append((key.replace("_", " ").capitalize(), rendered or "—"))
    body = "".join(
        f"<div class=\"app-detail-list__row\"><dt>{_esc(label)}</dt><dd>{value}</dd></div>"
        for label, value in rows
    )
    return f'<dl class="app-detail-list">{body}</dl>'


def render_contract_index(
    *,
    navigation: str,
    rows: list[dict[str, Any]],
    total_count: int,
    search: str,
    state: str,
    contract_type: str,
    options: dict[str, Any],
    template_approved: bool,
    can_prepare: bool,
    csrf_token: str,
    suggested_idempotency_key: str,
    notice: str = "",
    error: str = "",
) -> str:
    """Contract index: decision summary, command bar, workspace, preparation."""

    filtered = bool(search or state or contract_type)
    needs_review = sum(1 for row in rows if row["state_label"] == "Needs review")
    ready = sum(1 for row in rows if row["state_label"] == "Ready")
    blocked = sum(
        1 for row in rows
        if row["state_label"] in {"Blocked", "Failed", "Unverified"}
    )

    if not rows and not filtered:
        workspace = (
            '<div class="app-state-panel"><h2>No contracts yet</h2>'
            "<p>Contracts appear here once an agreement package is prepared "
            "against an event hold and a frozen quote.</p>"
            + (
                "<p>The next action is approving a reusable agreement template.</p>"
                if not template_approved
                else '<p>Use <a href="#prepare">Prepare a contract</a> below.</p>'
            )
            + "</div>"
        )
    elif not rows:
        workspace = (
            '<div class="app-state-panel"><h2>No contracts match this scope</h2>'
            "<p>Adjust the search or filters to see contracts again.</p>"
            f'<p><a class="admin-btn admin-btn--ghost" href="{CONTRACTS_URL}">'
            "Clear filters</a></p></div>"
        )
    else:
        body = "".join(
            f"""<tr>
              <td><a href="{CONTRACTS_URL}/{_esc(row['id'])}"><strong>{_esc(row['customer_name'])}</strong></a>
                <div class="app-table__sub">{_esc(row['customer_email'])}</div></td>
              <td>{_esc(row['space_name'])}<div class="app-table__sub">{_esc(row['contract_type_label'])} · v{_esc(row['version'])}</div></td>
              <td>{_esc(_when(row['starts_at'], with_time=False))}
                <div class="app-table__sub">to {_esc(_when(row['ends_at'], with_time=False))}</div></td>
              <td class="is-numeric">{_money(row['amount_cents'], row['currency'])}
                <div class="app-table__sub">{'Deposit required' if row['deposit_required'] else 'No deposit'}</div></td>
              <td>{_status(row['state_label'], row['state_modifier'])}</td>
              <td>{_status(row['payment_label'], row['payment_modifier'])}</td>
              <td>{_esc(row['owner'] or '—')}</td>
              <td>{_esc(_when(row['updated_at']))}</td>
            </tr>"""
            for row in rows
        )
        workspace = (
            '<div class="app-data-workspace"><table class="app-table app-table--sticky">'
            "<thead><tr><th>Customer</th><th>Space</th><th>Dates</th>"
            "<th class=\"is-numeric\">Value</th><th>Contract</th><th>Payment request</th>"
            "<th>Owner</th><th>Updated</th></tr></thead>"
            f"<tbody>{body}</tbody></table></div>"
        )

    state_options = "".join(
        f'<option value="{_esc(item)}"{" selected" if item == state else ""}>{_esc(item)}</option>'
        for item in CONTRACT_STATE_FILTERS
    )
    type_options = "".join(
        f'<option value="{_esc(key)}"{" selected" if key == contract_type else ""}>{_esc(label)}</option>'
        for key, label in CONTRACT_TYPE_LABELS.items()
    )
    command_bar = f"""<form class="app-command-bar" method="get" action="{CONTRACTS_URL}" role="search">
      <div class="app-command-bar__group">
        <label class="app-field app-field--inline"><span>Search</span>
          <input type="search" name="q" value="{_esc(search)}" placeholder="Customer, space, or reservation"></label>
        <label class="app-field app-field--inline"><span>Contract state</span>
          <select name="state"><option value="">All states</option>{state_options}</select></label>
        <label class="app-field app-field--inline"><span>Type</span>
          <select name="type"><option value="">All types</option>{type_options}</select></label>
        <button class="admin-btn admin-btn--ghost" type="submit">Apply</button>
        {f'<a class="admin-btn admin-btn--ghost" href="{CONTRACTS_URL}">Clear</a>' if filtered else ''}
      </div>
      <p class="app-command-bar__count" aria-live="polite">Showing {len(rows)} of {total_count} contracts</p>
    </form>"""

    if not can_prepare:
        prepare = (
            '<section id="prepare" class="admin-panel"><h2>Prepare a contract</h2>'
            "<p>You do not have the <code>building.agreements.prepare</code> "
            "permission.</p></section>"
        )
    elif not template_approved:
        prepare = (
            '<section id="prepare" class="admin-panel"><h2>Prepare a contract</h2>'
            '<div class="app-state-panel"><h3>Blocked on an approved template</h3>'
            "<p>Preparation is unavailable until a reusable agreement template is "
            "approved. This is the same precondition the internal API enforces.</p>"
            "</div></section>"
        )
    elif not options.get("reservations"):
        prepare = (
            '<section id="prepare" class="admin-panel"><h2>Prepare a contract</h2>'
            '<div class="app-state-panel"><h3>No eligible event holds</h3>'
            "<p>Preparation requires an event reservation with an active, unexpired "
            "Agent hold and a frozen quote draft.</p></div></section>"
        )
    else:
        reservation_options = "".join(
            f'<option value="{_esc(item["id"])}">{_esc(item["label"])}</option>'
            for item in options["reservations"]
        )
        # Quotes are grouped by their event hold so the operator can see which
        # quote belongs to which reservation without JavaScript. A mismatched
        # pairing still fails closed in the guarded route.
        reservation_labels = {
            item["id"]: item["label"] for item in options["reservations"]
        }
        grouped: dict[str, list[dict[str, Any]]] = {}
        for item in options["quotes"]:
            grouped.setdefault(item["reservation_id"], []).append(item)
        quote_options = "".join(
            f'<optgroup label="{_esc(reservation_labels.get(reservation_id, reservation_id))}">'
            + "".join(
                f'<option value="{_esc(item["id"])}">{_esc(item["label"])}</option>'
                for item in items
            )
            + "</optgroup>"
            for reservation_id, items in grouped.items()
        )
        template_options = "".join(
            f'<option value="{_esc(item["id"])}">{_esc(item["label"])}</option>'
            for item in options["templates"]
        )
        prepare = f"""<section id="prepare" class="admin-panel">
      <h2>Prepare a contract</h2>
      <p class="app-muted">Preparation freezes an immutable evidence package. It creates no document, signature request, invoice, or charge.</p>
      <form class="app-form-grid" method="post" action="{CONTRACTS_URL}/packages">
        <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
        <input type="hidden" name="idempotency_key" value="{_esc(suggested_idempotency_key)}">
        <label class="app-field"><span>Event hold</span>
          <select name="reservation_id" required>{reservation_options}</select></label>
        <label class="app-field"><span>Frozen quote draft</span>
          <select name="quote_id" required>{quote_options}</select></label>
        <label class="app-field"><span>Approved template</span>
          <select name="template_id" required>{template_options}</select></label>
        <label class="app-field"><span>Agreement version</span>
          <input type="number" name="agreement_version" min="1" value="1"></label>
        <label class="app-field"><span>Payment readiness version</span>
          <input type="number" name="payment_version" min="1" value="1"></label>
        <div class="app-form-grid__actions">
          <button class="admin-btn" type="submit">Prepare immutable package</button>
          <span class="app-muted">Idempotency key <code>{_esc(suggested_idempotency_key)}</code> is generated for this form.</span>
        </div>
      </form>
    </section>"""

    body = f"""<header class="app-page-header">
      <div>
        <p class="app-eyebrow">Building</p>
        <h1>Contracts</h1>
        <p>Every agreement package, its frozen terms, its true state, and the payment request it authorizes. Nothing on this page sends, signs, invoices, or charges.</p>
      </div>
      <div class="app-page-actions">
        <a class="admin-btn admin-btn--ghost" href="/admin/building">Building Control</a>
        <a class="admin-btn" href="{TEMPLATES_URL}">Templates</a>
      </div>
    </header>
    {_messages(notice, error)}
    {'' if template_approved else _blocked_template_alert()}
    <div class="app-metric-strip">
      {_metric(str(total_count), "Contracts")}
      {_metric(str(needs_review), "Needs review")}
      {_metric(str(ready), "Ready")}
      {_metric(str(blocked), "Blocked or unverified")}
    </div>
    {command_bar}
    {workspace}
    {prepare}"""
    return render_operator_document(
        title="Contracts · Building · Anata Agent",
        navigation=navigation,
        body=body,
        page_class="building-contracts-page",
        extra_head=_CONTRACT_STYLES,
    )


def render_contract_detail(
    *,
    navigation: str,
    contract: dict[str, Any],
    can_approve: bool,
    can_prepare_signature: bool,
    can_prepare_payment: bool,
    google_doc_url: str = "",
    google_doc_error: str = "",
    can_manage: bool,
    csrf_token: str,
    notice: str = "",
    error: str = "",
) -> str:
    """One contract: reconciled state, frozen terms, linked records, actions, audit."""

    payment = contract.get("payment") or {}
    signature = contract.get("signature") or {}
    document = dict((contract.get("snapshot") or {}).get("document") or {})
    comparison = dict(contract.get("template_comparison") or {})
    handoff_manifest = json.dumps(
        {
            "provider": "QuickBooks Contract Builder",
            "agreement_id": contract["id"],
            "agreement_version": contract["version"],
            "agreement_checksum": contract["checksum"],
            "document_checksum": str(document.get("checksum") or ""),
            "customer_name": contract["customer_name"],
            "signer_email": str(signature.get("signer_email") or contract["customer_email"]),
            "event_space": contract["space_name"],
            "event_starts_at": (
                contract["starts_at"].isoformat() if contract.get("starts_at") else ""
            ),
            "contract_total_cents": contract["amount_cents"],
            "currency": contract["currency"],
        },
        indent=2,
        sort_keys=True,
    )
    # The document link only appears once the package is approved, matching the
    # route's own precondition.
    has_document = bool(
        document.get("text") and contract["preparation_status"] == "approved"
    )
    linked = [
        ("Reservation", contract["reservation_id"], contract["reservation_status"]),
        ("Quote", contract["quote"]["id"], str(contract["quote"]["status"] or "")),
        ("Template", contract["template"]["name"], str(contract["template"]["status"] or "")),
        ("Space", contract["space_name"], ""),
        ("Customer", contract["customer_name"], contract["customer_email"]),
    ]
    linked_rows = "".join(
        f'<div class="app-detail-list__row"><dt>{_esc(label)}</dt>'
        f"<dd>{_esc(value or '—')}"
        + (f'<span class="app-table__sub">{_esc(meta)}</span>' if meta else "")
        + "</dd></div>"
        for label, value, meta in linked
    )

    if contract["verified"]:
        evidence = f"""<dl class="app-detail-list">
      <div class="app-detail-list__row"><dt>Package checksum</dt><dd><code>{_esc(contract['checksum'])}</code></dd></div>
      <div class="app-detail-list__row"><dt>Preparation status</dt><dd>{_esc(contract['preparation_status'])}</dd></div>
      <div class="app-detail-list__row"><dt>Provider status</dt><dd>{_esc(contract['provider_status'] or '—')}</dd></div>
      <div class="app-detail-list__row"><dt>Reviewed</dt><dd>{_esc(contract['reviewed_by'] or '—')} · {_esc(_when(contract['reviewed_at']))}</dd></div>
      <div class="app-detail-list__row"><dt>Approved</dt><dd>{_esc(contract['approved_by'] or '—')} · {_esc(_when(contract['approved_at']))}</dd></div>
      <div class="app-detail-list__row"><dt>Agent hold</dt><dd>{'Active until ' + _esc(_when(contract['hold_expires_at'])) if contract['hold_active'] else 'Not active'}</dd></div>
      <div class="app-detail-list__row"><dt>Frozen document</dt><dd>{
        f'<code>{_esc(document.get("checksum"))}</code>'
        if document.get("text")
        else 'None. This template holds its text outside Agent.'
      }</dd></div>
    </dl>"""
    else:
        evidence = _alert(
            "blocked",
            "<p><strong>Unverified legacy record.</strong> This agreement was "
            "recorded as free text with no frozen package or checksum. It carries "
            "no verifiable terms and offers no governed action.</p>",
        )

    if comparison.get("matches"):
        template_comparison = _alert(
            "notice",
            "<p><strong>Template and package match.</strong> The approved template "
            "identity and rendered document checksum match this frozen package.</p>",
        )
    else:
        difference_items = "".join(
            f"<li>{_esc(item)}</li>" for item in comparison.get("differences", [])
        ) or "<li>No comparison evidence is available.</li>"
        template_comparison = _alert(
            "blocked",
            "<p><strong>Template/package differences require a new package.</strong></p>"
            f"<ul>{difference_items}</ul>",
        )

    audit_rows = "".join(
        f"<tr><td>{_esc(_when(item['created_at']))}</td>"
        f"<td>{_esc(item['action'].replace('_', ' '))}</td>"
        f"<td>{_esc(item['actor'] or '—')}</td>"
        f"<td>{_esc(item['entity_type'])}</td></tr>"
        for item in contract["audit"]
    ) or '<tr><td colspan="4">No audit history for this contract yet.</td></tr>'

    actions: list[str] = []
    agreement_next = {
        "prepared": (
            "in_review",
            f"REVIEW AGREEMENT {contract['id']}",
            "Submit contract for review",
            "I confirm the frozen customer, event, pricing, and contract evidence is ready for formal review.",
        ),
        "in_review": (
            "approved",
            f"APPROVE AGREEMENT {contract['id']}",
            "Approve contract package",
            "I confirm this frozen contract package has completed the required review.",
        ),
    }.get(contract["preparation_status"])
    agreement_comparison_allows_action = bool(
        comparison.get("matches")
        or (agreement_next and agreement_next[0] == "in_review")
    )
    if (
        contract["verified"]
        and can_approve
        and agreement_next
        and agreement_comparison_allows_action
    ):
        target, confirmation, action_label, confirmation_copy = agreement_next
        actions.append(f"""<form class="app-form-grid" method="post" action="{CONTRACTS_URL}/{_esc(contract['id'])}/transition">
        <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
        <input type="hidden" name="target_status" value="{_esc(target)}">
        <input type="hidden" name="confirmation" value="{_esc(confirmation)}">
        <h3>Agreement package</h3>
        <label class="app-confirmation"><input type="checkbox" required> <span>{_esc(confirmation_copy)}</span></label>
        <div class="app-form-grid__actions"><button class="admin-btn" type="submit">{_esc(action_label)}</button></div>
      </form>""")
    if (
        contract["verified"]
        and contract["preparation_status"] == "approved"
        and can_prepare_signature
        and not signature
    ):
        actions.append(f"""<form class="app-form-grid" method="post" action="{CONTRACTS_URL}/{_esc(contract['id'])}/signature-readiness">
        <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
        <h3>Customer signature</h3>
        <p class="app-muted">Freeze the customer and approved agreement for the QuickBooks Contract Builder handoff. This creates no QuickBooks contract and sends no message.</p>
        <label class="app-confirmation"><input type="checkbox" required> <span>I confirm the customer name, email, and approved agreement are ready to freeze.</span></label>
        <div class="app-form-grid__actions"><button class="admin-btn" type="submit">Prepare signature request</button></div>
      </form>""")
    signature_next = {
        "prepared": (
            "in_review",
            f"REVIEW SIGNATURE {signature.get('id')}",
            "Submit signature request for review",
            "I confirm the frozen signer and agreement checksum are ready for formal review.",
        ),
        "in_review": (
            "approved",
            f"APPROVE SIGNATURE {signature.get('id')}",
            "Approve QuickBooks handoff",
            "I confirm this QuickBooks Contract Builder handoff has completed review. Approval still sends nothing.",
        ),
    }.get(str(signature.get("status") or ""))
    if contract["verified"] and signature and can_approve and signature_next:
        target, confirmation, action_label, confirmation_copy = signature_next
        actions.append(f"""<form class="app-form-grid" method="post" action="{CONTRACTS_URL}/{_esc(contract['id'])}/signature-readiness/transition">
        <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
        <input type="hidden" name="target_status" value="{_esc(target)}">
        <input type="hidden" name="confirmation" value="{_esc(confirmation)}">
        <h3>Customer signature</h3>
        <label class="app-confirmation"><input type="checkbox" required> <span>{_esc(confirmation_copy)}</span></label>
        <div class="app-form-grid__actions"><button class="admin-btn" type="submit">{_esc(action_label)}</button><span class="app-muted">Delivery remains {_esc(signature.get('delivery_status') or 'not sent')}.</span></div>
      </form>""")
    if (
        contract["verified"]
        and signature.get("status") == "approved"
        and has_document
    ):
        # Google Docs path: Agent copies the approved template and fills it in.
        # It never sends and never signs — the signature block comes from the
        # template untouched, and requesting the signature stays a Docs action.
        if google_doc_url:
            google_block = (
                f'<p class="app-muted">Draft created. Open it, check it reads '
                f'correctly, then use <strong>Tools &rarr; eSignature</strong> in '
                f'Google Docs to send it to '
                f'{_esc(signature.get("signer_email"))}.</p>'
                f'<div class="app-form-grid__actions">'
                f'<a class="admin-btn" href="{_esc(google_doc_url)}" target="_blank" rel="noopener">Open the contract Doc</a>'
                f'</div>'
            )
        elif google_doc_error:
            google_block = (
                f'<div class="app-alert app-alert--blocked"><p>{_esc(google_doc_error)}</p></div>'
            )
        else:
            google_block = (
                f'<p class="app-muted">Copies the approved template Doc and fills '
                f'in this booking. Nothing is sent and the signature block is left '
                f'exactly as the template has it.</p>'
                f'<form method="post" action="{CONTRACTS_URL}/{_esc(contract["id"])}/google-doc">'
                f'<input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">'
                f'<div class="app-form-grid__actions">'
                f'<button class="admin-btn" type="submit">Create the contract Doc</button>'
                f'</div></form>'
            )
        actions.append(
            '<section class="app-form-grid" aria-label="Google Docs contract draft">'
            '<h3>Signing copy in Google Docs</h3>' + google_block + '</section>'
        )
        actions.append(f"""<section class="app-form-grid" aria-label="QuickBooks contract handoff">
        <h3>Create in QuickBooks</h3>
        <ol class="app-muted">
          <li>Open <strong>Read contract</strong>, print it to PDF, and keep the displayed checksum with the file.</li>
          <li>In QuickBooks, open <strong>All apps → Customer Hub → Contracts</strong>, upload that PDF, place the signature and date fields, and send it to {_esc(signature.get("signer_email"))}.</li>
          <li>After QuickBooks completes the contract, download the signed PDF and e-sign certificate, calculate the PDF SHA-256, then record that evidence here.</li>
        </ol>
        <div class="app-form-grid__actions">
          <a class="admin-btn" href="{CONTRACTS_URL}/{_esc(contract['id'])}/document" target="_blank" rel="noopener">Open frozen contract</a>
          <a class="admin-btn admin-btn--secondary" href="https://qbo.intuit.com/" target="_blank" rel="noopener">Open QuickBooks</a>
        </div>
        <label class="app-field"><span>Copy-ready handoff manifest</span>
          <textarea rows="12" readonly>{_esc(handoff_manifest)}</textarea></label>
      </section>""")
        if signature.get("delivery_status") in {"not_sent", "failed"}:
            if signature.get("delivery_status") == "failed":
                actions.append(f"""<form class="app-form-grid" method="post" action="{CONTRACTS_URL}/{_esc(contract['id'])}/signature-readiness/recovery">
        <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
        <input type="hidden" name="target_status" value="not_sent">
        <h3>Retry QuickBooks handoff</h3>
        <p class="app-muted">Reset only Agent's manual handoff state. This does not create or resend a customer document.</p>
        <div class="app-form-grid__actions"><button class="admin-btn" type="submit">Mark ready to retry</button></div>
      </form>""")
            else:
                actions.append(f"""<form class="app-form-grid" method="post" action="{CONTRACTS_URL}/{_esc(contract['id'])}/signature-readiness/recovery">
        <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
        <input type="hidden" name="target_status" value="failed">
        <h3>Record a failed handoff</h3>
        <label class="app-field"><span>What failed</span>
          <textarea name="failure_reason" rows="3" required placeholder="QuickBooks error or recovery detail"></textarea></label>
        <div class="app-form-grid__actions"><button class="admin-btn admin-btn--secondary" type="submit">Record failure</button><span class="app-muted">No customer message is sent.</span></div>
      </form>""")
    payment_next = {
        "prepared": (
            "in_review",
            f"REVIEW PAYMENT {payment.get('id')}",
            "Submit payment request for review",
            "I confirm the requested amount and frozen payment terms are ready for review.",
        ),
        "in_review": (
            "approved",
            f"APPROVE PAYMENT {payment.get('id')}",
            "Approve payment request",
            "I confirm the amount, tax treatment, and payment terms have completed review.",
        ),
    }.get(str(payment.get("status") or ""))
    if contract["verified"] and payment and can_prepare_payment and payment_next:
        target, confirmation, action_label, confirmation_copy = payment_next
        actions.append(f"""<form class="app-form-grid" method="post" action="{CONTRACTS_URL}/{_esc(contract['id'])}/payments/transition">
        <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
        <input type="hidden" name="target_status" value="{_esc(target)}">
        <input type="hidden" name="confirmation" value="{_esc(confirmation)}">
        <h3>Payment request</h3>
        <label class="app-confirmation"><input type="checkbox" required> <span>{_esc(confirmation_copy)}</span></label>
        <div class="app-form-grid__actions"><button class="admin-btn" type="submit">{_esc(action_label)}</button></div>
      </form>""")
    if can_manage and contract["reservation_id"]:
        actions.append(f"""<form class="app-form-grid" method="post" action="/admin/building/reservations/{_esc(contract['reservation_id'])}/agreements">
        <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
        <h3>Record QuickBooks Contract Builder evidence</h3>
        <label class="app-field"><span>Signature state</span>
          <select name="status"><option value="draft">Draft</option><option value="sent">Sent</option><option value="signed">Signed</option><option value="voided">Voided</option></select></label>
        <label class="app-field"><span>Version</span>
          <input type="number" name="version" min="1" value="{_esc(contract['version'])}"></label>
        <label class="app-field"><span>Provider</span>
          <input name="provider" value="quickbooks_contract_builder" readonly></label>
        <label class="app-field"><span>Evidence reference</span>
          <input name="provider_reference" value="{_esc(contract['provider_reference'])}" placeholder="QuickBooks contract ID or reference"></label>
        <label class="app-field"><span>QuickBooks contract URL</span>
          <input type="url" name="document_url" value="{_esc(contract['document_url'])}" placeholder="Required once sent"></label>
        <label class="app-field"><span>E-sign certificate reference</span>
          <input name="esign_certificate_reference" placeholder="Required when signed"></label>
        <label class="app-field"><span>Signed PDF SHA-256</span>
          <input name="signed_document_checksum" minlength="64" maxlength="64" placeholder="Required when signed"></label>
        <div class="app-form-grid__actions">
          <button class="admin-btn" type="submit">Record evidence</button>
          <span class="app-muted">Records what QuickBooks already did. Agent sends nothing and does not infer a signature.</span>
        </div>
      </form>""")
    actions_section = (
        f'<section class="admin-panel"><h2>Actions</h2><div class="app-action-grid">{"".join(actions)}</div></section>'
        if actions
        else '<section class="admin-panel"><h2>Actions</h2><div class="app-state-panel">'
        "<h3>No action available</h3><p>Either this record is unverified or your "
        "role does not grant contract approval.</p></div></section>"
    )

    body = f"""<p class="app-backlink"><a href="{CONTRACTS_URL}">← All contracts</a></p>
    <header class="app-page-header">
      <div>
        <p class="app-eyebrow">Contract · v{_esc(contract['version'])}</p>
        <h1>{_esc(contract['customer_name'])}</h1>
        <p>{_esc(contract['contract_type_label'])} contract for {_esc(contract['space_name'])}, {_esc(_when(contract['starts_at'], with_time=False))}. Agent freezes the terms; QuickBooks Contract Builder is the signature and contract workspace.</p>
      </div>
      <div class="app-page-actions">
        {_status(contract['state_label'], contract['state_modifier'])}
        {f'<a class="admin-btn" href="{CONTRACTS_URL}/{_esc(contract["id"])}/document">Read contract</a>' if has_document else ''}
        {f'<a class="admin-btn" href="{_esc(google_doc_url)}" target="_blank" rel="noopener">Open Google Doc</a>' if google_doc_url else ''}
      </div>
    </header>
    {_messages(notice, error)}
    <div class="app-metric-strip">
      {_metric(_money(contract['amount_cents'], contract['currency']), "Contract value")}
      {_metric(_money(int(payment.get('amount_cents') or 0), str(payment.get('currency') or contract['currency'])), "Required payment")}
      {_metric(_status(contract['state_label'], contract['state_modifier']), "Contract state")}
      {_metric(_status(contract['payment_label'], contract['payment_modifier']), "Payment request")}
      {_metric(
        _status(
          "Ready" if signature.get("status") == "approved" else (
            "Needs review" if signature.get("status") == "in_review" else (
              "Queued" if signature.get("status") == "prepared" else "Missing"
            )
          ),
          "ready" if signature.get("status") == "approved" else (
            "review" if signature.get("status") == "in_review" else (
              "queued" if signature.get("status") == "prepared" else "blocked"
            )
          ),
        ),
        "Signature request",
      )}
    </div>
    <section class="admin-panel"><h2>Frozen terms</h2>{_terms(contract['snapshot'], payment)}</section>
    <section class="admin-panel"><h2>Template/package comparison</h2>{template_comparison}</section>
    <section class="admin-panel"><h2>Signature handoff</h2>{
      (
        '<dl class="app-detail-list">'
        f'<div class="app-detail-list__row"><dt>Signer</dt><dd>{_esc(signature.get("signer_name"))}<span class="app-table__sub">{_esc(signature.get("signer_email"))}</span></dd></div>'
        f'<div class="app-detail-list__row"><dt>Review state</dt><dd>{_esc(str(signature.get("status") or "").replace("_", " ").title())}</dd></div>'
        f'<div class="app-detail-list__row"><dt>Provider</dt><dd>QuickBooks Contract Builder</dd></div>'
        f'<div class="app-detail-list__row"><dt>Delivery</dt><dd>{_esc(str(signature.get("delivery_status") or "not_sent").replace("_", " ").title())} — Agent does not claim a QuickBooks request exists without recorded evidence.</dd></div>'
        f'<div class="app-detail-list__row"><dt>Frozen checksum</dt><dd><code>{_esc(signature.get("checksum"))}</code></dd></div>'
        '</dl>'
        if signature
        else '<div class="app-state-panel"><h3>Not prepared</h3><p>Approve the agreement package, then freeze the customer signer for QuickBooks Contract Builder. Agent cannot create or send that QuickBooks contract through an API.</p></div>'
      )
    }</section>
    <section class="admin-panel"><h2>Evidence</h2>{evidence}</section>
    <section class="admin-panel"><h2>Linked records</h2><dl class="app-detail-list">{linked_rows}</dl></section>
    {actions_section}
    <section class="admin-panel"><h2>Audit history</h2>
      <div class="app-data-workspace"><table class="app-table">
        <thead><tr><th>When</th><th>Action</th><th>Actor</th><th>Record</th></tr></thead>
        <tbody>{audit_rows}</tbody></table></div>
    </section>"""
    return render_operator_document(
        title=f"{contract['customer_name']} · Contracts · Anata Agent",
        navigation=navigation,
        body=body,
        page_class="building-contracts-page",
        extra_head=_CONTRACT_STYLES,
    )


_CONTRACT_STYLES = """<style>
.building-contracts-page .app-eyebrow{margin:0;color:var(--agent-ink-muted);font:700 .75rem/1.3 "Montserrat",sans-serif;letter-spacing:.06em;text-transform:uppercase;}
.building-contracts-page .app-backlink{margin:0 0 12px;}
.building-contracts-page .app-muted{color:var(--agent-ink-muted);font-size:13px;}
.building-contracts-page .app-table__sub{display:block;margin-top:3px;color:var(--agent-ink-muted);font-size:12px;}
.building-contracts-page .app-command-bar__group{display:flex;flex-wrap:wrap;align-items:end;gap:10px;}
.building-contracts-page .app-command-bar__count{margin:0;color:var(--agent-ink-muted);font-size:13px;}
.building-contracts-page .admin-panel{margin:18px 0;}
.building-contracts-page .admin-panel>h2{margin:0 0 12px;font:800 1.1rem/1.2 "Montserrat",sans-serif;}
.building-contracts-page .app-detail-list{display:grid;gap:0;margin:0;}
.building-contracts-page .app-detail-list__row{display:grid;grid-template-columns:minmax(140px,220px) minmax(0,1fr);gap:16px;padding:10px 0;border-bottom:1px solid var(--agent-border);}
.building-contracts-page .app-detail-list__row:last-child{border-bottom:0;}
.building-contracts-page .app-detail-list dt{color:var(--agent-ink-muted);font:700 .75rem/1.5 "Montserrat",sans-serif;letter-spacing:.04em;text-transform:uppercase;}
.building-contracts-page .app-detail-list dd{margin:0;overflow-wrap:anywhere;}
.building-contracts-page .app-action-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:20px;}
.building-contracts-page .app-action-grid h3{margin:0;grid-column:1/-1;font:800 .95rem/1.2 "Montserrat",sans-serif;}
.building-contracts-page .app-confirmation{display:flex;align-items:flex-start;gap:10px;grid-column:1/-1;padding:12px 14px;border:1px solid var(--agent-border);border-radius:var(--agent-radius-control);background:var(--agent-surface-soft);}
.building-contracts-page .app-confirmation input{width:18px;height:18px;flex:0 0 auto;}
</style>"""
