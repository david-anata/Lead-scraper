"""Operator pages for authoring and approving contract templates.

Authoring produces contract text inside Agent. It never sends a contract,
requests a signature, creates an invoice, or charges a card. Approved versions
are immutable; revising one means starting the next version.
"""

from __future__ import annotations

import html
from typing import Any, Optional

from sales_support_agent.services.building_contract_templates import (
    CONTRACT_TYPES,
    MERGE_FIELD_HELP,
    merge_fields_for,
    render_document_html,
)
from sales_support_agent.services.ui_shell import render_operator_document


TEMPLATES_URL = "/admin/building/contracts/templates"
CONTRACTS_URL = "/admin/building/contracts"
#: Blank clause slots offered beyond the ones already authored.
SPARE_CLAUSE_SLOTS = 2

_TEMPLATE_STATES = {
    "draft": ("Queued", "queued"),
    "in_review": ("Needs review", "review"),
    "approved": ("Confirmed", "confirmed"),
    "retired": ("Stale", "stale"),
}


def _esc(value: Any) -> str:
    return html.escape(str(value if value is not None else ""))


def _status(status: str) -> str:
    label, modifier = _TEMPLATE_STATES.get(str(status), ("Stale", "stale"))
    return f'<span class="app-status app-status--{modifier}">{_esc(label)}</span>'


def _alert(kind: str, body: str) -> str:
    return f'<div class="app-alert app-alert--{_esc(kind)}">{body}</div>'


def _messages(notice: str, error: str) -> str:
    blocks = []
    if notice:
        blocks.append(_alert("notice", f"<p>{_esc(notice)}</p>"))
    if error:
        blocks.append(_alert("error", f"<p>{_esc(error)}</p>"))
    return "".join(blocks)


def _merge_field_palette(contract_type: str) -> str:
    items = "".join(
        f"<li><code>{{{{{_esc(field)}}}}}</code>"
        f'<span class="app-table__sub">{_esc(MERGE_FIELD_HELP.get(field, ""))}</span></li>'
        for field in merge_fields_for(contract_type)
    )
    return (
        '<div class="template-palette"><h3>Merge fields</h3>'
        "<p class=\"app-muted\">Paste a token into the body or a clause. Every "
        "token must appear in this list or the draft will not save.</p>"
        f"<ul>{items}</ul></div>"
    )


def render_template_index(
    *,
    navigation: str,
    templates: list[dict[str, Any]],
    can_author: bool,
    csrf_token: str,
    notice: str = "",
    error: str = "",
) -> str:
    """Template registry: what exists, what is approved, what can be authored."""

    if templates:
        rows = "".join(
            f"""<tr>
              <td><a href="{TEMPLATES_URL}/{_esc(item['id'])}"><strong>{_esc(item['name'])}</strong></a>
                <div class="app-table__sub">{_esc(item['template_key'])} · v{_esc(item['version'])}</div></td>
              <td>{_esc(item['contract_type'].title())}</td>
              <td>{_status(item['status'])}</td>
              <td>{'Authored in Agent' if item['body_markdown'] or item['clauses'] else 'External reference'}
                <div class="app-table__sub">{_esc(len(item['clauses']))} clauses</div></td>
              <td>{_esc(', '.join(item['merge_fields']) or '—')}</td>
              <td>{_esc(item['approved_by'] or '—')}</td>
            </tr>"""
            for item in templates
        )
        workspace = (
            '<div class="app-data-workspace"><table class="app-table">'
            "<thead><tr><th>Template</th><th>Type</th><th>State</th><th>Source</th>"
            "<th>Merge fields</th><th>Approved by</th></tr></thead>"
            f"<tbody>{rows}</tbody></table></div>"
        )
    else:
        workspace = (
            '<div class="app-state-panel"><h2>No templates yet</h2>'
            "<p>Contract preparation stays blocked until one reusable template "
            "is authored, reviewed, and approved. A customer-specific agreement "
            "is evidence, not a template.</p></div>"
        )

    if can_author:
        type_options = "".join(
            f'<option value="{_esc(item)}">{_esc(item.title())}</option>'
            for item in CONTRACT_TYPES
        )
        create = f"""<section class="admin-panel"><h2>Start a template version</h2>
      <form class="app-form-grid" method="post" action="{TEMPLATES_URL}">
        <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
        <label class="app-field"><span>Template key</span>
          <input name="template_key" required placeholder="event-agreement"></label>
        <label class="app-field"><span>Name</span>
          <input name="name" required placeholder="Event agreement"></label>
        <label class="app-field"><span>Contract type</span>
          <select name="contract_type">{type_options}</select></label>
        <div class="app-form-grid__actions">
          <button class="admin-btn" type="submit">Create draft</button>
          <span class="app-muted">The next unused version number is assigned automatically.</span>
        </div>
      </form></section>"""
    else:
        create = (
            '<section class="admin-panel"><h2>Start a template version</h2>'
            '<div class="app-state-panel"><h3>Not permitted</h3>'
            "<p>Authoring requires <code>building.agreements.prepare</code>.</p>"
            "</div></section>"
        )

    body = f"""<p class="app-backlink"><a href="{CONTRACTS_URL}">← All contracts</a></p>
    <header class="app-page-header">
      <div>
        <p class="app-eyebrow">Building · Contracts</p>
        <h1>Templates</h1>
        <p>Reusable, versioned contract text. Approved versions are immutable and are the only versions a contract package can be prepared against.</p>
      </div>
    </header>
    {_messages(notice, error)}
    {workspace}
    {create}"""
    return render_operator_document(
        title="Contract templates · Building · Anata Agent",
        navigation=navigation,
        body=body,
        page_class="building-contracts-page",
        extra_head=_TEMPLATE_STYLES,
    )


def render_template_editor(
    *,
    navigation: str,
    template: dict[str, Any],
    preview_options: list[dict[str, str]],
    preview_reservation_id: str,
    preview_html: str,
    preview_values: list[tuple[str, str]],
    preview_error: str,
    can_author: bool,
    can_approve: bool,
    csrf_token: str,
    notice: str = "",
    error: str = "",
) -> str:
    """Author, preview, and move one template version through its lifecycle."""

    editable = template["editable"] and can_author
    clauses = list(template["clauses"])
    slots = clauses + [
        {"title": "", "body": ""} for _ in range(SPARE_CLAUSE_SLOTS)
    ]
    clause_fields = "".join(
        f"""<fieldset class="template-clause">
          <legend>Clause {index + 1}</legend>
          <label class="app-field"><span>Title</span>
            <input name="clause_title_{index}" value="{_esc(item['title'])}"{'' if editable else ' readonly'}></label>
          <label class="app-field"><span>Body</span>
            <textarea name="clause_body_{index}" rows="4"{'' if editable else ' readonly'}>{_esc(item['body'])}</textarea></label>
        </fieldset>"""
        for index, item in enumerate(slots)
    )

    if editable:
        editor = f"""<form method="post" action="{TEMPLATES_URL}/{_esc(template['id'])}">
        <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
        <div class="app-form-grid">
          <label class="app-field"><span>Name</span>
            <input name="name" value="{_esc(template['name'])}" required></label>
          <label class="app-field"><span>Contract type</span>
            <select name="contract_type">{''.join(
              f'<option value="{_esc(item)}"{" selected" if item == template["contract_type"] else ""}>{_esc(item.title())}</option>'
              for item in CONTRACT_TYPES
            )}</select></label>
          <label class="app-field"><span>External reference (optional)</span>
            <input name="template_reference" value="{_esc(template['template_reference'])}" placeholder="approved-repository:event-agreement-v1"></label>
        </div>
        <label class="app-field"><span>Contract body (Markdown)</span>
          <textarea name="body_markdown" rows="18" placeholder="This agreement is made between Anata Building and {{{{customer_name}}}} for {{{{event_space}}}}...">{_esc(template['body_markdown'])}</textarea></label>
        <div class="template-clauses">{clause_fields}</div>
        <div class="app-form-grid__actions">
          <button class="admin-btn" type="submit">Save draft</button>
          <span class="app-muted">Saving validates every merge token. Nothing is sent.</span>
        </div>
      </form>"""
    else:
        reason = {
            "in_review": (
                "This version is locked while legal and owner review is underway. "
                "Approval evidence is required before it can back a customer contract."
            ),
            "approved": (
                "This approved version is locked. Start the next version to revise it."
            ),
            "retired": (
                "This retired version is locked and cannot be used for new contracts."
            ),
        }.get(
            template["status"],
            "Your role can review this version but cannot edit its contract text.",
        )
        preview_body = template["body_markdown"] or "(no body text)"
        review_html = render_document_html(preview_body)
        editor = (
            f'<div class="app-alert app-alert--blocked"><p>{reason}</p></div>'
            f'<article class="template-preview template-review">{review_html}</article>'
            f'<details class="template-source"><summary>View source text</summary>'
            f'<pre class="template-frozen">{_esc(preview_body)}</pre></details>'
            + "".join(
                f'<h3>{_esc(item["title"])}</h3><pre class="template-frozen">{_esc(item["body"])}</pre>'
                for item in clauses
            )
        )

    if preview_error:
        preview_panel = _alert("blocked", f"<p>{_esc(preview_error)}</p>")
    elif preview_html:
        sourced = "".join(
            f'<div class="app-detail-list__row"><dt>{_esc(field)}</dt><dd>{_esc(value)}</dd></div>'
            for field, value in preview_values
        )
        preview_panel = (
            f'<div class="template-preview">{preview_html}</div>'
            f'<h3>Merge values used</h3><dl class="app-detail-list">{sourced}</dl>'
        )
    else:
        preview_panel = (
            '<div class="app-state-panel"><h3>No preview yet</h3>'
            "<p>Choose a booking to render this template against real, current "
            "values. Preview uses the same merge logic a prepared package "
            "freezes.</p></div>"
        )
    reservation_options = "".join(
        f'<option value="{_esc(item["id"])}"{" selected" if item["id"] == preview_reservation_id else ""}>'
        f'{_esc(item["label"])}</option>'
        for item in preview_options
    ) or '<option value="">No eligible bookings</option>'

    lifecycle = ""
    if can_approve and template["status"] in {"draft", "in_review", "approved"}:
        next_states = {
            "draft": [("in_review", "In review")],
            "in_review": [("approved", "Approved")],
            "approved": [("retired", "Retired")],
        }[template["status"]]
        target_status, _ = next_states[0]
        verb = {"draft": "IN_REVIEW", "in_review": "APPROVED", "approved": "RETIRED"}[template["status"]]
        action_label = {
            "draft": "Submit for review",
            "in_review": "Approve template",
            "approved": "Retire template",
        }[template["status"]]
        confirmation_copy = {
            "draft": "I confirm this draft is complete enough for formal review.",
            "in_review": "I confirm the complete contract received the required legal and owner approval.",
            "approved": "I confirm this version must no longer be used for new contracts.",
        }[template["status"]]
        lifecycle = f"""<form class="app-form-grid" method="post" action="{TEMPLATES_URL}/{_esc(template['id'])}/transition">
        <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
        <input type="hidden" name="target_status" value="{_esc(target_status)}">
        <input type="hidden" name="confirmation" value="{verb} TEMPLATE {_esc(template['id'])}">
        <label class="app-field"><span>Approval evidence</span>
          <input name="evidence" value="{_esc(template['approval_evidence'])}" placeholder="Counsel approval, signed memo, or review reference"></label>
        <label class="app-confirmation"><input type="checkbox" required> <span>{_esc(confirmation_copy)}</span></label>
        <div class="app-form-grid__actions"><button class="admin-btn" type="submit">{_esc(action_label)}</button></div>
      </form>"""
    new_version = ""
    if can_author and template["status"] in {"approved", "retired"}:
        new_version = f"""<form method="post" action="{TEMPLATES_URL}/{_esc(template['id'])}/new-version">
        <input type="hidden" name="_csrf_token" value="{_esc(csrf_token)}">
        <div class="app-form-grid__actions">
          <button class="admin-btn admin-btn--ghost" type="submit">Start version {_esc(int(template['version']) + 1)}</button>
          <span class="app-muted">Copies this text into a new editable draft.</span>
        </div>
      </form>"""

    merge_palette = _merge_field_palette(template["contract_type"]) if editable else ""
    layout_class = "template-layout" if editable else "template-layout template-layout--review"
    body = f"""<p class="app-backlink"><a href="{TEMPLATES_URL}">← All templates</a></p>
    <header class="app-page-header">
      <div>
        <p class="app-eyebrow">Template · {_esc(template['template_key'])} v{_esc(template['version'])}</p>
        <h1>{_esc(template['name'])}</h1>
        <p>{_esc(template['contract_type'].title())} contract text. Only an approved version can back a prepared contract package.</p>
      </div>
      <div class="app-page-actions">{_status(template['status'])}</div>
    </header>
    {_messages(notice, error)}
    <div class="{layout_class}">
      <section class="admin-panel"><h2>Contract text</h2>{editor}</section>
      {merge_palette}
    </div>
    <section class="admin-panel"><h2>Preview against a real booking</h2>
      <form class="app-command-bar" method="get" action="{TEMPLATES_URL}/{_esc(template['id'])}">
        <div class="app-command-bar__group">
          <label class="app-field app-field--inline"><span>Booking</span>
            <select name="preview">{reservation_options}</select></label>
          <button class="admin-btn admin-btn--ghost" type="submit">Render preview</button>
        </div>
      </form>
      {preview_panel}
    </section>
    <section class="admin-panel"><h2>Lifecycle</h2>
      {lifecycle or '<p class="app-muted">Your role does not include contract-template approval.</p>'}
      {new_version}
    </section>"""
    return render_operator_document(
        title=f"{template['name']} · Templates · Anata Agent",
        navigation=navigation,
        body=body,
        page_class="building-contracts-page",
        extra_head=_TEMPLATE_STYLES,
    )


def render_contract_document(
    *,
    contract: dict[str, Any],
    document_html: str,
    checksum: str,
) -> str:
    """Print-optimized rendering of the frozen contract text.

    Read-only. The operator prints or saves this from the browser; Agent does
    not send it, request a signature on it, or bill against it.
    """

    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{_esc(contract['customer_name'])} · Contract</title>
<style>
:root{{color-scheme:light;}}
body{{margin:0;padding:48px 24px;background:#f9f7f3;color:#2b3644;font:16px/1.6 "Inter",system-ui,sans-serif;}}
article{{max-width:760px;margin:0 auto;padding:56px 64px;background:#fff;border:1px solid #dfe3e6;border-radius:8px;}}
h1{{margin:0 0 24px;font:800 1.9rem/1.2 "Montserrat",sans-serif;letter-spacing:-.03em;}}
h2{{margin:32px 0 10px;font:800 1.15rem/1.3 "Montserrat",sans-serif;}}
h3{{margin:24px 0 8px;font:800 1rem/1.3 "Montserrat",sans-serif;}}
p,li{{margin:0 0 12px;}}
table{{width:100%;border-collapse:collapse;margin:16px 0;}}
th,td{{padding:8px 10px;border:1px solid #dfe3e6;text-align:left;}}
.meta{{max-width:760px;margin:0 auto 20px;display:flex;flex-wrap:wrap;gap:12px;justify-content:space-between;align-items:center;}}
.meta a{{color:#2b3644;}}
.evidence{{max-width:760px;margin:20px auto 0;color:#5d6977;font-size:13px;word-break:break-all;}}
@media print{{body{{padding:0;background:#fff;}}article{{border:0;padding:0;border-radius:0;}}.meta{{display:none;}}}}
</style></head>
<body>
<div class="meta">
  <a href="{CONTRACTS_URL}/{_esc(contract['id'])}">← Back to contract</a>
  <span>Print or save as PDF from your browser.</span>
</div>
<article>{document_html}</article>
<p class="evidence">Frozen document checksum {_esc(checksum)}. This page renders
recorded evidence only. Agent has not sent this contract, requested a signature,
created an invoice, or charged a card.</p>
</body></html>"""


_TEMPLATE_STYLES = """<style>
.building-contracts-page .app-eyebrow{margin:0;color:var(--agent-ink-muted);font:700 .75rem/1.3 "Montserrat",sans-serif;letter-spacing:.06em;text-transform:uppercase;}
.building-contracts-page .app-backlink{margin:0 0 12px;}
.building-contracts-page .app-muted{color:var(--agent-ink-muted);font-size:13px;}
.building-contracts-page .app-table__sub{display:block;margin-top:3px;color:var(--agent-ink-muted);font-size:12px;}
.building-contracts-page .app-command-bar__group{display:flex;flex-wrap:wrap;align-items:end;gap:10px;}
.building-contracts-page .admin-panel{margin:18px 0;}
.building-contracts-page .admin-panel>h2{margin:0 0 12px;font:800 1.1rem/1.2 "Montserrat",sans-serif;}
.building-contracts-page .app-detail-list{display:grid;gap:0;margin:0;}
.building-contracts-page .app-detail-list__row{display:grid;grid-template-columns:minmax(140px,220px) minmax(0,1fr);gap:16px;padding:10px 0;border-bottom:1px solid var(--agent-border);}
.building-contracts-page .app-detail-list__row:last-child{border-bottom:0;}
.building-contracts-page .app-detail-list dt{color:var(--agent-ink-muted);font:700 .75rem/1.5 "Montserrat",sans-serif;letter-spacing:.04em;text-transform:uppercase;}
.building-contracts-page .app-detail-list dd{margin:0;overflow-wrap:anywhere;}
.building-contracts-page .template-layout{display:grid;grid-template-columns:minmax(0,1fr) minmax(240px,300px);gap:18px;align-items:start;}
.building-contracts-page .template-layout--review{grid-template-columns:minmax(0,1fr);}
.building-contracts-page .template-palette{position:sticky;top:16px;padding:18px 20px;border:1px solid var(--agent-border);border-radius:var(--agent-radius-panel);background:var(--agent-surface);}
.building-contracts-page .template-palette h3{margin:0 0 8px;font:800 .95rem/1.2 "Montserrat",sans-serif;}
.building-contracts-page .template-palette ul{margin:0;padding:0;list-style:none;display:grid;gap:8px;}
.building-contracts-page .template-palette code{font-size:12px;word-break:break-all;}
.building-contracts-page .template-clauses{display:grid;gap:14px;margin:16px 0;}
.building-contracts-page .template-clause{display:grid;gap:10px;margin:0;padding:14px;border:1px dashed var(--agent-border);border-radius:var(--agent-radius-panel);}
.building-contracts-page .template-clause legend{padding:0 6px;color:var(--agent-ink-muted);font:700 .7rem/1.3 "Montserrat",sans-serif;letter-spacing:.06em;text-transform:uppercase;}
.building-contracts-page .template-frozen{margin:0 0 14px;padding:14px;border:1px solid var(--agent-border);border-radius:var(--agent-radius-control);background:var(--agent-surface-soft);white-space:pre-wrap;font:13px/1.6 ui-monospace,SFMono-Regular,Menlo,monospace;}
.building-contracts-page .template-preview{padding:24px 28px;border:1px solid var(--agent-border);border-radius:var(--agent-radius-panel);background:var(--agent-surface);}
.building-contracts-page .template-preview table{width:100%;border-collapse:collapse;}
.building-contracts-page .template-preview th,.building-contracts-page .template-preview td{padding:8px 10px;border:1px solid var(--agent-border);text-align:left;}
.building-contracts-page .template-review{max-height:760px;overflow:auto;line-height:1.65;}
.building-contracts-page .template-review h1{font-size:1.55rem;}
.building-contracts-page .template-review h2{margin-top:1.6rem;font-size:1.15rem;}
.building-contracts-page .template-source{margin-top:14px;}
.building-contracts-page .template-source>summary{cursor:pointer;color:var(--agent-ink-muted);font-weight:700;}
.building-contracts-page .app-confirmation{display:flex;align-items:flex-start;gap:10px;grid-column:1/-1;padding:12px 14px;border:1px solid var(--agent-border);border-radius:var(--agent-radius-control);background:var(--agent-surface-soft);}
.building-contracts-page .app-confirmation input{width:18px;height:18px;flex:0 0 auto;}
.building-contracts-page textarea{font:13px/1.6 ui-monospace,SFMono-Regular,Menlo,monospace;}
@media(max-width:900px){.building-contracts-page .template-layout{grid-template-columns:1fr;}.building-contracts-page .template-palette{position:static;}}
</style>"""
