"""Sales Priorities — HubSpot-backed deal board controller.

Phase 0: a read-only deal board sorted top-down by close date, plus an
on-request background sync that refreshes the local HubSpot mirror. Tool-gated
by `sales.deals`. Lives under /admin/sales/* alongside the existing Sales
Priority Queue page (kept) and the gated Sales Assets feature.
"""

from __future__ import annotations

import json
import logging
import html

from urllib.parse import quote

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response

from sales_support_agent.integrations.hubspot import HubSpotAPIError, HubSpotClient
from sales_support_agent.models.database import session_scope
from sales_support_agent.models.entities import (
    HubSpotContact,
    HubSpotDeal,
    HubSpotDealContact,
    HubSpotLineItem,
    MailboxSignal,
)
from sales_support_agent.services.auth_deps import get_current_user, require_tool
from sales_support_agent.services.hubspot_sync.trigger import (
    hubspot_sync_status,
    start_hubspot_sync,
)
from sales_support_agent.services.sales.actions import ContactInfo, compute_pending_actions
from sales_support_agent.services.sales.deal_board import (
    build_deal_board,
    render_deal_board_page,
)
from sales_support_agent.services.sales.deal_detail import (
    build_deal_detail,
    render_deal_detail_page,
)
from sales_support_agent.services.sales.deal_create import (
    SalesDealRulesError,
    SelectOption,
    build_deal_associations,
    load_deal_create_options,
    mirror_created_deal,
    normalize_deal_create_request,
    read_sales_rules,
    validate_deal_create_request,
)
from sales_support_agent.services.sales import hubspot_links
from sales_support_agent.integrations.gmail import GmailClient
from sales_support_agent.services.sales.deal_batch import (
    build_batch_cleanup,
    record_note_applied,
    render_batch_cleanup_page,
)
from sales_support_agent.services.sales.email_send import send_followup_email
from sales_support_agent.services.sales.followup_draft import (
    _HOOK_ORDER,
    build_followup_draft,
    render_draft_followup_page,
    render_send_preview_page,
)
from sales_support_agent.services.sales.operator_dashboard import (
    get_operator_snapshot,
    render_operator_page,
    run_writeback,
)
from sales_support_agent.services.sales.rep_dashboard import (
    build_rep_dashboard,
    render_rep_dashboard_page,
)
from sales_support_agent.services.sales.slack_alerts import send_critical_deal_alerts

from sqlalchemy import func, select
from typing import Any, List

logger = logging.getLogger(__name__)


def _deal_not_found_page(request: Request) -> str:
    from sales_support_agent.services.admin_nav import (
        render_agent_favicon_links,
        render_agent_nav,
        render_agent_nav_styles,
    )
    nav_styles = render_agent_nav_styles()
    nav = render_agent_nav("sales", sales_section="sales_deals", user=get_current_user(request))
    favicons = render_agent_favicon_links()
    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <title>agent | Deal Not Found</title>
    {favicons}
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Montserrat:wght@700;800&display=swap" rel="stylesheet">
    <style>
      :root {{--dark-blue:#2B3644;--light-brown:#F9F7F3;--border:rgba(43,54,68,0.12);--shadow:rgba(43,54,68,0.10);--white:#FFF;}}
      *{{box-sizing:border-box;}} body{{margin:0;background:var(--light-brown);color:var(--dark-blue);font-family:"Inter","Segoe UI",sans-serif;}}
      a{{color:var(--dark-blue);}}
      {nav_styles}
      .shell{{max-width:1320px;margin:0 auto;padding:48px 24px;}}
      .workspace{{background:var(--white);border:1px solid var(--border);border-radius:20px;box-shadow:0 18px 40px var(--shadow);padding:32px 28px;}}
      h1{{font-family:"Montserrat",sans-serif;font-weight:800;font-size:24px;margin:0 0 10px;}}
    </style>
  </head>
  <body>
    {nav}
    <main class="shell">
      <div class="workspace">
        <h1>Deal not found</h1>
        <p style="color:rgba(43,54,68,0.7)">This deal doesn't exist in the local mirror. It may not have synced yet, or the link is stale.</p>
        <p><a href="/admin/sales/deals">← Back to Deal Board</a></p>
      </div>
    </main>
  </body>
</html>"""


def _render_sales_operator_unavailable(request: Request, message: str) -> str:
    from sales_support_agent.services.admin_nav import (
        render_agent_favicon_links,
        render_agent_nav,
        render_agent_nav_styles,
    )
    nav_styles = render_agent_nav_styles()
    nav = render_agent_nav("sales", sales_section="sales_operator", user=get_current_user(request))
    favicons = render_agent_favicon_links()
    safe_message = html.escape(str(message or "Sales Control Room is temporarily unavailable."))
    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <title>agent | Sales Control Room Unavailable</title>
    {favicons}
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Montserrat:wght@700;800&display=swap" rel="stylesheet">
    <style>
      :root {{--dark-blue:#2B3644;--light-brown:#F9F7F3;--border:rgba(43,54,68,0.12);--shadow:rgba(43,54,68,0.10);--white:#FFF;}}
      *{{box-sizing:border-box;}} body{{margin:0;background:var(--light-brown);color:var(--dark-blue);font-family:"Inter","Segoe UI",sans-serif;}}
      a{{color:var(--dark-blue);}}
      {nav_styles}
      .shell{{max-width:1320px;margin:0 auto;padding:48px 24px;}}
      .workspace{{background:var(--white);border:1px solid var(--border);border-radius:20px;box-shadow:0 18px 40px var(--shadow);padding:32px 28px;}}
      h1{{font-family:"Montserrat",sans-serif;font-weight:800;font-size:24px;margin:0 0 10px;}}
      .note{{margin:16px 0 0;padding:14px 16px;border-radius:14px;background:rgba(133,187,218,0.14);border:1px solid rgba(43,54,68,0.08);}}
    </style>
  </head>
  <body>
    {nav}
    <main class="shell">
      <div class="workspace">
        <h1>Sales Control Room unavailable</h1>
        <p style="color:rgba(43,54,68,0.7)">The HubSpot-backed sales operator layer could not load right now. The rest of the admin workspace is still available.</p>
        <div class="note">{safe_message}</div>
        <p style="margin-top:18px"><a href="/admin/settings">Open settings</a> · <a href="/admin/sales/deals">Open deal board</a></p>
      </div>
    </main>
  </body>
</html>"""


def _esc(value: object) -> str:
    return html.escape(str(value or ""))


def _wants_json(request: Request) -> bool:
    content_type = request.headers.get("content-type", "")
    accept = request.headers.get("accept", "")
    return "application/json" in content_type or "application/json" in accept


def _safe_internal_return_path(value: object) -> str:
    path = str(value or "").strip()
    if not path.startswith("/admin/") or path.startswith("//") or "\n" in path or "\r" in path:
        return ""
    return path


def _normalize_lookup(value: object) -> str:
    chars = []
    for char in str(value or "").lower():
        chars.append(char if char.isalnum() else " ")
    return " ".join("".join(chars).split())


def _domain_lookup(value: object) -> str:
    raw = str(value or "").lower().strip()
    raw = raw.removeprefix("https://").removeprefix("http://").removeprefix("www.")
    raw = raw.split("/", 1)[0].split("?", 1)[0].strip()
    return raw.removeprefix("www.")


def _deal_company_candidate(value: object) -> str:
    text = str(value or "")
    for suffix in (" fulfillment", " rate sheet", " 3pl", " deal"):
        if text.lower().endswith(suffix):
            text = text[: -len(suffix)]
    return text.strip()


def _option_text(option: SelectOption) -> str:
    return f"{option.label} {option.detail} {option.value}".strip()


def _match_option_by_context(options: tuple[SelectOption, ...], *, name: str = "", domain: str = "", email: str = "") -> tuple[str, str]:
    domain = _domain_lookup(domain)
    email = str(email or "").lower().strip()
    name_norm = _normalize_lookup(name)
    if domain:
        for option in options:
            if domain in _option_text(option).lower():
                return option.value, f"matched {option.label} by domain {domain}"
    if email:
        for option in options:
            if email in _option_text(option).lower():
                return option.value, f"matched {option.label} by email {email}"
    if len(name_norm) >= 3:
        for option in options:
            option_norm = _normalize_lookup(_option_text(option))
            if name_norm in option_norm:
                return option.value, f"matched {option.label} by name {name}"
    return "", ""


async def _deal_create_payload(request: Request) -> dict[str, Any]:
    if "application/json" in request.headers.get("content-type", ""):
        payload = await request.json()
        return dict(payload) if isinstance(payload, dict) else {}
    form = await request.form()
    return {str(key): str(value) for key, value in form.items()}


def _select_html(
    *,
    field_id: str,
    name: str,
    options: tuple[SelectOption, ...],
    selected: str = "",
    placeholder: str,
    required: bool = True,
) -> str:
    selected = str(selected or "").strip()
    seen_selected = False
    required_attr = " required" if required else ""
    rows = [f'<option value="">Select {placeholder}</option>']
    for opt in options:
        value = str(opt.value or "").strip()
        if not value:
            continue
        label = str(opt.label or value).strip()
        detail = str(opt.detail or "").strip()
        text = label if not detail else f"{label} - {detail}"
        selected_attr = ""
        if selected and selected == value:
            selected_attr = " selected"
            seen_selected = True
        rows.append(f'<option value="{_esc(value)}"{selected_attr}>{_esc(text)}</option>')
    if selected and not seen_selected:
        rows.append(f'<option value="{_esc(selected)}" selected>{_esc(selected)} - current value</option>')
    return f'<select id="{_esc(field_id)}" name="{_esc(name)}"{required_attr}>{"".join(rows)}</select>'


def _render_create_deal_page(
    request: Request,
    *,
    message: str = "",
    errors: list[str] | None = None,
    values: dict[str, Any] | None = None,
) -> str:
    from sales_support_agent.services.admin_nav import (
        render_agent_favicon_links,
        render_agent_nav,
        render_agent_nav_styles,
    )

    values = values or dict(request.query_params)
    errors = errors or []
    error_html = ""
    if errors:
        error_html = (
            '<div class="flash flash--warn"><strong>Fix before creating:</strong><ul>'
            + "".join(f"<li>{_esc(err)}</li>" for err in errors)
            + "</ul></div>"
        )
    elif message:
        error_html = f'<div class="flash flash--warn">{_esc(message)}</div>'

    settings = _sales_settings(request)
    rules_warning = ""
    try:
        rules = read_sales_rules()
    except SalesDealRulesError as exc:
        rules = {}
        rules_warning = str(exc)
    options = load_deal_create_options(settings, rules)
    warning_html = ""
    option_warnings = list(options.warnings)
    if rules_warning:
        option_warnings.insert(0, rules_warning)
    if option_warnings:
        warning_html = (
            '<div class="flash flash--soft"><strong>Dropdowns are partial:</strong><ul>'
            + "".join(f"<li>{_esc(item)}</li>" for item in option_warnings)
            + "</ul></div>"
        )

    def raw(key: str) -> str:
        return str(values.get(key, "") or "").strip()

    def v(key: str) -> str:
        return _esc(raw(key))

    return_to = _safe_internal_return_path(raw("return_to"))
    rate_sheet_run_id = raw("rate_sheet_run_id")
    company_context = raw("company_name") or raw("brand") or raw("prospect") or _deal_company_candidate(raw("dealname"))
    company_domain = raw("company_domain") or raw("website") or raw("website_url")
    contact_context = raw("contact_name")
    contact_email = raw("contact_email")

    selected_company_value = raw("company_id") or raw("hubspot_company_id")
    selected_contact_value = raw("contact_id") or raw("hubspot_contact_id")
    company_audit = ""
    contact_audit = ""
    if not selected_company_value:
        selected_company_value, company_audit = _match_option_by_context(
            options.companies,
            name=company_context,
            domain=company_domain,
        )
    if not selected_contact_value:
        selected_contact_value, contact_audit = _match_option_by_context(
            options.contacts,
            name=contact_context,
            email=contact_email,
        )
    audit_items = []
    if selected_company_value and company_audit:
        audit_items.append(f"Company auto-selected: {company_audit}.")
    elif company_context or company_domain:
        audit_items.append("Company not auto-selected: no accessible HubSpot company matched the rate sheet context.")
    if selected_contact_value and contact_audit:
        audit_items.append(f"Contact auto-selected: {contact_audit}.")
    elif contact_context or contact_email:
        audit_items.append("Contact not auto-selected: no accessible HubSpot contact matched the rate sheet context.")
    audit_html = (
        '<div class="flash flash--soft"><strong>Access audit</strong><ul>'
        + "".join(f"<li>{_esc(item)}</li>" for item in audit_items)
        + "</ul></div>"
        if audit_items else ""
    )
    context_hidden = "".join(
        f'<input type="hidden" name="{key}" value="{_esc(raw(key))}">'
        for key in ("company_name", "company_domain", "website", "website_url", "brand", "prospect", "contact_name", "contact_email")
        if raw(key)
    )

    fallback_pipeline = settings.hubspot_sales_pipeline_id or "default"
    selected_pipeline = raw("pipeline") or fallback_pipeline
    if selected_pipeline == "default" and options.pipelines:
        selected_pipeline = options.pipelines[0].value
    first_pipeline = next((p for p in options.pipelines if p.value == selected_pipeline), None)
    selected_stage = raw("dealstage") or ((first_pipeline.stages[0].value if first_pipeline and first_pipeline.stages else "") or "appointmentscheduled")
    selected_service = raw("anata_service_line") or ("fulfillment" if any(o.value == "fulfillment" for o in options.service_lines) else (options.service_lines[0].value if options.service_lines else "fulfillment"))
    selected_source = raw("anata_lead_source_detail") or ("agent" if any(o.value == "agent" for o in options.lead_sources) else (options.lead_sources[0].value if options.lead_sources else "agent"))

    pipeline_select = _select_html(
        field_id="pipeline",
        name="pipeline",
        options=tuple(options.pipelines),
        selected=selected_pipeline,
        placeholder="pipeline",
    )
    stage_select = _select_html(
        field_id="dealstage",
        name="dealstage",
        options=first_pipeline.stages if first_pipeline else (),
        selected=selected_stage,
        placeholder="deal stage",
    )
    service_select = _select_html(
        field_id="anata_service_line",
        name="anata_service_line",
        options=options.service_lines,
        selected=selected_service,
        placeholder="service line",
    )
    source_select = _select_html(
        field_id="anata_lead_source_detail",
        name="anata_lead_source_detail",
        options=options.lead_sources,
        selected=selected_source,
        placeholder="lead source",
    )
    owner_select = _select_html(
        field_id="hubspot_owner_id",
        name="hubspot_owner_id",
        options=options.owners,
        selected=raw("hubspot_owner_id"),
        placeholder="owner",
    )
    company_select = _select_html(
        field_id="company_id",
        name="company_id",
        options=options.companies,
        selected=selected_company_value,
        placeholder="company",
    )
    contact_select = _select_html(
        field_id="contact_id",
        name="contact_id",
        options=options.contacts,
        selected=selected_contact_value,
        placeholder="contact",
    )
    pipeline_stage_data = {
        pipeline.value: [
            {"value": stage.value, "label": stage.label, "detail": stage.detail}
            for stage in pipeline.stages
        ]
        for pipeline in options.pipelines
    }
    pipeline_stage_json = html.escape(json.dumps(pipeline_stage_data), quote=False)
    nav_styles = render_agent_nav_styles()
    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>agent | Create HubSpot Deal</title>
    {render_agent_favicon_links()}
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Montserrat:wght@700;800&display=swap" rel="stylesheet">
    <style>
      :root{{--dark-blue:#2B3644;--light-blue:#85BBDA;--light-brown:#F9F7F3;--white:#FFF;--border:rgba(43,54,68,0.12);--shadow:rgba(43,54,68,0.10);}}
      *{{box-sizing:border-box;}} body{{margin:0;background:var(--light-brown);color:var(--dark-blue);font-family:"Inter","Segoe UI",sans-serif;}} a{{color:var(--dark-blue);}}
      {nav_styles}
      .shell{{max-width:900px;margin:0 auto;padding:28px 18px 64px;}} .workspace{{background:var(--white);border:1px solid var(--border);border-radius:20px;box-shadow:0 18px 40px var(--shadow);padding:26px 28px 30px;}}
      h1{{font-family:"Montserrat",sans-serif;font-size:26px;margin:0 0 6px;}} .intro{{font-size:14px;color:rgba(43,54,68,.72);margin:0 0 18px;}}
      label{{display:block;font-weight:700;font-size:12px;margin:13px 0 5px;}} input,select,textarea{{width:100%;border:1px solid var(--border);border-radius:10px;padding:10px 12px;font:inherit;color:var(--dark-blue);background:#fff;}}
      .grid{{display:grid;grid-template-columns:1fr 1fr;gap:12px;}} .btn{{border:1px solid var(--dark-blue);background:var(--dark-blue);color:#fff;border-radius:10px;padding:10px 16px;font-weight:700;cursor:pointer;}} .btn--ghost{{background:#fff;color:var(--dark-blue);text-decoration:none;display:inline-block;}}
      .actions{{display:flex;gap:10px;align-items:center;margin-top:18px;flex-wrap:wrap;}} .flash{{border:1px solid rgba(178,59,59,.25);background:#fff4f4;color:#8a2424;border-radius:12px;padding:12px 14px;margin:0 0 16px;font-size:13px;}} .flash ul{{margin:6px 0 0;padding-left:18px;}}
      .flash--soft{{border-color:rgba(133,187,218,.45);background:rgba(133,187,218,.08);color:rgba(43,54,68,.78);}}
      .hint{{display:block;color:rgba(43,54,68,.55);font-size:11.5px;margin-top:4px;line-height:1.35;}}
      @media(max-width:720px){{.grid{{grid-template-columns:1fr;}}}}
    </style>
  </head>
  <body>
    {render_agent_nav("sales", sales_section="sales_deals", user=get_current_user(request))}
    <main class="shell">
      <div class="workspace">
        <p style="font-family:Montserrat,sans-serif;font-size:11px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:rgba(43,54,68,.55);margin:0 0 4px">Sales — HubSpot</p>
        <h1>Create Deal.</h1>
        <p class="intro">Creates a HubSpot deal only after validating required fields and company/contact associations from <code>config/hubspot_sales_rules.json</code>.</p>
        {error_html}
        {warning_html}
        {audit_html}
        <form method="post" action="/admin/sales/deals/create">
          <input type="hidden" name="return_to" value="{_esc(return_to)}">
          <input type="hidden" name="rate_sheet_run_id" value="{_esc(rate_sheet_run_id)}">
          {context_hidden}
          <label for="dealname">Deal name</label>
          <input id="dealname" name="dealname" value="{v('dealname')}" required>
          <div class="grid">
            <div><label for="pipeline">Pipeline</label>{pipeline_select}<span class="hint">Loaded from HubSpot pipelines.</span></div>
            <div><label for="dealstage">Deal stage</label>{stage_select}<span class="hint">Updates when the pipeline changes.</span></div>
          </div>
          <div class="grid">
            <div><label for="anata_service_line">Service line</label>{service_select}</div>
            <div><label for="anata_lead_source_detail">Lead source detail</label>{source_select}</div>
          </div>
          <div class="grid">
            <div><label for="hubspot_owner_id">Owner</label>{owner_select}<span class="hint">Shows name/email, submits HubSpot owner ID.</span></div>
            <div><label for="amount">Amount</label><input id="amount" name="amount" value="{v('amount')}" inputmode="decimal"></div>
          </div>
          <div class="grid">
            <div><label for="company_id">Company</label>{company_select}<span class="hint">Shows company/domain, submits HubSpot company ID.</span></div>
            <div><label for="contact_id">Contact</label>{contact_select}<span class="hint">Shows contact/email, submits HubSpot contact ID.</span></div>
          </div>
          <label for="closedate">Close date</label>
          <input id="closedate" name="closedate" value="{v('closedate')}" placeholder="YYYY-MM-DD">
          <div class="actions">
            <button class="btn" type="submit">Create HubSpot Deal</button>
            <a class="btn btn--ghost" href="{_esc(return_to or '/admin/sales/deals')}">Back</a>
          </div>
        </form>
      </div>
    </main>
    <script type="application/json" id="pipeline-stage-data">{pipeline_stage_json}</script>
    <script>
      (function(){{
        var pipeline=document.getElementById('pipeline');
        var stage=document.getElementById('dealstage');
        var dataEl=document.getElementById('pipeline-stage-data');
        if(!pipeline||!stage||!dataEl)return;
        var data={{}};
        try{{data=JSON.parse(dataEl.textContent||'{{}}');}}catch(e){{data={{}};}}
        function renderStages(){{
          var current=stage.value||stage.getAttribute('data-selected')||'';
          var rows=data[pipeline.value]||[];
          var html='<option value="">Select deal stage</option>';
          var found=false;
          rows.forEach(function(row){{
            var text=row.label||row.value;
            if(row.detail)text+=' - '+row.detail;
            var selected=current&&current===row.value;
            if(selected)found=true;
            html+='<option value="'+String(row.value).replace(/"/g,'&quot;')+'"'+(selected?' selected':'')+'>'+text.replace(/</g,'&lt;')+'</option>';
          }});
          if(current&&!found)html+='<option value="'+String(current).replace(/"/g,'&quot;')+'" selected>'+String(current).replace(/</g,'&lt;')+' - current value</option>';
          stage.innerHTML=html;
          if(!stage.value&&rows[0])stage.value=rows[0].value;
        }}
        stage.setAttribute('data-selected', stage.value||'');
        pipeline.addEventListener('change', function(){{stage.setAttribute('data-selected','');renderStages();}});
        renderStages();
      }})();
    </script>
  </body>
</html>"""


router = APIRouter(
    prefix="/admin/sales",
    tags=["sales-deals"],
    dependencies=[Depends(require_tool("sales.deals"))],
)


def _sales_settings(request: Request):
    """Return agent settings (has HubSpot/sales fields) regardless of entrypoint.

    Under sales_support_agent.main:app both app.state.settings and
    app.state.agent_settings are the agent settings. Under main:app only
    app.state.agent_settings is correct — app.state.settings is the root
    settings which lacks stale_deal_days, hubspot_portal_id, etc.

    If agent_settings is None or is the root Settings (no stale_deal_days),
    we load directly from the environment and cache the result so the next
    request is fast. This is safe to call on every request.
    """
    s = getattr(request.app.state, "agent_settings", None)
    if s is not None and hasattr(s, "stale_deal_days"):
        return s
    # agent_settings is None (startup exception) or root Settings.
    # Load the agent settings directly and cache so startup failures don't
    # silently serve the wrong settings object on every request.
    from sales_support_agent.config import load_settings as _load_agent_settings
    s = _load_agent_settings()
    request.app.state.agent_settings = s
    return s


@router.get("", response_class=HTMLResponse)
@router.get("/", response_class=HTMLResponse)
def sales_operator(request: Request) -> HTMLResponse:
    settings = _sales_settings(request)
    try:
        snapshot = get_operator_snapshot(settings, session_factory=request.app.state.session_factory)
        return HTMLResponse(render_operator_page(snapshot, user=get_current_user(request)))
    except Exception as exc:  # noqa: BLE001 - this route should surface blockers, not 500
        logger.exception("Sales operator page failed")
        return HTMLResponse(_render_sales_operator_unavailable(request, str(exc)), status_code=503)


@router.get("/snapshot")
def sales_operator_snapshot(request: Request) -> JSONResponse:
    settings = _sales_settings(request)
    try:
        snapshot = get_operator_snapshot(settings, session_factory=request.app.state.session_factory, force_refresh=True)
        return JSONResponse({"ok": True, "snapshot": snapshot})
    except Exception as exc:  # noqa: BLE001 - expose the setup/runtime blocker in JSON
        logger.exception("Sales operator snapshot failed")
        return JSONResponse({"ok": False, "error": str(exc) or "Sales operator unavailable."}, status_code=503)


@router.post("/writeback")
def sales_operator_writeback(
    request: Request,
    mode: str = Form(default="preview"),
    limit: str = Form(default="10"),
) -> HTMLResponse:
    settings = _sales_settings(request)
    try:
        parsed_limit = int(limit)
    except ValueError:
        parsed_limit = 10
    try:
        result = run_writeback(
            settings,
            session_factory=request.app.state.session_factory,
            mode=("apply" if mode == "apply" else "preview"),
            limit=parsed_limit,
        )
        snapshot = get_operator_snapshot(
            settings,
            session_factory=request.app.state.session_factory,
            force_refresh=(mode == "apply"),
        )
        message = "High-confidence sales write-back actions were applied." if mode == "apply" else "Sales write-back preview generated."
        return HTMLResponse(render_operator_page(snapshot, user=get_current_user(request), writeback=result, status_message=message))
    except Exception as exc:  # noqa: BLE001 - surface the blocker in-page instead of 500ing
        logger.exception("Sales operator write-back failed")
        return HTMLResponse(_render_sales_operator_unavailable(request, str(exc)), status_code=503)


@router.get("/deals/create", response_class=HTMLResponse)
def create_deal_form(request: Request) -> HTMLResponse:
    return HTMLResponse(_render_create_deal_page(request))


@router.post("/deals/create")
async def create_deal(request: Request) -> Response:
    settings = _sales_settings(request)
    payload: dict[str, Any] = {}
    try:
        payload = await _deal_create_payload(request)
        rules = read_sales_rules()
        deal_request = normalize_deal_create_request(payload, rules, settings=settings)
    except (ValueError, TypeError):
        if _wants_json(request):
            return JSONResponse({"ok": False, "error": "bad-request"}, status_code=400)
        return HTMLResponse(
            _render_create_deal_page(
                request,
                message="Deal creation request was incomplete.",
                values=payload,
            ),
            status_code=400,
        )
    except SalesDealRulesError as exc:
        logger.exception("[sales] deal create rules load failed")
        if _wants_json(request):
            return JSONResponse({"ok": False, "error": str(exc)}, status_code=500)
        return HTMLResponse(
            _render_create_deal_page(request, message=str(exc), values=payload),
            status_code=500,
        )

    validation_errors = validate_deal_create_request(deal_request, rules)
    if validation_errors:
        if _wants_json(request):
            return JSONResponse(
                {"ok": False, "error": "validation-failed", "errors": validation_errors},
                status_code=400,
            )
        return HTMLResponse(
            _render_create_deal_page(
                request,
                message="Deal creation request failed validation.",
                errors=validation_errors,
                values=payload,
            ),
            status_code=400,
        )

    client = HubSpotClient(settings)
    if not client.is_configured:
        msg = "HubSpot token is not configured. Set HUBSPOT_API_TOKEN or HUBSPOT_PRIVATE_APP_TOKEN in Render."
        if _wants_json(request):
            return JSONResponse({"ok": False, "error": msg}, status_code=503)
        return HTMLResponse(
            _render_create_deal_page(request, message=msg, errors=[msg], values=payload),
            status_code=503,
        )

    try:
        created = client.create_deal(
            deal_request.properties,
            associations=build_deal_associations(deal_request),
        )
    except HubSpotAPIError as exc:
        logger.exception("[sales] HubSpot deal create failed")
        msg = str(exc)[:240]
        if _wants_json(request):
            return JSONResponse(
                {"ok": False, "error": msg, "status_code": exc.status_code},
                status_code=502,
            )
        return HTMLResponse(
            _render_create_deal_page(request, message=msg, errors=[msg], values=payload),
            status_code=502,
        )

    deal_id = str(created.get("id") or "").strip()
    if deal_id:
        try:
            with session_scope(request.app.state.session_factory) as session:
                mirror_created_deal(session, created, deal_request)
        except Exception:
            logger.exception("[sales] local mirror insert failed for created HubSpot deal %s", deal_id)
        try:
            start_hubspot_sync(request.app, force=True)
        except Exception:
            logger.warning("[sales] post-create HubSpot sync failed to start")

    hubspot_url = hubspot_links.deal_url(settings.hubspot_portal_id or "", deal_id)
    return_to = _safe_internal_return_path(payload.get("return_to"))
    rate_sheet_run_id = str(payload.get("rate_sheet_run_id") or "").strip()
    if deal_id and rate_sheet_run_id:
        try:
            from sqlalchemy.orm import Session

            from sales_support_agent.models.database import get_engine
            from sales_support_agent.services.fulfillment_deck import storage as fulfillment_storage
            from sales_support_agent.services.fulfillment_deck.service import apply_profile_edits
            from sales_support_agent.services.sales.asset_linker import link_asset_to_deal

            run_id = int(rate_sheet_run_id)
            apply_profile_edits(
                run_id,
                {
                    "hubspot_deal_id": deal_id,
                    "hubspot_deal_url": hubspot_url,
                },
                settings=settings,
            )
            run = fulfillment_storage.get_run(run_id)
            summary = dict(run.summary_json or {}) if run is not None else {}
            view_path = str(summary.get("view_path") or "")
            if view_path:
                with Session(get_engine()) as session:
                    link_asset_to_deal(
                        session,
                        hubspot_deal_id=deal_id,
                        asset_type="rate_sheet",
                        run_id=run_id,
                        url=view_path,
                        label="Fulfillment Rate Sheet",
                    )
                    session.commit()
        except Exception:
            logger.exception("[sales] failed to attach created deal to fulfillment rate sheet")
    if _wants_json(request):
        return JSONResponse(
            {"ok": True, "deal_id": deal_id, "hubspot_url": hubspot_url, "deal": created},
            status_code=201,
        )
    return RedirectResponse(
        url=return_to or hubspot_url or (f"/admin/sales/deals/{deal_id}" if deal_id else "/admin/sales/deals"),
        status_code=303,
    )


@router.get("/deals", response_class=HTMLResponse)
def deal_board(request: Request, my: bool = False) -> HTMLResponse:
    import traceback
    try:
        # Kick a background refresh on load (non-blocking); render from the mirror.
        try:
            start_hubspot_sync(request.app, force=False)
        except Exception:  # noqa: BLE001 — a sync hiccup must not break the page
            logger.exception("[sales] failed to start hubspot sync")
        status = hubspot_sync_status(request.app)
        settings = _sales_settings(request)
        user = get_current_user(request)
        owner_filter = user.get("email") if (my and user) else None
        with session_scope(request.app.state.session_factory) as session:
            board = build_deal_board(
                session,
                owner_filter=owner_filter,
                stale_days=settings.stale_deal_days,
            )
        return HTMLResponse(
            render_deal_board_page(
                board,
                user=user,
                sync_status=status,
                show_my=my,
                portal_id=settings.hubspot_portal_id or "",
            )
        )
    except Exception as _exc:  # noqa: BLE001
        _tb = traceback.format_exc()
        logger.exception("[sales] deal_board route error")
        return HTMLResponse(
            f"<pre style='font-family:monospace;padding:2rem;white-space:pre-wrap'>"
            f"Deal Board Error — check Render logs for full context.\n\n{_tb}</pre>",
            status_code=500,
        )


@router.post("/deals/sync")
def trigger_sync(request: Request) -> RedirectResponse:
    start_hubspot_sync(request.app, force=True)
    return RedirectResponse(url="/admin/sales/deals", status_code=303)


@router.get("/deals/sync/status")
def sync_status(request: Request) -> JSONResponse:
    return JSONResponse(hubspot_sync_status(request.app))


@router.get("/deals/cleanup", response_class=HTMLResponse)
def batch_cleanup(
    request: Request,
    applied: int = 0,
    failed: int = 0,
    error: str = "",
) -> HTMLResponse:
    settings = _sales_settings(request)
    with session_scope(request.app.state.session_factory) as session:
        rows = build_batch_cleanup(session, portal_id=settings.hubspot_portal_id or "")
    return HTMLResponse(render_batch_cleanup_page(
        rows,
        user=get_current_user(request),
        applied=applied,
        failed=failed,
        error=error,
    ))


@router.post("/deals/cleanup")
def batch_cleanup_apply(
    request: Request,
    action_ids: List[str] = Form(default=[]),
) -> RedirectResponse:
    """Apply a batch of selected mid-confidence actions to HubSpot."""
    settings = request.app.state.agent_settings
    client = HubSpotClient(settings)
    if not client.is_configured:
        return RedirectResponse(
            url="/admin/sales/deals/cleanup?error=HubSpot+token+not+configured",
            status_code=303,
        )
    if not action_ids:
        return RedirectResponse(url="/admin/sales/deals/cleanup", status_code=303)

    action_id_set = set(action_ids)
    with session_scope(request.app.state.session_factory) as session:
        rows = build_batch_cleanup(session, portal_id=settings.hubspot_portal_id or "")

    # Flatten all actions across all deal rows and match by action_id.
    all_actions = [a for row in rows for a in row.actions]
    matched = [a for a in all_actions if a.action_id in action_id_set]

    applied = failed = 0
    for a in matched:
        # Flags have no writeable content — skip silently (shouldn't be submitted).
        if a.action_type == "flag":
            continue
        try:
            if a.action_type == "create_note":
                if a.note_body:
                    client.create_note(deal_id=a.hubspot_object_id, body=a.note_body)
                    record_note_applied(a.hubspot_object_id)
                    applied += 1
            elif a.action_type == "update_deal" and a.properties:
                client.update_deal(a.hubspot_object_id, a.properties)
                applied += 1
            elif a.action_type == "update_contact" and a.properties:
                client.update_contact(a.hubspot_object_id, a.properties)
                applied += 1
        except Exception:
            logger.exception("[sales] batch cleanup action failed: %s", a.action_id)
            failed += 1

    if applied:
        try:
            start_hubspot_sync(request.app, force=True)
        except Exception:
            logger.warning("[sales] post-cleanup sync failed to start")

    return RedirectResponse(
        url=f"/admin/sales/deals/cleanup?applied={applied}&failed={failed}",
        status_code=303,
    )


# Defined after the static /deals/* paths so {deal_id} can't shadow them.
@router.get("/deals/{deal_id}", response_class=HTMLResponse)
def deal_detail(
    request: Request,
    deal_id: str,
    actioned: str = "",
    sent: str = "",
    error: str = "",
) -> Response:
    settings = _sales_settings(request)
    flash = ""
    flash_ok = True
    if actioned:
        flash = "Done — action pushed to HubSpot. The board will refresh on next load."
    elif sent:
        flash = "Email sent and logged to HubSpot. Timeline will update on next sync."
    elif error:
        flash = f"Could not apply: {error}"
        flash_ok = False
    with session_scope(request.app.state.session_factory) as session:
        detail = build_deal_detail(session, deal_id, settings=settings)
        if detail is None:
            return HTMLResponse(_deal_not_found_page(request), status_code=404)
        html = render_deal_detail_page(
            detail, user=get_current_user(request), flash=flash, flash_ok=flash_ok
        )
    return HTMLResponse(html)


@router.post("/deals/{deal_id}/actions/approve")
def approve_action(
    request: Request,
    deal_id: str,
    action_id: str = Form(...),
) -> RedirectResponse:
    """Re-derive the action from current deal state and execute it against HubSpot."""
    settings = request.app.state.agent_settings
    client = HubSpotClient(settings)
    if not client.is_configured:
        return RedirectResponse(
            url=f"/admin/sales/deals/{deal_id}?error=HubSpot+token+not+configured",
            status_code=303,
        )

    with session_scope(request.app.state.session_factory) as session:
        deal = session.get(HubSpotDeal, deal_id)
        if deal is None:
            return RedirectResponse(url="/admin/sales/deals", status_code=303)
        signals = list(session.scalars(
            select(MailboxSignal).where(MailboxSignal.matched_deal_id == deal_id)
        ).all())
        li_total = session.execute(
            select(func.sum(HubSpotLineItem.amount_cents))
            .where(HubSpotLineItem.hubspot_deal_id == deal_id)
        ).scalar() or 0
        contact_link_ids = [
            r.hubspot_contact_id for r in session.scalars(
                select(HubSpotDealContact).where(HubSpotDealContact.hubspot_deal_id == deal_id)
            ).all()
        ]
        contacts = []
        for cid in contact_link_ids:
            c = session.get(HubSpotContact, cid)
            if c:
                contacts.append(ContactInfo(contact_id=cid, email=c.email or ""))
        actions = compute_pending_actions(
            deal, signals,
            line_item_total_cents=int(li_total),
            contacts=contacts,
            portal_id=settings.hubspot_portal_id or "",
        )

    action = next((a for a in actions if a.action_id == action_id), None)
    if action is None or action.action_type == "flag":
        return RedirectResponse(url=f"/admin/sales/deals/{deal_id}", status_code=303)

    try:
        if action.action_type == "create_note" and action.note_body:
            client.create_note(deal_id=action.hubspot_object_id, body=action.note_body)
            record_note_applied(action.hubspot_object_id)
        elif action.hubspot_object_type == "deals" and action.properties:
            client.update_deal(action.hubspot_object_id, action.properties)
        elif action.hubspot_object_type == "contacts" and action.properties:
            client.update_contact(action.hubspot_object_id, action.properties)
    except Exception as exc:  # noqa: BLE001
        logger.exception("[sales] action approve failed for %s", action_id)
        msg = quote(str(exc)[:120], safe="")
        return RedirectResponse(
            url=f"/admin/sales/deals/{deal_id}?error={msg}", status_code=303
        )

    try:
        start_hubspot_sync(request.app, force=True)
    except Exception:  # noqa: BLE001
        logger.warning("[sales] post-approve sync failed to start")

    return RedirectResponse(url=f"/admin/sales/deals/{deal_id}?actioned=1", status_code=303)


@router.get("/deals/{deal_id}/draft-followup", response_class=HTMLResponse)
def draft_followup(request: Request, deal_id: str) -> Response:
    settings = _sales_settings(request)
    with session_scope(request.app.state.session_factory) as session:
        detail = build_deal_detail(session, deal_id, settings=settings)
        if detail is None:
            return HTMLResponse("Deal not found.", status_code=404)

    hooks_sent = [a.asset_type for a in detail.assets]
    hooks_pending = [h for h in _HOOK_ORDER if h not in hooks_sent]
    contact_first = detail.contacts[0].name.split()[0] if detail.contacts else ""
    contact_emails = [c.email for c in detail.contacts if c.email]
    recent_subject = detail.timeline[0].title if detail.timeline else ""
    base_url = str(request.base_url).rstrip("/")
    asset_links = []
    for a in detail.assets:
        url = a.url or ""
        if url.startswith("/"):
            url = base_url + url
        if url:
            asset_links.append({"type": a.asset_type, "label": a.label, "url": url})
        if a.quote_url:
            asset_links.append({"type": "quote", "label": "HubSpot Quote", "url": a.quote_url})
    last_contact_at = ""
    last_contact_type = ""
    for label, value in (
        ("inbound", detail.last_inbound_at),
        ("outbound", detail.last_outbound_at),
        ("touch", detail.last_touch_at),
    ):
        if value and (not last_contact_at or value.isoformat() > last_contact_at):
            last_contact_at = value.isoformat()
            last_contact_type = label
    mailbox_snippets = [
        f"{entry.title}: {entry.detail}" for entry in detail.timeline if entry.kind == "inbound_email"
    ][:5]
    pending_actions = [a.label for a in detail.pending_actions[:6]]
    prospect_activity = ""
    pricing_summary = ""
    rate_sheet = next((a for a in detail.assets if a.asset_type == "rate_sheet"), None)
    if rate_sheet:
        prospect_activity = f"Fulfillment deck linked; {rate_sheet.external_views} prospect views."
        cost_state = (
            f"signed fulfillment costs by {rate_sheet.latest_cost_submitter or rate_sheet.latest_cost_email}"
            if rate_sheet.cost_submission_count else
            "fulfillment costs not signed yet"
        )
        quote_state = (
            "HubSpot quote ready"
            if rate_sheet.quote_url else
            ("quote blocked: " + rate_sheet.quote_blockers[0] if rate_sheet.quote_blockers else "quote ready to create")
        )
        margin_state = (
            f"margin {rate_sheet.margin_pct:.1f}%"
            if rate_sheet.margin_pct is not None else
            "margin not calculated"
        )
        pricing_summary = f"Rate sheet status: {cost_state}; {quote_state}; {margin_state}."

    draft = build_followup_draft(
        company_name=detail.company_name,
        contact_first_name=contact_first,
        owner_email=detail.owner_email,
        deal_name=detail.name,
        deal_amount_cents=detail.amount_cents,
        hooks_sent=hooks_sent,
        hooks_pending=hooks_pending,
        recent_subject=recent_subject,
        contact_emails=contact_emails,
        last_contact_at=last_contact_at,
        last_contact_type=last_contact_type,
        conversation_summary=detail.communication_summary,
        recommended_next_action=detail.recommended_next_action or detail.next_action,
        asset_links=asset_links,
        recent_mailbox_snippets=mailbox_snippets,
        pending_actions=pending_actions,
        prospect_activity=prospect_activity,
        pricing_summary=pricing_summary,
    )
    draft.gmail_configured = GmailClient(settings).is_configured()

    html = render_draft_followup_page(
        draft,
        deal_id=deal_id,
        deal_name=detail.name or deal_id,
        user=get_current_user(request),
    )
    return HTMLResponse(html)


@router.post("/deals/{deal_id}/send-followup")
def send_followup(
    request: Request,
    deal_id: str,
    subject: str = Form(...),
    body: str = Form(...),
    to_emails: str = Form(""),
    confirmed: str = Form(""),
) -> Response:
    """Two-step send: first POST shows preview; second POST (confirmed=1) sends."""
    settings = request.app.state.agent_settings
    gmail_client = GmailClient(settings)

    if not gmail_client.is_configured():
        return RedirectResponse(
            url=f"/admin/sales/deals/{deal_id}/draft-followup?error=Gmail+not+configured",
            status_code=303,
        )

    if not confirmed:
        # Step 1: show the confirmation preview page.
        with session_scope(request.app.state.session_factory) as session:
            deal = session.get(HubSpotDeal, deal_id)
            deal_name = deal.deal_name if deal else deal_id

        from_email = ""
        try:
            profile = gmail_client.get_profile()
            from_email = str(profile.get("emailAddress") or "").strip()
        except Exception:
            logger.warning("[sales] could not fetch Gmail profile for send preview")

        return HTMLResponse(
            render_send_preview_page(
                deal_id=deal_id,
                deal_name=deal_name,
                subject=subject,
                body=body,
                to_emails=to_emails,
                from_email=from_email,
                user=get_current_user(request),
            )
        )

    # Step 2: confirmed — execute the send.
    to_list = [e.strip() for e in to_emails.split(",") if e.strip()]
    if not to_list:
        return RedirectResponse(
            url=f"/admin/sales/deals/{deal_id}/draft-followup?error=No+recipients",
            status_code=303,
        )

    hubspot_client = HubSpotClient(settings)
    with session_scope(request.app.state.session_factory) as session:
        deal = session.get(HubSpotDeal, deal_id)
        if deal is None:
            return RedirectResponse(url="/admin/sales/deals", status_code=303)
        contact_ids = [
            r.hubspot_contact_id for r in session.scalars(
                select(HubSpotDealContact).where(HubSpotDealContact.hubspot_deal_id == deal_id)
            ).all()
        ]

    result = send_followup_email(
        gmail_client=gmail_client,
        hubspot_client=hubspot_client,
        deal_id=deal_id,
        contact_ids=contact_ids,
        to_emails=to_list,
        subject=subject,
        body_text=body,
    )

    if not result.ok:
        msg = quote(result.error[:120], safe="")
        return RedirectResponse(
            url=f"/admin/sales/deals/{deal_id}?error={msg}", status_code=303
        )

    try:
        with session_scope(request.app.state.session_factory) as session:
            deal = session.get(HubSpotDeal, deal_id)
            if deal is not None and result.sent_at is not None:
                deal.last_outbound_at = result.sent_at
                deal.last_meaningful_touch_at = result.sent_at
    except Exception:
        logger.warning("[sales] failed to update local outbound touch for %s", deal_id)

    try:
        start_hubspot_sync(request.app, force=True)
    except Exception:
        logger.warning("[sales] post-send sync failed to start")

    return RedirectResponse(
        url=f"/admin/sales/deals/{deal_id}?sent=1", status_code=303
    )


@router.get("/reps", response_class=HTMLResponse)
def rep_accountability(request: Request) -> HTMLResponse:
    user = get_current_user(request)
    settings = _sales_settings(request)
    with session_scope(request.app.state.session_factory) as session:
        dashboard = build_rep_dashboard(session, stale_days=settings.stale_deal_days)
    return HTMLResponse(render_rep_dashboard_page(dashboard, user=user))


@router.post("/deals/alerts/send")
def send_slack_alerts(request: Request) -> JSONResponse:
    settings = _sales_settings(request)
    with session_scope(request.app.state.session_factory) as session:
        result = send_critical_deal_alerts(session, settings)
    return JSONResponse(result)
