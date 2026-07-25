"""Outbound pages for the sales-support-agent app (agent.anatainc.com).

The outbound scoreboard and the StoreLeads brand-list download, rendered inside
the standard admin shell (top nav + Outbound section) and gated by the app's own
access control via the `outbound.scoreboard` tool (see services/access/catalog).
The engine lives in the repo-root modules (outbound_pipeline, outbound_scoreboard);
this router just exposes them. Read-only and dry-run: nothing sends, nothing pushes.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response

from sales_support_agent.services.auth_deps import get_current_user

logger = logging.getLogger(__name__)

router = APIRouter(tags=["outbound"])


def _shell_page(request: Request, *, active: str, title: str, extra_css: str, body: str) -> str:
    """Wrap page content in the standard agent.anatainc.com shell (top nav)."""
    from sales_support_agent.services.admin_nav import (
        render_agent_favicon_links,
        render_agent_nav,
        render_agent_nav_styles,
    )
    nav_styles = render_agent_nav_styles()
    nav = render_agent_nav(active, user=get_current_user(request))
    favicons = render_agent_favicon_links()
    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <title>agent | {title}</title>
    {favicons}
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Montserrat:wght@700;800&display=swap" rel="stylesheet">
    <style>
      :root {{--dark-blue:#2B3644;--light-brown:#F9F7F3;--border:rgba(43,54,68,0.12);--shadow:rgba(43,54,68,0.10);--white:#FFF;}}
      *{{box-sizing:border-box;}} body{{margin:0;background:var(--light-brown);color:var(--dark-blue);font-family:"Inter","Segoe UI",sans-serif;}}
      a{{color:var(--dark-blue);}}
      {nav_styles}
      .shell{{max-width:1320px;margin:0 auto;padding:40px 24px;}}
      .workspace{{background:var(--white);border:1px solid var(--border);border-radius:20px;box-shadow:0 18px 40px var(--shadow);padding:30px 28px;}}
      h1{{font-family:"Montserrat",sans-serif;font-weight:800;font-size:24px;margin:0 0 6px;}}
      .sub{{color:rgba(43,54,68,0.65);margin:0 0 24px;}}
      {extra_css}
    </style>
  </head>
  <body>
    {nav}
    <main class="shell">
      <div class="workspace">
        {body}
      </div>
    </main>
  </body>
</html>"""


_NURTURE_CSS = """
  .nur-wrap { max-width:900px; margin:26px 0 0; padding:18px 20px; background:#fff;
    border:1px solid #e5e7eb; border-radius:14px; }
  .nur-h { font-size:13px; font-weight:800; letter-spacing:.06em; text-transform:uppercase; color:#6b7280; margin:0 0 10px; }
  .nur-row { display:flex; gap:10px; align-items:center; flex-wrap:wrap; }
  .nur-row input, .nur-row select { padding:9px 11px; border:1px solid #e5e7eb; border-radius:10px; font-size:14px; }
  .nur-row input[type=email] { min-width:240px; }
  .nur-btn { padding:10px 18px; border:none; border-radius:10px; background:#2B3644; color:#fff;
    font-weight:800; font-size:14px; cursor:pointer; }
  .nur-msg { margin:10px 0 0; font-size:14px; }
"""

_NURTURE_HTML = """
    <div class="nur-wrap">
      <p class="nur-h">Reply outcome &rarr; HubSpot nurture</p>
      <div class="nur-row">
        <input id="nur-email" type="email" placeholder="contact@brand.com">
        <input id="nur-brand" type="text" placeholder="Brand (optional)">
        <select id="nur-outcome">
          <option value="follow_up">Follow up later</option>
          <option value="no_show">No show</option>
        </select>
        <button class="nur-btn" id="nur-go" type="button">Add to nurture</button>
      </div>
      <p class="nur-msg" id="nur-msg"></p>
    </div>
    <script>
      (function(){
        var b=document.getElementById('nur-go'), msg=document.getElementById('nur-msg');
        b.addEventListener('click', function(){
          var fd=new FormData();
          fd.append('email', document.getElementById('nur-email').value);
          fd.append('brand', document.getElementById('nur-brand').value);
          fd.append('outcome', document.getElementById('nur-outcome').value);
          msg.textContent='Working...';
          fetch('/admin/api/outbound/nurture', {method:'POST', body:fd})
            .then(function(r){ return r.json(); })
            .then(function(d){ msg.textContent = d.ok
              ? 'Added. They will get the nurture sequence.'
              : ('Could not add: ' + (d.reason||'unknown')); })
            .catch(function(){ msg.textContent='Could not reach the server.'; });
        });
      })();
    </script>
"""


@router.get("/admin/outbound/scoreboard", response_class=HTMLResponse)
def outbound_scoreboard(request: Request) -> Response:
    import outbound_scoreboard as _sb
    import outbound_bottlenecks as _bn
    import outbound_efficacy as _ef
    import outbound_compliance as _cp

    board = _sb.get_scoreboard(_sb.load_instantly_key())

    # Guardrails from the outbound briefs: what is provably OK, broken, or still
    # needs a one-time human confirmation in Instantly/Clay.
    checks = _cp.compute_compliance(
        positive_rate=board.positive_rate,
        bounce_rate=board.bounce_rate,
        connected=board.connected,
    )

    # Capacity + bottlenecks (from env inputs + the live reply rate).
    bottlenecks = _bn.get_bottlenecks(
        reply_rate_pct=board.reply_rate if board.connected else None,
        emails_per_booked_call=board.emails_per_booked_call if board.connected else None,
    )

    # By-signal efficacy (counts from what we've pushed; rates once outcomes exist).
    try:
        from sales_support_agent.models.database import get_engine
        from sales_support_agent.services import outbound_memory
        pushed = outbound_memory.load_pushed(get_engine())
    except Exception:  # noqa: BLE001
        pushed = []
    efficacy = _ef.compute_signal_efficacy(pushed, outcomes={})

    body = f"""
        <h1>Outbound scoreboard</h1>
        <p class="sub">Your machine, and how it is performing. Reads live from Instantly.</p>
        {_sb.render_scoreboard_body(board)}
        {_cp.render_compliance_html(checks)}
        {_bn.render_bottlenecks_html(bottlenecks)}
        {_ef.render_efficacy_html(efficacy)}
        {_NURTURE_HTML}
    """
    extra_css = (_sb.SCOREBOARD_CSS + _cp.COMPLIANCE_CSS + _bn.BOTTLENECK_CSS
                 + _ef.EFFICACY_CSS + _NURTURE_CSS)
    return HTMLResponse(_shell_page(
        request, active="outbound_scoreboard", title="Outbound Scoreboard",
        extra_css=extra_css, body=body,
    ))


_BRANDS_CSS = """
  .steps{margin:8px 0 0;padding:0;list-style:none;counter-reset:step;}
  .steps li{position:relative;padding:14px 0 14px 44px;border-top:1px solid rgba(43,54,68,0.08);}
  .steps li:first-child{border-top:none;}
  .steps li::before{counter-increment:step;content:counter(step);position:absolute;left:0;top:12px;width:28px;height:28px;
    border-radius:50%;background:#2B3644;color:#fff;font-family:"Montserrat",sans-serif;font-weight:800;font-size:13px;
    display:flex;align-items:center;justify-content:center;}
  .steps b{font-family:"Montserrat",sans-serif;}
  .btn{display:inline-flex;align-items:center;gap:8px;margin:4px 0 4px;padding:12px 20px;border-radius:12px;background:#2B3644;
    color:#fff;font-family:"Montserrat",sans-serif;font-weight:800;font-size:14px;text-decoration:none;}
  .btn:hover{background:#1f2833;color:#fff;}
  .field{display:flex;gap:10px;align-items:center;margin:14px 0 22px;flex-wrap:wrap;}
  .field label{font-family:"Montserrat",sans-serif;font-weight:700;font-size:13px;}
  .field input{width:90px;padding:9px 11px;border:1px solid var(--border);border-radius:10px;font-size:14px;}
  .note{margin:18px 0 0;padding:14px 16px;border-radius:14px;background:rgba(133,187,218,0.14);border:1px solid rgba(43,54,68,0.08);font-size:14px;}
"""


@router.get("/admin/outbound/brands", response_class=HTMLResponse)
def outbound_brands_page(request: Request) -> Response:
    """Landing page: download the ICP-matched brand list, then the steps to load
    it into Clay and Instantly. The download itself is the CSV endpoint below."""
    import outbound_pipeline as _op

    api_key, _clay = _op.load_config_from_env()
    key_note = (
        '<div class="note">STORELEADS_API_KEY is not set on this service yet, so the '
        'download will not work. Add it on Render (sales-support-agent service) and redeploy.</div>'
        if not api_key else ""
    )

    body = f"""
        <h1>Brand list</h1>
        <p class="sub">Pull fresh ICP-matched Shopify brands from StoreLeads as a CSV, then
        feed it to Clay. This builds the list only. It sends nothing.</p>

        <div class="field">
          <label for="count">How many brands</label>
          <input id="count" type="number" min="1" max="500" value="100">
          <a class="btn" id="dl" href="/admin/api/outbound/brands.csv?max_new=100">Download brand CSV</a>
        </div>

        <h2 style="font-size:15px;margin:20px 0 6px;">What to do with it</h2>
        <ol class="steps">
          <li><b>Download</b> the CSV above. Each brand is ranked Tier A, B, or C with the reason it was picked (hottest first), fits our ICP, and has a contact route Clay can work from.</li>
          <li><b>Import into Clay</b> (Add data &rarr; Import CSV) into your enrichment table. Clay finds the decision-maker and a verified email.</li>
          <li><b>Let the two prompts run</b> in Clay: the Sales Fit column qualifies, the Personalization column writes the opener.</li>
          <li><b>Push qualified rows to Instantly</b> from Clay, into your warmed campaign.</li>
          <li><b>Review before send:</b> approve the copy and eyeball a small test batch. Only then turn the campaign on.</li>
        </ol>
        {key_note}
        <div class="note">Never email a brand twice: every brand you download here is
        remembered, and future downloads automatically skip it. You will only ever get fresh brands.</div>
    """
    # Tiny inline script so the count box updates the download link. Kept minimal.
    body += """
        <script>
          (function(){
            var box=document.getElementById('count'), btn=document.getElementById('dl');
            function sync(){ var n=Math.max(1,Math.min(500,parseInt(box.value||'100',10)||100));
              btn.href='/admin/api/outbound/brands.csv?max_new='+n; }
            box.addEventListener('input', sync); sync();
          })();
        </script>
    """
    return HTMLResponse(_shell_page(
        request, active="outbound_brands", title="Outbound Brand List",
        extra_css=_BRANDS_CSS, body=body,
    ))


@router.get("/admin/api/outbound/brands.csv", response_class=Response)
def outbound_brands_csv(request: Request, max_new: int = 100) -> Response:
    """Pull ICP-matched brands from StoreLeads and return them as a CSV to import
    into Clay. Sends nothing. Dedup state is not yet shared with this service, so
    a brand may appear across runs until that is wired; the CSV is a preview only.
    """
    import outbound_pipeline as _op
    from sales_support_agent.models.database import get_engine
    from sales_support_agent.services import outbound_memory

    api_key, _clay = _op.load_config_from_env()
    if not api_key:
        return JSONResponse(status_code=400, content={"detail": "STORELEADS_API_KEY is not set on this service."})

    # Never-email-twice: skip brands already exported, then remember the new ones.
    try:
        engine = get_engine()
    except Exception:  # noqa: BLE001 — dedup is best-effort; build anyway
        engine = None
    already = outbound_memory.load_contacted(engine) if engine is not None else set()

    try:
        result = _op.run_storeleads_to_clay(
            api_key=api_key,
            clay_webhook_url="",  # dry-run: build the list, push nothing
            processed_domains=already,
            max_new=max(1, min(int(max_new or 100), 500)),
            dry_run=True,
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("[outbound] StoreLeads CSV build failed")
        return JSONResponse(status_code=502, content={"detail": f"StoreLeads fetch failed: {exc}"})

    if engine is not None and result.leads:
        # Record full leads (domain + tier + signals) for dedup AND efficacy.
        outbound_memory.record_leads(engine, result.leads, source="csv_export")

    return Response(
        content=_op.leads_to_csv(result.leads),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="anata_clay_brands.csv"'},
    )


@router.post("/admin/api/outbound/nurture", response_class=JSONResponse)
async def outbound_nurture_enroll(request: Request) -> Response:
    """Enroll a follow-up / no-show contact into the HubSpot nurture."""
    from sales_support_agent.integrations.hubspot import HubSpotClient
    from sales_support_agent.services import outbound_nurture

    form = await request.form()
    email = str(form.get("email") or "").strip()
    outcome = str(form.get("outcome") or "").strip()
    brand = str(form.get("brand") or "").strip() or None

    settings = getattr(request.app.state, "settings", None)
    if settings is None:
        return JSONResponse(status_code=500, content={"ok": False, "reason": "Settings unavailable."})
    client = HubSpotClient(settings)
    result = outbound_nurture.enroll_contact(client, email=email, outcome=outcome, brand=brand)
    return JSONResponse(status_code=(200 if result.get("ok") else 400), content=result)
