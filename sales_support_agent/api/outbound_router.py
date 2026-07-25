"""Outbound pages for the sales-support-agent app (agent.anatainc.com).

The outbound scoreboard and the StoreLeads brand-list download, rendered inside
the standard admin shell (top nav + Outbound section) and gated by the app's own
access control via the `outbound.scoreboard` tool (see services/access/catalog).
The engine lives in the repo-root modules (outbound_pipeline, outbound_scoreboard);
this router just exposes them. Read-only and dry-run: nothing sends, nothing pushes.
"""

from __future__ import annotations

import html
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
def outbound_brands_csv(request: Request, max_new: int = 100, recipe: str = "") -> Response:
    """Pull ICP-matched brands from StoreLeads and return them as a CSV to import
    into Clay. Sends nothing. Dedup state is not yet shared with this service, so
    a brand may appear across runs until that is wired; the CSV is a preview only.
    """
    import outbound_pipeline as _op
    import outbound_recipes as _rx
    from sales_support_agent.models.database import get_engine
    from sales_support_agent.services import outbound_memory

    # Validate the request before the environment, so a typo'd recipe always
    # reports as a typo rather than as a missing key.
    chosen = _rx.recipe(recipe) if recipe else None
    if recipe and chosen is None:
        return JSONResponse(status_code=400, content={"detail": f"Unknown recipe '{recipe}'."})

    api_key, _clay = _op.load_config_from_env()
    if not api_key:
        return JSONResponse(status_code=400, content={"detail": "STORELEADS_API_KEY is not set on this service."})

    # Never-email-twice: skip brands already exported, then remember the new ones.
    try:
        engine = get_engine()
    except Exception:  # noqa: BLE001 — dedup is best-effort; build anyway
        engine = None
    already = outbound_memory.load_contacted(engine) if engine is not None else set()

    from sales_support_agent.services import outbound_settings as _st
    tunables = _st.effective(engine, _rx.DEFAULT_SETTINGS) if engine is not None else _rx.DEFAULT_SETTINGS
    version = _st.config_version(engine) if engine is not None else 0
    cap = chosen.cap(tunables) if chosen else max(1, min(int(max_new or 100), 500))
    try:
        result = _op.run_storeleads_to_clay(
            api_key=api_key,
            clay_webhook_url="",  # dry-run: build the list, push nothing
            processed_domains=already,
            max_new=cap,
            dry_run=True,
            recipe=chosen,
            settings=tunables,
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("[outbound] StoreLeads CSV build failed")
        return JSONResponse(status_code=502, content={"detail": f"StoreLeads fetch failed: {exc}"})

    if engine is not None:
        if result.leads:
            # Record full leads (domain + tier + signals) for dedup AND efficacy.
            outbound_memory.record_leads(engine, result.leads, source=result.recipe or "csv_export")
        # Log the pull itself, so Lead Ops shows what we pulled and when.
        outbound_memory.record_run(
            engine, recipe=result.recipe or "icp_baseline", scanned=result.scanned,
            matched=result.matched_icp, fresh=result.fresh,
            skipped_seen=result.skipped_already_contacted, partial=result.partial,
            config_version=version, delivery="file",
        )

    return Response(
        content=_op.leads_to_csv(result.leads),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="anata_clay_brands.csv"'},
    )


_LEADOPS_CSS = """
  .lo-h { font-size:13px; font-weight:800; letter-spacing:.06em; text-transform:uppercase; color:#6b7280; margin:22px 0 8px; }
  .lo-table { border-collapse:collapse; width:100%; max-width:1000px; background:#fff; border:1px solid #e5e7eb; border-radius:14px; overflow:hidden; }
  .lo-table th, .lo-table td { text-align:left; padding:10px 14px; border-bottom:1px solid #f0f0f3; font-size:14px; vertical-align:top; }
  .lo-table th { background:#fafafa; font-size:11px; text-transform:uppercase; letter-spacing:.06em; color:#6b7280; }
  .lo-tier { font-weight:800; }
  .lo-A { color:#0a7d33; } .lo-B { color:#b54708; } .lo-C { color:#6b7280; }
  .lo-btn { display:inline-block; padding:7px 14px; border-radius:9px; background:#2B3644; color:#fff;
    font-family:"Montserrat",sans-serif; font-weight:800; font-size:12px; text-decoration:none; white-space:nowrap; }
  .lo-btn:hover { background:#1f2833; color:#fff; }
  .lo-note { margin:10px 0 0; font-size:14px; color:rgba(43,54,68,.7); }
  .lo-form { display:flex; gap:10px; align-items:flex-end; flex-wrap:wrap; margin:8px 0 0; }
  .lo-field { display:flex; flex-direction:column; gap:4px; }
  .lo-field label { font-size:12px; font-weight:700; color:rgba(43,54,68,.7); }
  .lo-field input { width:110px; padding:8px 10px; border:1px solid var(--border); border-radius:9px; font-size:14px; }
  .lo-field.wide input { width:280px; }
  .lo-save { padding:10px 18px; border:none; border-radius:10px; background:#2B3644; color:#fff; font-weight:800; font-size:14px; cursor:pointer; }
  .lo-msg { margin:10px 0 0; font-size:14px; font-weight:600; }
  .lo-ver { display:inline-block; padding:2px 8px; border-radius:999px; background:rgba(43,54,68,.08); font-size:12px; font-weight:800; }
  .lo-clay { margin:10px 0 0; padding:12px 16px; border-radius:14px; background:#fff;
    border:1px solid var(--border); font-size:14px; }
  .lo-push { border:none; cursor:pointer; font-family:"Montserrat",sans-serif; }
  .lo-today { padding:14px 16px; border-radius:14px; background:rgba(133,187,218,.14); border:1px solid rgba(43,54,68,.08); font-size:15px; }
"""


@router.get("/admin/outbound/lead-ops", response_class=HTMLResponse)
def outbound_lead_ops(request: Request) -> Response:
    """What we pull, when it fires, and what every past pull returned."""
    import outbound_recipes as _rx
    import outbound_clay as _cl
    from sales_support_agent.services import outbound_settings as _st

    try:
        from sales_support_agent.models.database import get_engine
        _eng = get_engine()
    except Exception:  # noqa: BLE001
        _eng = None
    tunables = _st.effective(_eng, _rx.DEFAULT_SETTINGS)
    version = _st.config_version(_eng)

    plan = _rx.daily_plan(settings=tunables)
    todays_keys = {r["key"] for r in plan["recipes"]}

    _clay_url, _ = _cl.load_clay_config()
    try:
        from sales_support_agent.services import outbound_memory as _mem
        _used = _mem.total_delivered(_eng)
    except Exception:  # noqa: BLE001
        _used = 0
    if _clay_url:
        clay_strip = (f"<b>Clay: connected.</b> {html.escape(_cl.budget_note(_used))} "
                      "Send to Clay puts brands straight into your table, no file needed.")
    else:
        clay_strip = ("<b>Clay: not connected.</b> Add the webhook address on Render as "
                      "CLAY_WEBHOOK_URL and the Send to Clay buttons switch on. "
                      "Pull now and the file download work either way.")

    if plan["recipes"]:
        today_line = (
            f"Today is {plan['weekday']}. {len(plan['recipes'])} pull(s) scheduled, "
            f"up to {plan['planned_total']} fresh brands."
        )
    else:
        today_line = (
            f"Today is {plan['weekday']}. Nothing scheduled - we do not pull on weekends "
            "because we do not send on weekends."
        )

    rows = []
    for r in _rx.RECIPES:
        due = "Today" if r.key in todays_keys else ("Tue / Wed" if r.cadence == "weekly" else "Weekdays")
        cap_now = r.cap(tunables)
        rows.append(
            f"<tr><td class='lo-tier lo-{r.tier}'>{r.tier}</td>"
            f"<td><b>{html.escape(r.label)}</b><br>"
            f"<span style='color:rgba(43,54,68,.6)'>{html.escape(r.reason_for(tunables))}</span></td>"
            f"<td>{html.escape(due)}</td><td>{cap_now}</td>"
            f"<td><a class='lo-btn' href='/admin/api/outbound/brands.csv?recipe={r.key}'>Pull now</a>"
            + (f" <button class='lo-btn lo-push' data-recipe='{r.key}' type='button'>Send to Clay</button>"
               if _clay_url else "")
            + "</td></tr>"
        )

    # Past pulls, so we can see what each recipe actually returns over time.
    try:
        from sales_support_agent.services import outbound_memory
        runs = outbound_memory.load_runs(_eng, limit=25)
        changes = _st.load_changes(_eng, limit=20)
    except Exception:  # noqa: BLE001
        runs, changes = [], []

    if runs:
        run_rows = "".join(
            f"<tr><td>{html.escape(str(x['ran_at'])[:16])}</td><td>{html.escape(x['recipe'] or '-')}</td>"
            f"<td>{x['scanned']:,}</td><td>{x['matched']:,}</td><td><b>{x['fresh']:,}</b></td>"
            f"<td>{x['skipped_seen']:,}</td>"
            f"<td>{'Clay' if x.get('delivery') == 'clay' else 'file'}"
            + (f" ({x.get('delivered')})" if x.get('delivery') == 'clay' else "") + "</td>"
            f"<td>{'cut short' if x['partial'] else 'complete'}</td>"
            f"<td>v{x.get('config_version') or 0}</td></tr>"
            for x in runs
        )
    else:
        run_rows = "<tr><td colspan='9'>No pulls yet. Use a Pull now button above.</td></tr>"


    try:
        from sales_support_agent.services import outbound_memory as _mem2
        contacted_count = len(_mem2.load_contacted(_eng))
    except Exception:  # noqa: BLE001
        contacted_count = 0

    # Tuning: the numbers an operator should be able to change without a deploy.
    tune_fields = "".join(
        f'<div class="lo-field"><label for="s_{k}">{html.escape(lbl)}</label>'
        f'<input id="s_{k}" data-key="{k}" type="number" min="1" value="{html.escape(str(tunables.get(k, "")))}"></div>'
        for k, lbl in _rx.TUNABLE_LABELS.items()
    )

    if changes:
        change_rows = "".join(
            f"<tr><td>v{c['version']}</td><td>{html.escape(str(c['changed_at'])[:16])}</td>"
            f"<td>{html.escape(_rx.TUNABLE_LABELS.get(c['key'], c['key']))}</td>"
            f"<td>{html.escape(str(c['old_value'] or '-'))} &rarr; <b>{html.escape(str(c['new_value']))}</b></td>"
            f"<td>{html.escape(c['note'] or '-')}</td><td>{html.escape(c['changed_by'] or '-')}</td></tr>"
            for c in changes
        )
    else:
        change_rows = "<tr><td colspan='6'>No changes yet. Settings are at their defaults.</td></tr>"

    body = f"""
        <h1>Lead ops</h1>
        <p class="sub">What we pull from StoreLeads, what makes it fire, and what every
        pull actually returned. Building the list only - nothing here sends.</p>

        <div class="lo-today">{html.escape(today_line)}</div>
        <div class="lo-clay">{clay_strip}</div>

        <p class="lo-msg" id="push-msg"></p>

        <p class="lo-h">Pull recipes</p>
        <table class="lo-table">
          <thead><tr><th>Tier</th><th>Recipe / why now</th><th>Runs</th><th>Cap</th><th></th></tr></thead>
          <tbody>{''.join(rows)}</tbody>
        </table>
        <p class="lo-note">Triggers run Tuesday and Wednesday because StoreLeads refreshes
        its data weekly on Monday. The core ICP pull runs every weekday to keep volume steady.
        Caps are deliberately small: frequent and low beats one big blast.</p>

        <p class="lo-h">Recent pulls</p>
        <table class="lo-table">
          <thead><tr><th>When</th><th>Recipe</th><th>Scanned</th><th>Fit ICP</th><th>Fresh</th><th>Already seen</th><th>Delivered</th><th>Status</th><th>Settings</th></tr></thead>
          <tbody>{run_rows}</tbody>
        </table>
        <p class="lo-note">Fresh is what you actually get: brands that fit, that we have
        never contacted before. Already seen is the never-email-twice memory doing its job.
        Settings shows which tuning version each pull ran under.</p>

        <p class="lo-h">Brands already used</p>
        <div class="lo-clay">
          <b>{contacted_count} brands</b> are marked as already contacted and will be skipped
          on future pulls. If any of those were pulled but never actually emailed, for example
          a test file you discarded, you can put them back in the pool.
          <div style="margin-top:10px">
            <button class="lo-save" id="rel-go" type="button">Release all back into the pool</button>
            <span class="lo-msg" id="rel-msg" style="margin-left:10px"></span>
          </div>
        </div>

        <p class="lo-h">Tuning <span class="lo-ver">now on v{version}</span></p>
        <div class="lo-form">
          {tune_fields}
          <div class="lo-field wide"><label for="s_note">Why are you changing it?</label>
            <input id="s_note" type="text" placeholder="e.g. widening to lift the just-installed yield"></div>
          <button class="lo-save" id="s_save" type="button">Save changes</button>
        </div>
        <p class="lo-msg" id="s_msg"></p>
        <p class="lo-note">Changing anything here bumps the settings version. Every pull records
        the version it ran under, so you can compare results before and after a change instead
        of guessing whether it helped.</p>

        <p class="lo-h">Change log</p>
        <table class="lo-table">
          <thead><tr><th>Version</th><th>When</th><th>Setting</th><th>Change</th><th>Note</th><th>Who</th></tr></thead>
          <tbody>{change_rows}</tbody>
        </table>
        <script>
          (function(){{
            var rb=document.getElementById('rel-go'), rm=document.getElementById('rel-msg');
            if(rb) rb.addEventListener('click', function(){{
              if(!confirm('Only do this if these brands were never actually emailed. Release them all?')) return;
              var fd=new FormData(); fd.append('confirm','yes');
              rm.textContent='Releasing...';
              fetch('/admin/api/outbound/release',{{method:'POST',body:fd}})
                .then(function(r){{return r.json();}})
                .then(function(d){{ rm.textContent=d.summary||d.reason;
                  if(d.released) setTimeout(function(){{location.reload();}},1200); }})
                .catch(function(){{ rm.textContent='Could not reach the server.'; }});
            }});
          }})();
        </script>
        <script>
          (function(){{
            var msg=document.getElementById('push-msg');
            document.querySelectorAll('.lo-push').forEach(function(b){{
              b.addEventListener('click', function(){{
                var fd=new FormData(); fd.append('recipe', b.dataset.recipe);
                msg.textContent='Pulling and sending to Clay...';
                b.disabled=true;
                fetch('/admin/api/outbound/push', {{method:'POST', body:fd}})
                  .then(function(r){{return r.json();}})
                  .then(function(d){{
                    msg.textContent = d.summary || (d.reason || 'Done.');
                    b.disabled=false;
                    setTimeout(function(){{location.reload();}}, 2500);
                  }})
                  .catch(function(){{ msg.textContent='Could not reach the server.'; b.disabled=false; }});
              }});
            }});
          }})();
        </script>
        <script>
          (function(){{
            var btn=document.getElementById('s_save'), msg=document.getElementById('s_msg');
            btn.addEventListener('click', function(){{
              var fd=new FormData();
              document.querySelectorAll('input[data-key]').forEach(function(i){{
                fd.append(i.dataset.key, i.value);
              }});
              fd.append('note', document.getElementById('s_note').value);
              msg.textContent='Saving...';
              fetch('/admin/api/outbound/settings', {{method:'POST', body:fd}})
                .then(function(r){{return r.json();}})
                .then(function(d){{
                  if(!d.ok){{ msg.textContent='Could not save: '+(d.reason||'unknown'); return; }}
                  msg.textContent = d.changed
                    ? ('Saved. Now on v'+d.version+'. Reloading...')
                    : 'Nothing changed.';
                  if(d.changed) setTimeout(function(){{location.reload();}}, 900);
                }})
                .catch(function(){{ msg.textContent='Could not reach the server.'; }});
            }});
          }})();
        </script>
    """
    return HTMLResponse(_shell_page(
        request, active="outbound_leadops", title="Outbound Lead Ops",
        extra_css=_LEADOPS_CSS, body=body,
    ))


@router.post("/admin/api/outbound/push", response_class=JSONResponse)
async def outbound_push_to_clay(request: Request) -> Response:
    """Pull one recipe and send it straight to Clay, no file in between.

    Only brands Clay actually accepted are marked as contacted, so a rejected
    brand comes back on the next pull rather than being silently lost.
    """
    import outbound_pipeline as _op
    import outbound_recipes as _rx
    import outbound_clay as _cl
    from sales_support_agent.services import outbound_memory, outbound_settings as _st

    form = await request.form()
    key = str(form.get("recipe") or "").strip()
    chosen = _rx.recipe(key) if key else None
    if key and chosen is None:
        return JSONResponse(status_code=400, content={"ok": False, "reason": f"Unknown recipe '{key}'."})

    webhook_url, token = _cl.load_clay_config()
    if not webhook_url:
        return JSONResponse(status_code=400, content={
            "ok": False, "reason": "Clay is not connected yet. Add the webhook address on "
                                   "Render as CLAY_WEBHOOK_URL to turn this on."})

    api_key, _ = _op.load_config_from_env()
    if not api_key:
        return JSONResponse(status_code=400, content={
            "ok": False, "reason": "STORELEADS_API_KEY is not set on this service."})

    try:
        from sales_support_agent.models.database import get_engine
        engine = get_engine()
    except Exception:  # noqa: BLE001
        engine = None

    tunables = _st.effective(engine, _rx.DEFAULT_SETTINGS) if engine is not None else _rx.DEFAULT_SETTINGS
    version = _st.config_version(engine) if engine is not None else 0
    already = outbound_memory.load_contacted(engine)
    used = outbound_memory.total_delivered(engine)

    try:
        result = _op.run_storeleads_to_clay(
            api_key=api_key, clay_webhook_url="", processed_domains=already,
            max_new=chosen.cap(tunables) if chosen else 25, dry_run=True,
            recipe=chosen, settings=tunables,
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("[outbound] pull before Clay push failed")
        return JSONResponse(status_code=502, content={"ok": False, "reason": f"StoreLeads fetch failed: {exc}"})

    pushed = _cl.push_leads(webhook_url, result.leads, token=token, config_version=version,
                            used_submissions=used)

    # ONLY what Clay accepted counts as contacted.
    if engine is not None:
        accepted = {d for d in pushed.accepted_domains if d}
        if accepted:
            outbound_memory.record_leads(
                engine, [l for l in result.leads if l.get("domain") in accepted],
                source=result.recipe or "clay_push")
        outbound_memory.record_run(
            engine, recipe=result.recipe or "icp_baseline", scanned=result.scanned,
            matched=result.matched_icp, fresh=result.fresh,
            skipped_seen=result.skipped_already_contacted, partial=result.partial,
            config_version=version, delivery="clay", delivered=pushed.accepted,
            note=pushed.reason[:200],
        )

    return JSONResponse(content={
        "ok": pushed.rejected == 0,
        "found": result.fresh,
        "accepted": pushed.accepted,
        "rejected": pushed.rejected,
        "skipped_already_contacted": result.skipped_already_contacted,
        "summary": (f"Found {result.fresh} fresh brands. " + pushed.summary
                    if result.fresh else "No fresh brands to send right now."),
    })


@router.post("/admin/api/outbound/release", response_class=JSONResponse)
async def outbound_release(request: Request) -> Response:
    """Put brands back in the pool that were pulled but never actually contacted.

    Deliberately manual and deliberately explicit: releasing a brand that really
    was emailed would let us email it twice. The caller must confirm.
    """
    from sales_support_agent.services import outbound_memory

    form = await request.form()
    if str(form.get("confirm") or "").lower() != "yes":
        return JSONResponse(status_code=400, content={
            "ok": False, "reason": "Not confirmed, so nothing was released."})

    try:
        from sales_support_agent.models.database import get_engine
        engine = get_engine()
    except Exception:  # noqa: BLE001
        engine = None
    released = outbound_memory.release_contacted(engine)
    return JSONResponse(content={
        "ok": True, "released": released,
        "summary": (f"Released {released} brands back into the pool. They can be "
                    "sourced again on the next pull.") if released
                   else "There was nothing to release.",
    })


@router.post("/admin/api/outbound/settings", response_class=JSONResponse)
async def outbound_save_settings(request: Request) -> Response:
    """Save tuning changes. Only real differences are stored, and each one is
    logged against a new settings version so results stay attributable."""
    import outbound_recipes as _rx
    from sales_support_agent.services import outbound_settings as _st

    form = await request.form()
    note = str(form.get("note") or "").strip()
    updates = {k: str(form.get(k)).strip() for k in _rx.TUNABLE_LABELS if form.get(k) not in (None, "")}
    # Reject nonsense before it reaches a live pull.
    for k, v in list(updates.items()):
        try:
            if int(v) < 1:
                return JSONResponse(status_code=400, content={
                    "ok": False, "reason": f"{_rx.TUNABLE_LABELS[k]} must be 1 or more."})
        except ValueError:
            return JSONResponse(status_code=400, content={
                "ok": False, "reason": f"{_rx.TUNABLE_LABELS[k]} must be a whole number."})

    user = get_current_user(request) or {}
    try:
        from sales_support_agent.models.database import get_engine
        engine = get_engine()
    except Exception:  # noqa: BLE001
        engine = None
    result = _st.apply_changes(engine, updates, note=note,
                               changed_by=str(user.get("email") or ""))
    return JSONResponse(status_code=(200 if result.get("ok") else 400), content=result)


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
