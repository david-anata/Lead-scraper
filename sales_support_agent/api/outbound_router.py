"""Outbound pages for the sales-support-agent app (agent.anatainc.com).

The outbound scoreboard and the StoreLeads brand-list download, rendered inside
the standard admin shell (top nav + Outbound section) and gated by the app's own
access control via the `outbound.scoreboard` tool (see services/access/catalog).
The engine lives in the repo-root modules (outbound_pipeline, outbound_scoreboard);
this router just exposes them. Read-only and dry-run: nothing sends, nothing pushes.
"""

from __future__ import annotations

import html
import json
import logging
from datetime import datetime, timezone
from typing import Any

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
def outbound_brands_csv(request: Request, max_new: int = 100, recipe: str = "",
                        scanned: int = 0) -> Response:
    """Brands as a CSV to import into Clay. Sends nothing.

    Two sources, and the difference matters.

    Default: pull fresh brands from StoreLeads. Fast, but a brand sourced this
    second has not been near Amazon, so every amz_* column is empty and Clay has
    nothing to write an opening line from.

    scanned=1: export the brands we already hold that have been through the
    Amazon check. This is the one to hand Clay. Without it the scan writes its
    findings to our own records and they never reach the file, which is exactly
    what happened the first time this ran.
    """
    import outbound_pipeline as _op
    import outbound_recipes as _rx
    from sales_support_agent.models.database import get_engine
    from sales_support_agent.services import outbound_memory

    if scanned:
        try:
            engine = get_engine()
        except Exception:  # noqa: BLE001
            engine = None
        held = outbound_memory.load_leads(engine, limit=2000) if engine is not None else []
        # Checked, matched, AND actually found something worth opening with. A
        # brand we looked at and found nothing on still carries the old
        # plan-upgrade line, which pitches the previous offer and reads as a
        # non sequitur under an Amazon email. Not sendable on this campaign.
        ready = [l for l in held
                 if str(l.get("amazon_checked_at") or "").strip()
                 and not str(l.get("amazon_skipped_reason") or "").strip()
                 and str(l.get("amz_situation") or "").strip()]
        ready.sort(key=lambda l: -int(l.get("score") or 0))
        ready = ready[:max(1, min(int(max_new or 100), 2000))]
        body = _op.leads_to_csv(ready)
        return Response(
            content=body, media_type="text/csv",
            headers={"Content-Disposition": 'attachment; filename="anata_scanned_brands.csv"'},
        )

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
            outbound_memory.record_leads(engine, result.leads, source=result.recipe or "csv_export", config_version=version)
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


_AMAZON_CSS = """
  .am-panel { margin:8px 0 0; padding:16px 18px; border-radius:14px; background:#fff; border:1px solid var(--border); }
  .am-counters { display:flex; gap:34px; flex-wrap:wrap; margin:0 0 12px; }
  .am-c { display:flex; flex-direction:column; gap:2px; }
  .am-c b { font-family:"Montserrat",sans-serif; font-size:26px; line-height:1; }
  .am-c span { font-size:11px; text-transform:uppercase; letter-spacing:.06em; color:#6b7280; font-weight:700; }
  .am-sum { margin:0 0 10px; padding:8px 12px; border-radius:10px; background:rgba(133,187,218,.14);
    font-size:14px; font-weight:700; font-variant-numeric:tabular-nums; }
  .am-line { margin:4px 0 0; font-size:14px; color:rgba(43,54,68,.75); }
  .am-run { margin:14px 0 0; display:flex; gap:12px; align-items:center; flex-wrap:wrap; }
  .am-btn { padding:10px 18px; border:none; border-radius:10px; background:#2B3644; color:#fff;
    font-weight:800; font-size:14px; cursor:pointer; }
  .am-btn:disabled { opacity:.55; cursor:default; }
  .am-ghost { background:#fff; color:#2B3644; border:1px solid var(--border); }
  .am-note { font-size:13px; color:rgba(43,54,68,0.65); }
  .am-empty { margin:0; font-size:15px; }
  .am-badge { display:inline-block; padding:2px 8px; border-radius:999px; font-size:11px; font-weight:800; letter-spacing:.04em; }
  .am-high { background:rgba(10,125,51,.12); color:#0a7d33; }
  .am-med { background:rgba(181,71,8,.12); color:#b54708; }
  .am-low { background:rgba(43,54,68,.10); color:#6b7280; }
  .am-none { color:#9ca3af; }
  .am-open { white-space:normal; min-width:280px; max-width:420px; }
  .am-skip { color:#6b7280; font-style:italic; }
"""


def _amazon_checker(tunables: dict[str, Any]):
    """The per-lead Amazon lookup, for a bounded scan. NOT for a pull.

    Measured on live data: one brand costs up to six sequential Amazon lookups
    at 20 to 35 seconds each, so 2 to 4 minutes per brand. Wiring this into
    run_storeleads_to_clay made a 30 brand pull take one to two hours and the
    request never returned. It belongs in a scan that does a few brands at a
    time and can be stopped, which is how the spec drew it.

    Returns None rather than a no-op callable when the check is disabled or the
    key is missing, so a pull behaves exactly as it did before this existed.
    Every failure degrades to no finding: a data provider outage should cost us
    the opening line, never the batch.
    """
    import os

    import outbound_amazon as _az

    if not _az._bool_setting(tunables, "amazon.enabled"):
        return None
    if not (os.getenv("RAINFOREST_API_KEY", "") or "").strip():
        logger.info("[outbound] amazon check skipped: no RAINFOREST_API_KEY")
        return None

    try:
        from sales_support_agent.services.rainforest import RainforestClient
        client = RainforestClient()
    except Exception:  # noqa: BLE001
        logger.exception("[outbound] could not build the Amazon client")
        return None

    def check(lead: dict[str, Any]) -> Any:
        try:
            return _az.brand_control(
                str(lead.get("brand") or ""),
                str(lead.get("domain") or ""),
                str(lead.get("country") or ""),
                niche=str(lead.get("niche") or ""),
                client=client,
                settings=tunables,
            )
        except Exception:  # noqa: BLE001
            logger.exception("[outbound] amazon check failed for %s", lead.get("domain"))
            return None

    return check


def _amazon_scan_summary(leads: list[dict]) -> dict[str, Any]:
    """The Amazon scan as numbers that have to add up.

    Every scanned brand lands in exactly one of findings / skipped / absent, so
    the panel can print the arithmetic. A brand quietly dropped from a scan is
    the failure this whole panel exists to catch.
    """
    scanned = findings = skipped = absent = 0
    leaking = clean = 0
    reasons: dict[str, int] = {}
    markets: dict[str, int] = {}

    for lead in leads:
        if not str(lead.get("amazon_checked_at") or "").strip():
            continue
        scanned += 1
        market = str(lead.get("amazon_marketplace") or "").strip() or "not recorded"
        markets[market] = markets.get(market, 0) + 1

        reason = str(lead.get("amazon_skipped_reason") or "").strip()
        if reason:
            skipped += 1
            reasons[reason] = reasons.get(reason, 0) + 1
        elif lead.get("amazon_absent"):
            absent += 1
        else:
            findings += 1
            if int(lead.get("amazon_sellers_unknown") or 0) > 0:
                leaking += 1
            else:
                clean += 1

    return {
        "scanned": scanned, "findings": findings, "skipped": skipped, "absent": absent,
        "leaking": leaking, "clean": clean, "never_checked": max(0, len(leads) - scanned),
        "reasons": reasons, "markets": markets, "held": len(leads),
    }


def _lower_first(text: str) -> str:
    return text[:1].lower() + text[1:] if text else text


_AMAZON_SCAN_CONTROL = """
    <div class="am-run">
      <button class="am-btn" id="am-go" type="button">Check next 3 brands</button>
      <button class="am-btn am-ghost" id="am-run" type="button">Run the whole morning now</button>
      <button class="am-btn am-ghost" id="am-mail" type="button">Email me what is ready</button>
      <span class="am-note" id="am-msg">Checks the best brands we have not looked at yet.
        Roughly two minutes each, so this runs in the background.</span>
    </div>
    <script>
      (function(){
        var b=document.getElementById('am-go'), m=document.getElementById('am-msg');
        if(!b) return;
        function poll(){
          fetch('/admin/api/outbound/amazon-scan').then(function(r){return r.json();})
            .then(function(s){
              if(s.running){ m.textContent='Checking... '+s.done+' of '+s.total+' done.';
                             setTimeout(poll, 5000); }
              else { m.textContent=(s.last||'Done.')+' Refresh to see the results.';
                     b.disabled=false; b.textContent='Check next 3 brands'; }
            }).catch(function(){ m.textContent='Lost track of the scan. Refresh the page.'; });
        }
        var runB=document.getElementById('am-run'), mailB=document.getElementById('am-mail');
        if(runB) runB.addEventListener('click', function(){
          runB.disabled=true; m.textContent='Starting the full run...';
          fetch('/admin/api/outbound/run-morning',{method:'POST'})
            .then(function(r){return r.json();})
            .then(function(d){ m.textContent=d.reason||''; if(d.ok){ poll(); } else { runB.disabled=false; } })
            .catch(function(){ m.textContent='Could not reach the server.'; runB.disabled=false; });
        });
        if(mailB) mailB.addEventListener('click', function(){
          mailB.disabled=true; m.textContent='Sending...';
          fetch('/admin/api/outbound/email-batch',{method:'POST'})
            .then(function(r){return r.json();})
            .then(function(d){ m.textContent=d.reason||''; mailB.disabled=false; })
            .catch(function(){ m.textContent='Could not reach the server.'; mailB.disabled=false; });
        });
        b.addEventListener('click', function(){
          b.disabled=true; b.textContent='Starting...'; m.textContent='Starting...';
          var fd=new FormData(); fd.append('limit','3');
          fetch('/admin/api/outbound/amazon-scan',{method:'POST',body:fd})
            .then(function(r){return r.json();})
            .then(function(d){ m.textContent=d.reason||''; if(d.ok&&d.started){ poll(); }
                               else { b.disabled=false; b.textContent='Check next 3 brands'; } })
            .catch(function(){ m.textContent='Could not reach the server.';
                               b.disabled=false; b.textContent='Check next 3 brands'; });
        });
      })();
    </script>
"""


def _amazon_panel(leads: list[dict]) -> str:
    """The Amazon brand control panel for Lead Ops."""
    s = _amazon_scan_summary(leads)

    if not s["scanned"]:
        return ('<div class="am-panel"><p class="am-empty">No scan yet. Run one to see '
                'what is happening on Amazon.</p>' + _AMAZON_SCAN_CONTROL + '</div>')

    counters = "".join(
        f'<div class="am-c"><b>{s[key]:,}</b><span>{label}</span></div>'
        for key, label in (("scanned", "Scanned"), ("findings", "Findings"),
                           ("skipped", "Skipped"), ("absent", "Absent"))
    )

    # The identity, printed rather than assumed, so a truncated scan is obvious.
    reconcile = (f"{s['scanned']:,} scanned = {s['findings']:,} with findings "
                 f"+ {s['skipped']:,} skipped + {s['absent']:,} absent")

    lines = []
    for reason, count in sorted(s["reasons"].items(), key=lambda x: -x[1]):
        lines.append(f'<p class="am-line">{count:,} skipped: '
                     f'{html.escape(_lower_first(reason))}</p>')
    if s["findings"]:
        lines.append(f'<p class="am-line">Of the {s["findings"]:,} with findings, '
                     f'{s["leaking"]:,} show unknown sellers and {s["clean"]:,} look clean.</p>')
    market_split = " ".join(f"{html.escape(m)} ({n:,})" for m, n
                            in sorted(s["markets"].items(), key=lambda x: -x[1]))
    lines.append(f'<p class="am-line">Checked on: {market_split}</p>')
    if s["never_checked"]:
        lines.append(f'<p class="am-line">{s["never_checked"]:,} of the {s["held"]:,} stored '
                     "brands have never been through the Amazon check.</p>")

    return (f'<div class="am-panel"><div class="am-counters">{counters}</div>'
            f'<p class="am-sum">{reconcile}</p>{"".join(lines)}'
            f'{_AMAZON_SCAN_CONTROL}</div>')


_CONF_BADGE = {"high": ("HIGH", "am-high"), "medium": ("MED", "am-med"), "low": ("LOW", "am-low")}


def _confidence_badge(value: Any) -> str:
    label, cls = _CONF_BADGE.get(str(value or "").strip().lower(), ("", ""))
    if not label:
        return '<span class="am-none">-</span>'
    return f'<span class="am-badge {cls}">{label}</span>'


def _marketplace_short(value: Any) -> str:
    """amazon.co.uk reads as .co.uk, which is all the column needs to say."""
    market = str(value or "").strip().lower()
    if not market:
        return '<span class="am-none">-</span>'
    short = market[len("amazon"):] if market.startswith("amazon") else market
    return html.escape(short or market)


def _opening_line_cell(lead: dict) -> str:
    """What we would open with, or why we cannot. A skipped brand keeps its row
    and states its reason rather than vanishing from the list."""
    skipped = str(lead.get("amazon_skipped_reason") or "").strip()
    if skipped:
        return f'<span class="am-skip">Skipped: {html.escape(_lower_first(skipped))}</span>'
    if not str(lead.get("amazon_checked_at") or "").strip():
        return '<span class="am-none">Not checked yet</span>'
    reason = str(lead.get("reason") or "").strip()
    if not reason:
        return '<span class="am-none">Checked, nothing to open with</span>'
    return html.escape(reason)


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

    # Amazon brand control: what the last scan actually covered, and what it did not.
    try:
        from sales_support_agent.services import outbound_memory as _mem3
        amazon_panel = _amazon_panel(_mem3.load_leads(_eng, limit=2000))
    except Exception:  # noqa: BLE001
        amazon_panel = _amazon_panel([])

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

        <p class="lo-h">Amazon brand control</p>
        {amazon_panel}
        <p class="lo-note">Scanned always equals findings plus skipped plus absent. If those
        do not add up, a scan was cut short and the numbers above are hiding it. Skipped is
        not a failure: it is us refusing to guess which listings belong to a brand.</p>

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
        extra_css=_LEADOPS_CSS + _AMAZON_CSS, body=body,
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
                source=result.recipe or "clay_push", config_version=version)
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


_LEADS_CSS = """
  .ld-table { border-collapse:collapse; width:100%; background:#fff; border:1px solid #e5e7eb; border-radius:14px; overflow:hidden; }
  .ld-table th, .ld-table td { text-align:left; padding:9px 12px; border-bottom:1px solid #f0f0f3; font-size:13px; white-space:nowrap; }
  .ld-table th { background:#fafafa; font-size:11px; text-transform:uppercase; letter-spacing:.06em; color:#6b7280; }
  .ld-scroll { max-width:100%; overflow-x:auto; }
  .ld-A{color:#0a7d33;font-weight:800} .ld-B{color:#b54708;font-weight:800} .ld-C{color:#6b7280;font-weight:800}
  .ld-note { margin:12px 0 0; font-size:14px; color:rgba(43,54,68,.7); }
  .ld-stat { display:inline-block; margin-right:22px; font-size:14px; }
  .ld-stat b { font-size:20px; font-family:"Montserrat",sans-serif; }
  .lo-btn { display:inline-block; padding:6px 12px; border-radius:9px; background:#2B3644; color:#fff;
    font-family:"Montserrat",sans-serif; font-weight:800; font-size:11px; text-decoration:none; white-space:nowrap; }
  .lo-btn:hover { background:#1f2833; color:#fff; }
"""


@router.get("/admin/outbound/leads", response_class=HTMLResponse)
def outbound_leads(request: Request) -> Response:
    """Our own record of every brand sourced. Clay and Instantly process these;
    the leads themselves live here, so losing a tool never loses the leads."""
    from sales_support_agent.services import outbound_memory

    try:
        from sales_support_agent.models.database import get_engine
        engine = get_engine()
    except Exception:  # noqa: BLE001
        engine = None
    leads = outbound_memory.load_leads(engine, limit=500)

    tiers: dict[str, int] = {}
    niches: dict[str, int] = {}
    revenue = 0
    for l in leads:
        tiers[l.get("tier") or "-"] = tiers.get(l.get("tier") or "-", 0) + 1
        if l.get("niche"):
            niches[l["niche"]] = niches.get(l["niche"], 0) + 1
        revenue += int(l.get("revenue_cents") or 0)
    avg = f"${revenue // max(len(leads), 1) // 100:,}" if leads else "-"

    if leads:
        rows = "".join(
            f"<tr><td class='ld-{html.escape(str(l.get('tier') or '-'))}'>{html.escape(str(l.get('tier') or '-'))}</td>"
            f"<td>{html.escape(str(l.get('brand') or '-'))}</td>"
            f"<td>{html.escape(str(l.get('domain') or ''))}</td>"
            f"<td>{html.escape(str(l.get('niche') or '-'))}</td>"
            f"<td>{html.escape(str(l.get('country') or '-'))}</td>"
            f"<td>${(int(l.get('revenue_cents') or 0)//100):,}</td>"
            f"<td>{html.escape(str(l.get('score') if l.get('score') is not None else '-'))}</td>"
            f"<td>{_confidence_badge(l.get('amazon_confidence'))}</td>"
            f"<td>{_marketplace_short(l.get('amazon_marketplace'))}</td>"
            f"<td class='am-open'>{_opening_line_cell(l)}</td>"
            f"<td>{html.escape(str(l.get('recipe') or '-'))}</td>"
            f"<td>v{l.get('config_version') or 0}</td>"
            f"<td>{html.escape(str(l.get('first_seen_at'))[:16])}</td>"
            f"<td><a class='lo-btn' href='/admin/outbound/leak-report/"
            f"{html.escape(str(l.get('domain') or ''))}'>Leak report</a></td></tr>"
            for l in leads
        )
    else:
        rows = "<tr><td colspan='14'>No leads stored yet. Pull a batch on Lead Ops.</td></tr>"

    tier_line = " &middot; ".join(f"{k}: {v}" for k, v in sorted(tiers.items())) or "-"
    top_niches = ", ".join(f"{k} ({v})" for k, v in sorted(niches.items(), key=lambda x: -x[1])[:4]) or "-"

    body = f"""
        <h1>Leads</h1>
        <p class="sub">Our own record of every brand we have sourced. Clay enriches these and
        Instantly sends to them, but the leads themselves live here, so losing access to
        either tool never loses the leads.</p>

        <div style="margin:0 0 18px">
          <span class="ld-stat"><b>{len(leads):,}</b> leads held</span>
          <span class="ld-stat"><b>{avg}</b> average size</span>
          <span class="ld-stat">{html.escape(tier_line)}</span>
        </div>
        <p class="ld-note" style="margin:0 0 14px">Top niches: {html.escape(top_niches)}</p>

        <div class="ld-scroll">
        <table class="ld-table">
          <thead><tr><th>Tier</th><th>Brand</th><th>Domain</th><th>Niche</th><th>Country</th>
          <th>Revenue/yr</th><th>Score</th><th>Amazon</th><th>Market</th><th>Opening line</th>
          <th>Recipe</th><th>Settings</th><th>Sourced</th><th></th></tr></thead>
          <tbody>{rows}</tbody>
        </table>
        </div>
        <p class="ld-note">Showing the {len(leads):,} most recent. Every row records which
        recipe found it and which settings version was live at the time, so results stay
        attributable.</p>
        <p class="ld-note">Amazon shows how sure we are that we found the right listings.
        A brand we could not match keeps its row and says why, because a brand that
        disappears from this list looks like a brand we never had.</p>
    """
    return HTMLResponse(_shell_page(
        request, active="outbound_leads", title="Outbound Leads",
        extra_css=_LEADS_CSS + _AMAZON_CSS, body=body,
    ))


_LEAK_CSS = """
  .lk-tape { margin:22px 0 0; padding:16px 18px; border:1px solid var(--border); border-radius:10px; }
  .lk-tape-lab { font-size:13px; font-weight:800; letter-spacing:.04em; text-transform:uppercase; }
  .lk-tape-note { font-size:14px; margin:6px 0 12px; max-width:60ch; }
  .lk-tape-in { flex:1; min-width:260px; padding:9px 11px; font-size:14px;
                border:1px solid var(--border); border-radius:8px; }
  .lk-meta { display:flex; gap:34px; flex-wrap:wrap; margin:0 0 18px; }
  .lk-m { display:flex; flex-direction:column; gap:3px; }
  .lk-m span { font-size:11px; text-transform:uppercase; letter-spacing:.06em; color:#6b7280; font-weight:700; }
  .lk-m b { font-family:"Montserrat",sans-serif; font-size:17px; }
  .lk-table { border-collapse:collapse; width:100%; background:#fff; border:1px solid #e5e7eb;
    border-radius:14px; overflow:hidden; margin:6px 0 0; }
  .lk-table th, .lk-table td { text-align:left; padding:10px 13px; border-bottom:1px solid #f0f0f3;
    font-size:13px; vertical-align:top; }
  .lk-table th { background:#fafafa; font-size:11px; text-transform:uppercase; letter-spacing:.06em; color:#6b7280; }
  .lk-num { text-align:right; font-variant-numeric:tabular-nums; white-space:nowrap; }
  .lk-unknown { font-weight:800; color:#b54708; }
  .lk-scroll { max-width:100%; overflow-x:auto; }
  .lk-h { font-size:13px; font-weight:800; letter-spacing:.06em; text-transform:uppercase; color:#6b7280; margin:24px 0 8px; }
  .lk-caveat { margin:8px 0 0; padding:14px 16px; border-radius:14px; background:rgba(133,187,218,.14);
    border:1px solid rgba(43,54,68,.08); font-size:14px; }
  .lk-caveat b { font-family:"Montserrat",sans-serif; }
  .lk-note { margin:10px 0 0; font-size:14px; color:rgba(43,54,68,.7); }
  .lk-btn { display:inline-block; padding:10px 18px; border:none; border-radius:10px; background:#2B3644;
    color:#fff; font-family:"Montserrat",sans-serif; font-weight:800; font-size:13px; cursor:pointer; text-decoration:none; }
  .lk-btn:hover { background:#1f2833; color:#fff; }
  .lk-bar { display:flex; gap:10px; align-items:center; flex-wrap:wrap; margin:20px 0 0; }
  .lk-stale { color:#b54708; font-weight:700; }
  .lk-plain { position:absolute; left:-9999px; top:-9999px; }
"""

_LEAK_COPY_JS = """
    <script>
      (function(){
        var btn=document.getElementById('lk-copy'), box=document.getElementById('lk-plain'),
            msg=document.getElementById('lk-copied');
        if(!btn||!box) return;
        btn.addEventListener('click', function(){
          var done=function(){ msg.textContent='Copied.'; setTimeout(function(){msg.textContent='';},2000); };
          if(navigator.clipboard && navigator.clipboard.writeText){
            navigator.clipboard.writeText(box.value).then(done, function(){ box.select(); document.execCommand('copy'); done(); });
          } else { box.select(); document.execCommand('copy'); done(); }
        });
      })();
    </script>
"""

_CURRENCY = {"amazon.co.uk": "£", "amazon.com": "$", "amazon.ca": "$", "amazon.com.au": "$"}


def _money(value: Any, symbol: str) -> str:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return "-"
    return f"{symbol}{value:,.2f}"


def _erosion_pct(brand_price: Any, cheapest: Any) -> Any:
    """How far below the brand's OWN current price the cheapest unknown offer sits.
    Never measured against a struck-through list price."""
    if not isinstance(brand_price, (int, float)) or isinstance(brand_price, bool):
        return None
    if not isinstance(cheapest, (int, float)) or isinstance(cheapest, bool):
        return None
    if brand_price <= 0 or cheapest >= brand_price:
        return None
    return (brand_price - cheapest) / brand_price * 100.0


@router.get("/admin/outbound/leak-report/{domain}", response_class=HTMLResponse)
def outbound_leak_report(request: Request, domain: str, refresh: int = 0) -> Response:
    """One brand's Amazon picture, in a form that can be shown to that brand.

    The stored record is the summary only, because listing level detail is not
    kept between runs. Asking for the detail re-checks Amazon live rather than
    dressing up an old number, which on a page a prospect may read is the
    difference between a useful report and a wrong one.
    """
    import outbound_amazon as _az
    import outbound_recipes as _rx
    from sales_support_agent.services import outbound_memory, outbound_settings as _st

    try:
        from sales_support_agent.models.database import get_engine
        engine = get_engine()
    except Exception:  # noqa: BLE001
        engine = None

    wanted = str(domain or "").strip().lower()
    lead = next((l for l in outbound_memory.load_leads(engine, limit=5000)
                 if str(l.get("domain") or "").strip().lower() == wanted), None)

    if lead is None:
        body = f"""
        <h1>Leak report</h1>
        <p class="sub">We hold no record for {html.escape(wanted or "that brand")}. Only brands
        we have sourced can have a report. Check the domain, or pull a batch on Lead Ops.</p>
        <p><a class="lk-btn" href="/admin/outbound/leads">Back to leads</a></p>
        """
        return HTMLResponse(_shell_page(
            request, active="outbound_leads", title="Leak Report",
            extra_css=_LEAK_CSS, body=body,
        ))

    tunables = _st.effective(engine, _rx.DEFAULT_SETTINGS)
    brand = str(lead.get("brand") or "").strip() or wanted
    marketplace = str(lead.get("amazon_marketplace") or "").strip()
    checked_at = str(lead.get("amazon_checked_at") or "").strip()
    skipped = str(lead.get("amazon_skipped_reason") or "").strip()
    symbol = _CURRENCY.get(marketplace, "")

    fresh = _az.finding_is_fresh(checked_at, settings=tunables) if checked_at else False
    if not checked_at:
        stamp = "Never checked"
    elif fresh:
        stamp = html.escape(checked_at)
    else:
        stamp = (f'{html.escape(checked_at)} <span class="lk-stale">(old enough that Amazon '
                 "has likely moved)</span>")

    # The live re-check, only when explicitly asked for. brand_control never raises.
    result: dict[str, Any] = {}
    if refresh:
        try:
            result = _az.brand_control(
                brand, wanted, str(lead.get("country") or ""),
                niche=str(lead.get("niche") or ""), settings=tunables,
            )
        except Exception:  # noqa: BLE001 - a report must render even if the check dies
            logger.exception("[outbound] live Amazon re-check failed for %s", wanted)
            result = {}

    findings = result.get("findings") if isinstance(result.get("findings"), dict) else {}
    listings = findings.get("listings") or []
    sponsored = findings.get("sponsored_competitors") or []
    live_skip = str(result.get("skipped_reason") or "").strip()

    plain: list[str] = [
        f"Amazon report: {brand} ({wanted})",
        f"Marketplace: {marketplace or 'not recorded'}",
        f"Checked: {checked_at or 'never'}",
        "",
    ]

    if listings:
        rows = []
        for item in listings:
            pct = _erosion_pct(item.get("brand_price"), item.get("cheapest"))
            pct_cell = f"{pct:.0f}% below" if pct is not None else "-"
            title = str(item.get("title") or "-")
            rows.append(
                f"<tr><td>{html.escape(title[:70])}<br>"
                f"<span style='color:rgba(43,54,68,.55)'>{html.escape(str(item.get('asin') or ''))}</span></td>"
                f"<td class='lk-num lk-unknown'>{int(item.get('sellers_unknown') or 0):,}</td>"
                f"<td class='lk-num'>{int(item.get('sellers_retailer') or 0):,}</td>"
                f"<td class='lk-num'>{int(item.get('sellers_used') or 0):,}</td>"
                f"<td class='lk-num'>{_money(item.get('brand_price'), symbol)}</td>"
                f"<td class='lk-num'>{_money(item.get('cheapest'), symbol)}</td>"
                f"<td class='lk-num'>{pct_cell}</td>"
                f"<td>{'in stock' if item.get('in_stock') else 'not confirmed'}</td></tr>"
            )
            plain.append(
                f"- {title[:70]} | unknown sellers {int(item.get('sellers_unknown') or 0)} "
                f"| retailers {int(item.get('sellers_retailer') or 0)} "
                f"| your price {_money(item.get('brand_price'), symbol)} "
                f"| cheapest other offer {_money(item.get('cheapest'), symbol)}"
                + (f" ({pct:.0f}% below)" if pct is not None else "")
            )
        listing_block = f"""
        <div class="lk-scroll">
        <table class="lk-table">
          <thead><tr><th>Listing</th><th class="lk-num">Unknown</th><th class="lk-num">Retailer</th>
          <th class="lk-num">Used</th><th class="lk-num">Your price</th>
          <th class="lk-num">Cheapest other</th><th class="lk-num">Difference</th><th>Stock</th></tr></thead>
          <tbody>{''.join(rows)}</tbody>
        </table>
        </div>
        <p class="lk-note">Unknown and retailer are counted separately on purpose. A known
        retailer on a listing is normal trade. An unknown seller is the one worth asking about.
        The difference column compares the cheapest unknown offer against the brand's own
        current selling price, never against a struck-through list price.</p>
        """
    elif refresh and live_skip:
        listing_block = (f'<p class="lk-note">No listing detail: {html.escape(_lower_first(live_skip))}. '
                         "Nothing is being hidden here, we simply could not stand behind a match.</p>")
        plain.append(f"No listing detail: {_lower_first(live_skip)}")
    elif refresh:
        listing_block = ('<p class="lk-note">The check ran and found no listings we could '
                         "confirm belong to this brand.</p>")
        plain.append("The check ran and found no listings we could confirm belong to this brand.")
    else:
        listing_block = ('<p class="lk-note">Listing level detail is not kept between runs, '
                         "because a stored price goes stale within hours. Run a fresh check "
                         "to fill in the table below it.</p>")

    if sponsored:
        items = "".join(f"<li>{html.escape(str(s))}</li>" for s in sponsored)
        sponsored_block = f"<ul style='margin:6px 0 0;padding-left:20px;font-size:14px'>{items}</ul>"
        plain.append("")
        plain.append("Competitors sponsored on your brand name: "
                     + ", ".join(str(s) for s in sponsored))
    elif refresh:
        sponsored_block = '<p class="lk-note">No competitors were sponsored on the brand name at check time.</p>'
    else:
        sponsored_block = '<p class="lk-note">Run a fresh check to see who is buying ads on this brand name.</p>'

    plain.extend([
        "",
        "Two things to be straight about:",
        "- We cannot tell which of these sellers you have authorized. That is the question "
        "to ask, not something we can answer from the outside.",
        "- Sponsored placements are an auction and change often, so this is a snapshot "
        "rather than a fixed picture.",
    ])

    stored_note = (f"{int(lead.get('amazon_sellers_unknown') or 0):,} unknown sellers"
                   if not lead.get("amazon_absent") else "absent from this marketplace")

    body = f"""
        <h1>Leak report: {html.escape(brand)}</h1>
        <p class="sub">What we can see on Amazon for {html.escape(wanted)}, and what we
        deliberately cannot. Safe to share with the brand.</p>

        <div class="lk-meta">
          <div class="lk-m"><span>Marketplace</span><b>{html.escape(marketplace or "not recorded")}</b></div>
          <div class="lk-m"><span>Match confidence</span><b>{_confidence_badge(lead.get("amazon_confidence"))}</b></div>
          <div class="lk-m"><span>Last stored check</span><b>{stamp}</b></div>
          <div class="lk-m"><span>Stored result</span><b>{html.escape(stored_note)}</b></div>
        </div>

        {f'<p class="lk-note">The stored check was skipped: {html.escape(_lower_first(skipped))}.</p>' if skipped else ''}

        <p class="lk-h">Listings</p>
        {listing_block}

        <p class="lk-h">Sponsored on this brand name</p>
        {sponsored_block}

        <p class="lk-h">Before you read anything into this</p>
        <div class="lk-caveat"><b>We cannot tell which sellers are authorized.</b>
        Some of the sellers above may be distributors or resellers the brand has approved,
        and from the outside those look identical to the ones it has not. This report says
        who is on the listing, not who is allowed to be. That is the question to put to the brand.</div>
        <div class="lk-caveat"><b>Sponsored placements are an auction.</b>
        Who appears on a brand name search changes daily, and often hourly. Treat the list
        above as a snapshot taken at the time stamped on this page, not a standing fact.</div>

        <div class="lk-bar">
          <a class="lk-btn" href="/admin/outbound/leak-report/{html.escape(wanted)}?refresh=1">Run a fresh check</a>
          <button class="lk-btn" id="lk-copy" type="button">Copy as plain text</button>
          <span id="lk-copied" style="font-size:14px;font-weight:700"></span>
          <a class="lk-btn" href="/admin/outbound/leads" style="background:rgba(43,54,68,.12);color:#2B3644">Back to leads</a>
        </div>
        <textarea class="lk-plain" id="lk-plain" readonly>{html.escape(chr(10).join(plain))}</textarea>

        <div class="lk-tape">
          <div class="lk-tape-lab">Tape recording for {html.escape(brand)}</div>
          <p class="lk-tape-note">Record this report in Tape, press Copy share link, and paste it
          here. It travels out with the brand on every future export, so it is typed once.
          Send it as the reply when they answer, never in the first email: a link in a cold
          opener is a deliverability hit, and this one points at anatainc.com.</p>
          <div class="lk-bar">
            <input class="lk-tape-in" id="lk-tape" type="url" spellcheck="false"
                   placeholder="https://tape.anatainc.com/share/..."
                   value="{html.escape(str(lead.get('video_url') or ''))}">
            <button class="lk-btn" id="lk-tape-save" type="button">Save link</button>
            <span id="lk-tape-msg" style="font-size:14px;font-weight:700"></span>
          </div>
        </div>
        {_LEAK_COPY_JS}
        <script>
          (function(){{
            var i=document.getElementById('lk-tape'), b=document.getElementById('lk-tape-save'),
                m=document.getElementById('lk-tape-msg');
            if(!i||!b) return;
            b.addEventListener('click', function(){{
              b.disabled=true; m.textContent='Saving...';
              fetch('/admin/api/outbound/video-url',{{method:'POST',
                headers:{{'Content-Type':'application/json'}},
                body:JSON.stringify({{domain:{json.dumps(wanted)}, url:i.value}})}})
                .then(function(r){{return r.json();}})
                .then(function(d){{ m.textContent=d.reason||''; b.disabled=false; }})
                .catch(function(){{ m.textContent='Could not reach the server.'; b.disabled=false; }});
            }});
          }})();
        </script>
    """
    return HTMLResponse(_shell_page(
        request, active="outbound_leads", title="Leak Report",
        extra_css=_LEAK_CSS + _AMAZON_CSS, body=body,
    ))


# One scan at a time, process-wide. Two scans would double the spend at the data
# provider and race each other writing the same rows.
_AMAZON_SCAN: dict[str, Any] = {"running": False, "done": 0, "total": 0, "started": "", "last": ""}


def _run_amazon_scan(domains: list[str], tunables: dict[str, Any]) -> None:
    """Check a short list of brands, saving each finding as it lands.

    Runs after the response has already gone back, because one brand costs
    minutes. Saving per brand means a deploy or a restart halfway through keeps
    everything found so far instead of losing the batch.
    """
    from sales_support_agent.services import outbound_memory

    check = _amazon_checker(tunables)
    try:
        from sales_support_agent.models.database import get_engine
        engine = get_engine()
    except Exception:  # noqa: BLE001
        engine = None

    if check is None or engine is None:
        _AMAZON_SCAN.update(running=False, last="Could not start: no Amazon key, or the check is switched off.")
        return

    for domain in domains:
        try:
            lead = next((l for l in outbound_memory.load_leads(engine, limit=1000)
                         if str(l.get("domain") or "") == domain), None)
            if lead is None:
                continue
            outbound_memory.update_amazon_finding(engine, domain, check(lead))
        except Exception:  # noqa: BLE001
            logger.exception("[outbound] amazon scan failed on %s", domain)
        finally:
            _AMAZON_SCAN["done"] = _AMAZON_SCAN.get("done", 0) + 1

    _AMAZON_SCAN.update(running=False, last=f"Finished {_AMAZON_SCAN['done']} of {_AMAZON_SCAN['total']}.")


@router.post("/admin/api/outbound/amazon-scan", response_class=JSONResponse)
async def outbound_amazon_scan(request: Request) -> Response:
    """Check the next few brands on Amazon, best ones first.

    Deliberately small. Each brand costs a couple of minutes and real money at
    the data provider, so we only ever look up brands we would actually email,
    a few at a time, when asked.
    """
    from starlette.background import BackgroundTask

    from sales_support_agent.services import outbound_memory, outbound_settings as _st
    import outbound_recipes as _rx

    if _AMAZON_SCAN.get("running"):
        return JSONResponse(status_code=409, content={
            "ok": False,
            "reason": f"A scan is already running ({_AMAZON_SCAN.get('done', 0)} of {_AMAZON_SCAN.get('total', 0)} done).",
        })

    form = await request.form()
    try:
        limit = max(1, min(int(str(form.get("limit") or 3)), 10))
    except (TypeError, ValueError):
        limit = 3

    try:
        from sales_support_agent.models.database import get_engine
        engine = get_engine()
    except Exception:  # noqa: BLE001
        engine = None

    tunables = _st.effective(engine, _rx.DEFAULT_SETTINGS) if engine is not None else _rx.DEFAULT_SETTINGS
    import outbound_amazon as _az
    max_age = _az._int_setting(tunables, "amazon.finding_max_age_days")
    pending = outbound_memory.leads_needing_amazon(engine, limit=limit, max_age_days=max_age)
    domains = [str(l.get("domain") or "") for l in pending if l.get("domain")]

    if not domains:
        return JSONResponse(content={
            "ok": True, "started": 0,
            "reason": "Nothing to check. Every brand we hold has a recent Amazon result.",
        })

    _AMAZON_SCAN.update(running=True, done=0, total=len(domains),
                        started=datetime.now(timezone.utc).isoformat(timespec="seconds"),
                        last="")
    minutes = len(domains) * 2
    return JSONResponse(
        content={
            "ok": True, "started": len(domains), "brands": domains,
            "reason": f"Checking {len(domains)} brand(s). Expect roughly {minutes} to {minutes * 2} minutes. Refresh to see progress.",
        },
        background=BackgroundTask(_run_amazon_scan, domains, tunables),
    )


@router.get("/admin/api/outbound/amazon-scan", response_class=JSONResponse)
def outbound_amazon_scan_status(request: Request) -> Response:
    """Where the running scan has got to."""
    return JSONResponse(content=dict(_AMAZON_SCAN))


@router.post("/admin/api/outbound/video-url", response_class=JSONResponse)
async def outbound_set_video_url(request: Request) -> JSONResponse:
    """Attach a Tape share link to one brand, or clear it with an empty value.

    The store rejects any host other than tape.anatainc.com. This is the one
    field on the whole pipeline where a person types a URL that later goes out
    inside an email, so it is checked there rather than trusted here.
    """
    from sales_support_agent.models.database import get_engine
    from sales_support_agent.services import outbound_memory

    try:
        payload = await request.json()
    except Exception:  # noqa: BLE001
        payload = {}
    domain = str((payload or {}).get("domain") or "").strip()
    url = str((payload or {}).get("url") or "").strip()
    if not domain:
        return JSONResponse(status_code=400, content={"ok": False, "reason": "No brand given."})

    try:
        engine = get_engine()
    except Exception:  # noqa: BLE001
        return JSONResponse(status_code=503, content={"ok": False, "reason": "No database."})

    if outbound_memory.set_video_url(engine, domain, url):
        return JSONResponse(content={"ok": True, "saved": bool(url),
                                     "reason": "Saved." if url else "Link removed."})
    return JSONResponse(status_code=400, content={
        "ok": False,
        "reason": "Not saved. The link has to be a tape.anatainc.com address, "
                  "and the brand has to be one we already hold."})


@router.post("/admin/api/outbound/run-morning", response_class=JSONResponse)
async def outbound_run_morning(request: Request) -> Response:
    """Run the whole morning routine now: pull, check Amazon, email the batch.

    Backgrounded, because a full run is roughly half an hour of Amazon checks
    and would never return inside a request. The same routine the 7am schedule
    calls, so testing it here tests the real thing rather than a copy.
    """
    from starlette.background import BackgroundTask

    from sales_support_agent.api import outbound_jobs as _jobs

    if _AMAZON_SCAN.get("running"):
        return JSONResponse(status_code=409, content={
            "ok": False,
            "reason": f"A scan is already running ({_AMAZON_SCAN.get('done', 0)} of {_AMAZON_SCAN.get('total', 0)} done)."})

    _AMAZON_SCAN.update(running=True, done=0, total=_jobs._SCAN_PER_DAY,
                        started=datetime.now(timezone.utc).isoformat(timespec="seconds"),
                        last="")

    def _go() -> None:
        try:
            _jobs.run_morning_routine()
        finally:
            _AMAZON_SCAN.update(running=False, last="Morning routine finished. Check your email.")

    return JSONResponse(
        content={"ok": True, "started": True,
                 "reason": "Running now: pulling brands, checking Amazon, then emailing you. "
                           "Roughly 30 minutes. You can close this."},
        background=BackgroundTask(_go),
    )


@router.get("/admin/api/outbound/email-batch", response_class=JSONResponse)
@router.post("/admin/api/outbound/email-batch", response_class=JSONResponse)
async def outbound_email_batch(request: Request) -> Response:
    """Send today's ready-for-Clay list now, rather than waiting for 7am.

    Same email the morning job sends. Useful after a manual scan, or when the
    morning one was missed.

    GET as well as POST, deliberately: this needs to be a link David can click
    from anywhere. It sends him an email he already receives daily, so there is
    nothing here worth protecting behind a form submission.
    """
    from sales_support_agent.api import outbound_jobs as _jobs

    try:
        from sales_support_agent.models.database import get_engine
        engine = get_engine()
    except Exception:  # noqa: BLE001
        engine = None
    if engine is None:
        return JSONResponse(status_code=503, content={
            "ok": False, "reason": "No database, so there is nothing to send."})

    ready = _jobs._sendable_brands(engine)
    if not ready:
        return JSONResponse(content={
            "ok": True, "sent": False, "brands": 0,
            "reason": "No brands have a finding yet, so there is nothing worth emailing."})

    sent = _jobs._email_the_batch(engine, {"pulled": 0, "scanned": len(ready)})
    return JSONResponse(content={
        "ok": True, "sent": bool(sent), "brands": len(ready),
        "reason": (f"Emailed {len(ready)} brand(s)." if sent else
                   "Could not send: no email provider is configured on this service."),
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
