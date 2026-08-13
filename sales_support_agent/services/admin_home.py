"""Human-facing service status and personalized Agent workspace home."""

from __future__ import annotations

import hashlib
import html
import json
import logging
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import PurePosixPath
from zoneinfo import ZoneInfo

from sales_support_agent.models.database import kv_get_json, kv_set_json
from sales_support_agent.services.access.catalog import grants_tool
from sales_support_agent.services.admin_nav import (
    render_agent_favicon_links,
    render_agent_nav,
    render_agent_stylesheet_links,
)

logger = logging.getLogger(__name__)


_WORKSPACES = (
    ("sales", "sales.deals", "Sales", "Move opportunities forward and keep follow-up work visible.", "/admin/sales"),
    ("website_ops", "website_ops.seo", "Website Ops", "Prioritize publishing, indexing, and site-health actions.", "/admin/website-ops"),
    ("content", "content.view", "Content", "Run the Riverside-to-multichannel content engine.", "/admin/content"),
    ("finance", "finance", "Finance", "Review cash, obligations, evidence, and next actions.", "/admin/finances"),
    ("building", "building.manage", "Building", "Manage active builds, agreements, and delivery readiness.", "/admin/building"),
    ("advertising", "advertising.audit", "Advertising", "Audit accounts and resolve performance issues.", "/admin/advertising/audit"),
    ("executive", "executive.summary", "Owner Overview", "Review material company-wide exceptions and next actions.", "/admin/executive"),
    ("fulfillment", "fulfillment.rate_sheets", "Fulfillment", "Manage prospects, assets, and customer-success work.", "/admin/fulfillment/sales"),
    ("hr", "hr.access", "HR", "Manage people, onboarding, time, and employee records.", "/admin/hr"),
)

_KNOWN_RECENT_TITLES = {
    "/admin/sales": "Sales Control Room",
    "/admin/sales/deals": "Sales Deal Board",
    "/admin/sales/reps": "Rep Accountability",
    "/admin/website-ops": "Website Ops Today",
    "/admin/website-ops/content": "Website publishing",
    "/admin/website-ops/site-health": "Website site health",
    "/admin/content": "Content",
    "/admin/finances": "Finance Today",
    "/admin/building": "Building Today",
    "/admin/advertising/audit": "Advertising Audit",
    "/admin/executive": "Owner Overview",
    "/admin/fulfillment/sales": "Prospects & Assets",
    "/admin/fulfillment/cs/": "Customer Success Queue",
    "/admin/hr": "HR Dashboard",
    "/admin/hr/time": "Time & PTO",
}


def accessible_workspaces(user: dict | None) -> list[dict[str, str]]:
    user = user or {}
    permissions: Iterable[str] = user.get("permissions") or ()
    is_superadmin = bool(user.get("is_superadmin"))
    return [
        {"id": workspace_id, "tool_key": tool_key, "title": title, "description": description, "href": href}
        for workspace_id, tool_key, title, description, href in _WORKSPACES
        if is_superadmin or grants_tool(set(permissions), tool_key)
    ]


def _preference_key(email: str) -> str:
    digest = hashlib.sha256((email or "anonymous").strip().lower().encode("utf-8")).hexdigest()[:24]
    return f"agent_home:{digest}"


def get_home_preferences(email: str, workspaces: list[dict[str, str]]) -> dict:
    allowed_ids = [item["id"] for item in workspaces]
    stored = kv_get_json(_preference_key(email), {}) or {}
    shortcuts = [item for item in stored.get("shortcuts", []) if item in allowed_ids]
    if not shortcuts:
        shortcuts = allowed_ids[:4]
    recent = [
        item for item in stored.get("recent", [])
        if isinstance(item, dict) and _valid_recent_path(str(item.get("path") or ""))
    ][:4]
    return {"shortcuts": shortcuts[:6], "recent": recent}


def save_home_shortcuts(email: str, shortcut_ids: list[str], workspaces: list[dict[str, str]]) -> None:
    allowed = {item["id"] for item in workspaces}
    current = get_home_preferences(email, workspaces)
    current["shortcuts"] = list(dict.fromkeys(item for item in shortcut_ids if item in allowed))[:6]
    kv_set_json(_preference_key(email), current)


def _valid_recent_path(path: str) -> bool:
    return (
        path.startswith("/admin/")
        and not path.startswith(("/admin/login", "/admin/logout", "/admin/auth", "/admin/home"))
        and "?" not in path
        and "#" not in path
    )


def record_recent_page(email: str, path: str, workspaces: list[dict[str, str]]) -> bool:
    if not _valid_recent_path(path):
        return False
    current = get_home_preferences(email, workspaces)
    title = _KNOWN_RECENT_TITLES.get(path)
    if not title:
        name = PurePosixPath(path.rstrip("/")).name.replace("-", " ").strip()
        title = name.title() if name else "Agent page"
    recent = [item for item in current["recent"] if item.get("path") != path]
    current["recent"] = [{"path": path, "title": title}, *recent][:4]
    kv_set_json(_preference_key(email), current)
    return True


def clear_recent_pages(email: str, workspaces: list[dict[str, str]]) -> None:
    current = get_home_preferences(email, workspaces)
    current["recent"] = []
    kv_set_json(_preference_key(email), current)


def build_home_context(user: dict, session_factory) -> dict:
    """Build conservative, user-owned home context without external calls."""
    email = (user.get("email") or "").strip().lower()
    permissions = set(user.get("permissions") or set())
    is_superadmin = bool(user.get("is_superadmin"))
    context: dict = {"clock": None, "needs_you": []}

    def can(tool_key: str) -> bool:
        return is_superadmin or grants_tool(permissions, tool_key)

    if can("hr.access"):
        try:
            from sales_support_agent.services.hr import store as hr_store

            employee = hr_store.get_employee_by_email(email)
            if employee:
                record_email = (employee.get("email") or email).strip().lower()
                open_clock = hr_store.current_clock(record_email)
                summary = hr_store.time_clock_summary(record_email)
                today = datetime.now(ZoneInfo("America/Denver")).date()
                today_entries = [
                    item for item in hr_store.list_time_entries(record_email, limit=20)
                    if item.get("date") == today
                ]
                today_hours = sum(float(item.get("hours") or 0) for item in today_entries)
                if open_clock:
                    today_hours += float(summary.get("open_elapsed_hours") or 0)
                if open_clock:
                    started = open_clock.get("clocked_in_at")
                    started_label = started.astimezone(ZoneInfo("America/Denver")).strftime("%I:%M %p").lstrip("0") if started else "today"
                    last_event = f"Clocked in {started_label}"
                elif summary.get("last_shift"):
                    shift = summary["last_shift"]
                    last_event = f"Last clock out {shift.get('stop_time') or 'recorded'}"
                else:
                    last_event = "No time recorded yet"
                context["clock"] = {
                    "is_clocked_in": bool(open_clock),
                    "last_event": last_event,
                    "today_hours": round(today_hours, 2),
                }
                if not employee.get("onboarding_complete"):
                    context["needs_you"].append({
                        "workspace": "HR", "title": "Finish your employee onboarding",
                        "detail": "Complete the remaining profile, tax, identity, or policy steps.",
                        "href": "/admin/hr/onboarding", "priority": 10,
                    })
        except Exception:  # Home remains usable if HR data is unavailable.
            logger.exception("Unable to build HR home context")

    try:
        from sales_support_agent.models.entities import BuildingInquiry, FinanceSavingsReview, HubSpotDeal

        with session_factory() as session:
            if can("sales.deals") and email:
                deals = (
                    session.query(HubSpotDeal)
                    .filter(HubSpotDeal.is_closed.is_(False), HubSpotDeal.owner_email == email)
                    .order_by(HubSpotDeal.next_follow_up_at.asc().nullsfirst(), HubSpotDeal.close_date.asc().nullsfirst())
                    .limit(10).all()
                )
                now = datetime.now(timezone.utc)
                for deal in deals:
                    follow_up = deal.next_follow_up_at
                    if follow_up and follow_up.tzinfo is None:
                        follow_up = follow_up.replace(tzinfo=timezone.utc)
                    needs_attention = (
                        (follow_up is not None and follow_up <= now)
                        or deal.follow_up_state in {"overdue", "needs_review", "stale"}
                    )
                    if needs_attention:
                        context["needs_you"].append({
                            "workspace": "Sales", "title": deal.deal_name or "Sales follow-up",
                            "detail": deal.recommended_next_action or "Review the deal and record the next action.",
                            "href": f"/admin/sales/deals/{deal.hubspot_deal_id}", "priority": 20,
                        })
                        break
            if can("building.manage") and email:
                inquiry = (
                    session.query(BuildingInquiry)
                    .filter(BuildingInquiry.assigned_owner == email, BuildingInquiry.status.in_(("new", "open", "needs_response")))
                    .order_by(BuildingInquiry.response_due_at.asc().nullsfirst()).first()
                )
                if inquiry:
                    context["needs_you"].append({
                        "workspace": "Building", "title": f"Respond to {inquiry.name}",
                        "detail": "A Building inquiry assigned to you still needs a response.",
                        "href": "/admin/building", "priority": 30,
                    })
            if can("finance") and email:
                review = (
                    session.query(FinanceSavingsReview)
                    .filter(FinanceSavingsReview.owner == email, FinanceSavingsReview.state.in_(("reviewing", "needs_review")))
                    .order_by(FinanceSavingsReview.updated_at.asc()).first()
                )
                if review:
                    context["needs_you"].append({
                        "workspace": "Finance", "title": review.display_name or "Review a savings decision",
                        "detail": review.reason or "Review the evidence and choose the next action.",
                        "href": "/admin/finances/review", "priority": 40,
                    })
    except Exception:  # Home remains usable if an operational table is unavailable.
        logger.exception("Unable to build assigned home context")

    context["needs_you"] = sorted(context["needs_you"], key=lambda item: item["priority"])[:5]
    return context


def _document(*, title: str, body: str, description: str, nav: str = "") -> str:
    return f"""<!doctype html><html lang="en"><head>
  <meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="description" content="{html.escape(description, quote=True)}"><title>{html.escape(title)}</title>
  {render_agent_favicon_links()}<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Montserrat:wght@700;800&display=swap" rel="stylesheet">{render_agent_stylesheet_links()}
  <style>
    .agent-home{{padding-block:34px 72px}}.agent-home__header{{display:flex;align-items:end;justify-content:space-between;gap:24px;margin-bottom:24px}}.agent-home__eyebrow,.home-kicker{{color:var(--agent-blue-strong);font:800 .72rem/1.2 "Montserrat",sans-serif;letter-spacing:.08em;text-transform:uppercase}}.agent-home h1{{margin:7px 0 8px;color:var(--agent-ink);font:800 clamp(1.9rem,3.4vw,2.8rem)/1.06 "Montserrat",sans-serif;letter-spacing:-.04em}}.agent-home__intro{{margin:0;color:var(--agent-ink-muted);line-height:1.55}}.home-section{{margin-top:26px}}.home-section__head{{display:flex;align-items:end;justify-content:space-between;gap:16px;margin-bottom:12px}}.home-section h2{{margin:0;color:var(--agent-ink);font:800 1.25rem/1.2 "Montserrat",sans-serif}}.home-section__hint{{margin:4px 0 0;color:var(--agent-ink-muted);font-size:.88rem}}.home-panel{{border:1px solid var(--agent-border);border-radius:var(--agent-radius-card);background:var(--agent-surface);box-shadow:0 10px 28px var(--agent-shadow)}}
    .clock-bar{{display:grid;grid-template-columns:auto minmax(0,1fr) auto auto;align-items:center;gap:18px;padding:15px 18px}}.clock-state{{display:flex;align-items:center;gap:9px;font:800 .82rem/1.2 "Montserrat",sans-serif}}.clock-dot{{width:9px;height:9px;border-radius:50%;background:var(--agent-good)}}.clock-dot--out{{background:var(--agent-ink-muted)}}.clock-meta{{color:var(--agent-ink-muted);font-size:.85rem}}.clock-hours{{font:800 .9rem/1.2 "Montserrat",sans-serif}}.home-button,.home-link-button{{min-height:40px;display:inline-flex;align-items:center;justify-content:center;padding:0 15px;border:1px solid var(--agent-border);border-radius:var(--agent-radius-control);background:var(--agent-surface);color:var(--agent-ink);font:800 .78rem/1 "Montserrat",sans-serif;text-decoration:none;cursor:pointer}}.home-button--primary{{border-color:var(--agent-ink);background:var(--agent-ink);color:#fff}}
    .needs-list{{display:grid}}.need-item{{display:grid;grid-template-columns:100px minmax(0,1fr) auto;align-items:center;gap:18px;padding:16px 18px;color:var(--agent-ink);text-decoration:none}}.need-item+.need-item{{border-top:1px solid var(--agent-border)}}.need-workspace{{color:var(--agent-blue-strong);font:800 .69rem/1.2 "Montserrat",sans-serif;letter-spacing:.06em;text-transform:uppercase}}.need-copy strong{{display:block;margin-bottom:4px;font:800 .92rem/1.3 "Montserrat",sans-serif}}.need-copy span{{color:var(--agent-ink-muted);font-size:.86rem;line-height:1.45}}.need-arrow{{color:var(--agent-blue-strong);font-weight:800}}.home-empty{{padding:20px;color:var(--agent-ink-muted)}}
    .shortcut-grid,.workspace-grid{{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px}}.shortcut-card,.workspace-card{{min-width:0;display:flex;flex-direction:column;padding:18px;border:1px solid var(--agent-border);border-radius:var(--agent-radius-panel);background:var(--agent-surface);color:var(--agent-ink);text-decoration:none}}.shortcut-card{{min-height:112px}}.workspace-card{{min-height:158px}}.shortcut-card:hover,.workspace-card:hover{{border-color:var(--agent-blue)}}.shortcut-card strong,.workspace-card h3{{margin:0 0 7px;font:800 .98rem/1.3 "Montserrat",sans-serif}}.shortcut-card span,.workspace-card p{{margin:0;color:var(--agent-ink-muted);font-size:.85rem;line-height:1.45}}.workspace-card em{{margin-top:auto;padding-top:16px;color:var(--agent-blue-strong);font:800 .72rem/1.2 "Montserrat",sans-serif;font-style:normal}}.recent-list{{display:flex;gap:8px;flex-wrap:wrap}}.recent-link{{display:inline-flex;align-items:center;min-height:36px;padding:0 12px;border:1px solid var(--agent-border);border-radius:999px;background:var(--agent-surface);color:var(--agent-ink);font-size:.82rem;text-decoration:none}}.subtle-action{{border:0;background:transparent;color:var(--agent-ink-muted);font:700 .75rem/1 "Montserrat",sans-serif;cursor:pointer;text-decoration:underline;text-underline-offset:3px}}
    .shortcut-editor{{position:relative}}.shortcut-editor>summary{{list-style:none}}.shortcut-editor>summary::-webkit-details-marker{{display:none}}.shortcut-editor__panel{{position:absolute;right:0;z-index:20;width:min(440px,calc(100vw - 48px));margin-top:8px;padding:18px;border:1px solid var(--agent-border);border-radius:var(--agent-radius-panel);background:#fff;box-shadow:0 20px 48px rgba(43,54,68,.18)}}.shortcut-editor__panel h3{{margin:0 0 5px;font:800 1rem/1.3 "Montserrat",sans-serif}}.shortcut-editor__panel>p{{margin:0 0 14px;color:var(--agent-ink-muted);font-size:.84rem}}.shortcut-option{{display:grid;grid-template-columns:auto minmax(0,1fr) 74px;align-items:center;gap:10px;padding:9px 0}}.shortcut-option+.shortcut-option{{border-top:1px solid var(--agent-border)}}.shortcut-option select{{min-height:36px;border:1px solid var(--agent-border);border-radius:8px;background:#fff}}.shortcut-save{{margin-top:14px;display:flex;justify-content:flex-end}}
    .status-shell{{min-height:100vh;display:grid;place-items:center;padding-block:48px}}.status-panel{{width:min(720px,100%);padding:clamp(28px,6vw,52px);border:1px solid var(--agent-border);border-radius:var(--agent-radius-card);background:var(--agent-surface);box-shadow:0 18px 50px var(--agent-shadow)}}.status-mark{{width:52px;height:52px;display:grid;place-items:center;border-radius:50%;background:var(--agent-blue-soft);color:var(--agent-good);font:800 1.4rem/1 "Montserrat",sans-serif}}.status-panel h1{{margin:22px 0 8px;font:800 clamp(2rem,5vw,3rem)/1.06 "Montserrat",sans-serif;letter-spacing:-.04em}}.status-panel>p{{color:var(--agent-ink-muted);line-height:1.6}}.status-list{{display:grid;gap:10px;margin:28px 0}}.status-row{{display:flex;justify-content:space-between;gap:20px;padding:14px 0;border-bottom:1px solid var(--agent-border)}}.status-row strong{{font:700 .85rem/1.3 "Montserrat",sans-serif}}.status-actions{{display:flex;flex-wrap:wrap;gap:10px}}.status-actions a{{min-height:var(--agent-control-height);display:inline-flex;align-items:center;padding:0 16px;border-radius:var(--agent-radius-control);font:700 .82rem/1 "Montserrat",sans-serif;text-decoration:none}}.status-actions__primary{{background:var(--agent-ink);color:white}}.status-actions__secondary{{border:1px solid var(--agent-border);color:var(--agent-ink)}}
    @media(max-width:900px){{.shortcut-grid,.workspace-grid{{grid-template-columns:repeat(2,minmax(0,1fr))}}.clock-bar{{grid-template-columns:auto 1fr auto}}.clock-hours{{display:none}}}}@media(max-width:620px){{.shortcut-grid,.workspace-grid{{grid-template-columns:1fr}}.agent-home{{padding-block:26px 48px}}.agent-home__header{{align-items:start;flex-direction:column}}.clock-bar{{grid-template-columns:1fr auto}}.clock-meta{{grid-column:1/-1}}.need-item{{grid-template-columns:1fr auto}}.need-workspace{{grid-column:1/-1}}.shortcut-editor__panel{{position:fixed;left:16px;right:16px;top:82px;width:auto;max-height:75vh;overflow:auto}}}}@media(prefers-reduced-motion:reduce){{*{{scroll-behavior:auto!important;transition:none!important}}}}
  </style></head><body class="app">{nav}{body}</body></html>"""


def render_service_status_page(*, ready: bool) -> str:
    state = "Operational" if ready else "Starting"
    detail = "Agent is online and ready for authenticated work." if ready else "Agent is online and completing startup checks."
    mark = "✓" if ready else "…"
    body = f"""<main id="agent-main-content" class="app-container status-shell"><section class="status-panel" aria-labelledby="status-title"><div class="status-mark" aria-hidden="true">{mark}</div><h1 id="status-title">Agent is {state.lower()}.</h1><p>{html.escape(detail)}</p><div class="status-list" aria-label="Service status"><div class="status-row"><span>Application</span><strong>{state}</strong></div><div class="status-row"><span>Secure access</span><strong>Available</strong></div><div class="status-row"><span>Environment</span><strong>Staging</strong></div></div><div class="status-actions"><a class="status-actions__primary" href="/admin">Open Agent</a><a class="status-actions__secondary" href="/health/ready">Technical status</a></div></section></main>"""
    return _document(title="Agent status", description="Current Anata Agent service status.", body=body)


def render_admin_home_page(*, user: dict | None = None, preferences: dict | None = None, context: dict | None = None, flash: str = "") -> str:
    user = user or {}; context = context or {}; workspaces = accessible_workspaces(user)
    preferences = preferences or get_home_preferences(user.get("email") or "", workspaces)
    workspace_by_id = {item["id"]: item for item in workspaces}
    shortcuts = [workspace_by_id[item] for item in preferences.get("shortcuts", []) if item in workspace_by_id]
    shortcut_cards = "".join(f'<a class="shortcut-card" href="{html.escape(item["href"],quote=True)}"><strong>{html.escape(item["title"])}</strong><span>{html.escape(item["description"])}</span></a>' for item in shortcuts)
    workspace_cards = "".join(f'<a class="workspace-card" href="{html.escape(item["href"],quote=True)}"><h3>{html.escape(item["title"])}</h3><p>{html.escape(item["description"])}</p><em>Open workspace →</em></a>' for item in workspaces)
    shortcut_options = "".join(f'<label class="shortcut-option"><input type="checkbox" name="shortcut" value="{item["id"]}" {"checked" if item["id"] in preferences.get("shortcuts",[]) else ""}><span>{html.escape(item["title"])}</span><select name="order_{item["id"]}" aria-label="{html.escape(item["title"])} position">'+"".join(f'<option value="{n}" {"selected" if preferences.get("shortcuts",[]).index(item["id"])+1==n else ""}>{n}</option>' if item["id"] in preferences.get("shortcuts",[]) else f'<option value="{n}">{n}</option>' for n in range(1,7))+"</select></label>" for item in workspaces)
    needs = context.get("needs_you") or []
    needs_html = "".join(f'<a class="need-item" href="{html.escape(item["href"], quote=True)}"><span class="need-workspace">{html.escape(item["workspace"])}</span><span class="need-copy"><strong>{html.escape(item["title"])}</strong><span>{html.escape(item["detail"])}</span></span><span class="need-arrow" aria-hidden="true">→</span></a>' for item in needs) or '<div class="home-empty">Nothing assigned to you needs attention right now.</div>'
    recent = preferences.get("recent") or []
    recent_html = "".join(f'<a class="recent-link" href="{html.escape(item["path"],quote=True)}">{html.escape(item["title"])}</a>' for item in recent)
    recent_section = f'<section class="home-section" aria-labelledby="recent-title"><div class="home-section__head"><div><div class="home-kicker">Continue</div><h2 id="recent-title">Recent</h2></div><form method="post" action="/admin/home/recent/clear"><button class="subtle-action" type="submit">Clear</button></form></div><div class="recent-list">{recent_html}</div></section>' if recent_html else ""
    clock = context.get("clock")
    clock_html = ""
    if clock:
        clocked_in = bool(clock.get("is_clocked_in")); action = "out" if clocked_in else "in"; label = "Clock out" if clocked_in else "Clock in"
        clock_html = f'<section class="home-section" aria-label="Time clock"><div class="home-panel clock-bar"><div class="clock-state"><span class="clock-dot {"" if clocked_in else "clock-dot--out"}" aria-hidden="true"></span>{"Clocked in" if clocked_in else "Clocked out"}</div><div class="clock-meta">{html.escape(clock.get("last_event") or "")}</div><div class="clock-hours">{float(clock.get("today_hours") or 0):.2f} hours today</div><form method="post" action="/admin/home/clock"><input type="hidden" name="action" value="{action}"><button class="home-button home-button--primary" type="submit">{label}</button></form></div></section>'
    name = (user.get("name") or user.get("email") or "").strip().split(" ",1)[0]
    greeting = f"Welcome back, {html.escape(name)}." if name else "Welcome to Agent."
    flash_html = f'<div class="app-status app-status--confirmed" role="status">{html.escape(flash.replace("_"," ").title())}</div>' if flash else ""
    nav = render_agent_nav(user=user, include_content_target=False)
    body = f"""<main id="agent-main-content" class="app-container agent-home"><header class="agent-home__header"><div><div class="agent-home__eyebrow">Your Agent workspace</div><h1>{greeting}</h1><p class="agent-home__intro">Your work, shortcuts, and authorized operating areas in one place.</p></div>{flash_html}</header>{clock_html}<section class="home-section" aria-labelledby="needs-title"><div class="home-section__head"><div><div class="home-kicker">Assigned to you</div><h2 id="needs-title">Needs you</h2><p class="home-section__hint">The five highest-priority items across your workspaces.</p></div></div><div class="home-panel needs-list">{needs_html}</div></section><section class="home-section" aria-labelledby="shortcuts-title"><div class="home-section__head"><div><div class="home-kicker">Quick access</div><h2 id="shortcuts-title">Your shortcuts</h2></div><details class="shortcut-editor"><summary class="home-link-button">Edit shortcuts</summary><form class="shortcut-editor__panel" method="post" action="/admin/home/shortcuts"><h3>Choose your shortcuts</h3><p>Select up to six and set their order.</p>{shortcut_options}<div class="shortcut-save"><button class="home-button home-button--primary" type="submit">Save shortcuts</button></div></form></details></div><div class="shortcut-grid">{shortcut_cards or '<div class="home-empty home-panel">Choose shortcuts for faster access.</div>'}</div></section>{recent_section}<section class="home-section" aria-labelledby="workspaces-title"><div class="home-section__head"><div><div class="home-kicker">Directory</div><h2 id="workspaces-title">Your workspaces</h2><p class="home-section__hint">Every operating area your account can access.</p></div></div><div class="workspace-grid">{workspace_cards or '<section class="app-state-panel"><h3>No workspaces assigned</h3><p>Ask an Agent administrator to update your access.</p></section>'}</div></section></main>"""
    return _document(title="Agent workspace", description="Personalized Anata Agent workspace.", nav=nav, body=body)
