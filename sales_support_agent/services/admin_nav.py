"""Shared admin navigation shell for agent.anatainc.com pages."""

from __future__ import annotations

import base64
import html
from functools import lru_cache
from pathlib import Path
from typing import Optional


# ---------------------------------------------------------------------------
# Data-driven nav definition. ONE ordered structure replaces the old hardcoded
# `_primary_specs` list and the per-section secondary blocks.
#
# Each subpage is (tool_key, label, href, active_key):
#   tool_key   — the catalog tool that gates this page (used for access filtering
#                AND to compute the access-safe primary href).
#   label      — the pill / primary label.
#   href       — exact destination.
#   active_key — the value of the section's "current sub-page" that highlights it.
#
# A section is shown iff the user can reach at least one of its subpages, and the
# primary link points at the FIRST reachable subpage (the access-safe fix).
# ---------------------------------------------------------------------------
class _NavSubpage:
    __slots__ = ("tool_key", "label", "href", "active_key", "superadmin_only")

    def __init__(self, tool_key: str, label: str, href: str, active_key: str, *, superadmin_only: bool = False) -> None:
        self.tool_key = tool_key
        self.label = label
        self.href = href
        self.active_key = active_key
        self.superadmin_only = superadmin_only


class _NavSection:
    __slots__ = ("key", "label", "primary_active", "subpages")

    def __init__(self, key: str, label: str, primary_active: str, subpages: list) -> None:
        self.key = key
        self.label = label
        self.primary_active = primary_active
        self.subpages = subpages


_NAV_SECTIONS = [
    _NavSection("sales", "Sales", "sales", [
        _NavSubpage("sales.deals", "Control Room", "/admin/sales", "sales_operator"),
        _NavSubpage("sales.deals", "Deal Board", "/admin/sales/deals", "sales_deals"),
        _NavSubpage("sales.deals", "Rep Accountability", "/admin/sales/reps", "sales_reps"),
        _NavSubpage("sales.priorities", "Fix Queue", "/admin", "sales"),
        _NavSubpage("sales.decks", "Sales Decks", "/admin/sales-decks", "sales_decks"),
    ]),
    _NavSection("website_ops", "Website Ops", "website_ops", [
        _NavSubpage("website_ops.seo", "Overview", "/admin/website-ops", "seo_dashboard"),
        _NavSubpage("website_ops.queue", "Queue", "/admin/website-ops/queue", "queue"),
        _NavSubpage("website_ops.reports", "Reports", "/admin/website-ops/reports", "reports"),
    ]),
    _NavSection("finance", "Finance", "finance", [
        _NavSubpage("finance", "Finance", "/admin/finances", "finance"),
    ]),
    _NavSection("advertising", "Advertising", "advertising", [
        _NavSubpage("advertising.audit", "Audit", "/admin/advertising/audit", "advertising_audit"),
        _NavSubpage("advertising.audit", "Clients", "/admin/advertising/clients", "advertising_clients"),
        _NavSubpage("advertising.audit", "Profit Calculator", "/admin/advertising/profit-calculator", "advertising_profit_calculator", superadmin_only=True),
        _NavSubpage("advertising.audit", "Bulk Planner", "/admin/advertising/bulk-profitability", "advertising_bulk_profitability", superadmin_only=True),
    ]),
    _NavSection("executive", "Executive", "executive", [
        _NavSubpage("executive.summary", "Executive Summary", "/admin/executive", "executive"),
        _NavSubpage("executive.brand_analysis", "Brand Analysis", "/admin/executive/brand-analysis", "brand_analysis"),
    ]),
    _NavSection("fulfillment", "Fulfillment", "fulfillment", [
        _NavSubpage("fulfillment.rate_sheets", "Sales Pipeline", "/admin/fulfillment/sales", "fulfillment_sales"),
        _NavSubpage("fulfillment.dashboard", "CS Action Queue", "/admin/fulfillment/cs/", "fulfillment_dashboard"),
        _NavSubpage("fulfillment.reports", "CS Reports", "/admin/fulfillment/cs/reports/", "fulfillment_reports"),
    ]),
    _NavSection("hr", "HR", "hr", [
        _NavSubpage("hr.access", "HR", "/admin/hr", "hr"),
    ]),
    # Access/Team management is intentionally NOT a primary nav section — it lives
    # only in the profile dropdown ("Team"). Keeps the top nav identical on every
    # page.
]


def _nav_item(label: str, href: str, *, active: bool = False, extra_class: str = "") -> str:
    classes = ["top-link"]
    if active:
        classes.append("active")
    if extra_class:
        classes.append(extra_class)
    return f'<a class="{" ".join(classes)}" href="{href}">{html.escape(label)}</a>'


@lru_cache(maxsize=1)
def render_agent_favicon_links() -> str:
    favicon_path = Path(__file__).resolve().parents[2] / "shared" / "anata_brand" / "assets" / "agent-favicon.png"
    try:
        encoded = base64.b64encode(favicon_path.read_bytes()).decode("ascii")
    except OSError:
        return ""
    href = f"data:image/png;base64,{encoded}"
    return (
        f'<link rel="icon" type="image/png" href="{href}">'
        f'<link rel="apple-touch-icon" href="{href}">'
    )


def render_agent_nav_styles() -> str:
    return """
      .topbar {
        padding: 16px 24px;
        border-bottom: 1px solid rgba(43, 54, 68, 0.10);
        background: rgba(249, 247, 243, 0.94);
        backdrop-filter: blur(12px);
        position: sticky;
        top: 0;
        z-index: 20;
        box-shadow: 0 8px 24px rgba(43, 54, 68, 0.05);
      }
      .topbar-inner {
        max-width: 1180px;
        margin: 0 auto;
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 16px;
      }
      .topbar-shell {
        max-width: 1180px;
        margin: 0 auto;
        display: grid;
        gap: 10px;
      }
      .brandmark {
        display: inline-flex;
        align-items: center;
        gap: 0;
        font-family: "Montserrat", sans-serif;
        font-weight: 900;
        font-size: 28px;
        line-height: 1;
        letter-spacing: -0.03em;
        color: #2B3644;
        text-decoration: none;
      }
      .brandmark .dot {
        color: #85BBDA;
      }
      .top-actions {
        display: flex;
        align-items: center;
        gap: 10px;
        flex-wrap: wrap;
      }
      .top-actions--secondary {
        gap: 8px;
      }
      .top-link {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        min-height: 42px;
        padding: 0 16px;
        border-radius: 999px;
        background: #fff;
        border: 1px solid rgba(43, 54, 68, 0.12);
        color: #2B3644;
        font-family: "Montserrat", sans-serif;
        font-weight: 700;
        font-size: 13px;
        text-decoration: none;
        transition: background 120ms ease, color 120ms ease, border-color 120ms ease, box-shadow 120ms ease, transform 120ms ease;
      }
      .top-link:hover {
        background: #ffffff;
        border-color: rgba(43, 54, 68, 0.22);
        box-shadow: 0 8px 18px rgba(43, 54, 68, 0.08);
        transform: translateY(-1px);
      }
      .top-link.active {
        background: #2B3644;
        border-color: #2B3644;
        color: #fff;
        box-shadow: 0 10px 22px rgba(43, 54, 68, 0.16);
      }
      .top-link--secondary {
        min-height: 36px;
        padding: 0 14px;
        background: rgba(255, 255, 255, 0.62);
        border-color: rgba(43, 54, 68, 0.08);
        font-size: 12px;
        letter-spacing: 0.02em;
      }
      .top-link--secondary.active {
        background: rgba(133, 187, 218, 0.22);
        border-color: rgba(133, 187, 218, 0.52);
        color: #2B3644;
        box-shadow: inset 0 0 0 1px rgba(133, 187, 218, 0.16);
      }
      /* Primary nav item with an access-safe sub-page dropdown */
      .nav-item {
        position: relative;
        display: inline-flex;
        align-items: center;
      }
      .nav-item > .top-link {
        gap: 6px;
      }
      .nav-caret {
        font-size: 9px;
        line-height: 1;
        margin-left: 2px;
        opacity: 0.55;
      }
      .nav-dropdown {
        position: absolute;
        top: calc(100% + 6px);
        left: 0;
        display: none;
        min-width: 200px;
        background: #fff;
        border: 1px solid rgba(43, 54, 68, 0.12);
        border-radius: 14px;
        box-shadow: 0 16px 40px rgba(43, 54, 68, 0.14);
        padding: 6px;
        z-index: 100;
      }
      /* Invisible hover "bridge" so the 6px gap doesn't close the menu. */
      .nav-dropdown::before {
        content: "";
        position: absolute;
        top: -8px;
        left: 0;
        right: 0;
        height: 8px;
      }
      .nav-item:hover .nav-dropdown,
      .nav-item:focus-within .nav-dropdown,
      .nav-item[data-open] .nav-dropdown {
        display: block;
      }
      .nav-dropdown .top-link--secondary {
        display: flex;
        width: 100%;
        justify-content: flex-start;
        border-radius: 8px;
        margin: 2px 0;
        background: transparent;
        border-color: transparent;
      }
      .nav-dropdown .top-link--secondary:hover {
        background: rgba(43, 54, 68, 0.05);
        border-color: transparent;
        transform: none;
        box-shadow: none;
      }
      .topbar-divider {
        height: 1px;
        background: linear-gradient(90deg, rgba(43, 54, 68, 0.10) 0%, rgba(43, 54, 68, 0.04) 100%);
      }
      .topbar-section-row {
        display: flex;
        align-items: center;
        gap: 10px;
        padding-top: 2px;
        overflow-x: auto;
      }
      .topbar-section-label {
        font-family: "Montserrat", sans-serif;
        font-size: 10px;
        font-weight: 800;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        color: rgba(43, 54, 68, 0.48);
        white-space: nowrap;
      }
      /* User chip + dropdown */
      .user-chip {
        position: relative;
        display: inline-flex;
        align-items: center;
        gap: 5px;
        padding: 0 10px 0 5px;
        min-height: 42px;
        border-radius: 999px;
        background: #fff;
        border: 1px solid rgba(43, 54, 68, 0.12);
        font-family: "Montserrat", sans-serif;
        font-weight: 700;
        font-size: 13px;
        color: #2B3644;
        cursor: pointer;
        user-select: none;
        transition: border-color 120ms ease, box-shadow 120ms ease;
      }
      .user-chip:hover {
        border-color: rgba(43, 54, 68, 0.22);
        box-shadow: 0 4px 12px rgba(43, 54, 68, 0.08);
      }
      .user-chip-avatar {
        width: 32px;
        height: 32px;
        border-radius: 50%;
        background: #85BBDA;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 12px;
        color: #fff;
        font-weight: 900;
        flex-shrink: 0;
        overflow: hidden;
      }
      .user-chip-avatar img {
        width: 100%;
        height: 100%;
        object-fit: cover;
        display: block;
      }
      .user-chip-caret {
        font-size: 9px;
        color: rgba(43,54,68,0.4);
        margin-left: 2px;
      }
      .user-dropdown {
        display: none;
        position: absolute;
        top: calc(100% + 6px);
        right: 0;
        min-width: 220px;
        background: #fff;
        border: 1px solid rgba(43, 54, 68, 0.12);
        border-radius: 14px;
        box-shadow: 0 16px 40px rgba(43, 54, 68, 0.14);
        padding: 6px;
        z-index: 100;
      }
      .user-chip:focus-within .user-dropdown,
      .user-chip[data-open] .user-dropdown {
        display: block;
      }
      .user-dropdown-profile {
        padding: 10px 14px 10px;
        border-bottom: 1px solid rgba(43,54,68,0.07);
        margin-bottom: 4px;
      }
      .user-dropdown-profile-name {
        font-family: "Montserrat", sans-serif;
        font-size: 13px;
        font-weight: 800;
        color: #2B3644;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
        max-width: 190px;
      }
      .user-dropdown-profile-email {
        font-family: "Inter", sans-serif;
        font-size: 12px;
        font-weight: 500;
        color: rgba(43,54,68,0.6);
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
        max-width: 190px;
        margin-top: 1px;
      }
      .user-dropdown-profile-role {
        font-family: "Montserrat", sans-serif;
        font-size: 10px;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        color: rgba(43,54,68,0.45);
        margin-top: 2px;
      }
      .user-dropdown-divider {
        height: 1px;
        background: rgba(43,54,68,0.07);
        margin: 4px 0;
      }
      .user-dropdown a {
        display: flex;
        align-items: center;
        gap: 9px;
        padding: 9px 14px;
        border-radius: 8px;
        font-family: "Montserrat", sans-serif;
        font-weight: 700;
        font-size: 13px;
        color: #2B3644;
        text-decoration: none;
        transition: background 100ms ease;
      }
      .user-dropdown a:hover {
        background: rgba(43, 54, 68, 0.05);
      }
      .user-dropdown a .ud-icon {
        font-size: 15px;
        width: 18px;
        text-align: center;
        flex-shrink: 0;
      }
      .user-dropdown a.logout-link {
        color: #8b4c42;
        border-top: 1px solid rgba(43,54,68,0.07);
        margin-top: 4px;
        border-radius: 0 0 8px 8px;
      }
      .user-dropdown a.logout-link:hover {
        background: rgba(139,76,66,0.06);
      }
      @media (max-width: 960px) {
        .topbar-inner,
        .top-actions {
          flex-wrap: wrap;
        }
        .brandmark {
          font-size: 34px;
        }
        /* On touch, hover can't open the menu — rely on the [data-open] tap path
           (the .nav-item is tabindex/onclick toggleable). Keep panels readable. */
        .nav-dropdown {
          left: 0;
          right: auto;
          max-width: calc(100vw - 48px);
        }
      }
    """


def _user_chip_html(user: Optional[dict]) -> str:
    if not user:
        return f'<a class="top-link" href="/admin/logout">Log out</a>'
    name_raw = (user.get("name") or user.get("email") or "User").strip()
    name = html.escape(name_raw)
    email = html.escape(user.get("email") or "")
    role_raw = user.get("role") or user.get("role_name") or ""
    is_superadmin = bool(user.get("is_superadmin"))
    permissions = user.get("permissions") or set()
    can_manage = is_superadmin or "access.manage" in permissions
    role = html.escape(role_raw or ("Super-admin" if is_superadmin else ""))

    # Initials from first + last name; a single token (e.g. an email) gets
    # just its first letter.
    parts = [p for p in name_raw.split() if p]
    if len(parts) >= 2:
        initials = (parts[0][0] + parts[-1][0]).upper()
    else:
        initials = (parts[0][0].upper() if parts else "?")

    picture = (user.get("picture") or "").strip()
    if picture.startswith("https://"):
        avatar = f'<span class="user-chip-avatar"><img src="{html.escape(picture)}" alt="{initials}" referrerpolicy="no-referrer"></span>'
    else:
        avatar = f'<span class="user-chip-avatar">{initials}</span>'

    team_link = ""
    settings_link = ""
    if can_manage:
        team_link = '<a href="/admin/access"><span class="ud-icon">&#128101;</span> Team</a>'
        settings_link = '<a href="/admin/settings"><span class="ud-icon">&#9881;&#65039;</span> Settings</a>'

    show_name_line = bool(name_raw and name_raw != (user.get("email") or ""))
    return f"""<div class="user-chip" tabindex="0" onclick="this.toggleAttribute('data-open')" title="{email or name}">
      {avatar}
      <span class="user-chip-caret">&#9660;</span>
      <div class="user-dropdown">
        <div class="user-dropdown-profile">
          {f'<div class="user-dropdown-profile-name">{name}</div>' if show_name_line else ""}
          <div class="user-dropdown-profile-email">{email or name}</div>
          {f'<div class="user-dropdown-profile-role">{role}</div>' if role else ""}
        </div>
        {team_link}
        {settings_link}
        <a href="/admin/logout" class="logout-link"><span class="ud-icon">&#8594;</span> Log out</a>
      </div>
    </div>"""


def render_agent_nav(active: str = "", *, website_ops_section: str = "", sales_section: str = "", advertising_section: str = "", executive_section: str = "", fulfillment_section: str = "", permissions: Optional[set] = None, is_superadmin: bool = False, user: Optional[dict] = None) -> str:
    # Per-tool nav filtering. When neither permissions nor is_superadmin is
    # supplied, we keep the legacy "show everything" behaviour (the routes
    # themselves are still guarded server-side). The Access admin link is the
    # one exception: it only ever appears when explicitly granted.
    _granted: Optional[set] = None
    user_permissions = None
    if permissions is None and user is not None and ("permissions" in user or "is_superadmin" in user):
        user_permissions = user.get("permissions")
        if user_permissions is None:
            user_permissions = set()
    if is_superadmin or bool((user or {}).get("is_superadmin")):
        _granted = None  # superadmin sees all
        _show_all = True
    elif permissions is not None or user_permissions is not None:
        permissions = permissions if permissions is not None else user_permissions
        _granted = set(permissions)
        _show_all = False
    else:
        _show_all = True  # legacy callers that didn't pass permissions

    def _can(key: str) -> bool:
        return _show_all or (_granted is not None and key in _granted)

    primary_active = "website_ops" if active in {"website_ops", "seo_dashboard", "queue", "reports"} else active
    if active in {"sales", "sales_decks", "sales_reps"}:
        primary_active = "sales"
    if active in {"finance", "finances"}:
        primary_active = "finance"
    if active in {"advertising", "advertising_audit", "advertising_clients", "advertising_profit_calculator", "advertising_bulk_profitability"}:
        primary_active = "advertising"
    if active in {"executive", "brand_analysis"}:
        primary_active = "executive"
    if active in {"fulfillment", "fulfillment_sales", "fulfillment_dashboard", "fulfillment_reports", "fulfillment_latest"}:
        primary_active = "fulfillment"
    if active in {"access", "access_users", "access_roles", "access_invites", "access_requests"}:
        primary_active = "access"
    if active == "hr" or active.startswith("hr_"):
        primary_active = "hr"

    # Per-section "current sub-page" — preserves today's active-highlight logic
    # exactly. Each section reads from its dedicated *_section kwarg (falling back
    # to the same defaults as before); fulfillment derives from `active`.
    _current_subpage = {
        "sales": sales_section or active,
        "website_ops": website_ops_section or ("seo_dashboard" if active == "website_ops" else active),
        "advertising": advertising_section or ("advertising_audit" if active == "advertising" else active),
        "executive": executive_section or ("executive" if active == "executive" else active),
        "finance": active,
        "fulfillment": fulfillment_section or (website_ops_section if website_ops_section.startswith("fulfillment_") else "") or ("fulfillment_sales" if active == "fulfillment" else active),
        "hr": active,
    }

    nav_items: list = []
    active_section_label = ""
    active_section_subpages: list = []
    active_section_current = ""
    for section in _NAV_SECTIONS:
        accessible = [
            sp for sp in section.subpages
            if _can(sp.tool_key) and (is_superadmin or not sp.superadmin_only)
        ]
        if not accessible:
            continue  # zero reachable pages — section is hidden (was _can_section)

        is_primary_active = primary_active == section.primary_active
        primary_href = accessible[0].href  # first page the user can actually open
        current = _current_subpage.get(section.primary_active, active)
        if is_primary_active and len(accessible) > 1:
            active_section_label = section.label
            active_section_subpages = accessible
            active_section_current = current

        if len(accessible) == 1:
            # Single reachable page — plain link, no caret/dropdown.
            nav_items.append(_nav_item(accessible[0].label, primary_href, active=is_primary_active))
            continue

        # >=2 reachable pages — non-navigating trigger + caret + dropdown of pills.
        # The trigger is a <span> (not <a>) so clicking the header label only
        # opens/closes the dropdown; navigation happens via the pills inside.
        active_class = " active" if is_primary_active else ""
        primary_trigger = (
            f'<span class="top-link{active_class}" style="cursor:default;user-select:none">'
            f'{section.label}<span class="nav-caret">&#9660;</span></span>'
        )
        pills = "".join(
            _nav_item(sp.label, sp.href, active=(current == sp.active_key), extra_class="top-link--secondary")
            for sp in accessible
        )
        nav_items.append(
            f"""<div class="nav-item" tabindex="0" onclick="this.toggleAttribute('data-open')">
              {primary_trigger}
              <div class="nav-dropdown">{pills}</div>
            </div>"""
        )

    active_section_row = ""
    if active_section_subpages:
        secondary_pills = "".join(
            _nav_item(sp.label, sp.href, active=(active_section_current == sp.active_key), extra_class="top-link--secondary")
            for sp in active_section_subpages
        )
        active_section_row = f"""
        <div class="topbar-divider"></div>
        <div class="topbar-section-row" aria-label="{html.escape(active_section_label)} pages">
          <span class="topbar-section-label">{html.escape(active_section_label)} pages</span>
          <nav class="top-actions top-actions--secondary">
            {secondary_pills}
          </nav>
        </div>
        """

    # Access/Team management is intentionally NOT a primary nav section — it lives
    # only in the profile dropdown ("Team"), so the top bar stays identical on
    # every page.
    return f"""
    <header class="topbar">
      <div class="topbar-shell">
        <div class="topbar-inner">
          <a class="brandmark" href="/admin">agent<span class="dot">.</span></a>
          <nav class="top-actions">
            {"".join(nav_items)}
          </nav>
          {_user_chip_html(user)}
        </div>
        {active_section_row}
      </div>
    </header>
    """
