"""Shared admin navigation shell for agent.anatainc.com pages."""

from __future__ import annotations

import base64
import html
from functools import lru_cache
from pathlib import Path
from typing import Optional


from sales_support_agent.services.access.catalog import SECTIONS as _CATALOG_SECTIONS

# Section name -> the tool keys it contains (for nav visibility filtering).
_SECTION_TOOLS = {sec: [t.key for t in tools] for sec, tools in _CATALOG_SECTIONS.items()}


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
      .topbar-divider {
        height: 1px;
        background: linear-gradient(90deg, rgba(43, 54, 68, 0.10) 0%, rgba(43, 54, 68, 0.04) 100%);
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


def render_agent_nav(active: str = "", *, website_ops_section: str = "", sales_section: str = "", advertising_section: str = "", executive_section: str = "", permissions: Optional[set] = None, is_superadmin: bool = False, user: Optional[dict] = None) -> str:
    # Per-tool nav filtering. When neither permissions nor is_superadmin is
    # supplied, we keep the legacy "show everything" behaviour (the routes
    # themselves are still guarded server-side). The Access admin link is the
    # one exception: it only ever appears when explicitly granted.
    _granted: Optional[set] = None
    if is_superadmin:
        _granted = None  # superadmin sees all
        _show_all = True
    elif permissions is not None:
        _granted = set(permissions)
        _show_all = False
    else:
        _show_all = True  # legacy callers that didn't pass permissions

    def _can(key: str) -> bool:
        return _show_all or (_granted is not None and key in _granted)

    def _can_section(section: str) -> bool:
        if _show_all:
            return True
        return any(_can(k) for k in _SECTION_TOOLS.get(section, ()))

    show_access = is_superadmin or bool(permissions is not None and "access.manage" in permissions)

    primary_active = "website_ops" if active in {"website_ops", "seo_dashboard", "queue", "reports"} else active
    if active in {"sales", "sales_decks"}:
        primary_active = "sales"
    if active in {"finance", "finances"}:
        primary_active = "finance"
    if active in {"advertising", "advertising_audit"}:
        primary_active = "advertising"
    if active in {"executive", "brand_analysis"}:
        primary_active = "executive"
    if active in {"access", "access_users", "access_roles", "access_invites", "access_requests"}:
        primary_active = "access"
    _primary_specs = [
        ("Sales Priorities", "/admin", primary_active == "sales", _can_section("Sales Priorities")),
        ("Website Ops", "/admin/website-ops", primary_active == "website_ops", _can_section("Website Ops")),
        ("Finance", "/admin/finances", primary_active == "finance", _can_section("Finance")),
        ("Advertising", "/admin/advertising/audit", primary_active == "advertising", _can_section("Advertising")),
        ("Executive", "/admin/executive", primary_active == "executive", _can_section("Executive")),
        ("Fulfillment", "/admin/fulfillment", primary_active == "fulfillment", _can_section("Fulfillment")),
        ("Access", "/admin/access", primary_active == "access", show_access),
    ]
    primary_links = [
        _nav_item(label, href, active=is_active)
        for (label, href, is_active, visible) in _primary_specs if visible
    ]
    secondary_nav = ""
    current_sales_section = sales_section or active
    if primary_active == "sales":
        sales_links = [
            link for key, link in (
                ("sales.priorities", _nav_item("Sales Priorities", "/admin", active=current_sales_section == "sales", extra_class="top-link--secondary")),
                ("sales.decks", _nav_item("Generate sales deck", "/admin/sales-decks", active=current_sales_section == "sales_decks", extra_class="top-link--secondary")),
            ) if _can(key)
        ]
        secondary_nav = f"""
        <div class="topbar-divider"></div>
        <nav class="top-actions top-actions--secondary">
          {"".join(sales_links)}
        </nav>
        """
    current_section = website_ops_section or ("seo_dashboard" if active == "website_ops" else active)
    if primary_active == "website_ops":
        secondary_links = [
            link for key, link in (
                ("website_ops.seo", _nav_item("SEO Dashboard", "/admin/website-ops", active=current_section == "seo_dashboard", extra_class="top-link--secondary")),
                ("website_ops.queue", _nav_item("Queue", "/admin/website-ops/queue", active=current_section == "queue", extra_class="top-link--secondary")),
                ("website_ops.reports", _nav_item("Reports", "/admin/website-ops/reports", active=current_section == "reports", extra_class="top-link--secondary")),
            ) if _can(key)
        ]
        secondary_nav = f"""
        <div class="topbar-divider"></div>
        <nav class="top-actions top-actions--secondary">
          {"".join(secondary_links)}
        </nav>
        """
    current_advertising_section = advertising_section or ("advertising_audit" if active == "advertising" else active)
    if primary_active == "advertising":
        advertising_links = [
            link for key, link in (
                ("advertising.audit", _nav_item("Audit", "/admin/advertising/audit", active=current_advertising_section == "advertising_audit", extra_class="top-link--secondary")),
            ) if _can(key)
        ]
        secondary_nav = f"""
        <div class="topbar-divider"></div>
        <nav class="top-actions top-actions--secondary">
          {"".join(advertising_links)}
        </nav>
        """
    current_executive_section = executive_section or ("executive" if active == "executive" else active)
    if primary_active == "executive":
        executive_links = [
            link for key, link in (
                ("executive.summary", _nav_item("Executive Summary", "/admin/executive", active=current_executive_section == "executive", extra_class="top-link--secondary")),
                ("executive.brand_analysis", _nav_item("Brand Analysis", "/admin/executive/brand-analysis", active=current_executive_section == "brand_analysis", extra_class="top-link--secondary")),
            ) if _can(key)
        ]
        secondary_nav = f"""
        <div class="topbar-divider"></div>
        <nav class="top-actions top-actions--secondary">
          {"".join(executive_links)}
        </nav>
        """
    if primary_active == "fulfillment":
        fulfillment_links = [
            link for key, link in (
                ("fulfillment.rate_sheets", _nav_item("Sales Deck", "/admin/fulfillment/sales", active=current_section == "fulfillment_sales", extra_class="top-link--secondary")),
                ("fulfillment.dashboard", _nav_item("CS Dashboard", "/admin/fulfillment/cs/", active=current_section == "fulfillment_dashboard", extra_class="top-link--secondary")),
                ("fulfillment.reports", _nav_item("CS Reports", "/admin/fulfillment/cs/reports/", active=current_section == "fulfillment_reports", extra_class="top-link--secondary")),
                ("fulfillment.reports", _nav_item("Latest", "/admin/fulfillment/cs/reports/latest", active=current_section == "fulfillment_latest", extra_class="top-link--secondary")),
            ) if _can(key)
        ]
        secondary_nav = f"""
        <div class="topbar-divider"></div>
        <nav class="top-actions top-actions--secondary">
          {"".join(fulfillment_links)}
        </nav>
        """
    if primary_active == "access":
        access_links = [
            link for key, link in (
                ("access.manage", _nav_item("People", "/admin/access", active=active in {"access", "access_users", "access_invites", "access_requests", "access_roles"}, extra_class="top-link--secondary")),
            ) if _can(key)
        ]
        secondary_nav = f"""
        <div class="topbar-divider"></div>
        <nav class="top-actions top-actions--secondary">
          {"".join(access_links)}
        </nav>
        """
    return f"""
    <header class="topbar">
      <div class="topbar-shell">
        <div class="topbar-inner">
          <a class="brandmark" href="/admin">agent<span class="dot">.</span></a>
          <nav class="top-actions">
            {"".join(primary_links)}
          </nav>
          {_user_chip_html(user)}
        </div>
        {secondary_nav}
      </div>
    </header>
    """
