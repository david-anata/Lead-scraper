"""Shared admin navigation shell for agent.anatainc.com pages."""

from __future__ import annotations

import base64
import html
from functools import lru_cache
from pathlib import Path
from typing import Optional


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
        gap: 8px;
        padding: 0 14px 0 10px;
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
        width: 26px;
        height: 26px;
        border-radius: 50%;
        background: #85BBDA;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 11px;
        color: #fff;
        font-weight: 900;
        flex-shrink: 0;
      }
      .user-chip-role {
        font-size: 10px;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        color: #85BBDA;
        margin-left: -2px;
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
        min-width: 160px;
        background: #fff;
        border: 1px solid rgba(43, 54, 68, 0.12);
        border-radius: 12px;
        box-shadow: 0 12px 32px rgba(43, 54, 68, 0.12);
        padding: 6px;
        z-index: 100;
      }
      .user-chip:focus-within .user-dropdown,
      .user-chip[data-open] .user-dropdown {
        display: block;
      }
      .user-dropdown a {
        display: block;
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
        background: rgba(43, 54, 68, 0.06);
      }
      .user-dropdown a.logout-link {
        color: #8b4c42;
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
    name = html.escape(user.get("name") or user.get("email") or "User")
    role = html.escape(user.get("role") or "")
    initials = "".join(part[0].upper() for part in name.split()[:2]) or "?"
    return f"""<div class="user-chip" tabindex="0" onclick="this.toggleAttribute('data-open')">
      <span class="user-chip-avatar">{initials}</span>
      <span>{name}</span>
      {f'<span class="user-chip-role">{role}</span>' if role else ""}
      <span class="user-chip-caret">&#9660;</span>
      <div class="user-dropdown">
        <a href="/admin/logout" class="logout-link">Log out</a>
      </div>
    </div>"""


def render_agent_nav(active: str = "", *, website_ops_section: str = "", sales_section: str = "", user: Optional[dict] = None) -> str:
    primary_active = "website_ops" if active in {"website_ops", "seo_dashboard", "queue", "reports"} else active
    if active in {"sales", "sales_decks"}:
        primary_active = "sales"
    if active in {"finance", "finances"}:
        primary_active = "finance"
    primary_links = [
        _nav_item("Sales Priorities", "/admin", active=primary_active == "sales"),
        _nav_item("Website Ops", "/admin/website-ops", active=primary_active == "website_ops"),
        _nav_item("Finance", "/admin/finances", active=primary_active == "finance"),
        _nav_item("Executive", "/admin/executive", active=primary_active == "executive"),
        _nav_item("Fulfillment CS", "/admin/fulfillment-cs", active=primary_active == "fulfillment"),
    ]
    secondary_nav = ""
    current_sales_section = sales_section or active
    if primary_active == "sales":
        sales_links = [
            _nav_item("Sales Priorities", "/admin", active=current_sales_section == "sales", extra_class="top-link--secondary"),
            _nav_item("Generate sales deck", "/admin/sales-decks", active=current_sales_section == "sales_decks", extra_class="top-link--secondary"),
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
            _nav_item("SEO Dashboard", "/admin/website-ops", active=current_section == "seo_dashboard", extra_class="top-link--secondary"),
            _nav_item("Queue", "/admin/website-ops/queue", active=current_section == "queue", extra_class="top-link--secondary"),
            _nav_item("Reports", "/admin/website-ops/reports", active=current_section == "reports", extra_class="top-link--secondary"),
        ]
        secondary_nav = f"""
        <div class="topbar-divider"></div>
        <nav class="top-actions top-actions--secondary">
          {"".join(secondary_links)}
        </nav>
        """
    if primary_active == "fulfillment":
        fulfillment_links = [
            _nav_item("Dashboard", "/admin/fulfillment-cs/", active=current_section == "fulfillment_dashboard", extra_class="top-link--secondary"),
            _nav_item("Reports", "/admin/fulfillment-cs/reports/", active=current_section == "fulfillment_reports", extra_class="top-link--secondary"),
            _nav_item("Latest", "/admin/fulfillment-cs/reports/latest", active=current_section == "fulfillment_latest", extra_class="top-link--secondary"),
        ]
        secondary_nav = f"""
        <div class="topbar-divider"></div>
        <nav class="top-actions top-actions--secondary">
          {"".join(fulfillment_links)}
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
