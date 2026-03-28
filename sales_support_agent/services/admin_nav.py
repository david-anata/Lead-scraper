"""Shared admin navigation shell for agent.anatainc.com pages."""

from __future__ import annotations

import html


def _nav_item(label: str, href: str, *, active: bool = False, extra_class: str = "") -> str:
    classes = ["top-link"]
    if active:
        classes.append("active")
    if extra_class:
        classes.append(extra_class)
    return f'<a class="{" ".join(classes)}" href="{href}">{html.escape(label)}</a>'


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
      .top-link.logout {
        background: rgba(133, 187, 218, 0.18);
        border-color: rgba(133, 187, 218, 0.38);
      }
      .top-link.logout:hover {
        background: rgba(133, 187, 218, 0.24);
        border-color: rgba(133, 187, 218, 0.52);
      }
      .top-link.active.logout {
        background: #2B3644;
        border-color: #2B3644;
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


def render_agent_nav(active: str = "", *, website_ops_section: str = "") -> str:
    primary_active = "website_ops" if active in {"website_ops", "seo_dashboard", "queue", "reports"} else active
    primary_links = [
        _nav_item("Sales Priorities", "/admin", active=primary_active == "sales"),
        _nav_item("Website Ops", "/admin/website-ops", active=primary_active == "website_ops"),
        _nav_item("Executive", "/admin/executive", active=primary_active == "executive"),
        _nav_item("Fulfillment CS", "/admin/fulfillment-cs", active=primary_active == "fulfillment"),
    ]
    secondary_nav = ""
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
            {_nav_item("Log out", "/admin/logout", extra_class="logout")}
          </nav>
        </div>
        {secondary_nav}
      </div>
    </header>
    """
