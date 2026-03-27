"""Shared admin navigation shell for agent.anatainc.com pages."""

from __future__ import annotations

import html


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
      @media (max-width: 960px) {
        .topbar-inner {
          flex-wrap: wrap;
        }
        .brandmark {
          font-size: 34px;
        }
      }
    """


def render_agent_nav(active: str = "") -> str:
    links = [
        ("sales", "Sales Priorities", "/admin"),
        ("executive", "Executive", "/admin/executive"),
        ("website_ops", "Website Ops", "/admin/website-ops"),
        ("queue", "Queue", "/admin/website-ops/queue"),
        ("reports", "Reports", "/admin/website-ops/reports"),
    ]
    nav_links = []
    for key, label, href in links:
        classes = ["top-link"]
        if active == key:
            classes.append("active")
        nav_links.append(f'<a class="{" ".join(classes)}" href="{href}">{html.escape(label)}</a>')
    nav_links.append('<a class="top-link logout" href="/admin/logout">Log out</a>')
    return f"""
    <header class="topbar">
      <div class="topbar-inner">
        <a class="brandmark" href="/admin">agent<span class="dot">.</span></a>
        <nav class="top-actions">
          {"".join(nav_links)}
        </nav>
      </div>
    </header>
    """
