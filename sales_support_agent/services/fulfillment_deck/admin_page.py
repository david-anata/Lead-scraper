"""Admin page for Fulfillment > Sales Deck (rate sheet generator + history).

Same admin shell vocabulary as the Brand Analysis page (nav + workspace card),
so it reads as a sibling tool.
"""

from __future__ import annotations

import html
from datetime import datetime
from typing import Optional
from urllib.parse import urlencode

from sales_support_agent.services.admin_nav import (
    render_agent_favicon_links,
    render_agent_nav,
    render_agent_nav_styles,
)
from sales_support_agent.services.fulfillment_deck.pricing_rules import (
    merge_fee_rows,
    suggest_customer_price,
    validate_quote_readiness,
)
from sales_support_agent.services.fulfillment_deck.quote import BASELINE_RATES, INTERNAL_COST_BASELINES
from sales_support_agent.services.fulfillment_deck.schema import (
    ANATA_HQ_ADDRESS,
    ANATA_HQ_ZIP,
    ProspectProfile,
    RATE_SOURCE_WMS,
)
from sales_support_agent.services.public_report_ui import (
    PUBLIC_REPORT_DESIGN_VERSION,
    public_report_foundation_css,
)


def _esc(value: object) -> str:
    return html.escape(str(value or ""))


_STYLES = """
      :root {
        --dark-blue: #2B3644;
        --light-blue: #85BBDA;
        --light-brown: #F9F7F3;
        --white: #FFFFFF;
        --border: rgba(43, 54, 68, 0.12);
        --shadow: rgba(43, 54, 68, 0.10);
      }
      * { box-sizing: border-box; }
      body { margin: 0; background: var(--light-brown); color: var(--dark-blue);
        font-family: "Inter", "Segoe UI", sans-serif; }
      a { color: var(--dark-blue); }
      __NAV__
      .shell { max-width: 1320px; margin: 0 auto; padding: 28px 24px 64px; }
      .workspace { background: var(--white); border: 1px solid var(--border); border-radius: 20px;
        box-shadow: 0 18px 40px var(--shadow); padding: 26px 28px 30px; }
      h1 { font-family: "Montserrat", sans-serif; font-weight: 800; font-size: 26px; margin: 0 0 4px; }
      h2 { font-family: "Montserrat", sans-serif; font-weight: 800; font-size: 17px; margin: 26px 0 8px; }
      .eyebrow { font-family: "Montserrat", sans-serif; font-weight: 700; font-size: 11px;
        letter-spacing: 0.08em; text-transform: uppercase; color: rgba(43,54,68,0.55); margin: 0 0 4px; }
      .intro { font-size: 14px; color: rgba(43,54,68,0.75); margin: 0 0 18px; max-width: 760px; }
      .flash { background: rgba(133,187,218,0.18); border: 1px solid rgba(133,187,218,0.5);
        border-radius: 12px; padding: 12px 16px; margin-bottom: 14px; font-size: 13.5px; }
      .flash--warn { background: #fff4d9; border-color: #d2a94b; }
      .field { display: grid; gap: 5px; margin: 12px 0; }
      .field label { font-family: "Montserrat", sans-serif; font-weight: 700; font-size: 12px; }
      .field .hint { font-size: 12px; color: rgba(43,54,68,0.55); font-weight: 400; }
      .field input[type=text], .field input[type=url], .field input[type=number], .field input[type=search],
      .field select {
        width: 100%; min-height: 42px; padding: 0 12px;
        border-radius: 10px; border: 1px solid var(--border);
        background: #fff; color: var(--dark-blue); font-size: 14px; font-family: inherit;
      }
      .field input:focus, .field textarea:focus, .field select:focus {
        outline: 2px solid rgba(133,187,218,0.38);
        border-color: rgba(133,187,218,0.9);
      }
      .field textarea { min-height: 150px; padding: 10px 12px; border-radius: 10px;
        border: 1px solid var(--border); font-size: 14px; font-family: inherit; resize: vertical; }
      .drop { border: 2px dashed rgba(133,187,218,0.7); border-radius: 16px; padding: 22px;
        text-align: center; background: var(--light-brown); }
      .grid2 { display: grid; grid-template-columns: 2fr 1fr; gap: 14px; }
      .btn { display: inline-flex; align-items: center; gap: 8px; min-height: 44px; padding: 0 22px;
        border-radius: 999px; background: var(--dark-blue); color: #fff; font-family: "Montserrat", sans-serif;
        font-weight: 700; font-size: 13px; border: none; cursor: pointer; text-decoration: none; }
      .btn--ghost { background: #fff; color: var(--dark-blue); border: 1px solid var(--border); min-height: 34px; padding: 0 14px; font-size: 12px; }
      .btn--danger { background: #fff; color: #8b4c42; border: 1px solid rgba(139,76,66,0.4); min-height: 34px; padding: 0 14px; font-size: 12px; }
      .inline-chip { min-height: 24px; padding: 0 8px; border-radius: 999px; border: 1px solid rgba(133,187,218,0.55);
        background: rgba(133,187,218,0.12); color: var(--dark-blue); cursor: pointer; font-family: "Montserrat", sans-serif;
        font-weight: 700; font-size: 10.5px; white-space: nowrap; }
      table { width: 100%; border-collapse: collapse; font-size: 13.5px; margin: 6px 0 8px; }
      th, td { text-align: left; padding: 9px 11px; border-bottom: 1px solid var(--border); vertical-align: middle; }
      thead th { background: #e8f2f7; font-family: "Montserrat", sans-serif; font-size: 11px;
        letter-spacing: 0.04em; text-transform: uppercase; }
      .pipeline-table-wrap > table > thead th { position: sticky; top: 0; z-index: 3; }
      .pipeline-toolbar { display:grid; grid-template-columns:minmax(220px,1fr) minmax(170px,220px)
        minmax(170px,220px) auto auto; align-items:end; gap:10px; margin:0 0 12px; }
      .pipeline-toolbar__field { display:grid; gap:5px; }
      .pipeline-toolbar__field label { font:700 10px/1.2 "Montserrat",sans-serif; letter-spacing:.06em;
        text-transform:uppercase; color:rgba(43,54,68,.55); }
      .pipeline-toolbar input,.pipeline-toolbar select { width:100%; min-height:40px; padding:0 12px;
        border:1px solid var(--border); border-radius:10px; background:#fff; color:var(--dark-blue);
        font:500 13px/1.2 "Inter","Segoe UI",sans-serif; }
      .pipeline-toolbar .btn { align-self:end; justify-content:center; min-height:40px; border-radius:10px; }
      .pipeline-results-count { align-self:center; color:rgba(43,54,68,.62); font-size:12px; white-space:nowrap; }
      .pipeline-table-wrap { max-height:min(64vh,720px); overflow:auto; border:1px solid var(--border);
        border-radius:14px; }
      .pipeline-table-wrap table { margin:0; }
      .pipeline-filter-empty { display:none; margin:12px 0 0; padding:16px; border:1px dashed var(--border);
        border-radius:12px; color:rgba(43,54,68,.62); text-align:center; font-size:13px; }
      .pill { display: inline-block; padding: 3px 10px; border-radius: 999px; font-size: 11px;
        font-weight: 700; font-family: "Montserrat", sans-serif; letter-spacing: 0.03em; }
      .pill--live { background: rgba(46,125,91,0.16); color: #2e7d5b; }
      .pill--sample { background: #fff4d9; color: #7a5b14; border: 1px solid #d2a94b; }
      .pill--failed { background: rgba(139,76,66,0.16); color: #8b4c42; }
      .pill--draft { background: rgba(43,54,68,0.10); color: rgba(43,54,68,0.65); }
      .pill--running { background: rgba(14,165,233,0.12); color: #0369a1; border: 1px solid rgba(14,165,233,0.3); }
      .pill--estimated { background: #fff4d9; color: #7a5b14; border: 1px solid #d2a94b; }
      .row-actions { display: flex; justify-content: flex-end; }
      .action-menu { position: relative; display: inline-flex; }
      .action-menu-trigger {
        width: 34px; height: 34px; border-radius: 999px; border: 1px solid var(--border);
        background: #fff; color: var(--dark-blue); cursor: pointer; font-family: "Montserrat", sans-serif;
        font-weight: 800; font-size: 14px; line-height: 1; display: inline-flex; align-items: center;
        justify-content: center;
      }
      .action-menu-trigger:hover { border-color: rgba(43,54,68,0.24); box-shadow: 0 6px 14px rgba(43,54,68,0.08); }
      .action-menu-panel {
        position: absolute; right: 0; top: calc(100% + 6px); display: none; min-width: 180px;
        background: #fff; border: 1px solid var(--border); border-radius: 10px;
        box-shadow: 0 16px 34px rgba(43,54,68,0.16); padding: 6px; z-index: 30;
      }
      .action-menu[data-open] .action-menu-panel { display: grid; gap: 2px; }
      .action-menu-panel form { margin: 0; }
      .action-menu-item {
        width: 100%; min-height: 34px; padding: 0 10px; border: 0; border-radius: 7px;
        background: transparent; color: var(--dark-blue); text-decoration: none; cursor: pointer;
        font-family: "Montserrat", sans-serif; font-size: 12px; font-weight: 700;
        display: flex; align-items: center; justify-content: flex-start; text-align: left;
      }
      .action-menu-item:hover { background: rgba(43,54,68,0.06); }
      .action-menu-item--quote { color: #FF7A59; }
      .action-menu-item--danger { color: #8b4c42; }
      .muted { color: rgba(43,54,68,0.55); font-size: 12px; }
      .empty { color: rgba(43,54,68,0.55); font-size: 13.5px; padding: 18px 0; }
      .review-toolbar { display:flex; gap:8px; flex-wrap:wrap; align-items:center; justify-content:space-between; margin:0 0 14px; }
      .review-sections { display:grid; gap:12px; margin-top:14px; }
      .review-section {
        border: 1px solid var(--border); border-radius: 14px; background: #fff;
        box-shadow: 0 8px 18px rgba(43,54,68,0.04); overflow: clip;
      }
      .review-section > summary {
        list-style: none; cursor: pointer; padding: 15px 16px;
        display:flex; justify-content:space-between; gap:14px; align-items:center;
        font-family: "Montserrat", sans-serif; font-weight:800; font-size:14px;
      }
      .review-section > summary::-webkit-details-marker { display:none; }
      .review-section > summary::after { content:"▾"; color:rgba(43,54,68,0.45); font-size:12px; }
      .review-section:not([open]) > summary::after { transform: rotate(-90deg); }
      .review-section__sub { display:block; margin-top:3px; font-family:"Inter", sans-serif; font-weight:500; font-size:12px; color:rgba(43,54,68,0.55); }
      .review-section__body { padding: 0 16px 16px; }
      .form-grid { display: grid; grid-template-columns: repeat(2, minmax(280px, 1fr)); gap: 4px 22px; }
      .form-grid--wide { grid-template-columns: repeat(3, minmax(220px, 1fr)); }
      .history-bar {
        margin-top: 18px; padding: 14px 16px; border: 1px solid var(--border);
        border-radius: 14px; background: rgba(43,54,68,0.025);
      }
      .history-list { display: grid; gap: 8px; margin-top: 10px; }
      .history-item { display: grid; gap: 2px; padding: 8px 0; border-top: 1px solid rgba(43,54,68,0.07); }
      .history-item:first-child { border-top: 0; padding-top: 0; }
      .history-item span { color: rgba(43,54,68,0.55); font-size: 12px; }
      .history-item em { color: rgba(43,54,68,0.72); font-size: 12.5px; font-style: normal; }
      .edit-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 0 24px; }
      .prospect-summary { display:grid; grid-template-columns: repeat(4, minmax(140px, 1fr)); gap:10px; margin: 14px 0 12px; }
      .prospect-summary__item { border:1px solid var(--border); border-radius:12px; background:rgba(43,54,68,0.025); padding:10px 12px; min-width:0; }
      .prospect-summary__item span { display:block; font-size:10px; font-family:"Montserrat", sans-serif; font-weight:700; letter-spacing:.06em; text-transform:uppercase; color:rgba(43,54,68,.5); }
      .prospect-summary__item strong { display:block; margin-top:3px; font-size:13px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
      .pricing-lines { table-layout: fixed; border:1px solid var(--border); border-radius:12px; overflow:hidden; display:table; }
      .pricing-lines th, .pricing-lines td { vertical-align: top; }
      .pricing-lines th:nth-child(1), .pricing-lines td:nth-child(1) { width: 24%; }
      .pricing-lines th:nth-child(2), .pricing-lines td:nth-child(2) { width: 25%; }
      .pricing-lines th:nth-child(3), .pricing-lines td:nth-child(3) { width: 25%; }
      .pricing-lines th:nth-child(4), .pricing-lines td:nth-child(4) { width: 26%; }
      .pricing-lines__label { font-weight: 800; font-family: "Montserrat", sans-serif; font-size: 12px; }
      .pricing-lines__sub { margin-top: 3px; color: rgba(43,54,68,0.55); font-size: 11.5px; line-height: 1.35; }
      .pricing-cell { display:grid; gap:4px; }
      .pricing-cell label { font-size:10px; font-weight:800; font-family:"Montserrat", sans-serif; color:rgba(43,54,68,.58); letter-spacing:0; text-transform:uppercase; }
      .pricing-cell input { width:100%; min-height:36px; padding:0 10px; border-radius:8px; border:1px solid var(--border); font-size:13px; font-family:inherit; }
      .pricing-cell--empty { color:rgba(43,54,68,.42); font-size:12px; line-height:1.45; padding-top:20px; }
      .pricing-suggestion { display:grid; gap:5px; font-size:12px; line-height:1.4; }
      .pricing-suggestion strong { font-size:14px; font-family:"Montserrat", sans-serif; }
      .pricing-suggestion span { color:rgba(43,54,68,.58); }
      .pricing-note { color:rgba(43,54,68,.62); font-size:12px; line-height:1.45; }
      @media (max-width: 760px) { .grid2 { grid-template-columns: 1fr; } .edit-grid { grid-template-columns: 1fr; } }
      @media (max-width: 900px) { .form-grid, .form-grid--wide, .prospect-summary { grid-template-columns: 1fr; } .pricing-lines { min-width:760px; } .review-section__body { overflow-x:auto; } }
      /* On narrow screens keep only Prospect, Stage, Margin, Actions */
      @media (max-width: 640px) {
        table th:nth-child(3), table td:nth-child(3),
        table th:nth-child(4), table td:nth-child(4),
        table th:nth-child(5), table td:nth-child(5),
        table th:nth-child(7), table td:nth-child(7) { display: none; }
        .row-actions { justify-content: flex-start; }
      }
      /* Pipeline summary bar */
      .pipeline-stats { display: flex; gap: 12px; margin: 0 0 16px; flex-wrap: wrap; }
      .pipeline-stat { background: #fff; border: 1px solid var(--border); border-radius: 12px;
        padding: 12px 16px; flex: 1; min-width: 130px; }
      .pipeline-stat__val { font-family: "Montserrat", sans-serif; font-weight: 800;
        font-size: 20px; color: var(--dark-blue); line-height: 1.1; }
      .pipeline-stat__label { font-size: 10px; font-weight: 700; font-family: "Montserrat", sans-serif;
        letter-spacing: 0.07em; text-transform: uppercase; color: rgba(43,54,68,0.5); margin-top: 3px; }
      .pipeline-stat__sub { font-size: 11px; color: rgba(43,54,68,0.5); margin-top: 2px; }
      .pipeline-stat--won .pipeline-stat__val { color: #15803d; }
      .operator-callout {
        display: grid; grid-template-columns: minmax(0, 1fr) auto; gap: 18px; align-items: center;
        margin: 18px 0 20px; padding: 18px 20px; border: 1px solid rgba(133,187,218,0.55);
        border-radius: 16px; background: rgba(133,187,218,0.12);
      }
      .operator-callout h2 { margin: 0 0 6px; font-size: 20px; }
      .operator-callout p { margin: 0; color: rgba(43,54,68,0.72); line-height: 1.45; }
      .operator-callout__side { display: flex; gap: 8px; flex-wrap: wrap; justify-content: flex-end; align-items: center; }
      .operator-callout__meta { margin-top: 8px; color: rgba(43,54,68,0.55); font-size: 12px; }
      .review-hub { display: grid; gap: 12px; margin: 14px 0 16px; }
      .review-status-strip { display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:10px; }
      .review-status-card { border:1px solid var(--border); border-radius:12px; background:#fff; padding:11px 13px; display:grid; gap:3px; }
      .review-status-card span { font-family:"Montserrat", sans-serif; font-size:10.5px; font-weight:800; letter-spacing:.05em; text-transform:uppercase; color:rgba(43,54,68,.48); }
      .review-status-card strong { font-family:"Montserrat", sans-serif; font-size:13px; }
      .review-status-card em { font-style:normal; font-size:12px; color:rgba(43,54,68,.62); line-height:1.35; }
      .review-action-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; }
      .review-action-card {
        border: 1px solid var(--border); border-radius: 14px; background: rgba(43,54,68,0.025);
        padding: 14px 16px; min-width: 0;
      }
      .review-action-card__head { display:flex; align-items:center; justify-content:space-between; gap:10px; margin-bottom: 10px; }
      .review-action-card h3 { margin:0; font-family:"Montserrat", sans-serif; font-size:14px; }
      .review-action-card code { display:block; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; font-size:11.5px; color:rgba(43,54,68,.65); }
      .review-action-card p { margin:0 0 10px; color:rgba(43,54,68,.62); font-size:12.5px; line-height:1.45; }
      .review-action-card__buttons { display:flex; gap:8px; flex-wrap:wrap; }
      .review-drawers { display:grid; gap:8px; }
      .review-drawer { border:1px solid var(--border); border-radius:12px; background:#fff; overflow:hidden; }
      .review-drawer > summary {
        list-style:none; cursor:pointer; padding:11px 14px; display:flex; justify-content:space-between; align-items:center; gap:12px;
        font-family:"Montserrat", sans-serif; font-size:12px; font-weight:800;
      }
      .review-drawer > summary::-webkit-details-marker { display:none; }
      .review-drawer > summary::after { content:"▾"; color:rgba(43,54,68,.45); font-size:11px; }
      .review-drawer:not([open]) > summary::after { transform:rotate(-90deg); }
      .review-drawer__body { padding: 0 14px 13px; color:rgba(43,54,68,.72); font-size:13px; line-height:1.5; }
      .review-drawer__body ul { margin:6px 0 0; padding-left:18px; }
      .review-drawer__body pre { margin:8px 0 0; font-family:inherit; font-size:12.5px; white-space:pre-wrap; color:rgba(43,54,68,.72); line-height:1.55; }
      /* Pipeline table */
      .prospect-row { cursor: pointer; }
      .prospect-row:hover td { background: rgba(133,187,218,0.07); }
      .prospect-row td:first-child { border-left: 3px solid transparent; padding-left: 9px; transition: border-color 0.15s; }
      .prospect-row[data-stage="intake"] td:first-child { border-left-color: #94a3b8; }
      .prospect-row[data-stage="pending_fulfillment"] td:first-child { border-left-color: #38bdf8; }
      .prospect-row[data-stage="costs_received"] td:first-child { border-left-color: #a78bfa; }
      .prospect-row[data-stage="published"] td:first-child { border-left-color: #fbbf24; }
      .prospect-row[data-stage="won"] td:first-child { border-left-color: #4ade80; }
      .prospect-row[data-stage="lost"] td:first-child { border-left-color: #e2e8f0; }
      .row-chevron { display: inline-block; color: rgba(43,54,68,0.35); font-size: 13px;
        margin-right: 5px; transition: transform 0.15s; line-height: 1; vertical-align: middle; }
      .stage-select-wrap { position: relative; display: inline-block; }
      .stage-select-wrap::after {
        content: '▾'; position: absolute; right: 7px; top: 50%;
        transform: translateY(-50%); pointer-events: none;
        font-size: 9px; color: rgba(43,54,68,0.45); line-height: 1;
      }
      .stage-select {
        appearance: none; -webkit-appearance: none; border: none; border-radius: 999px;
        padding: 3px 20px 3px 10px; font-size: 11px; font-weight: 700;
        font-family: "Montserrat", sans-serif; letter-spacing: 0.03em; cursor: pointer;
      }
      .stage--intake        { background: #e2e8f0; color: #475569; }
      .stage--pending_fulfillment { background: #e0f2fe; color: #0369a1; }
      .stage--costs_received { background: #ede9fe; color: #6d28d9; }
      .stage--published     { background: #fef3c7; color: #b45309; }
      .stage--won           { background: #dcfce7; color: #15803d; }
      .stage--lost          { background: #f1f5f9; color: #94a3b8; }
      .expand-row td { padding: 0; border-bottom: 2px solid var(--border); }
      .expand-panel {
        padding: 18px 22px 22px; background: rgba(133,187,218,0.06);
        display: grid; grid-template-columns: 1fr 1fr; gap: 18px;
      }
      .expand-panel h3 { font-family: "Montserrat", sans-serif; font-size: 12px;
        font-weight: 700; letter-spacing: 0.06em; text-transform: uppercase;
        color: rgba(43,54,68,0.55); margin: 0 0 10px; }
      .cost-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 8px 14px; }
      .cost-grid label { font-size: 12px; font-weight: 600; display: block; margin-bottom: 2px; }
      .cost-grid input { width: 100%; min-height: 34px; padding: 0 10px;
        border-radius: 8px; border: 1px solid var(--border); font-size: 13px; }
      .margin-card { background: #fff; border: 1px solid var(--border); border-radius: 12px;
        padding: 12px 16px; margin-top: 10px; font-size: 13px; }
      .margin-card .big { font-size: 22px; font-weight: 800;
        font-family: "Montserrat", sans-serif; }
      .margin-card .big--pos { color: #15803d; }
      .margin-card .big--neg { color: #b91c1c; }
      .margin-line { display: flex; justify-content: space-between;
        padding: 3px 0; border-bottom: 1px solid rgba(43,54,68,0.07); font-size: 12px; }
      .margin-line:last-child { border: none; }
      .expand-notes { width: 100%; min-height: 70px; padding: 8px 10px;
        border-radius: 8px; border: 1px solid var(--border); font-size: 13px;
        font-family: inherit; resize: vertical; }
      @media (max-width: 900px) {
        .shell { padding-inline:16px; }
        .pipeline-toolbar { grid-template-columns:1fr 1fr; }
        .pipeline-toolbar .btn,.pipeline-results-count { justify-self:stretch; }
        .expand-panel { grid-template-columns: 1fr; }
        .grid2 { grid-template-columns: 1fr; }
        .operator-callout { grid-template-columns: 1fr; }
        .operator-callout__side { justify-content: flex-start; }
        .review-status-strip { grid-template-columns: 1fr; }
        .review-action-grid { grid-template-columns: 1fr; }
      }
"""


def _fmt_duration(seconds: int) -> str:
    if seconds <= 0:
        return "—"
    if seconds < 60:
        return f"{seconds}s"
    minutes, secs = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m {secs:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes:02d}m"


_STAGE_LABELS = {
    "intake": "Intake",
    "pending_fulfillment": "Sent to Fulfillment",
    "costs_received": "Costs Received",
    "published": "Published",
    "won": "Won",
    "lost": "Lost",
}

_STAGE_OPTIONS = "".join(
    f'<option value="{k}">{v}</option>' for k, v in _STAGE_LABELS.items()
)


def _stage_select(run_id: int, current: str) -> str:
    options = "".join(
        f'<option value="{k}" {"selected" if k == current else ""}>{v}</option>'
        for k, v in _STAGE_LABELS.items()
    )
    return (
        f'<div class="stage-select-wrap">'
        f'<select class="stage-select stage--{_esc(current)}" '
        f'onclick="event.stopPropagation()" '
        f'onchange="pipelineStage(this,{run_id})">{options}</select>'
        f'</div>'
    )


def _fmt_usd(value) -> str:
    if value is None:
        return "—"
    try:
        v = float(value)
        return f"−${abs(v):,.0f}" if v < 0 else f"${v:,.0f}"
    except (TypeError, ValueError):
        return "—"


def _fmt_rate(value) -> str:
    if value is None:
        return "—"
    try:
        v = float(value)
        sign = "−" if v < 0 else ""
        return f"{sign}${abs(v):,.2f}".rstrip("0").rstrip(".")
    except (TypeError, ValueError):
        return "—"


def _pass_through_monthly_from_quote(quote: dict) -> float:
    """Monthly revenue that should not be treated as profit."""
    total = 0.0
    for line in quote.get("lines") or []:
        if not isinstance(line, dict):
            continue
        if str(line.get("key") or "") == "shipping":
            try:
                total += float(line.get("monthly") or 0)
            except (TypeError, ValueError):
                pass
    return round(total, 2)


def _cost_form_path(run_id: int, summary: dict) -> str:
    token = str(summary.get("export_token") or "").strip()
    return f"/fulfillment-costs/{run_id}/{token}" if token else ""


def _build_brief(run: dict) -> str:
    """Plain-text fulfillment brief for clipboard copy."""
    profile = run.get("prospect_profile") or {}
    name = run.get("prospect") or run.get("design_title") or f"Run {run.get('id')}"
    origin = run.get("origin_zip") or "—"
    vol = run.get("monthly_order_volume")
    vol_str = f"{vol:,} orders/mo" if vol else "—"
    products = profile.get("products") or []
    prod_lines = []
    for p in products[:6]:
        pname = p.get("name") or "Product"
        l_, w_, h_, wt = (
            p.get("length_in"), p.get("width_in"),
            p.get("height_in"), p.get("weight_lb"),
        )
        units = p.get("monthly_units")
        dims = f"{l_}×{w_}×{h_}in" if None not in (l_, w_, h_) else "dims unknown"
        weight = f"{wt}lb" if wt else ""
        u_str = f" × {units:,} units/mo" if units else ""
        prod_lines.append(f"  {pname} ({dims}{', ' + weight if weight else ''}{u_str})")
    products_str = "\n".join(prod_lines) if prod_lines else "  (no products)"
    fragile = any(p.get("fragile") for p in products)
    cat = (products[0].get("product_category") or "unknown") if products else "unknown"
    return (
        f"Prospect: {name} | Origin ZIP: {origin} | Volume: {vol_str}\n"
        f"Products:\n{products_str}\n"
        f"Category: {cat} | Fragile: {'yes' if fragile else 'no'}"
    )


def _pricing_summary_html(summary: dict, profile: dict) -> str:
    quote = dict(summary.get("fulfillment_quote") or {})
    customer_monthly = float(quote.get("monthly_total") or 0)
    actual_costs = dict(summary.get("fulfillment_actual_costs") or {})
    has_actual_costs = bool(actual_costs and any(v for v in actual_costs.values() if v))
    if not customer_monthly and not has_actual_costs:
        return ""
    cost_html = '<div class="muted">Enter fulfillment costs in the pipeline row to calculate net.</div>'
    if has_actual_costs:
        try:
            from sales_support_agent.services.fulfillment_deck.quote import compute_margin
            from sales_support_agent.services.fulfillment_deck.schema import ProspectProfile
            pass_through = _pass_through_monthly_from_quote(quote)
            mg = compute_margin(customer_monthly, actual_costs, ProspectProfile.from_dict(profile or {}), pass_through)
            sign = "#15803d" if float(mg.get("monthly_margin") or 0) >= 0 else "#b91c1c"
            pass_through_html = (
                f"<div class=\"margin-line\"><span>Carrier/pass-through revenue</span><span>−{_fmt_usd(pass_through)}</span></div>"
                if pass_through else ""
            )
            cost_html = f"""
              {pass_through_html}
              <div class="margin-line"><span>Marginable monthly revenue</span><span>{_fmt_usd(mg.get('marginable_revenue'))}</span></div>
              <div class="margin-line"><span>Fulfillment monthly cost</span><span>−{_fmt_usd(mg.get('actual_monthly'))}</span></div>
              <div class="margin-line" style="font-weight:800;color:{sign}"><span>Estimated monthly net margin</span><span>{_fmt_usd(mg.get('monthly_margin'))} ({_esc(mg.get('margin_pct'))}%)</span></div>
              <div class="margin-line"><span>Estimated annual net margin</span><span>{_fmt_usd(mg.get('annual_margin'))}</span></div>
              <div class="margin-line" style="font-size:12px;color:rgba(43,54,68,0.62)"><span>Formula</span><span>(customer monthly − pass-through − fulfillment costs) × 12</span></div>
              {('<div class="margin-line" style="font-size:12px;color:#b91c1c"><span>Margin warning</span><span>Internal costs exceed marginable revenue.</span></div>' if float(mg.get("monthly_margin") or 0) < 0 else '')}
            """
        except Exception:
            cost_html = '<div class="muted">Could not calculate margin from stored fulfillment costs.</div>'
    return f"""
      <div class="flash" style="background:rgba(43,54,68,0.035);border-color:rgba(43,54,68,0.12);margin:14px 0 18px">
        <strong>Pricing definitions</strong>
        <div class="grid2" style="margin-top:10px">
          <div>
            <div class="margin-line"><span>Customer-facing monthly estimate</span><span>{_fmt_usd(customer_monthly)}</span></div>
            {cost_html}
          </div>
          <div class="muted" style="font-size:12px;line-height:1.5">
            <strong>Fee Card Adjustments</strong> are prices shown to the customer in the rate sheet and used for the monthly estimate.
            <br><strong>Fulfillment costs</strong> are internal warehouse costs from the pipeline.
            <br><strong>Net margin</strong> = customer-facing monthly estimate minus carrier/pass-through revenue, then minus internal fulfillment monthly cost. Annual margin is monthly net margin × 12.
          </div>
        </div>
      </div>
    """


def _history_bar_html(summary: dict) -> str:
    history = [h for h in (summary.get("negotiation_history") or []) if isinstance(h, dict)]
    if not history:
        return """
        <div class="flash" style="background:rgba(43,54,68,0.025);border-color:rgba(43,54,68,0.10);margin-top:18px">
          <strong>Negotiation history</strong>
          <p class="muted" style="margin:6px 0 0">No saved pricing changes yet. Future saves, re-publishes, and quote actions will appear here.</p>
        </div>"""
    items = []
    for h in reversed(history[-8:]):
        at = str(h.get("at") or "")
        when = at[:16].replace("T", " ") if at else "unknown time"
        user = str(h.get("user_email") or "").strip()
        detail = str(h.get("detail") or "").strip()
        meta = " · ".join(x for x in (when, user) if x)
        items.append(
            '<div class="history-item">'
            f'<strong>{_esc(h.get("event") or "Updated")}</strong>'
            f'<span>{_esc(meta)}</span>'
            f'{f"<em>{_esc(detail)}</em>" if detail else ""}'
            '</div>'
        )
    return (
        '<div class="history-bar">'
        '<strong>Negotiation history</strong>'
        '<div class="history-list">' + "".join(items) + '</div>'
        '</div>'
    )


_COST_FORM_FIELDS = (
    ("Core pick/pack", (
        ("actual_pick_pack_per_order", "pick_pack_per_order", "DTC pick & pack / order", INTERNAL_COST_BASELINES["dtc_base_per_order"], "per order"),
        ("actual_pick_pack_additional_item", "pick_pack_additional_item", "DTC additional item", INTERNAL_COST_BASELINES["dtc_additional_item"], "per additional item"),
        ("actual_monthly_tech_fee", "monthly_tech_fee", "Monthly tech fee", INTERNAL_COST_BASELINES["monthly_tech_fee"], "per month"),
        ("actual_customer_service_monthly", "customer_service_monthly", "Customer service", INTERNAL_COST_BASELINES["customer_service_monthly"], "per month"),
    )),
    ("Receiving & storage", (
        ("actual_receiving_precounted_box", "receiving_precounted_box", "Receiving pre-counted box", INTERNAL_COST_BASELINES["receiving_precounted_box"], "one-time / box"),
        ("actual_receiving_count_per_item", "receiving_count_per_item", "Receiving counted item", INTERNAL_COST_BASELINES["receiving_count_per_item"], "one-time / item"),
        ("actual_receiving_per_pallet", "receiving_per_pallet", "Receiving legacy pallet", INTERNAL_COST_BASELINES["receiving_per_pallet"], "one-time / pallet"),
        ("actual_storage_per_pallet_mo", "storage_per_pallet_mo", "Storage / pallet", INTERNAL_COST_BASELINES["storage_short_per_pallet_mo"], "per pallet / month"),
        ("actual_storage_cubic_foot_mo", "storage_cubic_foot_mo", "Storage / cubic foot", INTERNAL_COST_BASELINES["storage_cubic_foot_mo"], "per cubic foot / month"),
    )),
    ("Value-added services", (
        ("actual_pallet_order_per_pallet", "pallet_order_per_pallet", "Pallet orders", INTERNAL_COST_BASELINES["pallet_order_per_pallet"], "per pallet"),
        ("actual_kitting_per_item", "kitting_per_item", "Kitting", INTERNAL_COST_BASELINES["kitting_per_unit"], "per item"),
        ("actual_labeling_per_item", "labeling_per_item", "Labeling", INTERNAL_COST_BASELINES["labeling_per_unit"], "per item"),
        ("actual_bagging_labeling_per_item", "bagging_labeling_per_item", "Bagging + labeling", INTERNAL_COST_BASELINES["bagging_labeling_per_unit"], "per item"),
    )),
    ("Returns & projects", (
        ("actual_returns_units_mo", "returns_units_mo", "Returns units / month", 0, "units / month"),
        ("actual_returns_receive_per_unit", "returns_receive_per_unit", "Return receive", INTERNAL_COST_BASELINES["returns_receive_per_unit"], "per return"),
        ("actual_returns_examination_per_unit", "returns_examination_per_unit", "Return examination", INTERNAL_COST_BASELINES["returns_examination_per_unit"], "per return"),
        ("actual_returns_custom_steps_per_unit", "returns_custom_steps_per_unit", "Return custom steps", INTERNAL_COST_BASELINES["returns_custom_steps_per_unit"], "per return"),
        ("actual_special_project_hours_mo", "special_project_hours_mo", "Special project hours / month", 0, "hours / month"),
        ("actual_special_projects_per_hour", "special_projects_per_hour", "Special projects", INTERNAL_COST_BASELINES["special_projects_per_hour"], "per hour"),
    )),
)


def render_fulfillment_cost_form_page(
    run_id: int,
    summary: dict,
    *,
    saved: bool = False,
    error: str = "",
    form_values: dict[str, str] | None = None,
) -> str:
    profile = dict(summary.get("prospect_profile") or {})
    costs = dict(summary.get("fulfillment_actual_costs") or {})
    prospect = str(summary.get("prospect") or summary.get("design_title") or profile.get("brand") or f"Run {run_id}")
    products = [p for p in (profile.get("products") or []) if isinstance(p, dict)]
    token = str(summary.get("export_token") or "")
    form_path = _cost_form_path(run_id, summary)
    submitted = dict(form_values or {})

    def _value(form_name: str, key: str, suggested: float) -> str:
        if form_name in submitted:
            return str(submitted.get(form_name) or "")
        value = costs.get(key)
        if value in (None, ""):
            value = suggested
        try:
            return f"{float(value):g}"
        except (TypeError, ValueError):
            return ""

    def _product_rows() -> str:
        if not products:
            return '<tr><td colspan="5" class="muted">No products were parsed yet.</td></tr>'
        rows = []
        for p in products:
            dims = " × ".join(
                str(p.get(k) if p.get(k) is not None else "—")
                for k in ("length_in", "width_in", "height_in")
            )
            rows.append(
                "<tr>"
                f"<td>{_esc(p.get('name') or 'Product')}</td>"
                f"<td>{_esc(dims)} in</td>"
                f"<td>{_esc(p.get('weight_lb') or '—')}</td>"
                f"<td>{_esc(p.get('monthly_units') or '—')}</td>"
                f"<td>{'Yes' if p.get('fragile') else 'No'}</td>"
                "</tr>"
            )
        return "".join(rows)

    def _submission_history() -> str:
        submissions = [
            s for s in (summary.get("fulfillment_cost_submissions") or [])
            if isinstance(s, dict)
        ]
        if not submissions:
            return (
                '<section class="cost-group">'
                '<h2>Signed submission history</h2>'
                '<p class="muted">No signed fulfillment cost submissions yet.</p>'
                '</section>'
            )
        rows = []
        for item in reversed(submissions[-8:]):
            at = str(item.get("at") or "")
            when = at[:16].replace("T", " ") if at else "unknown time"
            rows.append(
                "<tr>"
                f"<td>{_esc(when)}</td>"
                f"<td>{_esc(item.get('name') or '—')}</td>"
                f"<td>{_esc(item.get('email') or '—')}</td>"
                "</tr>"
            )
        return (
            '<section class="cost-group">'
            '<h2>Signed submission history</h2>'
            '<p class="muted">These are previous fulfillment-only cost saves for this prospect. Sales pricing and margin are not shown here.</p>'
            '<table><thead><tr><th>Saved</th><th>Name</th><th>Email</th></tr></thead>'
            f'<tbody>{"".join(rows)}</tbody></table>'
            '</section>'
        )

    try:
        from sales_support_agent.services.fulfillment_deck.quote import (
            estimate_pallets_mo,
            estimate_storage_cuft_mo,
        )
        profile_obj = ProspectProfile.from_dict(profile)
        pallet_est = estimate_pallets_mo(profile_obj)
        cuft_est = estimate_storage_cuft_mo(profile_obj)
    except Exception:
        pallet_est = 0
        cuft_est = 0

    groups_html = []
    for group, fields in _COST_FORM_FIELDS:
        inputs = []
        for form_name, key, label, suggested, unit in fields:
            step = "1" if key in {"returns_units_mo"} else "0.01"
            inputs.append(
                '<div class="field">'
                f'<label for="{form_name}">{_esc(label)}</label>'
                f'<input type="number" id="{form_name}" name="{form_name}" step="{step}" min="0" value="{_esc(_value(form_name, key, suggested))}">'
                f'<span class="hint">Suggested: {_fmt_rate(suggested)} · {_esc(unit)}</span>'
                '</div>'
            )
        groups_html.append(
            '<section class="cost-group">'
            f'<h2>{_esc(group)}</h2>'
            '<div class="cost-grid-wide">' + "".join(inputs) + '</div>'
            '</section>'
        )

    saved_html = (
        '<div class="flash flash--ok"><strong>Saved.</strong> Fulfillment costs were pushed back to Agent.</div>'
        if saved else ""
    )
    error_html = (
        f'<div class="flash flash--error"><strong>Required before saving.</strong> {_esc(error)}</div>'
        if error else ""
    )
    return f"""<!doctype html>
<html lang="en" data-design-system="{PUBLIC_REPORT_DESIGN_VERSION}">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Fulfillment cost form | {_esc(prospect)}</title>
    {render_agent_favicon_links()}
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Montserrat:wght@700;800&display=swap" rel="stylesheet">
    <style>
      {public_report_foundation_css()}
      :root {{ --dark-blue:#2B3644; --light-blue:#85BBDA; --light-brown:#F9F7F3; --border:rgba(43,54,68,.12); --shadow:rgba(43,54,68,.10); }}
      * {{ box-sizing:border-box; }}
      body {{ margin:0; background:var(--light-brown); color:var(--dark-blue); font-family:"Inter","Segoe UI",sans-serif; }}
      .shell {{ max-width:1120px; margin:0 auto; padding:28px 18px 64px; }}
      .workspace {{ background:white; border:1px solid var(--border); border-radius:18px; box-shadow:0 18px 40px var(--shadow); padding:26px 28px 30px; }}
      h1 {{ font-family:"Montserrat",sans-serif; font-size:26px; margin:0 0 4px; }}
      h2 {{ font-family:"Montserrat",sans-serif; font-size:15px; margin:0 0 10px; }}
      .eyebrow {{ font-family:"Montserrat",sans-serif; font-size:11px; font-weight:800; letter-spacing:.08em; text-transform:uppercase; color:rgba(43,54,68,.55); margin:0 0 4px; }}
      .intro {{ color:rgba(43,54,68,.72); margin:0 0 18px; max-width:760px; line-height:1.45; }}
      .flash {{ border:1px solid rgba(133,187,218,.55); background:rgba(133,187,218,.16); border-radius:12px; padding:12px 14px; margin:14px 0; }}
      .flash--ok {{ border-color:rgba(46,125,91,.35); background:rgba(46,125,91,.12); }}
      .flash--error {{ border-color:rgba(178,72,54,.45); background:rgba(178,72,54,.10); }}
      .facts {{ display:grid; grid-template-columns:repeat(4,minmax(150px,1fr)); gap:10px; margin:16px 0; }}
      .fact {{ border:1px solid var(--border); border-radius:12px; padding:10px 12px; background:#fff; }}
      .fact span {{ display:block; font-size:11px; color:rgba(43,54,68,.55); font-weight:700; text-transform:uppercase; letter-spacing:.05em; }}
      .fact strong {{ display:block; margin-top:3px; font-size:16px; }}
      .cost-group {{ border:1px solid var(--border); border-radius:14px; padding:14px 16px; margin:12px 0; background:#fff; }}
      .cost-grid-wide {{ display:grid; grid-template-columns:repeat(3,minmax(220px,1fr)); gap:12px 16px; }}
      .field {{ display:grid; gap:5px; }}
      .field label {{ font-family:"Montserrat",sans-serif; font-size:12px; font-weight:800; }}
      .field input {{ width:100%; min-height:44px; padding:0 12px; border:1px solid var(--border); border-radius:10px; font-size:15px; font-family:inherit; }}
      .field input:focus {{ outline:2px solid rgba(133,187,218,.38); border-color:rgba(133,187,218,.9); }}
      .hint,.muted {{ font-size:12px; color:rgba(43,54,68,.55); }}
      table {{ width:100%; border-collapse:collapse; margin:10px 0 18px; font-size:13px; }}
      .table-wrap,.cost-group {{ max-width:100%; overflow-x:auto; }}
      th,td {{ text-align:left; padding:8px 10px; border-bottom:1px solid var(--border); }}
      th {{ background:rgba(133,187,218,.18); font-family:"Montserrat",sans-serif; font-size:11px; text-transform:uppercase; letter-spacing:.04em; }}
      .btn {{ display:inline-flex; align-items:center; min-height:46px; padding:0 22px; border-radius:999px; border:0; background:var(--dark-blue); color:white; font-family:"Montserrat",sans-serif; font-weight:800; cursor:pointer; }}
      .actions {{ display:flex; align-items:center; gap:12px; flex-wrap:wrap; margin-top:18px; }}
      @media (max-width:860px) {{ .facts,.cost-grid-wide {{ grid-template-columns:1fr; }} }}
    </style>
  </head>
  <body>
    <a class="public-report-skip" href="#cost-form">Skip to fulfillment cost form</a>
    <main id="cost-form" class="shell">
      <div class="workspace">
        <p class="public-report-wordmark" aria-label="Anata">anata<span>.</span></p>
        <p class="eyebrow">Anata fulfillment cost input</p>
        <h1>{_esc(prospect)}</h1>
        <p class="intro">This page is for fulfillment cost input only. It does not show sales pricing, customer pitch, margin, or quote details. Suggested values are based on current baseline operating costs; overwrite anything that needs warehouse-specific pricing.</p>
        <div aria-live="polite">{saved_html}</div>
        <div role="alert">{error_html}</div>
        <div class="facts">
          <div class="fact"><span>Monthly orders</span><strong>{_esc(profile.get('monthly_order_volume') or '—')}</strong></div>
          <div class="fact"><span>Products</span><strong>{len(products)}</strong></div>
          <div class="fact"><span>Est. pallets/mo</span><strong>{_esc(pallet_est or '—')}</strong></div>
          <div class="fact"><span>Est. cu ft/mo</span><strong>{_esc(cuft_est or '—')}</strong></div>
        </div>
        <h2>Latest product inputs</h2>
        <div class="table-wrap"><table>
          <thead><tr><th>Product</th><th>Dims</th><th>Weight lb</th><th>Units/mo</th><th>Fragile</th></tr></thead>
          <tbody>{_product_rows()}</tbody>
        </table></div>
        {_submission_history()}
        <form method="post" action="{_esc(form_path)}">
          <section class="cost-group">
            <h2>Cost submission signature</h2>
            <p class="muted">Required on every save so Agent can track who committed the fulfillment costs.</p>
            <div class="cost-grid-wide">
              <div class="field">
                <label for="submitter_name">Your name</label>
                <input type="text" id="submitter_name" name="submitter_name" autocomplete="name" value="{_esc(submitted.get('submitter_name') or '')}" required>
              </div>
              <div class="field">
                <label for="submitter_email">Your email</label>
                <input type="email" id="submitter_email" name="submitter_email" autocomplete="email" value="{_esc(submitted.get('submitter_email') or '')}" required>
              </div>
            </div>
          </section>
          {''.join(groups_html)}
          <div class="actions">
            <button class="btn" type="submit">Save fulfillment costs</button>
            <span class="muted">This pushes costs back to Agent and updates the sales-side margin view.</span>
          </div>
        </form>
      </div>
    </main>
  </body>
</html>"""


def _expand_panel(run: dict) -> str:
    """Collapsible expand panel for cost entry, margin, notes, brief."""
    run_id = int(run.get("id") or 0)
    costs = run.get("fulfillment_actual_costs") or {}
    notes = _esc(run.get("pipeline_notes") or "")
    pitched = run.get("pitched_monthly")

    def _cv(key: str) -> str:
        v = costs.get(key)
        return f"{v:g}" if v is not None else ""

    # Pre-compute margin if costs are present
    margin_html = ""
    if costs and pitched and any(v for v in costs.values() if v):
        try:
            from sales_support_agent.services.fulfillment_deck.quote import compute_margin
            from sales_support_agent.services.fulfillment_deck.schema import ProspectProfile
            profile_obj = ProspectProfile.from_dict(run.get("prospect_profile") or {})
            mg = compute_margin(float(pitched), costs, profile_obj, float(run.get("pass_through_monthly") or 0))
            sign = "pos" if mg["monthly_margin"] >= 0 else "neg"
            _rec_pp = float(costs.get("receiving_per_pallet") or 0)
            _rec_box = float(costs.get("receiving_precounted_box") or 0)
            _rec_count = float(costs.get("receiving_count_per_item") or 0)
            _rec_pallets = int(mg.get("pallets_mo") or 0)
            _rec_units = int(mg.get("units_total") or 0)
            _rec_total_val = (_rec_pp * _rec_pallets) + (_rec_box * _rec_pallets) + (_rec_count * _rec_units)
            _rec_line = (
                f'<div class="margin-line" style="opacity:0.65"><span>Receiving one-time (~{_rec_pallets} pallets)</span>'
                f'<span>−{_fmt_usd(_rec_total_val)}</span></div>'
                if _rec_total_val and _rec_pallets else ""
            )
            _optional_line = (
                f'<div class="margin-line"><span>Optional/service actuals</span><span>−{_fmt_usd(mg.get("actual_optional_monthly") or 0)}</span></div>'
                if mg.get("actual_optional_monthly") else ""
            )
            margin_html = f"""
            <div class="margin-card" id="margin-{run_id}">
              <div class="big big--{sign}">{_fmt_usd(mg['monthly_margin'])}<span style="font-size:14px;font-weight:400">/mo ({mg['margin_pct']}%)</span></div>
              <div class="margin-line"><span>Customer monthly estimate</span><span>{_fmt_usd(pitched)}</span></div>
              <div class="margin-line"><span>Carrier/pass-through revenue</span><span>−{_fmt_usd(mg.get('pass_through_monthly'))}</span></div>
              <div class="margin-line"><span>Marginable revenue</span><span>{_fmt_usd(mg.get('marginable_revenue'))}</span></div>
              <div class="margin-line"><span>Pick &amp; pack actual</span><span>−{_fmt_usd(mg['actual_pick_pack'])}</span></div>
              <div class="margin-line"><span>Storage actual</span><span>−{_fmt_usd(mg['actual_storage'])}</span></div>
              <div class="margin-line"><span>Tech fee actual</span><span>−{_fmt_usd(mg['actual_tech_fee'])}</span></div>
              {_optional_line}
              {_rec_line}
              <div class="margin-line" style="font-weight:700"><span>Annual margin</span><span>{_fmt_usd(mg['annual_margin'])}</span></div>
            </div>"""
        except Exception:
            margin_html = f'<div class="margin-card" id="margin-{run_id}"></div>'
    else:
        margin_html = f'<div class="margin-card" id="margin-{run_id}" style="color:rgba(43,54,68,0.45);font-size:12px">Enter actual costs above to see margin.</div>'

    brief_attr = html.escape(_build_brief(run), quote=True)
    _view_path = _esc(str(run.get("view_path") or ""))
    _hs_quote_url = _esc(str(run.get("hubspot_quote_url") or ""))
    _hs_deal_url = _esc(str(run.get("hubspot_deal_url") or ""))
    _cost_form_url_path = _esc(_cost_form_path(run_id, run))
    _quick_links = ""
    if _view_path or _hs_deal_url or _cost_form_url_path:
        _btns = []
        if _cost_form_url_path:
            _btns.append(
                f'<button class="btn btn--ghost" type="button" style="font-size:12px" '
                f"onclick=\"event.stopPropagation();navigator.clipboard.writeText(window.location.origin+'{_cost_form_url_path}');"
                f"this.textContent='Cost form copied!';setTimeout(()=>this.textContent='Copy cost form',1800)\">Copy cost form</button>"
                f'<a class="btn btn--ghost" href="{_cost_form_url_path}" target="_blank" rel="noreferrer" '
                f'onclick="event.stopPropagation()" style="font-size:12px">Open cost form</a>'
            )
        if _view_path:
            _btns.append(
                f'<button class="btn btn--ghost" type="button" style="font-size:12px" '
                f"onclick=\"navigator.clipboard.writeText(window.location.origin+'{_view_path}');"
                f"this.textContent='Copied!';setTimeout(()=>this.textContent='Copy link',1800)\">Copy link</button>"
                f'<a class="btn btn--ghost" href="/admin/fulfillment/sales/runs/{run_id}/review" '
                f'target="_blank" rel="noreferrer" onclick="event.stopPropagation()" style="font-size:12px">Edit rate sheet →</a>'
            )
        if _hs_deal_url:
            _btns.append(
                f'<a class="btn btn--ghost" href="{_hs_deal_url}" target="_blank" rel="noreferrer" '
                f'onclick="event.stopPropagation()" style="font-size:12px;color:#FF7A59">HubSpot Deal</a>'
            )
        if _hs_quote_url:
            _btns.append(
                f'<a class="btn btn--ghost" href="{_hs_quote_url}" target="_blank" rel="noreferrer" '
                f'onclick="event.stopPropagation()" style="font-size:12px;color:#FF7A59">HubSpot Quote ✍</a>'
            )
        _quick_links = f'<div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:16px">{"".join(_btns)}</div>'

    return f"""
    <div class="expand-panel">
      <div>
        {_quick_links}
        <h3>Fulfillment Team Costs</h3>
        <div class="cost-grid">
          <div><label>Pick &amp; pack ($/order)</label>
            <input type="number" step="0.01" min="0" placeholder="{INTERNAL_COST_BASELINES['dtc_base_per_order']:.2f}"
              id="pp-{run_id}" value="{_cv('pick_pack_per_order')}"></div>
          <div><label>Additional item ($/item)</label>
            <input type="number" step="0.01" min="0" placeholder="{INTERNAL_COST_BASELINES['dtc_additional_item']:.2f}"
              id="ppi-{run_id}" value="{_cv('pick_pack_additional_item')}"></div>
          <div><label>Storage ($/pallet/mo)</label>
            <input type="number" step="0.01" min="0" placeholder="{INTERNAL_COST_BASELINES['storage_short_per_pallet_mo']:.2f}"
              id="st-{run_id}" value="{_cv('storage_per_pallet_mo')}"></div>
          <div><label>Storage ($/cu ft/mo)</label>
            <input type="number" step="0.01" min="0" placeholder="{INTERNAL_COST_BASELINES['storage_cubic_foot_mo']:.2f}"
              id="scf-{run_id}" value="{_cv('storage_cubic_foot_mo')}"></div>
          <div><label>Receiving pre-counted box <span style="font-weight:400;font-size:11px;opacity:.6">— one-time</span></label>
            <input type="number" step="0.01" min="0" placeholder="{INTERNAL_COST_BASELINES['receiving_precounted_box']:.2f}"
              id="rcb-{run_id}" value="{_cv('receiving_precounted_box')}"></div>
          <div><label>Receiving counted item <span style="font-weight:400;font-size:11px;opacity:.6">— one-time</span></label>
            <input type="number" step="0.01" min="0" placeholder="{INTERNAL_COST_BASELINES['receiving_count_per_item']:.2f}"
              id="rci-{run_id}" value="{_cv('receiving_count_per_item')}"></div>
          <div><label>Receiving legacy ($/pallet) <span style="font-weight:400;font-size:11px;opacity:.6">— one-time</span></label>
            <input type="number" step="0.01" min="0" placeholder="{INTERNAL_COST_BASELINES['receiving_per_pallet']:.2f}"
              id="rc-{run_id}" value="{_cv('receiving_per_pallet')}"></div>
          <div><label>Tech fee ($/mo)</label>
            <input type="number" step="0.01" min="0" placeholder="{INTERNAL_COST_BASELINES['monthly_tech_fee']:.2f}"
              id="tf-{run_id}" value="{_cv('monthly_tech_fee')}"></div>
          <div><label>Customer service ($/mo)</label>
            <input type="number" step="0.01" min="0" placeholder="{INTERNAL_COST_BASELINES['customer_service_monthly']:.2f}"
              id="cs-{run_id}" value="{_cv('customer_service_monthly')}"></div>
          <div><label>Pallet orders ($/pallet)</label>
            <input type="number" step="0.01" min="0" placeholder="{INTERNAL_COST_BASELINES['pallet_order_per_pallet']:.2f}"
              id="po-{run_id}" value="{_cv('pallet_order_per_pallet')}"></div>
          <div><label>Kitting ($/item)</label>
            <input type="number" step="0.01" min="0" placeholder="{INTERNAL_COST_BASELINES['kitting_per_unit']:.2f}"
              id="kit-{run_id}" value="{_cv('kitting_per_item')}"></div>
          <div><label>Labeling ($/item)</label>
            <input type="number" step="0.01" min="0" placeholder="{INTERNAL_COST_BASELINES['labeling_per_unit']:.2f}"
              id="lab-{run_id}" value="{_cv('labeling_per_item')}"></div>
          <div><label>Bagging + labeling ($/item)</label>
            <input type="number" step="0.01" min="0" placeholder="{INTERNAL_COST_BASELINES['bagging_labeling_per_unit']:.2f}"
              id="bag-{run_id}" value="{_cv('bagging_labeling_per_item')}"></div>
          <div><label>Returns units (/mo)</label>
            <input type="number" step="1" min="0" placeholder="0"
              id="ru-{run_id}" value="{_cv('returns_units_mo')}"></div>
          <div><label>Return receive ($/unit)</label>
            <input type="number" step="0.01" min="0" placeholder="{INTERNAL_COST_BASELINES['returns_receive_per_unit']:.2f}"
              id="rr-{run_id}" value="{_cv('returns_receive_per_unit')}"></div>
          <div><label>Return exam ($/unit)</label>
            <input type="number" step="0.01" min="0" placeholder="{INTERNAL_COST_BASELINES['returns_examination_per_unit']:.2f}"
              id="re-{run_id}" value="{_cv('returns_examination_per_unit')}"></div>
          <div><label>Return custom steps ($/unit)</label>
            <input type="number" step="0.01" min="0" placeholder="{INTERNAL_COST_BASELINES['returns_custom_steps_per_unit']:.2f}"
              id="rs-{run_id}" value="{_cv('returns_custom_steps_per_unit')}"></div>
          <div><label>Special project hours (/mo)</label>
            <input type="number" step="0.25" min="0" placeholder="0"
              id="sph-{run_id}" value="{_cv('special_project_hours_mo')}"></div>
          <div><label>Special projects ($/hour)</label>
            <input type="number" step="0.01" min="0" placeholder="{INTERNAL_COST_BASELINES['special_projects_per_hour']:.2f}"
              id="spr-{run_id}" value="{_cv('special_projects_per_hour')}"></div>
        </div>
        <button class="btn btn--ghost" style="margin-top:10px" type="button"
          onclick="pipelineCosts(this,{run_id})">Save costs</button>
        {margin_html}
      </div>
      <div>
        <h3>Internal Notes</h3>
        <textarea class="expand-notes" placeholder="Call notes, deal context, next steps…"
          oninput="pipelineNotesDebounce(this,{run_id})">{notes}</textarea>
        <div style="display:flex;gap:6px;margin-top:10px">
          <button class="btn" type="button"
            style="background:#15803d;min-height:34px;font-size:12px;padding:0 14px"
            onclick="quickStage(this,'won',{run_id})">Mark as Won ✓</button>
          <button class="btn btn--ghost" type="button"
            style="color:#94a3b8;border-color:#e2e8f0;font-size:12px"
            onclick="quickStage(this,'lost',{run_id})">Archive / Lost</button>
        </div>
        <h3 style="margin-top:14px">Fulfillment Brief</h3>
        <p class="muted" style="margin:0 0 6px">Copy and share with the warehouse team for costing.</p>
        <div style="display:flex;gap:8px;flex-wrap:wrap">
          <button class="btn btn--ghost" type="button"
            data-brief="{brief_attr}"
            onclick="navigator.clipboard.writeText(this.dataset.brief);this.textContent='Copied!';setTimeout(()=>this.textContent='Copy brief',2000)">Copy brief</button>
          <button class="btn btn--ghost" type="button" style="font-size:12px"
            onclick="sendBriefEmail(this,{run_id})">Send to warehouse →</button>
        </div>
        <span id="send-err-{run_id}" style="font-size:11px;color:#b91c1c;display:none;margin-top:4px;display:none"></span>
      </div>
    </div>"""


def _pipeline_stats(runs: list[dict]) -> str:
    """Four-stat summary bar above the pipeline table."""
    active = [r for r in runs if r.get("pipeline_stage") not in ("won", "lost")]
    won = [r for r in runs if r.get("pipeline_stage") == "won"]

    pitched_active = sum(float(r.get("pitched_monthly") or 0) for r in active)
    pitched_won = sum(float(r.get("pitched_monthly") or 0) for r in won)

    margin_active = 0.0
    margin_runs = 0
    for r in active:
        costs = r.get("fulfillment_actual_costs") or {}
        pitched = r.get("pitched_monthly")
        if costs and pitched and any(v for v in costs.values() if v):
            try:
                from sales_support_agent.services.fulfillment_deck.quote import compute_margin
                from sales_support_agent.services.fulfillment_deck.schema import ProspectProfile
                mg = compute_margin(
                    float(pitched),
                    costs,
                    ProspectProfile.from_dict(r.get("prospect_profile") or {}),
                    float(r.get("pass_through_monthly") or 0),
                )
                margin_active += mg["monthly_margin"]
                margin_runs += 1
            except Exception:
                pass

    def _stat(label: str, val: str, sub: str = "", extra_cls: str = "") -> str:
        sub_html = f'<div class="pipeline-stat__sub">{_esc(sub)}</div>' if sub else ""
        return (
            f'<div class="pipeline-stat {extra_cls}">'
            f'<div class="pipeline-stat__val">{val}</div>'
            f'<div class="pipeline-stat__label">{_esc(label)}</div>'
            f'{sub_html}</div>'
        )

    active_str = str(len(active))
    pipeline_str = f"${pitched_active:,.0f}<span style='font-size:13px;font-weight:400'>/mo</span>" if pitched_active else "—"
    margin_str = f"${margin_active:,.0f}<span style='font-size:13px;font-weight:400'>/mo</span>" if margin_active else "—"
    margin_sub = f"{margin_runs} of {len(active)} with costs" if active else ""
    won_pct = round(len(won) / len(runs) * 100) if runs else 0
    won_str = str(len(won)) if won else "0"
    won_sub_parts = []
    if won and pitched_won:
        won_sub_parts.append(f"${pitched_won:,.0f}/mo booked")
    if runs:
        won_sub_parts.append(f"{won_pct}% conversion")
    won_sub = " · ".join(won_sub_parts) if won_sub_parts else "no wins yet"

    return (
        f'<div class="pipeline-stats">'
        f'{_stat("Active prospects", active_str)}'
        f'{_stat("Pitched pipeline", pipeline_str, f"${pitched_active * 12:,.0f}/yr potential" if pitched_active else "")}'
        f'<div class="pipeline-stat" id="stat-margin"><div class="pipeline-stat__val">{margin_str}</div>'
        f'<div class="pipeline-stat__label">Monthly margin</div>'
        f'<div class="pipeline-stat__sub">{_esc(margin_sub)}</div></div>'
        f'{_stat("Won", won_str, won_sub, "pipeline-stat--won" if won else "")}'
        f'</div>'
    )


def _pipeline_next_action(runs: list[dict], engagement: dict[int, dict]) -> str:
    """Resolve the single best pipeline action so the page does not start as a table audit."""
    if not runs:
        return (
            '<section class="operator-callout">'
            '<div><p class="eyebrow">Next action</p><h2>Create the first prospect rate sheet.</h2>'
            '<p>Paste prospect notes, upload supporting files, and generate the hosted rate sheet. The pipeline will track follow-up after the sheet exists.</p></div>'
            '<div class="operator-callout__side"><a class="btn" href="#notes">Start intake</a></div>'
            '</section>'
        )

    def _name(run: dict) -> str:
        return _esc(run.get("prospect") or run.get("design_title") or f"Run {int(run.get('id') or 0)}")

    for run in runs:
        if str(run.get("status") or "") == "running":
            return (
                '<section class="operator-callout">'
                f'<div><p class="eyebrow">Next action</p><h2>Rate sheet is building for {_name(run)}.</h2>'
                '<p>Wait for the generation pass to finish, then review the output before sending it to the prospect.</p>'
                '<div class="operator-callout__meta">The page can be refreshed safely while generation is running.</div></div>'
                '<div class="operator-callout__side"><a class="btn btn--ghost" href="#pipeline">Watch pipeline</a></div>'
                '</section>'
            )

    for run in runs:
        run_id = int(run.get("id") or 0)
        if str(run.get("status") or "") == "draft" or not bool(run.get("published")):
            return (
                '<section class="operator-callout">'
                f'<div><p class="eyebrow">Next action</p><h2>Review and publish {_name(run)}.</h2>'
                '<p>The rate sheet is not live yet. Review the extracted details, confirm pricing, and publish only when the offer is ready to share.</p>'
                '<div class="operator-callout__meta">Resolution path: review -> publish -> share link or create quote.</div></div>'
                f'<div class="operator-callout__side"><a class="btn" href="/admin/fulfillment/sales/runs/{run_id}/review">Review rate sheet</a></div>'
                '</section>'
            )

    for run in runs:
        run_id = int(run.get("id") or 0)
        stage = str(run.get("pipeline_stage") or "intake")
        costs = run.get("fulfillment_actual_costs") or {}
        if stage in {"intake", "pending_fulfillment"} and not any(v for v in costs.values() if v):
            cost_form_path = _cost_form_path(run_id, run)
            copy_cost_button = (
                f'<button class="btn" type="button" onclick="navigator.clipboard.writeText(window.location.origin + \'{_esc(cost_form_path)}\');this.textContent=\'Cost form copied\';">Copy cost form</button>'
                if cost_form_path else ""
            )
            return (
                '<section class="operator-callout">'
                f'<div><p class="eyebrow">Next action</p><h2>Get warehouse costs for {_name(run)}.</h2>'
                '<p>The public offer exists, but margin is not trusted until fulfillment costs are entered. Send the warehouse brief or cost form, then update the row.</p>'
                '<div class="operator-callout__meta">Resolution path: collect costs -> save margin -> create quote.</div></div>'
                f'<div class="operator-callout__side">{copy_cost_button}<a class="btn btn--ghost" href="/admin/fulfillment/sales/runs/{run_id}/review">Open review</a></div>'
                '</section>'
            )

    for run in runs:
        run_id = int(run.get("id") or 0)
        stage = str(run.get("pipeline_stage") or "intake")
        if stage == "costs_received" and not str(run.get("hubspot_quote_url") or "").strip():
            return (
                '<section class="operator-callout">'
                f'<div><p class="eyebrow">Next action</p><h2>Create the HubSpot quote for {_name(run)}.</h2>'
                '<p>Costs are in. Move from internal review to a quote the sales team can send and track.</p>'
                '<div class="operator-callout__meta">Resolution path: create quote -> send prospect follow-up.</div></div>'
                f'<div class="operator-callout__side"><a class="btn" href="/admin/fulfillment/sales/runs/{run_id}/review">Create quote</a></div>'
                '</section>'
            )

    for run in runs:
        run_id = int(run.get("id") or 0)
        stage = str(run.get("pipeline_stage") or "intake")
        view_count = int((engagement.get(run_id) or {}).get("external_sessions") or 0)
        if stage == "published" and view_count == 0:
            return (
                '<section class="operator-callout">'
                f'<div><p class="eyebrow">Next action</p><h2>Follow up on unopened sheet for {_name(run)}.</h2>'
                '<p>The offer is live but has no external view activity. Copy the outreach email or reopen the sheet before the prospect goes cold.</p>'
                '<div class="operator-callout__meta">Resolution path: resend link -> monitor views -> advance stage.</div></div>'
                f'<div class="operator-callout__side"><a class="btn" href="/admin/fulfillment/sales/runs/{run_id}/review">Open follow-up tools</a></div>'
                '</section>'
            )

    active = [r for r in runs if str(r.get("pipeline_stage") or "") not in {"won", "lost"}]
    if active:
        return (
            '<section class="operator-callout">'
            '<div><p class="eyebrow">Next action</p><h2>Pipeline is current. Work the next sales conversation.</h2>'
            '<p>No rate sheet is blocked by missing review, costs, or quote setup. Use the table only for stage updates or follow-up context.</p>'
            '<div class="operator-callout__meta">Resolution path: monitor views -> update stage -> close won or archive.</div></div>'
            '<div class="operator-callout__side"><a class="btn btn--ghost" href="#pipeline">Review pipeline</a></div>'
            '</section>'
        )
    return (
        '<section class="operator-callout">'
        '<div><p class="eyebrow">Next action</p><h2>No open fulfillment prospects.</h2>'
        '<p>Closed prospects are preserved for history. Start intake when the next fulfillment opportunity appears.</p></div>'
        '<div class="operator-callout__side"><a class="btn" href="#notes">Start intake</a></div>'
        '</section>'
    )


def _history_rows(runs: list[dict], engagement: dict[int, dict]) -> str:
    rows = []
    for run in runs:
        run_id = int(run.get("id") or 0)
        started_raw = str(run.get("started_at") or "")[:10]
        published_raw = str(run.get("published_at") or "")[:10]
        try:
            _date_src = published_raw if published_raw else started_raw
            _date_lbl = "sent" if published_raw else "created"
            _started_date = datetime.strptime(_date_src, "%Y-%m-%d")
            started = f"{_date_lbl} {_started_date.strftime('%b')} {_started_date.day}"
        except ValueError:
            started = started_raw
        prospect = _esc(run.get("prospect") or run.get("design_title") or f"Run {run_id}")
        status = str(run.get("status") or "")
        view_path = str(run.get("view_path") or "")
        cost_form_path = _cost_form_path(run_id, run)
        hs_quote_url = str(run.get("hubspot_quote_url") or "")
        published = bool(run.get("published")) and status == "completed"
        review_path = f"/admin/fulfillment/sales/runs/{run_id}/review"
        stage = str(run.get("pipeline_stage") or "intake")
        vol = run.get("monthly_order_volume")
        pitched = run.get("pitched_monthly")
        costs = run.get("fulfillment_actual_costs") or {}

        # Stale indicator (actionable follow-up cue)
        _stale_badge = ""
        try:
            _today = datetime.utcnow().date()
            _age_src = published_raw if published_raw else started_raw
            _age_days = (_today - datetime.strptime(_age_src, "%Y-%m-%d").date()).days if _age_src else 0
            _ext_views = int((engagement.get(run_id) or {}).get("external_sessions") or 0)
            if stage == "intake" and _age_days > 7 and published:
                # Sheet was published but stage wasn't advanced — one-click advance to pending_fulfillment
                _stale_badge = (
                    f'<div style="font-size:11px;color:#b45309;margin-top:3px;font-weight:500">'
                    f'⚠ Sent {_age_days}d ago &nbsp;'
                    f'<button type="button" onclick="event.stopPropagation();quickStage(this,\'pending_fulfillment\',{run_id})" '
                    f'style="font-size:10px;padding:1px 7px;border-radius:999px;border:1px solid #b45309;'
                    f'background:transparent;color:#b45309;cursor:pointer;font-weight:600">→ Mark as Sent</button>'
                    f'</div>'
                )
            elif stage == "published" and _ext_views == 0 and _age_days > 5:
                _stale_badge = (
                    f'<div style="font-size:11px;color:#b45309;margin-top:3px;font-weight:500">'
                    f'⚠ Unopened after {_age_days}d — follow up</div>'
                )
        except Exception:
            pass

        # Rates source pill (small, inside prospect cell)
        if status == "running":
            source_pill = '<span class="pill pill--running" style="font-size:10px">Generating…</span>'
        elif status == "failed":
            source_pill = '<span class="pill pill--failed" style="font-size:10px">Failed</span>'
        elif status == "draft":
            source_pill = '<span class="pill pill--draft" style="font-size:10px">Draft</span>'
        elif str(run.get("rates_source")) == RATE_SOURCE_WMS:
            source_pill = '<span class="pill pill--live" style="font-size:10px">Live</span>'
        else:
            source_pill = '<span class="pill pill--estimated" style="font-size:10px" title="Generated estimate; confirm costs with fulfillment">Estimated</span>'
        cost_form_quick = ""
        if cost_form_path:
            cost_form_quick = (
                f'<button class="inline-chip" type="button" title="Copy fulfillment-only cost form link" '
                f"onclick=\"event.stopPropagation();navigator.clipboard.writeText(window.location.origin + '{_esc(cost_form_path)}');"
                f"this.textContent='Cost form copied';setTimeout(()=>this.textContent='Cost form',1800);\">Cost form</button>"
            )

        # Engagement
        stats = engagement.get(run_id) or {}
        ext = int(stats.get("external_sessions") or 0)
        last_viewed = stats.get("last_viewed_at") or ""
        if ext and last_viewed:
            try:
                lv_date = datetime.fromisoformat(last_viewed[:10]).date()
                today = datetime.utcnow().date()
                days = (today - lv_date).days
                ago = "today" if days == 0 else ("yesterday" if days == 1 else f"{days}d ago")
                views_str = (
                    f'<span title="{ext} prospect session{"s" if ext != 1 else ""}, last {ago}">'
                    f'{ext}v <span class="muted" style="font-size:11px">{ago}</span></span>'
                )
            except Exception:
                views_str = f"{ext}v"
        elif ext:
            views_str = f"{ext}v"
        else:
            views_str = "—"

        # Margin + actual cost columns (single compute_margin call per row)
        actual_cell = '<span class="muted">—</span>'
        margin_cell = '<span class="muted">—</span>'
        _raw_margin: float = 0.0
        if costs and any(v for v in costs.values() if v):
            try:
                from sales_support_agent.services.fulfillment_deck.quote import compute_margin
                from sales_support_agent.services.fulfillment_deck.schema import ProspectProfile
                profile_obj = ProspectProfile.from_dict(run.get("prospect_profile") or {})
                mg = compute_margin(float(pitched or 0), costs, profile_obj, float(run.get("pass_through_monthly") or 0))
                actual_cell = _fmt_usd(mg["actual_monthly"])
                if pitched:
                    _raw_margin = float(mg["monthly_margin"])
                    sign_color = "#15803d" if _raw_margin >= 0 else "#b91c1c"
                    margin_cell = (
                        f'<span style="color:{sign_color};font-weight:700">'
                        f'{_fmt_usd(_raw_margin)}</span>'
                        f'<div class="muted">{mg["margin_pct"]}%</div>'
                    )
            except Exception:
                actual_cell = "—"

        actions = []
        if status == "running":
            pass  # just Delete below — page auto-refreshes
        elif status == "draft":
            actions.append(f'<a class="action-menu-item" href="{review_path}" target="_blank" rel="noreferrer">Review</a>')
            if cost_form_path:
                actions.append(
                    f'<button class="action-menu-item" type="button" '
                    f"onclick=\"navigator.clipboard.writeText(window.location.origin + '{_esc(cost_form_path)}');this.textContent='Cost form copied';\">Copy Cost Form</button>"
                )
        elif view_path and published:
            actions.append(f'<a class="action-menu-item" href="{_esc(view_path)}?viewer=internal" target="_blank" rel="noreferrer">Open</a>')
            actions.append(
                f'<button class="action-menu-item" type="button" '
                f"onclick=\"navigator.clipboard.writeText(window.location.origin + '{_esc(view_path)}');this.textContent='Copied';\">Share</button>"
            )
            if cost_form_path:
                actions.append(
                    f'<button class="action-menu-item" type="button" '
                    f"onclick=\"navigator.clipboard.writeText(window.location.origin + '{_esc(cost_form_path)}');this.textContent='Cost form copied';\">Copy Cost Form</button>"
                )
            if hs_quote_url:
                actions.append(f'<a class="action-menu-item action-menu-item--quote" href="{_esc(hs_quote_url)}" target="_blank" rel="noreferrer" title="Open e-signature quote in HubSpot">Open Quote</a>')
            else:
                actions.append(
                    f'<form method="post" action="/admin/fulfillment/sales/runs/{run_id}/quote" '
                    f'onclick="event.stopPropagation()">'
                    f'<button class="action-menu-item action-menu-item--quote" type="submit" title="Create HubSpot e-signature quote">Create Quote</button></form>'
                )
            actions.append(f'<a class="action-menu-item" href="{review_path}" target="_blank" rel="noreferrer">Edit</a>')
        actions.append(
            f'<form method="post" action="/admin/fulfillment/sales/runs/{run_id}/delete" '
            f'onclick="event.stopPropagation()" '
            f"onsubmit=\"return confirm('Delete this rate sheet? The public link will stop working.');\">"
            f'<button class="action-menu-item action-menu-item--danger" type="submit">Delete</button></form>'
        )
        action_menu = (
            f'<div class="action-menu" onclick="event.stopPropagation()">'
            f'<button class="action-menu-trigger" type="button" aria-label="Actions for {prospect}" '
            f'onclick="toggleActionMenu(this,event)">...</button>'
            f'<div class="action-menu-panel" role="menu">{"".join(actions)}</div>'
            f'</div>'
        )

        vol_str = f"{vol:,}" if vol else "—"
        notes_dot = (
            '<span title="Has internal notes" style="margin-left:4px;font-size:10px;'
            'opacity:0.5;vertical-align:middle">●</span>'
            if str(run.get("pipeline_notes") or "").strip() else ""
        )
        row_idx = len(rows)
        rows.append(
            f'<tr class="prospect-row" data-order="{row_idx}" data-stage="{_esc(stage)}" data-run="{run_id}" data-expand="expand-{run_id}" onclick="toggleExpand(event,\'expand-{run_id}\')">'
            f"<td><span class='row-chevron'>›</span><strong>{prospect}</strong>{notes_dot}{_stale_badge} {source_pill} {cost_form_quick}"
            f"<div class='muted'>{started}</div></td>"
            f"<td>{_stage_select(run_id, stage)}</td>"
            f"<td>{vol_str}</td>"
            f"<td>{_fmt_usd(pitched)}</td>"
            f"<td>{actual_cell}</td>"
            f'<td data-margin="{_raw_margin}">{margin_cell}</td>'
            f"<td>{views_str}</td>"
            f"<td><div class='row-actions'>{action_menu}</div></td></tr>"
            f'<tr class="expand-row" id="expand-{run_id}" style="display:none">'
            f'<td colspan="8">{_expand_panel(run)}</td></tr>'
        )
    return "".join(rows)


def render_fulfillment_sales_page(
    runs: list[dict],
    engagement: dict[int, dict],
    *,
    user: Optional[dict] = None,
    flash: str = "",
    flash_kind: str = "",
    intake_context: object | None = None,
) -> str:
    flash_html = (
        f'<div class="flash{" flash--warn" if flash_kind == "warn" else ""}">{_esc(flash)}</div>'
        if flash
        else ""
    )
    has_running = any(r.get("status") == "running" for r in runs)
    # Embed per-run margin data for live stats-bar update
    import json as _json
    _margin_seed: dict = {}
    for _r in runs:
        _costs = _r.get("fulfillment_actual_costs") or {}
        _pitched = _r.get("pitched_monthly")
        _stage = _r.get("pipeline_stage") or "intake"
        if _costs and _pitched and any(v for v in _costs.values() if v):
            try:
                from sales_support_agent.services.fulfillment_deck.quote import compute_margin
                from sales_support_agent.services.fulfillment_deck.schema import ProspectProfile
                _mg = compute_margin(
                    float(_pitched),
                    _costs,
                    ProspectProfile.from_dict(_r.get("prospect_profile") or {}),
                    float(_r.get("pass_through_monthly") or 0),
                )
                _margin_seed[str(_r["id"])] = {"m": _mg["monthly_margin"], "s": _stage}
            except Exception:
                pass
    _margin_json = _json.dumps(_margin_seed)
    _ctx = intake_context
    ctx_notes = _ctx.to_notes_block() if _ctx is not None and hasattr(_ctx, "to_notes_block") else ""
    ctx_deal_id = str(getattr(_ctx, "deal_id", "") or "") if _ctx is not None else ""
    ctx_company_id = str(getattr(_ctx, "company_id", "") or "") if _ctx is not None else ""
    ctx_contact_ids = ",".join(getattr(_ctx, "contact_ids", []) or []) if _ctx is not None else ""
    ctx_brand = str(getattr(_ctx, "company_name", "") or "") if _ctx is not None else ""
    ctx_website = str(getattr(_ctx, "website_url", "") or "") if _ctx is not None else ""
    ctx_banner = ""
    if ctx_deal_id:
        ctx_banner = (
            '<div class="flash" style="background:rgba(133,187,218,0.10);border-color:rgba(133,187,218,0.45)">'
            f'<strong>Creating from HubSpot deal:</strong> {_esc(getattr(_ctx, "deal_name", "") or ctx_deal_id)}'
            f' · {_esc(ctx_brand or "company unknown")}'
            '</div>'
        )
    table = (
        "<table><thead><tr>"
        "<th>Prospect</th><th>Stage</th><th>Vol/mo</th>"
        "<th>Pitched $/mo</th><th>Actual cost</th><th>Margin</th>"
        "<th>Views</th><th>Actions</th>"
        "</tr></thead>"
        f"<tbody>{_history_rows(runs, engagement)}</tbody></table>"
        if runs
        else '<p class="empty">No rate sheets generated yet — the first one will appear here with its shareable link.</p>'
    )
    next_action_html = _pipeline_next_action(runs, engagement)
    styles = _STYLES.replace("__NAV__", render_agent_nav_styles())
    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>agent | Fulfillment Pipeline</title>
    {render_agent_favicon_links()}
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Montserrat:wght@700;800&display=swap" rel="stylesheet">
    <link rel="stylesheet" href="/static/admin.css">
    <style>{styles}</style>
  </head>
  <body>
    {render_agent_nav("fulfillment", fulfillment_section="fulfillment_sales", user=user)}
    <main class="shell">
      <div class="workspace">
        <p class="eyebrow">Fulfillment — Pipeline</p>
        <h1>Prospect <span style="color:var(--light-blue)">Pipeline</span>.</h1>
        <p class="intro">Turn fulfillment opportunities into one next action: intake, review, collect costs, create quote, follow up, or close. The table below is supporting context, not the starting point.</p>
        {flash_html}
        {ctx_banner}
        {next_action_html}
        <form method="post" action="/admin/fulfillment/sales/generate" enctype="multipart/form-data">
          <input type="hidden" name="hubspot_deal_id" value="{_esc(ctx_deal_id)}">
          <input type="hidden" name="hubspot_company_id" value="{_esc(ctx_company_id)}">
          <input type="hidden" name="hubspot_contact_ids" value="{_esc(ctx_contact_ids)}">
          <div class="field">
            <label for="notes">Prospect notes <span class="hint">— free-form; anything goes (call notes, emails, product dims, volumes, current costs)</span></label>
            <textarea id="notes" name="notes" placeholder="e.g. Spoke with Sarah at GlowCo — they sell two SKUs: a serum (4 x 4 x 6 in, 1.2 lb) and a kit (10 x 8 x 4 in, 2.5 lb). ~3,000 orders/mo, mostly West Coast, paying about $9.80/parcel with UPS today.">{_esc(ctx_notes)}</textarea>
          </div>
          <div class="grid2">
            <div class="field">
              <label>Files <span class="hint">— optional CSV / XLSX / TXT, brand PDFs, or product images (specs, order exports, rate cards, line sheets)</span></label>
              <div class="drop"><input type="file" name="files" multiple accept=".csv,.xlsx,.xlsm,.txt,.md,.pdf,.png,.jpg,.jpeg,.webp"></div>
            </div>
            <div>
              <div class="field">
                <label for="website_url">Website <span class="hint">— optional</span></label>
                <input type="text" id="website_url" name="website_url" placeholder="prospect.com" value="{_esc(ctx_website)}">
              </div>
              <div class="field">
                <label for="brand">Brand name <span class="hint">— optional override</span></label>
                <input type="text" id="brand" name="brand" placeholder="Auto-detected from notes" list="existing-brands" autocomplete="off" value="{_esc(ctx_brand)}">
                <datalist id="existing-brands">{''.join(f'<option value="{_esc(r["prospect"])}" label="{_esc(r["prospect"])} ({r.get("pipeline_stage","intake").replace("_"," ")})"/>' for r in runs if r.get("prospect"))}</datalist>
              </div>
              <div class="field">
                <label for="origin_zip">Ship-from ZIP</label>
                <input type="text" id="origin_zip" name="origin_zip" value="{ANATA_HQ_ZIP}">
                <span class="hint">Anata HQ — {_esc(ANATA_HQ_ADDRESS)}</span>
              </div>
            </div>
          </div>
          <button class="btn" type="submit" id="generate-btn"
            onclick="setTimeout(()=>{{this.textContent='Building… (this takes ~20s)';this.disabled=true;}},10)">Create rate sheet</button>
          {'<a href="#pipeline" class="muted" style="margin-left:12px;font-size:13px;text-decoration:none;opacity:.65">↓ or jump to pipeline</a>' if runs else ''}
        </form>
        <h2 id="pipeline">Pipeline</h2>
        <p class="muted" style="margin:-6px 0 12px">Click a row to expand — enter fulfillment costs, track margin, update stage. Click again to close. Changes save automatically.</p>
        {_pipeline_stats(runs) if runs else ""}
        {f'''<div class="pipeline-toolbar" aria-label="Pipeline result controls">
          <div class="pipeline-toolbar__field"><label for="pipe-search">Search prospects</label><input id="pipe-search" type="search" placeholder="Prospect name" oninput="filterPipeline()"></div>
          <div class="pipeline-toolbar__field"><label for="pipe-stage">Stage</label><select id="pipe-stage" onchange="filterPipeline()"><option value="">All stages</option><option value="intake">Intake</option><option value="pending_fulfillment">Sent to Fulfillment</option><option value="costs_received">Costs Received</option><option value="published">Published</option><option value="won">Won</option><option value="lost">Lost</option></select></div>
          <div class="pipeline-toolbar__field"><label for="pipe-sort">Sort results</label><select id="pipe-sort" onchange="sortPipeline()"><option value="">Newest</option><option value="volume">Volume ↓</option><option value="pitched">Pitched $ ↓</option><option value="margin">Margin ↓</option><option value="views">Views ↓</option></select></div>
          <a href="/admin/fulfillment/sales/export.csv" class="btn btn--ghost" title="Download pipeline as CSV">Export CSV</a>
          <span id="pipe-count" class="pipeline-results-count" aria-live="polite">Showing {len(runs)} of {len(runs)} prospects</span>
        </div>''' if runs else ""}
        {f'<div class="pipeline-table-wrap">{table}</div>' if runs else table}
        {'<p class="pipeline-filter-empty" id="pipe-empty">No prospects match the current search and stage filters.</p>' if runs else ""}
      </div>
    </main>
    <script>
    // Per-run margin data for live stats-bar refresh
    var _marginData = {_margin_json};
    function refreshStatsBar() {{
      var terminal = new Set(['won','lost']);
      var total = 0, count = 0;
      for (var id in _marginData) {{
        var d = _marginData[id];
        var pRow = document.querySelector('tr.prospect-row[data-run="' + id + '"]');
        var stage = (pRow && pRow.dataset && pRow.dataset.stage) || d.s;
        if (!terminal.has(stage)) {{ total += (d.m || 0); count++; }}
      }}
      var el = document.getElementById('stat-margin');
      if (!el) return;
      var valEl = el.querySelector('.pipeline-stat__val');
      if (valEl && count > 0) {{
        var sign = total < 0 ? '−' : '';
        valEl.innerHTML = sign + '$' + Math.round(Math.abs(total)).toLocaleString('en-US') +
          '<span style="font-size:13px;font-weight:400">/mo</span>';
      }}
    }}
    // Generate form: loading state + duplicate brand warning.
    (function() {{
      var form = document.querySelector('form[action$="/generate"]');
      if (!form) return;
      var brandInput = form.querySelector('#brand');
      var existingBrands = [...document.querySelectorAll('#existing-brands option')].map(o => o.value.toLowerCase());
      if (brandInput && existingBrands.length) {{
        brandInput.addEventListener('input', function() {{
          var warn = document.getElementById('brand-dup-warn');
          if (this.value && existingBrands.includes(this.value.toLowerCase())) {{
            if (!warn) {{
              warn = document.createElement('span');
              warn.id = 'brand-dup-warn';
              warn.style.cssText = 'color:#b45309;font-size:12px;margin-left:6px';
              warn.textContent = '⚠ Rate sheet already exists — generating a new one will add to pipeline';
              this.parentNode.appendChild(warn);
            }}
          }} else if (warn) warn.remove();
        }});
      }}
      form.addEventListener('submit', function() {{
        var btn = form.querySelector('button[type="submit"]');
        if (btn) {{ btn.textContent = 'Generating… this takes ~30 sec'; btn.disabled = true; }}
      }});
    }})();
    function toggleExpand(e, id) {{
      if (e.target.closest('select,button,a,form,input,.action-menu')) return;
      var row = document.getElementById(id);
      if (!row) return;
      var open = row.style.display === 'none';
      row.style.display = open ? '' : 'none';
      var chev = row.previousElementSibling && row.previousElementSibling.querySelector('.row-chevron');
      if (chev) chev.style.transform = open ? 'rotate(90deg)' : '';
      if (open) setTimeout(() => row.scrollIntoView({{behavior: 'smooth', block: 'nearest'}}), 40);
    }}
    function toggleActionMenu(btn, event) {{
      event.stopPropagation();
      var menu = btn.closest('.action-menu');
      if (!menu) return;
      var willOpen = !menu.hasAttribute('data-open');
      document.querySelectorAll('.action-menu[data-open]').forEach(function(openMenu) {{
        if (openMenu !== menu) openMenu.removeAttribute('data-open');
      }});
      if (willOpen) menu.setAttribute('data-open', ''); else menu.removeAttribute('data-open');
    }}
    document.addEventListener('click', function() {{
      document.querySelectorAll('.action-menu[data-open]').forEach(function(menu) {{
        menu.removeAttribute('data-open');
      }});
    }});
    function quickStage(btn, stage, runId) {{
      var prospectRow = document.querySelector('tr.prospect-row[data-run="' + runId + '"]');
      var sel = prospectRow && prospectRow.querySelector('select');
      if (sel) {{ sel.value = stage; pipelineStage(sel, runId); }}
      var expandRow = document.getElementById('expand-' + runId);
      if (expandRow) {{
        expandRow.style.display = 'none';
        var chev = prospectRow && prospectRow.querySelector('.row-chevron');
        if (chev) chev.style.transform = '';
      }}
    }}
    function pipelineStage(sel, runId) {{
      sel.className = 'stage-select stage--' + sel.value;
      // Update left-border stage color immediately
      var pRow = document.querySelector('tr.prospect-row[data-run="' + runId + '"]');
      if (pRow) pRow.dataset.stage = sel.value;
      fetch('/admin/fulfillment/sales/runs/' + runId + '/stage', {{
        method: 'PATCH', headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify({{stage: sel.value}})
      }}).then(() => {{
        sel.style.outline = '2px solid #15803d';
        setTimeout(() => sel.style.outline = '', 1400);
        filterPipeline(); refreshStatsBar(); // re-apply filter and update active-margin stat
        // When advancing to "Sent to Fulfillment", auto-copy the brief so the rep can paste immediately.
        if (sel.value === 'pending_fulfillment') {{
          var expandRow = document.getElementById('expand-' + runId);
          var briefBtn = expandRow && expandRow.querySelector('button[data-brief]');
          if (briefBtn) {{
            navigator.clipboard.writeText(briefBtn.dataset.brief).catch(() => {{}});
            briefBtn.textContent = 'Brief copied! ✓';
            setTimeout(() => briefBtn.textContent = 'Copy brief', 2500);
          }}
        }}
      }});
    }}
    function pipelineCosts(btn, runId) {{
      function costVal(id) {{
        var el = document.getElementById(id + '-' + runId);
        if (!el) return null;
        var raw = String(el.value || '').trim();
        if (raw === '') return null;
        var parsed = parseFloat(raw);
        return Number.isFinite(parsed) ? parsed : null;
      }}
      var costs = {{
        pick_pack_per_order: costVal('pp'),
        pick_pack_additional_item: costVal('ppi'),
        storage_per_pallet_mo: costVal('st'),
        storage_cubic_foot_mo: costVal('scf'),
        receiving_precounted_box: costVal('rcb'),
        receiving_count_per_item: costVal('rci'),
        receiving_per_pallet: costVal('rc'),
        monthly_tech_fee: costVal('tf'),
        customer_service_monthly: costVal('cs'),
        pallet_order_per_pallet: costVal('po'),
        kitting_per_item: costVal('kit'),
        labeling_per_item: costVal('lab'),
        bagging_labeling_per_item: costVal('bag'),
        returns_units_mo: costVal('ru'),
        returns_receive_per_unit: costVal('rr'),
        returns_examination_per_unit: costVal('re'),
        returns_custom_steps_per_unit: costVal('rs'),
        special_project_hours_mo: costVal('sph'),
        special_projects_per_hour: costVal('spr'),
      }};
      btn.textContent = 'Saving…';
      fetch('/admin/fulfillment/sales/runs/' + runId + '/costs', {{
        method: 'PATCH', headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify(costs)
      }}).then(r => r.json()).then(data => {{
        btn.textContent = 'Saved ✓';
        var fmt = v => '$' + Math.abs(v).toLocaleString('en-US', {{maximumFractionDigits:0}});
        if (data.margin) {{
          var mg = data.margin;
          var sign = mg.monthly_margin >= 0 ? 'pos' : 'neg';
          // Update expand panel margin card
          var card = document.getElementById('margin-' + runId);
          if (card) {{
            var recLine = '';
            if (data.receiving_one_time && data.pallets_mo) {{
              recLine = '<div class="margin-line" style="opacity:0.65"><span>Receiving one-time (~' + data.pallets_mo + ' pallets)</span><span>−' + fmt(data.receiving_one_time) + '</span></div>';
            }}
            var optionalLine = mg.actual_optional_monthly ? '<div class="margin-line"><span>Optional/service actuals</span><span>−' + fmt(mg.actual_optional_monthly) + '</span></div>' : '';
            card.innerHTML =
              '<div class="big big--' + sign + '">' + (mg.monthly_margin < 0 ? '−' : '') + fmt(mg.monthly_margin) +
              '/mo (' + mg.margin_pct + '%)</div>' +
              '<div class="margin-line"><span>Customer monthly estimate</span><span>' + fmt(data.pitched) + '</span></div>' +
              '<div class="margin-line"><span>Carrier/pass-through revenue</span><span>−' + fmt(mg.pass_through_monthly || 0) + '</span></div>' +
              '<div class="margin-line"><span>Marginable revenue</span><span>' + fmt(mg.marginable_revenue || 0) + '</span></div>' +
              '<div class="margin-line"><span>Pick &amp; pack actual</span><span>−' + fmt(mg.actual_pick_pack) + '</span></div>' +
              '<div class="margin-line"><span>Storage actual</span><span>−' + fmt(mg.actual_storage) + '</span></div>' +
              '<div class="margin-line"><span>Tech fee actual</span><span>−' + fmt(mg.actual_tech_fee) + '</span></div>' +
              optionalLine +
              recLine +
              '<div class="margin-line" style="font-weight:700"><span>Annual margin</span><span>' + (mg.annual_margin < 0 ? '−' : '') + fmt(mg.annual_margin) + '</span></div>';
          }}
          // Update table row cells (actual cost + margin columns)
          var expandRow = document.getElementById('expand-' + runId);
          var prospectRow = expandRow ? expandRow.previousElementSibling : null;
          if (prospectRow) {{
            var tds = prospectRow.querySelectorAll('td');
            if (tds[4] && data.actual_monthly != null) tds[4].textContent = fmt(data.actual_monthly);
            if (tds[5]) {{
              var sc = mg.monthly_margin >= 0 ? '#15803d' : '#b91c1c';
              tds[5].innerHTML = '<span style="color:' + sc + ';font-weight:700">' +
                (mg.monthly_margin < 0 ? '−' : '') + fmt(mg.monthly_margin) + '</span>' +
                '<div class="muted">' + mg.margin_pct + '%</div>';
              tds[5].dataset.margin = String(mg.monthly_margin);
            }}
            // Auto-advance stage to Costs Received if still at an early stage
            var stageSelect = prospectRow.querySelector('select');
            if (stageSelect && (stageSelect.value === 'intake' || stageSelect.value === 'pending_fulfillment')) {{
              stageSelect.value = 'costs_received';
              pipelineStage(stageSelect, runId);
            }}
          }}
        }}
        // Update live stats bar with new margin
        if (data.margin) {{ _marginData[String(runId)] = {{m: data.margin.monthly_margin, s: 'costs_received'}}; refreshStatsBar(); }}
        setTimeout(() => btn.textContent = 'Save costs', 2000);
      }}).catch(() => {{ btn.textContent = 'Error — retry'; setTimeout(() => btn.textContent = 'Save costs', 3500); }});
    }}
    function sendBriefEmail(btn, runId) {{
      btn.textContent = 'Sending…'; btn.disabled = true;
      fetch('/admin/fulfillment/sales/runs/' + runId + '/send-brief', {{method: 'POST'}})
        .then(r => r.json()).then(d => {{
          btn.disabled = false;
          if (d.ok) {{
            btn.textContent = 'Sent ✓';
            setTimeout(() => {{ btn.textContent = 'Send to warehouse →'; }}, 3000);
          }} else {{
            btn.textContent = 'Send to warehouse →';
            var errMsg = (d.error || '').includes('FULFILLMENT_TEAM_EMAIL') ? 'Email not configured — set FULFILLMENT_TEAM_EMAIL in Render' : (d.error || 'Error sending');
            var errEl = document.getElementById('send-err-' + runId);
            if (errEl) {{ errEl.textContent = errMsg; errEl.style.display = 'block'; setTimeout(() => {{ errEl.style.display = 'none'; }}, 8000); }}
          }}
        }}).catch(() => {{ btn.textContent = 'Error — try again'; btn.disabled = false; setTimeout(() => {{ btn.textContent = 'Send to warehouse →'; }}, 3000); }});
    }}
    var _noteTimers = {{}};
    function pipelineNotesDebounce(el, runId) {{
      clearTimeout(_noteTimers[runId]);
      el.style.borderColor = '';
      _noteTimers[runId] = setTimeout(() => {{
        fetch('/admin/fulfillment/sales/runs/' + runId + '/notes', {{
          method: 'PATCH', headers: {{'Content-Type': 'application/json'}},
          body: JSON.stringify({{notes: el.value}})
        }}).then(() => {{
          el.style.borderColor = '#15803d';
          setTimeout(() => el.style.borderColor = '', 1400);
        }});
      }}, 900);
    }}
    {'if (true) { setTimeout(() => location.reload(), 8000); }' if has_running else ''}
    // Keyboard shortcuts
    document.addEventListener('keydown', function(e) {{
      // Escape: close any open expand panel
      if (e.key === 'Escape') {{
        document.querySelectorAll('.action-menu[data-open]').forEach(function(menu) {{
          menu.removeAttribute('data-open');
        }});
        document.querySelectorAll('tr.expand-row').forEach(function(row) {{
          if (row.style.display !== 'none') {{
            row.style.display = 'none';
            var chev = row.previousElementSibling && row.previousElementSibling.querySelector('.row-chevron');
            if (chev) chev.style.transform = '';
          }}
        }});
      }}
    }});
    // Ctrl/Cmd+Enter in the generate textarea submits the form
    (function() {{
      var ta = document.getElementById('notes');
      if (!ta) return;
      ta.addEventListener('keydown', function(e) {{
        if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {{
          var form = ta.closest('form');
          if (form) form.submit();
        }}
      }});
    }})();
    function filterPipeline() {{
      var q = (document.getElementById('pipe-search') || {{}}).value || '';
      var stageEl = document.getElementById('pipe-stage');
      var stage = (stageEl || {{}}).value || '';
      q = q.toLowerCase().trim();
      // Persist stage selection across page loads
      try {{ if (stage) localStorage.setItem('fsp_stage', stage); else localStorage.removeItem('fsp_stage'); }} catch(e) {{}}
      var tbody = document.querySelector('table tbody');
      if (!tbody) return;
      var rows = tbody.querySelectorAll('tr.prospect-row');
      var shown = 0;
      rows.forEach(function(row) {{
        var expRow = document.getElementById(row.getAttribute('data-expand') || '');
        var name = (row.querySelector('td strong') || {{}}).textContent || '';
        var stageVal = (row.querySelector('select') || {{}}).value || '';
        var show = (!q || name.toLowerCase().includes(q)) && (!stage || stageVal === stage);
        row.style.display = show ? '' : 'none';
        if (expRow) expRow.style.display = 'none'; // collapse on filter
        if (show) shown++;
      }});
      // Update count indicator
      var countEl = document.getElementById('pipe-count');
      if (countEl) {{
        var total = rows.length;
        countEl.textContent = 'Showing ' + shown + ' of ' + total + ' prospect' + (total !== 1 ? 's' : '');
      }}
      var emptyEl = document.getElementById('pipe-empty');
      if (emptyEl) emptyEl.style.display = shown ? 'none' : 'block';
    }}
    // Restore persisted stage filter on load
    (function() {{
      try {{
        var saved = localStorage.getItem('fsp_stage');
        if (saved) {{
          var stageEl = document.getElementById('pipe-stage');
          if (stageEl) {{ stageEl.value = saved; filterPipeline(); }}
        }}
      }} catch(e) {{}}
    }})();
    function sortPipeline() {{
      var key = (document.getElementById('pipe-sort') || {{}}).value || '';
      var tbody = document.querySelector('table tbody');
      if (!tbody) return;
      // Collect prospect+expand row pairs
      var allRows = [...tbody.querySelectorAll('tr')];
      var pairs = [];
      for (var i = 0; i < allRows.length; i += 2) {{
        if (allRows[i] && allRows[i+1]) pairs.push([allRows[i], allRows[i+1]]);
      }}
      if (!key) {{
        // Restore server-side order (by data-order attr added at render time)
        pairs.sort((a, b) => parseInt(a[0].dataset.order||0) - parseInt(b[0].dataset.order||0));
      }} else {{
        function getVal(row) {{
          var tds = row.querySelectorAll('td');
          var txt = function(i) {{ return ((tds[i] || {{}}).textContent || '').trim(); }};
          if (key === 'volume') return parseInt(txt(2).replace(/[^0-9]/g,'')) || 0;
          if (key === 'pitched') return parseFloat(txt(3).replace(/[^0-9.]/g,'')) || 0;
          if (key === 'margin') {{
            return parseFloat((tds[5] || {{}}).dataset.margin || 0) || 0;
          }}
          if (key === 'views') return parseInt(txt(6)) || 0;
          return 0;
        }}
        pairs.sort((a, b) => getVal(b[0]) - getVal(a[0]));
      }}
      pairs.forEach(function(pair, idx) {{
        pair[0].dataset.order = pair[0].dataset.order || idx;
        tbody.appendChild(pair[0]);
        pair[1].style.display = 'none';
        tbody.appendChild(pair[1]);
      }});
      // Re-apply filter after sort
      filterPipeline();
    }}
    </script>
  </body>
</html>"""


def _num_input(name: str, value: object, *, width: str = "76px", step: str = "any", placeholder: str = "") -> str:
    val = "" if value is None else f"{value:g}" if isinstance(value, float) else str(value)
    ph = f' placeholder="{_esc(placeholder)}"' if placeholder else ""
    return (
        f'<input type="number" name="{name}" value="{_esc(val)}" step="{step}" min="0"{ph} '
        f'style="width:{width};min-height:32px;padding:0 8px;border-radius:8px;'
        f'border:1px solid var(--border);font-size:13px">'
    )


def _product_row(index: int, product: dict, *, template: bool = False) -> str:
    name = str(product.get("name") or "")
    estimated = bool(product.get("dims_estimated"))
    est_tag = ' <span class="pill pill--estimated">estimated</span>' if estimated else ""
    remove_cell = (
        f'<input type="checkbox" name="product_remove" value="{index}" title="Remove this product">'
        if not template
        else ""
    )
    name_hint = ' placeholder="Add a product…"' if template else ""
    return (
        f"<tr>"
        f'<td><input type="text" name="product_name" value="{_esc(name)}"{name_hint} '
        f'style="width:100%;min-width:140px;min-height:32px;padding:0 8px;border-radius:8px;'
        f'border:1px solid var(--border);font-size:13px">{est_tag}'
        f'<input type="hidden" name="product_estimated" value="{1 if estimated else 0}"></td>'
        f"<td>{_num_input('product_length', product.get('length_in'))}</td>"
        f"<td>{_num_input('product_width', product.get('width_in'))}</td>"
        f"<td>{_num_input('product_height', product.get('height_in'))}</td>"
        f"<td>{_num_input('product_weight', product.get('weight_lb'))}</td>"
        f"<td>{_num_input('product_units', product.get('monthly_units'), width='90px', step='1', placeholder='—')}</td>"
        f"<td style='text-align:center'>{remove_cell}</td>"
        f"</tr>"
    )


def _assortment_hint(profile: dict) -> str:
    """Warehouse-approval vetting card: estimated SKU count + deterministic
    size variance, computed from the stored profile. Internal-only."""
    from sales_support_agent.services.fulfillment_deck.rendering import (
        assortment_profile,
    )
    from sales_support_agent.services.fulfillment_deck.schema import ProspectProfile

    info = assortment_profile(ProspectProfile.from_dict(profile))
    if not info["products_quoted"] and info["estimated_sku_count"] is None:
        return ""

    sku_count = info["estimated_sku_count"]
    sku_text = f"{sku_count:,}" if sku_count is not None else "not stated"
    basis = info["sku_count_basis"]
    bits = [
        f"<strong>Est. SKU count:</strong> {_esc(sku_text)}"
        + (f' <span class="muted">({_esc(basis)})</span>' if basis else ""),
        f"<strong>Products quoted:</strong> {info['products_quoted']}",
    ]
    if info["size_label"]:
        bits.append(f"<strong>Size range:</strong> {_esc(info['size_label'])}")
    if info["variance"]:
        bits.append(f"<strong>Size variance:</strong> {_esc(info['variance'])}")
    if info["any_fragile"]:
        bits.append('<strong>Fragile items:</strong> yes')
    items = "".join(f"<li>{b}</li>" for b in bits)
    return (
        '<div class="flash"><strong>Warehouse approval — assortment:</strong>'
        f'<ul style="margin:6px 0 0;padding-left:18px">{items}</ul>'
        '<p class="muted" style="margin:6px 0 0">Size figures computed from the '
        "product dims below — share with the warehouse team before publishing.</p>"
        "</div>"
    )


def _hubspot_deal_picker(current_deal_id: str) -> str:
    """Visible quote-readiness control for selecting a mirrored open deal."""
    from sales_support_agent.services.sales import hubspot_links

    try:
        from sqlalchemy import select
        from sqlalchemy.orm import Session

        from sales_support_agent.config import load_settings
        from sales_support_agent.models.database import get_engine
        from sales_support_agent.models.entities import HubSpotCompany, HubSpotDeal

        settings = load_settings()
        portal_id = settings.hubspot_portal_id or ""
        with Session(get_engine()) as session:
            rows = (
                session.execute(
                    select(HubSpotDeal)
                    .where(HubSpotDeal.is_closed.is_(False))
                    .order_by(HubSpotDeal.updated_at.desc().nullslast(), HubSpotDeal.deal_name.asc())
                    .limit(200)
                )
                .scalars()
                .all()
            )
            company_ids = {r.hubspot_company_id for r in rows if r.hubspot_company_id}
            companies = {}
            if company_ids:
                for co in session.execute(select(HubSpotCompany).where(HubSpotCompany.hubspot_company_id.in_(company_ids))).scalars():
                    companies[co.hubspot_company_id] = co.name
    except Exception:
        rows = []
        companies = {}
        portal_id = ""

    options = ['<option value="">Select open HubSpot deal...</option>']
    selected_label = ""
    for deal in rows:
        deal_id = str(deal.hubspot_deal_id or "")
        company = str(companies.get(deal.hubspot_company_id, "") or "")
        amount = f" · ${deal.amount_cents / 100:,.0f}" if int(deal.amount_cents or 0) else ""
        stage = str(deal.deal_stage_label or deal.deal_stage or "").strip()
        label = f"{deal.deal_name or deal_id}{(' · ' + company) if company else ''}{amount}{(' · ' + stage) if stage else ''}"
        if deal_id == current_deal_id:
            selected_label = label
        options.append(f'<option value="{_esc(deal_id)}" {"selected" if deal_id == current_deal_id else ""}>{_esc(label)}</option>')

    manual_hint = (
        '<span class="hint">No mirrored open deals loaded. Paste the HubSpot deal ID below, then save.</span>'
        if not rows else
        '<span class="hint">Choose the open HubSpot deal this rate sheet belongs to. Use manual ID only if the mirror is stale.</span>'
    )
    deal_url = hubspot_links.deal_url(portal_id, current_deal_id) if portal_id and current_deal_id else ""
    selected_html = (
        f'<div class="muted" style="margin-top:6px">Selected: {_esc(selected_label or current_deal_id)}'
        + (f' · <a href="{_esc(deal_url)}" target="_blank" rel="noreferrer">Open in HubSpot</a>' if deal_url else "")
        + "</div>"
        if current_deal_id else ""
    )
    return f"""
      <div class="field">
        <label for="hubspot_deal_id">HubSpot deal</label>
        <select id="hubspot_deal_id" name="hubspot_deal_id" style="min-height:40px;padding:0 12px;border-radius:10px;border:1px solid var(--border);font-size:14px;background:#fff">
          {''.join(options)}
        </select>
        {manual_hint}
        <input type="text" id="hubspot_deal_id_manual" name="hubspot_deal_id_manual" placeholder="Manual HubSpot deal ID" value="{_esc(current_deal_id if current_deal_id and not selected_label else '')}" style="margin-top:8px">
        {selected_html}
      </div>
    """


def _create_deal_href(run_id: int, summary: dict, profile: dict) -> str:
    prospect = str(summary.get("prospect") or summary.get("design_title") or profile.get("company") or "").strip()
    deal_name = f"{prospect} Fulfillment" if prospect else "Fulfillment Deal"
    company_id = str(summary.get("hubspot_company_id") or "").strip()
    company_name = str(profile.get("company") or profile.get("brand") or prospect).strip()
    company_domain = str(profile.get("website") or summary.get("company_domain") or "").strip()
    contact_ids = summary.get("hubspot_contact_ids") or []
    if isinstance(contact_ids, str):
        contact_id = contact_ids.split(",", 1)[0].strip()
    elif isinstance(contact_ids, list):
        contact_id = str(contact_ids[0] if contact_ids else "").strip()
    else:
        contact_id = ""
    params = {
        "dealname": deal_name,
        "anata_service_line": "fulfillment",
        "anata_lead_source_detail": "agent",
        "hubspot_company_id": company_id,
        "hubspot_contact_id": contact_id,
        "company_name": company_name,
        "company_domain": company_domain,
        "brand": str(profile.get("brand") or prospect).strip(),
        "contact_name": str(profile.get("contact_name") or "").strip(),
        "contact_email": str(profile.get("contact_email") or "").strip(),
        "return_to": f"/admin/fulfillment/sales/runs/{run_id}/review",
        "rate_sheet_run_id": str(run_id),
    }
    return "/admin/sales/deals/create?" + urlencode({k: v for k, v in params.items() if v})


def render_rate_sheet_review_page(
    run: dict,
    summary: dict,
    *,
    user: Optional[dict] = None,
    flash: str = "",
) -> str:
    """Review-before-publish page: live preview iframe + profile edit form."""
    run_id = int(run.get("id") or 0)
    status = str(run.get("status") or "")
    published = status == "completed"
    base = "/admin/fulfillment/sales"
    profile = dict(summary.get("prospect_profile") or {})
    products = [p for p in (profile.get("products") or []) if isinstance(p, dict)]

    flash_html = f'<div class="flash">{_esc(flash)}</div>' if flash else ""

    def _sanitize_warning(w: str) -> str:
        # Collapse raw LLM API exceptions into a one-liner — never show request IDs
        # or credit-balance details to the admin.
        if "LLM extraction failed" in w:
            suffix = "used basic text parsing instead" if "basic text" in w else "extraction fell back to basic parser"
            return f"Product details extracted with fallback parser ({suffix}) — review fields below."
        # Truncate any other internal exception messages
        return w[:200] + ("…" if len(w) > 200 else "")

    warnings = [_sanitize_warning(str(w)) for w in (summary.get("warnings") or []) if str(w).strip()]
    warnings_html = ""
    if warnings:
        items = "".join(f"<li>{_esc(w)}</li>" for w in warnings[:12])
        warn_label = "Published — notes:" if published else "Check before publishing:"
        warnings_html = (
            f'<div class="flash flash--warn"><strong>{warn_label}</strong>'
            f'<ul style="margin:6px 0 0;padding-left:18px">{items}</ul></div>'
        )

    view_path = str(summary.get("view_path") or "")
    cost_form_path = _cost_form_path(run_id, summary)
    hs_deal_id = str(summary.get("hubspot_deal_id") or "").strip()
    hs_deal_url = str(summary.get("hubspot_deal_url") or "").strip()
    hs_quote_url = str(summary.get("hubspot_quote_url") or "")
    quote_guard_errors = validate_quote_readiness(summary, published=published)
    quote_guard_html = ""
    if quote_guard_errors:
        quote_guard_html = (
            '<div class="flash flash--warn"><strong>Quote blocked until:</strong><ul style="margin:6px 0 0;padding-left:18px">'
            + "".join(f"<li>{_esc(item)}</li>" for item in quote_guard_errors)
            + "</ul></div>"
        )
    hs_quote_btn = (
        f'<a class="btn" href="{_esc(hs_quote_url)}" target="_blank" rel="noreferrer" '
        f'style="background:#ff7a59;border-color:#ff7a59;color:#fff">Open HubSpot Quote ✍</a>'
        if hs_quote_url else ""
    )
    hs_create_quote_btn = (
        f'<form method="post" action="{base}/runs/{run_id}/quote" style="display:inline">'
        f'<button class="btn" type="submit" style="background:#ff7a59;border-color:#ff7a59;color:#fff">Create HubSpot Quote ✍</button></form>'
        if not quote_guard_errors else
        '<button class="btn" type="button" disabled style="background:#d4d4d4;border-color:#d4d4d4;color:#666;cursor:not-allowed">Create HubSpot Quote ✍</button>'
    )
    hs_deal_chip = ""
    if hs_deal_url:
        hs_deal_chip = f'<a class="btn btn--ghost" href="{_esc(hs_deal_url)}" target="_blank" rel="noreferrer">Open HubSpot Deal</a>'
    elif hs_deal_id:
        hs_deal_chip = f'<span class="pill pill--live">HubSpot deal {_esc(hs_deal_id)}</span>'
    prospect_name = str(summary.get("prospect") or summary.get("design_title") or "your brand")
    cost_form_block = ""
    if cost_form_path:
        cost_form_block = f"""
        <div class="flash" style="background:rgba(43,54,68,0.035);border-color:rgba(43,54,68,0.12)">
          <strong>Fulfillment cost form.</strong>
          <span class="muted">Share this with fulfillment for internal cost input only. It does not show sales pricing, customer pitch, margin, or quote details.</span>
          <div style="display:flex;gap:8px;flex-wrap:wrap;margin-top:10px">
            <button class="btn btn--ghost" type="button"
              onclick="navigator.clipboard.writeText(window.location.origin+'{_esc(cost_form_path)}');this.textContent='Cost form copied!';setTimeout(()=>this.textContent='Copy cost form link',2000)">Copy cost form link</button>
            <a class="btn btn--ghost" href="{_esc(cost_form_path)}" target="_blank" rel="noreferrer">Open cost form</a>
          </div>
        </div>"""
    if published and view_path:
        _full_link_js = f"window.location.origin+'{_esc(view_path)}'"
        _subj_attr = html.escape(f"Anata 3PL — {prospect_name} Fulfillment Rate Sheet", quote=True)
        # Build the email body as a JS expression so the link is injected client-side.
        # Using string concatenation so no template literal escaping needed inside onclick.
        _copy_email_js = html.escape(
            "var l=window.location.origin+'" + _esc(view_path) + "';"
            "var s='Subject: Anata 3PL \\u2014 " + prospect_name.replace("'", "\\'") + " Fulfillment Rate Sheet';"
            "var b='Hi,\\n\\nI wanted to share a customized fulfillment rate sheet from Anata "
            "\\u2014 it includes pricing tailored to your order volume and product specs."
            "\\n\\nView your rate sheet here: '+l+'\\n\\nHappy to walk through it on a quick call "
            "whenever works for you.\\n\\nBest,';"
            "navigator.clipboard.writeText(s+'\\n\\n'+b);"
            "this.textContent='Email copied!';setTimeout(()=>this.textContent='Copy email',2000);",
            quote=True,
        )
        publish_block = f"""
        <div class="flash">
          <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-bottom:8px">
            <strong>Published.</strong>
            <code style="font-size:12px;flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{_esc(view_path)}</code>
          </div>
          <div style="display:flex;gap:8px;flex-wrap:wrap">
            <button class="btn btn--ghost" type="button"
              onclick="navigator.clipboard.writeText({_full_link_js});this.textContent='Copied!';">Copy link</button>
            <button class="btn btn--ghost" type="button"
              onclick="{_copy_email_js}">Copy email</button>
            <a class="btn btn--ghost" href="{_esc(view_path)}?viewer=internal" target="_blank" rel="noreferrer">Open</a>
            {hs_deal_chip}
            {hs_quote_btn if hs_quote_url else hs_create_quote_btn}
          </div>
          <p class="muted" style="margin:10px 0 0">Save &amp; re-render updates the agent preview and this public URL. Re-publish is the explicit action for refreshing the live shared sheet and HubSpot quote workflow.</p>
        </div>"""
        publish_button = '<button class="btn" type="submit">Re-publish live sheet</button>'
    else:
        publish_block = ""
        publish_button = '<button class="btn" type="submit">Publish — get shareable link</button>'
    publish_form_html = (
        f"""
        <form method="post" action="{base}/runs/{run_id}/publish" style="margin-top:10px">
          <div class="review-actions">
            {publish_button}
            <a class="btn btn--ghost" href="{base}">← Pipeline</a>
            <span class="muted" style="font-size:12px">Re-publish refreshes the prospect-facing rate sheet and can re-run the HubSpot quote workflow when quote guards pass.</span>
          </div>
        </form>"""
        if published else
        f"""
        <div class="review-actions" style="margin-top:10px">
          <a class="btn btn--ghost" href="{base}">← Pipeline</a>
        </div>"""
    )

    rows = "".join(_product_row(i, p) for i, p in enumerate(products))
    rows += _product_row(len(products), {}, template=True)

    # v7: warehouse-approval vetting hints — estimated SKU count + size
    # variance computed deterministically from the products. Warehouse sign-off
    # happens here, before publish.
    assortment_html = _assortment_hint(profile)

    # Fulfillment brief — shown on review page so the rep can send it to the
    # warehouse team right away, before or after publishing the rate sheet.
    review_brief_run = {
        "id": run_id,
        "prospect": summary.get("prospect") or summary.get("design_title"),
        "origin_zip": summary.get("origin_zip"),
        "monthly_order_volume": profile.get("monthly_order_volume"),
        "prospect_profile": profile,
    }
    review_brief_text = _build_brief(review_brief_run)
    review_brief_attr = html.escape(review_brief_text, quote=True)
    brief_block = f"""
    <div class="flash" style="background:rgba(133,187,218,0.08);border-color:rgba(133,187,218,0.4);margin-bottom:16px">
      <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:12px;margin-bottom:8px">
        <strong>Fulfillment Brief</strong>
        <button class="btn btn--ghost" type="button" style="flex-shrink:0"
          data-brief="{review_brief_attr}"
          onclick="navigator.clipboard.writeText(this.dataset.brief);this.textContent='Copied!';setTimeout(()=>this.textContent='Copy brief',2000)">Copy brief</button>
      </div>
      <pre style="margin:0;font-family:inherit;font-size:12.5px;white-space:pre-wrap;color:rgba(43,54,68,0.75);line-height:1.55">{_esc(review_brief_text)}</pre>
      <p class="muted" style="margin:8px 0 0;font-size:12px">Share with the warehouse team to get a cost quote — paste into email or Slack.</p>
    </div>"""

    monthly_volume = profile.get("monthly_order_volume")
    current_cost = profile.get("current_cost_per_parcel_usd")
    volume_basis = str(profile.get("volume_basis") or "").strip()
    volume_provenance = str(profile.get("volume_provenance") or "").strip()
    # Vetting hint: the arithmetic (basis) AND where the number came from
    # (provenance) — the public sheet only ever shows the basis.
    hint_parts = []
    if volume_basis:
        hint_parts.append(f"Basis: {_esc(volume_basis)}")
    if volume_provenance:
        hint_parts.append(f"Source: {_esc(volume_provenance)}")
    volume_basis_hint = (
        f'<span class="hint">{" · ".join(hint_parts)}</span>' if hint_parts else ""
    )
    margin_override = summary.get("quote_margin_override")
    margin_value = "" if margin_override is None else f"{margin_override:g}"
    sales_pricing = dict(summary.get("sales_pricing") or {})
    hubspot_deal_id = str(summary.get("hubspot_deal_id") or "").strip()
    deal_picker_html = _hubspot_deal_picker(hubspot_deal_id)
    create_deal_href = _esc(_create_deal_href(int(run_id), summary, profile))
    fee_rows = merge_fee_rows(sales_pricing.get("fee_rows") or summary.get("pricing_fee_rows") or [])
    waived_keys = {str(r.get("fee_key") or "") for r in fee_rows if r.get("waived")}
    waiver_reason = _esc(str(sales_pricing.get("waiver_reason") or ""))
    pricing_reviewed = bool(sales_pricing.get("reviewed"))
    margin_approved = bool(sales_pricing.get("margin_approved"))
    fee_checks = "".join(
        '<label style="display:flex;gap:8px;align-items:flex-start;font-size:12.5px;font-weight:600;margin:6px 0">'
        f'<input type="checkbox" name="fee_waived" value="{_esc(str(row.get("fee_key") or ""))}" {"checked" if str(row.get("fee_key") or "") in waived_keys else ""} style="width:auto;margin-top:2px">'
        f'<span>{_esc(row.get("label") or row.get("fee_key"))} <span class="muted">({_esc(row.get("unit") or "")})</span></span>'
        '</label>'
        for row in fee_rows
    )

    from sales_support_agent.services.fulfillment_deck.quote import (
        BASELINE_RATES,
        INTERNAL_COST_BASELINES,
    )
    _ro = dict(summary.get("rate_overrides") or {})
    _actual_costs = dict(summary.get("fulfillment_actual_costs") or {})
    signed_cost_submissions = [
        s for s in (summary.get("fulfillment_cost_submissions") or [])
        if isinstance(s, dict) and str(s.get("name") or "").strip() and str(s.get("email") or "").strip()
    ]
    latest_cost_submission = signed_cost_submissions[-1] if signed_cost_submissions else {}
    section_deal_status = "Deal attached" if hubspot_deal_id else "Select or create deal"
    section_pricing_status = "Reviewed" if pricing_reviewed else "Needs review"
    section_cost_status = "Signed by fulfillment" if signed_cost_submissions else "Needs signed costs"
    section_waiver_status = f"{len(waived_keys)} waived" if waived_keys else "No waivers"
    section_product_status = f"{len(products)} product{'s' if len(products) != 1 else ''}"
    pricing_summary_html = _pricing_summary_html(summary, profile)

    def _rval(key: str) -> str:
        v = _ro.get(key)
        return f"{v:g}" if v is not None else ""

    def _rate_hint(key: str, *, source: str = "Agreement default") -> str:
        value = BASELINE_RATES.get(key)
        if value in (None, ""):
            return ""
        try:
            shown = _fmt_rate(float(value))
        except (TypeError, ValueError):
            shown = str(value)
        return f"{source}: {shown}. Blank uses this before any margin override; entered values are final."

    def _cost_hint(*keys: str, label: str = "Fulfillment cost") -> str:
        vals = []
        for key in keys:
            value = _actual_costs.get(key)
            if value not in (None, ""):
                try:
                    vals.append(float(value))
                except (TypeError, ValueError):
                    pass
        if not vals:
            return ""
        shown = sum(vals) if len(vals) > 1 else vals[0]
        return f'<span class="hint">{_esc(label)}: {_fmt_rate(shown)}. Final customer price should cover this plus sales margin.</span>'

    def _aval(key: str, default_key: str | None = None) -> str:
        v = _actual_costs.get(key)
        if v is None and default_key:
            v = INTERNAL_COST_BASELINES.get(default_key)
        return f"{float(v):g}" if v is not None else ""

    def _internal_hint(key: str) -> str:
        value = INTERNAL_COST_BASELINES.get(key)
        if value in (None, ""):
            return ""
        return f"Fulfillment baseline: {_fmt_rate(float(value))}."

    def _actual_or_baseline(cost_key: str | None, default_key: str | None = None) -> float | None:
        if cost_key:
            value = _actual_costs.get(cost_key)
            if value not in (None, ""):
                try:
                    return float(value)
                except (TypeError, ValueError):
                    pass
        if default_key and default_key in INTERNAL_COST_BASELINES:
            try:
                return float(INTERNAL_COST_BASELINES[default_key])
            except (TypeError, ValueError):
                return None
        return None

    def _sum_costs(*pairs: tuple[str, str]) -> float:
        total = 0.0
        for cost_key, default_key in pairs:
            value = _actual_or_baseline(cost_key, default_key)
            if value is not None:
                total += value
        return total

    def _suggested_cell(
        key: str | None,
        cost_key: str | None = None,
        default_key: str | None = None,
        *,
        internal_cost_value: float | None = None,
    ) -> str:
        if not key:
            return _empty_cell("No customer price suggestion")
        agreement = BASELINE_RATES.get(key)
        try:
            agreement_value = float(agreement) if agreement not in (None, "") else None
        except (TypeError, ValueError):
            agreement_value = None
        suggested = suggest_customer_price(
            key,
            internal_cost=internal_cost_value if internal_cost_value is not None else _actual_or_baseline(cost_key, default_key or key),
            agreement_default=agreement_value,
            margin_override_pct=margin_override,
        )
        price = suggested.get("price")
        if price is None:
            return _empty_cell("No suggestion yet")
        return (
            '<div class="pricing-suggestion">'
            f'<strong>{_fmt_rate(price)}</strong>'
            f'<span>{_esc(suggested.get("rationale") or "")}</span>'
            '</div>'
        )

    def _number_cell(
        label: str,
        input_id: str,
        name: str,
        value: str,
        placeholder: object,
        *,
        step: str = "0.01",
        hint: str = "",
    ) -> str:
        try:
            placeholder_str = f"{float(placeholder):g}"
        except (TypeError, ValueError):
            placeholder_str = str(placeholder or "")
        hint_html = f'<span class="hint">{_esc(hint)}</span>' if hint else ""
        return (
            '<div class="pricing-cell">'
            f'<label for="{_esc(input_id)}">{_esc(label)}</label>'
            f'<input type="number" id="{_esc(input_id)}" name="{_esc(name)}" step="{_esc(step)}" min="0" '
            f'value="{_esc(value)}" placeholder="{_esc(placeholder_str)}">'
            f'{hint_html}'
            '</div>'
        )

    def _empty_cell(text: str = "No mapped field yet") -> str:
        return f'<div class="pricing-cell--empty">{_esc(text)}</div>'

    def _pricing_line(label: str, sub: str, cost: str, suggested: str, fee: str) -> str:
        return (
            "<tr>"
            f'<td><div class="pricing-lines__label">{_esc(label)}</div>'
            f'<div class="pricing-lines__sub">{_esc(sub)}</div></td>'
            f"<td>{cost}</td>"
            f"<td>{suggested}</td>"
            f"<td>{fee}</td>"
            "</tr>"
        )

    pricing_lines = [
        _pricing_line(
            "DTC pick & pack / order",
            "Monthly order handling. Feeds the public estimate and MRR.",
            _number_cell("Fulfillment cost", "actual_pick_pack_per_order", "actual_pick_pack_per_order", _aval("pick_pack_per_order", "dtc_base_per_order"), INTERNAL_COST_BASELINES["dtc_base_per_order"], hint=_internal_hint("dtc_base_per_order")),
            _suggested_cell("dtc_base_per_order", "pick_pack_per_order", "dtc_base_per_order"),
            _number_cell("Final customer price", "rate_pick_pack", "rate_pick_pack", _rval("dtc_base_per_order"), BASELINE_RATES["dtc_base_per_order"], hint=_rate_hint("dtc_base_per_order")),
        ),
        _pricing_line(
            "DTC additional item",
            "Extra item in an order. Uses average items per order.",
            _number_cell("Fulfillment cost", "actual_pick_pack_additional_item", "actual_pick_pack_additional_item", _aval("pick_pack_additional_item", "dtc_additional_item"), INTERNAL_COST_BASELINES["dtc_additional_item"], hint=_internal_hint("dtc_additional_item")),
            _suggested_cell("dtc_additional_item", "pick_pack_additional_item", "dtc_additional_item"),
            _number_cell("Final customer price", "rate_additional_item", "rate_additional_item", _rval("dtc_additional_item"), BASELINE_RATES["dtc_additional_item"], hint=_rate_hint("dtc_additional_item")),
        ),
        _pricing_line(
            "Receiving / pallet",
            "Agreement standard pallet receiving. One-time receiving support.",
            _number_cell("Fulfillment cost", "actual_receiving_per_pallet", "actual_receiving_per_pallet", _aval("receiving_per_pallet", "receiving_per_pallet"), INTERNAL_COST_BASELINES["receiving_per_pallet"], hint=_internal_hint("receiving_per_pallet")),
            _suggested_cell("receiving_per_pallet", "receiving_per_pallet", "receiving_per_pallet"),
            _number_cell("Final customer price", "rate_receiving", "rate_receiving", _rval("receiving_per_pallet"), BASELINE_RATES["receiving_per_pallet"], hint=_rate_hint("receiving_per_pallet")),
        ),
        _pricing_line(
            "Receiving pre-counted box",
            "Fulfillment manager baseline. Optional/custom receiving charge.",
            _number_cell("Fulfillment cost", "actual_receiving_precounted_box", "actual_receiving_precounted_box", _aval("receiving_precounted_box", "receiving_precounted_box"), INTERNAL_COST_BASELINES["receiving_precounted_box"], hint=_internal_hint("receiving_precounted_box")),
            _suggested_cell("receiving_precounted_box", "receiving_precounted_box", "receiving_precounted_box"),
            _number_cell("Final customer price", "rate_receiving_precounted_box", "rate_receiving_precounted_box", _rval("receiving_precounted_box"), BASELINE_RATES["receiving_precounted_box"], hint=_rate_hint("receiving_precounted_box", source="Chargeable default")),
        ),
        _pricing_line(
            "Receiving counted item",
            "Fulfillment counts units",
            _number_cell("Fulfillment cost", "actual_receiving_count_per_item", "actual_receiving_count_per_item", _aval("receiving_count_per_item", "receiving_count_per_item"), INTERNAL_COST_BASELINES["receiving_count_per_item"], hint=_internal_hint("receiving_count_per_item")),
            _suggested_cell("receiving_count_per_item", "receiving_count_per_item", "receiving_count_per_item"),
            _number_cell("Final customer price", "rate_receiving_count_per_item", "rate_receiving_count_per_item", _rval("receiving_count_per_item"), BASELINE_RATES["receiving_count_per_item"], hint=_rate_hint("receiving_count_per_item", source="Chargeable default")),
        ),
        _pricing_line(
            "Storage / pallet/mo",
            "Recurring storage. Uses the higher storage cost basis for margin.",
            _number_cell("Fulfillment cost", "actual_storage_per_pallet_mo", "actual_storage_per_pallet_mo", _aval("storage_per_pallet_mo", "storage_short_per_pallet_mo"), INTERNAL_COST_BASELINES["storage_short_per_pallet_mo"], hint=_internal_hint("storage_short_per_pallet_mo")),
            _suggested_cell("storage_short_per_pallet_mo", "storage_per_pallet_mo", "storage_short_per_pallet_mo"),
            _number_cell("Final customer price", "rate_storage", "rate_storage", _rval("storage_short_per_pallet_mo"), BASELINE_RATES["storage_short_per_pallet_mo"], hint=_rate_hint("storage_short_per_pallet_mo")),
        ),
        _pricing_line(
            "Storage / cubic foot/mo",
            "Recurring storage alternative. Useful when pallet pricing distorts usage.",
            _number_cell("Fulfillment cost", "actual_storage_cubic_foot_mo", "actual_storage_cubic_foot_mo", _aval("storage_cubic_foot_mo", "storage_cubic_foot_mo"), INTERNAL_COST_BASELINES["storage_cubic_foot_mo"], hint=_internal_hint("storage_cubic_foot_mo")),
            _suggested_cell("storage_cubic_foot_mo", "storage_cubic_foot_mo", "storage_cubic_foot_mo"),
            _number_cell("Final customer price", "rate_storage_cubic_foot", "rate_storage_cubic_foot", _rval("storage_cubic_foot_mo"), BASELINE_RATES["storage_cubic_foot_mo"], hint=_rate_hint("storage_cubic_foot_mo", source="Chargeable default")),
        ),
        _pricing_line(
            "Kitting / item",
            "Optional fulfillment service",
            _number_cell("Fulfillment cost", "actual_kitting_per_item", "actual_kitting_per_item", _aval("kitting_per_item", "kitting_per_unit"), INTERNAL_COST_BASELINES["kitting_per_unit"], hint=_internal_hint("kitting_per_unit")),
            _suggested_cell("kitting_per_unit", "kitting_per_item", "kitting_per_unit"),
            _number_cell("Final customer price", "rate_kitting", "rate_kitting", _rval("kitting_per_unit"), BASELINE_RATES["kitting_per_unit"], hint=_rate_hint("kitting_per_unit")),
        ),
        _pricing_line(
            "Labeling / item",
            "Optional fulfillment service",
            _number_cell("Fulfillment cost", "actual_labeling_per_item", "actual_labeling_per_item", _aval("labeling_per_item", "labeling_per_unit"), INTERNAL_COST_BASELINES["labeling_per_unit"], hint=_internal_hint("labeling_per_unit")),
            _suggested_cell("labeling_per_unit", "labeling_per_item", "labeling_per_unit"),
            _number_cell("Final customer price", "rate_labeling", "rate_labeling", _rval("labeling_per_unit"), BASELINE_RATES["labeling_per_unit"], hint=_rate_hint("labeling_per_unit")),
        ),
        _pricing_line(
            "Bagging + labeling / item",
            "Optional fulfillment service",
            _number_cell("Fulfillment cost", "actual_bagging_labeling_per_item", "actual_bagging_labeling_per_item", _aval("bagging_labeling_per_item", "bagging_labeling_per_unit"), INTERNAL_COST_BASELINES["bagging_labeling_per_unit"], hint=_internal_hint("bagging_labeling_per_unit")),
            _suggested_cell("bagging_labeling_per_unit", "bagging_labeling_per_item", "bagging_labeling_per_unit"),
            _number_cell("Final customer price", "rate_bagging_labeling", "rate_bagging_labeling", _rval("bagging_labeling_per_unit"), BASELINE_RATES["bagging_labeling_per_unit"], hint=_rate_hint("bagging_labeling_per_unit", source="Chargeable default")),
        ),
        _pricing_line(
            "Wholesale / unit",
            "Customer-facing wholesale fee",
            _empty_cell("No direct fulfillment cost field"),
            _suggested_cell("wholesale_per_unit", None, None),
            _number_cell("Final customer price", "rate_wholesale", "rate_wholesale", _rval("wholesale_per_unit"), BASELINE_RATES["wholesale_per_unit"], hint=_rate_hint("wholesale_per_unit")),
        ),
        _pricing_line(
            "Pallet orders / pallet",
            "Wholesale pallet handling cost",
            _number_cell("Fulfillment cost", "actual_pallet_order_per_pallet", "actual_pallet_order_per_pallet", _aval("pallet_order_per_pallet", "pallet_order_per_pallet"), INTERNAL_COST_BASELINES["pallet_order_per_pallet"], hint=_internal_hint("pallet_order_per_pallet")),
            _suggested_cell("pallet_order_per_pallet", "pallet_order_per_pallet", "pallet_order_per_pallet"),
            _number_cell("Final customer price", "rate_pallet_order", "rate_pallet_order", _rval("pallet_order_per_pallet"), BASELINE_RATES["pallet_order_per_pallet"], hint=_rate_hint("pallet_order_per_pallet")),
        ),
        _pricing_line(
            "Returns / unit",
            "Customer-facing return fee",
            _empty_cell("See return cost stack below"),
            _suggested_cell(
                "returns_per_unit",
                internal_cost_value=_sum_costs(
                    ("returns_receive_per_unit", "returns_receive_per_unit"),
                    ("returns_examination_per_unit", "returns_examination_per_unit"),
                    ("returns_custom_steps_per_unit", "returns_custom_steps_per_unit"),
                ),
            ),
            _number_cell("Final customer price", "rate_returns", "rate_returns", _rval("returns_per_unit"), BASELINE_RATES["returns_per_unit"], hint=_rate_hint("returns_per_unit")),
        ),
        _pricing_line(
            "Returns units / month",
            "Volume used for return costs",
            _number_cell("Monthly units", "actual_returns_units_mo", "actual_returns_units_mo", _aval("returns_units_mo"), 0, step="1"),
            _empty_cell("Volume input only"),
            _empty_cell("Not a customer fee"),
        ),
        _pricing_line(
            "Return receive / unit",
            "Cost stack",
            _number_cell("Fulfillment cost", "actual_returns_receive_per_unit", "actual_returns_receive_per_unit", _aval("returns_receive_per_unit", "returns_receive_per_unit"), INTERNAL_COST_BASELINES["returns_receive_per_unit"], hint=_internal_hint("returns_receive_per_unit")),
            _empty_cell("Covered by returns processing"),
            _empty_cell("Covered by returns fee"),
        ),
        _pricing_line(
            "Return examination / unit",
            "Cost stack",
            _number_cell("Fulfillment cost", "actual_returns_examination_per_unit", "actual_returns_examination_per_unit", _aval("returns_examination_per_unit", "returns_examination_per_unit"), INTERNAL_COST_BASELINES["returns_examination_per_unit"], hint=_internal_hint("returns_examination_per_unit")),
            _empty_cell("Covered by returns processing"),
            _empty_cell("Covered by returns fee"),
        ),
        _pricing_line(
            "Return custom steps / unit",
            "Cost stack",
            _number_cell("Fulfillment cost", "actual_returns_custom_steps_per_unit", "actual_returns_custom_steps_per_unit", _aval("returns_custom_steps_per_unit", "returns_custom_steps_per_unit"), INTERNAL_COST_BASELINES["returns_custom_steps_per_unit"], hint=_internal_hint("returns_custom_steps_per_unit")),
            _empty_cell("Covered by returns processing"),
            _empty_cell("Covered by returns fee"),
        ),
        _pricing_line(
            "Monthly tech fee",
            "Recurring platform/admin fee",
            _number_cell("Fulfillment cost", "actual_monthly_tech_fee", "actual_monthly_tech_fee", _aval("monthly_tech_fee", "monthly_tech_fee"), INTERNAL_COST_BASELINES["monthly_tech_fee"], hint=_internal_hint("monthly_tech_fee")),
            _suggested_cell("monthly_tech_fee", "monthly_tech_fee", "monthly_tech_fee"),
            _number_cell("Final customer price", "rate_tech_fee", "rate_tech_fee", _rval("monthly_tech_fee"), BASELINE_RATES["monthly_tech_fee"], hint=_rate_hint("monthly_tech_fee")),
        ),
        _pricing_line(
            "Customer service / month",
            "Fulfillment team relationship support",
            _number_cell("Fulfillment cost", "actual_customer_service_monthly", "actual_customer_service_monthly", _aval("customer_service_monthly", "customer_service_monthly"), INTERNAL_COST_BASELINES["customer_service_monthly"], hint=_internal_hint("customer_service_monthly")),
            _suggested_cell("customer_service_monthly", "customer_service_monthly", "customer_service_monthly"),
            _number_cell("Final customer price", "rate_customer_service_monthly", "rate_customer_service_monthly", _rval("customer_service_monthly"), BASELINE_RATES["customer_service_monthly"], hint=_rate_hint("customer_service_monthly")),
        ),
        _pricing_line(
            "Special projects / hour",
            "Hourly project work",
            _number_cell("Fulfillment cost", "actual_special_projects_per_hour", "actual_special_projects_per_hour", _aval("special_projects_per_hour", "special_projects_per_hour"), INTERNAL_COST_BASELINES["special_projects_per_hour"], hint=_internal_hint("special_projects_per_hour")),
            _suggested_cell("special_projects_per_hour", "special_projects_per_hour", "special_projects_per_hour"),
            _number_cell("Final customer price", "rate_special_projects", "rate_special_projects", _rval("special_projects_per_hour"), BASELINE_RATES["special_projects_per_hour"], hint=_rate_hint("special_projects_per_hour")),
        ),
        _pricing_line(
            "Special project hours / month",
            "Expected monthly hours",
            _number_cell("Monthly hours", "actual_special_project_hours_mo", "actual_special_project_hours_mo", _aval("special_project_hours_mo"), 0, step="0.25"),
            _empty_cell("Volume input only"),
            _empty_cell("Not a customer fee"),
        ),
        _pricing_line(
            "Implementation & integration setup",
            "One-time customer setup fee",
            _empty_cell("One-time cost not modeled yet"),
            _suggested_cell("integration_setup_fee", None, None),
            _number_cell("Final customer price", "rate_integration_setup_fee", "rate_integration_setup_fee", _rval("integration_setup_fee"), BASELINE_RATES["integration_setup_fee"], step="1", hint=_rate_hint("integration_setup_fee")),
        ),
        _pricing_line(
            "Monthly minimum",
            "Customer-facing floor",
            _empty_cell("Not an internal cost"),
            _suggested_cell("monthly_minimum", None, None),
            _number_cell("Final customer price", "rate_minimum", "rate_minimum", _rval("monthly_minimum"), BASELINE_RATES["monthly_minimum"], step="1", hint=_rate_hint("monthly_minimum")),
        ),
        _pricing_line(
            "Late fee",
            "Past-due agreement term",
            _empty_cell("Not an internal cost"),
            _empty_cell(f"{BASELINE_RATES['late_fee_pct_per_7_days']:g}% every 7 days past due"),
            _empty_cell("Agreement term; not included in MRR"),
        ),
    ]
    pricing_lines_html = (
        '<table class="pricing-lines"><thead><tr>'
        '<th>Line item</th><th>Internal fulfillment cost</th><th>Suggested customer price</th><th>Final customer price</th>'
        '</tr></thead><tbody>'
        + "".join(pricing_lines)
        + "</tbody></table>"
    )
    prospect_summary_html = f"""
      <div class="prospect-summary" aria-label="Prospect summary">
        <div class="prospect-summary__item"><span>Prospect</span><strong>{_esc(profile.get('brand') or summary.get('prospect') or '—')}</strong></div>
        <div class="prospect-summary__item"><span>Monthly volume</span><strong>{_esc(monthly_volume or '—')}</strong></div>
        <div class="prospect-summary__item"><span>Products</span><strong>{len(products)}</strong></div>
        <div class="prospect-summary__item"><span>HubSpot deal</span><strong>{_esc(hubspot_deal_id or 'Not attached')}</strong></div>
      </div>
    """

    rate_card_note_val = _esc(str(summary.get("rate_card_note") or ""))
    history_bar_html = _history_bar_html(summary)

    status_label = "Published" if published else "Draft — not publicly visible yet"
    status_pill_cls = "pill--live" if published else "pill--draft"
    if not published:
        primary_action_html = (
            '<section class="operator-callout">'
            '<div><p class="eyebrow">Next action</p><h2>Publish this rate sheet.</h2>'
            '<p>The sheet is still private. Confirm the extraction, pricing, and costs below, then publish to create the shareable link.</p>'
            '<div class="operator-callout__meta">Resolution path: publish -> copy email/link -> create quote when ready.</div></div>'
            f'<div class="operator-callout__side"><form method="post" action="{base}/runs/{run_id}/publish">'
            '<button class="btn" type="submit">Publish rate sheet</button></form></div>'
            '</section>'
        )
    elif quote_guard_errors:
        primary_action_html = (
            '<section class="operator-callout">'
            '<div><p class="eyebrow">Next action</p><h2>Clear quote blockers.</h2>'
            f'<p>{_esc(str(quote_guard_errors[0]))}</p>'
            '<div class="operator-callout__meta">Save the required deal, pricing, or cost fields below before creating the HubSpot quote.</div></div>'
            '<div class="operator-callout__side"><button class="btn btn--ghost" type="submit" form="rate-sheet-update">Save changes</button></div>'
            '</section>'
        )
    elif hs_quote_url:
        primary_action_html = (
            '<section class="operator-callout">'
            '<div><p class="eyebrow">Next action</p><h2>Quote is ready to send.</h2>'
            '<p>The rate sheet is live and the HubSpot quote exists. Open it to send or confirm e-signature status.</p></div>'
            f'<div class="operator-callout__side"><a class="btn" href="{_esc(hs_quote_url)}" target="_blank" rel="noreferrer">Open HubSpot quote</a></div>'
            '</section>'
        )
    else:
        primary_action_html = (
            '<section class="operator-callout">'
            '<div><p class="eyebrow">Next action</p><h2>Create the HubSpot quote.</h2>'
            '<p>The sheet is live and quote readiness checks passed. Create the quote so Sales has a sendable closing asset.</p></div>'
            f'<div class="operator-callout__side"><form method="post" action="{base}/runs/{run_id}/quote">'
            '<button class="btn" type="submit" style="background:#ff7a59;border-color:#ff7a59;color:#fff">Create HubSpot quote</button></form></div>'
            '</section>'
        )
    if published and view_path:
        rate_sheet_card = f"""
          <article class="review-action-card">
            <div class="review-action-card__head">
              <h3>Published rate sheet</h3>
              <span class="pill pill--live">Live</span>
            </div>
            <code>{_esc(view_path)}</code>
            <p>Prospect-facing sheet. Save &amp; re-render updates the preview and public URL; re-publish refreshes the live sheet and quote workflow.</p>
            <div class="review-action-card__buttons">
              <button class="btn btn--ghost" type="button"
                onclick="navigator.clipboard.writeText(window.location.origin+'{_esc(view_path)}');this.textContent='Copied!';setTimeout(()=>this.textContent='Copy link',1800)">Copy link</button>
              <button class="btn btn--ghost" type="button" onclick="{_copy_email_js}">Copy email</button>
              <a class="btn btn--ghost" href="{_esc(view_path)}?viewer=internal" target="_blank" rel="noreferrer">Open</a>
              {hs_deal_chip}
              {hs_quote_btn if hs_quote_url else hs_create_quote_btn}
            </div>
          </article>"""
    else:
        rate_sheet_card = f"""
          <article class="review-action-card">
            <div class="review-action-card__head">
              <h3>Draft rate sheet</h3>
              <span class="pill pill--draft">Private</span>
            </div>
            <p>Confirm the extracted prospect details, products, pricing, and costs below before publishing.</p>
          </article>"""

    if cost_form_path:
        cost_form_card = f"""
          <article class="review-action-card">
            <div class="review-action-card__head">
              <h3>Fulfillment cost form.</h3>
              <span class="pill pill--estimated">Internal</span>
            </div>
            <p>Share with fulfillment for warehouse cost input only. It does not show sales pricing, customer pitch, margin, or quote details.</p>
            <div class="review-action-card__buttons">
              <button class="btn btn--ghost" type="button"
                onclick="navigator.clipboard.writeText(window.location.origin+'{_esc(cost_form_path)}');this.textContent='Cost form copied!';setTimeout(()=>this.textContent='Copy cost form link',2000)">Copy cost form link</button>
              <a class="btn btn--ghost" href="{_esc(cost_form_path)}" target="_blank" rel="noreferrer">Open cost form</a>
            </div>
          </article>"""
    else:
        cost_form_card = """
          <article class="review-action-card">
            <div class="review-action-card__head"><h3>Fulfillment cost form.</h3></div>
            <p>Cost form link is not available yet. Save or regenerate this rate sheet if the link is missing.</p>
          </article>"""

    review_flag_sections = []
    if quote_guard_errors:
        review_flag_sections.append(
            '<strong>Quote blocked until:</strong><ul>'
            + "".join(f"<li>{_esc(item)}</li>" for item in quote_guard_errors)
            + "</ul>"
        )
    if warnings:
        warn_label = "Published — notes:" if published else "Check before publishing:"
        review_flag_sections.append(
            f"<strong>{warn_label}</strong><ul>"
            + "".join(f"<li>{_esc(w)}</li>" for w in warnings[:12])
            + "</ul>"
        )
    review_flags_drawer = ""
    if review_flag_sections:
        review_flags_drawer = (
            '<details class="review-drawer">'
            f'<summary>Review flags ({len(quote_guard_errors) + len(warnings)})</summary>'
            '<div class="review-drawer__body">'
            + "".join(review_flag_sections)
            + "</div></details>"
        )
    warehouse_drawer = f"""
      <details class="review-drawer">
        <summary>Warehouse approval &amp; handoff</summary>
        <div class="review-drawer__body">
          {assortment_html}
          <div style="display:flex;justify-content:space-between;align-items:center;gap:12px;margin-top:10px">
            <strong>Fulfillment Brief</strong>
            <button class="btn btn--ghost" type="button" style="flex-shrink:0"
              data-brief="{review_brief_attr}"
              onclick="navigator.clipboard.writeText(this.dataset.brief);this.textContent='Copied!';setTimeout(()=>this.textContent='Copy brief',2000)">Copy brief</button>
          </div>
          <pre>{_esc(review_brief_text)}</pre>
        </div>
      </details>"""
    latest_cost_sig = ""
    if latest_cost_submission:
        latest_cost_sig = " by " + _esc(latest_cost_submission.get("name") or latest_cost_submission.get("email") or "fulfillment")
    quote_status_text = "HubSpot quote ready" if hs_quote_url else ("Blocked" if quote_guard_errors else "Ready to create")
    quote_status_detail = "Open quote in HubSpot." if hs_quote_url else (quote_guard_errors[0] if quote_guard_errors else "Quote guards passed.")
    status_strip_html = f"""
      <div class="review-status-strip" aria-label="Rate sheet workflow status">
        <div class="review-status-card"><span>Rate sheet</span><strong>{_esc(status_label)}</strong><em>{'Public link is live.' if published else 'Publish before sharing.'}</em></div>
        <div class="review-status-card"><span>Cost form</span><strong>{_esc(section_cost_status)}</strong><em>{'Latest signed submission' + latest_cost_sig if signed_cost_submissions else 'Send the cost form to fulfillment.'}</em></div>
        <div class="review-status-card"><span>Quote</span><strong>{_esc(quote_status_text)}</strong><em>{_esc(quote_status_detail)}</em></div>
        <div class="review-status-card"><span>Sales follow-up</span><strong>{'Ready after quote' if hs_quote_url else 'Use Deal Detail'}</strong><em>Draft the email from the HubSpot deal command center.</em></div>
      </div>
    """
    action_hub_html = f"""
      <section class="review-hub">
        {status_strip_html}
        <div class="review-action-grid">
          {rate_sheet_card}
          {cost_form_card}
        </div>
        <div class="review-drawers">
          {review_flags_drawer}
          {warehouse_drawer}
        </div>
      </section>
    """

    styles = _STYLES.replace("__NAV__", render_agent_nav_styles())
    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>agent | Review Rate Sheet — {_esc(summary.get('prospect') or f'Run {run_id}')}</title>
    {render_agent_favicon_links()}
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Montserrat:wght@700;800&display=swap" rel="stylesheet">
    <link rel="stylesheet" href="/static/admin.css">
    <style>{styles}
      .preview-frame {{ width: 100%; height: 70vh; border: 1px solid var(--border);
        border-radius: 16px; background: #fff; box-shadow: 0 12px 28px var(--shadow); }}
      .review-actions {{ display: flex; gap: 10px; flex-wrap: wrap; align-items: center; margin-top: 16px; }}
      .products-table input[type=number] {{ font-family: inherit; }}
    </style>
  </head>
  <body>
    {render_agent_nav("fulfillment", fulfillment_section="fulfillment_sales", user=user)}
    <main class="shell">
      <div class="workspace">
        <p class="eyebrow"><a href="{base}" style="color:inherit;text-decoration:none;opacity:0.7">← Pipeline</a> · Review</p>
        <h1>{_esc(summary.get('prospect') or 'Rate sheet')} <span style="color:var(--light-blue)">rate sheet</span>.</h1>
        <p class="intro">{'Rate sheet is live — edit fields below and re-publish to update. Shareable link stays the same.' if published else 'Check the preview, fix anything the extraction got wrong, then publish to activate the shareable link.'} <span class="pill {status_pill_cls}">{_esc(status_label)}</span></p>
        {flash_html}
        {primary_action_html}
        {action_hub_html}
        <form method="post" action="{base}/runs/{run_id}/update" id="rate-sheet-update">
          {prospect_summary_html}
          <div class="review-sections">
          <details class="review-section" open>
            <summary>
              <span>Deal &amp; Quote Readiness <span class="review-section__sub">{_esc(section_deal_status)} · {_esc(section_pricing_status)}</span></span>
            </summary>
            <div class="review-section__body">
          <div class="grid2">
            <div>
              {deal_picker_html}
            </div>
            <div class="flash" style="margin:0;background:rgba(133,187,218,0.08);border-color:rgba(133,187,218,0.35)">
              <strong>Before creating a quote</strong>
              <label style="display:flex;gap:8px;align-items:center;font-size:13px;font-weight:700;margin:12px 0 8px">
                <input type="checkbox" name="sales_pricing_reviewed" value="1" {"checked" if pricing_reviewed else ""} style="width:auto"> Sales pricing reviewed
              </label>
              <p class="muted" style="margin:0 0 10px">If the deal already exists, select it and save. If it does not exist yet, create it from this rate sheet; it will return here attached.</p>
              <div style="display:flex;gap:8px;flex-wrap:wrap">
                <button class="btn btn--ghost" type="submit">Save deal &amp; pricing</button>
                <a class="btn" href="{create_deal_href}">Create new HubSpot deal</a>
              </div>
            </div>
          </div>
            </div>
          </details>
        <iframe class="preview-frame" id="preview" src="{base}/runs/{run_id}/preview" title="Rate sheet preview"></iframe>

          <details class="review-section">
            <summary>
              <span>Prospect Details <span class="review-section__sub">{_esc(profile.get('brand') or summary.get('prospect') or 'Brand details')} · {monthly_volume or 'no'} orders/mo</span></span>
            </summary>
            <div class="review-section__body">
          <div class="form-grid">
            <div>
              <div class="field">
                <label for="brand">Brand</label>
                <input type="text" id="brand" name="brand" value="{_esc(profile.get('brand') or '')}">
              </div>
              <div class="field">
                <label for="destinations_note">Destinations note</label>
                <input type="text" id="destinations_note" name="destinations_note" value="{_esc(profile.get('destinations_note') or '')}">
              </div>
              <div class="field">
                <label for="current_costs_note">Current costs note</label>
                <input type="text" id="current_costs_note" name="current_costs_note" value="{_esc(profile.get('current_costs_note') or '')}">
              </div>
            </div>
            <div>
              <div class="field">
                <label for="origin_zip">Ship-from ZIP</label>
                <input type="text" id="origin_zip" name="origin_zip" value="{_esc(summary.get('origin_zip') or '')}">
              </div>
              <div class="field">
                <label for="monthly_order_volume">Monthly order volume</label>
                <input type="text" id="monthly_order_volume" name="monthly_order_volume" value="{_esc('' if monthly_volume is None else monthly_volume)}">
                {volume_basis_hint}
              </div>
              <div class="field">
                <label for="current_cost_per_parcel_usd">Current $/parcel</label>
                <input type="text" id="current_cost_per_parcel_usd" name="current_cost_per_parcel_usd" value="{_esc('' if current_cost is None else f'{current_cost:g}')}">
                <span class="hint">Drives the savings section — leave blank to omit.</span>
              </div>
              <div class="field">
                <label for="quote_margin_override">Quote margin override %</label>
                <input type="number" id="quote_margin_override" name="quote_margin_override" step="any" min="0" value="{_esc(margin_value)}">
                <span class="hint">Blank = automatic by product category. Example: 12 suggests blank customer prices at agreement default x 1.12. Manual final prices are not multiplied again.</span>
              </div>
            </div>
          </div>
            </div>
          </details>

          <details class="review-section" open>
            <summary>
              <span>Pricing &amp; Cost Lines <span class="review-section__sub">{_esc(section_cost_status)} · customer fees beside fulfillment costs</span></span>
            </summary>
            <div class="review-section__body">
          <input type="hidden" name="actual_costs_form" value="1">
          {pricing_summary_html}
          <p class="muted" style="margin-bottom:12px">Internal fulfillment costs are warehouse-only and never shown to the prospect. Final customer prices are shown on the public rate sheet and used by Calculate My Estimate after save/re-publish. Leave a final price blank to use the agreement default plus any quote margin override.</p>
          {pricing_lines_html}
          <div class="field" style="margin-top:14px">
            <label for="rate_card_note">Rate card note (shown at bottom of Full Rate Card section)</label>
            <textarea id="rate_card_note" name="rate_card_note" rows="2" style="width:100%;resize:vertical">{rate_card_note_val}</textarea>
            <span class="hint">Use to call out specials, volume commitments, expiry dates, etc.</span>
          </div>
            </div>
          </details>

          <details class="review-section">
            <summary>
              <span>Sales Pricing &amp; Waivers <span class="review-section__sub">{_esc(section_waiver_status)} · reasons required before quote creation</span></span>
            </summary>
            <div class="review-section__body">
          <p class="muted" style="margin-bottom:12px">Sales can waive or override any fee. Waivers are allowed, but quote creation requires a reason so the deal stays auditable.</p>
          <div class="grid2">
            <div class="field">
              <label>Waived fees</label>
              <div style="border:1px solid var(--border);border-radius:12px;padding:10px 12px;background:#fff;max-height:220px;overflow:auto">{fee_checks}</div>
            </div>
            <div>
              <div class="field">
                <label for="waiver_reason">Waiver / pricing reason</label>
                <textarea id="waiver_reason" name="waiver_reason" rows="4" style="width:100%;resize:vertical" placeholder="Required when any fee is waived.">{waiver_reason}</textarea>
              </div>
              <label style="display:flex;gap:8px;align-items:center;font-size:13px;font-weight:700;margin:10px 0">
                <input type="checkbox" name="sales_pricing_reviewed" value="1" {"checked" if pricing_reviewed else ""} style="width:auto"> Sales pricing reviewed
              </label>
              <label style="display:flex;gap:8px;align-items:center;font-size:13px;font-weight:700;margin:10px 0">
                <input type="checkbox" name="margin_approved" value="1" {"checked" if margin_approved else ""} style="width:auto"> Approve low-margin exception
              </label>
            </div>
          </div>

            </div>
          </details>

          <details class="review-section">
            <summary>
              <span>Products <span class="review-section__sub">{_esc(section_product_status)} · dimensions drive pallets, storage, and packaging</span></span>
            </summary>
            <div class="review-section__body">
          {'<div class="flash flash--warn" style="margin-bottom:12px"><strong>No products on file.</strong> Fill in at least one product row below (name + dimensions + units/mo) so the rate sheet shows accurate savings estimates, then save &amp; re-publish.</div>' if not products else ''}
          <table class="products-table">
            <thead><tr><th>Name</th><th>L (in)</th><th>W (in)</th><th>H (in)</th><th>Weight (lb)</th><th>Units / mo <span style="font-weight:400;font-size:11px;opacity:0.55">(opt.)</span></th><th>Remove</th></tr></thead>
            <tbody>{rows}</tbody>
          </table>
          <p class="muted">Rows tagged <span class="pill pill--estimated">estimated</span> had dimensions guessed from the product type — confirm or correct them before sending. Editing a dimension clears the tag. Tick Remove to drop a product; fill the empty row to add one.</p>
            </div>
          </details>
          </div>
          <div class="review-actions">
            <button class="btn" type="submit">Save &amp; re-render agent preview</button>
            <span class="muted" style="font-size:12px">Saves changes and rebuilds the agent/public preview. Use Re-publish below when the prospect-facing sheet should be refreshed.</span>
          </div>
        </form>
        {publish_form_html}
        {history_bar_html}
      </div>
    </main>
    <script>
      // Editing any dim/weight input clears that row's "estimated" flag.
      document.querySelectorAll('.products-table tbody tr').forEach(function(tr) {{
        var hidden = tr.querySelector('input[name=product_estimated]');
        if (!hidden) return;
        tr.querySelectorAll('input[type=number]').forEach(function(inp) {{
          inp.addEventListener('change', function() {{
            hidden.value = '0';
            var tag = tr.querySelector('.pill--estimated');
            if (tag) tag.remove();
          }});
        }});
      }});
    </script>
  </body>
</html>"""
