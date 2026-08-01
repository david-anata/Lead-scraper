"""Non-technical control panels for joined inventory and daily action truth."""

from __future__ import annotations

import html
from typing import Any, Mapping


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _metric(label: str, value: Any, tone: str = "neutral") -> str:
    return (
        f'<div class="summary-chip summary-{html.escape(tone, quote=True)}">'
        f"<span>{html.escape(label)}</span><strong>{html.escape(str(value))}</strong></div>"
    )


def render_production_inventory_panel(inventory: Mapping[str, Any]) -> str:
    summary = dict(inventory.get("summary") or {})
    coverage = dict(inventory.get("evidence_coverage") or {})
    missing = [
        key.replace("_", " ").title()
        for key, status in coverage.items()
        if status == "missing_export"
    ]
    missing_copy = (
        "Missing evidence exports: " + ", ".join(missing) + "."
        if missing
        else "All requested Screaming Frog evidence categories are represented."
    )
    return f"""
    <section class="card stack">
      <div class="section-heading">
        <div class="stack">
          <p class="eyebrow">Production inventory</p>
          <h2>Every known URL, reconciled.</h2>
          <p class="lead-sm">Sitemap, crawl, indexing, and intent evidence are joined by canonical production URL. Crawler warnings remain unverified until rendered-page and repository checks agree.</p>
        </div>
        <span class="status-pill {'status-ok' if not missing else 'status-warn'}">{'Complete evidence set' if not missing else f'{len(missing)} export gaps'}</span>
      </div>
      <div class="summary-grid">
        {_metric("Known production URLs", summary.get("known_production_urls", 0))}
        {_metric("Intent mapped", summary.get("intent_mapped_urls", 0), "good")}
        {_metric("Missing intent owner", summary.get("urls_missing_intent_owner", 0), "warn" if summary.get("urls_missing_intent_owner") else "good")}
        {_metric("Orphan candidates", summary.get("orphan_candidates", 0), "warn" if summary.get("orphan_candidates") else "good")}
        {_metric("Broken candidates", summary.get("broken_candidates", 0), "warn" if summary.get("broken_candidates") else "good")}
        {_metric("Exact duplicate groups", summary.get("exact_duplicate_groups", 0), "warn" if summary.get("exact_duplicate_groups") else "good")}
      </div>
      <p class="muted">{html.escape(missing_copy)}</p>
      <div class="button-row">
        <a class="text-link" href="/admin/website-ops/crawl">Inspect crawl evidence</a>
        <a class="text-link" href="/admin/website-ops/queries">Inspect intent ownership</a>
      </div>
    </section>
    """


def render_daily_portfolio_panel(portfolio: Mapping[str, Any]) -> str:
    selected = [
        dict(item)
        for item in portfolio.get("qualified_actions", []) or []
        if isinstance(item, Mapping)
    ]
    blockers = [
        dict(item)
        for item in portfolio.get("empty_slot_reasons", []) or []
        if isinstance(item, Mapping)
    ]
    actions = "".join(
        f"""
        <article class="task-card">
          <div class="row-actions">
            <h3>{html.escape(_clean(item.get("page_title")) or _clean(item.get("action_type")).replace("_", " ").title())}</h3>
            <span class="status-pill status-ok">Qualified</span>
          </div>
          <p class="muted">{html.escape(_clean(item.get("page_url")))}</p>
          <p>{html.escape(_clean(item.get("reason")) or "Passed the current evidence and execution gates.")}</p>
          <div class="mini-grid">
            {_metric("Service", _clean(item.get("service_pillar")) or "Unassigned")}
            {_metric("Action", _clean(item.get("action_type")).replace("_", " ").title())}
          </div>
        </article>
        """
        for item in selected[:8]
    ) or "<div class='list-card'><p class='muted'>No action is currently safe to execute. Empty slots are reported, never filled with invented work.</p></div>"
    blocker_copy = "; ".join(
        f"{int(item.get('count', 0) or 0)} {html.escape(_clean(item.get('state')).replace('_', ' '))}: {html.escape(_clean(item.get('reason')))}"
        for item in blockers
    )
    return f"""
    <section class="card stack">
      <div class="section-heading">
        <div class="stack">
          <p class="eyebrow">Today’s action portfolio</p>
          <h2>{int(portfolio.get('qualified_action_count', 0) or 0)} of 8 qualified actions.</h2>
          <p class="lead-sm">{html.escape(_clean(portfolio.get("truthful_summary")))}</p>
        </div>
        <span class="status-pill {'status-ok' if portfolio.get('status') == 'target_met' else 'status-warn'}">{html.escape(_clean(portfolio.get("status")).replace("_", " ").title())}</span>
      </div>
      <div class="summary-grid">
        {_metric("Qualified", portfolio.get("qualified_action_count", 0), "good" if selected else "neutral")}
        {_metric("Remaining slots", portfolio.get("remaining_slots", 8), "warn" if portfolio.get("remaining_slots") else "good")}
        {_metric("Daily target", portfolio.get("daily_action_target", 8))}
      </div>
      <div class="widget-scroll compact-scroll">{actions}</div>
      {f'<p class="muted"><strong>Why slots are empty:</strong> {blocker_copy}</p>' if blocker_copy else ''}
      <p><strong>Next operation:</strong> {html.escape(_clean(portfolio.get("next_operation")))}</p>
    </section>
    """
