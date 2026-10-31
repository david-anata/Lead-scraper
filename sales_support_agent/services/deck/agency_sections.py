"""New-content sections rendered in the agency_v1 visual design.

PR #524 introduced new offer, methodology and case-study content inside the
``intelligence_v2`` template. Sales asked for decks that look exactly like the
agency_v1 layout (run 1607) while keeping that content, so these helpers
render the same content with agency_v1 markup only (``slide``, ``slide-head``,
``rec-grid``/``rec-card``, ``offer-grid``/``offer-card``, ``next-steps``).
No new CSS is introduced, so the sections inherit the agency_v1 look.
"""

from __future__ import annotations

import html
from datetime import date, timedelta
from typing import Any

from sales_support_agent.services.deck.intelligence_v2 import (
    TRIAL_LABEL,
    _LOOP,
    _present,
    load_case_studies,
)

_PILLARS = (
    "Keyword research from your own search terms and the category's demand.",
    "Listing suggestions: coordinated copy, images and A+ content from live performance data.",
    "Campaign creation and automated bid, budget, search-term and targeting work.",
    "Budget and ACoS/TACoS guardrails tied to fulfilled-order reality.",
    "Human approval on higher-risk changes. Run it yourself, alongside our team, or hand it off.",
)


def _e(value: Any) -> str:
    return html.escape(str(value if value is not None else "").replace("\u2014", "-").replace("\u2013", "-"), quote=True)


def _head(eyebrow: str, title: str, caption: str) -> str:
    return (
        '<header class="slide-head"><div class="heading-stack">'
        f'<p class="eyebrow">{_e(eyebrow)}</p><h2 class="slide-title">{_e(title)}</h2></div>'
        f'<p class="caption">{_e(caption)}</p></header>'
    )


def render_methodology_section() -> str:
    steps = "".join(
        f'<div class="next-step"><span class="num">{i:02d} · {_e(name)}</span><p>{_e(text)}</p></div>'
        for i, (name, text) in enumerate(_LOOP, start=1)
    )
    pillars = "".join(f"<li>{_e(item)}</li>" for item in _PILLARS)
    return (
        '<section class="slide slide-method" data-screen-label="08 How it works">'
        + _head(
            "How Anata Intelligence works",
            "One operating loop: Sync, Analyze, Decide, Act, Confirm",
            "The same loop runs every day across ads, listings and inventory.",
        )
        + f'<div class="next-steps" style="grid-template-columns:repeat(auto-fit,minmax(170px,1fr))">{steps}</div>'
        + '<div class="rec-grid" style="margin-top:24px"><div class="rec-card">'
        + f"<h3>What it does every day</h3><ul>{pillars}</ul></div></div>"
        + "</section>"
    )


def render_case_studies_section(case_study_url: str = "") -> str:
    cards = []
    for study in load_case_studies():
        facts = "".join(
            f"<li><strong>{label}:</strong> {_e(study.get(key))}</li>"
            for label, key in (("Before", "before"), ("After", "after"), ("Campaign volume", "campaign_volume"), ("Ad results", "ad_results"))
            if _present(study.get(key))
        )
        link = (
            f'<p><a class="link" href="{_e(study["url"])}" target="_blank" rel="noreferrer">Read the case study →</a></p>'
            if study.get("published") and study.get("url")
            else ""
        )
        cards.append(
            f'<div class="rec-card"><p class="eyebrow">{_e(study.get("name"))}</p><h3>{_e(study.get("headline"))}</h3>'
            f'<p>{_e(study.get("detail"))}</p>{f"<ul>{facts}</ul>" if facts else ""}{link}</div>'
        )
    extra = str(case_study_url or "").strip()
    extra_html = (
        f'<p class="caption" style="margin-top:16px"><a href="{_e(extra)}" target="_blank" rel="noreferrer">'
        "More proof: the case study picked for this brand →</a></p>"
        if extra
        else ""
    )
    return (
        '<section class="slide slide-proof" data-screen-label="09 Case studies">'
        + _head("Case studies", "Brands that run on Anata", "Anata client results as published on anatainc.com.")
        + f'<div class="rec-grid">{"".join(cards)}</div>{extra_html}'
        + "</section>"
    )


def render_offer_section(*, offer_cards_html: str, urls: dict[str, str], generated: date, impact: str = "") -> str:
    follow_up = generated + timedelta(days=3)
    while follow_up.weekday() >= 5:
        follow_up += timedelta(days=1)
    trial_card = (
        '<article class="offer-card featured"><h3>Anata Intelligence · Free 14-day trial</h3>'
        f'<p class="desc">$0 for 14 days. Then plans from $99/mo. Card on file, cancel anytime.</p>'
        "<dl>"
        "<div><dt>Trial</dt><dd>14 days free</dd></div>"
        "<div><dt>After trial</dt><dd>From $99/mo</dd></div>"
        "<div><dt>Includes</dt><dd>Profit by SKU, ad automation with approval, listing and CRO suggestions</dd></div>"
        "<div><dt>Alerts</dt><dd>Account health, Buy Box and inventory in one view</dd></div>"
        "</dl>"
        '<p class="bonus">+ Kickoff call to set your guardrails</p>'
        f'<a class="cta" href="{_e(urls["trial"])}" target="_blank" rel="noreferrer">{_e(TRIAL_LABEL)} →</a></article>'
    )
    return f"""
    <section class="slide slide-offers" data-screen-label="10 Offer and pricing">
      {_head("Offer and pricing", "Start with the platform. Add people when you want them.", "Start the free trial, or add Anata's operators for done-for-you execution.")}
      <div class="offer-grid">{trial_card}{offer_cards_html}</div>
      <div class="next-steps">
        <div class="next-step cta">
          <span class="num">Step 1</span>
          <h4>Sign up</h4>
          <p>Start the free 14-day trial and connect Amazon.</p>
          <a class="link" href="{_e(urls["trial"])}" target="_blank" rel="noreferrer">{_e(TRIAL_LABEL)} →</a>
        </div>
        <div class="next-step">
          <span class="num">Step 2</span>
          <h4>Follow-up on {_e(follow_up.strftime("%A, %B %-d"))}</h4>
          <p>{_e(impact) if impact else "We review this analysis and your first sync together."}</p>
        </div>
        <div class="next-step">
          <span class="num">Step 3</span>
          <h4>Book a call</h4>
          <p>Prefer to talk first? Pick a time that works.</p>
          <a class="link" href="{_e(urls["booking"])}" target="_blank" rel="noreferrer">Book a call →</a>
        </div>
      </div>
    </section>"""
