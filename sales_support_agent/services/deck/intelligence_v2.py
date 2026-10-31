"""Anata Intelligence brand-analysis deck (template_version="intelligence_v2").

The default deck template since v2. The old agency strategy deck stays
available as ``agency_v1`` (``DeckGenerationService._render_html_deck``), and
decks already generated keep rendering from the HTML stored on their run.

Rules this renderer follows:
- Every number on a slide carries its source (the uploaded file or API it came
  from). Missing values render as nothing, never "n/a", "0" or an estimate.
- No projected results. The 14-day plan lists actions, not outcomes.
- Case studies come from ``data/case_studies.json`` and hide empty fields.
- No em dashes anywhere in the output.
"""

from __future__ import annotations

import html
import json
import re
from dataclasses import dataclass
from datetime import date, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable

TEMPLATE_VERSION = "intelligence_v2"
LEGACY_TEMPLATE_VERSION = "agency_v1"
SUPPORTED_TEMPLATE_VERSIONS = (TEMPLATE_VERSION, LEGACY_TEMPLATE_VERSION)
DEFAULT_TEMPLATE_VERSION = LEGACY_TEMPLATE_VERSION
DEFAULT_BOOKING_URL = "https://calendar.app.google/5TwjbbpmKaGAQuBk8"
PLANS_FROM_LABEL = "Plans from $99/mo"
TRIAL_LABEL = "Start your free 14-day trial"

_CASE_STUDY_PATH = Path(__file__).resolve().parent / "data" / "case_studies.json"
# Same engagement tracking as agency_v1 (PR54): section dwell + heartbeat.
_ENGAGEMENT_PATH = Path(__file__).resolve().parent / "data" / "engagement.html"
_DASHES = re.compile("[\u2014\u2013]")
_PLACEHOLDER_VALUES = {"", "n/a", "na", "none", "null", "0", "0.0", "$0", "$0.00", "tbd", "$tbd", "-", "\u2014", "\u2013"}


def normalize_template_version(value: str | None) -> str:
    cleaned = str(value or "").strip().lower()
    # agency_v1 (run 1607 look, now with the #524 offer, methodology and case
    # studies sections) is the default; intelligence_v2 stays opt-in.
    return cleaned if cleaned in SUPPORTED_TEMPLATE_VERSIONS else DEFAULT_TEMPLATE_VERSION


def _e(value: Any) -> str:
    return html.escape(_DASHES.sub("-", str(value if value is not None else "")), quote=True)


def _present(value: Any) -> bool:
    """True when a value is real data worth showing."""

    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value > 0
    return str(value).strip().lower() not in _PLACEHOLDER_VALUES


def _money(value: float | None) -> str:
    if value is None or value <= 0:
        return ""
    if value >= 1_000_000:
        return f"${value / 1_000_000:.1f}M"
    if value >= 10_000:
        return f"${value / 1_000:.0f}K"
    return f"${value:,.0f}"


def _int(value: Any) -> str:
    try:
        number = int(round(float(value)))
    except (TypeError, ValueError):
        return ""
    return f"{number:,}" if number > 0 else ""


@lru_cache(maxsize=1)
def _engagement_script() -> str:
    try:
        return _ENGAGEMENT_PATH.read_text(encoding="utf-8")
    except OSError:  # pragma: no cover - packaging safety
        return ""


@lru_cache(maxsize=1)
def load_case_studies() -> tuple[dict[str, Any], ...]:
    data = json.loads(_CASE_STUDY_PATH.read_text(encoding="utf-8"))
    return tuple(dict(item) for item in data.get("case_studies", []))


# ---------------------------------------------------------------------------
# Market model: dedupe, shares that can never pass 100%
# ---------------------------------------------------------------------------


def unique_listings(products: Iterable[Any]) -> list[Any]:
    """Drop repeated ASINs and child variants that repeat the parent numbers.

    Helium 10 exports one row per variant child with identical parent-level
    units and revenue. Counting each row inflates a brand (run 1607 showed
    Drift at 113.1% share). The key mirrors ``parse_xray_csv``'s total so
    shares and totals are computed over the same set.
    """

    seen_asins: set[str] = set()
    seen_parents: set[tuple[str, int, int]] = set()
    out: list[Any] = []
    for product in products:
        asin = str(getattr(product, "asin", "") or "").strip().upper()
        if asin and asin in seen_asins:
            continue
        brand = str(getattr(product, "brand", "") or "").strip().lower()
        units = int(round(getattr(product, "units_sold", 0) or 0))
        revenue = int(round(getattr(product, "revenue", 0) or 0))
        if brand and units > 0:
            key = (brand, units, revenue)
            if key in seen_parents:
                continue
            seen_parents.add(key)
        if asin:
            seen_asins.add(asin)
        out.append(product)
    return out


def share_pct(part: float | None, total: float | None) -> float | None:
    if not part or not total or part <= 0 or total <= 0:
        return None
    return min(part / total * 100.0, 100.0)


@dataclass(frozen=True)
class BrandShare:
    brand: str
    listings: int
    revenue: float
    share: float | None


def brand_shares(products: list[Any]) -> list[BrandShare]:
    listings = unique_listings(products)
    total = sum(float(getattr(p, "revenue", 0) or 0) for p in listings)
    buckets: dict[str, list[Any]] = {}
    labels: dict[str, str] = {}
    for product in listings:
        name = str(getattr(product, "brand", "") or "").strip()
        if not name:
            continue
        buckets.setdefault(name.lower(), []).append(product)
        labels.setdefault(name.lower(), name)
    rows = []
    for key, items in buckets.items():
        revenue = sum(float(getattr(p, "revenue", 0) or 0) for p in items)
        rows.append(BrandShare(labels[key], len(items), revenue, share_pct(revenue, total)))
    rows.sort(key=lambda row: row.revenue, reverse=True)
    return rows


# ---------------------------------------------------------------------------
# Inputs -> facts
# ---------------------------------------------------------------------------


def _keywords(payload: dict[str, Any]) -> tuple[list[tuple[str, int | None]], str]:
    """(phrase, search volume) pairs and the source label.

    Phrases that share no word with the listing or category are dropped
    (Cerebro also ranks unrelated terms the ASIN happens to index for).
    """

    from sales_support_agent.services.deck.dataset import relevant_keyword_phrases

    pairs: list[tuple[str, int | None]] = []
    source = ""
    cerebro = payload.get("cerebro_report")
    report = payload.get("keyword_report")
    if cerebro is not None and getattr(cerebro, "keywords", None):
        pairs, source = [(k.phrase, k.search_volume) for k in cerebro.keywords], "Helium 10 Cerebro"
    elif report is not None and getattr(report, "keywords", None):
        pairs, source = [(k.phrase, k.search_volume) for k in report.keywords], "Helium 10 keyword export"
    if not pairs:
        return [], ""
    target = payload.get("target")
    title = target.get("title", "") if isinstance(target, dict) else getattr(target, "title", "")
    reference = " ".join(str(x) for x in (title, payload.get("category_label", "")) if x)
    keep = set(relevant_keyword_phrases([p for p, _ in pairs], reference_text=reference))
    return [pair for pair in pairs if pair[0] in keep], source


def _market_source(payload: dict[str, Any]) -> str:
    xray = payload.get("xray_report")
    if xray is not None and str(getattr(xray, "discovery_query", "") or "").strip():
        return "Amazon search results via Rainforest API (revenue estimated from BSR)"
    return "Helium 10 Xray export (estimated monthly revenue)"


def _requested_channels(payload: dict[str, Any]) -> list[str]:
    labels = {
        "amazon": "Amazon",
        "tiktok_shop": "TikTok Shop",
        "shopify": "Shopify (DTC)",
        "3pl": "3PL fulfillment",
        "shipping_os": "Shipping OS",
    }
    raw = list(payload.get("requested_channels") or payload.get("channels") or [])
    return [labels.get(str(c), str(c)) for c in raw if str(c).strip()]


def build_findings(payload: dict[str, Any]) -> list[dict[str, str]]:
    """Top 3 findings, each with its number and the source of that number."""

    findings: list[dict[str, str]] = []
    xray = payload.get("xray_report")
    market_ready = bool(getattr(xray, "market_evidence_sufficient", True)) if xray is not None else False
    category = str(payload.get("category_label") or "").strip() or "this category"
    if xray is not None and market_ready:
        listings = unique_listings(getattr(xray, "products", []) or [])
        total = sum(float(getattr(p, "revenue", 0) or 0) for p in listings)
        shares = brand_shares(listings)
        if total > 0 and shares and shares[0].share:
            findings.append(
                {
                    "headline": f"{shares[0].brand} holds {shares[0].share:.0f}% of {_money(total)}/mo in {category}",
                    "detail": f"Across {len(listings)} unique listings in the comparison set.",
                    "source": _market_source(payload),
                }
            )
    keywords, kw_source = _keywords(payload)
    if keywords:
        phrase, volume = keywords[0]
        if _int(volume):
            findings.append(
                {
                    "headline": f"\u201c{phrase}\u201d is searched {_int(volume)} times a month",
                    "detail": "The highest-volume relevant search in the uploaded keyword set.",
                    "source": kw_source,
                }
            )
    misses = list((payload.get("search_insights") or {}).get("title_misses") or [])
    if misses and keywords:
        findings.append(
            {
                "headline": (
                    "1 high-volume search is missing from the title"
                    if len(misses) == 1
                    else f"{len(misses)} high-volume searches are missing from the title"
                ),
                "detail": "Starting with: " + ", ".join(misses[:3]) + ".",
                "source": f"{kw_source} compared with the live listing title",
            }
        )
    target = dict(payload.get("target") or {})
    if len(findings) < 3 and _present(target.get("review_count")) and _present(target.get("rating")):
        findings.append(
            {
                "headline": f"{_int(target.get('review_count'))} reviews at {target.get('rating')} stars",
                "detail": "Current social proof on the listing reviewed.",
                "source": "Amazon listing data",
            }
        )
    return findings[:3]


def build_plan(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Day 1 / 7 / 14 actions built only from the brand's own inputs."""

    keywords, _ = _keywords(payload)
    phrases = [p for p, _v in keywords][:3]
    misses = list((payload.get("search_insights") or {}).get("title_misses") or [])[:3]
    copy_misses = list((payload.get("search_insights") or {}).get("copy_misses") or [])[:2]
    target_brand = str((payload.get("target") or {}).get("brand_name") or "").strip().lower()
    competitor_brands: list[str] = []
    for product in payload.get("primary_competitors") or []:
        brand = str(getattr(product, "brand", "") or "").strip()
        if brand and brand.lower() != target_brand and brand not in competitor_brands:
            competitor_brands.append(brand)
    growth_plan = payload.get("growth_plan")
    tacos = float(getattr(growth_plan, "tacos_target_pct", 0) or 0) if growth_plan is not None else 0.0

    day1 = ["Connect Amazon so Anata Intelligence syncs sales, ads and listing data."]
    if misses:
        day1.append("Listing suggestions queued for review: add " + ", ".join(misses) + " to the title.")
    if copy_misses:
        day1.append("Bullet and description suggestions covering " + ", ".join(copy_misses) + ".")
    day7 = []
    if phrases:
        day7.append("Exact-match campaigns drafted for " + ", ".join(phrases) + ", waiting on your approval.")
    if competitor_brands:
        day7.append("Product-targeting campaign drafted against " + ", ".join(competitor_brands[:2]) + " listings.")
    day7.append(
        f"Budget and ACoS guardrails set at a {tacos:g}% TACoS target from your inputs."
        if tacos > 0
        else "Budget and ACoS guardrails set with you before anything launches."
    )
    day14 = [
        "Search-term review: scale what converts, pause what wastes spend.",
        "Confirm every change on the marketplace and review the results together.",
        "Decide the next 30 days: keep running it yourself, or add Anata's operators.",
    ]
    return [
        {"day": "Day 1", "title": "Sync and analyze", "items": day1},
        {"day": "Day 7", "title": "Decide and act", "items": day7},
        {"day": "Day 14", "title": "Confirm and plan", "items": day14},
    ]


def resolve_cta_urls(settings: Any) -> dict[str, str]:
    booking = str(getattr(settings, "marketing_booking_url", "") or "").strip() or DEFAULT_BOOKING_URL
    trial = str(getattr(settings, "trial_signup_url", "") or "").strip() or booking
    return {"trial": trial, "booking": booking}


# ---------------------------------------------------------------------------
# HTML
# ---------------------------------------------------------------------------


def _source(text: str) -> str:
    return f"<p class='src'>Source: {_e(text)}</p>" if text else ""


def _cta_strip(urls: dict[str, str]) -> str:
    return (
        "<div class='cta-strip'>"
        f"<a class='cta primary' href='{_e(urls['trial'])}' target='_blank' rel='noreferrer'>{TRIAL_LABEL}</a>"
        f"<span class='cta-note'>{PLANS_FROM_LABEL}</span>"
        f"<a class='cta ghost' href='{_e(urls['booking'])}' target='_blank' rel='noreferrer'>Book a call</a>"
        "</div>"
    )


def _slide(number: int, key: str, eyebrow: str, heading: str, body: str, urls: dict[str, str]) -> str:
    return (
        f"<section class='slide' id='{key}' data-section='{key}' data-screen-label='{_e(eyebrow)}'>"
        f"<div class='slide-inner'><p class='eyebrow'><span>{number:02d}</span>{_e(eyebrow)}</p>"
        f"<h2>{_e(heading)}</h2>{body}</div>{_cta_strip(urls)}</section>"
    )


def _list(items: Iterable[str]) -> str:
    rows = "".join(f"<li>{_e(item)}</li>" for item in items if str(item).strip())
    return f"<ul class='ticks'>{rows}</ul>" if rows else ""


def _cover(payload: dict[str, Any], brand: str, findings: list[dict[str, str]], urls: dict[str, str], generated: date) -> str:
    cards = "".join(
        "<article class='finding'>"
        f"<p class='num'>{i}</p><h3>{_e(f['headline'])}</h3><p>{_e(f['detail'])}</p>{_source(f['source'])}"
        "</article>"
        for i, f in enumerate(findings, start=1)
    )
    if not cards:
        cards = "<p class='muted'>Upload Helium 10 files to add data-backed findings here.</p>"
    category = str(payload.get("category_label") or "").strip()
    return (
        "<section class='slide cover' id='cover' data-section='cover' data-screen-label='Cover'>"
        "<div class='slide-inner'>"
        f"<p class='eyebrow light'><span>01</span>Brand analysis · {_e(generated.strftime('%B %-d, %Y'))}</p>"
        f"<h1>{_e(brand)} <em>x</em> Anata Intelligence</h1>"
        + (f"<p class='lede'>What we found in {_e(category)} on Amazon, and what the first 14 days would look like.</p>" if category else "")
        + f"<div class='findings'>{cards}</div>"
        "<div class='offer-pill'><strong>The offer:</strong> a free 14-day trial of Anata Intelligence, card on file, cancel anytime. Plans from $99/mo after that.</div>"
        f"</div>{_cta_strip(urls)}</section>"
    )


def _snapshot(payload: dict[str, Any], link_check: dict[str, Any] | None, urls: dict[str, str]) -> str:
    target = dict(payload.get("target") or {})
    link_check = link_check or {}
    website = dict(link_check.get("website") or {})
    amazon = dict(link_check.get("amazon") or {})
    site_html = (
        f"<a href='{_e(website.get('url'))}' target='_blank' rel='noreferrer'>{_e(website.get('url'))}</a>"
        if website.get("verified") and website.get("url")
        else "<span class='muted'>Website: not verified</span>"
    )
    amazon_urls = [u for u in (amazon.get("urls") or []) if u] if amazon.get("verified") else []
    amazon_html = (
        " ".join(f"<a href='{_e(u)}' target='_blank' rel='noreferrer'>{_e(u)}</a>" for u in amazon_urls)
        if amazon_urls
        else "<span class='muted'>Amazon presence: not verified</span>"
    )
    facts = []
    for label, key in (("Listing reviewed", "title"), ("ASIN", "asin"), ("Price", "price"), ("Rating", "rating"), ("Reviews", "review_count"), ("Best Sellers Rank", "bsr")):
        value = target.get(key)
        if key == "review_count":
            value = _int(value)
        if _present(value):
            facts.append(f"<div class='fact'><dt>{label}</dt><dd>{_e(value)}</dd></div>")
    image = str(target.get("image_url") or "").strip()
    image_html = f"<img class='hero-img' src='{_e(image)}' alt='{_e(target.get('title'))}' loading='lazy' onerror=\"this.remove()\" />" if image else ""
    channels = _requested_channels(payload)
    body = (
        "<div class='two'>"
        f"<div><dl class='facts'>{''.join(facts)}</dl>{_source('Amazon listing data for the ASIN or URL provided') if facts else ''}"
        f"<div class='links'><p><strong>Website</strong><br>{site_html}</p><p><strong>Amazon</strong><br>{amazon_html}</p>"
        "<p class='src'>Links appear only when our system opened them during this analysis.</p></div>"
        + (f"<p><strong>Channels in scope:</strong> {_e(', '.join(channels))}</p>" if channels else "")
        + f"</div><div class='media'>{image_html}</div></div>"
    )
    return _slide(2, "snapshot", "Brand snapshot", str(target.get("brand_name") or "Brand snapshot"), body, urls)


def _market(payload: dict[str, Any], urls: dict[str, str]) -> str:
    xray = payload.get("xray_report")
    ready = bool(getattr(xray, "market_evidence_sufficient", True)) if xray is not None else False
    category = str(payload.get("category_label") or "").strip() or "the category"
    if not ready:
        reason = str(getattr(xray, "market_evidence_reason", "") or "The comparison set did not pass relevance checks.")
        body = f"<div class='callout'><strong>Market numbers withheld.</strong> {_e(reason)}</div>"
        return _slide(3, "market", "Market and competitors", f"Who wins {category} today", body, urls)
    listings = unique_listings(getattr(xray, "products", []) or [])
    total = sum(float(getattr(p, "revenue", 0) or 0) for p in listings)
    shares = [row for row in brand_shares(listings) if row.share]
    bars = "".join(
        "<div class='bar-row'>"
        f"<span class='bar-label'>{_e(row.brand)} <small>{row.listings} listing{'s' if row.listings != 1 else ''}</small></span>"
        f"<span class='bar-track'><span class='bar-fill' style='width:{max(row.share or 0, 2):.1f}%'></span></span>"
        f"<span class='bar-val'>{row.share:.1f}%</span></div>"
        for row in shares[:6]
    )
    has_revenue = total > 0
    ordered = (
        sorted(listings, key=lambda p: float(getattr(p, "revenue", 0) or 0), reverse=True)
        if has_revenue
        else sorted(listings, key=lambda p: int(getattr(p, "review_count", 0) or 0), reverse=True)
    )
    rows = []
    for product in ordered[:6]:
        share = share_pct(getattr(product, "revenue", None), total)
        cells = [
            f"<td><strong>{_e(getattr(product, 'brand', ''))}</strong><br><small>{_e(str(getattr(product, 'title', ''))[:70])}</small></td>",
            f"<td class='n'>{_e(getattr(product, 'price_label', '')) if _present(getattr(product, 'price', None)) else ''}</td>",
        ]
        if has_revenue:
            cells.append(f"<td class='n'>{_money(getattr(product, 'revenue', None))}</td>")
            cells.append(f"<td class='n'>{f'{share:.1f}%' if share else ''}</td>")
        cells.append(f"<td class='n'>{_int(getattr(product, 'review_count', None))}</td>")
        rows.append("<tr>" + "".join(cells) + "</tr>")
    revenue_heads = "<th class='n'>Revenue/mo</th><th class='n'>Share</th>" if has_revenue else ""
    table = (
        f"<table class='tbl'><thead><tr><th>Listing</th><th class='n'>Price</th>{revenue_heads}"
        f"<th class='n'>Reviews</th></tr></thead><tbody>{''.join(rows)}</tbody></table>"
        if rows
        else ""
    )
    stat = (
        f"<p class='stat'><strong>{_money(total)}</strong> estimated monthly revenue across {len(listings)} unique listings</p>"
        if has_revenue
        else ""
    )
    # No revenue in the upload means no share to show; never estimate one.
    if bars:
        layout = f"<div class='two'><div><h3>Brand share</h3>{bars}</div><div><h3>Top listings</h3>{table}</div></div>"
    else:
        layout = f"<h3>Top listings by reviews</h3>{table}" if table else ""
    source = _market_source(payload) if has_revenue else _market_source(payload).split(" (")[0]
    body = f"{stat}{layout}{_source(source + '; repeated ASINs and variant rows counted once')}"
    return _slide(3, "market", "Market and competitors", f"Who wins {category} today", body, urls)


def _search(payload: dict[str, Any], urls: dict[str, str]) -> str:
    keywords, kw_source = _keywords(payload)
    rows = "".join(
        f"<tr><td>{_e(phrase)}</td><td class='n'>{_int(volume)}</td></tr>" for phrase, volume in keywords[:8]
    )
    kw_html = (
        f"<h3>Top searches</h3><table class='tbl'><thead><tr><th>Search term</th><th class='n'>Monthly searches</th></tr></thead><tbody>{rows}</tbody></table>{_source(kw_source)}"
        if rows
        else ""
    )
    words = getattr(payload.get("word_frequency_report"), "words", None) or []
    word_html = (
        "<h3>Words competitors repeat</h3><p class='chips'>"
        + "".join(f"<span class='chip'>{_e(w.word)} <small>{_int(w.frequency)}</small></span>" for w in words[:12])
        + f"</p>{_source('Helium 10 word frequency export')}"
        if words
        else ""
    )
    insights = dict(payload.get("search_insights") or {})
    gaps = [f"Missing from the title: {phrase}" for phrase in list(insights.get("title_misses") or [])[:4]]
    gaps += [f"Not covered in bullets or description: {label}" for label in list(insights.get("copy_misses") or [])[:3]]
    gaps += [str(item) for item in list(payload.get("cro_recommendations") or [])[:2]]
    gap_html = f"<h3>Listing and PDP gaps</h3>{_list(gaps)}" if gaps else ""
    mockup = str(payload.get("creative_mockup_url") or "").strip()
    mockup_html = (
        f"<p class='resource'><a href='{_e(mockup)}' target='_blank' rel='noreferrer'>View the creative concept we drafted for this listing</a></p>"
        if mockup
        else ""
    )
    if not (kw_html or word_html or gap_html):
        gap_html = "<p class='muted'>Upload Cerebro, keyword or word frequency files to fill this section.</p>"
    body = f"<div class='two'><div>{kw_html}{word_html}</div><div>{gap_html}{mockup_html}</div></div>"
    return _slide(4, "search", "Search behavior and listing gaps", "How shoppers search, and where the listing misses", body, urls)


_LOOP = (
    ("Sync", "Sales, ads, listings and inventory pulled in from the marketplace."),
    ("Analyze", "True profit by SKU and channel, search terms, listings and account health."),
    ("Decide", "Recommended moves ranked by impact, with the reason for each."),
    ("Act", "Bids, budgets, campaigns and listing updates made for you. Higher-risk changes wait for your approval."),
    ("Confirm", "The marketplace result is checked before an action is reported complete."),
)


def _method(urls: dict[str, str]) -> str:
    loop = "".join(
        f"<li><span class='step'>{i}</span><strong>{name}</strong><p>{_e(text)}</p></li>" for i, (name, text) in enumerate(_LOOP, start=1)
    )
    pillars = _list(
        [
            "Keyword research from your own search terms and the category's demand.",
            "Listing suggestions: coordinated copy, images and A+ content from live performance data.",
            "Campaign creation and automated bid, budget, search-term and targeting work.",
            "Budget and ACoS/TACoS guardrails tied to fulfilled-order reality.",
            "Human approval on higher-risk changes. Run it yourself, alongside our team, or hand it off.",
        ]
    )
    body = f"<ol class='loop'>{loop}</ol><h3>What it does every day</h3>{pillars}"
    return _slide(5, "method", "How Anata Intelligence works", "One operating loop: Sync, Analyze, Decide, Act, Confirm", body, urls)


def _plan(payload: dict[str, Any], urls: dict[str, str]) -> str:
    cols = "".join(
        f"<article class='day'><p class='day-tag'>{_e(step['day'])}</p><h3>{_e(step['title'])}</h3>{_list(step['items'])}</article>"
        for step in build_plan(payload)
    )
    body = f"<div class='days'>{cols}</div><p class='src'>Built from your keyword, competitor and listing inputs. This is a plan of actions, not a forecast.</p>"
    return _slide(6, "plan", "Your first 14 days", "What the trial would do for this brand", body, urls)


def _proof(payload: dict[str, Any], urls: dict[str, str]) -> str:
    cards = []
    for study in load_case_studies():
        rows = "".join(
            f"<div class='fact'><dt>{label}</dt><dd>{_e(study.get(key))}</dd></div>"
            for label, key in (("Before", "before"), ("After", "after"), ("Campaign volume", "campaign_volume"), ("Ad results", "ad_results"))
            if _present(study.get(key))
        )
        link = (
            f"<a href='{_e(study['url'])}' target='_blank' rel='noreferrer'>Read the case study</a>"
            if study.get("published") and study.get("url")
            else ""
        )
        cards.append(
            f"<article class='proof'><p class='proof-name'>{_e(study.get('name'))}</p><h3>{_e(study.get('headline'))}</h3>"
            f"<p>{_e(study.get('detail'))}</p><dl class='facts'>{rows}</dl>{link}</article>"
        )
    extra = str(payload.get("case_study_url") or "").strip()
    extra_html = (
        f"<p class='resource'><a href='{_e(extra)}' target='_blank' rel='noreferrer'>More proof: the case study picked for this brand</a></p>"
        if extra
        else ""
    )
    body = f"<div class='proofs'>{''.join(cards)}</div>{extra_html}{_source('Anata client results as published on anatainc.com')}"
    return _slide(7, "proof", "Proof", "Brands that run on Anata", body, urls)


def _offer(payload: dict[str, Any], urls: dict[str, str]) -> str:
    included = _list(
        [
            "Anata Intelligence for 14 days, free. Card on file, cancel anytime.",
            "Profit by SKU, ad automation with approval, listing and CRO suggestions.",
            "Account health, Buy Box and inventory alerts in one view.",
            "A kickoff call to set your guardrails.",
        ]
    )
    addons = []
    for card in payload.get("offer_cards") or []:
        price = str(card.get("price") or "")
        price_html = f"<p class='price'>{_e(price)} <small>{_e(card.get('price_label'))}</small></p>" if _present(price) else ""
        addons.append(
            f"<article class='addon'><h3>{_e(card.get('title'))}</h3>{price_html}<p>{_e(card.get('description'))}</p></article>"
        )
    services = []
    for section in payload.get("offering_sections") or []:
        names = [str(item.get("title") or "") for item in section.get("items") or []][:4]
        if names:
            services.append(f"<p><strong>{_e(section.get('title'))}:</strong> {_e(', '.join(names))}</p>")
    addon_html = (
        "<h3>Optional: add Anata's operators</h3>"
        + (f"<div class='addons'>{''.join(addons)}</div>" if addons else "")
        + "".join(services)
    )
    body = (
        "<div class='two'><div class='plan-card'><p class='eyebrow'>Free 14-day trial</p>"
        "<p class='big'>$0 for 14 days</p><p>Then plans from $99/mo.</p>"
        f"{included}<a class='cta primary' href='{_e(urls['trial'])}' target='_blank' rel='noreferrer'>{TRIAL_LABEL}</a></div>"
        f"<div>{addon_html}</div></div>"
    )
    return _slide(8, "offer", "Offer and pricing", "Start with the platform. Add people when you want them.", body, urls)


def _next(urls: dict[str, str], generated: date) -> str:
    follow_up = generated + timedelta(days=3)
    while follow_up.weekday() >= 5:
        follow_up += timedelta(days=1)
    steps = (
        "<ol class='next'>"
        f"<li><strong>Sign up</strong><p>Start the free 14-day trial and connect Amazon.</p><a href='{_e(urls['trial'])}' target='_blank' rel='noreferrer'>{TRIAL_LABEL}</a></li>"
        f"<li><strong>Follow-up on {_e(follow_up.strftime('%A, %B %-d'))}</strong><p>We review this analysis and your first sync together.</p></li>"
        f"<li><strong>Book a call</strong><p>Prefer to talk first? Pick a time that works.</p><a href='{_e(urls['booking'])}' target='_blank' rel='noreferrer'>Book a call</a></li>"
        "</ol>"
    )
    return _slide(9, "next", "Next steps", "Three ways to keep going", steps, urls)


_CSS = """
:root{--ink:#2b3644;--muted:#5d6977;--page:#f9f7f3;--surface:#fff;--soft:#f2ece3;--blue:#85bbda;--blue-strong:#5e9fc4;--blue-soft:#e7f3f9;--support:#bfa889;--border:#dfe3e6;--good:#477a59;--coral:#e9896a;--mint:#cfe8d6}
*{box-sizing:border-box}html{scroll-behavior:smooth}
body{margin:0;background:var(--page);color:var(--ink);font:16px/1.5 Inter,"Segoe UI",system-ui,sans-serif}
h1,h2,h3,.eyebrow,.cta,.num,.step,.day-tag,.proof-name{font-family:Montserrat,Inter,sans-serif}
a{color:var(--blue-strong)}
.topbar{position:sticky;top:0;z-index:5;display:flex;align-items:center;justify-content:space-between;gap:12px;padding:10px 20px;background:rgba(249,247,243,.92);backdrop-filter:blur(8px);border-bottom:1px solid var(--border)}
.topbar .brand{display:flex;align-items:center;gap:8px;font-weight:800;font-family:Montserrat,sans-serif}.topbar img{width:28px;height:28px}
.topbar nav{display:flex;gap:12px;overflow-x:auto;font-size:13px}.topbar nav a{color:var(--muted);text-decoration:none;white-space:nowrap}
.slide{min-height:100vh;scroll-margin-top:60px;display:flex;flex-direction:column;justify-content:space-between;border-bottom:1px solid var(--border)}
.slide:nth-of-type(even){background:var(--surface)}
.slide-inner{max-width:1120px;width:100%;margin:0 auto;padding:88px 24px 32px}
.eyebrow{display:flex;align-items:center;gap:10px;margin:0 0 8px;font-size:12px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:var(--blue-strong)}
.eyebrow span{display:inline-grid;place-items:center;min-width:30px;height:22px;border-radius:999px;background:var(--blue-soft);color:var(--ink)}
.eyebrow.light{color:#cfe3ef}.eyebrow.light span{background:rgba(255,255,255,.15);color:#fff}
h1{font-size:clamp(32px,5vw,52px);line-height:1.1;margin:8px 0 12px}h1 em{color:var(--blue);font-style:normal}
h2{font-size:clamp(24px,3.4vw,36px);line-height:1.15;margin:0 0 24px;max-width:28ch}h3{font-size:16px;margin:20px 0 10px}
.cover{background:linear-gradient(135deg,#2b3644 0%,#33506a 60%,#5e9fc4 100%);color:#fff}
.cover .lede{font-size:18px;color:#dce8f0;max-width:60ch}
.findings{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:16px;margin:28px 0}
.finding{background:rgba(255,255,255,.1);border:1px solid rgba(255,255,255,.18);border-radius:20px;padding:20px}
.finding h3{margin:6px 0;font-size:18px;color:#fff}.finding p{margin:4px 0;color:#dce8f0}.finding .src{color:#b9cddb}
.num{display:inline-grid;place-items:center;width:30px;height:30px;border-radius:50%;background:var(--coral);color:#fff;font-weight:800;margin:0}
.offer-pill{display:inline-block;padding:12px 18px;border-radius:999px;background:var(--soft);color:var(--ink)}
.cta-strip{display:flex;flex-wrap:wrap;align-items:center;gap:12px;justify-content:center;padding:14px 20px;background:var(--ink);color:#fff}
.cover .cta-strip{background:rgba(0,0,0,.25)}
.cta{display:inline-flex;align-items:center;min-height:44px;padding:0 20px;border-radius:10px;font-weight:700;font-size:14px;text-decoration:none}
.cta.primary{background:var(--coral);color:#fff}.cta.ghost{border:1px solid rgba(255,255,255,.5);color:#fff}
.plan-card .cta.primary{margin-top:12px}.cta-note{font-size:14px;color:#dce8f0;font-weight:600}
.two{display:grid;grid-template-columns:1.1fr 1fr;gap:32px;align-items:start}
.facts{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin:0}
.fact{background:var(--page);border:1px solid var(--border);border-radius:12px;padding:10px 12px}.slide:nth-of-type(odd) .fact{background:var(--surface)}
.fact dt{font-size:12px;color:var(--muted)}.fact dd{margin:2px 0 0;font-weight:600;overflow-wrap:anywhere}
.hero-img{width:100%;max-height:420px;object-fit:contain;background:#fff;border-radius:20px;border:1px solid var(--border);padding:16px}
.links p{margin:12px 0;overflow-wrap:anywhere}.muted{color:var(--muted)}
.src{font-size:12px;color:var(--muted);margin:8px 0 0}
.stat{font-size:18px;margin:0 0 8px}.stat strong{font-size:32px;font-family:Montserrat,sans-serif;color:var(--blue-strong)}
.bar-row{display:grid;grid-template-columns:150px 1fr 56px;gap:10px;align-items:center;margin:8px 0;font-size:14px}
.bar-label small{display:block;color:var(--muted);font-size:11px}
.bar-track{height:12px;border-radius:999px;background:var(--blue-soft);overflow:hidden}.bar-fill{display:block;height:100%;background:linear-gradient(90deg,var(--blue-strong),var(--blue))}
.bar-val{text-align:right;font-variant-numeric:tabular-nums;font-weight:600}
.tbl{width:100%;border-collapse:collapse;font-size:14px}.tbl th{text-align:left;font-size:12px;color:var(--muted);border-bottom:1px solid var(--border);padding:6px}
.tbl td{padding:8px 6px;border-bottom:1px solid var(--border);vertical-align:top}.tbl .n{text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap}
.tbl small{color:var(--muted)}
.chips{display:flex;flex-wrap:wrap;gap:8px}.chip{background:var(--blue-soft);border-radius:999px;padding:4px 12px;font-size:14px}.chip small{color:var(--muted)}
.ticks{list-style:none;padding:0;margin:0}.ticks li{position:relative;padding:6px 0 6px 26px}
.ticks li:before{content:"";position:absolute;left:4px;top:13px;width:10px;height:10px;border-radius:50%;background:var(--good)}
.callout{background:var(--soft);border-radius:16px;padding:20px}
.resource{margin-top:16px}
.loop{list-style:none;padding:0;margin:0;display:grid;grid-template-columns:repeat(5,1fr);gap:12px}
.loop li{background:var(--page);border:1px solid var(--border);border-radius:20px;padding:16px;border-top:4px solid var(--blue)}
.loop li:nth-child(2){border-top-color:var(--support)}.loop li:nth-child(3){border-top-color:var(--coral)}.loop li:nth-child(4){border-top-color:var(--good)}.loop li:nth-child(5){border-top-color:var(--ink)}
.loop p{margin:6px 0 0;font-size:14px;color:var(--muted)}.step{display:block;font-size:12px;color:var(--muted)}
.days{display:grid;grid-template-columns:repeat(3,1fr);gap:16px}
.day{background:var(--surface);border:1px solid var(--border);border-radius:20px;padding:20px}
.day-tag{display:inline-block;margin:0;padding:4px 12px;border-radius:999px;background:var(--mint);font-weight:700;font-size:13px}
.day:nth-child(2) .day-tag{background:var(--blue-soft)}.day:nth-child(3) .day-tag{background:var(--soft)}
.proofs{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:16px}
.proof{background:var(--page);border:1px solid var(--border);border-radius:20px;padding:20px;display:flex;flex-direction:column;gap:6px}
.proof h3{margin:0;font-size:18px}.proof p{margin:0;color:var(--muted);font-size:14px}.proof-name{font-weight:800;color:var(--blue-strong)!important;text-transform:uppercase;font-size:12px!important;letter-spacing:.08em}
.plan-card{background:var(--ink);color:#fff;border-radius:20px;padding:24px}.plan-card .eyebrow{color:var(--blue)}.plan-card .big{font:800 36px/1.1 Montserrat,sans-serif;margin:4px 0}
.plan-card .ticks li:before{background:var(--blue)}
.addons{display:grid;gap:12px}.addon{border:1px solid var(--border);border-radius:16px;padding:14px}.addon h3{margin:0 0 4px}.addon p{margin:0;font-size:14px}.price{font-weight:700}
.next{list-style:none;padding:0;margin:0;display:grid;grid-template-columns:repeat(3,1fr);gap:16px;counter-reset:n}
.next li{background:var(--page);border:1px solid var(--border);border-radius:20px;padding:20px;counter-increment:n}
.next li:before{content:counter(n);display:inline-grid;place-items:center;width:32px;height:32px;border-radius:50%;background:var(--coral);color:#fff;font-weight:800;margin-bottom:8px}
@media (max-width:820px){.two,.days,.next{grid-template-columns:1fr}.loop{grid-template-columns:1fr 1fr}.slide{min-height:auto}.slide-inner{padding:72px 16px 24px}.bar-row{grid-template-columns:110px 1fr 50px}.topbar nav{display:none}.tbl{font-size:13px}}
@media (prefers-reduced-motion:reduce){html{scroll-behavior:auto}}
"""


def render_intelligence_v2_deck(
    *,
    payload: dict[str, Any],
    title: str,
    settings: Any,
    generated_on: date,
    monogram_html: str = "",
    favicon_link: str = "",
    public_url: str = "",
    preview_image_url: str = "",
    share_preview: dict[str, str] | None = None,
    link_check: dict[str, Any] | None = None,
) -> str:
    share_preview = share_preview or {}
    target = dict(payload.get("target") or {})
    brand = str(target.get("brand_name") or "").strip() or "Your brand"
    urls = resolve_cta_urls(settings)
    findings = build_findings(payload)
    slides = [
        _cover(payload, brand, findings, urls, generated_on),
        _snapshot(payload, link_check, urls),
        _market(payload, urls),
        _search(payload, urls),
        _method(urls),
        _plan(payload, urls),
        _proof(payload, urls),
        _offer(payload, urls),
        _next(urls, generated_on),
    ]
    nav = "".join(
        f"<a href='#{key}'>{label}</a>"
        for key, label in (("cover", "Summary"), ("snapshot", "Snapshot"), ("market", "Market"), ("search", "Search"), ("method", "How it works"), ("plan", "14 days"), ("proof", "Proof"), ("offer", "Pricing"), ("next", "Next steps"))
    )
    description = str(share_preview.get("description") or f"{brand} brand analysis by Anata Intelligence.")
    og_title = share_preview.get("title") or title
    meta = (
        f'<meta property="og:title" content="{_e(og_title)}">'
        f'<meta property="og:description" content="{_e(description)}">'
        '<meta property="og:type" content="website">'
        + (f'<meta property="og:url" content="{_e(public_url)}">' if public_url else "")
        + (
            f'<meta property="og:image" content="{_e(preview_image_url)}">'
            '<meta property="og:image:width" content="1200"><meta property="og:image:height" content="630">'
            '<meta name="twitter:card" content="summary_large_image">'
            if preview_image_url
            else ""
        )
    )
    document = (
        "<!doctype html><html lang='en' data-template-version='intelligence_v2'><head><meta charset='utf-8'>"
        "<meta name='viewport' content='width=device-width,initial-scale=1'>"
        f"<title>{_e(title)}</title><meta name='description' content='{_e(description)}'>{meta}{favicon_link}"
        "<link rel='preconnect' href='https://fonts.googleapis.com'>"
        "<link href='https://fonts.googleapis.com/css2?family=Inter:wght@400;600&family=Montserrat:wght@600;700;800&display=swap' rel='stylesheet'>"
        f"<style>{_CSS}</style></head><body>"
        f"<header class='topbar'><span class='brand'>{monogram_html}anata.</span><nav>{nav}</nav>"
        f"<a class='cta primary' href='{_e(urls['trial'])}' target='_blank' rel='noreferrer'>Start free trial</a></header>"
        f"<main>{''.join(slides)}</main>{_engagement_script()}</body></html>"
    )
    return _DASHES.sub("-", document)
