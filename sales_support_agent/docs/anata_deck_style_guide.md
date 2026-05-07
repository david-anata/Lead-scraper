# Anata Sales Deck — Style Guide

A portable reference for the Amazon-first Anata sales deck. Hand to a designer (Claude Design or human) so they can iterate on the visual treatment without re-deriving every token.

This guide is descriptive — it documents what's actually shipping in production at `agent.anatainc.com`, not aspirational. Source of truth: [`shared/anata_brand/style.css`](../../shared/anata_brand/style.css), [`shared/anata_brand/tokens.json`](../../shared/anata_brand/tokens.json), and the renderer in [`sales_support_agent/services/deck/`](../services/deck/).

---

## 1. Brand identity

### Color tokens

| Token | Hex | Use |
|---|---|---|
| `--anata-ink` | `#1d2d44` | Body text, headlines |
| `--anata-ink-soft` | `#314664` | Secondary text, table cells |
| `--anata-sky` | `#85bbda` | Eyebrows, accent links, KPI labels |
| `--anata-sky-deep` | `#4f84c4` | CTA backgrounds, primary buttons |
| `--anata-sand` | `#bfa889` | Tags, directional pills, secondary borders |
| `--anata-sand-soft` | `#f7f3ec` | Page background gradient stop |
| `--anata-paper` | `#fffdf9` | Slide background |
| `--anata-line` | `rgba(29,45,68,0.12)` | Card borders, dividers |
| `--anata-shadow` | `rgba(29,45,68,0.10)` | Slide drop shadow |
| `--anata-muted` | `#6b7688` | Secondary copy, captions |

The body has a soft `linear-gradient(180deg, #eef5fb 0%, #f7f3ec 100%)` background so paper-colored slides sit above a sky/sand wash.

### Cover gradient

The cover slide breaks from paper-colored slides — it's a deep navy with a sky bloom in the upper-right:

```css
background:
  radial-gradient(circle at top right, rgba(133,187,218,0.28), transparent 34%),
  linear-gradient(135deg, #10233d 0%, #294566 100%);
color: white;
```

### Typography

Single family across the deck — **Inter** with `"Segoe UI", sans-serif` fallback. Loaded from Google Fonts as `Inter:wght@400;500;600;700` plus `Montserrat:wght@700;800` for the dashboard wordmark only.

| Selector | Size | Weight | Notes |
|---|---|---|---|
| `h1` (cover headline) | `clamp(36px, 5vw, 62px)` | 800 | Line-height `0.98`; only used on cover |
| `h2` (slide title) | 30px | 700 | After every `.eyebrow` |
| `h3` (card heading) | 18px | 700 | Inside `.dashboard-card`, `.recommendation-card`, etc. |
| `.lead` (cover sub-copy) | 18px | 400 | `max-width: 760px`, line-height `1.6` |
| `.eyebrow` | 12px | 800 | All caps, letter-spacing `0.18em`, sky color |
| `.muted` | inherits | inherits | Color `--anata-muted` |
| Card body copy | 13–14px | 400 | |
| Small/legend | 11–12px | 500 | |

### Radii

`--radius-slide: 28px`, `--radius-card: 22px`, `--radius-pill: 999px`. Smaller cards inside the dashboard use `12–14px`. The brand monogram is square with `border-radius: 18px`.

### Logo

`shared/anata_brand/assets/`:
- `monogram.png` — square mark used in the deck toolbar
- `wordmark.png`, `anata wordmark logo - black.png` — full lockups
- `agent-favicon.png` — admin console favicon

The deck embeds the monogram inline as base64 in [`brand_assets.py`](../services/deck/brand_assets.py) (cached on first load). It's never linked externally — the deck must be self-contained for offline review.

---

## 2. Page geometry & print

### On screen

```css
.deck      { width: min(1200px, calc(100vw - 40px)); margin: 0 auto; }
.slide     { padding: 32px; border-radius: 28px; }
```

### Print (Letter / 0.5in margin)

```css
@page          { size: Letter; margin: 0.5in; }
.slide         { padding: 0.18in 0; page-break-inside: avoid; }
.slide + .slide { border-top: 1px solid var(--anata-line); padding-top: 0.22in; }
```

**Hard page breaks** are reserved for slides that genuinely need their own page:
- `.slide-cover` → `page-break-after: always` (always opens on its own)
- `.slide.growth-plan-slide` → KPIs + ramp + funnel + 5 channel cards
- `.slide.slide-conversion` → visual proof + recommendations
- `.slide.slide-offers` → offer cards + CTA + plan grid

Everything else flows naturally; small slides (Support-term demand, Case studies, Next action) double up with their neighbors and a 1px rule separates them.

### Print-specific overrides (the gotchas)

These are the surprises that bit us repeatedly during print iteration. Designer should mind them:

- `@media (max-width: 960px)` collapses many grids to single column. Letter at 96dpi is ~816px wide → matches that breakpoint and triggers the mobile layout in print. We override grids back to multi-column inside `@media print`:
  - `.gallery-grid → 4 col`
  - `.dashboard-grid → 2 col`
  - `.metric-grid → 3 col`
  - `.service-grid → 2 col`
- `.channel-grid` (Growth Plan) prints **1-col, full Letter width** so Campaign + Why prose doesn't get sliced.
- `.hero-media` cover image is capped to `max-height: 1.8in` in print to keep the cover on one page.
- `iframe`s are hidden in print — Canva embeds render as blank rectangles otherwise.
- `.growth-funnel-panel` defaults to the steady-state panel in print regardless of which tab the user clicked on screen (`[data-default="1"]` rule).

---

## 3. Information architecture

The deck always renders 8 slides in this order. Sections are sourced from a single dataset built in [`services/deck/dataset.py`](../services/deck/dataset.py).

| # | Slide | Heavy? | What it shows |
|---|---|---|---|
| 1 | **Cover** | yes | Brand + target listing + niche keyword + channels |
| 2 | **Market summary** | medium | 6-tile metric strip, niche table (ASIN/Brand toggle), 3 distribution donuts |
| 3 | **Target listing opportunities** | medium | Side-by-side comparison table + What's working / What's missing |
| 4 | **Competitor landscape** | medium | Top-10 table: price, revenue, share, BSR, rating, reviews |
| 5 | **Search behavior** | medium | Keyword metrics strip + title coverage + bullet coverage + top opportunities + ranking path + support-term cloud |
| 6 | **Growth plan synopsis** | yes | Closing the gap: 3-tile KPI strip, 5-tile growth ramp (Today + 4 phases), tabbed funnel SVG, 5 channel cards, total spend summary, methodology disclosure |
| 7 | **Conversion & PDP** | yes | Visual proof side-by-side + CRO recs + Creative recs |
| 8 | **Service offerings** | medium | 5-channel tabbed panel (Amazon / TikTok Shop / Shopify / 3PL / Shipping OS) |
| 9 | **Proposed offers** | yes | 1–3 offer cards + CTA + Why now + What happens next |

### The "Story" companion

Every deck also generates a markdown read-aloud version at `/decks/{slug}/{run_id}/{token}/story` (HTML viewer) and `/story.md` (download). Same 7 sections, optimized for reading on a sales call. Mirrors the deck's growth-ramp numbers and the 4-phase implementation roadmap with 15 cited sources.

---

## 4. Component library

### `.slide`

```html
<section class="slide [slide-cover|slide-conversion|slide-offers|growth-plan-slide]">
  <div class="slide-head">
    <div>
      <p class="eyebrow">Section name</p>
      <h2>Slide headline</h2>
    </div>
    <p class="muted">Optional one-line caption.</p>
  </div>
  <!-- content -->
</section>
```

`.slide-head` has `display: flex; justify-content: space-between` — eyebrow+h2 left, caption right.

### `.metric-grid` (the metric strip)

3-column grid of `.metric-card` tiles. Used on Market Summary and Search Behavior.

```html
<div class="metric-grid">
  <article class="metric-card">
    <span>Label small caps</span>
    <strong>$6,209,993.52</strong>
    <small>Avg per listing $101,803.17</small>
  </article>
  ...
</div>
```

Sizes: label 11px / 800 caps, strong 28px / 800 ink, small 12px muted. Card is `.metric-card { background: white; border: 1px solid line; border-radius: 14px; padding: 18px 20px; }`.

### `.dashboard-grid` (2-up layout)

```html
<div class="dashboard-grid market-summary-grid">
  <article class="dashboard-card niche-table-card">...</article>
  <div class="donut-grid compact-donut-grid">
    <!-- 1-3 distribution-card donuts -->
  </div>
</div>
```

`.market-summary-grid` overrides to `2fr 1fr` ratio so the table column is wider than the donut column.

### `.recommendation-card`

Used for "What's working / What's missing", CRO recs, Creative recs.

```html
<div class="two-col split-top">
  <div class="recommendation-card">
    <h3>Heading <small class="help-badge">?</small></h3>
    <ul>
      <li><strong>Open:</strong> body…</li>
    </ul>
  </div>
</div>
```

`.help-badge` is a 14px circular tooltip dot (sand-colored). Hidden in print.

### `.distribution-card` (donuts)

Each donut is an SVG `viewBox="0 0 100 100"`. The donut card sits in `.compact-donut-grid` which is single-column so 3 donuts stack vertically next to a wider primary card.

### Growth Plan components (dedicated module)

These live in [`services/deck/growth_plan.py`](../services/deck/growth_plan.py) and are styled under `.growth-plan-slide` in the stylesheet.

#### `.growth-kpis` — 3-tile KPI strip

```
| Current sessions   | Goal sessions    | Sessions delta   |
| 0                  | 69,204           | 69,204           |
| = 0 units ÷ 15.0%  |                  |                  |
```

Each `.growth-kpi` is sand-tinted, 14px label / 28px strong / 11px footnote.

#### `.growth-ramp` — per-phase ramp visualization (PR28/PR29)

The "Growth path — how sessions ramp from today to goal" section. 5-tile horizontal grid (Today + Phase 1–4), each tile is a `<li class="growth-ramp-step">` with:
- Step num + label + window ("Phase 1 · Foundation · Days 0–14")
- Cumulative sessions ("10,381 sessions") + this-phase delta ("+10,381 this phase")
- Progress bar (sky-deep fill on `--anata-line` track)
- Percent of goal
- New-this-phase channel labels

The first tile is `.is-today` with a dashed border. Bar fills are `--anata-sky-deep` on `--anata-line`. Each tile is print-`break-inside: avoid`.

#### `.growth-funnel` (tabbed)

Tabs across the top, one per phase. Default-active is the last (steady state). Each panel contains:
- Caption with phase summary + "New this phase: …"
- SVG funnel: 5 traffic boxes → PDP visits → units sold → revenue, with curved flow paths.

In print, all panels except `[data-default="1"]` are forced hidden via `display: none !important`.

Funnel SVG palette:
- Active boxes: `#4f84c4` fill, white stroke
- Inactive boxes: 18% sand fill, sand stroke
- Flow paths: `#85bbda` at 0.45 opacity
- PDP/Units/Revenue boxes: deep navy `#10233d` with sky-blue accents

#### `.channel-card` (5 cards in `.channel-grid`)

```html
<article class="channel-card channel-organic">
  <div class="card-head">
    <h3>Organic</h3>
    <span class="card-mix">25% of mix</span>
  </div>
  <div class="card-cost">SEO investment, no paid spend</div>
  <div class="card-outcome"><strong>17,301</strong> sessions → <strong>2,595</strong> units → <strong>$116,749</strong> / mo</div>
  <div class="card-block">
    <span class="card-block-label">Campaign</span>
    <p>Listing optimization (title, bullets, A+ content)…</p>
  </div>
  <div class="card-block">
    <span class="card-block-label">Why this channel</span>
    <p>Compounding equity. Every organic session won here is sticky…</p>
  </div>
  <small class="card-source">No paid spend — investment in title/bullet/imagery work</small>
  <small class="directional">Directional — calibrate with first-party data</small>  <!-- if applicable -->
</article>
```

Each channel gets a left border in a unique color:

| Channel | Accent color |
|---|---|
| `channel-organic` | `--anata-sand` (#bfa889) |
| `channel-on_channel_paid` | `--anata-sky-deep` (#4f84c4) |
| `channel-off_channel_paid` | `--anata-sky` (#85bbda) |
| `channel-affiliate` | `#d28b8b` (rose) |
| `channel-retargeting` | `#8bb59a` (sage) |

### `.offer-card` (Proposed Offers slide)

```html
<article class="offer-card">
  <h3>Channel management</h3>
  <p>Full-service Amazon marketing and operations support…</p>
  <dl>
    <dt>Monthly retainer fee</dt><dd>$3,000</dd>
    <dt>Commission on growth</dt><dd>5%</dd>
    <dt>Commission baseline</dt><dd>$10,000</dd>
  </dl>
  <p class="bonus">+TikTok Shop Support</p>
  <a class="get-started" href="…">Get started</a>
</article>
```

### `.plan-card` (Why now / What happens next)

3-card row, the first is `.plan-card-cta` with sky-deep background and white CTA button (`.plan-link`).

---

## 5. Per-channel ramp model (anchor numbers)

This is the math the Growth Plan slide and the Story markdown both use. Defaults are hard-coded but defensible — every curve is grounded in published industry timelines.

| Channel | Phase 1 (D0–14) | Phase 2 (W3–8) | Phase 3 (W8–16) | Phase 4 (M4+) | Source |
|---|---|---|---|---|---|
| Organic SEO | 10% | 40% | 75% | 100% | Helium 10 listing-optimization guide; BeBold PPC ramp-up |
| On-channel paid (SP/SB) | 50% | 85% | 100% | 100% | Amazon Ads first-30-days SP tips; Pacvue Q1 2026 |
| Off-channel paid (Meta/TikTok) | 0% | 50% | 95% | 100% | Amazon Attribution guide; Digital Applied 2026 |
| Affiliate (TikTok creators) | 0% | 0% | 40% | 100% | Later influencer-campaign timeline; Canopy Mgmt TikTok Shop 2026 |
| Retargeting / LTV | 0% | 0% | 0% | 100% | Amazon Sell Brand-Tailored Promotions; Sequence Commerce 2026 |

Cumulative sessions at end-of-phase = `sum(channel.sessions × ramp_pct[N])` across all channels. Source of truth: `cumulative_sessions_at_phase()` in [`growth_plan.py`](../services/deck/growth_plan.py).

---

## 6. Iconography & images

- **Brand monogram** (toolbar, cover): `shared/anata_brand/assets/monogram.png`, displayed at 56×56 / `border-radius: 18px`.
- **Target/competitor product images**: Amazon product image URLs scraped from PDP. Comparison thumbs are 160×160, gallery thumbs cap at 220px tall on screen, 140px in print.
- **No-product-image fallback**: `shared/anata_brand/assets/no-product-image-available.png` — used when a competitor row has no scrapable image.
- **Brand colors on images**: never overlay; always show on white card with `--anata-line` border.

---

## 7. What we know works (don't break)

- Single Inter family — no second display face. The cover already does enough work with size + weight.
- `0.5in` print margins are non-negotiable; `0.25in` was tried and clipped the niche table.
- The cover gradient is the ONLY non-paper slide background — every other slide is `--anata-paper` for paper-document continuity.
- `border-radius: 28px` on slides reads as soft and confident; `12–14px` on inner cards keeps the hierarchy clear.
- Three-column metric strips (the "stat tiles") are the deck's signature pattern. Designer should preserve the `Label / Strong / Small` rhythm.
- The growth-ramp progress bars (sky-deep on line color) are the only horizontal bar treatment in the deck. Don't introduce a second bar style elsewhere.

## 8. What we know is rough (open invitations)

- The cover image (target product photo) is bare — no treatment, no shadow, just a square. Could use a subtle paper-card backing or a duotone treatment.
- The Conversion & PDP "side-by-side" panel is two equal cards with an arrow → between them. The arrow is small and the cards have no visual hierarchy showing which is "your listing" vs "the benchmark". A treatment that makes the prospect's listing feel like the subject (warm border?) and the benchmark feel like a target (cool border?) would help.
- The funnel SVG is functional but generic. A designer could improve the way "active" vs "not yet active" channels are visualized — currently it's just opacity + outline.
- The methodology footnote (`<details>`) on the Growth Plan slide is open in print but its `<summary>` is hidden. Looks fine, but a stronger "Sources" headline would make the printed version more credible.
- Channel-card accent borders are functional; could be more brand-cohesive (e.g., subtle gradient strokes instead of flat colors).

## 9. How the deck is built (so designer knows what's mutable)

- HTML is generated server-side by [`services/deck/service.py`](../services/deck/service.py). No client-side templating.
- The full stylesheet is **inlined** in every deck — they're shareable URLs, no external CSS requests.
- Every component above is rendered by a small Python helper in [`services/deck/`](../services/deck/) — `growth_plan.py` for the entire growth section, `rendering.py` for cards/donuts, `service.py` for the slide skeleton.
- Print is the ONLY constrained output — screen is generous and shareable. If a treatment looks great on screen but breaks in print, print wins.

---

## 10. File map for designers

```
shared/anata_brand/
├── style.css           ← single source of truth, 2,239 lines
├── tokens.json         ← color + typography + radius tokens
├── components.md       ← this file's older sibling (component patterns)
├── README.md           ← brand README
└── assets/
    ├── monogram.png
    ├── wordmark.png
    ├── anata wordmark logo - black.png
    ├── agent-favicon.png
    └── no-product-image-available.png

sales_support_agent/services/deck/
├── service.py          ← slide skeleton + HTML scaffolding
├── growth_plan.py      ← Growth Plan slide (math + render)
├── rendering.py        ← cards, donuts, comparison panels
├── dataset.py          ← data assembly from Helium 10 / Amazon scrape
├── story.py            ← markdown story companion
└── brand_assets.py     ← inline-base64 monogram + cached stylesheet
```

To preview a deck locally:

```bash
uvicorn main:app --port 8010 --reload
# Open http://localhost:8010/admin/sales-decks
# Generate against any Xray CSV + ASIN to see the live treatment
```

---

*Last updated: PR30 (smart pagination). When a designer ships a refresh, append a "What changed" note here so the next iteration starts with full context.*
