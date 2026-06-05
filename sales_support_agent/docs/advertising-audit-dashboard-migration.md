# Advertising Audit → dashboard.anatainc.com — migration spec

## Purpose
The **Advertising > Audit** tool on agent.anatainc.com is a **mockup of the logic** for the
production tool on **dashboard.anatainc.com**. Its job: let an **account manager generate a
per-brand "burn-list" workbook** in one step — the same deliverable as the hand-made Zantrex
growth sheet that validated this work.

The web page is intentionally thin: it **runs an audit** and shows a **history** of runs
(brand · date/time · sales · TACoS · downloads). The analysis is **not** shown on the page — it
lives entirely in the downloadable workbook + Amazon bulk apply-sheet.

## Output (unchanged between mockup and dashboard)
1. **Growth-plan workbook** (`.xlsx`, 7 tabs): Exec Brief · Burn List · ASIN Scorecard ·
   Campaign Actions · Negatives to Add · Revenue Bridge · Data Requests.
2. **Amazon bulk apply-sheet** (`.xlsx`): create-negative + create-keyword rows in Amazon's
   official template, upload-ready (Ads Console → Bulk operations → Upload).

## Inputs — what they are, and how the user supplies them

| Input | What it is | Mockup (agent) — how the user inputs it | Dashboard (live) — how it should work |
|---|---|---|---|
| **Ads performance** | Search-term, Advertised-product, Targeting, Ad-group, Campaign reports (SP/SB/SD). New Amazon reporting-console columns: `Campaign name`, `Ad group name`, entity column, `Impressions`, `Clicks`, `Total cost`, `Purchases`, `Sales`, `Units sold`, + `Campaign ID` / `Ad group ID` (wrapped `="..."`). | Upload CSV/XLSX; auto-detected & routed by header content. | **Amazon Ads API** pull (SP/SB/SD) for the selected brand + **date-range picker**. Entity IDs come from the API directly. |
| **Business Report** | Detail Page Sales & Traffic **By Child Item**: `(Child) ASIN`, sessions, units, `Unit Session Percentage` (CVR), Buy Box %, `Ordered Product Sales`. | Upload CSV. | **SP-API** sales & traffic report for the brand's ASINs. |
| **Per-ASIN COGS** | Landed cost per unit (COGS + FBA + referral + freight). Drives **true break-even ACoS**. | Upload CSV (`ASIN, COGS[, FBA Fee, Referral Fee]`) **or** an XLSX margin sheet keyed by product name → ASINs matched by name+size (**approximate**, persisted in `kv_store`). | **Authoritative per-ASIN cost table**: entered/stored per product (or pulled from a cost system). No name-matching guesswork. |
| **Goals** | Revenue / ACoS / TACoS / units targets the workbook measures against. | One **global** form, saved (`ad_goals`). | **Per-brand** goals (each brand has its own targets) — the flexibility the mockup intentionally omits. |
| **Brand focus** | Which brand to scope the audit to. Brand campaigns are often named by **ASIN**, not the brand word, so scoping is **ASIN-aware** (brand ASINs from the Business Report). | Text field + detected-brand chips. | **Brand selector** (account → its brands); brand→ASIN set known from catalog. |
| **External spend** | Off-Amazon marketing (Meta / TikTok / influencer) for **blended TACoS**. | Manual repeatable rows (channel/label/amount) or CSV. | Channel integrations or manual entry per brand/period. |

## Reusable logic to port (the mockup's value)
All under `sales_support_agent/services/advertising/`:
- `normalizers.py` — tolerant column mapping incl. the new reporting-console format, ID unwrapping, COGS (CSV/XLSX + name→ASIN).
- `engine.py` — ACoS / TACoS / **blended TACoS**, per-ASIN-type total selection, rules (negatives, bid down/up, keyword harvest), ranking.
- `brand.py` — ASIN-aware brand scoping + brand detection.
- `deliverable.py` — the 7-tab workbook (incl. break-even ACoS when COGS present).
- `bulk_sheets.py` — `build_apply_sheet`: populates Amazon's bundled template from report entity IDs.

## Known gaps the dashboard must close
1. **Authoritative COGS** — the mockup approximates from a margin sheet by name+size match. The dashboard needs exact per-ASIN cost so break-even verdicts ("cut to break-even" vs "scale") are trustworthy.
2. **Bid *updates* on existing keywords** — needs **Keyword IDs**, only in a Targeting report / the Ads API. The mockup only has Campaign/Ad-Group IDs from the reports, so its apply-sheet does **negatives + new-keyword harvests**, not existing-keyword bid changes. The dashboard (with API entity IDs) can add bid-update rows — or apply changes directly via the Ads API instead of a bulk sheet.
3. **Per-brand goals** (see table).
4. **DSP** — DSP reports (often XLSX) are ignored in the mockup; DSP spend isn't in blended TACoS yet.

## Cadence
Mockup: manual run (upload → run). Dashboard: on-demand per brand, and a weekly scheduled run is a thin add — the schema already carries `week_start`/`week_end` and `storage.get_prior_run()` supports week-over-week deltas.

## Transfer-readiness — what to harden in `agent` so the dashboard is a swap, not a rewrite
Priority order. Each item makes the mockup more correct AND closer to a 1:1 port.

1. **Keep the core pure; isolate the I/O boundary.** The value lives in `normalizers → engine → brand → deliverable → bulk_sheets`, all of which speak the `schema.py` dataclasses (`AdRow`, `SalesRow`, `Goals`, COGS dict). The dashboard replaces *only* the intake: Amazon Ads/SP-API → map responses into those exact dataclasses → call the identical engine. **Action:** treat `schema.py` as the frozen contract; write one `adapter` module per source (CSV today, API tomorrow) so nothing else changes.
2. **Per-brand goals + per-brand config.** Mockup uses one global `Goals` and hardcoded engine `Thresholds` (bid factors, wasted-spend floor, target ACoS). **Action:** make goals + thresholds account/brand-scoped settings the dashboard owns.
3. **Authoritative per-ASIN COGS.** Replace the margin-sheet name-match (approximate) with a stored cost table keyed by ASIN. #1 lever for profit-true break-even. The COGS Mapping tab already shows what to trust/override.
4. **Apply-via-API (write path).** Mockup emits a bulk sheet for manual upload. With Ads API write access the dashboard can apply changes directly — behind a **dry-run + approval gate** and with the same **cross-brand safety exclusion** (`mixed_campaigns`). Never auto-apply without review.
5. **Safety/scoping as a first-class, visible output.** The hard-won multi-brand logic (ASIN-aware scope; exclude mixed/cross-brand campaigns from edits; count their spend in totals) must surface as a "what was scoped / excluded / why" panel. Critical for trust at scale; `summary["excluded_mixed_campaigns"]` is the seed.
6. **Entity IDs from the API.** The bulk-file parse exists only to recover Keyword IDs (for bid changes). The API returns all entity IDs natively → drop the 17.5MB parse, and extend bid changes to **SB/SD/DSP** (mockup is SP-only).
7. **Observability.** Log per run: rows ingested, brand ASINs, campaigns scoped/excluded, recs by type, rows applied. Needed for auditability when this drives real spend.

The migration is a **swap of #1's adapter**; #2–#7 are the hardening that makes it safe and complete.

## Advertising Engine spec — decisions from the brand review (2026-06-05)
David's answers to the "poke holes" review, organized. (D) = dashboard; (M) = makes sense in manual `agent` too.

**A. Data sync** (Q1,2,3,14) — (D) The dashboard auto-syncs the **attribution window** and **date ranges** across all data via the API, killing the apples-to-oranges problem. **DSP**: the seat isn't always available → optional **manual upload**, loosely treated like an external channel (caveat: not truly external — TBD). (M) The manual flow CAN mix windows on upload → **warn when uploaded reports span different date windows**.

**B. Strategy layer — objectives & phases** (Q6,7,8,18,20) — (D) The engine must be **objective- and phase-aware**. Brands set a **phase** (launch / grow / defend / harvest) in advertising settings; campaigns carry an **objective**; bid targets, placement moves, and harvest behavior flow from these. Flat 30% ACoS is the default, but **loss-leader / new-to-brand** products get their own (lower-ROAS-acceptable) targets — measure **new-to-brand** (the new-console reports already carry `Purchases/Sales (new to brand)` columns). **Harvest → also negate the source term** as a brand-set preference (avoid paying twice).

**C. Significance & settling** (Q5,10) — (D) Only act on **statistically meaningful** data; add a **cooldown** so a just-changed entity must accrue data before the next change (anti-flip-flop) — pushback if not enough movement, proceed if there is. (M) Already has a min-clicks gate; surface "skipped N low-data keywords."

**D. Human-in-control + overrides** (Q9,17,21) — (D) The user always **directs** the AI. Settings: **per-period overrides** (keep bidding up through Prime Day / a user-set sale window to hold seasonal traffic); **budget** suggestions the user can change; **bid-up only when there's impression-share headroom** (needs *lost impression share* — a separate API call, NOT in the bulk file / standard reports).

**E. Closed loop, memory & rollback** (Q22,23) — (D) **Log every advertising change**; learn from outcomes (did it work?); an in-platform AI agent can query the history ("what strategy worked last year?"); changes are emailed/outlined; **90-day "time-machine" rollback** (Google-Docs-style undo).

**F. Campaign BUILDER** (Q15) — (D, net-new) Beyond optimizing existing campaigns, the dashboard **creates campaigns by ASIN × objective**. Still to build.

**G. Profit** (Q11,12,13) — (D) Break-even at **list price** for simplicity (the P&L process reviews true net; list price ≈ margins sheet `Avg Sale Price`). COGS is **user-uploaded** (user-error if wrong) → **export the active COGS + ASIN mapping** for frequent review (the workbook's COGS Mapping tab seeds this). Referral/FBA fees handled **separately** in the dashboard's analysis, not folded into COGS.

**H. Safety / coverage** (Q14,15,16) — (D) Full **API coverage** removes partial-upload gaps; add **our own tag layer** for brand-attribution safety; always review **per-campaign AND per-listing**. (M) Surface the **scope summary** (brand ASIN count, campaigns excluded) so partial-data risk is visible.

**Net-new dashboard capabilities this implies:** advertising **settings** (phase, objectives, targets, override windows, budgets), a **campaign builder** (ASIN × objective), a **change log + outcome learning + 90-day rollback**, and **lost-impression-share / NTB** data pulls.

---

# Dashboard UI/UX spec — the frontend layer (2026-06-05)

Everything above is the **backend** (the ported engine, the API adapters, the settings/history stores). This section is the **frontend**: the Advertising section's sub-sections and the widgets in each. The two sides meet at `schema.py`'s dataclasses + the run `summary`/`recommendations` objects — the frontend renders those; the backend produces them.

## Frontend ⇄ backend split (at a glance)

| Layer | Owns | Source |
|---|---|---|
| **Backend** | Ingest (API adapters → `schema.py`), engine (ACoS/TACoS/blended, break-even, rules, ranking), brand scoping/safety, settings store (goals/phase/objectives/thresholds), change log + outcomes + rollback, campaign create via API. | Ported `services/advertising/*` + new dashboard services. |
| **Frontend** | The 8 sub-sections below; renders `summary`, `recommendations`, scorecards, charts; collects approvals/overrides; never computes — it only displays backend output and posts user intent back. | New dashboard UI. |

## Cross-cutting UI conventions
- **One action object, approvable from many surfaces:** a recommendation is a single stateful object. The AM can approve/edit/dismiss it from the **Burn List** *or* from the relevant table (Keywords, Products, Campaigns) — changing it anywhere updates it everywhere (shared state, no duplicate/conflicting copies). The Burn List is the fast review-everything view; the tables are the in-context view. Same object, many windows onto it.
- **Suggested-action columns everywhere, applied in one place:** Products / Keywords / Campaigns each show the engine's suggested action inline *and* let you approve it there. The single place a batch is **applied** (the API write) is **Pending Changes** — for manual + supervised flows — so the write is always one deliberate, reviewable act.
- **Trust is always visible:** every brand view carries the scope strip (N ASINs scoped · M cross-brand campaigns excluded · data fresh to {date}) and an amber banner on data-window mismatch.
- **Charting lib:** **match the dashboard's existing standard** (see open decision #2 — recorded as a question for the dashboard team, not assumed here).
- **Tables that grow** (Burn List, Campaigns, Keywords, Search Terms) paginate + server-filter; **bounded tables** (Products/ASIN, COGS) render in full.

## Manual vs automated — how a change reaches Amazon
Same engine, same recommendations; the **autonomy level** (per brand, settable per action-type in Settings → Automation & Control) decides what crosses the apply-gate without a human.

| | **Manual** | **Supervised auto** | **Full auto** |
|---|---|---|---|
| Triggered by | AM clicks Run | Scheduled job (e.g. weekly) | Scheduled job |
| Who approves | Human — in Burn List or any relevant table | Engine pre-approves within guardrails; human gives one click (or "apply unless I object by {date}") | Engine, within guardrails |
| Where it applies | Pending Changes | Pending Changes (pre-filled, pre-approved batch) | Applied directly via Ads API |
| Human sees it | **before** apply | **before** apply (staged) | **after** apply, in History (rollback available) |

**Surfaces:** Manual → Burn List/table → **Pending Changes**. Supervised → cron fills **Pending Changes** with a pre-approved batch → one click. Full auto → cron applies → writes **History**; Burn List shows the changes as already-applied (read-only).

**Automation guardrails (the safety contract — automation fires only if ALL pass):** significance gate + cooldown (Theme C) · cross-brand-safe, never touches mixed campaigns (Theme H) · within per-action $/% caps · on the brand's **action-type allowlist** (e.g. auto-apply negatives + small bid-downs; *always ask* on bid-ups, budgets, new campaigns) · phase/objective-aware (won't auto-cut a loss-leader / new-to-brand) · not inside a seasonal override window (Theme D). Any failure routes the change to the **manual** queue instead of applying. Every auto-applied change is logged to the change log + outcomes (Theme E) and is rollback-eligible for 90 days.
*Backend:* a scheduled run reuses the identical engine, then routes results by autonomy level + guardrails; the frontend only renders the resulting queue/log.

## Sub-sections & widgets

**1. Overview** — glanceable Exec Brief.
- KPI row (6): Revenue (vs goal), Ad Spend, Blended TACoS (vs target), ACoS, Units, Target-Ad-Spend headroom. Each: value · ▲▼ delta vs prior · sparkline.
- Chart A: Sales vs Ad Spend (dual-axis, weekly; toggle organic vs ad-attributed). Chart B: TACoS trend with target reference band.
- Scope strip (trust panel) + top-3-actions teaser (→ Burn List). No full table.
- *Backend:* run `summary` + prior-run deltas + time-series.

**2. Burn List** — weekly action workspace. Table **grows**.
- Filters: severity, category, campaign, ASIN, status, $-impact min, search. Summary chips recompute on filter.
- Cols: ☑ · severity · action · entity(+ASIN) · why · current→new · $ impact (default sort desc) · objective/phase tag · status · row actions (approve/edit/dismiss).
- Row expand → 8-week entity trend; inline-editable "new bid" (override). Sticky bulk bar: approve/dismiss/send-to-pending.
- *Backend:* `recommendations` (category, severity, $ impact, current/new, entity ids).

**3. Pending Changes / Approvals** — the dry-run gate.
- Accordion by change type (bids/negatives/new keywords/budget); diff table: entity · field · from→to · $ impact · source · cross-brand check.
- Safety banner (0 cross-brand affected, else blocks). Actions: apply-all-approved (confirm modal) · schedule · export bulk sheet · discard.
- *Backend:* applies via Ads API write path + `mixed_campaigns` safety; writes the change log.

**4. Campaigns** — structure + builder.
- Tab A table (**grows**): campaign · type · objective · phase · spend · sales · ACoS · TACoS contrib · budget · util% · status. Row drawer: placement breakdown, ad groups, budget gauge.
- Tab B Campaign Builder (net-new): wizard ASIN(s) → objective → engine-proposed structure → review → create (API, behind approval).
- Chart: budget utilization bars (flag capped campaigns).
- *Backend:* campaign rollups; builder = settings (objective/phase) + engine seed bids + API create.

**5. Products (ASIN Scorecard)** — bounded table.
- Cols: ASIN(+title) · org sales · ad sales · total · sessions · CVR · Buy Box% · ad spend · ACoS · break-even ACoS · verdict · NTB%. Conditional formatting (CVR/ACoS color scales, verdict pills).
- Filters: verdict, ACoS vs break-even, has-COGS. Row expand → ASIN trend. Chart: CVR×ACoS scatter, bubble=spend, quadrant lines.
- *Backend:* `deliverable.py` ASIN Scorecard join + break-even (COGS).

**6. Keywords & Negatives** — three tabs, all **grow**.
- Search Terms: term · campaign · clicks · spend · sales · orders · ACoS · suggested action (harvest/negate/leave).
- Keywords: keyword · match · bid · impressions · clicks · ACoS · impression share · suggested bid (inline edit).
- Negatives: existing + proposed, "also-negate-source" reflected. Each: send selected → Pending Changes.
- *Backend:* search-term + keyword rows; harvest/negative/bid rules; impression-share pull.

**7. History & Outcomes** — closed loop + undo.
- Runs timeline; outcome table (change · applied date · metric before→after · verdict worked/no-change/backfired); AI query box over history; rollback (revert within 90 days, version list); cumulative-impact-vs-baseline chart.
- *Backend:* change log + outcome attribution + 90-day rollback store + history-query agent.

**8. Settings** — forms (+ COGS & objectives-mapping tables).
- Targets & Strategy (goals, phase, objectives, per-product overrides, attribution window) · Decision Rules (bid factors, significance gate, cooldown, harvest behavior) · Seasonality & Overrides · Profit/COGS (editable ASIN→cost table, exportable) · Channels & Data (external spend, integrations, ASIN tag layer) · Automation & Control (autonomy level, approval gates, notifications, rollback retention).
- *Backend:* per-brand settings store; these values feed the engine's `Goals` + `Thresholds` + phase/objective logic.

## Frontend decisions
1. **Approvals placement — RESOLVED.** Approve from *any* surface (Burn List or the relevant Keywords/Products/Campaigns table); it's one synced action object. The single **apply** surface is Pending Changes (manual + supervised). Plus an automation tier (manual / supervised / full-auto) — see "Manual vs automated" above.
2. **Charting library — OPEN (question for the dashboard team).** Match the dashboard's existing chart standard; if none is set, pick one (e.g. Recharts) and standardize. Affects every section. *Carry this question into the dashboard project.*
3. **Suggested-action columns — RESOLVED.** Show the engine's suggested action on Products / Keywords / Campaigns *and* allow approve-in-context there (synced to the same action object). Burn List stays the fast review-all view; Pending Changes stays the single apply gate.
