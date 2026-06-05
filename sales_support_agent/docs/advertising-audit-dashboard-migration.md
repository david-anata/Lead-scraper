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
