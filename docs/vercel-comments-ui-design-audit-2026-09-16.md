# UI design audit and spec addendum

Reviewed September 16, 2026 (Mountain Time). Proposed design; no product code or business records changed.

Companion to [the Vercel feedback spec](vercel-comments-spec-2026-09-16.md). This addendum refines layout and interaction requirements and corrects baseline assumptions using live evidence. It does not replace pricing rules, source-of-truth contracts or the full comment ledger.

## Design direction

Make the next decision obvious through composition: a clear heading, the relevant controls, the working content and its current state. Reduce competing panels, repeated instructions and long introductions. Use Anata's existing typography, colors, components and spacing. Public pages can be expressive; operator pages should remain calm and efficient.

The first design batch should deliver:
1. A pipeline that opens on prospects and their next actions.
2. A pricing workbench with compact fee editing, a readable estimate and one clear save/review/publish sequence.
3. Searchable company/contact selection with creation in context.
4. Public pricing choices near the top, a stronger homepage hero and proof that names the category and measurement period.

## What was inspected

Live browser text and desktop screenshots, measured at approximately 1440 × 675 CSS pixels:
- [Homepage](https://anatainc.com/): hero, service-choice copy and proof content.
- [Pricing](https://anatainc.com/pricing): hero, four pricing paths, marketing panel and comparison guidance.
- [Intelligence](https://anatainc.com/platform/anata-intelligence): hero, capability navigation, pricing text and controls.
- [Fulfillment pipeline](https://agent.anatainc.com/admin/fulfillment/sales): page hierarchy, intake, results, counts, filters and actions.
- [Bloom Towels review](https://agent.anatainc.com/admin/fulfillment/sales/runs/3716/review): readiness, sharing, editable fees, draft preview and publication controls.
- [Create deal](https://agent.anatainc.com/admin/sales/deals/create): form hierarchy and entity selectors.

No forms were submitted, no prices changed, no customer links distributed. Vercel annotations were visible on the public site; their pink text highlighting is not treated as product styling.

Responsive override attempts did not produce reliable requested CSS dimensions: a requested 390px returned 520px in the isolated browser. Mobile screenshots are therefore excluded from confirmed defect findings. Mobile, zoom, keyboard and assistive-technology checks below are implementation acceptance gates, not claimed passes. Animation timing and source-data accuracy were not independently tested.

### Baseline corrections to the earlier spec

- Live fulfillment review already shows private drafts, published revision, pricing approval, a customer preview and optional HubSpot. Preserve these visible improvements. This read-only audit did not prove every persistence or publication boundary.
- Live pipeline says it stays in place and offers manual refresh. The earlier eight-second reload finding is a local-source observation, not evidence of the current production behavior. Reproduce before treating A7 as an unfixed live defect. Targeted status refresh remains a useful enhancement.
- Intelligence already displays Optimization, Health and Strategy in its navigation, a multicolor hero and a free-floating capability visual. Treat those W5 sub-items as observed present; verify details rather than rebuilding them.
- The homepage's rejected supporting sentence and panel remain visible.
- Company and contact creation is still absent from the inspected deal form.

### Established references

Agent DESIGN.md is authoritative: 1320px container, 24px desktop gutters, shared PageHeader, MetricStrip, CommandBar, DataWorkspace and state primitives.

As required by spec-designer, inspected the local Dashboard Overview and Performance references under `../Dashboard/amazon-sp-api-platform/frontend/src/react-app/`:
- `sections/overview/widgets/OverviewKpiRibbon.tsx`: compact metrics, source/readiness context, token-based surface/border/radius, 12px × 16px card padding.
- `sections/overview/widgets/PerformanceTrend.tsx`: a distinct full-width trend section using WidgetShell.
- `sections/performance/widgets/ProductPerformanceTable.tsx`: independent table state, search and 10/25/50 row choices.
- `shared/WidgetShell.tsx`: named section, optional help/actions, local loading/error/empty/unavailable states and retry scope.

These establish grouping and state behavior; do not copy React into the FastAPI application. Adapt through Agent primitives. The live Dashboard reference opened a client/setup flow rather than a populated comparison view, so reference claims above are source-based.

## Ranked findings

Priority reflects user impact and frequency. Confidence describes evidence; effort is relative implementation size, not a time estimate.

| Priority / mapping | Evidence and root cause | Desired change | Frequency / confidence / effort |
| --- | --- | --- | --- |
| P1 · A7 | Pipeline shows generation messages and a full intake form before the prospects. Existing opportunities require scrolling past a creation task. | Open on a compact summary, filters and prospect table; disclose intake from Create rate sheet. | Every pipeline visit / high / medium |
| P1 · A9 | Review places readiness, two sharing panels, handoff and prospect metadata before fee editing. The fee table repeats long explanations and mixes prices, costs, volume and terms. | Separate commercial summary, fee editor, assumptions and preview; put the frequent edit path first. | Every price review / high / medium |
| P1 · A8 | Create Deal exposes a configuration filename, numeric stage IDs and large native company/contact lists. No create controls. | Search by recognizable identity, hide IDs from primary labels, create missing entities in context. | Every unmatched prospect / high / medium |
| P1 · A9 | Blank final-price fields mean a default that can be changed by margin override; the live example shows blank storage with a $35 agreement default while customer output shows $38.50. | Show the resolved customer price and its source explicitly; preserve arithmetic. | Default-priced lines / high / medium |
| P2 · A7 | Pipeline says 62 active but shows 64 of 64 prospects; margin is based on only 5 of 62. Different scopes are easy to mistake for conflicting totals. | Name metric scope, keep coverage visible and state whether filters affect totals. | Every pipeline scan / high / small |
| P2 · W4 | Pricing's introductory hero occupies nearly the first desktop viewport; choices begin below it. Marketing repeats a large heading before the input. | Shorten the intro and place the four pricing paths and selected action earlier. | Every pricing visit / high / medium |
| P2 · W2/W3 | Homepage combines the rejected explanatory panel with a large intake container; downstream “arms,” “quadrants” and “tiers” require interpretation. | Remove the rejected panel; give the input a persistent visible label; name service/software choices plainly. | New visitors / high / small–medium |
| P2 · W5 | Intelligence headline wraps into roughly six short lines at the inspected desktop size, with supporting action below the viewport. “Talk with our team” points to a strategy-audit route. | Give the heading sufficient measure and make CTA wording match its actual destination. | New visitors / high / medium |
| P2 · W3 | Proof highlights Top 5 without naming its category in the selected copy. Stories use uneven amounts of context. | Consistent client/category/result/period/source composition using approved evidence. | Visitors assessing credibility / high / medium |
| P3 · W5/W6 | Motion changes are requested, but useful labels and some visual fixes already exist. | Keep existing good states; add restrained continuity after hierarchy and access are verified. | Navigation/tab changes / medium / small |

The different pipeline counts may be correct; this is a scope-labeling issue, not a claim of data corruption. The $35/$38.50 example illustrates hidden default resolution, not a request to change a fee.

## Proposed page compositions

### 1. Fulfillment pipeline: return to work immediately

Primary path: find a prospect → identify its blocker or next action → review or update it. Success: the operator resumes the correct opportunity without losing search, scroll or edits.

Composition:
- **PageHeader:** Prospect pipeline; Create rate sheet as the primary creation entry.
- **MetricStrip:** existing active prospect count, estimated pipeline revenue, modeled margin and won count. Keep “Costs available for 5 of 62 active prospects” attached to margin. Rename ACTUAL COST to the accurate evidence class if the value is modeled or submitted rather than posted.
- **CommandBar:** prospect search, stage, sort, result count, last refresh and secondary export.
- **Full-width DataWorkspace:** prospect, stage, monthly volume, customer estimate, modeled margin/coverage, engagement, next action. Put less frequent internal cost detail in expansion. Preserve the original data and sort options.
- **Activity/intake:** Create rate sheet opens an in-page disclosure or dedicated form route, retaining the list state. Default collapsed on return visits.

Keep a single compact generation indicator with prospect name, status and last update. “Run 2596” belongs in detail if no meaningful name exists. Do not let one old running job dominate the page indefinitely; stale status needs a distinct explanation using existing job evidence.

Use an explicit expand button with accessible name and expanded state. The prospect name opens review. Clicking a select, input or action menu must not toggle the entire row. Add pagination (25 default, 10/25/50 options) without silently omitting results. Search and stage filter the table before pagination; metrics initially retain whole-pipeline scope and say so. A filtered-metrics change needs a deliberate contract, not a CSS refactor.

Acceptance: at 1280 × 800 the first rows and controls are visible without opening intake; search/filter state survives review/back; all 64 current results remain reachable; missing costs read Unknown or Not collected rather than zero.

### 2. Fulfillment review: edit prices with consequences in sight

Primary path: inspect assumptions → set customer fees → save draft → inspect customer preview → approve → publish/share.

Use separate sibling sections instead of enclosing the entire workflow in one oversized card:

| Region | Desktop composition | Content |
| --- | --- | --- |
| Header + state | Full width | Prospect, document type, draft/live revision, one state-appropriate primary action |
| Decision summary | Compact strip | Estimated customer monthly total, pass-through, modeled fulfillment costs, estimated net margin; essential coverage warnings |
| Fee editor | Full width | Category groups, unit, resolved customer price, internal cost, suggested price, inclusion/status, accessible help |
| Assumptions + readiness | Two-thirds / one-third where readable | Volumes/package inputs beside blockers and approval evidence |
| Customer preview | Separate full-width section | Draft label/revision, embedded preview, explicit preview expansion |
| Sharing + history | Secondary sections | Copy live link; warehouse handoff and HubSpot in separate disclosures; audit trail |

Do not squeeze the four-column editable pricing table into a 75/25 split. A narrow readiness sidebar is useful beside short assumptions, not beside that table. At narrow widths stack regions in this same order; group each fee as a labeled row/card with full labels or allow contained table scrolling with the fee identity always available.

Fee design:
- Group by order handling, receiving, storage, optional services, recurring charges and one-time charges. Keep quantities/assumptions outside price columns. Keep agreement terms in their own disclosure.
- Make the **resolved customer price** the visually strongest numeric value. Show “Agreement default,” “Custom price,” “Waived” or “Not included” beside it. Editing switches to Custom; a labeled Reset to default restores inheritance. Never require the user to infer that clearing a field selects a pricing rule.
- Keep units next to values: $/order, $/item, $/pallet/month. Do not repeat FULFILLMENT COST and FINAL CUSTOMER PRICE in every desktop cell when column headers already explain them; retain complete accessible names such as “DTC pick and pack, final customer price.”
- Put market-range methodology and suggestion rationale in a click/focus disclosure. Keep required source age, uncertain costs, waiver requirements and publication consequences visible.
- Inclusion is available only for optional services. Waiver is a separate pricing decision with its existing reason/approval requirements. Omitted costs must not silently improve margin.
- Search filters the fee listing only; it never changes the proposal estimate. Show total included/available line count so hidden rows are understood.

Save behavior: one persistent action region may show unsaved count, Save draft and current revision. Only one primary action is emphasized for the current state. Before save, estimates from changed inputs must read Unsaved estimate; after save, only the successfully rendered revision is reviewable. Keep approval and publication distinct. A sticky action bar must not cover fields or focused controls.

Acceptance: an operator can identify the customer total, whether it is saved, the meaning of a blank/default value and the next action without reading explanatory paragraphs. Show at least several ordinary fee rows in a standard desktop viewport after jumping to Pricing. Preserve all source values and published behavior; no table search can alter price.

### 3. Deal creation: recognizable records and local recovery

Keep the form focused, with Deal details, Company and contact, and Ownership/source as clear groups. Remove the configuration filename and “submits ID” instructions from the product UI.

Replace huge entity dropdowns with the existing accessible search/select pattern: company name + domain; contact name + email. Show identifiers only in optional technical detail when needed to disambiguate. Label stage with its readable name. Name the amount's actual currency and business meaning from the configured deal contract; do not assume monthly revenue.

New company/contact opens one dialog at a time. Preserve the parent draft, make duplicates reviewable, select a successfully created record and announce the result. Pending, no matches, integration unavailable, permission denial and partial success have separate states. Existing IDs remain the submitted identity.

Acceptance: a known company is found by domain; unmatched records can be created without losing the deal; cancellation creates nothing; failed entity creation leaves the parent draft intact. Use the dialog focus and dismissal behavior in the accessibility gate below.

### 4. Public pricing: let people choose before asking them to read

Retain the four current paths and their working forms. Shorten the hero to **Pricing and plans**, with a useful line such as “Choose software or get a quote for managed services.” Validate wording against available offers.

Show the four path controls immediately below the heading, with the selected panel's purpose and primary action visible within the first 800px of desktop height. Suggested labels stay Marketing, Fulfillment, Shipping OS and Anata Intelligence; their short descriptors explain the pricing basis.

For services: input/action plus concise deliverables, followed by assumptions and exclusions. For software: account type, comparable plan cards, billing terms and matching signup CTA. Reserve aligned slots for plan name, price, limits and CTA; keep essential terms visible. Reduce redundant “start here / before you start” introductions. Place approved lifestyle imagery after the decision area, not above the selector.

Retain a readable content measure inside the full-width scope section. Add an icon only when it helps identify a service category. Avoid turning every paragraph into a boxed panel.

Acceptance: visitors can choose their path and identify whether it has fixed plan pricing or needs a quote without scrolling through a long introduction. Switching paths retains unsubmitted inputs but cannot submit a hidden form.

### 5. Home and Intelligence: expressive headings with usable actions

Home: retain the three-line brand statement and remove the rejected supporting panel. Give the analysis input a persistent visible label and a static example appropriate to accepted input. Keep the primary action reachable; do not make essential input guidance depend on a rotating placeholder or color.

Intelligence: rebalance the hero to approximately 55/45 text/visual at wide sizes. Set headline measure and fluid size together so it reads in approximately three or four purposeful lines at 1280/1440, rather than a narrow stack. Stack the visual below the text on smaller screens. Preserve the existing multicolor styling and shorter capability labels.

CTA audit: “Talk with our team” should reach the intended contact/booking flow; if the destination remains a self-serve audit, label the action accordingly. Choose one prominent action per hero and one quiet secondary action. Avoid adding a second generic “Explore” destination with different behavior.

Proof: one featured story plus two supporting stories. Each contains client, approved category, primary result, period and evidence link. Use a real product image with a reserved aspect ratio. A large number must retain its meaning at mobile widths. No chart is justified without underlying comparable data; a clear metric and period are sufficient.

Acceptance: headline, action and input remain understandable at narrow widths and 200% zoom; all proof claims have sources; destinations match labels; no new statistics or pricing claims are introduced.

## Accessibility and interaction gate

- Aim for 44px touch controls where practical; meet WCAG 2.2's 24 × 24 CSS-pixel minimum or a documented applicable spacing/other exception. Check small info, expand and menu buttons rather than assuming the visual glyph is the hit area. [W3C target-size guidance](https://www.w3.org/WAI/WCAG22/Understanding/target-size-minimum.html)
- Sticky navigation and action bars must not wholly obscure keyboard focus; the product target is a fully visible focused control. Test short viewports, zoom and validation scrolling. [W3C focus guidance](https://www.w3.org/WAI/WCAG22/Understanding/focus-not-obscured-minimum.html)
- Modal creation dialogs move focus inside, contain Tab/Shift+Tab, offer a visible cancel/close action and restore focus logically on closing. Escape closes when it can do so safely; pending writes must retain recoverable operation state. [W3C dialog pattern](https://www.w3.org/WAI/ARIA/apg/patterns/dialog-modal/)
- Pricing tabs expose tablist/tab/tabpanel semantics, selected state and arrow-key movement. Automatic activation is appropriate only when panel display has no noticeable latency. Hidden panels must not leave interactive controls in the tab order. [W3C tabs pattern](https://www.w3.org/WAI/ARIA/apg/patterns/tabs/)
- Use a disclosure for help containing links or actions; do not make an interactive mini-panel a hover-only tooltip. Keep financial consequences outside optional help.
- Meet text contrast requirements with actual rendered colors; gradients need checks across their lightest regions. Preserve reduced-motion alternatives. Do not animate prices, save state or queue movement for decoration.
- Group validation errors beside fields and in an error summary; preserve entered values. Announce save/error outcomes without reading the entire refreshed table.

## Validation and delivery sequence

**First:** confirm the deployed source revision and preserve changes already present. Refine pipeline order, resolved pricing display and the deal form. These address daily work and misunderstanding.

**Second:** shorten public pricing composition, remove the rejected home panel, rebalance Intelligence and standardize proof.

**Third:** add restrained transitions and imagery, using existing components. Motion must not delay content or replace useful state feedback.

For each slice record screenshot evidence, source thread IDs, current state and acceptance results. Browser-check 390, 768, 1280 and 1440 CSS pixels using verified viewport measurements; add 631/799 for reported cases. Exercise keyboard and 200% zoom, empty/loading/error/permission/stale states, and save/reload where relevant using fixtures or an authorized test environment. This audit did not perform live writes.

Suggested usability check before release: ask a sales operator to find an opportunity, identify an excluded/default fee, save a draft and locate the live proposal; ask a new visitor to choose the right pricing path and explain a case-study result. Record hesitation, wrong turns and recovery, not a subjective beauty score.

## Open decisions

- Optional-fee eligibility and charge inclusion remain governed by business rules; do not solve ambiguity with a universal toggle.
- Which proof categories and results have approved evidence? Keep existing verified content until supplied.
- Which current deployment/branch owns these already-shipped changes? Avoid rebuilding from the stale local checkout.
- Confirm the intended contact destination for “Talk with our team.”
- Shared card styling is constrained by Agent DESIGN.md. Dashboard references guide composition, not a new dependency or replacement theme.

Deliverable: this read-only audit and proposed page compositions. The product remains unchanged.
