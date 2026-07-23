# Shared reports UX/UI refresh

Status: build-ready specification
Scope: recipient-facing shared deliverables and authenticated report-detail/export surfaces
Design authority: `AGENTS.md`, `DESIGN.md`, and `docs/internal-app-canonical-structure-spec.md`
Prepared: July 23, 2026

## Outcome

Make every report or deliverable opened from Agent feel like one trustworthy Anata product family while preserving the different jobs of:

- recipient-facing presentations;
- operator-facing report review;
- print and download artifacts;
- tracked, token-gated delivery;
- machine-readable exports.

The work must not turn public deliverables into authenticated admin screens. Public reports should remain presentation-led, recipient-focused, factual, and easy to share. Authenticated report indexes and detail pages should remain dense operator software.

## Verified current behavior

The current system uses several independent rendering and styling paths:

1. Sales decks use `shared/anata_brand/deck.css` plus the deck renderer and standalone story HTML in `sales_support_agent/services/deck/`.
2. Fulfillment rate sheets use `shared/anata_brand/style.css` plus extensive renderer-specific CSS in `sales_support_agent/services/fulfillment_deck/rendering.py`.
3. Brand briefs use a separate token set and inline styles in `sales_support_agent/services/brand_analysis/share_page.py`.
4. Fulfillment CS and Website Ops report detail views are rendered independently inside authenticated operator pages.
5. The shared fulfillment cost form defines another standalone layout and token set in `sales_support_agent/services/fulfillment_deck/admin_page.py`.

The renderers currently disagree on foundational values:

- shared artifact CSS uses ink `#1d2d44`, sand-soft `#f7f3ec`, and paper `#fffdf9`;
- the approved Agent system uses ink `#2b3644`, background `#f9f7f3`, and surface `#ffffff`;
- the deck story renderer defines another palette including `#0d1f24`, `#1a4f4a`, and `#f6f3ec`;
- content widths range from approximately 820px to 1120px, with different spacing and breakpoint rules;
- public invalid-token or missing-artifact responses sometimes return unbranded raw text such as “Brief not found,” “Rate sheet not found,” or “Cost form not found.”

Existing behavior that must be preserved:

- public deck and rate-sheet URLs remain token-gated;
- invalid tokens return a non-revealing 404;
- draft fulfillment sheets remain unavailable publicly;
- published links remain stable across allowed reruns;
- deck and rate-sheet heartbeat tracking continues to work;
- internal previews do not pollute external engagement;
- old sales decks without a stored story continue to receive a usable fallback;
- public fulfillment requotes remain session-only and do not overwrite the operator-approved source;
- downloads preserve their current media type and content-disposition behavior;
- reports never expose internal notes, raw provenance that is intentionally private, secrets, or operator controls.

## Users and success conditions

### External recipient

The recipient opens a shared link and can answer within one scan:

- What is this?
- Who or what is it about?
- When was it generated or last updated?
- Which information is confirmed, estimated, unavailable, or requires review?
- What should I do next?
- Can I safely print, download, or share it?

Success means the recipient can understand and act on the result without needing an operator to explain the interface.

### Internal operator

The operator can:

- preview exactly what the recipient will see;
- distinguish draft, published, delivered, viewed, and stale states;
- copy or open the canonical share link;
- inspect source and freshness information;
- recover from missing or invalid artifacts;
- download the intended file without confusing it with the hosted view.

Success means presentation changes do not obscure workflow state or change business behavior.

### Support or administrator

The support user receives a safe, recognizable recovery screen and a non-secret reference identifier when useful. Invalid public links must not reveal whether a record exists, who owns it, or why access failed.

## Scope

### Public deliverables

| Surface | Route | Renderer |
| --- | --- | --- |
| Sales deck | `/decks/{deck_slug}/{run_id}/{token}` | `services/deck/service.py`, `services/deck/rendering.py`, `shared/anata_brand/deck.css` |
| Legacy deck alias | `/deck-exports/{run_id}/{token}` | `api/router.py` |
| Deck story | `/decks/{deck_slug}/{run_id}/{token}/story` | `api/router.py` |
| Deck preview image | `/decks/{deck_slug}/{run_id}/{token}/preview.png` | `services/deck/preview_image.py` |
| Deck story download | `/decks/{deck_slug}/{run_id}/{token}/story.md` | `api/router.py` |
| Brand brief | `/brand/{slug}/{report_id}/{token}` | `services/brand_analysis/share_page.py` |
| Brand intake guide | `/brand-intake` and print mode | `services/brand_analysis/intake_guide_page.py` |
| Fulfillment rate sheet | `/rate-sheets/{slug}/{run_id}/{token}` | `services/fulfillment_deck/rendering.py`, `shared/anata_brand/style.css` |
| Fulfillment cost form | `/fulfillment-costs/{run_id}/{token}` | `services/fulfillment_deck/admin_page.py` |

### Authenticated report surfaces

| Surface | Route |
| --- | --- |
| Sales Decks index and preview entry points | `/admin/sales-decks`, `/admin/sales/decks/` |
| Fulfillment draft review and preview | `/admin/fulfillment/sales/runs/{run_id}/review`, `/preview` |
| Fulfillment CS reports index, latest, detail, and artifacts | `/admin/fulfillment/cs/reports/…` |
| Website Ops reports index, latest, and detail | `/admin/website-ops/reports/…` |
| Brand Analysis report detail and download | `/admin/executive/brand-analysis/{report_id}`, `/download` |
| HR reports and CSV exports | `/admin/hr/reports`, `/admin/hr/reports/{kind}.csv` |

### States in scope

- valid current report;
- old but still supported report;
- draft preview;
- published but never viewed;
- viewed/delivered;
- stale data;
- partial or unavailable evidence;
- missing report;
- invalid or expired-looking token;
- unsupported or missing artifact;
- generation-in-progress handoff;
- failed generation;
- download and print;
- no-JavaScript and reduced-motion behavior;
- empty authenticated report list;
- permission denied.

## Non-goals

- Do not change report calculations, pricing, scoring, evidence classification, or CRM behavior.
- Do not alter token generation, token validation, access roles, heartbeat payloads, or analytics meaning.
- Do not redesign the marketing entry experiences on `anatainc.com`.
- Do not retroactively mutate stored historic HTML in place.
- Do not add a frontend framework, animation library, or second component system.
- Do not make JSON, Markdown, CSV, DOCX, or PNG outputs visually imitate an HTML page.
- Do not add dark mode in this phase.

## Design direction

### One recipient shell, multiple report bodies

Create a small shared public-deliverable shell that every hosted HTML report can use. It owns:

- approved wordmark and product attribution;
- skip link and semantic landmarks;
- report identity, recipient/subject, freshness, and evidence status;
- a consistent utility toolbar;
- a consistent content canvas and reading width;
- public-safe notices and recovery pages;
- print and responsive behavior;
- focus, reduced-motion, and no-JavaScript fallbacks.

The shell must not own report-specific charts, pricing tables, maps, narratives, or calculations.

### Canonical public-deliverable tokens

Move the shared public values into `shared/anata_brand/` and consume them from each renderer:

- background: `#f9f7f3`;
- surface: `#ffffff`;
- surface-soft: `#f2ece3`;
- ink: `#2b3644`;
- ink-muted: `#5d6977`;
- border: `rgba(43, 54, 68, 0.12)`;
- accent: `#85bbda`;
- accent-strong: `#5e9fc4`;
- accent-soft: `rgba(133, 187, 218, 0.18)`;
- support: `#bfa889`;
- semantic success, warning, and danger extensions from `DESIGN.md`.

Montserrat remains reserved for concise headings, labels, and controls. Inter/Segoe UI is the default for operationally dense public reports; Roboto may be used for longer recipient-facing narratives if loaded consistently.

Do not mechanically replace report-specific data colors until contrast and semantic meaning are verified.

### Public report anatomy

Hosted HTML deliverables should generally use:

1. skip link;
2. compact Anata identity and report type;
3. report title, subject or recipient, generated/updated date, and evidence/freshness state;
4. utility toolbar;
5. executive summary or primary result;
6. report-specific evidence and recommendations;
7. limitations, provenance, or methodology disclosure;
8. recipient-safe next action;
9. generated-by attribution and support path.

The first viewport must not be dominated by controls. Print, download, and copy-link actions should be discoverable but subordinate to the result.

### Utility toolbar contract

Use one shared order and language:

1. primary next action when the report genuinely has one;
2. `Download` when a real downloadable artifact exists;
3. `Print or save PDF`;
4. `Copy link`;
5. optional `Read summary` or `Open full report` counterpart.

Requirements:

- actions use button/link semantics correctly;
- copy success is announced through an accessible live region;
- print controls disappear in print output;
- controls do not depend on hover;
- failure to copy provides manual selection guidance;
- no toolbar action exposes or displays the raw access token;
- legacy deck rail controls may retain their spatial model, but labels, focus, feedback, and action order must match this contract.

### Evidence and freshness

Every hosted report must display:

- generated date or last material update;
- whether the report is current, stale, partial, or unavailable;
- a short source/evidence summary appropriate for the recipient;
- clear labels for estimates and assumptions;
- a limitations or methodology disclosure when interpretation could otherwise overstate certainty.

Internal-only provenance stays out of public HTML. Public-safe evidence summaries must be derived from already approved report data, never invented during rendering.

### Recovery shell

Replace raw public error strings with a shared, token-safe recovery page:

- Anata identity;
- heading such as `This report is unavailable`;
- neutral explanation covering expired, replaced, mistyped, or unavailable links without confirming record existence;
- `Check the link and try again`;
- `Contact the person who shared this report`;
- optional opaque support reference that contains no token, PII, record name, or infrastructure detail;
- `noindex`;
- correct 404 status.

Authenticated missing-report states use the application shell and can provide a safe route back to the relevant report index.

## Page-by-page migration

### 1. Sales deck hosted view

Keep the slide rail, section navigation, current storytelling, heartbeat tracking, and print expansion behavior.

Update:

- align foundational tokens with the canonical public package;
- add report metadata/freshness near the opening section without competing with the deck thesis;
- normalize rail utility order and keyboard focus;
- give copy-link success and errors an accessible live region;
- identify estimated, observed, and unavailable values consistently;
- ensure compact navigation remains usable at 1280px and collapses deliberately below the existing rail breakpoint;
- retain old deck compatibility through versioned CSS classes or a renderer-version attribute.

Acceptance:

- a valid token renders the same business content and sends the same heartbeat events;
- internal preview remains excluded from external engagement;
- every rail control is keyboard reachable with a visible focus ring;
- no global horizontal overflow at 1440px, 1280px, 768px, or 390px;
- print includes all report sections and excludes navigation/tool controls;
- missing and invalid-token states use the shared recovery shell.

### 2. Sales deck story

Keep the readable one-page format and Markdown download.

Update:

- replace its separate green/cream palette with canonical public tokens;
- use the shared report identity, metadata, toolbar, and footer;
- display a clear relationship back to the full deck;
- keep paragraphs at a readable measure;
- convert the old-deck fallback into a recipient-safe unavailable state rather than operator instructions such as “Re-generate.”

Acceptance:

- current stories render all persisted sections;
- old decks without a story still return 200 and explain that the summary is unavailable;
- the fallback never exposes admin workflow language to external recipients;
- Markdown download behavior and filename remain unchanged;
- bad tokens return the shared 404 recovery page.

### 3. Sales deck preview image

Keep the route, media type, and social-preview purpose.

Update:

- derive colors and typography from canonical public tokens;
- include only recipient-safe title/subject information;
- provide a deterministic branded fallback when preview rendering fails;
- verify long names and missing logos do not clip.

Acceptance:

- image dimensions and consuming metadata contracts remain compatible;
- no token, internal ID, private note, or debug message appears in pixels or metadata;
- representative long and short titles remain legible.

### 4. Brand brief

Keep its editorial, investment-brief structure, charts, flags, provenance disclosure, print support, and stable share link across reruns.

Update:

- replace local foundation tokens with canonical public tokens;
- reduce inline presentation styles by introducing report-level primitives;
- add the shared identity/freshness/toolbar pattern;
- distinguish hard disqualifiers, cautions, assumptions, and unavailable data using text plus color;
- verify chart text alternatives and reading order;
- make the public 404 state branded and neutral.

Acceptance:

- rerunning a report preserves its existing public URL and token;
- `noindex` remains present;
- flags and valuation information retain their current meaning;
- every chart or visual score has an equivalent textual summary;
- print avoids splitting key score, flag, and valuation blocks;
- invalid token, missing report, and invalid stored payload produce the same public-safe 404.

### 5. Brand intake guide

Keep it as a focused public preparation document rather than an operator page.

Update:

- adopt the shared identity, typography, token, print, and footer primitives;
- clarify printable versus interactive actions;
- ensure examples are visually distinct from required inputs;
- preserve the optional print-mode route behavior.

Acceptance:

- the guide reads cleanly on screen and Letter-size print;
- all requirements remain understandable without color;
- URLs and contact guidance remain actionable at narrow widths.

### 6. Fulfillment rate sheet

Keep the current quote content, zone map, scenario/requote controls, staged loading messages, no-JavaScript fallback, print behavior, draft gate, token gate, and heartbeat behavior.

Update:

- adopt the shared public identity/freshness/toolbar shell;
- normalize estimate, sample, confirmed-rate, and unavailable labels;
- make interactive quote adjustments visibly separate from the operator-approved published baseline;
- ensure the page explains that viewer requotes are temporary and do not alter the shared source;
- unify focus, field, button, table, and notice patterns;
- replace raw missing-sheet text with the public recovery shell.

Acceptance:

- draft public URLs and invalid tokens continue to return 404;
- published sheets render without exposing unapproved pricing or private provenance;
- viewer requotes remain session-only and do not mutate stored quote data;
- heartbeat events retain their existing contract;
- the full rate sheet prints with all necessary quote information and no editing controls;
- maps and tables provide text summaries and contained horizontal scrolling where unavoidable;
- no JavaScript still exposes essential quote content.

### 7. Shared fulfillment cost form

This is a recipient-input workflow, not a report. Keep its separate focused-form width and signed submission history.

Update:

- use the shared public identity and recovery shell;
- make the purpose, recipient, save behavior, and privacy boundary explicit;
- add field-level validation, a submission error summary, and preserved input on error;
- clearly separate required costs from optional notes;
- show a stable confirmation state describing what was saved and what happens next;
- never display sales pricing or internal negotiation data.

Acceptance:

- valid submissions retain existing persistence and negotiation-history behavior;
- bad tokens use the shared 404;
- input values survive validation errors;
- all fields are labeled and errors are programmatically associated;
- duplicate submission is prevented while saving;
- success is announced and remains visible after redirect;
- keyboard-only completion works in DOM order.

### 8. Fulfillment CS report index and detail

Keep these authenticated and operator-dense.

Update:

- use the canonical app shell, page header, freshness/source label, report toolbar, and state components;
- visually distinguish hosted detail from raw JSON, Markdown, and HTML artifact downloads;
- ensure a missing report routes back to the index through an app-shell recovery state;
- define stale and partial report states.

Acceptance:

- index, latest redirect, detail, and all artifact routes retain their contracts;
- HTML artifacts opened directly use the public-deliverable visual foundation if they are intended for people;
- JSON and Markdown remain semantically correct raw artifacts;
- result counts and latest-report status are visible;
- missing artifact responses do not become blank or raw-text dead ends.

### 9. Website Ops report index and detail

Keep these authenticated, evidence-first operational reports.

Update:

- use canonical report header and toolbar primitives;
- make collection date, execution scope, source connection, and partial/failure state explicit;
- separate observed issues, approved actions, executed actions, skipped actions, and recommendations;
- remove inline one-off layout where shared application primitives fit.

Acceptance:

- daily and other supported report modes still resolve through the same routes;
- latest redirect behavior remains unchanged;
- an unsupported mode or missing slug renders an app-shell recovery page;
- external-connection failures do not imply that data was collected;
- every warning provides a resolution route or next step when one exists.

### 10. Brand Analysis authenticated detail and DOCX download

Keep internal actions, permission gates, rerun behavior, report history, and DOCX content contracts.

Update:

- show draft/published/share-link state in the app header;
- clearly label operator-only actions;
- provide an exact recipient preview entry point;
- use the shared app download/action toolbar;
- add safe missing-document and generation-failure states.

Acceptance:

- report view remains permission-gated;
- DOCX media type, filename, and content remain compatible;
- public preview contains no operator actions;
- rerun preserves the stable public link;
- missing DOCX does not return a visually blank dead end.

### 11. HR reports and CSV exports

Keep reports and exports authenticated with existing payroll/report permissions.

Update:

- align the reports page to the canonical app report header and export toolbar;
- explain each export’s scope, freshness, and sensitivity before download;
- distinguish downloadable data from on-screen reports;
- add empty and permission states.

Acceptance:

- CSV columns, values, filenames, permission checks, and encoding remain unchanged;
- export controls are keyboard accessible and identify the file type;
- sensitive content is not included in URLs or analytics labels;
- lack of data produces a useful empty export/page state rather than an unexplained file.

## Shared primitives to build first

### Public HTML primitives

- `PublicReportShell`
- `PublicReportHeader`
- `ReportFreshness`
- `EvidenceSummary`
- `PublicReportToolbar`
- `PublicNotice`
- `PublicRecoveryPage`
- `PublicReportFooter`
- `PrintOnly` and `ScreenOnly`
- accessible live-region helper

These may be Python render helpers plus shared CSS. They must not require a client framework.

### Authenticated report primitives

Reuse or extend the canonical app primitives for:

- `AppReportHeader`
- `ReportArtifactToolbar`
- `ReportList`
- `ReportEmptyState`
- `ReportRecoveryState`
- `SourceFreshness`
- `PartialDataNotice`

### Renderer versioning

Stored sales-deck and fulfillment HTML may outlive the code that generated it. Add an explicit renderer or design-system version to newly generated HTML and persisted summary metadata.

Recommended default:

- serve historic HTML unchanged when it is already stored;
- apply compatible shared foundation styles only when the stored markup contract is known;
- regenerate only through the existing explicit operator workflow;
- never bulk rewrite stored customer deliverables.

## Accessibility requirements

Every hosted HTML deliverable must:

- include semantic `header`, `main`, and `footer` landmarks;
- include one level-one heading and a logical heading hierarchy;
- provide a skip link;
- expose strong `:focus-visible` styling;
- support toolbar and report navigation by keyboard;
- label icon-only controls;
- announce copy, save, loading, and failure results through live regions;
- provide text equivalents for charts, maps, grades, and color-coded status;
- meet WCAG AA contrast;
- remain usable at 200% zoom;
- not rely on hover;
- honor `prefers-reduced-motion`;
- preserve essential content without JavaScript.

## Responsive and print requirements

Validate hosted HTML at:

- 1440px;
- 1280px;
- 768px;
- approximately 390px;
- 200% browser zoom;
- Letter-size print/PDF.

Requirements:

- no global horizontal overflow;
- wide data tables use contained scrolling with persistent row identity;
- controls wrap without changing action hierarchy;
- fixed rails collapse before they obscure content;
- touch targets are at least 44px where practical;
- print removes navigation, copy controls, interactive inputs, and tracking-only UI;
- print preserves titles, generated date, evidence labels, table headers, and meaningful URLs;
- key cards, warnings, quotes, and valuation blocks avoid awkward page breaks.

## Security and privacy requirements

- Preserve token comparison and 404 behavior.
- Never reveal whether a protected record exists when the token is invalid.
- Never render tokens in visible UI, logs, titles, analytics labels, support references, or copied display text.
- Copy-link controls may copy the current canonical URL but must not separately expose the token.
- Preserve `noindex` on tokenized or private public reports.
- Keep internal preview/view tracking separate.
- Do not load unapproved third-party scripts, analytics, fonts, or assets into tokenized reports.
- Sanitize recipient, brand, title, URL, and narrative values before rendering.
- Public recovery pages must not echo raw exception text.
- Public reports must not expose internal provenance fields, prompts, costs, private notes, or operator actions.

## Test and validation plan

### Contract tests

For each public route:

- valid token;
- wrong token;
- missing record;
- malformed stored payload;
- draft versus published state where applicable;
- correct status code and content type;
- `noindex`;
- no token or private-field leakage;
- existing heartbeat or download contract.

For authenticated routes:

- signed out;
- permitted user;
- restricted user;
- empty list;
- valid detail;
- missing detail;
- latest redirect;
- artifact download;
- stale or partial source.

### Renderer tests

- canonical token stylesheet is included once;
- shared shell landmarks and H1 exist;
- toolbar actions are present only when supported;
- live regions and focus styles exist;
- report-specific content remains present;
- old-renderer fixtures continue to render;
- public recovery page contains no record-specific detail;
- print stylesheet hides interactive controls and exposes all essential content.

### Visual QA matrix

Capture screenshots for:

1. current sales deck;
2. old deck with no stored story;
3. deck story;
4. brand brief with complete data;
5. brand brief with partial evidence and hard flags;
6. fulfillment sheet before and after a temporary viewer requote;
7. fulfillment cost form default, validation error, and saved;
8. Fulfillment CS report index, detail, missing report, and direct HTML artifact;
9. Website Ops report index, detail, partial source, and missing report;
10. Brand Analysis operator detail and public preview;
11. HR reports default and empty state;
12. shared public 404.

Review every applicable case at 1440px, 1280px, 768px, 390px, and print.

### Interaction QA

- keyboard-only traversal;
- visible focus;
- skip-link behavior;
- copy-link success and fallback;
- print invocation;
- download;
- story/full-report cross-navigation;
- fulfillment tabs and temporary requote;
- cost-form validation and successful save;
- no-JavaScript essential-content check;
- reduced-motion check;
- heartbeat verification without recording internal previews as external engagement.

### Production smoke test

After each family ships:

- open one newly generated artifact;
- open one existing historical artifact;
- verify one invalid token;
- verify one download or print path;
- verify one narrow viewport;
- confirm tracking and permissions through their existing audit records;
- compare source data with visible evidence labels.

Do not use real customer links in screenshots or test artifacts committed to the repository.

## Implementation sequence

### Phase 1: shared foundation

1. Add canonical public tokens and shell primitives to `shared/anata_brand/`.
2. Add shared public recovery and toolbar helpers.
3. Add renderer-version metadata.
4. Add component fixtures and contract tests.

### Phase 2: sales deliverables

1. Sales deck shell and rail utilities.
2. Story page and old-story fallback.
3. Preview image.
4. Invalid-token and backend-proxy recovery states.

### Phase 3: fulfillment deliverables

1. Rate sheet.
2. Temporary requote state.
3. Shared cost form.
4. Print and no-JavaScript paths.

### Phase 4: brand deliverables

1. Brand brief.
2. Intake guide.
3. Public and authenticated missing-report states.
4. DOCX handoff and recipient preview.

### Phase 5: authenticated reports

1. Fulfillment CS reports and artifacts.
2. Website Ops reports.
3. HR reports and exports.
4. Shared app report primitives and remaining state alignment.

### Phase 6: hardening

1. Historical-artifact compatibility fixtures.
2. Accessibility and keyboard audit.
3. Responsive and print matrix.
4. Security/privacy inspection.
5. Production smoke tests.

## Global acceptance criteria

- All hosted HTML deliverables use the canonical public token foundation and recognizable Anata identity.
- Public deliverables retain report-specific storytelling rather than becoming identical templates.
- All authenticated report pages use the canonical application shell and report primitives.
- Raw-text HTML error pages are replaced by public-safe or app-shell recovery states.
- Token security, stable links, permissions, tracking, downloads, print, reruns, and report calculations remain unchanged.
- Every visible conclusion preserves its actual evidence and certainty.
- Historic stored artifacts remain available and are not silently rewritten.
- No public report exposes internal-only data or controls.
- Keyboard, focus, reduced motion, no-JavaScript essentials, zoom, responsive widths, and print are validated.
- There is no global horizontal overflow at required widths.
- Automated route, renderer, security, and compatibility tests pass.
- One current and one historic artifact from each public family are visually reviewed in production.

## Open decisions and recommended defaults

1. **Should public token links expire?**
   Recommended default: no behavior change in this visual phase. Record expiration as a separate security/product decision.

2. **Should every report offer a file download?**
   Recommended default: show Download only when a real, supported artifact exists. Do not generate a new file format merely to populate the toolbar.

3. **Should old stored HTML receive the new CSS?**
   Recommended default: only when the renderer version declares compatibility. Otherwise preserve it and improve its surrounding delivery/recovery path.

4. **Should public reports use the app’s 1320px canvas?**
   Recommended default: no universal width. Use a shared outer shell, then choose a report-body width by task: editorial stories around 760–900px, briefs around 1000px, and data-heavy decks/rate sheets around 1100–1200px.

5. **Should tokenized reports include external analytics?**
   Recommended default: retain only existing first-party heartbeat behavior. Do not add new third-party tracking during the design refresh.

6. **Should mobile implementation wait?**
   Recommended default: desktop visual refinement may lead, but public shared deliverables cannot ship without a basic 390px and print check because recipients commonly open shared links on phones.
