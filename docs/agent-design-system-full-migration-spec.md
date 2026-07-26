# Anata Agent design-system unification

Status: build-ready master specification
Scope: all authenticated Agent pages, transition states, and retained public deliverables
Primary implementation model: server-rendered Python/FastAPI with shared CSS and render helpers
Prepared: July 26, 2026

## Implementation status

All phases 0–12 were implemented and deployed on July 26, 2026.

- The route inventory covers 481 mounted routes and assigns each route family
  to its migration phase.
- Authenticated Sales, Fulfillment, Admin, Executive, Advertising, Brand,
  Website Ops, Building, HR, and Finance pages use the canonical application
  shell and 1320px container contract.
- Shared and public reports use the reconciled Anata semantic tokens while
  preserving recipient security, tracking, print behavior, and historical
  renderer compatibility.
- Request timing, SQL query timing/counts, request-level permission memoization,
  and immutable versioned static assets are active.
- Desktop visual QA confirmed a full-width header background, one page
  landmark and heading, visible empty states, and no global horizontal overflow
  on representative operator pages.
- Finance/Plaid behavior, permissions, routes, audits, and external-write
  contracts were not changed by this migration.

## Outcome

Make `agent.anatainc.com` operate as one coherent internal product without
flattening the distinct jobs of Sales, Finance, Building, Advertising,
Executive, Fulfillment, HR, Website Ops, and access administration.

Every authenticated page must inherit the same application shell, tokens, page
anatomy, controls, states, accessibility behavior, and validation contract.
Product families may vary their information density and workspace composition
when the operator task requires it.

Public reports remain a distinct recipient-facing mode, but they must use the
same canonical brand values and delivery-state language.

## Verified current state

### Working foundation

- Global and section navigation are centralized in
  `sales_support_agent/services/admin_nav.py`.
- The global and section-navigation backgrounds span the viewport while their
  contents use a constrained inner container.
- Most authenticated product families use a 1320px maximum canvas.
- Sales is the closest current reference for an authenticated results workflow.
- Finance has a deliberate evidence-first flow and intentional
  blue-to-warm-neutral background.
- Public decks, stories, brand briefs, rate sheets, intake, and recovery pages
  already use a dedicated public-report foundation.

### Verified structural drift

- Canonical tokens, `admin.css`, `finance.css`, navigation CSS, and embedded page
  styles disagree on typography roles, paper/background values, muted ink,
  accent strength, status colors, radii, and shadows.
- `admin.css` supplies only a small shell/button foundation. Most component
  contracts described in the design documentation are not implemented as
  shared primitives.
- Finance, HR, Fulfillment, Website Ops, Access, and other families render their
  own HTML document shells.
- Operator renderers continue to embed raw colors, inline styles, page headers,
  forms, tables, filters, alerts, and status patterns.
- Brand Analysis operator reports, Fulfillment Sales administration, and the
  Admin/Executive renderer contain the greatest concentration of duplicate
  styling.
- Loading, empty, filtered-empty, partial, stale, error, permission, success,
  long-running, and confirmation states are implemented inconsistently.
- The canonical-structure documentation still describes the old desktop
  section-header cutoff as unresolved even though it has been fixed.
- There is no committed route-by-state inventory or visual-regression fixture
  covering every live HTML family.

### Verified performance drift

Performance discovery on July 26, 2026 found:

- `render_agent_favicon_links()` embeds the 30,180-byte favicon twice as base64
  in every page. The resulting favicon markup is approximately 80.6KB before
  the page's actual content.
- `render_agent_nav_styles()` adds approximately 9.3KB of repeated inline CSS
  to every authenticated page.
- The production login response was 89,380 bytes. Most of that payload is the
  duplicated embedded favicon rather than useful login content.
- The production Brand Intake page was 63,984 bytes.
- The production Finance stylesheet was approximately 61.5KB, while
  `admin.css` was approximately 2KB.
- Production static responses exposed validators such as `ETag` but no
  `Cache-Control` policy. Cloudflare reported the CSS responses as dynamic
  rather than cache hits.
- Five warm production samples showed login time-to-first-byte between
  approximately 132ms and 200ms, and Brand Intake between approximately 114ms
  and 152ms. These public/transition routes are not themselves severely slow;
  they establish that authenticated page assembly and data access must be
  measured separately rather than blaming network latency alone.
- Authorization middleware parses the session identity, then calls
  `get_current_user()`, which parses it again and resolves database-backed
  access. Route dependencies and page renderers may call the same helpers again
  during one request. There is no request-scoped identity/access memoization.
- `build_dashboard_data()` always loads and evaluates the full current lead set,
  recent mailbox signals, up to 200 deck runs, and visit/section engagement for
  those runs. It is used by both the Fix Queue/Admin route and Sales Decks even
  though those pages require different data projections.
- Several dense Building routes load full collections and related collections
  with `.all()`. Pagination and aggregate projections are not consistently
  enforced.
- The SQLAlchemy engine is created without explicit production pool health
  options or performance instrumentation.
- The production web service runs one Uvicorn process on a Render Starter
  instance. Adding workers without first separating or proving idempotency of
  in-process executors, scheduled loops, and background tasks would create
  correctness risk.
- There is no request-duration, route-phase, SQL-query-count, response-size, or
  `Server-Timing` instrumentation. The authenticated latency distribution is
  therefore currently unknown.

These findings prove payload and over-fetching problems. They do not yet prove
which authenticated route is slowest. Instrumentation and an authenticated
route baseline are required before prioritizing database indexes, worker count,
or infrastructure upgrades.

## Users

- Daily operators acting on Sales, Finance, Building, Advertising,
  Fulfillment, Website Ops, and HR work.
- Managers reviewing health, ownership, exceptions, evidence, and completion.
- Administrators managing permissions, integrations, and consequential
  actions.
- Prospects and clients opening tracked, recipient-facing reports.

## Product principles

1. State, evidence, and next action come before decoration.
2. The same kind of control appears in the same page region.
3. Shared structure is inherited, not copied.
4. Information density may change by workflow; application anatomy does not.
5. An action must not appear more complete or certain than its backend state.
6. Marketing composition is not copied into authenticated operator pages.
7. Existing business logic, permissions, routes, audit trails, tracking, and
   external writes remain unchanged.
8. Changes ship incrementally by coherent page family.

## Authority

Resolve design decisions in this order:

1. `AGENTS.md`
2. `DESIGN.md`
3. Approved workflow and data-contract specifications
4. This master migration specification
5. Shared implementation primitives
6. Product-family extensions

`shared/anata_brand/tokens.json` becomes the machine-readable brand-token
authority. `DESIGN.md` remains the human-readable interaction and semantic
authority.

## Scope

### Included

- Canonical tokens and Agent semantic extensions
- Shared authenticated application shell
- Page headers, actions, metrics, command bars, forms, tables, badges, alerts,
  state panels, histories, and confirmations
- Every live authenticated HTML family
- Authentication and permission transition pages
- Retained public deliverables and their delivery/recovery chrome
- Desktop and essential responsive behavior
- Keyboard, focus, contrast, reduced motion, and state announcements
- Route-by-state inventory and visual validation fixtures
- Documentation cleanup and migration ownership

### Non-goals

- Changing Sales, Finance, Plaid, HR, advertising, fulfillment, or lead-building
  business rules
- Changing route or API contracts merely to make markup easier
- Changing permissions, tenant boundaries, audit requirements, or token access
- Rebuilding Agent in React, Tailwind, or another component framework
- Redesigning `anatainc.com`
- Adding dark mode
- Adding decorative motion
- Restyling binary, JSON, CSV, or plain-text API/download responses
- Retrofitting immutable historic report HTML when compatibility is uncertain

## Constraints

- Preserve unrelated working-tree changes.
- Preserve Finance evidence classes: actual, confirmed, expected, required, and
  manual exception.
- Preserve tokenized public-link validation, tracking, heartbeat, print, and
  download behavior.
- Preserve form values after server validation errors.
- Consequential actions retain preview/confirmation and audit behavior.
- No new unapproved UI dependency.
- Every phase must be independently deployable and reversible.
- Performance work must not weaken session validation, permission freshness,
  tenant boundaries, audit logging, source integrity, or external-write
  safeguards.
- Cross-request authorization caching is prohibited unless it has a bounded
  lifetime and explicit invalidation after access changes.
- Do not increase Uvicorn worker count until in-process scheduled/background
  behavior is proven safe under multiple processes.

## Performance objectives and budgets

Performance is a cross-cutting product requirement. Optimize perceived speed
and server work without hiding stale or incomplete data.

### Route classes

Measure routes separately:

1. transition: login, pending, permission, redirect
2. standard read: settings, lists, lightweight detail
3. dense read: dashboards, Finance Control, Building CRM, report administration
4. mutation: form submission, confirmation, writeback
5. long-running: sync, report generation, external analysis
6. public artifact: deck, brief, rate sheet, story

### Initial server budgets

Warm production targets, measured at the application boundary:

- Transition and standard-read TTFB: p50 at or below 300ms and p95 at or below
  750ms.
- Dense-read TTFB: p50 at or below 500ms and p95 at or below 1,200ms.
- Permission resolution after request-scoped memoization: one identity parse and
  no more than one access lookup per request.
- Ordinary page GET: no synchronous external API call unless the page explicitly
  represents a live refresh.
- Common authenticated shell overhead, excluding page data: at or below 25KB
  uncompressed HTML.
- Static CSS, fonts, images, and scripts: fingerprinted or versioned and served
  with a documented cache policy.
- No N+1 database access.
- Every collection visible to an operator has an aggregate, row limit,
  pagination, or an explicitly documented bounded maximum.
- Long-running work acknowledges the request promptly and reports real state
  asynchronously rather than holding an HTML request open.

These are starting budgets. Phase 1 establishes real p50/p95 baselines and may
tighten them. A budget may be relaxed only for a named route with measured
evidence and an approved reason.

### User-perceived performance

- Navigation gives immediate browser feedback.
- The shell and page title render without waiting for noncritical external
  sources.
- A slow data region shows a truthful loading or stale state within its stable
  workspace.
- Filters that can operate on already-loaded bounded data respond immediately.
- Long-running actions disable duplicate submission and show accepted, queued,
  running, success, or failed state.
- Performance improvements may not replace trustworthy freshness labels with
  silent caching.

## Canonical design foundation

### Brand tokens

The canonical machine-readable token set must include:

- `color.background`
- `color.surface`
- `color.surfaceSoft`
- `color.ink`
- `color.inkMuted`
- `color.accent`
- `color.accentStrong`
- `color.accentSoft`
- `color.support`
- `color.success`
- `color.warning`
- `color.danger`
- `color.border`
- `color.focus`
- `font.heading`
- `font.body`
- `font.mono`
- `font.weight`
- `font.size`
- `lineHeight`
- `space`
- `radius`
- `shadow`
- `control.height`
- `container`
- `motion.duration`
- `motion.easing`

Canonical defaults:

- Background: `#f9f7f3`
- Surface: `#ffffff`
- Surface soft: `#f2ece3`
- Ink: `#2b3644`
- Muted ink: `#5d6977`
- Accent: `#85bbda`
- Accent strong: `#5e9fc4`
- Support: `#bfa889`
- Heading/compact-control font: Montserrat
- Operational body font: Inter with Segoe UI fallback
- Application maximum width: 1320px
- Desktop gutter: 24px
- Standard interactive minimum height: 42px
- Touch-oriented minimum height: 44px

Finance may extend the background with its documented pale-blue-to-warm
gradient. It may not redefine shared text, focus, control, status, or spacing
contracts without an explicit contextual token.

### Application modes

#### Operator mode

- Authenticated, information-dense, calm, explicit, and auditable.
- Uses the persistent Agent shell.
- Uses restrained title scale.
- Prioritizes queues, tables, filters, evidence, and next actions.

#### Public-deliverable mode

- Recipient-facing and presentation-led.
- Shows subject, date/freshness, evidence state, and next action.
- Preserves token security and delivery tracking.
- May use a narrower editorial or wider presentation measure.

#### Authentication/transition mode

- Minimal, reassuring, and explicit about what happens next.
- Does not expose account existence, infrastructure, secrets, or raw errors.
- Provides recovery rather than an indefinite spinner.

## Canonical authenticated shell

Every authenticated document must be rendered through one shared helper.

Required structure:

```html
<body class="app app--operator">
  <a class="app-skip-link" href="#app-main">Skip to content</a>
  <header class="app-header">
    <div class="app-header__global-band">
      <div class="app-container">...</div>
    </div>
    <div class="app-header__section-band">
      <div class="app-container">...</div>
    </div>
  </header>
  <main id="app-main" class="app-container app-page">...</main>
</body>
```

Named container variants:

- `app-container`: 1320px standard
- `app-container--focused`: narrower inner content inside the standard canvas
- `app-container--wide`: prohibited by default; requires documented evidence
  that 1320px harms the operator task

The shell owns:

- document metadata and favicon
- font loading
- navigation
- page background
- container alignment
- skip link
- global focus treatment
- reduced-motion behavior
- core stylesheet loading

Product pages may not repeat those concerns.

## Canonical page anatomy

Use these regions in order when applicable:

1. Page header
2. Decision summary
3. Command bar
4. Primary workspace
5. Supporting evidence/history

### Page header

Required:

- one `h1`
- concise purpose
- optional real subsystem eyebrow
- optional source, freshness, permission, or scope context
- one primary action at the right

Authenticated page titles use a restrained product scale, not marketing-hero
scale.

### Decision summary

- Three to five decision-relevant metrics or one compact state summary.
- Omit when it does not help the operator decide or act.
- Avoid generic card grids.
- Numbers use tabular numerals.
- Every metric states its evidence class or source when ambiguity is possible.

### Command bar

One region contains:

- scope or view switcher
- search
- filters
- sort
- secondary actions
- source/freshness
- visible result count

Primary creation or workflow actions remain in the page header.

### Primary workspace

- Table, queue, board, detail, or form.
- Wide content scrolls inside this boundary.
- Loading, empty, filtered-empty, partial, stale, error, and permission states
  render inside the same boundary.
- Long tables use contained scrolling and sticky headers when useful.

## Shared primitive contracts

Implement as shared CSS classes plus small Python render helpers.

### Structural

- `AppShell`
- `AppContainer`
- `PageHeader`
- `PageActions`
- `MetricStrip`
- `CommandBar`
- `DataWorkspace`
- `ActivityHistory`

### Controls

- Primary button
- Secondary button
- Quiet button
- Destructive button
- Link
- Search field
- Filter/select field
- Text field
- Textarea
- Checkbox/radio
- Segmented control
- Tabs
- Pagination

Buttons perform actions. Links navigate. Badges communicate state. Do not nest
buttons inside links or style status labels like action controls.

### Feedback and state

- `StatusBadge`
- `EvidenceLabel`
- `Alert`
- `StatePanel`
- `FieldError`
- `ErrorSummary`
- `ProgressStatus`
- `LiveConfirmation`
- `ConfirmationDialog`

State labels use:

- Ready
- Queued
- Running
- Needs review
- Confirmed
- Delivered
- Failed
- Blocked
- Stale

### Tables

The shared table contract includes:

- caption or visible purpose
- result count
- stable column alignment
- tabular and right-aligned numbers
- textual status
- contained horizontal scroll
- optional sticky header
- row actions in a consistent final column
- empty and filtered-empty states
- accessible sort state where sorting exists

## Phase plan

## Phase 0: baseline, inventory, and rule reconciliation

### Objective

Create a reliable migration ledger and remove contradictions from the design
authority before changing more pages.

### Work

1. Commit or supersede the existing design handoff.
2. Update `DESIGN.md` with the final token, application-mode, page-anatomy, and
   primitive contracts.
3. Update the canonical-structure spec to mark the section-header cutoff fixed.
4. Generate the definitive inventory of every user-visible HTML route.
5. Record for each route:
   - URL and method
   - access requirement
   - renderer
   - stylesheets and embedded style blocks
   - primary job and action
   - data/evidence source
   - write/destructive behavior
   - state coverage
   - container width
   - migration phase
6. Capture representative baseline screenshots at 1280px and 1440px.
7. Record known default, empty, partial, stale, error, permission, and success
   states.

### Primary files

- `DESIGN.md`
- `docs/internal-app-canonical-structure-spec.md`
- `docs/agent-anatainc-design-handoff.md`
- new route/state inventory under `docs/`

### Exit criteria

- Every live HTML route is assigned to one family and phase.
- Design-authority documents no longer contradict current implementation.
- No page migration starts without a recorded baseline and state list.

## Phase 1: performance observability and fast-path foundation

### Objective

Make authenticated latency measurable, remove the proven common-path waste, and
split oversized page data assembly before broad visual migration.

### Work

1. Add lightweight request instrumentation:
   - normalized route name
   - status code
   - total duration
   - response bytes
   - database query count and cumulative query time
   - external-call count and cumulative time
   - render/serialization duration where material
2. Emit structured logs and a `Server-Timing` header for authenticated
   diagnostic use. Do not expose SQL, secrets, tokens, PII, or infrastructure
   details.
3. Create a repeatable authenticated benchmark for one representative route in
   each route class.
4. Record warm and cold p50/p95 results separately.
5. Add request-scoped identity and access memoization so middleware,
   dependencies, and renderers reuse one resolved user.
6. Preserve immediate permission changes by limiting memoization to the current
   request. Consider bounded cross-request caching only after measuring the
   remaining lookup cost and defining invalidation.
7. Serve the favicon from a versioned static URL rather than embedding two
   base64 copies in every document.
8. Move shared navigation and shell CSS into the versioned authenticated
   stylesheet.
9. Add explicit cache headers for versioned static assets. Keep HTML and
   tokenized/private responses noncacheable where security or freshness
   requires it.
10. Split `build_dashboard_data()` into page-specific projections:
    - Fix Queue/Admin action data
    - Sales Deck history and engagement data
    - Executive summary data
11. Paginate deck history and load detailed engagement only when requested.
12. Add limits/pagination/aggregate projections to Building and other routes
    that currently load unbounded collections.
13. Audit database query plans for the slowest measured routes before adding
    indexes.
14. Configure safe SQLAlchemy production pool health options only after
    measuring connection wait/reconnect behavior.
15. Audit long-running and scheduled work before changing process count.
    Separate it from web request serving or prove multi-process idempotency
    before adding Uvicorn workers.

### Primary files

- `sales_support_agent/services/admin_nav.py`
- `sales_support_agent/services/auth_deps.py`
- `sales_support_agent/services/access/middleware.py`
- `sales_support_agent/services/access/store.py`
- `sales_support_agent/services/admin_dashboard.py`
- `sales_support_agent/api/router.py`
- `sales_support_agent/api/building_crm_router.py`
- `sales_support_agent/models/database.py`
- `sales_support_agent/main.py`
- `sales_support_agent/static/`
- `render.yaml`
- new performance instrumentation and benchmark tests

### Measurement matrix

At minimum benchmark:

- `/admin/login`
- `/admin`
- `/admin/sales`
- `/admin/sales/deals`
- `/admin/sales/decks/`
- `/admin/executive`
- `/admin/fulfillment/sales`
- `/admin/fulfillment/cs/`
- `/admin/finances`
- `/admin/building`
- `/admin/hr`
- `/admin/website-ops`
- one public deck/report route

Use safe authenticated test data and record:

- status/redirect chain
- TTFB and total response time
- HTML bytes
- query count/time
- external calls/time
- render time
- row counts assembled
- cache outcome

### Exit criteria

- Authenticated route p50/p95 latency is known by route class.
- Slow-route evidence identifies server, database, external, render, and payload
  contributions.
- One identity parse and at most one permission lookup occur per request.
- Pages no longer embed the favicon or shared navigation CSS.
- Versioned static assets return an explicit cache policy.
- Fix Queue/Admin no longer builds 200-deck engagement history.
- Sales Decks does not rebuild unrelated lead queues.
- Deck history and other large collections are bounded.
- The representative route matrix meets the initial budgets or has named,
  measured follow-ups.
- No permission, tracking, source freshness, background-job, or business
  behavior changes.

## Phase 2: tokens and shared foundation

### Objective

Make global consistency automatic.

### Work

1. Expand `shared/anata_brand/tokens.json`.
2. Add generated or manually synchronized CSS custom properties.
3. Refactor `admin.css` into the canonical authenticated primitive stylesheet.
4. Convert `finance.css` into a contextual extension of shared tokens.
5. Make `admin_nav.py` consume canonical variables rather than raw values.
6. Implement structural, control, table, and state primitives.
7. Add an internal fixture/render page containing every primitive and state.
8. Add tests that fail when canonical token values or required accessibility
   contracts disappear.

### Primary files

- `shared/anata_brand/tokens.json`
- `shared/anata_brand/style.css`
- `shared/anata_brand/deck.css`
- `sales_support_agent/static/admin.css`
- `sales_support_agent/static/finance.css`
- `sales_support_agent/services/admin_nav.py`
- new shared shell/component helper module
- new component fixture and tests

### Exit criteria

- One documented value exists for every canonical token.
- `admin.css`, Finance extensions, navigation, and public foundations use those
  values.
- Shared primitives render without product-family CSS.
- Focus, contrast, reduced motion, and state text pass the component fixture.

## Phase 3: application shell and transition surfaces

### Objective

Put every authenticated page on one document and navigation shell.

### Work

1. Create one authenticated `AppShell` renderer.
2. Migrate login, pending access, access denied, invitation, authentication
   error, OAuth return, job handoff, and other transition pages.
3. Migrate access administration and settings.
4. Remove repeated font, document-head, navigation, page-background, and
   container declarations.
5. Preserve permission-filtered navigation and server authorization.
6. Ensure restricted users do not see inaccessible navigation destinations.

### Primary files

- `sales_support_agent/services/admin_nav.py`
- `sales_support_agent/services/access/pages.py`
- `sales_support_agent/services/settings_page.py`
- authentication/login renderers in `main.py` and API modules

### Exit criteria

- All transition and access pages inherit one shell.
- Signed-out, pending, restricted, administrator, and super-admin states are
  verified.
- Authentication errors remain non-enumerating.
- No route or permission behavior changes.

## Phase 4: reference result pages

### Objective

Prove the reusable anatomy on Sales Deal Board and Fulfillment Sales Pipeline.

### Sales Deal Board

- Page header contains title, purpose, freshness/source, and `Create deal`.
- Command bar contains scope, search, readiness, sort, synchronization,
  remediation, and result count.
- Table uses shared workspace and table contracts.
- Populated, filtered, filtered-empty, stale, partial, error, and permission
  states preserve the same structure.

### Fulfillment Sales Pipeline

- Uses the same shell, page header, metric strip, command bar, table, and states.
- Preserves pipeline stages, pricing workflow, review, exports, and rate-sheet
  behavior.
- Does not visually imitate Sales where the operator job differs.

### Primary files

- `sales_support_agent/services/sales/deal_board.py`
- `sales_support_agent/api/sales_router.py`
- `sales_support_agent/services/fulfillment_deck/admin_page.py`
- `sales_support_agent/api/fulfillment_deck_router.py`

### Exit criteria

- Equivalent structure and controls come from shared code.
- Neither page contains a copied application shell or duplicate component CSS.
- Both pages pass populated and state-matrix QA at 1280px and 1440px.

## Phase 5: complete Sales and Fulfillment

### Objective

Migrate both reference families end to end.

### Sales surfaces

- Control Room
- Deal Board
- Deal creation
- Deal detail
- Rep Accountability
- Follow-up draft
- Fix Queue
- Sales Decks administration
- Cleanup and batch operations

### Fulfillment surfaces

- Sales Pipeline
- Rate-sheet create/edit/review/history
- CS Action Queue
- CS Reports list and detail
- HTML, Markdown, and JSON artifact actions
- Empty and not-found states

### Work

- Reuse page headers, command bars, state panels, tables, forms, confirmations,
  and activity history.
- Reconcile route labels, page titles, active navigation, and back links.
- Standardize synchronization and freshness copy.
- Preserve all CRM, WMS, delivery, pricing, tracking, and audit behavior.

### Primary files

- `sales_support_agent/services/sales/`
- `sales_support_agent/api/sales_router.py`
- `sales_support_agent/services/fulfillment_dashboard.py`
- `sales_support_agent/services/fulfillment_deck/`
- `sales_support_agent/api/fulfillment_deck_router.py`

### Exit criteria

- Sales and Fulfillment share primitives rather than visual copies.
- Every consequential action retains its confirmation and audit trail.
- No global overflow, hidden actions, or inconsistent route naming remains.

## Phase 6: Admin, Executive, Advertising, and Brand Analysis

### Objective

Remove the highest remaining concentration of embedded styles.

### Admin and Executive

- Split the monolithic Admin renderer into bounded page renderers or shared
  sections without changing routes.
- Use the canonical header, summary, workspace, and state patterns.
- Keep the home page oriented around current health, priorities, and routes to
  action.

### Advertising

- Migrate Audit, Clients, Profit Calculator, and Bulk Planner.
- Preserve configuration, run, preview, approval, and export workflows.
- Use shared forms, result summaries, command bars, progress, and errors.

### Brand Analysis

- Migrate dashboard, pipeline, run, edit, discover, detail, download, and share
  administration.
- Remove page-level raw color and inline-style ownership.
- Preserve public brief generation and historic-artifact compatibility.
- Clearly separate observed evidence, generated analysis, review, and delivery.

### Primary files

- `sales_support_agent/services/admin_dashboard.py`
- `sales_support_agent/services/advertising/`
- `sales_support_agent/services/brand_analysis/report_page.py`
- `sales_support_agent/api/brand_analysis_router.py`

### Exit criteria

- Operator pages contain no independent design system.
- Long-running analyses show accepted, running, partial, failed, and recovery
  states.
- Public and internal controls remain strictly separated.

## Phase 7: Website Ops and Building

### Objective

Bring operational automation and building workflows into the same product
anatomy.

### Website Ops

- Migrate overview, queue, reports, feedback, and approved-action flows.
- Preserve source identities and evidence.
- Make automation status and approval state explicit.
- Use shared command bars, help/disclosure, task cards, histories, and state
  panels.

### Building

- Migrate Control Room, metrics, CRM/list workspaces, checklists, and row
  actions.
- Preserve building-specific business language and dense tables.
- Replace bespoke status colors and controls with canonical tokens and
  primitives.

### Primary files

- `sales_support_agent/services/website_ops.py`
- `sales_support_agent/services/website_ops_vendor/`
- `sales_support_agent/services/building_page.py`
- `sales_support_agent/api/building_crm_router.py`

### Exit criteria

- Both families inherit the application shell and state vocabulary.
- Approval and external-write actions remain explicit and auditable.
- Wide tables scroll inside their workspaces.

## Phase 8: HR and people operations

### Objective

Unify HR visually while preserving its privacy, employee self-service, and
payroll controls.

### Surfaces

- Dashboard
- Employees and employee details
- Onboarding
- Policies
- Teams
- Time and PTO
- Pay statements
- Payroll
- Compliance
- Contractors
- Offboarding
- Reports
- Settings

### Work

- Replace the independent HR shell with `AppShell`.
- Preserve permission-aware secondary navigation and the deliberate employee
  mobile shortcut bar.
- Migrate cards, forms, tables, callouts, buttons, validation, confirmations,
  and empty states.
- Keep sensitive-data warnings and retention language prominent.
- Preserve CSRF, self-approval restrictions, sealed data handling, audit
  evidence, and export behavior.

### Primary files

- `sales_support_agent/services/hr/pages.py`
- `sales_support_agent/api/hr_router.py`
- HR service/model modules only when necessary to expose truthful UI state

### Exit criteria

- HR feels part of Agent without losing privacy and workflow clarity.
- Employee and administrator roles see only appropriate actions.
- Sensitive and destructive actions name the object, effect, and next step.

## Phase 9: Finance outer-shell and component migration

### Objective

Unify Finance controls and structure without changing Finance semantics or
Plaid behavior.

### Surfaces

- Control overview
- Forecast
- AP
- AR
- Alerts
- Scenario
- Upload
- Recurring
- QuickBooks settings
- Reconciliation
- Ledger
- Calendar
- Source settings and readiness

### Work

- Retain the evidence-first page flow:
  source readiness, decision trust, cash posture, trajectory, operator queue.
- Retain the Finance background extension.
- Migrate shared buttons, filters, tables, badges, forms, alerts, and states.
- Replace inline raw-color controls and invalid link/button nesting.
- Label actual, confirmed, expected, required, and manual-exception values.
- Ensure every material risk exposed in a chart is also expressed in text with
  a resolution route.

### Primary files

- `sales_support_agent/static/finance.css`
- `sales_support_agent/services/cashflow/cashflow_helpers.py`
- `sales_support_agent/services/cashflow/`
- `sales_support_agent/api/cashflow_router.py`

### Explicit protection

- No Finance/Plaid behavior changes.
- No changes to source matching, categorization, reconciliation, forecast,
  payment, or writeback rules merely for the visual migration.
- Existing in-progress Finance/Plaid changes must remain isolated from this
  migration unless separately approved.

### Exit criteria

- Finance consumes shared tokens and primitives while retaining its documented
  operator flow.
- Cash and evidence classes remain truthful.
- Child views no longer introduce one-off control systems.

## Phase 10: public deliverables and transition consistency

### Objective

Complete recipient-facing consistency without forcing public reports into the
authenticated app shell.

### Surfaces

- Sales decks
- Sales Stories
- Brand briefs
- Brand intake guide
- Fulfillment rate sheets
- Fulfillment cost forms
- Public recovery states
- Print and download variants

### Work

- Reconcile public tokens with the canonical brand set.
- Preserve editorial/report-specific widths.
- Verify subject, freshness, evidence, next action, and artifact actions.
- Preserve token security, noindex behavior, tracking, heartbeat, and historic
  artifact compatibility.
- Keep raw/binary download responses in their appropriate formats.

### Primary files

- `sales_support_agent/services/public_report_ui.py`
- `shared/anata_brand/style.css`
- `shared/anata_brand/deck.css`
- public renderers under `deck`, `brand_analysis`, and `fulfillment_deck`

### Exit criteria

- Newly generated artifacts use the canonical public foundation.
- Historic artifacts remain valid.
- Invalid/expired links use neutral branded recovery without enumeration.
- Print, copy, download, and tracked-view behavior are intact.

## Phase 11: state, accessibility, responsive, and visual hardening

### Objective

Verify the system beyond default screenshots.

### State matrix

For every applicable family verify:

- default
- loading
- empty
- filtered-empty
- partial
- stale
- error
- permission denied
- success
- long-running
- destructive confirmation

### Accessibility

- Semantic landmarks and headings
- Skip link
- Visible focus
- Logical keyboard order
- Labeled controls and icon actions
- Field-linked validation
- Error summaries
- Live regions
- Text plus color for status
- WCAG AA contrast
- Chart text equivalents
- Reduced-motion support
- Zoom and text reflow

### Responsive

Validate:

- 1280px
- 1440px
- 1600px
- 1920px
- 768px transition
- approximately 390px for essential workflows

Desktop acceptance:

- full-width header bands
- aligned navigation and content gutters
- no global horizontal overflow
- no clipped controls
- contained dense workspaces

### Visual regression

- Create deterministic fixture data for shared primitives and representative
  family pages.
- Capture approved desktop baselines.
- Treat visual comparison as a review aid, not a replacement for state and
  behavior tests.

### Exit criteria

- Every live family has a completed route/state QA record.
- Keyboard and responsive failures are resolved or explicitly accepted.
- Visual baselines cover the shell and one representative page per family.

## Phase 12: incremental rollout and cleanup

### Objective

Finish migration ownership and remove obsolete systems safely.

### Rollout

1. Ship each phase in a small deployable commit or pull request.
2. Run affected route, renderer, permission, and behavior tests.
3. Verify production health and commit revision.
4. Smoke test one safe core workflow per migrated family.
5. Compare visual, behavioral, and performance results against the phase
   baseline.
6. Reject unexplained latency, query-count, response-size, or external-call
   regressions before continuing rollout.
7. Record follow-ups before starting the next family.

### Cleanup

- Remove unused local token declarations only after the consuming page is
  migrated.
- Remove obsolete CSS only after route inventory proves no live consumer.
- Retire duplicate shell helpers after their final caller migrates.
- Do not perform a repository-wide mechanical deletion.
- Update the route/state inventory and migration status after every phase.

### Exit criteria

- Every live HTML route is marked migrated, intentionally exempt, or separately
  scheduled.
- No undocumented application shell, token system, or arbitrary canvas width
  remains.
- Production verification evidence exists for every family.

## Data and API impact

This is primarily a renderer and stylesheet migration.

Expected:

- no database migration
- no API contract change
- no business-rule change
- no permission change
- no tracking change

If a truthful state requires additional backend data, add only a
backward-compatible read field and document it before implementation.

## Analytics and audit impact

- Preserve existing public-view tracking and heartbeat events.
- Preserve existing audit logs for external writes and consequential actions.
- Do not add third-party analytics.
- UI-only interactions do not need new tracking unless a separate product
  decision defines a measurable funnel.

## Test strategy

### Static/render tests

- Required shell landmarks and active navigation
- One `h1`
- Shared primitive classes
- Accessible names and live regions
- No token or secret leakage in recovery states
- Preserved form values
- Correct link-versus-button semantics
- Common shell byte budget
- No embedded favicon or duplicated shared-navigation stylesheet
- Versioned static-asset references

### Route and permission tests

- Signed out
- Pending/unprovisioned
- Restricted ordinary user
- Manager/administrator
- Super-admin
- Public valid/invalid token
- One request-scoped identity/access resolution

### Behavior tests

- Search/filter/sort/result count
- Pagination or row limits
- Consequential confirmation
- External-sync feedback
- Long-running accepted/running/success/failure
- Download/print/copy
- Public tracking

### Performance tests

- Repeatable warm benchmark by route class
- p50/p95 TTFB and total duration
- Response byte size
- SQL query count and cumulative duration
- External-call count and cumulative duration
- Render/serialization duration
- Bounded row counts and pagination
- Static cache headers and conditional requests
- No synchronous external call on ordinary page GET
- Long-running request acknowledgement time

### Browser validation

- Required viewport matrix
- Keyboard-only operation
- Focus visibility
- No global overflow
- Sticky/contained tables
- Empty/error/stale/partial stability
- Production smoke test with real but safe application data

## Master acceptance criteria

1. Every live HTML route is inventoried and assigned a migration decision.
2. Canonical tokens have one machine-readable source and one documented
   semantic authority.
3. Every authenticated page inherits one application shell.
4. Global navigation, section navigation, and page content align to the same
   container contract.
5. No authenticated page introduces an undocumented canvas width.
6. Every migrated page uses the canonical page anatomy where applicable.
7. Equivalent controls and states come from shared primitives.
8. Filterable results show a result count and keep related controls together.
9. Wide data workspaces scroll internally, never at page level.
10. Status and evidence use explicit text and truthful backend state.
11. Loading, empty, filtered-empty, partial, stale, error, permission, and
    success states preserve page structure.
12. Keyboard focus and DOM order follow the visual workflow.
13. Reduced motion and WCAG AA contrast are supported.
14. Public reports remain secure, tracked, printable, and recipient-safe.
15. Finance/Plaid behavior is unchanged.
16. Business rules, routes, permissions, audits, and external writes are
    unchanged unless a separate approved specification says otherwise.
17. Automated tests and production smoke tests pass for every migrated phase.
18. Obsolete page-local design systems are removed only after their callers
    migrate.
19. Request performance is instrumented without exposing sensitive data.
20. Each migrated route meets its route-class budget or has a named measured
    exception.
21. Permission resolution occurs once per request without weakening freshness.
22. Shared shell assets are referenced once, versioned, and cacheable.
23. Page-specific data builders fetch only the data required by that page.
24. Large collections are bounded and N+1 query patterns are absent.
25. Worker/process changes are made only after background work is safe under
    the proposed topology.

## Decisions and recommended defaults

### Token delivery

Decision: whether to generate CSS variables from JSON during build or maintain
the CSS mapping manually.

Recommended default: add a small deterministic generator or validation test so
JSON and CSS cannot silently diverge. Do not add a frontend build system solely
for tokens.

### Wide application canvas

Decision: whether any operator page needs more than 1320px.

Recommended default: migrate to 1320px. Approve `app-container--wide` only after
a 1280px/1440px comparison demonstrates a measurable usability problem.

### HR mobile shortcuts

Decision: whether the HR bottom shortcut bar should become global.

Recommended default: keep it HR-specific until a separate mobile navigation
study proves a shared operator pattern.

### Public historic artifacts

Decision: whether to restyle already stored HTML.

Recommended default: preserve stored HTML unless its renderer version declares
compatibility. Improve the surrounding delivery/recovery path instead.

### Visual regression tooling

Decision: snapshot technology and storage.

Recommended default: use the existing server-rendered test environment and a
small browser screenshot suite. Avoid adding a broad frontend testing stack.

### Authorization caching

Decision: whether to cache resolved permissions across requests.

Recommended default: begin with request-scoped memoization only. Add a short
cross-request cache only if measurement proves the database lookup remains
material, and require immediate invalidation after permission or suspension
changes.

### Web-process scaling

Decision: whether to add Uvicorn workers or increase the Render plan.

Recommended default: optimize and measure the single-process application first.
Before adding workers, move scheduled loops and in-process executors to a
separate worker model or prove that every task is singleton/idempotent across
processes. Consider infrastructure scaling only after application and query
work is measured.

### Static asset caching

Decision: cache duration for shared CSS and images.

Recommended default: use content/versioned URLs with long-lived immutable
caching. Keep HTML private/no-store where authentication, token security, or
freshness requires it.

## Definition of done

The migration is complete when Agent no longer depends on individual page
authors to recreate its visual and interaction rules. New authenticated pages
can be built from the shared shell and primitives, existing families have been
migrated or explicitly exempted, all important states are validated, and the
product remains behaviorally identical except for approved UX improvements.
