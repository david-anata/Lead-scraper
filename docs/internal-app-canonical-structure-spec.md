# Anata Agent Canonical Application Structure

Status: build-ready specification
Scope: authenticated internal application pages
Primary widths: 1280px and 1440px desktop
Mobile implementation: deferred, but the structure must not prevent it

## Outcome

Make Agent feel like one internal operating system rather than a collection of
individually styled tools. Every authenticated page must inherit the same
application shell, page hierarchy, control placement, density rules, and state
patterns while preserving the workflow requirements of Sales, Finance,
Fulfillment, Advertising, Executive, Website Ops, and HR.

## Verified problem

Agent grew through independent Python-rendered page families. The result is
structural drift, not merely inconsistent styling.

- Content canvases currently use competing widths including 900px, 1160px,
  1180px, 1280px, 1320px, and 1480px.
- Shared navigation exists, but page renderers still define their own shells,
  headings, metrics, actions, filters, tables, and responsive behavior.
- Page actions move between the title, metric area, standalone button rows, and
  inline table controls.
- Similar result pages use different orders for scope, search, filtering,
  sorting, result count, export, synchronization, and remediation.
- Some pages communicate hierarchy through nested cards while others rely on
  open whitespace or one-off sidebars.
- Route labels and page labels can describe the same workflow differently.

### Confirmed desktop header defect

`sales_support_agent/services/admin_nav.py` applies the warm section-navigation
background directly to `.topbar-section-row`, which is also constrained to
`max-width: 1320px`. On a wide desktop, the background stops at the constrained
container instead of extending across the viewport.

The canonical implementation must separate:

1. a full-width background band; and
2. a width-constrained inner content container.

## Users

- Operators completing daily Sales, Finance, Fulfillment, Advertising, Website
  Ops, and HR work.
- Managers reviewing health, exceptions, ownership, and completion.
- Administrators controlling access and consequential actions.

## Product principles

1. State, evidence, and next action come before decoration.
2. The same kind of control appears in the same region on every page.
3. Shared structure comes from reusable render helpers and CSS contracts, not
   copied page-specific markup.
4. Information density may differ by workflow, but navigation and page anatomy
   do not.
5. Marketing-site composition is not copied into authenticated operator pages.
6. Existing business logic, routes, permissions, audit trails, and external
   writes remain unchanged.

## Canonical desktop frame

### Layer 1: global navigation band

- Full viewport width with a white background and bottom border.
- Sticky at the top of the viewport.
- Contains a centered inner container using the canonical application width.
- Includes the Agent wordmark, product-area navigation, and account control.
- Product areas use one active-state treatment and one control height.

### Layer 2: section navigation band

- Full viewport width with the warm application background.
- The background must reach both viewport edges at every desktop width.
- Contains a separate centered inner container aligned with global navigation
  and page content.
- Includes the section label and local destinations.
- The band must not appear as a centered widget or stop at the content width.

Recommended structure:

```html
<header class="app-header">
  <div class="app-header__global-band">
    <div class="app-container">...</div>
  </div>
  <div class="app-header__section-band">
    <div class="app-container">...</div>
  </div>
</header>
```

### Layer 3: page canvas

- Use one canonical `1320px` maximum content width.
- Use consistent desktop gutters of `24px`.
- Focused forms may use a narrower inner measure, but remain positioned inside
  the canonical canvas rather than defining a different application shell.
- Exceptionally dense data workspaces may use a documented full-canvas variant;
  they may not introduce a new arbitrary width.

## Canonical page anatomy

Authenticated operator pages use the following order when the corresponding
region is needed:

1. **Page header**
   - Real subsystem eyebrow when useful
   - One page title
   - One-sentence purpose
   - Freshness, source, or permission context
   - One primary action aligned to the right
2. **Decision summary**
   - Three to five decision-relevant metrics or a compact status summary
   - Omitted when it does not help the operator decide or act
3. **Command bar**
   - Scope or view switcher
   - Search
   - Filters
   - Sort
   - Secondary actions
   - Visible result count
4. **Primary workspace**
   - Table, queue, board, detail, or form
5. **Supporting context**
   - Remediation, evidence, history, explanations, or settings

Focused login, confirmation, and single-purpose form pages may use reduced
anatomy, but must still inherit the shell and shared controls where applicable.

## Shared component contracts

Build or consolidate these primitives before migrating more page families:

- `AppShell`
- `AppContainer`
- `GlobalNav`
- `SectionNav`
- `PageHeader`
- `PageActions`
- `MetricStrip`
- `CommandBar`
- `ScopeTabs`
- `SearchField`
- `FilterField`
- `ResultCount`
- `DataWorkspace`
- `DataTable`
- `StatusBadge`
- `EvidenceLabel`
- `StatePanel`
- `ActivityHistory`

In the current server-rendered architecture, these may be Python render helpers
plus shared CSS classes. A JavaScript component framework is not required.

## Command-bar rules

- Scope selection, search, filters, sorting, result count, and table-level
  actions occupy one visual region.
- Actions that change data use buttons. Navigation uses links. Status labels
  are not styled as buttons.
- The primary page action belongs in the page header.
- Synchronization status is shown with its source and freshness. “Sync now” is
  secondary unless stale or blocked data prevents the page’s primary task.
- Data-quality remediation appears as a contextual command with a count when
  possible; it must not float between unrelated controls.
- Controls align to a common height and baseline.

## Data-workspace rules

- Filterable results always show a result count.
- Long tables use a contained scroll region and a sticky header.
- Wide tables scroll inside the workspace, never at the page level.
- Numeric fields use tabular numerals and consistent alignment.
- Status uses text in addition to color.
- Empty, filtered-empty, loading, stale, partial, error, and permission states
  render inside the same workspace boundary so the page does not jump.

## Page-family application

### Sales

- Canonical names: `Control Room`, `Deal Board`, `Rep Accountability`,
  `Fix Queue`, and `Sales Decks`.
- `Fix Queue` must resolve to a specific Sales route rather than the generic
  admin home.
- Deal Board places `Create deal` in the page header.
- Deal scope, search, readiness, sort, result count, synchronization, and
  data-quality review are consolidated into the command region.

### Fulfillment

- Use the same shell, header, summary, command bar, and workspace contracts as
  Sales.
- Preserve fulfillment-specific pipeline stages, exports, and rate-sheet
  workflow.
- Do not make Fulfillment visually imitate a sales pipeline when its operator
  task differs; consistency comes from anatomy and controls.

### Finance

- Retain its evidence-first flow and semantic distinctions.
- Migrate its outer shell and shared controls without flattening Finance into a
  generic metric dashboard.
- Actual, confirmed, expected, required, and manual-exception states remain
  explicit.

### HR

- Remove the special standalone-sidebar feeling where it conflicts with the
  canonical shell.
- Preserve the privacy, permissions, and focused workflows required for people
  operations.

### Advertising, Executive, and Website Ops

- Adopt the canonical header, canvas, command bar, states, and table contracts.
- Keep domain-specific workspace composition where it supports the page’s
  decision.

## Scope

### Included

- Authenticated global and section navigation
- Canonical content width and alignment
- Page anatomy and shared render contracts
- Sales and Fulfillment as the first reference migrations
- Subsequent migration requirements for all authenticated page families
- Desktop loading, empty, filtered-empty, partial, stale, error, permission,
  success, and consequential-action states
- Keyboard and focus behavior

### Not included

- Business-logic changes
- Route/API contract changes except correcting noncanonical navigation targets
  through an approved route mapping
- Finance/Plaid behavior changes
- Marketing-site redesign
- Public deliverable redesign
- New component frameworks
- Dark mode
- Full mobile redesign in this phase

## Migration plan

### Phase 1: structural foundation

1. Convert the header to full-width bands with constrained inner containers.
2. Establish one `AppContainer` width and gutter contract.
3. Move shell, page-header, command-bar, state, and table rules into shared
   helpers/styles.
4. Add a small internal fixture or render test covering the shared primitives.

### Phase 2: reference pages

1. Complete Deal Board as the reference result-page implementation.
2. Complete Fulfillment Pipeline using the same contracts.
3. Confirm that both pages share structure without duplicating CSS blocks.

### Phase 3: Sales and Fulfillment families

1. Migrate remaining Sales pages.
2. Migrate remaining Fulfillment pages.
3. Reconcile route labels, active navigation, browser titles, and destinations.

### Phase 4: remaining operator families

1. Executive and admin routing surfaces
2. Website Ops
3. Advertising and brand analysis
4. HR and access administration
5. Finance outer shell and shared primitives, preserving Finance semantics

### Phase 5: hardening

1. Complete route-by-state QA
2. Keyboard and focus review
3. Desktop visual-regression baselines
4. Mobile migration and validation as a separately scheduled continuation

## Acceptance criteria

1. At 1280px, 1440px, 1600px, and 1920px, both header backgrounds span the
   entire viewport without a visible cutoff.
2. Header content and page content share the same left and right alignment.
3. Global navigation, section navigation, and page content use one documented
   container contract.
4. No authenticated page introduces an undocumented canvas width.
5. Each migrated page places its title, purpose, freshness/context, and primary
   action in the canonical page header.
6. Filterable result pages place scope, search, filters, sort, secondary
   commands, and result count in one command region.
7. Long result tables have contained scrolling, sticky headers, and no global
   horizontal overflow.
8. Default, loading, empty, filtered-empty, partial, stale, error, permission,
   and success states preserve the page structure.
9. Keyboard users can reach navigation, commands, and workspace actions in
   visual order with a visible focus indicator.
10. Route/API behavior, permissions, audit history, tracking, and external-write
    behavior remain unchanged.
11. Finance/Plaid behavior is unchanged.
12. Sales and Fulfillment no longer carry separate copies of equivalent shell,
    command-bar, or table styling.

## Validation plan

- Render the shell at 1280px, 1440px, 1600px, and 1920px and compare the
  viewport edges, header bands, inner alignment, and content gutters.
- Test Deal Board and Fulfillment Pipeline with populated, filtered, and
  filtered-empty datasets.
- Verify sticky headers and contained scroll at a desktop viewport height of
  approximately 900px.
- Navigate all controls using only the keyboard.
- Verify focus, labels, live result-count updates, and status text.
- Run affected route and renderer tests.
- Deploy according to repository rules and repeat production browser checks with
  real, safe application data.

## Recommended defaults and open decisions

### Recommended defaults

- Canonical application width: `1320px`
- Desktop gutter: `24px`
- Full-width header bands with constrained inner containers
- Sales Deal Board as the reference result-page implementation
- Shared Python render helpers plus shared CSS, not a new UI framework

### Open decision

The current `1480px` Sales dashboard variants should either:

1. migrate to `1320px`; or
2. receive one named `AppContainer--wide` exception for evidence that cannot be
   presented effectively at 1320px.

Recommended default: migrate to 1320px first and approve a wide exception only
after a 1280px/1440px usability comparison demonstrates the need.
