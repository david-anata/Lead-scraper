# Outbound Daily Lead Batch Operating System — Build Specification

Status: Proposed, build-ready
Priority: P1 workflow simplification and automation
Primary user: Anata sales/outbound operator
Supersedes the page-level workflow proposed in `13-lead-ops-plan.md` while preserving its sourcing logic
Depends on the persistence guarantees in `19-pull-persistence-recovery-spec.md`

## Outcome

Agent automatically runs the approved outbound recipes each weekday morning,
combines their fresh results into one exact deduplicated Daily Lead Batch, saves
that batch permanently, and delivers the same CSV to the configured email and
Slack destinations. The operator does not need to trigger ordinary pulls or
assemble several files.

The product has four understandable destinations:

1. **Daily Leads** — today’s automation, one daily CSV, and prior daily batches.
2. **Company Library** — the permanent database of every sourced company.
3. **Recipes & ICP** — create, test, schedule, version, and manage sourcing rules.
4. **Outbound Performance** — downstream Instantly results and recipe efficacy.

The legacy **Brand List** page is retired. Manual execution remains available
only as an exception through `Run now` and `Test recipe` controls.

## Verified Current Behavior

- Render invokes the outbound morning endpoint at both possible UTC equivalents
  of 7:00 AM America/Denver; the endpoint accepts only the correct local-time
  invocation and uses a daily marker/lease to avoid a second scheduled run.
- Core ICP is scheduled every weekday. The five trigger recipes are additionally
  scheduled Tuesday and Wednesday. Nothing is scheduled on weekends.
- The scheduler currently persists each recipe as its own run and invokes
  per-pull delivery inside the recipe loop.
- Daily digest logic currently builds from all sendable companies in Company
  Library, not immutable membership in one specific day’s batch.
- The six recipes and their StoreLeads query builders are code-defined.
- Operators can edit some shared ICP, lookback, social, and Amazon values, but
  cannot create a recipe, freely assign its schedule, or clearly manage all
  recipe caps and enabled states through one coherent interface.
- `/admin/outbound/brands` is an older arbitrary-count manual download page and
  duplicates the sourcing function now present in Lead Ops.
- `/admin/outbound/scoreboard` reads Instantly/campaign performance; it is not a
  sourcing page, but its name and navigation placement do not explain that.
- Company Library is the durable cross-run company archive and currently has a
  complete export for Clay.

## Incorporated Vercel Feedback

All unresolved outbound toolbar comments reviewed on 2026-08-21 are requirements
of this specification:

| Comment | Required response |
|---|---|
| “shorten and add pagination” on Recent pulls | Replace the long run table with paginated daily batches; default 10 rows per page. |
| “is this correct — where does it go, can we change it here?” on Slack | Show the resolved Slack workspace/channel and allow an authorized operator to change an approved destination here. |
| “clean this up to be a better formatted form fill” on Tuning | Move tuning into a dedicated, grouped Recipes & ICP form with units, help, validation, preview, and save summary. |
| “trim row length and add pagination” on Change log | Compact each change into a one-line summary, paginate 10 per page, and expose full details in a row disclosure/drawer. |
| “Zero results — no automation — or managing the automation” on Company Library | Give automation its own visible status/control surface and provide classified zero-result explanations. |
| “this doesn’t seem to work” on Brand List | Retire the page and redirect it to Daily Leads; do not maintain a second manual sourcing workflow. |
| “clean way to update; more organized and proper UX/UI” | Adopt the four-page information architecture and the canonical Agent page/form/table patterns. |

## Scope

### Included

- One immutable Daily Lead Batch per scheduled business day.
- Consolidation and domain deduplication across all due recipe runs.
- One downloadable CSV for the daily batch.
- Automatic email attachment and Slack file delivery after batch completion.
- Daily automation status, next run, last success/failure, and retry controls.
- No-code creation and management of recipes using approved StoreLeads signals.
- Centralized ICP management applied to every active recipe.
- Recipe preview, versioning, scheduling, caps, priority, and activation.
- Paginated batch history and settings change history.
- Company Library filtering/export with daily-batch and recipe provenance.
- Renaming and navigation cleanup for Outbound Performance.
- Legacy Brand List redirect and compatibility-period telemetry.

### Non-goals

- Automatically sending prospect emails or starting Instantly campaigns.
- Replacing Clay enrichment or Instantly campaign execution.
- Arbitrary raw StoreLeads query entry by ordinary operators.
- Changing the existing Clay CSV column contract without a separate versioned
  migration.
- Reconstructing exact membership for legacy runs when evidence is insufficient.
- Removing the six current recipes or silently changing their ICP rules.

## Product Information Architecture

The Sales navigation exposes these adjacent items in this order:

1. Daily Leads
2. Company Library
3. Recipes & ICP
4. Outbound Performance

Routes:

- `/admin/outbound` redirects to `/admin/outbound/daily`.
- `/admin/outbound/daily` is the default operator workspace.
- `/admin/outbound/leads` remains Company Library.
- `/admin/outbound/recipes` contains Recipes & ICP.
- `/admin/outbound/performance` is the renamed scoreboard.
- `/admin/outbound/lead-ops` temporarily redirects to Daily Leads while preserving
  filters where possible.
- `/admin/outbound/scoreboard` redirects to Outbound Performance.
- `/admin/outbound/brands` permanently redirects to Daily Leads and records
  redirect usage during the compatibility window.

Navigation must not hide these destinations behind small in-page buttons.

## Target Daily Workflow

### 1. Plan

Before the scheduled time, the system resolves:

- the business date and America/Denver timezone;
- every active recipe scheduled for that weekday;
- the published ICP version;
- each recipe version, cap, priority, and query snapshot;
- delivery destinations and attachment mode;
- StoreLeads, database, artifact, email, and Slack readiness.

The planned recipe set becomes immutable once the batch begins. Later settings
changes apply to the next batch.

### 2. Run recipes

At 7:00 AM Denver time, one scheduler lease creates or resumes the day’s batch.
Each recipe runs independently inside that batch and records its own funnel:
scanned, source-matched, ICP-matched, already sourced, fresh, partial, or failed.

A failed recipe does not erase successful recipe results. The batch becomes
`Needs review` when at least one recipe fails or is partial.

### 3. Consolidate

After all due recipes finish:

- combine all fresh recipe memberships;
- normalize and deduplicate by domain;
- preserve every matched recipe on the company;
- assign `primary_recipe` using highest tier, then configured priority, then a
  stable recipe ID tie-break;
- reconcile recipe output counts against batch membership;
- generate exactly one versioned CSV artifact.

The CSV is generated from immutable batch membership—not from the current
Company Library—and can therefore be re-downloaded unchanged.

### 4. Save and deliver

The system commits batch membership, Company Library provenance, and artifact
metadata before reporting success. Delivery starts only after commit.

For a successful nonzero batch:

- email receives the actual combined CSV attachment plus a short summary;
- Slack receives the same file upload plus a short summary and secure Agent link;
- each destination records attempted, delivered, or failed independently.

No result is treated as a delivery failure. It produces a completed zero-result
batch with a classified explanation and, by default, a summary notification
without an attachment. The operator may disable zero-result notifications.

### 5. Operate

The operator opens Daily Leads and sees the outcome, not implementation detail:

- `Ready for download — 39 companies`;
- `Delivered to david@… and #sales-outbound`;
- `Needs review — Social Surge timed out; 27 companies from other recipes are safe`;
- `No fresh companies — all 18 matches were already in Company Library`;
- `Blocked — database persistence unavailable; StoreLeads was not called`.

## Daily Leads Page

Follow the canonical Agent anatomy: concise header, decision summary, command
bar, primary workspace, supporting history.

### Header

- Title: `Daily Leads`.
- Purpose: `Your automatically sourced, deduplicated companies—ready as one file.`
- Primary action: `Download today’s CSV` only when a nonzero artifact exists.
- Secondary exception action: `Run now` with confirmation of recipes,
  destinations, expected cap, and whether delivery will occur.

### Today status

Show:

- state badge;
- business date and timezone;
- scheduled/started/completed times;
- recipes planned, completed, partial, and failed;
- scanned, matched, previously sourced, and final unique counts;
- duplicates removed across recipes;
- email and Slack delivery state;
- last scheduler heartbeat and next scheduled run.

When blocked, state exactly which dependency failed and the safe next action.

### Recipe contribution

A compact list shows recipe, status, fresh contribution, duplicates shared with
other recipes, and primary failure/zero reason. Full diagnostic funnels live in
batch detail, not the landing-page table.

### Batch history

- One row per Daily Lead Batch, not one row per recipe.
- Default 10 rows per page; options 10, 25, 50.
- Server-side pagination with total results and stable newest-first ordering.
- Filters: date range, status, delivery state, and recipe.
- Columns: date, status, recipes, unique companies, delivery, action.
- Long recipe/error details are truncated with accessible disclosure.
- Selection and `Download selected batches` combine exact stored memberships,
  deduplicate by domain, and preview totals before download.

## Recipes & ICP Page

### ICP tab

Group fields by meaning rather than presenting one long tuning list:

1. **Company size:** annual revenue minimum/maximum, employee minimum/maximum.
2. **Market:** platform, countries, store status, required contact route.
3. **Exclusions:** dropship, print-on-demand, categories/tags.
4. **Qualification:** accepted niches and any additional shared gates.

Every field has a label, unit, current value, recommended range, plain-language
impact, and inline validation. Revenue values display as dollars/year even when
the StoreLeads query uses monthly cents.

Saving requires a change reason, shows an old-versus-new summary, increments the
ICP version, records the actor, and affects only future batches. Provide `Preview
impact` before publish using a bounded, read-only sample.

### Recipes tab

Recipe list columns/cards:

- name and status;
- tier and priority;
- signal;
- scheduled days;
- cap;
- last run/yield;
- actions: View, Edit, Duplicate, Test, Pause.

An authorized operator can create a recipe from approved primitives:

- baseline ICP;
- app installed;
- app removed;
- plan changed/upgraded;
- platform changed;
- social follower growth;
- approved StoreLeads category/tag/technology filters.

Recipe form fields:

- name, description, and operator-visible “why now”;
- supported signal and its parameters;
- tier and priority;
- weekdays and timezone;
- cap per run;
- active/inactive;
- include in Daily Lead Batch;
- optional start/end date;
- safe client-side verification rule when the signal needs it.

`Test recipe` is a bounded preview. It never writes Company Library membership,
suppression, a production run, or delivery. Results show the diagnostic funnel,
sample domains, estimated yield, and query limitations.

Publishing creates a new immutable recipe version. Existing batches retain the
version/query snapshot they used. Editing a recipe never rewrites history.

### Recipe builder limitations

- Ordinary operators cannot enter raw URLs, API credentials, or unrestricted
  StoreLeads parameters.
- The builder only exposes validated filter primitives supported by the current
  pipeline and StoreLeads contract.
- App-install/app-removal filters must respect StoreLeads’ one-app-per-query
  limitation. Multiple apps are executed as bounded subqueries or a documented
  rotation strategy, with expected API call cost shown before publish.
- Weekly-refresh signals default to Tuesday/Wednesday with a warning—not a hard
  prohibition—when scheduled differently.
- Per-recipe caps have an administrative maximum and a displayed projected
  daily/weekly total.
- A global daily unique-company cap prevents recipe proliferation from creating
  unsafe volume. Recommended initial default: 150 unique companies/day, editable
  only by an admin.
- Amazon review remains a separate bounded enrichment stage; its current default
  capacity is 12 companies/day and cannot gate creation of the core daily CSV.

### Change history

- Default 10 changes per page with server-side pagination.
- One compact row: version, date, object, concise change, actor.
- Full old/new values and reason appear in an accessible disclosure/drawer.
- Filters: ICP/recipe/delivery, actor, and date.
- No endless table and no raw internal setting keys as primary labels.

## Delivery Settings

Delivery is managed from Daily Leads through a clearly labeled settings panel
or linked Settings route. It applies to the completed Daily Lead Batch, never to
each recipe independently.

Show and allow authorized editing of:

- automation enabled/paused;
- scheduled time and America/Denver timezone;
- email enabled and recipient list;
- Slack enabled, connected workspace, and resolved channel name;
- whether successful CSVs are attached/uploaded;
- whether zero, partial, and failed batches notify;
- last test and last real delivery result.

Slack must display `Workspace → #channel`, not only “connected.” Channel choices
are restricted to channels available to the configured bot. Changing a channel
is an audited settings write. No hosting-provider or environment-variable
instructions appear in the operator UI.

`Send test` previews the exact destinations and sends a clearly marked synthetic
test message/file; it does not use customer lead data.

## Company Library

Company Library remains the permanent, cross-batch record. It adds:

- daily-batch filter;
- primary/all matched recipes;
- first sourced date and most recent lifecycle state;
- Clay/Instantly state where evidence exists;
- server-side pagination and total count;
- filtered export and `Download all for Clay`;
- direct navigation to Daily Leads, Recipes & ICP, and Performance.

An empty library shows automation readiness, next scheduled run, and `Manage
recipes`; it does not ask the operator to use the retired Brand List.

## Outbound Performance

Rename Scoreboard to Outbound Performance and describe its source and purpose.
It measures downstream outcomes, not sourcing execution.

Minimum views:

- Instantly connection/freshness;
- sends, bounces, replies, positive replies, booked calls;
- funnel from daily batch → Clay → Instantly → response;
- results by recipe and recipe version;
- warning when outcome attribution is unavailable.

Do not present a recipe as effective when outcomes are missing. Use `Not enough
evidence` rather than zero.

## State Model

Daily batch states:

- `Queued`
- `Running`
- `Consolidating`
- `Ready`
- `Needs review`
- `Delivered`
- `Failed`
- `Blocked`

Recipe-run states:

- `Queued`, `Running`, `Complete`, `Partial`, `Failed`, `Skipped`.

Delivery states are per destination:

- `Not requested`, `Queued`, `Delivered`, `Failed`.

Important rules:

- Zero fresh companies is a completed outcome, not a system failure.
- Persistence failure is `Blocked`; StoreLeads must not run when persistence
  preflight fails.
- A partial recipe may produce a safe batch, but the batch is `Needs review`.
- Delivery failure never removes or invalidates the saved artifact.
- Retry delivery reuses the saved artifact and never reruns StoreLeads.
- Retry recipe runs only the failed recipe under the batch’s frozen snapshots,
  then reconsolidates idempotently.

## Data Contract

Add or formalize these entities:

### `outbound_daily_batches`

- ID, business date, timezone, status, trigger, actor;
- ICP version and frozen delivery snapshot;
- planned/started/completed timestamps;
- counts: scanned, matched, previously sourced, recipe rows, unique rows,
  cross-recipe duplicates;
- artifact filename, checksum, row count, byte count, schema version/reference;
- scheduler lease/idempotency key, correlation ID, error code/detail.

Unique constraint: environment + business date + schedule identity. Manual runs
use a separate idempotency key and cannot overwrite the scheduled batch.

### `outbound_daily_batch_runs`

- batch ID, existing run ID, recipe ID/version, order/priority, status, counts,
  diagnostic summary, error classification.

### `outbound_daily_batch_companies`

- batch ID, normalized domain, immutable lead snapshot, primary recipe ID,
  matched recipe IDs, ordering score.

Unique constraint: batch ID + normalized domain.

### `outbound_recipe_definitions` and `outbound_recipe_versions`

- stable recipe identity and lifecycle status;
- immutable published versions containing name, explanation, supported signal,
  validated parameters, tier, priority, schedule, cap, include-in-batch state,
  and author/reason/timestamps.

### ICP versions

Store immutable published ICP snapshots rather than relying only on individual
key/value changes. Preserve the existing change log as audit evidence.

## API Contract

Final names may follow repository conventions, but responsibilities must remain
separate:

- `GET /admin/api/outbound/daily-batches`
- `GET /admin/api/outbound/daily-batches/{id}`
- `GET /admin/api/outbound/daily-batches/{id}/csv`
- `POST /admin/api/outbound/daily-batches/run-now`
- `POST /admin/api/outbound/daily-batches/{id}/retry`
- `POST /admin/api/outbound/daily-batch-exports`
- `GET/POST /admin/api/outbound/recipes`
- `GET/POST /admin/api/outbound/recipes/{id}`
- `POST /admin/api/outbound/recipes/{id}/preview`
- `POST /admin/api/outbound/recipes/{id}/publish`
- `GET/POST /admin/api/outbound/icp`
- `POST /admin/api/outbound/icp/preview`
- `POST /admin/api/outbound/icp/publish`
- `GET/POST /admin/api/outbound/delivery-settings`
- `POST /admin/api/outbound/delivery-settings/test`
- `GET /admin/api/outbound/automation-health`

All mutating routes use authorization, CSRF controls consistent with Agent,
idempotency keys, actor capture, correlation IDs, validation, and audit events.
Pagination is server-side with bounded page sizes.

## Migration and Compatibility

1. Create daily-batch, recipe-version, membership, artifact, and audit schema
   through the deployment migration path—never request-time DDL.
2. Seed the six current recipes as published version 1 without changing query
   behavior, schedules, caps, or CSV shape.
3. Convert current tunable settings into ICP/recipe version snapshots while
   preserving existing version numbers and change history.
4. Keep existing individual runs and Company Library records. New daily batches
   reference new runs only; do not invent batch membership for legacy runs.
5. Switch the scheduler to create one batch and suppress per-recipe delivery.
6. Enable daily-batch delivery only after the same stored artifact passes email
   attachment and Slack upload QA.
7. Add redirects for Lead Ops, Scoreboard, and Brand List. Measure redirect use
   for 30 days before removing legacy route implementations.
8. Preserve old API download routes temporarily for known bookmarks, but never
   let them become the primary UI workflow.

## Security and Permissions

- View Daily Leads/Library/Performance: existing outbound read permission.
- Download/export: outbound export permission.
- Run now/retry: outbound operate permission.
- Create/edit/test recipes: outbound manage permission.
- Publish recipes/ICP, change global cap, automation schedule, or destinations:
  outbound admin permission.
- Never expose StoreLeads, Slack, email, Clay, or Instantly credentials.
- Logs contain IDs/counts/error classes, not lead payloads or contact details.

## Accessibility and Responsive Behavior

- At 390px, today’s state, next action, and download remain readable without
  page-level horizontal scrolling.
- Dense tables use contained horizontal scrolling or responsive cards; users are
  not forced to view a scaled-down desktop table.
- Pagination, filters, disclosures, forms, validation, confirmation, and sticky
  batch actions are keyboard and screen-reader operable.
- Every input has a visible label, unit, description, and associated error.
- Status uses text plus color and the shared Agent vocabulary.
- Loading uses a visible progress state; no control silently appears inactive.
- The support widget respects safe areas around fields, actions, and pagination.
- Reduced-motion preferences are honored.

## Observability

Record structured events for batch planned/started, each recipe started/finished,
consolidation, persistence, artifact creation, delivery per destination, retry,
settings publish, recipe preview/publish, redirect usage, and download/export.

Automation health reports:

- last scheduler heartbeat;
- last scheduled batch start/completion;
- next scheduled run;
- current lease;
- StoreLeads/database/artifact/email/Slack readiness;
- consecutive failure count;
- most recent correlation ID.

Alert on missed scheduled run, overlapping lease, source success without batch
commit, count mismatch, artifact checksum mismatch, or delivery failure.

## Acceptance Criteria

1. One scheduled business day creates at most one scheduled Daily Lead Batch.
2. Every due recipe appears exactly once under that batch.
3. The batch CSV contains the union of fresh recipe results, deduplicated by
   normalized domain.
4. Every CSV company has a primary recipe and retains all matched recipes.
5. The batch CSV re-downloads without calling StoreLeads and retains its checksum.
6. Email and Slack receive the same stored CSV artifact when both are enabled.
7. Delivery is invoked once per batch, not once per recipe.
8. Delivery settings identify the actual email recipients and Slack
   workspace/channel and can be changed only by an authorized user.
9. A delivery retry never reruns StoreLeads.
10. Zero, partial, source failure, persistence failure, and delivery failure have
    distinct states, explanations, and next actions.
11. The scheduler page exposes last heartbeat, last result, next run, and timezone.
12. An operator can create, preview, publish, pause, duplicate, and schedule a
    recipe using supported primitives without a deploy.
13. Recipe preview does not modify Company Library, suppression, runs, batches,
    or delivery history beyond a separate preview audit event.
14. Publishing ICP or recipe changes requires a reason and preserves an immutable
    prior version.
15. Existing six recipes produce the same query and selection behavior immediately
    after migration.
16. Recent daily batches and settings change history default to 10 rows and have
    working server-side pagination.
17. `/admin/outbound/brands` redirects to Daily Leads and is absent from primary
    navigation.
18. Outbound Performance is directly reachable from every outbound page and
    explains its Instantly dependency/freshness.
19. Company Library links directly to Daily Leads and Recipes & ICP; an empty
    library explains automation readiness rather than suggesting manual Brand List.
20. All primary flows pass desktop widths and 390px without page-level overflow,
    clipped controls, or support-widget obstruction.
21. Unauthorized roles cannot publish recipes/ICP, run automation, change
    destinations, or export files.
22. Browser console and deployed runtime logs contain no new outbound errors during
    the complete scheduled-batch and download workflow.

## Validation Plan

### Automated

- Unit tests for schedule resolution across weekdays, DST, weekends, and pause.
- Recipe-builder validation for each supported signal and forbidden raw inputs.
- Regression fixtures for all six seeded recipes.
- Atomic persistence, idempotency, deduplication, primary-recipe tie-break, and
  checksum tests.
- Pagination/filter tests for batches, Company Library, and change history.
- Permission, CSRF, actor, correlation, and secret-redaction tests.
- Email attachment and Slack file-upload contract tests using the same artifact.
- Zero, partial, source error, persistence error, and delivery retry tests.
- Migration tests from the current production-compatible schema in Postgres and
  SQLite test mode.

### Production-safe browser QA

1. Verify scheduler/connection status without triggering a run.
2. Run one explicitly approved low-cap test batch with delivery disabled or sent
   only to a designated test destination.
3. Confirm one batch, expected child recipe runs, exact membership, and one CSV.
4. Re-download twice and confirm no StoreLeads activity/checksum change.
5. Enable test email/Slack delivery and confirm the same file in both.
6. Test combined historical export and Company Library export.
7. Preview and publish a reversible test recipe version, then verify audit history.
8. Verify legacy redirects and all four navigation destinations.
9. Verify 1280px, 1440px, and 390px; keyboard, labels, focus, overflow, and console.
10. Scan deployment/runtime logs for outbound errors and correlation completeness.

## Rollout Phases

### Phase 1 — Daily batch foundation

Add batch schema, immutable membership/artifact, one-batch scheduler orchestration,
status/health, and paginated history. Preserve existing recipe logic.

### Phase 2 — One-file delivery

Suppress per-recipe delivery. Deliver the stored batch artifact once through
email/Slack, expose resolved destinations, and add safe retry/test delivery.

### Phase 3 — Information architecture

Launch Daily Leads, simplify Company Library, expose direct navigation, rename
Performance, and redirect Brand List/old Lead Ops/Scoreboard routes.

### Phase 4 — ICP and recipe management

Seed/version existing recipes, launch grouped ICP forms and safe recipe builder,
add preview/publish/pause/duplicate/schedule, audit, and pagination.

### Phase 5 — Outcome feedback

Connect batch/recipe versions to Clay and Instantly evidence so Performance can
show which recipes create qualified contacts, replies, and booked calls.

Each phase is independently deployable and must pass production visual QA. Do
not enable automatic external delivery until Phase 2 end-to-end verification is
complete.

## Decisions and Recommended Defaults

1. **Daily time:** keep 7:00 AM America/Denver.
2. **Days:** weekdays only; trigger recipes default Tuesday/Wednesday.
3. **Daily cap:** 150 unique companies maximum until sending capacity supports more.
4. **Zero-result notification:** send summary, no CSV attachment.
5. **Partial batch:** save/download successful results, label `Needs review`, and
   notify with the failed recipe named.
6. **Duplicate attribution:** retain all matches; highest tier/priority is primary.
7. **Recipe creation:** approved filter primitives only; no raw query editor.
8. **History page size:** 10 default, 50 maximum.
9. **Legacy pages:** redirect for 30 days, then remove implementations after usage
   reaches zero.
10. **Amazon:** keep as bounded enrichment after sourcing; do not hold the daily
    lead CSV hostage to Amazon capacity.

## Success Measures

- At least 95% of business days with source matches produce one downloadable
  Daily Lead Batch before 8:00 AM Denver.
- 100% of successful daily batches have exact immutable membership and checksum.
- 100% of enabled delivery destinations record a visible outcome.
- Zero duplicate scheduled batches for the same business date.
- Zero per-recipe notification bursts during scheduled automation.
- Median time for an operator to find and download today’s leads is under 30 seconds.
- Recipe/ICP changes requiring a deploy falls to zero for supported primitives.
- Every zero-result batch has a classified explanation.
