# Building contract workspace and section cleanup

Status: proposed, 2026-07-29. Supersedes nothing; extends
`docs/building-agreement-payment-readiness.md` and applies
`docs/agent-design-system-full-migration-spec.md` to the Building family.

## Problem

Contracts cannot be tracked or created in Agent today.

Verified current behavior:

- No contract list exists. The only listing is a five-column table keyed by
  reservation ID (`api/building_agreement_readiness_router.py:826-833`): id,
  preparation status, twelve-character checksum, payment status, amount. No
  customer, space, date, owner, expiry, filter, search, or sort.
- No contract detail route exists anywhere in the app.
- No contract text is stored. A template is an opaque repository reference
  string (`models/entities.py:2241`); the body lives outside the system.
- Creation requires typing six opaque identifiers into a bare form
  (`api/building_agreement_readiness_router.py:869-878`).
- Three status fields can disagree: `BuildingAgreement.status` written by a
  free-text control-room form (`services/building_page.py:716`),
  `BuildingAgreement.preparation_status` written by the governed path, and the
  denormalized `BuildingReservation.agreement_status` (`models/entities.py:2023`).
- The governed page is reachable only from a link buried inside the
  "Bookings and holds" panel header (`services/building_page.py:1640`).
  Building navigation has two entries (`services/admin_nav.py:74-77`).
- `/admin/building` renders roughly thirty panels from one 1662-line function
  (`services/building_page.py:177`) behind one handler that queries every
  Building table in a single request (`api/building_crm_router.py:4756`).
  About fifty POST routes redirect back to it
  (`docs/agent-route-state-inventory.md:81-132`).
- Building embeds its own document shell and inline CSS instead of
  `services/ui_shell.py`, which only `settings_page.py` and `access/pages.py`
  currently use.

Blocking business state: launch decision `agreement_template` is unresolved.
The 2025 Vivint agreement is customer-specific and not reusable; Dropbox Sign
is recommended but not approved (`services/building_page.py:865`). No approved
template exists, so no package can be prepared today.

## Outcome

An operator can find every contract, see its true state, open one contract and
understand it completely, author and approve a reusable template with real
contract text, and produce an approved, checksummed document — without leaving
Agent and without any provider write.

The Building section becomes a set of purposeful pages instead of one scroll,
so each workflow can be validated on its own.

## Users

- Owner/operator (David): authors templates, approves contracts, needs an
  at-a-glance answer to "what is outstanding and what is blocked".
- Building operator: prepares packages against real reservations, tracks
  signature and deposit state.
- Finance: reads contract value and deposit terms; unchanged permissions.

## Scope

Four phases, each independently shippable and validatable.

### Phase 1 — Contract workspace

- New routes `GET /admin/building/contracts` and
  `GET /admin/building/contracts/{agreement_id}`.
- Index columns: customer, space, contract type, event or term dates, value,
  deposit required, agreement state, payment-readiness state, owner, updated.
- Command bar: text search (customer, space, reservation id), status filter,
  type filter, visible result count, and a stable default sort of most recently
  updated first.
- Detail page: reconciled status header, frozen package snapshot rendered as
  labeled terms rather than raw JSON, full checksum, template identity and
  version, linked reservation/quote/contact/space, payment-readiness record,
  and the `BuildingAuditEvent` history for that agreement.
- All contract write actions move onto the detail page: prepare, review,
  approve, record provider evidence. Each keeps its existing typed
  confirmation, permission, idempotency, and audit contract.
- Preparation form replaces typed identifiers with pickers scoped to eligible
  records: reservations with an active unexpired hold, frozen quote drafts for
  the chosen reservation, approved templates. Idempotency key is generated and
  shown, not typed.
- Remove the free-text "Record agreement" form from the control room. The
  control room's Agreement column becomes a link to contract detail. Remove the
  buried readiness link.
- Add `Contracts` to the Building navigation section.
- `/admin/building/agreement-readiness` redirects permanently to
  `/admin/building/contracts`.

### Phase 2 — Template editor

- Add to `building_agreement_templates`: `contract_type` (`event` or
  `membership`), `body_markdown`, `clauses_json`, `rendered_preview_checksum`.
  `template_reference` stays optional for externally held documents.
- Routes: `GET /admin/building/contracts/templates`,
  `GET|POST /admin/building/contracts/templates/{template_id}`.
- Editor: title, type, version, body authored in Markdown with a merge-field
  palette; ordered named clauses; server-side validation that every `{{field}}`
  token resolves to the allow-list for that contract type
  (`api/building_agreement_readiness_router.py:65-82`); unknown tokens are a
  field error, never a silent blank.
- Live preview renders the template against a selected eligible reservation and
  shows every merge value with its source record.
- Draft to in-review to approved to retired transitions, typed confirmations,
  approval evidence, and immutability of approved versions are preserved
  exactly. Editing an approved version is refused; the editor offers "start
  version N+1" instead.
- Package preparation additionally stores the deterministic rendered document
  text and its own checksum inside `package_snapshot_json`, so an approved
  contract has verifiable content.
- `GET /admin/building/contracts/{agreement_id}/document.pdf` renders the frozen
  snapshot for an approved package. Download only. No send, no signature
  request, no provider object.

### Phase 3 — Building section decomposition

Split `/admin/building` into purposeful routes, each querying only its own data
and rendered through `services/ui_shell.render_operator_document` with the
canonical primitives already present in `static/admin.css` (`app-page-header`,
`app-metric-strip`, `app-command-bar`, `app-data-workspace`, `app-table`,
`app-status--*`, `app-state-panel`).

| Route | Owns |
| --- | --- |
| `/admin/building` | Operator queue, performance, launch readiness, bookings and holds, inventory summary |
| `/admin/building/contracts` | Phase 1 and 2 |
| `/admin/building/crm` | Contacts, relationships, merges, roster imports, audiences, privacy requests |
| `/admin/building/campaigns` | Campaign drafting, preview, approval, scheduling, delivery, email events |
| `/admin/building/billing` | Billing accounts, schedules, invoices, collections, adjustments |
| `/admin/building/operations` | Service requests, checklists, tours, calendar projection |
| `/admin/building/catalog` | Spaces, media, offerings, rate plans |
| `/admin/building/content` | Unchanged |

Existing POST routes keep their paths and contracts. Only their redirect target
changes to the owning page, preserving the notice/error query convention.

### Phase 4 — Coworking membership contracts

- `contract_type=membership` end to end.
- Membership merge fields: `member_name`, `member_email`, `workspace`,
  `desk_count`, `term_start`, `term_end`, `monthly_rate`, `auto_renew`,
  `notice_period_days`, `included`, `addons`, `cancellation_policy`,
  `tax_terms`.
- Membership preparation preconditions replace the event-hold and event-quote
  requirements with an active workspace reservation, an approved membership
  rate plan, and an active responsible contact. The event path is unchanged.

## Non-goals

- No e-sign provider integration. Dropbox Sign remains unapproved; signature
  evidence stays manually recorded as today.
- No sending, emailing, invoicing, charging, or booking confirmation from any
  contract surface. The readiness invariant in
  `docs/building-agreement-payment-readiness.md:3-5` holds.
- No change to Finance, Plaid, QBO, Stripe, HubSpot, or calendar behavior.
- No change to existing permission names or route paths for current POST routes.
- No new frontend framework or build step. Server-rendered HTML with
  progressive enhancement, per `DESIGN.md:133`.

## Constraints

- `AGENTS.md`: preserve business logic, log external writes, never mutate
  records without an audit trail, keep changes incremental, add docstrings and
  type hints.
- `DESIGN.md:50-77`: canonical shell, 1320px container, page header then
  decision summary then command bar then workspace; shared state vocabulary of
  Ready, Queued, Running, Needs review, Confirmed, Delivered, Failed, Blocked,
  Stale.
- `DESIGN.md:86-88`: long queues use filters, row limits, and a visible result
  count, never endless scroll.
- Permissions already exist and are reused unchanged:
  `building.agreements.prepare`, `building.agreements.approve`,
  `building.payments.prepare`, with legacy `building.manage` compatibility
  (`services/access/catalog.py:81`).
- Schema changes are additive only and applied through the existing additive
  column path in `models/database.py:391` plus `create_all` for new tables.
- `scripts/generate_agent_route_inventory.py` must be re-run and
  `docs/agent-route-state-inventory.md` committed whenever routes change.

## Assumptions

- Markdown plus named clauses is sufficient contract authoring fidelity for
  now; a rich-text or clause-library product is deferred.
- PDF rendering may add one dependency. If no acceptable pure-Python option is
  available, phase 2 ships print-optimized HTML at
  `/document` and the PDF is deferred rather than blocking the editor.
- Existing agreements with an empty `package_checksum` are legacy free-text
  records; they appear in the index labeled `Unverified` and are read-only.

## States

Every new page implements: loading is not applicable (server-rendered), empty,
filtered-empty, permission-denied, error, partial evidence, and blocked.

- Empty index: "No contracts yet." plus the concrete next action, which is
  approving a template when none is approved.
- Blocked banner on the index and template list while no approved template
  exists, naming the unresolved `agreement_template` launch decision and
  linking to it. This is the honest reason preparation fails closed.
- Unverified rows are visibly distinguished and offer no governed action.
- Permission denied uses the existing transition document, not a raw 403.
- Responsive: tables scroll inside a contained region; the command bar stacks
  at 900px; no horizontal page overflow.

## Acceptance criteria

Phase 1

1. `/admin/building/contracts` lists every `BuildingAgreement` with customer,
   space, type, dates, value, both states, owner, and updated time.
2. Search by customer name, space name, or reservation id filters the list, and
   the result count reflects the filter.
3. Status and type filters combine with search; clearing them restores the full
   list.
4. `/admin/building/contracts/{id}` shows the reconciled state, the snapshot as
   labeled terms, the full checksum, the linked records, and the audit history.
5. Prepare, review, approve, and record-evidence actions succeed from the detail
   page with identical typed confirmations, permissions, and audit events as the
   current routes.
6. The preparation form offers only eligible reservations, quotes, and approved
   templates, and generates the idempotency key.
7. The control room no longer contains the free-text agreement form, and its
   Agreement column links to contract detail.
8. `Contracts` appears in Building navigation; the old readiness URL redirects.
9. All existing tests in `tests/test_building_agreement_readiness.py` and
   `tests/test_building_admin_operations.py` still pass.

Phase 2

10. A template can be authored with body text and clauses, saved as a draft,
    and previewed against a real reservation with every merge value sourced.
11. An unknown `{{token}}` produces a field error naming the token and the
    allowed fields; the draft is not saved.
12. An approved template cannot be edited; the editor offers a new version.
13. Preparing a package stores rendered document text plus its checksum in the
    snapshot, and the detail page displays the rendered contract.
14. The document endpoint returns the frozen content for approved packages only
    and 409s otherwise. No provider object is created by any of it.

Phase 3

15. Each Building route renders through `ui_shell.render_operator_document`
    with exactly one `h1` and one `main`.
16. Each route issues only the queries its own page needs; the control-room
    handler no longer loads campaigns, contacts, invoices, or privacy requests.
17. Every existing Building POST route path and response contract is unchanged;
    only redirect targets differ.
18. `docs/agent-route-state-inventory.md` is regenerated and committed.

Phase 4

19. A membership template can be authored, approved, and prepared against a
    workspace reservation without an event hold or event quote.
20. Event preparation behavior is byte-for-byte unchanged.

## Validation plan

1. `python -m pytest tests/ -q` from the Agent repo root after each phase.
2. New tests per phase, following the existing file conventions:
   - `tests/test_building_contract_workspace.py`: index filtering and counts,
     detail rendering, action parity, redirect, permission denial.
   - `tests/test_building_contract_templates.py`: authoring, merge validation,
     immutability, preview determinism, rendered snapshot checksum.
   - `tests/test_building_section_routes.py`: each split route returns 200,
     has one `h1`, and every legacy POST path still resolves.
3. Run the app locally on port 8010 and walk: navigate to Contracts from nav,
   observe the blocked-template banner, author and approve a template, prepare
   a package against a seeded reservation, approve it, open the rendered
   document.
4. Regenerate the route inventory and confirm the diff contains only intended
   routes.
5. Confirm no new outbound provider calls: grep the diff for Stripe, HubSpot,
   Dropbox Sign, calendar, and email clients.

## Rollout

Incremental commits per phase on a dedicated branch. Deploy-by-default applies
per `AGENTS.md:150-157`: push after validation. Phases 1 and 3 are pure UI and
routing over existing data and carry low risk. Phase 2 adds columns additively.
No data migration or backfill is required; legacy rows degrade to `Unverified`.

## Open decisions

| Decision | Recommended default |
| --- | --- |
| Does the editor replace the external repository reference model? | No. Augment. `body_markdown` becomes the primary source; `template_reference` stays optional for externally held paper. This removes the "no contract text anywhere" gap without invalidating existing approved evidence. |
| Does the contract surface ever send or request signature? | No, this phase. Ship approved PDF plus manual evidence recording. Revisit when an e-sign provider is actually approved. |
| Event or coworking contracts first? | Event first. The plumbing exists and can be validated end to end; membership follows as phase 4 with the same shapes. |
| PDF dependency? | Prefer a pure-Python renderer. If none is acceptable, ship print-optimized HTML and defer the PDF rather than block the editor. |
