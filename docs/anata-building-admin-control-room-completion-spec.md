# Anata Building Admin Control Room Completion

Status: Build-ready product and technical specification
Prepared: 2026-07-26
Primary application: `agent.anatainc.com`
Public storefront: `anata-building.vercel.app`
Supersedes for implementation planning: the unshipped assumptions and phase plan in `docs/anata-building-operating-system-spec.md`

## 1. Decision Summary

Complete the existing Building OS as the private administrative source of truth
for the public coworking and event journeys. Preserve the working inventory,
availability, intake, CRM, booking, calendar, operations, billing, communication,
privacy, analytics, and audit contracts. Add the missing merchandising,
Marketplace, event-cost, document/e-signature, publication, and permission
domains, then reorganize the current single long control-room page into bounded
operator workspaces.

The release is not a rewrite. It is an additive completion program with four
phases:

1. publication-safe catalog and media;
2. lead-to-tour and event commercial configuration;
3. documents, signing, money, and fulfillment;
4. permissions, reporting, and operational hardening.

Agent remains authoritative for commercial availability, holds, bookings,
content approval state, and why money is due. Google Calendar remains a
projection. Stripe remains the payment processor. HubSpot remains the external
sales CRM during this program. The public website consumes only published,
allow-listed snapshots.

## 2. Verified Baseline

This baseline was verified from the repository on 2026-07-26, especially
`sales_support_agent/models/entities.py`, the `building_*_router.py` modules,
`services/building_page.py`, `services/access/catalog.py`,
`services/building_*.py`, the Stripe and Google Calendar adapters, and the
Building test suite.

### 2.1 Already implemented and to be preserved

| Capability | Verified implementation | Completion work |
| --- | --- | --- |
| Space inventory | `BuildingSpace`; internal space upsert; status, floor, capacity, features, accessibility, public flag | Add stable slugs, structured capacities/layouts, amenities, merchandising readiness, optimistic locking, archive rules, and a dedicated inventory UI |
| Offerings and room pricing | `BuildingOffering`, versioned `BuildingRatePlan`; public starting price/range; deposits, cancellation terms, included items, effective dates; approved plans lock | Add explicit space/offering applicability, price components and add-ons, taxes/fees, event cost model, approval history, and conflict-safe effective dating |
| Availability | `BuildingAvailabilityBlock`, reservation-linked blocks, public projection, conflict checks, expiring-hold jobs | Add recurring maintenance/closures, turnover buffers, multi-space conflicts, availability search endpoint, calendar UI, and stale-publication guard |
| Media placement | `BuildingSpace.media_json`; placement, sort order, public approval, alt text; public payload filtering | Normalize into a media table, add asset library, renditions, focal point/crop, license/source, lifestyle vs room classification, usage map, and publish readiness |
| Public catalog | Public offerings, offering detail, availability, and inquiry endpoints | Add versioned publication snapshots, preview/diff/rollback, tenant logos/reviews/lifestyle content, stronger cache revisioning, and website acknowledgment |
| Lead intake and routing | Idempotent public intake, assisted Marketplace/Eventective/manual intake, owner and response SLA, HubSpot retry/recovery, lifecycle analytics | Add listing records, routing rules, source-link health, assignment explanations, escalation/fallback, and field-level intake validation by journey |
| Building CRM and audiences | Contacts, relationships, preferences, suppression, dedupe/merge, roster preview/apply, explainable segments, campaign preview/test/approve/schedule/send/retry, Resend webhooks | Add lead-routing policies, reusable audience templates, operational-message templates, per-contact timeline, and audience/reporting UI separation |
| Tours | `BuildingTour` creation/update and calendar-friendly schedule; tours explicitly do not block inventory | Add staff/resource availability, configurable tour types/durations/buffers, confirmation/reschedule/cancel links, reminders, and no-show follow-up |
| Holds and bookings | `BuildingReservation` states, transitions, soft holds, expiry, proposals, agreements, deposit evidence, readiness rules | Add atomic multi-space booking, clearer quote/contract/payment gates, booking amendments/cancellation, customer-facing status pages, and concurrency tokens |
| Calendar projection | Persistent projection queue and Google Calendar create/update/delete adapter; Agent remains authoritative | Add tours, staff/resource calendars, projection health controls, replay/dead-letter handling, and human-readable diff |
| Quotes/proposals | Versioned `BuildingProposal`, rate-plan snapshots, line items/terms, sent/accepted/declined states | Add calculated quote builder, event costs/add-ons/discount policy, branded rendering, secure recipient link, expiry, revisions, and acceptance handoff |
| Agreements | `BuildingAgreement` records signature status/evidence and gates booking transitions | Add templates, clauses, merge fields, rendered artifacts, provider envelopes, signer evidence, webhook processing, amendments, and void/resend |
| Billing | Billing accounts and approved locked schedules, Stripe invoice creation with preview/idempotency, invoice/payment state, QBO export/link, collections, reminders | Add invoice detail and delivery controls, payment links/status pages, partial/refund workflows wired to Stripe, taxes/fees, receipts, and reconciliation visibility |
| Refunds/credits/write-offs | Audited adjustment requests, two-person approval, provider/accounting evidence fields | Execute supported Stripe refunds idempotently, distinguish requested/provider-confirmed/posted evidence, enforce financial permission and amount limits |
| Communications | Campaign delivery, collection reminders, preference/suppression, Resend delivery events | Add event-driven transactional templates, recipient timeline, manual compose/send gate, customer portal notices, and failure queue |
| Operational checklists | Reservation checklists/items, required/waived/completed states, event/move-in/move-out support; service requests | Add versioned checklist templates, automatic instantiation, relative due times, dependencies, staff assignment, attachments, escalation, and closeout |
| Audit and reporting | `BuildingAuditEvent`, external-write logs, funnel/source/hold/contract/deposit/revenue analytics, operator queue | Add one cross-domain history service, export, field diffs, publication/delegation events, report filters, utilization and forecast measures |
| Staff access | Existing RBAC catalog and route guard; current Building grant is `building.manage`; finance access is separately checked for adjustments | Replace the single broad grant with capabilities for view, inventory, content, CRM, bookings, documents, operations, billing, publication, reporting, and audit |

### 2.2 Material gaps

The following requested capabilities have no complete native domain or operator
workflow:

- tenant logo management and approved placement;
- structured reviews/testimonials, consent, attribution, and expiration;
- reusable lifestyle asset library independent of a physical room;
- Facebook Marketplace listing inventory, canonical links, renewal/expiry,
  copy/media snapshots, and lead attribution health;
- configurable lead-routing rules and workload/fallback behavior;
- resource-aware tour scheduling and public reschedule/cancel flow;
- event cost configuration and margin-aware quote calculation;
- native contract templates, clause versions, merge fields, document rendering,
  e-signature provider envelopes, and webhook evidence;
- full Stripe refund execution and customer payment status experience;
- checklist templates and automated relative scheduling;
- granular Building permissions;
- immutable publication bundles with preview, validation, approval, website
  acknowledgment, rollback, and emergency unpublish.

The current Building admin is one server-rendered page containing nearly every
workflow. It proves the contracts but will not scale safely as the missing
domains are added. It needs route-level subpages while keeping a single Building
navigation section and shared design primitives.

### 2.3 Production constraints

- Existing route paths and response behavior remain compatible.
- Existing tables and records are migrated additively; no destructive reset.
- Existing approved rate-plan snapshots, campaign recipient snapshots, proposal
  versions, agreement evidence, Stripe events, and audit records are immutable.
- External writes require idempotency, actor identity, a preview where practical,
  typed confirmation for high-consequence actions, and an audit event.
- Database boot-time `create_all` and ad hoc additive columns currently provide
  compatibility. This completion program introduces ordered, recorded migrations
  before adding the new production tables; startup checks may create tables only
  in local/test environments after the migration runner exists.
- No public endpoint exposes tenant identity, internal costs, margins, unpublished
  content, hold owners, staff data, contract details, or payment evidence.
- The Building site secret stays server-to-server. Browser-facing secure actions
  use scoped, expiring, revocable tokens.
- UI follows `DESIGN.md`: calm operator system, canonical shell, visible
  freshness/evidence, accessible native controls, and no new UI framework.

## 3. Problem, Users, Scope, and Non-goals

### Problem

The implemented core can operate leads, bookings, and billing, but staff cannot
yet manage the full public merchandising surface or complete a customer journey
from configured event economics through rendered agreement, signature, payment,
readiness, and safe publication. Broad permissions and a single-page admin also
make consequential actions harder to review and delegate.

### Primary users

- Building administrator: configures inventory, content, policy, staff access,
  integrations, and publication.
- Sales/community operator: handles leads, tours, contacts, quotes, holds, and
  customer communication.
- Event operator: configures event requirements, costs, bookings, calendars,
  checklists, and closeout.
- Finance operator: approves schedules, creates invoices, manages collections
  and refunds, and verifies accounting evidence.
- Content operator: manages copy, room media, lifestyle assets, logos,
  testimonials, listings, and publication drafts.
- Leadership/report viewer: reads funnel, utilization, source, revenue, payment,
  and operational performance without mutation access.

### In scope

All capabilities named in the objective, their production-safe data/API/admin
contracts, migration path, acceptance criteria, and validation.

### Non-goals

- Replacing HubSpot, Google Calendar, Stripe, QuickBooks, Resend, or the public
  Next.js site in this program.
- Native instant booking before pricing, conflicts, cancellation, fraud, and
  staffing policies are approved.
- A general digital asset manager, general contract lifecycle platform, or
  general accounting ledger.
- Automatic Marketplace scraping or posting that violates platform terms.
- AI-decided pricing, discounts, availability, lead eligibility, contract
  language, refunds, or publication. AI may draft copy or summarize evidence;
  deterministic rules and human approval control state.
- A tenant self-service portal beyond secure quote, agreement, payment, tour,
  and booking-status actions required by these journeys.

## 4. Target Information Architecture

Keep `/admin/building` as the daily control room. Add these authenticated
subpages:

| Route | Purpose | Primary capability |
| --- | --- | --- |
| `/admin/building` | Metrics, source freshness, conflicts, and prioritized next-action queue | `building.view` |
| `/admin/building/inventory` | Spaces, offerings, availability calendar, rate plans, closures | `building.inventory.manage` |
| `/admin/building/content` | Copy, room media, lifestyle assets, logos, testimonials, usage map | `building.content.manage` |
| `/admin/building/marketplace` | Marketplace listings, links, status, refresh dates, source performance | `building.marketplace.manage` |
| `/admin/building/leads` | Lead queue, routing, CRM identity, audiences, communications | `building.crm.manage` |
| `/admin/building/tours` | Tour schedule, hosts/resources, reminders, outcomes | `building.tours.manage` |
| `/admin/building/bookings` | Holds, quotes, agreements, payments, events/occupancies | `building.bookings.manage` |
| `/admin/building/calendar` | Agent calendar, Google projection health, conflicts, replay | `building.calendar.manage` |
| `/admin/building/documents` | Quote and contract templates, versions, envelopes, signing evidence | `building.documents.manage` |
| `/admin/building/operations` | Checklist templates/runs, service requests, closeout | `building.operations.manage` |
| `/admin/building/billing` | Accounts, schedules, invoices, payments, collections, adjustments | `building.billing.view` plus write capabilities |
| `/admin/building/publish` | Draft bundle, validation, diff, approval, publish, rollback | `building.publish` |
| `/admin/building/reports` | Funnel, utilization, source, revenue, collection, operations | `building.reports.view` |
| `/admin/building/audit` | Filterable immutable history and export | `building.audit.view` |
| `/admin/building/settings` | Routing, event costs, tour policies, integrations, permissions links | scoped settings capabilities |

Every list page provides search, filters, sort, result count, pagination, source
freshness, export where justified, and a filtered-empty state. Detail pages put
current state and next action first, followed by commercial/customer evidence
and activity history. Desktop is primary; at 768px and below tables become
contained horizontal regions or labeled record lists without hiding actions.

## 5. Target Domain Model and Migrations

### 5.1 Migration foundation

Before feature tables:

1. add `schema_migrations(version, checksum, applied_at, applied_by)`;
2. add an ordered migration runner with PostgreSQL and SQLite test coverage;
3. record a baseline version representing the current Building schema;
4. retain `_ensure_building_tables` temporarily as a compatibility check, not
   the production migration mechanism;
5. abort startup with a clear health error if production schema is behind and
   automatic migration is disabled.

All new mutable resources receive `created_at`, `updated_at`, `created_by`,
`updated_by`, and integer `version` for optimistic concurrency. Publicly
publishable resources also receive `publication_state` and `published_revision`.
Archive instead of delete when a record has commercial or publication history.

### 5.2 Inventory and availability

Add:

- `building_space_layouts`: space, layout type, seated/standing capacity,
  public label, sort order.
- `building_space_offerings`: explicit many-to-many applicability and default.
- `building_add_ons`: code, name, unit, public price, internal cost, tax class,
  active/effective dates.
- `building_rate_plan_add_ons`: applicability, included quantity, min/max.
- `building_closure_rules`: one-time or recurring closure/maintenance rule,
  affected spaces, buffers, reason, publication behavior.

Extend `BuildingAvailabilityBlock` with `buffer_before_minutes`,
`buffer_after_minutes`, `parent_rule_id`, `version`, and `superseded_by_id`.
Do not use `BuildingSpace.status` alone to represent dated availability.

Availability calculation order:

1. active/retired space state;
2. closures and maintenance;
3. occupied or confirmed bookings;
4. contract-pending and approved holds;
5. setup/teardown and turnover buffers;
6. public display policy.

Writes acquire a database transaction and recheck overlap immediately before
commit. A stale version returns `409` with the current record and conflict
summary. Multi-space events succeed atomically or not at all.

### 5.3 Content, logos, reviews, and assets

Replace future writes to `BuildingSpace.media_json` with normalized records:

- `building_assets`: type (`room`, `lifestyle`, `logo`, `floor_plan`, `video`),
  original URL/storage key, checksum, dimensions, duration, source/license,
  photographer/credit, alt text, focal point, status, review/expiry dates.
- `building_asset_renditions`: width/height/format/storage URL/checksum.
- `building_content_placements`: asset, target type/id, slot (`hero`, `card`,
  `gallery`, `floor_plan`, `testimonial`, `tenant_logo`, `lifestyle`), locale,
  sort order, crop/focal overrides, publication state.
- `building_tenant_profiles`: relationship/company reference, public display
  name, logo asset, link, consent evidence, approval/expiry.
- `building_testimonials`: quote, person display name/title/company, source,
  rating when applicable, consent evidence, obtained/expiry dates, allowed
  channels, approval state.

Migration copies existing `media_json` entries into asset and placement rows,
preserving IDs, placement, order, approval, alt text, URL, and audit evidence.
Public serialization uses normalized data after parity tests pass; retain
read-only fallback for one release.

Publication readiness rejects missing alt text, missing consent/license,
expired approval, duplicate hero placement, inaccessible media, or unapproved
asset. Tenant/logo/testimonial source contacts remain private; only explicitly
approved display fields publish.

### 5.4 Marketplace listing management

Add `building_marketplace_listings`:

- platform (`facebook_marketplace` initially);
- external listing ID and canonical URL;
- linked offering/spaces;
- title, description, price presentation, location text, contact route;
- ordered asset snapshot;
- status (`draft`, `active`, `paused`, `expired`, `removed`, `needs_review`);
- posted/verified/refresh/expiry timestamps;
- owner;
- platform policy/notes;
- immutable published snapshot hash.

Add `building_marketplace_checks` for URL/status verification attempts and
errors. Phase 1 is assisted management: Agent stores the reviewed listing/link,
opens the external platform, tracks refresh work, and attributes inquiries.
No automated Facebook write is included until an approved official integration
and terms review exist.

### 5.5 Lead routing, audiences, and tours

Add:

- `building_routing_rules`: priority, active/effective dates, journey, source,
  offering, capacity/budget/date predicates, owner/team, SLA, fallback owner.
- `building_routing_decisions`: inquiry, rule version, evaluated facts,
  chosen owner/SLA, explanation, fallback/escalation.
- `building_tour_types`: duration, buffer, eligible spaces, required host role,
  lead time, cancellation window, active/public state.
- `building_staff_resources`: app user, calendar/resource IDs, working hours,
  blackout rules, eligible tour/event roles.
- `building_tour_tokens`: sealed hashed token, action scope, expiry, revocation,
  use timestamp.

Routing is deterministic, ordered, previewable against sample/open leads, and
audited. No match assigns the configured fallback and creates a visible
configuration warning. Reassignment never erases the initial decision.

Tour slot search intersects tour policy, space visitability, staff/resource
working hours, existing tours, and projected busy time. Booking inventory is
not blocked. Public confirmation/reschedule/cancel actions use scoped tokens
and send transactional notifications.

Existing segments/campaigns remain. Add reusable segment templates and
transactional communication templates, but never treat possession of an email
address as marketing consent.

### 5.6 Event costs and quote calculation

Add:

- `building_event_cost_profiles`: versioned defaults by offering/space;
- `building_event_cost_items`: category (`labor`, `cleaning`, `security`,
  `equipment`, `food_beverage`, `vendor`, `utilities`, `other`), unit,
  internal cost, customer price, quantity rule, taxable flag, required/optional;
- `building_discount_policies`: allowed type/range, approval threshold,
  compatible offerings, effective dates;
- `building_quote_calculations`: immutable input, rate/add-on/cost snapshots,
  subtotal, fees, taxes, discounts, deposit, internal cost, margin, warnings.

The quote builder derives customer lines from approved rate plans/add-ons and
event requirements. Internal costs and margin never appear in public payloads.
Below-threshold margin or out-of-policy discount blocks sending until an
authorized approver records a reason. Recalculation creates a new proposal
version; it never mutates a sent version.

### 5.7 Documents, contracts, and e-signature

Add:

- `building_document_templates`: kind, name, journey/offering applicability,
  active state.
- `building_document_template_versions`: immutable source artifact, allowed
  merge fields, clause snapshot, checksum, reviewed/approved by/at.
- `building_clause_versions`: clause key, text, applicability, legal approval
  evidence.
- `building_documents`: proposal/agreement/reservation links, template version,
  merge snapshot, rendered artifact URL/checksum, state.
- `building_signature_envelopes`: provider, provider envelope ID, document,
  signer snapshot, state, sent/expiry/completed/voided timestamps.
- `building_signature_events`: unique provider event ID, type, payload hash,
  received/processed/error state.

Provider integration sits behind `SignatureProvider` with create, resend, void,
get-status, and verify-webhook operations. Provider selection is configuration,
not embedded business logic.

Only approved template versions render sendable documents. Merge validation
fails closed. Rendered artifacts and signer identities are immutable. A
provider-completed event marks an agreement signed only after authentic webhook
verification and artifact/evidence retrieval. Manual signature evidence is an
explicit privileged fallback and is labeled manual in every UI/report.

### 5.8 Payments, refunds, and communications

Extend billing with:

- invoice line table if provider payload lines are currently only JSON;
- public payment/status token and receipt artifact references;
- refund provider object ID and idempotency key on adjustments;
- payment/refund failure and retry state;
- transactional communication log linked to contact, reservation, invoice,
  tour, or envelope.

Refund execution requires `building.billing.refund`, an approved adjustment,
remaining refundable provider-confirmed value, typed confirmation, and a stable
idempotency key. Provider success is not posted bank evidence. QBO linkage and
posted evidence remain separately visible.

Add versioned templates for inquiry acknowledgment, lead assignment alert, tour
confirmation/reminder/change, quote sent/expiring, signature request/reminder,
hold expiring, booking confirmation/change/cancellation, invoice/receipt/refund,
event readiness, and post-event follow-up. Each send records template version,
recipient, classification, provider ID, actor/trigger, and delivery state.
Operational messages ignore marketing unsubscribe only when their configured
classification and linked customer relationship justify delivery.

### 5.9 Checklists, permissions, publication, audit

Add checklist template/version records with applicability, required items,
relative due offsets, dependencies, default assignee role, and evidence rules.
Creating/confirming a booking instantiates an immutable template snapshot.

Replace `building.manage` gradually with:

- `building.view`
- `building.inventory.manage`
- `building.content.manage`
- `building.marketplace.manage`
- `building.crm.manage`
- `building.tours.manage`
- `building.bookings.manage`
- `building.calendar.manage`
- `building.documents.manage`
- `building.operations.manage`
- `building.billing.view`
- `building.billing.manage`
- `building.billing.refund`
- `building.publish`
- `building.reports.view`
- `building.audit.view`
- `building.settings.manage`

For one release, `building.manage` grants all non-refund Building capabilities;
refund still also requires the existing Finance authorization. Route guards,
nav visibility, server-rendered actions, and internal APIs use the same catalog.
Hiding a button is never the authorization control.

Add:

- `building_publication_drafts`: base revision, author, status;
- `building_publication_items`: target resource/revision and action;
- `building_publication_revisions`: immutable complete public snapshot,
  checksum, approval/publish/rollback metadata;
- `building_publication_deliveries`: website target, attempt, acknowledgment,
  error, retry state.

Publication flow:

```text
Draft -> Validate -> Preview/diff -> Approve -> Publish -> Acknowledged
                    \-> Needs changes
Published -> Superseded
Published -> Emergency unpublish or rollback
```

Validation covers referential integrity, pricing language, media/alt/license,
testimonial/logo consent, availability freshness, conversion target, URL
safety, and public allow-list schema. Approval freezes the exact snapshot.
Publish uses revision/checksum idempotency. The website acknowledges the
revision it serves. Failure leaves the previous revision live and opens an
operator action. Emergency unpublish requires reason and removes selected
content without falsely changing commercial availability.

Expand `BuildingAuditEvent` through one service used by all Building writes.
Store resource type/ID, action, actor, request correlation ID, source,
before/after field diff, reason, external provider IDs, and timestamp. Secrets,
tokens, document bodies, and unnecessary personal data are redacted. Audit
records are append-only and exportable by authorized users.

## 6. API Contract

### 6.1 Preserve existing APIs

Keep all current public and internal Building endpoints compatible, including:

- public offerings/detail/availability/inquiries/unsubscribe;
- internal spaces, media, offerings, rate plans, availability, analytics and
  inquiry lifecycle/retry;
- CRM contacts, relationships, preferences, segments, campaigns, roster,
  merge, privacy and scheduled-send actions;
- bookings, transitions, tours, proposals, agreements and deposit evidence;
- billing accounts, schedules, invoices, QBO handoff, collections and Stripe
  webhook;
- calendar projections/sync, checklists, adjustments and service requests.

New response fields are additive. Meaning changes require `/v2`, not silent
reinterpretation.

### 6.2 New internal resource families

Use JSON endpoints under `/api/internal/building`:

- `/inventory/search`, `/spaces/{id}/layouts`, `/closures`;
- `/assets`, `/placements`, `/tenant-profiles`, `/testimonials`;
- `/marketplace/listings`, `/marketplace/listings/{id}/verify`;
- `/routing-rules`, `/routing-rules/preview`, `/routing-decisions`;
- `/tour-types`, `/tour-slots`, `/staff-resources`;
- `/event-cost-profiles`, `/add-ons`, `/discount-policies`,
  `/quotes/{reservation_id}/calculate`;
- `/document-templates`, `/documents`, `/signature-envelopes`,
  `/signature-envelopes/{id}/resend|void`;
- `/checklist-templates`;
- `/publication/drafts`, `/publication/drafts/{id}/validate|approve|publish`,
  `/publication/revisions/{id}/rollback`;
- `/reports/*` and `/audit-events`.

Mutable endpoints accept `Idempotency-Key` for create/external action and
`If-Match` or body `version` for updates. Conflicts return `409`; validation
returns `422` with field errors; missing capability returns `403`; unavailable
integration returns `503` with a retryable flag and correlation ID.

### 6.3 Public API additions

Add a versioned, cacheable snapshot contract:

- `GET /api/public/building/v2/catalog`
- `GET /api/public/building/v2/availability?offering_id=&start=&end=&party_size=`
- `GET /api/public/building/v2/publication`
- `POST /api/public/building/tours/slots` through the trusted website server;
- tokenized customer actions under `/public/building/action/{token}`.

Every read includes `publication_revision`, `updated_at`, and `freshness_state`.
The catalog is an explicit allow-list. When availability is stale or the
projection fails, return `freshness_state: "stale"` and safe inquiry copy, not
an open slot.

### 6.4 Webhooks and jobs

Add `/api/integrations/signature/webhook` with provider-specific signature
verification. Preserve Stripe and Resend webhook idempotency.

Scheduled jobs:

- expire holds and public action tokens;
- remind on leads, tours, quotes, signatures, payments and checklist work;
- project calendar changes and retry dead letters;
- verify Marketplace links without scraping protected content;
- flag expiring assets/consents/listings/templates;
- execute scheduled campaigns;
- detect publication/website revision drift;
- generate daily control-room exceptions.

Every job uses a distributed lease or equivalent single-run guard and records a
run summary plus item-level failures.

## 7. Core Workflows and States

### 7.1 Catalog-to-publication

1. Content operator edits a draft resource.
2. System shows current public revision, draft changes, missing evidence, and
   affected pages.
3. Operator creates a publication draft.
4. Validation produces blockers and warnings.
5. Authorized publisher reviews a rendered public preview and structured diff.
6. Approval freezes the bundle.
7. Publish sends the immutable revision; website acknowledges it.
8. Control room reports served revision or drift.

No resource becomes public merely because `is_public` or `is_published` was
checked. Those legacy flags are migrated into draft readiness; only a published
bundle controls v2 public output.

### 7.2 Workspace journey

```text
Inquiry -> Routed -> Contacted -> Tour scheduled -> Tour outcome
-> Quote -> Optional hold -> Agreement sent -> Signed
-> Invoice/deposit -> Confirmed occupancy -> Move-in -> Active -> Closeout
```

Tour does not block inventory. Hold expiration releases availability. Signed
agreement does not equal paid. Occupied requires the configured agreement and
payment gates or a privileged audited override.

### 7.3 Event journey

```text
Inquiry -> Requirements complete -> Availability checked
-> Costed quote -> Approved hold -> Quote accepted
-> Agreement signed -> Deposit accepted -> Confirmed
-> Checklist ready -> Event -> Final invoice/refund -> Closed
```

Setup/teardown and all linked spaces participate in conflict checking. The
state header separately shows quote, hold, signature, payment, calendar, and
readiness; a green status in one does not imply the others.

### 7.4 Exceptional states

Every workspace implements loading, true empty, filtered empty, stale, partial
sync, integration unavailable, permission denied, validation error, concurrent
edit, external-write pending, retryable failure, terminal failure, and archived
record states. Public experiences have branded recovery copy and a safe inquiry
fallback. Admin errors retain entered values where safe and focus an error
summary.

## 8. Reporting

Preserve existing funnel/source analytics and add:

- live/soon/unavailable inventory and vacancy days;
- room/space utilization by sellable hours/days, excluding closures;
- requested vs held vs booked demand;
- tour slot availability, show/no-show and tour-to-quote conversion;
- Marketplace listing freshness and inquiry/conversion/revenue by listing;
- quote value, discount, cost, margin, acceptance and expiry;
- signature and payment cycle time;
- event readiness completion and exceptions;
- invoiced, provider-confirmed paid, refunded, QBO-linked, and posted evidence
  as separate measures;
- publication freshness, validation failures, rollback and drift;
- communication delivery/failure, with marketing engagement only where
  consent/provider telemetry permits.

Reports state timezone, filters, calculation definition, source freshness and
export timestamp. Money defaults to collected/provider-confirmed or posted
labels; forecast and expected value never masquerade as cash.

## 9. Phased Delivery

### Phase 0: Contract and migration safety

- Add migration runner/baseline and schema health.
- Add optimistic versions and shared Building audit writer.
- Split Building navigation/routes without removing the legacy page.
- Add granular permissions with `building.manage` compatibility.
- Capture golden public API payloads and current admin workflow tests.

Exit: production can deploy additive migrations safely; existing Building tests
and public contracts pass unchanged; permissions deny every new write by
default except legacy compatibility grants.

### Phase 1: Public catalog control

- Normalize media and migrate existing placements.
- Add lifestyle assets, tenant profiles/logos and testimonials.
- Add room/offering applicability and structured layouts.
- Add Marketplace listing/link management.
- Add publication draft, validation, preview, approval, acknowledgment,
  emergency unpublish and rollback.
- Integrate public site with v2 revisioned catalog behind a feature flag.

Exit: staff can prepare and publish a reviewed catalog revision without a code
deploy; unapproved/expired content cannot leak; the prior revision remains live
on failure.

### Phase 2: Demand, tours, pricing and calendar

- Add routing rules/decisions and escalation.
- Add tour types, resources, slots, secure changes and reminders.
- Add closures, buffers, multi-space availability and conflict-safe writes.
- Add event costs, add-ons, discounts and calculated quote snapshots.
- Extend calendar projection health and replay.

Exit: every inquiry has explainable ownership/SLA; a tour can be scheduled and
changed safely; an event quote is reproducible from approved inputs; concurrent
holds cannot double-book inventory.

### Phase 3: Documents, money and fulfillment

- Add templates, clauses, rendering, documents and signature adapter/webhooks.
- Add secure quote/agreement/payment status pages.
- Wire approved refund execution and payment/refund communications.
- Add checklist templates, automatic instantiation, relative due work and
  escalations.
- Add amendments/cancellation/closeout workflows.

Exit: one test workspace journey and one test event journey can proceed from
inquiry through signed evidence, provider-confirmed payment, calendar
projection, completed checklist and closeout with a complete audit trail.

### Phase 4: Reporting and hardening

- Complete utilization, listing, quote/margin, signature, payment, operations
  and publication reporting.
- Add audit export, retention checks, job dashboards and dead-letter tools.
- Run permission matrix, recovery, accessibility, performance, penetration and
  production-observability validation.
- Retire legacy media fallback and legacy single-page write forms after usage
  and parity evidence.

Exit: role-scoped operators can perform their work without broad admin access;
leadership metrics reconcile to source records; rollback and integration
recovery drills pass.

## 10. Acceptance Criteria

### Inventory, pricing, and availability

- An authorized operator can create, edit, archive and filter spaces,
  offerings, layouts, rate plans, add-ons and closures.
- An approved rate-plan version and any sent quote snapshot cannot be edited.
- Public pricing resolves to the correct effective approved plan and never
  exposes internal cost or margin.
- Setup, teardown, turnover, closure, hold, booking and occupancy blocks are
  included in conflict checks.
- Two simultaneous writes for an overlapping space/time cannot both succeed.
- Availability stale beyond configured tolerance returns safe fallback state.

### Content and publication

- Room media, lifestyle assets, logos and testimonials have explicit approved
  placements and evidence requirements.
- Missing alt text, license/consent, source URL, pricing wording or conversion
  target blocks publication.
- Admin preview renders the exact immutable public payload to be published.
- Publishing is idempotent and does not expose draft fields.
- Website acknowledgment records the served revision; mismatch is visible.
- Failed publish leaves the prior revision live; authorized rollback restores a
  prior complete revision and records reason/actor.

### Marketplace, leads, CRM, and audiences

- Every listing has owner, canonical link, status, linked offering, reviewed
  copy/media snapshot and refresh date.
- Marketplace inquiries preserve listing ID/link through HubSpot and Agent.
- Lead assignment records the evaluated rule version and a human-readable
  explanation.
- No-match and owner-unavailable cases assign the configured fallback and flag
  the exception.
- Audience preview explains inclusion/exclusion and rechecks consent and
  suppression at send time.
- Operational and marketing communications remain separately classified.

### Tours, quotes, contracts, and bookings

- Tour slot search cannot offer outside policy or staff/resource availability.
- Tour create/reschedule/cancel never creates a commercial availability block.
- Quote totals reproduce from immutable rate, add-on, cost, discount and tax
  snapshots.
- Out-of-policy discount or margin blocks send pending authorized approval.
- Only approved template versions create sendable documents.
- Signature completion requires authenticated, idempotent provider evidence;
  manual evidence is permissioned and visibly labeled.
- Booking state displays hold, quote, agreement, payment, calendar and
  readiness independently.
- Cancellation/amendment releases or changes inventory atomically and retains
  prior evidence.

### Billing, refunds, communication, and operations

- Stripe invoice/refund writes require preview, typed confirmation,
  authorization and idempotency.
- Provider-confirmed payment/refund, QBO linkage and posted bank evidence remain
  distinct.
- Refund amount cannot exceed remaining refundable provider-confirmed payment.
- Every transactional send stores recipient, template version, trigger/actor,
  provider ID and delivery state.
- Checklist instances retain the template version used and calculate due times
  from the booking milestone.
- Required incomplete/waived items visibly block readiness according to policy;
  waiver requires permission and reason.

### Permissions, audit, reporting, and UX

- Each new route and action has server-side capability tests for allow and deny.
- A content operator cannot alter bookings or billing; a finance operator
  cannot publish catalog content without the corresponding grant.
- Every consequential write emits one redacted audit event with before/after,
  actor, reason/source and correlation ID.
- Reports reconcile sampled totals to source records and label evidence class,
  timezone, freshness and filters.
- All admin pages are keyboard usable, have one `h1`, visible focus, associated
  labels, error summary/focus management, non-color status text, and no critical
  action available only on hover.
- At 1280px and 1440px there is no page-level accidental horizontal scroll; at
  768px critical work remains usable.

## 11. Validation Plan

### Automated

- Migration tests from a copy of the current schema and from an empty database,
  on SQLite and PostgreSQL.
- ORM constraints, unique provider IDs, optimistic concurrency, overlapping
  interval and multi-space transaction tests.
- Golden compatibility tests for current public endpoints and v2 allow-list
  snapshot tests proving private fields cannot serialize.
- Unit/property tests for pricing, taxes/fees, discounts, margins, routing,
  availability, buffers, token expiry and checklist due calculations.
- Route tests for all role/action combinations and CSRF/internal-key behavior.
- Webhook authenticity, duplicate, out-of-order, retry and terminal-failure
  tests for Stripe, Resend and signature provider.
- Idempotency tests for inquiry, publish, invoice, refund, envelope, calendar
  and communication writes.
- Publication validation, acknowledgment, failure, rollback and emergency
  unpublish tests.
- End-to-end workspace and event journey tests with provider fakes.

### Manual/staging

1. Import a sanitized production-shaped database and run every migration twice.
2. Reconcile counts and hashes for existing spaces, media, offerings, holds,
   proposals, agreements, campaigns, invoices, payments and audits.
3. Build and preview a full public revision containing each asset/content type.
4. Verify the live staging site serves the acknowledged revision and falls back
   safely when Agent is unavailable/stale.
5. Create overlapping booking attempts in separate sessions.
6. Complete one tour, workspace and event journey with failure/retry branches.
7. Exercise signed document, invoice, partial payment and refund in provider
   sandboxes.
8. Run role-based task scripts for content, sales, event, finance, reporting and
   read-only users.
9. Perform keyboard, screen-reader landmarks/errors, contrast, zoom, reduced
   motion, 768/1280/1440 layout and print/document checks.
10. Run rollback, webhook replay, calendar replay, expired-token, listing-link
    failure and previous-publication recovery drills.

### Production rollout

- Feature flags: `BUILDING_V2_ADMIN`, `BUILDING_V2_PUBLIC_CATALOG`,
  `BUILDING_SIGNATURE_WRITES`, `BUILDING_STRIPE_REFUND_WRITES`.
- Start with staff-only read parity, then admin writes, then staging public
  revision, then production public read.
- Keep provider writes in preview/sandbox until credentials, webhook health and
  role checks pass.
- Monitor error rate, webhook lag/failures, calendar backlog, public revision
  drift, stale availability, lead SLA misses, double-book conflicts, email
  bounces and payment/refund failures.
- Roll back by disabling v2 public reads and serving the prior acknowledged
  publication; migrations remain additive.

## 12. Decisions Still Required

| Decision | Recommended default |
| --- | --- |
| Signature provider | Select one provider after validating template API, embedded/remote signing, webhook evidence, artifact retrieval, sandbox quality and cost. Keep the adapter provider-neutral. |
| Public instant booking | Keep request/approval mode. Enable instant booking only per offering after conflict, cancellation, payment and staffing policies pass. |
| Facebook integration | Assisted listing/link management only. Do not automate posting or scrape protected content without an official approved API and terms review. |
| Tax calculation | Configure explicit reviewed tax/fee rules initially; adopt Stripe Tax only after finance/legal confirmation of product/location treatment. |
| Quote margin visibility | Finance/admin and authorized event managers only; never public or recipient-facing. |
| Publication approval | Require a separate publisher for production revisions that change price, availability policy, legal copy, tenant logo/testimonial consent, or conversion routing. Allow same-person approval for low-risk copy/media reorder only if policy explicitly enables it. |
| E-signature manual fallback | Allow only `building.documents.manage` plus administrator override, with uploaded evidence, reason and conspicuous manual label. |
| Public availability precision | Show inquiry-safe day/range language for offices/events initially; do not expose tenant or hold detail or promise a slot until instant-book policy is approved. |
| Data retention | Retain commercial/audit/payment/signature evidence per legal/accounting policy; define a reviewed schedule before production document launch. Default to archive and privacy review, never automatic hard delete. |

## 13. Implementation Sequence Within Each Phase

For every slice:

1. add migration and entity constraints;
2. add deterministic domain service and audit emission;
3. add internal API with permission, concurrency and idempotency;
4. add provider adapter/fake where applicable;
5. add server-rendered admin subpage using canonical primitives;
6. add public projection only after allow-list tests;
7. add automated tests and operational health;
8. update `sales_support_agent/README.md` and operator runbook;
9. deploy behind the phase flag and verify production evidence.

This sequence keeps the existing Building OS operational while each missing
capability becomes independently reviewable and reversible.
