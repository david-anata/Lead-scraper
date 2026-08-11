# Building Streamlined End-to-End Implementation

Status: Approved direction; ready for phased implementation  
Prepared: 2026-08-10  
Scope: Existing Arena lead page through event closeout

## Outcome

Finish the existing Building workflow so an authorized staff member can move one
Arena prospect from submitted inquiry through closeout without searching for the
same customer on another Agent page.

The canonical staff record remains:

`/admin/building/inquiries/{inquiry_id}`

This is a consolidation of the system already in production. It is not a new
event module, a new customer model, a parallel booking record, or a replacement
for QuickBooks, Google Docs, Google Calendar, or the existing governed Agent
services.

The target journey is:

`inquiry -> response -> interview -> customer -> date -> hold -> pricing -> contract -> signature -> invoice -> payment -> confirmation -> calendar -> operations -> closeout`

One page shows one current state, one next action, and the evidence already
recorded for that customer. Sections open as they become relevant and completed
sections collapse to summaries. Staff leave Agent only for a provider action
that cannot truthfully be performed through Agent, such as using Google Docs
eSignature or reviewing the authoritative QuickBooks invoice.

## Why this work exists

Current `origin/main` already puts the original submission, response,
interview, availability calendar, customer-specific pricing, date hold, and
contract creation on the lead page. The remaining journey is split among the
contract detail page, booking detail page, broad Building billing and operations
pages, Google Docs, and QuickBooks.

That split causes four avoidable problems:

1. Staff repeatedly leave the customer record and find the same customer again.
2. Lead pricing, booking quote state, contract state, and invoice state appear
   to be separate workflows even though they represent one event.
3. The original submission and interview are no longer visible while reviewing
   the contract, payment, or event-day work.
4. Multiple pages independently calculate or describe the next action.

## Product decisions

1. The inquiry page is the canonical staff journey page for every event that
   originated from an inquiry, even after a reservation and agreement exist.
2. Do not introduce `/events/{event_id}` or another parallel identity.
3. The existing inquiry, contact, reservation, proposal, agreement, billing,
   payment, calendar projection, communication, and checklist records remain
   authoritative for their respective facts.
4. The page joins those records; it does not copy their data into a new table.
5. All existing mutation services, RBAC, CSRF, audit, checksum, idempotency,
   conflict, and provider-evidence gates remain in force.
6. There is exactly one primary next action. Other available actions live in
   the applicable section and do not compete with it.
7. Separate index pages remain queues and administration. They route staff to
   the customer page instead of becoming competing customer workspaces.
8. Contract and booking detail URLs remain supported as fallback and audit
   views. Routine actions route back to the canonical inquiry page.
9. Google Docs is the agreement signing workspace. QuickBooks is the invoice
   and payment system of record. Remove active operator copy that describes
   QuickBooks Contract Builder as the chosen signing path.
10. The dedicated Anata Events Google Calendar remains the occupancy source
    consulted before a hold or confirmation and the projection destination for
    confirmed events.

## Non-goals

- No new CRM, HubSpot workflow, project-management system, or TidyCal dependency.
- No new customer portal beyond the signed status page already implemented.
- No new pricing engine, calendar engine, agreement engine, accounting ledger,
  messaging engine, or operations engine.
- No automatic provider claim without provider evidence.
- No silent contract send, invoice send, payment claim, calendar write, hold,
  confirmation, cancellation, refund, or closeout.
- No attempt to put global catalog, templates, standard rates, provider
  credentials, reporting, or cross-event queues inside a customer record.
- No decorative dashboard or duplicated stage tracker.

## Canonical page structure

The existing inquiry page keeps its current header and becomes progressively
complete. Render sections in this order:

1. **Next action** — one plain-language action and the exact reason it is next.
2. **Customer and event summary** — contact, company, requested outcome,
   attendance, chosen date/window, owner, and current authoritative state.
3. **Original submission** — every submitted answer under its public label.
4. **Response and interview** — current response evidence and structured
   interview; answered questions stay prefilled.
5. **Date and hold** — Anata Events calendar, selected access window, conflicts,
   freshness, hold expiry, and override evidence when applicable.
6. **Pricing and quote** — approved baseline, event-specific adjustments,
   add-ons, discount reason, Lehi tax snapshot, refundable deposit, total,
   version, and customer preview.
7. **Agreement and signature** — frozen terms, template/package comparison,
   approval, Google signing Doc, signer, delivery/signature evidence, and retry.
8. **QuickBooks invoice and payment** — customer match, prepared charges,
   invoice state/link, amount due, amount paid, and provider refresh evidence.
9. **Confirmation and calendar** — final gate results, booking state, Anata
   Events projection, customer invitation, and any recovery action.
10. **Customer communications** — milestone, version, state, sent/delivered
    evidence, failure, and retry.
11. **Event operations and closeout** — checklist owner, due date, completion or
    waiver evidence, vendor/access work, incident/deposit review, and closeout.
12. **Activity and technical evidence** — immutable audit history and record
    identifiers in a collapsed disclosure.

Only the current section opens by default. The summary and next action remain
visible near the top on desktop and phone. Anchor links may move within the
page; they must not masquerade as separate workflows.

## Single next-action resolver

Replace independent lead, contract, and booking next-action calculations with
one read-only resolver built from authoritative evidence. It returns:

- `stage`
- `title`
- `reason`
- `section_anchor`
- `action_label`
- `blocked_by`
- `evidence_state`

The order is:

1. Record meaningful response.
2. Complete required interview answers.
3. Link or create the responsible customer.
4. Select and conflict-check the complete access window.
5. Confirm the hold, including explicit authorized conflict override if used.
6. Finish and save event-specific pricing.
7. Create and approve the agreement package.
8. Create/open the Google signing Doc.
9. Record provider-backed signature evidence.
10. Prepare and approve exact billing drafts.
11. Create or match the QuickBooks customer and invoice.
12. Refresh and verify required payment evidence.
13. Run the final confirmation gate.
14. Verify the Anata Events calendar projection and customer invitation.
15. Complete event operations.
16. Complete closeout and refundable-deposit disposition.

Cancelled, expired, declined, lost, completed, permission-denied, provider
failure, and evidence-conflict states override the normal sequence with an
honest recovery action.

## Phase 1 — Consolidate the read model and navigation

### Work

- Extend the inquiry workspace loader to fetch its linked contact,
  reservation, quote versions, agreement, signature readiness, billing account,
  schedules, invoices, payment evidence, calendar projection, transactional
  messages, operational checklist, service requests, and audit events.
- Reuse existing query/service functions where available; do not reproduce
  business rules in the renderer.
- Render concise states for later sections before they become actionable.
- Add the shared next-action resolver.
- Change Today, Sales, Bookings, Contracts, Billing exceptions, Calendar
  exceptions, and Operations links to open the canonical inquiry page when an
  inquiry relationship exists.
- Preserve existing detail/index URLs for records without an inquiry and for
  advanced audit access.

### Acceptance

1. An event with linked records renders the complete journey without copying
   provider or financial data into the inquiry payload.
2. Jordan's original form and interview remain visible after a reservation,
   contract, invoice, and checklist exist.
3. Every queue opens the same customer record when an inquiry is linked.
4. Exactly one primary next action is rendered.
5. Missing optional records show a truthful not-started state, not an exception.
6. A missing required relationship names the exact recovery action.

## Phase 2 — Keep contract and signature work in context

### Work

- Keep “Create the contract” on the pricing section, but redirect back to
  `#agreement` after successful creation.
- Embed the verified frozen-terms summary, template/package comparison, contract
  status, payment-request readiness, and audit receipt.
- Reuse the existing `ready-to-send` orchestration to approve the agreement and
  payment request and create the Google Doc.
- Show “Open signing Doc” only after the provider document URL exists.
- Add the existing evidence-recording and failure-recovery actions to the
  agreement section.
- Keep the full contract detail page available through an “Advanced contract
  record” disclosure/link, not as the normal next step.
- Remove current operator wording and controls that imply QuickBooks Contract
  Builder is the selected signing provider.

### Acceptance

1. Creating a contract returns the operator to the same customer and agreement
   section.
2. The operator can prepare and approve the package and create the Google Doc
   without visiting the contract index or detail page.
3. Nothing is described as sent or signed until matching provider evidence is
   recorded.
4. Stale checksum, inactive hold, unapproved template, permission, and provider
   failures remain fail-closed and explain recovery in place.
5. Repeating an action cannot create a duplicate agreement or signing Doc.

## Phase 3 — Finish QuickBooks, confirmation, and calendar in context

### Work

- Embed exact billing preparation using the accepted quote and signed agreement.
- Show the linked QuickBooks customer or the existing match/create action.
- Add existing schedule approval, invoice creation, and payment-refresh actions
  to the customer's billing section.
- Preserve distinct booking charge, balance, and refundable security-deposit
  lines. Derive tax at the transaction date using the approved Lehi rule;
  refundable deposits remain nontaxable unless retained or applied.
- Show QuickBooks IDs, links, timestamps, amounts, balance, and error/retry
  evidence after every provider action.
- Embed the existing final confirmation transition with a preview of every gate:
  active inventory state, current conflict check, signed agreement, cleared
  required payment, and operator authority.
- Embed the existing calendar projection preview/sync/retry for this reservation
  only. Do not expose a page-wide “sync everything” action.
- Create or update the customer calendar invitation only after confirmation.

### Acceptance

1. Staff do not visit the broad Billing or Bookings pages to finish one event.
2. Invoice creation is idempotent and the invoice matches the frozen accepted
   quote checksum.
3. “Paid” requires provider-confirmed QuickBooks evidence.
4. Confirmation cannot proceed while any authoritative gate is missing or
   stale, and the page names the failed gate.
5. Calendar failure is shown as unknown/failed, never as confirmed availability.
6. A confirmed event has one verified Anata Events projection and no duplicate
   customer invitation.

## Phase 4 — Finish communications, operations, and closeout in context

### Work

- Keep the current versioned communication table and retries on the customer
  page.
- Make checklist items editable in place using the existing checklist endpoints:
  owner, due date, state, completion/waiver reason, and evidence reference.
- Show linked service requests and allow event-support requests to be added from
  the event section without opening the global Operations page.
- Put run sheet, vendor/access work, incident/damage review, refundable-deposit
  disposition, and closeout evidence on the same page.
- Add a final closeout action that requires all required checklist work or an
  explicit authorized waiver and preserves accounting evidence for any retained
  deposit.
- Keep cross-event Operations as a queue that links back to the customer page.

### Acceptance

1. Every required operation has an owner, due date, and completion or waiver
   evidence editable from the customer page.
2. Communication retries remain idempotent and cannot resend delivered content.
3. A retained deposit cannot be closed out without taxable-charge/accounting
   evidence where applicable.
4. Completed events retain the original submission, pricing, agreement,
   invoice, payment, calendar, communication, and operational audit history.
5. Closed events become read-only except for explicitly permissioned recovery or
   correction actions.

## Route and redirect strategy

Do not create a new public or staff identifier.

- Canonical staff route: `/admin/building/inquiries/{inquiry_id}`.
- Successful inquiry-scoped writes return to that route and the relevant anchor.
- Existing reservation, agreement, billing, calendar, and checklist endpoints
  remain the mutation authorities.
- Add an allowlisted `return_to` value or small inquiry-scoped adapter only when
  an existing endpoint cannot safely return to the customer page.
- Never accept an arbitrary external redirect target.
- Record the same actor, action, before/after evidence, and idempotency key as
  the existing endpoint.
- Booking or contract records without an inquiry continue to use their current
  detail pages.

## Data and migration impact

The target requires no new master event table and should require no schema
migration for ordinary consolidation.

Use existing foreign keys and identifiers:

- `BuildingInquiry.id`
- `BuildingReservation.inquiry_id`
- `BuildingReservation.contact_id`
- `BuildingProposal.reservation_id`
- `BuildingAgreement.reservation_id`
- billing/payment `reservation_id`
- `BuildingCalendarProjection.reservation_id`
- `BuildingTransactionalMessage.reservation_id`
- `BuildingOperationalChecklist.reservation_id`

If a relationship is missing, expose and repair that relationship explicitly;
do not infer it from matching names or emails. Add a migration only if current
production evidence proves a required relationship cannot be represented.

## Permissions and safety

- Page access remains `building.manage`.
- Provider or consequential actions retain their current narrower permissions.
- Controls are hidden or disabled with a reason when the user lacks permission;
  endpoints still enforce permission independently.
- Every POST retains CSRF validation.
- Typed confirmations remain only where they protect a genuinely consequential
  or exceptional action; do not add confirmation friction to reversible drafts.
- Hold conflicts, checksum mismatches, stale provider evidence, and calendar
  uncertainty fail closed.
- Provider failures do not discard Agent state and always offer an idempotent
  recovery path.

## Responsive and accessibility behavior

- At 390px, the customer, current state, next action, and contact method appear
  before secondary evidence.
- The current section opens automatically; completed and future sections remain
  reachable as native disclosures.
- Forms work without JavaScript. JavaScript may add live totals, autosave, focus,
  and in-place continuity.
- After a successful action, focus moves to the resulting notice or updated
  section heading.
- Errors use an error summary plus field-level messages.
- Tables become contained horizontal regions or stacked records without causing
  body overflow.
- Status never depends on color alone.

## Testing and production validation

Each phase must include:

1. Focused service, route, permission, CSRF, idempotency, and redirect tests.
2. A full runnable Building test suite.
3. Predeploy validation.
4. Desktop QA at 1280px and 1440px.
5. Phone QA at 390px.
6. Keyboard and no-JavaScript workflow checks.
7. Exact-commit production health verification.
8. A controlled production journey using one clearly labeled QA inquiry.
9. Immediate cleanup of every temporary hold, calendar projection, Google Doc,
   QuickBooks object, message, and checklist artifact created by the test.

The controlled journey must prove, requirement by requirement:

- the public submission appears intact;
- the receipt and staff notification evidence are visible;
- interview answers persist;
- the date check reads Anata Events and fails closed on provider uncertainty;
- the hold and quote are created once;
- event pricing, discount, tax, deposit, and total remain identical through the
  agreement and invoice;
- the Google Doc is created once and signature is not claimed prematurely;
- QuickBooks customer/invoice/payment evidence is provider-backed;
- confirmation happens only after all gates pass;
- the Anata Events calendar contains the correct confirmed window;
- the customer communication and invitation have delivery evidence;
- operations and closeout remain attached to the same customer page;
- cleanup is verified.

## Rollout order

Ship the four phases sequentially. Do not wait for a broad redesign and do not
replace working subsystems.

1. Read model, navigation, and one next action.
2. Agreement and Google signing flow in context.
3. QuickBooks, confirmation, and Anata Events calendar in context.
4. Communications, operations, closeout, and final synthetic journey.

During rollout, the existing contract and booking pages remain available as
fallbacks. After production evidence shows the inquiry page can complete the
journey, remove redundant primary actions from those pages while preserving
read-only audit access and legacy-record support.

## Definition of done

This implementation is complete only when an authorized operator can take one
realistic inquiry from intake through audited closeout while remaining on its
canonical inquiry page, except for the required Google Docs and QuickBooks
provider interactions, and every status claim is supported by authoritative
evidence.

Passing tests alone is insufficient. Production exact-commit evidence, desktop
and phone QA, provider receipts, calendar verification, and synthetic cleanup
must all agree with the completed journey.
