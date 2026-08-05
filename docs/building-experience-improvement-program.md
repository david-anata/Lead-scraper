# Building Experience Improvement Program

Status: Approved for phased implementation  
Prepared: 2026-08-04  
Scope: Public Arena discovery through post-event operations

## Outcome

Make the complete Arena journey feel like one continuous customer record:

`website -> inquiry -> staff response -> interview -> date review -> hold -> quote -> agreement -> QuickBooks -> payment -> confirmation -> calendar -> operations -> closeout`

Agent remains the audited booking system. The dedicated Anata Events Google
Calendar is the live occupancy source consulted by availability checks and the
projection target for confirmed Agent records. QuickBooks remains the formal
customer, contract-handoff, invoice, and payment-of-record system. No phase may
claim a hold, delivery, signature, payment, calendar write, or confirmation
without provider evidence.

## Program rules

- Ship one phase at a time with focused tests, the full Building suite,
  predeploy validation, production exact-commit verification, and desktop plus
  phone visual QA.
- Preserve every existing security, permission, checksum, idempotency, and
  audit gate.
- Reuse one customer/inquiry/reservation identity instead of copying data into
  disconnected forms.
- Keep test and production-QA records visible through an explicit filter, but
  out of the default staff queue.
- Prefer normal detail pages for review work. Tables prioritize and route work;
  they do not become miniature applications.
- Provider writes remain preview/review/approve operations with visible
  receipts and recovery states.

## Phase 1 — Lead workspace and queue clarity

### Problem

Complete website submissions are hidden inside a disclosure in a long table.
Today routes every inquiry to the general Sales page, test data competes with
real prospects, and staff cannot search or filter the queue.

### Scope

- Add `/admin/building/inquiries/{inquiry_id}` as the canonical lead workspace.
- Put submitted contact, candidate dates, times, event details, notes,
  notification evidence, follow-up plan, attribution, activity, and one next
  action on that page.
- Route Today, Sales, and Slack lead links to the canonical workspace.
- Add Sales search, lifecycle filters, urgency sorting, and an explicit test
  data view.
- Exclude recognized QA/test inquiries from the default staff queue without
  deleting them.

### States

Ready, overdue, responded, qualified, closed, test record, missing optional
details, notification failed, permission denied, and not found.

### Acceptance

1. Jordan's complete original submission is visible without expanding a table
   row.
2. A Today action opens Jordan, not the top of Sales.
3. Search matches name, email, phone, and date.
4. The default queue excludes deterministic QA/test records and shows a count
   explaining the exclusion.
5. Filters are URL-addressable and work without JavaScript.
6. Existing status updates retain CSRF, RBAC, and audit behavior.
7. Desktop and phone layouts keep the next action and contact method visible.

### Validates improvements

41-60 and the queue-routing parts of 33 and 37.

## Phase 2 — Public inquiry clarity and validation

### Scope

- Compress the phone hero and keep the form action visible sooner.
- Add company, flexibility, tour interest, setup/teardown, AV, accessibility,
  food/vendor, and conditional alcohol fields.
- Use plain labels throughout; preserve three candidate dates distinctly.
- Add phone normalization, date/time ordering, past-date, and capacity checks.
- Show a review summary and a public submission reference.
- Preserve safe availability language and progressive enhancement.

### Acceptance

1. The first form field is visible in the initial phone journey without an
   excessive decorative gap.
2. Invalid date/time and capacity combinations fail inline and server-side.
3. The confirmation repeats what was submitted and does not claim a hold.
4. Every new field arrives in Agent under the same plain-language label.
5. Existing attribution, consent separation, receipt, and idempotency remain.

### Validates improvements

1-30, excluding the full calendar delivered in Phase 5.

## Phase 3 — Notification and response automation

### Scope

- Put canonical lead links in Slack notifications.
- Expose customer receipt and Slack accepted/failed evidence on the lead.
- Add permissioned retries with idempotency and audit receipts.
- Schedule overdue escalation and a Building lead digest.
- Keep internal follow-up tasks separate from customer communication.

### Acceptance

1. One inquiry produces at most one initial Slack alert and one receipt per
   content version.
2. Failures never reject intake and always expose a retry action.
3. Escalations stop after a meaningful staff response or terminal lifecycle.
4. Staff can see sent, delivered, bounced, failed, and unknown distinctly.

### Validates improvements

31-40.

## Phase 4 — Structured event interview

### Scope

- Replace the flat 28-question disclosure with a sectioned interview workspace.
- Prefill answers from the public submission.
- Show unanswered and staff-review-needed questions first.
- Apply deterministic event-type conditions.
- Autosave through auditable progressive-enhancement endpoints.
- Provide a concise call guide and a one-action qualification handoff.

### Acceptance

1. No submitted answer is requested twice.
2. Progress is shown by section and overall.
3. Refreshing never loses a saved answer.
4. Qualification identifies the exact missing requirement when blocked.
5. Conversion to date review carries contact, dates, times, attendance, and
   requirements.

### Validates improvements

61-70.

## Phase 5 — Availability calendar and hold lifecycle

### Scope

- Add public and internal calendar views backed by the dedicated Anata Events
  calendar plus Agent reservation/hold evidence.
- Compare up to three candidate dates and suggest nearby alternatives.
- Check the full setup-through-teardown window.
- Show freshness, checked-by evidence, conflicts, and recovery.
- Create holds from a qualified lead with approved defaults and visible expiry.
- Notify staff before expiry and reconcile releases automatically.

### Acceptance

1. Public results reveal no private event information.
2. Agent and Google conflicts fail closed for holds.
3. Calendar/provider failure shows unknown, never available.
4. Expired/cancelled holds stop blocking and queue projection cleanup.
5. Cancelled bookings are outside the default active list.

### Validates improvements

1-5 and 71-90.

## Phase 6 — Booking workspace and quote builder

### Scope

- Move quote creation and lifecycle actions into the guided booking workspace.
- Carry approved rates, access hours, add-ons, discounts, tax determination,
  deposit, and totals without re-entry.
- Add customer preview and immutable version comparison.
- Keep the index focused on prioritization, not inline editing.

### Acceptance

1. Totals are derived from approved source evidence.
2. Discounts require a reason and never exceed the pre-tax subtotal.
3. The applicable Lehi rate and transaction date are visible.
4. Sent versions are immutable and revisions create a new version.
5. Customer preview excludes internal notes.

### Validates improvements

91-96.

## Phase 7 — Contract and QuickBooks signature handoff

### Scope

- Generate the customer-ready agreement preview from the approved template and
  frozen quote.
- Show template/package differences before approval.
- Create a reviewed QuickBooks Contract Builder handoff with a deep link or
  precise copyable payload supported by the available QuickBooks capability.
- Synchronize or record delivery/signature evidence with explicit provider
  references and recovery states.

### Acceptance

1. No package can use an unapproved template or stale checksum.
2. Approval alone never claims delivery or signature.
3. Provider identity, document reference, signer, timestamp, and evidence are
   retained before a signed state is accepted.
4. A failed handoff is retryable without duplicate customer delivery.

### Validates improvements

96-98 for contracts and signatures.

## Phase 8 — QuickBooks billing and payment lifecycle

### Scope

- Create or match the QuickBooks customer from the linked Agent contact.
- Prepare an unsent invoice from the accepted quote and approved schedule.
- Synchronize invoice, payment, balance, and accounting references.
- Keep the refundable security deposit distinct from taxable charges.
- Add due reminders, collection recovery, refunds, credits, and write-off
  evidence under existing finance permissions.

### Acceptance

1. Customer and invoice idempotency prevents duplicates.
2. Invoice amounts match the accepted quote checksum.
3. Paid means provider-confirmed payment evidence, not staff intent.
4. Refundable deposits are not taxed unless retained/applied to taxable
   charges.
5. Adjustments preserve two-person approval and accounting evidence.

### Validates improvements

97-99 for billing and payment.

## Phase 9 — Customer status and lifecycle communication

### Scope

- Automatically prepare/send approved transactional messages at inquiry,
  quote, agreement, invoice, payment, confirmation, reminder, change, and
  post-event milestones.
- Deliver a signed customer status link after the approved trigger.
- Show a customer document/status center and safe reschedule/cancellation
  request paths.
- Create the customer calendar invitation only after confirmation evidence.

### Acceptance

1. Each message has an immutable template version and delivery receipt.
2. Messages use “The Anata Team,” never a personal signature by default.
3. Marketing consent does not affect required operational messages.
4. No message claims held, signed, paid, or confirmed without evidence.
5. Reschedule/cancellation requests do not directly change authoritative state.

### Validates improvements

99 and customer-communication portions of 31-40.

## Phase 10 — Operations, reporting, cleanup, and monitoring

### Scope

- Generate event-type checklists, deadlines, owners, vendor work, run sheets,
  incident/damage review, and closeout from confirmed bookings.
- Reconcile stale runbooks and source-of-truth wording.
- Add explicit production/test-data management and duplicate cleanup tools.
- Add funnel, SLA, utilization, revenue, and exception reporting.
- Add a recurring synthetic journey that leaves no live hold, provider object,
  calendar event, invoice, message, or customer artifact behind.

### Acceptance

1. Required checklist work has an owner, due date, completion/waiver evidence,
   and escalation.
2. Test data is labeled, filterable, and safely removable under audit.
3. Documentation matches production provider ownership.
4. The synthetic test verifies intake through cleanup and alerts on the first
   broken handoff.
5. Every synthetic artifact is deterministic and cleanup is verified.

### Validates improvements

100 plus cross-cutting operational, reporting, and governance findings.

## Rollout and rollback

Each phase uses additive schema changes where possible. New surfaces launch
behind existing Building permissions. Redirects preserve old bookmarks.
Rollback disables the new route or UI while retaining additive data and audit
history. Provider-writing phases require controlled production verification
with test identities and immediate cleanup before general staff use.

