# Arena launch — final external activation packet

This is the minimal remaining packet after reconciling the approved owner
decisions, Agent workflows, public website, canonical private Arena catalog,
commercial draft, agreement readiness, and provider-neutral delivery
workflows.

Building Control is authoritative for the current checklist. It reports seven
saved owner rules, seven outside approval or provider items, and zero internal
blockers. The canonical `arena` space and `arena-events` offering remain
private, unavailable, and unpublished. Public estimates remain non-binding.
Nothing in this packet authorizes Agent to publish an offering, claim a date is
available, send an agreement or email, accept payment, write a calendar, or
confirm a booking without the evidence and governed action required for that
step.

## 1. Accountant — Arena tax determination

Required external action:

1. Identify which Arena base charges, add-ons, fees, deposits, and other
   categories are taxable.
2. Provide the applicable jurisdiction, numeric rate, effective date, and any
   category-specific exceptions.
3. Provide the accountant or firm name and a durable evidence reference.

Required evidence:

- written accountant determination;
- taxable and non-taxable category mapping;
- numeric rate when the treatment is taxable;
- jurisdiction and effective date.

Record in Agent:

- **Building → Spaces & website → Arena pricing**, in the tax fields and
  approval evidence for the private Arena rate-plan draft.

Until this is recorded, quotes must remain tax-review-required and the rate plan
must not be approved.

## 2. TidyCal administrator — stale booking-page copy

Required external action:

1. Replace the stale 70% deposit with the owner-approved 50% booking deposit.
2. Replace the stale 48-hour balance deadline with the owner-approved
   seven-day deadline.
3. Remove or replace the placeholder payment link without presenting an
   unverified live payment path.

Required evidence for each conflict:

- the exact corrected TidyCal URL;
- a dated screenshot or provider change record;
- the administrator and completion date.

Record in Agent:

- **Building → Spaces & website → Arena pricing → Evidence and conflicts**;
- select `provider_remediated` only after the matching provider change has
  actually occurred, and attach or reference its evidence.

`reconciled_in_agent` does not clear these conflicts. Agent does not write
TidyCal.

## 3. Legal reviewer — reusable Arena agreement

Already completed by Agent:

- owner-approved commercial and operating rules are consolidated in
  `docs/building/agreements/arena-event-agreement-business-terms-v1.md`;
- the artifact is versioned and SHA-256 checksum-backed;
- supported merge fields are allow-listed;
- Building Control can register the exact artifact as `in_review`;
- signature-request readiness freezes the signer and agreement checksum without
  sending anything.

Required external action:

1. Designate legal counsel or an authorized legal reviewer.
2. Review and supply the remaining legal clauses identified in section 11 of
   the business-terms artifact.
3. Store the complete reusable agreement in a durable repository or provider.
4. Approve one exact version for recurring Arena use.

Required evidence:

- durable document reference;
- version and checksum;
- approver and approval date;
- written legal approval reference.

Record in Agent:

- **Building → Contracts → Templates**, on the
  `arena-event-agreement` template record.

The dated Vivint-specific 2025 agreement is evidence only and is not an
acceptable reusable template.

## 4. Owner and platform administrator — electronic signatures

Required external action:

1. Select the production e-sign provider and account.
2. Approve credential custody, callback verification, signed-document
   retention, and failure/retry policy.
3. Authorize implementation of the provider adapter after those choices are
   recorded.
4. Complete a controlled delivery, callback, and signed-document verification
   using non-customer test data.

Required evidence:

- provider and production account identifier;
- approved credential owner;
- verified callback event and signature;
- retention location and policy;
- controlled-test request, delivery, completion, and stored-document
  references.

Record in Agent:

- **Building → Contracts**, alongside the prepared signature request and its
  audit history;
- production configuration only after the provider decision is authorized.

Current signature readiness sends nothing and must continue to report
`not_sent` until the provider workflow is implemented and verified.

## 5. Finance and platform administrator — customer payments

Required external action:

1. Authorize the production payment account.
2. Issue least-privilege production credentials and configure the verified
   confirmation webhook.
3. Approve the provider event types and reconciliation owner.
4. Complete a controlled payment and refund or reversal verification without
   using a real customer booking.

Required evidence:

- production account identifier;
- restricted-key owner and scope;
- verified webhook endpoint, signature, and event IDs;
- controlled payment, confirmation, reconciliation, and refund/reversal
  references.

Record in Agent:

- **Building → Billing**, on the payment-provider readiness and controlled-test
  audit records.

Configured credentials are readiness evidence only. Cleared provider evidence
is still required before Agent marks a deposit paid or confirms a booking.

## 6. Google Workspace and platform administrator — dedicated Arena calendar

Current evidence:

- Agent has a provider-neutral, idempotent Google Calendar projection outbox;
- Agent remains the booking source of truth;
- live writes are disabled and dry-run is the default;
- no acceptable dedicated Arena calendar ID, owner, and service-account access
  have been verified.

Required external action:

1. Reauthenticate with the required calendar-list and event permissions.
2. Create or identify one Anata-owned calendar dedicated to Arena events.
3. Grant the Agent service account only the access required to manage events on
   that calendar.
4. Configure its exact non-`primary` calendar ID and service-account
   credentials in production.
5. Review and accept Agent's dry-run projection.
6. Deliberately authorize live writes and verify one controlled create/update
   projection.

Required evidence:

- non-`primary` calendar ID;
- Anata owner;
- service-account access confirmation;
- accepted dry-run payload;
- controlled create/update provider event IDs and audit records.

Record in Agent:

- **Building → Spaces & website → Calendar projection**, then record the
  `event_calendar = provider_verified` readiness evidence.

Do not use a person's primary calendar or infer readiness from the past
“Event space tour” event.

## 7. Google Workspace and platform administrator — customer email

Required external action:

1. Verify `building@anatainc.com` and the `anatainc.com` sending domain with the
   approved email provider.
2. Configure least-privilege production credentials and signed delivery
   webhooks.
3. Complete controlled delivered, bounced, and complaint or suppression tests
   before automated customer use.

Required evidence:

- verified sender and domain;
- provider/account identifier and credential owner;
- verified delivery-webhook signature;
- controlled delivered and bounced event IDs;
- suppression or complaint-handling evidence.

Record in Agent:

- **Building → Contacts/customer communications**, on sender readiness and
  delivery-feedback audit records.

The approved inbox choice is already saved. Sender configuration alone is not
proof of delivery.

## Activation order after the seven items

No additional owner questionnaire is required.

1. Resolve the accountant and TidyCal evidence, then review and approve the
   private Arena rate plan. Its approved effective date supplies the governed
   launch-effective-date record.
2. Approve and freeze the reusable agreement.
3. Verify e-sign, payment, dedicated-calendar, and customer-email provider
   workflows with controlled non-customer records.
4. Confirm Building Control reports no outside items, no internal blockers, one
   current approved Arena rate plan, and `customer_launch_ready`.
5. Run one controlled end-to-end rehearsal through inquiry, date review, hold,
   quote, agreement, signature, payment, confirmation, calendar projection,
   customer status, communications, and operations.
6. Publish the offering or enable live customer paths only through their
   separate governed approvals after the rehearsal passes.

## Completion evidence

Arena launch is ready only when Agent shows:

- accountant-reviewed tax treatment and rate evidence;
- no blocking TidyCal source conflicts;
- one current approved Arena rate plan and its derived effective-date record;
- `agreement_template = approved_reference`;
- verified e-sign request, callback, and signed-document retention;
- verified production payment credentials, webhook, and controlled
  reconciliation;
- `event_calendar = provider_verified` for a dedicated non-primary calendar,
  with live writes deliberately enabled;
- verified `building@anatainc.com` sender/domain, delivery webhook, and
  controlled delivery results;
- `customer_launch_ready` with zero external and zero internal blockers;
- a passed controlled end-to-end rehearsal;
- customer-facing confirmation still requiring authoritative agreement,
  cleared payment, conflict check, and booking confirmation evidence.
