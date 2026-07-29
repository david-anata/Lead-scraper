# Building agreement and payment readiness

This phase prepares immutable evidence for later e-sign and payment-provider
adapters. It creates no document, signature request, invoice, checkout session,
payment intent, charge, email, calendar event, or booking confirmation.

Agent remains the source of truth.

## Existing systems reused

- active `BuildingReservation` temporary holds and their expiry job;
- frozen, versioned `BuildingProposal` quote snapshots;
- the existing `BuildingAgreement` record for the eventual provider lifecycle;
- native billing schedules, invoices, Stripe webhooks, payments, and deposit
  evidence as downstream systems, unchanged by this phase;
- `BuildingEventLifecycleCommand` for idempotency evidence;
- `BuildingAuditEvent` and the existing customer-safe booking status.

The new payment-readiness row is an outbox before the existing invoice system.
It is not an invoice or payment record.

## Template registry

`PUT /api/internal/building/agreement-readiness/templates/{template_id}`

Creates or edits a draft version with an opaque reviewed-repository reference.
Only these merge fields are accepted:

`customer_name`, `customer_email`, `event_space`, `setup_starts_at`,
`guest_starts_at`, `guest_ends_at`, `teardown_ends_at`, `attendance`,
`quote_total`, `currency`, `deposit_amount`, `deposit_type`,
`cancellation_policy`, `tax_terms`, `included`, and `addons`.

`POST /api/internal/building/agreement-readiness/templates/{template_id}/transition`

Transitions are `draft → in_review → approved → retired`. Typed confirmations:

- `IN_REVIEW TEMPLATE {template_id}`
- `APPROVED TEMPLATE {template_id}`
- `RETIRED TEMPLATE {template_id}`

Approval requires evidence. Approved and retired versions are immutable.

## Package preparation

`POST /api/internal/building/agreement-readiness/packages`

Required headers are `X-Internal-Api-Key` and `Idempotency-Key`.

```json
{
  "reservation_id": "event-2026-001",
  "quote_id": "quote-version-id",
  "template_id": "event-agreement-v1",
  "agreement_version": 1,
  "payment_version": 1,
  "actor": "operator@example.com"
}
```

Preparation fails closed unless:

- the Agent event hold is active and unexpired;
- the quote is a frozen internal draft for that reservation;
- the template version is approved;
- the responsible contact and event space still exist;
- the frozen deposit terms produce a positive required amount;
- the requested agreement version is unused.

The transaction creates:

1. a `BuildingAgreement` draft with `preparation_status=prepared`;
2. an immutable package snapshot containing the approved template evidence,
   allow-listed merge data, complete event window, frozen quote and pricing,
   cancellation, deposit, and tax terms;
3. a SHA-256 checksum over that snapshot;
4. a separate provider-neutral payment-readiness row with its own checksum,
   required amount, currency, request type, and false provider/payment flags;
5. audit and idempotency evidence.

An identical retry returns the original records. Reusing the key with changed
input returns HTTP 409.

## Review and approval

Agreement:

`POST /api/internal/building/agreement-readiness/packages/{agreement_id}/transition`

- `REVIEW AGREEMENT {agreement_id}`
- `APPROVE AGREEMENT {agreement_id}`

Payment:

`POST /api/internal/building/agreement-readiness/payments/{payment_id}/transition`

- `REVIEW PAYMENT {payment_id}`
- `APPROVE PAYMENT {payment_id}`

Both use `prepared → in_review → approved`. Checksums are recomputed before
every transition. Payment approval additionally requires an approved agreement
package and resolved tax treatment. Approval means ready for a future adapter,
not sent or paid.

`GET /api/internal/building/agreement-readiness/reservations/{reservation_id}`
returns the latest package and payment readiness plus explicit false
sent/signed/paid/booked gates.

## Expiry and cancellation

Hold expiry or reservation cancellation changes unsent agreement/payment
preparation records to `expired` or `cancelled`, retains their snapshots and
checksums, and adds audit events. It makes no provider call.

## Admin UI and permissions

The operator surface is the contract workspace at `/admin/building/contracts`,
reachable from Building navigation. `/admin/building/agreement-readiness`
returns a permanent redirect there.

- `building.manage`: view the contract index and contract detail
- `building.agreements.prepare`: package preparation
- `building.agreements.approve`: package review and approval
- `building.payments.prepare`: payment-readiness review and approval

Template drafting and template transitions remain internal-API operations at
`/api/internal/building/agreement-readiness/templates/...` until the template
editor ships. The workspace intentionally provides no document generation,
download, send, signature, invoice, or charge action, and it renders an
explicit blocked state while no approved template exists.

## Provider and business inputs still required

- approved agreement template content and durable repository/version reference;
- final template merge-field selection and legal approval evidence;
- e-sign provider, authentication, webhook verification, signer routing, and
  retention policy;
- payment provider workflow (invoice vs checkout), allowed methods, due date,
  ACH/check clearing behavior, idempotency strategy, and webhook verification;
- resolved tax treatment before payment approval;
- the policy for full-amount requests when a rate plan has no deposit;
- customer authentication and delivery channel;
- operational ownership and response expectations for expired approvals.
