# Arena commercial reconciliation

Building Control can prepare a reviewable Arena rate-plan draft from the
currently verified commercial evidence. Preparing the draft is a deliberate,
CSRF-protected action requiring `building.pricing.manage` and the typed phrase
`PREPARE ARENA DRAFT`.

## Draft baseline

- 6,000 square feet and maximum public capacity 200
- $175 per hour
- six-hour minimum ($1,050)
- required $250 cleaning fee
- 50% deposit holds the date
- balance due 15 days before the event
- setup and teardown are add-ons with prices requiring operator input
- overtime is hourly, but remains non-calculable until a numeric rate is
  explicitly approved
- tax treatment remains `review_required`

The draft does not supply cancellation language and therefore cannot pass the
existing approval gate until that policy is reviewed.

## Evidence and conflicts

The Listing Copy Pack is recorded as the verified commercial baseline. The
dated, Vivint-specific 2025 agreement is recorded as corroborating evidence
only and is explicitly not a reusable legal template.

TidyCal/Calendar conflicts are stored separately:

- 70% deposit instead of 50%
- 30% due 48 hours before instead of the evidenced 15-day balance timing
- a placeholder payment link

Each conflict requires its own disposition and evidence. `reconciled_in_agent`
records the Agent decision but remains blocking. Deposit and balance conflicts
may be explicitly accepted as exceptions by the pricing owner. The placeholder
payment-link conflict requires `provider_remediated`; this workflow does not
perform that remediation or make any provider call.

## Governance boundary

Preparing or reconciling a draft never approves or publishes a rate plan,
sends a message, creates a charge, or writes TidyCal, Google Calendar, or a
payment provider. Approval remains the existing separate
`building.pricing.approve` action with typed confirmation and approval
evidence.

Commercial metadata, source evidence, and conflict evidence remain internal.
They are retained in frozen proposal/agreement snapshots after approval but are
not exposed wholesale by the public offering or estimate APIs.
