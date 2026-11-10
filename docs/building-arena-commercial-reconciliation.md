# Arena commercial reconciliation

Building Control can prepare a reviewable Arena rate-plan draft from the
currently verified commercial evidence. Preparing the draft is a deliberate,
CSRF-protected action requiring `building.pricing.manage` and the typed phrase
`PREPARE ARENA DRAFT`.

## Draft baseline

Updated November 10, 2026:

- 6,000 square feet and maximum public capacity 200
- Half day (up to 6 hours) $1,500
- Full day $2,000
- Full day plus a half day $3,000
- Two full days $3,500
- No hourly rate
- No security deposit and no upfront cleaning fee
- 50% books the date; the remaining 50% is due before the event
- Full refund within 7 days of payment; after that the paid 50% is forfeited
- Weekday business events are not auto-quoted
- Parties are weekends only: Saturday 8:00 AM to midnight, Sunday 8:00 AM to
  10:00 PM, except New Year's Eve
- Booked time includes setup and teardown
- Extensions beyond the booked window are quoted separately
- Utah tax is 7.45% and is not included in the day rates

The draft includes the November 10, 2026 cancellation language: a full refund
within 7 days of payment, and forfeiture of the paid 50% after that. Approval
still requires the saved launch decisions and any blocking provider conflicts.

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
