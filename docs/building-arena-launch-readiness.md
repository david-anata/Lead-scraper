# Arena launch-readiness register

Building Control surfaces ten governed decisions required for an Arena launch:
cancellation policy, tax treatment/rate, setup price, teardown price, overtime
rate, venue payment workflow, reusable agreement template, dedicated event
calendar, transactional sender/owner, and effective date.

Before those decisions can be recorded, an authorized Building operator may use
**Prepare verified Arena catalog** with the typed phrase
`PREPARE ARENA CATALOG`. The idempotent action creates the stable `arena` space
and `arena-events` offering from the approved Listing Copy Pack evidence. Both
records remain private, unavailable, and unpublished. The action does not
approve pricing, claim availability, send, charge, or write a provider. An
existing incompatible identity or relationship fails closed for manual review.

Missing records remain `unresolved`. Each decision requires
`building.pricing.approve`, CSRF/same-origin protection, a key-specific status,
a value, evidence, and the typed phrase `DECIDE {decision_key}`. Every change
creates an audit event. The action never writes a provider, sends a message,
charges a payment method, or changes calendar state.

The payment-workflow row presents the supplied policy for explicit review:
venue card-only, no checks, overpayments, or third-party vendor payments, and
the date is held only after cleared funds. It is not accepted automatically.

Calendar readiness remains unresolved. Search evidence found one past “Event
space tour” on David’s primary calendar, no Arena event, and no calendar-list
evidence establishing a dedicated event calendar. Readiness requires a verified
calendar ID, owner, and service-account access.

For reconciled Arena rate plans, approval additionally requires the commercial
decision subset: cancellation, tax, setup, teardown, overtime, and effective
date. Agreement, payment, calendar, and sender decisions remain separate launch
blockers and are not falsely treated as rate-plan terms.
