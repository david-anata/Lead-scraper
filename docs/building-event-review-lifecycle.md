# Building event review lifecycle

Agent is the booking source of truth. This phase stops at an internal,
versioned quote draft and agreement readiness. It does not send a quote,
generate a contract, write Google Calendar, collect payment, or confirm a
booking.

## Existing primitives reused

- `BuildingInquiry` and its `qualified` lifecycle stage
- `BuildingReservation` and guarded event transitions
- `BuildingAvailabilityBlock` for conflicts and temporary holds
- approved, effective `BuildingRatePlan` records
- versioned `BuildingProposal` quote snapshots
- agreement, deposit-evidence, and confirmation gates
- the calendar projection outbox, audit events, and hold-expiry job

## New operation

`POST /api/internal/building/bookings/event-reviews`

Required headers are `X-Internal-Api-Key` and an `Idempotency-Key` of 8–128
characters. Reuse the key only for the exact same request.

```json
{
  "inquiry_id": "inquiry-123",
  "reservation_id": "event-2026-001",
  "space_id": "arena",
  "offering_id": "arena-events",
  "contact_id": "contact-123",
  "setup_starts_at": "2026-10-10T16:00:00-06:00",
  "guest_starts_at": "2026-10-10T18:00:00-06:00",
  "guest_ends_at": "2026-10-10T22:00:00-06:00",
  "teardown_ends_at": "2026-10-11T00:00:00-06:00",
  "hold_expires_at": "2026-08-01T17:00:00-06:00",
  "attendance": 80,
  "units": 4,
  "addons": [{"addon_id": "extra-cleaning", "quantity": 1}],
  "terms_summary": "Operator-reviewed terms for quote drafting.",
  "operator_notes": "Customer confirmed the full access window.",
  "assigned_owner": "events@anatabuilding.com",
  "actor": "operator@example.com"
}
```

The operation fails closed unless the inquiry is an event inquiry in
`qualified` or `closed_won`, the offering belongs to the selected space, the
full access window is conflict-free, and exactly one approved rate plan is
effective on the event date with approval evidence.

On success it atomically creates the `soft_hold` reservation, an availability
block from setup through teardown, quote version 1 with frozen commercial and
access-window evidence, a pending calendar projection, and audit/idempotency
evidence. The response keeps contract, signature, payment, and booking gates
false. An identical retry returns the original result; changed input with the
same key returns HTTP 409.

`GET /api/internal/building/bookings/{reservation_id}/lifecycle` returns the
admin timeline, quote versions, readiness gates, and a separately redacted
customer projection. Operator notes are not exposed in that projection.

The existing expiry job releases abandoned holds, queues calendar deletion,
and records audit evidence. The existing guarded `cancelled` transition is the
manual release path. Calendar records remain outbox data until a separately
authorized sync; this phase never calls Google Calendar.

## Permission and rollout

The Control Room form requires `building.events.manage`; legacy
`building.manage` remains compatible.

1. Apply the additive startup migration.
2. Grant event operators the narrow permission.
3. Verify a non-production approved rate plan and qualified inquiry.
4. Test create, identical retry, conflict rejection, manual release, and expiry.
5. Configure the expiry scheduler before enabling the form.
6. Select contract, payment, and calendar providers before enabling writes.
