# Building tour inquiry handoff

Building Control provides a deliberate **Schedule tour** action for open
`kind=tour` inquiries. An authorized operator selects a linked workspace
offering and space, a Mountain-time start, duration, host, and meeting
location.

## Transaction and authority

`POST /api/internal/building/bookings/tour-inquiry-handoffs` is the
authoritative operation. One database transaction:

1. validates the inquiry and its active contact;
2. validates the linked workspace offering and space;
3. rejects inventory or host conflicts;
4. creates, or safely reuses, the inquiry's workspace reservation;
5. creates the scheduled `BuildingTour`;
6. links the inquiry, contact, reservation, and tour;
7. advances the reservation to `tour_scheduled`; and
8. stores idempotency and audit evidence.

The operation never creates an availability block or inventory hold. Any
validation or write failure rolls back the entire handoff. Exact retries return
the original reservation and tour; reuse of an idempotency key with changed
input fails with `409`.

## Time and access controls

The Building Control form labels input as Mountain time
(`America/Denver`). The admin adapter converts it to UTC before persistence.
The internal endpoint requires the configured internal API key. The browser
action additionally requires an authenticated user with
`building.events.manage`, a valid same-origin CSRF token, and the existing
Building Control access middleware.

## Fail-closed conditions

The handoff is rejected for a missing or inactive contact, a non-tour or closed
inquiry, an invalid or mismatched offering/space, an unavailable space, a past
start, a duration outside 15–240 minutes, a missing host/location, an
inventory conflict, a host conflict, or incompatible pre-existing journey
evidence.

## Operator verification

In Building Control, locate an eligible tour inquiry and expand **Schedule
tour**. After a successful action:

- the notice says the tour was scheduled and linked;
- the schedule action is no longer offered on that inquiry;
- the workspace reservation is `tour_scheduled`;
- the tour shows the selected UTC-equivalent time, duration, host, and
  location; and
- no availability block was created.

This workflow schedules internal authoritative records only. It does not send
email or SMS, write Google Calendar, or create a workspace hold.
