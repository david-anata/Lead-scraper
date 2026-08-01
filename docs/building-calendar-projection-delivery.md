# Building calendar projection delivery

The dedicated Anata Events Google Calendar is the source of truth for date
occupancy. Agent remains the source of truth for the customer, quote, agreement,
payment, and operational evidence. When calendar authority is enabled, a new
event hold reads Calendar availability and writes its opaque hold synchronously;
either both Calendar and Agent succeed or Agent creates no hold.

## Safe defaults

- `BUILDING_GOOGLE_CALENDAR_ID` must identify a dedicated calendar. The
  `primary` alias is rejected.
- `BUILDING_GOOGLE_CALENDAR_SERVICE_ACCOUNT_JSON` must be present before the
  adapter is ready.
- When Workspace blocks direct write sharing to a service account, set
  `BUILDING_GOOGLE_CALENDAR_DELEGATED_SUBJECT` to the approved Anata calendar
  operator after authorizing only the Calendar scope for the service account's
  OAuth client in Workspace Admin.
- `BUILDING_GOOGLE_CALENDAR_WRITES_ENABLED` defaults to false.
- `BUILDING_GOOGLE_CALENDAR_AVAILABILITY_AUTHORITY` defaults to false during
  rollout. Enable it only with writes after controlled read/write verification.
- `POST /api/internal/building/calendar/sync` is a dry run unless the request
  explicitly sets both `execute=true` and `dry_run=false`.
- Building Control only performs a dry-run preview.

No production write should be enabled until the calendar is created, shared
only with the approved service account, and the readiness response is reviewed.

## Outbox lifecycle

`pending/error -> claimed -> synced`

- Agent queues one projection per reservation.
- The operation key and payload checksum identify the desired idempotent state.
- A worker commits a claim before invoking the adapter.
- Claims abandoned for ten minutes become reclaimable.
- Successful delivery records the provider event ID, delivery timestamp, and
  reconciliation timestamp.
- Failures return to `error` with bounded exponential backoff.
- Google event IDs remain deterministic, so a retry updates the same event.
- A projection targeting a different calendar fails before delivery and must be
  explicitly requeued.

## Inspection

- `GET /api/internal/building/calendar/readiness` reports non-secret provider,
  dedicated-calendar, dry-run, write-gate, and queue-count state.
- `GET /api/internal/building/calendar/projections` lists pending, claimed,
  synced, and error evidence.
- Building Control shows the projection queue and can preview pending work
  without an external call.

## Rollout

1. Deploy the additive columns with writes disabled.
2. Configure a dedicated calendar ID and service-account JSON.
3. Share only that calendar with the service-account email.
4. Verify readiness and dry-run output.
5. Review duplicate, error, retry, and mixed-calendar evidence.
6. Enable writes, run a controlled hold/create/delete verification, and then
   enable calendar availability authority. New event holds now fail closed when
   Calendar cannot be read or written.

Rollback is application-only: the additive columns can remain. Pending,
claimed, or error rows remain inert while writes are disabled.
