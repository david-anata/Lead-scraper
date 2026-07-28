# Building calendar projection delivery

Agent remains the booking source of truth. This slice projects approved holds
and reservation changes to one dedicated calendar through a provider-neutral
adapter boundary. It does not read calendar edits back into Agent.

## Safe defaults

- `BUILDING_GOOGLE_CALENDAR_ID` must identify a dedicated calendar. The
  `primary` alias is rejected.
- `BUILDING_GOOGLE_CALENDAR_SERVICE_ACCOUNT_JSON` must be present before the
  adapter is ready.
- `BUILDING_GOOGLE_CALENDAR_WRITES_ENABLED` defaults to false.
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
6. Enabling live writes is a separate production decision and is not part of
   this PR.

Rollback is application-only: the additive columns can remain. Pending,
claimed, or error rows remain inert while writes are disabled.
