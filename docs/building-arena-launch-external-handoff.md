# Arena launch — final external handoff

This is the minimal remaining packet after reconciling the approved owner
decisions, Agent workflows, public website, agreement readiness, and Google
Calendar connection.

## 1. Reusable agreement approval

Already completed by Agent:

- owner-approved commercial and operating rules are consolidated in
  `docs/building/agreements/arena-event-agreement-business-terms-v1.md`;
- the artifact is versioned and SHA-256 checksum-backed;
- all supported merge fields are allow-listed;
- Building Control can register the exact artifact as `in_review`;
- no provider send, signature, payment, hold, or booking success is implied.

External action:

1. Designate legal counsel or the authorized legal reviewer.
2. Review and supply the remaining legal clauses identified in section 11.
3. Store the complete reusable agreement in a durable repository/provider.
4. Record its approval reference and evidence in Agent.

The dated Vivint-specific 2025 agreement is evidence only and is not an
acceptable reusable template.

## 2. Dedicated event calendar verification

Current evidence:

- Agent already has a provider-neutral, idempotent Google Calendar projection
  outbox.
- Agent remains the source of truth.
- Live writes are disabled and dry-run is the default.
- The connected Google Calendar account currently returns
  `ACCESS_TOKEN_SCOPE_INSUFFICIENT` for calendar-list access.
- No dedicated Arena calendar ID, owner, or service-account access has been
  verified.

External action:

1. Reauthenticate the Google Calendar connection with calendar-list permission.
2. Create or identify one Anata-owned calendar dedicated to Arena events.
3. Grant the Agent service account only the access required to manage events on
   that calendar.
4. Configure its exact non-`primary` calendar ID and service-account
   credentials in production.
5. Review Agent’s readiness response and dry-run output.
6. Enable live writes only after an authorized operator accepts the dry run.

Do not use a person’s primary calendar and do not infer readiness from the past
“Event space tour” event.

## 3. Launch effective date

This is the final owner action, not a separate policy interview.

Choose the first future date on which the legally approved agreement and
verified dedicated calendar may be used consistently. Record that date in
Agent only after steps 1 and 2 are complete. It then unlocks approval of the
Arena rate plan; it does not retroactively approve older quotes or bookings.

## Completion evidence

Arena launch is ready only when Agent shows:

- `agreement_template = approved_reference`;
- `event_calendar = provider_verified`;
- `effective_date = accepted_policy`;
- one current approved Arena rate plan;
- no blocking source conflicts;
- customer-facing status still requires authoritative agreement, cleared
  payment, conflict check, and booking confirmation.
