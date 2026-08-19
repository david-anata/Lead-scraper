# One press creates the contract, one press undoes it

Status: awaiting approval
Author: Claude, 2026-08-03
Decision on file: David chose "just make it work with undo" (option a) over a
confirm-then-create step.

---

## Premise checks (done before writing this)

**1. What this is.** The Arena lead page at `/admin/building/inquiries/{id}` has a
pricing panel ending in a "Create the contract" button. Source: David's screenshot
and the live URL he sent, plus `services/building_inquiry_workspace.py:256`.

**2. Third-party latency.** The only outside call on this path is the Anata Events
calendar read, already in the page render and already degraded-safe
(`calendar_status != "connected"` renders a warning and continues). Preparing the
package is in-process. No new third-party call is added, so the existing web
request budget is unchanged.

**3. What already exists.** Read before estimating. Most of this is built.

| Piece | Exists today | This spec |
|---|---|---|
| Take the date, freeze the quote, prepare the package in one request | Yes, one route: `create_contract_from_lead` | Unchanged |
| Date auto-chosen from the lead's own preferred date | Yes, `_calendar_view` line 119 | Unchanged |
| Guest hours defaulted from the customer's submission | Yes, `_calendar_view` lines 162-167 | Unchanged |
| Setup and teardown buffers computed, not typed | Yes, `access_window` | Unchanged |
| Clash detection and deliberate override | Yes | Kept, see D2 |
| Conflict check, audit trail, idempotency on the hold | Yes, `create_event_review` | Unchanged |
| Error banner on the lead page | Yes, line 1015 | Unchanged |
| Hold expiry sweeper | Yes, `expire_building_holds` | Unchanged |
| Confirmation round trip (`?confirm=contract`) | Yes | **Removed on the clean path** |
| Button hidden when it cannot work | No | **New** |
| Any way to cancel a prepared agreement | No. `PREPARATION_TRANSITIONS` has no edge into `cancelled` | **New** |
| Any way to release a hold by hand | No, date-based expiry only | **New** |
| Six interview answers required before a date | Yes, `EVENT_QUALIFICATION_REQUIREMENTS` | **Reduced** |

The build is four narrow changes, not a system.

**4. Unanswered question, proceeding on assumption.**
ASSUMPTION: David's specific lead is recorded as something other than an event.
That is the only reproduced state that produces his exact symptom (page reloads,
lands at top, no message). I could not confirm it because I am signed out of
Agent. If it turns out to be an event lead, Phase 1 still fixes it, because Phase 1
removes every silent path, not just that one.

---

## Section A — Plain English Summary

**What this is.** Today you press "Create the contract" on a lead and the page
reloads, drops you at the top, and nothing visible changes. After this, one press
does the whole job. It takes the date the customer asked for, holds it, freezes
the price you are looking at, and prepares the contract. Then it tells you what it
did in one line, with an Undo button sitting right next to it. Press Undo and it
puts everything back: the date is released, the contract is cancelled, the lead
returns to how it was.

**Why it matters.** Right now producing one contract means marking the lead
qualified, answering six interview questions, choosing a day on a calendar,
choosing hours, taking a hold, then finding a separate screen to prepare the
package. Six screens and a silent failure if you miss one. After this it is one
press from the lead you are already looking at. And when it genuinely cannot work,
it says so in that spot instead of pretending the button is live.

**What is changing.** The button on the lead page, and only that. The contract
itself, the pricing, the quote, the conflict checks, the audit trail, billing and
the QuickBooks handoff all keep working exactly as they do now, because the press
runs the same code it runs today. The separate Contracts screen is untouched. The
one behaviour deliberately kept in your way is double-booking: if the date is
already taken, it stops and asks, because that is expensive and you only have one
Arena.

---

## Section B — Mockup

### B1. Lead page, ready to go (the normal case)

```
┌──────────────────────────────────────────────────────────────┐
│  Rosa Delgado · event · anata-building                        │
├──────────────────────────────────────────────────────────────┤
│  Pricing                                                      │
│  ...                                                          │
│  Balance before the event                            402.94   │
│                                                               │
│  [ Save pricing ]   Applies to this lead only. Nothing sent.  │
├──────────────────────────────────────────────────────────────┤
│  [ Create the contract ]                                      │
│  Holds Saturday, September 12 (guests 4pm to 9pm), freezes    │
│  this pricing, and prepares the contract. Nothing is sent.    │
└──────────────────────────────────────────────────────────────┘
```

The button now says up front which date it is about to take, read from the
customer's own request. No calendar click needed.

### B2. Same page, one press later

```
┌──────────────────────────────────────────────────────────────┐
│  ✓ Contract prepared. Saturday, September 12 is held for 7    │
│    days. Nothing was sent.                     [ Undo ]       │
├──────────────────────────────────────────────────────────────┤
│  [ Open the contract ]   Version 1 · prepared                 │
└──────────────────────────────────────────────────────────────┘
```

One line saying what happened, with Undo right there. Undo stays available while
the contract is still just prepared and nothing has been sent, signed or paid.

### B3. The button cannot work (replaces the button, never a dead click)

```
┌──────────────────────────────────────────────────────────────┐
│  Cannot create a contract from this lead yet                  │
│                                                               │
│  This lead is recorded as a tour, not an event. Contracts     │
│  attach to an event date.                                     │
│  [ Change this lead to an event ]                             │
└──────────────────────────────────────────────────────────────┘
```

Other wordings for the other causes, same shape:
- "No customer is linked to this lead yet."  → [ Link the customer ]
- "No contract template is approved yet."    → [ Open templates ]
- "This lead already has a contract."        → [ Open the contract ]

### B4. The one stop that stays: a date clash

```
┌──────────────────────────────────────────────────────────────┐
│  Saturday, September 12 is already taken                      │
│  Held by: Vivint holiday party                                │
│                                                               │
│  Going ahead books two things in The Arena at once, on your   │
│  authority, and it is recorded against the booking.           │
│                                                               │
│  [ Book it anyway and create the contract ]  [ Pick another ] │
└──────────────────────────────────────────────────────────────┘
```

The only time you are asked twice, because you have one Arena.

### B5. After Undo

```
┌──────────────────────────────────────────────────────────────┐
│  ✓ Undone. September 12 is free again and the contract is     │
│    cancelled. The lead is back where it was.                  │
├──────────────────────────────────────────────────────────────┤
│  [ Create the contract ]                                      │
│  Holds Saturday, September 12 (guests 4pm to 9pm) ...         │
└──────────────────────────────────────────────────────────────┘
```

---

## Section C — Technical Specification

### C1. Architecture

No new services and no new tables. Three touched files:

- `api/building_inquiry_workspace_router.py` — the POST route loses its
  confirm round trip and gains a sibling undo route.
- `services/building_inquiry_workspace.py` — the button becomes a rendered
  decision (`ready` / `blocked` / `done`) rather than an unconditional form.
- `api/building_agreement_readiness_router.py` — `PREPARATION_TRANSITIONS`
  gains one edge, `prepared -> cancelled`, so an undo has a legal state to
  move into instead of deleting a row.

A new pure function, `contract_readiness(session, inquiry) -> Readiness`, is the
single source of truth for whether the press can succeed. Both the renderer and
the route call it, so the button and the guard can never disagree. This is the
core fix: today the render decides one thing and the route decides another, which
is exactly how a live button ended up pointing at a section that does not exist.

### C2. Phases (each independently shippable)

**Phase 1 — no more silent failure.** Add `contract_readiness`. Render B3 in
place of the button whenever it returns blocked. Every redirect on this path
carries either a notice or an error, and the anchor it jumps to is asserted to
exist in the rendered page. Ship this alone and David's dead click is gone even
if nothing else lands.

**Phase 2 — one press.** Remove the `?confirm=contract` branch for the clean
path. The route takes the date, freezes the quote and prepares the package in the
one request it already runs, using the date and hours the page already computes.
Keep the clash branch (C4/D2).

**Phase 3 — undo.** New route `POST /admin/building/inquiries/{id}/contract/undo`.
In one transaction: agreement `prepared -> cancelled`; reservation released and
its availability block removed; calendar projection cleanup queued through the
existing path the expiry sweeper uses; quote marked superseded, never deleted;
one audit event, `lead_contract_undone`, carrying the before state. Refuses, with
a reason, if the agreement is past `prepared`, if any payment is recorded, or if
a signing document exists.

**Phase 4 — trim the questions.** `EVENT_QUALIFICATION_REQUIREMENTS` drops from
six to the three the contract actually merges: attendance, guest schedule, and
candidate dates. Purpose, format and agreed next step become optional notes.
Rationale: the other three are never read by the package builder, so requiring
them gates a contract on sales hygiene rather than on contract content.

### C3. Dependencies

None new. No new package, no new environment variable, no schema migration.
`cancelled` already exists as a value on the agreement; only the transition map
forbids reaching it.

### C4. Decisions

**D1. One shared readiness function instead of fixing the two call sites.**
Chosen because the bug is a disagreement between renderer and route, and patching
each one leaves the next pair free to disagree. Alternative: add the missing
anchor so the jump lands. Rejected because it hides a dead button behind a
scroll. Drawback: one more indirection to read.

**D2. The clash stop stays, against a literal reading of "just make it work."**
David asked for no confirmation step. I am keeping exactly one: double-booking the
only event room. Everything else proceeds on one press. Flagging this plainly
rather than burying it, because it is a deviation from what was asked. If David
wants it gone too, it is a one-line change and the override is already built.

**D3. Undo is state-gated, not time-gated.** Available while the contract is still
`prepared` and nothing has been sent, signed or paid. Alternative: a fixed window,
say ten minutes. Rejected because a clock either expires while David is still
looking at the screen, or allows an undo after a customer has seen the document.
State is the honest test. Drawback: no undo once the contract is moved to review,
which is correct but must be said on screen.

**D4. Undo cancels, it does not delete.** The agreement version, the quote version
and the audit trail all survive. Alternative: hard delete for a clean lead.
Rejected because the whole billing chain reads these records, and a deleted quote
would strand an invoice. Drawback: a lead that was undone twice shows cancelled
versions in its history, which is accurate but not pretty.

### C5. Risks

| Risk | Mitigation |
|---|---|
| Undo releases a date the customer was already told about | Undo refuses once anything is sent or signed, and the success line says the date is released |
| One press takes a wrong date silently | The button names the date before you press it (B1), and Undo is one press |
| Removing four required answers weakens qualification | They become optional and still display as missing; only the contract stops depending on them |
| The shared readiness function drifts from the internal API's own preconditions | A test asserts every blocked reason maps to a real precondition, and that a `ready` verdict actually succeeds |

### C6. Success criteria

1. On every lead in every state, the contract control is either a working button
   or a written reason. Never a button that does nothing.
2. On a normal event lead, one press produces a prepared contract with no second
   screen.
3. Undo returns the lead, the date and the calendar to their prior state, and the
   history shows both events.
4. A clash still stops and still requires deliberate authorisation.
5. Billing, invoicing and the QuickBooks handoff read the same records they read
   today. The existing 26-step walkthrough still passes.

### C7. Verification strategy — three-pass rule

The build cannot ship until the loop passes **three consecutive times with zero
findings**. Any finding resets the counter to zero.

- **Pass 1 — automated.** Full building suite plus the new tests. The
  `arena_event_flow_walkthrough` script must still report 26 of 26.
- **Pass 2 — adversarial states.** Drive every blocked reason and every refusal
  through the real routes: non-event lead, no linked customer, no approved
  template, contract already exists, clash without override, undo after send,
  undo after payment, double undo, two presses racing.
- **Pass 3 — end-to-end Chrome walkthrough of the real workflow.** Signed in as
  David, in the browser, reading what a human reads. Requires David signed in;
  the browser tools are the only way to prove the page a person sees. Workflows
  walked in full:
  1. Website enquiry to prepared contract: open a real lead, read the button
     text, press once, read the result line.
  2. Undo: press Undo, confirm the date is free on the calendar panel and the
     contract shows cancelled.
  3. Blocked lead: open a non-event lead, confirm a written reason and no button.
  4. Clash: aim at a taken date, confirm the stop, override, confirm the warning
     is recorded on the booking.
  5. Read every new sentence aloud and ask whether it makes sense to the person
     seeing it.

### C8. Testing strategy

New file `tests/test_building_one_press_contract.py`:
- readiness returns `ready` only when the press can actually succeed
- every blocked reason renders as text, with no submit control present
- one press on a clean event lead produces reservation, quote and agreement
- one press is idempotent: a double submit produces one contract
- clash refuses without override, proceeds with it, records the authorisation
- undo reverses all three, and the calendar shows the date free
- undo refuses after send, after signature, after payment
- undo twice is refused the second time

Existing suites that must stay green unchanged: `test_building_lead_to_contract`,
`test_building_contract_workspace`, `test_building_inquiry_workspace`,
`test_building_lead_pricing`, `test_building_hold_jobs`.

---

## Out of scope

Sending anything to a customer, e-signature, the separate Contracts screen, the
coworking membership contract type, and the wider building page decomposition.
