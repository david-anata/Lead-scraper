# Finance Source Reconciliation

Status: Build-ready

## Plain English Summary

### The job to be done

Every morning, an operator must be able to answer four questions from one
trusted Finance Control page:

1. What cash is actually available today?
2. What money is genuinely expected to arrive?
3. What cash commitment is still genuinely open?
4. What is the safest next action?

The current page has the intended presentation but not yet the necessary data
contract. A live ClickUp refresh updated 293 records while 240 payables still
remained in the queue. That is evidence that task status alone is not enough:
historical recurring tasks, source changes, provider completion, and bank
settlement are being represented as independent facts rather than one
obligation lifecycle.

### What will change

Finance Control will use a canonical obligation ledger behind the one-page UI.
The ledger does not become bookkeeping. It only decides whether a future cash
commitment is active, fulfilled, cancelled, superseded, uncertain, or settled.

- A ClickUp task is the work-plan record for an obligation.
- A QBO open invoice is the dated receivable record for an obligation.
- A Bank CSV row is the only actual cash and payment-settlement proof.
- A manual exception is explicit operator evidence, never an invisible override.

The system will stop treating every historical ClickUp row as a separate open
bill. Repeated payroll, rent, loan, and subscription work is grouped into an
obligation series. Each task instance is either the active occurrence, a
completed occurrence, a cancelled occurrence, or a superseded historical
occurrence. Only active occurrences reserve forecast cash.

### What the operator will notice

- The money queue will show a short **Needs action** list instead of old
  recurring history as current debt.
- A completed ClickUp task will be visible in **Completed / audit** but will
  not be counted twice as a payable. If the bank CSV is not newer, it will say
  exactly: **Completed in ClickUp. Upload newer bank evidence to release.**
- A deleted or moved ClickUp item will become **Source missing**, not silently
  disappear or remain indefinitely open.
- Partial payments reduce the remaining cash requirement rather than requiring
  an all-or-nothing close.
- All forecast figures will have a reason and source trail.

### Standard end state

An operator can trust the page for the morning payment decision because every
number is traceable to the latest source evidence. The page can still state
"decision paused" when the evidence is insufficient, but it must name the
specific record and the one action that resolves it. It must never manufacture
a cash gap from stale historical work.

## ASCII Mockup

### Cash decision scan

```text
+------------------------------------------------------------------------------------+
| FINANCE CONTROL                                   Sources current      [Update $] |
| Cash, collections, commitments, and one safest next action.                       |
+------------------------------------------------------------------------------------+
| CASH ON HAND       | CONFIRMED IN 14D | REQUIRED OUT 14D | FUNDING GAP             |
| $4,983.92          | $41,679          | $18,500          | $3,516                  |
| Bank CSV Jul 15    | QBO invoices     | 4 active items   | floor $10,000           |
+------------------------------------------------------------------------------------+
| HAPPENING                    | BROKEN                    | NEXT                    |
| 3 commitments need cash.     | 1 ClickUp completion has  | Upload the bank CSV     |
| 2 invoices are due this week.| no newer bank evidence.   | after the payroll run.  |
+------------------------------------------------------------------------------------+
| MONEY QUEUE [Needs action 5] [Incoming 3] [Payables 4] [Completed 18]              |
|------------------------------------------------------------------------------------|
| ACTION          PARTY          TIMING              AMOUNT      EVIDENCE        ... |
| Pay / split     Rent           Jul 18              $5,000      ClickUp + bank [...]|
| Verify payment  Payroll 5th    Completed Jul 14    $5,000      Bank CSV Jul 13 [...]|
| Collect now     Acme invoice   Jul 17 confirmed   +$8,000      QBO invoice     [...]|
+------------------------------------------------------------------------------------+
```

### Source/evidence drawer

```text
+--------------------------- PAYROLL 5TH -------------------------------------------+
| Current state: Completed in ClickUp; cash remains reserved                          |
| Why: task closed Jul 14, but latest Bank CSV is Jul 13.                             |
|                                                                                     |
| Timeline                                                                            |
|  Jul 04  ClickUp occurrence due                  $5,000                             |
|  Jul 14  ClickUp marked Complete                 operational evidence               |
|  Jul 13  Bank snapshot                           too old to prove payment           |
|                                                                                     |
| [Upload newer bank CSV] [Match existing bank row] [Keep as required] [Open task]  |
+------------------------------------------------------------------------------------+
```

### Reconciliation review

```text
+-------------------------- SOURCE EXCEPTIONS ---------------------------------------+
| 3 records need a decision. No other record blocks cash decisions.                  |
|------------------------------------------------------------------------------------|
| TYPE                 RECORD             SYSTEM FINDING                   ACTION    |
| Superseded occurrence Payroll 5th Mar 4  Newer recurring occurrence exists [Review]|
| Source missing       Vendor bill         Missing in 2 ClickUp syncs       [Review] |
| Payment ambiguity    Rent                2 candidate bank rows           [Review] |
+------------------------------------------------------------------------------------+
```

## Technical Specification

### Scope

1. Make a canonical obligation lifecycle authoritative for Finance Control.
2. Reconcile ClickUp recurring work, closures, cancels, moves, and deletions.
3. Match posted Bank CSV transactions to obligations automatically where evidence is
   strong, including partial payments.
4. Retain QBO as dated receivable truth and keep Bank CSV as actual cash truth.
5. Provide narrow, explainable exception handling for the residual ambiguous cases.
6. Rebuild queue, forecast, trust gate, and savings analysis from canonical active
   obligations only.
7. Preserve Finance Control as one desktop page; no new Finance navigation.

### Explicitly out of scope

- Replacing QuickBooks bookkeeping or a general ledger.
- Initiating payments, collecting funds, or modifying ClickUp/QBO automatically.
- Treating an LLM recommendation as financial truth or allowing it to settle records.
- Adding new data providers beyond Bank CSV, ClickUp, QBO, and manual exceptions.

### Canonical data contract

#### Obligation

An obligation represents one economic commitment or expected receipt. It has a
stable `obligation_key` and can have multiple source occurrences.

```text
obligations
- id
- obligation_key                 # normalized vendor + direction + recurrence identity
- event_type                     # inflow | outflow
- canonical_state                # active | completed_pending_evidence | settled |
                                 # cancelled | superseded | source_missing | review
- face_amount_cents
- outstanding_amount_cents
- due_date
- series_key                     # blank for one-time; stable for recurring work
- active_occurrence_id
- source_confidence
- created_at / updated_at
```

An **occurrence** is one provider representation of that obligation, such as a
ClickUp task, QBO invoice, manual item, or generated recurring date.

```text
obligation_occurrences
- id
- obligation_id
- source                          # clickup | qbo | manual
- source_id
- source_status
- source_updated_at
- amount_cents
- due_date
- recurrence_position             # ISO period or occurrence ordinal
- lifecycle_state                 # active | completed | cancelled | superseded | missing
- payload_hash
- source_url
- unique(source, source_id)
```

Bank CSV rows stay transactions. They never become obligations.

```text
settlement_allocations
- id
- obligation_id
- transaction_event_id            # Bank CSV transaction
- amount_cents
- matched_by                      # automatic | operator
- confidence_bps
- match_reason_json
- allocated_at
- reversed_at / reversal_reason
```

Manual overrides must use a dedicated append-only `finance_decisions` record.
They expire or require review after a declared period; they cannot permanently
overwrite provider truth.

### Source authority and precedence

| Fact | Authority | Fallback | Result |
|---|---|---|---|
| Cash on hand | Latest Bank CSV closing balance | unavailable | Never QBO or ClickUp |
| Actual payment/receipt | Posted Bank CSV transaction | explicit manual evidence | settlement allocation |
| Planned payable/receivable | ClickUp task | manual exception | active occurrence |
| Dated receivable | QBO open invoice | ClickUp/manual | confirmed incoming |
| Operational completion | ClickUp closed status | manual completion evidence | completed pending bank evidence |

Provider status must not overwrite a Bank CSV settlement allocation. A bank
transaction also must not silently close an unrelated occurrence merely because
the amount is identical.

### ClickUp reconciliation rules

1. Fetch all configured AP and AR lists with closed records included.
2. Record an immutable sync snapshot with list, task ID, status type, due date,
   amount, recurrence metadata, parent task, and `date_closed`/`date_updated`.
3. Map ClickUp `status.type == closed` to `completed`; map terminal cancellation
   labels to `cancelled`; do not infer from an arbitrary status label alone.
4. Build `series_key` from a configured source field when available. Otherwise use
   normalized direction, vendor, amount band, category, and recurrence cadence.
5. For a series, retain only the newest open/expected occurrence as `active` for
   a future period. Earlier occurrences become `superseded` when a later instance
   proves they are historical, unless they have an unpaid outstanding balance or
   conflicting payment evidence.
6. A closed occurrence moves to `completed_pending_evidence` until either:
   - a matched Bank CSV transaction proves settlement, or
   - a later Bank CSV closing snapshot exists and a policy explicitly permits the
     operator to release the reservation.
7. An occurrence absent from two successful snapshots becomes `source_missing`.
   It remains reserved, but only the individual exception blocks trust.
8. Reopened ClickUp tasks reactivate their occurrence unless bank settlement is
   already fully allocated; that conflict goes to the exception list.

### Bank matching rules

1. Candidate pool: only unresolved canonical obligations of the same direction.
2. Strong automatic match requires normalized counterparty similarity, amount
   compatibility, date window, and no competing candidate with comparable score.
3. Match full payments, partial payments, and a single payment split across
   installments. An allocation reduces `outstanding_amount_cents`; it does not
   destroy the original obligation.
4. Never auto-match when a candidate differs materially in counterparty, has a
   weak date relationship, or multiple candidates are within the configured
   ambiguity margin.
5. Each automatic match stores score components and is reversible.
6. New Bank CSV imports rerun matching idempotently, including ClickUp occurrences
   imported after the CSV.

### Forecast rules

```text
committed out = sum(outstanding active outflow obligations due in window)
confirmed in  = sum(QBO dated open invoices due in window)
expected in   = probability-weighted recurring deposits from Bank CSV history
cash on hand  = latest Bank CSV closing balance
funding gap   = cash floor + committed out - cash on hand - confirmed in
```

- `completed_pending_evidence`, `source_missing`, and ambiguous settlement records
  are included in committed cash and displayed as a separate exposure subtotal.
- `completed`, `cancelled`, `superseded`, and fully settled occurrences are audit
  visible but excluded from committed cash.
- Expected income never reduces the funding gap; it is displayed as scenario upside.
- A negative safe-to-spend metric is rendered as `Funding gap`, never as a spendable
  number.

### UX rules

- The queue defaults to `Needs action`, sorted by decision impact, not raw overdue
  age.
- Every active item shows exact date, source, remaining amount, evidence state, and
  one primary action.
- `Completed` is an audit filter, not a payables filter.
- Trust gate lists only material unresolved records. It must name the count, type,
  and one navigation path to resolve them.
- Update Money exposes Bank CSV, ClickUp refresh, QBO receivables refresh, cash floor,
  and manual exceptions. Each source reports its last successful snapshot.
- Savings opportunities run only from reconciled posted spend and active recurring
  commitments; they never use unresolved historical ClickUp records as spend evidence.

### Migration plan

1. Add new tables and backfill occurrences from existing `cash_events` without
   changing visible forecast results.
2. Build a dry-run reconciliation report for each existing ClickUp series:
   active, completed, superseded, source missing, ambiguous, and unmatched.
3. Compare old versus canonical required-out totals and produce a record-level delta.
4. Enable canonical calculation in shadow mode; display no operator-facing savings
   recommendations until the delta is reviewed.
5. Promote canonical calculation after targeted QA, retain legacy rows as immutable
   audit records, and stop reading them into forecast paths.
6. Remove legacy task-per-template expansions only after the backfill reconciliation
   verifies each is either linked, cancelled, or explicitly retained.

### Validation and acceptance criteria

The feature is complete only when all cases below are automated and verified in
the production-connected environment:

1. Closed one-time ClickUp task is audit-visible and excluded after valid bank
   evidence; it remains reserved before that evidence exists.
2. Completed recurring payroll/rent occurrence does not leave older occurrences
   active when the next occurrence supersedes it.
3. Reopened task returns to active unless fully settled by a bank allocation.
4. Cancelled ClickUp task is excluded immediately and remains audit-visible.
5. A task moved/deleted from ClickUp becomes source missing only after two complete
   snapshots; it remains cash-reserved and actionable.
6. A full bank payment settles exactly one obligation when evidence is strong.
7. Partial rent payment reduces only the remaining rent amount and preserves the
   full original balance/history.
8. Ambiguous bank payment creates an exception and cannot change the forecast.
9. Duplicate CSV import does not duplicate transactions, allocations, or cash balance.
10. QBO open invoice appears as confirmed incoming once and is not mistaken for a
    posted cash deposit.
11. A provider outage marks that source stale without overwriting the last trusted
    source state.
12. The queue's total required out equals the committed forecast outflow total for
    the selected window, excluding only explicitly labeled exposure categories.

### Risks and dependencies

- Correct ClickUp series grouping requires reviewing the finance list's recurrence
  configuration and any field available as a durable recurrence/parent identifier.
- Bank CSV merchant descriptions can be inconsistent; matching must remain fail-closed.
- QBO production connection must remain on the production company and expose open
  invoices. Its OAuth connection alone does not prove its selected company is correct.
- Historical data can be migrated safely only in shadow mode with a visible delta;
  a one-time destructive cleanup is not acceptable.

