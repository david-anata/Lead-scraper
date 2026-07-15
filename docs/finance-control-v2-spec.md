# Finance Control V2

Status: Build-ready

## Plain English Summary

### User outcome

Finance must answer four questions without requiring the operator to inspect raw tables:

1. How much cash is actually on hand?
2. What money is likely to arrive?
3. What must be paid, and when?
4. What is the safest next action?

The existing Finance page has the right foundation but behaves like a reporting dashboard. It treats incoming money as secondary, mixes overdue exposure with amounts actually scheduled for payment, hides exact dates in the action queue, and presents recommendations without enough explanation or confidence.

### What changes

`/admin/finances` remains the only Finance page. The desktop page is rebuilt into one scan path:

1. **Cash position**: cash on hand, incoming, outgoing, and either safe-to-commit or a funding gap.
2. **Smart brief**: compact Happening, Broken, and Next statements.
3. **Cash trajectory**: a compact actual-versus-forecast chart with Committed, Expected, and Stress paths.
4. **Money queue**: one queue with Needs action, Incoming, Payables, and Recent filters.

Smart suggestions are deterministic, explainable, preview-only, and shown behind a Smart mode toggle. AI may rewrite an explanation for clarity; it must not calculate, select, or execute a financial action.

### Data sources

- **Manual bank CSV** is the source of truth for posted cash, actual inflows/outflows, and historical trends.
- **ClickUp** supplies planned AP/AR, dates, priority, and notes.
- **QuickBooks Online** supplies open invoices and, when connected, bank/invoice records.

ClickUp completion is operational evidence, not payment proof. A completed
ClickUp obligation stays in the audit trail as **Completed in ClickUp**. It
leaves required cash only after a bank CSV snapshot dated after the completion;
otherwise it stays reserved in **Needs action** until bank evidence catches up.
Only a posted bank allocation can mark an obligation settled.

Manual bank CSV history is sufficient to detect burn, volatility, recurring deposits, and recurring expenses. Trend-inferred future income is always labeled Expected. It is never treated as confirmed cash until an operator or documented AR source confirms it.

### Why this matters

The change makes Finance an operator system rather than a larger dashboard. It protects against three current failure modes:

- paying a duplicate or already-paid bill;
- treating a chunk-payable balance as one immediate payment;
- relying on trend-inferred income as though it were guaranteed.

## ASCII Mockup

### Default desktop page

```text
+--------------------------------------------------------------------------------------+
| FINANCE CONTROL                         Updated 12m ago   Smart mode [ON]  [Update $] |
| One page for cash, collections, payments, and the next safest action.                |
+--------------------------------------------------------------------------------------+
| CASH ON HAND          | INCOMING 14 DAYS     | REQUIRED OUT 14 DAYS | FUNDING GAP    |
| $4,983.92             | $18,000 confirmed    | $22,500 required     | $9,516          |
| Bank CSV - Jul 13     | + $7,500 expected    | + $40,000 exposure   | Floor: $10,000  |
+--------------------------------------------------------------------------------------+
| HAPPENING             | BROKEN               | NEXT                                 |
| Cash fell 18% in 28d. | 2 receipts need      | Confirm Acme's $8k receipt before   |
| Confirmed income      | dates; 3 possible    | scheduling the next rent installment.|
| covers 80% of must-pay| payment matches.     | [Review recommendation]              |
+--------------------------------------------------------------------------------------+
| CASH TRAJECTORY - 28 DAYS                                                       [...] |
| $30k -- actual ----|-- committed ---- expected ---- stress                           |
|                    Today       floor breach Jul 19                                   |
+--------------------------------------------------------------------------------------+
| MONEY QUEUE                                                                          |
| [Needs action 5] [Incoming 6] [Payables 16] [Recent]               Show: 14 days v   |
|--------------------------------------------------------------------------------------|
| ACTION          PARTY             TIMING             AMOUNT      CASH IMPACT      ... |
| Resolve match   Fulfillment Pay   Jun 01 - 43d late  $1,100      +$1,100 if match  [...]|
| Collect now     Acme              Jul 17 - confirmed $8,000 in   funds rent        [...]|
| Pay installment Rent              Jul 18             $5,000 out  min cash $10,400  [...]|
| Confirm date    Client B          Date missing       $4,500 in   excluded today    [...]|
|--------------------------------------------------------------------------------------|
| Next week (7 items - $12,400)                                               [Expand] |
+--------------------------------------------------------------------------------------+
```

When the stress path remains above the floor, the fourth metric is **Safe to commit**. When it breaches the floor, the same location becomes **Funding gap**. A negative “safe to spend” number is never shown.

### Row quick-action menu

```text
Rent - $40,000 remaining                                                [...]
                                                                      +----------------------+
                                                                      | Preview cash impact  |
                                                                      | Record partial payment|
                                                                      | Split into installments|
                                                                      | Defer / change date   |
                                                                      | Match bank transaction|
                                                                      | Mark paid             |
                                                                      | Flag duplicate        |
                                                                      | Open ClickUp source   |
                                                                      +----------------------+
```

Incoming rows replace payment actions with:

- Confirm expected date
- Mark received
- Match bank deposit
- Change confidence
- Assign follow-up
- Open invoice or ClickUp source

All write actions open a preview before confirmation. No smart action executes directly from the menu.

### Recommendation preview drawer

```text
+-------------------------------- SMART RECOMMENDATION -------------------------------+
| Split rent into $5,000 on Jul 18 and $5,000 after Jul 25.                           |
|                                                                                     |
| Why: cash falls below the $10,000 floor if the full balance is reserved now.        |
| Before: minimum stress cash -$2,600   After: minimum stress cash $10,400            |
| Depends on: confirmed $8,000 receipt on Jul 17                                      |
| Confidence: Medium - receipt date is confirmed; only 6 weeks of trend history.      |
| Downside: $35,000 remains overdue and requires a payment plan.                      |
|                                                                                     |
| [Cancel]                                               [Create installment preview] |
+-------------------------------------------------------------------------------------+
```

### Update money modal

```text
+-------------------------------- UPDATE MONEY ---------------------------------------+
| Drag bank CSV or QBO Open Invoices CSV here                                         |
| [Choose files]                                                                       |
|                                                                                     |
| ClickUp                                        Connected - synced 12m ago [Refresh]  |
| Manual exception                              [Add payable] [Add incoming]           |
|                                                                                     |
| Accepted: bank transaction export, QBO Open Invoices report                         |
+-------------------------------------------------------------------------------------+
```

### Import preview

```text
+-------------------------------- IMPORT PREVIEW -------------------------------------+
| 842 rows read                                                                       |
| [Ready 18] [Duplicates 821] [Needs review 3]                                        |
|                                                                                     |
| Needs review                                                                        |
| - Missing transaction ID; fingerprint matches an existing debit                     |
| - Two possible matches for a $1,100 Fulfillment payment                             |
| - Balance date is older than the current snapshot                                   |
|                                                                                     |
| [Cancel]                                                         [Import 18 records]|
+-------------------------------------------------------------------------------------+
```

### Important states

**Loading**

- Keep the page shell visible.
- Show skeletons for the four cash metrics and queue.
- Show “Calculating forecast” in the trajectory area.

**Low confidence**

- Metrics remain visible but carry a Low confidence badge.
- Smart mode offers verification actions only.
- Payment/defer recommendations are suppressed.

**Stale or missing bank balance**

- Cash on hand shows “Needs update,” not `$0`.
- Safe to commit/funding gap shows “Unavailable.”
- Next action becomes “Upload the latest bank CSV.”

**No planned income**

- Incoming shows `$0 confirmed` plus any trend-detected expected patterns.
- The page explicitly says trend income is excluded from the Committed path.

**No queue items**

- Show “No money decisions require attention in the selected window.”
- Keep Update money and Add incoming/payable available.

**Import failure**

- Preserve the current page data.
- Show the failed file, reason, and downloadable row-error report.
- Never partially commit a failed import batch.

## Technical Specification

### Scope

- Replace the current `/admin/finances` renderer with the desktop Finance Control V2 hierarchy.
- Keep the existing Anata header, typography, colors, cards, and button language.
- Add the unified money queue, row drawer, quick-action menus, update modal, and import preview.
- Make incoming money a first-class part of summary metrics, forecast paths, queue, and recommendations.
- Add durable partial-payment allocations and planned installments.
- Reuse the existing bank CSV parser, QBO Open Invoices parser, ClickUp sync, matcher, and trend detector after hardening them.
- Hide or redirect all legacy Finance destinations to `/admin/finances`; do not restore Finance sub-navigation.

### Out of scope

- Mobile-specific layouts.
- Additional Finance pages.
- Direct bank integration beyond existing QBO support.
- Initiating bank payments, sending invoices, or sending collection messages.
- Autonomous financial actions.
- New external integrations.
- Multi-company accounting or formal general-ledger reporting.

### Canonical money model

An obligation is an AP or AR promise. A posted bank transaction is an actual. They must remain separate records connected through allocations.

Add `cash_events.record_kind` with values `obligation` or `transaction`. AP and AR remain obligations distinguished by `event_type`; separate AP and AR tables are unnecessary.

Add:

```text
payment_installments
- id
- obligation_event_id
- amount_cents
- due_date
- status: planned | paid | cancelled
- created_at / updated_at

settlement_allocations
- id
- obligation_event_id
- transaction_event_id (nullable for manual payment evidence)
- installment_id (nullable)
- amount_cents
- allocation_date
- source
- confidence
- idempotency_key
- reversed_allocation_id (nullable)
- notes
- created_at

finance_source_records
- id
- cash_event_id
- source_system
- scope_key
- entity_type
- external_id
- payload_hash
- soft_fingerprint
- unique(source_system, scope_key, entity_type, external_id)

finance_import_batches
- id
- source_type
- file_hash
- status: staged | posted | failed | cancelled
- ready_count / duplicate_count / review_count / invalid_count
- created_at / posted_at

finance_import_rows
- id
- import_batch_id
- row_number
- raw_payload / normalized_payload
- classification: new | update | duplicate | conflict | invalid
- reason
```

Add explicit obligation fields where the existing schema/notes are insufficient:

```text
cash_events.pay_priority: must_pay | should_pay | review | can_hold
cash_events.minimum_payment_cents: nullable
cash_events.flexibility: fixed | chunkable | deferrable | unknown
```

Existing `amount_cents` remains the face/open-source amount. Derived values are:

```text
settled_amount = sum(active settlement_allocations.amount_cents)
open_amount = max(amount_cents - settled_amount, 0)
scheduled_amount(window) = sum(open payment_installments in window)
```

An obligation becomes paid only when `open_amount == 0`. A partial match must never close the full obligation.

Settlement allocations are append-only. Corrections create reversal allocations; they do not delete financial history. One transaction may allocate across multiple obligations, and one obligation may receive multiple transactions.

### Source precedence

- Bank CSV posted transactions win for actual cash movement and cash balance.
- QBO invoice open balance wins for documented AR balance when newer than ClickUp/manual data.
- ClickUp wins for operational due date, priority, and notes when the obligation is sourced there.
- Manual edits are local operator overrides and must be audit logged.
- Local allocation/settlement state must not be overwritten by ClickUp or QBO sync.
- Probable cross-source duplicates are quarantined for review; they are not silently merged.

### Import flow

1. Upload one or more files.
2. Detect bank CSV versus QBO Open Invoices CSV.
3. Parse into a staging batch; do not write canonical records yet.
4. Classify rows as Ready, Duplicate, or Needs review.
5. Show preview and net effect on cash/obligations.
6. Commit the accepted rows atomically.
7. Run reconciliation and trend detection.
8. Rebuild Finance state and recommendation candidates.

Remove the user-facing `replace_range` option. Re-imports update or skip by stable identity; they do not delete historical actuals.

Identity rules:

- Bank actual: `(account/source, transaction_id)`; missing IDs use a fingerprint and require review.
- Obligation: `(source, external_id)`.
- Add database uniqueness where production data can be migrated safely.
- A single account/date range cannot use both QBO-bank and bank CSV actuals without reconciliation.

### Migration and cutover

Before applying schema changes, run a read-only production profile for:

- duplicate and blank `(source, source_id)` identities;
- dangling `matched_to_id` values;
- terminal paid/matched rows without bank evidence;
- QBO Payment/Deposit overlap;
- ClickUp/QBO records that probably represent the same obligation.

Then:

1. Add new columns/tables without changing current reads.
2. Backfill `record_kind` and `finance_source_records`.
3. Convert valid legacy matches into settlement allocations.
4. Preserve unsupported legacy paid states as reversible `legacy_assertion` allocations.
5. Dual-write and compare legacy versus allocation-derived totals.
6. Cut forecasts and queue reads to allocation-derived open balances only after reconciliation passes.

No destructive cleanup occurs during migration.

### Forecast paths

Build daily paths for at least 28 days:

**Committed**

- Current cash snapshot.
- Full remaining required outflows and scheduled installments.
- Only explicitly confirmed incoming obligations.

**Expected**

- Full outflows.
- Confirmed income plus probability-weighted expected income.
- Receipt date adjusted by customer-specific median payment lag when enough history exists.

**Stress**

- Required outflows on the earliest plausible dates.
- Unconfirmed income excluded or delayed to conservative/P80 timing.
- Flexible payments remain visible as exposure but are not assumed paid unless scheduled.

Core values:

```text
floor = configured operator floor (initial default: $10,000)
minimum_stress_cash = minimum daily cash on the stress path
safe_to_commit = max(0, minimum_stress_cash - floor)
funding_gap = max(0, floor - minimum_stress_cash)
```

The floor is displayed and configurable, not hidden in code.

### Trend rules

Use posted bank CSV history after excluding transfers and probable duplicates.

Calculate:

- 28- and 56-day net-cash direction;
- median weekly inflow/outflow;
- burn and volatility;
- recurring deposit/expense cadence;
- customer receipt lag when matched AR history exists;
- vendor payment cadence, including partial-payment patterns.

Trend signals are evidence, not confirmed future events. Require at least three comparable occurrences before suggesting a recurring pattern. Low-history trends must be labeled Low confidence and excluded from Committed calculations.

### Recommendation engine

Implement a pure deterministic function that accepts canonical Finance state and returns ranked recommendation candidates. Rank in this order:

1. Missing/stale data or low confidence.
2. Probable duplicate or payment/receipt match.
3. Collect confirmed income that prevents a floor breach.
4. Split/defer eligible AP that prevents a floor breach.
5. Pay/schedule must-pay AP when the stress path remains safe.
6. No action when the stress path is healthy.

Every recommendation returns:

- action type and eligible target;
- triggering facts;
- before/after minimum cash;
- dependencies and excluded income;
- confidence and limitations;
- downside;
- expiration/recalculation time.

Hard gates:

- Low-confidence or stale-balance state permits verification actions only.
- No proposed payment may exceed safe-to-commit unless explicitly labeled a funding-gap decision.
- `can_hold` and `chunkable` actions may be suggested; `must_pay` deferment requires explicit operator override.
- All writes require preview, confirmation, idempotency, and audit logging.

### Queue construction

Default filter is Needs action. Build queue groups in this order:

1. Resolve first: duplicate, possible actual match, missing amount/date, stale source.
2. Collect now: confirmed/overdue incoming with high cash impact.
3. Pay now: must-pay overdue/due within two days and funded.
4. Protect cash: chunkable/deferrable payments that breach the floor.
5. This week.
6. Next week collapsed.

Sort within groups by decision blocker, priority, operational category, overdue days, floor impact, due date, then amount.

Do not silently truncate rows. If a group is collapsed, show its count and subtotal.

### Route and service changes

Keep:

- `GET /admin/finances`
- existing manual AP/AR create/edit routes as exception flows
- existing ClickUp and QBO sync entry points

Add or refactor behind the page:

- staged import preview and commit services;
- canonical Finance-state builder;
- three-path forecast service;
- recommendation candidate builder;
- allocation/installment actions;
- calculation-detail payload for the drawer.

Legacy Finance GET routes continue redirecting to `/admin/finances` unless they are required as modal/form endpoints.

### Validation

Unit tests must cover:

- legitimate `$0` cash snapshots;
- newest-first bank CSV and same-day balances;
- source-local and cross-source dedupe;
- partial allocations never closing an obligation early;
- installment totals never exceeding open amount;
- ClickUp/QBO sync preserving local settlement;
- confirmed versus trend-inferred income treatment;
- Committed, Expected, and Stress path calculations;
- stale/low-confidence recommendation gates;
- quick-action eligibility;
- idempotent import and action writes;
- safe-to-commit/funding-gap formulas;
- queue grouping, ordering, and no silent truncation.

Desktop browser QA must verify:

- page scan order at 1280px and 1440px widths;
- exact due dates and days late;
- incoming and outgoing filters;
- quick-action menu and preview drawer;
- Update money detection and preview states;
- chart/card totals use the same cash snapshot;
- loading, empty, stale, low-confidence, and import-error states;
- all legacy Finance destinations return to the canonical page.

Production validation must confirm:

- Render deploy commit matches GitHub `main`;
- `/health` remains healthy;
- real bank CSV can re-upload idempotently;
- ClickUp refresh preserves allocations and does not recreate duplicates;
- QBO-disconnected state remains usable through CSV + ClickUp;
- actual Finance totals reconcile before and after migration.

### Risks and dependencies

- Existing matched rows can be reopened by source sync; migration must preserve terminal/local settlement state before UI rollout.
- Current partial-payment matching can incorrectly close near-full obligations; disable that behavior before relying on remaining balances.
- Historical records may not have stable external IDs; quarantine ambiguous records instead of forcing merges.
- Bank CSV alone cannot prove future income. The UI must preserve the Confirmed versus Expected distinction.
- The current fixed `$10,000` floor must move to an explicit setting before smart recommendations are trusted.
- QBO remains optional for the first release; the page must remain fully usable with bank CSV + ClickUp.

### Cross-surface consistency

- All Finance navigation resolves to the one-page control surface.
- Card, chart, forecast, queue, recommendation, and export use the same canonical cash snapshot and open-balance calculations.
- Income/outgoing labels and confidence meanings are identical in summary, queue, drawer, and imports.
- Manual AP/AR edits update the same canonical records used by ClickUp/QBO/CSV workflows.
- No hidden legacy page may calculate safe-to-spend, matching, or open balances differently.

## Delivery Sequence

1. Production profile: identity collisions, legacy settlements, source overlap, and baseline totals.
2. Settlement safety: allocations/installments, match preservation, source-status hardening, migrations.
3. Canonical Finance state: source precedence, dedupe, open balances, three forecast paths, recommendation tests.
4. Import workflow: staging, preview, atomic commit, error reporting, remove destructive replace.
5. One-page renderer: cash position, smart brief, compact chart, unified queue, drawer, quick actions.
6. Production audit: real-data reconciliation, Render verification, and operator QA.
