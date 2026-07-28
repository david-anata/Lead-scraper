# Finance Bill Queue Usability Upgrade

## Outcome

Make **What is coming** understandable and safe for a non-technical Finance
operator on desktop and iPhone. The operator should understand what each answer
will change before saving it, find a vendor quickly, and audit prior decisions
without needing database or accounting knowledge.

## Verified current behavior

The production page is a 55-row table with vendor, amount, frequency, next due,
confidence, evidence, row actions, filters, bulk selection, and vendor combine.
Desktop scanning is substantially faster than the former card list and the page
has no browser errors.

The production walkthrough also verified:

- the **Over $500/month** filter compares one projected payment to $500 rather
  than comparing normalized monthly cost;
- bulk fields remain visible for every action, including actions where those
  fields do not apply;
- phone width requires horizontal table scrolling and puts answers off-screen;
- bank-derived vendor names are often difficult to recognize;
- Track, Not now, and Combine do not fully explain their consequence before the
  final action;
- there is no vendor search, explicit sort control, or visible decision history.

## Users

- Primary: Finance operator reviewing detected bills.
- Secondary: Owner or auditor checking who made a decision and why.
- Mobile: the same operator performing quick reviews from an iPhone.

## Scope

### 1. Correct monthly-cost filtering

Calculate and expose a normalized monthly cost for every pattern:

- weekly: amount × 52 ÷ 12;
- every two weeks: amount × 26 ÷ 12;
- monthly: amount;
- quarterly: amount ÷ 3;
- annual: amount ÷ 12.

Use that value for **Over $500/month** and display the normalized monthly amount
where it helps explain why a row matches.

### 2. Make bulk controls action-specific

The operator chooses an action before seeing its additional fields.

- **Track:** optional category, payment structure, and next expected payment.
- **Not a bill:** optional reason only.
- **Ask me next week:** no unrelated fields.
- **Combine:** opens the dedicated preview workflow.

Fields that do not affect the selected action must not be submitted or saved.

### 3. Build a mobile review presentation

Below the approved mobile breakpoint, replace the wide operational table with a
compact row/card presentation:

- vendor and amount are first;
- next due, frequency, confidence, and monthly cost follow;
- evidence remains available through native disclosure;
- one visible **Review** control opens row actions in a native dialog or bottom
  sheet;
- selection and bulk review remain reachable without horizontal scrolling.

Do not remove information on mobile. Reorder it.

### 4. Improve vendor recognition

Keep the original bank wording as immutable evidence, but display a cleaned
vendor name when one is available.

Add deterministic cleanup first. The recommendation layer may suggest:

- a clearer display name;
- likely duplicate vendors;
- a likely canonical vendor to retain.

Suggestions never combine or rename vendors without operator confirmation.

### 5. Preview tracking consequences

Before Track is confirmed, state:

- amount added to the forecast;
- normalized monthly cost;
- next expected date;
- whether it affects the 14-day or 30-day view;
- whether an existing schedule or obligation appears to cover it.

If the detected bill may duplicate a schedule, pause confirmation and show the
possible match.

### 6. Make postponement explicit

Rename **Not now** to **Ask me next week**. Show the exact return date in the
confirmation and audit record. If postponement duration becomes configurable,
offer a small set of plain-language choices rather than an unrestricted date.

### 7. Separate combine preview from confirmation

The combine workflow has two unambiguous stages:

1. **Preview combination** computes the proposed name, amount, frequency, next
   due date, and source histories without writing.
2. **Confirm combination** performs the audited alias write.

Changing the canonical vendor or name invalidates the prior preview and returns
the dialog to stage one.

### 8. Add search and explicit sorting

Add vendor search above the workspace. Search covers cleaned display name and
original bank descriptions.

Offer sorting by:

- highest monthly cost;
- next due;
- confidence;
- vendor name.

The active sort must be visible. Default to highest normalized monthly cost.
Filters, search, and sort work together and update a visible result count.

### 9. Expose decision history

Add a recent-decision activity region and per-vendor history. Each entry shows:

- action;
- vendor;
- before and after values where applicable;
- actor;
- timestamp;
- reason or evidence;
- batch identifier for bulk actions;
- undo or reversal when still available.

The activity view reads the existing authoritative audit records. It must not
create a second audit source.

## Target workflow

1. Operator opens What is coming and sees the count, active sort, source
   freshness, and highest monthly-cost items.
2. Operator searches or filters the queue.
3. Operator opens evidence or chooses Review.
4. The interface explains the consequence of the proposed answer.
5. Operator confirms; the row leaves in place and count updates.
6. A visible confirmation offers Undo where permitted.
7. The activity region records who changed what and when.

## Required states

- **Loading:** preserve table/card dimensions and identify what is loading.
- **Empty queue:** explain that all detected patterns have an answer and link to
  schedules and recent decisions.
- **No filter results:** retain filters, show zero matching results, and provide
  Clear filters.
- **Stale row:** keep the row in place and explain that newer bank history
  changed it.
- **Save failure:** restore the exact row and selection with an inline message.
- **Possible duplicate:** block Track until the operator reviews the matching
  schedule.
- **No JavaScript:** real forms and server redirects remain usable.
- **Permission failure:** explain that Finance access is required without
  exposing internal details.

## Data and API impacts

- Add normalized monthly cost to the bill-pattern presentation contract.
- Preserve original bank descriptor separately from cleaned display name.
- Make preview endpoints read-only and confirmation endpoints idempotent.
- Continue using one transaction and one post-commit refresh for bulk writes.
- Reuse `finance_action_audit` for visible decision history.
- Do not alter historical transaction amounts or posted bank evidence.

## Accessibility

- Maintain native table, checkbox, details, dialog, form, and button semantics.
- Give every row selection and Review control a vendor-specific accessible name.
- Move focus into dialogs and return it to the originating row on close.
- Announce result counts, save progress, success, and failure.
- Support keyboard-only operation, 200% zoom, and reduced motion.
- Never use confidence color without its text label.

## Non-goals

- Automatically approving bills with an LLM.
- Changing posted bank transactions.
- Rewriting the Bookkeeping queue.
- Initiating payments or payroll.
- Replacing authoritative deterministic bill calculations with model output.

## Acceptance criteria

1. A $400 weekly bill appears in **Over $500/month**.
2. Default order uses normalized monthly cost, not single-payment amount.
3. Bulk Track shows Track fields; Not a bill and Ask me next week do not.
4. At 390px width, every row can be understood and answered without horizontal
   page or table scrolling.
5. Original bank descriptions remain available as evidence after display-name
   cleanup.
6. Track shows amount, date, forecast window, and possible duplicate before save.
7. Ask me next week shows the exact return date.
8. Combine cannot be confirmed until a current preview exists; editing its
   inputs invalidates that preview.
9. Search, filters, and sorting compose correctly and show the matching count.
10. An operator can locate actor, time, action, and before/after values for a
    completed single, bulk, combine, undo, or reversal action.
11. Stale and rejected actions leave the original row and selection in place.
12. The no-JavaScript workflow still completes each supported answer.

## Validation plan

### Automated

- Unit tests for every frequency-to-monthly conversion and the $500 boundary.
- Route tests for action-specific fields and ignored irrelevant fields.
- Preview/confirm tests for Track and Combine, including preview invalidation.
- Search/filter/sort composition tests.
- Audit-history tests for single, bulk, undo, combine, and revoke.
- Regression tests for historical amounts, alias persistence, stale keys, one
  transaction, and one post-commit recalculation.

### Visual and interaction

After deployment to production or a browser-reachable sandbox:

- walk the populated, empty, filtered-empty, error, and possible-duplicate
  states;
- verify desktop at 1280px and 1440px;
- verify iPhone-sized layouts at 390×844 and a second narrow viewport;
- use keyboard navigation through filters, rows, dialogs, confirmation, and
  undo;
- inspect browser warnings/errors and confirm no unintended overflow;
- exercise only safe test records in production; use sandbox data for mutations
  that could affect real finance decisions.

## Rollout

1. Ship normalized monthly cost and corrected filtering first.
2. Ship action-specific controls and consequence previews.
3. Ship mobile presentation.
4. Ship vendor cleanup, search, sorting, and visible activity.
5. Run the required post-deployment visual audit before declaring each phase
   complete.

## Recommended defaults

- Default sort: highest normalized monthly cost.
- Postponement: seven calendar days.
- Mobile breakpoint: use the established Finance responsive breakpoint unless
  visual testing shows it cannot support the card presentation.
- LLM use: suggestions only; deterministic calculations and explicit operator
  confirmation remain authoritative.
