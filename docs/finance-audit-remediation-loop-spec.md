# Finance Audit Remediation Loop

## Outcome

Finance must use one authoritative set of cash, rent, spending, and date facts
across Today, Cash plan, Calendar, Budget & savings, Review, and Accounts &
setup. A deployed pass is not complete until a production visual review confirms
that the visible numbers agree and the operator can correct the transactions
behind them.

## Verified production failures — 11 August 2026

- Calendar labels a $39,965 Boulder Ranch historical occurrence as planned on
  26 August while the same page says the operator-confirmed balance is $30,000
  and no payment is recommended.
- Cash plan ends after 14 days and can show a likely $36,739.62 while hiding the
  large rent event just beyond that horizon.
- Budget projects $26,027 of rent although the authoritative monthly rent is
  $40,000, Plaid shows $10,075 paid, and $30,000 remains.
- A posted Plaid transaction can show `Posted date: Unavailable`, put its date
  under `Planned date`, and say `$0 allocated from posted evidence`.
- The transaction drawer promises staged changes but exposes no correction
  actions. The shared workspace lacks duplicate, payroll, one-time, recurring,
  and merchant-normalization decisions.
- Today can use the next UTC date while Calendar uses the Denver business date.
- Budget recommendations use subscription wording for owner draws and other
  non-subscription transactions.
- Unsaved Budget changes appear on every Finance page without identifying their
  origin. Budget also has a second save bar that can compete with the global
  bar.
- Budget overflows horizontally around a 660px viewport even though phone and
  desktop layouts fit.

## Scope

### First remediation pass

1. Suppress historical rent occurrences whenever an authoritative rent balance
   exists. Show the balance as unscheduled until a safe proposed installment is
   calculated.
2. Build a shared Finance snapshot with both 14-day and month-end horizons.
   Today, Cash plan, Calendar, and Budget must consume the same rent facts and
   business date.
3. Show month-end cash alongside the 14-day scenarios. A favorable 14-day
   number may not conceal a known month-end obligation.
4. Make Budget rent use authoritative monthly, paid, and remaining amounts.
5. Expose transaction corrections directly from Calendar transaction rows and
   the details drawer using the existing draft → preview → batch save → undo
   contract.
6. Add correction actions for duplicate, payroll, internal transfer, one-time,
   recurring, merchant name, category, and note.
7. Preview the impact on Calendar, recurring spending, Budget, and forecast
   before saving.
8. Use one contextual save bar. It must identify the draft's origin and never
   overlap another save control.
9. Correct posted/effective date labels and describe unmatched posted evidence
   as `Not matched to a planned bill`.
10. Use category-specific savings advice and remove subscription language from
    owner draws, payroll, debt, and one-time purchases.
11. Fix the intermediate responsive breakpoint without introducing page-level
    horizontal scrolling.

### Non-goals

- No bank transfer, rent payment, payroll execution, vendor cancellation, or
  QuickBooks write.
- No deletion of Plaid or source evidence.
- No new independent Finance dashboard or separate cleanup product.
- No AI-authored financial totals. Rules calculate money; an LLM may explain
  already-calculated evidence.

## Target workflow

### Rent

- Calendar shows `Rent remaining: $30,000 · not scheduled` when no safe payment
  exists.
- A historical estimate cannot appear as a planned payable once the confirmed
  balance replaces it.
- Safe installments appear as `Proposed · not scheduled`, by day and week.
- Plaid alone moves an installment to paid and reduces the balance once.

### Cash horizons

- Today and Cash plan show `Next 14 days` and `Through month-end` together.
- Both use the same calculation ID and list the expenses that explain the gap.
- The normal recommendation preserves the $10,000 cash goal through month-end.

### Transaction cleanup

- Every transaction row has a visible `Clean up` action.
- The operator can select multiple rows and choose a correction.
- Selecting does not save. One `Review changes` action opens an exact preview.
- The preview explains affected totals and protected items.
- Saving is one auditable transaction and offers batch undo.
- Leaving with a draft triggers the established warning and retains the draft.

## Important states

- **Ready:** all surfaces share the calculation ID and business date.
- **Stale:** Plaid cash is older than yesterday; payment advice pauses.
- **Conflict:** historical rent or another source disagrees with authoritative
  facts; the authoritative value is shown and the conflicting occurrence is
  excluded from totals.
- **No safe installment:** rent remains visible but unscheduled.
- **Empty cleanup:** explain that no current transactions need classification.
- **Protected item:** payroll, tax, debt, and rent may be classified but are
  never silently removed from cash planning.
- **Save failure:** restore the draft and identify the rejected rows.

## Acceptance criteria

1. No page displays $39,965 as planned rent while the confirmed balance is
   $30,000.
2. Calendar, Budget, rent, and month-end forecast use the same authoritative
   rent facts and calculation ID.
3. Today and Cash plan display both 14-day and month-end results.
4. Budget cannot project $26,027 of rent when the current authoritative monthly
   obligation is $40,000 and the remaining balance is $30,000.
5. Posted Plaid rows show a posted date and never relabel it as planned.
6. A transaction can be marked duplicate, payroll, transfer, one-time,
   recurring, re-categorized, renamed, or noted without leaving the primary
   workflow.
7. Bulk corrections are previewed, saved once, audited, and undoable.
8. Corrections update every downstream read model on the next calculation.
9. Owner draws and protected categories receive category-appropriate advice.
10. One contextual save bar is visible at a time.
11. 390px, 660px, 1280px, and 1440px views have no page-level overflow,
    clipped controls, or obscured actions.
12. All dates use the Denver business date.
13. No route moves money or writes QuickBooks.

## Validation loop

For each deployed pass:

1. Compare visible rent, cash, Budget, and Calendar totals.
2. Open at least one posted transaction and one planned item.
3. Exercise transaction selection and preview without applying a real financial
   change.
4. Verify stale, conflict, empty, protected, and save-error implementations;
   use tests when production cannot safely create the state.
5. Inspect 390px, 660px, 1280px, and 1440px layouts.
6. Check keyboard focus, labels, contained scrolling, draft recovery, and the
   browser console.
7. Record any new material failure in this specification, fix it, redeploy, and
   repeat before calling the goal complete.

## Loop 1 — implemented and reviewed

- Rent now has one authority: conflicting historical and ledger rent candidates
  are suppressed when the saved payoff facts identify the same vendor.
- Today and Cash plan now separate the next 14 days from through month-end.
  Month-end reserves known Calendar costs plus the full remaining rent balance.
- Budget now uses the authoritative rent facts instead of inferring a different
  rent amount from historical bank activity.
- Transaction Details now provides Needed, Waste, Investigate, Duplicate, and
  Internal transfer actions. Duplicate and transfer corrections update the
  canonical transaction, flow into downstream calculations, are audited, and
  can be undone.
- Posted transactions show one posted date and clearly explain when they are
  not matched to a planned bill.
- A protected draft still triggers a leave-page warning until the operator
  explicitly reviews and saves or discards it.
- The Budget savings table switches to its contained card layout at tablet
  width, addressing the verified 660px page overflow.
- Automated review passed: 746 Finance tests, JavaScript syntax, Python syntax,
  and repository diff checks.

## Loop 2 — production visual review

Inspect Today, Cash plan, Calendar, Budget & savings, and a posted transaction
at 1440px, 1280px, 660px, and 390px after deployment. Confirm visible values
agree, the historical rent charge is absent, cleanup actions are usable, and no
page can silently abandon a draft. Any material failure becomes the next loop
before this work is complete. Payroll/one-time/recurring decisions, merchant
normalization, and cross-surface impact preview remain required scope if the
live review does not expose a safer consolidation path.

### Findings and correction

- Production commit `9e4f867` passed its readiness check.
- Calendar no longer counts the $39,965 estimate. It shows $30,000 remaining,
  not scheduled, and reserves $4,112 planned plus $18,773 possible first.
- Today now shows both the 14-day likely balance ($36,739.62) and the materially
  different month-end likely balance (-$28,890.29), making the horizon risk
  explicit.
- Budget shows $10,075.16 posted plus $30,000 remaining rent and no longer
  projects the old $26,027 value.
- The 660px Budget view and 390px Today, Cash plan, Calendar, and Budget views
  have no page-level overflow or competing fixed save controls.
- A real recovered five-change draft correctly stopped an attempted navigation;
  it was preserved and not discarded during QA.
- The posted transaction drawer exposes the cleanup controls and correctly says
  an unmatched bank withdrawal is still verified. Visual review found the
  posted date rendered as a raw ISO timestamp, so loop 2 reformats it as a plain
  business date.
