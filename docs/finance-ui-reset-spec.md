# Finance UI Reset

## Outcome

Replace the current Finance dashboard and side-drawer workflow with a small,
trustworthy money brief while preserving Plaid, QuickBooks, commitments,
settlement matching, and the deterministic calculation rules.

## Daily experience

Finance has five destinations:

1. **Today** — verified cash, confirmed and expected money in, confirmed and
   expected money out, three 14-day outlooks, and no more than three next
   actions.
2. **Cash plan** — a read-only comparison of conservative, likely, and
   optimistic cash positions.
3. **Budget & savings** — posted-spending budgets, deterministic savings
   targets, and an evidence-bound LLM review that recommends but never acts.
4. **Review** — a guided inbox. One case opens on a normal page; the list never
   saves an answer.
5. **Accounts & setup** — connected Plaid accounts, counted and excluded cash,
   freshness, refresh, and bank connection.

## Trust contract

- Every amount names its evidence class, source, and date.
- Posted cash is never blended with forecast income.
- The same evidence produces the same calculation ID.
- “See the math” shows the exact inputs and formulas.
- Stale calculation links disclose that current evidence changed.
- A consequential review answer requires a full-page preview, a reason,
  explicit confirmation, and a receipt with undo.
- Review decisions do not edit Plaid or QuickBooks.
- Payroll, tax, and debt items cannot be removed through the simple review
  action.

## Acceptance criteria

- The old dashboard cards, recommendation drawer, and bulk-answer queue are not
  part of the daily Finance home.
- The five evidence amounts and all three outlooks are visible and correctly
  labeled.
- Plaid connection and refresh remain available.
- QuickBooks-backed confirmed receivables remain part of the calculation.
- Desktop widths of 1280px and 1440px and a 390px phone view have no material
  clipping or unreachable controls.
- The production happy path, empty/error implementation, keyboard focus, and
  browser console pass the post-deployment visual gate.
