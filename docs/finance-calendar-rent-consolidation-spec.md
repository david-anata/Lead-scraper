# Finance Calendar and Rent Payoff Consolidation Spec

## Purpose

Correct the existing Finance Calendar so every rent recommendation uses the
same expenses, income, balances, and evidence already shown by the daily and
weekly calendar. This is a consolidation of current behavior, not a new page or
Finance subsystem.

## Verified current problem

On 11 August 2026, the live Calendar showed:

- $2,707 of possible expenses for the current week;
- $2,056 planned and unpaid for the following week;
- $12,906 of additional possible expenses for the following week; and
- further planned and possible expenses later in the month.

At the same time, the rent section said `Reserved for the rest of the month:
$0` and recommended $13,995 of rent payments. The values disagreed because the
weekly Calendar loaded recurring-history projections that the rent calculation
did not receive. The rent recommendations were also rendered separately instead
of appearing on the dates and weekly totals they would affect.

The current rent facts are:

- payee: Boulder Ranch Property Management;
- monthly rent: $40,000;
- operator-confirmed amount owed as of 11 August 2026: $30,000;
- normal operating-cash goal: $10,000;
- emergency floor: $0;
- TAX is a protected reserve and is not normal spendable cash; and
- Plaid-posted rent payments after the confirmed-balance date reduce the amount
  owed.

## Desired outcome

David can look at one Calendar and trust that:

1. all known and reasonably possible expenses are considered before rent;
2. every proposed rent payment appears on its proposed day and week;
3. the daily, weekly, and rent totals reconcile to one calculation;
4. normal rent recommendations preserve at least $10,000 of operating cash;
5. TAX is visible only as a last-resort scenario; and
6. Plaid evidence, not a manual status, confirms that a payment occurred.

## Scope

### In scope

- Make the existing Calendar loader the single source for the page.
- Pass that complete Calendar snapshot into the existing rent-paydown
  calculation.
- Include posted transactions, open obligations, payroll, taxes, tracked
  recurring costs, confirmed recurring-history costs, possible history
  warnings, and confirmed dated income.
- Reserve planned expenses at 100% before calculating rent.
- Reserve possible expenses at 100% in the normal rent recommendation until
  David classifies or dismisses them.
- Overlay calculated rent installments onto the existing daily drill-down and
  weekly roll-up.
- Keep proposed rent visibly separate from paid and unpaid obligations.
- Pause the rent recommendation when the Calendar and paydown inputs cannot be
  reconciled.
- Preserve the existing authoritative $30,000 balance, editable payoff facts,
  duplicate protection, Plaid confirmation, $10,000 cash goal, and TAX
  protection.

### Non-goals

- No new Finance page, navigation item, dashboard, or separate rent system.
- No automatic bank payment, ACH initiation, payroll action, vendor
  cancellation, or QuickBooks write.
- No redesign of the complete Finance information architecture.
- No change to the meaning of posted, confirmed, expected, or possible money.
- No assumption that a proposed rent installment has been approved or paid.

## One shared calculation

The Calendar route must load one immutable snapshot for the requested as-of
date. The snapshot must contain:

- the Calendar day buckets through month-end;
- canonical posted transactions after cross-source duplicate removal;
- open planned obligations and their remaining balances;
- recurring-history projections and their confirmation state;
- confirmed dated incoming money;
- effective spendable, reserve, liability, and excluded account balances;
- the $10,000 operating-cash goal and $0 emergency floor;
- the authoritative rent balance and its confirmation date; and
- a stable calculation identifier or input fingerprint.

The page's daily view, weekly roll-up, summary metrics, and rent calculation
must all consume this same snapshot. The route must not independently call a
reduced Calendar builder for rent.

## Calculation order

For each day from today through month-end:

1. Start with Plaid-verified spendable cash. TAX and other reserve accounts are
   excluded.
2. Add only confirmed incoming money dated for that day.
3. Subtract all planned non-rent expenses dated for that day.
4. Subtract all possible non-rent expenses as a cautious reserve.
5. Determine the lowest projected balance from that day through month-end.
6. Propose rent only from the amount above the $10,000 cash goal.
7. Never propose more than the authoritative rent balance remaining.

In plain English, every proposed installment must satisfy:

`verified spendable cash + confirmed income - other planned expenses - possible expense reserve - proposed rent >= $10,000`

The test applies to every later day through month-end, not just the proposed
payment date.

## Rent in the existing Calendar

### Daily drill-down

Add each suggested installment to its proposed date using the existing event
row pattern:

- name: `Boulder Ranch — proposed rent payment`;
- state: `Proposed · not scheduled`;
- evidence: `Calculated after other planned and possible expenses`;
- amount: the proposed installment;
- action: open the existing rent-balance section; and
- payment status: unconfirmed.

The proposal must not be described as Paid or Unpaid. Paid requires a matching
Plaid withdrawal.

### Weekly roll-up

Use the existing weekly table. Do not add another dashboard or duplicate table.
Include proposed rent in the `Possible · unconfirmed` amount and show a small
breakdown such as `Includes $8,249 proposed rent`. Planned non-rent expenses
remain in `Unpaid planned`.

When Plaid confirms a rent withdrawal, it moves to `Paid from bank`, leaves the
proposed total, and reduces the authoritative rent balance once.

### Rent section

Keep the existing Boulder Ranch section. It must show:

- monthly rent;
- operator-confirmed balance and confirmation date;
- Plaid-confirmed amount sent this month, deduplicated;
- other planned expenses reserved;
- possible expenses reserved;
- proposed rent by date and week;
- amount that cannot yet be funded;
- normal cash goal and emergency floor; and
- TAX last-resort amount separately, never blended into the recommendation.

Replace generic wording such as `$0 reserved` with explicit totals by evidence
class.

## Important states

### Ready

All sources share one calculation identifier, the Calendar totals reconcile,
and proposed rent can be shown.

### No safe rent payment

Show `No rent payment is recommended yet` and identify the planned or possible
expenses consuming the available cash.

### Data disagreement

Show:

> Rent recommendation paused because not all upcoming expenses were included.

Do not show installment amounts. The Calendar itself remains available.

### Stale bank data

Show the balance date and pause proposed payments when the balance exceeds the
existing freshness limit.

### No recurring-history projections

Show that no possible-expense reserve is available. Do not silently label it
as $0 of expected activity.

### Mobile

Preserve the existing single-column layout, visible Save action, draft safety
bar, contained weekly table, and no horizontal page overflow.

## Existing implementation to adjust

- `cashflow_router.finance_cash_calendar` must load the complete Calendar once
  and pass it to rent paydown.
- `rent_paydown.load_paydown_plan` must accept the existing Calendar snapshot
  instead of constructing a reduced one from ledger rows alone.
- `rent_paydown._outgoings_by_day` must consume both planned events and possible
  history warnings from that shared snapshot.
- `cash_calendar.build_cash_calendar` must support overlaying proposed rent
  events after the rent calculation without feeding those proposals back into
  the calculation.
- `cash_calendar._paydown_block` and the existing weekly/day renderers must use
  the reconciled output rather than independently derived totals.

No new persistent table is required. Existing Finance settings, cash events,
recurring-history decisions, Plaid accounts, and audit records remain the data
owners.

## Safety and ambiguity rules

- Canonical-source duplicate removal occurs before totals are calculated.
- A rent proposal is never treated as a second rent obligation.
- The current vendor's own tracked rent obligation is excluded from the
  non-rent reserve to prevent double counting.
- Possible expenses remain cautious reserves until classified. Recommended
  default: reserve 100% because over-reserving delays rent while
  under-reserving can bounce a payment.
- Confirmed incoming money may open a later installment; expected income may
  not.
- Same-day Plaid transactions already included in an operator-entered balance
  are not subtracted twice.
- TAX and other reserves are never included in the normal recommendation.

## Acceptance criteria

1. The rent calculation and weekly Calendar use the same calculation ID and
   source timestamp.
2. If the weekly table shows $2,056 planned and $12,906 possible, the rent
   calculation reserves those amounts before proposing rent.
3. `Reserved for the rest of the month` cannot display $0 when the shared
   Calendar contains future non-rent expenses.
4. Every proposed rent installment appears on the same date in the daily
   drill-down.
5. Every proposed rent installment is included in that week's `Possible ·
   unconfirmed` total with an explicit proposed-rent breakdown.
6. Proposed rent does not appear as Paid or Unpaid.
7. A Plaid-posted Boulder Ranch payment appears once, moves to Paid, and reduces
   the remaining rent balance once.
8. The normal recommendation never uses TAX or takes any later projected day
   below $10,000.
9. The last-resort TAX scenario remains clearly separate and never changes the
   recommended total.
10. Missing or inconsistent inputs pause the recommendation and explain the
    problem without breaking the Calendar.
11. Desktop and phone views have no clipping, covered actions, or horizontal
    page overflow.
12. No route moves money or writes to QuickBooks.

## Validation plan

### Calculation tests

- Planned and possible expenses from late in the month reduce today's safe rent
  amount.
- Confirmed incoming money permits a later installment only after its due date.
- Duplicate Plaid/QBO records count once.
- The rent obligation itself is not reserved and proposed twice.
- TAX never increases normal spendable cash.
- Every proposed schedule preserves $10,000 on every later day.

### Integration tests

- The Calendar route loads one snapshot and passes it to both renderers.
- Weekly, daily, and rent totals reconcile for the same fixture.
- A posted rent payment moves from proposed to paid and reduces the balance.
- A missing history feed pauses rent recommendations rather than returning $0
  possible expenses.

### Production visual gate

- Verify the live page with the real Boulder Ranch balance.
- Compare the rent reserve against current-week and later-week Calendar totals.
- Confirm proposed installments appear on their daily dates and weekly totals.
- Inspect the expanded payoff form and weekly table at desktop and 390px phone
  widths.
- Confirm no console errors, clipping, obscured Save action, or horizontal
  overflow.

## Rollout

1. Add the shared-snapshot tests and reproduce the current `$0 reserved`
   failure.
2. Replace the reduced rent Calendar input with the complete snapshot.
3. Add proposed-rent overlays to the existing day and week renderers.
4. Deploy without changing the authoritative rent facts.
5. Run the production visual gate and reconcile every visible total before
   marking the work complete.

## Decision recorded

Until David classifies a possible expense, reserve 100% of it when calculating
rent. This is intentionally conservative and supports the stated priority:
avoid a bounced expense or rent payment while rebuilding end-of-month cash.
