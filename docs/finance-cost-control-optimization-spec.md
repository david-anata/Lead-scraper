# Finance Cost Control Optimization

Status: Implemented release specification
Prepared: 2026-08-03
Depends on: `finance-ui-reset-spec.md`
Supersedes the CSV- and ClickUp-centered savings workflow in `finance-savings-ai-cfo-spec.md`.

## Outcome

Budget & Savings is a monthly cost-cutting workspace built from posted Plaid evidence. It answers what is still charging, what is truly recurring, which costs need a decision, which cancellations are underway, and which savings were later verified by the bank.

## Product Contract

- Plaid posted transactions are the cash evidence.
- Raw transactions and saved decisions are never deleted by classification.
- Payroll, checks, transfers, refunds, reversals, protected costs, one-time purchases, inactive merchants, and unresolved duplicates do not enter recurring savings totals.
- Dollar calculations and state transitions are deterministic. The LLM may explain and prioritize them but cannot alter the math or take a financial action.
- Classification changes are drafted locally, reviewed together, and saved in one batch.
- Reviewing a cost uses a full page, not a drawer, and shows transaction dates, account context, original bank wording, amounts, cadence, confidence, and exclusions.
- No service is canceled and no payment, payroll, or QuickBooks entry is made automatically.

## Recurrence Rules

- Monthly requires at least three comparable posted charges, a typical 20–40 day interval, and a charge in the latest complete month.
- Annual requires comparable charges 10–14 months apart; six months of data alone cannot establish it.
- Inactive means the recent expected charge is absent.
- Irregular and one-time activity remains visible only in history.
- Same-day, same-amount activity on the same account is quarantined as a probable duplicate.
- The stable recent median is the recurring baseline. A price increase is flagged at 10% and at least $10.

## Operator Workflow

The default workspace shows at most five current recurring costs requiring a decision. The groups are:

1. Needs a decision
2. Ready to cut
3. Cancellation started
4. Verifying the charge stopped
5. Savings confirmed

A Waste decision becomes Ready to cut. The operator then records an owner and action type, followed by the vendor confirmation, effective date, and proof. Finance waits one expected monthly charge window plus seven days before it can offer bank verification. Annual costs wait through their expected renewal window. A returned charge reopens the issue and removes it from verified savings.

## Savings Definitions

- Potential: an active cost that may be avoidable.
- Committed: cancellation or reduction work has started, but bank proof is pending.
- Realized: later posted Plaid evidence supports that the charge stopped.
- Reversed: a matching charge returned after realization.

Potential, committed, and realized amounts remain separate in every view. Only verified savings may be used as a cash-plan input.

## Monthly Brief

The first-working-day view leads with up to five cost exceptions: new recurring costs, price increases, probable duplicates, Waste items awaiting action, returned charges, and confirmed savings. It then exposes the full filtered backlog without turning historical cleanup into today's work.

## QuickBooks Boundary

Plaid remains the truth for money moved and QuickBooks remains the accounting system. Bookkeeping exceptions are separate from savings decisions. Savings review never writes to QuickBooks. Any future accounting write requires preview, explicit confirmation, idempotency, and an append-only audit record.

## Acceptance Criteria

- Identical inputs and as-of date produce identical merchant groups, states, and amounts.
- Every displayed amount is reproducible from visible source transactions.
- Client-submitted opportunity amounts are ignored; the server reloads and validates the current evidence hash before every save.
- Multiple decisions save atomically, survive reload, and preserve an unsaved local draft after accidental navigation.
- Potential or in-progress savings never count as realized cash.
- Cancellation state changes store owner, action, effective date, proof, actor, and timestamp.
- Realization cannot be confirmed until current Plaid evidence passes the deterministic verification rule.
- The deployed page works at phone and desktop widths, with keyboard focus, text status labels, no clipping, and no material console errors.

## Rollout

1. Ship merchant identity, recurrence, duplicate quarantine, and full-page review.
2. Preserve existing Needed, Unknown, Investigate, and Waste decisions.
3. Add cancellation and verification fields through additive database migration.
4. Run Finance regression tests and production data reconciliation.
5. Deploy, verify the exact release in production, and complete desktop and phone visual QA.
