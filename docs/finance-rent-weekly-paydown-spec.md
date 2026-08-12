# Weekly rent paydown correction

Status: Implemented specification  
Prepared: 12 August 2026

## Outcome

Calendar answers: **How much can I safely pay toward rent now while covering committed bills through next week?** It shows a specific read-only recommendation and its arithmetic. It never initiates payment.

## Rules

1. Plaid posted transactions are the authority that money moved.
2. Verified spendable cash funds the recommendation; Tax and reserves do not.
3. Confirmed and required expenses through the end of next calendar week reduce the envelope.
4. Possible expenses remain a visible warning but are not reserved automatically.
5. Future receivables do not fund a pay-now recommendation until posted.
6. The cash goal is advisory. Only the protected minimum constrains payment.
7. A reported payment is reserved while awaiting Plaid so it cannot be recommended twice.
8. A matching Plaid payment reduces the saved rent balance exactly once.

## Calculation

```text
maximum = verified spendable cash
        - confirmed/required expenses through next week
        - protected minimum
        - reported payments awaiting bank confirmation

recommended = round down 90% of maximum to the nearest $100
recommended = $0 when below the $500 action threshold
```

The recommendation cannot exceed rent remaining.

## Payment reported by David

David reported paying **$2,400 on 12 August 2026**. Finance stores this as `awaiting_bank`; it does not manufacture posted cash. Once Plaid matches the amount, Boulder Ranch merchant, and a date within seven days, Finance changes it to `bank_confirmed`, reduces the saved `$30,000` balance to `$27,600`, and recalculates. An unmatched report moves to Review after seven days.

## Acceptance criteria

- Possible warnings never force the base recommendation to zero.
- Required payments this week and next week always reduce it.
- The goal may change without changing the recommendation.
- The protected minimum changes the recommendation dollar-for-dollar.
- The exact window, maximum, cushion, recommendation, pending reports, and possible-warning total are visible.
- Reporting the same payment twice is idempotent.
- Matching the same Plaid row twice cannot reduce rent twice.
- The Calendar exposes payment reporting and rent settings even at a zero recommendation.
- Desktop and phone layouts do not overflow.
- No route initiates a payment.

## Validation

Run the focused rent tests and the full Finance suite, deploy the exact build, report the already-made `$2,400` payment once, and complete read-only desktop and phone visual QA in production.
